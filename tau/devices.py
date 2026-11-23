"""Linked devices: pair a second device with a signed pairing bundle.

Slice 1 of #469: the primary mints a pairing bundle carrying its
identity, and the second device redeems it to run as the same
identity.  The bundle is a signed, expiring, single-use JSON file —
the same trust model as ``tau id export``: it carries the private
identity, so it must travel over a channel the owner trusts (a file
handed over, a QR code scanned in person), exactly like the identity
backup today.  What the bundle adds over a bare export is proof and
expiry: the primary signs the canonical bundle (including the
identity bytes), so a tampered bundle is rejected; the bundle
expires, so a stale file cannot pair a device months later; and a
token redeemed once cannot be redeemed again on the same node.

History catch-up on first connect and ``devices revoke`` are later
slices — this one only pairs the identity.  The registry
(``devices.json`` in the data directory) records offers this node
issued, tokens it redeemed, and — when this node was itself paired
— the primary it paired from, so ``tau devices list`` can show it.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import math
import os
import secrets
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import RNS

from .identity import destination_hash, load_identity
from .protocol import encode
from .util import atomic_write_json

log = logging.getLogger(__name__)

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore[assignment]

try:  # Windows only; the lock uses msvcrt there, as identity.py does.
    import msvcrt
except ImportError:  # pragma: no cover - POSIX
    msvcrt = None  # type: ignore[assignment]

PAIRING_BUNDLE_TYPE = "tau-pairing"
PAIRING_BUNDLE_VERSION = 1
DEFAULT_PAIRING_TTL_SECONDS = 3600
MAX_PAIRING_TTL_SECONDS = 86400
# A serialized identity is a few kilobytes; anything near this bound
# is a malformed or hostile bundle, rejected before it is decoded or
# written to a temporary file (see #714).
MAX_PAIRING_IDENTITY_B64_LENGTH = 65536
# A pairing bundle file is a few kilobytes of JSON; a larger file is
# not a bundle, and reading it unbounded would let a hostile file
# exhaust memory before verification (see #714).
MAX_PAIRING_BUNDLE_BYTES = 1024 * 1024
DEVICES_FILE_MODE = 0o600


class DevicesError(ValueError):
    """Raised when a pairing bundle or the device registry is invalid."""


def _clean_hex(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) != 32:
        raise DevicesError(f"pairing bundle has an invalid {field}")
    lowered = value.lower()
    try:
        bytes.fromhex(lowered)
    except ValueError:
        raise DevicesError(f"pairing bundle has an invalid {field}")
    return lowered


def _clean_time(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DevicesError(f"pairing bundle has an invalid {field}")
    try:
        cleaned = float(value)
    except (OverflowError, ValueError):
        raise DevicesError(f"pairing bundle has an invalid {field}")
    if not math.isfinite(cleaned):
        raise DevicesError(f"pairing bundle has an invalid {field}")
    return cleaned


def _identity_bytes(identity: RNS.Identity) -> bytes:
    """Serialize *identity* to the bytes ``tau id export`` would write."""
    with tempfile.TemporaryDirectory(prefix="tau-pairing-") as tmp:
        path = Path(tmp) / "identity"
        if not identity.to_file(str(path)):
            raise DevicesError("could not serialize identity for pairing")
        return path.read_bytes()


def _identity_from_bytes(data: bytes) -> RNS.Identity:
    """Load an identity from serialized bytes; a bad blob is rejected."""
    with tempfile.TemporaryDirectory(prefix="tau-pairing-") as tmp:
        path = Path(tmp) / "identity"
        path.write_bytes(data)
        try:
            identity = load_identity(path, harden=False)
        except Exception as exc:
            raise DevicesError(f"pairing bundle has an invalid identity: {exc}") from exc
    if identity is None:  # pragma: no cover - the file always exists here
        raise DevicesError("pairing bundle has an invalid identity")
    return identity


def _unsigned_bundle(bundle: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "v": bundle["v"],
        "type": bundle["type"],
        "token": bundle["token"],
        "primary": bundle["primary"],
        "created_at": bundle["created_at"],
        "expires_at": bundle["expires_at"],
        "identity": bundle["identity"],
    }


def build_pairing_bundle(
    identity: RNS.Identity,
    *,
    ttl_seconds: float = DEFAULT_PAIRING_TTL_SECONDS,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Mint a pairing bundle for *identity* (see the module docstring).

    Raises :class:`DevicesError` when the TTL is out of range or the
    identity cannot sign.
    """
    if (
        isinstance(ttl_seconds, bool)
        or not isinstance(ttl_seconds, (int, float))
        or not math.isfinite(float(ttl_seconds))
        or float(ttl_seconds) <= 0
        or float(ttl_seconds) > MAX_PAIRING_TTL_SECONDS
    ):
        raise DevicesError(
            "pairing TTL must be between 1 and "
            f"{MAX_PAIRING_TTL_SECONDS} seconds"
        )
    moment = time.time() if now is None else float(now)
    unsigned = {
        "v": PAIRING_BUNDLE_VERSION,
        "type": PAIRING_BUNDLE_TYPE,
        "token": secrets.token_hex(16),
        "primary": destination_hash(identity).hex(),
        "created_at": moment,
        "expires_at": moment + float(ttl_seconds),
        "identity": base64.b64encode(_identity_bytes(identity)).decode("ascii"),
    }
    try:
        signature = identity.sign(encode(unsigned))
    except Exception as exc:
        raise DevicesError(f"could not sign pairing bundle: {exc}") from exc
    if signature is None:  # pragma: no cover - identity without private keys
        raise DevicesError("identity cannot sign a pairing bundle")
    return {**unsigned, "sig": base64.b64encode(signature).decode("ascii")}


def verify_pairing_bundle(
    raw: Any, *, now: Optional[float] = None
) -> Tuple[RNS.Identity, Dict[str, Any]]:
    """Verify a pairing bundle; the identity and cleaned bundle, or raise.

    The bundle is self-contained: the signature must verify with the
    bundled identity's own key over the canonical unsigned payload,
    and the claimed primary must be that identity's destination —
    so a bundle whose identity bytes were swapped fails even though
    the signature field itself was untouched.  Expiry is checked
    against *now* (the current time by default).  Anything malformed,
    mismatched, forged or expired raises :class:`DevicesError`
    instead of returning, so callers never act on a bad bundle.
    """
    if not isinstance(raw, dict):
        raise DevicesError("pairing bundle must be a JSON object")
    if raw.get("v") != PAIRING_BUNDLE_VERSION:
        raise DevicesError("pairing bundle has an unsupported version")
    if raw.get("type") != PAIRING_BUNDLE_TYPE:
        raise DevicesError("pairing bundle has an unsupported type")
    token = _clean_hex(raw.get("token"), "token")
    primary = _clean_hex(raw.get("primary"), "primary")
    created_at = _clean_time(raw.get("created_at"), "created_at")
    expires_at = _clean_time(raw.get("expires_at"), "expires_at")
    if expires_at <= created_at:
        raise DevicesError("pairing bundle expires before it was created")
    moment = time.time() if now is None else float(now)
    if moment >= expires_at:
        raise DevicesError("pairing bundle has expired")
    identity_text = raw.get("identity")
    if not isinstance(identity_text, str) or not identity_text:
        raise DevicesError("pairing bundle has no identity")
    if len(identity_text) > MAX_PAIRING_IDENTITY_B64_LENGTH:
        raise DevicesError("pairing bundle identity is too large")
    try:
        identity_bytes = base64.b64decode(identity_text, validate=True)
    except (binascii.Error, ValueError):
        raise DevicesError("pairing bundle identity is not valid base64")
    identity = _identity_from_bytes(identity_bytes)
    if destination_hash(identity).hex() != primary:
        raise DevicesError(
            "pairing bundle primary does not match the bundled identity"
        )
    signature_text = raw.get("sig")
    if not isinstance(signature_text, str) or not signature_text:
        raise DevicesError("pairing bundle has no signature")
    try:
        signature = base64.b64decode(signature_text, validate=True)
    except (binascii.Error, ValueError):
        raise DevicesError("pairing bundle signature is not valid base64")
    bundle = {
        "v": PAIRING_BUNDLE_VERSION,
        "type": PAIRING_BUNDLE_TYPE,
        "token": token,
        "primary": primary,
        "created_at": created_at,
        "expires_at": expires_at,
        "identity": identity_text,
    }
    try:
        verified = identity.validate(signature, encode(_unsigned_bundle(bundle)))
    except Exception:
        verified = False
    if not verified:
        raise DevicesError("pairing bundle signature does not verify")
    return identity, bundle


class DeviceRegistry:
    """Offers issued, tokens redeemed, and the pairing this node holds.

    The file is ``devices.json`` in the data directory.  A corrupt
    file fails *closed*: the redeemed-token list is what enforces
    single-use, so a registry that cannot be read must refuse a
    redeem rather than forget which tokens were already used.
    Mutations take an exclusive lock on a sidecar lock file and
    reload first, like :class:`tau.groups.GroupStore`, so two CLI
    invocations cannot interleave a load and a save.
    """

    def __init__(self, path: Optional[Union[str, Path]] = None) -> None:
        self.path = Path(path).expanduser() if path is not None else None
        self._lock = Lock()
        self._issued: List[Dict[str, Any]] = []
        self._redeemed: List[Dict[str, Any]] = []
        self._paired_from: Optional[Dict[str, Any]] = None
        self._load()

    def _load(self) -> None:
        if self.path is None:
            return
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            # Fail closed: the redeemed-token list is what enforces
            # single-use, so a registry that cannot be read must refuse
            # a redeem rather than forget which tokens were used.
            raise DevicesError(
                f"device registry {self.path} is corrupt: {exc}"
            ) from exc
        if not isinstance(raw, dict):
            raise DevicesError(f"device registry {self.path} is corrupt")
        self._issued = self._clean_offers(raw.get("issued"))
        self._redeemed = self._clean_redeemed(raw.get("redeemed"))
        paired = raw.get("paired_from")
        self._paired_from = (
            self._clean_paired_from(paired) if paired is not None else None
        )

    @staticmethod
    def _clean_offers(raw: Any) -> List[Dict[str, Any]]:
        if raw is None:
            return []
        if not isinstance(raw, list):
            raise DevicesError("device registry issued offers must be a list")
        cleaned = []
        for entry in raw:
            if not isinstance(entry, dict):
                raise DevicesError("device registry issued offer must be an object")
            cleaned.append(
                {
                    "token": _clean_hex(entry.get("token"), "token"),
                    "created_at": _clean_time(entry.get("created_at"), "created_at"),
                    "expires_at": _clean_time(entry.get("expires_at"), "expires_at"),
                }
            )
        return cleaned

    @staticmethod
    def _clean_redeemed(raw: Any) -> List[Dict[str, Any]]:
        if raw is None:
            return []
        if not isinstance(raw, list):
            raise DevicesError("device registry redeemed tokens must be a list")
        cleaned = []
        for entry in raw:
            if not isinstance(entry, dict):
                raise DevicesError("device registry redeemed token must be an object")
            cleaned.append(
                {
                    "token": _clean_hex(entry.get("token"), "token"),
                    "primary": _clean_hex(entry.get("primary"), "primary"),
                    "redeemed_at": _clean_time(
                        entry.get("redeemed_at"), "redeemed_at"
                    ),
                    "expires_at": _clean_time(
                        entry.get("expires_at"), "expires_at"
                    ),
                    # Records written before the pending marker existed
                    # count as completed, so they are never replayed.
                    "paired": entry.get("paired") is not False,
                    "reset_pending": entry.get("reset_pending") is True,
                }
            )
        return cleaned

    @staticmethod
    def _clean_paired_from(raw: Any) -> Dict[str, Any]:
        if not isinstance(raw, dict):
            raise DevicesError("device registry paired_from must be an object")
        return {
            "primary": _clean_hex(raw.get("primary"), "primary"),
            "token": _clean_hex(raw.get("token"), "token"),
            "paired_at": _clean_time(raw.get("paired_at"), "paired_at"),
        }

    def _save(self) -> None:
        if self.path is None:
            return
        atomic_write_json(
            self.path,
            {
                "issued": self._issued,
                "redeemed": self._redeemed,
                "paired_from": self._paired_from,
            },
            mode=DEVICES_FILE_MODE,
        )

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        """Serialise a read-modify-write across processes (see class docstring)."""
        with self._lock:
            if self.path is None:
                yield
                return
            if fcntl is None and msvcrt is None:
                # pragma: no cover - neither platform offers a
                # cross-process lock; best effort only.
                self._load()
                yield
                self._save()
                return
            lock_path = self.path.with_name(self.path.name + ".lock")
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            with open(lock_path, "a+") as handle:
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                else:  # pragma: no cover - exercised on Windows
                    handle.seek(0, os.SEEK_END)
                    if handle.tell() == 0:
                        handle.write(" ")
                        handle.flush()
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                try:
                    self._load()
                    yield
                    self._save()
                finally:
                    if fcntl is not None:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    else:  # pragma: no cover - exercised on Windows
                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)

    def is_redeemed(self, token: str) -> bool:
        return any(entry["token"] == token.lower() for entry in self._redeemed)

    def redeemed_entry(self, token: str) -> Optional[Dict[str, Any]]:
        """Return a copy of the redeemed-token record for *token*, if any."""
        wanted = token.lower()
        for entry in self._redeemed:
            if entry["token"] == wanted:
                return dict(entry)
        return None

    def issued_offers(self, *, now: Optional[float] = None) -> List[Dict[str, Any]]:
        """Offers this node minted that have not expired yet."""
        moment = time.time() if now is None else float(now)
        return [entry for entry in self._issued if entry["expires_at"] > moment]

    @property
    def paired_from(self) -> Optional[Dict[str, Any]]:
        return dict(self._paired_from) if self._paired_from is not None else None

    def record_issued(
        self,
        token: str,
        created_at: float,
        expires_at: float,
        *,
        owner: Optional[str] = None,
    ) -> None:
        """Remember an offer this node minted; expired ones are pruned.

        Every unexpired offer is kept, however many there are: capping
        the list would silently drop a still-redeemable offer from
        ``tau devices list`` (see #714).  Offers are rare human
        actions and each record is a few dozen bytes, so the list
        cannot grow without bound in practice.

        *owner* is the primary hash of the identity that minted the
        offer, so a later reset can tell which identity issued it
        without relying on wall-clock timestamps (see #732).
        """
        with self._transaction():
            moment = time.time()
            self._issued = [
                entry for entry in self._issued if entry["expires_at"] > moment
            ]
            self._issued.append(
                {
                    "token": _clean_hex(token, "token"),
                    "created_at": float(created_at),
                    "expires_at": float(expires_at),
                    "owner": None if owner is None else _clean_hex(owner, "owner"),
                }
            )

    def record_redeemed(
        self,
        token: str,
        primary: str,
        expires_at: float,
        *,
        now: Optional[float] = None,
        reset_pending: bool = False,
    ) -> None:
        """Remember a redeemed token, so the bundle cannot be reused.

        Only expired tokens are pruned: the token's own expiry bounds
        the record's life, so a token redeemed inside its validity
        window stays recorded until that window ends — capping the
        list would let an early bundle pass the replay check again
        (see #714).  This records the token only; the pairing itself
        is recorded separately by :meth:`record_paired_from` after
        the identity is installed, so a failed install never claims
        a pairing that did not happen.
        """
        moment = time.time() if now is None else float(now)
        with self._transaction():
            self._redeemed = [
                entry for entry in self._redeemed if entry["expires_at"] > moment
            ]
            self._redeemed.append(
                {
                    "token": _clean_hex(token, "token"),
                    "primary": _clean_hex(primary, "primary"),
                    "redeemed_at": moment,
                    "expires_at": float(expires_at),
                    # Pending until the pairing is recorded after the
                    # identity install; a retry may complete only a
                    # pending record (see #718).
                    "paired": False,
                    # True while the identity-change reset this redeem
                    # needs has not yet been applied; cleared atomically
                    # by reset() so a retry never repeats it (see #718).
                    "reset_pending": bool(reset_pending),
                }
            )

    def forget_redeemed(self, token: str) -> None:
        """Drop a redeemed-token record that was written for a redeem which then failed.

        Only used to roll back :meth:`record_redeemed` when the identity
        swap could not be completed, so the bundle can be retried.
        """
        wanted = token.lower()
        with self._transaction():
            self._redeemed = [
                entry for entry in self._redeemed if entry["token"] != wanted
            ]

    def record_paired_from(
        self, token: str, primary: str, *, now: Optional[float] = None
    ) -> None:
        """Remember that this node paired from *primary* with *token*."""
        moment = time.time() if now is None else float(now)
        cleaned_token = _clean_hex(token, "token")
        with self._transaction():
            self._paired_from = {
                "primary": _clean_hex(primary, "primary"),
                "token": cleaned_token,
                "paired_at": moment,
            }
            # The pairing is complete: a later retry must not treat
            # this token as pending again (see #718).
            for entry in self._redeemed:
                if entry["token"] == cleaned_token:
                    entry["paired"] = True

    def reset(
        self,
        *,
        now: Optional[float] = None,
        keep_offers_owned_by: Optional[str] = None,
    ) -> None:
        """Drop the pairing records tied to the replaced identity.

        Issued offers and the pairing itself belong to the identity
        that was installed when they were recorded.  After an identity
        replacement they would otherwise be reported as the new
        identity's pairing, so a swap resets them (see #714).

        Redeemed tokens are NOT dropped: they are what enforces
        single-use, and a token redeemed under the old identity is
        still a live bundle for up to 24 hours — clearing the list
        would let the same bundle be redeemed again with ``--force``
        after the swap (see #716).  Only expired tokens are pruned,
        the same rule :meth:`record_redeemed` applies.

        *keep_offers_owned_by* is only for the pending reset a
        stranded-pairing retry completes (see #732): after a failed
        redeem the node already serves as the new identity, so offers
        minted by that identity (matched on the recorded ``owner``)
        are kept and every other offer is dropped.  Ownership is
        recorded at mint time, so a clock rollback cannot move an
        offer across the cutoff.  Offers without a recorded owner are
        dropped.  A plain swap (``tau id import`` or a first redeem)
        passes no owner and drops every offer.
        """
        moment = time.time() if now is None else float(now)
        owner = (
            None
            if keep_offers_owned_by is None
            else _clean_hex(keep_offers_owned_by, "keep_offers_owned_by")
        )
        with self._transaction():
            if owner is None:
                self._issued = []
            else:
                self._issued = [
                    entry for entry in self._issued if entry["owner"] == owner
                ]
            self._redeemed = [
                entry for entry in self._redeemed if entry["expires_at"] > moment
            ]
            # The reset is applied: no redeem still owes one.  Cleared in
            # the same transaction so a retry never repeats it and drops
            # offers issued after this reset (see #718).
            for entry in self._redeemed:
                entry["reset_pending"] = False
            self._paired_from = None
= None
