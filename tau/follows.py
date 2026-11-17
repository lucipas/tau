"""Signed follow lists — a node's follow graph, shared over RNS.

A follow list is a signed document naming the authors its owner
follows.  It travels over a link (``/tau/follows``) when a peer asks
for it, so the tiny announce budget stays tiny — and unlike the
local ``follows`` table in the Tan database it is portable and
verifiable: only the newest signed version per author is kept, and
a receiver can check the signature instead of trusting a claim.

The document is signed exactly like a Tan entry, a public profile
and a personal page: the author hashes the canonical unsigned body
and signs the digest with their identity.  A receiver verifies the
signature against the public key the document carries, and checks
that key derives the author's destination hash, so a follow list
can only be published by the peer it describes.

The list carries destination hashes only — no display names.  Names
are resolved locally from the peer registry, so the document stays
small and never leaks a name a peer has not announced.  Publishing
is explicit (``tau tan publish-follows``), from the local follow
list; the sync-depth setting and 2-hop expansion that consume these
lists stay for a follow-up (issue #467).
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import RNS

from .identity import load_public_key
from .protocol import APP_NAME, ASPECT
from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

FOLLOW_LIST_VERSION = 1
# The follow list mirrors the local follow cap (see
# ``tau.tangents.tan.MAX_FOLLOWS``): a signed list can name every
# author the node follows, and no more.
MAX_FOLLOW_LIST_AUTHORS = 512
SIG_HEX_LENGTH = 128
KEY_HEX_LENGTH = 128
AUTHOR_HEX_LENGTH = 32

_HEX32 = re.compile(r"^[0-9a-fA-F]{32}$")
_HEX128 = re.compile(r"^[0-9a-fA-F]{128}$")


class FollowListError(ValueError):
    """Raised when a follow-list document is malformed or fails verification."""


def clean_authors(value: Any) -> List[str]:
    """Normalise a follow list: sorted, deduplicated destination hashes.

    An empty list is valid — a node may follow nobody and still
    publish that fact — but the list must be a list, every entry a
    32-character hex destination hash, and the total at most
    :data:`MAX_FOLLOW_LIST_AUTHORS`.
    """
    if not isinstance(value, list):
        raise FollowListError("authors must be a list")
    if len(value) > MAX_FOLLOW_LIST_AUTHORS:
        raise FollowListError(
            f"follow list holds more than {MAX_FOLLOW_LIST_AUTHORS} "
            f"authors ({len(value)} given)"
        )
    cleaned: List[str] = []
    for entry in value:
        if not isinstance(entry, str) or not _HEX32.match(entry.strip()):
            raise FollowListError("authors must be destination hashes")
        cleaned.append(entry.strip().lower())
    return sorted(set(cleaned))


def _require_hex(value: Any, length: int, field: str) -> str:
    if not isinstance(value, str):
        raise FollowListError(f"{field} must be a hex string")
    text = value.strip().lower()
    if len(text) != length:
        raise FollowListError(f"{field} must be {length} hex characters")
    try:
        bytes.fromhex(text)
    except ValueError as exc:
        raise FollowListError(f"{field} is not valid hex") from exc
    return text


def _require_timestamp(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FollowListError(f"{field} must be a number")
    timestamp = float(value)
    if not math.isfinite(timestamp) or timestamp < 0:
        raise FollowListError(f"{field} must be a finite, non-negative timestamp")
    return timestamp


def _unsigned_fields(doc: Dict[str, Any]) -> Dict[str, Any]:
    """The fields covered by the signature, in canonical form."""
    return {
        "v": doc["v"],
        "author": doc["author"],
        "key": doc["key"],
        "authors": doc["authors"],
        "version": doc["version"],
        "updated_at": doc["updated_at"],
    }


def canonical_follow_list_bytes(doc: Dict[str, Any]) -> bytes:
    """Canonical bytes of a follow list's unsigned body (what is signed)."""
    return json.dumps(
        _unsigned_fields(doc),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def follow_list_digest(doc: Dict[str, Any]) -> str:
    """SHA-256 of the canonical unsigned body, as lowercase hex."""
    return hashlib.sha256(canonical_follow_list_bytes(doc)).hexdigest()


def normalize_follow_list(doc: Any, *, now: Optional[float] = None) -> Dict[str, Any]:
    """Validate the unsigned part of a follow-list document.

    Returns the normalised document without its signature; the
    signature itself is checked by :func:`verify_follow_list`.
    Extra fields are ignored, so a list stays verifiable when a newer
    version adds one — only the signed fields matter.
    """
    if not isinstance(doc, dict):
        raise FollowListError("follow list must be a JSON object")

    version_field = doc.get("v", FOLLOW_LIST_VERSION)
    if isinstance(version_field, bool) or version_field != FOLLOW_LIST_VERSION:
        raise FollowListError(f"unsupported follow list version: {version_field!r}")

    author = _require_hex(doc.get("author"), AUTHOR_HEX_LENGTH, "author")
    key = _require_hex(doc.get("key"), KEY_HEX_LENGTH, "key")

    authors = clean_authors(doc.get("authors", []))

    version = doc.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise FollowListError("follow list version must be an integer of at least 1")

    updated_at = _require_timestamp(doc.get("updated_at"), "updated_at")
    if now is not None and updated_at > float(now) + 86400.0:
        raise FollowListError("follow list timestamp is too far in the future")

    return {
        "v": FOLLOW_LIST_VERSION,
        "author": author,
        "key": key,
        "authors": authors,
        "version": version,
        "updated_at": updated_at,
    }


def build_follow_list(
    identity: RNS.Identity,
    *,
    authors: List[str],
    version: int = 1,
    updated_at: Optional[float] = None,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Build and sign a follow-list document owned by *identity*.

    The signature covers the canonical unsigned body, exactly like a
    Tan entry: the digest is signed, so any change to a field breaks
    verification.  *version* is the author's monotonic revision
    number — callers bump it on every publish, and receivers keep
    only the newest version they have seen.
    """
    timestamp = round(float(updated_at if updated_at is not None else time.time()), 3)
    unsigned = normalize_follow_list(
        {
            "v": FOLLOW_LIST_VERSION,
            "author": RNS.Destination.hash(identity, APP_NAME, ASPECT).hex(),
            "key": identity.get_public_key().hex(),
            "authors": authors,
            "version": version,
            "updated_at": timestamp,
        },
        now=now,
    )
    digest = follow_list_digest(unsigned)
    doc = dict(unsigned)
    doc["sig"] = identity.sign(bytes.fromhex(digest)).hex()
    return doc


def verify_follow_list(doc: Any, *, now: Optional[float] = None) -> Dict[str, Any]:
    """Validate a complete follow list, including its signature and key.

    Returns the normalised document.  A list whose key does not
    derive its author, or whose signature does not cover its body,
    is rejected — a peer cannot publish a follow list for anyone else.
    """
    unsigned = normalize_follow_list(doc, now=now)
    digest = follow_list_digest(unsigned)

    signature = _require_hex(doc.get("sig"), SIG_HEX_LENGTH, "sig")
    unsigned["sig"] = signature

    try:
        identity = RNS.Identity(create_keys=False)
        if not load_public_key(identity, bytes.fromhex(unsigned["key"])):
            raise ValueError("could not load public key")
    except Exception as exc:  # pragma: no cover - depends on RNS internals
        raise FollowListError(f"invalid public key: {exc}") from exc
    author = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    if author != unsigned["author"]:
        raise FollowListError("public key does not match the follow list author")
    try:
        valid = identity.validate(bytes.fromhex(signature), bytes.fromhex(digest))
    except Exception as exc:  # pragma: no cover - depends on RNS internals
        raise FollowListError(f"could not check signature: {exc}") from exc
    if not valid:
        raise FollowListError("follow list signature is invalid")
    return unsigned


def public_follow_list(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The display view of a follow list: everything except key and signature.

    The key and signature are public by design (the key is announced),
    but the console and CLI JSON only need the list itself, so they
    stay out of display payloads.
    """
    if doc is None:
        return None
    return {
        "author": doc["author"],
        "authors": list(doc["authors"]),
        "version": doc["version"],
        "updated_at": doc["updated_at"],
    }


def load_local_follow_list(
    path: Optional[Union[str, Path]],
) -> Optional[Dict[str, Any]]:
    """Load and verify the local follow list, or ``None`` when absent/invalid.

    An invalid file reads as no list (with a warning), never as a
    crash: a hand-edited or half-written file must not stop the node
    from serving.
    """
    if path is None:
        return None
    raw = read_json(Path(path).expanduser())
    if raw is None:
        return None
    try:
        return verify_follow_list(raw)
    except FollowListError as exc:
        log.warning("Ignoring invalid local follow list at %s: %s", path, exc)
        return None


def save_local_follow_list(path: Union[str, Path], doc: Dict[str, Any]) -> Path:
    """Persist a signed local follow list atomically, owner-only."""
    target = Path(path).expanduser()
    atomic_write_json(target, doc, mode=0o600)
    return target


class FollowListStore:
    """Received peer follow lists, keyed by author, newest version only.

    Persisted as one JSON file next to the peer registry.  Only the
    newest signed version per author is kept — an older or
    invalidly-signed document is dropped on arrival, and a
    same-version document never replaces the one already stored, so
    two copies of one version cannot fight over the record.
    """

    def __init__(self, path: Optional[Union[str, Path]] = None) -> None:
        self.path = Path(path).expanduser() if path is not None else None
        self._lists: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        if self.path is not None:
            self.load()

    def __len__(self) -> int:
        with self._lock:
            return len(self._lists)

    def get(self, author: Union[bytes, str]) -> Optional[Dict[str, Any]]:
        """The stored follow list for *author* (destination hash), or None."""
        if isinstance(author, bytes):
            key = author.hex()
        elif isinstance(author, str):
            key = author.strip().lower()
        else:
            return None
        with self._lock:
            doc = self._lists.get(key)
            return dict(doc) if doc is not None else None

    def all(self) -> List[Dict[str, Any]]:
        """Every stored follow list, newest first by version then time."""
        with self._lock:
            docs = list(self._lists.values())
        docs.sort(
            key=lambda doc: (doc["version"], doc["updated_at"]), reverse=True
        )
        return [dict(doc) for doc in docs]

    def store(self, doc: Any, *, now: Optional[float] = None) -> bool:
        """Verify *doc* and keep it when it is newer than the stored one.

        Returns ``True`` when the stored record changed.  Raises
        :class:`FollowListError` for a malformed or invalidly-signed
        document — callers that must not fail on a bad peer document
        catch it and drop the document instead.
        """
        verified = verify_follow_list(doc, now=now)
        with self._lock:
            existing = self._lists.get(verified["author"])
            if existing is not None and verified["version"] <= existing["version"]:
                return False
            self._lists[verified["author"]] = verified
            return True

    def remove(self, author: Union[bytes, str]) -> bool:
        """Drop the stored follow list for *author*; True when one existed."""
        if isinstance(author, bytes):
            key = author.hex()
        elif isinstance(author, str):
            key = author.strip().lower()
        else:
            return False
        with self._lock:
            return self._lists.pop(key, None) is not None

    def load(self) -> None:
        if self.path is None:
            return
        raw = read_json(self.path)
        if raw is None:
            return
        if isinstance(raw, dict):
            entries = raw.get("follow_lists", [])
        elif isinstance(raw, list):  # tolerate an older hand-written format
            entries = raw
        else:
            log.warning(
                "Ignoring follow list store at %s: unexpected format", self.path
            )
            return
        if not isinstance(entries, list):
            log.warning(
                "Ignoring follow list store at %s: invalid follow lists list",
                self.path,
            )
            return
        loaded: Dict[str, Dict[str, Any]] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                verified = verify_follow_list(entry)
            except FollowListError as exc:
                log.debug("Skipping invalid follow list entry: %s", exc)
                continue
            existing = loaded.get(verified["author"])
            if existing is not None and verified["version"] <= existing["version"]:
                continue
            loaded[verified["author"]] = verified
        with self._lock:
            self._lists = loaded

    def save(self) -> bool:
        """Persist the store atomically, owner-only; False with no path."""
        if self.path is None:
            return False
        with self._lock:
            docs = [dict(doc) for doc in self._lists.values()]
        docs.sort(key=lambda doc: (doc["version"], doc["updated_at"]), reverse=True)
        atomic_write_json(self.path, {"follow_lists": docs}, mode=0o600)
        return True
