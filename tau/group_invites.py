"""Signed group invite links and pending join requests.

The only way into a tau group today is for the creator to add you
(``tau group add-member``), which makes every group a bottleneck on
one person and real-world onboarding ("join our mesh group")
impossible without the creator present.  Invite links are the fix:
the creator mints a signed token for a group (``tau group invite``)
and shares it as a link; anyone holding it can ask to join.

A token is a signed payload — group name, creator hash, open/closed
mode, a generation counter, and the issue time — carried as
``tau://invite/<base64>``.  The payload is sealed to the creator:
it is encrypted with the creator's own identity before it becomes
a link, so the link itself carries only the creator hash — the
joiner needs that to address the join-request DM — plus an opaque
sealed blob.  Anyone holding the link learns nothing about the
group: no name, no id, no mode, no generation.  The creator's node
decrypts the token and verifies the signature against its own
identity, so a token can only be minted by the creator, and a token
is only honoured while its generation matches the stored one:
``tau group invite --revoke`` bumps the generation and every token
minted before it stops working.  Tokens minted before sealing (v1
links, signed but unencrypted) still parse and verify, so
outstanding links keep working; new mints are always sealed (v2).

The token names the group by name, not by id: a group's id derives
from its roster, so the first join already retires the id the token
was minted for.  The name is the stable handle — the creator's node
resolves it to the current roster on every join — which is also why
renaming a group invalidates its outstanding invites (mint a fresh
one after a rename).  The invite and join-request stores are keyed
the same way, so pending requests survive roster changes too.

A secret group's token instead names the group by its opaque id
(see :func:`mint_invite_token`): the name never appears in the
token, so anyone holding the link learns nothing about the group.
The creator resolves the id to the group locally — a current id,
or a retired one via the store's retired record — and the
mode/generation check still uses the name-keyed store, so revoke
semantics are unchanged.  A rename still invalidates an id token,
exactly like a name token: the join is refused when the resolved
group's current name differs from the name the token was minted
for, so revoking under the new name cannot leave the old token
usable against the renamed group.  Minting also records an alias
(the id plus the group's name, creator and birth time) in the
invite store, so the id still resolves after the group store
evicts the retired record; a revoke drops the group's aliases
with its generation bump.  Since sealing, the id itself is hidden
too: it rides inside the sealed blob, and the link carries only
the creator hash.

The join itself is a direct message with kind
``group-join-request`` whose text is the invite link, sent to the
creator's destination hash (which the link carries).  The creator's
node intercepts that kind before the DM allow-list: a stranger
holding a valid token is not a stranger any more.  Open groups
add the requester to the roster at once; closed groups record a
pending join request the creator reviews with
``tau group requests`` and admits with ``tau group approve`` (or
refuses with ``tau group deny``).  Either way the new member
learns the roster from the welcome group message the creator's
node sends to the new roster — a group message pins the roster
for every recipient, including a joiner who has never seen the
group before.

An invalid token is rejected with the same generic error whether
the group does not exist, the mode or generation mismatches, or
the signature fails, so a join request never leaks whether a
group id is real.  A join-request message is never stored as
chat: the kind is intercepted on both the live path and the
store-and-forward path, so the token never lands in an inbox.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

import RNS

from .protocol import APP_NAME, ASPECT, decode, encode
from .util import atomic_write_json

log = logging.getLogger(__name__)

INVITE_LINK_PREFIX = "tau://invite/"
INVITE_MODES = ("open", "closed")
DEFAULT_INVITE_MODE = "open"
INVITES_FILE_MODE = 0o600

JOIN_REQUEST_TTL = 7 * 24 * 60 * 60
MAX_JOIN_REQUESTS = 200
JOIN_REQUESTS_FILE_MODE = 0o600

# The wire kind a join request travels under (see protocol.py: a
# kind is a short lowercase slug; "group"/"file"/"note" are reserved).
JOIN_REQUEST_KIND = "group-join-request"

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore[assignment]

try:  # Windows only; the lock uses msvcrt there, as identity.py does.
    import msvcrt
except ImportError:  # pragma: no cover - POSIX
    msvcrt = None  # type: ignore[assignment]


class InviteError(ValueError):
    """Raised when an invite link, token, or reference is malformed."""


def _clean_group_name(value: Any) -> str:
    """Normalise a group name (1-48 chars, as group names are capped)."""
    if not isinstance(value, str):
        raise InviteError("invite group must be a string")
    cleaned = " ".join(value.split()).strip()
    if not cleaned or len(cleaned) > 48:
        raise InviteError("invite group must be a 1-48 character name")
    return cleaned


def _clean_hash(value: Any, label: str) -> str:
    """Normalise a tau destination hash (32 hex chars, 16 bytes)."""
    if not isinstance(value, str):
        raise InviteError(f"invite {label} must be a string")
    cleaned = value.strip().lower()
    if len(cleaned) != 32 or any(c not in "0123456789abcdef" for c in cleaned):
        raise InviteError(f"invite {label} must be a 32-hex destination hash")
    return cleaned


def _clean_mode(value: Any) -> str:
    if not isinstance(value, str) or value.strip().lower() not in INVITE_MODES:
        raise InviteError("invite mode must be 'open' or 'closed'")
    return value.strip().lower()


def _clean_generation(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise InviteError("invite generation must be a non-negative integer")
    return value


def _clean_timestamp(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise InviteError(f"invite {label} must be a number")
    cleaned = float(value)
    if cleaned != cleaned or cleaned in (float("inf"), float("-inf")):
        raise InviteError(f"invite {label} must be finite")
    return cleaned


def invite_token_group_label(token: Dict[str, Any]) -> str:
    """The group a token names, for display in a queued-join reply.

    A name-keyed v1 token names the group; an id-keyed v1 token
    (secret group) has no name, so the opaque id is reported
    instead — a handler that reads ``token["group"]``
    unconditionally raises ``KeyError`` on an id token after the
    request was queued.  A sealed (v2) token names nothing in the
    clear — the joiner learns the group from the creator's reply,
    not the link — so the label is empty.
    """
    group = token.get("group")
    if isinstance(group, str) and group:
        return group
    group_id = token.get("group_id")
    if isinstance(group_id, str) and group_id:
        return group_id
    return ""


def _build_signed_invite_token(
    identity: RNS.Identity,
    *,
    group_name: Optional[str] = None,
    group_id: Optional[str] = None,
    creator_hash: str,
    mode: str = DEFAULT_INVITE_MODE,
    generation: int = 0,
    issued_at: Optional[float] = None,
) -> Dict[str, Any]:
    """Build the signed v1 invite payload (plus ``"sig"``).

    This is the plaintext a sealed token protects: exactly one of
    *group_name* and *group_id* names the group — a name for an
    ordinary group, an opaque id for a secret group.
    """
    if (group_name is None) == (group_id is None):
        raise InviteError(
            "invite token needs exactly one of group_name and group_id"
        )
    payload: Dict[str, Any] = {
        "v": 1,
        "type": "group-invite",
        "creator": _clean_hash(creator_hash, "creator"),
        "mode": _clean_mode(mode),
        "generation": _clean_generation(generation),
        "issued_at": _clean_timestamp(
            time.time() if issued_at is None else issued_at, "issued_at"
        ),
    }
    if group_name is not None:
        payload["group"] = _clean_group_name(group_name)
    else:
        payload["group_id"] = _clean_hash(group_id, "group_id")
    signature = identity.sign(encode(payload))
    if signature is None:  # pragma: no cover - identity without private keys
        raise InviteError("creator identity cannot sign")
    token = dict(payload)
    token["sig"] = base64.b64encode(signature).decode("ascii")
    return token


def _encode_v1_invite_link(token: Dict[str, Any]) -> str:
    """Carry a signed v1 token as a link (the pre-sealing format)."""
    return INVITE_LINK_PREFIX + base64.urlsafe_b64encode(encode(token)).decode(
        "ascii"
    )


# The sealed plaintext is padded to this fixed size before encryption,
# so the sealed blob's length does not reveal the group name's length.
# Worst-case inner JSON (a 48-character UTF-8 name plus the signature)
# is well under the bucket; a 2-byte big-endian length prefix says how
# much of it is payload, and the rest is zero padding.
SEAL_PLAINTEXT_BYTES = 512
_SEAL_LENGTH_PREFIX_BYTES = 2


def _pad_sealed_plaintext(body: bytes) -> bytes:
    capacity = SEAL_PLAINTEXT_BYTES - _SEAL_LENGTH_PREFIX_BYTES
    if len(body) > capacity:
        raise InviteError("invite token is too large to seal")
    return (
        len(body).to_bytes(_SEAL_LENGTH_PREFIX_BYTES, "big")
        + body
        + b"\0" * (capacity - len(body))
    )


def _unpad_sealed_plaintext(padded: bytes) -> bytes:
    """The payload of a padded sealed plaintext, or an error."""
    if len(padded) != SEAL_PLAINTEXT_BYTES:
        raise InviteError("invite is not valid for this group")
    length = int.from_bytes(padded[:_SEAL_LENGTH_PREFIX_BYTES], "big")
    capacity = SEAL_PLAINTEXT_BYTES - _SEAL_LENGTH_PREFIX_BYTES
    if length > capacity:
        raise InviteError("invite is not valid for this group")
    body = padded[_SEAL_LENGTH_PREFIX_BYTES : _SEAL_LENGTH_PREFIX_BYTES + length]
    if padded[_SEAL_LENGTH_PREFIX_BYTES + length :].strip(b"\0"):
        raise InviteError("invite is not valid for this group")
    return body


def _seal_invite_token(identity: RNS.Identity, token: Dict[str, Any]) -> str:
    """Seal a signed token to the creator's own identity (v2 link).

    The signed payload is padded to a fixed size and encrypted with
    the creator's identity, so the link carries only the creator
    hash — the routing info a joiner needs to address the join-request
    DM — plus an opaque sealed blob of constant length.  No group
    field of any kind appears in the clear, and the link length does
    not depend on the group name; a fresh mint seals with a fresh
    ephemeral key each time.
    """
    sealed = identity.encrypt(_pad_sealed_plaintext(encode(token)))
    if not sealed:  # pragma: no cover - RNS encrypt failure
        raise InviteError("creator identity cannot seal the invite")
    outer: Dict[str, Any] = {
        "v": 2,
        "type": "group-invite",
        "creator": token["creator"],
        "sealed": base64.b64encode(sealed).decode("ascii"),
    }
    return INVITE_LINK_PREFIX + base64.urlsafe_b64encode(encode(outer)).decode(
        "ascii"
    )


def mint_invite_token(
    identity: RNS.Identity,
    *,
    group_name: Optional[str] = None,
    group_id: Optional[str] = None,
    creator_hash: str,
    mode: str = DEFAULT_INVITE_MODE,
    generation: int = 0,
    issued_at: Optional[float] = None,
) -> str:
    """Mint a signed invite link for a group.

    The payload is signed with the creator's identity, so only the
    creator can mint a token anyone will honour.  *generation* is
    the invite generation the token belongs to; bumping it (see
    :func:`revoke_invites`) invalidates every token minted before.
    Exactly one of *group_name* and *group_id* names the group: a
    name for an ordinary group, an opaque id for a secret group —
    the id token carries no name, so anyone holding the link learns
    nothing about the group it opens.

    The link is sealed (v2): the signed payload is encrypted to
    the creator's own identity, so the link itself is opaque to
    anyone but the creator's node — no name, no id, no mode, no
    generation in the clear.  Only the creator hash rides outside
    the seal, because the joiner needs it to address the
    join-request DM.
    """
    token = _build_signed_invite_token(
        identity,
        group_name=group_name,
        group_id=group_id,
        creator_hash=creator_hash,
        mode=mode,
        generation=generation,
        issued_at=issued_at,
    )
    return _seal_invite_token(identity, token)


def _validate_v1_token_shape(token: Dict[str, Any]) -> Dict[str, Any]:
    """Check a v1 token's shape; the token itself, or an error.

    Exactly one of ``"group"`` (a name) and ``"group_id"`` (an
    opaque id, for a secret group) names the group — a token
    carrying both, or neither, is malformed.  Shared by the v1
    parse path and the v2 unseal path, so a sealed token's inner
    payload is held to the same shape as a bare v1 token.
    """
    if not isinstance(token, dict) or token.get("type") != "group-invite":
        raise InviteError("not a group invite token")
    if token.get("v") != 1:
        raise InviteError("unsupported invite token version")
    has_name = token.get("group") is not None
    has_id = token.get("group_id") is not None
    if has_name == has_id:
        raise InviteError(
            "invite token needs exactly one of group and group_id"
        )
    if has_name:
        _clean_group_name(token.get("group"))
    else:
        _clean_hash(token.get("group_id"), "group_id")
    _clean_hash(token.get("creator"), "creator")
    _clean_mode(token.get("mode"))
    _clean_generation(token.get("generation"))
    _clean_timestamp(token.get("issued_at"), "issued_at")
    signature_text = token.get("sig")
    if not isinstance(signature_text, str) or not signature_text:
        raise InviteError("invite token is missing its signature")
    try:
        base64.b64decode(signature_text, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise InviteError("invite token signature is not valid base64") from exc
    return token


def _parse_v2_invite_link(token: Dict[str, Any]) -> Dict[str, Any]:
    """Check a sealed (v2) token's outer envelope.

    The outer envelope carries only the creator hash — the routing
    info a joiner needs — plus the sealed blob.  Returns the
    envelope with the blob decoded to bytes; the inner payload
    stays sealed until :func:`verify_invite_link` opens it with
    the creator's identity.
    """
    if not isinstance(token, dict) or token.get("type") != "group-invite":
        raise InviteError("not a group invite token")
    if token.get("v") != 2:
        raise InviteError("unsupported invite token version")
    creator = _clean_hash(token.get("creator"), "creator")
    sealed_text = token.get("sealed")
    if not isinstance(sealed_text, str) or not sealed_text:
        raise InviteError("invite token is missing its sealed payload")
    try:
        sealed = base64.b64decode(sealed_text, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise InviteError("invite token sealed payload is not valid base64") from exc
    if not sealed:
        raise InviteError("invite token is missing its sealed payload")
    return {
        "v": 2,
        "type": "group-invite",
        "creator": creator,
        "sealed": sealed,
    }


def parse_invite_link(link: Any) -> Dict[str, Any]:
    """Decode an invite link into its signed payload.

    Returns the token dict (payload plus ``"sig"`` as base64 text)
    for a v1 link, or the outer envelope (``"creator"`` plus the
    sealed blob as bytes) for a sealed v2 link.  The signature
    itself is not checked here — that needs the creator's identity
    (see :func:`verify_invite_link`) — and a v2 link's inner
    payload is not opened here either: only the creator's node can
    decrypt it.  A joiner needs nothing more than ``"creator"``
    from the parsed link, to address the join-request DM.
    """
    if not isinstance(link, str):
        raise InviteError("invite link must be a string")
    text = link.strip()
    if not text.startswith(INVITE_LINK_PREFIX):
        raise InviteError("not an invite link")
    raw = text[len(INVITE_LINK_PREFIX) :].strip()
    if not raw:
        raise InviteError("invite link is empty")
    try:
        decoded = base64.urlsafe_b64decode(raw)
    except (binascii.Error, ValueError) as exc:
        raise InviteError("invite link is not valid base64") from exc
    try:
        token = decode(decoded)
    except Exception as exc:
        raise InviteError(f"invite link is not a valid token: {exc}") from exc
    if not isinstance(token, dict) or token.get("type") != "group-invite":
        raise InviteError("not a group invite token")
    version = token.get("v")
    if version == 1:
        # Validate the shape now so a malformed token fails the same way
        # everywhere instead of halfway through verification.
        return _validate_v1_token_shape(token)
    if version == 2:
        return _parse_v2_invite_link(token)
    raise InviteError("unsupported invite token version")


def _unseal_invite_token(
    identity: RNS.Identity, envelope: Dict[str, Any]
) -> Dict[str, Any]:
    """Open a sealed (v2) token's inner payload with the creator's identity.

    Returns the signed v1 token, held to the same shape as a bare
    v1 link.  The outer creator must match the inner one, so a
    sealed blob cannot be re-wrapped under another creator's hash.
    Every failure raises the same generic :class:`InviteError`,
    so a caller that reports it cannot leak which check failed —
    or whether the link was sealed at all.
    """
    try:
        plaintext = identity.decrypt(bytes(envelope["sealed"]))
    except Exception as exc:
        raise InviteError("invite is not valid for this group") from exc
    if not plaintext:
        raise InviteError("invite is not valid for this group")
    try:
        inner = decode(_unpad_sealed_plaintext(plaintext))
    except Exception as exc:
        raise InviteError("invite is not valid for this group") from exc
    try:
        token = _validate_v1_token_shape(inner)
    except InviteError as exc:
        raise InviteError("invite is not valid for this group") from exc
    if token["creator"] != envelope["creator"]:
        raise InviteError("invite is not valid for this group")
    return token


def verify_invite_link(
    link: Any,
    *,
    identity: RNS.Identity,
    invites: Dict[str, Any],
    resolve_group_id: Optional[Callable[[str], Optional[str]]] = None,
) -> Dict[str, Any]:
    """Verify an invite link against this node's identity and store.

    The token must be signed by this node (the creator), name a
    group this node created, and match the stored mode and
    generation for that group — a revoked token fails the
    generation check.  Returns the token payload on success.
    Every failure raises the same :class:`InviteError`, so a caller
    that reports it cannot leak which check failed.

    A token for a secret group carries the opaque group id instead
    of the name: *resolve_group_id* maps that id to the group's
    name, so the mode/generation check still reads the name-keyed
    store.  The resolver is the caller's local group store (a
    current id, or a retired one via the retired record); without
    one an id token cannot verify.  The returned dict carries both
    ``"group"`` (the name, ``None`` only when the token itself was
    rejected) and ``"group_id"`` (the id, or ``None`` for a
    name-keyed token).

    A sealed (v2) link is opened with the creator's identity
    first: only this node can decrypt its own tokens, so a wrong
    identity fails the decrypt before any other check runs.
    """
    token = parse_invite_link(link)
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    if token["creator"] != own_hash:
        raise InviteError("invite was not issued by this node")
    if token.get("v") == 2:
        token = _unseal_invite_token(identity, token)
    signature = base64.b64decode(token["sig"])
    unsigned = {k: v for k, v in token.items() if k != "sig"}
    try:
        verified = identity.validate(signature, encode(unsigned))
    except Exception as exc:
        raise InviteError(f"could not verify invite: {exc}") from exc
    if not verified:
        raise InviteError("invite signature does not verify")
    group_name = token.get("group")
    group_id = token.get("group_id")
    if group_id is not None:
        # The token names no group by name: resolve the id locally
        # instead, so the name never appears in the token itself.
        if resolve_group_id is None:
            raise InviteError("invite is not valid for this group")
        group_name = resolve_group_id(_clean_hash(group_id, "group_id"))
        if group_name is None:
            raise InviteError("invite is not valid for this group")
    record = None
    if isinstance(invites, dict):
        groups = invites.get("groups")
        if isinstance(groups, dict):
            # Store keys are casefolded names (group lookup is
            # case-insensitive, see GroupStore.find).
            record = groups.get(group_name.casefold())
    if not isinstance(record, dict):
        raise InviteError("invite is not valid for this group")
    if _clean_mode(record.get("mode")) != token["mode"]:
        raise InviteError("invite is not valid for this group")
    if _clean_generation(record.get("generation")) != token["generation"]:
        raise InviteError("invite is not valid for this group")
    return {
        "group": group_name,
        "group_id": (
            _clean_hash(group_id, "group_id") if group_id is not None else None
        ),
        "creator": token["creator"],
        "mode": token["mode"],
        "generation": token["generation"],
        "issued_at": token["issued_at"],
    }


# ---------------------------------------------------------------------------
# Invite store: per-group mode + generation (``invites.json``).
# ---------------------------------------------------------------------------


def _clean_aliases(data: Any) -> Dict[str, Dict[str, Any]]:
    """The id-to-group aliases of an invite store, cleaned.

    An alias records what an id-keyed token named when it was
    minted — the group's name, creator and birth time — so the
    creator can still resolve the id after the group store evicts
    the retired record the id used to resolve through (the retired
    cache is capped; the invite record is not).  A missing map
    means no id token was ever minted.
    """
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise InviteError("invites file aliases must be an object")
    aliases: Dict[str, Dict[str, Any]] = {}
    for group_id, record in data.items():
        if not isinstance(record, dict):
            raise InviteError("invite alias must be an object")
        aliases[_clean_hash(group_id, "group_id")] = {
            "name": _clean_group_name(record.get("name")),
            "creator": _clean_hash(record.get("creator"), "creator"),
            "created_at": _clean_timestamp(record.get("created_at"), "created_at"),
        }
    return aliases


def load_invites(path: Optional[Union[str, Path]]) -> Dict[str, Any]:
    """Load the invite store at *path*; a missing file means none."""
    if path is None:
        return {"groups": {}, "aliases": {}}
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return {"groups": {}, "aliases": {}}
    except OSError as exc:
        raise InviteError(f"could not read invites {target}: {exc}") from exc
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise InviteError(f"could not read invites {target}: {exc}") from exc
    if not isinstance(data, dict) or not isinstance(data.get("groups"), dict):
        raise InviteError("invites file must be an object with a groups map")
    groups: Dict[str, Any] = {}
    for group_name, record in data["groups"].items():
        if not isinstance(record, dict):
            raise InviteError("invite record must be an object")
        # The link is the last token minted for the group, so the
        # console can show the current link without minting a fresh
        # token on every view; a record written before links were
        # stored simply has none.
        groups[_clean_group_name(group_name).casefold()] = {
            "mode": _clean_mode(record.get("mode")),
            "generation": _clean_generation(record.get("generation")),
            "link": (
                record.get("link")
                if isinstance(record.get("link"), str)
                else None
            ),
        }
    return {"groups": groups, "aliases": _clean_aliases(data.get("aliases"))}


def save_invites(path: Union[str, Path], invites: Dict[str, Any]) -> Path:
    """Persist *invites* to *path* as JSON (mode 0600)."""
    target = Path(path).expanduser()
    document = {
        "groups": invites.get("groups", {}),
        "aliases": invites.get("aliases", {}),
    }
    return atomic_write_json(target, document, mode=INVITES_FILE_MODE)


@contextmanager
def invites_lock(path: Union[str, Path]) -> Iterator[None]:
    """Serialize read-modify-write updates of the invite store.

    Same shape as the DM-requests lock: a flock on a
    ``.invites.json.lock`` sidecar.  Callers must reload inside
    the lock.
    """
    if fcntl is None and msvcrt is None:  # pragma: no cover - neither platform
        yield
        return
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    lock_path = target.with_name(f".{target.name}.lock")
    with open(lock_path, "a+") as lock:
        if fcntl is not None:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        else:  # pragma: no cover - exercised on Windows
            lock.seek(0, os.SEEK_END)
            if lock.tell() == 0:
                lock.write(" ")
                lock.flush()
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            else:  # pragma: no cover - exercised on Windows
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)


def mint_invite(
    path: Union[str, Path],
    identity: RNS.Identity,
    *,
    group_name: str,
    creator_hash: str,
    mode: str = DEFAULT_INVITE_MODE,
    group_id: Optional[str] = None,
    group_created_at: Optional[float] = None,
) -> str:
    """Mint and record an invite link for *group_name*.

    The mode is stored per group (so verification can check it)
    and the generation is kept: minting never invalidates older
    tokens for the same group, only :func:`revoke_invites` does.
    The store is keyed by the casefolded group name, so the record
    survives the roster changes that retire the group's id.  The
    minted link itself is stored too, so the console can show the
    current link without minting a fresh token on every view.
    *group_id* mints a token for a secret group instead: the token
    carries the opaque id and no name, while the record still
    lives under the name (the store is local state, never shared).
    An id token also records an alias — the id plus the group's
    name, creator and birth time (*group_created_at*, required
    with *group_id*) — so the id still resolves after the group
    store evicts the retired record it used to resolve through.
    """
    group_name = _clean_group_name(group_name)
    mode = _clean_mode(mode)
    if group_id is not None and group_created_at is None:
        raise InviteError(
            "an id-keyed invite needs the group's created_at for its alias"
        )
    with invites_lock(path):
        invites = load_invites(path)
        record = invites["groups"].get(group_name.casefold())
        generation = (
            _clean_generation(record.get("generation"))
            if isinstance(record, dict)
            else 0
        )
        link = mint_invite_token(
            identity,
            group_name=group_name if group_id is None else None,
            group_id=group_id,
            creator_hash=creator_hash,
            mode=mode,
            generation=generation,
        )
        if group_id is not None:
            aliases = invites.setdefault("aliases", {})
            aliases[_clean_hash(group_id, "group_id")] = {
                "name": group_name,
                "creator": _clean_hash(creator_hash, "creator"),
                "created_at": _clean_timestamp(
                    group_created_at, "created_at"
                ),
            }
        invites["groups"][group_name.casefold()] = {
            "mode": mode,
            "generation": generation,
            "link": link,
        }
        save_invites(path, invites)
    return link


def revoke_invites(path: Union[str, Path], *, group_name: str) -> int:
    """Invalidate every invite minted for *group_name* so far.

    Bumps the stored generation; tokens carrying an older one no
    longer verify.  The stored link is dropped with it, so nothing
    keeps advertising a token that no longer verifies — and the
    group's id aliases go too, so a revoked id link cannot still
    resolve after its retired record is evicted.  Returns the
    new generation.
    """
    group_name = _clean_group_name(group_name)
    with invites_lock(path):
        invites = load_invites(path)
        record = invites["groups"].get(group_name.casefold())
        generation = (
            _clean_generation(record.get("generation"))
            if isinstance(record, dict)
            else 0
        )
        mode = (
            _clean_mode(record.get("mode"))
            if isinstance(record, dict)
            else DEFAULT_INVITE_MODE
        )
        generation += 1
        invites["groups"][group_name.casefold()] = {
            "mode": mode,
            "generation": generation,
        }
        aliases = invites.get("aliases")
        if isinstance(aliases, dict):
            for alias_id in [
                group_id
                for group_id, alias in aliases.items()
                if isinstance(alias, dict)
                and isinstance(alias.get("name"), str)
                and alias["name"].casefold() == group_name.casefold()
            ]:
                del aliases[alias_id]
        save_invites(path, invites)
    return generation


def invite_status(
    path: Optional[Union[str, Path]], *, group_name: str
) -> Optional[Dict[str, Any]]:
    """The stored invite record for *group_name*, or ``None``."""
    invites = load_invites(path)
    record = invites["groups"].get(_clean_group_name(group_name).casefold())
    if not isinstance(record, dict):
        return None
    return {
        "mode": _clean_mode(record.get("mode")),
        "generation": _clean_generation(record.get("generation")),
        "link": record.get("link") if isinstance(record.get("link"), str) else None,
    }


# ---------------------------------------------------------------------------
# Pending join requests (``join-requests.json``).
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class JoinRequest:
    # The group NAME, not its id: a roster change retires the id,
    # so a request keyed by id would be orphaned by the next join.
    group: str
    peer: str
    requested_at: float
    expires_at: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "group": self.group,
            "peer": self.peer,
            "requested_at": self.requested_at,
            "expires_at": self.expires_at,
        }

    @classmethod
    def from_dict(cls, data: Any) -> "JoinRequest":
        if not isinstance(data, dict):
            raise InviteError("join request must be an object")
        return cls(
            group=_clean_group_name(data.get("group")),
            peer=_clean_hash(data.get("peer"), "peer"),
            requested_at=_clean_timestamp(data.get("requested_at"), "requested_at"),
            expires_at=_clean_timestamp(data.get("expires_at"), "expires_at"),
        )

    def is_expired(self, now: Optional[float] = None) -> bool:
        return (time.time() if now is None else now) >= self.expires_at


def _clean_join_requests(data: Any) -> List[JoinRequest]:
    if not isinstance(data, dict) or not isinstance(data.get("requests"), list):
        raise InviteError("join requests file must be an object with a requests list")
    return [JoinRequest.from_dict(entry) for entry in data["requests"]]


def load_join_requests(path: Optional[Union[str, Path]]) -> List[JoinRequest]:
    """Load the pending join requests at *path*; missing means none."""
    if path is None:
        return []
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise InviteError(
            f"could not read join requests {target}: {exc}"
        ) from exc
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise InviteError(
            f"could not read join requests {target}: {exc}"
        ) from exc
    return _clean_join_requests(data)


def save_join_requests(path: Union[str, Path], requests: List[JoinRequest]) -> Path:
    """Persist *requests* to *path* as JSON (mode 0600)."""
    target = Path(path).expanduser()
    document = {"requests": [entry.to_dict() for entry in requests]}
    return atomic_write_json(target, document, mode=JOIN_REQUESTS_FILE_MODE)


@contextmanager
def join_requests_lock(path: Union[str, Path]) -> Iterator[None]:
    """Serialize read-modify-write updates of the join-request store."""
    if fcntl is None and msvcrt is None:  # pragma: no cover - neither platform
        yield
        return
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    lock_path = target.with_name(f".{target.name}.lock")
    with open(lock_path, "a+") as lock:
        if fcntl is not None:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        else:  # pragma: no cover - exercised on Windows
            lock.seek(0, os.SEEK_END)
            if lock.tell() == 0:
                lock.write(" ")
                lock.flush()
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            else:  # pragma: no cover - exercised on Windows
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)


def _prune(requests: List[JoinRequest], now: float) -> List[JoinRequest]:
    return [entry for entry in requests if not entry.is_expired(now)]


def _enforce_cap(requests: List[JoinRequest]) -> List[JoinRequest]:
    if len(requests) <= MAX_JOIN_REQUESTS:
        return requests
    return requests[-MAX_JOIN_REQUESTS:]


def record_join_request(
    path: Union[str, Path],
    group_name: str,
    peer: str,
    *,
    now: Optional[float] = None,
) -> JoinRequest:
    """Record (or refresh) a pending join request for *peer*.

    A repeat request from the same peer for the same group refreshes
    the timestamp instead of duplicating the entry.
    """
    moment = time.time() if now is None else now
    group_name = _clean_group_name(group_name)
    peer = _clean_hash(peer, "peer")
    with join_requests_lock(path):
        requests = _prune(load_join_requests(path), moment)
        existing = next(
            (
                entry
                for entry in requests
                if entry.group.casefold() == group_name.casefold()
                and entry.peer == peer
            ),
            None,
        )
        if existing is not None:
            entry = JoinRequest(
                group=group_name,
                peer=peer,
                requested_at=moment,
                expires_at=moment + JOIN_REQUEST_TTL,
            )
            requests = [entry if e is existing else e for e in requests]
        else:
            entry = JoinRequest(
                group=group_name,
                peer=peer,
                requested_at=moment,
                expires_at=moment + JOIN_REQUEST_TTL,
            )
            requests.append(entry)
        save_join_requests(path, _enforce_cap(requests))
    return entry


def list_join_requests(
    path: Optional[Union[str, Path]],
    *,
    group_name: Optional[str] = None,
    now: Optional[float] = None,
) -> List[JoinRequest]:
    """Pending join requests, optionally for one group, oldest first."""
    moment = time.time() if now is None else now
    requests = _prune(load_join_requests(path), moment)
    if group_name is not None:
        wanted = _clean_group_name(group_name).casefold()
        requests = [
            entry for entry in requests if entry.group.casefold() == wanted
        ]
    return sorted(requests, key=lambda entry: entry.requested_at)


def resolve_join_request(
    path: Union[str, Path], group_name: str, peer: str
) -> bool:
    """Drop the pending request from *peer* for *group_name*.

    Returns ``True`` when one was pending.  Approve and deny both
    resolve the request; only approve also changes the roster.
    """
    group_name = _clean_group_name(group_name)
    peer = _clean_hash(peer, "peer")
    with join_requests_lock(path):
        requests = load_join_requests(path)
        kept = [
            entry
            for entry in requests
            if not (
                entry.group.casefold() == group_name.casefold()
                and entry.peer == peer
            )
        ]
        if len(kept) == len(requests):
            return False
        save_join_requests(path, kept)
    return True
