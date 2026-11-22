"""Signed group invite links and pending join requests.

The only way into a tau group today is for the creator to add you
(``tau group add-member``), which makes every group a bottleneck on
one person and real-world onboarding ("join our mesh group")
impossible without the creator present.  Invite links are the fix:
the creator mints a signed token for a group (``tau group invite``)
and shares it as a link; anyone holding it can ask to join.

A token is a signed payload — group name, creator hash, open/closed
mode, a generation counter, and the issue time — carried as
``tau://invite/<base64>``.  The creator's node verifies the
signature against its own identity, so a token can only be minted
by the creator, and a token is only honoured while its generation
matches the stored one: ``tau group invite --revoke`` bumps the
generation and every token minted before it stops working.

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
semantics are unchanged.

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
    return INVITE_LINK_PREFIX + base64.urlsafe_b64encode(encode(token)).decode(
        "ascii"
    )


def parse_invite_link(link: Any) -> Dict[str, Any]:
    """Decode an invite link into its signed payload.

    Returns the token dict (payload plus ``"sig"`` as base64 text).
    The signature itself is not checked here — that needs the
    creator's identity (see :func:`verify_invite_link`).
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
    if token.get("v") != 1:
        raise InviteError("unsupported invite token version")
    # Validate the shape now so a malformed token fails the same way
    # everywhere instead of halfway through verification.  Exactly
    # one of "group" (a name) and "group_id" (an opaque id, for a
    # secret group) names the group — a token carrying both, or
    # neither, is malformed.
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
    """
    token = parse_invite_link(link)
    own_hash = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    if token["creator"] != own_hash:
        raise InviteError("invite was not issued by this node")
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


def load_invites(path: Optional[Union[str, Path]]) -> Dict[str, Any]:
    """Load the invite store at *path*; a missing file means none."""
    if path is None:
        return {"groups": {}}
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return {"groups": {}}
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
    return {"groups": groups}


def save_invites(path: Union[str, Path], invites: Dict[str, Any]) -> Path:
    """Persist *invites* to *path* as JSON (mode 0600)."""
    target = Path(path).expanduser()
    document = {"groups": invites.get("groups", {})}
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
    """
    group_name = _clean_group_name(group_name)
    mode = _clean_mode(mode)
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
    keeps advertising a token that no longer verifies.  Returns the
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
