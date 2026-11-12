"""Wire protocol for tau.

Every tau node announces a single Reticulum destination named
``tau.node.<identity-hash>``.  Small, signed JSON announcements advertise the
node name.  Once a peer is known, messages are exchanged as requests over an
encrypted Reticulum ``Link``:

    /tau/ping     -> liveness and clock information
    /tau/info     -> static node information
    /tau/message  -> deliver a text message (optionally tagged with a
                     kind, default ``chat``), echo it back
    /tau/file     -> offer a small file transfer (signed header first;
                    the bytes follow over an RNS Resource on the same link)
    /tau/peers    -> ask a peer which peers it has heard from; the answer
                    carries each peer's public key, so the receiver can verify
                    the key derives the reported destination hash (peer exchange)

Payloads are JSON encoded as UTF-8 and deliberately size limited: a compromised
or buggy peer must not be able to make us buffer large amounts of data.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import time
import uuid
from typing import Any, Dict, List, Optional

import RNS

from . import __version__

APP_NAME = "tau"
ASPECT = "node"
ASPECT_FILTER = f"{APP_NAME}.{ASPECT}"
PROTOCOL_VERSION = 1

DEFAULT_TCP_PORT = 4343

MAX_NAME_LENGTH = 48
MAX_MESSAGE_CHARS = 2000
# Message kinds (``tau send --kind``): a short lowercase slug tagging what
# a DM is for — ``chat`` is plain conversation, anything else is a typed
# message a receiver can filter on.  The wire field is optional so old
# peers keep working: a payload without a kind is a chat message.
DEFAULT_MESSAGE_KIND = "chat"
MAX_MESSAGE_KIND_LENGTH = 32
# ASCII only: str.isalnum() accepts non-ASCII letters (e.g. "é"), which
# would break the documented slug grammar for consumers that enforce it.
_MESSAGE_KIND_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,31}")
# ``group`` and ``file`` are inbox record types, not message kinds: a
# direct message tagged with either would fail inbox validation (a
# "group" text record needs a roster, a "file" one needs file fields)
# and be silently lost — `_record_received` swallows the failure while
# the sender already got success and the seen-id blocks a retry.  So
# they are reserved here, at the one choke point every kind passes
# through, and the wire parser rejects them the same way.
_RESERVED_MESSAGE_KINDS = frozenset({"group", "file"})
# A reaction is one unicode emoji or a short text token (``+1``, ``lol``) —
# never an image or a URL.  32 characters leaves room for a multi-codepoint
# emoji sequence while keeping the reaction far inside the request budget.
MAX_REACTION_CHARS = 32
MAX_REQUEST_BYTES = 8 * 1024
MAX_RESPONSE_BYTES = 64 * 1024
ANNOUNCE_MAX_BYTES = 256

# Store-and-forward mail: a sealed (encrypted + signed) message blob is
# larger than a plain message — the plaintext envelope plus Reticulum
# encryption overhead, base64 wrapped for JSON transport — so the mail
# paths get a bigger, separate budget.  Every handler still enforces its
# own per-path limit when parsing.
MAX_MAIL_REQUEST_BYTES = 24 * 1024
MAX_MAIL_BLOB_BYTES = 12 * 1024
MAX_MAIL_TTL = 30 * 24 * 60 * 60

# File transfer: one small file per transfer, direct links only (no
# store-and-forward in v1).  The receiver verifies the SHA-256 of the
# assembled bytes against the signed offer, so the cap also bounds how
# much an accepted-but-spoofed advertisement can make a node buffer.
MAX_FILE_BYTES = 10 * 1024 * 1024
MAX_FILE_NAME_LENGTH = 255

REQUEST_PING = "/tau/ping"
REQUEST_INFO = "/tau/info"
REQUEST_MESSAGE = "/tau/message"
REQUEST_FILE = "/tau/file"
REQUEST_READ = "/tau/read"
REQUEST_REACT = "/tau/react"
REQUEST_RETRACT = "/tau/dm/retract"
REQUEST_PEERS = "/tau/peers"
REQUEST_MAIL_STORE = "/tau/mail/store"
REQUEST_MAIL_FETCH = "/tau/mail/fetch"
REQUEST_GROUP_MESSAGE = "/tau/group"
REQUEST_PATHS = (
    REQUEST_PING,
    REQUEST_INFO,
    REQUEST_MESSAGE,
    REQUEST_FILE,
    REQUEST_READ,
    REQUEST_REACT,
    REQUEST_RETRACT,
    REQUEST_PEERS,
    REQUEST_MAIL_STORE,
    REQUEST_MAIL_FETCH,
    REQUEST_GROUP_MESSAGE,
)

# Group DMs: a fixed roster, carried in every group message so a
# recipient that has never seen the group can learn (and pin) it.
MAX_GROUP_NAME_LENGTH = 48
MAX_GROUP_MEMBERS = 32
MIN_GROUP_MEMBERS = 2
# A read receipt names at most this many message ids; a receipt is
# batched, but a compromised peer must not make the sender buffer an
# unbounded list.
MAX_READ_IDS = 100


class ProtocolError(ValueError):
    """Raised when a peer sends a malformed or oversized payload."""


def _clean_text(value: str, allow_newlines: bool = False) -> str:
    """Drop control characters, including terminal escape sequences."""
    allowed_whitespace = "\n\t" if allow_newlines else ""
    return "".join(
        char for char in value if char in allowed_whitespace or char.isprintable()
    )


def sanitize_name(name: Any, fallback: str = "anonymous") -> str:
    """Normalise a human readable node name."""
    if not isinstance(name, str):
        return fallback
    cleaned = " ".join(_clean_text(name).split())
    cleaned = cleaned[:MAX_NAME_LENGTH].strip()
    return cleaned or fallback


def normalize_message(text: Any) -> str:
    """Validate and normalise an outgoing or incoming message body."""
    if not isinstance(text, str):
        raise ProtocolError("message text must be a string")
    cleaned = _clean_text(text, allow_newlines=True).strip()
    if not cleaned:
        raise ProtocolError("message text must not be empty")
    if len(cleaned) > MAX_MESSAGE_CHARS:
        raise ProtocolError(f"message is longer than {MAX_MESSAGE_CHARS} characters")
    return cleaned


def normalize_message_kind(kind: Any) -> str:
    """Validate a message kind slug, defaulting an absent one to chat.

    A kind is a short ASCII lowercase slug (``[a-z0-9][a-z0-9_-]*``,
    at most 32 characters) so it is safe to print, compare and filter
    on.  ``None`` — the local default — means plain chat.  ``group``
    and ``file`` are reserved: they name inbox record types, and a
    direct message carrying either could never be stored.
    """
    if kind is None:
        return DEFAULT_MESSAGE_KIND
    if not isinstance(kind, str):
        raise ProtocolError("message kind must be a string")
    cleaned = kind.strip().lower()
    if _MESSAGE_KIND_RE.fullmatch(cleaned) is None:
        raise ProtocolError(
            "message kind must match [a-z0-9][a-z0-9_-]{0,31}"
        )
    if cleaned in _RESERVED_MESSAGE_KINDS:
        raise ProtocolError(f"message kind {cleaned!r} is reserved")
    return cleaned


def kind_from_wire(payload: Dict[str, Any]) -> str:
    """Kind from a peer payload: absent means chat, present must be valid.

    An explicit ``null`` is malformed, not chat — only a missing key
    gets the default, so a peer cannot smuggle an invalid kind past
    validation by sending ``null``.
    """
    if "kind" not in payload:
        return DEFAULT_MESSAGE_KIND
    kind = payload["kind"]
    if kind is None:
        raise ProtocolError("message kind must be a string")
    return normalize_message_kind(kind)


def encode(obj: Any, *, max_bytes: Optional[int] = None) -> bytes:
    """Encode *obj* as compact UTF-8 JSON, enforcing an optional size limit."""
    try:
        data = json.dumps(
            obj,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ProtocolError(f"could not encode payload: {exc}") from exc
    if max_bytes is not None and len(data) > max_bytes:
        raise ProtocolError(
            f"payload of {len(data)} bytes exceeds the {max_bytes} byte limit"
        )
    return data


def decode(raw: Any, *, max_bytes: int = MAX_RESPONSE_BYTES) -> Dict[str, Any]:
    """Decode a JSON object received from a peer."""
    if isinstance(raw, dict):
        # Reticulum's request API may hand back an already unpacked object.
        return raw
    if isinstance(raw, str):
        data = raw.encode("utf-8")
    elif isinstance(raw, (bytes, bytearray, memoryview)):
        data = bytes(raw)
    else:
        raise ProtocolError(f"expected bytes, got {type(raw).__name__}")
    if len(data) > max_bytes:
        raise ProtocolError(
            f"payload of {len(data)} bytes exceeds the {max_bytes} byte limit"
        )
    try:
        obj = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ProtocolError(f"invalid JSON payload: {exc}") from exc
    if not isinstance(obj, dict):
        raise ProtocolError("payload must be a JSON object")
    return obj


def build_announce(name: str) -> bytes:
    """Build the compact app_data advertised in a Reticulum announce."""
    payload = {
        "v": PROTOCOL_VERSION,
        "name": sanitize_name(name),
        "proto": APP_NAME,
        "caps": ["message", "ping", "peers", "mail", "file", "read"],
    }
    data = encode(payload)
    # Reticulum announces have a very small payload budget.  Shorten the name
    # rather than dropping the announcement entirely.
    while len(data) > ANNOUNCE_MAX_BYTES and len(payload["name"]) > 1:
        payload["name"] = payload["name"][:-1]
        data = encode(payload)
    if len(data) > ANNOUNCE_MAX_BYTES:  # pragma: no cover - defensive
        raise ProtocolError("announce payload exceeds the Reticulum budget")
    return data


def parse_announce(app_data: Any) -> Dict[str, Any]:
    """Parse announce app_data, tolerating older or foreign payloads."""
    if not app_data:
        return {
            "v": 0,
            "name": "unknown",
            "proto": None,
            "caps": [],
        }
    try:
        payload = decode(app_data, max_bytes=ANNOUNCE_MAX_BYTES)
    except ProtocolError:
        return {"v": 0, "name": "unknown", "proto": None, "caps": []}

    caps = payload.get("caps")
    if not isinstance(caps, list):
        caps = []
    return {
        "v": payload.get("v") if isinstance(payload.get("v"), int) else 0,
        "name": sanitize_name(payload.get("name")),
        "proto": payload.get("proto")
        if isinstance(payload.get("proto"), str)
        else None,
        "caps": [cap for cap in caps if isinstance(cap, str)][:16],
    }


def _clean_expires_at(value: Any) -> Optional[float]:
    """Validate an optional message expiry timestamp.

    ``None`` (absent) means the message never expires.  A present value
    must be a finite number; anything else is a malformed payload.
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProtocolError("expires_at must be a finite number")
    try:
        expires_at = float(value)
    except (OverflowError, ValueError) as exc:
        raise ProtocolError("expires_at must be a finite number") from exc
    if not math.isfinite(expires_at):
        raise ProtocolError("expires_at must be a finite number")
    return expires_at


def generate_message_id() -> str:
    """Return a fresh random message identifier (32 hex characters)."""
    return uuid.uuid4().hex


def generate_group_id() -> str:
    """Return a fresh random group identifier (32 hex characters)."""
    return uuid.uuid4().hex


def derive_group_id(creator: str, name: str, members: List[str]) -> str:
    """Derive a group's id from its creator, name and roster.

    The id commits to the roster: a message carrying a different roster
    for the same id cannot verify, so a member cannot fork a group id
    onto a recipient that has not seen it yet — the recipient pins the
    roster that matches the id, and only that roster.  Creating a group
    with a fresh id and an arbitrary roster is still possible, but that
    is unsolicited group creation, gated by the DM allow-list exactly
    like a direct message from a stranger.
    """
    canonical = "\n".join(
        [
            _clean_hexhash(creator, "creator"),
            sanitize_group_name(name),
            *[member.lower() for member in sorted(_clean_member_list(members))],
        ]
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def sanitize_group_name(name: Any, fallback: str = "group") -> str:
    """Normalise a human readable group name."""
    if not isinstance(name, str):
        return fallback
    cleaned = " ".join(_clean_text(name).split())
    cleaned = cleaned[:MAX_GROUP_NAME_LENGTH].strip()
    return cleaned or fallback


def _clean_member_list(value: Any) -> List[str]:
    """Validate a group roster: 2-32 unique 32-hex destination hashes."""
    if not isinstance(value, list):
        raise ProtocolError("members must be a list of destination hashes")
    members: List[str] = []
    for member in value:
        members.append(_clean_hexhash(member, "member"))
    if len(members) < MIN_GROUP_MEMBERS:
        raise ProtocolError(
            f"a group needs at least {MIN_GROUP_MEMBERS} members"
        )
    if len(members) > MAX_GROUP_MEMBERS:
        raise ProtocolError(
            f"a group has at most {MAX_GROUP_MEMBERS} members"
        )
    if len(set(members)) != len(members):
        raise ProtocolError("group members must be unique")
    return members


def encode_group_message(
    text: str,
    *,
    message_id: Optional[str] = None,
    group_id: str,
    group_name: str,
    creator: str,
    members: List[str],
    sent_at: Optional[float] = None,
    expires_at: Optional[float] = None,
) -> bytes:
    """Encode a group chat message for the ``/tau/group`` request.

    The roster rides in every message, signed by the link identity of
    the sender (the same authentication as a direct message), so a
    recipient that has never seen the group can learn and pin it.  The
    group id is derived from the creator, name and roster (see
    :func:`derive_group_id`), so the roster a message carries must match
    the id it claims.

    ``expires_at`` is an optional sender-set expiry (epoch seconds),
    exactly like a direct message: the receiver shows the deadline and
    purges the message once it passes.
    """
    payload = {
        "type": "group_message",
        "v": PROTOCOL_VERSION,
        "text": normalize_message(text),
        "sent_at": sent_at if isinstance(sent_at, (int, float)) else time.time(),
        "client": f"tau/{__version__}",
        "group": _clean_hexhash(group_id, "group"),
        "group_name": sanitize_group_name(group_name),
        "creator": _clean_hexhash(creator, "creator"),
        "members": _clean_member_list(members),
    }
    if message_id:
        payload["id"] = message_id
    cleaned_expires_at = _clean_expires_at(expires_at)
    if cleaned_expires_at is not None:
        payload["expires_at"] = cleaned_expires_at
    return encode(payload, max_bytes=MAX_REQUEST_BYTES)


def parse_group_message(raw: Any) -> Dict[str, Any]:
    """Validate an incoming ``/tau/group`` request body."""
    payload = decode(raw, max_bytes=MAX_REQUEST_BYTES)
    if payload.get("type") != "group_message":
        raise ProtocolError("expected a group_message payload")
    text = normalize_message(payload.get("text"))
    sent_at = payload.get("sent_at")
    if not isinstance(sent_at, (int, float)):
        sent_at = None
    message_id = payload.get("id")
    if not isinstance(message_id, str) or not message_id or len(message_id) > 64:
        message_id = None
    group = _clean_hexhash(payload.get("group"), "group")
    group_name = sanitize_group_name(payload.get("group_name"))
    creator = _clean_hexhash(payload.get("creator"), "creator")
    members = _clean_member_list(payload.get("members"))
    if creator not in members:
        raise ProtocolError("group creator must be a member of the group")
    if derive_group_id(creator, group_name, members) != group:
        raise ProtocolError("group id does not match its creator and roster")
    return {
        "text": text,
        "sent_at": sent_at,
        "id": message_id,
        "group": group,
        "group_name": group_name,
        "creator": creator,
        "members": members,
        "expires_at": _clean_expires_at(payload.get("expires_at")),
    }


def clean_reaction(value: Any) -> str:
    """Validate a reaction token, or ``""`` when it retracts one.

    A reaction is a unicode emoji or a short text token: printable
    characters only, at most :data:`MAX_REACTION_CHARS` long.  An empty
    (or whitespace-only) value is a retraction, not an error — ``tau
    react <id> --clear`` sends exactly that.  Anything else raises
    :class:`ProtocolError`.
    """
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ProtocolError("reaction must be a string")
    cleaned = _clean_text(value).strip()
    if len(cleaned) > MAX_REACTION_CHARS:
        raise ProtocolError(
            f"reaction is longer than {MAX_REACTION_CHARS} characters"
        )
    return cleaned


def reaction_dict(
    target: str,
    reaction: str,
    *,
    message_id: Optional[str] = None,
    sent_at: Optional[float] = None,
    signature: Optional[str] = None,
    group: Optional[str] = None,
    target_author: Optional[str] = None,
    target_sent_at: Optional[float] = None,
) -> Dict[str, Any]:
    """The ``/tau/react`` payload as a dict, unsigned unless signed.

    The signature — when present — covers every other field, exactly
    like a sealed mail envelope: the sender signs the canonical
    encoding of the payload without ``"sig"`` and attaches the base64
    signature, so the receiver can verify attribution against the link
    identity independently of the transport.

    ``group`` / ``target_author`` / ``target_sent_at`` name the group
    message being reacted to: group messages carry a different wire id
    per recipient leg, so the target id alone only matches the reactor's
    own copy and the author's sent record.  A receiver that does not
    hold the target id can resolve its own copy from this context
    instead (see :func:`tau.node.TauNode._handle_reaction`).
    """
    target = _clean_message_id(target)
    payload: Dict[str, Any] = {
        "type": "reaction",
        "v": PROTOCOL_VERSION,
        "target": target,
        "reaction": clean_reaction(reaction),
        "sent_at": sent_at if isinstance(sent_at, (int, float)) else time.time(),
        "client": f"tau/{__version__}",
    }
    if message_id:
        payload["id"] = message_id
    if group is not None:
        payload["group"] = _clean_hexhash(group, "group")
    if target_author is not None:
        payload["target_author"] = _clean_hexhash(target_author, "target_author")
    if target_sent_at is not None:
        if not isinstance(target_sent_at, (int, float)) or isinstance(
            target_sent_at, bool
        ):
            raise ProtocolError("target_sent_at must be a number")
        payload["target_sent_at"] = target_sent_at
    if signature is not None:
        payload["sig"] = signature
    return payload


def encode_reaction(
    target: str,
    reaction: str,
    *,
    message_id: Optional[str] = None,
    sent_at: Optional[float] = None,
    signature: Optional[str] = None,
) -> bytes:
    """Encode a DM reaction for the ``/tau/react`` request.

    ``target`` names the reacted-to message id; ``reaction`` is the token
    from :func:`clean_reaction` (``""`` retracts the reactor's reaction).
    ``signature`` is the base64 signature over the canonical unsigned
    payload; see :func:`reaction_dict`.
    """
    return encode(
        reaction_dict(
            target,
            reaction,
            message_id=message_id,
            sent_at=sent_at,
            signature=signature,
        ),
        max_bytes=MAX_REQUEST_BYTES,
    )


def parse_reaction(raw: Any) -> Dict[str, Any]:
    """Validate an incoming ``/tau/react`` request body."""
    payload = decode(raw, max_bytes=MAX_REQUEST_BYTES)
    if payload.get("type") != "reaction":
        raise ProtocolError("expected a reaction payload")
    target = _clean_message_id(payload.get("target"))
    if "reaction" not in payload:
        raise ProtocolError("reaction payload is missing 'reaction'")
    reaction = clean_reaction(payload.get("reaction"))
    sent_at = payload.get("sent_at")
    if not isinstance(sent_at, (int, float)):
        sent_at = None
    message_id = payload.get("id")
    if not isinstance(message_id, str) or not message_id or len(message_id) > 64:
        message_id = None
    signature = payload.get("sig")
    if signature is not None:
        if not isinstance(signature, str) or not signature:
            raise ProtocolError("reaction signature must be a base64 string")
        try:
            base64.b64decode(signature, validate=True)
        except ValueError as exc:
            raise ProtocolError(
                "reaction signature is not valid base64"
            ) from exc
    # Group context is advisory: it lets a receiver resolve its own
    # copy of a group message when the target id is not one it holds.
    # Malformed context is dropped, never a reason to reject the
    # reaction — the target id alone is still a valid reaction.
    group = payload.get("group")
    if not isinstance(group, str):
        group = None
    else:
        try:
            group = _clean_hexhash(group, "group")
        except ProtocolError:
            group = None
    target_author = payload.get("target_author")
    if not isinstance(target_author, str):
        target_author = None
    else:
        try:
            target_author = _clean_hexhash(target_author, "target_author")
        except ProtocolError:
            target_author = None
    target_sent_at = payload.get("target_sent_at")
    if not isinstance(target_sent_at, (int, float)) or isinstance(
        target_sent_at, bool
    ):
        target_sent_at = None
    return {
        "target": target,
        "reaction": reaction,
        "sent_at": sent_at,
        "id": message_id,
        "sig": signature,
        "group": group,
        "target_author": target_author,
        "target_sent_at": target_sent_at,
    }


def reaction_unsigned_bytes(raw: Any) -> bytes:
    """Canonical bytes of a reaction payload without its signature.

    Used to verify the signature: the signer signs exactly these bytes,
    and the deterministic encoder reproduces them from the received
    payload regardless of key order on the wire.
    """
    payload = decode(raw, max_bytes=MAX_REQUEST_BYTES)
    unsigned = {key: value for key, value in payload.items() if key != "sig"}
    return encode(unsigned, max_bytes=MAX_REQUEST_BYTES)


def retract_dict(
    target: str,
    *,
    message_id: Optional[str] = None,
    sent_at: Optional[float] = None,
    signature: Optional[str] = None,
) -> Dict[str, Any]:
    """The ``/tau/dm/retract`` payload as a dict, unsigned unless signed.

    The signature — when present — covers every other field, exactly
    like a reaction: the sender signs the canonical encoding of the
    payload without ``"sig"`` and attaches the base64 signature, so the
    receiver can verify attribution against the link identity
    independently of the transport.  A retract names only the target
    message id; the receiver checks that the caller is that message's
    original sender before deleting anything.
    """
    target = _clean_message_id(target)
    payload: Dict[str, Any] = {
        "type": "retraction",
        "v": PROTOCOL_VERSION,
        "target": target,
        "sent_at": sent_at if isinstance(sent_at, (int, float)) else time.time(),
        "client": f"tau/{__version__}",
    }
    if message_id:
        payload["id"] = message_id
    if signature is not None:
        payload["sig"] = signature
    return payload


def encode_retract(
    target: str,
    *,
    message_id: Optional[str] = None,
    sent_at: Optional[float] = None,
    signature: Optional[str] = None,
) -> bytes:
    """Encode a DM retract for the ``/tau/dm/retract`` request.

    ``target`` names the retracted message id.  ``signature`` is the
    base64 signature over the canonical unsigned payload; see
    :func:`retract_dict`.
    """
    return encode(
        retract_dict(
            target,
            message_id=message_id,
            sent_at=sent_at,
            signature=signature,
        ),
        max_bytes=MAX_REQUEST_BYTES,
    )


def parse_retract(raw: Any) -> Dict[str, Any]:
    """Validate an incoming ``/tau/dm/retract`` request body."""
    payload = decode(raw, max_bytes=MAX_REQUEST_BYTES)
    if payload.get("type") != "retraction":
        raise ProtocolError("expected a retraction payload")
    target = _clean_message_id(payload.get("target"))
    sent_at = payload.get("sent_at")
    if not isinstance(sent_at, (int, float)):
        sent_at = None
    message_id = payload.get("id")
    if not isinstance(message_id, str) or not message_id or len(message_id) > 64:
        message_id = None
    signature = payload.get("sig")
    if signature is not None:
        if not isinstance(signature, str) or not signature:
            raise ProtocolError("retraction signature must be a base64 string")
        try:
            base64.b64decode(signature, validate=True)
        except ValueError as exc:
            raise ProtocolError(
                "retraction signature is not valid base64"
            ) from exc
    return {
        "target": target,
        "sent_at": sent_at,
        "id": message_id,
        "sig": signature,
    }


def retract_unsigned_bytes(raw: Any) -> bytes:
    """Canonical bytes of a retract payload without its signature.

    Used to verify the signature: the signer signs exactly these bytes,
    and the deterministic encoder reproduces them from the received
    payload regardless of key order on the wire.
    """
    payload = decode(raw, max_bytes=MAX_REQUEST_BYTES)
    unsigned = {key: value for key, value in payload.items() if key != "sig"}
    return encode(unsigned, max_bytes=MAX_REQUEST_BYTES)
def excerpt_hash(text: str) -> str:
    """Short hash of a quoted message's text (first 16 hex of SHA-256).

    A reply carries this alongside the quoted message id so the
    receiver can check the copy it renders against what the sender
    quoted, without trusting any excerpt text on the wire.
    """
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def clean_in_reply_to(value: Any) -> Optional[Dict[str, Any]]:
    """Validate an ``in_reply_to`` reference, or ``None`` when absent.

    The reference names a message id and, optionally, the excerpt hash
    of the text the sender quoted.  A malformed reference is a protocol
    error — the sender is either buggy or trying to smuggle junk past
    the size budget — while a missing one simply means a standalone
    message.
    """
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ProtocolError("in_reply_to must be an object")
    message_id = value.get("id")
    if not isinstance(message_id, str) or not message_id or len(message_id) > 64:
        raise ProtocolError("in_reply_to id must be a string of 1-64 characters")
    excerpt = value.get("excerpt")
    if excerpt is not None:
        if (
            not isinstance(excerpt, str)
            or not excerpt
            or len(excerpt) > 32
            or any(char not in "0123456789abcdef" for char in excerpt.lower())
        ):
            raise ProtocolError("in_reply_to excerpt must be a short hex hash")
        excerpt = excerpt.lower()
    return {"id": message_id, "excerpt": excerpt}
def encode_message(
    text: str,
    *,
    message_id: Optional[str] = None,
    sent_at: Optional[float] = None,
    expires_at: Optional[float] = None,
    in_reply_to: Optional[Dict[str, Any]] = None,
    kind: Optional[str] = None,
) -> bytes:
    """Encode a chat message for the ``/tau/message`` request.

    ``expires_at`` is an optional sender-set expiry (epoch seconds):
    the receiver shows the deadline and purges the message once it
    passes.  It is advisory — a modified client can keep copies — but
    it travels inside the encrypted payload (and inside the signed
    sealed envelope for relayed mail), so a relay cannot extend it.
    ``in_reply_to`` optionally names the earlier message this one
    quotes.
    """
    payload = {
        "type": "message",
        "v": PROTOCOL_VERSION,
        "text": normalize_message(text),
        "kind": normalize_message_kind(kind),
        "sent_at": sent_at if isinstance(sent_at, (int, float)) else time.time(),
        "client": f"tau/{__version__}",
    }
    if message_id:
        payload["id"] = message_id
    cleaned_expires_at = _clean_expires_at(expires_at)
    if cleaned_expires_at is not None:
        payload["expires_at"] = cleaned_expires_at
    reply_to = clean_in_reply_to(in_reply_to)
    if reply_to is not None:
        payload["in_reply_to"] = reply_to
    return encode(payload, max_bytes=MAX_REQUEST_BYTES)


def parse_message(raw: Any) -> Dict[str, Any]:
    """Validate an incoming ``/tau/message`` request body."""
    payload = decode(raw, max_bytes=MAX_REQUEST_BYTES)
    if payload.get("type") != "message":
        raise ProtocolError("expected a message payload")
    text = normalize_message(payload.get("text"))
    sent_at = payload.get("sent_at")
    if not isinstance(sent_at, (int, float)):
        sent_at = None
    message_id = payload.get("id")
    if not isinstance(message_id, str) or not message_id or len(message_id) > 64:
        message_id = None
    return {
        "text": text,
        "kind": kind_from_wire(payload),
        "sent_at": sent_at,
        "id": message_id,
        "expires_at": _clean_expires_at(payload.get("expires_at")),
        "in_reply_to": clean_in_reply_to(payload.get("in_reply_to")),
    }


def build_read_receipt(
    reader_identity: Any,
    *,
    message_ids: List[str],
    read_at: Optional[float] = None,
) -> bytes:
    """Encode a signed read receipt for the ``/tau/read`` request.

    A receipt names the message ids its sender has *seen* — one
    receipt can cover many ids, and receipts are never receipted
    themselves.  The payload carries the reader's public key and a
    signature over every other field, so the sender can authenticate
    the receipt without trusting the link alone: the same signed
    payload rides the link when the sender is online and, sealed,
    the mailbox when it is not.  Mirrors the sealed-message signing
    in :mod:`tau.mailbox`.
    """
    if not message_ids:
        raise ProtocolError("read receipt must name at least one message")
    if len(message_ids) > MAX_READ_IDS:
        raise ProtocolError(
            f"read receipt names more than {MAX_READ_IDS} messages"
        )
    ids = []
    for message_id in message_ids:
        cleaned = _clean_message_id(message_id)
        if cleaned not in ids:
            ids.append(cleaned)
    if not ids:
        raise ProtocolError("read receipt must name at least one message")
    if len(ids) > MAX_READ_IDS:
        raise ProtocolError(
            f"read receipt names more than {MAX_READ_IDS} messages"
        )
    timestamp = read_at if isinstance(read_at, (int, float)) else time.time()
    if not math.isfinite(timestamp):
        raise ProtocolError("read_at must be a finite number")
    reader_hash = RNS.Destination.hash(reader_identity, APP_NAME, ASPECT).hex()
    payload: Dict[str, Any] = {
        "type": "read_receipt",
        "v": PROTOCOL_VERSION,
        "ids": ids,
        "read_at": float(timestamp),
        "from": reader_hash,
        "from_key": reader_identity.get_public_key().hex(),
    }
    signature = reader_identity.sign(encode(payload))
    if signature is None:  # pragma: no cover - identity without private keys
        raise ProtocolError("reader identity cannot sign")
    payload["sig"] = base64.b64encode(signature).decode("ascii")
    return encode(payload, max_bytes=MAX_REQUEST_BYTES)


def parse_read_receipt(
    raw: Any, *, remote_identity: Optional[Any] = None
) -> Dict[str, Any]:
    """Validate a signed ``/tau/read`` request body.

    Verifies the signature against the carried public key, checks the
    key derives the claimed reader hash, and — when the link supplied
    an identity — checks that identity is the claimed reader too.  A
    receipt from anyone else, or for ids that fail validation, is
    rejected outright; whether the ids name messages this node actually
    sent is the caller's check (see ``Mailbox.record_read``).
    """
    payload = decode(raw, max_bytes=MAX_REQUEST_BYTES)
    if payload.get("type") != "read_receipt":
        raise ProtocolError("expected a read_receipt payload")
    ids = payload.get("ids")
    if not isinstance(ids, list) or not ids:
        raise ProtocolError("read receipt must name at least one message")
    if len(ids) > MAX_READ_IDS:
        raise ProtocolError(
            f"read receipt names more than {MAX_READ_IDS} messages"
        )
    cleaned_ids = []
    for message_id in ids:
        cleaned = _clean_message_id(message_id)
        if cleaned not in cleaned_ids:
            cleaned_ids.append(cleaned)
    read_at = payload.get("read_at")
    if isinstance(read_at, bool) or not isinstance(read_at, (int, float)):
        raise ProtocolError("read_at must be a finite number")
    if not math.isfinite(float(read_at)):
        raise ProtocolError("read_at must be a finite number")

    signature_text = payload.get("sig")
    if not isinstance(signature_text, str):
        raise ProtocolError("read receipt is missing its signature")
    try:
        signature = base64.b64decode(signature_text, validate=True)
    except ValueError as exc:
        raise ProtocolError("read receipt signature is not valid base64") from exc
    from_key = payload.get("from_key")
    if not isinstance(from_key, str):
        raise ProtocolError("read receipt is missing its reader key")
    try:
        reader_key = bytes.fromhex(from_key)
    except ValueError as exc:
        raise ProtocolError("read receipt reader key is not hex") from exc
    if len(reader_key) != 64:
        raise ProtocolError("read receipt reader key has the wrong length")
    reader = RNS.Identity(create_keys=False)
    if (
        reader.load_public_key(reader_key) is False
        or reader.hash is None
    ):
        raise ProtocolError("could not load the reader public key")
    claimed_from = payload.get("from")
    if (
        not isinstance(claimed_from, str)
        or RNS.Destination.hash(reader, APP_NAME, ASPECT).hex()
        != claimed_from.lower()
    ):
        raise ProtocolError("reader key does not match the claimed reader")
    if remote_identity is not None:
        if (
            RNS.Destination.hash(remote_identity, APP_NAME, ASPECT).hex()
            != claimed_from.lower()
        ):
            raise ProtocolError("read receipt reader does not match the link")
    unsigned = {k: v for k, v in payload.items() if k != "sig"}
    try:
        verified = reader.validate(signature, encode(unsigned))
    except Exception as exc:
        raise ProtocolError(f"could not verify read receipt: {exc}") from exc
    if not verified:
        raise ProtocolError("read receipt signature does not verify")
    return {
        "ids": cleaned_ids,
        "read_at": float(read_at),
        "from": claimed_from.lower(),
    }


def sanitize_file_name(name: Any) -> str:
    """Reduce *name* to a safe bare file name, or ``""`` when unusable.

    Only the final path component survives — a sender cannot smuggle
    in ``../`` or an absolute path — control characters are dropped
    and the result is capped at :data:`MAX_FILE_NAME_LENGTH`.
    """
    if not isinstance(name, str):
        return ""
    cleaned = "".join(char for char in name if char.isprintable())
    cleaned = cleaned.replace("\\", "/").split("/")[-1].strip()
    if cleaned in ("", ".", ".."):
        return ""
    return cleaned[:MAX_FILE_NAME_LENGTH]


def build_file_offer(
    sender_identity: Any,
    *,
    transfer_id: str,
    to: str,
    name: str,
    size: int,
    sha256: str,
    sent_at: Optional[float] = None,
) -> bytes:
    """Encode a signed file offer for the ``/tau/file`` request.

    The offer is the receiver's chance to refuse before any bytes
    move: it names the file, its size and its SHA-256, and carries the
    sender's public key plus a signature over every other field, so
    the receiver can authenticate the offer without trusting the link
    alone.  It also names its intended recipient (``to``): a signed
    offer captured by one peer cannot be replayed to another, because
    the receiver only honors offers addressed to itself.  The transfer
    itself follows as an RNS Resource whose metadata is the transfer
    id, and the receiver re-checks the hash once the bytes are
    assembled.
    """
    clean_name = sanitize_file_name(name)
    if not clean_name:
        raise ProtocolError("file name must not be empty")
    if not isinstance(size, int) or isinstance(size, bool):
        raise ProtocolError("file size must be an integer")
    if size < 1 or size > MAX_FILE_BYTES:
        raise ProtocolError(
            f"file of {size} bytes exceeds the {MAX_FILE_BYTES} byte limit"
        )
    if not isinstance(sha256, str) or len(sha256) != 64:
        raise ProtocolError("sha256 must be a 64 character hex digest")
    try:
        bytes.fromhex(sha256)
    except ValueError as exc:
        raise ProtocolError("sha256 must be a 64 character hex digest") from exc
    sender_hash = RNS.Destination.hash(sender_identity, APP_NAME, ASPECT).hex()
    payload: Dict[str, Any] = {
        "type": "file_offer",
        "v": PROTOCOL_VERSION,
        "id": _clean_message_id(transfer_id),
        "to": _clean_hexhash(to, "to"),
        "name": clean_name,
        "size": size,
        "sha256": sha256.lower(),
        "sent_at": sent_at if isinstance(sent_at, (int, float)) else time.time(),
        "from": sender_hash,
        "from_key": sender_identity.get_public_key().hex(),
    }
    signature = sender_identity.sign(encode(payload))
    if signature is None:  # pragma: no cover - identity without private keys
        raise ProtocolError("sender identity cannot sign")
    payload["sig"] = base64.b64encode(signature).decode("ascii")
    return encode(payload, max_bytes=MAX_REQUEST_BYTES)


def parse_file_offer(
    raw: Any,
    *,
    remote_identity: Optional[Any] = None,
    expected_recipient: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate a signed ``/tau/file`` request body.

    Verifies the signature against the carried public key, checks the
    key derives the claimed sender hash, and — when the link supplied
    an identity — checks that identity is the claimed sender too.  The
    offer must also name its intended recipient: when
    ``expected_recipient`` is given, an offer addressed to anyone
    else is rejected, so a signed offer captured by one peer cannot
    be replayed to another.  A forged, oversized, badly named or
    misaddressed offer is rejected outright; the receiver still
    re-checks the assembled bytes against ``sha256`` after the
    transfer.
    """
    payload = decode(raw, max_bytes=MAX_REQUEST_BYTES)
    if payload.get("type") != "file_offer":
        raise ProtocolError("expected a file_offer payload")
    transfer_id = _clean_message_id(payload.get("id"))
    to = _clean_hexhash(payload.get("to"), "to")
    if expected_recipient is not None and to != expected_recipient.lower():
        raise ProtocolError("file offer is addressed to another node")
    name = payload.get("name")
    if not isinstance(name, str) or sanitize_file_name(name) != name:
        raise ProtocolError("file name is not a safe bare file name")
    size = payload.get("size")
    if not isinstance(size, int) or isinstance(size, bool):
        raise ProtocolError("file size must be an integer")
    if size < 1 or size > MAX_FILE_BYTES:
        raise ProtocolError(
            f"file of {size} bytes exceeds the {MAX_FILE_BYTES} byte limit"
        )
    sha256 = payload.get("sha256")
    if not isinstance(sha256, str) or len(sha256) != 64:
        raise ProtocolError("sha256 must be a 64 character hex digest")
    try:
        bytes.fromhex(sha256)
    except ValueError as exc:
        raise ProtocolError("sha256 must be a 64 character hex digest") from exc
    sent_at = payload.get("sent_at")
    if not isinstance(sent_at, (int, float)):
        sent_at = None

    signature_text = payload.get("sig")
    if not isinstance(signature_text, str):
        raise ProtocolError("file offer is missing its signature")
    try:
        signature = base64.b64decode(signature_text, validate=True)
    except ValueError as exc:
        raise ProtocolError("file offer signature is not valid base64") from exc
    from_key = payload.get("from_key")
    if not isinstance(from_key, str):
        raise ProtocolError("file offer is missing its sender key")
    try:
        sender_key = bytes.fromhex(from_key)
    except ValueError as exc:
        raise ProtocolError("file offer sender key is not hex") from exc
    if len(sender_key) != 64:
        raise ProtocolError("file offer sender key has the wrong length")
    sender = RNS.Identity(create_keys=False)
    if sender.load_public_key(sender_key) is False or sender.hash is None:
        raise ProtocolError("could not load the sender public key")
    claimed_from = payload.get("from")
    if (
        not isinstance(claimed_from, str)
        or RNS.Destination.hash(sender, APP_NAME, ASPECT).hex()
        != claimed_from.lower()
    ):
        raise ProtocolError("sender key does not match the claimed sender")
    if remote_identity is not None:
        if (
            RNS.Destination.hash(remote_identity, APP_NAME, ASPECT).hex()
            != claimed_from.lower()
        ):
            raise ProtocolError("file offer sender does not match the link")
    unsigned = {k: v for k, v in payload.items() if k != "sig"}
    try:
        verified = sender.validate(signature, encode(unsigned))
    except Exception as exc:
        raise ProtocolError(f"could not verify file offer: {exc}") from exc
    if not verified:
        raise ProtocolError("file offer signature does not verify")
    return {
        "id": transfer_id,
        "to": to,
        "name": name,
        "size": size,
        "sha256": sha256.lower(),
        "sent_at": sent_at,
        "from": claimed_from.lower(),
    }


def _clean_hexhash(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) != 32:
        raise ProtocolError(f"{field} must be a 32 character hex hash")
    try:
        bytes.fromhex(value)
    except ValueError as exc:
        raise ProtocolError(f"{field} must be a 32 character hex hash") from exc
    return value.lower()


def _clean_message_id(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > 64:
        raise ProtocolError("message id must be a string of 1-64 characters")
    return value


def build_mail_store(
    *, to: str, message_id: str, blob: bytes, expires_at: float
) -> bytes:
    """Encode a ``/tau/mail/store`` request: hold *blob* for a peer."""
    payload = {
        "type": "mail_store",
        "v": PROTOCOL_VERSION,
        "to": _clean_hexhash(to, "to"),
        "id": _clean_message_id(message_id),
        "blob": base64.b64encode(blob).decode("ascii"),
        "expires_at": float(expires_at),
    }
    return encode(payload, max_bytes=MAX_MAIL_REQUEST_BYTES)


def parse_mail_store(raw: Any) -> Dict[str, Any]:
    """Validate an incoming ``/tau/mail/store`` request body."""
    payload = decode(raw, max_bytes=MAX_MAIL_REQUEST_BYTES)
    if payload.get("type") != "mail_store":
        raise ProtocolError("expected a mail_store payload")
    to = _clean_hexhash(payload.get("to"), "to")
    message_id = _clean_message_id(payload.get("id"))
    blob_text = payload.get("blob")
    if not isinstance(blob_text, str) or not blob_text:
        raise ProtocolError("blob must be a base64 string")
    if len(blob_text) > 4 * (MAX_MAIL_BLOB_BYTES // 3 + 2):
        raise ProtocolError("blob is too large")
    try:
        blob = base64.b64decode(blob_text, validate=True)
    except ValueError as exc:
        raise ProtocolError("blob is not valid base64") from exc
    if len(blob) > MAX_MAIL_BLOB_BYTES:
        raise ProtocolError(
            f"blob of {len(blob)} bytes exceeds the {MAX_MAIL_BLOB_BYTES} byte limit"
        )
    expires_at = payload.get("expires_at")
    if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)):
        raise ProtocolError("expires_at must be a finite number")
    try:
        expires_at = float(expires_at)
    except OverflowError as exc:
        raise ProtocolError("expires_at must be a finite number") from exc
    if not math.isfinite(expires_at):
        raise ProtocolError("expires_at must be a finite number")
    return {
        "to": to,
        "id": message_id,
        "blob": blob,
        "expires_at": expires_at,
    }


def build_mail_fetch(ack_ids: List[str] = ()) -> bytes:
    """Encode a ``/tau/mail/fetch`` request, acking delivered mail ids."""
    payload = {
        "type": "mail_fetch",
        "v": PROTOCOL_VERSION,
        "ack": [_clean_message_id(mid) for mid in list(ack_ids)[:100]],
    }
    return encode(payload, max_bytes=MAX_REQUEST_BYTES)


def parse_mail_fetch(raw: Any) -> Dict[str, Any]:
    """Validate an incoming ``/tau/mail/fetch`` request body."""
    payload = decode(raw, max_bytes=MAX_REQUEST_BYTES)
    if payload.get("type") != "mail_fetch":
        raise ProtocolError("expected a mail_fetch payload")
    ack = payload.get("ack")
    if not isinstance(ack, list):
        ack = []
    cleaned = [
        mid for mid in ack if isinstance(mid, str) and 0 < len(mid) <= 64
    ][:100]
    return {"ack": cleaned}


def response_ok(**fields: Any) -> bytes:
    payload: Dict[str, Any] = {"ok": True, "v": PROTOCOL_VERSION}
    payload.update(fields)
    return encode(payload, max_bytes=MAX_RESPONSE_BYTES)


def response_error(code: str, message: str) -> bytes:
    return encode(
        {
            "ok": False,
            "v": PROTOCOL_VERSION,
            "error": {"code": code, "message": message},
        },
        max_bytes=MAX_RESPONSE_BYTES,
    )


def short_hash(value: str, length: int = 8) -> str:
    """Return a display friendly prefix of a hex hash."""
    return value[:length] if value else ""
