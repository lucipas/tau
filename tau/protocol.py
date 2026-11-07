"""Wire protocol for tau.

Every tau node announces a single Reticulum destination named
``tau.node.<identity-hash>``.  Small, signed JSON announcements advertise the
node name.  Once a peer is known, messages are exchanged as requests over an
encrypted Reticulum ``Link``:

    /tau/ping     -> liveness and clock information
    /tau/info     -> static node information
    /tau/message  -> deliver a text message, echo it back
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
import json
import math
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
REQUEST_PEERS = "/tau/peers"
REQUEST_MAIL_STORE = "/tau/mail/store"
REQUEST_MAIL_FETCH = "/tau/mail/fetch"
REQUEST_PATHS = (
    REQUEST_PING,
    REQUEST_INFO,
    REQUEST_MESSAGE,
    REQUEST_FILE,
    REQUEST_PEERS,
    REQUEST_MAIL_STORE,
    REQUEST_MAIL_FETCH,
)


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
        "caps": ["message", "ping", "peers", "mail", "file"],
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


def generate_message_id() -> str:
    """Return a fresh random message identifier (32 hex characters)."""
    return uuid.uuid4().hex


def encode_message(
    text: str,
    *,
    message_id: Optional[str] = None,
    sent_at: Optional[float] = None,
) -> bytes:
    """Encode a chat message for the ``/tau/message`` request."""
    payload = {
        "type": "message",
        "v": PROTOCOL_VERSION,
        "text": normalize_message(text),
        "sent_at": sent_at if isinstance(sent_at, (int, float)) else time.time(),
        "client": f"tau/{__version__}",
    }
    if message_id:
        payload["id"] = message_id
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
    return {"text": text, "sent_at": sent_at, "id": message_id}


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
    alone.  The transfer itself follows as an RNS Resource whose
    metadata is the transfer id, and the receiver re-checks the hash
    once the bytes are assembled.
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
    raw: Any, *, remote_identity: Optional[Any] = None
) -> Dict[str, Any]:
    """Validate a signed ``/tau/file`` request body.

    Verifies the signature against the carried public key, checks the
    key derives the claimed sender hash, and — when the link supplied
    an identity — checks that identity is the claimed sender too.  A
    forged, oversized or badly named offer is rejected outright; the
    receiver still re-checks the assembled bytes against ``sha256``
    after the transfer.
    """
    payload = decode(raw, max_bytes=MAX_REQUEST_BYTES)
    if payload.get("type") != "file_offer":
        raise ProtocolError("expected a file_offer payload")
    transfer_id = _clean_message_id(payload.get("id"))
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
