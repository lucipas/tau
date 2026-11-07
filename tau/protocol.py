"""Wire protocol for tau.

Every tau node announces a single Reticulum destination named
``tau.node.<identity-hash>``.  Small, signed JSON announcements advertise the
node name.  Once a peer is known, messages are exchanged as requests over an
encrypted Reticulum ``Link``:

    /tau/ping     -> liveness and clock information
    /tau/info     -> static node information
    /tau/message  -> deliver a text message, echo it back
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

REQUEST_PING = "/tau/ping"
REQUEST_INFO = "/tau/info"
REQUEST_MESSAGE = "/tau/message"
REQUEST_READ = "/tau/read"
REQUEST_PEERS = "/tau/peers"
REQUEST_MAIL_STORE = "/tau/mail/store"
REQUEST_MAIL_FETCH = "/tau/mail/fetch"
REQUEST_PATHS = (
    REQUEST_PING,
    REQUEST_INFO,
    REQUEST_MESSAGE,
    REQUEST_READ,
    REQUEST_PEERS,
    REQUEST_MAIL_STORE,
    REQUEST_MAIL_FETCH,
)

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
        "caps": ["message", "ping", "peers", "mail", "read"],
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
