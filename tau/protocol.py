"""Wire protocol for tau.

Every tau node announces a single Reticulum destination named
``tau.node.<identity-hash>``.  Small, signed JSON announcements advertise the
node name.  Once a peer is known, messages are exchanged as requests over an
encrypted Reticulum ``Link``:

    /tau/ping     -> liveness and clock information
    /tau/info     -> static node information
    /tau/message  -> deliver a text message, echo it back
    /tau/peers    -> ask a peer which peers it has heard from

Payloads are JSON encoded as UTF-8 and deliberately size limited: a compromised
or buggy peer must not be able to make us buffer large amounts of data.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional

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

REQUEST_PING = "/tau/ping"
REQUEST_INFO = "/tau/info"
REQUEST_MESSAGE = "/tau/message"
REQUEST_PEERS = "/tau/peers"
REQUEST_PATHS = (REQUEST_PING, REQUEST_INFO, REQUEST_MESSAGE, REQUEST_PEERS)


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
        "caps": ["message", "ping", "peers"],
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


def encode_message(text: str) -> bytes:
    """Encode a chat message for the ``/tau/message`` request."""
    payload = {
        "type": "message",
        "v": PROTOCOL_VERSION,
        "text": normalize_message(text),
        "sent_at": time.time(),
        "client": f"tau/{__version__}",
    }
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
    return {"text": text, "sent_at": sent_at}


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
