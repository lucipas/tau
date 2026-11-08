from __future__ import annotations

import json

import pytest

from tau import protocol
from tau.protocol import (
    ANNOUNCE_MAX_BYTES,
    MAX_MESSAGE_CHARS,
    MAX_REQUEST_BYTES,
    ProtocolError,
    build_announce,
    decode,
    encode,
    encode_message,
    normalize_message,
    parse_announce,
    parse_message,
    response_error,
    response_ok,
    sanitize_name,
    short_hash,
)


def test_sanitize_name_removes_control_characters_and_truncates():
    name = "  alice\x1b[31m\n\tthe   node  "
    assert sanitize_name(name) == "alice[31mthe node"
    assert len(sanitize_name("x" * 200)) == protocol.MAX_NAME_LENGTH
    assert sanitize_name("") == "anonymous"
    assert sanitize_name(None) == "anonymous"
    assert sanitize_name("   ") == "anonymous"


def test_normalize_message_keeps_newlines_but_drops_escapes():
    text = normalize_message("  hello\x1b[2J\nworld\t!  ")
    assert text == "hello[2J\nworld\t!"
    assert "\x1b" not in text


@pytest.mark.parametrize("value", ["", "   ", None, 42, b"bytes"])
def test_normalize_message_rejects_invalid_values(value):
    with pytest.raises(ProtocolError):
        normalize_message(value)


def test_normalize_message_rejects_oversized_text():
    with pytest.raises(ProtocolError):
        normalize_message("x" * (MAX_MESSAGE_CHARS + 1))


def test_encode_decode_roundtrip():
    payload = {"name": "café", "count": 2, "nested": {"ok": True}}
    assert decode(encode(payload)) == payload
    assert decode(json.dumps(payload).encode()) == payload


def test_decode_accepts_an_already_unpacked_dict():
    assert decode({"ok": True}) == {"ok": True}


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"not json",
        b"\xff\xfe",
        b"[]",
        b'"string"',
        b"123",
    ],
)
def test_decode_rejects_bad_payloads(raw):
    with pytest.raises(ProtocolError):
        decode(raw)


def test_decode_enforces_size_limit():
    with pytest.raises(ProtocolError):
        decode(b"{}" * 100, max_bytes=10)


def test_encode_rejects_non_serializable_and_nan():
    with pytest.raises(ProtocolError):
        encode({"bad": object()})
    with pytest.raises(ProtocolError):
        encode({"bad": float("nan")})


def test_announce_roundtrip_and_budget():
    data = build_announce("alice")
    assert len(data) <= ANNOUNCE_MAX_BYTES
    parsed = parse_announce(data)
    assert parsed["name"] == "alice"
    assert parsed["proto"] == "tau"
    assert parsed["v"] == protocol.PROTOCOL_VERSION
    assert "message" in parsed["caps"]


def test_announce_truncates_long_names_to_fit():
    data = build_announce("n" * 300)
    assert len(data) <= ANNOUNCE_MAX_BYTES
    assert len(parse_announce(data)["name"]) <= protocol.MAX_NAME_LENGTH


@pytest.mark.parametrize("app_data", [None, b"", b"junk", b"[]"])
def test_parse_announce_tolerates_foreign_payloads(app_data):
    parsed = parse_announce(app_data)
    assert parsed["name"] == "unknown"
    assert parsed["caps"] == []


def test_message_roundtrip():
    data = encode_message("hello there")
    parsed = parse_message(data)
    assert parsed["text"] == "hello there"
    assert parsed["kind"] == "chat"
    assert isinstance(parsed["sent_at"], float)


@pytest.mark.parametrize("kind", ["ping", "alert", "bot-command", "sensor_1", "A1"])
def test_message_kind_roundtrip_normalises(kind):
    parsed = parse_message(encode_message("hello", kind=kind))
    assert parsed["kind"] == kind.lower()


def test_message_without_kind_parses_as_chat():
    # A peer that predates kinds sends no kind field at all.
    data = encode(
        {"type": "message", "v": 1, "text": "old peer here", "sent_at": 1.0}
    )
    assert parse_message(data)["kind"] == "chat"


@pytest.mark.parametrize(
    "kind",
    [
        "",
        " ",
        "has space",
        "has/slash",
        "UPPER!",
        "-lead",
        "x" * 33,
        7,
        ["ping"],
        "café",
        "１",
        "١",
    ],
)
def test_message_kind_rejects_invalid(kind):
    with pytest.raises(ProtocolError):
        encode_message("hello", kind=kind)
    with pytest.raises(ProtocolError):
        parse_message(
            encode({"type": "message", "v": 1, "text": "hi", "kind": kind})
        )


def test_message_explicit_null_kind_is_rejected_but_absent_kind_is_chat():
    base = {"type": "message", "v": 1, "text": "hi", "sent_at": 1.0}
    assert parse_message(encode(base))["kind"] == "chat"
    with pytest.raises(ProtocolError):
        parse_message(encode({**base, "kind": None}))


def test_message_rejects_wrong_shape():
    with pytest.raises(ProtocolError):
        parse_message(encode({"type": "other", "text": "hi"}))
    with pytest.raises(ProtocolError):
        parse_message(encode({"type": "message"}))
    with pytest.raises(ProtocolError):
        parse_message(b"x" * (MAX_REQUEST_BYTES + 1))


def test_response_helpers():
    ok = decode(response_ok(echo="hi"))
    assert ok["ok"] is True and ok["echo"] == "hi"
    bad = decode(response_error("nope", "it failed"))
    assert bad["ok"] is False
    assert bad["error"] == {"code": "nope", "message": "it failed"}


def test_short_hash():
    assert short_hash("abcdef1234", 4) == "abcd"
    assert short_hash("", 4) == ""
