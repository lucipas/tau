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
    derive_group_id,
    encode,
    encode_group_message,
    encode_message,
    excerpt_hash,
    normalize_message,
    parse_announce,
    parse_group_message,
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
    assert parsed["in_reply_to"] is None


def test_message_roundtrip_with_reply():
    reply_to = {"id": "a" * 32, "excerpt": excerpt_hash("original thought")}
    data = encode_message("seconded", message_id="b" * 32, in_reply_to=reply_to)
    parsed = parse_message(data)
    assert parsed["text"] == "seconded"
    assert parsed["id"] == "b" * 32
    assert parsed["in_reply_to"] == reply_to


def test_message_reply_respects_request_budget():
    # Four-byte characters are the worst case: a full-length body fits the
    # budget alone, but the reference's overhead pushes it past the limit.
    text = "\U0001F600" * MAX_MESSAGE_CHARS
    reply_to = {"id": "a" * 32, "excerpt": excerpt_hash("original thought")}
    with pytest.raises(ProtocolError):
        encode_message(text, message_id="b" * 32, in_reply_to=reply_to)
    # A reply that fits stays under the limit.
    data = encode_message("x" * 1000, message_id="b" * 32, in_reply_to=reply_to)
    assert len(data) <= MAX_REQUEST_BYTES


def test_message_rejects_malformed_reply():
    with pytest.raises(ProtocolError):
        encode_message("hi", in_reply_to={"id": ""})
    with pytest.raises(ProtocolError):
        encode_message("hi", in_reply_to={"id": "x" * 65})
    with pytest.raises(ProtocolError):
        encode_message("hi", in_reply_to={"id": "a" * 32, "excerpt": "not-hex!"})
    with pytest.raises(ProtocolError):
        parse_message(
            encode(
                {
                    "type": "message",
                    "text": "hi",
                    "in_reply_to": {"id": "a" * 32, "excerpt": "zz"},
                }
            )
        )


def test_group_message_roundtrip_with_reply():
    members = ["ab" * 16, "cd" * 16, "ef" * 16]
    reply_to = {"id": "a" * 32, "excerpt": excerpt_hash("original thought")}
    data = encode_group_message(
        "seconded",
        message_id="b" * 32,
        group_id=derive_group_id(members[0], "weekend", members),
        group_name="weekend",
        creator=members[0],
        members=members,
        in_reply_to=reply_to,
    )
    parsed = parse_group_message(data)
    assert parsed["text"] == "seconded"
    assert parsed["id"] == "b" * 32
    assert parsed["in_reply_to"] == reply_to


def test_group_message_without_reply_has_no_reference():
    members = ["ab" * 16, "cd" * 16, "ef" * 16]
    data = encode_group_message(
        "hello all",
        message_id="b" * 32,
        group_id=derive_group_id(members[0], "weekend", members),
        group_name="weekend",
        creator=members[0],
        members=members,
    )
    assert parse_group_message(data)["in_reply_to"] is None


def test_group_message_rejects_malformed_reply():
    members = ["ab" * 16, "cd" * 16, "ef" * 16]
    group_id = derive_group_id(members[0], "weekend", members)
    with pytest.raises(ProtocolError):
        encode_group_message(
            "hi",
            group_id=group_id,
            group_name="weekend",
            creator=members[0],
            members=members,
            in_reply_to={"id": ""},
        )
    with pytest.raises(ProtocolError):
        parse_group_message(
            encode(
                {
                    "type": "group_message",
                    "text": "hi",
                    "group": group_id,
                    "group_name": "weekend",
                    "creator": members[0],
                    "members": members,
                    "in_reply_to": {"id": "a" * 32, "excerpt": "zz"},
                }
            )
        )


def test_excerpt_hash_is_stable_short_hex():
    assert excerpt_hash("original thought") == excerpt_hash("original thought")
    assert len(excerpt_hash("original thought")) == 16
    assert excerpt_hash("different") != excerpt_hash("original thought")


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
        # Reserved inbox record types, never a DM kind: a message
        # tagged "group", "file" or "note" could never be stored in the
        # recipient's inbox.
        "group",
        "file",
        "note",
        "NOTE",
        "GROUP",
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


def test_reaction_roundtrip():
    from tau.protocol import clean_reaction, encode_reaction, parse_reaction

    data = encode_reaction("msg-123", "👍", message_id="react-1")
    parsed = parse_reaction(data)
    assert parsed["target"] == "msg-123"
    assert parsed["reaction"] == "👍"
    assert parsed["id"] == "react-1"
    assert isinstance(parsed["sent_at"], float)


def test_reaction_clear_roundtrip():
    from tau.protocol import encode_reaction, parse_reaction

    parsed = parse_reaction(encode_reaction("msg-123", ""))
    assert parsed["reaction"] == ""


def test_clean_reaction_accepts_emoji_and_short_tokens():
    from tau.protocol import MAX_REACTION_CHARS, clean_reaction

    assert clean_reaction("👍") == "👍"
    assert clean_reaction("+1") == "+1"
    assert clean_reaction("  lol  ") == "lol"
    assert clean_reaction("") == ""
    assert clean_reaction("   ") == ""
    assert clean_reaction(None) == ""


def test_clean_reaction_rejects_long_or_non_string_tokens():
    from tau.protocol import MAX_REACTION_CHARS, clean_reaction

    with pytest.raises(ProtocolError):
        clean_reaction("x" * (MAX_REACTION_CHARS + 1))
    with pytest.raises(ProtocolError):
        clean_reaction(42)
    with pytest.raises(ProtocolError):
        clean_reaction(b"bytes")


def test_parse_reaction_rejects_wrong_shape():
    from tau.protocol import encode_reaction, parse_reaction

    with pytest.raises(ProtocolError):
        parse_reaction(encode({"type": "message", "text": "hi"}))
    with pytest.raises(ProtocolError):
        parse_reaction(encode({"type": "reaction", "target": "msg-1"}))
    with pytest.raises(ProtocolError):
        parse_reaction(encode({"type": "reaction", "target": "", "reaction": "👍"}))
    with pytest.raises(ProtocolError):
        parse_reaction(
            encode(
                {
                    "type": "reaction",
                    "target": "msg-1",
                    "reaction": "x" * 33,
                }
            )
        )


def test_reaction_signature_roundtrip():
    from tau.protocol import encode_reaction, parse_reaction

    parsed = parse_reaction(
        encode_reaction("msg-123", "👍", message_id="react-1", signature="aGVsbG8=")
    )
    assert parsed["sig"] == "aGVsbG8="
    assert parsed["reaction"] == "👍"


def test_retract_roundtrip():
    from tau.protocol import encode_retract, parse_retract

    data = encode_retract("msg-123", message_id="retract-1")
    parsed = parse_retract(data)
    assert parsed["target"] == "msg-123"
    assert parsed["id"] == "retract-1"
    assert isinstance(parsed["sent_at"], float)


def test_parse_retract_rejects_wrong_shape():
    from tau.protocol import encode_retract, parse_retract

    with pytest.raises(ProtocolError):
        parse_retract(encode({"type": "reaction", "target": "msg-1", "reaction": "👍"}))
    with pytest.raises(ProtocolError):
        parse_retract(encode({"type": "retraction", "target": ""}))
    with pytest.raises(ProtocolError):
        parse_retract(encode({"type": "retraction"}))
    with pytest.raises(ProtocolError):
        parse_retract(
            encode({"type": "retraction", "target": "msg-1", "sig": "not base64!!"})
        )


def test_retract_signature_roundtrip():
    from tau.protocol import encode_retract, parse_retract

    parsed = parse_retract(
        encode_retract("msg-123", message_id="retract-1", signature="aGVsbG8=")
    )
    assert parsed["sig"] == "aGVsbG8="
    assert parsed["target"] == "msg-123"


def test_reaction_without_signature_parses_with_none():
    from tau.protocol import encode_reaction, parse_reaction

    assert parse_reaction(encode_reaction("msg-1", "👍"))["sig"] is None


def test_parse_reaction_rejects_bad_signature():
    from tau.protocol import parse_reaction

    with pytest.raises(ProtocolError):
        parse_reaction(
            encode({"type": "reaction", "target": "msg-1", "reaction": "👍", "sig": "!!!"})
        )
    with pytest.raises(ProtocolError):
        parse_reaction(
            encode({"type": "reaction", "target": "msg-1", "reaction": "👍", "sig": 42})
        )


def test_reaction_unsigned_bytes_exclude_the_signature():
    import base64

    from tau.protocol import (
        encode,
        encode_reaction,
        parse_reaction,
        reaction_dict,
        reaction_unsigned_bytes,
    )

    payload = reaction_dict("msg-1", "👍", message_id="react-1", sent_at=12.0)
    unsigned = encode(payload)
    payload["sig"] = base64.b64encode(b"x" * 64).decode("ascii")
    signed = encode(payload)
    assert reaction_unsigned_bytes(signed) == unsigned
    assert parse_reaction(signed)["sig"] == payload["sig"]
    assert encode_reaction("msg-1", "👍", message_id="react-1", sent_at=12.0) == unsigned


def test_message_rejects_invalid_expiry():
    with pytest.raises(ProtocolError):
        encode_message("hi", expires_at="soon")
    with pytest.raises(ProtocolError):
        encode_message("hi", expires_at=float("inf"))
    with pytest.raises(ProtocolError):
        parse_message(encode({"type": "message", "text": "hi", "expires_at": "soon"}))
    with pytest.raises(ProtocolError):
        parse_message(encode({"type": "message", "text": "hi", "expires_at": True}))


def test_message_roundtrip_with_expiry():
    data = encode_message("vanish soon", expires_at=1893456000.0)
    parsed = parse_message(data)
    assert parsed["text"] == "vanish soon"
    assert parsed["expires_at"] == 1893456000.0
