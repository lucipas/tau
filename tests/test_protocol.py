from __future__ import annotations

import base64
import json

import RNS
import pytest

from tau import protocol
from tau.identity import destination_hash
from tau.protocol import (
    ANNOUNCE_MAX_BYTES,
    MAX_MESSAGE_CHARS,
    MAX_REQUEST_BYTES,
    ProtocolError,
    build_announce,
    clean_beacon,
    clean_contact,
    clean_location,
    decode,
    derive_group_id,
    encode,
    encode_group_message,
    encode_message,
    excerpt_hash,
    generate_group_id,
    contact_default_text,
    location_maps_url,
    mint_roster_attestation,
    normalize_message,
    parse_announce,
    parse_group_message,
    parse_location_arg,
    parse_message,
    response_error,
    response_ok,
    roster_attestation_matches,
    sanitize_name,
    short_hash,
    verify_roster_attestation,
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


def test_clean_location_accepts_named_and_bare_pins():
    assert clean_location({"lat": 34.05, "lon": -83.88}) == {
        "lat": 34.05,
        "lon": -83.88,
    }
    assert clean_location(
        {"lat": 34.05, "lon": -83.88, "name": "  Rock Springs Park  "}
    ) == {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"}
    # An empty name is no name, not a blank one.
    assert clean_location({"lat": 0, "lon": 0, "name": "   "}) == {
        "lat": 0.0,
        "lon": 0.0,
    }
    assert clean_location(None) is None
    # The poles and the antimeridian are valid pins.
    assert clean_location({"lat": -90, "lon": 180}) == {"lat": -90.0, "lon": 180.0}


def test_clean_location_rejects_out_of_range_and_bad_types():
    assert clean_location({"lat": 91, "lon": 0}) is None
    assert clean_location({"lat": 0, "lon": -181}) is None
    assert clean_location({"lat": "34.05", "lon": -83.88}) is None
    assert clean_location({"lat": True, "lon": 0}) is None
    assert clean_location({"lat": float("nan"), "lon": 0}) is None
    assert clean_location({"lat": 34.05}) is None
    assert clean_location({"lat": 34.05, "lon": -83.88, "name": 42}) is None
    assert clean_location(
        {"lat": 34.05, "lon": -83.88, "name": "x" * 201}
    ) is None
    assert clean_location("34.05,-83.88") is None


def test_clean_contact_accepts_named_cards():
    card = {"name": "carol", "destination_hash": "cc" * 16}
    assert clean_contact(card) == card
    # Whitespace trims and an uppercase hash lowercases, like a pin.
    assert clean_contact(
        {"name": "  carol  ", "destination_hash": "CC" * 16}
    ) == card
    assert clean_contact(None) is None


def test_clean_contact_rejects_malformed_cards():
    assert clean_contact({"name": "carol", "destination_hash": "zz"}) is None
    assert clean_contact({"name": "carol", "destination_hash": "cc" * 8}) is None
    assert clean_contact({"name": "carol"}) is None
    assert clean_contact({"destination_hash": "cc" * 16}) is None
    assert clean_contact({"name": "", "destination_hash": "cc" * 16}) is None
    assert clean_contact({"name": "   ", "destination_hash": "cc" * 16}) is None
    assert clean_contact({"name": 42, "destination_hash": "cc" * 16}) is None
    assert clean_contact({"name": "carol", "destination_hash": 42}) is None
    assert clean_contact("carol") is None


def test_contact_default_text_names_peer_and_short_hash():
    card = {"name": "carol", "destination_hash": "cc" * 16}
    assert contact_default_text(card) == "👤 Shared contact: carol (cccccccc…)"


def test_parse_location_arg():
    assert parse_location_arg("34.05,-83.88 Rock Springs Park") == {
        "lat": 34.05,
        "lon": -83.88,
        "name": "Rock Springs Park",
    }
    assert parse_location_arg("34.05,-83.88") == {"lat": 34.05, "lon": -83.88}
    assert parse_location_arg("  -12.5,  45.0  the park  ") == {
        "lat": -12.5,
        "lon": 45.0,
        "name": "the park",
    }
    for bad in ("", "34.05", "34.05,-83.88,10", "north,-83.88", "91,0", "0,181"):
        with pytest.raises(ValueError):
            parse_location_arg(bad)


def test_location_maps_url():
    url = location_maps_url({"lat": 34.05, "lon": -83.88})
    assert url.startswith("https://www.openstreetmap.org/?mlat=34.05&mlon=-83.88")
    assert url.endswith("#map=15/34.05/-83.88")


def test_message_roundtrip_with_location():
    pin = {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"}
    data = encode_message("meet here", message_id="b" * 32, location=pin)
    parsed = parse_message(data)
    assert parsed["text"] == "meet here"
    assert parsed["location"] == pin


def test_message_roundtrip_with_contact():
    card = {"name": "carol", "destination_hash": "cc" * 16}
    data = encode_message("meet carol", message_id="b" * 32, contact=card)
    parsed = parse_message(data)
    assert parsed["text"] == "meet carol"
    assert parsed["contact"] == card
    # A malformed card on the wire drops instead of the message.
    dropped = parse_message(
        encode(
            {
                "type": "message",
                "text": "hi",
                "contact": {"name": "carol", "destination_hash": "zz"},
            }
        )
    )
    assert dropped["contact"] is None
    assert dropped["text"] == "hi"
    # A card given to encode must be valid.
    with pytest.raises(ProtocolError):
        encode_message("hi", contact={"name": "carol", "destination_hash": "zz"})
    # A message without a card carries none.
    assert parse_message(encode_message("hi", message_id="c" * 32))["contact"] is None


def test_clean_beacon_accepts_valid_marker():
    assert clean_beacon({"id": "aabbccdd", "expires_at": 1800003600, "seq": 2}) == {
        "id": "aabbccdd",
        "expires_at": 1800003600.0,
        "seq": 2,
    }
    assert clean_beacon(None) is None


def test_clean_beacon_rejects_malformed_markers():
    assert clean_beacon({"expires_at": 1800003600, "seq": 1}) is None
    assert clean_beacon({"id": "", "expires_at": 1800003600, "seq": 1}) is None
    assert clean_beacon({"id": "a", "expires_at": "soon", "seq": 1}) is None
    assert clean_beacon({"id": "a", "expires_at": 1800003600, "seq": 0}) is None
    assert clean_beacon({"id": "a", "expires_at": 1800003600, "seq": 1.5}) is None
    # A 401-digit integer overflows float(): it must drop, not abort
    # the channel post handler before signature verification.
    assert clean_beacon({"id": "a", "expires_at": 10**400, "seq": 1}) is None
    assert clean_beacon("beacon") is None


def test_message_roundtrip_with_beacon():
    pin = {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"}
    marker = {"id": "aabbccdd", "expires_at": 1800003600, "seq": 2}
    data = encode_message(
        "on my way",
        message_id="b" * 32,
        location=pin,
        beacon=marker,
        kind="location",
    )
    parsed = parse_message(data)
    assert parsed["location"] == pin
    assert parsed["beacon"] == marker
    # A beacon without a pin is rejected, and so is a bad marker.
    with pytest.raises(ProtocolError):
        encode_message("on my way", beacon=marker)
    with pytest.raises(ProtocolError):
        encode_message("on my way", location=pin, beacon={"id": "a", "seq": 0})
    # A message without a beacon carries none.
    assert parse_message(
        encode_message("hi", message_id="c" * 32)
    )["beacon"] is None
    # A pin without coordinates is rejected at encode time, and a
    # malformed pin on the wire drops instead of the message.
    with pytest.raises(ProtocolError):
        encode_message("hi", location={"lat": 91, "lon": 0})
    with pytest.raises(ProtocolError):
        encode_message("hi", location={"lat": 34.05})
    dropped = parse_message(
        encode({"type": "message", "text": "hi", "location": {"lat": 91, "lon": 0}})
    )
    assert dropped["location"] is None
    assert dropped["text"] == "hi"


def test_group_message_roundtrip_with_location():
    members = ["ab" * 16, "cd" * 16, "ef" * 16]
    pin = {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"}
    data = encode_group_message(
        "meet here",
        message_id="b" * 32,
        group_id=derive_group_id(members[0], "weekend", members),
        group_name="weekend",
        creator=members[0],
        members=members,
        location=pin,
    )
    parsed = parse_group_message(data)
    assert parsed["text"] == "meet here"
    assert parsed["location"] == pin
    with pytest.raises(ProtocolError):
        encode_group_message(
            "hi",
            group_id=derive_group_id(members[0], "weekend", members),
            group_name="weekend",
            creator=members[0],
            members=members,
            location={"lat": 91, "lon": 0},
        )


def test_group_message_roundtrip_with_contact():
    members = ["ab" * 16, "cd" * 16, "ef" * 16]
    card = {"name": "carol", "destination_hash": "cc" * 16}
    data = encode_group_message(
        "meet carol",
        message_id="b" * 32,
        group_id=derive_group_id(members[0], "weekend", members),
        group_name="weekend",
        creator=members[0],
        members=members,
        contact=card,
    )
    parsed = parse_group_message(data)
    assert parsed["text"] == "meet carol"
    assert parsed["contact"] == card
    with pytest.raises(ProtocolError):
        encode_group_message(
            "hi",
            group_id=derive_group_id(members[0], "weekend", members),
            group_name="weekend",
            creator=members[0],
            members=members,
            contact={"name": "carol", "destination_hash": "zz"},
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


def test_group_message_roundtrip_with_logical_reply_reference():
    # A group reply may carry the quoted message's logical reference —
    # the group, its author, and the send time every leg shares — so a
    # member who holds a different leg id can still resolve the quote.
    members = ["ab" * 16, "cd" * 16, "ef" * 16]
    group_id = derive_group_id(members[0], "weekend", members)
    reply_to = {
        "id": "a" * 32,
        "excerpt": excerpt_hash("original thought"),
        "group": group_id,
        "author": members[0],
        "sent_at": 1234.5,
    }
    data = encode_group_message(
        "seconded",
        message_id="b" * 32,
        group_id=group_id,
        group_name="weekend",
        creator=members[0],
        members=members,
        in_reply_to=reply_to,
    )
    parsed = parse_group_message(data)
    assert parsed["in_reply_to"] == reply_to


def test_group_message_rejects_partial_logical_reference():
    members = ["ab" * 16, "cd" * 16, "ef" * 16]
    group_id = derive_group_id(members[0], "weekend", members)
    base = {
        "group_id": group_id,
        "group_name": "weekend",
        "creator": members[0],
        "members": members,
    }
    # The three travel together or not at all: a partial logical
    # reference names nothing resolvable.
    with pytest.raises(ProtocolError):
        encode_group_message(
            "hi",
            **base,
            in_reply_to={"id": "a" * 32, "group": group_id},
        )
    with pytest.raises(ProtocolError):
        encode_group_message(
            "hi",
            **base,
            in_reply_to={
                "id": "a" * 32,
                "group": group_id,
                "author": members[0],
            },
        )
    with pytest.raises(ProtocolError):
        encode_group_message(
            "hi",
            **base,
            in_reply_to={
                "id": "a" * 32,
                "group": group_id,
                "author": members[0],
                "sent_at": "yesterday",
            },
        )


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


# -- secret groups: roster attestations (#697) ------------------------------


def _secret_group_fixture():
    """A secret group: opaque id, creator-signed roster attestation."""
    creator = RNS.Identity()
    member = RNS.Identity()
    creator_hash = destination_hash(creator).hex()
    member_hash = destination_hash(member).hex()
    members = sorted([creator_hash, member_hash])
    group_id = generate_group_id()
    attestation = mint_roster_attestation(
        creator,
        group_id=group_id,
        name="weekend",
        creator=creator_hash,
        members=members,
    )
    return creator, member, creator_hash, member_hash, members, group_id, attestation


def test_roster_attestation_roundtrip_verifies():
    _, _, creator_hash, _, members, group_id, attestation = _secret_group_fixture()
    verified = verify_roster_attestation(attestation)
    assert verified == attestation
    assert roster_attestation_matches(
        verified,
        group_id=group_id,
        name="weekend",
        creator=creator_hash,
        members=members,
    )
    # The opaque id does not derive from the roster.
    assert group_id != derive_group_id(creator_hash, "weekend", members)


def test_roster_attestation_rejects_tampered_roster():
    _, _, _, _, _, _, attestation = _secret_group_fixture()
    tampered = dict(attestation)
    tampered["members"] = sorted([attestation["members"][0], "12" * 16])
    assert verify_roster_attestation(tampered) is None
    tampered_name = dict(attestation, name="other")
    assert verify_roster_attestation(tampered_name) is None


def test_roster_attestation_rejects_wrong_creator_key():
    _, other, _, _, _, _, attestation = _secret_group_fixture()
    wrong_key = dict(attestation, creator_key=other.get_public_key().hex())
    assert verify_roster_attestation(wrong_key) is None


def test_roster_attestation_rejects_forged_signature():
    _, other, _, _, _, _, attestation = _secret_group_fixture()
    unsigned = {k: v for k, v in attestation.items() if k != "sig"}
    forged_sig = base64.b64encode(other.sign(encode(unsigned))).decode("ascii")
    forged = dict(unsigned, sig=forged_sig)
    assert verify_roster_attestation(forged) is None
    # Minting with an identity that is not the creator is refused.
    _, _, creator_hash, _, members, group_id, _ = _secret_group_fixture()
    with pytest.raises(ProtocolError):
        mint_roster_attestation(
            other,
            group_id=group_id,
            name="weekend",
            creator=creator_hash,
            members=members,
        )


def test_secret_group_message_roundtrip_with_attestation():
    _, _, creator_hash, _, members, group_id, attestation = _secret_group_fixture()
    data = encode_group_message(
        "hi all",
        message_id="gm-secret-1",
        group_id=group_id,
        group_name="weekend",
        creator=creator_hash,
        members=members,
        roster_attestation=attestation,
    )
    parsed = parse_group_message(data)
    assert parsed["text"] == "hi all"
    assert parsed["group"] == group_id
    assert parsed["roster_attestation"] == attestation


def test_secret_group_message_rejects_without_attestation():
    # An opaque id cannot derive, so a leg without its attestation is
    # rejected at parse time instead of pinning an unverifiable group.
    _, _, creator_hash, _, members, group_id, _ = _secret_group_fixture()
    with pytest.raises(ProtocolError, match="does not match"):
        parse_group_message(
            encode_group_message(
                "hi all",
                message_id="gm-secret-2",
                group_id=group_id,
                group_name="weekend",
                creator=creator_hash,
                members=members,
            )
        )


def test_secret_group_message_rejects_mismatched_attestation():
    _, _, creator_hash, _, members, group_id, attestation = _secret_group_fixture()
    # The attestation binds the original roster: a leg carrying a
    # different roster under the same id is a fork, and is rejected —
    # at encode time, and at parse time for a hand-crafted payload
    # that bypasses the encoder.
    forked = sorted([creator_hash, "12" * 16])
    with pytest.raises(ProtocolError, match="does not verify"):
        encode_group_message(
            "forked roster",
            message_id="gm-secret-3",
            group_id=group_id,
            group_name="weekend",
            creator=creator_hash,
            members=forked,
            roster_attestation=attestation,
        )
    with pytest.raises(ProtocolError, match="does not verify"):
        parse_group_message(
            encode(
                {
                    "type": "group_message",
                    "v": 1,
                    "text": "forked roster",
                    "group": group_id,
                    "group_name": "weekend",
                    "creator": creator_hash,
                    "members": forked,
                    "sent_at": 1.0,
                    "id": "gm-secret-3b",
                    "roster_attestation": attestation,
                }
            )
        )
    # A garbage attestation field is rejected, not silently dropped.
    with pytest.raises(ProtocolError, match="does not verify"):
        parse_group_message(
            encode(
                {
                    "type": "group_message",
                    "v": 1,
                    "text": "hi",
                    "group": group_id,
                    "group_name": "weekend",
                    "creator": creator_hash,
                    "members": members,
                    "sent_at": 1.0,
                    "id": "gm-secret-4",
                    "roster_attestation": {"v": 1},
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


def test_typing_roundtrip():
    from tau.protocol import encode_typing, parse_typing

    parsed = parse_typing(encode_typing("start", sent_at=123.0))
    assert parsed == {"state": "start", "group": None, "sent_at": 123.0}


def test_typing_group_roundtrip():
    from tau.protocol import encode_typing, parse_typing

    parsed = parse_typing(encode_typing("stop", group="ab" * 16))
    assert parsed["state"] == "stop"
    assert parsed["group"] == "ab" * 16


def test_typing_rejects_bad_state_and_shape():
    from tau.protocol import encode_typing, parse_typing

    with pytest.raises(ProtocolError):
        encode_typing("maybe")
    with pytest.raises(ProtocolError):
        parse_typing(encode({"type": "message", "text": "hi"}))
    with pytest.raises(ProtocolError):
        parse_typing(encode({"type": "typing", "state": "maybe"}))
    with pytest.raises(ProtocolError):
        parse_typing(
            encode({"type": "typing", "state": "start", "group": "not-a-hash"})
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


def test_retract_group_context_roundtrip():
    from tau.protocol import encode_retract, parse_retract

    data = encode_retract(
        "leg-1",
        message_id="retract-1",
        group="ab" * 16,
        target_author="cd" * 16,
        target_sent_at=100.0,
    )
    parsed = parse_retract(data)
    assert parsed["target"] == "leg-1"
    assert parsed["group"] == "ab" * 16
    assert parsed["target_author"] == "cd" * 16
    assert parsed["target_sent_at"] == 100.0


def test_retract_drops_malformed_group_context():
    from tau.protocol import encode, parse_retract

    # Malformed context is advisory: the retract still parses, with
    # the context dropped — the target id alone is still valid.
    parsed = parse_retract(
        encode(
            {
                "type": "retraction",
                "target": "msg-1",
                "sent_at": 10.0,
                "group": "not-hex",
                "target_author": 42,
                "target_sent_at": "soon",
            }
        )
    )
    assert parsed["target"] == "msg-1"
    assert parsed["group"] is None
    assert parsed["target_author"] is None
    assert parsed["target_sent_at"] is None


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


def test_edit_roundtrip():
    from tau.protocol import encode_edit, parse_edit

    data = encode_edit("msg-123", "fixed text", message_id="edit-1")
    parsed = parse_edit(data)
    assert parsed["target"] == "msg-123"
    assert parsed["text"] == "fixed text"
    assert parsed["id"] == "edit-1"
    assert isinstance(parsed["sent_at"], float)


def test_edit_roundtrip_with_group_context():
    from tau.protocol import encode_edit, parse_edit

    data = encode_edit(
        "leg-1",
        "fixed text",
        message_id="edit-1",
        group="ab" * 16,
        target_author="cd" * 16,
        target_sent_at=100.0,
    )
    parsed = parse_edit(data)
    assert parsed["target"] == "leg-1"
    assert parsed["text"] == "fixed text"
    assert parsed["group"] == "ab" * 16
    assert parsed["target_author"] == "cd" * 16
    assert parsed["target_sent_at"] == 100.0


def test_parse_edit_drops_malformed_group_context():
    from tau.protocol import encode, parse_edit

    parsed = parse_edit(
        encode(
            {
                "type": "dm_edit",
                "target": "msg-1",
                "text": "fixed",
                "sent_at": 30.0,
                "group": "not-a-hash",
                "target_author": 42,
                "target_sent_at": "soon",
            }
        )
    )
    assert parsed["group"] is None
    assert parsed["target_author"] is None
    assert parsed["target_sent_at"] is None
    assert parsed["target"] == "msg-1"


def test_parse_edit_rejects_wrong_shape():
    from tau.protocol import encode_edit, parse_edit

    with pytest.raises(ProtocolError):
        parse_edit(encode({"type": "message", "text": "hi"}))
    with pytest.raises(ProtocolError):
        parse_edit(encode({"type": "dm_edit", "target": "msg-1"}))
    with pytest.raises(ProtocolError):
        parse_edit(encode({"type": "dm_edit", "target": "", "text": "hi"}))
    with pytest.raises(ProtocolError):
        parse_edit(encode({"type": "dm_edit", "target": "msg-1", "text": "   "}))
    with pytest.raises(ProtocolError):
        parse_edit(
            encode({"type": "dm_edit", "target": "msg-1", "text": "x" * 2001})
        )


def test_edit_signature_roundtrip():
    from tau.protocol import encode_edit, parse_edit

    parsed = parse_edit(
        encode_edit("msg-123", "fixed", message_id="edit-1", signature="aGVsbG8=")
    )
    assert parsed["sig"] == "aGVsbG8="
    assert parsed["text"] == "fixed"


def test_edit_without_signature_parses_with_none():
    from tau.protocol import encode_edit, parse_edit

    assert parse_edit(encode_edit("msg-1", "fixed"))["sig"] is None


def test_parse_edit_rejects_bad_signature():
    from tau.protocol import parse_edit

    with pytest.raises(ProtocolError):
        parse_edit(
            encode({"type": "dm_edit", "target": "msg-1", "text": "hi", "sig": "!!!"})
        )
    with pytest.raises(ProtocolError):
        parse_edit(
            encode({"type": "dm_edit", "target": "msg-1", "text": "hi", "sig": 42})
        )


def test_edit_unsigned_bytes_exclude_the_signature():
    import base64

    from tau.protocol import (
        edit_dict,
        edit_unsigned_bytes,
        encode,
        encode_edit,
        parse_edit,
    )

    payload = edit_dict("msg-1", "fixed", message_id="edit-1", sent_at=12.0)
    unsigned = encode(payload)
    payload["sig"] = base64.b64encode(b"x" * 64).decode("ascii")
    signed = encode(payload)
    assert edit_unsigned_bytes(signed) == unsigned
    assert parse_edit(signed)["text"] == "fixed"


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


def _poll_group_kwargs():
    members = ["ab" * 16, "cd" * 16, "ef" * 16]
    return {
        "group_id": derive_group_id(members[0], "weekend", members),
        "group_name": "weekend",
        "creator": members[0],
        "members": members,
    }


def test_group_poll_roundtrip():
    data = encode_group_message(
        "which day?",
        message_id="b" * 32,
        poll_options=["Saturday", "Sunday"],
        **_poll_group_kwargs(),
    )
    parsed = parse_group_message(data)
    assert parsed["text"] == "which day?"
    assert parsed["poll_options"] == ["Saturday", "Sunday"]
    assert parsed["poll_vote"] is None
    assert parsed["poll_close"] is None


def test_group_poll_vote_and_close_roundtrip():
    target = {"author": "ab" * 16, "sent_at": 1234.5}
    vote_data = encode_group_message(
        "voted: Saturday",
        message_id="c" * 32,
        poll_vote={"target": target, "option": 0},
        **_poll_group_kwargs(),
    )
    parsed = parse_group_message(vote_data)
    assert parsed["poll_vote"] == {"target": target, "option": 0}
    assert parsed["poll_options"] is None
    close_data = encode_group_message(
        "closed the poll",
        message_id="d" * 32,
        poll_close=target,
        **_poll_group_kwargs(),
    )
    assert parse_group_message(close_data)["poll_close"] == target


def test_group_poll_rejects_two_poll_fields():
    with pytest.raises(ProtocolError):
        encode_group_message(
            "which day?",
            poll_options=["Saturday", "Sunday"],
            poll_vote={
                "target": {"author": "ab" * 16, "sent_at": 1.0},
                "option": 0,
            },
            **_poll_group_kwargs(),
        )


def test_dm_poll_roundtrip():
    data = encode_message(
        "which day?",
        message_id="b" * 32,
        poll_options=["Saturday", "Sunday"],
    )
    parsed = parse_message(data)
    assert parsed["text"] == "which day?"
    assert parsed["poll_options"] == ["Saturday", "Sunday"]
    assert parsed["poll_vote"] is None
    assert parsed["poll_close"] is None


def test_dm_poll_vote_and_close_roundtrip():
    target = {"author": "ab" * 16, "sent_at": 1234.5}
    vote_data = encode_message(
        "voted: Saturday",
        message_id="c" * 32,
        poll_vote={"target": target, "option": 0},
    )
    parsed = parse_message(vote_data)
    assert parsed["poll_vote"] == {"target": target, "option": 0}
    assert parsed["poll_options"] is None
    close_data = encode_message(
        "closed the poll",
        message_id="d" * 32,
        poll_close=target,
    )
    assert parse_message(close_data)["poll_close"] == target


def test_dm_poll_rejects_two_poll_fields():
    with pytest.raises(ProtocolError):
        encode_message(
            "which day?",
            poll_options=["Saturday", "Sunday"],
            poll_vote={
                "target": {"author": "ab" * 16, "sent_at": 1.0},
                "option": 0,
            },
        )


def test_dm_poll_rejects_bad_options_and_votes():
    with pytest.raises(ProtocolError):
        encode_message("which day?", poll_options=["only one"])
    with pytest.raises(ProtocolError):
        encode_message("which day?", poll_options=["ok", 7])
    with pytest.raises(ProtocolError):
        encode_message(
            "voted",
            poll_vote={"target": {"author": "ab" * 16, "sent_at": 1.0}},
        )


def test_dm_poll_parse_drops_contradictory_fields():
    # A message carrying more than one poll field reads as plain chat,
    # like a group message: the sender cannot have meant both.
    raw = encode(
        {
            "type": "message",
            "v": 1,
            "text": "which day?",
            "kind": "chat",
            "sent_at": 1000.0,
            "poll_options": ["Saturday", "Sunday"],
            "poll_vote": {
                "target": {"author": "ab" * 16, "sent_at": 1000.0},
                "option": 0,
            },
        }
    )
    parsed = parse_message(raw)
    assert parsed["poll_options"] is None
    assert parsed["poll_vote"] is None
    assert parsed["poll_close"] is None
    assert parsed["text"] == "which day?"


def test_group_poll_rejects_bad_options_and_votes():
    with pytest.raises(ProtocolError):
        encode_group_message(
            "which day?", poll_options=["only one"], **_poll_group_kwargs()
        )
    with pytest.raises(ProtocolError):
        encode_group_message(
            "which day?",
            poll_options=["ok", 7],
            **_poll_group_kwargs(),
        )
    with pytest.raises(ProtocolError):
        encode_group_message(
            "voted",
            poll_vote={"target": {"author": "ab" * 16, "sent_at": 1.0}},
            **_poll_group_kwargs(),
        )
    with pytest.raises(ProtocolError):
        encode_group_message(
            "voted",
            poll_vote={
                "target": {"author": "ab" * 16, "sent_at": 1.0},
                "option": -1,
            },
            **_poll_group_kwargs(),
        )
    with pytest.raises(ProtocolError):
        encode_group_message(
            "closed",
            poll_close={"author": "not-a-hash", "sent_at": 1.0},
            **_poll_group_kwargs(),
        )


def test_parse_group_message_drops_malformed_poll_fields():
    # A contradictory or malformed poll payload reads as plain chat
    # instead of rejecting the message: an older client keeps working.
    base = {
        "type": "group_message",
        "v": 1,
        "text": "which day?",
        "sent_at": 1234.5,
        "group": _poll_group_kwargs()["group_id"],
        "group_name": "weekend",
        "creator": "ab" * 16,
        "members": ["ab" * 16, "cd" * 16, "ef" * 16],
    }
    both = dict(
        base,
        poll_options=["Saturday", "Sunday"],
        poll_vote={
            "target": {"author": "ab" * 16, "sent_at": 1.0},
            "option": 0,
        },
    )
    parsed = parse_group_message(encode(both))
    assert parsed["poll_options"] is None
    assert parsed["poll_vote"] is None
    bad_options = dict(base, poll_options=["only one"])
    assert parse_group_message(encode(bad_options))["poll_options"] is None
    bad_vote = dict(
        base,
        poll_vote={"target": {"author": "ab" * 16, "sent_at": 1.0}},
    )
    assert parse_group_message(encode(bad_vote))["poll_vote"] is None

def test_media_request_round_trip():
    from tau.protocol import build_media_request, parse_media_request

    digest = "ab" * 32
    assert parse_media_request(build_media_request(digest)) == {"sha256": digest}
    assert parse_media_request(build_media_request(digest.upper())) == {
        "sha256": digest
    }


def test_media_request_rejects_bad_digest():
    from tau.protocol import build_media_request, parse_media_request

    with pytest.raises(ProtocolError):
        build_media_request("not-a-digest")
    with pytest.raises(ProtocolError):
        build_media_request("zz" * 32)
    with pytest.raises(ProtocolError):
        parse_media_request(
            encode(
                {
                    "type": "media_request",
                    "v": protocol.PROTOCOL_VERSION,
                    "sha256": "zz" * 32,
                }
            )
        )
    with pytest.raises(ProtocolError):
        parse_media_request(
            encode(
                {
                    "type": "file_offer",
                    "v": protocol.PROTOCOL_VERSION,
                    "sha256": "ab" * 32,
                }
            )
        )
