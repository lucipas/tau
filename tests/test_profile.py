"""Tests for signed public peer profiles (tau.profile)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import RNS

from tau.profile import (
    MAX_BIO_CHARS,
    MAX_LINKS,
    ProfileError,
    ProfileStore,
    build_profile,
    clean_bio,
    clean_links,
    load_local_profile,
    normalize_profile,
    public_profile,
    resolve_avatar,
    save_local_profile,
    verify_profile,
)
from tau.protocol import (
    build_announce,
    decode,
    encode_profile_request,
    parse_announce,
    parse_profile_request,
)


def make_identity() -> RNS.Identity:
    return RNS.Identity()


def make_profile(identity=None, **overrides):
    identity = identity or make_identity()
    fields = {
        "name": "alice",
        "bio": "mesh tinkerer",
        "avatar": "ab" * 16,
        "links": ["https://example.com"],
        "version": 1,
        "created_at": 1000.0,
    }
    fields.update(overrides)
    return build_profile(identity, **fields)


# -- document build / verify ----------------------------------------------


def test_build_and_verify_round_trip():
    identity = make_identity()
    doc = make_profile(identity)
    verified = verify_profile(doc)
    assert verified["author"] == RNS.Destination.hash(
        identity, "tau", "node"
    ).hex()
    assert verified["key"] == identity.get_public_key().hex()
    assert verified["name"] == "alice"
    assert verified["bio"] == "mesh tinkerer"
    assert verified["avatar"] == "ab" * 16
    assert verified["links"] == ["https://example.com"]
    assert verified["version"] == 1
    assert verified["sig"] == doc["sig"]


def test_verify_rejects_tampered_bio():
    doc = make_profile()
    doc["bio"] = "someone else wrote this"
    with pytest.raises(ProfileError, match="signature is invalid"):
        verify_profile(doc)


def test_verify_rejects_tampered_links():
    doc = make_profile()
    doc["links"] = ["https://evil.example"]
    with pytest.raises(ProfileError, match="signature is invalid"):
        verify_profile(doc)


def test_verify_rejects_tampered_version():
    doc = make_profile(version=1)
    doc["version"] = 2
    with pytest.raises(ProfileError, match="signature is invalid"):
        verify_profile(doc)


def test_verify_rejects_wrong_key():
    identity = make_identity()
    other = make_identity()
    doc = make_profile(identity)
    doc["key"] = other.get_public_key().hex()
    with pytest.raises(ProfileError, match="does not match the profile author"):
        verify_profile(doc)


def test_verify_rejects_bad_signature():
    doc = make_profile()
    bad_sig = ("0" if doc["sig"][0] != "0" else "1") + doc["sig"][1:]
    doc["sig"] = bad_sig
    with pytest.raises(ProfileError, match="signature is invalid"):
        verify_profile(doc)


def test_verify_rejects_missing_signature():
    doc = make_profile()
    del doc["sig"]
    with pytest.raises(ProfileError, match="sig must be"):
        verify_profile(doc)


def test_verify_rejects_unknown_version():
    doc = make_profile()
    doc["v"] = 99
    with pytest.raises(ProfileError, match="unsupported profile version"):
        verify_profile(doc)


def test_verify_rejects_future_timestamp():
    doc = make_profile(created_at=1000.0)
    with pytest.raises(ProfileError, match="too far in the future"):
        verify_profile(doc, now=1000.0 - 86401.0)


def test_profile_without_optional_fields_verifies():
    identity = make_identity()
    doc = build_profile(identity, name="bob", version=1, created_at=1000.0)
    verified = verify_profile(doc)
    assert verified["bio"] == ""
    assert verified["avatar"] is None
    assert verified["links"] == []


def test_public_profile_drops_key_and_signature():
    doc = make_profile()
    view = public_profile(doc)
    assert view is not None
    assert "sig" not in view and "key" not in view
    assert view["name"] == "alice"
    assert view["version"] == 1
    assert public_profile(None) is None


# -- validation limits ----------------------------------------------------


def test_clean_bio_collapses_whitespace_and_allows_empty():
    assert clean_bio("  hello\n\n  world  ") == "hello world"
    assert clean_bio("") == ""
    assert clean_bio(None) == ""


def test_clean_bio_rejects_overlong_bio():
    with pytest.raises(ProfileError, match=f"longer than {MAX_BIO_CHARS}"):
        clean_bio("x" * (MAX_BIO_CHARS + 1))
    assert clean_bio("x" * MAX_BIO_CHARS) != ""


def test_clean_bio_rejects_non_string():
    with pytest.raises(ProfileError, match="bio must be a string"):
        clean_bio(42)


def test_clean_links_accepts_up_to_three():
    assert clean_links(["https://a.example", "http://b.example/x"]) == [
        "https://a.example",
        "http://b.example/x",
    ]
    assert clean_links(None) == []
    assert clean_links([]) == []


def test_clean_links_rejects_a_fourth_link():
    links = [f"https://example.com/{n}" for n in range(MAX_LINKS + 1)]
    with pytest.raises(ProfileError, match=f"at most {MAX_LINKS} links"):
        clean_links(links)


def test_clean_links_rejects_non_http_urls():
    with pytest.raises(ProfileError, match="must be an http\\(s\\) URL"):
        clean_links(["ftp://example.com/file"])
    with pytest.raises(ProfileError, match="must be an http\\(s\\) URL"):
        clean_links(["https://"])
    with pytest.raises(ProfileError, match="must be an http\\(s\\) URL"):
        clean_links(["not a url"])


def test_clean_links_rejects_overlong_link():
    with pytest.raises(ProfileError, match="longer than"):
        clean_links(["https://example.com/" + "x" * 200])


def test_normalize_rejects_bad_avatar():
    with pytest.raises(ProfileError, match="avatar must be"):
        normalize_profile(
            {
                "v": 1,
                "author": "ab" * 16,
                "key": "cd" * 64,
                "name": "alice",
                "bio": "",
                "avatar": "not-a-hash",
                "links": [],
                "version": 1,
                "created_at": 1000.0,
            }
        )


def test_normalize_rejects_version_zero():
    doc = make_profile(version=1)
    doc.pop("sig")
    doc["version"] = 0
    with pytest.raises(ProfileError, match="version must be"):
        normalize_profile(doc)


def test_build_rejects_overlong_bio_and_too_many_links():
    identity = make_identity()
    with pytest.raises(ProfileError, match="bio is longer"):
        build_profile(identity, name="alice", bio="x" * (MAX_BIO_CHARS + 1))
    with pytest.raises(ProfileError, match="at most"):
        build_profile(
            identity,
            name="alice",
            links=[f"https://example.com/{n}" for n in range(MAX_LINKS + 1)],
        )


# -- avatar resolution ----------------------------------------------------


def test_resolve_avatar_hashes_a_file(tmp_path):
    image = tmp_path / "avatar.png"
    image.write_bytes(b"\x89PNG fake image bytes")
    expected = hashlib.sha256(b"\x89PNG fake image bytes").hexdigest()
    assert resolve_avatar(str(image)) == expected


def test_resolve_avatar_accepts_hex_directly():
    assert resolve_avatar("AB" * 16) == "ab" * 16
    assert resolve_avatar("cd" * 32) == "cd" * 32
    assert resolve_avatar(None) is None


def test_resolve_avatar_rejects_garbage():
    with pytest.raises(ProfileError, match="neither an existing file"):
        resolve_avatar("no-such-file.png")
    with pytest.raises(ProfileError, match="neither an existing file"):
        resolve_avatar("xyz")
    with pytest.raises(ProfileError, match="avatar must be"):
        resolve_avatar(42)


# -- local profile persistence --------------------------------------------


def test_save_and_load_local_profile_round_trip(tmp_path):
    path = tmp_path / "profile.json"
    doc = make_profile()
    save_local_profile(path, doc)
    loaded = load_local_profile(path)
    assert loaded == doc


def test_load_local_profile_returns_none_when_absent(tmp_path):
    assert load_local_profile(tmp_path / "profile.json") is None


def test_load_local_profile_returns_none_when_invalid(tmp_path):
    path = tmp_path / "profile.json"
    path.write_text(json.dumps({"v": 1, "author": "nope"}), encoding="utf-8")
    assert load_local_profile(path) is None


# -- profile store: newest version only -----------------------------------


def test_store_keeps_only_the_newest_version():
    identity = make_identity()
    store = ProfileStore()
    old = make_profile(identity, version=1, created_at=1000.0)
    new = make_profile(identity, version=2, created_at=2000.0)
    assert store.store(old) is True
    assert store.store(new) is True
    assert store.get(old["author"])["version"] == 2
    # An older version arriving late is dropped.
    assert store.store(old) is False
    assert store.get(old["author"])["version"] == 2


def test_store_keeps_first_on_version_tie():
    identity = make_identity()
    store = ProfileStore()
    first = make_profile(identity, version=1, bio="first", created_at=1000.0)
    second = make_profile(identity, version=1, bio="second", created_at=2000.0)
    assert store.store(first) is True
    assert store.store(second) is False
    assert store.get(first["author"])["bio"] == "first"


def test_store_rejects_invalidly_signed_document():
    store = ProfileStore()
    doc = make_profile()
    doc["bio"] = "tampered"
    with pytest.raises(ProfileError):
        store.store(doc)
    assert len(store) == 0


def test_store_rejects_profile_for_another_author():
    store = ProfileStore()
    doc = make_profile()
    other = make_identity()
    doc["author"] = RNS.Destination.hash(other, "tau", "node").hex()
    with pytest.raises(ProfileError):
        store.store(doc)


def test_store_persists_and_reloads(tmp_path):
    identity = make_identity()
    path = tmp_path / "profiles.json"
    store = ProfileStore(path)
    doc = make_profile(identity, version=3, created_at=3000.0)
    assert store.store(doc) is True
    assert store.save() is True

    reloaded = ProfileStore(path)
    assert len(reloaded) == 1
    assert reloaded.get(doc["author"])["version"] == 3


def test_store_load_skips_invalid_entries(tmp_path):
    identity = make_identity()
    path = tmp_path / "profiles.json"
    good = make_profile(identity, version=1, created_at=1000.0)
    bad = dict(good)
    bad["bio"] = "tampered"
    payload = {"version": 1, "saved_at": 1000.0, "profiles": [good, bad]}
    path.write_text(json.dumps(payload), encoding="utf-8")

    store = ProfileStore(path)
    assert len(store) == 1
    assert store.get(good["author"])["bio"] == "mesh tinkerer"


def test_store_remove(tmp_path):
    identity = make_identity()
    store = ProfileStore()
    doc = make_profile(identity)
    store.store(doc)
    assert store.remove(doc["author"]) is True
    assert store.get(doc["author"]) is None
    assert store.remove(doc["author"]) is False


def test_store_all_newest_first():
    first = make_identity()
    second = make_identity()
    store = ProfileStore()
    store.store(make_profile(first, version=1, created_at=1000.0))
    store.store(make_profile(second, version=5, created_at=500.0))
    versions = [doc["version"] for doc in store.all()]
    assert versions == [5, 1]


# -- announce marker and request envelope ---------------------------------


def test_announce_carries_profile_version():
    data = build_announce("alice", profile_version=3)
    assert len(data) <= 256
    parsed = parse_announce(data)
    assert parsed["profile_version"] == 3
    assert parsed["name"] == "alice"


def test_announce_without_profile_has_no_version():
    parsed = parse_announce(build_announce("alice"))
    assert parsed["profile_version"] is None


def test_announce_with_long_name_and_profile_still_fits():
    data = build_announce("a" * 48, profile_version=12)
    assert len(data) <= 256
    assert parse_announce(data)["profile_version"] == 12


def test_announce_ignores_invalid_profile_version():
    parsed = parse_announce(build_announce("alice", profile_version=0))
    assert parsed["profile_version"] is None
    parsed = parse_announce(build_announce("alice", profile_version="3"))
    assert parsed["profile_version"] is None


def test_parse_announce_without_data_has_no_version():
    assert parse_announce(None)["profile_version"] is None
    assert parse_announce(b"")["profile_version"] is None


def test_profile_request_envelope_round_trip():
    assert parse_profile_request(encode_profile_request(2)) == {
        "known_version": 2
    }
    assert parse_profile_request(encode_profile_request()) == {
        "known_version": 0
    }
    assert parse_profile_request(decode(encode_profile_request(7))) == {
        "known_version": 7
    }


def test_profile_request_rejects_bad_payloads():
    with pytest.raises(Exception, match="must not be negative"):
        encode_profile_request(-1)
    with pytest.raises(Exception, match="expected a profile_request"):
        parse_profile_request(b'{"type": "ping"}')
