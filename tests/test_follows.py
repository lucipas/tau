"""Tests for signed follow lists (tau.follows)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import RNS

from tau.follows import (
    MAX_FOLLOW_LIST_AUTHORS,
    FollowListError,
    FollowListStore,
    build_follow_list,
    clean_authors,
    load_local_follow_list,
    normalize_follow_list,
    public_follow_list,
    save_local_follow_list,
    verify_follow_list,
)
from tau.protocol import (
    ProtocolError,
    encode_follows_request,
    parse_follows_request,
)


def make_identity() -> RNS.Identity:
    return RNS.Identity()


def author_hash(identity: RNS.Identity) -> str:
    return RNS.Destination.hash(identity, "tau", "node").hex()


def make_follow_list(identity=None, **overrides):
    identity = identity or make_identity()
    other_a = make_identity()
    other_b = make_identity()
    fields = {
        "authors": [author_hash(other_a), author_hash(other_b)],
        "version": 1,
        "updated_at": 1000.0,
    }
    fields.update(overrides)
    return build_follow_list(identity, **fields)


# -- document build / verify ----------------------------------------------


def test_build_and_verify_round_trip():
    identity = make_identity()
    doc = make_follow_list(identity)
    verified = verify_follow_list(doc)
    assert verified["author"] == author_hash(identity)
    assert verified["key"] == identity.get_public_key().hex()
    assert len(verified["authors"]) == 2
    assert verified["authors"] == sorted(verified["authors"])
    assert verified["version"] == 1
    assert verified["sig"] == doc["sig"]


def test_verify_rejects_tampered_authors():
    doc = make_follow_list()
    doc["authors"] = doc["authors"][:1]
    with pytest.raises(FollowListError, match="signature is invalid"):
        verify_follow_list(doc)


def test_verify_rejects_tampered_version():
    doc = make_follow_list()
    doc["version"] = 99
    with pytest.raises(FollowListError, match="signature is invalid"):
        verify_follow_list(doc)


def test_verify_rejects_wrong_author_key():
    identity = make_identity()
    other = make_identity()
    doc = make_follow_list(identity)
    # Re-sign the same body under a different key: the key no longer
    # derives the author the document claims.
    doc["key"] = other.get_public_key().hex()
    with pytest.raises(FollowListError, match="does not match the follow list author"):
        verify_follow_list(doc)


def test_verify_rejects_missing_signature():
    doc = make_follow_list()
    del doc["sig"]
    with pytest.raises(FollowListError, match="sig must be a hex string"):
        verify_follow_list(doc)


def test_verify_rejects_unsupported_version():
    doc = make_follow_list()
    doc["v"] = 99
    with pytest.raises(FollowListError, match="unsupported follow list version"):
        verify_follow_list(doc)


def test_verify_rejects_future_timestamp():
    doc = make_follow_list(updated_at=200000.0)
    with pytest.raises(FollowListError, match="too far in the future"):
        verify_follow_list(doc, now=1000.0)


def test_normalize_ignores_extra_fields():
    doc = make_follow_list()
    doc["future_field"] = "a newer version may add one"
    verified = verify_follow_list(doc)
    assert len(verified["authors"]) == 2
    assert "future_field" not in verified


def test_empty_follow_list_verifies():
    doc = make_follow_list(authors=[])
    verified = verify_follow_list(doc)
    assert verified["authors"] == []


# -- field cleaning --------------------------------------------------------


def test_clean_authors_sorts_and_dedupes():
    first = "aa" * 16
    second = "bb" * 16
    assert clean_authors([second, first, first]) == [first, second]


def test_clean_authors_accepts_empty_list():
    assert clean_authors([]) == []


def test_clean_authors_rejects_non_list():
    with pytest.raises(FollowListError, match="authors must be a list"):
        clean_authors("aa" * 16)


def test_clean_authors_rejects_short_hash():
    with pytest.raises(FollowListError, match="must be destination hashes"):
        clean_authors(["abc123"])


def test_clean_authors_rejects_non_hex():
    with pytest.raises(FollowListError, match="must be destination hashes"):
        clean_authors(["zz" * 16])


def test_clean_authors_rejects_overlong_list():
    with pytest.raises(FollowListError, match="more than 512 authors"):
        clean_authors(["aa" * 16] * (MAX_FOLLOW_LIST_AUTHORS + 1))


def test_normalize_rejects_version_zero():
    with pytest.raises(FollowListError, match="at least 1"):
        normalize_follow_list(
            {
                "author": "aa" * 16,
                "key": "bb" * 64,
                "authors": [],
                "version": 0,
                "updated_at": 1000.0,
            }
        )


# -- local store -----------------------------------------------------------


def test_save_and_load_local_follow_list(tmp_path):
    doc = make_follow_list()
    path = tmp_path / "follow-list.json"
    save_local_follow_list(path, doc)
    loaded = load_local_follow_list(path)
    assert loaded is not None
    assert loaded["authors"] == doc["authors"]
    assert loaded["version"] == 1


def test_load_local_follow_list_missing_is_none(tmp_path):
    assert load_local_follow_list(tmp_path / "nope.json") is None


def test_load_local_follow_list_invalid_is_none(tmp_path):
    path = tmp_path / "follow-list.json"
    path.write_text(
        json.dumps({"authors": ["not a signed list"]}), encoding="utf-8"
    )
    assert load_local_follow_list(path) is None


def test_public_follow_list_drops_key_and_signature():
    doc = make_follow_list()
    view = public_follow_list(doc)
    assert view is not None
    assert "key" not in view and "sig" not in view
    assert view["authors"] == doc["authors"]
    assert public_follow_list(None) is None


# -- follow list store -----------------------------------------------------


def test_store_keeps_newest_version_only():
    identity = make_identity()
    store = FollowListStore()
    old = make_follow_list(identity, version=1, updated_at=1000.0)
    new = make_follow_list(identity, version=2, updated_at=2000.0)
    assert store.store(old) is True
    assert store.store(new) is True
    assert store.store(old) is False
    kept = store.get(new["author"])
    assert kept is not None and kept["version"] == 2


def test_store_rejects_forged_follow_list():
    store = FollowListStore()
    doc = make_follow_list()
    doc["authors"] = []
    with pytest.raises(FollowListError, match="signature is invalid"):
        store.store(doc)
    assert len(store) == 0


def test_store_persists_and_reloads(tmp_path):
    identity = make_identity()
    path = tmp_path / "follow-lists.json"
    store = FollowListStore(path)
    store.store(make_follow_list(identity, version=3, updated_at=3000.0))
    store.save()
    reloaded = FollowListStore(path)
    assert len(reloaded) == 1
    kept = reloaded.get(author_hash(identity))
    assert kept is not None and kept["version"] == 3


def test_store_load_skips_invalid_entries(tmp_path):
    path = tmp_path / "follow-lists.json"
    good = make_follow_list(version=1, updated_at=1000.0)
    bad = dict(good)
    bad["authors"] = []
    path.write_text(json.dumps({"follow_lists": [bad, good]}), encoding="utf-8")
    store = FollowListStore(path)
    assert len(store) == 1
    assert store.get(good["author"]) is not None


def test_store_remove(tmp_path):
    store = FollowListStore()
    doc = make_follow_list()
    store.store(doc)
    assert store.remove(doc["author"]) is True
    assert store.remove(doc["author"]) is False
    assert store.get(doc["author"]) is None


# -- follows request wire format -------------------------------------------


def test_encode_decode_follows_request_round_trip():
    raw = encode_follows_request(3)
    assert parse_follows_request(raw) == {"known_version": 3}
    assert parse_follows_request(encode_follows_request()) == {"known_version": 0}


def test_parse_follows_request_rejects_wrong_type():
    with pytest.raises(ProtocolError, match="expected a follows_request payload"):
        parse_follows_request(b'{"type": "profile_request", "known_version": 0}')


def test_parse_follows_request_tolerates_bad_known_version():
    raw = json.dumps(
        {"type": "follows_request", "known_version": "three"}
    ).encode("utf-8")
    assert parse_follows_request(raw) == {"known_version": 0}


def test_encode_follows_request_rejects_negative_version():
    with pytest.raises(ProtocolError, match="must not be negative"):
        encode_follows_request(-1)


def test_encode_follows_request_rejects_non_integer():
    with pytest.raises(ProtocolError, match="must be an integer"):
        encode_follows_request("1")
