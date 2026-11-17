"""Tests for signed personal mesh pages (tau.pages)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import RNS

from tau.pages import (
    MAX_PAGE_BYTES,
    PageError,
    PageStore,
    build_page,
    clean_markdown,
    clean_title,
    load_local_page,
    normalize_page,
    public_page,
    save_local_page,
    title_from_markdown,
    verify_page,
)
from tau.protocol import (
    ProtocolError,
    encode_page_request,
    parse_page_request,
)


def make_identity() -> RNS.Identity:
    return RNS.Identity()


def make_page(identity=None, **overrides):
    identity = identity or make_identity()
    fields = {
        "title": "Mesh meetup",
        "markdown": "# Mesh meetup\n\nSaturday, 2pm, Rock Springs Park.\n",
        "version": 1,
        "updated_at": 1000.0,
    }
    fields.update(overrides)
    return build_page(identity, **fields)


# -- document build / verify ----------------------------------------------


def test_build_and_verify_round_trip():
    identity = make_identity()
    doc = make_page(identity)
    verified = verify_page(doc)
    assert verified["author"] == RNS.Destination.hash(
        identity, "tau", "node"
    ).hex()
    assert verified["key"] == identity.get_public_key().hex()
    assert verified["title"] == "Mesh meetup"
    assert verified["markdown"].startswith("# Mesh meetup")
    assert verified["version"] == 1
    assert verified["sig"] == doc["sig"]


def test_verify_rejects_tampered_markdown():
    doc = make_page()
    doc["markdown"] = "# Mesh meetup\n\nMoved to Sunday.\n"
    with pytest.raises(PageError, match="signature is invalid"):
        verify_page(doc)


def test_verify_rejects_tampered_title():
    doc = make_page()
    doc["title"] = "someone else's page"
    with pytest.raises(PageError, match="signature is invalid"):
        verify_page(doc)


def test_verify_rejects_wrong_author_key():
    identity = make_identity()
    other = make_identity()
    doc = make_page(identity)
    # Re-sign the same body under a different key: the key no longer
    # derives the author the document claims.
    doc["key"] = other.get_public_key().hex()
    with pytest.raises(PageError, match="does not match the page author"):
        verify_page(doc)


def test_verify_rejects_missing_signature():
    doc = make_page()
    del doc["sig"]
    with pytest.raises(PageError, match="sig must be a hex string"):
        verify_page(doc)


def test_verify_rejects_unsupported_version():
    doc = make_page()
    doc["v"] = 99
    with pytest.raises(PageError, match="unsupported page version"):
        verify_page(doc)


def test_verify_rejects_future_timestamp():
    doc = make_page(updated_at=200000.0)
    with pytest.raises(PageError, match="too far in the future"):
        verify_page(doc, now=1000.0)


def test_normalize_ignores_extra_fields():
    doc = make_page()
    doc["future_field"] = "a newer version may add one"
    verified = verify_page(doc)
    assert verified["title"] == "Mesh meetup"
    assert "future_field" not in verified


# -- field cleaning --------------------------------------------------------


def test_clean_title_collapses_whitespace():
    assert clean_title("  Mesh   meetup  ") == "Mesh meetup"
    assert clean_title(None) == ""
    assert clean_title("   ") == ""


def test_clean_title_rejects_long_title():
    with pytest.raises(PageError, match="longer than 200 characters"):
        clean_title("x" * 201)


def test_clean_title_rejects_non_string():
    with pytest.raises(PageError, match="title must be a string"):
        clean_title(42)


def test_clean_markdown_rejects_blank():
    with pytest.raises(PageError, match="must not be blank"):
        clean_markdown("   \n\n")


def test_clean_markdown_rejects_oversize():
    with pytest.raises(PageError, match="larger than 8192 bytes"):
        clean_markdown("# Big\n\n" + "x" * MAX_PAGE_BYTES)


def test_clean_markdown_rejects_nul():
    with pytest.raises(PageError, match="must not contain NUL"):
        clean_markdown("hello\x00world")


def test_clean_markdown_keeps_verbatim_whitespace():
    text = "# T\n\n  indented\n\ntrailing   \n"
    assert clean_markdown(text) == text


def test_title_from_markdown_takes_first_h1():
    assert title_from_markdown("# Meetup\n\nbody\n") == "Meetup"
    assert title_from_markdown("intro\n\n# Later heading\n") == "Later heading"
    assert title_from_markdown("no heading here\n") == ""
    assert title_from_markdown("## Sub only\n") == ""


# -- local store -----------------------------------------------------------


def test_save_and_load_local_page(tmp_path):
    doc = make_page()
    path = tmp_path / "page.json"
    save_local_page(path, doc)
    loaded = load_local_page(path)
    assert loaded is not None
    assert loaded["title"] == "Mesh meetup"
    assert loaded["version"] == 1


def test_load_local_page_missing_is_none(tmp_path):
    assert load_local_page(tmp_path / "nope.json") is None


def test_load_local_page_invalid_is_none(tmp_path):
    path = tmp_path / "page.json"
    path.write_text(json.dumps({"title": "not a signed page"}), encoding="utf-8")
    assert load_local_page(path) is None


def test_public_page_drops_key_and_signature():
    doc = make_page()
    view = public_page(doc)
    assert view is not None
    assert "key" not in view and "sig" not in view
    assert view["title"] == "Mesh meetup"
    assert view["markdown"] == doc["markdown"]
    assert public_page(None) is None


# -- page store ------------------------------------------------------------


def test_store_keeps_newest_version_only():
    identity = make_identity()
    store = PageStore()
    old = make_page(identity, version=1, updated_at=1000.0)
    new = make_page(identity, version=2, updated_at=2000.0)
    assert store.store(old) is True
    assert store.store(new) is True
    assert store.store(old) is False
    kept = store.get(new["author"])
    assert kept is not None and kept["version"] == 2


def test_store_rejects_forged_page():
    store = PageStore()
    doc = make_page()
    doc["markdown"] = "forged"
    with pytest.raises(PageError, match="signature is invalid"):
        store.store(doc)
    assert len(store) == 0


def test_store_persists_and_reloads(tmp_path):
    identity = make_identity()
    path = tmp_path / "pages.json"
    store = PageStore(path)
    store.store(make_page(identity, version=3, updated_at=3000.0))
    store.save()
    reloaded = PageStore(path)
    assert len(reloaded) == 1
    kept = reloaded.get(
        RNS.Destination.hash(identity, "tau", "node").hex()
    )
    assert kept is not None and kept["version"] == 3


def test_store_load_skips_invalid_entries(tmp_path):
    path = tmp_path / "pages.json"
    good = make_page(version=1, updated_at=1000.0)
    bad = dict(good)
    bad["markdown"] = "tampered"
    path.write_text(json.dumps({"pages": [bad, good]}), encoding="utf-8")
    store = PageStore(path)
    assert len(store) == 1
    assert store.get(good["author"]) is not None


def test_store_remove(tmp_path):
    store = PageStore()
    doc = make_page()
    store.store(doc)
    assert store.remove(doc["author"]) is True
    assert store.remove(doc["author"]) is False
    assert store.get(doc["author"]) is None


# -- page request wire format ----------------------------------------------


def test_encode_decode_page_request_round_trip():
    raw = encode_page_request(3)
    assert parse_page_request(raw) == {"known_version": 3}
    assert parse_page_request(encode_page_request()) == {"known_version": 0}


def test_parse_page_request_rejects_wrong_type():
    with pytest.raises(ProtocolError, match="expected a page_request payload"):
        parse_page_request(b'{"type": "profile_request", "known_version": 0}')


def test_parse_page_request_tolerates_bad_known_version():
    raw = json.dumps(
        {"type": "page_request", "known_version": "three"}
    ).encode("utf-8")
    assert parse_page_request(raw) == {"known_version": 0}


def test_encode_page_request_rejects_negative_version():
    with pytest.raises(ProtocolError, match="must not be negative"):
        encode_page_request(-1)


def test_encode_page_request_rejects_non_integer():
    with pytest.raises(ProtocolError, match="must be an integer"):
        encode_page_request("1")
