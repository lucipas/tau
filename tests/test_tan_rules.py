from __future__ import annotations

import time

import RNS
import pytest

from tau.cli import main
from tau.config import TauConfig
from tau.peers import Peer
from tau.protocol import APP_NAME, ASPECT, decode, encode
from tau.tangents.rules import RulesError, Ruleset, load_ruleset, save_ruleset
from tau.tangents.tan import (
    TAN_FEED_PATH,
    TAN_POST_PATH,
    TanApp,
    create_post,
    feed,
    follow,
    init_db,
)


@pytest.fixture()
def identities():
    return RNS.Identity(), RNS.Identity()


def author_of(identity):
    return RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()


def test_decide_matrix(identities):
    alice, bob = identities
    alice_hash, bob_hash = author_of(alice), author_of(bob)

    assert Ruleset().decide(bob_hash, "post") is True

    deny = Ruleset(default_allow=False)
    assert deny.decide(bob_hash, "post") is False
    assert deny.decide(bob_hash, "post", own_author=bob_hash) is True

    listed = Ruleset(default_allow=False, allow_authors=frozenset({bob_hash}))
    assert listed.decide(bob_hash, "post") is True
    assert listed.decide(alice_hash, "post") is False

    blocked = Ruleset(block_authors=frozenset({bob_hash}))
    assert blocked.decide(bob_hash, "post") is False

    # The block list wins over the allow list.
    both = Ruleset(
        allow_authors=frozenset({bob_hash}),
        block_authors=frozenset({bob_hash}),
    )
    assert both.decide(bob_hash, "post") is False

    posts_only = Ruleset(kinds=frozenset({"post"}))
    assert posts_only.decide(bob_hash, "post") is True
    assert posts_only.decide(bob_hash, "like") is False

    followed_only = Ruleset(default_allow=False, allow_followed=True)
    assert followed_only.decide(bob_hash, "post") is False
    assert (
        followed_only.decide(bob_hash, "post", followed=[bob_hash]) is True
    )


def test_ruleset_persistence_roundtrip(tmp_path, identities):
    _, bob = identities
    path = tmp_path / "tan-rules.json"
    assert load_ruleset(path) == Ruleset()

    ruleset = Ruleset(
        default_allow=False,
        allow_authors=frozenset({author_of(bob)}),
        allow_followed=True,
        kinds=frozenset({"post"}),
    )
    save_ruleset(path, ruleset)
    assert load_ruleset(path) == ruleset

    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(RulesError):
        load_ruleset(path)
    path.write_text('{"default": "sometimes"}', encoding="utf-8")
    with pytest.raises(RulesError):
        load_ruleset(path)


def _push(app, entry):
    response = app._handle_post(
        TAN_POST_PATH,
        encode({"post": entry, "ttl": 0}),
        b"id",
        None,
        time.time(),
    )
    return decode(response)


def test_handler_denies_unlisted_author_when_default_deny(identities, tmp_path):
    alice, bob = identities
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = TanApp(
        config, alice, name="alice", gossip=False,
        ruleset=Ruleset(default_allow=False),
    )
    post = create_post(
        init_db(tmp_path / "scratch.sqlite3"), bob, "stranger", name="bob", now=1000
    )
    payload = _push(app, post)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "not_admitted"
    conn = app.connect()
    try:
        assert feed(conn, all_authors=True) == []
    finally:
        conn.close()


def test_handler_admits_allowlisted_and_blocks_listed(identities, tmp_path):
    alice, bob = identities
    bob_hash = author_of(bob)
    post = create_post(
        init_db(tmp_path / "scratch.sqlite3"), bob, "hi", name="bob", now=1000
    )

    config = TauConfig.resolve(data_dir=tmp_path / "a", name="alice")
    app = TanApp(
        config, alice, name="alice", gossip=False,
        ruleset=Ruleset(
            default_allow=False, allow_authors=frozenset({bob_hash})
        ),
    )
    assert _push(app, post)["ok"] is True

    config = TauConfig.resolve(data_dir=tmp_path / "b", name="alice")
    app = TanApp(
        config, alice, name="alice", gossip=False,
        ruleset=Ruleset(block_authors=frozenset({bob_hash})),
    )
    payload = _push(app, post)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "not_admitted"


def test_handler_admits_own_author_and_followed(identities, tmp_path):
    alice, bob = identities
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = TanApp(
        config, alice, name="alice", gossip=False,
        ruleset=Ruleset(default_allow=False, allow_followed=True),
    )
    own = create_post(
        init_db(tmp_path / "s1.sqlite3"), alice, "mine", name="alice", now=1000
    )
    assert _push(app, own)["ok"] is True

    post = create_post(
        init_db(tmp_path / "s2.sqlite3"), bob, "followed", name="bob", now=1000
    )
    assert _push(app, post)["ok"] is False
    conn = app.connect()
    try:
        follow(conn, author_of(bob), name="bob")
    finally:
        conn.close()
    assert _push(app, post)["ok"] is True


class _FakeRegistry:
    def __init__(self, peer):
        self.peer = peer

    def all(self, max_age=None):
        return [self.peer]


class _FakeNode:
    def __init__(self, peer, source):
        self.registry = _FakeRegistry(peer)
        self.source = source

    def request(self, peer, path, payload, *, timeout=20.0):
        request = decode(payload)
        assert path == TAN_FEED_PATH
        return {
            "ok": True,
            "posts": feed(
                self.source,
                author=request["author"],
                after_seq=request.get("after_seq", 0),
                limit=request.get("limit", 20),
            ),
        }


def test_sync_skips_denied_authors(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    post = create_post(source, bob, "pulled", name="bob", now=1000)

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = TanApp(
        config, alice, name="alice", gossip=False,
        ruleset=Ruleset(block_authors=frozenset({author_of(bob)})),
    )
    peer = Peer(
        destination_hash=bytes.fromhex(post["author"]),
        name="bob",
        public_key=bob.get_public_key(),
        first_seen=1000.0,
        last_seen=1000.0,
    )
    node = _FakeNode(peer, source)
    app.node = node  # type: ignore[assignment]

    summary = app.sync(node, authors=[post["author"]], limit=20)
    assert summary["stored"] == 0
    assert summary["skipped"] == 1
    conn = app.connect()
    try:
        assert feed(conn, all_authors=True) == []
    finally:
        conn.close()


def test_broken_ruleset_file_fails_closed(identities, tmp_path):
    alice, bob = identities
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    config.tan_rules_path.write_text("{broken", encoding="utf-8")
    app = TanApp(config, alice, name="alice", gossip=False)
    post = create_post(
        init_db(tmp_path / "scratch.sqlite3"), bob, "hi", name="bob", now=1000
    )
    payload = _push(app, post)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "not_admitted"
    # Local creation is unaffected by the broken remote-admission file.
    record = app.create_post("still mine")
    assert record["text"] == "still mine"


def test_cli_rules_roundtrip(tmp_path):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(
            [
                "tan", "rules", "--data-dir", str(data_dir),
                "--default", "deny", "--followed-only", "--kinds", "post",
                "--json",
            ]
        )
        == 0
    )
    ruleset = load_ruleset(data_dir / "tan-rules.json")
    assert ruleset.default_allow is False
    assert ruleset.allow_followed is True
    assert ruleset.kinds == frozenset({"post"})
    assert main(["tan", "rules", "--data-dir", str(data_dir)]) == 0
