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
    create_quote,
    create_reaction,
    create_reply,
    feed,
    follow,
    init_db,
    max_seq,
)


@pytest.fixture()
def identities():
    return RNS.Identity(), RNS.Identity()


def author_of(identity):
    return RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()


def test_decide_matrix(identities):
    alice, bob = identities
    alice_hash, bob_hash = author_of(alice), author_of(bob)

    assert Ruleset().decide(bob_hash) is True

    deny = Ruleset(default_allow=False)
    assert deny.decide(bob_hash) is False
    assert deny.decide(bob_hash, own_author=bob_hash) is True

    listed = Ruleset(default_allow=False, allow_authors=frozenset({bob_hash}))
    assert listed.decide(bob_hash) is True
    assert listed.decide(alice_hash) is False

    blocked = Ruleset(block_authors=frozenset({bob_hash}))
    assert blocked.decide(bob_hash) is False

    # The block list wins over the allow list.
    both = Ruleset(
        allow_authors=frozenset({bob_hash}),
        block_authors=frozenset({bob_hash}),
    )
    assert both.decide(bob_hash) is False

    # Kinds never gate admission — a refused kind would punch a hole
    # in the author's hash chain. They only gate display.
    posts_only = Ruleset(kinds=frozenset({"post"}))
    assert posts_only.decide(bob_hash) is True
    assert posts_only.displays("post") is True
    assert posts_only.displays("like") is False
    assert posts_only.displays("unlike") is False
    assert Ruleset().displays("like") is True

    followed_only = Ruleset(default_allow=False, allow_followed=True)
    assert followed_only.decide(bob_hash) is False
    assert followed_only.decide(bob_hash, followed=[bob_hash]) is True


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


def test_handler_admits_reaction_despite_display_kinds(identities, tmp_path):
    # Regression (PR #13 review): admission used to deny kinds outside
    # the display set, so a skipped reaction orphaned every later
    # entry on the author's chain. Kinds are display-only now.
    alice, bob = identities
    scratch = init_db(tmp_path / "scratch.sqlite3")
    post = create_post(scratch, bob, "hi", name="bob", now=1000)
    like = create_reaction(
        scratch, bob, post["hash"], liked=True, name="bob", now=1001
    )

    config = TauConfig.resolve(data_dir=tmp_path / "node", name="alice")
    app = TanApp(
        config, alice, name="alice", gossip=False,
        ruleset=Ruleset(kinds=frozenset({"post"})),
    )
    assert _push(app, post)["ok"] is True
    assert _push(app, like)["ok"] is True
    conn = app.connect()
    try:
        assert max_seq(conn, post["author"]) == 2
    finally:
        conn.close()


def test_sync_keeps_chain_complete_across_display_kinds(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    first = create_post(source, bob, "first", name="bob", now=1000)
    create_reaction(source, bob, first["hash"], liked=True, name="bob", now=1001)
    create_post(source, bob, "second", name="bob", now=1002)

    config = TauConfig.resolve(data_dir=tmp_path / "node", name="alice")
    app = TanApp(
        config, alice, name="alice", gossip=False,
        ruleset=Ruleset(kinds=frozenset({"post"})),
    )
    peer = Peer(
        destination_hash=bytes.fromhex(first["author"]),
        name="bob",
        public_key=bob.get_public_key(),
        first_seen=1000.0,
        last_seen=1000.0,
    )
    node = _FakeNode(peer, source)
    app.node = node  # type: ignore[assignment]

    summary = app.sync(node, authors=[first["author"]], limit=20)
    assert summary["stored"] == 3
    assert summary["skipped"] == 0
    conn = app.connect()
    try:
        assert max_seq(conn, first["author"]) == 3
    finally:
        conn.close()
    texts = {post["text"] for post in app.timeline(authors=[first["author"]])}
    assert texts == {"first", "second"}


def test_timeline_applies_display_kinds(identities, tmp_path):
    alice, bob = identities
    scratch = init_db(tmp_path / "scratch.sqlite3")
    post = create_post(scratch, bob, "hi", name="bob", now=1000)
    like = create_reaction(
        scratch, bob, post["hash"], liked=True, name="bob", now=1001
    )
    bob_hash = author_of(bob)

    def build(dirname, ruleset):
        config = TauConfig.resolve(data_dir=tmp_path / dirname, name="alice")
        app = TanApp(
            config, alice, name="alice", gossip=False, ruleset=ruleset
        )
        assert _push(app, post)["ok"] is True
        assert _push(app, like)["ok"] is True
        return app

    # Reactions hidden: the post shows, but with no like annotation.
    posts_hidden = build("a", Ruleset(kinds=frozenset({"post"})))
    shown = posts_hidden.timeline(authors=[bob_hash])
    assert [entry["text"] for entry in shown] == ["hi"]
    assert "likes" not in shown[0]
    assert "liked" not in shown[0]

    # Everything displayed: the like count is annotated as usual.
    posts_shown = build("b", Ruleset())
    shown = posts_shown.timeline(authors=[bob_hash])
    assert shown[0]["likes"] == 1

    # Posts hidden: the timeline is empty even though both entries
    # are stored and linked.
    posts_only_reactions = build("c", Ruleset(kinds=frozenset({"like"})))
    assert posts_only_reactions.timeline(authors=[bob_hash]) == []
    conn = posts_only_reactions.connect()
    try:
        assert max_seq(conn, bob_hash) == 2
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


def test_display_kinds_cover_replies(identities, tmp_path):
    alice, bob = identities
    scratch = init_db(tmp_path / "scratch.sqlite3")
    post = create_post(scratch, bob, "hi", name="bob", now=1000)
    reply = create_reply(
        scratch, bob, post["hash"], "hello back", name="bob", now=1001
    )
    bob_hash = author_of(bob)

    def build(dirname, ruleset):
        config = TauConfig.resolve(data_dir=tmp_path / dirname, name="alice")
        app = TanApp(
            config, alice, name="alice", gossip=False, ruleset=ruleset
        )
        assert _push(app, post)["ok"] is True
        assert _push(app, reply)["ok"] is True
        return app

    # Replies hidden: only the top-level post shows, but the reply is
    # still admitted and stored, keeping the chain complete.
    hidden = build("a", Ruleset(kinds=frozenset({"post"})))
    assert [entry["text"] for entry in hidden.timeline(authors=[bob_hash])] == ["hi"]
    conn = hidden.connect()
    try:
        assert max_seq(conn, bob_hash) == 2
    finally:
        conn.close()

    # Replies displayed alongside posts.
    shown = build("b", Ruleset(kinds=frozenset({"post", "reply"})))
    assert [entry["text"] for entry in shown.timeline(authors=[bob_hash])] == [
        "hello back",
        "hi",
    ]

    # "reply" is a known kind for ruleset files; anything else is rejected.
    with pytest.raises(RulesError, match="unknown entry kinds"):
        Ruleset.from_dict({"kinds": ["post", "smash"]})
    assert Ruleset.from_dict({"kinds": ["reply"]}).displays("reply") is True


def test_display_kinds_cover_quotes(identities, tmp_path):
    alice, bob = identities
    scratch = init_db(tmp_path / "scratch.sqlite3")
    post = create_post(scratch, bob, "hi", name="bob", now=1000)
    quote = create_quote(
        scratch, bob, post["hash"], "worth sharing", name="bob", now=1001
    )
    bob_hash = author_of(bob)

    def build(dirname, ruleset):
        config = TauConfig.resolve(data_dir=tmp_path / dirname, name="alice")
        app = TanApp(
            config, alice, name="alice", gossip=False, ruleset=ruleset
        )
        assert _push(app, post)["ok"] is True
        assert _push(app, quote)["ok"] is True
        return app

    # Quotes hidden: only the top-level post shows, without its quote
    # count, but the quote is still admitted and stored, keeping the
    # chain complete.
    hidden = build("a", Ruleset(kinds=frozenset({"post"})))
    shown = hidden.timeline(authors=[bob_hash])
    assert [entry["text"] for entry in shown] == ["hi"]
    assert "quotes" not in shown[0]
    conn = hidden.connect()
    try:
        assert max_seq(conn, bob_hash) == 2
    finally:
        conn.close()

    # Quotes displayed alongside posts, with the count annotated.
    shown = build("b", Ruleset(kinds=frozenset({"post", "quote"})))
    entries = shown.timeline(authors=[bob_hash])
    assert [entry["text"] for entry in entries] == ["worth sharing", "hi"]
    assert entries[1]["quotes"] == 1

    # "quote" is a known kind for ruleset files.
    assert Ruleset.from_dict({"kinds": ["quote"]}).displays("quote") is True
