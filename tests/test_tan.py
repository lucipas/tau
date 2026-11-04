from __future__ import annotations

import json
import time

import RNS
import pytest

from tau.cli import main
from tau.config import TauConfig
from tau.peers import Peer
from tau.protocol import APP_NAME, ASPECT, decode, encode
from tau.tangents.tan import (
    HASH_FIELDS,
    TAN_FEED_PATH,
    TAN_POST_PATH,
    PostError,
    TanApp,
    TanError,
    create_post,
    feed,
    follow,
    following,
    hash,
    head,
    init_db,
    max_seq,
    store,
    store_post,
    timeline,
    unfollow,
    verify_post,
)


@pytest.fixture()
def identities():
    return RNS.Identity(), RNS.Identity()


def author_of(identity):
    return RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()


def test_hash_prefixes_the_previous_hash():
    post = {
        "v": 1,
        "author": "ab" * 16,
        "key": "cd" * 64,
        "name": "alice",
        "text": "hello",
        "created_at": 1000.0,
        "prev": "ef" * 32,
    }
    assert len(hash(post)) == 64
    assert hash(post) == hash(post, previous=post["prev"])
    other = dict(post, prev="00" * 32)
    assert hash(post) != hash(other)


def test_create_post_chains_and_verifies(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    first = create_post(conn, alice, "hello mesh", name="alice", now=1000)
    second = create_post(conn, alice, "second post", name="alice", now=1001)

    assert first["prev"] == ""
    assert second["prev"] == first["hash"]
    assert head(conn, first["author"]) == second["hash"]
    assert max_seq(conn, first["author"]) == 2
    assert verify_post(first)["hash"] == first["hash"]
    assert [post["text"] for post in feed(conn, author=first["author"])] == [
        "hello mesh",
        "second post",
    ]


def test_store_rejects_tampered_post(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "original", name="alice", now=1000)
    tampered = dict(post, text="edited")
    with pytest.raises(PostError, match="hash"):
        store(conn, tampered)


def test_store_rejects_a_key_from_another_identity(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "mine", name="alice", now=1000)
    forged = dict(post)
    forged["key"] = bob.get_public_key().hex()
    body = {field: forged[field] for field in HASH_FIELDS}
    forged["hash"] = hash({**body, "prev": forged["prev"]}, previous=forged["prev"])
    forged["sig"] = bob.sign(bytes.fromhex(forged["hash"])).hex()
    with pytest.raises(PostError, match="does not match"):
        store(conn, forged)


def test_duplicate_post_is_idempotent(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "once", name="alice", now=1000)
    record, is_new = store_post(conn, post)
    assert is_new is False
    assert record["hash"] == post["hash"]
    assert max_seq(conn, post["author"]) == 1


def test_out_of_order_posts_link_when_the_parent_arrives(identities, tmp_path):
    alice, _ = identities
    source = init_db(tmp_path / "source.sqlite3")
    first = create_post(source, alice, "first", name="alice", now=1000)
    second = create_post(source, alice, "second", name="alice", now=1001)

    destination = init_db(tmp_path / "destination.sqlite3")
    _, is_new = store_post(destination, second)
    assert is_new is True
    assert destination.execute(
        "SELECT linked, seq FROM posts WHERE hash = ?", (second["hash"],)
    ).fetchone()["linked"] == 0

    store_post(destination, first)
    row = destination.execute(
        "SELECT linked, seq FROM posts WHERE hash = ?", (second["hash"],)
    ).fetchone()
    assert row["linked"] == 1
    assert row["seq"] == 2
    assert head(destination, first["author"]) == second["hash"]


def test_follow_and_timeline(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    bob_post = create_post(conn, bob, "from bob", name="bob", now=1000)
    alice_post = create_post(conn, alice, "from alice", name="alice", now=1001)

    record = follow(conn, bob_post["author"], name="bob")
    assert record["name"] == "bob"
    assert following(conn)[0]["author"] == bob_post["author"]

    posts = timeline(conn, [bob_post["author"], alice_post["author"]])
    assert [post["text"] for post in posts] == ["from alice", "from bob"]
    assert unfollow(conn, bob_post["author"]) is True
    assert following(conn) == []
    assert timeline(conn, []) == []


def test_feed_paginates_by_sequence(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    for index in range(5):
        create_post(conn, alice, f"post {index}", name="alice", now=1000 + index)
    author = author_of(alice)
    page = feed(conn, author=author, after_seq=0, limit=2)
    assert [post["text"] for post in page] == ["post 0", "post 1"]
    page = feed(conn, author=author, after_seq=2, limit=2)
    assert [post["text"] for post in page] == ["post 2", "post 3"]


def test_app_handlers_store_and_serve_posts(identities, tmp_path):
    alice, bob = identities
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = TanApp(config, alice, name="alice", gossip=False)

    post = create_post(init_db(tmp_path / "scratch.sqlite3"), bob, "networked", name="bob", now=1000)
    response = app._handle_post(
        TAN_POST_PATH,
        encode({"post": post, "ttl": 0}),
        b"id",
        None,
        time.time(),
    )
    assert decode(response)["ok"] is True
    assert decode(response)["new"] is True

    response = app._handle_feed(
        TAN_FEED_PATH,
        encode({"author": post["author"], "after_seq": 0, "limit": 20}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert payload["ok"] is True
    assert [item["hash"] for item in payload["posts"]] == [post["hash"]]


class _FakeRegistry:
    def __init__(self, peer):
        self.peer = peer

    def all(self, max_age=None):
        return [self.peer]


class _FakeNode:
    def __init__(self, peer, source):
        self.registry = _FakeRegistry(peer)
        self.source = source
        self.requests = []

    def request(self, peer, path, payload, *, timeout=20.0):
        request = decode(payload)
        self.requests.append((path, request))
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


def test_sync_pulls_missing_posts(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    first = create_post(source, bob, "bob one", name="bob", now=1000)
    create_post(source, bob, "bob two", name="bob", now=1001)

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = TanApp(config, alice, name="alice", gossip=False)
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
    assert summary["stored"] == 2
    assert summary["errors"] == []
    assert [post["text"] for post in feed(app.connect(), author=first["author"])] == [
        "bob one",
        "bob two",
    ]


def test_cli_post_and_timeline(tmp_path):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(
            [
                "tan",
                "post",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                "hello from the cli",
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        posts = feed(conn, all_authors=True)
    finally:
        conn.close()
    assert len(posts) == 1
    assert posts[0]["text"] == "hello from the cli"
    assert posts[0]["name"] == "alice"
