from __future__ import annotations

import json
import time

import RNS
import pytest

from tau.cli import main
from tau.config import TauConfig
from tau.peers import Peer
from tau.protocol import APP_NAME, ASPECT, ProtocolError, decode, encode
from tau.tangents.tan import (
    HASH_FIELDS,
    TAN_EXPORT_FORMAT,
    TAN_FEED_PATH,
    TAN_POST_PATH,
    PostError,
    TanApp,
    TanError,
    annotate_replies,
    author_stats,
    create_delete,
    create_post,
    create_reaction,
    create_reply,
    deleted_targets,
    export_entries,
    feed,
    follow,
    following,
    get_post,
    hash,
    head,
    import_entries,
    init_db,
    like_counts,
    reaction_states,
    resolve_post,
    max_seq,
    search_posts,
    store,
    store_post,
    thread,
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


def test_verify_post_tolerates_none_returning_load(
    identities, tmp_path, load_public_key_without_return
):
    # RNS 1.4.1's load_public_key returns None even on success; post
    # verification (which rebuilds the author's identity from the
    # advertised key) must still work there.
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    first = create_post(conn, alice, "hello mesh", name="alice", now=1000)
    assert verify_post(first)["hash"] == first["hash"]


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


def test_export_import_roundtrip(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    try:
        first = create_post(source, alice, "first", name="alice", now=1000)
        second = create_post(source, alice, "second", name="alice", now=1001)
        create_reaction(
            source, alice, first["hash"], liked=True, name="alice", now=1002
        )
        create_delete(source, alice, second["hash"], name="alice", now=1003)
        create_post(source, bob, "bob's post", name="bob", now=1000)
        document = export_entries(source, author=author_of(alice))
        all_document = export_entries(source, all_authors=True)
    finally:
        source.close()
    assert document["format"] == TAN_EXPORT_FORMAT
    assert document["version"] == 1
    assert document["authors"] == [author_of(alice)]
    # Two posts plus the like and the delete on alice's chain; bob excluded.
    assert [entry["hash"] for entry in document["entries"]] == [
        first["hash"],
        second["hash"],
        document["entries"][2]["hash"],
        document["entries"][3]["hash"],
    ]
    assert [entry["kind"] for entry in document["entries"]] == [
        "post",
        "post",
        "like",
        "delete",
    ]
    assert all_document["authors"] == sorted([author_of(alice), author_of(bob)])
    assert len(all_document["entries"]) == 5

    target = init_db(tmp_path / "target.sqlite3")
    try:
        result = import_entries(target, document)
        assert result == {
            "imported": 4,
            "present": 0,
            "skipped": 0,
            "errors": [],
        }
        posts = feed(target, author=author_of(alice), posts_only=True)
        # "second" is deleted, so the posts-only feed hides it — but the
        # entry itself is still stored, keeping the chain verifiable.
        assert [post["text"] for post in posts] == ["first"]
        assert get_post(target, second["hash"]) is not None
        assert like_counts(target, [first["hash"]])[first["hash"]]["likes"] == 1
        # The imported delete still hides the second post from the timeline.
        shown = timeline(target, [author_of(alice)])
        assert [post["hash"] for post in shown] == [first["hash"]]
        assert feed(target, author=author_of(bob)) == []
        again = import_entries(target, document)
        assert again["imported"] == 0
        assert again["present"] == 4
        assert again["skipped"] == 0
    finally:
        target.close()


def test_export_empty_author(tmp_path):
    conn = init_db(tmp_path / "empty.sqlite3")
    try:
        document = export_entries(conn, author="0" * 32)
    finally:
        conn.close()
    assert document["entries"] == []
    assert document["authors"] == []


def test_import_skips_tampered_entry(identities, tmp_path):
    alice, _bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    try:
        first = create_post(source, alice, "original", name="alice", now=1000)
        second = create_post(source, alice, "untouched", name="alice", now=1001)
        document = export_entries(source, author=author_of(alice))
    finally:
        source.close()
    document["entries"][0]["text"] = "forged"
    target = init_db(tmp_path / "target.sqlite3")
    try:
        result = import_entries(target, document)
        assert result["imported"] == 1
        assert result["present"] == 0
        assert result["skipped"] == 1
        assert len(result["errors"]) == 1
        assert result["errors"][0]["hash"] == first["hash"]
        assert get_post(target, first["hash"]) is None
        # The untouched entry still imports even though its parent is missing.
        stored = get_post(target, second["hash"])
        assert stored is not None
        assert stored["linked"] is False
    finally:
        target.close()


def test_import_skips_bad_signature(identities, tmp_path):
    alice, _bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    try:
        post = create_post(source, alice, "signed", name="alice", now=1000)
        document = export_entries(source, author=author_of(alice))
    finally:
        source.close()
    entry = document["entries"][0]
    flipped = ("0" if entry["sig"][0] != "0" else "1") + entry["sig"][1:]
    entry["sig"] = flipped  # hash still matches; only the signature is bad
    target = init_db(tmp_path / "target.sqlite3")
    try:
        result = import_entries(target, document)
        assert result["imported"] == 0
        assert result["skipped"] == 1
        assert "signature is invalid" in result["errors"][0]["error"]
        assert get_post(target, post["hash"]) is None
    finally:
        target.close()


def test_import_skips_bad_entries_and_continues(identities, tmp_path):
    alice, _bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    try:
        create_post(source, alice, "one", name="alice", now=1000)
        create_post(source, alice, "two", name="alice", now=1001)
        third = create_post(source, alice, "three", name="alice", now=1002)
        document = export_entries(source, author=author_of(alice))
    finally:
        source.close()
    document["entries"][0]["text"] = ""  # ProtocolError from normalize_message
    document["entries"][1]["created_at"] = 10**400  # OverflowError in float()
    target = init_db(tmp_path / "target.sqlite3")
    try:
        result = import_entries(target, document)
        assert result["imported"] == 1
        assert result["skipped"] == 2
        assert len(result["errors"]) == 2
        assert get_post(target, third["hash"]) is not None
    finally:
        target.close()


def test_export_preserves_forked_head(identities, tmp_path):
    alice, _bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    try:
        parent = create_post(source, alice, "parent", name="alice", now=1000)
        # Two children of the same parent: a fork, as when one identity is
        # used on two cloned nodes. The first inserted becomes the head,
        # even though the second was created earlier.
        first_child = create_post(
            source,
            alice,
            "head branch",
            name="alice",
            now=1002,
            previous=parent["hash"],
        )
        second_child = create_post(
            source,
            alice,
            "other branch",
            name="alice",
            now=1001,
            previous=parent["hash"],
        )
        assert head(source, author_of(alice)) == first_child["hash"]
        document = export_entries(source, author=author_of(alice))
    finally:
        source.close()
    # The head sorts first among the same-sequence fork, ahead of the
    # earlier-created sibling.
    assert [entry["hash"] for entry in document["entries"]] == [
        parent["hash"],
        first_child["hash"],
        second_child["hash"],
    ]
    target = init_db(tmp_path / "target.sqlite3")
    try:
        result = import_entries(target, document)
        assert result["imported"] == 3
        assert head(target, author_of(alice)) == first_child["hash"]
    finally:
        target.close()


def test_export_orders_orphans_after_linked(identities, tmp_path):
    alice, _bob = identities
    conn = init_db(tmp_path / "x.sqlite3")
    try:
        first = create_post(conn, alice, "first", name="alice", now=1000)
        second = create_post(conn, alice, "second", name="alice", now=1001)
        orphan = create_post(
            conn, alice, "orphan", name="alice", now=1002, previous="f" * 64
        )
        assert get_post(conn, orphan["hash"])["linked"] is False
        document = export_entries(conn, author=author_of(alice))
    finally:
        conn.close()
    assert [entry["hash"] for entry in document["entries"]] == [
        first["hash"],
        second["hash"],
        orphan["hash"],
    ]


def test_import_rejects_bad_envelope(tmp_path):
    conn = init_db(tmp_path / "empty.sqlite3")
    try:
        with pytest.raises(TanError, match="must contain a JSON object"):
            import_entries(conn, ["not", "a", "dict"])
        with pytest.raises(TanError, match="unsupported export format"):
            import_entries(conn, {"format": "nope", "version": 1, "entries": []})
        with pytest.raises(TanError, match="unsupported export version"):
            import_entries(
                conn, {"format": TAN_EXPORT_FORMAT, "version": 99, "entries": []}
            )
        with pytest.raises(TanError, match="no entries list"):
            import_entries(conn, {"format": TAN_EXPORT_FORMAT, "version": 1})
    finally:
        conn.close()


def test_cli_tan_export_import(tmp_path):
    alice_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(alice_dir), "--name", "alice"]) == 0
    assert (
        main(
            [
                "tan",
                "post",
                "--data-dir",
                str(alice_dir),
                "--no-publish",
                "--json",
                "hello backup",
            ]
        )
        == 0
    )
    export_path = tmp_path / "tan-export.json"
    assert (
        main(
            [
                "tan",
                "export",
                "--data-dir",
                str(alice_dir),
                "--output",
                str(export_path),
            ]
        )
        == 0
    )
    assert export_path.exists()
    assert json.loads(export_path.read_text(encoding="utf-8"))["entries"][0][
        "text"
    ] == "hello backup"

    carol_dir = tmp_path / "carol"
    assert main(["init", "--data-dir", str(carol_dir), "--name", "carol"]) == 0
    assert (
        main(["tan", "import", "--data-dir", str(carol_dir), str(export_path)]) == 0
    )
    conn = init_db(carol_dir / "tan.sqlite3")
    try:
        posts = feed(conn, all_authors=True)
    finally:
        conn.close()
    assert len(posts) == 1
    assert posts[0]["text"] == "hello backup"
    assert posts[0]["name"] == "alice"

    # Carol has alice's posts cached but alice is not in her peer
    # registry, so export-by-name must fall back to the cached posts.
    cached_path = tmp_path / "cached-export.json"
    assert (
        main(
            [
                "tan",
                "export",
                "--data-dir",
                str(carol_dir),
                "--author",
                "alice",
                "--output",
                str(cached_path),
            ]
        )
        == 0
    )
    assert json.loads(cached_path.read_text(encoding="utf-8"))["entries"][0][
        "text"
    ] == "hello backup"


def test_cli_tan_export_import_errors(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    # --author and --all conflict: a clean CLI error, not a traceback.
    assert (
        main(
            [
                "tan",
                "export",
                "--data-dir",
                str(data_dir),
                "--author",
                "alice",
                "--all",
            ]
        )
        == 1
    )
    assert "cannot be combined" in capsys.readouterr().err
    bad_path = tmp_path / "bad.json"
    bad_path.write_text("not json", encoding="utf-8")
    assert (
        main(["tan", "import", "--data-dir", str(data_dir), str(bad_path)]) == 1
    )
    assert "not valid JSON" in capsys.readouterr().err


def test_cli_search(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    for text in ("mesh notes here", "something else"):
        assert (
            main(
                [
                    "tan",
                    "post",
                    "--data-dir",
                    str(data_dir),
                    "--no-publish",
                    "--json",
                    text,
                ]
            )
            == 0
        )
        capsys.readouterr()

    assert (
        main(
            [
                "tan",
                "search",
                "--data-dir",
                str(data_dir),
                "--json",
                "mesh",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["posts"][0]["text"] == "mesh notes here"

    # No match is a clean empty result, not an error.
    assert (
        main(
            [
                "tan",
                "search",
                "--data-dir",
                str(data_dir),
                "--json",
                "nope",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["count"] == 0


def test_reaction_chains_verifies_and_counts(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "likeable", name="alice", now=1000)
    bob_author = author_of(bob)

    like = create_reaction(conn, bob, post["hash"], liked=True, name="bob", now=1001)
    assert like["kind"] == "like"
    assert like["target"] == post["hash"]
    assert like["text"] == ""
    assert verify_post(like)["hash"] == like["hash"]
    assert head(conn, bob_author) == like["hash"]

    counts = like_counts(conn, [post["hash"]], viewer=bob_author)
    assert counts[post["hash"]] == {"likes": 1, "liked": True}

    # A second like from the same reactor still counts once.
    create_reaction(conn, bob, post["hash"], liked=True, name="bob", now=1002)
    assert like_counts(conn, [post["hash"]])[post["hash"]]["likes"] == 1

    # An unlike supersedes the like (latest chain entry wins).
    create_reaction(conn, bob, post["hash"], liked=False, name="bob", now=1003)
    counts = like_counts(conn, [post["hash"]], viewer=bob_author)
    assert counts[post["hash"]] == {"likes": 0, "liked": False}
    assert reaction_states(conn, [post["hash"]])[post["hash"]] == {
        bob_author: "unlike"
    }


def test_reaction_rejects_tampering(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "target practice", name="alice", now=1000)
    other = create_post(conn, alice, "decoy", name="alice", now=1001)
    like = create_reaction(conn, bob, post["hash"], liked=True, name="bob", now=1002)

    with pytest.raises(PostError):
        verify_post(dict(like, target=other["hash"]))
    with pytest.raises(PostError):
        verify_post(dict(post, target=post["hash"]))
    with pytest.raises(PostError):
        verify_post(dict(like, text="sneaky"))


def test_timeline_hides_reactions_but_feed_carries_them(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "shown", name="alice", now=1000)
    create_reaction(conn, bob, post["hash"], liked=True, name="bob", now=1001)

    # Sync feed carries the whole chain, reactions included.
    chain = feed(conn, author=author_of(bob))
    assert [entry["kind"] for entry in chain] == ["like"]

    items = timeline(conn, [post["author"]], viewer=author_of(bob))
    assert [item["hash"] for item in items] == [post["hash"]]
    assert items[0]["likes"] == 1
    assert items[0]["liked"] is True

    everything = timeline(conn, include_all=True)
    assert [item["hash"] for item in everything] == [post["hash"]]


def test_reaction_survives_store_and_reserve(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    post = create_post(source, alice, "portable", name="alice", now=1000)
    like = create_reaction(source, bob, post["hash"], liked=True, name="bob", now=1001)

    replica = init_db(tmp_path / "replica.sqlite3")
    store(replica, post)
    record, is_new = store_post(replica, like)
    assert is_new is True
    assert record["kind"] == "like"
    assert like_counts(replica, [post["hash"]])[post["hash"]]["likes"] == 1
    assert resolve_post(replica, post["hash"][:12])["hash"] == post["hash"]


def test_reaction_handler_roundtrip(identities, tmp_path):
    alice, bob = identities
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = TanApp(config, alice, name="alice", gossip=False)

    post = create_post(
        init_db(tmp_path / "scratch.sqlite3"), alice, "hosted", name="alice", now=1000
    )
    like = create_reaction(
        init_db(tmp_path / "scratch2.sqlite3"), bob, post["hash"],
        liked=True, name="bob", now=1001,
    )
    for entry in (post, like):
        response = app._handle_post(
            TAN_POST_PATH,
            encode({"post": entry, "ttl": 0}),
            b"id",
            None,
            time.time(),
        )
        assert decode(response)["ok"] is True

    conn = init_db(config.social_db_path)
    try:
        assert like_counts(conn, [post["hash"]])[post["hash"]]["likes"] == 1
    finally:
        conn.close()


def test_delete_hides_own_post_from_timeline_and_feed(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "regret this", name="alice", now=1000)
    other = create_post(conn, alice, "keeping this", name="alice", now=1001)
    create_reaction(conn, bob, post["hash"], liked=True, name="bob", now=1002)

    assert [item["hash"] for item in timeline(conn, [post["author"]])] == [
        other["hash"],
        post["hash"],
    ]

    delete = create_delete(conn, alice, post["hash"], name="alice", now=1003)
    assert delete["kind"] == "delete"
    assert delete["target"] == post["hash"]
    assert delete["text"] == ""
    assert verify_post(delete)["hash"] == delete["hash"]
    assert head(conn, post["author"]) == delete["hash"]
    assert deleted_targets(conn) == {post["hash"]}

    # Display surfaces hide the post (and the like on it goes with it).
    assert [item["hash"] for item in timeline(conn, [post["author"]])] == [
        other["hash"]
    ]
    assert [item["hash"] for item in timeline(conn, include_all=True)] == [
        other["hash"]
    ]
    assert [item["hash"] for item in feed(conn, posts_only=True)] == [
        other["hash"]
    ]
    assert [
        item["hash"]
        for item in feed(conn, author=post["author"], posts_only=True)
    ] == [other["hash"]]
    # The sync feed still carries the whole chain, delete included.
    chain = feed(conn, author=post["author"])
    assert [entry["kind"] for entry in chain] == ["post", "post", "delete"]

    with pytest.raises(TanError, match="has been deleted"):
        resolve_post(conn, post["hash"])

    # A second delete fails at the lowest layer, not just in the CLI/API.
    with pytest.raises(TanError, match="has been deleted"):
        create_delete(conn, alice, post["hash"], name="alice", now=1004)


def test_search_matches_substring_case_insensitively(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "hello mesh world", name="alice", now=1000)
    create_post(conn, bob, "HELLO from bob", name="bob", now=1001)
    create_post(conn, alice, "unrelated", name="alice", now=1002)

    results = search_posts(conn, "hello")
    assert [post["text"] for post in results] == [
        "HELLO from bob",
        "hello mesh world",
    ]
    assert [post["text"] for post in search_posts(conn, "MESH")] == [
        "hello mesh world"
    ]
    assert search_posts(conn, "nothing matches") == []




def test_search_folds_unicode_case(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "Äpfel und Öl", name="alice", now=1000)
    create_post(conn, alice, "plain apples", name="alice", now=1001)

    assert [post["text"] for post in search_posts(conn, "äpfel")] == [
        "Äpfel und Öl"
    ]
    assert [post["text"] for post in search_posts(conn, "ÖL")] == ["Äpfel und Öl"]
    assert search_posts(conn, "ÄPFEL") == search_posts(conn, "äpfel")
def test_search_treats_wildcards_literally(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "100% sure", name="alice", now=1000)
    create_post(conn, alice, "under_score", name="alice", now=1001)
    create_post(conn, alice, "back\\slash", name="alice", now=1002)
    create_post(conn, alice, "100x sure", name="alice", now=1003)

    assert [post["text"] for post in search_posts(conn, "100%")] == ["100% sure"]
    assert [post["text"] for post in search_posts(conn, "under_score")] == [
        "under_score"
    ]
    assert [post["text"] for post in search_posts(conn, "back\\slash")] == [
        "back\\slash"
    ]


def test_search_excludes_deleted_reactions_and_unlinked(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    kept = create_post(conn, alice, "keep this mesh post", name="alice", now=1000)
    doomed = create_post(conn, alice, "mesh regret", name="alice", now=1001)
    create_reaction(conn, bob, kept["hash"], liked=True, name="bob", now=1002)
    create_delete(conn, alice, doomed["hash"], name="alice", now=1003)

    results = search_posts(conn, "mesh")
    assert [post["text"] for post in results] == ["keep this mesh post"]
    # The surviving post carries its like annotation, like the timeline.
    assert results[0]["likes"] == 1

    # An unlinked post (parent not yet stored) never matches.
    orphan = create_post(conn, bob, "mesh orphan", name="bob", now=1004)
    conn.execute("UPDATE posts SET linked = 0 WHERE hash = ?", (orphan["hash"],))
    conn.commit()
    assert [post["text"] for post in search_posts(conn, "mesh")] == [
        "keep this mesh post"
    ]


def test_search_filters_by_author_and_limit(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    for index in range(3):
        create_post(conn, alice, f"mesh {index}", name="alice", now=1000 + index)
    create_post(conn, bob, "mesh from bob", name="bob", now=1003)

    author = author_of(alice)
    assert [post["text"] for post in search_posts(conn, "mesh", author=author)] == [
        "mesh 2",
        "mesh 1",
        "mesh 0",
    ]
    assert [post["text"] for post in search_posts(conn, "mesh", limit=2)] == [
        "mesh from bob",
        "mesh 2",
    ]
    with pytest.raises(TanError, match="author must be"):
        search_posts(conn, "mesh", author="not-a-hash")


def test_search_rejects_empty_query(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "hello", name="alice", now=1000)
    for query in ("", "   "):
        with pytest.raises(TanError, match="search text must not be empty"):
            search_posts(conn, query)
def test_timeline_marks_own_posts_for_viewer(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    mine = create_post(conn, alice, "mine", name="alice", now=1000)
    theirs = create_post(conn, bob, "theirs", name="bob", now=1001)

    # The viewer sees their own posts marked and everyone else's unmarked.
    items = timeline(conn, include_all=True, viewer=author_of(alice))
    by_hash = {item["hash"]: item for item in items}
    assert by_hash[mine["hash"]]["own"] is True
    assert by_hash[theirs["hash"]]["own"] is False

    # With no viewer there is nothing to mark as own.
    items = timeline(conn, include_all=True)
    assert all(item["own"] is False for item in items)

    # A different viewer flips the marks.
    items = timeline(conn, include_all=True, viewer=author_of(bob))
    by_hash = {item["hash"]: item for item in items}
    assert by_hash[mine["hash"]]["own"] is False
    assert by_hash[theirs["hash"]]["own"] is True


def test_author_stats_excludes_deleted_posts(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    old = create_post(conn, alice, "old", name="alice", now=1000)
    new = create_post(conn, alice, "new", name="alice", now=1001)
    stats = author_stats(conn, old["author"])
    assert stats["posts"] == 2
    assert stats["last_post"] == 1001

    create_delete(conn, alice, new["hash"], name="alice", now=1002)
    stats = author_stats(conn, old["author"])
    assert stats["posts"] == 1
    assert stats["last_post"] == 1000
    # The chain sequence is untouched: the tombstone is still an entry.
    assert stats["seq"] == 3


def test_sync_hides_deleted_posts(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    first = create_post(source, bob, "bob one", name="bob", now=1000)
    second = create_post(source, bob, "bob two", name="bob", now=1001)
    create_delete(source, bob, second["hash"], name="bob", now=1002)

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
    assert summary["stored"] == 3
    assert summary["errors"] == []

    conn = app.connect()
    try:
        assert deleted_targets(conn) == {second["hash"]}
        assert [item["text"] for item in timeline(conn, [first["author"]])] == [
            "bob one"
        ]
    finally:
        conn.close()


def test_delete_by_non_author_is_inert(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "mine", name="alice", now=1000)

    with pytest.raises(TanError, match="did not author"):
        create_delete(conn, bob, post["hash"], name="bob", now=1001)

    # A forged delete smuggled in through the store path is kept (chains
    # stay complete) but is inert: the post stays visible.
    scratch = init_db(tmp_path / "scratch.sqlite3")
    forged = create_delete(scratch, bob, post["hash"], name="bob", now=1001)
    record, is_new = store_post(conn, forged)
    assert is_new is True
    assert record["kind"] == "delete"
    assert [item["hash"] for item in timeline(conn, [post["author"]])] == [
        post["hash"]
    ]
    assert deleted_targets(conn) == set()
    assert resolve_post(conn, post["hash"])["hash"] == post["hash"]

    # Only posts and replies can be deleted, even by their own author.
    with pytest.raises(TanError, match="only posts and replies can be deleted"):
        create_delete(conn, bob, forged["hash"], name="bob", now=1002)


def test_delete_arriving_before_its_post_applies_on_arrival(
    identities, tmp_path
):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    scratch = init_db(tmp_path / "scratch.sqlite3")
    post = create_post(scratch, alice, "late arrival", name="alice", now=1000)
    delete = create_delete(scratch, alice, post["hash"], name="alice", now=1001)

    # The delete lands first; with no post cached it is inert.
    store_post(conn, delete)
    assert deleted_targets(conn) == set()

    # Once the post itself is synced in, the earlier delete hides it.
    store_post(conn, post)
    assert deleted_targets(conn) == {post["hash"]}
    assert timeline(conn, [post["author"]]) == []


def test_delete_rejects_tampering(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "target practice", name="alice", now=1000)
    other = create_post(conn, alice, "decoy", name="alice", now=1001)
    delete = create_delete(conn, alice, post["hash"], name="alice", now=1002)

    with pytest.raises(PostError):
        verify_post(dict(delete, target=other["hash"]))
    with pytest.raises(PostError):
        verify_post(dict(delete, text="sneaky"))
    with pytest.raises(PostError):
        verify_post(dict(post, kind="delete", target=post["hash"]))


def test_delete_handler_roundtrip(identities, tmp_path):
    alice, bob = identities
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = TanApp(config, alice, name="alice", gossip=False)

    post = create_post(
        init_db(tmp_path / "scratch.sqlite3"), alice, "hosted", name="alice", now=1000
    )
    delete = create_delete(
        init_db(tmp_path / "scratch2.sqlite3"), alice, post["hash"],
        name="alice", now=1001,
    )
    for entry in (post, delete):
        response = app._handle_post(
            TAN_POST_PATH,
            encode({"post": entry, "ttl": 0}),
            b"id",
            None,
            time.time(),
        )
        assert decode(response)["ok"] is True

    conn = init_db(config.social_db_path)
    try:
        assert deleted_targets(conn) == {post["hash"]}
        assert timeline(conn, [post["author"]]) == []
    finally:
        conn.close()


def test_cli_delete(tmp_path):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(
            [
                "tan", "post", "--data-dir", str(data_dir),
                "--no-publish", "--json", "cli delete me",
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "tan", "post", "--data-dir", str(data_dir),
                "--no-publish", "--json", "cli keeper",
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        posts = feed(conn, all_authors=True)
    finally:
        conn.close()
    doomed = next(item for item in posts if item["text"] == "cli delete me")

    assert (
        main(
            [
                "tan", "delete", "--data-dir", str(data_dir),
                "--no-publish", "--json", doomed["hash"][:16],
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        remaining = timeline(conn, include_all=True)
        assert [item["text"] for item in remaining] == ["cli keeper"]
        with pytest.raises(TanError, match="has been deleted"):
            resolve_post(conn, doomed["hash"])
    finally:
        conn.close()

    # Deleting the same post again fails instead of writing a second tombstone.
    assert (
        main(
            [
                "tan", "delete", "--data-dir", str(data_dir),
                "--no-publish", "--json", doomed["hash"],
            ]
        )
        != 0
    )

    # Liking a deleted post fails too.
    assert (
        main(
            [
                "tan", "like", "--data-dir", str(data_dir),
                "--no-publish", "--json", doomed["hash"],
            ]
        )
        != 0
    )


def test_cli_delete_rejects_foreign_post(tmp_path):
    data_dir = tmp_path / "bob"
    assert main(["init", "--data-dir", str(data_dir), "--name", "bob"]) == 0
    # Plant another author's post in bob's cache, as a sync would.
    alice = RNS.Identity()
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        post = create_post(conn, alice, "not bob's", name="alice", now=1000)
    finally:
        conn.close()

    assert (
        main(
            [
                "tan", "delete", "--data-dir", str(data_dir),
                "--no-publish", "--json", post["hash"],
            ]
        )
        != 0
    )


def test_cli_delete_accepts_uncached_full_hash(tmp_path):
    data_dir = tmp_path / "carol"
    assert main(["init", "--data-dir", str(data_dir), "--name", "carol"]) == 0
    # Carol knows the hash of a post she made on another device; this
    # node has never seen it, but the tombstone still applies on arrival.
    unknown = "ab" * 32
    assert (
        main(
            [
                "tan", "delete", "--data-dir", str(data_dir),
                "--no-publish", "--json", unknown,
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        entries = feed(conn)
    finally:
        conn.close()
    assert len(entries) == 1
    assert entries[0]["kind"] == "delete"
    assert entries[0]["target"] == unknown


def test_pre_reaction_database_migrates(tmp_path):
    import sqlite3

    path = tmp_path / "old.sqlite3"
    raw = sqlite3.connect(path)
    raw.execute(
        "CREATE TABLE posts ("
        "v INTEGER NOT NULL DEFAULT 1, hash TEXT PRIMARY KEY, "
        "author TEXT NOT NULL, prev TEXT NOT NULL DEFAULT '', "
        "key TEXT NOT NULL, name TEXT NOT NULL DEFAULT '', "
        "text TEXT NOT NULL, created_at REAL NOT NULL, "
        "received_at REAL NOT NULL, sig TEXT NOT NULL, "
        "seq INTEGER NOT NULL DEFAULT 0, linked INTEGER NOT NULL DEFAULT 0)"
    )
    raw.commit()
    raw.close()

    conn = init_db(path)
    try:
        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(posts)")
        }
        assert {"kind", "target"} <= columns
    finally:
        conn.close()


def test_cli_like_and_unlike(tmp_path):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(
            [
                "tan", "post", "--data-dir", str(data_dir),
                "--no-publish", "--json", "cli reactions",
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        post = feed(conn, all_authors=True)[0]
    finally:
        conn.close()

    assert (
        main(
            [
                "tan", "like", "--data-dir", str(data_dir),
                "--no-publish", "--json", post["hash"][:16],
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        assert like_counts(conn, [post["hash"]])[post["hash"]]["likes"] == 1
    finally:
        conn.close()

    assert (
        main(
            [
                "tan", "unlike", "--data-dir", str(data_dir),
                "--no-publish", "--json", post["hash"],
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        assert like_counts(conn, [post["hash"]])[post["hash"]]["likes"] == 0
    finally:
        conn.close()


def test_create_post_rejects_oversize_encoded_post(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    # 2,000 four-byte characters pass the character limit but encode to
    # more than the 8 KiB request budget: the post could never be
    # published, so it must not be stored as chain head either.
    with pytest.raises(PostError):
        create_post(conn, alice, "\U0001F600" * 2000, name="alice", now=1000)
    assert head(conn, author_of(alice)) is None
    assert feed(conn, author=author_of(alice)) == []

    # The chain is not poisoned: a normal post still becomes the head.
    post = create_post(conn, alice, "fits fine", name="alice", now=1001)
    assert head(conn, author_of(alice)) == post["hash"]


def test_multibyte_post_under_budget_is_accepted(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "\U0001F600" * 1500, name="alice", now=1000)
    assert verify_post(post)["hash"] == post["hash"]
    # And its real publish envelope fits the request budget.
    payload = encode({"post": post, "ttl": 3}, max_bytes=8 * 1024)
    assert len(payload) <= 8 * 1024


def test_store_rejects_oversize_post_from_a_peer(identities, tmp_path):
    alice, _ = identities
    body = {
        "v": 1,
        "author": author_of(alice),
        "key": alice.get_public_key().hex(),
        "name": "alice",
        "text": "\U0001F600" * 2000,
        "created_at": 1000.0,
        "prev": "",
    }
    digest = hash(body, previous="")
    mesg = dict(body, hash=digest, sig=alice.sign(bytes.fromhex(digest)).hex())
    with pytest.raises(PostError):
        verify_post(mesg)
    conn = init_db(tmp_path / "tan.sqlite3")
    with pytest.raises(PostError):
        store_post(conn, mesg)


def test_reply_chains_verifies_and_annotates(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "original thought", name="alice", now=1000)

    reply = create_reply(
        conn, bob, post["hash"], "seconded", name="bob", now=1001
    )
    assert reply["kind"] == "reply"
    assert reply["target"] == post["hash"]
    assert reply["text"] == "seconded"
    assert verify_post(reply)["hash"] == reply["hash"]
    assert head(conn, author_of(bob)) == reply["hash"]

    # The sync feed carries the reply; display surfaces show it with
    # its parent attached.
    chain = feed(conn, author=author_of(bob))
    assert [entry["kind"] for entry in chain] == ["reply"]
    items = timeline(conn, [post["author"], author_of(bob)])
    assert [item["hash"] for item in items] == [reply["hash"], post["hash"]]
    assert items[0]["reply_to"] == {
        "hash": post["hash"],
        "author": post["author"],
        "name": "alice",
        "text": "original thought",
        "deleted": False,
    }
    assert "reply_to" not in items[1]
    assert [item["hash"] for item in feed(conn, posts_only=True)] == [
        reply["hash"],
        post["hash"],
    ]
    assert [item["hash"] for item in timeline(conn, include_all=True)] == [
        reply["hash"],
        post["hash"],
    ]


def test_reply_rejects_tampering(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "target practice", name="alice", now=1000)
    other = create_post(conn, alice, "decoy", name="alice", now=1001)
    reply = create_reply(
        conn, bob, post["hash"], "agreed", name="bob", now=1002
    )

    # The parent link is signed: swapping it invalidates the signature.
    with pytest.raises(PostError):
        verify_post(dict(reply, target=other["hash"]))
    with pytest.raises(PostError):
        verify_post(dict(reply, text="sneaky"))
    # A reply without a parent, and an empty reply, are rejected.
    with pytest.raises(PostError):
        verify_post(dict(reply, target=""))
    with pytest.raises(ProtocolError, match="must not be empty"):
        create_reply(conn, bob, post["hash"], "   ", name="bob", now=1003)


def test_reply_to_uncached_parent_is_rejected_when_authoring(identities, tmp_path):
    _, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    with pytest.raises(TanError, match="unknown post"):
        create_reply(conn, bob, "cd" * 32, "early reply", name="bob", now=1000)


def test_reply_to_unlinked_parent_is_rejected(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    first = create_post(source, alice, "first", name="alice", now=1000)
    second = create_post(source, alice, "second", name="alice", now=1001)

    # Only the second post arrives: it is cached but unlinked, so
    # replying to it would produce a reply with no parent context.
    destination = init_db(tmp_path / "destination.sqlite3")
    store_post(destination, second)
    with pytest.raises(TanError, match="has not linked yet"):
        create_reply(
            destination, bob, second["hash"], "too early", name="bob", now=1002
        )

    # Once the chain links up, the same reply is accepted.
    store_post(destination, first)
    reply = create_reply(
        destination, bob, second["hash"], "in full", name="bob", now=1003
    )
    assert reply["kind"] == "reply"


def test_reply_to_parent_not_yet_synced_shows_without_snippet(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    post = create_post(source, alice, "original thought", name="alice", now=1000)
    reply = create_reply(
        source, bob, post["hash"], "early reply", name="bob", now=1001
    )

    # The reply arrives (via gossip or sync) before its parent: the
    # store path keeps it, and the timeline shows it without a parent
    # snippet until the parent is cached.
    replica = init_db(tmp_path / "replica.sqlite3")
    record, is_new = store_post(replica, reply)
    assert is_new is True
    items = timeline(replica, [author_of(bob)])
    assert [item["hash"] for item in items] == [reply["hash"]]
    assert items[0]["reply_to"] is None

    store(replica, post)
    items = timeline(replica, [author_of(bob)])
    assert items[0]["reply_to"]["hash"] == post["hash"]


def test_reply_to_reaction_parent_shows_no_snippet(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "parent post", name="alice", now=1000)
    like = create_reaction(conn, bob, post["hash"], liked=True, name="bob", now=1001)
    body = {
        "v": 1,
        "author": author_of(bob),
        "key": bob.get_public_key().hex(),
        "name": "bob",
        "text": "reply to a like",
        "created_at": 1002.0,
        "prev": like["hash"],
        "kind": "reply",
        "target": like["hash"],
    }
    digest = hash(body, previous=like["hash"])
    mesg = dict(body, hash=digest, sig=bob.sign(bytes.fromhex(digest)).hex())
    record, is_new = store_post(conn, mesg)
    assert is_new is True
    items = timeline(conn, [post["author"], author_of(bob)])
    assert items[0]["hash"] == record["hash"]
    assert items[0]["reply_to"] is None


def test_reply_to_deleted_post_or_reaction_is_rejected(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "doomed", name="alice", now=1000)
    like = create_reaction(conn, bob, post["hash"], liked=True, name="bob", now=1001)

    with pytest.raises(TanError, match="only posts and replies"):
        create_reply(conn, bob, like["hash"], "reply to a like", name="bob", now=1002)

    create_delete(conn, alice, post["hash"], name="alice", now=1003)
    with pytest.raises(TanError, match="has been deleted"):
        create_reply(conn, bob, post["hash"], "too late", name="bob", now=1004)


def test_reply_can_be_liked_deleted_and_replied_to(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "thread starter", name="alice", now=1000)
    reply = create_reply(
        conn, bob, post["hash"], "first reply", name="bob", now=1001
    )
    nested = create_reply(
        conn, alice, reply["hash"], "reply to the reply", name="alice", now=1002
    )

    # Replies are posts for liking and resolving: a nested reply's
    # parent annotation points at the reply, not the top-level post.
    items = timeline(conn, [post["author"], author_of(bob)])
    assert [item["hash"] for item in items] == [
        nested["hash"],
        reply["hash"],
        post["hash"],
    ]
    assert items[0]["reply_to"]["hash"] == reply["hash"]
    assert resolve_post(conn, reply["hash"][:12])["hash"] == reply["hash"]

    create_reaction(conn, alice, reply["hash"], liked=True, name="alice", now=1003)
    assert like_counts(conn, [reply["hash"]])[reply["hash"]]["likes"] == 1

    # Deleting the reply hides it (and the nested reply's parent
    # annotation reports the deletion without resurfacing the text).
    create_delete(conn, bob, reply["hash"], name="bob", now=1004)
    assert deleted_targets(conn) == {reply["hash"]}
    items = timeline(conn, [post["author"], author_of(bob)])
    assert [item["hash"] for item in items] == [nested["hash"], post["hash"]]
    assert items[0]["reply_to"] == {
        "hash": reply["hash"],
        "author": reply["author"],
        "name": "bob",
        "text": "",
        "deleted": True,
    }
    with pytest.raises(TanError, match="has been deleted"):
        create_reply(conn, alice, reply["hash"], "too late", name="alice", now=1005)


def test_thread_returns_root_and_nested_replies(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "thread starter", name="alice", now=1000)
    first = create_reply(
        conn, bob, post["hash"], "first reply", name="bob", now=1001
    )
    nested = create_reply(
        conn, alice, first["hash"], "reply to the reply", name="alice", now=1002
    )
    second = create_reply(
        conn, bob, post["hash"], "second reply", name="bob", now=1003
    )
    create_reaction(conn, bob, post["hash"], liked=True, name="bob", now=1004)

    # Depth-first, siblings oldest first: a reply, then its own
    # replies, then the next sibling.  Reactions never appear as
    # entries; the like only shows up as a count on the root.
    items = thread(conn, post["hash"])
    assert [item["hash"] for item in items] == [
        post["hash"],
        first["hash"],
        nested["hash"],
        second["hash"],
    ]
    assert [item["depth"] for item in items] == [0, 1, 2, 1]
    assert [item["text"] for item in items] == [
        "thread starter",
        "first reply",
        "reply to the reply",
        "second reply",
    ]
    assert items[0]["likes"] == 1
    assert items[1]["likes"] == 0

    # Naming any entry in the thread resolves to the same root.
    assert [item["hash"] for item in thread(conn, nested["hash"][:12])] == [
        post["hash"],
        first["hash"],
        nested["hash"],
        second["hash"],
    ]
    assert [item["hash"] for item in thread(conn, second["hash"])] == [
        post["hash"],
        first["hash"],
        nested["hash"],
        second["hash"],
    ]


def test_thread_hides_deleted_reply_subtree(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "thread starter", name="alice", now=1000)
    first = create_reply(
        conn, bob, post["hash"], "first reply", name="bob", now=1001
    )
    nested = create_reply(
        conn, alice, first["hash"], "reply to the reply", name="alice", now=1002
    )
    second = create_reply(
        conn, bob, post["hash"], "second reply", name="bob", now=1003
    )

    # Deleting the first reply hides it and the nested reply that
    # hangs off it, rather than re-parenting the nested reply.
    create_delete(conn, bob, first["hash"], name="bob", now=1004)
    items = thread(conn, post["hash"])
    assert [item["hash"] for item in items] == [post["hash"], second["hash"]]
    assert [item["depth"] for item in items] == [0, 1]
    assert nested["hash"] not in [item["hash"] for item in items]


def test_thread_rejects_unknown_and_deleted_references(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "thread starter", name="alice", now=1000)

    with pytest.raises(TanError, match="unknown post"):
        thread(conn, "cd" * 32)
    create_delete(conn, alice, post["hash"], name="alice", now=1001)
    with pytest.raises(TanError, match="has been deleted"):
        thread(conn, post["hash"])


def _insert_stub(conn, digest, *, author, linked, kind="post", target=""):
    """Store a bare entry row for states the public API cannot produce."""
    conn.execute(
        "INSERT INTO posts (hash, author, key, text, created_at, received_at, "
        "sig, seq, linked, kind, target) "
        "VALUES (?, ?, '', 'stub', 1000, 1000, '', 1, ?, ?, ?)",
        (digest, author, 1 if linked else 0, kind, target),
    )
    conn.commit()


def test_thread_rejects_ambiguous_prefix(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    shared = "ab" * 4
    _insert_stub(conn, shared + "1" * 56, author=author_of(alice), linked=True)
    _insert_stub(conn, shared + "2" * 56, author=author_of(alice), linked=True)

    with pytest.raises(TanError, match="2 posts match prefix"):
        thread(conn, shared)


def test_thread_rejects_unlinked_entry(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    digest = "cd" * 32
    _insert_stub(conn, digest, author=author_of(alice), linked=False)

    with pytest.raises(TanError, match="has not linked yet"):
        thread(conn, digest)


def test_thread_rejects_reply_beneath_deleted_ancestor(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "thread starter", name="alice", now=1000)
    first = create_reply(
        conn, bob, post["hash"], "first reply", name="bob", now=1001
    )
    nested = create_reply(
        conn, alice, first["hash"], "reply to the reply", name="alice", now=1002
    )
    create_delete(conn, bob, first["hash"], name="bob", now=1003)

    # Naming the reply directly must not show it as a standalone thread.
    with pytest.raises(TanError, match="has been deleted"):
        thread(conn, nested["hash"])


def test_thread_rejects_reply_beneath_deleted_unlinked_ancestor(
    identities, tmp_path
):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    parent = "aa" * 32
    delete = "bb" * 32
    reply = "cc" * 32
    # The parent is cached but its chain never linked up, and its
    # author deleted it: the deletion must win over the unlinked
    # shortcut, or the reply below it re-roots as its own thread.
    _insert_stub(conn, parent, author=author_of(alice), linked=False)
    _insert_stub(
        conn, delete, author=author_of(alice), linked=True,
        kind="delete", target=parent,
    )
    _insert_stub(
        conn, reply, author=author_of(bob), linked=True,
        kind="reply", target=parent,
    )

    with pytest.raises(TanError, match="has been deleted"):
        thread(conn, reply)


def test_thread_batches_wide_generations(identities, tmp_path, monkeypatch):
    # A batch size of 2 forces every generation and the reaction lookup
    # to be split across several queries.
    monkeypatch.setattr("tau.tangents.tan._SQL_BATCH", 2)
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "thread starter", name="alice", now=1000)
    replies = []
    children = []
    for index in range(5):
        reply = create_reply(
            conn, bob, post["hash"], f"reply {index}", name="bob", now=1001 + index
        )
        replies.append(reply)
        children.append(
            create_reply(
                conn, alice, reply["hash"], f"child {index}", name="alice",
                now=1010 + index,
            )
        )
    create_reaction(conn, bob, replies[3]["hash"], liked=True, name="bob", now=1020)

    items = thread(conn, post["hash"])
    expected = [post["hash"]]
    for reply, child in zip(replies, children):
        expected.extend([reply["hash"], child["hash"]])
    assert [item["hash"] for item in items] == expected
    assert [item["depth"] for item in items] == [0] + [1, 2] * 5
    likes = {item["hash"]: item["likes"] for item in items}
    assert likes[replies[3]["hash"]] == 1
    assert sum(likes.values()) == 1


def test_cli_tan_thread(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(
            [
                "tan", "post", "--data-dir", str(data_dir),
                "--no-publish", "--json", "cli thread starter",
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        post = feed(conn, all_authors=True)[0]
    finally:
        conn.close()
    assert (
        main(
            [
                "tan", "post", "--data-dir", str(data_dir),
                "--no-publish", "--json",
                "--reply-to", post["hash"][:16], "cli reply",
            ]
        )
        == 0
    )
    capsys.readouterr()

    # JSON output carries the nested thread with depths.
    assert (
        main(
            [
                "tan", "thread", "--data-dir", str(data_dir),
                "--json", post["hash"][:16],
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 2
    assert [entry["text"] for entry in payload["posts"]] == [
        "cli thread starter",
        "cli reply",
    ]
    assert [entry["depth"] for entry in payload["posts"]] == [0, 1]

    # Text output indents the reply under its parent.
    assert (
        main(
            ["tan", "thread", "--data-dir", str(data_dir), post["hash"][:16]]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "cli thread starter" in out
    assert "  cli reply" in out

    # An unknown reference is a clean CLI error, not a traceback.
    assert (
        main(["tan", "thread", "--data-dir", str(data_dir), "cd" * 32]) != 0
    )
    assert "tau: error: unknown post" in capsys.readouterr().err


def test_cli_post_reply_to(tmp_path):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(
            [
                "tan", "post", "--data-dir", str(data_dir),
                "--no-publish", "--json", "cli thread starter",
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        post = feed(conn, all_authors=True)[0]
    finally:
        conn.close()

    assert (
        main(
            [
                "tan", "post", "--data-dir", str(data_dir),
                "--no-publish", "--json",
                "--reply-to", post["hash"][:16], "cli reply",
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        items = timeline(conn, include_all=True)
        assert [item["text"] for item in items] == ["cli reply", "cli thread starter"]
        assert items[0]["kind"] == "reply"
        assert items[0]["reply_to"]["hash"] == post["hash"]
    finally:
        conn.close()

    # Replying to an unknown post fails instead of writing a dangling reply.
    assert (
        main(
            [
                "tan", "post", "--data-dir", str(data_dir),
                "--no-publish", "--json",
                "--reply-to", "cd" * 32, "nowhere",
            ]
        )
        != 0
    )


def test_cli_timeline_shows_reply_parent(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(
            [
                "tan", "post", "--data-dir", str(data_dir),
                "--no-publish", "--json", "cli thread starter",
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        post = feed(conn, all_authors=True)[0]
    finally:
        conn.close()
    assert (
        main(
            [
                "tan", "post", "--data-dir", str(data_dir),
                "--no-publish", "--json",
                "--reply-to", post["hash"][:16], "cli reply",
            ]
        )
        == 0
    )

    # Text output names the parent instead of showing a standalone post.
    assert (
        main(
            [
                "tan", "timeline", "--data-dir", str(data_dir),
                "--author", post["author"],
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "in reply to alice: cli thread starter" in out
    assert "cli reply" in out

    # JSON output carries the reply_to annotation on the author path.
    assert (
        main(
            [
                "tan", "timeline", "--data-dir", str(data_dir),
                "--author", post["author"], "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    reply = next(item for item in payload["posts"] if item["kind"] == "reply")
    assert reply["reply_to"]["hash"] == post["hash"]
    assert reply["reply_to"]["text"] == "cli thread starter"


# -- blocked authors are hidden from display surfaces ----------------------

def test_timeline_excludes_blocked_authors(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "from alice", name="alice", now=1000)
    other = create_post(conn, bob, "from bob", name="bob", now=1001)
    blocked = [post["author"]]

    assert [item["hash"] for item in timeline(conn, [post["author"], other["author"]], exclude_authors=blocked)] == [other["hash"]]
    assert [item["hash"] for item in timeline(conn, include_all=True, exclude_authors=blocked)] == [other["hash"]]
    # The blocked entries stay stored: the exclusion is display-only.
    assert [item["hash"] for item in timeline(conn, include_all=True)] == [other["hash"], post["hash"]]


def test_feed_excludes_blocked_authors(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "from alice", name="alice", now=1000)
    other = create_post(conn, bob, "from bob", name="bob", now=1001)
    blocked = [post["author"]]

    assert [item["hash"] for item in feed(conn, posts_only=True, exclude_authors=blocked)] == [other["hash"]]
    assert feed(conn, author=post["author"], posts_only=True, exclude_authors=blocked) == []
    assert [item["hash"] for item in feed(conn, author=other["author"], posts_only=True, exclude_authors=blocked)] == [other["hash"]]


def test_search_excludes_blocked_authors(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "shared word", name="alice", now=1000)
    other = create_post(conn, bob, "shared word", name="bob", now=1001)
    blocked = [author_of(alice)]

    assert [item["hash"] for item in search_posts(conn, "shared", exclude_authors=blocked)] == [other["hash"]]
    assert search_posts(conn, "shared", author=author_of(alice), exclude_authors=blocked) == []


def test_reply_snippet_blanks_blocked_parent(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "secret words", name="alice", now=1000)
    reply = create_reply(conn, bob, post["hash"], "answering", name="bob", now=1001)
    blocked = [post["author"]]

    items = timeline(conn, [reply["author"]], exclude_authors=blocked)
    assert [item["hash"] for item in items] == [reply["hash"]]
    parent = items[0]["reply_to"]
    assert parent["hash"] == post["hash"]
    assert parent["text"] == ""
    assert parent["author"] == ""
    assert parent["name"] == ""
    assert parent["blocked"] is True
    assert "secret words" not in json.dumps(items)


def test_thread_hides_blocked_subtree(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    root = create_post(conn, bob, "root", name="bob", now=1000)
    reply = create_reply(conn, alice, root["hash"], "from alice", name="alice", now=1001)
    nested = create_reply(conn, bob, reply["hash"], "under alice", name="bob", now=1002)
    blocked = [reply["author"]]

    # The blocked reply and everything beneath it stays hidden.
    items = thread(conn, root["hash"], exclude_authors=blocked)
    assert [item["hash"] for item in items] == [root["hash"]]

    # Naming a blocked post, or a reply beneath one, refuses outright.
    with pytest.raises(TanError, match="blocked author"):
        thread(conn, reply["hash"], exclude_authors=blocked)
    with pytest.raises(TanError, match="blocked author"):
        thread(conn, nested["hash"], exclude_authors=blocked)


def test_exclude_all_hides_everything(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "from alice", name="alice", now=1000)
    create_post(conn, bob, "from bob", name="bob", now=1001)

    assert timeline(conn, include_all=True, exclude_all=True) == []
    assert feed(conn, posts_only=True, exclude_all=True) == []
    assert search_posts(conn, "from", exclude_all=True) == []
    with pytest.raises(TanError, match="block-list is unusable"):
        thread(conn, post["hash"], exclude_all=True)
