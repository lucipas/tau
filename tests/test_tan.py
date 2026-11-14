from __future__ import annotations

import json
import sqlite3
import time

import RNS
import pytest

from tau.cli import main
from tau.config import TauConfig
from tau.identity import destination_hash, load_identity
from tau.peers import Peer
from tau.protocol import APP_NAME, ASPECT, ProtocolError, decode, encode
from tau.tangents import tan as tan_module
from tau.tangents.rules import Ruleset
from tau.tangents.tan import (
    HASH_FIELDS,
    TAN_EXPORT_FORMAT,
    TAN_FEED_PATH,
    TAN_POST_PATH,
    TAN_VERSION,
    PostError,
    TanApp,
    TanError,
    annotate_edits,
    annotate_pinned,
    annotate_quote_counts,
    annotate_quotes,
    annotate_reactions,
    annotate_replies,
    annotated_pinned_posts,
    annotated_saved_posts,
    author_stats,
    author_timeline,
    create_delete,
    create_edit,
    create_post,
    create_quote,
    create_reaction,
    create_reply,
    create_repost,
    clean_tag,
    deleted_targets,
    edit_states,
    export_entries,
    extract_tags,
    feed,
    follow,
    following,
    get_post,
    hash,
    head,
    hidden_posts,
    hide_post,
    import_entries,
    init_db,
    is_expired,
    like_counts,
    likers,
    list_authors,
    mute_keyword,
    muted_keywords,
    post_likers,
    quote_counts,
    reaction_states,
    repost_counts,
    resolve_author,
    resolve_post,
    resolve_muted_name,
    max_seq,
    mentions,
    mute,
    muted,
    muted_author_set,
    open_readonly_db,
    pin_post,
    pinned_posts,
    save_post,
    saved_posts,
    search_posts,
    store,
    store_post,
    SUGGEST_FOLLOWED_WEIGHT,
    suggest_authors,
    tags,
    thread,
    timeline,
    top_posts,
    unfollow,
    unhide_post,
    unmute,
    unmute_keyword,
    unpin_post,
    unsave_post,
    verify_post,
    verify_store,
    without_muted_keywords,
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


def test_concurrent_create_post_does_not_fork_the_chain(identities, tmp_path):
    # Two writers racing the head read used to append siblings with the
    # same seq — a fork.  The head read and the append are one atomic
    # step now, so concurrent creates (separate connections, as the
    # web, CLI and scheduled publisher each open) form one chain.
    import threading

    alice, _ = identities
    db_path = tmp_path / "tan.sqlite3"
    init_db(db_path).close()
    errors = []

    def create_many(tag, start):
        try:
            conn = init_db(db_path)
            try:
                for i in range(10):
                    create_post(conn, alice, f"{tag}-{i}", name="alice", now=1000 + start + i)
            finally:
                conn.close()
        except Exception as exc:  # noqa: BLE001 - collected and asserted below
            errors.append(exc)

    threads = [
        threading.Thread(target=create_many, args=("a", 0)),
        threading.Thread(target=create_many, args=("b", 100)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    conn = init_db(db_path)
    rows = conn.execute("SELECT prev, seq, linked FROM posts").fetchall()
    assert len(rows) == 20
    assert sorted(int(row["seq"]) for row in rows) == list(range(1, 21))
    assert len({str(row["prev"]) for row in rows if str(row["prev"])}) == 19
    assert all(bool(row["linked"]) for row in rows)
    assert max_seq(conn, author_of(alice)) == 20


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
    assert (
        destination.execute(
            "SELECT linked, seq FROM posts WHERE hash = ?", (second["hash"],)
        ).fetchone()["linked"]
        == 0
    )

    store_post(destination, first)
    row = destination.execute(
        "SELECT linked, seq FROM posts WHERE hash = ?", (second["hash"],)
    ).fetchone()
    assert row["linked"] == 1
    assert row["seq"] == 2
    assert head(destination, first["author"]) == second["hash"]


def test_verify_store_accepts_a_clean_store(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "from alice", name="alice", now=1000)
    create_post(conn, alice, "again", name="alice", now=1001)
    create_post(conn, bob, "from bob", name="bob", now=1002)

    report = verify_store(conn)

    assert report["ok"] is True
    assert report["entries"] == 3
    assert report["authors"] == 2
    assert report["unlinked"] == 0
    assert report["problems"] == []


def test_verify_store_accepts_an_empty_store(tmp_path):
    conn = init_db(tmp_path / "tan.sqlite3")

    assert verify_store(conn) == {
        "entries": 0,
        "authors": 0,
        "unlinked": 0,
        "problems": [],
        "ok": True,
    }


def test_verify_store_counts_unlinked_entries_without_failing(
    identities, tmp_path
):
    alice, _ = identities
    source = init_db(tmp_path / "source.sqlite3")
    create_post(source, alice, "first", name="alice", now=1000)
    second = create_post(source, alice, "second", name="alice", now=1001)

    conn = init_db(tmp_path / "tan.sqlite3")
    store_post(conn, second)

    report = verify_store(conn)
    assert report["ok"] is True
    assert report["entries"] == 1
    assert report["unlinked"] == 1
    assert report["problems"] == []


def test_verify_store_accepts_an_unlinked_entry_with_an_unlinked_parent(
    identities, tmp_path
):
    alice, _ = identities
    source = init_db(tmp_path / "source.sqlite3")
    create_post(source, alice, "first", name="alice", now=1000)
    second = create_post(source, alice, "second", name="alice", now=1001)
    third = create_post(source, alice, "third", name="alice", now=1002)

    conn = init_db(tmp_path / "tan.sqlite3")
    store_post(conn, second)
    store_post(conn, third)

    # Both entries are waiting on the missing first post: the third
    # entry's parent is stored but unlinked, which is normal partial
    # sync — only a *linked* parent left un-promoted is an inconsistency.
    report = verify_store(conn)
    assert report["ok"] is True
    assert report["entries"] == 2
    assert report["unlinked"] == 2
    assert report["problems"] == []


def test_verify_store_reports_tampered_text(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "original", name="alice", now=1000)
    conn.execute(
        "UPDATE posts SET text = 'edited' WHERE hash = ?", (post["hash"],)
    )
    conn.commit()

    report = verify_store(conn)
    assert report["ok"] is False
    assert len(report["problems"]) == 1
    assert report["problems"][0]["hash"] == post["hash"]
    assert "hash" in report["problems"][0]["problem"]


def test_verify_store_reports_a_bad_signature(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "original", name="alice", now=1000)
    conn.execute(
        "UPDATE posts SET sig = ? WHERE hash = ?",
        ("0" * 128, post["hash"]),
    )
    conn.commit()

    report = verify_store(conn)
    assert report["ok"] is False
    assert len(report["problems"]) == 1
    assert report["problems"][0]["hash"] == post["hash"]
    assert "signature" in report["problems"][0]["problem"]


def test_verify_store_reports_a_sequence_gap(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "first", name="alice", now=1000)
    second = create_post(conn, alice, "second", name="alice", now=1001)
    conn.execute(
        "UPDATE posts SET seq = 5 WHERE hash = ?", (second["hash"],)
    )
    conn.commit()

    report = verify_store(conn)
    assert report["ok"] is False
    assert any(
        problem["hash"] == second["hash"]
        and "jumps from 1 to 5" in problem["problem"]
        for problem in report["problems"]
    )


def test_verify_store_reports_a_stale_head(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    first = create_post(conn, alice, "first", name="alice", now=1000)
    create_post(conn, alice, "second", name="alice", now=1001)
    conn.execute(
        "UPDATE heads SET hash = ?, seq = 1 WHERE author = ?",
        (first["hash"], first["author"]),
    )
    conn.commit()

    report = verify_store(conn)
    assert report["ok"] is False
    assert any(
        "does not point at the author's latest entry" in problem["problem"]
        for problem in report["problems"]
    )


def test_verify_store_reports_an_unlinked_entry_with_its_parent_stored(
    identities, tmp_path
):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "first", name="alice", now=1000)
    second = create_post(conn, alice, "second", name="alice", now=1001)
    conn.execute(
        "UPDATE posts SET linked = 0, seq = 0 WHERE hash = ?",
        (second["hash"],),
    )
    conn.commit()

    report = verify_store(conn)
    assert report["ok"] is False
    assert any(
        problem["hash"] == second["hash"]
        and "unlinked but its parent is stored" in problem["problem"]
        for problem in report["problems"]
    )


def test_verify_store_accepts_a_forked_sequence(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    first = create_post(conn, alice, "first", name="alice", now=1000)
    create_post(conn, alice, "second", name="alice", now=1001)
    # A second entry chained to the first post reuses sequence 2; store_post
    # accepts such forks, so the audit must too.
    create_post(
        conn, alice, "forked", name="alice", now=1002, previous=first["hash"]
    )

    report = verify_store(conn)
    assert report["ok"] is True
    assert report["problems"] == []


def test_verify_store_reports_a_wrong_previous_link(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "first", name="alice", now=1000)
    second = create_post(conn, alice, "second", name="alice", now=1001)
    conn.execute(
        "UPDATE posts SET prev = ? WHERE hash = ?",
        ("0" * 64, second["hash"]),
    )
    conn.commit()

    report = verify_store(conn)
    assert report["ok"] is False
    assert any(
        problem["hash"] == second["hash"]
        and "does not point at the previous entry" in problem["problem"]
        for problem in report["problems"]
    )


def test_verify_store_reports_a_head_pointing_at_another_author(
    identities, tmp_path
):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "first", name="alice", now=1000)
    create_post(conn, alice, "second", name="alice", now=1001)
    create_post(conn, bob, "bob one", name="bob", now=1002)
    bob_second = create_post(conn, bob, "bob two", name="bob", now=1003)
    # Alice's head now points at Bob's latest entry, same sequence number.
    conn.execute(
        "UPDATE heads SET hash = ? WHERE author = ?",
        (bob_second["hash"], author_of(alice)),
    )
    conn.commit()

    report = verify_store(conn)
    assert report["ok"] is False
    assert any(
        "different author" in problem["problem"]
        for problem in report["problems"]
    )


def test_verify_store_reports_malformed_sequence_values(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "first", name="alice", now=1000)
    second = create_post(conn, alice, "second", name="alice", now=1001)
    conn.execute(
        "UPDATE posts SET seq = 'not-a-number' WHERE hash = ?",
        (second["hash"],),
    )
    conn.commit()

    # A hand-edited row is a store problem, not a traceback.
    report = verify_store(conn)
    assert report["ok"] is False
    assert any(
        problem["hash"] == second["hash"]
        and "non-numeric sequence number" in problem["problem"]
        for problem in report["problems"]
    )


def test_verify_store_reports_a_non_integer_sequence_value(
    identities, tmp_path
):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "first", name="alice", now=1000)
    second = create_post(conn, alice, "second", name="alice", now=1001)
    conn.execute(
        "UPDATE posts SET seq = 2.5 WHERE hash = ?", (second["hash"],)
    )
    conn.commit()

    report = verify_store(conn)
    assert report["ok"] is False
    assert any(
        problem["hash"] == second["hash"]
        and "non-numeric sequence number" in problem["problem"]
        for problem in report["problems"]
    )


def test_verify_store_reports_a_malformed_linked_flag(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "first", name="alice", now=1000)
    second = create_post(conn, alice, "second", name="alice", now=1001)
    conn.execute(
        "UPDATE posts SET linked = 'yes' WHERE hash = ?", (second["hash"],)
    )
    conn.commit()

    report = verify_store(conn)
    assert report["ok"] is False
    assert any(
        problem["hash"] == second["hash"]
        and "linked flag is not 0 or 1" in problem["problem"]
        for problem in report["problems"]
    )


def test_verify_store_reports_a_malformed_head_sequence(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "first", name="alice", now=1000)
    second = create_post(conn, alice, "second", name="alice", now=1001)
    conn.execute(
        "UPDATE heads SET seq = 'bogus' WHERE author = ?",
        (second["author"],),
    )
    conn.commit()

    report = verify_store(conn)
    assert report["ok"] is False
    assert any(
        "head sequence is not an integer" in problem["problem"]
        for problem in report["problems"]
    )


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


def test_suggest_authors_ranks_followed_engagement(identities, tmp_path):
    alice, bob = identities
    carol = RNS.Identity()
    dave = RNS.Identity()
    conn = init_db(tmp_path / "tan.sqlite3")
    carol_post = create_post(conn, carol, "carol here", name="carol", now=1000)
    create_post(conn, dave, "dave here", name="dave", now=1001)
    create_post(conn, alice, "alice here", name="alice", now=1002)
    follow(conn, author_of(bob), name="bob")
    create_reaction(conn, bob, carol_post["hash"], liked=True, name="bob", now=1003)

    suggestions = suggest_authors(conn, viewer=author_of(alice))
    assert [item["author"] for item in suggestions] == [
        author_of(carol),
        author_of(dave),
    ]
    assert suggestions[0]["name"] == "carol"
    assert suggestions[0]["posts"] == 1
    assert suggestions[0]["likes"] == 1
    assert suggestions[0]["engagers"] == ["bob"]
    assert "liked by bob" in suggestions[0]["reason"]
    # Dave has posts but no engagement, so he still ranks — last, at zero.
    assert suggestions[1]["score"] == 0
    assert "1 post(s)" in suggestions[1]["reason"]


def test_suggest_authors_counts_replies_and_unlikes(identities, tmp_path):
    alice, bob = identities
    carol = RNS.Identity()
    conn = init_db(tmp_path / "tan.sqlite3")
    carol_post = create_post(conn, carol, "carol here", name="carol", now=1000)
    follow(conn, author_of(bob), name="bob")
    create_reply(conn, bob, carol_post["hash"], "nice one", name="bob", now=1001)
    # An unlike from the same followed author still counts as engagement
    # via the reply, but the withdrawn like no longer counts as a like.
    create_reaction(conn, bob, carol_post["hash"], liked=True, name="bob", now=1002)
    create_reaction(conn, bob, carol_post["hash"], liked=False, name="bob", now=1003)

    suggestions = suggest_authors(conn, viewer=author_of(alice))
    assert len(suggestions) == 1
    assert suggestions[0]["author"] == author_of(carol)
    assert suggestions[0]["likes"] == 0
    assert suggestions[0]["engagers"] == ["bob"]
    assert "replied to by bob" in suggestions[0]["reason"]


def test_suggest_authors_counts_each_like_per_post(identities, tmp_path, monkeypatch):
    alice, bob = identities
    carol = RNS.Identity()
    conn = init_db(tmp_path / "tan.sqlite3")
    first = create_post(conn, carol, "one", name="carol", now=1000)
    second = create_post(conn, carol, "two", name="carol", now=1001)
    follow(conn, author_of(bob), name="bob")
    create_reaction(conn, bob, first["hash"], liked=True, name="bob", now=1002)
    create_reaction(conn, bob, second["hash"], liked=True, name="bob", now=1003)
    # Force the bounded-batch lookup path with a tiny batch size.
    monkeypatch.setattr(tan_module, "_SQL_BATCH", 1)

    suggestions = suggest_authors(conn, viewer=author_of(alice))
    assert len(suggestions) == 1
    # One liker on two posts is two likes, not one.
    assert suggestions[0]["likes"] == 2
    assert suggestions[0]["posts"] == 2
    assert suggestions[0]["engagers"] == ["bob"]
    assert suggestions[0]["score"] == SUGGEST_FOLLOWED_WEIGHT + 2


def test_suggest_authors_counts_replies_to_replies(identities, tmp_path):
    alice, bob = identities
    carol = RNS.Identity()
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, carol, "starter", name="carol", now=1000)
    reply = create_reply(conn, carol, post["hash"], "self reply", name="carol", now=1001)
    follow(conn, author_of(bob), name="bob")
    create_reply(conn, bob, reply["hash"], "replying to the reply", name="bob", now=1002)

    suggestions = suggest_authors(conn, viewer=author_of(alice))
    assert len(suggestions) == 1
    assert suggestions[0]["engagers"] == ["bob"]
    assert "replied to by bob" in suggestions[0]["reason"]
    # The candidate's own reply is an engagement target, not a post.
    assert suggestions[0]["posts"] == 1


def test_suggest_authors_skips_followed_self_deleted_and_unlinked(
    identities, tmp_path
):
    alice, bob = identities
    carol = RNS.Identity()
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "mine", name="alice", now=1000)
    followed_post = create_post(conn, bob, "followed", name="bob", now=1001)
    follow(conn, followed_post["author"], name="bob")
    deleted_post = create_post(conn, carol, "gone", name="carol", now=1002)
    create_delete(conn, carol, deleted_post["hash"], name="carol", now=1003)

    # Carol's only remaining entry is the delete, so she has no candidate
    # posts — and neither the viewer nor a followed author is suggested.
    assert suggest_authors(conn, viewer=author_of(alice)) == []

    # An unlinked post (chain head missing) does not make its author a
    # candidate either.
    source = init_db(tmp_path / "source.sqlite3")
    eve = RNS.Identity()
    create_post(source, eve, "first", name="eve", now=1000)
    second = create_post(source, eve, "second", name="eve", now=1001)
    store_post(conn, second)
    assert suggest_authors(conn, viewer=author_of(alice)) == []


def test_suggest_authors_limit_and_no_follow_fallback(identities, tmp_path):
    alice, bob = identities
    carol = RNS.Identity()
    dave = RNS.Identity()
    conn = init_db(tmp_path / "tan.sqlite3")
    carol_post = create_post(conn, carol, "popular", name="carol", now=1000)
    dave_post = create_post(conn, dave, "quieter", name="dave", now=1001)
    create_reaction(conn, alice, carol_post["hash"], liked=True, name="alice", now=1002)
    create_reaction(conn, bob, carol_post["hash"], liked=True, name="bob", now=1003)
    create_reaction(conn, alice, dave_post["hash"], liked=True, name="alice", now=1004)

    # No follows: the most-liked cached authors come first.
    suggestions = suggest_authors(conn, viewer=author_of(alice))
    assert [item["author"] for item in suggestions] == [
        author_of(carol),
        author_of(dave),
    ]
    assert suggestions[0]["likes"] == 2
    assert "2 like(s)" in suggestions[0]["reason"]
    assert [item["author"] for item in suggest_authors(conn, limit=1)] == [
        author_of(carol)
    ]
    assert suggest_authors(conn, limit=0) == []


def test_cli_tan_suggest(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    capsys.readouterr()
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        carol = RNS.Identity()
        bob = RNS.Identity()
        carol_post = create_post(conn, carol, "carol here", name="carol", now=1000)
        create_post(conn, bob, "bob here", name="bob", now=1001)
        follow(conn, author_of(bob), name="bob")
        create_reaction(conn, bob, carol_post["hash"], liked=True, name="bob", now=1002)
    finally:
        conn.close()

    assert (
        main(["tan", "suggest", "--data-dir", str(data_dir), "--json"]) == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["suggestions"][0]["name"] == "carol"
    assert payload["suggestions"][0]["engagers"] == ["bob"]

    # Text output names the suggestion and its reason.
    assert main(["tan", "suggest", "--data-dir", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "carol" in out
    assert "liked by bob" in out

    # A fresh node with nothing cached gets a clean empty result.
    empty_dir = tmp_path / "empty"
    assert main(["init", "--data-dir", str(empty_dir), "--name", "nobody"]) == 0
    capsys.readouterr()
    assert main(["tan", "suggest", "--data-dir", str(empty_dir)]) == 0
    assert "No suggestions yet" in capsys.readouterr().out


def test_mute_unmute_and_muted_list(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    bob_post = create_post(conn, bob, "from bob", name="bob", now=1000)

    record = mute(conn, bob_post["author"], name="bob", now=1001)
    assert record["name"] == "bob"
    assert record["muted_at"] == 1001
    # Re-muting keeps the original mute time.
    assert mute(conn, bob_post["author"], name="robert", now=1002)["muted_at"] == 1001
    assert [entry["author"] for entry in muted(conn)] == [bob_post["author"]]
    assert muted_author_set(conn) == {bob_post["author"]}
    # Muting never deletes anything: the posts stay stored.
    assert [post["text"] for post in feed(conn, author=bob_post["author"])] == [
        "from bob"
    ]

    assert unmute(conn, bob_post["author"]) is True
    assert unmute(conn, bob_post["author"]) is False
    assert muted(conn) == []
    assert muted_author_set(conn) == set()


def test_timeline_hides_muted_authors(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    bob_post = create_post(conn, bob, "from bob", name="bob", now=1000)
    alice_post = create_post(conn, alice, "from alice", name="alice", now=1001)
    authors = [bob_post["author"], alice_post["author"]]

    mute(conn, bob_post["author"], name="bob")
    assert [post["text"] for post in timeline(conn, authors)] == ["from alice"]
    assert [post["text"] for post in timeline(conn, include_all=True)] == ["from alice"]
    assert timeline(conn, [bob_post["author"]]) == []
    # An explicit author feed is a direct lookup and still shows them.
    assert [post["text"] for post in feed(conn, author=bob_post["author"])] == [
        "from bob"
    ]

    # Unmuting restores the full timeline.
    unmute(conn, bob_post["author"])
    assert [post["text"] for post in timeline(conn, authors)] == [
        "from alice",
        "from bob",
    ]


def test_search_hides_muted_authors(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "hello mesh world", name="alice", now=1000)
    create_post(conn, bob, "HELLO from bob", name="bob", now=1001)

    mute(conn, author_of(bob), name="bob")
    assert [post["text"] for post in search_posts(conn, "hello")] == [
        "hello mesh world"
    ]
    # An explicit author search still shows the muted author.
    assert [
        post["text"] for post in search_posts(conn, "hello", author=author_of(bob))
    ] == ["HELLO from bob"]

    unmute(conn, author_of(bob))
    assert [post["text"] for post in search_posts(conn, "hello")] == [
        "HELLO from bob",
        "hello mesh world",
    ]


def test_thread_hides_muted_subtree(identities, tmp_path):
    alice, bob = identities
    carol = RNS.Identity()
    conn = init_db(tmp_path / "tan.sqlite3")
    root = create_post(conn, alice, "root post", name="alice", now=1000)
    reply = create_reply(conn, bob, root["hash"], "bob replies", name="bob", now=1001)
    nested = create_reply(
        conn, carol, reply["hash"], "carol replies to bob", name="carol", now=1002
    )
    create_reply(
        conn, carol, root["hash"], "carol replies to root", name="carol", now=1003
    )

    mute(conn, author_of(bob), name="bob")
    entries = thread(conn, root["hash"])
    assert [entry["text"] for entry in entries] == [
        "root post",
        "carol replies to root",
    ]
    # The named entry always anchors the thread, even when its author
    # is muted — so the muted reply shows with its own descendants.
    assert [entry["text"] for entry in thread(conn, reply["hash"])] == [
        "bob replies",
        "carol replies to bob",
    ]
    # ...while a muted ancestor above the named entry simply ends the
    # climb instead of being promoted to the root.
    assert [entry["text"] for entry in thread(conn, nested["hash"])] == [
        "carol replies to bob"
    ]
    # ...and a muted author's own root post still shows when opened by hash.
    bob_root = create_post(conn, bob, "bob root", name="bob", now=1004)
    assert [entry["text"] for entry in thread(conn, bob_root["hash"])] == ["bob root"]

    unmute(conn, author_of(bob))
    assert [entry["text"] for entry in thread(conn, root["hash"])] == [
        "root post",
        "bob replies",
        "carol replies to bob",
        "carol replies to root",
    ]


def test_timeline_hides_muted_parent_preview(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    parent = create_post(conn, bob, "muted parent text", name="bob", now=1000)
    create_reply(conn, alice, parent["hash"], "alice replies", name="alice", now=1001)

    mute(conn, author_of(bob), name="bob")
    posts = timeline(conn, [author_of(alice)], viewer=author_of(alice))
    assert len(posts) == 1
    # The muted parent's name and text never surface in the preview.
    assert posts[0]["reply_to"] is None
    assert "muted parent text" not in json.dumps(posts)

    # An explicit author lookup still shows the parent preview.
    shown = feed(conn, author=author_of(alice), posts_only=True)
    annotated = annotate_replies(
        conn, annotate_reactions(conn, shown, viewer=author_of(alice))
    )
    assert annotated[0]["reply_to"]["text"] == "muted parent text"


def test_resolve_author_prefix_with_renamed_author(identities, tmp_path):
    _, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, bob, "first as bob", name="bob", now=1000)
    create_post(conn, bob, "now as robert", name="robert", now=1001)

    # A unique hash prefix resolves even though the author posted under
    # two names, and reports the latest name.
    author, name = resolve_author(conn, author_of(bob)[:8])
    assert author == author_of(bob)
    assert name == "robert"


def test_resolve_muted_name(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    mute(conn, author_of(bob), name="bob", now=1000)

    assert resolve_muted_name(conn, "bob") == (author_of(bob), "bob")
    assert resolve_muted_name(conn, "BOB")[0] == author_of(bob)
    assert resolve_muted_name(conn, "alice") is None

    # Two muted authors sharing a name must be disambiguated by hash.
    mute(conn, author_of(alice), name="bob", now=1001)
    with pytest.raises(TanError, match="use a destination hash"):
        resolve_muted_name(conn, "bob")




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

    post = create_post(
        init_db(tmp_path / "scratch.sqlite3"), bob, "networked", name="bob", now=1000
    )
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


def test_cli_mute_muted_unmute(tmp_path, capsys):
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
    capsys.readouterr()
    bob = RNS.Identity()
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        bob_post = create_post(conn, bob, "bob says hello", name="bob", now=1000)
        create_post(conn, bob, "bob again", name="bob", now=1001)
    finally:
        conn.close()
    bob_author = bob_post["author"]
    own = destination_hash(load_identity(data_dir / "identity")).hex()

    # Muting yourself is refused.
    assert main(["tan", "mute", "--data-dir", str(data_dir), own]) == 1
    assert "cannot mute yourself" in capsys.readouterr().err

    assert main(["tan", "mute", "--data-dir", str(data_dir), "--json", bob_author]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["author"] == bob_author

    # Muted authors leave the timeline; the muted list shows them.
    assert (
        main(["tan", "timeline", "--data-dir", str(data_dir), "--all", "--json"]) == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert [post["text"] for post in payload["posts"]] == ["hello from the cli"]
    assert main(["tan", "muted", "--data-dir", str(data_dir), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["muted"][0]["author"] == bob_author

    # Unmuting restores the timeline; a second unmute is a clean no-op.
    assert (
        main(["tan", "unmute", "--data-dir", str(data_dir), "--json", bob_author]) == 0
    )
    assert json.loads(capsys.readouterr().out)["unmuted"] is True
    assert (
        main(["tan", "timeline", "--data-dir", str(data_dir), "--all", "--json"]) == 0
    )
    assert len(json.loads(capsys.readouterr().out)["posts"]) == 3
    assert (
        main(["tan", "unmute", "--data-dir", str(data_dir), "--json", bob_author]) == 0
    )
    assert json.loads(capsys.readouterr().out)["unmuted"] is False



    # Names work too, even though bob is not in the peer registry: the
    # name comes from his cached posts, and from the mute record itself.
    assert main(["tan", "mute", "--data-dir", str(data_dir), "--json", "bob"]) == 0
    assert json.loads(capsys.readouterr().out)["author"] == bob_author
    assert main(["tan", "unmute", "--data-dir", str(data_dir), "--json", "bob"]) == 0
    assert json.loads(capsys.readouterr().out)["unmuted"] is True

    # A unique hash prefix works as well.
    assert (
        main(["tan", "mute", "--data-dir", str(data_dir), "--json", bob_author[:8]])
        == 0
    )
    assert json.loads(capsys.readouterr().out)["author"] == bob_author
    assert (
        main(["tan", "unmute", "--data-dir", str(data_dir), "--json", bob_author[:8]])
        == 0
    )
    assert json.loads(capsys.readouterr().out)["unmuted"] is True


def test_cli_hide_unhide_hidden(tmp_path, capsys):
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
    capsys.readouterr()
    bob = RNS.Identity()
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        bob_post = create_post(conn, bob, "bob says hello", name="bob", now=1000)
        create_post(conn, bob, "bob again", name="bob", now=1001)
    finally:
        conn.close()

    # Hide by prefix; hiding again reports the original hide time.
    assert (
        main(
            [
                "tan",
                "hide",
                "--data-dir",
                str(data_dir),
                "--json",
                bob_post["hash"][:16],
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["hash"] == bob_post["hash"]
    assert payload["already_hidden"] is False
    assert (
        main(["tan", "hide", "--data-dir", str(data_dir), bob_post["hash"]]) == 0
    )
    assert "already hidden" in capsys.readouterr().out

    # The hidden post leaves the timeline; the hidden list shows it.
    assert (
        main(["tan", "timeline", "--data-dir", str(data_dir), "--all", "--json"]) == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert [post["text"] for post in payload["posts"]] == [
        "hello from the cli",
        "bob again",
    ]
    assert main(["tan", "hidden", "--data-dir", str(data_dir), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["hidden"][0]["hash"] == bob_post["hash"]

    # Unhiding restores the timeline; a second unhide is a clean no-op.
    assert (
        main(
            ["tan", "unhide", "--data-dir", str(data_dir), "--json", bob_post["hash"]]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["unhidden"] is True
    assert (
        main(["tan", "timeline", "--data-dir", str(data_dir), "--all", "--json"]) == 0
    )
    assert len(json.loads(capsys.readouterr().out)["posts"]) == 3
    assert (
        main(
            ["tan", "unhide", "--data-dir", str(data_dir), "--json", bob_post["hash"]]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["unhidden"] is False


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
    assert (
        json.loads(export_path.read_text(encoding="utf-8"))["entries"][0]["text"]
        == "hello backup"
    )

    carol_dir = tmp_path / "carol"
    assert main(["init", "--data-dir", str(carol_dir), "--name", "carol"]) == 0
    assert main(["tan", "import", "--data-dir", str(carol_dir), str(export_path)]) == 0
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
    assert (
        json.loads(cached_path.read_text(encoding="utf-8"))["entries"][0]["text"]
        == "hello backup"
    )


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
    assert main(["tan", "import", "--data-dir", str(data_dir), str(bad_path)]) == 1
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


def test_cli_tan_verify(tmp_path, capsys):
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
                "hello from the cli",
            ]
        )
        == 0
    )
    capsys.readouterr()

    assert main(["tan", "verify", "--data-dir", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "Tan store OK: 1 entries from 1 authors verified." in out

    assert (
        main(["tan", "verify", "--data-dir", str(data_dir), "--json"]) == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "entries": 1,
        "authors": 1,
        "unlinked": 0,
        "problems": [],
        "ok": True,
    }


def test_cli_tan_verify_reports_problems(tmp_path, capsys):
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
                "hello from the cli",
            ]
        )
        == 0
    )
    capsys.readouterr()
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        conn.execute("UPDATE posts SET text = 'edited'")
        conn.commit()
    finally:
        conn.close()

    assert main(["tan", "verify", "--data-dir", str(data_dir)]) == 1
    out = capsys.readouterr().out
    assert "does not match its contents" in out
    assert "1 problem(s)" in out

    assert (
        main(["tan", "verify", "--data-dir", str(data_dir), "--json"]) == 1
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert len(payload["problems"]) == 1


def test_cli_tan_verify_is_read_only(tmp_path, capsys):
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
                "hello from the cli",
            ]
        )
        == 0
    )
    capsys.readouterr()
    db_path = data_dir / "tan.sqlite3"
    before = db_path.read_bytes()

    assert main(["tan", "verify", "--data-dir", str(data_dir)]) == 0
    capsys.readouterr()
    assert db_path.read_bytes() == before


def test_cli_tan_verify_missing_store(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["tan", "verify", "--data-dir", str(data_dir)]) == 1
    assert "no Tan store at" in capsys.readouterr().err


def test_cli_tan_verify_reports_a_corrupt_store(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    (data_dir / "tan.sqlite3").write_bytes(b"not a database")

    # A damaged store is exactly when this command is needed: a clean
    # failure, not a traceback.
    assert main(["tan", "verify", "--data-dir", str(data_dir)]) == 1
    assert "could not audit Tan store" in capsys.readouterr().err


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
    assert reaction_states(conn, [post["hash"]])[post["hash"]] == {bob_author: "unlike"}


def test_post_likers_lists_newest_first_and_drops_unlikes(identities, tmp_path):
    alice, bob = identities
    carol = RNS.Identity()
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "likeable", name="alice", now=1000)

    # No reactions yet: an empty list, not an error.
    assert post_likers(conn, post["hash"]) == []

    create_reaction(conn, bob, post["hash"], liked=True, name="bob", now=1001)
    create_reaction(conn, carol, post["hash"], liked=True, name="carol", now=1002)
    likers = post_likers(conn, post["hash"])
    assert [entry["author"] for entry in likers] == [
        author_of(carol),
        author_of(bob),
    ]
    assert [entry["liked_at"] for entry in likers] == [1002, 1001]

    # An unlike supersedes the like (latest chain entry wins).
    create_reaction(conn, bob, post["hash"], liked=False, name="bob", now=1003)
    assert [entry["author"] for entry in post_likers(conn, post["hash"])] == [
        author_of(carol)
    ]

    # Liking again puts the author back, at the newer timestamp.
    create_reaction(conn, bob, post["hash"], liked=True, name="bob", now=1004)
    assert [entry["author"] for entry in post_likers(conn, post["hash"])] == [
        author_of(bob),
        author_of(carol),
    ]

    # A well-formed hash with no reactions is an empty list.
    assert post_likers(conn, "cd" * 32) == []

    # A malformed target is rejected, not silently empty.
    with pytest.raises(PostError):
        post_likers(conn, "not-a-hash")


def test_likers_names_current_likers(identities, tmp_path):
    alice, bob = identities
    carol = RNS.Identity()
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "likeable", name="alice", now=1000)
    bob_author = author_of(bob)
    carol_author = author_of(carol)

    assert likers(conn, post["hash"])["likers"] == []

    create_reaction(conn, bob, post["hash"], liked=True, name="bob", now=1001)
    create_reaction(conn, carol, post["hash"], liked=True, name="carol", now=1002)
    result = likers(conn, post["hash"][:16])
    assert result["post"]["hash"] == post["hash"]
    assert result["likers"] == [
        {"author": bob_author, "name": "bob"},
        {"author": carol_author, "name": "carol"},
    ]

    # An unlike supersedes the like; a re-like restores it under the
    # name on the latest reaction entry.
    create_reaction(conn, bob, post["hash"], liked=False, name="bob", now=1003)
    assert likers(conn, post["hash"])["likers"] == [
        {"author": carol_author, "name": "carol"}
    ]
    create_reaction(conn, bob, post["hash"], liked=True, name="robert", now=1004)
    assert likers(conn, post["hash"])["likers"] == [
        {"author": carol_author, "name": "carol"},
        {"author": bob_author, "name": "robert"},
    ]


def test_likers_rejects_unknown_and_deleted(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "doomed", name="alice", now=1000)
    create_reaction(conn, bob, post["hash"], liked=True, name="bob", now=1001)

    with pytest.raises(TanError, match="unknown post"):
        likers(conn, "cd" * 32)

    create_delete(conn, alice, post["hash"], name="alice", now=1002)
    with pytest.raises(TanError, match="has been deleted"):
        likers(conn, post["hash"])


def test_likers_works_on_replies(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "root", name="alice", now=1000)
    reply = create_reply(conn, bob, post["hash"], "agreed", name="bob", now=1001)
    create_reaction(conn, alice, reply["hash"], liked=True, name="alice", now=1002)
    assert likers(conn, reply["hash"][:12])["likers"] == [
        {"author": author_of(alice), "name": "alice"}
    ]


def test_likers_rejects_unlinked_entry(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    digest = "cd" * 32
    _insert_stub(conn, digest, author=author_of(alice), linked=False)

    with pytest.raises(TanError, match="has not linked yet"):
        likers(conn, digest)


def test_likers_rejects_reply_beneath_deleted_ancestor(identities, tmp_path):
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

    # Naming the nested reply directly must not show its likers either.
    with pytest.raises(TanError, match="has been deleted"):
        likers(conn, nested["hash"])


def test_likers_refuses_blocked_authors(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "likeable", name="alice", now=1000)
    reply = create_reply(conn, bob, post["hash"], "agreed", name="bob", now=1001)
    create_reaction(conn, alice, reply["hash"], liked=True, name="alice", now=1002)

    # Naming a blocked author's post refuses outright, like thread().
    with pytest.raises(TanError, match="blocked author"):
        likers(conn, post["hash"], exclude_authors=[post["author"]])

    # Naming a reply beneath a blocked ancestor refuses outright too.
    with pytest.raises(TanError, match="blocked author"):
        likers(conn, reply["hash"], exclude_authors=[post["author"]])

    # Anyone else's posts still show their likers.
    assert likers(conn, reply["hash"])["likers"] == [
        {"author": author_of(alice), "name": "alice"}
    ]


def test_likers_refuses_when_block_list_unusable(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "likeable", name="alice", now=1000)
    create_reaction(conn, bob, post["hash"], liked=True, name="bob", now=1001)

    # Fail closed, like thread(): an unusable block list hides everything.
    with pytest.raises(TanError, match="block-list is unusable"):
        likers(conn, post["hash"], exclude_all=True)


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
        init_db(tmp_path / "scratch2.sqlite3"),
        bob,
        post["hash"],
        liked=True,
        name="bob",
        now=1001,
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
    assert [item["hash"] for item in feed(conn, posts_only=True)] == [other["hash"]]
    assert [
        item["hash"] for item in feed(conn, author=post["author"], posts_only=True)
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
    assert [post["text"] for post in search_posts(conn, "MESH")] == ["hello mesh world"]
    assert search_posts(conn, "nothing matches") == []


def test_search_folds_unicode_case(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "Äpfel und Öl", name="alice", now=1000)
    create_post(conn, alice, "plain apples", name="alice", now=1001)

    assert [post["text"] for post in search_posts(conn, "äpfel")] == ["Äpfel und Öl"]
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


def test_list_authors(identities, tmp_path):
    alice, bob = identities
    carol = RNS.Identity()
    conn = init_db(tmp_path / "tan.sqlite3")
    alice_second = create_post(conn, alice, "alice two", name="alice", now=1002)
    create_post(conn, alice, "alice one", name="alice", now=1000)
    create_post(conn, bob, "bob one", name="bob", now=1001)
    follow(conn, author_of(bob), name="bob")

    records = list_authors(conn)
    assert [record["name"] for record in records] == ["alice", "bob"]
    alice_record, bob_record = records
    assert alice_record["posts"] == 2
    assert alice_record["last_post"] == 1002
    assert alice_record["followed"] is False
    assert bob_record["posts"] == 1
    assert bob_record["last_post"] == 1001
    assert bob_record["followed"] is True

    assert [record["name"] for record in list_authors(conn, sort="posts")] == [
        "alice",
        "bob",
    ]
    assert [record["name"] for record in list_authors(conn, sort="name")] == [
        "alice",
        "bob",
    ]
    assert [record["name"] for record in list_authors(conn, limit=1)] == ["alice"]

    # Deleting alice's newest post drops her count and last-post time.
    create_delete(conn, alice, alice_second["hash"], name="alice", now=1003)
    (alice_record,) = [
        record for record in list_authors(conn) if record["name"] == "alice"
    ]
    assert alice_record["posts"] == 1
    assert alice_record["last_post"] == 1000

    # An author known only from a delete entry (target not cached yet)
    # has no visible posts, so they are not listed.
    create_delete(conn, carol, "ab" * 32, name="carol", now=1004)
    records = list_authors(conn)
    assert [record["name"] for record in records] == ["bob", "alice"]
    assert all(record["posts"] > 0 for record in records)

    with pytest.raises(TanError):
        list_authors(conn, sort="nope")
    with pytest.raises(TanError):
        list_authors(conn, limit=-1)


def test_open_readonly_db_leaves_store_untouched(identities, tmp_path):
    alice, _ = identities
    path = tmp_path / "tan.sqlite3"
    conn = init_db(path)
    create_post(conn, alice, "hello", name="alice", now=1000)
    conn.close()

    before = path.read_bytes()
    conn = open_readonly_db(path)
    try:
        assert len(list_authors(conn)) == 1
    finally:
        conn.close()
    assert path.read_bytes() == before


def test_open_readonly_db_rejects_outdated_schema(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    conn = sqlite3.connect(str(path))
    # A pre-reactions store: no kind/target columns, so list_authors
    # would fail mid-query on a missing column.
    conn.execute(
        "CREATE TABLE posts (v INTEGER NOT NULL DEFAULT 1, hash TEXT PRIMARY KEY, "
        "author TEXT NOT NULL, prev TEXT NOT NULL DEFAULT '', key TEXT NOT NULL, "
        "name TEXT NOT NULL DEFAULT '', text TEXT NOT NULL, created_at REAL NOT NULL, "
        "received_at REAL NOT NULL, sig TEXT NOT NULL, seq INTEGER NOT NULL DEFAULT 0, "
        "linked INTEGER NOT NULL DEFAULT 0)"
    )
    conn.execute(
        "CREATE TABLE follows (author TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '', "
        "added_at REAL NOT NULL)"
    )
    conn.commit()
    conn.close()
    with pytest.raises(TanError, match="outdated"):
        open_readonly_db(path)


def test_cli_tan_authors(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    # A fresh data dir has no store yet, and listing must not create one.
    assert (
        main(["tan", "authors", "--data-dir", str(data_dir), "--json"]) == 0
    )
    assert json.loads(capsys.readouterr().out) == {"authors": [], "count": 0}
    assert not (data_dir / "tan.sqlite3").exists()

    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    for text in ("first", "second"):
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
        main(["tan", "authors", "--data-dir", str(data_dir), "--json"]) == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    (record,) = payload["authors"]
    assert record["name"] == "alice"
    assert record["posts"] == 2
    assert record["followed"] is False

    assert main(["tan", "authors", "--data-dir", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "alice" in out
    assert "2 post(s)" in out

    # A pre-reactions store fails cleanly, not with a traceback.
    legacy_dir = tmp_path / "legacy"
    legacy_dir.mkdir()
    legacy_conn = sqlite3.connect(str(legacy_dir / "tan.sqlite3"))
    legacy_conn.execute(
        "CREATE TABLE posts (v INTEGER NOT NULL DEFAULT 1, hash TEXT PRIMARY KEY, "
        "author TEXT NOT NULL, prev TEXT NOT NULL DEFAULT '', key TEXT NOT NULL, "
        "name TEXT NOT NULL DEFAULT '', text TEXT NOT NULL, created_at REAL NOT NULL, "
        "received_at REAL NOT NULL, sig TEXT NOT NULL, seq INTEGER NOT NULL DEFAULT 0, "
        "linked INTEGER NOT NULL DEFAULT 0)"
    )
    legacy_conn.execute(
        "CREATE TABLE follows (author TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '', "
        "added_at REAL NOT NULL)"
    )
    legacy_conn.commit()
    legacy_conn.close()
    assert main(["tan", "authors", "--data-dir", str(legacy_dir), "--json"]) != 0
    captured = capsys.readouterr()
    assert "outdated" in captured.err
    assert "Traceback" not in captured.err

    assert main(["tan", "authors", "--data-dir", str(data_dir), "--limit", "-1"]) != 0


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
    assert [item["hash"] for item in timeline(conn, [post["author"]])] == [post["hash"]]
    assert deleted_targets(conn) == set()
    assert resolve_post(conn, post["hash"])["hash"] == post["hash"]

    # Only posts, replies, reposts and quotes can be deleted, even by their own author.
    with pytest.raises(TanError, match="only posts, replies, reposts and quotes can be deleted"):
        create_delete(conn, bob, forged["hash"], name="bob", now=1002)


def test_delete_arriving_before_its_post_applies_on_arrival(identities, tmp_path):
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
        init_db(tmp_path / "scratch2.sqlite3"),
        alice,
        post["hash"],
        name="alice",
        now=1001,
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


def test_edit_updates_timeline_feed_and_thread(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "orginal typo", name="alice", now=1000)
    reply = create_reply(
        conn, bob, post["hash"], "nice post", name="bob", now=1001
    )

    edit = create_edit(conn, alice, post["hash"], "original, fixed", name="alice", now=1002)
    assert edit["kind"] == "edit"
    assert edit["target"] == post["hash"]
    assert edit["text"] == "original, fixed"
    assert verify_post(edit)["hash"] == edit["hash"]

    # The original entry is untouched; display surfaces show the edit.
    assert get_post(conn, post["hash"])["text"] == "orginal typo"
    items = timeline(conn, [post["author"], reply["author"]])
    assert [item["text"] for item in items] == ["nice post", "original, fixed"]
    assert items[1]["edited"] is True
    assert items[1]["edited_at"] == 1002
    assert items[0]["edited"] is False
    assert [item["text"] for item in feed(conn, posts_only=True)] == [
        "nice post",
        "original, fixed",
    ]
    assert [item["text"] for item in thread(conn, post["hash"])] == [
        "original, fixed",
        "nice post",
    ]
    assert thread(conn, post["hash"])[0]["edited"] is True


def test_edit_latest_wins_and_reply_parent_shows_edited_text(
    identities, tmp_path
):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "v1", name="alice", now=1000)
    create_edit(conn, alice, post["hash"], "v2", name="alice", now=1001)
    create_edit(conn, alice, post["hash"], "v3", name="alice", now=1002)
    reply = create_reply(
        conn, bob, post["hash"], "quoting you", name="bob", now=1003
    )

    states = edit_states(conn, [post["hash"]])
    assert states[post["hash"]]["text"] == "v3"
    items = timeline(conn, [post["author"], reply["author"]])
    assert [item["text"] for item in items] == ["quoting you", "v3"]
    assert items[0]["reply_to"]["text"] == "v3"
    assert items[0]["reply_to"]["edited"] is True
    # Editing a reply works the same way.
    create_edit(conn, bob, reply["hash"], "quoting you (fixed)", name="bob", now=1004)
    items = timeline(conn, [post["author"], reply["author"]])
    assert [item["text"] for item in items] == [
        "quoting you (fixed)",
        "v3",
    ]


def test_edit_search_matches_edited_text_not_original(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "bananas", name="alice", now=1000)
    create_edit(conn, alice, post["hash"], "apples", name="alice", now=1001)

    assert [item["hash"] for item in search_posts(conn, "apples")] == [
        post["hash"]
    ]
    assert search_posts(conn, "bananas") == []
    assert search_posts(conn, "apples")[0]["edited"] is True


def test_edit_fork_keeps_retained_chain_text(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "v1", name="alice", now=1000)
    first = create_edit(
        conn, alice, post["hash"], "head branch", name="alice", now=1001
    )
    # A second edit branched from the same parent (e.g. written on
    # another device before it saw the first edit) shares the sequence
    # number; only the edit on the retained head chain counts.
    create_edit(
        conn, alice, post["hash"], "forked branch", name="alice", now=1002,
        previous=post["hash"],
    )
    assert head(conn, post["author"]) == first["hash"]
    items = timeline(conn, [post["author"]])
    assert [item["text"] for item in items] == ["head branch"]
    # An edit chained onto the retained head supersedes both.
    create_edit(
        conn, alice, post["hash"], "head branch, again", name="alice", now=1003
    )
    items = timeline(conn, [post["author"]])
    assert [item["text"] for item in items] == ["head branch, again"]


def test_search_batches_edit_lookups_over_large_feeds(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    for n in range(1100):
        create_post(conn, alice, f"bulk post {n}", name="alice", now=1000 + n)
    # 1100 candidates exceeds SQLite's 999 host-variable limit; the
    # edit lookup must batch instead of failing the whole search.
    results = search_posts(conn, "bulk", limit=200)
    assert len(results) == 200
    assert all(item["edited"] is False for item in results)


def test_edit_rejects_foreign_unknown_deleted_and_unlinked(
    identities, tmp_path
):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "mine", name="alice", now=1000)

    with pytest.raises(TanError, match="did not author"):
        create_edit(conn, bob, post["hash"], "hijack", name="bob", now=1001)
    with pytest.raises(TanError, match="unknown post"):
        create_edit(conn, alice, "ab" * 32, "ghost", name="alice", now=1001)
    # Only posts and replies can be edited, even by their own author.
    like = create_reaction(conn, alice, post["hash"], liked=True, name="alice", now=1001)
    with pytest.raises(TanError, match="only posts and replies can be edited"):
        create_edit(conn, alice, like["hash"], "nope", name="alice", now=1002)
    # A quote is a timeline entry, but edits to it would never display
    # (edit_states only covers posts and replies), so it is rejected
    # here rather than stored as a silent no-op.
    quote = create_quote(
        conn, alice, post["hash"], "sharing", name="alice", now=1002
    )
    with pytest.raises(TanError, match="only posts and replies can be edited"):
        create_edit(conn, alice, quote["hash"], "nope", name="alice", now=1003)

    create_delete(conn, alice, post["hash"], name="alice", now=1002)
    with pytest.raises(TanError, match="has been deleted"):
        create_edit(conn, alice, post["hash"], "too late", name="alice", now=1003)


def test_edit_by_non_author_is_inert(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "mine", name="alice", now=1000)

    # A forged edit smuggled in through the store path is kept (chains
    # stay complete) but is inert: the post keeps its own text.  It has
    # to be hand-built — create_edit refuses a foreign post outright.
    body = {
        "v": 1,
        "author": author_of(bob),
        "key": bob.get_public_key().hex(),
        "name": "bob",
        "text": "hijacked",
        "created_at": 1001.0,
        "prev": "",
        "kind": "edit",
        "target": post["hash"],
    }
    digest = hash(body, previous="")
    forged = dict(
        body,
        hash=digest,
        sig=bob.sign(bytes.fromhex(digest)).hex(),
    )
    record, is_new = store_post(conn, forged)
    assert is_new is True
    assert record["kind"] == "edit"
    items = timeline(conn, [post["author"]])
    assert [item["text"] for item in items] == ["mine"]
    assert items[0]["edited"] is False
    assert edit_states(conn, [post["hash"]]) == {}


def test_edit_arriving_before_its_post_applies_on_arrival(
    identities, tmp_path
):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    scratch = init_db(tmp_path / "scratch.sqlite3")
    post = create_post(scratch, alice, "late arrival", name="alice", now=1000)
    edit = create_edit(scratch, alice, post["hash"], "late arrival, fixed", name="alice", now=1001)

    # The edit lands first; with no post cached it is inert.
    store_post(conn, edit)
    assert edit_states(conn, [post["hash"]]) == {}

    # Once the post itself is synced in, the earlier edit applies.
    store_post(conn, post)
    items = timeline(conn, [post["author"]])
    assert [item["text"] for item in items] == ["late arrival, fixed"]
    assert items[0]["edited"] is True


def test_edit_rejects_tampering(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "target practice", name="alice", now=1000)
    other = create_post(conn, alice, "decoy", name="alice", now=1001)
    edit = create_edit(conn, alice, post["hash"], "fixed", name="alice", now=1002)

    with pytest.raises(PostError):
        verify_post(dict(edit, target=other["hash"]))
    with pytest.raises(PostError):
        verify_post(dict(edit, text="sneaky"))
    with pytest.raises(PostError):
        verify_post(dict(post, kind="edit", target=post["hash"]))


def test_edit_survives_export_import_roundtrip(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "typo here", name="alice", now=1000)
    create_edit(conn, alice, post["hash"], "typo fixed", name="alice", now=1001)
    document = export_entries(conn, all_authors=True)
    assert [entry["kind"] for entry in document["entries"]] == ["post", "edit"]

    restored = init_db(tmp_path / "restored.sqlite3")
    result = import_entries(restored, document)
    assert result == {"imported": 2, "present": 0, "skipped": 0, "errors": []}
    items = timeline(restored, [post["author"]])
    assert [item["text"] for item in items] == ["typo fixed"]
    assert items[0]["edited"] is True


def test_edit_then_delete_hides_post(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "typo here", name="alice", now=1000)
    create_edit(conn, alice, post["hash"], "typo fixed", name="alice", now=1001)
    create_delete(conn, alice, post["hash"], name="alice", now=1002)

    assert timeline(conn, [post["author"]]) == []
    assert search_posts(conn, "typo") == []
    assert deleted_targets(conn) == {post["hash"]}


def test_cli_edit(tmp_path):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(
            [
                "tan", "post", "--data-dir", str(data_dir),
                "--no-publish", "--json", "cli typo here",
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        posts = feed(conn, all_authors=True)
    finally:
        conn.close()
    target = next(item for item in posts if item["text"] == "cli typo here")

    assert (
        main(
            [
                "tan", "edit", "--data-dir", str(data_dir),
                "--no-publish", "--json", target["hash"][:16], "cli typo fixed",
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        remaining = timeline(conn, include_all=True)
        assert [item["text"] for item in remaining] == ["cli typo fixed"]
        assert remaining[0]["edited"] is True
        # The original entry is untouched.
        assert get_post(conn, target["hash"])["text"] == "cli typo here"
    finally:
        conn.close()


def test_cli_edit_rejects_foreign_post(tmp_path):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    # Plant another author's post in alice's cache, as a sync would.
    other = RNS.Identity()
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        foreign = create_post(conn, other, "not yours", name="mallory", now=1000)
    finally:
        conn.close()
    assert (
        main(
            [
                "tan", "edit", "--data-dir", str(data_dir),
                "--no-publish", "--json", foreign["hash"][:16], "hijack",
            ]
        )
        != 0
    )
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        assert timeline(conn, include_all=True)[0]["text"] == "not yours"
    finally:
        conn.close()


def test_cli_delete(tmp_path):
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
                "cli delete me",
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "tan",
                "post",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                "cli keeper",
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
                "tan",
                "delete",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                doomed["hash"][:16],
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
                "tan",
                "delete",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                doomed["hash"],
            ]
        )
        != 0
    )

    # Liking a deleted post fails too.
    assert (
        main(
            [
                "tan",
                "like",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                doomed["hash"],
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
                "tan",
                "delete",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                post["hash"],
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
                "tan",
                "delete",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                unknown,
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
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(posts)")}
        assert {"kind", "target"} <= columns
    finally:
        conn.close()


def test_cli_like_and_unlike(tmp_path):
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
                "cli reactions",
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
                "tan",
                "like",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                post["hash"][:16],
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
                "tan",
                "unlike",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                post["hash"],
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
        create_post(conn, alice, "\U0001f600" * 2000, name="alice", now=1000)
    assert head(conn, author_of(alice)) is None
    assert feed(conn, author=author_of(alice)) == []

    # The chain is not poisoned: a normal post still becomes the head.
    post = create_post(conn, alice, "fits fine", name="alice", now=1001)
    assert head(conn, author_of(alice)) == post["hash"]


def test_multibyte_post_under_budget_is_accepted(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "\U0001f600" * 1500, name="alice", now=1000)
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
        "text": "\U0001f600" * 2000,
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

    reply = create_reply(conn, bob, post["hash"], "seconded", name="bob", now=1001)
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
        "edited": False,
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


def test_mentions_lists_replies_to_own_posts_and_replies(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "original thought", name="alice", now=1000)
    own_reply = create_reply(
        conn, alice, post["hash"], "talking to myself", name="alice", now=1001
    )
    reply = create_reply(conn, bob, post["hash"], "seconded", name="bob", now=1002)
    reply_to_reply = create_reply(
        conn, bob, own_reply["hash"], "even this", name="bob", now=1003
    )
    other = create_post(conn, bob, "bob's own post", name="bob", now=1004)
    to_bob = create_reply(
        conn, alice, other["hash"], "not a mention", name="alice", now=1005
    )

    found = mentions(conn, author_of(alice))
    assert [item["hash"] for item in found] == [
        reply_to_reply["hash"],
        reply["hash"],
    ]
    # Each mention carries its parent, so the reader sees what was answered.
    assert found[0]["reply_to"]["hash"] == own_reply["hash"]
    assert found[0]["reply_to"]["text"] == "talking to myself"
    assert found[1]["reply_to"]["hash"] == post["hash"]
    assert found[1]["reply_to"]["text"] == "original thought"
    # A reply to someone else's post is a mention of them, not of you.
    assert [item["hash"] for item in mentions(conn, author_of(bob))] == [to_bob["hash"]]
    assert [item["hash"] for item in mentions(conn, author_of(alice), limit=1)] == [
        reply_to_reply["hash"]
    ]


def test_mentions_hides_deleted_and_unlinked(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "original thought", name="alice", now=1000)
    reply = create_reply(conn, bob, post["hash"], "seconded", name="bob", now=1001)
    assert [item["hash"] for item in mentions(conn, author_of(alice))] == [
        reply["hash"]
    ]

    # A reply its author deleted never appears.
    create_delete(conn, bob, reply["hash"], name="bob", now=1002)
    assert mentions(conn, author_of(alice)) == []

    # A reply to a post you deleted never appears either.
    kept = create_post(conn, alice, "keeping this", name="alice", now=1003)
    fresh = create_reply(conn, bob, kept["hash"], "still here", name="bob", now=1004)
    assert [item["hash"] for item in mentions(conn, author_of(alice))] == [
        fresh["hash"]
    ]
    create_delete(conn, alice, kept["hash"], name="alice", now=1005)
    assert mentions(conn, author_of(alice)) == []

    # An unlinked reply (its chain predecessor has not arrived yet)
    # never appears, and shows up once the predecessor lands.
    source = init_db(tmp_path / "source.sqlite3")
    store_post(source, post)
    bob_first = create_post(source, bob, "bob first", name="bob", now=1006)
    late = create_reply(source, bob, post["hash"], "late arrival", name="bob", now=1007)
    store_post(conn, late)
    assert mentions(conn, author_of(alice)) == []
    store_post(conn, bob_first)
    assert [item["hash"] for item in mentions(conn, author_of(alice))] == [late["hash"]]


def test_mentions_hides_replies_beneath_deleted_ancestor(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "original thought", name="alice", now=1000)
    first = create_reply(conn, bob, post["hash"], "first reply", name="bob", now=1001)
    mine = create_reply(conn, alice, first["hash"], "my answer", name="alice", now=1002)
    nested = create_reply(conn, bob, mine["hash"], "back at you", name="bob", now=1003)

    assert [item["hash"] for item in mentions(conn, author_of(alice))] == [
        nested["hash"],
        first["hash"],
    ]

    # Bob deletes his reply: the thread view hides the whole subtree
    # under it, so the reply to my reply beneath it must not leak
    # into mentions even though its immediate parent is untouched.
    create_delete(conn, bob, first["hash"], name="bob", now=1004)
    assert mentions(conn, author_of(alice)) == []


def test_mentions_with_huge_mute_list_still_returns(identities, tmp_path):
    # A mute list larger than SQLite's old 999-variable bound still
    # loads mentions: hidden authors are bound as one JSON parameter,
    # not one placeholder each.
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "original thought", name="alice", now=1000)
    reply = create_reply(conn, bob, post["hash"], "seconded", name="bob", now=1001)
    with conn:
        conn.executemany(
            "INSERT INTO muted_authors(author, name, muted_at) VALUES(?, ?, ?)",
            [(f"{index:032x}", "", 1000.0) for index in range(1200)],
        )
    assert [item["hash"] for item in mentions(conn, author_of(alice))] == [
        reply["hash"]
    ]


def test_cli_mentions(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    capsys.readouterr()
    assert (
        main(
            [
                "tan",
                "post",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                "hello mesh",
            ]
        )
        == 0
    )
    post = json.loads(capsys.readouterr().out)["post"]

    # No replies yet is a clean empty result, not an error.
    assert main(["tan", "mentions", "--data-dir", str(data_dir), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["count"] == 0

    # A reply from another author, stored as if it had synced in.
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        create_reply(
            conn, RNS.Identity(), post["hash"], "seconded", name="bob", now=1001
        )
    finally:
        conn.close()
    assert main(["tan", "mentions", "--data-dir", str(data_dir), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["posts"][0]["text"] == "seconded"
    assert payload["posts"][0]["reply_to"]["text"] == "hello mesh"

    assert main(["tan", "mentions", "--data-dir", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "seconded" in out
    assert "in reply to alice: hello mesh" in out


def test_reply_rejects_tampering(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "target practice", name="alice", now=1000)
    other = create_post(conn, alice, "decoy", name="alice", now=1001)
    reply = create_reply(conn, bob, post["hash"], "agreed", name="bob", now=1002)

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
    reply = create_reply(source, bob, post["hash"], "early reply", name="bob", now=1001)

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

    with pytest.raises(TanError, match="only posts, replies and quotes"):
        create_reply(conn, bob, like["hash"], "reply to a like", name="bob", now=1002)

    create_delete(conn, alice, post["hash"], name="alice", now=1003)
    with pytest.raises(TanError, match="has been deleted"):
        create_reply(conn, bob, post["hash"], "too late", name="bob", now=1004)


def test_reply_can_be_liked_deleted_and_replied_to(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "thread starter", name="alice", now=1000)
    reply = create_reply(conn, bob, post["hash"], "first reply", name="bob", now=1001)
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
        "edited": False,
    }
    with pytest.raises(TanError, match="has been deleted"):
        create_reply(conn, alice, reply["hash"], "too late", name="alice", now=1005)


def test_repost_chains_verifies_and_annotates(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "original thought", name="alice", now=1000)

    repost = create_repost(conn, bob, post["hash"], name="bob", now=1001)
    assert repost["kind"] == "repost"
    assert repost["target"] == post["hash"]
    assert repost["text"] == ""
    assert verify_post(repost)["hash"] == repost["hash"]
    assert head(conn, author_of(bob)) == repost["hash"]

    # The sync feed carries the repost; display surfaces show it with
    # the original attached and a repost count on the original.
    chain = feed(conn, author=author_of(bob))
    assert [entry["kind"] for entry in chain] == ["repost"]
    items = timeline(conn, [post["author"], author_of(bob)])
    assert [item["hash"] for item in items] == [repost["hash"], post["hash"]]
    assert items[0]["repost_of"] == {
        "hash": post["hash"],
        "author": post["author"],
        "name": "alice",
        "text": "original thought",
        "deleted": False,
    }
    assert "repost_of" not in items[1]
    assert repost_counts(conn, [post["hash"]])[post["hash"]] == {
        "reposts": 1,
        "reposted": False,
    }
    assert repost_counts(conn, [post["hash"]], viewer=author_of(bob))[
        post["hash"]
    ] == {"reposts": 1, "reposted": True}
    assert items[1]["reposts"] == 1
    assert items[1]["reposted"] is False
    assert items[0]["reposts"] == 0
    assert [item["hash"] for item in feed(conn, posts_only=True)] == [
        repost["hash"],
        post["hash"],
    ]
    assert [item["hash"] for item in timeline(conn, include_all=True)] == [
        repost["hash"],
        post["hash"],
    ]


def test_repost_rejects_bad_targets(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "worth sharing", name="alice", now=1000)
    like = create_reaction(conn, bob, post["hash"], liked=True, name="bob", now=1001)

    with pytest.raises(TanError, match="unknown post"):
        create_repost(conn, bob, "cd" * 32, name="bob", now=1002)
    with pytest.raises(TanError, match="only posts and replies"):
        create_repost(conn, bob, like["hash"], name="bob", now=1002)
    with pytest.raises(TanError, match="cannot repost your own post"):
        create_repost(conn, alice, post["hash"], name="alice", now=1002)

    # A repost of a repost is a repost of a repost, not of a post.
    repost = create_repost(conn, bob, post["hash"], name="bob", now=1002)
    with pytest.raises(TanError, match="only posts and replies"):
        create_repost(conn, alice, repost["hash"], name="alice", now=1003)
    # And the same post cannot be reposted twice while it stands.
    with pytest.raises(TanError, match="already been reposted"):
        create_repost(conn, bob, post["hash"], name="bob", now=1003)


def test_repost_to_unlinked_parent_is_rejected(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    first = create_post(source, alice, "first", name="alice", now=1000)
    second = create_post(source, alice, "second", name="alice", now=1001)

    # Only the second post arrives: it is cached but unlinked, so
    # reposting it would amplify a post with no chain context.
    destination = init_db(tmp_path / "destination.sqlite3")
    store_post(destination, second)
    with pytest.raises(TanError, match="has not linked yet"):
        create_repost(destination, bob, second["hash"], name="bob", now=1002)

    # Once the chain links up, the same repost is accepted.
    store_post(destination, first)
    repost = create_repost(destination, bob, second["hash"], name="bob", now=1003)
    assert repost["kind"] == "repost"


def test_repost_of_uncached_original_shows_without_text(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    post = create_post(source, alice, "original thought", name="alice", now=1000)
    repost = create_repost(source, bob, post["hash"], name="bob", now=1001)

    # The repost arrives (via gossip or sync) before its original: the
    # store path keeps it, and the timeline shows it without the
    # original's text until the original is cached.
    replica = init_db(tmp_path / "replica.sqlite3")
    record, is_new = store_post(replica, repost)
    assert is_new is True
    items = timeline(replica, [author_of(bob)])
    assert [item["hash"] for item in items] == [repost["hash"]]
    assert items[0]["repost_of"] is None

    store(replica, post)
    items = timeline(replica, [author_of(bob)])
    assert items[0]["repost_of"]["hash"] == post["hash"]
    assert items[0]["repost_of"]["text"] == "original thought"


def test_repost_of_deleted_original_blanks_the_text(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "doomed", name="alice", now=1000)
    repost = create_repost(conn, bob, post["hash"], name="bob", now=1001)
    create_delete(conn, alice, post["hash"], name="alice", now=1002)

    # The repost still stands — only the original was deleted — but
    # its text is blanked, and reposting the deleted post is refused.
    items = timeline(conn, [post["author"], author_of(bob)])
    assert [item["hash"] for item in items] == [repost["hash"]]
    assert items[0]["repost_of"] == {
        "hash": post["hash"],
        "author": post["author"],
        "name": "alice",
        "text": "",
        "deleted": True,
    }
    assert repost_counts(conn, [post["hash"]])[post["hash"]]["reposts"] == 1
    with pytest.raises(TanError, match="has been deleted"):
        create_repost(conn, bob, post["hash"], name="bob", now=1003)


def test_repost_of_expired_original_blanks_the_text(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    now = time.time()
    post = create_post(
        conn, alice, "fading", name="alice", now=now, expires_at=now + 3600
    )
    repost = create_repost(conn, bob, post["hash"], name="bob", now=now)
    _expire_stored_post(conn, post["hash"], at=now - 10)

    # The repost still stands — only the original expired — but its
    # text is blanked, so a live repost cannot resurface disappeared
    # content through its summary.
    items = timeline(conn, [post["author"], author_of(bob)])
    assert [item["hash"] for item in items] == [repost["hash"]]
    assert items[0]["repost_of"] == {
        "hash": post["hash"],
        "author": post["author"],
        "name": "alice",
        "text": "",
        "deleted": False,
        "expired": True,
    }


def test_repost_of_a_reply(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "thread starter", name="alice", now=1000)
    reply = create_reply(
        conn, bob, post["hash"], "first reply", name="bob", now=1001
    )
    repost = create_repost(conn, alice, reply["hash"], name="alice", now=1002)
    assert repost["kind"] == "repost"
    items = timeline(conn, [post["author"], author_of(bob)])
    assert [item["hash"] for item in items] == [
        repost["hash"],
        reply["hash"],
        post["hash"],
    ]
    assert items[0]["repost_of"]["hash"] == reply["hash"]
    assert items[0]["repost_of"]["text"] == "first reply"


def test_timeline_drops_repost_of_blocked_author(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, bob, "blocked words", name="bob", now=1000)
    create_repost(conn, alice, post["hash"], name="alice", now=1001)
    other = create_post(conn, alice, "alice's own post", name="alice", now=1002)

    # A repost carries no text of its own, so blocking the original's
    # author hides the repost itself — its words must not leak
    # through the reposter's timeline.
    blocked = [author_of(bob)]
    items = timeline(conn, [author_of(alice)], exclude_authors=blocked)
    assert [item["hash"] for item in items] == [other["hash"]]
    assert "blocked words" not in json.dumps(items)
    items = timeline(conn, include_all=True, exclude_authors=blocked)
    assert [item["hash"] for item in items] == [other["hash"]]


def test_timeline_drops_repost_of_muted_author(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, bob, "muted words", name="bob", now=1000)
    create_repost(conn, alice, post["hash"], name="alice", now=1001)
    other = create_post(conn, alice, "alice's own post", name="alice", now=1002)

    mute(conn, author_of(bob), name="bob")
    items = timeline(conn, [author_of(alice)])
    assert [item["hash"] for item in items] == [other["hash"]]
    assert "muted words" not in json.dumps(items)


def test_timeline_drops_repost_matching_muted_keyword(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "crypto secrets", name="alice", now=1000)
    create_repost(conn, bob, post["hash"], name="bob", now=1001)
    other = create_post(conn, bob, "clean post", name="bob", now=1002)
    mute_keyword(conn, "crypto")

    items = timeline(conn, [author_of(bob)])
    assert [item["hash"] for item in items] == [other["hash"]]
    assert "crypto secrets" not in json.dumps(items)


def test_repost_of_edited_original_shows_edited_text(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "original draft", name="alice", now=1000)
    repost = create_repost(conn, bob, post["hash"], name="bob", now=1001)
    create_edit(conn, alice, post["hash"], "original, fixed", name="alice", now=1002)

    items = timeline(conn, [author_of(bob)])
    assert [item["hash"] for item in items] == [repost["hash"]]
    assert items[0]["repost_of"]["text"] == "original, fixed"
    assert "original draft" not in json.dumps(items)


def test_author_timeline_drops_repost_of_blocked_author(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, bob, "blocked words", name="bob", now=1000)
    create_repost(conn, alice, post["hash"], name="alice", now=1001)
    other = create_post(conn, alice, "alice's own post", name="alice", now=1002)

    items = author_timeline(
        conn, author_of(alice), exclude_authors=[author_of(bob)]
    )
    assert [item["hash"] for item in items] == [other["hash"]]
    assert "blocked words" not in json.dumps(items)


def test_author_timeline_keeps_repost_of_muted_author(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, bob, "muted words", name="bob", now=1000)
    repost = create_repost(conn, alice, post["hash"], name="alice", now=1001)

    # An explicit author lookup still shows the repost, the same way
    # it still shows a reply to a muted parent.
    mute(conn, author_of(bob), name="bob")
    items = author_timeline(conn, author_of(alice))
    assert [item["hash"] for item in items] == [repost["hash"]]
    assert items[0]["repost_of"]["text"] == "muted words"


def test_repost_can_be_deleted_and_reposted_again(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "worth sharing", name="alice", now=1000)
    repost = create_repost(conn, bob, post["hash"], name="bob", now=1001)
    assert repost_counts(conn, [post["hash"]])[post["hash"]]["reposts"] == 1

    # Deleting your own repost removes it from views and from the count.
    create_delete(conn, bob, repost["hash"], name="bob", now=1002)
    assert deleted_targets(conn) == {repost["hash"]}
    items = timeline(conn, [post["author"], author_of(bob)])
    assert [item["hash"] for item in items] == [post["hash"]]
    assert items[0]["reposts"] == 0

    # And reposting again after deleting is allowed.
    again = create_repost(conn, bob, post["hash"], name="bob", now=1003)
    assert again["kind"] == "repost"
    assert repost_counts(conn, [post["hash"]])[post["hash"]]["reposts"] == 1


def test_repost_counts_distinct_reposters(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "worth sharing", name="alice", now=1000)
    first = create_repost(conn, bob, post["hash"], name="bob", now=1001)
    assert repost_counts(conn, [post["hash"]])[post["hash"]]["reposts"] == 1

    # A second repost of the same target from the same author — stored
    # past create_repost's duplicate guard, as a forked chain could
    # deliver — still counts as one reposter, not two.
    body = {
        "v": 1,
        "author": author_of(bob),
        "key": bob.get_public_key().hex(),
        "name": "bob",
        "text": "",
        "created_at": 1002.0,
        "prev": first["hash"],
        "kind": "repost",
        "target": post["hash"],
    }
    digest = hash(body, previous=first["hash"])
    mesg = dict(body, hash=digest, sig=bob.sign(bytes.fromhex(digest)).hex())
    record, is_new = store_post(conn, mesg)
    assert is_new is True
    assert repost_counts(conn, [post["hash"]])[post["hash"]] == {
        "reposts": 1,
        "reposted": False,
    }
    assert repost_counts(conn, [post["hash"]], viewer=author_of(bob))[
        post["hash"]
    ]["reposted"] is True


def test_thread_resolves_a_repost_to_its_original(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "thread starter", name="alice", now=1000)
    reply = create_reply(
        conn, bob, post["hash"], "first reply", name="bob", now=1001
    )
    repost = create_repost(conn, bob, post["hash"], name="bob", now=1002)

    # Naming the repost opens the original's thread; the repost itself
    # is not a thread entry.
    assert [item["hash"] for item in thread(conn, repost["hash"][:12])] == [
        post["hash"],
        reply["hash"],
    ]
    assert resolve_post(conn, repost["hash"][:12])["hash"] == repost["hash"]


def test_cli_tan_repost(tmp_path, capsys):
    bob = RNS.Identity()
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    capsys.readouterr()
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        post = create_post(conn, bob, "worth sharing", name="bob", now=1000)
    finally:
        conn.close()

    # JSON output carries the signed repost entry.
    assert (
        main(
            [
                "tan", "repost", "--data-dir", str(data_dir),
                "--no-publish", "--json", post["hash"][:16],
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["repost"]["kind"] == "repost"
    assert payload["repost"]["target"] == post["hash"]
    assert payload["published"] is None

    # Text timeline output shows who reposted and the original text.
    assert (
        main(["tan", "timeline", "--data-dir", str(data_dir), "--all"]) == 0
    )
    out = capsys.readouterr().out
    assert "reposted bob" in out
    assert "worth sharing" in out
    assert "↻ 1" in out

    # Reposting the same post again is a clean CLI error, not a traceback.
    assert (
        main(
            [
                "tan", "repost", "--data-dir", str(data_dir),
                "--no-publish", post["hash"][:16],
            ]
        )
        != 0
    )
    assert "already been reposted" in capsys.readouterr().err


def test_quote_chains_verifies_and_annotates(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "original thought", name="alice", now=1000)

    quote = create_quote(
        conn, bob, post["hash"], "worth sharing", name="bob", now=1001
    )
    assert quote["kind"] == "quote"
    assert quote["target"] == post["hash"]
    assert quote["text"] == "worth sharing"
    assert verify_post(quote)["hash"] == quote["hash"]
    assert head(conn, author_of(bob)) == quote["hash"]

    # The sync feed carries the quote; display surfaces show it with
    # the original attached and the original carrying a quote count.
    chain = feed(conn, author=author_of(bob))
    assert [entry["kind"] for entry in chain] == ["quote"]
    items = timeline(conn, [post["author"], author_of(bob)])
    assert [item["hash"] for item in items] == [quote["hash"], post["hash"]]
    assert items[0]["quoted"] == {
        "hash": post["hash"],
        "author": post["author"],
        "name": "alice",
        "text": "original thought",
        "deleted": False,
    }
    assert items[0]["quotes"] == 0
    assert items[1]["quotes"] == 1
    assert "quoted" not in items[1]
    assert [item["hash"] for item in feed(conn, posts_only=True)] == [
        quote["hash"],
        post["hash"],
    ]
    assert [item["hash"] for item in timeline(conn, include_all=True)] == [
        quote["hash"],
        post["hash"],
    ]
    assert resolve_post(conn, quote["hash"][:12])["hash"] == quote["hash"]


def test_quote_rejects_tampering(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "target practice", name="alice", now=1000)
    other = create_post(conn, alice, "decoy", name="alice", now=1001)
    quote = create_quote(
        conn, bob, post["hash"], "agreed", name="bob", now=1002
    )

    # The quoted link is signed: swapping it invalidates the signature.
    with pytest.raises(PostError):
        verify_post(dict(quote, target=other["hash"]))
    with pytest.raises(PostError):
        verify_post(dict(quote, text="sneaky"))
    # A quote without an original, and an empty quote, are rejected.
    with pytest.raises(PostError):
        verify_post(dict(quote, target=""))
    with pytest.raises(ProtocolError, match="must not be empty"):
        create_quote(conn, bob, post["hash"], "   ", name="bob", now=1003)


def test_quote_to_uncached_parent_is_rejected_when_authoring(identities, tmp_path):
    _, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    with pytest.raises(TanError, match="unknown post"):
        create_quote(conn, bob, "cd" * 32, "early quote", name="bob", now=1000)


def test_quote_to_unlinked_parent_is_rejected(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    first = create_post(source, alice, "first", name="alice", now=1000)
    second = create_post(source, alice, "second", name="alice", now=1001)

    # Only the second post arrives: it is cached but unlinked, so
    # quoting it would produce a quote with no original context.
    destination = init_db(tmp_path / "destination.sqlite3")
    store_post(destination, second)
    with pytest.raises(TanError, match="has not linked yet"):
        create_quote(
            destination, bob, second["hash"], "too early", name="bob", now=1002
        )

    # Once the chain links up, the same quote is accepted.
    store_post(destination, first)
    quote = create_quote(
        destination, bob, second["hash"], "in full", name="bob", now=1003
    )
    assert quote["kind"] == "quote"


def test_quote_to_parent_not_yet_synced_shows_without_embed(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    post = create_post(source, alice, "original thought", name="alice", now=1000)
    quote = create_quote(
        source, bob, post["hash"], "early quote", name="bob", now=1001
    )

    # The quote arrives (via gossip or sync) before its original: the
    # store path keeps it, and the timeline shows it without the
    # embedded original until the original is cached.
    replica = init_db(tmp_path / "replica.sqlite3")
    record, is_new = store_post(replica, quote)
    assert is_new is True
    items = timeline(replica, [author_of(bob)])
    assert [item["hash"] for item in items] == [quote["hash"]]
    assert items[0]["quoted"] is None

    store(replica, post)
    items = timeline(replica, [author_of(bob)])
    assert items[0]["quoted"]["hash"] == post["hash"]


def test_quote_to_invalid_target_is_rejected(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "doomed", name="alice", now=1000)
    like = create_reaction(conn, bob, post["hash"], liked=True, name="bob", now=1001)
    quote = create_quote(
        conn, bob, post["hash"], "first quote", name="bob", now=1002
    )

    # Reactions, deletes and quotes are not quotable: embeds stay one
    # level deep and always point at a post or a reply.
    with pytest.raises(TanError, match="only posts and replies"):
        create_quote(conn, bob, like["hash"], "quote a like", name="bob", now=1003)
    with pytest.raises(TanError, match="only posts and replies"):
        create_quote(
            conn, alice, quote["hash"], "quote a quote", name="alice", now=1004
        )

    create_delete(conn, alice, post["hash"], name="alice", now=1005)
    with pytest.raises(TanError, match="has been deleted"):
        create_quote(conn, bob, post["hash"], "too late", name="bob", now=1006)


def test_quote_of_own_post_is_allowed(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "my take", name="alice", now=1000)
    quote = create_quote(
        conn, alice, post["hash"], "adding context", name="alice", now=1001
    )
    assert quote["kind"] == "quote"
    assert quote["author"] == post["author"]
    assert quote_counts(conn, [post["hash"]])[post["hash"]]["quotes"] == 1


def test_quote_can_be_liked_deleted_and_replied_to(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "thread starter", name="alice", now=1000)
    quote = create_quote(
        conn, bob, post["hash"], "amplifying this", name="bob", now=1001
    )
    reply = create_reply(
        conn, alice, quote["hash"], "reply to the quote", name="alice", now=1002
    )

    # Quotes are posts for liking, resolving and replying: a reply to
    # a quote points at the quote, and the quote is its own thread
    # root rather than a child of the original's thread.
    items = timeline(conn, [post["author"], author_of(bob)])
    assert [item["hash"] for item in items] == [
        reply["hash"],
        quote["hash"],
        post["hash"],
    ]
    assert items[0]["reply_to"]["hash"] == quote["hash"]
    assert items[1]["quoted"]["hash"] == post["hash"]
    assert items[2]["quotes"] == 1
    assert resolve_post(conn, quote["hash"][:12])["hash"] == quote["hash"]

    create_reaction(conn, alice, quote["hash"], liked=True, name="alice", now=1003)
    assert like_counts(conn, [quote["hash"]])[quote["hash"]]["likes"] == 1

    items = thread(conn, quote["hash"])
    assert [item["hash"] for item in items] == [quote["hash"], reply["hash"]]
    assert [item["depth"] for item in items] == [0, 1]
    assert items[0]["quoted"]["hash"] == post["hash"]
    # The quote is not a child of the original's thread either.
    items = thread(conn, post["hash"])
    assert [item["hash"] for item in items] == [post["hash"]]

    # Deleting the quote hides it and drops the count; the reply to
    # it stays in the timeline, with its parent annotation reporting
    # the deletion without resurfacing the text.
    create_delete(conn, bob, quote["hash"], name="bob", now=1004)
    assert deleted_targets(conn) == {quote["hash"]}
    assert quote_counts(conn, [post["hash"]])[post["hash"]]["quotes"] == 0
    items = timeline(conn, [post["author"], author_of(bob)])
    assert [item["hash"] for item in items] == [reply["hash"], post["hash"]]
    assert items[0]["reply_to"] == {
        "hash": quote["hash"],
        "author": quote["author"],
        "name": "bob",
        "text": "",
        "deleted": True,
        "edited": False,
    }
    with pytest.raises(TanError, match="has been deleted"):
        thread(conn, quote["hash"])

    # A reaction is not a timeline entry, so it cannot be deleted.
    like = create_reaction(conn, alice, post["hash"], liked=True, name="alice", now=1005)
    with pytest.raises(
        TanError, match="only posts, replies, reposts and quotes can be deleted"
    ):
        create_delete(conn, alice, like["hash"], name="alice", now=1006)


def test_quote_of_deleted_post_renders_blanked(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "doomed", name="alice", now=1000)
    quote = create_quote(
        conn, bob, post["hash"], "still worth it", name="bob", now=1001
    )

    # The quote survives its original's deletion, but the embedded
    # original is blanked rather than resurfacing removed content.
    create_delete(conn, alice, post["hash"], name="alice", now=1002)
    items = timeline(conn, [post["author"], author_of(bob)])
    assert [item["hash"] for item in items] == [quote["hash"]]
    assert items[0]["quoted"] == {
        "hash": post["hash"],
        "author": post["author"],
        "name": "alice",
        "text": "",
        "deleted": True,
    }
    # The deleted original's quote count still describes the quoting.
    assert quote_counts(conn, [post["hash"]])[post["hash"]]["quotes"] == 1


def test_quote_of_blocked_post_renders_redacted(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "secret plans", name="alice", now=1000)
    quote = create_quote(
        conn, bob, post["hash"], "worth quoting", name="bob", now=1001
    )

    # Blocking the original's author redacts the embedded original —
    # name and text included — so the block never leaks through a
    # quote preview, on the timeline or in search.
    blocked = {post["author"]}
    items = timeline(
        conn, [post["author"], author_of(bob)], exclude_authors=blocked
    )
    assert [item["hash"] for item in items] == [quote["hash"]]
    assert items[0]["quoted"] == {
        "hash": post["hash"],
        "author": "",
        "name": "",
        "text": "",
        "deleted": False,
        "blocked": True,
    }
    results = search_posts(conn, "quoting", exclude_authors=blocked)
    assert [item["hash"] for item in results] == [quote["hash"]]
    assert results[0]["quoted"]["blocked"] is True
    assert results[0]["quoted"]["text"] == ""
    assert results[0]["quoted"]["name"] == ""


def test_search_includes_quotes(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "mesh networking", name="alice", now=1000)
    quote = create_quote(
        conn, bob, post["hash"], "meshtastic thoughts", name="bob", now=1001
    )

    # A quote matches on its own comment text, with the original
    # attached like the timeline attaches it.
    results = search_posts(conn, "meshtastic")
    assert [item["hash"] for item in results] == [quote["hash"]]
    assert results[0]["quoted"]["hash"] == post["hash"]
    # The original still matches on its own text.
    results = search_posts(conn, "networking")
    assert [item["hash"] for item in results] == [post["hash"]]


def test_cli_quote(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(
            [
                "tan", "post", "--data-dir", str(data_dir),
                "--no-publish", "--json", "original thought",
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
                "tan", "quote", "--data-dir", str(data_dir),
                "--no-publish", "--json", post["hash"][:16], "worth sharing",
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        assert quote_counts(conn, [post["hash"]])[post["hash"]]["quotes"] == 1
        items = timeline(conn, [post["author"]])
        assert [item["kind"] for item in items] == ["quote", "post"]
        assert items[0]["quoted"]["hash"] == post["hash"]
    finally:
        conn.close()
    # The timeline shows the quote with its embedded original.
    assert (
        main(["tan", "timeline", "--data-dir", str(data_dir), "--all"])
        == 0
    )
    captured = capsys.readouterr()
    assert "quoting alice:\n    original thought" in captured.out
    assert "❝ 1" in captured.out


def test_thread_returns_root_and_nested_replies(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "thread starter", name="alice", now=1000)
    first = create_reply(conn, bob, post["hash"], "first reply", name="bob", now=1001)
    nested = create_reply(
        conn, alice, first["hash"], "reply to the reply", name="alice", now=1002
    )
    second = create_reply(conn, bob, post["hash"], "second reply", name="bob", now=1003)
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
    first = create_reply(conn, bob, post["hash"], "first reply", name="bob", now=1001)
    nested = create_reply(
        conn, alice, first["hash"], "reply to the reply", name="alice", now=1002
    )
    second = create_reply(conn, bob, post["hash"], "second reply", name="bob", now=1003)

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
    first = create_reply(conn, bob, post["hash"], "first reply", name="bob", now=1001)
    nested = create_reply(
        conn, alice, first["hash"], "reply to the reply", name="alice", now=1002
    )
    create_delete(conn, bob, first["hash"], name="bob", now=1003)

    # Naming the reply directly must not show it as a standalone thread.
    with pytest.raises(TanError, match="has been deleted"):
        thread(conn, nested["hash"])


def test_thread_rejects_reply_beneath_deleted_unlinked_ancestor(identities, tmp_path):
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
        conn,
        delete,
        author=author_of(alice),
        linked=True,
        kind="delete",
        target=parent,
    )
    _insert_stub(
        conn,
        reply,
        author=author_of(bob),
        linked=True,
        kind="reply",
        target=parent,
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
                conn,
                alice,
                reply["hash"],
                f"child {index}",
                name="alice",
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
                "tan",
                "post",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                "cli thread starter",
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
                "tan",
                "post",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                "--reply-to",
                post["hash"][:16],
                "cli reply",
            ]
        )
        == 0
    )
    capsys.readouterr()

    # JSON output carries the nested thread with depths.
    assert (
        main(
            [
                "tan",
                "thread",
                "--data-dir",
                str(data_dir),
                "--json",
                post["hash"][:16],
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
    assert main(["tan", "thread", "--data-dir", str(data_dir), post["hash"][:16]]) == 0
    out = capsys.readouterr().out
    assert "cli thread starter" in out
    assert "  cli reply" in out
    # Each entry's text is printed exactly once in text mode.
    assert out.count("cli thread starter") == 1
    assert out.count("cli reply") == 1

    # An unknown reference is a clean CLI error, not a traceback.
    assert main(["tan", "thread", "--data-dir", str(data_dir), "cd" * 32]) != 0
    assert "tau: error: unknown post" in capsys.readouterr().err


def test_cli_tan_likes(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(
            [
                "tan", "post", "--data-dir", str(data_dir),
                "--no-publish", "--json", "cli likeable",
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        post = feed(conn, all_authors=True)[0]
    finally:
        conn.close()
    capsys.readouterr()

    # No likes yet is a successful empty result, not an error.
    assert (
        main(["tan", "likes", "--data-dir", str(data_dir), post["hash"][:16]])
        == 0
    )
    assert "No likes yet." in capsys.readouterr().out

    assert (
        main(
            [
                "tan", "like", "--data-dir", str(data_dir),
                "--no-publish", "--json", post["hash"][:16],
            ]
        )
        == 0
    )
    capsys.readouterr()

    # JSON output names the liker.
    assert (
        main(
            [
                "tan", "likes", "--data-dir", str(data_dir),
                "--json", post["hash"][:16],
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["post"]["hash"] == post["hash"]
    assert payload["likers"] == [
        {"author": payload["post"]["author"], "name": "alice"}
    ]

    # Text output shows the post and the liker.
    assert (
        main(["tan", "likes", "--data-dir", str(data_dir), post["hash"][:16]])
        == 0
    )
    out = capsys.readouterr().out
    assert "cli likeable" in out
    assert "♥ 1 like(s):" in out
    assert "alice" in out

    # An unknown reference is a clean CLI error, not a traceback.
    assert (
        main(["tan", "likes", "--data-dir", str(data_dir), "cd" * 32]) != 0
    )
    assert "tau: error: unknown post" in capsys.readouterr().err

    # A post the owner's display rules hide is refused, like the thread view.
    assert (
        main(
            [
                "tan", "rules", "--data-dir", str(data_dir),
                "--kinds", "like",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert (
        main(["tan", "likes", "--data-dir", str(data_dir), post["hash"][:16]])
        != 0
    )
    assert "hidden by your Tan display rules" in capsys.readouterr().err


def test_cli_post_reply_to(tmp_path):
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
                "cli thread starter",
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
                "tan",
                "post",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                "--reply-to",
                post["hash"][:16],
                "cli reply",
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
                "tan",
                "post",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                "--reply-to",
                "cd" * 32,
                "nowhere",
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
                "tan",
                "post",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                "cli thread starter",
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
                "tan",
                "post",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                "--reply-to",
                post["hash"][:16],
                "cli reply",
            ]
        )
        == 0
    )

    # Text output names the parent instead of showing a standalone post.
    assert (
        main(
            [
                "tan",
                "timeline",
                "--data-dir",
                str(data_dir),
                "--author",
                post["author"],
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
                "tan",
                "timeline",
                "--data-dir",
                str(data_dir),
                "--author",
                post["author"],
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    reply = next(item for item in payload["posts"] if item["kind"] == "reply")
    assert reply["reply_to"]["hash"] == post["hash"]
    assert reply["reply_to"]["text"] == "cli thread starter"


def test_resolve_author_ignores_aliases(tmp_path):
    from tau.peers import PeerRegistry

    conn = init_db(tmp_path / "tan.sqlite3")
    registry = PeerRegistry()
    named = registry.upsert(
        bytes([1]) * 16, bytes([1]) * 64, name="alice", now=100.0
    )
    aliased = registry.upsert(
        bytes([2]) * 16, bytes([2]) * 64, name="bob", now=200.0
    )
    registry.set_alias(aliased.destination_hash, "alice")
    author, name = resolve_author(conn, "alice", registry=registry)
    assert author == named.hexhash
    assert name == "alice"


def test_mute_keyword_round_trip(tmp_path):
    conn = init_db(tmp_path / "tan.sqlite3")
    record = mute_keyword(conn, "Crypto", now=1000)
    assert record == {"keyword": "crypto", "display": "Crypto", "added_at": 1000.0}
    mute_keyword(conn, "mesh", now=1001)

    # Newest first; the display form the user typed is kept.
    assert muted_keywords(conn) == [
        {"keyword": "mesh", "display": "mesh", "added_at": 1001.0},
        {"keyword": "crypto", "display": "Crypto", "added_at": 1000.0},
    ]

    # Re-muting is a no-op: same keyword, original time and display.
    assert mute_keyword(conn, "CRYPTO", now=1002) == {
        "keyword": "crypto",
        "display": "Crypto",
        "added_at": 1000.0,
    }
    assert len(muted_keywords(conn)) == 2

    assert unmute_keyword(conn, "crypto") is True
    assert unmute_keyword(conn, "crypto") is False
    assert [record["keyword"] for record in muted_keywords(conn)] == ["mesh"]


def test_mute_keyword_rejects_empty_or_oversized(tmp_path):
    conn = init_db(tmp_path / "tan.sqlite3")
    for bad in ("", "   "):
        with pytest.raises(TanError, match="keyword must not be empty"):
            mute_keyword(conn, bad)
        with pytest.raises(TanError, match="keyword must not be empty"):
            unmute_keyword(conn, bad)
    with pytest.raises(TanError, match="limited to 100 characters"):
        mute_keyword(conn, "x" * 101)


def test_timeline_hides_muted_keyword_posts(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    noisy = create_post(conn, alice, "CRYPTO to the moon", name="alice", now=1000)
    kept = create_post(conn, alice, "starting the mesh node", name="alice", now=1001)
    phrase = create_post(
        conn, alice, "talking about climate change today", name="alice", now=1002
    )
    talks = create_post(conn, alice, "climate talks next week", name="alice", now=1003)

    mute_keyword(conn, "crypto")
    mute_keyword(conn, "climate change")
    authors = [alice and author_of(alice)]

    # Whole-word and case-insensitive: "crypto" hides the shouty post,
    # "climate change" hides the phrase but not "climate talks", and
    # "starting" is untouched — muting "art" would never hide "start".
    assert [item["hash"] for item in timeline(conn, authors)] == [
        talks["hash"],
        kept["hash"],
    ]
    assert [item["hash"] for item in timeline(conn, include_all=True)] == [
        talks["hash"],
        kept["hash"],
    ]

    # Muting is display-only: unmuting restores the hidden posts.
    unmute_keyword(conn, "crypto")
    unmute_keyword(conn, "climate change")
    assert [item["hash"] for item in timeline(conn, authors)] == [
        talks["hash"],
        phrase["hash"],
        kept["hash"],
        noisy["hash"],
    ]


def test_mute_keyword_collapses_whitespace(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    record = mute_keyword(conn, "climate  change")
    assert record["keyword"] == "climate change"
    # Re-muting with different spacing is the same mute.
    assert mute_keyword(conn, "Climate Change")["added_at"] == record["added_at"]
    assert len(muted_keywords(conn)) == 1

    # Any whitespace run inside the phrase matches: newline, double
    # space, and tab alike.
    create_post(conn, alice, "climate\nchange is real", name="alice", now=1000)
    create_post(conn, alice, "climate  change again", name="alice", now=1001)
    create_post(conn, alice, "climate\tchange too", name="alice", now=1002)
    create_post(conn, alice, "climate talks", name="alice", now=1003)
    items = timeline(conn, [author_of(alice)])
    assert [item["text"] for item in items] == ["climate talks"]


def test_timeline_whole_word_match_ignores_substrings(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "start the engine", name="alice", now=1000)
    create_post(conn, alice, "the art of mesh", name="alice", now=1001)

    mute_keyword(conn, "art")
    items = timeline(conn, [author_of(alice)])
    assert [item["text"] for item in items] == ["start the engine"]


def test_timeline_blanks_muted_reply_parent_preview(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    parent = create_post(conn, alice, "crypto secrets", name="alice", now=1000)
    reply = create_reply(conn, bob, parent["hash"], "noted", name="bob", now=1001)
    mute_keyword(conn, "crypto")

    items = timeline(conn, [author_of(bob)])
    assert [item["hash"] for item in items] == [reply["hash"]]
    preview = items[0]["reply_to"]
    assert preview["hash"] == parent["hash"]
    assert preview["text"] == ""
    assert preview["muted"] is True
    assert preview["deleted"] is False


def test_timeline_pages_past_muted_posts(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "older clean post", name="alice", now=1000)
    for index in range(25):
        create_post(conn, alice, f"crypto {index}", name="alice", now=2000 + index)
    mute_keyword(conn, "crypto")

    # More muted posts than one page must not hide the older clean post.
    items = timeline(conn, include_all=True, limit=5)
    assert [item["text"] for item in items] == ["older clean post"]


def test_author_timeline_pages_past_muted_posts(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    for index in range(25):
        create_post(conn, alice, f"crypto {index}", name="alice", now=1000 + index)
    create_post(conn, alice, "clean ending", name="alice", now=2000)
    mute_keyword(conn, "crypto")

    items = author_timeline(conn, author_of(alice), limit=5)
    assert [item["text"] for item in items] == ["clean ending"]


def test_thread_hides_muted_reply_subtree(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "thread starter", name="alice", now=1000)
    first = create_reply(conn, bob, post["hash"], "crypto thread", name="bob", now=1001)
    nested = create_reply(
        conn, alice, first["hash"], "reply to the muted reply", name="alice", now=1002
    )
    second = create_reply(conn, bob, post["hash"], "second reply", name="bob", now=1003)
    mute_keyword(conn, "crypto")

    # The muted reply hides its whole subtree, like a deleted reply.
    items = thread(conn, post["hash"])
    assert [item["hash"] for item in items] == [post["hash"], second["hash"]]

    # The entry you name always anchors the thread, even when its own
    # text matches a muted keyword.
    assert [item["hash"] for item in thread(conn, first["hash"])] == [
        first["hash"],
        nested["hash"],
    ]


def test_thread_stops_climb_at_muted_ancestor(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "crypto news", name="alice", now=1000)
    reply = create_reply(conn, bob, post["hash"], "clean reply", name="bob", now=1001)
    nested = create_reply(
        conn, alice, reply["hash"], "deeper reply", name="alice", now=1002
    )
    mute_keyword(conn, "crypto")

    # The muted root is not promoted: the named reply anchors.
    items = thread(conn, nested["hash"])
    assert [item["hash"] for item in items] == [reply["hash"], nested["hash"]]
    assert [item["depth"] for item in items] == [0, 1]


def test_thread_rejects_muted_named_reply_beneath_deleted_post(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "thread starter", name="alice", now=1000)
    muted = create_reply(conn, bob, post["hash"], "crypto thread", name="bob", now=1001)
    create_delete(conn, alice, post["hash"], name="alice", now=1002)
    mute_keyword(conn, "crypto")

    # A muted entry skips the visible climb, but a deleted ancestor
    # still hides it rather than showing it as a fragment.
    with pytest.raises(TanError, match="has been deleted"):
        thread(conn, muted["hash"])


def test_thread_rejects_reply_beneath_muted_ancestor_with_deleted_parent(
    identities, tmp_path
):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "thread starter", name="alice", now=1000)
    muted = create_reply(conn, bob, post["hash"], "crypto chat", name="bob", now=1001)
    nested = create_reply(
        conn, alice, muted["hash"], "deeper reply", name="alice", now=1002
    )
    create_delete(conn, alice, post["hash"], name="alice", now=1003)
    mute_keyword(conn, "crypto")

    # The muted ancestor ends the visible climb, but the deleted post
    # above it must still be detected.
    with pytest.raises(TanError, match="has been deleted"):
        thread(conn, nested["hash"])


def test_search_ignores_muted_keywords(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "crypto roundup", name="alice", now=1000)
    mute_keyword(conn, "crypto")

    # Search is an explicit lookup: it still finds the muted word.
    assert [item["hash"] for item in search_posts(conn, "crypto")] == [post["hash"]]


def test_without_muted_keywords_keeps_unmuted_posts(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    noisy = create_post(conn, alice, "crypto spam", name="alice", now=1000)
    kept = create_post(conn, alice, "mesh notes", name="alice", now=1001)

    posts = [noisy, kept]
    assert without_muted_keywords(conn, posts) == posts
    mute_keyword(conn, "crypto")
    assert without_muted_keywords(conn, posts) == [kept]


def test_cli_mute_keyword(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    capsys.readouterr()
    for text in ("crypto to the moon", "mesh notes"):
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
                "mute-keyword",
                "--data-dir",
                str(data_dir),
                "--json",
                "crypto",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["keyword"] == "crypto"

    assert main(["tan", "timeline", "--data-dir", str(data_dir), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert [post["text"] for post in payload["posts"]] == ["mesh notes"]

    assert main(["tan", "muted-keywords", "--data-dir", str(data_dir), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["muted_keywords"][0]["keyword"] == "crypto"

    assert (
        main(
            [
                "tan",
                "unmute-keyword",
                "--data-dir",
                str(data_dir),
                "--json",
                "crypto",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["unmuted"] is True

    assert main(["tan", "timeline", "--data-dir", str(data_dir), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert [post["text"] for post in payload["posts"]] == [
        "mesh notes",
        "crypto to the moon",
    ]


def test_save_post_round_trip(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    first = create_post(conn, bob, "worth keeping", name="bob", now=1000)
    second = create_post(conn, alice, "mine too", name="alice", now=1001)

    record = save_post(conn, first["hash"][:16], now=2000)
    assert record["hash"] == first["hash"]
    assert record["already_saved"] is False
    assert record["saved_at"] == 2000

    # Saving again is a no-op that reports the original save time.
    again = save_post(conn, first["hash"], now=3000)
    assert again["already_saved"] is True
    assert again["saved_at"] == 2000

    save_post(conn, second["hash"], now=2001)
    saved = saved_posts(conn)
    assert [post["hash"] for post in saved] == [second["hash"], first["hash"]]
    assert saved[0]["saved_at"] == 2001

    removed = unsave_post(conn, first["hash"][:16])
    assert removed == {"hash": first["hash"], "unsaved": True}
    assert unsave_post(conn, first["hash"]) == {
        "hash": first["hash"],
        "unsaved": False,
    }
    assert [post["hash"] for post in saved_posts(conn)] == [second["hash"]]


def test_save_post_rejects_unknown_unlinked_and_deleted(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    first = create_post(source, alice, "first", name="alice", now=1000)
    second = create_post(source, alice, "second", name="alice", now=1001)

    with pytest.raises(TanError, match="unknown post"):
        save_post(source, "ab" * 32)

    # Only the second post arrives: it is cached but unlinked, so
    # saving it would file a post with no verified chain behind it.
    destination = init_db(tmp_path / "destination.sqlite3")
    store_post(destination, second)
    with pytest.raises(TanError, match="has not linked yet"):
        save_post(destination, second["hash"])

    # A post its author deleted cannot be saved.
    create_delete(source, alice, first["hash"], name="alice", now=1002)
    with pytest.raises(TanError, match="has been deleted"):
        save_post(source, first["hash"])
    # ... but a post deleted after being saved stays listed, blanked.
    keep = create_post(source, bob, "keep me", name="bob", now=1003)
    save_post(source, keep["hash"], now=2000)
    create_delete(source, bob, keep["hash"], name="bob", now=1004)
    saved = saved_posts(source)
    assert len(saved) == 1
    assert saved[0]["hash"] == keep["hash"]
    assert saved[0]["deleted"] is True
    assert saved[0]["text"] == ""


def test_hide_post_round_trip(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    first = create_post(conn, bob, "worth hiding", name="bob", now=1000)
    second = create_post(conn, alice, "mine too", name="alice", now=1001)

    record = hide_post(conn, first["hash"][:16], now=2000)
    assert record["hash"] == first["hash"]
    assert record["already_hidden"] is False
    assert record["hidden_at"] == 2000

    # Hiding again is a no-op that reports the original hide time.
    again = hide_post(conn, first["hash"], now=3000)
    assert again["already_hidden"] is True
    assert again["hidden_at"] == 2000

    hide_post(conn, second["hash"], now=2001)
    hidden = hidden_posts(conn)
    assert [post["hash"] for post in hidden] == [second["hash"], first["hash"]]
    assert hidden[0]["hidden_at"] == 2001

    removed = unhide_post(conn, first["hash"][:16])
    assert removed == {"hash": first["hash"], "unhidden": True}
    assert unhide_post(conn, first["hash"]) == {
        "hash": first["hash"],
        "unhidden": False,
    }
    assert [post["hash"] for post in hidden_posts(conn)] == [second["hash"]]


def test_hide_post_rejects_unknown_unlinked_and_deleted(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    first = create_post(source, alice, "first", name="alice", now=1000)
    second = create_post(source, alice, "second", name="alice", now=1001)

    with pytest.raises(TanError, match="unknown post"):
        hide_post(source, "ab" * 32)

    # Only the second post arrives: it is cached but unlinked, so
    # hiding it would file a post with no verified chain behind it.
    destination = init_db(tmp_path / "destination.sqlite3")
    store_post(destination, second)
    with pytest.raises(TanError, match="has not linked yet"):
        hide_post(destination, second["hash"])

    # A post its author deleted cannot be hidden.
    create_delete(source, alice, first["hash"], name="alice", now=1002)
    with pytest.raises(TanError, match="has been deleted"):
        hide_post(source, first["hash"])
    # ... but a post deleted after being hidden stays listed, blanked.
    keep = create_post(source, bob, "keep me", name="bob", now=1003)
    hide_post(source, keep["hash"], now=2000)
    create_delete(source, bob, keep["hash"], name="bob", now=1004)
    hidden = hidden_posts(source)
    assert len(hidden) == 1
    assert hidden[0]["hash"] == keep["hash"]
    assert hidden[0]["deleted"] is True
    assert hidden[0]["text"] == ""
    # ... and unhiding still works after the delete.
    assert unhide_post(source, keep["hash"]) == {
        "hash": keep["hash"],
        "unhidden": True,
    }


def test_hidden_posts_shows_edited_text(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, bob, "original", name="bob", now=1000)
    hide_post(conn, post["hash"], now=2000)

    # The hidden shelf shows the current text, not the text as hidden.
    create_edit(conn, bob, post["hash"], "fixed", name="bob", now=1001)
    hidden = hidden_posts(conn)
    assert len(hidden) == 1
    assert hidden[0]["text"] == "fixed"
    assert hidden[0]["edited"] is True


def test_timeline_hides_hidden_posts(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    bob_post = create_post(conn, bob, "from bob", name="bob", now=1000)
    alice_post = create_post(conn, alice, "from alice", name="alice", now=1001)
    authors = [bob_post["author"], alice_post["author"]]

    hide_post(conn, bob_post["hash"])
    assert [post["text"] for post in timeline(conn, authors)] == ["from alice"]
    assert [post["text"] for post in timeline(conn, include_all=True)] == [
        "from alice"
    ]
    assert timeline(conn, [bob_post["author"]]) == []
    assert author_timeline(conn, bob_post["author"]) == []
    # An explicit feed lookup still shows it: hiding is a list-view rule.
    assert [post["text"] for post in feed(conn, author=bob_post["author"])] == [
        "from bob"
    ]

    # Unhiding restores the full timeline.
    unhide_post(conn, bob_post["hash"])
    assert [post["text"] for post in timeline(conn, authors)] == [
        "from alice",
        "from bob",
    ]


def test_timeline_pages_past_hidden_posts(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "older clean post", name="alice", now=1000)
    hidden = [
        create_post(conn, alice, f"hidden {index}", name="alice", now=2000 + index)
        for index in range(25)
    ]
    for post in hidden:
        hide_post(conn, post["hash"])

    # More hidden posts than one page must not hide the older clean post.
    items = timeline(conn, include_all=True, limit=5)
    assert [item["text"] for item in items] == ["older clean post"]


def test_author_timeline_pages_past_hidden_posts(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    for index in range(25):
        post = create_post(
            conn, alice, f"hidden {index}", name="alice", now=1000 + index
        )
        hide_post(conn, post["hash"])
    create_post(conn, alice, "clean ending", name="alice", now=2000)

    items = author_timeline(conn, author_of(alice), limit=5)
    assert [item["text"] for item in items] == ["clean ending"]


def test_timeline_drops_repost_of_hidden_post(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, bob, "hidden words", name="bob", now=1000)
    create_repost(conn, alice, post["hash"], name="alice", now=1001)
    other = create_post(conn, alice, "alice's own post", name="alice", now=1002)

    hide_post(conn, post["hash"])
    items = timeline(conn, [author_of(alice)])
    assert [item["hash"] for item in items] == [other["hash"]]
    assert "hidden words" not in json.dumps(items)
    # Unlike a repost of a muted author's post, a repost of a hidden
    # post stays hidden in the explicit author view too: hiding is
    # about the post's words, not the author.
    items = author_timeline(conn, author_of(alice))
    assert [item["hash"] for item in items] == [other["hash"]]
    assert "hidden words" not in json.dumps(items)


def test_search_hides_hidden_posts(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "hello mesh world", name="alice", now=1000)
    hidden = create_post(conn, bob, "HELLO from bob", name="bob", now=1001)

    hide_post(conn, hidden["hash"])
    assert [post["text"] for post in search_posts(conn, "hello")] == [
        "hello mesh world"
    ]
    # ... even an explicit author search stays hidden: hiding is
    # per-post, unlike muting an author.
    assert search_posts(conn, "hello", author=author_of(bob)) == []

    unhide_post(conn, hidden["hash"])
    assert [post["text"] for post in search_posts(conn, "hello")] == [
        "HELLO from bob",
        "hello mesh world",
    ]


def test_top_posts_hides_hidden_posts(identities, tmp_path):
    alice, bob = identities
    carol = RNS.Identity()
    conn = init_db(tmp_path / "tan.sqlite3")
    popular = create_post(conn, bob, "popular", name="bob", now=1000)
    other = create_post(conn, alice, "other", name="alice", now=1001)
    third = create_post(conn, alice, "third", name="alice", now=1002)
    create_reaction(conn, alice, popular["hash"], liked=True, name="alice", now=1003)
    create_reaction(conn, carol, popular["hash"], liked=True, name="carol", now=1004)

    assert [post["text"] for post in top_posts(conn, limit=2)] == [
        "popular",
        "third",
    ]
    hide_post(conn, popular["hash"])
    # The hidden post drops out and never takes up a result slot.
    assert [post["text"] for post in top_posts(conn, limit=2)] == [
        "third",
        "other",
    ]
    unhide_post(conn, popular["hash"])
    assert [post["text"] for post in top_posts(conn, limit=2)] == [
        "popular",
        "third",
    ]


def test_thread_hides_hidden_reply_subtree(identities, tmp_path):
    alice, bob = identities
    carol = RNS.Identity()
    conn = init_db(tmp_path / "tan.sqlite3")
    root = create_post(conn, alice, "root post", name="alice", now=1000)
    reply = create_reply(conn, bob, root["hash"], "bob replies", name="bob", now=1001)
    nested = create_reply(
        conn, carol, reply["hash"], "carol replies to bob", name="carol", now=1002
    )
    create_reply(
        conn, carol, root["hash"], "carol replies to root", name="carol", now=1003
    )

    hide_post(conn, reply["hash"])
    entries = thread(conn, root["hash"])
    assert [entry["text"] for entry in entries] == [
        "root post",
        "carol replies to root",
    ]
    # The named entry always anchors the thread, even when it is hidden.
    assert [entry["text"] for entry in thread(conn, reply["hash"])] == [
        "bob replies",
        "carol replies to bob",
    ]
    # ...while a hidden ancestor above the named entry simply ends the
    # climb instead of being promoted to the root.
    assert [entry["text"] for entry in thread(conn, nested["hash"])] == [
        "carol replies to bob"
    ]

    unhide_post(conn, reply["hash"])
    assert [entry["text"] for entry in thread(conn, root["hash"])] == [
        "root post",
        "bob replies",
        "carol replies to bob",
        "carol replies to root",
    ]


def test_timeline_blanks_hidden_reply_parent_preview(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    parent = create_post(conn, bob, "secret words", name="bob", now=1000)
    reply = create_reply(conn, alice, parent["hash"], "noted", name="alice", now=1001)
    hide_post(conn, parent["hash"])

    items = timeline(conn, [author_of(alice)])
    assert [item["hash"] for item in items] == [reply["hash"]]
    preview = items[0]["reply_to"]
    assert preview["hash"] == parent["hash"]
    assert preview["text"] == ""
    assert preview["hidden"] is True
    assert preview["deleted"] is False


def test_timeline_blanks_hidden_quote_preview(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    parent = create_post(conn, bob, "secret words", name="bob", now=1000)
    quote = create_quote(conn, alice, parent["hash"], "quoting bob", name="alice", now=1001)
    hide_post(conn, parent["hash"])

    items = timeline(conn, [author_of(alice)])
    assert [item["hash"] for item in items] == [quote["hash"]]
    preview = items[0]["quoted"]
    assert preview["hash"] == parent["hash"]
    assert preview["text"] == ""
    assert preview["hidden"] is True
    assert preview["deleted"] is False


def test_mentions_hides_hidden_replies(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    mine = create_post(conn, alice, "my post", name="alice", now=1000)
    reply = create_reply(conn, bob, mine["hash"], "bob replies", name="bob", now=1001)
    assert [item["text"] for item in mentions(conn, author_of(alice))] == [
        "bob replies"
    ]

    hide_post(conn, reply["hash"])
    assert mentions(conn, author_of(alice)) == []

    unhide_post(conn, reply["hash"])
    assert [item["text"] for item in mentions(conn, author_of(alice))] == [
        "bob replies"
    ]


def test_annotated_saved_posts_drops_hidden(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, bob, "keep me", name="bob", now=1000)
    save_post(conn, post["hash"], now=2000)

    hide_post(conn, post["hash"], now=2001)
    posts, _ = annotated_saved_posts(conn)
    assert posts == []

    unhide_post(conn, post["hash"])
    posts, _ = annotated_saved_posts(conn)
    assert len(posts) == 1
    assert posts[0]["hash"] == post["hash"]

    # A saved repost of a hidden post drops out too: the repost
    # carries no text of its own, so hiding the original hides it.
    original = create_post(conn, bob, "original", name="bob", now=1002)
    repost = create_repost(conn, alice, original["hash"], name="alice", now=1003)
    save_post(conn, repost["hash"], now=2003)
    hide_post(conn, original["hash"], now=2004)
    posts, _ = annotated_saved_posts(conn)
    assert [item["hash"] for item in posts] == [post["hash"]]


def test_annotated_pinned_posts_drops_hidden(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    mine = create_post(conn, alice, "pin this", name="alice", now=1000)
    other = create_post(conn, alice, "keep this", name="alice", now=1001)
    owner = author_of(alice)
    pin_post(conn, mine["hash"], owner=owner, now=2000)
    pin_post(conn, other["hash"], owner=owner, now=2001)

    hide_post(conn, mine["hash"], now=2002)
    posts, _ = annotated_pinned_posts(conn)
    assert [post["hash"] for post in posts] == [other["hash"]]

    unhide_post(conn, mine["hash"])
    posts, _ = annotated_pinned_posts(conn)
    assert [post["hash"] for post in posts] == [other["hash"], mine["hash"]]


def test_tags_ignores_hidden_posts(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    first = create_post(conn, alice, "#mesh hello", name="alice", now=1000)
    create_post(conn, alice, "another #mesh post", name="alice", now=1001)
    assert tags(conn) == [{"tag": "mesh", "count": 2}]

    hide_post(conn, first["hash"])
    assert tags(conn) == [{"tag": "mesh", "count": 1}]

    unhide_post(conn, first["hash"])
    assert tags(conn) == [{"tag": "mesh", "count": 2}]


def test_annotated_saved_posts_applies_edits(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, bob, "original", name="bob", now=1000)
    save_post(conn, post["hash"], now=2000)

    # The saved list shows the current edit, not the original text.
    create_edit(conn, bob, post["hash"], "fixed", name="bob", now=1001)
    posts, _ = annotated_saved_posts(conn)
    assert len(posts) == 1
    assert posts[0]["text"] == "fixed"
    assert posts[0]["edited"] is True

    # ... and an edit that adds a muted keyword drops the post from
    # the view, because the filter now sees the edited text.
    mute_keyword(conn, "crypto")
    create_edit(
        conn, bob, post["hash"], "crypto to the moon", name="bob", now=1002
    )
    posts, _ = annotated_saved_posts(conn)
    assert posts == []

    # ... and an edit that landed before a delete stays blanked.
    unmute_keyword(conn, "crypto")
    create_delete(conn, bob, post["hash"], name="bob", now=1003)
    posts, _ = annotated_saved_posts(conn)
    assert len(posts) == 1
    assert posts[0]["text"] == ""
    assert posts[0]["deleted"] is True
    assert posts[0]["edited"] is False


def test_pin_post_round_trip(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    mine = create_post(conn, alice, "pin this", name="alice", now=1000)
    theirs = create_post(conn, bob, "not mine", name="bob", now=1001)
    owner = author_of(alice)

    # Only your own posts can be pinned.
    with pytest.raises(TanError, match="only your own posts"):
        pin_post(conn, theirs["hash"], owner=owner)

    record = pin_post(conn, mine["hash"][:16], owner=owner, now=2000)
    assert record["hash"] == mine["hash"]
    assert record["already_pinned"] is False
    assert record["pinned_at"] == 2000

    # Pinning again is a no-op that reports the original pin time.
    again = pin_post(conn, mine["hash"], owner=owner, now=3000)
    assert again["already_pinned"] is True
    assert again["pinned_at"] == 2000

    pinned = pinned_posts(conn)
    assert [post["hash"] for post in pinned] == [mine["hash"]]
    assert pinned[0]["pinned_at"] == 2000

    # The annotation flags the pinned post and leaves others clear.
    flagged = annotate_pinned(conn, [dict(mine), dict(theirs)])
    assert flagged[0]["pinned"] is True
    assert flagged[1]["pinned"] is False

    removed = unpin_post(conn, mine["hash"][:16])
    assert removed == {"hash": mine["hash"], "unpinned": True}
    assert unpin_post(conn, mine["hash"]) == {"hash": mine["hash"], "unpinned": False}
    assert pinned_posts(conn) == []


def test_pin_post_rejects_unknown_unlinked_and_deleted(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    first = create_post(source, alice, "first", name="alice", now=1000)
    second = create_post(source, alice, "second", name="alice", now=1001)
    owner = author_of(alice)

    with pytest.raises(TanError, match="unknown post"):
        pin_post(source, "ab" * 32, owner=owner)

    # Only the second post arrives: it is cached but unlinked, so
    # pinning it would file a post with no verified chain behind it.
    destination = init_db(tmp_path / "destination.sqlite3")
    store_post(destination, second)
    with pytest.raises(TanError, match="has not linked yet"):
        pin_post(destination, second["hash"], owner=owner)

    # A post its author deleted cannot be pinned.
    create_delete(source, alice, first["hash"], name="alice", now=1002)
    with pytest.raises(TanError, match="has been deleted"):
        pin_post(source, first["hash"], owner=owner)

    # ... but a post deleted after being pinned stays listed, blanked,
    # and can still be unpinned.
    keep = create_post(source, bob, "keep me", name="bob", now=1003)
    pin_post(source, keep["hash"], owner=author_of(bob), now=2000)
    create_delete(source, bob, keep["hash"], name="bob", now=1004)
    pinned = pinned_posts(source)
    assert len(pinned) == 1
    assert pinned[0]["hash"] == keep["hash"]
    assert pinned[0]["deleted"] is True
    assert pinned[0]["text"] == ""
    assert unpin_post(source, keep["hash"]) == {
        "hash": keep["hash"],
        "unpinned": True,
    }


def test_pin_post_cap(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    owner = author_of(alice)
    posts = [
        create_post(conn, alice, f"post {n}", name="alice", now=1000 + n)
        for n in range(11)
    ]
    for post in posts[:10]:
        pin_post(conn, post["hash"], owner=owner)
    with pytest.raises(TanError, match="limited to 10"):
        pin_post(conn, posts[10]["hash"], owner=owner)
    # Re-pinning a post already on the shelf still succeeds at the cap.
    assert pin_post(conn, posts[0]["hash"], owner=owner)["already_pinned"] is True


def test_annotated_pinned_posts_newest_first(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    owner = author_of(alice)
    older = create_post(conn, alice, "older pin", name="alice", now=1000)
    newer = create_post(conn, alice, "newer pin", name="alice", now=1001)
    pin_post(conn, older["hash"], owner=owner, now=2000)
    pin_post(conn, newer["hash"], owner=owner, now=2001)

    posts, next_offset = annotated_pinned_posts(conn, viewer=owner)
    assert [post["hash"] for post in posts] == [newer["hash"], older["hash"]]
    assert all(post["pinned"] for post in posts)
    assert next_offset is None


def test_annotated_pinned_posts_applies_edits(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    owner = author_of(alice)
    post = create_post(conn, alice, "original", name="alice", now=1000)
    pin_post(conn, post["hash"], owner=owner, now=2000)

    # The pinned list shows the current edit, not the original text.
    create_edit(conn, alice, post["hash"], "fixed", name="alice", now=1001)
    posts, _ = annotated_pinned_posts(conn, viewer=owner)
    assert len(posts) == 1
    assert posts[0]["text"] == "fixed"
    assert posts[0]["edited"] is True

    # ... and an edit that landed before a delete stays blanked.
    create_delete(conn, alice, post["hash"], name="alice", now=1002)
    posts, _ = annotated_pinned_posts(conn, viewer=owner)
    assert len(posts) == 1
    assert posts[0]["text"] == ""
    assert posts[0]["deleted"] is True
    assert posts[0]["edited"] is False


def test_cli_tan_pin(tmp_path, capsys):
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
                "pin me",
            ]
        )
        == 0
    )
    capsys.readouterr()
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        post = feed(conn, all_authors=True)[0]
    finally:
        conn.close()

    assert main(["tan", "pin", "--data-dir", str(data_dir), post["hash"][:16]]) == 0
    assert f"pinned {post['hash'][:8]}" in capsys.readouterr().out

    # Pinning again reports the original pin time.
    assert main(["tan", "pin", "--data-dir", str(data_dir), post["hash"][:16]]) == 0
    assert "already pinned" in capsys.readouterr().out

    # JSON output lists the pinned post with its pin time.
    assert main(["tan", "pinned", "--data-dir", str(data_dir), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["pinned"][0]["text"] == "pin me"
    assert payload["pinned"][0]["pinned_at"] > 0

    # Text output shows the post and when it was pinned, and the
    # timeline marks the pinned post.
    assert main(["tan", "pinned", "--data-dir", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "pin me" in out
    assert "📌" in out
    assert main(["tan", "timeline", "--data-dir", str(data_dir)]) == 0
    assert "📌" in capsys.readouterr().out

    # Unpinning by prefix reports the full hash in JSON output, so
    # machine-readable callers can identify the affected post.
    assert (
        main(
            [
                "tan",
                "unpin",
                "--data-dir",
                str(data_dir),
                "--json",
                post["hash"][:16],
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"hash": post["hash"], "unpinned": True}

    # Unpinning twice: the second time reports it was not pinned.
    assert main(["tan", "unpin", "--data-dir", str(data_dir), post["hash"][:16]]) == 0
    assert "was not pinned" in capsys.readouterr().out
    assert main(["tan", "pinned", "--data-dir", str(data_dir), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["count"] == 0

    # An unknown reference is a clean CLI error, not a traceback.
    assert main(["tan", "pin", "--data-dir", str(data_dir), "cd" * 32]) != 0
    assert "tau: error: unknown post" in capsys.readouterr().err


def test_cli_tan_save(tmp_path, capsys):
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
                "save this for later",
            ]
        )
        == 0
    )
    capsys.readouterr()
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        post = feed(conn, all_authors=True)[0]
    finally:
        conn.close()

    assert main(["tan", "save", "--data-dir", str(data_dir), post["hash"][:16]]) == 0
    assert f"saved {post['hash'][:8]}" in capsys.readouterr().out

    # JSON output lists the saved post with its save time.
    assert main(["tan", "saved", "--data-dir", str(data_dir), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["saved"][0]["text"] == "save this for later"
    assert payload["saved"][0]["saved_at"] > 0

    # Text output shows the post and when it was saved.
    assert main(["tan", "saved", "--data-dir", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "save this for later" in out
    assert "saved " in out

    # Unsaving by prefix reports the full hash in JSON output, so
    # machine-readable callers can identify the affected post.
    assert (
        main(
            [
                "tan",
                "unsave",
                "--data-dir",
                str(data_dir),
                "--json",
                post["hash"][:16],
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"hash": post["hash"], "unsaved": True}

    # Unsaving twice: the second time reports it was not saved.
    assert main(["tan", "unsave", "--data-dir", str(data_dir), post["hash"][:16]]) == 0
    assert "was not saved" in capsys.readouterr().out
    assert main(["tan", "saved", "--data-dir", str(data_dir), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["count"] == 0

    # An unknown reference is a clean CLI error, not a traceback.
    assert main(["tan", "save", "--data-dir", str(data_dir), "cd" * 32]) != 0
    assert "tau: error: unknown post" in capsys.readouterr().err


def test_cli_tan_saved_deleted_post(tmp_path, capsys):
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
                "soon to be deleted",
            ]
        )
        == 0
    )
    capsys.readouterr()
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        post = feed(conn, all_authors=True)[0]
    finally:
        conn.close()

    assert main(["tan", "save", "--data-dir", str(data_dir), post["hash"]]) == 0
    capsys.readouterr()
    assert (
        main(
            [
                "tan",
                "delete",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                post["hash"],
            ]
        )
        == 0
    )
    capsys.readouterr()

    # Text output keeps the save but marks the post as deleted.
    assert main(["tan", "saved", "--data-dir", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "(deleted post)" in out
    assert "soon to be deleted" not in out

    # JSON output lists it as deleted with blank text.
    assert main(["tan", "saved", "--data-dir", str(data_dir), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["saved"][0]["deleted"] is True
    assert payload["saved"][0]["text"] == ""

    # Unsaving the deleted post succeeds and empties the list.
    assert main(["tan", "unsave", "--data-dir", str(data_dir), post["hash"][:16]]) == 0
    assert "unsaved" in capsys.readouterr().out
    assert main(["tan", "saved", "--data-dir", str(data_dir), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["count"] == 0


def test_top_posts_ranks_by_likes_then_recency(identities, tmp_path):
    alice, bob = identities
    carol = RNS.Identity()
    conn = init_db(tmp_path / "tan.sqlite3")
    old_hit = create_post(conn, alice, "old hit", name="alice", now=1000)
    mid_hit = create_post(conn, bob, "mid hit", name="bob", now=1001)
    newer_hit = create_post(conn, alice, "newer hit", name="alice", now=1002)
    fresh_flop = create_post(conn, alice, "fresh flop", name="alice", now=1003)
    create_reaction(conn, bob, old_hit["hash"], liked=True, name="bob", now=1004)
    create_reaction(conn, carol, old_hit["hash"], liked=True, name="carol", now=1005)
    create_reaction(conn, bob, newer_hit["hash"], liked=True, name="bob", now=1006)
    create_reaction(conn, carol, mid_hit["hash"], liked=True, name="carol", now=1007)

    ranked = top_posts(conn)
    assert [post["text"] for post in ranked] == [
        "old hit",
        "newer hit",
        "mid hit",
        "fresh flop",
    ]
    assert [post["likes"] for post in ranked] == [2, 1, 1, 0]

    # A reactor's latest reaction wins: bob unlikes the old hit and it
    # drops to the one-like tier, newest first among the ties.
    create_reaction(conn, bob, old_hit["hash"], liked=False, name="bob", now=1008)
    assert [post["text"] for post in top_posts(conn)] == [
        "newer hit",
        "mid hit",
        "old hit",
        "fresh flop",
    ]

    # Limit and author filter compose with the ranking.
    assert [post["text"] for post in top_posts(conn, limit=2)] == [
        "newer hit",
        "mid hit",
    ]
    assert [post["text"] for post in top_posts(conn, author=author_of(alice))] == [
        "newer hit",
        "old hit",
        "fresh flop",
    ]

    # Viewer annotations ride along: alice owns her posts and has not
    # liked anything, so liked is False and own tracks authorship.
    ranked = top_posts(conn, viewer=author_of(alice))
    assert [(post["text"], post["liked"], post["own"]) for post in ranked] == [
        ("newer hit", False, True),
        ("mid hit", False, False),
        ("old hit", False, True),
        ("fresh flop", False, True),
    ]
    with pytest.raises(TanError, match="author must be"):
        top_posts(conn, author="not-a-hash")


def test_top_posts_batches_reaction_lookup(identities, tmp_path, monkeypatch):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    posts = [
        create_post(conn, alice, f"post {i}", name="alice", now=1000 + i)
        for i in range(5)
    ]
    create_reaction(conn, bob, posts[0]["hash"], liked=True, name="bob", now=2000)
    monkeypatch.setattr("tau.tangents.tan._SQL_BATCH", 2)
    ranked = top_posts(conn, limit=1)
    assert [post["text"] for post in ranked] == ["post 0"]
    assert ranked[0]["likes"] == 1


def test_top_posts_hidden_kinds_do_not_consume_limit(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "visible", name="alice", now=1000)
    create_post(conn, bob, "ignored", name="bob", now=1001)
    # Hiding the "post" kind drops every candidate before the limit is
    # applied, so nothing is returned; an unrestricted ruleset keeps both.
    assert top_posts(conn, limit=1, ruleset=Ruleset(kinds=frozenset({"like"}))) == []
    assert [post["text"] for post in top_posts(conn, limit=1, ruleset=Ruleset())] == [
        "ignored"
    ]


def test_top_posts_hidden_reactions_rank_by_recency(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    older = create_post(conn, alice, "older", name="alice", now=1000)
    create_post(conn, alice, "newer", name="alice", now=1001)
    create_reaction(conn, bob, older["hash"], liked=True, name="bob", now=1002)
    ruleset = Ruleset(kinds=frozenset({"post"}))
    ranked = top_posts(conn, limit=2, ruleset=ruleset)
    assert [post["text"] for post in ranked] == ["newer", "older"]
    # With reactions displayed, the liked post leads again.
    shown = top_posts(conn, limit=2, ruleset=Ruleset())
    assert [post["text"] for post in shown] == ["older", "newer"]


def test_top_posts_hides_deleted_muted_and_unlinked(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    liked = create_post(conn, alice, "everyone loves this", name="alice", now=1000)
    doomed = create_post(conn, alice, "liked then deleted", name="alice", now=1001)
    noisy = create_post(conn, alice, "CRYPTO to the moon", name="alice", now=1002)
    reply = create_reply(conn, bob, noisy["hash"], "to the moon", name="bob", now=1003)
    create_reaction(conn, bob, liked["hash"], liked=True, name="bob", now=1004)
    create_reaction(conn, bob, doomed["hash"], liked=True, name="bob", now=1005)
    create_reaction(conn, alice, noisy["hash"], liked=True, name="alice", now=1006)
    create_reaction(conn, alice, reply["hash"], liked=True, name="alice", now=1007)
    create_delete(conn, alice, doomed["hash"], name="alice", now=1008)
    mute_keyword(conn, "crypto")

    ranked = top_posts(conn)
    # The deleted post never ranks, and neither does the muted post —
    # but the liked reply to the muted post still ranks, with its
    # parent preview blanked like the timeline blanks it.
    assert [post["text"] for post in ranked] == [
        "to the moon",
        "everyone loves this",
    ]
    assert ranked[0]["reply_to"]["muted"] is True
    assert ranked[0]["reply_to"]["text"] == ""

    # An unlinked post never ranks even with likes on it.
    orphan = create_post(conn, bob, "unlinked hit", name="bob", now=1009)
    create_reaction(conn, alice, orphan["hash"], liked=True, name="alice", now=1010)
    conn.execute("UPDATE posts SET linked = 0 WHERE hash = ?", (orphan["hash"],))
    conn.commit()
    assert [post["text"] for post in top_posts(conn)] == [
        "to the moon",
        "everyone loves this",
    ]




def test_cli_top(tmp_path, capsys):
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
                "first post",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert (
        main(
            [
                "tan",
                "post",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                "second post",
            ]
        )
        == 0
    )
    capsys.readouterr()

    # With no likes anywhere the ranking falls back to recency.
    assert main(["tan", "top", "--data-dir", str(data_dir), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert [post["text"] for post in payload["posts"]] == [
        "second post",
        "first post",
    ]

    # Liking the older post promotes it to the top, and --limit trims.
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        older = feed(conn, all_authors=True)[-1]
    finally:
        conn.close()
    assert (
        main(
            [
                "tan",
                "like",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                older["hash"][:16],
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert (
        main(["tan", "top", "--data-dir", str(data_dir), "--json", "--limit", "1"])
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["posts"][0]["text"] == "first post"
    assert payload["posts"][0]["likes"] == 1


def test_top_posts_batches_reaction_lookups(identities, tmp_path):
    # Codex P2 on PR #149: annotating every candidate in one
    # `reaction_states` call passes all hashes to a single IN (...)
    # query, which raises "too many SQL variables" past 999 posts on
    # SQLite builds with the 999-variable limit. The ranking must
    # batch the lookup like thread() does; this pins the ranking at
    # real scale, past the limit and past several batches.
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    for index in range(1005):
        create_post(conn, alice, f"post {index}", name="alice", now=1000 + index)
    winner = create_post(conn, bob, "the winner", name="bob", now=3000)
    create_reaction(conn, alice, winner["hash"], liked=True, name="alice", now=3001)

    ranked = top_posts(conn, limit=1)
    assert [post["text"] for post in ranked] == ["the winner"]
    assert ranked[0]["likes"] == 1


def test_top_posts_hides_muted_authors(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    loved = create_post(conn, alice, "everyone loves this", name="alice", now=1000)
    noisy = create_post(conn, bob, "loud hit", name="bob", now=1001)
    reply = create_reply(conn, alice, noisy["hash"], "agreed", name="alice", now=1002)
    create_reaction(conn, alice, noisy["hash"], liked=True, name="alice", now=1003)
    create_reaction(conn, bob, loved["hash"], liked=True, name="bob", now=1004)
    create_reaction(conn, bob, reply["hash"], liked=True, name="bob", now=1005)
    mute(conn, author_of(bob), name="bob")

    ranked = top_posts(conn)
    # Bob's own liked post never ranks, but alice's reply to it still
    # ranks — with no parent preview, so bob's name and text never
    # surface through it.
    assert [post["text"] for post in ranked] == ["agreed", "everyone loves this"]
    assert ranked[0]["reply_to"] is None

    # An explicit author lookup still ranks a muted author's posts,
    # like timeline --author does.
    assert [post["text"] for post in top_posts(conn, author=author_of(bob))] == [
        "loud hit"
    ]

    # But even in an explicit lookup, a reply to a muted author keeps
    # no parent preview — the mute never leaks through a preview.
    by_alice = top_posts(conn, author=author_of(alice))
    assert [post["text"] for post in by_alice] == ["agreed", "everyone loves this"]
    assert by_alice[0]["reply_to"] is None


def test_top_posts_survives_a_large_block_list(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    loved = create_post(conn, alice, "everyone loves this", name="alice", now=1000)
    create_reaction(conn, bob, loved["hash"], liked=True, name="bob", now=1001)
    # A block-list far above SQLite's bound-parameter limit (999 on most
    # builds) must not break the query: the exclusion is a single JSON
    # parameter, not one placeholder per author.
    blocked = [f"{i:032x}" for i in range(1500)]
    assert [post["text"] for post in top_posts(conn, exclude_authors=blocked)] == [
        "everyone loves this"
    ]
    # A blocked author inside that list still never ranks.
    blocked.append(author_of(alice))
    assert top_posts(conn, exclude_authors=blocked) == []



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


def test_extract_tags():
    assert extract_tags("hello #meshnet world") == ["meshnet"]
    assert extract_tags("#MeshNet and #meshnet again") == ["meshnet"]
    assert extract_tags("#ato-2026 #café #tag_1") == ["ato-2026", "café", "tag_1"]
    assert extract_tags("trailing #tag- punctuation #hi! and (#yo)") == [
        "tag",
        "hi",
        "yo",
    ]
    # A '#' mid-word is not a tag: C# the language, or foo#bar.
    assert extract_tags("C# is not a tag, nor is foo#bar") == []
    # A tag must start with a letter.
    assert extract_tags("#123 #_x #") == []
    assert extract_tags("") == []
    assert extract_tags(None) == []
    assert extract_tags("#a #a-b #a") == ["a", "a-b"]


def test_clean_tag():
    assert clean_tag("#MeshNet") == "meshnet"
    assert clean_tag("  meshnet  ") == "meshnet"
    assert clean_tag("#ato-2026") == "ato-2026"
    for bad in ("", "   ", "#", "123", "two words", "#a-", "a#b", None, 42):
        with pytest.raises(TanError, match="not a tag|tag must be text"):
            clean_tag(bad)


def test_tags_lists_counts_most_used_first(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "#meshnet hello", name="alice", now=1000)
    create_post(conn, alice, "#meshnet again #meshnet", name="alice", now=1001)
    create_post(conn, bob, "#ato time", name="bob", now=1002)
    create_post(conn, alice, "#ato #meshnet both", name="alice", now=1003)
    create_post(conn, bob, "no tags here", name="bob", now=1004)

    assert tags(conn) == [
        {"tag": "meshnet", "count": 3},
        {"tag": "ato", "count": 2},
    ]
    assert tags(conn, limit=1) == [{"tag": "meshnet", "count": 3}]
    # A post counts once per tag no matter how often it repeats it.
    assert tags(conn, limit=2)[0]["count"] == 3


def test_tags_uses_edited_text_and_excludes_deleted_and_muted(
    identities, tmp_path
):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    edited = create_post(conn, alice, "#oldtag hello", name="alice", now=1000)
    create_edit(conn, alice, edited["hash"], "#newtag hello", name="alice", now=1001)
    deleted = create_post(conn, alice, "#gone hello", name="alice", now=1002)
    create_delete(conn, alice, deleted["hash"], name="alice", now=1003)
    create_post(conn, bob, "#muted hello", name="bob", now=1004)

    assert tags(conn) == [
        {"tag": "muted", "count": 1},
        {"tag": "newtag", "count": 1},
    ]
    mute(conn, author_of(bob), name="bob")
    assert tags(conn) == [{"tag": "newtag", "count": 1}]
    unmute(conn, author_of(bob))
    assert tags(conn) == [
        {"tag": "muted", "count": 1},
        {"tag": "newtag", "count": 1},
    ]


def test_timeline_tag_filters_posts_replies_and_reposts(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    tagged = create_post(conn, alice, "tagged #meshnet", name="alice", now=1000)
    plain = create_post(conn, bob, "plain post", name="bob", now=1001)
    reply = create_reply(
        conn, alice, plain["hash"], "reply #meshnet", name="alice", now=1002
    )
    repost_tagged = create_repost(conn, bob, tagged["hash"], name="bob", now=1003)
    create_repost(conn, alice, plain["hash"], name="alice", now=1004)
    authors = [tagged["author"], plain["author"]]

    found = timeline(conn, authors, tag="meshnet")
    assert [post["hash"] for post in found] == [
        repost_tagged["hash"],
        reply["hash"],
        tagged["hash"],
    ]
    # A leading '#' and upper case are accepted: the tag is cleaned.
    assert [post["hash"] for post in timeline(conn, authors, tag="#MESHNET")] == [
        repost_tagged["hash"],
        reply["hash"],
        tagged["hash"],
    ]
    # The author view filters the same way.
    assert [post["hash"] for post in author_timeline(conn, tagged["author"], tag="meshnet")] == [
        reply["hash"],
        tagged["hash"],
    ]
    # So does the --all view.
    assert [post["hash"] for post in timeline(conn, include_all=True, tag="meshnet")] == [
        repost_tagged["hash"],
        reply["hash"],
        tagged["hash"],
    ]
    assert timeline(conn, authors, tag="nothing") == []
    with pytest.raises(TanError, match="not a tag"):
        timeline(conn, authors, tag="two words")


def test_timeline_tag_uses_edited_text(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    added = create_post(conn, alice, "hello", name="alice", now=1000)
    create_edit(conn, alice, added["hash"], "hello #meshnet", name="alice", now=1001)
    removed = create_post(conn, bob, "#meshnet hello", name="bob", now=1002)
    create_edit(conn, bob, removed["hash"], "hello", name="bob", now=1003)
    authors = [added["author"], removed["author"]]

    assert [post["hash"] for post in timeline(conn, authors, tag="meshnet")] == [
        added["hash"]
    ]
    assert [post["hash"] for post in timeline(conn, include_all=True, tag="meshnet")] == [
        added["hash"]
    ]


def test_timeline_tag_hides_muted_authors(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    create_post(conn, alice, "#meshnet from alice", name="alice", now=1000)
    create_post(conn, bob, "#meshnet from bob", name="bob", now=1001)
    authors = [author_of(alice), author_of(bob)]

    mute(conn, author_of(bob), name="bob")
    assert [post["text"] for post in timeline(conn, authors, tag="meshnet")] == [
        "#meshnet from alice"
    ]
    assert tags(conn) == [{"tag": "meshnet", "count": 1}]


def test_cli_tan_tags(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert main(["tan", "tags", "--data-dir", str(data_dir)]) == 0
    assert "No hashtags in stored posts yet." in capsys.readouterr().out
    for text in ("#meshnet hello", "#meshnet again", "#ato time"):
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
    assert main(["tan", "tags", "--data-dir", str(data_dir), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "tags": [
            {"tag": "meshnet", "count": 2},
            {"tag": "ato", "count": 1},
        ],
        "count": 2,
    }
    assert main(["tan", "tags", "--data-dir", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "2  #meshnet" in out
    assert "1  #ato" in out


def test_cli_timeline_tag(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    for text in ("#meshnet hello", "plain post"):
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
                "timeline",
                "--data-dir",
                str(data_dir),
                "--all",
                "--tag",
                "meshnet",
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["posts"][0]["text"] == "#meshnet hello"
    assert (
        main(
            [
                "tan",
                "timeline",
                "--data-dir",
                str(data_dir),
                "--all",
                "--tag",
                "two words",
                "--json",
            ]
        )
        == 1
    )
    assert "not a tag" in capsys.readouterr().err


def test_extract_tags_skips_tags_that_do_not_survive_casefolding():
    # "İ" casefolds to "i" plus a combining mark, which a tag cannot
    # carry — so it is no tag at all, rather than a mangled one.
    assert extract_tags("about #İ") == []
    # Every tag extraction returns passes clean_tag: the two agree.
    for text in (
        "#MeshNet #ato-2026",
        "#café #tag_1 (see #yo!)",
        "plain text, no tags",
    ):
        for tag in extract_tags(text):
            assert clean_tag(tag) == tag
            assert clean_tag("#" + tag) == tag


def test_timeline_tag_excludes_repost_of_deleted_post(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    tagged = create_post(conn, alice, "tagged #meshnet", name="alice", now=1000)
    create_repost(conn, bob, tagged["hash"], name="bob", now=1001)
    create_delete(conn, alice, tagged["hash"], name="alice", now=1002)
    authors = [tagged["author"], author_of(bob)]

    # The repost must not match on the deleted original's text: the
    # tag view stays empty instead of showing a blank repost that
    # leaks that the deleted post carried the tag.
    assert timeline(conn, authors, tag="meshnet") == []
    assert timeline(conn, include_all=True, tag="meshnet") == []
    assert tags(conn) == []


def test_timeline_tag_hidden_repost_does_not_consume_limit(identities, tmp_path):
    alice, bob = identities
    carol = RNS.Identity()
    conn = init_db(tmp_path / "tan.sqlite3")
    visible = create_post(conn, alice, "#meshnet visible", name="alice", now=1000)
    hidden = create_post(conn, bob, "#meshnet hidden", name="bob", now=1001)
    repost = create_repost(conn, carol, hidden["hash"], name="carol", now=1002)
    authors = [visible["author"], hidden["author"], author_of(carol)]

    mute(conn, author_of(bob), name="bob")
    # The repost of bob's post is hidden by the timeline's repost
    # rules, so with limit 1 the tag view must page past it to the
    # visible post instead of coming back empty.
    assert [post["hash"] for post in timeline(conn, authors, tag="meshnet", limit=1)] == [
        visible["hash"]
    ]
    assert [post["hash"] for post in timeline(conn, include_all=True, tag="meshnet", limit=1)] == [
        visible["hash"]
    ]
    # The author view keeps reposts of muted authors, the way its
    # untagged view does — the filter mirrors the call site.
    assert [post["hash"] for post in author_timeline(conn, author_of(carol), tag="meshnet")] == [
        repost["hash"]
    ]


# -- disappearing posts -------------------------------------------------------


def _expire_stored_post(conn, digest, at=1.0):
    """Move a stored post's expiry into the past, as if time had passed."""
    conn.execute("UPDATE posts SET expires_at = ? WHERE hash = ?", (at, digest))
    conn.commit()


def test_create_post_with_expiry_round_trips(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    now = time.time()
    post = create_post(
        conn, alice, "here today", name="alice", now=now, expires_at=now + 3600
    )
    assert post["expires_at"] == pytest.approx(now + 3600)
    assert get_post(conn, post["hash"])["expires_at"] == pytest.approx(now + 3600)
    assert verify_post(post)["hash"] == post["hash"]
    assert is_expired(post, now=now) is False
    assert is_expired(post, now=now + 3600) is True


def test_create_post_rejects_expiry_at_or_before_creation(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    with pytest.raises(PostError, match="after created_at"):
        create_post(conn, alice, "dead on arrival", name="alice", now=1000, expires_at=1000)
    with pytest.raises(PostError, match="after created_at"):
        create_post(
            conn, alice, "already gone", name="alice", now=1000, expires_at=999
        )


def test_expiry_is_signed(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    now = time.time()
    post = create_post(
        conn, alice, "tamper-evident", name="alice", now=now, expires_at=now + 3600
    )
    stretched = dict(post, expires_at=now + 7200)
    with pytest.raises(PostError, match="hash does not match"):
        verify_post(stretched)
    stripped = dict(post)
    del stripped["expires_at"]
    with pytest.raises(PostError, match="hash does not match"):
        verify_post(stripped)


def test_post_without_expiry_still_verifies(identities, tmp_path):
    # A post written before expiry existed carries no expires_at and
    # must verify exactly as before.
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    post = create_post(conn, alice, "legacy post", name="alice", now=1000)
    assert post["expires_at"] is None
    assert verify_post(post)["hash"] == post["hash"]

    body = {
        "v": TAN_VERSION,
        "author": author_of(alice),
        "key": alice.get_public_key().hex(),
        "name": "alice",
        "text": "hand-built legacy",
        "created_at": 1000.0,
        "prev": "",
    }
    digest = hash(body, previous="")
    mesg = dict(body, hash=digest, sig=alice.sign(bytes.fromhex(digest)).hex())
    verified = verify_post(mesg, now=1000)
    assert verified["expires_at"] is None
    assert verified["hash"] == digest


def test_expired_post_hidden_from_display_but_kept_for_sync(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    now = time.time()
    live = create_post(conn, alice, "staying live mesh", name="alice", now=now)
    dying = create_post(
        conn, alice, "going away mesh", name="alice", now=now + 1,
        expires_at=now + 3601,
    )
    assert [item["hash"] for item in timeline(conn, [live["author"]])] == [
        dying["hash"],
        live["hash"],
    ]
    _expire_stored_post(conn, dying["hash"], at=now - 10)

    # Every display surface hides it; the sync feed still carries it so
    # chains stay complete.
    assert [item["hash"] for item in timeline(conn, [live["author"]])] == [
        live["hash"]
    ]
    assert [item["hash"] for item in timeline(conn, include_all=True)] == [
        live["hash"]
    ]
    assert [item["hash"] for item in author_timeline(conn, live["author"])] == [
        live["hash"]
    ]
    assert [item["hash"] for item in feed(conn, posts_only=True)] == [live["hash"]]
    assert [item["hash"] for item in feed(conn, author=live["author"], posts_only=True)] == [
        live["hash"]
    ]
    assert [item["hash"] for item in search_posts(conn, "mesh")] == [live["hash"]]
    assert [item["hash"] for item in top_posts(conn)] == [live["hash"]]
    assert [tag["tag"] for tag in tags(conn)] == []
    assert [item["hash"] for item in feed(conn)] == [dying["hash"], live["hash"]]
    # The row is still stored, only hidden.
    assert get_post(conn, dying["hash"]) is not None


def test_resolve_post_rejects_expired_post(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    now = time.time()
    post = create_post(
        conn, alice, "gone soon", name="alice", now=now, expires_at=now + 3600
    )
    assert resolve_post(conn, post["hash"])["hash"] == post["hash"]
    _expire_stored_post(conn, post["hash"], at=now - 10)
    with pytest.raises(TanError, match="has expired"):
        resolve_post(conn, post["hash"])
    with pytest.raises(TanError, match="has expired"):
        resolve_post(conn, post["hash"][:12])


def test_thread_hides_expired_reply_and_rejects_expired_root(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    now = time.time()
    root = create_post(conn, alice, "root post", name="alice", now=now)
    reply = create_reply(
        conn, bob, root["hash"], "short-lived reply", name="bob", now=now,
        expires_at=now + 3600,
    )
    assert [item["hash"] for item in thread(conn, root["hash"])] == [
        root["hash"],
        reply["hash"],
    ]
    _expire_stored_post(conn, reply["hash"], at=now - 10)
    assert [item["hash"] for item in thread(conn, root["hash"])] == [root["hash"]]
    _expire_stored_post(conn, root["hash"], at=now - 10)
    with pytest.raises(TanError, match="has expired"):
        thread(conn, root["hash"])


def test_reply_to_expired_post_raises(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    now = time.time()
    post = create_post(
        conn, alice, "about to go", name="alice", now=now, expires_at=now + 3600
    )
    _expire_stored_post(conn, post["hash"], at=now - 10)
    with pytest.raises(TanError, match="has expired"):
        create_reply(conn, bob, post["hash"], "too late", name="bob", now=now)


def test_reply_with_expiry_round_trips(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    now = time.time()
    root = create_post(conn, alice, "root post", name="alice", now=now)
    reply = create_reply(
        conn, bob, root["hash"], "fading reply", name="bob", now=now,
        expires_at=now + 3600,
    )
    assert reply["expires_at"] == pytest.approx(now + 3600)
    assert verify_post(reply)["hash"] == reply["hash"]
    assert [item["hash"] for item in thread(conn, root["hash"])] == [
        root["hash"],
        reply["hash"],
    ]


def test_expired_parent_preview_is_blanked(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    now = time.time()
    parent = create_post(
        conn, alice, "parent words", name="alice", now=now, expires_at=now + 3600
    )
    create_reply(conn, bob, parent["hash"], "child words", name="bob", now=now)
    authors = [parent["author"], author_of(bob)]
    shown = timeline(conn, authors)
    reply = next(item for item in shown if item.get("kind") == "reply")
    assert reply["reply_to"]["text"] == "parent words"
    _expire_stored_post(conn, parent["hash"], at=now - 10)
    shown = timeline(conn, authors)
    reply = next(item for item in shown if item.get("kind") == "reply")
    assert reply["reply_to"]["text"] == ""
    assert reply["reply_to"]["expired"] is True


def test_expired_post_dropped_from_saved_and_pinned(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    now = time.time()
    post = create_post(
        conn, alice, "save me quick", name="alice", now=now, expires_at=now + 3600
    )
    save_post(conn, post["hash"])
    pin_post(conn, post["hash"], owner=post["author"])
    assert [item["hash"] for item in saved_posts(conn)] == [post["hash"]]
    assert [item["hash"] for item in pinned_posts(conn)] == [post["hash"]]
    _expire_stored_post(conn, post["hash"], at=now - 10)
    assert saved_posts(conn) == []
    assert pinned_posts(conn) == []


def test_mentions_hide_expired_replies(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    now = time.time()
    post = create_post(conn, alice, "mention me", name="alice", now=now)
    reply = create_reply(
        conn, bob, post["hash"], "hello alice", name="bob", now=now,
        expires_at=now + 3600,
    )
    assert [item["hash"] for item in mentions(conn, post["author"])] == [
        reply["hash"]
    ]
    _expire_stored_post(conn, reply["hash"], at=now - 10)
    assert mentions(conn, post["author"]) == []


def test_init_db_migrates_pre_expiry_store(identities, tmp_path):
    alice, _ = identities
    db_path = tmp_path / "tan.sqlite3"
    conn = init_db(db_path)
    post = create_post(conn, alice, "legacy row", name="alice", now=1000)
    conn.execute("ALTER TABLE posts DROP COLUMN expires_at")
    conn.commit()
    conn.close()
    conn = init_db(db_path)
    try:
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(posts)").fetchall()
        }
        assert "expires_at" in columns
        stored = get_post(conn, post["hash"])
        assert stored is not None
        assert stored["expires_at"] is None
        assert verify_post(stored)["hash"] == post["hash"]
    finally:
        conn.close()


def test_cli_post_expire_stores_expiry(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    capsys.readouterr()
    before = time.time()
    assert (
        main(
            [
                "tan",
                "post",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                "--expire",
                "1h",
                "here for an hour",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    expires_at = payload["post"]["expires_at"]
    assert before + 3600 - 5 <= expires_at <= time.time() + 3600 + 5
    conn = init_db(data_dir / "tan.sqlite3")
    try:
        assert [item["hash"] for item in timeline(conn, include_all=True)] == [
            payload["post"]["hash"]
        ]
    finally:
        conn.close()


def test_cli_post_expire_rejects_delay_not_shorter_than_expiry(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(
            [
                "tan",
                "post",
                "--data-dir",
                str(data_dir),
                "--delay",
                "2h",
                "--expire",
                "1h",
                "never arrives",
            ]
        )
        == 2
    )
    assert "must be shorter than --expire" in capsys.readouterr().err


def test_cli_post_expire_rejects_over_cap(tmp_path, capsys):
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
                "--expire",
                "8d",
                "too long-lived",
            ]
        )
        == 2
    )
    assert "at most 1w" in capsys.readouterr().err


def test_saved_posts_page_past_expired_saves(identities, tmp_path):
    # Expired saves are filtered in SQL, before LIMIT/OFFSET, so a page
    # never comes up short and later saves are never hidden behind the
    # offset — the failure the first autofix left in place.
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    now = time.time()
    oldest = create_post(conn, alice, "oldest save", name="alice", now=now)
    dying = create_post(
        conn, alice, "middle save", name="alice", now=now + 1,
        expires_at=now + 3601,
    )
    newest = create_post(conn, alice, "newest save", name="alice", now=now + 2)
    save_post(conn, oldest["hash"], now=100.0)
    save_post(conn, dying["hash"], now=200.0)
    save_post(conn, newest["hash"], now=300.0)
    assert [item["hash"] for item in saved_posts(conn)] == [
        newest["hash"],
        dying["hash"],
        oldest["hash"],
    ]
    _expire_stored_post(conn, dying["hash"], at=now - 10)
    assert [item["hash"] for item in saved_posts(conn)] == [
        newest["hash"],
        oldest["hash"],
    ]
    assert [item["hash"] for item in saved_posts(conn, limit=1, offset=1)] == [
        oldest["hash"]
    ]


def test_timeline_survives_newest_expired_posts(identities, tmp_path):
    # CodeRabbit Major on PR #356: when the newest page of posts had
    # expired, the timeline came back empty even though older live posts
    # existed — the page was fetched first and expired rows filtered
    # after LIMIT, so _fill_unmuted read the short page as exhaustion.
    # Expired rows are now excluded in SQL, before LIMIT, so a page of
    # expired posts never hides the live ones behind it.
    alice, _ = identities
    conn = init_db(tmp_path / "tan.sqlite3")
    now = time.time()
    live = [
        create_post(conn, alice, f"live {index}", name="alice", now=now + index)
        for index in range(5)
    ]
    dying = [
        create_post(
            conn, alice, f"dying {index}", name="alice", now=now + 100 + index,
            expires_at=now + 3600,
        )
        for index in range(25)
    ]
    for post in dying:
        _expire_stored_post(conn, post["hash"], at=now - 10)
    author = author_of(alice)
    expected = [item["hash"] for item in reversed(live)]
    assert [item["hash"] for item in timeline(conn, [author])] == expected
    assert [item["hash"] for item in author_timeline(conn, author)] == expected
    assert [item["hash"] for item in feed(conn, posts_only=True)] == expected
