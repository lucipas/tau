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
from tau.tangents.tan import (
    HASH_FIELDS,
    TAN_EXPORT_FORMAT,
    TAN_FEED_PATH,
    TAN_POST_PATH,
    PostError,
    TanApp,
    TanError,
    annotate_edits,
    annotate_quote_counts,
    annotate_quotes,
    annotate_reactions,
    annotate_replies,
    author_stats,
    author_timeline,
    create_delete,
    create_edit,
    create_post,
    create_quote,
    create_reaction,
    create_reply,
    create_repost,
    deleted_targets,
    edit_states,
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
    likers,
    list_authors,
    mute_keyword,
    muted_keywords,
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
    save_post,
    saved_posts,
    search_posts,
    store,
    store_post,
    SUGGEST_FOLLOWED_WEIGHT,
    suggest_authors,
    thread,
    timeline,
    unfollow,
    unmute,
    unmute_keyword,
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
