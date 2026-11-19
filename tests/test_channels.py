from __future__ import annotations

import json
import time

import pytest
import RNS

from tau.blocklist import BlockList, save_blocklist
from tau.channels import (
    CHANNEL_FEED_PATH,
    CHANNEL_POST_PATH,
    CHANNEL_REACT_PATH,
    MAX_STORED_PER_CHANNEL,
    ChannelApp,
    ChannelError,
    channel_history,
    channel_history_page,
    channel_unread_counts,
    clean_channel_name,
    create_message,
    create_topic,
    create_reaction,
    get_message,
    get_topic,
    init_db,
    is_member,
    is_pinned,
    join_channel,
    leave_channel,
    list_memberships,
    list_pins,
    mark_channel_read,
    pin_message,
    reactions_for_messages,
    search_channel_messages,
    store_message,
    store_topic,
    store_reaction,
    unpin_message,
    summarize_reactions,
    verify_message,
    verify_topic,
    verify_reaction,
    create_edit,
    create_retraction,
    current_message_text,
    get_edit,
    get_retraction,
    hash_message,
    hash_retraction,
    latest_edit_for,
    list_edits,
    list_retractions,
    reply_parent,
    resolve_message_digest,
    store_edit,
    store_retraction,
    verify_edit,
    verify_retraction,
)
from tau.cli import main
from tau.config import TauConfig
from tau.peers import Peer
from tau.protocol import APP_NAME, ASPECT, decode, encode


@pytest.fixture()
def identities():
    return RNS.Identity(), RNS.Identity()


def author_of(identity):
    return RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()


# -- names -----------------------------------------------------------------


def test_clean_channel_name_accepts_plain_names():
    assert clean_channel_name("mesh-meetup") == "mesh-meetup"
    assert clean_channel_name("  general  ") == "general"
    assert clean_channel_name("a.b_c-1") == "a.b_c-1"


@pytest.mark.parametrize(
    "bad",
    ["", "   ", "has space", "has/slash", ".leading-dot", "-leading-dash", "x" * 65],
)
def test_clean_channel_name_rejects_bad_names(bad):
    with pytest.raises(ChannelError):
        clean_channel_name(bad)


# -- signing and storage -----------------------------------------------------


def test_create_message_signs_and_verifies(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    message = create_message(conn, alice, "general", "hello mesh", name="alice", now=1000)

    assert message["channel"] == "general"
    assert message["author"] == author_of(alice)
    assert verify_message(message)["hash"] == message["hash"]
    assert get_message(conn, message["hash"])["text"] == "hello mesh"


def test_channel_is_part_of_the_signed_body(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    message = create_message(conn, alice, "general", "hello mesh", name="alice", now=1000)

    # A relay cannot move a message into another room: the same body
    # under a different channel hashes differently and fails verification.
    moved = dict(message, channel="other-room")
    with pytest.raises(ChannelError):
        verify_message(moved)


def test_store_rejects_tampered_message(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    message = create_message(conn, alice, "general", "hello mesh", name="alice", now=1000)

    with pytest.raises(ChannelError):
        store_message(conn, dict(message, text="forged"))


def test_store_rejects_a_key_from_another_identity(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    message = create_message(conn, alice, "general", "hello mesh", name="alice", now=1000)

    with pytest.raises(ChannelError):
        store_message(conn, dict(message, key=bob.get_public_key().hex()))


def test_duplicate_message_is_idempotent(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    message = create_message(conn, alice, "general", "hello mesh", name="alice", now=1000)

    record, is_new = store_message(conn, message)
    assert is_new is False
    assert record["hash"] == message["hash"]
    assert len(channel_history(conn, "general")) == 1


# -- topics ------------------------------------------------------------------


def test_create_topic_signs_and_verifies(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    topic = create_topic(conn, alice, "general", "monthly mesh meetup", now=1000)

    assert topic["channel"] == "general"
    assert topic["author"] == author_of(alice)
    assert verify_topic(topic)["hash"] == topic["hash"]
    assert get_topic(conn, "general")["topic"] == "monthly mesh meetup"


def test_topic_newest_wins_and_stale_clear_is_ignored(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    create_topic(conn, alice, "general", "first plan", now=1000)

    # Envelopes are built on a scratch store so store_topic's own
    # verdict is what the test sees — create_topic would apply the
    # envelope to the main store first and mask it.
    scratch = init_db(tmp_path / "scratch.sqlite3")

    # An older topic never overwrites a newer one.
    record, changed = store_topic(
        conn, create_topic(scratch, bob, "general", "stale plan", now=999)
    )
    assert record["topic"] == "first plan"
    assert changed is False
    assert get_topic(conn, "general")["topic"] == "first plan"

    # A newer topic wins, whoever wrote it.
    record, changed = store_topic(
        conn, create_topic(scratch, bob, "general", "new plan", now=1001)
    )
    assert record["topic"] == "new plan"
    assert changed is True
    assert get_topic(conn, "general")["topic"] == "new plan"

    # An empty topic clears, but a stale clear must not wipe a
    # newer topic: the stored topic stands, unchanged.
    record, changed = store_topic(
        conn, create_topic(scratch, alice, "general", "", now=1000)
    )
    assert record is not None
    assert record["topic"] == "new plan"
    assert changed is False
    assert get_topic(conn, "general")["topic"] == "new plan"

    record, changed = store_topic(
        conn, create_topic(scratch, alice, "general", "", now=1002)
    )
    assert record is None
    assert changed is True
    assert get_topic(conn, "general") is None


def test_topic_rejects_overlong_and_unnormalised(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    with pytest.raises(ChannelError, match="longer than 140"):
        create_topic(conn, alice, "general", "x" * 141, now=1000)
    with pytest.raises(ChannelError, match="not normalised"):
        create_topic(conn, alice, "general", "two  spaces", now=1000)
    with pytest.raises(ChannelError, match="not normalised"):
        create_topic(conn, alice, "general", "line\nbreak", now=1000)


def test_verify_topic_rejects_tampered(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    topic = create_topic(conn, alice, "general", "monthly mesh meetup", now=1000)

    with pytest.raises(ChannelError):
        verify_topic(dict(topic, topic="forged"))
    with pytest.raises(ChannelError):
        verify_topic(dict(topic, channel="other-room"))
    with pytest.raises(ChannelError):
        store_topic(conn, dict(topic, topic="forged"))


def test_handle_post_stores_topic_and_answers(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    topic = create_topic(source, bob, "general", "bob's room", now=1000)

    app = _make_app(identities, tmp_path)
    response = app._handle_post(
        CHANNEL_POST_PATH,
        encode({"topic": topic, "ttl": 2}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert payload["ok"] is True
    assert payload["updated"] is True
    assert payload["topic"] == "general"
    assert app.topic("general")["topic"] == "bob's room"

    # A second copy is a duplicate, not an error.
    response = app._handle_post(
        CHANNEL_POST_PATH,
        encode({"topic": topic, "ttl": 2}),
        b"id",
        None,
        time.time(),
    )
    assert decode(response)["updated"] is False


def test_handle_post_refuses_blocked_topic_author(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    topic = create_topic(source, bob, "general", "bob's room", now=1000)

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    save_blocklist(config.blocklist_path, BlockList(blocked=frozenset({topic["author"]})))
    app = ChannelApp(config, alice, name="alice", gossip=False)
    response = app._handle_post(
        CHANNEL_POST_PATH,
        encode({"topic": topic, "ttl": 2}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "not_admitted"
    assert app.topic("general") is None


def test_handle_feed_includes_topic(identities, tmp_path):
    alice, bob = identities
    app = _make_app(identities, tmp_path)
    conn = app.connect()
    try:
        create_topic(conn, bob, "general", "bob's room", now=1000)
    finally:
        conn.close()

    response = app._handle_feed(
        CHANNEL_FEED_PATH,
        encode({"channel": "general", "limit": 20}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert payload["ok"] is True
    assert payload["topic"]["topic"] == "bob's room"

    # A topic from a blocked author is not served.
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    save_blocklist(config.blocklist_path, BlockList(blocked=frozenset({payload["topic"]["author"]})))
    blocked_app = ChannelApp(config, alice, name="alice", gossip=False)
    payload = decode(
        blocked_app._handle_feed(
            CHANNEL_FEED_PATH,
            encode({"channel": "general", "limit": 20}),
            b"id",
            None,
            time.time(),
        )
    )
    assert payload["topic"] is None


def test_sync_pulls_topic_without_messages(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    topic = create_topic(source, bob, "general", "bob's room", now=1000)

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)
    peer = Peer(
        destination_hash=bytes.fromhex(topic["author"]),
        name="bob",
        public_key=bob.get_public_key(),
        first_seen=1000.0,
        last_seen=1000.0,
    )

    class _TopicNode:
        def __init__(self, peer, topic):
            self.registry = _FakeRegistry(peer)
            self.topic = topic

        def request(self, peer, path, payload, *, timeout=20.0):
            assert path == CHANNEL_FEED_PATH
            return {"ok": True, "messages": [], "has_more": False, "topic": self.topic}

    node = _TopicNode(peer, topic)
    app.node = node  # type: ignore[assignment]

    summary = app.sync(node, "general", limit=20)
    assert summary["topic_updated"] is True
    assert summary["stored"] == 0
    assert summary["errors"] == []
    assert app.topic("general")["topic"] == "bob's room"

    # A second sync is a no-op for the topic.
    summary = app.sync(node, "general", limit=20)
    assert summary["topic_updated"] is False


def test_set_topic_requires_membership(identities, tmp_path):
    alice, _ = identities
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)
    with pytest.raises(ChannelError, match="not a member"):
        app.set_topic("general", "hello", publish=False)
    app.join("general")
    result = app.set_topic("general", "hello", publish=False)
    assert result["topic"]["topic"] == "hello"
    assert result["cleared"] is False
    assert app.topic("general")["topic"] == "hello"
    result = app.set_topic("general", "", publish=False)
    assert result["cleared"] is True
    assert app.topic("general") is None


def test_history_orders_newest_first_and_pages_by_before(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    create_message(conn, alice, "general", "one", name="alice", now=1000)
    create_message(conn, alice, "general", "two", name="alice", now=1001)
    create_message(conn, alice, "general", "three", name="alice", now=1002)
    create_message(conn, alice, "other", "elsewhere", name="alice", now=1003)

    page = channel_history(conn, "general", limit=2)
    assert [item["text"] for item in page] == ["three", "two"]
    older = channel_history(conn, "general", limit=2, before=page[-1]["created_at"])
    assert [item["text"] for item in older] == ["one"]
    assert [item["text"] for item in channel_history(conn, "other")] == ["elsewhere"]


def test_search_matches_substring_case_insensitively(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    create_message(conn, alice, "general", "the bridge is up", name="alice", now=1000)
    create_message(conn, alice, "general", "BRIDGE repairs tonight", name="alice", now=1001)
    create_message(conn, alice, "general", "Äpfel und Brötchen", name="alice", now=1002)
    create_message(conn, alice, "general", "unrelated", name="alice", now=1003)
    create_message(conn, alice, "other", "bridge in another channel", name="alice", now=1004)

    assert [
        item["text"] for item in search_channel_messages(conn, "general", "bridge")
    ] == ["BRIDGE repairs tonight", "the bridge is up"]
    assert [
        item["text"] for item in search_channel_messages(conn, "general", "äpfel")
    ] == ["Äpfel und Brötchen"]
    assert search_channel_messages(conn, "general", "nope") == []


def test_search_respects_limit_and_excluded_authors(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    create_message(conn, alice, "general", "mesh one", name="alice", now=1000)
    create_message(conn, bob, "general", "mesh two", name="bob", now=1001)
    create_message(conn, alice, "general", "mesh three", name="alice", now=1002)

    assert [
        item["text"]
        for item in search_channel_messages(conn, "general", "mesh", limit=2)
    ] == ["mesh three", "mesh two"]
    assert [
        item["text"]
        for item in search_channel_messages(
            conn, "general", "mesh", exclude_authors=[author_of(bob)]
        )
    ] == ["mesh three", "mesh one"]


def test_search_rejects_empty_query(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    create_message(conn, alice, "general", "hello", name="alice", now=1000)
    with pytest.raises(ChannelError, match="must not be empty"):
        search_channel_messages(conn, "general", "   ")


# -- replies -----------------------------------------------------------------


def test_reply_is_part_of_the_signed_body(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    parent = create_message(conn, alice, "general", "meet at the park", name="alice", now=1000)
    reply = create_message(
        conn, alice, "general", "see you there", name="alice", now=1001,
        reply_to=parent["hash"],
    )

    assert reply["reply_to"] == parent["hash"]
    assert verify_message(reply)["hash"] == reply["hash"]
    assert "reply_to" not in create_message(
        conn, alice, "general", "plain", name="alice", now=1002
    )

    # A relay cannot re-point a reply at another message: the digest
    # is covered by the hash, so a changed target fails verification.
    other = create_message(conn, alice, "general", "other", name="alice", now=1003)
    with pytest.raises(ChannelError):
        verify_message(dict(reply, reply_to=other["hash"]))
    with pytest.raises(ChannelError):
        verify_message({k: v for k, v in reply.items() if k != "reply_to"})


def test_reply_without_a_parent_field_still_verifies(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    message = create_message(conn, alice, "general", "hello mesh", name="alice", now=1000)

    # A message written before replies existed carries no reply_to at
    # all; it must still verify exactly as before.
    legacy = {k: v for k, v in message.items() if k != "reply_to"}
    assert verify_message(legacy)["hash"] == message["hash"]


def test_create_reply_requires_a_stored_parent(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    parent = create_message(conn, alice, "general", "meet at the park", name="alice", now=1000)
    elsewhere = create_message(conn, alice, "other", "elsewhere", name="alice", now=1001)

    with pytest.raises(ChannelError, match="unknown message"):
        create_message(conn, alice, "general", "reply", name="alice", now=1002,
                       reply_to="f" * 64)
    with pytest.raises(ChannelError, match="unknown message"):
        create_message(conn, alice, "general", "reply", name="alice", now=1002,
                       reply_to="abc12345")
    with pytest.raises(ChannelError, match="at least 4 hex"):
        create_message(conn, alice, "general", "reply", name="alice", now=1002,
                       reply_to="abc")
    with pytest.raises(ChannelError, match="at least 4 hex"):
        create_message(conn, alice, "general", "reply", name="alice", now=1002,
                       reply_to="not-hex!!")
    # A parent in another channel is not a parent in this one.
    with pytest.raises(ChannelError, match="unknown message"):
        create_message(conn, alice, "general", "reply", name="alice", now=1002,
                       reply_to=elsewhere["hash"])


def test_reply_resolves_a_unique_prefix(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    parent = create_message(conn, alice, "general", "meet at the park", name="alice", now=1000)

    reply = create_message(
        conn, alice, "general", "see you there", name="alice", now=1001,
        reply_to=parent["hash"][:12],
    )
    assert reply["reply_to"] == parent["hash"]
    assert resolve_message_digest(conn, "general", parent["hash"]) == parent["hash"]


def test_reply_parent_resolves_from_the_store(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    parent = create_message(conn, alice, "general", "meet at the park", name="alice", now=1000)
    reply = create_message(
        conn, alice, "general", "see you there", name="alice", now=1001,
        reply_to=parent["hash"],
    )
    plain = create_message(conn, alice, "general", "plain", name="alice", now=1002)

    assert reply_parent(conn, reply)["hash"] == parent["hash"]
    assert reply_parent(conn, reply)["text"] == "meet at the park"
    assert reply_parent(conn, plain) is None

    # A reply whose parent has not arrived yet still stores and
    # verifies; the parent simply resolves to nothing until it lands.
    body = {
        "v": 1,
        "channel": "general",
        "author": parent["author"],
        "key": alice.get_public_key().hex(),
        "name": "alice",
        "text": "early reply",
        "created_at": 1003.0,
        "reply_to": "a" * 64,
    }
    digest = hash_message(body)
    signed = dict(body, hash=digest, sig=alice.sign(bytes.fromhex(digest)).hex())
    record, is_new = store_message(conn, signed)
    assert is_new is True
    assert record["reply_to"] == "a" * 64
    assert reply_parent(conn, record) is None


def test_old_database_gains_a_reply_column(identities, tmp_path):
    import sqlite3

    alice, _ = identities
    path = tmp_path / "channels.sqlite3"
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE messages ("
        "v INTEGER NOT NULL DEFAULT 1, hash TEXT PRIMARY KEY, "
        "channel TEXT NOT NULL, author TEXT NOT NULL, key TEXT NOT NULL, "
        "name TEXT NOT NULL DEFAULT '', text TEXT NOT NULL, "
        "created_at REAL NOT NULL, received_at REAL NOT NULL, sig TEXT NOT NULL)"
    )
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute(
        "CREATE TABLE memberships (channel TEXT PRIMARY KEY, joined_at REAL NOT NULL)"
    )
    conn.commit()
    conn.close()

    conn = init_db(path)
    try:
        columns = [
            row[1] for row in conn.execute("PRAGMA table_info(messages)").fetchall()
        ]
        assert "reply_to" in columns
        parent = create_message(conn, alice, "general", "old db", name="alice", now=1000)
        reply = create_message(
            conn, alice, "general", "still works", name="alice", now=1001,
            reply_to=parent["hash"],
        )
        assert reply["reply_to"] == parent["hash"]
        assert get_message(conn, reply["hash"])["reply_to"] == parent["hash"]
    finally:
        conn.close()


def test_feed_serves_replies_with_their_parent_digest(identities, tmp_path):
    alice, bob = identities
    app = _make_app(identities, tmp_path)
    conn = app.connect()
    try:
        parent = create_message(conn, bob, "general", "one", name="bob", now=1000)
        create_message(
            conn, alice, "general", "two", name="alice", now=1001,
            reply_to=parent["hash"],
        )
    finally:
        conn.close()

    response = app._handle_feed(
        CHANNEL_FEED_PATH,
        encode({"channel": "general", "limit": 20}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert payload["ok"] is True
    assert [item["text"] for item in payload["messages"]] == ["two", "one"]
    assert payload["messages"][0]["reply_to"] == parent["hash"]
    assert "reply_to" not in payload["messages"][1]


# -- membership --------------------------------------------------------------


def test_join_leave_list(tmp_path):
    conn = init_db(tmp_path / "channels.sqlite3")
    assert is_member(conn, "general") is False
    assert join_channel(conn, "general")["channel"] == "general"
    assert is_member(conn, "general") is True
    # Joining twice is a no-op, not an error.
    join_channel(conn, "general")
    assert len(list_memberships(conn)) == 1
    assert list_memberships(conn)[0]["messages"] == 0
    assert leave_channel(conn, "general") is True
    assert leave_channel(conn, "general") is False
    assert is_member(conn, "general") is False


def test_send_requires_membership(identities, tmp_path):
    alice, _ = identities
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)
    with pytest.raises(ChannelError, match="not a member"):
        app.send("general", "hello", publish=False)


def test_send_stores_when_joined(identities, tmp_path):
    alice, _ = identities
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)
    app.join("general")
    message = app.send("general", "hello mesh", publish=False)
    assert message["channel"] == "general"
    assert [item["text"] for item in app.history("general")] == ["hello mesh"]


# -- request handlers ----------------------------------------------------------


def _make_app(identities, tmp_path, *, gossip=False):
    alice, _ = identities
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    return ChannelApp(config, alice, name="alice", gossip=gossip)


def test_handle_post_stores_and_answers(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "hi all", name="bob", now=1000)

    app = _make_app(identities, tmp_path)
    response = app._handle_post(
        CHANNEL_POST_PATH,
        encode({"message": message, "ttl": 2}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert payload["ok"] is True
    assert payload["new"] is True
    assert payload["stored"] == message["hash"]

    # A second copy is a duplicate, not an error.
    response = app._handle_post(
        CHANNEL_POST_PATH,
        encode({"message": message, "ttl": 2}),
        b"id",
        None,
        time.time(),
    )
    assert decode(response)["new"] is False


def test_handle_post_rejects_tampered_message(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "hi all", name="bob", now=1000)

    app = _make_app(identities, tmp_path)
    response = app._handle_post(
        CHANNEL_POST_PATH,
        encode({"message": dict(message, text="forged"), "ttl": 2}),
        b"id",
        None,
        time.time(),
    )
    assert decode(response)["ok"] is False


def test_handle_post_refuses_blocked_authors(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "hi all", name="bob", now=1000)

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    save_blocklist(config.blocklist_path, BlockList(blocked=frozenset({message["author"]})))
    app = ChannelApp(config, alice, name="alice", gossip=False)
    response = app._handle_post(
        CHANNEL_POST_PATH,
        encode({"message": message, "ttl": 2}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "not_admitted"
    conn = app.connect()
    try:
        assert channel_history(conn, "general") == []
    finally:
        conn.close()


def test_handle_feed_returns_channel_history(identities, tmp_path):
    alice, bob = identities
    app = _make_app(identities, tmp_path)
    conn = app.connect()
    try:
        create_message(conn, bob, "general", "one", name="bob", now=1000)
        create_message(conn, bob, "general", "two", name="bob", now=1001)
    finally:
        conn.close()

    response = app._handle_feed(
        CHANNEL_FEED_PATH,
        encode({"channel": "general", "limit": 20}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert payload["ok"] is True
    assert [item["text"] for item in payload["messages"]] == ["two", "one"]

    response = app._handle_feed(
        CHANNEL_FEED_PATH,
        encode({"channel": "general", "before": 1001.0, "limit": 20}),
        b"id",
        None,
        time.time(),
    )
    assert [item["text"] for item in decode(response)["messages"]] == ["one"]


# -- sync and publish ----------------------------------------------------------


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
        assert path == CHANNEL_FEED_PATH
        messages, has_more = channel_history_page(
            self.source,
            request["channel"],
            limit=request.get("limit", 20),
            before=request.get("before"),
            before_hash=request.get("before_hash"),
        )
        return {"ok": True, "messages": messages, "has_more": has_more}


def test_sync_pulls_missing_history(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    first = create_message(source, bob, "general", "bob one", name="bob", now=1000)
    create_message(source, bob, "general", "bob two", name="bob", now=1001)

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)
    peer = Peer(
        destination_hash=bytes.fromhex(first["author"]),
        name="bob",
        public_key=bob.get_public_key(),
        first_seen=1000.0,
        last_seen=1000.0,
    )
    node = _FakeNode(peer, source)
    app.node = node  # type: ignore[assignment]

    summary = app.sync(node, "general", limit=20)
    assert summary["stored"] == 2
    assert summary["errors"] == []
    assert [item["text"] for item in app.history("general")] == [
        "bob two",
        "bob one",
    ]


def test_sync_pages_through_a_timestamp_tie(identities, tmp_path):
    # Three messages share one created_at; a page of two ends inside
    # the tie.  A timestamp-only cursor would drop the third message
    # for good — the (timestamp, hash) cursor must fetch it.
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    for text in ("one", "two", "three"):
        create_message(source, bob, "general", text, name="bob", now=1000)

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)
    peer = Peer(
        destination_hash=bytes.fromhex(author_of(bob)),
        name="bob",
        public_key=bob.get_public_key(),
        first_seen=1000.0,
        last_seen=1000.0,
    )
    node = _FakeNode(peer, source)
    app.node = node  # type: ignore[assignment]

    summary = app.sync(node, "general", limit=2)
    assert summary["stored"] == 3
    assert summary["errors"] == []
    assert sorted(item["text"] for item in app.history("general")) == [
        "one",
        "three",
        "two",
    ]


def test_sync_survives_bad_peer_entries(identities, tmp_path):
    # A peer page with a non-object entry and a malformed message must
    # not abort the sync: the good entry still lands, the bad ones are
    # recorded as errors.
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    good = create_message(source, bob, "general", "good", name="bob", now=1000)
    bad_text = dict(good, hash="00" * 32, text=123)
    bad_sig = dict(good, hash="11" * 32, sig="zz" * 64)

    class _MessyNode(_FakeNode):
        def request(self, peer, path, payload, *, timeout=20.0):
            return {
                "ok": True,
                "messages": ["junk", bad_text, bad_sig, good],
                "has_more": False,
            }

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)
    peer = Peer(
        destination_hash=bytes.fromhex(author_of(bob)),
        name="bob",
        public_key=bob.get_public_key(),
        first_seen=1000.0,
        last_seen=1000.0,
    )
    node = _MessyNode(peer, source)
    app.node = node  # type: ignore[assignment]

    summary = app.sync(node, "general", limit=20)
    assert summary["stored"] == 1
    assert len(summary["errors"]) == 3
    assert [item["text"] for item in app.history("general")] == ["good"]


def test_sync_survives_huge_peer_timestamp(identities, tmp_path):
    # Qodo Warning on #477: sync built its pagination cursor with
    # float(raw["created_at"]) outside the per-entry error handler,
    # so one peer entry with a timestamp too large for a double raised
    # OverflowError and aborted sync for the remaining peers.  The
    # huge entry is recorded as an error; the good entry still lands.
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    good = create_message(source, bob, "general", "good", name="bob", now=1000)
    huge = dict(good, hash="22" * 32, created_at=10**400)

    class _HugeNode(_FakeNode):
        def request(self, peer, path, payload, *, timeout=20.0):
            return {"ok": True, "messages": [huge, good], "has_more": False}

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)
    peer = Peer(
        destination_hash=bytes.fromhex(author_of(bob)),
        name="bob",
        public_key=bob.get_public_key(),
        first_seen=1000.0,
        last_seen=1000.0,
    )
    node = _HugeNode(peer, source)
    app.node = node  # type: ignore[assignment]

    summary = app.sync(node, "general", limit=20)
    assert summary["stored"] == 1
    assert len(summary["errors"]) == 1
    assert "too large" in summary["errors"][0]["error"]
    assert [item["text"] for item in app.history("general")] == ["good"]


def test_handle_feed_trims_large_pages_and_reports_more(identities, tmp_path):
    # Fifty 2,000-character messages exceed the 64 KiB response
    # budget, so the feed must trim — and say so, so sync keeps paging.
    alice, bob = identities
    app = _make_app(identities, tmp_path)
    conn = app.connect()
    try:
        for index in range(50):
            create_message(
                conn, bob, "general", "x" * 2000, name="bob", now=1000 + index
            )
    finally:
        conn.close()

    response = app._handle_feed(
        CHANNEL_FEED_PATH,
        encode({"channel": "general", "limit": 50}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert payload["ok"] is True
    assert payload["has_more"] is True
    assert 0 < payload["count"] < 50

    # The cursor from the last returned message fetches the rest.
    last = payload["messages"][-1]
    response = app._handle_feed(
        CHANNEL_FEED_PATH,
        encode(
            {
                "channel": "general",
                "before": last["created_at"],
                "before_hash": last["hash"],
                "limit": 50,
            }
        ),
        b"id",
        None,
        time.time(),
    )
    second = decode(response)
    assert second["ok"] is True
    assert payload["count"] + second["count"] == 50
    assert second["has_more"] is False


def test_handle_feed_hides_blocked_authors(identities, tmp_path):
    alice, bob = identities
    app = _make_app(identities, tmp_path)
    conn = app.connect()
    try:
        create_message(conn, alice, "general", "from alice", name="alice", now=1000)
        create_message(conn, bob, "general", "from bob", name="bob", now=1001)
    finally:
        conn.close()

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    save_blocklist(
        config.blocklist_path, BlockList(blocked=frozenset({author_of(bob)}))
    )
    # The app was built before the block: the feed must still hide bob,
    # reading the live list rather than a startup snapshot.
    response = app._handle_feed(
        CHANNEL_FEED_PATH,
        encode({"channel": "general", "limit": 20}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert [item["text"] for item in payload["messages"]] == ["from alice"]
    assert payload["has_more"] is False


def test_blocked_check_follows_live_blocklist(identities, tmp_path):
    alice, bob = identities
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)

    class _StubNode:
        def __init__(self, blocklist):
            self.blocklist = blocklist

    app.node = _StubNode(BlockList())  # type: ignore[assignment]
    assert app._blocked(author_of(bob)) is False
    app.node = _StubNode(BlockList(blocked=frozenset({author_of(bob)})))  # type: ignore[assignment]
    assert app._blocked(author_of(bob)) is True


def test_huge_timestamps_are_rejected_not_crashed(identities, tmp_path):
    alice, bob = identities
    app = _make_app(identities, tmp_path)
    response = app._handle_feed(
        CHANNEL_FEED_PATH,
        encode({"channel": "general", "before": 10**400, "limit": 20}),
        b"id",
        None,
        time.time(),
    )
    assert decode(response)["ok"] is False

    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "hi", name="bob", now=1000)
    with pytest.raises(ChannelError):
        verify_message(dict(message, created_at=10**400))


def test_wire_size_measures_the_signed_envelope(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    # 2,000 four-byte characters: the unsigned body fits, but the
    # signed envelope does not — creation must refuse it rather than
    # store a message that can never be published.
    with pytest.raises(ChannelError, match="too large"):
        create_message(conn, alice, "general", "\U0001f600" * 2000, name="alice", now=1000)
    message = create_message(conn, alice, "general", "a" * 2000, name="alice", now=1000)
    assert message["text"] == "a" * 2000


def test_store_prunes_per_channel_and_evicted_rooms(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    # Joined channels are never evicted, so join before filling it.
    join_channel(conn, "general")
    for index in range(MAX_STORED_PER_CHANNEL + 5):
        create_message(conn, alice, "general", f"msg {index}", name="alice", now=1000 + index)
    stored = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE channel = 'general'"
    ).fetchone()[0]
    assert stored == MAX_STORED_PER_CHANNEL
    assert channel_history(conn, "general", limit=1)[0]["text"] == (
        f"msg {MAX_STORED_PER_CHANNEL + 4}"
    )

    # One message in each of 101 unjoined rooms: only 100 rooms survive.
    for index in range(101):
        create_message(
            conn, alice, f"room-{index:03d}", "hello", name="alice", now=2000 + index
        )
    rooms = conn.execute("SELECT DISTINCT channel FROM messages").fetchall()
    assert len(rooms) == 100
    assert len(channel_history(conn, "general", limit=1)) == 1


def test_future_dated_posts_do_not_block_local_sends(identities, tmp_path):
    # Qodo Critical on #477: _prune_store ranked by signed created_at,
    # so 500 peer posts dated a day ahead filled the retention budget
    # and a current-time local send was pruned during its own insert.
    # Retention is now capped at receipt time, so the local send lands.
    alice, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    join_channel(conn, "general")
    source = init_db(tmp_path / "source.sqlite3")
    now = time.time()
    for index in range(MAX_STORED_PER_CHANNEL):
        future = create_message(
            source, bob, "general", f"future {index}", name="bob",
            now=now + 3600 + index,
        )
        store_message(conn, future, now=now + index)
    record = create_message(
        conn, alice, "general", "hello now", name="alice",
        now=now + MAX_STORED_PER_CHANNEL,
    )
    assert get_message(conn, record["hash"]) is not None
    # Retention kept the local send and evicted a future-dated post
    # instead: the store still holds exactly the bound.
    stored = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE channel = 'general'"
    ).fetchone()[0]
    assert stored == MAX_STORED_PER_CHANNEL


def test_store_message_reports_eviction_instead_of_raising(identities, tmp_path):
    # An old message arriving into a store that already holds 500
    # newer messages is accepted, then immediately evicted by
    # retention during its own insert.  That is not a disappearance:
    # store_message returns it as not newly stored, without raising.
    alice, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    join_channel(conn, "general")
    for index in range(MAX_STORED_PER_CHANNEL):
        create_message(
            conn, alice, "general", f"msg {index}", name="alice", now=2000 + index
        )
    source = init_db(tmp_path / "source.sqlite3")
    old = create_message(source, bob, "general", "old news", name="bob", now=1000)
    record, is_new = store_message(conn, old, now=1500)
    assert is_new is False
    assert record["hash"] == old["hash"]
    assert get_message(conn, old["hash"]) is None


def test_sync_skips_blocked_authors(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    create_message(source, bob, "general", "bob one", name="bob", now=1000)

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    save_blocklist(
        config.blocklist_path,
        BlockList(blocked=frozenset({author_of(bob)})),
    )
    app = ChannelApp(config, alice, name="alice", gossip=False)
    peer = Peer(
        destination_hash=bytes.fromhex(author_of(bob)),
        name="bob",
        public_key=bob.get_public_key(),
        first_seen=1000.0,
        last_seen=1000.0,
    )
    node = _FakeNode(peer, source)
    app.node = node  # type: ignore[assignment]

    summary = app.sync(node, "general", limit=20)
    assert summary["stored"] == 0
    assert summary["skipped"] == 1


class _RecordingNode:
    def __init__(self, peer):
        self.registry = _FakeRegistry(peer)
        self.requests = []

    def request(self, peer, path, payload, *, timeout=20.0):
        self.requests.append((path, decode(payload)))
        return {"ok": True, "stored": "x", "new": True}


def test_publish_fans_out_to_peers(identities, tmp_path):
    alice, bob = identities
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)
    app.join("general")
    message = app.send("general", "hello", publish=False)

    peer = Peer(
        destination_hash=bytes.fromhex(author_of(bob)),
        name="bob",
        public_key=bob.get_public_key(),
        first_seen=1000.0,
        last_seen=1000.0,
    )
    node = _RecordingNode(peer)
    app.node = node  # type: ignore[assignment]
    result = app.publish(message, ttl=2)
    assert result["delivered"] == 1
    assert result["peers"] == 1
    path, request = node.requests[0]
    assert path == CHANNEL_POST_PATH
    assert request["message"]["hash"] == message["hash"]
    assert request["ttl"] == 2


# -- CLI -----------------------------------------------------------------------


def test_cli_join_send_read_list(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    )
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                "hello from the cli",
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "sent to #general" in out

    assert main(["channel", "read", "--data-dir", str(data_dir), "general"]) == 0
    out = capsys.readouterr().out
    assert "hello from the cli" in out

    assert main(["channel", "list", "--data-dir", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "#general" in out
    assert "1 cached message" in out

    conn = init_db(data_dir / "channels.sqlite3")
    try:
        assert len(channel_history(conn, "general")) == 1
    finally:
        conn.close()


def test_cli_channel_topic_round_trip(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    )

    # No topic yet.
    assert main(["channel", "topic", "--data-dir", str(data_dir), "general"]) == 0
    assert "has no topic set" in capsys.readouterr().out

    assert (
        main(
            [
                "channel",
                "topic",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                "monthly mesh meetup",
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "topic of #general: monthly mesh meetup" in out

    assert main(["channel", "topic", "--data-dir", str(data_dir), "general"]) == 0
    assert "#general: monthly mesh meetup" in capsys.readouterr().out

    assert (
        main(
            [
                "channel",
                "topic",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--clear",
                "general",
            ]
        )
        == 0
    )
    assert "cleared the topic of #general" in capsys.readouterr().out
    assert main(["channel", "topic", "--data-dir", str(data_dir), "general"]) == 0
    assert "has no topic set" in capsys.readouterr().out

    # Setting a topic needs membership, like sending.
    assert (
        main(
            [
                "channel",
                "topic",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "other-room",
                "hello",
            ]
        )
        == 2
    )
    assert "not a member" in capsys.readouterr().err

    # --clear and topic text do not mix.
    assert (
        main(
            [
                "channel",
                "topic",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--clear",
                "general",
                "oops",
            ]
        )
        == 2
    )
    assert "--clear cannot be combined" in capsys.readouterr().err


def test_cli_search_finds_matches_newest_first(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    )
    for text in ("the bridge is up", "bridge repairs tonight", "unrelated"):
        assert (
            main(
                [
                    "channel",
                    "send",
                    "--data-dir",
                    str(data_dir),
                    "--no-publish",
                    "general",
                    text,
                ]
            )
            == 0
        )

    assert main(["channel", "search", "--data-dir", str(data_dir), "general", "bridge"]) == 0
    out = capsys.readouterr().out
    assert "unrelated" not in out
    assert out.index("bridge repairs tonight") < out.index("the bridge is up")

    assert (
        main(
            [
                "channel",
                "search",
                "--json",
                "--data-dir",
                str(data_dir),
                "general",
                "bridge",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["query"] == "bridge"
    assert [item["text"] for item in payload["messages"]] == [
        "bridge repairs tonight",
        "the bridge is up",
    ]

    assert main(["channel", "search", "--data-dir", str(data_dir), "general", "nope"]) == 0
    assert "No messages in #general matching 'nope'." in capsys.readouterr().out

    assert main(["channel", "search", "--data-dir", str(data_dir), "general", "   "]) == 2
    assert "must not be empty" in capsys.readouterr().err


def test_cli_channel_reply_round_trip(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    )
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                "meet at the park",
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "sent to #general" in out

    conn = init_db(data_dir / "channels.sqlite3")
    try:
        parent = channel_history(conn, "general")[0]
    finally:
        conn.close()

    # A unique prefix is enough to name the parent.
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--reply-to",
                parent["hash"][:12],
                "general",
                "see you there",
            ]
        )
        == 0
    )
    assert "sent to #general" in capsys.readouterr().out

    assert main(["channel", "read", "--data-dir", str(data_dir), "general"]) == 0
    out = capsys.readouterr().out
    assert "meet at the park" in out
    assert "see you there" in out
    assert "↩ replying to alice: meet at the park" in out

    # An unknown parent is an error, not a dangling reply.
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--reply-to",
                "f" * 64,
                "general",
                "dangling",
            ]
        )
        == 2
    )
    assert "unknown message" in capsys.readouterr().err


def test_cli_send_without_join_fails(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                "hello",
            ]
        )
        == 2
    )
    assert "not a member" in capsys.readouterr().err


def test_cli_join_rejects_bad_name(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(["channel", "join", "--data-dir", str(data_dir), "has space"]) == 2
    )
    assert "channel name" in capsys.readouterr().err


def test_cli_channel_name_does_not_clobber_node_name(tmp_path, capsys):
    # The channel positional used to share argparse's "name" destination
    # with the shared --name node option, so `tau channel send mesh hi`
    # signed the message as "mesh" and renamed the node.  The channel
    # now has its own destination: --name names the node, the
    # positional names the channel, in either position.
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(
            [
                "channel",
                "join",
                "--data-dir",
                str(data_dir),
                "--name",
                "alice",
                "general",
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--name",
                "alice",
                "--no-publish",
                "general",
                "hello from the cli",
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "channels.sqlite3")
    try:
        messages = channel_history(conn, "general")
    finally:
        conn.close()
    assert len(messages) == 1
    assert messages[0]["name"] == "alice"
    assert messages[0]["channel"] == "general"
    # The node's own name is still alice, not "general".
    capsys.readouterr()
    assert main(["id", "--data-dir", str(data_dir), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["name"] == "alice"


def test_cli_channel_send_without_name_flag_keeps_node_name(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                "hello",
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "channels.sqlite3")
    try:
        messages = channel_history(conn, "general")
    finally:
        conn.close()
    assert messages[0]["name"] == "alice"
    capsys.readouterr()
    assert main(["id", "--data-dir", str(data_dir), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["name"] == "alice"


# -- read cursors / unread counts --------------------------------------------
#
# joined_at is real wall-clock time, so tests that need a deterministic
# join moment insert the membership row directly instead of joining.


def _join_at(conn, channel, joined_at):
    with conn:
        conn.execute(
            "INSERT INTO memberships(channel, joined_at) VALUES(?, ?)",
            (channel, joined_at),
        )


def _store_from(identities, source_conn, identity, channel, text, now):
    return create_message(source_conn, identity, channel, text, name="bob", now=now)


def test_unread_counts_messages_received_since_join(identities, tmp_path):
    _, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    source = init_db(tmp_path / "source.sqlite3")
    _join_at(conn, "general", 1000)
    # Received before joining: cached history, not unread.
    store_message(conn, _store_from(identities, source, bob, "general", "old", 900), now=900)
    store_message(conn, _store_from(identities, source, bob, "general", "one", 1001), now=1001)
    store_message(conn, _store_from(identities, source, bob, "general", "two", 1002), now=1002)
    # Another channel's messages never count here.
    _join_at(conn, "other", 1000)
    store_message(conn, _store_from(identities, source, bob, "other", "elsewhere", 1003), now=1003)

    assert channel_unread_counts(conn) == {"general": 2, "other": 1}


def test_unread_counts_empty_without_messages(tmp_path):
    conn = init_db(tmp_path / "channels.sqlite3")
    _join_at(conn, "general", 1000)
    assert channel_unread_counts(conn) == {}


def test_mark_read_clears_and_new_messages_count_again(identities, tmp_path):
    _, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    source = init_db(tmp_path / "source.sqlite3")
    _join_at(conn, "general", 1000)
    store_message(conn, _store_from(identities, source, bob, "general", "one", 1001), now=1001)
    store_message(conn, _store_from(identities, source, bob, "general", "two", 1002), now=1002)

    marked = mark_channel_read(conn, "general")
    assert marked["cleared"] == 2
    assert marked["hash"] is not None
    assert channel_unread_counts(conn) == {}

    store_message(conn, _store_from(identities, source, bob, "general", "three", 1003), now=1003)
    assert channel_unread_counts(conn) == {"general": 1}
    assert mark_channel_read(conn, "general")["cleared"] == 1
    assert channel_unread_counts(conn) == {}


def test_mark_read_requires_membership(tmp_path):
    conn = init_db(tmp_path / "channels.sqlite3")
    with pytest.raises(ChannelError, match="not a member"):
        mark_channel_read(conn, "general")


def test_mark_read_with_no_messages_leaves_no_cursor(identities, tmp_path):
    _, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    source = init_db(tmp_path / "source.sqlite3")
    _join_at(conn, "general", 1000)

    marked = mark_channel_read(conn, "general")
    assert marked == {
        "channel": "general",
        "hash": None,
        "created_at": None,
        "cleared": 0,
    }
    # No cursor was set, so a message arriving after joining still counts.
    store_message(conn, _store_from(identities, source, bob, "general", "hi", 1001), now=1001)
    assert channel_unread_counts(conn) == {"general": 1}


def test_late_message_before_cursor_does_not_relight(identities, tmp_path):
    _, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    source = init_db(tmp_path / "source.sqlite3")
    _join_at(conn, "general", 1000)
    store_message(conn, _store_from(identities, source, bob, "general", "one", 1001), now=1001)
    store_message(conn, _store_from(identities, source, bob, "general", "two", 1002), now=1002)
    mark_channel_read(conn, "general")

    # A gossip straggler created before the cursor but received after it.
    store_message(conn, _store_from(identities, source, bob, "general", "late", 1001), now=5000)
    assert channel_unread_counts(conn) == {}


def test_leave_drops_cursor_so_rejoin_starts_fresh(identities, tmp_path):
    _, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    source = init_db(tmp_path / "source.sqlite3")
    _join_at(conn, "general", 1000)
    store_message(conn, _store_from(identities, source, bob, "general", "one", 1001), now=1001)
    mark_channel_read(conn, "general")
    assert leave_channel(conn, "general") is True

    _join_at(conn, "general", 5000)
    # The cached message predates the rejoin: not unread again.
    assert channel_unread_counts(conn) == {}


def test_app_unread_counts_excludes_own_and_blocked_messages(identities, tmp_path):
    alice, bob = identities
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)
    app.join("general")
    app.send("general", "my own post", publish=False)

    source = init_db(tmp_path / "source.sqlite3")
    conn = app.connect()
    try:
        # Received now — after the real-time join above — so it counts.
        store_message(
            conn,
            _store_from(identities, source, bob, "general", "hi", 1001),
            now=time.time(),
        )
    finally:
        conn.close()
    assert app.unread_counts() == {"general": 1}

    save_blocklist(config.blocklist_path, BlockList(blocked=frozenset({author_of(bob)})))
    assert app.unread_counts() == {}

    marked = app.mark_read("general")
    assert marked["cleared"] == 0
    assert app.unread_counts() == {}


def test_cli_channel_list_shows_unread_and_mark_read_clears(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    # Own sends never light the badge.
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                "hello from the cli",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert main(["channel", "list", "--data-dir", str(data_dir), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["channels"][0]["unread"] == 0

    # A peer's message, received after joining, lights it.
    bob = RNS.Identity()
    source = init_db(tmp_path / "source.sqlite3")
    conn = init_db(data_dir / "channels.sqlite3")
    try:
        store_message(
            conn,
            create_message(source, bob, "general", "hi all", name="bob", now=1000),
            now=time.time(),
        )
    finally:
        conn.close()
    capsys.readouterr()
    assert main(["channel", "list", "--data-dir", str(data_dir)]) == 0
    out = capsys.readouterr().out
    assert "#general" in out
    assert "1 unread" in out

    assert main(["channel", "mark-read", "--data-dir", str(data_dir), "general"]) == 0
    out = capsys.readouterr().out
    assert "cleared 1 unread message" in out
    capsys.readouterr()
    assert main(["channel", "list", "--data-dir", str(data_dir), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["channels"][0]["unread"] == 0


def test_cli_channel_mark_read_requires_membership(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert main(["channel", "mark-read", "--data-dir", str(data_dir), "general"]) == 2
    assert "not a member" in capsys.readouterr().err


# -- polls -------------------------------------------------------------------

def test_poll_message_signs_verifies_and_round_trips(identities, tmp_path):
    from tau.channel_polls import channel_poll_tally, is_channel_poll

    alice, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    try:
        poll = create_message(
            conn,
            alice,
            "general",
            "which colour?",
            name="alice",
            now=1000,
            poll_options=["Red", "Blue"],
        )
        assert is_channel_poll(poll)
        assert verify_message(poll)["hash"] == poll["hash"]
        assert get_message(conn, poll["hash"])["poll_options"] == ["Red", "Blue"]
        vote = create_message(
            conn,
            bob,
            "general",
            "voted: Blue",
            name="bob",
            now=1001,
            poll_vote={
                "target": {"author": poll["author"], "sent_at": poll["created_at"]},
                "option": 1,
            },
        )
        tally = channel_poll_tally(
            channel_history(conn, "general"), poll, own_hash=author_of(bob)
        )
        assert tally["counts"] == [0, 1]
        assert tally["my_vote"] == 1
        close = create_message(
            conn,
            alice,
            "general",
            "closed the poll",
            name="alice",
            now=1002,
            poll_close={"author": poll["author"], "sent_at": poll["created_at"]},
        )
        assert get_message(conn, close["hash"])["poll_close"] == {
            "author": poll["author"],
            "sent_at": poll["created_at"],
        }
    finally:
        conn.close()


def test_poll_fields_are_covered_by_the_hash(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    try:
        poll = create_message(
            conn,
            alice,
            "general",
            "which colour?",
            name="alice",
            now=1000,
            poll_options=["Red", "Blue"],
        )
        tampered = dict(poll, poll_options=["Red", "Green"])
        with pytest.raises(ChannelError):
            verify_message(tampered)
        stripped = {k: v for k, v in poll.items() if k != "poll_options"}
        with pytest.raises(ChannelError):
            verify_message(stripped)
    finally:
        conn.close()


def test_poll_rejects_bad_options_and_mixed_fields(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    try:
        with pytest.raises(ChannelError, match="poll options"):
            create_message(
                conn, alice, "general", "q?", name="alice", now=1000,
                poll_options=["only one"],
            )
        with pytest.raises(ChannelError, match="only one of"):
            create_message(
                conn,
                alice,
                "general",
                "q?",
                name="alice",
                now=1000,
                poll_options=["A", "B"],
                poll_vote={"target": {"author": "ab" * 16, "sent_at": 1000.0}, "option": 0},
            )
        with pytest.raises(ChannelError, match="poll vote"):
            create_message(
                conn,
                alice,
                "general",
                "voted",
                name="alice",
                now=1000,
                poll_vote={"target": {"author": "ab" * 16, "sent_at": 1000.0}, "option": -1},
            )
        # A plain message still carries no poll fields at all.
        plain = create_message(
            conn, alice, "general", "hello", name="alice", now=1000
        )
        assert "poll_options" not in plain
        assert "poll_vote" not in plain
        assert "poll_close" not in plain
    finally:
        conn.close()


def test_init_db_migrates_a_pre_poll_store(identities, tmp_path):
    import sqlite3

    alice, _ = identities
    path = tmp_path / "channels.sqlite3"
    old = sqlite3.connect(str(path))
    old.executescript(
        """
        CREATE TABLE messages (
            v INTEGER NOT NULL DEFAULT 1,
            hash TEXT PRIMARY KEY,
            channel TEXT NOT NULL,
            author TEXT NOT NULL,
            key TEXT NOT NULL,
            name TEXT NOT NULL DEFAULT '',
            text TEXT NOT NULL,
            created_at REAL NOT NULL,
            received_at REAL NOT NULL,
            sig TEXT NOT NULL
        );
        """
    )
    old.commit()
    old.close()
    conn = init_db(path)
    try:
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(messages)")
        }
        assert {"poll_options", "poll_vote", "poll_close"} <= columns
        poll = create_message(
            conn,
            alice,
            "general",
            "which colour?",
            name="alice",
            now=1000,
            poll_options=["Red", "Blue"],
        )
        assert get_message(conn, poll["hash"])["poll_options"] == ["Red", "Blue"]
    finally:
        conn.close()


# -- pins ----------------------------------------------------------------------


def test_pin_unpin_list(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    first = create_message(conn, alice, "general", "meet at the park", name="alice", now=1000)
    second = create_message(conn, bob, "general", "bring snacks", name="bob", now=1001)
    other = create_message(conn, alice, "other", "not here", name="alice", now=1002)

    pin = pin_message(conn, "general", first["hash"], now=2000)
    assert pin == {"channel": "general", "hash": first["hash"], "pinned_at": 2000}
    pin_message(conn, "general", second["hash"], now=2001)
    assert is_pinned(conn, "general", first["hash"]) is True
    assert is_pinned(conn, "general", second["hash"]) is True

    # Newest pin first, each entry carrying the stored message.
    pins = list_pins(conn, "general")
    assert [entry["hash"] for entry in pins] == [second["hash"], first["hash"]]
    assert pins[0]["text"] == "bring snacks"
    assert pins[0]["pinned_at"] == 2001

    # Pinning twice, pinning a missing digest, and pinning a message
    # stored under another channel are all errors, not silent no-ops.
    with pytest.raises(ChannelError, match="already pinned"):
        pin_message(conn, "general", first["hash"])
    with pytest.raises(ChannelError, match="no stored message"):
        pin_message(conn, "general", "0" * 64)
    with pytest.raises(ChannelError, match="no stored message"):
        pin_message(conn, "general", other["hash"])

    assert unpin_message(conn, "general", first["hash"]) is True
    assert is_pinned(conn, "general", first["hash"]) is False
    assert unpin_message(conn, "general", first["hash"]) is False
    assert [entry["hash"] for entry in list_pins(conn, "general")] == [second["hash"]]


def test_list_pins_hides_blocked_authors(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    mine = create_message(conn, alice, "general", "mine", name="alice", now=1000)
    theirs = create_message(conn, bob, "general", "theirs", name="bob", now=1001)
    pin_message(conn, "general", mine["hash"], now=2000)
    pin_message(conn, "general", theirs["hash"], now=2001)
    pins = list_pins(conn, "general", exclude_authors=[author_of(bob)])
    assert [entry["hash"] for entry in pins] == [mine["hash"]]


def test_pinned_message_survives_pruning(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    join_channel(conn, "general")
    oldest = create_message(
        conn, alice, "general", "msg 0", name="alice", now=1000
    )
    for index in range(1, MAX_STORED_PER_CHANNEL):
        create_message(
            conn, alice, "general", f"msg {index}", name="alice", now=1000 + index
        )
    pin_message(conn, "general", oldest["hash"], now=3000)
    for index in range(MAX_STORED_PER_CHANNEL, MAX_STORED_PER_CHANNEL + 5):
        create_message(
            conn, alice, "general", f"msg {index}", name="alice", now=1000 + index
        )
    # The bound holds for unpinned messages, plus the one pinned extra.
    stored = conn.execute(
        "SELECT COUNT(*) FROM messages WHERE channel = 'general'"
    ).fetchone()[0]
    assert stored == MAX_STORED_PER_CHANNEL + 1
    assert get_message(conn, oldest["hash"]) is not None
    assert is_pinned(conn, "general", oldest["hash"]) is True
    assert list_pins(conn, "general")[0]["text"] == "msg 0"


def test_cli_channel_pin_unpin_pins(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    capsys.readouterr()
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                "general",
                "meet at the park",
            ]
        )
        == 0
    )
    digest = json.loads(capsys.readouterr().out)["message"]["hash"]

    assert (
        main(["channel", "pin", "--data-dir", str(data_dir), "general", digest])
        == 0
    )
    assert "pinned" in capsys.readouterr().out
    # Pinning twice is an error.
    assert (
        main(["channel", "pin", "--data-dir", str(data_dir), "general", digest])
        == 2
    )
    assert "already pinned" in capsys.readouterr().err
    # Pinning a digest that is not stored is an error.
    assert (
        main(
            ["channel", "pin", "--data-dir", str(data_dir), "general", "0" * 64]
        )
        == 2
    )
    assert "no stored message" in capsys.readouterr().err

    assert (
        main(["channel", "pins", "--data-dir", str(data_dir), "general"]) == 0
    )
    assert "meet at the park" in capsys.readouterr().out

    assert (
        main(["channel", "unpin", "--data-dir", str(data_dir), "general", digest])
        == 0
    )
    assert "unpinned" in capsys.readouterr().out
    # Unpinning again is a no-op that says so, not an error.
    assert (
        main(["channel", "unpin", "--data-dir", str(data_dir), "general", digest])
        == 0
    )
    assert "not pinned" in capsys.readouterr().out

    assert (
        main(
            [
                "channel",
                "pins",
                "--data-dir",
                str(data_dir),
                "--json",
                "general",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"channel": "general", "pins": [], "count": 0}


# -- reactions ---------------------------------------------------------------

def _make_reaction_target(identities, tmp_path):
    """A stored message plus a connected app; returns (app, target)."""
    alice, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    message = create_message(conn, bob, "general", "meet at the park", name="bob", now=1000)
    conn.close()
    return _make_app(identities, tmp_path), message


def test_create_reaction_signs_and_verifies(identities, tmp_path):
    alice, bob = identities
    app, message = _make_reaction_target(identities, tmp_path)
    conn = app.connect()
    try:
        record = create_reaction(
            conn, alice, "general", message["hash"], "👍", name="alice", now=1001
        )
    finally:
        conn.close()
    assert record["target"] == message["hash"]
    assert record["reactor"] == author_of(alice)
    assert record["reaction"] == "👍"
    assert verify_reaction(record)["hash"] == record["hash"]

    # The target is part of the signed body: re-pointing breaks the hash.
    with pytest.raises(ChannelError):
        verify_reaction(dict(record, target="0" * 64))
    # So does changing the token.
    with pytest.raises(ChannelError):
        verify_reaction(dict(record, reaction="🎉"))


def test_reaction_target_must_be_stored(identities, tmp_path):
    alice, bob = identities
    app, message = _make_reaction_target(identities, tmp_path)
    conn = app.connect()
    try:
        with pytest.raises(ChannelError, match="no stored message"):
            create_reaction(conn, alice, "general", "0" * 64, "👍", name="alice", now=1001)
        with pytest.raises(ChannelError, match="at least 8 hex"):
            create_reaction(conn, alice, "general", "abc", "👍", name="alice", now=1001)
        with pytest.raises(ChannelError, match="hex prefix"):
            create_reaction(conn, alice, "general", "zz" * 8, "👍", name="alice", now=1001)
        with pytest.raises(ChannelError, match="hex prefix"):
            create_reaction(conn, alice, "general", "a" * 65, "👍", name="alice", now=1001)
        # A unique prefix resolves; a reaction that is too long does not.
        record = create_reaction(
            conn, alice, "general", message["hash"][:12], "👍", name="alice", now=1001
        )
        assert record["target"] == message["hash"]
        with pytest.raises(ChannelError, match="longer than"):
            create_reaction(
                conn, alice, "general", message["hash"], "x" * 100, name="alice", now=1002
            )
    finally:
        conn.close()


def _signed_reaction(identities, tmp_path, token, now, *, subdir):
    """A signed reaction record built in a throwaway db, never stored here.

    ``create_reaction`` stores as it signs, so a record meant for a
    later ``store_reaction`` call is built in its own database: the
    message is identical (same author, text and time), so the target
    hash matches.
    """
    alice, bob = identities
    source = init_db(tmp_path / subdir / "channels.sqlite3")
    message = create_message(source, bob, "general", "meet at the park", name="bob", now=1000)
    record = create_reaction(
        source, alice, "general", message["hash"], token, name="alice", now=now
    )
    source.close()
    return record


def test_reaction_latest_wins_and_clear(identities, tmp_path):
    alice, bob = identities
    app, message = _make_reaction_target(identities, tmp_path)
    conn = app.connect()
    try:
        create_reaction(conn, alice, "general", message["hash"], "👍", name="alice", now=1001)
        create_reaction(conn, bob, "general", message["hash"], "🎉", name="bob", now=1002)
        grouped = reactions_for_messages(conn, "general", [message["hash"]])
        assert len(grouped[message["hash"]]) == 2

        # Alice changes her mind: one row per reactor, latest wins.
        heart = _signed_reaction(identities, tmp_path, "❤️", 1003, subdir="heart")
        record, changed = store_reaction(conn, dict(heart))
        assert changed is True
        grouped = reactions_for_messages(conn, "general", [message["hash"]])
        mine = [item for item in grouped[message["hash"]] if item["reactor"] == author_of(alice)]
        assert [item["reaction"] for item in mine] == ["❤️"]

        # Clearing stores a retraction record, and readers filter it out.
        clear = _signed_reaction(identities, tmp_path, "", 1004, subdir="clear")
        record, changed = store_reaction(conn, dict(clear))
        assert changed is True
        grouped = reactions_for_messages(conn, "general", [message["hash"]])
        assert [item["reactor"] for item in grouped[message["hash"]]] == [author_of(bob)]
        assert summarize_reactions(grouped[message["hash"]]) == [
            {"reaction": "🎉", "count": 1, "reactors": ["bob"]}
        ]
    finally:
        conn.close()


def test_stale_reaction_does_not_overwrite_newer(identities, tmp_path):
    alice, bob = identities
    app, message = _make_reaction_target(identities, tmp_path)
    conn = app.connect()
    try:
        newer = _signed_reaction(identities, tmp_path, "🎉", 1005, subdir="newer")
        older = _signed_reaction(identities, tmp_path, "👍", 1001, subdir="older")
        record, changed = store_reaction(conn, dict(newer))
        assert changed is True
        # A stale re-send of the older reaction arrives late.
        record, changed = store_reaction(conn, dict(older))
        assert changed is False
        assert record["reaction"] == "🎉"
    finally:
        conn.close()


def test_handle_react_stores_and_answers(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "hi all", name="bob", now=1000)
    reaction = create_reaction(
        source, alice, "general", message["hash"], "👍", name="alice", now=1001
    )

    app = _make_app(identities, tmp_path)
    response = app._handle_react(
        CHANNEL_REACT_PATH,
        encode({"reaction": reaction, "ttl": 2}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert payload["ok"] is True
    assert payload["new"] is True
    assert payload["stored"] == reaction["hash"]

    # A second copy is a duplicate, not an error.
    response = app._handle_react(
        CHANNEL_REACT_PATH,
        encode({"reaction": reaction, "ttl": 2}),
        b"id",
        None,
        time.time(),
    )
    assert decode(response)["new"] is False


def test_handle_react_rejects_tampered_and_blocked(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "hi all", name="bob", now=1000)
    reaction = create_reaction(
        source, alice, "general", message["hash"], "👍", name="alice", now=1001
    )

    app = _make_app(identities, tmp_path)
    response = app._handle_react(
        CHANNEL_REACT_PATH,
        encode({"reaction": dict(reaction, reaction="🎉"), "ttl": 2}),
        b"id",
        None,
        time.time(),
    )
    assert decode(response)["ok"] is False

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    save_blocklist(
        config.blocklist_path, BlockList(blocked=frozenset({reaction["reactor"]}))
    )
    app = ChannelApp(config, alice, name="alice", gossip=False)
    response = app._handle_react(
        CHANNEL_REACT_PATH,
        encode({"reaction": reaction, "ttl": 2}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "not_admitted"
    conn = app.connect()
    try:
        assert reactions_for_messages(conn, "general", [message["hash"]]) == {
            message["hash"]: []
        }
    finally:
        conn.close()


def test_handle_feed_carries_reactions(identities, tmp_path):
    alice, bob = identities
    app = _make_app(identities, tmp_path)
    conn = app.connect()
    try:
        message = create_message(conn, bob, "general", "one", name="bob", now=1000)
        create_reaction(conn, alice, "general", message["hash"], "👍", name="alice", now=1001)
        create_reaction(conn, bob, "general", message["hash"], "🎉", name="bob", now=1002)
    finally:
        conn.close()

    response = app._handle_feed(
        CHANNEL_FEED_PATH,
        encode({"channel": "general", "limit": 20}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert payload["ok"] is True
    assert [item["reaction"] for item in payload["reactions"]] == ["👍", "🎉"]

    # A blocked reactor's reactions are not served to peers either —
    # but the message itself (bob's, unblocked) still is.
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    save_blocklist(
        config.blocklist_path, BlockList(blocked=frozenset({author_of(alice)}))
    )
    app = ChannelApp(config, alice, name="alice", gossip=False)
    response = app._handle_feed(
        CHANNEL_FEED_PATH,
        encode({"channel": "general", "limit": 20}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert len(payload["messages"]) == 1
    assert [item["reaction"] for item in payload["reactions"]] == ["🎉"]


def test_sync_pulls_reactions(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "bob one", name="bob", now=1000)
    create_reaction(source, alice, "general", message["hash"], "👍", name="alice", now=1001)

    class _ReactingNode(_FakeNode):
        def request(self, peer, path, payload, *, timeout=20.0):
            request = decode(payload)
            self.requests.append((path, request))
            assert path == CHANNEL_FEED_PATH
            messages, has_more = channel_history_page(
                self.source,
                request["channel"],
                limit=request.get("limit", 20),
                before=request.get("before"),
                before_hash=request.get("before_hash"),
            )
            grouped = reactions_for_messages(
                self.source,
                request["channel"],
                [item["hash"] for item in messages],
            )
            reactions = [
                record for item in messages for record in grouped.get(item["hash"], [])
            ]
            return {
                "ok": True,
                "messages": messages,
                "reactions": reactions,
                "has_more": has_more,
            }

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)
    peer = Peer(
        destination_hash=bytes.fromhex(message["author"]),
        name="bob",
        public_key=bob.get_public_key(),
        first_seen=1000.0,
        last_seen=1000.0,
    )
    node = _ReactingNode(peer, source)
    app.node = node  # type: ignore[assignment]

    summary = app.sync(node, "general", limit=20)
    assert summary["stored"] == 1
    assert summary["reactions_stored"] == 1
    assert summary["errors"] == []
    conn = app.connect()
    try:
        grouped = reactions_for_messages(conn, "general", [message["hash"]])
    finally:
        conn.close()
    assert [item["reaction"] for item in grouped[message["hash"]]] == ["👍"]


def test_prune_drops_reactions_with_messages(identities, tmp_path, monkeypatch):
    from tau import channels as channels_mod

    alice, bob = identities
    monkeypatch.setattr(channels_mod, "MAX_STORED_PER_CHANNEL", 1)
    conn = init_db(tmp_path / "channels.sqlite3")
    join_channel(conn, "general")
    old = create_message(conn, bob, "general", "old", name="bob", now=1000)
    create_reaction(conn, alice, "general", old["hash"], "👍", name="alice", now=1001)
    create_message(conn, bob, "general", "new", name="bob", now=1002)
    assert reactions_for_messages(conn, "general", [old["hash"]]) == {old["hash"]: []}
    assert channel_history(conn, "general")[-1]["text"] == "new"


def test_cli_channel_react_round_trip(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                "hello from the cli",
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "channels.sqlite3")
    try:
        target = channel_history(conn, "general")[0]["hash"]
    finally:
        conn.close()

    assert (
        main(
            [
                "channel",
                "react",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                target[:12],
                "👍",
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "reacted 👍" in out

    assert main(["channel", "read", "--data-dir", str(data_dir), "general"]) == 0
    out = capsys.readouterr().out
    assert "hello from the cli" in out
    assert "👍×1" in out

    # JSON read carries the reactions with the message.
    assert (
        main(["channel", "read", "--data-dir", str(data_dir), "--json", "general"])
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["messages"][0]["reactions"][0]["reaction"] == "👍"

    # Clearing removes the count.
    assert (
        main(
            [
                "channel",
                "react",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--clear",
                "general",
                target,
            ]
        )
        == 0
    )
    assert "retracted your reaction" in capsys.readouterr().out
    assert main(["channel", "read", "--data-dir", str(data_dir), "general"]) == 0
    assert "👍" not in capsys.readouterr().out

    # An unknown target is a usage error, not a silent store.
    assert (
        main(
            [
                "channel",
                "react",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                "0" * 64,
                "👍",
            ]
        )
        == 2
    )
    assert "no stored message" in capsys.readouterr().err


# -- channel edits --------------------------------------------------------------


def _edit_fixture(identities, tmp_path, *, text="original text", now=1000):
    """A stored message from bob plus the app that owns the store."""
    alice, bob = identities
    conn = init_db(tmp_path / "edits.sqlite3")
    message = create_message(conn, bob, "general", text, name="bob", now=now)
    return conn, message, alice, bob


def test_create_edit_signs_verifies_and_applies(identities, tmp_path):
    alice, bob = identities
    conn, message, _, _ = _edit_fixture(identities, tmp_path)
    try:
        notice = create_edit(conn, bob, "general", message["hash"], "fixed text", now=1001)
        assert notice["kind"] == "channel-edit"
        assert notice["target"] == message["hash"]
        assert notice["text"] == "fixed text"
        assert notice["author"] == message["author"]
        assert verify_edit(notice)["hash"] == notice["hash"]

        shown = channel_history(conn, "general")
        assert [item["text"] for item in shown] == ["fixed text"]
        assert shown[0].get("edited") is True
        assert shown[0]["edited_at"] == 1001
        # The stored row is untouched: its hash still verifies.
        assert get_message(conn, message["hash"])["text"] == "original text"
        assert current_message_text(conn, message) == ("fixed text", 1001)
    finally:
        conn.close()


def test_create_edit_rejects_other_authors_message(identities, tmp_path):
    alice, bob = identities
    conn, message, _, _ = _edit_fixture(identities, tmp_path)
    try:
        with pytest.raises(ChannelError, match="only the message author"):
            create_edit(conn, alice, "general", message["hash"], "hijack", now=1001)
        assert channel_history(conn, "general")[0]["text"] == "original text"
    finally:
        conn.close()


def test_create_edit_rejects_unknown_digest(identities, tmp_path):
    alice, bob = identities
    conn, message, _, _ = _edit_fixture(identities, tmp_path)
    try:
        with pytest.raises(ChannelError, match="unknown message"):
            create_edit(conn, bob, "general", "00" * 32, "nope", now=1001)
    finally:
        conn.close()


def test_create_edit_same_text_is_idempotent(identities, tmp_path):
    alice, bob = identities
    conn, message, _, _ = _edit_fixture(identities, tmp_path)
    try:
        first = create_edit(conn, bob, "general", message["hash"], "fixed text", now=1001)
        second = create_edit(conn, bob, "general", message["hash"], "fixed text", now=1002)
        assert second["hash"] == first["hash"]
        assert len(list_edits(conn, "general")) == 1
        # Editing to the text the message already shows, with no
        # notice on file, is an error — there is nothing to change.
        other = create_message(conn, bob, "general", "untouched", name="bob", now=1005)
        with pytest.raises(ChannelError, match="already has that text"):
            create_edit(conn, bob, "general", other["hash"], "untouched", now=1006)
    finally:
        conn.close()


def test_create_edit_resolves_prefix(identities, tmp_path):
    alice, bob = identities
    conn, message, _, _ = _edit_fixture(identities, tmp_path)
    try:
        notice = create_edit(conn, bob, "general", message["hash"][:8], "prefixed", now=1001)
        assert notice["target"] == message["hash"]
    finally:
        conn.close()


def test_store_edit_rejects_tampered_notice(identities, tmp_path):
    alice, bob = identities
    conn, message, _, _ = _edit_fixture(identities, tmp_path)
    try:
        notice = create_edit(conn, bob, "general", message["hash"], "fixed text", now=1001)
        with pytest.raises(ChannelError):
            store_edit(conn, dict(notice, text="forged"))
        with pytest.raises(ChannelError):
            store_edit(conn, dict(notice, sig="00" * 64))
        with pytest.raises(ChannelError):
            store_edit(conn, dict(notice, kind="channel-retract"))
        with pytest.raises(ChannelError):
            store_edit(conn, dict(notice, hash="00" * 32))
        # The good notice still stands: one stored edit, text applied.
        assert len(list_edits(conn, "general")) == 1
        assert channel_history(conn, "general")[0]["text"] == "fixed text"
    finally:
        conn.close()


def test_forged_edit_is_stored_but_never_applies(identities, tmp_path):
    # Alice validly signs an edit for bob's message.  The notice is
    # kept (and listed) but the reader never sees her text.
    alice, bob = identities
    conn, message, _, _ = _edit_fixture(identities, tmp_path)
    try:
        # Build the notice by hand: create_edit refuses non-authors,
        # so sign alice's notice directly and store it.  created_at
        # is a float: the canonical body hashes 1001.0, not 1001.
        import hashlib as _hashlib
        import json as _json

        body = {
            "v": 1,
            "kind": "channel-edit",
            "channel": "general",
            "target": message["hash"],
            "text": "hijacked",
            "author": author_of(alice),
            "key": alice.get_public_key().hex(),
            "created_at": 1001.0,
        }
        digest = _hashlib.sha256(
            _json.dumps(
                body, ensure_ascii=False, separators=(",", ":"), sort_keys=True
            ).encode("utf-8")
        ).hexdigest()
        notice = dict(body, hash=digest, sig=alice.sign(bytes.fromhex(digest)).hex())
        stored, is_new = store_edit(conn, notice)
        assert is_new is True
        assert get_edit(conn, digest) is not None
        assert [item["text"] for item in channel_history(conn, "general")] == [
            "original text"
        ]
        assert "edited" not in channel_history(conn, "general")[0]
        # The author's own later edit still wins over the forgery.
        create_edit(conn, bob, "general", message["hash"], "author fixed", now=1002)
        assert channel_history(conn, "general")[0]["text"] == "author fixed"
    finally:
        conn.close()


def test_pending_edit_applies_when_message_arrives(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "pending.sqlite3")
    try:
        # The edit arrives before its message: stored, not applied.
        source = init_db(tmp_path / "source.sqlite3")
        message = create_message(source, bob, "general", "original", name="bob", now=1000)
        notice = create_edit(source, bob, "general", message["hash"], "fixed", now=1001)
        source.close()
        stored, is_new = store_edit(conn, notice)
        assert is_new is True
        assert channel_history(conn, "general") == []
        # Now the message lands — the pending edit applies on read.
        store_message(conn, message)
        shown = channel_history(conn, "general")
        assert [item["text"] for item in shown] == ["fixed"]
        assert shown[0].get("edited") is True
    finally:
        conn.close()


def test_resolve_message_digest_rejects_bad_prefixes(identities, tmp_path):
    alice, bob = identities
    conn, message, _, _ = _edit_fixture(identities, tmp_path)
    try:
        assert resolve_message_digest(conn, "general", message["hash"]) == message["hash"]
        with pytest.raises(ChannelError, match="at least 4 hex"):
            resolve_message_digest(conn, "general", "abc")
        with pytest.raises(ChannelError, match="at least 4 hex"):
            resolve_message_digest(conn, "general", "zzzz")
        with pytest.raises(ChannelError, match="unknown message"):
            resolve_message_digest(conn, "general", "00" * 32)
        # Two messages sharing a 4-hex prefix are ambiguous.
        conn.execute(
            "INSERT INTO messages(v, hash, channel, author, key, name, text, "
            "created_at, received_at, sig) VALUES(1, ?, 'general', ?, ?, '', "
            "'first', 1000, 1000, ?)",
            ("abcd" + "00" * 30, message["author"], message["key"], "00" * 64),
        )
        conn.execute(
            "INSERT INTO messages(v, hash, channel, author, key, name, text, "
            "created_at, received_at, sig) VALUES(1, ?, 'general', ?, ?, '', "
            "'second', 1000, 1000, ?)",
            ("abcd" + "11" * 30, message["author"], message["key"], "00" * 64),
        )
        conn.commit()
        with pytest.raises(ChannelError, match="ambiguous"):
            resolve_message_digest(conn, "general", "abcd")
        assert resolve_message_digest(conn, "general", "abcd00") == "abcd" + "00" * 30
    finally:
        conn.close()


def test_handle_post_accepts_edit(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "hi all", name="bob", now=1000)
    notice = create_edit(source, bob, "general", message["hash"], "hi all!", now=1001)

    app = _make_app(identities, tmp_path)
    # The message itself must be stored first (or the edit stays pending).
    app._handle_post(
        CHANNEL_POST_PATH, encode({"message": message, "ttl": 0}), b"id", None, time.time()
    )
    response = app._handle_post(
        CHANNEL_POST_PATH, encode({"edit": notice, "ttl": 0}), b"id", None, time.time()
    )
    payload = decode(response)
    assert payload["ok"] is True
    assert payload["new"] is True
    assert payload["stored"] == notice["hash"]
    assert app.history("general")[0]["text"] == "hi all!"

    # A second copy is a duplicate, not an error.
    response = app._handle_post(
        CHANNEL_POST_PATH, encode({"edit": notice, "ttl": 0}), b"id", None, time.time()
    )
    assert decode(response)["new"] is False


def test_handle_post_rejects_tampered_edit_and_mixed_post(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "hi all", name="bob", now=1000)
    notice = create_edit(source, bob, "general", message["hash"], "hi all!", now=1001)

    app = _make_app(identities, tmp_path)
    response = app._handle_post(
        CHANNEL_POST_PATH,
        encode({"edit": dict(notice, text="forged"), "ttl": 0}),
        b"id",
        None,
        time.time(),
    )
    assert decode(response)["ok"] is False

    response = app._handle_post(
        CHANNEL_POST_PATH,
        encode({"message": message, "edit": notice, "ttl": 0}),
        b"id",
        None,
        time.time(),
    )
    assert decode(response)["ok"] is False


def test_handle_post_refuses_blocked_edit_author(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "hi all", name="bob", now=1000)
    notice = create_edit(source, bob, "general", message["hash"], "hi all!", now=1001)

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    save_blocklist(config.blocklist_path, BlockList(blocked=frozenset({message["author"]})))
    app = ChannelApp(config, alice, name="alice", gossip=False)
    response = app._handle_post(
        CHANNEL_POST_PATH, encode({"edit": notice, "ttl": 0}), b"id", None, time.time()
    )
    payload = decode(response)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "not_admitted"
    assert app.edits("general") == []


def test_handle_feed_serves_originals_plus_edits(identities, tmp_path):
    alice, bob = identities
    app = _make_app(identities, tmp_path)
    conn = app.connect()
    try:
        message = create_message(conn, bob, "general", "one", name="bob", now=1000)
        create_edit(conn, bob, "general", message["hash"], "one, fixed", now=1001)
    finally:
        conn.close()

    response = app._handle_feed(
        CHANNEL_FEED_PATH,
        encode({"channel": "general", "limit": 20}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert payload["ok"] is True
    # The wire carries the original, hash-valid text...
    assert [item["text"] for item in payload["messages"]] == ["one"]
    assert "edited" not in payload["messages"][0]
    # ...plus the edit notice, so the requester can overlay it.
    assert [item["text"] for item in payload["edits"]] == ["one, fixed"]
    assert payload["edits_count"] == 1


class _FakeNodeWithEdits(_FakeNode):
    def request(self, peer, path, payload, *, timeout=20.0):
        request = decode(payload)
        self.requests.append((path, request))
        assert path == CHANNEL_FEED_PATH
        messages, has_more = channel_history_page(
            self.source,
            request["channel"],
            limit=request.get("limit", 20),
            before=request.get("before"),
            before_hash=request.get("before_hash"),
            apply_edits=False,
        )
        edits = list_edits(self.source, request["channel"])
        return {"ok": True, "messages": messages, "edits": edits, "has_more": has_more}


def test_sync_applies_edit_from_feed(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "bob one", name="bob", now=1000)
    create_edit(source, bob, "general", message["hash"], "bob one, fixed", now=1001)

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)
    peer = Peer(
        destination_hash=bytes.fromhex(message["author"]),
        name="bob",
        public_key=bob.get_public_key(),
        first_seen=1000.0,
        last_seen=1000.0,
    )
    node = _FakeNodeWithEdits(peer, source)
    app.node = node  # type: ignore[assignment]

    summary = app.sync(node, "general", limit=20)
    assert summary["stored"] == 1
    assert summary["edited"] == 1
    assert summary["errors"] == []
    shown = app.history("general")
    assert [item["text"] for item in shown] == ["bob one, fixed"]
    assert shown[0].get("edited") is True

    # A second sync is a duplicate on both counts.
    summary = app.sync(node, "general", limit=20)
    assert summary["stored"] == 0
    assert summary["edited"] == 0


def test_sync_survives_bad_peer_edit_entries(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "good", name="bob", now=1000)
    good = create_edit(source, bob, "general", message["hash"], "good, fixed", now=1001)
    bad_text = dict(good, hash="00" * 32, text=123)

    class _MessyEditNode(_FakeNode):
        def request(self, peer, path, payload, *, timeout=20.0):
            return {
                "ok": True,
                "messages": [message],
                "edits": ["junk", bad_text, good],
                "has_more": False,
            }

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)
    peer = Peer(
        destination_hash=bytes.fromhex(author_of(bob)),
        name="bob",
        public_key=bob.get_public_key(),
        first_seen=1000.0,
        last_seen=1000.0,
    )
    node = _MessyEditNode(peer, source)
    app.node = node  # type: ignore[assignment]

    summary = app.sync(node, "general", limit=20)
    assert summary["stored"] == 1
    assert summary["edited"] == 1
    assert len(summary["errors"]) == 2
    assert app.history("general")[0]["text"] == "good, fixed"


def test_prune_drops_orphaned_edits(identities, tmp_path):
    from tau.channels import _prune_store

    alice, bob = identities
    conn, message, _, _ = _edit_fixture(identities, tmp_path)
    try:
        create_edit(conn, bob, "general", message["hash"], "fixed", now=1001)
        assert len(list_edits(conn, "general")) == 1
        # The message is gone (retention evicted it); pruning must
        # drop the edit that points at nothing.
        conn.execute("DELETE FROM messages WHERE hash = ?", (message["hash"],))
        conn.commit()
        _prune_store(conn, "general")
        assert list_edits(conn, "general") == []
    finally:
        conn.close()


def test_app_edit_requires_membership(identities, tmp_path):
    alice, bob = identities
    conn, message, _, _ = _edit_fixture(identities, tmp_path)
    conn.close()
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)
    with pytest.raises(ChannelError, match="not a member"):
        app.edit("general", message["hash"], "nope", publish=False)


def test_cli_channel_edit_round_trip(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                "hello with a typo",
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "channels.sqlite3")
    try:
        digest = channel_history(conn, "general")[0]["hash"]
    finally:
        conn.close()

    assert (
        main(
            [
                "channel",
                "edit",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                digest[:8],
                "hello, fixed",
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "edited #general" in out

    assert main(["channel", "read", "--data-dir", str(data_dir), "general"]) == 0
    out = capsys.readouterr().out
    assert "hello, fixed" in out
    assert "(edited)" in out
    assert "hello with a typo" not in out

    assert main(["channel", "edits", "--data-dir", str(data_dir), "general"]) == 0
    out = capsys.readouterr().out
    assert "hello, fixed" in out

    # --json edit shape: the notice plus the publish outcome.
    assert (
        main(
            [
                "channel",
                "edit",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                "general",
                digest,
                "hello, fixed again",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["edit"]["target"] == digest
    assert payload["edit"]["text"] == "hello, fixed again"
    assert payload["published"] is None


def test_cli_channel_edit_rejects_unknown_digest(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    assert (
        main(
            [
                "channel",
                "edit",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                "00" * 32,
                "nope",
            ]
        )
        == 2
    )
    assert "unknown message" in capsys.readouterr().err


# -- retract notices -----------------------------------------------------------


def test_create_retraction_signs_and_deletes(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    message = create_message(
        conn, alice, "general", "take this back", name="alice", now=1000
    )
    notice = create_retraction(conn, alice, "general", message["hash"], now=1001)
    assert notice["kind"] == "channel-retract"
    assert notice["hash"] == message["hash"]
    assert notice["author"] == author_of(alice)
    assert verify_retraction(notice)["hash"] == message["hash"]
    assert get_message(conn, message["hash"]) is None
    assert channel_history(conn, "general") == []
    assert get_retraction(conn, message["hash"], author_of(alice)) is not None
    assert list_retractions(conn, "general")[0]["hash"] == message["hash"]


def test_create_retraction_rejects_others_message(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    message = create_message(conn, bob, "general", "not yours", name="bob", now=1000)
    with pytest.raises(ChannelError):
        create_retraction(conn, alice, "general", message["hash"])
    assert get_message(conn, message["hash"]) is not None


def test_create_retraction_rejects_unknown_digest(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    with pytest.raises(ChannelError):
        create_retraction(conn, alice, "general", "00" * 32)


def test_create_retraction_is_idempotent(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    message = create_message(conn, alice, "general", "twice", name="alice", now=1000)
    first = create_retraction(conn, alice, "general", message["hash"], now=1001)
    second = create_retraction(conn, alice, "general", message["hash"], now=1002)
    assert first["hash"] == second["hash"]
    assert len(list_retractions(conn, "general")) == 1


def test_verify_retraction_rejects_tampered_notice(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    message = create_message(conn, alice, "general", "hello", name="alice", now=1000)
    notice = create_retraction(conn, alice, "general", message["hash"], now=1001)
    with pytest.raises(ChannelError):
        verify_retraction(dict(notice, sig="00" * 64))
    with pytest.raises(ChannelError):
        verify_retraction(dict(notice, hash="11" * 32))
    with pytest.raises(ChannelError):
        verify_retraction(dict(notice, kind="channel-message"))


def test_store_retraction_applies_to_held_message(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "hi", name="bob", now=1000)
    notice = create_retraction(source, bob, "general", message["hash"], now=1001)
    assert get_message(source, message["hash"]) is None

    target = init_db(tmp_path / "target.sqlite3")
    _, is_new = store_message(target, message)
    assert is_new
    _, is_new, applied = store_retraction(target, notice)
    assert is_new and applied
    assert get_message(target, message["hash"]) is None
    assert channel_history(target, "general") == []


def test_store_retraction_from_non_author_does_not_delete(identities, tmp_path):
    # A notice signed by alice naming bob's message: the signature is
    # valid (alice signed it as herself), so it stores — but it must
    # never delete bob's message, or anyone could censor anyone.
    alice, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    message = create_message(conn, bob, "general", "mine", name="bob", now=1000)
    body = {
        "v": 1,
        "kind": "channel-retract",
        "channel": "general",
        "hash": message["hash"],
        "author": author_of(alice),
        "key": alice.get_public_key().hex(),
        "created_at": 1001.0,
    }
    forged = dict(body)
    forged["sig"] = alice.sign(bytes.fromhex(hash_retraction(body))).hex()
    _, is_new, applied = store_retraction(conn, forged)
    assert is_new and not applied
    assert get_message(conn, message["hash"]) is not None
    assert [item["text"] for item in channel_history(conn, "general")] == ["mine"]


def test_pending_retraction_blocks_later_message(identities, tmp_path):
    # The tombstone arrives before the message does: the message is
    # refused when it finally shows up from another peer.
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "late", name="bob", now=1000)
    notice = create_retraction(source, bob, "general", message["hash"], now=1001)

    target = init_db(tmp_path / "target.sqlite3")
    _, is_new, applied = store_retraction(target, notice)
    assert is_new and not applied
    _, was_new = store_message(target, message)
    assert not was_new
    assert channel_history(target, "general") == []


def test_history_page_excludes_retracted_message(identities, tmp_path):
    alice, bob = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    create_message(conn, bob, "general", "keep me", name="bob", now=1000)
    drop = create_message(conn, bob, "general", "drop me", name="bob", now=1001)
    create_retraction(conn, bob, "general", drop["hash"], now=1002)
    page, has_more = channel_history_page(conn, "general", limit=20)
    assert [item["text"] for item in page] == ["keep me"]
    assert has_more is False


def test_resolve_message_digest_prefix(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    message = create_message(conn, alice, "general", "hello", name="alice", now=1000)
    assert (
        resolve_message_digest(conn, "general", message["hash"][:8])
        == message["hash"]
    )
    assert resolve_message_digest(conn, "general", message["hash"]) == message["hash"]
    with pytest.raises(ChannelError):
        resolve_message_digest(conn, "general", "abc")
    with pytest.raises(ChannelError):
        resolve_message_digest(conn, "general", "zzzz")
    with pytest.raises(ChannelError):
        resolve_message_digest(conn, "general", "00" * 32)


def test_resolve_message_digest_rejects_ambiguous_prefix(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    # Two rows sharing a 4-hex prefix: the resolver must refuse to guess.
    conn.execute(
        "INSERT INTO messages(v, hash, channel, author, key, name, text, "
        "created_at, received_at, sig) VALUES(1, ?, 'general', ?, ?, '', "
        "'one', 1000, 1000, ?)",
        ("abcd" + "00" * 30, author_of(alice), alice.get_public_key().hex(), "00" * 64),
    )
    conn.execute(
        "INSERT INTO messages(v, hash, channel, author, key, name, text, "
        "created_at, received_at, sig) VALUES(1, ?, 'general', ?, ?, '', "
        "'two', 1001, 1001, ?)",
        ("abcd" + "11" * 30, author_of(alice), alice.get_public_key().hex(), "00" * 64),
    )
    conn.commit()
    with pytest.raises(ChannelError):
        resolve_message_digest(conn, "general", "abcd")
    assert resolve_message_digest(conn, "general", "abcd00") == "abcd" + "00" * 30


def test_handle_feed_includes_retractions_and_excludes_message(
    identities, tmp_path
):
    alice, bob = identities
    app = _make_app(identities, tmp_path)
    conn = app.connect()
    try:
        create_message(conn, bob, "general", "keep", name="bob", now=1000)
        drop = create_message(conn, bob, "general", "drop", name="bob", now=1001)
        create_retraction(conn, bob, "general", drop["hash"], now=1002)
    finally:
        conn.close()

    response = app._handle_feed(
        CHANNEL_FEED_PATH,
        encode({"channel": "general", "limit": 20}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert payload["ok"] is True
    assert [item["text"] for item in payload["messages"]] == ["keep"]
    assert len(payload["retractions"]) == 1
    assert payload["retractions"][0]["hash"] == drop["hash"]


def test_handle_post_accepts_retraction(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "hi all", name="bob", now=1000)
    notice = create_retraction(source, bob, "general", message["hash"], now=1001)

    app = _make_app(identities, tmp_path)
    conn = app.connect()
    try:
        store_message(conn, message)
    finally:
        conn.close()

    response = app._handle_post(
        CHANNEL_POST_PATH,
        encode({"retraction": notice, "ttl": 0}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert payload["ok"] is True
    assert payload["retraction"] is True
    assert payload["applied"] is True
    conn = app.connect()
    try:
        assert get_message(conn, message["hash"]) is None
    finally:
        conn.close()


def test_handle_post_rejects_tampered_retraction(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "hi all", name="bob", now=1000)
    notice = create_retraction(source, bob, "general", message["hash"], now=1001)

    app = _make_app(identities, tmp_path)
    response = app._handle_post(
        CHANNEL_POST_PATH,
        encode({"retraction": dict(notice, sig="00" * 64), "ttl": 0}),
        b"id",
        None,
        time.time(),
    )
    payload = decode(response)
    assert payload["ok"] is False


def test_handle_post_rejects_message_and_retraction_together(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    message = create_message(source, bob, "general", "hi all", name="bob", now=1000)
    notice = create_retraction(source, bob, "general", message["hash"], now=1001)

    app = _make_app(identities, tmp_path)
    response = app._handle_post(
        CHANNEL_POST_PATH,
        encode({"message": message, "retraction": notice, "ttl": 0}),
        b"id",
        None,
        time.time(),
    )
    assert decode(response)["ok"] is False


def test_sync_applies_retractions_from_feed(identities, tmp_path):
    alice, bob = identities
    bob_config = TauConfig.resolve(data_dir=tmp_path / "bob", name="bob")
    source = ChannelApp(bob_config, bob, name="bob", gossip=False)
    source.join("general")
    source.send("general", "keep me", publish=False)
    drop = source.send("general", "drop me", publish=False)

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)
    peer = Peer(
        destination_hash=bytes.fromhex(author_of(bob)),
        name="bob",
        public_key=bob.get_public_key(),
        first_seen=1000.0,
        last_seen=1000.0,
    )

    class _FeedNode(_FakeNode):
        def request(self, peer, path, payload, *, timeout=20.0):
            assert path == CHANNEL_FEED_PATH
            return decode(
                source._handle_feed(path, payload, b"id", None, time.time())
            )

    node = _FeedNode(peer, source)
    app.node = node  # type: ignore[assignment]

    summary = app.sync(node, "general", limit=20)
    assert summary["stored"] == 2
    assert summary["retracted"] == 0
    assert [item["text"] for item in app.history("general")] == [
        "drop me",
        "keep me",
    ]

    source.retract("general", drop["hash"])
    summary = app.sync(node, "general", limit=20)
    assert summary["retracted"] == 1
    assert summary["errors"] == []
    assert [item["text"] for item in app.history("general")] == ["keep me"]


def test_cli_channel_retract_and_retractions(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                "hello",
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                "take back",
            ]
        )
        == 0
    )

    assert main(["channel", "read", "--data-dir", str(data_dir), "general"]) == 0
    out = capsys.readouterr().out
    assert "hello" in out and "take back" in out

    conn = init_db(data_dir / "channels.sqlite3")
    try:
        messages = channel_history(conn, "general")
    finally:
        conn.close()
    target = [item for item in messages if item["text"] == "take back"][0]
    assert target["hash"][:8] in out

    assert (
        main(
            [
                "channel",
                "retract",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                target["hash"][:8],
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "retracted" in out

    assert main(["channel", "read", "--data-dir", str(data_dir), "general"]) == 0
    out = capsys.readouterr().out
    assert "take back" not in out
    assert "hello" in out

    assert (
        main(["channel", "retractions", "--data-dir", str(data_dir), "general"]) == 0
    )
    out = capsys.readouterr().out
    assert target["hash"][:8] in out


def test_cli_channel_retract_json(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                "hello",
            ]
        )
        == 0
    )
    conn = init_db(data_dir / "channels.sqlite3")
    try:
        digest = channel_history(conn, "general")[0]["hash"]
    finally:
        conn.close()
    capsys.readouterr()
    assert (
        main(
            [
                "channel",
                "retract",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                digest,
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["retraction"]["hash"] == digest
    assert payload["retraction"]["kind"] == "channel-retract"
    assert payload["published"] is None


def test_cli_channel_retract_rejects_unknown_and_others(tmp_path, capsys):
    alice = RNS.Identity()
    bob = RNS.Identity()
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                "hello",
            ]
        )
        == 0
    )
    # Unknown digest.
    assert (
        main(
            [
                "channel",
                "retract",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                "00" * 32,
            ]
        )
        == 2
    )
    # Someone else's message, stored directly in this node's database.
    conn = init_db(data_dir / "channels.sqlite3")
    try:
        other = create_message(conn, bob, "general", "not yours", name="bob", now=1000)
    finally:
        conn.close()
    assert (
        main(
            [
                "channel",
                "retract",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "general",
                other["hash"][:8],
            ]
        )
        == 2
    )
    conn = init_db(data_dir / "channels.sqlite3")
    try:
        assert get_message(conn, other["hash"]) is not None
    finally:
        conn.close()

# -- disappearing channel messages -------------------------------------------


def test_expiry_is_unsigned_and_leaves_the_hash_unchanged(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    now = time.time()
    plain = create_message(conn, alice, "general", "plain hello", name="alice", now=now)
    expiring = create_message(
        conn,
        alice,
        "general",
        "ephemeral hello",
        name="alice",
        now=now,
        expires_at=now + 3600,
    )

    # The expiry never enters the signed body: an old node verifies an
    # expiring message exactly like a plain one, and the hash is the
    # same with the expiry stripped off.
    assert expiring["hash"] == hash_message(
        {key: value for key, value in expiring.items() if key != "expires_at"}
    )
    assert verify_message(expiring)["expires_at"] == now + 3600
    assert verify_message(plain).get("expires_at") is None
    assert "expires_at" not in plain
    assert expiring["expires_at"] == now + 3600


def test_normalize_rejects_bad_expiry(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    now = time.time()
    message = create_message(conn, alice, "general", "hello", name="alice", now=now)
    created_at = message["created_at"]
    for bad in (
        created_at - 1,
        created_at,
        created_at + 31 * 24 * 3600,
        "soon",
        True,
        float("inf"),
    ):
        with pytest.raises(ChannelError):
            verify_message(dict(message, expires_at=bad))


def test_create_message_rejects_a_past_expiry(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    now = time.time()
    with pytest.raises(ChannelError, match="in the future"):
        create_message(
            conn, alice, "general", "hello", name="alice", now=now, expires_at=now - 1
        )


def test_store_drops_an_already_expired_message(identities, tmp_path):
    alice, bob = identities
    source = init_db(tmp_path / "source.sqlite3")
    now = time.time()
    # Created two hours ago, expired one hour ago: valid when signed,
    # but already dead on arrival.
    message = create_message(
        source, bob, "general", "too late", name="bob", now=now - 7200,
        expires_at=now - 3600,
    )
    conn = init_db(tmp_path / "channels.sqlite3")
    stored, is_new = store_message(conn, message, now=now)
    assert is_new is False
    assert get_message(conn, message["hash"]) is None
    assert channel_history(conn, "general") == []


def test_history_hides_expired_messages_but_keeps_live_ones(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    now = time.time()
    create_message(
        conn, alice, "general", "gone by now", name="alice",
        now=now - 7200, expires_at=now - 3600,
    )
    live = create_message(
        conn, alice, "general", "still here", name="alice",
        now=now - 60, expires_at=now + 3600,
    )
    assert [item["text"] for item in channel_history(conn, "general")] == [
        "still here"
    ]
    assert channel_history(conn, "general")[0]["expires_at"] == live["expires_at"]


def test_store_purges_expired_rows(identities, tmp_path):
    alice, _ = identities
    conn = init_db(tmp_path / "channels.sqlite3")
    base = time.time()
    short = create_message(
        conn, alice, "general", "short lived", name="alice",
        now=base, expires_at=base + 60,
    )
    assert get_message(conn, short["hash"]) is not None
    # A later store purges rows whose expiry has passed by then.
    create_message(
        conn, alice, "general", "next", name="alice",
        now=base + 3600, expires_at=base + 7200,
    )
    assert get_message(conn, short["hash"]) is None
    assert [item["text"] for item in channel_history(conn, "general")] == ["next"]


def test_init_db_migrates_a_store_without_expires_at(identities, tmp_path):
    import sqlite3

    alice, _ = identities
    path = tmp_path / "channels.sqlite3"
    # A store as written before expiring messages: no expires_at column.
    raw = sqlite3.connect(str(path))
    raw.executescript(
        "CREATE TABLE messages ("
        "v INTEGER NOT NULL DEFAULT 1, hash TEXT PRIMARY KEY, "
        "channel TEXT NOT NULL, author TEXT NOT NULL, key TEXT NOT NULL, "
        "name TEXT NOT NULL DEFAULT '', text TEXT NOT NULL, "
        "created_at REAL NOT NULL, received_at REAL NOT NULL, sig TEXT NOT NULL);"
        "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
        "CREATE TABLE retractions (hash TEXT NOT NULL, channel TEXT NOT NULL, "
        "author TEXT NOT NULL, key TEXT NOT NULL, created_at REAL NOT NULL, "
        "received_at REAL NOT NULL, sig TEXT NOT NULL, PRIMARY KEY (hash, author));"
        "CREATE TABLE memberships (channel TEXT PRIMARY KEY, joined_at REAL NOT NULL);"
    )
    raw.commit()
    raw.close()

    conn = init_db(path)
    try:
        columns = [
            row[1]
            for row in conn.execute("PRAGMA table_info(messages)").fetchall()
        ]
        assert "expires_at" in columns
        message = create_message(
            conn, alice, "general", "hello", name="alice", expires_at=time.time() + 60
        )
        assert message["expires_at"] is not None
        assert get_message(conn, message["hash"])["expires_at"] == message["expires_at"]
    finally:
        conn.close()


def test_send_applies_the_channel_default(identities, tmp_path, monkeypatch):
    from tau.disappearing import DisappearingDefaults, save_disappearing_defaults

    alice, _ = identities
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    save_disappearing_defaults(
        config.disappearing_path,
        DisappearingDefaults(channels={"general": 300.0}),
    )
    app = ChannelApp(config, alice, name="alice", gossip=False)
    app.join("general")
    message = app.send("general", "with default", publish=False)
    assert message["expires_at"] is not None
    assert message["expires_at"] - time.time() == pytest.approx(300, abs=5)

    # An explicit expiry wins over the default.
    explicit = time.time() + 7200
    message = app.send("general", "explicit", publish=False, expires_at=explicit)
    assert message["expires_at"] == explicit


def test_send_without_default_or_expiry_has_none(identities, tmp_path):
    alice, _ = identities
    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    app = ChannelApp(config, alice, name="alice", gossip=False)
    app.join("general")
    message = app.send("general", "forever", publish=False)
    assert "expires_at" not in message


def test_cli_channel_send_expire_roundtrip(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    )
    capsys.readouterr()
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--expire",
                "1h",
                "--json",
                "general",
                "ephemeral hello",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    expires_at = payload["message"]["expires_at"]
    assert expires_at - time.time() == pytest.approx(3600, abs=5)

    assert main(["channel", "read", "--data-dir", str(data_dir), "general"]) == 0
    assert "ephemeral hello" in capsys.readouterr().out

    # The channel default fills the expiry in when no --expire is passed.
    assert (
        main(
            ["disappearing", "--data-dir", str(data_dir), "--channel", "general", "5m"]
        )
        == 0
    )
    capsys.readouterr()
    assert (
        main(
            [
                "channel",
                "send",
                "--data-dir",
                str(data_dir),
                "--no-publish",
                "--json",
                "general",
                "defaulted hello",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["message"]["expires_at"] - time.time() == pytest.approx(
        300, abs=5
    )


def test_cli_channel_send_expire_rejects_bad_durations(tmp_path, capsys):
    data_dir = tmp_path / "alice"
    assert main(["init", "--data-dir", str(data_dir), "--name", "alice"]) == 0
    assert (
        main(["channel", "join", "--data-dir", str(data_dir), "general"]) == 0
    )
    for bad in ("0s", "31d", "not-a-duration"):
        assert (
            main(
                [
                    "channel",
                    "send",
                    "--data-dir",
                    str(data_dir),
                    "--no-publish",
                    "--expire",
                    bad,
                    "general",
                    "nope",
                ]
            )
            == 2
        )
        assert "error" in capsys.readouterr().err
