from __future__ import annotations

import time

import pytest
import RNS

from tau.blocklist import BlockList, save_blocklist
from tau.channels import (
    CHANNEL_FEED_PATH,
    CHANNEL_POST_PATH,
    ChannelApp,
    ChannelError,
    channel_history,
    clean_channel_name,
    create_message,
    get_message,
    init_db,
    is_member,
    join_channel,
    leave_channel,
    list_memberships,
    store_message,
    verify_message,
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
        return {
            "ok": True,
            "messages": channel_history(
                self.source,
                request["channel"],
                limit=request.get("limit", 20),
                before=request.get("before"),
            ),
        }


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
