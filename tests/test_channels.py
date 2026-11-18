from __future__ import annotations

import json
import time

import pytest
import RNS

from tau.blocklist import BlockList, save_blocklist
from tau.channels import (
    CHANNEL_FEED_PATH,
    CHANNEL_POST_PATH,
    MAX_STORED_PER_CHANNEL,
    ChannelApp,
    ChannelError,
    channel_history,
    channel_history_page,
    clean_channel_name,
    create_edit,
    create_message,
    current_message_text,
    get_edit,
    get_message,
    init_db,
    is_member,
    join_channel,
    latest_edit_for,
    leave_channel,
    list_edits,
    list_memberships,
    resolve_message_digest,
    store_edit,
    store_message,
    verify_edit,
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
