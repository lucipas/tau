from __future__ import annotations

import http.client
import json
import socket
import sys
import threading
import time
import urllib.error
import urllib.request

import pytest

from tau.inbox import Inbox
from tau.mailbox import Mailbox
from tau.peers import Peer
from tau.webapi import WebBridge, WebBridgeError, check_web_console_bind

PEER_HASH = "ab" * 16
OTHER_HASH = "cd" * 16


class FakeRegistry:
    def __init__(self, peers):
        self._peers = list(peers)

    def all(self, max_age=None):
        return list(self._peers)

    def get(self, destination_hash):
        if isinstance(destination_hash, str):
            try:
                destination_hash = bytes.fromhex(destination_hash)
            except ValueError:
                return None
        for peer in self._peers:
            if peer.destination_hash == destination_hash:
                return peer
        return None


class FakeNode:
    def __init__(self, mailbox_path=None):
        self.name = "tester"
        self.mailbox = Mailbox(mailbox_path)
        self.inbox = Inbox()
        self.registry = FakeRegistry(
            [
                Peer(
                    destination_hash=bytes.fromhex(PEER_HASH),
                    name="alice",
                    public_key=b"\x01" * 32,
                    first_seen=1000.0,
                    last_seen=time.time(),
                    hops=1,
                )
            ]
        )
        self.sent = []
        self.announced = 0

    def info(self):
        return {
            "name": self.name,
            "destination": "cd" * 16,
            "protocol": 1,
            "version": "0.1.0",
            "rns_version": "test",
            "uptime": 12.5,
            "peers": 1,
            "interfaces": [{"name": "TCP", "online": True}],
        }

    def resolve_peer(self, target, *, timeout=10.0):
        if target in ("alice", PEER_HASH):
            return self.registry.all()[0]
        raise ValueError(f"unknown peer: {target}")

    def send_message(
        self,
        peer,
        text,
        *,
        timeout=20.0,
        discover_timeout=None,
        in_reply_to=None,
        expires_at=None,
    ):
        self.sent.append((peer.name, text, in_reply_to))
        self.last_expires_at = expires_at
        # A distinct wire id per send, like the real node's generated id.
        wire_id = f"{len(self.sent):032d}"
        return {
            "ok": True,
            "echo": text,
            "server": self.name,
            "_rtt": 0.012,
            "id": wire_id,
        }

    def ping(self, peer, *, timeout=15.0, discover_timeout=None):
        return {"ok": True, "pong": True, "server": self.name, "_rtt": 0.004}

    def announce(self):
        self.announced += 1


@pytest.fixture()
def node(tmp_path):
    return FakeNode(tmp_path / "mailbox.json")


@pytest.fixture()
def bridge(node, tmp_path):
    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    (web_dir / "assets").mkdir()
    (web_dir / "assets" / "app.js").write_text("console.log('tau')", encoding="utf-8")
    bridge = WebBridge(node, host="127.0.0.1", port=0, web_dir=web_dir)
    bridge.start()
    try:
        yield bridge
    finally:
        bridge.stop()


def _request(bridge, path, *, method="GET", body=None, raw=None, headers=None):
    url = f"http://127.0.0.1:{bridge.port}{path}"
    data = None
    request_headers = dict(headers or {})
    if raw is not None:
        data = raw
        request_headers.setdefault("Content-Type", "application/json")
    elif body is not None:
        data = json.dumps(body).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")
    request = urllib.request.Request(
        url, data=data, method=method, headers=request_headers
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=5) as response:
        return response.status, dict(response.headers), response.read()


def _json(bridge, path, **kwargs):
    try:
        status, headers, body = _request(bridge, path, **kwargs)
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), json.loads(exc.read())
    return status, headers, json.loads(body)


def test_status_snapshot(bridge):
    status, _, payload = _json(bridge, "/api/status")
    assert status == 200
    assert payload["node"]["name"] == "tester"
    assert payload["peers"][0]["name"] == "alice"
    assert payload["messages"] == []


def test_peers_endpoint_includes_verification_state(bridge, node):
    peer = node.registry.all()[0]
    peer.verified_at = 1500.0
    peer.verified_fingerprint = "00001 00002"

    status, _, payload = _json(bridge, "/api/peers")
    assert status == 200
    assert payload["count"] == 1
    entry = payload["peers"][0]
    assert entry["verified"] is True
    assert entry["verified_at"] == 1500.0
    assert entry["key_change_at"] is None
    # Verification state is local-only: the public description stays clean.
    assert "verified" not in peer.public_dict()


def test_record_peer_event_carries_trust_fields(bridge, node):
    peer = node.registry.all()[0]
    peer.verified_at = 1500.0
    peer.verified_fingerprint = "00001 00002"
    peer.key_change_at = 1600.0

    bridge.record_peer(peer)
    events = bridge.bus.replay(0)
    peer_events = [event for event in events if event.get("type") == "peer"]
    assert peer_events
    # A live announce must not replace the snapshot record with a
    # trust-free one: the badge and warning survive the event.
    assert peer_events[-1]["peer"]["verified"] is True
    assert peer_events[-1]["peer"]["key_change_at"] == 1600.0


def test_send_message_is_recorded_and_published(bridge, node):
    from tau.protocol import excerpt_hash

    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    assert status == 200
    assert payload["message"]["direction"] == "out"
    assert payload["message"]["peer_name"] == "alice"
    assert payload["message"]["rtt_ms"] == 12.0
    assert payload["message"]["wire_id"] == "0" * 31 + "1"
    assert payload["message"]["excerpt_hash"] == excerpt_hash("hi")
    assert node.sent == [("alice", "hi", None)]

    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == ["hi"]

    events = bridge.bus.replay(0)
    assert any(e["type"] == "message" for e in events)


def test_send_reply_resolves_from_console_history(bridge, node):
    from tau.protocol import excerpt_hash

    status, _, first = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "original thought"},
    )
    assert status == 200
    wire_id = first["message"]["wire_id"]
    assert wire_id

    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "seconded", "reply_to": wire_id[:8]},
    )
    assert status == 200
    assert payload["message"]["in_reply_to"] == {
        "id": wire_id,
        "excerpt": excerpt_hash("original thought"),
    }
    assert node.sent[-1] == (
        "alice",
        "seconded",
        {"id": wire_id, "excerpt": excerpt_hash("original thought")},
    )

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][1]["in_reply_to"]["id"] == wire_id


def test_send_reply_to_unknown_or_other_peer_is_a_bad_request(bridge):
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "dangling", "reply_to": "zzz"},
    )
    assert status == 400
    assert "no message" in payload["error"]["message"]

    # A message from another peer must not resolve in this conversation.
    bridge.record_inbound(
        {
            "from": "carol",
            "destination": "cd" * 16,
            "text": "carol's thought",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "c" * 32,
        }
    )
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "cross", "reply_to": "c" * 8},
    )
    assert status == 400
    assert "no message" in payload["error"]["message"]


def test_inbound_message_keeps_reply_and_wire_id(bridge):
    from tau.protocol import excerpt_hash

    record = {
        "from": "alice",
        "destination": "ab" * 16,
        "text": "seconded",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "b" * 32,
        "in_reply_to": {
            "id": "a" * 32,
            "excerpt": excerpt_hash("original thought"),
        },
    }
    bridge.record_inbound(record)

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["wire_id"] == "b" * 32
    assert feed["messages"][0]["excerpt_hash"] == excerpt_hash("seconded")
    assert feed["messages"][0]["in_reply_to"] == {
        "id": "a" * 32,
        "excerpt": excerpt_hash("original thought"),
    }


def test_inbound_message_comes_back_over_the_api(bridge):
    record = {
        "from": "bob",
        "destination": "ef" * 16,
        "text": "from the mesh",
        "sent_at": 100.0,
        "received_at": 101.0,
    }
    bridge.record_inbound(record)

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["direction"] == "in"
    assert feed["messages"][0]["peer_name"] == "bob"
    assert feed["messages"][0]["text"] == "from the mesh"


def test_messages_search_via_api(bridge):
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello from the mesh",
            "sent_at": 100.0,
            "received_at": 101.0,
        }
    )
    bridge.record_inbound(
        {
            "from": "bob",
            "destination": OTHER_HASH,
            "text": "another MESH note",
            "sent_at": 102.0,
            "received_at": 103.0,
        }
    )
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "unrelated weather chat",
            "sent_at": 104.0,
            "received_at": 105.0,
        }
    )
    # An outbound message matches too: search covers the whole traffic view.
    _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "mesh reply from me"},
    )

    # Case-insensitive, newest first.
    status, _, payload = _json(bridge, "/api/messages/search?q=MESH")
    assert status == 200
    assert [item["text"] for item in payload["messages"]] == [
        "mesh reply from me",
        "another MESH note",
        "hello from the mesh",
    ]
    assert payload["count"] == 3

    status, _, payload = _json(bridge, "/api/messages/search?q=mesh&limit=1")
    assert status == 200
    assert payload["count"] == 1
    assert payload["messages"][0]["text"] == "mesh reply from me"

    # A peer filter narrows to one conversation, by name or hash prefix.
    status, _, payload = _json(bridge, "/api/messages/search?q=mesh&peer=alice")
    assert status == 200
    assert [item["text"] for item in payload["messages"]] == [
        "mesh reply from me",
        "hello from the mesh",
    ]

    status, _, payload = _json(
        bridge, f"/api/messages/search?q=mesh&peer={OTHER_HASH[:8]}"
    )
    assert status == 200
    assert [item["text"] for item in payload["messages"]] == ["another MESH note"]

    status, _, payload = _json(bridge, "/api/messages/search?q=mesh&peer=nobody")
    assert status == 200
    assert payload["messages"] == []

    # An empty query is a client error.
    status, _, payload = _json(bridge, "/api/messages/search?q=%20%20")
    assert status == 400
    assert "must not be empty" in payload["error"]["message"]


def test_messages_search_hides_blocked_and_expired(bridge, node):
    from tau.blocklist import BlockList

    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "mesh from alice",
            "sent_at": 100.0,
            "received_at": 101.0,
        }
    )
    bridge.record_inbound(
        {
            "from": "mallory",
            "destination": OTHER_HASH,
            "text": "mesh from mallory",
            "sent_at": 102.0,
            "received_at": 103.0,
        }
    )
    # A disappearing message whose deadline passed never matches.
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "mesh already gone",
            "sent_at": 104.0,
            "received_at": 105.0,
            "expires_at": 106.0,
        }
    )

    node.blocklist = BlockList(blocked=frozenset({OTHER_HASH}))

    status, _, payload = _json(bridge, "/api/messages/search?q=mesh")
    assert status == 200
    assert [item["text"] for item in payload["messages"]] == ["mesh from alice"]


def test_inbound_group_message_keeps_group_fields(bridge):
    record = {
        "from": "bob",
        "destination": "ef" * 16,
        "text": "hello group",
        "sent_at": 100.0,
        "received_at": 101.0,
        "kind": "group",
        "group": "aa" * 16,
        "group_name": "weekend",
    }
    bridge.record_inbound(record)

    _, _, feed = _json(bridge, "/api/messages")
    message = feed["messages"][0]
    assert message["group"] == "aa" * 16
    assert message["group_name"] == "weekend"

    plain = {
        "from": "bob",
        "destination": "ef" * 16,
        "text": "plain",
        "sent_at": 102.0,
        "received_at": 103.0,
    }
    bridge.record_inbound(plain)
    _, _, feed = _json(bridge, "/api/messages")
    assert "group" not in feed["messages"][1]


def test_api_groups_lists_known_groups(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    node.groups.create(
        "weekend", [PEER_HASH, OTHER_HASH], created_by=PEER_HASH
    )
    status, _, payload = _json(bridge, "/api/groups")
    assert status == 200
    assert payload["count"] == 1
    group = payload["groups"][0]
    assert group["name"] == "weekend"
    assert [member["hash"] for member in group["members"]] == [
        PEER_HASH,
        OTHER_HASH,
    ]
    # The registry resolves the first member's display name.
    assert group["members"][0]["name"] == "alice"


def _inbound_record(node, **overrides):
    """A received DM, appended to the node inbox first — the order the
    node itself uses before it hands the record to the bridge."""
    record = {
        "from": "alice",
        "destination": PEER_HASH,
        "text": "hello",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "m-1",
    }
    record.update(overrides)
    node.inbox.append(record)
    return record


def _counts(unread):
    """Unread counts without the revision stamp, for shape checks."""
    return {key: value for key, value in unread.items() if key != "revision"}


def test_snapshot_carries_unread_counts(bridge, node):
    _, _, payload = _json(bridge, "/api/status")
    assert _counts(payload["unread"]) == {"per_peer": {}, "total": 0}

    _inbound_record(node)
    _inbound_record(node, id="m-2", received_at=102.0)

    _, _, payload = _json(bridge, "/api/status")
    assert _counts(payload["unread"]) == {"per_peer": {PEER_HASH: 2}, "total": 2}


def test_unread_revisions_only_increase(bridge, node):
    first = bridge.unread()["revision"]
    second = bridge.unread()["revision"]
    assert second > first


def test_inbound_message_event_carries_unread_total(bridge, node):
    record = _inbound_record(node)
    bridge.record_inbound(record)

    events = bridge.bus.replay(0)
    message_events = [event for event in events if event["type"] == "message"]
    assert _counts(message_events[-1]["unread"]) == {
        "per_peer": {PEER_HASH: 1},
        "total": 1,
    }


def test_mark_read_endpoint_clears_a_conversation(bridge, node):
    _inbound_record(node)
    _inbound_record(node, id="m-2", received_at=102.0)
    _inbound_record(
        node,
        destination=OTHER_HASH,
        **{"from": "carol", "id": "m-3", "received_at": 103.0},
    )

    status, _, payload = _json(
        bridge, "/api/messages/read", method="POST", body={"peer": PEER_HASH}
    )
    assert status == 200
    assert payload["ok"] is True
    assert _counts(payload["unread"]) == {"per_peer": {OTHER_HASH: 1}, "total": 1}

    # Other tabs learn from the published unread event.
    events = bridge.bus.replay(0)
    unread_events = [event for event in events if event["type"] == "unread"]
    assert _counts(unread_events[-1]["unread"]) == {
        "per_peer": {OTHER_HASH: 1},
        "total": 1,
    }

    # Marking everything read clears the rest.
    status, _, payload = _json(
        bridge, "/api/messages/read", method="POST", body={}
    )
    assert status == 200
    assert _counts(payload["unread"]) == {"per_peer": {}, "total": 0}


def test_mark_read_endpoint_rejects_a_bad_peer(bridge):
    status, _, payload = _json(
        bridge, "/api/messages/read", method="POST", body={"peer": "  "}
    )
    assert status == 400
    assert "peer" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/messages/read", method="POST", body={"peer": 42}
    )
    assert status == 400
    assert "peer" in payload["error"]["message"]


def test_send_validation(bridge):
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": " "}
    )
    assert status == 400
    assert "text" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"text": "hi"}
    )
    assert status == 400
    assert "to" in payload["error"]["message"]


def test_invalid_json_is_a_bad_request(bridge):
    with pytest.raises(urllib.error.HTTPError) as excinfo:
        _request(
            bridge,
            "/api/messages",
            method="POST",
            raw=b"{not json",
        )
    assert excinfo.value.code == 400
    assert b"valid JSON" in excinfo.value.read()


def test_ping_peer(bridge):
    status, _, payload = _json(bridge, "/api/ping", method="POST", body={"to": "alice"})
    assert status == 200
    assert payload["rtt_ms"] == 4.0
    assert payload["peer"]["destination"] == PEER_HASH


def test_announce(bridge, node):
    status, _, payload = _json(bridge, "/api/announce", method="POST", body={})
    assert status == 200
    assert payload["ok"] is True
    assert node.announced == 1


def test_id_rename_via_api(bridge, node, tmp_path):
    from types import SimpleNamespace

    node.config = SimpleNamespace(data_dir=tmp_path)
    node.app = SimpleNamespace(name="tester")
    status, _, payload = _json(
        bridge, "/api/id/rename", method="POST", body={"name": "  renamed  "}
    )
    assert status == 200
    assert payload == {
        "ok": True,
        "name": "renamed",
        "previous_name": "tester",
        "changed": True,
    }
    # The live node takes the new name at once: announces, DMs and new
    # Tan posts all read it from memory.
    assert node.name == "renamed"
    assert node.app.name == "renamed"
    assert node.announced == 1
    stored = json.loads((tmp_path / "node.json").read_text(encoding="utf-8"))
    assert stored["name"] == "renamed"
    status, _, payload = _json(bridge, "/api/status")
    assert payload["node"]["name"] == "renamed"


def test_id_rename_concurrent_renames_stay_consistent(bridge, node, tmp_path):
    from types import SimpleNamespace

    node.config = SimpleNamespace(data_dir=tmp_path)
    errors = []

    def rename(name):
        try:
            bridge.id_rename(name)
        except Exception as exc:  # noqa: BLE001 - collected, asserted below
            errors.append(exc)

    threads = [
        threading.Thread(target=rename, args=(name,))
        for name in ("alpha", "beta")
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    # Both renames landed in some order; disk and the live node agree on
    # whichever ran second. (Regression: the serialising lock must exist.)
    assert node.name in ("alpha", "beta")
    stored = json.loads((tmp_path / "node.json").read_text(encoding="utf-8"))
    assert stored["name"] == node.name


def test_id_rename_same_name_is_a_noop(bridge, node, tmp_path):
    from types import SimpleNamespace

    node.config = SimpleNamespace(data_dir=tmp_path)
    status, _, payload = _json(
        bridge, "/api/id/rename", method="POST", body={"name": "tester"}
    )
    assert status == 200
    assert payload["changed"] is False
    assert payload["previous_name"] == "tester"
    assert node.announced == 0
    assert not (tmp_path / "node.json").exists()


def test_id_rename_rejects_blank_and_missing_names(bridge, node, tmp_path):
    from types import SimpleNamespace

    node.config = SimpleNamespace(data_dir=tmp_path)
    for body in ({"name": "   "}, {"name": ""}, {}, {"name": 42}):
        status, _, payload = _json(
            bridge, "/api/id/rename", method="POST", body=body
        )
        assert status == 400
        assert "name" in payload["error"]["message"]
    assert node.name == "tester"
    assert node.announced == 0


def test_static_console_and_assets(bridge):
    status, headers, body = _request(bridge, "/")
    assert status == 200
    assert headers["Content-Type"].startswith("text/html")
    assert b"tau console" in body

    status, headers, body = _request(bridge, "/assets/app.js")
    assert status == 200
    assert headers["Content-Type"].startswith("text/javascript")
    assert headers["Cache-Control"].startswith("public")


def test_static_traversal_is_blocked(bridge):
    assert bridge.resolve_static("/../../etc/passwd") is None
    status, _, _ = _json(bridge, "/%2e%2e/%2e%2e/etc/passwd")
    assert status == 404


def test_cross_origin_post_is_rejected(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "csrf"},
        headers={"Origin": "https://evil.example"},
    )
    assert status == 403
    assert node.sent == []


def test_post_requires_json_content_type(bridge):
    with pytest.raises(urllib.error.HTTPError) as excinfo:
        _request(
            bridge,
            "/api/announce",
            method="POST",
            raw=b"{}",
            headers={"Content-Type": "text/plain"},
        )
    assert excinfo.value.code == 400
    assert b"application/json" in excinfo.value.read()


def test_cors_allows_local_dev_origin(bridge):
    status, headers, _ = _request(
        bridge, "/api/status", headers={"Origin": "http://localhost:3000"}
    )
    assert status == 200
    assert headers["Access-Control-Allow-Origin"] == "http://localhost:3000"


def test_cors_ignores_remote_origin(bridge):
    status, headers, _ = _request(
        bridge, "/api/status", headers={"Origin": "https://evil.example"}
    )
    assert status == 200
    assert "Access-Control-Allow-Origin" not in headers


def test_options_preflight(bridge):
    status, headers, _ = _request(
        bridge,
        "/api/messages",
        method="OPTIONS",
        headers={"Origin": "http://127.0.0.1:3000"},
    )
    assert status == 204
    assert "POST" in headers["Access-Control-Allow-Methods"]


def test_events_endpoint_streams_a_snapshot(bridge):
    connection = http.client.HTTPConnection("127.0.0.1", bridge.port, timeout=5)
    try:
        connection.request("GET", "/api/events")
        response = connection.getresponse()
        assert response.status == 200
        assert response.getheader("Content-Type").startswith("text/event-stream")
        chunk = b""
        deadline = time.time() + 5
        while b"\n\n" not in chunk and time.time() < deadline:
            part = response.read1(4096)
            if not part:
                break
            chunk += part
        assert b"event: snapshot" in chunk
        assert b'"name":"tester"' in chunk
    finally:
        connection.close()


def test_event_replay_and_order(bridge):
    bridge.record_inbound(
        {
            "from": "bob",
            "destination": "ef" * 16,
            "text": "one",
            "sent_at": 1.0,
            "received_at": 2.0,
        }
    )
    events = bridge.bus.replay(0)
    assert events[-1]["type"] == "message"
    assert events[-1]["message"]["text"] == "one"
    first_id = events[0]["id"]
    assert [e["id"] for e in bridge.bus.replay(first_id)] == [
        e["id"] for e in events[1:]
    ]


def test_non_loopback_bind_requires_opt_in(node):
    with pytest.raises(WebBridgeError):
        WebBridge(node, host="0.0.0.0", port=0)


def test_unknown_api_route_returns_404(bridge):
    status, _, payload = _json(bridge, "/api/nope")
    assert status == 404
    assert payload["error"]["message"] == "not found"


def test_worker_threads_can_send_concurrently(bridge):
    def send(index):
        bridge.send_message("alice", f"m{index}")

    threads = [threading.Thread(target=send, args=(i,)) for i in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    _, _, feed = _json(bridge, "/api/messages")
    assert len(feed["messages"]) == 5
    assert len({m["id"] for m in feed["messages"]}) == 5


def test_tan_timeline_and_follow(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "hello from tan", name="bob", now=1000)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["hello from tan"]

        status, _, payload = _json(
            bridge,
            "/api/tan/follow",
            method="POST",
            body={"target": post["author"]},
        )
        assert status == 200
        assert payload["follow"]["author"] == post["author"]

        status, _, payload = _json(bridge, "/api/tan/following")
        assert status == 200
        assert payload["count"] == 1
        assert payload["following"][0]["posts"] == 1

        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={"text": "nope"},
        )
        assert status == 503
        assert "not enabled" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_mute_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(conn, identity, "hello from tan", name="tester", now=1000)
        theirs = create_post(conn, other, "bob's update", name="bob", now=1001)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/muted")
        assert status == 200
        assert payload == {"muted": [], "count": 0}

        status, _, payload = _json(
            bridge,
            "/api/tan/mute",
            method="POST",
            body={"target": theirs["author"]},
        )
        assert status == 200
        assert payload["mute"]["author"] == theirs["author"]
        first_muted_at = payload["mute"]["muted_at"]

        # The muted author's posts leave the timeline at once.
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["hello from tan"]

        status, _, payload = _json(bridge, "/api/tan/muted")
        assert status == 200
        assert payload["count"] == 1
        assert payload["muted"][0]["name"] == "bob"
        assert payload["muted"][0]["posts"] == 1

        # Re-muting is a no-op: the original mute time is kept.
        status, _, payload = _json(
            bridge,
            "/api/tan/mute",
            method="POST",
            body={"target": "bob"},
        )
        assert status == 200
        assert payload["mute"]["muted_at"] == first_muted_at
        status, _, relisted = _json(bridge, "/api/tan/muted")
        assert relisted["muted"][0]["muted_at"] == first_muted_at

        status, _, payload = _json(
            bridge,
            "/api/tan/unmute",
            method="POST",
            body={"target": "bob"},
        )
        assert status == 200
        assert payload["mute"]["unmuted"] is True

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert [item["text"] for item in payload["posts"]] == [
            "bob's update",
            "hello from tan",
        ]

        # Unmuting again reports that nothing was muted.
        status, _, payload = _json(
            bridge,
            "/api/tan/unmute",
            method="POST",
            body={"target": theirs["author"]},
        )
        assert payload["mute"]["unmuted"] is False

        # Muting yourself is refused, like the CLI.
        status, _, payload = _json(
            bridge,
            "/api/tan/mute",
            method="POST",
            body={"target": mine["author"]},
        )
        assert status == 400
        assert "cannot mute yourself" in payload["error"]["message"]

        # Unknown authors and missing fields are rejected.
        status, _, payload = _json(
            bridge,
            "/api/tan/mute",
            method="POST",
            body={"target": "nobody-here"},
        )
        assert status == 400
        status, _, payload = _json(
            bridge, "/api/tan/mute", method="POST", body={}
        )
        assert status == 400
        assert "field 'target' is required" in payload["error"]["message"]

        # Keyword mutes hide matching posts and list back.
        status, _, payload = _json(
            bridge,
            "/api/tan/mute-keyword",
            method="POST",
            body={"keyword": "hello"},
        )
        assert status == 200
        assert payload["mute"]["keyword"] == "hello"

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert [item["text"] for item in payload["posts"]] == ["bob's update"]

        status, _, payload = _json(bridge, "/api/tan/muted-keywords")
        assert status == 200
        assert payload["count"] == 1
        assert payload["muted_keywords"][0]["display"] == "hello"

        status, _, payload = _json(
            bridge,
            "/api/tan/unmute-keyword",
            method="POST",
            body={"keyword": "HELLO"},
        )
        assert status == 200
        assert payload["mute"]["unmuted"] is True

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert [item["text"] for item in payload["posts"]] == [
            "bob's update",
            "hello from tan",
        ]

        status, _, payload = _json(
            bridge,
            "/api/tan/mute-keyword",
            method="POST",
            body={"keyword": "   "},
        )
        assert status == 400
        assert "field 'keyword' is required" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_search_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import (
        create_delete,
        create_post,
        create_reply,
        init_db,
    )

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(
            conn, identity, "hello from the mesh", name="tester", now=1000
        )
        create_post(conn, identity, "another MESH note", name="tester", now=1001)
        create_post(conn, other, "unrelated weather chat", name="other", now=1002)
        # A reply is not a post: its text never matches, even on keyword.
        create_reply(
            conn, other, mine["hash"], "mesh reply", name="other", now=1003
        )
        # A deleted post never matches either.
        doomed = create_post(
            conn, other, "mesh post to delete", name="other", now=1004
        )
        create_delete(conn, other, doomed["hash"], name="other", now=1005)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # Case-insensitive, newest first.
        status, _, payload = _json(bridge, "/api/tan/search?q=MESH")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "another MESH note",
            "hello from the mesh",
        ]
        assert payload["count"] == 2

        status, _, payload = _json(bridge, "/api/tan/search?q=mesh&limit=1")
        assert status == 200
        assert payload["count"] == 1
        assert payload["posts"][0]["text"] == "another MESH note"

        # An explicit author narrows the search to their posts.
        status, _, payload = _json(
            bridge, f"/api/tan/search?q=mesh&author={mine['author']}"
        )
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "another MESH note",
            "hello from the mesh",
        ]

        status, _, payload = _json(
            bridge, f"/api/tan/search?q=weather&author={mine['author']}"
        )
        assert status == 200
        assert payload["posts"] == []

        # An empty query and an unknown author are client errors.
        status, _, payload = _json(bridge, "/api/tan/search?q=%20%20")
        assert status == 400
        assert "must not be empty" in payload["error"]["message"]

        status, _, payload = _json(bridge, "/api/tan/search?q=mesh&author=nobody")
        assert status == 400
        assert "unknown author" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_search_hides_blocked_and_muted(tmp_path):
    from types import SimpleNamespace

    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import (
        create_post,
        create_quote,
        init_db,
        mute,
        mute_keyword,
    )

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    blocked_id = RNS.Identity()
    muted_id = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, identity, "mesh from me", name="tester", now=1000)
        blocked_author = create_post(
            conn, blocked_id, "mesh from blocked", name="blocked", now=1001
        )["author"]
        muted_author = create_post(
            conn, muted_id, "mesh from muted", name="muted", now=1002
        )["author"]
        mute(conn, muted_author, name="muted", now=1003)
        # Searching for a muted keyword still finds it: search is explicit.
        create_post(conn, identity, "weather report", name="tester", now=1004)
        mute_keyword(conn, "weather", now=1005)
        # A quote of a blocked author's post still matches on its own
        # text, but the embedded original must stay redacted.
        blocked_original = create_post(
            conn, blocked_id, "blocked original words", name="blocked", now=1006
        )
        create_quote(
            conn,
            identity,
            blocked_original["hash"],
            "mesh quote",
            name="tester",
            now=1007,
        )
    finally:
        conn.close()

    node.blocklist = SimpleNamespace(
        blocked=frozenset({blocked_author}), block_all=False
    )
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/search?q=mesh")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "mesh quote",
            "mesh from me",
        ]
        # The quote matches on its own text, but its blocked original
        # stays redacted: no name, no words.
        quoted = payload["posts"][0]["quoted"]
        assert quoted["blocked"] is True
        assert quoted["name"] == ""
        assert quoted["text"] == ""
        assert "blocked original" not in json.dumps(payload)

        # An explicit author search still cannot surface a blocked author.
        status, _, payload = _json(
            bridge, f"/api/tan/search?q=mesh&author={blocked_author}"
        )
        assert status == 200
        assert payload["posts"] == []

        # ... but it can surface a muted one: that is a direct lookup.
        status, _, payload = _json(
            bridge, f"/api/tan/search?q=mesh&author={muted_author}"
        )
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["mesh from muted"]

        status, _, payload = _json(bridge, "/api/tan/search?q=weather")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["weather report"]

        # A block-list that failed closed hides every result.
        node.blocklist = SimpleNamespace(blocked=frozenset(), block_all=True)
        status, _, payload = _json(bridge, "/api/tan/search?q=mesh")
        assert status == 200
        assert payload["posts"] == []
    finally:
        bridge.stop()


def test_tan_mentions_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import (
        TanApp,
        create_delete,
        create_post,
        create_reply,
        init_db,
    )

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(
            conn, identity, "original thought", name="tester", now=1000
        )
        reply = create_reply(
            conn, other, mine["hash"], "seconded", name="other", now=1001
        )
        # A reply to my own post from myself is not a mention of me.
        create_reply(
            conn, identity, mine["hash"], "talking to myself", name="tester", now=1002
        )
        # A reply to someone else's post mentions them, not me.
        theirs = create_post(conn, other, "bob's own post", name="other", now=1003)
        create_reply(
            conn, identity, theirs["hash"], "not a mention", name="tester", now=1004
        )
        # A reply its author deleted never appears.
        doomed = create_reply(
            conn, other, mine["hash"], "take it back", name="other", now=1005
        )
        create_delete(conn, other, doomed["hash"], name="other", now=1006)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/mentions")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["seconded"]
        assert payload["count"] == 1
        # Each mention carries its parent, so the reader sees what was answered.
        assert payload["posts"][0]["reply_to"]["hash"] == mine["hash"]
        assert payload["posts"][0]["reply_to"]["text"] == "original thought"
        assert reply["hash"] == payload["posts"][0]["hash"]

        status, _, payload = _json(bridge, "/api/tan/mentions?limit=1")
        assert status == 200
        assert payload["count"] == 1
    finally:
        bridge.stop()

    # Without an identity there is nothing of yours to be replied to.
    node.app = None
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/mentions")
        assert status == 400
    finally:
        bridge.stop()


def test_tan_mentions_hide_blocked_and_muted(tmp_path):
    from types import SimpleNamespace

    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import (
        MAX_FEED_POSTS,
        TanApp,
        create_post,
        create_reply,
        init_db,
        mute,
        mute_keyword,
    )

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    blocked_id = RNS.Identity()
    muted_id = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(conn, identity, "my post", name="tester", now=1000)
        # Older visible reply: it must still fill the limit after the
        # newer hidden replies below are paged past.
        create_reply(conn, other, mine["hash"], "friendly reply", name="other", now=1001)
        blocked_author = create_reply(
            conn, blocked_id, mine["hash"], "blocked reply", name="blocked", now=1002
        )["author"]
        # Newer keyword-muted filler spans more than one page, so the
        # older visible reply is reached only by paging past it.
        for index in range(MAX_FEED_POSTS + 5):
            create_reply(
                conn,
                other,
                mine["hash"],
                f"spam filler {index}",
                name="other",
                now=2000 + index,
            )
        muted_author = create_reply(
            conn, muted_id, mine["hash"], "muted author reply", name="muted", now=1003
        )["author"]
        create_reply(conn, other, mine["hash"], "spam keyword reply", name="other", now=1004)
        mute(conn, muted_author, name="muted", now=1005)
        mute_keyword(conn, "spam", now=1006)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    node.blocklist = SimpleNamespace(
        blocked=frozenset({blocked_author}), block_all=False
    )
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/mentions?limit=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["friendly reply"]

        # A block-list that failed closed hides every mention.
        node.blocklist = SimpleNamespace(blocked=frozenset(), block_all=True)
        status, _, payload = _json(bridge, "/api/tan/mentions")
        assert status == 200
        assert payload["posts"] == []
    finally:
        bridge.stop()


def test_tan_mentions_unexpected_failure_is_500(tmp_path, monkeypatch):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, identity, "original thought", name="tester", now=1000)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        import tau.tangents.tan as tan_module

        def _boom(*args, **kwargs):
            raise RuntimeError("database exploded")

        monkeypatch.setattr(tan_module, "mentions", _boom)
        status, _, payload = _json(bridge, "/api/tan/mentions")
        assert status == 500
        assert payload["error"]["message"] == "internal error"
    finally:
        bridge.stop()


def test_tan_saved_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(conn, identity, "save me", name="tester", now=1000)
        theirs = create_post(conn, other, "save me too", name="other", now=1001)
        create_post(conn, other, "not saved", name="other", now=1002)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The timeline flags saved posts so the console can show save
        # state on each card.
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["save me"]["saved"] is False

        # Saving requires a target, and an unknown hash cannot be saved.
        status, _, payload = _json(
            bridge, "/api/tan/save", method="POST", body={}
        )
        assert status == 400
        status, _, payload = _json(
            bridge, "/api/tan/save", method="POST", body={"target": "ff" * 32}
        )
        assert status == 400

        status, _, payload = _json(
            bridge,
            "/api/tan/save",
            method="POST",
            body={"target": mine["hash"][:16]},
        )
        assert status == 200
        assert payload["save"]["hash"] == mine["hash"]
        assert payload["save"]["already_saved"] is False

        # Saving again is a no-op that reports the original save.
        status, _, payload = _json(
            bridge,
            "/api/tan/save",
            method="POST",
            body={"target": mine["hash"]},
        )
        assert status == 200
        assert payload["save"]["already_saved"] is True

        status, _, payload = _json(
            bridge,
            "/api/tan/save",
            method="POST",
            body={"target": theirs["hash"]},
        )
        assert status == 200

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["save me"]["saved"] is True
        assert by_text["save me too"]["saved"] is True
        assert by_text["not saved"]["saved"] is False

        # Saved posts come back newest save first, flagged as saved.
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "save me too",
            "save me",
        ]
        assert all(item["saved"] is True for item in payload["posts"])
        assert all("saved_at" in item for item in payload["posts"])

        # The list pages through older saves instead of stopping at the
        # first page. The resume offset comes from the response: it
        # counts raw rows, so filtering never shifts it.
        status, _, payload = _json(bridge, "/api/tan/saved?limit=1")
        assert [item["text"] for item in payload["posts"]] == ["save me too"]
        assert payload["next_offset"] == 1
        status, _, payload = _json(bridge, "/api/tan/saved?limit=1&offset=1")
        assert [item["text"] for item in payload["posts"]] == ["save me"]
        assert payload["next_offset"] is None
        status, _, payload = _json(bridge, "/api/tan/saved?limit=1&offset=2")
        assert payload["posts"] == []
        assert payload["next_offset"] is None

        # Unsaving removes it from the list; unsaving again is a no-op.
        status, _, payload = _json(
            bridge,
            "/api/tan/unsave",
            method="POST",
            body={"target": theirs["hash"][:16]},
        )
        assert status == 200
        assert payload["save"]["unsaved"] is True
        status, _, payload = _json(
            bridge,
            "/api/tan/unsave",
            method="POST",
            body={"target": theirs["hash"]},
        )
        assert status == 200
        assert payload["save"]["unsaved"] is False

        status, _, payload = _json(bridge, "/api/tan/saved")
        assert [item["text"] for item in payload["posts"]] == ["save me"]

        # A post deleted after being saved stays in the list with its
        # text blanked, so the save is not silently lost.
        node.app = TanApp(node.config, identity, name="tester", gossip=False)
        status, _, payload = _json(
            bridge,
            "/api/tan/delete",
            method="POST",
            body={"target": mine["hash"][:16]},
        )
        assert status == 200
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert status == 200
        assert len(payload["posts"]) == 1
        assert payload["posts"][0]["text"] == ""
        assert payload["posts"][0]["deleted"] is True
        assert payload["posts"][0]["saved"] is True
    finally:
        bridge.stop()


def test_tan_saved_annotations_and_muting(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import (
        create_post,
        create_quote,
        create_reply,
        init_db,
        mute,
        mute_keyword,
        save_post,
        unmute,
        unmute_keyword,
    )

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        original = create_post(
            conn, other, "original thought", name="other", now=1000
        )
        quote = create_quote(
            conn, identity, original["hash"], "worth sharing", name="tester", now=1001
        )
        reply = create_reply(
            conn, identity, original["hash"], "seconded", name="tester", now=1002
        )
        # Distinct save times keep the newest-first order deterministic.
        save_post(conn, quote["hash"], now=2001)
        save_post(conn, reply["hash"], now=2002)
        save_post(conn, original["hash"], now=2003)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A saved quote carries its original, like the timeline does.
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert status == 200
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["worth sharing"]["quoted"]["text"] == "original thought"
        assert by_text["seconded"]["reply_to"]["text"] == "original thought"

        # Muting the other author hides their saved post and redacts
        # them from reply parents; unmuting restores both.
        conn = init_db(node.config.social_db_path)
        try:
            mute(conn, original["author"], name="other")
        finally:
            conn.close()
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert [item["text"] for item in payload["posts"]] == [
            "seconded",
            "worth sharing",
        ]
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["seconded"]["reply_to"] is None
        # A muted author's post never surfaces through a quote preview
        # either — in the saved list or the timeline.
        assert by_text["worth sharing"]["quoted"] is None
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        timeline_by_text = {item["text"]: item for item in payload["posts"]}
        assert timeline_by_text["worth sharing"]["quoted"] is None
        # Paging fills past the muted rows: the first page still holds
        # one visible post, and the resume offset skips the muted row
        # instead of re-fetching it.
        status, _, payload = _json(bridge, "/api/tan/saved?limit=1")
        assert [item["text"] for item in payload["posts"]] == ["seconded"]
        assert payload["next_offset"] == 2
        status, _, payload = _json(bridge, "/api/tan/saved?limit=1&offset=2")
        assert [item["text"] for item in payload["posts"]] == ["worth sharing"]
        assert payload["next_offset"] is None
        conn = init_db(node.config.social_db_path)
        try:
            assert unmute(conn, original["author"]) is True
        finally:
            conn.close()
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert len(payload["posts"]) == 3

        # A muted keyword hides matching saves; unmuting restores them.
        conn = init_db(node.config.social_db_path)
        try:
            mute_keyword(conn, "sharing")
        finally:
            conn.close()
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert [item["text"] for item in payload["posts"]] == [
            "original thought",
            "seconded",
        ]
        conn = init_db(node.config.social_db_path)
        try:
            assert unmute_keyword(conn, "sharing") is True
        finally:
            conn.close()
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert len(payload["posts"]) == 3

        # A muted keyword blanks the quoted original (and the reply
        # parent) instead of leaking its text through the preview.
        conn = init_db(node.config.social_db_path)
        try:
            mute_keyword(conn, "thought")
        finally:
            conn.close()
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert [item["text"] for item in payload["posts"]] == [
            "seconded",
            "worth sharing",
        ]
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["worth sharing"]["quoted"]["text"] == ""
        assert by_text["worth sharing"]["quoted"]["muted"] is True
        assert by_text["seconded"]["reply_to"]["text"] == ""
        assert by_text["seconded"]["reply_to"]["muted"] is True
        conn = init_db(node.config.social_db_path)
        try:
            assert unmute_keyword(conn, "thought") is True
        finally:
            conn.close()
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert len(payload["posts"]) == 3
    finally:
        bridge.stop()


def test_tan_thread_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, create_reply, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        root = create_post(conn, identity, "root post", name="tester", now=1000)
        reply = create_reply(
            conn, other, root["hash"], "first reply", name="other", now=1001
        )
        nested = create_reply(
            conn, identity, reply["hash"], "reply to the reply", name="tester", now=1002
        )
        create_post(conn, other, "unrelated post", name="other", now=1003)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A hash prefix names the thread; entries come back oldest first
        # with their depth, and the unrelated post stays out.
        status, _, payload = _json(bridge, f"/api/tan/thread?ref={root['hash'][:16]}")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "root post",
            "first reply",
            "reply to the reply",
        ]
        assert [item["depth"] for item in payload["posts"]] == [0, 1, 2]
        assert payload["count"] == 3

        # Naming a reply resolves to the same thread, rooted at its post.
        status, _, payload = _json(bridge, f"/api/tan/thread?ref={nested['hash']}")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "root post",
            "first reply",
            "reply to the reply",
        ]

        # Unknown references and a missing ref fail the way the CLI does.
        status, _, payload = _json(bridge, "/api/tan/thread?ref=ab" * 16)
        assert status == 400
        assert "unknown post" in payload["error"]["message"]

        status, _, payload = _json(bridge, "/api/tan/thread")
        assert status == 400
        assert "field 'ref' is required" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_thread_unexpected_failure_is_500(tmp_path, monkeypatch):
    node = FakeNode()
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # An unexpected failure inside tan_thread is a server error,
        # not a bad reference: the console must not read it as a
        # deleted anchor and close the thread.
        monkeypatch.setattr(
            bridge,
            "tan_thread",
            lambda ref: (_ for _ in ()).throw(RuntimeError("database is locked")),
        )
        status, _, payload = _json(bridge, "/api/tan/thread?ref=abc12345")
        assert status == 500
        assert payload["error"]["message"] == "internal error"
    finally:
        bridge.stop()


def test_tan_delete_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, feed, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "delete me via api", name="tester", now=1000)
        create_post(conn, identity, "keeper via api", name="tester", now=1001)
        foreign = create_post(conn, other, "not yours", name="other", now=1002)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/tan/delete",
            method="POST",
            body={"target": post["hash"][:16]},
        )
        assert status == 200
        assert payload["delete"]["kind"] == "delete"
        assert payload["delete"]["target"] == post["hash"]

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "not yours",
            "keeper via api",
        ]
        # The timeline marks the viewer's own posts so the console only
        # offers the delete button on those.
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["keeper via api"]["own"] is True
        assert by_text["not yours"]["own"] is False

        # Deleting the same post again reports it as deleted.
        status, _, payload = _json(
            bridge,
            "/api/tan/delete",
            method="POST",
            body={"target": post["hash"]},
        )
        assert status == 400
        assert "has been deleted" in payload["error"]["message"]

        # Liking a deleted post fails the same way.
        status, _, payload = _json(
            bridge,
            "/api/tan/like",
            method="POST",
            body={"target": post["hash"]},
        )
        assert status == 400
        assert "has been deleted" in payload["error"]["message"]

        # Someone else's post cannot be deleted through the API.
        status, _, payload = _json(
            bridge,
            "/api/tan/delete",
            method="POST",
            body={"target": foreign["hash"]},
        )
        assert status == 400
        assert "did not author" in payload["error"]["message"]

        # A full hash this node has never seen is still accepted: the
        # tombstone applies when the post arrives.
        status, _, payload = _json(
            bridge,
            "/api/tan/delete",
            method="POST",
            body={"target": "cd" * 32},
        )
        assert status == 200
        assert payload["delete"]["target"] == "cd" * 32
    finally:
        bridge.stop()

    conn = init_db(node.config.social_db_path)
    try:
        entries = feed(conn, author=post["author"])
    finally:
        conn.close()
    assert [entry["kind"] for entry in entries] == [
        "post",
        "post",
        "delete",
        "delete",
    ]


def _queue(node, to_hex, text, message_id=None):
    return node.mailbox.enqueue(
        to=bytes.fromhex(to_hex),
        text=text,
        message_id=message_id,
    )


def test_outbox_empty(bridge):
    status, _, payload = _json(bridge, "/api/outbox")
    assert status == 200
    assert payload == {"outbox": [], "count": 0, "held_for_others": 0}


def test_outbox_lists_entries_with_names(bridge, node):
    first = _queue(node, PEER_HASH, "hello alice")
    second = _queue(node, OTHER_HASH, "hello stranger")
    node.mailbox.record_attempt(first.id, error="no route")

    status, _, payload = _json(bridge, "/api/outbox")
    assert status == 200
    assert payload["count"] == 2
    assert payload["held_for_others"] == 0
    by_id = {entry["id"]: entry for entry in payload["outbox"]}
    assert by_id[first.id]["to_name"] == "alice"
    assert by_id[first.id]["attempts"] == 1
    assert by_id[first.id]["last_error"] == "no route"
    assert by_id[first.id]["held_by"] == []
    # Unknown destination: falls back to the name stored at enqueue time.
    assert by_id[second.id]["to"] == OTHER_HASH
    assert by_id[second.id]["to_name"] == ""
    assert by_id[second.id]["text"] == "hello stranger"


def test_outbox_cancel_by_id_and_prefix(bridge, node):
    first = _queue(node, PEER_HASH, "first")
    second = _queue(node, PEER_HASH, "second")

    status, _, payload = _json(
        bridge, "/api/outbox/cancel", method="POST", body={"target": first.id}
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["cancelled"]["id"] == first.id
    assert payload["cancelled"]["to_name"] == "alice"

    status, _, payload = _json(
        bridge,
        "/api/outbox/cancel",
        method="POST",
        body={"target": second.id[:8]},
    )
    assert status == 200
    assert payload["cancelled"]["id"] == second.id

    status, _, payload = _json(bridge, "/api/outbox")
    assert payload == {"outbox": [], "count": 0, "held_for_others": 0}


def test_outbox_cancel_errors(bridge, node):
    status, _, payload = _json(
        bridge, "/api/outbox/cancel", method="POST", body={}
    )
    assert status == 400
    assert "target" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/outbox/cancel",
        method="POST",
        body={"target": "ff" * 16},
    )
    assert status == 404
    assert "unknown queued message" in payload["error"]["message"]

    # Two entries sharing a prefix make the prefix ambiguous.
    _queue(node, PEER_HASH, "one", message_id="aa" + "11" * 15)
    _queue(node, PEER_HASH, "two", message_id="aa" + "22" * 15)
    status, _, payload = _json(
        bridge, "/api/outbox/cancel", method="POST", body={"target": "aa"}
    )
    assert status == 404
    assert "match prefix" in payload["error"]["message"]

    # Cancelling an entry that left the outbox meanwhile is a 404 too.
    gone = _queue(node, PEER_HASH, "gone", message_id="bb" + "33" * 15)
    assert node.mailbox.remove_outbox(gone.id) is True
    status, _, payload = _json(
        bridge, "/api/outbox/cancel", method="POST", body={"target": gone.id}
    )
    assert status == 404
    assert "unknown queued message" in payload["error"]["message"]


def test_outbox_sees_entries_queued_by_another_process(bridge, node, tmp_path):
    from tau.mailbox import Mailbox

    other = Mailbox(tmp_path / "mailbox.json")
    entry = other.enqueue(to=bytes.fromhex(PEER_HASH), text="queued by cli")
    # The serving node's in-memory snapshot predates the other process.
    assert node.mailbox.get_outbox(entry.id) is None

    status, _, payload = _json(bridge, "/api/outbox")
    assert status == 200
    assert [item["id"] for item in payload["outbox"]] == [entry.id]
    assert payload["outbox"][0]["to_name"] == "alice"

    status, _, payload = _json(
        bridge, "/api/outbox/cancel", method="POST", body={"target": entry.id}
    )
    assert status == 200
    assert payload["cancelled"]["id"] == entry.id
    # A fresh view of the file agrees the entry is gone.
    assert Mailbox(tmp_path / "mailbox.json").get_outbox(entry.id) is None
def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _hold_port(port: int) -> socket.socket:
    holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    holder.bind(("127.0.0.1", port))
    holder.listen(1)
    return holder


def test_check_web_console_bind_passes_on_free_port():
    check_web_console_bind("127.0.0.1", _free_port())


def test_check_web_console_bind_names_held_port():
    port = _free_port()
    holder = _hold_port(port)
    try:
        with pytest.raises(WebBridgeError) as excinfo:
            check_web_console_bind("127.0.0.1", port)
    finally:
        holder.close()
    assert f"web console port {port} on 127.0.0.1 is already in use" in str(
        excinfo.value
    )


def test_check_web_console_bind_windows_names_live_listener(monkeypatch):
    # The Windows probe connects instead of binding: a live listener
    # completes the handshake, so the probe must report it.
    port = _free_port()
    holder = _hold_port(port)
    try:
        monkeypatch.setattr(sys, "platform", "win32")
        with pytest.raises(WebBridgeError) as excinfo:
            check_web_console_bind("127.0.0.1", port)
    finally:
        holder.close()
    assert f"web console port {port} on 127.0.0.1 is already in use" in str(
        excinfo.value
    )


def test_check_web_console_bind_windows_passes_on_free_port(monkeypatch):
    # ...while a free port refuses the connection, so the probe passes
    # even where a TIME_WAIT straggler could linger.
    monkeypatch.setattr(sys, "platform", "win32")
    check_web_console_bind("127.0.0.1", _free_port())


def test_bridge_start_names_held_port_instead_of_raw_oserror(node):
    port = _free_port()
    holder = _hold_port(port)
    try:
        bridge = WebBridge(node, host="127.0.0.1", port=port)
        with pytest.raises(WebBridgeError) as excinfo:
            bridge.start()
    finally:
        holder.close()
    assert f"web console port {port} on 127.0.0.1 is already in use" in str(
        excinfo.value
    )
def _queue_send(node, mail_id="mail-1"):
    """Make the fake node report a queued send with the given outbox id."""

    def _send(peer, text, **kwargs):
        return {
            "ok": True,
            "v": 1,
            "queued": True,
            "id": mail_id,
            "to": peer.hexhash,
            "expires_at": 9999.0,
        }

    node.send_message = _send


def test_queued_send_records_outbox_id(bridge, node):
    _queue_send(node)
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    assert status == 200
    assert payload["message"]["status"] == "queued"
    assert payload["message"]["mail_id"] == "mail-1"


def test_direct_send_records_no_outbox_id(bridge):
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    assert status == 200
    assert payload["message"]["status"] == "delivered"
    assert "mail_id" not in payload["message"]


def test_record_delivery_flips_queued_message_and_publishes(bridge, node):
    _queue_send(node)
    _, _, sent = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    message_id = sent["message"]["id"]

    updated = bridge.record_delivery("mail-1")
    assert updated is not None
    assert updated["id"] == message_id
    assert updated["status"] == "delivered"

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["status"] == "delivered"

    events = bridge.bus.replay(0)
    updates = [event for event in events if event["type"] == "message_updated"]
    assert len(updates) == 1
    assert updates[0]["message"]["id"] == message_id
    assert updates[0]["message"]["status"] == "delivered"


def test_record_delivery_ignores_unknown_id(bridge, node):
    _queue_send(node)
    _json(bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"})

    assert bridge.record_delivery("not-an-outbox-id") is None
    assert bridge.record_delivery("mail-1") is not None
    # A second delivery report for the same id is a no-op: it is no
    # longer queued, so there is nothing left to flip.
    assert bridge.record_delivery("mail-1") is None

    events = bridge.bus.replay(0)
    updates = [event for event in events if event["type"] == "message_updated"]
    assert len(updates) == 1


def test_delivery_flip_survives_bridge_restart(node, tmp_path):
    """The delivered status is persisted, not just held in memory.

    Regression test for the PR #35 / PR #32 merge: record_delivery was
    written against the in-memory-only bridge, so without a save the
    flip was lost on restart and the message came back queued.
    """
    _queue_send(node)
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        message, _ = bridge.send_message("alice", "hi")
        assert message["status"] == "queued"
        assert bridge.record_delivery("mail-1") is not None
    finally:
        bridge.stop()

    bridge2 = _bridge_with_data_dir(FakeNode(), tmp_path)
    try:
        assert bridge2.messages()[0]["status"] == "delivered"
    finally:
        bridge2.stop()


def test_queued_message_keeps_mail_id_across_restart(node, tmp_path):
    """A still-queued message can flip after a restart.

    The persisted record must keep its outbox id; the history loader
    originally rebuilt records from known keys and dropped it, leaving
    the message unmatchable when delivery was finally reported.
    """
    _queue_send(node)
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        bridge.send_message("alice", "hi")
    finally:
        bridge.stop()

    bridge2 = _bridge_with_data_dir(FakeNode(), tmp_path)
    try:
        assert bridge2.messages()[0]["status"] == "queued"
        updated = bridge2.record_delivery("mail-1")
        assert updated is not None
        assert updated["status"] == "delivered"
    finally:
        bridge2.stop()


def test_delivery_before_record_is_not_lost(bridge, node):
    """A flush that delivers before send_message records the message.

    Queueing starts a flush thread, so the delivery callback can fire
    before ``_append_message`` runs. The report is buffered and
    reconciled when the record is appended, so the message still flips
    to delivered instead of staying queued forever.
    """
    started = []

    def _send(peer, text, **kwargs):
        thread = threading.Thread(
            target=node.on_delivered, args=("mail-1",), name="tau-test-deliver"
        )
        thread.start()
        started.append(thread)
        # Let the delivery thread run first: without the send lock it
        # would find no record and the message would stay queued.
        time.sleep(0.05)
        return {
            "ok": True,
            "v": 1,
            "queued": True,
            "id": "mail-1",
            "to": peer.hexhash,
            "expires_at": 9999.0,
        }

    node.send_message = _send
    status, _, _ = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    assert status == 200
    started[0].join(timeout=5)
    assert not started[0].is_alive()

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["status"] == "delivered"


def test_early_delivery_buffer_evicts_oldest(bridge):
    """Unmatched delivery reports are bounded, oldest evicted first.

    Regression test for the CodeRabbit Minor on PR #35: the buffer was
    a plain dict but eviction called ``popitem(last=False)`` — an
    OrderedDict-only API — so the 129th unmatched report raised
    TypeError instead of evicting.
    """
    from tau.webapi import MAX_EARLY_DELIVERIES

    for index in range(MAX_EARLY_DELIVERIES + 5):
        assert bridge.record_delivery(f"unknown-{index}") is None
    assert len(bridge._early_deliveries) == MAX_EARLY_DELIVERIES
    assert "unknown-0" not in bridge._early_deliveries
    assert f"unknown-{MAX_EARLY_DELIVERIES + 4}" in bridge._early_deliveries


def test_delivery_report_not_blocked_by_slow_send(bridge, node):
    """A delivery report completes while another send is in flight.

    Regression test for the CodeRabbit finding on PR #35: the send lock
    was held across ``node.send_message``, so a delivery report for an
    earlier message stalled behind the network send (and with it the
    node's mail sweep, whose thread invokes the callback).
    """
    _queue_send(node, mail_id="mail-a")
    status, _, _ = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "first"}
    )
    assert status == 200

    send_started = threading.Event()
    release_send = threading.Event()

    def _slow_send(peer, text, **kwargs):
        send_started.set()
        release_send.wait(timeout=10)
        return {"ok": True, "v": 1, "to": peer.hexhash}

    node.send_message = _slow_send
    send_thread = threading.Thread(
        target=bridge.send_message, args=("alice", "second")
    )
    send_thread.start()
    try:
        assert send_started.wait(timeout=5)
        report_thread = threading.Thread(
            target=bridge.record_delivery, args=("mail-a",)
        )
        report_thread.start()
        report_thread.join(timeout=5)
        assert not report_thread.is_alive()
        assert bridge.messages()[0]["status"] == "delivered"
    finally:
        release_send.set()
        send_thread.join(timeout=5)


def test_raising_previous_delivery_callback_still_flips(node, tmp_path):
    """A previous callback that raises must not lose the status update."""

    def _boom(message_id):
        raise RuntimeError("boom")

    node.on_delivered = _boom
    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    chained = WebBridge(node, host="127.0.0.1", port=0, web_dir=web_dir)
    try:
        _queue_send(node)
        message, _ = chained.send_message("alice", "hi")
        assert message["status"] == "queued"

        # The failure propagates (the node's callback guard logs it in
        # production), but only after the record has flipped.
        with pytest.raises(RuntimeError, match="boom"):
            node.on_delivered("mail-1")
        assert chained.messages()[0]["status"] == "delivered"
    finally:
        chained.stop()


def test_bridge_chains_existing_delivery_callback(node, tmp_path):
    seen = []
    node.on_delivered = seen.append
    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    chained = WebBridge(node, host="127.0.0.1", port=0, web_dir=web_dir)
    try:
        _queue_send(node)
        message, _ = chained.send_message("alice", "hi")
        assert message["status"] == "queued"

        node.on_delivered("mail-1")
        assert seen == ["mail-1"]
        assert chained.messages()[0]["status"] == "delivered"
    finally:
        chained.stop()
def _stored_message(message_id, text, direction="in"):
    return {
        "id": message_id,
        "direction": direction,
        "peer": PEER_HASH,
        "peer_name": "alice",
        "text": text,
        "sent_at": 1000.0 + message_id,
        "received_at": 1001.0 + message_id,
        "status": "received" if direction == "in" else "delivered",
    }


def _bridge_with_data_dir(node, tmp_path):
    from tau.config import TauConfig

    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    return bridge


def test_message_history_survives_bridge_restart(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "hello before restart",
                "sent_at": 1000.0,
                "received_at": 1001.0,
                "id": "msg-1",
            }
        )
        bridge.send_message("alice", "reply before restart")
    finally:
        bridge.stop()

    stored = json.loads((tmp_path / "messages.json").read_text(encoding="utf-8"))
    assert [m["text"] for m in stored["messages"]] == [
        "hello before restart",
        "reply before restart",
    ]
    assert stored["next_message_id"] == 3

    node2 = FakeNode()
    bridge2 = _bridge_with_data_dir(node2, tmp_path)
    try:
        messages = bridge2.messages()
        assert [m["text"] for m in messages] == [
            "hello before restart",
            "reply before restart",
        ]
        assert [m["id"] for m in messages] == [1, 2]
        # Ids continue across the restart instead of restarting at 1.
        bridge2.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "hello after restart",
                "sent_at": 2000.0,
                "received_at": 2001.0,
                "id": "msg-2",
            }
        )
        assert bridge2.messages()[-1]["id"] == 3
    finally:
        bridge2.stop()


def test_message_history_without_data_dir_stays_in_memory(node):
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        assert bridge._messages_path is None
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "memory only",
                "sent_at": 1000.0,
                "received_at": 1001.0,
                "id": "msg-1",
            }
        )
        assert [m["text"] for m in bridge.messages()] == ["memory only"]
    finally:
        bridge.stop()


def test_message_history_load_caps_and_drops_invalid_entries(node, tmp_path):
    from tau.webapi import MAX_MESSAGE_LOG

    seeded = [_stored_message(i + 1, f"m{i + 1}") for i in range(MAX_MESSAGE_LOG + 5)]
    seeded.insert(3, {"id": "not-an-int", "text": "bad id"})
    seeded.insert(5, {"direction": "sideways", "text": "bad direction"})
    seeded.insert(7, "not a dict at all")
    (tmp_path / "messages.json").write_text(
        json.dumps({"messages": seeded, "next_message_id": 9999}),
        encoding="utf-8",
    )

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        messages = bridge.messages(limit=0)
        assert len(messages) == MAX_MESSAGE_LOG
        # The three invalid entries were dropped and the oldest valid
        # messages fell off the cap, so the log starts at m6.
        assert messages[0]["text"] == "m6"
        assert messages[-1]["text"] == f"m{MAX_MESSAGE_LOG + 5}"
        # The saved next id is honoured (and stays above the max id).
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "after load",
                "sent_at": 2000.0,
                "received_at": 2001.0,
                "id": "msg-3",
            }
        )
        assert bridge.messages()[-1]["id"] == 9999
    finally:
        bridge.stop()


def test_message_history_load_recomputes_excerpt_hash(node, tmp_path):
    """A stored excerpt hash is never trusted on load: the hash is
    derived from the stored text, so a hand-edited history that
    changed the text but kept the old hash cannot make a changed
    quote look verified."""
    from tau.protocol import excerpt_hash
    from tau.webapi import _clean_stored_message

    entry = _stored_message(1, "changed text")
    entry["excerpt_hash"] = excerpt_hash("original thought")
    cleaned = _clean_stored_message(entry)
    assert cleaned is not None
    assert cleaned["excerpt_hash"] == excerpt_hash("changed text")
    assert cleaned["excerpt_hash"] != excerpt_hash("original thought")

    # Records written before hashes existed gain one on load.
    legacy = _stored_message(2, "legacy text")
    assert _clean_stored_message(legacy)["excerpt_hash"] == excerpt_hash(
        "legacy text"
    )


def test_corrupt_message_history_starts_empty(node, tmp_path):
    (tmp_path / "messages.json").write_text("{not json", encoding="utf-8")
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        assert bridge.messages() == []
        bridge.send_message("alice", "fresh start")
        assert [m["text"] for m in bridge.messages()] == ["fresh start"]
        assert [m["id"] for m in bridge.messages()] == [1]
    finally:
        bridge.stop()


def test_concurrent_appends_all_persist(node, tmp_path, monkeypatch):
    """Overlapping appends with slow writes must not lose history.

    Regression test for the Codex/Qodo finding on PR #32: snapshots taken
    under the bridge lock but written after releasing it could persist out
    of order, dropping the later message and reusing its id.
    """
    import tau.webapi

    real_write = tau.webapi.atomic_write_json

    def slow_write(path, obj, mode=None):
        time.sleep(0.02)
        return real_write(path, obj, mode=mode)

    monkeypatch.setattr(tau.webapi, "atomic_write_json", slow_write)

    bridge = _bridge_with_data_dir(node, tmp_path)
    barrier = threading.Barrier(10)
    try:
        def append(index):
            barrier.wait(timeout=10)
            bridge.record_inbound(
                {
                    "from": "alice",
                    "destination": PEER_HASH,
                    "text": f"concurrent {index}",
                    "sent_at": 1000.0 + index,
                    "received_at": 1001.0 + index,
                    "id": f"msg-{index}",
                }
            )

        threads = [threading.Thread(target=append, args=(i,)) for i in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)

        stored = json.loads((tmp_path / "messages.json").read_text(encoding="utf-8"))
        assert len(stored["messages"]) == 10
        assert len({m["id"] for m in stored["messages"]}) == 10
        assert {m["text"] for m in stored["messages"]} == {
            f"concurrent {i}" for i in range(10)
        }
        assert stored["next_message_id"] == 11
    finally:
        bridge.stop()


def test_outbound_text_with_lone_surrogate_stays_persistable(node, tmp_path):
    """An escaped lone surrogate in a sent message must not poison saves.

    Regression test for the Qodo finding on PR #32: the bridge recorded
    the raw text while the node sent the normalised one, so the stored
    record could not be UTF-8 encoded and every later save failed.
    """
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        status, _, payload = _json(
            bridge,
            "/api/messages",
            method="POST",
            raw=b'{"to": "alice", "text": "hi \\ud800 there"}',
        )
        assert status == 200
        # The surrogate is dropped by normalisation, so the recorded text
        # is exactly what was sent.
        assert payload["message"]["text"] == "hi  there"

        # The history file round-trips, and a later message still persists.
        bridge.send_message("alice", "still saving")
        stored = json.loads((tmp_path / "messages.json").read_text(encoding="utf-8"))
        assert [m["text"] for m in stored["messages"]] == [
            "hi  there",
            "still saving",
        ]
    finally:
        bridge.stop()

    node2 = FakeNode()
    bridge2 = _bridge_with_data_dir(node2, tmp_path)
    try:
        assert [m["text"] for m in bridge2.messages()] == [
            "hi  there",
            "still saving",
        ]
    finally:
        bridge2.stop()


def test_lone_surrogate_entry_is_dropped_on_load(node, tmp_path):
    """A stored lone surrogate must not break startup.

    Regression test for the CodeRabbit finding on PR #95: hashing the
    text before the UTF-8 check raised UnicodeEncodeError out of
    _clean_stored_message instead of dropping the entry.
    """
    good = _stored_message(1, "good entry")
    bad = _stored_message(2, "bad \ud800 text")
    (tmp_path / "messages.json").write_text(
        json.dumps({"messages": [good, bad], "next_message_id": 3}),
        encoding="utf-8",
    )

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        assert [m["text"] for m in bridge.messages()] == ["good entry"]
    finally:
        bridge.stop()


def test_huge_timestamp_entry_is_dropped_on_load(node, tmp_path):
    """A parseable but overflowing integer timestamp must not break startup.

    Regression test for the Qodo warning on PR #32: float() of a huge JSON
    integer raises OverflowError during bridge construction.
    """
    good = _stored_message(1, "good entry")
    bad = _stored_message(2, "bad timestamp")
    bad["sent_at"] = 10**400
    (tmp_path / "messages.json").write_text(
        json.dumps({"messages": [good, bad], "next_message_id": 3}),
        encoding="utf-8",
    )

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        assert [m["text"] for m in bridge.messages()] == ["good entry"]
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "after load",
                "sent_at": 2000.0,
                "received_at": 2001.0,
                "id": "msg-3",
            }
        )
        assert bridge.messages()[-1]["id"] == 3
    finally:
        bridge.stop()


def test_tan_reply_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "api thread starter", name="tester", now=1000)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={"text": "api reply", "reply_to": post["hash"][:16]},
        )
        assert status == 200
        reply = payload["post"]
        assert reply["kind"] == "reply"
        assert reply["target"] == post["hash"]
        assert reply["text"] == "api reply"

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "api reply",
            "api thread starter",
        ]
        assert payload["posts"][0]["reply_to"] == {
            "hash": post["hash"],
            "author": post["author"],
            "name": "tester",
            "text": "api thread starter",
            "deleted": False,
            "edited": False,
        }

        # Replying to an unknown post fails.
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={"text": "nowhere", "reply_to": "cd" * 32},
        )
        assert status == 400
        assert "unknown post" in payload["error"]["message"]

        # A non-string reply_to is a bad request, not a crash.
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={"text": "nowhere", "reply_to": 42},
        )
        assert status == 400
        assert "reply_to" in payload["error"]["message"]

        # A plain post still works with no reply_to.
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={"text": "standalone"},
        )
        assert status == 200
        assert payload["post"]["kind"] == "post"
    finally:
        bridge.stop()


def test_peers_marks_blocked_peers(bridge, node):
    from tau.blocklist import BlockList

    node.blocklist = BlockList(blocked=frozenset({PEER_HASH}))
    _, _, payload = _json(bridge, "/api/peers")
    assert payload["peers"][0]["destination"] == PEER_HASH
    assert payload["peers"][0]["blocked"] is True

    node.blocklist = BlockList()
    _, _, payload = _json(bridge, "/api/peers")
    assert payload["peers"][0]["blocked"] is False


def test_peers_marks_everyone_blocked_when_list_failed_closed(bridge, node):
    from tau.blocklist import BlockList

    node.blocklist = BlockList(block_all=True)
    _, _, payload = _json(bridge, "/api/peers")
    assert payload["peers"][0]["blocked"] is True


def test_messages_hides_blocked_senders(bridge, node):
    from tau.blocklist import BlockList

    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "from alice",
            "sent_at": 100.0,
            "received_at": 101.0,
        }
    )
    bridge.record_inbound(
        {
            "from": "mallory",
            "destination": OTHER_HASH,
            "text": "from mallory",
            "sent_at": 102.0,
            "received_at": 103.0,
        }
    )
    node.blocklist = BlockList(blocked=frozenset({OTHER_HASH}))
    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == ["from alice"]

    # Unblocking restores the view: nothing was deleted.
    node.blocklist = BlockList()
    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == ["from alice", "from mallory"]

    # A list that failed closed hides every inbound message.
    node.blocklist = BlockList(block_all=True)
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []


def test_tan_timeline_hides_blocked_authors(tmp_path):
    import RNS

    from tau.blocklist import BlockList
    from tau.config import TauConfig
    from tau.identity import destination_hash
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    alice, bob = RNS.Identity(), RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, alice, "from alice", name="alice", now=1000)
        create_post(conn, bob, "from bob", name="bob", now=1001)
    finally:
        conn.close()
    # The console uses the node's cached block-list, the same snapshot
    # the DM gate enforces.
    node.blocklist = BlockList(blocked=frozenset({destination_hash(alice).hex()}))

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["from bob"]

        node.blocklist = BlockList(block_all=True)
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        assert payload["posts"] == []
    finally:
        bridge.stop()


def test_messages_reconciles_cli_made_reactions(node, tmp_path):
    """A reaction written to the shared inbox by another process (the
    CLI) shows up on the next read of the console history.

    The write goes through a *second* Inbox object on the same path —
    the way a separate ``tau react`` process writes — so the bridge's
    own inbox must reload from disk to see it; reading its in-memory
    cache would stay stale.
    """
    from tau.config import TauConfig
    from tau.inbox import Inbox

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        inbox_path = TauConfig.resolve(data_dir=tmp_path, name="tester").inbox_path
        node.inbox = Inbox(inbox_path)
        cli_inbox = Inbox(inbox_path)
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "hello",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "wire-1",
            }
        )
        # The CLI process writes only the inbox; the bridge never sees
        # a live callback for it.
        cli_inbox.record_reaction(
            {
                "target": "wire-1",
                "destination": PEER_HASH,
                "from": "alice",
                "reaction": "👍",
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "react-1",
            }
        )
        messages = bridge.messages()
        assert messages[0]["reactions"] == [
            {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
        ]
        # ...and a CLI retraction clears it on the next read too.
        cli_inbox.record_reaction(
            {
                "target": "wire-1",
                "destination": PEER_HASH,
                "from": "alice",
                "reaction": "",
                "sent_at": 104.0,
                "received_at": 105.0,
                "id": "react-2",
            }
        )
        assert bridge.messages()[0]["reactions"] == []
    finally:
        bridge.stop()


def test_react_endpoint_reacts_to_a_history_message(bridge, node):
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )
    sent = []

    def send_reaction(peer, target, token, *, timeout=20.0, discover_timeout=None):
        sent.append((peer, target, token))
        # Mirror the node's local recording so the console annotates.
        bridge.record_reaction(
            {
                "target": target,
                "reaction": token,
                "from": "tester",
                "destination": "cd" * 16,
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "react-1",
            }
        )
        return {"ok": True, "reaction": token, "target": target, "id": "react-1"}

    node.send_reaction = send_reaction
    status, _, payload = _json(
        bridge,
        "/api/react",
        method="POST",
        body={"target": "wire-1", "reaction": "👍"},
    )
    assert status == 200
    assert payload["target"] == "wire-1"
    assert payload["reaction"] == "👍"
    assert payload["cleared"] is False
    assert sent == [(PEER_HASH, "wire-1", "👍")]

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["reactions"] == [
        {"reactor": "cd" * 16, "name": "tester", "reaction": "👍"}
    ]


def test_react_endpoint_rejects_unknown_target_and_bad_token(bridge):
    status, _, payload = _json(
        bridge, "/api/react", method="POST", body={"target": "nope", "reaction": "👍"}
    )
    assert status == 400
    assert "unknown message" in payload["error"]["message"]

    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )
    status, _, payload = _json(
        bridge,
        "/api/react",
        method="POST",
        body={"target": "wire-1", "reaction": "x" * 33},
    )
    assert status == 400
    assert "longer than 32" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/react",
        method="POST",
        body={"target": "wire-1", "reaction": "👍", "clear": True},
    )
    assert status == 400


def test_reaction_annotates_matching_history_entry(bridge):
    record = {
        "from": "alice",
        "destination": PEER_HASH,
        "text": "hello",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "wire-1",
    }
    bridge.record_inbound(record)

    updated = bridge.record_reaction(_reaction_record())
    assert updated is not None
    assert updated["reactions"] == [
        {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
    ]

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["reactions"] == [
        {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
    ]
    assert feed["messages"][0]["wire_id"] == "wire-1"
    events = bridge.bus.replay(0)
    assert any(
        e["type"] == "message_updated"
        and e["message"]["reactions"]
        for e in events
    )


def test_reaction_latest_per_reactor_and_clear(bridge):
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )
    bridge.record_reaction(_reaction_record(reaction="👍"))
    bridge.record_reaction(_reaction_record(reaction="❤️", id="react-2"))
    updated = bridge.record_reaction(_reaction_record(reaction="", id="react-3"))
    assert updated is not None
    assert updated["reactions"] == []


def test_reaction_to_unknown_history_entry_is_dropped(bridge):
    assert bridge.record_reaction(_reaction_record()) is None


def test_reactions_and_wire_id_survive_bridge_restart(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "hello",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "wire-1",
            }
        )
        bridge.record_reaction(_reaction_record())
    finally:
        bridge.stop()

    bridge2 = _bridge_with_data_dir(FakeNode(), tmp_path)
    try:
        messages = bridge2.messages()
        assert len(messages) == 1
        assert messages[0]["wire_id"] == "wire-1"
        assert messages[0]["reactions"] == [
            {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
        ]
    finally:
        bridge2.stop()


def test_send_message_records_wire_id(node, tmp_path):
    def send_with_id(peer, text, *, timeout=20.0, discover_timeout=None, in_reply_to=None, expires_at=None):
        return {"ok": True, "echo": text, "server": "tester", "id": "wire-9"}

    node.send_message = send_with_id
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        message, _ = bridge.send_message("alice", "hi")
        assert message["wire_id"] == "wire-9"
    finally:
        bridge.stop()


def _reaction_record(**overrides):
    record = {
        "target": "wire-1",
        "reaction": "👍",
        "from": "alice",
        "destination": PEER_HASH,
        "sent_at": 12.0,
        "received_at": 13.0,
        "id": "react-1",
    }
    record.update(overrides)
    return record


def test_expired_history_is_purged_on_load(node, tmp_path):
    now = time.time()
    gone = _stored_message(1, "gone")
    gone["expires_at"] = now - 10
    staying = _stored_message(2, "staying")
    staying["expires_at"] = now + 3600
    (tmp_path / "messages.json").write_text(
        json.dumps(
            {
                "messages": [gone, staying, _stored_message(3, "plain")],
                "next_message_id": 4,
            }
        ),
        encoding="utf-8",
    )

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        assert [m["text"] for m in bridge.messages(limit=0)] == [
            "staying",
            "plain",
        ]
        # The purge is persisted: the expired entry is gone from disk.
        stored = json.loads((tmp_path / "messages.json").read_text(encoding="utf-8"))
        assert [m["text"] for m in stored["messages"]] == ["staying", "plain"]
    finally:
        bridge.stop()


def test_inbound_message_keeps_expiry(bridge):
    record = {
        "from": "bob",
        "destination": "ef" * 16,
        "text": "read fast",
        "sent_at": 100.0,
        "received_at": 101.0,
        "expires_at": 1893456000.0,
    }
    bridge.record_inbound(record)

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["expires_at"] == 1893456000.0


def test_messages_filters_expired_at_read_time(bridge):
    now = time.time()
    bridge._append_message(
        direction="in",
        peer="ef" * 16,
        peer_name="bob",
        text="gone",
        sent_at=now - 100,
        received_at=now - 100,
        status="received",
        expires_at=now - 10,
    )
    bridge._append_message(
        direction="in",
        peer="ef" * 16,
        peer_name="bob",
        text="staying",
        sent_at=now,
        received_at=now,
        status="received",
        expires_at=now + 3600,
    )
    # The deadline passed while the bridge was running: the read path
    # drops it without waiting for a restart.
    assert [m["text"] for m in bridge.messages(limit=0)] == ["staying"]


def test_send_rejects_bad_expires_in(bridge):
    for bad in (0, -5, "soon", True, 31 * 86400, 10**400):
        status, _, payload = _json(
            bridge,
            "/api/messages",
            method="POST",
            body={"to": "alice", "text": "hi", "expires_in": bad},
        )
        assert status == 400
        assert "expires_in" in payload["error"]["message"]


def test_send_requires_a_target(bridge):
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"text": "hi"}
    )
    assert status == 400
    assert "to" in payload["error"]["message"]


def test_send_with_expires_in_records_deadline(bridge, node):
    before = time.time()
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "vanish soon", "expires_in": 3600},
    )
    assert status == 200
    expires_at = payload["message"]["expires_at"]
    assert before + 3590 <= expires_at <= time.time() + 3610
    assert node.last_expires_at == expires_at

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["expires_at"] == expires_at
