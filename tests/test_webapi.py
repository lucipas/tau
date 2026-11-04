from __future__ import annotations

import http.client
import json
import threading
import time
import urllib.error
import urllib.request

import pytest

from tau.peers import Peer
from tau.webapi import WebBridge, WebBridgeError

PEER_HASH = "ab" * 16


class FakeRegistry:
    def __init__(self, peers):
        self._peers = list(peers)

    def all(self, max_age=None):
        return list(self._peers)


class FakeNode:
    def __init__(self):
        self.name = "tester"
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

    def send_message(self, peer, text, *, timeout=20.0, discover_timeout=None):
        self.sent.append((peer.name, text))
        return {"ok": True, "echo": text, "server": self.name, "_rtt": 0.012}

    def ping(self, peer, *, timeout=15.0, discover_timeout=None):
        return {"ok": True, "pong": True, "server": self.name, "_rtt": 0.004}

    def announce(self):
        self.announced += 1


@pytest.fixture()
def node():
    return FakeNode()


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


def test_send_message_is_recorded_and_published(bridge, node):
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    assert status == 200
    assert payload["message"]["direction"] == "out"
    assert payload["message"]["peer_name"] == "alice"
    assert payload["message"]["rtt_ms"] == 12.0
    assert node.sent == [("alice", "hi")]

    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == ["hi"]

    events = bridge.bus.replay(0)
    assert any(e["type"] == "message" for e in events)


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
