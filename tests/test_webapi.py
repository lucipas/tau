from __future__ import annotations

import hashlib
import http.client
import json
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace

import pytest
import RNS

from tau.allowlist import AllowList, load_allowlist, save_allowlist
from tau.blocklist import BlockList
from tau.config import TauConfig
from tau.groups import GroupStore
from tau.inbox import Inbox
from tau.mailbox import Mailbox
from tau.peers import AmbiguousPeer, Peer, PeerError
from tau.webapi import WebBridge, WebBridgeError, check_web_console_bind

PEER_HASH = "ab" * 16
OTHER_HASH = "cd" * 16
BOB_HASH = "ef" * 16


class FakeRegistry:
    def __init__(self, peers):
        self._peers = list(peers)

    def all(self, max_age=None):
        return list(self._peers)

    def get(self, destination_hash):
        if isinstance(destination_hash, str):
            value = destination_hash.strip()
            if len(value) == 32 and all(
                char in "0123456789abcdefABCDEF" for char in value
            ):
                for peer in self._peers:
                    if peer.hexhash == value.casefold():
                        return peer
                return None
            for peer in self._peers:
                if peer.name.casefold() == value.casefold():
                    return peer
            # Any other hex string (e.g. a full-length destination hash)
            # falls back to main's byte comparison below.
            try:
                destination_hash = bytes.fromhex(value)
            except ValueError:
                return None
        for peer in self._peers:
            if peer.destination_hash == destination_hash:
                return peer
        return None

    def resolve(self, target, *, include_alias=True):
        # Mirror PeerRegistry.resolve for the single-peer fixture: name,
        # alias (only when include_alias), destination hash or hash
        # prefix, with ambiguity rejected. A test may set an alias on
        # the fixture peer to prove a security boundary never resolves
        # by alias; tan.resolve_author calls
        # registry.resolve(value, include_alias=False).
        if not isinstance(target, str):
            raise PeerError("peer target must be a string or bytes")
        value = target.strip()
        if not value:
            return None
        if len(value) == 32 and all(
            char in "0123456789abcdefABCDEF" for char in value
        ):
            return self.get(value)
        if include_alias:
            by_alias = [
                p
                for p in self._peers
                if getattr(p, "alias", None)
                and p.alias.casefold() == value.casefold()
            ]
            if len(by_alias) == 1:
                return by_alias[0]
            if len(by_alias) > 1:
                raise AmbiguousPeer(
                    f"{len(by_alias)} peers share the alias {value!r}; use a destination hash"
                )
        by_name = [p for p in self._peers if p.name.casefold() == value.casefold()]
        if len(by_name) == 1:
            return by_name[0]
        if len(by_name) > 1:
            raise AmbiguousPeer(
                f"{len(by_name)} peers are named {value!r}; use a destination hash"
            )
        if len(value) >= 4:
            by_prefix = [
                p for p in self._peers if p.hexhash.startswith(value.casefold())
            ]
            if len(by_prefix) == 1:
                return by_prefix[0]
            if len(by_prefix) > 1:
                raise AmbiguousPeer(
                    f"{len(by_prefix)} peers match the hash prefix {value!r}"
                )
        return None

    def remove(self, destination_hash):
        for peer in list(self._peers):
            if peer.destination_hash == destination_hash:
                self._peers.remove(peer)
                return peer
        return None

    def save(self):
        return True


class FakeNode:
    def __init__(self, mailbox_path=None, groups_path=None):
        self.name = "tester"
        self.destination_hexhash = "ef" * 16
        self.mailbox = Mailbox(mailbox_path)
        self.inbox = Inbox()
        self.identity = RNS.Identity()
        data_dir = Path(mailbox_path).parent if mailbox_path else Path(".")
        self.config = SimpleNamespace(
            identity_path=data_dir / "identity",
            social_db_path=data_dir / "social.db",
            tan_rules_path=data_dir / "tan-rules.json",
        )
        self._allowlist = AllowList()
        self._allowlist_lock = threading.RLock()
        self.allowlist_path = (
            Path(mailbox_path).with_name("allowlist.json")
            if mailbox_path is not None
            else None
        )
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
        self.group_sent = []
        self.group_refusals = set()
        self.groups = GroupStore(groups_path)
        self.groups.create(
            "crew", [PEER_HASH, BOB_HASH], created_by=PEER_HASH
        )
        self.sent_files = []
        self.announced = 0
        self.flush_calls = []
        self.flush_lock_busy = False
        self.flush_deliver_all = False
        self.flush_deliver_target = True
        self.flush_concurrent_delivery = False
        self.blocklist = None

    def flush_outbox(
        self,
        *,
        timeout=6.0,
        max_peers=None,
        respect_backoff=True,
        to=None,
        message_id=None,
        wait_for_lock=0.0,
    ):
        """Test stand-in for the node's mail sweep: deliver what it is told to.

        A targeted flush removes that entry (delivery); a retry-all
        removes every entry only when ``flush_deliver_all`` is set,
        modelling unreachable peers otherwise.
        """
        self.flush_calls.append(
            {
                "timeout": timeout,
                "max_peers": max_peers,
                "respect_backoff": respect_backoff,
                "to": to,
                "message_id": message_id,
                "wait_for_lock": wait_for_lock,
            }
        )
        if self.flush_lock_busy:
            return {
                "delivered": 0,
                "deposited": 0,
                "received": 0,
                "receipts": 0,
                "lock_busy": True,
            }
        delivered = 0
        if message_id is not None:
            if self.flush_concurrent_delivery:
                # Another round delivered the entry while this flush
                # waited for the mail lock: it leaves the queue, but
                # this flush reports no delivery of its own.
                self.mailbox.remove_outbox(message_id)
            elif self.flush_deliver_target and self.mailbox.remove_outbox(
                message_id
            ):
                delivered = 1
        elif self.flush_deliver_all:
            for entry in self.mailbox.outbox_list():
                if self.mailbox.remove_outbox(entry.id):
                    delivered += 1
        return {
            "delivered": delivered,
            "deposited": 0,
            "received": 0,
            "receipts": 0,
            "lock_busy": False,
        }


    def set_blocklist(self, blocklist):
        self.blocklist = blocklist

    @property
    def allowlist(self):
        return self._allowlist

    def set_allowlist(self, allowlist):
        # Mirrors TauNode.set_allowlist: the running node's in-memory
        # list and the file change together, persist first.
        with self._allowlist_lock:
            save_allowlist(self.allowlist_path, allowlist)
            self._allowlist = allowlist

    def update_allowlist(self, transform):
        # Mirrors TauNode.update_allowlist: re-read the saved list so a
        # change another process saved is preserved, refuse a corrupt
        # file, and serialise the whole read-modify-write.
        with self._allowlist_lock:
            current = load_allowlist(self.allowlist_path)
            updated = transform(current)
            self.set_allowlist(updated)
            return updated

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

    def send_group_message(
        self, group, text, *, timeout=20.0, discover_timeout=None, queue=True
    ):
        self.group_sent.append((group["name"], text))
        names = {PEER_HASH: "alice", BOB_HASH: "bob"}
        results = []
        for member in group["members"]:
            if member in self.group_refusals:
                results.append(
                    {
                        "to": member,
                        "to_name": names.get(member, ""),
                        "ok": False,
                        "error": "not_allowed",
                        "id": f"wire-{member[:8]}",
                    }
                )
            elif member == BOB_HASH:
                # Offline member: queued in the outbox under an entry id.
                results.append(
                    {
                        "to": member,
                        "to_name": "bob",
                        "queued": True,
                        "id": "mail-bob-1",
                        "expires_at": 0,
                    }
                )
            else:
                results.append(
                    {
                        "to": member,
                        "to_name": names.get(member, ""),
                        "delivered": True,
                        "id": f"wire-{member[:8]}",
                        "rtt": 0.009,
                    }
                )
        all_ok = all(
            result.get("delivered") or result.get("queued") for result in results
        )
        return {
            "ok": all_ok,
            "v": 1,
            "group": group["id"],
            "name": group["name"],
            "results": results,
        }


    def send_file(
        self,
        target,
        path,
        *,
        timeout=300.0,
        discover_timeout=None,
        progress_callback=None,
    ):
        import hashlib
        from pathlib import Path

        data = Path(path).read_bytes()
        peer = target if isinstance(target, Peer) else self.resolve_peer(target)
        self.sent_files.append((peer.name, Path(path).name, data))
        return {
            "ok": True,
            "id": f"{len(self.sent_files):032d}",
            "name": Path(path).name,
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "to": peer.hexhash,
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


def _seed_alias_registry(node, tmp_path):
    """Swap the fake registry for a real one the alias endpoint can use."""
    from tau.peers import PeerRegistry

    registry = PeerRegistry(tmp_path / "peers.json")
    registry.upsert(
        bytes.fromhex(PEER_HASH), b"\x01" * 64, name="alice", hops=1
    )
    registry.upsert(
        bytes.fromhex(OTHER_HASH), b"\x02" * 64, name="bob", hops=1
    )
    node.registry = registry
    return registry


def test_peers_alias_endpoint_sets_lists_and_clears(bridge, node, tmp_path):
    _seed_alias_registry(node, tmp_path)

    status, _, payload = _json(
        bridge,
        "/api/peers/alias",
        method="POST",
        body={"destination": PEER_HASH, "alias": "laptop"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["peer"]["alias"] == "laptop"
    assert payload["peer"]["name"] == "alice"

    # The alias is visible in the peer list, so the console can show it.
    _, _, listed = _json(bridge, "/api/peers")
    entry = next(
        item
        for item in listed["peers"]
        if item["destination"] == PEER_HASH
    )
    assert entry["alias"] == "laptop"

    # An empty alias clears it, restoring the announced name as the label.
    status, _, payload = _json(
        bridge,
        "/api/peers/alias",
        method="POST",
        body={"destination": PEER_HASH, "alias": ""},
    )
    assert status == 200
    assert payload["peer"]["alias"] is None


def test_peers_alias_response_keeps_blocked_flag(bridge, node, tmp_path):
    """The alias response replaces the console's peer record, so it must
    carry the same annotations as the peer list — a bare local_dict
    would silently drop the blocked badge until the next snapshot."""
    from tau.blocklist import BlockList

    _seed_alias_registry(node, tmp_path)
    node.blocklist = BlockList(blocked=frozenset({PEER_HASH}))
    try:
        status, _, payload = _json(
            bridge,
            "/api/peers/alias",
            method="POST",
            body={"destination": PEER_HASH, "alias": "laptop"},
        )
    finally:
        node.blocklist = BlockList()
    assert status == 200
    assert payload["peer"]["alias"] == "laptop"
    assert payload["peer"]["blocked"] is True


def test_peers_alias_waits_for_node_peer_save_lock(bridge, node, tmp_path):
    """An alias edit must not race the node's peer save: _save_peers
    refreshes aliases from disk before rewriting the file, so an edit
    landing between the refresh and the save would be lost. The
    endpoint therefore holds the node's peer-save lock across the edit
    and its save, like peers_verify does."""
    _seed_alias_registry(node, tmp_path)
    node._save_lock = threading.Lock()
    node._save_lock.acquire()
    try:
        done = threading.Event()
        result: dict = {}

        def edit():
            result["payload"] = bridge.set_peer_alias(PEER_HASH, "laptop")
            done.set()

        worker = threading.Thread(target=edit, daemon=True)
        worker.start()
        # The edit is blocked on the lock: it must not complete (and
        # must not have written the alias) while a peer save holds it.
        assert not done.wait(timeout=1.0)
        assert worker.is_alive()
        assert node.registry.get(PEER_HASH).alias is None
    finally:
        node._save_lock.release()
    worker.join(timeout=5.0)
    assert done.is_set()
    assert result["payload"]["alias"] == "laptop"
    assert node.registry.get(PEER_HASH).alias == "laptop"


def test_peers_alias_endpoint_rejects_bad_requests(bridge, node, tmp_path):
    registry = _seed_alias_registry(node, tmp_path)
    registry.set_alias(bytes.fromhex(OTHER_HASH), "laptop")

    status, _, payload = _json(
        bridge,
        "/api/peers/alias",
        method="POST",
        body={"destination": "ff" * 16, "alias": "ghost"},
    )
    assert status == 400
    assert "unknown peer" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/peers/alias", method="POST", body={"alias": "laptop"}
    )
    assert status == 400
    assert "destination" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/peers/alias",
        method="POST",
        body={"destination": PEER_HASH, "alias": 7},
    )
    assert status == 400
    assert "alias" in payload["error"]["message"]

    # An alias another peer already uses is a clash, not a silent steal.
    status, _, payload = _json(
        bridge,
        "/api/peers/alias",
        method="POST",
        body={"destination": PEER_HASH, "alias": "laptop"},
    )
    assert status == 400
    assert "already used" in payload["error"]["message"]

    # A hex-like alias could never reliably select its owner.
    status, _, payload = _json(
        bridge,
        "/api/peers/alias",
        method="POST",
        body={"destination": PEER_HASH, "alias": "abcdef"},
    )
    assert status == 400
    assert "looks like a destination hash" in payload["error"]["message"]


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


def test_peers_verify_via_api(tmp_path):
    import RNS

    from tau.peers import PeerRegistry, safety_number

    identity = RNS.Identity()
    node = FakeNode(tmp_path / "mailbox.json")
    node.identity = identity
    registry = PeerRegistry(tmp_path / "peers.json")
    peer_key = b"\x03" * 64
    registry.upsert(bytes.fromhex(PEER_HASH), peer_key, name="alice", hops=1)
    node.registry = registry

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A key-change warning is answered by verifying, not left to warn.
        peer = registry.get(PEER_HASH)
        assert peer is not None
        peer.key_change_at = 1500.0

        status, _, payload = _json(
            bridge, "/api/peers/verify", method="POST", body={"target": "alice"}
        )
        assert status == 200
        verify = payload["verify"]
        assert verify["peer"] == PEER_HASH
        assert verify["name"] == "alice"
        assert verify["safety_number"] == safety_number(
            identity.get_public_key(), peer_key
        )
        assert verify["reverified"] is False
        assert verify["fingerprint_changed"] is False

        status, _, payload = _json(bridge, "/api/peers")
        entry = [
            item for item in payload["peers"] if item["destination"] == PEER_HASH
        ][0]
        assert entry["verified"] is True
        assert entry["key_change_at"] is None

        # Re-verifying under a different local identity changes the
        # pairwise number, and the response says so.
        node.identity = RNS.Identity()
        status, _, payload = _json(
            bridge, "/api/peers/verify", method="POST", body={"target": PEER_HASH}
        )
        assert status == 200
        assert payload["verify"]["reverified"] is True
        assert payload["verify"]["fingerprint_changed"] is True

        # Unknown and blank targets are rejected.
        status, _, payload = _json(
            bridge, "/api/peers/verify", method="POST", body={"target": "nobody"}
        )
        assert status == 400
        assert "no peer matches" in payload["error"]["message"]

        status, _, payload = _json(
            bridge, "/api/peers/verify", method="POST", body={"target": "  "}
        )
        assert status == 400
        assert "target" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_peers_safety_number_shows_without_verifying(tmp_path):
    import RNS

    from tau.peers import PeerRegistry, safety_number

    identity = RNS.Identity()
    node = FakeNode(tmp_path / "mailbox.json")
    node.identity = identity
    registry = PeerRegistry(tmp_path / "peers.json")
    peer_key = b"\x03" * 64
    registry.upsert(bytes.fromhex(PEER_HASH), peer_key, name="alice", hops=1)
    node.registry = registry

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A key-change warning is still open before the number is shown.
        peer = registry.get(PEER_HASH)
        assert peer is not None
        peer.key_change_at = 1500.0

        status, _, payload = _json(
            bridge,
            "/api/peers/safety-number",
            method="POST",
            body={"target": "alice"},
        )
        assert status == 200
        shown = payload["safety_number"]
        assert shown["peer"] == PEER_HASH
        assert shown["name"] == "alice"
        assert shown["safety_number"] == safety_number(
            identity.get_public_key(), peer_key
        )

        # Showing the number records nothing: the peer stays
        # unverified and the warning stays up until the owner
        # confirms the match.
        status, _, payload = _json(bridge, "/api/peers")
        entry = [
            item for item in payload["peers"] if item["destination"] == PEER_HASH
        ][0]
        assert entry["verified"] is False
        assert entry["key_change_at"] == 1500.0

        # Unknown and blank targets are rejected.
        status, _, payload = _json(
            bridge,
            "/api/peers/safety-number",
            method="POST",
            body={"target": "nobody"},
        )
        assert status == 400
        assert "no peer matches" in payload["error"]["message"]

        status, _, payload = _json(
            bridge,
            "/api/peers/safety-number",
            method="POST",
            body={"target": "  "},
        )
        assert status == 400
        assert "target" in payload["error"]["message"]
    finally:
        bridge.stop()


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


def test_send_group_message_records_one_entry_per_leg(bridge, node):
    group_id = node.groups.list_groups()[0]["id"]
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"group": "crew", "text": "hi all"}
    )
    assert status == 200
    assert payload["ok"] is True
    assert node.group_sent == [("crew", "hi all")]
    messages = payload["messages"]
    assert len(messages) == 2
    assert all(m["direction"] == "out" for m in messages)
    assert all(m["group"] == group_id for m in messages)
    assert all(m["group_name"] == "crew" for m in messages)
    assert [m["peer"] for m in messages] == [PEER_HASH, BOB_HASH]
    assert [m["peer_name"] for m in messages] == ["alice", "bob"]
    # One leg delivered, one queued: each keeps its own status and id.
    assert [m["status"] for m in messages] == ["delivered", "queued"]
    assert messages[0]["rtt_ms"] == 9.0
    assert messages[0]["wire_id"] == f"wire-{PEER_HASH[:8]}"
    assert messages[1]["mail_id"] == "mail-bob-1"
    # The queued leg is named by its outbox id, the same convention
    # as a queued peer DM: a receipt names the wire id, which for
    # queued mail is the outbox id.
    assert messages[1]["wire_id"] == "mail-bob-1"

    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == ["hi all", "hi all"]

    # A delivery report for the queued leg flips just that entry.
    bridge.record_delivery("mail-bob-1")
    _, _, feed = _json(bridge, "/api/messages")
    assert [m["status"] for m in feed["messages"]] == ["delivered", "delivered"]

    events = bridge.bus.replay(0)
    assert sum(1 for e in events if e["type"] == "message") == 2


def test_send_group_message_resolves_by_id_and_prefix(bridge, node):
    group_id = node.groups.list_groups()[0]["id"]
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": group_id, "text": "by id"},
    )
    assert status == 200
    assert len(payload["messages"]) == 2
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": group_id[:8], "text": "by prefix"},
    )
    assert status == 200
    assert len(payload["messages"]) == 2
    assert node.group_sent == [("crew", "by id"), ("crew", "by prefix")]


def test_send_group_message_sees_groups_changed_by_another_process(tmp_path):
    # The serving bridge holds a cached group store; another process
    # (a one-shot `tau group create`, or an inbound group message)
    # can create or delete a group on disk meanwhile. The send must
    # see the persisted state, not the stale cache.
    groups_path = tmp_path / "groups.json"
    node = FakeNode(tmp_path / "mailbox.json", groups_path=groups_path)
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        other = GroupStore(groups_path)
        other.create("late", [PEER_HASH, BOB_HASH], created_by=PEER_HASH)
        status, _, payload = _json(
            bridge,
            "/api/messages",
            method="POST",
            body={"group": "late", "text": "hi late"},
        )
        assert status == 200
        assert len(payload["messages"]) == 2
        assert node.group_sent == [("late", "hi late")]

        assert other.delete(other.find("late")["id"]) is True
        status, _, payload = _json(
            bridge,
            "/api/messages",
            method="POST",
            body={"group": "late", "text": "hi again"},
        )
        assert status == 404
        assert "unknown group" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_send_group_message_failed_leg_is_recorded(bridge, node):
    node.group_refusals = {BOB_HASH}
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"group": "crew", "text": "hi all"}
    )
    assert status == 200
    # The fan-out is partial: the response says so, and the refused
    # leg is visible in the feed instead of silently dropped.
    assert payload["ok"] is False
    assert [m["status"] for m in payload["messages"]] == ["delivered", "failed"]
    assert payload["messages"][1]["peer_name"] == "bob"
    # A refused leg carries no wire id: the member never saw that
    # message, so no reply may be quoted against it.
    assert "wire_id" not in payload["messages"][1]
    assert payload["response"]["results"][1]["error"] == "not_allowed"


def test_send_group_message_validation(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": "nope", "text": "hi"},
    )
    assert status == 404
    assert "unknown group" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "group": "crew", "text": "hi"},
    )
    assert status == 400
    assert "not both" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": "crew", "text": "hi", "reply_to": "a" * 8},
    )
    assert status == 400
    assert "reply_to" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": "crew", "text": "hi", "expires_in": 60},
    )
    assert status == 400
    assert "expires_in" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"group": "crew", "text": " "}
    )
    assert status == 400
    assert node.group_sent == []


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


def test_api_groups_create_adds_creator_and_lists_group(
    bridge, node, tmp_path
):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    status, _, payload = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )
    assert status == 200
    assert payload["ok"] is True
    group = payload["group"]
    assert group["name"] == "weekend"
    # The creator is always a member, even when the caller omits it.
    assert [member["hash"] for member in group["members"]] == sorted(
        [PEER_HASH, node.destination_hexhash]
    )
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 1
    assert listed["groups"][0]["id"] == group["id"]


def test_api_groups_create_is_idempotent(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    first = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )[2]["group"]
    status, _, payload = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )
    assert status == 200
    assert payload["group"]["id"] == first["id"]
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 1


def test_api_groups_create_rejects_unknown_member(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    status, _, payload = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [OTHER_HASH]},
    )
    assert status == 400
    assert "unknown peer" in payload["error"]["message"]
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 0


def test_api_groups_create_rejects_bad_input(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    cases = [
        ({}, "field 'name' is required"),
        ({"name": "  ", "members": [PEER_HASH]}, "field 'name' is required"),
        ({"name": "weekend"}, "field 'members' must be a non-empty list"),
        ({"name": "weekend", "members": []}, "field 'members' must be a non-empty list"),
        (
            {"name": "weekend", "members": ["not-a-hash"]},
            "unknown peer",
        ),
        (
            {"name": "weekend", "members": [node.destination_hexhash]},
            "a group needs at least one other peer",
        ),
    ]
    for body, message in cases:
        status, _, payload = _json(
            bridge, "/api/groups", method="POST", body=body
        )
        assert status == 400
        assert message in payload["error"]["message"]
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 0


def test_api_groups_delete_removes_group(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    created = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )[2]["group"]
    status, _, payload = _json(
        bridge,
        "/api/groups/delete",
        method="POST",
        body={"group": "weekend"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["deleted"] == {"id": created["id"], "name": "weekend"}
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 0


def test_api_groups_delete_sees_group_created_by_another_store(
    bridge, node, tmp_path
):
    from tau.groups import GroupStore

    path = tmp_path / "groups.json"
    node.groups = GroupStore(path)
    # A separate process (e.g. `tau group create`) writes the group.
    other = GroupStore(path)
    created = other.create(
        "weekend", [PEER_HASH, node.destination_hexhash], created_by=node.destination_hexhash
    )
    status, _, payload = _json(
        bridge,
        "/api/groups/delete",
        method="POST",
        body={"group": "weekend"},
    )
    assert status == 200
    assert payload["deleted"] == {"id": created["id"], "name": "weekend"}


def test_api_groups_delete_unknown_group_is_400(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    status, _, payload = _json(
        bridge, "/api/groups/delete", method="POST", body={"group": "nope"}
    )
    assert status == 400
    assert "nope" in payload["error"]["message"]
    status, _, payload = _json(
        bridge, "/api/groups/delete", method="POST", body={}
    )
    assert status == 400
    assert payload["error"]["message"] == "field 'group' is required"


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


def test_tan_verify_via_api(tmp_path):
    import sqlite3

    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, identity, "first post", name="tester", now=1000)
        second = create_post(conn, identity, "second post", name="tester", now=1001)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/verify")
        assert status == 200
        assert payload["ok"] is True
        assert payload["entries"] == 2
        assert payload["authors"] == 1
        assert payload["unlinked"] == 0
        assert payload["problems"] == []

        # Tampering with a stored post breaks its hash, which the audit
        # must report without flagging the store's other entries.
        conn = sqlite3.connect(node.config.social_db_path)
        try:
            conn.execute(
                "UPDATE posts SET text = 'tampered' WHERE hash = ?",
                (second["hash"],),
            )
            conn.commit()
        finally:
            conn.close()

        status, _, payload = _json(bridge, "/api/tan/verify")
        assert status == 200
        assert payload["ok"] is False
        assert payload["entries"] == 2
        assert len(payload["problems"]) == 1
        assert payload["problems"][0]["hash"] == second["hash"]
        assert "hash does not match" in payload["problems"][0]["problem"]
    finally:
        bridge.stop()


def test_tan_verify_without_a_store_is_an_error(tmp_path):
    from tau.config import TauConfig

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/verify")
        assert status == 400
        assert "no Tan store" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_verify_rejects_a_second_concurrent_audit(tmp_path, monkeypatch):
    import threading

    import RNS

    from tau.config import TauConfig
    from tau.tangents import tan as tan_module
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, identity, "first post", name="tester", now=1000)
    finally:
        conn.close()

    # Hold the first audit inside verify_store so a second request
    # arrives while it is still running.
    entered = threading.Event()
    release = threading.Event()
    real_verify = tan_module.verify_store

    def blocking_verify(conn):
        entered.set()
        assert release.wait(timeout=10)
        return real_verify(conn)

    monkeypatch.setattr(tan_module, "verify_store", blocking_verify)

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        results = {}

        def first_audit():
            results["first"] = _json(bridge, "/api/tan/verify")

        worker = threading.Thread(target=first_audit, daemon=True)
        worker.start()
        assert entered.wait(timeout=10)

        status, _, payload = _json(bridge, "/api/tan/verify")
        assert status == 429
        assert "already running" in payload["error"]["message"]

        release.set()
        worker.join(timeout=10)
        status, _, payload = results["first"]
        assert status == 200
        assert payload["ok"] is True
    finally:
        release.set()
        bridge.stop()


def test_tan_suggest_via_api(tmp_path):
    import RNS

    from tau.blocklist import BlockList
    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bob = RNS.Identity()
    carol = RNS.Identity()
    dave = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        bob_post = create_post(conn, bob, "bob posts", name="bob", now=1000)
        carol_post = create_post(conn, carol, "carol posts", name="carol", now=1001)
        dave_post = create_post(conn, dave, "dave posts", name="dave", now=1002)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # No follows yet and no likes: equal scores, newest post first.
        status, _, payload = _json(bridge, "/api/tan/suggest")
        assert status == 200
        assert [item["author"] for item in payload["suggestions"]] == [
            dave_post["author"],
            carol_post["author"],
            bob_post["author"],
        ]
        assert payload["count"] == 3
        assert payload["suggestions"][0]["name"] == "dave"
        assert payload["suggestions"][0]["posts"] == 1

        status, _, payload = _json(bridge, "/api/tan/suggest?limit=1")
        assert status == 200
        assert payload["count"] == 1
        assert payload["suggestions"][0]["author"] == dave_post["author"]

        # Following bob removes him from the suggestions.
        status, _, _ = _json(
            bridge,
            "/api/tan/follow",
            method="POST",
            body={"target": bob_post["author"]},
        )
        assert status == 200
        status, _, payload = _json(bridge, "/api/tan/suggest")
        assert [item["author"] for item in payload["suggestions"]] == [
            dave_post["author"],
            carol_post["author"],
        ]

        # A blocked author is never suggested; a failed-closed list
        # suggests nobody at all.
        node.blocklist = BlockList(blocked=frozenset({dave_post["author"]}))
        status, _, payload = _json(bridge, "/api/tan/suggest")
        assert [item["author"] for item in payload["suggestions"]] == [
            carol_post["author"]
        ]
        node.blocklist = BlockList(blocked=frozenset(), block_all=True)
        status, _, payload = _json(bridge, "/api/tan/suggest")
        assert payload["suggestions"] == []
        assert payload["count"] == 0
    finally:
        bridge.stop()


def test_snapshot_counts_tan_posts_from_the_store(tmp_path):
    """Regression test for the Codex P2 on PR #180: snapshot
    ``social.posts`` counted the session-scoped post-event buffer, so
    a node with posts already on disk reported 0. It must count the
    store instead."""
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        keeper = create_post(conn, identity, "keeper", name="tester", now=1000)
        doomed = create_post(conn, identity, "doomed", name="tester", now=1001)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    # A like entry and an author delete land in the same table; neither
    # is a readable post.
    node.app.react(keeper["hash"], liked=True)
    node.app.delete(doomed["hash"])

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # Nothing arrived over the wire this session: the event buffer
        # is empty, so a buffer count would say 0.
        assert len(bridge.tan_posts(limit=0)) == 0
        assert bridge.snapshot()["social"] == {"enabled": True, "posts": 1}
    finally:
        bridge.stop()


def test_peers_block_unblock_via_api(tmp_path):
    from tau.config import TauConfig

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # Block by name; the file and the node's cached list both update,
        # and the peers snapshot carries the new blocked flag.
        status, _, payload = _json(
            bridge, "/api/peers/block", method="POST", body={"target": "alice"}
        )
        assert status == 200
        assert payload["block"]["peer"]["destination"] == PEER_HASH
        assert payload["block"]["peer"]["name"] == "alice"
        assert payload["block"]["blocked"] is True
        assert payload["block"]["changed"] is True
        assert node.blocklist is not None
        assert node.blocklist.is_blocked(PEER_HASH)
        assert json.loads((tmp_path / "blocklist.json").read_text()) == {
            "blocked": [PEER_HASH]
        }
        status, _, payload = _json(bridge, "/api/peers")
        assert status == 200
        assert payload["peers"][0]["blocked"] is True

        # Re-blocking is a no-op.
        status, _, payload = _json(
            bridge, "/api/peers/block", method="POST", body={"target": "alice"}
        )
        assert status == 200
        assert payload["block"]["changed"] is False
        assert payload["block"]["blocked"] is True

        # Unblock by hash prefix.
        status, _, payload = _json(
            bridge,
            "/api/peers/unblock",
            method="POST",
            body={"target": PEER_HASH[:8]},
        )
        assert status == 200
        assert payload["block"]["blocked"] is False
        assert payload["block"]["changed"] is True
        assert not node.blocklist.is_blocked(PEER_HASH)
        status, _, payload = _json(bridge, "/api/peers")
        assert payload["peers"][0]["blocked"] is False

        # Unblocking again is a no-op.
        status, _, payload = _json(
            bridge, "/api/peers/unblock", method="POST", body={"target": "alice"}
        )
        assert status == 200
        assert payload["block"]["changed"] is False

        # Unknown peers and missing targets are rejected.
        status, _, payload = _json(
            bridge, "/api/peers/block", method="POST", body={"target": "nobody"}
        )
        assert status == 400
        status, _, payload = _json(
            bridge, "/api/peers/block", method="POST", body={}
        )
        assert status == 400
    finally:
        bridge.stop()


def test_peers_forget_via_api(tmp_path):
    node = FakeNode(tmp_path / "mailbox.json")
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge, "/api/peers/forget", method="POST", body={"target": "alice"}
        )
        assert status == 200
        assert payload["forget"]["peer"]["destination"] == PEER_HASH
        assert payload["forget"]["peer"]["name"] == "alice"
        assert payload["forget"]["queued_messages"] == 0
        # Gone from the registry and from the snapshot, and a
        # peer_removed event went out so other consoles drop it too.
        assert node.registry.get(PEER_HASH) is None
        status, _, payload = _json(bridge, "/api/peers")
        assert status == 200
        assert payload["peers"] == []
        assert [
            event
            for event in bridge.bus.replay(0)
            if event.get("type") == "peer_removed"
            and event.get("destination") == PEER_HASH
        ]
        # Forgetting again is a 400: the peer is unknown now.
        status, _, payload = _json(
            bridge, "/api/peers/forget", method="POST", body={"target": "alice"}
        )
        assert status == 400
        # Missing and non-boolean force are rejected.
        status, _, payload = _json(
            bridge, "/api/peers/forget", method="POST", body={}
        )
        assert status == 400
        status, _, payload = _json(
            bridge,
            "/api/peers/forget",
            method="POST",
            body={"target": "alice", "force": "yes"},
        )
        assert status == 400
    finally:
        bridge.stop()


def test_peers_forget_refuses_queued_mail_unless_forced(tmp_path):
    node = FakeNode(tmp_path / "mailbox.json")
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        node.mailbox.enqueue(to=bytes.fromhex(PEER_HASH), text="queued mail")
        # Refused while mail is queued, and the peer stays.
        status, _, payload = _json(
            bridge, "/api/peers/forget", method="POST", body={"target": "alice"}
        )
        assert status == 400
        assert "queued message" in payload["error"]["message"]
        assert node.registry.get(PEER_HASH) is not None
        # force forgets anyway and reports the stranded count.
        status, _, payload = _json(
            bridge,
            "/api/peers/forget",
            method="POST",
            body={"target": "alice", "force": True},
        )
        assert status == 200
        assert payload["forget"]["queued_messages"] == 1
        assert node.registry.get(PEER_HASH) is None
    finally:
        bridge.stop()


def test_peers_forget_refuses_pending_acks_even_when_forced(tmp_path):
    node = FakeNode(tmp_path / "mailbox.json")
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        node.mailbox.add_pending_acks(PEER_HASH, ["a" * 32])
        status, _, payload = _json(
            bridge, "/api/peers/forget", method="POST", body={"target": "alice"}
        )
        assert status == 400
        assert "acknowledgements owed" in payload["error"]["message"]
        # force never overrides the ack guard.
        status, _, payload = _json(
            bridge,
            "/api/peers/forget",
            method="POST",
            body={"target": "alice", "force": True},
        )
        assert status == 400
        assert node.registry.get(PEER_HASH) is not None
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


def test_peers_block_corrupt_list_refuses(tmp_path):
    from tau.config import TauConfig

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    (tmp_path / "blocklist.json").write_text("{not json", encoding="utf-8")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge, "/api/peers/block", method="POST", body={"target": "alice"}
        )
        assert status == 503
        assert "unusable" in payload["error"]["message"]
        # Nothing was persisted over the corrupt file, and the running
        # node fails closed until the owner repairs it.
        assert (tmp_path / "blocklist.json").read_text() == "{not json"
        assert node.blocklist is not None
        assert node.blocklist.block_all

        # A node still fail-closed in memory accepts edits again once the
        # owner repairs the file — and the repair clears the cached
        # block-all, so enforcement matches the file.
        node.blocklist = BlockList(block_all=True)
        (tmp_path / "blocklist.json").write_text(
            '{"blocked": []}', encoding="utf-8"
        )
        status, _, payload = _json(
            bridge, "/api/peers/block", method="POST", body={"target": "alice"}
        )
        assert status == 200
        assert payload["block"]["blocked"] is True
        assert node.blocklist is not None
        assert not node.blocklist.block_all
        assert node.blocklist.is_blocked(PEER_HASH)
        # The saved file holds the new entry, not just the cache.
        assert json.loads((tmp_path / "blocklist.json").read_text()) == {
            "blocked": [PEER_HASH]
        }
    finally:
        bridge.stop()


def test_peers_block_keeps_cli_edits(tmp_path):
    from tau.config import TauConfig

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    # The CLI blocked someone else while this node was running; the
    # node's cached list predates that edit.
    (tmp_path / "blocklist.json").write_text(
        json.dumps({"blocked": [OTHER_HASH]}), encoding="utf-8"
    )
    node.blocklist = BlockList()
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge, "/api/peers/block", method="POST", body={"target": "alice"}
        )
        assert status == 200
        # The CLI's entry survives: the edit is derived from the file,
        # not from the stale cache.
        assert json.loads((tmp_path / "blocklist.json").read_text()) == {
            "blocked": sorted([OTHER_HASH, PEER_HASH])
        }
        assert node.blocklist is not None
        assert node.blocklist.is_blocked(OTHER_HASH)
        assert node.blocklist.is_blocked(PEER_HASH)
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


def test_tan_edit_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, feed, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "orginal typo", name="tester", now=1000)
        create_post(conn, identity, "keeper via api", name="tester", now=1001)
        doomed = create_post(conn, identity, "delete me first", name="tester", now=1002)
        foreign = create_post(conn, other, "not yours", name="other", now=1003)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A short prefix resolves the same way the delete endpoint does.
        status, _, payload = _json(
            bridge,
            "/api/tan/edit",
            method="POST",
            body={"target": post["hash"][:16], "text": "original, fixed"},
        )
        assert status == 200
        assert payload["edit"]["kind"] == "edit"
        assert payload["edit"]["target"] == post["hash"]

        # The timeline shows the replacement text and marks the edit.
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["original, fixed"]["edited"] is True
        assert by_text["original, fixed"]["own"] is True
        assert "orginal typo" not in by_text

        # A second edit replaces the first; the latest edit wins.
        status, _, payload = _json(
            bridge,
            "/api/tan/edit",
            method="POST",
            body={"target": post["hash"], "text": "original, fixed again"},
        )
        assert status == 200
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert [item["text"] for item in payload["posts"]] == [
            "not yours",
            "delete me first",
            "keeper via api",
            "original, fixed again",
        ]

        # Someone else's post cannot be edited through the API.
        status, _, payload = _json(
            bridge,
            "/api/tan/edit",
            method="POST",
            body={"target": foreign["hash"], "text": "hijack"},
        )
        assert status == 400
        assert "did not author" in payload["error"]["message"]

        # Empty replacement text is a bad request.
        status, _, payload = _json(
            bridge,
            "/api/tan/edit",
            method="POST",
            body={"target": post["hash"], "text": "   "},
        )
        assert status == 400
        assert "field 'text' is required" in payload["error"]["message"]

        # A post this node has never seen cannot be edited.
        status, _, payload = _json(
            bridge,
            "/api/tan/edit",
            method="POST",
            body={"target": "cd" * 32, "text": "ghost"},
        )
        assert status == 400
        assert "unknown post" in payload["error"]["message"]

        # A deleted post cannot be edited either.
        status, _, payload = _json(
            bridge,
            "/api/tan/delete",
            method="POST",
            body={"target": doomed["hash"]},
        )
        assert status == 200
        status, _, payload = _json(
            bridge,
            "/api/tan/edit",
            method="POST",
            body={"target": doomed["hash"], "text": "too late"},
        )
        assert status == 400
        assert "has been deleted" in payload["error"]["message"]
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
        "post",
        "edit",
        "edit",
        "delete",
    ]



def test_tan_export_import_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path / "one", name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(conn, identity, "mine one", name="tester", now=1000)
        create_post(conn, identity, "mine two", name="tester", now=1001)
        create_post(conn, other, "not mine", name="other", now=1002)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The default scope is the node's own chain only.
        status, _, payload = _json(bridge, "/api/tan/export")
        assert status == 200
        assert payload["format"] == "tau-tan-export"
        assert payload["version"] == 1
        assert [entry["text"] for entry in payload["entries"]] == [
            "mine one",
            "mine two",
        ]
        assert payload["authors"] == [mine["author"]]

        status, _, payload = _json(bridge, "/api/tan/export?scope=all")
        assert status == 200
        assert len(payload["entries"]) == 3
        assert len(payload["authors"]) == 2
        assert mine["author"] in payload["authors"]

        status, _, payload = _json(bridge, "/api/tan/export?scope=bogus")
        assert status == 400
        assert "scope" in payload["error"]["message"]

        # A fresh node restores the whole document, then reports the
        # same entries as already present on a second import.
        node2 = FakeNode()
        node2.config = TauConfig.resolve(data_dir=tmp_path / "two", name="tester2")
        node2.app = TanApp(node2.config, RNS.Identity(), name="tester2", gossip=False)
        bridge2 = WebBridge(node2, host="127.0.0.1", port=0)
        bridge2.start()
        try:
            _, _, document = _json(bridge, "/api/tan/export?scope=all")
            status, _, payload = _json(
                bridge2, "/api/tan/import", method="POST", body=document
            )
            assert status == 200
            assert payload["import"] == {
                "imported": 3,
                "present": 0,
                "skipped": 0,
                "errors": [],
            }

            status, _, payload = _json(
                bridge2, "/api/tan/import", method="POST", body=document
            )
            assert status == 200
            assert payload["import"]["imported"] == 0
            assert payload["import"]["present"] == 3

            # A document with the wrong format is rejected, not stored.
            status, _, payload = _json(
                bridge2, "/api/tan/import", method="POST", body={"format": "nope"}
            )
            assert status == 400
            assert "unsupported export format" in payload["error"]["message"]

            # One entry with a tampered body fails verification and is
            # skipped; the rest of the document still imports. Drop the
            # store's copy of the tampered entry first so it is new here
            # rather than already present.
            tampered = json.loads(json.dumps(document))
            tampered["entries"][0]["text"] = "forged"
            conn2 = init_db(node2.config.social_db_path)
            try:
                conn2.execute(
                    "DELETE FROM posts WHERE hash = ?",
                    (tampered["entries"][0]["hash"],),
                )
                conn2.commit()
            finally:
                conn2.close()
            status, _, payload = _json(
                bridge2, "/api/tan/import", method="POST", body=tampered
            )
            assert status == 200
            assert payload["import"]["imported"] == 0
            assert payload["import"]["present"] == 2
            assert payload["import"]["skipped"] == 1
            assert len(payload["import"]["errors"]) == 1

            # An export document is bigger than the 32 KiB cap for
            # ordinary actions; the import route accepts it.
            big = json.loads(json.dumps(document))
            big["entries"] = big["entries"] * 100
            assert len(json.dumps(big).encode("utf-8")) > 32 * 1024
            status, _, payload = _json(
                bridge2, "/api/tan/import", method="POST", body=big
            )
            assert status == 200
            assert payload["import"]["skipped"] == 0
        finally:
            bridge2.stop()
    finally:
        bridge.stop()


def _likers_setup(tmp_path):
    """A node with one post by bob, liked by alice, carol and dave.

    Returns (node, post, authors) where authors maps a label to the
    liker's author hex. Carol unlikes at the end, so the visible likers
    are dave (1003) then alice (1001).
    """
    import RNS

    from tau.config import TauConfig
    from tau.identity import destination_hash
    from tau.tangents.tan import create_post, create_reaction, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    alice = RNS.Identity()
    bob = RNS.Identity()
    carol = RNS.Identity()
    dave = RNS.Identity()
    # A liker the node knows as a peer resolves to the registry name,
    # not the name on their stored posts.
    node.registry._peers.append(
        Peer(
            destination_hash=destination_hash(alice),
            name="alice-on-record",
            public_key=b"\x02" * 32,
            first_seen=1000.0,
            last_seen=time.time(),
            hops=1,
        )
    )
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, bob, "likeable via api", name="bob", now=1000)
        create_reaction(conn, alice, post["hash"], liked=True, name="alice", now=1001)
        create_reaction(conn, carol, post["hash"], liked=True, name="carol", now=1002)
        create_reaction(conn, dave, post["hash"], liked=True, name="dave", now=1003)
        # Carol's unlike supersedes her like.
        create_reaction(conn, carol, post["hash"], liked=False, name="carol", now=1004)
    finally:
        conn.close()
    authors = {
        label: destination_hash(identity).hex()
        for label, identity in (
            ("alice", alice),
            ("bob", bob),
            ("carol", carol),
            ("dave", dave),
        )
    }
    return node, post, authors


def test_tan_likers_via_api(tmp_path):
    node, post, authors = _likers_setup(tmp_path)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 200
        assert payload["target"] == post["hash"]
        assert payload["count"] == 2
        assert payload["total"] == 2
        assert payload["likers"] == [
            {
                "author": authors["dave"],
                "name": "dave",
                "liked_at": 1003.0,
            },
            {
                "author": authors["alice"],
                "name": "alice-on-record",
                "liked_at": 1001.0,
            },
        ]

        # An uppercase spelling of the same hash resolves, and the
        # response carries the canonical lowercase hash.
        status, _, payload = _json(
            bridge, f"/api/tan/likes?target={post['hash'].upper()}"
        )
        assert status == 200
        assert payload["target"] == post["hash"]
        assert payload["count"] == 2

        # Unknown post hash -> 400, per the issue spec.
        status, _, payload = _json(bridge, "/api/tan/likes?target=" + "cd" * 32)
        assert status == 400
        assert "unknown post" in payload["error"]["message"]

        # A missing target -> 400.
        status, _, payload = _json(bridge, "/api/tan/likes")
        assert status == 400
        assert "target is required" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_export_import_requires_social(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, RNS.Identity(), "cached", name="other", now=1000)
    finally:
        conn.close()

    # No TanApp: the node never enabled the social feed, even though a
    # post is cached on disk.
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/export")
        assert status == 503
        assert "not enabled" in payload["error"]["message"]

        status, _, payload = _json(
            bridge,
            "/api/tan/import",
            method="POST",
            body={"format": "tau-tan-export", "version": 1, "entries": []},
        )
        assert status == 503
        assert "not enabled" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_likers_excludes_muted_and_blocked(tmp_path):
    from tau.blocklist import BlockList
    from tau.config import TauConfig
    from tau.tangents.tan import init_db, mute

    node, post, authors = _likers_setup(tmp_path)
    conn = init_db(node.config.social_db_path)
    try:
        mute(conn, authors["dave"], name="dave")
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The muted liker is hidden, as in the timeline.
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 200
        assert [entry["author"] for entry in payload["likers"]] == [authors["alice"]]
        assert payload["total"] == 1

        # A blocked liker is hidden too.
        node.blocklist = BlockList(blocked=frozenset({authors["alice"]}))
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 200
        assert payload["likers"] == []
        assert payload["total"] == 0

        # A fail-closed block-all list hides the post entirely,
        # matching the timeline and the DM gate.
        node.blocklist = BlockList(blocked=frozenset(), block_all=True)
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 400
        assert "unknown post" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_export_too_large_for_import_is_refused(tmp_path, monkeypatch):
    import RNS

    import tau.webapi as webapi
    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, identity, "one post", name="tester", now=1000)
    finally:
        conn.close()
    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A cache the console could not import back must not be
        # exported either: shrink the bound to prove the check.
        monkeypatch.setattr(webapi, "MAX_IMPORT_BYTES", 10)
        status, _, payload = _json(bridge, "/api/tan/export")
        assert status == 400
        assert "larger than the console can import" in payload["error"]["message"]
        assert "tau tan export" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_likers_hides_post_by_hidden_author(tmp_path):
    from tau.config import TauConfig
    from tau.tangents.tan import init_db, mute

    node, post, authors = _likers_setup(tmp_path)
    conn = init_db(node.config.social_db_path)
    try:
        mute(conn, authors["bob"], name="bob")
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The timeline would not show a muted author's post at all, so
        # the endpoint does not admit the post exists.
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 400
        assert "unknown post" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_likers_rejects_deleted_post(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_delete, create_post, create_reaction, init_db

    node, _, _ = _likers_setup(tmp_path)
    # A post deleted by its own author: build one with an identity held
    # in this test, like it, then delete it.
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        own = create_post(conn, identity, "delete me", name="me", now=2000)
        create_reaction(conn, identity, own["hash"], liked=True, name="me", now=2001)
        create_delete(conn, identity, own["hash"], name="me", now=2002)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, f"/api/tan/likes?target={own['hash']}")
        assert status == 400
        assert "has been deleted" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_import_sanitizes_unencodable_error_hash(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    conn = init_db(node.config.social_db_path)
    conn.close()
    node.app = TanApp(node.config, RNS.Identity(), name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A hash with a lone surrogate comes from untrusted JSON; the
        # response encoder (ensure_ascii=False + UTF-8) cannot write it,
        # so the bridge must replace it instead of failing the request.
        document = {
            "format": "tau-tan-export",
            "version": 1,
            "exported_at": 1000,
            "authors": [],
            "entries": [{"hash": "\ud800", "text": "bogus"}],
        }
        status, _, payload = _json(
            bridge, "/api/tan/import", method="POST", body=document
        )
        assert status == 200
        assert payload["import"]["skipped"] == 1
        # The UTF-8 "replace" handler turns the lone surrogate into "?".
        assert payload["import"]["errors"][0]["hash"] == "?"
    finally:
        bridge.stop()


def test_tan_likers_hides_reactions_per_ruleset(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.rules import Ruleset
    from tau.tangents.tan import TanApp

    node, post, authors = _likers_setup(tmp_path)
    identity = RNS.Identity()
    # A ruleset that displays no reaction kind: the timeline strips its
    # like annotations, so the likers endpoint must disclose nothing.
    node.app = TanApp(
        node.config,
        identity,
        name="tester",
        gossip=False,
        ruleset=Ruleset(kinds=frozenset({"post"})),
    )
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 200
        assert payload["likers"] == []
        assert payload["total"] == 0
    finally:
        bridge.stop()


def test_is_loopback_client():
    from tau.webapi import _is_loopback_client

    assert _is_loopback_client("127.0.0.1") is True
    assert _is_loopback_client("::1") is True
    assert _is_loopback_client("192.168.1.5") is False
    assert _is_loopback_client("not an address") is False


def test_tan_likers_caps_at_max_likers(tmp_path, monkeypatch):
    import tau.webapi

    node, post, authors = _likers_setup(tmp_path)
    monkeypatch.setattr(tau.webapi, "MAX_LIKERS", 1)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 200
        assert payload["count"] == 1
        assert payload["total"] == 2
        # The cap keeps the newest liker.
        assert payload["likers"][0]["author"] == authors["dave"]
    finally:
        bridge.stop()


def test_tan_likers_server_error_is_500(tmp_path, monkeypatch):
    import sqlite3

    node, post, authors = _likers_setup(tmp_path)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A storage failure is a server error, not a bad target.
        def _boom():
            raise sqlite3.OperationalError("disk I/O error")

        monkeypatch.setattr(bridge, "_tan_conn", _boom)
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 500
        assert payload["error"]["message"] == "internal error"
    finally:
        bridge.stop()




def _queue(node, to_hex, text, message_id=None):
    return node.mailbox.enqueue(
        to=bytes.fromhex(to_hex),
        text=text,
        message_id=message_id,
    )


def test_doctor_endpoint_lists_all_checks(bridge, node, tmp_path):
    node.config = TauConfig.resolve(data_dir=tmp_path)

    status, _, payload = _json(bridge, "/api/doctor")
    assert status == 200
    # A fresh directory has no identity, so the verdict is not ok.
    assert payload["ok"] is False
    assert [check["name"] for check in payload["checks"]] == [
        "identity",
        "node metadata",
        "peers",
        "mailbox",
        "inbox",
        "messages",
        "tan",
        "tan rules",
        "allowlist",
        "instance lock",
    ]
    for check in payload["checks"]:
        assert check["status"] in ("ok", "warn", "fail")
        assert isinstance(check["detail"], str)
    by_name = {check["name"]: check for check in payload["checks"]}
    assert by_name["identity"]["status"] == "fail"
    assert by_name["peers"]["status"] == "ok"  # no peers yet is healthy


def test_doctor_reports_a_corrupt_store(bridge, node, tmp_path):
    node.config = TauConfig.resolve(data_dir=tmp_path)
    (tmp_path / "peers.json").write_text("{not json", encoding="utf-8")

    result = bridge.doctor()
    assert result["ok"] is False
    by_name = {check["name"]: check for check in result["checks"]}
    assert by_name["peers"]["status"] == "fail"
    assert "peers.json" in by_name["peers"]["detail"]


def test_doctor_rejects_a_concurrent_run(bridge, node, tmp_path):
    node.config = TauConfig.resolve(data_dir=tmp_path)
    assert bridge._doctor_lock.acquire(blocking=False)
    try:
        with pytest.raises(WebBridgeError, match="already running"):
            bridge.doctor()
    finally:
        bridge._doctor_lock.release()


def test_doctor_endpoint_reports_busy_when_a_check_is_running(
    bridge, node, tmp_path
):
    node.config = TauConfig.resolve(data_dir=tmp_path)
    assert bridge._doctor_lock.acquire(blocking=False)
    try:
        status, _, payload = _json(bridge, "/api/doctor")
    finally:
        bridge._doctor_lock.release()
    assert status == 503
    assert payload["error"]["message"] == "a health check is already running"


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


def test_outbox_retry_one_by_id_and_prefix(bridge, node):
    first = _queue(node, PEER_HASH, "first")
    second = _queue(node, PEER_HASH, "second")

    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={"target": first.id}
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["delivered"] == 1
    assert payload["still_queued"] == 1
    assert payload["target"] == first.id
    assert payload["target_delivered"] is True
    assert payload["target_still_queued"] is False
    # Backoff is ignored: the operator asked for a retry *now*.
    assert node.flush_calls[-1]["respect_backoff"] is False
    assert node.flush_calls[-1]["message_id"] == first.id
    assert node.flush_calls[-1]["to"] == bytes.fromhex(PEER_HASH)
    assert node.mailbox.get_outbox(first.id) is None

    status, _, payload = _json(
        bridge,
        "/api/outbox/retry",
        method="POST",
        body={"id": second.id[:8]},
    )
    assert status == 200
    assert payload["target_delivered"] is True
    assert payload["target_still_queued"] is False
    assert payload["still_queued"] == 0

    # A targeted retry against an unreachable peer leaves the entry
    # queued and says so: not delivered is not the same as gone.
    third = _queue(node, PEER_HASH, "third")
    node.flush_deliver_target = False
    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={"target": third.id}
    )
    assert status == 200
    assert payload["delivered"] == 0
    assert payload["target_delivered"] is False
    assert payload["target_still_queued"] is True
    assert payload["still_queued"] == 1
    assert node.mailbox.get_outbox(third.id) is not None

    status, _, payload = _json(bridge, "/api/outbox")
    assert [item["id"] for item in payload["outbox"]] == [third.id]


def test_outbox_retry_all_reports_what_is_left(bridge, node):
    first = _queue(node, PEER_HASH, "first")
    _queue(node, PEER_HASH, "second")

    # Peers unreachable: nothing moves, everything stays queued.
    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={}
    )
    assert status == 200
    assert payload["delivered"] == 0
    assert payload["still_queued"] == 2
    assert "target" not in payload
    assert node.flush_calls[-1]["message_id"] is None
    assert node.flush_calls[-1]["to"] is None

    # Peers reachable: the whole queue drains.
    node.flush_deliver_all = True
    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={}
    )
    assert status == 200
    assert payload["delivered"] == 2
    assert payload["still_queued"] == 0
    assert node.mailbox.get_outbox(first.id) is None


def test_outbox_retry_errors(bridge, node):
    # A non-string target is a 400, not a retry-all.
    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={"target": 42}
    )
    assert status == 400
    assert "target" in payload["error"]["message"]

    # A supplied-but-blank target is a 400 too: {"target": ""} must
    # not silently become a retry of the whole queue.
    for body in ({"target": ""}, {"target": "   "}, {"id": ""}, {"target": 0}):
        status, _, payload = _json(
            bridge, "/api/outbox/retry", method="POST", body=body
        )
        assert status == 400
        assert "target" in payload["error"]["message"]
    assert node.flush_calls == []

    # Unknown and ambiguous ids are 404s, like cancel.
    status, _, payload = _json(
        bridge,
        "/api/outbox/retry",
        method="POST",
        body={"target": "ff" * 16},
    )
    assert status == 404
    assert "unknown queued message" in payload["error"]["message"]

    _queue(node, PEER_HASH, "one", message_id="aa" + "11" * 15)
    _queue(node, PEER_HASH, "two", message_id="aa" + "22" * 15)
    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={"target": "aa"}
    )
    assert status == 404
    assert "match prefix" in payload["error"]["message"]

    # A mail round already running is a 409: the retry never ran, so
    # nothing is reported as delivered.
    node.flush_lock_busy = True
    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={}
    )
    assert status == 409
    assert "already running" in payload["error"]["message"]
    assert node.mailbox.counts()["outbox"] == 2

def test_outbox_retry_counts_concurrent_delivery(bridge, node):
    # A concurrent mail round delivers the entry after resolution but
    # before this flush visits it: the flush reports no delivery and
    # the entry is gone, yet the bridge's delivery record confirms it
    # was delivered — not expired or cancelled.
    entry = _queue(node, PEER_HASH, "concurrent")
    node.flush_concurrent_delivery = True
    assert bridge.record_delivery(entry.id) is None  # buffered: no console record

    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={"target": entry.id}
    )
    assert status == 200
    assert payload["delivered"] == 0
    assert payload["target_delivered"] is True
    assert payload["target_still_queued"] is False
    assert node.mailbox.get_outbox(entry.id) is None

def test_outbox_retry_unconfirmed_departure(bridge, node):
    # The entry leaves the queue with no delivery recorded anywhere —
    # expired, or cancelled by another process. The response says so
    # honestly (not delivered, not queued) instead of guessing.
    entry = _queue(node, PEER_HASH, "gone")
    node.flush_concurrent_delivery = True

    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={"target": entry.id}
    )
    assert status == 200
    assert payload["delivered"] == 0
    assert payload["target_delivered"] is False
    assert payload["target_still_queued"] is False

def _hold(node, to_hex, message_id, blob=b"sealed-bytes", expires_at=None,
          depositor=None, now=None):
    return node.mailbox.hold(
        to=bytes.fromhex(to_hex),
        blob=blob,
        message_id=message_id,
        expires_at=expires_at if expires_at is not None else time.time() + 3600,
        now=now,
        depositor=bytes.fromhex(depositor) if depositor else None,
    )

def test_outbox_held_lists_envelopes_with_names(bridge, node):
    first = "cc" + "11" * 15
    second = "dd" + "22" * 15
    assert _hold(
        node, PEER_HASH, first, blob=b"sealed-one",
        expires_at=5000.0, depositor=OTHER_HASH, now=100.0,
    )
    assert _hold(
        node, OTHER_HASH, second, blob=b"sealed-two!",
        expires_at=6000.0, depositor=PEER_HASH, now=200.0,
    )

    status, _, payload = _json(bridge, "/api/outbox/held")
    assert status == 200
    assert payload["count"] == 2
    by_id = {entry["id"]: entry for entry in payload["held"]}
    assert by_id[first]["to"] == PEER_HASH
    assert by_id[first]["to_name"] == "alice"
    # Unknown depositor: no name, but the hash is still shown.
    assert by_id[first]["from"] == OTHER_HASH
    assert by_id[first]["from_name"] == ""
    assert by_id[first]["stored_at"] == 100.0
    assert by_id[first]["expires_at"] == 5000.0
    assert by_id[first]["blob_bytes"] == len(b"sealed-one")
    # Unknown recipient: falls back to the hash, while the known
    # depositor resolves to a name.
    assert by_id[second]["to"] == OTHER_HASH
    assert by_id[second]["to_name"] == ""
    assert by_id[second]["from"] == PEER_HASH
    assert by_id[second]["from_name"] == "alice"
    assert by_id[second]["blob_bytes"] == len(b"sealed-two!")
    # Oldest first.
    assert [entry["id"] for entry in payload["held"]] == [first, second]

def test_outbox_drop_held_by_id_and_prefix(bridge, node):
    first = "cc" + "11" * 15
    second = "dd" + "22" * 15
    _hold(node, PEER_HASH, first)
    _hold(node, PEER_HASH, second)

    status, _, payload = _json(
        bridge, "/api/outbox/drop-held", method="POST", body={"target": first}
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["dropped"]["id"] == first
    assert payload["dropped"]["to_name"] == "alice"
    assert "already fetched" in payload["dropped"]["note"]
    # The tombstone stops a sender retry resurrecting the envelope.
    assert _hold(node, PEER_HASH, first) is False

    status, _, payload = _json(
        bridge, "/api/outbox/drop-held", method="POST", body={"target": second[:8]}
    )
    assert status == 200
    assert payload["dropped"]["id"] == second

    status, _, payload = _json(bridge, "/api/outbox/held")
    assert payload == {"held": [], "count": 0}

def test_outbox_drop_held_errors(bridge, node):
    status, _, payload = _json(
        bridge, "/api/outbox/drop-held", method="POST", body={}
    )
    assert status == 400
    assert "target" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/outbox/drop-held", method="POST", body={"target": "ff" * 16}
    )
    assert status == 400
    assert "unknown held message" in payload["error"]["message"]

    # Two envelopes sharing a prefix make the prefix ambiguous.
    _hold(node, PEER_HASH, "aa" + "11" * 15)
    _hold(node, PEER_HASH, "aa" + "22" * 15)
    status, _, payload = _json(
        bridge, "/api/outbox/drop-held", method="POST", body={"target": "aa"}
    )
    assert status == 400
    assert "match prefix" in payload["error"]["message"]

    # Dropping an envelope that is already gone is a 404 too.
    gone = "bb" + "33" * 15
    _hold(node, PEER_HASH, gone)
    status, _, payload = _json(
        bridge, "/api/outbox/drop-held", method="POST", body={"target": gone}
    )
    assert status == 200
    status, _, payload = _json(
        bridge, "/api/outbox/drop-held", method="POST", body={"target": gone}
    )
    assert status == 400
    assert "unknown held message" in payload["error"]["message"]



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


def test_allowlist_get_empty(bridge):
    status, _, payload = _json(bridge, "/api/allowlist")
    assert status == 200
    assert payload == {"enabled": False, "entries": []}


def test_allowlist_add_by_name_and_remove(bridge, node):
    status, _, payload = _json(
        bridge, "/api/allowlist/add", method="POST", body={"target": "alice"}
    )
    assert status == 200
    assert payload["enabled"] is False
    assert payload["entries"] == [
        {"destination": PEER_HASH, "name": "alice"}
    ]

    # The entry survives a fresh GET, with its registry name attached.
    status, _, payload = _json(bridge, "/api/allowlist")
    assert status == 200
    assert payload["entries"] == [
        {"destination": PEER_HASH, "name": "alice"}
    ]

    status, _, payload = _json(
        bridge, "/api/allowlist/remove", method="POST", body={"target": "alice"}
    )
    assert status == 200
    assert payload == {"enabled": False, "entries": []}


def test_allowlist_add_by_raw_hash_has_no_name(bridge):
    status, _, payload = _json(
        bridge, "/api/allowlist/add", method="POST", body={"target": OTHER_HASH}
    )
    assert status == 200
    assert payload["entries"] == [{"destination": OTHER_HASH, "name": ""}]


def test_allowlist_add_by_prefix(bridge):
    status, _, payload = _json(
        bridge, "/api/allowlist/add", method="POST", body={"target": "abab"}
    )
    assert status == 200
    assert payload["entries"] == [
        {"destination": PEER_HASH, "name": "alice"}
    ]


def test_allowlist_add_rejects_alias(bridge, node):
    # The allow-list is a security boundary: a local alias must never
    # stand in for the peer's asserted name, so the fixture peer's
    # alias "ally" does not resolve here even though the registry
    # would resolve it with include_alias=True.
    node.registry.get(PEER_HASH).alias = "ally"
    status, _, payload = _json(
        bridge, "/api/allowlist/add", method="POST", body={"target": "ally"}
    )
    assert status == 400
    assert "unknown peer" in payload["error"]["message"]


def test_allowlist_set_enabled_applies_immediately(bridge, node, tmp_path):
    _json(bridge, "/api/allowlist/add", method="POST", body={"target": "alice"})

    status, _, payload = _json(
        bridge,
        "/api/allowlist/set-enabled",
        method="POST",
        body={"enabled": True},
    )
    assert status == 200
    assert payload["enabled"] is True
    assert [entry["destination"] for entry in payload["entries"]] == [PEER_HASH]

    # The running node's in-memory gate changed too — no restart needed.
    assert node.allowlist.is_allowed(PEER_HASH) is True
    assert node.allowlist.is_allowed(OTHER_HASH) is False
    # And the file agrees, so a restart keeps the same state.
    from tau.allowlist import load_allowlist

    saved = load_allowlist(tmp_path / "allowlist.json")
    assert saved.enabled is True
    assert saved.allow == frozenset({PEER_HASH})


def test_allowlist_errors(bridge):
    status, _, payload = _json(
        bridge, "/api/allowlist/add", method="POST", body={}
    )
    assert status == 400
    assert "target" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/allowlist/add", method="POST", body={"target": "  "}
    )
    assert status == 400
    assert "target" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/allowlist/add", method="POST", body={"target": "nobody"}
    )
    assert status == 400
    assert "unknown peer" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/allowlist/remove",
        method="POST",
        body={"target": "nobody"},
    )
    assert status == 400
    assert "unknown peer" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/allowlist/set-enabled",
        method="POST",
        body={"enabled": "yes"},
    )
    assert status == 400
    assert "boolean" in payload["error"]["message"]


def test_allowlist_concurrent_adds_keep_every_entry(bridge):
    # Overlapping mutations must not lose each other's changes: each
    # add derives from the latest saved list, not a stale snapshot.
    hashes = [f"{n:02x}" * 16 for n in range(1, 11)]
    errors = []

    def add_one(destination):
        try:
            status, _, _ = _json(
                bridge,
                "/api/allowlist/add",
                method="POST",
                body={"target": destination},
            )
            assert status == 200
        except Exception as exc:  # noqa: BLE001 - collected, asserted below
            errors.append(exc)

    threads = [
        threading.Thread(target=add_one, args=(destination,))
        for destination in hashes
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []

    status, _, payload = _json(bridge, "/api/allowlist")
    assert status == 200
    assert [entry["destination"] for entry in payload["entries"]] == sorted(
        hashes
    )


def test_allowlist_toggle_keeps_cli_saved_entries(bridge, node, tmp_path):
    from tau.allowlist import AllowList, save_allowlist

    # The CLI saved a change while this node runs; the running node's
    # in-memory list does not know about it yet.
    save_allowlist(
        tmp_path / "allowlist.json",
        AllowList(enabled=False, allow=frozenset({OTHER_HASH})),
    )
    assert node.allowlist.is_allowed(OTHER_HASH) is True  # not enforced yet

    status, _, payload = _json(
        bridge,
        "/api/allowlist/set-enabled",
        method="POST",
        body={"enabled": True},
    )
    assert status == 200
    # The console toggle reconciled with the saved file instead of
    # overwriting it with the stale in-memory list.
    assert [entry["destination"] for entry in payload["entries"]] == [
        OTHER_HASH
    ]
    assert node.allowlist.is_allowed(OTHER_HASH) is True
    assert node.allowlist.is_allowed(PEER_HASH) is False


def test_allowlist_refuses_to_overwrite_corrupt_file(bridge, node, tmp_path):
    path = tmp_path / "allowlist.json"
    path.write_text("{not json", encoding="utf-8")
    before = path.read_bytes()

    status, _, payload = _json(
        bridge, "/api/allowlist/add", method="POST", body={"target": "alice"}
    )
    assert status == 400
    assert "allow-list" in payload["error"]["message"]
    # The file is untouched and the running node's in-memory list is
    # unchanged by the failed edit.
    assert path.read_bytes() == before
    assert node.allowlist.enabled is False
    assert node.allowlist.allow == frozenset()


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


def test_stored_file_metadata_survives_load_and_bad_shapes_drop():
    from tau.webapi import _clean_stored_file, _clean_stored_message

    good = {"name": "notes.txt", "size": 10, "sha256": "a" * 64}
    assert _clean_stored_file(good) == good
    assert _clean_stored_file({"name": "x", "size": 0, "sha256": "a" * 64}) is None
    assert _clean_stored_file({"name": "x", "size": 10, "sha256": "zz"}) is None
    assert _clean_stored_file("notes.txt") is None

    entry = _stored_message(1, "Sent file: notes.txt (10 bytes)", direction="out")
    entry["file"] = good
    cleaned = _clean_stored_message(entry)
    assert cleaned is not None
    assert cleaned["file"] == good

    entry["file"] = {"name": "x", "size": -1, "sha256": "a" * 64}
    assert "file" not in _clean_stored_message(entry)


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


def test_tan_quote_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "api quotable", name="tester", now=1000)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/tan/quote",
            method="POST",
            body={"target": post["hash"][:16], "text": "seconded, with notes"},
        )
        assert status == 200
        quote = payload["post"]
        assert quote["kind"] == "quote"
        assert quote["target"] == post["hash"]
        assert quote["text"] == "seconded, with notes"

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "seconded, with notes",
            "api quotable",
        ]
        assert payload["posts"][0]["quoted"] == {
            "hash": post["hash"],
            "author": post["author"],
            "name": "tester",
            "text": "api quotable",
            "deleted": False,
        }

        # Quoting a quote is rejected: embeds stay one level deep.
        status, _, payload = _json(
            bridge,
            "/api/tan/quote",
            method="POST",
            body={"target": quote["hash"], "text": "nesting"},
        )
        assert status == 400
        assert "only posts and replies can be quoted" in payload["error"]["message"]

        # Quoting an unknown post fails.
        status, _, payload = _json(
            bridge,
            "/api/tan/quote",
            method="POST",
            body={"target": "cd" * 32, "text": "nowhere"},
        )
        assert status == 400
        assert "unknown post" in payload["error"]["message"]

        # A missing target or comment is a bad request, not a crash.
        status, _, payload = _json(
            bridge,
            "/api/tan/quote",
            method="POST",
            body={"text": "no target"},
        )
        assert status == 400
        assert "target" in payload["error"]["message"]
        status, _, payload = _json(
            bridge,
            "/api/tan/quote",
            method="POST",
            body={"target": post["hash"]},
        )
        assert status == 400
        assert "text" in payload["error"]["message"]
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


def test_delete_message_endpoint_removes_history_and_inbox_entry(bridge, node):
    node.inbox.append(
        {
            "id": "wire-1",
            "destination": PEER_HASH,
            "from": "alice",
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
        }
    )
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
        bridge, "/api/messages/delete", method="POST", body={"id": 1}
    )
    assert status == 200
    assert payload == {"ok": True, "deleted": {"id": 1, "wire_id": "wire-1"}}
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []
    assert node.inbox.export_document()["messages"] == []
    # The bus stamps its own sequence into `id`, so the deleted
    # console id must ride under `message_id`: other tabs drop the
    # deleted bubble, not whichever one shares the event sequence.
    deleted_events = [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_deleted"
    ]
    assert len(deleted_events) == 1
    assert deleted_events[0]["message_id"] == 1


def test_delete_message_endpoint_removes_console_only_entry(bridge):
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            # No wire id: an entry from before ids existed, or one the
            # node inbox never stored. The console copy still goes.
        }
    )
    status, _, payload = _json(
        bridge, "/api/messages/delete", method="POST", body={"id": 1}
    )
    assert status == 200
    assert payload == {"ok": True, "deleted": {"id": 1, "wire_id": None}}
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []


def test_delete_message_endpoint_rejects_unknown_and_bad_ids(bridge):
    status, _, payload = _json(
        bridge, "/api/messages/delete", method="POST", body={"id": 7}
    )
    assert status == 400
    assert "unknown message" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/messages/delete", method="POST", body={}
    )
    assert status == 400
    assert "field 'id'" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/messages/delete", method="POST", body={"id": "1"}
    )
    assert status == 400
    assert "field 'id'" in payload["error"]["message"]


def test_delete_message_survives_bridge_restart(node, tmp_path):
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
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "still here",
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "wire-2",
            }
        )
        status, _, _ = _json(
            bridge, "/api/messages/delete", method="POST", body={"id": 1}
        )
        assert status == 200
    finally:
        bridge.stop()

    bridge2 = _bridge_with_data_dir(FakeNode(), tmp_path)
    try:
        messages = bridge2.messages()
        assert [message["id"] for message in messages] == [2]
        assert messages[0]["text"] == "still here"
    finally:
        bridge2.stop()


def test_delete_message_rejects_queued_entry(bridge, node):
    _queue_send(node)
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    assert status == 200
    assert payload["message"]["status"] == "queued"

    status, _, payload = _json(
        bridge, "/api/messages/delete", method="POST", body={"id": 1}
    )
    assert status == 400
    assert "still queued" in payload["error"]["message"]
    _, _, feed = _json(bridge, "/api/messages")
    assert len(feed["messages"]) == 1


def test_delete_message_fails_when_history_cannot_persist(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        node.inbox.append(
            {
                "id": "wire-1",
                "destination": PEER_HASH,
                "from": "alice",
                "text": "hello",
                "sent_at": 100.0,
                "received_at": 101.0,
            }
        )
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
        # A directory in the history file's place makes the atomic
        # replace fail, so the strict save raises.
        messages_path = tmp_path / "messages.json"
        messages_path.unlink()
        messages_path.mkdir()
        status, _, payload = _json(
            bridge, "/api/messages/delete", method="POST", body={"id": 1}
        )
        assert status == 500
        _, _, feed = _json(bridge, "/api/messages")
        assert len(feed["messages"]) == 1
        # Inbox-first ordering: the inbox copy is already gone, but the
        # bubble stays visible, so a retry once the file is repaired
        # completes the delete (the inbox delete of the already-removed
        # wire id is ignored).
        assert node.inbox.export_document()["messages"] == []
        messages_path.rmdir()
        status, _, payload = _json(
            bridge, "/api/messages/delete", method="POST", body={"id": 1}
        )
        assert status == 200
        _, _, feed = _json(bridge, "/api/messages")
        assert feed["messages"] == []
        assert node.inbox.export_document()["messages"] == []
    finally:
        bridge.stop()


def test_delete_message_rollback_survives_append_to_full_log(
    node, tmp_path, monkeypatch
):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        # Fill the bounded log (500 entries) with console-only messages.
        for i in range(1, 501):
            bridge._messages.append(
                {
                    "id": i,
                    "direction": "inbound",
                    "peer": PEER_HASH,
                    "peer_name": "alice",
                    "text": f"m{i}",
                    "sent_at": float(i),
                    "received_at": float(i),
                    "status": "received",
                }
            )
        bridge._next_message_id = 501

        # A message arriving while the strict save is in flight refills
        # the log, then the write fails. The rollback must restore the
        # deleted entry instead of raising on the full deque.
        def _append_then_fail(*args, **kwargs):
            bridge._messages.append(
                {
                    "id": 501,
                    "direction": "inbound",
                    "peer": PEER_HASH,
                    "peer_name": "alice",
                    "text": "m501",
                    "sent_at": 501.0,
                    "received_at": 501.0,
                    "status": "received",
                }
            )
            raise OSError("disk full")

        monkeypatch.setattr(
            "tau.webapi.atomic_write_json", _append_then_fail
        )
        status, _, _ = _json(
            bridge, "/api/messages/delete", method="POST", body={"id": 250}
        )
        assert status == 500
        # The feed API pages the log, so check the deque itself: the
        # restore must have landed despite the full log.
        assert len(bridge._messages) == 500
        ids = [message["id"] for message in bridge._messages]
        assert 250 in ids  # the deleted entry was restored
        assert 501 in ids  # the interleaved append survived
        assert ids == sorted(ids)  # the restore kept id order
    finally:
        bridge.stop()


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


# -- sneakernet bundles ----------------------------------------------------


def _bundle_with_held_mail(tmp_path, *, now=None):
    """A signed bundle holding one sealed envelope for the fake peer."""
    from tau.bundle import build_bundle
    from tau.identity import destination_hash
    from tau.mailbox import seal_message
    from tau.peers import PeerRegistry

    sender = RNS.Identity()
    recipient = RNS.Identity()
    mailbox = Mailbox(tmp_path / "sender-mailbox.json")
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="held-1",
        text="held hello",
        sent_at=1000.0,
    )
    moment = time.time() if now is None else now
    mailbox.hold(
        to=destination_hash(recipient),
        blob=blob,
        message_id="held-1",
        expires_at=moment + 7 * 24 * 3600,
        now=moment,
    )
    document, summary = build_bundle(
        identity=sender,
        sender_name="alice",
        mailbox=mailbox,
        registry=PeerRegistry(),
        tan_conn=None,
        include_tan=False,
        now=moment,
    )
    assert summary["mail"] == 1
    return document


def _post_bundle(bridge, document):
    """Post a bundle document as its serialised bytes, like the console."""
    from tau.bundle import serialize_bundle

    return _json(
        bridge, "/api/bundle/import", method="POST", raw=serialize_bundle(document)
    )


def test_bundle_export_empty(bridge):
    status, _, payload = _json(
        bridge, "/api/bundle/export", method="POST", body={}
    )
    assert status == 200
    assert payload["ok"] is True
    # The export returns the server's serialised bundle text verbatim,
    # so the console downloads it without a parse round trip.
    assert isinstance(payload["bundle"], str)
    document = json.loads(payload["bundle"])
    assert document["format"] == "tau-bundle"
    assert document["sig"]
    assert payload["summary"] == {
        "mail": 0,
        "tan": 0,
        "skipped": [],
        "bytes": payload["summary"]["bytes"],
    }


def test_bundle_export_rejects_non_boolean_include_tan(bridge):
    status, _, payload = _json(
        bridge, "/api/bundle/export", method="POST", body={"include_tan": "yes"}
    )
    assert status == 400
    assert "include_tan" in payload["error"]["message"]


def test_bundle_export_includes_held_mail(bridge, node, tmp_path):
    node.mailbox.hold(
        to=bytes.fromhex(OTHER_HASH),
        blob=b"sealed-bytes",
        message_id="held-9",
        expires_at=time.time() + 3600,
        now=time.time(),
    )
    status, _, payload = _json(
        bridge, "/api/bundle/export", method="POST", body={}
    )
    assert status == 200
    assert payload["summary"]["mail"] == 1
    document = json.loads(payload["bundle"])
    assert document["mail"][0]["id"] == "held-9"
    assert document["mail"][0]["to"] == OTHER_HASH


def test_bundle_export_sees_mail_queued_by_another_process(bridge, node, tmp_path):
    from tau.identity import destination_hash
    from tau.mailbox import Mailbox

    # The serving node's in-memory snapshot predates the other process,
    # and the fake peer's key cannot seal to — so give the registry a
    # peer with a real key first, then queue through a second mailbox.
    recipient = RNS.Identity()
    node.registry._peers.append(
        Peer(
            destination_hash=destination_hash(recipient),
            name="carol",
            public_key=recipient.get_public_key(),
            first_seen=1000.0,
            last_seen=time.time(),
            hops=1,
        )
    )
    other = Mailbox(tmp_path / "mailbox.json")
    entry = other.enqueue(
        to=destination_hash(recipient), text="queued by cli"
    )
    assert node.mailbox.get_outbox(entry.id) is None

    status, _, payload = _json(
        bridge, "/api/bundle/export", method="POST", body={}
    )
    assert status == 200
    assert payload["summary"]["mail"] == 1
    document = json.loads(payload["bundle"])
    assert document["mail"][0]["id"] == entry.id


def test_bundle_export_with_tan_reports_empty_store(bridge):
    status, _, payload = _json(
        bridge, "/api/bundle/export", method="POST", body={"include_tan": True}
    )
    assert status == 200
    assert payload["summary"]["tan"] == 0
    document = json.loads(payload["bundle"])
    assert document["tan"] is not None
    assert document["tan"]["entries"] == []


def test_bundle_import_roundtrip_and_dedupe(bridge, node, tmp_path):
    document = _bundle_with_held_mail(tmp_path)
    status, _, payload = _post_bundle(bridge, document)
    assert status == 200
    report = payload["import"]
    assert report["imported"] == 1
    assert report["present"] == 0
    assert report["skipped"] == 0
    assert node.mailbox.counts()["held"] == 1

    # A second import of the same bundle dedupes instead of duplicating.
    status, _, payload = _post_bundle(bridge, document)
    assert status == 200
    report = payload["import"]
    assert report["imported"] == 0
    assert report["present"] == 1
    assert node.mailbox.counts()["held"] == 1


def test_bundle_import_accepts_integral_float_timestamps(bridge, node, tmp_path):
    # A bundle built at an exactly integral epoch second carries
    # e.g. 1793000000.0 in its signed manifest. A client that parses and
    # re-stringifies the document rewrites it as 1793000000 and breaks
    # the signature — so the route verifies the submitted bytes, and
    # the console sends the file text verbatim.
    now = float(int(time.time()))
    document = _bundle_with_held_mail(tmp_path, now=now)
    assert document["manifest"]["created_at"] == now
    status, _, payload = _post_bundle(bridge, document)
    assert status == 200
    assert payload["import"]["imported"] == 1


def test_bundle_import_rejects_tampered_bundle(bridge, tmp_path):
    document = _bundle_with_held_mail(tmp_path)
    document["mail"][0]["blob"] = "AAAA" + document["mail"][0]["blob"][4:]
    status, _, payload = _post_bundle(bridge, document)
    assert status == 400
    assert "signature" in payload["error"]["message"]


def test_bundle_import_rejects_garbage(bridge):
    status, _, payload = _json(
        bridge, "/api/bundle/import", method="POST", raw=b'{"format": "nope"}'
    )
    assert status == 400
    assert "invalid bundle" in payload["error"]["message"]


def test_bundle_import_rejects_remote_client(bridge, tmp_path, monkeypatch):
    # Bulk imports are localhost-only even when the bridge is bound to a
    # remote interface: a remote client must not be able to merge a bundle
    # into this node's held mail.
    import tau.webapi as webapi

    document = _bundle_with_held_mail(tmp_path)
    monkeypatch.setattr(webapi, "_is_loopback_client", lambda client: False)
    status, _, payload = _post_bundle(bridge, document)
    assert status == 403
    assert "only accepted from localhost" in payload["error"]["message"]


def test_bundle_import_accepts_body_over_default_cap(bridge, tmp_path):
    # A bundle import carries the whole signed document, so the route
    # reads up to the 8 MiB bundle cap instead of the 32 KiB limit
    # every other route uses. Pad past 32 KiB with an unknown key,
    # which parse_bundle ignores and the signature still covers.
    document = _bundle_with_held_mail(tmp_path)
    document["padding"] = "x" * (40 * 1024)
    status, _, payload = _post_bundle(bridge, document)
    assert status == 200
    assert payload["import"]["imported"] == 1


def test_messages_export_import_via_api(bridge, node):
    _inbound_record(node)
    node.inbox.append(
        {
            "from": "tester",
            "destination": PEER_HASH,
            "text": "reply back",
            "sent_at": 200.0,
            "received_at": 201.0,
            "id": "m-2",
            "direction": "out",
        }
    )

    status, _, payload = _json(bridge, "/api/messages/export")
    assert status == 200
    assert payload["format"] == "tau-messages-export"
    assert payload["version"] == 1
    assert [entry["text"] for entry in payload["messages"]] == [
        "hello",
        "reply back",
    ]

    # A fresh node restores the whole document; the restored messages
    # show up in its console feed, then report as already present on
    # a second import — without duplicating the feed.
    node2 = FakeNode()
    bridge2 = WebBridge(node2, host="127.0.0.1", port=0)
    bridge2.start()
    try:
        _, _, document = _json(bridge, "/api/messages/export")
        status, _, payload = _json(
            bridge2, "/api/messages/import", method="POST", body=document
        )
        assert status == 200
        assert payload["import"]["imported"] == 2
        assert payload["import"]["present"] == 0
        assert payload["import"]["skipped"] == 0
        assert payload["import"]["errors"] == []
        # Restored mail advances the unread counts: both entries sit
        # after the (absent) read cursor, so the fresh counts ride
        # along with the import result.
        assert payload["import"]["unread"]["total"] == 2
        assert payload["import"]["unread"]["per_peer"] == {PEER_HASH: 2}
        _, _, feed = _json(bridge2, "/api/messages")
        assert [m["text"] for m in feed["messages"]] == ["hello", "reply back"]
        assert feed["messages"][0]["direction"] == "in"
        assert feed["messages"][0]["peer_name"] == "alice"
        assert feed["messages"][1]["direction"] == "out"
        assert feed["messages"][1]["status"] == "delivered"
        assert feed["messages"][1]["peer_name"] == "alice"

        status, _, payload = _json(
            bridge2, "/api/messages/import", method="POST", body=document
        )
        assert status == 200
        assert payload["import"]["imported"] == 0
        assert payload["import"]["present"] == 2
        _, _, feed = _json(bridge2, "/api/messages")
        assert len(feed["messages"]) == 2

        # An export document is bigger than the 32 KiB cap for
        # ordinary actions; the import route accepts it.
        big = json.loads(json.dumps(document))
        big["messages"] = big["messages"] * 100
        assert len(json.dumps(big).encode("utf-8")) > 32 * 1024
        status, _, payload = _json(
            bridge2, "/api/messages/import", method="POST", body=big
        )
        assert status == 200
        assert payload["import"]["skipped"] == 0
    finally:
        bridge2.stop()


# -- received-file downloads -------------------------------------------------

SENDER_HASH = "ab" * 16


def _receive_file(node, tmp_path, *, sender, name, data, size=None, sha256=None, path=None):
    """Stage *data* as a file received from *sender* and record it."""
    from tau.config import TauConfig

    files_dir = node.config.files_dir
    sender_dir = files_dir / sender
    sender_dir.mkdir(parents=True, exist_ok=True)
    target = sender_dir / name
    target.write_bytes(data)
    record = {
        "kind": "file",
        "from": "alice",
        "destination": sender,
        "name": name,
        "size": len(data) if size is None else size,
        "sha256": hashlib.sha256(data).hexdigest() if sha256 is None else sha256,
        "path": str(target) if path is None else path,
        "sent_at": 1000.0,
        "received_at": 1001.0,
        "id": "00" * 16,
    }
    node.inbox.append(record)
    return record


def _with_config(node, tmp_path):
    from tau.config import TauConfig

    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")


def test_download_received_file(bridge, node, tmp_path):
    _with_config(node, tmp_path)
    data = b"hello file"
    _receive_file(node, tmp_path, sender=SENDER_HASH, name="notes.txt", data=data)
    status, headers, body = _request(
        bridge, f"/api/files/{SENDER_HASH}/notes.txt"
    )
    assert status == 200
    assert body == data
    assert headers["Content-Type"] == "application/octet-stream"
    assert headers["Content-Disposition"].startswith(
        'attachment; filename="notes.txt"'
    )
    assert headers["Content-Length"] == str(len(data))
    # A destination hash is case-insensitive: an uppercase sender is
    # normalised to the same hash, not rejected.
    status, _, body = _request(bridge, f"/api/files/{SENDER_HASH.upper()}/notes.txt")
    assert status == 200
    assert body == data


def test_download_response_is_not_followed_by_a_second_response(
    bridge, node, tmp_path
):
    data = b"hello file"
    _with_config(node, tmp_path)
    _receive_file(node, tmp_path, sender=SENDER_HASH, name="notes.txt", data=data)
    connection = http.client.HTTPConnection("127.0.0.1", bridge.port, timeout=5)
    try:
        connection.request("GET", f"/api/files/{SENDER_HASH}/notes.txt")
        response = connection.getresponse()
        assert response.status == 200
        assert response.read() == data
        # The download must end the response: a second request on the
        # same keep-alive connection has to get a clean answer, not
        # the stray bytes of a second response written onto the socket.
        connection.request("GET", "/api/status")
        second = connection.getresponse()
        assert second.status == 200
        second.read()
    finally:
        connection.close()


def test_download_received_file_unknown_is_404(bridge, node, tmp_path):
    _with_config(node, tmp_path)
    status, _, payload = _json(bridge, f"/api/files/{SENDER_HASH}/missing.txt")
    assert status == 404
    assert "no such received file" in payload["error"]["message"]
    status, _, _ = _json(bridge, f"/api/files/{'cd' * 16}/notes.txt")
    assert status == 404
    # Wrong shape: no name, or an extra segment, is a 404 too.
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}")
    assert status == 404
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}/a/b.txt")
    assert status == 404


def test_download_received_file_rejects_bad_sender_and_name(bridge, node, tmp_path):
    _with_config(node, tmp_path)
    for path in (
        "/api/files/short/notes.txt",
        "/api/files/" + "AB" + "ab" * 31 + "/notes.txt",
        "/api/files/" + SENDER_HASH + "/..%2F..%2Fetc-passwd",
        "/api/files/" + SENDER_HASH + "/a%2Fb.txt",
    ):
        status, _, payload = _json(bridge, path)
        assert status == 400, path
        assert payload["error"]["message"]


def test_download_received_file_rejects_tampered_bytes(bridge, node, tmp_path):
    _with_config(node, tmp_path)
    data = b"hello file"
    record = _receive_file(
        node, tmp_path, sender=SENDER_HASH, name="notes.txt", data=data
    )
    # Same length, different bytes: the stored SHA-256 no longer matches.
    from pathlib import Path

    Path(record["path"]).write_bytes(b"tampered!!")
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}/notes.txt")
    assert status == 404
    # A record whose size disagrees with the bytes on disk is not served.
    _receive_file(
        node, tmp_path, sender=SENDER_HASH, name="other.txt", data=b"abc", size=99
    )
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}/other.txt")
    assert status == 404


def test_download_received_file_rejects_escaped_path(bridge, node, tmp_path):
    from pathlib import Path

    _with_config(node, tmp_path)
    data = b"outside"
    outside = tmp_path / "outside.txt"
    outside.write_bytes(data)
    # The record points outside the files directory, with a matching
    # size and hash: still not served.
    node.inbox.append(
        {
            "kind": "file",
            "from": "alice",
            "destination": SENDER_HASH,
            "name": "outside.txt",
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "path": str(outside),
            "sent_at": 1000.0,
            "received_at": 1001.0,
            "id": "11" * 16,
        }
    )
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}/outside.txt")
    assert status == 404
    # A symlink inside the sender directory pointing out is resolved
    # to the same outside path and rejected the same way.
    sender_dir = node.config.files_dir / SENDER_HASH
    sender_dir.mkdir(parents=True, exist_ok=True)
    link = sender_dir / "linked.txt"
    link.symlink_to(outside)
    node.inbox.append(
        {
            "kind": "file",
            "from": "alice",
            "destination": SENDER_HASH,
            "name": "linked.txt",
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "path": str(link),
            "sent_at": 1000.0,
            "received_at": 1001.0,
            "id": "22" * 16,
        }
    )
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}/linked.txt")
    assert status == 404


def test_record_inbound_keeps_file_metadata(bridge):
    message = bridge.record_inbound(
        {
            "destination": SENDER_HASH,
            "from": "alice",
            "text": "Received file: notes.txt (10 bytes, sha256 ab…)",
            "sent_at": 1000.0,
            "received_at": 1001.0,
            "file": {"name": "notes.txt", "size": 10, "sha256": "ab" * 32},
        }
    )
    assert message["file"] == {"name": "notes.txt", "size": 10, "sha256": "ab" * 32}
    # File metadata that is not a safe bare name is dropped, not stored.
    message = bridge.record_inbound(
        {
            "destination": SENDER_HASH,
            "from": "alice",
            "text": "Received file: x (10 bytes, sha256 ab…)",
            "sent_at": 1000.0,
            "received_at": 1001.0,
            "file": {"name": "../x", "size": 10, "sha256": "ab" * 32},
        }
    )
    assert "file" not in message


def test_stored_message_keeps_file_metadata(tmp_path):
    from tau.config import TauConfig

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    try:
        bridge.record_inbound(
            {
                "destination": SENDER_HASH,
                "from": "alice",
                "text": "Received file: notes.txt (10 bytes, sha256 ab…)",
                "sent_at": 1000.0,
                "received_at": 1001.0,
                "file": {"name": "notes.txt", "size": 10, "sha256": "ab" * 32},
            }
        )
    finally:
        bridge.stop()
    reloaded = WebBridge(node, host="127.0.0.1", port=0)
    try:
        assert reloaded.messages()[0]["file"] == {
            "name": "notes.txt",
            "size": 10,
            "sha256": "ab" * 32,
        }
    finally:
        reloaded.stop()


def test_download_received_file_blocked_sender_is_404(bridge, node, tmp_path):
    from tau.blocklist import BlockList

    _with_config(node, tmp_path)
    _receive_file(
        node, tmp_path, sender=SENDER_HASH, name="notes.txt", data=b"hello file"
    )
    node.blocklist = BlockList(blocked=frozenset({SENDER_HASH}))
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}/notes.txt")
    assert status == 404


def test_download_received_file_symlink_loop_is_404(bridge, node, tmp_path):
    _with_config(node, tmp_path)
    sender_dir = node.config.files_dir / SENDER_HASH
    sender_dir.mkdir(parents=True, exist_ok=True)
    loop = sender_dir / "loop.txt"
    loop.symlink_to(loop)
    node.inbox.append(
        {
            "kind": "file",
            "from": "alice",
            "destination": SENDER_HASH,
            "name": "loop.txt",
            "size": 10,
            "sha256": "ab" * 32,
            "path": str(loop),
            "sent_at": 1000.0,
            "received_at": 1001.0,
            "id": "33" * 16,
        }
    )
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}/loop.txt")
    assert status == 404


def test_download_received_file_oversized_replacement_is_404(bridge, node, tmp_path):
    from pathlib import Path

    _with_config(node, tmp_path)
    record = _receive_file(
        node, tmp_path, sender=SENDER_HASH, name="notes.txt", data=b"hello file"
    )
    # The bytes are replaced by something far larger than the recorded
    # size: the download must 404 without loading the whole file.
    Path(record["path"]).write_bytes(b"x" * (1024 * 1024))
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}/notes.txt")
    assert status == 404


def test_send_file_is_recorded_with_file_metadata(bridge, node):
    import hashlib

    data = b"hello mesh"
    message, response = bridge.send_file("alice", "notes.txt", data)
    assert message["direction"] == "out"
    assert message["status"] == "delivered"
    assert message["text"] == "Sent file: notes.txt (10 bytes)"
    assert message["file"] == {
        "name": "notes.txt",
        "size": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    assert message["wire_id"] == response["id"]
    assert response["name"] == "notes.txt"
    assert node.sent_files == [("alice", "notes.txt", data)]
    # The feed carries the entry, and the bus tells live consoles.
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["file"]["sha256"] == hashlib.sha256(data).hexdigest()
    published = [e for e in bridge.bus.replay(0) if e.get("type") == "message"]
    assert published[-1]["message"]["id"] == message["id"]


def test_send_file_sanitises_the_name(bridge, node):
    message, _ = bridge.send_file("alice", "../escape.txt", b"x")
    assert message["file"]["name"] == "escape.txt"
    assert node.sent_files[0][1] == "escape.txt"


def test_send_file_validation(bridge):
    from tau.protocol import MAX_FILE_BYTES

    with pytest.raises(ValueError, match="not usable"):
        bridge.send_file("alice", "..", b"x")
    with pytest.raises(ValueError, match="outside the"):
        bridge.send_file("alice", "a.txt", b"")
    with pytest.raises(ValueError, match="outside the"):
        bridge.send_file("alice", "a.txt", b"x" * (MAX_FILE_BYTES + 1))
    with pytest.raises(ValueError, match="unknown peer"):
        bridge.send_file("nobody", "a.txt", b"x")


def test_send_file_survives_bridge_restart(node, tmp_path):
    import hashlib

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        data = b"restart me"
        bridge.send_file("alice", "keep.txt", data)
    finally:
        bridge.stop()
    stored = json.loads((tmp_path / "messages.json").read_text(encoding="utf-8"))
    assert stored["messages"][0]["file"]["name"] == "keep.txt"

    bridge2 = _bridge_with_data_dir(FakeNode(), tmp_path)
    try:
        [loaded] = bridge2.messages()
        assert loaded["file"] == {
            "name": "keep.txt",
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    finally:
        bridge2.stop()


def test_messages_import_rejects_wrong_format(bridge):
    status, _, payload = _json(
        bridge, "/api/messages/import", method="POST", body={"format": "nope"}
    )
    assert status == 400
    assert "unsupported export format" in payload["error"]["message"]


def test_messages_import_skips_bad_entries(bridge):
    document = {
        "format": "tau-messages-export",
        "version": 1,
        "exported_at": 1000.0,
        "messages": [
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "good one",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "good-1",
            },
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "bad-1",
            },
        ],
    }
    status, _, payload = _json(
        bridge, "/api/messages/import", method="POST", body=document
    )
    assert status == 200
    assert payload["import"]["imported"] == 1
    assert payload["import"]["skipped"] == 1
    assert len(payload["import"]["errors"]) == 1
    assert payload["import"]["errors"][0]["id"] == "bad-1"
    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == ["good one"]


def test_messages_import_skips_expired_in_feed(bridge):
    document = {
        "format": "tau-messages-export",
        "version": 1,
        "exported_at": 1000.0,
        "messages": [
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "already gone",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "gone-1",
                "expires_at": 200.0,
            }
        ],
    }
    status, _, payload = _json(
        bridge, "/api/messages/import", method="POST", body=document
    )
    assert status == 200
    assert payload["import"]["imported"] == 1
    # The restore landed in the inbox, but a message whose deadline
    # already passed is not resurrected into the feed.
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []
    _, _, exported = _json(bridge, "/api/messages/export")
    assert len(exported["messages"]) == 1


def test_messages_export_too_large_for_import_is_refused(bridge, node, monkeypatch):
    import tau.webapi as webapi

    _inbound_record(node)
    monkeypatch.setattr(webapi, "MAX_IMPORT_BYTES", 10)
    status, _, payload = _json(bridge, "/api/messages/export")
    assert status == 400
    assert "command line" in payload["error"]["message"]


def test_messages_import_loopback_guard():
    from tau.webapi import _is_loopback_client

    assert _is_loopback_client("127.0.0.1") is True
    assert _is_loopback_client("::1") is True
    assert _is_loopback_client("192.168.1.5") is False
    assert _is_loopback_client("not an address") is False


def test_messages_import_hides_blocked_sender_from_events(bridge, node):
    from tau.blocklist import BlockList

    document = {
        "format": "tau-messages-export",
        "version": 1,
        "messages": [
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "from alice",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "m-1",
                "direction": "in",
            },
            {
                "from": "mallory",
                "destination": OTHER_HASH,
                "text": "from mallory",
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "m-2",
                "direction": "in",
            },
        ],
    }
    node.blocklist = BlockList(blocked=frozenset({OTHER_HASH}))
    status, _, payload = _json(
        bridge, "/api/messages/import", method="POST", body=document
    )
    assert status == 200
    assert payload["import"]["imported"] == 2
    # Only the unblocked sender's restore is announced to live consoles.
    published = [
        event["message"]["text"]
        for event in bridge.bus.replay(0)
        if event.get("type") == "message"
        and event["message"].get("text") in ("from alice", "from mallory")
    ]
    assert published == ["from alice"]
    # The blocked record is still stored: unblocking restores the view.
    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == ["from alice"]
    node.blocklist = BlockList()
    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == [
        "from alice",
        "from mallory",
    ]


def test_send_file_endpoint(bridge, node):
    import base64

    data = base64.b64encode(b"hello mesh").decode("ascii")
    status, _, payload = _json(
        bridge,
        "/api/send-file",
        method="POST",
        body={"to": "alice", "name": "notes.txt", "data": data},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["message"]["file"]["name"] == "notes.txt"
    assert payload["response"]["size"] == 10
    assert node.sent_files == [("alice", "notes.txt", b"hello mesh")]


def test_send_file_endpoint_rejects_bad_requests(bridge):
    for body, fragment in (
        ({"name": "a.txt", "data": "eA=="}, "to"),
        ({"to": "alice", "data": "eA=="}, "name"),
        ({"to": "alice", "name": "a.txt"}, "data"),
        ({"to": "alice", "name": "a.txt", "data": "not base64!!"}, "data"),
        ({"to": "nobody", "name": "a.txt", "data": "eA=="}, "unknown peer"),
    ):
        status, _, payload = _json(
            bridge, "/api/send-file", method="POST", body=body
        )
        assert status == 400
        assert fragment in payload["error"]["message"]


def test_send_file_endpoint_accepts_bodies_over_the_json_cap(bridge, node):
    import base64

    # 40 KiB of file is ~55 KiB base64: far over the 32 KiB JSON cap,
    # but well under this endpoint's 15 MiB cap.
    data = base64.b64encode(bytes(40 * 1024)).decode("ascii")
    status, _, payload = _json(
        bridge,
        "/api/send-file",
        method="POST",
        body={"to": "alice", "name": "big.bin", "data": data},
    )
    assert status == 200
    assert payload["message"]["file"]["size"] == 40 * 1024
    assert node.sent_files[0][1] == "big.bin"


def test_other_endpoints_keep_the_json_body_cap(bridge):
    # The same oversized body to /api/messages must still be refused.
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "x" * (40 * 1024)},
    )
    assert status == 400
    assert "too large" in payload["error"]["message"]
