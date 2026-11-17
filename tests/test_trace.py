"""Mesh diagnostics: ``tau trace`` and ``tau mesh`` (issue #470).

The node methods read Reticulum's path table, so the transport is
stubbed here: ``has_path``/``hops_to`` decide what the node sees, and
the ping probe is stubbed to control the round-trip without a link.
"""
from __future__ import annotations

import RNS
import pytest

from tau.config import TauConfig
from tau.node import NodeError, TauNode
from tau.peers import Peer, PeerRegistry
from tau.protocol import APP_NAME, ASPECT


def make_peer(name: str = "alice", *, last_seen: float = 1000.0) -> Peer:
    identity = RNS.Identity()
    destination = RNS.Destination.hash(identity, APP_NAME, ASPECT)
    return Peer(
        destination_hash=destination,
        name=name,
        public_key=identity.get_public_key(),
        first_seen=last_seen,
        last_seen=last_seen,
    )


def make_node(tmp_path):
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    return TauNode(config, registry=PeerRegistry(config.peers_path))


def _stub_transport(monkeypatch, *, paths):
    """Stub the path table: *paths* maps destination bytes to hops."""

    def has_path(dest_hash):
        return dest_hash in paths

    def hops_to(dest_hash):
        return paths.get(dest_hash, RNS.Transport.PATHFINDER_M)

    monkeypatch.setattr(RNS.Transport, "has_path", has_path)
    monkeypatch.setattr(RNS.Transport, "hops_to", hops_to)
    monkeypatch.setattr(
        RNS.Transport, "request_path", lambda dest_hash: None
    )


def test_trace_reports_path_hops_and_probe_rtt(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1000.0
    )
    _stub_transport(monkeypatch, paths={peer.destination_hash: 3})
    monkeypatch.setattr(
        node, "ping", lambda target, **kwargs: {"ok": True, "_rtt": 0.412}
    )
    result = node.trace("alice", timeout=5.0)
    assert result["peer"] == "alice"
    assert result["destination"] == peer.hexhash
    assert result["has_path"] is True
    assert result["hops"] == 3
    assert result["rtt_ms"] == 412.0
    assert result["reachable"] is True
    assert result["last_seen"] == 1000.0
    assert result["probe_error"] is None


def test_trace_without_path_skips_probe_and_reports_last_seen(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1000.0
    )
    _stub_transport(monkeypatch, paths={})
    calls = []
    monkeypatch.setattr(
        node, "ping", lambda target, **kwargs: calls.append(target) or {}
    )
    result = node.trace("alice", timeout=0.5)
    assert result["has_path"] is False
    assert result["hops"] is None
    assert result["rtt_ms"] is None
    assert result["reachable"] is False
    assert result["last_seen"] == 1000.0
    assert calls == []


def test_trace_known_path_with_failed_probe_is_not_reachable(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1000.0
    )
    _stub_transport(monkeypatch, paths={peer.destination_hash: 2})

    def failing_ping(target, **kwargs):
        raise NodeError("no response to /tau/ping within 5.0s")

    monkeypatch.setattr(node, "ping", failing_ping)
    result = node.trace("alice", timeout=5.0)
    # The path is still reported: the peer may be up but not answering.
    assert result["has_path"] is True
    assert result["hops"] == 2
    assert result["rtt_ms"] is None
    assert result["reachable"] is False
    assert "no response" in (result["probe_error"] or "")


def test_trace_unknown_peer_raises(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    _stub_transport(monkeypatch, paths={})
    with pytest.raises(NodeError):
        node.trace("nobody", timeout=1.0)


def test_mesh_snapshot_uses_live_path_table_not_registry_hops(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    alice = make_peer("alice")
    bob = make_peer("bob")
    # Registry remembers alice at 5 hops from announce time; the live
    # table says 2 now, and bob has no path at all.
    node.registry.upsert(
        alice.destination_hash,
        alice.public_key,
        name="alice",
        hops=5,
        now=1000.0,
    )
    node.registry.upsert(
        bob.destination_hash,
        bob.public_key,
        name="bob",
        hops=1,
        now=900.0,
    )
    _stub_transport(monkeypatch, paths={alice.destination_hash: 2})
    rows = node.mesh_snapshot()
    assert [row["name"] for row in rows] == ["alice", "bob"]
    assert rows[0]["hops"] == 2
    assert rows[0]["has_path"] is True
    assert rows[0]["reachable"] is True
    assert rows[0]["last_seen"] == 1000.0
    assert rows[1]["hops"] is None
    assert rows[1]["has_path"] is False
    assert rows[1]["reachable"] is False
    assert rows[1]["last_seen"] == 900.0


def test_mesh_snapshot_empty_registry(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    _stub_transport(monkeypatch, paths={})
    assert node.mesh_snapshot() == []


def test_mesh_snapshot_tolerates_transport_not_up(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1000.0
    )

    def broken_has_path(dest_hash):
        raise RuntimeError("transport not started")

    monkeypatch.setattr(RNS.Transport, "has_path", broken_has_path)
    rows = node.mesh_snapshot()
    assert len(rows) == 1
    assert rows[0]["reachable"] is False
    assert rows[0]["hops"] is None


def test_trace_requests_path_when_none_known(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1000.0
    )
    # First read: no path. The explicit request_path then surfaces
    # one, as it would when the peer re-announces mid-trace.
    state = {"requested": False}
    paths = {}

    def has_path(dest_hash):
        return dest_hash in paths

    def hops_to(dest_hash):
        return paths.get(dest_hash, RNS.Transport.PATHFINDER_M)

    def request_path(dest_hash):
        state["requested"] = True
        paths[dest_hash] = 1

    monkeypatch.setattr(RNS.Transport, "has_path", has_path)
    monkeypatch.setattr(RNS.Transport, "hops_to", hops_to)
    monkeypatch.setattr(RNS.Transport, "request_path", request_path)
    monkeypatch.setattr(
        node, "ping", lambda target, **kwargs: {"ok": True, "_rtt": 0.05}
    )
    result = node.trace("alice", timeout=5.0)
    assert state["requested"] is True
    assert result["has_path"] is True
    assert result["hops"] == 1
    assert result["reachable"] is True
