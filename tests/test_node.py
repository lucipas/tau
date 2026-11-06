from __future__ import annotations

import logging
import time

import RNS
import pytest

from tau.config import TauConfig
from tau.node import NodeError, TauNode
from tau.peers import Peer, PeerRegistry
from tau.protocol import APP_NAME, ASPECT


def make_peer(name: str = "alice") -> Peer:
    identity = RNS.Identity()
    destination = RNS.Destination.hash(identity, APP_NAME, ASPECT)
    return Peer(
        destination_hash=destination,
        name=name,
        public_key=identity.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
    )


def make_node(tmp_path):
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    return TauNode(config, registry=PeerRegistry(config.peers_path))


def test_destination_for_rebuilds_the_same_destination(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer()
    destination = node._destination_for(peer)
    assert destination.hash == peer.destination_hash
    assert destination.hexhash == peer.hexhash


def test_destination_for_rejects_mismatched_identity(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer()
    peer.destination_hash = b"\x00" * 16
    with pytest.raises(NodeError):
        node._destination_for(peer)


def test_resolve_peer_uses_registry_when_not_started(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    assert node.resolve_peer(peer.hexhash) is not None
    assert node.resolve_peer("alice") is not None
    assert node.resolve_peer(peer) is peer


def test_resolve_unknown_peer_without_identity_data(tmp_path):
    node = make_node(tmp_path)
    with pytest.raises(NodeError):
        node.resolve_peer("nobody")
    with pytest.raises(NodeError):
        node.resolve_peer(b"\x00" * 16)


def test_info_requires_start(tmp_path):
    node = make_node(tmp_path)
    with pytest.raises(NodeError):
        node.info()
    with pytest.raises(NodeError):
        node.send_message("alice", "hi")


def test_node_cannot_be_restarted_after_shutdown(tmp_path):
    node = make_node(tmp_path)
    node._shutdown = True
    with pytest.raises(NodeError, match="cannot be restarted"):
        node.start()


def test_save_peers_prunes_stale_peers_but_keeps_owed_ones(tmp_path, caplog):
    node = make_node(tmp_path)
    stale = make_peer("stale")
    fresh = make_peer("fresh")
    owed = make_peer("owed")
    node.registry.upsert(
        stale.destination_hash, stale.public_key, name="stale", now=1.0
    )
    node.registry.upsert(fresh.destination_hash, fresh.public_key, name="fresh")
    node.registry.upsert(owed.destination_hash, owed.public_key, name="owed", now=1.0)
    node.mailbox.enqueue(to=owed.destination_hash, text="hello")

    with caplog.at_level(logging.DEBUG, logger="tau.node"):
        node._save_peers(force=True)

    assert "Pruned 1 stale peer(s) from the registry" in caplog.text
    assert node.registry.get(stale.destination_hash) is None
    assert node.registry.get(fresh.destination_hash) is not None
    assert node.registry.get(owed.destination_hash) is not None

    reloaded = PeerRegistry(node.registry.path)
    assert reloaded.get(stale.destination_hash) is None
    assert len(reloaded) == 2


def test_save_peers_keeps_holder_with_pending_acks(tmp_path):
    node = make_node(tmp_path)
    holder = make_peer("holder")
    node.registry.upsert(
        holder.destination_hash, holder.public_key, name="holder", now=1.0
    )
    node.mailbox.add_pending_acks(holder.hexhash, ["message-1"])

    node._save_peers(force=True)

    assert node.registry.get(holder.destination_hash) is not None
    assert node.mailbox.pending_ack_holders() == [holder.hexhash]


# -- peer exchange -------------------------------------------------------

from tau.protocol import decode  # noqa: E402


def make_exchange_entry(name="carol", *, last_seen=1000.0, hops=2):
    identity = RNS.Identity()
    destination = RNS.Destination.hash(identity, APP_NAME, ASPECT)
    entry = {
        "destination": destination.hex(),
        "name": name,
        "public_key": identity.get_public_key().hex(),
        "last_seen": last_seen,
        "hops": hops,
        "protocol": 1,
    }
    return entry, destination


def test_handle_peers_serves_exchange_entries(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer("alice")
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=time.time()
    )
    raw = node._handle_peers("/tau/peers", b"{}", b"request-id", None, time.time())
    response = decode(raw)

    assert response["ok"] is True
    assert response["count"] == 1
    served = response["peers"][0]
    assert served["destination"] == peer.hexhash
    assert served["public_key"] == peer.public_key.hex()


def test_exchange_peers_merges_new_peers(tmp_path):
    node = make_node(tmp_path)
    source = make_peer("bob")
    node.registry.upsert(
        source.destination_hash, source.public_key, name="bob", now=100.0
    )
    entry, destination = make_exchange_entry("carol", last_seen=time.time())
    node._request = lambda peer, path, payload, timeout: {
        "ok": True,
        "peers": [entry],
        "count": 1,
    }

    summary = node.exchange_peers(source)

    assert summary["source"] == source.hexhash
    assert summary["added"] == 1
    assert summary["skipped"] == 0
    learned = node.registry.get(destination)
    assert learned is not None
    assert learned.name == "carol"
    assert learned.public_key.hex() == entry["public_key"]


def test_exchange_peers_rejects_malformed_response(tmp_path):
    node = make_node(tmp_path)
    source = make_peer("bob")
    node._request = lambda peer, path, payload, timeout: {"ok": True}
    with pytest.raises(Exception, match="invalid peers response"):
        node.exchange_peers(source)


def test_mail_round_exchanges_peers(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer("bob")
    calls = []
    node._flush_peer = lambda p, timeout: calls.append(("flush", p.hexhash))
    node._collect_mail = lambda p, timeout: calls.append(("collect", p.hexhash))
    node.exchange_peers = lambda p, **kwargs: calls.append(("exchange", p.hexhash))

    node._mail_round_for_peer(peer)

    assert calls == [
        ("flush", peer.hexhash),
        ("collect", peer.hexhash),
        ("exchange", peer.hexhash),
    ]


def test_mail_round_survives_failed_exchange(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer("bob")
    node._flush_peer = lambda p, timeout: {"delivered": 0, "deposited": 0}
    node._collect_mail = lambda p, timeout: 0

    def failing_exchange(p, **kwargs):
        raise RuntimeError("link broke")

    node.exchange_peers = failing_exchange
    node._mail_round_for_peer(peer)  # must not raise
