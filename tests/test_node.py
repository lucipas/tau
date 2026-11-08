from __future__ import annotations

import logging
import threading
import time

import RNS
import pytest

from tau.config import TauConfig
from tau.mailbox import Mailbox
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


def test_destination_for_tolerates_none_returning_load(
    tmp_path, load_public_key_without_return
):
    # RNS 1.4.1's load_public_key returns None even on success; the
    # node must still resolve a peer's destination from its key.
    node = make_node(tmp_path)
    peer = make_peer()
    destination = node._destination_for(peer)
    assert destination.hash == peer.destination_hash


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
    node._flush_peer = lambda p, timeout, **_kwargs: calls.append(("flush", p.hexhash))
    node._collect_mail = lambda p, timeout: calls.append(("collect", p.hexhash))

    def exchange(p, **kwargs):
        # The exchange runs after the mail lock is released, so a slow
        # peer cannot hold up mail delivery.
        assert not node._mail_lock.locked()
        calls.append(("exchange", p.hexhash))

    node.exchange_peers = exchange

    node._mail_round_for_peer(peer)

    assert calls == [
        ("flush", peer.hexhash),
        ("collect", peer.hexhash),
        ("exchange", peer.hexhash),
    ]
    assert node._exchanged_at[peer.destination_hash] > 0


def test_mail_round_survives_failed_exchange(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer("bob")
    node._flush_peer = lambda p, timeout, **_kwargs: {"delivered": 0, "deposited": 0}
    node._collect_mail = lambda p, timeout: 0

    def failing_exchange(p, **kwargs):
        raise RuntimeError("link broke")

    node.exchange_peers = failing_exchange
    node._mail_round_for_peer(peer)  # must not raise


def test_exchange_sweep_asks_least_recent_first(tmp_path):
    node = make_node(tmp_path)
    first = make_peer("first")
    second = make_peer("second")
    third = make_peer("third")
    for peer, when in ((first, 100.0), (second, 200.0), (third, 300.0)):
        node.registry.upsert(
            peer.destination_hash, peer.public_key, name=peer.name, now=when
        )
    # Second was asked recently; the sweep should skip it and ask the
    # other two.  Never-asked peers keep registry (most-recently-seen) order.
    node._exchanged_at[second.destination_hash] = time.time()
    asked = []
    node.exchange_peers = lambda p, **kwargs: asked.append(p.hexhash)

    node._exchange_sweep(max_peers=2)

    assert asked == [third.hexhash, first.hexhash]


def test_exchange_sweep_skips_recently_asked(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer("only")
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="only", now=100.0
    )
    node._exchanged_at[peer.destination_hash] = time.time()
    asked = []
    node.exchange_peers = lambda p, **kwargs: asked.append(p.hexhash)

    node._exchange_sweep(max_peers=2)

    assert asked == []


def test_flush_outbox_prefers_direct_peers(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()  # started enough for flush_outbox
    direct = make_peer("direct")
    node.registry.upsert(
        direct.destination_hash, direct.public_key, name="direct", now=100.0
    )
    # A peer learned only via exchange, with a much fresher reported
    # last_seen, must still come after directly heard peers in the sweep.
    entry, destination = make_exchange_entry("gossip", last_seen=time.time())
    node.registry.merge_exchanged([entry], now=time.time())
    assert node.registry.get(destination).via_exchange is True

    order = []
    node._flush_peer = lambda p, timeout, **_kwargs: (
        order.append(p.hexhash),
        {"delivered": 0, "deposited": 0},
    )[1]
    node._collect_mail = lambda p, timeout: 0

    node.flush_outbox()

    assert order == [direct.hexhash, destination.hex()]


def test_flush_outbox_to_filter_contacts_only_that_peer(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()  # started enough for flush_outbox
    alice = make_peer("alice")
    bob = make_peer("bob")
    node.registry.upsert(
        alice.destination_hash, alice.public_key, name="alice", now=100.0
    )
    node.registry.upsert(bob.destination_hash, bob.public_key, name="bob", now=100.0)
    node.mailbox.enqueue(
        to=alice.destination_hash, text="for alice", message_id="aa" + "11" * 15
    )

    calls = []

    def _flush_peer(peer, timeout, **kwargs):
        calls.append((peer.hexhash, kwargs.get("respect_backoff")))
        return {"delivered": 0, "deposited": 0}

    node._flush_peer = _flush_peer
    node._collect_mail = lambda p, timeout: 0

    # A retry aimed at bob must not spend its budget on alice.
    node.flush_outbox(to=bob.destination_hash, respect_backoff=False)
    assert calls == [(bob.hexhash, False)]

    node.flush_outbox(to=alice.destination_hash, respect_backoff=False)
    assert calls == [(bob.hexhash, False), (alice.hexhash, False)]

    # An unknown recipient contacts nobody instead of sweeping the registry.
    node.flush_outbox(to=b"\x00" * 16, respect_backoff=False)
    assert calls == [(bob.hexhash, False), (alice.hexhash, False)]


def test_flush_outbox_skips_when_lock_busy_unless_waiting(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()  # started enough for flush_outbox
    peer = make_peer("alice")
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=100.0
    )
    node.mailbox.enqueue(
        to=peer.destination_hash, text="for alice", message_id="aa" + "11" * 15
    )

    calls = []
    node._flush_peer = lambda p, timeout, **_kwargs: (
        calls.append(p.hexhash),
        {"delivered": 0, "deposited": 0},
    )[1]
    node._collect_mail = lambda p, timeout: 0

    # A background round holding the lock makes a plain flush skip,
    # and the summary says so instead of looking like a quiet sweep.
    node._mail_lock.acquire()
    try:
        assert node.flush_outbox() == {
            "delivered": 0,
            "deposited": 0,
            "received": 0,
            "lock_busy": True,
        }
        assert calls == []
        # A bounded wait that expires reports the same, rather than
        # silently pretending the retry was attempted.
        assert node.flush_outbox(wait_for_lock=0.2) == {
            "delivered": 0,
            "deposited": 0,
            "received": 0,
            "lock_busy": True,
        }
        assert calls == []
    finally:
        node._mail_lock.release()

    # With wait_for_lock, the same flush waits for the round to finish
    # and then runs instead of reporting nothing moved.
    acquired = threading.Event()

    def _hold_then_release():
        node._mail_lock.acquire()
        acquired.set()
        time.sleep(0.2)
        node._mail_lock.release()

    holder = threading.Thread(target=_hold_then_release)
    holder.start()
    assert acquired.wait(timeout=5.0)
    assert node.flush_outbox(wait_for_lock=5.0) == {
        "delivered": 0,
        "deposited": 0,
        "received": 0,
        "lock_busy": False,
    }
    holder.join(timeout=5.0)
    assert calls == [peer.hexhash]


def test_wait_for_mail_rounds_joins_in_flight_rounds(tmp_path):
    node = make_node(tmp_path)
    peer = make_peer("alice")
    started = threading.Event()
    finished: list = []

    def _slow_round(other: Peer) -> None:
        started.set()
        time.sleep(0.2)
        finished.append(other.hexhash)

    node._mail_round_for_peer = _slow_round
    node._spawn_mail_round(peer)
    assert started.wait(timeout=5.0)
    assert node.wait_for_mail_rounds(timeout=5.0) is True
    assert finished == [peer.hexhash]

    # A round that outlasts the budget reports False instead of hanging.
    node._mail_round_for_peer = lambda other: time.sleep(2.0)
    node._spawn_mail_round(peer)
    assert node.wait_for_mail_rounds(timeout=0.1) is False
    assert node.wait_for_mail_rounds(timeout=5.0) is True
def test_deliver_entry_skips_message_cancelled_by_another_process(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    entry = node.mailbox.enqueue(
        to=b"\xbb" * 16, text="do not send", message_id="cc" + "33" * 15
    )
    # A second process (``tau outbox cancel``) removes the entry from the
    # shared mailbox file after this node's sweep listed it.
    other = Mailbox(node.config.mailbox_path)
    assert other.remove_outbox(entry.id) is True

    calls = []
    monkeypatch.setattr(
        node, "_request", lambda *args, **kwargs: calls.append(args)
    )
    assert node._deliver_entry(make_peer(), entry, timeout=1.0) is False
    assert calls == []


def test_deliver_entry_sends_message_still_queued(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    entry = node.mailbox.enqueue(
        to=b"\xbb" * 16, text="send me", message_id="cc" + "44" * 15
    )

    calls = []
    monkeypatch.setattr(
        node, "_request", lambda *args, **kwargs: calls.append(args)
    )
    assert node._deliver_entry(make_peer(), entry, timeout=1.0) is True
    assert len(calls) == 1
    assert node.mailbox.get_outbox(entry.id) is None


def test_deliver_entry_carries_reply_reference(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    reply_to = {"id": "a" * 32, "excerpt": "0123456789abcdef"}
    entry = node.mailbox.enqueue(
        to=b"\xbb" * 16,
        text="seconded",
        message_id="cc" + "66" * 15,
        in_reply_to=reply_to,
    )

    calls = []
    monkeypatch.setattr(
        node, "_request", lambda *args, **kwargs: calls.append(args)
    )
    assert node._deliver_entry(make_peer(), entry, timeout=1.0) is True
    assert len(calls) == 1
    from tau.protocol import parse_message

    sent = parse_message(calls[0][2])
    assert sent["in_reply_to"] == reply_to
    assert sent["text"] == "seconded"


def test_send_message_records_sent_message_for_quote_lookup(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    monkeypatch.setattr(node, "resolve_peer", lambda *args, **kwargs: peer)
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: {"ok": True, "echo": "hello"},
    )
    reply_to = {"id": "a" * 32, "excerpt": "0123456789abcdef"}

    response = node.send_message(peer, "hello", in_reply_to=reply_to)

    assert response["ok"] is True
    assert response["id"]
    # The sent message is recorded so the peer's reply to it renders
    # from local history; it stays out of the received listing.
    assert node.inbox.list_messages() == []
    sent = node.inbox.list_messages(direction="out")
    assert len(sent) == 1
    assert sent[0]["text"] == "hello"
    assert sent[0]["destination"] == peer.hexhash
    assert sent[0]["in_reply_to"] == reply_to
    assert node.inbox.find(response["id"], peer=peer.hexhash) is not None


def test_send_message_ignores_peer_supplied_id(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    monkeypatch.setattr(node, "resolve_peer", lambda *args, **kwargs: peer)
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: {"ok": True, "id": "ff" * 16},
    )

    response = node.send_message(peer, "hello")

    assert response["id"] != "ff" * 16
    sent = node.inbox.list_messages(direction="out")
    assert sent[0]["id"] == response["id"]


def test_send_message_queues_reply_when_peer_is_offline(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    peer = make_peer()
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name=peer.name, now=1.0
    )
    monkeypatch.setattr(node, "resolve_peer", lambda *args, **kwargs: peer)
    monkeypatch.setattr(node, "reticulum", object())

    def _boom(*args, **kwargs):
        from tau.node import NodeError

        raise NodeError("no route")

    monkeypatch.setattr(node, "_request", _boom)
    monkeypatch.setattr(node, "flush_outbox", lambda *args, **kwargs: None)
    reply_to = {"id": "a" * 32, "excerpt": "0123456789abcdef"}

    response = node.send_message(peer, "seconded", in_reply_to=reply_to)

    assert response["queued"] is True
    entry = node.mailbox.get_outbox(response["id"])
    assert entry is not None
    assert entry.in_reply_to == reply_to
    assert node.inbox.list_messages(direction="out")[0]["id"] == response["id"]


def test_deposit_entry_skips_message_cancelled_by_another_process(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    node.identity = RNS.Identity()
    recipient = make_peer("bob")
    node.registry.upsert(
        recipient.destination_hash, recipient.public_key, name="bob", now=1.0
    )
    entry = node.mailbox.enqueue(
        to=recipient.destination_hash,
        text="do not hold",
        message_id="cc" + "55" * 15,
    )
    other = Mailbox(node.config.mailbox_path)
    assert other.remove_outbox(entry.id) is True

    calls = []
    monkeypatch.setattr(
        node, "_request", lambda *args, **kwargs: calls.append(args)
    )
    assert node._deposit_entry(make_peer("carol"), entry, timeout=1.0) is False
    assert calls == []
