"""Node-level store-and-forward behaviour that needs no live network."""

from __future__ import annotations

import time
from types import SimpleNamespace

import RNS
import pytest

from tau.config import TauConfig
from tau.identity import destination_hash
from tau.mailbox import open_message, seal_message
from tau.node import NodeError, TauNode
from tau.peers import Peer, PeerRegistry
from tau.protocol import (
    build_mail_fetch,
    build_mail_store,
    decode,
    encode_message,
)


def make_node(tmp_path, name="holder", on_message=None, on_delivered=None):
    config = TauConfig.resolve(data_dir=tmp_path, name=name)
    node = TauNode(
        config,
        registry=PeerRegistry(config.peers_path),
        on_message=on_message,
        on_delivered=on_delivered,
    )
    return node


def call_handler(handler, data, remote_identity=None):
    return decode(
        handler("/tau/test", data, b"request-id", remote_identity, time.time())
    )


def test_direct_message_dedupes_by_id(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )

    payload = encode_message("hello once", message_id="dup-1")
    first = call_handler(node._handle_message, payload)
    second = call_handler(node._handle_message, payload)

    assert first["ok"] is True and first["duplicate"] is False
    assert second["ok"] is True and second["duplicate"] is True
    assert len(received) == 1
    assert received[0]["id"] == "dup-1"
    # The first delivery is recorded in the durable inbox; the duplicate
    # (already seen) is not recorded twice.
    assert len(node.inbox) == 1
    assert node.inbox.list_messages()[0]["text"] == "hello once"
    assert node.inbox.path.exists()


def test_direct_message_keeps_kind(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )

    payload = encode_message("are you there?", message_id="kind-1", kind="ping")
    response = call_handler(node._handle_message, payload)

    assert response["ok"] is True
    assert response["kind"] == "ping"
    assert received[0]["kind"] == "ping"
    assert node.inbox.list_messages()[0]["kind"] == "ping"


def test_direct_message_without_kind_is_chat(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )

    # A peer that predates kinds sends no kind field at all: build the
    # payload by hand instead of through encode_message, which always
    # stamps the kind now.
    from tau.protocol import encode as encode_payload

    payload = encode_payload(
        {"type": "message", "v": 1, "text": "old peer here", "sent_at": 1.0,
         "id": "kind-2"}
    )
    response = call_handler(node._handle_message, payload)

    assert response["ok"] is True
    assert response["kind"] == "chat"
    assert node.inbox.list_messages()[0]["kind"] == "chat"


def test_relayed_message_keeps_kind(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()

    message = {
        "id": "relay-kind-1",
        "from": destination_hash(sender).hex(),
        "from_name": "alice",
        "text": "doorbell rang",
        "kind": "alert",
        "sent_at": 42.0,
    }
    assert node._deliver_collected(message) is True
    assert received[0]["kind"] == "alert"
    assert received[0]["relayed"] is True
    assert node.inbox.list_messages()[0]["kind"] == "alert"


def test_direct_message_records_destination_hash_for_unknown_sender(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    response = call_handler(
        node._handle_message,
        encode_message("hello stranger", message_id="stranger-1"),
        remote_identity=sender,
    )
    assert response["ok"] is True
    entry = node.inbox.list_messages()[0]
    # The inbox keys on Tau destination hashes, not the RNS identity hash
    # _caller_label falls back to for senders outside the registry.
    assert entry["destination"] == destination_hash(sender).hex()
    assert entry["destination"] != sender.hexhash
    assert entry["from"] == destination_hash(sender).hex()[:8]


def test_direct_message_with_bad_sent_at_is_still_recorded(tmp_path):
    import json
    import math

    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    # A peer can send sent_at: 1e400 as raw JSON even though tau's own
    # encoder refuses non-finite floats; the parser accepts it.
    raw = json.dumps(
        {
            "type": "message",
            "v": 1,
            "text": "timeless",
            "sent_at": math.inf,
            "id": "bad-ts-1",
        }
    ).encode("utf-8")
    response = call_handler(node._handle_message, raw)
    assert response["ok"] is True
    assert len(received) == 1
    # Delivered and recorded, with the unusable send time normalized
    # rather than the message dropped from the inbox.
    assert len(node.inbox) == 1
    assert node.inbox.list_messages()[0]["sent_at"] == 0.0


def test_direct_message_survives_a_restart(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    call_handler(node._handle_message, encode_message("see you later", message_id="keep-1"))

    reloaded = make_node(tmp_path)
    assert len(reloaded.inbox) == 1
    assert reloaded.inbox.list_messages()[0]["text"] == "see you later"


def test_store_then_fetch_roundtrip(tmp_path):
    sender = RNS.Identity()
    recipient = RNS.Identity()
    recipient_hash = destination_hash(recipient)
    holder = make_node(tmp_path)

    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="relay-1",
        text="held for you",
        sent_at=55.0,
    )
    stored = call_handler(
        holder._handle_mail_store,
        build_mail_store(
            to=recipient_hash.hex(),
            message_id="relay-1",
            blob=blob,
            expires_at=time.time() + 3600,
        ),
        remote_identity=sender,
    )
    assert stored["ok"] is True and stored["stored"] is True

    again = call_handler(
        holder._handle_mail_store,
        build_mail_store(
            to=recipient_hash.hex(),
            message_id="relay-1",
            blob=blob,
            expires_at=time.time() + 3600,
        ),
        remote_identity=sender,
    )
    assert again["stored"] is False and again["duplicate"] is True

    # Someone else asking gets nothing: mail is bound to the caller's hash.
    stranger = RNS.Identity()
    empty = call_handler(
        holder._handle_mail_fetch, build_mail_fetch(), remote_identity=stranger
    )
    assert empty["messages"] == []

    fetched = call_handler(
        holder._handle_mail_fetch, build_mail_fetch(), remote_identity=recipient
    )
    assert fetched["count"] == 1
    item = fetched["messages"][0]
    assert item["id"] == "relay-1"
    import base64

    message = open_message(recipient, base64.b64decode(item["blob"]))
    assert message["text"] == "held for you"
    assert message["from_name"] == "alice"

    # The ack drops the holder's copy.
    call_handler(
        holder._handle_mail_fetch,
        build_mail_fetch(["relay-1"]),
        remote_identity=recipient,
    )
    assert holder.mailbox.held_for(recipient_hash) == []


def test_fetch_requires_an_identified_caller(tmp_path):
    holder = make_node(tmp_path)
    response = call_handler(holder._handle_mail_fetch, build_mail_fetch())
    assert response["ok"] is False
    assert response["error"]["code"] == "unidentified"


def test_store_rejects_expired_envelope(tmp_path):
    holder = make_node(tmp_path)
    sender = RNS.Identity()
    recipient = RNS.Identity()
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="stale-1",
        text="too late",
        sent_at=1.0,
    )
    response = call_handler(
        holder._handle_mail_store,
        build_mail_store(
            to=destination_hash(recipient).hex(),
            message_id="stale-1",
            blob=blob,
            expires_at=time.time() - 5,
        ),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "mail_expired"


@pytest.mark.parametrize(
    "expires_at", [True, False, float("nan"), float("inf"), float("-inf"), 10**400]
)
def test_store_rejects_invalid_expiry(tmp_path, expires_at):
    holder = make_node(tmp_path)
    response = call_handler(
        holder._handle_mail_store,
        {
            "type": "mail_store",
            "to": "ab" * 16,
            "id": "invalid",
            "blob": "eA==",
            "expires_at": expires_at,
        },
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_mail"
    assert holder.mailbox.counts()["held"] == 0


def test_collected_message_is_delivered_once(tmp_path):
    received = []
    node = make_node(tmp_path, name="bob", on_message=received.append)
    sender = RNS.Identity()
    message = {
        "id": "collect-1",
        "from": destination_hash(sender).hex(),
        "from_name": "alice",
        "text": "via a holder",
        "sent_at": 12.0,
    }
    assert node._deliver_collected(message) is True
    assert node._deliver_collected(message) is False
    assert len(received) == 1
    assert received[0]["relayed"] is True
    assert received[0]["text"] == "via a holder"
    # Relayed delivery is recorded too, marked as relayed.
    assert len(node.inbox) == 1
    assert node.inbox.list_messages()[0]["relayed"] is True


def test_inbox_failure_does_not_break_delivery(tmp_path, monkeypatch):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    monkeypatch.setattr(node.inbox, "append", lambda record: (_ for _ in ()).throw(OSError("disk gone")))

    response = call_handler(node._handle_message, encode_message("still arrives", message_id="fail-1"))

    assert response["ok"] is True
    assert len(received) == 1


def test_flush_outbox_is_a_noop_until_started(tmp_path):
    node = make_node(tmp_path)
    node.mailbox.enqueue(to=b"\x01" * 16, text="waiting")
    assert node.flush_outbox() == {
        "delivered": 0,
        "deposited": 0,
        "received": 0,
        "lock_busy": False,
    }


def test_flush_outbox_prioritises_targets_and_caps_peers(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()  # started enough for flush_outbox
    peers = []
    for i in range(4):
        identity = RNS.Identity()
        peer = Peer(
            destination_hash=destination_hash(identity),
            name=f"peer{i}",
            public_key=identity.get_public_key(),
            first_seen=float(i),
            last_seen=float(i),
        )
        node.registry._peers[peer.destination_hash] = peer
        peers.append(peer)
    # peers[0] is the least recently seen, but it is owed mail.
    node.mailbox.enqueue(
        to=peers[0].destination_hash, text="for peer0", now=time.time()
    )

    contacted = []
    node._flush_peer = lambda peer, timeout, **kwargs: contacted.append(
        peer.name
    ) or {
        "delivered": 0,
        "deposited": 0,
    }
    node._collect_mail = lambda peer, timeout: 0
    node._process_local_held = lambda: 0

    node.flush_outbox(max_peers=2)
    assert contacted == ["peer0", "peer3"]


def test_flush_outbox_respects_delivery_backoff(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()  # started enough for flush_outbox
    identity = RNS.Identity()
    peer = Peer(
        destination_hash=destination_hash(identity),
        name="peer0",
        public_key=identity.get_public_key(),
        first_seen=0.0,
        last_seen=0.0,
    )
    node.registry._peers[peer.destination_hash] = peer
    now = time.time()
    due = node.mailbox.enqueue(to=peer.destination_hash, text="due", now=now)
    waiting = node.mailbox.enqueue(
        to=peer.destination_hash, text="waiting", now=now
    )
    node.mailbox.record_attempt(waiting.id, error="no path", now=now)

    attempted = []

    def deliver(p, entry, timeout):
        # Mirror the real _deliver_entry: success removes the entry.
        attempted.append(entry.id)
        node.mailbox.remove_outbox(entry.id)
        return True

    node._deliver_entry = deliver
    node._collect_mail = lambda peer, timeout: 0
    node._process_local_held = lambda: 0

    # The periodic sweep only attempts entries whose backoff has elapsed.
    summary = node.flush_outbox()
    assert attempted == [due.id]
    assert summary["delivered"] == 1

    # An announce-triggered round is fresh evidence the peer is reachable,
    # so it bypasses the backoff and retries the waiting entry too.
    attempted.clear()
    node._mail_round_for_peer(peer)
    assert attempted == [waiting.id]


def test_capped_sweep_prioritises_due_recipients(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()  # started enough for flush_outbox
    now = time.time()
    peers = []
    for i in range(17):
        identity = RNS.Identity()
        peer = Peer(
            destination_hash=destination_hash(identity),
            name=f"peer{i}",
            public_key=identity.get_public_key(),
            first_seen=float(i),
            last_seen=float(i),
        )
        node.registry._peers[peer.destination_hash] = peer
        peers.append(peer)
    # peers[0] is least recently seen but its mail is due; the other
    # sixteen are more recent, but their entries are backed off.
    due_entry = node.mailbox.enqueue(
        to=peers[0].destination_hash, text="due", now=now
    )
    for peer in peers[1:]:
        entry = node.mailbox.enqueue(
            to=peer.destination_hash, text="waiting", now=now
        )
        node.mailbox.record_attempt(entry.id, error="no path", now=now)

    attempted = []

    def deliver(p, entry, timeout):
        attempted.append(entry.id)
        node.mailbox.remove_outbox(entry.id)
        return True

    node._deliver_entry = deliver
    node._collect_mail = lambda peer, timeout: 0
    node._process_local_held = lambda: 0

    summary = node.flush_outbox(max_peers=16)
    assert attempted == [due_entry.id]
    assert summary["delivered"] == 1


def test_busy_mail_round_is_queued_not_dropped(tmp_path):
    node = make_node(tmp_path)
    identity = RNS.Identity()
    peer = Peer(
        destination_hash=destination_hash(identity),
        name="busy",
        public_key=identity.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
    )
    node._mail_lock.acquire()
    try:
        node._mail_round_for_peer(peer)
    finally:
        node._mail_lock.release()
    assert peer.hexhash in node._mail_pending


def test_local_held_delivery_skips_dropped_envelope(tmp_path, monkeypatch):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    identity = RNS.Identity()
    recipient_hash = destination_hash(identity)
    node.identity = identity
    node.destination = SimpleNamespace(
        hash=recipient_hash, hexhash=recipient_hash.hex()
    )
    sender = RNS.Identity()
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=identity.get_public_key(),
        message_id="self-1",
        text="for me",
        sent_at=55.0,
    )
    node.mailbox.hold(
        to=recipient_hash,
        blob=blob,
        message_id="self-1",
        expires_at=time.time() + 3600,
    )
    assert node._process_local_held() == 1
    assert len(received) == 1
    assert received[0]["text"] == "for me"

    # A second envelope, dropped by the owner (tau outbox drop-held) after
    # the sweep listed it, must not be surfaced. Simulate the drop by
    # making drop_held report the envelope already gone.
    blob2 = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=identity.get_public_key(),
        message_id="self-2",
        text="dropped before delivery",
        sent_at=56.0,
    )
    node.mailbox.hold(
        to=recipient_hash,
        blob=blob2,
        message_id="self-2",
        expires_at=time.time() + 3600,
    )
    monkeypatch.setattr(node.mailbox, "drop_held", lambda *a, **k: False)
    assert node._process_local_held() == 0
    assert len(received) == 1


def test_fetch_ignores_envelope_dropped_by_another_process(tmp_path):
    from tau.mailbox import Mailbox

    sender = RNS.Identity()
    recipient = RNS.Identity()
    recipient_hash = destination_hash(recipient)
    holder = make_node(tmp_path)

    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="relay-2",
        text="held for you",
        sent_at=55.0,
    )
    stored = call_handler(
        holder._handle_mail_store,
        build_mail_store(
            to=recipient_hash.hex(),
            message_id="relay-2",
            blob=blob,
            expires_at=time.time() + 3600,
        ),
        remote_identity=sender,
    )
    assert stored["ok"] is True and stored["stored"] is True

    # A second process (e.g. `tau outbox drop-held`) removes the envelope
    # from disk; the daemon's cached mailbox must not keep handing it out.
    other_process = Mailbox(holder.config.mailbox_path)
    assert other_process.drop_held("relay-2") is True

    fetched = call_handler(
        holder._handle_mail_fetch, build_mail_fetch(), remote_identity=recipient
    )
    assert fetched["messages"] == []
    assert fetched["count"] == 0
def _recorded_request():
    calls = []

    def fake_request(*args, **kwargs):
        calls.append(args)
        return {"ok": True}

    return calls, fake_request


def test_deliver_entry_skips_cancelled_message(tmp_path):
    node = make_node(tmp_path)
    peer = node.registry.upsert(bytes.fromhex("ab" * 16), b"\x02" * 64, name="alice")
    calls, fake_request = _recorded_request()
    node._request = fake_request

    entry = node.mailbox.enqueue(to=bytes.fromhex("ab" * 16), text="hi")
    # Cancelled after the sweep snapshotted the outbox: no send.
    assert node.mailbox.remove_outbox(entry.id) is True
    assert node._deliver_entry(peer, entry, timeout=5) is False
    assert calls == []

    # Still queued: sends once and removes the entry.
    entry = node.mailbox.enqueue(to=bytes.fromhex("ab" * 16), text="hi again")
    assert node._deliver_entry(peer, entry, timeout=5) is True
    assert len(calls) == 1
    assert node.mailbox.get_outbox(entry.id) is None


def test_deposit_entry_skips_cancelled_message(tmp_path):
    node = make_node(tmp_path)
    node.identity = RNS.Identity()
    recipient = RNS.Identity()
    recipient_hash = destination_hash(recipient)
    holder = node.registry.upsert(bytes.fromhex("cd" * 16), b"\x03" * 64, name="holder")
    node.registry.upsert(recipient_hash, recipient.get_public_key(), name="bob")
    calls, fake_request = _recorded_request()
    node._request = fake_request

    entry = node.mailbox.enqueue(to=recipient_hash, text="held for bob")
    # Cancelled after the sweep snapshotted the outbox: no deposit.
    assert node.mailbox.remove_outbox(entry.id) is True
    assert node._deposit_entry(holder, entry, timeout=5) is False
    assert calls == []

    # Still queued: deposits the sealed copy once.
    entry = node.mailbox.enqueue(to=recipient_hash, text="held for bob again")
    assert node._deposit_entry(holder, entry, timeout=5) is True
    assert len(calls) == 1
    assert node.mailbox.get_outbox(entry.id) is not None
def test_deliver_entry_reports_delivery_via_callback(tmp_path):
    delivered = []
    node = make_node(tmp_path, on_delivered=delivered.append)
    identity = RNS.Identity()
    peer = Peer(
        destination_hash=destination_hash(identity),
        name="alice",
        public_key=identity.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
    )
    entry = node.mailbox.enqueue(to=peer.destination_hash, text="hello")
    node._request = lambda *args, **kwargs: {"ok": True}

    assert node._deliver_entry(peer, entry, timeout=5.0) is True
    assert node.mailbox.get_outbox(entry.id) is None
    assert delivered == [entry.id]


def test_deliver_entry_reports_nothing_when_delivery_fails(tmp_path):
    delivered = []
    node = make_node(tmp_path, on_delivered=delivered.append)
    identity = RNS.Identity()
    peer = Peer(
        destination_hash=destination_hash(identity),
        name="alice",
        public_key=identity.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
    )
    entry = node.mailbox.enqueue(to=peer.destination_hash, text="hello")

    def _fail(*args, **kwargs):
        raise NodeError("no path to peer")

    node._request = _fail

    assert node._deliver_entry(peer, entry, timeout=5.0) is False
    assert node.mailbox.get_outbox(entry.id) is not None
    assert delivered == []
