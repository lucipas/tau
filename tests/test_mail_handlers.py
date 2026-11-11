"""Node-level store-and-forward behaviour that needs no live network."""

from __future__ import annotations

import time
from types import SimpleNamespace

import RNS
import pytest

import tau.mailbox as mailbox_module
from tau.config import TauConfig
from tau.identity import destination_hash
from tau.mailbox import open_message, seal_message
from tau.node import NodeError, TauNode
from tau.peers import Peer, PeerRegistry
from tau.protocol import (
    ProtocolError,
    build_mail_fetch,
    build_mail_store,
    decode,
    encode,
    encode_message,
    encode_reaction,
    encode_retract,
    excerpt_hash,
    parse_message,
    reaction_dict,
    retract_dict,
)


def make_node(tmp_path, name="holder", on_message=None, on_delivered=None, on_reaction=None):
    config = TauConfig.resolve(data_dir=tmp_path, name=name)
    node = TauNode(
        config,
        registry=PeerRegistry(config.peers_path),
        on_message=on_message,
        on_delivered=on_delivered,
        on_reaction=on_reaction,
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


def test_direct_message_with_reserved_kind_is_rejected(tmp_path):
    # "group" and "file" name inbox record types: a DM tagged with
    # either would fail inbox validation and be silently lost (the
    # sender already got success, and the seen-id blocks a retry).
    # The wire parser rejects them instead, before anything is marked
    # seen — so a retry with a real kind still lands.
    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )

    from tau.protocol import encode as encode_payload

    for kind in ("group", "file"):
        payload = encode_payload(
            {
                "type": "message",
                "v": 1,
                "text": "reserved kind",
                "sent_at": 1.0,
                "id": f"kind-{kind}",
                "kind": kind,
            }
        )
        response = call_handler(node._handle_message, payload)
        assert response["ok"] is False
        assert response["error"]["code"] == "invalid_message"
    assert received == []
    assert node.inbox.list_messages() == []

    # Nothing was marked seen: the same id with a valid kind delivers.
    payload = encode_message("reserved kind", message_id="kind-group", kind="ping")
    response = call_handler(node._handle_message, payload)
    assert response["ok"] is True
    assert node.inbox.list_messages()[0]["kind"] == "ping"


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


_UNSET = object()


def _store(holder, sender, recipient, message_id, *, remote_identity=_UNSET):
    """Deposit one sealed envelope, from *sender* unless told otherwise."""
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id=message_id,
        text=f"held {message_id}",
        sent_at=55.0,
    )
    return call_handler(
        holder._handle_mail_store,
        build_mail_store(
            to=destination_hash(recipient).hex(),
            message_id=message_id,
            blob=blob,
            expires_at=time.time() + 3600,
        ),
        remote_identity=sender if remote_identity is _UNSET else remote_identity,
    )


def test_store_enforces_per_sender_quota(tmp_path, monkeypatch):
    monkeypatch.setattr(mailbox_module, "HELD_QUOTA_UNKNOWN_COUNT", 2)
    holder = make_node(tmp_path)
    sender = RNS.Identity()
    recipient = RNS.Identity()
    for message_id in ("q1", "q2", "q3"):
        response = _store(holder, sender, recipient, message_id)
        assert response["ok"] is True and response["stored"] is True
    # The third deposit shed the sender's oldest envelope.
    assert [e.id for e in holder.mailbox.held_list()] == ["q2", "q3"]


def test_store_trusted_peer_gets_higher_quota(tmp_path, monkeypatch):
    monkeypatch.setattr(mailbox_module, "HELD_QUOTA_UNKNOWN_COUNT", 1)
    monkeypatch.setattr(mailbox_module, "HELD_QUOTA_KNOWN_COUNT", 2)
    holder = make_node(tmp_path)
    sender = RNS.Identity()
    recipient = RNS.Identity()
    # Long-standing peer: in the registry for two days, heard directly,
    # with a delivered history.
    holder.registry.upsert(
        destination_hash(sender),
        sender.get_public_key(),
        name="alice",
        now=time.time() - 2 * 24 * 3600,
    )
    assert holder.registry.record_delivered(destination_hash(sender)) is True
    for message_id in ("t1", "t2"):
        response = _store(holder, sender, recipient, message_id)
        assert response["ok"] is True and response["stored"] is True
    assert [e.id for e in holder.mailbox.held_list()] == ["t1", "t2"]
    # A third deposit sheds the oldest instead of being refused.
    response = _store(holder, sender, recipient, "t3")
    assert response["ok"] is True and response["stored"] is True
    assert [e.id for e in holder.mailbox.held_list()] == ["t2", "t3"]


def test_store_long_standing_peer_without_delivery_uses_unknown_quota(
    tmp_path, monkeypatch
):
    # Age and a direct hearing alone are not enough: the higher tier
    # requires a delivered history (Qodo warning on PR #92).
    monkeypatch.setattr(mailbox_module, "HELD_QUOTA_UNKNOWN_COUNT", 1)
    holder = make_node(tmp_path)
    sender = RNS.Identity()
    recipient = RNS.Identity()
    holder.registry.upsert(
        destination_hash(sender),
        sender.get_public_key(),
        name="alice",
        now=time.time() - 2 * 24 * 3600,
    )
    assert _store(holder, sender, recipient, "n1")["stored"] is True
    assert _store(holder, sender, recipient, "n2")["stored"] is True
    assert [e.id for e in holder.mailbox.held_list()] == ["n2"]


def test_store_recent_peer_uses_unknown_quota(tmp_path, monkeypatch):
    monkeypatch.setattr(mailbox_module, "HELD_QUOTA_UNKNOWN_COUNT", 1)
    holder = make_node(tmp_path)
    sender = RNS.Identity()
    recipient = RNS.Identity()
    holder.registry.upsert(
        destination_hash(sender), sender.get_public_key(), name="alice"
    )
    assert _store(holder, sender, recipient, "r1")["stored"] is True
    assert _store(holder, sender, recipient, "r2")["stored"] is True
    assert [e.id for e in holder.mailbox.held_list()] == ["r2"]


def test_store_anonymous_senders_share_unknown_bucket(tmp_path, monkeypatch):
    monkeypatch.setattr(mailbox_module, "HELD_QUOTA_UNKNOWN_COUNT", 1)
    holder = make_node(tmp_path)
    sender = RNS.Identity()
    recipient = RNS.Identity()
    for message_id in ("a1", "a2"):
        response = _store(
            holder, sender, recipient, message_id, remote_identity=None
        )
        assert response["ok"] is True and response["stored"] is True
    assert [e.id for e in holder.mailbox.held_list()] == ["a2"]
    assert holder.mailbox.held_usage()[0]["sender"] == ""


def test_store_refuses_single_blob_over_sender_bytes_quota(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(mailbox_module, "HELD_QUOTA_UNKNOWN_BYTES", 4)
    holder = make_node(tmp_path)
    sender = RNS.Identity()
    recipient = RNS.Identity()
    response = _store(holder, sender, recipient, "big-1")
    assert response["ok"] is False
    assert response["error"]["code"] == "mailbox_full"
    assert holder.mailbox.counts()["held"] == 0


def test_deliver_entry_records_delivered_history(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = node.registry.upsert(
        b"\xaa" * 16, RNS.Identity().get_public_key(), name="alice"
    )
    entry = node.mailbox.enqueue(to=peer.destination_hash, text="hi", now=time.time())
    monkeypatch.setattr(node, "_request", lambda *args, **kwargs: {"ok": True})
    assert node._deliver_entry(peer, entry, timeout=5.0) is True
    stored = node.registry.get(peer.destination_hash)
    assert stored.delivered_count == 1
    assert stored.last_delivered_at is not None
    assert node.mailbox.get_outbox(entry.id) is None


def test_send_message_records_delivered_history(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    peer = node.registry.upsert(
        b"\xaa" * 16, RNS.Identity().get_public_key(), name="alice"
    )
    monkeypatch.setattr(
        node, "_request", lambda *args, **kwargs: {"ok": True, "echo": "hi"}
    )
    response = node.send_message(peer, "hello")
    assert response["ok"] is True
    stored = node.registry.get(peer.destination_hash)
    assert stored.delivered_count == 1
    assert stored.last_delivered_at is not None


def test_send_message_registers_unregistered_peer_for_delivery_history(
    tmp_path, monkeypatch
):
    # A Peer handed to send_message directly never entered the registry
    # (resolve_peer returns Peer objects as-is); a successful delivery
    # must still leave a record, or the peer never earns its quota tier
    # (Qodo warning on PR #92).
    node = make_node(tmp_path)
    identity = RNS.Identity()
    peer = Peer(
        destination_hash=destination_hash(identity),
        name="stranger",
        public_key=identity.get_public_key(),
        first_seen=100.0,
        last_seen=100.0,
    )
    assert node.registry.get(peer.destination_hash) is None
    monkeypatch.setattr(
        node, "_request", lambda *args, **kwargs: {"ok": True, "echo": "hi"}
    )
    response = node.send_message(peer, "hello")
    assert response["ok"] is True
    stored = node.registry.get(peer.destination_hash)
    assert stored is not None
    assert stored.delivered_count == 1
    assert stored.last_delivered_at is not None


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
        "receipts": 0,
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
    node._collect_mail = lambda peer, timeout, **_kwargs: 0
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

    def deliver(p, entry, timeout, **kwargs):
        # Mirror the real _deliver_entry: success removes the entry.
        attempted.append(entry.id)
        node.mailbox.remove_outbox(entry.id)
        return True

    node._deliver_entry = deliver
    node._collect_mail = lambda peer, timeout, **_kwargs: 0
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

    def deliver(p, entry, timeout, **kwargs):
        attempted.append(entry.id)
        node.mailbox.remove_outbox(entry.id)
        return True

    node._deliver_entry = deliver
    node._collect_mail = lambda peer, timeout, **_kwargs: 0
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


def test_direct_message_keeps_reply_reference(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    reply_to = {"id": "a" * 32, "excerpt": excerpt_hash("original thought")}
    payload = encode_message(
        "seconded", message_id="reply-1", in_reply_to=reply_to
    )
    response = call_handler(node._handle_message, payload)

    assert response["ok"] is True
    assert received[0]["in_reply_to"] == reply_to
    assert node.inbox.list_messages()[0]["in_reply_to"] == reply_to


def test_direct_message_without_reply_has_no_reference(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    call_handler(node._handle_message, encode_message("plain", message_id="plain-1"))

    assert "in_reply_to" not in received[0]
    assert "in_reply_to" not in node.inbox.list_messages()[0]


def test_collected_reply_keeps_reply_reference(tmp_path):
    received = []
    node = make_node(tmp_path, name="bob", on_message=received.append)
    sender = RNS.Identity()
    reply_to = {"id": "a" * 32, "excerpt": excerpt_hash("original thought")}
    message = {
        "id": "collect-reply-1",
        "from": destination_hash(sender).hex(),
        "from_name": "alice",
        "text": "seconded, via a holder",
        "sent_at": 12.0,
        "in_reply_to": reply_to,
    }
    assert node._deliver_collected(message) is True
    assert received[0]["in_reply_to"] == reply_to
    assert node.inbox.list_messages()[0]["in_reply_to"] == reply_to


def signed_reaction(sender, target, token, message_id, *, sent_at=None):
    """A reaction payload signed by *sender*, as send_reaction builds it."""
    import base64

    payload = reaction_dict(target, token, message_id=message_id, sent_at=sent_at)
    signature = sender.sign(encode(payload))
    payload["sig"] = base64.b64encode(signature).decode("ascii")
    return encode(payload)


def test_reaction_clear_removes_the_reaction(tmp_path):
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    call_handler(
        node._handle_reaction,
        signed_reaction(sender, "msg-1", "👍", "react-1"),
        remote_identity=sender,
    )
    assert len(node.inbox.reactions_for("msg-1")) == 1
    response = call_handler(
        node._handle_reaction,
        signed_reaction(sender, "msg-1", "", "react-2"),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert node.inbox.reactions_for("msg-1") == []


def test_reaction_dedupes_by_id(tmp_path):
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    payload = signed_reaction(sender, "msg-1", "👍", "react-dup")
    first = call_handler(node._handle_reaction, payload, remote_identity=sender)
    second = call_handler(node._handle_reaction, payload, remote_identity=sender)
    assert first["duplicate"] is False
    assert second["duplicate"] is True
    assert len(node.inbox.reactions_for("msg-1")) == 1


def test_reaction_denied_by_allow_list(tmp_path):
    from tau.allowlist import AllowList, save_allowlist

    config = TauConfig.resolve(data_dir=tmp_path, name="holder")
    save_allowlist(
        config.allowlist_path, AllowList(enabled=True, allow=frozenset())
    )
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    response = call_handler(
        node._handle_reaction,
        signed_reaction(sender, "msg-1", "👍", "react-1"),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_allowed"
    assert node.inbox.reactions_for("msg-1") == []


def test_reaction_is_recorded_and_fanned_out(tmp_path):
    received = []
    node = make_node(tmp_path, on_reaction=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()

    payload = signed_reaction(sender, "msg-1", "👍", "react-1")
    response = call_handler(node._handle_reaction, payload, remote_identity=sender)

    assert response["ok"] is True
    assert response["reaction"] == "👍"
    assert response["target"] == "msg-1"
    assert response["duplicate"] is False
    assert len(received) == 1
    assert received[0]["target"] == "msg-1"
    assert received[0]["reaction"] == "👍"
    assert received[0]["destination"] == destination_hash(sender).hex()
    reactions = node.inbox.reactions_for("msg-1")
    assert len(reactions) == 1
    assert reactions[0]["reaction"] == "👍"


def test_reaction_rejects_a_signature_from_another_identity(tmp_path):
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    signer = RNS.Identity()
    presenter = RNS.Identity()
    response = call_handler(
        node._handle_reaction,
        signed_reaction(signer, "msg-1", "👍", "react-1"),
        remote_identity=presenter,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_reaction"
    assert node.inbox.reactions_for("msg-1") == []


def test_reaction_rejects_a_tampered_payload(tmp_path):
    import base64

    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    payload = reaction_dict("msg-1", "👍", message_id="react-1")
    signature = sender.sign(encode(payload))
    payload["sig"] = base64.b64encode(signature).decode("ascii")
    payload["reaction"] = "❤️"  # tampered after signing
    response = call_handler(
        node._handle_reaction, encode(payload), remote_identity=sender
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_reaction"
    assert node.inbox.reactions_for("msg-1") == []


def test_reaction_rejects_invalid_payload(tmp_path):
    node = make_node(tmp_path)
    response = call_handler(node._handle_reaction, b"not json")
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_reaction"


def test_reaction_requires_a_signature(tmp_path):
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    response = call_handler(
        node._handle_reaction,
        encode_reaction("msg-1", "👍", message_id="react-1"),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_reaction"
    assert "signature" in response["error"]["message"]
    assert node.inbox.reactions_for("msg-1") == []


def test_reaction_requires_an_identified_sender(tmp_path):
    node = make_node(tmp_path)
    sender = RNS.Identity()
    response = call_handler(
        node._handle_reaction,
        signed_reaction(sender, "msg-1", "👍", "react-1"),
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "unidentified"
    assert node.inbox.reactions_for("msg-1") == []


def test_reaction_storage_failure_is_reported_and_retryable(tmp_path, monkeypatch):
    """A reaction the receiver cannot store is an error, not an ack —
    and because its id was never marked seen, the same reaction can be
    retried with the same id."""
    node = make_node(tmp_path)
    sender = RNS.Identity()
    payload = signed_reaction(sender, "msg-1", "👍", "react-1")

    monkeypatch.setattr(node.inbox, "_save", lambda: False)
    response = call_handler(node._handle_reaction, payload, remote_identity=sender)

    assert response["ok"] is False
    assert response["error"]["code"] == "storage_failed"
    assert "could not store" in response["error"]["message"]
    assert node.inbox.reactions_for("msg-1") == []
    assert not node.mailbox.is_seen(
        f"reaction:{destination_hash(sender).hex()}:react-1"
    )

    # The write heals; the same id retries cleanly.
    monkeypatch.setattr(node.inbox, "_save", lambda: True)
    retry = call_handler(node._handle_reaction, payload, remote_identity=sender)
    assert retry["ok"] is True
    assert retry["duplicate"] is False
    assert len(node.inbox.reactions_for("msg-1")) == 1


def test_record_reaction_strict_raises_on_storage_failure(tmp_path, monkeypatch):
    """The sender's own best-effort record swallows a failed write,
    but the strict (receive-handler) record raises so the handler can
    report it."""
    node = make_node(tmp_path)
    monkeypatch.setattr(node.inbox, "_save", lambda: False)
    record = {
        "target": "msg-1",
        "reaction": "👍",
        "from": "alice",
        "destination": "ab" * 16,
        "sent_at": 1.0,
        "received_at": 2.0,
        "id": "react-1",
    }
    node._record_reaction(record)  # non-strict: logged, not raised
    with pytest.raises(OSError):
        node._record_reaction(record, strict=True)
    # The failed merge was rolled back: nothing phantom is served.
    assert node.inbox.reactions_for("msg-1") == []


def test_send_reaction_validates_before_resolving(tmp_path):
    node = make_node(tmp_path)
    with pytest.raises(ProtocolError):
        node.send_reaction("alice", "msg-1", "x" * 33)


def signed_retract(sender, target, message_id, *, sent_at=None):
    """A retract payload signed by *sender*, as send_retraction builds it."""
    import base64

    payload = retract_dict(target, message_id=message_id, sent_at=sent_at)
    signature = sender.sign(encode(payload))
    payload["sig"] = base64.b64encode(signature).decode("ascii")
    return encode(payload)


def _store_received_message(node, sender, message_id):
    """Store one DM from *sender* in the node's inbox, as _handle_message does."""
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    payload = encode_message("take this back", message_id=message_id)
    response = call_handler(
        node._handle_message, payload, remote_identity=sender
    )
    assert response["ok"] is True
    assert len(node.inbox.list_messages()) == 1


def test_retract_deletes_the_senders_own_message(tmp_path):
    node = make_node(tmp_path)
    sender = RNS.Identity()
    _store_received_message(node, sender, "msg-1")

    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "msg-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["target"] == "msg-1"
    assert response["deleted"] is True
    assert node.inbox.list_messages() == []


def test_retract_is_idempotent_for_an_unknown_id(tmp_path):
    node = make_node(tmp_path)
    sender = RNS.Identity()
    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "msg-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["deleted"] is False


def test_retract_requires_an_exact_target_id(tmp_path):
    # resolve also matches unique prefixes: a retract for "msg-1"
    # must not delete "msg-123" from the same sender.
    node = make_node(tmp_path)
    sender = RNS.Identity()
    _store_received_message(node, sender, "msg-123")

    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "msg-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["deleted"] is False
    assert len(node.inbox.list_messages()) == 1


def test_retract_rejects_a_non_sender(tmp_path):
    node = make_node(tmp_path)
    sender = RNS.Identity()
    other = RNS.Identity()
    _store_received_message(node, sender, "msg-1")

    response = call_handler(
        node._handle_retract,
        signed_retract(other, "msg-1", "retract-1"),
        remote_identity=other,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_sender"
    assert len(node.inbox.list_messages()) == 1


def test_retract_requires_an_identified_sender(tmp_path):
    node = make_node(tmp_path)
    sender = RNS.Identity()
    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "msg-1", "retract-1"),
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "unidentified"


def test_retract_rejects_an_unsigned_payload(tmp_path):
    node = make_node(tmp_path)
    sender = RNS.Identity()
    _store_received_message(node, sender, "msg-1")

    payload = encode_retract("msg-1", message_id="retract-1")
    response = call_handler(
        node._handle_retract, payload, remote_identity=sender
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_retraction"
    assert len(node.inbox.list_messages()) == 1


def test_retract_rejects_a_bad_signature(tmp_path):
    node = make_node(tmp_path)
    sender = RNS.Identity()
    other = RNS.Identity()
    _store_received_message(node, sender, "msg-1")

    # Signed by someone else, presented as the sender: the link
    # identity cannot verify it.
    payload = signed_retract(other, "msg-1", "retract-1")
    response = call_handler(
        node._handle_retract, payload, remote_identity=sender
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_retraction"
    assert len(node.inbox.list_messages()) == 1


def test_retract_denied_by_allow_list(tmp_path):
    from tau.allowlist import AllowList

    node = make_node(tmp_path)
    sender = RNS.Identity()
    _store_received_message(node, sender, "msg-1")
    # Enable an empty allow-list after the message arrived: the sender
    # is no longer allowed, so the retract is refused.  The list loads
    # lazily, so swap the cached value directly.
    node._allowlist = AllowList(enabled=True, allow=frozenset())
    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "msg-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_allowed"
    assert len(node.inbox.list_messages()) == 1


def test_retract_refuses_group_messages(tmp_path):
    node = make_node(tmp_path)
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    node.inbox.append(
        {
            "from": "alice",
            "destination": sender_hash,
            "text": "group hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "msg-1",
            "kind": "group",
            "group": "c" * 32,
            "group_name": "team",
        }
    )
    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "msg-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_supported"
    assert len(node.inbox.list_messages()) == 1


def test_retract_rejects_an_outgoing_record(tmp_path):
    # Regression test for the CodeRabbit Major: a recipient knows the
    # wire id of a message they received, and the sender's own sent
    # copy carries the recipient's hash in ``destination`` — so without
    # the direction check the recipient could delete the sender's
    # sent-message history.
    node = make_node(tmp_path)
    recipient = RNS.Identity()
    recipient_hash = destination_hash(recipient).hex()
    node.inbox.append(
        {
            "from": "holder",
            "destination": recipient_hash,
            "text": "sent hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "msg-1",
            "direction": "out",
        }
    )
    response = call_handler(
        node._handle_retract,
        signed_retract(recipient, "msg-1", "retract-1"),
        remote_identity=recipient,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_sender"
    assert len(node.inbox.list_messages(direction="out")) == 1


def test_retract_refuses_file_records(tmp_path):
    # A received file lives on disk; deleting only the inbox record
    # would orphan it, so file records are not retractable.
    node = make_node(tmp_path)
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    node.inbox.append(
        {
            "from": "alice",
            "destination": sender_hash,
            "name": "photo.jpg",
            "size": 123,
            "sha256": "ab" * 32,
            "path": "/tmp/tau-files/photo.jpg",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "msg-1",
            "kind": "file",
        }
    )
    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "msg-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_supported"
    assert len(node.inbox.list_messages()) == 1


def test_retract_finds_a_message_imported_by_another_process(tmp_path):
    # A one-shot CLI import can persist a message after this node's
    # in-memory inbox was read; the handler reloads first, so the
    # retract still finds and deletes it.
    from tau.inbox import Inbox

    node = make_node(tmp_path)
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    config = TauConfig.resolve(data_dir=tmp_path, name="holder")
    other = Inbox(config.inbox_path)
    other.append(
        {
            "from": "alice",
            "destination": sender_hash,
            "text": "imported hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "msg-1",
        }
    )
    assert node.inbox.list_messages() == []
    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "msg-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["deleted"] is True
    assert node.inbox.list_messages() == []


def test_send_retraction_validates_before_resolving(tmp_path):
    node = make_node(tmp_path)
    with pytest.raises(NodeError):
        node.send_retraction("alice", "")
    with pytest.raises(NodeError):
        node.send_retraction("alice", "x" * 65)
    with pytest.raises(NodeError):
        node.send_reaction("alice", "", "👍")


def test_retract_records_a_marker_for_an_unknown_id(tmp_path):
    # A retract that arrives before the message still wins: the marker
    # is recorded for the authenticated unknown target, so a copy
    # arriving later is dropped, never stored.
    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()

    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "msg-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["deleted"] is False
    assert node.mailbox.is_retracted(sender_hash, "msg-1") is True

    delivery = call_handler(
        node._handle_message,
        encode_message("too late", message_id="msg-1"),
        remote_identity=sender,
    )
    assert delivery["ok"] is True
    assert delivery["retracted"] is True
    assert received == []
    assert node.inbox.list_messages() == []


def test_retracted_direct_message_is_dropped_after_seen_eviction(tmp_path):
    # The marker outlives the bounded seen cache: a copy that arrives
    # after the id was evicted still loses to the retract.
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender = RNS.Identity()
    _store_received_message(node, sender, "msg-1")
    assert len(received) == 1

    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "msg-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["deleted"] is True
    assert node.inbox.list_messages() == []

    # Simulate the seen cache evicting the id.
    del node.mailbox._seen["msg-1"]
    redelivery = call_handler(
        node._handle_message,
        encode_message("take this back", message_id="msg-1"),
        remote_identity=sender,
    )
    assert redelivery["ok"] is True
    assert redelivery["retracted"] is True
    assert len(received) == 1
    assert node.inbox.list_messages() == []


def test_relayed_copy_after_retract_is_dropped(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    message = {
        "id": "relay-retract-1",
        "from": sender_hash,
        "from_name": "alice",
        "text": "doorbell rang",
        "kind": "chat",
        "sent_at": 42.0,
    }
    assert node._deliver_collected(message) is True
    assert len(received) == 1

    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "relay-retract-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["deleted"] is True

    # The seen cache evicts the id; the relayed copy still loses.
    del node.mailbox._seen["relay-retract-1"]
    assert node._deliver_collected(message) is False
    assert len(received) == 1
    assert node.inbox.list_messages() == []


def test_refused_retract_records_no_marker(tmp_path):
    # A retract the handler refuses must not suppress the message:
    # only a retract that is honoured leaves a marker behind.
    node = make_node(tmp_path)
    sender = RNS.Identity()
    other = RNS.Identity()
    _store_received_message(node, sender, "msg-1")

    response = call_handler(
        node._handle_retract,
        signed_retract(other, "msg-1", "retract-1"),
        remote_identity=other,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_sender"
    assert node.mailbox.is_retracted(destination_hash(sender).hex(), "msg-1") is False
    assert node.mailbox.counts()["retracted"] == 0


def test_retract_marks_a_deleted_message_confirmed(tmp_path, monkeypatch):
    # A marker for a message the node actually deleted is confirmed:
    # a flood of unknown-ID markers from another sender cannot evict
    # it at the cap.
    monkeypatch.setattr(mailbox_module, "MAX_RETRACTED", 2)
    node = make_node(tmp_path)
    sender = RNS.Identity()
    _store_received_message(node, sender, "msg-1")

    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "msg-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["deleted"] is True

    other_hash = destination_hash(RNS.Identity()).hex()
    for target in ("junk-1", "junk-2", "junk-3"):
        node.mailbox.record_retraction(other_hash, target)
    assert node.mailbox.counts()["retracted"] == 2
    assert node.mailbox.is_retracted(destination_hash(sender).hex(), "msg-1") is True


def test_collected_message_drops_expired_arrival(tmp_path):
    import time as _time

    received = []
    node = make_node(tmp_path, name="bob", on_message=received.append)
    sender = RNS.Identity()
    message = {
        "id": "collect-past",
        "from": destination_hash(sender).hex(),
        "from_name": "alice",
        "text": "too late",
        "sent_at": 12.0,
        "expires_at": _time.time() - 10,
    }
    assert node._deliver_collected(message) is False
    assert received == []
    assert len(node.inbox) == 0
    # Not marked seen: another holder's copy may still be attempted,
    # and it will be dropped again the same way.
    assert node.mailbox.is_seen("collect-past") is False


def test_collected_message_records_expiry(tmp_path):
    received = []
    node = make_node(tmp_path, name="bob", on_message=received.append)
    sender = RNS.Identity()
    message = {
        "id": "collect-exp",
        "from": destination_hash(sender).hex(),
        "from_name": "alice",
        "text": "via a holder, briefly",
        "sent_at": 12.0,
        "expires_at": 1893456000.0,
    }
    assert node._deliver_collected(message) is True
    assert received[0]["expires_at"] == 1893456000.0
    assert node.inbox.list_messages()[0]["expires_at"] == 1893456000.0


def test_deliver_entry_carries_queued_expiry(tmp_path):
    import time as _time

    node = make_node(tmp_path)
    peer = node.registry.upsert(bytes.fromhex("ab" * 16), b"\x02" * 64, name="alice")
    calls, fake_request = _recorded_request()
    node._request = fake_request

    deadline = _time.time() + 3600
    entry = node.mailbox.enqueue(
        to=bytes.fromhex("ab" * 16),
        text="vanish soon",
        message_id="queued-exp",
        expires_at=deadline,
    )
    assert node._deliver_entry(peer, entry, timeout=5) is True
    assert len(calls) == 1
    payload = parse_message(calls[0][2])
    assert payload["expires_at"] == deadline


def test_deliver_entry_skips_expired_message(tmp_path):
    import time as _time

    node = make_node(tmp_path)
    peer = node.registry.upsert(bytes.fromhex("ab" * 16), b"\x02" * 64, name="alice")
    calls, fake_request = _recorded_request()
    node._request = fake_request

    entry = node.mailbox.enqueue(
        to=bytes.fromhex("ab" * 16),
        text="too late",
        message_id="queued-past",
        expires_at=_time.time() - 10,
    )
    # Past its deadline since the sweep listed it: no send is attempted.
    assert node._deliver_entry(peer, entry, timeout=5) is False
    assert calls == []
    # The entry itself is left for the next prune to remove.
    assert node.mailbox.get_outbox(entry.id) is not None


def test_deposit_entry_skips_expired_message(tmp_path):
    import time as _time

    node = make_node(tmp_path)
    node.identity = RNS.Identity()
    recipient = RNS.Identity()
    recipient_hash = destination_hash(recipient)
    holder = node.registry.upsert(bytes.fromhex("cd" * 16), b"\x03" * 64, name="holder")
    node.registry.upsert(recipient_hash, recipient.get_public_key(), name="bob")
    calls, fake_request = _recorded_request()
    node._request = fake_request

    entry = node.mailbox.enqueue(
        to=recipient_hash,
        text="too late",
        message_id="deposit-past",
        expires_at=_time.time() - 10,
    )
    assert node._deposit_entry(holder, entry, timeout=5) is False
    assert calls == []


def test_direct_message_drops_expired_arrival(tmp_path):
    import time as _time

    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    payload = encode_message(
        "too late", message_id="exp-past", expires_at=_time.time() - 10
    )
    response = call_handler(node._handle_message, payload)
    assert response["ok"] is True
    assert response["expired"] is True
    # Dropped unrecorded (and not marked seen): never shown, and a
    # re-delivery is not treated as a duplicate.
    assert received == []
    assert len(node.inbox) == 0
    assert node.mailbox.is_seen("exp-past") is False


def test_direct_message_records_expiry(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    payload = encode_message(
        "vanish soon", message_id="exp-1", expires_at=1893456000.0
    )
    response = call_handler(node._handle_message, payload)
    assert response["ok"] is True
    assert received[0]["expires_at"] == 1893456000.0
    assert node.inbox.list_messages()[0]["expires_at"] == 1893456000.0


def test_direct_message_without_expiry_has_none(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    response = call_handler(
        node._handle_message, encode_message("stays", message_id="plain-1")
    )
    assert response["ok"] is True
    assert received[0]["expires_at"] is None
    assert node.inbox.list_messages()[0]["expires_at"] is None
