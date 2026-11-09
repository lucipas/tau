"""Read receipts for direct messages (issue #83).

Covers the signed receipt payload, the sealed receipt envelope, the
sender-side sent history, the ``/tau/read`` handler and the receipt
queue — all without a live network.  The two-node round trip lives in
``test_integration.py``.
"""

from __future__ import annotations

import base64
import json
import time
from types import SimpleNamespace

import RNS
import pytest

from tau.config import TauConfig, send_read_receipts_enabled, set_node_setting
from tau.identity import destination_hash
from tau.inbox import Inbox
from tau.mailbox import (
    Mailbox,
    MailboxError,
    open_message,
    open_receipt,
    open_sealed_envelope,
    seal_message,
    seal_receipt,
)
from tau.node import NodeError, TauNode
from tau.peers import Peer, PeerRegistry
from tau.protocol import (
    MAX_READ_IDS,
    ProtocolError,
    build_read_receipt,
    decode,
    encode,
    parse_read_receipt,
)


def make_node(tmp_path, name="node", on_message=None, on_read=None):
    config = TauConfig.resolve(data_dir=tmp_path, name=name)
    return TauNode(
        config,
        registry=PeerRegistry(config.peers_path),
        on_message=on_message,
        on_read=on_read,
    )


def call_handler(handler, data, remote_identity=None):
    return decode(
        handler("/tau/test", data, b"request-id", remote_identity, time.time())
    )


def make_peer_identity():
    identity = RNS.Identity()
    return identity, destination_hash(identity)


# -- signed receipt payload ---------------------------------------------------


def test_read_receipt_roundtrip():
    reader, reader_hash = make_peer_identity()
    payload = build_read_receipt(
        reader, message_ids=["msg-1", "msg-2"], read_at=1234.5
    )
    receipt = parse_read_receipt(payload, remote_identity=reader)
    assert receipt["ids"] == ["msg-1", "msg-2"]
    assert receipt["read_at"] == 1234.5
    assert receipt["from"] == reader_hash.hex()


def test_read_receipt_rejects_tampered_signature():
    reader, _ = make_peer_identity()
    payload = build_read_receipt(reader, message_ids=["msg-1"])
    obj = decode(payload)
    obj["ids"] = ["msg-1", "msg-2"]  # signed ids no longer match
    with pytest.raises(ProtocolError, match="signature does not verify"):
        parse_read_receipt(encode(obj))


def test_read_receipt_rejects_wrong_reader():
    reader, _ = make_peer_identity()
    other, other_hash = make_peer_identity()
    payload = build_read_receipt(reader, message_ids=["msg-1"])
    obj = decode(payload)
    obj["from"] = other_hash.hex()  # key no longer derives the claimed reader
    with pytest.raises(ProtocolError, match="does not match the claimed reader"):
        parse_read_receipt(encode(obj))
    # And a receipt carried over someone else's link is rejected too.
    with pytest.raises(ProtocolError, match="does not match the link"):
        parse_read_receipt(payload, remote_identity=other)


def test_read_receipt_rejects_bad_ids():
    reader, _ = make_peer_identity()
    with pytest.raises(ProtocolError):
        build_read_receipt(reader, message_ids=[])
    with pytest.raises(ProtocolError):
        build_read_receipt(reader, message_ids=["x"] * (MAX_READ_IDS + 1))
    with pytest.raises(ProtocolError):
        build_read_receipt(reader, message_ids=[""], read_at=1.0)
    with pytest.raises(ProtocolError):
        build_read_receipt(reader, message_ids=["ok"], read_at=float("inf"))


def test_read_receipt_dedupes_ids():
    reader, _ = make_peer_identity()
    payload = build_read_receipt(reader, message_ids=["a", "b", "a"])
    assert parse_read_receipt(payload)["ids"] == ["a", "b"]


# -- sealed receipts ------------------------------------------------------------


def test_sealed_receipt_roundtrip():
    reader, reader_hash = make_peer_identity()
    sender, _ = make_peer_identity()
    blob = seal_receipt(
        reader,
        sender_name="reader",
        recipient_public_key=sender.get_public_key(),
        receipt_id="rcpt-1",
        message_ids=["msg-1", "msg-2"],
        read_at=99.0,
    )
    receipt = open_receipt(sender, blob)
    assert receipt["id"] == "rcpt-1"
    assert receipt["from"] == reader_hash.hex()
    assert receipt["from_name"] == "reader"
    assert receipt["ids"] == ["msg-1", "msg-2"]
    assert receipt["read_at"] == 99.0

    # The dispatcher tells messages and receipts apart.
    kind, payload = open_sealed_envelope(sender, blob)
    assert kind == "sealed_receipt"
    assert payload["ids"] == ["msg-1", "msg-2"]

    message_blob = seal_message(
        reader,
        sender_name="reader",
        recipient_public_key=sender.get_public_key(),
        message_id="msg-9",
        text="hello",
        sent_at=1.0,
    )
    kind, message = open_sealed_envelope(sender, message_blob)
    assert kind == "sealed_message"
    assert message["text"] == "hello"
    with pytest.raises(MailboxError):
        open_receipt(sender, message_blob)
    with pytest.raises(MailboxError):
        open_message(sender, blob)


def test_sealed_receipt_rejects_stranger_and_tampering():
    reader, _ = make_peer_identity()
    sender, _ = make_peer_identity()
    stranger, _ = make_peer_identity()
    blob = seal_receipt(
        reader,
        sender_name="reader",
        recipient_public_key=sender.get_public_key(),
        receipt_id="rcpt-2",
        message_ids=["msg-1"],
        read_at=5.0,
    )
    with pytest.raises(MailboxError):
        open_receipt(stranger, blob)
    with pytest.raises(MailboxError):
        open_receipt(sender, bytes(blob)[:-4] + b"xxxx")


# -- sender-side sent history ---------------------------------------------------


def test_record_sent_and_read(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    alice_hash = b"\xaa" * 16
    box.record_sent("msg-1", to=alice_hash, text="hello", sent_at=100.0)
    assert box.read_at("msg-1") is None
    assert box.read_at("unknown") is None

    # A receipt from the recorded recipient is believed.
    assert box.record_read("msg-1", 150.0, reader_hash=alice_hash.hex()) is True
    assert box.read_at("msg-1") == 150.0
    # A later re-read does not move the first read time.
    assert box.record_read("msg-1", 200.0, reader_hash=alice_hash.hex()) is False
    assert box.read_at("msg-1") == 150.0

    # Re-recording a send keeps the read.
    box.record_sent("msg-1", to=alice_hash, text="hello", sent_at=100.0)
    assert box.read_at("msg-1") == 150.0

    sent = box.sent_list()
    assert [entry["id"] for entry in sent] == ["msg-1"]
    assert sent[0]["text"] == "hello"
    assert sent[0]["to"] == alice_hash.hex()

    reloaded = Mailbox(tmp_path / "mailbox.json")
    assert reloaded.read_at("msg-1") == 150.0


def test_record_read_rejects_forgery(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    alice_hash = b"\xaa" * 16
    stranger_hash = b"\xbb" * 16
    box.record_sent("msg-1", to=alice_hash, text="hello", sent_at=100.0)
    # From a peer who is not the recipient: rejected.
    assert (
        box.record_read("msg-1", 150.0, reader_hash=stranger_hash.hex()) is False
    )
    assert box.read_at("msg-1") is None
    # For a message this node never sent: rejected.
    assert (
        box.record_read("never-sent", 150.0, reader_hash=alice_hash.hex())
        is False
    )
    # A non-finite read time is rejected.
    assert (
        box.record_read("msg-1", float("nan"), reader_hash=alice_hash.hex())
        is False
    )


def test_receipt_queue_roundtrip_and_prune(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.queue_receipt(
        to=b"\xaa" * 16, message_ids=["m1", "m2"], read_at=50.0, now=100.0
    )
    assert box.counts()["receipts"] == 1
    assert box.due_receipts(to=b"\xaa" * 16)[0].id == entry.id
    assert box.due_receipts(to=b"\xbb" * 16) == []

    box.record_receipt_attempt(entry.id, now=100.0)
    assert box.get_receipt_outbox(entry.id).attempts == 1
    # A failed attempt backs off: not due again for a minute.
    assert box.due_receipts(now=110.0) == []
    assert len(box.due_receipts(now=200.0)) == 1

    box.record_receipt_deposit(entry.id, "cc" * 16, now=200.0)
    assert box.get_receipt_outbox(entry.id).held_by == {"cc" * 16: 200.0}

    reloaded = Mailbox(tmp_path / "mailbox.json")
    assert reloaded.get_receipt_outbox(entry.id).message_ids == ["m1", "m2"]

    assert box.remove_receipt_outbox(entry.id) is True
    assert box.remove_receipt_outbox(entry.id) is False
    assert box.counts()["receipts"] == 0

    # Expired receipts and stale sent records prune away.
    box.queue_receipt(to=b"\xaa" * 16, message_ids=["m9"], read_at=1.0, now=100.0)
    box.record_sent("old", to=b"\xaa" * 16, text="old", sent_at=100.0)
    removed = box.prune(now=100.0 + 31 * 24 * 60 * 60)
    assert removed["receipts"] == 1
    assert removed["sent"] == 1


# -- inbox receipt tracking -------------------------------------------------------


def test_inbox_receipt_tracking(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(
        {
            "from": "alice",
            "destination": "aa" * 16,
            "text": "hi",
            "sent_at": 1.0,
            "received_at": 2.0,
            "id": "msg-1",
        }
    )
    inbox.append(
        {
            "from": "alice",
            "destination": "aa" * 16,
            "text": "again",
            "sent_at": 3.0,
            "received_at": 4.0,
            "id": "msg-2",
        }
    )
    assert len(inbox.unreceipted()) == 2
    assert inbox.mark_receipt_sent(["msg-1"], at=10.0) == ["msg-1"]
    # Stamping twice does not re-stamp.
    assert inbox.mark_receipt_sent(["msg-1", "msg-2"], at=11.0) == ["msg-2"]
    assert [entry["id"] for entry in inbox.unreceipted()] == []
    assert inbox.list_messages()[1]["receipt_sent_at"] == 10.0

    reloaded = Inbox(tmp_path / "inbox.json")
    assert len(reloaded.unreceipted()) == 0


def test_inbox_mark_read_hands_off_to_the_serving_node(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(
        {
            "from": "alice",
            "destination": "aa" * 16,
            "text": "hi",
            "sent_at": 1.0,
            "received_at": 2.0,
            "id": "msg-1",
        }
    )
    inbox.append(
        {
            "from": "alice",
            "destination": "aa" * 16,
            "text": "again",
            "sent_at": 3.0,
            "received_at": 4.0,
            "id": "msg-2",
        }
    )
    # Nothing displayed yet: no receipt is owed.
    assert inbox.read_unreceipted() == []
    assert inbox.mark_read(["msg-1"], at=10.0) == ["msg-1"]
    assert inbox.mark_read(["msg-1"], at=11.0) == []  # first read time kept
    assert [entry["id"] for entry in inbox.read_unreceipted()] == ["msg-1"]
    assert inbox.list_messages()[1]["read_at"] == 10.0
    # Once receipted, the message leaves the owed set.
    inbox.mark_receipt_sent(["msg-1"], at=12.0)
    assert inbox.read_unreceipted() == []

    reloaded = Inbox(tmp_path / "inbox.json")
    assert [entry["id"] for entry in reloaded.read_unreceipted()] == []


# -- /tau/read handler --------------------------------------------------------------


def test_handle_read_records_verified_receipt(tmp_path):
    received_reads = []
    node = make_node(tmp_path, name="bob", on_read=received_reads.append)
    node.destination = SimpleNamespace(hexhash="ab" * 16, hash=b"\xab" * 16)
    reader, reader_hash = make_peer_identity()
    node.mailbox.record_sent(
        "msg-1", to=reader_hash, text="hello", sent_at=100.0
    )
    node.mailbox.record_sent(
        "msg-2", to=reader_hash, text="hello again", sent_at=101.0
    )

    payload = build_read_receipt(reader, message_ids=["msg-1", "msg-2"], read_at=150.0)
    response = call_handler(node._handle_read, payload, remote_identity=reader)
    assert response["ok"] is True
    assert response["recorded"] == ["msg-1", "msg-2"]
    assert node.mailbox.read_at("msg-1") == 150.0
    assert node.mailbox.read_at("msg-2") == 150.0
    assert received_reads == [
        {"reader": reader_hash.hex(), "ids": ["msg-1", "msg-2"], "read_at": 150.0}
    ]


def test_handle_read_rejects_forged_and_bad_receipts(tmp_path):
    node = make_node(tmp_path, name="bob")
    node.destination = SimpleNamespace(hexhash="ab" * 16, hash=b"\xab" * 16)
    reader, reader_hash = make_peer_identity()
    stranger, _ = make_peer_identity()
    node.mailbox.record_sent(
        "msg-1", to=reader_hash, text="hello", sent_at=100.0
    )

    # A receipt signed by a stranger for our message to the reader:
    # the signature verifies, but the reader is not the recipient.
    payload = build_read_receipt(stranger, message_ids=["msg-1"], read_at=150.0)
    response = call_handler(node._handle_read, payload, remote_identity=stranger)
    assert response["ok"] is True
    assert response["recorded"] == []
    assert node.mailbox.read_at("msg-1") is None

    # A receipt for a message we never sent records nothing.
    payload = build_read_receipt(reader, message_ids=["nope"], read_at=150.0)
    response = call_handler(node._handle_read, payload, remote_identity=reader)
    assert response["recorded"] == []

    # A tampered payload is an error, not a silent drop.
    raw = decode(build_read_receipt(reader, message_ids=["msg-1"]))
    raw["ids"] = ["msg-1", "msg-2"]
    response = call_handler(node._handle_read, encode(raw), remote_identity=reader)
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_receipt"


def test_send_read_receipts_queues_when_sender_unreachable(tmp_path):
    node = make_node(tmp_path, name="alice")
    node.reticulum = object()  # started enough for send_read_receipts
    node.identity = RNS.Identity()
    reader, reader_hash = make_peer_identity()
    peer = Peer(
        destination_hash=reader_hash,
        name="bob",
        public_key=reader.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
        caps=("message", "ping", "peers", "mail", "read"),
    )
    node.registry._peers[peer.destination_hash] = peer

    def _fail(*args, **kwargs):
        raise NodeError("no path to peer")

    node._request = _fail
    result = node.send_read_receipts(peer, ["msg-1", "msg-2"], read_at=42.0)
    assert result["ok"] is True
    assert result["queued"] is True
    queued = node.mailbox.receipt_outbox_list(to=reader_hash)
    assert len(queued) == 1
    assert queued[0].message_ids == ["msg-1", "msg-2"]
    assert queued[0].read_at == 42.0


def test_send_read_receipts_respects_opt_out(tmp_path):
    node = make_node(tmp_path, name="alice")
    node.reticulum = object()
    node.identity = RNS.Identity()
    set_node_setting(node.config.data_dir, "send_read_receipts", False)
    assert node.read_receipts_enabled is False
    result = node.send_read_receipts(b"\xaa" * 16, ["msg-1"])
    # Disabled: no ids are reported handled, so no caller stamps them
    # as receipted.
    assert result == {"ok": False, "disabled": True, "ids": []}
    assert node.mailbox.counts()["receipts"] == 0
    assert send_read_receipts_enabled(node.config.data_dir) is False


def test_flush_receipts_delivers_direct_and_deposits(tmp_path):
    node = make_node(tmp_path, name="alice")
    node.reticulum = object()
    node.identity = RNS.Identity()
    reader, reader_hash = make_peer_identity()
    holder, holder_hash = make_peer_identity()
    for identity, dest_hash in ((reader, reader_hash), (holder, holder_hash)):
        peer = Peer(
            destination_hash=dest_hash,
            name="peer",
            public_key=identity.get_public_key(),
            first_seen=1.0,
            last_seen=1.0,
            caps=("message", "ping", "peers", "mail", "read"),
        )
        node.registry._peers[dest_hash] = peer
    reader_peer = node.registry.get(reader_hash)
    holder_peer = node.registry.get(holder_hash)

    # Direct delivery: the sender answers /tau/read, the queue empties.
    entry = node.mailbox.queue_receipt(
        to=reader_hash, message_ids=["msg-1"], read_at=10.0
    )
    calls = []
    node._request = lambda peer, path, payload, timeout: calls.append(
        (peer.hexhash, path)
    ) or {"ok": True, "recorded": ["msg-1"]}
    assert node._flush_receipts(reader_peer, timeout=5.0) == 1
    assert node.mailbox.get_receipt_outbox(entry.id) is None
    assert calls == [(reader_hash.hex(), "/tau/read")]

    # The sender is unreachable: the receipt is sealed and deposited
    # with the holder instead.
    entry = node.mailbox.queue_receipt(
        to=reader_hash, message_ids=["msg-2"], read_at=20.0
    )
    node._request = lambda peer, path, payload, timeout: (
        {"ok": True, "stored": True} if path == "/tau/mail/store" else (_ for _ in ()).throw(NodeError("down"))
    )
    assert node._flush_receipts(reader_peer, timeout=5.0) == 0
    assert node._flush_receipts(holder_peer, timeout=5.0) == 1
    kept = node.mailbox.get_receipt_outbox(entry.id)
    assert kept is not None
    assert kept.held_by == {holder_hash.hex(): pytest.approx(time.time(), abs=60)}


def test_collected_sealed_receipt_records_reads(tmp_path):
    received = []
    node = make_node(tmp_path, name="bob", on_message=received.append)
    node.destination = SimpleNamespace(hexhash="ab" * 16, hash=b"\xab" * 16)
    node.identity = RNS.Identity()
    reader, reader_hash = make_peer_identity()
    node.mailbox.record_sent(
        "msg-1", to=reader_hash, text="hello", sent_at=100.0
    )
    blob = seal_receipt(
        reader,
        sender_name="alice",
        recipient_public_key=node.identity.get_public_key(),
        receipt_id="rcpt-9",
        message_ids=["msg-1"],
        read_at=150.0,
    )
    node._request = lambda peer, path, payload, timeout: {
        "ok": True,
        "messages": [
            {
                "id": "rcpt-9",
                "blob": base64.b64encode(blob).decode("ascii"),
                "expires_at": time.time() + 3600,
            }
        ],
        "count": 1,
    }
    peer = Peer(
        destination_hash=reader_hash,
        name="alice",
        public_key=reader.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
        caps=("message", "ping", "peers", "mail", "read"),
    )
    node.registry._peers[reader_hash] = peer
    assert node._collect_mail(peer, timeout=5.0) == 0  # a receipt is not a message
    assert node.mailbox.read_at("msg-1") == 150.0
    assert received == []


def test_flush_read_receipts_sends_stamped(tmp_path):
    node = make_node(tmp_path, name="alice")
    node.reticulum = object()
    node.identity = RNS.Identity()
    reader, reader_hash = make_peer_identity()
    node.inbox.append(
        {
            "from": "bob",
            "destination": reader_hash.hex(),
            "text": "hello",
            "sent_at": 1.0,
            "received_at": 2.0,
            "id": "msg-1",
        }
    )
    # Displayed but never receipted: the sweep owes a receipt.
    assert node._flush_read_receipts() == 0
    node.inbox.mark_read(["msg-1"], at=10.0)
    sent = []
    node.send_read_receipts = lambda target, ids, **kwargs: sent.append(
        (target, list(ids), kwargs.get("read_at"))
    ) or {"ok": True, "ids": ids}
    assert node._flush_read_receipts() == 1
    assert sent == [(reader_hash.hex(), ["msg-1"], 10.0)]
    assert node.inbox.read_unreceipted() == []
    # Second sweep sends nothing.
    assert node._flush_read_receipts() == 0
    assert sent == [(reader_hash.hex(), ["msg-1"], 10.0)]


def test_send_message_records_sent_before_request_returns(tmp_path):
    # PR #99 (Codex P2): a live console receipts on display, so the
    # reader's receipt can arrive while the /tau/message request is
    # still in flight.  The send must already be recorded then, or
    # the receipt is discarded as an unknown id and never retried.
    node = make_node(tmp_path, name="bob")
    node.reticulum = object()
    node.identity = RNS.Identity()
    node.destination = SimpleNamespace(hexhash="ab" * 16, hash=b"\xab" * 16)
    reader, reader_hash = make_peer_identity()
    peer = Peer(
        destination_hash=reader_hash,
        name="alice",
        public_key=reader.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
        caps=("message", "ping", "peers", "mail", "read"),
    )
    node.registry._peers[reader_hash] = peer

    def _request(peer, path, payload, timeout):
        message = decode(payload)
        receipt = build_read_receipt(
            reader, message_ids=[message["id"]], read_at=150.0
        )
        response = call_handler(node._handle_read, receipt, remote_identity=reader)
        assert response["recorded"] == [message["id"]]
        return {"ok": True}

    node._request = _request
    response = node.send_message(peer, "hello")
    assert response["ok"] is True
    assert node.mailbox.read_at(response["id"]) == 150.0


def test_send_read_receipts_chunks_over_100_ids(tmp_path):
    # PR #99 (Codex P2): a per-peer list over 100 ids used to fail
    # build_read_receipt and then the queue fallback, leaving the
    # messages unreceipted.  It is now split into ≤100-id receipts.
    node = make_node(tmp_path, name="alice")
    node.reticulum = object()
    node.identity = RNS.Identity()
    reader, reader_hash = make_peer_identity()
    peer = Peer(
        destination_hash=reader_hash,
        name="bob",
        public_key=reader.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
        caps=("message", "ping", "peers", "mail", "read"),
    )
    node.registry._peers[reader_hash] = peer
    ids = [f"msg-{i}" for i in range(250)]
    batch_sizes = []

    def _request(peer, path, payload, timeout):
        batch = decode(payload)["ids"]
        batch_sizes.append(len(batch))
        return {"ok": True, "recorded": batch}

    node._request = _request
    result = node.send_read_receipts(peer, ids, read_at=42.0)
    assert result["ok"] is True
    assert result.get("queued") is None
    assert result["recorded"] == ids
    assert batch_sizes == [100, 100, 50]
    assert node.mailbox.counts()["receipts"] == 0


def test_send_read_receipts_queues_oversized_batches(tmp_path):
    # Same chunking on the queue fallback: an unreachable sender gets
    # three queued receipts, each within the 100-id limit.
    node = make_node(tmp_path, name="alice")
    node.reticulum = object()
    node.identity = RNS.Identity()
    reader, reader_hash = make_peer_identity()
    peer = Peer(
        destination_hash=reader_hash,
        name="bob",
        public_key=reader.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
        caps=("message", "ping", "peers", "mail", "read"),
    )
    node.registry._peers[reader_hash] = peer

    def _fail(*args, **kwargs):
        raise NodeError("no path to peer")

    node._request = _fail
    ids = [f"msg-{i}" for i in range(250)]
    result = node.send_read_receipts(peer, ids, read_at=42.0)
    assert result["ok"] is True
    assert result["ids"] == ids
    queued = node.mailbox.receipt_outbox_list(to=reader_hash)
    assert len(queued) == 3
    assert [len(entry.message_ids) for entry in queued] == [100, 100, 50]
    assert [mid for entry in queued for mid in entry.message_ids] == ids


def test_send_read_receipts_queues_by_hash_when_discovery_fails(
    tmp_path, monkeypatch
):
    # PR #99 (CodeRabbit Major): a sender that cannot be discovered
    # gets its receipt queued by destination hash instead of dropped.
    node = make_node(tmp_path, name="alice")
    node.reticulum = object()
    node.identity = RNS.Identity()
    node.destination = SimpleNamespace(hexhash="ab" * 16, hash=b"\xab" * 16)
    sender_hash = b"\xaa" * 16

    def _no_peer(dest_hash, *, timeout=10.0):
        raise NodeError("peer did not announce within 10.0s")

    monkeypatch.setattr(node, "wait_for_peer", _no_peer)
    result = node.send_read_receipts(sender_hash.hex(), ["msg-1", "msg-2"])
    assert result["ok"] is True
    assert result["queued"] is True
    queued = node.mailbox.receipt_outbox_list(to=sender_hash)
    assert len(queued) == 1
    assert queued[0].message_ids == ["msg-1", "msg-2"]

    # A target with no recoverable hash still raises.
    with pytest.raises(NodeError):
        node.send_read_receipts("nobody-here", ["msg-1"])


def test_flush_read_receipts_queues_unknown_sender_once(tmp_path, monkeypatch):
    # PR #99 (CodeRabbit Major): once the receipt is queued the ids
    # are stamped, so later sweeps do not repeat the discovery wait.
    node = make_node(tmp_path, name="alice")
    node.reticulum = object()
    node.identity = RNS.Identity()
    node.destination = SimpleNamespace(hexhash="ab" * 16, hash=b"\xab" * 16)
    sender_hash = b"\xaa" * 16
    node.inbox.append(
        {
            "from": "bob",
            "destination": sender_hash.hex(),
            "text": "hello",
            "sent_at": 1.0,
            "received_at": 2.0,
            "id": "msg-1",
        }
    )
    node.inbox.mark_read(["msg-1"], at=10.0)
    attempts = []

    def _no_peer(dest_hash, *, timeout=10.0):
        attempts.append(dest_hash)
        raise NodeError("peer did not announce within 10.0s")

    monkeypatch.setattr(node, "wait_for_peer", _no_peer)
    assert node._flush_read_receipts() == 1
    assert attempts == [sender_hash]
    assert node.inbox.read_unreceipted() == []
    assert len(node.mailbox.receipt_outbox_list(to=sender_hash)) == 1
    # Second sweep owes nothing: no repeat discovery wait.
    assert node._flush_read_receipts() == 0
    assert attempts == [sender_hash]


def test_flush_outbox_releases_mail_lock_before_receipts(tmp_path, monkeypatch):
    # PR #99 (CodeRabbit Major): the receipt flush runs after the
    # mail lock is released, so a discovery wait cannot stall other
    # mail rounds.
    node = make_node(tmp_path, name="alice")
    node.reticulum = object()
    node.identity = RNS.Identity()
    node.inbox.append(
        {
            "from": "bob",
            "destination": "aa" * 16,
            "text": "hello",
            "sent_at": 1.0,
            "received_at": 2.0,
            "id": "msg-1",
        }
    )
    node.inbox.mark_read(["msg-1"], at=10.0)
    lock_states = []
    real_flush = node._flush_read_receipts

    def _recording_flush():
        lock_states.append(node._mail_lock.locked())
        return real_flush()

    monkeypatch.setattr(node, "_flush_read_receipts", _recording_flush)
    summary = node.flush_outbox()
    assert lock_states == [False]
    assert summary["receipts"] == 1
    assert node.inbox.read_unreceipted() == []


def test_config_roundtrip(tmp_path):
    data_dir = tmp_path / "node"
    assert send_read_receipts_enabled(data_dir) is True  # missing file: on
    set_node_setting(data_dir, "send_read_receipts", False)
    assert send_read_receipts_enabled(data_dir) is False
    assert json.loads((data_dir / "node.json").read_text())[
        "send_read_receipts"
    ] is False


# -- CLI --------------------------------------------------------------------------


def test_cli_config_read_receipts(tmp_path, capsys):
    from tau import cli

    data_dir = tmp_path / "node"
    assert cli.main(["config", "--data-dir", str(data_dir)]) == 0
    assert "send read receipts: on" in capsys.readouterr().out

    assert (
        cli.main(["config", "read-receipts", "off", "--data-dir", str(data_dir)])
        == 0
    )
    assert "Read receipts: off" in capsys.readouterr().out
    assert send_read_receipts_enabled(data_dir) is False

    code = cli.main(["config", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    assert json.loads(capsys.readouterr().out) == {"send_read_receipts": False}

    assert (
        cli.main(["config", "read-receipts", "on", "--data-dir", str(data_dir)])
        == 0
    )
    assert send_read_receipts_enabled(data_dir) is True


def test_cli_messages_sent_shows_read_state(tmp_path, capsys):
    from tau import cli

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    box = Mailbox(config.mailbox_path)
    box.record_sent("msg-1", to=b"\xaa" * 16, text="hello alice", sent_at=100.0)
    box.record_sent("msg-2", to=b"\xaa" * 16, text="hello again", sent_at=200.0)
    box.record_read("msg-1", 150.0, reader_hash=(b"\xaa" * 16).hex())
    # msg-2 was delivered (but not read): delivery confirmation is what
    # separates "delivered" from "unknown" in the sent-messages display.
    box.record_delivered("msg-2", now=250.0)
    box.enqueue(to=b"\xaa" * 16, text="still waiting", message_id="msg-3")
    # A queued message is recorded as sent when it is queued (as
    # TauNode.send_message does); the direct enqueue above does not.
    box.record_sent("msg-3", to=b"\xaa" * 16, text="still waiting", sent_at=300.0)

    code = cli.main(["messages", "--sent", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 3
    by_id = {entry["id"]: entry for entry in payload["messages"]}
    assert by_id["msg-1"]["state"] == "read"
    assert by_id["msg-1"]["read_at"] == 150.0
    assert by_id["msg-2"]["state"] == "delivered"
    assert by_id["msg-2"]["read_at"] is None
    assert by_id["msg-3"]["state"] == "queued"
    assert by_id["msg-3"]["text"] == "still waiting"


def test_cli_messages_lists_without_receipts_when_opted_out(
    tmp_path, capsys, monkeypatch
):
    from tau import cli

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": "bb" * 16,
            "text": "hello",
            "sent_at": 1.0,
            "received_at": 2.0,
            "id": "msg-1",
        }
    )
    set_node_setting(data_dir, "send_read_receipts", False)

    started = []
    monkeypatch.setattr(cli.TauNode, "start", lambda self: started.append(True))
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    assert "hello" in capsys.readouterr().out
    assert started == []  # opted out: no node is started to send receipts
    assert len(inbox.unreceipted()) == 1
    assert inbox.list_messages()[0].get("read_at") is None


def test_cli_messages_stamps_read_when_no_node_can_start(tmp_path, capsys):
    from tau import cli

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "bob",
            "destination": "bb" * 16,
            "text": "hello",
            "sent_at": 1.0,
            "received_at": 2.0,
            "id": "msg-1",
        }
    )
    # No identity file: listing must not create one, but the display is
    # still stamped so the serving node's sweep can receipt it later.
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    assert "hello" in capsys.readouterr().out
    assert not config.identity_path.exists()
    reloaded = Inbox(config.inbox_path)
    assert reloaded.list_messages()[0]["read_at"] is not None
    assert len(reloaded.read_unreceipted()) == 1


def test_cli_outbox_reports_queued_receipts(tmp_path, capsys):
    from tau import cli

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="alice")
    box = Mailbox(config.mailbox_path)
    box.queue_receipt(to=b"\xaa" * 16, message_ids=["m1"], read_at=10.0)
    code = cli.main(["outbox", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["receipts_queued"] == 1


# -- web console --------------------------------------------------------------------


def test_bridge_record_read_flips_outbound_message(tmp_path):
    from tau.webapi import WebBridge

    node = make_node(tmp_path, name="bob")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    # Record an outbound message the way the console does, then a receipt.
    record = bridge._append_message(
        direction="out",
        peer="aa" * 16,
        peer_name="alice",
        text="hi",
        sent_at=100.0,
        received_at=100.0,
        status="delivered",
        mail_id="msg-1",
    )
    assert record["status"] == "delivered"
    updated = bridge.record_read("aa" * 16, ["msg-1"], 150.0)
    assert len(updated) == 1
    assert updated[0]["status"] == "read"
    assert updated[0]["read_at"] == 150.0
    assert bridge.messages()[0]["status"] == "read"
    # A receipt from someone else, or for another message, flips nothing.
    assert bridge.record_read("bb" * 16, ["msg-1"], 160.0) == []
    assert bridge.record_read("aa" * 16, ["other"], 160.0) == []
    # A second receipt keeps the earliest read time.
    assert bridge.record_read("aa" * 16, ["msg-1"], 160.0) == []
    assert bridge.messages()[0]["read_at"] == 150.0
    # The read state survives a reload of messages.json.
    reloaded = WebBridge(make_node(tmp_path, name="bob"), host="127.0.0.1", port=0)
    assert reloaded.messages()[0]["status"] == "read"
    assert reloaded.messages()[0]["read_at"] == 150.0


def test_bridge_mark_displayed_read_sends_once(tmp_path):
    from tau.webapi import WebBridge

    node = make_node(tmp_path, name="alice")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    node.inbox.append(
        {
            "from": "bob",
            "destination": "bb" * 16,
            "text": "hello",
            "sent_at": 1.0,
            "received_at": 2.0,
            "id": "msg-1",
        }
    )
    bridge._append_message(
        direction="in",
        peer="bb" * 16,
        peer_name="bob",
        text="hello",
        sent_at=1.0,
        received_at=2.0,
        status="received",
        mail_id="msg-1",
    )
    sent = []
    node.send_read_receipts = lambda target, ids, **kwargs: sent.append(
        (target, list(ids))
    ) or {"ok": True, "ids": ids}
    bridge.mark_displayed_read()
    deadline = time.time() + 5.0
    while time.time() < deadline and not sent:
        time.sleep(0.05)
    assert sent == [("bb" * 16, ["msg-1"])]
    assert node.inbox.unreceipted() == []

    # A second display does not re-send.
    sent.clear()
    bridge.mark_displayed_read()
    time.sleep(0.3)
    assert sent == []


def test_bridge_record_inbound_waits_for_a_viewer(tmp_path):
    from tau.webapi import WebBridge

    node = make_node(tmp_path, name="alice")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    node.inbox.append(
        {
            "from": "bob",
            "destination": "bb" * 16,
            "text": "hello",
            "sent_at": 1.0,
            "received_at": 2.0,
            "id": "msg-1",
        }
    )
    sent = []
    node.send_read_receipts = lambda target, ids, **kwargs: sent.append(
        (target, list(ids))
    ) or {"ok": True, "ids": ids}

    # Nobody is watching the console: arrival alone sends no receipt.
    bridge.record_inbound(
        {
            "from": "bob",
            "destination": "bb" * 16,
            "text": "hello",
            "sent_at": 1.0,
            "received_at": 2.0,
            "id": "msg-1",
        }
    )
    time.sleep(0.3)
    assert sent == []

    # A live subscriber means the thread view is displaying it.
    subscriber = bridge.bus.subscribe()
    try:
        bridge.record_inbound(
            {
                "from": "bob",
                "destination": "bb" * 16,
                "text": "hello again",
                "sent_at": 3.0,
                "received_at": 4.0,
                "id": "msg-1",
            }
        )
        deadline = time.time() + 5.0
        while time.time() < deadline and not sent:
            time.sleep(0.05)
        assert sent == [("bb" * 16, ["msg-1"])]
    finally:
        bridge.bus.unsubscribe(subscriber)
