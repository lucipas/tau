"""Node-level store-and-forward behaviour that needs no live network."""

from __future__ import annotations

import time
from types import SimpleNamespace

import RNS
import pytest

import tau.mailbox as mailbox_module
from tau.config import TauConfig
from tau.identity import destination_hash
from tau.mailbox import MailboxError, open_message, seal_message
from tau.node import DeliveryDeferredError, NodeError, TauNode
from tau.peers import Peer, PeerRegistry
from tau.protocol import (
    ProtocolError,
    build_mail_fetch,
    build_mail_store,
    decode,
    derive_group_id,
    encode,
    encode_group_message,
    encode_message,
    encode_reaction,
    encode_retract,
    excerpt_hash,
    parse_group_message,
    parse_mail_store,
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


def test_direct_group_delivery_skips_when_competitor_marks_seen(
    tmp_path, monkeypatch
):
    """A direct group delivery rechecks seen-ness inside the
    transaction (CodeRabbit review, flagged on the stacked PR #295).

    Two direct deliveries can both pass the pre-check, then both
    append and notify.  The relayed path already rechecks inside
    the mailbox transaction; the direct group path did not.  Here a
    competing delivery stores its copy and marks the id seen after
    the pre-check — this delivery must skip both the append and the
    notification, and report the duplicate.
    """
    from tau.inbox import Inbox
    from tau.mailbox import Mailbox

    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = RNS.Identity()
    author_hash = destination_hash(author).hex()
    members = ["ab" * 16, author_hash, destination_hash(RNS.Identity()).hex()]
    group = _pinned_group(node, members=members, creator="ab" * 16)
    real_is_seen = node.mailbox.is_seen
    calls = {"n": 0}

    def flaky_is_seen(message_id):
        calls["n"] += 1
        if calls["n"] == 1:
            # The pre-check: not seen yet.  A competing delivery
            # lands now — it stores its copy and marks the id seen
            # via its own handles, then this handler's transaction
            # reloads and must see the mark.
            other_inbox = Inbox(node.config.inbox_path)
            other_inbox.append(
                {
                    "from": "alice",
                    "destination": author_hash,
                    "text": "crew hello",
                    "sent_at": 100.0,
                    "received_at": 101.0,
                    "id": "race-1",
                    "kind": "group",
                    "group": group,
                    "group_name": "crew",
                }
            )
            Mailbox(node.config.mailbox_path).mark_seen("race-1")
            return False
        return real_is_seen(message_id)

    monkeypatch.setattr(node.mailbox, "is_seen", flaky_is_seen)
    response = call_handler(
        node._handle_group_message,
        encode_group_message(
            "crew hello",
            message_id="race-1",
            group_id=group,
            group_name="crew",
            creator="ab" * 16,
            members=members,
            sent_at=100.0,
        ),
        remote_identity=author,
    )
    assert response["ok"] is True
    assert response["duplicate"] is True
    assert received == []
    assert calls["n"] == 2
    # Exactly one copy is stored: the competitor's, not a second one.
    fresh = Inbox(node.config.inbox_path)
    assert [m["id"] for m in fresh.list_messages()] == ["race-1"]


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


def test_sealed_group_message_carries_rename_lineage(tmp_path):
    sender = RNS.Identity()
    recipient = RNS.Identity()
    creator_hash = destination_hash(sender).hex()
    members = [creator_hash, "ab" * 16, "cd" * 16]
    old_id = derive_group_id(creator_hash, "weekend", members)
    new_id = derive_group_id(creator_hash, "road trip", members)
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="seal-group-1",
        text="after the rename",
        sent_at=55.0,
        group={
            "id": new_id,
            "name": "road trip",
            "creator": creator_hash,
            "members": members,
            "renamed_from": [old_id],
        },
    )
    opened = open_message(recipient, blob)
    assert opened["group"]["renamed_from"] == [old_id]
    # A sealed envelope without lineage opens with no lineage key.
    plain = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="seal-group-2",
        text="plain",
        sent_at=56.0,
        group={
            "id": new_id,
            "name": "road trip",
            "creator": creator_hash,
            "members": members,
        },
    )
    assert "renamed_from" not in open_message(recipient, plain)["group"]
    # A malformed lineage is refused at seal time.
    with pytest.raises(MailboxError):
        seal_message(
            sender,
            sender_name="alice",
            recipient_public_key=recipient.get_public_key(),
            message_id="seal-group-3",
            text="bad",
            sent_at=57.0,
            group={
                "id": new_id,
                "name": "road trip",
                "creator": creator_hash,
                "members": members,
                "renamed_from": ["not-a-hash"],
            },
        )


def test_relayed_group_message_retires_rename_predecessor(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    node_hash = "ab" * 16
    node.destination = SimpleNamespace(
        hexhash=node_hash, hash=bytes.fromhex(node_hash)
    )
    creator = RNS.Identity()
    creator_hash = destination_hash(creator).hex()
    members = [creator_hash, node_hash, "ef" * 16]
    old_id = derive_group_id(creator_hash, "weekend", members)
    old = node.groups.record(
        old_id, "weekend", members, creator=creator_hash, sender="ef" * 16
    )
    assert old is not None
    new_id = derive_group_id(creator_hash, "road trip", members)
    message = {
        "id": "relay-group-1",
        "from": creator_hash,
        "from_name": "alice",
        "text": "after the rename",
        "sent_at": 100.0,
        "group": {
            "id": new_id,
            "name": "road trip",
            "creator": creator_hash,
            "members": members,
            "renamed_from": [old_id],
        },
    }
    assert node._deliver_collected(message) is True
    assert node.groups.get(old_id) is None
    assert node.groups.get(new_id) is not None
    assert received[0]["kind"] == "group"
    assert received[0]["group"] == new_id


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
    # making is_held report the envelope already gone: the drop now
    # happens after delivery, so the still-held check is the guard.
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
    monkeypatch.setattr(node.mailbox, "is_held", lambda *a, **k: False)
    assert node._process_local_held() == 0
    assert len(received) == 1


def test_local_held_delivery_skips_envelope_dropped_by_another_process(
    tmp_path,
):
    # CodeRabbit round-10 (fp4P): the pre-delivery is_held check
    # reads the serving process's cached held pool, so a drop by
    # another process (tau outbox drop-held) after the sweep listed
    # the envelope was invisible to it — the message was stored and
    # published anyway.  The delivery now rechecks ownership inside
    # the mailbox transaction that guards the store, which reloads
    # from disk.  Drop via a second Mailbox instance, exactly as the
    # CLI would, and assert the serving process's stale cache still
    # lists it — the pre-check passes, and only the in-transaction
    # recheck stops the delivery.
    from tau.mailbox import Mailbox

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
        text="dropped by another process",
        sent_at=55.0,
    )
    node.mailbox.hold(
        to=recipient_hash,
        blob=blob,
        message_id="self-1",
        expires_at=time.time() + 3600,
    )
    other_process = Mailbox(node.config.mailbox_path)
    assert other_process.drop_held("self-1") is True
    assert node.mailbox.is_held("self-1", to=recipient_hash) is True
    assert node._process_local_held() == 0
    assert received == []
    assert node.inbox.list_messages() == []


def test_local_held_group_delivery_skips_envelope_dropped_by_another_process(
    tmp_path,
):
    # The group branch of _deliver_collected guards its store with
    # its own mailbox transaction, so it rechecks held ownership
    # there too — a group copy dropped by another process after the
    # sweep listed it is not stored or published either.
    from tau.mailbox import Mailbox

    received = []
    node = make_node(tmp_path, on_message=received.append)
    identity = RNS.Identity()
    recipient_hash = destination_hash(identity)
    node.identity = identity
    node.destination = SimpleNamespace(
        hash=recipient_hash, hexhash=recipient_hash.hex()
    )
    own = recipient_hash.hex()
    sender = RNS.Identity()
    author_hash = destination_hash(sender).hex()
    members = [own, author_hash, destination_hash(RNS.Identity()).hex()]
    group = derive_group_id(author_hash, "crew", members)
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=identity.get_public_key(),
        message_id="self-group-1",
        text="crew hello",
        sent_at=100.0,
        group={
            "id": group,
            "name": "crew",
            "creator": author_hash,
            "members": members,
        },
    )
    node.mailbox.hold(
        to=recipient_hash,
        blob=blob,
        message_id="self-group-1",
        expires_at=time.time() + 3600,
    )
    other_process = Mailbox(node.config.mailbox_path)
    assert other_process.drop_held("self-group-1") is True
    assert node.mailbox.is_held("self-group-1", to=recipient_hash) is True
    assert node._process_local_held() == 0
    assert received == []
    assert node.inbox.list_messages() == []


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


def _signed_group_reaction(
    sender, target, token, message_id, *, group, author, sent_at
):
    """A group reaction payload signed by *sender*, as the fan-out builds it."""
    import base64

    payload = reaction_dict(
        target,
        token,
        message_id=message_id,
        group=group,
        target_author=author,
        target_sent_at=sent_at,
    )
    signature = sender.sign(encode(payload))
    payload["sig"] = base64.b64encode(signature).decode("ascii")
    return encode(payload)


def _seed_group_copy(node, *, group, author, sent_at, message_id):
    node.inbox.append(
        {
            "from": "alice",
            "destination": author,
            "text": "crew hello",
            "kind": "group",
            "group": group,
            "group_name": "crew",
            "sent_at": sent_at,
            "received_at": sent_at + 1.0,
            "id": message_id,
        }
    )


def test_group_reaction_resolves_local_copy_from_context(tmp_path):
    # Group sends carry a different wire id per leg: the reactor's
    # target names a copy this node never held.  The signed group
    # context resolves this node's own copy instead, so the reaction
    # displays under the message here too.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    reactor = RNS.Identity()
    # A copy that arrived through the handler pins its roster, so a
    # retained copy with no pinned group is a deleted group — and a
    # reaction to it fails closed (see the deleted-group tests
    # below).  Pin the group here, with the reactor as a member, so
    # this test models a live group and keeps asserting the
    # context resolution itself.  The id derives from the roster,
    # so the group is pinned first and the copy seeded under it.
    group = _pinned_group(
        node,
        members=["ab" * 16, author, destination_hash(reactor).hex()],
        creator="ab" * 16,
    )
    _seed_group_copy(
        node, group=group, author=author, sent_at=100.0, message_id="local-copy-1"
    )
    response = call_handler(
        node._handle_reaction,
        _signed_group_reaction(
            reactor,
            "other-leg-1",
            "👍",
            "react-1",
            group=group,
            author=author,
            sent_at=100.0,
        ),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    stored = node.inbox.reactions_for("local-copy-1")
    assert len(stored) == 1
    assert stored[0]["reaction"] == "👍"
    assert stored[0]["id"] == "react-1"
    # Nothing is stored under the foreign wire id.
    assert node.inbox.reactions_for("other-leg-1") == []


def test_group_reaction_without_context_stays_on_unknown_target(tmp_path):
    # No group context, no local copy under the target: the reaction
    # is stored as-is (the author can still display it from their
    # sent record), never guessed onto another message.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    _seed_group_copy(
        node,
        group="cd" * 16,
        author=author,
        sent_at=100.0,
        message_id="local-copy-1",
    )
    reactor = RNS.Identity()
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "other-leg-1", "👍", "react-1"),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    assert node.inbox.reactions_for("local-copy-1") == []
    assert len(node.inbox.reactions_for("other-leg-1")) == 1


def test_group_reaction_unresolvable_context_stays_on_target(tmp_path):
    # Group context that matches no local copy — a copy that arrived
    # through an outbox retry, re-stamped — degrades the same way:
    # stored under the wire target, displayed for the author.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    _seed_group_copy(
        node,
        group="cd" * 16,
        author=author,
        sent_at=100.0,
        message_id="local-copy-1",
    )
    reactor = RNS.Identity()
    response = call_handler(
        node._handle_reaction,
        _signed_group_reaction(
            reactor,
            "other-leg-1",
            "👍",
            "react-1",
            group="cd" * 16,
            author=author,
            sent_at=999.0,
        ),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    assert node.inbox.reactions_for("local-copy-1") == []
    assert len(node.inbox.reactions_for("other-leg-1")) == 1


def _pinned_group(node, *, members, creator):
    group = derive_group_id(creator, "crew", members)
    assert (
        node.groups.record(group, "crew", members, creator=creator, sender=creator)
        is not None
    )
    return group


def test_group_reaction_from_member_resolves_on_pinned_group(tmp_path):
    # The pinned roster names the reactor, so the signed context
    # still resolves the node's own copy.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, destination_hash(reactor).hex()],
        creator="ab" * 16,
    )
    _seed_group_copy(
        node, group=group, author=author, sent_at=100.0, message_id="local-copy-1"
    )
    response = call_handler(
        node._handle_reaction,
        _signed_group_reaction(
            reactor,
            "other-leg-1",
            "👍",
            "react-1",
            group=group,
            author=author,
            sent_at=100.0,
        ),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    assert len(node.inbox.reactions_for("local-copy-1")) == 1
    assert node.inbox.reactions_for("other-leg-1") == []


def test_group_reaction_refused_for_non_member(tmp_path):
    # A valid signature proves who chose the context, not that they
    # belong to the group: an allow-listed outsider who learned the
    # group id, author and send time must not get a reaction onto a
    # message in a group they never joined.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, destination_hash(RNS.Identity()).hex()],
        creator="ab" * 16,
    )
    _seed_group_copy(
        node, group=group, author=author, sent_at=100.0, message_id="local-copy-1"
    )
    response = call_handler(
        node._handle_reaction,
        _signed_group_reaction(
            reactor,
            "other-leg-1",
            "👍",
            "react-1",
            group=group,
            author=author,
            sent_at=100.0,
        ),
        remote_identity=reactor,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_a_member"
    assert node.inbox.reactions_for("local-copy-1") == []


def test_group_reaction_refused_when_group_deleted(tmp_path):
    # Qodo Critical (flagged by the user from the stacked PR #297's
    # reviews): a failed group-roster lookup used to count as
    # permission, so after a group is deleted an allow-listed
    # non-member who knows a retained message id could get a
    # reaction stored and displayed.  A missing roster for a
    # resolved group message now fails closed.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, destination_hash(RNS.Identity()).hex()],
        creator="ab" * 16,
    )
    _seed_group_copy(
        node, group=group, author=author, sent_at=100.0, message_id="local-copy-1"
    )
    assert node.groups.delete(group) is True
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "local-copy-1", "👍", "react-1"),
        remote_identity=reactor,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_a_member"
    assert node.inbox.reactions_for("local-copy-1") == []


def test_group_reaction_refused_when_namesake_masks_deleted_group(tmp_path):
    # CodeRabbit Major (node.py:2149, CWE-863): after a group is
    # deleted, an outsider can deliver a group named after the
    # deleted group's 32-character id — find() then returns the
    # outsider's group by name, and the reaction passes the roster
    # check against the wrong roster.  Authorization now uses an
    # exact-id lookup, so the namesake does not substitute.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    reactor_hash = destination_hash(reactor).hex()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, destination_hash(RNS.Identity()).hex()],
        creator="ab" * 16,
    )
    _seed_group_copy(
        node, group=group, author=author, sent_at=100.0, message_id="local-copy-1"
    )
    # The outsider's group is named after the deleted group's id,
    # and names the outsider as a member.
    namesake = node.groups.create(
        group, [reactor_hash, destination_hash(RNS.Identity()).hex()],
        created_by=reactor_hash,
    )
    assert namesake["id"] != group
    assert node.groups.delete(group) is True
    # The name fallback still resolves the deleted id to the namesake.
    assert node.groups.find(group)["id"] == namesake["id"]
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "local-copy-1", "👍", "react-1"),
        remote_identity=reactor,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_a_member"
    assert node.inbox.reactions_for("local-copy-1") == []


def test_group_reaction_recheck_voids_when_namesake_masks_deleted_group(
    tmp_path, monkeypatch
):
    # Same finding, second net: the post-store recheck re-reads the
    # roster, so a group deleted (and masked by a namesake) between
    # the pre-store check and the recheck must void the stored
    # reaction instead of letting the namesake's roster keep it.  A
    # missing roster fails closed here too.  The copy arrives between
    # the check and the recheck, so the reaction is stored with
    # group context first and only the recheck sees the message.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    reactor_hash = destination_hash(reactor).hex()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, reactor_hash],
        creator="ab" * 16,
    )
    copy = {
        "from": "alice",
        "destination": author,
        "text": "crew hello",
        "kind": "group",
        "group": group,
        "group_name": "crew",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "local-copy-1",
    }
    # Swap the roster between the pre-store resolve and the
    # post-store recheck: the copy lands, the real group is deleted,
    # and a namesake (named after the deleted id) that still lists
    # the reactor is pinned.
    calls = {"n": 0}
    real_resolve = node.inbox.resolve

    def swapping_resolve(target_id):
        calls["n"] += 1
        if calls["n"] == 2:
            node.inbox._messages.append(dict(copy))
            assert node.groups.delete(group) is True
            node.groups.create(
                group,
                [reactor_hash, destination_hash(RNS.Identity()).hex()],
                created_by=reactor_hash,
            )
        return real_resolve(target_id)

    monkeypatch.setattr(node.inbox, "resolve", swapping_resolve)
    call_handler(
        node._handle_reaction,
        _signed_group_reaction(
            reactor, "local-copy-1", "👍", "react-1",
            group=group, author=author, sent_at=100.0,
        ),
        remote_identity=reactor,
    )
    assert calls["n"] == 2
    assert node.inbox.reactions_for("local-copy-1") == []


def test_group_reaction_naming_a_leg_id_stores_under_canonical_id(
    tmp_path,
):
    # CodeRabbit Major: a group send stores one leg id as the record
    # id and the rest in leg_ids.  A reaction naming the other leg
    # must resolve to the stored record and be stored under the
    # canonical id, or it never displays under the message listing.
    # The target here is the author's own outbound record, whose
    # destination is empty — so the signed-context lookup misses it
    # too, and only the leg-aware resolve saves the reaction.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = "ab" * 16
    reactor = RNS.Identity()
    reactor_hash = destination_hash(reactor).hex()
    group = _pinned_group(
        node,
        members=["ab" * 16, reactor_hash],
        creator="ab" * 16,
    )
    node.inbox.append(
        {
            "from": "alice",
            "destination": "",
            "text": "crew hello",
            "kind": "group",
            "group": group,
            "group_name": "crew",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "g-1",
            "leg_ids": ["g-1", "g-2"],
            "direction": "out",
        }
    )
    response = call_handler(
        node._handle_reaction,
        _signed_group_reaction(
            reactor, "g-2", "👍", "react-1",
            group=group, author=author, sent_at=100.0,
        ),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    reactions = node.inbox.reactions_for("g-1")
    assert [r["reaction"] for r in reactions] == ["👍"]
    assert node.inbox.reactions_for("g-2") == []


def test_group_reaction_final_check_refuses_when_group_deleted(
    tmp_path, monkeypatch
):
    # The same fail-closed rule in the atomic final check: a
    # no-context reaction whose target lands between the first
    # lookup and the final check, on a group that was deleted
    # before the check runs.  The copy is stubbed into the final
    # lookup because a real import cannot land inside the
    # transaction — the flock is held there, and a second Inbox
    # import would deadlock (the round-11 lesson).
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, destination_hash(RNS.Identity()).hex()],
        creator="ab" * 16,
    )
    assert node.groups.delete(group) is True
    real_resolve = node.inbox.resolve
    calls = {"n": 0}

    def flaky_resolve(ref):
        calls["n"] += 1
        if calls["n"] == 1:
            # The first lookup: the copy has not landed yet.
            return real_resolve(ref)
        # The final check, inside the transaction: the copy is here
        # now, on a group whose roster is gone.
        return {"id": "local-copy-1", "kind": "group", "group": group}

    monkeypatch.setattr(node.inbox, "resolve", flaky_resolve)
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "local-copy-1", "👍", "react-1"),
        remote_identity=reactor,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_a_member"
    assert calls["n"] == 2
    assert node.inbox.reactions_for("local-copy-1") == []


def test_group_reaction_reconciles_when_message_arrives_late(tmp_path):
    # The reaction outruns the message: stored under the reactor's
    # foreign wire id, invisible.  When the copy lands, it moves onto
    # the local id so it displays under the message.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = RNS.Identity()
    author_hash = destination_hash(author).hex()
    reactor = RNS.Identity()
    reactor_hash = destination_hash(reactor).hex()
    members = ["ab" * 16, author_hash, reactor_hash]
    group = derive_group_id("ab" * 16, "crew", members)
    response = call_handler(
        node._handle_reaction,
        _signed_group_reaction(
            reactor,
            "other-leg-1",
            "👍",
            "react-1",
            group=group,
            author=author_hash,
            sent_at=100.0,
        ),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    assert len(node.inbox.reactions_for("other-leg-1")) == 1
    assert node.inbox.reactions_for("local-copy-1") == []
    delivered = call_handler(
        node._handle_group_message,
        encode_group_message(
            "crew hello",
            message_id="local-copy-1",
            group_id=group,
            group_name="crew",
            creator="ab" * 16,
            members=members,
            sent_at=100.0,
        ),
        remote_identity=author,
    )
    assert delivered["ok"] is True
    assert node.inbox.reactions_for("other-leg-1") == []
    shown = node.inbox.reactions_for("local-copy-1")
    assert len(shown) == 1
    assert shown[0]["reaction"] == "👍"
    assert shown[0]["id"] == "react-1"


def test_group_reaction_reconciles_when_relayed_copy_arrives_late(tmp_path):
    # The same late arrival, but the copy comes through sealed relay
    # mail: _deliver_collected must reconcile the stored reaction onto
    # the local copy id too, not just the direct /tau/group path.
    node = make_node(tmp_path)
    recipient = RNS.Identity()
    node.identity = recipient
    node.destination = SimpleNamespace(
        hexhash=destination_hash(recipient).hex(),
        hash=destination_hash(recipient),
    )
    own = destination_hash(recipient).hex()
    author = RNS.Identity()
    author_hash = destination_hash(author).hex()
    reactor = RNS.Identity()
    reactor_hash = destination_hash(reactor).hex()
    members = [own, author_hash, reactor_hash]
    group = derive_group_id(author_hash, "crew", members)
    response = call_handler(
        node._handle_reaction,
        _signed_group_reaction(
            reactor,
            "other-leg-1",
            "👍",
            "react-1",
            group=group,
            author=author_hash,
            sent_at=100.0,
        ),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    assert len(node.inbox.reactions_for("other-leg-1")) == 1
    assert node.inbox.reactions_for("local-copy-1") == []
    blob = seal_message(
        author,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="local-copy-1",
        text="crew hello",
        sent_at=100.0,
        group={
            "id": group,
            "name": "crew",
            "creator": author_hash,
            "members": members,
        },
    )
    message = open_message(recipient, blob)
    assert node._deliver_collected(message) is True
    assert node.inbox.reactions_for("other-leg-1") == []
    shown = node.inbox.reactions_for("local-copy-1")
    assert len(shown) == 1
    assert shown[0]["reaction"] == "👍"
    assert shown[0]["id"] == "react-1"


def test_queued_group_leg_retry_reuses_shared_send_time(tmp_path):
    # A queued leg was enqueued with the fan-out's shared send time;
    # the retry must carry that time on the wire, not the later
    # enqueue time — otherwise a reaction left on the live leg's copy
    # can never reconcile onto the retried copy.
    node = make_node(tmp_path)
    peer = node.registry.upsert(bytes.fromhex("ab" * 16), b"\x02" * 64, name="alice")
    calls, fake_request = _recorded_request()
    node._request = fake_request

    group = derive_group_id("ab" * 16, "crew", ["ab" * 16, "cd" * 16])
    entry = node.mailbox.enqueue(
        to=bytes.fromhex("ab" * 16),
        text="crew hello",
        message_id="leg-1",
        group={
            "id": group,
            "name": "crew",
            "creator": "ab" * 16,
            "members": ["ab" * 16, "cd" * 16],
        },
        sent_at=100.0,
    )
    assert entry.created_at != 100.0
    assert node._deliver_entry(peer, entry, timeout=5) is True
    assert len(calls) == 1
    payload = parse_group_message(calls[0][2])
    assert payload["sent_at"] == 100.0


def test_queued_group_leg_relay_copy_carries_shared_send_time(tmp_path):
    # One leg live, one queued: the queued leg's copy arrives through
    # sealed relay mail.  The deposit must seal it with the fan-out's
    # shared send time, so an early reaction left on the live copy's
    # id reconciles onto the relayed copy instead of staying stranded.
    (tmp_path / "recipient").mkdir()
    (tmp_path / "sender").mkdir()
    recipient_node = make_node(tmp_path / "recipient")
    recipient = RNS.Identity()
    recipient_node.identity = recipient
    recipient_node.destination = SimpleNamespace(
        hexhash=destination_hash(recipient).hex(),
        hash=destination_hash(recipient),
    )
    own = destination_hash(recipient).hex()
    author = RNS.Identity()
    author_hash = destination_hash(author).hex()
    reactor = RNS.Identity()
    reactor_hash = destination_hash(reactor).hex()
    members = [own, author_hash, reactor_hash]
    group = derive_group_id(author_hash, "crew", members)
    response = call_handler(
        recipient_node._handle_reaction,
        _signed_group_reaction(
            reactor,
            "other-leg-1",
            "👍",
            "react-1",
            group=group,
            author=author_hash,
            sent_at=100.0,
        ),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    assert len(recipient_node.inbox.reactions_for("other-leg-1")) == 1
    assert recipient_node.inbox.reactions_for("local-copy-1") == []

    sender_node = make_node(tmp_path / "sender")
    sender_node.identity = author
    holder = sender_node.registry.upsert(
        bytes.fromhex("cd" * 16), b"\x03" * 64, name="holder"
    )
    sender_node.registry.upsert(
        destination_hash(recipient), recipient.get_public_key(), name="bob"
    )
    calls, fake_request = _recorded_request()
    sender_node._request = fake_request
    entry = sender_node.mailbox.enqueue(
        to=destination_hash(recipient),
        text="crew hello",
        message_id="local-copy-1",
        group={
            "id": group,
            "name": "crew",
            "creator": author_hash,
            "members": members,
        },
        sent_at=100.0,
    )
    assert sender_node._deposit_entry(holder, entry, timeout=5) is True
    assert len(calls) == 1
    stored = parse_mail_store(calls[0][2])
    message = open_message(recipient, stored["blob"])
    assert recipient_node._deliver_collected(message) is True
    assert recipient_node.inbox.reactions_for("other-leg-1") == []
    shown = recipient_node.inbox.reactions_for("local-copy-1")
    assert len(shown) == 1
    assert shown[0]["reaction"] == "👍"
    assert shown[0]["id"] == "react-1"


def test_group_reaction_not_reconciled_for_non_member(tmp_path):
    # The same late arrival, but the reactor is not on the roster the
    # message pins: the reaction stays stranded under the foreign id
    # and never surfaces under the message.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = RNS.Identity()
    author_hash = destination_hash(author).hex()
    reactor = RNS.Identity()
    members = ["ab" * 16, author_hash, destination_hash(RNS.Identity()).hex()]
    group = derive_group_id("ab" * 16, "crew", members)
    response = call_handler(
        node._handle_reaction,
        _signed_group_reaction(
            reactor,
            "other-leg-1",
            "👍",
            "react-1",
            group=group,
            author=author_hash,
            sent_at=100.0,
        ),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    delivered = call_handler(
        node._handle_group_message,
        encode_group_message(
            "crew hello",
            message_id="local-copy-1",
            group_id=group,
            group_name="crew",
            creator="ab" * 16,
            members=members,
            sent_at=100.0,
        ),
        remote_identity=author,
    )
    assert delivered["ok"] is True
    assert node.inbox.reactions_for("local-copy-1") == []
    assert len(node.inbox.reactions_for("other-leg-1")) == 1


def test_group_reaction_to_known_id_checked_for_non_member(tmp_path):
    # CodeRabbit Major (node.py:1874): the roster check ran only when
    # the target id did not resolve — an allow-listed non-member
    # naming a known local group-message id directly, with no group
    # context, slipped past it.  The resolved message's roster is
    # checked too now.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, destination_hash(RNS.Identity()).hex()],
        creator="ab" * 16,
    )
    _seed_group_copy(
        node, group=group, author=author, sent_at=100.0, message_id="local-copy-1"
    )
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "local-copy-1", "👍", "react-1"),
        remote_identity=reactor,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_a_member"
    assert node.inbox.reactions_for("local-copy-1") == []


def test_group_reaction_reloads_before_resolving_imported_copy(tmp_path):
    # CodeRabbit Major (node.py:1835): resolve() reads the cached
    # inbox, so a group-message copy imported by another process is
    # invisible to it — a reaction naming that id would skip both
    # roster checks.  The handler reloads first, mirroring
    # _handle_retract.
    from tau.inbox import Inbox

    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, destination_hash(RNS.Identity()).hex()],
        creator="ab" * 16,
    )
    # Another process imports the copy; this node's cache is stale.
    other = Inbox(node.inbox.path)
    other.append(
        {
            "from": "alice",
            "destination": author,
            "text": "crew hello",
            "kind": "group",
            "group": group,
            "group_name": "crew",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "local-copy-1",
        }
    )
    with pytest.raises(ValueError):
        node.inbox.resolve("local-copy-1")
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "local-copy-1", "👍", "react-1"),
        remote_identity=reactor,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_a_member"
    assert node.inbox.reactions_for("local-copy-1") == []


def test_group_reaction_to_known_id_accepted_for_member(tmp_path):
    # The same direct reaction, from a reactor the roster names, is
    # stored under the message as before.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, destination_hash(reactor).hex()],
        creator="ab" * 16,
    )
    _seed_group_copy(
        node, group=group, author=author, sent_at=100.0, message_id="local-copy-1"
    )
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "local-copy-1", "👍", "react-1"),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    assert len(node.inbox.reactions_for("local-copy-1")) == 1


def test_group_reaction_rechecks_after_storing_unresolved(tmp_path, monkeypatch):
    # CodeRabbit Major (node.py:1648): the reaction's lookup can race
    # the message's arrival — the copy lands after find_group_message
    # misses it but before the reaction is stored, and the message
    # handler's reconcile already ran.  The handler rechecks once
    # after storing, so the reaction still lands on the copy.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, destination_hash(reactor).hex()],
        creator="ab" * 16,
    )
    _seed_group_copy(
        node, group=group, author=author, sent_at=100.0, message_id="local-copy-1"
    )
    real_find = node.inbox.find_group_message
    calls = {"n": 0}

    def _miss_once(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return None
        return real_find(*args, **kwargs)

    monkeypatch.setattr(node.inbox, "find_group_message", _miss_once)
    response = call_handler(
        node._handle_reaction,
        _signed_group_reaction(
            reactor,
            "other-leg-1",
            "👍",
            "react-1",
            group=group,
            author=author,
            sent_at=100.0,
        ),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    assert calls["n"] == 2
    assert node.inbox.reactions_for("other-leg-1") == []
    stored = node.inbox.reactions_for("local-copy-1")
    assert len(stored) == 1
    assert stored[0]["reaction"] == "👍"


def test_group_reaction_voided_when_message_imported_mid_handler(
    tmp_path, monkeypatch
):
    # CodeRabbit follow-up (node.py:1850): a copy can be imported
    # after the reaction is stored but before the post-store recheck
    # runs — the handler rechecks after storing and voids a
    # non-member's reaction on the imported copy.  (The final check
    # and the persist now hold one transaction, so the copy can no
    # longer land between them — the import is forced between the
    # store and the recheck instead, via the recheck's reload.)
    from tau.inbox import Inbox

    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, destination_hash(RNS.Identity()).hex()],
        creator="ab" * 16,
    )
    copy = {
        "from": "alice",
        "destination": author,
        "text": "crew hello",
        "kind": "group",
        "group": group,
        "group_name": "crew",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "local-copy-1",
    }
    real_reload = node.inbox.reload
    calls = {"n": 0}

    def flaky_reload(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            other = Inbox(node.inbox.path)
            other.append(copy)
        return real_reload(*args, **kwargs)

    monkeypatch.setattr(node.inbox, "reload", flaky_reload)
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "local-copy-1", "👍", "react-1", sent_at=200.0),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    assert node.inbox.reactions_for("local-copy-1") == []
    # A replay of the voided reaction cannot resurrect it: the
    # tombstone is strictly newer than the original, so merging the
    # original back is a no-op.  (Replaying through the handler
    # would dedupe on the reaction id instead, so replay at the
    # inbox level, with the monkeypatch removed.)
    monkeypatch.undo()
    node.inbox.record_reaction(
        {
            "target": "local-copy-1",
            "reaction": "👍",
            "from": "unknown",
            "destination": destination_hash(reactor).hex(),
            "sent_at": 200.0,
            "received_at": 201.0,
            "id": "react-1",
        }
    )
    assert node.inbox.reactions_for("local-copy-1") == []


def test_group_reaction_final_check_refuses_when_copy_lands_before_store(
    tmp_path, monkeypatch
):
    # CodeRabbit round-9 follow-up (node.py:2032): a no-context
    # reaction can miss the target in the first lookup, then be
    # stored and published before the post-store recheck runs — and
    # if that recheck fails, the non-member's reaction stays
    # visible.  The handler now re-checks authorization immediately
    # before storing: a copy imported after the first lookup is
    # caught here, and the reaction is refused before anything is
    # stored or published.  The import is forced between the two
    # lookups by a resolve that misses once, then delegates.
    from tau.inbox import Inbox

    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, destination_hash(RNS.Identity()).hex()],
        creator="ab" * 16,
    )
    Inbox(node.inbox.path).append(
        {
            "from": "alice",
            "destination": author,
            "text": "crew hello",
            "kind": "group",
            "group": group,
            "group_name": "crew",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "local-copy-1",
        }
    )
    assert node.inbox.list_messages() == []
    real_resolve = node.inbox.resolve
    calls = {"n": 0}

    def flaky_resolve(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return None
        return real_resolve(*args, **kwargs)

    monkeypatch.setattr(node.inbox, "resolve", flaky_resolve)
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "local-copy-1", "👍", "react-1"),
        remote_identity=reactor,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_a_member"
    assert calls["n"] == 2
    assert node.inbox.reactions_for("local-copy-1") == []


def test_group_reaction_final_check_storage_failure_stores_nothing(
    tmp_path, monkeypatch
):
    # The atomic final check + store reloads at the transaction
    # entry; if that read fails, the reaction is refused with
    # storage_failed and nothing is stored — it is never stored
    # against a stale view of the inbox.
    from tau.inbox import InboxError

    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    reactor = RNS.Identity()
    real_load = node.inbox._load
    calls = {"n": 0}

    def flaky_load(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise InboxError("boom")
        return real_load(*args, **kwargs)

    monkeypatch.setattr(node.inbox, "_load", flaky_load)
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "unknown-target", "👍", "react-1"),
        remote_identity=reactor,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "storage_failed"
    assert calls["n"] == 2
    assert node.inbox.reactions_for("unknown-target") == []


def test_group_reaction_recheck_reload_failure_is_storage_failed(
    tmp_path, monkeypatch
):
    # The post-store recheck is the backstop for a copy that lands
    # between the final pre-store check and the persist.  If its
    # reload fails, the handler answers storage_failed instead of
    # acknowledging a reaction it could not verify — the sender
    # retries, and the retry's refusal voids the orphan.  The
    # reaction IS stored (the store already ran); only the
    # acknowledgement is withheld.
    from tau.inbox import Inbox, InboxError

    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, destination_hash(RNS.Identity()).hex()],
        creator="ab" * 16,
    )
    copy = {
        "from": "alice",
        "destination": author,
        "text": "crew hello",
        "kind": "group",
        "group": group,
        "group_name": "crew",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "local-copy-1",
    }
    real_reload = node.inbox.reload
    calls = {"n": 0}

    def flaky_reload(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            # The copy lands between the store and the recheck; the
            # recheck's reload then fails, so it cannot verify it.
            # (The import happens here, not inside the store: the
            # final check and the persist hold one transaction, so
            # no other process can commit between them.)
            other = Inbox(node.inbox.path)
            other.append(copy)
            raise InboxError("boom")
        return real_reload(*args, **kwargs)

    monkeypatch.setattr(node.inbox, "reload", flaky_reload)
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "local-copy-1", "👍", "react-1", sent_at=200.0),
        remote_identity=reactor,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "storage_failed"
    assert len(node.inbox.reactions_for("local-copy-1")) == 1


def test_group_reaction_recheck_tombstone_failure_is_storage_failed(
    tmp_path, monkeypatch
):
    # Same ordering as the reload-failure test, but the failure is
    # the tombstone write itself: the recheck finds the non-member
    # and cannot void the reaction, so the handler answers
    # storage_failed instead of acknowledging it.
    from tau.inbox import Inbox

    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, destination_hash(RNS.Identity()).hex()],
        creator="ab" * 16,
    )
    copy = {
        "from": "alice",
        "destination": author,
        "text": "crew hello",
        "kind": "group",
        "group": group,
        "group_name": "crew",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "local-copy-1",
    }
    real_record = node.inbox.record_reaction
    real_reload = node.inbox.reload
    calls = {"n": 0}

    def _fail_tombstone(record):
        if record.get("reaction") == "":
            raise OSError("disk full")
        return real_record(record)

    def flaky_reload(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            # The copy lands between the store and the recheck (see
            # the reload-failure test: the import cannot happen
            # inside the store, which holds the transaction).
            other = Inbox(node.inbox.path)
            other.append(copy)
        return real_reload(*args, **kwargs)

    monkeypatch.setattr(node.inbox, "record_reaction", _fail_tombstone)
    monkeypatch.setattr(node.inbox, "reload", flaky_reload)
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "local-copy-1", "👍", "react-1", sent_at=200.0),
        remote_identity=reactor,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "storage_failed"
    assert len(node.inbox.reactions_for("local-copy-1")) == 1


def test_group_reaction_retry_voids_orphan_after_failed_recheck(
    tmp_path, monkeypatch
):
    # The fail-closed recheck leaves the orphan stored (the store
    # already ran) but unacknowledged, so the sender retries with
    # the same id.  The retry resolves the copy, is refused for
    # membership, and the refusal voids the orphan — the reaction
    # does not stay displayed.
    from tau.inbox import Inbox

    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, destination_hash(RNS.Identity()).hex()],
        creator="ab" * 16,
    )
    copy = {
        "from": "alice",
        "destination": author,
        "text": "crew hello",
        "kind": "group",
        "group": group,
        "group_name": "crew",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "local-copy-1",
    }
    real_record = node.inbox.record_reaction
    real_reload = node.inbox.reload
    calls = {"n": 0}

    def _fail_tombstone(record):
        if record.get("reaction") == "":
            raise OSError("disk full")
        return real_record(record)

    def flaky_reload(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            # The copy lands between the store and the recheck (see
            # the reload-failure test).
            other = Inbox(node.inbox.path)
            other.append(copy)
        return real_reload(*args, **kwargs)

    monkeypatch.setattr(node.inbox, "record_reaction", _fail_tombstone)
    monkeypatch.setattr(node.inbox, "reload", flaky_reload)
    payload = signed_reaction(
        reactor, "local-copy-1", "👍", "react-1", sent_at=200.0
    )
    first = call_handler(node._handle_reaction, payload, remote_identity=reactor)
    assert first["ok"] is False
    assert first["error"]["code"] == "storage_failed"
    assert len(node.inbox.reactions_for("local-copy-1")) == 1
    monkeypatch.undo()
    second = call_handler(node._handle_reaction, payload, remote_identity=reactor)
    assert second["ok"] is False
    assert second["error"]["code"] == "not_a_member"
    assert node.inbox.reactions_for("local-copy-1") == []


def test_group_reaction_not_published_when_recheck_fails(
    tmp_path, monkeypatch
):
    # CodeRabbit round-11: publication must wait for authorization.
    # A reaction whose post-store recheck fails is stored (the store
    # already ran) but never fanned out to the live console — the
    # sender retries instead of the console showing a reaction that
    # was never verified.  The copy lands between the store and the
    # recheck (via the recheck's reload, as in the reload-failure
    # test); the recheck's reload then fails.  The stored reaction
    # test); the recheck's reload then fails.  The stored reaction
    # stays in the inbox (round-12 note: it is never displayed —
    # the console and `tau messages` filter stored reactions against
    # the pinned roster, see test_group_message_hides_non_member_
    # reaction and test_messages_hides_non_member_group_reaction),
    # and the retry's refusal voids it.
    from tau.inbox import Inbox, InboxError

    published = []
    node = make_node(tmp_path, on_reaction=published.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, destination_hash(RNS.Identity()).hex()],
        creator="ab" * 16,
    )
    copy = {
        "from": "alice",
        "destination": author,
        "text": "crew hello",
        "kind": "group",
        "group": group,
        "group_name": "crew",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "local-copy-1",
    }
    real_reload = node.inbox.reload
    calls = {"n": 0}

    def flaky_reload(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            other = Inbox(node.inbox.path)
            other.append(copy)
            raise InboxError("boom")
        return real_reload(*args, **kwargs)

    monkeypatch.setattr(node.inbox, "reload", flaky_reload)
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "local-copy-1", "👍", "react-1", sent_at=200.0),
        remote_identity=reactor,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "storage_failed"
    assert published == []
    assert len(node.inbox.reactions_for("local-copy-1")) == 1


def test_group_reaction_published_after_authorization(tmp_path):
    # Control for the deferred publish: a member's reaction is
    # fanned out live once authorization has completed.
    published = []
    node = make_node(tmp_path, on_reaction=published.append)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = destination_hash(RNS.Identity()).hex()
    reactor = RNS.Identity()
    member = destination_hash(reactor).hex()
    group = _pinned_group(
        node,
        members=["ab" * 16, author, member],
        creator="ab" * 16,
    )
    _seed_group_copy(
        node, group=group, author=author, sent_at=100.0, message_id="local-copy-1"
    )
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "local-copy-1", "👍", "react-1"),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    assert len(published) == 1
    assert published[0]["reaction"] == "👍"
    assert published[0]["target"] == "local-copy-1"


def test_group_reaction_final_check_and_store_hold_one_transaction(
    tmp_path, monkeypatch
):
    # CodeRabbit round-11: the final check and the persist hold one
    # inbox transaction, so no other process can import the target
    # between them.  Prove it by having each lookup attempt a
    # non-blocking flock on the inbox lock file: the initial lookup
    # and the recheck run outside any transaction (probe succeeds),
    # but the final check runs inside the transaction that also
    # stores the reaction (probe fails — this process already holds
    # the flock).  In the old code the final check ran outside any
    # transaction too, so every probe succeeded.
    try:
        import fcntl
    except ImportError:  # pragma: no cover - POSIX-only lock
        pytest.skip("fcntl is unavailable")

    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    reactor = RNS.Identity()
    real_resolve = node.inbox.resolve
    probes = []

    def _resolve_and_probe(*args, **kwargs):
        lock_path = node.inbox.path.with_name(node.inbox.path.name + ".lock")
        with open(lock_path, "a") as handle:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                probes.append(True)
            else:
                probes.append(False)
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return real_resolve(*args, **kwargs)

    monkeypatch.setattr(node.inbox, "resolve", _resolve_and_probe)
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "unknown-target", "👍", "react-1"),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    assert probes == [False, True, False]


def test_group_reaction_purged_when_message_arrives_after_it(tmp_path):
    # CodeRabbit follow-up (node.py:1850): a reaction stored before
    # its target arrives — no context, so no roster check could run
    # — would surface under the exact id when the copy lands.  The
    # message handler voids non-members' reactions on arrival.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = RNS.Identity()
    author_hash = destination_hash(author).hex()
    reactor = RNS.Identity()
    members = ["ab" * 16, author_hash, destination_hash(RNS.Identity()).hex()]
    group = derive_group_id("ab" * 16, "crew", members)
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "local-copy-1", "👍", "react-1"),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    assert len(node.inbox.reactions_for("local-copy-1")) == 1
    delivered = call_handler(
        node._handle_group_message,
        encode_group_message(
            "crew hello",
            message_id="local-copy-1",
            group_id=group,
            group_name="crew",
            creator="ab" * 16,
            members=members,
            sent_at=100.0,
        ),
        remote_identity=author,
    )
    assert delivered["ok"] is True
    assert node.inbox.reactions_for("local-copy-1") == []


def test_group_reaction_kept_when_member_reacts_before_arrival(tmp_path):
    # The same early reaction, from a reactor the roster names, is
    # kept when the message arrives.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    author = RNS.Identity()
    author_hash = destination_hash(author).hex()
    reactor = RNS.Identity()
    reactor_hash = destination_hash(reactor).hex()
    members = ["ab" * 16, author_hash, reactor_hash]
    group = derive_group_id("ab" * 16, "crew", members)
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "local-copy-1", "👍", "react-1"),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    delivered = call_handler(
        node._handle_group_message,
        encode_group_message(
            "crew hello",
            message_id="local-copy-1",
            group_id=group,
            group_name="crew",
            creator="ab" * 16,
            members=members,
            sent_at=100.0,
        ),
        remote_identity=author,
    )
    assert delivered["ok"] is True
    shown = node.inbox.reactions_for("local-copy-1")
    assert len(shown) == 1
    assert shown[0]["reaction"] == "👍"


def test_group_reaction_reconciles_when_relayed_copy_arrives(tmp_path):
    # Issue #293: a group message arriving via sealed relay mail
    # reconciles stored context reactions onto the relayed copy,
    # exactly as a direct group message does.
    node = make_node(tmp_path)
    recipient = RNS.Identity()
    node.identity = recipient
    node.destination = SimpleNamespace(
        hexhash=destination_hash(recipient).hex(),
        hash=destination_hash(recipient),
    )
    own = destination_hash(recipient).hex()
    author = RNS.Identity()
    author_hash = destination_hash(author).hex()
    reactor = RNS.Identity()
    reactor_hash = destination_hash(reactor).hex()
    members = [own, author_hash, reactor_hash]
    group = derive_group_id(author_hash, "crew", members)
    response = call_handler(
        node._handle_reaction,
        _signed_group_reaction(
            reactor,
            "other-leg-1",
            "👍",
            "react-1",
            group=group,
            author=author_hash,
            sent_at=100.0,
        ),
        remote_identity=reactor,
    )
    assert response["ok"] is True
    assert len(node.inbox.reactions_for("other-leg-1")) == 1
    blob = seal_message(
        author,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="local-copy-1",
        text="crew hello",
        sent_at=100.0,
        group={
            "id": group,
            "name": "crew",
            "creator": author_hash,
            "members": members,
        },
    )
    message = open_message(recipient, blob)
    assert node._deliver_collected(message) is True
    assert node.inbox.reactions_for("other-leg-1") == []
    shown = node.inbox.reactions_for("local-copy-1")
    assert len(shown) == 1
    assert shown[0]["reaction"] == "👍"
    assert shown[0]["id"] == "react-1"


def test_group_reaction_purge_also_clears_console_cache(tmp_path):
    # CodeRabbit Major (node.py:1664): the inbox purge voids a
    # non-member's early reaction, but the console bridge caches
    # reactions separately and seeds appended messages from that
    # cache — so the voided reaction still displayed on the live
    # group message.  The purge fans its tombstones out through
    # on_reaction, so the bridge cache is corrected before the
    # message is appended.
    from tau.webapi import WebBridge

    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # Capture the live entry record_inbound publishes, not the
        # snapshot serve: messages() reconciles against the inbox on
        # read and would hide a stale seed, but the live push already
        # showed it.
        published: list = []

        def _capture(record):
            message = bridge.record_inbound(record)
            if message is not None:
                published.append(message)

        node.on_message = _capture
        author = RNS.Identity()
        author_hash = destination_hash(author).hex()
        reactor = RNS.Identity()
        members = ["ab" * 16, author_hash, destination_hash(RNS.Identity()).hex()]
        group = derive_group_id("ab" * 16, "crew", members)
        response = call_handler(
            node._handle_reaction,
            signed_reaction(reactor, "local-copy-1", "👍", "react-1"),
            remote_identity=reactor,
        )
        assert response["ok"] is True
        # The bridge cached the early reaction...
        assert len(bridge._reaction_state) == 1
        delivered = call_handler(
            node._handle_group_message,
            encode_group_message(
                "crew hello",
                message_id="local-copy-1",
                group_id=group,
                group_name="crew",
                creator="ab" * 16,
                members=members,
                sent_at=100.0,
            ),
            remote_identity=author,
        )
        assert delivered["ok"] is True
        # ...but the live published entry carries no reaction.
        assert len(published) == 1
        assert published[0]["reactions"] == []
        assert node.inbox.reactions_for("local-copy-1") == []
    finally:
        bridge.stop()


def test_group_reaction_purge_clears_console_cache_for_relayed_copy(tmp_path):
    # The same bridge-cache correction on the relayed arrival path:
    # the purge runs before on_message, so the tombstone reaches
    # the bridge before the entry is appended.
    from tau.webapi import WebBridge

    node = make_node(tmp_path)
    recipient = RNS.Identity()
    node.identity = recipient
    node.destination = SimpleNamespace(
        hexhash=destination_hash(recipient).hex(),
        hash=destination_hash(recipient),
    )
    own = destination_hash(recipient).hex()
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # Capture the live entry, as in the direct-path test: the
        # snapshot serve reconciles against the inbox on read.
        published: list = []

        def _capture(record):
            message = bridge.record_inbound(record)
            if message is not None:
                published.append(message)

        node.on_message = _capture
        author = RNS.Identity()
        author_hash = destination_hash(author).hex()
        reactor = RNS.Identity()
        members = [own, author_hash, destination_hash(RNS.Identity()).hex()]
        group = derive_group_id(author_hash, "crew", members)
        response = call_handler(
            node._handle_reaction,
            signed_reaction(reactor, "local-copy-1", "👍", "react-1"),
            remote_identity=reactor,
        )
        assert response["ok"] is True
        assert len(bridge._reaction_state) == 1
        blob = seal_message(
            author,
            sender_name="alice",
            recipient_public_key=recipient.get_public_key(),
            message_id="local-copy-1",
            text="crew hello",
            sent_at=100.0,
            group={
                "id": group,
                "name": "crew",
                "creator": author_hash,
                "members": members,
            },
        )
        message = open_message(recipient, blob)
        assert node._deliver_collected(message) is True
        assert len(published) == 1
        assert published[0]["reactions"] == []
        assert node.inbox.reactions_for("local-copy-1") == []
    finally:
        bridge.stop()


def test_group_message_refused_when_purge_fails(tmp_path, monkeypatch):
    # CodeRabbit follow-up (node.py:1664): if the purge fails, the
    # voided reaction is still stored and still cached, so recording
    # the message would leave the reaction readable through
    # reactions_for.  The arrival fails closed: the delivery is
    # refused and nothing is recorded or marked seen — the sender
    # retries once the store is healthy.  The purge runs inside
    # Inbox.append_group_message now, so the failure is injected at
    # its locked helper.
    from tau.webapi import WebBridge

    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        published: list = []

        def _capture(record):
            message = bridge.record_inbound(record)
            if message is not None:
                published.append(message)

        node.on_message = _capture
        author = RNS.Identity()
        author_hash = destination_hash(author).hex()
        reactor = RNS.Identity()
        members = ["ab" * 16, author_hash, destination_hash(RNS.Identity()).hex()]
        group = derive_group_id("ab" * 16, "crew", members)
        response = call_handler(
            node._handle_reaction,
            signed_reaction(reactor, "local-copy-1", "👍", "react-1"),
            remote_identity=reactor,
        )
        assert response["ok"] is True

        def _fail(*args, **kwargs):
            raise OSError("disk full")

        monkeypatch.setattr(
            node.inbox, "_purge_non_member_reactions_locked", _fail
        )
        delivered = call_handler(
            node._handle_group_message,
            encode_group_message(
                "crew hello",
                message_id="local-copy-1",
                group_id=group,
                group_name="crew",
                creator="ab" * 16,
                members=members,
                sent_at=100.0,
            ),
            remote_identity=author,
        )
        assert delivered["ok"] is False
        assert delivered["error"]["code"] == "storage_failed"
        assert published == []
        assert node.inbox.list_messages() == []
        assert not node.mailbox.is_seen("local-copy-1")
        # The reaction is still stored — but with no message holding
        # its target, it is readable nowhere.
        assert len(node.inbox.reactions_for("local-copy-1")) == 1
    finally:
        bridge.stop()


def test_relayed_message_refused_when_purge_fails(tmp_path, monkeypatch):
    # The same fail-closed refusal on the relayed path, now as a
    # distinct retryable outcome: _deliver_collected raises
    # DeliveryDeferredError instead of returning False, so the
    # callers keep the held copy and leave the envelope
    # unacknowledged.  Nothing is recorded or marked seen, so a
    # later sweep retries it.
    node = make_node(tmp_path)
    recipient = RNS.Identity()
    node.identity = recipient
    node.destination = SimpleNamespace(
        hexhash=destination_hash(recipient).hex(),
        hash=destination_hash(recipient),
    )
    own = destination_hash(recipient).hex()
    author = RNS.Identity()
    author_hash = destination_hash(author).hex()
    reactor = RNS.Identity()
    members = [own, author_hash, destination_hash(RNS.Identity()).hex()]
    group = derive_group_id(author_hash, "crew", members)
    response = call_handler(
        node._handle_reaction,
        signed_reaction(reactor, "local-copy-1", "👍", "react-1"),
        remote_identity=reactor,
    )
    assert response["ok"] is True

    def _fail(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(
        node.inbox, "_purge_non_member_reactions_locked", _fail
    )
    blob = seal_message(
        author,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="local-copy-1",
        text="crew hello",
        sent_at=100.0,
        group={
            "id": group,
            "name": "crew",
            "creator": author_hash,
            "members": members,
        },
    )
    message = open_message(recipient, blob)
    with pytest.raises(DeliveryDeferredError):
        node._deliver_collected(message)
    assert node.inbox.list_messages() == []
    assert not node.mailbox.is_seen("local-copy-1")


def test_group_message_recorded_but_not_published_when_fan_out_fails(
    tmp_path, monkeypatch
):
    # CodeRabbit follow-up (node.py:1664): if the console cache
    # update fails, the bridge still holds the stale reaction and
    # would seed it into the live entry.  The message still records
    # — the inbox purge succeeded, so the reaction is voided there —
    # but the live event is withheld.  The console history is still
    # but the live event is withheld.  The console history is still
    # written, through the record-only callback: messages() reads
    # only that history, so skipping it would leave the copy
    # invisible in the console even though it is in the inbox.
    # (CodeRabbit round-14 review, thread iB3I.)
    from tau.webapi import WebBridge

    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        published: list = []

        def _capture(record):
            message = bridge.record_inbound(record)
            if message is not None:
                published.append(message)

        def _capture_record_only(record):
            bridge.record_inbound(record, publish=False)

        node.on_message = _capture
        node.on_message_record_only = _capture_record_only
        author = RNS.Identity()
        author_hash = destination_hash(author).hex()
        reactor = RNS.Identity()
        members = ["ab" * 16, author_hash, destination_hash(RNS.Identity()).hex()]
        group = derive_group_id("ab" * 16, "crew", members)
        response = call_handler(
            node._handle_reaction,
            signed_reaction(reactor, "local-copy-1", "👍", "react-1"),
            remote_identity=reactor,
        )
        assert response["ok"] is True
        assert len(bridge._reaction_state) == 1

        def _raise(record):
            raise RuntimeError("console callback failed")

        monkeypatch.setattr(node, "on_reaction", _raise)
        delivered = call_handler(
            node._handle_group_message,
            encode_group_message(
                "crew hello",
                message_id="local-copy-1",
                group_id=group,
                group_name="crew",
                creator="ab" * 16,
                members=members,
                sent_at=100.0,
            ),
            remote_identity=author,
        )
        assert delivered["ok"] is True
        assert published == []
        assert len(node.inbox.list_messages()) == 1
        assert node.inbox.reactions_for("local-copy-1") == []
        # The live event was withheld, but the console history holds
        # the copy — with the stale reaction filtered out of the
        # stored entry — and no live message event was published.
        history = bridge.messages()
        assert len(history) == 1
        assert history[0]["wire_id"] == "local-copy-1"
        assert history[0]["reactions"] == []
        assert [
            event
            for event in bridge.bus.replay()
            if event.get("type") == "message"
            and event.get("message", {}).get("wire_id") == "local-copy-1"
        ] == []
    finally:
        bridge.stop()


def test_relayed_group_message_recorded_when_fan_out_fails(
    tmp_path, monkeypatch
):
    """The relayed path writes the console history too when a
    voided-reaction fan-out fails (CodeRabbit round-14 review,
    thread iB3I).

    Same ordering as the direct path, delivered through the fetch
    path instead: a non-member reaction is cached before its target
    arrives, the purge voids it, the tombstone fan-out fails, and
    the live event is withheld — but the copy is recorded and the
    console history holds it, and the delivery still resolves (the
    envelope is acked, not deferred).
    """
    import base64

    from tau.webapi import WebBridge

    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.identity = RNS.Identity()
    own = destination_hash(node.identity).hex()
    node.destination = SimpleNamespace(
        hexhash=own, hash=destination_hash(node.identity)
    )
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        def _capture_record_only(record):
            bridge.record_inbound(record, publish=False)

        node.on_message_record_only = _capture_record_only
        holder = RNS.Identity()
        holder_hash = destination_hash(holder)
        peer = Peer(
            destination_hash(holder_hash),
            name="holder",
            public_key=holder.get_public_key(),
            first_seen=1.0,
            last_seen=1.0,
            caps=("message", "ping", "peers", "mail", "read"),
        )
        node.registry._peers[holder_hash] = peer
        author = RNS.Identity()
        author_hash = destination_hash(author).hex()
        reactor = RNS.Identity()
        members = [own, author_hash, destination_hash(RNS.Identity()).hex()]
        group = derive_group_id(author_hash, "crew", members)
        response = call_handler(
            node._handle_reaction,
            signed_reaction(reactor, "fetch-group-1", "👍", "react-1"),
            remote_identity=reactor,
        )
        assert response["ok"] is True
        assert len(bridge._reaction_state) == 1

        def _raise(record):
            raise RuntimeError("console callback failed")

        monkeypatch.setattr(node, "on_reaction", _raise)
        blob = seal_message(
            author,
            sender_name="alice",
            recipient_public_key=node.identity.get_public_key(),
            message_id="fetch-group-1",
            text="crew hello",
            sent_at=100.0,
            group={
                "id": group,
                "name": "crew",
                "creator": author_hash,
                "members": members,
            },
        )
        node._request = lambda peer, path, payload, timeout, deadline=None: {
            "ok": True,
            "messages": [
                {
                    "id": "fetch-group-1",
                    "blob": base64.b64encode(blob).decode("ascii"),
                    "expires_at": time.time() + 3600,
                }
            ],
            "count": 1,
        }
        assert node._collect_mail(peer, timeout=5.0) == 1
        assert received == []
        assert len(node.inbox.list_messages()) == 1
        assert node.inbox.reactions_for("fetch-group-1") == []
        assert node.mailbox.take_pending_acks(peer.hexhash) == [
            "fetch-group-1"
        ]
        history = bridge.messages()
        assert len(history) == 1
        assert history[0]["wire_id"] == "fetch-group-1"
        assert history[0]["reactions"] == []
        assert [
            event
            for event in bridge.bus.replay()
            if event.get("type") == "message"
            and event.get("message", {}).get("wire_id") == "fetch-group-1"
        ] == []
    finally:
        bridge.stop()


def test_relayed_reconcile_failure_does_not_skip_purge(tmp_path, monkeypatch):
    # CodeRabbit follow-up: on the relayed path the reconcile and the
    # purge shared one try block, so a reconcile failure skipped the
    # purge and the message published with the reaction.  The reconcile
    # now runs inside Inbox.append_group_message and its failure is
    # tolerated there, so the purge still runs and the live entry
    # publishes clean.
    from tau.webapi import WebBridge

    node = make_node(tmp_path)
    recipient = RNS.Identity()
    node.identity = recipient
    node.destination = SimpleNamespace(
        hexhash=destination_hash(recipient).hex(),
        hash=destination_hash(recipient),
    )
    own = destination_hash(recipient).hex()
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        published: list = []

        def _capture(record):
            message = bridge.record_inbound(record)
            if message is not None:
                published.append(message)

        node.on_message = _capture
        author = RNS.Identity()
        author_hash = destination_hash(author).hex()
        reactor = RNS.Identity()
        members = [own, author_hash, destination_hash(RNS.Identity()).hex()]
        group = derive_group_id(author_hash, "crew", members)
        response = call_handler(
            node._handle_reaction,
            signed_reaction(reactor, "local-copy-1", "👍", "react-1"),
            remote_identity=reactor,
        )
        assert response["ok"] is True

        def _fail(*args, **kwargs):
            raise OSError("disk full")

        monkeypatch.setattr(
            node.inbox, "_reconcile_group_reactions_locked", _fail
        )
        blob = seal_message(
            author,
            sender_name="alice",
            recipient_public_key=recipient.get_public_key(),
            message_id="local-copy-1",
            text="crew hello",
            sent_at=100.0,
            group={
                "id": group,
                "name": "crew",
                "creator": author_hash,
                "members": members,
            },
        )
        message = open_message(recipient, blob)
        assert node._deliver_collected(message) is True
        assert node.inbox.reactions_for("local-copy-1") == []
        assert len(published) == 1
        assert published[0]["reactions"] == []
    finally:
        bridge.stop()


def test_local_held_group_delivery_keeps_copy_when_purge_fails(
    tmp_path, monkeypatch
):
    # CodeRabbit (node.py:3372): the local-held sweep used to drop
    # the copy before delivery, so a deferred delivery lost the only
    # copy of a message nobody recorded.  The copy stays held until
    # the delivery resolves, and a later sweep retries it.
    received = []
    node = make_node(tmp_path, on_message=received.append)
    identity = RNS.Identity()
    recipient_hash = destination_hash(identity)
    node.identity = identity
    node.destination = SimpleNamespace(
        hash=recipient_hash, hexhash=recipient_hash.hex()
    )
    own = recipient_hash.hex()
    sender = RNS.Identity()
    author_hash = destination_hash(sender).hex()
    members = [own, author_hash, destination_hash(RNS.Identity()).hex()]
    group = derive_group_id(author_hash, "crew", members)
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=identity.get_public_key(),
        message_id="self-group-1",
        text="crew hello",
        sent_at=100.0,
        group={
            "id": group,
            "name": "crew",
            "creator": author_hash,
            "members": members,
        },
    )
    node.mailbox.hold(
        to=recipient_hash,
        blob=blob,
        message_id="self-group-1",
        expires_at=time.time() + 3600,
    )

    def _fail(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(
        node.inbox, "_purge_non_member_reactions_locked", _fail
    )
    assert node._process_local_held() == 0
    assert received == []
    assert not node.mailbox.is_seen("self-group-1")
    held = node.mailbox.held_for(recipient_hash)
    assert [envelope.id for envelope in held] == ["self-group-1"]
    # Once the store is healthy, the next sweep delivers the kept copy.
    monkeypatch.undo()
    assert node._process_local_held() == 1
    assert len(received) == 1
    assert received[0]["text"] == "crew hello"
    assert node.mailbox.held_for(recipient_hash) == []


def test_collected_group_delivery_leaves_envelope_unacked_when_purge_fails(
    tmp_path, monkeypatch
):
    # CodeRabbit (node.py:3372): the fetch path used to ack an
    # envelope before delivery, so a deferred delivery let the
    # holder drop the only copy of a message nobody recorded.  The
    # envelope is acked only once the delivery resolves; a deferred
    # one stays unacknowledged and the next fetch retries it.
    import base64

    received = []
    node = make_node(tmp_path, on_message=received.append)
    node.identity = RNS.Identity()
    own = destination_hash(node.identity).hex()
    node.destination = SimpleNamespace(
        hexhash=own, hash=destination_hash(node.identity)
    )
    holder = RNS.Identity()
    holder_hash = destination_hash(holder)
    peer = Peer(
        destination_hash=holder_hash,
        name="holder",
        public_key=holder.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
        caps=("message", "ping", "peers", "mail", "read"),
    )
    node.registry._peers[holder_hash] = peer
    author = RNS.Identity()
    author_hash = destination_hash(author).hex()
    members = [own, author_hash, destination_hash(RNS.Identity()).hex()]
    group = derive_group_id(author_hash, "crew", members)
    blob = seal_message(
        author,
        sender_name="alice",
        recipient_public_key=node.identity.get_public_key(),
        message_id="fetch-group-1",
        text="crew hello",
        sent_at=100.0,
        group={
            "id": group,
            "name": "crew",
            "creator": author_hash,
            "members": members,
        },
    )
    node._request = lambda peer, path, payload, timeout, deadline=None: {
        "ok": True,
        "messages": [
            {
                "id": "fetch-group-1",
                "blob": base64.b64encode(blob).decode("ascii"),
                "expires_at": time.time() + 3600,
            }
        ],
        "count": 1,
    }

    def _fail(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(
        node.inbox, "_purge_non_member_reactions_locked", _fail
    )
    assert node._collect_mail(peer, timeout=5.0) == 0
    assert received == []
    assert node.mailbox.take_pending_acks(peer.hexhash) == []
    # The envelope was never acked, so the next fetch serves it
    # again; once the store is healthy it delivers and is acked.
    monkeypatch.undo()
    assert node._collect_mail(peer, timeout=5.0) == 1
    assert len(received) == 1
    assert received[0]["text"] == "crew hello"
    assert node.mailbox.take_pending_acks(peer.hexhash) == ["fetch-group-1"]


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


def signed_retract(
    sender,
    target,
    message_id,
    *,
    sent_at=None,
    group=None,
    target_author=None,
    target_sent_at=None,
):
    """A retract payload signed by *sender*, as send_retraction builds it."""
    import base64

    payload = retract_dict(
        target,
        message_id=message_id,
        sent_at=sent_at,
        group=group,
        target_author=target_author,
        target_sent_at=target_sent_at,
    )
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


def test_retract_deletes_the_senders_own_group_message(tmp_path):
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    members = ["ab" * 16, sender_hash, destination_hash(RNS.Identity()).hex()]
    group = _pinned_group(node, members=members, creator="ab" * 16)
    _seed_group_copy(
        node, group=group, author=sender_hash, sent_at=100.0, message_id="msg-1"
    )

    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "msg-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["target"] == "msg-1"
    assert response["deleted"] is True
    assert node.inbox.list_messages() == []
    # The id marker guards a relayed copy arriving later.
    assert node.mailbox.is_retracted(sender_hash, "msg-1") is True


def test_retract_deletes_group_message_after_rename(tmp_path):
    # Regression test (CodeRabbit Major on PR #327): the recipient
    # processed a rename, so the original group left the active store
    # — but its roster is retained, and the sender's retract must
    # still authorize against that roster instead of answering
    # not_a_member.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    members = ["ab" * 16, sender_hash, destination_hash(RNS.Identity()).hex()]
    group = _pinned_group(node, members=members, creator="ab" * 16)
    _seed_group_copy(
        node, group=group, author=sender_hash, sent_at=100.0, message_id="msg-1"
    )
    renamed_id = derive_group_id("ab" * 16, "road trip", members)
    assert (
        node.groups.record(
            renamed_id,
            "road trip",
            members,
            creator="ab" * 16,
            sender="ab" * 16,
            renamed_from=[group],
        )
        is not None
    )
    assert node.groups.get(group) is None
    assert node.groups.get_retired(group) is not None

    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "msg-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["deleted"] is True
    assert node.inbox.list_messages() == []


def test_retract_still_refuses_non_member_after_rename(tmp_path):
    # The retained roster authorizes the members it names — and no
    # one else.  With no stored copy, the group-context path checks
    # membership before recording a marker: an outsider still gets
    # not_a_member after a rename, and no marker is recorded for them.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    outsider = RNS.Identity()
    members = ["ab" * 16, sender_hash, destination_hash(RNS.Identity()).hex()]
    group = _pinned_group(node, members=members, creator="ab" * 16)
    renamed_id = derive_group_id("ab" * 16, "road trip", members)
    assert (
        node.groups.record(
            renamed_id,
            "road trip",
            members,
            creator="ab" * 16,
            sender="ab" * 16,
            renamed_from=[group],
        )
        is not None
    )
    assert node.groups.get_retired(group) is not None

    response = call_handler(
        node._handle_retract,
        signed_retract(
            outsider,
            "author-leg-1",
            "retract-1",
            group=group,
            target_author=sender_hash,
            target_sent_at=100.0,
        ),
        remote_identity=outsider,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_a_member"
    assert (
        node.mailbox.is_group_retracted(
            destination_hash(outsider).hex(), group, 100.0
        )
        is False
    )


def test_retract_resolves_group_copy_by_context(tmp_path):
    # Group sends carry a different wire id per leg: the retract names
    # the author's leg id, not this node's copy.  The signed group
    # context resolves this node's own copy instead.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    members = ["ab" * 16, sender_hash, destination_hash(RNS.Identity()).hex()]
    group = _pinned_group(node, members=members, creator="ab" * 16)
    _seed_group_copy(
        node,
        group=group,
        author=sender_hash,
        sent_at=100.0,
        message_id="local-leg-9",
    )

    response = call_handler(
        node._handle_retract,
        signed_retract(
            sender,
            "author-leg-1",
            "retract-1",
            group=group,
            target_author=sender_hash,
            target_sent_at=100.0,
        ),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["deleted"] is True
    assert node.inbox.list_messages() == []
    # The group marker guards a copy arriving later under any leg id.
    assert node.mailbox.is_group_retracted(sender_hash, group, 100.0) is True


def test_retract_response_reports_group_retract_after_deleting_copy(tmp_path):
    # Regression test (CodeRabbit Major on PR #322): the successful
    # deletion response must carry group_retract, or the sender's
    # fan-out reports the member as unsupported and keeps its own
    # copy until a second attempt.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    members = ["ab" * 16, sender_hash, destination_hash(RNS.Identity()).hex()]
    group = _pinned_group(node, members=members, creator="ab" * 16)
    _seed_group_copy(
        node, group=group, author=sender_hash, sent_at=100.0, message_id="msg-1"
    )

    response = call_handler(
        node._handle_retract,
        signed_retract(
            sender,
            "author-leg-1",
            "retract-1",
            group=group,
            target_author=sender_hash,
            target_sent_at=100.0,
        ),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["deleted"] is True
    assert response["group_retract"] is True


def test_retract_records_group_marker_for_unknown_group_copy(tmp_path):
    # No copy held yet: the retract still records the group-context
    # marker, and a copy arriving later loses to it instead of
    # showing a message that was already retracted.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    members = ["ab" * 16, sender_hash, destination_hash(RNS.Identity()).hex()]
    group = _pinned_group(node, members=members, creator="ab" * 16)

    response = call_handler(
        node._handle_retract,
        signed_retract(
            sender,
            "author-leg-1",
            "retract-1",
            group=group,
            target_author=sender_hash,
            target_sent_at=100.0,
        ),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["deleted"] is False
    assert node.mailbox.is_group_retracted(sender_hash, group, 100.0) is True

    late = call_handler(
        node._handle_group_message,
        encode_group_message(
            "crew hello",
            message_id="late-leg-5",
            group_id=group,
            group_name="crew",
            creator="ab" * 16,
            members=members,
            sent_at=100.0,
        ),
        remote_identity=sender,
    )
    assert late["ok"] is True
    assert late["duplicate"] is True
    assert node.inbox.list_messages() == []


def test_retract_records_group_marker_without_a_pinned_roster(tmp_path):
    # Regression test: a member who has not received any group
    # traffic yet has no pinned roster — their retract must still
    # record the marker, or the copy arriving later would show a
    # message that was already retracted.  (A Qodo autofix refused
    # this case; the roster check must only refuse when a roster
    # exists and excludes the caller.)
    from tau.protocol import derive_group_id

    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    members = ["ab" * 16, sender_hash, destination_hash(RNS.Identity()).hex()]
    group = derive_group_id("ab" * 16, "crew", members)

    response = call_handler(
        node._handle_retract,
        signed_retract(
            sender,
            "author-leg-1",
            "retract-1",
            group=group,
            target_author=sender_hash,
            target_sent_at=100.0,
        ),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["deleted"] is False
    assert node.mailbox.is_group_retracted(sender_hash, group, 100.0) is True

    late = call_handler(
        node._handle_group_message,
        encode_group_message(
            "crew hello",
            message_id="late-leg-5",
            group_id=group,
            group_name="crew",
            creator="ab" * 16,
            members=members,
            sent_at=100.0,
        ),
        remote_identity=sender,
    )
    assert late["ok"] is True
    assert late["duplicate"] is True
    assert node.inbox.list_messages() == []


def test_unknown_group_retracts_do_not_evict_confirmed_marker(
    tmp_path, monkeypatch
):
    # Regression test (CodeRabbit Major on PR #322): retracts for
    # groups the node has no roster for land in the unknown pool, so
    # flooding them cannot evict a confirmed marker — a late copy of
    # the confirmed retract's message still loses to it.
    monkeypatch.setattr(mailbox_module, "MAX_GROUP_RETRACTED", 2)
    from tau.protocol import derive_group_id

    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    members = ["ab" * 16, sender_hash, destination_hash(RNS.Identity()).hex()]
    group = _pinned_group(node, members=members, creator="ab" * 16)
    _seed_group_copy(
        node, group=group, author=sender_hash, sent_at=100.0, message_id="msg-1"
    )

    # Confirmed marker: retract the stored copy.
    response = call_handler(
        node._handle_retract,
        signed_retract(
            sender,
            "author-leg-1",
            "retract-1",
            group=group,
            target_author=sender_hash,
            target_sent_at=100.0,
        ),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["deleted"] is True

    # Flood with unknown-group retracts from another sender.
    other = RNS.Identity()
    other_hash = destination_hash(other).hex()
    for index, name in enumerate(("g-one", "g-two")):
        other_group = derive_group_id("ab" * 16, name, members)
        flood = call_handler(
            node._handle_retract,
            signed_retract(
                other,
                f"leg-{index}",
                f"retract-{index}",
                group=other_group,
                target_author=other_hash,
                target_sent_at=200.0 + index,
            ),
            remote_identity=other,
        )
        assert flood["ok"] is True
        assert flood["deleted"] is False

    # The confirmed marker survived the flood: a late copy still
    # loses to it.
    assert node.mailbox.is_group_retracted(sender_hash, group, 100.0) is True
    late = call_handler(
        node._handle_group_message,
        encode_group_message(
            "crew hello",
            message_id="late-leg-5",
            group_id=group,
            group_name="crew",
            creator="ab" * 16,
            members=members,
            sent_at=100.0,
        ),
        remote_identity=sender,
    )
    assert late["ok"] is True
    assert late["duplicate"] is True
    assert node.inbox.list_messages() == []


def test_retract_refuses_group_marker_from_non_member(tmp_path):
    # The roster is pinned and does not name the caller: no marker
    # is recorded, so an allow-listed outsider cannot flood the
    # bounded group-marker store.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    members = [
        "ab" * 16,
        destination_hash(RNS.Identity()).hex(),
        destination_hash(RNS.Identity()).hex(),
    ]
    group = _pinned_group(node, members=members, creator="ab" * 16)

    response = call_handler(
        node._handle_retract,
        signed_retract(
            sender,
            "author-leg-1",
            "retract-1",
            group=group,
            target_author=sender_hash,
            target_sent_at=100.0,
        ),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_a_member"
    assert node.mailbox.is_group_retracted(sender_hash, group, 100.0) is False


def test_retract_refuses_group_message_from_non_author(tmp_path):
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    other = RNS.Identity()
    members = [
        "ab" * 16,
        sender_hash,
        destination_hash(other).hex(),
    ]
    group = _pinned_group(node, members=members, creator="ab" * 16)
    _seed_group_copy(
        node, group=group, author=sender_hash, sent_at=100.0, message_id="msg-1"
    )

    response = call_handler(
        node._handle_retract,
        signed_retract(other, "msg-1", "retract-1"),
        remote_identity=other,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_sender"
    assert len(node.inbox.list_messages()) == 1


def test_retract_refuses_group_message_from_non_member(tmp_path):
    # The author left the group after the message arrived: the roster
    # no longer names them, so the retract fails closed — exactly as
    # a group reaction from a non-member does.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    sender_hash = destination_hash(sender).hex()
    members = [
        "ab" * 16,
        destination_hash(RNS.Identity()).hex(),
        destination_hash(RNS.Identity()).hex(),
    ]
    group = _pinned_group(node, members=members, creator="ab" * 16)
    _seed_group_copy(
        node, group=group, author=sender_hash, sent_at=100.0, message_id="msg-1"
    )

    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "msg-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_a_member"
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


def test_concurrent_retracts_cannot_delete_a_different_record(tmp_path):
    # Regression test for the CodeRabbit Major TOCTOU: two concurrent
    # retracts of one short id must not let the loser reselect another
    # record by prefix — the node's own outgoing copy shares the id
    # prefix and must survive.
    import threading

    node = make_node(tmp_path)
    sender = RNS.Identity()
    _store_received_message(node, sender, "abc")
    node.inbox.append(
        {
            "from": "holder",
            "destination": "cd" * 16,
            "text": "my sent copy",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "abcdef" + "1" * 26,
            "direction": "out",
        }
    )

    barrier = threading.Barrier(2)
    responses = []

    def _retract(suffix):
        barrier.wait(timeout=10)
        responses.append(
            call_handler(
                node._handle_retract,
                signed_retract(sender, "abc", f"retract-{suffix}"),
                remote_identity=sender,
            )
        )

    threads = [
        threading.Thread(target=_retract, args=(1,)),
        threading.Thread(target=_retract, args=(2,)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert not any(thread.is_alive() for thread in threads)
    assert [response["ok"] for response in responses] == [True, True]
    assert sorted(response["deleted"] for response in responses) == [False, True]
    # The short id is gone; the longer outgoing record sharing its
    # prefix is untouched.
    assert [entry["id"] for entry in node.inbox.list_messages(direction=None)] == [
        "abcdef" + "1" * 26
    ]


def test_retract_notifies_the_console_callback(tmp_path):
    # A serving console keeps its own copy of the message; the node
    # tells it the wire id so it can drop that copy too.
    node = make_node(tmp_path)
    sender = RNS.Identity()
    _store_received_message(node, sender, "msg-1")
    notified = []
    node.on_retract = notified.append

    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "msg-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["deleted"] is True
    assert [record["id"] for record in notified] == ["msg-1"]


def test_retract_callback_failure_does_not_fail_the_retract(tmp_path):
    # The inbox delete already landed; a failing console callback
    # must not turn the retraction into an error.
    node = make_node(tmp_path)
    sender = RNS.Identity()
    _store_received_message(node, sender, "msg-1")

    def _boom(record):
        raise RuntimeError("console is down")

    node.on_retract = _boom
    response = call_handler(
        node._handle_retract,
        signed_retract(sender, "msg-1", "retract-1"),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["deleted"] is True
    assert node.inbox.list_messages() == []


def test_concurrent_deliveries_store_only_one_copy(tmp_path):
    # Regression test for the CodeRabbit Minor: two deliveries of the
    # same id that both pass the pre-check must still store once —
    # the in-transaction re-check sees the first delivery's mark.
    import threading

    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender = RNS.Identity()
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    payload = encode_message("take this back", message_id="dup-1")

    barrier = threading.Barrier(2)

    def _deliver():
        barrier.wait(timeout=10)
        call_handler(node._handle_message, payload, remote_identity=sender)

    threads = [threading.Thread(target=_deliver) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert not any(thread.is_alive() for thread in threads)
    assert len(node.inbox.list_messages()) == 1
    assert len(received) == 1


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


def signed_edit(
    sender,
    target,
    text,
    message_id,
    *,
    sent_at=None,
    group=None,
    target_author=None,
    target_sent_at=None,
):
    """An edit payload signed by *sender*, as send_edit builds it."""
    import base64

    from tau.protocol import edit_dict

    payload = edit_dict(
        target,
        text,
        message_id=message_id,
        sent_at=sent_at,
        group=group,
        target_author=target_author,
        target_sent_at=target_sent_at,
    )
    signature = sender.sign(encode(payload))
    payload["sig"] = base64.b64encode(signature).decode("ascii")
    return encode(payload)


def _seed_inbound(node, sender, *, message_id="msg-1", text="typo here"):
    node.inbox.append(
        {
            "from": "alice",
            "destination": destination_hash(sender).hex(),
            "text": text,
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": message_id,
        }
    )


def test_edit_applies_the_senders_own_edit(tmp_path):
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    _seed_inbound(node, sender)
    response = call_handler(
        node._handle_dm_edit,
        signed_edit(sender, "msg-1", "fixed here", "edit-1", sent_at=30.0),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["edited"] is True
    assert response["target"] == "msg-1"
    entry = node.inbox.list_messages()[0]
    assert entry["text"] == "fixed here"
    assert entry["edited_at"] == 30.0


def test_edit_notifies_the_console_callback(tmp_path):
    # A serving console keeps its own copy of the message: the node
    # tells it the new text so the console shows the edit too.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    _seed_inbound(node, sender)
    seen = []
    node.on_edit = seen.append
    response = call_handler(
        node._handle_dm_edit,
        signed_edit(sender, "msg-1", "fixed here", "edit-1", sent_at=30.0),
        remote_identity=sender,
    )
    assert response["edited"] is True
    assert len(seen) == 1
    assert seen[0]["target"] == "msg-1"
    assert seen[0]["text"] == "fixed here"
    assert seen[0]["edited_at"] == 30.0
    assert seen[0]["destination"] == destination_hash(sender).hex()


def test_edit_unknown_id_answers_ok_without_editing(tmp_path):
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    response = call_handler(
        node._handle_dm_edit,
        signed_edit(sender, "missing", "fixed", "edit-1", sent_at=30.0),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["edited"] is False
    assert len(node.inbox) == 0


def test_edit_reports_not_applied_when_the_record_vanished(
    tmp_path, monkeypatch
):
    # A concurrent delete or retract can remove the record between the
    # lookup and the edit: the handler must report not applied instead
    # of acknowledging an edit that landed nowhere.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    _seed_inbound(node, sender)
    monkeypatch.setattr(node.inbox, "apply_edit", lambda *args, **kwargs: None)
    response = call_handler(
        node._handle_dm_edit,
        signed_edit(sender, "msg-1", "fixed", "edit-1", sent_at=30.0),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["edited"] is False


def test_edit_rejects_a_non_sender(tmp_path):
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    other = RNS.Identity()
    _seed_inbound(node, sender)
    response = call_handler(
        node._handle_dm_edit,
        signed_edit(other, "msg-1", "forged", "edit-1", sent_at=30.0),
        remote_identity=other,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_sender"
    assert node.inbox.list_messages()[0]["text"] == "typo here"


def test_edit_rejects_an_outbound_record(tmp_path):
    # The record is this node's own sent message: its `destination` is
    # the recipient, so a destination-only check would wrongly admit the
    # recipient as its editor.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    recipient = RNS.Identity()
    node.inbox.append(
        {
            "from": "holder",
            "destination": destination_hash(recipient).hex(),
            "text": "my message",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "msg-1",
            "direction": "out",
        }
    )
    response = call_handler(
        node._handle_dm_edit,
        signed_edit(recipient, "msg-1", "forged", "edit-1", sent_at=30.0),
        remote_identity=recipient,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_sender"
    assert node.inbox.list_messages(direction="out")[0]["text"] == "my message"


def test_edit_ignores_a_stale_edit(tmp_path):
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    _seed_inbound(node, sender)
    first = call_handler(
        node._handle_dm_edit,
        signed_edit(sender, "msg-1", "newer text", "edit-1", sent_at=30.0),
        remote_identity=sender,
    )
    assert first["edited"] is True
    stale = call_handler(
        node._handle_dm_edit,
        signed_edit(sender, "msg-1", "older text", "edit-2", sent_at=25.0),
        remote_identity=sender,
    )
    assert stale["ok"] is True
    assert stale["edited"] is False
    assert node.inbox.list_messages()[0]["text"] == "newer text"
    assert node.inbox.list_messages()[0]["edited_at"] == 30.0


def test_edit_rejects_a_signature_from_another_identity(tmp_path):
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    signer = RNS.Identity()
    presenter = RNS.Identity()
    _seed_inbound(node, signer)
    response = call_handler(
        node._handle_dm_edit,
        signed_edit(signer, "msg-1", "fixed", "edit-1", sent_at=30.0),
        remote_identity=presenter,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_edit"
    assert node.inbox.list_messages()[0]["text"] == "typo here"


def test_edit_rejects_a_tampered_payload(tmp_path):
    import base64

    from tau.protocol import edit_dict

    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    _seed_inbound(node, sender)
    payload = edit_dict("msg-1", "fixed", message_id="edit-1")
    signature = sender.sign(encode(payload))
    payload["sig"] = base64.b64encode(signature).decode("ascii")
    payload["text"] = "tampered"  # tampered after signing
    response = call_handler(
        node._handle_dm_edit, encode(payload), remote_identity=sender
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_edit"
    assert node.inbox.list_messages()[0]["text"] == "typo here"


def test_edit_rejects_invalid_payload(tmp_path):
    node = make_node(tmp_path)
    response = call_handler(node._handle_dm_edit, b"not json")
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_edit"


def test_edit_requires_a_signature(tmp_path):
    from tau.protocol import encode_edit

    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    _seed_inbound(node, sender)
    response = call_handler(
        node._handle_dm_edit,
        encode_edit("msg-1", "fixed", message_id="edit-1"),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "invalid_edit"
    assert "signature" in response["error"]["message"]
    assert node.inbox.list_messages()[0]["text"] == "typo here"


def test_edit_requires_an_identified_sender(tmp_path):
    node = make_node(tmp_path)
    sender = RNS.Identity()
    _seed_inbound(node, sender)
    response = call_handler(
        node._handle_dm_edit,
        signed_edit(sender, "msg-1", "fixed", "edit-1", sent_at=30.0),
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "unidentified"
    assert node.inbox.list_messages()[0]["text"] == "typo here"


def test_edit_refuses_a_group_message(tmp_path):
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    node.inbox.append(
        {
            "from": "alice",
            "destination": destination_hash(sender).hex(),
            "text": "group hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "msg-1",
            "kind": "group",
            "group": "c" * 32,
            "group_name": "weekend",
        }
    )
    response = call_handler(
        node._handle_dm_edit,
        signed_edit(sender, "msg-1", "fixed", "edit-1", sent_at=30.0),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "group_message"
    assert node.inbox.list_messages()[0]["text"] == "group hello"


def _seed_group_leg(
    node, sender, *, leg_id="leg-9", text="group typo", members=None,
    created_by=None,
):
    # The group record pins the roster the edit handler checks the
    # editor against, like a group retract: the sender must be a
    # current member.  `members` / `created_by` override the roster,
    # so an edit from a sender the roster does not name can be tested.
    own_hash = "ab" * 16  # node.destination hexhash in these tests
    if members is None:
        members = [destination_hash(sender).hex(), own_hash]
    if created_by is None:
        created_by = destination_hash(sender).hex()
    group = node.groups.create(
        "weekend",
        members,
        created_by=created_by,
    )
    node.inbox.append(
        {
            "from": "alice",
            "destination": destination_hash(sender).hex(),
            "text": text,
            "sent_at": 100.0,
            "received_at": 20.0,
            "id": leg_id,
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
        }
    )
    return group


def _signed_group_edit(
    sender, target, text, message_id, *, sent_at=30.0, author=None, group=None
):
    return signed_edit(
        sender,
        target,
        text,
        message_id,
        sent_at=sent_at,
        group=group or "c" * 32,
        target_author=destination_hash(author or sender).hex(),
        target_sent_at=100.0,
    )


def test_edit_applies_a_group_edit_via_group_context(tmp_path):
    # Legs carry different wire ids per member, so the edit names the
    # sender's record id — which this member does not hold — and the
    # signed group context resolves this member's own leg instead.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    group = _seed_group_leg(node, sender)
    seen = []
    node.on_edit = seen.append
    response = call_handler(
        node._handle_dm_edit,
        _signed_group_edit(
            sender, "leg-1", "group fixed", "edit-1", group=group["id"]
        ),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["edited"] is True
    assert response["group_edit"] is True
    assert response["target"] == "leg-9"
    entry = node.inbox.list_messages()[0]
    assert entry["text"] == "group fixed"
    assert entry["edited_at"] == 30.0
    # The console callback names this member's own leg id, so the
    # console rewrites the bubble it actually shows.
    assert len(seen) == 1
    assert seen[0]["target"] == "leg-9"
    assert seen[0]["text"] == "group fixed"


def test_edit_group_unknown_copy_answers_ok_without_editing(tmp_path):
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    response = call_handler(
        node._handle_dm_edit,
        _signed_group_edit(sender, "leg-1", "group fixed", "edit-1"),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["edited"] is False
    assert response["group_edit"] is True


def test_edit_group_ignores_a_stale_edit(tmp_path):
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    group = _seed_group_leg(node, sender)
    first = call_handler(
        node._handle_dm_edit,
        _signed_group_edit(
            sender, "leg-1", "newer text", "edit-1", sent_at=40.0, group=group["id"]
        ),
        remote_identity=sender,
    )
    assert first["edited"] is True
    second = call_handler(
        node._handle_dm_edit,
        _signed_group_edit(
            sender, "leg-1", "older text", "edit-2", sent_at=30.0, group=group["id"]
        ),
        remote_identity=sender,
    )
    assert second["ok"] is True
    assert second["edited"] is False
    assert second["group_edit"] is True
    assert node.inbox.list_messages()[0]["text"] == "newer text"


def test_edit_group_rejects_a_non_sender(tmp_path):
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    other = RNS.Identity()
    group = _seed_group_leg(node, sender)
    # The edit is signed by `other` but names the real sender as the
    # group author, so the context resolves the leg — and the author
    # check must still refuse it.
    response = call_handler(
        node._handle_dm_edit,
        _signed_group_edit(
            other, "leg-1", "group fixed", "edit-1", author=sender, group=group["id"]
        ),
        remote_identity=other,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_sender"
    assert node.inbox.list_messages()[0]["text"] == "group typo"


def test_edit_group_rejects_a_removed_member(tmp_path):
    # Like a group retract, an edit must come from a current member: a
    # sender the pinned roster does not name — removed, or never a
    # member — may not rewrite a retained copy.
    node = make_node(tmp_path)
    node.destination = SimpleNamespace(
        hexhash="ab" * 16, hash=b"\xab" * 16
    )
    sender = RNS.Identity()
    other = RNS.Identity()
    group = _seed_group_leg(
        node,
        sender,
        members=[destination_hash(other).hex(), "ab" * 16],
        created_by=destination_hash(other).hex(),
    )
    response = call_handler(
        node._handle_dm_edit,
        _signed_group_edit(
            sender, "leg-1", "group fixed", "edit-1", group=group["id"]
        ),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_a_member"
    assert node.inbox.list_messages()[0]["text"] == "group typo"


def test_edit_denied_by_allow_list(tmp_path):
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
    _seed_inbound(node, sender)
    response = call_handler(
        node._handle_dm_edit,
        signed_edit(sender, "msg-1", "fixed", "edit-1", sent_at=30.0),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_allowed"
    assert node.inbox.list_messages()[0]["text"] == "typo here"


def test_send_edit_validates_before_resolving(tmp_path):
    node = make_node(tmp_path)
    with pytest.raises(ProtocolError):
        node.send_edit("alice", "msg-1", "   ")
    with pytest.raises(ProtocolError):
        node.send_edit("alice", "msg-1", "x" * 2001)
    with pytest.raises(NodeError):
        node.send_edit("alice", "", "fixed")


def test_send_edit_sends_a_signed_payload_and_updates_the_local_copy(
    tmp_path, monkeypatch
):
    from tau.protocol import parse_edit

    node = make_node(tmp_path)
    sender_identity = RNS.Identity()
    node.identity = sender_identity
    node.destination = SimpleNamespace(
        hexhash=destination_hash(sender_identity).hex(),
        hash=destination_hash(sender_identity),
    )
    peer_identity = RNS.Identity()
    peer = node.registry.upsert(
        destination_hash(peer_identity),
        peer_identity.get_public_key(),
        name="alice",
        now=1.0,
    )
    node.inbox.append(
        {
            "from": "holder",
            "destination": peer.hexhash,
            "text": "typo here",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "msg-1",
            "direction": "out",
        }
    )
    sent = {}

    def fake_request(peer_arg, path, payload, timeout, **kwargs):
        sent["peer"] = peer_arg
        sent["path"] = path
        sent["payload"] = payload
        return {"ok": True, "edited": True, "target": "msg-1"}

    monkeypatch.setattr(node, "_request", fake_request)
    response = node.send_edit("alice", "msg-1", "fixed here")

    assert response["ok"] is True
    assert response["edited"] is True
    assert response["id"]
    assert sent["path"] == "/tau/dm/edit"
    assert sent["peer"].hexhash == peer.hexhash
    parsed = parse_edit(sent["payload"])
    assert parsed["target"] == "msg-1"
    assert parsed["text"] == "fixed here"
    assert parsed["id"] == response["id"]
    assert parsed["sig"]
    entry = node.inbox.list_messages(direction="out")[0]
    assert entry["text"] == "fixed here"
    assert entry["edited_at"]


def test_send_edit_leaves_the_local_copy_when_the_receiver_did_not_apply(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    node.identity = RNS.Identity()
    peer_identity = RNS.Identity()
    peer = node.registry.upsert(
        destination_hash(peer_identity),
        peer_identity.get_public_key(),
        name="alice",
        now=1.0,
    )
    node.inbox.append(
        {
            "from": "holder",
            "destination": peer.hexhash,
            "text": "typo here",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "msg-1",
            "direction": "out",
        }
    )
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: {"ok": True, "edited": False, "target": "msg-1"},
    )
    response = node.send_edit("alice", "msg-1", "fixed here")

    assert response["edited"] is False
    assert node.inbox.list_messages(direction="out")[0]["text"] == "typo here"
    assert "edited_at" not in node.inbox.list_messages(direction="out")[0]


def test_send_edit_updates_the_sent_record(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    node.identity = RNS.Identity()
    peer_identity = RNS.Identity()
    peer = node.registry.upsert(
        destination_hash(peer_identity),
        peer_identity.get_public_key(),
        name="alice",
        now=1.0,
    )
    node.inbox.append(
        {
            "from": "holder",
            "destination": peer.hexhash,
            "text": "typo here",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "msg-1",
            "direction": "out",
        }
    )
    node.mailbox.record_sent(
        "msg-1", to=destination_hash(peer_identity), text="typo here"
    )
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: {"ok": True, "edited": True, "target": "msg-1"},
    )
    response = node.send_edit("alice", "msg-1", "fixed here")

    assert response["edited"] is True
    # ``tau messages --sent`` reads the mailbox, not the inbox: the sent
    # record must carry the new text too.
    sent = node.mailbox.sent_record("msg-1")
    assert sent is not None
    assert sent["text"] == "fixed here"
    assert sent["edited_at"]


def test_send_edit_leaves_the_sent_record_when_the_receiver_did_not_apply(
    tmp_path, monkeypatch
):
    node = make_node(tmp_path)
    node.identity = RNS.Identity()
    peer_identity = RNS.Identity()
    peer = node.registry.upsert(
        destination_hash(peer_identity),
        peer_identity.get_public_key(),
        name="alice",
        now=1.0,
    )
    node.inbox.append(
        {
            "from": "holder",
            "destination": peer.hexhash,
            "text": "typo here",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "msg-1",
            "direction": "out",
        }
    )
    node.mailbox.record_sent(
        "msg-1", to=destination_hash(peer_identity), text="typo here"
    )
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: {"ok": True, "edited": False, "target": "msg-1"},
    )
    response = node.send_edit("alice", "msg-1", "fixed here")

    assert response["edited"] is False
    sent = node.mailbox.sent_record("msg-1")
    assert sent is not None
    assert sent["text"] == "typo here"
    assert "edited_at" not in sent


def test_send_edit_keeps_a_newer_local_edit(tmp_path, monkeypatch):
    node = make_node(tmp_path)
    node.identity = RNS.Identity()
    peer_identity = RNS.Identity()
    peer = node.registry.upsert(
        destination_hash(peer_identity),
        peer_identity.get_public_key(),
        name="alice",
        now=1.0,
    )
    # The local copy already holds a newer edit than the one this send
    # is delivering: its write must not rewind it, even though the
    # receiver applied this (older) edit.
    node.inbox.append(
        {
            "from": "holder",
            "destination": peer.hexhash,
            "text": "newest text",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "msg-1",
            "direction": "out",
            "edited_at": 9999999999.0,
        }
    )
    monkeypatch.setattr(
        node,
        "_request",
        lambda *args, **kwargs: {"ok": True, "edited": True, "target": "msg-1"},
    )
    response = node.send_edit("alice", "msg-1", "older edit")

    assert response["edited"] is True
    entry = node.inbox.list_messages(direction="out")[0]
    assert entry["text"] == "newest text"
    assert entry["edited_at"] == 9999999999.0


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
