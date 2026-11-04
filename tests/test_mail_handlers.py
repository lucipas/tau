"""Node-level store-and-forward behaviour that needs no live network."""

from __future__ import annotations

import time
from types import SimpleNamespace

import RNS

from tau.config import TauConfig
from tau.identity import destination_hash
from tau.mailbox import open_message, seal_message
from tau.node import TauNode
from tau.peers import PeerRegistry
from tau.protocol import (
    build_mail_fetch,
    build_mail_store,
    decode,
    encode_message,
)


def make_node(tmp_path, name="holder", on_message=None):
    config = TauConfig.resolve(data_dir=tmp_path, name=name)
    node = TauNode(
        config,
        registry=PeerRegistry(config.peers_path),
        on_message=on_message,
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


def test_flush_outbox_is_a_noop_until_started(tmp_path):
    node = make_node(tmp_path)
    node.mailbox.enqueue(to=b"\x01" * 16, text="waiting")
    assert node.flush_outbox() == {"delivered": 0, "deposited": 0, "received": 0}
