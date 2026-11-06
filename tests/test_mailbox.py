from __future__ import annotations

import base64
import json
import time

import RNS
import pytest

import tau.mailbox as mailbox_module
from tau.config import TauConfig
from tau.identity import destination_hash
from tau.mailbox import (
    Mailbox,
    MailboxError,
    MailboxFull,
    open_message,
    seal_message,
)
from tau.protocol import (
    MAX_MAIL_BLOB_BYTES,
    ProtocolError,
    build_mail_fetch,
    build_mail_store,
    encode_message,
    parse_mail_fetch,
    parse_mail_store,
    parse_message,
)

ALICE = b"\xaa" * 16
BOB = b"\xbb" * 16


# -- outbox ------------------------------------------------------------------


def test_enqueue_and_reload_roundtrip(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    entry = box.enqueue(to=BOB, text="hello bob", to_name="bob", now=1000.0)
    assert entry.to == BOB
    assert box.counts() == {"outbox": 1, "held": 0, "seen": 0}

    reloaded = Mailbox(path)
    entries = reloaded.outbox_list()
    assert len(entries) == 1
    assert entries[0].id == entry.id
    assert entries[0].text == "hello bob"
    assert entries[0].to_name == "bob"
    assert entries[0].created_at == 1000.0


def test_enqueue_dedupes_by_message_id(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    first = box.enqueue(to=BOB, text="once", message_id="fixed-id")
    second = box.enqueue(to=BOB, text="once", message_id="fixed-id")
    # Equal, not identical: enqueue now reloads state under the
    # cross-process transaction, so the deduped entry is a fresh copy.
    assert first == second
    assert box.counts()["outbox"] == 1


def test_enqueue_rejects_bad_destination(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    with pytest.raises(MailboxError):
        box.enqueue(to=b"\x00" * 4, text="nope")


def test_outbox_full(tmp_path, monkeypatch):
    monkeypatch.setattr(mailbox_module, "MAX_OUTBOX", 2)
    box = Mailbox(tmp_path / "mailbox.json")
    box.enqueue(to=BOB, text="one")
    box.enqueue(to=BOB, text="two")
    with pytest.raises(MailboxFull):
        box.enqueue(to=BOB, text="three")


def test_attempts_and_deposits_are_recorded(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(to=BOB, text="track me", now=10.0)
    box.record_attempt(entry.id, error="no path", now=20.0)
    box.record_deposit(entry.id, "cc" * 16, now=30.0)
    stored = box.get_outbox(entry.id)
    assert stored.attempts == 1
    assert stored.last_attempt_at == 20.0
    assert stored.last_error == "no path"
    assert stored.held_by == {"cc" * 16: 30.0}
    assert box.remove_outbox(entry.id) is True
    assert box.remove_outbox(entry.id) is False


def test_delivery_backoff_schedule(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(to=BOB, text="back off", now=1000.0)
    assert entry.next_attempt_at is None
    assert entry.is_due(now=1000.0) is True
    box.record_attempt(entry.id, error="no path", now=1000.0)
    stored = box.get_outbox(entry.id)
    assert stored.attempts == 1
    assert stored.next_attempt_at == 1060.0
    assert stored.is_due(now=1059.0) is False
    assert stored.is_due(now=1060.0) is True
    # Delay doubles after each failure, capped at an hour.
    for attempt, delay in (
        (2, 120.0),
        (3, 240.0),
        (4, 480.0),
        (5, 960.0),
        (6, 1920.0),
        (7, 3600.0),
        (8, 3600.0),
    ):
        box.record_attempt(entry.id, error="no path", now=2000.0)
        stored = box.get_outbox(entry.id)
        assert stored.attempts == attempt
        assert stored.next_attempt_at == 2000.0 + delay
    assert stored.to_dict()["next_attempt_at"] == 2000.0 + 3600.0


def test_due_outbox_filters_backed_off_entries(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    due = box.enqueue(to=BOB, text="due", now=1000.0)
    waiting = box.enqueue(to=BOB, text="waiting", now=1000.0)
    box.record_attempt(waiting.id, error="no path", now=1000.0)
    assert [e.id for e in box.due_outbox(now=1010.0)] == [due.id]
    assert [e.id for e in box.due_outbox(now=1060.0)] == [due.id, waiting.id]


# -- held envelopes ------------------------------------------------------------


def test_hold_dedupe_caps_and_drop(tmp_path, monkeypatch):
    box = Mailbox(tmp_path / "mailbox.json")
    blob = b"sealed-bytes"
    assert box.hold(to=BOB, blob=blob, message_id="m1", expires_at=5000.0, now=100.0)
    assert (
        box.hold(to=BOB, blob=blob, message_id="m1", expires_at=5000.0, now=101.0)
        is False
    )
    assert [e.id for e in box.held_for(BOB)] == ["m1"]
    assert box.drop_held("m1", to=ALICE) is False
    assert box.drop_held("m1", to=BOB) is True
    assert box.held_for(BOB) == []

    monkeypatch.setattr(mailbox_module, "MAX_HELD_PER_RECIPIENT", 1)
    box.hold(to=BOB, blob=blob, message_id="m2", expires_at=5000.0, now=102.0)
    with pytest.raises(MailboxFull):
        box.hold(to=BOB, blob=blob, message_id="m3", expires_at=5000.0, now=103.0)
    # A different recipient still has room.
    assert box.hold(to=ALICE, blob=blob, message_id="m4", expires_at=5000.0, now=104.0)


def test_hold_rejects_oversize_blob(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    with pytest.raises(MailboxError):
        box.hold(
            to=BOB,
            blob=b"\x00" * (MAX_MAIL_BLOB_BYTES + 1),
            message_id="big",
            expires_at=5000.0,
        )


@pytest.mark.parametrize("expires_at", [float("nan"), float("inf"), float("-inf")])
def test_hold_rejects_nonfinite_expiry(tmp_path, expires_at):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    with pytest.raises(MailboxError, match="finite"):
        box.hold(to=BOB, blob=b"x", message_id="invalid", expires_at=expires_at)
    assert box.held_for(BOB) == []
    assert Mailbox(path).held_for(BOB) == []


def test_held_persistence_roundtrip(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    box.hold(to=BOB, blob=b"\x01\x02\x03", message_id="keep", expires_at=5000.0, now=7.0)
    reloaded = Mailbox(path)
    envelopes = reloaded.held_for(BOB)
    assert len(envelopes) == 1
    assert envelopes[0].blob == b"\x01\x02\x03"
    assert envelopes[0].stored_at == 7.0


# -- prune / seen / acks ---------------------------------------------------------


def test_prune_expires_everything(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    box.enqueue(to=BOB, text="old", ttl=60.0, now=100.0)
    box.hold(to=BOB, blob=b"x", message_id="held-old", expires_at=150.0, now=100.0)
    box.mark_seen("seen-old", now=100.0)
    removed = box.prune(now=100.0 + 31 * 24 * 60 * 60)
    assert removed == {"outbox": 1, "held": 1, "seen": 1}
    assert box.counts() == {"outbox": 0, "held": 0, "seen": 0}


def test_seen_and_pending_acks(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    assert box.is_seen("nope") is False
    box.mark_seen("yep")
    assert box.is_seen("yep") is True

    box.add_pending_acks("holder1", ["a", "b"])
    box.add_pending_acks("holder1", ["b", "c"])
    assert box.take_pending_acks("holder1") == ["a", "b", "c"]
    assert box.take_pending_acks("holder1") == []


# -- sealed envelopes -------------------------------------------------------------


def make_identity_pair():
    sender = RNS.Identity()
    recipient = RNS.Identity()
    return sender, recipient


def test_seal_and_open_roundtrip():
    sender, recipient = make_identity_pair()
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-1",
        text="meet at the dock",
        sent_at=4242.0,
    )
    message = open_message(recipient, blob)
    assert message["id"] == "msg-1"
    assert message["text"] == "meet at the dock"
    assert message["from_name"] == "alice"
    assert message["from"] == destination_hash(sender).hex()
    assert message["sent_at"] == 4242.0


def test_open_rejects_wrong_recipient():
    sender, recipient = make_identity_pair()
    stranger = RNS.Identity()
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-2",
        text="for bob only",
        sent_at=1.0,
    )
    with pytest.raises(MailboxError):
        open_message(stranger, blob)


def test_open_rejects_tampered_blob():
    sender, recipient = make_identity_pair()
    blob = bytearray(
        seal_message(
            sender,
            sender_name="alice",
            recipient_public_key=recipient.get_public_key(),
            message_id="msg-3",
            text="integrity matters",
            sent_at=1.0,
        )
    )
    blob[len(blob) // 2] ^= 0xFF
    with pytest.raises(MailboxError):
        open_message(recipient, bytes(blob))


def test_open_rejects_forged_sender():
    """A holder swaps in its own key but keeps the victim's name/address."""
    from tau.protocol import encode

    sender, recipient = make_identity_pair()
    forger = RNS.Identity()
    inner = {
        "type": "sealed_message",
        "v": 1,
        "id": "forged",
        "from": destination_hash(sender).hex(),
        "from_name": "alice",
        "from_key": forger.get_public_key().hex(),
        "text": "trust me",
        "sent_at": 1.0,
    }
    inner["sig"] = base64.b64encode(forger.sign(encode(inner))).decode("ascii")
    box = RNS.Identity(create_keys=False)
    assert box.load_public_key(recipient.get_public_key())
    blob = box.encrypt(encode(inner, max_bytes=MAX_MAIL_BLOB_BYTES))
    with pytest.raises(MailboxError):
        open_message(recipient, blob)


# -- protocol ---------------------------------------------------------------------


def test_message_id_roundtrip():
    raw = encode_message("with an id", message_id="abc123", sent_at=9.0)
    parsed = parse_message(raw)
    assert parsed["id"] == "abc123"
    assert parsed["sent_at"] == 9.0
    legacy = parse_message(encode_message("no id"))
    assert legacy["id"] is None


def test_mail_store_roundtrip_and_validation():
    blob = b"\x10" * 100
    raw = build_mail_store(
        to=BOB.hex(), message_id="m-9", blob=blob, expires_at=9999.0
    )
    parsed = parse_mail_store(raw)
    assert parsed["to"] == BOB.hex()
    assert parsed["id"] == "m-9"
    assert parsed["blob"] == blob
    assert parsed["expires_at"] == 9999.0

    with pytest.raises(ProtocolError):
        parse_mail_store(b"{}")
    with pytest.raises(ProtocolError):
        build_mail_store(to="zz" * 16, message_id="m", blob=b"x", expires_at=1.0)
    with pytest.raises(ProtocolError):
        parse_mail_store(
            build_mail_store(
                to=BOB.hex(),
                message_id="m",
                blob=b"\x00" * (MAX_MAIL_BLOB_BYTES + 1),
                expires_at=1.0,
            )
        )


@pytest.mark.parametrize(
    "expires_at",
    [True, False, None, "123", [], float("nan"), float("inf"), float("-inf"), 10**400],
)
def test_mail_store_rejects_invalid_expiry(expires_at):
    payload = {
        "type": "mail_store",
        "to": BOB.hex(),
        "id": "m",
        "blob": "eA==",
        "expires_at": expires_at,
    }
    for raw in (payload, json.dumps(payload).encode()):
        with pytest.raises(ProtocolError, match="expires_at must be a finite number"):
            parse_mail_store(raw)


@pytest.mark.parametrize("expires_at", [0, 123, 123.5, -1.5])
def test_mail_store_accepts_finite_numeric_expiry(expires_at):
    payload = {
        "type": "mail_store",
        "to": BOB.hex(),
        "id": "m",
        "blob": "eA==",
        "expires_at": expires_at,
    }
    assert parse_mail_store(json.dumps(payload))["expires_at"] == float(expires_at)


def test_mail_fetch_ack_parsing():
    raw = build_mail_fetch(["one", "two"])
    assert parse_mail_fetch(raw)["ack"] == ["one", "two"]
    assert parse_mail_fetch(build_mail_fetch())["ack"] == []


def test_config_has_mailbox_path(tmp_path):
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    assert config.mailbox_path == tmp_path / "mailbox.json"


def test_mailbox_file_is_owner_only(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    box.enqueue(to=BOB, text="secret-ish", now=time.time())
    assert path.exists()
    assert (path.stat().st_mode & 0o777) == 0o600


def test_sequential_writers_do_not_clobber_each_other(tmp_path):
    """Two Mailbox instances share one file (``tau serve`` + one-shot
    ``tau send``).  Every writer reloads under the transaction lock, so
    a save from a stale instance can't silently drop the other side's
    changes."""
    path = tmp_path / "mailbox.json"
    serve_side = Mailbox(path)
    cli_side = Mailbox(path)

    entry = cli_side.enqueue(to=BOB, text="queued from the CLI", now=time.time())
    # serve_side has never seen this entry in memory; both of its writes
    # must preserve it, and the second must build on the first.
    serve_side.mark_seen("some-delivered-id", now=time.time())
    serve_side.record_attempt(entry.id, error="path timeout", now=time.time())

    fresh = Mailbox(path)
    kept = fresh.get_outbox(entry.id)
    assert kept is not None
    assert kept.attempts == 1
    assert kept.last_error == "path timeout"
    assert fresh.is_seen("some-delivered-id")

    assert cli_side.remove_outbox(entry.id) is True
    assert Mailbox(path).get_outbox(entry.id) is None


def test_transaction_is_reentrant(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    with box._transaction():
        entry = box.enqueue(to=BOB, text="nested write", now=time.time())
    assert box.get_outbox(entry.id) is not None
