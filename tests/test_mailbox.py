from __future__ import annotations

import base64
import json
import threading
import time

import RNS
import pytest

import tau.mailbox as mailbox_module
from tau.config import TauConfig
from tau.identity import destination_hash, load_public_key
from tau.mailbox import (
    MAX_GROUP_PENDING_EDITS_PER_SENDER,
    RETRACTED_CLOCK_SKEW_TOLERANCE,
    RETRACTED_RETENTION,
    SENT_RETRACTED_RETENTION,
    Mailbox,
    MailboxError,
    MailboxFull,
    open_message,
    seal_message,
)
from tau.protocol import (
    MAX_MAIL_BLOB_BYTES,
    MAX_MAIL_TTL,
    ProtocolError,
    build_mail_fetch,
    build_mail_store,
    encode_message,
    excerpt_hash,
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
    assert box.counts() == {
        "outbox": 1,
        "held": 0,
        "seen": 0,
        "sent": 0,
        "receipts": 0,
        "retracted": 0,
        "sent_retracted": 0,
    }

    reloaded = Mailbox(path)
    entries = reloaded.outbox_list()
    assert len(entries) == 1
    assert entries[0].id == entry.id
    assert entries[0].text == "hello bob"
    assert entries[0].to_name == "bob"
    assert entries[0].created_at == 1000.0


def test_enqueue_sent_at_survives_reload(tmp_path):
    # A group fan-out records its shared send time on the queued leg,
    # which is earlier than the enqueue time: it must survive the
    # on-disk round trip, not collapse back to created_at.
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    entry = box.enqueue(to=BOB, text="crew hello", now=2000.0, sent_at=100.0)
    assert entry.sent_at == 100.0
    assert entry.wire_sent_at == 100.0
    assert entry.to_dict()["sent_at"] == 100.0

    reloaded = Mailbox(path)
    assert reloaded.outbox_list()[0].sent_at == 100.0
    assert reloaded.outbox_list()[0].wire_sent_at == 100.0


def test_enqueue_without_sent_at_uses_created_at(tmp_path):
    # A single-leg send records no logical time: the enqueue time is
    # the send time, before and after a reload.
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    entry = box.enqueue(to=BOB, text="hello bob", now=1000.0)
    assert entry.sent_at is None
    assert entry.wire_sent_at == 1000.0

    assert Mailbox(path).outbox_list()[0].wire_sent_at == 1000.0


def test_outbox_entry_without_sent_at_loads_with_created_at(tmp_path):
    # An entry written before the field existed carries no sent_at:
    # loading it must not fail, and its wire time is its enqueue time.
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    box.enqueue(to=BOB, text="old entry", now=1000.0)
    raw = json.loads(path.read_text())
    del raw["outbox"][0]["sent_at"]
    path.write_text(json.dumps(raw))

    loaded = Mailbox(path).outbox_list()[0]
    assert loaded.sent_at is None
    assert loaded.wire_sent_at == 1000.0


def test_enqueue_rejects_invalid_sent_at(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    with pytest.raises(MailboxError):
        box.enqueue(to=BOB, text="hello", sent_at=float("inf"))


def test_enqueue_kind_survives_reload(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    entry = box.enqueue(to=BOB, text="ping?", kind="ping", now=1000.0)
    assert entry.kind == "ping"
    assert entry.to_dict()["kind"] == "ping"

    reloaded = Mailbox(path)
    assert reloaded.outbox_list()[0].kind == "ping"


def test_enqueue_contact_survives_reload(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    card = {"name": "carol", "destination_hash": "cc" * 16}
    entry = box.enqueue(to=BOB, text="meet carol", contact=card, now=1000.0)
    assert entry.contact == card
    assert entry.to_dict()["contact"] == card

    reloaded = Mailbox(path)
    assert reloaded.outbox_list()[0].contact == card


def test_enqueue_defaults_to_chat_kind(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(to=BOB, text="hello")
    assert entry.kind == "chat"


def test_enqueue_rejects_invalid_kind(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    with pytest.raises(ProtocolError):
        box.enqueue(to=BOB, text="hello", kind="has space")


def _edit_payload(**overrides):
    payload = {
        "target": "m-1",
        "text": "fixed here",
        "edit_id": "e-1",
        "sent_at": 1000.0,
        "payload_b64": base64.b64encode(b"signed-edit-bytes").decode("ascii"),
    }
    payload.update(overrides)
    return payload


def test_enqueue_edit_entry_round_trips(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    entry = box.enqueue(
        to=BOB,
        text="fixed here",
        to_name="bob",
        now=1000.0,
        sent_at=1000.0,
        edited_at=1000.0,
        edit=_edit_payload(),
    )
    assert entry.kind == "edit"
    assert entry.edit["target"] == "m-1"
    assert entry.edit["text"] == "fixed here"
    assert entry.edit["edit_id"] == "e-1"
    assert entry.edit["payload_b64"] == _edit_payload()["payload_b64"]

    reloaded = Mailbox(path)
    again = reloaded.outbox_list()[0]
    assert again.kind == "edit"
    assert again.edit == entry.edit


def test_enqueue_edit_forces_edit_kind(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(
        to=BOB, text="fixed here", kind="chat", edit=_edit_payload()
    )
    assert entry.kind == "edit"


def test_enqueue_edit_rejects_invalid_payload(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    with pytest.raises(MailboxError):
        box.enqueue(to=BOB, text="fixed here", edit={"target": "m-1"})
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB,
            text="fixed here",
            edit=_edit_payload(payload_b64="!!!not-base64"),
        )
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB, text="fixed here", edit=_edit_payload(target_author="xyz")
        )
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB, text="fixed here", edit=_edit_payload(text="   ")
        )


def test_enqueue_edit_rejects_combined_fields(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB,
            text="fixed here",
            edit=_edit_payload(),
            poll_options=["a", "b"],
        )
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB,
            text="fixed here",
            edit=_edit_payload(),
            location={"lat": 1.0, "lon": 2.0},
        )
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB,
            text="fixed here",
            edit=_edit_payload(),
            in_reply_to={"id": "m-0", "excerpt": "00" * 8},
        )


def test_reload_drops_corrupt_edit_but_keeps_edit_kind(tmp_path):
    # A hand-edited or corrupt payload must never turn the entry into
    # a text message: the kind stays "edit" so delivery drops it
    # instead of sending the display text as chat.
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    box.enqueue(to=BOB, text="fixed here", now=1000.0, edit=_edit_payload())
    raw = json.loads(path.read_text())
    raw["outbox"][0]["edit"]["payload_b64"] = "!!!not-base64"
    path.write_text(json.dumps(raw))

    entry = Mailbox(path).outbox_list()[0]
    assert entry.kind == "edit"
    assert entry.edit is None


def _reaction_payload(**overrides):
    payload = {
        "target": "m-1",
        "token": "👍",
        "reaction_id": "r-1",
        "sent_at": 1000.0,
        "payload_b64": base64.b64encode(b"signed-reaction-bytes").decode("ascii"),
    }
    payload.update(overrides)
    return payload


def test_enqueue_reaction_entry_round_trips(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    entry = box.enqueue(
        to=BOB,
        text="👍",
        to_name="bob",
        now=1000.0,
        reaction=_reaction_payload(),
    )
    assert entry.kind == "reaction"
    assert entry.reaction["target"] == "m-1"
    assert entry.reaction["token"] == "👍"
    assert entry.reaction["reaction_id"] == "r-1"
    assert entry.reaction["payload_b64"] == _reaction_payload()["payload_b64"]

    reloaded = Mailbox(path)
    again = reloaded.outbox_list()[0]
    assert again.kind == "reaction"
    assert again.reaction == entry.reaction


def test_enqueue_reaction_forces_reaction_kind(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(
        to=BOB, text="👍", kind="chat", reaction=_reaction_payload()
    )
    assert entry.kind == "reaction"


def test_enqueue_reaction_rejects_invalid_payload(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    with pytest.raises(MailboxError):
        box.enqueue(to=BOB, text="👍", reaction={"target": "m-1"})
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB,
            text="👍",
            reaction=_reaction_payload(payload_b64="!!!not-base64"),
        )
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB, text="👍", reaction=_reaction_payload(target_author="xyz")
        )


def test_enqueue_reaction_rejects_combined_fields(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB,
            text="👍",
            reaction=_reaction_payload(),
            poll_options=["a", "b"],
        )
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB,
            text="👍",
            reaction=_reaction_payload(),
            location={"lat": 1.0, "lon": 2.0},
        )
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB,
            text="👍",
            reaction=_reaction_payload(),
            in_reply_to={"id": "m-0", "excerpt": "00" * 8},
        )


def test_reload_drops_corrupt_reaction_but_keeps_reaction_kind(tmp_path):
    # A hand-edited or corrupt payload must never turn the entry into
    # a text message: the kind stays "reaction" so delivery drops it
    # instead of sending the display text as chat.
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    box.enqueue(to=BOB, text="👍", now=1000.0, reaction=_reaction_payload())
    raw = json.loads(path.read_text())
    raw["outbox"][0]["reaction"]["payload_b64"] = "!!!not-base64"
    path.write_text(json.dumps(raw))

    entry = Mailbox(path).outbox_list()[0]
    assert entry.kind == "reaction"
    assert entry.reaction is None


def test_reload_keeps_retract_kind_when_kind_and_payload_are_corrupt(
    tmp_path,
):
    # A corrupt retract payload drops the payload but keeps kind
    # "retract" — and that must hold even when the stored kind is
    # itself corrupt: otherwise the entry falls back to plain chat
    # and its display text ("retraction") goes out as a message.
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    box.enqueue(to=BOB, text="retraction", now=1000.0, retract=_retract_payload())
    raw = json.loads(path.read_text())
    raw["outbox"][0]["retract"]["payload_b64"] = "!!!not-base64"
    raw["outbox"][0]["kind"] = "not-a-kind"
    path.write_text(json.dumps(raw))

    entry = Mailbox(path).outbox_list()[0]
    assert entry.kind == "retract"
    assert entry.retract is None


def _retract_payload(**overrides):
    payload = {
        "target": "m-1",
        "retract_id": "x-1",
        "sent_at": 1000.0,
        "payload_b64": base64.b64encode(b"signed-retract-bytes").decode("ascii"),
    }
    payload.update(overrides)
    return payload


def test_enqueue_retract_entry_round_trips(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    entry = box.enqueue(
        to=BOB,
        text="retraction",
        to_name="bob",
        now=1000.0,
        retract=_retract_payload(),
    )
    assert entry.kind == "retract"
    assert entry.retract["target"] == "m-1"
    assert entry.retract["retract_id"] == "x-1"
    assert entry.retract["payload_b64"] == _retract_payload()["payload_b64"]

    reloaded = Mailbox(path)
    again = reloaded.outbox_list()[0]
    assert again.kind == "retract"
    assert again.retract == entry.retract


def test_enqueue_retract_forces_retract_kind(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(
        to=BOB, text="retraction", kind="chat", retract=_retract_payload()
    )
    assert entry.kind == "retract"


def test_enqueue_retract_rejects_invalid_payload(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    with pytest.raises(MailboxError):
        box.enqueue(to=BOB, text="retraction", retract={"target": "m-1"})
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB,
            text="retraction",
            retract=_retract_payload(payload_b64="!!!not-base64"),
        )
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB,
            text="retraction",
            retract=_retract_payload(sent_at=float("nan")),
        )


def test_enqueue_retract_rejects_combined_fields(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB,
            text="retraction",
            retract=_retract_payload(),
            poll_options=["a", "b"],
        )
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB,
            text="retraction",
            retract=_retract_payload(),
            location={"lat": 1.0, "lon": 2.0},
        )


def test_enqueue_group_retract_round_trips(tmp_path):
    # A group retract carries its group context plus the author and
    # send time of the retracted copy, so a sealed relay copy names
    # the copy the recipient holds even though wire ids differ per leg.
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(
        to=BOB,
        text="retraction",
        retract=_retract_payload(
            target_author="ab" * 16, target_sent_at=100.0
        ),
        group={
            "id": "ab" * 16,
            "name": "crew",
            "creator": "ab" * 16,
            "members": ["ab" * 16],
        },
    )
    assert entry.kind == "retract"
    assert entry.group_id == "ab" * 16
    assert entry.retract["target_author"] == "ab" * 16
    assert entry.retract["target_sent_at"] == 100.0

    reloaded = Mailbox(tmp_path / "mailbox.json")
    again = reloaded.outbox_list()[0]
    assert again.kind == "retract"
    assert again.group_id == "ab" * 16
    assert again.retract == entry.retract


def test_enqueue_group_retract_rejects_bad_group_context(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB,
            text="retraction",
            retract=_retract_payload(target_author="not-a-hash"),
            group={
                "id": "ab" * 16,
                "name": "crew",
                "creator": "ab" * 16,
                "members": ["ab" * 16],
            },
        )
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB,
            text="retraction",
            retract=_retract_payload(target_sent_at=float("nan")),
            group={
                "id": "ab" * 16,
                "name": "crew",
                "creator": "ab" * 16,
                "members": ["ab" * 16],
            },
        )


def test_reload_drops_corrupt_retract_but_keeps_retract_kind(tmp_path):
    # A hand-edited or corrupt payload must never turn the entry into
    # a text message: the kind stays "retract" so delivery drops it
    # instead of sending the display text as chat.
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    box.enqueue(to=BOB, text="retraction", now=1000.0, retract=_retract_payload())
    raw = json.loads(path.read_text())
    raw["outbox"][0]["retract"]["payload_b64"] = "!!!not-base64"
    path.write_text(json.dumps(raw))

    entry = Mailbox(path).outbox_list()[0]
    assert entry.kind == "retract"
    assert entry.retract is None



def test_outbox_entry_without_kind_loads_as_chat(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    box.enqueue(to=BOB, text="old entry", now=1000.0)
    raw = json.loads(path.read_text())
    del raw["outbox"][0]["kind"]
    path.write_text(json.dumps(raw))

    assert Mailbox(path).outbox_list()[0].kind == "chat"


def test_enqueue_dedupes_by_message_id(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    first = box.enqueue(to=BOB, text="once", message_id="fixed-id")
    second = box.enqueue(to=BOB, text="once", message_id="fixed-id")
    # Equal, not identical: enqueue now reloads state under the
    # cross-process transaction, so the deduped entry is a fresh copy.
    assert first == second
    assert box.counts()["outbox"] == 1


def test_sent_record_keeps_delivered_at_through_reload(tmp_path):
    """`delivered_at` must survive a reload: the queued-to-delivered flip
    and the sent-messages display both read it after one."""
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    box.record_sent("m1", to=BOB, text="hello", sent_at=1000.0)
    box.record_sent("m2", to=BOB, text="later", sent_at=1001.0)
    box.record_delivered("m1", now=1002.0)

    reloaded = Mailbox(path)
    delivered = reloaded.sent_record("m1")
    assert delivered is not None
    assert delivered["delivered_at"] == 1002.0
    pending = reloaded.sent_record("m2")
    assert pending is not None
    assert pending["delivered_at"] is None

    # A record written before delivery tracking has no such key and
    # keeps counting as delivered in the sent-messages display.
    raw = json.loads(path.read_text())
    del raw["sent"]["m2"]["delivered_at"]
    path.write_text(json.dumps(raw))
    reloaded = Mailbox(path)
    assert "delivered_at" not in reloaded.sent_record("m2")


def test_record_delivered_keeps_lxmf_after_direct_delivery(tmp_path):
    """A later LXMF confirmation must win over a direct one: the
    recipient now also holds an `lxmf:` copy, so a retract reading
    the stale "direct" route would miss it. "lxmf" is sticky, and
    the first delivery time stands."""
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    box.record_sent("m1", to=BOB, text="hello", sent_at=1000.0)
    box.record_delivered("m1", now=1002.0, via="direct")
    assert box.sent_record("m1")["delivered_via"] == "direct"
    assert box.sent_record("m1")["delivered_at"] == 1002.0

    box.record_delivered("m1", now=1003.0, via="lxmf")
    assert box.sent_record("m1")["delivered_via"] == "lxmf"
    assert box.sent_record("m1")["delivered_at"] == 1002.0

    # A direct confirmation after the LXMF one must not erase it.
    box.record_delivered("m1", now=1004.0, via="direct")
    assert box.sent_record("m1")["delivered_via"] == "lxmf"
    assert box.sent_record("m1")["delivered_at"] == 1002.0


def test_sent_record_keeps_delivery_route_through_reload(tmp_path):
    """`delivered_via` must survive a reload: a later retract reads it
    to tell whether the recipient holds the copy under the tau wire
    id or under an LXMF id."""
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    box.record_sent("m1", to=BOB, text="hello", sent_at=1000.0)
    box.record_sent("m2", to=BOB, text="later", sent_at=1001.0)
    box.record_delivered("m1", now=1002.0, via="lxmf")
    box.record_delivered("m2", now=1003.0, via="direct")

    reloaded = Mailbox(path)
    assert reloaded.sent_record("m1")["delivered_via"] == "lxmf"
    assert reloaded.sent_record("m2")["delivered_via"] == "direct"

    # A record written before route tracking has no such key, and an
    # unknown route value is dropped on load rather than trusted.
    raw = json.loads(path.read_text())
    del raw["sent"]["m2"]["delivered_via"]
    raw["sent"]["m1"]["delivered_via"] = "carrier-pigeon"
    path.write_text(json.dumps(raw))
    reloaded = Mailbox(path)
    assert "delivered_via" not in reloaded.sent_record("m2")
    assert "delivered_via" not in reloaded.sent_record("m1")


def test_update_sent_text_ignores_a_stale_edit(tmp_path):
    """Two overlapping edit sends can finish out of order: the older
    one's local write must not rewind the newer one's text."""
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    box.record_sent("m1", to=BOB, text="original", sent_at=1000.0)

    box.update_sent_text("m1", "newer text", edited_at=200.0)
    assert box.sent_record("m1")["text"] == "newer text"

    box.update_sent_text("m1", "older text", edited_at=100.0)
    assert box.sent_record("m1")["text"] == "newer text"
    assert box.sent_record("m1")["edited_at"] == 200.0

    box.update_sent_text("m1", "newest text", edited_at=300.0)
    assert box.sent_record("m1")["text"] == "newest text"
    assert box.sent_record("m1")["edited_at"] == 300.0


def test_sent_record_keeps_edited_at_through_reload(tmp_path):
    """`edited_at` must survive a reload: the staleness check in
    update_sent_text compares against it after the transaction
    reloads the document."""
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    box.record_sent("m1", to=BOB, text="original", sent_at=1000.0)
    box.update_sent_text("m1", "newer text", edited_at=200.0)

    reloaded = Mailbox(path)
    assert reloaded.sent_record("m1")["edited_at"] == 200.0

    # A stale edit arriving after the reload is still rejected.
    reloaded.update_sent_text("m1", "older text", edited_at=100.0)
    assert reloaded.sent_record("m1")["text"] == "newer text"


def test_outbox_entry_keeps_edited_at_through_reload(tmp_path):
    """A re-queued edited leg keeps its edit timestamp across a
    reload, so a later sweep still delivers the edited version with
    its ordering state intact."""
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    box.enqueue(
        to=BOB,
        text="fixed here",
        message_id="leg-1",
        sent_at=100.0,
        edited_at=150.0,
    )
    reloaded = Mailbox(path)
    entry = reloaded.get_outbox("leg-1")
    assert entry is not None
    assert entry.text == "fixed here"
    assert entry.edited_at == 150.0
    # An unedited entry carries no timestamp.
    box.enqueue(to=BOB, text="plain", message_id="leg-2", sent_at=100.0)
    assert Mailbox(path).get_outbox("leg-2").edited_at is None


def test_load_dedupes_duplicate_message_ids(tmp_path):
    """A mailbox.json with duplicate ids keeps the newest copy of each
    and is rewritten on load, so the duplicates do not survive a
    restart."""
    path = tmp_path / "mailbox.json"
    payload = {
        "version": 1,
        "outbox": [
            {
                "id": "dup-id",
                "to": BOB.hex(),
                "text": "newer copy",
                "created_at": 2000.0,
                "expires_at": 9000.0,
            },
            {
                "id": "dup-id",
                "to": BOB.hex(),
                "text": "older copy",
                "created_at": 1000.0,
                "expires_at": 9000.0,
            },
            {
                "id": "other-id",
                "to": ALICE.hex(),
                "text": "untouched",
                "created_at": 1500.0,
                "expires_at": 9000.0,
            },
        ],
        "held": [
            {
                "id": "held-dup",
                "to": ALICE.hex(),
                "blob": base64.b64encode(b"newer envelope").decode("ascii"),
                "stored_at": 3000.0,
                "expires_at": 9000.0,
            },
            {
                "id": "held-dup",
                "to": ALICE.hex(),
                "blob": base64.b64encode(b"older envelope").decode("ascii"),
                "stored_at": 2500.0,
                "expires_at": 9000.0,
            },
        ],
        "seen": {},
        "pending_acks": {"holder-hash": ["mid-1", "mid-1", "mid-2"]},
        "dropped_held": {},
    }
    path.write_text(json.dumps(payload), encoding="utf-8")

    box = Mailbox(path)
    assert box.counts() == {"outbox": 2, "held": 1, "seen": 0, "sent": 0, "receipts": 0, "retracted": 0, "sent_retracted": 0}
    kept = box.get_outbox("dup-id")
    assert kept is not None
    assert kept.text == "newer copy"
    assert kept.created_at == 2000.0
    held = box.held_for(ALICE)
    assert len(held) == 1
    assert held[0].blob == b"newer envelope"

    # The file itself was rewritten without the duplicates.
    reloaded = json.loads(path.read_text(encoding="utf-8"))
    assert len(reloaded["outbox"]) == 2
    assert len(reloaded["held"]) == 1
    assert reloaded["pending_acks"]["holder-hash"] == ["mid-1", "mid-2"]

    # The load report counts what was dropped; a clean file drops none.
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert box.load() == {"outbox": 1, "held": 1, "pending_acks": 1}
    assert box.load() == {"outbox": 0, "held": 0, "pending_acks": 0}


def test_mailbox_defer_load_reads_nothing_until_transaction(tmp_path):
    """A mailbox constructed with defer_load=True (#744) reads nothing
    from disk: its collections stay empty until a transaction reloads
    them, so a caller with its own bounded wait never blocks on the
    mailbox lock before that wait begins."""
    path = tmp_path / "mailbox.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "outbox": [
                    {
                        "id": "one",
                        "to": BOB.hex(),
                        "text": "hello",
                        "created_at": 1000.0,
                        "expires_at": 9000.0,
                    }
                ],
                "held": [],
                "seen": {},
                "pending_acks": {},
                "dropped_held": {},
            }
        ),
        encoding="utf-8",
    )

    box = Mailbox(path, defer_load=True)
    assert box.counts() == {
        "outbox": 0,
        "held": 0,
        "seen": 0,
        "sent": 0,
        "receipts": 0,
        "retracted": 0,
        "sent_retracted": 0,
    }
    with box.transaction():
        assert box.counts()["outbox"] == 1
        assert box.get_outbox("one") is not None
        assert box.get_outbox("one").text == "hello"


def test_load_skips_nonfinite_timestamps(tmp_path):
    """NaN / infinite timestamps never parse: a corrupt copy cannot
    replace a newer valid entry in the newest-wins comparison."""
    path = tmp_path / "mailbox.json"
    payload = {
        "version": 1,
        "outbox": [
            {
                "id": "nan-created",
                "to": BOB.hex(),
                "text": "bad timestamp",
                "created_at": float("nan"),
                "expires_at": 9000.0,
            },
            {
                "id": "good-id",
                "to": BOB.hex(),
                "text": "good",
                "created_at": 1000.0,
                "expires_at": 9000.0,
            },
        ],
        "held": [
            {
                "id": "inf-stored",
                "to": ALICE.hex(),
                "blob": base64.b64encode(b"bad").decode("ascii"),
                "stored_at": float("inf"),
                "expires_at": 9000.0,
            },
        ],
        "seen": {},
        "pending_acks": {},
        "dropped_held": {},
    }
    path.write_text(json.dumps(payload), encoding="utf-8")

    box = Mailbox(path)
    assert box.counts() == {"outbox": 1, "held": 0, "seen": 0, "sent": 0, "receipts": 0, "retracted": 0, "sent_retracted": 0}
    assert box.get_outbox("nan-created") is None
    assert box.get_outbox("good-id") is not None


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


def test_resolve_outbox_by_full_id_and_prefix(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(
        to=BOB, text="find me", message_id="ab" * 16, now=10.0
    )
    assert box.resolve_outbox(entry.id) == entry
    assert box.resolve_outbox(entry.id[:8]) == entry
    assert box.resolve_outbox(entry.id[:8].upper()) == entry


def test_resolve_outbox_rejects_unknown_and_ambiguous(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    box.enqueue(to=BOB, text="one", message_id="aa" + "11" * 15, now=10.0)
    box.enqueue(to=BOB, text="two", message_id="aa" + "22" * 15, now=11.0)
    with pytest.raises(MailboxError, match="unknown queued message"):
        box.resolve_outbox("ff" * 16)
    with pytest.raises(MailboxError, match="unknown queued message"):
        box.resolve_outbox("ff")
    with pytest.raises(MailboxError, match="2 queued messages match prefix"):
        box.resolve_outbox("aa")
    with pytest.raises(MailboxError, match="must not be empty"):
        box.resolve_outbox("  ")


def test_edit_scheduled_outbox_text(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    entry = box.enqueue(
        to=BOB,
        text="see you soon",
        message_id="cc" + "11" * 15,
        now=1000.0,
        send_after=5000.0,
    )
    edited = box.edit_scheduled_outbox(
        entry.id[:8], text="see you at noon", now=2000.0
    )
    assert [item.id for item in edited] == [entry.id]
    stored = box.get_outbox(entry.id)
    assert stored.text == "see you at noon"
    assert stored.send_after == 5000.0
    assert stored.sent_at is None
    # The edit survives a reload: the new text is what gets delivered.
    assert Mailbox(path).get_outbox(entry.id).text == "see you at noon"


def test_edit_scheduled_outbox_send_time(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(
        to=BOB,
        text="later",
        message_id="cc" + "22" * 15,
        now=1000.0,
        send_after=5000.0,
    )
    edited = box.edit_scheduled_outbox(
        entry.id, send_after=9000.0, now=2000.0
    )
    assert [item.id for item in edited] == [entry.id]
    stored = box.get_outbox(entry.id)
    assert stored.text == "later"
    assert stored.send_after == 9000.0


def test_edit_scheduled_outbox_text_and_time(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(
        to=BOB,
        text="later",
        message_id="cc" + "33" * 15,
        now=1000.0,
        send_after=5000.0,
    )
    box.edit_scheduled_outbox(
        entry.id, text="sooner, and different", send_after=3000.0, now=2000.0
    )
    stored = box.get_outbox(entry.id)
    assert stored.text == "sooner, and different"
    assert stored.send_after == 3000.0


def test_edit_scheduled_outbox_requires_a_change(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(
        to=BOB,
        text="later",
        message_id="cc" + "44" * 15,
        now=1000.0,
        send_after=5000.0,
    )
    with pytest.raises(MailboxError, match="nothing to change"):
        box.edit_scheduled_outbox(entry.id, now=2000.0)
    assert box.get_outbox(entry.id).text == "later"


def test_edit_scheduled_outbox_rejects_unknown_and_ambiguous(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    box.enqueue(
        to=BOB,
        text="one",
        message_id="dd" + "11" * 15,
        now=1000.0,
        send_after=5000.0,
    )
    box.enqueue(
        to=BOB,
        text="two",
        message_id="dd" + "22" * 15,
        now=1000.0,
        send_after=5000.0,
    )
    with pytest.raises(MailboxError, match="unknown queued message"):
        box.edit_scheduled_outbox("ff", text="nope", now=2000.0)
    with pytest.raises(MailboxError, match="2 queued messages match prefix"):
        box.edit_scheduled_outbox("dd", text="nope", now=2000.0)


def test_edit_scheduled_outbox_refuses_not_scheduled(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    immediate = box.enqueue(to=BOB, text="now", now=1000.0)
    with pytest.raises(MailboxError, match="no longer scheduled"):
        box.edit_scheduled_outbox(immediate.id, text="too late", now=2000.0)
    # A send time that already passed is no longer scheduled either.
    elapsed = box.enqueue(
        to=BOB,
        text="was later",
        message_id="cc" + "55" * 15,
        now=1000.0,
        send_after=3000.0,
    )
    with pytest.raises(MailboxError, match="no longer scheduled"):
        box.edit_scheduled_outbox(elapsed.id, text="too late", now=4000.0)
    assert box.get_outbox(elapsed.id).text == "was later"


def test_edit_scheduled_outbox_validates_text(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(
        to=BOB,
        text="later",
        message_id="cc" + "66" * 15,
        now=1000.0,
        send_after=5000.0,
    )
    with pytest.raises(MailboxError, match="must not be empty"):
        box.edit_scheduled_outbox(entry.id, text="   ", now=2000.0)
    with pytest.raises(MailboxError, match="longer than 2000 characters"):
        box.edit_scheduled_outbox(entry.id, text="x" * 2001, now=2000.0)
    assert box.get_outbox(entry.id).text == "later"


def test_edit_scheduled_outbox_validates_send_time(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(
        to=BOB,
        text="later",
        message_id="cc" + "77" * 15,
        now=1000.0,
        send_after=5000.0,
    )
    with pytest.raises(MailboxError, match="must be in the future"):
        box.edit_scheduled_outbox(entry.id, send_after=2000.0, now=2000.0)
    with pytest.raises(MailboxError, match="at most 29d23h"):
        box.edit_scheduled_outbox(
            entry.id, send_after=2000.0 + 30 * 86400, now=2000.0
        )
    assert box.get_outbox(entry.id).send_after == 5000.0


def test_edit_scheduled_outbox_refuses_send_after_expiry(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(
        to=BOB,
        text="later",
        message_id="cc" + "88" * 15,
        now=1000.0,
        send_after=5000.0,
        expires_at=6000.0,
    )
    with pytest.raises(MailboxError, match="before the message expiry"):
        box.edit_scheduled_outbox(entry.id, send_after=6000.0, now=2000.0)
    assert box.get_outbox(entry.id).send_after == 5000.0


def test_edit_scheduled_outbox_refuses_poll_leg(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(
        to=BOB,
        text="lunch?",
        message_id="cc" + "99" * 15,
        now=1000.0,
        send_after=5000.0,
        poll_options=["yes", "no"],
    )
    with pytest.raises(MailboxError, match="not plain text"):
        box.edit_scheduled_outbox(entry.id, text="dinner?", now=2000.0)
    assert box.get_outbox(entry.id).text == "lunch?"


def test_edit_scheduled_outbox_updates_every_group_leg(tmp_path):
    # A scheduled group message fans out to one leg per member, all
    # sharing the group id and the logical send time: editing one leg
    # must rewrite them all, so members still receive identical text.
    # A leg of another group keeps its own text.
    box = Mailbox(tmp_path / "mailbox.json")
    group = {
        "id": "ee" * 16,
        "name": "crew",
        "creator": "aa" * 16,
        "members": ["aa" * 16, "bb" * 16, "cc" * 16],
    }
    leg_bob = box.enqueue(
        to=BOB,
        text="crew hello",
        message_id="ee" + "11" * 15,
        now=1000.0,
        send_after=5000.0,
        sent_at=900.0,
        group=group,
    )
    leg_carol = box.enqueue(
        to=b"\xcc" * 16,
        text="crew hello",
        message_id="ee" + "22" * 15,
        now=1000.0,
        send_after=5000.0,
        sent_at=900.0,
        group=group,
    )
    other = box.enqueue(
        to=BOB,
        text="other crew",
        message_id="ee" + "33" * 15,
        now=1000.0,
        send_after=5000.0,
        sent_at=900.0,
        group={**group, "id": "ff" * 16},
    )
    edited = box.edit_scheduled_outbox(
        leg_bob.id[:8], text="crew hello, edited", send_after=7000.0, now=2000.0
    )
    assert {item.id for item in edited} == {leg_bob.id, leg_carol.id}
    assert box.get_outbox(leg_bob.id).text == "crew hello, edited"
    assert box.get_outbox(leg_carol.id).text == "crew hello, edited"
    assert box.get_outbox(leg_carol.id).send_after == 7000.0
    # The logical send time is untouched, so reactions and retracts
    # still name the message; the other group is untouched.
    assert box.get_outbox(leg_bob.id).sent_at == 900.0
    assert box.get_outbox(other.id).text == "other crew"
    assert box.get_outbox(other.id).send_after == 5000.0


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


def test_delivery_backoff_survives_large_attempt_count(tmp_path):
    # Entries that accumulated thousands of attempts under the old fixed
    # retry interval must still load, schedule and serialize.
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    entry = box.enqueue(to=BOB, text="long offline", now=1000.0)
    box.record_attempt(entry.id, error="no path", now=2000.0)
    raw = json.loads(path.read_text())
    for item in raw["outbox"]:
        item["attempts"] = 5000
    path.write_text(json.dumps(raw))

    reloaded = Mailbox(path)
    stored = reloaded.get_outbox(entry.id)
    assert stored.next_attempt_at == 2000.0 + 3600.0
    assert stored.to_dict()["next_attempt_at"] == 2000.0 + 3600.0
    assert [e.id for e in reloaded.due_outbox(now=5600.0)] == [entry.id]


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


def test_hold_per_sender_count_quota_sheds_oldest(tmp_path, monkeypatch):
    monkeypatch.setattr(mailbox_module, "HELD_QUOTA_UNKNOWN_COUNT", 2)
    box = Mailbox(tmp_path / "mailbox.json")
    blob = b"sealed-bytes"
    sender = b"\xcc" * 16
    assert box.hold(
        to=BOB, blob=blob, message_id="s1", expires_at=5000.0, now=100.0,
        depositor=sender,
    )
    assert box.hold(
        to=ALICE, blob=blob, message_id="s2", expires_at=5000.0, now=101.0,
        depositor=sender,
    )
    # A third deposit from the same sender sheds the oldest of theirs.
    assert box.hold(
        to=BOB, blob=blob, message_id="s3", expires_at=5000.0, now=102.0,
        depositor=sender,
    )
    assert [e.id for e in box.held_list()] == ["s2", "s3"]
    # Another sender is unaffected.
    assert box.hold(
        to=BOB, blob=blob, message_id="o1", expires_at=5000.0, now=103.0,
        depositor=b"\xdd" * 16,
    )
    assert [e.id for e in box.held_list()] == ["s2", "s3", "o1"]


def test_hold_per_sender_bytes_quota_sheds_oldest(tmp_path, monkeypatch):
    monkeypatch.setattr(mailbox_module, "HELD_QUOTA_UNKNOWN_BYTES", 10)
    box = Mailbox(tmp_path / "mailbox.json")
    sender = b"\xcc" * 16
    box.hold(
        to=BOB, blob=b"123456", message_id="b1", expires_at=5000.0, now=100.0,
        depositor=sender,
    )
    box.hold(
        to=BOB, blob=b"123456", message_id="b2", expires_at=5000.0, now=101.0,
        depositor=sender,
    )
    assert [e.id for e in box.held_list()] == ["b2"]


def test_hold_known_sender_gets_higher_quota(tmp_path, monkeypatch):
    monkeypatch.setattr(mailbox_module, "HELD_QUOTA_UNKNOWN_COUNT", 1)
    monkeypatch.setattr(mailbox_module, "HELD_QUOTA_KNOWN_COUNT", 3)
    box = Mailbox(tmp_path / "mailbox.json")
    sender = b"\xcc" * 16
    for index in range(3):
        assert box.hold(
            to=BOB, blob=b"x", message_id=f"k{index}", expires_at=5000.0,
            now=100.0 + index, depositor=sender, trusted=True,
        )
    assert len(box.held_list()) == 3
    # A fourth trusted deposit sheds the oldest instead of refusing.
    assert box.hold(
        to=BOB, blob=b"x", message_id="k3", expires_at=5000.0, now=103.0,
        depositor=sender, trusted=True,
    )
    assert [e.id for e in box.held_list()] == ["k1", "k2", "k3"]
    # The same sender untrusted is capped at one: its earlier envelopes
    # are shed to make room for the single allowed slot.
    assert box.hold(
        to=ALICE, blob=b"x", message_id="u1", expires_at=5000.0, now=104.0,
        depositor=sender,
    )
    assert [e.id for e in box.held_list()] == ["u1"]


def test_hold_unidentified_depositors_share_one_bucket(tmp_path, monkeypatch):
    monkeypatch.setattr(mailbox_module, "HELD_QUOTA_UNKNOWN_COUNT", 1)
    box = Mailbox(tmp_path / "mailbox.json")
    box.hold(to=BOB, blob=b"a", message_id="anon-1", expires_at=5000.0, now=100.0)
    box.hold(to=BOB, blob=b"b", message_id="anon-2", expires_at=5000.0, now=101.0)
    assert [e.id for e in box.held_list()] == ["anon-2"]


def test_hold_single_blob_over_bytes_quota_is_refused(tmp_path, monkeypatch):
    monkeypatch.setattr(mailbox_module, "HELD_QUOTA_UNKNOWN_BYTES", 4)
    box = Mailbox(tmp_path / "mailbox.json")
    with pytest.raises(MailboxFull):
        box.hold(
            to=BOB, blob=b"123456", message_id="too-big", expires_at=5000.0,
            depositor=b"\xcc" * 16,
        )
    assert box.held_list() == []


def test_refused_oversized_deposit_keeps_existing_held_mail(tmp_path, monkeypatch):
    monkeypatch.setattr(mailbox_module, "HELD_QUOTA_UNKNOWN_BYTES", 4)
    box = Mailbox(tmp_path / "mailbox.json")
    sender = b"\xcc" * 16
    assert box.hold(
        to=BOB, blob=b"abc", message_id="keep", expires_at=5000.0, now=100.0,
        depositor=sender,
    )
    with pytest.raises(MailboxFull):
        box.hold(
            to=BOB, blob=b"123456", message_id="too-big", expires_at=5000.0,
            now=101.0, depositor=sender,
        )
    assert [e.id for e in box.held_list()] == ["keep"]


def test_hold_recipient_cap_counts_shed_victims(tmp_path, monkeypatch):
    monkeypatch.setattr(mailbox_module, "MAX_HELD_PER_RECIPIENT", 2)
    monkeypatch.setattr(mailbox_module, "HELD_QUOTA_UNKNOWN_COUNT", 2)
    box = Mailbox(tmp_path / "mailbox.json")
    sender = b"\xcc" * 16
    box.hold(
        to=BOB, blob=b"a", message_id="r1", expires_at=5000.0, now=100.0,
        depositor=sender,
    )
    box.hold(
        to=BOB, blob=b"b", message_id="r2", expires_at=5000.0, now=101.0,
        depositor=sender,
    )
    # The recipient is full, but the sender's own oldest envelope is shed
    # to make room — refusing here would waste the sender's own slot.
    assert box.hold(
        to=BOB, blob=b"c", message_id="r3", expires_at=5000.0, now=102.0,
        depositor=sender,
    )
    assert [e.id for e in box.held_for(BOB)] == ["r2", "r3"]


def test_hold_recipient_cap_still_refuses_other_senders_mail(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(mailbox_module, "MAX_HELD_PER_RECIPIENT", 2)
    box = Mailbox(tmp_path / "mailbox.json")
    other = b"\xdd" * 16
    box.hold(
        to=BOB, blob=b"a", message_id="o1", expires_at=5000.0, now=100.0,
        depositor=other,
    )
    box.hold(
        to=BOB, blob=b"b", message_id="o2", expires_at=5000.0, now=101.0,
        depositor=other,
    )
    sender = b"\xcc" * 16
    with pytest.raises(MailboxFull):
        box.hold(
            to=BOB, blob=b"c", message_id="s1", expires_at=5000.0, now=102.0,
            depositor=sender,
        )
    # Nothing of the sender's was shed for a refused deposit.
    assert [e.id for e in box.held_for(BOB)] == ["o1", "o2"]


def test_held_depositor_and_usage_persist(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    sender = b"\xcc" * 16
    box.hold(
        to=BOB, blob=b"abc", message_id="m1", expires_at=5000.0, now=100.0,
        depositor=sender,
    )
    box.hold(
        to=ALICE, blob=b"abcd", message_id="m2", expires_at=5000.0, now=101.0,
        depositor=sender,
    )
    box.hold(to=BOB, blob=b"ab", message_id="m3", expires_at=5000.0, now=102.0)
    reloaded = Mailbox(path)
    assert [e.id for e in reloaded.held_list()] == ["m1", "m2", "m3"]
    assert reloaded.held_for(BOB)[0].depositor == sender.hex()
    assert reloaded.held_usage() == [
        {"sender": sender.hex(), "count": 2, "bytes": 7},
        {"sender": "", "count": 1, "bytes": 2},
    ]


def test_held_envelope_without_depositor_loads_as_unknown(tmp_path):
    path = tmp_path / "mailbox.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "outbox": [],
                "held": [
                    {
                        "id": "legacy",
                        "to": BOB.hex(),
                        "blob": base64.b64encode(b"old").decode("ascii"),
                        "stored_at": 100.0,
                        "expires_at": 5000.0,
                    }
                ],
                "seen": {},
            }
        ),
        encoding="utf-8",
    )
    box = Mailbox(path)
    assert box.held_for(BOB)[0].depositor == ""
    assert box.held_usage() == [{"sender": "", "count": 1, "bytes": 3}]


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


def test_held_list_returns_all_oldest_first(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    box.hold(to=BOB, blob=b"a", message_id="second", expires_at=5000.0, now=100.0)
    box.hold(to=ALICE, blob=b"b", message_id="first", expires_at=5000.0, now=50.0)
    box.hold(to=BOB, blob=b"c", message_id="third", expires_at=5000.0, now=150.0)
    assert [e.id for e in box.held_list()] == ["first", "second", "third"]


def test_resolve_held_full_id_and_prefix(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    full_id = "a" * 32
    box.hold(to=BOB, blob=b"a", message_id=full_id, expires_at=5000.0)
    box.hold(to=BOB, blob=b"b", message_id="shared-alpha", expires_at=5000.0)
    box.hold(to=BOB, blob=b"c", message_id="shared-beta", expires_at=5000.0)
    assert box.resolve_held(full_id).id == full_id
    assert box.resolve_held(full_id[:8]).id == full_id
    assert box.resolve_held("shared-a").id == "shared-alpha"
    with pytest.raises(MailboxError, match="unknown held message"):
        box.resolve_held("no-such-id")
    with pytest.raises(MailboxError, match="2 held messages match prefix"):
        box.resolve_held("shared-")
    with pytest.raises(MailboxError, match="must not be empty"):
        box.resolve_held("  ")


def test_resolve_held_mixed_case_and_long_ids(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    box.hold(to=BOB, blob=b"a", message_id="AbC123XyZ", expires_at=5000.0)
    long_id = "q" * 32 + "r" * 32
    box.hold(to=BOB, blob=b"b", message_id=long_id, expires_at=5000.0)
    # The stored spelling resolves, and so does a differently-cased prefix.
    assert box.resolve_held("AbC123XyZ").id == "AbC123XyZ"
    assert box.resolve_held("abc123").id == "AbC123XyZ"
    # A 32-character reference is a prefix when no exact id matches it.
    assert box.resolve_held("q" * 32).id == long_id


def test_drop_held_tombstone_blocks_redeposit(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    assert box.hold(to=BOB, blob=b"a", message_id="m1", expires_at=5000.0, now=100.0)
    assert box.drop_held("m1", tombstone=True) is True
    # A sender retrying its deposit gets a refusal, not a fresh slot.
    assert (
        box.hold(to=BOB, blob=b"a", message_id="m1", expires_at=5000.0, now=200.0)
        is False
    )
    assert box.counts()["held"] == 0
    # The refusal survives a reload, and lifts once the tombstone expires.
    reloaded = Mailbox(path)
    assert (
        reloaded.hold(
            to=BOB, blob=b"a", message_id="m1", expires_at=5000.0, now=200.0
        )
        is False
    )
    assert (
        reloaded.hold(
            to=BOB, blob=b"a", message_id="m1", expires_at=9000.0, now=6000.0
        )
        is True
    )


def test_drop_held_without_tombstone_allows_redeposit(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    assert box.hold(to=BOB, blob=b"a", message_id="m1", expires_at=5000.0, now=100.0)
    assert box.drop_held("m1") is True
    assert box.hold(to=BOB, blob=b"a", message_id="m1", expires_at=5000.0, now=200.0)


# -- prune / seen / acks ---------------------------------------------------------


def test_prune_expires_everything(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    box.enqueue(to=BOB, text="old", ttl=60.0, now=100.0)
    box.hold(to=BOB, blob=b"x", message_id="held-old", expires_at=150.0, now=100.0)
    box.mark_seen("seen-old", now=100.0)
    box.hold(to=BOB, blob=b"y", message_id="dropped-old", expires_at=150.0, now=100.0)
    assert box.drop_held("dropped-old", tombstone=True) is True
    removed = box.prune(now=100.0 + 31 * 24 * 60 * 60)
    assert removed == {
        "outbox": 1,
        "held": 1,
        "seen": 1,
        "sent": 0,
        "receipts": 0,
        "dropped_held": 1,
        "retracted": 0,
        "sent_retracted": 0,
    }
    assert box.counts() == {
        "outbox": 0,
        "held": 0,
        "seen": 0,
        "sent": 0,
        "receipts": 0,
        "retracted": 0,
        "sent_retracted": 0,
    }


def _enqueue_group_edit_leg(box, *, leg_id, to_hex, edit_id, group, ttl):
    """One leg of a queued group edit, for the prune/cancel tests."""
    return box.enqueue(
        to=bytes.fromhex(to_hex),
        text="fixed here",
        group=group,
        edit={
            "target": "leg-1",
            "text": "fixed here",
            "edit_id": edit_id,
            "sent_at": 100.0,
            "payload_b64": base64.b64encode(b"payload").decode("ascii"),
        },
        ttl=ttl,
        now=100.0,
        message_id=leg_id,
    )


def _edit_group():
    return {
        "id": "aa" * 16,
        "name": "crew",
        "creator": "bb" * 16,
        "members": ["cc" * 16, "dd" * 16, "ee" * 16],
    }


def test_prune_drops_siblings_of_expired_group_edit_leg(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    group = _edit_group()
    _enqueue_group_edit_leg(
        box, leg_id="leg-alice", to_hex="cc" * 16, edit_id="edit-1",
        group=group, ttl=60.0,
    )
    _enqueue_group_edit_leg(
        box, leg_id="leg-bob", to_hex="dd" * 16, edit_id="edit-1",
        group=group, ttl=3600.0,
    )
    box.enqueue(
        to=BOB, text="keep me", ttl=3600.0, now=100.0, message_id="plain"
    )
    # Only the first leg expired, but the edit can no longer complete
    # its fan-out: the still-live sibling goes with it.
    removed = box.prune(now=200.0)
    assert removed["outbox"] == 2
    assert [e.id for e in box.outbox_list()] == ["plain"]


def test_cancel_aborts_deferred_local_apply(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    group = _edit_group()
    _enqueue_group_edit_leg(
        box, leg_id="leg-alice", to_hex="cc" * 16, edit_id="edit-1",
        group=group, ttl=3600.0,
    )
    _enqueue_group_edit_leg(
        box, leg_id="leg-bob", to_hex="dd" * 16, edit_id="edit-1",
        group=group, ttl=3600.0,
    )
    box.add_pending_local_edit(
        {
            "edit_id": "edit-1",
            "target": "leg-1",
            "text": "fixed here",
            "sent_at": 100.0,
            "peer_hex": "cc" * 16,
        }
    )
    box.add_pending_local_edit(
        {
            "edit_id": "edit-2",
            "target": "leg-2",
            "text": "other edit",
            "sent_at": 101.0,
            "peer_hex": "dd" * 16,
        }
    )
    # The record survives a reload: the deferral is durable.
    reloaded = Mailbox(tmp_path / "mailbox.json")
    assert [r["edit_id"] for r in reloaded.pending_local_edits()] == [
        "edit-1",
        "edit-2",
    ]
    assert box.remove_outbox("leg-alice", abort_group_edit=True) is True
    assert box.outbox_list() == []
    # The aborted edit's apply goes with it; the other stands.
    assert [r["edit_id"] for r in box.pending_local_edits()] == ["edit-2"]


def test_prune_aborts_deferred_local_apply(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    group = _edit_group()
    _enqueue_group_edit_leg(
        box, leg_id="leg-alice", to_hex="cc" * 16, edit_id="edit-1",
        group=group, ttl=60.0,
    )
    _enqueue_group_edit_leg(
        box, leg_id="leg-bob", to_hex="dd" * 16, edit_id="edit-1",
        group=group, ttl=3600.0,
    )
    box.add_pending_local_edit(
        {
            "edit_id": "edit-1",
            "target": "leg-1",
            "text": "fixed here",
            "sent_at": 100.0,
            "peer_hex": "cc" * 16,
        }
    )
    box.add_pending_local_edit(
        {
            "edit_id": "edit-2",
            "target": "leg-2",
            "text": "other edit",
            "sent_at": 101.0,
            "peer_hex": "dd" * 16,
        }
    )
    removed = box.prune(now=200.0)
    assert removed["outbox"] == 2
    assert [r["edit_id"] for r in box.pending_local_edits()] == ["edit-2"]


def test_aborted_edit_refuses_later_pending_local_edit(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    group = _edit_group()
    _enqueue_group_edit_leg(
        box, leg_id="leg-alice", to_hex="cc" * 16, edit_id="edit-1",
        group=group, ttl=3600.0,
    )
    _enqueue_group_edit_leg(
        box, leg_id="leg-bob", to_hex="dd" * 16, edit_id="edit-1",
        group=group, ttl=3600.0,
    )
    record = {
        "edit_id": "edit-1",
        "target": "leg-1",
        "text": "fixed here",
        "sent_at": 100.0,
        "peer_hex": "cc" * 16,
    }
    # Cancelling the edit aborts it before any record exists: a
    # record inserted afterwards for the same edit is refused, so a
    # cancellation that landed between a timed-out sibling check and
    # the insertion cannot resurrect the edit.
    assert box.remove_outbox("leg-alice", abort_group_edit=True) is True
    assert box.is_local_edit_aborted("edit-1") is True
    assert box.add_pending_local_edit(record) is False
    assert box.pending_local_edits() == []
    # Other edits are unaffected, and the mark survives a reload.
    assert box.add_pending_local_edit(
        {
            "edit_id": "edit-2",
            "target": "leg-2",
            "text": "other edit",
            "sent_at": 101.0,
            "peer_hex": "dd" * 16,
        }
    ) is True
    reloaded = Mailbox(tmp_path / "mailbox.json")
    assert reloaded.is_local_edit_aborted("edit-1") is True
    assert [r["edit_id"] for r in reloaded.pending_local_edits()] == [
        "edit-2"
    ]


def test_aborted_local_edit_mark_expires_with_prune(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    box.abort_pending_local_edit("edit-1", now=1000.0)
    assert box.is_local_edit_aborted("edit-1", now=1000.0) is True
    # Still live inside its retention; gone once it expires.
    box.prune(now=1000.0 + RETRACTED_RETENTION - 1.0)
    assert box.is_local_edit_aborted("edit-1", now=1000.0) is True
    box.prune(now=1000.0 + RETRACTED_RETENTION)
    assert box.is_local_edit_aborted("edit-1", now=1000.0) is False
    # Malformed marks are dropped on parse, not loaded.
    path = tmp_path / "mailbox.json"
    raw = json.loads(path.read_text())
    raw["aborted_local_edits"] = {
        "edit-1": 1000.0 + RETRACTED_RETENTION,
        "bad": "soon",
    }
    path.write_text(json.dumps(raw))
    reloaded = Mailbox(path)
    assert reloaded.is_local_edit_aborted("edit-1", now=1000.0) is True
    assert reloaded.is_local_edit_aborted("bad", now=1000.0) is False


def test_drop_pending_local_edits_by_text_marks_aborted(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    box.add_pending_local_edit(
        {
            "edit_id": "edit-1",
            "target": "leg-1",
            "text": "fixed here",
            "sent_at": 100.0,
            "peer_hex": "cc" * 16,
        }
    )
    # A wrong replacement text does not match the record.
    box.drop_pending_local_edits_by_text("other text", 100.0)
    assert box.pending_local_edits() != []
    assert box.is_local_edit_aborted("edit-1") is False
    # The corrupt-leg fallback identity drops the record and marks
    # the edit aborted, so a later insertion for it is refused.
    box.drop_pending_local_edits_by_text("fixed here", 100.0)
    assert box.pending_local_edits() == []
    assert box.is_local_edit_aborted("edit-1") is True
    assert box.add_pending_local_edit(
        {
            "edit_id": "edit-1",
            "target": "leg-1",
            "text": "fixed here",
            "sent_at": 100.0,
            "peer_hex": "cc" * 16,
        }
    ) is False


def test_prune_drops_siblings_of_expired_corrupt_group_edit_leg(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    group = _edit_group()
    _enqueue_group_edit_leg(
        box, leg_id="leg-alice", to_hex="cc" * 16, edit_id="edit-1",
        group=group, ttl=60.0,
    )
    _enqueue_group_edit_leg(
        box, leg_id="leg-bob", to_hex="dd" * 16, edit_id="edit-1",
        group=group, ttl=3600.0,
    )
    # Corrupt the first leg's stored payload: validation drops the
    # edit, but the entry keeps the edit id as a hint, so the prune
    # still knows which edit the expired leg belongs to.
    path = tmp_path / "mailbox.json"
    raw = json.loads(path.read_text())
    for stored in raw["outbox"]:
        if stored["id"] == "leg-alice":
            stored["edit"]["payload_b64"] = "!!!not-base64"
    path.write_text(json.dumps(raw))
    reloaded = Mailbox(path)
    assert reloaded.get_outbox("leg-alice").edit is None
    assert reloaded.get_outbox("leg-alice").group_edit_id == "edit-1"
    removed = reloaded.prune(now=200.0)
    assert removed["outbox"] == 2
    assert reloaded.outbox_list() == []


def test_remove_outbox_abort_group_edit_drops_siblings(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    group = _edit_group()
    _enqueue_group_edit_leg(
        box, leg_id="leg-alice", to_hex="cc" * 16, edit_id="edit-1",
        group=group, ttl=3600.0,
    )
    _enqueue_group_edit_leg(
        box, leg_id="leg-bob", to_hex="dd" * 16, edit_id="edit-1",
        group=group, ttl=3600.0,
    )
    box.enqueue(
        to=BOB, text="keep me", ttl=3600.0, now=100.0, message_id="plain"
    )
    assert box.remove_outbox("leg-alice", abort_group_edit=True) is True
    assert [e.id for e in box.outbox_list()] == ["plain"]


def test_remove_outbox_leaves_siblings_without_abort_flag(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    group = _edit_group()
    _enqueue_group_edit_leg(
        box, leg_id="leg-alice", to_hex="cc" * 16, edit_id="edit-1",
        group=group, ttl=3600.0,
    )
    _enqueue_group_edit_leg(
        box, leg_id="leg-bob", to_hex="dd" * 16, edit_id="edit-1",
        group=group, ttl=3600.0,
    )
    box.enqueue(
        to=BOB, text="keep me", ttl=3600.0, now=100.0, message_id="plain"
    )
    # The default is a plain removal: a confirmed delivery removes
    # only its own leg, so the siblings must stay queued.
    assert box.remove_outbox("leg-alice") is True
    assert [e.id for e in box.outbox_list()] == ["leg-bob", "plain"]


def test_seen_and_pending_acks(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    assert box.is_seen("nope") is False
    box.mark_seen("yep")
    assert box.is_seen("yep") is True

    box.add_pending_acks("holder1", ["a", "b"])
    box.add_pending_acks("holder1", ["b", "c"])
    assert box.take_pending_acks("holder1") == ["a", "b", "c"]
    assert box.take_pending_acks("holder1") == []


def test_retraction_markers_persist_and_expire(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    sender = "aa" * 32
    assert box.is_retracted(sender, "msg-1") is False

    recorded_at = time.time()
    box.record_retraction(sender, "msg-1", now=recorded_at)
    assert box.is_retracted(sender, "msg-1") is True
    # A marker is keyed by (sender, id): another sender's message with
    # the same id, or another id from the same sender, is unaffected.
    assert box.is_retracted("bb" * 32, "msg-1") is False
    assert box.is_retracted(sender, "msg-2") is False
    assert box.counts()["retracted"] == 1

    # The marker survives a reload, and lifts once its retention ends.
    reloaded = Mailbox(path)
    assert reloaded.is_retracted(sender, "msg-1") is True
    assert (
        reloaded.is_retracted(
            sender, "msg-1", now=recorded_at + RETRACTED_RETENTION - 1
        )
        is True
    )
    assert (
        reloaded.is_retracted(sender, "msg-1", now=recorded_at + RETRACTED_RETENTION)
        is False
    )

    removed = reloaded.prune(now=recorded_at + RETRACTED_RETENTION)
    assert removed["retracted"] == 1
    assert reloaded.counts()["retracted"] == 0
    assert reloaded.is_retracted(sender, "msg-1") is False


def test_group_retraction_markers_persist_and_expire(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    sender = "aa" * 32
    group = "cc" * 16
    assert box.is_group_retracted(sender, group, 100.0) is False

    recorded_at = time.time()
    box.record_group_retraction(sender, group, 100.0, now=recorded_at)
    assert box.is_group_retracted(sender, group, 100.0) is True
    # Keyed by (sender, group, send time): any other triple is
    # unaffected, and a non-numeric send time never matches.
    assert box.is_group_retracted("bb" * 32, group, 100.0) is False
    assert box.is_group_retracted(sender, "dd" * 16, 100.0) is False
    assert box.is_group_retracted(sender, group, 101.0) is False
    assert box.is_group_retracted(sender, group, "soon") is False
    assert box.counts()["retracted"] == 1

    # The marker survives a reload, and lifts once its retention ends.
    reloaded = Mailbox(path)
    assert reloaded.is_group_retracted(sender, group, 100.0) is True
    assert (
        reloaded.is_group_retracted(
            sender, group, 100.0, now=recorded_at + RETRACTED_RETENTION - 1
        )
        is True
    )
    assert (
        reloaded.is_group_retracted(
            sender, group, 100.0, now=recorded_at + RETRACTED_RETENTION
        )
        is False
    )

    removed = reloaded.prune(now=recorded_at + RETRACTED_RETENTION)
    assert removed["retracted"] == 1
    assert reloaded.counts()["retracted"] == 0
    assert reloaded.is_group_retracted(sender, group, 100.0) is False


def test_group_pending_edit_markers_persist_and_expire(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    sender = "aa" * 32
    group = "cc" * 16
    assert box.pending_group_edit(sender, group, 100.0) is None

    recorded_at = time.time()
    assert (
        box.record_group_pending_edit(
            sender, group, 100.0, "fixed here", 150.0, now=recorded_at
        )
        is True
    )
    assert box.pending_group_edit(sender, group, 100.0) == {
        "text": "fixed here",
        "edited_at": 150.0,
    }
    # Keyed by (sender, group, send time): any other triple is
    # unaffected, and a non-numeric send time never matches.
    assert box.pending_group_edit("bb" * 32, group, 100.0) is None
    assert box.pending_group_edit(sender, "dd" * 16, 100.0) is None
    assert box.pending_group_edit(sender, group, 101.0) is None
    assert box.pending_group_edit(sender, group, "soon") is None

    # An older edit never rewinds the marker — and does not refresh it.
    assert (
        box.record_group_pending_edit(
            sender, group, 100.0, "older text", 140.0, now=recorded_at + 10
        )
        is False
    )
    assert box.pending_group_edit(sender, group, 100.0) == {
        "text": "fixed here",
        "edited_at": 150.0,
    }
    # A newer edit replaces it.
    assert (
        box.record_group_pending_edit(
            sender, group, 100.0, "newest text", 160.0, now=recorded_at + 20
        )
        is True
    )
    assert box.pending_group_edit(sender, group, 100.0) == {
        "text": "newest text",
        "edited_at": 160.0,
    }

    # The marker survives a reload, and lifts once its retention ends
    # (the newer record refreshed the expiry).
    reloaded = Mailbox(path)
    assert reloaded.pending_group_edit(sender, group, 100.0) == {
        "text": "newest text",
        "edited_at": 160.0,
    }
    assert (
        reloaded.pending_group_edit(
            sender, group, 100.0,
            now=recorded_at + 20 + RETRACTED_RETENTION - 1,
        )
        == {"text": "newest text", "edited_at": 160.0}
    )
    assert (
        reloaded.pending_group_edit(
            sender, group, 100.0,
            now=recorded_at + 20 + RETRACTED_RETENTION,
        )
        is None
    )

    reloaded.prune(now=recorded_at + 20 + RETRACTED_RETENTION)
    assert reloaded.pending_group_edit(sender, group, 100.0) is None


def test_group_pending_edit_markers_bound_each_sender(tmp_path):
    # Qodo warning on PR #344: one allow-listed peer flooding the pool
    # with edits for absent copies must not evict another sender's
    # live markers.  Each sender is bounded independently, before the
    # global cap applies.
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    flooder = "aa" * 32
    victim = "bb" * 32
    group = "cc" * 16
    box.record_group_pending_edit(
        victim, group, 100.0, "victim text", 150.0
    )
    for index in range(MAX_GROUP_PENDING_EDITS_PER_SENDER + 5):
        box.record_group_pending_edit(
            flooder,
            f"group-{index}",
            100.0 + index,
            f"flood {index}",
            150.0 + index,
        )
    # The victim's marker survives the flood.
    assert box.pending_group_edit(victim, group, 100.0) == {
        "text": "victim text",
        "edited_at": 150.0,
    }
    # The flooder keeps only their newest markers, up to their cap.
    kept = [
        index
        for index in range(MAX_GROUP_PENDING_EDITS_PER_SENDER + 5)
        if box.pending_group_edit(flooder, f"group-{index}", 100.0 + index)
        is not None
    ]
    assert len(kept) == MAX_GROUP_PENDING_EDITS_PER_SENDER
    assert kept == list(range(5, MAX_GROUP_PENDING_EDITS_PER_SENDER + 5))


def test_group_pending_edit_markers_drop_malformed_entries():
    parsed = mailbox_module._parse_pending_edit_markers(
        {
            "good": {
                "text": "fixed",
                "edited_at": 150.0,
                "until": time.time() + 3600,
            },
            "not-a-dict": "nope",
            "bad-text": {
                "text": 42,
                "edited_at": 150.0,
                "until": time.time() + 3600,
            },
            "bad-time": {
                "text": "fixed",
                "edited_at": "soon",
                "until": time.time() + 3600,
            },
            "bad-until": {
                "text": "fixed",
                "edited_at": 150.0,
                "until": "soon",
            },
            "far-future": {
                "text": "fixed",
                "edited_at": 150.0,
                "until": time.time() + 10**12,
            },
        }
    )
    assert list(parsed) == ["good"]
    assert parsed["good"]["text"] == "fixed"
    assert parsed["good"]["edited_at"] == 150.0


def test_sent_retraction_markers_persist_and_expire(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    assert box.is_sent_retracted("msg-1") is False

    recorded_at = time.time()
    box.record_sent_retraction("msg-1", now=recorded_at)
    assert box.is_sent_retracted("msg-1") is True
    # Keyed by message id alone: another id is unaffected.
    assert box.is_sent_retracted("msg-2") is False
    assert box.counts()["sent_retracted"] == 1
    # A sent marker never reads as a received one.
    assert box.is_retracted("aa" * 32, "msg-1") is False

    # The marker survives a reload, and lifts once its retention ends.
    reloaded = Mailbox(path)
    assert reloaded.is_sent_retracted("msg-1") is True
    assert (
        reloaded.is_sent_retracted(
            "msg-1", now=recorded_at + SENT_RETRACTED_RETENTION - 1
        )
        is True
    )
    assert (
        reloaded.is_sent_retracted(
            "msg-1", now=recorded_at + SENT_RETRACTED_RETENTION
        )
        is False
    )

    removed = reloaded.prune(now=recorded_at + SENT_RETRACTED_RETENTION)
    assert removed["sent_retracted"] == 1
    assert reloaded.counts()["sent_retracted"] == 0
    assert reloaded.is_sent_retracted("msg-1") is False


def test_sent_group_retraction_markers_persist_and_expire(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    group = "cc" * 16
    assert box.is_sent_group_retracted(group, 100.0) is False

    recorded_at = time.time()
    box.record_sent_group_retraction(group, 100.0, now=recorded_at)
    assert box.is_sent_group_retracted(group, 100.0) is True
    # Keyed by (group, send time): any other pair is unaffected, and
    # a non-numeric send time never matches.
    assert box.is_sent_group_retracted("dd" * 16, 100.0) is False
    assert box.is_sent_group_retracted(group, 101.0) is False
    assert box.is_sent_group_retracted(group, "soon") is False
    assert box.counts()["sent_retracted"] == 1
    # A sent group marker never reads as a received one.
    assert box.is_group_retracted("aa" * 32, group, 100.0) is False

    # The marker survives a reload, and lifts once its retention ends.
    reloaded = Mailbox(path)
    assert reloaded.is_sent_group_retracted(group, 100.0) is True
    assert (
        reloaded.is_sent_group_retracted(
            group, 100.0, now=recorded_at + SENT_RETRACTED_RETENTION - 1
        )
        is True
    )
    assert (
        reloaded.is_sent_group_retracted(
            group, 100.0, now=recorded_at + SENT_RETRACTED_RETENTION
        )
        is False
    )

    removed = reloaded.prune(now=recorded_at + SENT_RETRACTED_RETENTION)
    assert removed["sent_retracted"] == 1
    assert reloaded.counts()["sent_retracted"] == 0
    assert reloaded.is_sent_group_retracted(group, 100.0) is False


def test_retract_cleanup_blocked_markers_persist_and_expire(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    assert box.is_retract_cleanup_blocked("retract-1") is False

    recorded_at = time.time()
    box.record_retract_cleanup_blocked("retract-1", now=recorded_at)
    assert box.is_retract_cleanup_blocked("retract-1") is True
    # Keyed by retract id alone: another retract is unaffected.
    assert box.is_retract_cleanup_blocked("retract-2") is False

    # The marker survives a reload, and lifts once its retention ends.
    reloaded = Mailbox(path)
    assert reloaded.is_retract_cleanup_blocked("retract-1") is True
    assert (
        reloaded.is_retract_cleanup_blocked(
            "retract-1", now=recorded_at + SENT_RETRACTED_RETENTION - 1
        )
        is True
    )
    assert (
        reloaded.is_retract_cleanup_blocked(
            "retract-1", now=recorded_at + SENT_RETRACTED_RETENTION
        )
        is False
    )

    removed = reloaded.prune(now=recorded_at + SENT_RETRACTED_RETENTION)
    assert reloaded.is_retract_cleanup_blocked("retract-1") is False


def test_prune_blocks_cleanup_for_expired_group_retract_leg(tmp_path):
    # An expired leg of a queued group retract blocks the fan-out's
    # local cleanup, exactly like a refused leg: the member never got
    # the retract, so the sender's copies must survive even after the
    # surviving legs deliver.
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    now = time.time()
    box.enqueue(
        to=bytes.fromhex("cd" * 16),
        text="retraction",
        message_id="leg-bob",
        group={
            "id": "cc" * 16,
            "name": "crew",
            "creator": "ab" * 16,
            "members": ["ab" * 16, "cd" * 16],
        },
        sent_at=now - 100,
        now=now,
        expires_at=now + 10,
        retract={
            "target": "leg-1",
            "retract_id": "retract-1",
            "sent_at": now - 100,
            "target_author": "ab" * 16,
            "target_sent_at": 100.0,
            "payload_b64": "AAAA",
        },
    )
    entry = box.get_outbox("leg-bob")
    assert entry is not None
    # Past the leg's expiry, prune drops it — and records the block.
    removed = box.prune(now=now + 11)
    assert removed["outbox"] == 1
    assert box.get_outbox("leg-bob") is None
    assert box.is_retract_cleanup_blocked("retract-1") is True


def test_prune_blocks_cleanup_for_corrupt_group_retract_leg(tmp_path):
    # Qodo Warning on PR #706: a group-retract leg whose stored
    # payload did not survive carried no retract id, so pruning it
    # recorded no block and a valid sibling's delivery deleted the
    # sender's copies. The id now survives as a hint (see
    # OutboxEntry.group_retract_id), so the corrupt leg still blocks
    # the cleanup.
    import json

    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    now = time.time()
    box.enqueue(
        to=bytes.fromhex("cd" * 16),
        text="retraction",
        message_id="leg-bob",
        group={
            "id": "cc" * 16,
            "name": "crew",
            "creator": "ab" * 16,
            "members": ["ab" * 16, "cd" * 16],
        },
        sent_at=now - 100,
        now=now,
        expires_at=now + 10,
        retract={
            "target": "leg-1",
            "retract_id": "retract-1",
            "sent_at": now - 100,
            "target_author": "ab" * 16,
            "target_sent_at": 100.0,
            "payload_b64": "AAAA",
        },
    )
    # Corrupt the stored payload on disk: the signed bytes are gone,
    # but the retract id survives in the stored record.
    data = json.loads(path.read_text())
    [stored] = [e for e in data["outbox"] if e["id"] == "leg-bob"]
    stored["retract"] = {"retract_id": "retract-1"}
    path.write_text(json.dumps(data))

    reloaded = Mailbox(path)
    entry = reloaded.get_outbox("leg-bob")
    assert entry is not None
    assert entry.kind == "retract"
    assert entry.retract is None
    assert entry.group_retract_id == "retract-1"
    removed = reloaded.prune(now=now + 11)
    assert removed["outbox"] == 1
    assert reloaded.get_outbox("leg-bob") is None
    assert reloaded.is_retract_cleanup_blocked("retract-1") is True


def test_retract_cleanup_blocked_eviction_keeps_markers_with_queued_legs(
    tmp_path, monkeypatch
):
    # Qodo Warning on PR #706: the cap eviction could drop a marker
    # whose retract still had a queued leg, so the surviving leg's
    # delivery deleted the sender's copies. Eviction now skips
    # markers with a queued leg.
    monkeypatch.setattr(mailbox_module, "MAX_RETRACT_CLEANUP_BLOCKED", 2)
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    now = time.time()
    box.enqueue(
        to=bytes.fromhex("cd" * 16),
        text="retraction",
        message_id="leg-bob",
        group={
            "id": "cc" * 16,
            "name": "crew",
            "creator": "ab" * 16,
            "members": ["ab" * 16, "cd" * 16],
        },
        sent_at=now - 100,
        now=now,
        expires_at=now + 1000,
        retract={
            "target": "leg-1",
            "retract_id": "retract-protected",
            "sent_at": now - 100,
            "target_author": "ab" * 16,
            "target_sent_at": 100.0,
            "payload_b64": "AAAA",
        },
    )
    # The protected marker expires first, but its leg is still queued.
    box.record_retract_cleanup_blocked("retract-protected", now=now)
    box.record_retract_cleanup_blocked("other-1", now=now + 1)
    box.record_retract_cleanup_blocked("other-2", now=now + 2)
    assert box.is_retract_cleanup_blocked("retract-protected", now=now) is True
    assert box.is_retract_cleanup_blocked("other-1", now=now) is False
    assert box.is_retract_cleanup_blocked("other-2", now=now) is True
    # And the plain case still evicts the earliest expiry.
    box.record_retract_cleanup_blocked("other-3", now=now + 3)
    assert box.is_retract_cleanup_blocked("other-2", now=now) is False
    assert box.is_retract_cleanup_blocked("other-3", now=now) is True
    assert box.is_retract_cleanup_blocked("retract-protected", now=now) is True


def test_retract_cleanup_blocked_read_and_write_time_out_on_busy_lock(
    tmp_path,
):
    # Qodo Warning on PR #706: the marker read and write waited on
    # the mailbox locks without a limit inside the deadline-bounded
    # sweep. Both now take a timeout and raise TimeoutError once the
    # budget is spent, like outbox_list.
    import threading

    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    held = threading.Event()
    release = threading.Event()

    def hold_lock():
        box._lock.acquire()
        held.set()
        release.wait(timeout=30)
        box._lock.release()

    holder = threading.Thread(target=hold_lock, daemon=True)
    holder.start()
    assert held.wait(timeout=5)
    try:
        with pytest.raises(TimeoutError):
            box.is_retract_cleanup_blocked("retract-1", timeout=0.0)
        with pytest.raises(TimeoutError):
            box.record_retract_cleanup_blocked("retract-1", timeout=0.0)
    finally:
        release.set()
        holder.join(timeout=5)
    assert box.is_retract_cleanup_blocked("retract-1") is False


def test_group_retraction_markers_are_capped(tmp_path, monkeypatch):
    monkeypatch.setattr(mailbox_module, "MAX_GROUP_RETRACTED", 2)
    box = Mailbox(tmp_path / "mailbox.json")
    sender = "aa" * 32
    box.record_group_retraction(sender, "cc" * 16, 100.0, now=1000.0)
    box.record_group_retraction(sender, "dd" * 16, 100.0, now=1001.0)
    assert box.counts()["retracted"] == 2
    # The third marker evicts the one expiring soonest.
    box.record_group_retraction(sender, "ee" * 16, 100.0, now=1002.0)
    assert box.counts()["retracted"] == 2
    assert box.is_group_retracted(sender, "cc" * 16, 100.0, now=1002.0) is False
    assert box.is_group_retracted(sender, "ee" * 16, 100.0, now=1002.0) is True
    # Re-recording a marker refreshes it instead of growing the set.
    box.record_group_retraction(sender, "dd" * 16, 100.0, now=2000.0)
    assert box.counts()["retracted"] == 2


def test_unknown_group_retraction_markers_do_not_evict_confirmed(
    tmp_path, monkeypatch
):
    # Regression test (CodeRabbit Major on PR #322): a flood of
    # unknown-group retracts must not evict a confirmed marker —
    # eviction at the cap spends the unknown pool first.
    monkeypatch.setattr(mailbox_module, "MAX_GROUP_RETRACTED", 3)
    box = Mailbox(tmp_path / "mailbox.json")
    sender = "aa" * 32
    other = "bb" * 32
    box.record_group_retraction(sender, "cc" * 16, 100.0, confirmed=True, now=1000.0)
    box.record_group_retraction(sender, "dd" * 16, 100.0, confirmed=True, now=1001.0)
    assert box.counts()["retracted"] == 2
    # Two unknown-group markers arrive: the cap spends the unknown
    # pool first, so both confirmed markers survive and the oldest
    # unknown marker gives way.
    box.record_group_retraction(other, "ee" * 16, 100.0, now=1002.0)
    box.record_group_retraction(other, "ff" * 16, 100.0, now=1003.0)
    assert box.counts()["retracted"] == 3
    assert box.is_group_retracted(sender, "cc" * 16, 100.0, now=1003.0) is True
    assert box.is_group_retracted(sender, "dd" * 16, 100.0, now=1003.0) is True
    assert box.is_group_retracted(other, "ee" * 16, 100.0, now=1003.0) is False
    assert box.is_group_retracted(other, "ff" * 16, 100.0, now=1003.0) is True
    # Promoting an unknown marker to confirmed moves it out of the
    # unknown pool, so a later flood cannot evict it either.
    box.record_group_retraction(other, "ff" * 16, 100.0, confirmed=True, now=1004.0)
    box.record_group_retraction(other, "gg" * 16, 100.0, now=1005.0)
    assert box.counts()["retracted"] == 3
    assert box.is_group_retracted(other, "ff" * 16, 100.0, now=1005.0) is True
    assert box.is_group_retracted(other, "gg" * 16, 100.0, now=1005.0) is False
    # Both pools survive a reload.
    reloaded = Mailbox(tmp_path / "mailbox.json")
    assert reloaded.is_group_retracted(sender, "cc" * 16, 100.0, now=1005.0) is True
    assert reloaded.is_group_retracted(other, "ff" * 16, 100.0, now=1005.0) is True


def test_unconfirmed_group_retraction_does_not_downgrade_confirmed(
    tmp_path, monkeypatch
):
    # A retried retract arrives unconfirmed: it refreshes the
    # confirmed marker instead of moving it to the unknown pool.
    monkeypatch.setattr(mailbox_module, "MAX_GROUP_RETRACTED", 2)
    box = Mailbox(tmp_path / "mailbox.json")
    sender = "aa" * 32
    box.record_group_retraction(sender, "cc" * 16, 100.0, confirmed=True, now=1000.0)
    box.record_group_retraction(sender, "cc" * 16, 100.0, now=2000.0)
    box.record_group_retraction("bb" * 32, "dd" * 16, 100.0, now=2001.0)
    box.record_group_retraction("bb" * 32, "ee" * 16, 100.0, now=2002.0)
    assert box.is_group_retracted(sender, "cc" * 16, 100.0, now=2002.0) is True


def test_retraction_markers_are_capped(tmp_path, monkeypatch):
    monkeypatch.setattr(mailbox_module, "MAX_RETRACTED", 3)
    box = Mailbox(tmp_path / "mailbox.json")
    sender = "aa" * 32
    box.record_retraction(sender, "m-1", now=1000.0)
    box.record_retraction(sender, "m-2", now=1001.0)
    box.record_retraction(sender, "m-3", now=1002.0)
    assert box.counts()["retracted"] == 3
    # The fourth marker evicts the one expiring soonest (m-1).
    box.record_retraction(sender, "m-4", now=1003.0)
    assert box.counts()["retracted"] == 3
    assert box.is_retracted(sender, "m-1", now=1003.0) is False
    assert box.is_retracted(sender, "m-4", now=1003.0) is True
    # Re-recording a marker refreshes it instead of growing the set.
    box.record_retraction(sender, "m-2", now=2000.0)
    assert box.counts()["retracted"] == 3


def test_confirmed_markers_survive_unknown_flood(tmp_path, monkeypatch):
    monkeypatch.setattr(mailbox_module, "MAX_RETRACTED", 3)
    box = Mailbox(tmp_path / "mailbox.json")
    sender = "aa" * 32
    other = "bb" * 32
    box.record_retraction(sender, "real", confirmed=True, now=1000.0)
    box.record_retraction(other, "junk-1", now=1001.0)
    box.record_retraction(other, "junk-2", now=1002.0)
    assert box.counts()["retracted"] == 3
    # A flood of unknown-ID markers evicts unknown markers, never the
    # confirmed one — capacity pressure must not invalidate a
    # retraction the sender saw succeed.
    box.record_retraction(other, "junk-3", now=1003.0)
    box.record_retraction(other, "junk-4", now=1004.0)
    assert box.counts()["retracted"] == 3
    assert box.is_retracted(sender, "real", now=1004.0) is True
    # A retried retract refreshes a confirmed marker, never downgrades
    # it: it still survives the next flood.
    box.record_retraction(sender, "real", now=2000.0)
    box.record_retraction(other, "junk-5", now=2001.0)
    box.record_retraction(other, "junk-6", now=2002.0)
    box.record_retraction(other, "junk-7", now=2003.0)
    assert box.is_retracted(sender, "real", now=2003.0) is True


def test_retraction_markers_survive_a_backward_clock_correction(tmp_path):
    # Regression test for the CodeRabbit Major: a marker is written as
    # now + RETRACTED_RETENTION, so if the wall clock moves backward
    # after the write, a reload must not drop the still-live marker —
    # only markers past the bounded skew tolerance are malformed.
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    sender = "aa" * 32
    now = time.time()
    # The clock stood an hour ahead when the first marker was recorded,
    # then moved back: its expiry sits past now + RETRACTED_RETENTION.
    box.record_retraction(sender, "kept", now=now + 3600.0)
    # Two days ahead is past the tolerance: malformed, not a correction.
    box.record_retraction(sender, "dropped", now=now + 2 * 24 * 60 * 60)
    assert box.is_retracted(sender, "kept", now=now) is True
    assert RETRACTED_CLOCK_SKEW_TOLERANCE == 24 * 60 * 60

    reloaded = Mailbox(path)
    assert reloaded.is_retracted(sender, "kept", now=now) is True
    assert reloaded.is_retracted(sender, "dropped", now=now) is False
    assert reloaded.counts()["retracted"] == 1


def test_confirmed_markers_evict_oldest_when_all_confirmed(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(mailbox_module, "MAX_RETRACTED", 2)
    box = Mailbox(tmp_path / "mailbox.json")
    sender = "aa" * 32
    box.record_retraction(sender, "m-1", confirmed=True, now=1000.0)
    box.record_retraction(sender, "m-2", confirmed=True, now=1001.0)
    assert box.counts()["retracted"] == 2
    # With no unknown marker left to spend, the cap still holds: the
    # earliest-expiry confirmed marker gives way.
    box.record_retraction(sender, "m-3", confirmed=True, now=1002.0)
    assert box.counts()["retracted"] == 2
    assert box.is_retracted(sender, "m-1", now=1002.0) is False
    assert box.is_retracted(sender, "m-2", now=1002.0) is True
    assert box.is_retracted(sender, "m-3", now=1002.0) is True


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


def test_seal_and_open_keep_kind():
    sender, recipient = make_identity_pair()
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-kind",
        text="are you there?",
        sent_at=4242.0,
        kind="ping",
    )
    message = open_message(recipient, blob)
    assert message["kind"] == "ping"


def test_seal_and_open_keep_contact_card():
    sender, recipient = make_identity_pair()
    card = {"name": "carol", "destination_hash": "cc" * 16}
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-card",
        text="meet carol",
        sent_at=4242.0,
        contact=card,
    )
    message = open_message(recipient, blob)
    assert message["contact"] == card
    # A malformed card is refused to seal, like a malformed pin.
    with pytest.raises(MailboxError):
        seal_message(
            sender,
            sender_name="alice",
            recipient_public_key=recipient.get_public_key(),
            message_id="msg-bad-card",
            text="meet carol",
            sent_at=4242.0,
            contact={"name": "carol", "destination_hash": "zz"},
        )


def test_seal_message_rejects_reaction_without_token():
    """A reaction dict without a token must not seal as a clear.

    clean_reaction turns an absent token into "" (a retraction), so
    sealing {"reaction_id": ...} with no token would emit a leg that
    clears the sender's reaction.
    """
    sender, recipient = make_identity_pair()
    with pytest.raises(MailboxError):
        seal_message(
            sender,
            sender_name="alice",
            recipient_public_key=recipient.get_public_key(),
            message_id="msg-no-token",
            text="👍",
            sent_at=4242.0,
            kind="reaction",
            reaction={"reaction_id": "r-1"},
        )


def test_seal_message_keeps_explicit_empty_reaction_token():
    """An explicit empty token is still a valid clear, not an error."""
    sender, recipient = make_identity_pair()
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-clear",
        text="reaction cleared",
        sent_at=4242.0,
        kind="reaction",
        reaction={"token": "", "reaction_id": "r-1"},
    )
    message = open_message(recipient, blob)
    assert message["reaction"] == {"token": "", "reaction_id": "r-1"}


def test_validated_sealed_message_drops_reaction_without_token():
    """A leg with a reaction id but no token drops the reaction field.

    The message itself still validates — only the malformed reaction
    payload is dropped, never the message.
    """
    from tau.mailbox import _validated_sealed_message

    payload = {
        "id": "m-1",
        "from": "aa" * 16,
        "from_name": "alice",
        "text": "👍",
        "sent_at": 4242.0,
        "reaction": {"reaction_id": "r-1"},
    }
    assert "reaction" not in _validated_sealed_message(payload)
    payload["reaction"] = {"token": "", "reaction_id": "r-1"}
    assert _validated_sealed_message(payload)["reaction"] == {
        "token": "",
        "reaction_id": "r-1",
    }


def test_seal_and_open_carries_retract():
    """A retract leg seals and opens with its retract id."""
    sender, recipient = make_identity_pair()
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="m-1",
        text="retraction",
        sent_at=4242.0,
        kind="retract",
        retract={"retract_id": "x-1"},
    )
    message = open_message(recipient, blob)
    assert message["kind"] == "retract"
    assert message["retract"] == {"retract_id": "x-1"}


def test_seal_message_rejects_retract_without_id():
    sender, recipient = make_identity_pair()
    with pytest.raises(MailboxError):
        seal_message(
            sender,
            sender_name="alice",
            recipient_public_key=recipient.get_public_key(),
            message_id="m-1",
            text="retraction",
            sent_at=4242.0,
            kind="retract",
            retract={},
        )


def test_seal_message_carries_group_retract_context():
    # A sealed group retract names the retracted copy by its author
    # and shared send time, so the recipient resolves its own leg
    # even though the target id only matches the author's sent record.
    from tau.mailbox import open_message

    sender, recipient = make_identity_pair()
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="leg-author",
        text="retraction",
        sent_at=4242.0,
        group={
            "id": "ab" * 16,
            "name": "crew",
            "creator": "cd" * 16,
            "members": ["ab" * 16, "cd" * 16],
        },
        kind="retract",
        retract={
            "retract_id": "r-1",
            "target_author": "ab" * 16,
            "target_sent_at": 100.0,
        },
    )
    leg = open_message(recipient, blob)
    assert leg["id"] == "leg-author"
    assert leg["kind"] == "retract"
    assert leg["retract"] == {
        "retract_id": "r-1",
        "target_author": "ab" * 16,
        "target_sent_at": 100.0,
    }
    assert leg["group"]["id"] == "ab" * 16
    with pytest.raises(MailboxError):
        seal_message(
            sender,
            sender_name="alice",
            recipient_public_key=recipient.get_public_key(),
            message_id="leg-author",
            text="retraction",
            sent_at=4242.0,
            kind="retract",
            retract={
                "retract_id": "r-1",
                "target_author": "not-a-hash",
                "target_sent_at": 100.0,
            },
        )


def test_validated_sealed_message_drops_retract_without_id():
    """A leg with a malformed retract drops the retract field.

    The message itself still validates — only the malformed retract
    payload is dropped, never the message.
    """
    from tau.mailbox import _validated_sealed_message

    payload = {
        "id": "m-1",
        "from": "aa" * 16,
        "from_name": "alice",
        "text": "retraction",
        "sent_at": 4242.0,
        "retract": {},
    }
    assert "retract" not in _validated_sealed_message(payload)
    payload["retract"] = {"retract_id": "x-1"}
    assert _validated_sealed_message(payload)["retract"] == {
        "retract_id": "x-1",
    }


def test_seal_and_open_carries_location():
    sender, recipient = make_identity_pair()
    pin = {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"}
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-pin",
        text="meet here",
        sent_at=4242.0,
        location=pin,
    )
    message = open_message(recipient, blob)
    assert message["location"] == pin
    # A sealed copy without a pin carries none, like a plain message.
    plain = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-plain-pin",
        text="hello",
        sent_at=4242.0,
    )
    assert open_message(recipient, plain)["location"] is None


def test_seal_and_open_carries_announcements_setting():
    sender, recipient = make_identity_pair()
    group = {
        "id": "ab" * 16,
        "name": "weekend",
        "creator": "cd" * 16,
        "members": ["ab" * 16, "cd" * 16],
        "announcements_only": True,
        "announcements_sent_at": 4300.0,
    }
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-ann",
        text="announcement",
        sent_at=4242.0,
        group=group,
    )
    message = open_message(recipient, blob)
    assert message["group"]["announcements_only"] is True
    assert message["group"]["announcements_sent_at"] == 4300.0
    # A sealed copy without the setting carries none, like a plain message.
    plain = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-plain-ann",
        text="hello",
        sent_at=4242.0,
        group={k: v for k, v in group.items() if not k.startswith("announcements")},
    )
    assert "announcements_only" not in open_message(recipient, plain)["group"]


def test_seal_and_open_announcements_flag_always_carries_a_stamp():
    # A group that was never toggled has no stamp, so the flag rides
    # with 0.0 inside the sealed envelope too — the same wire
    # contract as the direct path.
    sender, recipient = make_identity_pair()
    group = {
        "id": "ab" * 16,
        "name": "weekend",
        "creator": "cd" * 16,
        "members": ["ab" * 16, "cd" * 16],
        "announcements_only": False,
    }
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-ann-stamp",
        text="open again",
        sent_at=4242.0,
        group=group,
    )
    message = open_message(recipient, blob)
    assert message["group"]["announcements_only"] is False
    assert message["group"]["announcements_sent_at"] == 0.0


def test_seal_and_open_carries_poll_vote():
    sender, recipient = make_identity_pair()
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-vote",
        text="voted: Sunday",
        sent_at=4242.0,
        poll_vote={
            "target": {"author": "cd" * 16, "sent_at": 4200.0},
            "option": 1,
        },
    )
    message = open_message(recipient, blob)
    assert message["poll_vote"] == {
        "target": {"author": "cd" * 16, "sent_at": 4200.0},
        "option": 1,
    }
    # A contradictory envelope drops all poll fields, like the
    # direct path: a vote and a close cannot both be true.
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-vote-close",
        text="voted: Sunday",
        sent_at=4242.0,
        poll_vote={
            "target": {"author": "cd" * 16, "sent_at": 4200.0},
            "option": 1,
        },
        poll_close={"author": "cd" * 16, "sent_at": 4200.0},
    )
    message = open_message(recipient, blob)
    assert "poll_vote" not in message
    assert "poll_close" not in message


def test_seal_and_open_default_kind_is_chat():
    sender, recipient = make_identity_pair()
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-chat",
        text="hello",
        sent_at=4242.0,
    )
    assert open_message(recipient, blob)["kind"] == "chat"


def test_seal_and_open_carries_edited_at():
    # A sealed copy of an edited leg carries the edit's send time
    # inside the signed envelope, so a relayed copy initializes the
    # recipient's edit ordering state exactly like a direct one.
    sender, recipient = make_identity_pair()
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-edited",
        text="fixed here",
        sent_at=4242.0,
        edited_at=4300.0,
    )
    message = open_message(recipient, blob)
    assert message["edited_at"] == 4300.0
    plain = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-plain",
        text="hello",
        sent_at=4242.0,
    )
    assert open_message(recipient, plain)["edited_at"] is None


def test_seal_and_open_roundtrip_with_reply():
    sender, recipient = make_identity_pair()
    reply_to = {"id": "a" * 32, "excerpt": excerpt_hash("original thought")}
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-9",
        text="seconded",
        sent_at=4242.0,
        in_reply_to=reply_to,
    )
    message = open_message(recipient, blob)
    assert message["id"] == "msg-9"
    assert message["in_reply_to"] == reply_to


def test_seal_and_open_without_reply_has_no_reference():
    sender, recipient = make_identity_pair()
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-10",
        text="standalone",
        sent_at=4242.0,
    )
    assert open_message(recipient, blob)["in_reply_to"] is None


def test_enqueue_keeps_reply_through_reload(tmp_path):
    mailbox = Mailbox(tmp_path / "mailbox.json")
    reply_to = {"id": "a" * 32, "excerpt": excerpt_hash("original thought")}
    entry = mailbox.enqueue(
        to=bytes.fromhex("ab" * 16),
        text="seconded",
        message_id="msg-11",
        in_reply_to=reply_to,
    )
    assert entry.in_reply_to == reply_to
    reloaded = Mailbox(tmp_path / "mailbox.json")
    assert reloaded.get_outbox("msg-11").in_reply_to == reply_to


def test_outbox_entry_drops_corrupt_reply_instead_of_failing(tmp_path):
    mailbox = Mailbox(tmp_path / "mailbox.json")
    mailbox.enqueue(
        to=bytes.fromhex("ab" * 16), text="seconded", message_id="msg-12"
    )
    raw = json.loads((tmp_path / "mailbox.json").read_text())
    raw["outbox"][0]["in_reply_to"] = {"id": ""}
    (tmp_path / "mailbox.json").write_text(json.dumps(raw))
    reloaded = Mailbox(tmp_path / "mailbox.json")
    assert reloaded.get_outbox("msg-12").in_reply_to is None


def test_seal_and_open_roundtrip_with_none_returning_load(
    load_public_key_without_return,
):
    # RNS 1.4.1's load_public_key returns None even on success; sealing
    # and opening must still work there.
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
    assert load_public_key(box, recipient.get_public_key())
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


def test_transaction_timeout_when_lock_held(tmp_path):
    # A stalled lock holder (another process holding the flock) must
    # not block a bounded transaction past its budget.
    fcntl = pytest.importorskip("fcntl")
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    box.enqueue(to=BOB, text="queued", now=time.time())
    lock_path = tmp_path / "mailbox.json.lock"
    handle = open(lock_path, "a")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            with box.transaction(timeout=0.1):
                raise AssertionError("must not acquire a held lock")
        assert time.monotonic() - started < 2.0
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
    # Once the holder releases, the same bounded transaction succeeds.
    with box.transaction(timeout=1.0):
        assert box.get_outbox(box.outbox_list()[0].id) is not None


def test_transaction_shares_timeout_across_both_locks(tmp_path):
    # The in-process lock and the flock share one deadline: a
    # transaction that waits on both must not take up to twice its
    # timeout.
    fcntl = pytest.importorskip("fcntl")
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    box.enqueue(to=BOB, text="queued", now=time.time())
    lock_path = tmp_path / "mailbox.json.lock"
    handle = open(lock_path, "a")
    acquired = threading.Event()

    def hold_in_process_lock():
        box._lock.acquire()
        acquired.set()
        try:
            time.sleep(0.4)
        finally:
            box._lock.release()

    holder = threading.Thread(target=hold_in_process_lock, daemon=True)
    holder.start()
    assert acquired.wait(5.0)
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            with box.transaction(timeout=0.5):
                raise AssertionError("must not acquire the held locks")
        elapsed = time.monotonic() - started
        # 0.4s on the in-process lock leaves 0.1s for the flock: the
        # shared deadline expires at ~0.5s, not 0.4 + 0.5.
        assert elapsed < 0.75, "the two lock waits must share one deadline"
        assert elapsed >= 0.4
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
    holder.join(timeout=5.0)


def test_outbox_list_timeout_when_lock_held(tmp_path):
    # A bounded outbox read raises instead of waiting out a stalled
    # in-process lock holder.
    box = Mailbox(tmp_path / "mailbox.json")
    box.enqueue(to=BOB, text="queued", now=time.time())
    acquired = threading.Event()
    release = threading.Event()

    def hold_lock():
        box._lock.acquire()
        acquired.set()
        try:
            release.wait(5.0)
        finally:
            box._lock.release()

    holder = threading.Thread(target=hold_lock, daemon=True)
    holder.start()
    assert acquired.wait(5.0)
    try:
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            box.outbox_list(timeout=0.1)
        assert time.monotonic() - started < 2.0
    finally:
        release.set()
        holder.join(timeout=5.0)
    assert len(box.outbox_list()) == 1


def test_enqueue_caps_entry_expiry_at_message_expiry(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(
        to=BOB, text="vanish", now=1000.0, expires_at=1060.0, message_id="exp-1"
    )
    # The queue entry expires at the message deadline, not the 7-day TTL.
    assert entry.expires_at == 1060.0
    assert entry.message_expires_at == 1060.0

    reloaded = Mailbox(tmp_path / "mailbox.json")
    assert reloaded.outbox_list()[0].message_expires_at == 1060.0


def test_enqueue_rejects_invalid_message_expiry(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    with pytest.raises(MailboxError):
        box.enqueue(to=BOB, text="bad", expires_at="soon")
    with pytest.raises(MailboxError):
        box.enqueue(to=BOB, text="bad", expires_at=float("inf"))


def test_enqueue_without_message_expiry_keeps_ttl(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(to=BOB, text="stays", now=1000.0, message_id="plain-1")
    assert entry.expires_at > 1000.0 + 3600
    assert entry.message_expires_at is None

    reloaded = Mailbox(tmp_path / "mailbox.json")
    assert reloaded.outbox_list()[0].message_expires_at is None


def test_scheduled_entry_not_due_before_send_after(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(
        to=BOB,
        text="later",
        now=1000.0,
        send_after=2000.0,
        message_id="sched-1",
    )
    assert entry.send_after == 2000.0
    assert entry.is_scheduled(now=1500.0) is True
    assert entry.is_due(now=1500.0) is False
    assert entry.is_due(now=1999.0) is False
    assert entry.is_scheduled(now=2000.0) is False
    assert entry.is_due(now=2000.0) is True
    assert [e.id for e in box.due_outbox(now=1500.0)] == []
    assert [e.id for e in box.due_outbox(now=2000.0)] == [entry.id]


def test_scheduled_entry_survives_reload(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    entry = box.enqueue(
        to=BOB,
        text="later",
        now=1000.0,
        send_after=5000.0,
        message_id="sched-2",
    )
    reloaded = Mailbox(tmp_path / "mailbox.json")
    stored = reloaded.get_outbox(entry.id)
    assert stored.send_after == 5000.0
    assert stored.to_dict()["send_after"] == 5000.0
    assert stored.is_due(now=4999.0) is False


def test_enqueue_stretches_entry_ttl_past_send_after(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    # Ten days out is past the 7-day queue TTL: the entry must still
    # be in the outbox when its time comes.
    entry = box.enqueue(
        to=BOB,
        text="far",
        now=1000.0,
        send_after=1000.0 + 10 * 86400,
        message_id="sched-3",
    )
    assert entry.expires_at == 1000.0 + 10 * 86400 + 3600.0


def test_enqueue_scheduled_ttl_stretch_never_passes_message_expiry(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    # The message expires 20 minutes after its scheduled time: the
    # entry must die with the message, not live on for the hour of
    # TTL slack the stretch would otherwise add.
    entry = box.enqueue(
        to=BOB,
        text="short-lived",
        now=1000.0,
        send_after=2000.0,
        expires_at=3200.0,
        message_id="sched-5",
    )
    assert entry.expires_at == 3200.0


def test_enqueue_rejects_bad_send_after(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    with pytest.raises(MailboxError, match="before the message expiry"):
        box.enqueue(
            to=BOB,
            text="bad",
            now=1000.0,
            expires_at=1060.0,
            send_after=2000.0,
        )
    with pytest.raises(MailboxError, match="no delivery window"):
        box.enqueue(
            to=BOB, text="bad", now=1000.0, send_after=1000.0 + MAX_MAIL_TTL
        )
    with pytest.raises(MailboxError, match="finite number"):
        box.enqueue(to=BOB, text="bad", now=1000.0, send_after="tomorrow")
    with pytest.raises(MailboxError, match="finite number"):
        box.enqueue(to=BOB, text="bad", now=1000.0, send_after=float("inf"))


def test_reload_ignores_corrupt_send_after(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    entry = box.enqueue(
        to=BOB,
        text="later",
        now=1000.0,
        send_after=5000.0,
        message_id="sched-4",
    )
    raw = json.loads(path.read_text())
    for item in raw["outbox"]:
        item["send_after"] = "tomorrow"
    path.write_text(json.dumps(raw))

    reloaded = Mailbox(path)
    stored = reloaded.get_outbox(entry.id)
    assert stored.send_after is None
    assert stored.is_due(now=1000.0) is True


def test_seal_and_open_roundtrip_with_expiry():
    sender, recipient = make_identity_pair()
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-exp",
        text="read fast",
        sent_at=4242.0,
        expires_at=5000.0,
    )
    message = open_message(recipient, blob)
    assert message["id"] == "msg-exp"
    assert message["expires_at"] == 5000.0


def test_seal_and_open_roundtrip_without_expiry_has_none():
    sender, recipient = make_identity_pair()
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="msg-plain",
        text="stays",
        sent_at=4242.0,
    )
    assert open_message(recipient, blob)["expires_at"] is None


def test_seal_rejects_invalid_expiry():
    sender, recipient = make_identity_pair()
    with pytest.raises(MailboxError):
        seal_message(
            sender,
            sender_name="alice",
            recipient_public_key=recipient.get_public_key(),
            message_id="msg-bad",
            text="bad",
            sent_at=4242.0,
            expires_at="soon",
        )


def test_enqueue_group_poll_fields_survive_reload(tmp_path):
    # A queued poll leg re-sends the poll payload on retry, so the
    # poll fields must survive the on-disk round trip like sent_at.
    from tau.mailbox import OutboxEntry

    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    entry = box.enqueue(
        to=BOB,
        text="which day?",
        now=2000.0,
        sent_at=100.0,
        group={
            "id": "cd" * 16,
            "name": "crew",
            "creator": "aa" * 16,
            "members": ["aa" * 16, "bb" * 16],
        },
        poll_options=["Saturday", "Sunday"],
    )
    assert entry.poll_options == ["Saturday", "Sunday"]
    assert entry.poll_vote is None
    assert entry.poll_close is None
    assert entry.to_dict()["poll_options"] == ["Saturday", "Sunday"]

    reloaded = Mailbox(path)
    stored = reloaded.outbox_list()[0]
    assert stored.poll_options == ["Saturday", "Sunday"]
    assert OutboxEntry.from_dict(stored.to_dict()).poll_options == [
        "Saturday",
        "Sunday",
    ]

    # A queued pin survives the outbox round-trip like a quote does.
    pin = {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"}
    pinned = box.enqueue(to=BOB, text="meet here", now=2002.0, location=pin)
    assert pinned.location == pin
    assert pinned.to_dict()["location"] == pin
    reloaded = Mailbox(path)
    stored = [e for e in reloaded.outbox_list() if e.id == pinned.id][0]
    assert stored.location == pin
    assert OutboxEntry.from_dict(stored.to_dict()).location == pin
    # A malformed pin on an already-queued entry drops instead of
    # making the outbox unreadable.
    raw = pinned.to_dict()
    raw["location"] = {"lat": 91, "lon": 0}
    assert OutboxEntry.from_dict(raw).location is None

    vote = box.enqueue(
        to=BOB,
        text="voted: Sunday",
        now=2001.0,
        group={
            "id": "cd" * 16,
            "name": "crew",
            "creator": "aa" * 16,
            "members": ["aa" * 16, "bb" * 16],
        },
        poll_vote={"target": {"author": "aa" * 16, "sent_at": 100.0}, "option": 1},
    )
    assert vote.poll_vote == {
        "target": {"author": "aa" * 16, "sent_at": 100.0},
        "option": 1,
    }
    assert Mailbox(path).outbox_list()[1].poll_vote == vote.poll_vote


def test_enqueue_rejects_two_poll_fields(tmp_path):
    path = tmp_path / "mailbox.json"
    box = Mailbox(path)
    with pytest.raises(MailboxError):
        box.enqueue(
            to=BOB,
            text="which day?",
            poll_options=["Saturday", "Sunday"],
            poll_vote={
                "target": {"author": "aa" * 16, "sent_at": 100.0},
                "option": 0,
            },
        )
    with pytest.raises(MailboxError):
        box.enqueue(to=BOB, text="which day?", poll_options=["only one"])
