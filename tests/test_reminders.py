"""Tests for message reminders: the local store, the `tau remind` CLI,
and the mail-loop firing pass."""

from __future__ import annotations

import json

import pytest
import RNS

from tau import cli
from tau.config import TauConfig
from tau.groups import GroupStore
from tau.identity import destination_hash
from tau.inbox import Inbox
from tau.peers import PeerRegistry
from tau.reminders import (
    MAX_REMINDER_DELAY,
    MAX_REMINDERS,
    ReminderError,
    ReminderStore,
    fire_due_reminders,
    parse_remind_at,
)

ALICE_HASH = "aa" * 16
BOB_HASH = "bb" * 16
GROUP_ID = "cc" * 16


def _seed_peer(tmp_path, name="alice"):
    """A data dir with one known peer; returns (data_dir, peer hash)."""
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    identity = RNS.Identity()
    peer_hash = destination_hash(identity).hex()
    registry = PeerRegistry(config.peers_path)
    registry.upsert(
        destination_hash(identity), identity.get_public_key(), name=name, now=100.0
    )
    registry.save()
    return data_dir, peer_hash


def _seed_group(tmp_path, name="crew"):
    """A data dir with one known group; returns (data_dir, group id)."""
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    store = GroupStore(config.groups_path)
    group = store.create(name, [ALICE_HASH, BOB_HASH], created_by=ALICE_HASH)
    return data_dir, str(group["id"])


def _remind_main(data_dir, *argv):
    return cli.main(["remind", "--data-dir", str(data_dir), *argv])


def _record(**overrides):
    record = {
        "from": "alice",
        "destination": ALICE_HASH,
        "text": "meet at noon?",
        "sent_at": 10.0,
        "received_at": 20.0,
        "id": "msg-1",
    }
    record.update(overrides)
    return record


def _seed_inbox_message(tmp_path, **overrides):
    """An inbox holding one received DM; returns (config, id, peer hash).

    The message's sender is the seeded peer, so the CLI resolves the
    sender's display name instead of falling back to a short hash.
    """
    data_dir, peer_hash = _seed_peer(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    inbox = Inbox(config.inbox_path)
    inbox.append(_record(destination=peer_hash, **overrides))
    return config, overrides.get("id", "msg-1"), peer_hash


# --- Store ---


def test_store_add_lists_soonest_first_and_persists(tmp_path):
    store = ReminderStore(tmp_path / "reminders.json")
    first = store.add(
        message_id="msg-1",
        kind="peer",
        target=ALICE_HASH,
        text="first",
        sender="alice",
        due_at=2000.0,
        now=1000.0,
    )
    second = store.add(
        message_id="msg-2",
        kind="peer",
        target=ALICE_HASH,
        text="second",
        sender="alice",
        due_at=1500.0,
        now=1000.0,
    )
    assert first["id"] != second["id"]
    assert [entry["id"] for entry in store.list_reminders()] == [
        second["id"],
        first["id"],
    ]
    reopened = ReminderStore(tmp_path / "reminders.json")
    assert [entry["id"] for entry in reopened.list_reminders()] == [
        second["id"],
        first["id"],
    ]


def test_store_add_validates(tmp_path):
    store = ReminderStore(tmp_path / "reminders.json")
    with pytest.raises(ReminderError):
        store.add(
            message_id="msg-1",
            kind="channel",
            target=ALICE_HASH,
            text="nope",
            sender="alice",
            due_at=2000.0,
            now=1000.0,
        )
    with pytest.raises(ReminderError):
        store.add(
            message_id="msg-1",
            kind="peer",
            target="not-a-hash",
            text="nope",
            sender="alice",
            due_at=2000.0,
            now=1000.0,
        )
    with pytest.raises(ReminderError):
        store.add(
            message_id="msg-1",
            kind="peer",
            target=ALICE_HASH,
            text="   ",
            sender="alice",
            due_at=2000.0,
            now=1000.0,
        )
    with pytest.raises(ReminderError):
        store.add(
            message_id="msg-1",
            kind="peer",
            target=ALICE_HASH,
            text="past",
            sender="alice",
            due_at=999.0,
            now=1000.0,
        )
    with pytest.raises(ReminderError):
        store.add(
            message_id="msg-1",
            kind="peer",
            target=ALICE_HASH,
            text="too far",
            sender="alice",
            due_at=1000.0 + MAX_REMINDER_DELAY + 1,
            now=1000.0,
        )
    assert len(store) == 0


def test_store_cap(tmp_path):
    store = ReminderStore(tmp_path / "reminders.json")
    for index in range(MAX_REMINDERS):
        store.add(
            message_id=f"msg-{index}",
            kind="peer",
            target=ALICE_HASH,
            text=f"reminder {index}",
            sender="alice",
            due_at=2000.0 + index,
            now=1000.0,
        )
    with pytest.raises(ReminderError, match="reminder limit reached"):
        store.add(
            message_id="msg-extra",
            kind="peer",
            target=ALICE_HASH,
            text="one too many",
            sender="alice",
            due_at=3000.0,
            now=1000.0,
        )


def test_store_cancel_by_prefix_and_ambiguity(tmp_path):
    store = ReminderStore(tmp_path / "reminders.json")
    first = store.add(
        message_id="msg-1",
        kind="peer",
        target=ALICE_HASH,
        text="one",
        sender="alice",
        due_at=2000.0,
        now=1000.0,
    )
    second = store.add(
        message_id="msg-2",
        kind="peer",
        target=ALICE_HASH,
        text="two",
        sender="alice",
        due_at=2000.0,
        now=1000.0,
    )
    cancelled = store.cancel(first["id"][:8])
    assert cancelled["id"] == first["id"]
    assert len(store) == 1
    with pytest.raises(ReminderError, match="unknown reminder"):
        store.cancel("deadbeef")
    # Two reminders can share a long prefix only by construction; the
    # full-id path always wins over a prefix match.
    assert store.cancel(second["id"])["id"] == second["id"]
    assert len(store) == 0


def test_store_cancel_ambiguous_prefix(tmp_path):
    path = tmp_path / "reminders.json"
    path.write_text(
        json.dumps(
            {
                "reminders": [
                    {
                        "id": "ab12" + "00" * 6,
                        "message_id": "msg-1",
                        "kind": "peer",
                        "target": ALICE_HASH,
                        "text": "one",
                        "sender": "alice",
                        "due_at": 2000.0,
                        "created_at": 1000.0,
                    },
                    {
                        "id": "ab12" + "ff" * 6,
                        "message_id": "msg-2",
                        "kind": "peer",
                        "target": ALICE_HASH,
                        "text": "two",
                        "sender": "alice",
                        "due_at": 2000.0,
                        "created_at": 1000.0,
                    },
                ]
            }
        )
    )
    store = ReminderStore(path)
    with pytest.raises(ReminderError, match="2 reminders match prefix"):
        store.cancel("ab12")


def test_store_load_ignores_malformed_entries(tmp_path):
    path = tmp_path / "reminders.json"
    path.write_text(
        json.dumps(
            {
                "reminders": [
                    {
                        "id": "ab" * 8,
                        "message_id": "msg-1",
                        "kind": "peer",
                        "target": ALICE_HASH,
                        "text": "kept",
                        "sender": "alice",
                        "due_at": 2000.0,
                        "created_at": 1000.0,
                    },
                    {"id": "bad", "kind": "peer"},
                    "not-a-dict",
                ]
            }
        )
    )
    assert len(ReminderStore(path).list_reminders()) == 1


def test_parse_remind_at():
    # 2026-11-21T09:00:00Z, one day after `now`.
    due = parse_remind_at("2026-11-21T09:00:00+00:00", now=1_795_165_200.0)
    assert due == 1_795_251_600.0
    assert parse_remind_at("2026-11-21T09:00Z", now=1_795_165_200.0) == due
    with pytest.raises(ValueError, match="must be in the future"):
        parse_remind_at("2020-01-01T00:00:00Z", now=1_795_165_200.0)
    with pytest.raises(ValueError, match="must be within 30d"):
        parse_remind_at("2027-06-01T00:00:00Z", now=1_795_165_200.0)
    with pytest.raises(ValueError, match="ISO 8601"):
        parse_remind_at("next tuesday", now=1_795_165_200.0)


# --- CLI ---


def test_cli_add_list_cancel(tmp_path, capsys):
    config, message_id, _peer_hash = _seed_inbox_message(tmp_path)
    data_dir = config.data_dir
    assert _remind_main(data_dir, "add", message_id, "--in", "1h") == 0
    out = capsys.readouterr().out
    assert "reminder set for alice in 1h" in out
    assert "meet at noon?" in out
    assert _remind_main(data_dir, "list") == 0
    out = capsys.readouterr().out
    assert "alice" in out and "meet at noon?" in out and "in 1h" in out
    reminder_id = out.split("[")[1].split("]")[0]
    assert _remind_main(data_dir, "cancel", reminder_id) == 0
    assert "cancelled reminder for alice" in capsys.readouterr().out
    assert _remind_main(data_dir, "list") == 0
    assert "No reminders yet" in capsys.readouterr().out


def test_cli_add_group_message(tmp_path, capsys):
    data_dir, group_id = _seed_group(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    inbox = Inbox(config.inbox_path)
    inbox.append_group_message(
        _record(
            id="grp-1",
            text="agenda?",
            destination=ALICE_HASH,
            kind="group",
            group=group_id,
        ),
        group=group_id,
        author=ALICE_HASH,
        sent_at=10.0,
        members=[ALICE_HASH, BOB_HASH],
    )
    assert _remind_main(data_dir, "add", "grp-1", "--in", "30m") == 0
    assert "reminder set for crew in 30m" in capsys.readouterr().out


def test_cli_add_rejects_unknown_message_and_bad_time(tmp_path, capsys):
    config, _message_id, _peer_hash = _seed_inbox_message(tmp_path)
    assert (
        _remind_main(config.data_dir, "add", "nope", "--in", "1h")
        == cli._EXIT_ERROR
    )
    assert "unknown message" in capsys.readouterr().err
    assert (
        _remind_main(config.data_dir, "add", "msg-1", "--in", "banana")
        == cli._EXIT_ERROR
    )
    assert "duration" in capsys.readouterr().err
    assert (
        _remind_main(config.data_dir, "add", "msg-1", "--at", "2020-01-01T00:00:00Z")
        == cli._EXIT_ERROR
    )
    assert "must be in the future" in capsys.readouterr().err


def test_cli_add_rejects_file_message(tmp_path, capsys):
    data_dir, _peer_hash = _seed_peer(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "kind": "file",
            "id": "file-1",
            "destination": ALICE_HASH,
            "name": "photo.jpg",
            "size": 10,
            "sha256": "ab" * 32,
            "path": "/tmp/photo.jpg",
            "sent_at": 10.0,
            "received_at": 20.0,
        }
    )
    assert (
        _remind_main(data_dir, "add", "file-1", "--in", "1h") == cli._EXIT_ERROR
    )
    assert "no text" in capsys.readouterr().err


def test_cli_cancel_unknown(tmp_path, capsys):
    data_dir, _peer_hash = _seed_peer(tmp_path)
    assert _remind_main(data_dir, "cancel", "deadbeef") == cli._EXIT_ERROR
    assert "unknown reminder" in capsys.readouterr().err


def test_cli_json_round_trip(tmp_path, capsys):
    config, message_id, _peer_hash = _seed_inbox_message(tmp_path)
    assert (
        _remind_main(config.data_dir, "add", message_id, "--in", "2h", "--json") == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["reminder"]["text"] == "meet at noon?"
    assert payload["reminder"]["sender"] == "alice"
    assert payload["reminder"]["target_name"] == "alice"
    assert _remind_main(config.data_dir, "list", "--json") == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1


# --- Firing ---


def test_fire_due_marks_unread_and_consumes(tmp_path, caplog):
    config, message_id, peer_hash = _seed_inbox_message(tmp_path)
    store = ReminderStore(config.reminders_path)
    store.add(
        message_id=message_id,
        kind="peer",
        target=peer_hash,
        text="meet at noon?",
        sender="alice",
        due_at=1500.0,
        now=1000.0,
    )
    store.add(
        message_id=message_id,
        kind="peer",
        target=peer_hash,
        text="not yet",
        sender="alice",
        due_at=5000.0,
        now=1000.0,
    )
    inbox = Inbox(config.inbox_path)
    # The message starts read (cursor advanced past it); firing must
    # light it again.
    inbox.mark_read(peer_hash)
    assert inbox.unread_counts()["total"] == 0
    with caplog.at_level("INFO", logger="tau.reminders"):
        result = fire_due_reminders(config.reminders_path, inbox, now=2000.0)
    assert len(result["fired"]) == 1
    assert result["fired"][0]["text"] == "meet at noon?"
    assert result["dropped"] == []
    assert inbox.unread_counts()["total"] == 1
    assert "Reminder from alice: meet at noon?" in caplog.text
    assert len(ReminderStore(config.reminders_path)) == 1


def test_fire_drops_reminder_for_deleted_message(tmp_path):
    config, message_id, peer_hash = _seed_inbox_message(tmp_path)
    store = ReminderStore(config.reminders_path)
    store.add(
        message_id=message_id,
        kind="peer",
        target=peer_hash,
        text="meet at noon?",
        sender="alice",
        due_at=1500.0,
        now=1000.0,
    )
    inbox = Inbox(config.inbox_path)
    inbox.delete(message_id)
    result = fire_due_reminders(config.reminders_path, inbox, now=2000.0)
    assert result["fired"] == []
    assert len(result["dropped"]) == 1
    assert len(ReminderStore(config.reminders_path)) == 0


def test_fire_group_reminder(tmp_path):
    data_dir, group_id = _seed_group(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="owner")
    inbox = Inbox(config.inbox_path)
    inbox.append_group_message(
        _record(
            id="grp-1",
            text="agenda?",
            destination=ALICE_HASH,
            kind="group",
            group=group_id,
        ),
        group=group_id,
        author=ALICE_HASH,
        sent_at=10.0,
        members=[ALICE_HASH, BOB_HASH],
    )
    store = ReminderStore(config.reminders_path)
    store.add(
        message_id="grp-1",
        kind="group",
        target=group_id,
        text="agenda?",
        sender="alice",
        due_at=1500.0,
        now=1000.0,
    )
    inbox.mark_read(group=group_id)
    assert inbox.unread_counts()["total"] == 0
    result = fire_due_reminders(config.reminders_path, inbox, now=2000.0)
    assert len(result["fired"]) == 1
    assert inbox.unread_counts()["total"] == 1
    assert len(ReminderStore(config.reminders_path)) == 0
