"""Unit tests for the durable node inbox (tau/inbox.py)."""

from __future__ import annotations

import json
import stat

import pytest

from tau.inbox import Inbox


def _record(**overrides):
    record = {
        "from": "alice",
        "destination": "ab" * 16,
        "text": "hello",
        "sent_at": 10.0,
        "received_at": 20.0,
        "id": "msg-1",
    }
    record.update(overrides)
    return record


def test_append_lists_newest_first(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="first", received_at=20.0, text="one"))
    inbox.append(_record(id="second", received_at=30.0, text="two"))

    entries = inbox.list_messages()
    assert [entry["id"] for entry in entries] == ["second", "first"]
    assert len(inbox) == 2


def test_append_keeps_only_the_cap(tmp_path, monkeypatch):
    import tau.inbox

    monkeypatch.setattr(tau.inbox, "MAX_INBOX", 3)
    inbox = Inbox(tmp_path / "inbox.json")
    for index in range(5):
        inbox.append(_record(id=f"msg-{index}", received_at=float(index)))

    entries = inbox.list_messages()
    assert [entry["id"] for entry in entries] == ["msg-4", "msg-3", "msg-2"]


def test_inbox_persists_across_instances(tmp_path):
    path = tmp_path / "inbox.json"
    Inbox(path).append(_record(text="keep me"))

    reloaded = Inbox(path)
    assert len(reloaded) == 1
    assert reloaded.list_messages()[0]["text"] == "keep me"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_corrupt_file_loads_empty(tmp_path):
    path = tmp_path / "inbox.json"
    path.write_text("{not json", encoding="utf-8")
    assert len(Inbox(path)) == 0


def test_invalid_stored_entries_are_skipped(tmp_path):
    path = tmp_path / "inbox.json"
    path.write_text(
        json.dumps(
            {
                "messages": [
                    _record(id="good"),
                    {"from": "bob", "text": ""},  # empty text
                    {"text": 42},  # non-string text
                    "not a record",
                    _record(id="also-good", text="second"),
                ]
            }
        ),
        encoding="utf-8",
    )
    entries = Inbox(path).list_messages()
    assert [entry["id"] for entry in entries] == ["also-good", "good"]


def test_list_filters_by_peer_and_limit(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", destination="aa" * 16, received_at=1.0))
    inbox.append(_record(id="b1", destination="bb" * 16, received_at=2.0))
    inbox.append(_record(id="a2", destination="aa" * 16, received_at=3.0))

    assert [e["id"] for e in inbox.list_messages(peer="aa" * 16)] == ["a2", "a1"]
    assert [e["id"] for e in inbox.list_messages(limit=2)] == ["a2", "b1"]
    assert inbox.list_messages(peer="cc" * 16) == []
    assert inbox.list_messages(limit=0) == []


def test_concurrent_writers_do_not_lose_entries(tmp_path):
    """Two Inbox handles on one file (serve + one-shot) keep each other's entries."""
    path = tmp_path / "inbox.json"
    first = Inbox(path)
    second = Inbox(path)
    first.append(_record(id="from-first", received_at=1.0))
    second.append(_record(id="from-second", received_at=2.0))
    first.append(_record(id="from-first-again", received_at=3.0))
    second.append(_record(id="from-second-again", received_at=4.0))

    entries = Inbox(path).list_messages()
    assert [entry["id"] for entry in entries] == [
        "from-second-again",
        "from-first-again",
        "from-second",
        "from-first",
    ]
    # Each handle reloaded under the lock, so it saw the other's entries.
    assert len(second) == 4


def test_nonfinite_timestamps_are_rejected(tmp_path):
    import math

    path = tmp_path / "inbox.json"
    path.write_text(
        json.dumps(
            {
                "messages": [
                    _record(id="inf-received", received_at=math.inf),
                    _record(id="nan-received", received_at=math.nan),
                    _record(id="nan-sent", sent_at=math.nan),
                    _record(id="good"),
                ]
            }
        ),
        encoding="utf-8",
    )
    entries = Inbox(path).list_messages()
    # Non-finite received_at is node-generated, so a bad value means a
    # corrupt entry and it is skipped; a non-finite sent_at comes from
    # the wire and is kept with an unknown send time instead.
    assert [entry["id"] for entry in entries] == ["good", "nan-sent"]
    kept = [entry for entry in entries if entry["id"] == "nan-sent"][0]
    assert kept["sent_at"] == 0.0


def test_memory_only_inbox(tmp_path):
    inbox = Inbox()
    inbox.append(_record())
    assert len(inbox) == 1
    assert inbox.list_messages()[0]["text"] == "hello"


def test_append_rejects_record_without_text(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    with pytest.raises(ValueError):
        inbox.append({"from": "alice"})
    assert len(inbox) == 0


def test_export_document_round_trips_through_import(tmp_path):
    source = Inbox(tmp_path / "source.json")
    source.append(_record(id="one", text="first"))
    source.append(_record(id="two", text="second"))

    document = source.export_document()
    assert document["format"] == "tau-messages-export"
    assert document["version"] == 1
    assert [entry["id"] for entry in document["messages"]] == ["one", "two"]

    target = Inbox(tmp_path / "target.json")
    result = target.import_messages(document)
    assert result == {"imported": 2, "present": 0, "skipped": 0, "errors": []}
    assert [entry["id"] for entry in target.list_messages()] == ["two", "one"]

    # Re-importing the same export is a no-op, not a duplicate.
    again = target.import_messages(document)
    assert again["imported"] == 0
    assert again["present"] == 2
    assert len(target) == 2


def test_export_document_filters_by_peer(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", destination="aa" * 16))
    inbox.append(_record(id="b1", destination="bb" * 16))

    document = inbox.export_document(peer="aa" * 16)
    assert [entry["id"] for entry in document["messages"]] == ["a1"]


def test_import_dedupes_records_without_ids_by_content(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    document = {
        "format": "tau-messages-export",
        "version": 1,
        "messages": [
            {"from": "alice", "destination": "ab" * 16, "text": "hi",
             "sent_at": 5.0, "received_at": 6.0},
        ],
    }
    first = inbox.import_messages(document)
    assert first["imported"] == 1
    second = inbox.import_messages(document)
    assert second["imported"] == 0
    assert second["present"] == 1
    assert len(inbox) == 1


def test_import_skips_invalid_entries_with_reasons(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    document = {
        "format": "tau-messages-export",
        "version": 1,
        "messages": [
            _record(id="good"),
            {"from": "bob", "text": ""},  # empty text
            {"text": 42},  # non-string text
            "not a record",
            _record(id="bad-time", received_at="yesterday"),
        ],
    }
    result = inbox.import_messages(document)
    assert result["imported"] == 1
    assert result["present"] == 0
    assert result["skipped"] == 4
    assert [error["error"] for error in result["errors"]] == [
        "inbox record needs text",
        "inbox record needs text",
        "entry is not an object",
        "invalid received_at",
    ]
    assert len(inbox) == 1


def test_import_rejects_bad_documents(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    with pytest.raises(ValueError, match="must contain a JSON object"):
        inbox.import_messages(["not", "a", "dict"])
    with pytest.raises(ValueError, match="unsupported export format"):
        inbox.import_messages({"format": "other", "version": 1, "messages": []})
    with pytest.raises(ValueError, match="unsupported export version"):
        inbox.import_messages(
            {"format": "tau-messages-export", "version": 99, "messages": []}
        )
    with pytest.raises(ValueError, match="no messages list"):
        inbox.import_messages({"format": "tau-messages-export", "version": 1})
    assert len(inbox) == 0


def test_import_respects_the_cap(tmp_path, monkeypatch):
    import tau.inbox

    monkeypatch.setattr(tau.inbox, "MAX_INBOX", 3)
    inbox = Inbox(tmp_path / "inbox.json")
    document = {
        "format": "tau-messages-export",
        "version": 1,
        "messages": [_record(id=f"msg-{index}") for index in range(5)],
    }
    result = inbox.import_messages(document)
    assert result["imported"] == 5
    assert [entry["id"] for entry in inbox.list_messages()] == [
        "msg-4",
        "msg-3",
        "msg-2",
    ]
