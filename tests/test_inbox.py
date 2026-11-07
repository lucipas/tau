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


def test_list_filters_by_text_query(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", destination="aa" * 16, text="Hello world", received_at=1.0))
    inbox.append(_record(id="b1", destination="bb" * 16, text="HELLO again", received_at=2.0))
    inbox.append(_record(id="a2", destination="aa" * 16, text="unrelated", received_at=3.0))

    # Case-insensitive substring match, newest first.
    assert [e["id"] for e in inbox.list_messages(query="hello")] == ["b1", "a1"]
    assert [e["id"] for e in inbox.list_messages(query="WORLD")] == ["a1"]
    assert inbox.list_messages(query="missing") == []
    # Query composes with the peer filter and the limit.
    assert [e["id"] for e in inbox.list_messages(query="hello", peer="aa" * 16)] == ["a1"]
    assert [e["id"] for e in inbox.list_messages(query="hello", limit=1)] == ["b1"]


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
