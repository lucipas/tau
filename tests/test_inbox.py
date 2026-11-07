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


def test_sent_messages_are_stored_but_not_listed_by_default(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="in-1", direction="in", text="from alice"))
    inbox.append(_record(id="out-1", direction="out", text="from me"))

    # The received view is unchanged: sent messages do not surface in
    # `tau messages` or unread counts.
    assert [entry["id"] for entry in inbox.list_messages()] == ["in-1"]
    assert [entry["id"] for entry in inbox.list_messages(direction="out")] == [
        "out-1"
    ]
    assert [entry["id"] for entry in inbox.list_messages(direction=None)] == [
        "out-1",
        "in-1",
    ]
    assert len(inbox) == 2


def test_find_resolves_quotes_in_both_directions(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="in-1", direction="in", text="from alice"))
    inbox.append(_record(id="out-1", direction="out", text="from me"))

    assert inbox.find("in-1")["text"] == "from alice"
    assert inbox.find("out-1")["text"] == "from me"
    assert inbox.find("missing") is None


def test_find_is_scoped_to_the_conversation(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="msg-1", destination="ab" * 16, text="alice's"))
    inbox.append(_record(id="msg-1", destination="cd" * 16, text="carol's"))

    # Same id, different peer: the lookup must not cross conversations.
    assert inbox.find("msg-1", peer="cd" * 16)["text"] == "carol's"
    assert inbox.find("msg-1", peer="ee" * 16) is None


def test_find_prefix_needs_a_unique_match_in_the_conversation(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="abc111", destination="ab" * 16, text="one"))
    inbox.append(_record(id="abc222", destination="ab" * 16, text="two"))
    inbox.append(_record(id="abc333", destination="cd" * 16, text="other peer"))

    assert [entry["id"] for entry in inbox.find_prefix("abc1", peer="ab" * 16)] == [
        "abc111"
    ]
    assert len(inbox.find_prefix("abc", peer="ab" * 16)) == 2
    assert inbox.find_prefix("abc", peer="cd" * 16)[0]["id"] == "abc333"
    assert inbox.find_prefix("zzz", peer="ab" * 16) == []


def test_record_keeps_reply_reference_and_direction(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    reply_to = {"id": "a" * 32, "excerpt": "0123456789abcdef"}
    inbox.append(_record(id="msg-9", in_reply_to=reply_to, direction="in"))

    reloaded = Inbox(tmp_path / "inbox.json")
    entry = reloaded.list_messages()[0]
    assert entry["in_reply_to"] == reply_to
    assert entry["direction"] == "in"


def test_record_drops_corrupt_reply_reference(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="msg-9", in_reply_to={"id": ""}))

    entry = inbox.list_messages()[0]
    assert "in_reply_to" not in entry
    assert len(inbox) == 1
