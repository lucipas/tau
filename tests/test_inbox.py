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


def _reaction(**overrides):
    record = {
        "target": "msg-1",
        "destination": "ab" * 16,
        "from": "alice",
        "reaction": "👍",
        "sent_at": 10.0,
        "received_at": 20.0,
        "id": "react-1",
    }
    record.update(overrides)
    return record


def test_reaction_latest_per_reactor_wins(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.record_reaction(_reaction(reaction="👍", received_at=20.0))
    inbox.record_reaction(_reaction(reaction="❤️", received_at=21.0, id="react-2"))

    reactions = inbox.reactions_for("msg-1")
    assert len(reactions) == 1
    assert reactions[0]["reaction"] == "❤️"
    assert reactions[0]["id"] == "react-2"


def test_reaction_clear_removes_only_that_reactor(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.record_reaction(_reaction(destination="ab" * 16))
    inbox.record_reaction(_reaction(destination="cd" * 16, **{"from": "bob"}))
    assert len(inbox.reactions_for("msg-1")) == 2

    inbox.record_reaction(_reaction(destination="ab" * 16, reaction=""))
    remaining = inbox.reactions_for("msg-1")
    assert len(remaining) == 1
    assert remaining[0]["destination"] == "cd" * 16


def test_reaction_clear_of_unknown_is_a_noop(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    assert inbox.record_reaction(_reaction(reaction="")) is None
    assert inbox.reactions_for("msg-1") == []


def test_reactions_annotate_listed_messages(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="msg-1"))
    inbox.append(_record(id="msg-2", text="other"))
    inbox.record_reaction(_reaction(target="msg-1"))

    entries = inbox.list_messages()
    annotated = {entry["id"]: entry for entry in entries}
    assert annotated["msg-1"]["reactions"][0]["reaction"] == "👍"
    assert "reactions" not in annotated["msg-2"]


def test_reactions_persist_across_instances(tmp_path):
    path = tmp_path / "inbox.json"
    Inbox(path).record_reaction(_reaction())
    reloaded = Inbox(path)
    assert len(reloaded.reactions_for("msg-1")) == 1
    assert reloaded.reactions_for("msg-1")[0]["reaction"] == "👍"


def test_reactions_keep_only_the_cap(tmp_path, monkeypatch):
    import tau.inbox

    monkeypatch.setattr(tau.inbox, "MAX_REACTIONS", 2)
    inbox = Inbox(tmp_path / "inbox.json")
    for index in range(4):
        inbox.record_reaction(
            _reaction(target=f"msg-{index}", received_at=float(index))
        )
    assert [item["target"] for item in inbox._reactions] == ["msg-2", "msg-3"]


def test_resolve_by_full_id_and_prefix(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a" * 32))
    inbox.append(_record(id="b" * 32))

    assert inbox.resolve("a" * 32)["id"] == "a" * 32
    assert inbox.resolve("aaaa")["id"] == "a" * 32
    with pytest.raises(ValueError, match="unknown message"):
        inbox.resolve("c" * 32)
    with pytest.raises(ValueError, match="unknown message"):
        inbox.resolve("cccc")


def test_resolve_ambiguous_prefix(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="abcdef01" + "0" * 24))
    inbox.append(_record(id="abcdef02" + "0" * 24))
    with pytest.raises(ValueError, match="2 messages match prefix"):
        inbox.resolve("abcdef")
