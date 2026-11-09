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


def test_list_search_skips_textless_file_records(tmp_path):
    # A received file has no "text" key; searching must exclude it
    # instead of raising KeyError.
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", destination="aa" * 16, text="Hello world", received_at=1.0))
    inbox.append(
        {
            "kind": "file",
            "from": "alice",
            "destination": "aa" * 16,
            "name": "notes.txt",
            "size": 12,
            "sha256": "ab" * 32,
            "path": "notes.txt",
            "sent_at": 2.0,
            "received_at": 2.0,
            "id": "f1",
        }
    )

    assert [e["id"] for e in inbox.list_messages(query="hello")] == ["a1"]
    assert inbox.list_messages(query="notes") == []


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


def test_new_messages_are_unread_until_marked_read(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", destination="aa" * 16, received_at=1.0))
    inbox.append(_record(id="b1", destination="bb" * 16, received_at=2.0))
    inbox.append(_record(id="a2", destination="aa" * 16, received_at=3.0))

    assert inbox.unread_counts() == {
        "per_peer": {"aa" * 16: 2, "bb" * 16: 1},
        "total": 3,
    }
    assert all(entry["unread"] for entry in inbox.list_messages())

    counts = inbox.mark_read(peer="aa" * 16)
    assert counts == {"per_peer": {"bb" * 16: 1}, "total": 1}
    listed = inbox.list_messages()
    assert [entry["id"] for entry in listed] == ["a2", "b1", "a1"]
    assert [entry["unread"] for entry in listed] == [False, True, False]

    # Marking one peer leaves the other's cursor alone.
    inbox.mark_read(peer="bb" * 16)
    assert inbox.unread_counts() == {"per_peer": {}, "total": 0}


def test_mark_read_advances_only_to_up_to(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", received_at=1.0))
    inbox.append(_record(id="a2", received_at=2.0))
    inbox.append(_record(id="a3", received_at=3.0))

    # A listing capped at the older two marks those read; the newest,
    # never shown, stays unread.
    inbox.mark_read(up_to_seq=2)
    listed = inbox.list_messages()
    assert [entry["id"] for entry in listed] == ["a3", "a2", "a1"]
    assert [entry["unread"] for entry in listed] == [True, False, False]


def test_mark_read_never_moves_a_cursor_backward(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", received_at=1.0))
    inbox.append(_record(id="a2", received_at=2.0))
    inbox.mark_read()

    # A delayed mark covering only the older message must not restore
    # the newer one to unread.
    inbox.mark_read(up_to_seq=1)
    assert inbox.unread_counts() == {"per_peer": {}, "total": 0}


def test_clock_going_backward_does_not_hide_new_mail(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", received_at=100.0))
    inbox.mark_read()

    # Arrives later, but the wall clock says it is earlier.
    inbox.append(_record(id="a2", received_at=50.0))
    assert inbox.unread_counts() == {"per_peer": {"ab" * 16: 1}, "total": 1}


def test_legacy_file_with_descending_timestamps_starts_read(tmp_path):
    path = tmp_path / "inbox.json"
    path.write_text(
        json.dumps(
            {
                "messages": [
                    _record(id="first", received_at=9.0),
                    _record(id="second", received_at=3.0),
                ]
            }
        ),
        encoding="utf-8",
    )

    inbox = Inbox(path)
    assert inbox.unread_counts() == {"per_peer": {}, "total": 0}


def test_read_cursors_persist_across_instances(tmp_path):
    path = tmp_path / "inbox.json"
    inbox = Inbox(path)
    inbox.append(_record(id="a1", received_at=1.0))
    inbox.mark_read()
    inbox.append(_record(id="a2", received_at=2.0))

    reloaded = Inbox(path)
    assert reloaded.unread_counts() == {"per_peer": {"ab" * 16: 1}, "total": 1}
    assert [entry["unread"] for entry in reloaded.list_messages()] == [True, False]


def test_old_file_without_cursors_starts_read(tmp_path):
    path = tmp_path / "inbox.json"
    path.write_text(
        json.dumps({"messages": [_record(id="old", received_at=5.0)]}),
        encoding="utf-8",
    )

    inbox = Inbox(path)
    assert inbox.unread_counts() == {"per_peer": {}, "total": 0}
    assert inbox.list_messages()[0]["unread"] is False

    # But mail that arrives after the upgrade is unread.
    inbox.append(_record(id="new", received_at=6.0))
    assert inbox.unread_counts() == {"per_peer": {"ab" * 16: 1}, "total": 1}


def test_same_timestamp_messages_are_all_read_after_marking(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="first", received_at=1.0))
    inbox.append(_record(id="second", received_at=1.0))

    # The cursor lands on the shared timestamp, so both tied messages
    # count as read — they were shown together.
    inbox.mark_read()
    listed = inbox.list_messages()
    assert [entry["id"] for entry in listed] == ["second", "first"]
    assert [entry["unread"] for entry in listed] == [False, False]
    assert inbox.unread_counts() == {"per_peer": {}, "total": 0}


def test_unread_counts_see_another_handles_mark(tmp_path):
    """A serving node sees a one-shot CLI's mark via the reload."""
    path = tmp_path / "inbox.json"
    server = Inbox(path)
    cli = Inbox(path)
    server.append(_record(id="a1", received_at=1.0))
    assert server.unread_counts()["total"] == 1

    cli.mark_read()
    assert server.unread_counts() == {"per_peer": {}, "total": 0}


def test_mark_read_on_empty_inbox_is_a_noop(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    assert inbox.mark_read(peer="aa" * 16) == {"per_peer": {}, "total": 0}
    assert inbox.mark_read() == {"per_peer": {}, "total": 0}


def test_sent_records_do_not_evict_received_history(tmp_path, monkeypatch):
    from tau import inbox as inbox_module

    monkeypatch.setattr(inbox_module, "MAX_INBOX", 2)
    monkeypatch.setattr(inbox_module, "MAX_INBOX_SENT", 1)
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="in-1", direction="in"))
    inbox.append(_record(id="in-2", direction="in"))
    for index in range(3):
        inbox.append(_record(id=f"out-{index}", direction="out"))

    received = inbox.list_messages()
    assert [item["id"] for item in received] == ["in-2", "in-1"]
    sent = inbox.list_messages(direction="out")
    assert [item["id"] for item in sent] == ["out-2"]


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


def test_reaction_clear_of_unknown_is_a_noop(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    assert inbox.record_reaction(_reaction(reaction="")) is None
    assert inbox.reactions_for("msg-1") == []


def test_reaction_clear_removes_only_that_reactor(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.record_reaction(_reaction(destination="ab" * 16))
    inbox.record_reaction(_reaction(destination="cd" * 16, **{"from": "bob"}))
    assert len(inbox.reactions_for("msg-1")) == 2

    inbox.record_reaction(_reaction(destination="ab" * 16, reaction=""))
    remaining = inbox.reactions_for("msg-1")
    assert len(remaining) == 1
    assert remaining[0]["destination"] == "cd" * 16


def test_reaction_latest_per_reactor_wins(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.record_reaction(_reaction(reaction="👍", received_at=20.0))
    inbox.record_reaction(_reaction(reaction="❤️", received_at=21.0, id="react-2"))

    reactions = inbox.reactions_for("msg-1")
    assert len(reactions) == 1
    assert reactions[0]["reaction"] == "❤️"
    assert reactions[0]["id"] == "react-2"


def test_reaction_stale_update_is_ignored(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.record_reaction(
        _reaction(reaction="❤️", sent_at=30.0, received_at=30.0, id="r-2")
    )
    inbox.record_reaction(
        _reaction(reaction="👍", sent_at=20.0, received_at=31.0, id="r-1")
    )
    assert [item["reaction"] for item in inbox.reactions_for("msg-1")] == ["❤️"]


def test_reaction_tombstone_keeps_stale_reactions_cleared(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.record_reaction(_reaction(sent_at=20.0, received_at=20.0, id="r-1"))
    inbox.record_reaction(
        _reaction(reaction="", sent_at=21.0, received_at=21.0, id="r-2")
    )
    assert inbox.reactions_for("msg-1") == []
    # A late copy of the retracted reaction must not resurrect it.
    inbox.record_reaction(_reaction(sent_at=20.0, received_at=22.0, id="r-1"))
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


def test_reactions_keep_only_the_cap(tmp_path, monkeypatch):
    import tau.inbox

    monkeypatch.setattr(tau.inbox, "MAX_REACTIONS", 2)
    inbox = Inbox(tmp_path / "inbox.json")
    for index in range(4):
        inbox.record_reaction(
            _reaction(target=f"msg-{index}", received_at=float(index))
        )
    assert [item["target"] for item in inbox._reactions] == ["msg-2", "msg-3"]


def test_reactions_persist_across_instances(tmp_path):
    path = tmp_path / "inbox.json"
    Inbox(path).record_reaction(_reaction())
    reloaded = Inbox(path)
    assert len(reloaded.reactions_for("msg-1")) == 1
    assert reloaded.reactions_for("msg-1")[0]["reaction"] == "👍"


def test_resolve_ambiguous_prefix(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="abcdef01" + "0" * 24))
    inbox.append(_record(id="abcdef02" + "0" * 24))
    with pytest.raises(ValueError, match="2 messages match prefix"):
        inbox.resolve("abcdef")


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


def test_resolve_exact_id_beats_a_longer_prefix(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="abc", text="short"))
    inbox.append(_record(id="abcdef", text="long"))
    assert inbox.resolve("abc")["text"] == "short"
    assert inbox.resolve("abcdef")["text"] == "long"


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


def test_expired_messages_are_purged_on_load(tmp_path):
    import time as _time

    now = _time.time()
    path = tmp_path / "inbox.json"
    inbox = Inbox(path)
    inbox.append(_record(id="gone", expires_at=now - 10))
    inbox.append(_record(id="staying", expires_at=now + 3600))
    inbox.append(_record(id="plain"))

    reloaded = Inbox(path)
    assert [entry["id"] for entry in reloaded.list_messages()] == [
        "plain",
        "staying",
    ]
    # The purge is persisted: a later load does not resurrect the entry.
    assert "gone" not in path.read_text(encoding="utf-8")


def test_expiry_is_recorded(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="exp-1", expires_at=9999999999.0))
    entry = inbox.list_messages()[0]
    assert entry["expires_at"] == 9999999999.0


def test_invalid_expiry_is_treated_as_no_expiry(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="bad-exp", expires_at="soon"))
    assert inbox.list_messages()[0]["expires_at"] is None


def test_message_without_expiry_has_none(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="plain-1"))
    assert inbox.list_messages()[0]["expires_at"] is None
