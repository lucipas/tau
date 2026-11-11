"""Unit tests for the durable node inbox (tau/inbox.py)."""

from __future__ import annotations

import json
import stat

import pytest

from tau.inbox import Inbox
from tau.protocol import ProtocolError


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


def test_records_default_to_chat_kind(tmp_path):
    # Records written before kinds existed carry no kind field.
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="old"))
    assert inbox.list_messages()[0]["kind"] == "chat"


def test_list_filters_by_kind(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="c1", kind="chat", received_at=1.0))
    inbox.append(_record(id="p1", kind="ping", received_at=2.0))
    inbox.append(_record(id="p2", kind="ping", received_at=3.0))

    assert [e["id"] for e in inbox.list_messages(kind="ping")] == ["p2", "p1"]
    assert [e["id"] for e in inbox.list_messages(kind="chat")] == ["c1"]
    assert inbox.list_messages(kind="alert") == []


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


def test_delete_by_full_id(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a" * 32, text="one"))
    inbox.append(_record(id="b" * 32, text="two"))

    removed = inbox.delete("a" * 32)
    assert removed["text"] == "one"
    assert [entry["id"] for entry in inbox.list_messages()] == ["b" * 32]


def test_delete_by_unique_prefix(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="abcd" + "1" * 28, text="one"))
    inbox.append(_record(id="efgh" + "2" * 28, text="two"))

    removed = inbox.delete("abcd")
    assert removed["text"] == "one"
    assert [entry["id"] for entry in inbox.list_messages()] == ["efgh" + "2" * 28]


def test_delete_unknown_and_ambiguous(tmp_path):
    from tau.inbox import InboxError

    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="abcd" + "1" * 28, text="one"))
    inbox.append(_record(id="abcd" + "2" * 28, text="two"))

    with pytest.raises(InboxError, match="unknown message"):
        inbox.delete("zzzz")
    with pytest.raises(InboxError, match="2 messages match prefix"):
        inbox.delete("abcd")
    with pytest.raises(InboxError, match="must not be empty"):
        inbox.delete("  ")
    assert len(inbox) == 2


def test_delete_persists_across_instances(tmp_path):
    path = tmp_path / "inbox.json"
    inbox = Inbox(path)
    inbox.append(_record(id="keep", text="keep me"))
    inbox.append(_record(id="drop", text="drop me"))
    inbox.delete("drop")

    reloaded = Inbox(path)
    assert [entry["id"] for entry in reloaded.list_messages()] == ["keep"]


def test_delete_ignores_records_without_id(tmp_path):
    from tau.inbox import InboxError

    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id=None, text="old entry"))
    with pytest.raises(InboxError, match="unknown message"):
        inbox.delete("old")
    assert len(inbox) == 1


def test_delete_memory_only_inbox():
    inbox = Inbox()
    inbox.append(_record(id="gone", text="bye"))
    assert inbox.delete("gone")["text"] == "bye"
    assert len(inbox) == 0


def test_delete_exact_match_wins_over_folded_prefix(tmp_path):
    """An exact id wins even when a case-folded prefix would be ambiguous."""
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="A" * 32, text="upper"))
    inbox.append(_record(id="a" * 32, text="lower"))

    removed = inbox.delete("A" * 32)
    assert removed["text"] == "upper"
    assert [entry["id"] for entry in inbox.list_messages()] == ["a" * 32]


def test_delete_write_failure_rolls_back(tmp_path, monkeypatch):
    import tau.inbox
    from tau.inbox import InboxError

    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="keep", text="keep me"))

    def _boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(tau.inbox, "atomic_write_json", _boom)
    with pytest.raises(InboxError, match="could not persist inbox"):
        inbox.delete("keep")
    # The removal was rolled back: the message is still there.
    assert [entry["id"] for entry in inbox.list_messages()] == ["keep"]


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


def test_export_document_sees_messages_committed_by_another_process(tmp_path):
    # A serving node's in-memory inbox goes stale when another process
    # (e.g. `tau messages import`) commits to the same file: the export
    # must read through the cross-process transaction, not the stale copy.
    path = tmp_path / "inbox.json"
    serving = Inbox(path)
    serving.append(_record(id="one", text="first"))

    cli = Inbox(path)
    cli.append(_record(id="two", text="second"))

    document = serving.export_document()
    assert [entry["id"] for entry in document["messages"]] == ["one", "two"]


def test_import_reports_inserted_records_under_concurrent_appends(tmp_path):
    # The records an import stored are collected while its transaction
    # still holds the lock: a concurrent append reloads the inbox and
    # replaces the record objects, so collecting them afterwards would
    # report a successful restore that mirrors nothing.
    import threading

    path = tmp_path / "inbox.json"
    inbox = Inbox(path)
    other = Inbox(path)
    stop = threading.Event()

    def append_forever():
        index = 0
        while not stop.is_set():
            other.append(_record(id=f"race-{index}", text="noise"))
            index += 1

    document = {
        "format": "tau-messages-export",
        "version": 1,
        "messages": [
            _record(id=f"imp-{index}", text=f"imported {index}")
            for index in range(100)
        ],
    }
    worker = threading.Thread(target=append_forever)
    worker.start()
    try:
        inserted: list = []
        result = inbox.import_messages(document, inserted=inserted)
    finally:
        stop.set()
        worker.join()
    assert result["imported"] == 100
    assert [entry["id"] for entry in inserted] == [
        f"imp-{index}" for index in range(100)
    ]


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


def test_import_skips_huge_timestamp_without_aborting(tmp_path):
    """A JSON integer too large for float() is skipped, not fatal.

    float() on a huge int raises OverflowError; an import must record
    the entry as skipped and keep merging the entries around it.
    """
    inbox = Inbox(tmp_path / "inbox.json")
    document = {
        "format": "tau-messages-export",
        "version": 1,
        "messages": [
            _record(id="before"),
            _record(id="huge-sent", sent_at=10**400),
            _record(id="huge-received", received_at=10**400),
            _record(id="after"),
        ],
    }
    result = inbox.import_messages(document)
    assert result["imported"] == 2
    assert result["skipped"] == 2
    assert [error["error"] for error in result["errors"]] == [
        "invalid sent_at",
        "invalid received_at",
    ]
    assert [entry["id"] for entry in inbox.list_messages()] == ["after", "before"]


def test_duplicate_only_import_keeps_mixed_inbox_over_the_cap(tmp_path):
    """Re-importing an export must not evict records to one 500 cap.

    Retention is per direction (500 received + 500 sent), so a
    duplicate-only import into a mixed inbox holding 600 records
    keeps all of them instead of slicing to the newest 500.
    """
    inbox = Inbox(tmp_path / "inbox.json")
    for index in range(300):
        inbox.append(_record(id=f"in-{index}", direction="in"))
        inbox.append(_record(id=f"out-{index}", direction="out"))
    assert len(inbox) == 600

    result = inbox.import_messages(inbox.export_document())
    assert result == {"imported": 0, "present": 600, "skipped": 0, "errors": []}
    assert len(inbox) == 600
    ids = {entry["id"] for entry in inbox.list_messages(direction=None)}
    assert ids == {f"in-{index}" for index in range(300)} | {
        f"out-{index}" for index in range(300)
    }


def test_import_assigns_local_sequences(tmp_path):
    """Imported records get a local seq so a later append works.

    An export carries the source node's sequences (or none at all);
    without renumbering, the next append's max() over a None seq
    raises TypeError.
    """
    inbox = Inbox()  # in-memory: no load renumbers anything
    result = inbox.import_messages(
        {
            "format": "tau-messages-export",
            "version": 1,
            "messages": [_record(id="imported")],
        }
    )
    assert result["imported"] == 1
    assert [entry["seq"] for entry in inbox.list_messages()] == [1]

    appended = inbox.append(_record(id="local"))
    assert appended["seq"] == 2
    assert [entry["id"] for entry in inbox.list_messages()] == ["local", "imported"]


def test_import_raises_when_the_save_fails(tmp_path, monkeypatch):
    """A save failure raises instead of reporting a restore."""
    import tau.inbox

    def _fail(*args, **kwargs):
        raise OSError("disk full")

    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="kept"))
    monkeypatch.setattr(tau.inbox, "atomic_write_json", _fail)
    document = {
        "format": "tau-messages-export",
        "version": 1,
        "messages": [_record(id="new")],
    }
    with pytest.raises(OSError, match="could not persist inbox"):
        inbox.import_messages(document)
    # The unstored import is rolled back in memory too.
    assert [entry["id"] for entry in inbox.list_messages()] == ["kept"]


def test_load_skips_huge_timestamp_without_crashing(tmp_path):
    path = tmp_path / "inbox.json"
    path.write_text(
        json.dumps(
            {
                "messages": [
                    _record(id="huge", sent_at=10**400),
                    _record(id="good"),
                ]
            }
        ),
        encoding="utf-8",
    )
    entries = Inbox(path).list_messages()
    assert [entry["id"] for entry in entries] == ["good"]


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


def test_records_default_to_received_direction(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="legacy"))

    entry = inbox.list_messages()[0]
    assert entry["direction"] == "in"


def test_list_filters_by_direction(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="in-1", direction="in", received_at=1.0))
    inbox.append(_record(id="out-1", direction="out", received_at=2.0))
    inbox.append(_record(id="in-2", direction="in", received_at=3.0))

    assert [e["id"] for e in inbox.list_messages(direction="out")] == ["out-1"]
    assert [e["id"] for e in inbox.list_messages(direction="in")] == [
        "in-2",
        "in-1",
    ]
    assert [e["id"] for e in inbox.list_messages(direction=None)] == [
        "in-2",
        "out-1",
        "in-1",
    ]


def test_list_rejects_unknown_direction(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    with pytest.raises(ValueError):
        inbox.list_messages(direction="sideways")


def test_sent_record_persists_direction(tmp_path):
    path = tmp_path / "inbox.json"
    Inbox(path).append(_record(id="sent-1", direction="out"))

    reloaded = Inbox(path)
    assert [e["id"] for e in reloaded.list_messages(direction="out")] == ["sent-1"]
    assert reloaded.list_messages(direction="in") == []


def test_cap_applies_per_direction(tmp_path, monkeypatch):
    import tau.inbox

    monkeypatch.setattr(tau.inbox, "MAX_INBOX", 3)
    inbox = Inbox(tmp_path / "inbox.json")
    for index in range(4):
        inbox.append(
            _record(id=f"in-{index}", direction="in", received_at=float(index))
        )
    for index in range(2):
        inbox.append(
            _record(
                id=f"out-{index}", direction="out", received_at=10.0 + index
            )
        )
    inbox.append(_record(id="in-4", direction="in", received_at=20.0))

    # Sends did not evict received history: each direction kept its own
    # newest three.
    entries = inbox.list_messages(direction=None)
    assert [e["id"] for e in entries] == [
        "in-4",
        "out-1",
        "out-0",
        "in-3",
        "in-2",
    ]


def test_reload_caps_each_direction(tmp_path, monkeypatch):
    import tau.inbox

    monkeypatch.setattr(tau.inbox, "MAX_INBOX", 2)
    monkeypatch.setattr(tau.inbox, "MAX_INBOX_SENT", 2)
    path = tmp_path / "inbox.json"
    path.write_text(
        json.dumps(
            {
                "messages": [
                    _record(id=f"in-{index}", direction="in")
                    for index in range(4)
                ]
                + [
                    _record(id=f"out-{index}", direction="out")
                    for index in range(3)
                ]
            }
        ),
        encoding="utf-8",
    )

    entries = Inbox(path).list_messages(direction=None)
    assert [e["id"] for e in entries] == ["out-2", "out-1", "in-3", "in-2"]


def test_add_note_stores_a_read_note_from_self(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    own = "cc" * 16
    entry = inbox.add_note(
        "  buy milk on the way home  ", sender_name="alice", destination=own
    )

    assert entry["kind"] == "note"
    assert entry["direction"] == "in"
    assert entry["from"] == "alice"
    assert entry["destination"] == own
    assert entry["text"] == "buy milk on the way home"
    assert entry["id"] and len(entry["id"]) == 32

    # Read the moment it is written: no unread marker, no count.
    listed = inbox.list_messages()
    assert [item["id"] for item in listed] == [entry["id"]]
    assert listed[0]["unread"] is False
    assert inbox.unread_counts() == {"per_peer": {}, "total": 0}

    # Found by the kind filter and by search, hidden by other kinds.
    assert [item["id"] for item in inbox.list_messages(kind="note")] == [
        entry["id"]
    ]
    assert inbox.list_messages(kind="chat") == []
    assert [item["id"] for item in inbox.list_messages(query="MILK")] == [
        entry["id"]
    ]


def test_add_note_rejects_empty_text(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    with pytest.raises(ProtocolError):
        inbox.add_note("   ", sender_name="alice", destination="cc" * 16)
    assert len(inbox) == 0


def test_note_survives_export_import(tmp_path):
    source = Inbox(tmp_path / "source.json")
    entry = source.add_note("remember the mesh node", sender_name="alice", destination="cc" * 16)

    target = Inbox(tmp_path / "target.json")
    result = target.import_messages(source.export_document())
    assert result == {"imported": 1, "present": 0, "skipped": 0, "errors": []}
    imported = target.list_messages(kind="note")
    assert [item["id"] for item in imported] == [entry["id"]]
    assert imported[0]["text"] == "remember the mesh node"
    # Read state does not travel with an export — cursors are not in
    # the document — so an imported note is unread in the new inbox,
    # like any other imported message; listing it marks it read.
    assert imported[0]["unread"] is True
    target.mark_read(peer="cc" * 16)
    assert target.list_messages(kind="note")[0]["unread"] is False


def test_note_cursor_does_not_cover_peer_mail(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    note = inbox.add_note("a note", sender_name="alice", destination="cc" * 16)
    inbox.append(_record(id="peer-1", destination="ab" * 16, text="hello"))

    # The note's own-hash cursor advanced, the peer's did not.
    assert inbox.unread_counts() == {"per_peer": {"ab" * 16: 1}, "total": 1}
    listed = inbox.list_messages()
    assert [entry["id"] for entry in listed] == ["peer-1", note["id"]]
    assert [entry["unread"] for entry in listed] == [True, False]


def test_add_note_raises_when_persist_fails(tmp_path, monkeypatch):
    inbox = Inbox(tmp_path / "inbox.json")

    def _failing_save(*, strict=False):
        raise OSError("disk full")

    monkeypatch.setattr(inbox, "_save", _failing_save)
    with pytest.raises(OSError):
        inbox.add_note("lost note", sender_name="alice", destination="cc" * 16)
    # Nothing was kept in memory, and nothing landed on disk.
    assert len(inbox) == 0
    assert inbox.unread_counts() == {"per_peer": {}, "total": 0}
    assert len(Inbox(tmp_path / "inbox.json")) == 0


def test_unreceipted_excludes_notes(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    note = inbox.add_note("a note", sender_name="alice", destination="cc" * 16)
    peer = inbox.append(_record(id="peer-1", destination="ab" * 16, text="hello"))
    # Both displayed: the serving node's receipt sweep must only ever
    # see the peer's message, never the note.
    inbox.mark_read([note["id"], peer["id"]])
    assert [item["id"] for item in inbox.read_unreceipted()] == ["peer-1"]
    assert [item["id"] for item in inbox.unreceipted()] == ["peer-1"]
