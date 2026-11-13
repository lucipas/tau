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


def test_prune_drops_only_messages_older_than_cutoff(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="old", received_at=10.0, text="old"))
    inbox.append(_record(id="edge", received_at=99000.0, text="at the cutoff"))
    inbox.append(_record(id="new", received_at=100000.0, text="new"))
    inbox.append(
        _record(id="old-sent", received_at=10.0, direction="out", text="old sent")
    )

    result = inbox.prune(1000.0, now=100000.0)
    assert result == {"pruned": 2, "per_peer": {"ab" * 16: 2}, "dry_run": False}
    assert [
        entry["id"] for entry in inbox.list_messages(direction=None)
    ] == ["new", "edge"]


def test_prune_scopes_to_one_peer(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="alice-old", received_at=10.0, text="alice old"))
    inbox.append(
        _record(id="bob-old", destination="cd" * 16, received_at=10.0, text="bob old")
    )
    inbox.append(_record(id="alice-new", received_at=100000.0, text="alice new"))

    result = inbox.prune(1000.0, peer="cd" * 16, now=100000.0)
    assert result == {"pruned": 1, "per_peer": {"cd" * 16: 1}, "dry_run": False}
    assert [entry["id"] for entry in inbox.list_messages(direction=None)] == [
        "alice-new",
        "alice-old",
    ]


def test_prune_scopes_to_one_group(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(
        _record(
            id="group-old",
            kind="group",
            group="cd" * 16,
            group_name="team",
            received_at=10.0,
            text="group old",
        )
    )
    inbox.append(_record(id="dm-old", received_at=10.0, text="dm old"))

    result = inbox.prune(1000.0, group="cd" * 16, now=100000.0)
    assert result == {"pruned": 1, "per_peer": {"ab" * 16: 1}, "dry_run": False}
    assert [entry["id"] for entry in inbox.list_messages(direction=None)] == [
        "dm-old"
    ]


def test_prune_dry_run_deletes_nothing(tmp_path):
    path = tmp_path / "inbox.json"
    inbox = Inbox(path)
    inbox.append(_record(id="old", received_at=10.0, text="old"))
    inbox.append(_record(id="new", received_at=100000.0, text="new"))

    result = inbox.prune(1000.0, dry_run=True, now=100000.0)
    assert result == {"pruned": 1, "per_peer": {"ab" * 16: 1}, "dry_run": True}
    assert len(inbox) == 2
    assert [entry["id"] for entry in Inbox(path).list_messages()] == ["new", "old"]


def test_prune_drops_reactions_to_pruned_messages(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="old", received_at=10.0, text="old"))
    inbox.append(_record(id="new", received_at=100000.0, text="new"))
    inbox.record_reaction(_reaction(target="old", id="react-old"))
    inbox.record_reaction(_reaction(target="new", id="react-new"))

    inbox.prune(1000.0, now=100000.0)
    assert inbox.reactions_for("old") == []
    assert len(inbox.reactions_for("new")) == 1


def test_prune_leaves_read_cursor_and_unread_counts(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="old", received_at=10.0, text="old"))
    inbox.append(_record(id="mid", received_at=1000.0, text="mid"))
    inbox.mark_read()
    inbox.append(_record(id="new", received_at=2000.0, text="new"))

    result = inbox.prune(1500.0, now=2000.0)
    assert result["pruned"] == 1
    assert inbox.unread_counts() == {"per_peer": {"ab" * 16: 1}, "total": 1}


def test_prune_rejects_non_positive_cutoff(tmp_path):
    from tau.inbox import InboxError

    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="old", received_at=10.0, text="old"))
    with pytest.raises(InboxError, match="must be positive"):
        inbox.prune(0)
    with pytest.raises(InboxError, match="must be positive"):
        inbox.prune(-5.0)
    assert len(inbox) == 1


def test_prune_persists_across_instances(tmp_path):
    path = tmp_path / "inbox.json"
    inbox = Inbox(path)
    inbox.append(_record(id="old", received_at=10.0, text="old"))
    inbox.append(_record(id="new", received_at=100000.0, text="new"))
    inbox.prune(1000.0, now=100000.0)

    reloaded = Inbox(path)
    assert [entry["id"] for entry in reloaded.list_messages()] == ["new"]


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


def test_pin_by_full_id_and_unique_prefix(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a" * 32, text="one"))
    inbox.append(_record(id="b" * 32, text="two"))

    entry, changed = inbox.pin("a" * 32)
    assert changed is True
    assert entry["pinned"] is True
    entry, changed = inbox.pin("b" * 28)
    assert changed is True
    # Pinning twice is a no-op, not an error.
    _, changed = inbox.pin("a" * 32)
    assert changed is False

    pinned = inbox.list_messages(pinned_only=True)
    assert [item["id"] for item in pinned] == ["b" * 32, "a" * 32]
    # The default listing still shows everything, pinned included.
    assert len(inbox.list_messages()) == 2


def test_pin_32_char_prefix_of_64_char_lxmf_id(tmp_path):
    """A 32-char prefix of a 64-char LXMF id pins when no full id matches."""
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="ab" * 32, text="lxmf copy"))

    entry, changed = inbox.pin("ab" * 16)
    assert changed is True
    assert entry["id"] == "ab" * 32
    entry, changed = inbox.unpin("ab" * 16)
    assert changed is True
    assert inbox.list_messages(pinned_only=True) == []


def test_pin_32_char_prefix_ambiguous_across_lxmf_ids(tmp_path):
    from tau.inbox import InboxError

    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="ab" * 16 + "11" * 16, text="first"))
    inbox.append(_record(id="ab" * 16 + "22" * 16, text="second"))

    with pytest.raises(InboxError, match="2 messages match prefix"):
        inbox.pin("ab" * 16)


def test_pin_exact_full_id_wins_over_lxmf_prefix(tmp_path):
    """A full 32-char id still wins over a 64-char id sharing its prefix."""
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="ab" * 16, text="short id"))
    inbox.append(_record(id="ab" * 16 + "cd" * 16, text="long id"))

    entry, _ = inbox.pin("ab" * 16)
    assert entry["text"] == "short id"


def test_delete_32_char_prefix_of_64_char_id(tmp_path):
    """The shared matcher fixes delete the same way it fixes pin."""
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="ab" * 32, text="lxmf copy"))

    removed = inbox.delete("ab" * 16)
    assert removed["text"] == "lxmf copy"
    assert len(inbox) == 0


def test_sent_messages_are_never_unread(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="sent-one", text="sent", direction="out"))
    inbox.append(_record(id="recv-one", text="received"))

    entries = inbox.list_messages(direction=None)
    by_id = {entry["id"]: entry for entry in entries}
    assert by_id["sent-one"]["unread"] is False
    assert by_id["recv-one"]["unread"] is True


def test_pin_unknown_and_ambiguous(tmp_path):
    from tau.inbox import InboxError

    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="abcd" + "1" * 28, text="one"))
    inbox.append(_record(id="abcd" + "2" * 28, text="two"))

    with pytest.raises(InboxError, match="unknown message"):
        inbox.pin("zzzz")
    with pytest.raises(InboxError, match="2 messages match prefix"):
        inbox.pin("abcd")
    with pytest.raises(InboxError, match="must not be empty"):
        inbox.pin("  ")
    assert inbox.list_messages(pinned_only=True) == []


def test_pin_persists_across_instances_and_unpin_clears(tmp_path):
    path = tmp_path / "inbox.json"
    inbox = Inbox(path)
    inbox.append(_record(id="keep", text="keep me"))
    inbox.append(_record(id="other", text="other"))
    inbox.pin("keep")

    reloaded = Inbox(path)
    assert [entry["id"] for entry in reloaded.list_messages(pinned_only=True)] == [
        "keep"
    ]
    entry, changed = reloaded.unpin("keep")
    assert changed is True
    assert "pinned" not in entry
    assert Inbox(path).list_messages(pinned_only=True) == []


def test_unpin_never_pinned_is_noop(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="plain", text="nothing pinned here"))

    entry, changed = inbox.unpin("plain")
    assert changed is False
    assert "pinned" not in entry


def test_pin_write_failure_rolls_back(tmp_path, monkeypatch):
    import tau.inbox
    from tau.inbox import InboxError

    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="keep", text="keep me"))

    def _boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(tau.inbox, "atomic_write_json", _boom)
    with pytest.raises(InboxError, match="could not persist inbox"):
        inbox.pin("keep")
    # The pin was rolled back: the message is still unpinned.
    assert inbox.list_messages(pinned_only=True) == []


def test_delete_drops_pin_with_message(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="gone", text="bye"))
    inbox.pin("gone")
    assert len(inbox.list_messages(pinned_only=True)) == 1

    inbox.delete("gone")
    assert inbox.list_messages(pinned_only=True) == []


def test_pinned_only_covers_sent_messages_too(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="sent-one", text="sent", direction="out"))
    inbox.append(_record(id="recv-one", text="received"))
    inbox.pin("sent-one")
    inbox.pin("recv-one")

    # The default listing is received-only, so only the received pin shows.
    assert [entry["id"] for entry in inbox.list_messages(pinned_only=True)] == [
        "recv-one"
    ]
    # With direction=None, pinned sent messages show up too, newest first.
    assert [
        entry["id"]
        for entry in inbox.list_messages(pinned_only=True, direction=None)
    ] == ["recv-one", "sent-one"]
    assert [
        entry["id"]
        for entry in inbox.list_messages(pinned_only=True, direction="in")
    ] == ["recv-one"]


def test_pin_file_record(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
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

    entry, changed = inbox.pin("f1")
    assert changed is True
    assert entry["kind"] == "file"
    assert [item["id"] for item in inbox.list_messages(pinned_only=True)] == ["f1"]


def test_export_import_preserves_pin(tmp_path):
    source = Inbox(tmp_path / "source.json")
    source.append(_record(id="one", text="first"))
    source.append(_record(id="two", text="second"))
    source.pin("one")

    target = Inbox(tmp_path / "target.json")
    result = target.import_messages(source.export_document())
    assert result == {"imported": 2, "present": 0, "skipped": 0, "errors": []}
    assert [entry["id"] for entry in target.list_messages(pinned_only=True)] == [
        "one"
    ]


def test_delete_authorized_deletes_only_the_exact_match(tmp_path):
    # The retract race: a short id must never delete a longer id that
    # merely shares its prefix, and a second retract of the same id
    # finds nothing instead of reselecting.
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(
        _record(id="abc", text="short", direction="in", destination="ab" * 16)
    )
    inbox.append(
        _record(id="abcdef" + "1" * 26, text="long", direction="out")
    )

    removed = inbox.delete_authorized("abc", direction="in", destination="ab" * 16)
    assert removed is not None
    assert removed["text"] == "short"
    assert inbox.delete_authorized("abc", direction="in", destination="ab" * 16) is None
    assert [entry["id"] for entry in inbox.list_messages(direction=None)] == [
        "abcdef" + "1" * 26
    ]


def test_delete_authorized_never_matches_a_prefix(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(
        _record(id="abcd" + "1" * 28, text="one", direction="in", destination="ab" * 16)
    )

    assert inbox.delete_authorized("abcd", direction="in", destination="ab" * 16) is None
    assert len(inbox) == 1


def test_delete_authorized_refuses_a_mismatched_record(tmp_path):
    from tau.inbox import InboxError

    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(
        _record(id="msg-1", text="mine", direction="out", destination="ab" * 16)
    )

    with pytest.raises(InboxError):
        inbox.delete_authorized("msg-1", direction="in", destination="ab" * 16)
    with pytest.raises(InboxError):
        inbox.delete_authorized("msg-1", direction="out", destination="cd" * 16)
    assert len(inbox) == 1

def test_apply_edit_replaces_text_and_marks_edited(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="msg-1", text="typo here"))

    updated = inbox.apply_edit("msg-1", "fixed here", edited_at=42.0)
    assert updated is not None
    assert updated["text"] == "fixed here"
    assert updated["edited_at"] == 42.0
    assert inbox.list_messages()[0]["text"] == "fixed here"
    assert inbox.list_messages()[0]["edited_at"] == 42.0

    reloaded = Inbox(tmp_path / "inbox.json")
    assert reloaded.list_messages()[0]["text"] == "fixed here"
    assert reloaded.list_messages()[0]["edited_at"] == 42.0


def test_apply_edit_unknown_id_returns_none(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="msg-1", text="keep me"))

    assert inbox.apply_edit("nope", "new", edited_at=1.0) is None
    assert inbox.list_messages()[0]["text"] == "keep me"


def test_apply_edit_needs_exact_id(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="abcd" + "1" * 28, text="keep me"))

    # A unique prefix is not an edit target: only the full id applies.
    assert inbox.apply_edit("abcd", "new", edited_at=1.0) is None
    assert inbox.list_messages()[0]["text"] == "keep me"


def test_apply_edit_refuses_file_records(tmp_path):
    from tau.inbox import InboxError

    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(
        {
            "kind": "file",
            "from": "alice",
            "destination": "ab" * 16,
            "name": "notes.txt",
            "size": 4,
            "sha256": "a" * 64,
            "path": "/tmp/notes.txt",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "file-1",
        }
    )
    with pytest.raises(InboxError, match="no text to edit"):
        inbox.apply_edit("file-1", "new", edited_at=1.0)


def test_apply_edit_rejects_empty_text(tmp_path):
    from tau.inbox import InboxError

    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="msg-1", text="keep me"))
    with pytest.raises(InboxError, match="must not be empty"):
        inbox.apply_edit("msg-1", "", edited_at=1.0)
    assert inbox.list_messages()[0]["text"] == "keep me"


def test_apply_edit_rejects_a_stale_edit_inside_the_transaction(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(
        _record(id="msg-1", text="newer text", edited_at=200.0)
    )

    # An edit older than the stored one must not overwrite it, even
    # when the caller's snapshot check already passed — the transaction
    # re-checks after its reload.
    assert (
        inbox.apply_edit(
            "msg-1", "older text", edited_at=100.0, newer_than_existing=True
        )
        is None
    )
    assert inbox.list_messages()[0]["text"] == "newer text"
    assert inbox.list_messages()[0]["edited_at"] == 200.0

    updated = inbox.apply_edit(
        "msg-1", "newest text", edited_at=300.0, newer_than_existing=True
    )
    assert updated is not None
    assert updated["text"] == "newest text"
    assert updated["edited_at"] == 300.0


def test_apply_edit_rechecks_direction_inside_the_transaction(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(
        _record(
            id="msg-1",
            text="mine",
            direction="out",
            destination="ab" * 16,
        )
    )

    # A record that is not inbound to the caller cannot be edited by
    # them, even when the caller's snapshot said otherwise.
    assert (
        inbox.apply_edit(
            "msg-1",
            "not yours",
            edited_at=42.0,
            direction="in",
            destination="ab" * 16,
        )
        is None
    )
    assert inbox.list_messages(direction="out")[0]["text"] == "mine"

    updated = inbox.apply_edit(
        "msg-1",
        "fixed here",
        edited_at=42.0,
        direction="out",
        destination="ab" * 16,
    )
    assert updated is not None
    assert updated["text"] == "fixed here"


def test_edited_at_survives_reload(tmp_path):
    path = tmp_path / "inbox.json"
    inbox = Inbox(path)
    inbox.append(_record(id="msg-1", text="original", edited_at=7.0))

    reloaded = Inbox(path)
    assert reloaded.list_messages()[0]["edited_at"] == 7.0


def test_corrupt_edited_at_drops_the_record(tmp_path):
    path = tmp_path / "inbox.json"
    inbox = Inbox(path)
    inbox.append(_record(id="msg-1", text="original", edited_at=7.0))
    document = json.loads(path.read_text(encoding="utf-8"))
    document["messages"][0]["edited_at"] = "not-a-time"
    path.write_text(json.dumps(document), encoding="utf-8")

    reloaded = Inbox(path)
    assert reloaded.list_messages() == []


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


def test_import_skips_retracted_messages(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    document = {
        "format": "tau-messages-export",
        "version": 1,
        "messages": [
            _record(id="gone", destination="aa" * 32, text="retracted"),
            _record(id="kept", destination="aa" * 32, text="still here"),
            # The sender's own copy of a message: its destination is
            # the recipient, so it never matches a retraction marker.
            _record(
                id="mine",
                destination="bb" * 32,
                text="my copy",
                direction="out",
            ),
        ],
    }
    # The check fires for the retracted id only: the received record
    # is skipped — an export made before the retract must not resurrect
    # the message — but the sender's own copy is unaffected.
    result = inbox.import_messages(
        document, is_retracted=lambda sender, message_id: message_id == "gone"
    )
    assert result["imported"] == 2
    assert result["skipped"] == 1
    assert result["errors"] == [
        {"id": "gone", "error": "message was retracted by its sender"}
    ]
    assert [entry["id"] for entry in inbox.list_messages()] == ["kept"]
    assert len(inbox) == 2  # the sent record was imported, not skipped


def test_import_skips_retracted_group_messages(tmp_path):
    # Regression test (CodeRabbit Major on PR #322): group messages
    # are retractable, so an import must check a received group record
    # against both its per-ID marker and its group-context marker.
    inbox = Inbox(tmp_path / "inbox.json")
    sender = "aa" * 32
    group = "cc" * 16
    document = {
        "format": "tau-messages-export",
        "version": 1,
        "messages": [
            # Retracted by group context only: its leg id was never recorded.
            _record(
                id="leg-1",
                destination=sender,
                kind="group",
                group=group,
                sent_at=100.0,
                text="retracted group",
            ),
            # Retracted by per-ID marker only.
            _record(
                id="leg-2",
                destination=sender,
                kind="group",
                group=group,
                sent_at=101.0,
                text="retracted by id",
            ),
            # Same group, different send time: no marker covers it.
            _record(
                id="leg-3",
                destination=sender,
                kind="group",
                group=group,
                sent_at=102.0,
                received_at=30.0,
                text="still here",
            ),
            _record(
                id="kept",
                destination=sender,
                received_at=20.0,
                text="still here",
            ),
        ],
    }
    result = inbox.import_messages(
        document,
        is_retracted=lambda sender_hash, message_id: message_id == "leg-2",
        is_group_retracted=(
            lambda sender_hash, group_id, sent_at: (
                sender_hash,
                group_id,
                sent_at,
            )
            == (sender, group, 100.0)
        ),
    )
    assert result["imported"] == 2
    assert result["skipped"] == 2
    assert result["errors"] == [
        {"id": "leg-1", "error": "message was retracted by its sender"},
        {"id": "leg-2", "error": "message was retracted by its sender"},
    ]
    assert sorted(entry["id"] for entry in inbox.list_messages()) == [
        "kept",
        "leg-3",
    ]


def test_import_applies_a_pending_group_edit(tmp_path):
    # Qodo warning on PR #344: an old export imported after a live
    # edit was answered against an absent copy must land already
    # edited, not resurrect the old text — like the retraction markers
    # above, the import consults the pending edit.
    inbox = Inbox(tmp_path / "inbox.json")
    sender = "aa" * 32
    group = "cc" * 16
    document = {
        "format": "tau-messages-export",
        "version": 1,
        "messages": [
            _record(
                id="leg-1",
                destination=sender,
                kind="group",
                group=group,
                sent_at=100.0,
                text="group typo",
            ),
            # A copy that already carries a newer edit keeps its own text.
            _record(
                id="leg-2",
                destination=sender,
                kind="group",
                group=group,
                sent_at=101.0,
                text="newer fix",
                edited_at=200.0,
            ),
        ],
    }

    def pending(sender_hash, group_id, sent_at):
        if (sender_hash, group_id, sent_at) == (sender, group, 100.0):
            return {"text": "group fixed", "edited_at": 150.0}
        if (sender_hash, group_id, sent_at) == (sender, group, 101.0):
            return {"text": "older fix", "edited_at": 140.0}
        return None

    result = inbox.import_messages(document, pending_group_edit=pending)
    assert result["imported"] == 2
    assert result["skipped"] == 0
    entries = {entry["id"]: entry for entry in inbox.list_messages()}
    assert entries["leg-1"]["text"] == "group fixed"
    assert entries["leg-1"]["edited_at"] == 150.0
    assert entries["leg-2"]["text"] == "newer fix"
    assert entries["leg-2"]["edited_at"] == 200.0


def test_import_dedupes_idless_group_record_after_pending_edit(tmp_path):
    # CodeRabbit Major on PR #344: an id-less group record deduped on
    # its text, so a pending edit changed the stored copy's text and
    # every re-import appended another copy — and after the marker
    # expired the export text still mismatched. Id-less group records
    # now dedupe on (sender, group, sent_at), which never depends on
    # the edited text.
    inbox = Inbox(tmp_path / "inbox.json")
    sender = "aa" * 32
    group = "cc" * 16
    document = {
        "format": "tau-messages-export",
        "version": 1,
        "messages": [
            {
                "from": "alice",
                "destination": sender,
                "kind": "group",
                "group": group,
                "sent_at": 100.0,
                "received_at": 110.0,
                "text": "group typo",
            },
        ],
    }

    def pending(sender_hash, group_id, sent_at):
        if (sender_hash, group_id, sent_at) == (sender, group, 100.0):
            return {"text": "group fixed", "edited_at": 150.0}
        return None

    first = inbox.import_messages(document, pending_group_edit=pending)
    assert first["imported"] == 1
    assert inbox.list_messages()[0]["text"] == "group fixed"
    # Re-import while the marker is still live: no duplicate.
    second = inbox.import_messages(document, pending_group_edit=pending)
    assert second["imported"] == 0
    assert second["present"] == 1
    # Re-import after the marker expires: still no duplicate, and the
    # edited text is not resurrected into the old text.
    third = inbox.import_messages(document)
    assert third["imported"] == 0
    assert third["present"] == 1
    assert len(inbox) == 1
    assert inbox.list_messages()[0]["text"] == "group fixed"


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


def test_unread_counts_ignore_sent_records(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", destination="aa" * 16, received_at=1.0))
    inbox.append(_record(id="a2", destination="aa" * 16, received_at=2.0))
    inbox.mark_read()

    # A sent record past the cursor is your own mail, never unread
    # for you — it must not inflate the counts.
    inbox.append(
        _record(
            id="sent-1",
            destination="aa" * 16,
            received_at=3.0,
            direction="out",
        )
    )
    assert inbox.unread_counts() == {"per_peer": {}, "total": 0}

    # Received mail after the sent record still counts.
    inbox.append(_record(id="a3", destination="aa" * 16, received_at=4.0))
    assert inbox.unread_counts() == {"per_peer": {"aa" * 16: 1}, "total": 1}


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


def test_mark_unread_marks_newest_message_unread_again(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", received_at=1.0))
    inbox.append(_record(id="a2", received_at=2.0))
    inbox.mark_read()

    counts = inbox.mark_unread(peer="ab" * 16)
    assert counts == {"per_peer": {"ab" * 16: 1}, "total": 1}
    listed = inbox.list_messages()
    assert [entry["id"] for entry in listed] == ["a2", "a1"]
    assert [entry["unread"] for entry in listed] == [True, False]


def test_mark_unread_all_marks_whole_conversation_unread(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", received_at=1.0))
    inbox.append(_record(id="a2", received_at=2.0))
    inbox.mark_read()

    counts = inbox.mark_unread(peer="ab" * 16, all=True)
    assert counts == {"per_peer": {"ab" * 16: 2}, "total": 2}
    assert all(entry["unread"] for entry in inbox.list_messages())


def test_mark_unread_is_noop_when_nothing_was_read(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", received_at=1.0))

    counts = inbox.mark_unread(peer="ab" * 16)
    assert counts == {"per_peer": {"ab" * 16: 1}, "total": 1}


def test_mark_unread_on_peer_with_no_messages_is_a_noop(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", received_at=1.0))

    assert inbox.mark_unread(peer="bb" * 16) is None
    assert inbox.unread_counts() == {"per_peer": {"ab" * 16: 1}, "total": 1}


def test_mark_unread_leaves_other_peers_alone(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", destination="aa" * 16, received_at=1.0))
    inbox.append(_record(id="b1", destination="bb" * 16, received_at=2.0))
    inbox.mark_read()

    inbox.mark_unread(peer="aa" * 16)
    assert inbox.unread_counts() == {"per_peer": {"aa" * 16: 1}, "total": 1}


def test_mark_unread_ignores_a_trailing_sent_record(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", received_at=1.0))
    inbox.append(_record(id="a2", received_at=2.0))
    inbox.append(_record(id="s1", direction="out", text="reply", received_at=3.0))
    inbox.mark_read()

    counts = inbox.mark_unread(peer="ab" * 16)
    assert counts == {"per_peer": {"ab" * 16: 1}, "total": 1}
    listed = inbox.list_messages()
    assert [entry["id"] for entry in listed] == ["a2", "a1"]
    assert [entry["unread"] for entry in listed] == [True, False]


def test_mark_unread_does_not_mark_unread_messages_read(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", received_at=1.0))
    inbox.append(_record(id="a2", received_at=2.0))
    inbox.append(_record(id="a3", received_at=3.0))
    inbox.mark_read(up_to_seq=1)

    counts = inbox.mark_unread(peer="ab" * 16)
    assert counts == {"per_peer": {"ab" * 16: 2}, "total": 2}
    listed = inbox.list_messages()
    assert [entry["unread"] for entry in listed] == [True, True, False]


def test_sent_records_do_not_inflate_unread_counts(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", received_at=1.0))
    inbox.mark_read()
    inbox.append(_record(id="s1", direction="out", text="reply", received_at=2.0))

    # A reply this node sent is never unread for its owner.
    assert inbox.unread_counts() == {"per_peer": {}, "total": 0}


def test_mark_unread_and_unread_counts_agree_with_a_trailing_sent_record(tmp_path):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", received_at=1.0))
    inbox.append(_record(id="s1", direction="out", text="reply", received_at=2.0))
    inbox.mark_read()

    counts = inbox.mark_unread(peer="ab" * 16, all=True)
    assert counts == {"per_peer": {"ab" * 16: 1}, "total": 1}
    assert inbox.unread_counts() == counts


def test_mark_unread_raises_when_the_cursor_cannot_persist(tmp_path, monkeypatch):
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="a1", received_at=1.0))
    inbox.append(_record(id="a2", received_at=2.0))
    inbox.mark_read()

    def _fail(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(Inbox, "_save", _fail)
    with pytest.raises(OSError):
        inbox.mark_unread(peer="ab" * 16)

    # The in-memory cursor is restored, and the on-disk state is untouched.
    assert inbox.unread_counts() == {"per_peer": {}, "total": 0}
    assert Inbox(tmp_path / "inbox.json").unread_counts() == {
        "per_peer": {},
        "total": 0,
    }


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


def test_find_matches_group_send_leg_ids(tmp_path):
    """One record per group send keeps every leg id: any leg resolves."""
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(
        _record(
            id="leg-1",
            leg_ids=["leg-1", "leg-2"],
            kind="group",
            group="a" * 32,
            group_name="weekend",
            direction="out",
            destination="",
            text="hello group",
        )
    )

    for leg_id in ("leg-1", "leg-2"):
        assert inbox.find(leg_id, group="a" * 32)["text"] == "hello group"
        assert len(inbox.find_prefix(leg_id[:4], group="a" * 32)) == 1
    assert inbox.find("leg-2", group="b" * 32) is None
    assert inbox.find("missing", group="a" * 32) is None


def test_find_by_excerpt_needs_one_author(tmp_path):
    from tau.protocol import excerpt_hash

    inbox = Inbox(tmp_path / "inbox.json")
    excerpt = excerpt_hash("same words")
    inbox.append(
        _record(
            id="m-1",
            destination="ab" * 16,
            text="same words",
            kind="group",
            group="a" * 32,
            group_name="weekend",
        )
    )
    inbox.append(
        _record(
            id="m-2",
            destination="ab" * 16,
            text="same words",
            kind="group",
            group="a" * 32,
            group_name="weekend",
        )
    )
    # Same author repeating the text still attributes to that author.
    assert inbox.find_by_excerpt(excerpt, group="a" * 32)["id"] == "m-2"
    inbox.append(
        _record(
            id="m-3",
            destination="cd" * 16,
            text="same words",
            kind="group",
            group="a" * 32,
            group_name="weekend",
        )
    )
    # Two authors, same text: the quote cannot be attributed.
    assert inbox.find_by_excerpt(excerpt, group="a" * 32) is None


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


def _group_copy(**overrides):
    record = _record(
        kind="group",
        group="cd" * 16,
        group_name="crew",
        destination="aa" * 16,
        sent_at=100.0,
        received_at=101.0,
        id="local-copy-1",
    )
    record.update(overrides)
    return record


def test_find_group_message_sees_copy_imported_by_another_process(tmp_path):
    # A copy imported by a second process (``tau import`` while ``tau
    # serve`` runs) must be visible to the lookup: the serving inbox's
    # in-memory cache alone would miss it and strand the reaction
    # under a foreign wire id.
    path = tmp_path / "inbox.json"
    inbox = Inbox(path)
    assert inbox.find_group_message("cd" * 16, "aa" * 16, 100.0) is None
    Inbox(path).append(_group_copy())
    found = inbox.find_group_message("cd" * 16, "aa" * 16, 100.0)
    assert found is not None
    assert found["id"] == "local-copy-1"


def test_group_reaction_context_survives_a_reload(tmp_path):
    path = tmp_path / "inbox.json"
    Inbox(path).record_reaction(
        _reaction(
            target="foreign-leg-1",
            group="cd" * 16,
            target_author="aa" * 16,
            target_sent_at=100.0,
        )
    )
    reloaded = Inbox(path)
    stored = reloaded.all_reactions()
    assert len(stored) == 1
    assert stored[0]["group"] == "cd" * 16
    assert stored[0]["target_author"] == "aa" * 16
    assert stored[0]["target_sent_at"] == 100.0


def test_reconcile_group_reactions_retargets_onto_arriving_copy(tmp_path):
    # The reaction arrived before the message: stored under the
    # reactor's foreign wire id, invisible.  When the copy lands, it
    # moves onto the local id so it displays under the message.
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.record_reaction(
        _reaction(
            target="foreign-leg-1",
            group="cd" * 16,
            target_author="aa" * 16,
            target_sent_at=100.0,
        )
    )
    assert inbox.reactions_for("foreign-leg-1") != []
    assert inbox.reactions_for("local-copy-1") == []
    moved = inbox.reconcile_group_reactions(
        "cd" * 16, "aa" * 16, 100.0, "local-copy-1",
        members={"ab" * 16, "aa" * 16},
    )
    assert moved == 1
    assert inbox.reactions_for("foreign-leg-1") == []
    shown = inbox.reactions_for("local-copy-1")
    assert len(shown) == 1
    assert shown[0]["reaction"] == "👍"


def test_reconcile_group_reactions_skips_non_members(tmp_path):
    # An allow-listed outsider's reaction stays stranded: it was
    # stored before the group was pinned, and the roster on arrival
    # does not name the reactor, so it must never surface.
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.record_reaction(
        _reaction(
            target="foreign-leg-1",
            destination="ff" * 16,
            group="cd" * 16,
            target_author="aa" * 16,
            target_sent_at=100.0,
        )
    )
    moved = inbox.reconcile_group_reactions(
        "cd" * 16, "aa" * 16, 100.0, "local-copy-1",
        members={"ab" * 16, "aa" * 16},
    )
    assert moved == 0
    assert inbox.reactions_for("local-copy-1") == []
    assert len(inbox.reactions_for("foreign-leg-1")) == 1


def test_reconcile_group_reactions_keeps_newer_reaction_on_copy(tmp_path):
    # The reactor reacted again after the copy arrived, so the copy
    # already carries their newer reaction: retargeting the older
    # pre-arrival one must not override it.
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.record_reaction(
        _reaction(
            target="local-copy-1",
            sent_at=30.0,
            received_at=31.0,
            id="react-new",
            reaction="❤️",
        )
    )
    inbox.record_reaction(
        _reaction(
            target="foreign-leg-1",
            sent_at=20.0,
            received_at=21.0,
            id="react-old",
            group="cd" * 16,
            target_author="aa" * 16,
            target_sent_at=100.0,
        )
    )
    moved = inbox.reconcile_group_reactions(
        "cd" * 16, "aa" * 16, 100.0, "local-copy-1"
    )
    assert moved == 0
    assert [item["reaction"] for item in inbox.reactions_for("local-copy-1")] == [
        "❤️"
    ]


def test_append_group_message_purges_and_appends_in_one_persist(
    tmp_path, monkeypatch
):
    # CodeRabbit (node.py:1702): the purge used to run in one
    # transaction and the append in the next, so a non-member's
    # reaction stored between them skipped every roster check and
    # became readable once the message appended.  The arrival is one
    # locked step now: the stored reaction is voided and the message
    # appended under a single persist.
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.record_reaction(_reaction(target="local-copy-1", destination="ff" * 16))
    inbox.record_reaction(
        _reaction(
            target="foreign-leg-1",
            destination="ab" * 16,
            group="cd" * 16,
            target_author="aa" * 16,
            target_sent_at=100.0,
        )
    )
    saves = []
    original_save = inbox._save

    def _counting_save(*args, **kwargs):
        saves.append(1)
        return original_save(*args, **kwargs)

    monkeypatch.setattr(inbox, "_save", _counting_save)
    voided = inbox.append_group_message(
        _group_copy(),
        group="cd" * 16,
        author="aa" * 16,
        sent_at=100.0,
        members={"ab" * 16, "aa" * 16},
    )
    assert len(voided) == 1
    assert voided[0]["destination"] == "ff" * 16
    assert voided[0]["reaction"] == ""
    assert len(saves) == 1
    assert len(inbox.list_messages()) == 1
    # The member's context reaction moved onto the copy; the
    # non-member's is a tombstone and never displays.
    shown = inbox.reactions_for("local-copy-1")
    assert len(shown) == 1
    assert shown[0]["reaction"] == "👍"
    assert shown[0]["destination"] == "ab" * 16


def test_append_group_message_skips_duplicate_id(tmp_path):
    # CodeRabbit Major (inbox.py:1267): the append persisted before
    # the seen marker, with no id check — a retry after a failed
    # seen-marker write appended a second copy, which at the cap
    # evicted unrelated history.  A duplicate id now skips the
    # append (the reconcile/purge still run, and the tombstones
    # return) so the retry can finish mark_seen.
    inbox = Inbox(tmp_path / "inbox.json")
    members = {"ab" * 16, "aa" * 16}
    first = inbox.append_group_message(
        _group_copy(),
        group="cd" * 16,
        author="aa" * 16,
        sent_at=100.0,
        members=members,
    )
    assert first == []
    assert len(inbox.list_messages()) == 1
    again = inbox.append_group_message(
        _group_copy(),
        group="cd" * 16,
        author="aa" * 16,
        sent_at=100.0,
        members=members,
    )
    assert again == []
    assert len(inbox.list_messages()) == 1
    assert inbox.find("local-copy-1") is not None


def test_append_group_message_reconcile_failure_does_not_break_arrival(
    tmp_path, monkeypatch
):
    # A failed reconcile only strands legitimate reactions: the purge
    # and the append still run, so the arrival is not refused.
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.record_reaction(_reaction(target="local-copy-1", destination="ff" * 16))

    def _fail(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(inbox, "_reconcile_group_reactions_locked", _fail)
    voided = inbox.append_group_message(
        _group_copy(),
        group="cd" * 16,
        author="aa" * 16,
        sent_at=100.0,
        members={"ab" * 16, "aa" * 16},
    )
    assert len(voided) == 1
    assert len(inbox.list_messages()) == 1
    assert inbox.reactions_for("local-copy-1") == []


def test_append_group_message_save_failure_restores_state(tmp_path, monkeypatch):
    # A failed persist raises and leaves nothing behind: no message
    # recorded, no reaction voided, so the sender can retry the
    # delivery once the store is healthy.
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.record_reaction(_reaction(target="local-copy-1", destination="ff" * 16))
    monkeypatch.setattr(inbox, "_save", lambda *args, **kwargs: False)
    with pytest.raises(OSError):
        inbox.append_group_message(
            _group_copy(),
            group="cd" * 16,
            author="aa" * 16,
            sent_at=100.0,
            members={"ab" * 16, "aa" * 16},
        )
    assert inbox.list_messages() == []
    kept = inbox.reactions_for("local-copy-1")
    assert len(kept) == 1
    assert kept[0]["reaction"] == "👍"


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


def test_resolve_matches_a_group_leg_id(tmp_path):
    # CodeRabbit Major: a group send stores one leg id as the record
    # id and the rest in leg_ids — resolve must match any leg, exactly
    # as find() already does, or a reaction naming the other leg
    # never resolves to the stored record.
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(_record(id="g-1", leg_ids=["g-1", "g-2"]))
    assert inbox.resolve("g-2")["id"] == "g-1"
    assert inbox.resolve("g-1")["id"] == "g-1"
    with pytest.raises(ValueError, match="unknown message"):
        inbox.resolve("g-3")


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


def test_read_only_prune_dry_run_does_not_persist_expiry_purge(tmp_path):
    import json as _json
    import time as _time

    now = _time.time()
    path = tmp_path / "inbox.json"
    inbox = Inbox(path)
    inbox.append(_record(id="gone", received_at=10.0, expires_at=now + 3600))
    inbox.append(_record(id="kept", received_at=10.0))
    # Expire "gone" on disk without the instance noticing.
    raw = _json.loads(path.read_text(encoding="utf-8"))
    for entry in raw["messages"]:
        if entry["id"] == "gone":
            entry["expires_at"] = now - 10
    path.write_text(_json.dumps(raw), encoding="utf-8")
    before = path.read_text(encoding="utf-8")

    preview = Inbox(path, read_only=True)
    result = preview.prune(1000.0, dry_run=True, now=now)
    assert result["pruned"] == 1
    assert result["dry_run"] is True
    # The expired record is not in the preview and nothing was written.
    assert [entry["id"] for entry in preview.list_messages()] == ["kept"]
    assert path.read_text(encoding="utf-8") == before


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


def test_transaction_nests_without_deadlock(tmp_path):
    # The transaction is reentrant (RLock + depth counter, mirroring
    # Mailbox): a handler that must check and store atomically — the
    # reaction handler's final roster check and persist — can hold
    # one transaction across both, instead of deadlocking on the
    # lock or the flock.  The inner append and resolve join the
    # outer transaction; the outer persist still lands on disk.
    inbox = Inbox(tmp_path / "inbox.json")
    with inbox.transaction():
        inbox.append(_record(id="nested-1"))
        assert inbox.resolve("nested-1")["id"] == "nested-1"
    assert inbox.resolve("nested-1")["id"] == "nested-1"
    assert [e["id"] for e in Inbox(tmp_path / "inbox.json").list_messages()] == [
        "nested-1"
    ]


def test_reload_runs_under_the_cross_process_lock(tmp_path):
    # CodeRabbit Major: reload() read inbox.json and wrote the purged
    # snapshot back without the cross-process flock — another process
    # committing an entry between that read and write lost it.  The
    # reload runs inside the transaction now, so holding the flock
    # externally must block it until the lock is released.
    try:
        import fcntl
    except ImportError:  # pragma: no cover - POSIX-only lock
        pytest.skip("fcntl is unavailable")
    import threading

    path = tmp_path / "inbox.json"
    inbox = Inbox(path)
    inbox.append(_record(id="msg-1"))
    lock_path = path.with_name(path.name + ".lock")
    with open(lock_path, "a") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        finished = threading.Event()

        def _reload() -> None:
            inbox.reload()
            finished.set()

        worker = threading.Thread(target=_reload, daemon=True)
        worker.start()
        assert not finished.wait(timeout=1.0)
    assert finished.wait(timeout=5.0)
    worker.join(timeout=5.0)
    assert len(inbox) == 1


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
