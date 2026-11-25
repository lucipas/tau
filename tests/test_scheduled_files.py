from __future__ import annotations

import hashlib
import json
import threading
import time
from pathlib import Path
from types import MethodType, SimpleNamespace

import pytest

from tau.node import NodeError, TauNode
from tau.scheduled_files import (
    CLAIM_LEASE_SECONDS,
    MAX_SEND_ATTEMPTS,
    PartialSend,
    SEND_RETRY_BASE_SECONDS,
    ScheduledFile,
    ScheduledFileError,
    ScheduledFiles,
    cancel_scheduled_file,
    load_scheduled_files,
    publish_due_scheduled_files,
    read_file_payload,
    remove_staged_file,
    save_scheduled_files,
    schedule_file,
    stage_file,
    staged_file_path,
)

FILE = {
    "sha256": hashlib.sha256(b"hello").hexdigest(),
    "size": 5,
    "name": "notes.txt",
}


def _entry(
    entry_id: str = "a" * 16,
    *,
    target: str = "alice",
    kind: str = "dm",
    publish_at: float = 1000.0,
    **overrides,
):
    values = {
        "id": entry_id,
        "target": target,
        "kind": kind,
        "file": dict(FILE),
        "publish_at": publish_at,
        "created_at": 900.0,
    }
    values.update(overrides)
    return ScheduledFile(**values)


def test_schedule_and_load_round_trip(tmp_path):
    path = tmp_path / "scheduled-files.json"
    entry = schedule_file(
        path,
        target="alice",
        kind="dm",
        file=dict(FILE),
        publish_at=2000.0,
        now=1000.0,
    )
    queue = load_scheduled_files(path)
    assert len(queue) == 1
    assert queue.files[0] == entry
    assert entry.target == "alice"
    assert entry.kind == "dm"
    assert entry.file == FILE
    assert entry.publish_at == 2000.0
    assert entry.created_at == 1000.0


def test_retry_state_survives_a_save_round_trip(tmp_path):
    # The retry fields are load-bearing: a save that drops them would
    # reset every failed file to zero attempts and retry it forever
    # at the base interval instead of backing off.
    path = tmp_path / "scheduled-files.json"
    entry = _entry(attempts=2, next_attempt_at=5000.0)
    save_scheduled_files(path, ScheduledFiles(files=(entry,)))
    assert load_scheduled_files(path).files[0] == entry


def test_load_missing_file_is_empty(tmp_path):
    assert len(load_scheduled_files(tmp_path / "missing.json")) == 0


def test_load_corrupt_file_raises(tmp_path):
    path = tmp_path / "scheduled-files.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(ScheduledFileError):
        load_scheduled_files(path)


def test_schedule_rejects_bad_kind(tmp_path):
    with pytest.raises(ScheduledFileError):
        schedule_file(
            tmp_path / "scheduled-files.json",
            target="alice",
            kind="channel",
            file=dict(FILE),
            publish_at=2000.0,
        )


def test_schedule_rejects_empty_target(tmp_path):
    with pytest.raises(ScheduledFileError):
        schedule_file(
            tmp_path / "scheduled-files.json",
            target="   ",
            kind="dm",
            file=dict(FILE),
            publish_at=2000.0,
        )


def test_schedule_rejects_bad_descriptor(tmp_path):
    path = tmp_path / "scheduled-files.json"
    for bad in (
        {"sha256": "zz" * 32, "size": 5, "name": "notes.txt"},
        {"sha256": "a" * 64, "size": 0, "name": "notes.txt"},
        {"sha256": "a" * 64, "size": 10 * 1024 * 1024 + 1, "name": "notes.txt"},
        {"sha256": "a" * 64, "size": 5, "name": "../escape.txt"},
        {"sha256": "a" * 64, "size": 5, "name": ""},
    ):
        with pytest.raises(ScheduledFileError):
            schedule_file(
                path, target="alice", kind="dm", file=bad, publish_at=2000.0
            )
    assert len(load_scheduled_files(path)) == 0


def test_schedule_keeps_voice_marker_and_drops_non_bool(tmp_path):
    path = tmp_path / "scheduled-files.json"
    entry = schedule_file(
        path,
        target="alice",
        kind="dm",
        file={**FILE, "voice": True},
        publish_at=2000.0,
    )
    assert entry.file["voice"] is True
    assert load_scheduled_files(path).files[0].file["voice"] is True

    path = tmp_path / "other.json"
    entry = schedule_file(
        path,
        target="alice",
        kind="dm",
        file={**FILE, "voice": "yes"},
        publish_at=2000.0,
    )
    assert "voice" not in entry.file


def test_schedule_with_explicit_id_uses_it(tmp_path):
    path = tmp_path / "scheduled-files.json"
    entry = schedule_file(
        path,
        target="alice",
        kind="dm",
        file=dict(FILE),
        publish_at=2000.0,
        file_id="b" * 16,
    )
    assert entry.id == "b" * 16


def test_resolve_prefix_and_ambiguity(tmp_path):
    path = tmp_path / "scheduled-files.json"
    first = schedule_file(
        path, target="alice", kind="dm", file=dict(FILE), publish_at=2000.0,
        file_id="ab" + "0" * 14,
    )
    schedule_file(
        path, target="bob", kind="dm", file=dict(FILE), publish_at=2000.0,
        file_id="ab" + "1" * 14,
    )
    queue = load_scheduled_files(path)
    assert queue.resolve(first.id[:6]) == first
    with pytest.raises(ScheduledFileError, match="ambiguous"):
        queue.resolve("ab")
    with pytest.raises(ScheduledFileError, match="no scheduled file"):
        queue.resolve("ff" * 8)


def test_cancel_removes_entry_and_runs_callback(tmp_path):
    path = tmp_path / "scheduled-files.json"
    entry = schedule_file(
        path, target="alice", kind="dm", file=dict(FILE), publish_at=2000.0,
    )
    seen = []
    cancelled = cancel_scheduled_file(path, entry.id[:8], on_cancel=seen.append)
    assert cancelled == entry
    assert seen == [entry]
    assert len(load_scheduled_files(path)) == 0
    with pytest.raises(ScheduledFileError):
        cancel_scheduled_file(path, entry.id)


def test_read_file_payload_validates(tmp_path):
    source = tmp_path / "hello.txt"
    source.write_bytes(b"hello")
    data, name, digest = read_file_payload(source)
    assert data == b"hello"
    assert name == "hello.txt"
    assert digest == hashlib.sha256(b"hello").hexdigest()
    with pytest.raises(ScheduledFileError, match="no such file"):
        read_file_payload(tmp_path / "missing.txt")
    big = tmp_path / "big.bin"
    big.write_bytes(b"x" * (10 * 1024 * 1024 + 1))
    with pytest.raises(ScheduledFileError, match="outside the"):
        read_file_payload(big)


def test_stage_and_remove_round_trip(tmp_path):
    staged_dir = tmp_path / "scheduled-files"
    staged = stage_file(staged_dir, "c" * 16, "notes.txt", b"hello")
    assert staged == staged_dir / ("c" * 16) / "notes.txt"
    assert staged.read_bytes() == b"hello"
    assert staged_file_path(staged_dir, "c" * 16, "notes.txt") == staged
    remove_staged_file(staged_dir, "c" * 16)
    assert not (staged_dir / ("c" * 16)).exists()
    # Removing again is a no-op, not an error.
    remove_staged_file(staged_dir, "c" * 16)


def _seed(tmp_path, *, publish_at=1000.0, attempts=0, next_attempt_at=0.0):
    path = tmp_path / "scheduled-files.json"
    staged_dir = tmp_path / "scheduled-files"
    entry = schedule_file(
        path,
        target="alice",
        kind="dm",
        file=dict(FILE),
        publish_at=publish_at,
        now=900.0,
    )
    stage_file(staged_dir, entry.id, "notes.txt", b"hello")
    if attempts or next_attempt_at:
        queue = load_scheduled_files(path)
        updated = ScheduledFile(
            **{**entry.to_dict(), "attempts": attempts,
               "next_attempt_at": next_attempt_at}
        )
        save_scheduled_files(path, queue.with_replaced(updated))
        entry = updated
    return path, staged_dir, entry


def test_publish_due_sends_and_cleans_up(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)
    sent = []

    def _send(sent_entry, staged):
        assert staged.read_bytes() == b"hello"
        sent.append((sent_entry, staged))

    result = publish_due_scheduled_files(path, staged_dir, _send, now=2000.0)
    assert result == {"published": 1, "kept": 0}
    # The send callable receives the claimed entry.
    from dataclasses import replace as _replace

    assert [item[0] for item in sent] == [_replace(entry, claimed_at=2000.0)]
    assert len(load_scheduled_files(path)) == 0
    assert not (staged_dir / entry.id).exists()


def test_publish_skips_not_yet_due(tmp_path):
    path, staged_dir, entry = _seed(tmp_path, publish_at=5000.0)
    sent = []
    result = publish_due_scheduled_files(
        path, staged_dir, lambda e, p: sent.append(e), now=2000.0
    )
    assert result == {"published": 0, "kept": 0}
    assert sent == []
    assert len(load_scheduled_files(path)) == 1


def test_publish_failure_keeps_entry_with_backoff(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)

    def _fail(sent_entry, staged):
        raise RuntimeError("peer unreachable")

    result = publish_due_scheduled_files(path, staged_dir, _fail, now=2000.0)
    assert result == {"published": 0, "kept": 1}
    kept = load_scheduled_files(path).files[0]
    assert kept.attempts == 1
    assert kept.next_attempt_at == 2000.0 + SEND_RETRY_BASE_SECONDS
    # Staged bytes survive a failed attempt for the retry.
    assert (staged_dir / entry.id / "notes.txt").read_bytes() == b"hello"
    # And the backoff is honoured: not due again until it elapses.
    sent = []
    result = publish_due_scheduled_files(
        path, staged_dir, lambda e, p: sent.append(e), now=2059.0
    )
    assert result == {"published": 0, "kept": 0}
    assert sent == []


def test_publish_gives_up_after_max_attempts(tmp_path):
    path, staged_dir, entry = _seed(
        tmp_path, attempts=MAX_SEND_ATTEMPTS - 1, next_attempt_at=0.0
    )

    def _fail(sent_entry, staged):
        raise RuntimeError("peer unreachable")

    result = publish_due_scheduled_files(path, staged_dir, _fail, now=2000.0)
    assert result == {"published": 0, "kept": 1}
    kept = load_scheduled_files(path).files[0]
    assert kept.attempts == MAX_SEND_ATTEMPTS
    # At the cap it is no longer retried, but stays queued.
    sent = []
    result = publish_due_scheduled_files(
        path, staged_dir, lambda e, p: sent.append(e), now=99999.0
    )
    assert result == {"published": 0, "kept": 0}
    assert sent == []
    assert len(load_scheduled_files(path)) == 1


def test_publish_skips_cancelled_entry(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)
    cancel_scheduled_file(path, entry.id)
    sent = []
    result = publish_due_scheduled_files(
        path, staged_dir, lambda e, p: sent.append(e), now=2000.0
    )
    assert result == {"published": 0, "kept": 0}
    assert sent == []


def test_publish_stops_when_asked(tmp_path):
    path, staged_dir, first = _seed(tmp_path)
    second = schedule_file(
        path, target="bob", kind="dm", file=dict(FILE), publish_at=1000.0,
        now=900.0,
    )
    stage_file(staged_dir, second.id, "notes.txt", b"hello")
    sent = []
    result = publish_due_scheduled_files(
        path,
        staged_dir,
        lambda e, p: sent.append(e),
        now=2000.0,
        should_stop=lambda: True,
    )
    assert result == {"published": 0, "kept": 2}
    assert sent == []


def test_publish_corrupt_queue_sends_nothing(tmp_path):
    path = tmp_path / "scheduled-files.json"
    path.write_text("{not json", encoding="utf-8")
    sent = []
    result = publish_due_scheduled_files(
        path, tmp_path / "scheduled-files",
        lambda e, p: sent.append(e), now=2000.0,
    )
    assert result == {"published": 0, "kept": 0}
    assert sent == []


def test_due_orders_oldest_first(tmp_path):
    path = tmp_path / "scheduled-files.json"
    later = schedule_file(
        path, target="bob", kind="dm", file=dict(FILE), publish_at=3000.0,
        now=900.0,
    )
    earlier = schedule_file(
        path, target="alice", kind="dm", file=dict(FILE), publish_at=1000.0,
        now=900.0,
    )
    queue = load_scheduled_files(path)
    assert queue.due(now=5000.0) == [earlier, later]


def _fake_node(tmp_path, path, staged_dir, **overrides):
    config = SimpleNamespace(
        scheduled_files_path=path, scheduled_files_dir=staged_dir
    )
    node = SimpleNamespace(
        config=config,
        _stop_event=threading.Event(),
        _scheduled_files_thread=None,
        send_file=lambda *args, **kwargs: {"ok": True},
        send_group_file=lambda *args, **kwargs: {"ok": True, "results": []},
    )
    for key, value in overrides.items():
        setattr(node, key, value)
    return node


def test_node_hook_sends_due_file_on_worker_thread(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)
    sent = []

    def _send_file(target, staged, **kwargs):
        assert staged.read_bytes() == b"hello"
        sent.append((target, staged))
        return {"ok": True}

    node = _fake_node(tmp_path, path, staged_dir, send_file=_send_file)
    node._send_scheduled_file = MethodType(
        TauNode._send_scheduled_file, node
    )
    TauNode._publish_due_scheduled_files(node)
    # The pass runs off the mail-loop thread: wait for it to finish.
    node._scheduled_files_thread.join(timeout=10.0)
    assert not node._scheduled_files_thread.is_alive()
    assert sent == [("alice", staged_dir / entry.id / "notes.txt")]
    assert len(load_scheduled_files(path)) == 0
    assert not (staged_dir / entry.id).exists()


def test_node_hook_skips_while_a_pass_is_running(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)
    started = threading.Event()
    release = threading.Event()

    def _slow_send(target, staged, **kwargs):
        started.set()
        assert release.wait(timeout=10.0)
        return {"ok": True}

    node = _fake_node(tmp_path, path, staged_dir, send_file=_slow_send)
    node._send_scheduled_file = MethodType(
        TauNode._send_scheduled_file, node
    )
    TauNode._publish_due_scheduled_files(node)
    assert started.wait(timeout=10.0)
    # A second mail-loop pass while the first is still working spawns
    # nothing — the entry must not be sent twice.
    TauNode._publish_due_scheduled_files(node)
    release.set()
    node._scheduled_files_thread.join(timeout=10.0)
    assert len(load_scheduled_files(path)) == 0


def test_node_send_rejects_staged_bytes_that_mismatch(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)
    staged = staged_dir / entry.id / "notes.txt"
    staged.write_bytes(b"different bytes")
    node = _fake_node(tmp_path, path, staged_dir)
    with pytest.raises(ScheduledFileError, match="do not match"):
        TauNode._send_scheduled_file(node, entry, staged)


def test_node_send_group_partial_failure_raises(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)
    members = ("a" * 32, "b" * 32, "c" * 32)
    group_entry = ScheduledFile(
        **{
            **entry.to_dict(),
            "kind": "group",
            "target": "weekend",
            "group_id": "d" * 32,
            "group_members": members,
        }
    )

    def _fail_group(group, staged, **kwargs):
        assert group["id"] == "d" * 32
        return {
            "ok": False,
            "results": [
                {"to": "a" * 32, "to_name": "alice", "delivered": True},
                {"to": "b" * 32, "to_name": "bob", "error": "no route"},
                {"to": "c" * 32, "to_name": "carol", "error": "no route"},
            ],
        }

    node = _fake_node(tmp_path, path, staged_dir, send_group_file=_fail_group)
    # The fake node resolves the group by id and reports the roster.
    record = {"id": "d" * 32, "members": list(members)}

    def _find(query):
        if query == "d" * 32:
            return record
        raise ValueError(f"unknown group: {query!r}")

    node.groups = SimpleNamespace(find=_find)
    staged = staged_dir / entry.id / "notes.txt"
    with pytest.raises(PartialSend, match="2 member") as caught:
        TauNode._send_scheduled_file(node, group_entry, staged)
    assert caught.value.delivered_members == ("a" * 32,)

def test_node_send_group_uses_entry_timeouts(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)
    seen = {}

    def _send_file(target, staged, **kwargs):
        seen.update(kwargs)
        return {"ok": True}

    node = _fake_node(tmp_path, path, staged_dir, send_file=_send_file)
    entry = ScheduledFile(
        **{**entry.to_dict(), "timeout": 60.0, "discover_timeout": 5.0}
    )
    TauNode._send_scheduled_file(
        node, entry, staged_dir / entry.id / "notes.txt"
    )
    assert seen == {"timeout": 60.0, "discover_timeout": 5.0, "voice": False}


def test_node_send_group_refuses_changed_roster(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)
    members = ("a" * 32, "b" * 32)
    group_entry = ScheduledFile(
        **{
            **entry.to_dict(),
            "kind": "group",
            "target": "weekend",
            "group_id": "d" * 32,
            "group_members": members,
        }
    )
    record = {"id": "d" * 32, "members": ["a" * 32, "e" * 32]}
    node = _fake_node(tmp_path, path, staged_dir)
    node.groups = SimpleNamespace(find=lambda query: record)
    with pytest.raises(NodeError, match="roster changed"):
        TauNode._send_scheduled_file(
            node, group_entry, staged_dir / entry.id / "notes.txt"
        )


def test_node_send_group_refuses_unknown_group(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)
    group_entry = ScheduledFile(
        **{
            **entry.to_dict(),
            "kind": "group",
            "target": "weekend",
            "group_id": "d" * 32,
            "group_members": ("a" * 32, "b" * 32),
        }
    )

    def _find(query):
        raise ValueError(f"unknown group: {query!r}")

    node = _fake_node(tmp_path, path, staged_dir)
    node.groups = SimpleNamespace(find=_find)
    with pytest.raises(NodeError, match="no longer exists"):
        TauNode._send_scheduled_file(
            node, group_entry, staged_dir / entry.id / "notes.txt"
        )


def test_node_send_broadcast_partial_failure_raises(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)
    members = ("a" * 32, "b" * 32, "c" * 32)
    broadcast_entry = ScheduledFile(
        **{
            **entry.to_dict(),
            "kind": "broadcast",
            "target": "team",
            "group_members": members,
        }
    )

    def _fail_broadcast(broadcast, staged, **kwargs):
        assert broadcast["name"] == "team"
        return {
            "ok": False,
            "results": [
                {"to": "a" * 32, "to_name": "alice", "delivered": True},
                {"to": "b" * 32, "to_name": "bob", "error": "no route"},
                {"to": "c" * 32, "to_name": "carol", "error": "no route"},
            ],
        }

    node = _fake_node(
        tmp_path, path, staged_dir, send_broadcast_file=_fail_broadcast
    )
    # The fake node resolves the list by name and reports its members.
    record = {"name": "team", "members": list(members)}

    def _find(query):
        if query == "team":
            return record
        raise ValueError(f"unknown broadcast list: {query!r}")

    node.broadcasts = SimpleNamespace(find=_find)
    staged = staged_dir / entry.id / "notes.txt"
    with pytest.raises(PartialSend, match="2 member") as caught:
        TauNode._send_scheduled_file(node, broadcast_entry, staged)
    assert caught.value.delivered_members == ("a" * 32,)


def test_node_send_broadcast_uses_entry_timeouts_and_pending(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)
    seen = {}

    def _send_broadcast(broadcast, staged, **kwargs):
        seen.update(kwargs)
        return {"ok": True, "results": []}

    node = _fake_node(
        tmp_path, path, staged_dir, send_broadcast_file=_send_broadcast
    )
    members = ("a" * 32, "b" * 32)
    record = {"name": "team", "members": list(members)}
    node.broadcasts = SimpleNamespace(find=lambda query: record)
    entry = ScheduledFile(
        **{
            **entry.to_dict(),
            "kind": "broadcast",
            "target": "team",
            "group_members": members,
            "pending_members": ("b" * 32,),
            "timeout": 60.0,
            "discover_timeout": 5.0,
            "file": {**FILE, "voice": True},
        }
    )
    TauNode._send_scheduled_file(
        node, entry, staged_dir / entry.id / "notes.txt"
    )
    assert seen == {
        "timeout": 60.0,
        "discover_timeout": 5.0,
        "only": ["b" * 32],
        "voice": True,
    }


def test_node_send_broadcast_refuses_changed_members(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)
    members = ("a" * 32, "b" * 32)
    broadcast_entry = ScheduledFile(
        **{
            **entry.to_dict(),
            "kind": "broadcast",
            "target": "team",
            "group_members": members,
        }
    )
    record = {"name": "team", "members": ["a" * 32, "e" * 32]}
    node = _fake_node(tmp_path, path, staged_dir)
    node.broadcasts = SimpleNamespace(find=lambda query: record)
    with pytest.raises(NodeError, match="members changed"):
        TauNode._send_scheduled_file(
            node, broadcast_entry, staged_dir / entry.id / "notes.txt"
        )


def test_node_send_broadcast_refuses_unknown_list(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)
    broadcast_entry = ScheduledFile(
        **{
            **entry.to_dict(),
            "kind": "broadcast",
            "target": "team",
            "group_members": ("a" * 32, "b" * 32),
        }
    )

    def _find(query):
        raise ValueError(f"unknown broadcast list: {query!r}")

    node = _fake_node(tmp_path, path, staged_dir)
    node.broadcasts = SimpleNamespace(find=_find)
    with pytest.raises(NodeError, match="no longer exists"):
        TauNode._send_scheduled_file(
            node, broadcast_entry, staged_dir / entry.id / "notes.txt"
        )


def test_stop_workers_joins_scheduled_files_thread(tmp_path):
    import time as _time

    started = threading.Event()
    finished = threading.Event()

    def _worker():
        started.set()
        _time.sleep(0.5)
        finished.set()

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    assert started.wait(timeout=5.0)
    node = SimpleNamespace(
        _stop_event=threading.Event(),
        _announce_thread=None,
        _mail_thread=None,
        _scheduled_files_thread=thread,
    )
    before = _time.monotonic()
    TauNode._stop_workers(node)
    assert _time.monotonic() - before >= 0.4
    assert not thread.is_alive()
    assert finished.is_set()


def test_schedule_stores_group_snapshot_and_timeouts(tmp_path):
    path = tmp_path / "scheduled-files.json"
    members = ("a" * 32, "b" * 32)
    entry = schedule_file(
        path,
        target="weekend",
        kind="group",
        file=dict(FILE),
        publish_at=2000.0,
        now=1000.0,
        group_id="d" * 32,
        group_members=members,
        timeout=60.0,
        discover_timeout=5.0,
    )
    loaded = load_scheduled_files(path).files[0]
    assert loaded == entry
    assert loaded.group_id == "d" * 32
    assert loaded.group_members == members
    assert loaded.timeout == 60.0
    assert loaded.discover_timeout == 5.0


def test_cancel_refuses_fresh_claim_but_not_stale_one(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)
    queue = load_scheduled_files(path)
    claimed = ScheduledFile(**{**entry.to_dict(), "claimed_at": 2000.0})
    save_scheduled_files(path, queue.with_replaced(claimed))
    with pytest.raises(ScheduledFileError, match="already sending"):
        cancel_scheduled_file(path, entry.id, now=2000.0)
    # A stale claim (older than the lease) does not block the cancel.
    cancelled = cancel_scheduled_file(
        path, entry.id, now=2000.0 + CLAIM_LEASE_SECONDS + 1
    )
    assert cancelled.id == entry.id
    assert len(load_scheduled_files(path)) == 0


def test_claim_cleared_on_failure(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)

    def _fail(sent_entry, staged):
        raise RuntimeError("peer unreachable")

    result = publish_due_scheduled_files(path, staged_dir, _fail, now=2000.0)
    assert result == {"published": 0, "kept": 1}
    kept = load_scheduled_files(path).files[0]
    assert kept.attempts == 1
    assert kept.claimed_at == 0.0


def test_partial_send_remembers_owed_members(tmp_path):
    path = tmp_path / "scheduled-files.json"
    staged_dir = tmp_path / "scheduled-files"
    members = ("a" * 32, "b" * 32, "c" * 32)
    entry = schedule_file(
        path,
        target="weekend",
        kind="group",
        file=dict(FILE),
        publish_at=1000.0,
        now=900.0,
        group_id="d" * 32,
        group_members=members,
    )
    stage_file(staged_dir, entry.id, "notes.txt", b"hello")

    def _partial(sent_entry, staged):
        raise PartialSend("2 members failed", delivered_members=("a" * 32,))

    result = publish_due_scheduled_files(path, staged_dir, _partial, now=2000.0)
    assert result == {"published": 0, "kept": 1}
    kept = load_scheduled_files(path).files[0]
    assert kept.pending_members == ("b" * 32, "c" * 32)
    assert kept.claimed_at == 0.0
    # The retry is offered only the members still owed.
    seen = []

    def _succeed(sent_entry, staged):
        seen.append(sent_entry.pending_members)

    result = publish_due_scheduled_files(
        path, staged_dir, _succeed, now=3000.0
    )
    assert result == {"published": 1, "kept": 0}
    assert seen == [("b" * 32, "c" * 32)]


def test_failure_retry_measured_from_failure_time(tmp_path, monkeypatch):
    path, staged_dir, entry = _seed(tmp_path)

    def _fail(sent_entry, staged):
        raise RuntimeError("peer unreachable")

    # Pass time 2000.0, failure time 2100.0: the retry must start
    # from the failure, not from the pass.  The fake clock returns
    # 2000.0 once (the pass start) and 2100.0 after that, so stray
    # time.time() calls elsewhere cannot disturb the assertion.
    calls = []

    def _fake_time():
        calls.append(1)
        return 2000.0 if len(calls) == 1 else 2100.0

    monkeypatch.setattr(time, "time", _fake_time)
    result = publish_due_scheduled_files(path, staged_dir, _fail, now=None)
    assert result == {"published": 0, "kept": 1}
    kept = load_scheduled_files(path).files[0]
    assert kept.next_attempt_at == 2100.0 + SEND_RETRY_BASE_SECONDS


def test_remove_staged_file_raises_when_deletion_fails(tmp_path):
    staged_dir = tmp_path / "scheduled-files"
    # A file where the id directory should be: rmtree cannot remove it.
    target = staged_dir / ("e" * 16)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"not a directory")
    with pytest.raises(OSError):
        remove_staged_file(staged_dir, "e" * 16)


def test_oversized_timeout_and_timestamp_raise_not_crash(tmp_path):
    path = tmp_path / "scheduled-files.json"
    data = {"files": [_entry().to_dict()]}
    # A JSON integer beyond the float range: float() would raise
    # OverflowError; loading must raise ScheduledFileError instead,
    # so the publish pass can skip the queue rather than aborting.
    data["files"][0]["timeout"] = 10**400
    path.write_text(json.dumps(data))
    with pytest.raises(ScheduledFileError):
        load_scheduled_files(path)
    data["files"][0]["timeout"] = 300.0
    data["files"][0]["publish_at"] = 10**400
    path.write_text(json.dumps(data))
    with pytest.raises(ScheduledFileError):
        load_scheduled_files(path)
    data["files"][0]["publish_at"] = 1000.0
    data["files"][0]["claimed_at"] = 10**400
    path.write_text(json.dumps(data))
    with pytest.raises(ScheduledFileError):
        load_scheduled_files(path)
    # And the publish pass treats such a queue as corrupt: it sends
    # nothing instead of raising out of the mail loop.
    sent = []
    result = publish_due_scheduled_files(
        path, tmp_path / "scheduled-files", lambda e, p: sent.append(e),
        now=2000.0,
    )
    assert result == {"published": 0, "kept": 0}
    assert sent == []


def test_claim_stamped_at_acquisition_not_pass_start(tmp_path):
    path, staged_dir, first = _seed(tmp_path)
    second = schedule_file(
        path,
        target="bob",
        kind="dm",
        file=dict(FILE),
        publish_at=1000.0,
        now=900.0,
    )
    stage_file(staged_dir, second.id, "notes.txt", b"hello")
    seen = []

    def _slow(sent_entry, staged):
        if not seen:
            time.sleep(0.3)
        seen.append(sent_entry)

    result = publish_due_scheduled_files(path, staged_dir, _slow)
    assert result == {"published": 2, "kept": 0}
    assert len(seen) == 2
    # The second entry's claim was stamped when its transfer began,
    # after the first (slow) transfer — not at the pass start.
    assert seen[1].claimed_at >= seen[0].claimed_at + 0.2


def test_renewal_keeps_long_transfer_claim_fresh(tmp_path):
    path, staged_dir, entry = _seed(tmp_path)
    observed = []

    def _slow(sent_entry, staged):
        time.sleep(0.3)
        observed.append(load_scheduled_files(path).files[0].claimed_at)
        # A cancel mid-transfer must refuse while the claim is live.
        with pytest.raises(ScheduledFileError, match="already sending"):
            cancel_scheduled_file(path, entry.id)
        time.sleep(0.3)
        observed.append(load_scheduled_files(path).files[0].claimed_at)

    result = publish_due_scheduled_files(
        path, staged_dir, _slow, claim_renew_seconds=0.1
    )
    assert result == {"published": 1, "kept": 0}
    assert len(observed) == 2
    # Renewal re-stamped the claim while the transfer ran.
    assert observed[1] > observed[0]
