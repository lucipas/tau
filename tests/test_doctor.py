"""Unit tests for the read-only data-directory health check (tau doctor)."""

from __future__ import annotations

import json
import sqlite3

import pytest

from tau import cli
from tau.allowlist import AllowList, save_allowlist
from tau.config import TauConfig, write_node_metadata
from tau.doctor import Check, all_ok, run_checks
from tau.identity import load_or_create_identity
from tau.inbox import Inbox
from tau.mailbox import HeldEnvelope, Mailbox, OutboxEntry
from tau.peers import PeerRegistry
from tau.tangents.rules import Ruleset, save_ruleset
from tau.tangents.tan import init_db


def _config(tmp_path):
    return TauConfig.resolve(data_dir=tmp_path)


def _by_name(checks, name):
    return next(check for check in checks if check.name == name)


def _healthy_dir(tmp_path):
    """Build a data directory every check passes on. Returns the config."""
    load_or_create_identity(tmp_path / "identity")
    write_node_metadata(tmp_path, "doctor-test")
    registry = PeerRegistry(tmp_path / "peers.json")
    registry.upsert(bytes([1]) * 16, bytes([1]) * 64, name="alice", now=100.0)
    registry.save()
    mailbox = Mailbox(tmp_path / "mailbox.json")
    mailbox.enqueue(
        to=bytes([2]) * 16,
        text="hello",
        to_name="bob",
        now=100.0,
    )
    mailbox.hold(
        to=bytes([3]) * 16,
        blob=b"sealed",
        message_id="held-1",
        expires_at=200.0,
        now=100.0,
    )
    mailbox.save()
    inbox = Inbox(tmp_path / "inbox.json")
    inbox.append(
        {
            "from": "alice",
            "destination": "ab" * 16,
            "text": "hi",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "msg-1",
        }
    )
    (tmp_path / "messages.json").write_text(
        json.dumps(
            {
                "messages": [
                    {
                        "id": 1,
                        "direction": "in",
                        "peer": "ab" * 16,
                        "peer_name": "alice",
                        "text": "hi",
                        "sent_at": 10.0,
                        "received_at": 20.0,
                        "status": "delivered",
                    }
                ],
                "next_message_id": 2,
            }
        )
    )
    init_db(tmp_path / "tan.sqlite3").close()
    save_ruleset(tmp_path / "tan-rules.json", Ruleset())
    save_allowlist(tmp_path / "allowlist.json", AllowList(enabled=True, allow=frozenset({"cd" * 16})))
    return _config(tmp_path)


def test_healthy_directory_all_ok(tmp_path):
    config = _healthy_dir(tmp_path)
    checks = run_checks(config)

    assert all_ok(checks)
    assert all(check.status == "ok" for check in checks)
    assert _by_name(checks, "peers").detail == "1 peer(s)"
    assert _by_name(checks, "mailbox").detail == "1 queued, 1 held"
    assert _by_name(checks, "inbox").detail == "1 message(s)"
    assert _by_name(checks, "messages").detail == "1 message(s)"
    assert _by_name(checks, "tan").detail == "0 post(s)"
    assert _by_name(checks, "instance lock").detail == "no node serving"


def test_doctor_cli_exits_zero_on_healthy_directory(tmp_path, capsys):
    _healthy_dir(tmp_path)

    assert cli.main(["doctor", "--data-dir", str(tmp_path)]) == 0
    out = capsys.readouterr().out
    assert "ok" in out
    assert "data directory healthy" in out


def test_doctor_cli_json_output(tmp_path, capsys):
    _healthy_dir(tmp_path)

    assert cli.main(["doctor", "--data-dir", str(tmp_path), "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert {check["name"] for check in payload["checks"]} == {
        "identity",
        "node metadata",
        "peers",
        "mailbox",
        "inbox",
        "messages",
        "tan",
        "tan rules",
        "allowlist",
        "instance lock",
    }
    assert all(check["status"] == "ok" for check in payload["checks"])


def test_missing_identity_fails_and_missing_stores_are_ok(tmp_path):
    checks = run_checks(_config(tmp_path))

    assert _by_name(checks, "identity").status == "fail"
    assert "tau init" in _by_name(checks, "identity").detail
    assert _by_name(checks, "peers").status == "ok"
    assert _by_name(checks, "mailbox").status == "ok"
    assert _by_name(checks, "inbox").status == "ok"
    assert _by_name(checks, "tan").status == "ok"
    assert _by_name(checks, "allowlist").status == "ok"
    assert not all_ok(checks)


def test_corrupt_identity_fails(tmp_path):
    (tmp_path / "identity").write_bytes(b"this is not a private key")

    assert _by_name(run_checks(_config(tmp_path)), "identity").status == "fail"


def test_missing_node_name_warns(tmp_path):
    load_or_create_identity(tmp_path / "identity")
    (tmp_path / "node.json").write_text(json.dumps({"protocol": "tau"}))

    check = _by_name(run_checks(_config(tmp_path)), "node metadata")
    assert check.status == "warn"
    assert "no node name" in check.detail


@pytest.mark.parametrize(
    "filename",
    [
        "node.json",
        "peers.json",
        "mailbox.json",
        "inbox.json",
        "messages.json",
        "tan-rules.json",
        "allowlist.json",
    ],
)
def test_corrupt_json_store_fails(tmp_path, filename):
    config = _healthy_dir(tmp_path)
    (tmp_path / filename).write_text("{not json")

    name = {
        "node.json": "node metadata",
        "peers.json": "peers",
        "mailbox.json": "mailbox",
        "inbox.json": "inbox",
        "messages.json": "messages",
        "tan-rules.json": "tan rules",
        "allowlist.json": "allowlist",
    }[filename]
    check = _by_name(run_checks(config), name)
    assert check.status == "fail"
    assert "could not parse JSON" in check.detail


def test_invalid_peer_entries_warned_not_failed(tmp_path):
    config = _healthy_dir(tmp_path)
    registry = PeerRegistry(tmp_path / "peers.json")
    good = registry.all()[0].to_dict()
    bad = dict(good, destination="not-a-hash")
    (tmp_path / "peers.json").write_text(
        json.dumps({"version": 1, "peers": [good, bad, "junk", good]})
    )

    check = _by_name(run_checks(config), "peers")
    assert check.status == "warn"
    assert "2 invalid entries skipped on load" in check.detail
    assert "1 duplicate(s)" in check.detail
    assert check.detail.startswith("1 peer(s)")


def test_invalid_mailbox_entries_warned(tmp_path):
    config = _healthy_dir(tmp_path)
    good_outbox = OutboxEntry(
        id="m1", to=bytes([2]) * 16, text="hi", created_at=100.0, expires_at=200.0
    ).to_dict()
    good_held = HeldEnvelope(
        id="h1", to=bytes([3]) * 16, blob=b"sealed", stored_at=100.0, expires_at=200.0
    ).to_dict()
    (tmp_path / "mailbox.json").write_text(
        json.dumps(
            {
                "version": 1,
                "outbox": [good_outbox, {"id": "broken"}],
                "held": [good_held, {"blob": "!!! not base64 !!!"}],
            }
        )
    )

    check = _by_name(run_checks(config), "mailbox")
    assert check.status == "warn"
    assert check.detail.startswith("1 queued, 1 held")
    assert "1 invalid outbox entry skipped on load" in check.detail
    assert "1 invalid held envelope(s) skipped on load" in check.detail


def test_invalid_inbox_records_warned(tmp_path):
    config = _healthy_dir(tmp_path)
    (tmp_path / "inbox.json").write_text(
        json.dumps(
            {
                "messages": [
                    {
                        "from": "alice",
                        "destination": "ab" * 16,
                        "text": "hi",
                        "sent_at": 10.0,
                        "received_at": 20.0,
                        "id": "msg-1",
                    },
                    {"from": "alice"},  # no text: unusable, skipped on load
                ]
            }
        )
    )

    check = _by_name(run_checks(config), "inbox")
    assert check.status == "warn"
    assert "1 message(s); 1 invalid record(s) skipped on load" in check.detail


def test_invalid_web_history_records_warned(tmp_path):
    config = _healthy_dir(tmp_path)
    (tmp_path / "messages.json").write_text(
        json.dumps(
            {
                "messages": [
                    {
                        "id": 1,
                        "direction": "in",
                        "peer": "ab" * 16,
                        "peer_name": "alice",
                        "text": "hi",
                        "sent_at": 10.0,
                        "received_at": 20.0,
                        "status": "delivered",
                    },
                    {"id": 2, "direction": "sideways", "text": "not a message"},
                ],
                "next_message_id": 3,
            }
        )
    )

    check = _by_name(run_checks(config), "messages")
    assert check.status == "warn"
    assert "1 message(s); 1 invalid record(s) skipped on load" in check.detail


def test_invalid_utf8_store_fails_without_aborting_report(tmp_path, capsys):
    config = _healthy_dir(tmp_path)
    (tmp_path / "peers.json").write_bytes(b"\xff\xfe{not json")

    checks = run_checks(config)
    assert len(checks) == 10
    assert _by_name(checks, "peers").status == "fail"
    assert "is not valid UTF-8" in _by_name(checks, "peers").detail
    assert _by_name(checks, "mailbox").status == "ok"

    assert cli.main(["doctor", "--data-dir", str(tmp_path), "--json"]) == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert any(
        check["name"] == "peers" and check["status"] == "fail"
        for check in payload["checks"]
    )


def test_duplicate_mailbox_ids_warned(tmp_path):
    config = _healthy_dir(tmp_path)
    entry = OutboxEntry(
        id="m1", to=bytes([2]) * 16, text="first", created_at=100.0, expires_at=200.0
    ).to_dict()
    duplicate = OutboxEntry(
        id="m1", to=bytes([2]) * 16, text="second", created_at=101.0, expires_at=201.0
    ).to_dict()
    held = HeldEnvelope(
        id="h1", to=bytes([3]) * 16, blob=b"sealed", stored_at=100.0, expires_at=200.0
    ).to_dict()
    (tmp_path / "mailbox.json").write_text(
        json.dumps({"version": 1, "outbox": [entry, duplicate], "held": [held, held]})
    )

    check = _by_name(run_checks(config), "mailbox")
    assert check.status == "warn"
    assert check.detail.startswith("1 queued, 1 held")
    assert "1 duplicate outbox entry (later copy overwrites earlier on load)" in check.detail
    assert "1 duplicate held envelope(s) (later copy overwrites earlier on load)" in check.detail


def test_peer_count_over_registry_cap_warned(tmp_path):
    from tau.peers import Peer

    config = _healthy_dir(tmp_path)
    peers = [
        Peer(
            destination_hash=bytes([i // 256, i % 256] + [0] * 14),
            name=f"peer-{i}",
            public_key=bytes([i % 256]) * 64,
            first_seen=100.0,
            last_seen=100.0 + i,
        ).to_dict()
        for i in range(257)
    ]
    (tmp_path / "peers.json").write_text(json.dumps({"version": 1, "peers": peers}))

    check = _by_name(run_checks(config), "peers")
    assert check.status == "warn"
    assert check.detail.startswith("256 peer(s)")
    assert "1 peer(s) evicted on load (limit 256)" in check.detail


def _inbox_record(index):
    return {
        "from": "alice",
        "destination": "ab" * 16,
        "text": f"message {index}",
        "sent_at": 10.0,
        "received_at": 20.0,
        "id": f"msg-{index}",
    }


def test_inbox_over_retention_cap_warned(tmp_path):
    config = _healthy_dir(tmp_path)
    (tmp_path / "inbox.json").write_text(
        json.dumps({"messages": [_inbox_record(i) for i in range(501)]})
    )

    check = _by_name(run_checks(config), "inbox")
    assert check.status == "warn"
    assert check.detail.startswith("500 message(s)")
    assert "1 older message(s) dropped on load (limit 500)" in check.detail


def test_web_history_over_retention_cap_warned(tmp_path):
    config = _healthy_dir(tmp_path)
    (tmp_path / "messages.json").write_text(
        json.dumps(
            {
                "messages": [
                    {
                        "id": i + 1,
                        "direction": "in",
                        "peer": "ab" * 16,
                        "peer_name": "alice",
                        "text": f"message {i}",
                        "sent_at": 10.0,
                        "received_at": 20.0,
                        "status": "delivered",
                    }
                    for i in range(501)
                ],
                "next_message_id": 502,
            }
        )
    )

    check = _by_name(run_checks(config), "messages")
    assert check.status == "warn"
    assert check.detail.startswith("500 message(s)")
    assert "1 older message(s) dropped on load (limit 500)" in check.detail


def test_tan_database_missing_columns_fails(tmp_path):
    config = _healthy_dir(tmp_path)
    (tmp_path / "tan.sqlite3").unlink()
    conn = sqlite3.connect(tmp_path / "tan.sqlite3")
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("CREATE TABLE posts (hash TEXT PRIMARY KEY, author TEXT NOT NULL)")
    conn.execute(
        "CREATE TABLE heads (author TEXT PRIMARY KEY, hash TEXT NOT NULL, seq INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE follows (author TEXT PRIMARY KEY, name TEXT NOT NULL DEFAULT '', added_at REAL NOT NULL)"
    )
    conn.commit()
    conn.close()

    check = _by_name(run_checks(config), "tan")
    assert check.status == "fail"
    assert "missing column(s)" in check.detail


def test_tan_wal_pending_schema_is_unverified_not_failed(tmp_path):
    from tau.tangents.tan import _SCHEMA

    config = _healthy_dir(tmp_path)
    db_path = tmp_path / "tan.sqlite3"
    # A fresh database whose schema lives only in the WAL: the main file
    # has no tables yet, the way a serving node leaves it after init.
    db_path.unlink()
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_SCHEMA)
    conn.commit()
    try:
        assert (tmp_path / "tan.sqlite3-wal").stat().st_size > 0
        check = _by_name(run_checks(config), "tan")
        assert check.status == "warn"
        assert "uncheckpointed WAL changes could not be read" in check.detail
    finally:
        conn.close()


def test_corrupt_tan_database_fails(tmp_path):
    config = _healthy_dir(tmp_path)
    (tmp_path / "tan.sqlite3").write_bytes(b"this is not a sqlite database")

    check = _by_name(run_checks(config), "tan")
    assert check.status == "fail"
    assert "integrity check failed" in check.detail


def test_tan_database_missing_tables_fails(tmp_path):
    config = _healthy_dir(tmp_path)
    # Replace the real store with a schema-less database.
    (tmp_path / "tan.sqlite3").unlink()
    conn = sqlite3.connect(tmp_path / "tan.sqlite3")
    conn.execute("CREATE TABLE unrelated (id INTEGER)")
    conn.commit()
    conn.close()

    check = _by_name(run_checks(config), "tan")
    assert check.status == "fail"
    assert "missing table(s)" in check.detail


def test_instance_lock_reports_serving_node(tmp_path):
    import os

    config = _healthy_dir(tmp_path)
    lock_path = tmp_path / "node.lock"
    # This process is alive, so its PID reads as a serving node.
    lock_path.write_text(str(os.getpid()))
    check = _by_name(run_checks(config), "instance lock")
    assert check.status == "ok"
    assert check.detail == f"node probably serving (pid {os.getpid()})"

    # A leftover lock file from a stopped node is not an error.
    lock_path.write_text("999999")
    check = _by_name(run_checks(config), "instance lock")
    assert check.status == "ok"
    assert check.detail == "no node serving (stale pid 999999)"


def test_instance_lock_unreadable_is_indeterminate(tmp_path):
    config = _healthy_dir(tmp_path)
    lock_path = tmp_path / "node.lock"
    lock_path.write_bytes(b"\xff\xfe not a pid")
    check = _by_name(run_checks(config), "instance lock")
    assert check.status == "warn"
    assert "serving state unknown" in check.detail

    lock_path.write_text("not-a-pid")
    check = _by_name(run_checks(config), "instance lock")
    assert check.status == "warn"
    assert "serving state unknown" in check.detail


def test_doctor_is_readonly(tmp_path):
    import os
    import stat

    config = _healthy_dir(tmp_path)
    # A permissive identity mode must survive the check untouched: the
    # identity loader tightens it to 0600, which doctor must not do.
    os.chmod(tmp_path / "identity", 0o644)
    before = {
        path.name: (path.read_bytes(), stat.S_IMODE(os.stat(path).st_mode))
        for path in sorted(tmp_path.iterdir())
        if path.is_file()
    }

    assert run_checks(config)
    assert cli.main(["doctor", "--data-dir", str(tmp_path)]) == 0

    after = {
        path.name: (path.read_bytes(), stat.S_IMODE(os.stat(path).st_mode))
        for path in sorted(tmp_path.iterdir())
        if path.is_file()
    }
    assert before == after


def test_check_to_dict_and_all_ok():
    assert Check("x", "ok", "fine").to_dict() == {
        "name": "x",
        "status": "ok",
        "detail": "fine",
    }
    assert all_ok([Check("x", "warn", "hmm")])
    assert not all_ok([Check("x", "fail", "bad")])
