"""Unit tests for the read-only data-directory health check (tau doctor)."""

from __future__ import annotations

import json
import sqlite3

import fcntl
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
    ["node.json", "peers.json", "mailbox.json", "inbox.json", "tan-rules.json", "allowlist.json"],
)
def test_corrupt_json_store_fails(tmp_path, filename):
    config = _healthy_dir(tmp_path)
    (tmp_path / filename).write_text("{not json")

    name = {
        "node.json": "node metadata",
        "peers.json": "peers",
        "mailbox.json": "mailbox",
        "inbox.json": "inbox",
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
    config = _healthy_dir(tmp_path)
    lock_path = tmp_path / "node.lock"
    lock_path.write_text("12345")
    with open(lock_path, "r+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        check = _by_name(run_checks(config), "instance lock")
        assert check.status == "ok"
        assert check.detail == "node serving (pid 12345)"

    # Lock released: a leftover lock file is not an error.
    check = _by_name(run_checks(config), "instance lock")
    assert check.status == "ok"
    assert check.detail == "no node serving"


def test_doctor_is_readonly(tmp_path):
    config = _healthy_dir(tmp_path)
    before = {
        path.name: path.read_bytes()
        for path in sorted(tmp_path.iterdir())
        if path.is_file()
    }

    assert run_checks(config)
    assert cli.main(["doctor", "--data-dir", str(tmp_path)]) == 0

    after = {
        path.name: path.read_bytes()
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
