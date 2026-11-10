from __future__ import annotations

import io
import json
import os
import sqlite3
import stat
import tarfile

import pytest

from tau import cli
from tau.backup import BACKUP_FILES, MANIFEST_NAME, create_backup, restore_backup
from tau.config import TauConfig
from tau.identity import destination_hash, load_identity
from tau.node import InstanceLock
from tau.tangents.tan import init_db


def _sqlite_dump(path):
    """Logical dump of a SQLite file, for comparing database content."""
    conn = sqlite3.connect(str(path))
    try:
        return "\n".join(conn.iterdump())
    finally:
        conn.close()


def _init_node(tmp_path, dirname, name):
    data_dir = tmp_path / dirname
    assert (
        cli.main(["init", "--data-dir", str(data_dir), "--name", name, "--json"]) == 0
    )
    return data_dir


def _id_destination(data_dir, capsys):
    assert cli.main(["id", "--data-dir", str(data_dir), "--json"]) == 0
    return json.loads(capsys.readouterr().out)["destination"]


def _seed_state(data_dir):
    """Write one of every back-up file so the round-trip is meaningful."""
    (data_dir / "peers.json").write_text('{"peers": []}\n', encoding="utf-8")
    (data_dir / "mailbox.json").write_text('{"outbox": []}\n', encoding="utf-8")
    (data_dir / "inbox.json").write_text('{"messages": []}\n', encoding="utf-8")
    (data_dir / "messages.json").write_text('{"messages": []}\n', encoding="utf-8")
    (data_dir / "tan-rules.json").write_text('{"kinds": ["post"]}\n', encoding="utf-8")
    (data_dir / "allowlist.json").write_text('{"allowed": []}\n', encoding="utf-8")
    conn = init_db(data_dir / "tan.sqlite3")
    conn.close()
    # Runtime state that must never be archived.
    (data_dir / "node.lock").write_text("1234", encoding="utf-8")


def test_parser_backup_restore_subcommands():
    parser = cli.build_parser()

    backup = parser.parse_args(["backup", "node.tar.gz"])
    assert backup.func is cli.cmd_backup
    assert str(backup.file) == "node.tar.gz"
    assert backup.force is False

    restore = parser.parse_args(["restore", "node.tar.gz", "--force"])
    assert restore.func is cli.cmd_restore
    assert str(restore.file) == "node.tar.gz"
    assert restore.force is True


def test_backup_restore_round_trip(tmp_path, capsys):
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_state(node_a)
    capsys.readouterr()
    destination_a = _id_destination(node_a, capsys)
    contents_before = {
        name: (node_a / name).read_bytes()
        for name in BACKUP_FILES
        if (node_a / name).is_file()
    }
    assert "identity" in contents_before
    assert "tan.sqlite3" in contents_before
    assert "reticulum/config" in contents_before

    archive = tmp_path / "alice.tar.gz"
    assert (
        cli.main(
            [
                "backup",
                str(archive),
                "--data-dir",
                str(node_a),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["backup"] == str(archive)
    assert payload["destination"] == destination_a
    assert payload["node"] == "alice"
    assert sorted(payload["files"]) == sorted(contents_before)
    assert stat.S_IMODE(os.stat(archive).st_mode) == 0o600

    # The runtime lock file is never archived.
    with tarfile.open(archive, mode="r:gz") as tar:
        assert "node.lock" not in tar.getnames()
        manifest = json.loads(tar.extractfile(MANIFEST_NAME).read())
    assert manifest["format"] == 1
    assert manifest["node"] == "alice"
    assert sorted(manifest["files"]) == sorted(contents_before)

    # A fresh node restores the backup and takes over the same address
    # with the same state files, byte for byte.
    node_b = tmp_path / "b"
    assert (
        cli.main(
            [
                "restore",
                str(archive),
                "--data-dir",
                str(node_b),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["destination"] == destination_a
    assert payload["data_dir"] == str(node_b)
    assert sorted(payload["files"]) == sorted(contents_before)
    assert _id_destination(node_b, capsys) == destination_a
    assert stat.S_IMODE(os.stat(node_b / "identity").st_mode) == 0o600
    for name, data in contents_before.items():
        # The Tan database is snapshotted with SQLite's backup API, so
        # the restored file is content-identical but not byte-identical
        # (header counters may differ); compare it by logical dump.
        if name == "tan.sqlite3":
            assert _sqlite_dump(node_b / name) == _sqlite_dump(node_a / name)
        else:
            assert (node_b / name).read_bytes() == data


def test_backup_refuses_existing_archive_without_force(tmp_path, capsys):
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    archive = tmp_path / "backup.tar.gz"
    assert cli.main(["backup", str(archive), "--data-dir", str(node)]) == 0
    capsys.readouterr()

    code = cli.main(["backup", str(archive), "--data-dir", str(node)])
    assert code == 1
    assert "already exists" in capsys.readouterr().err
    assert (
        cli.main(["backup", str(archive), "--data-dir", str(node), "--force"]) == 0
    )


def test_backup_refuses_target_inside_data_dir(tmp_path, capsys):
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    code = cli.main(
        ["backup", str(node / "backup.tar.gz"), "--data-dir", str(node)]
    )
    assert code == 1
    assert "inside the data directory" in capsys.readouterr().err


def test_backup_without_identity_returns_error(tmp_path, capsys):
    code = cli.main(
        [
            "backup",
            str(tmp_path / "backup.tar.gz"),
            "--data-dir",
            str(tmp_path / "missing"),
        ]
    )
    assert code == 1
    assert "run `tau init` first" in capsys.readouterr().err


def test_backup_refuses_while_node_serving(tmp_path, capsys):
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    config = TauConfig.resolve(data_dir=node)
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    try:
        code = cli.main(
            ["backup", str(tmp_path / "backup.tar.gz"), "--data-dir", str(node)]
        )
    finally:
        lock.release()
    assert code == 1
    assert "already serving" in capsys.readouterr().err


def test_backup_live_runs_while_node_serving(tmp_path, capsys):
    # The serving node's own backup: the instance lock is held (this
    # process stands in for the serving node), so the plain path would
    # refuse — but the live path skips the lock and still writes a
    # valid archive, because the node's own writes are atomic.
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_state(node_a)
    capsys.readouterr()
    config = TauConfig.resolve(data_dir=node_a)
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    try:
        archive = tmp_path / "backup.tar.gz"
        target, manifest = create_backup(config, archive, live=True)
    finally:
        lock.release()
    assert target == archive
    assert manifest["format"] == 1

    node_b = tmp_path / "b"
    assert cli.main(["restore", str(archive), "--data-dir", str(node_b)]) == 0
    capsys.readouterr()
    assert _id_destination(node_b, capsys) == _id_destination(node_a, capsys)
    assert (node_b / "mailbox.json").exists()
    assert (node_b / "tan.sqlite3").exists()


def test_backup_captures_uncheckpointed_wal_rows(tmp_path, capsys):
    # A row committed to the Tan database while the only writer holds
    # its connection open lives in tan.sqlite3-wal, not in the main
    # file.  The backup must still carry it: the snapshot reads through
    # the WAL instead of copying the raw file.
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_state(node_a)
    capsys.readouterr()
    conn = init_db(node_a / "tan.sqlite3")
    conn.execute(
        "INSERT INTO posts (hash, author, key, text, created_at, received_at,"
        " sig, seq) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("abc123", "alice", "key", "hello from the wal", 1000.0, 1000.0, "sig", 1),
    )
    conn.commit()
    assert (node_a / "tan.sqlite3-wal").exists()
    archive = tmp_path / "backup.tar.gz"
    try:
        assert cli.main(["backup", str(archive), "--data-dir", str(node_a)]) == 0
    finally:
        conn.close()
    capsys.readouterr()

    node_b = tmp_path / "b"
    assert cli.main(["restore", str(archive), "--data-dir", str(node_b)]) == 0
    capsys.readouterr()
    restored = sqlite3.connect(str(node_b / "tan.sqlite3"))
    try:
        rows = restored.execute(
            "SELECT text FROM posts WHERE hash = ?", ("abc123",)
        ).fetchall()
    finally:
        restored.close()
    assert rows == [("hello from the wal",)]


def test_restore_force_removes_state_absent_from_archive(tmp_path, capsys):
    # A --force restore replaces the whole node state: files the archive
    # does not hold (including SQLite sidecars) must not survive it.
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    archive = tmp_path / "backup.tar.gz"
    assert cli.main(["backup", str(archive), "--data-dir", str(node_a)]) == 0
    capsys.readouterr()

    node_b = _init_node(tmp_path, "b", "bob")
    (node_b / "peers.json").write_text('{"peers": []}\n', encoding="utf-8")
    (node_b / "tan.sqlite3").write_bytes(b"stale database")
    (node_b / "tan.sqlite3-wal").write_bytes(b"stale wal")
    capsys.readouterr()
    assert (
        cli.main(["restore", str(archive), "--data-dir", str(node_b), "--force"])
        == 0
    )
    capsys.readouterr()
    assert not (node_b / "peers.json").exists()
    assert not (node_b / "tan.sqlite3").exists()
    assert not (node_b / "tan.sqlite3-wal").exists()
    assert _id_destination(node_b, capsys) == _id_destination(node_a, capsys)


def test_restore_rejects_archive_without_identity(tmp_path):
    node = tmp_path / "node"
    config = TauConfig.resolve(data_dir=node)
    archive = tmp_path / "bad.tar.gz"
    _write_archive(
        archive,
        {"format": 1, "files": ["peers.json"]},
        [("peers.json", b"{}")],
    )
    with pytest.raises(Exception, match="contains no identity"):
        restore_backup(config, archive)
    assert not (node / "identity").exists()


def test_backup_restores_configured_rns_config(tmp_path, capsys):
    # With a custom --rns-config directory, the backup must carry that
    # config file (not the default data_dir/reticulum/config) and the
    # restore must put it back at the configured location.
    node_a = tmp_path / "a"
    custom_a = tmp_path / "rns-a"
    assert (
        cli.main(
            [
                "init",
                "--data-dir",
                str(node_a),
                "--name",
                "alice",
                "--rns-config",
                str(custom_a),
                "--json",
            ]
        )
        == 0
    )
    capsys.readouterr()
    marker = "# custom rns config for alice\n"
    (custom_a / "config").write_text(marker, encoding="utf-8")

    archive = tmp_path / "backup.tar.gz"
    assert (
        cli.main(
            [
                "backup",
                str(archive),
                "--data-dir",
                str(node_a),
                "--rns-config",
                str(custom_a),
            ]
        )
        == 0
    )
    capsys.readouterr()

    node_b = tmp_path / "b"
    custom_b = tmp_path / "rns-b"
    assert (
        cli.main(
            [
                "restore",
                str(archive),
                "--data-dir",
                str(node_b),
                "--rns-config",
                str(custom_b),
            ]
        )
        == 0
    )
    assert (custom_b / "config").read_text(encoding="utf-8") == marker
    capsys.readouterr()
    assert _id_destination(node_b, capsys) == _id_destination(node_a, capsys)


def test_restore_refuses_existing_state_without_force(tmp_path, capsys):
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    archive = tmp_path / "backup.tar.gz"
    assert cli.main(["backup", str(archive), "--data-dir", str(node_a)]) == 0
    capsys.readouterr()

    node_b = _init_node(tmp_path, "b", "bob")
    capsys.readouterr()
    code = cli.main(["restore", str(archive), "--data-dir", str(node_b)])
    assert code == 1
    err = capsys.readouterr().err
    assert "already exists" in err
    assert "pass --force" in err
    # The refused restore left node B untouched.
    assert _id_destination(node_b, capsys) != _id_destination(node_a, capsys)

    assert (
        cli.main(["restore", str(archive), "--data-dir", str(node_b), "--force"])
        == 0
    )
    capsys.readouterr()
    assert _id_destination(node_b, capsys) == _id_destination(node_a, capsys)


def test_restore_refuses_while_node_serving(tmp_path, capsys):
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    archive = tmp_path / "backup.tar.gz"
    assert cli.main(["backup", str(archive), "--data-dir", str(node_a)]) == 0
    capsys.readouterr()

    node_b = tmp_path / "b"
    node_b.mkdir()
    config = TauConfig.resolve(data_dir=node_b)
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    try:
        code = cli.main(["restore", str(archive), "--data-dir", str(node_b)])
    finally:
        lock.release()
    assert code == 1
    assert "already serving" in capsys.readouterr().err
    assert not (node_b / "identity").exists()


def _write_archive(path, manifest, members):
    with tarfile.open(path, mode="w:gz") as tar:
        raw = json.dumps(manifest).encode("utf-8")
        info = tarfile.TarInfo(MANIFEST_NAME)
        info.size = len(raw)
        tar.addfile(info, io.BytesIO(raw))
        for name, data in members:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))


def test_restore_rejects_archive_without_manifest(tmp_path):
    node = tmp_path / "node"
    config = TauConfig.resolve(data_dir=node)
    archive = tmp_path / "bad.tar.gz"
    with tarfile.open(archive, mode="w:gz") as tar:
        info = tarfile.TarInfo("identity")
        info.size = 3
        tar.addfile(info, io.BytesIO(b"abc"))
    with pytest.raises(Exception, match="manifest.json is missing"):
        restore_backup(config, archive)


def test_restore_rejects_unknown_format_version(tmp_path):
    node = tmp_path / "node"
    config = TauConfig.resolve(data_dir=node)
    archive = tmp_path / "bad.tar.gz"
    _write_archive(archive, {"format": 99, "files": []}, [])
    with pytest.raises(Exception, match="unsupported backup format"):
        restore_backup(config, archive)


def test_restore_rejects_member_outside_allowlist(tmp_path):
    node = tmp_path / "node"
    config = TauConfig.resolve(data_dir=node)
    archive = tmp_path / "bad.tar.gz"
    _write_archive(
        archive,
        {"format": 1, "files": ["evil.sh"]},
        [("evil.sh", b"#!/bin/sh\n")],
    )
    with pytest.raises(Exception, match="unexpected member"):
        restore_backup(config, archive)
    assert not (node / "evil.sh").exists()


def test_restore_rejects_path_traversal(tmp_path):
    node = tmp_path / "node"
    config = TauConfig.resolve(data_dir=node)
    archive = tmp_path / "bad.tar.gz"
    _write_archive(
        archive,
        {"format": 1, "files": ["../escape"]},
        [("../escape", b"x")],
    )
    with pytest.raises(Exception, match="unsafe member path"):
        restore_backup(config, archive)
    assert not (tmp_path / "escape").exists()


def test_restore_rejects_member_missing_from_manifest(tmp_path):
    node = tmp_path / "node"
    config = TauConfig.resolve(data_dir=node)
    archive = tmp_path / "bad.tar.gz"
    _write_archive(
        archive,
        {"format": 1, "files": ["peers.json"]},
        [("peers.json", b"{}"), ("allowlist.json", b"{}")],
    )
    with pytest.raises(Exception, match="is not in the manifest"):
        restore_backup(config, archive)


def test_restore_rejects_non_archive(tmp_path):
    node = tmp_path / "node"
    config = TauConfig.resolve(data_dir=node)
    junk = tmp_path / "junk"
    junk.write_text("this is not a backup\n", encoding="utf-8")
    with pytest.raises(Exception, match="not a valid tau backup"):
        restore_backup(config, junk)
    assert not (node / "identity").exists()


def test_backup_round_trip_without_optional_files(tmp_path, capsys):
    # Only identity + node.json + reticulum/config exist: the archive
    # holds just those, and restore still takes over the address.
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    destination_a = _id_destination(node_a, capsys)
    archive = tmp_path / "minimal.tar.gz"
    assert cli.main(["backup", str(archive), "--data-dir", str(node_a)]) == 0
    capsys.readouterr()

    node_b = tmp_path / "b"
    assert cli.main(["restore", str(archive), "--data-dir", str(node_b)]) == 0
    capsys.readouterr()
    assert _id_destination(node_b, capsys) == destination_a
    assert not (node_b / "peers.json").exists()
