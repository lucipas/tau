from __future__ import annotations

import io
import json
import os
import sqlite3
import stat
import tarfile
from pathlib import Path
import tempfile
import threading

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
    """Write one of every back-up file so the round-trip is meaningful.

    Returns the received files seeded under ``files/``, as
    ``{member name: bytes}``, for the caller to compare after restore.
    """
    (data_dir / "peers.json").write_text('{"peers": []}\n', encoding="utf-8")
    (data_dir / "mailbox.json").write_text('{"outbox": []}\n', encoding="utf-8")
    (data_dir / "inbox.json").write_text('{"messages": []}\n', encoding="utf-8")
    (data_dir / "messages.json").write_text('{"messages": []}\n', encoding="utf-8")
    (data_dir / "tan-rules.json").write_text('{"kinds": ["post"]}\n', encoding="utf-8")
    (data_dir / "allowlist.json").write_text('{"allowed": []}\n', encoding="utf-8")
    (data_dir / "groups.json").write_text('{"groups": []}\n', encoding="utf-8")
    (data_dir / "blocklist.json").write_text('{"blocked": []}\n', encoding="utf-8")
    conn = init_db(data_dir / "tan.sqlite3")
    conn.close()
    # Runtime state that must never be archived.
    (data_dir / "node.lock").write_text("1234", encoding="utf-8")
    # Received files, one per sender, as the node stores them.
    received = {
        "files/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/report.pdf": b"%PDF-1.4 fake\n",
        "files/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/photo.png": b"\x89PNG fake\n",
    }
    for name, data in received.items():
        path = data_dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    return received


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
    received = _seed_state(node_a)
    capsys.readouterr()
    destination_a = _id_destination(node_a, capsys)
    contents_before = {
        name: (node_a / name).read_bytes()
        for name in BACKUP_FILES
        if (node_a / name).is_file()
    }
    contents_before.update(received)
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


def test_live_backup_survives_concurrent_state_replacement(tmp_path, capsys):
    # A live node replaces state files atomically while a backup reads
    # them.  Members are read from one open descriptor, so a replacement
    # between the size lookup and the read cannot truncate a member:
    # every archive holds either the old or the new complete file,
    # never a mix of the two.
    node = _init_node(tmp_path, "node", "alice")
    _seed_state(node)
    capsys.readouterr()
    config = TauConfig.resolve(data_dir=node)
    small = b'{"peers": []}\n'
    large = b'{"peers": [' + b'{"x": 1}, ' * 5000 + b']}\n'
    stop = threading.Event()

    def atomic_write(path, data):
        fd, tmp_name = tempfile.mkstemp(
            dir=str(path.parent), prefix=".peers.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
            os.replace(tmp_name, path)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

    def rewrite():
        toggle = False
        while not stop.is_set():
            atomic_write(node / "peers.json", large if toggle else small)
            toggle = not toggle

    writer = threading.Thread(target=rewrite, daemon=True)
    writer.start()
    try:
        for i in range(15):
            archive = tmp_path / f"backup-{i}.tar.gz"
            create_backup(config, archive, live=True)
            with tarfile.open(archive, mode="r:gz") as tar:
                member = tar.extractfile("peers.json")
                assert member is not None
                content = member.read()
            assert content in (small, large), "truncated or mixed peers.json"
    finally:
        stop.set()
        writer.join(timeout=5)


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


def test_backup_skips_symlinks_under_files(tmp_path, capsys):
    # A symlink under files/ — whether a symlinked file or a symlinked
    # sender directory — must never be followed into the archive: the
    # target could sit outside the data directory.
    node_a = _init_node(tmp_path, "a", "alice")
    received = _seed_state(node_a)
    capsys.readouterr()
    outside = tmp_path / "outside.txt"
    outside.write_text("secret\n", encoding="utf-8")
    sender = node_a / "files" / "cccccccccccccccccccccccccccccccc"
    sender.mkdir(parents=True, exist_ok=True)
    (sender / "link.bin").symlink_to(outside)
    (node_a / "files" / "dddddddddddddddddddddddddddddddd").symlink_to(
        sender, target_is_directory=True
    )
    archive = tmp_path / "backup.tar.gz"
    assert cli.main(["backup", str(archive), "--data-dir", str(node_a)]) == 0
    capsys.readouterr()

    with tarfile.open(archive, mode="r:gz") as tar:
        names = tar.getnames()
        manifest = json.loads(tar.extractfile(MANIFEST_NAME).read())
        assert all(
            tar.getmember(name).isreg()
            for name in names
            if name != MANIFEST_NAME
        )
    assert sorted(manifest["files"]) == sorted(
        [name for name in BACKUP_FILES if (node_a / name).is_file()]
        + sorted(received)
    )
    assert not any(name.endswith("link.bin") for name in names)
    assert not any("dddddddd" in name for name in names)


def test_backup_stores_hard_linked_received_files_as_regular_members(
    tmp_path, capsys
):
    # Two received files sharing one inode (e.g. a deduplicating
    # filesystem, or a sender dir hard-linked by hand) must both be
    # archived as regular members: tarfile would otherwise emit the
    # second as a hard-link member, which restore rejects outright.
    node_a = _init_node(tmp_path, "a", "alice")
    received = _seed_state(node_a)
    capsys.readouterr()
    sender = node_a / "files" / "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    linked = sender / "report-copy.pdf"
    os.link(sender / "report.pdf", linked)
    received["files/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/report-copy.pdf"] = (
        b"%PDF-1.4 fake\n"
    )
    archive = tmp_path / "backup.tar.gz"
    assert cli.main(["backup", str(archive), "--data-dir", str(node_a)]) == 0
    capsys.readouterr()

    with tarfile.open(archive, mode="r:gz") as tar:
        names = tar.getnames()
        manifest = json.loads(tar.extractfile(MANIFEST_NAME).read())
        assert all(
            tar.getmember(name).isreg()
            for name in names
            if name != MANIFEST_NAME
        )
    assert sorted(manifest["files"]) == sorted(
        [name for name in BACKUP_FILES if (node_a / name).is_file()]
        + sorted(received)
    )

    node_b = tmp_path / "b"
    assert (
        cli.main(["restore", str(archive), "--data-dir", str(node_b), "--json"])
        == 0
    )
    assert (node_b / "files" / "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" / "report.pdf").read_bytes() == (
        b"%PDF-1.4 fake\n"
    )
    assert (node_b / "files" / "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" / "report-copy.pdf").read_bytes() == (
        b"%PDF-1.4 fake\n"
    )


def test_backup_skips_unpublished_received_temp_files(tmp_path, capsys):
    # A `.tau-received-*.tmp` file is a transfer interrupted before its
    # content was linked into place — it was never a received file, so
    # the backup leaves it out instead of archiving a partial file.
    node_a = _init_node(tmp_path, "a", "alice")
    received = _seed_state(node_a)
    capsys.readouterr()
    sender = node_a / "files" / "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    (sender / ".tau-received-abc123.tmp").write_bytes(b"half-written")
    archive = tmp_path / "backup.tar.gz"
    assert cli.main(["backup", str(archive), "--data-dir", str(node_a)]) == 0
    capsys.readouterr()

    with tarfile.open(archive, mode="r:gz") as tar:
        manifest = json.loads(tar.extractfile(MANIFEST_NAME).read())
    assert sorted(manifest["files"]) == sorted(
        [name for name in BACKUP_FILES if (node_a / name).is_file()]
        + sorted(received)
    )


def test_restore_force_removes_stale_groups_blocklist_files(tmp_path, capsys):
    # A --force restore replaces the whole node state, including the
    # newly backed-up files: groups, blocklist and received files the
    # archive does not hold must not survive it.
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    archive = tmp_path / "backup.tar.gz"
    assert cli.main(["backup", str(archive), "--data-dir", str(node_a)]) == 0
    capsys.readouterr()

    node_b = _init_node(tmp_path, "b", "bob")
    (node_b / "groups.json").write_text('{"groups": []}\n', encoding="utf-8")
    (node_b / "blocklist.json").write_text('{"blocked": []}\n', encoding="utf-8")
    stale = node_b / "files" / "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" / "stale.bin"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_bytes(b"stale\n")
    capsys.readouterr()
    assert (
        cli.main(["restore", str(archive), "--data-dir", str(node_b), "--force"])
        == 0
    )
    capsys.readouterr()
    assert not (node_b / "groups.json").exists()
    assert not (node_b / "blocklist.json").exists()
    assert not stale.exists()
    assert _id_destination(node_b, capsys) == _id_destination(node_a, capsys)


def test_restore_rejects_misplaced_files_member(tmp_path):
    # Received-file members are exactly files/<sender>/<name>: a file
    # directly under files/, or nested any deeper, is not a member
    # this module ever writes.
    node = tmp_path / "node"
    config = TauConfig.resolve(data_dir=node)
    for bad_name in ("files/evil", "files/a/b/c"):
        archive = tmp_path / "bad.tar.gz"
        _write_archive(
            archive,
            {"format": 1, "files": ["identity", bad_name]},
            [("identity", b"x"), (bad_name, b"x")],
        )
        with pytest.raises(Exception, match="unexpected member"):
            restore_backup(config, archive)
        assert not (node / "identity").exists()


def test_backup_refuses_symlinked_state_file(tmp_path, capsys):
    # A state file that is a symlink would be archived as a symlink
    # member, which restore rejects — so the backup must refuse
    # loudly instead of publishing an archive that can never restore.
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    outside = tmp_path / "outside-groups.json"
    outside.write_text('{"groups": []}\n', encoding="utf-8")
    (node_a / "groups.json").symlink_to(outside)
    archive = tmp_path / "backup.tar.gz"
    code = cli.main(["backup", str(archive), "--data-dir", str(node_a)])
    assert code == 1
    assert "is a symlink" in capsys.readouterr().err
    assert not archive.exists()


def test_backup_refuses_when_over_restore_size_limit(tmp_path, capsys, monkeypatch):
    # The restore side enforces an 8 GiB expanded-size budget; a
    # backup that would exceed it must fail up front instead of
    # publishing an archive restore rejects on every attempt.
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_state(node_a)
    capsys.readouterr()
    monkeypatch.setattr("tau.backup.MAX_TOTAL_BYTES", 10)
    code = cli.main(
        ["backup", str(tmp_path / "backup.tar.gz"), "--data-dir", str(node_a)]
    )
    assert code == 1
    err = capsys.readouterr().err
    assert "exceeds the restore size limit" in err
    assert not (tmp_path / "backup.tar.gz").exists()


def test_backup_refuses_when_manifest_over_limit(tmp_path, capsys, monkeypatch):
    # Same for the manifest: restore refuses a manifest over 1 MiB,
    # so a backup whose member list would overflow it must fail here.
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_state(node_a)
    capsys.readouterr()
    monkeypatch.setattr("tau.backup.MAX_MANIFEST_BYTES", 10)
    code = cli.main(
        ["backup", str(tmp_path / "backup.tar.gz"), "--data-dir", str(node_a)]
    )
    assert code == 1
    err = capsys.readouterr().err
    assert "manifest" in err
    assert "restore limit" in err
    assert not (tmp_path / "backup.tar.gz").exists()


def test_backup_fails_when_received_files_unreadable(
    tmp_path, capsys, monkeypatch
):
    # An unreadable received-files directory must fail the backup,
    # not silently publish an archive missing those files.
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_state(node_a)
    capsys.readouterr()
    files_dir = node_a / "files"
    real_iterdir = Path.iterdir

    def _failing_iterdir(self):
        if self == files_dir:
            raise OSError("permission denied")
        return real_iterdir(self)

    monkeypatch.setattr(Path, "iterdir", _failing_iterdir)
    code = cli.main(
        ["backup", str(tmp_path / "backup.tar.gz"), "--data-dir", str(node_a)]
    )
    assert code == 1
    assert "could not list received files" in capsys.readouterr().err
    assert not (tmp_path / "backup.tar.gz").exists()


def test_restore_rebases_inbox_file_paths(tmp_path, capsys):
    # File records store the absolute path the file was saved to.  A
    # restore into a different directory installs the file at the new
    # location, so the record must be rebased to match.
    node_a = _init_node(tmp_path, "a", "alice")
    received = _seed_state(node_a)
    member = "files/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/report.pdf"
    record = {
        "kind": "file",
        "from": "alice",
        "destination": "a" * 32,
        "name": "report.pdf",
        "size": len(received[member]),
        "sha256": "0" * 64,
        "path": str(node_a / member),
        "sent_at": 1000.0,
        "received_at": 1000.0,
        "id": "x" * 32,
    }
    (node_a / "inbox.json").write_text(
        json.dumps({"messages": [record]}), encoding="utf-8"
    )
    capsys.readouterr()
    archive = tmp_path / "backup.tar.gz"
    assert cli.main(["backup", str(archive), "--data-dir", str(node_a)]) == 0
    capsys.readouterr()

    node_b = tmp_path / "b"
    assert cli.main(["restore", str(archive), "--data-dir", str(node_b)]) == 0
    capsys.readouterr()
    restored = json.loads((node_b / "inbox.json").read_text(encoding="utf-8"))
    assert restored["messages"][0]["path"] == str(node_b / member)
    assert (node_b / member).read_bytes() == received[member]


def test_restore_leaves_unparseable_inbox_untouched(tmp_path, capsys):
    # Rebasing must never make an otherwise valid restore fail: an
    # inbox that cannot be parsed is restored byte for byte.
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_state(node_a)
    (node_a / "inbox.json").write_text("not json at all\n", encoding="utf-8")
    capsys.readouterr()
    archive = tmp_path / "backup.tar.gz"
    assert cli.main(["backup", str(archive), "--data-dir", str(node_a)]) == 0
    capsys.readouterr()

    node_b = tmp_path / "b"
    assert cli.main(["restore", str(archive), "--data-dir", str(node_b)]) == 0
    capsys.readouterr()
    assert (node_b / "inbox.json").read_text(encoding="utf-8") == "not json at all\n"


def test_restore_refuses_symlinked_received_file_dir(tmp_path, capsys):
    # If files/ (or a sender directory under it) in the target is a
    # symlink pointing outside the data directory, staging would write
    # the archive contents there.  Restore refuses instead.
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_state(node_a)
    capsys.readouterr()
    archive = tmp_path / "backup.tar.gz"
    assert cli.main(["backup", str(archive), "--data-dir", str(node_a)]) == 0
    capsys.readouterr()

    outside = tmp_path / "outside-files"
    outside.mkdir()
    node_b = tmp_path / "b"
    node_b.mkdir()
    (node_b / "files").symlink_to(outside, target_is_directory=True)
    code = cli.main(["restore", str(archive), "--data-dir", str(node_b)])
    assert code == 1
    assert "is a symlink" in capsys.readouterr().err
    assert not (outside / "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").exists()

    # Same for a symlinked sender directory under a real files/.
    (node_b / "files").unlink()
    sender = node_b / "files" / "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    sender.mkdir(parents=True, exist_ok=True)
    sender.rmdir()
    sender.symlink_to(outside, target_is_directory=True)
    capsys.readouterr()
    code = cli.main(["restore", str(archive), "--data-dir", str(node_b)])
    assert code == 1
    assert "is a symlink" in capsys.readouterr().err
    assert not list(outside.iterdir())


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
