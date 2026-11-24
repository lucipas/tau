from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from tau import cli
from tau.auto_backup import (
    AUTO_BACKUP_ENCRYPTED_SUFFIX,
    AUTO_BACKUP_PLAIN_SUFFIX,
    AutoBackupScheduler,
    auto_backup_filename,
    is_auto_backup_name,
    list_auto_backups,
    prune_auto_backups,
)
from tau.backup import (
    create_backup,
    is_encrypted_backup,
    restore_backup,
)
from tau.config import (
    BACKUP_DIR_NAME,
    TauConfig,
    backup_dir,
    backup_interval,
    backup_keep,
    backup_passphrase_file,
    set_node_setting,
)


def _init_node(tmp_path, dirname, name):
    data_dir = tmp_path / dirname
    assert (
        cli.main(["init", "--data-dir", str(data_dir), "--name", name, "--json"])
        == 0
    )
    return data_dir


def _config(data_dir):
    return TauConfig.resolve(data_dir=data_dir)


def _set(data_dir, key, value):
    set_node_setting(data_dir, key, value)


# --- settings readers -----------------------------------------------------


def test_backup_settings_defaults(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    assert backup_interval(node) == 0.0
    assert backup_dir(node) == node.parent / BACKUP_DIR_NAME
    assert backup_keep(node) == 7
    assert backup_passphrase_file(node) is None


def test_backup_interval_reader(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    _set(node, "backup_interval", 3600)
    assert backup_interval(node) == 3600.0
    for bad in ("soon", -5, float("inf"), True, None, 10**12):
        _set(node, "backup_interval", bad)
        assert backup_interval(node) == 0.0


def test_backup_dir_reader(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    custom = tmp_path / "elsewhere"
    _set(node, "backup_dir", str(custom))
    assert backup_dir(node) == custom
    # Inside the data directory is refused: the default applies.
    _set(node, "backup_dir", str(node / "nested"))
    assert backup_dir(node) == node.parent / BACKUP_DIR_NAME
    # A relative setting resolves against the data directory's parent.
    _set(node, "backup_dir", "rel-backups")
    assert backup_dir(node) == node.parent / "rel-backups"
    for bad in ("", 42, None):
        _set(node, "backup_dir", bad)
        assert backup_dir(node) == node.parent / BACKUP_DIR_NAME


def test_backup_keep_reader(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    _set(node, "backup_keep", 3)
    assert backup_keep(node) == 3
    _set(node, "backup_keep", 0)
    assert backup_keep(node) == 0
    for bad in ("many", -1, 1001, True, 2.5, None):
        _set(node, "backup_keep", bad)
        assert backup_keep(node) == 7


def test_backup_passphrase_file_reader(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    _set(node, "backup_passphrase_file", "~/secret.txt")
    assert backup_passphrase_file(node) == Path("~/secret.txt").expanduser()
    for bad in ("", 42, None):
        _set(node, "backup_passphrase_file", bad)
        assert backup_passphrase_file(node) is None


# --- filenames, listing, pruning ------------------------------------------


def test_auto_backup_filename():
    name = auto_backup_filename("alice", encrypted=False, now=1764028800.0)
    assert name == "tau-backup-alice-20251125-000000.tar.gz"
    encrypted = auto_backup_filename("alice", encrypted=True, now=1764028800.0)
    assert encrypted == "tau-backup-alice-20251125-000000.tau"
    # Unsafe characters in the node name become dashes.
    assert auto_backup_filename(
        "Al ice/x", encrypted=False, now=1764028800.0
    ).startswith("tau-backup-Al-ice-x-20251125-")
    assert auto_backup_filename("", encrypted=False, now=0).startswith(
        "tau-backup-node-"
    )


def test_is_auto_backup_name():
    assert is_auto_backup_name("tau-backup-alice-20251125-000000.tar.gz", "alice")
    assert is_auto_backup_name("tau-backup-alice-20251125-000000.tau", "alice")
    assert is_auto_backup_name("tau-backup-alice-20251125-000000-2.tau", "alice")
    assert not is_auto_backup_name("backup.tar.gz", "alice")
    assert not is_auto_backup_name("tau-backup-alice-20251125-000000.zip", "alice")
    assert not is_auto_backup_name("tau-backup-notes.txt", "alice")
    # A manual archive that merely shares the prefix is not ours.
    assert not is_auto_backup_name("tau-backup-notes.tar.gz", "alice")
    assert not is_auto_backup_name("tau-backup-bob-20251125-000000.tar.gz", "alice")


def test_list_and_prune_auto_backups(tmp_path):
    directory = tmp_path / "backups"
    directory.mkdir()
    names = [
        "tau-backup-alice-20251125-000000.tar.gz",
        "tau-backup-alice-20251125-010000.tar.gz",
        "tau-backup-alice-20251125-020000.tau",
    ]
    for name in names:
        (directory / name).write_bytes(b"x")
    (directory / "backup.tar.gz").write_bytes(b"manual")
    (directory / "notes.txt").write_text("keep me", encoding="utf-8")
    assert [p.name for p in list_auto_backups(directory, "alice")] == names
    assert list_auto_backups(tmp_path / "missing", "alice") == []

    deleted = prune_auto_backups(directory, 2, "alice")
    assert [p.name for p in deleted] == [names[0]]
    assert [p.name for p in list_auto_backups(directory, "alice")] == names[1:]
    # A manual backup and an unrelated file are never pruned.
    assert (directory / "backup.tar.gz").exists()
    assert (directory / "notes.txt").exists()

    assert prune_auto_backups(directory, 0, "alice") == []
    assert len(list_auto_backups(directory, "alice")) == 2


def test_prune_never_deletes_manual_prefixed_archive(tmp_path):
    directory = tmp_path / "backups"
    directory.mkdir()
    manual = directory / "tau-backup-notes.tar.gz"
    manual.write_bytes(b"manual")
    for hour in range(3):
        (directory / f"tau-backup-alice-20251125-0{hour}0000.tar.gz").write_bytes(b"x")
    prune_auto_backups(directory, 1, "alice")
    assert manual.exists()
    assert len(list_auto_backups(directory, "alice")) == 1


def test_prune_scoped_to_node_sharing_directory(tmp_path):
    directory = tmp_path / "backups"
    directory.mkdir()
    # Sibling node "bob" wrote its archives into the same directory.
    bob = [
        "tau-backup-bob-20251125-000000.tar.gz",
        "tau-backup-bob-20251125-010000.tar.gz",
    ]
    for name in bob:
        (directory / name).write_bytes(b"x")
    alice = [
        "tau-backup-alice-20251125-000000.tar.gz",
        "tau-backup-alice-20251125-010000.tar.gz",
    ]
    for name in alice:
        (directory / name).write_bytes(b"x")
    prune_auto_backups(directory, 1, "alice")
    assert [p.name for p in list_auto_backups(directory, "alice")] == [alice[1]]
    assert [p.name for p in list_auto_backups(directory, "bob")] == bob


# --- scheduler ------------------------------------------------------------


def test_scheduler_disabled_without_interval(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    scheduler = AutoBackupScheduler(_config(node))
    assert scheduler.due(now=1000.0) is False
    assert scheduler.run(now=1000.0) is None


def test_scheduler_due_when_no_backup_yet(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    _set(node, "backup_interval", 3600)
    scheduler = AutoBackupScheduler(_config(node))
    assert scheduler.due(now=1000.0) is True


def test_scheduler_due_follows_newest_backup(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    directory = tmp_path / "backups"
    directory.mkdir()
    _set(node, "backup_interval", 3600)
    _set(node, "backup_dir", str(directory))
    newest = directory / "tau-backup-alice-20251125-000000.tar.gz"
    newest.write_bytes(b"x")
    os.utime(newest, (5000.0, 5000.0))
    scheduler = AutoBackupScheduler(_config(node))
    # Newest backup at 5000, interval 3600: not due until 8600.
    assert scheduler.due(now=8599.0) is False
    assert scheduler.due(now=8600.0) is True


def test_scheduler_run_writes_restorable_plaintext_backup(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    directory = tmp_path / "backups"
    _set(node, "backup_interval", 3600)
    _set(node, "backup_dir", str(directory))
    scheduler = AutoBackupScheduler(_config(node))
    target = scheduler.run(now=1764028800.0)
    assert target is not None
    assert target.parent == directory
    assert target.name == "tau-backup-alice-20251125-000000.tar.gz"
    assert not is_encrypted_backup(target)
    # Not due again until the interval has passed since the backup.
    assert scheduler.due(now=1764028800.0 + 3599) is False
    assert scheduler.due(now=1764028800.0 + 3600) is True
    # The archive restores like a manual backup.
    restored = tmp_path / "restored"
    manifest, files = restore_backup(
        TauConfig.resolve(data_dir=restored), target
    )
    assert "identity" in files


def test_scheduler_run_same_second_does_not_collide(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    directory = tmp_path / "backups"
    _set(node, "backup_interval", 3600)
    _set(node, "backup_dir", str(directory))
    scheduler = AutoBackupScheduler(_config(node))
    first = scheduler.run(now=1764028800.0)
    second = scheduler.run(now=1764028800.0)
    assert first is not None and second is not None
    assert first != second
    assert second.name == "tau-backup-alice-20251125-000000-2.tar.gz"


def test_scheduler_run_encrypts_with_passphrase_file(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    directory = tmp_path / "backups"
    passphrase_file = tmp_path / "pass.txt"
    passphrase_file.write_text("correct horse battery staple\n", encoding="utf-8")
    _set(node, "backup_interval", 3600)
    _set(node, "backup_dir", str(directory))
    _set(node, "backup_passphrase_file", str(passphrase_file))
    scheduler = AutoBackupScheduler(_config(node))
    target = scheduler.run(now=1764028800.0)
    assert target is not None
    assert target.name.endswith(AUTO_BACKUP_ENCRYPTED_SUFFIX)
    assert is_encrypted_backup(target)
    restored = tmp_path / "restored"
    manifest, files = restore_backup(
        TauConfig.resolve(data_dir=restored),
        target,
        passphrase="correct horse battery staple",
    )
    assert "identity" in files


def test_scheduler_run_prunes_to_keep(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    directory = tmp_path / "backups"
    _set(node, "backup_interval", 60)
    _set(node, "backup_dir", str(directory))
    _set(node, "backup_keep", 2)
    scheduler = AutoBackupScheduler(_config(node))
    for offset in (0, 120, 240):
        assert scheduler.run(now=1764028800.0 + offset) is not None
    kept = [p.name for p in list_auto_backups(directory, "alice")]
    assert kept == [
        "tau-backup-alice-20251125-000200.tar.gz",
        "tau-backup-alice-20251125-000400.tar.gz",
    ]


def test_scheduler_failure_advances_schedule(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    directory = tmp_path / "backups"
    _set(node, "backup_interval", 3600)
    _set(node, "backup_dir", str(directory))
    # An unreadable passphrase file breaks the backup...
    _set(node, "backup_passphrase_file", str(tmp_path / "missing.txt"))
    scheduler = AutoBackupScheduler(_config(node))
    # The attempt "started" at 1000, long before now, so its retry must
    # be anchored to when it finished, not when it started.
    with pytest.raises(Exception):
        scheduler.run(now=1000.0)
    finished = time.time()
    # ...but the next backup is not due until the interval passes after
    # the failure, so a failing backup does not retry every mail-loop pass.
    assert scheduler.due(now=finished) is False
    assert scheduler.due(now=finished + 3600) is True


def test_scheduler_reservation_skips_taken_names(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    directory = tmp_path / "backups"
    directory.mkdir()
    _set(node, "backup_interval", 3600)
    _set(node, "backup_dir", str(directory))
    # Another worker already claimed the base name for this second.
    base = "tau-backup-alice-20251125-000000.tar.gz"
    (directory / base).write_bytes(b"")
    scheduler = AutoBackupScheduler(_config(node))
    target = scheduler.run(now=1764028800.0)
    assert target is not None
    assert target.name == "tau-backup-alice-20251125-000000-2.tar.gz"
    assert (directory / base).read_bytes() == b""


def test_scheduler_short_interval_follows_live_setting(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    directory = tmp_path / "backups"
    _set(node, "backup_interval", 3600)
    _set(node, "backup_dir", str(directory))
    scheduler = AutoBackupScheduler(_config(node))
    assert scheduler.run(now=1764028800.0) is not None
    # Shortening the live interval takes effect without waiting out the old one.
    _set(node, "backup_interval", 60)
    assert scheduler.due(now=1764028800.0 + 59) is False
    assert scheduler.due(now=1764028800.0 + 60) is True
    assert scheduler.seconds_until_due(now=1764028800.0 + 30) == 30.0


# --- CLI config commands --------------------------------------------------


def test_parser_backup_config_subcommands():
    parser = cli.build_parser()
    parsed = parser.parse_args(["config", "backup-interval", "3600"])
    assert parsed.func is cli.cmd_config
    assert parsed.config_command == "backup-interval"
    assert parsed.seconds == 3600.0
    parsed = parser.parse_args(["config", "backup-dir", "/tmp/backups"])
    assert parsed.config_command == "backup-dir"
    parsed = parser.parse_args(["config", "backup-keep", "3"])
    assert parsed.config_command == "backup-keep"
    assert parsed.count == 3
    parsed = parser.parse_args(["config", "backup-passphrase-file", "p.txt"])
    assert parsed.config_command == "backup-passphrase-file"
    with pytest.raises(SystemExit):
        parser.parse_args(["config", "backup-interval", "-5"])
    with pytest.raises(SystemExit):
        parser.parse_args(["config", "backup-keep", "abc"])


def test_cli_backup_interval_show_set(tmp_path, capsys):
    node = _init_node(tmp_path, "a", "alice")
    args = ["--data-dir", str(node)]
    assert cli.main(["config", "backup-interval", *args]) == 0
    out = capsys.readouterr().out
    assert "Backup interval: off" in out
    assert cli.main(["config", "backup-interval", "3600", *args]) == 0
    assert backup_interval(node) == 3600.0
    capsys.readouterr()
    assert cli.main(["config", "backup-interval", "--json", *args]) == 0
    assert json.loads(capsys.readouterr().out) == {"backup_interval": 3600.0}


def test_cli_backup_dir_rejects_relative_and_inside(tmp_path, capsys):
    node = _init_node(tmp_path, "a", "alice")
    args = ["--data-dir", str(node)]
    assert cli.main(["config", "backup-dir", "relative/path", *args]) != 0
    assert cli.main(["config", "backup-dir", str(node), *args]) != 0
    assert cli.main(["config", "backup-dir", str(node / "sub"), *args]) != 0
    directory = tmp_path / "backups"
    assert cli.main(["config", "backup-dir", str(directory), *args]) == 0
    assert backup_dir(node) == directory


def test_cli_backup_keep_show_set(tmp_path, capsys):
    node = _init_node(tmp_path, "a", "alice")
    args = ["--data-dir", str(node)]
    assert cli.main(["config", "backup-keep", *args]) == 0
    assert "Backup keep: 7" in capsys.readouterr().out
    assert cli.main(["config", "backup-keep", "0", *args]) == 0
    assert backup_keep(node) == 0


def test_cli_backup_passphrase_file_show_set_clear(tmp_path, capsys):
    node = _init_node(tmp_path, "a", "alice")
    args = ["--data-dir", str(node)]
    assert cli.main(["config", "backup-passphrase-file", *args]) == 0
    assert "Backup passphrase file: none" in capsys.readouterr().out

    short_file = tmp_path / "short.txt"
    short_file.write_text("tiny\n", encoding="utf-8")
    assert (
        cli.main(["config", "backup-passphrase-file", str(short_file), *args])
        != 0
    )
    assert backup_passphrase_file(node) is None

    missing = tmp_path / "missing.txt"
    assert (
        cli.main(["config", "backup-passphrase-file", str(missing), *args])
        != 0
    )

    good_file = tmp_path / "good.txt"
    good_file.write_text("correct horse battery staple\n", encoding="utf-8")
    assert (
        cli.main(["config", "backup-passphrase-file", str(good_file), *args])
        == 0
    )
    assert backup_passphrase_file(node) == good_file

    assert cli.main(["config", "backup-passphrase-file", "--clear", *args]) == 0
    assert backup_passphrase_file(node) is None


def test_cli_config_summary_lists_backup_settings(tmp_path, capsys):
    node = _init_node(tmp_path, "a", "alice")
    assert cli.main(["config", "--data-dir", str(node)]) == 0
    out = capsys.readouterr().out
    assert "backup interval: off" in out
    assert "backup directory:" in out
    assert "backup keep: 7" in out
    assert "backup passphrase file: none" in out
    assert "tau config backup-interval <seconds>" in out
