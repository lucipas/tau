from __future__ import annotations

import json
import stat
import time
from pathlib import Path

import pytest

from tau import cli
from tau.auto_backup import (
    AUTO_BACKUP_ENCRYPTED_SUFFIX,
    AutoBackupScheduler,
    auto_backup_filename,
    is_auto_backup_name,
    list_auto_backups,
    load_auto_backup_registry,
    newest_auto_backup_time,
    parse_auto_backup_name,
    prune_auto_backups,
    save_auto_backup_registry,
)
from tau.backup import (
    is_encrypted_backup,
    restore_backup,
)
from tau.config import (
    AUTO_BACKUPS_FILE_NAME,
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


def _register(data_dir, *names):
    """Seed the auto-backup registry as if the worker had created these."""
    save_auto_backup_registry(data_dir, list(names))


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
    # A relative stored value resolves against the data directory's
    # parent, so it means the same file from any working directory.
    _set(node, "backup_passphrase_file", "rel-secret.txt")
    assert backup_passphrase_file(node) == node.parent / "rel-secret.txt"
    for bad in ("", 42, None):
        _set(node, "backup_passphrase_file", bad)
        assert backup_passphrase_file(node) is None


# --- filenames, parsing, registry -----------------------------------------


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


def test_parse_auto_backup_name():
    assert parse_auto_backup_name(
        "tau-backup-alice-20251125-000000.tar.gz", "alice"
    ) == ("20251125-000000", 0)
    assert parse_auto_backup_name(
        "tau-backup-alice-20251125-000000-2.tar.gz", "alice"
    ) == ("20251125-000000", 2)
    assert parse_auto_backup_name(
        "tau-backup-alice-20251125-000000.tau", "alice"
    ) == ("20251125-000000", 0)
    # A dotted node name keeps its dots; the counter lands after the stamp.
    assert parse_auto_backup_name(
        "tau-backup-my.node-20251125-000000-2.tar.gz", "my.node"
    ) == ("20251125-000000", 2)
    # Another node's archive, a manual archive, and a wrong suffix
    # never parse as this node's automatic backup.
    assert parse_auto_backup_name(
        "tau-backup-bob-20251125-000000.tar.gz", "alice"
    ) is None
    assert parse_auto_backup_name("tau-backup-notes.tar.gz", "alice") is None
    assert parse_auto_backup_name(
        "tau-backup-alice-20251125-000000.zip", "alice"
    ) is None
    assert is_auto_backup_name(
        "tau-backup-alice-20251125-000000.tar.gz", "alice"
    )
    assert not is_auto_backup_name("tau-backup-notes.tar.gz", "alice")


def test_auto_backup_registry_round_trip_and_corruption(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    assert load_auto_backup_registry(node) == []
    save_auto_backup_registry(node, ["tau-backup-alice-1.tar.gz"])
    assert load_auto_backup_registry(node) == ["tau-backup-alice-1.tar.gz"]
    # A corrupt registry fails safe: no provenance known, nothing pruned.
    (node / AUTO_BACKUPS_FILE_NAME).write_text("{not json", encoding="utf-8")
    assert load_auto_backup_registry(node) == []
    (node / AUTO_BACKUPS_FILE_NAME).write_text(
        '{"backups": "not a list"}\n', encoding="utf-8"
    )
    assert load_auto_backup_registry(node) == []


def test_list_and_prune_are_registry_gated_and_per_node(tmp_path):
    directory = tmp_path / "backups"
    directory.mkdir()
    alice_old = "tau-backup-alice-20251125-000000.tar.gz"
    alice_new = "tau-backup-alice-20251125-010000.tar.gz"
    bob = "tau-backup-bob-20251125-020000.tar.gz"
    manual = "tau-backup-alice-20251125-030000.tar.gz"
    for name in (alice_old, alice_new, bob, manual):
        (directory / name).write_bytes(b"x")
    (directory / "notes.txt").write_text("keep me", encoding="utf-8")
    # Only alice's *registered* files list: bob's archive and a manual
    # archive named exactly like an automatic one are invisible.
    listed = list_auto_backups(directory, "alice", [alice_old, alice_new])
    assert [p.name for p in listed] == [alice_old, alice_new]
    assert list_auto_backups(directory, "bob", [bob]) == [directory / bob]
    assert list_auto_backups(tmp_path / "missing", "alice", [alice_old]) == []

    deleted = prune_auto_backups(directory, 1, "alice", [alice_old, alice_new])
    assert [p.name for p in deleted] == [alice_old]
    assert (directory / alice_new).exists()
    # Bob's archive and the manual lookalike survive alice's prune.
    assert (directory / bob).exists()
    assert (directory / manual).exists()
    assert (directory / "notes.txt").exists()

    assert prune_auto_backups(directory, 0, "alice", [alice_new]) == []
    assert len(list_auto_backups(directory, "alice", [alice_new])) == 1


def test_prune_keeps_newest_same_second_backup(tmp_path):
    directory = tmp_path / "backups"
    directory.mkdir()
    original = "tau-backup-alice-20251125-000000.tar.gz"
    second = "tau-backup-alice-20251125-000000-2.tar.gz"
    for name in (original, second):
        (directory / name).write_bytes(b"x")
    # The -2 file sorts after the original, so keep=1 keeps the newer one.
    deleted = prune_auto_backups(
        directory, 1, "alice", [original, second]
    )
    assert [p.name for p in deleted] == [original]
    assert (directory / second).exists()
    # And a dotted node name prunes by its own files only.
    dotted = "tau-backup-my.node-20251125-000000.tar.gz"
    (directory / dotted).write_bytes(b"x")
    assert list_auto_backups(directory, "my.node", [dotted]) == [
        directory / dotted
    ]
    assert prune_auto_backups(directory, 0, "my.node", [dotted]) == []


# --- scheduler ------------------------------------------------------------


def test_scheduler_disabled_without_interval(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    scheduler = AutoBackupScheduler(_config(node))
    assert scheduler.due(now=1000.0) is False
    assert scheduler.next_due_in(now=1000.0) is None
    assert scheduler.run(now=1000.0) is None


def test_scheduler_due_when_no_backup_yet(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    _set(node, "backup_interval", 3600)
    scheduler = AutoBackupScheduler(_config(node))
    assert scheduler.due(now=1000.0) is True
    assert scheduler.next_due_in(now=1000.0) == 0.0


def test_scheduler_due_follows_newest_registered_backup(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    directory = tmp_path / "backups"
    directory.mkdir()
    _set(node, "backup_interval", 3600)
    _set(node, "backup_dir", str(directory))
    newest = "tau-backup-alice-20251125-000000.tar.gz"
    (directory / newest).write_bytes(b"x")
    # An unregistered file does not seed the schedule...
    scheduler = AutoBackupScheduler(_config(node))
    assert scheduler.due(now=1000.0) is True
    # ...but once recorded, the newest backup's filename timestamp
    # does: 20251125-000000 is epoch 1764028800, so with a 3600s
    # interval the next backup is due at 1764032400.
    _register(node, newest)
    scheduler = AutoBackupScheduler(_config(node))
    assert scheduler.due(now=1764032399.0) is False
    assert scheduler.due(now=1764032400.0) is True
    assert scheduler.next_due_in(now=1764030000.0) == 2400.0


def test_scheduler_due_follows_live_interval_changes(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    directory = tmp_path / "backups"
    directory.mkdir()
    _set(node, "backup_interval", 3600)
    _set(node, "backup_dir", str(directory))
    newest = "tau-backup-alice-20251125-000000.tar.gz"
    (directory / newest).write_bytes(b"x")
    _register(node, newest)
    scheduler = AutoBackupScheduler(_config(node))
    # Newest backup at epoch 1764028800, interval 3600: due at 1764032400.
    assert scheduler.due(now=1764032399.0) is False
    # Shortening the interval takes effect at once: due at 1764028860.
    _set(node, "backup_interval", 60)
    assert scheduler.due(now=1764028859.0) is False
    assert scheduler.due(now=1764028860.0) is True
    # Lengthening it delays the backup instead of firing early.
    _set(node, "backup_interval", 7200)
    assert scheduler.due(now=1764032400.0) is False
    assert scheduler.due(now=1764036000.0) is True


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
    # The backup is recorded in the registry, and the directory was
    # created private.
    assert load_auto_backup_registry(node) == [target.name]
    assert stat.S_IMODE(directory.stat().st_mode) == 0o700
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
    assert load_auto_backup_registry(node) == [first.name, second.name]


def test_scheduler_run_prunes_newest_same_second_with_keep_one(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    directory = tmp_path / "backups"
    _set(node, "backup_interval", 60)
    _set(node, "backup_dir", str(directory))
    _set(node, "backup_keep", 1)
    scheduler = AutoBackupScheduler(_config(node))
    first = scheduler.run(now=1764028800.0)
    second = scheduler.run(now=1764028800.0)
    # keep=1 keeps the newer -2 archive, not the original.
    assert not first.exists()
    assert second.exists()
    assert load_auto_backup_registry(node) == [second.name]


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
    registered = load_auto_backup_registry(node)
    kept = [p.name for p in list_auto_backups(directory, "alice", registered)]
    assert kept == [
        "tau-backup-alice-20251125-000200.tar.gz",
        "tau-backup-alice-20251125-000400.tar.gz",
    ]
    assert registered == kept


def test_scheduler_failure_advances_schedule(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    directory = tmp_path / "backups"
    _set(node, "backup_interval", 3600)
    _set(node, "backup_dir", str(directory))
    # An unreadable passphrase file breaks the backup...
    _set(node, "backup_passphrase_file", str(tmp_path / "missing.txt"))
    scheduler = AutoBackupScheduler(_config(node))
    with pytest.raises(Exception):
        scheduler.run(now=1000.0)
    # ...but the next backup is not due until the interval passes,
    # so a failing backup does not retry every mail-loop pass.
    assert scheduler.due(now=1000.0) is False
    assert scheduler.due(now=1000.0 + 3600) is True


def test_scheduler_slow_failure_backs_off_from_completion(
    tmp_path, monkeypatch
):
    node = _init_node(tmp_path, "a", "alice")
    directory = tmp_path / "backups"
    _set(node, "backup_interval", 60)
    _set(node, "backup_dir", str(directory))
    scheduler = AutoBackupScheduler(_config(node))

    import tau.auto_backup as auto_backup

    def slow_fail(*args, **kwargs):
        time.sleep(0.5)
        raise auto_backup.BackupError("boom")

    monkeypatch.setattr(auto_backup, "create_backup", slow_fail)
    started = time.time()
    with pytest.raises(auto_backup.BackupError):
        scheduler.run()
    finished = time.time()
    # The retry deadline is an interval after the failure finished,
    # not after it started.
    assert scheduler.due(now=finished + 59) is False
    assert scheduler.due(now=finished + 60) is True
    assert finished - started >= 0.5


def test_scheduler_next_due_in_while_running(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    _set(node, "backup_interval", 60)
    scheduler = AutoBackupScheduler(_config(node))
    scheduler._running = True
    # While a backup runs, the next one is at least an interval away.
    assert scheduler.next_due_in(now=1000.0) == 60.0
    scheduler._running = False
    assert scheduler.next_due_in(now=1000.0) == 0.0


def test_scheduler_start_failure_backs_off(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    _set(node, "backup_interval", 3600)
    scheduler = AutoBackupScheduler(_config(node))
    assert scheduler.due(now=1000.0) is True
    assert scheduler.next_due_in(now=1000.0) == 0.0
    scheduler.note_start_failure(now=1000.0)
    # Within the retry delay the backup is not due, and the mail
    # loop waits out the delay instead of spinning on a zero wait.
    assert scheduler.due(now=1000.0) is False
    assert scheduler.due(now=1059.0) is False
    assert scheduler.next_due_in(now=1000.0) == 60.0
    assert scheduler.next_due_in(now=1030.0) == 30.0
    # Once the delay elapses the backup is due again at once.
    assert scheduler.due(now=1060.0) is True
    assert scheduler.next_due_in(now=1060.0) == 0.0


def test_scheduler_run_clears_start_failure(tmp_path):
    node = _init_node(tmp_path, "a", "alice")
    _set(node, "backup_interval", 30)
    scheduler = AutoBackupScheduler(_config(node))
    scheduler.note_start_failure(now=1000.0)
    assert scheduler.due(now=1000.0) is False
    scheduler.run(now=1000.0)
    # The worker started, so the failure is behind us: the next
    # backup is due an interval after the attempt, not after the
    # longer start-failure delay.
    assert scheduler.due(now=1030.0) is True


def test_maybe_auto_backup_start_failure_backs_off(tmp_path, monkeypatch):
    import threading
    from types import SimpleNamespace

    from tau.node import TauNode

    node = _init_node(tmp_path, "a", "alice")
    _set(node, "backup_interval", 3600)
    scheduler = AutoBackupScheduler(_config(node))
    namespace = SimpleNamespace(
        _auto_backup_thread=None,
        _stop_event=threading.Event(),
        _auto_backup_scheduler=scheduler,
    )

    class _FailingThread:
        def __init__(self, *args, **kwargs):
            pass

        def start(self):
            raise RuntimeError("can't start new thread")

        def is_alive(self):
            return False

    monkeypatch.setattr(threading, "Thread", _FailingThread)
    # A worker that cannot start must not raise into the mail loop,
    # and the failure is recorded so the scheduler backs off.
    TauNode._maybe_auto_backup(namespace)
    assert scheduler._last_start_failure_at is not None
    assert scheduler.due() is False
    assert scheduler.next_due_in() > 0


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


def test_cli_backup_passphrase_file_show_set_clear(
    tmp_path, capsys, monkeypatch
):
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

    # A relative path is stored resolved, so it means the same file
    # no matter which directory a later `tau serve` starts from.
    monkeypatch.chdir(tmp_path)
    assert (
        cli.main(["config", "backup-passphrase-file", "good.txt", *args])
        == 0
    )
    assert backup_passphrase_file(node) == good_file.resolve()

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
