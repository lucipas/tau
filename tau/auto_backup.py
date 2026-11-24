"""Automatic scheduled backups for a serving node.

``tau backup`` is manual-only, so a node that serves for weeks backs
up exactly as often as its owner remembers to run the command.  The
automatic backup worker closes that gap: while ``tau serve`` runs, it
takes a backup every ``backup_interval`` seconds (see #750), through
the same :func:`tau.backup.create_backup` path a manual backup uses —
``live=True``, so the serving node's own writes are consistent — and
prunes older automatic backups past ``backup_keep``.

The schedule is read live from ``node.json`` on every check, like the
announce interval: ``tau config backup-interval`` takes effect at the
next mail-loop pass without a restart.  The next backup is due when
the newest automatic backup is older than the interval, or when there
is no automatic backup yet — enabling the feature backs up promptly,
and a restart does not back up again until the interval has passed
since the last one.  A failed backup (a full disk, an unreadable
passphrase file) is logged and the schedule advances anyway, so the
worker retries at the next interval instead of hammering the disk
every mail-loop pass; it never stops the node.

Only files this worker created are ever pruned: the ``tau-backup-``
prefix plus the plaintext (``.tar.gz``) or encrypted (``.tau``) suffix.
A manual backup that happens to share the directory is untouched.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from .backup import (
    BackupError,
    _clean_passphrase,
    create_backup,
    read_passphrase_file,
)
from .config import (
    TauConfig,
    backup_dir,
    backup_interval,
    backup_keep,
    backup_passphrase_file,
)

log = logging.getLogger(__name__)

AUTO_BACKUP_PREFIX = "tau-backup-"
AUTO_BACKUP_PLAIN_SUFFIX = ".tar.gz"
AUTO_BACKUP_ENCRYPTED_SUFFIX = ".tau"

# A backup filename carries the node name, so backups from several
# nodes sharing one directory stay distinguishable. Only filename-safe
# characters survive; anything else becomes a dash.
_UNSAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


def auto_backup_filename(
    node_name: str, *, encrypted: bool, now: Optional[float] = None
) -> str:
    """Timestamped name for one automatic backup.

    ``tau-backup-<node>-<YYYYMMDD-HHMMSS>.tar.gz`` for a plaintext
    backup, ``.tau`` for an encrypted one. The timestamp is UTC, so
    names sort in the order the backups were taken.
    """
    name = _UNSAFE_NAME_RE.sub("-", str(node_name or "")).strip("-") or "node"
    stamp = datetime.fromtimestamp(
        time.time() if now is None else now, tz=timezone.utc
    ).strftime("%Y%m%d-%H%M%S")
    suffix = (
        AUTO_BACKUP_ENCRYPTED_SUFFIX if encrypted else AUTO_BACKUP_PLAIN_SUFFIX
    )
    return f"{AUTO_BACKUP_PREFIX}{name}-{stamp}{suffix}"


def is_auto_backup_name(name: str) -> bool:
    """True when *name* is a file this worker created."""
    return name.startswith(AUTO_BACKUP_PREFIX) and (
        name.endswith(AUTO_BACKUP_PLAIN_SUFFIX)
        or name.endswith(AUTO_BACKUP_ENCRYPTED_SUFFIX)
    )


def list_auto_backups(backup_dir_path: Path) -> List[Path]:
    """Automatic backups in *backup_dir_path*, oldest first.

    Names sort in the order the backups were taken (the timestamp is
    part of the name), so no stat call is needed. A missing directory
    holds no backups, and a directory that cannot be read is treated
    the same way — the worker backs up rather than failing.
    """
    try:
        entries = list(Path(backup_dir_path).iterdir())
    except OSError:
        return []
    backups = [
        entry
        for entry in entries
        if entry.is_file() and is_auto_backup_name(entry.name)
    ]
    return sorted(backups, key=lambda entry: entry.name)


def newest_auto_backup_mtime(backup_dir_path: Path) -> Optional[float]:
    """Modification time of the newest automatic backup, or None."""
    backups = list_auto_backups(backup_dir_path)
    if not backups:
        return None
    try:
        return backups[-1].stat().st_mtime
    except OSError:
        return None


def prune_auto_backups(backup_dir_path: Path, keep: int) -> List[Path]:
    """Delete automatic backups past the newest *keep*.

    ``keep`` of 0 keeps all. Only files this worker created are ever
    deleted; a delete that fails is logged and the rest still prune,
    so one unreadable file cannot wedge the worker.
    """
    if keep <= 0:
        return []
    backups = list_auto_backups(backup_dir_path)
    doomed = backups[: max(0, len(backups) - keep)]
    deleted: List[Path] = []
    for entry in doomed:
        try:
            entry.unlink()
        except OSError:
            log.warning("Could not prune old automatic backup %s", entry)
            continue
        deleted.append(entry)
    return deleted


class AutoBackupScheduler:
    """When the next automatic backup is due, and taking it.

    One instance lives on the serving node; the mail loop asks
    :meth:`due` each pass and, when due, runs :meth:`run` on a worker
    thread (a backup can take minutes, so it never runs on the
    mail-loop thread). All settings are read live from ``node.json``
    on every call, so ``tau config`` changes apply without a restart.
    """

    def __init__(self, config: TauConfig) -> None:
        self.config = config
        # When the next backup may run. None until the first due check,
        # which seeds it from the newest automatic backup so a restart
        # respects the schedule instead of backing up immediately.
        self._next_at: Optional[float] = None

    def due(self, now: Optional[float] = None) -> bool:
        """True when an automatic backup should run now."""
        moment = time.time() if now is None else now
        interval = backup_interval(self.config.data_dir)
        if interval <= 0:
            return False
        if self._next_at is None:
            newest = newest_auto_backup_mtime(
                backup_dir(self.config.data_dir)
            )
            self._next_at = (
                newest + interval if newest is not None else 0.0
            )
        return moment >= self._next_at

    def run(self, now: Optional[float] = None) -> Optional[Path]:
        """Take one automatic backup, or None when disabled.

        The schedule advances even when the backup fails, so a failure
        retries at the next interval rather than every mail-loop pass.
        Raises :class:`tau.backup.BackupError` when the backup itself
        fails; the caller logs it and the node keeps serving.
        """
        moment = time.time() if now is None else now
        interval = backup_interval(self.config.data_dir)
        try:
            return self._take_backup(moment)
        finally:
            if interval > 0:
                self._next_at = moment + interval

    def _take_backup(self, now: float) -> Optional[Path]:
        if backup_interval(self.config.data_dir) <= 0:
            return None
        directory = backup_dir(self.config.data_dir)
        passphrase: Optional[str] = None
        passphrase_path = backup_passphrase_file(self.config.data_dir)
        if passphrase_path is not None:
            passphrase = _clean_passphrase(
                read_passphrase_file(passphrase_path)
            )
        directory.mkdir(parents=True, exist_ok=True)
        target = self._fresh_target(directory, passphrase is not None, now)
        target, _manifest = create_backup(
            self.config, target, live=True, passphrase=passphrase
        )
        pruned = prune_auto_backups(
            directory, backup_keep(self.config.data_dir)
        )
        if pruned:
            log.info("Pruned %d old automatic backup(s)", len(pruned))
        log.info("Automatic backup written to %s", target)
        return target

    def _fresh_target(
        self, directory: Path, encrypted: bool, now: float
    ) -> Path:
        """A target name that does not already exist in *directory*.

        Two backups in the same second (a restart racing the schedule,
        or a manual backup landing on the same name) must not collide:
        the second takes a ``-2`` suffix, then ``-3``, and so on.
        """
        base = auto_backup_filename(
            self.config.name, encrypted=encrypted, now=now
        )
        target = directory / base
        if not target.exists():
            return target
        # Split the suffix off the generated name: ".tar.gz" is two
        # dots, ".tau" is one, so split on the first dot after the
        # timestamp instead of assuming one suffix shape.
        stem = base.split(".")[0]
        suffix = base[len(stem):]
        for attempt in range(2, 100):
            candidate = directory / f"{stem}-{attempt}{suffix}"
            if not candidate.exists():
                return candidate
        raise BackupError(
            f"could not find a free automatic backup name in {directory}"
        )
