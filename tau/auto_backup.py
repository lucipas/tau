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
import os
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


def _node_slug(node_name: str) -> str:
    return _UNSAFE_NAME_RE.sub("-", str(node_name or "")).strip("-") or "node"


def auto_backup_filename(
    node_name: str, *, encrypted: bool, now: Optional[float] = None
) -> str:
    """Timestamped name for one automatic backup.

    ``tau-backup-<node>-<YYYYMMDD-HHMMSS>.tar.gz`` for a plaintext
    backup, ``.tau`` for an encrypted one. The timestamp is UTC, so
    names sort in the order the backups were taken.
    """
    name = _node_slug(node_name)
    stamp = datetime.fromtimestamp(
        time.time() if now is None else now, tz=timezone.utc
    ).strftime("%Y%m%d-%H%M%S")
    suffix = (
        AUTO_BACKUP_ENCRYPTED_SUFFIX if encrypted else AUTO_BACKUP_PLAIN_SUFFIX
    )
    return f"{AUTO_BACKUP_PREFIX}{name}-{stamp}{suffix}"


def _auto_backup_pattern(node_name: str) -> "re.Pattern[str]":
    """Strict pattern for the automatic backups *node_name* writes.

    Matches only ``tau-backup-<slug>-<YYYYMMDD-HHMMSS>[-N]`` with the
    plaintext or encrypted suffix, where ``<slug>`` is this node's own
    sanitized name. A manual backup such as ``tau-backup-notes.tar.gz``
    or a sibling node's archive does not match.
    """
    slug = re.escape(_node_slug(node_name))
    return re.compile(
        rf"^{re.escape(AUTO_BACKUP_PREFIX)}{slug}-"
        r"(?P<stamp>\d{8}-\d{6})(?:-(?P<n>\d+))?"
        rf"(?:{re.escape(AUTO_BACKUP_PLAIN_SUFFIX)}"
        rf"|{re.escape(AUTO_BACKUP_ENCRYPTED_SUFFIX)})$"
    )


def is_auto_backup_name(name: str, node_name: str) -> bool:
    """True when *name* is an automatic backup written by *node_name*."""
    return _auto_backup_pattern(node_name).match(name) is not None


def list_auto_backups(backup_dir_path: Path, node_name: str) -> List[Path]:
    """Automatic backups of *node_name* in *backup_dir_path*, oldest first.

    Ordered by the timestamp in the name, then the collision counter,
    so nodes sharing one directory never mix series. A missing
    directory holds no backups, and a directory that cannot be read is
    treated the same way — the worker backs up rather than failing.
    """
    try:
        entries = list(Path(backup_dir_path).iterdir())
    except OSError:
        return []
    pattern = _auto_backup_pattern(node_name)
    matched = []
    for entry in entries:
        match = pattern.match(entry.name)
        if match is None or not entry.is_file():
            continue
        matched.append(
            (match.group("stamp"), int(match.group("n") or 0), entry)
        )
    matched.sort(key=lambda item: item[:2])
    return [entry for _stamp, _n, entry in matched]


def newest_auto_backup_mtime(
    backup_dir_path: Path, node_name: str
) -> Optional[float]:
    """Modification time of the newest automatic backup of *node_name*."""
    backups = list_auto_backups(backup_dir_path, node_name)
    if not backups:
        return None
    try:
        return backups[-1].stat().st_mtime
    except OSError:
        return None


def prune_auto_backups(
    backup_dir_path: Path, keep: int, node_name: str
) -> List[Path]:
    """Delete this node's automatic backups past the newest *keep*.

    ``keep`` of 0 keeps all. Only files this worker created are ever
    deleted; a delete that fails is logged and the rest still prune,
    so one unreadable file cannot wedge the worker.
    """
    if keep <= 0:
        return []
    backups = list_auto_backups(backup_dir_path, node_name)
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
        # Start of the last backup, or the time the last attempt
        # finished when it failed. None until the first schedule check,
        # which seeds it from this node's newest automatic backup so a
        # restart respects the schedule instead of backing up at once.
        self._last_at: Optional[float] = None
        self._schedule_dir: Optional[Path] = None

    def _sync_schedule(self) -> None:
        """Seed the schedule on first use, and again if the directory moves."""
        directory = backup_dir(self.config.data_dir)
        if self._last_at is None or directory != self._schedule_dir:
            newest = newest_auto_backup_mtime(directory, self.config.name)
            self._last_at = newest if newest is not None else 0.0
            self._schedule_dir = directory

    def due(self, now: Optional[float] = None) -> bool:
        """True when an automatic backup should run now."""
        moment = time.time() if now is None else now
        interval = backup_interval(self.config.data_dir)
        if interval <= 0:
            return False
        self._sync_schedule()
        # The live interval is read on every call, so a shortened or
        # lengthened setting applies at the next check.
        return moment >= self._last_at + interval

    def seconds_until_due(self, now: Optional[float] = None) -> Optional[float]:
        """Seconds until the next backup is due, or None when disabled."""
        moment = time.time() if now is None else now
        interval = backup_interval(self.config.data_dir)
        if interval <= 0:
            return None
        self._sync_schedule()
        return max(0.0, self._last_at + interval - moment)

    def run(self, now: Optional[float] = None) -> Optional[Path]:
        """Take one automatic backup, or None when disabled.

        A failed attempt schedules its retry one interval after it
        finished, so a slow failure cannot leave the retry deadline in
        the past. Raises :class:`tau.backup.BackupError` when the backup
        itself fails; the caller logs it and the node keeps serving.
        """
        moment = time.time() if now is None else now
        if backup_interval(self.config.data_dir) <= 0:
            return None
        self._sync_schedule()
        try:
            target = self._take_backup(moment)
        except BaseException:
            self._last_at = time.time()
            raise
        self._last_at = moment
        return target

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
        placeholder = self._reserve_target(
            directory, passphrase is not None, now
        )
        try:
            # The placeholder is ours, so force=True only replaces it;
            # create_backup writes a temp file and renames it into place.
            target, _manifest = create_backup(
                self.config,
                placeholder,
                force=True,
                live=True,
                passphrase=passphrase,
            )
        except BaseException:
            _discard_placeholder(placeholder)
            raise
        pruned = prune_auto_backups(
            directory, backup_keep(self.config.data_dir), self.config.name
        )
        if pruned:
            log.info("Pruned %d old automatic backup(s)", len(pruned))
        log.info("Automatic backup written to %s", target)
        return target

    def _reserve_target(
        self, directory: Path, encrypted: bool, now: float
    ) -> Path:
        """Atomically claim a fresh target name in *directory*.

        The name is created with O_EXCL, so two workers (even in
        different processes) that pick the same timestamp cannot both
        claim it: the loser moves on to the next free ``-N`` suffix.
        """
        base = auto_backup_filename(
            self.config.name, encrypted=encrypted, now=now
        )
        suffix = (
            AUTO_BACKUP_ENCRYPTED_SUFFIX
            if base.endswith(AUTO_BACKUP_ENCRYPTED_SUFFIX)
            else AUTO_BACKUP_PLAIN_SUFFIX
        )
        stem = base[: -len(suffix)]
        for attempt in range(1, 100):
            name = base if attempt == 1 else f"{stem}-{attempt}{suffix}"
            candidate = directory / name
            try:
                fd = os.open(
                    candidate, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
                )
            except FileExistsError:
                continue
            os.close(fd)
            return candidate
        raise BackupError(
            f"could not find a free automatic backup name in {directory}"
        )


def _discard_placeholder(path: Path) -> None:
    """Remove a reservation left behind by a failed backup."""
    try:
        path.unlink()
    except OSError:
        pass
