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
next mail-loop pass without a restart.  The next backup is due an
interval after the last attempt — success or failure — measured from
when that attempt *finished*, so a failed backup retries at the next
interval instead of hammering the disk every mail-loop pass, and a
slow backup does not immediately trigger another attempt.  When no
backup has ever been attempted, the worker seeds from the newest
automatic backup's filename timestamp, or backs up promptly when there
is none — enabling the feature backs up promptly, and a restart does
not back up again until the interval has passed since the last one.

Provenance: only files this worker created are ever pruned or counted
toward the schedule.  Each backup the worker writes is recorded by
name in ``auto-backups.json`` (in the data directory, itself backed
up), and listing, pruning, and schedule seeding consider only names
that are both recorded there and present on disk, and that match this
node's own filename pattern.  A manual archive that happens to share
the directory — even one named like an automatic backup — is never
touched, and two nodes sharing one backup directory never prune each
other's archives.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .backup import (
    BackupError,
    _clean_passphrase,
    create_backup,
    read_passphrase_file,
)
from .config import (
    AUTO_BACKUPS_FILE_NAME,
    TauConfig,
    backup_dir,
    backup_interval,
    backup_keep,
    backup_passphrase_file,
)
from .util import atomic_write_json, ensure_dir, read_json

log = logging.getLogger(__name__)

AUTO_BACKUP_PREFIX = "tau-backup-"
AUTO_BACKUP_PLAIN_SUFFIX = ".tar.gz"
AUTO_BACKUP_ENCRYPTED_SUFFIX = ".tau"

# A worker that cannot start (thread exhaustion) is retried after this
# bounded delay, not immediately: without it the mail loop would spin
# on a zero-second wait, retrying thread creation every pass.
START_FAILURE_RETRY_DELAY = 60.0

# A backup filename carries the node name, so backups from several
# nodes sharing one directory stay distinguishable. Only filename-safe
# characters survive; anything else becomes a dash.
_UNSAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
# The strict shape of a worker-created filename, parsed from the end
# so a node name holding digits or dashes cannot swallow the stamp:
# ``tau-backup-<node>-<YYYYMMDD-HHMMSS>[-<counter>].tar.gz|.tau``.
# The counter disambiguates two backups taken in the same second.
_AUTO_BACKUP_NAME_RE = re.compile(
    r"^tau-backup-(?P<node>.+)-(?P<stamp>\d{8}-\d{6})"
    r"(?:-(?P<counter>\d+))?\.(?P<suffix>tar\.gz|tau)$"
)

# The registry is a plain list of filenames the worker created, kept
# in the data directory. A corrupt or missing registry means "no
# provenance known": backups still happen, nothing is pruned, and new
# backups rebuild the record.
_AUTO_BACKUPS_KEY = "backups"


def sanitize_backup_node_name(node_name: str) -> str:
    """The node-name part of an automatic backup filename."""
    return _UNSAFE_NAME_RE.sub("-", str(node_name or "")).strip("-") or "node"


def auto_backup_filename(
    node_name: str, *, encrypted: bool, now: Optional[float] = None
) -> str:
    """Timestamped name for one automatic backup.

    ``tau-backup-<node>-<YYYYMMDD-HHMMSS>.tar.gz`` for a plaintext
    backup, ``.tau`` for an encrypted one. The timestamp is UTC, so
    names sort in the order the backups were taken.
    """
    name = sanitize_backup_node_name(node_name)
    stamp = datetime.fromtimestamp(
        time.time() if now is None else now, tz=timezone.utc
    ).strftime("%Y%m%d-%H%M%S")
    suffix = (
        AUTO_BACKUP_ENCRYPTED_SUFFIX if encrypted else AUTO_BACKUP_PLAIN_SUFFIX
    )
    return f"{AUTO_BACKUP_PREFIX}{name}-{stamp}{suffix}"


def parse_auto_backup_name(
    name: str, node_name: str
) -> Optional[Tuple[str, int]]:
    """Parse a worker-created filename for *node_name*.

    Returns ``(stamp, counter)`` — the ``YYYYMMDD-HHMMSS`` stamp and
    the same-second collision counter (0 when absent) — or None when
    the name is not one of this node's automatic backups. The node
    part must match exactly, so another node's archives, and manual
    archives that merely share the prefix, never match.
    """
    match = _AUTO_BACKUP_NAME_RE.match(name)
    if match is None:
        return None
    if match.group("node") != sanitize_backup_node_name(node_name):
        return None
    counter = match.group("counter")
    return match.group("stamp"), int(counter) if counter is not None else 0


def is_auto_backup_name(name: str, node_name: str) -> bool:
    """True when *name* is one of *node_name*'s automatic backups."""
    return parse_auto_backup_name(name, node_name) is not None


def load_auto_backup_registry(data_dir: Path) -> List[str]:
    """Filenames this node's worker recorded as its own backups."""
    raw = read_json(Path(data_dir) / AUTO_BACKUPS_FILE_NAME, default=None)
    if not isinstance(raw, dict):
        return []
    names = raw.get(_AUTO_BACKUPS_KEY)
    if not isinstance(names, list):
        return []
    return [name for name in names if isinstance(name, str)]


def save_auto_backup_registry(data_dir: Path, names: List[str]) -> None:
    """Persist the filenames this node's worker created."""
    atomic_write_json(
        Path(data_dir) / AUTO_BACKUPS_FILE_NAME,
        {_AUTO_BACKUPS_KEY: list(names)},
        mode=0o600,
    )


def list_auto_backups(
    backup_dir_path: Path, node_name: str, registered: List[str]
) -> List[Path]:
    """This node's automatic backups, oldest first.

    Only names that are both recorded in the registry and present on
    disk count — a manual archive is never listed, even when it is
    named exactly like an automatic backup. Ordered by parsed
    (timestamp, collision counter), so a same-second ``-2`` backup
    sorts after the original instead of before it. A missing or
    unreadable directory holds no backups.
    """
    try:
        present = {entry.name for entry in Path(backup_dir_path).iterdir()}
    except OSError:
        return []
    parsed = []
    for name in registered:
        if name not in present:
            continue
        stamp = parse_auto_backup_name(name, node_name)
        if stamp is None:
            continue
        parsed.append((stamp, Path(backup_dir_path) / name))
    parsed.sort(key=lambda item: item[0])
    return [path for _stamp, path in parsed]


def newest_auto_backup_time(
    backup_dir_path: Path, node_name: str, registered: List[str]
) -> Optional[float]:
    """When this node's newest automatic backup was taken, or None.

    Read from the filename's timestamp, not the file's mtime: the
    stamp is the time the worker took the backup, and a copied or
    restored archive keeps its stamp while its mtime changes.
    """
    backups = list_auto_backups(backup_dir_path, node_name, registered)
    if not backups:
        return None
    stamp = parse_auto_backup_name(backups[-1].name, node_name)
    if stamp is None:
        return None
    return (
        datetime.strptime(stamp[0], "%Y%m%d-%H%M%S")
        .replace(tzinfo=timezone.utc)
        .timestamp()
    )


def prune_auto_backups(
    backup_dir_path: Path,
    keep: int,
    node_name: str,
    registered: List[str],
) -> List[Path]:
    """Delete this node's automatic backups past the newest *keep*.

    ``keep`` of 0 keeps all. Only registered files are ever deleted;
    a delete that fails is logged and the rest still prune, so one
    unreadable file cannot wedge the worker.
    """
    if keep <= 0:
        return []
    backups = list_auto_backups(backup_dir_path, node_name, registered)
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
        # Completion time of the last backup attempt (success or
        # failure). None until the first attempt: the schedule then
        # seeds from the newest recorded backup instead.
        self._last_attempt_at: Optional[float] = None
        # Time the backup worker last failed to start (thread
        # exhaustion), or None. While the retry delay after such a
        # failure is running, the scheduler reports not-due, so the
        # mail loop backs off instead of retrying thread creation
        # on every pass.
        self._last_start_failure_at: Optional[float] = None
        # True while run() is executing on the worker thread.
        self._running = False

    def note_start_failure(self, now: Optional[float] = None) -> None:
        """Record that the backup worker failed to start.

        The failure does not count as a backup attempt — the schedule
        still owes a backup, just not right now. The next attempt
        waits out START_FAILURE_RETRY_DELAY (see due() and
        next_due_in()) instead of retrying at once.
        """
        self._last_start_failure_at = time.time() if now is None else now

    def _start_failure_retry_in(self, moment: float) -> float:
        """Seconds left in the start-failure backoff, or 0 when clear."""
        if self._last_start_failure_at is None:
            return 0.0
        return max(
            0.0,
            self._last_start_failure_at + START_FAILURE_RETRY_DELAY - moment,
        )

    def _last_backup_at(self) -> Optional[float]:
        """Last time a backup completed, for scheduling purposes.

        The later of the last attempt's completion and the newest
        recorded backup's filename timestamp, so a restart seeds from
        the newest backup and a failure still backs off. None when no
        backup has ever been attempted or recorded: the first backup
        is then due at once, so enabling the feature backs up
        promptly.
        """
        registered = load_auto_backup_registry(self.config.data_dir)
        newest = newest_auto_backup_time(
            backup_dir(self.config.data_dir),
            self.config.name,
            registered,
        )
        candidates = []
        if newest is not None:
            candidates.append(newest)
        if self._last_attempt_at is not None:
            candidates.append(self._last_attempt_at)
        return max(candidates) if candidates else None

    def due(self, now: Optional[float] = None) -> bool:
        """True when an automatic backup should run now."""
        moment = time.time() if now is None else now
        interval = backup_interval(self.config.data_dir)
        if interval <= 0:
            return False
        if self._start_failure_retry_in(moment) > 0:
            return False
        last = self._last_backup_at()
        if last is None:
            return True
        return moment >= last + interval

    def next_due_in(self, now: Optional[float] = None) -> Optional[float]:
        """Seconds until the next automatic backup, or None when disabled.

        While a backup is running, the next one cannot start before
        an interval after the running one finishes, so the configured
        interval is a safe lower bound — the mail loop can sleep that
        long instead of spinning on the deadline.
        """
        moment = time.time() if now is None else now
        interval = backup_interval(self.config.data_dir)
        if interval <= 0:
            return None
        if self._running:
            return interval
        retry_in = self._start_failure_retry_in(moment)
        if retry_in > 0:
            return retry_in
        last = self._last_backup_at()
        if last is None:
            return 0.0
        return max(0.0, last + interval - moment)

    def run(self, now: Optional[float] = None) -> Optional[Path]:
        """Take one automatic backup, or None when disabled.

        The attempt time is recorded at completion, success or
        failure, so a slow or failing backup retries an interval
        after it finished rather than immediately. Raises
        :class:`tau.backup.BackupError` when the backup itself fails;
        the caller logs it and the node keeps serving.
        """
        moment = time.time() if now is None else now
        self._running = True
        # The worker started, so a recorded start failure is behind
        # us; a later failure is governed by the attempt time.
        self._last_start_failure_at = None
        try:
            return self._take_backup(moment)
        finally:
            # Completion time in production; the caller's clock in
            # tests, so fake-now tests stay deterministic.
            self._last_attempt_at = time.time() if now is None else moment
            self._running = False

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
        # A missing backup directory is created private (0700): it
        # holds identity material. An existing directory is left
        # untouched — a caller-selected folder's mode is not ours.
        ensure_dir(directory, restrict_existing=False)
        target = self._fresh_target(directory, passphrase is not None, now)
        target, _manifest = create_backup(
            self.config, target, live=True, passphrase=passphrase
        )
        registered = load_auto_backup_registry(self.config.data_dir)
        if target.name not in registered:
            registered.append(target.name)
        pruned = prune_auto_backups(
            directory,
            backup_keep(self.config.data_dir),
            self.config.name,
            registered,
        )
        if pruned:
            pruned_names = {entry.name for entry in pruned}
            registered = [
                name for name in registered if name not in pruned_names
            ]
            log.info("Pruned %d old automatic backup(s)", len(pruned))
        # Drop registry entries for files no longer on disk (a
        # restored registry, a hand-deleted archive) so the record
        # stays the set of backups this node can actually prune.
        registered = [
            name
            for name in registered
            if (directory / name).is_file()
            and parse_auto_backup_name(name, self.config.name) is not None
        ]
        save_auto_backup_registry(self.config.data_dir, registered)
        log.info("Automatic backup written to %s", target)
        return target

    def _fresh_target(
        self, directory: Path, encrypted: bool, now: float
    ) -> Path:
        """A target name that does not already exist in *directory*.

        Two backups in the same second (a restart racing the schedule,
        or a manual backup landing on the same name) must not collide:
        the second takes a ``-2`` suffix, then ``-3``, and so on. The
        suffix is stripped from the end, so a dotted node name keeps
        its dots and the counter lands after the timestamp.
        """
        base = auto_backup_filename(
            self.config.name, encrypted=encrypted, now=now
        )
        target = directory / base
        if not target.exists():
            return target
        if base.endswith(AUTO_BACKUP_PLAIN_SUFFIX):
            stem = base[: -len(AUTO_BACKUP_PLAIN_SUFFIX)]
            suffix = AUTO_BACKUP_PLAIN_SUFFIX
        else:
            stem = base[: -len(AUTO_BACKUP_ENCRYPTED_SUFFIX)]
            suffix = AUTO_BACKUP_ENCRYPTED_SUFFIX
        for attempt in range(2, 100):
            candidate = directory / f"{stem}-{attempt}{suffix}"
            if not candidate.exists():
                return candidate
        raise BackupError(
            f"could not find a free automatic backup name in {directory}"
        )
