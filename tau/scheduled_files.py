"""Scheduled file sends: queue a file to send later.

The file analogue of a scheduled DM (``tau send --delay``): ``tau
send-file --to PEER FILE --delay 5m`` stages the file instead of
transferring it, and a serving node sends it when the time comes.
The same for ``tau group send-file GROUP FILE --delay 5m`` (fan-out
to the group's members).  A file whose send time has come goes out
on the next mail-loop pass, about every 45 seconds.

The queue lives in the data directory
(``scheduled-files.json``) so scheduled files survive restarts, and
the staged bytes live beside it in ``scheduled-files/<id>/<name>``
from schedule time, so the original file can move or be deleted.
The queue entry carries only the file descriptor (SHA-256, size,
name) — the bytes are never in the queue itself.  A send that fails
keeps the entry queued and retries with exponential backoff (up to
a limit) instead of dropping the file — the same failure direction
as the outbox: never lose a queued message silently.  Cancelling
removes the entry and deletes its staged bytes.

A transfer runs off the mail-loop thread (see
``TauNode._publish_due_scheduled_files``): a file transfer can take
minutes, so the publish pass never holds the queue lock across a
send.  Each entry is claimed under the lock just before sending —
stamped with the acquisition time, and renewed while the transfer
runs — so a cancel that lands after the claim refuses instead of
racing the transfer, and a transfer longer than the claim lease
cannot let its own claim expire under it; a cancel before the claim
removes the entry and its staged bytes.  A claim older than the
lease is treated as stale (its worker is presumed dead) and the
entry becomes claimable — and cancellable — again.

Entries are validated on load the way the other small JSON stores
are: a malformed file raises rather than silently dropping queued
files, and the publish pass treats a corrupt queue as nothing-due
(with a warning) so one bad file cannot stall mail.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import shutil
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

from .protocol import MAX_FILE_BYTES, sanitize_file_name
from .util import atomic_write_json, ensure_dir

log = logging.getLogger(__name__)

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore[assignment]

try:  # Windows only; the lock uses msvcrt there, as identity.py does.
    import msvcrt
except ImportError:  # pragma: no cover - POSIX
    msvcrt = None  # type: ignore[assignment]

SCHEDULED_FILE_MODE = 0o600
SCHEDULED_FILE_ID_BYTES = 8
# Same bound as a scheduled DM: the 30d mail TTL less an hour of
# delivery slack, so a file cannot come due after its envelope
# would already have expired.
MAX_SCHEDULED_FILE_DELAY = 30 * 24 * 60 * 60 - 3600
# Retry policy for a failed send: exponential backoff from the base,
# capped, and a hard attempt limit after which the entry stays queued
# (never dropped silently) but is no longer retried automatically.
MAX_SEND_ATTEMPTS = 12
SEND_RETRY_BASE_SECONDS = 60.0
SEND_RETRY_CAP_SECONDS = 60.0 * 60.0
# How long a send claim lasts.  A claim marks an entry as in flight
# so a cancel cannot start racing a transfer; a claim older than this
# is treated as stale (the worker that made it is presumed dead) and
# the entry becomes claimable — and cancellable — again.  The lease
# is generous because one group fan-out can take a transfer timeout
# per member; within a live process only one send pass runs at a time,
# so a live transfer is never double-claimed.
CLAIM_LEASE_SECONDS = 3600.0


class ScheduledFileError(ValueError):
    """Raised when a scheduled-files file or value is malformed."""


class PartialSend(Exception):
    """A group fan-out that delivered to some members but not all.

    Carries the member hashes that received the file, so the retry
    sends only to the members still owed instead of redelivering to
    everyone.
    """

    def __init__(self, message: str, *, delivered_members: Tuple[str, ...] = ()):
        super().__init__(message)
        self.delivered_members = tuple(delivered_members)


def new_scheduled_file_id() -> str:
    """A fresh queue id: 16 lowercase hex characters."""
    return secrets.token_hex(SCHEDULED_FILE_ID_BYTES)


def _clean_id(value: Any) -> str:
    if not isinstance(value, str):
        raise ScheduledFileError(f"not a scheduled file id: {value!r}")
    lowered = value.lower()
    if len(lowered) != 2 * SCHEDULED_FILE_ID_BYTES:
        raise ScheduledFileError(f"not a scheduled file id: {value!r}")
    try:
        bytes.fromhex(lowered)
    except ValueError:
        raise ScheduledFileError(f"not a scheduled file id: {value!r}") from None
    return lowered


def _clean_target(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ScheduledFileError("scheduled file target must not be empty")
    return value.strip()


def _clean_kind(value: Any) -> str:
    if value not in ("dm", "group"):
        raise ScheduledFileError(
            f"scheduled file kind must be 'dm' or 'group': {value!r}"
        )
    return value


def _clean_group_id(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ScheduledFileError(f"not a group id: {value!r}")
    lowered = value.lower()
    if len(lowered) != 32:
        raise ScheduledFileError(f"not a group id: {value!r}")
    try:
        bytes.fromhex(lowered)
    except ValueError:
        raise ScheduledFileError(f"not a group id: {value!r}") from None
    return lowered


def _clean_members(value: Any) -> Tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise ScheduledFileError("group members must be a list")
    cleaned = []
    for member in value:
        if not isinstance(member, str):
            raise ScheduledFileError(f"not a member hash: {member!r}")
        lowered = member.lower()
        if len(lowered) != 32:
            raise ScheduledFileError(f"not a member hash: {member!r}")
        try:
            bytes.fromhex(lowered)
        except ValueError:
            raise ScheduledFileError(f"not a member hash: {member!r}") from None
        cleaned.append(lowered)
    return tuple(cleaned)


def _clean_timeout(value: Any, label: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ScheduledFileError(f"not a timeout: {label}={value!r}")
    try:
        seconds = float(value)
    except OverflowError:
        # A JSON integer beyond the float range is a malformed value,
        # not a crash: the load must raise ScheduledFileError so the
        # publish pass can skip the queue instead of aborting.
        raise ScheduledFileError(f"not a timeout: {label}={value!r}") from None
    if seconds != seconds or seconds == float("inf"):
        raise ScheduledFileError(f"not a timeout: {label}={value!r}")
    if positive and seconds <= 0:
        raise ScheduledFileError(f"not a timeout: {label}={value!r}")
    if seconds < 0:
        raise ScheduledFileError(f"not a timeout: {label}={value!r}")
    return seconds


def _clean_file(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ScheduledFileError("scheduled file descriptor must be an object")
    sha256 = value.get("sha256")
    if not isinstance(sha256, str) or len(sha256) != 64:
        raise ScheduledFileError(
            "scheduled file sha256 must be a 64 character hex digest"
        )
    try:
        bytes.fromhex(sha256)
    except ValueError:
        raise ScheduledFileError(
            "scheduled file sha256 must be a 64 character hex digest"
        ) from None
    size = value.get("size")
    if isinstance(size, bool) or not isinstance(size, int):
        raise ScheduledFileError("scheduled file size must be an integer")
    if size < 1 or size > MAX_FILE_BYTES:
        raise ScheduledFileError(
            f"scheduled file of {size} bytes is outside the "
            f"1..{MAX_FILE_BYTES} byte limit"
        )
    name = value.get("name")
    if (
        not isinstance(name, str)
        or not sanitize_file_name(name)
        or sanitize_file_name(name) != name
    ):
        raise ScheduledFileError(
            "scheduled file name must be a safe bare file name"
        )
    return {"sha256": sha256.lower(), "size": size, "name": name}


def _clean_timestamp(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ScheduledFileError(f"not a timestamp: {label}={value!r}")
    try:
        stamp = float(value)
    except OverflowError:
        # As in _clean_timeout: a malformed value must raise
        # ScheduledFileError, not abort the load with OverflowError.
        raise ScheduledFileError(f"not a timestamp: {label}={value!r}") from None
    if stamp <= 0 or stamp != stamp or stamp in (float("inf"), float("-inf")):
        raise ScheduledFileError(f"not a timestamp: {label}={value!r}")
    return stamp


def _clean_attempts(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ScheduledFileError(f"not an attempt count: {value!r}")
    return value


def _clean_retry_at(value: Any) -> float:
    # 0.0 means "eligible now"; unlike publish_at it is not a real time.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ScheduledFileError(f"not a retry time: {value!r}")
    try:
        stamp = float(value)
    except OverflowError:
        # As in _clean_timeout: a malformed value must raise
        # ScheduledFileError, not abort the load with OverflowError.
        raise ScheduledFileError(f"not a retry time: {value!r}") from None
    if stamp < 0 or stamp != stamp or stamp == float("inf"):
        raise ScheduledFileError(f"not a retry time: {value!r}")
    return stamp


@dataclass(frozen=True)
class ScheduledFile:
    """One queued file, waiting for its send time."""

    id: str
    target: str
    kind: str
    file: Dict[str, Any] = field(default_factory=dict)
    publish_at: float = 0.0
    created_at: float = 0.0
    attempts: int = 0
    next_attempt_at: float = 0.0
    # Group deliveries pin the group they were queued for: its id and
    # the roster snapshot at schedule time.  Delivery resolves by id
    # and refuses to send when the roster no longer matches, so a
    # roster change or name reuse can never redirect the file to
    # members who were not in the group when it was queued.  DM
    # entries leave both empty.
    group_id: Optional[str] = None
    group_members: Tuple[str, ...] = ()
    # Members still owed after a partial group fan-out; empty means
    # "every member of the roster snapshot".  Updated on each failed
    # attempt so retries never redeliver to members who already
    # received the file.
    pending_members: Tuple[str, ...] = ()
    # Transfer limits the user set with --timeout / --discover-timeout
    # at schedule time, carried through to the eventual send instead
    # of being silently replaced by defaults.
    timeout: float = 300.0
    discover_timeout: float = 10.0
    # When a send pass claimed this entry (0.0 means unclaimed).  A
    # fresh claim makes the entry un-cancellable until the transfer
    # finishes; a stale one (older than CLAIM_LEASE_SECONDS) is
    # treated as dead and the entry becomes claimable again.
    claimed_at: float = 0.0

    def __post_init__(self) -> None:
        # A frozen dataclass still builds through __init__: validate
        # here so a hand-built entry cannot slip past from_dict's
        # checks with a bad descriptor or an out-of-range size.
        object.__setattr__(self, "id", _clean_id(self.id))
        object.__setattr__(self, "target", _clean_target(self.target))
        object.__setattr__(self, "kind", _clean_kind(self.kind))
        object.__setattr__(self, "file", _clean_file(self.file))
        object.__setattr__(self, "publish_at", _clean_timestamp(self.publish_at, "publish_at"))
        object.__setattr__(self, "created_at", _clean_timestamp(self.created_at, "created_at"))
        object.__setattr__(self, "attempts", _clean_attempts(self.attempts))
        object.__setattr__(self, "next_attempt_at", _clean_retry_at(self.next_attempt_at))
        object.__setattr__(self, "group_id", _clean_group_id(self.group_id))
        object.__setattr__(self, "group_members", _clean_members(self.group_members))
        object.__setattr__(self, "pending_members", _clean_members(self.pending_members))
        object.__setattr__(self, "timeout", _clean_timeout(self.timeout, "timeout", positive=True))
        object.__setattr__(
            self,
            "discover_timeout",
            _clean_timeout(self.discover_timeout, "discover_timeout"),
        )
        object.__setattr__(self, "claimed_at", _clean_retry_at(self.claimed_at))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "target": self.target,
            "kind": self.kind,
            "file": dict(self.file),
            "publish_at": self.publish_at,
            "created_at": self.created_at,
            "attempts": self.attempts,
            "next_attempt_at": self.next_attempt_at,
            "group_id": self.group_id,
            "group_members": list(self.group_members),
            "pending_members": list(self.pending_members),
            "timeout": self.timeout,
            "discover_timeout": self.discover_timeout,
            "claimed_at": self.claimed_at,
        }

    @classmethod
    def from_dict(cls, data: Any) -> "ScheduledFile":
        if not isinstance(data, dict):
            raise ScheduledFileError("scheduled file must be a JSON object")
        return cls(
            id=_clean_id(data.get("id")),
            target=_clean_target(data.get("target")),
            kind=_clean_kind(data.get("kind")),
            file=_clean_file(data.get("file")),
            publish_at=_clean_timestamp(data.get("publish_at"), "publish_at"),
            created_at=_clean_timestamp(data.get("created_at"), "created_at"),
            attempts=_clean_attempts(data.get("attempts", 0)),
            next_attempt_at=_clean_retry_at(data.get("next_attempt_at", 0.0)),
            group_id=_clean_group_id(data.get("group_id")),
            group_members=_clean_members(data.get("group_members")),
            pending_members=_clean_members(data.get("pending_members")),
            timeout=_clean_timeout(
                data.get("timeout", 300.0), "timeout", positive=True
            ),
            discover_timeout=_clean_timeout(
                data.get("discover_timeout", 10.0), "discover_timeout"
            ),
            claimed_at=_clean_retry_at(data.get("claimed_at", 0.0)),
        )


@dataclass(frozen=True)
class ScheduledFiles:
    """The queued files, oldest send time first."""

    files: Tuple[ScheduledFile, ...] = ()

    def __len__(self) -> int:
        return len(self.files)

    def due(self, now: Optional[float] = None) -> List[ScheduledFile]:
        """Queued files whose send time has come, oldest first."""
        at = now if now is not None else time.time()
        return [entry for entry in self.files if entry.publish_at <= at]

    def resolve(self, prefix: str) -> ScheduledFile:
        """Find one queued file by full id or unique prefix."""
        if not isinstance(prefix, str) or not prefix.strip():
            raise ScheduledFileError("a scheduled file id is required")
        matches = [
            entry for entry in self.files if entry.id.startswith(prefix.lower())
        ]
        if not matches:
            raise ScheduledFileError(f"no scheduled file {prefix!r}")
        if len(matches) > 1:
            raise ScheduledFileError(
                f"ambiguous scheduled file id {prefix!r} ({len(matches)} match)"
            )
        return matches[0]

    def with_added(self, entry: ScheduledFile) -> "ScheduledFiles":
        if any(existing.id == entry.id for existing in self.files):
            raise ScheduledFileError(
                f"scheduled file {entry.id} is already queued"
            )
        ordered = tuple(sorted((*self.files, entry), key=lambda e: e.publish_at))
        return ScheduledFiles(files=ordered)

    def with_replaced(self, entry: ScheduledFile) -> "ScheduledFiles":
        if not any(existing.id == entry.id for existing in self.files):
            raise ScheduledFileError(f"no scheduled file {entry.id!r}")
        swapped = tuple(
            entry if existing.id == entry.id else existing
            for existing in self.files
        )
        return ScheduledFiles(files=swapped)

    def with_removed(self, file_id: str) -> "ScheduledFiles":
        kept = tuple(entry for entry in self.files if entry.id != file_id)
        if len(kept) == len(self.files):
            raise ScheduledFileError(f"no scheduled file {file_id!r}")
        return ScheduledFiles(files=kept)

    def to_dict(self) -> Dict[str, Any]:
        return {"files": [entry.to_dict() for entry in self.files]}

    @classmethod
    def from_dict(cls, data: Any) -> "ScheduledFiles":
        if not isinstance(data, dict):
            raise ScheduledFileError("scheduled-files must be a JSON object")
        if "files" not in data:
            raise ScheduledFileError("files is required")
        raw = data.get("files")
        if not isinstance(raw, list):
            raise ScheduledFileError("files must be a list")
        return cls(files=tuple(ScheduledFile.from_dict(item) for item in raw))


def load_scheduled_files(
    path: Optional[Union[str, Path]],
) -> ScheduledFiles:
    """Load the queue at *path*; a missing file means nothing scheduled."""
    if path is None:
        return ScheduledFiles()
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return ScheduledFiles()
    except OSError as exc:
        raise ScheduledFileError(
            f"could not read scheduled-files {target}: {exc}"
        ) from exc
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ScheduledFileError(
            f"could not read scheduled-files {target}: {exc}"
        ) from exc
    return ScheduledFiles.from_dict(data)


def save_scheduled_files(path: Union[str, Path], files: ScheduledFiles) -> Path:
    """Persist *files* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    return atomic_write_json(target, files.to_dict(), mode=SCHEDULED_FILE_MODE)


@contextmanager
def scheduled_files_lock(path: Union[str, Path]) -> Iterator[None]:
    """Serialize read-modify-write updates of the scheduled-files queue.

    Same shape as the scheduled-channel-messages lock: a flock on a
    ``.scheduled-files.json.lock`` sidecar.  An atomic file
    replacement alone does not protect the whole update — without the
    lock, two commands racing load the same file and the last save
    discards the other's change.  Callers must reload inside the lock.
    """
    if fcntl is None and msvcrt is None:  # pragma: no cover - neither platform
        yield
        return
    target = Path(path).expanduser()
    # The data directory may not exist yet (a first schedule on a
    # fresh node): the lock sidecar cannot be opened until it does.
    ensure_dir(target.parent)
    lock_path = target.with_name(f".{target.name}.lock")
    with open(lock_path, "a+") as lock:
        if fcntl is not None:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        else:  # pragma: no cover - exercised on Windows
            lock.seek(0, os.SEEK_END)
            if lock.tell() == 0:
                lock.write(" ")
                lock.flush()
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            else:  # pragma: no cover - exercised on Windows
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)


def read_file_payload(path: Union[str, Path]) -> Tuple[bytes, str, str]:
    """Read and validate a file for sending; returns ``(data, name, sha256)``.

    The read is bounded at one byte past the limit, so an oversized (or
    growing) file is rejected without loading it all.  The same limit
    an immediate ``tau send-file`` enforces, so a file that could never
    be sent is refused here, not on every send attempt.
    """
    source = Path(path).expanduser()
    if not source.is_file():
        raise ScheduledFileError(f"no such file: {source}")
    try:
        with source.open("rb") as handle:
            data = handle.read(MAX_FILE_BYTES + 1)
    except OSError as exc:
        raise ScheduledFileError(f"could not read {source}: {exc}") from exc
    if len(data) < 1 or len(data) > MAX_FILE_BYTES:
        raise ScheduledFileError(
            f"file of {len(data)} bytes is outside the 1..{MAX_FILE_BYTES} "
            "byte limit"
        )
    name = sanitize_file_name(source.name)
    if not name:
        raise ScheduledFileError(f"file name {source.name!r} is not usable")
    return data, name, hashlib.sha256(data).hexdigest()


def staged_file_path(
    staged_dir: Union[str, Path], file_id: str, name: str
) -> Path:
    """Where a staged file's bytes live: ``<dir>/<id>/<name>``.

    The file keeps its original (sanitized) name under its queue id,
    so the transfer path reads the same name the user sent — the id
    directory is what makes the staged copy unique.
    """
    clean_name = sanitize_file_name(name)
    if not clean_name or clean_name != name:
        raise ScheduledFileError(
            "scheduled file name must be a safe bare file name"
        )
    return Path(staged_dir).expanduser() / _clean_id(file_id) / clean_name


def stage_file(
    staged_dir: Union[str, Path], file_id: str, name: str, data: bytes
) -> Path:
    """Write *data* to its staged path (mode 0600) and return the path.

    Writes atomically (temp file + rename) so a concurrent send pass
    never reads a half-written copy, then fsyncs the file and its
    directory: the queue entry is committed after this returns, so
    the staged bytes must be durable before the caller records them
    — otherwise a crash in between leaves a queued entry pointing at
    missing or incomplete bytes.  The directory fsync is best-effort
    on platforms without it (Windows cannot fsync a directory).
    """
    target = staged_file_path(staged_dir, file_id, name)
    ensure_dir(target.parent)
    tmp = target.parent / f".{target.name}.tmp-{secrets.token_hex(4)}"
    try:
        with open(tmp, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.chmod(tmp, SCHEDULED_FILE_MODE)
        except OSError:  # pragma: no cover - best effort
            pass
        os.replace(tmp, target)
        _fsync_dir(target.parent)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
        except OSError:  # pragma: no cover - best effort
            pass
    return target


def _fsync_dir(directory: Path) -> None:
    """Flush *directory* to disk; best-effort where unsupported."""
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def remove_staged_file(staged_dir: Union[str, Path], file_id: str) -> None:
    """Delete a staged file's directory (bytes + temp files, if any).

    A missing directory is already the goal state and is fine, but a
    real deletion failure raises: callers must not report a cancel or
    a delivery as fully done while the bytes are still on disk.
    """
    target = Path(staged_dir).expanduser() / _clean_id(file_id)
    if not target.exists():
        return
    shutil.rmtree(target)


def schedule_file(
    path: Union[str, Path],
    *,
    target: str,
    kind: str,
    file: Dict[str, Any],
    publish_at: float,
    now: Optional[float] = None,
    file_id: Optional[str] = None,
    group_id: Optional[str] = None,
    group_members: Tuple[str, ...] = (),
    timeout: float = 300.0,
    discover_timeout: float = 10.0,
) -> ScheduledFile:
    """Queue a file for *publish_at*; returns the stored entry.

    The file descriptor gets the same validation as an immediate
    ``tau send-file`` (1..10 MiB, safe bare name, 64-hex SHA-256), so
    a file that could never be sent is refused here, not on every
    send pass.  The caller stages the bytes first (see
    :func:`stage_file`) under *file_id* when given — otherwise a
    fresh id is generated — so the queue entry and the staged bytes
    agree on the id.  For a group file, *group_id* and the roster
    snapshot *group_members* pin the delivery target (see
    :class:`ScheduledFile`); *timeout* / *discover_timeout* are the
    transfer limits the eventual send uses.
    """
    entry = ScheduledFile(
        id=_clean_id(file_id) if file_id is not None else new_scheduled_file_id(),
        target=_clean_target(target),
        kind=_clean_kind(kind),
        file=_clean_file(file),
        publish_at=_clean_timestamp(publish_at, "publish_at"),
        created_at=now if now is not None else time.time(),
        group_id=_clean_group_id(group_id),
        group_members=_clean_members(group_members),
        timeout=_clean_timeout(timeout, "timeout", positive=True),
        discover_timeout=_clean_timeout(discover_timeout, "discover_timeout"),
    )
    with scheduled_files_lock(path):
        return _schedule_file_locked(path, entry)


def _schedule_file_locked(path: Union[str, Path], entry: ScheduledFile) -> ScheduledFile:
    """Queue *entry* assuming the scheduled-files lock is already held.

    The lock-holding split of :func:`schedule_file`: callers that
    stage bytes first (see ``tau.cli._queue_file_for_later``) hold
    the lock across staging and the queue commit, so a crash or a
    backup cannot slip between the two and leave orphaned staged
    bytes.  The lock is an flock sidecar and is not reentrant, so
    that caller cannot just call :func:`schedule_file` inside its
    own lock block.
    """
    queue = load_scheduled_files(path)
    save_scheduled_files(path, queue.with_added(entry))
    return entry


def cancel_scheduled_file(
    path: Union[str, Path],
    prefix: str,
    *,
    on_cancel: Optional[Callable[[ScheduledFile], None]] = None,
    now: Optional[float] = None,
) -> ScheduledFile:
    """Drop one queued file by full id or unique prefix; returns it.

    When *on_cancel* is given it runs under the queue lock, after the
    entry is resolved and before it is removed — so a send pass
    cannot start the transfer between the callback and the removal.
    The callback is how the CLI deletes the staged bytes for the
    cancelled entry; if it raises, the entry stays queued and the
    cancel fails instead of reporting success while bytes remain.

    A freshly claimed entry — a transfer is in flight — cannot be
    cancelled: the bytes are already going out.  A stale claim (the
    worker is presumed dead) does not block the cancel.
    """
    at = now if now is not None else time.time()
    with scheduled_files_lock(path):
        queue = load_scheduled_files(path)
        entry = queue.resolve(prefix)
        if entry.claimed_at and entry.claimed_at + CLAIM_LEASE_SECONDS > at:
            raise ScheduledFileError(
                f"scheduled file {entry.id} is already sending; "
                "it cannot be cancelled"
            )
        if on_cancel is not None:
            on_cancel(entry)
        save_scheduled_files(path, queue.with_removed(entry.id))
    return entry


def _claim_renewer(
    path: Union[str, Path],
    file_id: str,
    claim_value: float,
    clock: Callable[[], float],
    stop: threading.Event,
    interval: float,
) -> threading.Thread:
    """A daemon thread that keeps one send claim fresh.

    While a transfer runs, re-stamps the entry's ``claimed_at`` to
    the clock every *interval* seconds, so a transfer longer than
    ``CLAIM_LEASE_SECONDS`` never lets its own claim expire and a
    cancel cannot delete the entry under the live transfer.  Only
    renews while the entry still carries *claim_value* — a cleared
    or replaced claim means this transfer is over, and the thread
    exits.  A non-positive *interval* disables renewal (the claim
    is still stamped at acquisition).  The caller sets *stop* and
    joins before the post-send queue update.
    """

    def _run() -> None:
        if interval <= 0:
            return
        # The claim this thread owns: it advances on every renewal,
        # so the ownership check below must track the latest stamp,
        # not the original one — otherwise the first renewal would
        # make the next check think the claim had been replaced.
        current_claim = claim_value
        while not stop.wait(interval):
            try:
                with scheduled_files_lock(path):
                    queue = load_scheduled_files(path)
                    current = queue.resolve(file_id)
                    if current.claimed_at != current_claim:
                        return
                    current_claim = clock()
                    save_scheduled_files(
                        path,
                        queue.with_replaced(
                            replace(current, claimed_at=current_claim)
                        ),
                    )
            except ScheduledFileError:
                return
            except OSError:
                # A transient queue-write failure must not kill the
                # transfer; the next renewal retries, and the lease
                # still expires on its own if renewal keeps failing.
                continue

    return threading.Thread(
        target=_run, name=f"scheduled-file-claim-{file_id}", daemon=True
    )


def publish_due_scheduled_files(
    path: Union[str, Path],
    staged_dir: Union[str, Path],
    send: Callable[[ScheduledFile, Path], None],
    *,
    now: Optional[float] = None,
    should_stop: Optional[Callable[[], bool]] = None,
    claim_renew_seconds: float = 300.0,
) -> Dict[str, int]:
    """Send every due queued file through *send* (a ``(entry, path)``
    callable that raises on failure).

    The pass runs off the mail-loop thread (see
    ``TauNode._publish_due_scheduled_files``): a file transfer can
    take minutes, so unlike the scheduled-channel pass this one never
    holds the queue lock across a send.  Each entry is claimed under
    the lock just before sending — stamped with the acquisition time,
    not the pass start, so a long earlier transfer cannot leave a
    later entry's claim already stale — and the claim is renewed
    every *claim_renew_seconds* while the transfer runs, so a
    transfer longer than the lease cannot expire its own claim and
    let a cancel delete the entry under it.  A cancel that lands
    before the claim is honoured; a cancel during the transfer
    itself cannot recall the bytes.

    A failed attempt (exception from *send*, or the staged bytes
    missing) keeps the entry queued with exponential backoff measured
    from when the attempt actually failed, not from when the pass
    began — a slow transfer must not eat the retry delay.  After
    ``MAX_SEND_ATTEMPTS`` it is no longer retried automatically but
    stays queued for the user to inspect or cancel.  A pass ends
    early when *should_stop* says so, between entries.  When *send*
    raises :class:`PartialSend` (a group fan-out that reached some
    members), the members still owed are remembered on the entry so
    the retry sends only to them.

    Returns ``{"published": n, "kept": m}``.  A corrupt queue file logs
    a warning and sends nothing, so one bad file cannot stall the
    mail loop that calls this.
    """
    clock = (lambda: now) if now is not None else time.time
    at = clock()
    with scheduled_files_lock(path):
        try:
            queue = load_scheduled_files(path)
        except ScheduledFileError as exc:
            log.warning(
                "Scheduled-files at %s is unusable (%s); skipping",
                path,
                exc,
            )
            return {"published": 0, "kept": 0}
        candidates = [
            entry
            for entry in queue.due(at)
            if entry.next_attempt_at <= at and entry.attempts < MAX_SEND_ATTEMPTS
        ]
    published = 0
    for entry in candidates:
        if should_stop is not None and should_stop():
            break
        staged = staged_file_path(staged_dir, entry.id, entry.file["name"])
        # Claim under the lock, stamped at acquisition: a cancel
        # since the snapshot, or a fresh claim from another pass,
        # means this entry is gone or already in flight and must not
        # be sent again.  Stamping the acquisition time (not the pass
        # start) keeps a long earlier transfer from leaving this
        # entry's claim already stale when its own transfer begins.
        claim_at = clock()
        with scheduled_files_lock(path):
            try:
                queue = load_scheduled_files(path)
                current = queue.resolve(entry.id)
            except ScheduledFileError:
                continue
            if (
                current.claimed_at
                and current.claimed_at + CLAIM_LEASE_SECONDS > claim_at
            ):
                continue
            entry = replace(current, claimed_at=claim_at)
            save_scheduled_files(path, queue.with_replaced(entry))
        # Renew the lease while the transfer runs, so a transfer
        # longer than the lease cannot expire its own claim and let a
        # cancel delete the entry under it.  The renewer stops before
        # the post-send queue update below.
        stop_renew = threading.Event()
        renewer = _claim_renewer(
            path, entry.id, claim_at, clock, stop_renew, claim_renew_seconds
        )
        renewer.start()
        try:
            send(entry, staged)
        except Exception as exc:  # noqa: BLE001 - a failed send retries
            stop_renew.set()
            renewer.join(timeout=2.0)
            # The retry clock starts when this attempt failed, not
            # when the pass began: an earlier slow transfer must not
            # consume the backoff.  With an injected `now` (tests)
            # the pass time stands in for the failure time.
            failed_at = clock()
            delivered = tuple(
                getattr(exc, "delivered_members", ()) or ()
            )
            with scheduled_files_lock(path):
                try:
                    queue = load_scheduled_files(path)
                    current = queue.resolve(entry.id)
                except ScheduledFileError:
                    # Cancelled while the transfer ran: the entry is
                    # gone, so there is nothing to retry.
                    continue
                attempts = current.attempts + 1
                retry_in = min(
                    SEND_RETRY_BASE_SECONDS * 2 ** (attempts - 1),
                    SEND_RETRY_CAP_SECONDS,
                )
                pending = current.pending_members
                if delivered:
                    owed = pending or current.group_members
                    pending = tuple(
                        member for member in owed if member not in delivered
                    )
                updated = replace(
                    current,
                    attempts=attempts,
                    next_attempt_at=failed_at + retry_in,
                    pending_members=pending,
                    claimed_at=0.0,
                )
                save_scheduled_files(path, queue.with_replaced(updated))
            log.warning(
                "Scheduled file %s could not send (%s); keeping it queued",
                entry.id,
                exc,
            )
            if attempts >= MAX_SEND_ATTEMPTS:
                log.warning(
                    "Scheduled file %s gave up after %d attempts; "
                    "it stays queued until cancelled",
                    entry.id,
                    attempts,
                )
            continue
        stop_renew.set()
        renewer.join(timeout=2.0)
        with scheduled_files_lock(path):
            try:
                queue = load_scheduled_files(path)
                save_scheduled_files(path, queue.with_removed(entry.id))
            except ScheduledFileError:
                # Cancelled while the transfer ran: the entry is
                # already gone, which is the same outcome.
                pass
            else:
                # Delete the staged bytes under the same lock hold as
                # the queue removal: a backup listing members between
                # the two would archive bytes no queue entry owns,
                # and the restore would refuse the whole archive.
                try:
                    remove_staged_file(staged_dir, entry.id)
                except OSError as exc:
                    # The file went out and the queue is clean; the
                    # orphaned staged bytes are a leak to report, not
                    # a reason to pretend the send failed.
                    log.warning(
                        "Scheduled file %s sent but its staged bytes could not "
                        "be deleted (%s)",
                        entry.id,
                        exc,
                    )
        published += 1
    return {"published": published, "kept": len(candidates) - published}
