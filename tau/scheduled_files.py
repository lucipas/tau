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
send.  A cancel that lands while a transfer is already in flight
cannot recall the bytes — the entry is gone, but the transfer
finishes.  Entries are validated on load the way the other small
JSON stores are: a malformed file raises rather than silently
dropping queued files, and the publish pass treats a corrupt queue
as nothing-due (with a warning) so one bad file cannot stall mail.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import shutil
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


class ScheduledFileError(ValueError):
    """Raised when a scheduled-files file or value is malformed."""


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
    stamp = float(value)
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
    stamp = float(value)
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
    never reads a half-written copy.
    """
    target = staged_file_path(staged_dir, file_id, name)
    ensure_dir(target.parent)
    tmp = target.parent / f".{target.name}.tmp-{secrets.token_hex(4)}"
    try:
        tmp.write_bytes(data)
        try:
            os.chmod(tmp, SCHEDULED_FILE_MODE)
        except OSError:  # pragma: no cover - best effort
            pass
        os.replace(tmp, target)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
        except OSError:  # pragma: no cover - best effort
            pass
    return target


def remove_staged_file(staged_dir: Union[str, Path], file_id: str) -> None:
    """Delete a staged file's directory (bytes + temp files, if any)."""
    target = Path(staged_dir).expanduser() / _clean_id(file_id)
    shutil.rmtree(target, ignore_errors=True)


def schedule_file(
    path: Union[str, Path],
    *,
    target: str,
    kind: str,
    file: Dict[str, Any],
    publish_at: float,
    now: Optional[float] = None,
    file_id: Optional[str] = None,
) -> ScheduledFile:
    """Queue a file for *publish_at*; returns the stored entry.

    The file descriptor gets the same validation as an immediate
    ``tau send-file`` (1..10 MiB, safe bare name, 64-hex SHA-256), so
    a file that could never be sent is refused here, not on every
    send pass.  The caller stages the bytes first (see
    :func:`stage_file`) under *file_id* when given — otherwise a
    fresh id is generated — so the queue entry and the staged bytes
    agree on the id.
    """
    entry = ScheduledFile(
        id=_clean_id(file_id) if file_id is not None else new_scheduled_file_id(),
        target=_clean_target(target),
        kind=_clean_kind(kind),
        file=_clean_file(file),
        publish_at=_clean_timestamp(publish_at, "publish_at"),
        created_at=now if now is not None else time.time(),
    )
    with scheduled_files_lock(path):
        queue = load_scheduled_files(path)
        save_scheduled_files(path, queue.with_added(entry))
    return entry


def cancel_scheduled_file(
    path: Union[str, Path],
    prefix: str,
    *,
    on_cancel: Optional[Callable[[ScheduledFile], None]] = None,
) -> ScheduledFile:
    """Drop one queued file by full id or unique prefix; returns it.

    When *on_cancel* is given it runs under the queue lock, after the
    entry is resolved and before it is removed — so a send pass
    cannot start the transfer between the callback and the removal.
    The callback is how the CLI deletes the staged bytes for the
    cancelled entry.
    """
    with scheduled_files_lock(path):
        queue = load_scheduled_files(path)
        entry = queue.resolve(prefix)
        if on_cancel is not None:
            on_cancel(entry)
        save_scheduled_files(path, queue.with_removed(entry.id))
    return entry


def publish_due_scheduled_files(
    path: Union[str, Path],
    staged_dir: Union[str, Path],
    send: Callable[[ScheduledFile, Path], None],
    *,
    now: Optional[float] = None,
    should_stop: Optional[Callable[[], bool]] = None,
) -> Dict[str, int]:
    """Send every due queued file through *send* (a ``(entry, path)``
    callable that raises on failure).

    The pass runs off the mail-loop thread (see
    ``TauNode._publish_due_scheduled_files``): a file transfer can
    take minutes, so unlike the scheduled-channel pass this one never
    holds the queue lock across a send.  Each entry is re-checked
    under the lock just before sending, so a cancel that landed
    before the transfer started is honoured; a cancel during the
    transfer itself cannot recall the bytes.

    A failed attempt (exception from *send*, or the staged bytes
    missing) keeps the entry queued with exponential backoff.  After
    ``MAX_SEND_ATTEMPTS`` it is no longer retried automatically but
    stays queued for the user to inspect or cancel.  A pass ends
    early when *should_stop* says so, between entries.

    Returns ``{"published": n, "kept": m}``.  A corrupt queue file logs
    a warning and sends nothing, so one bad file cannot stall the
    mail loop that calls this.
    """
    at = now if now is not None else time.time()
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
        # Re-check under the lock: a cancel since the snapshot means
        # this entry is gone and must not be sent.
        with scheduled_files_lock(path):
            try:
                queue = load_scheduled_files(path)
                queue.resolve(entry.id)
            except ScheduledFileError:
                continue
        try:
            send(entry, staged)
        except Exception as exc:  # noqa: BLE001 - a failed send retries
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
                updated = replace(
                    current,
                    attempts=attempts,
                    next_attempt_at=at + retry_in,
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
        with scheduled_files_lock(path):
            try:
                queue = load_scheduled_files(path)
                save_scheduled_files(path, queue.with_removed(entry.id))
            except ScheduledFileError:
                # Cancelled while the transfer ran: the entry is
                # already gone, which is the same outcome.
                pass
        remove_staged_file(staged_dir, entry.id)
        published += 1
    return {"published": published, "kept": len(candidates) - published}
