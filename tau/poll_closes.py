"""Scheduled Tan poll closes: close a poll when its deadline arrives.

The counterpart of ``tau tan close-poll`` for polls that should end on
their own: ``tau tan poll ... --close-after 3d`` schedules the close,
and a node serving with ``--social`` appends the signed close entry
when the deadline arrives, freezing the tally and showing the poll as
closed to every peer that holds it — the same close a manual
``tau tan close-poll`` writes, gossiped through the same paths.

The deadline is local scheduling state, not wire state: the close
schedule lives in the data directory (``poll-closes.json``), keyed by
the poll's hash, so a close scheduled on this node fires on this node
even when the poll itself was published to peers that never heard of
the deadline.  A poll scheduled with ``--delay`` registers its close
when the publish pass signs it, so the deadline counts from when the
poll goes out, not from when it was queued.  A manual close before
the deadline wins: the sweep drops a schedule whose poll is already
closed (or deleted, or expired) instead of failing on it.

The store is a plain ``{poll_hash: close_at}`` JSON object, validated
on load the way the other small JSON stores are: a malformed file
raises rather than silently dropping scheduled closes, and the
mail-loop sweep treats a corrupt file as nothing-due (with a warning)
so one bad file cannot stall mail.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

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

POLL_CLOSES_FILE_MODE = 0o600
# A close deadline shares the 7-day horizon of the other Tan
# scheduling caps (``MAX_TAN_POST_EXPIRY``, ``MAX_SCHEDULED_POST_DELAY``):
# a poll cannot be asked to close itself further out than it could be
# scheduled or kept alive.
MAX_POLL_CLOSE_AFTER = 7 * 24 * 60 * 60


class PollClosesError(ValueError):
    """Raised when a poll-close schedule file or value is malformed."""


def _clean_target(value: Any) -> str:
    if not isinstance(value, str):
        raise PollClosesError(f"not a post hash: {value!r}")
    lowered = value.lower()
    if len(lowered) != 64:
        raise PollClosesError(f"not a post hash: {value!r}")
    try:
        bytes.fromhex(lowered)
    except ValueError as exc:
        raise PollClosesError(f"not a post hash: {value!r}") from exc
    return lowered


def _clean_close_at(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PollClosesError(f"not a close time: {value!r}")
    stamp = float(value)
    if stamp <= 0 or not math.isfinite(stamp):
        raise PollClosesError(f"not a close time: {value!r}")
    return stamp


def load_poll_closes(path: Union[str, Path]) -> Dict[str, float]:
    """Read the close schedule: ``{poll_hash: close_at}`` ({} when absent)."""
    target = Path(path).expanduser()
    try:
        with target.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as exc:
        raise PollClosesError(
            f"poll closes at {target} are unusable ({exc})"
        ) from exc
    if not isinstance(data, dict):
        raise PollClosesError("poll closes must be a JSON object")
    cleaned: Dict[str, float] = {}
    for raw_target, raw_close_at in data.items():
        cleaned[_clean_target(raw_target)] = _clean_close_at(raw_close_at)
    return cleaned


def save_poll_closes(
    path: Union[str, Path], closes: Dict[str, float]
) -> Path:
    """Persist *closes* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    return atomic_write_json(target, closes, mode=POLL_CLOSES_FILE_MODE)


@contextmanager
def poll_closes_lock(path: Union[str, Path]) -> Iterator[None]:
    """Serialize read-modify-write updates of the poll-close schedule.

    Same shape as the scheduled-posts lock: a flock on a
    ``.poll-closes.json.lock`` sidecar.  An atomic file replacement
    alone does not protect the whole update — without the lock, two
    commands racing load the same file and the last save discards the
    other's change.  Callers must reload inside the lock.
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


def schedule_poll_close(
    path: Union[str, Path],
    target: str,
    close_at: float,
    *,
    now: Optional[float] = None,
) -> float:
    """Schedule the close of poll *target* at *close_at*; returns it.

    One schedule per poll: scheduling the same poll again replaces
    its deadline.  A deadline that has already passed is refused — a
    close due now is a manual close, not a schedule.
    """
    cleaned_target = _clean_target(target)
    cleaned_close_at = _clean_close_at(close_at)
    at = now if now is not None else time.time()
    if cleaned_close_at <= at:
        raise PollClosesError("a poll close must be scheduled in the future")
    with poll_closes_lock(path):
        closes = load_poll_closes(path)
        closes[cleaned_target] = cleaned_close_at
        save_poll_closes(path, closes)
    return cleaned_close_at


def drop_poll_close(path: Union[str, Path], target: str) -> bool:
    """Drop the scheduled close for *target*; True when one existed."""
    cleaned_target = _clean_target(target)
    with poll_closes_lock(path):
        closes = load_poll_closes(path)
        if cleaned_target not in closes:
            return False
        del closes[cleaned_target]
        save_poll_closes(path, closes)
    return True


def due_poll_closes(
    path: Union[str, Path], now: Optional[float] = None
) -> List[Tuple[str, float]]:
    """Scheduled closes whose deadline has come, soonest first."""
    at = now if now is not None else time.time()
    closes = load_poll_closes(path)
    due = [(target, close_at) for target, close_at in closes.items() if close_at <= at]
    due.sort(key=lambda item: item[1])
    return due


def close_due_polls(
    path: Union[str, Path],
    app: Any,
    *,
    now: Optional[float] = None,
) -> Dict[str, int]:
    """Close every poll whose scheduled deadline has come, via *app*.

    Each close goes through ``app.close_poll(target, publish=True)`` —
    the same path a manual ``tau tan close-poll`` uses, so the
    in-lock recheck still decides a race with a manual close and the
    close entry gossips to every peer that holds the poll.  A close
    that fails because the poll is already closed, deleted, expired
    or unknown is terminal: the schedule is dropped, not retried.  Any
    other failure keeps the schedule for the next pass.  A corrupt
    schedule file logs a warning and closes nothing, so one bad file
    cannot stall the mail loop that calls this.

    Returns ``{\"closed\": n, \"dropped\": m, \"kept\": k}``.
    """
    # Lazy import: this module stays importable without Reticulum, the
    # same reason scheduled_posts imports its Tan helpers lazily.
    from .tangents.tan import TanError

    try:
        due = due_poll_closes(path, now=now)
    except PollClosesError as exc:
        log.warning("Poll closes at %s are unusable (%s); skipping", path, exc)
        return {"closed": 0, "dropped": 0, "kept": 0}
    closed = 0
    dropped = 0
    kept = 0
    for target, _close_at in due:
        try:
            app.close_poll(target, publish=True)
        except TanError as exc:
            # Terminal: the poll is already closed, deleted, expired
            # or unknown — nothing a later pass could close.
            log.info("Dropping scheduled close for %s: %s", target, exc)
            drop_poll_close(path, target)
            dropped += 1
        except Exception:  # noqa: BLE001 - a failed close retries
            log.warning(
                "Scheduled close for %s failed; keeping it queued",
                target,
                exc_info=True,
            )
            kept += 1
        else:
            drop_poll_close(path, target)
            closed += 1
    return {"closed": closed, "dropped": dropped, "kept": kept}
