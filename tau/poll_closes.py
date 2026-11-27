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

A close that reaches no peer is not retried on every pass at a
fixed cadence: each failure backs off — 30s, then 60s, 120s, ...
doubling up to 30 minutes — and the close is skipped by
``due_poll_closes`` until its next attempt time.  The backoff is
in-memory, keyed by poll hash, so the schedule file keeps its
``{poll_hash: close_at}`` shape; a restart simply retries at the
base delay again.  Backoff only spaces retries: the close keeps its
original deadline, so it still fires at the time it was scheduled
for, and a close that lands or is dropped clears its backoff.
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
# A close pass shares the 20-second budget of the scheduled-post
# publish pass (``PUBLISH_PASS_BUDGET_SECONDS``): one mail-loop pass
# must never wait out an unbounded sweep of blocking sends.
CLOSE_PASS_BUDGET_SECONDS = 20.0
# A close deadline shares the 7-day horizon of the other Tan
# scheduling caps (``MAX_TAN_POST_EXPIRY``, ``MAX_SCHEDULED_POST_DELAY``):
# a poll cannot be asked to close itself further out than it could be
# scheduled or kept alive.
MAX_POLL_CLOSE_AFTER = 7 * 24 * 60 * 60
# A close that reaches no peer retries with exponential backoff:
# 30s, then 60s, 120s, ... up to 30 minutes.  The cap is a spacing,
# not a give-up: the close keeps retrying at the capped delay until
# it lands or is dropped, because a close that stops retrying would
# leave the poll open past its deadline.
CLOSE_RETRY_BASE_DELAY_SECONDS = 30.0
CLOSE_RETRY_MAX_DELAY_SECONDS = 30 * 60.0

# In-memory retry backoff, keyed by (schedule file, poll hash):
# ``(attempts, next_attempt_at)`` in wall-clock seconds.  Keyed by
# file as well as hash so two data directories never share backoff,
# and so a test's tmp dir cannot leak a delay into another test.
_CLOSE_BACKOFF: Dict[Tuple[str, str], Tuple[int, float]] = {}


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


def _backoff_key(path: Union[str, Path], target: str) -> Tuple[str, str]:
    return (str(Path(path).expanduser()), _clean_target(target))


def _record_close_failure(
    path: Union[str, Path], target: str, now: float
) -> float:
    """Note a failed close attempt; return its next attempt time.

    The delay doubles from ``CLOSE_RETRY_BASE_DELAY_SECONDS`` on
    each consecutive failure, up to
    ``CLOSE_RETRY_MAX_DELAY_SECONDS``.
    """
    key = _backoff_key(path, target)
    attempts, _ = _CLOSE_BACKOFF.get(key, (0, 0.0))
    attempts += 1
    delay = min(
        CLOSE_RETRY_BASE_DELAY_SECONDS * (2 ** (attempts - 1)),
        CLOSE_RETRY_MAX_DELAY_SECONDS,
    )
    next_attempt_at = now + delay
    _CLOSE_BACKOFF[key] = (attempts, next_attempt_at)
    return next_attempt_at


def _clear_close_backoff(path: Union[str, Path], target: str) -> None:
    """Forget any retry backoff for *target* (it landed or is gone)."""
    _CLOSE_BACKOFF.pop(_backoff_key(path, target), None)


def _close_backoff_until(
    path: Union[str, Path], target: str
) -> Optional[float]:
    """When *target* may next be retried, or None when not backing off."""
    entry = _CLOSE_BACKOFF.get(_backoff_key(path, target))
    return entry[1] if entry is not None else None


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
    allow_due: bool = False,
) -> float:
    """Schedule the close of poll *target* at *close_at*; returns it.

    One schedule per poll: scheduling the same poll again replaces
    its deadline.  A deadline that has already passed is refused — a
    close due now is a manual close, not a schedule — unless
    *allow_due* is set, which the publish paths use for a close whose
    deadline came due before it could be registered (or whose stored
    close still needs re-sending): the sweep then closes it.  A fresh
    schedule clears any retry backoff from an earlier one.
    """
    cleaned_target = _clean_target(target)
    cleaned_close_at = _clean_close_at(close_at)
    at = now if now is not None else time.time()
    if cleaned_close_at <= at and not allow_due:
        raise PollClosesError("a poll close must be scheduled in the future")
    with poll_closes_lock(path):
        closes = load_poll_closes(path)
        closes[cleaned_target] = cleaned_close_at
        save_poll_closes(path, closes)
    _clear_close_backoff(path, cleaned_target)
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
    _clear_close_backoff(path, cleaned_target)
    return True


def due_poll_closes(
    path: Union[str, Path], now: Optional[float] = None
) -> List[Tuple[str, float]]:
    """Scheduled closes whose deadline has come, soonest first.

    A close that failed to reach a peer is skipped until its retry
    backoff has elapsed (see ``_record_close_failure``): it stays
    scheduled, it is just not due yet.
    """
    at = now if now is not None else time.time()
    closes = load_poll_closes(path)
    due: List[Tuple[str, float]] = []
    for target, close_at in closes.items():
        if close_at > at:
            continue
        retry_at = _close_backoff_until(path, target)
        if retry_at is not None and retry_at > at:
            continue
        due.append((target, close_at))
    due.sort(key=lambda item: item[1])
    return due


def _close_record(
    app: Any, target: str, close_at: float
) -> Optional[Dict[str, Any]]:
    """The signed close entry for *target*, created or re-found.

    A pass that already wrote the close but reached no peer re-sends
    the stored entry: creating a second close would raise (a poll is
    closed at most once) and wrongly drop the schedule as terminal.
    A close created before the deadline is not the sweep's — it is a
    manual close, so ``None`` is returned and the caller drops the
    schedule instead of republishing it.  The sweep's own closes are
    always created at or after the deadline they fire on; a manual
    close made after the deadline is indistinguishable from the
    sweep's and is re-sent once.  A record without a creation time is
    treated as the sweep's (every close this code writes is stamped).
    Apps without ``get_poll_close`` (older fakes) go straight to
    creating, as before.
    """
    existing = getattr(app, "get_poll_close", None)
    if existing is not None:
        record = existing(target)
        if record is not None:
            created_at = record.get("created_at")
            if (
                isinstance(created_at, (int, float))
                and not isinstance(created_at, bool)
                and float(created_at) < close_at
            ):
                return None
            return record
    return app.close_poll(target, publish=False)


def _publish_close(
    app: Any,
    record: Dict[str, Any],
    *,
    should_stop: Optional[Any] = None,
    deadline: Optional[float] = None,
) -> str:
    """Publish a signed close; ``"closed"``, ``"kept"`` or ``"paused"``.

    The close is sent with the gossip fanout, the pass deadline and
    the stop callback, so one slow peer cannot stall the mail loop.
    ``"closed"`` when at least one peer took it — gossip and feed
    sync carry it to the rest — or when there is no node to publish
    from (the close is stored locally, as a manual ``close_poll``
    with no node does).  ``"kept"`` when no peer took it: the
    schedule stays for a later pass, which re-sends the same signed
    record.  ``"paused"`` when the pass budget or stop ended the send
    mid-way: the pass stops, and the unattempted peers stay queued
    with the schedule.
    """
    if getattr(app, "node", None) is None or not hasattr(app, "publish"):
        return "closed"
    result = app.publish(
        record,
        fanout=getattr(app, "gossip_fanout", None),
        should_stop=should_stop,
        deadline=deadline,
    )
    if not isinstance(result, dict):
        return "closed"
    if result.get("unsent"):
        return "paused"
    peers = result.get("peers") or 0
    delivered = result.get("delivered") or 0
    if peers > 0 and delivered == 0:
        return "kept"
    return "closed"


def close_due_polls(
    path: Union[str, Path],
    app: Any,
    *,
    now: Optional[float] = None,
    should_stop: Optional[Any] = None,
    deadline: Optional[float] = None,
) -> Dict[str, int]:
    """Close every poll whose scheduled deadline has come, via *app*.

    Each close is created through ``app.close_poll(target,
    publish=False)`` — the same path a manual ``tau tan close-poll``
    uses, so the in-lock recheck still decides a race with a manual
    close — and then published with the gossip fanout, the pass
    deadline and the stop callback.  A close the sweep already wrote
    but could not deliver is re-sent from storage instead of created
    again; a close created before the deadline is a manual close, and
    its schedule is dropped rather than republished.  The pass is
    bounded: it stops starting new closes once *deadline* (a
    ``time.monotonic`` value, defaulting to
    ``CLOSE_PASS_BUDGET_SECONDS`` from now) passes or *should_stop*
    (the node's stop event) is set, leaving the rest due for a later
    pass.  A close that fails because the poll is already closed,
    deleted, expired or unknown is terminal: the schedule is dropped,
    not retried.  A close that reached no peer keeps its schedule —
    the next pass re-sends the same signed close record instead of
    creating a second one — but not on the very next pass: each
    failed attempt backs off (30s, 60s, 120s, ... up to 30 minutes),
    and the close is skipped until its next attempt time.  Any other
    failure keeps the schedule for the next pass, with the same
    backoff.  A close cut short by the pass budget or stop is not a
    failure: it carries no backoff, so the next pass retries it
    straight away.  A corrupt schedule file logs a warning and closes
    nothing, so one bad file cannot stall the mail loop that calls
    this.

    Returns ``{"closed": n, "dropped": m, "kept": k}``.
    """
    # Lazy import: this module stays importable without Reticulum, the
    # same reason scheduled_posts imports its Tan helpers lazily.
    from .tangents.tan import TanError

    at = now if now is not None else time.time()
    try:
        due = due_poll_closes(path, now=at)
    except PollClosesError as exc:
        log.warning("Poll closes at %s are unusable (%s); skipping", path, exc)
        return {"closed": 0, "dropped": 0, "kept": 0}
    if deadline is None:
        deadline = time.monotonic() + CLOSE_PASS_BUDGET_SECONDS
    closed = 0
    dropped = 0
    kept = 0
    for index, (target, close_at) in enumerate(due):
        if should_stop is not None and should_stop():
            kept += len(due) - index
            break
        if time.monotonic() > deadline:
            kept += len(due) - index
            break
        try:
            record = _close_record(app, target, close_at)
        except TanError as exc:
            # Terminal: the poll is already closed, deleted, expired
            # or unknown — nothing a later pass could close.
            log.info("Dropping scheduled close for %s: %s", target, exc)
            drop_poll_close(path, target)
            dropped += 1
            continue
        except Exception:  # noqa: BLE001 - a failed close retries
            log.warning(
                "Scheduled close for %s failed; keeping it queued",
                target,
                exc_info=True,
            )
            _record_close_failure(path, target, time.time())
            kept += 1
            continue
        if record is None:
            # A manual close made before the deadline: the poll is
            # already closed — drop the schedule, do not republish it.
            log.info(
                "Dropping scheduled close for %s: poll is already closed",
                target,
            )
            drop_poll_close(path, target)
            dropped += 1
            continue
        try:
            outcome = _publish_close(
                app, record, should_stop=should_stop, deadline=deadline
            )
        except Exception:  # noqa: BLE001 - a failed publish retries
            log.warning(
                "Scheduled close for %s failed to publish; keeping it queued",
                target,
                exc_info=True,
            )
            _record_close_failure(path, target, time.time())
            kept += 1
            continue
        if outcome == "paused":
            # The send stopped mid-way (stop or budget): end the pass
            # here, with this close and the rest still scheduled.
            kept += len(due) - index
            break
        if outcome == "kept":
            log.warning(
                "Scheduled close for %s reached no peer; keeping it queued",
                target,
            )
            _record_close_failure(path, target, time.time())
            kept += 1
            continue
        drop_poll_close(path, target)
        closed += 1
    return {"closed": closed, "dropped": dropped, "kept": kept}
