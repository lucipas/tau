"""Scheduled group poll closes: close a group poll when its deadline arrives.

The counterpart of ``tau group close-poll`` for polls that should end
on their own: ``tau group poll ... --close-after 3d`` schedules the
close, and a serving node sends the author's close record to the group
when the deadline arrives, freezing the tally and showing the poll as
closed to every member — the same close a manual
``tau group close-poll`` sends, fanned out through the same group
send path (offline members get it queued in the outbox).

The deadline is local scheduling state, not wire state: the close
schedule lives in the data directory (``group-poll-closes.json``),
keyed by the poll's logical identity — the group id, the author's
destination hash, and the send time every leg of the poll shares — so
a close scheduled on this node fires on this node even when the poll
itself was posted to members that never heard of the deadline.  A
manual close before the deadline wins: the sweep drops a schedule
whose poll is already closed (or gone) instead of sending a second
close.  A duplicate close is harmless in any case — the tally takes
the earliest close from the poll's author — so the sweep needs no
lock against a manual close racing it; it re-reads the tally right
before sending.

The store is a plain JSON object, validated on load the way the
other small JSON stores are: a malformed file raises rather than
silently dropping scheduled closes, and the mail-loop sweep treats a
corrupt file as nothing-due (with a warning) so one bad file cannot
stall mail.  Each entry carries the group, author, send time and
close time; the key is those fields joined, and the sweep reads the
entry, never the key, so a send time survives its round trip exactly.

A close that reaches no member is not retried on every pass at a
fixed cadence: each failure backs off — 30s, then 60s, 120s, ...
doubling up to 30 minutes — and the close is skipped by
``due_group_poll_closes`` until its next attempt time.  The backoff
is in-memory, keyed by schedule file and poll key, so the schedule
file keeps its shape; a restart simply retries at the base delay
again.  Backoff only spaces retries: the close keeps its original
deadline, so it still fires at the time it was scheduled for, and a
close that lands or is dropped clears its backoff.  The cap is a
spacing, not a give-up: like the Tan closes this mirrors, the close
keeps retrying at the capped delay until it lands or is dropped,
because a close that stops retrying would leave the poll open past
its deadline — and the outbox already tolerates week-long delivery
windows, so a one-day give-up would be incoherent with it.
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

from .group_polls import (
    group_poll_tally,
    is_group_poll,
    is_group_poll_close,
    poll_identity,
)
from .poll_closes import (
    CLOSE_PASS_BUDGET_SECONDS,
    CLOSE_RETRY_BASE_DELAY_SECONDS,
    CLOSE_RETRY_MAX_DELAY_SECONDS,
    MAX_POLL_CLOSE_AFTER,
)
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

GROUP_POLL_CLOSES_FILE_MODE = 0o600
# A close leg to one member shares the sweep's short budget: a leg
# that does not answer fast is queued in the outbox instead of
# holding the mail loop, and the outbox delivers it on retry — a
# queued close is as good as a delivered one.
GROUP_CLOSE_LEG_TIMEOUT_SECONDS = 5.0

# In-memory retry backoff, keyed by (schedule file, poll key):
# ``(attempts, next_attempt_at)`` in wall-clock seconds.  Keyed by
# file as well as key so two data directories never share backoff,
# and so a test's tmp dir cannot leak a delay into another test.
_CLOSE_BACKOFF: Dict[Tuple[str, str], Tuple[int, float]] = {}

# A leg that failed because the member explicitly refused the close
# (or it expired) can never land: retrying it would fail the same
# way, so the sweep drops the schedule instead of backing off.
_TERMINAL_LEG_ERRORS = {"not_allowed", "not_a_member"}


class GroupPollClosesError(ValueError):
    """Raised when a group-poll-close schedule file or value is malformed."""


def _clean_hex32(value: Any, what: str) -> str:
    if not isinstance(value, str):
        raise GroupPollClosesError(f"not a {what}: {value!r}")
    lowered = value.lower()
    if len(lowered) != 32:
        raise GroupPollClosesError(f"not a {what}: {value!r}")
    try:
        bytes.fromhex(lowered)
    except ValueError as exc:
        raise GroupPollClosesError(f"not a {what}: {value!r}") from exc
    return lowered


def _clean_time(value: Any, what: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GroupPollClosesError(f"not a {what}: {value!r}")
    stamp = float(value)
    if stamp <= 0 or not math.isfinite(stamp):
        raise GroupPollClosesError(f"not a {what}: {value!r}")
    return stamp


def group_poll_close_key(group: str, author: str, sent_at: float) -> str:
    """The schedule key for one group poll: its logical identity.

    ``repr`` round-trips the send time exactly, so the key names the
    same poll after a save/load cycle; the sweep reads the stored
    entry, never this key, so the format is only a uniqueness
    guarantee.
    """
    return f"{group}:{author}:{sent_at!r}"


def _clean_entry(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise GroupPollClosesError("a group poll close must be an object")
    group = _clean_hex32(value.get("group"), "group id")
    author = _clean_hex32(value.get("author"), "author hash")
    sent_at = _clean_time(value.get("sent_at"), "poll send time")
    close_at = _clean_time(value.get("close_at"), "close time")
    return {
        "group": group,
        "author": author,
        "sent_at": sent_at,
        "close_at": close_at,
    }


def _backoff_key(path: Union[str, Path], key: str) -> Tuple[str, str]:
    return (str(Path(path).expanduser()), key)


def _record_close_failure(
    path: Union[str, Path], key: str, now: float
) -> float:
    """Note a failed close attempt; return its next attempt time.

    The delay doubles from ``CLOSE_RETRY_BASE_DELAY_SECONDS`` on
    each consecutive failure, up to
    ``CLOSE_RETRY_MAX_DELAY_SECONDS``.
    """
    backoff_key = _backoff_key(path, key)
    attempts, _ = _CLOSE_BACKOFF.get(backoff_key, (0, 0.0))
    attempts += 1
    delay = min(
        CLOSE_RETRY_BASE_DELAY_SECONDS * (2 ** (attempts - 1)),
        CLOSE_RETRY_MAX_DELAY_SECONDS,
    )
    next_attempt_at = now + delay
    _CLOSE_BACKOFF[backoff_key] = (attempts, next_attempt_at)
    return next_attempt_at


def _clear_close_backoff(path: Union[str, Path], key: str) -> None:
    """Forget any retry backoff for *key* (it landed or is gone)."""
    _CLOSE_BACKOFF.pop(_backoff_key(path, key), None)


def _close_backoff_until(
    path: Union[str, Path], key: str
) -> Optional[float]:
    """When *key* may next be retried, or None when not backing off."""
    entry = _CLOSE_BACKOFF.get(_backoff_key(path, key))
    return entry[1] if entry is not None else None


def load_group_poll_closes(
    path: Union[str, Path],
) -> Dict[str, Dict[str, Any]]:
    """Read the close schedule: ``{key: entry}`` ({} when absent)."""
    target = Path(path).expanduser()
    try:
        with target.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as exc:
        raise GroupPollClosesError(
            f"group poll closes at {target} are unusable ({exc})"
        ) from exc
    if not isinstance(data, dict):
        raise GroupPollClosesError("group poll closes must be a JSON object")
    cleaned: Dict[str, Dict[str, Any]] = {}
    for raw_key, raw_entry in data.items():
        entry = _clean_entry(raw_entry)
        key = group_poll_close_key(
            entry["group"], entry["author"], entry["sent_at"]
        )
        if not isinstance(raw_key, str) or raw_key != key:
            raise GroupPollClosesError(
                f"group poll close key {raw_key!r} does not match its entry"
            )
        cleaned[key] = entry
    return cleaned


def save_group_poll_closes(
    path: Union[str, Path], closes: Dict[str, Dict[str, Any]]
) -> Path:
    """Persist *closes* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    return atomic_write_json(target, closes, mode=GROUP_POLL_CLOSES_FILE_MODE)


@contextmanager
def group_poll_closes_lock(path: Union[str, Path]) -> Iterator[None]:
    """Serialize read-modify-write updates of the close schedule.

    Same shape as the Tan poll-closes lock: a flock on a
    ``.group-poll-closes.json.lock`` sidecar.  An atomic file
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


def schedule_group_poll_close(
    path: Union[str, Path],
    group: str,
    author: str,
    sent_at: float,
    close_at: float,
    *,
    now: Optional[float] = None,
    allow_due: bool = False,
) -> float:
    """Schedule the close of one group poll at *close_at*; returns it.

    One schedule per poll: scheduling the same poll again replaces
    its deadline.  A deadline that has already passed is refused — a
    close due now is a manual close, not a schedule — unless
    *allow_due* is set, which the publish path uses for a close whose
    deadline came due while the poll was going out: the sweep then
    closes it.  A fresh schedule clears any retry backoff from an
    earlier one.
    """
    entry = _clean_entry(
        {
            "group": group,
            "author": author,
            "sent_at": sent_at,
            "close_at": close_at,
        }
    )
    key = group_poll_close_key(
        entry["group"], entry["author"], entry["sent_at"]
    )
    at = now if now is not None else time.time()
    if entry["close_at"] <= at and not allow_due:
        raise GroupPollClosesError(
            "a group poll close must be scheduled in the future"
        )
    with group_poll_closes_lock(path):
        closes = load_group_poll_closes(path)
        closes[key] = entry
        save_group_poll_closes(path, closes)
    _clear_close_backoff(path, key)
    return entry["close_at"]


def drop_group_poll_close(
    path: Union[str, Path], group: str, author: str, sent_at: float
) -> bool:
    """Drop the scheduled close for one group poll; True when one existed."""
    key = group_poll_close_key(
        _clean_hex32(group, "group id"),
        _clean_hex32(author, "author hash"),
        _clean_time(sent_at, "poll send time"),
    )
    with group_poll_closes_lock(path):
        closes = load_group_poll_closes(path)
        if key not in closes:
            return False
        del closes[key]
        save_group_poll_closes(path, closes)
    _clear_close_backoff(path, key)
    return True


def due_group_poll_closes(
    path: Union[str, Path], now: Optional[float] = None
) -> List[Tuple[str, Dict[str, Any]]]:
    """Scheduled closes whose deadline has come, soonest first.

    A close that failed to reach a member is skipped until its retry
    backoff has elapsed (see ``_record_close_failure``): it stays
    scheduled, it is just not due yet.
    """
    at = now if now is not None else time.time()
    closes = load_group_poll_closes(path)
    due: List[Tuple[str, Dict[str, Any]]] = []
    for key, entry in closes.items():
        if entry["close_at"] > at:
            continue
        retry_at = _close_backoff_until(path, key)
        if retry_at is not None and retry_at > at:
            continue
        due.append((key, entry))
    due.sort(key=lambda item: item[1]["close_at"])
    return due


def _find_poll(
    node: Any, entry: Dict[str, Any]
) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    """This node's stored copy of the scheduled poll, plus its history.

    The poll is the group's stored poll record whose identity is the
    entry's (author, send time): a member's copy would carry a
    different leg id, so the identity — not the id — is the match.
    The history is returned with it so the caller can tally without
    re-reading the inbox.
    """
    history = node.inbox.list_messages(
        limit=None, group=entry["group"], direction=None
    )
    own_hash = node.destination_hexhash
    target = (entry["author"], float(entry["sent_at"]))
    for record in history:
        if not is_group_poll(record):
            continue
        if record.get("group") != entry["group"]:
            continue
        if poll_identity(record, own_hash) != target:
            continue
        return record, history
    return None, history


def _has_close_record(
    history: List[Dict[str, Any]],
    entry: Dict[str, Any],
    own_hash: Optional[str],
) -> bool:
    """True when the poll already has a close on record from its author.

    The close record is what the tally itself reads, so this is the
    same check the tally would make — only without needing the poll
    record, which is what the caller is missing.  A close from anyone
    but the poll's author is not a close.
    """
    for record in history:
        if record.get("group") != entry["group"]:
            continue
        if not is_group_poll_close(record):
            continue
        target = record["poll_close"]
        if target["author"] != entry["author"]:
            continue
        if float(target["sent_at"]) != float(entry["sent_at"]):
            continue
        if record.get("direction") == "out":
            author = own_hash
        else:
            author = record.get("destination")
        if isinstance(author, str) and author.lower() == entry["author"]:
            return True
    return False


def _poll_retracted(node: Any, entry: Dict[str, Any]) -> bool:
    """True when the poll was explicitly retracted.

    A retraction marker is the durable record that the poll is gone
    on purpose — as opposed to its inbox record being evicted by the
    sent cap or deleted locally, which leave no marker.  Both sides
    count: an inbound marker from a member's retract, and the
    *sent* marker this node wrote when it retracted its own outgoing
    poll — without the second, the sweep re-sends a close to members
    who already got the retract.  Nodes without a mailbox (test
    stubs) have no markers to consult.
    """
    mailbox = getattr(node, "mailbox", None)
    if mailbox is None:
        return False
    if mailbox.is_group_retracted(
        entry["author"], entry["group"], entry["sent_at"]
    ):
        return True
    is_sent = getattr(mailbox, "is_sent_group_retracted", None)
    if is_sent is None:
        return False
    return bool(is_sent(entry["group"], entry["sent_at"]))


def _send_close(
    node: Any, entry: Dict[str, Any], *, deadline: Optional[float] = None
) -> str:
    """Send the close to the group; ``"closed"``, ``"dropped"`` or ``"kept"``.

    ``"closed"`` when the close reached at least one member, was
    queued for one (the outbox owns it from there), or there is no
    other member to reach.  ``"dropped"`` when every leg failed
    terminally — the member refused it or it expired — so no retry
    could land it.  ``"kept"`` when a leg failed transiently (the
    outbox could not take it) and nothing went out: the schedule
    stays for a later pass, with backoff.

    *deadline* is the sweep's pass deadline (a ``time.monotonic``
    value): one close must not hold the mail loop past it.  The
    remaining budget is shared across the legs — each leg gets an
    equal slice of what is left, capped at the usual per-leg
    timeout — so a slow member cannot stall the pass, and no leg
    starves a later one of an attempt the way a single fan-out-wide
    deadline did (see
    ``test_send_group_message_slow_first_leg_does_not_starve_later_legs``).
    A leg whose slice is exhausted is queued, never dropped: the
    outbox delivers it on retry.
    """
    group = node.groups.find(entry["group"])
    if group is None:
        # A group change retires the poll's original roster: the
        # predecessor's record still names the poll's recipients.
        group = node.groups.get_retired(entry["group"])
    if group is None:
        raise _UnknownGroup()
    leg_timeout = GROUP_CLOSE_LEG_TIMEOUT_SECONDS
    if deadline is not None:
        others = [
            member
            for member in group.get("members") or []
            if member != node.destination_hexhash
        ]
        if others:
            remaining = deadline - time.monotonic()
            leg_timeout = min(
                leg_timeout, max(remaining, 0.0) / len(others)
            )
    result = node.send_group_message(
        group,
        "closed the poll",
        poll_close={"author": entry["author"], "sent_at": entry["sent_at"]},
        timeout=leg_timeout,
    )
    if not isinstance(result, dict):
        return "closed"
    results = result.get("results") or []
    if not results:
        # Nothing was attempted.  A solo group has no member to close
        # for, so that is done; otherwise keep the schedule and retry.
        others = [
            member
            for member in group.get("members") or []
            if member != node.destination_hexhash
        ]
        return "kept" if others else "closed"
    if any(
        leg.get("delivered") or leg.get("queued") for leg in results
    ):
        return "closed"
    if all(
        leg.get("expired")
        or (leg.get("error") in _TERMINAL_LEG_ERRORS)
        for leg in results
    ):
        return "dropped"
    return "kept"


class _UnknownGroup(Exception):
    """The scheduled poll's group is gone from this node."""


def close_due_group_polls(
    path: Union[str, Path],
    node: Any,
    *,
    now: Optional[float] = None,
    should_stop: Optional[Any] = None,
    deadline: Optional[float] = None,
) -> Dict[str, int]:
    """Close every group poll whose scheduled deadline has come.

    Each close is sent through ``node.send_group_message`` — the same
    path a manual ``tau group close-poll`` uses, so the tally is
    re-read right before sending and a manual close that landed first
    drops the schedule instead of sending a second close.  A poll
    whose group is gone is terminal: the schedule is dropped, not
    retried.  A poll whose stored record is gone still closes: the
    sent inbox evicts it after 500 newer sent records and a local
    delete or prune removes it too, but the schedule entry is the
    durable state that names the poll — only a close already on
    record, or an explicit retract, ends it.  A close that reached
    no member keeps its schedule — the next pass re-sends it — but
    not on the very next pass: each failed attempt backs off (30s,
    60s, 120s, ... up to 30 minutes), and the close is skipped until
    its next attempt time.  The backoff is a spacing, not a give-up:
    the close keeps retrying at the capped delay until it lands or
    is dropped, like the Tan closes this mirrors.
    A close cut short by the pass budget or stop is not a failure:
    it carries no backoff, so the next pass retries it straight away.
    A corrupt schedule file logs a warning and closes nothing, so one
    bad file cannot stall the mail loop that calls this.

    Returns ``{"closed": n, "dropped": m, "kept": k}``.
    """
    # Lazy import: this module stays importable without the node
    # (and its Reticulum import), the same reason poll_closes imports
    # its Tan helpers lazily.
    from .node import NodeError

    at = now if now is not None else time.time()
    try:
        due = due_group_poll_closes(path, now=at)
    except GroupPollClosesError as exc:
        log.warning(
            "Group poll closes at %s are unusable (%s); skipping", path, exc
        )
        return {"closed": 0, "dropped": 0, "kept": 0}
    if deadline is None:
        deadline = time.monotonic() + CLOSE_PASS_BUDGET_SECONDS
    closed = 0
    dropped = 0
    kept = 0
    own_hash = node.destination_hexhash
    for index, (key, entry) in enumerate(due):
        if should_stop is not None and should_stop():
            kept += len(due) - index
            break
        if time.monotonic() > deadline:
            kept += len(due) - index
            break
        try:
            poll, history = _find_poll(node, entry)
        except Exception:  # noqa: BLE001 - a failed lookup retries
            log.warning(
                "Scheduled group poll close for %s failed to resolve; "
                "keeping it queued",
                key,
                exc_info=True,
            )
            # Stamped in the pass's own clock frame (``at``), not the
            # wall clock: ``due_group_poll_closes`` reads backoff
            # against the same ``now``, so a failure stamped in
            # another frame would never become due again under it.
            _record_close_failure(path, key, at)
            kept += 1
            continue
        if entry["author"] != (own_hash or "").lower():
            # Only the poll's creator may close it; a schedule for
            # someone else's poll can never fire from here.
            log.info(
                "Dropping scheduled group poll close for %s: not our poll",
                key,
            )
            drop_group_poll_close(
                path, entry["group"], entry["author"], entry["sent_at"]
            )
            dropped += 1
            continue
        if poll is None:
            # The poll record is gone from this node's history — but
            # that is not the poll being gone.  The sent inbox keeps
            # only the newest 500 sent records, so a busy node evicts
            # its copy long before the deadline; a local delete or
            # prune removes it too.  The schedule entry is the
            # durable state: it names the poll and proves it went out
            # from here.  Only a close already on record, or an
            # explicit retract, ends it — otherwise the close still
            # goes out from the entry alone.
            if _has_close_record(history, entry, own_hash):
                log.info(
                    "Dropping scheduled group poll close for %s: poll is "
                    "already closed",
                    key,
                )
                drop_group_poll_close(
                    path, entry["group"], entry["author"], entry["sent_at"]
                )
                dropped += 1
                continue
            if _poll_retracted(node, entry):
                log.info(
                    "Dropping scheduled group poll close for %s: poll was "
                    "retracted",
                    key,
                )
                drop_group_poll_close(
                    path, entry["group"], entry["author"], entry["sent_at"]
                )
                dropped += 1
                continue
            # No close, no retract: retention or a local delete took
            # the record, not the poll — fall through and send.
        elif group_poll_tally(history, poll, own_hash=own_hash)["closed"]:
            # A manual close made before the deadline: the poll is
            # already closed — drop the schedule, do not re-send it.
            log.info(
                "Dropping scheduled group poll close for %s: poll is "
                "already closed",
                key,
            )
            drop_group_poll_close(
                path, entry["group"], entry["author"], entry["sent_at"]
            )
            dropped += 1
            continue
        try:
            outcome = _send_close(node, entry, deadline=deadline)
        except _UnknownGroup:
            log.info(
                "Dropping scheduled group poll close for %s: group is gone",
                key,
            )
            drop_group_poll_close(
                path, entry["group"], entry["author"], entry["sent_at"]
            )
            dropped += 1
            continue
        except NodeError as exc:
            # Terminal: the group record cannot take a close (no
            # creator, or a send this node refuses) — nothing a later
            # pass could close.
            log.info("Dropping scheduled group poll close for %s: %s", key, exc)
            drop_group_poll_close(
                path, entry["group"], entry["author"], entry["sent_at"]
            )
            dropped += 1
            continue
        except Exception:  # noqa: BLE001 - a failed close retries
            log.warning(
                "Scheduled group poll close for %s failed; keeping it queued",
                key,
                exc_info=True,
            )
            _record_close_failure(path, key, at)
            kept += 1
            continue
        if outcome == "kept":
            log.warning(
                "Scheduled group poll close for %s reached no member; "
                "keeping it queued",
                key,
            )
            _record_close_failure(path, key, at)
            kept += 1
            continue
        drop_group_poll_close(
            path, entry["group"], entry["author"], entry["sent_at"]
        )
        if outcome == "closed":
            closed += 1
        else:
            dropped += 1
    return {"closed": closed, "dropped": dropped, "kept": kept}


__all__ = [
    "CLOSE_PASS_BUDGET_SECONDS",
    "GROUP_CLOSE_LEG_TIMEOUT_SECONDS",
    "MAX_POLL_CLOSE_AFTER",
    "GroupPollClosesError",
    "close_due_group_polls",
    "drop_group_poll_close",
    "due_group_poll_closes",
    "group_poll_close_key",
    "load_group_poll_closes",
    "save_group_poll_closes",
    "schedule_group_poll_close",
]
