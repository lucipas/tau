"""Message reminders: snooze a stored message and be re-pinged later.

A reminder is a one-shot nudge attached to a stored DM or group
message, set with ``tau remind add <MSG> --in 1h`` (or ``--at`` an ISO
time).  When the node is serving, its mail loop fires due reminders:
the conversation is marked unread again, so it lights the unread
badge, and the reminder is printed with the original message text and
sender — then consumed, never fired twice.

Reminders are local-only by construction — there is no wire format
for them, so a reminder never leaves the node and no peer can learn
one exists.  The text snapshot is taken at set time, so the reminder
still reads even if the message is edited later; a reminder whose
message was deleted before it fires is dropped silently, since there
is nothing left to be reminded about.  The store is capped at 50
reminders, mirroring the draft cap: reminders are local-only, so the
cap is really about a runaway script, not a remote peer.
"""

from __future__ import annotations

import logging
import math
import os
import secrets
import threading
import time
from contextlib import contextmanager, nullcontext
from datetime import datetime
from pathlib import Path
from typing import Any, ContextManager, Dict, Iterator, List, Optional, Union

try:  # POSIX only; Windows locks through msvcrt instead (see below).
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None  # type: ignore[assignment]

try:  # Windows only; the lock uses msvcrt there, as identity.py does.
    import msvcrt
except ImportError:  # pragma: no cover - POSIX platforms
    msvcrt = None  # type: ignore[assignment]

from .protocol import ProtocolError, normalize_message
from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

# Local reminders are capped like drafts: a peer cannot make this
# node hold unbounded state through any wire path (reminders are
# local-only, so the cap is really about a runaway script), and
# `tau remind cancel` frees capacity when the cap is reached.
MAX_REMINDERS = 50

# A reminder cannot outlive the store-and-forward envelope that might
# carry a reply to it, mirroring the 30-day mail TTL other delayed
# work is capped by.
MAX_REMINDER_DELAY = 30 * 24 * 60 * 60

REMINDER_ID_BYTES = 8

REMINDER_KINDS = ("peer", "group")


class ReminderError(ValueError):
    """A reminder request that cannot be stored or found."""


def _valid_hash(value: Any) -> Optional[str]:
    """A 32-hex destination hash (or group id), lowercased, or ``None``."""
    if not isinstance(value, str) or len(value) != 32:
        return None
    try:
        bytes.fromhex(value)
    except ValueError:
        return None
    return value.lower()


def _valid_reminder(raw: Any) -> Optional[Dict[str, Any]]:
    """Normalize one stored reminder, or ``None`` when it is unusable."""
    if not isinstance(raw, dict):
        return None
    kind = raw.get("kind")
    if kind not in REMINDER_KINDS:
        return None
    target = _valid_hash(raw.get("target"))
    if target is None:
        return None
    message_id = raw.get("message_id")
    if not isinstance(message_id, str) or not message_id.strip():
        return None
    reminder_id = raw.get("id")
    if not isinstance(reminder_id, str) or not reminder_id.strip():
        return None
    try:
        text = normalize_message(raw.get("text"))
    except ProtocolError:
        return None
    sender = raw.get("sender")
    if not isinstance(sender, str) or not sender:
        sender = "unknown"
    try:
        due_at = float(raw.get("due_at"))
        created_at = float(raw.get("created_at") or 0.0)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(due_at) or not math.isfinite(created_at):
        return None
    return {
        "id": reminder_id.strip(),
        "message_id": message_id.strip(),
        "kind": kind,
        "target": target,
        "text": text,
        "sender": sender,
        "due_at": due_at,
        "created_at": created_at,
    }


def parse_remind_at(value: str, *, now: Optional[float] = None) -> float:
    """Parse a ``--at`` ISO 8601 time into an epoch due time.

    Accepts a trailing ``Z`` and a bare date (midnight); a naive time
    is read as local time, like the rest of tau's human-facing times.
    Raises :class:`ValueError` for anything unparseable, in the past,
    or further out than :data:`MAX_REMINDER_DELAY`.
    """
    text = (value or "").strip()
    if not text:
        raise ValueError("--at needs an ISO 8601 time like 2026-11-21T09:00")
    candidate = text[:-1] + "+00:00" if text[-1] in ("Z", "z") else text
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(
            f"invalid --at time {value!r} (try an ISO 8601 time like 2026-11-21T09:00)"
        ) from exc
    due = parsed.timestamp()
    moment = time.time() if now is None else now
    if due <= moment:
        raise ValueError("--at must be in the future")
    if due - moment > MAX_REMINDER_DELAY:
        raise ValueError("--at must be within 30d")
    return due


class ReminderStore:
    """One-shot message reminders, persisted in the data dir.

    A node without a data directory (tests, embeds) keeps reminders
    in memory only.  Writes are atomic and the file is mode 0600,
    like the peer registry and mailbox; a failed write only logs a
    warning.
    """

    def __init__(self, path: Optional[Union[str, Path]] = None) -> None:
        self.path = Path(path).expanduser() if path is not None else None
        self._lock = threading.Lock()
        # Keyed by reminder id: several reminders may target one
        # conversation (different messages, different times).
        self._reminders: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if self.path is None:
            return
        raw = read_json(self.path, default=None)
        if not isinstance(raw, dict):
            return
        entries = raw.get("reminders")
        if not isinstance(entries, list):
            return
        loaded: Dict[str, Dict[str, Any]] = {}
        for entry in entries:
            reminder = _valid_reminder(entry)
            if reminder is not None:
                loaded[reminder["id"]] = reminder
        self._reminders = loaded

    def _save(self) -> None:
        """Persist the store; raises :class:`OSError` when the write fails."""
        if self.path is None:
            return
        atomic_write_json(
            self.path,
            {"reminders": list(self._reminders.values())},
            mode=0o600,
        )

    def _snapshot(self) -> Dict[str, Dict[str, Any]]:
        """In-memory state before a mutation, for rollback on a failed save."""
        return dict(self._reminders)

    def _restore(self, snapshot: Dict[str, Dict[str, Any]]) -> None:
        """Restore the state captured by :meth:`_snapshot`."""
        self._reminders = dict(snapshot)

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        """Serialise a read-modify-write of the store across processes.

        Mirrors :meth:`DraftStore._transaction`: each mutation takes
        an exclusive lock on a sidecar lock file and reloads the
        latest on-disk state first, so a serving process and a CLI
        command cannot interleave a load and a save.
        """
        with self._lock:
            if self.path is None:
                yield
                return
            if fcntl is None and msvcrt is None:
                # pragma: no cover - neither platform offers a
                # cross-process lock; best effort only.
                self._load()
                yield
                return
            lock_path = self.path.with_name(self.path.name + ".lock")
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            with open(lock_path, "a+") as handle:
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                else:  # pragma: no cover - exercised on Windows
                    handle.seek(0, os.SEEK_END)
                    if handle.tell() == 0:
                        handle.write(" ")
                        handle.flush()
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                try:
                    self._load()
                    yield
                finally:
                    if fcntl is not None:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    else:  # pragma: no cover - exercised on Windows
                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)

    def add(
        self,
        *,
        message_id: str,
        kind: str,
        target: str,
        text: str,
        sender: str,
        due_at: float,
        now: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Store a reminder for one message.

        Raises :class:`ReminderError` when the kind or target is
        invalid, the text is empty or over the send limit, the due
        time is not in the future (or further out than
        :data:`MAX_REMINDER_DELAY`), or the reminder cap is reached,
        and :class:`OSError` when the write fails (the in-memory
        store is left as it was on disk).
        """
        if kind not in REMINDER_KINDS:
            raise ReminderError(f"unknown reminder target kind: {kind!r}")
        key_target = _valid_hash(target)
        if key_target is None:
            raise ReminderError("reminder target must be a destination hash")
        if not isinstance(message_id, str) or not message_id.strip():
            raise ReminderError("reminder needs a message id")
        try:
            cleaned = normalize_message(text)
        except ProtocolError as exc:
            raise ReminderError(str(exc)) from exc
        moment = time.time() if now is None else now
        try:
            due = float(due_at)
        except (TypeError, ValueError) as exc:
            raise ReminderError("reminder due time must be a number") from exc
        if not math.isfinite(due) or due <= moment:
            raise ReminderError("reminder due time must be in the future")
        if due - moment > MAX_REMINDER_DELAY:
            raise ReminderError("reminder must be within 30d")
        sender_label = sender if isinstance(sender, str) and sender else "unknown"
        reminder = {
            "id": secrets.token_hex(REMINDER_ID_BYTES),
            "message_id": message_id.strip(),
            "kind": kind,
            "target": key_target,
            "text": cleaned,
            "sender": sender_label,
            "due_at": due,
            "created_at": moment,
        }
        with self._transaction():
            if len(self._reminders) >= MAX_REMINDERS:
                raise ReminderError(
                    f"reminder limit reached ({MAX_REMINDERS} reminders;"
                    " cancel one with `tau remind cancel`)"
                )
            snapshot = self._snapshot()
            self._reminders[reminder["id"]] = reminder
            try:
                self._save()
            except OSError:
                self._restore(snapshot)
                raise
        return dict(reminder)

    def cancel(self, ref: str) -> Dict[str, Any]:
        """Cancel a reminder by full id or unique prefix.

        Raises :class:`ReminderError` when no reminder matches or
        the prefix is ambiguous, and :class:`OSError` when the write
        fails (the in-memory store is left as it was on disk).
        """
        if not isinstance(ref, str) or not ref.strip():
            raise ReminderError("reminder reference must not be empty")
        value = ref.strip()
        with self._transaction():
            if value in self._reminders:
                removed = self._reminders[value]
            else:
                matches = [
                    entry
                    for entry in self._reminders.values()
                    if entry["id"].startswith(value)
                ]
                if not matches:
                    raise ReminderError(f"unknown reminder: {ref!r}")
                if len(matches) > 1:
                    raise ReminderError(
                        f"{len(matches)} reminders match prefix {ref!r}"
                    )
                removed = matches[0]
            snapshot = self._snapshot()
            del self._reminders[removed["id"]]
            try:
                self._save()
            except OSError:
                self._restore(snapshot)
                raise
        return dict(removed)

    def list_reminders(self) -> List[Dict[str, Any]]:
        """All reminders, soonest first.

        Reloads from disk first, so reminders saved by another
        process (for example ``tau remind add`` while ``tau serve``
        runs) are seen.
        """
        with self._transaction():
            return sorted(
                (dict(entry) for entry in self._reminders.values()),
                key=lambda entry: entry["due_at"],
            )

    def due(self, now: Optional[float] = None) -> List[Dict[str, Any]]:
        """Reminders whose due time has come, soonest first."""
        moment = time.time() if now is None else now
        return [
            entry for entry in self.list_reminders() if entry["due_at"] <= moment
        ]

    def __len__(self) -> int:
        with self._lock:
            return len(self._reminders)


def _find_stored(inbox: Any, reminder: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The stored inbox record the reminded message is, or ``None`` if gone.

    A group send stores one leg id as the record id and the rest in
    ``leg_ids``, so a reminder naming any leg still matches — the
    same convention as :meth:`Inbox.resolve`.  Callers must hold the
    inbox transaction, so the answer reflects the shared inbox file
    rather than a stale in-memory copy.
    """
    message_id = reminder["message_id"]
    if reminder["kind"] == "group":
        entries = inbox.list_messages(
            limit=None, group=[reminder["target"]], direction=None
        )
    else:
        entries = inbox.list_messages(
            limit=None, peer=reminder["target"], direction=None
        )
    for entry in entries:
        if entry.get("id") == message_id:
            return entry
        legs = entry.get("leg_ids")
        if isinstance(legs, list) and message_id in legs:
            return entry
    return None


def _inbox_transaction(inbox: Any) -> ContextManager[Any]:
    """The inbox's cross-process transaction, or a no-op for plain stand-ins."""
    transaction = getattr(inbox, "transaction", None)
    if callable(transaction):
        return transaction()
    return nullcontext()


def fire_due_reminders(
    path: Union[str, Path],
    inbox: Any,
    *,
    now: Optional[float] = None,
    blocklist_path: Optional[Union[str, Path]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Fire every due reminder through an inbox.

    For each due reminder whose message is still stored, the
    conversation is marked unread again (so it lights the unread
    badge), the reminder is consumed, and then it is printed with the
    full original message text and sender.  The consume is an exclusive
    claim on the reminder store: when two passes or a ``tau remind
    cancel`` race for the same reminder, only the one whose removal
    succeeds prints it, and a failed removal means it is not fired.
    A reminder whose message was deleted first, or whose sender is
    blocked now, is dropped silently — nothing is shown for it.  When
    ``blocklist_path`` is given and the block-list cannot be read, the
    reminder stays due (fail closed).  A failure on one reminder never
    blocks the others, and this never raises into the mail loop: a bad
    reminder file is treated as nothing-due, with a warning, so one
    corrupt file cannot stall mail.
    """
    from .blocklist import load_blocked_set

    moment = time.time() if now is None else now
    try:
        store = ReminderStore(path)
    except Exception:  # pragma: no cover - defensive; store logs
        log.warning("Reminders could not be loaded; skipping this pass")
        return {"fired": [], "dropped": []}
    fired: List[Dict[str, Any]] = []
    dropped: List[Dict[str, Any]] = []
    for reminder in store.due(moment):
        try:
            blocked: Any = frozenset()
            if blocklist_path is not None:
                blocked = load_blocked_set(blocklist_path)
                if blocked is None:
                    # Unreadable block-list: keep the reminder due and
                    # show nothing this pass.
                    continue
            with _inbox_transaction(inbox):
                entry = _find_stored(inbox, reminder)
                if entry is None:
                    outcome = "dropped"
                elif entry.get("destination") in blocked:
                    outcome = "dropped"
                else:
                    if reminder["kind"] == "group":
                        counts = inbox.mark_unread(group=reminder["target"])
                    else:
                        counts = inbox.mark_unread(reminder["target"])
                    # No received message to mark unread (for example a
                    # sent-only conversation): nothing to re-ping about.
                    outcome = "fire" if counts is not None else "dropped"
        except Exception:  # pragma: no cover - defensive; keep firing
            log.warning("Reminder %s failed to fire", reminder["id"], exc_info=True)
            continue
        try:
            store.cancel(reminder["id"])
        except ReminderError:
            # Already cancelled or claimed by another pass: not ours to show.
            continue
        except Exception:
            log.warning(
                "Reminder %s could not be consumed; it will be retried",
                reminder["id"],
                exc_info=True,
            )
            continue
        if outcome == "dropped":
            dropped.append(reminder)
            continue
        fired.append(reminder)
        first = reminder["text"].splitlines()[0] if reminder["text"] else ""
        preview = first if len(first) <= 120 else first[:117] + "…"
        log.info("Reminder from %s: %s", reminder["sender"], preview)
        print(f"Reminder from {reminder['sender']}:", flush=True)
        for line in reminder["text"].splitlines() or [""]:
            print(f"  {line}", flush=True)
    return {"fired": fired, "dropped": dropped}
