"""Durable inbox: received direct messages kept in the data directory.

A long-running node prints every DM to its terminal and forgets it; the
inbox is the on-disk record instead.  Both delivery paths — a direct
``/tau/message`` request and a relayed store-and-forward copy — append
here, so ``tau messages`` can read back what arrived even after a
restart.  The web console keeps its own history (``messages.json``);
this store is the node's own, and it exists whether or not ``--http``
is in use.

Read state lives here too, as a per-conversation cursor: for each
sender destination, the arrival sequence (and id) of the newest message
the owner has read.  A message is unread when it arrived after the
cursor for its sender.  Arrival order comes from the append sequence,
not the wall clock, so a clock change cannot hide new mail.  Read state is strictly local — it never leaves
the node — and files written before cursors existed migrate with every
stored message marked read, so an upgrade does not surface a wall of
"unread" for mail the owner already saw.
"""

from __future__ import annotations

import logging
import math
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

try:  # POSIX only; the cross-process lock is a no-op without it.
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None  # type: ignore[assignment]

from .protocol import clean_in_reply_to
from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

INBOX_FILE_NAME = "inbox.json"
MAX_INBOX = 500
# Sent records have their own cap so outgoing DMs never evict received ones.
MAX_INBOX_SENT = 500


def _retain(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep the newest ``MAX_INBOX`` received and ``MAX_INBOX_SENT`` sent records."""
    limits = {"in": MAX_INBOX, "out": MAX_INBOX_SENT}
    counts = {"in": 0, "out": 0}
    kept: List[Dict[str, Any]] = []
    for record in reversed(messages):
        direction = record.get("direction")
        if counts.get(direction, 0) >= limits.get(direction, MAX_INBOX):
            continue
        counts[direction] = counts.get(direction, 0) + 1
        kept.append(record)
    kept.reverse()
    return kept


def _valid_record(raw: Any) -> Optional[Dict[str, Any]]:
    """Normalize one stored record, or ``None`` when it is unusable.

    A peer can send a non-finite ``sent_at`` (the message parser
    accepts any float), and the message is still delivered — so a bad
    send time is normalized to 0.0 (unknown) instead of dropping the
    record from the inbox.  ``received_at`` is node-generated, so a
    non-finite value there means a corrupt file and the entry is
    skipped instead.
    """
    if not isinstance(raw, dict):
        return None
    text = raw.get("text")
    if not isinstance(text, str) or not text:
        return None
    try:
        sent_at = float(raw.get("sent_at") or 0.0)
    except (TypeError, ValueError):
        return None
    received_at = raw.get("received_at")
    try:
        received_at = float(received_at) if received_at is not None else time.time()
    except (TypeError, ValueError):
        return None
    if not math.isfinite(sent_at):
        sent_at = 0.0
    if not math.isfinite(received_at):
        return None
    direction = raw.get("direction")
    if direction not in ("in", "out"):
        direction = "in"
    try:
        in_reply_to = clean_in_reply_to(raw.get("in_reply_to"))
    except ValueError:
        # A corrupt or hand-edited reference never blocks the inbox;
        # the message itself is still readable, just unquoted.
        in_reply_to = None
    seq = raw.get("seq")
    if isinstance(seq, bool) or not isinstance(seq, int) or seq <= 0:
        seq = None
    record = {
        "from": str(raw.get("from") or "unknown"),
        "destination": str(raw.get("destination") or ""),
        "text": text,
        "sent_at": sent_at,
        "received_at": received_at,
        "id": raw.get("id"),
        "relayed": bool(raw.get("relayed", False)),
        "direction": direction,
        "seq": seq,
    }
    if in_reply_to is not None:
        record["in_reply_to"] = in_reply_to
    return record


def _number_sequence(messages: List[Dict[str, Any]]) -> None:
    """Give every message a strictly increasing arrival sequence, in place.

    The list is already in arrival order.  Records written before
    sequence numbers existed (or with a bad one) are numbered after the
    last valid one, so the order never depends on wall-clock time.
    """
    last = 0
    for entry in messages:
        seq = entry.get("seq")
        if seq is None or seq <= last:
            seq = last + 1
        entry["seq"] = seq
        last = seq


def _valid_cursors(
    raw: Any, messages: List[Dict[str, Any]]
) -> Optional[Dict[str, Dict[str, Any]]]:
    """Normalize stored read cursors, or ``None`` when the file predates them.

    A cursor is ``{"last_read_seq": int, "last_read_id": str | None}``
    per sender destination.  A cursor saved before sequence numbers
    carries only ``last_read_at``; it is resolved to the newest stored
    message it covered.  Malformed entries are skipped; a missing or
    non-object ``read_cursors`` section means the file was written
    before read state existed, and the caller migrates instead.
    """
    if raw is None or not isinstance(raw, dict):
        return None
    cursors: Dict[str, Dict[str, Any]] = {}
    for destination, value in raw.items():
        if not isinstance(destination, str) or not isinstance(value, dict):
            continue
        last_read_id = value.get("last_read_id")
        if last_read_id is not None and not isinstance(last_read_id, str):
            continue
        seq = value.get("last_read_seq")
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            try:
                last_read_at = float(value.get("last_read_at"))
            except (TypeError, ValueError):
                continue
            if not math.isfinite(last_read_at):
                continue
            covered = [
                entry["seq"]
                for entry in messages
                if (entry.get("destination") or "") == destination
                and entry["received_at"] <= last_read_at
            ]
            seq = max(covered, default=0)
        cursors[destination] = {"last_read_seq": seq, "last_read_id": last_read_id}
    return cursors


def _is_unread(entry: Dict[str, Any], cursor: Optional[Dict[str, Any]]) -> bool:
    """Whether *entry* arrived after its sender's read cursor.

    No cursor means nothing from that sender has been read yet.  The
    comparison is on the arrival sequence, not the wall clock, so a
    clock that moves backward cannot hide newly arrived mail.
    """
    if cursor is None:
        return True
    return entry["seq"] > cursor["last_read_seq"]


class Inbox:
    """Received direct messages, persisted in the node data directory.

    A node without a data directory (tests, embeds) keeps messages in
    memory only.  Writes are atomic and the file is mode 0600, like the
    peer registry and mailbox; a failed write only logs a warning.
    """

    def __init__(self, path: Optional[Union[str, Path]] = None) -> None:
        self.path = Path(path).expanduser() if path is not None else None
        self._lock = threading.Lock()
        self._messages: List[Dict[str, Any]] = []
        self._read_cursors: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if self.path is None:
            return
        raw = read_json(self.path, default=None)
        if not isinstance(raw, dict):
            return
        entries = raw.get("messages")
        if not isinstance(entries, list):
            return
        loaded = []
        for entry in entries:
            record = _valid_record(entry)
            if record is not None:
                loaded.append(record)
        self._messages = _retain(loaded)
        _number_sequence(self._messages)
        cursors = _valid_cursors(raw.get("read_cursors"), self._messages)
        if cursors is None:
            # No read state on disk: every stored message starts as
            # read, so an upgrade never surfaces old mail as unread.
            # The arrival sequence is ordered, so the newest message
            # per sender is the greatest sequence, whatever its time.
            cursors = {}
            for entry in self._messages:
                destination = entry.get("destination") or ""
                current = cursors.get(destination)
                if current is None or entry["seq"] > current["last_read_seq"]:
                    cursors[destination] = {
                        "last_read_seq": entry["seq"],
                        "last_read_id": entry.get("id"),
                    }
        self._read_cursors = cursors

    def _next_seq(self) -> int:
        """The sequence number for the next arrival (caller holds the lock)."""
        highest = [entry["seq"] for entry in self._messages]
        highest += [cursor["last_read_seq"] for cursor in self._read_cursors.values()]
        return max(highest, default=0) + 1

    def _save(self) -> None:
        if self.path is None:
            return
        try:
            atomic_write_json(
                self.path,
                {"messages": self._messages, "read_cursors": self._read_cursors},
                mode=0o600,
            )
        except OSError as exc:
            log.warning("Could not persist inbox to %s: %s", self.path, exc)

    def transaction(self) -> Iterator[None]:
        """Public handle on the cross-process inbox lock (see ``_transaction``)."""
        return self._transaction()

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        """Serialise a read-modify-write of the inbox across processes.

        ``tau serve`` and one-shot commands such as ``tau send`` share
        the same ``inbox.json``.  Each append takes an exclusive
        ``flock`` on a sidecar lock file, reloads the latest on-disk
        state, and then applies its change, so a process never
        overwrites another's entries.  Mirrors
        :meth:`Mailbox._transaction`.
        """
        with self._lock:
            if self.path is None or fcntl is None:
                if self.path is not None:
                    self._load()
                yield
                return
            lock_path = self.path.with_name(self.path.name + ".lock")
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            with open(lock_path, "a") as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    self._load()
                    yield
                finally:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def append(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Record one message, dropping the oldest past its direction's cap."""
        entry = _valid_record(record)
        if entry is None:
            raise ValueError("inbox record needs text")
        with self._transaction():
            entry["seq"] = self._next_seq()
            self._messages.append(entry)
            self._messages = _retain(self._messages)
            self._save()
        return entry

    def list_messages(
        self,
        *,
        limit: Optional[int] = None,
        peer: Optional[str] = None,
        direction: str = "in",
    ) -> List[Dict[str, Any]]:
        """Received messages, newest first.

        ``peer`` filters to one sender destination hash; ``limit`` caps
        how many are returned (``None`` means all of them).  Sent
        messages are recorded here too — a reply quotes the sender's
        own earlier message as often as the peer's — but they stay out
        of the default listing, which is the received-message view
        ``tau messages`` has always shown; pass ``direction="out"``
        or ``direction=None`` to see them.  Each entry carries
        ``unread``: whether it arrived after its sender's read cursor.
        """
        with self._lock:
            entries = list(self._messages)
            cursors = {
                destination: dict(cursor)
                for destination, cursor in self._read_cursors.items()
            }
        if direction is not None:
            entries = [item for item in entries if item.get("direction") == direction]
        if peer is not None:
            entries = [item for item in entries if item.get("destination") == peer]
        entries.reverse()
        if limit is not None:
            entries = entries[: max(0, limit)]
        return [
            dict(
                item,
                unread=_is_unread(
                    item, cursors.get(item.get("destination") or "")
                ),
            )
            for item in entries
        ]

    def _unread_counts_locked(self) -> Dict[str, Any]:
        """Per-conversation unread counts from the in-memory state."""
        per_peer: Dict[str, int] = {}
        for entry in self._messages:
            if _is_unread(
                entry, self._read_cursors.get(entry.get("destination") or "")
            ):
                destination = entry.get("destination") or ""
                per_peer[destination] = per_peer.get(destination, 0) + 1
        return {"per_peer": per_peer, "total": sum(per_peer.values())}

    def unread_counts(self) -> Dict[str, Any]:
        """Per-conversation unread counts, reloaded from disk first.

        The reload under the cross-process lock is what lets a serving
        node see a read cursor a one-shot ``tau messages`` just
        advanced (and vice versa).
        """
        with self._transaction():
            return self._unread_counts_locked()

    def mark_read(
        self, peer: Optional[str] = None, *, up_to_seq: Optional[int] = None
    ) -> Dict[str, Any]:
        """Advance the read cursor for one conversation, or all of them.

        The cursor lands on the newest message for the peer with an
        arrival sequence at or before ``up_to_seq`` (every message for
        the peer when ``up_to_seq`` is ``None``), so listing what was
        just shown marks exactly that much read.  A cursor never moves
        backward, so a delayed mark cannot restore read mail as unread.
        A peer with no messages is left alone.  Returns the unread
        counts after the mark.
        """
        with self._transaction():
            if peer is not None:
                targets = [peer]
            else:
                targets = sorted(
                    {entry.get("destination") or "" for entry in self._messages}
                )
            for destination in targets:
                candidates = [
                    entry
                    for entry in self._messages
                    if (entry.get("destination") or "") == destination
                    and (up_to_seq is None or entry["seq"] <= up_to_seq)
                ]
                if not candidates:
                    continue
                newest = max(candidates, key=lambda entry: entry["seq"])
                current = self._read_cursors.get(destination)
                if current is not None and current["last_read_seq"] >= newest["seq"]:
                    continue
                self._read_cursors[destination] = {
                    "last_read_seq": newest["seq"],
                    "last_read_id": newest.get("id"),
                }
            self._save()
            return self._unread_counts_locked()

    def find(self, message_id: str, *, peer: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Newest record with this wire id, optionally in one conversation.

        Quote resolution is scoped to the conversation: a reply
        reference only resolves against a message with the same peer,
        so a reference to another peer's message never renders.
        """
        if not isinstance(message_id, str) or not message_id:
            return None
        with self._lock:
            entries = list(self._messages)
        for item in reversed(entries):
            if item.get("id") != message_id:
                continue
            if peer is not None and item.get("destination") != peer:
                continue
            return dict(item)
        return None

    def find_prefix(
        self, prefix: str, *, peer: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Records whose wire id starts with *prefix*, newest first.

        Used to resolve ``tau send --reply-to``: the prefix must match
        exactly one message in the conversation.
        """
        if not isinstance(prefix, str) or not prefix:
            return []
        value = prefix.strip().lower()
        with self._lock:
            entries = list(self._messages)
        matches = []
        for item in reversed(entries):
            message_id = item.get("id")
            if not isinstance(message_id, str):
                continue
            if not message_id.lower().startswith(value):
                continue
            if peer is not None and item.get("destination") != peer:
                continue
            matches.append(dict(item))
        return matches

    def __len__(self) -> int:
        with self._lock:
            return len(self._messages)
