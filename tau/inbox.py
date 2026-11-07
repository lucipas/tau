"""Durable inbox: received direct messages kept in the data directory.

A long-running node prints every DM to its terminal and forgets it; the
inbox is the on-disk record instead.  Both delivery paths — a direct
``/tau/message`` request and a relayed store-and-forward copy — append
here, so ``tau messages`` can read back what arrived even after a
restart.  The web console keeps its own history (``messages.json``);
this store is the node's own, and it exists whether or not ``--http``
is in use.

Read state lives here too, as a per-conversation cursor: for each
sender destination, the ``received_at`` (and id) of the newest message
the owner has read.  A message is unread when it arrived after the
cursor for its sender.  Read state is strictly local — it never leaves
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

from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

INBOX_FILE_NAME = "inbox.json"
MAX_INBOX = 500


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
    return {
        "from": str(raw.get("from") or "unknown"),
        "destination": str(raw.get("destination") or ""),
        "text": text,
        "sent_at": sent_at,
        "received_at": received_at,
        "id": raw.get("id"),
        "relayed": bool(raw.get("relayed", False)),
    }


def _valid_cursors(raw: Any) -> Optional[Dict[str, Dict[str, Any]]]:
    """Normalize stored read cursors, or ``None`` when the file predates them.

    A cursor is ``{"last_read_at": float, "last_read_id": str | None}``
    per sender destination.  Malformed entries are skipped; a missing
    or non-object ``read_cursors`` section means the file was written
    before read state existed, and the caller migrates instead.
    """
    if raw is None or not isinstance(raw, dict):
        return None
    cursors: Dict[str, Dict[str, Any]] = {}
    for destination, value in raw.items():
        if not isinstance(destination, str) or not isinstance(value, dict):
            continue
        try:
            last_read_at = float(value.get("last_read_at"))
        except (TypeError, ValueError):
            continue
        if not math.isfinite(last_read_at):
            continue
        last_read_id = value.get("last_read_id")
        if last_read_id is not None and not isinstance(last_read_id, str):
            continue
        cursors[destination] = {
            "last_read_at": last_read_at,
            "last_read_id": last_read_id,
        }
    return cursors


def _is_unread(entry: Dict[str, Any], cursor: Optional[Dict[str, Any]]) -> bool:
    """Whether *entry* arrived after its sender's read cursor.

    No cursor means nothing from that sender has been read yet.  The
    comparison is on ``received_at`` alone: the cursor always lands on
    the newest message read, so anything at or before it was shown
    alongside it.  The cursor still records the message id, so the
    read position is identifiable.
    """
    if cursor is None:
        return True
    return entry["received_at"] > cursor["last_read_at"]


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
        self._messages = loaded[-MAX_INBOX:]
        cursors = _valid_cursors(raw.get("read_cursors"))
        if cursors is None:
            # No read state on disk: every stored message starts as
            # read, so an upgrade never surfaces old mail as unread.
            cursors = {}
            for entry in self._messages:
                destination = entry.get("destination") or ""
                cursors[destination] = {
                    "last_read_at": entry["received_at"],
                    "last_read_id": entry.get("id"),
                }
        self._read_cursors = cursors

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
        """Record one received message, dropping the oldest past the cap."""
        entry = _valid_record(record)
        if entry is None:
            raise ValueError("inbox record needs text")
        with self._transaction():
            self._messages.append(entry)
            self._messages = self._messages[-MAX_INBOX:]
            self._save()
        return entry

    def list_messages(
        self, *, limit: Optional[int] = None, peer: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Received messages, newest first.

        ``peer`` filters to one sender destination hash; ``limit`` caps
        how many are returned (``None`` means all of them).  Each entry
        carries ``unread``: whether it arrived after its sender's read
        cursor.
        """
        with self._lock:
            entries = list(self._messages)
            cursors = {
                destination: dict(cursor)
                for destination, cursor in self._read_cursors.items()
            }
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
        self, peer: Optional[str] = None, *, up_to: Optional[float] = None
    ) -> Dict[str, Any]:
        """Advance the read cursor for one conversation, or all of them.

        The cursor lands on the newest message for the peer at or
        before ``up_to`` (every message for the peer when ``up_to`` is
        ``None``), so listing what was just shown marks exactly that
        much read.  A peer with no messages is left alone.  Returns the
        unread counts after the mark.
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
                    and (up_to is None or entry["received_at"] <= up_to)
                ]
                if not candidates:
                    continue
                newest = max(
                    candidates, key=lambda entry: entry["received_at"]
                )
                self._read_cursors[destination] = {
                    "last_read_at": newest["received_at"],
                    "last_read_id": newest.get("id"),
                }
            self._save()
            return self._unread_counts_locked()

    def __len__(self) -> int:
        with self._lock:
            return len(self._messages)
