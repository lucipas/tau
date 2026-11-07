"""Durable inbox: received direct messages kept in the data directory.

A long-running node prints every DM to its terminal and forgets it; the
inbox is the on-disk record instead.  Both delivery paths — a direct
``/tau/message`` request and a relayed store-and-forward copy — append
here, so ``tau messages`` can read back what arrived even after a
restart.  The web console keeps its own history (``messages.json``);
this store is the node's own, and it exists whether or not ``--http``
is in use.
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

    File records (``kind == "file"``) carry no text: they describe a
    received file by name, size, SHA-256 and where it was saved.
    """
    if not isinstance(raw, dict):
        return None
    if raw.get("kind") == "file":
        return _valid_file_record(raw)
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


def _valid_file_record(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize a received-file record, or ``None`` when unusable."""
    from .protocol import sanitize_file_name

    name = raw.get("name")
    if not isinstance(name, str) or sanitize_file_name(name) != name:
        return None
    size = raw.get("size")
    if not isinstance(size, int) or isinstance(size, bool) or size < 1:
        return None
    sha256 = raw.get("sha256")
    if not isinstance(sha256, str) or len(sha256) != 64:
        return None
    try:
        bytes.fromhex(sha256)
    except ValueError:
        return None
    path = raw.get("path")
    if not isinstance(path, str) or not path:
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
        "kind": "file",
        "from": str(raw.get("from") or "unknown"),
        "destination": str(raw.get("destination") or ""),
        "name": name,
        "size": size,
        "sha256": sha256.lower(),
        "path": path,
        "sent_at": sent_at,
        "received_at": received_at,
        "id": raw.get("id"),
    }


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

    def _save(self) -> None:
        if self.path is None:
            return
        try:
            atomic_write_json(self.path, {"messages": self._messages}, mode=0o600)
        except OSError as exc:
            log.warning("Could not persist inbox to %s: %s", self.path, exc)

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
        how many are returned (``None`` means all of them).
        """
        with self._lock:
            entries = list(self._messages)
        if peer is not None:
            entries = [item for item in entries if item.get("destination") == peer]
        entries.reverse()
        if limit is not None:
            entries = entries[: max(0, limit)]
        return [dict(item) for item in entries]

    def __len__(self) -> int:
        with self._lock:
            return len(self._messages)
