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
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

try:  # POSIX only; the cross-process lock is a no-op without it.
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None  # type: ignore[assignment]

from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

INBOX_FILE_NAME = "inbox.json"
MAX_INBOX = 500
# Reactions are keyed by (target message, reactor): the latest entry per
# pair wins, so the store stays small, but a hostile peer could still
# spray reactions at made-up targets.  Cap the total and drop the
# oldest first; a reaction whose target is not in the inbox is stored
# but never rendered, so spraying buys the sender nothing visible.
MAX_REACTIONS = 1000


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


def _valid_reaction(raw: Any) -> Optional[Dict[str, Any]]:
    """Normalize one stored reaction, or ``None`` when it is unusable.

    A reaction names the target message id, the reactor's destination
    hash, and the token.  An empty token is a retraction tombstone: it
    IS stored (see :func:`merge_reaction`), so a late older reaction
    cannot resurrect a reaction the reactor already retracted.
    Non-finite timestamps are normalized the same way message records
    are: a bad ``sent_at`` becomes 0.0, a bad ``received_at`` drops the
    entry.
    """
    if not isinstance(raw, dict):
        return None
    target = raw.get("target")
    if not isinstance(target, str) or not target or len(target) > 64:
        return None
    destination = raw.get("destination")
    if not isinstance(destination, str) or not destination:
        return None
    reaction = raw.get("reaction")
    if not isinstance(reaction, str):
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
        "target": target,
        "destination": destination,
        "from": str(raw.get("from") or "unknown"),
        "reaction": reaction,
        "sent_at": sent_at,
        "received_at": received_at,
        "id": raw.get("id"),
    }


def reaction_order(entry: Dict[str, Any]) -> Tuple[float, str]:
    """Ordering key for competing reactions from one reactor on one target."""
    return (float(entry.get("sent_at") or 0.0), str(entry.get("id") or ""))


def merge_reaction(items: List[Dict[str, Any]], entry: Dict[str, Any]) -> bool:
    """Apply ``entry`` to a reaction list in place, latest per reactor wins.

    One entry is kept per (target, destination).  The newer entry, ordered
    by ``(sent_at, id)``, replaces the stored one; an equal order replaces
    too, so a replayed reaction is idempotent.  A strictly older entry is
    ignored and ``False`` is returned.  Empty-token tombstones are kept
    like any other entry.  Shared by the inbox and the web console so both
    resolve reactions the same way.
    """
    for index, item in enumerate(items):
        if (
            item["target"] == entry["target"]
            and item["destination"] == entry["destination"]
        ):
            if reaction_order(entry) < reaction_order(item):
                return False
            del items[index]
            break
    items.append(entry)
    return True


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
        self._reactions: List[Dict[str, Any]] = []
        self._load()

    def _load(self) -> None:
        if self.path is None:
            return
        raw = read_json(self.path, default=None)
        if not isinstance(raw, dict):
            return
        entries = raw.get("messages")
        if isinstance(entries, list):
            loaded = []
            for entry in entries:
                record = _valid_record(entry)
                if record is not None:
                    loaded.append(record)
            self._messages = loaded[-MAX_INBOX:]
        reactions = raw.get("reactions")
        if isinstance(reactions, list):
            stored = []
            for entry in reactions:
                record = _valid_reaction(entry)
                if record is not None:
                    stored.append(record)
            self._reactions = stored[-MAX_REACTIONS:]

    def _save(self) -> None:
        if self.path is None:
            return
        try:
            atomic_write_json(
                self.path,
                {"messages": self._messages, "reactions": self._reactions},
                mode=0o600,
            )
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

    def record_reaction(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Apply one reaction, or retract when its token is empty.

        Per (target message, reactor) the newest reaction wins, ordered
        by the reactor's ``sent_at`` with ``id`` as tiebreak (see
        :func:`merge_reaction`).  A replayed reaction cannot double-count,
        and a late older reaction or retraction cannot override a newer
        one.  Returns the stored record when it became the reactor's
        current reaction, or ``None`` for a retraction, a stale update,
        or a malformed record.
        """
        entry = _valid_reaction(record)
        if entry is None:
            return None
        with self._transaction():
            if not merge_reaction(self._reactions, entry):
                return None
            self._reactions = self._reactions[-MAX_REACTIONS:]
            self._save()
        return entry if entry["reaction"] else None

    def reactions_for(self, target: str) -> List[Dict[str, Any]]:
        """Reactions on one message, oldest first.

        Each entry carries the reactor's destination hash, display name
        and token.  Only the reactor's latest reaction per message is
        ever stored, so these are already de-duplicated; tombstones for
        retractions are not returned.
        """
        with self._lock:
            entries = [
                dict(item)
                for item in self._reactions
                if item["target"] == target and item["reaction"]
            ]
        entries.sort(key=lambda item: item["received_at"])
        return entries

    def all_reactions(self) -> List[Dict[str, Any]]:
        """Every stored reaction, including retraction tombstones.

        The web bridge reconciles its history against this in one pass
        when serving messages, so reactions made from another process
        (e.g. ``tau react`` from the CLI) show up on the next read.
        """
        with self._lock:
            return [dict(item) for item in self._reactions]

    def resolve(self, ref: str) -> Dict[str, Any]:
        """Resolve a full message id or a unique prefix to its record.

        ``tau messages`` shows only the first 8 characters of each id,
        so a short prefix is the usual way to name a message — the same
        convention as ``tau outbox``.  An exact id always wins over a
        prefix match first, whatever its length: two ids can share a
        prefix where one is the whole of the other, and the full id must
        still resolve to its own message.
        """
        if not isinstance(ref, str) or not ref.strip():
            raise ValueError("message reference must not be empty")
        value = ref.strip()
        with self._lock:
            for entry in self._messages:
                if entry.get("id") == value:
                    return dict(entry)
            matches = [
                entry
                for entry in self._messages
                if isinstance(entry.get("id"), str)
                and entry["id"].startswith(value)
            ]
        if not matches:
            raise ValueError(f"unknown message: {ref!r}")
        if len(matches) > 1:
            raise ValueError(f"{len(matches)} messages match prefix {ref!r}")
        return dict(matches[0])

    def list_messages(
        self, *, limit: Optional[int] = None, peer: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Received messages, newest first.

        ``peer`` filters to one sender destination hash; ``limit`` caps
        how many are returned (``None`` means all of them).  Each entry
        carries a ``reactions`` list when the message has any — derived
        from the reaction store, never stored counters.
        """
        with self._lock:
            entries = list(self._messages)
            by_target: Dict[str, List[Dict[str, Any]]] = {}
            for item in self._reactions:
                if item["reaction"]:
                    by_target.setdefault(item["target"], []).append(dict(item))
        if peer is not None:
            entries = [item for item in entries if item.get("destination") == peer]
        entries.reverse()
        if limit is not None:
            entries = entries[: max(0, limit)]
        annotated = []
        for item in entries:
            copy = dict(item)
            reactions = by_target.get(str(item.get("id") or ""))
            if reactions:
                reactions.sort(key=lambda entry: entry["received_at"])
                copy["reactions"] = reactions
            annotated.append(copy)
        return annotated

    def __len__(self) -> int:
        with self._lock:
            return len(self._messages)
