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
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

try:  # POSIX only; the cross-process lock is a no-op without it.
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None  # type: ignore[assignment]

from .protocol import clean_in_reply_to
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
    receipt_sent_at = raw.get("receipt_sent_at")
    if receipt_sent_at is not None:
        try:
            receipt_sent_at = float(receipt_sent_at)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(receipt_sent_at):
            return None
    read_at = raw.get("read_at")
    if read_at is not None:
        try:
            read_at = float(read_at)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(read_at):
            return None
    expires_at = raw.get("expires_at")
    if expires_at is not None:
        try:
            expires_at = float(expires_at)
        except (TypeError, ValueError):
            expires_at = None
        if expires_at is not None and not math.isfinite(expires_at):
            expires_at = None
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
        "expires_at": expires_at,
        "direction": direction,
        "seq": seq,
    }
    if receipt_sent_at is not None:
        record["receipt_sent_at"] = receipt_sent_at
    if read_at is not None:
        record["read_at"] = read_at
    if in_reply_to is not None:
        record["in_reply_to"] = in_reply_to
    kind = raw.get("kind")
    if kind == "group":
        group_id = raw.get("group")
        if not isinstance(group_id, str) or len(group_id) != 32:
            return None
        record["kind"] = "group"
        record["group"] = group_id.lower()
        record["group_name"] = str(raw.get("group_name") or "group")
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
    # File records are always received: main's direction tracking
    # (added after this feature branched) defaults text records to
    # "in" the same way, and `list_messages` only shows "in" by
    # default — without it a received file never lists.
    seq = raw.get("seq")
    if isinstance(seq, bool) or not isinstance(seq, int) or seq <= 0:
        seq = None
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
        "direction": "in",
        "seq": seq,
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
        self._reactions: List[Dict[str, Any]] = []
        self._read_cursors: Dict[str, Dict[str, Any]] = {}
        # Load (and purge) under the cross-process lock: a purge saves the
        # file, which must not overwrite an append made by another process.
        with self._transaction():
            pass

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
        now = time.time()
        kept = [
            record
            for record in loaded
            if record.get("expires_at") is None or record["expires_at"] > now
        ]
        purged = len(loaded) - len(kept)
        self._messages = _retain(kept)
        _number_sequence(self._messages)
        reactions = raw.get("reactions")
        if isinstance(reactions, list):
            stored = []
            for entry in reactions:
                record = _valid_reaction(entry)
                if record is not None:
                    stored.append(record)
            self._reactions = stored[-MAX_REACTIONS:]
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
        if purged:
            # Disappearing messages whose deadline passed are gone for
            # good: persist the purge so a later load does not resurrect
            # them, and so `tau messages` never shows an expired entry.
            # Saved only now, with reactions and cursors loaded, so the
            # write cannot clobber them.
            log.info("Purged %d expired message(s) from the inbox", purged)
            self._save()

    def _next_seq(self) -> int:
        """The sequence number for the next arrival (caller holds the lock)."""
        highest = [entry["seq"] for entry in self._messages]
        highest += [cursor["last_read_seq"] for cursor in self._read_cursors.values()]
        return max(highest, default=0) + 1

    def _save(self) -> bool:
        """Persist the inbox; ``True`` when the write landed.

        A path-less (in-memory) inbox has nothing to write and reports
        success.  A failed write is logged and reported — callers that
        must not acknowledge unstored state (reaction handling) raise
        on ``False``; callers that must never break delivery (DM
        appends) ignore the return value.
        """
        if self.path is None:
            return True
        try:
            atomic_write_json(
                self.path,
                {
                    "messages": self._messages,
                    "reactions": self._reactions,
                    "read_cursors": self._read_cursors,
                },
                mode=0o600,
            )
        except OSError as exc:
            log.warning("Could not persist inbox to %s: %s", self.path, exc)
            return False
        return True

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

    def reload(self) -> None:
        """Reload the on-disk inbox, discarding the in-memory copy.

        One-shot commands (``tau messages``) stamp read/receipt state
        on disk while a serving node holds its own copy in memory; the
        serving sweep reloads before deciding what is owed.  Safe to
        call any time: every append persists immediately, so the file
        is never behind the cache.
        """
        with self._lock:
            self._load()

    def _stamp_read_ids(
        self, message_ids: List[str], *, at: Optional[float] = None
    ) -> List[str]:
        """Stamp *message_ids* as displayed by a human (read, not receipted).

        A one-shot reader (``tau messages``) cannot always start a node
        to send the receipt itself — a serving node already holds the
        TCP listen port, and Reticulum exits the process when a second
        node tries to bind it — so the stamp is the durable handoff:
        the serving node's mail sweep sends receipts for stamped
        messages it has not receipted yet.  Returns the ids stamped.
        """
        stamp = at if at is not None else time.time()
        wanted = set(message_ids)
        stamped: List[str] = []
        with self._transaction():
            for entry in self._messages:
                message_id = entry.get("id")
                if (
                    isinstance(message_id, str)
                    and message_id in wanted
                    and entry.get("read_at") is None
                ):
                    entry["read_at"] = stamp
                    stamped.append(message_id)
            if stamped:
                self._save()
        return stamped

    def read_unreceipted(self) -> List[Dict[str, Any]]:
        """Messages displayed (``read_at`` set) with no receipt sent yet."""
        with self._lock:
            return [
                dict(item)
                for item in self._messages
                if isinstance(item.get("id"), str)
                and item.get("id")
                and item.get("read_at") is not None
                and item.get("receipt_sent_at") is None
            ]

    def mark_receipt_sent(
        self, message_ids: List[str], *, at: Optional[float] = None
    ) -> List[str]:
        """Stamp *message_ids* as receipted (sent live, or queued).

        A receipt is sent once per message: listing the same inbox
        twice must not re-send it.  Returns the ids that were actually
        stamped — only messages this inbox holds, and only ones not
        already stamped.
        """
        stamp = at if at is not None else time.time()
        wanted = set(message_ids)
        stamped: List[str] = []
        with self._transaction():
            for entry in self._messages:
                message_id = entry.get("id")
                if (
                    isinstance(message_id, str)
                    and message_id in wanted
                    and entry.get("receipt_sent_at") is None
                ):
                    entry["receipt_sent_at"] = stamp
                    stamped.append(message_id)
            if stamped:
                self._save()
        return stamped

    def unreceipted(
        self, *, peer: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Received messages with an id that never had a receipt sent."""
        with self._lock:
            entries = [
                dict(item)
                for item in self._messages
                if isinstance(item.get("id"), str)
                and item.get("id")
                and item.get("receipt_sent_at") is None
            ]
        if peer is not None:
            entries = [item for item in entries if item.get("destination") == peer]
        return entries

    def record_reaction(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Apply one reaction, or retract when its token is empty.

        Per (target message, reactor) the newest reaction wins, ordered
        by the reactor's ``sent_at`` with ``id`` as tiebreak (see
        :func:`merge_reaction`).  A replayed reaction cannot double-count,
        and a late older reaction or retraction cannot override a newer
        one.  Returns the stored record when it became the reactor's
        current reaction, or ``None`` for a retraction, a stale update,
        or a malformed record.  Raises :class:`OSError` when the merged
        state cannot be persisted — a caller that acknowledges receipt
        must know the reaction was not stored, instead of marking it
        seen and losing it on a replay.
        """
        entry = _valid_reaction(record)
        if entry is None:
            return None
        with self._transaction():
            previous = list(self._reactions)
            if not merge_reaction(self._reactions, entry):
                return None
            self._reactions = self._reactions[-MAX_REACTIONS:]
            if not self._save():
                # The merge lives in memory only; restore the pre-merge
                # state so a later read does not show a reaction that
                # was never stored.  Reloading from disk is not enough:
                # a file that was never written (or has no reactions
                # key) would leave the merged entry in place.
                self._reactions = previous
                raise OSError(f"could not persist inbox to {self.path}")
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
        Read through the transaction path, so a serving process sees
        what another process wrote to ``inbox.json`` instead of its
        own stale in-memory cache.
        """
        with self._transaction():
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
        self,
        *,
        limit: Optional[int] = None,
        peer: Optional[str] = None,
        query: Optional[str] = None,
        group: Optional[str] = None,
        direction: Optional[str] = "in",
    ) -> List[Dict[str, Any]]:
        """Received messages, newest first.

        ``peer`` filters to one sender destination hash; ``group``
        filters to one group id; ``query`` keeps only messages whose
        text contains it (case-insensitive substring); ``limit`` caps
        how many are returned (``None`` means all of them).  Sent
        messages are recorded here too — a reply quotes the sender's
        own earlier message as often as the peer's — but they stay out
        of the default listing, which is the received-message view
        ``tau messages`` has always shown; pass ``direction="out"`` or
        ``direction=None`` to see them.
        Each entry carries ``unread``: whether it arrived after its
        sender's read cursor, and a ``reactions`` list when the message
        has any — derived from the reaction store, never stored counters.
        """
        with self._lock:
            entries = list(self._messages)
            cursors = {
                destination: dict(cursor)
                for destination, cursor in self._read_cursors.items()
            }
            by_target: Dict[str, List[Dict[str, Any]]] = {}
            for item in self._reactions:
                if item["reaction"]:
                    by_target.setdefault(item["target"], []).append(dict(item))
        if direction is not None:
            entries = [item for item in entries if item.get("direction") == direction]
        if peer is not None:
            entries = [item for item in entries if item.get("destination") == peer]
        if query is not None:
            needle = query.casefold()
            entries = [item for item in entries if needle in item["text"].casefold()]
        if group is not None:
            entries = [
                item
                for item in entries
                if item.get("kind") == "group" and item.get("group") == group
            ]
        entries.reverse()
        if limit is not None:
            entries = entries[: max(0, limit)]
        annotated = []
        for item in entries:
            copy = dict(
                item,
                unread=_is_unread(
                    item, cursors.get(item.get("destination") or "")
                ),
            )
            reactions = by_target.get(str(item.get("id") or ""))
            if reactions:
                reactions.sort(key=lambda entry: entry["received_at"])
                copy["reactions"] = reactions
            annotated.append(copy)
        return annotated

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
        self,
        peer: Union[None, str, List[str]] = None,
        *,
        up_to_seq: Optional[int] = None,
        at: Optional[float] = None,
    ) -> Any:
        """Advance the read cursor for one conversation, or all of them.

        The cursor lands on the newest message for the peer with an
        arrival sequence at or before ``up_to_seq`` (every message for
        the peer when ``up_to_seq`` is ``None``), so listing what was
        just shown marks exactly that much read.  A cursor never moves
        backward, so a delayed mark cannot restore read mail as unread.
        A peer with no messages is left alone.  Returns the unread
        counts after the mark.

        Called with a list of message ids instead of a peer, it stamps
        those messages' ``read_at`` for the read-receipt handoff (see
        :meth:`_stamp_read_ids`) and returns the ids stamped.
        """
        if isinstance(peer, (list, tuple)):
            return self._stamp_read_ids(list(peer), at=at)
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
