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
from typing import Any, Collection, Dict, Iterator, List, Optional, Tuple, Union

try:  # POSIX only; the cross-process lock is a no-op without it.
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None  # type: ignore[assignment]

from .protocol import (
    DEFAULT_MESSAGE_KIND,
    ProtocolError,
    clean_in_reply_to,
    normalize_message_kind,
)
from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

INBOX_FILE_NAME = "inbox.json"
MAX_INBOX = 500
INBOX_EXPORT_FORMAT = "tau-messages-export"
INBOX_EXPORT_VERSION = 1
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


class InboxError(ValueError):
    """Raised when an inbox lookup or delete fails."""


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
    # A lone surrogate passes the checks above but cannot be stored as
    # UTF-8, so it is rejected per record instead of failing the save.
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return None
    try:
        sent_at = float(raw.get("sent_at") or 0.0)
    except (TypeError, ValueError, OverflowError):
        return None
    received_at = raw.get("received_at")
    try:
        received_at = float(received_at) if received_at is not None else time.time()
    except (TypeError, ValueError, OverflowError):
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
    if raw.get("pinned"):
        record["pinned"] = True
    kind = raw.get("kind")
    if kind == "group":
        group_id = raw.get("group")
        if not isinstance(group_id, str) or len(group_id) != 32:
            return None
        record["kind"] = "group"
        record["group"] = group_id.lower()
        record["group_name"] = str(raw.get("group_name") or "group")
    else:
        try:
            kind = normalize_message_kind(raw.get("kind"))
        except ProtocolError:
            # A hand-edited record keeps its text; the kind falls back to
            # plain chat rather than dropping the message.
            kind = DEFAULT_MESSAGE_KIND
        record["kind"] = kind
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
    entry = {
        "target": target,
        "destination": destination,
        "from": str(raw.get("from") or "unknown"),
        "reaction": reaction,
        "sent_at": sent_at,
        "received_at": received_at,
        "id": raw.get("id"),
    }
    # A group reaction carries the signed context that names the
    # logical message — group, author, send time — so a reaction that
    # arrives before its target copy can be reconciled onto the copy
    # when it lands (see reconcile_group_reactions).  The context is
    # advisory only: malformed values are dropped, never a reason to
    # reject the reaction.
    group = raw.get("group")
    target_author = raw.get("target_author")
    target_sent_at = raw.get("target_sent_at")
    if (
        isinstance(group, str)
        and len(group) == 32
        and isinstance(target_author, str)
        and len(target_author) == 32
        and isinstance(target_sent_at, (int, float))
        and not isinstance(target_sent_at, bool)
        and math.isfinite(target_sent_at)
    ):
        entry["group"] = group
        entry["target_author"] = target_author
        entry["target_sent_at"] = float(target_sent_at)
    return entry


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
    record = {
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
    if raw.get("pinned"):
        record["pinned"] = True
    return record


def _record_error(raw: Any) -> Optional[str]:
    """Why :func:`_valid_record` would reject ``raw``, or ``None``.

    Import reports one reason per skipped entry instead of silently
    dropping it, so this mirrors the validation above closely enough
    to name the failing field.  A non-finite ``sent_at`` is normalized
    to 0.0 by ``_valid_record`` and is not an error here either.
    """
    if not isinstance(raw, dict):
        return "entry is not an object"
    text = raw.get("text")
    if not isinstance(text, str) or not text:
        return "inbox record needs text"
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return "text is not valid UTF-8"
    try:
        float(raw.get("sent_at") or 0.0)
    except (TypeError, ValueError, OverflowError):
        return "invalid sent_at"
    received_at = raw.get("received_at")
    try:
        received_at = float(received_at) if received_at is not None else time.time()
    except (TypeError, ValueError, OverflowError):
        return "invalid received_at"
    if not math.isfinite(received_at):
        return "invalid received_at"
    return None


def _dedupe_key(record: Dict[str, Any]) -> Any:
    """Identity of a normalized record for import dedupe.

    A record with a message id dedupes on that id; one without an id
    (older stores predate ids) dedupes on sender, destination, text
    and send time, so re-importing the same export is a no-op either
    way.
    """
    message_id = record.get("id")
    if message_id is not None and str(message_id):
        return ("id", str(message_id))
    return (
        "content",
        str(record.get("from") or ""),
        str(record.get("destination") or ""),
        str(record.get("text") or ""),
        float(record.get("sent_at") or 0.0),
    )


def _match_ref(entries: List[Dict[str, Any]], ref: str) -> Dict[str, Any]:
    """The stored entry addressed by *ref*, or raise :class:`InboxError`.

    Same convention as :meth:`Mailbox.resolve_outbox`: a full id
    matches exactly, a shorter prefix must match exactly one stored
    message.  Peer-supplied ids are case-sensitive: an exact match
    wins outright; only then fall back to a case-folded full match,
    and then to a case-folded prefix match.  The prefix step runs
    whatever the reference length — a 32-character prefix of a
    64-character LXMF id still resolves when no full id matches.
    Records with no id (older stores predate ids) cannot be addressed
    this way.
    """
    if not isinstance(ref, str) or not ref.strip():
        raise InboxError("message reference must not be empty")
    value = ref.strip()
    matches = [
        entry for entry in entries if str(entry.get("id") or "") == value
    ]
    if not matches:
        folded = value.lower()
        matches = [
            entry
            for entry in entries
            if str(entry.get("id") or "").lower() == folded
        ]
    if not matches:
        folded = value.lower()
        matches = [
            entry
            for entry in entries
            if str(entry.get("id") or "").lower().startswith(folded)
        ]
    if not matches:
        raise InboxError(f"unknown message: {ref!r}")
    if len(matches) > 1:
        raise InboxError(f"{len(matches)} messages match prefix {ref!r}")
    return matches[0]


class Inbox:
    """Received direct messages, persisted in the node data directory.

    A node without a data directory (tests, embeds) keeps messages in
    memory only.  Writes are atomic and the file is mode 0600, like the
    peer registry and mailbox; a failed write only logs a warning.
    """

    def __init__(self, path: Optional[Union[str, Path]] = None) -> None:
        self.path = Path(path).expanduser() if path is not None else None
        self._lock = threading.RLock()
        self._txn_depth = 0
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


    def _save(self, *, strict: bool = False) -> bool:
        """Persist the inbox; ``True`` when the write landed.

        A path-less (in-memory) inbox has nothing to write and reports
        success.  A failed write is logged and reported — callers that
        must not acknowledge unstored state (reaction handling) raise
        on ``False``; callers that must never break delivery (DM
        appends) ignore the return value.  :meth:`delete` passes
        ``strict=True`` so a failed persist raises instead of
        claiming success.
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
            if strict:
                raise
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

        Nested transactions join the outer one (the lock is
        reentrant, and the depth counter skips a second flock and
        reload): a handler that must check and store atomically —
        the reaction handler's final roster check and persist — can
        hold one transaction across both, so no other process can
        import the target between the check and the store.  A second
        flock from this process would block on the first.
        """
        self._lock.acquire()
        try:
            if self._txn_depth:
                # Nested transaction: the outer one already holds the
                # flock and reloaded state, so just join it.  (A second
                # flock from this process would block on the first.)
                yield
                return
            self._txn_depth = 1
            try:
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
            finally:
                self._txn_depth = 0
        finally:
            self._lock.release()

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

    def delete(self, ref: str) -> Dict[str, Any]:
        """Remove one stored message by full id or unique prefix.

        Same convention as :meth:`Mailbox.resolve_outbox`: a full
        32-character id matches exactly, a shorter prefix must match
        exactly one stored message.  The removal runs inside the
        cross-process transaction, so a concurrent ``tau serve`` cannot
        lose an entry appended while this delete runs.  Only this
        node's copy is removed — the sender keeps theirs, and sealed
        copies already held by relays are unaffected.  Records with no
        id (older stores predate ids) cannot be addressed this way.
        """
        with self._transaction():
            removed = _match_ref(list(self._messages), ref)
            previous = self._messages
            self._messages = [entry for entry in self._messages if entry is not removed]
            try:
                self._save(strict=True)
            except OSError as exc:
                # Roll back so this instance matches what is still on disk.
                self._messages = previous
                raise InboxError(
                    f"could not persist inbox; message was not deleted: {exc}"
                ) from exc
        return dict(removed)

    def pin(self, ref: str) -> Tuple[Dict[str, Any], bool]:
        """Pin one stored message by full id or unique prefix.

        Pins are local-only: nothing is sent to the peer, and the flag
        lives on the stored record, so it survives restarts and rides
        along with an export.  Deleting the message drops its pin with
        it.  Returns the pinned record and whether the pin is new —
        pinning an already-pinned message is a no-op, not an error.
        The change runs inside the cross-process transaction, like
        :meth:`delete`, so a concurrent ``tau serve`` cannot lose it.
        """
        with self._transaction():
            entry = _match_ref(list(self._messages), ref)
            if entry.get("pinned"):
                return dict(entry), False
            entry["pinned"] = True
            try:
                self._save(strict=True)
            except OSError as exc:
                # Roll back so this instance matches what is still on disk.
                del entry["pinned"]
                raise InboxError(
                    f"could not persist inbox; message was not pinned: {exc}"
                ) from exc
        return dict(entry), True

    def unpin(self, ref: str) -> Tuple[Dict[str, Any], bool]:
        """Remove the pin from one stored message.

        Same addressing as :meth:`pin`.  Returns the record and
        whether a pin was actually removed — unpinning a message that
        was never pinned is a no-op, not an error.
        """
        with self._transaction():
            entry = _match_ref(list(self._messages), ref)
            if not entry.get("pinned"):
                return dict(entry), False
            del entry["pinned"]
            try:
                self._save(strict=True)
            except OSError as exc:
                # Roll back so this instance matches what is still on disk.
                entry["pinned"] = True
                raise InboxError(
                    f"could not persist inbox; message was not unpinned: {exc}"
                ) from exc
        return dict(entry), True

    def delete_authorized(
        self, message_id: str, *, direction: str, destination: str
    ) -> Optional[Dict[str, Any]]:
        """Delete the record with exactly this id, when it matches.

        The exact-id match and the direction/destination checks run
        inside the same cross-process transaction as the delete, so a
        retraction authorized against one record can never delete a
        different record: a concurrent retract of the same id either
        wins this record first (then this call finds nothing) or loses
        it, and no prefix fallback ever re-selects another entry —
        unlike :meth:`delete`, which re-matches by prefix inside its
        own transaction.  Returns the deleted record, or ``None`` when
        no record has exactly this id.  Raises :class:`InboxError`
        when the record exists but fails a supplied check, or when the
        delete cannot be persisted.
        """
        if not isinstance(message_id, str) or not message_id.strip():
            raise InboxError("message reference must not be empty")
        value = message_id.strip()
        with self._transaction():
            for entry in self._messages:
                if str(entry.get("id") or "") != value:
                    continue
                if entry.get("direction") != direction:
                    raise InboxError("message was not received from this sender")
                if entry.get("destination") != destination:
                    raise InboxError("message was not sent by this sender")
                removed = entry
                previous = self._messages
                self._messages = [
                    other for other in self._messages if other is not removed
                ]
                try:
                    self._save(strict=True)
                except OSError as exc:
                    # Roll back so this instance matches what is still on disk.
                    self._messages = previous
                    raise InboxError(
                        f"could not persist inbox; message was not deleted: {exc}"
                    ) from exc
                return dict(removed)
            return None

    def reload(self) -> None:
        """Reload the on-disk inbox, discarding the in-memory copy.

        One-shot commands (``tau messages``) stamp read/receipt state
        on disk while a serving node holds its own copy in memory; the
        serving sweep reloads before deciding what is owed.  Safe to
        call any time: every append persists immediately, so the file
        is never behind the cache.

        The reload runs inside the cross-process transaction: a plain
        load can purge expired messages and write the purged snapshot
        back, and another process committing between that read and
        write would have its entries overwritten.  Under the flock the
        purge write is serialised with every other commit.
        """
        with self._transaction():
            pass

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

    def find_group_message(
        self, group: str, author: str, sent_at: float
    ) -> Optional[Dict[str, Any]]:
        """This node's copy of one group message, by shared identity.

        Group sends carry a different wire id per recipient leg, so a
        reaction that names another member's copy cannot be resolved by
        id here.  All legs of one send share the group, the author and
        the send time, so those three identify the local copy:
        ``sent_at`` is a float timestamp, so two distinct sends never
        share one; when several records match (a copy stored twice),
        the most recently received wins.  Returns ``None`` when no copy
        matches (for example a copy that arrived through an outbox
        retry, which re-stamps the send time).
        """
        try:
            wanted_at = float(sent_at)
        except (TypeError, ValueError):
            return None
        # Read through the transaction path, like reactions(): a copy
        # imported by another process (``tau import`` while ``tau
        # serve`` runs) must be visible here, or a reaction naming
        # that copy's wire id resolves against the stale cache and is
        # stored under a foreign id instead.
        with self._transaction():
            matches = [
                entry
                for entry in self._messages
                if entry.get("kind") == "group"
                and entry.get("group") == group
                and entry.get("destination") == author
                and isinstance(entry.get("sent_at"), (int, float))
                and not isinstance(entry.get("sent_at"), bool)
                and float(entry["sent_at"]) == wanted_at
            ]
        if not matches:
            return None
        matches.sort(key=lambda entry: entry.get("received_at") or 0)
        return dict(matches[-1])

    def reconcile_group_reactions(
        self,
        group: str,
        author: str,
        sent_at: float,
        message_id: str,
        *,
        members: Optional[Collection[str]] = None,
    ) -> int:
        """Retarget stored group reactions onto a newly arrived copy.

        A reaction can arrive before the message it names — legs fan
        out one member at a time, so an early reactor's reaction lands
        while another member's copy is still in flight — and it is
        stored under the reactor's foreign wire id with its signed
        group context retained.  When this node's copy arrives, move
        those reactions onto it so they display under the message
        instead of staying invisible forever.

        Only reactions whose stored context matches this copy exactly
        (group, author, send time) and whose target no message in the
        inbox holds are moved.  When *members* is given, only
        reactions from a reactor on that roster move: an allow-listed
        outsider's reaction is never associated with the message.
        Retargeting runs through :func:`merge_reaction`, so a newer
        reaction the same reactor already left on this copy wins and
        the older retargeted one is dropped.  Returns how many
        reactions were retargeted.  Raises :class:`OSError` when the
        retargeted state cannot be persisted.
        """
        try:
            wanted_at = float(sent_at)
        except (TypeError, ValueError):
            return 0
        if (
            not isinstance(group, str)
            or not group
            or not isinstance(author, str)
            or not author
            or not isinstance(message_id, str)
            or not message_id
            or not math.isfinite(wanted_at)
        ):
            return 0
        allowed = None if members is None else set(members)
        retargeted = 0
        with self._transaction():
            previous = list(self._reactions)
            retargeted = self._reconcile_group_reactions_locked(
                group, author, wanted_at, message_id, allowed
            )
            if retargeted:
                if not self._save():
                    self._reactions = previous
                    raise OSError(f"could not persist inbox to {self.path}")
        return retargeted

    def _reconcile_group_reactions_locked(
        self,
        group: str,
        author: str,
        wanted_at: float,
        message_id: str,
        allowed: Optional[Collection[str]],
    ) -> int:
        """Retarget stored group reactions onto a newly arrived copy.

        Caller holds the inbox transaction (state already reloaded):
        the reconcile mutates ``self._reactions`` in place and never
        saves.  ``allowed`` is the pinned roster, or ``None`` for no
        roster filter.  See :meth:`reconcile_group_reactions` for the
        matching rules.
        """
        held = {
            entry.get("id")
            for entry in self._messages
            if isinstance(entry.get("id"), str)
        }
        pending = [
            entry
            for entry in self._reactions
            if entry.get("group") == group
            and entry.get("target_author") == author
            and isinstance(entry.get("target_sent_at"), (int, float))
            and not isinstance(entry.get("target_sent_at"), bool)
            and float(entry["target_sent_at"]) == wanted_at
            and entry.get("target") not in held
            and (allowed is None or entry.get("destination") in allowed)
        ]
        retargeted = 0
        for entry in pending:
            moved = dict(entry)
            moved["target"] = message_id
            self._reactions.remove(entry)
            if merge_reaction(self._reactions, moved):
                retargeted += 1
        if retargeted:
            self._reactions = self._reactions[-MAX_REACTIONS:]
        return retargeted

    def append_group_message(
        self,
        record: Dict[str, Any],
        *,
        group: str,
        author: str,
        sent_at: float,
        members: Collection[str],
    ) -> List[Dict[str, Any]]:
        """Record one group message with its reaction authorization.

        A group message's arrival has three inbox effects — retargeting
        stored reactions onto it, voiding stored reactions from
        non-members, and appending the message itself — and they must
        land together: the purge used to run in one transaction and
        the append in the next, so a non-member's reaction stored
        between them never saw the target, skipped every roster
        check, and became readable once the message appended.  Here
        all three run under a single cross-process lock with a single
        persist, so no reaction can slip between the purge and the
        append, and no second ``Inbox._transaction`` is nested inside
        the first.

        A failed reconcile never breaks the arrival — it only strands
        legitimate reactions, which stay invisible — so it is logged
        and the purge and append still run.  A failed persist raises
        :class:`OSError` and the in-memory state is restored, so the
        caller can refuse the delivery and the sender can retry it.
        Returns the tombstones written, one per voided reaction, so
        the caller can fan them out to any reaction cache.
        """
        entry = _valid_record(record)
        if entry is None:
            raise ValueError("inbox record needs text")
        try:
            wanted_at: Optional[float] = float(sent_at)
        except (TypeError, ValueError):
            wanted_at = None
        message_id = entry.get("id")
        allowed = set(members)
        voided: List[Dict[str, Any]] = []
        with self._transaction():
            previous_reactions = list(self._reactions)
            previous_messages = list(self._messages)
            try:
                if (
                    wanted_at is not None
                    and math.isfinite(wanted_at)
                    and isinstance(group, str)
                    and group
                    and isinstance(author, str)
                    and author
                    and isinstance(message_id, str)
                    and message_id
                ):
                    try:
                        self._reconcile_group_reactions_locked(
                            group, author, wanted_at, message_id, allowed
                        )
                    except Exception:
                        # A failed reconcile only strands legitimate
                        # reactions (invisible); it never breaks the
                        # arrival, and the purge below still runs.
                        log.warning(
                            "Could not reconcile group reactions",
                            exc_info=True,
                        )
                voided = self._purge_non_member_reactions_locked(
                    message_id if isinstance(message_id, str) else "",
                    allowed,
                    time.time(),
                )
                entry["seq"] = self._next_seq()
                self._messages.append(entry)
                self._messages = _retain(self._messages)
                if not self._save():
                    raise OSError(f"could not persist inbox to {self.path}")
            except OSError:
                self._reactions = previous_reactions
                self._messages = previous_messages
                raise
        return voided

    def purge_non_member_reactions(
        self, message_id: str, *, members: Collection[str]
    ) -> List[Dict[str, Any]]:
        """Void stored reactions on one group message from non-members.

        A reaction can be stored before its target arrives — the target
        was unknown then, so no roster check could run — and then
        surface under the exact message id when the copy lands, without
        any authorization ever running.  When a group message arrives,
        void every stored reaction on it whose reactor is not on the
        pinned roster, so only members' reactions display.

        Voiding is a tombstone, not a deletion: its ``(sent_at, id)``
        is strictly newer than the reaction it voids, so a replayed
        copy of the voided reaction cannot resurrect it, while a later
        reaction from the same reactor — for example after joining the
        group — replaces the tombstone.  Returns the tombstones
        written, one per voided reaction, so the caller can fan them
        out to any reaction cache (the console bridge keeps its own).
        Raises :class:`OSError` when the voided state cannot be
        persisted.
        """
        if not isinstance(message_id, str) or not message_id:
            return []
        allowed = set(members)
        voided: List[Dict[str, Any]] = []
        with self._transaction():
            previous = list(self._reactions)
            voided = self._purge_non_member_reactions_locked(
                message_id, allowed, time.time()
            )
            if voided:
                if not self._save():
                    self._reactions = previous
                    raise OSError(f"could not persist inbox to {self.path}")
        return voided

    def _purge_non_member_reactions_locked(
        self,
        message_id: str,
        allowed: Collection[str],
        now: float,
    ) -> List[Dict[str, Any]]:
        """Void stored reactions on one group message from non-members.

        Caller holds the inbox transaction: the tombstones are merged
        into ``self._reactions`` in place and never saved here.  See
        :meth:`purge_non_member_reactions` for the tombstone rules.
        """
        voided: List[Dict[str, Any]] = []
        for entry in list(self._reactions):
            if (
                entry.get("target") == message_id
                and entry.get("reaction")
                and entry.get("destination") not in allowed
            ):
                tombstone = dict(entry)
                tombstone["reaction"] = ""
                tombstone["sent_at"] = (
                    max(float(entry.get("sent_at") or 0.0), now) + 1.0
                )
                tombstone["received_at"] = now
                if merge_reaction(self._reactions, tombstone):
                    voided.append(tombstone)
        if voided:
            self._reactions = self._reactions[-MAX_REACTIONS:]
        return voided

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
        kind: Optional[str] = None,
        direction: Optional[str] = "in",
        pinned_only: bool = False,
    ) -> List[Dict[str, Any]]:
        """Received messages, newest first.

        ``peer`` filters to one sender destination hash; ``group``
        filters to one group id; ``kind`` filters to one message kind;
        ``query`` keeps only messages whose
        text contains it (case-insensitive substring); ``limit`` caps
        how many are returned (``None`` means all of them).  Sent
        messages are recorded here too — a reply quotes the sender's
        own earlier message as often as the peer's — but they stay out
        of the default listing, which is the received-message view
        ``tau messages`` has always shown; pass ``direction="out"`` or
        ``direction=None`` to see them.  ``pinned_only`` keeps only
        messages flagged by :meth:`pin`.
        Each entry carries ``unread``: whether it arrived after its
        sender's read cursor, and a ``reactions`` list when the message
        has any — derived from the reaction store, never stored counters.
        Sent messages are never unread: the cursor belongs to the
        conversation's received side, so comparing a sent record's
        sequence against it would flag it arbitrarily.
        """
        if direction is not None and direction not in ("in", "out"):
            raise ValueError('direction must be "in" or "out"')
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
            entries = [
                item
                for item in entries
                if needle in str(item.get("text") or "").casefold()
            ]
        if group is not None:
            entries = [
                item
                for item in entries
                if item.get("kind") == "group" and item.get("group") == group
            ]
        if kind is not None:
            entries = [item for item in entries if item.get("kind") == kind]
        if pinned_only:
            entries = [item for item in entries if item.get("pinned")]
        entries.reverse()
        if limit is not None:
            entries = entries[: max(0, limit)]
        annotated = []
        for item in entries:
            # Unread is a received-message state: a message this node
            # sent is never unread, whatever the conversation's read
            # cursor says about its arrival sequence.
            unread = item.get("direction") != "out" and _is_unread(
                item, cursors.get(item.get("destination") or "")
            )
            copy = dict(item, unread=unread)
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

    def export_document(self, *, peer: Optional[str] = None) -> Dict[str, Any]:
        """Return a portable backup of the stored messages.

        Messages are in stored (oldest-first) order so an import replays
        them in order; ``peer`` restricts the export to one sender
        destination hash.  The document is the same shape
        :meth:`import_messages` accepts.  The snapshot is read through
        the cross-process transaction, so messages another process
        committed since this inbox last loaded are included instead of
        silently omitted from the backup.
        """
        with self.transaction():
            entries = [dict(item) for item in self._messages]
        if peer is not None:
            entries = [item for item in entries if item.get("destination") == peer]
        return {
            "format": INBOX_EXPORT_FORMAT,
            "version": INBOX_EXPORT_VERSION,
            "exported_at": time.time(),
            "messages": entries,
        }

    def import_messages(
        self,
        document: Any,
        inserted: Optional[List[Dict[str, Any]]] = None,
        *,
        is_retracted: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Merge every message in an export document into the inbox.

        Returns ``{"imported": n, "present": n, "skipped": n, "errors":
        [...]}``.  An entry that fails validation is skipped with its
        error recorded; it never aborts the rest of the import, and
        entries already in the inbox are counted as present rather than
        stored twice.  When *inserted* is given, the records this call
        actually stored are appended to it, in arrival order, so a
        caller mirrors exactly what this import added.  The whole merge
        runs in one transaction, so a concurrent ``tau serve`` cannot
        lose entries, and the
        per-direction 500-record caps still apply.  Imported records
        get local arrival sequences (an export carries the source
        node's, which mean nothing here), and a failed persist raises
        :class:`OSError` instead of reporting a restore that never
        landed — the in-memory merge is rolled back with it.

        With ``is_retracted`` given (a ``(sender_hash, message_id) ->
        bool`` check), a received record its sender retracted is
        skipped with its reason recorded instead of being stored: an
        export made before the retract must not resurrect the message.
        """
        if not isinstance(document, dict):
            raise ValueError("export file must contain a JSON object")
        if document.get("format") != INBOX_EXPORT_FORMAT:
            raise ValueError(
                f"unsupported export format: {document.get('format')!r}"
            )
        if document.get("version") != INBOX_EXPORT_VERSION:
            raise ValueError(
                f"unsupported export version: {document.get('version')!r}"
            )
        entries = document.get("messages")
        if not isinstance(entries, list):
            raise ValueError("export file has no messages list")

        imported = present = skipped = 0
        errors: List[Dict[str, Any]] = []
        added: List[Dict[str, Any]] = []
        with self._transaction():
            previous = list(self._messages)
            seen = {_dedupe_key(item) for item in self._messages}
            for entry in entries:
                record = _valid_record(entry)
                if record is None:
                    ref = None
                    if isinstance(entry, dict) and entry.get("id") is not None:
                        ref = str(entry.get("id"))
                    skipped += 1
                    errors.append(
                        {
                            "id": ref,
                            "error": _record_error(entry) or "invalid inbox record",
                        }
                    )
                    continue
                key = _dedupe_key(record)
                if key in seen:
                    present += 1
                    continue
                if (
                    is_retracted is not None
                    and record.get("direction") == "in"
                    # Group and file records are never retractable (see
                    # the retract handler), so a marker can't cover them.
                    and record.get("kind") not in ("group", "file")
                    and isinstance(record.get("destination"), str)
                    and isinstance(record.get("id"), str)
                    and is_retracted(record["destination"], record["id"])
                ):
                    # A retracted message stays retracted: an export
                    # made before the retract must not resurrect it.
                    # Sent records are never checked — their
                    # destination is the recipient, not the sender, so
                    # they cannot match a marker for this message.
                    skipped += 1
                    errors.append(
                        {
                            "id": record["id"],
                            "error": "message was retracted by its sender",
                        }
                    )
                    continue
                seen.add(key)
                self._messages.append(record)
                added.append(record)
                imported += 1
            self._messages = _retain(self._messages)
            _number_sequence(self._messages)
            if not self._save():
                # The merge lives in memory only; restore the pre-merge
                # state so a later read does not show messages that were
                # never stored.
                self._messages = previous
                raise OSError(f"could not persist inbox to {self.path}")
            if inserted is not None:
                # Only records that survived the retention cap were
                # stored. Collect them while the transaction still holds
                # the lock: a concurrent reload replaces the record
                # objects in self._messages, so an identity check after
                # the transaction would find none of them and the caller
                # would mirror nothing.
                kept = {id(item) for item in self._messages}
                inserted.extend(item for item in added if id(item) in kept)
        return {
            "imported": imported,
            "present": present,
            "skipped": skipped,
            "errors": errors,
        }

    def __len__(self) -> int:
        with self._lock:
            return len(self._messages)
