"""Persistent store-and-forward mail for tau.

Direct messages are live-only: both nodes must be reachable at the same
moment.  This module adds the missing half — a small persistent mailbox in
the node's data directory holding three collections:

``outbox``
    Messages this node owes a peer that was unreachable when they were
    sent.  Entries are retried whenever the peer is heard from again and
    expire after a TTL.  Failed attempts back off exponentially (60s,
    doubling, capped at an hour), so a dead peer does not cost a link
    attempt on every sweep; a peer (re)announcing bypasses the wait.
``held``
    Sealed envelopes other nodes asked this node to hold for a third
    peer.  An envelope is encrypted to the recipient and signed by the
    sender, so a holder can neither read nor forge what it carries; it
    only knows the recipient hash, the message id and the expiry.
    Held mail is also capped per depositing peer (see the
    ``HELD_QUOTA_*`` constants): one sender cannot fill a relay's
    storage, and when a deposit would push a sender over quota the
    relay sheds that sender's oldest envelopes first.
``seen``
    Ids of messages already delivered to the user, so a message that
    arrives twice — once directly, once via a holder — is shown once.
``sent``
    Messages this node sent, keyed by message id, with the recipient
    and the time they were read (``read_at``).  A read receipt is only
    believed when it names an id here *and* its reader is the recorded
    recipient — that is what stops a forged receipt.
``receipts``
    Signed read receipts this node owes a sender that was unreachable
    when the message was read.  They ride the same store-and-forward
    machinery as the outbox: direct delivery when the sender is
    reachable, sealed deposits with holders when it is not.

The file format and locking mirror the peer registry: one JSON document,
atomic writes, thread-safe access.
"""

from __future__ import annotations

import base64
import logging
import math
import os
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Union

try:  # POSIX only; mailbox cross-process locking is a no-op without it.
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None  # type: ignore[assignment]

import RNS

from .identity import (
    destination_hash as tau_destination_hash,
    load_public_key,
)
from .protocol import (
    DEFAULT_MESSAGE_KIND,
    MAX_MAIL_BLOB_BYTES,
    MAX_MAIL_TTL,
    ProtocolError,
    clean_in_reply_to,
    decode,
    encode,
    generate_message_id,
    kind_from_wire,
    normalize_message,
    normalize_message_kind,
    sanitize_group_name,
    sanitize_name,
)
from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

MAILBOX_VERSION = 1
DEFAULT_TTL = 7 * 24 * 60 * 60
SEEN_RETENTION = 30 * 24 * 60 * 60
# Retraction markers: (sender hash, message id) -> expiry.  A retract
# deletes the inbox copy, but a sealed relay copy can still arrive
# afterwards — and the bounded seen-id cache can evict the id — so the
# marker outlives both, for the possible delivery lifetime of the
# message (the 7-day mail TTL), then expires.
RETRACTED_RETENTION = DEFAULT_TTL
MAX_OUTBOX = 256
MAX_HELD_TOTAL = 512
MAX_HELD_PER_RECIPIENT = 64
MAX_SEEN = 1024
MAX_HOLDERS_PER_MESSAGE = 3
# Sent-message history (for read receipts) and queued receipts waiting
# on an unreachable sender.  Both are small metadata records, capped
# like the seen set; sent entries also keep the text so `tau messages
# --sent` can show what was read.
MAX_SENT = 256
MAX_RECEIPTS = 256
SENT_RETENTION = 30 * 24 * 60 * 60


def _retraction_key(sender_hash: str, message_id: str) -> str:
    """One flat key for a (sender, message id) pair; both parts are hex."""
    return f"{sender_hash}:{message_id}"


def _env_int(name: str, default: int) -> int:
    """Read an integer quota override from the environment, or *default*."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


# Held-mail fairness quotas (issue #89): one sender may not fill a
# relay's storage with envelopes for many recipients.  A holder can only
# see the peer that asked it to hold an envelope — the sealed blob hides
# the inner sender — so quotas are keyed by that depositing peer, and
# deposits over an unidentified link share one "unknown" bucket.  When a
# deposit would push a sender over quota, the relay sheds that sender's
# oldest envelopes first; only a single envelope that cannot fit even on
# its own is refused.  Long-standing known peers with a delivered
# history get the higher tier.
HELD_QUOTA_UNKNOWN_COUNT = _env_int("TAU_HELD_QUOTA_UNKNOWN_COUNT", 16)
HELD_QUOTA_KNOWN_COUNT = _env_int("TAU_HELD_QUOTA_KNOWN_COUNT", 128)
HELD_QUOTA_UNKNOWN_BYTES = _env_int(
    "TAU_HELD_QUOTA_UNKNOWN_BYTES", 128 * 1024
)
HELD_QUOTA_KNOWN_BYTES = _env_int("TAU_HELD_QUOTA_KNOWN_BYTES", 1024 * 1024)
# A peer counts as long-standing once it has been in the local registry
# this long (and was heard directly, not via exchange).
TRUSTED_PEER_MIN_AGE_SECONDS = _env_int(
    "TAU_TRUSTED_PEER_MIN_AGE_SECONDS", 24 * 3600
)

# Direct-delivery retry pacing: after a failed attempt, wait 60s, then
# double after each further failure, up to an hour.  An announce from the
# recipient bypasses the wait (see TauNode._mail_round_for_peer).
DELIVERY_BACKOFF_BASE_SECONDS = 60.0
DELIVERY_BACKOFF_MAX_SECONDS = 3600.0

_DESTINATION_BYTES = 16
_PUBLIC_KEY_BYTES = 64


def _is_hex(value: str) -> bool:
    try:
        bytes.fromhex(value)
    except ValueError:
        return False
    return True


class MailboxError(RuntimeError):
    """Raised when a mailbox or sealed-message operation fails."""


class MailboxFull(MailboxError):
    """Raised when a mailbox collection is at its capacity limit."""


@dataclass
class OutboxEntry:
    """A message this node still owes one of its peers."""

    id: str
    to: bytes
    text: str
    created_at: float
    expires_at: float
    to_name: str = ""
    kind: str = DEFAULT_MESSAGE_KIND
    attempts: int = 0
    last_attempt_at: Optional[float] = None
    last_error: Optional[str] = None
    held_by: Dict[str, float] = field(default_factory=dict)
    # Group DM context: when present, this entry is one leg of a group
    # message — the payload is a /tau/group request carrying this roster
    # instead of a plain /tau/message.  ``members`` is the full roster
    # (destination hashes, hex) so a retry re-sends the same group.
    group_id: Optional[str] = None
    group_name: str = ""
    group_creator: str = ""
    group_members: List[str] = field(default_factory=list)
    # Sender-set expiry of the message itself, if any.  Distinct from
    # ``expires_at`` (when the queue entry itself expires): a relayed
    # copy is sealed with this deadline so the recipient purges the
    # message on schedule, while a message with no sender-set expiry
    # stays in the recipient's inbox even though its queue entry still
    # has a TTL.
    message_expires_at: Optional[float] = None
    in_reply_to: Optional[Dict[str, Any]] = None

    @property
    def to_hex(self) -> str:
        return self.to.hex()

    @property
    def next_attempt_at(self) -> Optional[float]:
        """Earliest time this entry may be attempted again.

        ``None`` when the entry has never been attempted (always due).
        Otherwise the last attempt plus an exponential backoff — 60s
        after the first failure, doubling per failure, capped at an hour.
        """
        if self.attempts <= 0 or self.last_attempt_at is None:
            return None
        # Clamp the exponent before exponentiating: an entry that
        # accumulated attempts under the old fixed retry interval can
        # carry a huge stored count, and 2 ** attempts would overflow
        # before the cap below could apply.  60s * 2**6 already exceeds
        # the one-hour cap, so anything larger is indistinguishable.
        exponent = min(self.attempts - 1, 6)
        delay = min(
            DELIVERY_BACKOFF_BASE_SECONDS * (2 ** exponent),
            DELIVERY_BACKOFF_MAX_SECONDS,
        )
        return self.last_attempt_at + delay

    def is_due(self, now: Optional[float] = None) -> bool:
        """True when the entry may be attempted at *now* (default: now)."""
        target = self.next_attempt_at
        if target is None:
            return True
        return (now if now is not None else time.time()) >= target

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "id": self.id,
            "to": self.to_hex,
            "to_name": self.to_name,
            "text": self.text,
            "kind": self.kind,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "attempts": self.attempts,
            "last_attempt_at": self.last_attempt_at,
            "last_error": self.last_error,
            "next_attempt_at": self.next_attempt_at,
            "held_by": dict(self.held_by),
            "message_expires_at": self.message_expires_at,
        }
        if self.group_id is not None:
            payload["group"] = {
                "id": self.group_id,
                "name": self.group_name,
                "creator": self.group_creator,
                "members": list(self.group_members),
            }
        if self.in_reply_to is not None:
            payload["in_reply_to"] = dict(self.in_reply_to)
        return payload

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "OutboxEntry":
        to_hex = raw.get("to")
        if not isinstance(to_hex, str) or len(to_hex) != 32:
            raise ValueError("invalid outbox destination")
        destination = bytes.fromhex(to_hex)
        if len(destination) != _DESTINATION_BYTES:
            raise ValueError("invalid outbox destination length")
        message_id = raw.get("id")
        if not isinstance(message_id, str) or not message_id:
            raise ValueError("invalid outbox message id")
        created_at = raw.get("created_at")
        expires_at = raw.get("expires_at")
        if not isinstance(created_at, (int, float)) or not isinstance(
            expires_at, (int, float)
        ):
            raise ValueError("invalid outbox timestamps")
        if not math.isfinite(created_at) or not math.isfinite(expires_at):
            raise ValueError("non-finite outbox timestamps")
        held_by = raw.get("held_by")
        if not isinstance(held_by, dict):
            held_by = {}
        attempts = raw.get("attempts")
        try:
            kind = normalize_message_kind(raw.get("kind"))
        except ProtocolError:
            # A hand-edited or corrupt entry keeps its text; the kind
            # falls back to plain chat rather than dropping the message.
            kind = DEFAULT_MESSAGE_KIND
        group_raw = raw.get("group")
        group_id: Optional[str] = None
        group_name = ""
        group_creator = ""
        group_members: List[str] = []
        if isinstance(group_raw, dict):
            candidate_id = group_raw.get("id")
            candidate_creator = group_raw.get("creator")
            candidate_members = group_raw.get("members")
            if (
                isinstance(candidate_id, str)
                and len(candidate_id) == 32
                and isinstance(candidate_creator, str)
                and len(candidate_creator) == 32
                and isinstance(candidate_members, list)
                and all(
                    isinstance(m, str) and len(m) == 32 for m in candidate_members
                )
            ):
                try:
                    bytes.fromhex(candidate_id)
                    bytes.fromhex(candidate_creator)
                    for member in candidate_members:
                        bytes.fromhex(member)
                except ValueError:
                    pass
                else:
                    group_id = candidate_id.lower()
                    group_name = sanitize_group_name(group_raw.get("name"))
                    group_creator = candidate_creator.lower()
                    group_members = [m.lower() for m in candidate_members]
        message_expires_at = raw.get("message_expires_at")
        if isinstance(message_expires_at, bool) or not isinstance(
            message_expires_at, (int, float)
        ):
            message_expires_at = None
        else:
            try:
                message_expires_at = float(message_expires_at)
            except (OverflowError, ValueError):
                message_expires_at = None
            if message_expires_at is not None and not math.isfinite(
                message_expires_at
            ):
                message_expires_at = None
        try:
            in_reply_to = clean_in_reply_to(raw.get("in_reply_to"))
        except ValueError:
            # A corrupt reference on an already-queued entry must not
            # make the outbox unreadable; the message still delivers,
            # just without its quote.
            in_reply_to = None
        return cls(
            id=message_id,
            to=destination,
            text=normalize_message(raw.get("text")),
            created_at=float(created_at),
            expires_at=float(expires_at),
            to_name=sanitize_name(raw.get("to_name"), fallback="")
            if isinstance(raw.get("to_name"), str)
            else "",
            kind=kind,
            attempts=int(attempts) if isinstance(attempts, int) else 0,
            last_attempt_at=raw.get("last_attempt_at")
            if isinstance(raw.get("last_attempt_at"), (int, float))
            else None,
            last_error=raw.get("last_error")
            if isinstance(raw.get("last_error"), str)
            else None,
            held_by={
                str(k): float(v)
                for k, v in held_by.items()
                if isinstance(v, (int, float))
            },
            group_id=group_id,
            group_name=group_name,
            group_creator=group_creator,
            group_members=group_members,
            message_expires_at=message_expires_at,
            in_reply_to=in_reply_to,
        )


@dataclass
class ReceiptEntry:
    """A read receipt this node owes a sender that was unreachable.

    Mirrors :class:`OutboxEntry` (same backoff, same holder deposits),
    but the payload is a signed list of message ids the reader has
    seen rather than a text message.  A receipt entry is removed once
    the sender acknowledges it; receipts are never receipted.
    """

    id: str
    to: bytes
    message_ids: List[str]
    read_at: float
    created_at: float
    expires_at: float
    attempts: int = 0
    last_attempt_at: Optional[float] = None
    held_by: Dict[str, float] = field(default_factory=dict)

    @property
    def to_hex(self) -> str:
        return self.to.hex()

    @property
    def next_attempt_at(self) -> Optional[float]:
        if self.attempts <= 0 or self.last_attempt_at is None:
            return None
        exponent = min(self.attempts - 1, 6)
        delay = min(
            DELIVERY_BACKOFF_BASE_SECONDS * (2 ** exponent),
            DELIVERY_BACKOFF_MAX_SECONDS,
        )
        return self.last_attempt_at + delay

    def is_due(self, now: Optional[float] = None) -> bool:
        target = self.next_attempt_at
        if target is None:
            return True
        return (now if now is not None else time.time()) >= target

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "to": self.to_hex,
            "message_ids": list(self.message_ids),
            "read_at": self.read_at,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "attempts": self.attempts,
            "last_attempt_at": self.last_attempt_at,
            "next_attempt_at": self.next_attempt_at,
            "held_by": dict(self.held_by),
        }

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "ReceiptEntry":
        to_hex = raw.get("to")
        if not isinstance(to_hex, str) or len(to_hex) != 32:
            raise ValueError("invalid receipt destination")
        destination = bytes.fromhex(to_hex)
        if len(destination) != _DESTINATION_BYTES:
            raise ValueError("invalid receipt destination length")
        receipt_id = raw.get("id")
        if not isinstance(receipt_id, str) or not receipt_id:
            raise ValueError("invalid receipt id")
        message_ids = raw.get("message_ids")
        if not isinstance(message_ids, list) or not message_ids:
            raise ValueError("invalid receipt message ids")
        cleaned_ids = []
        for message_id in message_ids:
            if not isinstance(message_id, str) or not message_id or len(message_id) > 64:
                raise ValueError("invalid receipt message id")
            if message_id not in cleaned_ids:
                cleaned_ids.append(message_id)
        created_at = raw.get("created_at")
        expires_at = raw.get("expires_at")
        read_at = raw.get("read_at")
        if not isinstance(created_at, (int, float)) or not isinstance(
            expires_at, (int, float)
        ) or not isinstance(read_at, (int, float)):
            raise ValueError("invalid receipt timestamps")
        held_by = raw.get("held_by")
        if not isinstance(held_by, dict):
            held_by = {}
        attempts = raw.get("attempts")
        return cls(
            id=receipt_id,
            to=destination,
            message_ids=cleaned_ids,
            read_at=float(read_at),
            created_at=float(created_at),
            expires_at=float(expires_at),
            attempts=int(attempts) if isinstance(attempts, int) else 0,
            last_attempt_at=raw.get("last_attempt_at")
            if isinstance(raw.get("last_attempt_at"), (int, float))
            else None,
            held_by={
                str(k): float(v)
                for k, v in held_by.items()
                if isinstance(v, (int, float))
            },
        )


@dataclass
class HeldEnvelope:
    """A sealed message this node is holding for another peer."""

    id: str
    to: bytes
    blob: bytes
    stored_at: float
    expires_at: float
    # Hex destination hash of the peer that asked this node to hold the
    # envelope ("" when the deposit arrived over an unidentified link).
    # The sealed blob hides the inner sender, so this is the only
    # attribution a holder has — and the key for per-sender quotas.
    depositor: str = ""

    @property
    def to_hex(self) -> str:
        return self.to.hex()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "to": self.to_hex,
            "blob": base64.b64encode(self.blob).decode("ascii"),
            "stored_at": self.stored_at,
            "expires_at": self.expires_at,
            "depositor": self.depositor,
        }

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "HeldEnvelope":
        to_hex = raw.get("to")
        if not isinstance(to_hex, str) or len(to_hex) != 32:
            raise ValueError("invalid held destination")
        destination = bytes.fromhex(to_hex)
        if len(destination) != _DESTINATION_BYTES:
            raise ValueError("invalid held destination length")
        message_id = raw.get("id")
        if not isinstance(message_id, str) or not message_id:
            raise ValueError("invalid held message id")
        blob_text = raw.get("blob")
        if not isinstance(blob_text, str):
            raise ValueError("invalid held blob")
        try:
            blob = base64.b64decode(blob_text, validate=True)
        except ValueError as exc:
            raise ValueError("invalid held blob encoding") from exc
        if len(blob) > MAX_MAIL_BLOB_BYTES:
            raise ValueError("held blob too large")
        stored_at = raw.get("stored_at")
        expires_at = raw.get("expires_at")
        if not isinstance(stored_at, (int, float)) or not isinstance(
            expires_at, (int, float)
        ):
            raise ValueError("invalid held timestamps")
        if not math.isfinite(stored_at) or not math.isfinite(expires_at):
            raise ValueError("non-finite held timestamps")
        depositor = raw.get("depositor")
        if (
            not isinstance(depositor, str)
            or (depositor and len(depositor) != 32)
            or (depositor and not _is_hex(depositor))
        ):
            # Envelopes stored before per-sender quotas carry no
            # attribution; they join the unknown bucket.
            depositor = ""
        return cls(
            id=message_id,
            to=destination,
            blob=blob,
            stored_at=float(stored_at),
            expires_at=float(expires_at),
            depositor=depositor,
        )


def _lock_file(handle: Any, timeout: Optional[float]) -> None:
    """Take the exclusive flock on a mailbox lock file.

    With no timeout this blocks, as before.  With a timeout the wait
    is bounded by it: the lock is tried non-blocking in a short loop
    and a ``TimeoutError`` is raised once the budget is spent, so a
    stalled holder cannot block a caller that has its own deadline.
    """
    if timeout is None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        return
    deadline = time.monotonic() + max(0.0, timeout)
    while True:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            if time.monotonic() >= deadline:
                raise TimeoutError("mailbox lock is held by another process")
            time.sleep(0.005)


class Mailbox:
    """Thread-safe store-and-forward storage, optionally JSON backed."""

    def __init__(self, path: Optional[Union[str, Path]] = None) -> None:
        self.path = Path(path).expanduser() if path is not None else None
        self._lock = threading.RLock()
        self._txn_depth = 0
        self._outbox: Dict[str, OutboxEntry] = {}
        self._held: Dict[str, HeldEnvelope] = {}
        self._seen: Dict[str, float] = {}
        self._sent: Dict[str, Dict[str, Any]] = {}
        self._receipts: Dict[str, ReceiptEntry] = {}
        self._pending_acks: Dict[str, List[str]] = {}
        # Ids the owner dropped with `tau outbox drop-held`, with the
        # expiry of the envelope that was dropped: a sender retrying its
        # deposit must not resurrect the copy until the original would
        # have expired anyway.
        self._dropped_held: Dict[str, float] = {}
        # Retraction markers, keyed by (sender hash, message id), with
        # the time the marker expires (see RETRACTED_RETENTION).
        self._retracted: Dict[str, float] = {}
        if self.path is not None:
            self.load()

    @contextmanager
    def _transaction(self, timeout: Optional[float] = None) -> Iterator[None]:
        """Serialise a read-modify-write of the mailbox across processes.

        ``tau serve`` and one-shot commands such as ``tau send`` share the
        same ``mailbox.json``.  Each mutation takes an exclusive ``flock`` on
        a sidecar lock file, reloads the latest on-disk state, and then
        applies its change, so a process never overwrites another's writes.

        With ``timeout`` set, the wait for both the in-process lock and
        the flock is bounded by it (a ``TimeoutError`` is raised once
        the budget is spent), so a stalled holder cannot block a caller
        that has its own deadline.  With no timeout the wait is
        unbounded, as before.
        """
        deadline = None if timeout is None else time.monotonic() + max(0.0, timeout)
        if deadline is None:
            self._lock.acquire()
        elif not self._lock.acquire(timeout=max(0.0, deadline - time.monotonic())):
            raise TimeoutError("mailbox lock is held by another thread")
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
                        self._apply_document()
                    yield
                    return
                lock_path = self.path.with_name(self.path.name + ".lock")
                lock_path.parent.mkdir(parents=True, exist_ok=True)
                with open(lock_path, "a") as handle:
                    _lock_file(
                        handle,
                        None if deadline is None else max(0.0, deadline - time.monotonic()),
                    )
                    try:
                        self._apply_document()
                        yield
                    finally:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                self._txn_depth = 0
        finally:
            self._lock.release()

    def transaction(self, timeout: Optional[float] = None) -> Iterator[None]:
        """Public handle on the cross-process mailbox lock (see ``_transaction``)."""
        return self._transaction(timeout=timeout)

    # -- persistence -------------------------------------------------------

    def load(self) -> Dict[str, int]:
        """Load the mailbox from disk, dropping duplicate message ids.

        ``mailbox.json`` is a list per collection, so a restored, merged
        or hand-edited file can carry the same message id twice.  The
        in-memory collections are keyed by id and already collapse such
        duplicates, but the file itself never shrank back — every restart
        re-read the stale copies.  Loading now keeps the newest copy of
        each duplicated id (by ``created_at`` / ``stored_at``, later in
        the file winning ties) and, when anything was dropped, rewrites
        the file so the duplicates are gone for good.

        Returns how many duplicate entries were dropped per collection.
        """
        dropped = self._apply_document()
        if not sum(dropped.values()):
            return dropped
        log.info(
            "Found duplicate mailbox entries at %s: %s",
            self.path,
            {k: v for k, v in dropped.items() if v},
        )
        try:
            # Re-read and rewrite under the cross-process lock so a write
            # from another process between our read and write is not lost.
            with self._transaction():
                dropped = self._apply_document()
                if sum(dropped.values()):
                    self._write_deduplicated()
        except OSError as exc:
            # The in-memory state is already deduplicated; a read-only
            # mailbox must still load.
            log.warning("Could not persist mailbox dedupe at %s: %s", self.path, exc)
        return dropped

    def _apply_document(self) -> Dict[str, int]:
        """Read the mailbox file and replace the in-memory state with it."""
        empty: Dict[str, int] = {"outbox": 0, "held": 0, "pending_acks": 0}
        if self.path is None:
            return dict(empty)
        raw = read_json(self.path)
        if raw is None:
            return dict(empty)
        if not isinstance(raw, dict):
            log.warning("Ignoring mailbox at %s: unexpected format", self.path)
            return dict(empty)
        state, dropped, _, _ = self._parse_document(raw)
        with self._lock:
            self._outbox = state["outbox"]
            self._held = state["held"]
            self._seen = state["seen"]
            self._pending_acks = state["pending_acks"]
            self._dropped_held = state["dropped_held"]
            self._retracted = state["retracted"]
            self._sent = state["sent"]
            self._receipts = state["receipts"]
        log.debug(
            "Loaded mailbox from %s: %d outbox, %d held",
            self.path,
            len(self._outbox),
            len(self._held),
        )
        return dropped

    def _write_deduplicated(self) -> None:
        """Rewrite the file minus only the duplicate copies.

        Everything else in the on-disk document (invalid entries, full
        pending-ack lists) is preserved verbatim rather than being
        replaced by the filtered in-memory view.
        """
        raw = read_json(self.path)
        if not isinstance(raw, dict):
            return
        _, dropped, outbox_drop, held_drop = self._parse_document(raw)
        if not sum(dropped.values()):
            return
        cleaned = dict(raw)
        if isinstance(raw.get("outbox"), list):
            cleaned["outbox"] = [
                e for i, e in enumerate(raw["outbox"]) if i not in outbox_drop
            ]
        if isinstance(raw.get("held"), list):
            cleaned["held"] = [
                e for i, e in enumerate(raw["held"]) if i not in held_drop
            ]
        if isinstance(raw.get("pending_acks"), dict):
            cleaned_acks: Dict[str, Any] = {}
            for holder, ids in raw["pending_acks"].items():
                if isinstance(ids, list):
                    seen_ids: Set[str] = set()
                    unique: List[Any] = []
                    for mid in ids:
                        if isinstance(mid, str):
                            if mid in seen_ids:
                                continue
                            seen_ids.add(mid)
                        unique.append(mid)
                    cleaned_acks[holder] = unique
                else:
                    cleaned_acks[holder] = ids
            cleaned["pending_acks"] = cleaned_acks
        atomic_write_json(self.path, cleaned, mode=0o600)
        log.info("Dropped duplicate mailbox entries at %s", self.path)

    def _parse_document(
        self, raw: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, int], Set[int], Set[int]]:
        """Parse a raw mailbox document.

        Returns ``(state, dropped, outbox_drop, held_drop)`` where ``state``
        holds the parsed collections and the two index sets name the list
        positions of duplicate copies in the raw ``outbox`` / ``held`` lists.
        """
        empty: Dict[str, int] = {"outbox": 0, "held": 0, "pending_acks": 0}
        dropped: Dict[str, int] = dict(empty)
        outbox_drop: Set[int] = set()
        held_drop: Set[int] = set()
        outbox: Dict[str, OutboxEntry] = {}
        outbox_index: Dict[str, int] = {}
        entries = raw.get("outbox")
        if isinstance(entries, list):
            for index, entry in enumerate(entries):
                if not isinstance(entry, dict):
                    continue
                try:
                    parsed = OutboxEntry.from_dict(entry)
                except (TypeError, ValueError) as exc:
                    log.debug("Skipping invalid outbox entry: %s", exc)
                    continue
                existing = outbox.get(parsed.id)
                if existing is not None:
                    dropped["outbox"] += 1
                    if parsed.created_at < existing.created_at:
                        outbox_drop.add(index)
                        continue
                    outbox_drop.add(outbox_index[parsed.id])
                outbox[parsed.id] = parsed
                outbox_index[parsed.id] = index

        held: Dict[str, HeldEnvelope] = {}
        held_index: Dict[str, int] = {}
        envelopes = raw.get("held")
        if isinstance(envelopes, list):
            for index, entry in enumerate(envelopes):
                if not isinstance(entry, dict):
                    continue
                try:
                    parsed_held = HeldEnvelope.from_dict(entry)
                except (TypeError, ValueError) as exc:
                    log.debug("Skipping invalid held envelope: %s", exc)
                    continue
                existing_held = held.get(parsed_held.id)
                if existing_held is not None:
                    dropped["held"] += 1
                    if parsed_held.stored_at < existing_held.stored_at:
                        held_drop.add(index)
                        continue
                    held_drop.add(held_index[parsed_held.id])
                held[parsed_held.id] = parsed_held
                held_index[parsed_held.id] = index

        seen: Dict[str, float] = {}
        raw_seen = raw.get("seen")
        if isinstance(raw_seen, dict):
            seen = {
                str(k): float(v)
                for k, v in raw_seen.items()
                if isinstance(v, (int, float))
            }

        acks: Dict[str, List[str]] = {}
        raw_acks = raw.get("pending_acks")
        if isinstance(raw_acks, dict):
            for holder, ids in raw_acks.items():
                if isinstance(ids, list):
                    kept: List[str] = []
                    seen_ids: Set[str] = set()
                    for mid in ids:
                        if not isinstance(mid, str):
                            continue
                        if mid in seen_ids:
                            dropped["pending_acks"] += 1
                            continue
                        seen_ids.add(mid)
                        kept.append(mid)
                    acks[str(holder)] = kept[:100]

        dropped_held: Dict[str, float] = {}
        raw_dropped = raw.get("dropped_held")
        if isinstance(raw_dropped, dict):
            dropped_held = {
                str(k): float(v)
                for k, v in raw_dropped.items()
                if isinstance(v, (int, float))
            }

        retracted: Dict[str, float] = {}
        raw_retracted = raw.get("retracted")
        if isinstance(raw_retracted, dict):
            retracted = {
                str(k): float(v)
                for k, v in raw_retracted.items()
                if isinstance(v, (int, float))
            }

        sent: Dict[str, Dict[str, Any]] = {}
        raw_sent = raw.get("sent")
        if isinstance(raw_sent, dict):
            for message_id, record in raw_sent.items():
                if not isinstance(record, dict):
                    continue
                to_hex = record.get("to")
                sent_at = record.get("sent_at")
                read_at = record.get("read_at")
                text = record.get("text")
                if (
                    not isinstance(message_id, str)
                    or not isinstance(to_hex, str)
                    or len(to_hex) != 32
                    or not isinstance(sent_at, (int, float))
                    or (
                        read_at is not None
                        and not isinstance(read_at, (int, float))
                    )
                    or not isinstance(text, str)
                ):
                    continue
                try:
                    bytes.fromhex(to_hex)
                except ValueError:
                    continue
                sent[message_id] = {
                    "to": to_hex.lower(),
                    "sent_at": float(sent_at),
                    "read_at": float(read_at) if read_at is not None else None,
                    "text": text,
                }

        receipts: Dict[str, ReceiptEntry] = {}
        raw_receipts = raw.get("receipts")
        if isinstance(raw_receipts, list):
            for entry in raw_receipts:
                if not isinstance(entry, dict):
                    continue
                try:
                    parsed_receipt = ReceiptEntry.from_dict(entry)
                except (TypeError, ValueError) as exc:
                    log.debug("Skipping invalid receipt entry: %s", exc)
                    continue
                receipts[parsed_receipt.id] = parsed_receipt

        state: Dict[str, Any] = {
            "outbox": outbox,
            "held": held,
            "seen": seen,
            "pending_acks": acks,
            "dropped_held": dropped_held,
            "retracted": retracted,
            "sent": sent,
            "receipts": receipts,
        }
        return state, dropped, outbox_drop, held_drop

    def save(self) -> bool:
        if self.path is None:
            return False
        with self._lock:
            payload = {
                "version": MAILBOX_VERSION,
                "saved_at": time.time(),
                "outbox": [entry.to_dict() for entry in self._outbox.values()],
                "held": [env.to_dict() for env in self._held.values()],
                "seen": dict(self._seen),
                "sent": {
                    message_id: dict(record)
                    for message_id, record in self._sent.items()
                },
                "receipts": [
                    entry.to_dict() for entry in self._receipts.values()
                ],
                "dropped_held": dict(self._dropped_held),
                "retracted": dict(self._retracted),
                "pending_acks": {
                    holder: list(ids)
                    for holder, ids in self._pending_acks.items()
                    if ids
                },
            }
        atomic_write_json(self.path, payload, mode=0o600)
        return True

    def counts(self) -> Dict[str, int]:
        with self._lock:
            return {
                "outbox": len(self._outbox),
                "held": len(self._held),
                "seen": len(self._seen),
                "sent": len(self._sent),
                "receipts": len(self._receipts),
                "retracted": len(self._retracted),
            }

    # -- outbox ------------------------------------------------------------

    def enqueue(
        self,
        *,
        to: bytes,
        text: str,
        to_name: str = "",
        kind: Optional[str] = None,
        ttl: float = DEFAULT_TTL,
        message_id: Optional[str] = None,
        expires_at: Optional[float] = None,
        now: Optional[float] = None,
        group: Optional[Dict[str, Any]] = None,
        in_reply_to: Optional[Dict[str, Any]] = None,
    ) -> OutboxEntry:
        """Queue *text* for delivery to destination *to*.

        ``group`` (``{"id", "name", "creator", "members"}``) marks this entry as one
        leg of a group message, so a retry re-sends the group payload
        instead of a plain direct message.

        ``expires_at`` is a sender-set message expiry (epoch seconds):
        the entry expires at the earlier of the queue TTL and the
        message expiry, so a queued ephemeral message — and any sealed
        copy a holder deposits from it — is dropped at its deadline
        instead of lingering for the full TTL.
        """
        if len(to) != _DESTINATION_BYTES:
            raise MailboxError("destination hash must be 16 bytes")
        timestamp = now if now is not None else time.time()
        clean_text = normalize_message(text)
        clean_kind = normalize_message_kind(kind)
        entry_id = message_id or generate_message_id()
        group_id: Optional[str] = None
        group_name = ""
        group_creator = ""
        group_members: List[str] = []
        if group is not None:
            candidate_id = group.get("id")
            candidate_creator = group.get("creator")
            candidate_members = group.get("members")
            if not (
                isinstance(candidate_id, str)
                and len(candidate_id) == 32
                and isinstance(candidate_creator, str)
                and len(candidate_creator) == 32
                and isinstance(candidate_members, list)
                and all(
                    isinstance(m, str) and len(m) == 32 for m in candidate_members
                )
            ):
                raise MailboxError(
                    "group context needs an id, a creator and member hashes"
                )
            try:
                bytes.fromhex(candidate_id)
                bytes.fromhex(candidate_creator)
                for member in candidate_members:
                    bytes.fromhex(member)
            except ValueError as exc:
                raise MailboxError("group context has a non-hex hash") from exc
            group_id = candidate_id.lower()
            group_name = sanitize_group_name(group.get("name"))
            group_creator = candidate_creator.lower()
            group_members = [m.lower() for m in candidate_members]
        if expires_at is not None:
            if isinstance(expires_at, bool) or not isinstance(
                expires_at, (int, float)
            ):
                raise MailboxError("expires_at must be a finite number")
            try:
                expires_at = float(expires_at)
            except (OverflowError, ValueError) as exc:
                raise MailboxError("expires_at must be a finite number") from exc
            if not math.isfinite(expires_at):
                raise MailboxError("expires_at must be a finite number")
        reply_to = clean_in_reply_to(in_reply_to)
        with self._transaction():
            existing = self._outbox.get(entry_id)
            if existing is not None:
                return existing
            if len(self._outbox) >= MAX_OUTBOX:
                raise MailboxFull(f"outbox is full ({MAX_OUTBOX} messages)")
            entry_expires_at = timestamp + max(
                60.0, min(float(ttl), MAX_MAIL_TTL)
            )
            if expires_at is not None:
                entry_expires_at = min(entry_expires_at, expires_at)
            entry = OutboxEntry(
                id=entry_id,
                to=bytes(to),
                text=clean_text,
                created_at=timestamp,
                expires_at=entry_expires_at,
                to_name=sanitize_name(to_name, fallback="")
                if to_name
                else "",
                kind=clean_kind,
                group_id=group_id,
                group_name=group_name,
                group_creator=group_creator,
                group_members=group_members,
                message_expires_at=expires_at,
                in_reply_to=reply_to,
            )
            self._outbox[entry.id] = entry
            self.save()
            return entry

    def get_outbox(self, message_id: str) -> Optional[OutboxEntry]:
        with self._lock:
            return self._outbox.get(message_id)

    def resolve_outbox(self, ref: str) -> OutboxEntry:
        """Resolve a full message id or a unique prefix to its outbox entry.

        ``tau outbox`` shows only the first 8 characters of each id, so a
        short prefix is the usual way to name a queued message.
        """
        if not isinstance(ref, str) or not ref.strip():
            raise MailboxError("message reference must not be empty")
        value = ref.strip().lower()
        with self._lock:
            if len(value) == 32:
                entry = self._outbox.get(value)
                if entry is None:
                    raise MailboxError(f"unknown queued message: {ref!r}")
                return entry
            matches = [e for e in self._outbox.values() if e.id.startswith(value)]
        if not matches:
            raise MailboxError(f"unknown queued message: {ref!r}")
        if len(matches) > 1:
            raise MailboxError(
                f"{len(matches)} queued messages match prefix {ref!r}"
            )
        return matches[0]

    def outbox_list(
        self, to: Optional[bytes] = None, *, timeout: Optional[float] = None
    ) -> List[OutboxEntry]:
        """Outbox entries, oldest first.

        With ``timeout`` set, the wait for the in-process lock is bounded
        by it (a ``TimeoutError`` is raised once the budget is spent), so
        a caller with its own deadline — a delivery wait, say — is not
        held past it by a stalled lock holder.  With no timeout the wait
        is unbounded, as before.
        """
        if timeout is None:
            with self._lock:
                entries = list(self._outbox.values())
        else:
            acquired = self._lock.acquire(timeout=max(0.0, timeout))
            if not acquired:
                raise TimeoutError("mailbox lock is held by another thread")
            try:
                entries = list(self._outbox.values())
            finally:
                self._lock.release()
        if to is not None:
            entries = [e for e in entries if e.to == to]
        entries.sort(key=lambda e: e.created_at)
        return entries

    def due_outbox(
        self,
        to: Optional[bytes] = None,
        *,
        now: Optional[float] = None,
        timeout: Optional[float] = None,
    ) -> List[OutboxEntry]:
        """Outbox entries whose delivery backoff has elapsed, oldest first."""
        timestamp = now if now is not None else time.time()
        return [
            entry
            for entry in self.outbox_list(to=to, timeout=timeout)
            if entry.is_due(timestamp)
        ]

    def remove_outbox(self, message_id: str, *, timeout: Optional[float] = None) -> bool:
        with self._transaction(timeout=timeout):
            if message_id not in self._outbox:
                return False
            del self._outbox[message_id]
            self.save()
            return True

    def record_attempt(
        self,
        message_id: str,
        *,
        error: Optional[str] = None,
        now: Optional[float] = None,
        timeout: Optional[float] = None,
    ) -> None:
        with self._transaction(timeout=timeout):
            entry = self._outbox.get(message_id)
            if entry is None:
                return
            entry.attempts += 1
            entry.last_attempt_at = now if now is not None else time.time()
            entry.last_error = error[:200] if error else None
            self.save()

    def record_deposit(
        self,
        message_id: str,
        holder_hexhash: str,
        *,
        now: Optional[float] = None,
        timeout: Optional[float] = None,
    ) -> None:
        with self._transaction(timeout=timeout):
            entry = self._outbox.get(message_id)
            if entry is None:
                return
            entry.held_by[holder_hexhash] = (
                now if now is not None else time.time()
            )
            self.save()

    # -- sent messages and read receipts ------------------------------------

    def record_sent(
        self,
        message_id: str,
        *,
        to: bytes,
        text: str,
        sent_at: Optional[float] = None,
    ) -> None:
        """Remember a message this node sent, for read receipts.

        The record is what authorises a later receipt: only a reader
        matching ``to`` can mark this id read.  Re-recording an id
        (a retry) keeps the first send time and any read already
        recorded.
        """
        if len(to) != _DESTINATION_BYTES:
            raise MailboxError("destination hash must be 16 bytes")
        timestamp = sent_at if sent_at is not None else time.time()
        with self._transaction():
            existing = self._sent.get(message_id)
            if existing is not None:
                return
            if len(self._sent) >= MAX_SENT:
                # Messages still queued for delivery keep their record: a
                # receipt for them may arrive long after newer sends.
                evictable = sorted(
                    (
                        kv
                        for kv in self._sent.items()
                        if kv[0] not in self._outbox
                    ),
                    key=lambda kv: kv[1]["sent_at"],
                )
                for key, _ in evictable[: len(self._sent) - MAX_SENT + 1]:
                    del self._sent[key]
            self._sent[message_id] = {
                "to": to.hex(),
                "sent_at": float(timestamp),
                "read_at": None,
                "delivered_at": None,
                "text": normalize_message(text),
            }
            self.save()

    def record_delivered(
        self,
        message_id: str,
        *,
        now: Optional[float] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """Mark a sent message as having reached its recipient."""
        with self._transaction(timeout=timeout):
            record = self._sent.get(message_id)
            if record is None or record.get("delivered_at") is not None:
                return
            record["delivered_at"] = now if now is not None else time.time()
            self.save()

    def discard_sent(self, message_id: str) -> None:
        """Forget a sent record whose send was refused or never went out."""
        with self._transaction():
            if self._sent.pop(message_id, None) is not None:
                self.save()

    def sent_record(self, message_id: str) -> Optional[Dict[str, Any]]:
        """A copy of one sent record, or ``None`` when unknown."""
        with self._lock:
            record = self._sent.get(message_id)
            return dict(record) if record is not None else None

    def read_at(self, message_id: str) -> Optional[float]:
        """When *message_id* was read, or ``None`` (unknown, not unread)."""
        with self._lock:
            record = self._sent.get(message_id)
            return record["read_at"] if record is not None else None

    def record_read(
        self, message_id: str, read_at: float, *, reader_hash: str
    ) -> bool:
        """Mark *message_id* read at *read_at*, if *reader_hash* may.

        Returns ``True`` when the record changed.  ``False`` when the
        id was never sent by this node, or the reader is not the
        recorded recipient — a forged receipt for a message the sender
        never sent, or from a peer who is not the recipient, is
        rejected here.  Absence of a receipt stays unknown: a first
        read keeps the earliest time, never a later re-read.
        """
        if not math.isfinite(read_at):
            return False
        with self._transaction():
            record = self._sent.get(message_id)
            if record is None:
                return False
            if record["to"] != reader_hash.lower():
                return False
            previous = record["read_at"]
            if previous is None or read_at < previous:
                record["read_at"] = float(read_at)
                self.save()
                return True
            return False

    def sent_list(self) -> List[Dict[str, Any]]:
        """Sent messages, oldest first, with their read state."""
        with self._lock:
            entries = [
                {"id": message_id, **dict(record)}
                for message_id, record in self._sent.items()
            ]
        entries.sort(key=lambda entry: entry["sent_at"])
        return entries

    def queue_receipt(
        self,
        *,
        to: bytes,
        message_ids: List[str],
        read_at: float,
        now: Optional[float] = None,
    ) -> ReceiptEntry:
        """Queue a signed read receipt for a sender we cannot reach.

        The receipt rides the same store-and-forward machinery as the
        outbox: a direct ``/tau/read`` when the sender is reachable,
        sealed deposits with holders when it is not.
        """
        if len(to) != _DESTINATION_BYTES:
            raise MailboxError("destination hash must be 16 bytes")
        cleaned_ids = []
        for message_id in message_ids:
            if not isinstance(message_id, str) or not message_id or len(message_id) > 64:
                raise MailboxError("invalid receipt message id")
            if message_id not in cleaned_ids:
                cleaned_ids.append(message_id)
        if not cleaned_ids:
            raise MailboxError("read receipt must name at least one message")
        if len(cleaned_ids) > 100:
            raise MailboxError("read receipt names too many messages")
        if not math.isfinite(read_at):
            raise MailboxError("read_at must be a finite number")
        timestamp = now if now is not None else time.time()
        with self._transaction():
            if len(self._receipts) >= MAX_RECEIPTS:
                raise MailboxFull(f"receipt queue is full ({MAX_RECEIPTS} receipts)")
            entry = ReceiptEntry(
                id=generate_message_id(),
                to=bytes(to),
                message_ids=cleaned_ids,
                read_at=float(read_at),
                created_at=timestamp,
                expires_at=timestamp + max(60.0, min(float(DEFAULT_TTL), MAX_MAIL_TTL)),
            )
            self._receipts[entry.id] = entry
            self.save()
            return entry

    def get_receipt_outbox(self, receipt_id: str) -> Optional[ReceiptEntry]:
        with self._lock:
            return self._receipts.get(receipt_id)

    def receipt_outbox_list(
        self, to: Optional[bytes] = None
    ) -> List[ReceiptEntry]:
        with self._lock:
            entries = list(self._receipts.values())
        if to is not None:
            entries = [e for e in entries if e.to == to]
        entries.sort(key=lambda e: e.created_at)
        return entries

    def due_receipts(
        self, to: Optional[bytes] = None, *, now: Optional[float] = None
    ) -> List[ReceiptEntry]:
        """Queued receipts whose delivery backoff has elapsed."""
        timestamp = now if now is not None else time.time()
        return [
            entry
            for entry in self.receipt_outbox_list(to=to)
            if entry.is_due(timestamp)
        ]

    def remove_receipt_outbox(self, receipt_id: str) -> bool:
        with self._transaction():
            if receipt_id not in self._receipts:
                return False
            del self._receipts[receipt_id]
            self.save()
            return True

    def record_receipt_attempt(
        self, receipt_id: str, *, error: Optional[str] = None, now: Optional[float] = None
    ) -> None:
        with self._transaction():
            entry = self._receipts.get(receipt_id)
            if entry is None:
                return
            entry.attempts += 1
            entry.last_attempt_at = now if now is not None else time.time()
            self.save()

    def record_receipt_deposit(
        self, receipt_id: str, holder_hexhash: str, *, now: Optional[float] = None
    ) -> None:
        with self._transaction():
            entry = self._receipts.get(receipt_id)
            if entry is None:
                return
            entry.held_by[holder_hexhash] = (
                now if now is not None else time.time()
            )
            self.save()

    # -- held envelopes ------------------------------------------------------

    def hold(
        self,
        *,
        to: bytes,
        blob: bytes,
        message_id: str,
        expires_at: float,
        now: Optional[float] = None,
        depositor: Optional[bytes] = None,
        trusted: bool = False,
    ) -> bool:
        """Store a sealed envelope for *to*.

        Returns ``True`` when the envelope was newly stored, ``False``
        when an envelope with the same id was already held.

        *depositor* is the destination hash of the peer that asked this
        node to hold the envelope (``None`` for an unidentified link);
        it is recorded on the envelope and keys the per-sender fairness
        quota.  A *trusted* depositor — a long-standing known peer with
        a delivered history — gets the higher quota tier.  When a
        deposit would push its sender over quota, that sender's oldest
        envelopes are shed first, so one sender can crowd out only its
        own mail.  A deposit that cannot fit even on its own is refused
        before anything is shed.
        """
        if len(to) != _DESTINATION_BYTES:
            raise MailboxError("destination hash must be 16 bytes")
        if depositor is not None and len(depositor) != _DESTINATION_BYTES:
            raise MailboxError("depositor hash must be 16 bytes")
        if len(blob) > MAX_MAIL_BLOB_BYTES:
            raise MailboxError("sealed message is too large to hold")
        if not math.isfinite(expires_at):
            raise MailboxError("expiry must be a finite time")
        timestamp = now if now is not None else time.time()
        depositor_hex = depositor.hex() if depositor is not None else ""
        with self._transaction():
            if message_id in self._held:
                return False
            dropped_until = self._dropped_held.get(message_id)
            if dropped_until is not None:
                if dropped_until > timestamp:
                    # The owner dropped this envelope: a sender retrying
                    # its deposit must not resurrect it before it would
                    # have expired anyway.
                    return False
                del self._dropped_held[message_id]
            quota_count = (
                HELD_QUOTA_KNOWN_COUNT if trusted else HELD_QUOTA_UNKNOWN_COUNT
            )
            quota_bytes = (
                HELD_QUOTA_KNOWN_BYTES if trusted else HELD_QUOTA_UNKNOWN_BYTES
            )
            if 1 > quota_count or len(blob) > quota_bytes:
                # A single envelope that cannot fit even on its own is
                # refused before anything is shed: shedding first would
                # destroy the sender's existing mail for a deposit that
                # is rejected anyway.
                raise MailboxFull("mailbox is full for this sender")
            own = sorted(
                (e for e in self._held.values() if e.depositor == depositor_hex),
                key=lambda e: e.stored_at,
            )
            own_count = len(own)
            own_bytes = sum(len(e.blob) for e in own)
            victims: List[HeldEnvelope] = []
            while own and (
                own_count + 1 > quota_count
                or own_bytes + len(blob) > quota_bytes
            ):
                victim = own.pop(0)
                victims.append(victim)
                own_count -= 1
                own_bytes -= len(victim.blob)
            # Plan first, then apply: every refusal happens before any
            # envelope is deleted, so a rejected deposit hides no mail.
            # Both caps net the planned victims: a sender whose own
            # oldest envelopes are shed frees those slots, including
            # slots for this recipient.
            per_recipient = sum(1 for e in self._held.values() if e.to == to)
            freed_for_recipient = sum(1 for v in victims if v.to == to)
            if per_recipient - freed_for_recipient >= MAX_HELD_PER_RECIPIENT:
                raise MailboxFull(
                    f"mailbox is full for this recipient ({MAX_HELD_PER_RECIPIENT})"
                )
            if len(self._held) - len(victims) >= MAX_HELD_TOTAL:
                raise MailboxFull(f"mailbox is full ({MAX_HELD_TOTAL} envelopes)")
            for victim in victims:
                del self._held[victim.id]
            if victims:
                log.info(
                    "Shed %d held envelope(s) from %s to stay within quota",
                    len(victims),
                    depositor_hex or "unknown sender",
                )
            self._held[message_id] = HeldEnvelope(
                id=message_id,
                to=bytes(to),
                blob=bytes(blob),
                stored_at=timestamp,
                expires_at=min(float(expires_at), timestamp + MAX_MAIL_TTL),
                depositor=depositor_hex,
            )
            self.save()
            return True

    def held_for(self, to: bytes) -> List[HeldEnvelope]:
        with self._lock:
            envelopes = [e for e in self._held.values() if e.to == to]
        envelopes.sort(key=lambda e: e.stored_at)
        return envelopes

    def held_list(self) -> List[HeldEnvelope]:
        """All held envelopes, oldest first."""
        with self._lock:
            envelopes = list(self._held.values())
        envelopes.sort(key=lambda e: e.stored_at)
        return envelopes

    def held_usage(self) -> List[Dict[str, Any]]:
        """Held-mail usage per depositing peer, busiest first.

        Each entry is ``{"sender": <hex hash or "">, "count": n,
        "bytes": b}``; ``sender == ""`` is the shared bucket for
        deposits that arrived over an unidentified link.  Operators use
        it to see quota pressure (``tau outbox held``).
        """
        with self._lock:
            usage: Dict[str, Dict[str, Any]] = {}
            for envelope in self._held.values():
                entry = usage.setdefault(
                    envelope.depositor,
                    {"sender": envelope.depositor, "count": 0, "bytes": 0},
                )
                entry["count"] += 1
                entry["bytes"] += len(envelope.blob)
        return sorted(
            usage.values(),
            key=lambda row: (row["count"], row["bytes"]),
            reverse=True,
        )

    def resolve_held(self, ref: str) -> HeldEnvelope:
        """Resolve a full message id or a unique prefix to its held envelope.

        ``tau outbox held`` shows only the first 8 characters of each id, so a
        short prefix is the usual way to name a held envelope. Matching is
        case-insensitive, and a 32-character reference is tried as an exact
        id first and then as a prefix: a remote sender may store an id in
        any case, and mail-store ids can be up to 64 characters long.
        """
        if not isinstance(ref, str) or not ref.strip():
            raise MailboxError("message reference must not be empty")
        value = ref.strip()
        lowered = value.lower()
        with self._lock:
            if len(value) == 32:
                envelope = self._held.get(value)
                if envelope is not None:
                    return envelope
            matches = [
                e for e in self._held.values() if e.id.lower().startswith(lowered)
            ]
        if not matches:
            raise MailboxError(f"unknown held message: {ref!r}")
        if len(matches) > 1:
            raise MailboxError(
                f"{len(matches)} held messages match prefix {ref!r}"
            )
        return matches[0]

    def drop_held(
        self, message_id: str, *, to: Optional[bytes] = None, tombstone: bool = False
    ) -> bool:
        """Drop a held envelope.

        With ``tombstone=True`` (the owner's explicit drop), the id is
        remembered until the envelope would have expired, so a sender
        retrying its deposit cannot resurrect it; protocol-driven drops
        (fetch acks, local delivery) leave no marker.
        """
        with self._transaction():
            envelope = self._held.get(message_id)
            if envelope is None:
                return False
            if to is not None and envelope.to != to:
                return False
            del self._held[message_id]
            if tombstone:
                self._dropped_held[message_id] = envelope.expires_at
            self.save()
            return True

    # -- dedupe --------------------------------------------------------------

    def is_seen(self, message_id: str) -> bool:
        with self._lock:
            return message_id in self._seen

    def mark_seen(self, message_id: str, *, now: Optional[float] = None) -> None:
        with self._transaction():
            self._seen[message_id] = now if now is not None else time.time()
            if len(self._seen) > MAX_SEEN:
                oldest = sorted(self._seen.items(), key=lambda kv: kv[1])
                for key, _ in oldest[: len(self._seen) - MAX_SEEN]:
                    del self._seen[key]
            self.save()

    # -- retraction markers -------------------------------------------------

    def record_retraction(
        self, sender_hash: str, message_id: str, *, now: Optional[float] = None
    ) -> None:
        """Remember that *sender_hash* retracted *message_id*.

        The marker lives for the possible delivery lifetime of the
        message (RETRACTED_RETENTION), independently of the bounded
        seen-id cache, so a relayed copy arriving after the retract —
        or after the id leaves the seen cache — is still dropped.
        Recording the same retraction again refreshes the marker.
        """
        timestamp = now if now is not None else time.time()
        with self._transaction():
            self._retracted[_retraction_key(sender_hash, message_id)] = (
                timestamp + RETRACTED_RETENTION
            )
            self.save()

    def is_retracted(
        self, sender_hash: str, message_id: str, *, now: Optional[float] = None
    ) -> bool:
        """True while a live retraction marker covers this message.

        An expired marker reads as absent even before :meth:`prune`
        sweeps it away; a marker for another sender never matches.
        """
        with self._lock:
            until = self._retracted.get(_retraction_key(sender_hash, message_id))
            if until is None:
                return False
            timestamp = now if now is not None else time.time()
            return timestamp < until

    # -- pending fetch acknowledgements --------------------------------------

    def add_pending_acks(self, holder_hexhash: str, message_ids: List[str]) -> None:
        if not message_ids:
            return
        with self._transaction():
            current = self._pending_acks.setdefault(holder_hexhash, [])
            for mid in message_ids:
                if mid not in current:
                    current.append(mid)
            self._pending_acks[holder_hexhash] = current[:100]
            self.save()

    def take_pending_acks(self, holder_hexhash: str) -> List[str]:
        with self._transaction():
            ids = self._pending_acks.pop(holder_hexhash, [])
            if ids:
                self.save()
            return list(ids)

    def pending_ack_holders(self) -> List[str]:
        """Hex destination hashes with mail acknowledgements still owed."""
        with self._lock:
            return [holder for holder, ids in self._pending_acks.items() if ids]

    # -- maintenance -----------------------------------------------------------

    def prune(
        self, *, now: Optional[float] = None, timeout: Optional[float] = None
    ) -> Dict[str, int]:
        """Drop expired outbox entries, held envelopes and stale seen ids.

        With ``timeout`` set, the wait for the mailbox lock is bounded
        by it (see ``_transaction``); with no timeout it is unbounded,
        as before.
        """
        timestamp = now if now is not None else time.time()
        with self._transaction(timeout=timeout):
            dead_outbox = [
                key
                for key, entry in self._outbox.items()
                if entry.expires_at <= timestamp
            ]
            for key in dead_outbox:
                del self._outbox[key]
            dead_held = [
                key
                for key, env in self._held.items()
                if env.expires_at <= timestamp
            ]
            for key in dead_held:
                del self._held[key]
            dead_seen = [
                key
                for key, seen_at in self._seen.items()
                if timestamp - seen_at > SEEN_RETENTION
            ]
            for key in dead_seen:
                del self._seen[key]
            dead_sent = [
                key
                for key, record in self._sent.items()
                if timestamp - record["sent_at"] > SENT_RETENTION
            ]
            for key in dead_sent:
                del self._sent[key]
            dead_receipts = [
                key
                for key, entry in self._receipts.items()
                if entry.expires_at <= timestamp
            ]
            for key in dead_receipts:
                del self._receipts[key]
            dead_dropped = [
                key
                for key, until in self._dropped_held.items()
                if until <= timestamp
            ]
            for key in dead_dropped:
                del self._dropped_held[key]
            dead_retracted = [
                key
                for key, until in self._retracted.items()
                if until <= timestamp
            ]
            for key in dead_retracted:
                del self._retracted[key]
            if (
                dead_outbox
                or dead_held
                or dead_seen
                or dead_sent
                or dead_receipts
                or dead_dropped
                or dead_retracted
            ):
                self.save()
            return {
                "outbox": len(dead_outbox),
                "held": len(dead_held),
                "seen": len(dead_seen),
                "sent": len(dead_sent),
                "receipts": len(dead_receipts),
                "dropped_held": len(dead_dropped),
                "retracted": len(dead_retracted),
            }


# -- sealed envelopes --------------------------------------------------------


def seal_message(
    sender_identity: RNS.Identity,
    *,
    sender_name: str,
    recipient_public_key: bytes,
    message_id: str,
    text: str,
    sent_at: float,
    group: Optional[Dict[str, Any]] = None,
    expires_at: Optional[float] = None,
    in_reply_to: Optional[Dict[str, Any]] = None,
    kind: Optional[str] = None,
) -> bytes:
    """Encrypt *text* to a recipient and sign it as the sender.

    The plaintext envelope carries the sender's public key and a signature
    over every other field, so the recipient can authenticate the message
    without trusting whichever peer happened to hold it.  ``group``
    (``{"id", "name", "creator", "members"}``) marks the envelope as one
    leg of a group message; it is covered by the same signature.
    without trusting whichever peer happened to hold it.  ``expires_at``
    (when given) travels inside that signed envelope, so a holder cannot
    extend a disappearing message's deadline.
    """
    if len(recipient_public_key) != _PUBLIC_KEY_BYTES:
        raise MailboxError("recipient public key must be 64 bytes")
    sender_hash = tau_destination_hash(sender_identity)
    inner: Dict[str, Any] = {
        "type": "sealed_message",
        "v": 1,
        "id": message_id,
        "from": sender_hash.hex(),
        "from_name": sanitize_name(sender_name),
        "from_key": sender_identity.get_public_key().hex(),
        "text": normalize_message(text),
        "kind": normalize_message_kind(kind),
        "sent_at": float(sent_at),
    }
    if group is not None:
        group_id = group.get("id")
        group_creator = group.get("creator")
        group_members = group.get("members")
        if not (
            isinstance(group_id, str)
            and len(group_id) == 32
            and isinstance(group_creator, str)
            and len(group_creator) == 32
            and isinstance(group_members, list)
            and all(isinstance(m, str) and len(m) == 32 for m in group_members)
        ):
            raise MailboxError(
                "group context needs an id, a creator and member hashes"
            )
        try:
            bytes.fromhex(group_id)
            bytes.fromhex(group_creator)
            for member in group_members:
                bytes.fromhex(member)
        except ValueError as exc:
            raise MailboxError("group context has a non-hex hash") from exc
        inner["group"] = {
            "id": group_id.lower(),
            "name": sanitize_group_name(group.get("name")),
            "creator": group_creator.lower(),
            "members": [m.lower() for m in group_members],
        }
    if expires_at is not None:
        if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)):
            raise MailboxError("expires_at must be a finite number")
        try:
            expires_at = float(expires_at)
        except (OverflowError, ValueError) as exc:
            raise MailboxError("expires_at must be a finite number") from exc
        if not math.isfinite(expires_at):
            raise MailboxError("expires_at must be a finite number")
        inner["expires_at"] = expires_at
    reply_to = clean_in_reply_to(in_reply_to)
    if reply_to is not None:
        inner["in_reply_to"] = reply_to
    canonical = encode(inner)
    signature = sender_identity.sign(canonical)
    if signature is None:  # pragma: no cover - identity without private keys
        raise MailboxError("sender identity cannot sign")
    inner["sig"] = base64.b64encode(signature).decode("ascii")
    plaintext = encode(inner, max_bytes=MAX_MAIL_BLOB_BYTES)

    recipient = RNS.Identity(create_keys=False)
    if not load_public_key(recipient, recipient_public_key):
        raise MailboxError("could not load recipient public key")
    blob = recipient.encrypt(plaintext)
    if not blob:
        raise MailboxError("could not encrypt sealed message")
    if len(blob) > MAX_MAIL_BLOB_BYTES:
        raise MailboxError("sealed message is too large to store or forward")
    return blob


def open_message(recipient_identity: RNS.Identity, blob: bytes) -> Dict[str, Any]:
    """Decrypt and authenticate a sealed message addressed to this node."""
    kind, message = open_sealed_envelope(recipient_identity, blob)
    if kind != "sealed_message":
        raise MailboxError("unexpected sealed payload type")
    return message


def seal_receipt(
    sender_identity: RNS.Identity,
    *,
    sender_name: str,
    recipient_public_key: bytes,
    receipt_id: str,
    message_ids: List[str],
    read_at: float,
) -> bytes:
    """Encrypt a signed read receipt to the original sender.

    Same envelope shape as :func:`seal_message` — the plaintext
    carries the reader's public key and a signature over every other
    field — but the payload names message ids instead of carrying
    text.  A holder can neither read nor forge what it carries; the
    recipient verifies the signature and then checks each id against
    the messages it actually sent (see ``Mailbox.record_read``).
    """
    if len(recipient_public_key) != _PUBLIC_KEY_BYTES:
        raise MailboxError("recipient public key must be 64 bytes")
    cleaned_ids = []
    for message_id in message_ids:
        if not isinstance(message_id, str) or not message_id or len(message_id) > 64:
            raise MailboxError("sealed receipt has an invalid message id")
        if message_id not in cleaned_ids:
            cleaned_ids.append(message_id)
    if not cleaned_ids:
        raise MailboxError("sealed receipt must name at least one message")
    if not math.isfinite(read_at):
        raise MailboxError("sealed receipt read_at must be a finite number")
    sender_hash = tau_destination_hash(sender_identity)
    inner: Dict[str, Any] = {
        "type": "sealed_receipt",
        "v": 1,
        "id": receipt_id,
        "from": sender_hash.hex(),
        "from_name": sanitize_name(sender_name),
        "from_key": sender_identity.get_public_key().hex(),
        "ids": cleaned_ids,
        "read_at": float(read_at),
    }
    canonical = encode(inner)
    signature = sender_identity.sign(canonical)
    if signature is None:  # pragma: no cover - identity without private keys
        raise MailboxError("sender identity cannot sign")
    inner["sig"] = base64.b64encode(signature).decode("ascii")
    plaintext = encode(inner, max_bytes=MAX_MAIL_BLOB_BYTES)

    recipient = RNS.Identity(create_keys=False)
    if not load_public_key(recipient, recipient_public_key):
        raise MailboxError("could not load recipient public key")
    blob = recipient.encrypt(plaintext)
    if not blob:
        raise MailboxError("could not encrypt sealed receipt")
    if len(blob) > MAX_MAIL_BLOB_BYTES:
        raise MailboxError("sealed receipt is too large to store or forward")
    return blob


def open_receipt(recipient_identity: RNS.Identity, blob: bytes) -> Dict[str, Any]:
    """Decrypt and authenticate a sealed read receipt addressed to this node."""
    kind, receipt = open_sealed_envelope(recipient_identity, blob)
    if kind != "sealed_receipt":
        raise MailboxError("unexpected sealed payload type")
    return receipt


def open_sealed_envelope(
    recipient_identity: RNS.Identity, blob: bytes
) -> Tuple[str, Dict[str, Any]]:
    """Decrypt and authenticate any sealed envelope addressed to this node.

    Returns ``(kind, payload)`` where kind is ``"sealed_message"`` or
    ``"sealed_receipt"`` and the payload is the validated dict that
    :func:`open_message` / :func:`open_receipt` return.  An envelope
    of any other type raises :class:`MailboxError`.
    """
    payload = _open_sealed(recipient_identity, blob)
    kind = payload.get("type")
    if kind == "sealed_message":
        return kind, _validated_sealed_message(payload)
    if kind == "sealed_receipt":
        return kind, _validated_sealed_receipt(payload)
    raise MailboxError("unexpected sealed payload type")


def _open_sealed(recipient_identity: RNS.Identity, blob: bytes) -> Dict[str, Any]:
    """Decrypt a sealed envelope and verify its signature.

    Shared by sealed messages and sealed receipts: the plaintext
    carries the sender's public key and a signature over every other
    field, so the recipient can authenticate the payload without
    trusting whichever peer happened to hold it.
    """
    try:
        plaintext = recipient_identity.decrypt(bytes(blob))
    except Exception as exc:
        raise MailboxError(f"could not decrypt sealed message: {exc}") from exc
    if not plaintext:
        raise MailboxError("sealed message was not addressed to this identity")
    try:
        payload = decode(plaintext, max_bytes=MAX_MAIL_BLOB_BYTES)
    except ProtocolError as exc:
        raise MailboxError(f"invalid sealed message: {exc}") from exc

    signature_text = payload.get("sig")
    if not isinstance(signature_text, str):
        raise MailboxError("sealed message is missing its signature")
    try:
        signature = base64.b64decode(signature_text, validate=True)
    except ValueError as exc:
        raise MailboxError("sealed message signature is not valid base64") from exc

    from_key = payload.get("from_key")
    if not isinstance(from_key, str):
        raise MailboxError("sealed message is missing its sender key")
    try:
        sender_key = bytes.fromhex(from_key)
    except ValueError as exc:
        raise MailboxError("sealed message sender key is not hex") from exc
    if len(sender_key) != _PUBLIC_KEY_BYTES:
        raise MailboxError("sealed message sender key has the wrong length")

    sender = RNS.Identity(create_keys=False)
    if not load_public_key(sender, sender_key):
        raise MailboxError("could not load the sender public key")
    claimed_from = payload.get("from")
    if (
        not isinstance(claimed_from, str)
        or tau_destination_hash(sender).hex() != claimed_from.lower()
    ):
        raise MailboxError("sender key does not match the claimed sender")

    unsigned = {k: v for k, v in payload.items() if k != "sig"}
    try:
        verified = sender.validate(signature, encode(unsigned))
    except Exception as exc:
        raise MailboxError(f"could not verify sealed message: {exc}") from exc
    if not verified:
        raise MailboxError("sealed message signature does not verify")
    return payload


def _validated_sealed_message(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Field checks for a verified sealed-message payload."""
    message_id = payload.get("id")
    if not isinstance(message_id, str) or not message_id or len(message_id) > 64:
        raise MailboxError("sealed message has an invalid id")
    try:
        text = normalize_message(payload.get("text"))
    except ProtocolError as exc:
        raise MailboxError(f"sealed message text is invalid: {exc}") from exc
    try:
        kind = kind_from_wire(payload)
    except ProtocolError as exc:
        raise MailboxError(f"sealed message kind is invalid: {exc}") from exc
    sent_at = payload.get("sent_at")
    expires_at = payload.get("expires_at")
    if expires_at is not None:
        if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)):
            raise MailboxError("sealed message expires_at is invalid")
        try:
            expires_at = float(expires_at)
        except (OverflowError, ValueError) as exc:
            raise MailboxError("sealed message expires_at is invalid") from exc
        if not math.isfinite(expires_at):
            raise MailboxError("sealed message expires_at is invalid")
    try:
        in_reply_to = clean_in_reply_to(payload.get("in_reply_to"))
    except ProtocolError as exc:
        raise MailboxError(f"sealed message reply is invalid: {exc}") from exc
    result: Dict[str, Any] = {
        "id": message_id,
        "from": payload["from"].lower(),
        "from_name": sanitize_name(payload.get("from_name")),
        "text": text,
        "kind": kind,
        "sent_at": float(sent_at) if isinstance(sent_at, (int, float)) else None,
        "expires_at": expires_at,
        "in_reply_to": in_reply_to,
    }
    group_raw = payload.get("group")
    if group_raw is not None:
        if not isinstance(group_raw, dict):
            raise MailboxError("sealed message group must be an object")
        group_id = group_raw.get("id")
        group_creator = group_raw.get("creator")
        group_members = group_raw.get("members")
        if not (
            isinstance(group_id, str)
            and len(group_id) == 32
            and isinstance(group_creator, str)
            and len(group_creator) == 32
            and isinstance(group_members, list)
            and all(isinstance(m, str) and len(m) == 32 for m in group_members)
        ):
            raise MailboxError("sealed message group is invalid")
        try:
            bytes.fromhex(group_id)
            bytes.fromhex(group_creator)
            for member in group_members:
                bytes.fromhex(member)
        except ValueError as exc:
            raise MailboxError("sealed message group has a non-hex hash") from exc
        result["group"] = {
            "id": group_id.lower(),
            "name": sanitize_group_name(group_raw.get("name")),
            "creator": group_creator.lower(),
            "members": [m.lower() for m in group_members],
        }
    return result


def _validated_sealed_receipt(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Field checks for a verified sealed-receipt payload."""
    receipt_id = payload.get("id")
    if not isinstance(receipt_id, str) or not receipt_id or len(receipt_id) > 64:
        raise MailboxError("sealed receipt has an invalid id")
    ids = payload.get("ids")
    if not isinstance(ids, list) or not ids:
        raise MailboxError("sealed receipt must name at least one message")
    cleaned_ids = []
    for message_id in ids:
        if not isinstance(message_id, str) or not message_id or len(message_id) > 64:
            raise MailboxError("sealed receipt has an invalid message id")
        if message_id not in cleaned_ids:
            cleaned_ids.append(message_id)
    read_at = payload.get("read_at")
    if not isinstance(read_at, (int, float)) or not math.isfinite(float(read_at)):
        raise MailboxError("sealed receipt has an invalid read_at")
    return {
        "id": receipt_id,
        "from": payload["from"].lower(),
        "from_name": sanitize_name(payload.get("from_name")),
        "ids": cleaned_ids,
        "read_at": float(read_at),
    }
