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
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

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
    MAX_MAIL_BLOB_BYTES,
    MAX_MAIL_TTL,
    ProtocolError,
    decode,
    encode,
    generate_message_id,
    normalize_message,
    sanitize_name,
)
from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

MAILBOX_VERSION = 1
DEFAULT_TTL = 7 * 24 * 60 * 60
SEEN_RETENTION = 30 * 24 * 60 * 60
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

# Direct-delivery retry pacing: after a failed attempt, wait 60s, then
# double after each further failure, up to an hour.  An announce from the
# recipient bypasses the wait (see TauNode._mail_round_for_peer).
DELIVERY_BACKOFF_BASE_SECONDS = 60.0
DELIVERY_BACKOFF_MAX_SECONDS = 3600.0

_DESTINATION_BYTES = 16
_PUBLIC_KEY_BYTES = 64


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
    attempts: int = 0
    last_attempt_at: Optional[float] = None
    last_error: Optional[str] = None
    held_by: Dict[str, float] = field(default_factory=dict)

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
        return {
            "id": self.id,
            "to": self.to_hex,
            "to_name": self.to_name,
            "text": self.text,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "attempts": self.attempts,
            "last_attempt_at": self.last_attempt_at,
            "last_error": self.last_error,
            "next_attempt_at": self.next_attempt_at,
            "held_by": dict(self.held_by),
        }

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
        held_by = raw.get("held_by")
        if not isinstance(held_by, dict):
            held_by = {}
        attempts = raw.get("attempts")
        return cls(
            id=message_id,
            to=destination,
            text=normalize_message(raw.get("text")),
            created_at=float(created_at),
            expires_at=float(expires_at),
            to_name=sanitize_name(raw.get("to_name"), fallback="")
            if isinstance(raw.get("to_name"), str)
            else "",
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
        return cls(
            id=message_id,
            to=destination,
            blob=blob,
            stored_at=float(stored_at),
            expires_at=float(expires_at),
        )


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
        if self.path is not None:
            self.load()

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        """Serialise a read-modify-write of the mailbox across processes.

        ``tau serve`` and one-shot commands such as ``tau send`` share the
        same ``mailbox.json``.  Each mutation takes an exclusive ``flock`` on
        a sidecar lock file, reloads the latest on-disk state, and then
        applies its change, so a process never overwrites another's writes.
        """
        with self._lock:
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
                        self.load()
                    yield
                    return
                lock_path = self.path.with_name(self.path.name + ".lock")
                lock_path.parent.mkdir(parents=True, exist_ok=True)
                with open(lock_path, "a") as handle:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                    try:
                        self.load()
                        yield
                    finally:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                self._txn_depth = 0

    def transaction(self) -> Iterator[None]:
        """Public handle on the cross-process mailbox lock (see ``_transaction``)."""
        return self._transaction()

    # -- persistence -------------------------------------------------------

    def load(self) -> None:
        if self.path is None:
            return
        raw = read_json(self.path)
        if raw is None:
            return
        if not isinstance(raw, dict):
            log.warning("Ignoring mailbox at %s: unexpected format", self.path)
            return

        outbox: Dict[str, OutboxEntry] = {}
        entries = raw.get("outbox")
        if isinstance(entries, list):
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                try:
                    parsed = OutboxEntry.from_dict(entry)
                except (TypeError, ValueError) as exc:
                    log.debug("Skipping invalid outbox entry: %s", exc)
                    continue
                outbox[parsed.id] = parsed

        held: Dict[str, HeldEnvelope] = {}
        envelopes = raw.get("held")
        if isinstance(envelopes, list):
            for entry in envelopes:
                if not isinstance(entry, dict):
                    continue
                try:
                    parsed_held = HeldEnvelope.from_dict(entry)
                except (TypeError, ValueError) as exc:
                    log.debug("Skipping invalid held envelope: %s", exc)
                    continue
                held[parsed_held.id] = parsed_held

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
                    acks[str(holder)] = [
                        mid for mid in ids if isinstance(mid, str)
                    ][:100]

        dropped_held: Dict[str, float] = {}
        raw_dropped = raw.get("dropped_held")
        if isinstance(raw_dropped, dict):
            dropped_held = {
                str(k): float(v)
                for k, v in raw_dropped.items()
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

        with self._lock:
            self._outbox = outbox
            self._held = held
            self._seen = seen
            self._sent = sent
            self._receipts = receipts
            self._pending_acks = acks
            self._dropped_held = dropped_held
        log.debug(
            "Loaded mailbox from %s: %d outbox, %d held",
            self.path,
            len(outbox),
            len(held),
        )

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
            }

    # -- outbox ------------------------------------------------------------

    def enqueue(
        self,
        *,
        to: bytes,
        text: str,
        to_name: str = "",
        ttl: float = DEFAULT_TTL,
        message_id: Optional[str] = None,
        now: Optional[float] = None,
    ) -> OutboxEntry:
        """Queue *text* for delivery to destination *to*."""
        if len(to) != _DESTINATION_BYTES:
            raise MailboxError("destination hash must be 16 bytes")
        timestamp = now if now is not None else time.time()
        clean_text = normalize_message(text)
        entry_id = message_id or generate_message_id()
        with self._transaction():
            existing = self._outbox.get(entry_id)
            if existing is not None:
                return existing
            if len(self._outbox) >= MAX_OUTBOX:
                raise MailboxFull(f"outbox is full ({MAX_OUTBOX} messages)")
            entry = OutboxEntry(
                id=entry_id,
                to=bytes(to),
                text=clean_text,
                created_at=timestamp,
                expires_at=timestamp + max(60.0, min(float(ttl), MAX_MAIL_TTL)),
                to_name=sanitize_name(to_name, fallback="")
                if to_name
                else "",
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

    def outbox_list(self, to: Optional[bytes] = None) -> List[OutboxEntry]:
        with self._lock:
            entries = list(self._outbox.values())
        if to is not None:
            entries = [e for e in entries if e.to == to]
        entries.sort(key=lambda e: e.created_at)
        return entries

    def due_outbox(
        self, to: Optional[bytes] = None, *, now: Optional[float] = None
    ) -> List[OutboxEntry]:
        """Outbox entries whose delivery backoff has elapsed, oldest first."""
        timestamp = now if now is not None else time.time()
        return [
            entry for entry in self.outbox_list(to=to) if entry.is_due(timestamp)
        ]

    def remove_outbox(self, message_id: str) -> bool:
        with self._transaction():
            if message_id not in self._outbox:
                return False
            del self._outbox[message_id]
            self.save()
            return True

    def record_attempt(
        self, message_id: str, *, error: Optional[str] = None, now: Optional[float] = None
    ) -> None:
        with self._transaction():
            entry = self._outbox.get(message_id)
            if entry is None:
                return
            entry.attempts += 1
            entry.last_attempt_at = now if now is not None else time.time()
            entry.last_error = error[:200] if error else None
            self.save()

    def record_deposit(
        self, message_id: str, holder_hexhash: str, *, now: Optional[float] = None
    ) -> None:
        with self._transaction():
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
                oldest = sorted(
                    self._sent.items(), key=lambda kv: kv[1]["sent_at"]
                )
                for key, _ in oldest[: len(self._sent) - MAX_SENT + 1]:
                    del self._sent[key]
            self._sent[message_id] = {
                "to": to.hex(),
                "sent_at": float(timestamp),
                "read_at": None,
                "text": normalize_message(text),
            }
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
    ) -> bool:
        """Store a sealed envelope for *to*.

        Returns ``True`` when the envelope was newly stored, ``False``
        when an envelope with the same id was already held.
        """
        if len(to) != _DESTINATION_BYTES:
            raise MailboxError("destination hash must be 16 bytes")
        if len(blob) > MAX_MAIL_BLOB_BYTES:
            raise MailboxError("sealed message is too large to hold")
        if not math.isfinite(expires_at):
            raise MailboxError("expiry must be a finite time")
        timestamp = now if now is not None else time.time()
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
            if len(self._held) >= MAX_HELD_TOTAL:
                raise MailboxFull(f"mailbox is full ({MAX_HELD_TOTAL} envelopes)")
            per_recipient = sum(1 for e in self._held.values() if e.to == to)
            if per_recipient >= MAX_HELD_PER_RECIPIENT:
                raise MailboxFull(
                    f"mailbox is full for this recipient ({MAX_HELD_PER_RECIPIENT})"
                )
            self._held[message_id] = HeldEnvelope(
                id=message_id,
                to=bytes(to),
                blob=bytes(blob),
                stored_at=timestamp,
                expires_at=min(float(expires_at), timestamp + MAX_MAIL_TTL),
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

    def prune(self, *, now: Optional[float] = None) -> Dict[str, int]:
        """Drop expired outbox entries, held envelopes and stale seen ids."""
        timestamp = now if now is not None else time.time()
        with self._transaction():
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
            if (
                dead_outbox
                or dead_held
                or dead_seen
                or dead_sent
                or dead_receipts
                or dead_dropped
            ):
                self.save()
            return {
                "outbox": len(dead_outbox),
                "held": len(dead_held),
                "seen": len(dead_seen),
                "sent": len(dead_sent),
                "receipts": len(dead_receipts),
                "dropped_held": len(dead_dropped),
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
) -> bytes:
    """Encrypt *text* to a recipient and sign it as the sender.

    The plaintext envelope carries the sender's public key and a signature
    over every other field, so the recipient can authenticate the message
    without trusting whichever peer happened to hold it.
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
        "sent_at": float(sent_at),
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
    sent_at = payload.get("sent_at")
    return {
        "id": message_id,
        "from": payload["from"].lower(),
        "from_name": sanitize_name(payload.get("from_name")),
        "text": text,
        "sent_at": float(sent_at) if isinstance(sent_at, (int, float)) else None,
    }


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
