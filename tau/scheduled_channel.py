"""Scheduled channel messages: queue a channel message to send later.

The channel analogue of a scheduled Tan post (``tau tan post
--delay``): ``tau channel send NAME TEXT --delay 5m`` stores the
message instead of publishing it, and a node serving with
``--channels`` publishes due messages on its mail-loop pass.  A
message whose publish time has come goes out at the next pass,
about every 45 seconds.

The queue lives in the data directory
(``scheduled-channel-messages.json``) so scheduled messages survive
restarts.  A publish that fails keeps the entry queued and retries
with exponential backoff (up to a limit) instead of dropping the
message — the same failure direction as the outbox: never lose a
queued message silently.  Cancelling removes the entry before it
goes out.

Polls queue the same way (``tau channel poll --delay 5m``): a queued
entry carries ``kind`` ``"poll"`` with the question as its text and
the fixed options alongside it, and the publish pass signs the poll
at publish time.  Queue files written before polls could be
scheduled carry no ``kind`` and load as messages, exactly as before.

A location pin queues the same way (``tau channel send --location
... --delay 5m``): the queued entry carries the pin and the publish
pass attaches it when the message goes out, so the published message
is the same one an immediate send would have made.  Queue files
written before pins could be scheduled carry no ``location`` and
load with no pin, exactly as before.

A shared contact card queues the same way (``tau channel send
--contact <peer> --delay 5m``): the queued entry carries the card
and the publish pass attaches it when the message goes out, so the
published message is the same one an immediate send would have
made.  Queue files written before cards could be scheduled carry
no ``contact`` and load with no card, exactly as before.

An explicit expiry queues the same way (``tau channel send
--expire 2h --delay 5m``): the queued entry carries the lifetime in
seconds from publish, and the publish pass signs the message with
``expires_at`` set that far past the publish time, so the published
message disappears exactly like an immediate ``--expire`` send
would.  The explicit expiry wins over the channel's disappearing
default at publish time, the same way an explicit ``expires_at``
wins in ``ChannelApp.send``.  Queue files written before expiries
could be scheduled carry no ``expires_in`` and load with no explicit
expiry, exactly as before.

Entries are validated on load the way the other small JSON stores
are: a malformed file raises rather than silently dropping queued
messages, and the mail-loop publisher treats a corrupt queue as
nothing-due (with a warning) so one bad file cannot stall mail.
"""

from __future__ import annotations

import json
import logging
import math
import os
import secrets
import time
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union

from .protocol import (
    MAX_MAIL_TTL,
    ProtocolError,
    clean_contact,
    clean_location,
    normalize_message,
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

SCHEDULED_CHANNEL_FILE_MODE = 0o600
SCHEDULED_CHANNEL_ID_BYTES = 8
MAX_SCHEDULED_CHANNEL_DELAY = 7 * 24 * 60 * 60
MAX_SCHEDULED_CHANNEL_TTL = 255
# Retry policy for a failed publish: exponential backoff from the base,
# capped, and a hard attempt limit after which the entry stays queued
# (never dropped silently) but is no longer retried automatically.
MAX_PUBLISH_ATTEMPTS = 12
PUBLISH_RETRY_BASE_SECONDS = 60.0
PUBLISH_RETRY_CAP_SECONDS = 60.0 * 60.0
# Wall-clock bound on one publish pass.  The pass runs on the mail-loop
# thread, so entries not started within the budget wait for a later pass.
PUBLISH_PASS_BUDGET_SECONDS = 20.0


class ScheduledChannelError(ValueError):
    """Raised when a scheduled-channel-messages file or value is malformed."""


def new_scheduled_channel_id() -> str:
    """A fresh queue id: 16 lowercase hex characters."""
    return secrets.token_hex(SCHEDULED_CHANNEL_ID_BYTES)


def _clean_id(value: Any) -> str:
    if not isinstance(value, str):
        raise ScheduledChannelError(f"not a scheduled channel id: {value!r}")
    lowered = value.lower()
    if len(lowered) != 2 * SCHEDULED_CHANNEL_ID_BYTES:
        raise ScheduledChannelError(f"not a scheduled channel id: {value!r}")
    try:
        bytes.fromhex(lowered)
    except ValueError:
        raise ScheduledChannelError(f"not a scheduled channel id: {value!r}") from None
    return lowered


def _clean_channel(value: Any) -> str:
    # Imported lazily: channels.py imports the protocol layer, and this
    # module is imported by the CLI before the node starts.
    from .channels import ChannelError, clean_channel_name

    try:
        return clean_channel_name(value)
    except ChannelError as exc:
        raise ScheduledChannelError(str(exc)) from exc


def _clean_text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ScheduledChannelError("scheduled channel text must not be empty")
    try:
        return normalize_message(value)
    except ProtocolError as exc:
        raise ScheduledChannelError(str(exc)) from exc


def _clean_message_hash(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ScheduledChannelError(f"not a message hash: {value!r}")
    lowered = value.lower()
    if len(lowered) != 64:
        raise ScheduledChannelError(f"not a message hash: {value!r}")
    try:
        bytes.fromhex(lowered)
    except ValueError:
        raise ScheduledChannelError(f"not a message hash: {value!r}") from None
    return lowered


def _clean_targets(value: Any) -> Tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise ScheduledChannelError("scheduled channel targets must be a list")
    cleaned = []
    for target in value:
        if not isinstance(target, str) or not target.strip():
            raise ScheduledChannelError(f"not a publish target: {target!r}")
        cleaned.append(target)
    return tuple(cleaned)


def _clean_ttl(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ScheduledChannelError(f"not a gossip TTL: {value!r}")
    if value < 0 or value > MAX_SCHEDULED_CHANNEL_TTL:
        raise ScheduledChannelError(f"not a gossip TTL: {value!r}")
    return value


def _clean_timestamp(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ScheduledChannelError(f"not a timestamp: {label}={value!r}")
    stamp = float(value)
    if stamp <= 0 or stamp != stamp or stamp in (float("inf"), float("-inf")):
        raise ScheduledChannelError(f"not a timestamp: {label}={value!r}")
    return stamp


def _clean_attempts(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ScheduledChannelError(f"not an attempt count: {value!r}")
    return value


def _clean_retry_at(value: Any) -> float:
    # 0.0 means "eligible now"; unlike publish_at it is not a real time.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ScheduledChannelError(f"not a retry time: {value!r}")
    stamp = float(value)
    if stamp < 0 or stamp != stamp or stamp == float("inf"):
        raise ScheduledChannelError(f"not a retry time: {value!r}")
    return stamp


def _clean_kind(value: Any) -> str:
    if value not in ("message", "poll"):
        raise ScheduledChannelError(f"not a scheduled entry kind: {value!r}")
    return value


def _clean_options(value: Any) -> Tuple[str, ...]:
    # Lightweight shape check for the dataclass: the full option rules
    # (distinct, normalised, length-capped) run at schedule time via
    # ``_clean_poll_options`` and again when the poll is created, so a
    # hand-built entry cannot slip a malformed list into the queue.
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise ScheduledChannelError("scheduled poll options must be a list")
    cleaned = []
    for option in value:
        if not isinstance(option, str) or not option.strip():
            raise ScheduledChannelError(f"not a poll option: {option!r}")
        cleaned.append(option)
    if len(cleaned) > 8:
        raise ScheduledChannelError("a scheduled poll has at most 8 options")
    return tuple(cleaned)


def _clean_location(value: Any) -> Optional[Dict[str, Any]]:
    # None stays None (no pin); a present value must clean to a pin,
    # or the entry is malformed.  A scheduled message must not
    # silently drop a pin the user asked for — the publish pass would
    # then send a message that reads differently from the one that
    # was scheduled.
    if value is None:
        return None
    cleaned = clean_location(value)
    if cleaned is None:
        raise ScheduledChannelError("scheduled channel message location is invalid")
    return cleaned


def _clean_contact(value: Any) -> Optional[Dict[str, Any]]:
    # None stays None (no card); a present value must clean to a
    # card, or the entry is malformed.  A scheduled message must not
    # silently drop a card the user asked for — the publish pass
    # would then send a message that reads differently from the one
    # that was scheduled.
    if value is None:
        return None
    cleaned = clean_contact(value)
    if cleaned is None:
        raise ScheduledChannelError("scheduled channel message contact is invalid")
    return cleaned


def _clean_expires_in(value: Any) -> Optional[float]:
    # None stays None (no explicit expiry — the publish pass falls
    # back to the channel's disappearing default, exactly like an
    # immediate send with no --expire).  A present value must be a
    # positive lifetime no longer than the mail TTL, or the entry is
    # malformed: a scheduled message must not silently drop an expiry
    # the user asked for, and an expiry longer than the envelope that
    # might carry the message could never be honoured.
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ScheduledChannelError(
            "scheduled channel message expires_in must be a number of seconds"
        )
    try:
        seconds = float(value)
    except OverflowError as exc:
        # A huge JSON integer cannot convert to a float; treat it as the
        # same invalid duration so the queue reads as unusable, not as a
        # crash that skips every queued message.
        raise ScheduledChannelError(
            "scheduled channel message expires_in must be more than 0s and "
            "at most 30d"
        ) from exc
    if not math.isfinite(seconds) or seconds <= 0 or seconds > MAX_MAIL_TTL:
        raise ScheduledChannelError(
            "scheduled channel message expires_in must be more than 0s and "
            "at most 30d"
        )
    return seconds


def channel_contact_refusal(
    contact: Dict[str, Any],
    *,
    own_hash: str,
    blocklist: Any,
) -> Optional[str]:
    """Why a cleaned *contact* card may not be shared to a channel, or ``None``.

    The single policy for immediate and scheduled channel sends: never
    publish your own card, never amplify a blocked peer, and fail closed
    when the block-list is unavailable (``blocklist is None``).
    """
    if contact["destination_hash"] == own_hash:
        return (
            "cannot share your own contact: the recipient "
            "already has your hash"
        )
    if blocklist is None:
        return "cannot share a contact: the block-list could not be read"
    if blocklist.is_blocked(contact["destination_hash"]):
        return "cannot share a contact for a blocked peer"
    return None


@dataclass(frozen=True)
class ScheduledChannelMessage:
    """One queued channel message, waiting for its publish time."""

    id: str
    channel: str
    text: str
    publish_at: float
    created_at: float
    to: Tuple[str, ...] = ()
    ttl: int = 3
    # Once a publish has created the signed message, its hash is kept
    # so retries re-send that same message instead of signing a new
    # copy — peers that already received it must not see a duplicate.
    message_hash: Optional[str] = None
    attempts: int = 0
    next_attempt_at: float = 0.0
    # When a publish pass last paused on this entry (stop or budget):
    # 0.0 means it never paused.  A pass starts with entries that have
    # never paused (or paused longest ago), so one message whose peers
    # keep stalling cannot monopolize the queue and starve later due
    # messages — the pause is scheduling state, not a failure, so it
    # leaves attempts and the retry limit untouched.
    paused_at: float = 0.0
    # Destination hashes that still owe delivery after a partial
    # failure; empty means "all of ``to`` (or every peer)".
    pending_peers: Tuple[str, ...] = ()
    # What the entry publishes as: ``"message"`` (the text is the
    # message) or ``"poll"`` (the text is the question and ``options``
    # carries its fixed choices).  Defaults to ``"message"`` so queue
    # files written before polls could be scheduled load unchanged.
    kind: str = "message"
    options: Tuple[str, ...] = ()
    # A location pin on a top-level message, like an immediate
    # ``tau channel send --location``: ``{"lat": .., "lon": ..}`` with
    # an optional ``"name"``.  Defaults to None so queue files written
    # before pins could be scheduled load unchanged.
    location: Optional[Dict[str, Any]] = None
    # A shared contact card on a top-level message, like an immediate
    # ``tau channel send --contact``: ``{"name": ..,
    # "destination_hash": <32 hex>}``.  Defaults to None so queue
    # files written before cards could be scheduled load unchanged.
    contact: Optional[Dict[str, Any]] = None
    # An explicit expiry on the queued message, in seconds from
    # publish, like an immediate ``tau channel send --expire``: the
    # publish pass signs the message with ``expires_at`` that far past
    # the publish time.  Defaults to None so queue files written
    # before expiries could be scheduled load unchanged — and a None
    # here means the channel's disappearing default applies at
    # publish time, exactly like an immediate send with no --expire.
    expires_in: Optional[float] = None

    def __post_init__(self) -> None:
        # A frozen dataclass still builds through __init__: validate
        # here so a hand-built entry cannot slip past from_dict's
        # checks with an empty text or an out-of-range TTL.
        object.__setattr__(self, "id", _clean_id(self.id))
        object.__setattr__(self, "channel", _clean_channel(self.channel))
        object.__setattr__(self, "text", _clean_text(self.text))
        object.__setattr__(self, "to", _clean_targets(list(self.to)))
        object.__setattr__(self, "ttl", _clean_ttl(self.ttl))
        object.__setattr__(self, "publish_at", _clean_timestamp(self.publish_at, "publish_at"))
        object.__setattr__(self, "created_at", _clean_timestamp(self.created_at, "created_at"))
        object.__setattr__(self, "message_hash", _clean_message_hash(self.message_hash))
        object.__setattr__(self, "attempts", _clean_attempts(self.attempts))
        object.__setattr__(self, "next_attempt_at", _clean_retry_at(self.next_attempt_at))
        object.__setattr__(self, "paused_at", _clean_retry_at(self.paused_at))
        object.__setattr__(self, "pending_peers", _clean_targets(list(self.pending_peers)))
        object.__setattr__(self, "kind", _clean_kind(self.kind))
        object.__setattr__(self, "options", _clean_options(self.options))
        object.__setattr__(self, "location", _clean_location(self.location))
        object.__setattr__(self, "contact", _clean_contact(self.contact))
        object.__setattr__(self, "expires_in", _clean_expires_in(self.expires_in))
        if self.kind == "poll":
            # A poll is a top-level entry: its options are fixed at
            # schedule time.
            if not 2 <= len(self.options) <= 8:
                raise ScheduledChannelError("a scheduled poll needs 2-8 options")
            if self.location is not None:
                raise ScheduledChannelError(
                    "a scheduled poll cannot carry a location"
                )
            if self.contact is not None:
                raise ScheduledChannelError(
                    "a scheduled poll cannot carry a contact card"
                )
        elif self.options:
            raise ScheduledChannelError(
                "a scheduled channel message cannot carry poll options"
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "channel": self.channel,
            "text": self.text,
            "publish_at": self.publish_at,
            "created_at": self.created_at,
            "to": list(self.to),
            "ttl": self.ttl,
            "message_hash": self.message_hash,
            "attempts": self.attempts,
            "next_attempt_at": self.next_attempt_at,
            "paused_at": self.paused_at,
            "pending_peers": list(self.pending_peers),
            "kind": self.kind,
            "options": list(self.options),
            "location": self.location,
            "contact": self.contact,
            "expires_in": self.expires_in,
        }

    @classmethod
    def from_dict(cls, data: Any) -> "ScheduledChannelMessage":
        if not isinstance(data, dict):
            raise ScheduledChannelError("scheduled channel message must be a JSON object")
        return cls(
            id=_clean_id(data.get("id")),
            channel=_clean_channel(data.get("channel")),
            text=_clean_text(data.get("text")),
            publish_at=_clean_timestamp(data.get("publish_at"), "publish_at"),
            created_at=_clean_timestamp(data.get("created_at"), "created_at"),
            to=_clean_targets(data.get("to")),
            ttl=_clean_ttl(data.get("ttl", 3)),
            message_hash=_clean_message_hash(data.get("message_hash")),
            attempts=_clean_attempts(data.get("attempts", 0)),
            next_attempt_at=_clean_retry_at(data.get("next_attempt_at", 0.0)),
            paused_at=_clean_retry_at(data.get("paused_at", 0.0)),
            pending_peers=_clean_targets(data.get("pending_peers")),
            kind=_clean_kind(data.get("kind", "message")),
            options=_clean_options(data.get("options")),
            location=_clean_location(data.get("location")),
            contact=_clean_contact(data.get("contact")),
            expires_in=_clean_expires_in(data.get("expires_in")),
        )


@dataclass(frozen=True)
class ScheduledChannelMessages:
    """The queued messages, oldest publish time first."""

    messages: Tuple[ScheduledChannelMessage, ...] = ()

    def __len__(self) -> int:
        return len(self.messages)

    def due(self, now: Optional[float] = None) -> List[ScheduledChannelMessage]:
        """Queued messages whose publish time has come, oldest first."""
        at = now if now is not None else time.time()
        return [message for message in self.messages if message.publish_at <= at]

    def resolve(self, prefix: str) -> ScheduledChannelMessage:
        """Find one queued message by full id or unique prefix."""
        if not isinstance(prefix, str) or not prefix.strip():
            raise ScheduledChannelError("a scheduled channel message id is required")
        matches = [
            message
            for message in self.messages
            if message.id.startswith(prefix.lower())
        ]
        if not matches:
            raise ScheduledChannelError(f"no scheduled channel message {prefix!r}")
        if len(matches) > 1:
            raise ScheduledChannelError(
                f"ambiguous scheduled channel message id {prefix!r} "
                f"({len(matches)} match)"
            )
        return matches[0]

    def with_added(self, message: ScheduledChannelMessage) -> "ScheduledChannelMessages":
        if any(existing.id == message.id for existing in self.messages):
            raise ScheduledChannelError(
                f"scheduled channel message {message.id} is already queued"
            )
        ordered = tuple(sorted((*self.messages, message), key=lambda m: m.publish_at))
        return ScheduledChannelMessages(messages=ordered)

    def with_replaced(self, message: ScheduledChannelMessage) -> "ScheduledChannelMessages":
        if not any(existing.id == message.id for existing in self.messages):
            raise ScheduledChannelError(f"no scheduled channel message {message.id!r}")
        swapped = tuple(
            message if existing.id == message.id else existing
            for existing in self.messages
        )
        return ScheduledChannelMessages(messages=swapped)

    def with_removed(self, message_id: str) -> "ScheduledChannelMessages":
        kept = tuple(message for message in self.messages if message.id != message_id)
        if len(kept) == len(self.messages):
            raise ScheduledChannelError(f"no scheduled channel message {message_id!r}")
        return ScheduledChannelMessages(messages=kept)

    def to_dict(self) -> Dict[str, Any]:
        return {"messages": [message.to_dict() for message in self.messages]}

    @classmethod
    def from_dict(cls, data: Any) -> "ScheduledChannelMessages":
        if not isinstance(data, dict):
            raise ScheduledChannelError("scheduled-channel-messages must be a JSON object")
        if "messages" not in data:
            raise ScheduledChannelError("messages is required")
        raw = data.get("messages")
        if not isinstance(raw, list):
            raise ScheduledChannelError("messages must be a list")
        return cls(
            messages=tuple(ScheduledChannelMessage.from_dict(item) for item in raw)
        )


def load_scheduled_channel_messages(
    path: Optional[Union[str, Path]],
) -> ScheduledChannelMessages:
    """Load the queue at *path*; a missing file means nothing scheduled."""
    if path is None:
        return ScheduledChannelMessages()
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return ScheduledChannelMessages()
    except OSError as exc:
        raise ScheduledChannelError(
            f"could not read scheduled-channel-messages {target}: {exc}"
        ) from exc
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ScheduledChannelError(
            f"could not read scheduled-channel-messages {target}: {exc}"
        ) from exc
    return ScheduledChannelMessages.from_dict(data)


def save_scheduled_channel_messages(
    path: Union[str, Path], messages: ScheduledChannelMessages
) -> Path:
    """Persist *messages* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    return atomic_write_json(target, messages.to_dict(), mode=SCHEDULED_CHANNEL_FILE_MODE)


@contextmanager
def scheduled_channel_messages_lock(path: Union[str, Path]) -> Iterator[None]:
    """Serialize read-modify-write updates of the scheduled-channel queue.

    Same shape as the scheduled-posts lock: a flock on a
    ``.scheduled-channel-messages.json.lock`` sidecar.  An atomic file
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


def _preflight_envelope(
    *,
    channel: str,
    text: str,
    created_at: float,
    name: Optional[str] = None,
    location: Optional[Dict[str, Any]] = None,
    contact: Optional[Dict[str, Any]] = None,
) -> None:
    """Measure *text* against the wire budget with the envelope the
    publish pass measures.

    The publish pass signs the message through
    ``channels._normalize_message``, which adds ``v``, ``expires_at``
    and ``reply_to`` to the body before ``_check_wire_size`` measures
    it — so the preflight must carry those fields too, at their real
    encoded sizes (an empty reply), or a replacement near the budget
    would pass here and then fail the definitive check on every
    publish attempt, leaving the entry queued for failed retries.

    The expiry is measured at its largest: publishing fills in a
    numeric expiry from the channel's disappearing-message default
    (``ChannelApp.send``), and that default can be set or changed
    after scheduling, so the preflight assumes an expiry is present
    and uses the longest allowed one (``created_at`` plus the 30-day
    cap).  A float encodes at a fixed size, so this covers any
    default the channel may carry by publish time.

    A location pin rides in the measured envelope too, at its real
    shape, so a message that only fits without its pin is refused
    here rather than failing every publish attempt.

    A contact card rides in the measured envelope too, at its real
    shape, for the same reason.
    """
    # Lazy import: this module stays importable without Reticulum,
    # which ``tau.backup`` relies on.
    from .channels import (
        CHANNEL_VERSION,
        MAX_CHANNEL_EXPIRE_SECONDS,
        ChannelError,
        _check_wire_size,
        sanitize_name,
    )

    envelope = {
        # Placeholder author/key at their real lengths (destination
        # hash 32 hex, RNS public key 128 hex): a shorter placeholder
        # would admit a message that only fails once signed.
        "v": CHANNEL_VERSION,
        "channel": channel,
        "author": "0" * 32,
        "key": "0" * 128,
        "name": sanitize_name(name or "anonymous"),
        "text": text,
        "created_at": created_at,
        "expires_at": created_at + MAX_CHANNEL_EXPIRE_SECONDS,
        "reply_to": "",
    }
    if location is not None:
        envelope["location"] = location
    if contact is not None:
        envelope["contact"] = contact
    try:
        _check_wire_size(envelope)
    except ChannelError as exc:
        raise ScheduledChannelError(str(exc)) from exc


def schedule_channel_message(
    path: Union[str, Path],
    *,
    channel: str,
    text: str,
    publish_at: float,
    to: Optional[Sequence[str]] = None,
    ttl: int = 3,
    now: Optional[float] = None,
    name: Optional[str] = None,
    kind: str = "message",
    options: Optional[Sequence[str]] = None,
    location: Optional[Dict[str, Any]] = None,
    contact: Optional[Dict[str, Any]] = None,
    expires_in: Optional[float] = None,
    disappearing_path: Optional[Union[str, Path]] = None,
) -> ScheduledChannelMessage:
    """Queue a channel message for *publish_at*; returns the stored entry.

    The text gets the same validation as an immediate ``tau channel
    send`` (``normalize_message``: non-empty, at most
    ``MAX_MESSAGE_CHARS``), so a message that could never be created
    is refused here, not on every publish pass.  The publish envelope
    is also measured against the wire budget with placeholder hash,
    signature, author and key of the real lengths: a multibyte message
    near the budget would otherwise pass the character limit but fail
    every publish attempt forever.  The real envelope is measured
    again when the message is created, so a long display name can
    still only shrink the margin, never grow it past the check here.

    The measured envelope carries the expiry the publish pass will
    actually apply: the channel's disappearing default (resolved from
    *disappearing_path*) when one is set, and no expiry at all when
    the channel has none — a channel without a default publishes a
    message with no ``expires_at``, so sizing one in would reject a
    poll that fits.  When no disappearing path is given, no default
    is assumed.

    With ``kind="poll"`` the text is the poll question and *options*
    carries its fixed choices: the question plus options get the same
    wire-size preflight an immediate ``tau channel poll`` gets, so a
    poll that could never fit is refused here.

    A *location* pins a place on the queued message, like an
    immediate ``tau channel send --location``: it is validated here,
    rides in the measured envelope, and the publish pass attaches it
    when the message goes out.  A poll cannot carry one.

    A *contact* shares a peer's card on the queued message, like an
    immediate ``tau channel send --contact``: it is validated here,
    rides in the measured envelope, and the publish pass attaches it
    when the message goes out.  A poll cannot carry one.

    An *expires_in* makes the queued message a disappearing one, like
    an immediate ``tau channel send --expire``: seconds from publish
    until the message expires, at most the 30-day mail TTL.  It is
    validated here, rides in the measured envelope as ``expires_at``
    that far past the publish time, and the publish pass signs the
    message with that expiry — winning over the channel's
    disappearing default, exactly like an explicit ``expires_at``
    wins in ``ChannelApp.send``.  A poll can carry one: expiry is
    not a top-level attachment like a pin or a card, and the protocol
    already allows ``expires_at`` on poll messages.
    """
    clean_channel = _clean_channel(channel)
    clean_text = _clean_text(text)
    # Lazy import: this module stays importable without Reticulum,
    # which ``tau.backup`` relies on.
    from .channels import (
        CHANNEL_VERSION,
        ChannelError,
        _check_wire_size,
        sanitize_name,
    )
    from .disappearing import channel_default_for

    clean_kind = _clean_kind(kind)
    clean_options: Tuple[str, ...] = ()
    if clean_kind == "poll":
        from .tangents.tan import _clean_poll_options

        try:
            clean_options = tuple(_clean_poll_options(list(options or [])))
        except ValueError as exc:
            raise ScheduledChannelError(str(exc)) from exc
    elif options:
        raise ScheduledChannelError(
            "a scheduled channel message cannot carry poll options"
        )
    clean_location_value = _clean_location(location)
    if clean_kind == "poll" and clean_location_value is not None:
        raise ScheduledChannelError("a scheduled poll cannot carry a location")
    clean_contact_value = _clean_contact(contact)
    if clean_kind == "poll" and clean_contact_value is not None:
        raise ScheduledChannelError("a scheduled poll cannot carry a contact card")
    clean_expires_in = _clean_expires_in(expires_in)
    clean_publish_at = _clean_timestamp(publish_at, "publish_at")
    # The publish pass stamps created_at with the send time, rounded to
    # milliseconds, so its fractional digits can be longer than a whole
    # publish_at.  Measure with a full three-digit fraction so the
    # preflight never undercounts the bytes the timestamps take on the wire.
    wire_at = math.floor(clean_publish_at) + 0.999
    envelope = {
        # Placeholder author/key at their real lengths (destination
        # hash 32 hex, RNS public key 128 hex): a shorter placeholder
        # would admit a message that only fails once signed.  The
        # remaining signed fields are carried at their real shape too:
        # the version and an empty reply target.  An expiry rides
        # only when the message will actually carry one at publish
        # time — an explicit *expires_in* here, else the channel's
        # disappearing default — and a channel without either
        # publishes with no expiry at all, so sizing one in here
        # would reject a poll that fits on the wire.
        "v": CHANNEL_VERSION,
        "channel": clean_channel,
        "author": "0" * 32,
        "key": "0" * 128,
        "name": sanitize_name(name or "anonymous"),
        "text": clean_text,
        "created_at": wire_at,
        "reply_to": "",
    }
    default_expire = channel_default_for(disappearing_path, clean_channel)
    if clean_expires_in is not None:
        envelope["expires_at"] = round(wire_at + clean_expires_in, 3)
    elif default_expire is not None:
        envelope["expires_at"] = round(wire_at + default_expire, 3)
    if clean_kind == "poll":
        envelope["poll_options"] = list(clean_options)
    if clean_location_value is not None:
        envelope["location"] = clean_location_value
    if clean_contact_value is not None:
        envelope["contact"] = clean_contact_value
    try:
        _check_wire_size(envelope)
    except ChannelError as exc:
        raise ScheduledChannelError(str(exc)) from exc
    message = ScheduledChannelMessage(
        id=new_scheduled_channel_id(),
        channel=clean_channel,
        text=clean_text,
        to=_clean_targets(list(to) if to is not None else None),
        ttl=_clean_ttl(ttl),
        publish_at=_clean_timestamp(publish_at, "publish_at"),
        created_at=now if now is not None else time.time(),
        kind=clean_kind,
        options=clean_options,
        location=clean_location_value,
        contact=clean_contact_value,
        expires_in=clean_expires_in,
    )
    with scheduled_channel_messages_lock(path):
        queue = load_scheduled_channel_messages(path)
        save_scheduled_channel_messages(path, queue.with_added(message))
    return message


def schedule_channel_poll(
    path: Union[str, Path],
    *,
    channel: str,
    question: str,
    options: Sequence[str],
    publish_at: float,
    to: Optional[Sequence[str]] = None,
    ttl: int = 3,
    now: Optional[float] = None,
    name: Optional[str] = None,
    expires_in: Optional[float] = None,
    disappearing_path: Optional[Union[str, Path]] = None,
) -> ScheduledChannelMessage:
    """Queue a poll for *publish_at*; returns the stored entry.

    Thin wrapper over :func:`schedule_channel_message` with
    ``kind="poll"``: the question is the entry text and the options
    ride alongside it.  An *expires_in* makes the poll a disappearing
    one, seconds from publish until it expires, like an immediate
    poll's channel disappearing default — only explicit.
    """
    return schedule_channel_message(
        path,
        channel=channel,
        text=question,
        publish_at=publish_at,
        to=to,
        ttl=ttl,
        now=now,
        name=name,
        kind="poll",
        options=options,
        expires_in=expires_in,
        disappearing_path=disappearing_path,
    )


def cancel_scheduled_channel_message(
    path: Union[str, Path],
    prefix: str,
    *,
    on_cancel: Optional[Callable[[ScheduledChannelMessage], None]] = None,
) -> ScheduledChannelMessage:
    """Drop one queued message by full id or unique prefix; returns it.

    When *on_cancel* is given it runs under the queue lock, after the
    entry is resolved and before it is removed — so a mail-loop pass
    cannot publish the message between the callback and the removal.
    The callback is how the CLI deletes the locally stored message for
    an entry whose publish already created one.
    """
    with scheduled_channel_messages_lock(path):
        queue = load_scheduled_channel_messages(path)
        message = queue.resolve(prefix)
        if on_cancel is not None:
            on_cancel(message)
        save_scheduled_channel_messages(path, queue.with_removed(message.id))
    return message


def edit_scheduled_channel_message(
    path: Union[str, Path],
    prefix: str,
    *,
    text: Optional[str] = None,
    publish_at: Optional[float] = None,
    name: Optional[str] = None,
    now: Optional[float] = None,
) -> ScheduledChannelMessage:
    """Change the text and/or publish time of one queued channel message.

    At least one of *text* / *publish_at* must be given.  The
    resulting entry gets the same validation as scheduling does
    (``normalize_message`` plus the wire-size preflight against the
    entry's channel, measured with the full normalized envelope the
    publish pass measures), so an edit cannot queue a message that
    could never be published — a time-only edit re-checks the stored
    text too.  A new publish time must be in the future and within
    the 7-day queue cap.

    An entry whose publish already created the signed message (it has
    a stored ``message_hash``) cannot be edited: the text is already
    signed, so changing it would orphan the stored copy.  Cancel it
    instead.  Everything else about the entry — channel, targets, TTL,
    retry state — is left untouched.  The queue stays ordered soonest
    first, so a later publish time moves the entry down the list.
    """
    if text is None and publish_at is None:
        raise ScheduledChannelError(
            "nothing to change — pass a new text and/or a new publish time"
        )
    at = now if now is not None else time.time()
    clean_text: Optional[str] = None
    if text is not None:
        clean_text = _clean_text(text)
    clean_publish_at: Optional[float] = None
    if publish_at is not None:
        clean_publish_at = _clean_timestamp(publish_at, "publish_at")
        if clean_publish_at <= at:
            raise ScheduledChannelError("publish time must be in the future")
        if clean_publish_at - at > MAX_SCHEDULED_CHANNEL_DELAY:
            raise ScheduledChannelError(
                "publish time must be within 7d (the scheduled-channel delay cap)"
            )
    with scheduled_channel_messages_lock(path):
        queue = load_scheduled_channel_messages(path)
        message = queue.resolve(prefix)
        if message.message_hash is not None:
            raise ScheduledChannelError(
                "that message was already created — cancel it instead of editing it"
            )
        # The preflight measures the resulting entry — text and
        # publish time — against the same envelope the publish pass
        # measures, so an edit cannot queue a message that could never
        # be published.  A time-only edit re-checks the stored text
        # against the full normalized shape too.  The entry's pin rides
        # in the measurement: a time-only edit must not bless a message
        # that only fits without it.  The entry's card rides in the
        # measurement too, for the same reason.
        _preflight_envelope(
            channel=message.channel,
            text=clean_text if clean_text is not None else message.text,
            created_at=(
                clean_publish_at
                if clean_publish_at is not None
                else message.publish_at
            ),
            name=name,
            location=message.location,
            contact=message.contact,
        )
        edited = replace(
            message,
            text=clean_text if clean_text is not None else message.text,
            publish_at=(
                clean_publish_at if clean_publish_at is not None else message.publish_at
            ),
        )
        replaced = queue.with_replaced(edited)
        ordered = ScheduledChannelMessages(
            messages=tuple(sorted(replaced.messages, key=lambda m: m.publish_at))
        )
        save_scheduled_channel_messages(path, ordered)
    return edited


class _PassPaused(Exception):
    """Internal: the publish pass ends early — node stopping, or the
    pass budget elapsed.  Raised instead of recording a publish
    failure, so the entry being worked on keeps its attempts and its
    unsent peers stay queued for the next pass.  The handler records
    the pause on the entry (``paused_at``) so the next pass starts
    with messages that have not paused, instead of this one again.
    """


def publish_due_scheduled_channel_messages(
    path: Union[str, Path],
    app: Any,
    *,
    resolve_peer: Optional[Callable[[str], Any]] = None,
    now: Optional[float] = None,
    should_stop: Optional[Callable[[], bool]] = None,
    contact_refusal: Optional[Callable[[Dict[str, Any]], Optional[str]]] = None,
) -> Dict[str, int]:
    """Publish every due queued channel message through *app* (a ``ChannelApp``).

    The whole pass runs under the queue lock, so a cancel either lands
    before the entry is considered (and it is never sent) or waits until
    the entry has been sent and removed (and then reports it gone).  The
    lock is held across the network sends, which is the price of that
    guarantee.

    A message is published when ``app.publish`` returns with no
    per-peer errors — the same bar as an immediate ``tau channel
    send``, where zero known peers still counts as published.  The
    signed message is created once and its hash saved before sending,
    so retries re-send the same message.  Peers that already accepted
    it are not sent it again.  A queued poll publishes the same way,
    through ``app.send`` with its question and fixed options.  A queued
    message with an explicit expiry publishes with ``expires_at`` that
    far past the publish time, winning over the channel's disappearing
    default; without one the default applies, exactly like an
    immediate send.

    A failed attempt (exception or per-peer errors) keeps the entry
    queued with exponential backoff.  After ``MAX_PUBLISH_ATTEMPTS`` it
    is no longer retried automatically but stays queued for the user to
    inspect or cancel.  A pass stops starting new entries once
    ``PUBLISH_PASS_BUDGET_SECONDS`` have elapsed.  A pass that pauses
    on an entry remembers the pause on it, so the next pass starts
    with entries that have not paused — a message whose peers keep
    stalling cannot starve later due messages, and it is still retried
    once they have had their turn.

    When ``should_stop`` is given (the node's stop event), a pass ends
    early — between messages, between peer resolutions, and before
    each send — so ``stop()`` never waits out a whole publish pass; the
    entry being worked on stays queued with its attempts untouched
    and its unsent peers kept for the next pass.  The pass budget
    (``deadline``) is checked at the same points, so several peers
    that each announce near their timeout cannot stretch one pass
    into minutes either.  The in-flight peer resolution or send
    still runs to its own timeout, so a stop waits at most one such
    wait, not a whole pass.

    Returns ``{"published": n, "kept": m}``.  A corrupt queue file logs a
    warning and publishes nothing, so one bad file cannot stall the mail
    loop that calls this.
    """
    at = now if now is not None else time.time()
    deadline = time.monotonic() + PUBLISH_PASS_BUDGET_SECONDS
    with scheduled_channel_messages_lock(path):
        try:
            queue = load_scheduled_channel_messages(path)
        except ScheduledChannelError as exc:
            log.warning(
                "Scheduled-channel-messages at %s is unusable (%s); skipping",
                path,
                exc,
            )
            return {"published": 0, "kept": 0}
        candidates = [
            message
            for message in queue.due(at)
            if message.next_attempt_at <= at and message.attempts < MAX_PUBLISH_ATTEMPTS
        ]
        # Fairness across passes: entries that have never paused — or
        # paused longest ago — go first, so a message whose peers keep
        # stalling the pass does not monopolize the queue while later
        # due messages wait.  A paused entry is still attempted in the
        # same pass once the fresher entries have had their turn.
        candidates.sort(key=lambda message: (message.paused_at, message.publish_at))
        published = 0
        try:
            for message in candidates:
                if should_stop is not None and should_stop():
                    break
                if time.monotonic() > deadline:
                    break
                if message.contact is not None and contact_refusal is not None:
                    # Re-check the card's policy now, not only when it was
                    # queued: the peer may have been blocked since.  A
                    # refused entry is parked (never published or retried
                    # automatically) and stays queued for the user to cancel.
                    refusal = contact_refusal(message.contact)
                    if refusal is not None:
                        log.warning(
                            "Scheduled channel message %s not published: %s",
                            message.id,
                            refusal,
                        )
                        message = replace(message, attempts=MAX_PUBLISH_ATTEMPTS)
                        queue = queue.with_replaced(message)
                        save_scheduled_channel_messages(path, queue)
                        continue
                failed_peers: Optional[Tuple[str, ...]] = None
                try:
                    targets = _resolve_publish_targets(
                        message, resolve_peer, should_stop, deadline
                    )
                    # The send-time clock, not the pass start: target
                    # resolution and earlier messages can use up a short
                    # lifetime before signing.  Rounded like ChannelApp.send
                    # rounds created_at, so the lifetime never grows past
                    # what was asked for.
                    sent_at = round(now if now is not None else time.time(), 3)
                    stored = (
                        app.get_message(message.message_hash)
                        if message.message_hash
                        else None
                    )
                    expired_copy = (
                        stored is not None
                        and stored.get("expires_at") is not None
                        and float(stored["expires_at"]) <= sent_at
                    )
                    if expired_copy:
                        # The stored copy expired while earlier attempts
                        # backed off; recipients would discard it, so
                        # sign a fresh one instead of resending it.
                        stored = None
                    if stored is None:
                        if message.message_hash is not None:
                            # The signed message was created on an
                            # earlier pass but is gone from the local
                            # store (retention evicted it), or it expired
                            # before it could be delivered: sign a fresh
                            # copy rather than leaving the entry stuck
                            # forever.  Peers that received the first
                            # copy see a second message — the window
                            # makes this rare, and a stuck queue entry
                            # is worse than a duplicate.
                            log.warning(
                                "Scheduled channel message %s %s; signing a "
                                "fresh one",
                                message.id,
                                "expired before delivery"
                                if expired_copy
                                else "lost its stored copy",
                            )
                        created = app.send(
                            message.channel,
                            message.text,
                            publish=False,
                            expires_at=(
                                sent_at + message.expires_in
                                if message.expires_in is not None
                                else None
                            ),
                            poll_options=(
                                list(message.options)
                                if message.kind == "poll"
                                else None
                            ),
                            location=message.location,
                            contact=message.contact,
                        )
                        # Persist the signed message's identity before
                        # sending, so a retry after a crash re-sends it
                        # rather than signing a second copy.
                        message = replace(message, message_hash=created["hash"])
                        queue = queue.with_replaced(message)
                        save_scheduled_channel_messages(path, queue)
                    else:
                        created = stored
                    result = app.publish(
                        created,
                        targets=targets,
                        ttl=message.ttl,
                    )
                    errors = (
                        list(result.get("errors") or [])
                        if isinstance(result, dict)
                        else []
                    )
                    if errors:
                        failed_peers = tuple(str(err.get("peer") or "") for err in errors)
                        raise ScheduledChannelError(
                            f"{len(errors)} peer(s) did not receive the message"
                        )
                except _PassPaused:
                    # Not a publish failure: end the pass without burning
                    # an attempt on the entry being worked on — but
                    # remember the pause, so the next pass starts with
                    # messages that have not paused instead of this one
                    # again.  A message that keeps pausing yields to later
                    # due messages and is retried once they have had
                    # their turn; its attempts and retry limit are
                    # untouched.
                    message = replace(message, paused_at=at)
                    queue = queue.with_replaced(message)
                    save_scheduled_channel_messages(path, queue)
                    raise
                except Exception as exc:  # noqa: BLE001 - a failed publish retries
                    attempts = message.attempts + 1
                    retry_in = min(
                        PUBLISH_RETRY_BASE_SECONDS * 2 ** (attempts - 1),
                        PUBLISH_RETRY_CAP_SECONDS,
                    )
                    pending = message.pending_peers
                    if failed_peers is not None:
                        # Retry only the peers that failed.  An unknown peer
                        # hash cannot be re-targeted, so fall back to all targets.
                        pending = failed_peers if all(p and p != "?" for p in failed_peers) else ()
                    message = replace(
                        message,
                        attempts=attempts,
                        next_attempt_at=at + retry_in,
                        pending_peers=pending,
                    )
                    queue = queue.with_replaced(message)
                    save_scheduled_channel_messages(path, queue)
                    log.warning(
                        "Scheduled channel message %s could not publish (%s); "
                        "keeping it queued",
                        message.id,
                        exc,
                    )
                    if attempts >= MAX_PUBLISH_ATTEMPTS:
                        log.warning(
                            "Scheduled channel message %s gave up after %d attempts; "
                            "it stays queued until cancelled",
                            message.id,
                            attempts,
                        )
                    continue
                queue = queue.with_removed(message.id)
                save_scheduled_channel_messages(path, queue)
                published += 1
        except _PassPaused:
            pass
    return {"published": published, "kept": len(candidates) - published}


def _resolve_publish_targets(
    message: ScheduledChannelMessage,
    resolve_peer: Optional[Callable[[str], Any]],
    should_stop: Optional[Callable[[], bool]] = None,
    deadline: Optional[float] = None,
) -> Optional[List[Any]]:
    """Peers for one publish attempt: the still-owed ones, else ``to``.

    A stop or an expired pass budget between resolutions pauses the
    pass (``_PassPaused``) before the next peer is resolved, so a
    queue of slow announcers cannot stretch one pass into minutes.
    """
    names = message.pending_peers or message.to
    if not names:
        return None
    if resolve_peer is None:
        raise ScheduledChannelError(
            "scheduled channel message has targets but no peer resolver"
        )
    resolved: List[Any] = []
    for name in names:
        if should_stop is not None and should_stop():
            raise _PassPaused()
        if deadline is not None and time.monotonic() > deadline:
            raise _PassPaused()
        resolved.append(resolve_peer(name))
    return resolved
