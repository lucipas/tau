"""Owner-kept starred chat messages.

The counterpart of Tan's saved posts for the chat side: ``tau save``
stars one stored DM, group, or channel message, and ``tau saved``
lists the stars newest-save first, so an address in a DM, a decision
in a group, or a link in a channel is one command away instead of a
search through whole histories.

A star is purely a local display preference, like a pinned
conversation: nothing is sent on the wire, the peer is never told,
and starring never deletes anything or stops delivery.  The list
lives next to the pinned-conversation list, persisted as JSON in the
data directory (``saved_messages.json``), with the same load/save
pattern — and the same failure direction as the pinned list.  A star
list is a display convenience, so a corrupt file fails *open* for
listings: the stars are dropped, with a warning, rather than a bad
file silently hiding messages the owner marked.  Save/unsave refuse
to overwrite a corrupt list, so a bad file cannot silently drop the
owner's other stars.

A star names one stored message by its wire id, scoped to the
conversation it was starred in, so the same id in another
conversation is a different star.  The message itself stays in the
inbox or the channel store: a star whose message was later deleted
or expired still holds its place in the list but renders as no
longer stored, the way a saved Tan post keeps its stub when the
post is deleted — except the blocklist still applies to the stub:
a star records its sender and direction when it is created, so a
blocked sender's star stays hidden after the message is gone, and
a stub whose sender was never recorded stays hidden too.  A group
star keeps the group id the message was stored under, so a rename
does not move the star — the record keeps its original group id,
and the star still finds it.
"""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from .pinned import PinnedError, clean_pinned_target
from .util import atomic_write_json

log = logging.getLogger(__name__)

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore[assignment]

SAVED_MESSAGES_FILE_MODE = 0o600

# Stars are capped like pinned conversations: a peer cannot make this
# node hold unbounded state through any wire path (stars are
# local-only, so the cap is really about a runaway script), and
# `tau unsave` frees capacity when the cap is reached.
MAX_SAVED_MESSAGES = 100

SAVED_MESSAGE_KINDS = ("peer", "group", "channel")


class SavedMessagesError(ValueError):
    """Raised when a starred-message list or value is malformed."""


def clean_saved_scope(kind: str, scope: Any) -> str:
    """Normalise a star's conversation scope for *kind*.

    Peers and groups are 32-hex destination hashes; channels are
    cleaned channel names — the same cleaning a pinned conversation
    uses, so a scope that cannot name a conversation is refused
    before it is stored.
    """
    if kind not in SAVED_MESSAGE_KINDS:
        raise SavedMessagesError(f"unknown starred-message kind: {kind!r}")
    try:
        return clean_pinned_target(kind, scope)
    except PinnedError as exc:
        raise SavedMessagesError(str(exc)) from exc


def clean_saved_message_id(message_id: Any) -> str:
    """Normalise a starred message id, or raise :class:`SavedMessagesError`."""
    if not isinstance(message_id, str) or not message_id.strip():
        raise SavedMessagesError("message id must not be empty")
    return message_id.strip()


def clean_saved_sender(sender: Any) -> Optional[str]:
    """Normalise a persisted star sender, or ``None`` when unknown."""
    if sender is None:
        return None
    if not isinstance(sender, str) or not sender.strip():
        raise SavedMessagesError("star sender must be a non-empty string")
    return sender.strip()


def clean_saved_direction(direction: Any) -> Optional[str]:
    """Normalise a persisted star direction, or ``None`` when unknown."""
    if direction is None:
        return None
    if direction not in ("in", "out"):
        raise SavedMessagesError("star direction must be 'in' or 'out'")
    return direction


@dataclass(frozen=True)
class SavedMessageEntry:
    """One starred message, newest save first in the list.

    ``sender`` is the hash the blocklist is checked against — the
    peer for a DM, the message sender for a group, the author for a
    channel — and ``direction`` is ``"out"`` for a message this node
    sent.  Both are recorded when the star is created so the
    blocklist still applies after the message itself is gone; stars
    written before they existed load with both as ``None``.
    """

    kind: str
    scope: str
    message_id: str
    saved_at: float
    sender: Optional[str] = None
    direction: Optional[str] = None


@dataclass(frozen=True)
class SavedMessages:
    """Starred messages, newest save first."""

    entries: Tuple[SavedMessageEntry, ...] = ()

    def is_saved(self, kind: str, scope: Any, message_id: Any) -> bool:
        """True when the message named by *kind*/*scope*/*message_id* is starred."""
        try:
            cleaned_scope = clean_saved_scope(kind, scope)
            cleaned_id = clean_saved_message_id(message_id)
        except SavedMessagesError:
            return False
        return any(
            entry.kind == kind
            and entry.scope == cleaned_scope
            and entry.message_id == cleaned_id
            for entry in self.entries
        )

    def with_saved(
        self,
        kind: str,
        scope: Any,
        message_id: Any,
        now: Optional[float] = None,
        *,
        sender: Any = None,
        direction: Any = None,
    ) -> Tuple["SavedMessages", bool]:
        """Star the message, moving it to the front when already starred.

        Returns the new list and whether the star already existed —
        starring an already-starred message is a no-op, not an error.
        ``sender`` and ``direction`` are recorded with the star so the
        blocklist still applies once the message itself is gone.
        """
        cleaned_scope = clean_saved_scope(kind, scope)
        cleaned_id = clean_saved_message_id(message_id)
        cleaned_sender = clean_saved_sender(sender)
        cleaned_direction = clean_saved_direction(direction)
        stamped = now if now is not None else time.time()
        already = any(
            entry.kind == kind
            and entry.scope == cleaned_scope
            and entry.message_id == cleaned_id
            for entry in self.entries
        )
        rest = tuple(
            entry
            for entry in self.entries
            if not (
                entry.kind == kind
                and entry.scope == cleaned_scope
                and entry.message_id == cleaned_id
            )
        )
        if not already and len(rest) >= MAX_SAVED_MESSAGES:
            raise SavedMessagesError(
                f"too many starred messages (max {MAX_SAVED_MESSAGES}); "
                "unstar one first"
            )
        return SavedMessages(
            entries=(
                SavedMessageEntry(
                    kind,
                    cleaned_scope,
                    cleaned_id,
                    stamped,
                    cleaned_sender,
                    cleaned_direction,
                ),
            )
            + rest
        ), already

    def without(
        self, kind: str, scope: Any, message_id: Any
    ) -> Tuple["SavedMessages", bool]:
        """Unstar the message; returns the new list and whether one was removed."""
        cleaned_scope = clean_saved_scope(kind, scope)
        cleaned_id = clean_saved_message_id(message_id)
        kept = tuple(
            entry
            for entry in self.entries
            if not (
                entry.kind == kind
                and entry.scope == cleaned_scope
                and entry.message_id == cleaned_id
            )
        )
        return SavedMessages(entries=kept), len(kept) != len(self.entries)

    def to_dict(self) -> Dict[str, Any]:
        entries = []
        for entry in self.entries:
            item: Dict[str, Any] = {
                "kind": entry.kind,
                "scope": entry.scope,
                "message_id": entry.message_id,
                "saved_at": entry.saved_at,
            }
            # Sender/direction are omitted when unknown so stars
            # written before they existed round-trip byte-identically.
            if entry.sender is not None:
                item["sender"] = entry.sender
            if entry.direction is not None:
                item["direction"] = entry.direction
            entries.append(item)
        return {"saved": entries}

    @classmethod
    def from_dict(cls, data: Any) -> "SavedMessages":
        if not isinstance(data, dict):
            raise SavedMessagesError("starred-message list must be a JSON object")
        if "saved" not in data:
            # A list that lost its saved key must not silently turn
            # the stars off; the loader treats the file as corrupt
            # instead.
            raise SavedMessagesError("saved is required")
        raw = data.get("saved")
        if not isinstance(raw, list):
            raise SavedMessagesError("saved must be a list of starred messages")
        entries: List[SavedMessageEntry] = []
        seen = set()
        for item in raw:
            if not isinstance(item, dict):
                raise SavedMessagesError("saved entries must be JSON objects")
            kind = item.get("kind")
            if kind not in SAVED_MESSAGE_KINDS:
                raise SavedMessagesError(f"unknown starred-message kind: {kind!r}")
            scope = clean_saved_scope(kind, item.get("scope"))
            message_id = clean_saved_message_id(item.get("message_id"))
            key = (kind, scope, message_id)
            if key in seen:
                raise SavedMessagesError(f"duplicate starred message: {key}")
            seen.add(key)
            saved_at = item.get("saved_at")
            if not isinstance(saved_at, (int, float)) or isinstance(
                saved_at, bool
            ):
                raise SavedMessagesError("saved_at must be a number")
            entries.append(
                SavedMessageEntry(
                    kind,
                    scope,
                    message_id,
                    float(saved_at),
                    clean_saved_sender(item.get("sender")),
                    clean_saved_direction(item.get("direction")),
                )
            )
        entries.sort(key=lambda entry: entry.saved_at, reverse=True)
        return cls(entries=tuple(entries))


@contextmanager
def saved_messages_lock(path: Union[str, Path]) -> Iterator[None]:
    """Serialize read-modify-write updates of the starred-message list.

    Same shape as the pinned lock in ``tau.pinned``: a flock on a
    ``.saved_messages.json.lock`` sidecar.  An atomic file replacement
    alone does not protect the whole update — without the lock, two
    save commands racing load the same file and the last save
    discards the other's change.  Callers must reload inside the
    lock.  Non-POSIX platforms get no lock, like the node lock.
    """
    if fcntl is None:  # pragma: no cover - non-POSIX
        yield
        return
    target = Path(path).expanduser()
    lock_path = target.with_name(f".{target.name}.lock")
    with open(lock_path, "a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def load_saved_messages(path: Optional[Union[str, Path]]) -> SavedMessages:
    """Load the starred-message list at *path*; a missing file means none starred."""
    if path is None:
        return SavedMessages()
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return SavedMessages()
    except OSError as exc:
        raise SavedMessagesError(
            f"could not read starred-message list {target}: {exc}"
        ) from exc
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SavedMessagesError(
            f"could not read starred-message list {target}: {exc}"
        ) from exc
    return SavedMessages.from_dict(data)


def save_saved_messages(path: Union[str, Path], saved: SavedMessages) -> Path:
    """Persist *saved* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    return atomic_write_json(target, saved.to_dict(), mode=SAVED_MESSAGES_FILE_MODE)
