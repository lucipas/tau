"""Owner-controlled pinned-conversation list.

The opposite of archiving: pinning a conversation keeps it at the
top of every conversation list — ``tau peers``, ``tau group list``,
``tau channel list``, and the console's conversation and channel
lists — so a quiet-but-important DM, group, or channel never sinks
out of sight.  Pinning never deletes anything, never stops delivery,
and never tells the peer: it is purely a local display preference,
like a muted thread or an archived conversation.

The list lives next to the archive list, persisted as JSON in the
data directory (``pinned.json``), with the same load/save pattern —
and the same failure direction as the archive list.  A pin list is
a display convenience, so a corrupt file fails *open*: the
conversations reappear unpinned, with a warning, rather than a bad
file silently reordering mail the owner never saw.

A group pin covers the whole rename lineage, like a group mute: a
rename makes a successor group with a new id, and the pin must
follow the conversation, not the id.  Callers pin every id in the
lineage (see :func:`tau.muted.group_lineage`); a conversation reads
as pinned when any of its lineage ids is pinned.
"""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, FrozenSet, Iterator, List, Optional, Tuple, Union

from .allowlist import AllowListError, clean_destination
from .channels import ChannelError, clean_channel_name
from .util import atomic_write_json

log = logging.getLogger(__name__)

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore[assignment]

PINNED_FILE_MODE = 0o600

# Local pins are capped like drafts: a peer cannot make this node
# hold unbounded state through any wire path (pins are local-only,
# so the cap is really about a runaway script), and
# `tau unpin-conversation` frees capacity when the cap is reached.
MAX_PINNED = 100

PINNED_KINDS = ("peer", "group", "channel")


class PinnedError(ValueError):
    """Raised when a pinned-conversation file or value is malformed."""


def clean_pinned_target(kind: str, value: Any) -> str:
    """Normalise a pin target for *kind*, or raise :class:`PinnedError`.

    Peers and groups are 32-hex destination hashes; channels are
    cleaned channel names.
    """
    if kind not in PINNED_KINDS:
        raise PinnedError(f"unknown conversation kind: {kind!r}")
    if kind == "channel":
        try:
            return clean_channel_name(value)
        except ChannelError as exc:
            raise PinnedError(str(exc)) from exc
    try:
        return clean_destination(value)
    except AllowListError as exc:
        raise PinnedError(str(exc)) from exc


@dataclass(frozen=True)
class PinnedEntry:
    """One pinned conversation, newest pin first in the list."""

    kind: str
    target: str
    pinned_at: float


@dataclass(frozen=True)
class PinnedList:
    """Pinned conversations, newest pin first."""

    entries: Tuple[PinnedEntry, ...] = ()

    def is_pinned(self, kind: str, target: Any) -> bool:
        """True when the conversation named by *kind*/*target* is pinned."""
        try:
            cleaned = clean_pinned_target(kind, target)
        except PinnedError:
            return False
        return any(
            entry.kind == kind and entry.target == cleaned
            for entry in self.entries
        )

    def pinned_targets(self, kind: str) -> FrozenSet[str]:
        """Targets pinned under *kind*, as cleaned values."""
        if kind not in PINNED_KINDS:
            raise PinnedError(f"unknown conversation kind: {kind!r}")
        return frozenset(
            entry.target for entry in self.entries if entry.kind == kind
        )

    def with_pinned(
        self, kind: str, target: Any, now: Optional[float] = None
    ) -> "PinnedList":
        """Pin the conversation, moving it to the front when already pinned."""
        cleaned = clean_pinned_target(kind, target)
        stamped = now if now is not None else time.time()
        rest = tuple(
            entry
            for entry in self.entries
            if not (entry.kind == kind and entry.target == cleaned)
        )
        if len(rest) >= MAX_PINNED:
            raise PinnedError(
                f"too many pinned conversations (max {MAX_PINNED}); "
                "unpin one first"
            )
        return PinnedList(
            entries=(PinnedEntry(kind, cleaned, stamped),) + rest
        )

    def without(self, kind: str, target: Any) -> "PinnedList":
        """Unpin the conversation; unpinning an unpinned one is a no-op."""
        cleaned = clean_pinned_target(kind, target)
        return PinnedList(
            entries=tuple(
                entry
                for entry in self.entries
                if not (entry.kind == kind and entry.target == cleaned)
            )
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pinned": [
                {
                    "kind": entry.kind,
                    "target": entry.target,
                    "pinned_at": entry.pinned_at,
                }
                for entry in self.entries
            ]
        }

    @classmethod
    def from_dict(cls, data: Any) -> "PinnedList":
        if not isinstance(data, dict):
            raise PinnedError("pinned-conversation list must be a JSON object")
        if "pinned" not in data:
            # A list that lost its pinned key must not silently turn
            # the ordering off; the fail-open loader treats the file
            # as corrupt instead.
            raise PinnedError("pinned is required")
        raw = data.get("pinned")
        if not isinstance(raw, list):
            raise PinnedError("pinned must be a list of pinned conversations")
        entries: List[PinnedEntry] = []
        seen = set()
        for item in raw:
            if not isinstance(item, dict):
                raise PinnedError("pinned entries must be JSON objects")
            kind = item.get("kind")
            if kind not in PINNED_KINDS:
                raise PinnedError(f"unknown conversation kind: {kind!r}")
            target = clean_pinned_target(kind, item.get("target"))
            key = (kind, target)
            if key in seen:
                raise PinnedError(f"duplicate pinned conversation: {key}")
            seen.add(key)
            pinned_at = item.get("pinned_at")
            if not isinstance(pinned_at, (int, float)) or isinstance(
                pinned_at, bool
            ):
                raise PinnedError("pinned_at must be a number")
            entries.append(
                PinnedEntry(kind, target, float(pinned_at))
            )
        entries.sort(key=lambda entry: entry.pinned_at, reverse=True)
        return cls(entries=tuple(entries))


def load_pinned(path: Optional[Union[str, Path]]) -> PinnedList:
    """Load the pinned-conversation list at *path*; a missing file means none pinned."""
    if path is None:
        return PinnedList()
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return PinnedList()
    except OSError as exc:
        raise PinnedError(
            f"could not read pinned-conversation list {target}: {exc}"
        ) from exc
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PinnedError(
            f"could not read pinned-conversation list {target}: {exc}"
        ) from exc
    return PinnedList.from_dict(data)


def save_pinned(path: Union[str, Path], pinned: PinnedList) -> Path:
    """Persist *pinned* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    return atomic_write_json(target, pinned.to_dict(), mode=PINNED_FILE_MODE)


@contextmanager
def pinned_lock(path: Union[str, Path]) -> Iterator[None]:
    """Serialize read-modify-write updates of the pinned list.

    Same shape as the archive lock in ``tau.archive``: a flock on a
    ``.pinned.json.lock`` sidecar.  An atomic file replacement alone
    does not protect the whole update — without the lock, two pin
    commands racing load the same file and the last save discards the
    other's change.  Callers must reload inside the lock.  Non-POSIX
    platforms get no lock, like the node lock.
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


def load_pinned_set(
    path: Optional[Union[str, Path]], kind: str
) -> FrozenSet[str]:
    """Load the pinned targets of *kind* at *path* for display ordering.

    Returns an empty set when the list cannot be read or parsed:
    display callers must then show everything unpinned (fail open),
    matching the pin list's role as a convenience — a corrupt file
    must never hide or reorder mail the owner has not seen.  The
    warning is what surfaces the corruption.
    """
    try:
        return load_pinned(path).pinned_targets(kind)
    except PinnedError as exc:
        log.warning(
            "Pinned-conversation list at %s is unusable (%s); showing everything unpinned",
            path,
            exc,
        )
        return frozenset()
