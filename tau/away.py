"""Away message: one automatic reply to each peer while the owner is away.

A DM sent while the owner is away just sits unread, and the sender
cannot tell whether it was seen, ignored, or never arrived.  The away
reply answers that once per peer per away period: the first DM from
an established peer gets the away message, later DMs from the same
peer get none until away is turned off and back on.

The state lives next to the muted and disappearing stores, persisted
as JSON in the data directory (``away.json``), with the same
load/save/lock pattern — and the same failure direction.  A corrupt
file fails *open* on the receive path (no reply, with a warning)
rather than breaking delivery, while the setters (CLI) fail loudly
so a bad file is never silently overwritten.
"""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, FrozenSet, Iterator, Optional, Union

from .allowlist import AllowListError, clean_destination
from .protocol import MAX_MESSAGE_CHARS, ProtocolError, normalize_message
from .util import atomic_write_json

log = logging.getLogger(__name__)

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore[assignment]

AWAY_FILE_MODE = 0o600

DEFAULT_AWAY_MESSAGE = "I'm away right now. I'll reply when I'm back."


class AwayError(ValueError):
    """Raised when an away state file or value is malformed."""


def _clean_message(value: Any) -> str:
    if not isinstance(value, str):
        raise AwayError("away message must be a string")
    try:
        cleaned = normalize_message(value)
    except ProtocolError as exc:
        raise AwayError(str(exc)) from exc
    if not cleaned:
        raise AwayError("away message must not be empty")
    return cleaned


def _clean_since(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AwayError("away since must be a number or null")
    try:
        since = float(value)
    except (OverflowError, ValueError) as exc:
        raise AwayError("away since must be a finite number") from exc
    if since != since or since in (float("inf"), float("-inf")):
        raise AwayError("away since must be a finite number")
    return since


def _clean_replied_to(values: Any) -> FrozenSet[str]:
    if not isinstance(values, list):
        raise AwayError("away replied_to must be a JSON array")
    cleaned = set()
    for key in values:
        try:
            cleaned.add(clean_destination(key))
        except AllowListError as exc:
            raise AwayError(str(exc)) from exc
    return frozenset(cleaned)


@dataclass(frozen=True)
class AwayState:
    """Whether the away reply is on, what it says, and who got one.

    ``replied_to`` holds the destination hashes that already received
    an auto-reply in this away period; it resets when away is turned
    on, so a fresh period answers every peer once again.
    """

    enabled: bool = False
    message: str = DEFAULT_AWAY_MESSAGE
    since: Optional[float] = None
    replied_to: FrozenSet[str] = field(default_factory=frozenset)

    @classmethod
    def on(cls, message: Optional[str], *, now: Optional[float] = None) -> "AwayState":
        """An enabled state with *message* (or the default), fresh period."""
        text = (
            DEFAULT_AWAY_MESSAGE if message is None else _clean_message(message)
        )
        return cls(
            enabled=True,
            message=text,
            since=now if now is not None else time.time(),
            replied_to=frozenset(),
        )

    def off(self) -> "AwayState":
        """The same message, disabled, with the per-peer list cleared."""
        return AwayState(
            enabled=False,
            message=self.message,
            since=None,
            replied_to=frozenset(),
        )

    def with_replied(self, destination: str) -> "AwayState":
        return AwayState(
            enabled=self.enabled,
            message=self.message,
            since=self.since,
            replied_to=self.replied_to | {_clean_destination(destination)},
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "message": self.message,
            "since": self.since,
            "replied_to": sorted(self.replied_to),
        }

    @classmethod
    def from_dict(cls, data: Any) -> "AwayState":
        if not isinstance(data, dict):
            raise AwayError("away state must be a JSON object")
        # A file that lost a section must not silently change what the
        # owner is away with; the fail-open loader treats the file as
        # corrupt instead.
        if "enabled" not in data or "message" not in data:
            raise AwayError("enabled and message are required")
        if not isinstance(data["enabled"], bool):
            raise AwayError("enabled must be a boolean")
        return cls(
            enabled=data["enabled"],
            message=_clean_message(data["message"]),
            since=_clean_since(data.get("since")),
            replied_to=_clean_replied_to(data.get("replied_to", [])),
        )


def _clean_destination(value: Any) -> str:
    try:
        return clean_destination(value)
    except AllowListError as exc:
        raise AwayError(str(exc)) from exc


def load_away(path: Optional[Union[str, Path]]) -> AwayState:
    """Load the away state at *path*; a missing file means not away."""
    if path is None:
        return AwayState()
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return AwayState()
    except OSError as exc:
        # The file is there but cannot be inspected (permissions, a
        # broken path component): fail loudly rather than treat it as
        # missing and silently drop an away reply the owner set.
        raise AwayError(f"could not read away state {target}: {exc}") from exc
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise AwayError(f"could not read away state {target}: {exc}") from exc
    return AwayState.from_dict(data)


def save_away(path: Union[str, Path], state: AwayState) -> Path:
    """Persist *state* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    return atomic_write_json(target, state.to_dict(), mode=AWAY_FILE_MODE)


@contextmanager
def away_lock(path: Union[str, Path]) -> Iterator[None]:
    """Serialize read-modify-write updates of the away state.

    Same shape as the node.json lock in ``tau.config``: a flock on a
    ``.away.json.lock`` sidecar.  An atomic file replacement alone
    does not protect the whole update — without the lock, two
    deliveries racing would both see a peer as unreplied and queue
    two replies.  Callers must reload inside the lock.  Non-POSIX
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


def away_state_for(path: Optional[Union[str, Path]]) -> AwayState:
    """Away state, failing open on a corrupt file.

    The receive path uses this: a corrupt away file must never break
    a delivery — the message is stored and no reply goes out, and the
    warning is what surfaces the corruption.
    """
    try:
        return load_away(path)
    except AwayError as exc:
        log.warning(
            "Away state at %s is unusable (%s); sending no auto-reply",
            path,
            exc,
        )
        return AwayState()
