"""Recurring daily quiet hours.

A timed mute silences one conversation once; quiet hours silence the
whole node every day between two wall-clock times — 22:00 to 07:00
means the unread badge stays dark overnight without re-muting every
conversation each evening.  While the window is active, every
conversation's unread count reads 0 and no message carries the unread
marker, in ``tau messages`` (counts and listings) and in the console's
unread counts — exactly as if every conversation were muted.  The read
cursors are untouched, so when the window ends the counts come back,
including messages that arrived during quiet hours.

The window lives in its own file (``quiet_hours.json`` in the data
directory) next to ``muted.json``, with the same load/save pattern and
the same failure direction.  A corrupt file fails *open* in the count
paths (the badge lights again, with a warning) rather than breaking
every unread read, while the setters (CLI) fail loudly so a bad file
is never silently overwritten.

Times are local wall-clock ``HH:MM`` (24-hour).  A window whose end
is earlier than its start crosses midnight; start equal to end is
refused — it would mean either always or never quiet, and neither is
what the owner asked for.
"""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional, Union

from .util import atomic_write_json

log = logging.getLogger(__name__)

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore[assignment]

QUIET_HOURS_FILE_MODE = 0o600

MINUTES_PER_DAY = 24 * 60


class QuietHoursError(ValueError):
    """Raised when a quiet-hours file or value is malformed."""


def _clean_time(value: Any, *, label: str) -> int:
    """Parse an ``HH:MM`` wall-clock time to minutes since midnight."""
    if not isinstance(value, str):
        raise QuietHoursError(f"quiet-hours {label} must be an HH:MM time")
    parts = value.strip().split(":")
    if (
        len(parts) != 2
        or not parts[0].strip().isdigit()
        or not parts[1].strip().isdigit()
    ):
        raise QuietHoursError(f"quiet-hours {label} must be an HH:MM time")
    hour = int(parts[0], 10)
    minute = int(parts[1], 10)
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        raise QuietHoursError(f"quiet-hours {label} must be an HH:MM time")
    return hour * 60 + minute


def format_time(minutes: int) -> str:
    """Minutes since midnight as an ``HH:MM`` wall-clock time."""
    return f"{minutes // 60:02d}:{minutes % 60:02d}"


def _local_minutes(now: Optional[float] = None) -> int:
    """Minutes since local midnight for *now* (an epoch, defaulting to now)."""
    local = time.localtime(now)
    return local.tm_hour * 60 + local.tm_min


@dataclass(frozen=True)
class QuietHours:
    """A daily do-not-disturb window, as minutes since local midnight.

    ``enabled`` is False when the owner turned the window off (the
    start/end are kept, so turning it back on restores the same
    window).  A disabled window is never quiet.
    """

    enabled: bool = False
    start: int = 22 * 60
    end: int = 7 * 60

    def __post_init__(self) -> None:
        for label, value in (("start", self.start), ("end", self.end)):
            if isinstance(value, bool) or not isinstance(value, int):
                raise QuietHoursError(f"quiet-hours {label} must be an HH:MM time")
            if not 0 <= value < MINUTES_PER_DAY:
                raise QuietHoursError(f"quiet-hours {label} must be an HH:MM time")
        if self.start == self.end:
            raise QuietHoursError("quiet-hours start and end must differ")

    def is_quiet(self, now: Optional[float] = None) -> bool:
        """Whether *now* (an epoch, defaulting to now) falls in the window."""
        if not self.enabled:
            return False
        current = _local_minutes(now)
        if self.start < self.end:
            return self.start <= current < self.end
        return current >= self.start or current < self.end

    def minutes_until_end(self, now: Optional[float] = None) -> Optional[int]:
        """Minutes until the window ends, or ``None`` when not quiet."""
        if not self.is_quiet(now):
            return None
        current = _local_minutes(now)
        if self.start < self.end:
            return self.end - current
        if current >= self.start:
            return MINUTES_PER_DAY - current + self.end
        return self.end - current

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "start": format_time(self.start),
            "end": format_time(self.end),
        }

    @classmethod
    def from_dict(cls, data: Any) -> "QuietHours":
        if not isinstance(data, dict):
            raise QuietHoursError("quiet hours must be a JSON object")
        if "start" not in data or "end" not in data:
            raise QuietHoursError("quiet-hours start and end are required")
        enabled = data.get("enabled", True)
        if not isinstance(enabled, bool):
            raise QuietHoursError("quiet-hours enabled must be true or false")
        return cls(
            enabled=enabled,
            start=_clean_time(data.get("start"), label="start"),
            end=_clean_time(data.get("end"), label="end"),
        )


def load_quiet_hours(path: Optional[Union[str, Path]]) -> QuietHours:
    """Load the quiet-hours window at *path*; a missing file means off."""
    if path is None:
        return QuietHours()
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return QuietHours()
    except OSError as exc:
        # The file is there but cannot be inspected (permissions, a
        # broken path component): fail loudly rather than treat it as
        # missing and silently drop a window the owner set.
        raise QuietHoursError(
            f"could not read quiet hours {target}: {exc}"
        ) from exc
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise QuietHoursError(
            f"could not read quiet hours {target}: {exc}"
        ) from exc
    return QuietHours.from_dict(data)


def save_quiet_hours(path: Union[str, Path], quiet_hours: QuietHours) -> Path:
    """Persist *quiet_hours* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    return atomic_write_json(target, quiet_hours.to_dict(), mode=QUIET_HOURS_FILE_MODE)


@contextmanager
def quiet_hours_lock(path: Union[str, Path]) -> Iterator[None]:
    """Serialize read-modify-write updates of the quiet-hours window.

    Same shape as the muted-conversations lock: a flock on a
    ``.quiet_hours.json.lock`` sidecar.  Callers must reload inside
    the lock.  Non-POSIX platforms get no lock, like the node lock.
    """
    if fcntl is None:  # pragma: no cover - non-POSIX
        yield
        return
    target = Path(path).expanduser()
    # A fresh data directory has no parent for the sidecar yet; the
    # save creates it anyway, so make it here instead of failing the
    # first `set` on a node that has never written a data file.
    target.parent.mkdir(parents=True, exist_ok=True)
    lock_path = target.with_name(f".{target.name}.lock")
    with open(lock_path, "a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def quiet_hours_active(
    path: Optional[Union[str, Path]], now: Optional[float] = None
) -> bool:
    """Whether quiet hours are in force right now, failing open.

    Count paths use this: a corrupt window file must never break an
    unread read — the badge lights again, and the warning is what
    surfaces the corruption.
    """
    try:
        return load_quiet_hours(path).is_quiet(now)
    except QuietHoursError as exc:
        log.warning(
            "Quiet hours at %s are unusable (%s); counting as not quiet",
            path,
            exc,
        )
        return False
