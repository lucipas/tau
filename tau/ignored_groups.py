"""Owner-controlled ignore list for group DMs.

The group analogue of DM archive: ignoring a group hides its messages
from the default ``tau messages`` listing while the messages keep
arriving and stay stored, so unignoring restores the full history.
A member of a group they did not create has no other way out — ``tau
group delete`` only drops the local record and the next incoming group
message re-pins it, and blocking the sender does not hide group posts,
which are the group's mail, not the sender's DM.  Ignoring never
deletes anything, never stops delivery, and never tells the group —
it is purely a local display preference, like a muted thread.

The list lives next to the block-list, persisted as JSON in the data
directory (``ignored-groups.json``), with the same load/save pattern
as the archive list — and the same failure direction.  An ignore list
is a display convenience, not a security boundary, so a corrupt file
fails *open*: the groups reappear, with a warning, rather than a bad
file silently hiding mail the owner never saw.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, FrozenSet, Iterator, Optional, Union

from .util import atomic_write_json

log = logging.getLogger(__name__)

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore[assignment]

IGNORED_GROUPS_FILE_MODE = 0o600


class IgnoredGroupsError(ValueError):
    """Raised when an ignored-groups file or value is malformed."""


def _clean_group_ids(values: Any) -> FrozenSet[str]:
    # A present ``null`` is malformed, not an empty list: treating it
    # as empty would switch the filter off instead of failing loudly.
    # Group ids are 32 lowercase hex characters (see
    # ``tau.groups._valid_group``); anything else is not a group.
    if not isinstance(values, (list, tuple, set, frozenset)):
        raise IgnoredGroupsError("ignored must be a list of group ids")
    cleaned = set()
    for value in values:
        if not isinstance(value, str):
            raise IgnoredGroupsError(f"not a group id: {value!r}")
        lowered = value.lower()
        if len(lowered) != 32:
            raise IgnoredGroupsError(f"not a group id: {value!r}")
        try:
            bytes.fromhex(lowered)
        except ValueError:
            raise IgnoredGroupsError(f"not a group id: {value!r}") from None
        cleaned.add(lowered)
    return frozenset(cleaned)


@dataclass(frozen=True)
class IgnoredGroups:
    """Which group ids have their messages ignored, if any."""

    ignored: FrozenSet[str] = field(default_factory=frozenset)

    def is_ignored(self, group_id: Optional[str]) -> bool:
        """True when *group_id*'s messages are ignored."""
        if group_id is None:
            return False
        if not isinstance(group_id, str):
            return False
        return group_id.lower() in self.ignored

    def with_added(self, group_id: str) -> "IgnoredGroups":
        return IgnoredGroups(
            ignored=self.ignored | _clean_group_ids([group_id]),
        )

    def with_removed(self, group_id: str) -> "IgnoredGroups":
        return IgnoredGroups(
            ignored=self.ignored - _clean_group_ids([group_id]),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {"ignored": sorted(self.ignored)}

    @classmethod
    def from_dict(cls, data: Any) -> "IgnoredGroups":
        if not isinstance(data, dict):
            raise IgnoredGroupsError("ignored-groups must be a JSON object")
        if "ignored" not in data:
            # A list that lost its ignored key must not silently
            # turn the filter off; the fail-open loader treats the
            # file as corrupt instead.
            raise IgnoredGroupsError("ignored is required")
        return cls(ignored=_clean_group_ids(data.get("ignored")))


def load_ignored_groups(path: Optional[Union[str, Path]]) -> IgnoredGroups:
    """Load the ignored-groups list at *path*; a missing file means none ignored."""
    if path is None:
        return IgnoredGroups()
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return IgnoredGroups()
    except OSError as exc:
        # The file is there but cannot be inspected (permissions, a
        # broken path component): fail loudly rather than treat it as
        # missing and silently un-ignore groups.
        raise IgnoredGroupsError(
            f"could not read ignored-groups {target}: {exc}"
        ) from exc
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise IgnoredGroupsError(
            f"could not read ignored-groups {target}: {exc}"
        ) from exc
    return IgnoredGroups.from_dict(data)


def save_ignored_groups(path: Union[str, Path], ignored: IgnoredGroups) -> Path:
    """Persist *ignored* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    return atomic_write_json(target, ignored.to_dict(), mode=IGNORED_GROUPS_FILE_MODE)


@contextmanager
def ignored_groups_lock(path: Union[str, Path]) -> Iterator[None]:
    """Serialize read-modify-write updates of the ignored-groups list.

    Same shape as the node.json lock in ``tau.config``: a flock on a
    ``.ignored-groups.json.lock`` sidecar.  An atomic file replacement
    alone does not protect the whole update — without the lock, two
    ignore commands racing load the same file and the last save
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


def load_ignored_group_ids(path: Optional[Union[str, Path]]) -> FrozenSet[str]:
    """Load the ignored group ids at *path* for display filtering.

    Returns an empty set when the list cannot be read or parsed:
    display callers must then show everything (fail open), matching
    the ignore list's role as a convenience — a corrupt file must never
    hide mail the owner has not seen.  The warning is what surfaces
    the corruption.
    """
    try:
        return load_ignored_groups(path).ignored
    except IgnoredGroupsError as exc:
        log.warning(
            "Ignored-groups at %s is unusable (%s); showing everything", path, exc
        )
        return frozenset()
