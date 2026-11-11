"""Owner-controlled archive list for direct-message conversations.

The middle option between deleting history and silencing a peer:
archiving a conversation hides it from the default ``tau messages``
listing while the messages keep arriving and stay stored, so
unarchiving restores the full history.  Archiving never deletes
anything, never stops delivery, and never tells the peer — it is
purely a local display preference, like a muted thread.

The list lives next to the block-list, persisted as JSON in the data
directory (``archived.json``), with the same load/save pattern — but
the opposite failure direction.  A block-list is a security boundary
and fails closed; an archive list is a display convenience, so a
corrupt file fails *open*: the conversations reappear, with a
warning, rather than a bad file silently hiding mail the owner
never saw.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, FrozenSet, Optional, Union

from .allowlist import AllowListError, clean_destination
from .util import atomic_write_json

log = logging.getLogger(__name__)

ARCHIVE_FILE_MODE = 0o600


class ArchiveError(ValueError):
    """Raised when an archive-list file or value is malformed."""


def _clean_destinations(values: Any) -> FrozenSet[str]:
    # A present ``null`` is malformed, not an empty list: treating it
    # as empty would switch the filter off instead of failing loudly.
    if not isinstance(values, (list, tuple, set, frozenset)):
        raise ArchiveError("archived must be a list of destination hashes")
    try:
        return frozenset(clean_destination(value) for value in values)
    except AllowListError as exc:
        raise ArchiveError(str(exc)) from exc


@dataclass(frozen=True)
class ArchiveList:
    """Which destination hashes have archived conversations, if any."""

    archived: FrozenSet[str] = field(default_factory=frozenset)

    def is_archived(self, destination: Optional[str]) -> bool:
        """True when *destination*'s conversation is archived."""
        if destination is None:
            return False
        try:
            return clean_destination(destination) in self.archived
        except AllowListError:
            return False

    def with_added(self, destination: str) -> "ArchiveList":
        return ArchiveList(
            archived=self.archived | {clean_destination(destination)},
        )

    def with_removed(self, destination: str) -> "ArchiveList":
        return ArchiveList(
            archived=self.archived - {clean_destination(destination)},
        )

    def to_dict(self) -> Dict[str, Any]:
        return {"archived": sorted(self.archived)}

    @classmethod
    def from_dict(cls, data: Any) -> "ArchiveList":
        if not isinstance(data, dict):
            raise ArchiveError("archive-list must be a JSON object")
        if "archived" not in data:
            # A list that lost its archived key must not silently
            # turn the filter off; the fail-open loader treats the
            # file as corrupt instead.
            raise ArchiveError("archived is required")
        return cls(archived=_clean_destinations(data.get("archived")))


def load_archive(path: Optional[Union[str, Path]]) -> ArchiveList:
    """Load the archive-list at *path*; a missing file means none archived."""
    if path is None:
        return ArchiveList()
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return ArchiveList()
    except OSError as exc:
        # The file is there but cannot be inspected (permissions, a
        # broken path component): fail loudly rather than treat it as
        # missing and silently un-archive conversations.
        raise ArchiveError(f"could not read archive-list {target}: {exc}") from exc
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ArchiveError(f"could not read archive-list {target}: {exc}") from exc
    return ArchiveList.from_dict(data)


def save_archive(path: Union[str, Path], archive: ArchiveList) -> Path:
    """Persist *archive* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    return atomic_write_json(target, archive.to_dict(), mode=ARCHIVE_FILE_MODE)


def load_archived_set(path: Optional[Union[str, Path]]) -> FrozenSet[str]:
    """Load the archived hashes at *path* for display filtering.

    Returns an empty set when the list cannot be read or parsed:
    display callers must then show everything (fail open), matching
    the archive's role as a convenience — a corrupt file must never
    hide mail the owner has not seen.  The warning is what surfaces
    the corruption.
    """
    try:
        return load_archive(path).archived
    except ArchiveError as exc:
        log.warning("Archive-list at %s is unusable (%s); showing everything", path, exc)
        return frozenset()
