"""Owner-controlled muted conversations.

A muted conversation still receives and lists its messages, but it
never lights the unread badge: its per-conversation unread count is
0, the total excludes it, and its messages carry no unread marker.
Archiving hides a conversation entirely and ignoring a group drops
it from the list; muting is the middle ground — visible, just quiet.

The mutes live next to the disappearing defaults, persisted as JSON
in the data directory (``muted.json``), with the same load/save
pattern as the archive list — and the same failure direction.  A
mute is a display preference, not a security boundary: a corrupt
file fails *open* in the count paths (the conversation counts as
unread again, with a warning) rather than breaking every unread
read, while the setters (CLI and console) fail loudly so a bad file
is never silently overwritten.  Unmuting restores normal counting
from that point on, including messages that arrived while muted —
the read cursor is untouched by muting, so nothing is lost.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, FrozenSet, Iterable, Iterator, Optional, Union

from .allowlist import AllowListError, clean_destination
from .util import atomic_write_json

log = logging.getLogger(__name__)

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore[assignment]

MUTED_FILE_MODE = 0o600


class MutedError(ValueError):
    """Raised when a muted-conversations file or value is malformed."""


def _clean_destination(value: Any) -> str:
    try:
        return clean_destination(value)
    except AllowListError as exc:
        raise MutedError(str(exc)) from exc


def _clean_group_id(value: Any) -> str:
    # Group ids are 32 lowercase hex characters (see
    # ``tau.groups._valid_group``); anything else is not a group.
    if not isinstance(value, str):
        raise MutedError("group id must be a string")
    cleaned = value.strip().lower()
    if len(cleaned) != 32:
        raise MutedError("group id must be 32 hex characters")
    try:
        bytes.fromhex(cleaned)
    except ValueError as exc:
        raise MutedError("group id must be 32 hex characters") from exc
    return cleaned


def _clean_muted(values: Any, *, kind: str) -> FrozenSet[str]:
    if not isinstance(values, list):
        raise MutedError(f"{kind} muted conversations must be a JSON array")
    cleaned = set()
    for key in values:
        if kind == "peer":
            try:
                cleaned.add(clean_destination(key))
            except AllowListError as exc:
                raise MutedError(str(exc)) from exc
        else:
            cleaned.add(_clean_group_id(key))
    return frozenset(cleaned)


@dataclass(frozen=True)
class MutedConversations:
    """Muted DM peers and groups, as destination hashes and group ids."""

    peers: FrozenSet[str] = field(default_factory=frozenset)
    groups: FrozenSet[str] = field(default_factory=frozenset)

    def is_muted_peer(self, destination: Optional[str]) -> bool:
        """Whether the DM peer is muted, or ``False`` when unusable."""
        if destination is None:
            return False
        try:
            return clean_destination(destination) in self.peers
        except AllowListError:
            return False

    def is_muted_group(self, group_id: Optional[str]) -> bool:
        """Whether the group is muted, or ``False`` when unusable."""
        if group_id is None:
            return False
        try:
            return _clean_group_id(group_id) in self.groups
        except MutedError:
            return False

    def with_peer(self, destination: str) -> "MutedConversations":
        peers = set(self.peers)
        peers.add(_clean_destination(destination))
        return MutedConversations(peers=frozenset(peers), groups=self.groups)

    def without_peer(self, destination: str) -> "MutedConversations":
        peers = set(self.peers)
        peers.discard(_clean_destination(destination))
        return MutedConversations(peers=frozenset(peers), groups=self.groups)

    def is_muted_group_lineage(self, lineage: Iterable[str]) -> bool:
        """Whether any id of a group's rename lineage is muted."""
        return any(group_id in self.groups for group_id in lineage)

    def with_group_lineage(self, lineage: Iterable[str]) -> "MutedConversations":
        groups = set(self.groups) | set(lineage)
        return MutedConversations(peers=self.peers, groups=frozenset(groups))

    def without_group_lineage(self, lineage: Iterable[str]) -> "MutedConversations":
        groups = set(self.groups) - set(lineage)
        return MutedConversations(peers=self.peers, groups=frozenset(groups))

    def with_group(self, group_id: str) -> "MutedConversations":
        groups = set(self.groups)
        groups.add(_clean_group_id(group_id))
        return MutedConversations(peers=self.peers, groups=frozenset(groups))

    def without_group(self, group_id: str) -> "MutedConversations":
        groups = set(self.groups)
        groups.discard(_clean_group_id(group_id))
        return MutedConversations(peers=self.peers, groups=frozenset(groups))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "peers": sorted(self.peers),
            "groups": sorted(self.groups),
        }

    @classmethod
    def from_dict(cls, data: Any) -> "MutedConversations":
        if not isinstance(data, dict):
            raise MutedError("muted conversations must be a JSON object")
        # A file that lost a section must not silently unmute that
        # side; the fail-open loader treats the file as corrupt
        # instead.
        if "peers" not in data or "groups" not in data:
            raise MutedError("peers and groups are required")
        return cls(
            peers=_clean_muted(data.get("peers"), kind="peer"),
            groups=_clean_muted(data.get("groups"), kind="group"),
        )


def group_lineage(record: Any) -> FrozenSet[str]:
    """A group's id plus its rename predecessors, as valid group ids.

    A rename makes a successor group with a new id, and messages sent
    before it still carry the predecessor ids in ``renamed_from``.  A
    group mute covers the whole lineage, so the conversation stays
    muted across a rename.  Malformed entries are skipped.
    """
    if not isinstance(record, dict):
        return frozenset()
    ids = [record.get("id"), *(record.get("renamed_from") or [])]
    cleaned = set()
    for value in ids:
        try:
            cleaned.add(_clean_group_id(value))
        except MutedError:
            continue
    return frozenset(cleaned)


def expand_muted_groups(
    groups: FrozenSet[str], records: Iterable[Any]
) -> FrozenSet[str]:
    """Add the lineage of every group that shares a muted id.

    A group muted before a rename stores only its old id; the successor
    is the group whose lineage names that id, so its new posts must
    stay muted too.  Lineages are matched in both directions this way.
    """
    if not groups:
        return groups
    expanded = set(groups)
    for record in records:
        lineage = group_lineage(record)
        if lineage & groups:
            expanded |= lineage
    return frozenset(expanded)


def load_muted(path: Optional[Union[str, Path]]) -> MutedConversations:
    """Load the muted conversations at *path*; a missing file means none muted."""
    if path is None:
        return MutedConversations()
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return MutedConversations()
    except OSError as exc:
        # The file is there but cannot be inspected (permissions, a
        # broken path component): fail loudly rather than treat it as
        # missing and silently unmute conversations the owner muted.
        raise MutedError(
            f"could not read muted conversations {target}: {exc}"
        ) from exc
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise MutedError(
            f"could not read muted conversations {target}: {exc}"
        ) from exc
    return MutedConversations.from_dict(data)


def save_muted(path: Union[str, Path], muted: MutedConversations) -> Path:
    """Persist *muted* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    return atomic_write_json(target, muted.to_dict(), mode=MUTED_FILE_MODE)


@contextmanager
def muted_lock(path: Union[str, Path]) -> Iterator[None]:
    """Serialize read-modify-write updates of the muted conversations.

    Same shape as the node.json lock in ``tau.config``: a flock on a
    ``.muted.json.lock`` sidecar.  An atomic file replacement alone
    does not protect the whole update — without the lock, two set
    commands racing load the same file and the last save discards the
    other's change.  Callers must reload inside the lock.
    Non-POSIX platforms get no lock, like the node lock.
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


def muted_peers_for(path: Optional[Union[str, Path]]) -> FrozenSet[str]:
    """Muted DM peers, failing open on a corrupt file.

    Count paths use this: a corrupt mutes file must never break an
    unread read — the conversations count as unread again, and the
    warning is what surfaces the corruption.
    """
    try:
        return load_muted(path).peers
    except MutedError as exc:
        log.warning(
            "Muted conversations at %s are unusable (%s); counting as unmuted",
            path,
            exc,
        )
        return frozenset()


def muted_groups_for(path: Optional[Union[str, Path]]) -> FrozenSet[str]:
    """Muted groups, failing open on a corrupt file."""
    try:
        return load_muted(path).groups
    except MutedError as exc:
        log.warning(
            "Muted conversations at %s are unusable (%s); counting as unmuted",
            path,
            exc,
        )
        return frozenset()
