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

A mute may be timed: ``peer_until`` / ``group_until`` map a muted
key to the epoch second the mute lifts.  An entry with no expiry is
indefinite, as before, and files written before timed mutes existed
carry no expiry maps at all and load unchanged.  Expiry is enforced
at every read — an expired entry counts as unmuted — and expired
entries are dropped on load, so the file self-cleans on the next
save; a mute is never resurrected by a stale entry.
"""

from __future__ import annotations

import json
import logging
import math
import time
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


def _clean_until_map(
    values: Any, *, kind: str, keys: FrozenSet[str], now: float
) -> tuple[Dict[str, float], FrozenSet[str]]:
    """Validate an expiry map: keys must be muted, values epoch seconds.

    Returns the live expiries plus the keys whose mute has already
    expired — the caller drops those keys from the muted set itself,
    so an expired entry never survives as an indefinite mute.  A
    missing map means no timed mutes.  An expiry for a key that is
    not muted is dropped (it can only be stale — unmuting drops the
    expiry with the key); a malformed value fails loudly, like the
    rest of the file.
    """
    if values is None:
        return {}, frozenset()
    if not isinstance(values, dict):
        raise MutedError(f"{kind} mute expiries must be a JSON object")
    cleaned: Dict[str, float] = {}
    expired: set[str] = set()
    for key, until in values.items():
        if kind == "peer":
            cleaned_key = _clean_destination(key)
        else:
            cleaned_key = _clean_group_id(key)
        if cleaned_key not in keys:
            continue
        if isinstance(until, bool) or not isinstance(until, (int, float)):
            raise MutedError(f"{kind} mute expiry must be a number of seconds")
        until_float = float(until)
        if not math.isfinite(until_float) or until_float <= 0:
            raise MutedError(f"{kind} mute expiry must be a positive number")
        if until_float <= now:
            expired.add(cleaned_key)
            continue
        cleaned[cleaned_key] = until_float
    return cleaned, frozenset(expired)


def _with_until(
    untils: Dict[str, float], key: str, until: Optional[float]
) -> Dict[str, float]:
    """Copy *untils* with *key* set to *until*.

    ``until=None`` leaves any existing expiry alone: a plain re-mute
    of a timed-muted conversation is a no-op, not a silent promotion
    to indefinite (that takes ``--off`` followed by a fresh mute).
    An expiry must be a finite positive epoch, like the values
    ``from_dict`` accepts.
    """
    copied = dict(untils)
    if until is None:
        return copied
    if isinstance(until, bool) or not isinstance(until, (int, float)):
        raise MutedError("mute expiry must be a number of seconds")
    until_float = float(until)
    if not math.isfinite(until_float) or until_float <= 0:
        raise MutedError("mute expiry must be a positive number")
    copied[key] = until_float
    return copied


@dataclass(frozen=True)
class MutedConversations:
    """Muted DM peers and groups, as destination hashes and group ids.

    ``peer_until`` / ``group_until`` hold the epoch second a timed
    mute lifts, per muted key; a key absent from its map is muted
    indefinitely.  The maps are plain dicts (the dataclass is never
    hashed), rebuilt — never mutated — by the ``with_*`` methods.
    """

    peers: FrozenSet[str] = field(default_factory=frozenset)
    groups: FrozenSet[str] = field(default_factory=frozenset)
    peer_until: Dict[str, float] = field(default_factory=dict)
    group_until: Dict[str, float] = field(default_factory=dict)

    def _live(
        self, key: str, *, kind: str, now: Optional[float] = None
    ) -> bool:
        """Whether *key*'s mute is still in force: no expiry, or unexpired."""
        until = (self.peer_until if kind == "peer" else self.group_until).get(key)
        if until is None:
            return True
        now = time.time() if now is None else now
        return until > now

    def is_muted_peer(
        self, destination: Optional[str], now: Optional[float] = None
    ) -> bool:
        """Whether the DM peer is muted, or ``False`` when unusable."""
        if destination is None:
            return False
        try:
            key = clean_destination(destination)
        except AllowListError:
            return False
        return key in self.peers and self._live(key, kind="peer", now=now)

    def muted_until_peer(
        self, destination: Optional[str], now: Optional[float] = None
    ) -> Optional[float]:
        """Epoch second the peer's timed mute lifts, else ``None``."""
        if destination is None:
            return None
        try:
            key = clean_destination(destination)
        except AllowListError:
            return None
        if key not in self.peers or not self._live(key, kind="peer", now=now):
            return None
        return self.peer_until.get(key)

    def is_muted_group(
        self, group_id: Optional[str], now: Optional[float] = None
    ) -> bool:
        """Whether the group is muted, or ``False`` when unusable."""
        if group_id is None:
            return False
        try:
            key = _clean_group_id(group_id)
        except MutedError:
            return False
        return key in self.groups and self._live(key, kind="group", now=now)

    def muted_until_group(
        self, group_id: Optional[str], now: Optional[float] = None
    ) -> Optional[float]:
        """Epoch second the group's timed mute lifts, else ``None``."""
        if group_id is None:
            return None
        try:
            key = _clean_group_id(group_id)
        except MutedError:
            return None
        if key not in self.groups or not self._live(key, kind="group", now=now):
            return None
        return self.group_until.get(key)

    def with_peer(
        self, destination: str, until: Optional[float] = None
    ) -> "MutedConversations":
        key = _clean_destination(destination)
        peers = set(self.peers)
        peers.add(key)
        return MutedConversations(
            peers=frozenset(peers),
            groups=self.groups,
            peer_until=_with_until(self.peer_until, key, until),
            group_until=dict(self.group_until),
        )

    def without_peer(self, destination: str) -> "MutedConversations":
        key = _clean_destination(destination)
        peers = set(self.peers)
        peers.discard(key)
        peer_until = dict(self.peer_until)
        peer_until.pop(key, None)
        return MutedConversations(
            peers=frozenset(peers),
            groups=self.groups,
            peer_until=peer_until,
            group_until=dict(self.group_until),
        )

    def is_muted_group_lineage(
        self, lineage: Iterable[str], now: Optional[float] = None
    ) -> bool:
        """Whether any id of a group's rename lineage is muted."""
        return any(
            group_id in self.groups
            and self._live(group_id, kind="group", now=now)
            for group_id in lineage
        )

    def muted_until_group_lineage(
        self, lineage: Iterable[str], now: Optional[float] = None
    ) -> Optional[float]:
        """Epoch second the lineage's timed mute lifts, else ``None``.

        A lineage muted in one call shares one expiry, so the latest
        live expiry among its ids is the answer; an indefinite mute
        anywhere in the lineage means no expiry.
        """
        live = [
            group_id
            for group_id in lineage
            if group_id in self.groups
            and self._live(group_id, kind="group", now=now)
        ]
        if not live:
            return None
        untils = [self.group_until.get(group_id) for group_id in live]
        if any(until is None for until in untils):
            return None
        return max(until for until in untils if until is not None)

    def with_group_lineage(
        self, lineage: Iterable[str], until: Optional[float] = None
    ) -> "MutedConversations":
        groups = set(self.groups) | set(lineage)
        group_until = dict(self.group_until)
        for group_id in lineage:
            group_until = _with_until(group_until, group_id, until)
        return MutedConversations(
            peers=self.peers,
            groups=frozenset(groups),
            peer_until=dict(self.peer_until),
            group_until=group_until,
        )

    def without_group_lineage(
        self, lineage: Iterable[str]
    ) -> "MutedConversations":
        groups = set(self.groups) - set(lineage)
        group_until = dict(self.group_until)
        for group_id in lineage:
            group_until.pop(group_id, None)
        return MutedConversations(
            peers=self.peers,
            groups=frozenset(groups),
            peer_until=dict(self.peer_until),
            group_until=group_until,
        )

    def with_group(
        self, group_id: str, until: Optional[float] = None
    ) -> "MutedConversations":
        key = _clean_group_id(group_id)
        groups = set(self.groups)
        groups.add(key)
        return MutedConversations(
            peers=self.peers,
            groups=frozenset(groups),
            peer_until=dict(self.peer_until),
            group_until=_with_until(self.group_until, key, until),
        )

    def without_group(self, group_id: str) -> "MutedConversations":
        key = _clean_group_id(group_id)
        groups = set(self.groups)
        groups.discard(key)
        group_until = dict(self.group_until)
        group_until.pop(key, None)
        return MutedConversations(
            peers=self.peers,
            groups=frozenset(groups),
            peer_until=dict(self.peer_until),
            group_until=group_until,
        )

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "peers": sorted(self.peers),
            "groups": sorted(self.groups),
        }
        # Expiry maps are omitted when empty, so a file of indefinite
        # mutes keeps exactly the old shape.
        peer_until = {
            key: until
            for key, until in sorted(self.peer_until.items())
            if key in self.peers
        }
        if peer_until:
            data["peer_until"] = peer_until
        group_until = {
            key: until
            for key, until in sorted(self.group_until.items())
            if key in self.groups
        }
        if group_until:
            data["group_until"] = group_until
        return data

    @classmethod
    def from_dict(cls, data: Any, now: Optional[float] = None) -> "MutedConversations":
        if not isinstance(data, dict):
            raise MutedError("muted conversations must be a JSON object")
        # A file that lost a section must not silently unmute that
        # side; the fail-open loader treats the file as corrupt
        # instead.
        if "peers" not in data or "groups" not in data:
            raise MutedError("peers and groups are required")
        now = time.time() if now is None else now
        peers = _clean_muted(data.get("peers"), kind="peer")
        groups = _clean_muted(data.get("groups"), kind="group")
        peer_until, expired_peers = _clean_until_map(
            data.get("peer_until"), kind="peer", keys=peers, now=now
        )
        group_until, expired_groups = _clean_until_map(
            data.get("group_until"), kind="group", keys=groups, now=now
        )
        # A key whose expiry has passed leaves the muted set with it:
        # dropping only the expiry would promote the timed mute to an
        # indefinite one, and the count paths read the raw sets.
        return cls(
            peers=peers - expired_peers,
            groups=groups - expired_groups,
            peer_until=peer_until,
            group_until=group_until,
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
