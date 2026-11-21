"""Durable broadcast-list store: named sets of peers for one-to-many DMs.

A broadcast list is a name plus a fixed set of peer destination hashes,
owned by this node only.  Unlike a group it has no shared context: a
``tau broadcast send`` fans out as one ordinary DM per member, so a
recipient never sees the other members, the list name, or that the
message was a broadcast — and a reply comes back as a normal one-to-one
DM.  Lists never travel on the wire and carry no protocol of their own:
the store is local state, like the peer registry, and the send path is
just ``tau send`` with the list's members as targets.

Names are unique case-insensitively.  A list needs at least one member
(a list with nobody to send to is refused instead of stored empty);
``tau broadcast delete`` removes the whole list.  The store is capped
at 50 lists of 50 members each, mirroring the group caps.
"""

from __future__ import annotations

import logging
import math
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

try:  # POSIX only; Windows locks through msvcrt instead (see below).
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None  # type: ignore[assignment]

try:  # Windows only; the lock uses msvcrt there, as identity.py does.
    import msvcrt
except ImportError:  # pragma: no cover - POSIX platforms
    msvcrt = None  # type: ignore[assignment]

from .protocol import sanitize_group_name
from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

BROADCASTS_FILE_NAME = "broadcasts.json"
# Local lists and their members are capped like groups: a peer cannot
# make this node hold unbounded state through any wire path (lists are
# local-only, so the caps are really about a runaway script), and
# `tau broadcast delete` frees capacity when a cap is reached.
MAX_BROADCASTS = 50
MAX_BROADCAST_MEMBERS = 50


def _valid_hash(value: Any) -> Optional[str]:
    """A 32-hex destination hash, lowercased, or ``None`` when unusable."""
    if not isinstance(value, str) or len(value) != 32:
        return None
    try:
        bytes.fromhex(value)
    except ValueError:
        return None
    return value.lower()


def _valid_broadcast(raw: Any) -> Optional[Dict[str, Any]]:
    """Normalize one stored broadcast list, or ``None`` when it is unusable."""
    if not isinstance(raw, dict):
        return None
    name = raw.get("name")
    if not isinstance(name, str) or not name.strip():
        return None
    name = sanitize_group_name(name)
    members = raw.get("members")
    if not isinstance(members, list):
        return None
    cleaned: List[str] = []
    for member in members:
        hashed = _valid_hash(member)
        if hashed is None:
            return None
        cleaned.append(hashed)
    if (
        len(cleaned) < 1
        or len(cleaned) > MAX_BROADCAST_MEMBERS
        or len(set(cleaned)) != len(cleaned)
    ):
        return None
    created_at = raw.get("created_at")
    try:
        created_at = float(created_at) if created_at is not None else time.time()
    except (TypeError, ValueError):
        return None
    if not math.isfinite(created_at):
        return None
    return {
        "name": name,
        "members": sorted(cleaned),
        "created_at": created_at,
    }


class BroadcastStore:
    """Named broadcast lists, persisted in the node data directory.

    A node without a data directory (tests, embeds) keeps lists in
    memory only.  Writes are atomic and the file is mode 0600, like the
    peer registry and mailbox; a failed write only logs a warning.
    """

    def __init__(self, path: Optional[Union[str, Path]] = None) -> None:
        self.path = Path(path).expanduser() if path is not None else None
        self._lock = threading.Lock()
        # Keyed by name casefold: names are unique case-insensitively.
        self._lists: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if self.path is None:
            return
        raw = read_json(self.path, default=None)
        if not isinstance(raw, dict):
            return
        entries = raw.get("broadcasts")
        if not isinstance(entries, list):
            return
        loaded: Dict[str, Dict[str, Any]] = {}
        for entry in entries:
            broadcast = _valid_broadcast(entry)
            if broadcast is not None:
                loaded[broadcast["name"].casefold()] = broadcast
        self._lists = loaded

    def _save(self) -> None:
        """Persist the store; raises :class:`OSError` when the write fails."""
        if self.path is None:
            return
        atomic_write_json(
            self.path,
            {"broadcasts": list(self._lists.values())},
            mode=0o600,
        )

    def _snapshot(self) -> Dict[str, Dict[str, Any]]:
        """In-memory state before a mutation, for rollback on a failed save."""
        return dict(self._lists)

    def _restore(self, snapshot: Dict[str, Dict[str, Any]]) -> None:
        """Restore the state captured by :meth:`_snapshot`."""
        self._lists = dict(snapshot)

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        """Serialise a read-modify-write of the store across processes.

        Mirrors :meth:`GroupStore._transaction`: each mutation takes an
        exclusive lock on a sidecar lock file and reloads the latest
        on-disk state first, so a serving process and a CLI command
        cannot interleave a load and a save.
        """
        with self._lock:
            if self.path is None:
                yield
                return
            if fcntl is None and msvcrt is None:
                # pragma: no cover - neither platform offers a
                # cross-process lock; best effort only.
                self._load()
                yield
                return
            lock_path = self.path.with_name(self.path.name + ".lock")
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            with open(lock_path, "a+") as handle:
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                else:  # pragma: no cover - exercised on Windows
                    handle.seek(0, os.SEEK_END)
                    if handle.tell() == 0:
                        handle.write(" ")
                        handle.flush()
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                try:
                    self._load()
                    yield
                finally:
                    if fcntl is not None:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    else:  # pragma: no cover - exercised on Windows
                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)

    def create(self, name: str, members: List[str]) -> Dict[str, Any]:
        """Create a broadcast list and persist it.

        Raises :class:`ValueError` when the name is blank, the list
        already exists, the members are empty or over the cap, or the
        list cap is reached, and :class:`OSError` when the write fails
        (the in-memory store is left as it was on disk).
        """
        if not isinstance(name, str) or not name.strip():
            raise ValueError("a broadcast list needs a name")
        broadcast = _valid_broadcast(
            {"name": name, "members": members, "created_at": time.time()}
        )
        if broadcast is None:
            raise ValueError(
                "invalid broadcast list: needs 1.."
                f"{MAX_BROADCAST_MEMBERS} distinct members"
            )
        with self._transaction():
            key = broadcast["name"].casefold()
            if key in self._lists:
                raise ValueError(
                    f"a broadcast list named {broadcast['name']!r} already exists"
                )
            if len(self._lists) >= MAX_BROADCASTS:
                raise ValueError(
                    f"broadcast list limit reached ({MAX_BROADCASTS} lists;"
                    " delete one with `tau broadcast delete`)"
                )
            snapshot = self._snapshot()
            self._lists[key] = broadcast
            try:
                self._save()
            except OSError:
                self._restore(snapshot)
                raise
        return dict(broadcast)

    def add_members(self, name: str, members: List[str]) -> Dict[str, Any]:
        """Add members to a list, ignoring ones already on it.

        Raises :class:`ValueError` when the list is unknown or the
        member cap would be exceeded, and :class:`OSError` when the
        write fails.
        """
        new = [_valid_hash(member) for member in members]
        if any(member is None for member in new):
            raise ValueError("members must be destination hashes")
        with self._transaction():
            broadcast = self._lists.get(name.strip().casefold())
            if broadcast is None:
                raise ValueError(f"unknown broadcast list: {name!r}")
            merged = sorted(set(broadcast["members"]) | set(new))  # type: ignore[arg-type]
            if len(merged) > MAX_BROADCAST_MEMBERS:
                raise ValueError(
                    "a broadcast list has at most "
                    f"{MAX_BROADCAST_MEMBERS} members"
                )
            snapshot = self._snapshot()
            updated = dict(broadcast, members=merged)
            self._lists[broadcast["name"].casefold()] = updated
            try:
                self._save()
            except OSError:
                self._restore(snapshot)
                raise
        return dict(updated)

    def remove_members(self, name: str, members: List[str]) -> Dict[str, Any]:
        """Remove members from a list, ignoring ones not on it.

        Raises :class:`ValueError` when the list is unknown or the
        removal would leave it empty (delete the list instead), and
        :class:`OSError` when the write fails.
        """
        doomed = {_valid_hash(member) for member in members}
        if any(member is None for member in doomed):
            raise ValueError("members must be destination hashes")
        with self._transaction():
            broadcast = self._lists.get(name.strip().casefold())
            if broadcast is None:
                raise ValueError(f"unknown broadcast list: {name!r}")
            remaining = [
                member for member in broadcast["members"] if member not in doomed
            ]
            if not remaining:
                raise ValueError(
                    "a broadcast list needs at least one member;"
                    f" delete {broadcast['name']!r} instead"
                )
            snapshot = self._snapshot()
            updated = dict(broadcast, members=remaining)
            self._lists[broadcast["name"].casefold()] = updated
            try:
                self._save()
            except OSError:
                self._restore(snapshot)
                raise
        return dict(updated)

    def delete(self, name: str) -> bool:
        """Delete a list by name; ``True`` when one was removed."""
        with self._transaction():
            key = name.strip().casefold()
            if key not in self._lists:
                return False
            snapshot = self._snapshot()
            del self._lists[key]
            try:
                self._save()
            except OSError:
                self._restore(snapshot)
                raise
        return True

    def rename(self, name: str, new_name: str) -> Dict[str, Any]:
        """Rename a list in place, keeping its members and creation time.

        Raises :class:`ValueError` when the list is unknown, the new
        name is blank/invalid, or another list already has the new
        name (names are unique case-insensitively), and
        :class:`OSError` when the write fails (the in-memory store is
        left as it was on disk).  A case-only rename (``team`` ->
        ``Team``) is allowed: the key is unchanged, only the stored
        name changes.
        """
        if not isinstance(new_name, str) or not new_name.strip():
            raise ValueError("a broadcast list needs a name")
        cleaned = sanitize_group_name(new_name.strip(), fallback="")
        if not cleaned:
            raise ValueError("a broadcast list needs a name")
        with self._transaction():
            broadcast = self._lists.get(name.strip().casefold())
            if broadcast is None:
                raise ValueError(f"unknown broadcast list: {name!r}")
            key = broadcast["name"].casefold()
            new_key = cleaned.casefold()
            if new_key != key and new_key in self._lists:
                raise ValueError(
                    f"a broadcast list named {cleaned!r} already exists"
                )
            snapshot = self._snapshot()
            renamed = dict(broadcast, name=cleaned)
            # Rebuild in place so the list keeps its position: equal
            # created_at values keep dict order when listing.
            self._lists = {
                (new_key if k == key else k): (renamed if k == key else v)
                for k, v in self._lists.items()
            }
            try:
                self._save()
            except OSError:
                self._restore(snapshot)
                raise
        return dict(renamed)

    def find(self, query: str) -> Optional[Dict[str, Any]]:
        """Resolve a list by exact name (case-insensitive).

        Raises :class:`ValueError` when the query matches nothing, so
        callers can report it plainly.
        """
        with self._transaction():
            broadcast = self._lists.get(query.strip().casefold())
        if broadcast is None:
            raise ValueError(f"unknown broadcast list: {query!r}")
        return dict(broadcast)

    def list_broadcasts(self) -> List[Dict[str, Any]]:
        """All broadcast lists, oldest first.

        Reloads from disk first, so lists created by another process
        (for example ``tau broadcast create`` while ``tau serve`` runs)
        are seen.
        """
        with self._transaction():
            return sorted(
                (dict(entry) for entry in self._lists.values()),
                key=lambda entry: entry["created_at"],
            )

    def __len__(self) -> int:
        with self._lock:
            return len(self._lists)
