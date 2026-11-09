"""Durable group roster store: private group DMs with fixed membership.

A group is a fixed roster — the creator plus named peers.  The group id
is derived from the creator, name and roster (see
:func:`tau.protocol.derive_group_id`), so the id commits to the roster:
a message carrying a different roster for the same id cannot verify.
The roster is created locally with ``tau group create`` and travels in
every group message, so a recipient that has never seen the group learns
it from the first message and pins it.  Locally created groups and
remotely learned ones are capped separately and neither cap evicts, so
a peer flooding group ids can never push out a group this node created;
``tau group delete`` frees capacity when a cap is reached.
"""

from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

try:  # POSIX only; the cross-process lock is a no-op without it.
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None  # type: ignore[assignment]

from .protocol import (
    MAX_GROUP_MEMBERS,
    MIN_GROUP_MEMBERS,
    ProtocolError,
    derive_group_id,
    sanitize_group_name,
)
from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

GROUPS_FILE_NAME = "groups.json"
# Locally created groups and remotely learned groups are capped
# separately, and neither cap evicts: a peer that floods group ids can
# only fill its own quota and can never push out a group this node
# created or one it already pinned.  `tau group delete` frees capacity
# when a cap is reached.
MAX_GROUPS = 100
MAX_LEARNED_GROUPS = 50


def _valid_group(raw: Any) -> Optional[Dict[str, Any]]:
    """Normalize one stored group, or ``None`` when it is unusable."""
    if not isinstance(raw, dict):
        return None
    group_id = raw.get("id")
    if not isinstance(group_id, str) or len(group_id) != 32:
        return None
    try:
        bytes.fromhex(group_id)
    except ValueError:
        return None
    members = raw.get("members")
    if not isinstance(members, list):
        return None
    cleaned: List[str] = []
    for member in members:
        if not isinstance(member, str) or len(member) != 32:
            return None
        try:
            bytes.fromhex(member)
        except ValueError:
            return None
        cleaned.append(member.lower())
    if (
        len(cleaned) < MIN_GROUP_MEMBERS
        or len(cleaned) > MAX_GROUP_MEMBERS
        or len(set(cleaned)) != len(cleaned)
    ):
        return None
    # Canonical roster order: the id derives from the sorted roster, so
    # a stored record must too, or the same group recreated with the
    # members in a different order would look like a different roster.
    cleaned = sorted(cleaned)
    creator = raw.get("creator")
    if creator is None:
        # Groups written before the creator field existed carry no
        # derivation to check; treat them as learned.
        creator = ""
    elif not isinstance(creator, str) or len(creator) != 32:
        return None
    else:
        try:
            bytes.fromhex(creator)
        except ValueError:
            return None
        creator = creator.lower()
    name = sanitize_group_name(raw.get("name"))
    if creator:
        if creator not in cleaned:
            return None
        try:
            expected = derive_group_id(creator, name, cleaned)
        except (ProtocolError, ValueError):
            return None
        if group_id.lower() != expected:
            # The id must commit to the roster; a stored record that
            # does not verify is corrupt (or hand-edited) and is dropped.
            return None
    created_at = raw.get("created_at")
    try:
        created_at = float(created_at) if created_at is not None else time.time()
    except (TypeError, ValueError):
        return None
    return {
        "id": group_id.lower(),
        "name": name,
        "members": cleaned,
        "created_at": created_at,
        "creator": creator,
        "created_by": creator,
        "local": bool(raw.get("local", False)),
    }


class GroupStore:
    """Known groups, persisted in the node data directory.

    A node without a data directory (tests, embeds) keeps groups in
    memory only.  Writes are atomic and the file is mode 0600, like the
    peer registry and mailbox; a failed write only logs a warning.
    """

    def __init__(self, path: Optional[Union[str, Path]] = None) -> None:
        self.path = Path(path).expanduser() if path is not None else None
        self._lock = threading.Lock()
        self._groups: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if self.path is None:
            return
        raw = read_json(self.path, default=None)
        if not isinstance(raw, dict):
            return
        entries = raw.get("groups")
        if not isinstance(entries, list):
            return
        loaded: Dict[str, Dict[str, Any]] = {}
        for entry in entries:
            group = _valid_group(entry)
            if group is not None:
                loaded[group["id"]] = group
        self._groups = loaded

    def _save(self) -> None:
        """Persist the store; raises :class:`OSError` when the write fails."""
        if self.path is None:
            return
        atomic_write_json(
            self.path, {"groups": list(self._groups.values())}, mode=0o600
        )

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        """Serialise a read-modify-write of the store across processes.

        Mirrors :meth:`Inbox._transaction`: each mutation takes an
        exclusive ``flock`` on a sidecar lock file and reloads the
        latest on-disk state first.
        """
        with self._lock:
            if self.path is None or fcntl is None:
                if self.path is not None:
                    self._load()
                yield
                return
            lock_path = self.path.with_name(self.path.name + ".lock")
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            with open(lock_path, "a") as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    self._load()
                    yield
                finally:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def create(
        self, name: str, members: List[str], *, created_by: str = ""
    ) -> Dict[str, Any]:
        """Create a group and persist it.

        The id is derived from the creator, name and roster, so creating
        the same group twice returns the same record.  Raises
        :class:`ValueError` when the group is invalid or the local cap
        is reached, and :class:`OSError` when the write fails (the
        in-memory store is left as it was on disk).
        """
        try:
            group_id = derive_group_id(created_by, name, members)
        except ProtocolError as exc:
            raise ValueError(f"invalid group: {exc}") from exc
        group = _valid_group(
            {
                "id": group_id,
                "name": name,
                "members": members,
                "created_at": time.time(),
                "creator": created_by,
                "created_by": created_by,
                "local": True,
            }
        )
        if group is None:  # pragma: no cover - create() validates first
            raise ValueError("invalid group")
        with self._transaction():
            existing = self._groups.get(group["id"])
            if existing is not None:
                if existing["members"] != group["members"]:
                    raise ValueError("group id already exists with another roster")
                return dict(existing)
            created = sum(1 for g in self._groups.values() if g["local"])
            if created >= MAX_GROUPS:
                raise ValueError(
                    f"group limit reached ({MAX_GROUPS} created groups;"
                    " delete one with `tau group delete`)"
                )
            self._groups[group["id"]] = group
            try:
                self._save()
            except OSError:
                # The id was not stored before (an existing id returns
                # above), so dropping it restores the on-disk state.
                del self._groups[group["id"]]
                raise
        return dict(group)

    def record(
        self,
        group_id: str,
        name: str,
        members: List[str],
        *,
        creator: str,
    ) -> Optional[Dict[str, Any]]:
        """Pin a roster learned from an incoming group message.

        The id must derive from the creator, name and roster, so a
        message carrying a different roster for the same id returns
        ``None`` — a changed roster means a new group, and a member
        cannot fork an id onto a recipient that has not seen it.  A
        learned group is also refused when the learned cap is full
        (delete a group to make room); locally created groups are never
        evicted to make room for one.
        """
        candidate = _valid_group(
            {
                "id": group_id,
                "name": name,
                "members": members,
                "created_at": time.time(),
                "creator": creator,
                "created_by": creator,
                "local": False,
            }
        )
        if candidate is None:
            return None
        with self._transaction():
            existing = self._groups.get(candidate["id"])
            if existing is not None:
                if existing["members"] != candidate["members"]:
                    return None
                return dict(existing)
            learned = sum(1 for g in self._groups.values() if not g["local"])
            if learned >= MAX_LEARNED_GROUPS:
                log.info("Refusing to pin group %s: learned-group limit reached", group_id)
                return None
            self._groups[candidate["id"]] = candidate
            try:
                self._save()
            except OSError as exc:
                # Do not acknowledge a pin that would be lost on restart.
                del self._groups[candidate["id"]]
                log.warning("Could not persist groups to %s: %s", self.path, exc)
                return None
        return dict(candidate)

    def delete(self, group_id: str) -> bool:
        """Delete a known group; ``False`` when it was not stored.

        Raises :class:`OSError` when the write fails, leaving the
        in-memory store as it was on disk.
        """
        with self._transaction():
            existing = self._groups.get(group_id.lower())
            if existing is None:
                return False
            del self._groups[group_id.lower()]
            try:
                self._save()
            except OSError:
                self._load()
                raise
        return True

    def get(self, group_id: str) -> Optional[Dict[str, Any]]:
        """One group by id, or ``None``."""
        with self._lock:
            group = self._groups.get(group_id.lower())
        return dict(group) if group is not None else None

    def find(self, query: str) -> Optional[Dict[str, Any]]:
        """Resolve a group by id, id prefix, or exact name.

        Raises :class:`ValueError` when the query is ambiguous or
        matches nothing, so callers can report it plainly.
        """
        query = query.strip()
        # Reload from disk first, so a group created by another process
        # (for example `tau group create` while `tau serve` runs) resolves.
        with self._transaction():
            groups = [dict(group) for group in self._groups.values()]
        if len(query) == 32:
            group = self.get(query)
            if group is None:
                raise ValueError(f"unknown group: {query!r}")
            return group
        by_name = [
            group for group in groups if group["name"].casefold() == query.casefold()
        ]
        if len(by_name) == 1:
            return dict(by_name[0])
        if len(by_name) > 1:
            raise ValueError(f"{len(by_name)} groups are named {query!r}")
        if len(query) >= 4:
            matches = [
                group for group in groups if group["id"].startswith(query.lower())
            ]
            if len(matches) == 1:
                return dict(matches[0])
            if len(matches) > 1:
                raise ValueError(
                    f"{len(matches)} groups match the id prefix {query!r}"
                )
        raise ValueError(f"unknown group: {query!r}")

    def list_groups(self) -> List[Dict[str, Any]]:
        """All known groups, oldest first.

        Reloads from disk first, so groups created by another process
        (for example ``tau group create`` while ``tau serve`` runs) are seen.
        """
        with self._transaction():
            return [dict(group) for group in self._groups.values()]

    def __len__(self) -> int:
        with self._lock:
            return len(self._groups)
