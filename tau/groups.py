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

The creator can replace the roster with :meth:`GroupStore.update_members`
(``tau group add-member`` / ``remove-member``): the successor group —
same name, new id — replaces the old record, and the old id is retired,
so a removed peer cannot restore the retired roster by replaying an old
group message.  A roster learned from a message likewise supersedes a
learned group from the same creator under the same name — but only when
the message comes from the creator themself, since the creator field is
only a claim in the payload: a successor carried by another member is
pinned alongside instead of replacing the predecessor, so no member can
forge a successor and hijack sends by group name.  The superseded
predecessor is only deleted, not retired, because the old roster may
still be live elsewhere and its messages should be able to pin it again.
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
# Retired group ids (rosters replaced by their creator) are kept so a
# removed peer cannot restore one by replaying an old group message.
# The list is capped like the groups themselves; the oldest retirement
# is forgotten first, so a very old roster can be pinned again.
MAX_RETIRED_GROUPS = MAX_GROUPS + MAX_LEARNED_GROUPS


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
        self._retired: List[str] = []
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
        retired = raw.get("retired")
        self._retired = []
        if isinstance(retired, list):
            seen = set()
            for entry in retired:
                if not isinstance(entry, str) or len(entry) != 32:
                    continue
                try:
                    bytes.fromhex(entry)
                except ValueError:
                    continue
                entry = entry.lower()
                if entry not in seen:
                    seen.add(entry)
                    self._retired.append(entry)

    def _save(self) -> None:
        """Persist the store; raises :class:`OSError` when the write fails."""
        if self.path is None:
            return
        atomic_write_json(
            self.path,
            {"groups": list(self._groups.values()), "retired": self._retired},
            mode=0o600,
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
            if group["id"] in self._retired:
                # The roster is live again (recreated after a roster
                # change), so it must not stay retired.
                self._retired.remove(group["id"])
            try:
                self._save()
            except OSError:
                # Reload restores both the groups and the retired list
                # to what is on disk.
                self._load()
                raise
        return dict(group)

    def update_members(
        self, group_id: str, members: List[str], *, created_by: str
    ) -> Dict[str, Any]:
        """Replace the roster of a group this node created.

        The group id derives from the roster, so a changed roster is a
        new group: the successor (same name, new id, same ``created_at``)
        is created and the old record deleted inside one transaction,
        so the local cap cannot block a net-zero change and a failure
        cannot leave both groups or neither.  The old id is retired
        (persisted): a removed peer cannot restore the retired roster
        by replaying an old group message, and the retirement is lifted
        if the roster is ever changed back to it.

        Raises :class:`ValueError` when the group is unknown, was not
        created by this node, or the new roster is invalid, and
        :class:`OSError` when the write fails (the store is left as it
        was on disk).
        """
        with self._transaction():
            existing = self._groups.get(group_id.lower())
            if existing is None:
                raise ValueError(f"unknown group: {group_id!r}")
            if not existing["local"] or existing["creator"] != created_by.lower():
                raise ValueError("only the creator can change a group's members")
            try:
                new_id = derive_group_id(
                    existing["creator"], existing["name"], members
                )
            except ProtocolError as exc:
                raise ValueError(f"invalid group: {exc}") from exc
            if new_id == existing["id"]:
                return dict(existing)
            if new_id in self._groups:  # pragma: no cover - hash collision
                raise ValueError("group id already exists with another roster")
            candidate = _valid_group(
                {
                    "id": new_id,
                    "name": existing["name"],
                    "members": members,
                    "created_at": existing["created_at"],
                    "creator": existing["creator"],
                    "created_by": existing["creator"],
                    "local": True,
                }
            )
            if candidate is None:
                raise ValueError("invalid group: the creator must be a member")
            del self._groups[existing["id"]]
            self._groups[new_id] = candidate
            if existing["id"] not in self._retired:
                self._retired.append(existing["id"])
                if len(self._retired) > MAX_RETIRED_GROUPS:
                    del self._retired[: len(self._retired) - MAX_RETIRED_GROUPS]
            if new_id in self._retired:
                # The roster is live again (a member re-added restores
                # the original roster), so it must not stay retired.
                self._retired.remove(new_id)
            try:
                self._save()
            except OSError:
                self._load()
                raise
        return dict(candidate)

    def record(
        self,
        group_id: str,
        name: str,
        members: List[str],
        *,
        creator: str,
        sender: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Pin a roster learned from an incoming group message.

        The id must derive from the creator, name and roster, so a
        message carrying a different roster for the same id returns
        ``None`` — a changed roster means a new group, and a member
        cannot fork an id onto a recipient that has not seen it.  A
        learned group is also refused when the learned cap is full
        (delete a group to make room); locally created groups are never
        evicted to make room for one.  A roster the creator retired
        with a membership change is refused as well, so a removed peer
        cannot restore it by replaying an old group message.

        ``sender`` is the authenticated sender of the message.  A new
        roster supersedes a learned group from the same creator under
        the same name only when the sender *is* the creator: the
        creator field is only a claim in the payload, so a successor
        carried by another member is pinned alongside the predecessor
        instead of replacing it — otherwise any member could forge a
        successor naming the real creator, drop other members from the
        roster, and hijack sends by group name.  The message itself is
        still accepted and pinned either way; only the replacement
        needs the creator's proof.
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
        creator_authorized = bool(sender) and sender.lower() == candidate["creator"]
        with self._transaction():
            existing = self._groups.get(candidate["id"])
            if existing is not None:
                if existing["members"] != candidate["members"]:
                    return None
            elif candidate["id"] in self._retired:
                return None
            superseded = []
            if creator_authorized:
                superseded = [
                    other
                    for other in self._groups.values()
                    if (
                        not other["local"]
                        and other["creator"] == candidate["creator"]
                        and other["name"] == candidate["name"]
                        and other["id"] != candidate["id"]
                    )
                ]
            if existing is None:
                learned = sum(1 for g in self._groups.values() if not g["local"])
                if learned - len(superseded) >= MAX_LEARNED_GROUPS:
                    log.info(
                        "Refusing to pin group %s: learned-group limit reached",
                        group_id,
                    )
                    return None
            for other in superseded:
                del self._groups[other["id"]]
            if existing is not None:
                if superseded:
                    try:
                        self._save()
                    except OSError as exc:
                        # Reload restores the groups the reconcile above
                        # deleted.
                        self._load()
                        log.warning(
                            "Could not persist groups to %s: %s", self.path, exc
                        )
                        return None
                return dict(existing)
            self._groups[candidate["id"]] = candidate
            try:
                self._save()
            except OSError as exc:
                # Do not acknowledge a pin that would be lost on restart.
                # Reload restores the groups the reconcile above deleted.
                self._load()
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
        with self._lock:
            groups = list(self._groups.values())
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
