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
forge a successor and hijack sends by group name.  A creator message
whose ``sent_at`` predates the message that pinned the successor is
stale — a queued copy sent before the roster changed, delivered late —
and pins alongside instead of replacing it, so sends by name keep
resolving to the successor; an intentional creator restore is sent
after the change, so its ``sent_at`` is newer and it still replaces.
Both timestamps come from the creator's own clock, so the comparison
holds regardless of the recipient's clock.  While both versions
coexist, :meth:`GroupStore.find` keeps resolving the name to the roster
this node has known longest (a locally created group first), so sends
by name keep working until the creator themself sends the successor.
The superseded predecessor is only deleted, not retired, because the
old roster may still be live elsewhere and its messages should be able
to pin it again.

The creator can also set a short topic (:meth:`GroupStore.set_topic`,
``tau group topic``): it is metadata, not part of the group id, so it
survives a roster change into the successor, and it travels in group
messages — a recipient pins it only from the creator, with the same
``sent_at`` staleness rule as the roster, so a queued copy cannot
overwrite a newer topic.
A member can leave a group they did not create with
:meth:`GroupStore.leave` (``tau group leave``): the record is deleted
and the id is remembered as left, so a later group message for it
cannot re-pin the group — unlike :meth:`GroupStore.delete`, whose
removal the next message undoes.  A successor roster for the same
creator and name that was already pinned alongside leaves with it: it
is the same group under a new roster, and leaving one version while
the other keeps receiving would not be leaving the group.  Leaving is
local only: the creator is not told and keeps the member on the
roster, so the messages keep coming and are rejected on arrival.
:meth:`GroupStore.rejoin` (``tau group rejoin``) clears a left id, so
the next message pins the group again; a re-invite under a changed
roster is a new id and pins without a rejoin.
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
except ImportError:  # pragma: no cover - POSIX
    msvcrt = None  # type: ignore[assignment]

from .protocol import (
    MAX_GROUP_MEMBERS,
    MAX_GROUP_TOPIC_LENGTH,
    MIN_GROUP_MEMBERS,
    ProtocolError,
    clean_group_topic,
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
# A rename lineage is capped, newest first: each predecessor id is 32
# hex chars (~35 bytes on the wire), so an unbounded chain would
# eventually push even a short group message past the 8 KiB request
# budget and make the group unsendable.  32 ids (~1.1 KiB) leaves room
# for a full 32-member roster and a 2000-character message; a member
# that missed more renames than that still retires same-name
# predecessors through the supersede path.
MAX_RENAME_LINEAGE = 32
# Retired group ids (rosters replaced by their creator) are kept so a
# removed peer cannot restore one by replaying an old group message.
# The list is capped like the groups themselves; the oldest retirement
# is forgotten first, so a very old roster can be pinned again.
MAX_RETIRED_GROUPS = MAX_GROUPS + MAX_LEARNED_GROUPS
# Groups this node has left are remembered the same way, so a later
# message cannot re-pin one: the oldest departure is forgotten first,
# so a very old group can be pinned again.
MAX_LEFT_GROUPS = MAX_RETIRED_GROUPS


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
    # The sent_at of the creator message that last pinned this roster, or
    # None when unknown (locally created groups, member-carried pins, and
    # records written before this field existed).  Only creator messages
    # stamp it: the comparison that tells a stale creator message from an
    # intentional restore is only meaningful between two timestamps from
    # the creator's own clock.
    roster_sent_at = raw.get("roster_sent_at")
    if roster_sent_at is not None:
        try:
            roster_sent_at = float(roster_sent_at)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(roster_sent_at):
            return None
    # The creator-set topic, and the sent_at of the creator message that
    # last pinned it (None when never set or set locally before any
    # message carried it).  Same staleness rule as the roster: only a
    # creator message stamps it, so a queued copy sent before a newer
    # topic cannot overwrite the newer one.
    topic_sent_at = raw.get("topic_sent_at")
    if topic_sent_at is not None:
        try:
            topic_sent_at = float(topic_sent_at)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(topic_sent_at):
            return None
    # Rename lineage: predecessor ids, newest first.  The lineage is
    # not part of the id derivation, so a stored record with a malformed
    # entry keeps only the usable ids instead of dropping the group.
    renamed_from: List[str] = []
    lineage = raw.get("renamed_from")
    if isinstance(lineage, list):
        seen_ids = set()
        for entry in lineage:
            if not isinstance(entry, str) or len(entry) != 32:
                continue
            try:
                bytes.fromhex(entry)
            except ValueError:
                continue
            entry = entry.lower()
            if entry not in seen_ids:
                seen_ids.add(entry)
                renamed_from.append(entry)
    return {
        "id": group_id.lower(),
        "name": name,
        "members": cleaned,
        "created_at": created_at,
        "creator": creator,
        "created_by": creator,
        "local": bool(raw.get("local", False)),
        "roster_sent_at": roster_sent_at,
        "topic": clean_group_topic(raw.get("topic")),
        "topic_sent_at": topic_sent_at,
        "renamed_from": renamed_from,
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
        self._left: List[Dict[str, Any]] = []
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
        left = raw.get("left")
        self._left = []
        if isinstance(left, list):
            seen = set()
            for entry in left:
                if not isinstance(entry, dict):
                    continue
                entry_id = entry.get("id")
                left_at = entry.get("left_at")
                if not isinstance(entry_id, str) or len(entry_id) != 32:
                    continue
                try:
                    bytes.fromhex(entry_id)
                except ValueError:
                    continue
                if not isinstance(left_at, (int, float)) or not math.isfinite(
                    left_at
                ):
                    continue
                entry_id = entry_id.lower()
                if entry_id not in seen:
                    seen.add(entry_id)
                    self._left.append({"id": entry_id, "left_at": float(left_at)})

    def _save(self) -> None:
        """Persist the store; raises :class:`OSError` when the write fails."""
        if self.path is None:
            return
        atomic_write_json(
            self.path,
            {
                "groups": list(self._groups.values()),
                "retired": self._retired,
                "left": self._left,
            },
            mode=0o600,
        )

    def _snapshot(
        self,
    ) -> "tuple[Dict[str, Dict[str, Any]], List[str], List[Dict[str, Any]]]":
        """In-memory state before a mutation, for rollback on a failed save.

        A shallow copy is enough: mutations replace whole records, they
        never edit a stored group dict in place.
        """
        return (dict(self._groups), list(self._retired), list(self._left))

    def _restore(
        self,
        snapshot: "tuple[Dict[str, Dict[str, Any]], List[str], List[Dict[str, Any]]]",
    ) -> None:
        """Restore the state captured by :meth:`_snapshot`.

        Reloading from disk cannot do this when the file is missing or
        unreadable — :meth:`_load` then leaves the mutated in-memory
        state in place, so a group whose first write failed would stay
        visible until restart.
        """
        groups, retired, left = snapshot
        self._groups = dict(groups)
        self._retired = list(retired)
        self._left = list(left)

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        """Serialise a read-modify-write of the store across processes.

        Mirrors :meth:`Inbox._transaction`: each mutation takes an
        exclusive lock on a sidecar lock file and reloads the latest
        on-disk state first, so a serving process and a CLI command
        cannot interleave a load and a save.  The lock is ``flock``
        on POSIX and ``msvcrt.locking`` on the first byte on Windows,
        as :mod:`tau.ignored_groups` does — without it, a ``record()``
        that loaded before a concurrent ``leave()`` could save after
        it and silently drop the left marker, letting the next
        message re-pin the group.
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
            snapshot = self._snapshot()
            self._groups[group["id"]] = group
            if group["id"] in self._retired:
                # The roster is live again (recreated after a roster
                # change), so it must not stay retired.
                self._retired.remove(group["id"])
            try:
                self._save()
            except OSError:
                # Restore the pre-mutation state: reloading cannot do
                # this when the file is missing or unreadable yet.
                self._restore(snapshot)
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
                    # A roster change is the same group continuing: the
                    # topic carries into the successor.
                    "topic": existing.get("topic", ""),
                    "topic_sent_at": existing.get("topic_sent_at"),
                    "renamed_from": existing.get("renamed_from") or [],
                }
            )
            if candidate is None:
                raise ValueError("invalid group: the creator must be a member")
            snapshot = self._snapshot()
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
                self._restore(snapshot)
                raise
        return dict(candidate)

    def set_topic(
        self, group_id: str, topic: str, *, created_by: str
    ) -> Dict[str, Any]:
        """Set the topic of a group this node created (``""`` clears it).

        Only the creator can set it, mirroring roster changes: a
        member, or the creator of a learned group, is refused.  An
        over-long topic is refused rather than silently truncated.
        The local clock stamps the change, so the next message the
        creator sends carries a newer ``sent_at`` and recipients accept
        it over any older topic they hold.

        Raises :class:`ValueError` when the group is unknown, was not
        created by this node, or the topic is too long, and
        :class:`OSError` when the write fails (the store is left as it
        was on disk).
        """
        if not isinstance(topic, str):
            raise ValueError("topic must be text")
        if len(" ".join(topic.split())) > MAX_GROUP_TOPIC_LENGTH:
            raise ValueError(
                f"topic is longer than {MAX_GROUP_TOPIC_LENGTH} characters"
            )
        cleaned = clean_group_topic(topic)
        with self._transaction():
            existing = self._groups.get(group_id.lower())
            if existing is None:
                raise ValueError(f"unknown group: {group_id!r}")
            if not existing["local"] or existing["creator"] != created_by.lower():
                raise ValueError("only the creator can change a group's topic")
            if existing.get("topic", "") == cleaned:
                return dict(existing)
            snapshot = self._snapshot()
            self._groups[existing["id"]] = {
                **existing,
                "topic": cleaned,
                "topic_sent_at": time.time(),
            }
            try:
                self._save()
            except OSError:
                self._restore(snapshot)
                raise
        return dict(self._groups[group_id.lower()])

    def rename(
        self, group_id: str, new_name: str, *, created_by: str
    ) -> Dict[str, Any]:
        """Rename a group this node created.

        The group id derives from the name, so a renamed group is a new
        group: the successor (same roster, new name, new id, same
        ``created_at``) is created and the old record deleted inside
        one transaction, so the local cap cannot block a net-zero
        change and a failure cannot leave both groups or neither.  The
        old id is retired (persisted): a replayed old message cannot
        restore the pre-rename name, and the retirement is lifted if
        the name is ever changed back to it.  The new name is
        sanitized exactly as :func:`tau.protocol.sanitize_group_name`
        sanitizes it at creation, so a blank name becomes ``"group"``.

        Raises :class:`ValueError` when the group is unknown or was
        not created by this node, and :class:`OSError` when the write
        fails (the store is left as it was on disk).
        """
        with self._transaction():
            existing = self._groups.get(group_id.lower())
            if existing is None:
                raise ValueError(f"unknown group: {group_id!r}")
            if not existing["local"] or existing["creator"] != created_by.lower():
                raise ValueError("only the creator can rename a group")
            name = sanitize_group_name(new_name)
            try:
                new_id = derive_group_id(
                    existing["creator"], name, existing["members"]
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
                    "name": name,
                    "members": existing["members"],
                    "created_at": existing["created_at"],
                    "creator": existing["creator"],
                    "created_by": existing["creator"],
                    "local": True,
                    "roster_sent_at": existing.get("roster_sent_at"),
                    # The successor remembers the newest predecessors,
                    # newest first, so a member that missed an
                    # intermediate rename still retires them, and group
                    # history can span the renames.  The lineage never
                    # names the successor itself: renaming back to an
                    # earlier name drops that name from the chain.  It
                    # is capped so repeated renames can never make the
                    # group unsendable (see MAX_RENAME_LINEAGE).
                    "renamed_from": (
                        [existing["id"]]
                        + [
                            pred
                            for pred in (existing.get("renamed_from") or [])
                            if pred != new_id
                        ]
                    )[:MAX_RENAME_LINEAGE],
                }
            )
            if candidate is None:
                raise ValueError("invalid group: the creator must be a member")
            snapshot = self._snapshot()
            del self._groups[existing["id"]]
            self._groups[new_id] = candidate
            if existing["id"] not in self._retired:
                self._retired.append(existing["id"])
                if len(self._retired) > MAX_RETIRED_GROUPS:
                    del self._retired[: len(self._retired) - MAX_RETIRED_GROUPS]
            if new_id in self._retired:
                # The name is live again (renamed back to it), so the
                # old id must not stay retired.
                self._retired.remove(new_id)
            try:
                self._save()
            except OSError:
                self._restore(snapshot)
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
        sent_at: Optional[float] = None,
        topic: Optional[str] = None,
        topic_sent_at: Optional[float] = None,
        renamed_from: Optional[List[str]] = None,
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
        cannot restore it by replaying an old group message — and so is
        a group this node left, so leaving cannot be undone by the next
        message for it.

        ``sender`` is the authenticated sender of the message.  A new
        roster supersedes a learned group from the same creator under
        the same name only when the sender *is* the creator: the
        creator field is only a claim in the payload, so a successor
        carried by another member is pinned alongside the predecessor
        instead of replacing it — otherwise any member could forge a
        successor naming the real creator, drop other members from the
        roster, and hijack sends by group name.  The message itself is
        still accepted and pinned either way; only the replacement
        needs the creator's proof.  While both versions coexist,
        :meth:`find` keeps resolving the name to the roster known
        longest (a locally created group first), so sends by name keep
        working until the creator sends the successor themself.

        ``renamed_from`` is the successor's rename lineage (predecessor
        ids, newest first).  A creator-authorized message carrying it
        retires those predecessors the same way: a learned predecessor
        from the same creator is deleted and its id retired, so a
        member that still holds the old name stops sending to an id
        the creator retired, and a queued copy of an old message
        cannot re-pin it.  A member-carried message never retires
        anything — and its lineage is not stored at all: the lineage
        is only a claim until the creator themself sends under the new
        name, and storing it would let this node's later sends
        retransmit it under the creator's authenticated identity
        (``send_group_message`` and the sealed re-send both read the
        stored lineage), retiring a group the creator never retired.

        ``sent_at`` is the message's send time on the creator's clock.
        A creator message sent before the successor was learned is
        stale — a queued copy that predates the roster change — and
        pins alongside instead of replacing the successor, so later
        sends by name cannot include a removed member.  A roster whose
        pinning message carried no ``sent_at``, or a candidate with
        none, cannot prove staleness and is superseded as before, so
        an intentional creator restore (sent after the change, with a
        newer ``sent_at``) still replaces the successor.

        ``topic`` is the topic the message carried, or ``None`` when
        the sender predates topics.  It is pinned only from the
        creator: a member's message carries whatever topic that member
        knows, which must not let a member set the group's topic.  The
        same staleness rule applies — a queued creator copy sent before
        a newer topic was set must not overwrite it — with the stamp
        left alone when the message carries no ``sent_at``.
        ``topic_sent_at`` is the time the carried topic was set on the
        sender's clock (``None`` falls back to ``sent_at``): a retried
        message re-reads the current topic, whose set time can postdate
        the queued message's send time, so the topic is judged by its
        own timestamp, not the message's.
        """
        if sent_at is not None:
            try:
                sent_at = float(sent_at)
            except (TypeError, ValueError, OverflowError):
                sent_at = None
            if sent_at is not None and not math.isfinite(sent_at):
                sent_at = None
        # Normalize the carried topic the same way the wire does: a
        # non-text value is not a topic at all, and a present string
        # (including "") is authoritative.
        topic = clean_group_topic(topic) if isinstance(topic, str) else None
        # A retried topic carries the time it was set, which can be later
        # than the queued message's send time; the topic is judged by that.
        if topic_sent_at is not None:
            try:
                topic_sent_at = float(topic_sent_at)
            except (TypeError, ValueError, OverflowError):
                topic_sent_at = None
            if topic_sent_at is not None and not math.isfinite(topic_sent_at):
                topic_sent_at = None
        if topic_sent_at is None:
            topic_sent_at = sent_at
        candidate = _valid_group(
            {
                "id": group_id,
                "name": name,
                "members": members,
                "created_at": time.time(),
                "creator": creator,
                "created_by": creator,
                "local": False,
                "renamed_from": list(renamed_from or [])[:MAX_RENAME_LINEAGE],
            }
        )
        if candidate is None:
            return None
        creator_authorized = bool(sender) and sender.lower() == candidate["creator"]
        if not creator_authorized:
            # A member-carried lineage is only a claim: never store it.
            # Otherwise a later send to this group would retransmit it
            # under the creator's authenticated identity (the send and
            # sealed re-send paths read the stored lineage), and other
            # members would retire a group the creator never retired.
            # The creator's own message adopts the authoritative
            # lineage on the existing record below.
            candidate["renamed_from"] = []
        if creator_authorized:
            # Only a creator message stamps the roster: the staleness
            # comparison is only meaningful between two timestamps from
            # the creator's own clock.
            candidate["roster_sent_at"] = sent_at
            if topic is not None:
                # A first sighting from the creator carries the topic;
                # one from a member keeps "" — members cannot set it.
                candidate["topic"] = topic
                candidate["topic_sent_at"] = topic_sent_at
        with self._transaction():
            existing = self._groups.get(candidate["id"])
            restored = False
            if existing is not None:
                if existing["members"] != candidate["members"]:
                    return None
            elif candidate["id"] in self._retired or self.is_left(
                candidate["id"]
            ):
                # A retired id stays refused — except a creator restore:
                # the creator renamed back to this name, so their
                # message carries the lineage naming the successor this
                # node currently holds, sent after that successor was
                # pinned.  A queued copy of an old message carries no
                # lineage, and a stale successor copy predates the
                # successor's pin, so neither can pass this gate.
                # A group this node left stays refused: no restore path.
                restored = (
                    candidate["id"] in self._retired
                    and not self.is_left(candidate["id"])
                    and creator_authorized
                    and sent_at is not None
                    and any(
                        (other := self._groups.get(pred_id)) is not None
                        and not other["local"]
                        and other["creator"] == candidate["creator"]
                        and (
                            other.get("roster_sent_at") is None
                            or sent_at >= other["roster_sent_at"]
                        )
                        for pred_id in dict.fromkeys(
                            candidate.get("renamed_from") or []
                        )
                    )
                )
                if not restored:
                    return None
            superseded = []
            predecessors = []
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
                # A rename successor names its predecessors: learned
                # predecessors from the same creator are retired, so
                # the member stops sending to an id the creator no
                # longer accepts.  A predecessor this node created
                # itself is never touched — only the creator retires
                # their own groups, and a local copy means this node
                # is the creator.
                for pred_id in dict.fromkeys(renamed_from or []):
                    other = self._groups.get(pred_id)
                    if (
                        other is None
                        or other["local"]
                        or other["creator"] != candidate["creator"]
                        or other["id"] == candidate["id"]
                    ):
                        continue
                    predecessors.append(other)
                if sent_at is not None:
                    # A queued creator message sent before the successor
                    # was learned must not replace it.  Supersede only
                    # the rosters the candidate is not older than; a
                    # roster with no recorded sent_at cannot prove the
                    # candidate stale, so it is superseded as before.
                    # The rest are pinned alongside the candidate, and
                    # find() keeps resolving the name to the roster
                    # known longest — the successor.
                    stale_for = [
                        other
                        for other in superseded + predecessors
                        if other.get("roster_sent_at") is not None
                        and sent_at < other["roster_sent_at"]
                    ]
                    if stale_for:
                        log.info(
                            "Stale creator message for group %s: sent "
                            "before the successor roster was learned; "
                            "pinning alongside instead of replacing it",
                            group_id,
                        )
                        superseded = [
                            other
                            for other in superseded
                            if other not in stale_for
                        ]
                        predecessors = [
                            other
                            for other in predecessors
                            if other not in stale_for
                        ]
                if superseded:
                    # The topic is judged independently of the roster:
                    # a successor whose message omits the topic, or
                    # carries an older one, must not wipe a newer topic
                    # a predecessor already holds.  Take the newest
                    # topic among the predecessors being replaced and
                    # adopt it when the incoming one is older or absent
                    # (an unstamped incoming topic cannot prove itself
                    # newer; a tie keeps the incoming message, like the
                    # same-id rule below).  The candidate is only stored
                    # when it is a new id; when it re-pins an existing
                    # one the branch below decides instead.
                    best_topic = None
                    best_at = float("-inf")
                    for other in superseded:
                        if other.get("topic") is None:
                            continue
                        other_at = other.get("topic_sent_at")
                        other_key = (
                            other_at
                            if isinstance(other_at, (int, float))
                            else float("-inf")
                        )
                        if best_topic is None or other_key > best_at:
                            best_topic = other["topic"]
                            best_at = other_key
                    cand_at = (
                        topic_sent_at
                        if isinstance(topic_sent_at, (int, float))
                        else float("-inf")
                    )
                    if best_topic is not None and (
                        topic is None or cand_at < best_at
                    ):
                        candidate["topic"] = best_topic
                        candidate["topic_sent_at"] = (
                            None if best_at == float("-inf") else best_at
                        )
                        # The same reconciled topic governs the
                        # existing branch below: a successor pinned
                        # earlier by a member message carries no topic
                        # of its own, so without this the branch would
                        # keep the incoming older (or empty) topic
                        # while the predecessor holding the newer one
                        # is deleted.
                        topic = best_topic
                        topic_sent_at = (
                            None if best_at == float("-inf") else best_at
                        )
            # A group named in both lists is replaced once: a
            # predecessor that also shares the successor's name
            # (a rename back) must not be deleted twice or counted
            # twice against the learned cap.
            replaced = {other["id"]: other for other in superseded}
            for other in predecessors:
                replaced.setdefault(other["id"], other)
            if existing is None:
                learned = sum(1 for g in self._groups.values() if not g["local"])
                if learned - len(replaced) >= MAX_LEARNED_GROUPS:
                    log.info(
                        "Refusing to pin group %s: learned-group limit reached",
                        group_id,
                    )
                    return None
            snapshot = self._snapshot()
            for other in replaced.values():
                del self._groups[other["id"]]
            for other in predecessors:
                if other["id"] not in self._retired:
                    self._retired.append(other["id"])
                    if len(self._retired) > MAX_RETIRED_GROUPS:
                        del self._retired[: len(self._retired) - MAX_RETIRED_GROUPS]
            if existing is not None:
                refreshed = False
                updates: Dict[str, Any] = {}
                if (
                    creator_authorized
                    and sent_at is not None
                    and (
                        existing.get("roster_sent_at") is None
                        or sent_at > existing["roster_sent_at"]
                    )
                ):
                    # A creator message carrying this roster again stamps
                    # it with its send time, so a later queued copy of an
                    # older roster cannot replace it — including one the
                    # node first learned from a member-carried message,
                    # which carries no creator timestamp of its own.
                    updates["roster_sent_at"] = sent_at
                if creator_authorized and topic is not None:
                    # Only the creator sets the topic, and a queued copy
                    # sent before the topic now held must not overwrite
                    # it.  A message with no sent_at cannot prove
                    # staleness, so it pins the text but leaves the
                    # stamp alone — clearing it would let an older
                    # queued copy win afterwards.  An update that
                    # changes neither the text nor the stamp writes
                    # nothing, so an ordinary creator message does not
                    # rewrite the groups file.
                    existing_topic_at = existing.get("topic_sent_at")
                    stale = (
                        existing_topic_at is not None
                        and topic_sent_at is not None
                        and topic_sent_at < existing_topic_at
                    )
                    if not stale and (
                        existing.get("topic", "") != topic
                        or (
                            topic_sent_at is not None
                            and topic_sent_at != existing_topic_at
                        )
                    ):
                        updates["topic"] = topic
                        if topic_sent_at is not None:
                            updates["topic_sent_at"] = topic_sent_at
                if updates:
                    self._groups[existing["id"]] = {**existing, **updates}
                    refreshed = True
                if creator_authorized and renamed_from:
                    # A creator message carries the authoritative
                    # lineage: a successor first pinned from a
                    # member-carried message has none, so adopt it
                    # here (a message without lineage never wipes
                    # one already known).
                    current = self._groups[existing["id"]]
                    merged = list(
                        dict.fromkeys(
                            list(renamed_from)
                            + list(current.get("renamed_from") or [])
                        )
                    )[:MAX_RENAME_LINEAGE]
                    if merged != list(current.get("renamed_from") or []):
                        self._groups[existing["id"]] = {
                            **current,
                            "renamed_from": merged,
                        }
                        refreshed = True
                if replaced or refreshed:
                    try:
                        self._save()
                    except OSError as exc:
                        # Restore the groups the reconcile above deleted.
                        self._restore(snapshot)
                        log.warning(
                            "Could not persist groups to %s: %s", self.path, exc
                        )
                        return None
                return dict(self._groups[existing["id"]])
            self._groups[candidate["id"]] = candidate
            if restored and candidate["id"] in self._retired:
                # A creator restore brings the id back to life.
                self._retired.remove(candidate["id"])
            try:
                self._save()
            except OSError as exc:
                # Do not acknowledge a pin that would be lost on restart.
                # Restore the groups the reconcile above deleted.
                self._restore(snapshot)
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
            snapshot = self._snapshot()
            del self._groups[group_id.lower()]
            try:
                self._save()
            except OSError:
                self._restore(snapshot)
                raise
        return True

    def is_left(self, group_id: str) -> bool:
        """Whether *group_id* was left and must not be re-pinned.

        Reads the in-memory list; callers that need a fresh read go
        through :meth:`find` or :meth:`list_groups` first, which reload
        from disk.  Must not take the lock: it is called from
        :meth:`record`, which already holds it.
        """
        group_id = group_id.lower()
        return any(entry["id"] == group_id for entry in self._left)

    def leave(self, group_id: str) -> List[str]:
        """Leave a known group; ``[]`` when it was not stored.

        Returns the ids left: the resolved group plus any successor
        roster for the same creator and name already pinned alongside
        it — the same group under a new roster, which would keep
        receiving if it stayed.  Each id is remembered as left, so a
        later group message for it cannot re-pin the group (see
        :meth:`record`) — unlike :meth:`delete`, whose removal the next
        message undoes.  A genuinely new roster arriving after the
        leave is a new id and pins normally.  Raises :class:`OSError`
        when the write fails, leaving the in-memory store as it was on
        disk.
        """
        with self._transaction():
            existing = self._groups.get(group_id.lower())
            if existing is None:
                return []
            snapshot = self._snapshot()
            family = [existing] + [
                other
                for other in self._groups.values()
                if other["id"] != existing["id"]
                and not other["local"]
                and other["creator"] == existing["creator"]
                and other["name"] == existing["name"]
            ]
            left_ids = []
            for group in family:
                del self._groups[group["id"]]
                left_ids.append(group["id"])
                if not self.is_left(group["id"]):
                    self._left.append(
                        {"id": group["id"], "left_at": time.time()}
                    )
                    if len(self._left) > MAX_LEFT_GROUPS:
                        del self._left[: len(self._left) - MAX_LEFT_GROUPS]
            try:
                self._save()
            except OSError:
                self._restore(snapshot)
                raise
        return left_ids

    def rejoin(self, group_id: str) -> bool:
        """Clear a left group id, so the next message can pin it again.

        ``False`` when the id was never left.  Raises :class:`OSError`
        when the write fails, leaving the in-memory store as it was on
        disk.
        """
        with self._transaction():
            group_id = group_id.lower()
            remaining = [
                entry for entry in self._left if entry["id"] != group_id
            ]
            if len(remaining) == len(self._left):
                return False
            snapshot = self._snapshot()
            self._left = remaining
            try:
                self._save()
            except OSError:
                self._restore(snapshot)
                raise
        return True

    def left_groups(self) -> List[Dict[str, Any]]:
        """Groups this node has left, oldest first.

        Reloads from disk first, so a leave in another process is seen.
        Each entry carries the group ``id`` and the ``left_at`` time it
        was left.
        """
        with self._transaction():
            return [dict(entry) for entry in self._left]

    def get(self, group_id: str) -> Optional[Dict[str, Any]]:
        """One group by id, or ``None``."""
        with self._lock:
            group = self._groups.get(group_id.lower())
        return dict(group) if group is not None else None

    def find(self, query: str) -> Optional[Dict[str, Any]]:
        """Resolve a group by id, id prefix, or exact name.

        Raises :class:`ValueError` when the query is ambiguous or
        matches nothing, so callers can report it plainly.  Same-name
        groups from one creator are not ambiguous: a locally created
        group wins, otherwise the roster this node has known longest
        wins, so a member-carried successor neither hijacks sends by
        name nor blocks them until the creator sends it themself.
        """
        query = query.strip()
        # Reload from disk first, so a group created by another process
        # (for example `tau group create` while `tau serve` runs) resolves.
        with self._transaction():
            groups = [dict(group) for group in self._groups.values()]
        if len(query) == 32:
            group = self.get(query)
            if group is not None:
                return group
            # A 32-character name is also possible: fall through to the
            # name match below instead of erroring here.  An actual id
            # still wins — get() above already checked it.
        by_name = [
            group for group in groups if group["name"].casefold() == query.casefold()
        ]
        if len(by_name) == 1:
            return dict(by_name[0])
        if len(by_name) > 1:
            creators = {group["creator"] for group in by_name}
            if len(creators) == 1:
                local = [group for group in by_name if group["local"]]
                if len(local) == 1:
                    return dict(local[0])
                if not local:
                    return dict(
                        sorted(by_name, key=lambda group: group["created_at"])[0]
                    )
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
