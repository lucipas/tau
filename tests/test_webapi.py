from __future__ import annotations

import base64
import hashlib
import http.client
import io
import json
import os
import socket
import sys
import tarfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace

import pytest
import RNS

from tau import cli
from tau.allowlist import AllowList, load_allowlist, save_allowlist
from tau.auto_backup import AutoBackupScheduler
from tau.away import DEFAULT_AWAY_MESSAGE
from tau.backup import MANIFEST_NAME, BackupError, restore_backup
from tau.beacons import (
    BEACON_DEFAULT_DURATION,
    BEACON_UPDATE_INTERVAL,
    BeaconsError,
    beacons_lock,
    load_beacons,
    save_beacons,
)
from tau.blocklist import BlockList, load_blocklist, save_blocklist
from tau.config import TauConfig, set_node_setting
from tau.devices import verify_pairing_bundle
from tau.groups import GroupStore
from tau.identity import destination_hash, load_identity
from tau.inbox import Inbox
from tau.mailbox import Mailbox
from tau.node import NodeError
from tau.peers import AmbiguousPeer, Peer, PeerError
from tau.protocol import location_default_text
from tau.webapi import (
    SUBSCRIBER_QUEUE_SIZE,
    PeerForgetBlocked,
    WebBridge,
    WebBridgeError,
    _WebSocketFrameError,
    _WebSocketReader,
    _WS_MAX_FRAME_BYTES,
    _backup_name,
    _clean_stored_message,
    _ws_accept_key,
    _ws_encode_frame,
    check_web_console_bind,
)

PEER_HASH = "ab" * 16
OTHER_HASH = "cd" * 16
BOB_HASH = "ef" * 16


class FakeRegistry:
    def __init__(self, peers):
        self._peers = list(peers)
        self.saved = False
        self.fail_save = False

    def all(self, max_age=None):
        return list(self._peers)

    def get(self, destination_hash):
        if isinstance(destination_hash, str):
            value = destination_hash.strip()
            if len(value) == 32 and all(
                char in "0123456789abcdefABCDEF" for char in value
            ):
                for peer in self._peers:
                    if peer.hexhash == value.casefold():
                        return peer
                return None
            for peer in self._peers:
                if peer.name.casefold() == value.casefold():
                    return peer
            # Any other hex string (e.g. a full-length destination hash)
            # falls back to main's byte comparison below.
            try:
                destination_hash = bytes.fromhex(value)
            except ValueError:
                return None
        for peer in self._peers:
            if peer.destination_hash == destination_hash:
                return peer
        return None

    def resolve(self, target, *, include_alias=True):
        # Mirror PeerRegistry.resolve for the single-peer fixture: name,
        # alias (only when include_alias), destination hash or hash
        # prefix, with ambiguity rejected. A test may set an alias on
        # the fixture peer to prove a security boundary never resolves
        # by alias; tan.resolve_author calls
        # registry.resolve(value, include_alias=False).
        if isinstance(target, bytes):
            return self.get(target)
        if not isinstance(target, str):
            raise PeerError("peer target must be a string or bytes")
        value = target.strip()
        if not value:
            return None
        if len(value) == 32 and all(
            char in "0123456789abcdefABCDEF" for char in value
        ):
            return self.get(value)
        if include_alias:
            by_alias = [
                p
                for p in self._peers
                if getattr(p, "alias", None)
                and p.alias.casefold() == value.casefold()
            ]
            if len(by_alias) == 1:
                return by_alias[0]
            if len(by_alias) > 1:
                raise AmbiguousPeer(
                    f"{len(by_alias)} peers share the alias {value!r}; use a destination hash"
                )
        by_name = [p for p in self._peers if p.name.casefold() == value.casefold()]
        if len(by_name) == 1:
            return by_name[0]
        if len(by_name) > 1:
            raise AmbiguousPeer(
                f"{len(by_name)} peers are named {value!r}; use a destination hash"
            )
        if len(value) >= 4:
            by_prefix = [
                p for p in self._peers if p.hexhash.startswith(value.casefold())
            ]
            if len(by_prefix) == 1:
                return by_prefix[0]
            if len(by_prefix) > 1:
                raise AmbiguousPeer(
                    f"{len(by_prefix)} peers match the hash prefix {value!r}"
                )
        return None

    def forget(self, target):
        peer = self.resolve(target)
        if peer is None:
            raise PeerError(f"no peer matches {target!r}")
        self._peers = [entry for entry in self._peers if entry is not peer]
        return peer

    def refresh_aliases(self):
        return 0

    def lookalike_for(self, peer, *, include_notes=True):
        # The fixture holds a single peer, so nothing can collide with
        # it; the real registry (PeerRegistry.lookalike_for) is covered
        # by tests that swap one in (see _seed_alias_registry).
        return None

    def restore(self, peer):
        if self.get(peer.destination_hash) is None:
            self._peers.append(peer)

    def save(self):
        if self.fail_save:
            raise OSError("disk full")
        self.saved = True
        return True

class FakeNode:
    def __init__(self, mailbox_path=None, groups_path=None):
        self.name = "tester"
        self.destination_hexhash = "ef" * 16
        self.mailbox = Mailbox(mailbox_path)
        self.inbox = Inbox()
        self.identity = RNS.Identity()
        data_dir = Path(mailbox_path).parent if mailbox_path else Path(".")
        self.config = SimpleNamespace(
            identity_path=data_dir / "identity",
            social_db_path=data_dir / "social.db",
            tan_rules_path=data_dir / "tan-rules.json",
            dm_requests_path=data_dir / "dm-requests.json",
            invites_path=data_dir / "invites.json",
            join_requests_path=data_dir / "join-requests.json",
            allowlist_path=data_dir / "allowlist.json",
            blocklist_path=data_dir / "blocklist.json",
            ignored_groups_path=data_dir / "ignored-groups.json",
            beacons_path=data_dir / "beacons.json",
            channels_db_path=data_dir / "channels.sqlite3",
            files_dir=data_dir / "files",
        )
        self._allowlist = AllowList()
        self._allowlist_lock = threading.RLock()
        self.allowlist_path = (
            Path(mailbox_path).with_name("allowlist.json")
            if mailbox_path is not None
            else None
        )
        self.registry = FakeRegistry(
            [
                Peer(
                    destination_hash=bytes.fromhex(PEER_HASH),
                    name="alice",
                    public_key=b"\x01" * 32,
                    first_seen=1000.0,
                    last_seen=time.time(),
                    hops=1,
                )
            ]
        )
        self.sent = []
        self.group_sent = []
        self.last_location = None
        self.last_contact = None
        self.last_beacon = None
        self.last_group_location = None
        self.last_group_contact = None
        self.last_group_beacon = None
        self.group_reactions = []
        self.typing = []
        self.group_typing = []
        self.group_retractions = []
        self.group_retract_failures = {}
        self.group_retract_cancelled = []
        self.group_edits = []
        self.group_edit_failures = {}
        self.group_edit_cancelled = []
        self.group_edit_requeued = []
        self.group_edit_local_errors = []
        self.group_refusals = set()
        self.broadcast_refusals = set()
        self.groups = GroupStore(groups_path)
        self.groups.create(
            "crew", [PEER_HASH, BOB_HASH], created_by=PEER_HASH
        )
        self.sent_files = []
        self.sent_file_kwargs = []
        self.group_sent_files = []
        self.group_sent_file_kwargs = []
        self.broadcast_sent_files = []
        self.media_fetches = []
        self.announced = 0
        self.flush_calls = []
        self.flush_lock_busy = False
        self.flush_deliver_all = False
        self.flush_deliver_target = True
        self.flush_concurrent_delivery = False
        self.blocklist = None

    def flush_outbox(
        self,
        *,
        timeout=6.0,
        max_peers=None,
        respect_backoff=True,
        to=None,
        message_id=None,
        wait_for_lock=0.0,
    ):
        """Test stand-in for the node's mail sweep: deliver what it is told to.

        A targeted flush removes that entry (delivery); a retry-all
        removes every entry only when ``flush_deliver_all`` is set,
        modelling unreachable peers otherwise.
        """
        self.flush_calls.append(
            {
                "timeout": timeout,
                "max_peers": max_peers,
                "respect_backoff": respect_backoff,
                "to": to,
                "message_id": message_id,
                "wait_for_lock": wait_for_lock,
            }
        )
        if self.flush_lock_busy:
            return {
                "delivered": 0,
                "deposited": 0,
                "received": 0,
                "receipts": 0,
                "lock_busy": True,
            }
        delivered = 0
        if message_id is not None:
            if self.flush_concurrent_delivery:
                # Another round delivered the entry while this flush
                # waited for the mail lock: it leaves the queue, but
                # this flush reports no delivery of its own.
                self.mailbox.remove_outbox(message_id)
            elif self.flush_deliver_target and self.mailbox.remove_outbox(
                message_id
            ):
                delivered = 1
        elif self.flush_deliver_all:
            for entry in self.mailbox.outbox_list():
                if self.mailbox.remove_outbox(entry.id):
                    delivered += 1
        return {
            "delivered": delivered,
            "deposited": 0,
            "received": 0,
            "receipts": 0,
            "lock_busy": False,
        }


    def set_blocklist(self, blocklist):
        self.blocklist = blocklist

    @property
    def allowlist(self):
        return self._allowlist

    def set_allowlist(self, allowlist):
        # Mirrors TauNode.set_allowlist: the running node's in-memory
        # list and the file change together, persist first.
        with self._allowlist_lock:
            save_allowlist(self.allowlist_path, allowlist)
            self._allowlist = allowlist

    def update_allowlist(self, transform):
        # Mirrors TauNode.update_allowlist: re-read the saved list so a
        # change another process saved is preserved, refuse a corrupt
        # file, and serialise the whole read-modify-write.
        with self._allowlist_lock:
            current = load_allowlist(self.allowlist_path)
            updated = transform(current)
            self.set_allowlist(updated)
            return updated

    def info(self):
        return {
            "name": self.name,
            "destination": "cd" * 16,
            "protocol": 1,
            "version": "0.1.0",
            "rns_version": "test",
            "uptime": 12.5,
            "peers": 1,
            "interfaces": [{"name": "TCP", "online": True}],
        }

    def resolve_peer(self, target, *, timeout=10.0):
        if target in ("alice", PEER_HASH):
            return self.registry.all()[0]
        # Mirror TauNode.resolve_peer: an unknown name is a NodeError.
        raise NodeError(f"unknown peer: {target}")

    def send_message(
        self,
        peer,
        text,
        *,
        timeout=20.0,
        discover_timeout=None,
        in_reply_to=None,
        expires_at=None,
        send_after=None,
        kind=None,
        location=None,
        contact=None,
        beacon=None,
        poll_options=None,
        poll_vote=None,
        poll_close=None,
    ):
        self.sent.append((peer.name, text, in_reply_to))
        self.last_expires_at = expires_at
        self.last_send_after = send_after
        self.last_kind = kind
        self.last_location = location
        self.last_contact = contact
        self.last_beacon = beacon
        self.last_poll_options = poll_options
        self.last_poll_vote = poll_vote
        self.last_poll_close = poll_close
        # A distinct wire id per send, like the real node's generated id.
        wire_id = f"{len(self.sent):032d}"
        return {
            "ok": True,
            "echo": text,
            "server": self.name,
            "_rtt": 0.012,
            "id": wire_id,
        }

    def send_group_message(
        self, group, text, *, timeout=20.0, discover_timeout=None, queue=True,
        expires_at=None, send_after=None, in_reply_to=None,
        kind=None, location=None, contact=None, beacon=None,
        poll_options=None, poll_vote=None, poll_close=None,
    ):
        self.group_sent.append((group["name"], text))
        self.last_group_expires_at = expires_at
        self.last_group_send_after = send_after
        self.last_group_in_reply_to = in_reply_to
        self.last_group_kind = kind
        self.last_group_location = location
        self.last_group_contact = contact
        self.last_group_beacon = beacon
        self.last_group_poll_options = poll_options
        self.last_group_poll_vote = poll_vote
        self.last_group_poll_close = poll_close
        names = {PEER_HASH: "alice", BOB_HASH: "bob"}
        results = []
        for member in group["members"]:
            if member in self.group_refusals:
                results.append(
                    {
                        "to": member,
                        "to_name": names.get(member, ""),
                        "ok": False,
                        "error": "not_allowed",
                        "id": f"wire-{member[:8]}",
                    }
                )
            elif member == BOB_HASH:
                # Offline member: queued in the outbox under an entry id.
                results.append(
                    {
                        "to": member,
                        "to_name": "bob",
                        "queued": True,
                        "id": "mail-bob-1",
                        "expires_at": 0,
                    }
                )
            else:
                results.append(
                    {
                        "to": member,
                        "to_name": names.get(member, ""),
                        "delivered": True,
                        "id": f"wire-{member[:8]}",
                        "rtt": 0.009,
                    }
                )
        all_ok = all(
            result.get("delivered") or result.get("queued") for result in results
        )
        return {
            "ok": all_ok,
            "v": 1,
            "group": group["id"],
            "name": group["name"],
            # One send time shared by every leg, like the real node.
            "sent_at": 1000.0,
            "results": results,
        }

    def start_beacon(
        self,
        target,
        location,
        *,
        group=False,
        broadcast=False,
        channel=False,
        coarse=False,
        duration=BEACON_DEFAULT_DURATION,
        interval=BEACON_UPDATE_INTERVAL,
        location_file=None,
        timeout=20.0,
        discover_timeout=None,
        now=None,
    ):
        """Test stand-in for TauNode.start_beacon: same store, no network."""
        import time as _time

        now = _time.time() if now is None else now
        target_id = ""
        if group:
            self.groups.list_groups()
            record = self.groups.find(target)
            if record is None:
                raise NodeError(f"unknown group: {target!r}")
            target_hash = record["id"]
            target_kind = "group"
            target_name = record["name"]
        elif broadcast:
            try:
                record = self.broadcasts.find(target)
            except ValueError as exc:
                raise NodeError(str(exc)) from exc
            target_hash = record["name"]
            target_kind = "broadcast"
            target_name = record["name"]
            target_id = record["id"]
        elif channel:
            from tau.channels import ChannelError, clean_channel_name, init_db, is_member

            try:
                name = clean_channel_name(target)
            except ChannelError as exc:
                raise NodeError(str(exc)) from exc
            conn = init_db(self.config.channels_db_path)
            try:
                member = is_member(conn, name)
            finally:
                conn.close()
            if not member:
                raise NodeError(
                    f"not a member of channel {name!r}; join it first"
                )
            target_hash = name
            target_kind = "channel"
            target_name = name
        else:
            peer = self.resolve_peer(
                target,
                timeout=discover_timeout
                if discover_timeout
                else min(timeout, 10.0),
            )
            target_hash = peer.hexhash
            target_kind = "dm"
            target_name = peer.display_name
        try:
            with beacons_lock(self.config.beacons_path):
                beacons = load_beacons(self.config.beacons_path)
                beacon = beacons.start(
                    target=target_hash,
                    target_kind=target_kind,
                    target_name=target_name,
                    target_id=target_id,
                    location=location,
                    coarse=coarse,
                    duration=duration,
                    interval=interval,
                    location_file=location_file,
                    now=now,
                )
                save_beacons(self.config.beacons_path, beacons)
        except BeaconsError as exc:
            raise NodeError(str(exc)) from exc
        return beacon

    def send_beacon_update(self, beacon, *, now=None):
        """Test stand-in for TauNode.send_beacon_update: same sends, no network."""
        import time as _time

        now = _time.time() if now is None else now
        try:
            location = beacon.current_location()
        except BeaconsError as exc:
            raise NodeError(str(exc)) from exc
        seq = beacon.updates_sent + 1
        marker = beacon.wire_marker(seq)
        text = location_default_text(location)
        if beacon.target_kind == "group":
            self.groups.list_groups()
            record = self.groups.find(beacon.target)
            if record is None:
                raise NodeError(f"unknown group: {beacon.target!r}")
            result = self.send_group_message(
                record, text, location=location, beacon=marker, kind="location"
            )
        elif beacon.target_kind == "channel":
            # No gossip in the fake: the update is stored in the
            # channel's own history, which is what the console reads.
            from tau.channels import ChannelApp

            app = ChannelApp(
                self.config, self.identity, name=getattr(self, "name", None)
            )
            message = app.send(
                beacon.target, text, location=location, beacon=marker
            )
            result = {
                "message": message,
                "published": {"delivered": 0, "peers": 0, "errors": []},
            }
        elif beacon.target_kind == "broadcast":
            try:
                if beacon.target_id:
                    broadcast = self.broadcasts.find_by_id(beacon.target_id)
                else:
                    broadcast = self.broadcasts.find(beacon.target)
            except ValueError as exc:
                raise NodeError(str(exc)) from exc
            results = []
            for member in broadcast["members"]:
                peer = self.resolve_peer(member)
                leg = self.send_message(
                    peer, text, location=location, beacon=marker, kind="location"
                )
                results.append(
                    {
                        "to": member,
                        "to_name": peer.name,
                        "delivered": True,
                        "queued": False,
                        "id": leg.get("id"),
                    }
                )
            result = {
                "ok": True,
                "v": 1,
                "broadcast": broadcast["name"],
                "sent_at": 1000.0,
                "results": results,
            }
        else:
            peer = self.resolve_peer(beacon.target)
            result = self.send_message(
                peer, text, location=location, beacon=marker, kind="location"
            )
        beacon.updates_sent = seq
        beacon.next_update_at = now + beacon.interval
        try:
            with beacons_lock(self.config.beacons_path):
                stored = load_beacons(self.config.beacons_path)
                current = stored.get(beacon.id)
                if current is not None:
                    current.updates_sent = seq
                    current.next_update_at = beacon.next_update_at
                    save_beacons(self.config.beacons_path, stored)
        except BeaconsError:  # pragma: no cover - disk failure
            pass
        callback = getattr(self, "on_beacon_update", None)
        if callable(callback):
            callback(beacon, result)
        return result

    def approve_join_request(self, group, peer):
        """Test stand-in for TauNode.approve_join_request."""
        from tau.group_invites import resolve_join_request

        found = self.groups.find(group)
        if not found["local"] or found["creator"] != self.destination_hexhash:
            raise ValueError("only the creator can approve join requests")
        if not resolve_join_request(
            self.config.join_requests_path, found["name"], peer
        ):
            raise ValueError("no pending join request from that peer")
        summary = {
            "id": found["id"],
            "name": found["name"],
            "members": list(found["members"]),
            "creator": found["creator"],
        }
        if peer in found["members"]:
            return {"status": "already_member", "group": summary}
        members = list(found["members"]) + [peer]
        new_group = self.groups.update_members(
            found["id"], members, created_by=self.destination_hexhash
        )
        self.send_group_message(
            new_group, f"joined {found['name']} via an invite link."
        )
        summary = {
            "id": new_group["id"],
            "name": new_group["name"],
            "members": list(new_group["members"]),
            "creator": new_group["creator"],
        }
        return {"status": "added", "group": summary}

    def deny_join_request(self, group, peer):
        """Test stand-in for TauNode.deny_join_request."""
        from tau.group_invites import resolve_join_request

        found = self.groups.find(group)
        if not found["local"] or found["creator"] != self.destination_hexhash:
            raise ValueError("only the creator can deny join requests")
        if not resolve_join_request(
            self.config.join_requests_path, found["name"], peer
        ):
            raise ValueError("no pending join request from that peer")
        return {
            "status": "denied",
            "group": {
                "id": found["id"],
                "name": found["name"],
                "members": list(found["members"]),
                "creator": found["creator"],
            },
        }

    def send_group_reaction(
        self,
        group,
        message_id,
        reaction,
        *,
        timeout=20.0,
        discover_timeout=None,
        target_author=None,
        target_sent_at=None,
    ):
        if isinstance(group, str):
            group = self.groups.find(group)
        self.group_reactions.append(
            (group["name"], message_id, reaction, target_author, target_sent_at)
        )
        names = {PEER_HASH: "alice", BOB_HASH: "bob"}
        results = []
        for member in group["members"]:
            if member in self.group_refusals:
                results.append(
                    {
                        "to": member,
                        "to_name": names.get(member, ""),
                        "ok": False,
                        "error": "not_allowed",
                        "id": "react-1",
                    }
                )
            elif member == BOB_HASH:
                # Offline member: reactions have no outbox in v1.
                results.append(
                    {
                        "to": member,
                        "to_name": "bob",
                        "ok": False,
                        "error": "no route to peer",
                        "id": "react-1",
                    }
                )
            else:
                results.append(
                    {
                        "to": member,
                        "to_name": names.get(member, ""),
                        "delivered": True,
                        "id": "react-1",
                        "rtt": 0.009,
                    }
                )
        return {
            "ok": all(result.get("delivered") for result in results),
            "v": 1,
            "group": group["id"],
            "name": group["name"],
            "target": message_id,
            "reaction": reaction,
            "cleared": not reaction,
            "id": "react-1",
            "results": results,
        }

    def send_typing(self, peer, state, *, timeout=20.0, discover_timeout=None):
        self.typing.append((peer, state))
        return {"ok": True, "state": state, "to": peer}

    def send_group_typing(self, group, state, *, timeout=20.0, discover_timeout=None):
        if isinstance(group, str):
            group = self.groups.find(group)
        self.group_typing.append((group["id"], state))
        return {
            "ok": True,
            "v": 1,
            "group": group["id"],
            "name": group["name"],
            "state": state,
            "results": [
                {
                    "to": member,
                    "to_name": "alice" if member == PEER_HASH else "bob",
                    "delivered": True,
                    "state": state,
                }
                for member in group["members"]
            ],
        }

    def send_group_retraction(
        self,
        group,
        message_id,
        *,
        timeout=20.0,
        discover_timeout=None,
        target_sent_at=None,
    ):
        # Mirrors send_group_reaction, but a retract is live-only: a
        # member in group_retract_failures answers with that error
        # instead of a delivery, and queued legs are reported back in
        # group_retract_cancelled.
        if isinstance(group, str):
            group = self.groups.find(group)
        self.group_retractions.append(
            (group["name"], message_id, target_sent_at)
        )
        names = {PEER_HASH: "alice", BOB_HASH: "bob"}
        results = []
        for member in group["members"]:
            if member in self.group_retract_failures:
                results.append(
                    {
                        "to": member,
                        "to_name": names.get(member, ""),
                        "ok": False,
                        "error": self.group_retract_failures[member],
                        "id": "retract-1",
                    }
                )
            else:
                results.append(
                    {
                        "to": member,
                        "to_name": names.get(member, ""),
                        "delivered": True,
                        "deleted": True,
                        "id": "retract-1",
                        "rtt": 0.009,
                    }
                )
        return {
            "ok": all(result.get("delivered") for result in results),
            "v": 1,
            "group": group["id"],
            "name": group["name"],
            "target": message_id,
            "id": "retract-1",
            "cancelled_queued": list(self.group_retract_cancelled),
            "results": results,
        }

    def send_group_edit(
        self,
        group,
        message_id,
        text,
        *,
        timeout=20.0,
        discover_timeout=None,
        target_sent_at=None,
    ):
        # Mirrors send_group_retraction, but an edit carries the new
        # text and the signed edit time: a member in
        # group_edit_failures answers with that error instead of a
        # delivery, queued legs are reported back in
        # group_edit_cancelled / group_edit_requeued, and a local
        # copy failure is reported in group_edit_local_errors.
        if isinstance(group, str):
            group = self.groups.find(group)
        self.group_edits.append(
            (group["name"], message_id, target_sent_at, text)
        )
        names = {PEER_HASH: "alice", BOB_HASH: "bob"}
        results = []
        for member in group["members"]:
            if member in self.group_edit_failures:
                results.append(
                    {
                        "to": member,
                        "to_name": names.get(member, ""),
                        "ok": False,
                        "error": self.group_edit_failures[member],
                        "id": "edit-1",
                    }
                )
            else:
                results.append(
                    {
                        "to": member,
                        "to_name": names.get(member, ""),
                        "delivered": True,
                        "edited": True,
                        "id": "edit-1",
                        "rtt": 0.009,
                    }
                )
        return {
            "ok": all(result.get("delivered") for result in results)
            and not self.group_edit_local_errors,
            "local_errors": list(self.group_edit_local_errors),
            "v": 1,
            "group": group["id"],
            "name": group["name"],
            "target": message_id,
            "id": "edit-1",
            "sent_at": 2000.0,
            "cancelled_queued": list(self.group_edit_cancelled),
            "requeued": list(self.group_edit_requeued),
            "results": results,
        }

    def send_file(
        self,
        target,
        path,
        *,
        timeout=300.0,
        discover_timeout=None,
        progress_callback=None,
        forwarded_from=None,
        voice=False,
    ):
        import hashlib
        from pathlib import Path

        data = Path(path).read_bytes()
        peer = target if isinstance(target, Peer) else self.resolve_peer(target)
        self.sent_files.append((peer.name, Path(path).name, data))
        self.sent_file_kwargs.append(
            {"timeout": timeout, "forwarded_from": forwarded_from, "voice": voice}
        )
        return {
            "ok": True,
            "id": f"{len(self.sent_files):032d}",
            "name": Path(path).name,
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "to": peer.hexhash,
        }

    def send_group_file(
        self,
        group,
        path,
        *,
        timeout=300.0,
        discover_timeout=None,
        forwarded_from=None,
        voice=False,
    ):
        import hashlib
        from pathlib import Path

        if isinstance(group, str):
            group = self.groups.find(group)
        data = Path(path).read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        self.group_sent_files.append((group["name"], Path(path).name, data))
        self.group_sent_file_kwargs.append(
            {"timeout": timeout, "forwarded_from": forwarded_from, "voice": voice}
        )
        names = {PEER_HASH: "alice", BOB_HASH: "bob"}
        results = []
        for member in group["members"]:
            if member in self.group_refusals:
                results.append(
                    {
                        "to": member,
                        "to_name": names.get(member, ""),
                        "ok": False,
                        "error": "not_allowed",
                    }
                )
            else:
                results.append(
                    {
                        "to": member,
                        "to_name": names.get(member, ""),
                        "delivered": True,
                        "id": f"file-{member[:8]}",
                        "name": Path(path).name,
                        "size": len(data),
                        "sha256": digest,
                    }
                )
        return {
            "ok": all(r.get("delivered") for r in results),
            "v": 1,
            "group": group["id"],
            "name": group["name"],
            "results": results,
        }

    def send_broadcast_file(
        self, broadcast, path, *, timeout=300.0, discover_timeout=None, voice=False
    ):
        import hashlib
        from pathlib import Path

        if isinstance(broadcast, str):
            broadcast = self.broadcasts.find(broadcast)
        data = Path(path).read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        self.broadcast_sent_files.append(
            (broadcast["name"], Path(path).name, data)
        )
        names = {PEER_HASH: "alice", BOB_HASH: "bob"}
        results = []
        for member in broadcast["members"]:
            if member in self.broadcast_refusals:
                results.append(
                    {
                        "to": member,
                        "to_name": names.get(member, ""),
                        "ok": False,
                        "error": "not_allowed",
                    }
                )
            else:
                results.append(
                    {
                        "to": member,
                        "to_name": names.get(member, ""),
                        "delivered": True,
                        "id": f"file-{member[:8]}",
                        "name": Path(path).name,
                        "size": len(data),
                        "sha256": digest,
                    }
                )
        return {
            "ok": all(r.get("delivered") for r in results),
            "v": 1,
            "broadcast": broadcast["name"],
            "results": results,
        }

    def ping(self, peer, *, timeout=15.0, discover_timeout=None):
        return {"ok": True, "pong": True, "server": self.name, "_rtt": 0.004}

    def fetch_tan_media(self, author, sha256, *, timeout=60.0, discover_timeout=None):
        self.media_fetches.append((author, sha256))
        return {"ok": True, "sha256": sha256, "path": "", "size": 0, "cached": False}

    def request_info(self, peer, *, timeout=15.0, discover_timeout=None):
        return {
            "ok": True,
            "name": peer.name,
            "destination": peer.destination_hash.hex(),
            "protocol": 1,
            "version": "0.2.0",
            "rns_version": "0.9.0",
            "uptime": 3600.0,
            "peers": 3,
            "lxmf": {"enabled": True, "delivery": "ab" * 16},
            "caller": "tester",
        }

    def announce(self):
        self.announced += 1

    def mesh_snapshot(self):
        # Live path-table view, derived from the registry like the real
        # node derives it from Reticulum: every known peer reads as
        # reachable here unless a test overrides the method.
        return [
            {
                "destination": peer.hexhash,
                "name": peer.display_name,
                "hops": peer.hops,
                "has_path": True,
                "reachable": True,
                "last_seen": peer.last_seen,
                "verified": peer.is_verified,
            }
            for peer in self.registry.all()
        ]


@pytest.fixture()
def node(tmp_path):
    return FakeNode(tmp_path / "mailbox.json")


@pytest.fixture()
def bridge(node, tmp_path):
    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    (web_dir / "assets").mkdir()
    (web_dir / "assets" / "app.js").write_text("console.log('tau')", encoding="utf-8")
    bridge = WebBridge(node, host="127.0.0.1", port=0, web_dir=web_dir)
    bridge.start()
    try:
        yield bridge
    finally:
        bridge.stop()


@pytest.fixture()
def proxied_bridge(node, tmp_path):
    """A bridge behind a loopback reverse proxy that is trusted.

    The test client always connects from 127.0.0.1, exactly like a
    reverse proxy forwarding to the bridge over loopback — so the
    socket peer is loopback and only the trusted proxy's
    X-Forwarded-For decides whether the request is local.
    """
    web_dir = tmp_path / "proxied-out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    (web_dir / "assets").mkdir()
    (web_dir / "assets" / "app.js").write_text("console.log('tau')", encoding="utf-8")
    bridge = WebBridge(
        node,
        host="127.0.0.1",
        port=0,
        web_dir=web_dir,
        trusted_proxies=["127.0.0.1"],
    )
    bridge.start()
    try:
        yield bridge
    finally:
        bridge.stop()


def _request(bridge, path, *, method="GET", body=None, raw=None, headers=None):
    url = f"http://127.0.0.1:{bridge.port}{path}"
    data = None
    request_headers = dict(headers or {})
    if raw is not None:
        data = raw
        request_headers.setdefault("Content-Type", "application/json")
    elif body is not None:
        data = json.dumps(body).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")
    request = urllib.request.Request(
        url, data=data, method=method, headers=request_headers
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=5) as response:
        return response.status, dict(response.headers), response.read()


def _request_headers(bridge, path, header_pairs):
    """One GET with repeated headers, in wire order.

    urllib cannot send two fields with the same name, so this uses
    http.client directly — the multi-field X-Forwarded-For spoof case
    needs a visitor-supplied first field and the proxy's second one.
    """
    conn = http.client.HTTPConnection("127.0.0.1", bridge.port, timeout=5)
    try:
        conn.putrequest("GET", path)
        for name, value in header_pairs:
            conn.putheader(name, value)
        conn.endheaders()
        response = conn.getresponse()
        return response.status, response.read()
    finally:
        conn.close()


def _json(bridge, path, **kwargs):
    try:
        status, headers, body = _request(bridge, path, **kwargs)
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), json.loads(exc.read())
    return status, headers, json.loads(body)


def test_status_snapshot(bridge):
    status, _, payload = _json(bridge, "/api/status")
    assert status == 200
    assert payload["node"]["name"] == "tester"
    assert payload["peers"][0]["name"] == "alice"
    assert payload["messages"] == []


def test_peers_endpoint_includes_verification_state(bridge, node):
    peer = node.registry.all()[0]
    peer.verified_at = 1500.0
    peer.verified_fingerprint = "00001 00002"

    status, _, payload = _json(bridge, "/api/peers")
    assert status == 200
    assert payload["count"] == 1
    entry = payload["peers"][0]
    assert entry["verified"] is True
    assert entry["verified_at"] == 1500.0
    assert entry["key_change_at"] is None
    # Verification state is local-only: the public description stays clean.
    assert "verified" not in peer.public_dict()


def test_mesh_endpoint_returns_live_snapshot(bridge, node):
    status, _, payload = _json(bridge, "/api/mesh")
    assert status == 200
    assert payload["count"] == 1
    row = payload["peers"][0]
    assert row["name"] == "alice"
    assert row["destination"] == PEER_HASH
    assert row["hops"] == 1
    assert row["has_path"] is True
    assert row["reachable"] is True


def test_peers_endpoint_includes_live_reachability(bridge, node):
    # The registry hop count is announce-time; the mesh snapshot is
    # the live one, so the peers row carries both.
    node.mesh_snapshot = lambda: [
        {
            "destination": PEER_HASH,
            "name": "alice",
            "hops": 2,
            "has_path": True,
            "reachable": True,
            "last_seen": 1000.0,
            "verified": False,
        }
    ]
    status, _, payload = _json(bridge, "/api/peers")
    assert status == 200
    entry = payload["peers"][0]
    assert entry["hops"] == 1
    assert entry["live_hops"] == 2
    assert entry["reachable"] is True


def test_peers_endpoint_without_mesh_snapshot_omits_reachability(
    bridge, node
):
    node.mesh_snapshot = None
    status, _, payload = _json(bridge, "/api/peers")
    assert status == 200
    entry = payload["peers"][0]
    assert "reachable" not in entry
    assert "live_hops" not in entry


def test_peers_endpoint_flags_lookalike_and_rename(bridge, node, tmp_path):
    registry = _seed_alias_registry(node, tmp_path)
    alice = registry.get(bytes.fromhex(PEER_HASH))
    registry.mark_verified(alice.destination_hash, "00001", now=1500.0)
    registry.upsert(
        bytes.fromhex("ef" * 16), b"\x03" * 64, name="Alice", hops=1
    )
    carol = registry.upsert(
        bytes.fromhex("12" * 16), b"\x04" * 64, name="carol", hops=1
    )
    registry.mark_verified(carol.destination_hash, "00002", now=1500.0)
    registry.upsert(
        bytes.fromhex("12" * 16), b"\x04" * 64, name="caroline", hops=1
    )

    status, _, payload = _json(bridge, "/api/peers")
    assert status == 200
    by_name = {item["name"]: item for item in payload["peers"]}
    assert by_name["Alice"]["lookalike_of"] == "alice"
    assert by_name["Alice"]["verified"] is False
    assert by_name["alice"]["lookalike_of"] is None
    assert by_name["caroline"]["name_change_at"] is not None
    assert by_name["caroline"]["previous_name"] == "carol"
    assert by_name["caroline"]["lookalike_of"] is None


def test_heard_endpoint_lists_foreign_announces(bridge, node):
    node.foreign_announces = lambda: [
        {
            "destination": "ab" * 16,
            "hops": 2,
            "first_seen": 1000.0,
            "last_seen": 2000.0,
            "count": 3,
        }
    ]

    status, _, payload = _json(bridge, "/api/heard")
    assert status == 200
    assert payload["count"] == 1
    assert payload["heard"][0]["destination"] == "ab" * 16
    assert payload["heard"][0]["count"] == 3


def test_heard_endpoint_empty_without_accessor(bridge, node):
    # A node-like without foreign_announces (like this fixture's
    # FakeNode) simply has nothing heard.
    assert not hasattr(node, "foreign_announces")

    status, _, payload = _json(bridge, "/api/heard")
    assert status == 200
    assert payload == {"heard": [], "count": 0}


def test_peers_endpoint_includes_profile(bridge, node, tmp_path):
    from tau.profile import ProfileStore, build_profile

    registry = _seed_alias_registry(node, tmp_path)
    identity = RNS.Identity()
    peer_hash = destination_hash(identity)
    registry.upsert(
        peer_hash, identity.get_public_key(), name="carol", hops=1
    )
    registry.save()
    store = ProfileStore(tmp_path / "profiles.json")
    doc = build_profile(
        identity,
        name="carol",
        bio="mesh tinkerer",
        avatar="ab" * 16,
        links=["https://example.com"],
        version=2,
        created_at=1000.0,
    )
    assert store.store(doc) is True
    assert store.save() is True
    node.profile_store = store

    status, _, payload = _json(bridge, "/api/peers")
    assert status == 200
    by_name = {item["name"]: item for item in payload["peers"]}
    carol = by_name["carol"]["profile"]
    assert carol["bio"] == "mesh tinkerer"
    assert carol["avatar"] == "ab" * 16
    assert carol["links"] == ["https://example.com"]
    assert carol["version"] == 2
    assert carol["author"] == peer_hash.hex()
    # The display view carries no key or signature.
    assert "sig" not in carol and "key" not in carol
    # A peer with no profile reads as null, not as a missing key.
    assert by_name["alice"]["profile"] is None


def test_peers_endpoint_without_profile_store_has_null_profiles(
    bridge, node
):
    # A node-like without a profile store (like this fixture's
    # FakeNode) simply has no profiles — the endpoint still answers.
    assert not hasattr(node, "profile_store")

    status, _, payload = _json(bridge, "/api/peers")
    assert status == 200
    assert payload["peers"][0]["profile"] is None


def _seed_alias_registry(node, tmp_path):
    """Swap the fake registry for a real one the alias endpoint can use."""
    from tau.peers import PeerRegistry

    registry = PeerRegistry(tmp_path / "peers.json")
    registry.upsert(
        bytes.fromhex(PEER_HASH), b"\x01" * 64, name="alice", hops=1
    )
    registry.upsert(
        bytes.fromhex(OTHER_HASH), b"\x02" * 64, name="bob", hops=1
    )
    node.registry = registry
    return registry


def test_peers_alias_endpoint_sets_lists_and_clears(bridge, node, tmp_path):
    _seed_alias_registry(node, tmp_path)

    status, _, payload = _json(
        bridge,
        "/api/peers/alias",
        method="POST",
        body={"destination": PEER_HASH, "alias": "laptop"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["peer"]["alias"] == "laptop"
    assert payload["peer"]["name"] == "alice"

    # The alias is visible in the peer list, so the console can show it.
    _, _, listed = _json(bridge, "/api/peers")
    entry = next(
        item
        for item in listed["peers"]
        if item["destination"] == PEER_HASH
    )
    assert entry["alias"] == "laptop"

    # An empty alias clears it, restoring the announced name as the label.
    status, _, payload = _json(
        bridge,
        "/api/peers/alias",
        method="POST",
        body={"destination": PEER_HASH, "alias": ""},
    )
    assert status == 200
    assert payload["peer"]["alias"] is None


def test_peers_notes_reach_loopback_console_only(
    bridge, node, tmp_path, monkeypatch
):
    """Private peer notes reach the local console only: the console can
    be exposed remotely without owner authentication, so a remote
    client gets no notes in the listing or the snapshot, cannot write
    them — and peer events and mutation replies never carry them, so a
    remote console cannot learn one from the live stream either."""
    from tau.peers import PeerRegistry

    registry = _seed_alias_registry(node, tmp_path)
    registry.set_note(PEER_HASH, "met at the mesh meetup")

    # Loopback (this test's real HTTP client): notes included.
    _, _, listed = _json(bridge, "/api/peers")
    entry = next(
        item
        for item in listed["peers"]
        if item["destination"] == PEER_HASH
    )
    assert entry["note"] == "met at the mesh meetup"

    _, _, status = _json(bridge, "/api/status")
    entry = next(
        item
        for item in status["peers"]
        if item["destination"] == PEER_HASH
    )
    assert entry["note"] == "met at the mesh meetup"

    # The alias reply is a mutation reply, like a peer event: no note.
    status, _, payload = _json(
        bridge,
        "/api/peers/alias",
        method="POST",
        body={"destination": PEER_HASH, "alias": "laptop"},
    )
    assert status == 200
    assert "note" not in payload["peer"]
    assert payload["peer"]["alias"] == "laptop"

    # A remote client sees no notes anywhere, and cannot write them.
    import tau.webapi as webapi

    monkeypatch.setattr(webapi, "_is_loopback_client", lambda client: False)
    _, _, listed = _json(bridge, "/api/peers")
    entry = next(
        item
        for item in listed["peers"]
        if item["destination"] == PEER_HASH
    )
    assert "note" not in entry
    _, _, status = _json(bridge, "/api/status")
    entry = next(
        item
        for item in status["peers"]
        if item["destination"] == PEER_HASH
    )
    assert "note" not in entry
    status, _, payload = _json(
        bridge,
        "/api/peers/note",
        method="POST",
        body={"destination": PEER_HASH, "note": "from afar"},
    )
    assert status == 403


def test_peers_note_endpoint_sets_lists_and_clears(bridge, node, tmp_path):
    _seed_alias_registry(node, tmp_path)

    status, _, payload = _json(
        bridge,
        "/api/peers/note",
        method="POST",
        body={"destination": PEER_HASH, "note": "met at the mesh meetup"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["peer"]["note"] == "met at the mesh meetup"
    assert payload["peer"]["name"] == "alice"

    # The note is visible in the peer list, so the console can show it.
    _, _, listed = _json(bridge, "/api/peers")
    entry = next(
        item
        for item in listed["peers"]
        if item["destination"] == PEER_HASH
    )
    assert entry["note"] == "met at the mesh meetup"

    # A blank note clears it.
    status, _, payload = _json(
        bridge,
        "/api/peers/note",
        method="POST",
        body={"destination": PEER_HASH, "note": ""},
    )
    assert status == 200
    assert payload["peer"]["note"] is None

    # Clearing again has nothing to clear.
    status, _, payload = _json(
        bridge,
        "/api/peers/note",
        method="POST",
        body={"destination": PEER_HASH, "note": ""},
    )
    assert status == 400

    # Unknown peers, bad shapes, and overlong notes are all 400s.
    for body in (
        {"destination": "ff" * 32, "note": "x"},
        {"note": "x"},
        {"destination": PEER_HASH},
        {"destination": PEER_HASH, "note": 42},
        {"destination": PEER_HASH, "note": "x" * 501},
    ):
        status, _, payload = _json(
            bridge, "/api/peers/note", method="POST", body=body
        )
        assert status == 400, body


def test_console_alias_edit_keeps_cli_set_note(bridge, node, tmp_path):
    """A note saved by a one-shot CLI command while the node serves must
    survive a console alias edit: the edit saves the whole registry, so
    it refreshes from disk first instead of writing back its stale copy."""
    from tau.peers import PeerRegistry

    _seed_alias_registry(node, tmp_path)
    # The CLI is a separate registry instance on the same file, so the
    # seeded registry must be on disk first.
    node.registry.save()
    cli_registry = PeerRegistry(tmp_path / "peers.json")
    cli_registry.set_note(PEER_HASH, "met at the mesh meetup")

    status, _, payload = _json(
        bridge,
        "/api/peers/alias",
        method="POST",
        body={"destination": PEER_HASH, "alias": "laptop"},
    )
    assert status == 200
    assert payload["peer"]["alias"] == "laptop"

    reloaded = PeerRegistry(tmp_path / "peers.json")
    stored = reloaded.get(PEER_HASH)
    assert stored is not None
    assert stored.note == "met at the mesh meetup"
    assert stored.alias == "laptop"


def test_peers_alias_response_keeps_blocked_flag(bridge, node, tmp_path):
    """The alias response replaces the console's peer record, so it must
    carry the same annotations as the peer list — a bare local_dict
    would silently drop the blocked badge until the next snapshot."""
    from tau.blocklist import BlockList

    _seed_alias_registry(node, tmp_path)
    node.blocklist = BlockList(blocked=frozenset({PEER_HASH}))
    try:
        status, _, payload = _json(
            bridge,
            "/api/peers/alias",
            method="POST",
            body={"destination": PEER_HASH, "alias": "laptop"},
        )
    finally:
        node.blocklist = BlockList()
    assert status == 200
    assert payload["peer"]["alias"] == "laptop"
    assert payload["peer"]["blocked"] is True


def test_peers_alias_waits_for_node_peer_save_lock(bridge, node, tmp_path):
    """An alias edit must not race the node's peer save: _save_peers
    refreshes aliases from disk before rewriting the file, so an edit
    landing between the refresh and the save would be lost. The
    endpoint therefore holds the node's peer-save lock across the edit
    and its save, like peers_verify does."""
    _seed_alias_registry(node, tmp_path)
    node._save_lock = threading.Lock()
    node._save_lock.acquire()
    try:
        done = threading.Event()
        result: dict = {}

        def edit():
            result["payload"] = bridge.set_peer_alias(PEER_HASH, "laptop")
            done.set()

        worker = threading.Thread(target=edit, daemon=True)
        worker.start()
        # The edit is blocked on the lock: it must not complete (and
        # must not have written the alias) while a peer save holds it.
        assert not done.wait(timeout=1.0)
        assert worker.is_alive()
        assert node.registry.get(PEER_HASH).alias is None
    finally:
        node._save_lock.release()
    worker.join(timeout=5.0)
    assert done.is_set()
    assert result["payload"]["alias"] == "laptop"
    assert node.registry.get(PEER_HASH).alias == "laptop"


def test_peers_alias_refresh_covers_intervening_alias(bridge, node, tmp_path):
    """A concurrent alias edit must not leave neighbours stale: the
    alias captured before the lock may not be the one the edit
    replaces, so the neighbours announcing the intervening alias
    must still be re-published when the edit lands."""
    registry = _seed_alias_registry(node, tmp_path)
    # M announces "A" but alice carries no alias yet, so M has no warning.
    registry.upsert(bytes.fromhex(BOB_HASH), b"\x03" * 64, name="A", hops=1)
    node._save_lock = threading.Lock()
    node._save_lock.acquire()
    try:
        done = threading.Event()
        result: dict = {}

        def edit():
            result["payload"] = bridge.set_peer_alias(PEER_HASH, "B")
            done.set()

        worker = threading.Thread(target=edit, daemon=True)
        worker.start()
        # The edit is blocked on the lock, having already read its
        # previous alias (none). A concurrent edit lands while it
        # waits: alice is now "A", so M announcing "A" has a warning
        # the waiting edit must clear when it replaces "A" with "B".
        assert not done.wait(timeout=1.0)
        assert worker.is_alive()
        registry.set_alias(PEER_HASH, "A")
    finally:
        node._save_lock.release()
    worker.join(timeout=5.0)
    assert done.is_set()
    assert result["payload"]["alias"] == "B"
    assert node.registry.get(PEER_HASH).alias == "B"

    events = [e for e in bridge.bus.replay(0) if e["type"] == "peer"]
    mine = [e["peer"] for e in events if e["peer"]["destination"] == BOB_HASH]
    assert mine, "neighbour announcing the intervening alias was not re-published"
    assert mine[-1]["lookalike_of"] is None


def test_peers_alias_refresh_covers_pre_refresh_alias(bridge, node, tmp_path):
    """A one-shot command may have changed the alias on disk since
    the last refresh: the neighbour announcing the alias the console
    last saw must still be re-published when the edit lands, even
    though the refresh under the lock restores a different alias."""
    registry = _seed_alias_registry(node, tmp_path)
    registry.set_alias(PEER_HASH, "disk")
    # M announces "disk", so it carries a warning for alice-as-"disk".
    registry.upsert(bytes.fromhex(BOB_HASH), b"\x03" * 64, name="disk", hops=1)
    listed = {item["destination"]: item for item in bridge.peers()}
    assert listed[PEER_HASH]["lookalike_of"] is None  # alice herself
    assert listed[BOB_HASH]["lookalike_of"] == "disk"

    # A one-shot command changed the alias on disk to "old"; the
    # bridge's in-memory alias is still "disk" until its next refresh.
    peers_file = tmp_path / "peers.json"
    raw = json.loads(peers_file.read_text(encoding="utf-8"))
    for entry in raw["peers"]:
        if entry["destination"] == PEER_HASH:
            entry["alias"] = "old"
    peers_file.write_text(json.dumps(raw), encoding="utf-8")

    payload = bridge.set_peer_alias(PEER_HASH, "new")
    assert payload["alias"] == "new"

    events = [e for e in bridge.bus.replay(0) if e["type"] == "peer"]
    mine = [e["peer"] for e in events if e["peer"]["destination"] == BOB_HASH]
    assert mine, "neighbour announcing the pre-refresh alias was not re-published"
    assert mine[-1]["lookalike_of"] is None


def test_peers_note_refreshes_lookalike_neighbours(bridge, node, tmp_path):
    """A note makes a peer a saved contact — or stops it being one —
    which anchors or releases the lookalike warning on peers
    announcing its name, so those rows must be re-published too."""
    registry = _seed_alias_registry(node, tmp_path)
    # N announces "alice" but alice carries no alias or note yet.
    registry.upsert(bytes.fromhex(BOB_HASH), b"\x03" * 64, name="alice", hops=1)

    payload = bridge.set_peer_note(PEER_HASH, "met at the meetup")
    assert payload["note"] == "met at the meetup"

    events = [e for e in bridge.bus.replay(0) if e["type"] == "peer"]
    mine = [e["peer"] for e in events if e["peer"]["destination"] == BOB_HASH]
    assert mine, "neighbour was not re-published after a note was set"

    # Clearing the note releases the anchor again.
    bridge.set_peer_note(PEER_HASH, "")
    events = [e for e in bridge.bus.replay(0) if e["type"] == "peer"]
    mine = [e["peer"] for e in events if e["peer"]["destination"] == BOB_HASH]
    assert len(mine) >= 2, "neighbour was not re-published after a note was cleared"


def test_peers_note_refresh_covers_pre_refresh_alias(bridge, node, tmp_path):
    """A one-shot command may have changed the alias on disk since
    the last refresh: a note edit refreshes aliases from disk, so the
    neighbour announcing the alias the console last saw must still be
    re-published when the note edit lands."""
    registry = _seed_alias_registry(node, tmp_path)
    registry.set_alias(PEER_HASH, "disk")
    # M announces "disk" and carries a warning for alice-as-"disk".
    registry.upsert(bytes.fromhex(BOB_HASH), b"\x03" * 64, name="disk", hops=1)
    listed = {item["destination"]: item for item in bridge.peers()}
    assert listed[BOB_HASH]["lookalike_of"] == "disk"

    # A one-shot command changed the alias on disk to "old"; the
    # bridge's in-memory alias is still "disk" until its next refresh.
    peers_file = tmp_path / "peers.json"
    raw = json.loads(peers_file.read_text(encoding="utf-8"))
    for entry in raw["peers"]:
        if entry["destination"] == PEER_HASH:
            entry["alias"] = "old"
    peers_file.write_text(json.dumps(raw), encoding="utf-8")

    # The note edit refreshes aliases (alice becomes "old"), so M
    # announcing "disk" must be re-published with no warning.
    bridge.set_peer_note(PEER_HASH, "met at the meetup")

    events = [e for e in bridge.bus.replay(0) if e["type"] == "peer"]
    mine = [e["peer"] for e in events if e["peer"]["destination"] == BOB_HASH]
    assert mine, "neighbour announcing the pre-refresh alias was not re-published"
    assert mine[-1]["lookalike_of"] is None


def test_peers_alias_endpoint_rejects_bad_requests(bridge, node, tmp_path):
    registry = _seed_alias_registry(node, tmp_path)
    registry.set_alias(bytes.fromhex(OTHER_HASH), "laptop")

    status, _, payload = _json(
        bridge,
        "/api/peers/alias",
        method="POST",
        body={"destination": "ff" * 16, "alias": "ghost"},
    )
    assert status == 400
    assert "unknown peer" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/peers/alias", method="POST", body={"alias": "laptop"}
    )
    assert status == 400
    assert "destination" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/peers/alias",
        method="POST",
        body={"destination": PEER_HASH, "alias": 7},
    )
    assert status == 400
    assert "alias" in payload["error"]["message"]

    # An alias another peer already uses is a clash, not a silent steal.
    status, _, payload = _json(
        bridge,
        "/api/peers/alias",
        method="POST",
        body={"destination": PEER_HASH, "alias": "laptop"},
    )
    assert status == 400
    assert "already used" in payload["error"]["message"]

    # A hex-like alias could never reliably select its owner.
    status, _, payload = _json(
        bridge,
        "/api/peers/alias",
        method="POST",
        body={"destination": PEER_HASH, "alias": "abcdef"},
    )
    assert status == 400
    assert "looks like a destination hash" in payload["error"]["message"]


def test_record_peer_event_carries_trust_fields(bridge, node):
    peer = node.registry.all()[0]
    peer.verified_at = 1500.0
    peer.verified_fingerprint = "00001 00002"
    peer.key_change_at = 1600.0

    bridge.record_peer(peer)
    events = bridge.bus.replay(0)
    peer_events = [event for event in events if event.get("type") == "peer"]
    assert peer_events
    # A live announce must not replace the snapshot record with a
    # trust-free one: the badge and warning survive the event.
    assert peer_events[-1]["peer"]["verified"] is True
    assert peer_events[-1]["peer"]["key_change_at"] == 1600.0


def test_peers_verify_via_api(tmp_path):
    import RNS

    from tau.peers import PeerRegistry, safety_number

    identity = RNS.Identity()
    node = FakeNode(tmp_path / "mailbox.json")
    node.identity = identity
    registry = PeerRegistry(tmp_path / "peers.json")
    peer_key = b"\x03" * 64
    registry.upsert(bytes.fromhex(PEER_HASH), peer_key, name="alice", hops=1)
    node.registry = registry

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A key-change warning is answered by verifying, not left to warn.
        peer = registry.get(PEER_HASH)
        assert peer is not None
        peer.key_change_at = 1500.0

        status, _, payload = _json(
            bridge, "/api/peers/verify", method="POST", body={"target": "alice"}
        )
        assert status == 200
        verify = payload["verify"]
        assert verify["peer"] == PEER_HASH
        assert verify["name"] == "alice"
        assert verify["safety_number"] == safety_number(
            identity.get_public_key(), peer_key
        )
        assert verify["reverified"] is False
        assert verify["fingerprint_changed"] is False

        status, _, payload = _json(bridge, "/api/peers")
        entry = [
            item for item in payload["peers"] if item["destination"] == PEER_HASH
        ][0]
        assert entry["verified"] is True
        assert entry["key_change_at"] is None

        # Re-verifying under a different local identity changes the
        # pairwise number, and the response says so.
        node.identity = RNS.Identity()
        status, _, payload = _json(
            bridge, "/api/peers/verify", method="POST", body={"target": PEER_HASH}
        )
        assert status == 200
        assert payload["verify"]["reverified"] is True
        assert payload["verify"]["fingerprint_changed"] is True

        # Unknown and blank targets are rejected.
        status, _, payload = _json(
            bridge, "/api/peers/verify", method="POST", body={"target": "nobody"}
        )
        assert status == 400
        assert "no peer matches" in payload["error"]["message"]

        status, _, payload = _json(
            bridge, "/api/peers/verify", method="POST", body={"target": "  "}
        )
        assert status == 400
        assert "target" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_peers_add_resolves_contact_card(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/peers/add",
        method="POST",
        body={"destination_hash": PEER_HASH},
    )
    assert status == 200
    added = payload["peer"]
    assert added["peer"] == PEER_HASH
    # The name is the peer's own announced name, not the card's claim.
    assert added["name"] == "alice"
    assert added["known"] is True
    assert added["verified"] is False

    # A peer that is not announcing cannot be resolved.
    status, _, payload = _json(
        bridge,
        "/api/peers/add",
        method="POST",
        body={"destination_hash": "cc" * 16},
    )
    assert status == 502
    assert "unknown peer" in payload["error"]["message"]

    # Malformed and missing hashes are bad requests.
    for bad in ("", "zz", "ab" * 8):
        status, _, payload = _json(
            bridge,
            "/api/peers/add",
            method="POST",
            body={"destination_hash": bad},
        )
        assert status == 400
    status, _, payload = _json(bridge, "/api/peers/add", method="POST", body={})
    assert status == 400
    assert "destination_hash" in payload["error"]["message"]


def test_peers_add_persists_newly_discovered_peer(bridge, node, monkeypatch):
    # A peer discovered through identity recall is only in the
    # in-memory registry until it is saved: add must persist it, or
    # the contact is gone after the node restarts.
    from tau.peers import Peer

    new_hash = "dd" * 16
    discovered = Peer(
        destination_hash=bytes.fromhex(new_hash),
        name="dave",
        public_key=b"\x02" * 32,
        first_seen=1000.0,
        last_seen=time.time(),
        hops=1,
    )

    def _resolve_with_discovery(target, *, timeout=10.0):
        # Mirror TauNode.resolve_peer: an unknown hash that recalls
        # an identity is upserted into the registry.
        peer = node.registry.get(target)
        if peer is not None:
            return peer
        if target == new_hash:
            node.registry.restore(discovered)
            return discovered
        raise NodeError(f"unknown peer: {target}")

    monkeypatch.setattr(node, "resolve_peer", _resolve_with_discovery)
    assert node.registry.get(new_hash) is None
    status, _, payload = _json(
        bridge,
        "/api/peers/add",
        method="POST",
        body={"destination_hash": new_hash},
    )
    assert status == 200
    assert payload["peer"]["known"] is False
    assert node.registry.saved is True
    assert node.registry.get(new_hash) is not None

    # A registry that cannot be saved is a failure, not a success.
    node.registry.fail_save = True
    other_hash = "ee" * 16
    other = Peer(
        destination_hash=bytes.fromhex(other_hash),
        name="erin",
        public_key=b"\x03" * 32,
        first_seen=1000.0,
        last_seen=time.time(),
        hops=1,
    )

    def _resolve_other(target, *, timeout=10.0):
        if target == other_hash:
            node.registry.restore(other)
            return other
        raise NodeError(f"unknown peer: {target}")

    monkeypatch.setattr(node, "resolve_peer", _resolve_other)
    status, _, payload = _json(
        bridge,
        "/api/peers/add",
        method="POST",
        body={"destination_hash": other_hash},
    )
    assert status == 503
    assert "could not save" in payload["error"]["message"]


def test_peers_add_retry_after_failed_save_persists(bridge, node, monkeypatch):
    # A failed save leaves the discovered peer in memory: the retry
    # must save it instead of treating it as known-and-done and
    # reporting success for a peer that vanishes on restart.
    from tau.peers import Peer

    new_hash = "cc" * 16
    discovered = Peer(
        destination_hash=bytes.fromhex(new_hash),
        name="carol",
        public_key=b"\x02" * 32,
        first_seen=1000.0,
        last_seen=time.time(),
        hops=1,
    )

    def _resolve_with_discovery(target, *, timeout=10.0):
        peer = node.registry.get(target)
        if peer is not None:
            return peer
        if target == new_hash:
            node.registry.restore(discovered)
            return discovered
        raise NodeError(f"unknown peer: {target}")

    monkeypatch.setattr(node, "resolve_peer", _resolve_with_discovery)
    node.registry.fail_save = True
    status, _, payload = _json(
        bridge,
        "/api/peers/add",
        method="POST",
        body={"destination_hash": new_hash},
    )
    assert status == 503
    assert "could not save" in payload["error"]["message"]
    # The peer is in memory but not on disk after the failed save.
    assert node.registry.get(new_hash) is discovered
    assert node.registry.saved is False

    # The registry is writable again: the retry must persist the peer
    # it now finds already known.
    node.registry.fail_save = False
    status, _, payload = _json(
        bridge,
        "/api/peers/add",
        method="POST",
        body={"destination_hash": new_hash},
    )
    assert status == 200
    assert payload["peer"]["known"] is True
    assert node.registry.saved is True


def test_peers_add_keeps_disk_alias_set_by_one_shot_command(tmp_path):
    # Adding an already-known contact saves the registry: without a
    # refresh first, an alias (or note) a one-shot `tau peers alias`
    # command wrote to disk since the node's last refresh would be
    # silently overwritten with the stale in-memory value.
    import threading
    from types import SimpleNamespace

    from tau.peers import PeerRegistry

    path = tmp_path / "peers.json"
    registry = PeerRegistry(path)
    registry.upsert(bytes.fromhex(PEER_HASH), b"\x01" * 64, name="alice", now=1000.0)
    registry.save()

    # A one-shot command edits the file through its own registry.
    other = PeerRegistry(path)
    other.set_alias(bytes.fromhex(PEER_HASH), "Al")
    other.save()

    peer = registry.get(PEER_HASH)
    assert peer.alias is None  # the running node has not refreshed yet
    node = SimpleNamespace(
        registry=registry,
        resolve_peer=lambda target, *, timeout=10.0: peer,
        _save_lock=threading.RLock(),
    )
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    result = bridge.add_peer(PEER_HASH)
    assert result["known"] is True

    reloaded = PeerRegistry(path)
    assert reloaded.get(PEER_HASH).alias == "Al"


def test_peers_safety_number_shows_without_verifying(tmp_path):
    import RNS

    from tau.peers import PeerRegistry, safety_number

    identity = RNS.Identity()
    node = FakeNode(tmp_path / "mailbox.json")
    node.identity = identity
    registry = PeerRegistry(tmp_path / "peers.json")
    peer_key = b"\x03" * 64
    registry.upsert(bytes.fromhex(PEER_HASH), peer_key, name="alice", hops=1)
    node.registry = registry

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A key-change warning is still open before the number is shown.
        peer = registry.get(PEER_HASH)
        assert peer is not None
        peer.key_change_at = 1500.0

        status, _, payload = _json(
            bridge,
            "/api/peers/safety-number",
            method="POST",
            body={"target": "alice"},
        )
        assert status == 200
        shown = payload["safety_number"]
        assert shown["peer"] == PEER_HASH
        assert shown["name"] == "alice"
        assert shown["safety_number"] == safety_number(
            identity.get_public_key(), peer_key
        )

        # Showing the number records nothing: the peer stays
        # unverified and the warning stays up until the owner
        # confirms the match.
        status, _, payload = _json(bridge, "/api/peers")
        entry = [
            item for item in payload["peers"] if item["destination"] == PEER_HASH
        ][0]
        assert entry["verified"] is False
        assert entry["key_change_at"] == 1500.0

        # Unknown and blank targets are rejected.
        status, _, payload = _json(
            bridge,
            "/api/peers/safety-number",
            method="POST",
            body={"target": "nobody"},
        )
        assert status == 400
        assert "no peer matches" in payload["error"]["message"]

        status, _, payload = _json(
            bridge,
            "/api/peers/safety-number",
            method="POST",
            body={"target": "  "},
        )
        assert status == 400
        assert "target" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_send_message_is_recorded_and_published(bridge, node):
    from tau.protocol import excerpt_hash

    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    assert status == 200
    assert payload["message"]["direction"] == "out"
    assert payload["message"]["peer_name"] == "alice"
    assert payload["message"]["rtt_ms"] == 12.0
    assert payload["message"]["wire_id"] == "0" * 31 + "1"
    assert payload["message"]["excerpt_hash"] == excerpt_hash("hi")
    assert node.sent == [("alice", "hi", None)]

    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == ["hi"]

    events = bridge.bus.replay(0)
    assert any(e["type"] == "message" for e in events)


def test_retracted_dm_send_returns_null_message_over_api(bridge, node):
    # Finding 2 on #330: a retract landing between the node's inbox
    # store and the bridge append drops nothing yet, so the send's
    # later append must not resurrect the message. The wire send
    # already succeeded, so the API still reports that — with no
    # record to show.
    wire_id = "0" * 31 + "1"  # first send id from the fake node
    assert bridge.delete_sent_by_wire_id(wire_id) is None
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    assert status == 200
    assert payload["message"] is None
    assert payload["response"]["ok"] is True
    assert node.sent == [("alice", "hi", None)]

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []
    events = bridge.bus.replay(0)
    assert not any(e["type"] == "message" for e in events)


def test_retracted_group_send_legs_do_not_resurrect(bridge, node):
    group_id = node.groups.list_groups()[0]["id"]
    # The fake group send shares sent_at 1000.0 across legs.
    assert bridge.delete_sent_group(group_id, 1000.0) is None
    messages, response = bridge.send_group_message("crew", "hi all")
    assert messages == []
    assert response["ok"] is True
    assert node.group_sent == [("crew", "hi all")]

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []
    events = bridge.bus.replay(0)
    assert not any(e["type"] == "message" for e in events)


def test_retracted_sent_id_does_not_hide_later_inbound(bridge):
    # The outbound retraction set is separate from the inbound
    # removal set: a peer reusing a retracted sent id must not hide a
    # later inbound message (the finding-1 lesson on #330).
    wire_id = "reused-wire-id"
    assert bridge.delete_sent_by_wire_id(wire_id) is None
    message = bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello back",
            "sent_at": 1000.0,
            "received_at": 1001.0,
            "id": wire_id,
        }
    )
    assert message is not None
    assert message["direction"] == "in"
    assert message["wire_id"] == wire_id


def test_deleted_sent_dm_stays_deleted_on_reappend(bridge, node):
    # Remembering also covers the entry-found case: a duplicate append
    # for the same wire id after the delete must not resurrect it.
    message, _ = bridge.send_message("alice", "hi")
    wire_id = message["wire_id"]
    assert bridge.delete_sent_by_wire_id(wire_id) is not None
    again = bridge._append_message(
        direction="out",
        peer=PEER_HASH,
        peer_name="alice",
        text="hi",
        sent_at=time.time(),
        received_at=time.time(),
        status="delivered",
        wire_id=wire_id,
    )
    assert again is None
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []


def test_retracted_file_send_does_not_resurrect(bridge, node):
    wire_id = "0" * 31 + "1"  # first file send id from the fake node
    assert bridge.delete_sent_by_wire_id(wire_id) is None
    message, response = bridge.send_file("alice", "notes.txt", b"hello")
    assert message is None
    assert response["ok"] is True
    assert node.sent_files == [("alice", "notes.txt", b"hello")]
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []


def test_send_group_message_records_one_entry_per_leg(bridge, node):
    group_id = node.groups.list_groups()[0]["id"]
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"group": "crew", "text": "hi all"}
    )
    assert status == 200
    assert payload["ok"] is True
    assert node.group_sent == [("crew", "hi all")]
    messages = payload["messages"]
    assert len(messages) == 2
    assert all(m["direction"] == "out" for m in messages)
    assert all(m["group"] == group_id for m in messages)
    assert all(m["group_name"] == "crew" for m in messages)
    assert [m["peer"] for m in messages] == [PEER_HASH, BOB_HASH]
    assert [m["peer_name"] for m in messages] == ["alice", "bob"]
    # One leg delivered, one queued: each keeps its own status and id.
    assert [m["status"] for m in messages] == ["delivered", "queued"]
    assert messages[0]["rtt_ms"] == 9.0
    assert messages[0]["wire_id"] == f"wire-{PEER_HASH[:8]}"
    assert messages[1]["mail_id"] == "mail-bob-1"
    # Both legs carry the one shared wire send time, not a
    # post-delivery timestamp per leg.
    assert [m["sent_at"] for m in messages] == [1000.0, 1000.0]
    # The queued leg is named by its outbox id, the same convention
    # as a queued peer DM: a receipt names the wire id, which for
    # queued mail is the outbox id.
    assert messages[1]["wire_id"] == "mail-bob-1"

    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == ["hi all", "hi all"]

    # A delivery report for the queued leg flips just that entry.
    bridge.record_delivery("mail-bob-1")
    _, _, feed = _json(bridge, "/api/messages")
    assert [m["status"] for m in feed["messages"]] == ["delivered", "delivered"]

    events = bridge.bus.replay(0)
    assert sum(1 for e in events if e["type"] == "message") == 2


def test_send_group_message_accepts_expires_in(bridge, node):
    before = time.time()
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": "crew", "text": "vanish soon", "expires_in": 3600},
    )
    assert status == 200
    assert payload["ok"] is True
    expires_at = node.last_group_expires_at
    assert expires_at is not None
    assert before + 3590 <= expires_at <= time.time() + 3610
    # Every leg's feed entry carries the deadline, like a sent DM.
    assert all(m["expires_at"] == expires_at for m in payload["messages"])

    _, _, feed = _json(bridge, "/api/messages")
    assert all(m["expires_at"] == expires_at for m in feed["messages"])


def test_send_group_message_accepts_send_in(bridge, node):
    before = time.time()
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": "crew", "text": "later all", "send_in": 3600},
    )
    assert status == 200
    assert payload["ok"] is True
    send_after = node.last_group_send_after
    assert send_after is not None
    assert before + 3590 <= send_after <= time.time() + 3610
    # Every leg's feed entry carries the scheduled time, like a sent DM.
    assert all(m["send_after"] == send_after for m in payload["messages"])

    _, _, feed = _json(bridge, "/api/messages")
    assert all(m["send_after"] == send_after for m in feed["messages"])


def test_send_group_message_accepts_reply_to(bridge, node):
    status, _, first = _json(
        bridge, "/api/messages", method="POST", body={"group": "crew", "text": "hi all"}
    )
    assert status == 200
    quoted_wire_id = first["messages"][0]["wire_id"]
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={
            "group": "crew",
            "text": "seconded",
            "reply_to": quoted_wire_id[:12],
        },
    )
    assert status == 200
    assert payload["ok"] is True
    assert node.last_group_in_reply_to == {
        "id": quoted_wire_id,
        "excerpt": hashlib.sha256(b"hi all").hexdigest()[:16],
    }
    # Every leg's feed entry carries the quote, like a sent DM.
    assert all(
        m["in_reply_to"] == node.last_group_in_reply_to
        for m in payload["messages"]
    )


def test_send_group_message_reply_to_must_be_in_this_group(bridge, node):
    # No group message exists yet: any id is unresolvable.
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": "crew", "text": "hi", "reply_to": "a" * 8},
    )
    assert status == 400
    assert "no message" in payload["error"]["message"]
    assert node.group_sent == []

    # A message in another conversation does not count: the peer DM's
    # wire id must not resolve as a group quote.
    status, _, dm = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "for alice"},
    )
    assert status == 200
    dm_wire_id = dm["message"]["wire_id"]
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": "crew", "text": "hi", "reply_to": dm_wire_id},
    )
    assert status == 400
    assert "no message" in payload["error"]["message"]
    assert node.group_sent == []

    # A prefix matching more than one entry is ambiguous: the fake
    # node reuses one wire id per member across sends, so a second
    # group send leaves two entries under the same id.
    status, _, first = _json(
        bridge, "/api/messages", method="POST", body={"group": "crew", "text": "hi all"}
    )
    assert status == 200
    status, _, _ = _json(
        bridge, "/api/messages", method="POST", body={"group": "crew", "text": "hi again"}
    )
    assert status == 200
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={
            "group": "crew",
            "text": "hi",
            "reply_to": first["messages"][0]["wire_id"],
        },
    )
    assert status == 400
    assert "use a longer id" in payload["error"]["message"]
    assert len(node.group_sent) == 2


def test_send_group_message_send_in_validation(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": "crew", "text": "hi", "send_in": -5},
    )
    assert status == 400
    assert "send_in" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": "crew", "text": "hi", "send_in": "soon"},
    )
    assert status == 400
    assert "send_in" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={
            "group": "crew",
            "text": "hi",
            "send_in": 3600,
            "expires_in": 1800,
        },
    )
    assert status == 400
    assert "send_in" in payload["error"]["message"]
    assert node.group_sent == []


def test_send_group_message_resolves_by_id_and_prefix(bridge, node):
    group_id = node.groups.list_groups()[0]["id"]
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": group_id, "text": "by id"},
    )
    assert status == 200
    assert len(payload["messages"]) == 2
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": group_id[:8], "text": "by prefix"},
    )
    assert status == 200
    assert len(payload["messages"]) == 2
    assert node.group_sent == [("crew", "by id"), ("crew", "by prefix")]


def test_send_group_message_sees_groups_changed_by_another_process(tmp_path):
    # The serving bridge holds a cached group store; another process
    # (a one-shot `tau group create`, or an inbound group message)
    # can create or delete a group on disk meanwhile. The send must
    # see the persisted state, not the stale cache.
    groups_path = tmp_path / "groups.json"
    node = FakeNode(tmp_path / "mailbox.json", groups_path=groups_path)
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        other = GroupStore(groups_path)
        other.create("late", [PEER_HASH, BOB_HASH], created_by=PEER_HASH)
        status, _, payload = _json(
            bridge,
            "/api/messages",
            method="POST",
            body={"group": "late", "text": "hi late"},
        )
        assert status == 200
        assert len(payload["messages"]) == 2
        assert node.group_sent == [("late", "hi late")]

        assert other.delete(other.find("late")["id"]) is True
        status, _, payload = _json(
            bridge,
            "/api/messages",
            method="POST",
            body={"group": "late", "text": "hi again"},
        )
        assert status == 404
        assert "unknown group" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_send_group_message_failed_leg_is_recorded(bridge, node):
    node.group_refusals = {BOB_HASH}
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"group": "crew", "text": "hi all"}
    )
    assert status == 200
    # The fan-out is partial: the response says so, and the refused
    # leg is visible in the feed instead of silently dropped.
    assert payload["ok"] is False
    assert [m["status"] for m in payload["messages"]] == ["delivered", "failed"]
    assert payload["messages"][1]["peer_name"] == "bob"
    # A refused leg carries no wire id: the member never saw that
    # message, so no reply may be quoted against it.
    assert "wire_id" not in payload["messages"][1]
    assert payload["response"]["results"][1]["error"] == "not_allowed"


def test_send_group_message_validation(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": "nope", "text": "hi"},
    )
    assert status == 404
    assert "unknown group" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "group": "crew", "text": "hi"},
    )
    assert status == 400
    assert "not both" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": "crew", "text": "hi", "reply_to": "a" * 8},
    )
    assert status == 400
    assert "no message" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": "crew", "text": "hi", "expires_in": -5},
    )
    assert status == 400
    assert "expires_in" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": "crew", "text": "hi", "expires_in": "soon"},
    )
    assert status == 400
    assert "expires_in" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"group": "crew", "text": " "}
    )
    assert status == 400
    assert node.group_sent == []


def test_send_reply_resolves_from_console_history(bridge, node):
    from tau.protocol import excerpt_hash

    status, _, first = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "original thought"},
    )
    assert status == 200
    wire_id = first["message"]["wire_id"]
    assert wire_id

    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "seconded", "reply_to": wire_id[:8]},
    )
    assert status == 200
    assert payload["message"]["in_reply_to"] == {
        "id": wire_id,
        "excerpt": excerpt_hash("original thought"),
    }
    assert node.sent[-1] == (
        "alice",
        "seconded",
        {"id": wire_id, "excerpt": excerpt_hash("original thought")},
    )

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][1]["in_reply_to"]["id"] == wire_id


def test_send_reply_to_unknown_or_other_peer_is_a_bad_request(bridge):
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "dangling", "reply_to": "zzz"},
    )
    assert status == 400
    assert "no message" in payload["error"]["message"]

    # A message from another peer must not resolve in this conversation.
    bridge.record_inbound(
        {
            "from": "carol",
            "destination": "cd" * 16,
            "text": "carol's thought",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "c" * 32,
        }
    )
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "cross", "reply_to": "c" * 8},
    )
    assert status == 400
    assert "no message" in payload["error"]["message"]


def test_inbound_message_keeps_reply_and_wire_id(bridge):
    from tau.protocol import excerpt_hash

    record = {
        "from": "alice",
        "destination": "ab" * 16,
        "text": "seconded",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "b" * 32,
        "in_reply_to": {
            "id": "a" * 32,
            "excerpt": excerpt_hash("original thought"),
        },
    }
    bridge.record_inbound(record)

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["wire_id"] == "b" * 32
    assert feed["messages"][0]["excerpt_hash"] == excerpt_hash("seconded")
    assert feed["messages"][0]["in_reply_to"] == {
        "id": "a" * 32,
        "excerpt": excerpt_hash("original thought"),
    }


def test_inbound_message_comes_back_over_the_api(bridge):
    record = {
        "from": "bob",
        "destination": "ef" * 16,
        "text": "from the mesh",
        "sent_at": 100.0,
        "received_at": 101.0,
    }
    bridge.record_inbound(record)

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["direction"] == "in"
    assert feed["messages"][0]["peer_name"] == "bob"
    assert feed["messages"][0]["text"] == "from the mesh"


def test_messages_search_via_api(bridge):
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello from the mesh",
            "sent_at": 100.0,
            "received_at": 101.0,
        }
    )
    bridge.record_inbound(
        {
            "from": "bob",
            "destination": OTHER_HASH,
            "text": "another MESH note",
            "sent_at": 102.0,
            "received_at": 103.0,
        }
    )
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "unrelated weather chat",
            "sent_at": 104.0,
            "received_at": 105.0,
        }
    )
    # An outbound message matches too: search covers the whole traffic view.
    _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "mesh reply from me"},
    )

    # Case-insensitive, newest first.
    status, _, payload = _json(bridge, "/api/messages/search?q=MESH")
    assert status == 200
    assert [item["text"] for item in payload["messages"]] == [
        "mesh reply from me",
        "another MESH note",
        "hello from the mesh",
    ]
    assert payload["count"] == 3

    status, _, payload = _json(bridge, "/api/messages/search?q=mesh&limit=1")
    assert status == 200
    assert payload["count"] == 1
    assert payload["messages"][0]["text"] == "mesh reply from me"

    # A peer filter narrows to one conversation, by name or hash prefix.
    status, _, payload = _json(bridge, "/api/messages/search?q=mesh&peer=alice")
    assert status == 200
    assert [item["text"] for item in payload["messages"]] == [
        "mesh reply from me",
        "hello from the mesh",
    ]

    status, _, payload = _json(
        bridge, f"/api/messages/search?q=mesh&peer={OTHER_HASH[:8]}"
    )
    assert status == 200
    assert [item["text"] for item in payload["messages"]] == ["another MESH note"]

    status, _, payload = _json(bridge, "/api/messages/search?q=mesh&peer=nobody")
    assert status == 200
    assert payload["messages"] == []

    # An empty query is a client error.
    status, _, payload = _json(bridge, "/api/messages/search?q=%20%20")
    assert status == 400
    assert "must not be empty" in payload["error"]["message"]


def test_messages_search_hides_blocked_and_expired(bridge, node):
    from tau.blocklist import BlockList

    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "mesh from alice",
            "sent_at": 100.0,
            "received_at": 101.0,
        }
    )
    bridge.record_inbound(
        {
            "from": "mallory",
            "destination": OTHER_HASH,
            "text": "mesh from mallory",
            "sent_at": 102.0,
            "received_at": 103.0,
        }
    )
    # A disappearing message whose deadline passed never matches.
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "mesh already gone",
            "sent_at": 104.0,
            "received_at": 105.0,
            "expires_at": 106.0,
        }
    )

    node.blocklist = BlockList(blocked=frozenset({OTHER_HASH}))

    status, _, payload = _json(bridge, "/api/messages/search?q=mesh")
    assert status == 200
    assert [item["text"] for item in payload["messages"]] == ["mesh from alice"]


def test_inbound_group_message_keeps_group_fields(bridge):
    record = {
        "from": "bob",
        "destination": "ef" * 16,
        "text": "hello group",
        "sent_at": 100.0,
        "received_at": 101.0,
        "kind": "group",
        "group": "aa" * 16,
        "group_name": "weekend",
    }
    bridge.record_inbound(record)

    _, _, feed = _json(bridge, "/api/messages")
    message = feed["messages"][0]
    assert message["group"] == "aa" * 16
    assert message["group_name"] == "weekend"

    plain = {
        "from": "bob",
        "destination": "ef" * 16,
        "text": "plain",
        "sent_at": 102.0,
        "received_at": 103.0,
    }
    bridge.record_inbound(plain)
    _, _, feed = _json(bridge, "/api/messages")
    assert "group" not in feed["messages"][1]


def test_api_groups_lists_known_groups(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    node.groups.create(
        "weekend", [PEER_HASH, OTHER_HASH], created_by=PEER_HASH
    )
    status, _, payload = _json(bridge, "/api/groups")
    assert status == 200
    assert payload["count"] == 1
    group = payload["groups"][0]
    assert group["name"] == "weekend"
    assert group["topic"] == ""
    assert [member["hash"] for member in group["members"]] == [
        PEER_HASH,
        OTHER_HASH,
    ]
    # The registry resolves the first member's display name.
    assert group["members"][0]["name"] == "alice"


def test_api_groups_create_adds_creator_and_lists_group(
    bridge, node, tmp_path
):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    status, _, payload = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )
    assert status == 200
    assert payload["ok"] is True
    group = payload["group"]
    assert group["name"] == "weekend"
    # The creator is always a member, even when the caller omits it.
    assert [member["hash"] for member in group["members"]] == sorted(
        [PEER_HASH, node.destination_hexhash]
    )
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 1
    assert listed["groups"][0]["id"] == group["id"]


def test_api_groups_create_is_idempotent(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    first = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )[2]["group"]
    status, _, payload = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )
    assert status == 200
    assert payload["group"]["id"] == first["id"]
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 1


def test_api_broadcasts_list_empty(bridge, node, tmp_path):
    from tau.broadcasts import BroadcastStore

    node.broadcasts = BroadcastStore(tmp_path / "broadcasts.json")
    status, _, payload = _json(bridge, "/api/broadcasts")
    assert status == 200
    assert payload == {"broadcasts": [], "count": 0}


def test_api_broadcasts_create_and_list(bridge, node, tmp_path):
    from tau.broadcasts import BroadcastStore

    node.broadcasts = BroadcastStore(tmp_path / "broadcasts.json")
    status, _, payload = _json(
        bridge,
        "/api/broadcasts",
        method="POST",
        body={"name": "team", "members": [PEER_HASH]},
    )
    assert status == 200
    assert payload["ok"] is True
    broadcast = payload["broadcast"]
    assert broadcast["name"] == "team"
    assert broadcast["members"] == [{"hash": PEER_HASH, "name": "alice"}]
    _, _, listed = _json(bridge, "/api/broadcasts")
    assert listed["count"] == 1
    assert listed["broadcasts"][0]["name"] == "team"


def test_api_broadcasts_create_rejects_bad_input(bridge, node, tmp_path):
    from tau.broadcasts import BroadcastStore

    node.broadcasts = BroadcastStore(tmp_path / "broadcasts.json")
    cases = [
        ({}, "field 'name' is required"),
        ({"name": "  ", "members": [PEER_HASH]}, "field 'name' is required"),
        ({"name": "team"}, "field 'members' must be a non-empty list"),
        ({"name": "team", "members": []}, "field 'members' must be a non-empty list"),
        ({"name": "team", "members": ["not-a-hash"]}, "unknown peer"),
        ({"name": "team", "members": [OTHER_HASH]}, "unknown peer"),
        (
            {"name": "team", "members": [node.destination_hexhash]},
            "cannot add yourself",
        ),
    ]
    for body, message in cases:
        status, _, payload = _json(
            bridge, "/api/broadcasts", method="POST", body=body
        )
        assert status == 400
        assert message in payload["error"]["message"]
    _, _, listed = _json(bridge, "/api/broadcasts")
    assert listed["count"] == 0


def test_api_broadcasts_create_duplicate_name_is_rejected(
    bridge, node, tmp_path
):
    from tau.broadcasts import BroadcastStore

    node.broadcasts = BroadcastStore(tmp_path / "broadcasts.json")
    _json(
        bridge,
        "/api/broadcasts",
        method="POST",
        body={"name": "team", "members": [PEER_HASH]},
    )
    status, _, payload = _json(
        bridge,
        "/api/broadcasts",
        method="POST",
        body={"name": "Team", "members": [PEER_HASH]},
    )
    assert status == 400
    assert "already exists" in payload["error"]["message"]
    _, _, listed = _json(bridge, "/api/broadcasts")
    assert listed["count"] == 1


def test_api_broadcasts_delete_removes_list(bridge, node, tmp_path):
    from tau.broadcasts import BroadcastStore

    node.broadcasts = BroadcastStore(tmp_path / "broadcasts.json")
    _json(
        bridge,
        "/api/broadcasts",
        method="POST",
        body={"name": "team", "members": [PEER_HASH]},
    )
    status, _, payload = _json(
        bridge,
        "/api/broadcasts/delete",
        method="POST",
        body={"broadcast": "team"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["name"] == "team"
    _, _, listed = _json(bridge, "/api/broadcasts")
    assert listed["count"] == 0
    status, _, payload = _json(
        bridge,
        "/api/broadcasts/delete",
        method="POST",
        body={"broadcast": "team"},
    )
    assert status == 400
    assert "unknown broadcast list" in payload["error"]["message"]


def test_api_broadcasts_rename_renames_list(bridge, node, tmp_path):
    from tau.broadcasts import BroadcastStore

    node.broadcasts = BroadcastStore(tmp_path / "broadcasts.json")
    _json(
        bridge,
        "/api/broadcasts",
        method="POST",
        body={"name": "team", "members": [PEER_HASH]},
    )
    _json(
        bridge,
        "/api/broadcasts",
        method="POST",
        body={"name": "other", "members": [PEER_HASH]},
    )
    status, _, payload = _json(
        bridge,
        "/api/broadcasts/rename",
        method="POST",
        body={"broadcast": "team", "new_name": "squad"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["broadcast"]["name"] == "squad"
    assert payload["broadcast"]["members"] == [{"hash": PEER_HASH, "name": "alice"}]
    assert payload["previous_name"] == "team"
    _, _, listed = _json(bridge, "/api/broadcasts")
    assert [item["name"] for item in listed["broadcasts"]] == ["squad", "other"]
    # Renaming onto another list, an unknown list, or a blank name fails.
    for body, message in [
        ({"broadcast": "squad", "new_name": "other"}, "already exists"),
        ({"broadcast": "nope", "new_name": "squad"}, "unknown broadcast list"),
        ({"broadcast": "squad", "new_name": "  "}, "field 'new_name' is required"),
        ({"broadcast": "squad"}, "field 'new_name' is required"),
        ({"new_name": "squad"}, "field 'broadcast' is required"),
    ]:
        status, _, payload = _json(
            bridge, "/api/broadcasts/rename", method="POST", body=body
        )
        assert status == 400
        assert message in payload["error"]["message"]


def test_api_broadcasts_rename_repoints_beacon(bridge, node, tmp_path):
    from tau.broadcasts import BroadcastStore

    node.broadcasts = BroadcastStore(tmp_path / "broadcasts.json")
    node.broadcasts.create("family", [PEER_HASH])
    status, _, started = _json(
        bridge,
        "/api/beacons/start",
        method="POST",
        body={
            "broadcast": "family",
            "lat": 34.05,
            "lon": -83.88,
            "duration_seconds": 900,
        },
    )
    assert status == 200
    beacon_id = started["beacon"]["id"]
    # A rename re-points the running beacon at the new name, so the
    # console's list row keeps its banner and Stop button.
    status, _, payload = _json(
        bridge,
        "/api/broadcasts/rename",
        method="POST",
        body={"broadcast": "family", "new_name": "Crew"},
    )
    assert status == 200
    status, _, listed = _json(bridge, "/api/beacons")
    assert status == 200
    assert listed["count"] == 1
    assert listed["beacons"][0]["id"] == beacon_id
    assert listed["beacons"][0]["target"] == "Crew"


def test_api_broadcasts_delete_stops_beacon(bridge, node, tmp_path):
    from tau.broadcasts import BroadcastStore

    node.broadcasts = BroadcastStore(tmp_path / "broadcasts.json")
    node.broadcasts.create("family", [PEER_HASH])
    status, _, started = _json(
        bridge,
        "/api/beacons/start",
        method="POST",
        body={
            "broadcast": "family",
            "lat": 34.05,
            "lon": -83.88,
            "duration_seconds": 900,
        },
    )
    assert status == 200
    # Deleting the list stops its beacon at once instead of leaving
    # it due until expiry.
    status, _, payload = _json(
        bridge,
        "/api/broadcasts/delete",
        method="POST",
        body={"broadcast": "family"},
    )
    assert status == 200
    # The stopped beacon still lists (like `tau beacon list` shows
    # ended beacons), but it is no longer active.
    status, _, listed = _json(bridge, "/api/beacons")
    assert status == 200
    assert listed["count"] == 1
    assert listed["beacons"][0]["active"] is False


def test_api_broadcasts_send_fans_out_to_members(bridge, node, tmp_path):
    from tau.broadcasts import BroadcastStore

    node.broadcasts = BroadcastStore(tmp_path / "broadcasts.json")
    _json(
        bridge,
        "/api/broadcasts",
        method="POST",
        body={"name": "team", "members": [PEER_HASH]},
    )
    status, _, payload = _json(
        bridge,
        "/api/broadcasts/send",
        method="POST",
        body={"broadcast": "team", "text": "meet at noon"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["name"] == "team"
    assert payload["delivered"] == 1
    assert payload["queued"] == 0
    assert payload["failed"] == 0
    assert payload["results"] == [
        {"hash": PEER_HASH, "name": "alice", "status": "delivered"}
    ]
    assert node.sent == [("alice", "meet at noon", None)]


def test_api_broadcasts_send_reports_partial_failure(
    bridge, node, tmp_path
):
    from tau.broadcasts import BroadcastStore
    from tau.peers import Peer

    # A second registry peer the fake node cannot resolve: its send
    # fails, but the first member still gets the message.
    node.registry._peers.append(
        Peer(
            destination_hash=bytes.fromhex(OTHER_HASH),
            name="bob",
            public_key=b"\x02" * 32,
            first_seen=1000.0,
            last_seen=time.time(),
            hops=1,
        )
    )
    node.broadcasts = BroadcastStore(tmp_path / "broadcasts.json")
    _json(
        bridge,
        "/api/broadcasts",
        method="POST",
        body={"name": "team", "members": [PEER_HASH, OTHER_HASH]},
    )
    status, _, payload = _json(
        bridge,
        "/api/broadcasts/send",
        method="POST",
        body={"broadcast": "team", "text": "meet at noon"},
    )
    assert status == 200
    assert payload["delivered"] == 1
    assert payload["failed"] == 1
    assert payload["results"][0]["status"] == "delivered"
    assert payload["results"][1] == {
        "hash": OTHER_HASH,
        "name": "bob",
        "status": "failed",
        "error": f"unknown peer: {OTHER_HASH}",
    }
    assert node.sent == [("alice", "meet at noon", None)]


def test_api_broadcasts_send_reports_queued_member(bridge, node, tmp_path):
    from tau.broadcasts import BroadcastStore

    node.broadcasts = BroadcastStore(tmp_path / "broadcasts.json")
    _json(
        bridge,
        "/api/broadcasts",
        method="POST",
        body={"name": "team", "members": [PEER_HASH]},
    )
    real_send = node.send_message

    def queued_send(peer, text, **kwargs):
        real_send(peer, text, **kwargs)
        return {"ok": True, "queued": True, "id": "ab" * 16}

    node.send_message = queued_send
    try:
        status, _, payload = _json(
            bridge,
            "/api/broadcasts/send",
            method="POST",
            body={"broadcast": "team", "text": "meet at noon"},
        )
    finally:
        node.send_message = real_send
    assert status == 200
    assert payload["delivered"] == 0
    assert payload["queued"] == 1
    assert payload["failed"] == 0
    assert payload["results"][0]["status"] == "queued"


def test_api_broadcasts_send_rejects_bad_input(bridge, node, tmp_path):
    from tau.broadcasts import BroadcastStore

    node.broadcasts = BroadcastStore(tmp_path / "broadcasts.json")
    _json(
        bridge,
        "/api/broadcasts",
        method="POST",
        body={"name": "team", "members": [PEER_HASH]},
    )
    cases = [
        ({"text": "hi"}, "field 'broadcast' is required"),
        ({"broadcast": "  ", "text": "hi"}, "field 'broadcast' is required"),
        ({"broadcast": "team"}, "field 'text' is required"),
        ({"broadcast": "team", "text": "  "}, "message text is required"),
        ({"broadcast": "nope", "text": "hi"}, "unknown broadcast list"),
    ]
    for body, message in cases:
        status, _, payload = _json(
            bridge, "/api/broadcasts/send", method="POST", body=body
        )
        assert status == 400
        assert message in payload["error"]["message"]
    assert node.sent == []


def test_api_groups_create_rejects_unknown_member(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    status, _, payload = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [OTHER_HASH]},
    )
    assert status == 400
    assert "unknown peer" in payload["error"]["message"]
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 0


def test_api_groups_create_rejects_bad_input(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    cases = [
        ({}, "field 'name' is required"),
        ({"name": "  ", "members": [PEER_HASH]}, "field 'name' is required"),
        ({"name": "weekend"}, "field 'members' must be a non-empty list"),
        ({"name": "weekend", "members": []}, "field 'members' must be a non-empty list"),
        (
            {"name": "weekend", "members": ["not-a-hash"]},
            "unknown peer",
        ),
        (
            {"name": "weekend", "members": [node.destination_hexhash]},
            "a group needs at least one other peer",
        ),
    ]
    for body, message in cases:
        status, _, payload = _json(
            bridge, "/api/groups", method="POST", body=body
        )
        assert status == 400
        assert message in payload["error"]["message"]
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 0


def test_api_groups_create_secret_group(bridge, node, tmp_path):
    from tau.groups import GroupStore
    from tau.protocol import derive_group_id

    store = GroupStore(tmp_path / "groups.json")
    node.groups = store
    # The fake node carries a fixed destination hash but a random
    # identity; a secret group signs its roster attestation with the
    # identity, so the two must agree for the signer to be the
    # creator — a real node always satisfies this by construction.
    node.destination_hexhash = destination_hash(node.identity).hex()
    members = [node.destination_hexhash, PEER_HASH]
    status, _, payload = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH], "secret": True},
    )
    assert status == 200
    group = payload["group"]
    assert group["secret"] is True
    # A secret group's id is an opaque random token, not the id a
    # plain group with the same creator, name and roster derives.
    assert group["id"] != derive_group_id(
        node.destination_hexhash, "weekend", members
    )
    stored = store.find(group["id"])
    assert stored is not None
    assert stored["secret"] is True
    assert stored["roster_attestation"] is not None
    # The same name created plain still derives its id, with no
    # attestation — the secret flag changes the minting, not the name.
    status, _, plain = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "plain", "members": [PEER_HASH]},
    )
    assert status == 200
    assert plain["group"]["secret"] is False
    assert plain["group"]["id"] == derive_group_id(
        node.destination_hexhash, "plain", members
    )
    assert store.find(plain["group"]["id"])["roster_attestation"] is None
    # A non-boolean secret is rejected, not coerced.
    status, _, payload = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "nope", "members": [PEER_HASH], "secret": "yes"},
    )
    assert status == 400
    assert "field 'secret' must be true or false" in payload["error"]["message"]


def test_api_groups_delete_removes_group(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    created = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )[2]["group"]
    status, _, payload = _json(
        bridge,
        "/api/groups/delete",
        method="POST",
        body={"group": "weekend"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["deleted"] == {"id": created["id"], "name": "weekend"}
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 0


def test_api_groups_rename_returns_successor_group(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    created = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )[2]["group"]
    status, _, payload = _json(
        bridge,
        "/api/groups/rename",
        method="POST",
        body={"group": "weekend", "name": "weekdays"},
    )
    assert status == 200
    assert payload["ok"] is True
    # The name derives the id, so the rename is a successor group.
    assert payload["previous_id"] == created["id"]
    assert payload["group"]["id"] != created["id"]
    assert payload["group"]["name"] == "weekdays"
    assert payload["group"]["created_at"] == created["created_at"]
    assert sorted(
        member["hash"] for member in payload["group"]["members"]
    ) == sorted([PEER_HASH, node.destination_hexhash])
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 1
    assert listed["groups"][0]["id"] == payload["group"]["id"]


def test_api_groups_rename_to_same_name_is_a_noop(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    created = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )[2]["group"]
    status, _, payload = _json(
        bridge,
        "/api/groups/rename",
        method="POST",
        body={"group": "weekend", "name": "weekend"},
    )
    assert status == 200
    assert payload["previous_id"] == created["id"]
    assert payload["group"]["id"] == created["id"]
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 1


def test_api_groups_rename_rejects_bad_input(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )
    cases = [
        ({}, "field 'group' is required"),
        ({"group": "weekend"}, "field 'name' is required"),
        ({"group": "weekend", "name": "  "}, "field 'name' is required"),
        ({"group": "missing", "name": "other"}, "unknown group"),
    ]
    for body, message in cases:
        status, _, payload = _json(
            bridge, "/api/groups/rename", method="POST", body=body
        )
        assert status == 400
        assert message in payload["error"]["message"]
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 1


def test_api_groups_rename_refuses_non_creator(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    node.groups.create(
        "weekend", [PEER_HASH, OTHER_HASH], created_by=PEER_HASH
    )
    status, _, payload = _json(
        bridge,
        "/api/groups/rename",
        method="POST",
        body={"group": "weekend", "name": "weekdays"},
    )
    assert status == 400
    assert "only the creator can rename a group" in payload["error"]["message"]
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["groups"][0]["name"] == "weekend"


def test_api_groups_topic_set_and_clear(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    created = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )[2]["group"]
    status, _, payload = _json(
        bridge,
        "/api/groups/topic",
        method="POST",
        body={"group": "weekend", "topic": "  hiking plans  "},
    )
    assert status == 200
    assert payload["ok"] is True
    # A topic change keeps the id.
    assert payload["group"]["id"] == created["id"]
    assert payload["group"]["topic"] == "hiking plans"
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["groups"][0]["topic"] == "hiking plans"
    status, _, payload = _json(
        bridge,
        "/api/groups/topic",
        method="POST",
        body={"group": "weekend", "clear": True},
    )
    assert status == 200
    assert payload["group"]["id"] == created["id"]
    assert payload["group"]["topic"] == ""
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["groups"][0]["topic"] == ""


def test_api_groups_topic_rejects_bad_input(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )
    cases = [
        ({}, "field 'group' is required"),
        ({"group": "weekend"}, "field 'topic' is required"),
        ({"group": "weekend", "topic": 12}, "field 'topic' must be text"),
        (
            {"group": "weekend", "topic": "plans", "clear": True},
            "pass a topic or clear, not both",
        ),
        (
            {"group": "weekend", "topic": "x" * 141},
            "topic is longer than 140 characters",
        ),
        ({"group": "missing", "topic": "plans"}, "unknown group"),
    ]
    for body, message in cases:
        status, _, payload = _json(
            bridge, "/api/groups/topic", method="POST", body=body
        )
        assert status == 400
        assert message in payload["error"]["message"]
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["groups"][0]["topic"] == ""


def test_api_groups_topic_refuses_non_creator(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    node.groups.create(
        "weekend", [PEER_HASH, OTHER_HASH], created_by=PEER_HASH
    )
    status, _, payload = _json(
        bridge,
        "/api/groups/topic",
        method="POST",
        body={"group": "weekend", "topic": "plans"},
    )
    assert status == 400
    assert (
        "only the creator can change a group's topic"
        in payload["error"]["message"]
    )
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["groups"][0]["topic"] == ""


def test_api_groups_announcements_set_and_clear(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    created = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )[2]["group"]
    assert created["announcements_only"] is False
    status, _, payload = _json(
        bridge,
        "/api/groups/announcements",
        method="POST",
        body={"group": "weekend", "announcements_only": True},
    )
    assert status == 200
    assert payload["ok"] is True
    # Switching the setting keeps the id.
    assert payload["group"]["id"] == created["id"]
    assert payload["group"]["announcements_only"] is True
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["groups"][0]["announcements_only"] is True
    status, _, payload = _json(
        bridge,
        "/api/groups/announcements",
        method="POST",
        body={"group": "weekend", "announcements_only": False},
    )
    assert status == 200
    assert payload["group"]["id"] == created["id"]
    assert payload["group"]["announcements_only"] is False
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["groups"][0]["announcements_only"] is False


def test_api_groups_announcements_rejects_bad_input(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )
    cases = [
        ({}, "field 'group' is required"),
        ({"group": "weekend"}, "field 'announcements_only' must be true or false"),
        (
            {"group": "weekend", "announcements_only": "yes"},
            "field 'announcements_only' must be true or false",
        ),
        ({"group": "missing", "announcements_only": True}, "unknown group"),
    ]
    for body, message in cases:
        status, _, payload = _json(
            bridge, "/api/groups/announcements", method="POST", body=body
        )
        assert status == 400
        assert message in payload["error"]["message"]
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["groups"][0]["announcements_only"] is False


def test_api_groups_announcements_refuses_non_creator(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    node.groups.create(
        "weekend", [PEER_HASH, OTHER_HASH], created_by=PEER_HASH
    )
    status, _, payload = _json(
        bridge,
        "/api/groups/announcements",
        method="POST",
        body={"group": "weekend", "announcements_only": True},
    )
    assert status == 400
    assert (
        "only the creator can change a group's announcements setting"
        in payload["error"]["message"]
    )
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["groups"][0]["announcements_only"] is False


def _add_carol_to_registry(node):
    node.registry._peers.append(
        Peer(
            destination_hash=bytes.fromhex(OTHER_HASH),
            name="carol",
            public_key=b"\x02" * 32,
            first_seen=1000.0,
            last_seen=time.time(),
            hops=1,
        )
    )


def test_api_groups_add_member_returns_successor_group(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    _add_carol_to_registry(node)
    created = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )[2]["group"]
    status, _, payload = _json(
        bridge,
        "/api/groups/add-member",
        method="POST",
        body={"group": "weekend", "members": [OTHER_HASH]},
    )
    assert status == 200
    assert payload["ok"] is True
    # The roster changed, so the group is a successor with a new id.
    assert payload["previous_id"] == created["id"]
    assert payload["group"]["id"] != created["id"]
    assert payload["group"]["name"] == "weekend"
    assert sorted(
        member["hash"] for member in payload["group"]["members"]
    ) == sorted([PEER_HASH, OTHER_HASH, node.destination_hexhash])
    assert payload["group"]["created_by_me"] is True
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 1
    assert listed["groups"][0]["id"] == payload["group"]["id"]


def test_api_groups_add_member_carries_default_and_draft_to_successor(
    node, tmp_path
):
    # A roster rotation retires the group id, so the disappearing
    # default and saved draft keyed by it must follow the successor —
    # otherwise the group silently loses both (Qodo review of #772).
    from tau.groups import GroupStore

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        node.groups = GroupStore(tmp_path / "groups.json")
        _add_carol_to_registry(node)
        created = _json(
            bridge,
            "/api/groups",
            method="POST",
            body={"name": "weekend", "members": [PEER_HASH]},
        )[2]["group"]
        status, _, _ = _json(
            bridge,
            "/api/messages/disappearing",
            method="POST",
            body={"group": created["id"], "seconds": 3600},
        )
        assert status == 200
        status, _, _ = _json(
            bridge,
            "/api/drafts",
            method="POST",
            body={
                "kind": "group",
                "target": created["id"],
                "text": "hello crew",
            },
        )
        assert status == 200
        status, _, payload = _json(
            bridge,
            "/api/groups/add-member",
            method="POST",
            body={"group": "weekend", "members": [OTHER_HASH]},
        )
        assert status == 200
        successor_id = payload["group"]["id"]
        assert successor_id != created["id"]
        _, _, defaults = _json(bridge, "/api/messages/disappearing")
        assert defaults["groups"] == {successor_id: 3600.0}
        _, _, drafts = _json(bridge, "/api/drafts")
        assert drafts["count"] == 1
        assert drafts["drafts"][0]["target"] == successor_id
        assert drafts["drafts"][0]["text"] == "hello crew"
    finally:
        bridge.stop()


def test_api_groups_remove_member_drops_offline_member(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    _add_carol_to_registry(node)
    created = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH, OTHER_HASH]},
    )[2]["group"]
    # Carol goes offline: she leaves the registry, but the roster hash
    # is enough to drop her without a lookup.
    node.registry._peers = [
        peer for peer in node.registry._peers if peer.hexhash != OTHER_HASH
    ]
    status, _, payload = _json(
        bridge,
        "/api/groups/remove-member",
        method="POST",
        body={"group": "weekend", "members": [OTHER_HASH]},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["previous_id"] == created["id"]
    assert payload["group"]["id"] != created["id"]
    assert sorted(
        member["hash"] for member in payload["group"]["members"]
    ) == sorted([PEER_HASH, node.destination_hexhash])


def test_api_groups_add_member_accepts_a_name(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    _add_carol_to_registry(node)
    created = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )[2]["group"]
    status, _, payload = _json(
        bridge,
        "/api/groups/add-member",
        method="POST",
        body={"group": "weekend", "members": ["carol"]},
    )
    assert status == 200
    assert payload["previous_id"] == created["id"]
    assert OTHER_HASH in [
        member["hash"] for member in payload["group"]["members"]
    ]


def test_api_groups_change_members_refuses_ambiguous_names(
    bridge, node, tmp_path
):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    _add_carol_to_registry(node)
    # A second peer asserts the same name: neither add nor remove may
    # guess which carol the caller means.
    node.registry._peers.append(
        Peer(
            destination_hash=bytes.fromhex("99" * 16),
            name="carol",
            public_key=b"\x03" * 32,
            first_seen=1000.0,
            last_seen=time.time(),
            hops=1,
        )
    )
    created = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH, OTHER_HASH]},
    )[2]["group"]
    for path in ("/api/groups/add-member", "/api/groups/remove-member"):
        status, _, payload = _json(
            bridge,
            path,
            method="POST",
            body={"group": "weekend", "members": ["carol"]},
        )
        assert status == 400, path
        assert "are named" in payload["error"]["message"], path
    # The roster hash still removes exactly one carol, unambiguously.
    status, _, payload = _json(
        bridge,
        "/api/groups/remove-member",
        method="POST",
        body={"group": "weekend", "members": [OTHER_HASH]},
    )
    assert status == 200
    assert payload["previous_id"] == created["id"]
    assert OTHER_HASH not in [
        member["hash"] for member in payload["group"]["members"]
    ]


def test_api_groups_ban_removes_member_and_lists_banned(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    _add_carol_to_registry(node)
    created = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH, OTHER_HASH]},
    )[2]["group"]
    status, _, payload = _json(
        bridge,
        "/api/groups/ban",
        method="POST",
        body={"group": "weekend", "peer": OTHER_HASH},
    )
    assert status == 200
    assert payload["ok"] is True
    # Banning a member is a roster change, so the successor id applies.
    assert payload["previous_id"] == created["id"]
    assert payload["group"]["id"] != created["id"]
    assert OTHER_HASH not in [
        member["hash"] for member in payload["group"]["members"]
    ]
    assert payload["banned"] == [{"hash": OTHER_HASH, "name": "carol"}]
    status, _, listed = _json(
        bridge, "/api/groups/banned?group=weekend", method="GET"
    )
    assert status == 200
    assert listed["count"] == 1
    assert listed["banned"] == [{"hash": OTHER_HASH, "name": "carol"}]
    # A banned peer cannot be re-added or approved.
    status, _, refused = _json(
        bridge,
        "/api/groups/add-member",
        method="POST",
        body={"group": "weekend", "members": [OTHER_HASH]},
    )
    assert status == 400
    assert "is banned" in refused["error"]["message"]


def test_api_groups_unban_lifts_ban_without_readding(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    _add_carol_to_registry(node)
    _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )
    _json(
        bridge,
        "/api/groups/ban",
        method="POST",
        body={"group": "weekend", "peer": OTHER_HASH},
    )
    status, _, payload = _json(
        bridge,
        "/api/groups/unban",
        method="POST",
        body={"group": "weekend", "peer": OTHER_HASH},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["unbanned"] == {"hash": OTHER_HASH, "name": "carol"}
    assert payload["was_banned"] is True
    # The peer is not re-added to the roster by the unban.
    assert OTHER_HASH not in [
        member["hash"] for member in payload["group"]["members"]
    ]
    # Unbanning again says so instead of failing.
    _, _, again = _json(
        bridge,
        "/api/groups/unban",
        method="POST",
        body={"group": "weekend", "peer": OTHER_HASH},
    )
    assert again["was_banned"] is False
    _, _, listed = _json(bridge, "/api/groups/banned?group=weekend")
    assert listed["count"] == 0


def test_api_groups_ban_rejects_bad_input(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    _add_carol_to_registry(node)
    _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )
    cases = [
        (
            "/api/groups/ban",
            {"group": "weekend", "peer": "00" * 16},
            "unknown peer",
        ),
        (
            "/api/groups/ban",
            {"group": "weekend"},
            "field 'peer' is required",
        ),
        (
            "/api/groups/ban",
            {"group": "nope", "peer": OTHER_HASH},
            "unknown group",
        ),
        (
            "/api/groups/unban",
            {"group": "weekend", "peer": "mallory"},
            "unknown peer",
        ),
    ]
    for path, body, message in cases:
        status, _, payload = _json(bridge, path, method="POST", body=body)
        assert status == 400, (path, body)
        assert message in payload["error"]["message"], (path, body)


def test_api_groups_learned_group_is_not_created_by_me(bridge, node, tmp_path):
    from tau.groups import GroupStore, derive_group_id

    node.groups = GroupStore(tmp_path / "groups.json")
    # A group learned from the mesh that merely claims this node as
    # its creator: the console must not offer member changes for it.
    group_id = derive_group_id(
        node.destination_hexhash,
        "learned",
        [PEER_HASH, node.destination_hexhash],
    )
    assert node.groups.record(
        group_id,
        "learned",
        [PEER_HASH, node.destination_hexhash],
        creator=node.destination_hexhash,
        sender=PEER_HASH,
    )
    status, _, payload = _json(bridge, "/api/groups")
    assert status == 200
    assert payload["groups"][0]["name"] == "learned"
    assert payload["groups"][0]["created_by_me"] is False
    status, _, payload = _json(
        bridge,
        "/api/groups/add-member",
        method="POST",
        body={"group": "learned", "members": [OTHER_HASH]},
    )
    assert status == 400
    assert "only the creator" in payload["error"]["message"]


def test_api_groups_change_members_rejects_bad_input(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    _add_carol_to_registry(node)
    created = _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )[2]["group"]
    cases = [
        (
            "/api/groups/add-member",
            {"group": "weekend", "members": ["00" * 16]},
            "unknown peer",
        ),
        (
            "/api/groups/add-member",
            {"group": "weekend", "members": [PEER_HASH]},
            "already a member",
        ),
        (
            "/api/groups/add-member",
            {"group": "weekend", "members": [node.destination_hexhash]},
            "already a member",
        ),
        (
            "/api/groups/remove-member",
            {"group": "weekend", "members": [OTHER_HASH]},
            "is not a member",
        ),
        (
            "/api/groups/remove-member",
            {"group": "weekend", "members": [node.destination_hexhash]},
            "cannot remove the creator",
        ),
        (
            "/api/groups/add-member",
            {"group": "weekend", "members": []},
            "field 'members' must be a non-empty list",
        ),
        (
            "/api/groups/add-member",
            {"group": "weekend"},
            "field 'members' must be a non-empty list",
        ),
        (
            "/api/groups/add-member",
            {"members": [OTHER_HASH]},
            "field 'group' is required",
        ),
        (
            "/api/groups/add-member",
            {"group": "nope", "members": [OTHER_HASH]},
            "unknown group",
        ),
    ]
    for path, body, message in cases:
        status, _, payload = _json(bridge, path, method="POST", body=body)
        assert status == 400, (path, body)
        assert message in payload["error"]["message"], (path, body)
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 1
    assert listed["groups"][0]["id"] == created["id"]


def test_api_groups_change_members_refuses_non_creator(bridge, node):
    # The fixture group "crew" was created by alice, not this node.
    status, _, payload = _json(bridge, "/api/groups")
    assert status == 200
    assert payload["groups"][0]["name"] == "crew"
    assert payload["groups"][0]["created_by_me"] is False
    status, _, payload = _json(
        bridge,
        "/api/groups/add-member",
        method="POST",
        body={"group": "crew", "members": [OTHER_HASH]},
    )
    assert status == 400
    assert "only the creator" in payload["error"]["message"]
    status, _, payload = _json(
        bridge,
        "/api/groups/remove-member",
        method="POST",
        body={"group": "crew", "members": [PEER_HASH]},
    )
    assert status == 400
    assert "only the creator" in payload["error"]["message"]


def test_api_groups_delete_sees_group_created_by_another_store(
    bridge, node, tmp_path
):
    from tau.groups import GroupStore

    path = tmp_path / "groups.json"
    node.groups = GroupStore(path)
    # A separate process (e.g. `tau group create`) writes the group.
    other = GroupStore(path)
    created = other.create(
        "weekend", [PEER_HASH, node.destination_hexhash], created_by=node.destination_hexhash
    )
    status, _, payload = _json(
        bridge,
        "/api/groups/delete",
        method="POST",
        body={"group": "weekend"},
    )
    assert status == 200
    assert payload["deleted"] == {"id": created["id"], "name": "weekend"}


def test_api_groups_delete_unknown_group_is_400(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    status, _, payload = _json(
        bridge, "/api/groups/delete", method="POST", body={"group": "nope"}
    )
    assert status == 400
    assert "nope" in payload["error"]["message"]
    status, _, payload = _json(
        bridge, "/api/groups/delete", method="POST", body={}
    )
    assert status == 400
    assert payload["error"]["message"] == "field 'group' is required"


def _learned_group(node, tmp_path, name="weekend"):
    """A group learned from the mesh, so the console may leave it."""
    from tau.groups import GroupStore, derive_group_id

    node.groups = GroupStore(tmp_path / "groups.json")
    group_id = derive_group_id(
        PEER_HASH, name, [PEER_HASH, node.destination_hexhash]
    )
    assert node.groups.record(
        group_id,
        name,
        [PEER_HASH, node.destination_hexhash],
        creator=PEER_HASH,
        sender=PEER_HASH,
    )
    return group_id


def test_api_groups_leave_removes_learned_group_and_rejoin_clears_it(
    bridge, node, tmp_path
):
    group_id = _learned_group(node, tmp_path)
    status, _, payload = _json(
        bridge, "/api/groups/leave", method="POST", body={"group": "weekend"}
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["id"] == group_id
    assert payload["name"] == "weekend"
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 0
    _, _, left = _json(bridge, "/api/groups/left")
    assert [entry["id"] for entry in left["left"]] == [group_id]
    assert "left_at" in left["left"][0]
    assert left["ignored"] == []
    # Rejoining clears the left list, but the record is gone: the
    # group only returns when the next message pins it again.
    status, _, payload = _json(
        bridge, "/api/groups/rejoin", method="POST", body={"group": group_id}
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["rejoined"] == group_id
    _, _, left = _json(bridge, "/api/groups/left")
    assert left["left"] == []
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 0


def test_api_groups_leave_refuses_created_group(bridge, node, tmp_path):
    from tau.groups import GroupStore

    node.groups = GroupStore(tmp_path / "groups.json")
    _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "weekend", "members": [PEER_HASH]},
    )
    status, _, payload = _json(
        bridge, "/api/groups/leave", method="POST", body={"group": "weekend"}
    )
    assert status == 400
    assert "only members can leave" in payload["error"]["message"]
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["count"] == 1


def test_api_groups_leave_unknown_group_is_400(bridge, node, tmp_path):
    _learned_group(node, tmp_path)
    status, _, payload = _json(
        bridge, "/api/groups/leave", method="POST", body={"group": "nope"}
    )
    assert status == 400
    assert "nope" in payload["error"]["message"]
    status, _, payload = _json(
        bridge, "/api/groups/leave", method="POST", body={}
    )
    assert status == 400
    assert payload["error"]["message"] == "field 'group' is required"


def test_api_groups_rejoin_rejects_bad_input(bridge, node, tmp_path):
    _learned_group(node, tmp_path)
    status, _, payload = _json(
        bridge, "/api/groups/rejoin", method="POST", body={"group": "nope"}
    )
    assert status == 400
    assert "invalid group id" in payload["error"]["message"]
    never_left = "ab" * 16
    status, _, payload = _json(
        bridge,
        "/api/groups/rejoin",
        method="POST",
        body={"group": never_left},
    )
    assert status == 400
    assert "was never left" in payload["error"]["message"]
    status, _, payload = _json(
        bridge, "/api/groups/rejoin", method="POST", body={}
    )
    assert status == 400
    assert payload["error"]["message"] == "field 'group' is required"


def test_api_groups_ignore_and_unignore_round_trip(bridge, node, tmp_path):
    group_id = _learned_group(node, tmp_path)
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["groups"][0]["ignored"] is False
    status, _, payload = _json(
        bridge, "/api/groups/ignore", method="POST", body={"group": "weekend"}
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["group"] == group_id
    assert payload["name"] == "weekend"
    assert payload["ignored"] is True
    assert payload["changed"] is True
    # Ignoring twice is not an error: the second call reports no change.
    _, _, payload = _json(
        bridge, "/api/groups/ignore", method="POST", body={"group": "weekend"}
    )
    assert payload["ignored"] is True
    assert payload["changed"] is False
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["groups"][0]["ignored"] is True
    _, _, left = _json(bridge, "/api/groups/left")
    assert left["left"] == []
    assert left["ignored"] == [{"id": group_id, "name": "weekend"}]
    status, _, payload = _json(
        bridge,
        "/api/groups/unignore",
        method="POST",
        body={"group": "weekend"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["ignored"] is False
    assert payload["changed"] is True
    _, _, payload = _json(
        bridge,
        "/api/groups/unignore",
        method="POST",
        body={"group": "weekend"},
    )
    assert payload["ignored"] is False
    assert payload["changed"] is False
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["groups"][0]["ignored"] is False
    _, _, left = _json(bridge, "/api/groups/left")
    assert left["ignored"] == []


def test_api_groups_ignore_publishes_groups_event(bridge, node, tmp_path):
    group_id = _learned_group(node, tmp_path)

    def groups_events():
        return [
            event for event in bridge.bus.replay(0) if event["type"] == "groups"
        ]

    _json(
        bridge, "/api/groups/ignore", method="POST", body={"group": "weekend"}
    )
    events = groups_events()
    assert len(events) == 1
    assert events[-1]["groups"][0]["id"] == group_id
    assert events[-1]["groups"][0]["ignored"] is True
    # A no-op ignore (already ignored) publishes nothing new: no state
    # changed, so no console needs to reconcile.
    _json(
        bridge, "/api/groups/ignore", method="POST", body={"group": "weekend"}
    )
    assert len(groups_events()) == 1
    _json(
        bridge,
        "/api/groups/unignore",
        method="POST",
        body={"group": "weekend"},
    )
    events = groups_events()
    assert len(events) == 2
    assert events[-1]["groups"][0]["ignored"] is False


def test_api_groups_ignore_serializes_publish_with_mutation(
    bridge, node, tmp_path, monkeypatch
):
    _learned_group(node, tmp_path)
    entered = threading.Event()
    release = threading.Event()
    original_publish_groups = bridge._publish_groups

    def blocking_publish_groups():
        entered.set()
        assert release.wait(timeout=10)
        original_publish_groups()

    monkeypatch.setattr(bridge, "_publish_groups", blocking_publish_groups)
    results = {}

    def do_ignore():
        results["ignore"] = bridge.ignore_group("weekend")

    ignoring = threading.Thread(target=do_ignore)
    ignoring.start()
    assert entered.wait(timeout=10)
    # The ignore has saved but its publish is still blocked inside
    # the lock: a racing unignore must wait for it, so it cannot
    # publish a newer state first and leave stale flags behind.
    finished = threading.Event()

    def do_unignore():
        results["unignore"] = bridge.unignore_group("weekend")
        finished.set()

    unignoring = threading.Thread(target=do_unignore)
    unignoring.start()
    assert not finished.wait(timeout=0.5)
    release.set()
    ignoring.join(timeout=10)
    unignoring.join(timeout=10)
    assert results["ignore"]["changed"] is True
    assert results["unignore"]["changed"] is True
    events = [event for event in bridge.bus.replay(0) if event["type"] == "groups"]
    assert events[-1]["groups"][0]["ignored"] is False


def test_api_groups_ignore_skips_groups_event_when_group_read_fails(
    bridge, node, tmp_path, monkeypatch
):
    _learned_group(node, tmp_path)
    before = len(
        [event for event in bridge.bus.replay(0) if event["type"] == "groups"]
    )

    def _failing_load(path):
        raise RuntimeError("group store down")

    monkeypatch.setattr("tau.webapi.load_ignored_group_ids", _failing_load)
    status, _, payload = _json(
        bridge, "/api/groups/ignore", method="POST", body={"group": "weekend"}
    )
    assert status == 200
    assert payload["ignored"] is True
    # The ignore itself landed; only the broadcast is skipped, so no
    # console replaces its sidebar with the [] fallback.
    assert (
        len([event for event in bridge.bus.replay(0) if event["type"] == "groups"])
        == before
    )


def test_api_groups_unignore_orphan_publishes_groups_event(
    bridge, node, tmp_path
):
    group_id = _learned_group(node, tmp_path)
    _json(
        bridge, "/api/groups/ignore", method="POST", body={"group": "weekend"}
    )
    assert node.groups.delete(group_id)
    before = len(
        [event for event in bridge.bus.replay(0) if event["type"] == "groups"]
    )
    _json(
        bridge,
        "/api/groups/unignore",
        method="POST",
        body={"group": group_id[:8]},
    )
    events = [
        event for event in bridge.bus.replay(0) if event["type"] == "groups"
    ]
    # The record is gone so the list is empty, but the event still
    # fires: every open console re-reads /api/groups/left and the
    # orphan row clears (#784).
    assert len(events) == before + 1
    assert events[-1]["groups"] == []


def test_api_groups_ignore_unknown_group_is_400(bridge, node, tmp_path):
    _learned_group(node, tmp_path)
    status, _, payload = _json(
        bridge, "/api/groups/ignore", method="POST", body={"group": "nope"}
    )
    assert status == 400
    assert "nope" in payload["error"]["message"]
    status, _, payload = _json(
        bridge, "/api/groups/ignore", method="POST", body={}
    )
    assert status == 400
    assert payload["error"]["message"] == "field 'group' is required"


def test_api_groups_unignore_resolves_orphaned_ignored_id(
    bridge, node, tmp_path
):
    group_id = _learned_group(node, tmp_path)
    _json(
        bridge, "/api/groups/ignore", method="POST", body={"group": "weekend"}
    )
    # The record is gone but the ignore list survived: the console
    # must still offer unignore, by full id or unique prefix.
    assert node.groups.delete(group_id)
    _, _, left = _json(bridge, "/api/groups/left")
    assert left["ignored"] == [{"id": group_id, "name": ""}]
    status, _, payload = _json(
        bridge,
        "/api/groups/unignore",
        method="POST",
        body={"group": group_id[:8]},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["group"] == group_id
    assert payload["name"] == ""
    assert payload["changed"] is True
    # Once removed from the ignore list, the orphaned id no longer
    # resolves at all.
    status, _, payload = _json(
        bridge,
        "/api/groups/unignore",
        method="POST",
        body={"group": group_id},
    )
    assert status == 400
    assert "unknown group" in payload["error"]["message"]


def test_ignored_id_for_query_matches_full_id_and_unique_prefix():
    from tau.webapi import _ignored_id_for_query

    first = "abcd" + "ab" * 14
    second = "abcd" + "ef" * 14
    ignored = frozenset([first, second])
    assert _ignored_id_for_query(ignored, first) == first
    assert _ignored_id_for_query(ignored, first[:8]) == first
    assert _ignored_id_for_query(ignored, "cd" * 16) is None
    assert _ignored_id_for_query(ignored, "ab") is None
    with pytest.raises(ValueError, match="2 ignored groups match"):
        _ignored_id_for_query(ignored, "abcd")


def test_api_groups_leave_and_ignore_flag_in_group_list(
    bridge, node, tmp_path
):
    _learned_group(node, tmp_path)
    _, _, listed = _json(bridge, "/api/groups")
    assert listed["groups"][0]["ignored"] is False
    # A learned group can be left; a group this node created cannot.
    assert listed["groups"][0]["can_leave"] is True
    _json(
        bridge,
        "/api/groups",
        method="POST",
        body={"name": "mine", "members": [PEER_HASH]},
    )
    _, _, listed = _json(bridge, "/api/groups")
    mine = [g for g in listed["groups"] if g["name"] == "mine"][0]
    assert mine["created_by_me"] is True
    assert mine["can_leave"] is False


def test_api_messages_hide_ignored_group_posts(bridge, node, tmp_path):
    group_id = _learned_group(node, tmp_path)
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "crew chat",
            "sent_at": 100.0,
            "received_at": 101.0,
            "kind": "group",
            "group": group_id,
            "group_name": "weekend",
        }
    )
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "plain dm",
            "sent_at": 102.0,
            "received_at": 103.0,
        }
    )
    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == [
        "crew chat",
        "plain dm",
    ]

    _json(
        bridge, "/api/groups/ignore", method="POST", body={"group": "weekend"}
    )
    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == ["plain dm"]
    # A search is an explicit lookup: it still finds the ignored
    # group's posts, like the CLI's `tau messages --search`.
    _, _, found = _json(bridge, "/api/messages/search?q=crew")
    assert [m["text"] for m in found["messages"]] == ["crew chat"]

    _json(
        bridge,
        "/api/groups/unignore",
        method="POST",
        body={"group": "weekend"},
    )
    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == [
        "crew chat",
        "plain dm",
    ]


def test_api_messages_group_include_ignored(bridge, node, tmp_path):
    group_id = _learned_group(node, tmp_path)
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "crew chat",
            "sent_at": 100.0,
            "received_at": 101.0,
            "kind": "group",
            "group": group_id,
            "group_name": "weekend",
        }
    )
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "plain dm",
            "sent_at": 102.0,
            "received_at": 103.0,
        }
    )
    _json(
        bridge, "/api/groups/ignore", method="POST", body={"group": "weekend"}
    )
    # The default listing still hides the ignored group's posts.
    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == ["plain dm"]
    # A bare group lookup hides them too: only the explicit
    # include-ignored flag on a single-group lookup brings them back,
    # so an open ignored thread can show its history.
    _, _, feed = _json(bridge, f"/api/messages?group={group_id}")
    assert feed["messages"] == []
    _, _, feed = _json(
        bridge, f"/api/messages?group={group_id}&include_ignored=1"
    )
    assert [m["text"] for m in feed["messages"]] == ["crew chat"]
    # include_ignored without a group is ignored: the feed must never
    # leak ignored posts by accident.
    _, _, feed = _json(bridge, "/api/messages?include_ignored=1")
    assert [m["text"] for m in feed["messages"]] == ["plain dm"]


def test_api_unread_excludes_ignored_group_posts(bridge, node, tmp_path):
    group_id = _learned_group(node, tmp_path)
    _inbound_record(
        node,
        id="dm-1",
        destination=PEER_HASH,
        text="plain dm",
        sent_at=100.0,
        received_at=101.0,
    )
    _inbound_record(
        node,
        id="group-1",
        destination=PEER_HASH,
        text="crew chat",
        sent_at=102.0,
        received_at=103.0,
        kind="group",
        group=group_id,
        group_name="weekend",
    )
    _, _, status = _json(bridge, "/api/status")
    assert _counts(status["unread"]) == {
        "per_peer": {PEER_HASH: 1},
        "per_group": {group_id: 1},
        "per_channel": {},
        "total": 2,
    }

    _json(
        bridge, "/api/groups/ignore", method="POST", body={"group": "weekend"}
    )
    _, _, status = _json(bridge, "/api/status")
    assert _counts(status["unread"]) == {
        "per_peer": {PEER_HASH: 1},
        "per_group": {},
        "per_channel": {},
        "total": 1,
    }
    # The ignore pushed fresh counts to every open console, so the
    # badge drops without waiting for the next message.
    unread_events = [
        event for event in bridge.bus.replay(0) if event["type"] == "unread"
    ]
    assert _counts(unread_events[-1]["unread"]) == {
        "per_peer": {PEER_HASH: 1},
        "per_group": {},
        "per_channel": {},
        "total": 1,
    }

    _json(
        bridge,
        "/api/groups/unignore",
        method="POST",
        body={"group": "weekend"},
    )
    _, _, status = _json(bridge, "/api/status")
    assert _counts(status["unread"]) == {
        "per_peer": {PEER_HASH: 1},
        "per_group": {group_id: 1},
        "per_channel": {},
        "total": 2,
    }


def test_api_unread_zero_during_quiet_hours(bridge, node, tmp_path):
    _inbound_record(node)
    _, _, status = _json(bridge, "/api/status")
    assert _counts(status["unread"]) == {
        "per_peer": {PEER_HASH: 1},
        "per_group": {},
        "per_channel": {},
        "total": 1,
    }

    # A window covering right now silences the badge; turning it off
    # brings the counts back — the read cursor never moved.
    now = time.localtime()
    current = now.tm_hour * 60 + now.tm_min
    start = (current - 60) % (24 * 60)
    end = (current + 60) % (24 * 60)
    node.config.quiet_hours_path = tmp_path / "quiet_hours.json"
    (tmp_path / "quiet_hours.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "start": f"{start // 60:02d}:{start % 60:02d}",
                "end": f"{end // 60:02d}:{end % 60:02d}",
            }
        ),
        encoding="utf-8",
    )
    _, _, status = _json(bridge, "/api/status")
    assert _counts(status["unread"]) == {
        "per_peer": {},
        "per_group": {},
        "per_channel": {},
        "total": 0,
    }

    (tmp_path / "quiet_hours.json").write_text(
        json.dumps(
            {
                "enabled": False,
                "start": f"{start // 60:02d}:{start % 60:02d}",
                "end": f"{end // 60:02d}:{end % 60:02d}",
            }
        ),
        encoding="utf-8",
    )
    _, _, status = _json(bridge, "/api/status")
    assert _counts(status["unread"]) == {
        "per_peer": {PEER_HASH: 1},
        "per_group": {},
        "per_channel": {},
        "total": 1,
    }


def test_api_groups_ignore_with_corrupt_list_is_500(bridge, node, tmp_path):
    _learned_group(node, tmp_path)
    (tmp_path / "ignored-groups.json").write_text(
        "{not json", encoding="utf-8"
    )
    for path in ("/api/groups/ignore", "/api/groups/unignore"):
        status, _, payload = _json(
            bridge, path, method="POST", body={"group": "weekend"}
        )
        assert status == 500
        assert "ignored-groups" in payload["error"]["message"]


def test_own_profile_endpoints(node, tmp_path):
    from tau.identity import save_identity
    from tau.profile import load_local_profile

    bridge = _bridge_with_data_dir(node, tmp_path)
    save_identity(RNS.Identity(), node.config.identity_path)
    try:
        # A fresh node has nothing published: the node-name default.
        status, _, payload = _json(bridge, "/api/profile")
        assert status == 200
        assert payload == {
            "author": None,
            "name": "tester",
            "bio": "",
            "avatar": None,
            "links": [],
            "version": 0,
            "created_at": None,
            "published": False,
        }

        # Publishing a name and bio signs version 1 and writes it.
        status, _, payload = _json(
            bridge,
            "/api/profile",
            method="POST",
            body={"name": "Tester Two", "bio": "mesh enjoyer"},
        )
        assert status == 200
        assert payload["ok"] is True
        profile = payload["profile"]
        assert profile["name"] == "Tester Two"
        assert profile["bio"] == "mesh enjoyer"
        assert profile["avatar"] is None
        assert profile["links"] == []
        assert profile["version"] == 1
        assert profile["published"] is True
        assert profile["changed"] is True
        assert profile["author"] is not None
        stored = load_local_profile(tmp_path / "profile.json")
        assert stored is not None
        assert stored["version"] == 1
        assert stored["name"] == "Tester Two"

        # A no-op save publishes nothing and leaves the version alone.
        status, _, payload = _json(
            bridge,
            "/api/profile",
            method="POST",
            body={"name": "Tester Two", "bio": "mesh enjoyer"},
        )
        assert payload["profile"]["changed"] is False
        assert payload["profile"]["version"] == 1

        # Omitted fields keep their stored value; links and avatar set.
        avatar = "ab" * 32
        status, _, payload = _json(
            bridge,
            "/api/profile",
            method="POST",
            body={"links": ["https://example.org"], "avatar": avatar},
        )
        assert payload["profile"]["version"] == 2
        assert payload["profile"]["name"] == "Tester Two"
        assert payload["profile"]["bio"] == "mesh enjoyer"
        assert payload["profile"]["links"] == ["https://example.org"]
        assert payload["profile"]["avatar"] == avatar

        # Clearing the bio and links keeps the name and avatar.
        status, _, payload = _json(
            bridge,
            "/api/profile",
            method="POST",
            body={"bio": "", "links": []},
        )
        assert payload["profile"]["version"] == 3
        assert payload["profile"]["bio"] == ""
        assert payload["profile"]["links"] == []
        assert payload["profile"]["name"] == "Tester Two"
        assert payload["profile"]["avatar"] == avatar
    finally:
        bridge.stop()


def test_own_profile_endpoint_rejects_bad_input(node, tmp_path):
    from tau.identity import save_identity

    bridge = _bridge_with_data_dir(node, tmp_path)
    save_identity(RNS.Identity(), node.config.identity_path)
    try:
        for body in (
            {},
            {"name": 42},
            {"bio": "x" * 281},
            {"bio": 42},
            {"avatar": "not-a-hash"},
            {"avatar": 42},
            {"links": ["not-a-url"]},
            {
                "links": [
                    "https://a.example",
                    "https://b.example",
                    "https://c.example",
                    "https://d.example",
                ]
            },
            {"links": "https://example.org"},
        ):
            status, _, payload = _json(
                bridge, "/api/profile", method="POST", body=body
            )
            assert status == 400, body
            assert "error" in payload
    finally:
        bridge.stop()


def test_own_profile_corrupt_file_reads_as_unpublished(node, tmp_path):
    from tau.identity import save_identity

    bridge = _bridge_with_data_dir(node, tmp_path)
    save_identity(RNS.Identity(), node.config.identity_path)
    try:
        # A corrupt profile reads as no profile (like the CLI), never
        # as a crash; a save overwrites it with version 1.
        (tmp_path / "profile.json").write_text("{not json", encoding="utf-8")
        status, _, payload = _json(bridge, "/api/profile")
        assert status == 200
        assert payload["published"] is False
        status, _, payload = _json(
            bridge, "/api/profile", method="POST", body={"name": "Tester Two"}
        )
        assert status == 200
        assert payload["profile"]["version"] == 1
        assert payload["profile"]["changed"] is True
    finally:
        bridge.stop()


def test_own_profile_without_identity(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        status, _, payload = _json(
            bridge, "/api/profile", method="POST", body={"name": "Tester"}
        )
        assert status == 503
        assert "identity" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_own_profile_without_data_dir(bridge):
    # A dataless node has no profile path: edits are refused, but the
    # read still answers the unpublished default instead of failing.
    status, _, payload = _json(bridge, "/api/profile")
    assert status == 200
    assert payload["published"] is False
    assert payload["name"] == ""
    status, _, payload = _json(
        bridge, "/api/profile", method="POST", body={"name": "Tester"}
    )
    assert status == 503
    assert "profile path" in payload["error"]["message"]


def _inbound_record(node, **overrides):
    """A received DM, appended to the node inbox first — the order the
    node itself uses before it hands the record to the bridge."""
    record = {
        "from": "alice",
        "destination": PEER_HASH,
        "text": "hello",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "m-1",
    }
    record.update(overrides)
    node.inbox.append(record)
    return record


def _counts(unread):
    """Unread counts without the revision stamp, for shape checks."""
    return {key: value for key, value in unread.items() if key != "revision"}


def test_snapshot_carries_unread_counts(bridge, node):
    _, _, payload = _json(bridge, "/api/status")
    assert _counts(payload["unread"]) == {"per_peer": {}, "per_group": {}, "per_channel": {}, "total": 0}

    _inbound_record(node)
    _inbound_record(node, id="m-2", received_at=102.0)

    _, _, payload = _json(bridge, "/api/status")
    assert _counts(payload["unread"]) == {"per_peer": {PEER_HASH: 2}, "per_group": {}, "per_channel": {}, "total": 2}


def test_unread_revisions_only_increase(bridge, node):
    first = bridge.unread()["revision"]
    second = bridge.unread()["revision"]
    assert second > first


def test_inbound_message_event_carries_unread_total(bridge, node):
    record = _inbound_record(node)
    bridge.record_inbound(record)

    events = bridge.bus.replay(0)
    message_events = [event for event in events if event["type"] == "message"]
    assert _counts(message_events[-1]["unread"]) == {
        "per_peer": {PEER_HASH: 1},
        "per_group": {},
        "per_channel": {},
        "total": 1,
    }


def test_mark_read_endpoint_clears_a_conversation(bridge, node):
    _inbound_record(node)
    _inbound_record(node, id="m-2", received_at=102.0)
    _inbound_record(
        node,
        destination=OTHER_HASH,
        **{"from": "carol", "id": "m-3", "received_at": 103.0},
    )

    status, _, payload = _json(
        bridge, "/api/messages/read", method="POST", body={"peer": PEER_HASH}
    )
    assert status == 200
    assert payload["ok"] is True
    assert _counts(payload["unread"]) == {"per_peer": {OTHER_HASH: 1}, "per_group": {}, "per_channel": {}, "total": 1}

    # Other tabs learn from the published unread event.
    events = bridge.bus.replay(0)
    unread_events = [event for event in events if event["type"] == "unread"]
    assert _counts(unread_events[-1]["unread"]) == {
        "per_peer": {OTHER_HASH: 1},
        "per_group": {},
        "per_channel": {},
        "total": 1,
    }

    # Marking everything read clears the rest.
    status, _, payload = _json(
        bridge, "/api/messages/read", method="POST", body={}
    )
    assert status == 200
    assert _counts(payload["unread"]) == {"per_peer": {}, "per_group": {}, "per_channel": {}, "total": 0}


def test_mark_read_endpoint_rejects_a_bad_peer(bridge):
    status, _, payload = _json(
        bridge, "/api/messages/read", method="POST", body={"peer": "  "}
    )
    assert status == 400
    assert "peer" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/messages/read", method="POST", body={"peer": 42}
    )
    assert status == 400
    assert "peer" in payload["error"]["message"]


def test_mark_read_endpoint_group_leaves_dm_unread(bridge, node):
    group_id = "11" * 16
    _inbound_record(node)
    _inbound_record(
        node,
        id="g-1",
        received_at=102.0,
        kind="group",
        group=group_id,
        group_name="crew",
    )

    status, _, payload = _json(
        bridge, "/api/messages/read", method="POST", body={"group": group_id}
    )
    assert status == 200
    assert _counts(payload["unread"]) == {
        "per_peer": {PEER_HASH: 1},
        "per_group": {},
        "per_channel": {},
        "total": 1,
    }

    # Reading the DM afterwards clears the rest.
    status, _, payload = _json(
        bridge, "/api/messages/read", method="POST", body={"peer": PEER_HASH}
    )
    assert status == 200
    assert _counts(payload["unread"]) == {"per_peer": {}, "per_group": {}, "per_channel": {}, "total": 0}


def test_unread_counts_fold_a_renamed_groups_predecessors(bridge, node, tmp_path):
    """A renamed group's older posts count under its current id."""
    from tau.groups import derive_group_id

    old_id = _learned_group(node, tmp_path)
    new_id = derive_group_id(
        PEER_HASH,
        "weekend",
        [PEER_HASH, node.destination_hexhash, OTHER_HASH],
    )
    assert node.groups.record(
        new_id,
        "weekend",
        [PEER_HASH, node.destination_hexhash, OTHER_HASH],
        creator=PEER_HASH,
        sender=PEER_HASH,
        renamed_from=[old_id],
    )
    _inbound_record(
        node,
        id="g-old",
        received_at=101.0,
        kind="group",
        group=old_id,
        group_name="weekend",
    )
    _inbound_record(
        node,
        id="g-new",
        received_at=102.0,
        kind="group",
        group=new_id,
        group_name="weekend",
    )

    # Both posts count under the current id — the console has no
    # badge or read action for the retired one.
    _, _, status = _json(bridge, "/api/status")
    assert _counts(status["unread"]) == {
        "per_peer": {},
        "per_group": {new_id: 2},
        "per_channel": {},
        "total": 2,
    }

    # Opening the group clears the whole lineage, old id included.
    status, _, payload = _json(
        bridge, "/api/messages/read", method="POST", body={"group": new_id}
    )
    assert status == 200
    assert _counts(payload["unread"]) == {"per_peer": {}, "per_group": {}, "per_channel": {}, "total": 0}

    # And marking it unread again restores the newest post across
    # the lineage, still under the current id.
    status, _, payload = _json(
        bridge, "/api/messages/unread", method="POST", body={"group": new_id}
    )
    assert status == 200
    assert _counts(payload["unread"]) == {
        "per_peer": {},
        "per_group": {new_id: 1},
        "per_channel": {},
        "total": 1,
    }


def test_mark_read_group_ignores_poisoned_lineage(bridge, node, tmp_path):
    """A claimed predecessor from another creator never clears its mail."""
    from tau.groups import derive_group_id

    mine_id = _learned_group(node, tmp_path)
    other_id = derive_group_id(
        OTHER_HASH, "side", [OTHER_HASH, node.destination_hexhash]
    )
    assert node.groups.record(
        other_id,
        "side",
        [OTHER_HASH, node.destination_hexhash],
        creator=OTHER_HASH,
        sender=OTHER_HASH,
    )
    # The weekend group's creator claims the side group as lineage.
    assert node.groups.record(
        mine_id,
        "weekend",
        [PEER_HASH, node.destination_hexhash],
        creator=PEER_HASH,
        sender=PEER_HASH,
        renamed_from=[other_id],
    )
    _inbound_record(
        node,
        id="g-mine",
        received_at=101.0,
        kind="group",
        group=mine_id,
        group_name="weekend",
    )
    _inbound_record(
        node,
        id="g-other",
        received_at=102.0,
        kind="group",
        group=other_id,
        group_name="side",
    )

    # Opening the weekend group clears only its own post: the side
    # group's unread count stays, and is never folded into weekend's.
    status, _, payload = _json(
        bridge, "/api/messages/read", method="POST", body={"group": mine_id}
    )
    assert status == 200
    assert _counts(payload["unread"]) == {
        "per_peer": {},
        "per_group": {other_id: 1},
        "per_channel": {},
        "total": 1,
    }


def test_mark_read_group_leaves_ignored_predecessor_unread(
    bridge, node, tmp_path
):
    """Opening a renamed group never consumes an ignored predecessor."""
    from tau.groups import derive_group_id

    old_id = _learned_group(node, tmp_path)
    # The group is ignored before it is renamed.
    _json(bridge, "/api/groups/ignore", method="POST", body={"group": "weekend"})
    new_id = derive_group_id(
        PEER_HASH,
        "weekend",
        [PEER_HASH, node.destination_hexhash, OTHER_HASH],
    )
    assert node.groups.record(
        new_id,
        "weekend",
        [PEER_HASH, node.destination_hexhash, OTHER_HASH],
        creator=PEER_HASH,
        sender=PEER_HASH,
        renamed_from=[old_id],
    )
    _inbound_record(
        node,
        id="g-old",
        received_at=101.0,
        kind="group",
        group=old_id,
        group_name="weekend",
    )
    _inbound_record(
        node,
        id="g-new",
        received_at=102.0,
        kind="group",
        group=new_id,
        group_name="weekend",
    )

    # Opening the renamed group clears only its own post; the
    # published counts hide the ignored predecessor entirely.
    status, _, payload = _json(
        bridge, "/api/messages/read", method="POST", body={"group": new_id}
    )
    assert status == 200
    assert _counts(payload["unread"]) == {"per_peer": {}, "per_group": {}, "per_channel": {}, "total": 0}
    # And the predecessor's cursor never moved: its unread post is
    # still there in the raw inbox counts, for when it is unignored.
    assert node.inbox.unread_counts()["per_group"] == {old_id: 1}


def test_mark_read_endpoint_rejects_peer_and_group_together(bridge):
    status, _, payload = _json(
        bridge,
        "/api/messages/read",
        method="POST",
        body={"peer": PEER_HASH, "group": "11" * 16},
    )
    assert status == 400
    assert "peer" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/messages/read", method="POST", body={"group": "  "}
    )
    assert status == 400
    assert "group" in payload["error"]["message"]


def test_mark_unread_endpoint_group_scopes_to_the_group(bridge, node):
    group_id = "11" * 16
    _inbound_record(node)
    _inbound_record(
        node,
        id="g-1",
        received_at=102.0,
        kind="group",
        group=group_id,
        group_name="crew",
    )
    _json(bridge, "/api/messages/read", method="POST", body={})

    status, _, payload = _json(
        bridge, "/api/messages/unread", method="POST", body={"group": group_id}
    )
    assert status == 200
    assert _counts(payload["unread"]) == {
        "per_peer": {},
        "per_group": {group_id: 1},
        "per_channel": {},
        "total": 1,
    }

    # Marking the DM unread too leaves the group mark alone.
    status, _, payload = _json(
        bridge, "/api/messages/unread", method="POST", body={"peer": PEER_HASH}
    )
    assert status == 200
    assert _counts(payload["unread"]) == {
        "per_peer": {PEER_HASH: 1},
        "per_group": {group_id: 1},
        "per_channel": {},
        "total": 2,
    }


def test_mark_unread_endpoint_rejects_peer_and_group_together(bridge, node):
    _inbound_record(node)
    status, _, payload = _json(
        bridge,
        "/api/messages/unread",
        method="POST",
        body={"peer": PEER_HASH, "group": "11" * 16},
    )
    assert status == 400
    assert "peer" in payload["error"]["message"]


def test_mark_unread_endpoint_marks_newest_unread_again(bridge, node):
    _inbound_record(node)
    _inbound_record(node, id="m-2", received_at=102.0)

    # Read first: the conversation shows nothing unread.
    status, _, payload = _json(
        bridge, "/api/messages/read", method="POST", body={"peer": PEER_HASH}
    )
    assert status == 200
    assert _counts(payload["unread"]) == {"per_peer": {}, "per_group": {}, "per_channel": {}, "total": 0}

    status, _, payload = _json(
        bridge, "/api/messages/unread", method="POST", body={"peer": PEER_HASH}
    )
    assert status == 200
    assert payload["ok"] is True
    assert _counts(payload["unread"]) == {"per_peer": {PEER_HASH: 1}, "per_group": {}, "per_channel": {}, "total": 1}

    # Other tabs learn from the published unread event.
    events = bridge.bus.replay(0)
    unread_events = [event for event in events if event["type"] == "unread"]
    assert _counts(unread_events[-1]["unread"]) == {
        "per_peer": {PEER_HASH: 1},
        "per_group": {},
        "per_channel": {},
        "total": 1,
    }

    # Marking again is a no-op: the newest message is already unread.
    status, _, payload = _json(
        bridge, "/api/messages/unread", method="POST", body={"peer": PEER_HASH}
    )
    assert status == 200
    assert _counts(payload["unread"]) == {"per_peer": {PEER_HASH: 1}, "per_group": {}, "per_channel": {}, "total": 1}


def test_mark_unread_endpoint_all_marks_whole_conversation(bridge, node):
    _inbound_record(node)
    _inbound_record(node, id="m-2", received_at=102.0)
    _inbound_record(
        node,
        destination=OTHER_HASH,
        **{"from": "carol", "id": "m-3", "received_at": 103.0},
    )
    _json(bridge, "/api/messages/read", method="POST", body={})

    status, _, payload = _json(
        bridge,
        "/api/messages/unread",
        method="POST",
        body={"peer": PEER_HASH, "all": True},
    )
    assert status == 200
    assert _counts(payload["unread"]) == {"per_peer": {PEER_HASH: 2}, "per_group": {}, "per_channel": {}, "total": 2}


def test_mark_unread_endpoint_rejects_bad_input(bridge, node):
    for body in ({"peer": "  "}, {"peer": 42}, {"peer": PEER_HASH, "all": "yes"}, {}):
        status, _, payload = _json(
            bridge, "/api/messages/unread", method="POST", body=body
        )
        assert status == 400
        assert (
            "peer" in payload["error"]["message"]
            or "all" in payload["error"]["message"]
        )

    # A peer with no stored messages has nothing to mark.
    status, _, payload = _json(
        bridge, "/api/messages/unread", method="POST", body={"peer": OTHER_HASH}
    )
    assert status == 400
    assert "no stored messages" in payload["error"]["message"]


def test_mark_unread_counts_exclude_archived_conversations(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        _inbound_record(node)
        _inbound_record(node, id="m-2", received_at=102.0)
        _inbound_record(
            node,
            destination=OTHER_HASH,
            **{"from": "carol", "id": "m-3", "received_at": 103.0},
        )
        # Carol's conversation reads; alice's stays unread, then her
        # thread is archived.
        _json(bridge, "/api/messages/read", method="POST", body={"peer": OTHER_HASH})
        bridge.archive_conversation("alice")

        # Marking carol's whole conversation unread must not resurrect
        # the archived thread's counts: the bridge reports unread the
        # same way its unread endpoint does.
        status, _, payload = _json(
            bridge,
            "/api/messages/unread",
            method="POST",
            body={"peer": OTHER_HASH, "all": True},
        )
        assert status == 200
        assert _counts(payload["unread"]) == {"per_peer": {OTHER_HASH: 1}, "per_group": {}, "per_channel": {}, "total": 1}
    finally:
        bridge.stop()


def test_mark_unread_counts_exclude_ignored_groups(bridge, node, tmp_path):
    group_id = _learned_group(node, tmp_path)
    _inbound_record(node, id="dm-1", received_at=101.0)
    _inbound_record(
        node,
        id="group-1",
        received_at=103.0,
        kind="group",
        group=group_id,
        group_name="weekend",
    )
    # The DM reads; the group post stays unread, then its group is ignored.
    _json(bridge, "/api/messages/read", method="POST", body={"peer": PEER_HASH})
    _json(bridge, "/api/groups/ignore", method="POST", body={"group": "weekend"})

    # Marking the DM unread must not resurrect the ignored group's
    # hidden count: the bridge reports unread the same way its unread
    # endpoint does, so no console is told to clear it.
    status, _, payload = _json(
        bridge,
        "/api/messages/unread",
        method="POST",
        body={"peer": PEER_HASH, "all": True},
    )
    assert status == 200
    assert _counts(payload["unread"]) == {"per_peer": {PEER_HASH: 1}, "per_group": {}, "per_channel": {}, "total": 1}


def test_mark_unread_endpoint_reports_unsaved_cursor(
    bridge, node, monkeypatch
):
    _inbound_record(node)
    _json(bridge, "/api/messages/read", method="POST", body={"peer": PEER_HASH})

    def failing_save(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(node.inbox, "_save", failing_save)
    status, _, payload = _json(
        bridge, "/api/messages/unread", method="POST", body={"peer": PEER_HASH}
    )
    assert status == 503
    assert "could not save the read state" in payload["error"]["message"]


def test_mark_unread_endpoint_without_inbox_is_an_error(
    bridge, node, monkeypatch
):
    monkeypatch.setattr(node, "inbox", None)
    status, _, payload = _json(
        bridge, "/api/messages/unread", method="POST", body={"peer": PEER_HASH}
    )
    assert status == 400
    assert "no inbox" in payload["error"]["message"]


def test_send_validation(bridge):
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": " "}
    )
    assert status == 400
    assert "text" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"text": "hi"}
    )
    assert status == 400
    assert "to" in payload["error"]["message"]


def test_send_accepts_oversized_timeout(bridge):
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "hi", "timeout": 10**400},
    )
    assert status == 200
    assert payload["ok"] is True


def test_invalid_json_is_a_bad_request(bridge):
    with pytest.raises(urllib.error.HTTPError) as excinfo:
        _request(
            bridge,
            "/api/messages",
            method="POST",
            raw=b"{not json",
        )
    assert excinfo.value.code == 400
    assert b"valid JSON" in excinfo.value.read()


def test_ping_peer(bridge):
    status, _, payload = _json(bridge, "/api/ping", method="POST", body={"to": "alice"})
    assert status == 200
    assert payload["rtt_ms"] == 4.0
    assert payload["peer"]["destination"] == PEER_HASH


def test_ping_accepts_oversized_timeout(bridge):
    status, _, payload = _json(
        bridge, "/api/ping", method="POST", body={"to": "alice", "timeout": 10**400}
    )
    assert status == 200
    assert payload["rtt_ms"] == 4.0


def test_ping_accepts_non_finite_timeout(bridge):
    status, _, payload = _json(
        bridge,
        "/api/ping",
        method="POST",
        body={"to": "alice", "timeout": float("nan")},
    )
    assert status == 200
    assert payload["rtt_ms"] == 4.0


def test_clamped_timeout():
    from tau.webapi import _clamped_timeout

    assert _clamped_timeout(5, 20.0) == 5.0
    assert _clamped_timeout(0.5, 20.0) == 1.0
    assert _clamped_timeout(10**400, 20.0) == 120.0
    assert _clamped_timeout(-(10**400), 20.0) == 1.0
    assert _clamped_timeout(10**6, 15.0) == 120.0
    assert _clamped_timeout("soon", 20.0) == 20.0
    assert _clamped_timeout(True, 20.0) == 20.0
    assert _clamped_timeout(float("nan"), 20.0) == 20.0
    assert _clamped_timeout(float("inf"), 20.0) == 20.0


def test_ping_unknown_peer(bridge):
    status, _, _ = _json(bridge, "/api/ping", method="POST", body={"to": "nobody"})
    assert status == 404


def test_peer_info(bridge):
    status, _, payload = _json(bridge, "/api/info", method="POST", body={"to": "alice"})
    assert status == 200
    assert payload["peer"]["destination"] == PEER_HASH
    assert payload["info"]["version"] == "0.2.0"
    assert payload["info"]["protocol"] == 1
    assert payload["info"]["uptime"] == 3600.0
    assert payload["info"]["peers"] == 3
    assert payload["info"]["lxmf_enabled"] is True


def test_peer_info_unknown_peer(bridge):
    status, _, _ = _json(bridge, "/api/info", method="POST", body={"to": "nobody"})
    assert status == 404


def test_peer_info_missing_target(bridge):
    status, _, _ = _json(bridge, "/api/info", method="POST", body={})
    assert status == 400


def test_peer_info_unknown_name_from_real_resolver(bridge, node, monkeypatch):
    from tau.node import NodeError

    def resolve(target, *, timeout=10.0):
        raise NodeError(f"unknown peer: {target!r}")

    monkeypatch.setattr(node, "resolve_peer", resolve)
    status, _, _ = _json(bridge, "/api/info", method="POST", body={"to": "nobody"})
    assert status == 404


def test_peer_info_accepts_oversized_timeout(bridge):
    status, _, payload = _json(
        bridge, "/api/info", method="POST", body={"to": "alice", "timeout": 10**400}
    )
    assert status == 200
    assert payload["info"]["version"] == "0.2.0"


def test_public_peer_info_drops_non_finite_numbers():
    from tau.webapi import _public_peer_info

    info = _public_peer_info(
        {"protocol": float("nan"), "uptime": float("inf"), "peers": 2}
    )
    assert info == {"peers": 2}


def test_public_peer_info_drops_unexpected_types():
    from tau.webapi import _public_peer_info

    info = _public_peer_info(
        {
            "name": "bob",
            "version": 3,
            "rns_version": "0.9.0",
            "protocol": 2,
            "uptime": "soon",
            "peers": True,
            "lxmf": {"enabled": "yes"},
        }
    )
    assert info == {"name": "bob", "rns_version": "0.9.0", "protocol": 2}


def test_forget_peer(bridge, node):
    result = bridge.forget_peer("alice")
    assert result["ok"] is True
    assert result["peer"]["destination"] == PEER_HASH
    assert node.registry.all() == []
    assert node.registry.saved is True


def test_forget_peer_holds_save_lock(bridge, node):
    # A concurrent alias, verification or node save must not snapshot
    # the peer and write it back over the removal: forget_peer holds
    # the node's peer-save lock across the removal and its save.
    node._save_lock = threading.Lock()
    held_during_save = []
    original_save = node.registry.save

    def save():
        held_during_save.append(node._save_lock.locked())
        return original_save()

    node.registry.save = save
    result = bridge.forget_peer("alice")
    assert result["ok"] is True
    assert held_during_save == [True]


def test_forget_peer_unknown(bridge):
    with pytest.raises(ValueError, match="no peer matches"):
        bridge.forget_peer("nobody")


def test_forget_peer_refuses_queued_mail(bridge, node):
    node.mailbox.enqueue(to=bytes.fromhex(PEER_HASH), text="hello")
    with pytest.raises(PeerForgetBlocked) as excinfo:
        bridge.forget_peer("alice")
    assert excinfo.value.code == "queued_mail"
    assert excinfo.value.queued == 1
    assert node.registry.all() != []


def test_forget_peer_force_strands_queued_mail(bridge, node):
    node.mailbox.enqueue(to=bytes.fromhex(PEER_HASH), text="hello")
    result = bridge.forget_peer("alice", force=True)
    assert result["ok"] is True
    assert node.registry.all() == []


def test_forget_peer_refuses_pending_acks(bridge, node):
    node.mailbox.add_pending_acks(PEER_HASH, ["mid-1"])
    with pytest.raises(ValueError, match="acknowledgements owed"):
        bridge.forget_peer("alice")
    assert node.registry.all() != []


def test_forget_peer_restores_record_when_save_fails(bridge, node):
    node.registry.fail_save = True
    with pytest.raises(OSError, match="disk full"):
        bridge.forget_peer("alice")
    # The removal did not persist, so the live record comes back and
    # the serving node agrees with the file again.
    assert [peer.name for peer in node.registry.all()] == ["alice"]


def test_forget_peer_endpoint(bridge, node):
    status, _, payload = _json(
        bridge, "/api/peers/forget", method="POST", body={"target": "alice"}
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["peer"]["destination"] == PEER_HASH
    assert node.registry.all() == []


def test_forget_peer_endpoint_reports_queued_mail(bridge, node):
    node.mailbox.enqueue(to=bytes.fromhex(PEER_HASH), text="hello")
    status, _, payload = _json(
        bridge, "/api/peers/forget", method="POST", body={"target": "alice"}
    )
    assert status == 400
    assert payload["error"]["code"] == "queued_mail"
    assert payload["error"]["queued"] == 1
    assert node.registry.all() != []
    # Forgetting anyway strands the queued mail, as the CLI's --force does.
    status, _, payload = _json(
        bridge,
        "/api/peers/forget",
        method="POST",
        body={"target": "alice", "force": True},
    )
    assert status == 200
    assert node.registry.all() == []


def test_announce(bridge, node):
    status, _, payload = _json(bridge, "/api/announce", method="POST", body={})
    assert status == 200
    assert payload["ok"] is True
    assert node.announced == 1


def test_id_rename_via_api(bridge, node, tmp_path):
    from types import SimpleNamespace

    node.config = SimpleNamespace(data_dir=tmp_path)
    node.app = SimpleNamespace(name="tester")
    status, _, payload = _json(
        bridge, "/api/id/rename", method="POST", body={"name": "  renamed  "}
    )
    assert status == 200
    assert payload == {
        "ok": True,
        "name": "renamed",
        "previous_name": "tester",
        "changed": True,
    }
    # The live node takes the new name at once: announces, DMs and new
    # Tan posts all read it from memory.
    assert node.name == "renamed"
    assert node.app.name == "renamed"
    assert node.announced == 1
    stored = json.loads((tmp_path / "node.json").read_text(encoding="utf-8"))
    assert stored["name"] == "renamed"
    status, _, payload = _json(bridge, "/api/status")
    assert payload["node"]["name"] == "renamed"


def test_id_rename_concurrent_renames_stay_consistent(bridge, node, tmp_path):
    from types import SimpleNamespace

    node.config = SimpleNamespace(data_dir=tmp_path)
    errors = []

    def rename(name):
        try:
            bridge.id_rename(name)
        except Exception as exc:  # noqa: BLE001 - collected, asserted below
            errors.append(exc)

    threads = [
        threading.Thread(target=rename, args=(name,))
        for name in ("alpha", "beta")
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    # Both renames landed in some order; disk and the live node agree on
    # whichever ran second. (Regression: the serialising lock must exist.)
    assert node.name in ("alpha", "beta")
    stored = json.loads((tmp_path / "node.json").read_text(encoding="utf-8"))
    assert stored["name"] == node.name


def test_id_rename_same_name_is_a_noop(bridge, node, tmp_path):
    from types import SimpleNamespace

    node.config = SimpleNamespace(data_dir=tmp_path)
    status, _, payload = _json(
        bridge, "/api/id/rename", method="POST", body={"name": "tester"}
    )
    assert status == 200
    assert payload["changed"] is False
    assert payload["previous_name"] == "tester"
    assert node.announced == 0
    assert not (tmp_path / "node.json").exists()


def test_id_rename_rejects_blank_and_missing_names(bridge, node, tmp_path):
    from types import SimpleNamespace

    node.config = SimpleNamespace(data_dir=tmp_path)
    for body in ({"name": "   "}, {"name": ""}, {}, {"name": 42}):
        status, _, payload = _json(
            bridge, "/api/id/rename", method="POST", body=body
        )
        assert status == 400
        assert "name" in payload["error"]["message"]
    assert node.name == "tester"
    assert node.announced == 0


def test_backup_name_is_filename_safe():
    assert _backup_name("alice") == "alice"
    assert _backup_name("al ice/x") == "al-ice-x"
    assert _backup_name("") == "node"
    assert _backup_name("a" * 100) == "a" * 64


def test_backup_downloads_a_valid_archive(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert (
        cli.main(["init", "--data-dir", str(data_dir), "--name", "alice", "--json"])
        == 0
    )
    capsys.readouterr()
    (data_dir / "peers.json").write_text('{"peers": []}\n', encoding="utf-8")
    node = FakeNode(tmp_path / "mailbox.json")
    node.config = TauConfig.resolve(data_dir=data_dir)
    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    bridge = WebBridge(node, host="127.0.0.1", port=0, web_dir=web_dir)
    bridge.start()
    try:
        # A local Origin (the dev console on :3000) is what makes the
        # bridge send CORS headers at all.
        status, headers, body = _request(
            bridge, "/api/backup", headers={"Origin": "http://localhost:3000"}
        )
    finally:
        bridge.stop()
    assert status == 200
    assert headers["Content-Type"] == "application/gzip"
    assert headers["Access-Control-Expose-Headers"] == "Content-Disposition"
    disposition = headers["Content-Disposition"]
    assert disposition.startswith("attachment; ")
    assert disposition.endswith('.tar.gz"')
    assert "tau-backup-alice-" in disposition
    assert headers["Content-Length"] == str(len(body))

    archive = tarfile.open(fileobj=io.BytesIO(body), mode="r:gz")
    with archive:
        manifest = json.loads(archive.extractfile(MANIFEST_NAME).read())
        assert manifest["format"] == 1
        assert "identity" in manifest["files"]
        assert "peers.json" in manifest["files"]

    # The download restores as a working node with the same identity.
    restored_dir = tmp_path / "restored"
    archive_path = tmp_path / "downloaded.tar.gz"
    archive_path.write_bytes(body)
    manifest, _files = restore_backup(
        TauConfig.resolve(data_dir=restored_dir), archive_path
    )
    assert manifest["format"] == 1
    original = load_identity(data_dir / "identity")
    restored = load_identity(restored_dir / "identity")
    assert destination_hash(restored) == destination_hash(original)


def test_backup_without_a_data_directory_is_unavailable(bridge):
    status, _, payload = _json(bridge, "/api/backup")
    assert status == 503
    assert "no data directory" in payload["error"]["message"]


def test_backup_download_rejects_remote_client(bridge, monkeypatch):
    # The archive holds the node identity and peers.json — which now
    # stores private peer notes — so a remote client must not be able
    # to download it even when the bridge is bound to a remote interface.
    import tau.webapi as webapi

    monkeypatch.setattr(webapi, "_is_loopback_client", lambda client: False)
    status, _, payload = _json(bridge, "/api/backup")
    assert status == 403
    assert "only available from localhost" in payload["error"]["message"]


def _start_auto_backup_bridge(tmp_path, *, encrypted=False):
    """Start a bridge over a real node dir with one auto-backup taken."""
    data_dir = tmp_path / "node"
    assert (
        cli.main(["init", "--data-dir", str(data_dir), "--name", "alice", "--json"])
        == 0
    )
    directory = tmp_path / "backups"
    set_node_setting(data_dir, "backup_interval", 3600)
    set_node_setting(data_dir, "backup_dir", str(directory))
    if encrypted:
        passphrase_file = tmp_path / "pass.txt"
        passphrase_file.write_text(
            "correct horse battery staple\n", encoding="utf-8"
        )
        set_node_setting(data_dir, "backup_passphrase_file", str(passphrase_file))
    scheduler = AutoBackupScheduler(TauConfig.resolve(data_dir=data_dir))
    target = scheduler.run(now=1764028800.0)
    assert target is not None
    node = FakeNode(tmp_path / "mailbox.json")
    node.config = TauConfig.resolve(data_dir=data_dir)
    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    bridge = WebBridge(node, host="127.0.0.1", port=0, web_dir=web_dir)
    bridge.start()
    return bridge, data_dir, target


def test_verify_latest_auto_backup_plaintext(tmp_path):
    bridge, data_dir, target = _start_auto_backup_bridge(tmp_path)
    try:
        result = bridge.verify_latest_auto_backup()
    finally:
        bridge.stop()
    assert result["backup"] == target.name
    assert result["node"] == "alice"
    assert result["destination"] == destination_hash(
        load_identity(data_dir / "identity")
    ).hex()
    assert "identity" in result["files"]
    assert result["encrypted"] is False
    assert result["valid"] is True


def test_verify_latest_auto_backup_encrypted(tmp_path):
    bridge, _data_dir, target = _start_auto_backup_bridge(
        tmp_path, encrypted=True
    )
    try:
        result = bridge.verify_latest_auto_backup()
    finally:
        bridge.stop()
    assert result["backup"] == target.name
    assert result["encrypted"] is True
    assert result["valid"] is True


def test_verify_latest_auto_backup_newest_wins(tmp_path):
    bridge, data_dir, first = _start_auto_backup_bridge(tmp_path)
    try:
        scheduler = AutoBackupScheduler(TauConfig.resolve(data_dir=data_dir))
        second = scheduler.run(now=1764028800.0 + 3600)
        assert second is not None and second != first
        result = bridge.verify_latest_auto_backup()
    finally:
        bridge.stop()
    assert result["backup"] == second.name


def test_verify_latest_auto_backup_ignores_unregistered_archive(tmp_path):
    # A valid archive that merely shares the backup directory — even
    # one named exactly like an automatic backup — is never verified:
    # only registry names count.
    bridge, data_dir, target = _start_auto_backup_bridge(tmp_path)
    try:
        from tau.auto_backup import auto_backup_filename

        later = auto_backup_filename(
            "alice", encrypted=False, now=1764028800.0 + 7200
        )
        (tmp_path / "backups" / later).write_bytes(target.read_bytes())
        result = bridge.verify_latest_auto_backup()
    finally:
        bridge.stop()
    assert result["backup"] == target.name


def test_verify_latest_auto_backup_ignores_missing_passphrase_file_for_plaintext(
    tmp_path,
):
    # A passphrase file configured after the plaintext backup was taken —
    # and since deleted — must not fail verification of a valid archive.
    bridge, data_dir, target = _start_auto_backup_bridge(tmp_path)
    try:
        set_node_setting(
            data_dir, "backup_passphrase_file", str(tmp_path / "gone.txt")
        )
        result = bridge.verify_latest_auto_backup()
    finally:
        bridge.stop()
    assert result["backup"] == target.name
    assert result["encrypted"] is False
    assert result["valid"] is True


def test_verify_latest_auto_backup_retries_after_prune(tmp_path, monkeypatch):
    # The scheduled worker prunes the selected archive between selection
    # and open (retention 1): verification retries against the current
    # newest instead of reporting a missing file.
    import tau.webapi as webapi

    bridge, data_dir, first = _start_auto_backup_bridge(tmp_path)
    set_node_setting(data_dir, "backup_keep", 1)
    scheduler = AutoBackupScheduler(TauConfig.resolve(data_dir=data_dir))
    real_verify = webapi.verify_backup
    calls: list = []
    created: list = []

    def _pruning_verify(source, *, passphrase=None):
        calls.append(Path(source).name)
        if len(calls) == 1:
            # Scheduled run creates a newer archive and, with retention 1,
            # prunes the archive verification just selected.
            second = scheduler.run(now=1764028800.0 + 7200)
            assert second is not None and second != first
            created.append(second)
        return real_verify(Path(source), passphrase=passphrase)

    try:
        monkeypatch.setattr(webapi, "verify_backup", _pruning_verify)
        result = bridge.verify_latest_auto_backup()
    finally:
        bridge.stop()
    assert not first.exists()
    assert len(created) == 1 and created[0] is not None
    second = created[0]
    assert second != first
    assert result["backup"] == second.name
    assert result["valid"] is True
    assert calls == [first.name, second.name]


def test_verify_latest_auto_backup_rejects_corrupt_archive(tmp_path):
    bridge, _data_dir, target = _start_auto_backup_bridge(tmp_path)
    try:
        with pytest.raises(BackupError):
            target.write_bytes(target.read_bytes()[:100])
            bridge.verify_latest_auto_backup()
    finally:
        bridge.stop()


def test_verify_latest_auto_backup_without_backup(tmp_path):
    data_dir = tmp_path / "node"
    assert (
        cli.main(["init", "--data-dir", str(data_dir), "--name", "alice", "--json"])
        == 0
    )
    node = FakeNode(tmp_path / "mailbox.json")
    node.config = TauConfig.resolve(data_dir=data_dir)
    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    bridge = WebBridge(node, host="127.0.0.1", port=0, web_dir=web_dir)
    bridge.start()
    try:
        with pytest.raises(WebBridgeError, match="no automatic backup yet"):
            bridge.verify_latest_auto_backup()
    finally:
        bridge.stop()


def test_backup_verify_route(tmp_path):
    bridge, _data_dir, target = _start_auto_backup_bridge(tmp_path)
    try:
        status, _, payload = _json(
            bridge, "/api/backup/verify", method="POST", body={}
        )
    finally:
        bridge.stop()
    assert status == 200
    assert payload["backup"] == target.name
    assert payload["node"] == "alice"
    assert payload["encrypted"] is False
    assert payload["valid"] is True


def test_backup_verify_route_without_backup_is_unavailable(bridge):
    status, _, payload = _json(
        bridge, "/api/backup/verify", method="POST", body={}
    )
    assert status == 503
    assert "no data directory" in payload["error"]["message"]


def test_backup_verify_route_rejects_remote_client(tmp_path, monkeypatch):
    import tau.webapi as webapi

    bridge, _data_dir, _target = _start_auto_backup_bridge(tmp_path)
    try:
        monkeypatch.setattr(webapi, "_is_loopback_client", lambda client: False)
        status, _, payload = _json(
            bridge, "/api/backup/verify", method="POST", body={}
        )
    finally:
        bridge.stop()
    assert status == 403
    assert "only available from localhost" in payload["error"]["message"]


def test_live_backup_never_splits_seen_marker_from_inbox(tmp_path, capsys):
    # A delivery marks a message seen (mailbox) and then records it
    # (inbox).  A backup read between those two writes would restore a
    # seen marker with no inbox entry, and the message would be lost:
    # redelivery is rejected as already seen.  Both sides hold the
    # mailbox transaction — the delivery across both steps, the backup
    # across the build — so every archive holds either both writes or
    # neither.  The append is slowed on purpose: without the
    # coordination the backup would reliably land inside the widened
    # window.  The simulated delivery below follows the same protocol
    # the node's receive paths do.
    data_dir = tmp_path / "node"
    assert (
        cli.main(["init", "--data-dir", str(data_dir), "--name", "alice", "--json"])
        == 0
    )
    capsys.readouterr()
    # The node's mailbox and inbox must live in the data directory:
    # create_backup archives the config's paths, not the objects.
    node = FakeNode(data_dir / "mailbox.json")
    node.config = TauConfig.resolve(data_dir=data_dir)
    node.inbox = Inbox(data_dir / "inbox.json")
    bridge = WebBridge(node, host="127.0.0.1", port=0, web_dir=tmp_path)
    original_append = node.inbox.append

    def slow_append(record):
        time.sleep(0.02)
        return original_append(record)

    node.inbox.append = slow_append  # type: ignore[method-assign]
    # Seed one delivered message first, so mailbox.json and inbox.json
    # exist before the first backup: without them the archive has no
    # seen/inbox pair to check at all.
    with node.mailbox.transaction():
        node.mailbox.mark_seen("msg-seed")
        node.inbox.append(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "seed",
                "sent_at": 1000.0,
                "received_at": 1000.0,
                "id": "msg-seed",
                "direction": "in",
            }
        )
    stop = threading.Event()

    def deliver():
        n = 0
        while not stop.is_set():
            message_id = f"msg-{n:04d}"
            n += 1
            with node.mailbox.transaction():
                node.mailbox.mark_seen(message_id)
                node.inbox.append(
                    {
                        "from": "alice",
                        "destination": PEER_HASH,
                        "text": f"hello {n}",
                        "sent_at": 1000.0 + n,
                        "received_at": 1000.0 + n,
                        "id": message_id,
                        "direction": "in",
                    }
                )

    worker = threading.Thread(target=deliver, daemon=True)
    worker.start()
    try:
        for i in range(8):
            result = bridge.backup()
            archive_path = Path(result["path"])
            try:
                restored_dir = tmp_path / f"restored-{i}"
                restore_backup(
                    TauConfig.resolve(data_dir=restored_dir), archive_path
                )
                restored_mailbox = json.loads(
                    (restored_dir / "mailbox.json").read_text(encoding="utf-8")
                )
                restored_inbox = json.loads(
                    (restored_dir / "inbox.json").read_text(encoding="utf-8")
                )
                seen = set(restored_mailbox.get("seen", {}))
                kept = {
                    message["id"]
                    for message in restored_inbox.get("messages", [])
                }
                assert seen <= kept, f"backup lost messages: {sorted(seen - kept)}"
            finally:
                bridge.finish_backup(archive_path)
    finally:
        stop.set()
        worker.join(timeout=5)


def test_backup_reports_busy_when_two_are_in_flight(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert (
        cli.main(["init", "--data-dir", str(data_dir), "--name", "alice", "--json"])
        == 0
    )
    capsys.readouterr()
    node = FakeNode(tmp_path / "mailbox.json")
    node.config = TauConfig.resolve(data_dir=data_dir)
    bridge = WebBridge(node, host="127.0.0.1", port=0, web_dir=tmp_path)
    first = bridge.backup()
    second = bridge.backup()
    try:
        with pytest.raises(WebBridgeError, match="too many backups"):
            bridge.backup()
    finally:
        bridge.finish_backup(Path(first["path"]))
        bridge.finish_backup(Path(second["path"]))
    # A finished backup frees its slot.
    third = bridge.backup()
    bridge.finish_backup(Path(third["path"]))


def test_bridge_stop_removes_pending_backup_archive(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert (
        cli.main(["init", "--data-dir", str(data_dir), "--name", "alice", "--json"])
        == 0
    )
    capsys.readouterr()
    node = FakeNode(tmp_path / "mailbox.json")
    node.config = TauConfig.resolve(data_dir=data_dir)
    bridge = WebBridge(node, host="127.0.0.1", port=0, web_dir=tmp_path)
    result = bridge.backup()
    archive = Path(result["path"])
    assert archive.exists()
    bridge.stop()
    assert not archive.exists()


def test_backup_missing_archive_is_a_clean_error(bridge, monkeypatch):
    monkeypatch.setattr(
        bridge,
        "backup",
        lambda: {
            "path": "/nonexistent/tau-backup.tar.gz",
            "filename": "tau-backup.tar.gz",
            "files": [],
            "size": 0,
        },
    )
    status, _, payload = _json(bridge, "/api/backup")
    assert status == 500
    assert payload["error"]["message"] == "internal error"


def _devices_bridge(tmp_path, capsys):
    """A live bridge whose node has a real data dir (identity + devices.json)."""
    data_dir = tmp_path / "node"
    assert (
        cli.main(["init", "--data-dir", str(data_dir), "--name", "alice", "--json"])
        == 0
    )
    capsys.readouterr()
    node = FakeNode(tmp_path / "mailbox.json")
    node.config = TauConfig.resolve(data_dir=data_dir)
    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    bridge = WebBridge(node, host="127.0.0.1", port=0, web_dir=web_dir)
    bridge.start()
    return data_dir, bridge


def test_devices_lists_this_device_and_offers(tmp_path, capsys):
    _, bridge = _devices_bridge(tmp_path, capsys)
    try:
        status, _, payload = _json(bridge, "/api/devices")
    finally:
        bridge.stop()
    assert status == 200
    assert payload["identity"]
    assert payload["destination"]
    assert payload["paired_from"] is None
    assert payload["issued_offers"] == []


def test_devices_pair_mints_a_verifiable_bundle(tmp_path, capsys):
    _, bridge = _devices_bridge(tmp_path, capsys)
    try:
        status, _, payload = _json(bridge, "/api/devices/pair", method="POST", body={})
        assert status == 200
        assert payload["ok"] is True
        token = payload["token"]
        assert len(token) == 32
        assert payload["expires_at"] > time.time()
        assert payload["download"] == f"/api/devices/bundle/{token}"
        assert payload["filename"].startswith(f"tau-pairing-{token[:8]}-")
        assert payload["filename"].endswith(".json")

        # The offer is listed until it is revoked.
        _, _, listed = _json(bridge, "/api/devices")
        assert [offer["token"] for offer in listed["issued_offers"]] == [token]

        # The bundle downloads once and verifies as a real pairing bundle.
        status, headers, body = _request(bridge, payload["download"])
        assert status == 200
        assert headers["Content-Type"] == "application/json"
        assert headers["Content-Disposition"].endswith(f'"{payload["filename"]}"')
        bundle = json.loads(body)
        identity, cleaned = verify_pairing_bundle(bundle)
        assert cleaned["token"] == token
        assert destination_hash(identity).hex() == listed["destination"]

        # The download consumed the only copy: a second fetch is a 404.
        status, _, missing = _json(bridge, payload["download"])
        assert status == 404
        assert "no pairing bundle" in missing["error"]["message"]
    finally:
        bridge.stop()


def test_devices_bundle_download_missing_file_is_a_clean_404(tmp_path, capsys):
    # A concurrent download (or bridge shutdown) can delete the staged
    # file between the token lookup and the open: the route must report
    # the bundle as gone, not as a 500.
    _, bridge = _devices_bridge(tmp_path, capsys)
    try:
        _, _, minted = _json(bridge, "/api/devices/pair", method="POST", body={})
        staged, _name, _expires = bridge.device_bundle(minted["token"])
        staged.unlink()
        status, _, payload = _json(bridge, minted["download"])
        assert status == 404
        assert "no longer available" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_devices_pair_without_history_carries_no_snapshot(tmp_path, capsys):
    _, bridge = _devices_bridge(tmp_path, capsys)
    try:
        status, _, payload = _json(
            bridge, "/api/devices/pair", method="POST", body={"history": False}
        )
        assert status == 200
        assert payload["history_messages"] == 0
        _, _, body = _request(bridge, payload["download"])
        bundle = json.loads(body)
        _, cleaned = verify_pairing_bundle(bundle)
        assert cleaned["history"] is None
    finally:
        bridge.stop()


def test_devices_pair_rejects_bad_fields(tmp_path, capsys):
    _, bridge = _devices_bridge(tmp_path, capsys)
    try:
        status, _, payload = _json(
            bridge, "/api/devices/pair", method="POST", body={"history": "yes"}
        )
        assert status == 400
        assert "history" in payload["error"]["message"]
        status, _, payload = _json(
            bridge, "/api/devices/pair", method="POST", body={"ttl_seconds": "soon"}
        )
        assert status == 400
        assert "ttl_seconds" in payload["error"]["message"]
        status, _, payload = _json(
            bridge, "/api/devices/pair", method="POST", body={"ttl_seconds": 0}
        )
        assert status == 400
        assert "TTL" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_devices_revoke_cancels_the_offer(tmp_path, capsys):
    _, bridge = _devices_bridge(tmp_path, capsys)
    try:
        _, _, minted = _json(bridge, "/api/devices/pair", method="POST", body={})
        token = minted["token"]
        status, _, payload = _json(
            bridge, "/api/devices/revoke", method="POST", body={"token": token[:8]}
        )
        assert status == 200
        assert payload["ok"] is True
        assert payload["revoked"]["token"] == token
        _, _, listed = _json(bridge, "/api/devices")
        assert listed["issued_offers"] == []
        # Revoking twice fails: the offer is already gone.
        status, _, payload = _json(
            bridge, "/api/devices/revoke", method="POST", body={"token": token}
        )
        assert status == 400
        # A missing token field is a 400, not a revoke of nothing.
        status, _, payload = _json(
            bridge, "/api/devices/revoke", method="POST", body={}
        )
        assert status == 400
        assert "token" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_devices_endpoints_reject_remote_clients(tmp_path, capsys, monkeypatch):
    import tau.webapi as webapi

    _, bridge = _devices_bridge(tmp_path, capsys)
    monkeypatch.setattr(webapi, "_is_loopback_client", lambda client: False)
    try:
        status, _, payload = _json(bridge, "/api/devices")
        assert status == 403
        status, _, payload = _json(
            bridge, "/api/devices/pair", method="POST", body={}
        )
        assert status == 403
        status, _, payload = _json(
            bridge, "/api/devices/revoke", method="POST", body={"token": "ab" * 16}
        )
        assert status == 403
        status, _, payload = _json(bridge, "/api/devices/bundle/" + "ab" * 16)
        assert status == 403
    finally:
        bridge.stop()


def test_devices_without_a_data_directory_is_unavailable(bridge):
    status, _, payload = _json(bridge, "/api/devices")
    assert status == 503
    assert "no data directory" in payload["error"]["message"]
    status, _, payload = _json(bridge, "/api/devices/pair", method="POST", body={})
    assert status == 503


def test_bridge_stop_removes_unclaimed_pairing_bundle(tmp_path, capsys):
    _, bridge = _devices_bridge(tmp_path, capsys)
    result = bridge.pair_device()
    staged, _name, _expires = bridge.device_bundle(result["token"])
    assert staged.exists()
    bridge.stop()
    assert not staged.exists()


def test_devices_revoke_deletes_the_staged_bundle(tmp_path, capsys):
    _, bridge = _devices_bridge(tmp_path, capsys)
    try:
        _, _, minted = _json(bridge, "/api/devices/pair", method="POST", body={})
        staged, _name, _expires = bridge.device_bundle(minted["token"])
        assert staged.exists()
        status, _, _ = _json(
            bridge, "/api/devices/revoke", method="POST", body={"token": minted["token"]}
        )
        assert status == 200
        assert not staged.exists()
        # The bundle is gone with the offer: no download survives revoke.
        status, _, payload = _json(bridge, minted["download"])
        assert status == 404
    finally:
        bridge.stop()


def test_devices_claim_is_single_use_and_head_does_not_consume(tmp_path, capsys):
    _, bridge = _devices_bridge(tmp_path, capsys)
    try:
        _, _, minted = _json(bridge, "/api/devices/pair", method="POST", body={})
        # A HEAD peeks without consuming: the bundle still downloads after.
        status, headers, _ = _request(bridge, minted["download"], method="HEAD")
        assert status == 200
        assert headers["Content-Disposition"].endswith(f'"{minted["filename"]}"')
        # The first claim wins; a second claim is a missing bundle.
        staged, _name = bridge.claim_device_bundle(minted["token"])
        assert staged.exists()
        with pytest.raises(WebBridgeError, match="no pairing bundle"):
            bridge.claim_device_bundle(minted["token"])
    finally:
        bridge.stop()


def test_devices_claimed_bundle_cleaned_up_on_stop(tmp_path, capsys):
    # A claimed bundle leaves the staged map but stays tracked in
    # flight, so bridge shutdown still deletes the private-identity
    # file instead of stranding it in the temp directory.
    _, bridge = _devices_bridge(tmp_path, capsys)
    try:
        _, _, minted = _json(bridge, "/api/devices/pair", method="POST", body={})
        staged, _name = bridge.claim_device_bundle(minted["token"])
        assert staged.exists()
        assert staged in bridge._device_bundles_in_flight
    finally:
        bridge.stop()
    assert not staged.exists()
    assert not bridge._device_bundles_in_flight


def test_devices_finish_claimed_drops_inflight_and_deletes(tmp_path, capsys):
    _, bridge = _devices_bridge(tmp_path, capsys)
    try:
        result = bridge.pair_device()
        staged, _name = bridge.claim_device_bundle(result["token"])
        assert staged in bridge._device_bundles_in_flight
        bridge.finish_claimed_device_bundle(staged)
        assert staged not in bridge._device_bundles_in_flight
        assert not staged.exists()
    finally:
        bridge.stop()


def test_devices_prune_deletes_expired_staged_bundles(tmp_path, capsys):
    _, bridge = _devices_bridge(tmp_path, capsys)
    try:
        result = bridge.pair_device()
        token = result["token"]
        staged, name, _expires = bridge.device_bundle(token)
        assert staged.exists()
        # Age the staged entry past expiry, then mint again: the
        # prune on mint must delete the stale file.
        bridge._device_bundles[token] = (staged, name, time.time() - 1)
        bridge.pair_device()
        assert token not in bridge._device_bundles
        assert not staged.exists()
    finally:
        bridge.stop()


def test_static_console_and_assets(bridge):
    status, headers, body = _request(bridge, "/")
    assert status == 200
    assert headers["Content-Type"].startswith("text/html")
    assert b"tau console" in body

    status, headers, body = _request(bridge, "/assets/app.js")
    assert status == 200
    assert headers["Content-Type"].startswith("text/javascript")
    assert headers["Cache-Control"].startswith("public")


def test_static_traversal_is_blocked(bridge):
    assert bridge.resolve_static("/../../etc/passwd") is None
    status, _, _ = _json(bridge, "/%2e%2e/%2e%2e/etc/passwd")
    assert status == 404


def test_cross_origin_post_is_rejected(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "csrf"},
        headers={"Origin": "https://evil.example"},
    )
    assert status == 403
    assert node.sent == []


def test_post_requires_json_content_type(bridge):
    with pytest.raises(urllib.error.HTTPError) as excinfo:
        _request(
            bridge,
            "/api/announce",
            method="POST",
            raw=b"{}",
            headers={"Content-Type": "text/plain"},
        )
    assert excinfo.value.code == 400
    assert b"application/json" in excinfo.value.read()


def test_cors_allows_local_dev_origin(bridge):
    status, headers, _ = _request(
        bridge, "/api/status", headers={"Origin": "http://localhost:3000"}
    )
    assert status == 200
    assert headers["Access-Control-Allow-Origin"] == "http://localhost:3000"


def test_cors_ignores_remote_origin(bridge):
    status, headers, _ = _request(
        bridge, "/api/status", headers={"Origin": "https://evil.example"}
    )
    assert status == 200
    assert "Access-Control-Allow-Origin" not in headers


def test_options_preflight(bridge):
    status, headers, _ = _request(
        bridge,
        "/api/messages",
        method="OPTIONS",
        headers={"Origin": "http://127.0.0.1:3000"},
    )
    assert status == 204
    assert "POST" in headers["Access-Control-Allow-Methods"]


def test_events_endpoint_streams_a_snapshot(bridge):
    connection = http.client.HTTPConnection("127.0.0.1", bridge.port, timeout=5)
    try:
        connection.request("GET", "/api/events")
        response = connection.getresponse()
        assert response.status == 200
        assert response.getheader("Content-Type").startswith("text/event-stream")
        chunk = b""
        deadline = time.time() + 5
        while b"\n\n" not in chunk and time.time() < deadline:
            part = response.read1(4096)
            if not part:
                break
            chunk += part
        assert b"event: snapshot" in chunk
        assert b'"name":"tester"' in chunk
    finally:
        connection.close()


def test_ws_accept_key_matches_rfc_vector():
    assert (
        _ws_accept_key("dGhlIHNhbXBsZSBub25jZQ==")
        == "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    )


def test_ws_encode_frame_lengths():
    assert _ws_encode_frame(b"hi") == b"\x81\x02hi"
    assert _ws_encode_frame(b"x" * 200)[:4] == b"\x81\x7e\x00\xc8"
    assert _ws_encode_frame(b"x" * 70000)[:10] == (
        b"\x81\x7f\x00\x00\x00\x00\x00\x01\x11\x70"
    )


def _ws_client_frame(payload, *, opcode=1, fin=True, mask=b"\x01\x02\x03\x04"):
    header = bytearray([(0x80 if fin else 0x00) | opcode, 0x80])
    length = len(payload)
    if length < 126:
        header[1] |= length
    elif length < 65536:
        header[1] |= 126
        header.extend(length.to_bytes(2, "big"))
    else:
        header[1] |= 127
        header.extend(length.to_bytes(8, "big"))
    masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    return bytes(header) + mask + masked


def test_ws_reader_reassembles_fragments_and_answers_ping():
    sent = []
    frames = _ws_client_frame(b"are-you-there", opcode=9)
    frames += _ws_client_frame(b"hello ", opcode=1, fin=False)
    frames += _ws_client_frame(b"world", opcode=0, fin=True)
    reader = _WebSocketReader(
        io.BytesIO(frames),
        send_frame=lambda payload, *, opcode: sent.append((opcode, payload)),
    )
    assert reader.read_message() == b"hello world"
    assert sent == [(10, b"are-you-there")]


def test_ws_reader_rejects_unmasked_frames():
    reader = _WebSocketReader(
        io.BytesIO(b"\x81\x02hi"),
        send_frame=lambda *args, **kwargs: None,
    )
    with pytest.raises(_WebSocketFrameError):
        reader.read_message()


def test_ws_reader_close_frame_returns_none():
    reader = _WebSocketReader(
        io.BytesIO(_ws_client_frame(b"", opcode=8)),
        send_frame=lambda *args, **kwargs: None,
    )
    assert reader.read_message() is None


def test_ws_reader_rejects_message_over_cap():
    half = b"x" * (_WS_MAX_FRAME_BYTES // 2 + 1)
    frames = _ws_client_frame(half, opcode=1, fin=False)
    frames += _ws_client_frame(half, opcode=0, fin=True)
    reader = _WebSocketReader(
        io.BytesIO(frames),
        send_frame=lambda *args, **kwargs: None,
    )
    with pytest.raises(_WebSocketFrameError):
        reader.read_message()


def test_ws_reader_rejects_bad_control_frames():
    oversized_ping = _ws_client_frame(b"x" * 126, opcode=9)
    reader = _WebSocketReader(
        io.BytesIO(oversized_ping),
        send_frame=lambda *args, **kwargs: None,
    )
    with pytest.raises(_WebSocketFrameError):
        reader.read_message()
    fragmented_ping = _ws_client_frame(b"p", opcode=9, fin=False)
    reader = _WebSocketReader(
        io.BytesIO(fragmented_ping),
        send_frame=lambda *args, **kwargs: None,
    )
    with pytest.raises(_WebSocketFrameError):
        reader.read_message()


class _WSBuffer:
    """Buffered reads over a socket: the handshake and the first frame
    can arrive in one segment, so bytes past the headers must survive."""

    def __init__(self, sock):
        self._sock = sock
        self._buffer = b""

    def read_exact(self, count):
        while len(self._buffer) < count:
            chunk = self._sock.recv(count - len(self._buffer))
            if not chunk:
                raise ConnectionResetError("websocket closed")
            self._buffer += chunk
        data, self._buffer = self._buffer[:count], self._buffer[count:]
        return data

    def read_until(self, marker):
        while marker not in self._buffer:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise ConnectionResetError("websocket closed")
            self._buffer += chunk
        data, self._buffer = self._buffer.split(marker, 1)
        return data + marker


def _ws_read_frame(buffer):
    first, second = buffer.read_exact(2)
    opcode = first & 0x0F
    length = second & 0x7F
    if length == 126:
        length = int.from_bytes(buffer.read_exact(2), "big")
    elif length == 127:
        length = int.from_bytes(buffer.read_exact(8), "big")
    assert not second & 0x80, "server frames must not be masked"
    return opcode, buffer.read_exact(length)


def test_websocket_requires_upgrade(bridge):
    status, _, payload = _json(bridge, "/ws")
    assert status == 400
    assert payload["error"]["message"] == "websocket upgrade required"


def _ws_raw_request(bridge, *, key="dGhlIHNhbXBsZSBub25jZQ==", extra=""):
    sock = socket.create_connection(("127.0.0.1", bridge.port), timeout=5)
    sock.sendall(
        (
            "GET /ws HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            f"{extra}\r\n"
        ).encode("latin-1")
    )
    return sock, _WSBuffer(sock)


def test_websocket_rejects_malformed_keys(bridge):
    bad_keys = (
        "not-base64!!",
        "café",
        base64.b64encode(b"short").decode("ascii"),
    )
    for bad_key in bad_keys:
        before = bridge.bus.subscriber_count()
        sock, buffer = _ws_raw_request(bridge, key=bad_key)
        try:
            headers = buffer.read_until(b"\r\n\r\n")
            assert headers.startswith(b"HTTP/1.1 400")
            assert bridge.bus.subscriber_count() == before
        finally:
            sock.close()


def test_websocket_rejects_foreign_origin(bridge):
    before = bridge.bus.subscriber_count()
    sock, buffer = _ws_raw_request(
        bridge, extra="Origin: https://evil.example\r\n"
    )
    try:
        headers = buffer.read_until(b"\r\n\r\n")
        assert headers.startswith(b"HTTP/1.1 403")
        assert bridge.bus.subscriber_count() == before
    finally:
        sock.close()


def test_websocket_survives_idle_client(bridge):
    # A client that sends nothing for longer than the read timeout
    # must stay connected: its next event and ping still arrive.
    sock, buffer = _ws_raw_request(bridge)
    try:
        headers = buffer.read_until(b"\r\n\r\n")
        assert headers.startswith(b"HTTP/1.1 101")
        opcode, payload = _ws_read_frame(buffer)
        assert opcode == 1
        assert json.loads(payload)["type"] == "snapshot"
        time.sleep(6)
        bridge.bus.publish({"type": "status", "node": {"name": "tester"}})
        opcode, payload = _ws_read_frame(buffer)
        assert opcode == 1
        assert json.loads(payload)["type"] == "status"
        sock.sendall(_ws_client_frame(b"ping-me", opcode=9))
        opcode, payload = _ws_read_frame(buffer)
        assert opcode == 10
        assert payload == b"ping-me"
        sock.sendall(_ws_client_frame(b"", opcode=8))
        opcode, payload = _ws_read_frame(buffer)
        assert opcode == 8
    finally:
        sock.close()


def test_websocket_slow_client_does_not_outlive_bridge_stop(bridge):
    # A client that stops reading must not keep the bridge alive: once
    # its TCP buffers fill, the sender blocks in wfile.flush(), and
    # bridge.stop() must still return promptly with the connection
    # closed (see #425).
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # A small receive buffer caps the bytes in flight, so the sender
    # blocks after a modest flood instead of megabytes of buffering.
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    sock.settimeout(5)
    sock.connect(("127.0.0.1", bridge.port))
    sock.sendall(
        (
            "GET /ws HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode("latin-1")
    )
    buffer = _WSBuffer(sock)
    try:
        headers = buffer.read_until(b"\r\n\r\n")
        assert headers.startswith(b"HTTP/1.1 101")
        opcode, payload = _ws_read_frame(buffer)
        assert opcode == 1
        assert json.loads(payload)["type"] == "snapshot"
        # Flood the stream and never read again: the sender drains the
        # subscriber queue into the socket until the kernel send buffer
        # fills, then blocks in wfile.flush(). The flood must attempt
        # far more than the send buffer can absorb (several MiB) plus
        # the subscriber queue, so the sender is parked in a write
        # rather than merely behind.
        flooding = threading.Event()
        published = [0]

        def flood() -> None:
            while not flooding.is_set():
                bridge.bus.publish(
                    {"type": "status", "node": {"name": "x" * 1024}}
                )
                published[0] += 1

        flooder = threading.Thread(target=flood, daemon=True)
        flooder.start()
        try:
            deadline = time.monotonic() + 30
            while published[0] < 65536 and time.monotonic() < deadline:
                time.sleep(0.05)
            assert published[0] >= 65536, "flooder did not publish enough"
            # Poll for the blocked state while the flood still runs:
            # the flooder refills the queue as fast as the pump
            # drains it, so once the sender parks in a write the
            # queue sits at (near) capacity.  A single read after a
            # fixed sleep is marginal under CI load — the queue drains
            # a few messages into kernel buffers in the gap between
            # the flooder stopping and the read (250/256 on 3.10, and
            # 244/256 before that) — while a sender that kept up
            # would leave the queue empty, so the poll still fails
            # the test instead of masking a real regression.
            deadline = time.monotonic() + 10
            while True:
                subscribers = list(bridge.bus._subscribers)
                assert len(subscribers) == 1
                if subscribers[0].qsize() >= SUBSCRIBER_QUEUE_SIZE - 1:
                    break
                assert time.monotonic() < deadline, (
                    "sender was not blocked on the slow client"
                )
                time.sleep(0.05)
            flooding.set()
            flooder.join(timeout=5)
        finally:
            flooding.set()
            flooder.join(timeout=5)
        stopped = []
        stopper = threading.Thread(
            target=lambda: (bridge.stop(), stopped.append(True)),
            daemon=True,
        )
        stopper.start()
        stopper.join(timeout=10)
        assert not stopper.is_alive(), "bridge.stop() hung on a slow client"
        assert stopped == [True]
        # The sender was parked in wfile.flush(): on the fixed code the
        # shutdown fails that write and the pump thread exits. Draining
        # the client socket cannot be the check — reading frees window
        # and unblocks the sender on its own, so the pump would drain
        # its queue, notice bridge.closing, and exit even unfixed.
        deadline = time.monotonic() + 5
        while any(
            thread.name == "tau-ws-pump" and thread.is_alive()
            for thread in threading.enumerate()
        ):
            assert time.monotonic() < deadline, (
                "ws pump thread outlived bridge.stop()"
            )
            time.sleep(0.05)
        # With the pump gone the connection is closed: drain whatever
        # the client already buffered, then expect EOF.
        sock.settimeout(5)
        deadline = time.monotonic() + 5
        while True:
            try:
                chunk = sock.recv(65536)
            except socket.timeout:
                pytest.fail("server left the slow client's connection open")
            if not chunk:
                break
            assert time.monotonic() < deadline
    finally:
        sock.close()


def test_websocket_streams_snapshot_events_and_ping(bridge):
    sock = socket.create_connection(("127.0.0.1", bridge.port), timeout=5)
    try:
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        sock.sendall(
            (
                "GET /ws HTTP/1.1\r\n"
                "Host: 127.0.0.1\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Key: {key}\r\n"
                "Sec-WebSocket-Version: 13\r\n"
                "Origin: http://127.0.0.1\r\n\r\n"
            ).encode("ascii")
        )
        buffer = _WSBuffer(sock)
        headers = buffer.read_until(b"\r\n\r\n")
        assert headers.startswith(b"HTTP/1.1 101")
        assert _ws_accept_key(key).encode("ascii") in headers
        opcode, payload = _ws_read_frame(buffer)
        assert opcode == 1
        snapshot = json.loads(payload)
        assert snapshot["type"] == "snapshot"
        assert snapshot["snapshot"]["node"]["name"] == "tester"
        bridge.bus.publish({"type": "status", "node": {"name": "tester"}})
        opcode, payload = _ws_read_frame(buffer)
        assert opcode == 1
        assert json.loads(payload)["type"] == "status"
        sock.sendall(_ws_client_frame(b"ping-me", opcode=9))
        opcode, payload = _ws_read_frame(buffer)
        assert opcode == 10
        assert payload == b"ping-me"
        sock.sendall(_ws_client_frame(b"", opcode=8))
        opcode, payload = _ws_read_frame(buffer)
        assert opcode == 8
    finally:
        sock.close()


def test_event_replay_and_order(bridge):
    bridge.record_inbound(
        {
            "from": "bob",
            "destination": "ef" * 16,
            "text": "one",
            "sent_at": 1.0,
            "received_at": 2.0,
        }
    )
    events = bridge.bus.replay(0)
    assert events[-1]["type"] == "message"
    assert events[-1]["message"]["text"] == "one"
    first_id = events[0]["id"]
    assert [e["id"] for e in bridge.bus.replay(first_id)] == [
        e["id"] for e in events[1:]
    ]


def test_non_loopback_bind_requires_opt_in(node):
    with pytest.raises(WebBridgeError):
        WebBridge(node, host="0.0.0.0", port=0)


def test_unknown_api_route_returns_404(bridge):
    status, _, payload = _json(bridge, "/api/nope")
    assert status == 404
    assert payload["error"]["message"] == "not found"


def test_worker_threads_can_send_concurrently(bridge):
    def send(index):
        bridge.send_message("alice", f"m{index}")

    threads = [threading.Thread(target=send, args=(i,)) for i in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    _, _, feed = _json(bridge, "/api/messages")
    assert len(feed["messages"]) == 5
    assert len({m["id"] for m in feed["messages"]}) == 5


def test_tan_timeline_and_follow(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "hello from tan", name="bob", now=1000)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["hello from tan"]

        # A located post carries its place through the API, so the
        # console post card can render it with an open-in-maps link.
        conn = init_db(node.config.social_db_path)
        try:
            located = create_post(
                conn,
                identity,
                "from the park",
                name="bob",
                now=1001,
                location={"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"},
            )
        finally:
            conn.close()
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        by_hash = {item["hash"]: item for item in payload["posts"]}
        assert by_hash[located["hash"]]["location"] == {
            "lat": 34.05,
            "lon": -83.88,
            "name": "Rock Springs Park",
        }

        status, _, payload = _json(
            bridge,
            "/api/tan/follow",
            method="POST",
            body={"target": post["author"]},
        )
        assert status == 200
        assert payload["follow"]["author"] == post["author"]

        status, _, payload = _json(bridge, "/api/tan/following")
        assert status == 200
        assert payload["count"] == 1
        # The author now has two posts: the plain one and the located one.
        assert payload["following"][0]["posts"] == 2

        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={"text": "nope"},
        )
        assert status == 503
        assert "not enabled" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_followers_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.follows import FollowListStore, build_follow_list
    from tau.identity import destination_hash, save_identity
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    bob = RNS.Identity()
    save_identity(identity, node.config.identity_path)
    own_author = destination_hash(identity).hex()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, bob, "hello from bob", name="bob", now=1000)
    finally:
        conn.close()
    doc = build_follow_list(bob, authors=[own_author], version=1)
    store = FollowListStore(node.config.follow_lists_path)
    assert store.store(doc) is True
    assert store.save() is True

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/followers")
        assert status == 200
        assert payload["count"] == 1
        record = payload["followers"][0]
        assert record["author"] == destination_hash(bob).hex()
        assert record["name"] == "bob"
        assert record["posts"] == 1
        assert record["followed"] is False
    finally:
        bridge.stop()


def test_channels_via_api(tmp_path):
    from types import SimpleNamespace

    import RNS

    from tau import cli
    from tau.channels import (
        create_message,
        init_db as init_channel_db,
        join_channel,
    )
    from tau.config import TauConfig

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bob = RNS.Identity()
    tester = RNS.Identity()
    conn = init_channel_db(node.config.channels_db_path)
    try:
        join_channel(conn, "general")
        bob_message = create_message(
            conn, bob, "general", "hello channels", name="bob", now=1000
        )
        create_message(
            conn, tester, "general", "from tester", name="tester", now=1001
        )
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/channels")
        assert status == 200
        assert [item["channel"] for item in payload["channels"]] == ["general"]
        assert payload["channels"][0]["messages"] == 2

        status, _, payload = _json(bridge, "/api/channels/messages?channel=general")
        assert status == 200
        assert [item["text"] for item in payload["messages"]] == [
            "from tester",
            "hello channels",
        ]

        status, _, payload = _json(bridge, "/api/channels/messages")
        assert status == 400

        status, _, payload = _json(bridge, "/api/channels/search?channel=general&q=hello")
        assert status == 200
        assert payload["query"] == "hello"
        assert [item["text"] for item in payload["messages"]] == ["hello channels"]

        status, _, payload = _json(bridge, "/api/channels/search?channel=general")
        assert status == 400
        status, _, payload = _json(bridge, "/api/channels/search?q=hello")
        assert status == 400

        # A blocked author is hidden, like the timeline hides their posts.
        node.blocklist = SimpleNamespace(
            blocked=frozenset({bob_message["author"]}), block_all=False
        )
        status, _, payload = _json(bridge, "/api/channels/messages?channel=general")
        assert status == 200
        assert [item["text"] for item in payload["messages"]] == ["from tester"]
        status, _, payload = _json(bridge, "/api/channels/search?channel=general&q=hello")
        assert status == 200
        assert payload["messages"] == []
        node.blocklist = SimpleNamespace(blocked=frozenset(), block_all=True)
        status, _, payload = _json(bridge, "/api/channels/messages?channel=general")
        assert status == 200
        assert payload["messages"] == []
        status, _, payload = _json(bridge, "/api/channels/search?channel=general&q=tester")
        assert status == 200
        assert payload["messages"] == []
        node.blocklist = None

        status, _, payload = _json(
            bridge, "/api/channels/join", method="POST", body={"channel": "other"}
        )
        assert status == 200
        assert payload["channel"] == "other"
        status, _, payload = _json(
            bridge, "/api/channels/leave", method="POST", body={"channel": "other"}
        )
        assert status == 200
        assert payload["left"] is True

        # Sending and syncing need channels enabled on the serving node.
        status, _, payload = _json(
            bridge,
            "/api/channels/send",
            method="POST",
            body={"channel": "general", "text": "nope"},
        )
        assert status == 503
        assert "not enabled" in payload["error"]["message"]

        # Editing validates its fields before touching the bridge...
        status, _, payload = _json(
            bridge,
            "/api/channels/edit",
            method="POST",
            body={"channel": "general", "text": "nope"},
        )
        assert status == 400
        status, _, payload = _json(
            bridge,
            "/api/channels/edit",
            method="POST",
            body={"channel": "general", "target": bob_message["hash"]},
        )
        assert status == 400
        # ...and reports the same 503 when channels are not enabled.
        status, _, payload = _json(
            bridge,
            "/api/channels/edit",
            method="POST",
            body={
                "channel": "general",
                "target": bob_message["hash"],
                "text": "nope",
            },
        )
        assert status == 503
        assert "not enabled" in payload["error"]["message"]

        status, _, payload = _json(
            bridge, "/api/channels/sync", method="POST", body={}
        )
        assert status == 503

        # Pins: pinning, listing, blocked-author hiding, unpinning.
        status, _, payload = _json(
            bridge,
            "/api/channels/pin",
            method="POST",
            body={"channel": "general", "hash": bob_message["hash"]},
        )
        assert status == 200
        assert payload["channel"] == "general"
        assert payload["hash"] == bob_message["hash"]
        status, _, payload = _json(
            bridge, "/api/channels/pins?channel=general"
        )
        assert status == 200
        assert [item["text"] for item in payload["pins"]] == ["hello channels"]
        assert payload["pins"][0]["pinned_at"] is not None
        # Pinning twice, and pinning without a hash, are client errors.
        status, _, payload = _json(
            bridge,
            "/api/channels/pin",
            method="POST",
            body={"channel": "general", "hash": bob_message["hash"]},
        )
        assert status == 400
        status, _, payload = _json(
            bridge,
            "/api/channels/pin",
            method="POST",
            body={"channel": "general"},
        )
        assert status == 400
        status, _, payload = _json(bridge, "/api/channels/pins")
        assert status == 400
        # A blocked author is hidden from pins, like from history.
        node.blocklist = SimpleNamespace(
            blocked=frozenset({bob_message["author"]}), block_all=False
        )
        status, _, payload = _json(
            bridge, "/api/channels/pins?channel=general"
        )
        assert status == 200
        assert payload["pins"] == []
        node.blocklist = None
        status, _, payload = _json(
            bridge,
            "/api/channels/unpin",
            method="POST",
            body={"channel": "general", "hash": bob_message["hash"]},
        )
        assert status == 200
        assert payload["unpinned"] is True
        status, _, payload = _json(
            bridge,
            "/api/channels/unpin",
            method="POST",
            body={"channel": "general", "hash": bob_message["hash"]},
        )
        assert status == 200
        assert payload["unpinned"] is False
        status, _, payload = _json(
            bridge, "/api/channels/pins?channel=general"
        )
        assert status == 200
        assert payload["pins"] == []
    finally:
        bridge.stop()


def test_channels_send_reply_via_api(tmp_path):
    import RNS

    from tau import cli
    from tau.channels import (
        ChannelApp,
        create_message,
        init_db as init_channel_db,
        join_channel,
    )
    from tau.config import TauConfig

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bob = RNS.Identity()
    conn = init_channel_db(node.config.channels_db_path)
    try:
        join_channel(conn, "general")
        parent = create_message(
            conn, bob, "general", "meet at the park", name="bob", now=1000
        )
    finally:
        conn.close()
    node.channel_app = ChannelApp(node.config, node.identity, name="tester")

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/channels/send",
            method="POST",
            body={
                "channel": "general",
                "text": "see you there",
                "reply_to": parent["hash"],
            },
        )
        assert status == 200
        assert payload["message"]["reply_to"] == parent["hash"]

        # An unknown parent is a 400, not a dangling reply.
        status, _, payload = _json(
            bridge,
            "/api/channels/send",
            method="POST",
            body={"channel": "general", "text": "dangling", "reply_to": "f" * 64},
        )
        assert status == 400
        assert "unknown message" in payload["error"]["message"]

        # A non-string reply_to is a 400 too.
        status, _, payload = _json(
            bridge,
            "/api/channels/send",
            method="POST",
            body={"channel": "general", "text": "bad", "reply_to": 123},
        )
        assert status == 400

        # The messages list carries the reply with its parent digest.
        status, _, payload = _json(
            bridge, "/api/channels/messages?channel=general"
        )
        assert status == 200
        assert [item["text"] for item in payload["messages"]] == [
            "see you there",
            "meet at the park",
        ]
        assert payload["messages"][0]["reply_to"] == parent["hash"]
        assert "reply_to" not in payload["messages"][1]
    finally:
        bridge.stop()


def test_channel_polls_via_api(tmp_path):
    from tau.channels import (
        ChannelApp,
        create_message,
        init_db as init_channel_db,
        join_channel,
    )
    from tau.config import TauConfig
    from tau.protocol import APP_NAME, ASPECT

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    alice = RNS.Identity()
    bob = RNS.Identity()
    tester = RNS.Identity()
    tester_hash = RNS.Destination.hash(tester, APP_NAME, ASPECT).hex()
    node.destination_hexhash = tester_hash
    conn = init_channel_db(node.config.channels_db_path)
    try:
        join_channel(conn, "general")
    finally:
        conn.close()
    app = ChannelApp(node.config, tester, name="tester")
    node.channel_app = app

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # Posting needs 2-8 options.
        status, _, payload = _json(
            bridge,
            "/api/channels/poll",
            method="POST",
            body={"channel": "general", "question": "q?", "options": ["only"]},
        )
        assert status == 400

        status, _, payload = _json(
            bridge,
            "/api/channels/poll",
            method="POST",
            body={
                "channel": "general",
                "question": "which day?",
                "options": ["Saturday", "Sunday"],
            },
        )
        assert status == 200
        poll = payload["message"]
        assert poll["poll_options"] == ["Saturday", "Sunday"]
        assert poll["author"] == tester_hash

        # A peer's vote, stored directly, counts in the tally — and
        # the vote record itself never shows in the listing.
        conn = init_channel_db(node.config.channels_db_path)
        try:
            create_message(
                conn,
                bob,
                "general",
                "voted: Sunday",
                name="bob",
                now=1001,
                poll_vote={
                    "target": {
                        "author": poll["author"],
                        "sent_at": poll["created_at"],
                    },
                    "option": 1,
                },
            )
        finally:
            conn.close()

        status, _, payload = _json(
            bridge, "/api/channels/messages?channel=general"
        )
        assert status == 200
        assert [item["text"] for item in payload["messages"]] == ["which day?"]
        shown = payload["messages"][0]
        assert shown["poll"]["counts"] == [0, 1]
        assert shown["poll"]["total"] == 1
        assert shown["poll"]["closed"] is False
        assert shown["poll"]["own"] is True

        # Voting through the API moves the tally; a bad option is 400.
        status, _, payload = _json(
            bridge,
            "/api/channels/vote",
            method="POST",
            body={"channel": "general", "poll": poll["hash"][:8], "option": "3"},
        )
        assert status == 400
        status, _, payload = _json(
            bridge,
            "/api/channels/vote",
            method="POST",
            body={"channel": "general", "poll": poll["hash"][:8], "option": 1},
        )
        assert status == 200
        status, _, payload = _json(
            bridge, "/api/channels/messages?channel=general"
        )
        assert payload["messages"][0]["poll"]["counts"] == [1, 1]
        assert payload["messages"][0]["poll"]["my_vote"] == 0

        # Only the poll's creator can close it: a poll from alice
        # refuses the close, then the own poll closes.
        conn = init_channel_db(node.config.channels_db_path)
        try:
            alice_poll = create_message(
                conn,
                alice,
                "general",
                "alice asks?",
                name="alice",
                now=1002,
                poll_options=["Yes", "No"],
            )
        finally:
            conn.close()
        status, _, payload = _json(
            bridge,
            "/api/channels/close-poll",
            method="POST",
            body={"channel": "general", "poll": alice_poll["hash"]},
        )
        assert status == 400
        assert "only the poll's creator" in payload["error"]["message"]

        status, _, payload = _json(
            bridge,
            "/api/channels/close-poll",
            method="POST",
            body={"channel": "general", "poll": poll["hash"]},
        )
        assert status == 200
        status, _, payload = _json(
            bridge, "/api/channels/messages?channel=general"
        )
        assert payload["messages"][0]["poll"]["closed"] is True

        # A closed poll takes no more votes.
        status, _, payload = _json(
            bridge,
            "/api/channels/vote",
            method="POST",
            body={"channel": "general", "poll": poll["hash"], "option": 2},
        )
        assert status == 400
        assert "closed" in payload["error"]["message"]

        # An unknown poll hash is a 400, not a 500.
        status, _, payload = _json(
            bridge,
            "/api/channels/vote",
            method="POST",
            body={"channel": "general", "poll": "f" * 8, "option": 1},
        )
        assert status == 400
    finally:
        bridge.stop()


def test_channel_unread_counts_via_api(tmp_path):
    import time

    import RNS

    from tau import cli
    from tau.channels import (
        create_message,
        init_db as init_channel_db,
        join_channel,
        store_message,
    )
    from tau.config import TauConfig

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bob = RNS.Identity()
    source = init_channel_db(tmp_path / "source.sqlite3")
    conn = init_channel_db(node.config.channels_db_path)
    try:
        join_channel(conn, "general")
        message = create_message(
            source, bob, "general", "hello channels", name="bob", now=1000
        )
        # Received now — after joining — so it counts as unread.
        store_message(conn, message, now=time.time())
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The snapshot carries the channel count, folded into the total.
        _, _, status = _json(bridge, "/api/status")
        assert _counts(status["unread"]) == {
            "per_peer": {},
            "per_group": {},
            "per_channel": {"general": 1},
            "total": 1,
        }

        # The channel list carries the same count for its badge.
        _, _, payload = _json(bridge, "/api/channels")
        assert payload["channels"][0]["unread"] == 1

        # A new channel message re-publishes the unread counts.
        bridge.record_channel_message(
            {"channel": "general", "text": "another", "author": "x" * 32}
        )
        unread_events = [
            event for event in bridge.bus.replay(0) if event["type"] == "unread"
        ]
        assert _counts(unread_events[-1]["unread"]) == {
            "per_peer": {},
            "per_group": {},
            "per_channel": {"general": 1},
            "total": 1,
        }

        # Marking the channel read leaves a DM's badge alone.
        _inbound_record(node)
        status, _, payload = _json(
            bridge, "/api/messages/read", method="POST", body={"channel": "general"}
        )
        assert status == 200
        assert _counts(payload["unread"]) == {
            "per_peer": {PEER_HASH: 1},
            "per_group": {},
            "per_channel": {},
            "total": 1,
        }
        _, _, payload = _json(bridge, "/api/channels")
        assert payload["channels"][0]["unread"] == 0

        # Exactly one target, and it must be a channel you joined.
        status, _, payload = _json(
            bridge,
            "/api/messages/read",
            method="POST",
            body={"channel": "general", "peer": PEER_HASH},
        )
        assert status == 400
        status, _, payload = _json(
            bridge, "/api/messages/read", method="POST", body={"channel": "nope"}
        )
        assert status == 400
        assert "not a member" in payload["error"]["message"]
        status, _, payload = _json(
            bridge, "/api/messages/read", method="POST", body={"channel": 42}
        )
        assert status == 400
        assert "channel" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_channel_mute_suppresses_unread_via_api(tmp_path):
    import time

    import RNS

    from tau import cli
    from tau.channels import (
        create_message,
        init_db as init_channel_db,
        join_channel,
        store_message,
    )
    from tau.config import TauConfig

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bob = RNS.Identity()
    source = init_channel_db(tmp_path / "source.sqlite3")
    conn = init_channel_db(node.config.channels_db_path)
    try:
        join_channel(conn, "general")
        message = create_message(
            source, bob, "general", "hello channels", name="bob", now=1000
        )
        # Received now — after joining — so it counts as unread.
        store_message(conn, message, now=time.time())
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        _, _, status = _json(bridge, "/api/status")
        assert _counts(status["unread"])["per_channel"] == {"general": 1}

        def unread_events():
            return [
                event for event in bridge.bus.replay(0) if event["type"] == "unread"
            ]

        # Muting the channel darkens its badge and drops it from the total.
        status, _, payload = _json(
            bridge,
            "/api/messages/mute",
            method="POST",
            body={"channel": "general"},
        )
        assert status == 200
        assert payload["mute"]["changed"] is True
        assert _counts(payload["mute"]["unread"])["per_channel"] == {}
        assert _counts(payload["mute"]["unread"])["total"] == 0
        _, _, status = _json(bridge, "/api/status")
        assert _counts(status["unread"])["per_channel"] == {}
        _, _, payload = _json(bridge, "/api/channels")
        assert payload["channels"][0]["unread"] == 0
        assert payload["channels"][0]["muted"] is True

        # Unmuting restores the count and names it as restored mail,
        # like a DM or group unmute does.
        status, _, payload = _json(
            bridge,
            "/api/messages/mute",
            method="POST",
            body={"channel": "general", "muted": False},
        )
        assert status == 200
        assert _counts(payload["mute"]["unread"])["per_channel"] == {"general": 1}
        assert unread_events()[-1]["restored"] == [
            {"target": "general", "unread": 1}
        ]
        _, _, payload = _json(bridge, "/api/channels")
        assert payload["channels"][0]["unread"] == 1
        assert payload["channels"][0]["muted"] is False
    finally:
        bridge.stop()


def test_channel_send_with_expiry_via_api(tmp_path):
    from tau.channels import ChannelApp, join_channel

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    # Attach a real channel app so sends work; its node points at the
    # fake so gossip has nowhere to go (delivered 0, no raise).
    app = ChannelApp(node.config, node.identity, name="tester", gossip=False)
    app.node = node
    node.channel_app = app
    app.join("general")

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/channels/send",
            method="POST",
            body={"channel": "general", "text": "ephemeral", "expires_in": 3600},
        )
        assert status == 200
        expires_at = payload["message"]["expires_at"]
        assert expires_at - time.time() == pytest.approx(3600, abs=5)

        # The stored message carries the expiry into history.
        status, _, payload = _json(
            bridge, "/api/channels/messages?channel=general"
        )
        assert status == 200
        assert payload["messages"][0]["expires_at"] == expires_at

        # A bad expires_in is a 400 before the bridge is touched.
        for bad in (0, -5, "1h", True, 31 * 24 * 3600):
            status, _, payload = _json(
                bridge,
                "/api/channels/send",
                method="POST",
                body={"channel": "general", "text": "nope", "expires_in": bad},
            )
            assert status == 400, bad
            assert "error" in payload

        # Without expires_in, a channel default fills the expiry in.
        status, _, payload = _json(
            bridge,
            "/api/messages/disappearing",
            method="POST",
            body={"channel": "general", "seconds": 300},
        )
        assert status == 200
        status, _, payload = _json(
            bridge,
            "/api/channels/send",
            method="POST",
            body={"channel": "general", "text": "defaulted"},
        )
        assert status == 200
        assert payload["message"]["expires_at"] - time.time() == pytest.approx(
            300, abs=5
        )
    finally:
        bridge.stop()


def test_channel_send_with_location_via_api(tmp_path):
    from tau.channels import ChannelApp, join_channel

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    # Attach a real channel app so sends work; its node points at the
    # fake so gossip has nowhere to go (delivered 0, no raise).
    app = ChannelApp(node.config, node.identity, name="tester", gossip=False)
    app.node = node
    node.channel_app = app
    app.join("general")

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/channels/send",
            method="POST",
            body={
                "channel": "general",
                "text": "meet here",
                "location": {
                    "lat": 34.05,
                    "lon": -83.88,
                    "name": "Rock Springs Park",
                },
            },
        )
        assert status == 200
        assert payload["message"]["location"] == {
            "lat": 34.05,
            "lon": -83.88,
            "name": "Rock Springs Park",
        }

        # The stored message carries the pin into history.
        status, _, payload = _json(
            bridge, "/api/channels/messages?channel=general"
        )
        assert status == 200
        assert payload["messages"][0]["location"] == {
            "lat": 34.05,
            "lon": -83.88,
            "name": "Rock Springs Park",
        }

        # A malformed pin is a 400 before the bridge is touched.
        for bad in (
            {"lat": 91, "lon": 0},
            {"lat": 34.05},
            "34.05,-83.88",
            42,
        ):
            status, _, payload = _json(
                bridge,
                "/api/channels/send",
                method="POST",
                body={"channel": "general", "text": "nope", "location": bad},
            )
            assert status == 400, bad
            assert "error" in payload

        # A pin cannot ride on a reply.
        status, _, payload = _json(
            bridge, "/api/channels/messages?channel=general"
        )
        assert status == 200
        parent = payload["messages"][0]
        status, _, payload = _json(
            bridge,
            "/api/channels/send",
            method="POST",
            body={
                "channel": "general",
                "text": "see you",
                "reply_to": parent["hash"],
                "location": {"lat": 34.05, "lon": -83.88},
            },
        )
        assert status == 400
        assert "top-level" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_channel_send_with_contact_via_api(tmp_path):
    from tau.channels import ChannelApp, join_channel

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    # Attach a real channel app so sends work; its node points at the
    # fake so gossip has nowhere to go (delivered 0, no raise).
    app = ChannelApp(node.config, node.identity, name="tester", gossip=False)
    app.node = node
    node.channel_app = app
    app.join("general")

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The bridge enforces the contact-sharing policy, like the DM
        # and group sends: no block-list, no send.
        node.blocklist = BlockList()
        card = {"name": "carol", "destination_hash": "cc" * 16}
        status, _, payload = _json(
            bridge,
            "/api/channels/send",
            method="POST",
            body={"channel": "general", "text": "meet carol", "contact": card},
        )
        assert status == 200
        assert payload["message"]["contact"] == card

        # The stored message carries the card into history.
        status, _, payload = _json(
            bridge, "/api/channels/messages?channel=general"
        )
        assert status == 200
        assert payload["messages"][0]["contact"] == card

        # A malformed card is a 400 before the bridge is touched.
        for bad in (
            {"name": "carol"},
            {"destination_hash": "cc" * 16},
            {"name": "carol", "destination_hash": "not-a-hash"},
            "carol",
            42,
        ):
            status, _, payload = _json(
                bridge,
                "/api/channels/send",
                method="POST",
                body={"channel": "general", "text": "nope", "contact": bad},
            )
            assert status == 400, bad
            assert "error" in payload

        # A blocked peer's card must not reach the channel, and
        # neither may your own — the recipient already has your hash.
        node.blocklist = BlockList(blocked=frozenset({"cc" * 16}))
        status, _, payload = _json(
            bridge,
            "/api/channels/send",
            method="POST",
            body={"channel": "general", "text": "meet carol", "contact": card},
        )
        assert status == 400
        assert "blocked peer" in payload["error"]["message"]
        node.blocklist = BlockList()
        status, _, payload = _json(
            bridge,
            "/api/channels/send",
            method="POST",
            body={
                "channel": "general",
                "text": "meet me",
                "contact": {
                    "name": "tester",
                    "destination_hash": node.destination_hexhash,
                },
            },
        )
        assert status == 400
        assert "cannot share your own contact" in payload["error"]["message"]

        # An unreadable block-list fails closed, like the DM send.
        node.blocklist = None
        status, _, payload = _json(
            bridge,
            "/api/channels/send",
            method="POST",
            body={"channel": "general", "text": "meet carol", "contact": card},
        )
        assert status == 400
        assert "block-list could not be read" in payload["error"]["message"]
        node.blocklist = BlockList()

        # A card cannot ride on a reply.
        status, _, payload = _json(
            bridge, "/api/channels/messages?channel=general"
        )
        assert status == 200
        parent = payload["messages"][0]
        status, _, payload = _json(
            bridge,
            "/api/channels/send",
            method="POST",
            body={
                "channel": "general",
                "text": "see her",
                "reply_to": parent["hash"],
                "contact": card,
            },
        )
        assert status == 400
        assert "top-level" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_pages_via_api(tmp_path):
    from types import SimpleNamespace

    import RNS

    from tau.config import TauConfig
    from tau.pages import (
        PageStore,
        build_page,
        load_local_page,
        make_page_file_entry,
        save_local_page,
        store_page_file,
    )

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")

    # A peer page with one linked file, stored as if fetched over RNS.
    author_identity = RNS.Identity()
    file_bytes = b"meetup notes"
    file_entry = make_page_file_entry("notes.txt", file_bytes)
    author_page = build_page(
        author_identity,
        title="Mesh meetup",
        markdown="# Mesh meetup\n\nSaturday, 2pm.\n",
        files=[file_entry],
        version=1,
        updated_at=1000.0,
    )
    author_hash = author_page["author"]
    store = PageStore(node.config.pages_path)
    assert store.store(author_page) is True
    store.save()
    node.page_store = store
    node.config.page_files_dir.mkdir(parents=True, exist_ok=True)
    store_page_file(node.config.page_files_dir, file_entry["sha256"], file_bytes)

    # This node's own page, as `tau page publish` writes it.
    own_page = build_page(
        node.identity,
        title="Tester page",
        markdown="# Hello\n\nThis is my page.\n",
        version=1,
        updated_at=1001.0,
    )
    save_local_page(node.config.page_path, own_page)
    node.local_page = load_local_page(node.config.page_path)

    # The console fetches through the node: "alice" resolves to the
    # page author, "bob" to a peer with no page, anything else fails.
    def _resolve(target):
        if target == "alice":
            return SimpleNamespace(hexhash=author_hash)
        if target == "bob":
            return SimpleNamespace(hexhash=BOB_HASH)
        raise NodeError(f"unknown peer: {target!r}")

    node.resolve_peer = _resolve
    node.request_page = lambda peer: node.page_store.get(peer.hexhash)

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/pages")
        assert status == 200
        assert payload["count"] == 1
        assert payload["pages"][0]["title"] == "Mesh meetup"
        assert payload["pages"][0]["author"] == author_hash
        assert payload["pages"][0]["files"] == [file_entry]
        assert "sig" not in payload["pages"][0]
        assert "key" not in payload["pages"][0]
        assert payload["mine"]["title"] == "Tester page"

        # A blocked author never reaches the list; the own page stays.
        node.blocklist = SimpleNamespace(
            blocked=frozenset({author_hash}), block_all=False
        )
        status, _, payload = _json(bridge, "/api/pages")
        assert status == 200
        assert payload["pages"] == []
        assert payload["mine"]["title"] == "Tester page"
        node.blocklist = None

        status, _, payload = _json(
            bridge, "/api/pages/fetch", method="POST", body={"peer": "alice"}
        )
        assert status == 200
        assert payload["page"]["title"] == "Mesh meetup"

        status, _, payload = _json(
            bridge, "/api/pages/fetch", method="POST", body={"peer": "bob"}
        )
        assert status == 200
        assert payload["page"] is None

        status, _, payload = _json(
            bridge, "/api/pages/fetch", method="POST", body={"peer": "nobody"}
        )
        assert status == 400
        status, _, payload = _json(
            bridge, "/api/pages/fetch", method="POST", body={}
        )
        assert status == 400

        # An unreachable peer is a 502, like a failed channel sync.
        def _failing_request(peer):
            raise NodeError("peer unreachable")

        node.request_page = _failing_request
        status, _, payload = _json(
            bridge, "/api/pages/fetch", method="POST", body={"peer": "alice"}
        )
        assert status == 502

        status, headers, body = _request(
            bridge, f"/api/pages/{author_hash}/files/{file_entry['sha256']}"
        )
        assert status == 200
        assert body == file_bytes
        assert "attachment" in headers["Content-Disposition"]
        assert "notes.txt" in headers["Content-Disposition"]

        # An unknown digest, an unlinked digest, or a malformed
        # author is a 404 — never a neighbouring page's file.
        status, _, payload = _json(
            bridge, f"/api/pages/{author_hash}/files/{'00' * 32}"
        )
        assert status == 404
        status, _, payload = _json(
            bridge, f"/api/pages/{'cd' * 16}/files/{file_entry['sha256']}"
        )
        assert status == 404
        status, _, payload = _json(
            bridge, f"/api/pages/not-a-hash/files/{file_entry['sha256']}"
        )
        assert status == 404
        status, _, payload = _json(
            bridge, f"/api/pages/{author_hash}/files/not-a-digest"
        )
        assert status == 404
    finally:
        bridge.stop()


def test_tan_authors_via_api(tmp_path):
    import RNS
    from types import SimpleNamespace

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    alice = RNS.Identity()
    bob = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, alice, "first", name="alice", now=1000)
        create_post(conn, alice, "second", name="alice", now=1001)
        bob_post = create_post(conn, bob, "hello", name="bob", now=1002)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # Default sort is most recent first; both authors listed with
        # post counts, last-post times and followed state, plus the
        # directory total behind the page.
        status, _, payload = _json(bridge, "/api/tan/authors")
        assert status == 200
        assert payload["count"] == 2
        assert payload["total"] == 2
        assert [item["name"] for item in payload["authors"]] == ["bob", "alice"]
        assert payload["authors"][1]["posts"] == 2
        assert payload["authors"][1]["last_post"] == 1001
        assert payload["authors"][1]["followed"] is False

        # Sort by post count puts the prolific author first.
        status, _, payload = _json(bridge, "/api/tan/authors?sort=posts")
        assert status == 200
        assert [item["name"] for item in payload["authors"]] == ["alice", "bob"]

        # Limit clamps to the page size, offset pages past it, and an
        # unknown sort is a 400.
        status, _, payload = _json(bridge, "/api/tan/authors?limit=1")
        assert status == 200
        assert payload["count"] == 1
        assert payload["total"] == 2
        status, _, payload = _json(bridge, "/api/tan/authors?limit=1&offset=1")
        assert status == 200
        assert [item["name"] for item in payload["authors"]] == ["alice"]
        assert payload["total"] == 2
        status, _, payload = _json(bridge, "/api/tan/authors?sort=nope")
        assert status == 400

        # A blocked author is hidden from the directory, like the
        # timeline hides their posts; a fail-closed list hides all.
        node.blocklist = SimpleNamespace(
            blocked=frozenset({bob_post["author"]}), block_all=False
        )
        status, _, payload = _json(bridge, "/api/tan/authors")
        assert status == 200
        assert [item["name"] for item in payload["authors"]] == ["alice"]
        node.blocklist = SimpleNamespace(blocked=frozenset(), block_all=True)
        status, _, payload = _json(bridge, "/api/tan/authors")
        assert status == 200
        assert payload == {"authors": [], "count": 0, "total": 0}
    finally:
        bridge.stop()


def test_tan_authors_paging_beyond_first_page(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    conn = init_db(node.config.social_db_path)
    try:
        # 55 authors: more than the default page of 50, so the second
        # page must exist and the total must count all of them.
        for index in range(55):
            create_post(
                conn,
                RNS.Identity(),
                f"post {index}",
                name=f"author {index:02d}",
                now=1000 + index,
            )
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/authors")
        assert status == 200
        assert payload["count"] == 50
        assert payload["total"] == 55
        status, _, payload = _json(bridge, "/api/tan/authors?limit=50&offset=50")
        assert status == 200
        assert payload["count"] == 5
        assert payload["total"] == 55
        # The two pages together cover every author exactly once.
        first = _json(bridge, "/api/tan/authors?limit=50")[2]["authors"]
        second = payload["authors"]
        assert len({item["author"] for item in first + second}) == 55
    finally:
        bridge.stop()


def test_tan_authors_empty_store(tmp_path):
    from tau.config import TauConfig

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A read-only listing must not create the Tan store on a fresh
        # data directory — but an unknown sort is still a 400 there.
        status, _, payload = _json(bridge, "/api/tan/authors")
        assert status == 200
        assert payload == {"authors": [], "count": 0, "total": 0}
        assert not node.config.social_db_path.exists()
        status, _, payload = _json(bridge, "/api/tan/authors?sort=nope")
        assert status == 400
        assert not node.config.social_db_path.exists()
    finally:
        bridge.stop()


def test_tan_verify_via_api(tmp_path):
    import sqlite3

    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, identity, "first post", name="tester", now=1000)
        second = create_post(conn, identity, "second post", name="tester", now=1001)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/verify")
        assert status == 200
        assert payload["ok"] is True
        assert payload["entries"] == 2
        assert payload["authors"] == 1
        assert payload["unlinked"] == 0
        assert payload["problems"] == []

        # Tampering with a stored post breaks its hash, which the audit
        # must report without flagging the store's other entries.
        conn = sqlite3.connect(node.config.social_db_path)
        try:
            conn.execute(
                "UPDATE posts SET text = 'tampered' WHERE hash = ?",
                (second["hash"],),
            )
            conn.commit()
        finally:
            conn.close()

        status, _, payload = _json(bridge, "/api/tan/verify")
        assert status == 200
        assert payload["ok"] is False
        assert payload["entries"] == 2
        assert len(payload["problems"]) == 1
        assert payload["problems"][0]["hash"] == second["hash"]
        assert "hash does not match" in payload["problems"][0]["problem"]
    finally:
        bridge.stop()


def test_tan_verify_without_a_store_is_an_error(tmp_path):
    from tau.config import TauConfig

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/verify")
        assert status == 400
        assert "no Tan store" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_verify_rejects_a_second_concurrent_audit(tmp_path, monkeypatch):
    import threading

    import RNS

    from tau.config import TauConfig
    from tau.tangents import tan as tan_module
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, identity, "first post", name="tester", now=1000)
    finally:
        conn.close()

    # Hold the first audit inside verify_store so a second request
    # arrives while it is still running.
    entered = threading.Event()
    release = threading.Event()
    real_verify = tan_module.verify_store

    def blocking_verify(conn):
        entered.set()
        assert release.wait(timeout=10)
        return real_verify(conn)

    monkeypatch.setattr(tan_module, "verify_store", blocking_verify)

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        results = {}

        def first_audit():
            results["first"] = _json(bridge, "/api/tan/verify")

        worker = threading.Thread(target=first_audit, daemon=True)
        worker.start()
        assert entered.wait(timeout=10)

        status, _, payload = _json(bridge, "/api/tan/verify")
        assert status == 429
        assert "already running" in payload["error"]["message"]

        release.set()
        worker.join(timeout=10)
        status, _, payload = results["first"]
        assert status == 200
        assert payload["ok"] is True
    finally:
        release.set()
        bridge.stop()


def test_tan_suggest_via_api(tmp_path):
    import RNS

    from tau.blocklist import BlockList
    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bob = RNS.Identity()
    carol = RNS.Identity()
    dave = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        bob_post = create_post(conn, bob, "bob posts", name="bob", now=1000)
        carol_post = create_post(conn, carol, "carol posts", name="carol", now=1001)
        dave_post = create_post(conn, dave, "dave posts", name="dave", now=1002)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # No follows yet and no likes: equal scores, newest post first.
        status, _, payload = _json(bridge, "/api/tan/suggest")
        assert status == 200
        assert [item["author"] for item in payload["suggestions"]] == [
            dave_post["author"],
            carol_post["author"],
            bob_post["author"],
        ]
        assert payload["count"] == 3
        assert payload["suggestions"][0]["name"] == "dave"
        assert payload["suggestions"][0]["posts"] == 1

        status, _, payload = _json(bridge, "/api/tan/suggest?limit=1")
        assert status == 200
        assert payload["count"] == 1
        assert payload["suggestions"][0]["author"] == dave_post["author"]

        # Following bob removes him from the suggestions.
        status, _, _ = _json(
            bridge,
            "/api/tan/follow",
            method="POST",
            body={"target": bob_post["author"]},
        )
        assert status == 200
        status, _, payload = _json(bridge, "/api/tan/suggest")
        assert [item["author"] for item in payload["suggestions"]] == [
            dave_post["author"],
            carol_post["author"],
        ]

        # A blocked author is never suggested; a failed-closed list
        # suggests nobody at all.
        node.blocklist = BlockList(blocked=frozenset({dave_post["author"]}))
        status, _, payload = _json(bridge, "/api/tan/suggest")
        assert [item["author"] for item in payload["suggestions"]] == [
            carol_post["author"]
        ]
        node.blocklist = BlockList(blocked=frozenset(), block_all=True)
        status, _, payload = _json(bridge, "/api/tan/suggest")
        assert payload["suggestions"] == []
        assert payload["count"] == 0
    finally:
        bridge.stop()


def test_snapshot_counts_tan_posts_from_the_store(tmp_path):
    """Regression test for the Codex P2 on PR #180: snapshot
    ``social.posts`` counted the session-scoped post-event buffer, so
    a node with posts already on disk reported 0. It must count the
    store instead."""
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        keeper = create_post(conn, identity, "keeper", name="tester", now=1000)
        doomed = create_post(conn, identity, "doomed", name="tester", now=1001)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    # A like entry and an author delete land in the same table; neither
    # is a readable post.
    node.app.react(keeper["hash"], liked=True)
    node.app.delete(doomed["hash"])

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # Nothing arrived over the wire this session: the event buffer
        # is empty, so a buffer count would say 0.
        assert len(bridge.tan_posts(limit=0)) == 0
        assert bridge.snapshot()["social"] == {"enabled": True, "posts": 1}
    finally:
        bridge.stop()


def test_peers_block_unblock_via_api(tmp_path):
    from tau.config import TauConfig

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # Block by name; the file and the node's cached list both update,
        # and the peers snapshot carries the new blocked flag.
        status, _, payload = _json(
            bridge, "/api/peers/block", method="POST", body={"target": "alice"}
        )
        assert status == 200
        assert payload["block"]["peer"]["destination"] == PEER_HASH
        assert payload["block"]["peer"]["name"] == "alice"
        assert payload["block"]["blocked"] is True
        assert payload["block"]["changed"] is True
        assert node.blocklist is not None
        assert node.blocklist.is_blocked(PEER_HASH)
        assert json.loads((tmp_path / "blocklist.json").read_text()) == {
            "blocked": [PEER_HASH]
        }
        status, _, payload = _json(bridge, "/api/peers")
        assert status == 200
        assert payload["peers"][0]["blocked"] is True

        # Re-blocking is a no-op.
        status, _, payload = _json(
            bridge, "/api/peers/block", method="POST", body={"target": "alice"}
        )
        assert status == 200
        assert payload["block"]["changed"] is False
        assert payload["block"]["blocked"] is True

        # Unblock by hash prefix.
        status, _, payload = _json(
            bridge,
            "/api/peers/unblock",
            method="POST",
            body={"target": PEER_HASH[:8]},
        )
        assert status == 200
        assert payload["block"]["blocked"] is False
        assert payload["block"]["changed"] is True
        assert not node.blocklist.is_blocked(PEER_HASH)
        status, _, payload = _json(bridge, "/api/peers")
        assert payload["peers"][0]["blocked"] is False

        # Unblocking again is a no-op.
        status, _, payload = _json(
            bridge, "/api/peers/unblock", method="POST", body={"target": "alice"}
        )
        assert status == 200
        assert payload["block"]["changed"] is False

        # Unknown peers and missing targets are rejected.
        status, _, payload = _json(
            bridge, "/api/peers/block", method="POST", body={"target": "nobody"}
        )
        assert status == 400
        status, _, payload = _json(
            bridge, "/api/peers/block", method="POST", body={}
        )
        assert status == 400
    finally:
        bridge.stop()


def test_conversations_archive_unarchive_via_api(tmp_path):
    node = FakeNode()
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        # Archive by name; the file updates and the list carries it.
        status, _, payload = _json(
            bridge,
            "/api/conversations/archive",
            method="POST",
            body={"target": "alice"},
        )
        assert status == 200
        assert payload["ok"] is True
        assert payload["archive"]["destination"] == PEER_HASH
        assert payload["archive"]["name"] == "alice"
        assert payload["archive"]["archived"] is True
        assert payload["archive"]["changed"] is True
        assert json.loads((tmp_path / "archived.json").read_text()) == {
            "archived": [PEER_HASH]
        }

        status, _, payload = _json(bridge, "/api/conversations/archived")
        assert status == 200
        assert payload["count"] == 1
        assert payload["archived"][0]["destination"] == PEER_HASH
        assert payload["archived"][0]["name"] == "alice"
        assert payload["archived"][0]["messages"] == 0

        # Re-archiving is a no-op.
        status, _, payload = _json(
            bridge,
            "/api/conversations/archive",
            method="POST",
            body={"target": "alice"},
        )
        assert status == 200
        assert payload["archive"]["changed"] is False
        assert payload["archive"]["archived"] is True

        # Unarchive by hash prefix.
        status, _, payload = _json(
            bridge,
            "/api/conversations/unarchive",
            method="POST",
            body={"target": PEER_HASH[:8]},
        )
        assert status == 200
        assert payload["archive"]["archived"] is False
        assert payload["archive"]["changed"] is True
        assert json.loads((tmp_path / "archived.json").read_text()) == {
            "archived": []
        }

        # Unarchiving again is a no-op.
        status, _, payload = _json(
            bridge,
            "/api/conversations/unarchive",
            method="POST",
            body={"target": "alice"},
        )
        assert status == 200
        assert payload["archive"]["changed"] is False

        # Unknown peers and missing targets are rejected.
        status, _, payload = _json(
            bridge,
            "/api/conversations/archive",
            method="POST",
            body={"target": "nobody"},
        )
        assert status == 400
        status, _, payload = _json(
            bridge, "/api/conversations/archive", method="POST", body={}
        )
        assert status == 400
    finally:
        bridge.stop()


def test_archived_conversations_hide_messages_and_unread(node, tmp_path):
    seeded = [
        _stored_message(1, "from alice"),
        _stored_message(2, "to alice", direction="out"),
        {
            **_stored_message(3, "from carol"),
            "peer": OTHER_HASH,
            "peer_name": "carol",
        },
        {
            **_stored_message(4, "crew hello"),
            "group": "11" * 16,
            "group_name": "crew",
        },
    ]
    (tmp_path / "messages.json").write_text(
        json.dumps({"messages": seeded, "next_message_id": 5}),
        encoding="utf-8",
    )
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        _inbound_record(node)
        _inbound_record(
            node,
            destination=OTHER_HASH,
            **{"from": "carol", "id": "m-2", "received_at": 102.0},
        )
        # A group post from alice: same destination as her DM, but it
        # belongs to the group thread, which archiving must not hide.
        _inbound_record(
            node,
            kind="group",
            group="11" * 16,
            **{"from": "alice", "id": "m-3", "received_at": 103.0},
        )

        assert [m["text"] for m in bridge.messages()] == [
            "from alice",
            "to alice",
            "from carol",
            "crew hello",
        ]
        assert _counts(bridge.unread()) == {
            "per_peer": {PEER_HASH: 1, OTHER_HASH: 1},
            "per_group": {"11" * 16: 1},
            "per_channel": {},
            "total": 3,
        }

        result = bridge.archive_conversation("alice")
        assert result["archived"] is True

        # Both directions of the archived thread hide; the other peer
        # and the group post — which is never part of an archived
        # conversation — stay visible.
        assert [m["text"] for m in bridge.messages()] == [
            "from carol",
            "crew hello",
        ]
        # Search reuses messages(), so it hides the thread too.
        assert bridge.messages_search(query="alice") == []
        assert [m["text"] for m in bridge.messages_search(query="carol")] == [
            "from carol"
        ]
        # The list counts the hidden thread, both directions.
        listed = bridge.archived_conversations()
        assert listed["count"] == 1
        assert listed["archived"][0]["messages"] == 2
        # The archived thread stops lighting the badge — but alice's
        # group post still counts, since the group thread stays visible.
        assert _counts(bridge.unread()) == {
            "per_peer": {OTHER_HASH: 1},
            "per_group": {"11" * 16: 1},
            "per_channel": {},
            "total": 2,
        }

        bridge.unarchive_conversation(PEER_HASH)
        assert [m["text"] for m in bridge.messages()] == [
            "from alice",
            "to alice",
            "from carol",
            "crew hello",
        ]
        assert _counts(bridge.unread()) == {
            "per_peer": {PEER_HASH: 1, OTHER_HASH: 1},
            "per_group": {"11" * 16: 1},
            "per_channel": {},
            "total": 3,
        }
    finally:
        bridge.stop()


def test_archive_rejects_corrupt_list_but_lists_fail_open(tmp_path):
    node = FakeNode()
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        (tmp_path / "archived.json").write_text("{not json", encoding="utf-8")
        status, _, payload = _json(
            bridge,
            "/api/conversations/archive",
            method="POST",
            body={"target": "alice"},
        )
        assert status == 503
        assert "unusable" in payload["error"]["message"]
        # The list fails open: a corrupt file never hides mail.
        status, _, payload = _json(bridge, "/api/conversations/archived")
        assert status == 200
        assert payload == {"archived": [], "count": 0}
    finally:
        bridge.stop()


def test_archive_without_data_dir(bridge):
    # A dataless node has no archive-list path: edits are refused, but
    # the list still answers empty instead of failing the console poll.
    status, _, payload = _json(
        bridge, "/api/conversations/archive", method="POST", body={"target": "alice"}
    )
    assert status == 503
    status, _, payload = _json(bridge, "/api/conversations/archived")
    assert status == 200
    assert payload == {"archived": [], "count": 0}


def test_disappearing_defaults_endpoints(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        status, _, payload = _json(bridge, "/api/messages/disappearing")
        assert status == 200
        assert payload == {"peers": {}, "groups": {}, "channels": {}}

        status, _, payload = _json(
            bridge,
            "/api/messages/disappearing",
            method="POST",
            body={"peer": "alice", "seconds": 3600},
        )
        assert status == 200
        assert payload["disappearing"] == {
            "target": PEER_HASH,
            "group": False,
            "channel": False,
            "disappearing_seconds": 3600,
            "changed": True,
        }
        status, _, payload = _json(bridge, "/api/messages/disappearing")
        assert payload == {"peers": {PEER_HASH: 3600.0}, "groups": {}, "channels": {}}

        # A channel default is keyed by channel name, no lookup needed.
        status, _, payload = _json(
            bridge,
            "/api/messages/disappearing",
            method="POST",
            body={"channel": "general", "seconds": 600},
        )
        assert status == 200
        assert payload["disappearing"]["channel"] is True
        assert payload["disappearing"]["group"] is False
        assert payload["disappearing"]["target"] == "general"
        assert payload["disappearing"]["disappearing_seconds"] == 600
        status, _, payload = _json(bridge, "/api/messages/disappearing")
        assert payload["channels"] == {"general": 600.0}

        # Clearing a channel default removes the entry.
        status, _, payload = _json(
            bridge,
            "/api/messages/disappearing",
            method="POST",
            body={"channel": "general", "seconds": None},
        )
        assert status == 200
        assert payload["disappearing"]["disappearing_seconds"] is None
        assert payload["disappearing"]["changed"] is True
        status, _, payload = _json(bridge, "/api/messages/disappearing")
        assert payload["channels"] == {}

        # A group default is keyed by group id, resolved by name.
        status, _, payload = _json(
            bridge,
            "/api/messages/disappearing",
            method="POST",
            body={"group": "crew", "seconds": 300},
        )
        assert status == 200
        assert payload["disappearing"]["group"] is True
        assert payload["disappearing"]["disappearing_seconds"] == 300
        status, _, payload = _json(bridge, "/api/messages/disappearing")
        assert payload["groups"] != {}
        assert list(payload["groups"].values()) == [300.0]

        # Clearing removes the entry; clearing again is a no-op.
        status, _, payload = _json(
            bridge,
            "/api/messages/disappearing",
            method="POST",
            body={"peer": "alice", "seconds": None},
        )
        assert status == 200
        assert payload["disappearing"]["disappearing_seconds"] is None
        assert payload["disappearing"]["changed"] is True
        status, _, payload = _json(
            bridge,
            "/api/messages/disappearing",
            method="POST",
            body={"peer": "alice", "seconds": None},
        )
        assert payload["disappearing"]["changed"] is False
        status, _, payload = _json(bridge, "/api/messages/disappearing")
        assert payload["peers"] == {}
    finally:
        bridge.stop()


def test_disappearing_defaults_endpoint_rejects_bad_input(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        for body in (
            {},
            {"peer": "alice", "group": "crew", "seconds": 60},
            {"peer": "alice", "channel": "general", "seconds": 60},
            {"group": "crew", "channel": "general", "seconds": 60},
            {"peer": "alice", "seconds": 0},
            {"peer": "alice", "seconds": -5},
            {"peer": "alice", "seconds": 31 * 24 * 3600},
            {"peer": "alice", "seconds": "1h"},
            {"peer": "alice", "seconds": True},
            {"peer": "nobody", "seconds": 60},
            {"peer": "  ", "seconds": 60},
            {"group": "nobody", "seconds": 60},
            {"channel": "bad name!", "seconds": 60},
            {"channel": "general", "seconds": 8 * 24 * 3600},
        ):
            status, _, payload = _json(
                bridge, "/api/messages/disappearing", method="POST", body=body
            )
            assert status == 400, body
            assert "error" in payload
    finally:
        bridge.stop()


def test_disappearing_defaults_rejects_corrupt_file(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        (tmp_path / "disappearing.json").write_text("{not json", encoding="utf-8")
        status, _, payload = _json(
            bridge,
            "/api/messages/disappearing",
            method="POST",
            body={"peer": "alice", "seconds": 60},
        )
        assert status == 503
        assert "unusable" in payload["error"]["message"]
        status, _, payload = _json(bridge, "/api/messages/disappearing")
        assert status == 503
    finally:
        bridge.stop()


def test_disappearing_defaults_without_data_dir(bridge):
    # A dataless node has no disappearing-defaults path: edits are
    # refused, but the list still answers empty instead of failing the
    # console poll.
    status, _, payload = _json(
        bridge,
        "/api/messages/disappearing",
        method="POST",
        body={"peer": "alice", "seconds": 60},
    )
    assert status == 503
    status, _, payload = _json(bridge, "/api/messages/disappearing")
    assert status == 200
    assert payload == {"peers": {}, "groups": {}, "channels": {}}


def test_quiet_hours_endpoints(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        # A fresh node has no window: off, with the default times kept.
        status, _, payload = _json(bridge, "/api/settings/quiet-hours")
        assert status == 200
        assert payload == {
            "enabled": False,
            "start": "22:00",
            "end": "07:00",
            "active": False,
            "minutes_until_end": None,
        }

        # A window covering right now reports active, with minutes left.
        now = time.localtime()
        current = now.tm_hour * 60 + now.tm_min
        start = (current - 60) % (24 * 60)
        end = (current + 60) % (24 * 60)
        status, _, payload = _json(
            bridge,
            "/api/settings/quiet-hours",
            method="POST",
            body={
                "enabled": True,
                "start": f"{start // 60:02d}:{start % 60:02d}",
                "end": f"{end // 60:02d}:{end % 60:02d}",
            },
        )
        assert status == 200
        assert payload["ok"] is True
        assert payload["quiet_hours"]["enabled"] is True
        assert payload["quiet_hours"]["active"] is True
        assert payload["quiet_hours"]["minutes_until_end"] is not None
        assert 0 < payload["quiet_hours"]["minutes_until_end"] <= 120
        assert payload["quiet_hours"]["changed"] is True
        status, _, payload = _json(bridge, "/api/settings/quiet-hours")
        assert payload["enabled"] is True
        assert payload["active"] is True

        # Turning it off keeps the start/end, so turning it back on
        # restores the same window; the no-op set reports unchanged.
        status, _, payload = _json(
            bridge,
            "/api/settings/quiet-hours",
            method="POST",
            body={"enabled": False},
        )
        assert status == 200
        assert payload["quiet_hours"]["enabled"] is False
        assert payload["quiet_hours"]["active"] is False
        assert payload["quiet_hours"]["minutes_until_end"] is None
        assert payload["quiet_hours"]["start"] == (
            f"{start // 60:02d}:{start % 60:02d}"
        )
        assert payload["quiet_hours"]["end"] == f"{end // 60:02d}:{end % 60:02d}"
        assert payload["quiet_hours"]["changed"] is True
        status, _, payload = _json(
            bridge,
            "/api/settings/quiet-hours",
            method="POST",
            body={"enabled": False},
        )
        assert payload["quiet_hours"]["changed"] is False
    finally:
        bridge.stop()


def test_quiet_hours_endpoint_rejects_bad_input(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        for body in (
            {},
            {"enabled": "yes"},
            {"start": "25:00", "end": "07:00"},
            {"start": "22:00", "end": "7am"},
            {"start": "22:00", "end": "22:00"},
            {"start": 2200, "end": "07:00"},
        ):
            status, _, payload = _json(
                bridge, "/api/settings/quiet-hours", method="POST", body=body
            )
            assert status == 400, body
            assert "error" in payload
    finally:
        bridge.stop()


def test_quiet_hours_rejects_corrupt_file(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        (tmp_path / "quiet_hours.json").write_text("{not json", encoding="utf-8")
        status, _, payload = _json(
            bridge,
            "/api/settings/quiet-hours",
            method="POST",
            body={"enabled": True, "start": "22:00", "end": "07:00"},
        )
        assert status == 503
        assert "unusable" in payload["error"]["message"]
        status, _, payload = _json(bridge, "/api/settings/quiet-hours")
        assert status == 503
    finally:
        bridge.stop()


def test_quiet_hours_without_data_dir(bridge):
    # A dataless node has no quiet-hours path: edits are refused, but
    # the read still answers the default off window instead of failing
    # the console poll.
    status, _, payload = _json(
        bridge,
        "/api/settings/quiet-hours",
        method="POST",
        body={"enabled": True, "start": "22:00", "end": "07:00"},
    )
    assert status == 503
    status, _, payload = _json(bridge, "/api/settings/quiet-hours")
    assert status == 200
    assert payload["enabled"] is False
    assert payload["active"] is False


def test_away_message_endpoints(node, tmp_path):
    from tau.away import AwayState, save_away

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        # A fresh node has no away file: off, with the default message kept.
        status, _, payload = _json(bridge, "/api/settings/away-message")
        assert status == 200
        assert payload == {
            "enabled": False,
            "message": DEFAULT_AWAY_MESSAGE,
            "since": None,
            "replied_to": [],
            "replied_count": 0,
        }

        # Turning it on with a custom message starts a fresh period.
        status, _, payload = _json(
            bridge,
            "/api/settings/away-message",
            method="POST",
            body={"enabled": True, "message": "back on Monday"},
        )
        assert status == 200
        assert payload["ok"] is True
        assert payload["away_message"]["enabled"] is True
        assert payload["away_message"]["message"] == "back on Monday"
        assert payload["away_message"]["since"] is not None
        assert payload["away_message"]["replied_count"] == 0
        assert payload["away_message"]["changed"] is True

        # A peer that already got a reply shows in the count.
        save_away(
            tmp_path / "away.json",
            AwayState.on("back on Monday").with_replied(PEER_HASH),
        )
        status, _, payload = _json(bridge, "/api/settings/away-message")
        assert payload["replied_count"] == 1
        assert payload["replied_to"] == [PEER_HASH]

        # A no-op save — on already, same message — leaves the period
        # (and its replied list) untouched.
        status, _, payload = _json(
            bridge,
            "/api/settings/away-message",
            method="POST",
            body={"enabled": True, "message": "back on Monday"},
        )
        assert payload["away_message"]["changed"] is False
        status, _, payload = _json(bridge, "/api/settings/away-message")
        assert payload["replied_count"] == 1

        # Changing the message while on starts a fresh period.
        status, _, payload = _json(
            bridge,
            "/api/settings/away-message",
            method="POST",
            body={"message": "back Tuesday instead"},
        )
        assert payload["away_message"]["message"] == "back Tuesday instead"
        assert payload["away_message"]["replied_count"] == 0
        assert payload["away_message"]["changed"] is True

        # Turning it off keeps the message, so turning it back on
        # restores the same text; the no-op set reports unchanged.
        status, _, payload = _json(
            bridge,
            "/api/settings/away-message",
            method="POST",
            body={"enabled": False},
        )
        assert status == 200
        assert payload["away_message"]["enabled"] is False
        assert payload["away_message"]["message"] == "back Tuesday instead"
        assert payload["away_message"]["changed"] is True
        status, _, payload = _json(
            bridge,
            "/api/settings/away-message",
            method="POST",
            body={"enabled": False},
        )
        assert payload["away_message"]["changed"] is False
    finally:
        bridge.stop()


def test_away_message_endpoint_rejects_bad_input(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        for body in (
            {},
            {"enabled": "yes"},
            {"enabled": True, "message": ""},
            {"enabled": True, "message": "   "},
            {"enabled": True, "message": 42},
            {"enabled": True, "message": "x" * 2001},
        ):
            status, _, payload = _json(
                bridge, "/api/settings/away-message", method="POST", body=body
            )
            assert status == 400, body
            assert "error" in payload
    finally:
        bridge.stop()


def test_away_message_rejects_corrupt_file(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        (tmp_path / "away.json").write_text("{not json", encoding="utf-8")
        status, _, payload = _json(
            bridge,
            "/api/settings/away-message",
            method="POST",
            body={"enabled": True},
        )
        assert status == 503
        assert "unusable" in payload["error"]["message"]
        status, _, payload = _json(bridge, "/api/settings/away-message")
        assert status == 503
    finally:
        bridge.stop()


def test_away_message_without_data_dir(bridge):
    # A dataless node has no away-message path: edits are refused, but
    # the read still answers the default off state instead of failing
    # the console poll.
    status, _, payload = _json(
        bridge,
        "/api/settings/away-message",
        method="POST",
        body={"enabled": True},
    )
    assert status == 503
    status, _, payload = _json(bridge, "/api/settings/away-message")
    assert status == 200
    assert payload["enabled"] is False
    assert payload["message"] == DEFAULT_AWAY_MESSAGE


def test_drafts_endpoints(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        status, _, payload = _json(bridge, "/api/drafts")
        assert status == 200
        assert payload == {"drafts": [], "count": 0}

        # Save a peer draft; the list resolves the peer's registry name.
        status, _, payload = _json(
            bridge,
            "/api/drafts",
            method="POST",
            body={"kind": "peer", "target": PEER_HASH, "text": "hello alice"},
        )
        assert status == 200
        assert payload["ok"] is True
        assert payload["draft"]["kind"] == "peer"
        assert payload["draft"]["target"] == PEER_HASH
        assert payload["draft"]["text"] == "hello alice"

        status, _, payload = _json(bridge, "/api/drafts")
        assert payload["count"] == 1
        assert payload["drafts"][0]["name"] == "alice"

        # A group draft is keyed by group id and resolves the group name.
        group_id = node.groups.list_groups()[0]["id"]
        status, _, payload = _json(
            bridge,
            "/api/drafts",
            method="POST",
            body={"kind": "group", "target": group_id, "text": "hello crew"},
        )
        assert status == 200
        status, _, payload = _json(bridge, "/api/drafts")
        assert payload["count"] == 2
        assert {d["kind"] for d in payload["drafts"]} == {"peer", "group"}
        group_entry = [d for d in payload["drafts"] if d["kind"] == "group"][0]
        assert group_entry["name"] == "crew"

        # A draft for an unknown target still lists, with no name.
        status, _, payload = _json(
            bridge,
            "/api/drafts",
            method="POST",
            body={"kind": "peer", "target": OTHER_HASH, "text": "stranger"},
        )
        assert status == 200
        status, _, payload = _json(bridge, "/api/drafts")
        assert payload["count"] == 3
        stranger = [d for d in payload["drafts"] if d["target"] == OTHER_HASH][0]
        assert stranger["name"] is None

        # Saving again overwrites the text (and re-sorts newest last).
        status, _, payload = _json(
            bridge,
            "/api/drafts",
            method="POST",
            body={"kind": "peer", "target": PEER_HASH, "text": "hello again"},
        )
        assert status == 200
        status, _, payload = _json(bridge, "/api/drafts")
        assert payload["count"] == 3
        peer_entry = [d for d in payload["drafts"] if d["target"] == PEER_HASH][0]
        assert peer_entry["text"] == "hello again"

        # Deleting removes the draft; deleting again is a no-op.
        status, _, payload = _json(
            bridge,
            "/api/drafts/delete",
            method="POST",
            body={"kind": "peer", "target": PEER_HASH},
        )
        assert status == 200
        assert payload == {"ok": True, "deleted": True}
        status, _, payload = _json(
            bridge,
            "/api/drafts/delete",
            method="POST",
            body={"kind": "peer", "target": PEER_HASH},
        )
        assert payload == {"ok": True, "deleted": False}
        status, _, payload = _json(bridge, "/api/drafts")
        assert payload["count"] == 2
    finally:
        bridge.stop()


def test_reminders_endpoints(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        status, _, payload = _json(bridge, "/api/reminders")
        assert status == 200
        assert payload == {"reminders": [], "count": 0}

        # A received DM can be snoozed; the list resolves the peer name.
        _seed_pinnable_message(bridge, node)
        before = time.time()
        status, _, payload = _json(
            bridge,
            "/api/reminders",
            method="POST",
            body={"id": 1, "in_seconds": 3600},
        )
        assert status == 200
        assert payload["ok"] is True
        reminder = payload["reminder"]
        assert reminder["kind"] == "peer"
        assert reminder["target"] == PEER_HASH
        assert reminder["target_name"] == "alice"
        assert reminder["sender"] == "alice"
        assert reminder["text"] == "hello"
        assert reminder["message_id"] == "wire-1"
        assert before + 3500 < reminder["due_at"] <= time.time() + 3600

        # The list prefers the owner's alias over the peer's own name.
        node.registry._peers[0].alias = "ally"
        status, _, payload = _json(bridge, "/api/reminders")
        assert payload["reminders"][0]["target_name"] == "ally"
        node.registry._peers[0].alias = None

        # An ISO time works too, and the list is soonest first.
        from datetime import datetime

        at_time = datetime.fromtimestamp(time.time() + 2 * 86400).strftime(
            "%Y-%m-%dT%H:%M"
        )
        status, _, payload = _json(
            bridge,
            "/api/reminders",
            method="POST",
            body={"id": 1, "at": at_time},
        )
        assert status == 200
        status, _, payload = _json(bridge, "/api/reminders")
        assert payload["count"] == 2
        assert payload["reminders"][0]["id"] == reminder["id"]
        assert payload["reminders"][1]["due_at"] > payload["reminders"][0]["due_at"]

        # Cancelling by unique prefix drops just that reminder.
        status, _, payload = _json(
            bridge,
            "/api/reminders/cancel",
            method="POST",
            body={"id": reminder["id"][:8]},
        )
        assert status == 200
        assert payload["ok"] is True
        assert payload["cancelled"]["id"] == reminder["id"]
        status, _, payload = _json(bridge, "/api/reminders")
        assert payload["count"] == 1

        # Bad inputs are 400s, not 500s.
        for body in (
            {"id": 999, "in_seconds": 60},
            {"id": 1},
            {"id": 1, "in_seconds": 0},
            {"id": 1, "in_seconds": -5},
            {"id": 1, "in_seconds": "1h"},
            {"id": 1, "in_seconds": 60, "at": at_time},
            {"id": 1, "at": "2020-01-01T09:00"},
            {"id": 1, "at": "not a time"},
        ):
            status, _, payload = _json(
                bridge, "/api/reminders", method="POST", body=body
            )
            assert status == 400, body
        status, _, payload = _json(
            bridge, "/api/reminders/cancel", method="POST", body={"id": "nope"}
        )
        assert status == 400
        status, _, payload = _json(
            bridge, "/api/reminders/cancel", method="POST", body={"id": ""}
        )
        assert status == 400

        # A message you sent has no received copy to re-ping.
        bridge.send_message("alice", "hi out")
        sent_id = bridge._messages[-1]["id"]
        status, _, payload = _json(
            bridge,
            "/api/reminders",
            method="POST",
            body={"id": sent_id, "in_seconds": 60},
        )
        assert status == 400

        # A file message has no text to be reminded about.
        node.inbox.append(
            {
                "id": "wire-file",
                "kind": "file",
                "destination": PEER_HASH,
                "from": "alice",
                "name": "photo.jpg",
                "size": 123,
                "sha256": "ab" * 32,
                "path": "files/photo.jpg",
                "sent_at": 100.0,
                "received_at": 101.0,
            }
        )
        file_entry = bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "wire-file",
                "file": {"name": "photo.jpg", "size": 123, "sha256": "ab" * 32},
            }
        )
        assert file_entry is not None and file_entry.get("file") is not None
        status, _, payload = _json(
            bridge,
            "/api/reminders",
            method="POST",
            body={"id": file_entry["id"], "in_seconds": 60},
        )
        assert status == 400

        # A group message snoozes against the group, with its name.
        group_id = node.groups.list_groups()[0]["id"]
        node.inbox.append(
            {
                "id": "wire-g1",
                "kind": "group",
                "group": group_id,
                "destination": PEER_HASH,
                "from": "alice",
                "text": "crew hello",
                "sent_at": 100.0,
                "received_at": 101.0,
            }
        )
        group_entry = bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "crew hello",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "wire-g1",
                "kind": "group",
                "group": group_id,
                "group_name": "crew",
            }
        )
        assert group_entry is not None
        status, _, payload = _json(
            bridge,
            "/api/reminders",
            method="POST",
            body={"id": group_entry["id"], "in_seconds": 60},
        )
        assert status == 200
        assert payload["reminder"]["kind"] == "group"
        assert payload["reminder"]["target"] == group_id
        assert payload["reminder"]["target_name"] == "crew"

        # A reminder whose source message is gone stays hidden: the
        # sidebar must not keep showing the text of a message the
        # inbox purged (for example a disappearing DM that expired).
        node.inbox.delete("wire-1")
        status, _, payload = _json(bridge, "/api/reminders")
        assert status == 200
        assert payload["count"] == 1
        assert payload["reminders"][0]["message_id"] == "wire-g1"

        # An expired disappearing message is hidden the same way,
        # even though its reminder is still pending in the store.
        from tau.reminders import ReminderStore

        node.inbox.append(
            {
                "id": "wire-old",
                "destination": PEER_HASH,
                "from": "alice",
                "text": "gone soon",
                "sent_at": 100.0,
                "received_at": 101.0,
                "expires_at": time.time() - 60,
            }
        )
        ReminderStore(bridge._reminders_path()).add(
            message_id="wire-old",
            kind="peer",
            target=PEER_HASH,
            text="gone soon",
            sender="alice",
            due_at=time.time() + 3600,
        )
        status, _, payload = _json(bridge, "/api/reminders")
        assert status == 200
        assert payload["count"] == 1
        assert payload["reminders"][0]["message_id"] == "wire-g1"

        # Blocking the sender refuses a new reminder and hides the
        # pending ones, like the CLI does.
        save_blocklist(
            tmp_path / "blocklist.json",
            BlockList(blocked=frozenset({PEER_HASH})),
        )
        status, _, payload = _json(
            bridge,
            "/api/reminders",
            method="POST",
            body={"id": 1, "in_seconds": 60},
        )
        assert status == 400
        status, _, payload = _json(bridge, "/api/reminders")
        assert payload == {"reminders": [], "count": 0}
    finally:
        bridge.stop()


def test_reminders_endpoint_returns_503_when_store_locked(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        # A directory where the lock file goes makes every store
        # listing fail: the endpoint must answer storage-unavailable,
        # not a generic internal error.
        (tmp_path / "reminders.json.lock").mkdir()
        status, _, payload = _json(bridge, "/api/reminders")
        assert status == 503
        assert "could not read the reminders" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_reminders_list_shows_channel_reminder(node, tmp_path):
    """A channel reminder lists with its #name; blocking the author hides it.

    The console cannot create one yet (POST /api/reminders only
    knows inbox message ids — see #797), so the reminder is seeded
    straight into the store, the way the CLI creates it.
    """
    from tau.channels import ChannelApp, create_message
    from tau.identity import save_identity
    from tau.reminders import ReminderStore

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        config = node.config
        owner = RNS.Identity()
        save_identity(owner, config.identity_path)
        author = RNS.Identity()
        app = ChannelApp(config, owner, name="tester", gossip=False)
        app.join("general")
        conn = app.connect()
        try:
            message = create_message(
                conn, author, "general", "agenda?", name="bob", now=time.time()
            )
        finally:
            conn.close()
        ReminderStore(bridge._reminders_path()).add(
            message_id=message["hash"],
            kind="channel",
            target="general",
            text="agenda?",
            sender="bob",
            due_at=time.time() + 3600,
        )
        status, _, payload = _json(bridge, "/api/reminders")
        assert status == 200
        assert payload["count"] == 1
        shown = payload["reminders"][0]
        assert shown["kind"] == "channel"
        assert shown["target"] == "general"
        assert shown["target_name"] == "#general"
        assert shown["sender"] == "bob"

        # Blocking the author hides it, like any other reminder.
        save_blocklist(
            config.blocklist_path,
            BlockList(blocked=frozenset({destination_hash(author).hex()})),
        )
        status, _, payload = _json(bridge, "/api/reminders")
        assert payload == {"reminders": [], "count": 0}
    finally:
        bridge.stop()


def test_reminders_hide_message_deleted_by_another_process(node, tmp_path):
    """The listing reloads the inbox file, so a delete from another
    process hides the reminder even though the serving bridge's
    inbox copy is stale."""
    inbox_path = tmp_path / "inbox.json"
    node.inbox = Inbox(inbox_path)
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        _seed_pinnable_message(bridge, node)
        status, _, payload = _json(
            bridge,
            "/api/reminders",
            method="POST",
            body={"id": 1, "in_seconds": 3600},
        )
        assert status == 200
        status, _, payload = _json(bridge, "/api/reminders")
        assert payload["count"] == 1

        # Another process deletes the message from the shared file.
        Inbox(inbox_path).delete("wire-1")
        status, _, payload = _json(bridge, "/api/reminders")
        assert status == 200
        assert payload == {"reminders": [], "count": 0}
    finally:
        bridge.stop()


def test_drafts_channel_endpoint(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        # A channel draft is keyed by the channel name, and the list
        # resolves its name to the target itself — no registry lookup.
        status, _, payload = _json(
            bridge,
            "/api/drafts",
            method="POST",
            body={"kind": "channel", "target": "general", "text": "hello channel"},
        )
        assert status == 200
        assert payload["ok"] is True
        assert payload["draft"]["kind"] == "channel"
        assert payload["draft"]["target"] == "general"
        assert payload["draft"]["text"] == "hello channel"

        status, _, payload = _json(bridge, "/api/drafts")
        assert status == 200
        assert payload["count"] == 1
        entry = payload["drafts"][0]
        assert entry["kind"] == "channel"
        assert entry["name"] == "general"

        # Deleting a channel draft works the same as the other kinds.
        status, _, payload = _json(
            bridge,
            "/api/drafts/delete",
            method="POST",
            body={"kind": "channel", "target": "general"},
        )
        assert status == 200
        assert payload == {"ok": True, "deleted": True}
        status, _, payload = _json(bridge, "/api/drafts")
        assert payload == {"drafts": [], "count": 0}
    finally:
        bridge.stop()


def test_drafts_endpoints_reject_bad_input(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        for body in (
            {},
            {"kind": "peer", "target": PEER_HASH},
            {"kind": "peer", "text": "hi"},
            {"kind": "bogus", "target": PEER_HASH, "text": "hi"},
            {"kind": "peer", "target": "not-a-hash", "text": "hi"},
            {"kind": "peer", "target": PEER_HASH, "text": "   "},
            {"kind": "peer", "target": PEER_HASH, "text": 42},
        ):
            status, _, payload = _json(
                bridge, "/api/drafts", method="POST", body=body
            )
            assert status == 400, body
            assert "error" in payload
        for body in (
            {},
            {"kind": "peer"},
            {"target": PEER_HASH},
            {"kind": "bogus", "target": "general"},
        ):
            status, _, payload = _json(
                bridge, "/api/drafts/delete", method="POST", body=body
            )
            assert status == 400, body
            assert "error" in payload
    finally:
        bridge.stop()


def test_drafts_endpoints_without_data_dir(bridge):
    # A dataless node has no drafts path: edits are refused, but the
    # list still answers empty instead of failing the console poll.
    status, _, payload = _json(bridge, "/api/drafts")
    assert status == 200
    assert payload == {"drafts": [], "count": 0}
    status, _, payload = _json(
        bridge,
        "/api/drafts",
        method="POST",
        body={"kind": "peer", "target": PEER_HASH, "text": "hi"},
    )
    assert status == 503
    status, _, payload = _json(
        bridge,
        "/api/drafts/delete",
        method="POST",
        body={"kind": "peer", "target": PEER_HASH},
    )
    assert status == 503


def test_muted_conversations_endpoints(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        status, _, payload = _json(bridge, "/api/messages/muted")
        assert status == 200
        assert payload == {"peers": [], "groups": [], "channels": []}

        status, _, payload = _json(
            bridge,
            "/api/messages/mute",
            method="POST",
            body={"peer": "alice"},
        )
        assert status == 200
        assert payload["mute"]["target"] == PEER_HASH
        assert payload["mute"]["group"] is False
        assert payload["mute"]["muted"] is True
        assert payload["mute"]["changed"] is True
        # set_muted returns the unread counts after a change, so the
        # console can hold the restored count when unmuting.
        assert payload["mute"]["unread"]["per_peer"] == {}
        assert payload["mute"]["unread"]["total"] == 0
        assert isinstance(payload["mute"]["unread"]["revision"], int)
        status, _, payload = _json(bridge, "/api/messages/muted")
        assert payload == {"peers": [PEER_HASH], "groups": [], "channels": []}

        # Muting again is a no-op; the unread count for the muted
        # peer is already 0.
        status, _, payload = _json(
            bridge,
            "/api/messages/mute",
            method="POST",
            body={"peer": "alice"},
        )
        assert payload["mute"]["changed"] is False

        # A group mute is keyed by group id, resolved by name.
        status, _, payload = _json(
            bridge,
            "/api/messages/mute",
            method="POST",
            body={"group": "crew"},
        )
        assert status == 200
        assert payload["mute"]["group"] is True
        assert payload["mute"]["muted"] is True
        status, _, payload = _json(bridge, "/api/messages/muted")
        assert payload["peers"] == [PEER_HASH]
        assert payload["channels"] == []
        assert len(payload["groups"]) == 1

        # A channel mute is keyed by name, with no store lookup.
        status, _, payload = _json(
            bridge,
            "/api/messages/mute",
            method="POST",
            body={"channel": "general"},
        )
        assert status == 200
        assert payload["mute"]["target"] == "general"
        assert payload["mute"]["channel"] is True
        assert payload["mute"]["group"] is False
        assert payload["mute"]["muted"] is True
        assert payload["mute"]["changed"] is True
        status, _, payload = _json(bridge, "/api/messages/muted")
        assert payload["channels"] == ["general"]

        # Unmuting the channel drops it from the list.
        status, _, payload = _json(
            bridge,
            "/api/messages/mute",
            method="POST",
            body={"channel": "general", "muted": False},
        )
        assert status == 200
        assert payload["mute"]["muted"] is False
        assert payload["mute"]["changed"] is True
        status, _, payload = _json(bridge, "/api/messages/muted")
        assert payload["channels"] == []

        # Unmuting restores counting, including messages that arrived
        # while muted.
        node.inbox.append(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "hello while muted",
                "sent_at": 1000.0,
                "received_at": 1001.0,
                "id": "msg-muted",
            }
        )
        assert bridge.unread()["total"] == 0
        status, _, payload = _json(
            bridge,
            "/api/messages/mute",
            method="POST",
            body={"peer": "alice", "muted": False},
        )
        assert status == 200
        assert payload["mute"]["muted"] is False
        assert payload["mute"]["changed"] is True
        # The unmute answer carries the restored count: the message
        # that arrived while muted counts again.
        assert payload["mute"]["unread"]["per_peer"] == {PEER_HASH: 1}
        assert payload["mute"]["unread"]["total"] == 1
        counts = bridge.unread()
        assert counts["per_peer"] == {PEER_HASH: 1}
        assert counts["total"] == 1
        status, _, payload = _json(bridge, "/api/messages/muted")
        assert payload["peers"] == []
    finally:
        bridge.stop()


def test_unmute_names_restored_mail_in_the_unread_event(node, tmp_path):
    # Issue #411 repro 3: an unmute restores mail that arrived while
    # muted, and every open console must hold it instead of
    # auto-reading it as new mail. The published unread event names
    # the restored conversation, so the other consoles can tell
    # restored counts from new arrivals.
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        group_id = node.groups.list_groups()[0]["id"]

        def unread_events():
            return [
                event
                for event in bridge.bus.replay(0)
                if event["type"] == "unread"
            ]

        # Muting publishes an ordinary unread event: nothing restored.
        _json(bridge, "/api/messages/mute", method="POST", body={"peer": "alice"})
        assert unread_events()[-1].get("restored") is None
        _json(bridge, "/api/messages/mute", method="POST", body={"group": "crew"})
        assert unread_events()[-1].get("restored") is None

        # Mail arrives while muted, for the DM and for the group.
        _inbound_record(
            node,
            id="dm-muted",
            text="hello while muted",
            sent_at=1000.0,
            received_at=1001.0,
        )
        _inbound_record(
            node,
            id="group-muted",
            text="crew chat while muted",
            sent_at=1002.0,
            received_at=1003.0,
            kind="group",
            group=group_id,
            group_name="crew",
        )
        assert bridge.unread()["total"] == 0

        # Unmuting the DM names only its restored mail.
        _json(
            bridge,
            "/api/messages/mute",
            method="POST",
            body={"peer": "alice", "muted": False},
        )
        assert unread_events()[-1]["restored"] == [
            {"target": PEER_HASH, "unread": 1}
        ]

        # Unmuting the group names its restored posts, under the
        # current group id the console holds the badge under.
        _json(
            bridge,
            "/api/messages/mute",
            method="POST",
            body={"group": "crew", "muted": False},
        )
        assert unread_events()[-1]["restored"] == [
            {"target": group_id, "unread": 1}
        ]

        # Unignoring a group restores its posts the same way.
        _json(
            bridge, "/api/groups/ignore", method="POST", body={"group": "crew"}
        )
        assert unread_events()[-1].get("restored") is None
        _json(
            bridge, "/api/groups/unignore", method="POST", body={"group": "crew"}
        )
        assert unread_events()[-1]["restored"] == [
            {"target": group_id, "unread": 1}
        ]
    finally:
        bridge.stop()


def test_muted_conversations_endpoint_rejects_bad_input(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        for body in (
            {},
            {"peer": "alice", "group": "crew"},
            {"peer": "alice", "channel": "general"},
            {"group": "crew", "channel": "general"},
            {"peer": "alice", "muted": "yes"},
            {"peer": "nobody"},
            {"peer": "  "},
            {"group": "nobody"},
            {"channel": "  "},
            {"channel": "bad name!"},
        ):
            status, _, payload = _json(
                bridge, "/api/messages/mute", method="POST", body=body
            )
            assert status == 400, body
            assert "error" in payload
    finally:
        bridge.stop()


def test_muted_conversations_rejects_corrupt_file(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        node.inbox.append(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "hello",
                "sent_at": 1000.0,
                "received_at": 1001.0,
                "id": "msg-1",
            }
        )
        assert bridge.unread()["total"] == 1
        (tmp_path / "muted.json").write_text("{not json", encoding="utf-8")
        status, _, payload = _json(
            bridge,
            "/api/messages/mute",
            method="POST",
            body={"peer": "alice"},
        )
        assert status == 503
        assert "unusable" in payload["error"]["message"]
        status, _, payload = _json(bridge, "/api/messages/muted")
        assert status == 503
        # The counts fail open: a corrupt file never hides the badge,
        # it just counts the conversation as unmuted.
        assert bridge.unread()["total"] == 1
    finally:
        bridge.stop()


def test_pinned_conversations_endpoints(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        status, _, payload = _json(bridge, "/api/conversations/pinned")
        assert status == 200
        assert payload == {"pinned": [], "count": 0}

        status, _, payload = _json(
            bridge,
            "/api/conversations/pin",
            method="POST",
            body={"peer": "alice"},
        )
        assert status == 200
        assert payload["pin"]["target"] == PEER_HASH
        assert payload["pin"]["kind"] == "peer"
        assert payload["pin"]["pinned"] is True
        assert payload["pin"]["changed"] is True
        status, _, payload = _json(bridge, "/api/conversations/pinned")
        assert payload["count"] == 1
        assert payload["pinned"][0]["kind"] == "peer"
        assert payload["pinned"][0]["target"] == PEER_HASH
        assert payload["pinned"][0]["label"].startswith("alice")

        # Pinning again is a no-op.
        status, _, payload = _json(
            bridge,
            "/api/conversations/pin",
            method="POST",
            body={"peer": "alice"},
        )
        assert payload["pin"]["changed"] is False

        # A group pin is keyed by group id, resolved by name.
        status, _, payload = _json(
            bridge,
            "/api/conversations/pin",
            method="POST",
            body={"group": "crew"},
        )
        assert status == 200
        assert payload["pin"]["kind"] == "group"
        assert payload["pin"]["pinned"] is True
        status, _, payload = _json(bridge, "/api/conversations/pinned")
        assert payload["count"] == 2
        # Newest pin first.
        assert payload["pinned"][0]["kind"] == "group"
        assert payload["pinned"][1]["kind"] == "peer"

        # Unpinning restores the empty list.
        status, _, payload = _json(
            bridge,
            "/api/conversations/pin",
            method="POST",
            body={"peer": "alice", "pinned": False},
        )
        assert status == 200
        assert payload["pin"]["pinned"] is False
        assert payload["pin"]["changed"] is True
        status, _, payload = _json(
            bridge,
            "/api/conversations/pin",
            method="POST",
            body={"group": "crew", "pinned": False},
        )
        assert payload["pin"]["changed"] is True
        status, _, payload = _json(bridge, "/api/conversations/pinned")
        assert payload == {"pinned": [], "count": 0}
    finally:
        bridge.stop()


def test_pinned_conversations_endpoint_rejects_bad_input(node, tmp_path):
    from tau import cli
    from tau.config import TauConfig

    # The channel case resolves membership through the channel app,
    # which needs an identity on disk first.
    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        for body in (
            {},
            {"peer": "alice", "group": "crew"},
            {"peer": "alice", "pinned": "yes"},
            {"peer": "nobody"},
            {"peer": "  "},
            {"group": "nobody"},
            {"channel": "general"},
        ):
            status, _, payload = _json(
                bridge, "/api/conversations/pin", method="POST", body=body
            )
            assert status == 400, body
            assert "error" in payload
    finally:
        bridge.stop()


def test_pinned_conversations_rejects_corrupt_file(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        (tmp_path / "pinned.json").write_text("{not json", encoding="utf-8")
        status, _, payload = _json(
            bridge,
            "/api/conversations/pin",
            method="POST",
            body={"peer": "alice"},
        )
        assert status == 503
        assert "unusable" in payload["error"]["message"]
        status, _, payload = _json(bridge, "/api/conversations/pinned")
        assert status == 503
        # The list endpoints fail open: a corrupt file never hides or
        # reorders the console, it just reads every conversation as
        # unpinned.
        status, _, payload = _json(bridge, "/api/peers")
        assert status == 200
        assert payload["peers"][0]["pinned"] is False
    finally:
        bridge.stop()


def test_list_endpoints_carry_pinned_flag(node, tmp_path):
    from tau import cli
    from tau.channels import init_db as init_channel_db
    from tau.channels import join_channel
    from tau.config import TauConfig

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    conn = init_channel_db(node.config.channels_db_path)
    try:
        join_channel(conn, "general")
        join_channel(conn, "random")
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/peers")
        assert status == 200
        assert payload["peers"][0]["pinned"] is False
        status, _, payload = _json(bridge, "/api/groups")
        assert status == 200
        assert payload["groups"][0]["pinned"] is False
        status, _, payload = _json(bridge, "/api/channels")
        assert status == 200
        assert [item["channel"] for item in payload["channels"]] == [
            "random",
            "general",
        ]
        assert [item["pinned"] for item in payload["channels"]] == [False, False]

        # Pinning the older channel pulls it to the front everywhere.
        status, _, payload = _json(
            bridge,
            "/api/conversations/pin",
            method="POST",
            body={"peer": "alice"},
        )
        assert status == 200
        status, _, payload = _json(
            bridge,
            "/api/conversations/pin",
            method="POST",
            body={"group": "crew"},
        )
        assert status == 200
        status, _, payload = _json(
            bridge,
            "/api/conversations/pin",
            method="POST",
            body={"channel": "general"},
        )
        assert status == 200
        status, _, payload = _json(bridge, "/api/peers")
        assert payload["peers"][0]["pinned"] is True
        status, _, payload = _json(bridge, "/api/groups")
        assert payload["groups"][0]["pinned"] is True
        status, _, payload = _json(bridge, "/api/channels")
        assert [item["channel"] for item in payload["channels"]] == [
            "general",
            "random",
        ]
        assert [item["pinned"] for item in payload["channels"]] == [True, False]
    finally:
        bridge.stop()


def test_tan_mute_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(conn, identity, "hello from tan", name="tester", now=1000)
        theirs = create_post(conn, other, "bob's update", name="bob", now=1001)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/muted")
        assert status == 200
        assert payload == {"muted": [], "count": 0}

        status, _, payload = _json(
            bridge,
            "/api/tan/mute",
            method="POST",
            body={"target": theirs["author"]},
        )
        assert status == 200
        assert payload["mute"]["author"] == theirs["author"]
        first_muted_at = payload["mute"]["muted_at"]

        # The muted author's posts leave the timeline at once.
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["hello from tan"]

        status, _, payload = _json(bridge, "/api/tan/muted")
        assert status == 200
        assert payload["count"] == 1
        assert payload["muted"][0]["name"] == "bob"
        assert payload["muted"][0]["posts"] == 1

        # Re-muting is a no-op: the original mute time is kept.
        status, _, payload = _json(
            bridge,
            "/api/tan/mute",
            method="POST",
            body={"target": "bob"},
        )
        assert status == 200
        assert payload["mute"]["muted_at"] == first_muted_at
        status, _, relisted = _json(bridge, "/api/tan/muted")
        assert relisted["muted"][0]["muted_at"] == first_muted_at

        status, _, payload = _json(
            bridge,
            "/api/tan/unmute",
            method="POST",
            body={"target": "bob"},
        )
        assert status == 200
        assert payload["mute"]["unmuted"] is True

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert [item["text"] for item in payload["posts"]] == [
            "bob's update",
            "hello from tan",
        ]

        # Unmuting again reports that nothing was muted.
        status, _, payload = _json(
            bridge,
            "/api/tan/unmute",
            method="POST",
            body={"target": theirs["author"]},
        )
        assert payload["mute"]["unmuted"] is False

        # Muting yourself is refused, like the CLI.
        status, _, payload = _json(
            bridge,
            "/api/tan/mute",
            method="POST",
            body={"target": mine["author"]},
        )
        assert status == 400
        assert "cannot mute yourself" in payload["error"]["message"]

        # Unknown authors and missing fields are rejected.
        status, _, payload = _json(
            bridge,
            "/api/tan/mute",
            method="POST",
            body={"target": "nobody-here"},
        )
        assert status == 400
        status, _, payload = _json(
            bridge, "/api/tan/mute", method="POST", body={}
        )
        assert status == 400
        assert "field 'target' is required" in payload["error"]["message"]

        # Keyword mutes hide matching posts and list back.
        status, _, payload = _json(
            bridge,
            "/api/tan/mute-keyword",
            method="POST",
            body={"keyword": "hello"},
        )
        assert status == 200
        assert payload["mute"]["keyword"] == "hello"

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert [item["text"] for item in payload["posts"]] == ["bob's update"]

        status, _, payload = _json(bridge, "/api/tan/muted-keywords")
        assert status == 200
        assert payload["count"] == 1
        assert payload["muted_keywords"][0]["display"] == "hello"

        status, _, payload = _json(
            bridge,
            "/api/tan/unmute-keyword",
            method="POST",
            body={"keyword": "HELLO"},
        )
        assert status == 200
        assert payload["mute"]["unmuted"] is True

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert [item["text"] for item in payload["posts"]] == [
            "bob's update",
            "hello from tan",
        ]

        status, _, payload = _json(
            bridge,
            "/api/tan/mute-keyword",
            method="POST",
            body={"keyword": "   "},
        )
        assert status == 400
        assert "field 'keyword' is required" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_lists_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    third = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(conn, identity, "hello from tan", name="tester", now=1000)
        theirs = create_post(conn, other, "bob's update", name="bob", now=1001)
        hers = create_post(conn, third, "carol's update", name="carol", now=1002)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/lists")
        assert status == 200
        assert payload == {"lists": [], "count": 0}

        status, _, payload = _json(
            bridge, "/api/tan/lists", method="POST", body={"name": "Friends"}
        )
        assert status == 200
        assert payload["list"]["name"] == "Friends"
        assert payload["list"]["members"] == 0

        # A duplicate name (any case) is a client error.
        status, _, payload = _json(
            bridge, "/api/tan/lists", method="POST", body={"name": "friends"}
        )
        assert status == 400

        # Members resolve by name, like the CLI; bob is followed here
        # only after the follow below, so the first add notes that.
        status, _, payload = _json(
            bridge,
            "/api/tan/lists/add",
            method="POST",
            body={"name": "Friends", "target": "bob"},
        )
        assert status == 200
        assert payload["member"]["author"] == theirs["author"]
        assert payload["member"]["added"] is True
        assert payload["member"]["followed"] is False

        status, _, payload = _json(
            bridge,
            "/api/tan/lists/add",
            method="POST",
            body={"name": "Friends", "target": hers["author"]},
        )
        assert status == 200
        assert payload["member"]["added"] is True

        status, _, payload = _json(bridge, "/api/tan/lists/members?name=Friends")
        assert status == 200
        assert payload["name"] == "Friends"
        assert payload["count"] == 2
        assert {item["author"] for item in payload["members"]} == {
            theirs["author"],
            hers["author"],
        }

        # The list timeline reads only its members.
        status, _, payload = _json(bridge, "/api/tan/timeline?list=Friends")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "carol's update",
            "bob's update",
        ]

        status, _, payload = _json(
            bridge,
            "/api/tan/lists/remove",
            method="POST",
            body={"name": "Friends", "target": "carol"},
        )
        assert status == 200
        assert payload["member"]["removed"] is True

        status, _, payload = _json(bridge, "/api/tan/timeline?list=Friends")
        assert [item["text"] for item in payload["posts"]] == ["bob's update"]

        # Unknown lists and missing fields are client errors.
        status, _, payload = _json(bridge, "/api/tan/timeline?list=Nope")
        assert status == 400
        status, _, payload = _json(bridge, "/api/tan/lists/members?name=Nope")
        assert status == 400
        status, _, payload = _json(
            bridge, "/api/tan/lists", method="POST", body={}
        )
        assert status == 400
        assert "field 'name' is required" in payload["error"]["message"]

        status, _, payload = _json(
            bridge,
            "/api/tan/lists/delete",
            method="POST",
            body={"name": "Friends"},
        )
        assert status == 200
        assert payload["list"]["deleted"] is True
        status, _, payload = _json(bridge, "/api/tan/lists")
        assert payload == {"lists": [], "count": 0}
    finally:
        bridge.stop()


def test_tan_follow_tag_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, identity, "hello from tan", name="tester", now=1000)
        create_post(
            conn, other, "bob's #gardening update", name="bob", now=1001
        )
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/followed-tags")
        assert status == 200
        assert payload == {"followed_tags": [], "count": 0}

        # A stranger's tagged post stays out of the plain timeline.
        status, _, payload = _json(bridge, "/api/tan/timeline")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["hello from tan"]

        status, _, payload = _json(
            bridge,
            "/api/tan/follow-tag",
            method="POST",
            body={"tag": "#gardening"},
        )
        assert status == 200
        assert payload["follow"]["tag"] == "gardening"
        first_followed_at = payload["follow"]["added_at"]

        # The followed tag pulls the post into the timeline at once.
        status, _, payload = _json(bridge, "/api/tan/timeline")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "bob's #gardening update",
            "hello from tan",
        ]

        status, _, payload = _json(bridge, "/api/tan/followed-tags")
        assert status == 200
        assert payload["count"] == 1
        assert payload["followed_tags"][0]["display"] == "#gardening"

        # Re-following is a no-op: the original follow time is kept.
        status, _, payload = _json(
            bridge,
            "/api/tan/follow-tag",
            method="POST",
            body={"tag": "GARDENING"},
        )
        assert status == 200
        assert payload["follow"]["added_at"] == first_followed_at
        status, _, relisted = _json(bridge, "/api/tan/followed-tags")
        assert relisted["followed_tags"][0]["added_at"] == first_followed_at

        status, _, payload = _json(
            bridge,
            "/api/tan/unfollow-tag",
            method="POST",
            body={"tag": "gardening"},
        )
        assert status == 200
        assert payload["follow"]["unfollowed"] is True

        status, _, payload = _json(bridge, "/api/tan/timeline")
        assert [item["text"] for item in payload["posts"]] == ["hello from tan"]

        # Unfollowing again reports that nothing was followed.
        status, _, payload = _json(
            bridge,
            "/api/tan/unfollow-tag",
            method="POST",
            body={"tag": "gardening"},
        )
        assert payload["follow"]["unfollowed"] is False

        # Missing fields and invalid tags are rejected.
        status, _, payload = _json(
            bridge, "/api/tan/follow-tag", method="POST", body={}
        )
        assert status == 400
        assert "field 'tag' is required" in payload["error"]["message"]
        status, _, payload = _json(
            bridge,
            "/api/tan/follow-tag",
            method="POST",
            body={"tag": "not a tag!!"},
        )
        assert status == 400
        assert "not a tag" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_mute_tag_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, identity, "hello from tan", name="tester", now=1000)
        create_post(
            conn, identity, "my #gardening update", name="tester", now=1001
        )
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/muted-tags")
        assert status == 200
        assert payload == {"muted_tags": [], "count": 0}

        # Both posts are in the plain timeline before the mute.
        status, _, payload = _json(bridge, "/api/tan/timeline")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "my #gardening update",
            "hello from tan",
        ]

        status, _, payload = _json(
            bridge,
            "/api/tan/mute-tag",
            method="POST",
            body={"tag": "#gardening"},
        )
        assert status == 200
        assert payload["mute"]["tag"] == "gardening"
        first_muted_at = payload["mute"]["added_at"]

        # The muted tag drops the post from the timeline at once.
        status, _, payload = _json(bridge, "/api/tan/timeline")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["hello from tan"]

        status, _, payload = _json(bridge, "/api/tan/muted-tags")
        assert status == 200
        assert payload["count"] == 1
        assert payload["muted_tags"][0]["display"] == "#gardening"

        # Re-muting is a no-op: the original mute time is kept.
        status, _, payload = _json(
            bridge,
            "/api/tan/mute-tag",
            method="POST",
            body={"tag": "GARDENING"},
        )
        assert status == 200
        assert payload["mute"]["added_at"] == first_muted_at
        status, _, relisted = _json(bridge, "/api/tan/muted-tags")
        assert relisted["muted_tags"][0]["added_at"] == first_muted_at

        status, _, payload = _json(
            bridge,
            "/api/tan/unmute-tag",
            method="POST",
            body={"tag": "gardening"},
        )
        assert status == 200
        assert payload["mute"]["unmuted"] is True

        status, _, payload = _json(bridge, "/api/tan/timeline")
        assert [item["text"] for item in payload["posts"]] == [
            "my #gardening update",
            "hello from tan",
        ]

        # Unmuting again reports that nothing was muted.
        status, _, payload = _json(
            bridge,
            "/api/tan/unmute-tag",
            method="POST",
            body={"tag": "gardening"},
        )
        assert payload["mute"]["unmuted"] is False

        # Missing fields and invalid tags are rejected.
        status, _, payload = _json(
            bridge, "/api/tan/mute-tag", method="POST", body={}
        )
        assert status == 400
        assert "field 'tag' is required" in payload["error"]["message"]
        status, _, payload = _json(
            bridge,
            "/api/tan/mute-tag",
            method="POST",
            body={"tag": "not a tag!!"},
        )
        assert status == 400
        assert "not a tag" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_hidden_annotates_own_posts_and_respects_blocklist(tmp_path):
    import RNS

    from tau.blocklist import BlockList
    from tau.config import TauConfig
    from tau.identity import destination_hash
    from tau.tangents.tan import TanApp, create_post, hide_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(conn, identity, "my own words", name="tester", now=1000)
        theirs = create_post(conn, other, "blocked words", name="bob", now=1001)
        hide_post(conn, mine["hash"])
        hide_post(conn, theirs["hash"])
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    node.blocklist = BlockList(blocked=frozenset({destination_hash(other).hex()}))
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The blocked author's hidden post is not shown in full; the
        # viewer's own hidden post keeps its owner annotation.
        status, _, payload = _json(bridge, "/api/tan/hidden")
        assert status == 200
        assert [item["hash"] for item in payload["posts"]] == [mine["hash"]]
        assert payload["posts"][0]["own"] is True
        assert payload["next_offset"] is None

        # An unreadable block-list hides everything, as the timeline does.
        node.blocklist = BlockList(block_all=True)
        status, _, payload = _json(bridge, "/api/tan/hidden")
        assert status == 200
        assert payload["posts"] == []
        assert payload["next_offset"] is None
    finally:
        bridge.stop()


def test_tan_hide_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(conn, identity, "hello from tan", name="tester", now=1000)
        theirs = create_post(conn, other, "bob's update", name="bob", now=1001)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/hidden")
        assert status == 200
        assert payload == {"posts": [], "count": 0, "next_offset": None}

        status, _, payload = _json(
            bridge,
            "/api/tan/hide",
            method="POST",
            body={"target": theirs["hash"][:16]},
        )
        assert status == 200
        assert payload["hide"]["hash"] == theirs["hash"]
        assert payload["hide"]["already_hidden"] is False
        first_hidden_at = payload["hide"]["hidden_at"]

        # The hidden post leaves the timeline at once, and the hidden
        # list shows it newest first.
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["hello from tan"]

        status, _, payload = _json(bridge, "/api/tan/hidden")
        assert status == 200
        assert payload["count"] == 1
        assert payload["posts"][0]["hash"] == theirs["hash"]
        assert payload["posts"][0]["text"] == "bob's update"
        assert payload["next_offset"] is None

        # Re-hiding is a no-op: the original hide time is kept.
        status, _, payload = _json(
            bridge,
            "/api/tan/hide",
            method="POST",
            body={"target": theirs["hash"]},
        )
        assert status == 200
        assert payload["hide"]["already_hidden"] is True
        assert payload["hide"]["hidden_at"] == first_hidden_at

        status, _, payload = _json(
            bridge,
            "/api/tan/unhide",
            method="POST",
            body={"target": theirs["hash"][:16]},
        )
        assert status == 200
        assert payload["hide"]["unhidden"] is True

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert [item["text"] for item in payload["posts"]] == [
            "bob's update",
            "hello from tan",
        ]

        # Unhiding again reports that nothing was hidden.
        status, _, payload = _json(
            bridge,
            "/api/tan/unhide",
            method="POST",
            body={"target": theirs["hash"]},
        )
        assert payload["hide"]["unhidden"] is False

        # Hiding your own post works — hiding is per-post, not per-author.
        status, _, payload = _json(
            bridge,
            "/api/tan/hide",
            method="POST",
            body={"target": mine["hash"]},
        )
        assert status == 200
        assert payload["hide"]["hash"] == mine["hash"]
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert [item["text"] for item in payload["posts"]] == ["bob's update"]

        # Unknown refs and missing fields are rejected.
        status, _, payload = _json(
            bridge,
            "/api/tan/hide",
            method="POST",
            body={"target": "0" * 64},
        )
        assert status == 400
        status, _, payload = _json(
            bridge, "/api/tan/hide", method="POST", body={}
        )
        assert status == 400
        assert "field 'target' is required" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_peers_block_corrupt_list_refuses(tmp_path):
    from tau.config import TauConfig

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    (tmp_path / "blocklist.json").write_text("{not json", encoding="utf-8")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge, "/api/peers/block", method="POST", body={"target": "alice"}
        )
        assert status == 503
        assert "unusable" in payload["error"]["message"]
        # Nothing was persisted over the corrupt file, and the running
        # node fails closed until the owner repairs it.
        assert (tmp_path / "blocklist.json").read_text() == "{not json"
        assert node.blocklist is not None
        assert node.blocklist.block_all

        # A node still fail-closed in memory accepts edits again once the
        # owner repairs the file — and the repair clears the cached
        # block-all, so enforcement matches the file.
        node.blocklist = BlockList(block_all=True)
        (tmp_path / "blocklist.json").write_text(
            '{"blocked": []}', encoding="utf-8"
        )
        status, _, payload = _json(
            bridge, "/api/peers/block", method="POST", body={"target": "alice"}
        )
        assert status == 200
        assert payload["block"]["blocked"] is True
        assert node.blocklist is not None
        assert not node.blocklist.block_all
        assert node.blocklist.is_blocked(PEER_HASH)
        # The saved file holds the new entry, not just the cache.
        assert json.loads((tmp_path / "blocklist.json").read_text()) == {
            "blocked": [PEER_HASH]
        }
    finally:
        bridge.stop()


def test_peers_block_keeps_cli_edits(tmp_path):
    from tau.config import TauConfig

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    # The CLI blocked someone else while this node was running; the
    # node's cached list predates that edit.
    (tmp_path / "blocklist.json").write_text(
        json.dumps({"blocked": [OTHER_HASH]}), encoding="utf-8"
    )
    node.blocklist = BlockList()
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge, "/api/peers/block", method="POST", body={"target": "alice"}
        )
        assert status == 200
        # The CLI's entry survives: the edit is derived from the file,
        # not from the stale cache.
        assert json.loads((tmp_path / "blocklist.json").read_text()) == {
            "blocked": sorted([OTHER_HASH, PEER_HASH])
        }
        assert node.blocklist is not None
        assert node.blocklist.is_blocked(OTHER_HASH)
        assert node.blocklist.is_blocked(PEER_HASH)
    finally:
        bridge.stop()


def test_tan_search_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import (
        create_delete,
        create_post,
        create_reply,
        init_db,
    )

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(
            conn, identity, "hello from the mesh", name="tester", now=1000
        )
        create_post(conn, identity, "another MESH note", name="tester", now=1001)
        create_post(conn, other, "unrelated weather chat", name="other", now=1002)
        # A reply is not a post: its text never matches, even on keyword.
        create_reply(
            conn, other, mine["hash"], "mesh reply", name="other", now=1003
        )
        # A deleted post never matches either.
        doomed = create_post(
            conn, other, "mesh post to delete", name="other", now=1004
        )
        create_delete(conn, other, doomed["hash"], name="other", now=1005)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # Case-insensitive, newest first.
        status, _, payload = _json(bridge, "/api/tan/search?q=MESH")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "another MESH note",
            "hello from the mesh",
        ]
        assert payload["count"] == 2

        status, _, payload = _json(bridge, "/api/tan/search?q=mesh&limit=1")
        assert status == 200
        assert payload["count"] == 1
        assert payload["posts"][0]["text"] == "another MESH note"

        # An explicit author narrows the search to their posts.
        status, _, payload = _json(
            bridge, f"/api/tan/search?q=mesh&author={mine['author']}"
        )
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "another MESH note",
            "hello from the mesh",
        ]

        status, _, payload = _json(
            bridge, f"/api/tan/search?q=weather&author={mine['author']}"
        )
        assert status == 200
        assert payload["posts"] == []

        # An empty query and an unknown author are client errors.
        status, _, payload = _json(bridge, "/api/tan/search?q=%20%20")
        assert status == 400
        assert "must not be empty" in payload["error"]["message"]

        status, _, payload = _json(bridge, "/api/tan/search?q=mesh&author=nobody")
        assert status == 400
        assert "unknown author" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_search_hides_blocked_and_muted(tmp_path):
    from types import SimpleNamespace

    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import (
        create_post,
        create_quote,
        init_db,
        mute,
        mute_keyword,
    )

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    blocked_id = RNS.Identity()
    muted_id = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, identity, "mesh from me", name="tester", now=1000)
        blocked_author = create_post(
            conn, blocked_id, "mesh from blocked", name="blocked", now=1001
        )["author"]
        muted_author = create_post(
            conn, muted_id, "mesh from muted", name="muted", now=1002
        )["author"]
        mute(conn, muted_author, name="muted", now=1003)
        # Searching for a muted keyword still finds it: search is explicit.
        create_post(conn, identity, "weather report", name="tester", now=1004)
        mute_keyword(conn, "weather", now=1005)
        # A quote of a blocked author's post still matches on its own
        # text, but the embedded original must stay redacted.
        blocked_original = create_post(
            conn, blocked_id, "blocked original words", name="blocked", now=1006
        )
        create_quote(
            conn,
            identity,
            blocked_original["hash"],
            "mesh quote",
            name="tester",
            now=1007,
        )
    finally:
        conn.close()

    node.blocklist = SimpleNamespace(
        blocked=frozenset({blocked_author}), block_all=False
    )
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/search?q=mesh")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "mesh quote",
            "mesh from me",
        ]
        # The quote matches on its own text, but its blocked original
        # stays redacted: no name, no words.
        quoted = payload["posts"][0]["quoted"]
        assert quoted["blocked"] is True
        assert quoted["name"] == ""
        assert quoted["text"] == ""
        assert "blocked original" not in json.dumps(payload)

        # An explicit author search still cannot surface a blocked author.
        status, _, payload = _json(
            bridge, f"/api/tan/search?q=mesh&author={blocked_author}"
        )
        assert status == 200
        assert payload["posts"] == []

        # ... but it can surface a muted one: that is a direct lookup.
        status, _, payload = _json(
            bridge, f"/api/tan/search?q=mesh&author={muted_author}"
        )
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["mesh from muted"]

        status, _, payload = _json(bridge, "/api/tan/search?q=weather")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["weather report"]

        # A block-list that failed closed hides every result.
        node.blocklist = SimpleNamespace(blocked=frozenset(), block_all=True)
        status, _, payload = _json(bridge, "/api/tan/search?q=mesh")
        assert status == 200
        assert payload["posts"] == []
    finally:
        bridge.stop()


def test_tan_mentions_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import (
        TanApp,
        create_delete,
        create_post,
        create_reply,
        init_db,
    )

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(
            conn, identity, "original thought", name="tester", now=1000
        )
        reply = create_reply(
            conn, other, mine["hash"], "seconded", name="other", now=1001
        )
        # A reply to my own post from myself is not a mention of me.
        create_reply(
            conn, identity, mine["hash"], "talking to myself", name="tester", now=1002
        )
        # A reply to someone else's post mentions them, not me.
        theirs = create_post(conn, other, "bob's own post", name="other", now=1003)
        create_reply(
            conn, identity, theirs["hash"], "not a mention", name="tester", now=1004
        )
        # A reply its author deleted never appears.
        doomed = create_reply(
            conn, other, mine["hash"], "take it back", name="other", now=1005
        )
        create_delete(conn, other, doomed["hash"], name="other", now=1006)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/mentions")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["seconded"]
        assert payload["count"] == 1
        # Each mention carries its parent, so the reader sees what was answered.
        assert payload["posts"][0]["reply_to"]["hash"] == mine["hash"]
        assert payload["posts"][0]["reply_to"]["text"] == "original thought"
        assert reply["hash"] == payload["posts"][0]["hash"]

        status, _, payload = _json(bridge, "/api/tan/mentions?limit=1")
        assert status == 200
        assert payload["count"] == 1
    finally:
        bridge.stop()

    # Without an identity there is nothing of yours to be replied to.
    node.app = None
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/mentions")
        assert status == 400
    finally:
        bridge.stop()


def test_tan_mentions_hide_blocked_and_muted(tmp_path):
    from types import SimpleNamespace

    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import (
        MAX_FEED_POSTS,
        TanApp,
        create_post,
        create_reply,
        init_db,
        mute,
        mute_keyword,
    )

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    blocked_id = RNS.Identity()
    muted_id = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(conn, identity, "my post", name="tester", now=1000)
        # Older visible reply: it must still fill the limit after the
        # newer hidden replies below are paged past.
        create_reply(conn, other, mine["hash"], "friendly reply", name="other", now=1001)
        blocked_author = create_reply(
            conn, blocked_id, mine["hash"], "blocked reply", name="blocked", now=1002
        )["author"]
        # Newer keyword-muted filler spans more than one page, so the
        # older visible reply is reached only by paging past it.
        for index in range(MAX_FEED_POSTS + 5):
            create_reply(
                conn,
                other,
                mine["hash"],
                f"spam filler {index}",
                name="other",
                now=2000 + index,
            )
        muted_author = create_reply(
            conn, muted_id, mine["hash"], "muted author reply", name="muted", now=1003
        )["author"]
        create_reply(conn, other, mine["hash"], "spam keyword reply", name="other", now=1004)
        mute(conn, muted_author, name="muted", now=1005)
        mute_keyword(conn, "spam", now=1006)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    node.blocklist = SimpleNamespace(
        blocked=frozenset({blocked_author}), block_all=False
    )
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/mentions?limit=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["friendly reply"]

        # A block-list that failed closed hides every mention.
        node.blocklist = SimpleNamespace(blocked=frozenset(), block_all=True)
        status, _, payload = _json(bridge, "/api/tan/mentions")
        assert status == 200
        assert payload["posts"] == []
    finally:
        bridge.stop()


def test_tan_mentions_unexpected_failure_is_500(tmp_path, monkeypatch):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, identity, "original thought", name="tester", now=1000)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        import tau.tangents.tan as tan_module

        def _boom(*args, **kwargs):
            raise RuntimeError("database exploded")

        monkeypatch.setattr(tan_module, "mentions", _boom)
        status, _, payload = _json(bridge, "/api/tan/mentions")
        assert status == 500
        assert payload["error"]["message"] == "internal error"
    finally:
        bridge.stop()


def test_tan_saved_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(conn, identity, "save me", name="tester", now=1000)
        theirs = create_post(conn, other, "save me too", name="other", now=1001)
        create_post(conn, other, "not saved", name="other", now=1002)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The timeline flags saved posts so the console can show save
        # state on each card.
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["save me"]["saved"] is False

        # Saving requires a target, and an unknown hash cannot be saved.
        status, _, payload = _json(
            bridge, "/api/tan/save", method="POST", body={}
        )
        assert status == 400
        status, _, payload = _json(
            bridge, "/api/tan/save", method="POST", body={"target": "ff" * 32}
        )
        assert status == 400

        status, _, payload = _json(
            bridge,
            "/api/tan/save",
            method="POST",
            body={"target": mine["hash"][:16]},
        )
        assert status == 200
        assert payload["save"]["hash"] == mine["hash"]
        assert payload["save"]["already_saved"] is False

        # Saving again is a no-op that reports the original save.
        status, _, payload = _json(
            bridge,
            "/api/tan/save",
            method="POST",
            body={"target": mine["hash"]},
        )
        assert status == 200
        assert payload["save"]["already_saved"] is True

        status, _, payload = _json(
            bridge,
            "/api/tan/save",
            method="POST",
            body={"target": theirs["hash"]},
        )
        assert status == 200

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["save me"]["saved"] is True
        assert by_text["save me too"]["saved"] is True
        assert by_text["not saved"]["saved"] is False

        # Saved posts come back newest save first, flagged as saved.
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "save me too",
            "save me",
        ]
        assert all(item["saved"] is True for item in payload["posts"])
        assert all("saved_at" in item for item in payload["posts"])

        # The list pages through older saves instead of stopping at the
        # first page. The resume offset comes from the response: it
        # counts raw rows, so filtering never shifts it.
        status, _, payload = _json(bridge, "/api/tan/saved?limit=1")
        assert [item["text"] for item in payload["posts"]] == ["save me too"]
        assert payload["next_offset"] == 1
        status, _, payload = _json(bridge, "/api/tan/saved?limit=1&offset=1")
        assert [item["text"] for item in payload["posts"]] == ["save me"]
        assert payload["next_offset"] is None
        status, _, payload = _json(bridge, "/api/tan/saved?limit=1&offset=2")
        assert payload["posts"] == []
        assert payload["next_offset"] is None

        # Unsaving removes it from the list; unsaving again is a no-op.
        status, _, payload = _json(
            bridge,
            "/api/tan/unsave",
            method="POST",
            body={"target": theirs["hash"][:16]},
        )
        assert status == 200
        assert payload["save"]["unsaved"] is True
        status, _, payload = _json(
            bridge,
            "/api/tan/unsave",
            method="POST",
            body={"target": theirs["hash"]},
        )
        assert status == 200
        assert payload["save"]["unsaved"] is False

        status, _, payload = _json(bridge, "/api/tan/saved")
        assert [item["text"] for item in payload["posts"]] == ["save me"]

        # A post deleted after being saved leaves the list with the
        # post: deleting purges the save ref along with the post row.
        node.app = TanApp(node.config, identity, name="tester", gossip=False)
        status, _, payload = _json(
            bridge,
            "/api/tan/delete",
            method="POST",
            body={"target": mine["hash"][:16]},
        )
        assert status == 200
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert status == 200
        assert payload["posts"] == []
    finally:
        bridge.stop()


def test_tan_pinned_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(conn, identity, "pin me", name="tester", now=1000)
        theirs = create_post(conn, other, "not mine", name="other", now=1001)
        extra = [
            create_post(
                conn, identity, f"pin me {index}", name="tester", now=1010 + index
            )
            for index in range(10)
        ]
    finally:
        conn.close()
    # The pin owner is the node's own author hash, so the Tan app must
    # be attached before pinning — the same identity authored "mine".
    node.app = TanApp(node.config, identity, name="tester", gossip=False)

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The timeline flags pinned posts so the console can show pin
        # state on each card.
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["pin me"]["pinned"] is False

        # Pinning requires a target, and an unknown hash cannot be pinned.
        status, _, payload = _json(
            bridge, "/api/tan/pin", method="POST", body={}
        )
        assert status == 400
        status, _, payload = _json(
            bridge, "/api/tan/pin", method="POST", body={"target": "ff" * 32}
        )
        assert status == 400

        # Only your own posts can be pinned.
        status, _, payload = _json(
            bridge,
            "/api/tan/pin",
            method="POST",
            body={"target": theirs["hash"]},
        )
        assert status == 400

        status, _, payload = _json(
            bridge,
            "/api/tan/pin",
            method="POST",
            body={"target": mine["hash"][:16]},
        )
        assert status == 200
        assert payload["pin"]["hash"] == mine["hash"]
        assert payload["pin"]["already_pinned"] is False

        # Pinning again is a no-op that reports the original pin.
        status, _, payload = _json(
            bridge,
            "/api/tan/pin",
            method="POST",
            body={"target": mine["hash"]},
        )
        assert status == 200
        assert payload["pin"]["already_pinned"] is True

        # The shelf holds ten posts: the nine extra pins fill it, and
        # an eleventh own post is refused.
        for post in extra[:9]:
            status, _, payload = _json(
                bridge,
                "/api/tan/pin",
                method="POST",
                body={"target": post["hash"]},
            )
            assert status == 200
        status, _, payload = _json(
            bridge,
            "/api/tan/pin",
            method="POST",
            body={"target": extra[9]["hash"]},
        )
        assert status == 400

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["pin me"]["pinned"] is True
        assert by_text["not mine"]["pinned"] is False

        # Pinned posts come back newest pin first, flagged as pinned.
        status, _, payload = _json(bridge, "/api/tan/pinned")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            f"pin me {index}" for index in range(8, -1, -1)
        ] + ["pin me"]
        assert all(item["pinned"] is True for item in payload["posts"])
        assert all("pinned_at" in item for item in payload["posts"])

        # The list pages through older pins instead of stopping at the
        # first page. The resume offset comes from the response: it
        # counts raw rows, so filtering never shifts it.
        status, _, payload = _json(bridge, "/api/tan/pinned?limit=1")
        assert [item["text"] for item in payload["posts"]] == ["pin me 8"]
        assert payload["next_offset"] == 1
        status, _, payload = _json(bridge, "/api/tan/pinned?limit=1&offset=1")
        assert [item["text"] for item in payload["posts"]] == ["pin me 7"]
        assert payload["next_offset"] == 2
        status, _, payload = _json(bridge, "/api/tan/pinned?limit=1&offset=10")
        assert payload["posts"] == []
        assert payload["next_offset"] is None

        # Unpinning removes it from the list; unpinning again is a no-op.
        status, _, payload = _json(
            bridge,
            "/api/tan/unpin",
            method="POST",
            body={"target": extra[0]["hash"][:16]},
        )
        assert status == 200
        assert payload["pin"]["unpinned"] is True
        status, _, payload = _json(
            bridge,
            "/api/tan/unpin",
            method="POST",
            body={"target": extra[0]["hash"]},
        )
        assert status == 200
        assert payload["pin"]["unpinned"] is False

        status, _, payload = _json(bridge, "/api/tan/pinned")
        assert [item["text"] for item in payload["posts"]].count("pin me 0") == 0

        # Other surfaces carry the pinned flag too, so a pin made in
        # one view shows up in the rest: search and top flag it, and a
        # saved pinned post carries its saved flag in the pinned list.
        status, _, payload = _json(
            bridge,
            "/api/tan/save",
            method="POST",
            body={"target": mine["hash"]},
        )
        assert status == 200
        status, _, payload = _json(bridge, "/api/tan/pinned")
        pinned = {item["hash"]: item for item in payload["posts"]}
        assert pinned[mine["hash"]]["saved"] is True
        status, _, payload = _json(bridge, "/api/tan/search?q=pin")
        assert status == 200
        by_hash = {item["hash"]: item for item in payload["posts"]}
        assert by_hash[mine["hash"]]["pinned"] is True
        assert by_hash[extra[9]["hash"]]["pinned"] is False
        status, _, payload = _json(bridge, "/api/tan/top?limit=100")
        assert status == 200
        by_hash = {item["hash"]: item for item in payload["posts"]}
        assert by_hash[mine["hash"]]["pinned"] is True
        assert by_hash[theirs["hash"]]["pinned"] is False

        # A post deleted after being pinned leaves the list with the
        # post: deleting purges the pin ref along with the post row,
        # while the eight other pins stay (extra[0] was unpinned
        # earlier in this test, extra[9] never fit on the shelf).
        status, _, payload = _json(
            bridge,
            "/api/tan/delete",
            method="POST",
            body={"target": mine["hash"][:16]},
        )
        assert status == 200
        status, _, payload = _json(bridge, "/api/tan/pinned")
        assert status == 200
        pinned = {item["hash"]: item for item in payload["posts"]}
        assert mine["hash"] not in pinned
        assert len(payload["posts"]) == 8
    finally:
        bridge.stop()


def test_tan_pin_without_identity_returns_400(tmp_path):
    from tau.config import TauConfig

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # No identity is a permanent setup state, not a temporary
        # outage: the pin fails as a 400 that says how to fix it,
        # like the mentions endpoint does.
        status, _, payload = _json(
            bridge,
            "/api/tan/pin",
            method="POST",
            body={"target": "ab" * 32},
        )
        assert status == 400
        assert "tau init" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_saved_annotations_and_muting(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import (
        create_post,
        create_quote,
        create_reply,
        init_db,
        mute,
        mute_keyword,
        save_post,
        unmute,
        unmute_keyword,
    )

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        original = create_post(
            conn, other, "original thought", name="other", now=1000
        )
        quote = create_quote(
            conn, identity, original["hash"], "worth sharing", name="tester", now=1001
        )
        reply = create_reply(
            conn, identity, original["hash"], "seconded", name="tester", now=1002
        )
        # Distinct save times keep the newest-first order deterministic.
        save_post(conn, quote["hash"], now=2001)
        save_post(conn, reply["hash"], now=2002)
        save_post(conn, original["hash"], now=2003)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A saved quote carries its original, like the timeline does.
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert status == 200
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["worth sharing"]["quoted"]["text"] == "original thought"
        assert by_text["seconded"]["reply_to"]["text"] == "original thought"

        # Muting the other author hides their saved post and redacts
        # them from reply parents; unmuting restores both.
        conn = init_db(node.config.social_db_path)
        try:
            mute(conn, original["author"], name="other")
        finally:
            conn.close()
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert [item["text"] for item in payload["posts"]] == [
            "seconded",
            "worth sharing",
        ]
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["seconded"]["reply_to"] is None
        # A muted author's post never surfaces through a quote preview
        # either — in the saved list or the timeline.
        assert by_text["worth sharing"]["quoted"] is None
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        timeline_by_text = {item["text"]: item for item in payload["posts"]}
        assert timeline_by_text["worth sharing"]["quoted"] is None
        # Paging fills past the muted rows: the first page still holds
        # one visible post, and the resume offset skips the muted row
        # instead of re-fetching it.
        status, _, payload = _json(bridge, "/api/tan/saved?limit=1")
        assert [item["text"] for item in payload["posts"]] == ["seconded"]
        assert payload["next_offset"] == 2
        status, _, payload = _json(bridge, "/api/tan/saved?limit=1&offset=2")
        assert [item["text"] for item in payload["posts"]] == ["worth sharing"]
        assert payload["next_offset"] is None
        conn = init_db(node.config.social_db_path)
        try:
            assert unmute(conn, original["author"]) is True
        finally:
            conn.close()
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert len(payload["posts"]) == 3

        # A muted keyword hides matching saves; unmuting restores them.
        conn = init_db(node.config.social_db_path)
        try:
            mute_keyword(conn, "sharing")
        finally:
            conn.close()
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert [item["text"] for item in payload["posts"]] == [
            "original thought",
            "seconded",
        ]
        conn = init_db(node.config.social_db_path)
        try:
            assert unmute_keyword(conn, "sharing") is True
        finally:
            conn.close()
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert len(payload["posts"]) == 3

        # A muted keyword blanks the quoted original (and the reply
        # parent) instead of leaking its text through the preview.
        conn = init_db(node.config.social_db_path)
        try:
            mute_keyword(conn, "thought")
        finally:
            conn.close()
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert [item["text"] for item in payload["posts"]] == [
            "seconded",
            "worth sharing",
        ]
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["worth sharing"]["quoted"]["text"] == ""
        assert by_text["worth sharing"]["quoted"]["muted"] is True
        assert by_text["seconded"]["reply_to"]["text"] == ""
        assert by_text["seconded"]["reply_to"]["muted"] is True
        conn = init_db(node.config.social_db_path)
        try:
            assert unmute_keyword(conn, "thought") is True
        finally:
            conn.close()
        status, _, payload = _json(bridge, "/api/tan/saved")
        assert len(payload["posts"]) == 3
    finally:
        bridge.stop()


def test_tan_thread_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, create_reply, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        root = create_post(conn, identity, "root post", name="tester", now=1000)
        reply = create_reply(
            conn, other, root["hash"], "first reply", name="other", now=1001
        )
        nested = create_reply(
            conn, identity, reply["hash"], "reply to the reply", name="tester", now=1002
        )
        create_post(conn, other, "unrelated post", name="other", now=1003)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A hash prefix names the thread; entries come back oldest first
        # with their depth, and the unrelated post stays out.
        status, _, payload = _json(bridge, f"/api/tan/thread?ref={root['hash'][:16]}")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "root post",
            "first reply",
            "reply to the reply",
        ]
        assert [item["depth"] for item in payload["posts"]] == [0, 1, 2]
        assert payload["count"] == 3

        # Naming a reply resolves to the same thread, rooted at its post.
        status, _, payload = _json(bridge, f"/api/tan/thread?ref={nested['hash']}")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "root post",
            "first reply",
            "reply to the reply",
        ]

        # Unknown references and a missing ref fail the way the CLI does.
        status, _, payload = _json(bridge, "/api/tan/thread?ref=ab" * 16)
        assert status == 400
        assert "unknown post" in payload["error"]["message"]

        status, _, payload = _json(bridge, "/api/tan/thread")
        assert status == 400
        assert "field 'ref' is required" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_thread_unexpected_failure_is_500(tmp_path, monkeypatch):
    node = FakeNode()
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # An unexpected failure inside tan_thread is a server error,
        # not a bad reference: the console must not read it as a
        # deleted anchor and close the thread.
        monkeypatch.setattr(
            bridge,
            "tan_thread",
            lambda ref: (_ for _ in ()).throw(RuntimeError("database is locked")),
        )
        status, _, payload = _json(bridge, "/api/tan/thread?ref=abc12345")
        assert status == 500
        assert payload["error"]["message"] == "internal error"
    finally:
        bridge.stop()


def test_tan_delete_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, feed, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "delete me via api", name="tester", now=1000)
        create_post(conn, identity, "keeper via api", name="tester", now=1001)
        foreign = create_post(conn, other, "not yours", name="other", now=1002)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/tan/delete",
            method="POST",
            body={"target": post["hash"][:16]},
        )
        assert status == 200
        assert payload["delete"]["kind"] == "delete"
        assert payload["delete"]["target"] == post["hash"]

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "not yours",
            "keeper via api",
        ]
        # The timeline marks the viewer's own posts so the console only
        # offers the delete button on those.
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["keeper via api"]["own"] is True
        assert by_text["not yours"]["own"] is False

        # Deleting the same post again reports it as deleted.
        status, _, payload = _json(
            bridge,
            "/api/tan/delete",
            method="POST",
            body={"target": post["hash"]},
        )
        assert status == 400
        assert "has been deleted" in payload["error"]["message"]

        # Liking a deleted post fails the same way.
        status, _, payload = _json(
            bridge,
            "/api/tan/like",
            method="POST",
            body={"target": post["hash"]},
        )
        assert status == 400
        assert "has been deleted" in payload["error"]["message"]

        # Someone else's post cannot be deleted through the API.
        status, _, payload = _json(
            bridge,
            "/api/tan/delete",
            method="POST",
            body={"target": foreign["hash"]},
        )
        assert status == 400
        assert "did not author" in payload["error"]["message"]

        # A full hash this node has never seen is still accepted: the
        # tombstone applies when the post arrives.
        status, _, payload = _json(
            bridge,
            "/api/tan/delete",
            method="POST",
            body={"target": "cd" * 32},
        )
        assert status == 200
        assert payload["delete"]["target"] == "cd" * 32
    finally:
        bridge.stop()

    conn = init_db(node.config.social_db_path)
    try:
        entries = feed(conn, author=post["author"])
    finally:
        conn.close()
    # The deleted post is purged from the store, so the feed carries
    # the keeper post and the two deletes — the purged post itself is
    # gone from it.
    assert [entry["kind"] for entry in entries] == [
        "post",
        "delete",
        "delete",
    ]


def test_tan_vote_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_poll, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        poll = create_poll(
            conn,
            identity,
            "which day?",
            ["Thursday", "Friday"],
            name="tester",
            now=1000,
        )
        plain = create_post(conn, identity, "not a poll", name="tester", now=1001)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # Voting casts a signed vote entry for the chosen option.
        status, _, payload = _json(
            bridge,
            "/api/tan/vote",
            method="POST",
            body={"target": poll["hash"][:16], "option": 1},
        )
        assert status == 200
        assert payload["vote"]["kind"] == "vote"
        assert payload["vote"]["target"] == poll["hash"]
        assert payload["vote"]["text"] == "1"

        # The timeline carries the new tally and the viewer's vote.
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["which day?"]["poll"] == {
            "options": ["Thursday", "Friday"],
            "counts": [0, 1],
            "total": 1,
            "closed": False,
            "voted": 1,
        }

        # Voting again moves the vote.
        status, _, payload = _json(
            bridge,
            "/api/tan/vote",
            method="POST",
            body={"target": poll["hash"], "option": 0},
        )
        assert status == 200
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["which day?"]["poll"]["counts"] == [1, 0]
        assert by_text["which day?"]["poll"]["voted"] == 0

        # A plain post is not a poll, and bad options are rejected.
        status, _, payload = _json(
            bridge,
            "/api/tan/vote",
            method="POST",
            body={"target": plain["hash"], "option": 0},
        )
        assert status == 400
        assert "only polls can be voted on" in payload["error"]["message"]

        status, _, payload = _json(
            bridge,
            "/api/tan/vote",
            method="POST",
            body={"target": poll["hash"], "option": 2},
        )
        assert status == 400
        assert "poll has 2 options" in payload["error"]["message"]

        status, _, payload = _json(
            bridge,
            "/api/tan/vote",
            method="POST",
            body={"target": poll["hash"], "option": "1"},
        )
        assert status == 400
        assert "field 'option' must be an option index" in payload["error"]["message"]

        status, _, payload = _json(
            bridge,
            "/api/tan/vote",
            method="POST",
            body={"target": poll["hash"]},
        )
        assert status == 400
        assert "field 'option' must be an option index" in payload["error"]["message"]

        status, _, payload = _json(
            bridge,
            "/api/tan/vote",
            method="POST",
            body={"option": 0},
        )
        assert status == 400
        assert "field 'target' is required" in payload["error"]["message"]

        status, _, payload = _json(
            bridge,
            "/api/tan/vote",
            method="POST",
            body={"target": "cd" * 32, "option": 0},
        )
        assert status == 400
        assert "unknown post" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_poll_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # Creating a poll stores a signed poll entry on the author's chain.
        status, _, payload = _json(
            bridge,
            "/api/tan/poll",
            method="POST",
            body={"question": "which day?", "options": ["Thursday", "Friday"]},
        )
        assert status == 200
        assert payload["poll"]["kind"] == "poll"
        assert payload["poll"]["text"] == "which day?"
        assert payload["poll"]["options"] == ["Thursday", "Friday"]
        poll_hash = payload["poll"]["hash"]

        # The timeline carries the new poll with a zero tally, and a
        # vote cast through the API moves it.
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["which day?"]["poll"] == {
            "options": ["Thursday", "Friday"],
            "counts": [0, 0],
            "total": 0,
            "closed": False,
            "voted": None,
        }
        status, _, payload = _json(
            bridge,
            "/api/tan/vote",
            method="POST",
            body={"target": poll_hash, "option": 0},
        )
        assert status == 200
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["which day?"]["poll"]["counts"] == [1, 0]

        # A missing question, a non-list of options, and an option
        # count outside 2-8 are all rejected.
        status, _, payload = _json(
            bridge,
            "/api/tan/poll",
            method="POST",
            body={"options": ["Thursday", "Friday"]},
        )
        assert status == 400
        assert "field 'question' is required" in payload["error"]["message"]

        status, _, payload = _json(
            bridge,
            "/api/tan/poll",
            method="POST",
            body={"question": "which day?", "options": "Thursday"},
        )
        assert status == 400
        assert "field 'options' must be a list" in payload["error"]["message"]

        status, _, payload = _json(
            bridge,
            "/api/tan/poll",
            method="POST",
            body={"question": "which day?", "options": ["Thursday"]},
        )
        assert status == 400
        assert "a poll needs 2-8 options" in payload["error"]["message"]

        status, _, payload = _json(
            bridge,
            "/api/tan/poll",
            method="POST",
            body={
                "question": "which day?",
                "options": ["Thursday", "Thursday"],
            },
        )
        assert status == 400
        assert "duplicate poll option" in payload["error"]["message"]

        status, _, payload = _json(
            bridge,
            "/api/tan/poll",
            method="POST",
            body={
                "question": "which day?",
                "options": ["Thursday", "Friday"],
                "expires_in": 0,
            },
        )
        assert status == 400
        assert "field 'expires_in' must be a number" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_poll_close_after_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.poll_closes import load_poll_closes
    from tau.tangents.tan import TanApp, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        before = time.time()
        status, _, payload = _json(
            bridge,
            "/api/tan/poll",
            method="POST",
            body={
                "question": "which day?",
                "options": ["Thursday", "Friday"],
                "close_after": 3 * 24 * 60 * 60,
            },
        )
        after = time.time()
        assert status == 200
        poll = payload["poll"]
        assert poll["kind"] == "poll"
        assert before + 3 * 24 * 60 * 60 <= poll["close_at"] <= after + 3 * 24 * 60 * 60
        assert load_poll_closes(tmp_path / "poll-closes.json") == {
            poll["hash"]: poll["close_at"]
        }

        # A deadline past the 7d horizon, and one at or past the
        # poll's own expiry, are both refused before anything is stored.
        for body, fragment in [
            (
                {
                    "question": "which day?",
                    "options": ["Thursday", "Friday"],
                    "close_after": 8 * 24 * 60 * 60,
                },
                "close_after",
            ),
            (
                {
                    "question": "which day?",
                    "options": ["Thursday", "Friday"],
                    "close_after": 0,
                },
                "close_after",
            ),
            (
                {
                    "question": "which day?",
                    "options": ["Thursday", "Friday"],
                    "expires_in": 3600,
                    "close_after": 3600,
                },
                "shorter than 'expires_in'",
            ),
        ]:
            status, _, payload = _json(
                bridge, "/api/tan/poll", method="POST", body=body
            )
            assert status == 400, body
            assert fragment in payload["error"]["message"], body
    finally:
        bridge.stop()


def test_tan_poll_close_after_reports_a_lost_deadline(tmp_path):
    # A close that cannot be recorded (a corrupt close file) still
    # returns the created poll — but the response says so, instead of
    # reporting success and silently dropping the requested close.
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        (tmp_path / "poll-closes.json").write_text("{not json", encoding="utf-8")
        status, _, payload = _json(
            bridge,
            "/api/tan/poll",
            method="POST",
            body={
                "question": "which day?",
                "options": ["Thursday", "Friday"],
                "close_after": 3600,
            },
        )
        assert status == 200
        assert payload["ok"] is True
        poll = payload["poll"]
        assert poll["kind"] == "poll"
        assert "close_at" not in poll
        assert "close_schedule_error" in poll
    finally:
        bridge.stop()


def test_tan_repost_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, feed, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        foreign = create_post(conn, other, "worth sharing", name="other", now=1000)
        mine = create_post(conn, identity, "my own thought", name="tester", now=1001)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/tan/repost",
            method="POST",
            body={"target": foreign["hash"][:16]},
        )
        assert status == 200
        assert payload["repost"]["kind"] == "repost"
        assert payload["repost"]["target"] == foreign["hash"]

        # The timeline now carries the repost, and the original is
        # marked as reposted by you.
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["worth sharing"]["reposted"] is True
        assert by_text["worth sharing"]["reposts"] == 1

        # Reposting the same post again is refused while the first
        # repost still stands.
        status, _, payload = _json(
            bridge,
            "/api/tan/repost",
            method="POST",
            body={"target": foreign["hash"]},
        )
        assert status == 400
        assert "already been reposted" in payload["error"]["message"]

        # Your own post cannot be reposted.
        status, _, payload = _json(
            bridge,
            "/api/tan/repost",
            method="POST",
            body={"target": mine["hash"]},
        )
        assert status == 400
        assert "own post" in payload["error"]["message"]

        # A post this node has never seen cannot be reposted.
        status, _, payload = _json(
            bridge,
            "/api/tan/repost",
            method="POST",
            body={"target": "cd" * 32},
        )
        assert status == 400
        assert "unknown post" in payload["error"]["message"]

        # A missing target is a bad request, not a server error.
        status, _, payload = _json(
            bridge, "/api/tan/repost", method="POST", body={}
        )
        assert status == 400
        assert "target" in payload["error"]["message"]
    finally:
        bridge.stop()

    conn = init_db(node.config.social_db_path)
    try:
        entries = feed(conn, author=foreign["author"])
    finally:
        conn.close()
    assert [entry["kind"] for entry in entries] == ["post"]


def test_tan_share_via_api(tmp_path):
    import RNS

    from tau.blocklist import BlockList
    from tau.config import TauConfig
    from tau.tangents.tan import (
        TanApp,
        create_poll,
        create_post,
        create_repost,
        init_db,
    )

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    third = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        foreign = create_post(conn, other, "worth sharing", name="other", now=1000)
        mine = create_post(conn, identity, "my own thought", name="tester", now=1001)
        poll = create_poll(
            conn, other, "best colour?", ["red", "blue"], name="other", now=1002
        )
        repost = create_repost(conn, third, foreign["hash"], name="third", now=1003)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    node.set_blocklist(BlockList(blocked=frozenset()))
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # Sharing a post sends it as an attributed DM; the author is
        # not in the registry, so the recorded name is used.
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": foreign["hash"][:16], "to": "alice"},
        )
        assert status == 200
        assert payload["ok"] is True
        assert node.sent[0] == (
            "alice",
            f"Shared Tan post by other:\nworth sharing\n(tan {foreign['hash']})",
            None,
        )

        # Your own post attributes you.
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": mine["hash"], "to": "alice"},
        )
        assert status == 200
        assert "Shared Tan post by you:\nmy own thought" in node.sent[1][1]

        # A poll shares its question and options.
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": poll["hash"], "to": "alice"},
        )
        assert status == 200
        assert node.sent[2][1] == (
            "Shared Tan poll by other:\nbest colour?\n1. red\n2. blue\n"
            f"(tan {poll['hash']})"
        )

        # A repost shares its original, not the repost.
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": repost["hash"], "to": "alice"},
        )
        assert status == 200
        assert node.sent[3][1] == (
            f"Shared Tan post by other:\nworth sharing\n(tan {foreign['hash']})"
        )

        # Sharing into a group fans out one leg per member.
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": foreign["hash"], "group": "crew"},
        )
        assert status == 200
        assert payload["ok"] is True
        assert node.group_sent[0][0] == "crew"
        assert node.group_sent[0][1].startswith("Shared Tan post by other:")

        # A post by a blocked author is refused.
        node.set_blocklist(BlockList(blocked=frozenset({foreign["author"]})))
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": foreign["hash"], "to": "alice"},
        )
        assert status == 400
        assert "blocked peer" in payload["error"]["message"]

        # An unreadable block-list fails closed.
        node.set_blocklist(None)
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": foreign["hash"], "to": "alice"},
        )
        assert status == 400
        assert "could not be read" in payload["error"]["message"]

        # A post this node has never seen cannot be shared.
        node.set_blocklist(BlockList(blocked=frozenset()))
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": "cd" * 32, "to": "alice"},
        )
        assert status == 400
        assert "unknown post" in payload["error"]["message"]

        # Exactly one of `to` and `group` is required.
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": foreign["hash"]},
        )
        assert status == 400
        assert "exactly one of" in payload["error"]["message"]
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": foreign["hash"], "to": "alice", "group": "crew"},
        )
        assert status == 400
        assert "exactly one of" in payload["error"]["message"]

        # A missing target is a bad request, not a server error.
        status, _, payload = _json(
            bridge, "/api/tan/share", method="POST", body={"to": "alice"}
        )
        assert status == 400
        assert "target" in payload["error"]["message"]

        # A poll the owner's display rules hide is refused, not shared.
        from tau.tangents.rules import Ruleset, save_ruleset

        save_ruleset(
            node.config.tan_rules_path,
            Ruleset(kinds=frozenset({"post"})),
        )
        # The console's rules save reloads the running app, so the
        # share guard sees the new ruleset.
        node.app.reload_ruleset()
        sent_before = len(node.sent)
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": poll["hash"], "to": "alice"},
        )
        assert status == 400
        assert "hidden by your Tan display rules" in payload["error"]["message"]
        assert len(node.sent) == sent_before
    finally:
        bridge.stop()


def test_tan_share_to_channel_via_api(tmp_path):
    import RNS

    from tau.blocklist import BlockList
    from tau.channels import ChannelApp
    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        foreign = create_post(conn, other, "worth sharing", name="other", now=1000)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    node.set_blocklist(BlockList(blocked=frozenset()))
    # Attach a real channel app so the share lands in channel history;
    # its node points at the fake so gossip has nowhere to go.
    app = ChannelApp(node.config, node.identity, name="tester", gossip=False)
    app.node = node
    node.channel_app = app
    app.join("general")

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # Sharing a post into a channel sends it as an attributed
        # channel message; the author is not in the registry, so the
        # recorded name is used.
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": foreign["hash"][:16], "channel": "general"},
        )
        assert status == 200
        assert payload["ok"] is True
        assert payload["message"]["text"] == (
            f"Shared Tan post by other:\nworth sharing\n(tan {foreign['hash']})"
        )

        # The shared message is in the channel history.
        status, _, payload = _json(
            bridge, "/api/channels/messages?channel=general"
        )
        assert status == 200
        assert payload["messages"][0]["text"].startswith(
            "Shared Tan post by other:"
        )

        # Exactly one of `to`, `group`, and `channel` is required.
        for bad in (
            {"to": "alice", "channel": "general"},
            {"group": "crew", "channel": "general"},
            {"channel": ""},
            {"channel": "  "},
            {"channel": 123},
        ):
            status, _, payload = _json(
                bridge,
                "/api/tan/share",
                method="POST",
                body={"target": foreign["hash"], **bad},
            )
            assert status == 400, bad
            assert "exactly one of" in payload["error"]["message"] or (
                "channel" in payload["error"]["message"]
            ), bad

        # Sharing into a channel this node has not joined is refused.
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": foreign["hash"], "channel": "nowhere"},
        )
        assert status == 400
        assert "not a member" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_share_to_broadcast_via_api(tmp_path):
    import RNS

    from tau.blocklist import BlockList
    from tau.broadcasts import BroadcastStore
    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        foreign = create_post(conn, other, "worth sharing", name="other", now=1000)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    node.set_blocklist(BlockList(blocked=frozenset()))
    node.broadcasts = BroadcastStore(tmp_path / "broadcasts.json")
    node.broadcasts.create("team", [PEER_HASH])

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # Sharing a post with a broadcast list sends it as an
        # attributed DM to every member; the author is not in the
        # registry, so the recorded name is used.
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": foreign["hash"][:16], "broadcast": "team"},
        )
        assert status == 200
        assert payload["ok"] is True
        assert payload["response"]["name"] == "team"
        assert payload["response"]["delivered"] == 1
        assert payload["response"]["failed"] == 0
        assert node.sent == [
            (
                "alice",
                f"Shared Tan post by other:\nworth sharing\n(tan {foreign['hash']})",
                None,
            )
        ]

        # Exactly one of `to`, `group`, `broadcast`, and `channel` is
        # required.
        for bad in (
            {"to": "alice", "broadcast": "team"},
            {"group": "crew", "broadcast": "team"},
            {"broadcast": "team", "channel": "general"},
            {"broadcast": ""},
            {"broadcast": "  "},
            {"broadcast": 123},
        ):
            status, _, payload = _json(
                bridge,
                "/api/tan/share",
                method="POST",
                body={"target": foreign["hash"], **bad},
            )
            assert status == 400, bad
            assert "exactly one of" in payload["error"]["message"] or (
                "broadcast" in payload["error"]["message"]
            ), bad

        # Sharing with an unknown list is refused.
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": foreign["hash"], "broadcast": "nope"},
        )
        assert status == 400
        assert "unknown broadcast list" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_share_to_broadcast_reports_partial_failure_via_api(tmp_path):
    import time

    import RNS

    from tau.blocklist import BlockList
    from tau.broadcasts import BroadcastStore
    from tau.config import TauConfig
    from tau.peers import Peer
    from tau.tangents.tan import TanApp, create_post, init_db

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        foreign = create_post(conn, other, "worth sharing", name="other", now=1000)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    node.set_blocklist(BlockList(blocked=frozenset()))
    # A second registry peer the fake node cannot resolve: its send
    # fails, but the first member still gets the message.
    node.registry._peers.append(
        Peer(
            destination_hash=bytes.fromhex(OTHER_HASH),
            name="bob",
            public_key=b"\x02" * 32,
            first_seen=1000.0,
            last_seen=time.time(),
            hops=1,
        )
    )
    node.broadcasts = BroadcastStore(tmp_path / "broadcasts.json")
    node.broadcasts.create("team", [PEER_HASH, OTHER_HASH])

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": foreign["hash"][:16], "broadcast": "team"},
        )
        assert status == 200
        assert payload["ok"] is False
        assert payload["response"]["delivered"] == 1
        assert payload["response"]["failed"] == 1
        assert payload["response"]["results"][0]["status"] == "delivered"
        assert payload["response"]["results"][1] == {
            "hash": OTHER_HASH,
            "name": "bob",
            "status": "failed",
            "error": f"unknown peer: {OTHER_HASH}",
        }
        assert node.sent == [
            (
                "alice",
                f"Shared Tan post by other:\nworth sharing\n(tan {foreign['hash']})",
                None,
            )
        ]
    finally:
        bridge.stop()


def test_tan_share_uses_cached_app_ruleset(tmp_path):
    # Regression test for Qodo's Critical on PR #450: the share guard
    # must agree with what the console shows. A rules file changed by
    # another process without reloading the app must not let a share
    # send a poll the console still hides.
    import RNS

    from tau.blocklist import BlockList
    from tau.config import TauConfig
    from tau.tangents.rules import Ruleset, save_ruleset
    from tau.tangents.tan import TanApp, create_poll, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    save_ruleset(
        node.config.tan_rules_path,
        Ruleset(kinds=frozenset({"post"})),
    )
    conn = init_db(node.config.social_db_path)
    try:
        poll = create_poll(
            conn, other, "best colour?", ["red", "blue"], name="other", now=1002
        )
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    node.set_blocklist(BlockList(blocked=frozenset()))
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The app loads the hide-polls ruleset, so the poll is refused.
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": poll["hash"], "to": "alice"},
        )
        assert status == 400
        assert "hidden by your Tan display rules" in payload["error"]["message"]
        assert node.sent == []

        # Another process rewrites the file to allow polls, without
        # reloading the app: the console still hides the poll, so the
        # share is still refused.
        save_ruleset(node.config.tan_rules_path, Ruleset())
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": poll["hash"], "to": "alice"},
        )
        assert status == 400
        assert "hidden by your Tan display rules" in payload["error"]["message"]
        assert node.sent == []
    finally:
        bridge.stop()


def test_tan_share_malformed_rules_is_an_error(tmp_path):
    # A rules file that cannot be parsed fails the share instead of
    # silently becoming "display everything".
    import RNS

    from tau.blocklist import BlockList
    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_poll, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        poll = create_poll(
            conn, other, "best colour?", ["red", "blue"], name="other", now=1002
        )
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    node.set_blocklist(BlockList(blocked=frozenset()))
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        node.config.tan_rules_path.write_text("{not json", encoding="utf-8")
        status, _, payload = _json(
            bridge,
            "/api/tan/share",
            method="POST",
            body={"target": poll["hash"], "to": "alice"},
        )
        assert status == 400
        assert "could not be read" in payload["error"]["message"]
        assert node.sent == []
    finally:
        bridge.stop()


def test_tan_repost_state_on_mentions_and_saved(tmp_path):
    # Regression test for the repost-annotation gap Qodo's review caught:
    # mentions and saved posts did not carry repost counts, so a card
    # there kept offering Repost for a post already reposted.
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, create_reply, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(
            conn, identity, "original thought", name="tester", now=1000
        )
        reply = create_reply(
            conn, other, mine["hash"], "seconded", name="other", now=1001
        )
        foreign = create_post(conn, other, "worth sharing", name="other", now=1002)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge, "/api/tan/save", method="POST", body={"target": foreign["hash"]}
        )
        assert status == 200

        for target in (reply["hash"], foreign["hash"]):
            status, _, payload = _json(
                bridge, "/api/tan/repost", method="POST", body={"target": target}
            )
            assert status == 200

        status, _, payload = _json(bridge, "/api/tan/mentions")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["seconded"]
        assert payload["posts"][0]["reposted"] is True
        assert payload["posts"][0]["reposts"] == 1

        status, _, payload = _json(bridge, "/api/tan/saved")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["worth sharing"]
        assert payload["posts"][0]["reposted"] is True
        assert payload["posts"][0]["reposts"] == 1
    finally:
        bridge.stop()


def test_tan_rules_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    node.app = TanApp(node.config, identity, name="tester", gossip=False)

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The default ruleset admits everything.
        status, _, payload = _json(bridge, "/api/tan/rules")
        assert status == 200
        assert payload["rules"] == {
            "default": "allow",
            "allow_authors": [],
            "block_authors": [],
            "allow_followed": False,
            "kinds": None,
        }
        assert payload["available_kinds"] == [
            "post",
            "poll",
            "like",
            "unlike",
            "delete",
            "repost",
            "close",
            "unvote",
            "reply",
            "edit",
            "quote",
            "vote",
        ]

        # Cache the live app's ruleset first: the update below must
        # reload it, not just be picked up on a lazy first load.
        assert node.app.ruleset.default_allow is True

        # A peer name resolves to its author hash, like `tau tan rules`.
        status, _, payload = _json(
            bridge,
            "/api/tan/rules",
            method="POST",
            body={
                "default": "deny",
                "allow_authors": ["alice"],
                "block_authors": [OTHER_HASH],
                "allow_followed": True,
                "kinds": ["post", "reply"],
            },
        )
        assert status == 200
        assert payload["rules"] == {
            "default": "deny",
            "allow_authors": [PEER_HASH],
            "block_authors": [OTHER_HASH],
            "allow_followed": True,
            "kinds": ["post", "reply"],
        }

        # The saved ruleset persists, and the live app picked it up
        # without a restart.
        status, _, payload = _json(bridge, "/api/tan/rules")
        assert status == 200
        assert payload["rules"]["default"] == "deny"
        assert node.app.ruleset.default_allow is False
        assert node.app.ruleset.allow_authors == frozenset({PEER_HASH})
        assert node.app.ruleset.displays("post") is True
        assert node.app.ruleset.displays("like") is False

        # Bad input is a 400 and leaves the saved ruleset untouched.
        for body in (
            {"default": "maybe"},
            {"allow_authors": ["nobody-here"]},
            {"block_authors": [OTHER_HASH], "kinds": ["poke"]},
            {"allow_authors": "alice"},
            ["not", "an", "object"],
        ):
            status, _, payload = _json(
                bridge, "/api/tan/rules", method="POST", body=body
            )
            assert status == 400
        status, _, payload = _json(bridge, "/api/tan/rules")
        assert status == 200
        assert payload["rules"]["default"] == "deny"
    finally:
        bridge.stop()


def test_tan_edit_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, feed, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "orginal typo", name="tester", now=1000)
        create_post(conn, identity, "keeper via api", name="tester", now=1001)
        doomed = create_post(conn, identity, "delete me first", name="tester", now=1002)
        foreign = create_post(conn, other, "not yours", name="other", now=1003)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A short prefix resolves the same way the delete endpoint does.
        status, _, payload = _json(
            bridge,
            "/api/tan/edit",
            method="POST",
            body={"target": post["hash"][:16], "text": "original, fixed"},
        )
        assert status == 200
        assert payload["edit"]["kind"] == "edit"
        assert payload["edit"]["target"] == post["hash"]

        # The timeline shows the replacement text and marks the edit.
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        by_text = {item["text"]: item for item in payload["posts"]}
        assert by_text["original, fixed"]["edited"] is True
        assert by_text["original, fixed"]["own"] is True
        assert "orginal typo" not in by_text

        # A second edit replaces the first; the latest edit wins.
        status, _, payload = _json(
            bridge,
            "/api/tan/edit",
            method="POST",
            body={"target": post["hash"], "text": "original, fixed again"},
        )
        assert status == 200
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert [item["text"] for item in payload["posts"]] == [
            "not yours",
            "delete me first",
            "keeper via api",
            "original, fixed again",
        ]

        # Someone else's post cannot be edited through the API.
        status, _, payload = _json(
            bridge,
            "/api/tan/edit",
            method="POST",
            body={"target": foreign["hash"], "text": "hijack"},
        )
        assert status == 400
        assert "did not author" in payload["error"]["message"]

        # Empty replacement text is a bad request.
        status, _, payload = _json(
            bridge,
            "/api/tan/edit",
            method="POST",
            body={"target": post["hash"], "text": "   "},
        )
        assert status == 400
        assert "field 'text' is required" in payload["error"]["message"]

        # A post this node has never seen cannot be edited.
        status, _, payload = _json(
            bridge,
            "/api/tan/edit",
            method="POST",
            body={"target": "cd" * 32, "text": "ghost"},
        )
        assert status == 400
        assert "unknown post" in payload["error"]["message"]

        # A deleted post cannot be edited either.
        status, _, payload = _json(
            bridge,
            "/api/tan/delete",
            method="POST",
            body={"target": doomed["hash"]},
        )
        assert status == 200
        status, _, payload = _json(
            bridge,
            "/api/tan/edit",
            method="POST",
            body={"target": doomed["hash"], "text": "too late"},
        )
        assert status == 400
        assert "has been deleted" in payload["error"]["message"]
    finally:
        bridge.stop()

    conn = init_db(node.config.social_db_path)
    try:
        entries = feed(conn, author=post["author"])
    finally:
        conn.close()
    # The deleted post is purged from the store, so the feed carries
    # the two surviving posts, both edits, and the delete — the purged
    # post itself is gone from it.
    assert [entry["kind"] for entry in entries] == [
        "post",
        "post",
        "edit",
        "edit",
        "delete",
    ]



def test_tan_export_import_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path / "one", name="tester")
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        mine = create_post(conn, identity, "mine one", name="tester", now=1000)
        create_post(conn, identity, "mine two", name="tester", now=1001)
        create_post(conn, other, "not mine", name="other", now=1002)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The default scope is the node's own chain only.
        status, _, payload = _json(bridge, "/api/tan/export")
        assert status == 200
        assert payload["format"] == "tau-tan-export"
        assert payload["version"] == 1
        assert [entry["text"] for entry in payload["entries"]] == [
            "mine one",
            "mine two",
        ]
        assert payload["authors"] == [mine["author"]]

        status, _, payload = _json(bridge, "/api/tan/export?scope=all")
        assert status == 200
        assert len(payload["entries"]) == 3
        assert len(payload["authors"]) == 2
        assert mine["author"] in payload["authors"]

        status, _, payload = _json(bridge, "/api/tan/export?scope=bogus")
        assert status == 400
        assert "scope" in payload["error"]["message"]

        # A fresh node restores the whole document, then reports the
        # same entries as already present on a second import.
        node2 = FakeNode()
        node2.config = TauConfig.resolve(data_dir=tmp_path / "two", name="tester2")
        node2.app = TanApp(node2.config, RNS.Identity(), name="tester2", gossip=False)
        bridge2 = WebBridge(node2, host="127.0.0.1", port=0)
        bridge2.start()
        try:
            _, _, document = _json(bridge, "/api/tan/export?scope=all")
            status, _, payload = _json(
                bridge2, "/api/tan/import", method="POST", body=document
            )
            assert status == 200
            assert payload["import"] == {
                "imported": 3,
                "present": 0,
                "skipped": 0,
                "errors": [],
            }

            status, _, payload = _json(
                bridge2, "/api/tan/import", method="POST", body=document
            )
            assert status == 200
            assert payload["import"]["imported"] == 0
            assert payload["import"]["present"] == 3

            # A document with the wrong format is rejected, not stored.
            status, _, payload = _json(
                bridge2, "/api/tan/import", method="POST", body={"format": "nope"}
            )
            assert status == 400
            assert "unsupported export format" in payload["error"]["message"]

            # One entry with a tampered body fails verification and is
            # skipped; the rest of the document still imports. Drop the
            # store's copy of the tampered entry first so it is new here
            # rather than already present.
            tampered = json.loads(json.dumps(document))
            tampered["entries"][0]["text"] = "forged"
            conn2 = init_db(node2.config.social_db_path)
            try:
                conn2.execute(
                    "DELETE FROM posts WHERE hash = ?",
                    (tampered["entries"][0]["hash"],),
                )
                conn2.commit()
            finally:
                conn2.close()
            status, _, payload = _json(
                bridge2, "/api/tan/import", method="POST", body=tampered
            )
            assert status == 200
            assert payload["import"]["imported"] == 0
            assert payload["import"]["present"] == 2
            assert payload["import"]["skipped"] == 1
            assert len(payload["import"]["errors"]) == 1

            # An export document is bigger than the 32 KiB cap for
            # ordinary actions; the import route accepts it.
            big = json.loads(json.dumps(document))
            big["entries"] = big["entries"] * 100
            assert len(json.dumps(big).encode("utf-8")) > 32 * 1024
            status, _, payload = _json(
                bridge2, "/api/tan/import", method="POST", body=big
            )
            assert status == 200
            assert payload["import"]["skipped"] == 0
        finally:
            bridge2.stop()
    finally:
        bridge.stop()


def test_tan_rules_without_tan_app(tmp_path):
    # No Tan app attached: the rules file is still readable.
    from tau.config import TauConfig
    from tau.tangents.rules import Ruleset, save_ruleset

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    save_ruleset(
        node.config.tan_rules_path,
        Ruleset(default_allow=False, allow_followed=True),
    )
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/rules")
        assert status == 200
        assert payload["rules"]["default"] == "deny"
        assert payload["rules"]["allow_followed"] is True

        # A rules file that cannot be parsed fails closed, like admission.
        node.config.tan_rules_path.write_text("not json", encoding="utf-8")
        status, _, payload = _json(bridge, "/api/tan/rules")
        assert status == 200
        assert payload["rules"]["default"] == "deny"
        assert payload["rules"]["allow_authors"] == []
    finally:
        bridge.stop()


def _likers_setup(tmp_path):
    """A node with one post by bob, liked by alice, carol and dave.

    Returns (node, post, authors) where authors maps a label to the
    liker's author hex. Carol unlikes at the end, so the visible likers
    are dave (1003) then alice (1001).
    """
    import RNS

    from tau.config import TauConfig
    from tau.identity import destination_hash
    from tau.tangents.tan import create_post, create_reaction, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    alice = RNS.Identity()
    bob = RNS.Identity()
    carol = RNS.Identity()
    dave = RNS.Identity()
    # A liker the node knows as a peer resolves to the registry name,
    # not the name on their stored posts.
    node.registry._peers.append(
        Peer(
            destination_hash=destination_hash(alice),
            name="alice-on-record",
            public_key=b"\x02" * 32,
            first_seen=1000.0,
            last_seen=time.time(),
            hops=1,
        )
    )
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, bob, "likeable via api", name="bob", now=1000)
        create_reaction(conn, alice, post["hash"], liked=True, name="alice", now=1001)
        create_reaction(conn, carol, post["hash"], liked=True, name="carol", now=1002)
        create_reaction(conn, dave, post["hash"], liked=True, name="dave", now=1003)
        # Carol's unlike supersedes her like.
        create_reaction(conn, carol, post["hash"], liked=False, name="carol", now=1004)
    finally:
        conn.close()
    authors = {
        label: destination_hash(identity).hex()
        for label, identity in (
            ("alice", alice),
            ("bob", bob),
            ("carol", carol),
            ("dave", dave),
        )
    }
    return node, post, authors


def test_tan_likers_via_api(tmp_path):
    node, post, authors = _likers_setup(tmp_path)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 200
        assert payload["target"] == post["hash"]
        assert payload["count"] == 2
        assert payload["total"] == 2
        assert payload["likers"] == [
            {
                "author": authors["dave"],
                "name": "dave",
                "liked_at": 1003.0,
            },
            {
                "author": authors["alice"],
                "name": "alice-on-record",
                "liked_at": 1001.0,
            },
        ]

        # An uppercase spelling of the same hash resolves, and the
        # response carries the canonical lowercase hash.
        status, _, payload = _json(
            bridge, f"/api/tan/likes?target={post['hash'].upper()}"
        )
        assert status == 200
        assert payload["target"] == post["hash"]
        assert payload["count"] == 2

        # Unknown post hash -> 400, per the issue spec.
        status, _, payload = _json(bridge, "/api/tan/likes?target=" + "cd" * 32)
        assert status == 400
        assert "unknown post" in payload["error"]["message"]

        # A missing target -> 400.
        status, _, payload = _json(bridge, "/api/tan/likes")
        assert status == 400
        assert "target is required" in payload["error"]["message"]
    finally:
        bridge.stop()


def _engagers_setup(tmp_path):
    """A node with one post by bob, reposted and quoted by others.

    Returns (node, post, authors, quotes) where authors maps a label
    to the engager's author hex and quotes maps a label to the quote's
    hash. Alice reposts at 1001 and quotes at 1001; dave reposts at
    1003; carol quotes at 1002.
    """
    import RNS

    from tau.config import TauConfig
    from tau.identity import destination_hash
    from tau.tangents.tan import (
        create_post,
        create_quote,
        create_repost,
        init_db,
    )

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    alice = RNS.Identity()
    bob = RNS.Identity()
    carol = RNS.Identity()
    dave = RNS.Identity()
    # An engager the node knows as a peer resolves to the registry name,
    # not the name on their stored posts.
    node.registry._peers.append(
        Peer(
            destination_hash=destination_hash(alice),
            name="alice-on-record",
            public_key=b"\x02" * 32,
            first_seen=1000.0,
            last_seen=time.time(),
            hops=1,
        )
    )
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, bob, "shareable via api", name="bob", now=1000)
        create_repost(conn, alice, post["hash"], name="alice", now=1001)
        create_repost(conn, dave, post["hash"], name="dave", now=1003)
        alice_quote = create_quote(
            conn, alice, post["hash"], "seconded", name="alice", now=1001
        )
        carol_quote = create_quote(
            conn, carol, post["hash"], "hard agree", name="carol", now=1002
        )
    finally:
        conn.close()
    authors = {
        label: destination_hash(identity).hex()
        for label, identity in (
            ("alice", alice),
            ("bob", bob),
            ("carol", carol),
            ("dave", dave),
        )
    }
    quotes = {
        "alice": alice_quote["hash"],
        "carol": carol_quote["hash"],
    }
    return node, post, authors, quotes


def test_tan_reposters_via_api(tmp_path):
    node, post, authors, _ = _engagers_setup(tmp_path)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, f"/api/tan/reposts?target={post['hash']}")
        assert status == 200
        assert payload["target"] == post["hash"]
        assert payload["count"] == 2
        assert payload["total"] == 2
        assert payload["reposters"] == [
            {
                "author": authors["dave"],
                "name": "dave",
                "reposted_at": 1003.0,
            },
            {
                "author": authors["alice"],
                "name": "alice-on-record",
                "reposted_at": 1001.0,
            },
        ]

        # An uppercase spelling of the same hash resolves, and the
        # response carries the canonical lowercase hash.
        status, _, payload = _json(
            bridge, f"/api/tan/reposts?target={post['hash'].upper()}"
        )
        assert status == 200
        assert payload["target"] == post["hash"]
        assert payload["count"] == 2

        # Unknown post hash -> 400, per the issue spec.
        status, _, payload = _json(bridge, "/api/tan/reposts?target=" + "cd" * 32)
        assert status == 400
        assert "unknown post" in payload["error"]["message"]

        # A missing target -> 400.
        status, _, payload = _json(bridge, "/api/tan/reposts")
        assert status == 400
        assert "target is required" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_quoters_via_api(tmp_path):
    node, post, authors, quotes = _engagers_setup(tmp_path)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, f"/api/tan/quotes?target={post['hash']}")
        assert status == 200
        assert payload["target"] == post["hash"]
        assert payload["count"] == 2
        assert payload["total"] == 2
        assert payload["quoters"] == [
            {
                "author": authors["carol"],
                "name": "carol",
                "hash": quotes["carol"],
                "text": "hard agree",
                "quoted_at": 1002.0,
            },
            {
                "author": authors["alice"],
                "name": "alice-on-record",
                "hash": quotes["alice"],
                "text": "seconded",
                "quoted_at": 1001.0,
            },
        ]

        # An uppercase spelling of the same hash resolves, and the
        # response carries the canonical lowercase hash.
        status, _, payload = _json(
            bridge, f"/api/tan/quotes?target={post['hash'].upper()}"
        )
        assert status == 200
        assert payload["target"] == post["hash"]
        assert payload["count"] == 2

        # Unknown post hash -> 400, per the issue spec.
        status, _, payload = _json(bridge, "/api/tan/quotes?target=" + "cd" * 32)
        assert status == 400
        assert "unknown post" in payload["error"]["message"]

        # A missing target -> 400.
        status, _, payload = _json(bridge, "/api/tan/quotes")
        assert status == 400
        assert "target is required" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_engagers_excludes_muted_and_blocked(tmp_path):
    from tau.blocklist import BlockList
    from tau.config import TauConfig
    from tau.tangents.tan import init_db, mute

    node, post, authors, _ = _engagers_setup(tmp_path)
    conn = init_db(node.config.social_db_path)
    try:
        mute(conn, authors["dave"], name="dave")
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The muted reposter is hidden, as in the timeline.
        status, _, payload = _json(bridge, f"/api/tan/reposts?target={post['hash']}")
        assert status == 200
        assert [entry["author"] for entry in payload["reposters"]] == [
            authors["alice"]
        ]
        assert payload["total"] == 1

        # A blocked quoter is hidden too; the unblocked one stays.
        node.blocklist = BlockList(blocked=frozenset({authors["alice"]}))
        status, _, payload = _json(bridge, f"/api/tan/quotes?target={post['hash']}")
        assert status == 200
        assert [entry["author"] for entry in payload["quoters"]] == [
            authors["carol"]
        ]
        assert payload["total"] == 1

        # A fail-closed block-all list hides the post entirely,
        # matching the timeline and the DM gate.
        node.blocklist = BlockList(blocked=frozenset(), block_all=True)
        status, _, payload = _json(bridge, f"/api/tan/reposts?target={post['hash']}")
        assert status == 400
        assert "unknown post" in payload["error"]["message"]
        status, _, payload = _json(bridge, f"/api/tan/quotes?target={post['hash']}")
        assert status == 400
        assert "unknown post" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_rules_without_data_dir(tmp_path):
    # The node has no config, so there is no rules file to read. (The
    # fixture node always carries a config now, so build one here and
    # take it away instead of relying on the shared bridge fixture.)
    node = FakeNode(tmp_path / "mailbox.json")
    del node.config
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/rules")
        assert status == 503
        status, _, payload = _json(
            bridge, "/api/tan/rules", method="POST", body={"default": "deny"}
        )
        assert status == 503
    finally:
        bridge.stop()

def test_tan_export_import_requires_social(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, RNS.Identity(), "cached", name="other", now=1000)
    finally:
        conn.close()

    # No TanApp: the node never enabled the social feed, even though a
    # post is cached on disk.
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/export")
        assert status == 503
        assert "not enabled" in payload["error"]["message"]

        status, _, payload = _json(
            bridge,
            "/api/tan/import",
            method="POST",
            body={"format": "tau-tan-export", "version": 1, "entries": []},
        )
        assert status == 503
        assert "not enabled" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_likers_excludes_muted_and_blocked(tmp_path):
    from tau.blocklist import BlockList
    from tau.config import TauConfig
    from tau.tangents.tan import init_db, mute

    node, post, authors = _likers_setup(tmp_path)
    conn = init_db(node.config.social_db_path)
    try:
        mute(conn, authors["dave"], name="dave")
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The muted liker is hidden, as in the timeline.
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 200
        assert [entry["author"] for entry in payload["likers"]] == [authors["alice"]]
        assert payload["total"] == 1

        # A blocked liker is hidden too.
        node.blocklist = BlockList(blocked=frozenset({authors["alice"]}))
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 200
        assert payload["likers"] == []
        assert payload["total"] == 0

        # A fail-closed block-all list hides the post entirely,
        # matching the timeline and the DM gate.
        node.blocklist = BlockList(blocked=frozenset(), block_all=True)
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 400
        assert "unknown post" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_export_too_large_for_import_is_refused(tmp_path, monkeypatch):
    import RNS

    import tau.webapi as webapi
    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, identity, "one post", name="tester", now=1000)
    finally:
        conn.close()
    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A cache the console could not import back must not be
        # exported either: shrink the bound to prove the check.
        monkeypatch.setattr(webapi, "MAX_IMPORT_BYTES", 10)
        status, _, payload = _json(bridge, "/api/tan/export")
        assert status == 400
        assert "larger than the console can import" in payload["error"]["message"]
        assert "tau tan export" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_likers_hides_post_by_hidden_author(tmp_path):
    from tau.config import TauConfig
    from tau.tangents.tan import init_db, mute

    node, post, authors = _likers_setup(tmp_path)
    conn = init_db(node.config.social_db_path)
    try:
        mute(conn, authors["bob"], name="bob")
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The timeline would not show a muted author's post at all, so
        # the endpoint does not admit the post exists.
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 400
        assert "unknown post" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_likers_rejects_deleted_post(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_delete, create_post, create_reaction, init_db

    node, _, _ = _likers_setup(tmp_path)
    # A post deleted by its own author: build one with an identity held
    # in this test, like it, then delete it.
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        own = create_post(conn, identity, "delete me", name="me", now=2000)
        create_reaction(conn, identity, own["hash"], liked=True, name="me", now=2001)
        create_delete(conn, identity, own["hash"], name="me", now=2002)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, f"/api/tan/likes?target={own['hash']}")
        assert status == 400
        assert "has been deleted" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_reposters_rejects_deleted_post(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_delete, create_post, create_repost, init_db

    node, _, _ = _likers_setup(tmp_path)
    # A post deleted by its own author: build one with an identity held
    # in this test, have another identity repost it, then delete it.
    # (Reposting your own post is refused, so the reposter differs.)
    identity = RNS.Identity()
    other = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        own = create_post(conn, identity, "delete me", name="me", now=2000)
        create_repost(conn, other, own["hash"], name="other", now=2001)
        create_delete(conn, identity, own["hash"], name="me", now=2002)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, f"/api/tan/reposts?target={own['hash']}")
        assert status == 400
        assert "has been deleted" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_quoters_rejects_deleted_post(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_delete, create_post, create_quote, init_db

    node, _, _ = _likers_setup(tmp_path)
    # A post deleted by its own author: build one with an identity held
    # in this test, quote it, then delete it.
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        own = create_post(conn, identity, "delete me", name="me", now=2000)
        create_quote(conn, identity, own["hash"], "quoting this", name="me", now=2001)
        create_delete(conn, identity, own["hash"], name="me", now=2002)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, f"/api/tan/quotes?target={own['hash']}")
        assert status == 400
        assert "has been deleted" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_likers_ignores_forged_delete(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_delete, init_db, store_post

    node, post, authors = _likers_setup(tmp_path)
    # A delete from anyone but the post's author is inert: the post
    # stays visible, so its likers stay disclosed.  The handler checks
    # the row before the tombstone, so a forged delete smuggled in
    # through the store path must not hide a stored post.
    scratch = init_db(tmp_path / "scratch.sqlite3")
    forger = RNS.Identity()
    forged = create_delete(scratch, forger, post["hash"], name="forger", now=2000)
    conn = init_db(node.config.social_db_path)
    try:
        record, is_new = store_post(conn, forged)
        assert is_new is True
        assert record["kind"] == "delete"
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 200
        assert [entry["author"] for entry in payload["likers"]] == [
            authors["dave"],
            authors["alice"],
        ]
        assert payload["total"] == 2
    finally:
        bridge.stop()


def test_tan_import_sanitizes_unencodable_error_hash(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    conn = init_db(node.config.social_db_path)
    conn.close()
    node.app = TanApp(node.config, RNS.Identity(), name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A hash with a lone surrogate comes from untrusted JSON; the
        # response encoder (ensure_ascii=False + UTF-8) cannot write it,
        # so the bridge must replace it instead of failing the request.
        document = {
            "format": "tau-tan-export",
            "version": 1,
            "exported_at": 1000,
            "authors": [],
            "entries": [{"hash": "\ud800", "text": "bogus"}],
        }
        status, _, payload = _json(
            bridge, "/api/tan/import", method="POST", body=document
        )
        assert status == 200
        assert payload["import"]["skipped"] == 1
        # The UTF-8 "replace" handler turns the lone surrogate into "?".
        assert payload["import"]["errors"][0]["hash"] == "?"
    finally:
        bridge.stop()


def test_tan_likers_hides_reactions_per_ruleset(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.rules import Ruleset
    from tau.tangents.tan import TanApp

    node, post, authors = _likers_setup(tmp_path)
    identity = RNS.Identity()
    # A ruleset that displays no reaction kind: the timeline strips its
    # like annotations, so the likers endpoint must disclose nothing.
    node.app = TanApp(
        node.config,
        identity,
        name="tester",
        gossip=False,
        ruleset=Ruleset(kinds=frozenset({"post"})),
    )
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 200
        assert payload["likers"] == []
        assert payload["total"] == 0
    finally:
        bridge.stop()


def test_is_loopback_client():
    from tau.webapi import _is_loopback_client

    assert _is_loopback_client("127.0.0.1") is True
    assert _is_loopback_client("::1") is True
    assert _is_loopback_client("192.168.1.5") is False
    assert _is_loopback_client("not an address") is False


def test_parse_trusted_proxies():
    import ipaddress

    from tau.webapi import WebBridgeError, _parse_trusted_proxies

    assert _parse_trusted_proxies(None) == frozenset()
    assert _parse_trusted_proxies([]) == frozenset()
    assert _parse_trusted_proxies(["127.0.0.1"]) == frozenset(
        [ipaddress.ip_address("127.0.0.1")]
    )
    assert _parse_trusted_proxies(["localhost"]) == frozenset(
        [ipaddress.ip_address("127.0.0.1"), ipaddress.ip_address("::1")]
    )
    assert _parse_trusted_proxies(["  203.0.113.7 "]) == frozenset(
        [ipaddress.ip_address("203.0.113.7")]
    )
    with pytest.raises(WebBridgeError, match="not an IP address"):
        _parse_trusted_proxies(["proxy.example"])
    with pytest.raises(WebBridgeError, match="must not be blank"):
        _parse_trusted_proxies(["  "])


def test_trusted_proxy_value_rejected_at_startup(node, tmp_path):
    from tau.webapi import WebBridge, WebBridgeError

    web_dir = tmp_path / "out"
    web_dir.mkdir()
    with pytest.raises(WebBridgeError, match="not an IP address"):
        WebBridge(
            node,
            host="127.0.0.1",
            port=0,
            web_dir=web_dir,
            trusted_proxies=["not-an-ip"],
        )


def test_tan_likers_caps_at_max_likers(tmp_path, monkeypatch):
    import tau.webapi

    node, post, authors = _likers_setup(tmp_path)
    monkeypatch.setattr(tau.webapi, "MAX_LIKERS", 1)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 200
        assert payload["count"] == 1
        assert payload["total"] == 2
        # The cap keeps the newest liker.
        assert payload["likers"][0]["author"] == authors["dave"]
    finally:
        bridge.stop()


def test_tan_likers_server_error_is_500(tmp_path, monkeypatch):
    import sqlite3

    node, post, authors = _likers_setup(tmp_path)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A storage failure is a server error, not a bad target.
        def _boom():
            raise sqlite3.OperationalError("disk I/O error")

        monkeypatch.setattr(bridge, "_tan_conn", _boom)
        status, _, payload = _json(bridge, f"/api/tan/likes?target={post['hash']}")
        assert status == 500
        assert payload["error"]["message"] == "internal error"
    finally:
        bridge.stop()


def _queue(node, to_hex, text, message_id=None):
    return node.mailbox.enqueue(
        to=bytes.fromhex(to_hex),
        text=text,
        message_id=message_id,
    )


def test_doctor_endpoint_lists_all_checks(bridge, node, tmp_path):
    node.config = TauConfig.resolve(data_dir=tmp_path)

    status, _, payload = _json(bridge, "/api/doctor")
    assert status == 200
    # A fresh directory has no identity, so the verdict is not ok.
    assert payload["ok"] is False
    assert [check["name"] for check in payload["checks"]] == [
        "identity",
        "node metadata",
        "peers",
        "mailbox",
        "inbox",
        "messages",
        "tan",
        "tan rules",
        "allowlist",
        "instance lock",
    ]
    for check in payload["checks"]:
        assert check["status"] in ("ok", "warn", "fail")
        assert isinstance(check["detail"], str)
    by_name = {check["name"]: check for check in payload["checks"]}
    assert by_name["identity"]["status"] == "fail"
    assert by_name["peers"]["status"] == "ok"  # no peers yet is healthy


def test_doctor_reports_a_corrupt_store(bridge, node, tmp_path):
    node.config = TauConfig.resolve(data_dir=tmp_path)
    (tmp_path / "peers.json").write_text("{not json", encoding="utf-8")

    result = bridge.doctor()
    assert result["ok"] is False
    by_name = {check["name"]: check for check in result["checks"]}
    assert by_name["peers"]["status"] == "fail"
    assert "peers.json" in by_name["peers"]["detail"]


def test_doctor_rejects_a_concurrent_run(bridge, node, tmp_path):
    node.config = TauConfig.resolve(data_dir=tmp_path)
    assert bridge._doctor_lock.acquire(blocking=False)
    try:
        with pytest.raises(WebBridgeError, match="already running"):
            bridge.doctor()
    finally:
        bridge._doctor_lock.release()


def test_doctor_endpoint_reports_busy_when_a_check_is_running(
    bridge, node, tmp_path
):
    node.config = TauConfig.resolve(data_dir=tmp_path)
    assert bridge._doctor_lock.acquire(blocking=False)
    try:
        status, _, payload = _json(bridge, "/api/doctor")
    finally:
        bridge._doctor_lock.release()
    assert status == 503
    assert payload["error"]["message"] == "a health check is already running"


def test_outbox_empty(bridge):
    status, _, payload = _json(bridge, "/api/outbox")
    assert status == 200
    assert payload == {"outbox": [], "count": 0, "held_for_others": 0}


def test_outbox_lists_entries_with_names(bridge, node):
    first = _queue(node, PEER_HASH, "hello alice")
    second = _queue(node, OTHER_HASH, "hello stranger")
    node.mailbox.record_attempt(first.id, error="no route")

    status, _, payload = _json(bridge, "/api/outbox")
    assert status == 200
    assert payload["count"] == 2
    assert payload["held_for_others"] == 0
    by_id = {entry["id"]: entry for entry in payload["outbox"]}
    assert by_id[first.id]["to_name"] == "alice"
    assert by_id[first.id]["attempts"] == 1
    assert by_id[first.id]["last_error"] == "no route"
    assert by_id[first.id]["held_by"] == []
    # Unknown destination: falls back to the name stored at enqueue time.
    assert by_id[second.id]["to"] == OTHER_HASH
    assert by_id[second.id]["to_name"] == ""
    assert by_id[second.id]["text"] == "hello stranger"


def test_outbox_cancel_by_id_and_prefix(bridge, node):
    first = _queue(node, PEER_HASH, "first")
    second = _queue(node, PEER_HASH, "second")

    status, _, payload = _json(
        bridge, "/api/outbox/cancel", method="POST", body={"target": first.id}
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["cancelled"]["id"] == first.id
    assert payload["cancelled"]["to_name"] == "alice"

    status, _, payload = _json(
        bridge,
        "/api/outbox/cancel",
        method="POST",
        body={"target": second.id[:8]},
    )
    assert status == 200
    assert payload["cancelled"]["id"] == second.id

    status, _, payload = _json(bridge, "/api/outbox")
    assert payload == {"outbox": [], "count": 0, "held_for_others": 0}


def test_outbox_cancel_drops_sibling_group_edit_legs(bridge, node):
    import base64

    group = {
        "id": "aa" * 16,
        "name": "crew",
        "creator": "bb" * 16,
        "members": ["cc" * 16, "dd" * 16, "ee" * 16],
    }
    edit = {
        "target": "leg-1",
        "text": "fixed here",
        "edit_id": "edit-1",
        "sent_at": 100.0,
        "payload_b64": base64.b64encode(b"payload").decode("ascii"),
    }
    first = node.mailbox.enqueue(
        to=bytes.fromhex("cc" * 16),
        text="fixed here",
        group=group,
        edit=dict(edit),
        message_id="aa" + "11" * 15,
    )
    node.mailbox.enqueue(
        to=bytes.fromhex("dd" * 16),
        text="fixed here",
        group=group,
        edit=dict(edit),
        message_id="aa" + "22" * 15,
    )
    _queue(node, PEER_HASH, "keep me", message_id="bb" + "22" * 15)

    status, _, payload = _json(
        bridge, "/api/outbox/cancel", method="POST", body={"target": first.id}
    )
    assert status == 200
    assert payload["cancelled"]["id"] == first.id
    # Cancelling one leg of a queued group edit cancels the whole edit;
    # the unrelated entry stays queued.
    status, _, payload = _json(bridge, "/api/outbox")
    assert [entry["id"] for entry in payload["outbox"]] == [
        "bb" + "22" * 15
    ]


def test_outbox_cancel_errors(bridge, node):
    status, _, payload = _json(
        bridge, "/api/outbox/cancel", method="POST", body={}
    )
    assert status == 400
    assert "target" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/outbox/cancel",
        method="POST",
        body={"target": "ff" * 16},
    )
    assert status == 404
    assert "unknown queued message" in payload["error"]["message"]

    # Two entries sharing a prefix make the prefix ambiguous.
    _queue(node, PEER_HASH, "one", message_id="aa" + "11" * 15)
    _queue(node, PEER_HASH, "two", message_id="aa" + "22" * 15)
    status, _, payload = _json(
        bridge, "/api/outbox/cancel", method="POST", body={"target": "aa"}
    )
    assert status == 404
    assert "match prefix" in payload["error"]["message"]

    # Cancelling an entry that left the outbox meanwhile is a 404 too.
    gone = _queue(node, PEER_HASH, "gone", message_id="bb" + "33" * 15)
    assert node.mailbox.remove_outbox(gone.id) is True
    status, _, payload = _json(
        bridge, "/api/outbox/cancel", method="POST", body={"target": gone.id}
    )
    assert status == 404
    assert "unknown queued message" in payload["error"]["message"]


def _queue_scheduled(node, to_hex, text, message_id=None, delay=7200.0):
    return node.mailbox.enqueue(
        to=bytes.fromhex(to_hex),
        text=text,
        message_id=message_id,
        send_after=time.time() + delay,
    )


def test_outbox_edit_by_id_and_prefix(bridge, node):
    first = _queue_scheduled(node, PEER_HASH, "first", "aa" + "11" * 15)
    second = _queue_scheduled(node, PEER_HASH, "second", "bb" + "22" * 15)

    status, _, payload = _json(
        bridge,
        "/api/outbox/edit",
        method="POST",
        body={"target": first.id, "text": "first, reworded"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["legs"] == 1
    assert payload["edited"][0]["id"] == first.id
    assert payload["edited"][0]["text"] == "first, reworded"
    assert payload["edited"][0]["send_after"] == first.send_after

    before = time.time()
    status, _, payload = _json(
        bridge,
        "/api/outbox/edit",
        method="POST",
        body={"target": second.id[:8], "send_in": 3600},
    )
    assert status == 200
    assert payload["edited"][0]["text"] == "second"
    assert abs(payload["edited"][0]["send_after"] - (before + 3600)) < 5.0

    _, _, listed = _json(bridge, "/api/outbox")
    by_id = {entry["id"]: entry for entry in listed["outbox"]}
    assert by_id[first.id]["text"] == "first, reworded"
    assert by_id[second.id]["send_after"] > before + 3500


def test_outbox_edit_errors(bridge, node):
    scheduled = _queue_scheduled(node, PEER_HASH, "later", "cc" + "11" * 15)
    immediate = _queue(node, PEER_HASH, "now")

    # No change requested.
    status, _, payload = _json(
        bridge,
        "/api/outbox/edit",
        method="POST",
        body={"target": scheduled.id},
    )
    assert status == 400
    assert "nothing to change" in payload["error"]["message"]

    # Missing target.
    status, _, payload = _json(
        bridge,
        "/api/outbox/edit",
        method="POST",
        body={"text": "nope"},
    )
    assert status == 400
    assert "target" in payload["error"]["message"]

    # Unknown id is a 404; an ambiguous prefix too.
    status, _, payload = _json(
        bridge,
        "/api/outbox/edit",
        method="POST",
        body={"target": "ff" * 16, "text": "nope"},
    )
    assert status == 404
    assert "unknown queued message" in payload["error"]["message"]

    # A message that is not scheduled cannot be edited.
    status, _, payload = _json(
        bridge,
        "/api/outbox/edit",
        method="POST",
        body={"target": immediate.id, "text": "too late"},
    )
    assert status == 400
    assert "no longer scheduled" in payload["error"]["message"]
    assert node.mailbox.get_outbox(immediate.id).text == "now"

    # Bad send_in values are 400s, and the entry is untouched.
    for bad in (0, -30, "soon", 30 * 86400):
        status, _, payload = _json(
            bridge,
            "/api/outbox/edit",
            method="POST",
            body={"target": scheduled.id, "send_in": bad},
        )
        assert status == 400
        assert "send_in" in payload["error"]["message"]
    assert node.mailbox.get_outbox(scheduled.id).text == "later"


def test_outbox_edit_group_updates_every_leg(bridge, node):
    crew = node.groups.list_groups()[0]
    group = {
        "id": crew["id"],
        "name": "crew",
        "creator": PEER_HASH,
        "members": [PEER_HASH, BOB_HASH],
    }
    first = node.mailbox.enqueue(
        to=bytes.fromhex(BOB_HASH),
        text="crew hello",
        message_id="dd" + "11" * 15,
        send_after=time.time() + 7200.0,
        sent_at=1000.0,
        group=group,
    )
    second = node.mailbox.enqueue(
        to=bytes.fromhex(OTHER_HASH),
        text="crew hello",
        message_id="dd" + "22" * 15,
        send_after=time.time() + 7200.0,
        sent_at=1000.0,
        group=group,
    )
    status, _, payload = _json(
        bridge,
        "/api/outbox/edit",
        method="POST",
        body={"target": first.id[:8], "text": "crew hello, edited"},
    )
    assert status == 200
    assert payload["legs"] == 2
    assert {entry["id"] for entry in payload["edited"]} == {
        first.id,
        second.id,
    }
    assert node.mailbox.get_outbox(first.id).text == "crew hello, edited"
    assert node.mailbox.get_outbox(second.id).text == "crew hello, edited"


def test_settings_defaults(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        status, _, payload = _json(bridge, "/api/settings")
        assert status == 200
        # A fresh data directory has no node.json: the CLI defaults apply.
        assert payload == {
            "accept_files": False,
            "send_read_receipts": True,
            "tan_sync_depth": 2,
            "announce_interval": 300.0,
        }
        assert not (tmp_path / "node.json").exists()
    finally:
        bridge.stop()


def test_settings_update_persists_and_keeps_the_other(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        status, _, payload = _json(
            bridge,
            "/api/settings",
            method="POST",
            body={"setting": "accept_files", "value": True},
        )
        assert status == 200
        assert payload == {
            "ok": True,
            "settings": {
                "accept_files": True,
                "send_read_receipts": True,
                "tan_sync_depth": 2,
                "announce_interval": 300.0,
            },
        }

        # Toggling the second setting keeps the first, on disk and on read.
        status, _, payload = _json(
            bridge,
            "/api/settings",
            method="POST",
            body={"setting": "send_read_receipts", "value": False},
        )
        assert status == 200
        assert payload["settings"] == {
            "accept_files": True,
            "send_read_receipts": False,
            "tan_sync_depth": 2,
            "announce_interval": 300.0,
        }

        # The depth setting takes 1 or 2 and keeps the others too.
        status, _, payload = _json(
            bridge,
            "/api/settings",
            method="POST",
            body={"setting": "tan_sync_depth", "value": 1},
        )
        assert status == 200
        assert payload["settings"] == {
            "accept_files": True,
            "send_read_receipts": False,
            "tan_sync_depth": 1,
            "announce_interval": 300.0,
        }
        status, _, payload = _json(bridge, "/api/settings")
        assert payload == {
            "accept_files": True,
            "send_read_receipts": False,
            "tan_sync_depth": 1,
            "announce_interval": 300.0,
        }
        stored = json.loads(
            (tmp_path / "node.json").read_text(encoding="utf-8")
        )
        assert stored["accept_files"] is True
        assert stored["send_read_receipts"] is False
        assert stored["tan_sync_depth"] == 1
    finally:
        bridge.stop()


def test_settings_announce_interval_update_and_disable(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        # A number of seconds persists and keeps the other settings.
        status, _, payload = _json(
            bridge,
            "/api/settings",
            method="POST",
            body={"setting": "announce_interval", "value": 60},
        )
        assert status == 200
        assert payload["settings"] == {
            "accept_files": False,
            "send_read_receipts": True,
            "tan_sync_depth": 2,
            "announce_interval": 60.0,
        }

        # 0 disables automatic announces.
        status, _, payload = _json(
            bridge,
            "/api/settings",
            method="POST",
            body={"setting": "announce_interval", "value": 0},
        )
        assert status == 200
        assert payload["settings"]["announce_interval"] == 0.0
        stored = json.loads(
            (tmp_path / "node.json").read_text(encoding="utf-8")
        )
        assert stored["announce_interval"] == 0.0
    finally:
        bridge.stop()


def test_settings_update_rejects_bad_input(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        for body, fragment in [
            ({}, "setting"),
            ({"setting": "accept_files"}, "value"),
            ({"setting": "name", "value": "bob"}, "unknown setting"),
            ({"setting": "accept_files", "value": "yes"}, "must be true or false"),
            ({"setting": "accept_files", "value": 1}, "must be true or false"),
            ({"setting": "accept_files", "value": None}, "must be true or false"),
            ({"setting": "tan_sync_depth", "value": 3}, "must be 1 or 2"),
            ({"setting": "tan_sync_depth", "value": 0}, "must be 1 or 2"),
            ({"setting": "tan_sync_depth", "value": True}, "must be 1 or 2"),
            ({"setting": "tan_sync_depth", "value": "2"}, "must be 1 or 2"),
            (
                {"setting": "announce_interval", "value": -1},
                "must be 0 or more seconds",
            ),
            (
                {"setting": "announce_interval", "value": True},
                "must be 0 or more seconds",
            ),
            (
                {"setting": "announce_interval", "value": "60"},
                "must be 0 or more seconds",
            ),
            (
                {"setting": "announce_interval", "value": 10**400},
                "must be 0 or more seconds",
            ),
            (
                {"setting": "announce_interval", "value": 1e308},
                "must be 0 or more seconds",
            ),
        ]:
            status, _, payload = _json(
                bridge, "/api/settings", method="POST", body=body
            )
            assert status == 400
            assert fragment in payload["error"]["message"]
    finally:
        bridge.stop()


def test_settings_without_data_dir(bridge, node):
    # A bridge on a dataless node has nowhere to read or persist settings.
    node.data_dir = None
    status, _, payload = _json(bridge, "/api/settings")
    assert status == 503
    assert "no data directory" in payload["error"]["message"]
    status, _, payload = _json(
        bridge,
        "/api/settings",
        method="POST",
        body={"setting": "accept_files", "value": True},
    )
    assert status == 503
    assert "no data directory" in payload["error"]["message"]


def test_outbox_retry_one_by_id_and_prefix(bridge, node):
    first = _queue(node, PEER_HASH, "first")
    second = _queue(node, PEER_HASH, "second")

    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={"target": first.id}
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["delivered"] == 1
    assert payload["still_queued"] == 1
    assert payload["target"] == first.id
    assert payload["target_delivered"] is True
    assert payload["target_still_queued"] is False
    # Backoff is ignored: the operator asked for a retry *now*.
    assert node.flush_calls[-1]["respect_backoff"] is False
    assert node.flush_calls[-1]["message_id"] == first.id
    assert node.flush_calls[-1]["to"] == bytes.fromhex(PEER_HASH)
    assert node.mailbox.get_outbox(first.id) is None

    status, _, payload = _json(
        bridge,
        "/api/outbox/retry",
        method="POST",
        body={"id": second.id[:8]},
    )
    assert status == 200
    assert payload["target_delivered"] is True
    assert payload["target_still_queued"] is False
    assert payload["still_queued"] == 0

    # A targeted retry against an unreachable peer leaves the entry
    # queued and says so: not delivered is not the same as gone.
    third = _queue(node, PEER_HASH, "third")
    node.flush_deliver_target = False
    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={"target": third.id}
    )
    assert status == 200
    assert payload["delivered"] == 0
    assert payload["target_delivered"] is False
    assert payload["target_still_queued"] is True
    assert payload["still_queued"] == 1
    assert node.mailbox.get_outbox(third.id) is not None

    status, _, payload = _json(bridge, "/api/outbox")
    assert [item["id"] for item in payload["outbox"]] == [third.id]


def test_outbox_retry_all_reports_what_is_left(bridge, node):
    first = _queue(node, PEER_HASH, "first")
    _queue(node, PEER_HASH, "second")

    # Peers unreachable: nothing moves, everything stays queued.
    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={}
    )
    assert status == 200
    assert payload["delivered"] == 0
    assert payload["still_queued"] == 2
    assert "target" not in payload
    assert node.flush_calls[-1]["message_id"] is None
    assert node.flush_calls[-1]["to"] is None

    # Peers reachable: the whole queue drains.
    node.flush_deliver_all = True
    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={}
    )
    assert status == 200
    assert payload["delivered"] == 2
    assert payload["still_queued"] == 0
    assert node.mailbox.get_outbox(first.id) is None


def test_outbox_retry_errors(bridge, node):
    # A non-string target is a 400, not a retry-all.
    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={"target": 42}
    )
    assert status == 400
    assert "target" in payload["error"]["message"]

    # A supplied-but-blank target is a 400 too: {"target": ""} must
    # not silently become a retry of the whole queue.
    for body in ({"target": ""}, {"target": "   "}, {"id": ""}, {"target": 0}):
        status, _, payload = _json(
            bridge, "/api/outbox/retry", method="POST", body=body
        )
        assert status == 400
        assert "target" in payload["error"]["message"]
    assert node.flush_calls == []

    # Unknown and ambiguous ids are 404s, like cancel.
    status, _, payload = _json(
        bridge,
        "/api/outbox/retry",
        method="POST",
        body={"target": "ff" * 16},
    )
    assert status == 404
    assert "unknown queued message" in payload["error"]["message"]

    _queue(node, PEER_HASH, "one", message_id="aa" + "11" * 15)
    _queue(node, PEER_HASH, "two", message_id="aa" + "22" * 15)
    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={"target": "aa"}
    )
    assert status == 404
    assert "match prefix" in payload["error"]["message"]

    # A mail round already running is a 409: the retry never ran, so
    # nothing is reported as delivered.
    node.flush_lock_busy = True
    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={}
    )
    assert status == 409
    assert "already running" in payload["error"]["message"]
    assert node.mailbox.counts()["outbox"] == 2

def test_outbox_retry_counts_concurrent_delivery(bridge, node):
    # A concurrent mail round delivers the entry after resolution but
    # before this flush visits it: the flush reports no delivery and
    # the entry is gone, yet the bridge's delivery record confirms it
    # was delivered — not expired or cancelled.
    entry = _queue(node, PEER_HASH, "concurrent")
    node.flush_concurrent_delivery = True
    assert bridge.record_delivery(entry.id) is None  # buffered: no console record

    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={"target": entry.id}
    )
    assert status == 200
    assert payload["delivered"] == 0
    assert payload["target_delivered"] is True
    assert payload["target_still_queued"] is False
    assert node.mailbox.get_outbox(entry.id) is None

def test_outbox_retry_unconfirmed_departure(bridge, node):
    # The entry leaves the queue with no delivery recorded anywhere —
    # expired, or cancelled by another process. The response says so
    # honestly (not delivered, not queued) instead of guessing.
    entry = _queue(node, PEER_HASH, "gone")
    node.flush_concurrent_delivery = True

    status, _, payload = _json(
        bridge, "/api/outbox/retry", method="POST", body={"target": entry.id}
    )
    assert status == 200
    assert payload["delivered"] == 0
    assert payload["target_delivered"] is False
    assert payload["target_still_queued"] is False

def _hold(node, to_hex, message_id, blob=b"sealed-bytes", expires_at=None,
          depositor=None, now=None):
    return node.mailbox.hold(
        to=bytes.fromhex(to_hex),
        blob=blob,
        message_id=message_id,
        expires_at=expires_at if expires_at is not None else time.time() + 3600,
        now=now,
        depositor=bytes.fromhex(depositor) if depositor else None,
    )

def test_outbox_held_lists_envelopes_with_names(bridge, node):
    first = "cc" + "11" * 15
    second = "dd" + "22" * 15
    assert _hold(
        node, PEER_HASH, first, blob=b"sealed-one",
        expires_at=5000.0, depositor=OTHER_HASH, now=100.0,
    )
    assert _hold(
        node, OTHER_HASH, second, blob=b"sealed-two!",
        expires_at=6000.0, depositor=PEER_HASH, now=200.0,
    )

    status, _, payload = _json(bridge, "/api/outbox/held")
    assert status == 200
    assert payload["count"] == 2
    by_id = {entry["id"]: entry for entry in payload["held"]}
    assert by_id[first]["to"] == PEER_HASH
    assert by_id[first]["to_name"] == "alice"
    # Unknown depositor: no name, but the hash is still shown.
    assert by_id[first]["from"] == OTHER_HASH
    assert by_id[first]["from_name"] == ""
    assert by_id[first]["stored_at"] == 100.0
    assert by_id[first]["expires_at"] == 5000.0
    assert by_id[first]["blob_bytes"] == len(b"sealed-one")
    # Unknown recipient: falls back to the hash, while the known
    # depositor resolves to a name.
    assert by_id[second]["to"] == OTHER_HASH
    assert by_id[second]["to_name"] == ""
    assert by_id[second]["from"] == PEER_HASH
    assert by_id[second]["from_name"] == "alice"
    assert by_id[second]["blob_bytes"] == len(b"sealed-two!")
    # Oldest first.
    assert [entry["id"] for entry in payload["held"]] == [first, second]

def test_outbox_drop_held_by_id_and_prefix(bridge, node):
    first = "cc" + "11" * 15
    second = "dd" + "22" * 15
    _hold(node, PEER_HASH, first)
    _hold(node, PEER_HASH, second)

    status, _, payload = _json(
        bridge, "/api/outbox/drop-held", method="POST", body={"target": first}
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["dropped"]["id"] == first
    assert payload["dropped"]["to_name"] == "alice"
    assert "already fetched" in payload["dropped"]["note"]
    # The tombstone stops a sender retry resurrecting the envelope.
    assert _hold(node, PEER_HASH, first) is False

    status, _, payload = _json(
        bridge, "/api/outbox/drop-held", method="POST", body={"target": second[:8]}
    )
    assert status == 200
    assert payload["dropped"]["id"] == second

    status, _, payload = _json(bridge, "/api/outbox/held")
    assert payload == {"held": [], "count": 0}

def test_outbox_drop_held_errors(bridge, node):
    status, _, payload = _json(
        bridge, "/api/outbox/drop-held", method="POST", body={}
    )
    assert status == 400
    assert "target" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/outbox/drop-held", method="POST", body={"target": "ff" * 16}
    )
    assert status == 400
    assert "unknown held message" in payload["error"]["message"]

    # Two envelopes sharing a prefix make the prefix ambiguous.
    _hold(node, PEER_HASH, "aa" + "11" * 15)
    _hold(node, PEER_HASH, "aa" + "22" * 15)
    status, _, payload = _json(
        bridge, "/api/outbox/drop-held", method="POST", body={"target": "aa"}
    )
    assert status == 400
    assert "match prefix" in payload["error"]["message"]

    # Dropping an envelope that is already gone is a 404 too.
    gone = "bb" + "33" * 15
    _hold(node, PEER_HASH, gone)
    status, _, payload = _json(
        bridge, "/api/outbox/drop-held", method="POST", body={"target": gone}
    )
    assert status == 200
    status, _, payload = _json(
        bridge, "/api/outbox/drop-held", method="POST", body={"target": gone}
    )
    assert status == 400
    assert "unknown held message" in payload["error"]["message"]


def test_outbox_sees_entries_queued_by_another_process(bridge, node, tmp_path):
    from tau.mailbox import Mailbox

    other = Mailbox(tmp_path / "mailbox.json")
    entry = other.enqueue(to=bytes.fromhex(PEER_HASH), text="queued by cli")
    # The serving node's in-memory snapshot predates the other process.
    assert node.mailbox.get_outbox(entry.id) is None

    status, _, payload = _json(bridge, "/api/outbox")
    assert status == 200
    assert [item["id"] for item in payload["outbox"]] == [entry.id]
    assert payload["outbox"][0]["to_name"] == "alice"

    status, _, payload = _json(
        bridge, "/api/outbox/cancel", method="POST", body={"target": entry.id}
    )
    assert status == 200
    assert payload["cancelled"]["id"] == entry.id
    # A fresh view of the file agrees the entry is gone.
    assert Mailbox(tmp_path / "mailbox.json").get_outbox(entry.id) is None


def test_allowlist_get_empty(bridge):
    status, _, payload = _json(bridge, "/api/allowlist")
    assert status == 200
    assert payload == {"enabled": False, "entries": []}


def test_allowlist_add_by_name_and_remove(bridge, node):
    status, _, payload = _json(
        bridge, "/api/allowlist/add", method="POST", body={"target": "alice"}
    )
    assert status == 200
    assert payload["enabled"] is False
    assert payload["entries"] == [
        {"destination": PEER_HASH, "name": "alice"}
    ]

    # The entry survives a fresh GET, with its registry name attached.
    status, _, payload = _json(bridge, "/api/allowlist")
    assert status == 200
    assert payload["entries"] == [
        {"destination": PEER_HASH, "name": "alice"}
    ]

    status, _, payload = _json(
        bridge, "/api/allowlist/remove", method="POST", body={"target": "alice"}
    )
    assert status == 200
    assert payload == {"enabled": False, "entries": []}


def test_allowlist_add_by_raw_hash_has_no_name(bridge):
    status, _, payload = _json(
        bridge, "/api/allowlist/add", method="POST", body={"target": OTHER_HASH}
    )
    assert status == 200
    assert payload["entries"] == [{"destination": OTHER_HASH, "name": ""}]


def test_allowlist_add_by_prefix(bridge):
    status, _, payload = _json(
        bridge, "/api/allowlist/add", method="POST", body={"target": "abab"}
    )
    assert status == 200
    assert payload["entries"] == [
        {"destination": PEER_HASH, "name": "alice"}
    ]


def test_allowlist_add_rejects_alias(bridge, node):
    # The allow-list is a security boundary: a local alias must never
    # stand in for the peer's asserted name, so the fixture peer's
    # alias "ally" does not resolve here even though the registry
    # would resolve it with include_alias=True.
    node.registry.get(PEER_HASH).alias = "ally"
    status, _, payload = _json(
        bridge, "/api/allowlist/add", method="POST", body={"target": "ally"}
    )
    assert status == 400
    assert "unknown peer" in payload["error"]["message"]


def test_allowlist_set_enabled_applies_immediately(bridge, node, tmp_path):
    _json(bridge, "/api/allowlist/add", method="POST", body={"target": "alice"})

    status, _, payload = _json(
        bridge,
        "/api/allowlist/set-enabled",
        method="POST",
        body={"enabled": True},
    )
    assert status == 200
    assert payload["enabled"] is True
    assert [entry["destination"] for entry in payload["entries"]] == [PEER_HASH]

    # The running node's in-memory gate changed too — no restart needed.
    assert node.allowlist.is_allowed(PEER_HASH) is True
    assert node.allowlist.is_allowed(OTHER_HASH) is False
    # And the file agrees, so a restart keeps the same state.
    from tau.allowlist import load_allowlist

    saved = load_allowlist(tmp_path / "allowlist.json")
    assert saved.enabled is True
    assert saved.allow == frozenset({PEER_HASH})


def test_allowlist_errors(bridge):
    status, _, payload = _json(
        bridge, "/api/allowlist/add", method="POST", body={}
    )
    assert status == 400
    assert "target" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/allowlist/add", method="POST", body={"target": "  "}
    )
    assert status == 400
    assert "target" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/allowlist/add", method="POST", body={"target": "nobody"}
    )
    assert status == 400
    assert "unknown peer" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/allowlist/remove",
        method="POST",
        body={"target": "nobody"},
    )
    assert status == 400
    assert "unknown peer" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/allowlist/set-enabled",
        method="POST",
        body={"enabled": "yes"},
    )
    assert status == 400
    assert "boolean" in payload["error"]["message"]


def test_allowlist_concurrent_adds_keep_every_entry(bridge):
    # Overlapping mutations must not lose each other's changes: each
    # add derives from the latest saved list, not a stale snapshot.
    hashes = [f"{n:02x}" * 16 for n in range(1, 11)]
    errors = []

    def add_one(destination):
        try:
            status, _, _ = _json(
                bridge,
                "/api/allowlist/add",
                method="POST",
                body={"target": destination},
            )
            assert status == 200
        except Exception as exc:  # noqa: BLE001 - collected, asserted below
            errors.append(exc)

    threads = [
        threading.Thread(target=add_one, args=(destination,))
        for destination in hashes
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []

    status, _, payload = _json(bridge, "/api/allowlist")
    assert status == 200
    assert [entry["destination"] for entry in payload["entries"]] == sorted(
        hashes
    )


def test_allowlist_toggle_keeps_cli_saved_entries(bridge, node, tmp_path):
    from tau.allowlist import AllowList, save_allowlist

    # The CLI saved a change while this node runs; the running node's
    # in-memory list does not know about it yet.
    save_allowlist(
        tmp_path / "allowlist.json",
        AllowList(enabled=False, allow=frozenset({OTHER_HASH})),
    )
    assert node.allowlist.is_allowed(OTHER_HASH) is True  # not enforced yet

    status, _, payload = _json(
        bridge,
        "/api/allowlist/set-enabled",
        method="POST",
        body={"enabled": True},
    )
    assert status == 200
    # The console toggle reconciled with the saved file instead of
    # overwriting it with the stale in-memory list.
    assert [entry["destination"] for entry in payload["entries"]] == [
        OTHER_HASH
    ]
    assert node.allowlist.is_allowed(OTHER_HASH) is True
    assert node.allowlist.is_allowed(PEER_HASH) is False


def test_allowlist_refuses_to_overwrite_corrupt_file(bridge, node, tmp_path):
    path = tmp_path / "allowlist.json"
    path.write_text("{not json", encoding="utf-8")
    before = path.read_bytes()

    status, _, payload = _json(
        bridge, "/api/allowlist/add", method="POST", body={"target": "alice"}
    )
    assert status == 400
    assert "allow-list" in payload["error"]["message"]
    # The file is untouched and the running node's in-memory list is
    # unchanged by the failed edit.
    assert path.read_bytes() == before
    assert node.allowlist.enabled is False
    assert node.allowlist.allow == frozenset()


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _hold_port(port: int) -> socket.socket:
    holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    holder.bind(("127.0.0.1", port))
    holder.listen(1)
    return holder


def test_check_web_console_bind_passes_on_free_port():
    check_web_console_bind("127.0.0.1", _free_port())


def test_check_web_console_bind_names_held_port():
    port = _free_port()
    holder = _hold_port(port)
    try:
        with pytest.raises(WebBridgeError) as excinfo:
            check_web_console_bind("127.0.0.1", port)
    finally:
        holder.close()
    assert f"web console port {port} on 127.0.0.1 is already in use" in str(
        excinfo.value
    )


def test_check_web_console_bind_windows_names_live_listener(monkeypatch):
    # The Windows probe connects instead of binding: a live listener
    # completes the handshake, so the probe must report it.
    port = _free_port()
    holder = _hold_port(port)
    try:
        monkeypatch.setattr(sys, "platform", "win32")
        with pytest.raises(WebBridgeError) as excinfo:
            check_web_console_bind("127.0.0.1", port)
    finally:
        holder.close()
    assert f"web console port {port} on 127.0.0.1 is already in use" in str(
        excinfo.value
    )


def test_check_web_console_bind_windows_passes_on_free_port(monkeypatch):
    # ...while a free port refuses the connection, so the probe passes
    # even where a TIME_WAIT straggler could linger.
    monkeypatch.setattr(sys, "platform", "win32")
    check_web_console_bind("127.0.0.1", _free_port())


def test_bridge_start_names_held_port_instead_of_raw_oserror(node):
    port = _free_port()
    holder = _hold_port(port)
    try:
        bridge = WebBridge(node, host="127.0.0.1", port=port)
        with pytest.raises(WebBridgeError) as excinfo:
            bridge.start()
    finally:
        holder.close()
    assert f"web console port {port} on 127.0.0.1 is already in use" in str(
        excinfo.value
    )
def _queue_send(node, mail_id="mail-1"):
    """Make the fake node report a queued send with the given outbox id."""

    def _send(peer, text, **kwargs):
        node.last_send_kwargs = kwargs
        return {
            "ok": True,
            "v": 1,
            "queued": True,
            "id": mail_id,
            "to": peer.hexhash,
            "expires_at": time.time() + 3600,
        }

    node.send_message = _send


def test_queued_send_records_outbox_id(bridge, node):
    _queue_send(node)
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    assert status == 200
    assert payload["message"]["status"] == "queued"
    assert payload["message"]["mail_id"] == "mail-1"


def test_direct_send_records_no_outbox_id(bridge):
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    assert status == 200
    assert payload["message"]["status"] == "delivered"
    assert "mail_id" not in payload["message"]


def test_record_delivery_flips_queued_message_and_publishes(bridge, node):
    _queue_send(node)
    _, _, sent = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    message_id = sent["message"]["id"]

    updated = bridge.record_delivery("mail-1")
    assert updated is not None
    assert updated["id"] == message_id
    assert updated["status"] == "delivered"

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["status"] == "delivered"

    events = bridge.bus.replay(0)
    updates = [event for event in events if event["type"] == "message_updated"]
    assert len(updates) == 1
    assert updates[0]["message"]["id"] == message_id
    assert updates[0]["message"]["status"] == "delivered"


def test_record_delivery_reads_route_before_bridge_lock(bridge, node):
    # CodeRabbit Major: _delivery_route takes the mailbox lock, so
    # it must not run while the bridge lock is held — an inbound
    # retraction takes the locks in the opposite order (mailbox
    # transaction, then the bridge lock through on_retract), and the
    # two paths would deadlock if they overlapped.
    _queue_send(node)
    _json(bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"})
    seen = []
    real_sent_record = node.mailbox.sent_record

    def spy(message_id):
        seen.append(bridge._lock.locked())
        return real_sent_record(message_id)

    node.mailbox.sent_record = spy
    try:
        updated = bridge.record_delivery("mail-1")
    finally:
        node.mailbox.sent_record = real_sent_record
    assert updated is not None
    assert updated["status"] == "delivered"
    assert seen == [False]


def test_send_message_flip_reads_route_before_bridge_lock(bridge, node):
    # Same lock-order finding, second path: a delivery report that
    # arrived before the send is reconciled in send_message's
    # early-delivery flip, which also read the route under the
    # bridge lock.
    _queue_send(node)
    bridge._early_deliveries["mail-1"] = None
    seen = []
    real_sent_record = node.mailbox.sent_record

    def spy(message_id):
        seen.append(bridge._lock.locked())
        return real_sent_record(message_id)

    node.mailbox.sent_record = spy
    try:
        _, _, sent = _json(
            bridge, "/api/messages", method="POST",
            body={"to": "alice", "text": "hi"},
        )
    finally:
        node.mailbox.sent_record = real_sent_record
    assert sent["message"]["status"] == "delivered"
    assert seen == [False]


def test_record_delivery_updates_route_to_lxmf_after_direct_delivery(
    bridge, node
):
    # CodeRabbit Major: a message delivered directly and later
    # re-delivered over LXMF must not keep the stale "direct"
    # route — the recipient now holds an lxmf: copy, and a retract
    # reading "direct" would miss it while reporting success.
    _queue_send(node)
    node.mailbox.record_sent(
        "mail-1", to=bytes.fromhex(PEER_HASH), text="hi", sent_at=1000.0
    )
    node.mailbox.record_delivered("mail-1", now=1002.0, via="direct")
    _json(bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"})
    updated = bridge.record_delivery("mail-1")
    assert updated is not None
    assert updated["delivered_via"] == "direct"

    node.mailbox.record_delivered("mail-1", now=1003.0, via="lxmf")
    updated = bridge.record_delivery("mail-1")
    assert updated is not None
    assert updated["delivered_via"] == "lxmf"

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["delivered_via"] == "lxmf"
    assert feed["messages"][0]["status"] == "delivered"


def test_record_delivery_ignores_unknown_id(bridge, node):
    _queue_send(node)
    _json(bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"})

    assert bridge.record_delivery("not-an-outbox-id") is None
    assert bridge.record_delivery("mail-1") is not None
    # A second delivery report for the same id is a no-op: it is no
    # longer queued, so there is nothing left to flip.
    assert bridge.record_delivery("mail-1") is None

    events = bridge.bus.replay(0)
    updates = [event for event in events if event["type"] == "message_updated"]
    assert len(updates) == 1


def test_delivery_flip_survives_bridge_restart(node, tmp_path):
    """The delivered status is persisted, not just held in memory.

    Regression test for the PR #35 / PR #32 merge: record_delivery was
    written against the in-memory-only bridge, so without a save the
    flip was lost on restart and the message came back queued.
    """
    _queue_send(node)
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        message, _ = bridge.send_message("alice", "hi")
        assert message["status"] == "queued"
        assert bridge.record_delivery("mail-1") is not None
    finally:
        bridge.stop()

    bridge2 = _bridge_with_data_dir(FakeNode(), tmp_path)
    try:
        assert bridge2.messages()[0]["status"] == "delivered"
    finally:
        bridge2.stop()


def test_queued_message_keeps_mail_id_across_restart(node, tmp_path):
    """A still-queued message can flip after a restart.

    The persisted record must keep its outbox id; the history loader
    originally rebuilt records from known keys and dropped it, leaving
    the message unmatchable when delivery was finally reported.
    """
    _queue_send(node)
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        bridge.send_message("alice", "hi")
    finally:
        bridge.stop()

    bridge2 = _bridge_with_data_dir(FakeNode(), tmp_path)
    try:
        assert bridge2.messages()[0]["status"] == "queued"
        updated = bridge2.record_delivery("mail-1")
        assert updated is not None
        assert updated["status"] == "delivered"
    finally:
        bridge2.stop()


def test_delivery_before_record_is_not_lost(bridge, node):
    """A flush that delivers before send_message records the message.

    Queueing starts a flush thread, so the delivery callback can fire
    before ``_append_message`` runs. The report is buffered and
    reconciled when the record is appended, so the message still flips
    to delivered instead of staying queued forever.
    """
    started = []

    def _send(peer, text, **kwargs):
        thread = threading.Thread(
            target=node.on_delivered, args=("mail-1",), name="tau-test-deliver"
        )
        thread.start()
        started.append(thread)
        # Let the delivery thread run first: without the send lock it
        # would find no record and the message would stay queued.
        time.sleep(0.05)
        return {
            "ok": True,
            "v": 1,
            "queued": True,
            "id": "mail-1",
            "to": peer.hexhash,
            "expires_at": time.time() + 3600,
        }

    node.send_message = _send
    status, _, _ = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    assert status == 200
    started[0].join(timeout=5)
    assert not started[0].is_alive()

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["status"] == "delivered"


def test_early_delivery_buffer_evicts_oldest(bridge):
    """Unmatched delivery reports are bounded, oldest evicted first.

    Regression test for the CodeRabbit Minor on PR #35: the buffer was
    a plain dict but eviction called ``popitem(last=False)`` — an
    OrderedDict-only API — so the 129th unmatched report raised
    TypeError instead of evicting.
    """
    from tau.webapi import MAX_EARLY_DELIVERIES

    for index in range(MAX_EARLY_DELIVERIES + 5):
        assert bridge.record_delivery(f"unknown-{index}") is None
    assert len(bridge._early_deliveries) == MAX_EARLY_DELIVERIES
    assert "unknown-0" not in bridge._early_deliveries
    assert f"unknown-{MAX_EARLY_DELIVERIES + 4}" in bridge._early_deliveries


def test_delivery_report_not_blocked_by_slow_send(bridge, node):
    """A delivery report completes while another send is in flight.

    Regression test for the CodeRabbit finding on PR #35: the send lock
    was held across ``node.send_message``, so a delivery report for an
    earlier message stalled behind the network send (and with it the
    node's mail sweep, whose thread invokes the callback).
    """
    _queue_send(node, mail_id="mail-a")
    status, _, _ = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "first"}
    )
    assert status == 200

    send_started = threading.Event()
    release_send = threading.Event()

    def _slow_send(peer, text, **kwargs):
        send_started.set()
        release_send.wait(timeout=10)
        return {"ok": True, "v": 1, "to": peer.hexhash}

    node.send_message = _slow_send
    send_thread = threading.Thread(
        target=bridge.send_message, args=("alice", "second")
    )
    send_thread.start()
    try:
        assert send_started.wait(timeout=5)
        report_thread = threading.Thread(
            target=bridge.record_delivery, args=("mail-a",)
        )
        report_thread.start()
        report_thread.join(timeout=5)
        assert not report_thread.is_alive()
        assert bridge.messages()[0]["status"] == "delivered"
    finally:
        release_send.set()
        send_thread.join(timeout=5)


def test_raising_previous_delivery_callback_still_flips(node, tmp_path):
    """A previous callback that raises must not lose the status update."""

    def _boom(message_id):
        raise RuntimeError("boom")

    node.on_delivered = _boom
    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    chained = WebBridge(node, host="127.0.0.1", port=0, web_dir=web_dir)
    try:
        _queue_send(node)
        message, _ = chained.send_message("alice", "hi")
        assert message["status"] == "queued"

        # The failure propagates (the node's callback guard logs it in
        # production), but only after the record has flipped.
        with pytest.raises(RuntimeError, match="boom"):
            node.on_delivered("mail-1")
        assert chained.messages()[0]["status"] == "delivered"
    finally:
        chained.stop()


def test_bridge_chains_existing_delivery_callback(node, tmp_path):
    seen = []
    node.on_delivered = seen.append
    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    chained = WebBridge(node, host="127.0.0.1", port=0, web_dir=web_dir)
    try:
        _queue_send(node)
        message, _ = chained.send_message("alice", "hi")
        assert message["status"] == "queued"

        node.on_delivered("mail-1")
        assert seen == ["mail-1"]
        assert chained.messages()[0]["status"] == "delivered"
    finally:
        chained.stop()
def _stored_message(message_id, text, direction="in"):
    return {
        "id": message_id,
        "direction": direction,
        "peer": PEER_HASH,
        "peer_name": "alice",
        "text": text,
        "sent_at": 1000.0 + message_id,
        "received_at": 1001.0 + message_id,
        "status": "received" if direction == "in" else "delivered",
    }


def _bridge_with_data_dir(node, tmp_path):
    from tau.config import TauConfig

    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    return bridge


def test_message_history_survives_bridge_restart(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "hello before restart",
                "sent_at": 1000.0,
                "received_at": 1001.0,
                "id": "msg-1",
            }
        )
        bridge.send_message("alice", "reply before restart")
    finally:
        bridge.stop()

    stored = json.loads((tmp_path / "messages.json").read_text(encoding="utf-8"))
    assert [m["text"] for m in stored["messages"]] == [
        "hello before restart",
        "reply before restart",
    ]
    assert stored["next_message_id"] == 3

    node2 = FakeNode()
    bridge2 = _bridge_with_data_dir(node2, tmp_path)
    try:
        messages = bridge2.messages()
        assert [m["text"] for m in messages] == [
            "hello before restart",
            "reply before restart",
        ]
        assert [m["id"] for m in messages] == [1, 2]
        # Ids continue across the restart instead of restarting at 1.
        bridge2.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "hello after restart",
                "sent_at": 2000.0,
                "received_at": 2001.0,
                "id": "msg-2",
            }
        )
        assert bridge2.messages()[-1]["id"] == 3
    finally:
        bridge2.stop()


def test_message_history_without_data_dir_stays_in_memory(node):
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        assert bridge._messages_path is None
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "memory only",
                "sent_at": 1000.0,
                "received_at": 1001.0,
                "id": "msg-1",
            }
        )
        assert [m["text"] for m in bridge.messages()] == ["memory only"]
    finally:
        bridge.stop()


def test_message_history_load_caps_and_drops_invalid_entries(node, tmp_path):
    from tau.webapi import MAX_MESSAGE_LOG

    seeded = [_stored_message(i + 1, f"m{i + 1}") for i in range(MAX_MESSAGE_LOG + 5)]
    seeded.insert(3, {"id": "not-an-int", "text": "bad id"})
    seeded.insert(5, {"direction": "sideways", "text": "bad direction"})
    seeded.insert(7, "not a dict at all")
    (tmp_path / "messages.json").write_text(
        json.dumps({"messages": seeded, "next_message_id": 9999}),
        encoding="utf-8",
    )

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        messages = bridge.messages(limit=0)
        assert len(messages) == MAX_MESSAGE_LOG
        # The three invalid entries were dropped and the oldest valid
        # messages fell off the cap, so the log starts at m6.
        assert messages[0]["text"] == "m6"
        assert messages[-1]["text"] == f"m{MAX_MESSAGE_LOG + 5}"
        # The saved next id is honoured (and stays above the max id).
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "after load",
                "sent_at": 2000.0,
                "received_at": 2001.0,
                "id": "msg-3",
            }
        )
        assert bridge.messages()[-1]["id"] == 9999
    finally:
        bridge.stop()


def test_message_history_load_drops_sent_retracted_entries(node, tmp_path):
    """A CLI retract that ran while `tau serve` was down leaves a
    durable marker; the bridge drops the outbound entry carrying it
    when it loads, so a restarted console never resurrects the
    retracted message — while an inbound entry reusing the same wire
    id, an unretracted outbound entry, and a group leg with a
    different send time all survive."""
    from tau.mailbox import Mailbox

    mailbox = Mailbox(tmp_path / "mailbox.json")
    mailbox.record_sent_retraction("gone-dm")
    mailbox.record_sent_group_retraction("ab" * 16, 1100.0)

    dm = _stored_message(1, "retracted dm", direction="out")
    dm["wire_id"] = "gone-dm"
    kept_dm = _stored_message(2, "kept dm", direction="out")
    kept_dm["wire_id"] = "kept-dm"
    inbound = _stored_message(3, "kept inbound")
    inbound["wire_id"] = "gone-dm"
    leg = _stored_message(4, "retracted group leg", direction="out")
    leg["group"] = "ab" * 16
    leg["sent_at"] = 1100.0
    leg["wire_id"] = "leg-1"
    other_leg = _stored_message(5, "kept group leg", direction="out")
    other_leg["group"] = "ab" * 16
    other_leg["sent_at"] = 1200.0
    other_leg["wire_id"] = "leg-2"
    (tmp_path / "messages.json").write_text(
        json.dumps(
            {
                "messages": [dm, kept_dm, inbound, leg, other_leg],
                "next_message_id": 6,
            }
        ),
        encoding="utf-8",
    )

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        assert [m["text"] for m in bridge.messages()] == [
            "kept dm",
            "kept inbound",
            "kept group leg",
        ]
        # The purge is persisted, so the dropped entries never come back.
        stored = json.loads(
            (tmp_path / "messages.json").read_text(encoding="utf-8")
        )
        assert [m["text"] for m in stored["messages"]] == [
            "kept dm",
            "kept inbound",
            "kept group leg",
        ]
    finally:
        bridge.stop()


def test_bridge_start_reconciles_sent_retracted_after_load(node, tmp_path):
    """A marker written after the load-time check but before the bridge
    starts (a one-shot CLI retract landing in the startup window) is
    applied when the bridge starts, so the retracted message is not
    served for the whole session."""
    from tau.config import TauConfig
    from tau.mailbox import Mailbox

    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    entry = _stored_message(
        1, "retracted in the startup window", direction="out"
    )
    entry["wire_id"] = "gone-dm"
    (tmp_path / "messages.json").write_text(
        json.dumps({"messages": [entry], "next_message_id": 2}),
        encoding="utf-8",
    )
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    try:
        assert [m["text"] for m in bridge.messages()] == [
            "retracted in the startup window"
        ]
        # A one-shot CLI retract lands after the load-time check.
        Mailbox(tmp_path / "mailbox.json").record_sent_retraction("gone-dm")
        bridge.start()
        assert bridge.messages() == []
        stored = json.loads(
            (tmp_path / "messages.json").read_text(encoding="utf-8")
        )
        assert stored["messages"] == []
    finally:
        bridge.stop()


def test_message_history_load_recomputes_excerpt_hash(node, tmp_path):
    """A stored excerpt hash is never trusted on load: the hash is
    derived from the stored text, so a hand-edited history that
    changed the text but kept the old hash cannot make a changed
    quote look verified."""
    from tau.protocol import excerpt_hash
    from tau.webapi import _clean_stored_message

    entry = _stored_message(1, "changed text")
    entry["excerpt_hash"] = excerpt_hash("original thought")
    cleaned = _clean_stored_message(entry)
    assert cleaned is not None
    assert cleaned["excerpt_hash"] == excerpt_hash("changed text")
    assert cleaned["excerpt_hash"] != excerpt_hash("original thought")

    # Records written before hashes existed gain one on load.
    legacy = _stored_message(2, "legacy text")
    assert _clean_stored_message(legacy)["excerpt_hash"] == excerpt_hash(
        "legacy text"
    )


def test_stored_file_metadata_survives_load_and_bad_shapes_drop():
    from tau.webapi import _clean_stored_file, _clean_stored_message

    good = {"name": "notes.txt", "size": 10, "sha256": "a" * 64}
    assert _clean_stored_file(good) == good
    assert _clean_stored_file({"name": "x", "size": 0, "sha256": "a" * 64}) is None
    assert _clean_stored_file({"name": "x", "size": 10, "sha256": "zz"}) is None
    assert _clean_stored_file("notes.txt") is None

    entry = _stored_message(1, "Sent file: notes.txt (10 bytes)", direction="out")
    entry["file"] = good
    cleaned = _clean_stored_message(entry)
    assert cleaned is not None
    assert cleaned["file"] == good

    entry["file"] = {"name": "x", "size": -1, "sha256": "a" * 64}
    assert "file" not in _clean_stored_message(entry)


def test_stored_file_keeps_voice_marker_and_drops_non_bool():
    from tau.webapi import _clean_stored_file, _clean_stored_message

    voice = {"name": "voice-note-1.webm", "size": 10, "sha256": "a" * 64, "voice": True}
    assert _clean_stored_file(voice) == voice
    assert _clean_stored_file({**voice, "voice": "yes"}) == {
        "name": "voice-note-1.webm",
        "size": 10,
        "sha256": "a" * 64,
    }

    entry = _stored_message(1, "Sent file: voice-note-1.webm (10 bytes)", direction="out")
    entry["file"] = voice
    cleaned = _clean_stored_message(entry)
    assert cleaned is not None
    assert cleaned["file"]["voice"] is True


def test_stored_location_pin_survives_load_and_bad_shapes_drop():
    from tau.webapi import _clean_stored_message

    pin = {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"}
    entry = _stored_message(1, "meet here", direction="out")
    entry["location"] = pin
    cleaned = _clean_stored_message(entry)
    assert cleaned is not None
    assert cleaned["location"] == pin

    entry["location"] = {"lat": 91, "lon": 0}
    assert "location" not in _clean_stored_message(entry)
    entry["location"] = "34.05,-83.88"
    assert "location" not in _clean_stored_message(entry)


def test_stored_beacon_marker_survives_load_and_bad_shapes_drop():
    from tau.webapi import _clean_stored_message

    pin = {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"}
    marker = {"id": "aabbccdd", "expires_at": 1800003600, "seq": 2}
    entry = _stored_message(1, "on my way", direction="out")
    entry["location"] = pin
    entry["beacon"] = marker
    cleaned = _clean_stored_message(entry)
    assert cleaned is not None
    assert cleaned["beacon"] == marker

    entry["beacon"] = {"id": "a", "seq": 0}
    assert "beacon" not in _clean_stored_message(entry)
    entry["beacon"] = "live"
    assert "beacon" not in _clean_stored_message(entry)


def test_corrupt_message_history_starts_empty(node, tmp_path):
    (tmp_path / "messages.json").write_text("{not json", encoding="utf-8")
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        assert bridge.messages() == []
        bridge.send_message("alice", "fresh start")
        assert [m["text"] for m in bridge.messages()] == ["fresh start"]
        assert [m["id"] for m in bridge.messages()] == [1]
    finally:
        bridge.stop()


def test_concurrent_appends_all_persist(node, tmp_path, monkeypatch):
    """Overlapping appends with slow writes must not lose history.

    Regression test for the Codex/Qodo finding on PR #32: snapshots taken
    under the bridge lock but written after releasing it could persist out
    of order, dropping the later message and reusing its id.
    """
    import tau.webapi

    real_write = tau.webapi.atomic_write_json

    def slow_write(path, obj, mode=None):
        time.sleep(0.02)
        return real_write(path, obj, mode=mode)

    monkeypatch.setattr(tau.webapi, "atomic_write_json", slow_write)

    bridge = _bridge_with_data_dir(node, tmp_path)
    barrier = threading.Barrier(10)
    try:
        def append(index):
            barrier.wait(timeout=10)
            bridge.record_inbound(
                {
                    "from": "alice",
                    "destination": PEER_HASH,
                    "text": f"concurrent {index}",
                    "sent_at": 1000.0 + index,
                    "received_at": 1001.0 + index,
                    "id": f"msg-{index}",
                }
            )

        threads = [threading.Thread(target=append, args=(i,)) for i in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)

        stored = json.loads((tmp_path / "messages.json").read_text(encoding="utf-8"))
        assert len(stored["messages"]) == 10
        assert len({m["id"] for m in stored["messages"]}) == 10
        assert {m["text"] for m in stored["messages"]} == {
            f"concurrent {i}" for i in range(10)
        }
        assert stored["next_message_id"] == 11
    finally:
        bridge.stop()


def test_outbound_text_with_lone_surrogate_stays_persistable(node, tmp_path):
    """An escaped lone surrogate in a sent message must not poison saves.

    Regression test for the Qodo finding on PR #32: the bridge recorded
    the raw text while the node sent the normalised one, so the stored
    record could not be UTF-8 encoded and every later save failed.
    """
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        status, _, payload = _json(
            bridge,
            "/api/messages",
            method="POST",
            raw=b'{"to": "alice", "text": "hi \\ud800 there"}',
        )
        assert status == 200
        # The surrogate is dropped by normalisation, so the recorded text
        # is exactly what was sent.
        assert payload["message"]["text"] == "hi  there"

        # The history file round-trips, and a later message still persists.
        bridge.send_message("alice", "still saving")
        stored = json.loads((tmp_path / "messages.json").read_text(encoding="utf-8"))
        assert [m["text"] for m in stored["messages"]] == [
            "hi  there",
            "still saving",
        ]
    finally:
        bridge.stop()

    node2 = FakeNode()
    bridge2 = _bridge_with_data_dir(node2, tmp_path)
    try:
        assert [m["text"] for m in bridge2.messages()] == [
            "hi  there",
            "still saving",
        ]
    finally:
        bridge2.stop()


def test_lone_surrogate_entry_is_dropped_on_load(node, tmp_path):
    """A stored lone surrogate must not break startup.

    Regression test for the CodeRabbit finding on PR #95: hashing the
    text before the UTF-8 check raised UnicodeEncodeError out of
    _clean_stored_message instead of dropping the entry.
    """
    good = _stored_message(1, "good entry")
    bad = _stored_message(2, "bad \ud800 text")
    (tmp_path / "messages.json").write_text(
        json.dumps({"messages": [good, bad], "next_message_id": 3}),
        encoding="utf-8",
    )

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        assert [m["text"] for m in bridge.messages()] == ["good entry"]
    finally:
        bridge.stop()


def test_huge_timestamp_entry_is_dropped_on_load(node, tmp_path):
    """A parseable but overflowing integer timestamp must not break startup.

    Regression test for the Qodo warning on PR #32: float() of a huge JSON
    integer raises OverflowError during bridge construction.
    """
    good = _stored_message(1, "good entry")
    bad = _stored_message(2, "bad timestamp")
    bad["sent_at"] = 10**400
    (tmp_path / "messages.json").write_text(
        json.dumps({"messages": [good, bad], "next_message_id": 3}),
        encoding="utf-8",
    )

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        assert [m["text"] for m in bridge.messages()] == ["good entry"]
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "after load",
                "sent_at": 2000.0,
                "received_at": 2001.0,
                "id": "msg-3",
            }
        )
        assert bridge.messages()[-1]["id"] == 3
    finally:
        bridge.stop()


def test_tan_reply_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "api thread starter", name="tester", now=1000)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={"text": "api reply", "reply_to": post["hash"][:16]},
        )
        assert status == 200
        reply = payload["post"]
        assert reply["kind"] == "reply"
        assert reply["target"] == post["hash"]
        assert reply["text"] == "api reply"

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "api reply",
            "api thread starter",
        ]
        assert payload["posts"][0]["reply_to"] == {
            "hash": post["hash"],
            "author": post["author"],
            "name": "tester",
            "kind": "post",
            "text": "api thread starter",
            "deleted": False,
            "edited": False,
        }

        # Replying to an unknown post fails.
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={"text": "nowhere", "reply_to": "cd" * 32},
        )
        assert status == 400
        assert "unknown post" in payload["error"]["message"]

        # A non-string reply_to is a bad request, not a crash.
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={"text": "nowhere", "reply_to": 42},
        )
        assert status == 400
        assert "reply_to" in payload["error"]["message"]

        # A plain post still works with no reply_to.
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={"text": "standalone"},
        )
        assert status == 200
        assert payload["post"]["kind"] == "post"
    finally:
        bridge.stop()


def test_tan_post_via_api_accepts_expires_in(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    init_db(node.config.social_db_path)

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        before = time.time()
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={"text": "vanish soon", "expires_in": 3600},
        )
        assert status == 200
        expires_at = payload["post"]["expires_at"]
        assert expires_at is not None
        assert before + 3590 <= expires_at <= time.time() + 3610

        # A post without expires_in never expires.
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={"text": "stays"},
        )
        assert status == 200
        assert payload["post"]["expires_at"] is None

        # Invalid lifetimes are bad requests, not crashes — and queue
        # nothing.
        for body, fragment in [
            ({"text": "x", "expires_in": 0}, "expires_in"),
            ({"text": "x", "expires_in": -10}, "expires_in"),
            ({"text": "x", "expires_in": "soon"}, "expires_in"),
            ({"text": "x", "expires_in": True}, "expires_in"),
            ({"text": "x", "expires_in": 604801}, "expires_in"),
            ({"text": "x", "expires_in": 2592001}, "expires_in"),
        ]:
            status, _, payload = _json(
                bridge, "/api/tan/posts", method="POST", body=body
            )
            assert status == 400, body
            assert fragment in payload["error"]["message"], body
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert [item["text"] for item in payload["posts"]] == [
            "stays",
            "vanish soon",
        ]
    finally:
        bridge.stop()


def test_tan_post_via_api_accepts_image(tmp_path):
    import base64
    import hashlib

    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        parent = create_post(conn, identity, "parent", name="tester", now=1000)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
        data_url = (
            "data:image/png;base64," + base64.b64encode(png).decode("ascii")
        )
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={"text": "with a picture", "image": data_url},
        )
        assert status == 200
        post = payload["post"]
        assert post["kind"] == "post"
        assert post["image"] == {
            "sha256": hashlib.sha256(png).hexdigest(),
            "size": len(png),
            "mime": "image/png",
            "name": "image.png",
        }

        # The bytes land in the media store, so the console (and peers)
        # can fetch them by digest right away.
        digest = hashlib.sha256(png).hexdigest()
        status, headers, body = _request(bridge, f"/api/tan/media/{digest}")
        assert status == 200
        assert body == png
        assert headers["Content-Type"] == "image/png"

        # A JPEG upload is detected from its bytes, not the declared type.
        jpeg = b"\xff\xd8\xff\xe0" + b"\x00" * 32
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={
                "text": "mislabeled",
                "image": "data:image/png;base64,"
                + base64.b64encode(jpeg).decode("ascii"),
            },
        )
        assert status == 200
        assert payload["post"]["image"]["mime"] == "image/jpeg"
        assert payload["post"]["image"]["name"] == "image.jpg"

        # Bad uploads are 400s, not crashes — and queue nothing.
        oversized = b"\x89PNG\r\n\x1a\n" + b"\x00" * (2 * 1024 * 1024)
        for body, fragment in [
            ({"text": "x", "image": "not a data url"}, "data URL"),
            ({"text": "x", "image": "data:image/png;base64,!!!"}, "base64"),
            (
                {"text": "x", "image": "data:text/plain;base64,aGVsbG8="},
                "JPEG, PNG, GIF or WebP",
            ),
            ({"text": "x", "image": 42}, "data URL"),
            (
                {
                    "text": "x",
                    "image": "data:image/png;base64,"
                    + base64.b64encode(oversized).decode("ascii"),
                },
                "byte limit",
            ),
        ]:
            status, _, payload = _json(
                bridge, "/api/tan/posts", method="POST", body=body
            )
            assert status == 400, body
            assert fragment in payload["error"]["message"], body

        # A reply cannot carry an image — same rule as the CLI.
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={
                "text": "reply with pic",
                "reply_to": parent["hash"][:16],
                "image": data_url,
            },
        )
        assert status == 400
        assert "not a reply" in payload["error"]["message"]

        # Only the two good posts were stored.
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert [item["text"] for item in payload["posts"]] == [
            "mislabeled",
            "with a picture",
            "parent",
        ]
    finally:
        bridge.stop()


def test_tan_post_via_api_accepts_contact(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    node.blocklist = BlockList()
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        parent = create_post(conn, identity, "parent", name="tester", now=1000)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={
                "text": "talk to carol",
                "contact": {"name": "carol", "destination_hash": "cc" * 16},
            },
        )
        assert status == 200
        post = payload["post"]
        assert post["kind"] == "post"
        assert post["contact"] == {
            "name": "carol",
            "destination_hash": "cc" * 16,
        }

        # Bad cards are 400s, not crashes — and store nothing.
        for body, fragment in [
            ({"text": "x", "contact": "carol"}, "contact"),
            ({"text": "x", "contact": {"name": "carol"}}, "contact"),
            (
                {"text": "x", "contact": {"name": "", "destination_hash": "cc" * 16}},
                "contact",
            ),
            (
                {
                    "text": "x",
                    "contact": {"name": "carol", "destination_hash": "zz" * 16},
                },
                "contact",
            ),
            (
                {
                    "text": "x",
                    "contact": {"name": "carol", "destination_hash": "cc" * 15},
                },
                "contact",
            ),
        ]:
            status, _, payload = _json(
                bridge, "/api/tan/posts", method="POST", body=body
            )
            assert status == 400, body
            assert fragment in payload["error"]["message"], body

        # A reply cannot carry a card — same rule as the CLI.
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={
                "text": "reply with card",
                "reply_to": parent["hash"][:16],
                "contact": {"name": "carol", "destination_hash": "cc" * 16},
            },
        )
        assert status == 400
        assert "not a reply" in payload["error"]["message"]

        # The contact-sharing policy applies here too, like the
        # channel, DM and group sends: a blocked peer's card and
        # your own card are refused, and an unreadable block-list
        # fails closed.
        node.blocklist = BlockList(blocked=frozenset({"cc" * 16}))
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={
                "text": "meet carol",
                "contact": {"name": "carol", "destination_hash": "cc" * 16},
            },
        )
        assert status == 400
        assert "blocked peer" in payload["error"]["message"]
        node.blocklist = BlockList()
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={
                "text": "meet me",
                "contact": {
                    "name": "tester",
                    "destination_hash": node.destination_hexhash,
                },
            },
        )
        assert status == 400
        assert "cannot share your own contact" in payload["error"]["message"]
        node.blocklist = None
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={
                "text": "meet carol",
                "contact": {"name": "carol", "destination_hash": "cc" * 16},
            },
        )
        assert status == 400
        assert "block-list could not be read" in payload["error"]["message"]
        node.blocklist = BlockList()

        # Only the one good post was stored.
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert [item["text"] for item in payload["posts"]] == [
            "talk to carol",
            "parent",
        ]
    finally:
        bridge.stop()


def test_tan_post_via_api_accepts_location(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        parent = create_post(conn, identity, "parent", name="tester", now=1000)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={
                "text": "at the park",
                "location": {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"},
            },
        )
        assert status == 200
        assert payload["post"]["location"] == {
            "lat": 34.05,
            "lon": -83.88,
            "name": "Rock Springs Park",
        }

        # Bad pins are 400s, not crashes — and store nothing.
        for body, fragment in [
            ({"text": "x", "location": "34.05,-83.88"}, "location"),
            ({"text": "x", "location": {"lat": 91.0, "lon": 0.0}}, "location"),
            ({"text": "x", "location": {"lat": 34.05}}, "location"),
        ]:
            status, _, payload = _json(
                bridge, "/api/tan/posts", method="POST", body=body
            )
            assert status == 400, body
            assert fragment in payload["error"]["message"], body

        # A reply cannot carry a pin — same rule as the CLI.
        status, _, payload = _json(
            bridge,
            "/api/tan/posts",
            method="POST",
            body={
                "text": "reply with pin",
                "reply_to": parent["hash"][:16],
                "location": {"lat": 34.05, "lon": -83.88},
            },
        )
        assert status == 400
        assert "not a reply" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_drafts_via_api_carry_attachments(tmp_path):
    import base64
    import hashlib

    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    node.blocklist = BlockList()
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        parent = create_post(conn, identity, "parent", name="tester", now=1000)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
        data_url = (
            "data:image/png;base64," + base64.b64encode(png).decode("ascii")
        )
        status, _, payload = _json(
            bridge,
            "/api/tan/drafts",
            method="POST",
            body={
                "text": "with everything",
                "image": data_url,
                "location": {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"},
                "contact": {"name": "carol", "destination_hash": "cc" * 16},
            },
        )
        assert status == 200, payload
        draft = payload["draft"]
        assert draft["image"] == {
            "sha256": hashlib.sha256(png).hexdigest(),
            "size": len(png),
            "mime": "image/png",
            "name": "image.png",
        }
        assert draft["location"] == {
            "lat": 34.05,
            "lon": -83.88,
            "name": "Rock Springs Park",
        }
        assert draft["contact"] == {
            "name": "carol",
            "destination_hash": "cc" * 16,
        }

        # The bytes land in the media store, so the console can load
        # the draft's image back for the composer.
        digest = hashlib.sha256(png).hexdigest()
        status, headers, body = _request(bridge, f"/api/tan/media/{digest}")
        assert status == 200
        assert body == png

        # The list round-trips the attachments.
        status, _, payload = _json(bridge, "/api/tan/drafts")
        assert status == 200
        assert payload["count"] == 1
        listed = payload["drafts"][0]
        assert listed["image"]["sha256"] == digest
        assert listed["location"]["name"] == "Rock Springs Park"
        assert listed["contact"]["name"] == "carol"

        # Bad attachments are 400s — and a reply draft cannot carry any.
        for body, fragment in [
            ({"text": "x", "image": "not a data url"}, "data URL"),
            ({"text": "x", "location": {"lat": 91.0, "lon": 0.0}}, "location"),
            ({"text": "x", "contact": {"name": "carol"}}, "contact"),
            (
                {
                    "text": "reply with pic",
                    "reply_to": parent["hash"][:16],
                    "image": data_url,
                },
                "not a reply",
            ),
            (
                {
                    "text": "reply with pin",
                    "reply_to": parent["hash"][:16],
                    "location": {"lat": 34.05, "lon": -83.88},
                },
                "not a reply",
            ),
            (
                {
                    "text": "reply with card",
                    "reply_to": parent["hash"][:16],
                    "contact": {"name": "carol", "destination_hash": "cc" * 16},
                },
                "not a reply",
            ),
        ]:
            status, _, payload = _json(
                bridge, "/api/tan/drafts", method="POST", body=body
            )
            assert status == 400, body
            assert fragment in payload["error"]["message"], body

        # The contact-sharing policy applies at save time too: a
        # blocked peer's card is refused.
        node.blocklist = BlockList(blocked=frozenset({"cc" * 16}))
        status, _, payload = _json(
            bridge,
            "/api/tan/drafts",
            method="POST",
            body={
                "text": "meet carol",
                "contact": {"name": "carol", "destination_hash": "cc" * 16},
            },
        )
        assert status == 400
        assert "blocked peer" in payload["error"]["message"]

        # Only the one good draft was stored.
        status, _, payload = _json(bridge, "/api/tan/drafts")
        assert payload["count"] == 1
    finally:
        bridge.stop()


def test_tan_draft_with_large_image_saves(tmp_path):
    """A draft whose image data URL pushes the body past 32 KiB saves.

    The save route carries a base64 image, so it gets the file body
    cap like a post does — the ordinary 32 KiB cap would reject the
    request before the draft handler ever runs.
    """
    import base64
    import hashlib

    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    node.blocklist = BlockList()
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    conn.close()
    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        png = b"\x89PNG\r\n\x1a\n" + b"\x00" * (40 * 1024)
        data_url = (
            "data:image/png;base64," + base64.b64encode(png).decode("ascii")
        )
        assert len(json.dumps({"text": "big pic", "image": data_url})) > 32 * 1024
        status, _, payload = _json(
            bridge,
            "/api/tan/drafts",
            method="POST",
            body={"text": "big pic", "image": data_url},
        )
        assert status == 200, payload
        assert payload["draft"]["image"]["sha256"] == hashlib.sha256(png).hexdigest()
        # The pinned bytes are served back to the owner's console.
        digest = hashlib.sha256(png).hexdigest()
        status, _, body = _request(bridge, f"/api/tan/media/{digest}")
        assert status == 200
        assert body == png
    finally:
        bridge.stop()


def test_tan_quote_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "api quotable", name="tester", now=1000)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/tan/quote",
            method="POST",
            body={"target": post["hash"][:16], "text": "seconded, with notes"},
        )
        assert status == 200
        quote = payload["post"]
        assert quote["kind"] == "quote"
        assert quote["target"] == post["hash"]
        assert quote["text"] == "seconded, with notes"

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "seconded, with notes",
            "api quotable",
        ]
        assert payload["posts"][0]["quoted"] == {
            "hash": post["hash"],
            "author": post["author"],
            "name": "tester",
            "kind": "post",
            "text": "api quotable",
            "deleted": False,
        }

        # Quoting a quote is rejected: embeds stay one level deep.
        status, _, payload = _json(
            bridge,
            "/api/tan/quote",
            method="POST",
            body={"target": quote["hash"], "text": "nesting"},
        )
        assert status == 400
        assert "only posts and replies can be quoted" in payload["error"]["message"]

        # Quoting an unknown post fails.
        status, _, payload = _json(
            bridge,
            "/api/tan/quote",
            method="POST",
            body={"target": "cd" * 32, "text": "nowhere"},
        )
        assert status == 400
        assert "unknown post" in payload["error"]["message"]

        # A missing target or comment is a bad request, not a crash.
        status, _, payload = _json(
            bridge,
            "/api/tan/quote",
            method="POST",
            body={"text": "no target"},
        )
        assert status == 400
        assert "target" in payload["error"]["message"]
        status, _, payload = _json(
            bridge,
            "/api/tan/quote",
            method="POST",
            body={"target": post["hash"]},
        )
        assert status == 400
        assert "text" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_quote_via_api_accepts_image(tmp_path):
    import base64
    import hashlib

    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "api quotable", name="tester", now=1000)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
        data_url = (
            "data:image/png;base64," + base64.b64encode(png).decode("ascii")
        )
        status, _, payload = _json(
            bridge,
            "/api/tan/quote",
            method="POST",
            body={"target": post["hash"][:16], "text": "with a picture", "image": data_url},
        )
        assert status == 200
        quote = payload["post"]
        assert quote["kind"] == "quote"
        assert quote["image"] == {
            "sha256": hashlib.sha256(png).hexdigest(),
            "size": len(png),
            "mime": "image/png",
            "name": "image.png",
        }

        # The bytes land in the media store, and the timeline shows
        # the quote's own image above the embedded original.
        digest = hashlib.sha256(png).hexdigest()
        status, headers, body = _request(bridge, f"/api/tan/media/{digest}")
        assert status == 200
        assert body == png
        assert headers["Content-Type"] == "image/png"
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert payload["posts"][0]["image"]["sha256"] == digest
        assert payload["posts"][0]["quoted"]["hash"] == post["hash"]

        # A non-string image is a 400, not a crash.
        status, _, payload = _json(
            bridge,
            "/api/tan/quote",
            method="POST",
            body={"target": post["hash"], "text": "x", "image": 42},
        )
        assert status == 400
        assert "data URL" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_quote_via_api_accepts_large_image(tmp_path):
    # A quote image near the 2 MiB limit is far past the generic 32 KiB
    # JSON-body cap: both quote routes get the file-body cap, like
    # /api/tan/posts, or most quote images would fail to upload.
    import base64
    import hashlib

    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "api quotable", name="tester", now=1000)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        png = b"\x89PNG\r\n\x1a\n" + b"\x00" * (100 * 1024)
        data_url = (
            "data:image/png;base64," + base64.b64encode(png).decode("ascii")
        )
        assert len(data_url) > 32 * 1024
        status, _, payload = _json(
            bridge,
            "/api/tan/quote",
            method="POST",
            body={"target": post["hash"], "text": "big picture", "image": data_url},
        )
        assert status == 200
        assert payload["post"]["image"]["size"] == len(png)

        # A scheduled quote with the same image rides the same cap.
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={
                "kind": "quote",
                "target_post": post["hash"],
                "text": "big picture, later",
                "send_in": 60,
                "image": data_url,
            },
        )
        assert status == 200
        assert payload["scheduled"]["image"]["size"] == len(png)
    finally:
        bridge.stop()


def test_tan_quote_via_api_refuses_unknown_target_without_pinning(tmp_path):
    # The target is resolved before the image is decoded or pinned:
    # a refused quote must not leave unreferenced bytes in the
    # pinned media store, which is never pruned.
    import base64
    import hashlib

    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_post, init_db, media_path

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "api quotable", name="tester", now=1000)
    finally:
        conn.close()

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
        data_url = (
            "data:image/png;base64," + base64.b64encode(png).decode("ascii")
        )
        digest = hashlib.sha256(png).hexdigest()
        assert media_path(tmp_path, digest) is None
        status, _, payload = _json(
            bridge,
            "/api/tan/quote",
            method="POST",
            body={"target": "cd" * 32, "text": "nowhere", "image": data_url},
        )
        assert status == 400
        assert "unknown post" in payload["error"]["message"]
        assert media_path(tmp_path, digest) is None

        # A scheduled quote to an unknown target refuses the same way.
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={
                "kind": "quote",
                "target_post": "cd" * 32,
                "text": "nowhere",
                "send_in": 60,
                "image": data_url,
            },
        )
        assert status == 400
        assert "unknown post" in payload["error"]["message"]
        assert media_path(tmp_path, digest) is None
    finally:
        bridge.stop()


def test_tan_schedule_post_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "api quotable", name="tester", now=1000)
    finally:
        conn.close()

    # No Tan app attached: scheduling writes the queue file, like the
    # CLI, and the serving node publishes due entries later.
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={"text": "hello later", "send_in": 3600},
        )
        assert status == 200
        scheduled = payload["scheduled"]
        assert scheduled["text"] == "hello later"
        assert scheduled["reply_to"] is None
        assert abs(scheduled["publish_at"] - (time.time() + 3600)) < 5

        # A reply resolves its target to the full hash, like tan_post.
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={"text": "agreed", "reply_to": post["hash"][:16], "send_in": 60},
        )
        assert status == 200
        assert payload["scheduled"]["reply_to"] == post["hash"]

        # An expiry rides along: the post disappears that long after it
        # is published.
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={"text": "vanish later", "send_in": 60, "expires_in": 3600},
        )
        assert status == 200
        assert payload["scheduled"]["expires_in"] == 3600
        assert abs(payload["scheduled"]["publish_at"] - (time.time() + 60)) < 5

        # An image rides the queue entry: the bytes land pinned in the
        # media store at schedule time, and the descriptor is stored
        # with the entry until the publish pass attaches it.
        png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
        data_url = (
            "data:image/png;base64," + base64.b64encode(png).decode("ascii")
        )
        digest = hashlib.sha256(png).hexdigest()
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={"text": "morning photo", "send_in": 30, "image": data_url},
        )
        assert status == 200
        assert payload["scheduled"]["image"] == {
            "sha256": digest,
            "size": len(png),
            "mime": "image/png",
            "name": "image.png",
        }
        # The bytes are pinned in the media store for the publish pass —
        # not served yet: a local upload stays private until a post
        # carrying it is published, so /api/tan/media/<digest> 404s
        # until the scheduled post goes out.
        from tau.tangents.tan import media_path

        assert media_path(tmp_path, digest) is not None

        status, _, payload = _json(bridge, "/api/tan/scheduled")
        assert status == 200
        assert payload["count"] == 4
        # Soonest first: the 30-second photo precedes the 60-second posts.
        assert [item["text"] for item in payload["scheduled"]] == [
            "morning photo",
            "agreed",
            "vanish later",
            "hello later",
        ]

        # Cancelling by id prefix drops only that entry.
        status, _, payload = _json(
            bridge,
            "/api/tan/cancel-scheduled",
            method="POST",
            body={"id": scheduled["id"][:8]},
        )
        assert status == 200
        assert payload["cancelled"]["id"] == scheduled["id"]
        assert payload["deleted_local"] is False
        status, _, payload = _json(bridge, "/api/tan/scheduled")
        assert payload["count"] == 3
        assert payload["scheduled"][0]["text"] == "morning photo"

        # Cancelling an unknown id is a bad request, not a crash.
        status, _, payload = _json(
            bridge,
            "/api/tan/cancel-scheduled",
            method="POST",
            body={"id": "zz" * 8},
        )
        assert status == 400
        assert "no scheduled post" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_schedule_post_with_location_via_api(tmp_path):
    from tau.config import TauConfig

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A pin rides the queue entry, like an image descriptor.
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={
                "text": "meet me here tomorrow",
                "send_in": 3600,
                "location": {
                    "lat": 34.05,
                    "lon": -83.88,
                    "name": "Rock Springs Park",
                },
            },
        )
        assert status == 200
        assert payload["scheduled"]["location"] == {
            "lat": 34.05,
            "lon": -83.88,
            "name": "Rock Springs Park",
        }

        status, _, payload = _json(bridge, "/api/tan/scheduled")
        assert status == 200
        assert payload["count"] == 1
        assert payload["scheduled"][0]["location"] == {
            "lat": 34.05,
            "lon": -83.88,
            "name": "Rock Springs Park",
        }
    finally:
        bridge.stop()


def test_tan_schedule_post_with_contact_via_api(tmp_path):
    from tau.config import TauConfig

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    node.blocklist = BlockList()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A card rides the queue entry, like a pin.
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={
                "text": "talk to carol about this",
                "send_in": 3600,
                "contact": {"name": "carol", "destination_hash": "cc" * 16},
            },
        )
        assert status == 200
        assert payload["scheduled"]["contact"] == {
            "name": "carol",
            "destination_hash": "cc" * 16,
        }

        status, _, payload = _json(bridge, "/api/tan/scheduled")
        assert status == 200
        assert payload["count"] == 1
        assert payload["scheduled"][0]["contact"] == {
            "name": "carol",
            "destination_hash": "cc" * 16,
        }

        # A malformed card is a 400, not a crash — and queues nothing.
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={
                "text": "talk to mallory",
                "send_in": 3600,
                "contact": {"name": "mallory", "destination_hash": "zz" * 16},
            },
        )
        assert status == 400
        assert "contact" in payload["error"]["message"]
        status, _, payload = _json(bridge, "/api/tan/scheduled")
        assert payload["count"] == 1

        # The contact-sharing policy applies to scheduling too: a
        # blocked peer's card and your own card are refused, and an
        # unreadable block-list fails closed — before anything is
        # queued.
        node.blocklist = BlockList(blocked=frozenset({"cc" * 16}))
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={
                "text": "meet carol",
                "send_in": 3600,
                "contact": {"name": "carol", "destination_hash": "cc" * 16},
            },
        )
        assert status == 400
        assert "blocked peer" in payload["error"]["message"]
        node.blocklist = BlockList()
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={
                "text": "meet me",
                "send_in": 3600,
                "contact": {
                    "name": "tester",
                    "destination_hash": node.destination_hexhash,
                },
            },
        )
        assert status == 400
        assert "cannot share your own contact" in payload["error"]["message"]
        node.blocklist = None
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={
                "text": "meet carol",
                "send_in": 3600,
                "contact": {"name": "carol", "destination_hash": "cc" * 16},
            },
        )
        assert status == 400
        assert "block-list could not be read" in payload["error"]["message"]
        node.blocklist = BlockList()
        status, _, payload = _json(bridge, "/api/tan/scheduled")
        assert payload["count"] == 1

        # A card cannot ride a poll or a reply — same rule as the CLI.
        for body in [
            {
                "text": "who?",
                "send_in": 3600,
                "poll_options": ["Carol", "Dave"],
                "contact": {"name": "carol", "destination_hash": "cc" * 16},
            },
            {
                "text": "agreed",
                "send_in": 3600,
                "reply_to": "ab" * 32,
                "contact": {"name": "carol", "destination_hash": "cc" * 16},
            },
        ]:
            status, _, payload = _json(
                bridge, "/api/tan/schedule", method="POST", body=body
            )
            assert status == 400, body
            assert "contact card" in payload["error"]["message"], body
    finally:
        bridge.stop()


def test_tan_schedule_post_bad_location_pins_no_image(tmp_path):
    # A valid image with a malformed pin must fail before the bytes
    # are pinned: otherwise the schedule 400s and the image sits
    # orphaned in the pinned store with nothing referencing it.
    from tau.config import TauConfig

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
        data_url = "data:image/png;base64," + base64.b64encode(png).decode("ascii")
        digest = hashlib.sha256(png).hexdigest()
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={
                "text": "meet me here tomorrow",
                "send_in": 3600,
                "image": data_url,
                "location": {"lat": 999, "lon": 0},
            },
        )
        assert status == 400
        assert "location" in payload["error"]["message"]

        from tau.tangents.tan import media_path

        assert media_path(tmp_path, digest) is None
        status, _, payload = _json(bridge, "/api/tan/scheduled")
        assert payload["count"] == 0
    finally:
        bridge.stop()


def test_tan_schedule_poll_via_api(tmp_path):
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={
                "text": "pizza or tacos?",
                "poll_options": ["Pizza", "Tacos"],
                "send_in": 3600,
            },
        )
        assert status == 200
        scheduled = payload["scheduled"]
        assert scheduled["kind"] == "poll"
        assert scheduled["text"] == "pizza or tacos?"
        assert scheduled["options"] == ["Pizza", "Tacos"]
        assert abs(scheduled["publish_at"] - (time.time() + 3600)) < 5

        # With an expiry the poll disappears that long after it is
        # published, and the delay must stay shorter than the expiry.
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={
                "text": "vanishing poll?",
                "poll_options": ["Yes", "No"],
                "send_in": 60,
                "expires_in": 3600,
            },
        )
        assert status == 200
        assert payload["scheduled"]["kind"] == "poll"
        assert payload["scheduled"]["expires_in"] == 3600

        status, _, payload = _json(bridge, "/api/tan/scheduled")
        assert status == 200
        assert payload["count"] == 2
        assert [item["kind"] for item in payload["scheduled"]] == [
            "poll",
            "poll",
        ]

        # Cancelling a queued poll drops only that entry.
        status, _, payload = _json(
            bridge,
            "/api/tan/cancel-scheduled",
            method="POST",
            body={"id": scheduled["id"][:8]},
        )
        assert status == 200
        assert payload["cancelled"]["id"] == scheduled["id"]
        assert payload["deleted_local"] is False
    finally:
        bridge.stop()


def test_tan_schedule_poll_close_after_via_api(tmp_path):
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The deadline rides the queue entry, so it counts from
        # publication, not from queue time.
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={
                "text": "pizza or tacos?",
                "poll_options": ["Pizza", "Tacos"],
                "send_in": 3600,
                "close_after": 3600,
            },
        )
        assert status == 200
        scheduled = payload["scheduled"]
        assert scheduled["kind"] == "poll"
        assert scheduled["close_after"] == 3600

        # A close deadline on anything but a poll, past the 7d
        # horizon, or at or past the expiry is refused.
        for body, fragment in [
            (
                {"text": "plain post", "send_in": 60, "close_after": 3600},
                "only a scheduled poll",
            ),
            (
                {
                    "text": "pick?",
                    "poll_options": ["Yes", "No"],
                    "send_in": 60,
                    "close_after": 8 * 24 * 60 * 60,
                },
                "close_after",
            ),
            (
                {
                    "text": "pick?",
                    "poll_options": ["Yes", "No"],
                    "send_in": 60,
                    "expires_in": 3600,
                    "close_after": 3600,
                },
                "close before it expires",
            ),
        ]:
            status, _, payload = _json(
                bridge, "/api/tan/schedule", method="POST", body=body
            )
            assert status == 400, body
            assert fragment in payload["error"]["message"], body

        status, _, payload = _json(bridge, "/api/tan/scheduled")
        assert status == 200
        assert payload["count"] == 1
    finally:
        bridge.stop()


def test_tan_schedule_repost_and_quote_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "api quotable", name="tester", now=1000)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A repost carries no text: the target resolves to its full hash.
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={"kind": "repost", "target_post": post["hash"][:16], "send_in": 3600},
        )
        assert status == 200
        scheduled = payload["scheduled"]
        assert scheduled["kind"] == "repost"
        assert scheduled["text"] == ""
        assert scheduled["target_post"] == post["hash"]
        assert abs(scheduled["publish_at"] - (time.time() + 3600)) < 5

        # A quote carries its comment as the text.
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={
                "kind": "quote",
                "target_post": post["hash"],
                "text": "my take",
                "send_in": 60,
            },
        )
        assert status == 200
        scheduled = payload["scheduled"]
        assert scheduled["kind"] == "quote"
        assert scheduled["text"] == "my take"
        assert scheduled["target_post"] == post["hash"]

        status, _, payload = _json(bridge, "/api/tan/scheduled")
        assert status == 200
        assert payload["count"] == 2
        assert [item["kind"] for item in payload["scheduled"]] == [
            "quote",
            "repost",
        ]

        # A quote is a top-level entry, so it may carry an image: the
        # scheduled entry carries the descriptor and the bytes are
        # pinned in the media store at schedule time.
        png2 = b"\x89PNG\r\n\x1a\n" + b"\x01" * 64
        data_url2 = (
            "data:image/png;base64," + base64.b64encode(png2).decode("ascii")
        )
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={
                "kind": "quote",
                "target_post": post["hash"],
                "text": "with a picture",
                "send_in": 60,
                "image": data_url2,
            },
        )
        assert status == 200
        scheduled = payload["scheduled"]
        assert scheduled["kind"] == "quote"
        assert scheduled["image"] == {
            "sha256": hashlib.sha256(png2).hexdigest(),
            "size": len(png2),
            "mime": "image/png",
            "name": "image.png",
        }
        from tau.tangents.tan import media_path

        assert media_path(tmp_path, hashlib.sha256(png2).hexdigest()) is not None

        # Bad repost/quote requests are 400s and queue nothing.
        png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
        data_url = (
            "data:image/png;base64," + base64.b64encode(png).decode("ascii")
        )
        for body, fragment in [
            ({"kind": "repost", "send_in": 60}, "target_post"),
            ({"kind": "quote", "send_in": 60}, "target_post"),
            (
                {"kind": "quote", "target_post": post["hash"], "send_in": 60},
                "text",
            ),
            (
                {"kind": "poll", "target_post": post["hash"], "send_in": 60},
                "kind",
            ),
            (
                {
                    "kind": "repost",
                    "target_post": post["hash"],
                    "send_in": 60,
                    "reply_to": post["hash"],
                },
                "reply",
            ),
            (
                {
                    "kind": "quote",
                    "target_post": post["hash"],
                    "text": "x",
                    "send_in": 60,
                    "expires_in": 3600,
                },
                "expire",
            ),
            (
                {"kind": "repost", "target_post": "cd" * 32, "send_in": 60},
                "unknown post",
            ),
            # An image on a repost, or poll options on a repost/quote,
            # is refused up front, not scheduled and silently dropped
            # at publish time: a repost has no comment of its own, and
            # a poll is its own kind.  (A quote may carry an image —
            # see above — so only the repost case is refused here.)
            (
                {
                    "kind": "repost",
                    "target_post": post["hash"],
                    "send_in": 60,
                    "image": data_url,
                },
                "cannot carry an image",
            ),
            (
                {
                    "kind": "repost",
                    "target_post": post["hash"],
                    "send_in": 60,
                    "poll_options": ["A", "B"],
                },
                "cannot carry poll options",
            ),
            (
                {
                    "kind": "quote",
                    "target_post": post["hash"],
                    "text": "x",
                    "send_in": 60,
                    "poll_options": ["A", "B"],
                },
                "cannot carry poll options",
            ),
        ]:
            status, _, payload = _json(
                bridge, "/api/tan/schedule", method="POST", body=body
            )
            assert status == 400, body
            assert fragment in payload["error"]["message"], body
        # The refused image was never decoded or pinned: the refusal
        # happens before the media store sees the bytes.
        assert media_path(tmp_path, hashlib.sha256(png).hexdigest()) is None
        status, _, payload = _json(bridge, "/api/tan/scheduled")
        assert payload["count"] == 3
    finally:
        bridge.stop()


def test_tan_schedule_poll_rejects_bad_requests(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "api quotable", name="tester", now=1000)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        for body, fragment in [
            (
                {"text": "pick?", "poll_options": ["only one"], "send_in": 60},
                "poll_options",
            ),
            (
                {
                    "text": "pick?",
                    "poll_options": ["a", "b", "c", "d", "e", "f", "g", "h", "i"],
                    "send_in": 60,
                },
                "poll_options",
            ),
            (
                {"text": "pick?", "poll_options": ["Yes", "  "], "send_in": 60},
                "poll_options",
            ),
            (
                {"text": "pick?", "poll_options": ["Yes", 7], "send_in": 60},
                "poll_options",
            ),
            (
                {"text": "pick?", "poll_options": "Yes/No", "send_in": 60},
                "poll_options",
            ),
            (
                {
                    "text": "pick?",
                    "poll_options": ["Yes", "No"],
                    "reply_to": post["hash"][:16],
                    "send_in": 60,
                },
                "cannot be a reply",
            ),
            (
                {
                    "text": "pick?",
                    "poll_options": ["Yes", "No"],
                    "send_in": 3600,
                    "expires_in": 60,
                },
                "shorter than",
            ),
        ]:
            status, _, payload = _json(
                bridge, "/api/tan/schedule", method="POST", body=body
            )
            assert status == 400, body
            assert fragment in payload["error"]["message"], body
        # Nothing queued by the rejected requests.
        status, _, payload = _json(bridge, "/api/tan/scheduled")
        assert status == 200
        assert payload == {"scheduled": [], "count": 0}
    finally:
        bridge.stop()


def test_tan_schedule_post_rejects_bad_requests(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "api quotable", name="tester", now=1000)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        for body, fragment in [
            ({"text": "  ", "send_in": 60}, "text"),
            ({"text": "hello"}, "send_in"),
            ({"text": "hello", "send_in": 0}, "send_in"),
            ({"text": "hello", "send_in": -10}, "send_in"),
            ({"text": "hello", "send_in": 604801}, "send_in"),
            ({"text": "hello", "send_in": "soon"}, "send_in"),
            ({"text": "hello", "send_in": True}, "send_in"),
            ({"text": "hello", "send_in": 60, "reply_to": "cd" * 32}, "unknown post"),
            ({"text": "hello", "send_in": 60, "expires_in": 0}, "expires_in"),
            ({"text": "hello", "send_in": 60, "expires_in": -10}, "expires_in"),
            ({"text": "hello", "send_in": 60, "expires_in": "soon"}, "expires_in"),
            ({"text": "hello", "send_in": 60, "expires_in": True}, "expires_in"),
            (
                {"text": "hello", "send_in": 60, "expires_in": 604801},
                "expires_in",
            ),
            (
                {"text": "hello", "send_in": 3600, "expires_in": 60},
                "shorter than",
            ),
            (
                {"text": "hello", "send_in": 3600, "expires_in": 3600},
                "shorter than",
            ),
            (
                {
                    "text": "reply with pic",
                    "send_in": 60,
                    "reply_to": post["hash"][:16],
                    "image": "data:image/png;base64,iVBORw0KGgo=",
                },
                "not a reply",
            ),
            (
                {
                    "text": "reply with pin",
                    "send_in": 60,
                    "reply_to": post["hash"][:16],
                    "location": {"lat": 34.05, "lon": -83.88},
                },
                "not a reply",
            ),
            (
                {"text": "hello", "send_in": 60, "location": "34.05,-83.88"},
                "must be an object",
            ),
            (
                {
                    "text": "hello",
                    "send_in": 60,
                    "location": {"lat": 91.0, "lon": 0},
                },
                "latitude",
            ),
            (
                {
                    "text": "where?",
                    "send_in": 60,
                    "poll_options": ["here", "there"],
                    "location": {"lat": 34.05, "lon": -83.88},
                },
                "cannot carry a location",
            ),
            (
                {
                    "text": "look",
                    "send_in": 60,
                    "kind": "quote",
                    "target_post": post["hash"][:16],
                    "location": {"lat": 34.05, "lon": -83.88},
                },
                "cannot carry a location",
            ),
            (
                {"text": "hello", "send_in": 60, "image": 42},
                "data URL",
            ),
            (
                {"text": "hello", "send_in": 60, "image": "not a data url"},
                "data URL",
            ),
        ]:
            status, _, payload = _json(
                bridge, "/api/tan/schedule", method="POST", body=body
            )
            assert status == 400, body
            assert fragment in payload["error"]["message"], body
        # Nothing queued by the rejected requests.
        status, _, payload = _json(bridge, "/api/tan/scheduled")
        assert status == 200
        assert payload == {"scheduled": [], "count": 0}
    finally:
        bridge.stop()


def test_tan_edit_scheduled_post_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.scheduled_posts import (
        ScheduledPost,
        load_scheduled_posts,
        new_scheduled_post_id,
        save_scheduled_posts,
    )
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, identity, "api quotable", name="tester", now=1000)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/tan/schedule",
            method="POST",
            body={"text": "hello later", "send_in": 3600},
        )
        assert status == 200
        entry = payload["scheduled"]

        # Text and time change together; the id stays the same.
        status, _, payload = _json(
            bridge,
            "/api/tan/edit-scheduled",
            method="POST",
            body={"id": entry["id"][:8], "text": "hello sooner", "send_in": 60},
        )
        assert status == 200
        assert payload["scheduled"]["id"] == entry["id"]
        assert payload["scheduled"]["text"] == "hello sooner"
        assert abs(payload["scheduled"]["publish_at"] - (time.time() + 60)) < 5

        # Text-only and time-only edits leave the other half alone.
        status, _, payload = _json(
            bridge,
            "/api/tan/edit-scheduled",
            method="POST",
            body={"id": entry["id"], "text": "final wording"},
        )
        assert status == 200
        assert payload["scheduled"]["text"] == "final wording"
        first_publish_at = payload["scheduled"]["publish_at"]
        status, _, payload = _json(
            bridge,
            "/api/tan/edit-scheduled",
            method="POST",
            body={"id": entry["id"], "send_in": 7200},
        )
        assert status == 200
        assert payload["scheduled"]["text"] == "final wording"
        assert payload["scheduled"]["publish_at"] > first_publish_at

        # Bad requests: missing id, neither field, bad send_in, unknown id.
        for body, fragment in [
            ({"text": "x", "send_in": 60}, "id"),
            ({"id": entry["id"]}, "text"),
            ({"id": entry["id"], "text": "  "}, "text"),
            ({"id": entry["id"], "send_in": 0}, "send_in"),
            ({"id": entry["id"], "send_in": 604801}, "send_in"),
            ({"id": "zz" * 8, "text": "x"}, "no scheduled post"),
        ]:
            status, _, payload = _json(
                bridge, "/api/tan/edit-scheduled", method="POST", body=body
            )
            assert status == 400, body
            assert fragment in payload["error"]["message"], body

        # An entry whose publish already stored the signed post cannot
        # be edited — cancel it instead.
        queue = load_scheduled_posts(node.config.scheduled_posts_path)
        created = ScheduledPost(
            id=new_scheduled_post_id(),
            text="already stored",
            publish_at=time.time() + 3600,
            created_at=time.time(),
            post_hash="b" * 64,
        )
        save_scheduled_posts(
            node.config.scheduled_posts_path, queue.with_added(created)
        )
        status, _, payload = _json(
            bridge,
            "/api/tan/edit-scheduled",
            method="POST",
            body={"id": created.id, "text": "too late"},
        )
        assert status == 400
        assert "already created" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_drafts_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "api quotable", name="tester", now=1000)
    finally:
        conn.close()

    # Drafts need no Tan app attached: they live in the social DB, so
    # a draft saved from the CLI shows up in the console too.
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/drafts")
        assert status == 200
        assert payload == {"drafts": [], "count": 0}

        status, _, payload = _json(
            bridge,
            "/api/tan/drafts",
            method="POST",
            body={"text": "half-written thought"},
        )
        assert status == 200
        draft = payload["draft"]
        assert draft["text"] == "half-written thought"
        assert draft["reply_to"] == ""
        assert draft["expires_in"] is None

        # A reply resolves its target to the full hash, like tan_post,
        # and the expiry rides along for the publish.
        status, _, payload = _json(
            bridge,
            "/api/tan/drafts",
            method="POST",
            body={
                "text": "agreed",
                "reply_to": post["hash"][:16],
                "expires_in": 3600,
            },
        )
        assert status == 200
        reply_draft = payload["draft"]
        assert reply_draft["reply_to"] == post["hash"]
        assert reply_draft["expires_in"] == 3600

        status, _, payload = _json(bridge, "/api/tan/drafts")
        assert status == 200
        assert payload["count"] == 2
        # Newest first: the reply draft precedes the plain one.
        assert [item["id"] for item in payload["drafts"]] == [
            reply_draft["id"],
            draft["id"],
        ]

        # Discarding by id drops only that draft.
        status, _, payload = _json(
            bridge,
            "/api/tan/drafts/delete",
            method="POST",
            body={"id": draft["id"]},
        )
        assert status == 200
        assert payload["draft"]["id"] == draft["id"]
        status, _, payload = _json(bridge, "/api/tan/drafts")
        assert payload["count"] == 1
        assert payload["drafts"][0]["id"] == reply_draft["id"]

        # Discarding an unknown draft is a bad request, not a crash.
        status, _, payload = _json(
            bridge,
            "/api/tan/drafts/delete",
            method="POST",
            body={"id": 9999},
        )
        assert status == 400
        assert "unknown draft" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_tan_drafts_rejects_bad_requests(tmp_path):
    from tau.config import TauConfig

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        for body, fragment in [
            ({"text": "  "}, "text"),
            ({"reply_to": "ab" * 16}, "text"),
            ({"text": "hello", "reply_to": "cd" * 32}, "unknown post"),
            ({"text": "hello", "reply_to": "zz" * 16}, "unknown post"),
            ({"text": "hello", "expires_in": 0}, "expires_in"),
            ({"text": "hello", "expires_in": -10}, "expires_in"),
            ({"text": "hello", "expires_in": "soon"}, "expires_in"),
            ({"text": "hello", "expires_in": True}, "expires_in"),
            ({"text": "hello", "expires_in": 604801}, "expires_in"),
        ]:
            status, _, payload = _json(
                bridge, "/api/tan/drafts", method="POST", body=body
            )
            assert status == 400, body
            assert fragment in payload["error"]["message"], body
        for body in [{}]:
            status, _, payload = _json(
                bridge, "/api/tan/drafts/delete", method="POST", body=body
            )
            assert status == 400, body
            assert "id" in payload["error"]["message"], body
        # An id supplied under "draft" is accepted — and an unknown one
        # is a bad request, not a crash.
        status, _, payload = _json(
            bridge, "/api/tan/drafts/delete", method="POST", body={"draft": 1}
        )
        assert status == 400
        assert "unknown draft" in payload["error"]["message"]
        # Nothing saved by the rejected requests.
        status, _, payload = _json(bridge, "/api/tan/drafts")
        assert status == 200
        assert payload == {"drafts": [], "count": 0}
    finally:
        bridge.stop()


def test_tan_cancel_scheduled_post_removes_stored_post(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.scheduled_posts import (
        ScheduledPost,
        load_scheduled_posts,
        new_scheduled_post_id,
        save_scheduled_posts,
    )
    from tau.tangents.tan import (
        TanApp,
        _delete_entry_exists,
        create_post,
        get_post,
        init_db,
    )

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        post = create_post(conn, identity, "already stored", name="tester", now=1000)
    finally:
        conn.close()

    # A publish attempt already stored the signed post, so the queue
    # entry carries its hash — the case the CLI's cancel handles by
    # removing the stored copy too.
    queue = load_scheduled_posts(node.config.scheduled_posts_path)
    entry = ScheduledPost(
        id=new_scheduled_post_id(),
        text="already stored",
        publish_at=time.time() + 3600,
        created_at=time.time(),
        post_hash=post["hash"],
    )
    save_scheduled_posts(node.config.scheduled_posts_path, queue.with_added(entry))

    node.app = TanApp(node.config, identity, name="tester", gossip=False)
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/tan/cancel-scheduled",
            method="POST",
            body={"id": entry.id},
        )
        assert status == 200
        assert payload["cancelled"]["id"] == entry.id
        assert payload["deleted_local"] is True
        status, _, payload = _json(bridge, "/api/tan/scheduled")
        assert payload == {"scheduled": [], "count": 0}
        # The stored post is purged by the delete entry, like the CLI's
        # cancel: the row is gone, and the tombstone is the only trace.
        conn = init_db(node.config.social_db_path)
        try:
            assert get_post(conn, post["hash"]) is None
            assert _delete_entry_exists(conn, post["hash"])
        finally:
            conn.close()
    finally:
        bridge.stop()


def test_peers_marks_blocked_peers(bridge, node):
    from tau.blocklist import BlockList

    node.blocklist = BlockList(blocked=frozenset({PEER_HASH}))
    _, _, payload = _json(bridge, "/api/peers")
    assert payload["peers"][0]["destination"] == PEER_HASH
    assert payload["peers"][0]["blocked"] is True

    node.blocklist = BlockList()
    _, _, payload = _json(bridge, "/api/peers")
    assert payload["peers"][0]["blocked"] is False


def test_peers_marks_everyone_blocked_when_list_failed_closed(bridge, node):
    from tau.blocklist import BlockList

    node.blocklist = BlockList(block_all=True)
    _, _, payload = _json(bridge, "/api/peers")
    assert payload["peers"][0]["blocked"] is True


def test_messages_hides_blocked_senders(bridge, node):
    from tau.blocklist import BlockList

    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "from alice",
            "sent_at": 100.0,
            "received_at": 101.0,
        }
    )
    bridge.record_inbound(
        {
            "from": "mallory",
            "destination": OTHER_HASH,
            "text": "from mallory",
            "sent_at": 102.0,
            "received_at": 103.0,
        }
    )
    node.blocklist = BlockList(blocked=frozenset({OTHER_HASH}))
    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == ["from alice"]

    # Unblocking restores the view: nothing was deleted.
    node.blocklist = BlockList()
    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == ["from alice", "from mallory"]

    # A list that failed closed hides every inbound message.
    node.blocklist = BlockList(block_all=True)
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []


def test_tan_timeline_hides_blocked_authors(tmp_path):
    import RNS

    from tau.blocklist import BlockList
    from tau.config import TauConfig
    from tau.identity import destination_hash
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    alice, bob = RNS.Identity(), RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, alice, "from alice", name="alice", now=1000)
        create_post(conn, bob, "from bob", name="bob", now=1001)
    finally:
        conn.close()
    # The console uses the node's cached block-list, the same snapshot
    # the DM gate enforces.
    node.blocklist = BlockList(blocked=frozenset({destination_hash(alice).hex()}))

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["from bob"]

        node.blocklist = BlockList(block_all=True)
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1")
        assert status == 200
        assert payload["posts"] == []
    finally:
        bridge.stop()


def test_tan_top_via_api(tmp_path):
    import RNS

    from tau.blocklist import BlockList
    from tau.config import TauConfig
    from tau.identity import destination_hash
    from tau.tangents.tan import create_post, create_reaction, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    alice, bob = RNS.Identity(), RNS.Identity()
    carol = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, alice, "fresh flop", name="alice", now=1003)
        hit = create_post(conn, bob, "old hit", name="bob", now=1000)
        create_reaction(conn, alice, hit["hash"], liked=True, name="alice", now=1004)
        create_reaction(conn, carol, hit["hash"], liked=True, name="carol", now=1005)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/top")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "old hit",
            "fresh flop",
        ]
        assert payload["count"] == 2
        assert payload["posts"][0]["likes"] == 2

        status, _, payload = _json(bridge, "/api/tan/top?limit=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["old hit"]

        status, _, payload = _json(
            bridge, f"/api/tan/top?author={destination_hash(alice).hex()}"
        )
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["fresh flop"]

        status, _, payload = _json(bridge, "/api/tan/top?author=nope")
        assert status == 400

        # Blocked authors never rank, and a failed-closed list hides all.
        node.blocklist = BlockList(blocked=frozenset({destination_hash(bob).hex()}))
        status, _, payload = _json(bridge, "/api/tan/top")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["fresh flop"]

        node.blocklist = BlockList(block_all=True)
        status, _, payload = _json(bridge, "/api/tan/top")
        assert status == 200
        assert payload["posts"] == []
    finally:
        bridge.stop()


def test_tan_tags_via_api(tmp_path):
    import RNS

    from tau.blocklist import BlockList
    from tau.config import TauConfig
    from tau.identity import destination_hash
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    alice, bob = RNS.Identity(), RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, alice, "first #mesh post", name="alice", now=1000)
        create_post(conn, alice, "second #mesh #radio post", name="alice", now=1001)
        create_post(conn, bob, "bob #radio post", name="bob", now=1002)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/tags")
        assert status == 200
        assert payload["tags"] == [
            {"tag": "mesh", "count": 2},
            {"tag": "radio", "count": 2},
        ]
        assert payload["count"] == 2

        status, _, payload = _json(bridge, "/api/tan/tags?limit=1")
        assert status == 200
        assert payload["tags"] == [{"tag": "mesh", "count": 2}]

        # A blocked author's posts never count, and a failed-closed
        # list hides every tag.
        node.blocklist = BlockList(blocked=frozenset({destination_hash(bob).hex()}))
        status, _, payload = _json(bridge, "/api/tan/tags")
        assert status == 200
        assert payload["tags"] == [
            {"tag": "mesh", "count": 2},
            {"tag": "radio", "count": 1},
        ]

        node.blocklist = BlockList(block_all=True)
        status, _, payload = _json(bridge, "/api/tan/tags")
        assert status == 200
        assert payload["tags"] == []
    finally:
        bridge.stop()


def test_tan_timeline_tag_filter_via_api(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    alice, bob = RNS.Identity(), RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(conn, alice, "tagged #mesh post", name="alice", now=1000)
        create_post(conn, bob, "plain post", name="bob", now=1001)
        create_post(conn, bob, "another #mesh post", name="bob", now=1002)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1&tag=mesh")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "another #mesh post",
            "tagged #mesh post",
        ]

        # A leading hash and case do not matter: the tag is normalised.
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1&tag=%23MESH")
        assert status == 200
        assert payload["count"] == 2

        # An unused tag matches nothing, and a malformed tag is a 400.
        status, _, payload = _json(bridge, "/api/tan/timeline?all=1&tag=radio")
        assert status == 200
        assert payload["posts"] == []

        status, _, payload = _json(bridge, "/api/tan/timeline?all=1&tag=1bad")
        assert status == 400
    finally:
        bridge.stop()


def test_tan_top_matches_timeline_annotations(tmp_path):
    import RNS

    from tau.blocklist import BlockList
    from tau.config import TauConfig
    from tau.identity import destination_hash
    from tau.tangents.tan import (
        create_edit,
        create_post,
        create_quote,
        create_reaction,
        create_reply,
        create_repost,
        init_db,
        mute_keyword,
    )

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    alice, bob = RNS.Identity(), RNS.Identity()
    carol = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        hit = create_post(conn, bob, "old hit", name="bob", now=1000)
        draft = create_post(conn, alice, "rough draft", name="alice", now=1001)
        create_reply(conn, alice, hit["hash"], "agreed", name="alice", now=1002)
        create_post(conn, alice, "fresh flop", name="alice", now=1003)
        create_reaction(conn, alice, hit["hash"], liked=True, name="alice", now=1004)
        create_reaction(conn, carol, hit["hash"], liked=True, name="carol", now=1005)
        create_reaction(conn, bob, draft["hash"], liked=True, name="bob", now=1006)
        create_edit(conn, alice, draft["hash"], "polished post", name="alice", now=1007)
        create_repost(conn, carol, hit["hash"], name="carol", now=1008)
        create_quote(conn, carol, hit["hash"], "second this", name="carol", now=1009)
        # An edit that introduces a muted keyword hides the post: the
        # keyword test sees the edited text, as in the timeline.
        bait = create_post(conn, alice, "clean post", name="alice", now=1010)
        mute_keyword(conn, "crypto")
        create_edit(conn, alice, bait["hash"], "crypto post", name="alice", now=1011)
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(bridge, "/api/tan/top")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == [
            "old hit",
            "polished post",
            "fresh flop",
            "agreed",
        ]
        ranked = {item["text"]: item for item in payload["posts"]}
        assert ranked["polished post"]["edited"] is True
        assert ranked["old hit"]["reposts"] == 1
        assert ranked["old hit"]["quotes"] == 1
        # The edit that introduced the muted keyword hid the post.
        assert "crypto post" not in ranked

        # A blocked top-ranked author never takes a result slot: with
        # limit=1 the next visible post ranks instead.
        node.blocklist = BlockList(blocked=frozenset({destination_hash(bob).hex()}))
        status, _, payload = _json(bridge, "/api/tan/top?limit=1")
        assert status == 200
        assert [item["text"] for item in payload["posts"]] == ["polished post"]

        # A visible reply to a blocked author's post keeps its parent
        # redacted — no name, hash or text leaks through the preview.
        status, _, payload = _json(bridge, "/api/tan/top")
        assert status == 200
        reply = next(item for item in payload["posts"] if item["text"] == "agreed")
        assert reply["reply_to"] == {
            "hash": hit["hash"],
            "author": "",
            "name": "",
            "text": "",
            "deleted": False,
            "blocked": True,
        }

        # An explicit lookup of a blocked author shows nothing, as in
        # author_timeline.
        status, _, payload = _json(
            bridge, f"/api/tan/top?author={destination_hash(bob).hex()}"
        )
        assert status == 200
        assert payload["posts"] == []
    finally:
        bridge.stop()


def test_messages_reconciles_cli_made_reactions(node, tmp_path):
    """A reaction written to the shared inbox by another process (the
    CLI) shows up on the next read of the console history.

    The write goes through a *second* Inbox object on the same path —
    the way a separate ``tau react`` process writes — so the bridge's
    own inbox must reload from disk to see it; reading its in-memory
    cache would stay stale.
    """
    from tau.config import TauConfig
    from tau.inbox import Inbox

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        inbox_path = TauConfig.resolve(data_dir=tmp_path, name="tester").inbox_path
        node.inbox = Inbox(inbox_path)
        cli_inbox = Inbox(inbox_path)
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "hello",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "wire-1",
            }
        )
        # The CLI process writes only the inbox; the bridge never sees
        # a live callback for it.
        cli_inbox.record_reaction(
            {
                "target": "wire-1",
                "destination": PEER_HASH,
                "from": "alice",
                "reaction": "👍",
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "react-1",
            }
        )
        messages = bridge.messages()
        assert messages[0]["reactions"] == [
            {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
        ]
        # ...and a CLI retraction clears it on the next read too.
        cli_inbox.record_reaction(
            {
                "target": "wire-1",
                "destination": PEER_HASH,
                "from": "alice",
                "reaction": "",
                "sent_at": 104.0,
                "received_at": 105.0,
                "id": "react-2",
            }
        )
        assert bridge.messages()[0]["reactions"] == []
    finally:
        bridge.stop()


def test_group_message_hides_non_member_reaction(node, tmp_path):
    """A stored reaction from a non-member never displays on a group
    message, even when the authorization recheck or its cleanup failed
    and left it in the inbox (CodeRabbit round-11 follow-up).

    The copy is appended directly and the reaction stored directly —
    the way a copy imported by another process lands, with no
    arrival purge running in this process — so the console read is
    the only thing standing between the stored reaction and the
    display.  A member's reaction on the same message still shows.
    """
    from tau.config import TauConfig
    from tau.inbox import Inbox

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        inbox_path = TauConfig.resolve(data_dir=tmp_path, name="tester").inbox_path
        node.inbox = Inbox(inbox_path)
        group = node.groups.find("crew")
        node.inbox.append(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "crew hello",
                "kind": "group",
                "group": group["id"],
                "group_name": "crew",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "wire-g1",
            }
        )
        node.inbox.record_reaction(
            {
                "target": "wire-g1",
                "destination": OTHER_HASH,
                "from": "mallory",
                "reaction": "👎",
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "react-1",
            }
        )
        node.inbox.record_reaction(
            {
                "target": "wire-g1",
                "destination": PEER_HASH,
                "from": "alice",
                "reaction": "👍",
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "react-2",
            }
        )
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "crew hello",
                "kind": "group",
                "group": group["id"],
                "group_name": "crew",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "wire-g1",
            }
        )
        messages = bridge.messages()
        assert messages[0]["reactions"] == [
            {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
        ]
    finally:
        bridge.stop()


def test_imported_group_message_event_hides_non_member_reaction(node, tmp_path):
    """A live import event never carries a non-member's reaction
    (CodeRabbit round-12 follow-up).

    Ordering: a no-context reaction arrives before its target, so
    both lookups miss and the console caches it; the group message
    is then imported, which seeds the entry from that cache and
    publishes it live.  The roster is pinned throughout, so the
    import event must drop the non-member reaction while keeping a
    member's.  The snapshot read hides it too.
    """
    from tau.config import TauConfig
    from tau.inbox import Inbox

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        inbox_path = TauConfig.resolve(data_dir=tmp_path, name="tester").inbox_path
        node.inbox = Inbox(inbox_path)
        group = node.groups.find("crew")
        # The reaction arrives before its target: no history entry
        # matches, so nothing is published, but the console caches it.
        assert (
            bridge.record_reaction(
                {
                    "target": "wire-g1",
                    "destination": OTHER_HASH,
                    "from": "mallory",
                    "reaction": "👎",
                    "sent_at": 102.0,
                    "received_at": 103.0,
                    "id": "react-1",
                }
            )
            is None
        )
        assert (
            bridge.record_reaction(
                {
                    "target": "wire-g1",
                    "destination": PEER_HASH,
                    "from": "alice",
                    "reaction": "👍",
                    "sent_at": 102.0,
                    "received_at": 103.0,
                    "id": "react-2",
                }
            )
            is None
        )
        node.inbox.record_reaction(
            {
                "target": "wire-g1",
                "destination": OTHER_HASH,
                "from": "mallory",
                "reaction": "👎",
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "react-1",
            }
        )
        node.inbox.record_reaction(
            {
                "target": "wire-g1",
                "destination": PEER_HASH,
                "from": "alice",
                "reaction": "👍",
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "react-2",
            }
        )
        bridge._mirror_imported_messages(
            [
                {
                    "from": "alice",
                    "destination": PEER_HASH,
                    "text": "crew hello",
                    "kind": "group",
                    "group": group["id"],
                    "group_name": "crew",
                    "sent_at": 100.0,
                    "received_at": 101.0,
                    "id": "wire-g1",
                }
            ]
        )
        events = [
            event
            for event in bridge.bus.replay()
            if event.get("type") == "message"
            and event.get("message", {}).get("wire_id") == "wire-g1"
        ]
        assert len(events) == 1
        assert events[0]["message"]["reactions"] == [
            {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
        ]
        assert bridge.messages()[0]["reactions"] == [
            {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
        ]
    finally:
        bridge.stop()



def test_imported_group_message_hides_all_reactions_without_pinned_roster(
    node, tmp_path
):
    """A copy imported without a pinned roster shows no reactions at
    all — not even a member's — until a roster pins (CodeRabbit
    round-13 follow-up).

    The group id was never created, so pinned_rosters returns None
    for it; visible_reactions(None) is fail-closed, so the live import
    event and the snapshot read both hide every stored reaction.
    """
    from tau.config import TauConfig
    from tau.inbox import Inbox

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        inbox_path = TauConfig.resolve(data_dir=tmp_path, name="tester").inbox_path
        node.inbox = Inbox(inbox_path)
        unpinned = "ff" * 16
        # A member's reaction arrives before its target and is cached.
        assert (
            bridge.record_reaction(
                {
                    "target": "wire-g2",
                    "destination": PEER_HASH,
                    "from": "alice",
                    "reaction": "👍",
                    "sent_at": 102.0,
                    "received_at": 103.0,
                    "id": "react-3",
                }
            )
            is None
        )
        node.inbox.record_reaction(
            {
                "target": "wire-g2",
                "destination": PEER_HASH,
                "from": "alice",
                "reaction": "👍",
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "react-3",
            }
        )
        bridge._mirror_imported_messages(
            [
                {
                    "from": "alice",
                    "destination": PEER_HASH,
                    "text": "orphan crew hello",
                    "kind": "group",
                    "group": unpinned,
                    "group_name": "orphan",
                    "sent_at": 100.0,
                    "received_at": 101.0,
                    "id": "wire-g2",
                }
            ]
        )
        events = [
            event
            for event in bridge.bus.replay()
            if event.get("type") == "message"
            and event.get("message", {}).get("wire_id") == "wire-g2"
        ]
        assert len(events) == 1
        assert events[0]["message"]["reactions"] == []
        assert bridge.messages()[0]["reactions"] == []
    finally:
        bridge.stop()

def test_delayed_reaction_event_hides_non_member_reaction(node, tmp_path):
    """A delayed live reaction never reaches the console unfiltered
    (CodeRabbit round-13 follow-up on the import-event fix).

    Ordering: the group message is already in the console history
    when a non-member's no-context reaction reaches the callback —
    it passed the handler's final lookup before the copy arrived,
    so the callback is the first roster check it meets.  The
    message_updated event must drop the non-member reaction while
    keeping a member's, and the stored entry must not keep it either.
    """
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        group = node.groups.find("crew")
        assert (
            bridge.record_inbound(
                {
                    "from": "alice",
                    "destination": PEER_HASH,
                    "text": "crew hello",
                    "kind": "group",
                    "group": group["id"],
                    "group_name": "crew",
                    "sent_at": 100.0,
                    "received_at": 101.0,
                    "id": "wire-g1",
                }
            )
            is not None
        )
        assert (
            bridge.record_reaction(
                {
                    "target": "wire-g1",
                    "destination": OTHER_HASH,
                    "from": "mallory",
                    "reaction": "👎",
                    "sent_at": 102.0,
                    "received_at": 103.0,
                    "id": "react-1",
                }
            )
            is not None
        )
        assert (
            bridge.record_reaction(
                {
                    "target": "wire-g1",
                    "destination": PEER_HASH,
                    "from": "alice",
                    "reaction": "👍",
                    "sent_at": 102.0,
                    "received_at": 103.0,
                    "id": "react-2",
                }
            )
            is not None
        )
        events = [
            event
            for event in bridge.bus.replay()
            if event.get("type") == "message_updated"
            and event.get("message", {}).get("wire_id") == "wire-g1"
        ]
        assert len(events) == 2
        assert events[0]["message"]["reactions"] == []
        assert events[1]["message"]["reactions"] == [
            {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
        ]
        assert bridge.messages()[0]["reactions"] == [
            {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
        ]
    finally:
        bridge.stop()


def test_live_group_message_shows_early_reaction_after_reconcile(
    node, tmp_path
):
    """A live group copy shows a reaction that arrived before it (#304).

    Ordering: a member's reaction names another member's copy of the
    message (the foreign wire id), so the console caches it under an
    id no entry will ever carry; the inbox retargets it onto this
    node's copy when the copy lands.  The live event for the arriving
    copy must carry the retargeted reaction — the snapshot read
    already did — while still dropping a non-member's reaction.
    """
    from tau.config import TauConfig
    from tau.inbox import Inbox

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        inbox_path = TauConfig.resolve(data_dir=tmp_path, name="tester").inbox_path
        node.inbox = Inbox(inbox_path)
        group = node.groups.find("crew")
        members = set(group.get("members") or [])
        # Both reactions arrive before the copy, naming the foreign id.
        for record in (
            {
                "target": "wire-foreign",
                "destination": OTHER_HASH,
                "from": "mallory",
                "reaction": "👎",
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "react-1",
                "group": group["id"],
                "target_author": PEER_HASH,
                "target_sent_at": 100.0,
            },
            {
                "target": "wire-foreign",
                "destination": PEER_HASH,
                "from": "alice",
                "reaction": "👍",
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "react-2",
                "group": group["id"],
                "target_author": PEER_HASH,
                "target_sent_at": 100.0,
            },
        ):
            assert bridge.record_reaction(record) is None
            node.inbox.record_reaction(record)
        # The copy lands: the inbox retargets the member's reaction
        # onto this node's wire id.
        node.inbox.append_group_message(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "crew hello",
                "kind": "group",
                "group": group["id"],
                "group_name": "crew",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "wire-local",
            },
            group=group["id"],
            author=PEER_HASH,
            sent_at=100.0,
            members=members,
        )
        assert (
            bridge.record_inbound(
                {
                    "from": "alice",
                    "destination": PEER_HASH,
                    "text": "crew hello",
                    "kind": "group",
                    "group": group["id"],
                    "group_name": "crew",
                    "sent_at": 100.0,
                    "received_at": 101.0,
                    "id": "wire-local",
                }
            )
            is not None
        )
        events = [
            event
            for event in bridge.bus.replay()
            if event.get("type") == "message"
            and event.get("message", {}).get("wire_id") == "wire-local"
        ]
        assert len(events) == 1
        assert events[0]["message"]["reactions"] == [
            {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
        ]
        assert bridge.messages()[0]["reactions"] == [
            {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
        ]
    finally:
        bridge.stop()


def test_live_group_message_shows_inbox_only_reaction(node, tmp_path):
    """A live group copy shows a reaction the console never cached.

    Same gap as #304, without the foreign id: the reaction is stored
    in the inbox only (recorded by another process), so the cache
    seeds nothing and only the inbox overlay can carry it onto the
    live event.  The snapshot read already showed it.
    """
    from tau.config import TauConfig
    from tau.inbox import Inbox

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        inbox_path = TauConfig.resolve(data_dir=tmp_path, name="tester").inbox_path
        node.inbox = Inbox(inbox_path)
        group = node.groups.find("crew")
        node.inbox.record_reaction(
            {
                "target": "wire-g1",
                "destination": PEER_HASH,
                "from": "alice",
                "reaction": "👍",
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "react-2",
            }
        )
        assert (
            bridge.record_inbound(
                {
                    "from": "alice",
                    "destination": PEER_HASH,
                    "text": "crew hello",
                    "kind": "group",
                    "group": group["id"],
                    "group_name": "crew",
                    "sent_at": 100.0,
                    "received_at": 101.0,
                    "id": "wire-g1",
                }
            )
            is not None
        )
        events = [
            event
            for event in bridge.bus.replay()
            if event.get("type") == "message"
            and event.get("message", {}).get("wire_id") == "wire-g1"
        ]
        assert len(events) == 1
        assert events[0]["message"]["reactions"] == [
            {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
        ]
        assert bridge.messages()[0]["reactions"] == [
            {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
        ]
    finally:
        bridge.stop()


def test_live_group_message_shows_reaction_written_by_another_process(
    node, tmp_path
):
    """A live group copy shows a reaction another process wrote (#304).

    The reaction is recorded straight to inbox.json by a second Inbox
    (the way ``tau react`` from the CLI lands) after the copy is in
    this node's inbox, so the node's in-memory reaction list is stale.
    The live overlay reads through the transaction path, like the
    snapshot read, so the live event still carries it.
    """
    from tau.config import TauConfig
    from tau.inbox import Inbox

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        inbox_path = TauConfig.resolve(data_dir=tmp_path, name="tester").inbox_path
        node.inbox = Inbox(inbox_path)
        group = node.groups.find("crew")
        node.inbox.append_group_message(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "crew hello",
                "kind": "group",
                "group": group["id"],
                "group_name": "crew",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "wire-local",
            },
            group=group["id"],
            author=PEER_HASH,
            sent_at=100.0,
            members=set(group.get("members") or []),
        )
        # Another process writes the reaction after this node's last
        # inbox reload.
        other = Inbox(inbox_path)
        other.record_reaction(
            {
                "target": "wire-local",
                "destination": PEER_HASH,
                "from": "alice",
                "reaction": "👍",
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "react-2",
            }
        )
        assert (
            bridge.record_inbound(
                {
                    "from": "alice",
                    "destination": PEER_HASH,
                    "text": "crew hello",
                    "kind": "group",
                    "group": group["id"],
                    "group_name": "crew",
                    "sent_at": 100.0,
                    "received_at": 101.0,
                    "id": "wire-local",
                }
            )
            is not None
        )
        events = [
            event
            for event in bridge.bus.replay()
            if event.get("type") == "message"
            and event.get("message", {}).get("wire_id") == "wire-local"
        ]
        assert len(events) == 1
        assert events[0]["message"]["reactions"] == [
            {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
        ]
    finally:
        bridge.stop()


def test_overlaid_group_reactions_persist_to_history(node, tmp_path):
    """Reactions overlaid onto a live entry are saved, not just held
    in memory (#304).

    The overlay runs after _append_message saved, so without a second
    save the on-disk history would reload the entry without them.
    """
    from tau.config import TauConfig
    from tau.inbox import Inbox

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        inbox_path = TauConfig.resolve(data_dir=tmp_path, name="tester").inbox_path
        node.inbox = Inbox(inbox_path)
        group = node.groups.find("crew")
        node.inbox.record_reaction(
            {
                "target": "wire-g1",
                "destination": PEER_HASH,
                "from": "alice",
                "reaction": "👍",
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "react-2",
            }
        )
        assert (
            bridge.record_inbound(
                {
                    "from": "alice",
                    "destination": PEER_HASH,
                    "text": "crew hello",
                    "kind": "group",
                    "group": group["id"],
                    "group_name": "crew",
                    "sent_at": 100.0,
                    "received_at": 101.0,
                    "id": "wire-g1",
                }
            )
            is not None
        )
        stored = json.loads((tmp_path / "messages.json").read_text(encoding="utf-8"))
        entry = next(
            item
            for item in stored["messages"]
            if item.get("wire_id") == "wire-g1"
        )
        assert entry["reactions"] == [
            {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
        ]
    finally:
        bridge.stop()


def test_pin_event_hides_non_member_group_reaction(bridge, node, tmp_path):
    """Pinning a group message must not leak a non-member reaction
    (CodeRabbit Major on PR #303).

    The pin endpoint publishes a ``message_updated`` carrying the
    inbox's overlays; it overlaid reactions without the group-roster
    filter that :meth:`messages` applies, so an open tab learned of
    a CLI-made reaction a fresh read hides.  The event must agree
    with the listing.
    """
    from tau.config import TauConfig
    from tau.inbox import Inbox

    inbox_path = TauConfig.resolve(data_dir=tmp_path, name="tester").inbox_path
    node.inbox = Inbox(inbox_path)
    cli_inbox = Inbox(inbox_path)
    group = node.groups.find("crew")
    message = {
        "from": "alice",
        "destination": PEER_HASH,
        "text": "crew hello",
        "kind": "group",
        "group": group["id"],
        "group_name": "crew",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "wire-g1",
    }
    node.inbox.append(dict(message))
    assert bridge.record_inbound(dict(message)) is not None
    # CLI-made reactions, written by a second process: one from a
    # non-member, one from a member.
    cli_inbox.record_reaction(
        {
            "target": "wire-g1",
            "destination": OTHER_HASH,
            "from": "mallory",
            "reaction": "👎",
            "sent_at": 102.0,
            "received_at": 103.0,
            "id": "react-1",
        }
    )
    cli_inbox.record_reaction(
        {
            "target": "wire-g1",
            "destination": PEER_HASH,
            "from": "alice",
            "reaction": "👍",
            "sent_at": 102.0,
            "received_at": 103.0,
            "id": "react-2",
        }
    )
    status, _, _ = _json(
        bridge, "/api/messages/pin", method="POST", body={"id": 1}
    )
    assert status == 200
    updates = [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_updated"
        and event.get("message", {}).get("wire_id") == "wire-g1"
    ]
    assert len(updates) == 1
    assert updates[0]["message"]["reactions"] == [
        {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
    ]
    assert updates[0]["message"]["pinned"] is True
    assert bridge.messages()[0]["reactions"] == [
        {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
    ]




def test_react_endpoint_reacts_to_a_history_message(bridge, node):
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )
    sent = []

    def send_reaction(peer, target, token, *, timeout=20.0, discover_timeout=None):
        sent.append((peer, target, token))
        # Mirror the node's local recording so the console annotates.
        bridge.record_reaction(
            {
                "target": target,
                "reaction": token,
                "from": "tester",
                "destination": "cd" * 16,
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "react-1",
            }
        )
        return {"ok": True, "reaction": token, "target": target, "id": "react-1"}

    node.send_reaction = send_reaction
    status, _, payload = _json(
        bridge,
        "/api/react",
        method="POST",
        body={"target": "wire-1", "reaction": "👍"},
    )
    assert status == 200
    assert payload["target"] == "wire-1"
    assert payload["reaction"] == "👍"
    assert payload["cleared"] is False
    assert sent == [(PEER_HASH, "wire-1", "👍")]

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["reactions"] == [
        {"reactor": "cd" * 16, "name": "tester", "reaction": "👍"}
    ]


def test_react_endpoint_reports_queued_reaction(bridge, node):
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )

    def send_reaction(peer, target, token, *, timeout=20.0, discover_timeout=None):
        return {
            "ok": False,
            "queued": True,
            "target": target,
            "reaction": token,
            "cleared": False,
            "id": "react-1",
            "queued_id": "q-1",
        }

    node.send_reaction = send_reaction
    status, _, payload = _json(
        bridge,
        "/api/react",
        method="POST",
        body={"target": "wire-1", "reaction": "👍"},
    )
    assert status == 200
    assert payload["target"] == "wire-1"
    assert payload["reaction"] == "👍"
    assert payload["queued"] is True
    assert payload["queued_id"] == "q-1"


def test_react_endpoint_rejects_unknown_target_and_bad_token(bridge):
    status, _, payload = _json(
        bridge, "/api/react", method="POST", body={"target": "nope", "reaction": "👍"}
    )
    assert status == 400
    assert "unknown message" in payload["error"]["message"]


def test_typing_endpoint_sends_to_a_peer(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/typing",
        method="POST",
        body={"peer": PEER_HASH, "state": "start"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["state"] == "start"
    assert node.typing == [(PEER_HASH, "start")]


def test_typing_endpoint_fans_out_to_a_group(bridge, node):
    group_id = node.groups.list_groups()[0]["id"]
    status, _, payload = _json(
        bridge,
        "/api/typing",
        method="POST",
        body={"group": group_id, "state": "start"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["group"] == group_id
    assert node.group_typing == [(group_id, "start")]


def test_typing_endpoint_rejects_bad_state_and_both_targets(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/typing",
        method="POST",
        body={"peer": PEER_HASH, "state": "maybe"},
    )
    assert status == 400
    status, _, payload = _json(
        bridge,
        "/api/typing",
        method="POST",
        body={"peer": PEER_HASH, "group": "ab" * 16, "state": "start"},
    )
    assert status == 400
    assert node.typing == []
    assert node.group_typing == []


def test_typing_endpoint_rejects_unknown_group(bridge):
    status, _, payload = _json(
        bridge,
        "/api/typing",
        method="POST",
        body={"group": "00" * 16, "state": "start"},
    )
    assert status == 400
    assert "unknown group" in payload["error"]["message"]


def test_typing_event_reaches_the_bus(bridge, node):
    # The bridge chains the node's on_typing callback and publishes
    # the event on the bus, so the live console learns about it.
    subscriber = bridge.bus.subscribe()
    try:
        node.on_typing(
            {
                "conversation": PEER_HASH,
                "peer": PEER_HASH,
                "name": "alice",
                "state": "start",
                "group": None,
                "received_at": 1000.0,
            }
        )
        event = subscriber.get(timeout=5)
    finally:
        bridge.bus.unsubscribe(subscriber)
    assert event["type"] == "typing"
    assert event["peer"] == PEER_HASH
    assert event["state"] == "start"


# ------------------------------------------------------------------ beacons

BEACON_PIN = {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"}
BEACON_PIN_2 = {"lat": 34.06, "lon": -83.87}


def _start_beacon(bridge, **body):
    return _json(
        bridge,
        "/api/beacons/start",
        method="POST",
        body={"peer": "alice", **BEACON_PIN, **body},
    )


def test_beacons_endpoint_lists_active_beacons(bridge, node):
    status, _, payload = _json(bridge, "/api/beacons")
    assert status == 200
    assert payload == {"beacons": [], "count": 0}

    status, _, started = _start_beacon(bridge, duration_seconds=900)
    assert status == 200
    beacon = started["beacon"]
    assert beacon["target"] == PEER_HASH
    assert beacon["target_kind"] == "dm"
    assert beacon["active"] is True
    assert beacon["updates_sent"] == 1

    status, _, payload = _json(bridge, "/api/beacons")
    assert status == 200
    assert payload["count"] == 1
    assert payload["beacons"][0]["id"] == beacon["id"]


def test_beacon_start_sends_first_update_and_records_it(bridge, node, tmp_path):
    status, _, payload = _start_beacon(bridge, duration_seconds=900)
    assert status == 200
    beacon = payload["beacon"]
    # The first update goes out at once, as a location message with
    # the beacon marker — and the sender's thread shows it.
    assert node.sent[0][1] == "Rock Springs Park"
    assert node.last_location == BEACON_PIN
    assert node.last_beacon == {
        "id": beacon["id"],
        "expires_at": beacon["expires_at"],
        "seq": 1,
    }
    assert node.last_kind == "location"
    # The browser feeds positions through a file the mail loop
    # re-reads; it starts life holding the start position.
    position_file = tmp_path / "beacon-positions" / f"{beacon['id']}.json"
    assert position_file.is_file()
    assert json.loads(position_file.read_text(encoding="utf-8")) == BEACON_PIN
    status, _, listed = _json(bridge, "/api/messages")
    entries = [m for m in listed["messages"] if m.get("beacon")]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["direction"] == "out"
    assert entry["peer"] == PEER_HASH
    assert entry["location"] == BEACON_PIN
    assert entry["beacon"] == {
        "id": beacon["id"],
        "expires_at": beacon["expires_at"],
        "seq": 1,
    }


def test_beacon_start_records_one_entry_for_a_group(bridge, node):
    group_id = node.groups.list_groups()[0]["id"]
    status, _, payload = _json(
        bridge,
        "/api/beacons/start",
        method="POST",
        body={
            "group": group_id,
            "lat": 34.05,
            "lon": -83.88,
            "duration_seconds": 900,
        },
    )
    assert status == 200
    beacon = payload["beacon"]
    assert beacon["target_kind"] == "group"
    assert node.last_group_location == {"lat": 34.05, "lon": -83.88}
    assert node.last_group_beacon["id"] == beacon["id"]
    # One entry for the update, not one per leg: a group beacon
    # must not spam the sender's thread once per member.
    status, _, listed = _json(bridge, "/api/messages")
    entries = [m for m in listed["messages"] if m.get("beacon")]
    assert len(entries) == 1
    assert entries[0]["group"] == group_id
    assert entries[0]["peer"] is None
    assert entries[0]["sent_at"] == 1000.0


def test_beacon_start_records_one_entry_per_member_for_a_broadcast(
    bridge, node, tmp_path
):
    from tau.broadcasts import BroadcastStore

    node.broadcasts = BroadcastStore(tmp_path / "broadcasts.json")
    node.broadcasts.create("family", [PEER_HASH])
    status, _, payload = _json(
        bridge,
        "/api/beacons/start",
        method="POST",
        body={
            "broadcast": "family",
            "lat": 34.05,
            "lon": -83.88,
            "duration_seconds": 900,
        },
    )
    assert status == 200
    beacon = payload["beacon"]
    assert beacon["target_kind"] == "broadcast"
    assert beacon["target"] == "family"
    assert node.last_beacon["id"] == beacon["id"]
    # One entry per member, in that member's DM thread — the update
    # was one DM per member, and there is no broadcast thread to put
    # a single entry in.
    status, _, listed = _json(bridge, "/api/messages")
    entries = [m for m in listed["messages"] if m.get("beacon")]
    assert len(entries) == 1
    assert entries[0]["peer"] == PEER_HASH
    assert entries[0].get("group") is None
    assert entries[0]["location"] == {"lat": 34.05, "lon": -83.88}
    assert entries[0]["sent_at"] == 1000.0


def test_beacon_start_posts_the_update_into_the_channel(bridge, node):
    from tau.channels import init_db, join_channel

    conn = init_db(node.config.channels_db_path)
    try:
        join_channel(conn, "mesh-meetup")
    finally:
        conn.close()
    status, _, payload = _json(
        bridge,
        "/api/beacons/start",
        method="POST",
        body={
            "channel": "mesh-meetup",
            "lat": 34.05,
            "lon": -83.88,
            "name": "Rock Springs Park",
            "duration_seconds": 900,
        },
    )
    assert status == 200
    beacon = payload["beacon"]
    assert beacon["target_kind"] == "channel"
    assert beacon["target"] == "mesh-meetup"
    # The update is a channel message with the pin and the marker —
    # and nothing is recorded in the DM threads: the channel's own
    # history is where the update lives.
    status, _, history = _json(
        bridge, "/api/channels/messages?channel=mesh-meetup"
    )
    assert status == 200
    assert len(history["messages"]) == 1
    entry = history["messages"][0]
    assert entry["text"] == "Rock Springs Park"
    assert entry["location"] == BEACON_PIN
    assert entry["beacon"] == {
        "id": beacon["id"],
        "expires_at": beacon["expires_at"],
        "seq": 1,
    }
    status, _, listed = _json(bridge, "/api/messages")
    assert [m for m in listed["messages"] if m.get("beacon")] == []
    # A channel this node has not joined refuses the beacon.
    status, _, payload = _json(
        bridge,
        "/api/beacons/start",
        method="POST",
        body={
            "channel": "other-room",
            "lat": 34.05,
            "lon": -83.88,
            "duration_seconds": 900,
        },
    )
    assert status == 502


def test_beacon_start_rejects_bad_requests(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/beacons/start",
        method="POST",
        body={"peer": "alice", "lat": 91.0, "lon": -83.88},
    )
    assert status == 400
    status, _, payload = _json(
        bridge,
        "/api/beacons/start",
        method="POST",
        body={"peer": "alice", "lat": 34.05, "lon": -83.88, "duration_seconds": 30},
    )
    assert status == 400
    status, _, payload = _json(
        bridge,
        "/api/beacons/start",
        method="POST",
        body={
            "peer": "alice",
            "lat": 34.05,
            "lon": -83.88,
            "duration_seconds": 99999,
        },
    )
    assert status == 400
    status, _, payload = _json(
        bridge,
        "/api/beacons/start",
        method="POST",
        body={"lat": 34.05, "lon": -83.88},
    )
    assert status == 400
    status, _, payload = _json(
        bridge,
        "/api/beacons/start",
        method="POST",
        body={"peer": "alice", "group": "ab" * 16, "lat": 34.05, "lon": -83.88},
    )
    assert status == 400
    status, _, payload = _json(
        bridge,
        "/api/beacons/start",
        method="POST",
        body={"peer": "alice", "lat": 34.05, "lon": -83.88, "coarse": "yes"},
    )
    assert status == 400
    assert node.sent == []
    status, _, payload = _json(
        bridge,
        "/api/beacons/start",
        method="POST",
        body={"peer": "nobody", "lat": 34.05, "lon": -83.88},
    )
    assert status == 502


def test_beacon_position_feeds_the_beacon(bridge, node, tmp_path):
    _, _, started = _start_beacon(bridge, duration_seconds=900)
    beacon_id = started["beacon"]["id"]
    status, _, payload = _json(
        bridge,
        "/api/beacons/position",
        method="POST",
        body={"id": beacon_id, **BEACON_PIN_2},
    )
    assert status == 200
    assert payload["beacon"]["location"] == BEACON_PIN_2
    position_file = tmp_path / "beacon-positions" / f"{beacon_id}.json"
    assert json.loads(position_file.read_text(encoding="utf-8")) == BEACON_PIN_2
    # The next tick sends the fed position, not the start one.
    beacon = load_beacons(node.config.beacons_path).get(beacon_id)
    node.send_beacon_update(beacon)
    assert node.last_location == BEACON_PIN_2
    assert node.last_beacon["seq"] == 2


def test_beacon_position_rejects_unknown_stopped_and_bad(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/beacons/position",
        method="POST",
        body={"id": "00" * 8, "lat": 34.05, "lon": -83.88},
    )
    assert status == 400
    _, _, started = _start_beacon(bridge, duration_seconds=900)
    beacon_id = started["beacon"]["id"]
    status, _, payload = _json(
        bridge, "/api/beacons/stop", method="POST", body={"id": beacon_id}
    )
    assert status == 200
    status, _, payload = _json(
        bridge,
        "/api/beacons/position",
        method="POST",
        body={"id": beacon_id, "lat": 34.05, "lon": -83.88},
    )
    assert status == 400
    assert "not running" in payload["error"]["message"]
    _, _, started = _start_beacon(bridge, duration_seconds=900)
    status, _, payload = _json(
        bridge,
        "/api/beacons/position",
        method="POST",
        body={"id": started["beacon"]["id"], "lat": 91.0, "lon": -83.88},
    )
    assert status == 400


def test_beacon_position_refuses_a_foreign_position_file(bridge, node, tmp_path):
    # A beacon the CLI started with its own --location-file keeps
    # that file: the console must not clobber a GPS writer it
    # does not own.
    foreign = tmp_path / "gps.json"
    foreign.write_text(
        json.dumps({"lat": 34.05, "lon": -83.88}), encoding="utf-8"
    )
    beacon = node.start_beacon(
        "alice",
        dict(BEACON_PIN),
        location_file=str(foreign),
        duration=900.0,
    )
    status, _, payload = _json(
        bridge,
        "/api/beacons/position",
        method="POST",
        body={"id": beacon.id, "lat": 34.06, "lon": -83.87},
    )
    assert status == 400
    assert "another writer" in payload["error"]["message"]
    assert json.loads(foreign.read_text(encoding="utf-8")) == {
        "lat": 34.05,
        "lon": -83.88,
    }


def test_beacon_stop_stops_and_drops_the_position_file(bridge, node, tmp_path):
    _, _, started = _start_beacon(bridge, duration_seconds=900)
    beacon_id = started["beacon"]["id"]
    position_file = tmp_path / "beacon-positions" / f"{beacon_id}.json"
    assert position_file.is_file()
    status, _, payload = _json(
        bridge, "/api/beacons/stop", method="POST", body={"id": beacon_id}
    )
    assert status == 200
    assert payload["beacon"]["stopped"] is True
    assert payload["beacon"]["active"] is False
    assert not position_file.exists()
    # Stopping again is a no-op the API reports, not a silent ok.
    status, _, payload = _json(
        bridge, "/api/beacons/stop", method="POST", body={"id": beacon_id}
    )
    assert status == 400
    status, _, payload = _json(
        bridge, "/api/beacons/stop", method="POST", body={"id": "00" * 8}
    )
    assert status == 400
    status, _, payload = _json(bridge, "/api/beacons/stop", method="POST", body={})
    assert status == 400


def test_record_inbound_attaches_location_and_beacon(bridge):
    message = bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "Rock Springs Park",
            "sent_at": 1000.0,
            "received_at": 1001.0,
            "id": "00" * 16,
            "location": dict(BEACON_PIN),
            "beacon": {"id": "aa" * 8, "expires_at": 4600.0, "seq": 3},
        }
    )
    assert message is not None
    assert message["location"] == BEACON_PIN
    assert message["beacon"] == {"id": "aa" * 8, "expires_at": 4600.0, "seq": 3}
    # A malformed marker drops instead of the message.
    message = bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "Rock Springs Park",
            "sent_at": 1002.0,
            "received_at": 1003.0,
            "id": "11" * 16,
            "location": dict(BEACON_PIN),
            "beacon": {"id": "not-a-beacon"},
        }
    )
    assert message is not None
    assert message["location"] == BEACON_PIN
    assert "beacon" not in message


def test_beacon_update_event_reaches_the_bus(bridge, node):
    # The bridge chains the node's on_beacon_update callback, so a
    # mail-loop tick lands in the console history as a live event.
    beacon = node.start_beacon("alice", dict(BEACON_PIN), duration=900.0)
    subscriber = bridge.bus.subscribe()
    try:
        node.on_beacon_update(beacon, {"id": "00" * 16, "queued": False})
        event = subscriber.get(timeout=5)
    finally:
        bridge.bus.unsubscribe(subscriber)
    assert event["type"] == "message"
    assert event["message"]["beacon"]["id"] == beacon.id
    assert event["message"]["location"] == BEACON_PIN


def test_react_endpoint_fans_out_to_group_members(bridge, node):
    group_id = node.groups.list_groups()[0]["id"]
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "crew hi",
            "kind": "group",
            "group": group_id,
            "group_name": "crew",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-g",
        }
    )
    status, _, payload = _json(
        bridge,
        "/api/react",
        method="POST",
        body={"target": "wire-g", "reaction": "👍"},
    )
    assert status == 200
    assert payload["target"] == "wire-g"
    assert payload["group"] == group_id
    assert payload["cleared"] is False
    # The author and send time of the reacted-to copy travel with the
    # fan-out, so each member can resolve its own copy.
    assert node.group_reactions == [
        ("crew", "wire-g", "👍", PEER_HASH, 100.0)
    ]
    # The offline member's leg fails instead of queueing: reactions
    # are live-only in v1, and the member results come back.
    assert payload["ok"] is False
    assert [bool(r.get("delivered")) for r in payload["results"]] == [True, False]
    assert payload["results"][1]["error"] == "no route to peer"


def test_react_endpoint_refuses_retired_group_id_matching_a_name(bridge, node):
    # CodeRabbit Major (webapi.py:2885, CWE-706): GroupStore.find
    # falls back to a name match when the id lookup misses, so a
    # retired group id that is also some other group's name resolved
    # to that group and the reaction fanned out to its members.  The
    # stored id is resolved exactly now — a retired id fails instead
    # of falling back to the name.
    retired = node.groups.create(
        "crew", [OTHER_HASH, BOB_HASH], created_by=OTHER_HASH
    )
    # A second group whose NAME is the retired id: find() would
    # resolve the retired id to this group through the name fallback.
    namesake = node.groups.create(
        retired["id"], [PEER_HASH, BOB_HASH], created_by=PEER_HASH
    )
    bridge.record_inbound(
        {
            "from": "bob",
            "destination": OTHER_HASH,
            "text": "old crew hi",
            "kind": "group",
            "group": retired["id"],
            "group_name": "crew",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-old",
        }
    )
    assert node.groups.delete(retired["id"]) is True
    # The name fallback still resolves the retired id to the namesake.
    assert node.groups.find(retired["id"])["id"] == namesake["id"]
    status, _, payload = _json(
        bridge,
        "/api/react",
        method="POST",
        body={"target": "wire-old", "reaction": "👍"},
    )
    assert status == 404
    assert "unknown group" in payload["error"]["message"]
    assert node.group_reactions == []


def test_react_endpoint_fans_out_group_message_from_inbox(bridge, node):
    # The message is past the console's window, so the bridge resolves
    # it from the node inbox instead of its history.
    group_id = node.groups.list_groups()[0]["id"]
    node.inbox.append(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "crew hi",
            "kind": "group",
            "group": group_id,
            "group_name": "crew",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-g",
        }
    )
    status, _, payload = _json(
        bridge,
        "/api/react",
        method="POST",
        body={"target": "wire-g", "reaction": "🎉"},
    )
    assert status == 200
    assert payload["group"] == group_id
    assert node.group_reactions == [("crew", "wire-g", "🎉", PEER_HASH, 100.0)]



def test_react_endpoint_uses_own_hash_for_outbound_group_record_in_inbox(bridge, node):
    # The message is past the console's window, so the bridge resolves
    # it from the node inbox.  An outbound record's destination is
    # empty — the author is this node, not "" — so the fan-out names
    # this node's hash (CodeRabbit round-13 follow-up).
    group_id = node.groups.list_groups()[0]["id"]
    node.inbox.append(
        {
            "from": "tester",
            "destination": "",
            "text": "crew hi",
            "kind": "group",
            "group": group_id,
            "group_name": "crew",
            "direction": "out",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-g",
        }
    )
    status, _, payload = _json(
        bridge,
        "/api/react",
        method="POST",
        body={"target": "wire-g", "reaction": "🎉"},
    )
    assert status == 200
    assert payload["group"] == group_id
    assert node.group_reactions == [
        ("crew", "wire-g", "🎉", node.destination_hexhash, 100.0)
    ]

def test_react_endpoint_uses_own_hash_for_outbound_group_leg(bridge, node):
    # Reacting to a leg this node sent: the history entry's peer is
    # the recipient, not the author — the author is this node — and
    # the send time is the shared wire time, so every member can
    # resolve its own copy.
    group_id = node.groups.list_groups()[0]["id"]
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": "crew", "text": "hi all"},
    )
    assert status == 200
    wire_id = payload["messages"][0]["wire_id"]
    status, _, payload = _json(
        bridge,
        "/api/react",
        method="POST",
        body={"target": wire_id, "reaction": "👍"},
    )
    assert status == 200
    assert payload["group"] == group_id
    assert node.group_reactions == [
        ("crew", wire_id, "👍", node.destination_hexhash, 1000.0)
    ]


CAROL_HASH = "cd" * 16


def _seed_group_poll(node, bridge, *, author=PEER_HASH, close=False):
    """A group poll in the node inbox and the console history.

    Alice (``PEER_HASH``) asks "Tea or coffee?" at sent time 100;
    carol votes for Coffee (option index 1).  An optional close from
    the poll's author freezes the tally.  Returns the group id.
    """
    group_id = node.groups.list_groups()[0]["id"]
    poll = {
        "from": "alice",
        "destination": author,
        "text": "Tea or coffee?",
        "kind": "group",
        "group": group_id,
        "group_name": "crew",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "wire-poll",
        "poll_options": ["Tea", "Coffee"],
    }
    node.inbox.append(dict(poll))
    bridge.record_inbound(dict(poll), publish=False)
    vote = {
        "from": "carol",
        "destination": CAROL_HASH,
        "text": "voted: Coffee",
        "kind": "group",
        "group": group_id,
        "group_name": "crew",
        "sent_at": 110.0,
        "received_at": 111.0,
        "id": "wire-vote",
        "poll_vote": {"target": {"author": author, "sent_at": 100.0}, "option": 1},
    }
    node.inbox.append(dict(vote))
    bridge.record_inbound(dict(vote), publish=False)
    if close:
        close_record = {
            "from": "alice",
            "destination": author,
            "text": "closed the poll",
            "kind": "group",
            "group": group_id,
            "group_name": "crew",
            "sent_at": 120.0,
            "received_at": 121.0,
            "id": "wire-close",
            "poll_close": {"author": author, "sent_at": 100.0},
        }
        node.inbox.append(dict(close_record))
        bridge.record_inbound(dict(close_record), publish=False)
    return group_id


def test_messages_overlays_group_poll_tally(bridge, node):
    _seed_group_poll(node, bridge)
    entries = bridge.messages()
    poll = next(entry for entry in entries if entry.get("wire_id") == "wire-poll")
    assert poll["poll_options"] == ["Tea", "Coffee"]
    assert poll["poll"] == {
        "options": ["Tea", "Coffee"],
        "counts": [0, 1],
        "total": 1,
        "closed": False,
        "voters": [
            {
                "author": CAROL_HASH,
                "name": "carol",
                "option": 1,
                "voted_at": 110.0,
            }
        ],
        "my_vote": None,
    }
    vote = next(entry for entry in entries if entry.get("wire_id") == "wire-vote")
    assert vote["poll_vote"] == {
        "target": {"author": PEER_HASH, "sent_at": 100.0},
        "option": 1,
    }


def test_group_send_with_poll_options_posts_poll(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={
            "group": "crew",
            "text": "Tea or coffee?",
            "poll_options": ["Tea", "Coffee"],
        },
    )
    assert status == 200
    assert node.last_group_poll_options == ["Tea", "Coffee"]
    assert node.last_group_poll_vote is None
    assert node.last_group_poll_close is None


def test_group_poll_vote_endpoint_votes_by_number_and_text(bridge, node):
    group_id = _seed_group_poll(node, bridge)
    status, _, payload = _json(
        bridge,
        "/api/groups/vote",
        method="POST",
        body={"group": "crew", "poll": "wire-poll", "option": 2},
    )
    assert status == 200
    assert node.group_sent[-1] == ("crew", "voted: Coffee")
    assert node.last_group_poll_vote == {
        "target": {"author": PEER_HASH, "sent_at": 100.0},
        "option": 1,
    }
    status, _, payload = _json(
        bridge,
        "/api/groups/vote",
        method="POST",
        body={"group": group_id, "poll": "wire-p", "option": "Tea"},
    )
    assert status == 200
    assert node.last_group_poll_vote["option"] == 0


def test_group_poll_vote_endpoint_rejects_bad_option(bridge, node):
    _seed_group_poll(node, bridge)
    status, _, payload = _json(
        bridge,
        "/api/groups/vote",
        method="POST",
        body={"group": "crew", "poll": "wire-poll", "option": 3},
    )
    assert status == 400
    assert "option must be" in payload["error"]["message"]
    status, _, payload = _json(
        bridge,
        "/api/groups/vote",
        method="POST",
        body={"group": "crew", "poll": "wire-nope", "option": 1},
    )
    assert status == 400
    assert "no poll" in payload["error"]["message"]


def test_group_poll_vote_endpoint_refuses_closed_poll(bridge, node):
    _seed_group_poll(node, bridge, close=True)
    status, _, payload = _json(
        bridge,
        "/api/groups/vote",
        method="POST",
        body={"group": "crew", "poll": "wire-poll", "option": 1},
    )
    assert status == 400
    assert "closed" in payload["error"]["message"]


def test_group_poll_close_endpoint_requires_creator(bridge, node):
    _seed_group_poll(node, bridge)
    status, _, payload = _json(
        bridge,
        "/api/groups/close-poll",
        method="POST",
        body={"group": "crew", "poll": "wire-poll"},
    )
    assert status == 400
    assert "only the poll's creator" in payload["error"]["message"]


def test_group_poll_close_endpoint_closes_own_poll(bridge, node):
    group_id = node.groups.list_groups()[0]["id"]
    poll = {
        "from": "tester",
        "destination": PEER_HASH,
        "text": "Tea or coffee?",
        "kind": "group",
        "group": group_id,
        "group_name": "crew",
        "direction": "out",
        "leg_sent_at": 100.0,
        "sent_at": 101.0,
        "received_at": 101.0,
        "id": "wire-own-poll",
        "poll_options": ["Tea", "Coffee"],
    }
    node.inbox.append(dict(poll))
    status, _, payload = _json(
        bridge,
        "/api/groups/close-poll",
        method="POST",
        body={"group": "crew", "poll": "wire-own-poll"},
    )
    assert status == 200
    assert node.group_sent[-1] == ("crew", "closed the poll")
    assert node.last_group_poll_close == {
        "author": node.destination_hexhash,
        "sent_at": 100.0,
    }


def _seed_dm_poll(node, bridge, *, author=PEER_HASH, close=False):
    """A DM poll in the node inbox and the console history.

    Alice (``PEER_HASH``) asks "Tea or coffee?" at sent time 100;
    she votes for Coffee (option index 1).  An optional close from
    the poll's author freezes the tally.
    """
    poll = {
        "from": "alice",
        "destination": PEER_HASH,
        "text": "Tea or coffee?",
        "kind": "chat",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "wire-dm-poll",
        "poll_options": ["Tea", "Coffee"],
    }
    node.inbox.append(dict(poll))
    bridge.record_inbound(dict(poll), publish=False)
    vote = {
        "from": "alice",
        "destination": PEER_HASH,
        "text": "voted: Coffee",
        "kind": "chat",
        "sent_at": 110.0,
        "received_at": 111.0,
        "id": "wire-dm-vote",
        "poll_vote": {"target": {"author": author, "sent_at": 100.0}, "option": 1},
    }
    node.inbox.append(dict(vote))
    bridge.record_inbound(dict(vote), publish=False)
    if close:
        close_record = {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "closed the poll",
            "kind": "chat",
            "sent_at": 120.0,
            "received_at": 121.0,
            "id": "wire-dm-close",
            "poll_close": {"author": author, "sent_at": 100.0},
        }
        node.inbox.append(dict(close_record))
        bridge.record_inbound(dict(close_record), publish=False)


def test_messages_overlays_dm_poll_tally(bridge, node):
    _seed_dm_poll(node, bridge)
    entries = bridge.messages()
    poll = next(
        entry for entry in entries if entry.get("wire_id") == "wire-dm-poll"
    )
    assert poll["poll_options"] == ["Tea", "Coffee"]
    assert poll["poll"] == {
        "options": ["Tea", "Coffee"],
        "counts": [0, 1],
        "total": 1,
        "closed": False,
        "voters": [
            {
                "author": PEER_HASH,
                "name": "alice",
                "option": 1,
                "voted_at": 110.0,
            }
        ],
        "my_vote": None,
    }
    vote = next(
        entry for entry in entries if entry.get("wire_id") == "wire-dm-vote"
    )
    assert vote["poll_vote"] == {
        "target": {"author": PEER_HASH, "sent_at": 100.0},
        "option": 1,
    }


def test_dm_send_with_poll_options_posts_poll(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={
            "to": "alice",
            "text": "Tea or coffee?",
            "poll_options": ["Tea", "Coffee"],
        },
    )
    assert status == 200
    assert node.last_poll_options == ["Tea", "Coffee"]
    assert node.last_poll_vote is None
    assert node.last_poll_close is None


PIN = {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"}
CARD = {"name": "carol", "destination_hash": "cc" * 16}


def test_dm_send_with_location_pins_place(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "meet here", "location": dict(PIN)},
    )
    assert status == 200
    assert node.last_location == PIN
    assert payload["message"]["location"] == PIN
    assert payload["message"]["text"] == "meet here"


def test_dm_send_with_contact_shares_card(bridge, node):
    from tau.blocklist import BlockList

    node.blocklist = BlockList()
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "meet carol", "contact": dict(CARD)},
    )
    assert status == 200
    assert node.last_contact == CARD
    assert payload["message"]["contact"] == CARD
    assert payload["message"]["text"] == "meet carol"


def test_dm_send_contact_without_text_defaults_to_card_line(bridge, node):
    from tau.blocklist import BlockList

    node.blocklist = BlockList()
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "contact": dict(CARD)},
    )
    assert status == 200
    assert node.sent[-1][1] == "👤 Shared contact: carol (cccccccc…)"
    assert payload["message"]["text"] == "👤 Shared contact: carol (cccccccc…)"
    assert payload["message"]["contact"] == CARD


def test_dm_send_with_bad_contact_is_400(bridge, node):
    from tau.blocklist import BlockList

    node.blocklist = BlockList()
    for bad in (
        {"name": "carol", "destination_hash": "zz"},
        {"name": "carol"},
        "carol",
    ):
        status, _, payload = _json(
            bridge,
            "/api/messages",
            method="POST",
            body={"to": "alice", "text": "meet carol", "contact": bad},
        )
        assert status == 400
        assert "contact" in payload["error"]["message"]
    assert node.sent == []


def test_dm_send_contact_for_blocked_peer_is_400(bridge, node):
    from tau.blocklist import BlockList

    node.blocklist = BlockList(blocked=frozenset({"cc" * 16}))
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "meet carol", "contact": dict(CARD)},
    )
    assert status == 400
    assert "blocked peer" in payload["error"]["message"]
    assert node.sent == []


def test_dm_send_location_without_text_defaults_to_place_name(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "location": dict(PIN)},
    )
    assert status == 200
    assert node.sent[-1][1] == "Rock Springs Park"
    assert payload["message"]["text"] == "Rock Springs Park"
    assert payload["message"]["location"] == PIN


def test_dm_send_location_without_text_or_name_defaults_to_coordinates(
    bridge, node
):
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "location": {"lat": 34.05, "lon": -83.88}},
    )
    assert status == 200
    assert node.sent[-1][1] == "📍 34.05, -83.88"
    assert payload["message"]["location"] == {"lat": 34.05, "lon": -83.88}


def test_dm_send_with_bad_location_is_400(bridge, node):
    for bad in ({"lat": 91, "lon": 0}, {"lat": 34.05}, "34.05,-83.88"):
        status, _, payload = _json(
            bridge,
            "/api/messages",
            method="POST",
            body={"to": "alice", "text": "meet here", "location": bad},
        )
        assert status == 400
        assert "latitude" in payload["error"]["message"]
    assert node.sent == []


def test_group_send_with_location_pins_place(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": "crew", "text": "meet here", "location": dict(PIN)},
    )
    assert status == 200
    assert node.last_group_location == PIN
    assert payload["messages"]
    assert all(m["location"] == PIN for m in payload["messages"])


def test_group_send_with_contact_shares_card(bridge, node):
    from tau.blocklist import BlockList

    node.blocklist = BlockList()
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"group": "crew", "text": "meet carol", "contact": dict(CARD)},
    )
    assert status == 200
    assert node.last_group_contact == CARD
    assert payload["messages"]
    assert all(m["contact"] == CARD for m in payload["messages"])


def test_inbound_contact_card_is_recorded(bridge, node):
    card_message = {
        "from": "alice",
        "destination": PEER_HASH,
        "text": "👤 Shared contact: carol (cccccccc…)",
        "kind": "contact",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "wire-card",
        "contact": dict(CARD),
    }
    node.inbox.append(dict(card_message))
    bridge.record_inbound(dict(card_message), publish=False)
    entry = next(
        entry for entry in bridge.messages() if entry.get("wire_id") == "wire-card"
    )
    assert entry["contact"] == CARD


def test_inbound_location_pin_is_recorded(bridge, node):
    pin = {
        "from": "alice",
        "destination": PEER_HASH,
        "text": "meet here",
        "kind": "chat",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "wire-pin",
        "location": dict(PIN),
    }
    node.inbox.append(dict(pin))
    bridge.record_inbound(dict(pin), publish=False)
    entry = next(
        entry for entry in bridge.messages() if entry.get("wire_id") == "wire-pin"
    )
    assert entry["location"] == PIN
    assert entry["text"] == "meet here"


def test_dm_poll_vote_endpoint_votes_by_number_and_text(bridge, node):
    _seed_dm_poll(node, bridge)
    status, _, payload = _json(
        bridge,
        "/api/dm/vote",
        method="POST",
        body={"to": "alice", "poll": "wire-dm-poll", "option": 2},
    )
    assert status == 200
    assert node.sent[-1][1] == "voted: Coffee"
    assert node.last_poll_vote == {
        "target": {"author": PEER_HASH, "sent_at": 100.0},
        "option": 1,
    }
    status, _, payload = _json(
        bridge,
        "/api/dm/vote",
        method="POST",
        body={"to": "alice", "poll": "wire-dm-p", "option": "Tea"},
    )
    assert status == 200
    assert node.last_poll_vote["option"] == 0


def test_dm_poll_vote_endpoint_rejects_bad_option(bridge, node):
    _seed_dm_poll(node, bridge)
    status, _, payload = _json(
        bridge,
        "/api/dm/vote",
        method="POST",
        body={"to": "alice", "poll": "wire-dm-poll", "option": 3},
    )
    assert status == 400
    assert "option must be" in payload["error"]["message"]
    status, _, payload = _json(
        bridge,
        "/api/dm/vote",
        method="POST",
        body={"to": "alice", "poll": "wire-dm-nope", "option": 1},
    )
    assert status == 400
    assert "no poll" in payload["error"]["message"]


def test_dm_poll_vote_endpoint_refuses_closed_poll(bridge, node):
    _seed_dm_poll(node, bridge, close=True)
    status, _, payload = _json(
        bridge,
        "/api/dm/vote",
        method="POST",
        body={"to": "alice", "poll": "wire-dm-poll", "option": 1},
    )
    assert status == 400
    assert "closed" in payload["error"]["message"]


def test_dm_poll_close_endpoint_requires_creator(bridge, node):
    _seed_dm_poll(node, bridge)
    status, _, payload = _json(
        bridge,
        "/api/dm/close-poll",
        method="POST",
        body={"to": "alice", "poll": "wire-dm-poll"},
    )
    assert status == 400
    assert "only the poll's creator" in payload["error"]["message"]


def test_dm_poll_close_endpoint_closes_own_poll(bridge, node):
    poll = {
        "from": "tester",
        "destination": PEER_HASH,
        "text": "Tea or coffee?",
        "kind": "chat",
        "direction": "out",
        "leg_sent_at": 100.0,
        "sent_at": 101.0,
        "received_at": 101.0,
        "id": "wire-dm-own-poll",
        "poll_options": ["Tea", "Coffee"],
    }
    node.inbox.append(dict(poll))
    status, _, payload = _json(
        bridge,
        "/api/dm/close-poll",
        method="POST",
        body={"to": "alice", "poll": "wire-dm-own-poll"},
    )
    assert status == 200
    assert node.sent[-1][1] == "closed the poll"
    assert node.last_poll_close == {
        "author": node.destination_hexhash,
        "sent_at": 100.0,
    }


def test_react_endpoint_group_gone_is_a_404(bridge, node):
    group_id = node.groups.list_groups()[0]["id"]
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "crew hi",
            "kind": "group",
            "group": group_id,
            "group_name": "crew",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-g",
        }
    )
    assert node.groups.delete(group_id) is True
    status, _, payload = _json(
        bridge,
        "/api/react",
        method="POST",
        body={"target": "wire-g", "reaction": "👍"},
    )
    assert status == 404
    assert "unknown group" in payload["error"]["message"]


def test_react_accepts_oversized_timeout(bridge, node):
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )

    def send_reaction(peer, target, token, *, timeout=20.0, discover_timeout=None):
        return {"ok": True, "reaction": token, "target": target, "id": "react-1"}

    node.send_reaction = send_reaction
    status, _, payload = _json(
        bridge,
        "/api/react",
        method="POST",
        body={"target": "wire-1", "reaction": "👍", "timeout": 10**400},
    )
    assert status == 200
    assert payload["reaction"] == "👍"

    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )
    status, _, payload = _json(
        bridge,
        "/api/react",
        method="POST",
        body={"target": "wire-1", "reaction": "x" * 33},
    )
    assert status == 400
    assert "longer than 32" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/react",
        method="POST",
        body={"target": "wire-1", "reaction": "👍", "clear": True},
    )
    assert status == 400


def test_delete_message_endpoint_removes_history_and_inbox_entry(bridge, node):
    node.inbox.append(
        {
            "id": "wire-1",
            "destination": PEER_HASH,
            "from": "alice",
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
        }
    )
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )
    status, _, payload = _json(
        bridge, "/api/messages/delete", method="POST", body={"id": 1}
    )
    assert status == 200
    assert payload == {"ok": True, "deleted": {"id": 1, "wire_id": "wire-1"}}
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []
    assert node.inbox.export_document()["messages"] == []
    # The bus stamps its own sequence into `id`, so the deleted
    # console id must ride under `message_id`: other tabs drop the
    # deleted bubble, not whichever one shares the event sequence.
    deleted_events = [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_deleted"
    ]
    assert len(deleted_events) == 1
    assert deleted_events[0]["message_id"] == 1


def test_delete_by_wire_id_never_touches_a_different_inbox_record(bridge, node):
    # The retract handler already deleted the exact inbox record; a
    # console-only removal must not re-delete by wire id, or a prefix
    # match would drop another sender's message sharing the prefix.
    node.inbox.append(
        {
            "id": "msg-123",
            "destination": PEER_HASH,
            "from": "alice",
            "text": "another sender's message",
            "sent_at": 100.0,
            "received_at": 101.0,
        }
    )
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "take this back",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "msg-1",
        }
    )
    result = bridge.delete_by_wire_id("msg-1")
    assert result == {"ok": True, "deleted": {"id": 1, "wire_id": "msg-1"}}
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []
    assert [entry["id"] for entry in node.inbox.list_messages()] == ["msg-123"]


def test_retract_before_delivery_does_not_resurrect(bridge):
    # The retraction landed while the delivery callback was still in
    # flight: no console entry existed to remove, but the id is
    # remembered, so the late append is refused.
    assert bridge.delete_by_wire_id("wire-1") is None
    assert bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "take this back",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    ) is None
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []


def test_delete_message_endpoint_removes_console_only_entry(bridge):
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            # No wire id: an entry from before ids existed, or one the
            # node inbox never stored. The console copy still goes.
        }
    )
    status, _, payload = _json(
        bridge, "/api/messages/delete", method="POST", body={"id": 1}
    )
    assert status == 200
    assert payload == {"ok": True, "deleted": {"id": 1, "wire_id": None}}
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []


def test_delete_message_endpoint_rejects_unknown_and_bad_ids(bridge):
    status, _, payload = _json(
        bridge, "/api/messages/delete", method="POST", body={"id": 7}
    )
    assert status == 400
    assert "unknown message" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/messages/delete", method="POST", body={}
    )
    assert status == 400
    assert "field 'id'" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/messages/delete", method="POST", body={"id": "1"}
    )
    assert status == 400
    assert "field 'id'" in payload["error"]["message"]


def _record_dm_request(bridge, *, request_id="req-1", sender=PEER_HASH, now=None):
    from tau.dm_requests import DM_REQUEST_TTL, DmRequest, record_dm_request

    moment = time.time() if now is None else now
    record_dm_request(
        bridge.node.config.dm_requests_path,
        DmRequest(
            id=request_id,
            sender=sender,
            text="hello, let me in",
            kind="message",
            sent_at=moment - 1,
            received_at=moment,
            request_expires_at=moment + DM_REQUEST_TTL,
        ),
        now=moment,
    )


def test_dm_requests_endpoint_lists_held_requests(bridge):
    status, _, payload = _json(bridge, "/api/messages/requests")
    assert status == 200
    assert payload == {"requests": [], "count": 0}

    _record_dm_request(bridge, request_id="req-1", sender=PEER_HASH)
    _record_dm_request(bridge, request_id="req-2", sender=OTHER_HASH)
    status, _, payload = _json(bridge, "/api/messages/requests")
    assert status == 200
    assert payload["count"] == 2
    assert [entry["id"] for entry in payload["requests"]] == ["req-1", "req-2"]
    # The sender's display name comes from the node's registry; an
    # unknown peer falls back to the short hash.
    assert payload["requests"][0]["sender_name"] == "alice"
    assert payload["requests"][1]["sender_name"] == OTHER_HASH[:8]


def _record_join_request(bridge, *, group="crew", peer=PEER_HASH, now=None):
    from tau.group_invites import record_join_request

    moment = time.time() if now is None else now
    return record_join_request(
        bridge.node.config.join_requests_path, group, peer, now=moment
    )


def test_join_requests_endpoint_lists_pending_requests(bridge, node):
    status, _, payload = _json(bridge, "/api/groups/join-requests")
    assert status == 200
    assert payload == {"requests": [], "count": 0}

    _record_join_request(bridge, group="crew", peer=PEER_HASH)
    _record_join_request(bridge, group="crew", peer=OTHER_HASH)
    status, _, payload = _json(bridge, "/api/groups/join-requests")
    assert status == 200
    assert payload["count"] == 2
    assert [entry["peer"] for entry in payload["requests"]] == [
        PEER_HASH,
        OTHER_HASH,
    ]
    # The requester's display name comes from the node's registry; an
    # unknown peer falls back to the short hash.
    assert payload["requests"][0]["peer_name"] == "alice"
    assert payload["requests"][1]["peer_name"] == OTHER_HASH[:8]
    assert payload["requests"][0]["group"] == "crew"


def test_approve_join_request_endpoint_admits_peer(bridge, node):
    node.groups.create("team", [BOB_HASH, PEER_HASH], created_by=BOB_HASH)
    _record_join_request(bridge, group="team", peer=OTHER_HASH)
    status, _, payload = _json(
        bridge,
        "/api/groups/approve-join",
        method="POST",
        body={"group": "team", "peer": OTHER_HASH},
    )
    assert status == 200
    assert payload["status"] == "added"
    assert OTHER_HASH in payload["group"]["members"]
    _, _, listed = _json(bridge, "/api/groups/join-requests")
    assert listed == {"requests": [], "count": 0}
    # The welcome group message went out to the new roster.
    assert node.group_sent


def test_deny_join_request_endpoint_refuses_peer(bridge, node):
    node.groups.create("team", [BOB_HASH, PEER_HASH], created_by=BOB_HASH)
    _record_join_request(bridge, group="team", peer=OTHER_HASH)
    status, _, payload = _json(
        bridge,
        "/api/groups/deny-join",
        method="POST",
        body={"group": "team", "peer": OTHER_HASH},
    )
    assert status == 200
    assert payload["status"] == "denied"
    _, _, listed = _json(bridge, "/api/groups/join-requests")
    assert listed == {"requests": [], "count": 0}
    assert node.group_sent == []


def _seed_own_identity(node):
    """Make the fixture node's disk identity its real identity.

    The bridge mints with the identity loaded from disk, and the
    token's creator must be this node's destination hash — so the
    fixture's hash follows the identity, and groups it creates are
    created by that hash.
    """
    from tau.identity import destination_hash, save_identity

    save_identity(node.identity, node.config.identity_path)
    node.destination_hexhash = destination_hash(node.identity).hex()
    return node.destination_hexhash


def _own_group(node, name="team"):
    own_hash = _seed_own_identity(node)
    node.groups.create(name, [PEER_HASH, own_hash], created_by=own_hash)
    return own_hash


def test_group_invite_status_endpoint_reports_no_link_before_mint(bridge, node):
    _own_group(node)
    status, _, payload = _json(bridge, "/api/groups/invite?group=team")
    assert status == 200
    assert payload["group"]["name"] == "team"
    assert payload["mode"] == "open"
    assert payload["generation"] == 0
    assert payload["link"] is None
    # A group this node did not create is not its link to manage.
    status, _, payload = _json(bridge, "/api/groups/invite?group=crew")
    assert status == 400
    assert "only the creator" in payload["error"]["message"]
    status, _, payload = _json(bridge, "/api/groups/invite?group=nope")
    assert status == 400
    assert "unknown group" in payload["error"]["message"]
    status, _, payload = _json(bridge, "/api/groups/invite")
    assert status == 400
    assert "field 'group'" in payload["error"]["message"]


def test_group_invite_endpoint_mints_a_verifiable_link(bridge, node):
    from tau.group_invites import (
        load_invites,
        parse_invite_link,
        verify_invite_link,
    )

    own_hash = _own_group(node)
    status, _, payload = _json(
        bridge, "/api/groups/invite", method="POST", body={"group": "team"}
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["mode"] == "open"
    assert payload["generation"] == 0
    link = payload["link"]
    assert link.startswith("tau://invite/")
    # The minted link is sealed: parsing it shows only the outer
    # envelope — the creator hash plus an opaque blob — never the
    # group name.  Verification still opens it.
    token = parse_invite_link(link)
    assert token["v"] == 2
    assert token["creator"] == own_hash
    assert "group" not in token
    assert (
        verify_invite_link(
            link,
            identity=node.identity,
            invites=load_invites(node.config.invites_path),
        )["mode"]
        == "open"
    )
    # Minting again with a mode switches the stored mode without
    # bumping the generation.
    status, _, payload = _json(
        bridge,
        "/api/groups/invite",
        method="POST",
        body={"group": "team", "mode": "closed"},
    )
    assert status == 200
    assert payload["mode"] == "closed"
    assert payload["generation"] == 0
    assert (
        verify_invite_link(
            payload["link"],
            identity=node.identity,
            invites=load_invites(node.config.invites_path),
        )["mode"]
        == "closed"
    )
    # The status endpoint now shows the current link.
    _, _, listed = _json(bridge, "/api/groups/invite?group=team")
    assert listed["link"] == payload["link"]
    assert listed["mode"] == "closed"
    # Bad mode and non-creator groups are refused.
    status, _, payload = _json(
        bridge,
        "/api/groups/invite",
        method="POST",
        body={"group": "team", "mode": "wide-open"},
    )
    assert status == 400
    status, _, payload = _json(
        bridge,
        "/api/groups/invite",
        method="POST",
        body={"group": "crew"},
    )
    assert status == 400
    assert "only the creator" in payload["error"]["message"]
    status, _, payload = _json(bridge, "/api/groups/invite", method="POST", body={})
    assert status == 400
    assert "field 'group'" in payload["error"]["message"]


def test_group_invite_revoke_endpoint_drops_link(bridge, node):
    from tau.group_invites import InviteError, load_invites, verify_invite_link

    _own_group(node)
    _, _, minted = _json(
        bridge, "/api/groups/invite", method="POST", body={"group": "team"}
    )
    link = minted["link"]
    status, _, payload = _json(
        bridge, "/api/groups/invite/revoke", method="POST", body={"group": "team"}
    )
    assert status == 200
    assert payload["generation"] == 1
    _, _, listed = _json(bridge, "/api/groups/invite?group=team")
    assert listed["link"] is None
    assert listed["generation"] == 1
    with pytest.raises(InviteError):
        verify_invite_link(
            link,
            identity=node.identity,
            invites=load_invites(node.config.invites_path),
        )
    status, _, payload = _json(
        bridge, "/api/groups/invite/revoke", method="POST", body={"group": "crew"}
    )
    assert status == 400
    assert "only the creator" in payload["error"]["message"]


def test_group_join_endpoint_joins_from_link(bridge, node, monkeypatch, tmp_path):
    from tau.group_invites import mint_invite
    from tau.identity import destination_hash
    from tau.protocol import derive_group_id

    creator = RNS.Identity()
    creator_hash = destination_hash(creator).hex()
    own_hash = _seed_own_identity(node)
    link = mint_invite(
        tmp_path / "invites.json",
        creator,
        group_name="remote-crew",
        creator_hash=creator_hash,
        mode="open",
    )
    members = [creator_hash, own_hash]
    group_id = derive_group_id(creator_hash, "remote-crew", members)
    reply_group = {
        "id": group_id,
        "name": "remote-crew",
        "members": members,
        "creator": creator_hash,
    }
    sent = {}
    replies = {
        "reply": {"ok": True, "status": "added", "group": dict(reply_group)}
    }

    def fake_send(target, text, **kwargs):
        sent["target"] = target
        sent["text"] = text
        sent["kind"] = kwargs.get("kind")
        return dict(replies["reply"])

    monkeypatch.setattr(node, "send_message", fake_send)
    status, _, payload = _json(
        bridge, "/api/groups/join", method="POST", body={"link": link}
    )
    assert status == 200
    assert payload["status"] == "added"
    assert payload["group"]["id"] == group_id
    assert own_hash in [member["hash"] for member in payload["group"]["members"]]
    # The link went to the creator as a join request, and the roster
    # from the reply is pinned locally.
    assert sent["target"] == creator_hash
    assert sent["text"] == link
    assert sent["kind"] == "group-join-request"
    assert node.groups.get(group_id) is not None
    # A closed group's reply leaves the request pending.
    replies["reply"] = {"ok": True, "status": "pending", "group": dict(reply_group)}
    status, _, payload = _json(
        bridge, "/api/groups/join", method="POST", body={"link": link}
    )
    assert status == 200
    assert payload["status"] == "pending"
    assert payload["group"]["id"] == group_id
    # An unreachable creator queues the request.  The sealed link
    # names nothing in the clear, so the queued reply carries no
    # group label — the joiner learns the group from the creator's
    # reply, not the link.
    replies["reply"] = {"queued": True, "id": "mail-new-1"}
    status, _, payload = _json(
        bridge, "/api/groups/join", method="POST", body={"link": link}
    )
    assert status == 200
    assert payload["status"] == "queued"
    assert payload["group"] == ""
    # An error reply and malformed links are refused.
    replies["reply"] = {
        "ok": False,
        "error": {"message": "invite link is not valid"},
    }
    status, _, payload = _json(
        bridge, "/api/groups/join", method="POST", body={"link": link}
    )
    assert status == 400
    assert "join request failed" in payload["error"]["message"]
    status, _, payload = _json(
        bridge, "/api/groups/join", method="POST", body={"link": "garbage"}
    )
    assert status == 400
    assert "not an invite link" in payload["error"]["message"]
    status, _, payload = _json(bridge, "/api/groups/join", method="POST", body={})
    assert status == 400
    assert "field 'link'" in payload["error"]["message"]


def test_group_join_endpoint_queued_sealed_link_reports_no_group(
    bridge, node, monkeypatch, tmp_path
):
    from tau.group_invites import mint_invite
    from tau.identity import destination_hash

    creator = RNS.Identity()
    creator_hash = destination_hash(creator).hex()
    own_hash = _seed_own_identity(node)
    # A sealed id-keyed link for a secret group: the queued reply
    # reports no group — the link names nothing in the clear — and
    # must not raise on the missing group name.
    link = mint_invite(
        tmp_path / "invites.json",
        creator,
        group_name="remote-crew",
        creator_hash=creator_hash,
        mode="open",
        group_id="cd" * 16,
        group_created_at=1000.0,
    )

    def fake_send(target, text, **kwargs):
        return {"queued": True, "id": "mail-new-1"}

    monkeypatch.setattr(node, "send_message", fake_send)
    status, _, payload = _json(
        bridge, "/api/groups/join", method="POST", body={"link": link}
    )
    assert status == 200
    assert payload["status"] == "queued"
    assert payload["group"] == ""


def test_join_request_endpoints_validate_fields(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/groups/approve-join",
        method="POST",
        body={},
    )
    assert status == 400
    assert "field 'group'" in payload["error"]["message"]
    status, _, payload = _json(
        bridge,
        "/api/groups/approve-join",
        method="POST",
        body={"group": "team"},
    )
    assert status == 400
    assert "field 'peer'" in payload["error"]["message"]
    status, _, payload = _json(
        bridge,
        "/api/groups/approve-join",
        method="POST",
        body={"group": "nope", "peer": OTHER_HASH},
    )
    assert status == 400
    assert "unknown group" in payload["error"]["message"]


def test_accept_dm_request_endpoint_delivers_and_allowlists(bridge, node):
    save_allowlist(
        node.config.allowlist_path,
        AllowList(enabled=True, allow=frozenset()),
    )
    _record_dm_request(bridge, request_id="req-1", sender=PEER_HASH)
    status, _, payload = _json(
        bridge,
        "/api/messages/requests/accept",
        method="POST",
        body={"id": "req-1"},
    )
    assert status == 200
    assert payload["accepted"]["id"] == "req-1"
    assert payload["delivered"] is True
    assert payload["allowlisted"] is True
    assert payload["skipped"] is None
    # The sender is allow-listed, the request is gone, the message is
    # in the inbox and mirrored into the console feed.
    assert PEER_HASH in load_allowlist(node.config.allowlist_path).allow
    _, _, listed = _json(bridge, "/api/messages/requests")
    assert listed == {"requests": [], "count": 0}
    assert [entry["id"] for entry in node.inbox.list_messages()] == ["req-1"]
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["wire_id"] for entry in feed["messages"]] == ["req-1"]
    assert feed["messages"][0]["text"] == "hello, let me in"


def test_accept_dm_request_endpoint_rejects_unknown_and_blocked(bridge, node):
    status, _, payload = _json(
        bridge,
        "/api/messages/requests/accept",
        method="POST",
        body={"id": "nope"},
    )
    assert status == 400
    assert "no DM request" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/messages/requests/accept",
        method="POST",
        body={},
    )
    assert status == 400
    assert "field 'id'" in payload["error"]["message"]

    _record_dm_request(bridge, request_id="req-1", sender=PEER_HASH)
    save_blocklist(
        node.config.blocklist_path, BlockList(blocked=frozenset({PEER_HASH}))
    )
    status, _, payload = _json(
        bridge,
        "/api/messages/requests/accept",
        method="POST",
        body={"id": "req-1"},
    )
    assert status == 400
    assert "blocked" in payload["error"]["message"]
    # Nothing was allow-listed or delivered. The request stays in the
    # store, but the console list hides blocked senders, like the rest
    # of the console hides their content.
    _, _, listed = _json(bridge, "/api/messages/requests")
    assert listed["count"] == 0
    from tau.dm_requests import list_dm_requests

    assert len(list_dm_requests(node.config.dm_requests_path)) == 1
    assert node.inbox.list_messages() == []


def test_decline_dm_request_endpoint_drops_request(bridge, node):
    save_allowlist(
        node.config.allowlist_path,
        AllowList(enabled=True, allow=frozenset()),
    )
    _record_dm_request(bridge, request_id="req-1", sender=PEER_HASH)
    status, _, payload = _json(
        bridge,
        "/api/messages/requests/decline",
        method="POST",
        body={"id": "req-1"},
    )
    assert status == 200
    assert payload["declined"]["id"] == "req-1"
    _, _, listed = _json(bridge, "/api/messages/requests")
    assert listed == {"requests": [], "count": 0}
    assert PEER_HASH not in load_allowlist(node.config.allowlist_path).allow
    assert node.inbox.list_messages() == []

    status, _, payload = _json(
        bridge,
        "/api/messages/requests/decline",
        method="POST",
        body={"id": "req-1"},
    )
    assert status == 400
    assert "no DM request" in payload["error"]["message"]


def test_decline_dm_request_endpoint_with_block_blocks_sender(bridge, node):
    save_allowlist(
        node.config.allowlist_path,
        AllowList(enabled=True, allow=frozenset()),
    )
    _record_dm_request(bridge, request_id="req-1", sender=PEER_HASH)
    status, _, payload = _json(
        bridge,
        "/api/messages/requests/decline",
        method="POST",
        body={"id": "req-1", "block": True},
    )
    assert status == 200
    assert payload["declined"]["id"] == "req-1"
    assert payload["blocked"] is True
    _, _, listed = _json(bridge, "/api/messages/requests")
    assert listed == {"requests": [], "count": 0}
    assert PEER_HASH in load_blocklist(node.config.blocklist_path).blocked


def test_decline_dm_request_endpoint_non_bool_block_is_400(bridge, node):
    _record_dm_request(bridge, request_id="req-1", sender=PEER_HASH)
    status, _, payload = _json(
        bridge,
        "/api/messages/requests/decline",
        method="POST",
        body={"id": "req-1", "block": "yes"},
    )
    assert status == 400
    assert "block" in payload["error"]["message"]
    # The request is still held: a bad flag declines nothing.
    _, _, listed = _json(bridge, "/api/messages/requests")
    assert listed["count"] == 1


def test_dm_requests_endpoint_hides_blocked_senders(bridge, node):
    # The console list hides blocked senders' requests, like the rest
    # of the console hides their content; the CLI can still decline
    # them, and the store keeps them.
    _record_dm_request(bridge, request_id="req-1", sender=PEER_HASH)
    _record_dm_request(bridge, request_id="req-2", sender=OTHER_HASH)
    save_blocklist(
        node.config.blocklist_path, BlockList(blocked=frozenset({PEER_HASH}))
    )
    status, _, payload = _json(bridge, "/api/messages/requests")
    assert status == 200
    assert payload["count"] == 1
    assert [entry["id"] for entry in payload["requests"]] == ["req-2"]
    from tau.dm_requests import list_dm_requests

    assert len(list_dm_requests(node.config.dm_requests_path)) == 2


def test_accept_dm_request_endpoint_store_failure_is_503(
    bridge, node, monkeypatch
):
    # A store write that fails during acceptance is a 503, not a 500
    # crash: the console reports the failed write instead of a server
    # error page.
    import tau.dm_requests as dm_requests_mod

    save_allowlist(
        node.config.allowlist_path,
        AllowList(enabled=True, allow=frozenset()),
    )
    _record_dm_request(bridge, request_id="req-1", sender=PEER_HASH)

    def failing_save(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(dm_requests_mod, "save_dm_requests", failing_save)
    status, _, payload = _json(
        bridge,
        "/api/messages/requests/accept",
        method="POST",
        body={"id": "req-1"},
    )
    assert status == 503
    assert "disk full" in payload["error"]["message"]


def test_decline_dm_request_endpoint_store_failure_is_503(
    bridge, node, monkeypatch
):
    import tau.dm_requests as dm_requests_mod

    _record_dm_request(bridge, request_id="req-1", sender=PEER_HASH)

    def failing_save(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(dm_requests_mod, "save_dm_requests", failing_save)
    status, _, payload = _json(
        bridge,
        "/api/messages/requests/decline",
        method="POST",
        body={"id": "req-1"},
    )
    assert status == 503
    assert "disk full" in payload["error"]["message"]


def test_accept_dm_request_endpoint_without_inbox_is_503(bridge, node):
    # A missing inbox is a missing capability, not rate limiting: the
    # endpoint answers 503 instead of the top-level 429.
    node.inbox = None
    _record_dm_request(bridge, request_id="req-1", sender=PEER_HASH)
    status, _, payload = _json(
        bridge,
        "/api/messages/requests/accept",
        method="POST",
        body={"id": "req-1"},
    )
    assert status == 503
    assert "no message inbox" in payload["error"]["message"]


def test_mirror_rejects_removed_wire_id_when_asked(bridge):
    # A retraction that landed after a record was stored must not be
    # mirrored back into the feed: the guarded mirror refuses a wire
    # id the console already removed, like the live inbound path.
    record = {
        "id": "wire-1",
        "destination": PEER_HASH,
        "from": "alice",
        "text": "take this back",
        "kind": "message",
        "sent_at": 100.0,
        "received_at": 101.0,
        "direction": "in",
    }
    with bridge._lock:
        bridge._remember_removed_wire_id("wire-1")
    bridge._mirror_imported_messages([record], reject_removed_wire_id=True)
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []
    # The import path does not ask for the guard: a restore mirrors
    # the record regardless, as before.
    bridge._mirror_imported_messages([record])
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["wire_id"] for entry in feed["messages"]] == ["wire-1"]


def _sent_console_entry(bridge, node, *, wire_id="wire-1", **overrides):
    node.inbox.append(
        {
            "id": wire_id,
            "destination": PEER_HASH,
            "direction": "out",
            "from": "tester",
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
        }
    )
    node.mailbox.record_sent(
        wire_id, to=bytes.fromhex(PEER_HASH), text="hello", sent_at=100.0
    )
    entry = bridge._append_message(
        **{
            "direction": "out",
            "peer": PEER_HASH,
            "peer_name": "alice",
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            "status": "delivered",
            "wire_id": wire_id,
            **overrides,
        }
    )
    assert entry is not None
    return entry


def test_retract_message_endpoint_retracts_and_removes_local_copies(
    bridge, node
):
    _sent_console_entry(bridge, node)
    sent = []

    def send_retraction(peer, message_id, *, timeout=20.0, discover_timeout=None):
        sent.append((peer, message_id, timeout))
        return {"ok": True, "deleted": True, "id": "retract-1"}

    node.send_retraction = send_retraction
    status, _, payload = _json(
        bridge, "/api/messages/retract", method="POST", body={"id": 1}
    )
    assert status == 200
    assert payload == {
        "ok": True,
        "retracted": {"id": 1, "wire_id": "wire-1"},
        "deleted": True,
        "id": "retract-1",
    }
    assert sent == [(PEER_HASH, "wire-1", 20.0)]
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []
    assert node.inbox.export_document()["messages"] == []
    assert node.mailbox.sent_record("wire-1") is None
    retracted_events = [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_deleted"
    ]
    assert len(retracted_events) == 1
    assert retracted_events[0]["message_id"] == 1


def test_retract_message_endpoint_queues_for_offline_peer(bridge, node):
    _sent_console_entry(bridge, node)

    def send_retraction(peer, message_id, *, timeout=20.0, discover_timeout=None):
        return {
            "ok": False,
            "queued": True,
            "target": message_id,
            "id": "retract-1",
            "queued_id": "q-1",
        }

    node.send_retraction = send_retraction
    status, _, payload = _json(
        bridge, "/api/messages/retract", method="POST", body={"id": 1}
    )
    assert status == 200
    assert payload == {
        "ok": True,
        "queued": True,
        "retracted": {"id": 1, "wire_id": "wire-1"},
        "id": "retract-1",
        "queued_id": "q-1",
    }
    # The queued retract leaves the local copies in place until the
    # sweep delivers it: the bubble stays for now.
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["id"] for entry in feed["messages"]] == [1]
    assert node.inbox.export_document()["messages"] != []


def test_retract_message_endpoint_reports_peer_without_a_copy(bridge, node):
    _sent_console_entry(bridge, node)

    def send_retraction(peer, message_id, *, timeout=20.0, discover_timeout=None):
        return {"ok": True, "deleted": False, "id": "retract-1"}

    node.send_retraction = send_retraction
    status, _, payload = _json(
        bridge, "/api/messages/retract", method="POST", body={"id": 1}
    )
    assert status == 200
    assert payload["deleted"] is False
    # The peer had no copy, but the local copies still go.
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []
    assert node.inbox.export_document()["messages"] == []


def _sent_group_console_entries(
    bridge,
    node,
    group_id,
    *,
    sent_at=1000.0,
    second_status="delivered",
    second_wire_id="wire-bob",
    leg_sent_at=None,
    omit_leg_sent_at=False,
):
    # One inbox record for the whole send, like TauNode._record_sent:
    # its id is the first leg id, the rest in leg_ids.  ``leg_sent_at``
    # defaults to the entries' time; pass it explicitly to model an
    # entry whose time drifted from the stored one, or omit it to
    # model a send from before group retracts landed.
    if leg_sent_at is None:
        leg_sent_at = sent_at
    record = {
        "id": "wire-alice",
        "leg_ids": ["wire-alice", second_wire_id],
        "leg_sent_at": leg_sent_at,
        "destination": "",
        "direction": "out",
        "from": "tester",
        "text": "hello",
        "kind": "group",
        "group": group_id,
        "group_name": "crew",
        "sent_at": sent_at,
        "received_at": sent_at + 1.0,
    }
    if omit_leg_sent_at:
        del record["leg_sent_at"]
    node.inbox.append(record)
    node.mailbox.record_sent(
        "wire-alice", to=bytes.fromhex(PEER_HASH), text="hello", sent_at=sent_at
    )
    node.mailbox.record_sent(
        second_wire_id,
        to=bytes.fromhex(BOB_HASH),
        text="hello",
        sent_at=sent_at,
    )
    entries = []
    for member, name, wire_id, status in (
        (PEER_HASH, "alice", "wire-alice", "delivered"),
        (BOB_HASH, "bob", second_wire_id, second_status),
    ):
        entries.append(
            bridge._append_message(
                direction="out",
                peer=member,
                peer_name=name,
                text="hello",
                sent_at=sent_at,
                received_at=sent_at + 1.0,
                status=status,
                wire_id=wire_id,
                group=group_id,
                group_name="crew",
            )
        )
    return entries


def test_retract_message_endpoint_retracts_group_message(bridge, node):
    crew = node.groups.list_groups()[0]
    entries = _sent_group_console_entries(bridge, node, crew["id"])
    status, _, payload = _json(
        bridge,
        "/api/messages/retract",
        method="POST",
        body={"id": entries[0]["id"]},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["retracted"] == {
        "id": entries[0]["id"],
        "wire_id": "wire-alice",
    }
    assert payload["retracted_ids"] == [entry["id"] for entry in entries]
    assert payload["deleted"] is True
    assert payload["group"] == crew["id"]
    assert payload["cancelled_queued"] == []
    assert [result["to"] for result in payload["results"]] == [
        PEER_HASH,
        BOB_HASH,
    ]
    assert node.group_retractions == [("crew", "wire-alice", 1000.0)]
    # Every leg leaves the feed, the inbox and the sent records.
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []
    assert node.inbox.export_document()["messages"] == []
    assert node.mailbox.sent_record("wire-alice") is None
    assert node.mailbox.sent_record("wire-bob") is None
    retracted_events = [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_deleted"
    ]
    assert [event["message_id"] for event in retracted_events] == [
        entry["id"] for entry in entries
    ]


def test_retract_message_endpoint_retracts_group_message_after_roster_change(
    bridge, node
):
    crew = node.groups.list_groups()[0]
    old_id = crew["id"]
    # A rename retires the id the message was sent under, with the
    # same roster: the retract must still fan out to that roster.
    node.groups.rename(old_id, "road trip", created_by=PEER_HASH)
    assert node.groups.get(old_id) is None
    assert node.groups.get_retired(old_id) is not None
    entries = _sent_group_console_entries(bridge, node, old_id)
    status, _, payload = _json(
        bridge,
        "/api/messages/retract",
        method="POST",
        body={"id": entries[0]["id"]},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["retracted"] == {
        "id": entries[0]["id"],
        "wire_id": "wire-alice",
    }
    assert payload["group"] == old_id
    assert [result["to"] for result in payload["results"]] == [
        PEER_HASH,
        BOB_HASH,
    ]
    assert node.group_retractions == [("crew", "wire-alice", 1000.0)]
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []


def test_retract_message_endpoint_keeps_group_bubbles_on_partial_fanout(
    bridge, node
):
    crew = node.groups.list_groups()[0]
    entries = _sent_group_console_entries(bridge, node, crew["id"])
    node.group_retract_failures = {BOB_HASH: "no route to peer"}
    status, _, payload = _json(
        bridge,
        "/api/messages/retract",
        method="POST",
        body={"id": entries[0]["id"]},
    )
    assert status == 502
    assert "bob" in payload["error"]["message"]
    assert "no route to peer" in payload["error"]["message"]
    assert "retry the retract" in payload["error"]["message"]
    # A failed retract never erases the local copies: both bubbles
    # stay, so the members still holding the message stay retryable.
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["id"] for entry in feed["messages"]] == [
        entry["id"] for entry in entries
    ]
    assert len(node.inbox.export_document()["messages"]) == 1
    assert node.mailbox.sent_record("wire-alice") is not None
    assert node.mailbox.sent_record("wire-bob") is not None


def test_retract_message_endpoint_cancels_queued_group_leg(bridge, node):
    crew = node.groups.list_groups()[0]
    entries = _sent_group_console_entries(
        bridge,
        node,
        crew["id"],
        second_status="queued",
        second_wire_id="mail-bob-1",
    )
    node.group_retract_cancelled = ["mail-bob-1"]
    status, _, payload = _json(
        bridge,
        "/api/messages/retract",
        method="POST",
        body={"id": entries[0]["id"]},
    )
    assert status == 200
    assert payload["cancelled_queued"] == ["mail-bob-1"]
    # The queued leg is cancelled by the retract itself, so its bubble
    # goes with the delivered one.
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []
    assert node.inbox.export_document()["messages"] == []
    assert node.mailbox.sent_record("mail-bob-1") is None


def test_retract_message_endpoint_rejects_unknown_group(bridge, node):
    _sent_console_entry(bridge, node, wire_id="wire-1", group="ab" * 16)
    status, _, payload = _json(
        bridge, "/api/messages/retract", method="POST", body={"id": 1}
    )
    assert status == 400
    assert "unknown group" in payload["error"]["message"]


def test_retract_message_endpoint_rejects_group_message_with_no_wire_id(
    bridge, node
):
    crew = node.groups.list_groups()[0]
    for member, name in ((PEER_HASH, "alice"), (BOB_HASH, "bob")):
        bridge._append_message(
            direction="out",
            peer=member,
            peer_name=name,
            text="hello",
            sent_at=1000.0,
            received_at=1001.0,
            status="failed",
            wire_id=None,
            group=crew["id"],
            group_name="crew",
        )
    status, _, payload = _json(
        bridge, "/api/messages/retract", method="POST", body={"id": 1}
    )
    assert status == 400
    assert "never delivered" in payload["error"]["message"]


def test_retract_message_endpoint_refuses_group_message_with_no_leg_send_time(
    bridge, node
):
    # A console entry restored from before group retracts landed
    # carries its write time, which members cannot match: they would
    # answer delivered with deleted False, and the fan-out would
    # still drop the local copies.  Refuse instead, and keep the
    # bubbles, the inbox record and the sent records.
    crew = node.groups.list_groups()[0]
    entries = _sent_group_console_entries(
        bridge, node, crew["id"], omit_leg_sent_at=True
    )
    status, _, payload = _json(
        bridge,
        "/api/messages/retract",
        method="POST",
        body={"id": entries[0]["id"]},
    )
    assert status == 400
    assert "no leg send time" in payload["error"]["message"]
    assert node.group_retractions == []
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["id"] for entry in feed["messages"]] == [
        entry["id"] for entry in entries
    ]
    assert len(node.inbox.export_document()["messages"]) == 1
    assert node.mailbox.sent_record("wire-alice") is not None
    assert node.mailbox.sent_record("wire-bob") is not None


def test_retract_message_endpoint_uses_stored_leg_send_time(
    bridge, node
):
    # The entry's time drifted from the stored shared time: the
    # fan-out must carry the stored time the members can match,
    # while the cleanup still removes the feed's legs.
    crew = node.groups.list_groups()[0]
    entries = _sent_group_console_entries(
        bridge, node, crew["id"], sent_at=1001.0, leg_sent_at=1000.0
    )
    status, _, payload = _json(
        bridge,
        "/api/messages/retract",
        method="POST",
        body={"id": entries[0]["id"]},
    )
    assert status == 200
    assert payload["ok"] is True
    assert node.group_retractions == [("crew", "wire-alice", 1000.0)]
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []
    assert node.inbox.export_document()["messages"] == []


def test_retract_message_endpoint_refuses_group_message_when_only_inbound_record_matches(
    bridge, node
):
    # The sent copy is pruned, so resolve() answers the leg id with
    # an inbound record instead: its time is not this node's shared
    # send time, so the retract is refused — and the received record
    # is never deleted.
    crew = node.groups.list_groups()[0]
    node.inbox.append(
        {
            "id": "wire-alice",
            "destination": PEER_HASH,
            "direction": "in",
            "from": "alice",
            "text": "hello",
            "kind": "group",
            "group": crew["id"],
            "group_name": "crew",
            "sent_at": 1000.0,
            "received_at": 1001.0,
        }
    )
    entries = []
    for member, name, wire_id in (
        (PEER_HASH, "alice", "wire-alice"),
        (BOB_HASH, "bob", "wire-bob"),
    ):
        entries.append(
            bridge._append_message(
                direction="out",
                peer=member,
                peer_name=name,
                text="hello",
                sent_at=1000.0,
                received_at=1001.0,
                status="delivered",
                wire_id=wire_id,
                group=crew["id"],
                group_name="crew",
            )
        )
    status, _, payload = _json(
        bridge,
        "/api/messages/retract",
        method="POST",
        body={"id": entries[0]["id"]},
    )
    assert status == 400
    assert "no leg send time" in payload["error"]["message"]
    assert node.group_retractions == []
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["id"] for entry in feed["messages"]] == [
        entry["id"] for entry in entries
    ]
    assert len(node.inbox.export_document()["messages"]) == 1


def test_retract_message_endpoint_rejects_inbound_and_queued(bridge, node):
    # A received message is not ours to retract.
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )
    status, _, payload = _json(
        bridge, "/api/messages/retract", method="POST", body={"id": 1}
    )
    assert status == 400
    assert "only your own sent" in payload["error"]["message"]

    # Still queued: cancel it from the outbox instead.
    _sent_console_entry(bridge, node, wire_id="wire-3", status="queued")
    status, _, payload = _json(
        bridge, "/api/messages/retract", method="POST", body={"id": 2}
    )
    assert status == 400
    assert "still queued" in payload["error"]["message"]

    # Delivered on the console but still queued in the mailbox: the
    # outbox check, not just the bubble status, refuses the retract,
    # or a "not found" answer would let the queued copy arrive later.
    _sent_console_entry(bridge, node, wire_id="wire-4")
    node.mailbox.enqueue(
        to=bytes.fromhex(PEER_HASH), text="hello", message_id="wire-4"
    )
    status, _, payload = _json(
        bridge, "/api/messages/retract", method="POST", body={"id": 3}
    )
    assert status == 400
    assert "still queued" in payload["error"]["message"]


def test_edit_message_endpoint_edits_group_message(bridge, node):
    crew = node.groups.list_groups()[0]
    entries = _sent_group_console_entries(bridge, node, crew["id"])
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": entries[0]["id"], "text": "hello again"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["edited"] is True
    assert payload["message"]["id"] == entries[0]["id"]
    assert payload["message"]["text"] == "hello again"
    assert payload["message"]["edited_at"] == 2000.0
    assert [entry["id"] for entry in payload["messages"]] == [
        entry["id"] for entry in entries
    ]
    assert all(
        entry["text"] == "hello again" and entry["edited_at"] == 2000.0
        for entry in payload["messages"]
    )
    assert payload["group"] == crew["id"]
    assert payload["cancelled_queued"] == []
    assert [result["to"] for result in payload["results"]] == [
        PEER_HASH,
        BOB_HASH,
    ]
    assert node.group_edits == [("crew", "wire-alice", 1000.0, "hello again")]
    # Every leg takes the new text with the edited marker.
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["text"] for entry in feed["messages"]] == [
        "hello again",
        "hello again",
    ]
    assert all(
        entry["edited_at"] == 2000.0 for entry in feed["messages"]
    )
    edited_events = [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_updated"
    ]
    assert [event["message"]["id"] for event in edited_events] == [
        entry["id"] for entry in entries
    ]


def test_edit_message_endpoint_edits_group_message_after_roster_change(
    bridge, node
):
    crew = node.groups.list_groups()[0]
    old_id = crew["id"]
    # A rename retires the id the message was sent under, with the
    # same roster: the edit must still fan out to that roster.
    node.groups.rename(old_id, "road trip", created_by=PEER_HASH)
    assert node.groups.get(old_id) is None
    assert node.groups.get_retired(old_id) is not None
    entries = _sent_group_console_entries(bridge, node, old_id)
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": entries[0]["id"], "text": "hello again"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["group"] == old_id
    assert [result["to"] for result in payload["results"]] == [
        PEER_HASH,
        BOB_HASH,
    ]
    assert node.group_edits == [("crew", "wire-alice", 1000.0, "hello again")]
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["text"] for entry in feed["messages"]] == [
        "hello again",
        "hello again",
    ]


def test_edit_message_endpoint_keeps_group_bubbles_on_partial_fanout(
    bridge, node
):
    crew = node.groups.list_groups()[0]
    entries = _sent_group_console_entries(bridge, node, crew["id"])
    node.group_edit_failures = {BOB_HASH: "no route to peer"}
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": entries[0]["id"], "text": "hello again"},
    )
    assert status == 502
    assert "bob" in payload["error"]["message"]
    assert "no route to peer" in payload["error"]["message"]
    assert "retry the edit" in payload["error"]["message"]
    # A failed edit never rewrites the local copies: both bubbles
    # keep the old text, so the members still holding it stay
    # retryable.
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["text"] for entry in feed["messages"]] == [
        "hello",
        "hello",
    ]
    assert all("edited_at" not in entry for entry in feed["messages"])
    assert not [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_updated"
    ]


def test_edit_message_endpoint_reports_queued_group_legs(bridge, node):
    crew = node.groups.list_groups()[0]
    entries = _sent_group_console_entries(bridge, node, crew["id"])

    def send_group_edit(group, message_id, text, **kwargs):
        return {
            "ok": False,
            "group": group["id"] if isinstance(group, dict) else group,
            "target": message_id,
            "id": "edit-1",
            "sent_at": 2000.0,
            "cancelled_queued": [],
            "queued": 1,
            "results": [
                {
                    "to": PEER_HASH,
                    "to_name": "alice",
                    "delivered": True,
                    "edited": True,
                    "id": "edit-1",
                },
                {
                    "to": BOB_HASH,
                    "to_name": "bob",
                    "queued": True,
                    "queued_id": "q-1",
                    "id": "edit-1",
                },
            ],
        }

    node.send_group_edit = send_group_edit
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": entries[0]["id"], "text": "hello again"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["queued"] is True
    assert payload["edited"] is False
    assert payload["group"] == crew["id"]
    # No bubble takes the new text until the last queued leg delivers.
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["text"] for entry in feed["messages"]] == [
        "hello",
        "hello",
    ]
    assert all("edited_at" not in entry for entry in feed["messages"])
    assert not [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_updated"
    ]


def test_edit_message_endpoint_refuses_group_message_with_queued_leg(
    bridge, node
):
    # Group edits are live-only: a queued leg is refused outright
    # instead of cancelled and re-queued — a concurrent mail sweep
    # could deliver the queued leg with the old text after the edit
    # was acknowledged (see Qodo on #341). The user cancels the
    # queued leg from the outbox first, as for a queued DM edit.
    crew = node.groups.list_groups()[0]
    entries = _sent_group_console_entries(
        bridge,
        node,
        crew["id"],
        second_status="queued",
        second_wire_id="mail-bob-1",
    )
    node.mailbox.enqueue(
        to=bytes.fromhex(BOB_HASH),
        text="hello",
        message_id="mail-bob-1",
        group={
            "id": crew["id"],
            "name": "crew",
            "creator": PEER_HASH,
            "members": [PEER_HASH, BOB_HASH],
        },
        sent_at=1000.0,
    )
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": entries[0]["id"], "text": "hello again"},
    )
    assert status == 400
    assert "still queued for an offline member" in payload["error"]["message"]
    assert "cancel it from the outbox" in payload["error"]["message"]
    assert node.group_edits == []
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["text"] for entry in feed["messages"]] == [
        "hello",
        "hello",
    ]
    # The prescribed recovery completes the edit: once the queued leg
    # is cancelled from the outbox, the stale console status must not
    # keep blocking it (see CodeRabbit on #341).
    assert node.mailbox.remove_outbox("mail-bob-1")
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": entries[0]["id"], "text": "hello again"},
    )
    assert status == 200
    assert payload["ok"] is True
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["text"] for entry in feed["messages"]] == [
        "hello again",
        "hello again",
    ]


def test_edit_message_endpoint_refuses_group_message_still_in_the_outbox(
    bridge, node
):
    # The bubbles say delivered, but the mailbox still holds a leg:
    # the outbox check, not just the bubble status, refuses the edit,
    # or a sweep could deliver the old leg after the edit. The check
    # scans the mailbox for the whole logical send (group id plus the
    # shared send time), so it also catches a leg whose console entry
    # left the history (see CodeRabbit on #341).
    crew = node.groups.list_groups()[0]
    entries = _sent_group_console_entries(bridge, node, crew["id"])
    node.mailbox.enqueue(
        to=bytes.fromhex(BOB_HASH),
        text="hello",
        message_id="wire-bob",
        group={
            "id": crew["id"],
            "name": "crew",
            "creator": PEER_HASH,
            "members": [PEER_HASH, BOB_HASH],
        },
        sent_at=1000.0,
    )
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": entries[0]["id"], "text": "hello again"},
    )
    assert status == 400
    assert "still queued for an offline member" in payload["error"]["message"]
    assert node.group_edits == []
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["text"] for entry in feed["messages"]] == [
        "hello",
        "hello",
    ]


def test_edit_message_endpoint_refuses_group_message_with_trimmed_queued_leg(
    bridge, node
):
    # The queued leg's console entry left the 500-entry history, so
    # its wire id is no longer among the retained legs — but the
    # mailbox still holds it, and the edit must still be refused
    # (see CodeRabbit on #341). The guard scans the mailbox for the
    # whole logical send (group id plus the shared send time), and
    # the refused edit leaves the queued leg untouched.
    crew = node.groups.list_groups()[0]
    entries = _sent_group_console_entries(
        bridge,
        node,
        crew["id"],
        second_status="queued",
        second_wire_id="mail-bob-1",
    )
    node.mailbox.enqueue(
        to=bytes.fromhex(BOB_HASH),
        text="hello",
        message_id="mail-bob-1",
        group={
            "id": crew["id"],
            "name": "crew",
            "creator": PEER_HASH,
            "members": [PEER_HASH, BOB_HASH],
        },
        sent_at=1000.0,
    )
    # The queued leg's console entry is gone from the history; only
    # the delivered leg remains to edit by.
    bridge._messages.remove(entries[1])
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": entries[0]["id"], "text": "hello again"},
    )
    assert status == 400
    assert "still queued for an offline member" in payload["error"]["message"]
    assert node.group_edits == []
    queued = node.mailbox.get_outbox("mail-bob-1")
    assert queued is not None
    assert queued.text == "hello"
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["text"] for entry in feed["messages"]] == ["hello"]


def test_edit_message_endpoint_rejects_unknown_group(bridge, node):
    _sent_console_entry(bridge, node, wire_id="wire-1", group="ab" * 16)
    status, _, payload = _json(
        bridge, "/api/messages/edit", method="POST", body={"id": 1, "text": "fixed"}
    )
    assert status == 400
    assert "unknown group" in payload["error"]["message"]


def test_edit_message_endpoint_rejects_group_message_with_no_wire_id(
    bridge, node
):
    crew = node.groups.list_groups()[0]
    for member, name in ((PEER_HASH, "alice"), (BOB_HASH, "bob")):
        bridge._append_message(
            direction="out",
            peer=member,
            peer_name=name,
            text="hello",
            sent_at=1000.0,
            received_at=1001.0,
            status="failed",
            wire_id=None,
            group=crew["id"],
            group_name="crew",
        )
    status, _, payload = _json(
        bridge, "/api/messages/edit", method="POST", body={"id": 1, "text": "fixed"}
    )
    assert status == 400
    assert "never delivered" in payload["error"]["message"]


def test_edit_message_endpoint_refuses_group_message_with_no_leg_send_time(
    bridge, node
):
    # A console entry restored from before group edits landed
    # carries its write time, which members cannot match: they would
    # answer delivered with edited False, and the fan-out would
    # still update the local copies.  Refuse instead, and keep the
    # bubbles.
    crew = node.groups.list_groups()[0]
    entries = _sent_group_console_entries(
        bridge, node, crew["id"], omit_leg_sent_at=True
    )
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": entries[0]["id"], "text": "hello again"},
    )
    assert status == 400
    assert "no leg send time" in payload["error"]["message"]
    assert node.group_edits == []
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["text"] for entry in feed["messages"]] == [
        "hello",
        "hello",
    ]


def test_edit_message_endpoint_uses_stored_leg_send_time(
    bridge, node
):
    # The entry's time drifted from the stored shared time: the
    # fan-out must carry the stored time the members can match,
    # while the update still rewrites the feed's legs.
    crew = node.groups.list_groups()[0]
    entries = _sent_group_console_entries(
        bridge, node, crew["id"], sent_at=1001.0, leg_sent_at=1000.0
    )
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": entries[0]["id"], "text": "hello again"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert node.group_edits == [("crew", "wire-alice", 1000.0, "hello again")]
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["text"] for entry in feed["messages"]] == [
        "hello again",
        "hello again",
    ]


def test_edit_message_endpoint_refuses_group_message_when_only_inbound_record_matches(
    bridge, node
):
    # The sent copy is pruned, so resolve() answers the leg id with
    # an inbound record instead: its time is not this node's shared
    # send time, so the edit is refused — and the received record is
    # never rewritten.
    crew = node.groups.list_groups()[0]
    node.inbox.append(
        {
            "id": "wire-alice",
            "destination": PEER_HASH,
            "direction": "in",
            "from": "alice",
            "text": "hello",
            "kind": "group",
            "group": crew["id"],
            "group_name": "crew",
            "sent_at": 1000.0,
            "received_at": 1001.0,
        }
    )
    entries = []
    for member, name, wire_id in (
        (PEER_HASH, "alice", "wire-alice"),
        (BOB_HASH, "bob", "wire-bob"),
    ):
        entries.append(
            bridge._append_message(
                direction="out",
                peer=member,
                peer_name=name,
                text="hello",
                sent_at=1000.0,
                received_at=1001.0,
                status="delivered",
                wire_id=wire_id,
                group=crew["id"],
                group_name="crew",
            )
        )
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": entries[0]["id"], "text": "hello again"},
    )
    assert status == 400
    assert "no leg send time" in payload["error"]["message"]
    assert node.group_edits == []
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["text"] for entry in feed["messages"]] == [
        "hello",
        "hello",
    ]
    assert len(node.inbox.export_document()["messages"]) == 1


def test_edit_message_endpoint_reports_local_copy_failure_after_group_fanout(
    bridge, node
):
    # Every member received the edit, but this node's own copies did
    # not update: the bubbles keep the old text so a retry re-sends
    # the edit and retries the local update.
    crew = node.groups.list_groups()[0]
    entries = _sent_group_console_entries(bridge, node, crew["id"])
    node.group_edit_local_errors = ["sent copy not updated: disk full"]
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": entries[0]["id"], "text": "hello again"},
    )
    assert status == 502
    assert "local copies" in payload["error"]["message"]
    assert "disk full" in payload["error"]["message"]
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["text"] for entry in feed["messages"]] == [
        "hello",
        "hello",
    ]


def _received_console_entry(bridge, *, text="hello there"):
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": text,
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )


def _received_file_entry(bridge, node, tmp_path, *, data=b"photo-bytes!", name="photo.jpg"):
    """A received file entry in the feed, with its bytes stored on disk."""
    import hashlib
    from pathlib import Path

    files_dir = Path(tmp_path) / "files"
    stored = files_dir / PEER_HASH / name
    stored.parent.mkdir(parents=True, exist_ok=True)
    stored.write_bytes(data)
    digest = hashlib.sha256(data).hexdigest()
    node.inbox.append(
        {
            "kind": "file",
            "from": "alice",
            "destination": PEER_HASH,
            "name": name,
            "size": len(data),
            "sha256": digest,
            "path": str(stored),
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-file-1",
            "direction": "in",
        }
    )
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": f"Received file: {name} ({len(data)} bytes, sha256 {digest[:12]})",
            "file": {"name": name, "size": len(data), "sha256": digest},
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-file-1",
        }
    )
    return stored


def test_forward_message_endpoint_forwards_received_message(bridge, node):
    from tau.blocklist import BlockList

    _received_console_entry(bridge)
    node.set_blocklist(BlockList())
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["message"]["text"] == "Forwarded from alice:\nhello there"
    assert payload["message"]["direction"] == "out"
    assert node.sent == [("alice", "Forwarded from alice:\nhello there", None)]
    # The original stays in the feed: forwarding sends a copy.
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["id"] for entry in feed["messages"]] == [1, 2]


def test_forward_message_endpoint_forwards_to_group(bridge, node):
    from tau.blocklist import BlockList

    _received_console_entry(bridge)
    node.set_blocklist(BlockList())
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "group": "crew"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert "message" not in payload
    assert [entry["text"] for entry in payload["messages"]] == [
        "Forwarded from alice:\nhello there",
        "Forwarded from alice:\nhello there",
    ]
    assert all(entry["group_name"] == "crew" for entry in payload["messages"])
    assert node.group_sent == [("crew", "Forwarded from alice:\nhello there")]
    # The original stays in the feed, plus one entry per member leg.
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["id"] for entry in feed["messages"]] == [1, 2, 3]


def test_forward_message_endpoint_forwards_received_file(
    bridge, node, tmp_path
):
    from tau.blocklist import BlockList

    _received_file_entry(bridge, node, tmp_path)
    node.set_blocklist(BlockList())
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert "message" not in payload
    assert payload["file_message"]["text"] == (
        "Forwarded from alice:\nSent file: photo.jpg (12 bytes)"
    )
    assert payload["file_message"]["file"]["forwarded_from"] == {
        "hash": PEER_HASH,
        "name": "alice",
    }
    assert payload["file_response"]["name"] == "photo.jpg"
    assert payload["file_response"]["size"] == 12
    # One leg only: no attribution text goes out, and the offer names
    # the original sender.
    assert node.sent == []
    assert node.sent_files == [("alice", "photo.jpg", b"photo-bytes!")]
    assert node.sent_file_kwargs[0]["forwarded_from"] == {
        "hash": PEER_HASH,
        "name": "alice",
    }
    # The original stays in the feed, plus the file.
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["id"] for entry in feed["messages"]] == [1, 2]


def test_forward_message_endpoint_forwards_received_file_to_group(
    bridge, node, tmp_path
):
    from tau.blocklist import BlockList

    _received_file_entry(bridge, node, tmp_path)
    node.set_blocklist(BlockList())
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "group": "crew"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert "messages" not in payload
    assert [entry["text"] for entry in payload["file_messages"]] == [
        "Forwarded from alice:\nSent file: photo.jpg (12 bytes)",
        "Forwarded from alice:\nSent file: photo.jpg (12 bytes)",
    ]
    assert payload["file_messages"][0]["file"]["forwarded_from"] == {
        "hash": PEER_HASH,
        "name": "alice",
    }
    assert payload["file_response"]["ok"] is True
    assert node.group_sent == []
    assert node.group_sent_files == [("crew", "photo.jpg", b"photo-bytes!")]
    assert node.group_sent_file_kwargs[0]["forwarded_from"] == {
        "hash": PEER_HASH,
        "name": "alice",
    }


def test_forward_message_endpoint_file_forward_keeps_original_sender(
    bridge, node, tmp_path
):
    from tau.blocklist import BlockList

    # A file that arrived already forwarded keeps its original sender
    # instead of re-attributing to the intermediate forwarder.
    import hashlib
    from pathlib import Path

    files_dir = Path(tmp_path) / "files"
    stored = files_dir / PEER_HASH / "photo.jpg"
    stored.parent.mkdir(parents=True, exist_ok=True)
    stored.write_bytes(b"photo-bytes!")
    digest = hashlib.sha256(b"photo-bytes!").hexdigest()
    forwarded_from = {"hash": "cd" * 16, "name": "carol"}
    node.inbox.append(
        {
            "kind": "file",
            "from": "alice",
            "destination": PEER_HASH,
            "name": "photo.jpg",
            "size": 12,
            "sha256": digest,
            "path": str(stored),
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-file-1",
            "direction": "in",
            "forwarded_from": forwarded_from,
        }
    )
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "Forwarded from carol:\nReceived file: photo.jpg (12 bytes, sha256 abababababab)",
            "file": {
                "name": "photo.jpg",
                "size": 12,
                "sha256": digest,
                "forwarded_from": forwarded_from,
            },
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-file-1",
        }
    )
    node.set_blocklist(BlockList())
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["file_message"]["text"] == (
        "Forwarded from carol:\nSent file: photo.jpg (12 bytes)"
    )
    assert payload["file_message"]["file"]["forwarded_from"] == forwarded_from
    assert node.sent_file_kwargs[0]["forwarded_from"] == forwarded_from


def test_forward_message_endpoint_file_forward_missing_bytes_is_an_error(
    bridge, node, tmp_path
):
    from tau.blocklist import BlockList

    # The entry exists but the bytes were never stored: nothing goes out.
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "Received file: photo.jpg (12 bytes, sha256 abababababab)",
            "file": {"name": "photo.jpg", "size": 12, "sha256": "ab" * 32},
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-file-1",
        }
    )
    node.set_blocklist(BlockList())
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice"},
    )
    assert status == 400
    assert "no longer available" in payload["error"]["message"]
    assert node.sent == []
    assert node.sent_files == []


def test_forward_message_endpoint_file_forward_blocked_sender_is_an_error(
    bridge, node, tmp_path
):
    from tau.blocklist import BlockList

    _received_file_entry(bridge, node, tmp_path)
    node.set_blocklist(BlockList(blocked=frozenset({PEER_HASH})))
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice"},
    )
    assert status == 400
    assert "blocked peer" in payload["error"]["message"]
    assert node.sent == []
    assert node.sent_files == []


def test_forward_message_endpoint_file_forward_failed_leg_reports_partial(
    bridge, node, tmp_path
):
    from tau.blocklist import BlockList
    from tau.node import NodeError

    _received_file_entry(bridge, node, tmp_path)
    node.set_blocklist(BlockList())

    def send_file(target, path, **kwargs):
        raise NodeError("no path to alice")

    node.send_file = send_file
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice"},
    )
    assert status == 200
    assert payload["ok"] is False
    # Nothing went out: there is no text leg anymore, only the file.
    assert "no path to alice" in payload["file_error"]
    assert "file_message" not in payload
    assert node.sent == []


def test_forward_message_endpoint_file_forward_uses_entry_record_not_newest(
    bridge, node, tmp_path
):
    # Qodo finding: the forward used to look the bytes up by
    # (sender, name) — the newest same-name record — so an old entry
    # forwarded a newer file that arrived under the same name.  The
    # old entry must fail when its own bytes are gone, and the new
    # entry must forward the new bytes.
    import hashlib
    from pathlib import Path

    from tau.blocklist import BlockList

    old_bytes = b"old-photo-bytes!"
    new_bytes = b"new-photo!"
    files_dir = Path(tmp_path) / "files"
    stored = files_dir / PEER_HASH / "photo.jpg"
    stored.parent.mkdir(parents=True, exist_ok=True)
    stored.write_bytes(new_bytes)
    for wire_id, data in (("wire-old-1", old_bytes), ("wire-new-1", new_bytes)):
        digest = hashlib.sha256(data).hexdigest()
        node.inbox.append(
            {
                "kind": "file",
                "from": "alice",
                "destination": PEER_HASH,
                "name": "photo.jpg",
                "size": len(data),
                "sha256": digest,
                "path": str(stored),
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": wire_id,
                "direction": "in",
            }
        )
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": f"Received file: photo.jpg ({len(data)} bytes, sha256 {digest[:12]})",
                "file": {"name": "photo.jpg", "size": len(data), "sha256": digest},
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": wire_id,
            }
        )
    node.set_blocklist(BlockList())
    # The old entry's bytes were overwritten by the new arrival: the
    # forward fails instead of sending the new file as the old one.
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice"},
    )
    assert status == 400
    assert "changed since it arrived" in payload["error"]["message"]
    assert node.sent == []
    assert node.sent_files == []
    # The new entry forwards its own bytes.
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 2, "to": "alice"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert node.sent_files == [("alice", "photo.jpg", new_bytes)]


def test_forward_message_endpoint_file_forward_passes_timeout(
    bridge, node, tmp_path
):
    from tau.blocklist import BlockList

    _received_file_entry(bridge, node, tmp_path)
    node.set_blocklist(BlockList())
    seen = {}
    original_send_file = node.send_file

    def capturing_send_file(target, path, **kwargs):
        seen.update(kwargs)
        return original_send_file(target, path, **kwargs)

    node.send_file = capturing_send_file
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice", "timeout": 60},
    )
    assert status == 200
    assert payload["ok"] is True
    assert seen.get("timeout") == 60


def test_forward_message_endpoint_file_forward_defaults_file_timeout_to_300(
    bridge, node, tmp_path
):
    from tau.blocklist import BlockList

    _received_file_entry(bridge, node, tmp_path)
    node.set_blocklist(BlockList())
    seen = {}
    original_send_file = node.send_file

    def capturing_send_file(target, path, **kwargs):
        seen["file_timeout"] = kwargs.get("timeout")
        return original_send_file(target, path, **kwargs)

    node.send_file = capturing_send_file
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice"},
    )
    assert status == 200
    assert payload["ok"] is True
    # No timeout in the request: the file leg gets the 300s send-file
    # default, so a file of a few MiB on a slow Reticulum link can
    # still forward.
    assert seen.get("file_timeout") == 300.0


def test_forward_message_endpoint_file_forward_oserror_reports_partial(
    bridge, node, tmp_path
):
    from tau.blocklist import BlockList

    _received_file_entry(bridge, node, tmp_path)
    node.set_blocklist(BlockList())

    def send_file(target, path, **kwargs):
        raise OSError("disk full")

    node.send_file = send_file
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice"},
    )
    assert status == 200
    assert payload["ok"] is False
    # Nothing went out: there is no text leg anymore, only the file.
    assert "disk full" in payload["file_error"]
    assert node.sent == []


def test_forward_message_endpoint_rejects_both_or_neither_target(bridge, node):
    from tau.blocklist import BlockList

    _received_console_entry(bridge)
    node.set_blocklist(BlockList())
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1},
    )
    assert status == 400
    assert "exactly one of 'to' and 'group'" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice", "group": "crew"},
    )
    assert status == 400
    assert "exactly one of 'to' and 'group'" in payload["error"]["message"]

    # An unknown group is a plain 400, like an unknown peer.
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "group": "nope"},
    )
    assert status == 400
    assert "unknown group" in payload["error"]["message"]


def test_forward_message_endpoint_reports_failed_group_leg(bridge, node):
    from tau.blocklist import BlockList

    _received_console_entry(bridge)
    node.set_blocklist(BlockList())
    node.group_refusals = {BOB_HASH}
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "group": "crew"},
    )
    assert status == 200
    # The fan-out is partial: the top-level ok says so, like the
    # group-send endpoint, instead of claiming success.
    assert payload["ok"] is False
    assert [m["status"] for m in payload["messages"]] == ["delivered", "failed"]
    assert payload["messages"][1]["peer_name"] == "bob"
    assert payload["response"]["results"][1]["error"] == "not_allowed"


def test_forward_message_endpoint_attributes_own_message_as_you(bridge, node):
    _sent_console_entry(bridge, node)
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice"},
    )
    assert status == 200
    assert payload["message"]["text"] == "Forwarded from you:\nhello"


def test_forward_message_endpoint_prefers_registry_name(bridge, node):
    from tau.blocklist import BlockList

    # The entry was recorded under a stale name; the registry's current
    # display name wins, like `tau forward`.
    bridge.record_inbound(
        {
            "from": "stale-name",
            "destination": PEER_HASH,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )
    node.set_blocklist(BlockList())
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice"},
    )
    assert status == 200
    assert payload["message"]["text"] == "Forwarded from alice:\nhello"


def test_forward_message_endpoint_rejects_group_file_and_blocked(bridge, node):
    from tau.blocklist import BlockList

    # Group messages would leak the group's text to a nonmember.
    _sent_console_entry(bridge, node, wire_id="wire-1", group="ab" * 16)
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice"},
    )
    assert status == 400
    assert "group messages cannot be forwarded" in payload["error"]["message"]

    # A file you sent cannot be forwarded: its bytes were staged in a
    # temporary directory for the transfer and not kept.
    _sent_console_entry(
        bridge,
        node,
        wire_id="wire-2",
        file={"name": "photo.jpg", "size": 12, "sha256": "ab" * 32},
    )
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 2, "to": "alice"},
    )
    assert status == 400
    assert "cannot forward a file you sent" in payload["error"]["message"]

    # A received message from a blocked peer stays unforwardable.
    _received_console_entry(bridge)
    node.set_blocklist(BlockList(blocked=frozenset({PEER_HASH})))
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 3, "to": "alice"},
    )
    assert status == 400
    assert "blocked peer" in payload["error"]["message"]

    # An unreadable block-list fails closed for received messages,
    # matching `tau forward`.
    node.set_blocklist(None)
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 3, "to": "alice"},
    )
    assert status == 400
    assert "could not be read" in payload["error"]["message"]


def test_forward_message_endpoint_rejects_unknown_and_bad_input(bridge, node):
    _received_console_entry(bridge)
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 99, "to": "alice"},
    )
    assert status == 400
    assert "unknown message" in payload["error"]["message"]

    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": "1", "to": "alice"},
    )
    assert status == 400
    assert "field 'id'" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/messages/forward", method="POST", body={"id": 1}
    )
    assert status == 400
    assert "exactly one of 'to' and 'group'" in payload["error"]["message"]
    assert node.sent == []


def test_forward_message_endpoint_rejects_restored_file_entry(bridge, node):
    # Regression test for the Qodo review finding: a file restored
    # from a backup used to arrive in the feed without its file
    # metadata, so the forward guard passed it and the peer got a
    # fake "Received file: ..." line as a DM.  The entry carries its
    # file metadata now — but its bytes were never stored under
    # files/, so forwarding fails cleanly instead of sending anything.
    from tau.blocklist import BlockList

    node.set_blocklist(BlockList())
    bridge._mirror_imported_messages(
        [
            {
                "from": "alice",
                "destination": PEER_HASH,
                "kind": "file",
                "name": "photo.jpg",
                "size": 12,
                "sha256": "ab" * 32,
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "wire-file-1",
            }
        ]
    )
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["file"] == {
        "name": "photo.jpg",
        "size": 12,
        "sha256": "ab" * 32,
    }
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice"},
    )
    assert status == 400
    assert "no longer available" in payload["error"]["message"]
    assert node.sent == []


def test_forward_message_endpoint_reports_full_outbox(bridge, node):
    from tau.mailbox import MailboxFull

    _received_console_entry(bridge)
    node.set_blocklist(BlockList())

    def send_message(peer, text, **kwargs):
        raise MailboxFull("outbox is full (100 messages)")

    node.send_message = send_message
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice"},
    )
    assert status == 503
    assert "outbox is full" in payload["error"]["message"]


def test_forward_message_endpoint_refuses_expired_disappearing_message(
    bridge, node
):
    # The Qodo autofix refuses forwarding past the expiry: the text
    # was meant to vanish, so forwarding it would resurrect it.
    _received_console_entry(bridge)
    node.set_blocklist(BlockList())
    entry = bridge._messages[0]
    entry["expires_at"] = time.time() - 1.0
    status, _, payload = _json(
        bridge,
        "/api/messages/forward",
        method="POST",
        body={"id": 1, "to": "alice"},
    )
    assert status == 400
    assert "unknown message" in payload["error"]["message"]
    assert node.sent == []


def test_retract_message_endpoint_rejects_unknown_and_bad_ids(bridge):
    status, _, payload = _json(
        bridge, "/api/messages/retract", method="POST", body={"id": 7}
    )
    assert status == 400
    assert "unknown message" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/messages/retract", method="POST", body={}
    )
    assert status == 400
    assert "field 'id'" in payload["error"]["message"]


def test_retract_message_endpoint_keeps_bubble_when_peer_unreachable(
    bridge, node
):
    _sent_console_entry(bridge, node)

    def send_retraction(peer, message_id, *, timeout=20.0, discover_timeout=None):
        raise NodeError("peer did not answer")

    node.send_retraction = send_retraction
    status, _, payload = _json(
        bridge, "/api/messages/retract", method="POST", body={"id": 1}
    )
    assert status == 502
    assert "peer did not answer" in payload["error"]["message"]
    # A failed retract never erases the local copy: the bubble stays
    # for a retry.
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["id"] for entry in feed["messages"]] == [1]
    assert node.inbox.export_document()["messages"] != []


def test_retract_still_refuses_lxmf_copy_after_sent_record_evicted(
    bridge, node
):
    # A queued entry, delivered over LXMF: the delivery flip stamps
    # the route on the console entry itself.
    entry = bridge._append_message(
        direction="out",
        peer=PEER_HASH,
        peer_name="alice",
        text="hello",
        sent_at=100.0,
        received_at=101.0,
        status="queued",
        wire_id="wire-1",
        mail_id="wire-1",
    )
    assert entry is not None
    node.mailbox.record_sent(
        "wire-1", to=bytes.fromhex(PEER_HASH), text="hello", sent_at=100.0
    )
    node.mailbox.record_delivered("wire-1", via="lxmf")
    updated = bridge.record_delivery("wire-1")
    assert updated is not None
    assert updated["status"] == "delivered"
    assert updated["delivered_via"] == "lxmf"

    # The sent record is evicted (256-send cap / 30-day sweep) while
    # the console entry remains retractable: the entry's own stamp
    # still refuses the retract, and nothing goes out.
    node.mailbox.discard_sent("wire-1")
    called = []

    def send_retraction(peer, message_id, *, timeout=20.0, discover_timeout=None):
        called.append((peer, message_id))
        return {"ok": True, "deleted": True, "id": "retract-1"}

    node.send_retraction = send_retraction
    status, _, payload = _json(
        bridge, "/api/messages/retract", method="POST", body={"id": entry["id"]}
    )
    assert status == 400
    assert "delivered over LXMF" in payload["error"]["message"]
    assert called == []


def test_stored_message_keeps_delivery_route(bridge):
    entry = bridge._append_message(
        direction="out",
        peer=PEER_HASH,
        peer_name="alice",
        text="hello",
        sent_at=100.0,
        received_at=101.0,
        status="delivered",
        wire_id="wire-1",
        delivered_via="direct",
    )
    assert entry is not None
    assert entry["delivered_via"] == "direct"

    # The loader keeps a known route across a restart, and drops an
    # unknown one instead of trusting it.
    stored = {
        "id": 1,
        "direction": "out",
        "peer": PEER_HASH,
        "peer_name": "alice",
        "text": "hello",
        "sent_at": 100.0,
        "received_at": 101.0,
        "status": "delivered",
        "wire_id": "wire-1",
        "delivered_via": "lxmf",
    }
    cleaned = _clean_stored_message(stored)
    assert cleaned is not None
    assert cleaned["delivered_via"] == "lxmf"
    assert "delivered_via" not in _clean_stored_message(
        {**stored, "delivered_via": "carrier-pigeon"}
    )


def test_retract_message_endpoint_rejects_file_entries(bridge, node):
    # The peer refuses retracts of saved files (the bytes live on
    # disk), so offering retract on a sent file would always fail.
    _sent_console_entry(
        bridge,
        node,
        file={"name": "notes.txt", "size": 10, "sha256": "ab" * 32},
    )
    status, _, payload = _json(
        bridge, "/api/messages/retract", method="POST", body={"id": 1}
    )
    assert status == 400
    assert "file transfers cannot be retracted" in payload["error"]["message"]


def test_retract_message_endpoint_rejects_lxmf_delivered_copy(bridge, node):
    _sent_console_entry(bridge, node)
    node.mailbox.record_delivered("wire-1", via="lxmf")
    called = []

    def send_retraction(peer, message_id, *, timeout=20.0, discover_timeout=None):
        called.append((peer, message_id))
        return {"ok": True, "deleted": True, "id": "retract-1"}

    node.send_retraction = send_retraction
    status, _, payload = _json(
        bridge, "/api/messages/retract", method="POST", body={"id": 1}
    )
    assert status == 400
    assert "delivered over LXMF" in payload["error"]["message"]
    # The retract never went out: the recipient stores that copy
    # under an `lxmf:` id, so naming the tau wire id could not
    # delete it — and the sender's copies stay in place.
    assert called == []
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["id"] for entry in feed["messages"]] == [1]


def test_retract_message_endpoint_never_deletes_a_prefix_sharing_record(
    bridge, node
):
    # The sender's inbox copy is already gone, and another sender's
    # record shares the wire id's prefix: the local delete must not
    # fall through to that record.
    node.inbox.append(
        {
            "id": "wire-1-other",
            "destination": PEER_HASH,
            "from": "alice",
            "text": "another sender's message",
            "sent_at": 100.0,
            "received_at": 101.0,
        }
    )
    node.mailbox.record_sent(
        "wire-1", to=bytes.fromhex(PEER_HASH), text="hello", sent_at=100.0
    )
    bridge._append_message(
        direction="out",
        peer=PEER_HASH,
        peer_name="alice",
        text="hello",
        sent_at=100.0,
        received_at=101.0,
        status="delivered",
        wire_id="wire-1",
    )

    def send_retraction(peer, message_id, *, timeout=20.0, discover_timeout=None):
        return {"ok": True, "deleted": True, "id": "retract-1"}

    node.send_retraction = send_retraction
    status, _, payload = _json(
        bridge, "/api/messages/retract", method="POST", body={"id": 1}
    )
    assert status == 200
    assert payload["deleted"] is True
    assert [
        entry["id"] for entry in node.inbox.list_messages()
    ] == ["wire-1-other"]


def _seed_pinnable_message(bridge, node, wire_id="wire-1"):
    node.inbox.append(
        {
            "id": wire_id,
            "destination": PEER_HASH,
            "from": "alice",
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
        }
    )
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": wire_id,
        }
    )


def test_pin_message_endpoint_pins_inbox_and_annotates_feed(bridge, node):
    _seed_pinnable_message(bridge, node)
    status, _, payload = _json(
        bridge, "/api/messages/pin", method="POST", body={"id": 1}
    )
    assert status == 200
    assert payload == {
        "ok": True,
        "pinned": {"id": 1, "wire_id": "wire-1", "already_pinned": False},
    }
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["pinned"] is True
    assert [entry["id"] for entry in node.inbox.list_messages(pinned_only=True)] == [
        "wire-1"
    ]
    # Other tabs learn through a message_updated event carrying the
    # annotated entry, like a reaction does.
    updated_events = [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_updated"
    ]
    assert len(updated_events) == 1
    assert updated_events[0]["message"]["id"] == 1
    assert updated_events[0]["message"]["pinned"] is True


def test_pin_message_endpoint_writes_exactly_one_response(bridge, node, monkeypatch):
    # A merge left two _send_json calls at the end of the pin handler:
    # on a persistent connection the second response is read as the
    # answer to the client's next request.
    import tau.webapi as webapi

    _seed_pinnable_message(bridge, node)
    calls = []
    original = webapi._WebHandler._send_json

    def counting(self, payload, head_only=False, status=200):
        calls.append(payload)
        return original(self, payload, head_only, status)

    monkeypatch.setattr(webapi._WebHandler, "_send_json", counting)
    status, _, payload = _json(
        bridge, "/api/messages/pin", method="POST", body={"id": 1}
    )
    assert status == 200
    assert payload["pinned"]["id"] == 1
    assert len(calls) == 1


def test_pin_message_endpoint_is_a_noop_when_already_pinned(bridge, node):
    _seed_pinnable_message(bridge, node)
    status, _, payload = _json(
        bridge, "/api/messages/pin", method="POST", body={"id": 1}
    )
    assert status == 200
    status, _, payload = _json(
        bridge, "/api/messages/pin", method="POST", body={"id": 1}
    )
    assert status == 200
    assert payload["pinned"]["already_pinned"] is True
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["pinned"] is True


def test_unpin_message_endpoint_clears_pin(bridge, node):
    _seed_pinnable_message(bridge, node)
    _json(bridge, "/api/messages/pin", method="POST", body={"id": 1})
    status, _, payload = _json(
        bridge, "/api/messages/unpin", method="POST", body={"id": 1}
    )
    assert status == 200
    assert payload == {
        "ok": True,
        "unpinned": {"id": 1, "wire_id": "wire-1", "was_pinned": True},
    }
    _, _, feed = _json(bridge, "/api/messages")
    assert "pinned" not in feed["messages"][0]
    assert node.inbox.list_messages(pinned_only=True) == []
    # Unpinning again is a no-op, not an error.
    status, _, payload = _json(
        bridge, "/api/messages/unpin", method="POST", body={"id": 1}
    )
    assert status == 200
    assert payload["unpinned"]["was_pinned"] is False


def test_console_feed_shows_cli_pin(bridge, node):
    # A pin made from another process (``tau pin``) lands in the shared
    # inbox.json only; the listing must still show it on the next read.
    _seed_pinnable_message(bridge, node)
    node.inbox.pin("wire-1")
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["pinned"] is True


def test_messages_pinned_filter_lists_only_pinned(bridge, node):
    _seed_pinnable_message(bridge, node)
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "not pinned",
            "sent_at": 102.0,
            "received_at": 103.0,
            "id": "wire-2",
        }
    )
    node.inbox.append(
        {
            "id": "wire-2",
            "destination": PEER_HASH,
            "from": "alice",
            "text": "not pinned",
            "sent_at": 102.0,
            "received_at": 103.0,
        }
    )
    _json(bridge, "/api/messages/pin", method="POST", body={"id": 1})
    _, _, feed = _json(bridge, "/api/messages?pinned=1")
    assert [entry["id"] for entry in feed["messages"]] == [1]
    assert feed["messages"][0]["pinned"] is True
    _, _, feed = _json(bridge, "/api/messages")
    assert len(feed["messages"]) == 2


def test_pin_message_endpoint_rejects_unknown_and_bad_ids(bridge):
    status, _, payload = _json(
        bridge, "/api/messages/pin", method="POST", body={"id": 7}
    )
    assert status == 400
    assert "unknown message" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/messages/pin", method="POST", body={}
    )
    assert status == 400
    assert "field 'id'" in payload["error"]["message"]

    status, _, payload = _json(
        bridge, "/api/messages/pin", method="POST", body={"id": "1"}
    )
    assert status == 400
    assert "field 'id'" in payload["error"]["message"]


def test_pin_message_endpoint_rejects_message_without_stored_copy(bridge):
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            # No wire id: an entry from before ids existed, or one the
            # node inbox never stored. There is no inbox record to pin.
        }
    )
    status, _, payload = _json(
        bridge, "/api/messages/pin", method="POST", body={"id": 1}
    )
    assert status == 400
    assert "no stored copy" in payload["error"]["message"]


def test_pin_message_endpoint_rejects_queued_entry(bridge, node):
    _queue_send(node)
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    assert status == 200
    assert payload["message"]["status"] == "queued"

    status, _, payload = _json(
        bridge, "/api/messages/pin", method="POST", body={"id": 1}
    )
    assert status == 400
    assert "still queued" in payload["error"]["message"]


def test_pin_message_does_not_publish_blocked_message(bridge, node):
    # A message received before its sender was blocked is retained but
    # hidden from the listing; pinning it must not publish its text to
    # /api/events, where a remote client could read it back by id.
    _seed_pinnable_message(bridge, node)
    node.blocklist = BlockList(blocked=frozenset({PEER_HASH}))
    status, _, payload = _json(
        bridge, "/api/messages/pin", method="POST", body={"id": 1}
    )
    assert status == 200
    assert payload["pinned"]["already_pinned"] is False
    # The pin itself landed — only the event is suppressed.
    assert [
        entry["id"] for entry in node.inbox.list_messages(pinned_only=True)
    ] == ["wire-1"]
    assert [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_updated"
    ] == []


def test_messages_pinned_reports_inbox_read_failure(bridge, node, monkeypatch):
    _seed_pinnable_message(bridge, node)

    def _boom(*args, **kwargs):
        raise RuntimeError("inbox unreadable")

    monkeypatch.setattr(node.inbox, "list_messages", _boom)
    # The pinned view reports the failure instead of an empty pin list.
    with pytest.raises(RuntimeError, match="inbox unreadable"):
        bridge.messages(limit=-1, strict_pins=True)
    # The live feed stays up: a failed pin overlay degrades to no pins.
    _, _, feed = _json(bridge, "/api/messages")
    assert "pinned" not in feed["messages"][0]


def test_delete_by_wire_id_removes_the_console_copy(bridge):
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "take this back",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "keep me",
            "sent_at": 102.0,
            "received_at": 103.0,
            "id": "wire-2",
        }
    )
    assert bridge.delete_by_wire_id("missing") is None
    assert bridge.delete_by_wire_id("") is None

    result = bridge.delete_by_wire_id("wire-1")
    assert result == {"ok": True, "deleted": {"id": 1, "wire_id": "wire-1"}}
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["wire_id"] for entry in feed["messages"]] == ["wire-2"]
    deleted_events = [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_deleted"
    ]
    assert len(deleted_events) == 1
    assert deleted_events[0]["message_id"] == 1


def test_delete_sent_by_wire_id_removes_only_the_outbound_copy(bridge):
    sent = bridge._append_message(
        direction="out",
        peer=PEER_HASH,
        peer_name="alice",
        text="take this back",
        sent_at=100.0,
        received_at=101.0,
        status="delivered",
        wire_id="wire-1",
    )
    bridge._append_message(
        direction="out",
        peer=PEER_HASH,
        peer_name="alice",
        text="keep me",
        sent_at=102.0,
        received_at=103.0,
        status="delivered",
        wire_id="wire-2",
    )
    # An inbound message carrying the same wire id is a different
    # record: the sent delete must not touch it.
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "same id, other direction",
            "sent_at": 104.0,
            "received_at": 105.0,
            "id": "wire-1",
        }
    )
    assert bridge.delete_sent_by_wire_id("missing") is None
    assert bridge.delete_sent_by_wire_id("") is None

    result = bridge.delete_sent_by_wire_id("wire-1")
    assert result == {"ok": True, "deleted": {"id": sent["id"], "wire_id": "wire-1"}}
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["wire_id"] for entry in feed["messages"]] == ["wire-2", "wire-1"]
    deleted_events = [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_deleted"
    ]
    assert len(deleted_events) == 1
    assert deleted_events[0]["message_id"] == sent["id"]


def test_retracted_sent_id_does_not_suppress_later_inbound(bridge):
    # Regression test (Qodo on PR #328): deleting a sent entry must not
    # put its wire id in the inbound removal set, or a peer reusing
    # that id could hide a later inbound message from the console.
    bridge._append_message(
        direction="out",
        peer=PEER_HASH,
        peer_name="alice",
        text="take this back",
        sent_at=100.0,
        received_at=101.0,
        status="delivered",
        wire_id="wire-1",
    )
    assert bridge.delete_sent_by_wire_id("wire-1") is not None
    arrived = bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "same id, later message",
            "sent_at": 102.0,
            "received_at": 103.0,
            "id": "wire-1",
        }
    )
    assert arrived is not None
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["wire_id"] for entry in feed["messages"]] == ["wire-1"]
    assert feed["messages"][0]["direction"] == "in"


def test_delete_sent_group_removes_only_the_matching_legs(bridge):
    group = "cd" * 16
    leg_one = bridge._append_message(
        direction="out",
        peer=PEER_HASH,
        peer_name="alice",
        text="group hello",
        sent_at=100.0,
        received_at=101.0,
        status="delivered",
        group=group,
        wire_id="leg-1",
    )
    leg_two = bridge._append_message(
        direction="out",
        peer="ef" * 16,
        peer_name="bob",
        text="group hello",
        sent_at=100.0,
        received_at=101.0,
        status="delivered",
        group=group,
        wire_id="leg-2",
    )
    # Same group, a different send: its leg stays.
    bridge._append_message(
        direction="out",
        peer=PEER_HASH,
        peer_name="alice",
        text="later group hello",
        sent_at=200.0,
        received_at=201.0,
        status="delivered",
        group=group,
        wire_id="leg-3",
    )
    # An inbound message in the same group at the same time stays too.
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "their group hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "leg-9",
            "kind": "group",
            "group": group,
        }
    )
    assert bridge.delete_sent_group("missing", 100.0) is None
    assert bridge.delete_sent_group(group, 999.0) is None
    assert bridge.delete_sent_group("", 100.0) is None
    assert bridge.delete_sent_group(group, float("nan")) is None

    result = bridge.delete_sent_group(group, 100.0)
    assert result == {
        "ok": True,
        "deleted_ids": [leg_one["id"], leg_two["id"]],
        "group": group,
        "sent_at": 100.0,
    }
    _, _, feed = _json(bridge, "/api/messages")
    assert [entry["wire_id"] for entry in feed["messages"]] == ["leg-3", "leg-9"]
    deleted_events = [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_deleted"
    ]
    assert [event["message_id"] for event in deleted_events] == [
        leg_one["id"],
        leg_two["id"],
    ]


def test_delete_sent_group_persists_all_legs_in_one_write(
    node, tmp_path, monkeypatch
):
    """A multi-leg group delete is one strict write, not one per leg.

    Regression test for issue #330 finding 4: the per-leg loop
    persisted each leg separately, so a disk error mid-way left a
    partially deleted group with no inbox record to retry from.
    """
    import tau.webapi

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        real_write = tau.webapi.atomic_write_json
        writes = []

        def counting_write(path, obj, mode=None):
            writes.append(path)
            return real_write(path, obj, mode=mode)

        monkeypatch.setattr(tau.webapi, "atomic_write_json", counting_write)
        group = "cd" * 16
        leg_one = bridge._append_message(
            direction="out",
            peer=PEER_HASH,
            peer_name="alice",
            text="group hello",
            sent_at=100.0,
            received_at=101.0,
            status="delivered",
            group=group,
            wire_id="leg-1",
        )
        leg_two = bridge._append_message(
            direction="out",
            peer="ef" * 16,
            peer_name="bob",
            text="group hello",
            sent_at=100.0,
            received_at=101.0,
            status="delivered",
            group=group,
            wire_id="leg-2",
        )
        writes.clear()
        result = bridge.delete_sent_group(group, 100.0)
        assert result["deleted_ids"] == [leg_one["id"], leg_two["id"]]
        assert len(writes) == 1
        deleted_events = [
            event
            for event in bridge.bus.replay()
            if event.get("type") == "message_deleted"
        ]
        assert [event["message_id"] for event in deleted_events] == [
            leg_one["id"],
            leg_two["id"],
        ]
    finally:
        bridge.stop()


def test_delete_sent_group_restores_all_legs_when_the_write_fails(
    node, tmp_path, monkeypatch
):
    """A failed group delete restores every leg and publishes nothing.

    Regression test for issue #330 finding 4: with one strict write
    for the whole batch, a disk error restores the complete group
    instead of leaving some legs deleted, and no tab is told the
    legs are gone.
    """
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        import tau.webapi

        group = "cd" * 16
        bridge._append_message(
            direction="out",
            peer=PEER_HASH,
            peer_name="alice",
            text="group hello",
            sent_at=100.0,
            received_at=101.0,
            status="delivered",
            group=group,
            wire_id="leg-1",
        )
        bridge._append_message(
            direction="out",
            peer="ef" * 16,
            peer_name="bob",
            text="group hello",
            sent_at=100.0,
            received_at=101.0,
            status="delivered",
            group=group,
            wire_id="leg-2",
        )
        # Same group, a different send: untouched by the delete.
        bridge._append_message(
            direction="out",
            peer=PEER_HASH,
            peer_name="alice",
            text="later group hello",
            sent_at=200.0,
            received_at=201.0,
            status="delivered",
            group=group,
            wire_id="leg-3",
        )

        def _failing_write(*args, **kwargs):
            raise OSError("disk full")

        monkeypatch.setattr(tau.webapi, "atomic_write_json", _failing_write)
        with pytest.raises(OSError, match="disk full"):
            bridge.delete_sent_group(group, 100.0)
        _, _, feed = _json(bridge, "/api/messages")
        assert [entry["wire_id"] for entry in feed["messages"]] == [
            "leg-1",
            "leg-2",
            "leg-3",
        ]
        deleted_events = [
            event
            for event in bridge.bus.replay()
            if event.get("type") == "message_deleted"
        ]
        assert deleted_events == []
    finally:
        bridge.stop()


def test_retract_removes_the_recipients_console_copy(tmp_path):
    # End to end: a retract accepted by the node also drops the
    # console's copy, so /api/messages stops showing the text.
    import base64

    from tau.node import TauNode
    from tau.peers import PeerRegistry
    from tau.protocol import decode, encode, encode_message, retract_dict

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="holder")
    node = TauNode(config, registry=PeerRegistry(config.peers_path))
    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    bridge = WebBridge(node, host="127.0.0.1", port=0, web_dir=web_dir)
    bridge.start()
    try:
        node.on_message = bridge.record_inbound
        node.on_retract = lambda record: bridge.delete_by_wire_id(
            str(record.get("id") or "")
        )
        sender = RNS.Identity()
        node.destination = SimpleNamespace(
            hexhash="ab" * 16, hash=b"\xab" * 16
        )
        payload = encode_message("take this back", message_id="msg-1")
        response = decode(
            node._handle_message("/tau/test", payload, b"request-id", sender, time.time())
        )
        assert response["ok"] is True
        _, _, feed = _json(bridge, "/api/messages")
        assert len(feed["messages"]) == 1

        retract_payload = retract_dict("msg-1", message_id="retract-1")
        signature = sender.sign(encode(retract_payload))
        retract_payload["sig"] = base64.b64encode(signature).decode("ascii")
        response = decode(
            node._handle_retract(
                "/tau/test", encode(retract_payload), b"request-id", sender, time.time()
            )
        )
        assert response["ok"] is True
        assert response["deleted"] is True
        assert node.inbox.list_messages() == []
        _, _, feed = _json(bridge, "/api/messages")
        assert feed["messages"] == []
        deleted_events = [
            event
            for event in bridge.bus.replay()
            if event.get("type") == "message_deleted"
        ]
        assert len(deleted_events) == 1
    finally:
        bridge.stop()


def test_retract_before_pending_delivery_does_not_resurrect(tmp_path):
    # The delivery persisted the DM to the inbox but its console
    # callback had not run yet when the retract landed: the callback
    # must not write the message into console history afterwards.
    import base64

    from tau.identity import destination_hash
    from tau.node import TauNode
    from tau.peers import PeerRegistry
    from tau.protocol import decode, encode, retract_dict

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="holder")
    node = TauNode(config, registry=PeerRegistry(config.peers_path))
    web_dir = tmp_path / "out"
    web_dir.mkdir()
    (web_dir / "index.html").write_text("<html>tau console</html>", encoding="utf-8")
    bridge = WebBridge(node, host="127.0.0.1", port=0, web_dir=web_dir)
    bridge.start()
    try:
        node.on_message = bridge.record_inbound
        node.on_retract = lambda record: bridge.delete_by_wire_id(
            str(record.get("id") or "")
        )
        sender = RNS.Identity()
        sender_hash = destination_hash(sender).hex()
        node.destination = SimpleNamespace(
            hexhash="ab" * 16, hash=b"\xab" * 16
        )
        record = {
            "from": "alice",
            "destination": sender_hash,
            "text": "take this back",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "msg-1",
            "direction": "in",
        }
        # Phase 1 of the delivery: persisted to the inbox, console
        # callback still pending.
        node.inbox.append(record)
        node.mailbox.mark_seen("msg-1")
        # The retract lands before the callback runs.
        retract_payload = retract_dict("msg-1", message_id="retract-1")
        signature = sender.sign(encode(retract_payload))
        retract_payload["sig"] = base64.b64encode(signature).decode("ascii")
        response = decode(
            node._handle_retract(
                "/tau/test", encode(retract_payload), b"request-id", sender, time.time()
            )
        )
        assert response["ok"] is True
        assert response["deleted"] is True
        assert node.inbox.list_messages() == []
        # Phase 2: the pending delivery callback finally runs.
        assert bridge.record_inbound(record) is None
        _, _, feed = _json(bridge, "/api/messages")
        assert feed["messages"] == []
    finally:
        bridge.stop()


def test_delete_message_survives_bridge_restart(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "hello",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "wire-1",
            }
        )
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "still here",
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "wire-2",
            }
        )
        status, _, _ = _json(
            bridge, "/api/messages/delete", method="POST", body={"id": 1}
        )
        assert status == 200
    finally:
        bridge.stop()

    bridge2 = _bridge_with_data_dir(FakeNode(), tmp_path)
    try:
        messages = bridge2.messages()
        assert [message["id"] for message in messages] == [2]
        assert messages[0]["text"] == "still here"
    finally:
        bridge2.stop()


def test_delete_message_rejects_queued_entry(bridge, node):
    _queue_send(node)
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    assert status == 200
    assert payload["message"]["status"] == "queued"

    status, _, payload = _json(
        bridge, "/api/messages/delete", method="POST", body={"id": 1}
    )
    assert status == 400
    assert "still queued" in payload["error"]["message"]
    _, _, feed = _json(bridge, "/api/messages")
    assert len(feed["messages"]) == 1


def _send_for_edit(bridge, node, *, text="typo here"):
    """A delivered outbound console entry, with its inbox record, ready to edit."""
    message, _ = bridge.send_message("alice", text)
    node.inbox.append(
        {
            "id": message["wire_id"],
            "destination": PEER_HASH,
            "from": "tester",
            "text": text,
            "sent_at": 100.0,
            "received_at": 101.0,
            "direction": "out",
        }
    )
    return message


def _stub_send_edit(node, *, edited=True, edited_at=200.0):
    """Stand in for the node's live-link edit, mirroring its local recording."""
    calls = []

    def send_edit(peer, message_id, text, *, timeout=20.0, discover_timeout=None):
        calls.append((peer, message_id, text, timeout))
        if edited:
            node.inbox.apply_edit(
                message_id, text, edited_at=edited_at, newer_than_existing=True
            )
        return {
            "ok": True,
            "target": message_id,
            "edited": edited,
            "received_at": 201.0,
            "id": "edit-1",
            "sent_at": edited_at,
        }

    node.send_edit = send_edit
    return calls


def test_edit_message_endpoint_updates_the_bubble(bridge, node):
    from tau.protocol import excerpt_hash

    message = _send_for_edit(bridge, node)
    calls = _stub_send_edit(node)
    wire_id = message["wire_id"]

    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": message["id"], "text": "fixed here"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["edited"] is True
    assert payload["id"] == "edit-1"
    assert payload["message"]["id"] == message["id"]
    assert payload["message"]["text"] == "fixed here"
    assert payload["message"]["edited_at"] == 200.0
    assert payload["message"]["excerpt_hash"] == excerpt_hash("fixed here")
    assert calls == [(PEER_HASH, wire_id, "fixed here", 20.0)]

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["text"] == "fixed here"
    assert feed["messages"][0]["edited_at"] == 200.0
    # The node inbox copy carries the edit too (the stub mirrors
    # send_edit), so `tau messages` agrees with the console.
    assert node.inbox.list_messages(direction="out")[0]["text"] == "fixed here"
    updated_events = [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_updated"
    ]
    assert len(updated_events) == 1
    assert updated_events[0]["message"]["text"] == "fixed here"


def test_edit_message_endpoint_leaves_the_bubble_when_the_peer_has_no_copy(
    bridge, node
):
    message = _send_for_edit(bridge, node)
    _stub_send_edit(node, edited=False)

    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": message["id"], "text": "fixed here"},
    )
    assert status == 200
    assert payload["edited"] is False
    assert payload["message"] is None
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["text"] == "typo here"
    assert "edited_at" not in feed["messages"][0]
    assert node.inbox.list_messages(direction="out")[0]["text"] == "typo here"
    assert not [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_updated"
    ]


def test_edit_message_endpoint_rejects_guards(bridge, node):
    message = _send_for_edit(bridge, node)
    _stub_send_edit(node)

    # Unknown console id.
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": 99, "text": "fixed"},
    )
    assert status == 400
    assert "unknown message" in payload["error"]["message"]

    # A received message is not the node's to edit.
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-in",
        }
    )
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": 2, "text": "fixed"},
    )
    assert status == 400
    assert "only your own sent" in payload["error"]["message"]

    # Non-string and empty replacement text.
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": message["id"], "text": 42},
    )
    assert status == 400
    assert "must be a string" in payload["error"]["message"]
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": message["id"], "text": "   "},
    )
    assert status == 400
    assert "must not be empty" in payload["error"]["message"]

    # A group message under an unknown group is refused by the group
    # path (the fan-out itself is covered by the group-edit tests).
    bridge._append_message(
        direction="out",
        peer=PEER_HASH,
        peer_name="alice",
        text="group typo",
        sent_at=100.0,
        received_at=101.0,
        status="delivered",
        wire_id="wire-group",
        group="crew",
        group_name="crew",
    )
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": 3, "text": "fixed"},
    )
    assert status == 400
    assert "unknown group" in payload["error"]["message"]

    # A file record has no text to edit.
    bridge._append_message(
        direction="out",
        peer=PEER_HASH,
        peer_name="alice",
        text="",
        sent_at=100.0,
        received_at=101.0,
        status="delivered",
        wire_id="wire-file",
        file={
            "name": "notes.txt",
            "size": 4,
            "sha256": "ab" * 32,
            "kind": "file",
        },
    )
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": 4, "text": "fixed"},
    )
    assert status == 400
    assert "file transfers cannot be edited" in payload["error"]["message"]


def test_edit_message_endpoint_rejects_a_queued_entry(bridge, node):
    _queue_send(node)
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    assert status == 200
    assert payload["message"]["status"] == "queued"
    _stub_send_edit(node)

    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": 1, "text": "fixed"},
    )
    assert status == 400
    assert "still queued" in payload["error"]["message"]


def test_edit_message_endpoint_rejects_a_message_still_in_the_outbox(
    bridge, node
):
    # The bubble says delivered, but the mailbox still holds the send:
    # an edit answered "not found" would let the queued copy arrive
    # later with the old text.
    message = _send_for_edit(bridge, node)
    node.mailbox.enqueue(
        to=bytes.fromhex(PEER_HASH),
        text="typo here",
        message_id=message["wire_id"],
    )
    _stub_send_edit(node)

    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": message["id"], "text": "fixed"},
    )
    assert status == 400
    assert "still queued" in payload["error"]["message"]


def test_edit_message_endpoint_keeps_the_bubble_when_the_peer_is_unreachable(
    bridge, node
):
    message = _send_for_edit(bridge, node)

    def send_edit(peer, message_id, text, **kwargs):
        raise NodeError("peer is unreachable")

    node.send_edit = send_edit

    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": message["id"], "text": "fixed here"},
    )
    assert status == 502
    assert "unreachable" in payload["error"]["message"]
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["text"] == "typo here"


def test_edit_message_endpoint_reports_queued_edit(bridge, node):
    message = _send_for_edit(bridge, node)

    def send_edit(peer, message_id, text, **kwargs):
        return {
            "ok": False,
            "queued": True,
            "target": message_id,
            "id": "edit-1",
            "queued_id": "q-1",
            "sent_at": 200.0,
        }

    node.send_edit = send_edit

    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": message["id"], "text": "fixed here"},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["edited"] is False
    assert payload["queued"] is True
    assert payload["queued_id"] == "q-1"
    assert payload["id"] == "edit-1"
    assert payload["message"] is None
    # The bubble keeps the old text until the sweep delivers the edit.
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["text"] == "typo here"
    assert "edited_at" not in feed["messages"][0]
    assert not [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_updated"
    ]


def test_record_edit_applies_an_inbound_edit(bridge):
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "typo here",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )
    updated = bridge.record_edit(
        {
            "target": "wire-1",
            "text": "fixed here",
            "edited_at": 200.0,
            "from": "alice",
            "destination": PEER_HASH,
        }
    )
    assert updated is not None
    assert updated["text"] == "fixed here"
    assert updated["edited_at"] == 200.0
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["text"] == "fixed here"
    assert feed["messages"][0]["edited_at"] == 200.0
    updated_events = [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_updated"
    ]
    assert len(updated_events) == 1

    # A replayed older edit must not rewind the newer one.
    assert (
        bridge.record_edit(
            {
                "target": "wire-1",
                "text": "older text",
                "edited_at": 150.0,
                "from": "alice",
                "destination": PEER_HASH,
            }
        )
        is None
    )
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["text"] == "fixed here"

    # An edit for a message outside this console's history, and a
    # malformed record, are dropped quietly.
    assert (
        bridge.record_edit(
            {
                "target": "wire-missing",
                "text": "fixed",
                "edited_at": 300.0,
                "from": "alice",
                "destination": PEER_HASH,
            }
        )
        is None
    )
    assert (
        bridge.record_edit({"target": "wire-1", "edited_at": 300.0}) is None
    )


def test_record_edit_keeps_cli_made_reactions(bridge, node):
    # A reaction made from the CLI lives only in the shared inbox; the
    # edit's message_updated event must carry it, or the open tab loses
    # the reaction until reload.
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "typo here",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )
    node.inbox.record_reaction(
        {
            "target": "wire-1",
            "destination": BOB_HASH,
            "reaction": "👍",
            "from": "bob",
            "sent_at": 150.0,
            "received_at": 151.0,
            "id": "react-1",
        }
    )
    updated = bridge.record_edit(
        {
            "target": "wire-1",
            "text": "fixed here",
            "edited_at": 200.0,
            "from": "alice",
            "destination": PEER_HASH,
        }
    )
    assert updated is not None
    assert updated["reactions"] == [
        {"reactor": BOB_HASH, "name": "bob", "reaction": "👍"}
    ]
    updated_events = [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_updated"
    ]
    assert len(updated_events) == 1
    assert updated_events[0]["message"]["reactions"] == updated["reactions"]


def test_record_edit_does_not_publish_a_blocked_senders_edit(bridge, node):
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "typo here",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )
    # The edit was accepted, then the sender was blocked before the
    # bridge published it: the stored copy keeps the new text, but no
    # live tab learns it while the block stands.
    node.blocklist = BlockList(blocked=frozenset({PEER_HASH}))
    updated = bridge.record_edit(
        {
            "target": "wire-1",
            "text": "fixed here",
            "edited_at": 200.0,
            "from": "alice",
            "destination": PEER_HASH,
        }
    )
    assert updated is not None
    assert updated["text"] == "fixed here"
    assert not [
        event
        for event in bridge.bus.replay()
        if event.get("type") == "message_updated"
    ]
    # Unblocking restores the updated history.
    node.blocklist = BlockList()
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["text"] == "fixed here"


def test_edit_survives_bridge_restart(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        message = _send_for_edit(bridge, node)
        _stub_send_edit(node)
        status, _, payload = _json(
            bridge,
            "/api/messages/edit",
            method="POST",
            body={"id": message["id"], "text": "fixed here"},
        )
        assert status == 200
        assert payload["edited"] is True
    finally:
        bridge.stop()

    bridge2 = _bridge_with_data_dir(FakeNode(), tmp_path)
    try:
        messages = bridge2.messages()
        assert len(messages) == 1
        assert messages[0]["text"] == "fixed here"
        assert messages[0]["edited_at"] == 200.0
    finally:
        bridge2.stop()


def test_edit_message_endpoint_adopts_a_newer_inbox_edit(bridge, node):
    # A newer edit already landed in the inbox (e.g. the peer's own
    # re-send raced this one): the console shows the newest text, not
    # the older text this request carried.
    message = _send_for_edit(bridge, node)
    node.inbox.apply_edit(
        message["wire_id"],
        "newer text",
        edited_at=250.0,
        newer_than_existing=True,
    )
    _stub_send_edit(node, edited_at=200.0)

    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": message["id"], "text": "fixed here"},
    )
    assert status == 200
    assert payload["edited"] is True
    assert payload["message"]["text"] == "newer text"
    assert payload["message"]["edited_at"] == 250.0


def test_edit_message_endpoint_ignores_an_inbound_record_with_the_same_wire_id(
    bridge, node
):
    # The sent inbox copy was pruned, and a peer's inbound message
    # reused the wire id and carries a newer edit.  The console must
    # show this request's text, not the peer's: the inbox copy only
    # speaks for the edit when it is our own sent copy to this peer.
    message, _ = bridge.send_message("alice", "typo here")
    node.inbox.append(
        {
            "id": message["wire_id"],
            "destination": PEER_HASH,
            "from": PEER_HASH,
            "text": "peer text",
            "sent_at": 100.0,
            "received_at": 101.0,
            "direction": "in",
        }
    )
    node.inbox.apply_edit(
        message["wire_id"],
        "peer edit",
        edited_at=250.0,
        newer_than_existing=True,
    )
    _stub_send_edit(node, edited_at=200.0)

    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": message["id"], "text": "fixed here"},
    )
    assert status == 200
    assert payload["edited"] is True
    assert payload["message"]["text"] == "fixed here"
    assert payload["message"]["edited_at"] == 200.0


def test_edit_message_endpoint_uses_the_signed_time_without_an_inbox_copy(
    bridge, node
):
    # The inbox record is gone (pruned since the send): the console
    # still shows the edit, ordered by the edit's signed time.
    message, _ = bridge.send_message("alice", "typo here")
    _stub_send_edit(node, edited_at=200.0)

    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": message["id"], "text": "fixed here"},
    )
    assert status == 200
    assert payload["edited"] is True
    assert payload["message"]["text"] == "fixed here"
    assert payload["message"]["edited_at"] == 200.0


def test_record_edit_is_scoped_to_direction_and_peer(bridge):
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "typo here",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )
    edit = {
        "target": "wire-1",
        "text": "fixed here",
        "edited_at": 200.0,
        "from": "alice",
        "destination": PEER_HASH,
    }
    # A local edit (direction out) may not rewrite an inbound entry.
    assert bridge.record_edit({**edit, "direction": "out"}) is None
    # Neither may an edit naming a different peer, or no peer at all:
    # a wire id alone must not let one peer rewrite another's message.
    assert (
        bridge.record_edit({**edit, "destination": "ff" * 16}) is None
    )
    assert bridge.record_edit({k: v for k, v in edit.items() if k != "destination"}) is None
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["text"] == "typo here"
    # The correctly scoped edit still lands.
    assert bridge.record_edit(edit) is not None
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["text"] == "fixed here"


def test_record_edit_buffers_an_edit_for_a_message_not_yet_appended(bridge):
    # The edit overtook the callback that appends its target: it is
    # buffered, not dropped, and lands when the entry arrives.
    assert (
        bridge.record_edit(
            {
                "target": "wire-9",
                "text": "fixed here",
                "edited_at": 200.0,
                "from": "alice",
                "destination": PEER_HASH,
            }
        )
        is None
    )
    # An unrelated message does not consume the buffered edit.
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "other",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-8",
        }
    )
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["text"] == "other"
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "typo here",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-9",
        }
    )
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][1]["text"] == "fixed here"
    assert feed["messages"][1]["edited_at"] == 200.0


def test_delete_message_fails_when_history_cannot_persist(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        node.inbox.append(
            {
                "id": "wire-1",
                "destination": PEER_HASH,
                "from": "alice",
                "text": "hello",
                "sent_at": 100.0,
                "received_at": 101.0,
            }
        )
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "hello",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "wire-1",
            }
        )
        # A directory in the history file's place makes the atomic
        # replace fail, so the strict save raises.
        messages_path = tmp_path / "messages.json"
        messages_path.unlink()
        messages_path.mkdir()
        status, _, payload = _json(
            bridge, "/api/messages/delete", method="POST", body={"id": 1}
        )
        assert status == 500
        _, _, feed = _json(bridge, "/api/messages")
        assert len(feed["messages"]) == 1
        # Inbox-first ordering: the inbox copy is already gone, but the
        # bubble stays visible, so a retry once the file is repaired
        # completes the delete (the inbox delete of the already-removed
        # wire id is ignored).
        assert node.inbox.export_document()["messages"] == []
        messages_path.rmdir()
        status, _, payload = _json(
            bridge, "/api/messages/delete", method="POST", body={"id": 1}
        )
        assert status == 200
        _, _, feed = _json(bridge, "/api/messages")
        assert feed["messages"] == []
        assert node.inbox.export_document()["messages"] == []
    finally:
        bridge.stop()


def test_delete_message_rollback_survives_append_to_full_log(
    node, tmp_path, monkeypatch
):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        # Fill the bounded log (500 entries) with console-only messages.
        for i in range(1, 501):
            bridge._messages.append(
                {
                    "id": i,
                    "direction": "inbound",
                    "peer": PEER_HASH,
                    "peer_name": "alice",
                    "text": f"m{i}",
                    "sent_at": float(i),
                    "received_at": float(i),
                    "status": "received",
                }
            )
        bridge._next_message_id = 501

        # A message arriving while the strict save is in flight refills
        # the log, then the write fails. The rollback must restore the
        # deleted entry instead of raising on the full deque.
        def _append_then_fail(*args, **kwargs):
            bridge._messages.append(
                {
                    "id": 501,
                    "direction": "inbound",
                    "peer": PEER_HASH,
                    "peer_name": "alice",
                    "text": "m501",
                    "sent_at": 501.0,
                    "received_at": 501.0,
                    "status": "received",
                }
            )
            raise OSError("disk full")

        monkeypatch.setattr(
            "tau.webapi.atomic_write_json", _append_then_fail
        )
        status, _, _ = _json(
            bridge, "/api/messages/delete", method="POST", body={"id": 250}
        )
        assert status == 500
        # The feed API pages the log, so check the deque itself: the
        # restore must have landed despite the full log.
        assert len(bridge._messages) == 500
        ids = [message["id"] for message in bridge._messages]
        assert 250 in ids  # the deleted entry was restored
        assert 501 in ids  # the interleaved append survived
        assert ids == sorted(ids)  # the restore kept id order
    finally:
        bridge.stop()


def test_delete_sent_group_rollback_keeps_all_legs_on_full_log(
    node, tmp_path, monkeypatch
):
    """Rolling back a group delete never evicts a restored leg.

    The group legs are the oldest entries in a full log, and an append
    refills the log while the strict write is in flight. Every leg must
    come back, not just the last one restored.
    """
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        group = "ab" * 16
        for wire_id, peer in (("leg-1", PEER_HASH), ("leg-2", "ef" * 16)):
            bridge._append_message(
                direction="out",
                peer=peer,
                peer_name="peer",
                text="group hello",
                sent_at=100.0,
                received_at=101.0,
                status="delivered",
                group=group,
                wire_id=wire_id,
            )
        leg_ids = [
            message["id"]
            for message in bridge._messages
            if message.get("group") == group
        ]
        # Fill the rest of the bounded log with console-only messages.
        next_id = max(leg_ids) + 1
        while len(bridge._messages) < bridge._messages.maxlen:
            bridge._messages.append(
                {
                    "id": next_id,
                    "direction": "inbound",
                    "peer": PEER_HASH,
                    "peer_name": "alice",
                    "text": f"m{next_id}",
                    "sent_at": float(next_id),
                    "received_at": float(next_id),
                    "status": "received",
                }
            )
            next_id += 1
        bridge._next_message_id = next_id

        def _append_then_fail(*args, **kwargs):
            # Two appends refill the log during the failing write.
            for _ in range(2):
                bridge._messages.append(
                    {
                        "id": bridge._next_message_id,
                        "direction": "inbound",
                        "peer": PEER_HASH,
                        "peer_name": "alice",
                        "text": "late",
                        "sent_at": 900.0,
                        "received_at": 900.0,
                        "status": "received",
                    }
                )
                bridge._next_message_id += 1
            raise OSError("disk full")

        monkeypatch.setattr(
            "tau.webapi.atomic_write_json", _append_then_fail
        )
        with pytest.raises(OSError, match="disk full"):
            bridge.delete_sent_group(group, 100.0)
        restored = [m["id"] for m in bridge._messages if m.get("group") == group]
        assert restored == leg_ids
        ids = [message["id"] for message in bridge._messages]
        assert ids == sorted(ids)
        assert len(ids) == bridge._messages.maxlen
    finally:
        bridge.stop()


def test_reaction_annotates_matching_history_entry(bridge):
    record = {
        "from": "alice",
        "destination": PEER_HASH,
        "text": "hello",
        "sent_at": 100.0,
        "received_at": 101.0,
        "id": "wire-1",
    }
    bridge.record_inbound(record)

    updated = bridge.record_reaction(_reaction_record())
    assert updated is not None
    assert updated["reactions"] == [
        {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
    ]

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["reactions"] == [
        {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
    ]
    assert feed["messages"][0]["wire_id"] == "wire-1"
    events = bridge.bus.replay(0)
    assert any(
        e["type"] == "message_updated"
        and e["message"]["reactions"]
        for e in events
    )


def test_reaction_latest_per_reactor_and_clear(bridge):
    bridge.record_inbound(
        {
            "from": "alice",
            "destination": PEER_HASH,
            "text": "hello",
            "sent_at": 100.0,
            "received_at": 101.0,
            "id": "wire-1",
        }
    )
    bridge.record_reaction(_reaction_record(reaction="👍"))
    bridge.record_reaction(_reaction_record(reaction="❤️", id="react-2"))
    updated = bridge.record_reaction(_reaction_record(reaction="", id="react-3"))
    assert updated is not None
    assert updated["reactions"] == []


def test_reaction_to_unknown_history_entry_is_dropped(bridge):
    assert bridge.record_reaction(_reaction_record()) is None


def test_reactions_and_wire_id_survive_bridge_restart(node, tmp_path):
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        bridge.record_inbound(
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "hello",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "wire-1",
            }
        )
        bridge.record_reaction(_reaction_record())
    finally:
        bridge.stop()

    bridge2 = _bridge_with_data_dir(FakeNode(), tmp_path)
    try:
        messages = bridge2.messages()
        assert len(messages) == 1
        assert messages[0]["wire_id"] == "wire-1"
        assert messages[0]["reactions"] == [
            {"reactor": PEER_HASH, "name": "alice", "reaction": "👍"}
        ]
    finally:
        bridge2.stop()


def test_send_message_records_wire_id(node, tmp_path):
    def send_with_id(peer, text, *, timeout=20.0, discover_timeout=None, in_reply_to=None, expires_at=None, send_after=None, **kwargs):
        return {"ok": True, "echo": text, "server": "tester", "id": "wire-9"}

    node.send_message = send_with_id
    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        message, _ = bridge.send_message("alice", "hi")
        assert message["wire_id"] == "wire-9"
    finally:
        bridge.stop()


def _reaction_record(**overrides):
    record = {
        "target": "wire-1",
        "reaction": "👍",
        "from": "alice",
        "destination": PEER_HASH,
        "sent_at": 12.0,
        "received_at": 13.0,
        "id": "react-1",
    }
    record.update(overrides)
    return record


def test_expired_history_is_purged_on_load(node, tmp_path):
    now = time.time()
    gone = _stored_message(1, "gone")
    gone["expires_at"] = now - 10
    staying = _stored_message(2, "staying")
    staying["expires_at"] = now + 3600
    (tmp_path / "messages.json").write_text(
        json.dumps(
            {
                "messages": [gone, staying, _stored_message(3, "plain")],
                "next_message_id": 4,
            }
        ),
        encoding="utf-8",
    )

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        assert [m["text"] for m in bridge.messages(limit=0)] == [
            "staying",
            "plain",
        ]
        # The purge is persisted: the expired entry is gone from disk.
        stored = json.loads((tmp_path / "messages.json").read_text(encoding="utf-8"))
        assert [m["text"] for m in stored["messages"]] == ["staying", "plain"]
    finally:
        bridge.stop()


def test_inbound_message_keeps_expiry(bridge):
    record = {
        "from": "bob",
        "destination": "ef" * 16,
        "text": "read fast",
        "sent_at": 100.0,
        "received_at": 101.0,
        "expires_at": 1893456000.0,
    }
    bridge.record_inbound(record)

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["expires_at"] == 1893456000.0


def test_messages_filters_expired_at_read_time(bridge):
    now = time.time()
    bridge._append_message(
        direction="in",
        peer="ef" * 16,
        peer_name="bob",
        text="gone",
        sent_at=now - 100,
        received_at=now - 100,
        status="received",
        expires_at=now - 10,
    )
    bridge._append_message(
        direction="in",
        peer="ef" * 16,
        peer_name="bob",
        text="staying",
        sent_at=now,
        received_at=now,
        status="received",
        expires_at=now + 3600,
    )
    # The deadline passed while the bridge was running: the read path
    # drops it without waiting for a restart.
    assert [m["text"] for m in bridge.messages(limit=0)] == ["staying"]


def test_send_rejects_bad_expires_in(bridge):
    for bad in (0, -5, "soon", True, 31 * 86400, 10**400):
        status, _, payload = _json(
            bridge,
            "/api/messages",
            method="POST",
            body={"to": "alice", "text": "hi", "expires_in": bad},
        )
        assert status == 400
        assert "expires_in" in payload["error"]["message"]


def test_send_requires_a_target(bridge):
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"text": "hi"}
    )
    assert status == 400
    assert "to" in payload["error"]["message"]


def test_send_with_expires_in_records_deadline(bridge, node):
    before = time.time()
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "vanish soon", "expires_in": 3600},
    )
    assert status == 200
    expires_at = payload["message"]["expires_at"]
    assert before + 3590 <= expires_at <= time.time() + 3610
    assert node.last_expires_at == expires_at

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["expires_at"] == expires_at


def test_send_rejects_bad_send_in(bridge):
    for bad in (0, -5, "soon", True, 30 * 86400, 10**400):
        status, _, payload = _json(
            bridge,
            "/api/messages",
            method="POST",
            body={"to": "alice", "text": "hi", "send_in": bad},
        )
        assert status == 400
        assert "send_in" in payload["error"]["message"]


def test_send_with_send_in_records_schedule(bridge, node):
    _queue_send(node)
    before = time.time()
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "later", "send_in": 3600},
    )
    assert status == 200
    send_after = payload["message"]["send_after"]
    assert before + 3590 <= send_after <= time.time() + 3610
    assert node.last_send_kwargs["send_after"] == send_after
    assert payload["message"]["status"] == "queued"

    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["send_after"] == send_after


def test_send_rejects_send_in_past_expiry(bridge):
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "hi", "send_in": 7200, "expires_in": 3600},
    )
    assert status == 400
    assert "send_in" in payload["error"]["message"]


def test_send_maps_mailbox_error_to_503(bridge, node, monkeypatch):
    from tau.mailbox import MailboxFull

    def _full(peer, text, **kwargs):
        raise MailboxFull("outbox is full (10 messages)")

    monkeypatch.setattr(node, "send_message", _full)
    status, _, payload = _json(
        bridge, "/api/messages", method="POST", body={"to": "alice", "text": "hi"}
    )
    assert status == 503
    assert "outbox is full" in payload["error"]["message"]


# -- sneakernet bundles ----------------------------------------------------


def _bundle_with_held_mail(tmp_path, *, now=None):
    """A signed bundle holding one sealed envelope for the fake peer."""
    from tau.bundle import build_bundle
    from tau.identity import destination_hash
    from tau.mailbox import seal_message
    from tau.peers import PeerRegistry

    sender = RNS.Identity()
    recipient = RNS.Identity()
    mailbox = Mailbox(tmp_path / "sender-mailbox.json")
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="held-1",
        text="held hello",
        sent_at=1000.0,
    )
    moment = time.time() if now is None else now
    mailbox.hold(
        to=destination_hash(recipient),
        blob=blob,
        message_id="held-1",
        expires_at=moment + 7 * 24 * 3600,
        now=moment,
    )
    document, summary = build_bundle(
        identity=sender,
        sender_name="alice",
        mailbox=mailbox,
        registry=PeerRegistry(),
        tan_conn=None,
        include_tan=False,
        now=moment,
    )
    assert summary["mail"] == 1
    return document


def _post_bundle(bridge, document):
    """Post a bundle document as its serialised bytes, like the console."""
    from tau.bundle import serialize_bundle

    return _json(
        bridge, "/api/bundle/import", method="POST", raw=serialize_bundle(document)
    )


def test_bundle_export_empty(bridge):
    status, _, payload = _json(
        bridge, "/api/bundle/export", method="POST", body={}
    )
    assert status == 200
    assert payload["ok"] is True
    # The export returns the server's serialised bundle text verbatim,
    # so the console downloads it without a parse round trip.
    assert isinstance(payload["bundle"], str)
    document = json.loads(payload["bundle"])
    assert document["format"] == "tau-bundle"
    assert document["sig"]
    assert payload["summary"] == {
        "mail": 0,
        "tan": 0,
        "skipped": [],
        "bytes": payload["summary"]["bytes"],
    }


def test_bundle_export_rejects_non_boolean_include_tan(bridge):
    status, _, payload = _json(
        bridge, "/api/bundle/export", method="POST", body={"include_tan": "yes"}
    )
    assert status == 400
    assert "include_tan" in payload["error"]["message"]


def test_bundle_export_includes_held_mail(bridge, node, tmp_path):
    node.mailbox.hold(
        to=bytes.fromhex(OTHER_HASH),
        blob=b"sealed-bytes",
        message_id="held-9",
        expires_at=time.time() + 3600,
        now=time.time(),
    )
    status, _, payload = _json(
        bridge, "/api/bundle/export", method="POST", body={}
    )
    assert status == 200
    assert payload["summary"]["mail"] == 1
    document = json.loads(payload["bundle"])
    assert document["mail"][0]["id"] == "held-9"
    assert document["mail"][0]["to"] == OTHER_HASH


def test_bundle_export_sees_mail_queued_by_another_process(bridge, node, tmp_path):
    from tau.identity import destination_hash
    from tau.mailbox import Mailbox

    # The serving node's in-memory snapshot predates the other process,
    # and the fake peer's key cannot seal to — so give the registry a
    # peer with a real key first, then queue through a second mailbox.
    recipient = RNS.Identity()
    node.registry._peers.append(
        Peer(
            destination_hash=destination_hash(recipient),
            name="carol",
            public_key=recipient.get_public_key(),
            first_seen=1000.0,
            last_seen=time.time(),
            hops=1,
        )
    )
    other = Mailbox(tmp_path / "mailbox.json")
    entry = other.enqueue(
        to=destination_hash(recipient), text="queued by cli"
    )
    assert node.mailbox.get_outbox(entry.id) is None

    status, _, payload = _json(
        bridge, "/api/bundle/export", method="POST", body={}
    )
    assert status == 200
    assert payload["summary"]["mail"] == 1
    document = json.loads(payload["bundle"])
    assert document["mail"][0]["id"] == entry.id


def test_bundle_export_with_tan_reports_empty_store(bridge):
    status, _, payload = _json(
        bridge, "/api/bundle/export", method="POST", body={"include_tan": True}
    )
    assert status == 200
    assert payload["summary"]["tan"] == 0
    document = json.loads(payload["bundle"])
    assert document["tan"] is not None
    assert document["tan"]["entries"] == []


def test_bundle_import_roundtrip_and_dedupe(bridge, node, tmp_path):
    document = _bundle_with_held_mail(tmp_path)
    status, _, payload = _post_bundle(bridge, document)
    assert status == 200
    report = payload["import"]
    assert report["imported"] == 1
    assert report["present"] == 0
    assert report["skipped"] == 0
    assert node.mailbox.counts()["held"] == 1

    # A second import of the same bundle dedupes instead of duplicating.
    status, _, payload = _post_bundle(bridge, document)
    assert status == 200
    report = payload["import"]
    assert report["imported"] == 0
    assert report["present"] == 1
    assert node.mailbox.counts()["held"] == 1


def test_bundle_import_accepts_integral_float_timestamps(bridge, node, tmp_path):
    # A bundle built at an exactly integral epoch second carries
    # e.g. 1793000000.0 in its signed manifest. A client that parses and
    # re-stringifies the document rewrites it as 1793000000 and breaks
    # the signature — so the route verifies the submitted bytes, and
    # the console sends the file text verbatim.
    now = float(int(time.time()))
    document = _bundle_with_held_mail(tmp_path, now=now)
    assert document["manifest"]["created_at"] == now
    status, _, payload = _post_bundle(bridge, document)
    assert status == 200
    assert payload["import"]["imported"] == 1


def test_bundle_import_rejects_tampered_bundle(bridge, tmp_path):
    document = _bundle_with_held_mail(tmp_path)
    document["mail"][0]["blob"] = "AAAA" + document["mail"][0]["blob"][4:]
    status, _, payload = _post_bundle(bridge, document)
    assert status == 400
    assert "signature" in payload["error"]["message"]


def test_bundle_import_rejects_garbage(bridge):
    status, _, payload = _json(
        bridge, "/api/bundle/import", method="POST", raw=b'{"format": "nope"}'
    )
    assert status == 400
    assert "invalid bundle" in payload["error"]["message"]


def test_bundle_import_rejects_remote_client(bridge, tmp_path, monkeypatch):
    # Bulk imports are localhost-only even when the bridge is bound to a
    # remote interface: a remote client must not be able to merge a bundle
    # into this node's held mail.
    import tau.webapi as webapi

    document = _bundle_with_held_mail(tmp_path)
    monkeypatch.setattr(webapi, "_is_loopback_client", lambda client: False)
    status, _, payload = _post_bundle(bridge, document)
    assert status == 403
    assert "only accepted from localhost" in payload["error"]["message"]


def test_bundle_import_accepts_body_over_default_cap(bridge, tmp_path):
    # A bundle import carries the whole signed document, so the route
    # reads up to the 8 MiB bundle cap instead of the 32 KiB limit
    # every other route uses. Pad past 32 KiB with an unknown key,
    # which parse_bundle ignores and the signature still covers.
    document = _bundle_with_held_mail(tmp_path)
    document["padding"] = "x" * (40 * 1024)
    status, _, payload = _post_bundle(bridge, document)
    assert status == 200
    assert payload["import"]["imported"] == 1


def test_messages_export_import_via_api(bridge, node):
    _inbound_record(node)
    node.inbox.append(
        {
            "from": "tester",
            "destination": PEER_HASH,
            "text": "reply back",
            "sent_at": 200.0,
            "received_at": 201.0,
            "id": "m-2",
            "direction": "out",
        }
    )

    status, _, payload = _json(bridge, "/api/messages/export")
    assert status == 200
    assert payload["format"] == "tau-messages-export"
    assert payload["version"] == 1
    assert [entry["text"] for entry in payload["messages"]] == [
        "hello",
        "reply back",
    ]

    # A fresh node restores the whole document; the restored messages
    # show up in its console feed, then report as already present on
    # a second import — without duplicating the feed.
    node2 = FakeNode()
    bridge2 = WebBridge(node2, host="127.0.0.1", port=0)
    bridge2.start()
    try:
        _, _, document = _json(bridge, "/api/messages/export")
        status, _, payload = _json(
            bridge2, "/api/messages/import", method="POST", body=document
        )
        assert status == 200
        assert payload["import"]["imported"] == 2
        assert payload["import"]["present"] == 0
        assert payload["import"]["skipped"] == 0
        assert payload["import"]["errors"] == []
        # Restored mail advances the unread counts: the received entry
        # sits after the (absent) read cursor, so the fresh counts ride
        # along with the import result.  The restored sent reply does
        # not count — sent mail is never unread for its owner.
        assert payload["import"]["unread"]["total"] == 1
        assert payload["import"]["unread"]["per_peer"] == {PEER_HASH: 1}
        _, _, feed = _json(bridge2, "/api/messages")
        assert [m["text"] for m in feed["messages"]] == ["hello", "reply back"]
        assert feed["messages"][0]["direction"] == "in"
        assert feed["messages"][0]["peer_name"] == "alice"
        assert feed["messages"][1]["direction"] == "out"
        assert feed["messages"][1]["status"] == "delivered"
        assert feed["messages"][1]["peer_name"] == "alice"

        status, _, payload = _json(
            bridge2, "/api/messages/import", method="POST", body=document
        )
        assert status == 200
        assert payload["import"]["imported"] == 0
        assert payload["import"]["present"] == 2
        _, _, feed = _json(bridge2, "/api/messages")
        assert len(feed["messages"]) == 2

        # An export document is bigger than the 32 KiB cap for
        # ordinary actions; the import route accepts it.
        big = json.loads(json.dumps(document))
        big["messages"] = big["messages"] * 100
        assert len(json.dumps(big).encode("utf-8")) > 32 * 1024
        status, _, payload = _json(
            bridge2, "/api/messages/import", method="POST", body=big
        )
        assert status == 200
        assert payload["import"]["skipped"] == 0
    finally:
        bridge2.stop()


def test_messages_import_skips_a_retracted_message(bridge, node, tmp_path):
    # Regression test for the CodeRabbit Major: the console import
    # must apply the retraction markers like the CLI does, or an old
    # export resurrects a message its sender retracted — and mirrors
    # it into the console feed.
    _inbound_record(node)
    node.inbox.append(
        {
            "from": "tester",
            "destination": PEER_HASH,
            "text": "reply back",
            "sent_at": 200.0,
            "received_at": 201.0,
            "id": "m-2",
            "direction": "out",
        }
    )
    _, _, document = _json(bridge, "/api/messages/export")

    node2 = FakeNode(tmp_path / "mailbox2.json")
    bridge2 = WebBridge(node2, host="127.0.0.1", port=0)
    bridge2.start()
    try:
        node2.mailbox.record_retraction(PEER_HASH, "m-1")
        status, _, payload = _json(
            bridge2, "/api/messages/import", method="POST", body=document
        )
        assert status == 200
        assert payload["import"]["imported"] == 1
        assert payload["import"]["skipped"] == 1
        assert payload["import"]["errors"] == [
            {"id": "m-1", "error": "message was retracted by its sender"}
        ]
        _, _, feed = _json(bridge2, "/api/messages")
        assert [m["text"] for m in feed["messages"]] == ["reply back"]
    finally:
        bridge2.stop()


def test_messages_import_reports_failed_lineage_restore(
    bridge, node, monkeypatch
):
    # Regression test (Qodo Warning on PR #802): a group-store write
    # failure during the lineage restore must not hide behind the
    # successful message import — the console gets an explicit
    # warning to retry.
    def _boom(group_id, entries):
        raise OSError("disk full")

    monkeypatch.setattr(node.groups, "record_imported_lineage", _boom)
    crew_id = node.groups.find("crew")["id"]
    document = {
        "format": "tau-messages-export",
        "version": 1,
        "messages": [
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "crew hello",
                "sent_at": 10.0,
                "received_at": 20.0,
                "id": "m-1",
                "kind": "group",
                "group": crew_id,
            }
        ],
        "group_id": crew_id,
        "group_lineage": [
            {
                "id": "aa" * 16,
                "name": "old-crew",
                "members": [PEER_HASH, BOB_HASH],
            }
        ],
    }
    result = bridge.messages_import(document)
    assert result["imported"] == 1
    assert result["skipped"] == 0
    # The console key follows the Fixer's already-pushed
    # `group_lineage_error` (same report, different name).
    assert result["group_lineage_error"] == (
        "messages were imported, but group lineage could not "
        "be recorded; retry the import to restore it"
    )
    assert "group_lineage_recorded" not in result


# -- received-file downloads -------------------------------------------------

SENDER_HASH = "ab" * 16


def _receive_file(node, tmp_path, *, sender, name, data, size=None, sha256=None, path=None):
    """Stage *data* as a file received from *sender* and record it."""
    from tau.config import TauConfig

    files_dir = node.config.files_dir
    sender_dir = files_dir / sender
    sender_dir.mkdir(parents=True, exist_ok=True)
    target = sender_dir / name
    target.write_bytes(data)
    record = {
        "kind": "file",
        "from": "alice",
        "destination": sender,
        "name": name,
        "size": len(data) if size is None else size,
        "sha256": hashlib.sha256(data).hexdigest() if sha256 is None else sha256,
        "path": str(target) if path is None else path,
        "sent_at": 1000.0,
        "received_at": 1001.0,
        "id": "00" * 16,
    }
    node.inbox.append(record)
    return record


def _with_config(node, tmp_path):
    from tau.config import TauConfig

    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")


def test_download_received_file(bridge, node, tmp_path):
    _with_config(node, tmp_path)
    data = b"hello file"
    _receive_file(node, tmp_path, sender=SENDER_HASH, name="notes.txt", data=data)
    status, headers, body = _request(
        bridge, f"/api/files/{SENDER_HASH}/notes.txt"
    )
    assert status == 200
    assert body == data
    assert headers["Content-Type"] == "application/octet-stream"
    assert headers["Content-Disposition"].startswith(
        'attachment; filename="notes.txt"'
    )
    assert headers["Content-Length"] == str(len(data))
    # A destination hash is case-insensitive: an uppercase sender is
    # normalised to the same hash, not rejected.
    status, _, body = _request(bridge, f"/api/files/{SENDER_HASH.upper()}/notes.txt")
    assert status == 200
    assert body == data


def test_download_response_is_not_followed_by_a_second_response(
    bridge, node, tmp_path
):
    data = b"hello file"
    _with_config(node, tmp_path)
    _receive_file(node, tmp_path, sender=SENDER_HASH, name="notes.txt", data=data)
    connection = http.client.HTTPConnection("127.0.0.1", bridge.port, timeout=5)
    try:
        connection.request("GET", f"/api/files/{SENDER_HASH}/notes.txt")
        response = connection.getresponse()
        assert response.status == 200
        assert response.read() == data
        # The download must end the response: a second request on the
        # same keep-alive connection has to get a clean answer, not
        # the stray bytes of a second response written onto the socket.
        connection.request("GET", "/api/status")
        second = connection.getresponse()
        assert second.status == 200
        second.read()
    finally:
        connection.close()


def test_download_received_file_unknown_is_404(bridge, node, tmp_path):
    _with_config(node, tmp_path)
    status, _, payload = _json(bridge, f"/api/files/{SENDER_HASH}/missing.txt")
    assert status == 404
    assert "no such received file" in payload["error"]["message"]
    status, _, _ = _json(bridge, f"/api/files/{'cd' * 16}/notes.txt")
    assert status == 404
    # Wrong shape: no name, or an extra segment, is a 404 too.
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}")
    assert status == 404
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}/a/b.txt")
    assert status == 404


def test_tan_media_serves_cached_image(bridge, node, tmp_path):
    import hashlib

    from tau.tangents.tan import create_post, init_db, store_media

    _with_config(node, tmp_path)
    data = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
    digest = hashlib.sha256(data).hexdigest()
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(
            conn,
            identity,
            "pic",
            name="alice",
            now=1000,
            image={
                "sha256": digest,
                "size": len(data),
                "mime": "image/png",
                "name": "photo.png",
            },
        )
    finally:
        conn.close()
    store_media(tmp_path, digest, data)
    status, headers, body = _request(bridge, f"/api/tan/media/{digest}")
    assert status == 200
    assert body == data
    assert headers["Content-Type"] == "image/png"
    assert headers["Content-Disposition"].startswith('inline; filename="photo.png"')
    assert headers["Content-Length"] == str(len(data))
    # HEAD answers the same headers without the bytes, so the
    # console's fetch poll can tell the image arrived.
    status, headers, body = _request(
        bridge, f"/api/tan/media/{digest}", method="HEAD"
    )
    assert status == 200
    assert body == b""
    assert headers["Content-Type"] == "image/png"
    assert headers["Content-Length"] == str(len(data))
    status, _, payload = _json(bridge, f"/api/tan/media/{'00' * 32}")
    assert status == 404
    assert "no such cached image" in payload["error"]["message"]


def test_tan_media_draft_image_is_loopback_only(bridge, node, tmp_path):
    """A draft's image is served to the owner's console, not to remotes.

    The digest alone is not authorization: with the draft fallback
    off — the default, which is what a non-loopback client gets —
    the bytes are not served, while the owner's console (a loopback
    client, and the explicit allow_draft call) still loads the draft
    image back for the composer.  Once the same image is published,
    it is public again: the gate only ever covers unpublished bytes.
    """
    import base64
    import hashlib

    from tau.tangents.tan import (
        create_post,
        image_attachment_from_data_url,
        init_db,
        save_draft,
    )

    _with_config(node, tmp_path)
    png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
    digest = hashlib.sha256(png).hexdigest()
    data_url = "data:image/png;base64," + base64.b64encode(png).decode("ascii")
    conn = init_db(node.config.social_db_path)
    try:
        attachment = image_attachment_from_data_url(data_url, tmp_path)
        save_draft(conn, "draft pic", image=attachment)
    finally:
        conn.close()
    assert bridge.tan_media(digest) is None
    found = bridge.tan_media(digest, allow_draft=True)
    assert found is not None
    assert found[0] == png
    assert found[1] == "image/png"
    status, _, body = _request(bridge, f"/api/tan/media/{digest}")
    assert status == 200
    assert body == png
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        create_post(
            conn, identity, "pic", name="alice", now=1000, image=attachment
        )
    finally:
        conn.close()
    found = bridge.tan_media(digest)
    assert found is not None
    assert found[0] == png


def test_trusted_proxy_forwarded_for_gates_owner_data(bridge, proxied_bridge):
    """A proxied visitor stops looking like the local console (#821).

    The test client always connects from 127.0.0.1, like a reverse
    proxy forwarding to the bridge over loopback. Without a trusted
    proxy the forwarded header is ignored — the socket peer is the
    client, as before. Once the loopback proxy is trusted, the gate
    reads the real client from X-Forwarded-For: a remote visitor gets
    the remote answer (403 on the export), the owner on loopback keeps
    the local one, a headerless loopback peer is the owner's direct
    console (a proxy always sends the header), and a spoofed loopback
    entry to the left of the proxy's own hop buys nothing — only the
    last address of the last field counts, even across fields.
    """
    # No trusted proxy: the header is ignored, the loopback peer wins.
    status, _, _ = _json(
        bridge, "/api/messages/export", headers={"X-Forwarded-For": "203.0.113.7"}
    )
    assert status == 200

    # Trusted proxy, remote visitor: owner-only data is refused.
    status, _, _ = _json(
        proxied_bridge,
        "/api/messages/export",
        headers={"X-Forwarded-For": "203.0.113.7"},
    )
    assert status == 403

    # Trusted proxy, owner on loopback: still local.
    status, _, _ = _json(
        proxied_bridge,
        "/api/messages/export",
        headers={"X-Forwarded-For": "127.0.0.1"},
    )
    assert status == 200

    # Trusted proxy, no forwarding header: the owner's direct console.
    status, _, _ = _json(proxied_bridge, "/api/messages/export")
    assert status == 200

    # Spoofed loopback entry to the left of the proxy's hop: the last
    # entry is the real client, so this is still a remote visitor.
    status, _, _ = _json(
        proxied_bridge,
        "/api/messages/export",
        headers={"X-Forwarded-For": "127.0.0.1, 203.0.113.7"},
    )
    assert status == 403

    # Spoofed first field, proxy's hop in a second field: the final
    # field's final address is the real client (Qodo Critical on #837).
    status, _ = _request_headers(
        proxied_bridge,
        "/api/messages/export",
        [
            ("X-Forwarded-For", "127.0.0.1"),
            ("X-Forwarded-For", "203.0.113.7"),
        ],
    )
    assert status == 403

    # A present-but-empty header names no client: fails closed.
    status, _ = _request_headers(
        proxied_bridge, "/api/messages/export", [("X-Forwarded-For", "")]
    )
    assert status == 403


def test_trusted_proxy_appended_forwarded_field_wins(proxied_bridge):
    """A visitor's first X-Forwarded-For field cannot spoof loopback.

    The proxy appends a second X-Forwarded-For field instead of
    extending the first; the last value of the last field is the real
    client, so a remote visitor stays remote (#821).
    """
    import http.client

    conn = http.client.HTTPConnection("127.0.0.1", proxied_bridge.port, timeout=5)
    try:
        conn.putrequest("GET", "/api/messages/export")
        conn.putheader("X-Forwarded-For", "127.0.0.1")
        conn.putheader("X-Forwarded-For", "203.0.113.7")
        conn.endheaders()
        response = conn.getresponse()
        response.read()
        assert response.status == 403
    finally:
        conn.close()


def test_trusted_proxy_draft_media(proxied_bridge, bridge, node, tmp_path):
    """A draft's image stays private behind a trusted proxy (#821).

    Same setup as test_tan_media_draft_image_is_loopback_only, but
    the request arrives through a loopback proxy: without the trust
    the digest serves (the hole), with the trust and a remote
    X-Forwarded-For it 404s, and with a loopback X-Forwarded-For the
    owner's console still loads the draft image.
    """
    import base64
    import hashlib

    from tau.tangents.tan import (
        image_attachment_from_data_url,
        init_db,
        save_draft,
    )

    _with_config(node, tmp_path)
    png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
    digest = hashlib.sha256(png).hexdigest()
    data_url = "data:image/png;base64," + base64.b64encode(png).decode("ascii")
    conn = init_db(node.config.social_db_path)
    try:
        attachment = image_attachment_from_data_url(data_url, tmp_path)
        save_draft(conn, "draft pic", image=attachment)
    finally:
        conn.close()

    # The hole: no trusted proxy, the proxied visitor reads the draft.
    status, _, body = _request(bridge, f"/api/tan/media/{digest}")
    assert status == 200
    assert body == png

    # Trusted proxy, remote visitor: the draft's bytes are not served.
    with pytest.raises(urllib.error.HTTPError) as excinfo:
        _request(
            proxied_bridge,
            f"/api/tan/media/{digest}",
            headers={"X-Forwarded-For": "203.0.113.7"},
        )
    assert excinfo.value.code == 404

    # Spoofed first field, proxy's hop in a second field: still remote.
    status, _ = _request_headers(
        proxied_bridge,
        f"/api/tan/media/{digest}",
        [
            ("X-Forwarded-For", "127.0.0.1"),
            ("X-Forwarded-For", "203.0.113.7"),
        ],
    )
    assert status == 404

    # Trusted proxy, no header: the owner's direct console still loads
    # it — a proxy always sends the header.
    status, _, body = _request(proxied_bridge, f"/api/tan/media/{digest}")
    assert status == 200
    assert body == png

    # Trusted proxy, owner on loopback: the composer still loads it.
    status, _, body = _request(
        proxied_bridge,
        f"/api/tan/media/{digest}",
        headers={"X-Forwarded-For": "127.0.0.1"},
    )
    assert status == 200
    assert body == png


def test_tan_save_draft_reattaches_image_by_descriptor(bridge, node, tmp_path):
    """A draft re-saved by descriptor keeps its image (#822).

    A remote console cannot fetch the draft image's bytes (the media
    endpoint serves an unpublished draft's bytes to loopback clients
    only), so it re-sends the ``{sha256, name}`` descriptor; the
    server re-attaches the pinned bytes by digest instead of dropping
    the image.  When the bytes are genuinely gone the save is refused
    loudly, so the stored draft — image included — survives.
    """
    import base64
    import hashlib

    import pytest

    from tau.tangents.tan import (
        PostError,
        image_attachment_from_data_url,
        image_attachment_from_descriptor,
        init_db,
        save_draft,
    )

    _with_config(node, tmp_path)
    png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
    digest = hashlib.sha256(png).hexdigest()
    data_url = "data:image/png;base64," + base64.b64encode(png).decode("ascii")
    conn = init_db(node.config.social_db_path)
    try:
        attachment = image_attachment_from_data_url(data_url, tmp_path)
        save_draft(conn, "draft pic", image=attachment)
    finally:
        conn.close()

    # The descriptor helper re-derives size and mime from the stored
    # bytes; the name rides the descriptor.
    rebuilt = image_attachment_from_descriptor(
        {"sha256": digest, "name": "pic.png"}, tmp_path
    )
    assert rebuilt == {
        "sha256": digest,
        "size": len(png),
        "mime": "image/png",
        "name": "pic.png",
    }
    # A descriptor for bytes that were never stored is refused.
    with pytest.raises(PostError, match="no longer cached"):
        image_attachment_from_descriptor(
            {"sha256": "cd" * 32, "name": "gone.png"}, tmp_path
        )
    # A malformed digest is refused, not looked up.
    with pytest.raises(PostError, match="64 character hex digest"):
        image_attachment_from_descriptor({"sha256": "zz", "name": "x.png"}, tmp_path)

    # The bridge re-saves the draft from the descriptor alone.
    saved = bridge.tan_save_draft(
        "re-saved pic", image={"sha256": digest, "name": "pic.png"}
    )
    assert saved["image"]["sha256"] == digest
    assert saved["image"]["mime"] == "image/png"
    assert saved["image"]["name"] == "pic.png"
    # ...and the HTTP route accepts the descriptor form too, and
    # refuses genuinely missing bytes instead of dropping the image.
    status, _, payload = _json(
        bridge,
        "/api/tan/drafts",
        method="POST",
        body={"text": "via http", "image": {"sha256": digest, "name": "pic.png"}},
    )
    assert status == 200
    assert payload["draft"]["image"]["sha256"] == digest
    status, _, payload = _json(
        bridge,
        "/api/tan/drafts",
        method="POST",
        body={
            "text": "gone bytes",
            "image": {"sha256": "cd" * 32, "name": "gone.png"},
        },
    )
    assert status == 400
    assert "no longer cached" in payload["error"]["message"]
    # A non-string, non-object image is still a 400 at the route.
    status, _, payload = _json(
        bridge,
        "/api/tan/drafts",
        method="POST",
        body={"text": "bad image", "image": 42},
    )
    assert status == 400


def test_image_attachment_from_upload_refuses_descriptor_when_remote(tmp_path):
    """A descriptor is refused when the caller may not use one (#836).

    The upload helper is the single choke point for descriptor
    images: with ``allow_descriptor`` false, a ``{sha256, name}``
    descriptor is refused before the store is even consulted, while a
    data URL carrying fresh bytes still decodes.
    """
    import base64
    import hashlib

    import pytest

    from tau.tangents.tan import PostError, image_attachment_from_upload

    png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
    digest = hashlib.sha256(png).hexdigest()
    data_url = "data:image/png;base64," + base64.b64encode(png).decode("ascii")
    image_attachment_from_upload(data_url, tmp_path)
    with pytest.raises(PostError, match="local client"):
        image_attachment_from_upload(
            {"sha256": digest, "name": "pic.png"},
            tmp_path,
            allow_descriptor=False,
        )
    rebuilt = image_attachment_from_upload(
        data_url, tmp_path, allow_descriptor=False
    )
    assert rebuilt == {
        "sha256": digest,
        "size": len(png),
        "mime": "image/png",
        "name": "image.png",
    }


def test_remote_client_cannot_publish_draft_image_by_descriptor(
    proxied_bridge, node, tmp_path
):
    """A remote client cannot publish a private draft photo by digest (#836).

    The drafts listing hands a draft image's digest and name to any
    remote client, so a descriptor is no proof the caller may publish
    those bytes: only a local client may attach a draft-only image by
    reference when publishing or scheduling.  A remote client that
    holds the bytes can still publish them, and saving a draft by
    descriptor still works — the draft itself never leaves the node.
    """
    import base64
    import hashlib

    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import (
        TanApp,
        image_attachment_from_data_url,
        init_db,
        save_draft,
    )

    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(node.config.social_db_path)
    try:
        png = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
        digest = hashlib.sha256(png).hexdigest()
        attachment = image_attachment_from_data_url(
            "data:image/png;base64," + base64.b64encode(png).decode("ascii"),
            tmp_path,
        )
        save_draft(conn, "draft pic", image=attachment)
    finally:
        conn.close()
    node.app = TanApp(node.config, identity, name="tester", gossip=False)

    descriptor = {"sha256": digest, "name": "pic.png"}
    remote = {"X-Forwarded-For": "203.0.113.7"}

    # The attack from the issue: digest + name from the drafts
    # listing, submitted to /api/tan/posts by a remote client.
    status, _, payload = _json(
        proxied_bridge,
        "/api/tan/posts",
        method="POST",
        body={"text": "stolen pic", "image": descriptor},
        headers=remote,
    )
    assert status == 400
    assert "local client" in payload["error"]["message"]
    # Nothing was published.
    status, _, payload = _json(proxied_bridge, "/api/tan/timeline?all=1")
    assert status == 200
    assert payload["posts"] == []

    # Scheduling the same descriptor remotely is refused the same way.
    status, _, payload = _json(
        proxied_bridge,
        "/api/tan/schedule",
        method="POST",
        body={"text": "stolen later", "image": descriptor, "send_in": 3600},
        headers=remote,
    )
    assert status == 400
    assert "local client" in payload["error"]["message"]

    # A remote client that holds the bytes can still publish them.
    data_url = "data:image/png;base64," + base64.b64encode(png).decode("ascii")
    status, _, payload = _json(
        proxied_bridge,
        "/api/tan/posts",
        method="POST",
        body={"text": "my own pic", "image": data_url},
        headers=remote,
    )
    assert status == 200
    assert payload["post"]["image"]["sha256"] == digest

    # The owner's own console — local, through the trusted proxy —
    # can still publish the draft image by descriptor.
    status, _, payload = _json(
        proxied_bridge,
        "/api/tan/posts",
        method="POST",
        body={"text": "draft pic", "image": descriptor},
    )
    assert status == 200
    assert payload["post"]["image"]["sha256"] == digest

    # And a remote console can still save a draft by descriptor: the
    # draft stays on the node, nothing is broadcast.
    status, _, payload = _json(
        proxied_bridge,
        "/api/tan/drafts",
        method="POST",
        body={"text": "remote re-save", "image": descriptor},
        headers=remote,
    )
    assert status == 200
    assert payload["draft"]["image"]["sha256"] == digest


def test_tan_media_fetch_accepts_and_validates(bridge, node, tmp_path):
    _with_config(node, tmp_path)
    digest = "ab" * 32
    status, _, payload = _json(
        bridge,
        f"/api/tan/media/{digest}/fetch",
        method="POST",
        body={"author": "cd" * 16},
    )
    assert status == 202
    assert payload["accepted"] is True
    assert payload["sha256"] == digest
    # A bad author hash is a 400, not a background fetch.
    status, _, payload = _json(
        bridge,
        f"/api/tan/media/{digest}/fetch",
        method="POST",
        body={"author": "short"},
    )
    assert status == 400
    status, _, payload = _json(
        bridge,
        f"/api/tan/media/{digest}/fetch",
        method="POST",
        body={},
    )
    assert status == 400


def test_download_received_file_rejects_bad_sender_and_name(bridge, node, tmp_path):
    _with_config(node, tmp_path)
    for path in (
        "/api/files/short/notes.txt",
        "/api/files/" + "AB" + "ab" * 31 + "/notes.txt",
        "/api/files/" + SENDER_HASH + "/..%2F..%2Fetc-passwd",
        "/api/files/" + SENDER_HASH + "/a%2Fb.txt",
    ):
        status, _, payload = _json(bridge, path)
        assert status == 400, path
        assert payload["error"]["message"]


def test_download_received_file_rejects_tampered_bytes(bridge, node, tmp_path):
    _with_config(node, tmp_path)
    data = b"hello file"
    record = _receive_file(
        node, tmp_path, sender=SENDER_HASH, name="notes.txt", data=data
    )
    # Same length, different bytes: the stored SHA-256 no longer matches.
    from pathlib import Path

    Path(record["path"]).write_bytes(b"tampered!!")
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}/notes.txt")
    assert status == 404
    # A record whose size disagrees with the bytes on disk is not served.
    _receive_file(
        node, tmp_path, sender=SENDER_HASH, name="other.txt", data=b"abc", size=99
    )
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}/other.txt")
    assert status == 404


def test_download_received_file_rejects_escaped_path(bridge, node, tmp_path):
    from pathlib import Path

    _with_config(node, tmp_path)
    data = b"outside"
    outside = tmp_path / "outside.txt"
    outside.write_bytes(data)
    # The record points outside the files directory, with a matching
    # size and hash: still not served.
    node.inbox.append(
        {
            "kind": "file",
            "from": "alice",
            "destination": SENDER_HASH,
            "name": "outside.txt",
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "path": str(outside),
            "sent_at": 1000.0,
            "received_at": 1001.0,
            "id": "11" * 16,
        }
    )
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}/outside.txt")
    assert status == 404
    # A symlink inside the sender directory pointing out is resolved
    # to the same outside path and rejected the same way.
    sender_dir = node.config.files_dir / SENDER_HASH
    sender_dir.mkdir(parents=True, exist_ok=True)
    link = sender_dir / "linked.txt"
    link.symlink_to(outside)
    node.inbox.append(
        {
            "kind": "file",
            "from": "alice",
            "destination": SENDER_HASH,
            "name": "linked.txt",
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "path": str(link),
            "sent_at": 1000.0,
            "received_at": 1001.0,
            "id": "22" * 16,
        }
    )
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}/linked.txt")
    assert status == 404


def test_record_inbound_keeps_file_metadata(bridge):
    message = bridge.record_inbound(
        {
            "destination": SENDER_HASH,
            "from": "alice",
            "text": "Received file: notes.txt (10 bytes, sha256 ab…)",
            "sent_at": 1000.0,
            "received_at": 1001.0,
            "file": {"name": "notes.txt", "size": 10, "sha256": "ab" * 32},
        }
    )
    assert message["file"] == {"name": "notes.txt", "size": 10, "sha256": "ab" * 32}
    # File metadata that is not a safe bare name is dropped, not stored.
    message = bridge.record_inbound(
        {
            "destination": SENDER_HASH,
            "from": "alice",
            "text": "Received file: x (10 bytes, sha256 ab…)",
            "sent_at": 1000.0,
            "received_at": 1001.0,
            "file": {"name": "../x", "size": 10, "sha256": "ab" * 32},
        }
    )
    assert "file" not in message


def test_stored_message_keeps_file_metadata(tmp_path):
    from tau.config import TauConfig

    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    try:
        bridge.record_inbound(
            {
                "destination": SENDER_HASH,
                "from": "alice",
                "text": "Received file: notes.txt (10 bytes, sha256 ab…)",
                "sent_at": 1000.0,
                "received_at": 1001.0,
                "file": {"name": "notes.txt", "size": 10, "sha256": "ab" * 32},
            }
        )
    finally:
        bridge.stop()
    reloaded = WebBridge(node, host="127.0.0.1", port=0)
    try:
        assert reloaded.messages()[0]["file"] == {
            "name": "notes.txt",
            "size": 10,
            "sha256": "ab" * 32,
        }
    finally:
        reloaded.stop()


def test_download_received_file_blocked_sender_is_404(bridge, node, tmp_path):
    from tau.blocklist import BlockList

    _with_config(node, tmp_path)
    _receive_file(
        node, tmp_path, sender=SENDER_HASH, name="notes.txt", data=b"hello file"
    )
    node.blocklist = BlockList(blocked=frozenset({SENDER_HASH}))
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}/notes.txt")
    assert status == 404


def test_download_received_file_symlink_loop_is_404(bridge, node, tmp_path):
    _with_config(node, tmp_path)
    sender_dir = node.config.files_dir / SENDER_HASH
    sender_dir.mkdir(parents=True, exist_ok=True)
    loop = sender_dir / "loop.txt"
    loop.symlink_to(loop)
    node.inbox.append(
        {
            "kind": "file",
            "from": "alice",
            "destination": SENDER_HASH,
            "name": "loop.txt",
            "size": 10,
            "sha256": "ab" * 32,
            "path": str(loop),
            "sent_at": 1000.0,
            "received_at": 1001.0,
            "id": "33" * 16,
        }
    )
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}/loop.txt")
    assert status == 404


def test_download_received_file_oversized_replacement_is_404(bridge, node, tmp_path):
    from pathlib import Path

    _with_config(node, tmp_path)
    record = _receive_file(
        node, tmp_path, sender=SENDER_HASH, name="notes.txt", data=b"hello file"
    )
    # The bytes are replaced by something far larger than the recorded
    # size: the download must 404 without loading the whole file.
    Path(record["path"]).write_bytes(b"x" * (1024 * 1024))
    status, _, _ = _json(bridge, f"/api/files/{SENDER_HASH}/notes.txt")
    assert status == 404


def test_send_file_is_recorded_with_file_metadata(bridge, node):
    import hashlib

    data = b"hello mesh"
    message, response = bridge.send_file("alice", "notes.txt", data)
    assert message["direction"] == "out"
    assert message["status"] == "delivered"
    assert message["text"] == "Sent file: notes.txt (10 bytes)"
    assert message["file"] == {
        "name": "notes.txt",
        "size": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    assert message["wire_id"] == response["id"]
    assert response["name"] == "notes.txt"
    assert node.sent_files == [("alice", "notes.txt", data)]
    # The feed carries the entry, and the bus tells live consoles.
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"][0]["file"]["sha256"] == hashlib.sha256(data).hexdigest()
    published = [e for e in bridge.bus.replay(0) if e.get("type") == "message"]
    assert published[-1]["message"]["id"] == message["id"]


def test_send_file_with_forwarded_from_names_the_origin(bridge, node):
    import hashlib

    data = b"hello mesh"
    forwarded_from = {"hash": "cd" * 16, "name": "carol"}
    message, _ = bridge.send_file(
        "alice", "notes.txt", data, forwarded_from=forwarded_from
    )
    assert message["text"] == (
        "Forwarded from carol:\nSent file: notes.txt (10 bytes)"
    )
    assert message["file"] == {
        "name": "notes.txt",
        "size": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "forwarded_from": forwarded_from,
    }


def test_send_file_sanitises_the_name(bridge, node):
    message, _ = bridge.send_file("alice", "../escape.txt", b"x")
    assert message["file"]["name"] == "escape.txt"
    assert node.sent_files[0][1] == "escape.txt"


def test_send_file_validation(bridge):
    from tau.protocol import MAX_FILE_BYTES

    with pytest.raises(ValueError, match="not usable"):
        bridge.send_file("alice", "..", b"x")
    with pytest.raises(ValueError, match="outside the"):
        bridge.send_file("alice", "a.txt", b"")
    with pytest.raises(ValueError, match="outside the"):
        bridge.send_file("alice", "a.txt", b"x" * (MAX_FILE_BYTES + 1))
    with pytest.raises(ValueError, match="unknown peer"):
        bridge.send_file("nobody", "a.txt", b"x")


def test_send_file_survives_bridge_restart(node, tmp_path):
    import hashlib

    bridge = _bridge_with_data_dir(node, tmp_path)
    try:
        data = b"restart me"
        bridge.send_file("alice", "keep.txt", data)
    finally:
        bridge.stop()
    stored = json.loads((tmp_path / "messages.json").read_text(encoding="utf-8"))
    assert stored["messages"][0]["file"]["name"] == "keep.txt"

    bridge2 = _bridge_with_data_dir(FakeNode(), tmp_path)
    try:
        [loaded] = bridge2.messages()
        assert loaded["file"] == {
            "name": "keep.txt",
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    finally:
        bridge2.stop()


def test_send_group_file_records_one_entry_per_leg(bridge, node):
    import hashlib

    group_id = node.groups.list_groups()[0]["id"]
    data = b"hello mesh"
    messages, response = bridge.send_group_file("crew", "notes.txt", data)
    assert response["ok"] is True
    assert node.group_sent_files == [("crew", "notes.txt", data)]
    assert len(messages) == 2
    assert all(m["direction"] == "out" for m in messages)
    assert all(m["group"] == group_id for m in messages)
    assert all(m["group_name"] == "crew" for m in messages)
    assert all(m["status"] == "delivered" for m in messages)
    assert all(m["text"] == "Sent file: notes.txt (10 bytes)" for m in messages)
    assert all(
        m["file"]
        == {
            "name": "notes.txt",
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        for m in messages
    )
    assert messages[0]["wire_id"] == "file-" + PEER_HASH[:8]
    # The feed carries both legs, and the bus tells live consoles.
    _, _, feed = _json(bridge, "/api/messages")
    assert len(feed["messages"]) == 2
    published = [e for e in bridge.bus.replay(0) if e.get("type") == "message"]
    assert [e["message"]["id"] for e in published[-2:]] == [
        m["id"] for m in messages
    ]


def test_send_group_file_failed_leg_is_recorded(bridge, node):
    node.group_refusals = {BOB_HASH}
    data = b"hello mesh"
    messages, response = bridge.send_group_file("crew", "notes.txt", data)
    assert response["ok"] is False
    assert len(messages) == 2
    by_peer = {m["peer"]: m for m in messages}
    assert by_peer[PEER_HASH]["status"] == "delivered"
    assert by_peer[PEER_HASH]["wire_id"] == "file-" + PEER_HASH[:8]
    assert by_peer[BOB_HASH]["status"] == "failed"
    assert "wire_id" not in by_peer[BOB_HASH]
    assert by_peer[BOB_HASH]["file"]["name"] == "notes.txt"


def test_send_group_file_validation(bridge):
    from tau.protocol import MAX_FILE_BYTES

    with pytest.raises(ValueError, match="target group is required"):
        bridge.send_group_file("", "a.txt", b"x")
    with pytest.raises(ValueError, match="unknown group"):
        bridge.send_group_file("nobody", "a.txt", b"x")
    with pytest.raises(ValueError, match="not usable"):
        bridge.send_group_file("crew", "..", b"x")
    with pytest.raises(ValueError, match="outside the"):
        bridge.send_group_file("crew", "a.txt", b"")
    with pytest.raises(ValueError, match="outside the"):
        bridge.send_group_file("crew", "a.txt", b"x" * (MAX_FILE_BYTES + 1))


def _seed_broadcast(node, tmp_path, name="team", members=(PEER_HASH, BOB_HASH)):
    from tau.broadcasts import BroadcastStore

    node.broadcasts = BroadcastStore(tmp_path / "broadcasts.json")
    node.broadcasts.create(name, list(members))
    return node.broadcasts


def test_send_broadcast_file_records_one_entry_per_leg(bridge, node, tmp_path):
    import hashlib

    _seed_broadcast(node, tmp_path)
    data = b"hello mesh"
    messages, response = bridge.send_broadcast_file("team", "notes.txt", data)
    assert response["ok"] is True
    assert node.broadcast_sent_files == [("team", "notes.txt", data)]
    assert len(messages) == 2
    assert all(m["direction"] == "out" for m in messages)
    # A broadcast leg is an ordinary one-to-one file entry: no list
    # metadata is stored on it, because none ever reaches a recipient.
    assert all("group" not in m for m in messages)
    assert all(m["status"] == "delivered" for m in messages)
    assert all(m["text"] == "Sent file: notes.txt (10 bytes)" for m in messages)
    assert all(
        m["file"]
        == {
            "name": "notes.txt",
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        for m in messages
    )
    assert messages[0]["wire_id"] == "file-" + PEER_HASH[:8]
    # The feed carries both legs, and the bus tells live consoles.
    _, _, feed = _json(bridge, "/api/messages")
    assert len(feed["messages"]) == 2
    published = [e for e in bridge.bus.replay(0) if e.get("type") == "message"]
    assert [e["message"]["id"] for e in published[-2:]] == [
        m["id"] for m in messages
    ]


def test_send_broadcast_file_failed_leg_is_recorded(bridge, node, tmp_path):
    _seed_broadcast(node, tmp_path)
    node.broadcast_refusals = {BOB_HASH}
    data = b"hello mesh"
    messages, response = bridge.send_broadcast_file("team", "notes.txt", data)
    assert response["ok"] is False
    assert len(messages) == 2
    by_peer = {m["peer"]: m for m in messages}
    assert by_peer[PEER_HASH]["status"] == "delivered"
    assert by_peer[PEER_HASH]["wire_id"] == "file-" + PEER_HASH[:8]
    assert by_peer[BOB_HASH]["status"] == "failed"
    assert "wire_id" not in by_peer[BOB_HASH]
    assert by_peer[BOB_HASH]["file"]["name"] == "notes.txt"


def test_send_broadcast_file_validation(bridge, node, tmp_path):
    from tau.protocol import MAX_FILE_BYTES

    _seed_broadcast(node, tmp_path)
    with pytest.raises(ValueError, match="target broadcast list is required"):
        bridge.send_broadcast_file("", "a.txt", b"x")
    with pytest.raises(ValueError, match="unknown broadcast list"):
        bridge.send_broadcast_file("nobody", "a.txt", b"x")
    with pytest.raises(ValueError, match="not usable"):
        bridge.send_broadcast_file("team", "..", b"x")
    with pytest.raises(ValueError, match="outside the"):
        bridge.send_broadcast_file("team", "a.txt", b"")
    with pytest.raises(ValueError, match="outside the"):
        bridge.send_broadcast_file("team", "a.txt", b"x" * (MAX_FILE_BYTES + 1))


def test_messages_import_rejects_wrong_format(bridge):
    status, _, payload = _json(
        bridge, "/api/messages/import", method="POST", body={"format": "nope"}
    )
    assert status == 400
    assert "unsupported export format" in payload["error"]["message"]


def test_messages_import_skips_bad_entries(bridge):
    document = {
        "format": "tau-messages-export",
        "version": 1,
        "exported_at": 1000.0,
        "messages": [
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "good one",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "good-1",
            },
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "bad-1",
            },
        ],
    }
    status, _, payload = _json(
        bridge, "/api/messages/import", method="POST", body=document
    )
    assert status == 200
    assert payload["import"]["imported"] == 1
    assert payload["import"]["skipped"] == 1
    assert len(payload["import"]["errors"]) == 1
    assert payload["import"]["errors"][0]["id"] == "bad-1"
    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == ["good one"]


def test_messages_import_skips_expired_in_feed(bridge):
    document = {
        "format": "tau-messages-export",
        "version": 1,
        "exported_at": 1000.0,
        "messages": [
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "already gone",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "gone-1",
                "expires_at": 200.0,
            }
        ],
    }
    status, _, payload = _json(
        bridge, "/api/messages/import", method="POST", body=document
    )
    assert status == 200
    assert payload["import"]["imported"] == 1
    # The restore landed in the inbox, but a message whose deadline
    # already passed is not resurrected into the feed.
    _, _, feed = _json(bridge, "/api/messages")
    assert feed["messages"] == []
    _, _, exported = _json(bridge, "/api/messages/export")
    assert len(exported["messages"]) == 1


def test_messages_export_too_large_for_import_is_refused(bridge, node, monkeypatch):
    import tau.webapi as webapi

    _inbound_record(node)
    monkeypatch.setattr(webapi, "MAX_IMPORT_BYTES", 10)
    status, _, payload = _json(bridge, "/api/messages/export")
    assert status == 400
    assert "command line" in payload["error"]["message"]


def test_messages_import_loopback_guard():
    from tau.webapi import _is_loopback_client

    assert _is_loopback_client("127.0.0.1") is True
    assert _is_loopback_client("::1") is True
    assert _is_loopback_client("192.168.1.5") is False
    assert _is_loopback_client("not an address") is False


def test_messages_import_hides_blocked_sender_from_events(bridge, node):
    from tau.blocklist import BlockList

    document = {
        "format": "tau-messages-export",
        "version": 1,
        "messages": [
            {
                "from": "alice",
                "destination": PEER_HASH,
                "text": "from alice",
                "sent_at": 100.0,
                "received_at": 101.0,
                "id": "m-1",
                "direction": "in",
            },
            {
                "from": "mallory",
                "destination": OTHER_HASH,
                "text": "from mallory",
                "sent_at": 102.0,
                "received_at": 103.0,
                "id": "m-2",
                "direction": "in",
            },
        ],
    }
    node.blocklist = BlockList(blocked=frozenset({OTHER_HASH}))
    status, _, payload = _json(
        bridge, "/api/messages/import", method="POST", body=document
    )
    assert status == 200
    assert payload["import"]["imported"] == 2
    # Only the unblocked sender's restore is announced to live consoles.
    published = [
        event["message"]["text"]
        for event in bridge.bus.replay(0)
        if event.get("type") == "message"
        and event["message"].get("text") in ("from alice", "from mallory")
    ]
    assert published == ["from alice"]
    # The blocked record is still stored: unblocking restores the view.
    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == ["from alice"]
    node.blocklist = BlockList()
    _, _, feed = _json(bridge, "/api/messages")
    assert [m["text"] for m in feed["messages"]] == [
        "from alice",
        "from mallory",
    ]


def test_send_file_endpoint(bridge, node):
    import base64

    data = base64.b64encode(b"hello mesh").decode("ascii")
    status, _, payload = _json(
        bridge,
        "/api/send-file",
        method="POST",
        body={"to": "alice", "name": "notes.txt", "data": data},
    )
    assert status == 200
    assert payload["ok"] is True
    assert payload["message"]["file"]["name"] == "notes.txt"
    assert payload["response"]["size"] == 10
    assert node.sent_files == [("alice", "notes.txt", b"hello mesh")]


def test_send_file_endpoint_rejects_bad_requests(bridge):
    for body, fragment in (
        ({"name": "a.txt", "data": "eA=="}, "to"),
        ({"to": "alice", "data": "eA=="}, "name"),
        ({"to": "alice", "name": "a.txt"}, "data"),
        ({"to": "alice", "name": "a.txt", "data": "not base64!!"}, "data"),
        ({"to": "nobody", "name": "a.txt", "data": "eA=="}, "unknown peer"),
    ):
        status, _, payload = _json(
            bridge, "/api/send-file", method="POST", body=body
        )
        assert status == 400
        assert fragment in payload["error"]["message"]


def test_send_file_endpoint_accepts_bodies_over_the_json_cap(bridge, node):
    import base64

    # 40 KiB of file is ~55 KiB base64: far over the 32 KiB JSON cap,
    # but well under this endpoint's 15 MiB cap.
    data = base64.b64encode(bytes(40 * 1024)).decode("ascii")
    status, _, payload = _json(
        bridge,
        "/api/send-file",
        method="POST",
        body={"to": "alice", "name": "big.bin", "data": data},
    )
    assert status == 200
    assert payload["message"]["file"]["size"] == 40 * 1024
    assert node.sent_files[0][1] == "big.bin"


def test_send_group_file_endpoint(bridge, node):
    import base64

    data = base64.b64encode(b"hello mesh").decode("ascii")
    status, _, payload = _json(
        bridge,
        "/api/groups/send-file",
        method="POST",
        body={"group": "crew", "name": "notes.txt", "data": data},
    )
    assert status == 200
    assert payload["ok"] is True
    assert len(payload["messages"]) == 2
    assert all(m["file"]["name"] == "notes.txt" for m in payload["messages"])
    assert payload["response"]["results"][0]["size"] == 10
    assert node.group_sent_files == [("crew", "notes.txt", b"hello mesh")]


def test_send_group_file_endpoint_rejects_bad_requests(bridge):
    for body, fragment, status_code in (
        ({"name": "a.txt", "data": "eA=="}, "group", 400),
        ({"group": "crew", "data": "eA=="}, "name", 400),
        ({"group": "crew", "name": "a.txt"}, "data", 400),
        ({"group": "crew", "name": "a.txt", "data": "not base64!!"}, "data", 400),
        ({"group": "nobody", "name": "a.txt", "data": "eA=="}, "unknown group", 404),
    ):
        status, _, payload = _json(
            bridge, "/api/groups/send-file", method="POST", body=body
        )
        assert status == status_code
        assert fragment in payload["error"]["message"]


def test_send_broadcast_file_endpoint(bridge, node, tmp_path):
    import base64

    _seed_broadcast(node, tmp_path)
    data = base64.b64encode(b"hello mesh").decode("ascii")
    status, _, payload = _json(
        bridge,
        "/api/broadcasts/send-file",
        method="POST",
        body={"broadcast": "team", "name": "notes.txt", "data": data},
    )
    assert status == 200
    assert payload["ok"] is True
    assert len(payload["messages"]) == 2
    assert all(m["file"]["name"] == "notes.txt" for m in payload["messages"])
    assert payload["response"]["results"][0]["size"] == 10
    assert node.broadcast_sent_files == [("team", "notes.txt", b"hello mesh")]


def test_send_broadcast_file_endpoint_rejects_bad_requests(bridge, node, tmp_path):
    _seed_broadcast(node, tmp_path)
    for body, fragment, status_code in (
        ({"name": "a.txt", "data": "eA=="}, "broadcast", 400),
        ({"broadcast": "team", "data": "eA=="}, "name", 400),
        ({"broadcast": "team", "name": "a.txt"}, "data", 400),
        (
            {"broadcast": "team", "name": "a.txt", "data": "not base64!!"},
            "data",
            400,
        ),
        (
            {"broadcast": "nobody", "name": "a.txt", "data": "eA=="},
            "unknown broadcast list",
            404,
        ),
    ):
        status, _, payload = _json(
            bridge, "/api/broadcasts/send-file", method="POST", body=body
        )
        assert status == status_code
        assert fragment in payload["error"]["message"]


def test_edit_message_endpoint_rejects_group_file_leg(bridge, node):
    # A group file leg carries both `group` and `file`: the file guard
    # must run before the group dispatch, or the group edit path
    # refuses it with a misleading leg-timing error instead.
    messages, _ = bridge.send_group_file("crew", "notes.txt", b"hello mesh")
    status, _, payload = _json(
        bridge,
        "/api/messages/edit",
        method="POST",
        body={"id": messages[0]["id"], "text": "fixed"},
    )
    assert status == 400
    assert "file transfers cannot be edited" in payload["error"]["message"]


def test_retract_message_endpoint_rejects_group_file_leg(bridge, node):
    messages, _ = bridge.send_group_file("crew", "notes.txt", b"hello mesh")
    status, _, payload = _json(
        bridge,
        "/api/messages/retract",
        method="POST",
        body={"id": messages[0]["id"]},
    )
    assert status == 400
    assert "file transfers cannot be retracted" in payload["error"]["message"]


def test_react_endpoint_rejects_group_file_leg(bridge, node):
    # The Qodo autofix on this PR blocks reactions to a group file
    # leg: there is no message copy for members to resolve it against.
    messages, _ = bridge.send_group_file("crew", "notes.txt", b"hello mesh")
    status, _, payload = _json(
        bridge,
        "/api/react",
        method="POST",
        body={"target": messages[0]["wire_id"], "reaction": "👍"},
    )
    assert status == 400
    assert "cannot be reacted to" in payload["error"]["message"]


def test_group_reply_to_a_file_leg_is_refused(bridge, node):
    # The Qodo autofix on this PR excludes file legs from group
    # reply resolution, so quoting a group file is refused instead
    # of dangling at a transfer the members hold no message for.
    messages, _ = bridge.send_group_file("crew", "notes.txt", b"hello mesh")
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={
            "group": "crew",
            "text": "seconded",
            "reply_to": messages[0]["wire_id"][:12],
        },
    )
    assert status == 400
    assert "no message" in payload["error"]["message"]


def test_other_endpoints_keep_the_json_body_cap(bridge):
    # The same oversized body to /api/messages must still be refused.
    status, _, payload = _json(
        bridge,
        "/api/messages",
        method="POST",
        body={"to": "alice", "text": "x" * (40 * 1024)},
    )
    assert status == 400
    assert "too large" in payload["error"]["message"]


def test_channel_schedule_via_api(tmp_path):
    from tau.channels import init_db as init_channel_db, join_channel
    from tau.config import TauConfig

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    conn = init_channel_db(node.config.channels_db_path)
    try:
        join_channel(conn, "general")
    finally:
        conn.close()

    # No channel app attached: scheduling writes the queue file, like the
    # CLI, and the serving node publishes due entries later.
    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={"channel": "general", "text": "hello later", "send_in": 3600},
        )
        assert status == 200
        scheduled = payload["scheduled"]
        assert scheduled["channel"] == "general"
        assert scheduled["text"] == "hello later"
        assert abs(scheduled["publish_at"] - (time.time() + 3600)) < 5

        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={"channel": "general", "text": "sooner", "send_in": 60},
        )
        assert status == 200

        status, _, payload = _json(bridge, "/api/channels/scheduled")
        assert status == 200
        assert payload["count"] == 2
        # Soonest first: the 60-second message precedes the hour-out one.
        assert [item["text"] for item in payload["scheduled"]] == [
            "sooner",
            "hello later",
        ]

        # The channel filter narrows the list.
        status, _, payload = _json(bridge, "/api/channels/scheduled?channel=other")
        assert status == 200
        assert payload["count"] == 0

        # Cancelling by id prefix drops only that entry.
        status, _, payload = _json(
            bridge,
            "/api/channels/cancel-scheduled",
            method="POST",
            body={"id": scheduled["id"][:8]},
        )
        assert status == 200
        assert payload["cancelled"]["id"] == scheduled["id"]
        assert payload["deleted_local"] is False
        status, _, payload = _json(bridge, "/api/channels/scheduled")
        assert payload["count"] == 1
        assert payload["scheduled"][0]["text"] == "sooner"

        # Cancelling an unknown id is a bad request, not a crash.
        status, _, payload = _json(
            bridge,
            "/api/channels/cancel-scheduled",
            method="POST",
            body={"id": "zz" * 8},
        )
        assert status == 400
        assert "no scheduled channel message" in payload["error"]["message"]

        # Editing the remaining entry changes its text and time.
        status, _, payload = _json(bridge, "/api/channels/scheduled")
        remaining = payload["scheduled"][0]["id"]
        status, _, payload = _json(
            bridge,
            "/api/channels/edit-scheduled",
            method="POST",
            body={"id": remaining[:8], "text": "sooner, edited", "send_in": 120},
        )
        assert status == 200
        assert payload["edited"]["id"] == remaining
        assert payload["edited"]["text"] == "sooner, edited"
        status, _, payload = _json(bridge, "/api/channels/scheduled")
        assert payload["scheduled"][0]["text"] == "sooner, edited"

        # Editing with neither text nor send_in, and an unknown id, are
        # bad requests, not crashes.
        status, _, payload = _json(
            bridge,
            "/api/channels/edit-scheduled",
            method="POST",
            body={"id": remaining[:8]},
        )
        assert status == 400
        assert "nothing to change" in payload["error"]["message"]
        status, _, payload = _json(
            bridge,
            "/api/channels/edit-scheduled",
            method="POST",
            body={"id": "zz" * 8, "text": "nope"},
        )
        assert status == 400
        assert "no scheduled channel message" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_channel_schedule_reply_via_api(tmp_path):
    import RNS

    from tau.channels import (
        create_message,
        init_db as init_channel_db,
        join_channel,
    )
    from tau.config import TauConfig

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    bob = RNS.Identity()
    conn = init_channel_db(node.config.channels_db_path)
    try:
        join_channel(conn, "general")
        parent = create_message(
            conn, bob, "general", "meet at the park", name="bob", now=1000
        )
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # A reply schedules with its parent's hash attached, so it
        # publishes as a reply, not a top-level message.
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "see you there",
                "send_in": 3600,
                "reply_to": parent["hash"],
            },
        )
        assert status == 200
        assert payload["scheduled"]["reply_to"] == parent["hash"]

        # A unique prefix resolves to the same full hash.
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "me too",
                "send_in": 60,
                "reply_to": parent["hash"][:8],
            },
        )
        assert status == 200
        assert payload["scheduled"]["reply_to"] == parent["hash"]

        # An unknown parent is a bad request, not a queued message
        # that fails every publish attempt.
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "nowhere",
                "send_in": 60,
                "reply_to": "00" * 32,
            },
        )
        assert status == 400
        assert "unknown message" in payload["error"]["message"]

        # A poll cannot carry a reply.
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "which day?",
                "send_in": 60,
                "poll_options": ["Saturday", "Sunday"],
                "reply_to": parent["hash"],
            },
        )
        assert status == 400
        assert "cannot carry a reply" in payload["error"]["message"]

        # A reply cannot carry a location pin or a contact card: the
        # publish pass would fail every attempt on such an entry.
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "agreed",
                "send_in": 60,
                "reply_to": parent["hash"],
                "location": {"lat": 34.05, "lon": -83.88},
            },
        )
        assert status == 400
        assert "cannot carry a location" in payload["error"]["message"]
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "agreed",
                "send_in": 60,
                "reply_to": parent["hash"],
                "contact": {"name": "carol", "destination_hash": "cc" * 16},
            },
        )
        assert status == 400
        assert "cannot carry a contact card" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_channel_schedule_poll_via_api(tmp_path):
    from tau.channels import init_db as init_channel_db, join_channel
    from tau.config import TauConfig

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    conn = init_channel_db(node.config.channels_db_path)
    try:
        join_channel(conn, "general")
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "which day?",
                "send_in": 3600,
                "poll_options": ["Saturday", "Sunday"],
            },
        )
        assert status == 200
        scheduled = payload["scheduled"]
        assert scheduled["kind"] == "poll"
        assert scheduled["text"] == "which day?"
        assert scheduled["options"] == ["Saturday", "Sunday"]
        assert abs(scheduled["publish_at"] - (time.time() + 3600)) < 5

        # The scheduled list carries the poll alongside messages.
        status, _, payload = _json(bridge, "/api/channels/scheduled")
        assert status == 200
        assert payload["count"] == 1
        assert payload["scheduled"][0]["kind"] == "poll"

        # Cancelling a queued poll works like cancelling a message.
        status, _, payload = _json(
            bridge,
            "/api/channels/cancel-scheduled",
            method="POST",
            body={"id": scheduled["id"][:8]},
        )
        assert status == 200
        assert payload["cancelled"]["kind"] == "poll"
        status, _, payload = _json(bridge, "/api/channels/scheduled")
        assert payload["count"] == 0
    finally:
        bridge.stop()


def test_channel_schedule_location_via_api(tmp_path):
    from tau.channels import init_db as init_channel_db, join_channel
    from tau.config import TauConfig

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    conn = init_channel_db(node.config.channels_db_path)
    try:
        join_channel(conn, "general")
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "meet here",
                "send_in": 3600,
                "location": {
                    "lat": 34.05,
                    "lon": -83.88,
                    "name": "Rock Springs Park",
                },
            },
        )
        assert status == 200
        scheduled = payload["scheduled"]
        assert scheduled["location"] == {
            "lat": 34.05,
            "lon": -83.88,
            "name": "Rock Springs Park",
        }

        # The scheduled list carries the pin.
        status, _, payload = _json(bridge, "/api/channels/scheduled")
        assert status == 200
        assert payload["count"] == 1
        assert payload["scheduled"][0]["location"] == {
            "lat": 34.05,
            "lon": -83.88,
            "name": "Rock Springs Park",
        }

        # A malformed pin is a bad request, not a silently dropped pin.
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "meet here",
                "send_in": 60,
                "location": {"lat": 91.0, "lon": 0.0},
            },
        )
        assert status == 400
        assert "location" in payload["error"]["message"]

        # A poll cannot carry a pin.
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "which day?",
                "send_in": 60,
                "poll_options": ["Saturday", "Sunday"],
                "location": {"lat": 34.05, "lon": -83.88},
            },
        )
        assert status == 400
        assert "location" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_channel_schedule_contact_via_api(tmp_path):
    from tau.blocklist import BlockList
    from tau.channels import init_db as init_channel_db, join_channel
    from tau.config import TauConfig

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    conn = init_channel_db(node.config.channels_db_path)
    try:
        join_channel(conn, "general")
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # The schedule endpoint enforces the contact-sharing policy,
        # like the immediate send: no block-list, no card.
        node.blocklist = BlockList()
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "meet carol",
                "send_in": 3600,
                "contact": {
                    "name": "carol",
                    "destination_hash": "cc" * 16,
                },
            },
        )
        assert status == 200
        scheduled = payload["scheduled"]
        assert scheduled["contact"] == {
            "name": "carol",
            "destination_hash": "cc" * 16,
        }

        # The scheduled list carries the card.
        status, _, payload = _json(bridge, "/api/channels/scheduled")
        assert status == 200
        assert payload["count"] == 1
        assert payload["scheduled"][0]["contact"] == {
            "name": "carol",
            "destination_hash": "cc" * 16,
        }

        # A malformed card is a bad request, not a silently dropped card.
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "meet carol",
                "send_in": 60,
                "contact": {"name": "carol"},
            },
        )
        assert status == 400
        assert "contact" in payload["error"]["message"]

        # A poll cannot carry a card.
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "which day?",
                "send_in": 60,
                "poll_options": ["Saturday", "Sunday"],
                "contact": {
                    "name": "carol",
                    "destination_hash": "cc" * 16,
                },
            },
        )
        assert status == 400
        assert "contact" in payload["error"]["message"]

        # A blocked peer's card must not be queued, and neither may
        # your own — the recipient already has your hash.
        node.blocklist = BlockList(blocked=frozenset({"cc" * 16}))
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "meet carol",
                "send_in": 60,
                "contact": {
                    "name": "carol",
                    "destination_hash": "cc" * 16,
                },
            },
        )
        assert status == 400
        assert "blocked peer" in payload["error"]["message"]
        node.blocklist = BlockList()
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "meet me",
                "send_in": 60,
                "contact": {
                    "name": "tester",
                    "destination_hash": node.destination_hexhash,
                },
            },
        )
        assert status == 400
        assert "own contact" in payload["error"]["message"]

        # Without a readable block-list the endpoint fails closed:
        # it cannot tell a blocked peer from an allowed one.
        node.blocklist = None
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "meet carol",
                "send_in": 60,
                "contact": {
                    "name": "carol",
                    "destination_hash": "cc" * 16,
                },
            },
        )
        assert status == 400
        assert "block-list" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_channel_schedule_expires_in_via_api(tmp_path):
    from tau.channels import init_db as init_channel_db, join_channel
    from tau.config import TauConfig

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    conn = init_channel_db(node.config.channels_db_path)
    try:
        join_channel(conn, "general")
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "goodbye soon",
                "send_in": 3600,
                "expires_in": 7200,
            },
        )
        assert status == 200
        scheduled = payload["scheduled"]
        assert scheduled["expires_in"] == 7200

        # The scheduled list carries the expiry.
        status, _, payload = _json(bridge, "/api/channels/scheduled")
        assert status == 200
        assert payload["count"] == 1
        assert payload["scheduled"][0]["expires_in"] == 7200

        # A scheduled poll can carry one too.
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={
                "channel": "general",
                "text": "which day?",
                "send_in": 60,
                "poll_options": ["Saturday", "Sunday"],
                "expires_in": 3600,
            },
        )
        assert status == 200
        assert payload["scheduled"]["expires_in"] == 3600

        # A bad lifetime is a bad request, not a silently dropped expiry.
        for bad in (0, -5, 30 * 24 * 60 * 60 + 1, "2h", True):
            status, _, payload = _json(
                bridge,
                "/api/channels/schedule",
                method="POST",
                body={
                    "channel": "general",
                    "text": "goodbye soon",
                    "send_in": 60,
                    "expires_in": bad,
                },
            )
            assert status == 400
            assert "expires_in" in payload["error"]["message"]
    finally:
        bridge.stop()


def test_channel_schedule_rejects_bad_requests(tmp_path):
    from tau.channels import init_db as init_channel_db, join_channel
    from tau.config import TauConfig

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    conn = init_channel_db(node.config.channels_db_path)
    try:
        join_channel(conn, "general")
    finally:
        conn.close()

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        for body, fragment in [
            ({"channel": "general", "text": "  ", "send_in": 60}, "text"),
            ({"channel": "general", "text": "hello"}, "send_in"),
            ({"channel": "general", "text": "hello", "send_in": 0}, "send_in"),
            ({"channel": "general", "text": "hello", "send_in": 604801}, "send_in"),
            ({"channel": "  ", "text": "hello", "send_in": 60}, "channel"),
            ({"text": "hello", "send_in": 60}, "channel"),
            (
                {
                    "channel": "general",
                    "text": "which day?",
                    "send_in": 60,
                    "poll_options": ["only-one"],
                },
                "poll_options",
            ),
            (
                {
                    "channel": "general",
                    "text": "which day?",
                    "send_in": 60,
                    "poll_options": ["a", "b", "c", "d", "e", "f", "g", "h", "i"],
                },
                "poll_options",
            ),
            (
                {
                    "channel": "general",
                    "text": "which day?",
                    "send_in": 60,
                    "poll_options": ["Saturday", "  "],
                },
                "poll_options",
            ),
            (
                {
                    "channel": "general",
                    "text": "which day?",
                    "send_in": 60,
                    "poll_options": "Saturday",
                },
                "poll_options",
            ),
        ]:
            status, _, payload = _json(
                bridge, "/api/channels/schedule", method="POST", body=body
            )
            assert status == 400
            assert fragment in payload["error"]["message"]

        # A channel never joined is refused, not queued for a publish
        # pass that would fail every attempt.
        status, _, payload = _json(
            bridge,
            "/api/channels/schedule",
            method="POST",
            body={"channel": "other", "text": "hello", "send_in": 60},
        )
        assert status == 400
        assert "not a member of channel 'other'" in payload["error"]["message"]

        status, _, payload = _json(
            bridge, "/api/channels/cancel-scheduled", method="POST", body={}
        )
        assert status == 400
        assert "id" in payload["error"]["message"]

        status, _, payload = _json(
            bridge, "/api/channels/edit-scheduled", method="POST", body={}
        )
        assert status == 400
        assert "id" in payload["error"]["message"]
    finally:
        bridge.stop()

def test_channel_react_via_api(tmp_path):
    from tau.channels import (
        ChannelApp,
        create_message,
        init_db as init_channel_db,
        join_channel,
    )
    from tau.config import TauConfig

    assert cli.main(["init", "--data-dir", str(tmp_path), "--name", "tester"]) == 0
    node = FakeNode()
    node.config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    node.registry = FakeRegistry([])
    bob = RNS.Identity()
    tester = RNS.Identity()
    conn = init_channel_db(node.config.channels_db_path)
    try:
        join_channel(conn, "general")
        bob_message = create_message(
            conn, bob, "general", "hello channels", name="bob", now=1000
        )
    finally:
        conn.close()

    app = ChannelApp(node.config, tester, name="tester")
    app.node = node  # type: ignore[assignment]
    node.channel_app = app

    bridge = WebBridge(node, host="127.0.0.1", port=0)
    bridge.start()
    try:
        # Reacting attaches a summary to the message in the feed.
        status, _, payload = _json(
            bridge,
            "/api/channels/react",
            method="POST",
            body={
                "channel": "general",
                "target": bob_message["hash"][:12],
                "reaction": "👍",
            },
        )
        assert status == 200
        assert payload["reaction"]["reaction"] == "👍"
        status, _, payload = _json(
            bridge, "/api/channels/messages?channel=general"
        )
        assert status == 200
        assert payload["messages"][0]["reactions"] == [
            {"reaction": "👍", "count": 1, "reactors": ["tester"]}
        ]

        # Validation errors are 400s: a non-string token, an unknown target.
        status, _, payload = _json(
            bridge,
            "/api/channels/react",
            method="POST",
            body={"channel": "general", "target": bob_message["hash"], "reaction": 5},
        )
        assert status == 400
        status, _, payload = _json(
            bridge,
            "/api/channels/react",
            method="POST",
            body={"channel": "general", "target": "0" * 64, "reaction": "👍"},
        )
        assert status == 400
        assert "no stored message" in payload["error"]["message"]

        # An empty reaction clears yours: the summary disappears.
        status, _, payload = _json(
            bridge,
            "/api/channels/react",
            method="POST",
            body={
                "channel": "general",
                "target": bob_message["hash"],
                "reaction": "",
            },
        )
        assert status == 200
        status, _, payload = _json(
            bridge, "/api/channels/messages?channel=general"
        )
        assert payload["messages"][0]["reactions"] == []
    finally:
        bridge.stop()
