"""Group DMs: wire format, roster store, handler and fan-out.

Covers issue #80: private group DMs with a fixed member list, carried
over the existing encrypted links with offline legs queued in the
store-and-forward outbox.
"""

from __future__ import annotations

import json
import threading
import time
from types import SimpleNamespace

import RNS
import pytest

from tau import cli
from tau.config import TauConfig
from tau.groups import GroupStore
from tau.identity import destination_hash
from tau.inbox import Inbox
from tau.mailbox import Mailbox, open_message, seal_message
from tau.node import NodeError, TauNode
from tau.peers import Peer, PeerRegistry
from tau.protocol import (
    ProtocolError,
    decode,
    derive_group_id,
    encode_group_message,
    generate_group_id,
    parse_group_message,
)

OWN_HASH = "ab" * 16
OTHER_HASH = "cd" * 16
THIRD_HASH = "ef" * 16
STRANGER_HASH = "12" * 16


def make_node(tmp_path, name="alice", on_message=None):
    config = TauConfig.resolve(data_dir=tmp_path, name=name)
    node = TauNode(
        config,
        registry=PeerRegistry(config.peers_path),
        on_message=on_message,
    )
    node.destination = SimpleNamespace(hexhash=OWN_HASH, hash=bytes.fromhex(OWN_HASH))
    return node


def make_sender():
    identity = RNS.Identity()
    return identity, destination_hash(identity).hex()


def make_group_payload(
    text="hello group",
    *,
    sender_members=None,
    group_id=None,
    group_name="weekend",
    creator=None,
    message_id="group-msg-1",
):
    members = sender_members or [OWN_HASH, OTHER_HASH, THIRD_HASH]
    creator = creator or members[0]
    return encode_group_message(
        text,
        message_id=message_id,
        group_id=group_id or derive_group_id(creator, group_name, members),
        group_name=group_name,
        creator=creator,
        members=members,
    )


def call_handler(handler, data, remote_identity=None):
    return decode(
        handler("/tau/test", data, b"request-id", remote_identity, time.time())
    )


# -- wire format ------------------------------------------------------------


def test_group_message_roundtrip():
    members = [OWN_HASH, OTHER_HASH, THIRD_HASH]
    group_id = derive_group_id(OWN_HASH, "weekend", members)
    payload = encode_group_message(
        "hi all",
        message_id="gm-1",
        group_id=group_id,
        group_name="weekend",
        creator=OWN_HASH,
        members=members,
        sent_at=123.0,
    )
    parsed = parse_group_message(payload)
    assert parsed["text"] == "hi all"
    assert parsed["id"] == "gm-1"
    assert parsed["group"] == group_id
    assert parsed["group_name"] == "weekend"
    assert parsed["creator"] == OWN_HASH
    assert parsed["members"] == members
    assert parsed["sent_at"] == 123.0


def test_group_id_derives_from_creator_name_and_roster():
    members = [OWN_HASH, OTHER_HASH, THIRD_HASH]
    first = derive_group_id(OWN_HASH, "weekend", members)
    # Order-independent and deterministic.
    assert derive_group_id(OWN_HASH, "weekend", list(reversed(members))) == first
    assert derive_group_id(OWN_HASH, "weekend", members) == first
    # Any change to the roster, name or creator is a new group.
    assert derive_group_id(OWN_HASH, "weekend", [OWN_HASH, OTHER_HASH]) != first
    assert derive_group_id(OWN_HASH, "other", members) != first
    assert derive_group_id(OTHER_HASH, "weekend", members) != first


def test_group_message_rejects_bad_rosters():
    group_id = derive_group_id(OWN_HASH, "g", [OWN_HASH, OTHER_HASH])
    with pytest.raises(ProtocolError):
        encode_group_message(
            "x", group_id=group_id, group_name="g", creator=OWN_HASH, members=[OWN_HASH]
        )
    with pytest.raises(ProtocolError):
        encode_group_message(
            "x",
            group_id=group_id,
            group_name="g",
            creator=OWN_HASH,
            members=[OWN_HASH, OWN_HASH],
        )
    with pytest.raises(ProtocolError):
        encode_group_message(
            "x",
            group_id=group_id,
            group_name="g",
            creator=OWN_HASH,
            members=["not-a-hash", OTHER_HASH],
        )
    with pytest.raises(ProtocolError):
        encode_group_message(
            "x",
            group_id=group_id,
            group_name="g",
            creator=OWN_HASH,
            members=[f"{i:032x}" for i in range(33)],
        )


def test_parse_group_message_rejects_forked_roster():
    # A member reuses a known group id with a different roster: the id
    # no longer derives from the roster, so the message is rejected at
    # parse time instead of pinning a fork.
    members = [OWN_HASH, OTHER_HASH, THIRD_HASH]
    real_id = derive_group_id(OWN_HASH, "weekend", members)
    forked = [OWN_HASH, OTHER_HASH, STRANGER_HASH]
    raw = json.dumps(
        {
            "type": "group_message",
            "v": 1,
            "text": "forked roster",
            "group": real_id,
            "group_name": "weekend",
            "creator": OWN_HASH,
            "members": forked,
            "sent_at": 1.0,
            "id": "gm-fork",
        }
    ).encode("utf-8")
    with pytest.raises(ProtocolError, match="does not match"):
        parse_group_message(raw)


def test_parse_group_message_rejects_wrong_type():
    raw = json.dumps(
        {
            "type": "message",
            "v": 1,
            "text": "not a group message",
            "group": derive_group_id(OWN_HASH, "g", [OWN_HASH, OTHER_HASH]),
            "group_name": "g",
            "creator": OWN_HASH,
            "members": [OWN_HASH, OTHER_HASH],
        }
    ).encode("utf-8")
    with pytest.raises(ProtocolError):
        parse_group_message(raw)


# -- roster store -----------------------------------------------------------


def test_group_store_create_find_and_list(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    group = store.create(
        "weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH
    )
    assert group["id"] == derive_group_id(OWN_HASH, "weekend", [OWN_HASH, OTHER_HASH])
    assert group["name"] == "weekend"
    assert group["members"] == [OWN_HASH, OTHER_HASH]
    assert group["creator"] == OWN_HASH
    assert group["local"] is True
    assert store.get(group["id"]) == group
    assert store.find(group["id"]) == group
    assert store.find(group["id"][:8]) == group
    assert store.find("weekend") == group
    assert [item["id"] for item in store.list_groups()] == [group["id"]]
    assert len(store) == 1
    # Creating the same group again is idempotent: same derived id.
    again = store.create("weekend", [OTHER_HASH, OWN_HASH], created_by=OWN_HASH)
    assert again["id"] == group["id"]
    assert len(store) == 1


def test_group_store_find_errors(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    first = store.create("same", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    second = store.create("same", [OWN_HASH, THIRD_HASH], created_by=OWN_HASH)
    with pytest.raises(ValueError, match="unknown group"):
        store.find("nope")
    with pytest.raises(ValueError, match="2 groups are named"):
        store.find("same")
    assert store.find(first["id"][:4]) == first or store.find(second["id"][:4]) == second


def test_group_store_record_first_roster_wins(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    group_id = derive_group_id(OWN_HASH, "weekend", [OWN_HASH, OTHER_HASH])
    first = store.record(
        group_id, "weekend", [OWN_HASH, OTHER_HASH], creator=OWN_HASH
    )
    assert first is not None
    assert first["id"] == group_id
    assert first["local"] is False
    # Same roster again: the pinned record comes back unchanged.
    assert (
        store.record(group_id, "weekend", [OWN_HASH, OTHER_HASH], creator=OWN_HASH)
        == first
    )
    # A different roster cannot reuse the id: derivation fails, so the
    # record is refused before the roster comparison is even reached.
    assert (
        store.record(group_id, "weekend", [OWN_HASH, THIRD_HASH], creator=OWN_HASH)
        is None
    )
    assert store.get(group_id) == first


def test_group_store_learned_cap_refuses_without_evicting(tmp_path):
    from tau.groups import MAX_LEARNED_GROUPS

    store = GroupStore(tmp_path / "groups.json")
    created = store.create("mine", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    for i in range(MAX_LEARNED_GROUPS):
        members = [OTHER_HASH, f"{i + 1:032x}"]
        group_id = derive_group_id(OTHER_HASH, f"learned-{i}", members)
        assert (
            store.record(group_id, f"learned-{i}", members, creator=OTHER_HASH)
            is not None
        )
    # The learned cap is full: one more learned group is refused, and
    # the locally created group is still there.
    extra_members = [OTHER_HASH, "ff" * 16]
    extra_id = derive_group_id(OTHER_HASH, "extra", extra_members)
    assert (
        store.record(extra_id, "extra", extra_members, creator=OTHER_HASH) is None
    )
    assert store.get(created["id"]) is not None
    assert len(store) == MAX_LEARNED_GROUPS + 1
    # Deleting a learned group frees a slot for the next one.
    assert store.delete(
        derive_group_id(OTHER_HASH, "learned-0", [OTHER_HASH, f"{1:032x}"])
    ) is True
    assert (
        store.record(extra_id, "extra", extra_members, creator=OTHER_HASH)
        is not None
    )


def test_group_store_delete(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    group = store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    assert store.delete(group["id"]) is True
    assert store.get(group["id"]) is None
    assert store.list_groups() == []
    assert store.delete(group["id"]) is False
    assert store.delete("00" * 16) is False


def test_group_store_update_members_adds_and_removes(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    group = store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    created_at = group["created_at"]
    grown = store.update_members(
        group["id"], [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )
    assert grown["id"] == derive_group_id(
        OWN_HASH, "weekend", [OWN_HASH, OTHER_HASH, THIRD_HASH]
    )
    assert grown["id"] != group["id"]
    assert grown["members"] == [OWN_HASH, OTHER_HASH, THIRD_HASH]
    assert grown["name"] == "weekend"
    assert grown["created_at"] == created_at
    assert grown["local"] is True
    assert store.get(group["id"]) is None
    assert store.get(grown["id"]) == grown
    assert len(store) == 1
    # The same roster again is a no-op: the same record comes back.
    assert (
        store.update_members(
            grown["id"],
            [THIRD_HASH, OWN_HASH, OTHER_HASH],
            created_by=OWN_HASH,
        )
        == grown
    )
    shrunk = store.update_members(
        grown["id"], [OWN_HASH, THIRD_HASH], created_by=OWN_HASH
    )
    assert shrunk["id"] == derive_group_id(
        OWN_HASH, "weekend", [OWN_HASH, THIRD_HASH]
    )
    assert shrunk["members"] == [OWN_HASH, THIRD_HASH]
    assert len(store) == 1


def test_group_store_retired_roster_cannot_be_repinned(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    group = store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    store.update_members(
        group["id"], [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )
    # A removed peer replaying the old roster must not re-pin the old id.
    assert (
        store.record(
            group["id"], "weekend", [OWN_HASH, OTHER_HASH], creator=OWN_HASH
        )
        is None
    )
    assert store.get(group["id"]) is None
    # Survives a restart.
    reloaded = GroupStore(tmp_path / "groups.json")
    assert (
        reloaded.record(
            group["id"], "weekend", [OWN_HASH, OTHER_HASH], creator=OWN_HASH
        )
        is None
    )


def test_group_store_leave_removes_and_refuses_repin(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    group = store.record(
        derive_group_id(OTHER_HASH, "theirs", [OWN_HASH, OTHER_HASH, THIRD_HASH]),
        "theirs",
        [OWN_HASH, OTHER_HASH, THIRD_HASH],
        creator=OTHER_HASH,
    )
    assert group is not None
    assert store.leave(group["id"]) == [group["id"]]
    assert store.get(group["id"]) is None
    assert store.list_groups() == []
    # A later message for the left group must not re-pin it — unlike
    # delete, whose removal the next message undoes.
    assert (
        store.record(
            group["id"],
            "theirs",
            [OWN_HASH, OTHER_HASH, THIRD_HASH],
            creator=OTHER_HASH,
        )
        is None
    )
    assert store.get(group["id"]) is None
    # Survives a restart.
    reloaded = GroupStore(tmp_path / "groups.json")
    assert reloaded.is_left(group["id"]) is True
    assert (
        reloaded.record(
            group["id"],
            "theirs",
            [OWN_HASH, OTHER_HASH, THIRD_HASH],
            creator=OTHER_HASH,
        )
        is None
    )
    left = reloaded.left_groups()
    assert [entry["id"] for entry in left] == [group["id"]]
    assert isinstance(left[0]["left_at"], float)


def test_group_store_transaction_uses_msvcrt_when_fcntl_missing(
    tmp_path, monkeypatch
):
    """Without fcntl (Windows) the store still takes a cross-process lock.

    Regression test for the CodeRabbit Major on PR #265: with only the
    per-instance lock, a serving process's ``record()`` could save
    after a concurrent CLI ``leave()`` and drop the left marker, so
    the next message re-pinned the group.
    """
    import tau.groups as groups_mod

    calls = []

    class FakeMsvcrt:
        LK_LOCK = 1
        LK_UNLCK = 0

        @staticmethod
        def locking(fd, mode, nbytes):
            calls.append(mode)

    monkeypatch.setattr(groups_mod, "fcntl", None)
    monkeypatch.setattr(groups_mod, "msvcrt", FakeMsvcrt)
    store = GroupStore(tmp_path / "groups.json")
    group = store.record(
        derive_group_id(OTHER_HASH, "theirs", [OWN_HASH, OTHER_HASH, THIRD_HASH]),
        "theirs",
        [OWN_HASH, OTHER_HASH, THIRD_HASH],
        creator=OTHER_HASH,
    )
    assert group is not None
    assert calls == [FakeMsvcrt.LK_LOCK, FakeMsvcrt.LK_UNLCK]
    assert (tmp_path / "groups.json.lock").exists()


def test_group_store_leave_marker_survives_concurrent_record(
    tmp_path, monkeypatch
):
    """A record() racing a leave() cannot drop the left marker.

    Two stores on one path stand in for the serving process and the
    CLI.  With fcntl missing, a fake msvcrt backed by a real lock
    serialises the transactions; every interleaving must end with
    the group left and unpinnable.
    """
    import tau.groups as groups_mod

    gate = threading.Lock()

    class FakeMsvcrt:
        LK_LOCK = 1
        LK_UNLCK = 0

        @staticmethod
        def locking(fd, mode, nbytes):
            if mode == FakeMsvcrt.LK_LOCK:
                gate.acquire()
            else:
                gate.release()

    monkeypatch.setattr(groups_mod, "fcntl", None)
    monkeypatch.setattr(groups_mod, "msvcrt", FakeMsvcrt)
    group_id = derive_group_id(
        OTHER_HASH, "theirs", [OWN_HASH, OTHER_HASH, THIRD_HASH]
    )
    server = GroupStore(tmp_path / "groups.json")
    cli_store = GroupStore(tmp_path / "groups.json")
    assert (
        server.record(
            group_id,
            "theirs",
            [OWN_HASH, OTHER_HASH, THIRD_HASH],
            creator=OTHER_HASH,
        )
        is not None
    )

    def hammer_record():
        for _ in range(25):
            server.record(
                group_id,
                "theirs",
                [OWN_HASH, OTHER_HASH, THIRD_HASH],
                creator=OTHER_HASH,
            )

    def hammer_leave():
        for _ in range(25):
            cli_store.leave(group_id)

    first = threading.Thread(target=hammer_record)
    second = threading.Thread(target=hammer_leave)
    first.start()
    second.start()
    first.join(timeout=30)
    second.join(timeout=30)
    assert not first.is_alive() and not second.is_alive()
    reloaded = GroupStore(tmp_path / "groups.json")
    assert reloaded.get(group_id) is None
    assert reloaded.is_left(group_id) is True
    assert (
        reloaded.record(
            group_id,
            "theirs",
            [OWN_HASH, OTHER_HASH, THIRD_HASH],
            creator=OTHER_HASH,
        )
        is None
    )


def test_group_store_leave_unknown_and_rejoin_never_left(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    assert store.leave("00" * 16) == []
    assert store.rejoin("00" * 16) is False
    assert store.is_left("00" * 16) is False
    assert store.left_groups() == []
    # Leaving twice reports nothing left to do, but stays left.
    group = store.record(
        derive_group_id(OTHER_HASH, "theirs", [OWN_HASH, OTHER_HASH, THIRD_HASH]),
        "theirs",
        [OWN_HASH, OTHER_HASH, THIRD_HASH],
        creator=OTHER_HASH,
    )
    assert store.leave(group["id"]) == [group["id"]]
    assert store.leave(group["id"]) == []
    assert store.is_left(group["id"]) is True


def test_group_store_rejoin_allows_repin(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    group = store.record(
        derive_group_id(OTHER_HASH, "theirs", [OWN_HASH, OTHER_HASH, THIRD_HASH]),
        "theirs",
        [OWN_HASH, OTHER_HASH, THIRD_HASH],
        creator=OTHER_HASH,
    )
    assert store.leave(group["id"]) == [group["id"]]
    assert store.rejoin(group["id"]) is True
    assert store.is_left(group["id"]) is False
    assert store.left_groups() == []
    assert store.rejoin(group["id"]) is False
    # The next message pins the group again.
    assert (
        store.record(
            group["id"],
            "theirs",
            [OWN_HASH, OTHER_HASH, THIRD_HASH],
            creator=OTHER_HASH,
        )
        is not None
    )
    assert store.get(group["id"]) is not None
    # A re-invite under a changed roster is a new id and pins without
    # a rejoin, even while the old id stays left.
    assert store.leave(group["id"]) == [group["id"]]
    grown_id = derive_group_id(
        OTHER_HASH, "theirs", [OWN_HASH, OTHER_HASH, THIRD_HASH, "12" * 16]
    )
    assert (
        store.record(
            grown_id,
            "theirs",
            [OWN_HASH, OTHER_HASH, THIRD_HASH, "12" * 16],
            creator=OTHER_HASH,
        )
        is not None
    )
    assert store.is_left(group["id"]) is True


def test_group_store_leave_also_leaves_pinned_successor(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    first = store.record(
        derive_group_id(OTHER_HASH, "theirs", [OWN_HASH, OTHER_HASH, THIRD_HASH]),
        "theirs",
        [OWN_HASH, OTHER_HASH, THIRD_HASH],
        creator=OTHER_HASH,
    )
    # A successor roster carried by another member pins alongside the
    # predecessor instead of replacing it.
    grown_id = derive_group_id(
        OTHER_HASH, "theirs", [OWN_HASH, OTHER_HASH, THIRD_HASH, "12" * 16]
    )
    second = store.record(
        grown_id,
        "theirs",
        [OWN_HASH, OTHER_HASH, THIRD_HASH, "12" * 16],
        creator=OTHER_HASH,
        sender=THIRD_HASH,
    )
    assert second is not None
    assert len(store.list_groups()) == 2
    # Leaving by name leaves both versions of the same group: the
    # successor would otherwise keep receiving.
    assert sorted(store.leave(first["id"])) == sorted([first["id"], grown_id])
    assert store.list_groups() == []
    assert store.is_left(first["id"]) is True
    assert store.is_left(grown_id) is True
    assert (
        store.record(
            first["id"],
            "theirs",
            [OWN_HASH, OTHER_HASH, THIRD_HASH],
            creator=OTHER_HASH,
        )
        is None
    )
    assert (
        store.record(
            grown_id,
            "theirs",
            [OWN_HASH, OTHER_HASH, THIRD_HASH, "12" * 16],
            creator=OTHER_HASH,
        )
        is None
    )
    # A genuinely new roster arriving after the leave is a new id and
    # pins normally.
    newer_id = derive_group_id(
        OTHER_HASH, "theirs", [OWN_HASH, OTHER_HASH, "34" * 16]
    )
    assert (
        store.record(
            newer_id,
            "theirs",
            [OWN_HASH, OTHER_HASH, "34" * 16],
            creator=OTHER_HASH,
        )
        is not None
    )


def test_group_store_find_resolves_32_char_name(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    group = store.create("n" * 32, [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    # A 32-character name resolves even though it is id-length.
    assert store.find("n" * 32)["id"] == group["id"]
    # An actual id still wins over a name that spells the same id.
    other = store.create("other", [OWN_HASH, THIRD_HASH], created_by=OWN_HASH)
    named = store.create(other["id"], [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    assert store.find(other["id"])["id"] == other["id"]
    assert named["id"] != other["id"]


def test_group_store_left_list_is_capped(tmp_path):
    from tau.groups import MAX_LEFT_GROUPS

    store = GroupStore(tmp_path / "groups.json")
    left_ids = []
    for i in range(MAX_LEFT_GROUPS + 5):
        group = store.record(
            derive_group_id(
                OTHER_HASH, f"g{i}", [OWN_HASH, OTHER_HASH, THIRD_HASH]
            ),
            f"g{i}",
            [OWN_HASH, OTHER_HASH, THIRD_HASH],
            creator=OTHER_HASH,
        )
        assert store.leave(group["id"]) == [group["id"]]
        left_ids.append(group["id"])
    raw = json.loads((tmp_path / "groups.json").read_text())
    assert len(raw["left"]) == MAX_LEFT_GROUPS
    assert left_ids[0] not in [entry["id"] for entry in raw["left"]]
    assert left_ids[-1] in [entry["id"] for entry in raw["left"]]


def test_group_store_update_members_refusals(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    group = store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    learned = store.record(
        derive_group_id(OTHER_HASH, "theirs", [OTHER_HASH, THIRD_HASH]),
        "theirs",
        [OTHER_HASH, THIRD_HASH],
        creator=OTHER_HASH,
    )
    assert learned is not None
    with pytest.raises(ValueError, match="unknown group"):
        store.update_members(
            "00" * 16, [OWN_HASH, OTHER_HASH], created_by=OWN_HASH
        )
    with pytest.raises(ValueError, match="only the creator"):
        store.update_members(
            learned["id"],
            [OTHER_HASH, THIRD_HASH, OWN_HASH],
            created_by=OWN_HASH,
        )
    with pytest.raises(ValueError, match="only the creator"):
        store.update_members(
            group["id"],
            [OWN_HASH, OTHER_HASH, THIRD_HASH],
            created_by=OTHER_HASH,
        )
    with pytest.raises(ValueError, match="at least 2 members"):
        store.update_members(group["id"], [OWN_HASH], created_by=OWN_HASH)
    with pytest.raises(ValueError, match="creator must be a member"):
        store.update_members(
            group["id"], [OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
        )
    # Refusals leave the store untouched.
    assert store.get(group["id"]) == group
    assert len(store) == 2


def test_group_store_update_members_ignores_local_cap(tmp_path):
    from tau.groups import MAX_GROUPS

    store = GroupStore(tmp_path / "groups.json")
    first = store.create("g0", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    for i in range(1, MAX_GROUPS):
        store.create(f"g{i}", [OWN_HASH, f"{i + 1:032x}"], created_by=OWN_HASH)
    assert len(store) == MAX_GROUPS
    # The swap is net-zero, so a full local cap must not block it.
    grown = store.update_members(
        first["id"], [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )
    assert grown["members"] == [OWN_HASH, OTHER_HASH, THIRD_HASH]
    assert len(store) == MAX_GROUPS


def test_group_store_record_stale_creator_message_does_not_replace_successor(
    tmp_path,
):
    store = GroupStore(tmp_path / "groups.json")
    old_id = derive_group_id(OTHER_HASH, "weekend", [OTHER_HASH, THIRD_HASH])
    assert (
        store.record(
            old_id,
            "weekend",
            [OTHER_HASH, THIRD_HASH],
            creator=OTHER_HASH,
            sender=OTHER_HASH,
            sent_at=100.0,
        )
        is not None
    )
    new_id = derive_group_id(
        OTHER_HASH, "weekend", [OTHER_HASH, THIRD_HASH, OWN_HASH]
    )
    assert (
        store.record(
            new_id,
            "weekend",
            [OTHER_HASH, THIRD_HASH, OWN_HASH],
            creator=OTHER_HASH,
            sender=OTHER_HASH,
            sent_at=200.0,
        )
        is not None
    )
    assert store.get(old_id) is None
    # A queued creator message carrying the old roster, sent before the
    # successor was sent and delivered late, pins alongside instead of
    # replacing the successor: the message is still accepted, but sends
    # by name keep resolving to the current roster.
    assert (
        store.record(
            old_id,
            "weekend",
            [OTHER_HASH, THIRD_HASH],
            creator=OTHER_HASH,
            sender=OTHER_HASH,
            sent_at=150.0,
        )
        is not None
    )
    assert store.get(new_id) is not None
    assert store.get(old_id) is not None
    assert store.find("weekend")["id"] == new_id
    assert len(store) == 2


def test_group_store_record_creator_restore_replaces_successor(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    old_id = derive_group_id(OTHER_HASH, "weekend", [OTHER_HASH, THIRD_HASH])
    new_id = derive_group_id(
        OTHER_HASH, "weekend", [OTHER_HASH, THIRD_HASH, OWN_HASH]
    )
    store.record(
        old_id,
        "weekend",
        [OTHER_HASH, THIRD_HASH],
        creator=OTHER_HASH,
        sender=OTHER_HASH,
        sent_at=100.0,
    )
    store.record(
        new_id,
        "weekend",
        [OTHER_HASH, THIRD_HASH, OWN_HASH],
        creator=OTHER_HASH,
        sender=OTHER_HASH,
        sent_at=200.0,
    )
    # The creator restores the old roster after the change and sends a
    # message with it: its sent_at is newer than the successor's, so it
    # is an intentional restore, not a stale copy, and it replaces.
    assert (
        store.record(
            old_id,
            "weekend",
            [OTHER_HASH, THIRD_HASH],
            creator=OTHER_HASH,
            sender=OTHER_HASH,
            sent_at=300.0,
        )
        is not None
    )
    assert store.get(new_id) is None
    assert store.get(old_id) is not None
    assert store.find("weekend")["id"] == old_id
    assert len(store) == 1


def test_group_store_record_out_of_order_successor_does_not_replace_restore(
    tmp_path,
):
    store = GroupStore(tmp_path / "groups.json")
    old_id = derive_group_id(OTHER_HASH, "weekend", [OTHER_HASH, THIRD_HASH])
    new_id = derive_group_id(
        OTHER_HASH, "weekend", [OTHER_HASH, THIRD_HASH, OWN_HASH]
    )
    # The restore (sent at 300) arrives before the successor's teaching
    # message (sent at 200): the successor is the stale one here, so it
    # pins alongside and the restore keeps resolving by name.
    store.record(
        old_id,
        "weekend",
        [OTHER_HASH, THIRD_HASH],
        creator=OTHER_HASH,
        sender=OTHER_HASH,
        sent_at=300.0,
    )
    assert (
        store.record(
            new_id,
            "weekend",
            [OTHER_HASH, THIRD_HASH, OWN_HASH],
            creator=OTHER_HASH,
            sender=OTHER_HASH,
            sent_at=200.0,
        )
        is not None
    )
    assert store.get(old_id) is not None
    assert store.get(new_id) is not None
    assert store.find("weekend")["id"] == old_id


def test_group_store_record_successor_retires_predecessor(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    old_id = derive_group_id(OTHER_HASH, "weekend", [OTHER_HASH, THIRD_HASH])
    old = store.record(
        old_id,
        "weekend",
        [OTHER_HASH, THIRD_HASH],
        creator=OTHER_HASH,
        sender=THIRD_HASH,
    )
    assert old is not None
    new_id = derive_group_id(
        OTHER_HASH, "weekend", [OTHER_HASH, THIRD_HASH, OWN_HASH]
    )
    new = store.record(
        new_id,
        "weekend",
        [OTHER_HASH, THIRD_HASH, OWN_HASH],
        creator=OTHER_HASH,
        sender=OTHER_HASH,
    )
    assert new is not None
    assert new["id"] == new_id
    # Name lookup points at the current roster, not the old one.
    assert store.find("weekend")["id"] == new_id
    assert store.get(old_id) is None
    assert len(store) == 1
    # The predecessor was only deleted, not retired: the latest
    # creator-authorized roster wins, so its messages pin it again.
    assert (
        store.record(
            old_id,
            "weekend",
            [OTHER_HASH, THIRD_HASH],
            creator=OTHER_HASH,
            sender=OTHER_HASH,
        )
        is not None
    )
    repinned = store.get(old_id)
    assert repinned is not None
    assert repinned["members"] == [OTHER_HASH, THIRD_HASH]
    assert store.get(new_id) is None
    assert len(store) == 1


def test_group_store_record_successor_from_member_does_not_replace(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    old_id = derive_group_id(OTHER_HASH, "weekend", [OTHER_HASH, THIRD_HASH])
    assert (
        store.record(
            old_id,
            "weekend",
            [OTHER_HASH, THIRD_HASH],
            creator=OTHER_HASH,
            sender=THIRD_HASH,
        )
        is not None
    )
    new_id = derive_group_id(
        OTHER_HASH, "weekend", [OTHER_HASH, THIRD_HASH, OWN_HASH]
    )
    # A member other than the creator carries the successor roster: it
    # is pinned (the message is still accepted), but the predecessor
    # stays — the creator field is only a claim, so any member could
    # otherwise forge a successor and hijack sends by group name.
    new = store.record(
        new_id,
        "weekend",
        [OTHER_HASH, THIRD_HASH, OWN_HASH],
        creator=OTHER_HASH,
        sender=THIRD_HASH,
    )
    assert new is not None
    assert new["id"] == new_id
    assert store.get(old_id) is not None
    assert len(store) == 2
    # Name lookup keeps resolving to the roster known longest, so a
    # member-carried successor neither hijacks sends by name nor
    # blocks them until the creator sends it themself.
    assert store.find("weekend")["id"] == old_id
    # The creator themself sending the same roster does replace it.
    assert (
        store.record(
            new_id,
            "weekend",
            [OTHER_HASH, THIRD_HASH, OWN_HASH],
            creator=OTHER_HASH,
            sender=OTHER_HASH,
        )
        is not None
    )
    assert store.get(old_id) is None
    assert store.find("weekend")["id"] == new_id
    assert len(store) == 1


def test_group_store_find_prefers_local_group(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    mine = store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    learned_id = derive_group_id(OWN_HASH, "weekend", [OWN_HASH, THIRD_HASH])
    assert (
        store.record(
            learned_id, "weekend", [OWN_HASH, THIRD_HASH], creator=OWN_HASH
        )
        is not None
    )
    # A locally created group is authoritative for its name, even when
    # a learned group of the same name is pinned alongside it.
    assert store.find("weekend")["id"] == mine["id"]


def test_group_store_find_prefers_earliest_roster_after_reload(tmp_path):
    path = tmp_path / "groups.json"
    store = GroupStore(path)
    old_id = derive_group_id(OTHER_HASH, "weekend", [OTHER_HASH, THIRD_HASH])
    store.record(
        old_id,
        "weekend",
        [OTHER_HASH, THIRD_HASH],
        creator=OTHER_HASH,
        sender=THIRD_HASH,
    )
    new_id = derive_group_id(
        OTHER_HASH, "weekend", [OTHER_HASH, THIRD_HASH, OWN_HASH]
    )
    store.record(
        new_id,
        "weekend",
        [OTHER_HASH, THIRD_HASH, OWN_HASH],
        creator=OTHER_HASH,
        sender=THIRD_HASH,
    )
    assert store.find("weekend")["id"] == old_id
    reloaded = GroupStore(path)
    assert len(reloaded) == 2
    assert reloaded.find("weekend")["id"] == old_id


def test_group_store_find_still_ambiguous_across_creators(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    first_id = derive_group_id(OTHER_HASH, "same", [OTHER_HASH, THIRD_HASH])
    second_id = derive_group_id(STRANGER_HASH, "same", [STRANGER_HASH, THIRD_HASH])
    store.record(
        first_id, "same", [OTHER_HASH, THIRD_HASH], creator=OTHER_HASH
    )
    store.record(
        second_id, "same", [STRANGER_HASH, THIRD_HASH], creator=STRANGER_HASH
    )
    with pytest.raises(ValueError, match="2 groups are named"):
        store.find("same")


def test_group_store_record_never_replaces_local_group(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    mine = store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    learned_id = derive_group_id(OWN_HASH, "weekend", [OWN_HASH, THIRD_HASH])
    learned = store.record(
        learned_id, "weekend", [OWN_HASH, THIRD_HASH], creator=OWN_HASH
    )
    assert learned is not None
    assert store.get(mine["id"]) == mine
    assert len(store) == 2


def test_group_store_retirement_lifts_when_roster_returns(tmp_path):
    store = GroupStore(tmp_path / "groups.json")
    group = store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    grown = store.update_members(
        group["id"], [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )
    # The old roster is retired while the successor is live.
    assert (
        store.record(
            group["id"], "weekend", [OWN_HASH, OTHER_HASH], creator=OWN_HASH
        )
        is None
    )
    restored = store.update_members(
        grown["id"], [OWN_HASH, OTHER_HASH], created_by=OWN_HASH
    )
    assert restored["id"] == group["id"]
    # The group is live again, so delete it and re-pin it from a message.
    assert store.delete(restored["id"]) is True
    assert (
        store.record(
            group["id"], "weekend", [OWN_HASH, OTHER_HASH], creator=OWN_HASH
        )
        is not None
    )


def test_group_store_retired_list_is_capped(tmp_path):
    from tau.groups import MAX_RETIRED_GROUPS

    store = GroupStore(tmp_path / "groups.json")
    group = store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    retired_ids = []
    current = group
    for i in range(MAX_RETIRED_GROUPS + 5):
        member = f"{i + 1:032x}"
        nxt = store.update_members(
            current["id"], [OWN_HASH, member], created_by=OWN_HASH
        )
        retired_ids.append(current["id"])
        current = nxt
    raw = json.loads((tmp_path / "groups.json").read_text())
    assert len(raw["retired"]) == MAX_RETIRED_GROUPS
    assert retired_ids[0] not in raw["retired"]
    assert retired_ids[-1] in raw["retired"]


def test_group_store_update_members_save_failure_rolls_back(
    tmp_path, monkeypatch
):
    import tau.groups as groups_module

    store = GroupStore(tmp_path / "groups.json")
    group = store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)

    def _boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(groups_module, "atomic_write_json", _boom)
    with pytest.raises(OSError, match="disk full"):
        store.update_members(
            group["id"],
            [OWN_HASH, OTHER_HASH, THIRD_HASH],
            created_by=OWN_HASH,
        )
    # Neither the old record lost nor the successor kept.
    assert store.get(group["id"]) == group
    assert len(store) == 1
    reloaded = GroupStore(tmp_path / "groups.json")
    assert [item["id"] for item in reloaded.list_groups()] == [group["id"]]


def test_group_store_save_failure_rolls_back(tmp_path, monkeypatch):
    import tau.groups as groups_module

    store = GroupStore(tmp_path / "groups.json")
    group = store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)

    def _boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(groups_module, "atomic_write_json", _boom)
    # A failed pin is not acknowledged: the record is refused and the
    # store keeps only what is on disk.
    members = [OWN_HASH, THIRD_HASH]
    group_id = derive_group_id(OWN_HASH, "other", members)
    assert store.record(group_id, "other", members, creator=OWN_HASH) is None
    assert store.get(group_id) is None
    # A failed create raises instead of reporting a group that is gone
    # after a restart, and the earlier group survives.
    with pytest.raises(OSError):
        store.create("third", [OWN_HASH, "ff" * 16], created_by=OWN_HASH)
    assert store.get(group["id"]) is not None
    reloaded = GroupStore(tmp_path / "groups.json")
    assert [item["id"] for item in reloaded.list_groups()] == [group["id"]]


def test_group_store_first_save_failure_leaves_no_phantom_group(
    tmp_path, monkeypatch
):
    import tau.groups as groups_module

    store = GroupStore(tmp_path / "groups.json")

    def _boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(groups_module, "atomic_write_json", _boom)
    # The file does not exist yet, so reloading from disk cannot undo
    # the mutation — a failed first write must not leave the group
    # visible in memory.
    with pytest.raises(OSError, match="disk full"):
        store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    assert len(store) == 0
    group_id = derive_group_id(OWN_HASH, "other", [OWN_HASH, THIRD_HASH])
    assert (
        store.record(group_id, "other", [OWN_HASH, THIRD_HASH], creator=OWN_HASH)
        is None
    )
    assert len(store) == 0
    reloaded = GroupStore(tmp_path / "groups.json")
    assert len(reloaded) == 0


def test_group_store_failed_save_restores_superseded_and_deleted(
    tmp_path, monkeypatch
):
    import tau.groups as groups_module

    store = GroupStore(tmp_path / "groups.json")
    group = store.create("local", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    old_id = derive_group_id(OTHER_HASH, "weekend", [OTHER_HASH, THIRD_HASH])
    store.record(
        old_id,
        "weekend",
        [OTHER_HASH, THIRD_HASH],
        creator=OTHER_HASH,
        sender=OTHER_HASH,
    )

    def _boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(groups_module, "atomic_write_json", _boom)
    # A creator-sent successor whose save fails must not delete the
    # predecessor it was about to replace.
    new_id = derive_group_id(
        OTHER_HASH, "weekend", [OTHER_HASH, THIRD_HASH, OWN_HASH]
    )
    assert (
        store.record(
            new_id,
            "weekend",
            [OTHER_HASH, THIRD_HASH, OWN_HASH],
            creator=OTHER_HASH,
            sender=OTHER_HASH,
        )
        is None
    )
    assert store.get(old_id) is not None
    assert store.get(new_id) is None
    assert store.find("weekend")["id"] == old_id
    # A delete whose save fails must not drop the group either.
    with pytest.raises(OSError, match="disk full"):
        store.delete(group["id"])
    assert store.get(group["id"]) == group
    reloaded = GroupStore(tmp_path / "groups.json")
    assert [item["id"] for item in reloaded.list_groups()] == [
        group["id"],
        old_id,
    ]


def test_group_store_survives_reload_and_drops_corrupt(tmp_path):
    path = tmp_path / "groups.json"
    store = GroupStore(path)
    group = store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    raw = json.loads(path.read_text())
    raw["groups"].append({"id": "not-a-group"})
    raw["groups"].append({"id": generate_group_id(), "members": [OWN_HASH]})
    # An id that does not derive from its roster is dropped as corrupt,
    # even though the shape is otherwise valid.
    raw["groups"].append(
        {
            "id": generate_group_id(),
            "name": "forged",
            "members": [OWN_HASH, OTHER_HASH],
            "creator": OWN_HASH,
            "created_by": OWN_HASH,
            "created_at": 1.0,
        }
    )
    path.write_text(json.dumps(raw))
    reloaded = GroupStore(path)
    assert [item["id"] for item in reloaded.list_groups()] == [group["id"]]


# -- handler ----------------------------------------------------------------


def test_group_message_delivered_and_group_pinned(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    members = [OWN_HASH, sender_hash, THIRD_HASH]
    group_id = derive_group_id(sender_hash, "weekend", members)
    response = call_handler(
        node._handle_group_message,
        encode_group_message(
            "hi all",
            message_id="gm-1",
            group_id=group_id,
            group_name="weekend",
            creator=sender_hash,
            members=members,
        ),
        remote_identity=sender,
    )
    assert response["ok"] is True
    assert response["group"] == group_id
    assert len(received) == 1
    record = received[0]
    assert record["kind"] == "group"
    assert record["group"] == group_id
    assert record["group_name"] == "weekend"
    assert record["text"] == "hi all"
    # The inbox keeps the group fields; the store pinned the roster.
    entry = node.inbox.list_messages()[0]
    assert entry["kind"] == "group" and entry["group"] == group_id
    assert node.inbox.list_messages(group=group_id)[0]["text"] == "hi all"
    assert node.inbox.list_messages(group=generate_group_id()) == []
    pinned = node.groups.get(group_id)
    assert pinned is not None and pinned["members"] == sorted(members)
    assert pinned["name"] == "weekend"
    assert pinned["creator"] == sender_hash


def test_group_message_from_non_member_rejected(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    # The sender's link identity is not on the roster it claims.
    response = call_handler(
        node._handle_group_message,
        make_group_payload(
            sender_members=[OWN_HASH, OTHER_HASH, THIRD_HASH]
        ),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_a_member"
    assert received == []
    assert len(node.inbox) == 0
    assert len(node.groups) == 0


def test_group_message_after_leave_is_rejected(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    members = [OWN_HASH, sender_hash, THIRD_HASH]
    group_id = derive_group_id(sender_hash, "weekend", members)
    first = call_handler(
        node._handle_group_message,
        encode_group_message(
            "hi all",
            message_id="gm-1",
            group_id=group_id,
            group_name="weekend",
            creator=sender_hash,
            members=members,
        ),
        remote_identity=sender,
    )
    assert first["ok"] is True
    assert len(received) == 1
    assert node.groups.leave(group_id) == [group_id]
    # A later message for the left group is rejected, never stored,
    # and does not re-pin the group — unlike delete, whose removal the
    # next message undoes.
    second = call_handler(
        node._handle_group_message,
        encode_group_message(
            "are you still there?",
            message_id="gm-2",
            group_id=group_id,
            group_name="weekend",
            creator=sender_hash,
            members=members,
        ),
        remote_identity=sender,
    )
    assert second["ok"] is False
    assert second["error"]["code"] == "not_a_member"
    assert len(received) == 1
    assert len(node.inbox) == 1
    assert node.groups.get(group_id) is None
    assert node.groups.is_left(group_id) is True


def test_group_message_from_blocked_sender_dropped_silently(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    # Blocking wins over the allow-list: the sender gets a generic
    # delivery failure, never a signal that they are blocked.
    node._blocklist = node.blocklist.with_added(sender_hash)
    members = [OWN_HASH, sender_hash, THIRD_HASH]
    group_id = derive_group_id(sender_hash, "weekend", members)
    response = call_handler(
        node._handle_group_message,
        encode_group_message(
            "hi all",
            message_id="gm-blocked",
            group_id=group_id,
            group_name="weekend",
            creator=sender_hash,
            members=members,
        ),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "unavailable"
    assert received == []
    assert len(node.inbox) == 0
    # The group is not pinned from a blocked sender's message.
    assert node.groups.get(group_id) is None


def test_group_message_for_group_without_this_node_rejected(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    response = call_handler(
        node._handle_group_message,
        make_group_payload(
            sender_members=[sender_hash, OTHER_HASH, THIRD_HASH]
        ),
        remote_identity=sender,
    )
    assert response["ok"] is False
    assert response["error"]["code"] == "not_a_member"
    assert received == []
    assert len(node.inbox) == 0


def test_group_message_with_changed_roster_rejected(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    first_members = [OWN_HASH, sender_hash, THIRD_HASH]
    group_id = derive_group_id(sender_hash, "weekend", first_members)
    first = call_handler(
        node._handle_group_message,
        encode_group_message(
            "first",
            message_id="gm-1",
            group_id=group_id,
            group_name="weekend",
            creator=sender_hash,
            members=first_members,
        ),
        remote_identity=sender,
    )
    assert first["ok"] is True
    # Same id, different roster: the id no longer derives from the
    # roster, so the message is rejected at parse time — a changed
    # roster means a new group, and a member cannot fork the id.
    second = call_handler(
        node._handle_group_message,
        json.dumps(
            {
                "type": "group_message",
                "v": 1,
                "text": "second",
                "group": group_id,
                "group_name": "weekend",
                "creator": sender_hash,
                "members": [OWN_HASH, sender_hash, OTHER_HASH],
                "sent_at": 1.0,
                "id": "gm-2",
            }
        ).encode("utf-8"),
        remote_identity=sender,
    )
    assert second["ok"] is False
    assert second["error"]["code"] == "invalid_message"
    assert len(received) == 1


def test_group_message_successor_from_member_does_not_replace(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    creator, creator_hash = make_sender()
    member, member_hash = make_sender()
    first_members = [OWN_HASH, creator_hash, THIRD_HASH]
    group_id = derive_group_id(creator_hash, "weekend", first_members)
    first = call_handler(
        node._handle_group_message,
        encode_group_message(
            "first",
            message_id="gm-1",
            group_id=group_id,
            group_name="weekend",
            creator=creator_hash,
            members=first_members,
        ),
        remote_identity=creator,
    )
    assert first["ok"] is True
    assert len(node.groups) == 1
    # A member other than the creator claims a successor roster with
    # the creator's name on it: the message is accepted and the new
    # roster pinned, but the learned group stays — the creator field
    # is only a claim, so the member must not be able to hijack sends
    # by group name.
    forged_members = [OWN_HASH, creator_hash, member_hash]
    forged_id = derive_group_id(creator_hash, "weekend", forged_members)
    forged = call_handler(
        node._handle_group_message,
        encode_group_message(
            "forged",
            message_id="gm-2",
            group_id=forged_id,
            group_name="weekend",
            creator=creator_hash,
            members=forged_members,
        ),
        remote_identity=member,
    )
    assert forged["ok"] is True
    assert len(received) == 2
    assert len(node.groups) == 2
    assert node.groups.get(group_id) is not None
    assert node.groups.get(forged_id) is not None
    # The creator themself sending the same successor roster does
    # replace the predecessor.
    confirmed = call_handler(
        node._handle_group_message,
        encode_group_message(
            "confirmed",
            message_id="gm-3",
            group_id=forged_id,
            group_name="weekend",
            creator=creator_hash,
            members=forged_members,
        ),
        remote_identity=creator,
    )
    assert confirmed["ok"] is True
    assert node.groups.get(group_id) is None
    assert node.groups.get(forged_id) is not None
    assert len(node.groups) == 1
    assert node.groups.find("weekend")["id"] == forged_id


def test_group_message_stale_creator_message_does_not_replace_successor(
    tmp_path,
):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    creator, creator_hash = make_sender()
    first_members = [OWN_HASH, creator_hash, THIRD_HASH]
    group_id = derive_group_id(creator_hash, "weekend", first_members)
    first = call_handler(
        node._handle_group_message,
        encode_group_message(
            "first",
            message_id="gm-1",
            group_id=group_id,
            group_name="weekend",
            creator=creator_hash,
            members=first_members,
            sent_at=100.0,
        ),
        remote_identity=creator,
    )
    assert first["ok"] is True
    # The creator removes a member and sends with the new roster.
    new_members = [OWN_HASH, creator_hash]
    new_id = derive_group_id(creator_hash, "weekend", new_members)
    second = call_handler(
        node._handle_group_message,
        encode_group_message(
            "second",
            message_id="gm-2",
            group_id=new_id,
            group_name="weekend",
            creator=creator_hash,
            members=new_members,
            sent_at=200.0,
        ),
        remote_identity=creator,
    )
    assert second["ok"] is True
    assert node.groups.get(group_id) is None
    assert node.groups.find("weekend")["id"] == new_id
    # A queued copy of a creator message sent with the old roster
    # before the change arrives late: it is delivered, but the
    # successor stays pinned and name lookup keeps resolving to it.
    stale = call_handler(
        node._handle_group_message,
        encode_group_message(
            "stale",
            message_id="gm-3",
            group_id=group_id,
            group_name="weekend",
            creator=creator_hash,
            members=first_members,
            sent_at=150.0,
        ),
        remote_identity=creator,
    )
    assert stale["ok"] is True
    assert len(received) == 3
    assert node.groups.get(new_id) is not None
    assert node.groups.get(group_id) is not None
    assert node.groups.find("weekend")["id"] == new_id


def test_group_message_dedupes_by_id(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    payload = encode_group_message(
        "once",
        message_id="gm-dup",
        group_id=derive_group_id(sender_hash, "weekend", [OWN_HASH, sender_hash, THIRD_HASH]),
        group_name="weekend",
        creator=sender_hash,
        members=[OWN_HASH, sender_hash, THIRD_HASH],
    )
    first = call_handler(node._handle_group_message, payload, remote_identity=sender)
    second = call_handler(node._handle_group_message, payload, remote_identity=sender)
    assert first["duplicate"] is False
    assert second["duplicate"] is True
    assert len(received) == 1
    assert len(node.inbox) == 1


# -- relayed group messages --------------------------------------------------


def test_relayed_group_message_delivered_as_group(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    recipient = RNS.Identity()
    # The sealed envelope is addressed to this node: its identity is the
    # node destination, so the test recipient is the node's own identity.
    node.identity = recipient
    node.destination = SimpleNamespace(
        hexhash=destination_hash(recipient).hex(),
        hash=destination_hash(recipient),
    )
    own = destination_hash(recipient).hex()
    members = [own, sender_hash, THIRD_HASH]
    group_id = derive_group_id(sender_hash, "weekend", members)
    blob = seal_message(
        sender,
        sender_name="bob",
        recipient_public_key=recipient.get_public_key(),
        message_id="gm-relay-1",
        text="via a holder",
        sent_at=99.0,
        group={
            "id": group_id,
            "name": "weekend",
            "creator": sender_hash,
            "members": members,
        },
    )
    message = open_message(recipient, blob)
    assert message["group"]["id"] == group_id
    assert node._deliver_collected(message) is True
    assert len(received) == 1
    record = received[0]
    assert record["kind"] == "group"
    assert record["group"] == group_id
    assert record["relayed"] is True
    assert node.groups.get(group_id) is not None


def test_relayed_group_message_with_forked_roster_rejected(tmp_path):
    """A sealed envelope cannot fork an id either: the relayed group
    is validated against the derived id before delivery."""
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    recipient = RNS.Identity()
    node.identity = recipient
    node.destination = SimpleNamespace(
        hexhash=destination_hash(recipient).hex(),
        hash=destination_hash(recipient),
    )
    own = destination_hash(recipient).hex()
    members = [own, sender_hash, THIRD_HASH]
    real_id = derive_group_id(sender_hash, "weekend", members)
    forked_members = [own, sender_hash, OTHER_HASH]
    blob = seal_message(
        sender,
        sender_name="bob",
        recipient_public_key=recipient.get_public_key(),
        message_id="gm-relay-fork",
        text="forked roster",
        sent_at=99.0,
        group={
            "id": real_id,
            "name": "weekend",
            "creator": sender_hash,
            "members": forked_members,
        },
    )
    message = open_message(recipient, blob)
    assert node._deliver_collected(message) is False
    assert received == []
    assert len(node.inbox) == 0
    assert node.groups.get(real_id) is None


def test_relayed_group_message_from_non_member_rejected(tmp_path):
    received = []
    node = make_node(tmp_path, on_message=received.append)
    sender, sender_hash = make_sender()
    recipient = RNS.Identity()
    node.identity = recipient
    node.destination = SimpleNamespace(
        hexhash=destination_hash(recipient).hex(),
        hash=destination_hash(recipient),
    )
    own = destination_hash(recipient).hex()
    members = [own, OTHER_HASH, THIRD_HASH]
    blob = seal_message(
        sender,
        sender_name="mallory",
        recipient_public_key=recipient.get_public_key(),
        message_id="gm-relay-2",
        text="forged roster",
        sent_at=99.0,
        group={
            "id": derive_group_id(sender_hash, "weekend", members),
            "name": "weekend",
            "creator": sender_hash,
            # The sender is not on the roster it claims.
            "members": members,
        },
    )
    message = open_message(recipient, blob)
    assert node._deliver_collected(message) is False
    assert received == []
    assert len(node.inbox) == 0


def test_seal_open_without_group_has_no_group_field():
    sender = RNS.Identity()
    recipient = RNS.Identity()
    blob = seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id="plain-1",
        text="plain",
        sent_at=1.0,
    )
    assert "group" not in open_message(recipient, blob)


# -- outbox group context ----------------------------------------------------


def test_outbox_entry_group_roundtrip(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    members = [OWN_HASH, OTHER_HASH, THIRD_HASH]
    entry = box.enqueue(
        to=bytes.fromhex(OTHER_HASH),
        text="queued group leg",
        group={
            "id": derive_group_id(OWN_HASH, "weekend", members),
            "name": "weekend",
            "creator": OWN_HASH,
            "members": members,
        },
    )
    assert entry.group_id is not None
    assert entry.group_creator == OWN_HASH
    reloaded = Mailbox(tmp_path / "mailbox.json")
    again = reloaded.get_outbox(entry.id)
    assert again is not None
    assert again.group_id == entry.group_id
    assert again.group_name == "weekend"
    assert again.group_creator == OWN_HASH
    assert again.group_members == [OWN_HASH, OTHER_HASH, THIRD_HASH]
    assert again.to_dict()["group"]["id"] == entry.group_id
    assert again.to_dict()["group"]["creator"] == OWN_HASH


def test_outbox_entry_rejects_bad_group_context(tmp_path):
    box = Mailbox(tmp_path / "mailbox.json")
    with pytest.raises(Exception):
        box.enqueue(
            to=bytes.fromhex(OTHER_HASH),
            text="bad group",
            group={"id": "short", "name": "g", "members": [OWN_HASH]},
        )


# -- fan-out -----------------------------------------------------------------


def _stub_peer(hexhash, name="peer"):
    now = time.time()
    return Peer(
        destination_hash=bytes.fromhex(hexhash),
        name=name,
        hops=1,
        first_seen=now,
        last_seen=now,
        public_key=b"\x00" * 64,
    )


def test_send_group_message_fans_out_and_queues_offline(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()  # started enough for queueing
    group = node.groups.create(
        "weekend", [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )
    calls = []

    def fake_request(peer, path, payload, timeout, *, deadline=None):
        calls.append((peer.hexhash, path, payload, deadline))
        return {"ok": True}

    def fake_resolve(target, timeout=10.0):
        if target == OTHER_HASH:
            return _stub_peer(OTHER_HASH, "bob")
        raise NodeError("unreachable")

    node._request = fake_request  # type: ignore[method-assign]
    node.resolve_peer = fake_resolve  # type: ignore[method-assign]
    result = node.send_group_message(group, "hello all")
    assert result["ok"] is True
    assert result["group"] == group["id"]
    by_to = {entry["to"]: entry for entry in result["results"]}
    assert by_to[OTHER_HASH]["delivered"] is True
    assert by_to[THIRD_HASH]["queued"] is True
    assert len(calls) == 1
    assert calls[0][1] == "/tau/group"
    # The leg's deadline rides along, so one slow leg cannot
    # consume its whole budget stage by stage.
    assert calls[0][3] is not None
    parsed = parse_group_message(calls[0][2])
    assert parsed["text"] == "hello all"
    assert parsed["group"] == group["id"]
    assert parsed["members"] == [OWN_HASH, OTHER_HASH, THIRD_HASH]
    # The offline leg is queued with its group context intact.
    queued = node.mailbox.outbox_list()
    assert len(queued) == 1
    assert queued[0].to_hex == THIRD_HASH
    assert queued[0].group_id == group["id"]
    assert queued[0].group_creator == OWN_HASH
    assert queued[0].group_members == [OWN_HASH, OTHER_HASH, THIRD_HASH]

    # Each leg is registered for read receipts, like a peer DM: the
    # delivered leg under its wire id, so a recipient's receipt
    # verifies instead of being rejected as unknown. (The queued leg
    # is registered under the same id twice over: the outbox entry
    # reuses the wire id as its id, so the second registration is a
    # no-op, exactly as for a queued peer DM.)
    delivered_id = by_to[OTHER_HASH]["id"]
    assert node.mailbox.sent_record(delivered_id) is not None
    assert (
        node.mailbox.record_read(delivered_id, time.time(), reader_hash=OTHER_HASH)
        is True
    )
    assert node.mailbox.read_at(delivered_id) is not None
    third_records = [
        record for record in node.mailbox.sent_list() if record["to"] == THIRD_HASH
    ]
    assert len(third_records) == 1
    assert third_records[0]["id"] == by_to[THIRD_HASH]["id"]
    assert third_records[0]["id"] == queued[0].id


def test_send_group_message_skips_self_and_records_refusal(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()
    group = node.groups.create(
        "weekend", [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )

    def fake_request(peer, path, payload, timeout, *, deadline=None):
        from tau.node import NotAllowedError

        raise NotAllowedError("not allowed")

    node._request = fake_request  # type: ignore[method-assign]
    node.resolve_peer = lambda target, timeout=10.0: _stub_peer(  # type: ignore[method-assign]
        target, "peer"
    )
    result = node.send_group_message(group, "hello")
    # Neither leg is delivered or queued: both members refused, and the
    # sender never sends to itself. The fan-out reports failure.
    assert result["ok"] is False
    assert all(entry.get("error") == "not_allowed" for entry in result["results"])
    assert len(result["results"]) == 2
    assert node.mailbox.outbox_list() == []
    # A refused leg leaves no sent record behind: a later receipt
    # for its id must not verify.
    assert node.mailbox.sent_list() == []


def test_send_group_message_treats_not_a_member_as_final(tmp_path):
    from tau.node import NotAMemberError

    node = make_node(tmp_path)
    node.reticulum = object()
    group = node.groups.create(
        "weekend", [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )

    def fake_request(peer, path, payload, timeout, *, deadline=None):
        raise NotAMemberError("peer rejected the request: not a member")

    node._request = fake_request  # type: ignore[method-assign]
    node.resolve_peer = lambda target, timeout=10.0: _stub_peer(  # type: ignore[method-assign]
        target, "peer"
    )
    result = node.send_group_message(group, "hello")
    # A membership refusal is permanent: the legs are reported failed,
    # never queued for retry until their TTL.
    assert result["ok"] is False
    assert all(entry.get("error") == "not_a_member" for entry in result["results"])
    assert node.mailbox.outbox_list() == []


def test_send_group_message_keeps_results_when_outbox_full(tmp_path):
    from tau.mailbox import MailboxFull

    node = make_node(tmp_path)
    node.reticulum = object()
    group = node.groups.create(
        "weekend", [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )

    def unreachable(peer, path, payload, timeout, *, deadline=None):
        raise NodeError("unreachable")

    node._request = unreachable  # type: ignore[method-assign]
    node.resolve_peer = lambda target, timeout=10.0: _stub_peer(  # type: ignore[method-assign]
        target, "peer"
    )
    real_enqueue = node.mailbox.enqueue
    calls = {"n": 0}

    def flaky_enqueue(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] > 1:
            raise MailboxFull("outbox is full")
        return real_enqueue(*args, **kwargs)

    node.mailbox.enqueue = flaky_enqueue  # type: ignore[method-assign]
    result = node.send_group_message(group, "hello")
    # The first leg queued; the second leg's full outbox is a per-leg
    # failure, not a lost fan-out.
    assert result["ok"] is False
    assert len(result["results"]) == 2
    assert result["results"][0].get("queued") is True
    assert result["results"][1].get("ok") is False
    assert result["results"][1]["error"].startswith("could not queue")
    assert len(node.mailbox.outbox_list()) == 1
    # The queued leg is registered under its id (wire id and outbox
    # id are the same here); the leg that could not be queued leaves
    # no sent record behind.
    sent = node.mailbox.sent_list()
    assert len(sent) == 1
    assert sent[0]["to"] == OTHER_HASH
    assert sent[0]["id"] == result["results"][0]["id"]


def test_send_group_message_queues_legs_past_the_deadline(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()
    group = node.groups.create(
        "weekend", [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )

    def no_request(peer, path, payload, timeout, *, deadline=None):
        raise AssertionError("leg attempted past the deadline")

    node._request = no_request  # type: ignore[method-assign]
    # A zero timeout leaves no budget for any attempt: every leg must
    # still reach the outbox instead of being dropped to the deadline,
    # so the members get the message on a later retry.
    result = node.send_group_message(group, "hello", timeout=0.0)
    assert result["ok"] is True
    assert len(result["results"]) == 2
    assert all(entry.get("queued") is True for entry in result["results"])
    queued = node.mailbox.outbox_list()
    assert len(queued) == 2
    assert all(entry.group_id == group["id"] for entry in queued)
    assert all(
        node.mailbox.sent_record(entry["id"]) is not None
        for entry in result["results"]
    )


def test_send_group_message_slow_first_leg_does_not_starve_later_legs(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()
    group = node.groups.create(
        "weekend", [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )
    attempts = []

    def fake_resolve(target, timeout=10.0):
        return _stub_peer(target, "peer")

    def fake_request(peer, path, payload, timeout, *, deadline=None):
        attempts.append(peer.hexhash)
        if peer.hexhash == OTHER_HASH:
            # An offline member's path wait consumes its whole leg
            # budget before the attempt fails.
            time.sleep(1.2)
            raise NodeError("no path")
        return {"ok": True}

    node.resolve_peer = fake_resolve  # type: ignore[method-assign]
    node._request = fake_request  # type: ignore[method-assign]
    # The roster is attempted in sorted order (OTHER before THIRD).
    # With a single fan-out-wide deadline, the first leg burning its
    # budget left every later member queued without an attempt — the
    # regression behind the flaky
    # test_three_nodes_exchange_group_messages integration failure,
    # where an offline member sorting first starved the online one.
    result = node.send_group_message(group, "hello", timeout=1.0)
    by_to = {entry["to"]: entry for entry in result["results"]}
    assert by_to[OTHER_HASH]["queued"] is True
    assert by_to[THIRD_HASH]["delivered"] is True
    assert attempts == [OTHER_HASH, THIRD_HASH]


def test_send_group_message_rejects_oversized_text_before_any_leg(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()
    members = [OWN_HASH] + [f"{i:032x}" for i in range(1, 32)]
    group = node.groups.create("big", members, created_by=OWN_HASH)
    calls = []
    node._request = lambda peer, path, payload, timeout, **kwargs: calls.append(peer)  # type: ignore[method-assign]
    node.resolve_peer = lambda target, timeout=10.0: _stub_peer(  # type: ignore[method-assign]
        target, "peer"
    )
    # 2000 four-byte characters fit the DM text limit but exceed the
    # 8 KiB group request once the 32-member roster is included.
    with pytest.raises(NodeError, match="too large"):
        node.send_group_message(group, "\U0001d11e" * 2000)
    assert calls == []
    assert node.mailbox.outbox_list() == []


def test_deliver_entry_re_encodes_group_payload(tmp_path):
    node = make_node(tmp_path)
    node.reticulum = object()
    group = node.groups.create(
        "weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH
    )
    entry = node.mailbox.enqueue(
        to=bytes.fromhex(OTHER_HASH),
        text="retry me",
        group={
            "id": group["id"],
            "name": "weekend",
            "creator": OWN_HASH,
            "members": [OWN_HASH, OTHER_HASH],
        },
    )
    seen = []

    def fake_request(peer, path, payload, timeout, *, deadline=None):
        seen.append((path, parse_group_message(payload)))
        return {"ok": True}

    node._request = fake_request  # type: ignore[method-assign]
    assert node._deliver_entry(_stub_peer(OTHER_HASH), entry, 5.0) is True
    assert len(seen) == 1
    assert seen[0][0] == "/tau/group"
    assert seen[0][1]["text"] == "retry me"
    assert seen[0][1]["group"] == group["id"]
    assert node.mailbox.get_outbox(entry.id) is None


# -- CLI ---------------------------------------------------------------------


def test_parser_group_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(
        ["group", "create", "weekend", "--peer", "bob", "--peer", "carol"]
    )
    assert args.group_command == "create"
    assert args.name == "weekend"
    assert args.peer == ["bob", "carol"]

    send_args = parser.parse_args(["group", "send", "weekend", "hello all"])
    assert send_args.group_command == "send"
    assert send_args.group == "weekend"
    assert send_args.message == "hello all"

    list_args = parser.parse_args(["group", "list"])
    assert list_args.group_command == "list"

    delete_args = parser.parse_args(["group", "delete", "weekend"])
    assert delete_args.group_command == "delete"
    assert delete_args.group == "weekend"

    add_args = parser.parse_args(
        ["group", "add-member", "weekend", "--peer", "carol"]
    )
    assert add_args.group_command == "add-member"
    assert add_args.group == "weekend"
    assert add_args.peer == ["carol"]

    remove_args = parser.parse_args(
        ["group", "remove-member", "weekend", "--peer", "bob"]
    )
    assert remove_args.group_command == "remove-member"
    assert remove_args.group == "weekend"
    assert remove_args.peer == ["bob"]

    messages_args = parser.parse_args(["messages", "--group", "weekend"])
    assert messages_args.group == "weekend"


def test_group_list_on_empty_store(tmp_path, capsys):
    code = cli.main(["group", "list", "--data-dir", str(tmp_path / "node")])
    assert code == 0
    assert "No groups yet" in capsys.readouterr().out


def test_group_list_json(tmp_path, capsys):
    config = TauConfig.resolve(data_dir=tmp_path / "node", name="alice")
    store = GroupStore(config.groups_path)
    store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    code = cli.main(
        ["group", "list", "--data-dir", str(tmp_path / "node"), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["groups"][0]["name"] == "weekend"
    assert len(payload["groups"][0]["members"]) == 2


def test_group_delete_via_cli(tmp_path, capsys):
    config = TauConfig.resolve(data_dir=tmp_path / "node", name="alice")
    store = GroupStore(config.groups_path)
    group = store.create("weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    code = cli.main(
        ["group", "delete", "weekend", "--data-dir", str(tmp_path / "node")]
    )
    assert code == 0
    assert "deleted group weekend" in capsys.readouterr().out
    # list_groups reloads from disk, so it sees the other process's delete.
    assert store.list_groups() == []
    assert (
        cli.main(["group", "delete", "weekend", "--data-dir", str(tmp_path / "node")])
        != 0
    )


def _seed_learned_group(tmp_path):
    config = TauConfig.resolve(data_dir=tmp_path / "node", name="alice")
    store = GroupStore(config.groups_path)
    group = store.record(
        derive_group_id(OTHER_HASH, "theirs", [OWN_HASH, OTHER_HASH, THIRD_HASH]),
        "theirs",
        [OWN_HASH, OTHER_HASH, THIRD_HASH],
        creator=OTHER_HASH,
    )
    return config, store, group


def test_group_leave_via_cli(tmp_path, capsys):
    _, store, group = _seed_learned_group(tmp_path)
    code = cli.main(
        ["group", "leave", "theirs", "--data-dir", str(tmp_path / "node")]
    )
    assert code == 0
    assert "left group theirs" in capsys.readouterr().out
    assert store.list_groups() == []
    # list_groups/left_groups reload from disk, so they see the CLI's
    # leave; is_left alone reads the in-memory list.
    assert [entry["id"] for entry in store.left_groups()] == [group["id"]]
    # Leaving twice is not an error.
    code = cli.main(
        ["group", "leave", group["id"], "--data-dir", str(tmp_path / "node")]
    )
    assert code == 0
    assert "already left" in capsys.readouterr().out
    # `group left` lists it, in text and JSON — the text listing prints
    # the full id, so it round-trips straight into `group rejoin`.
    code = cli.main(["group", "left", "--data-dir", str(tmp_path / "node")])
    assert code == 0
    out = capsys.readouterr().out
    assert group["id"] in out
    printed_id = out.strip().splitlines()[0].split(" ")[0]
    assert printed_id == group["id"]
    code = cli.main(
        ["group", "left", "--json", "--data-dir", str(tmp_path / "node")]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["left"][0]["id"] == group["id"]
    # `group rejoin` restores it, and the next message pins it again.
    code = cli.main(
        ["group", "rejoin", printed_id, "--data-dir", str(tmp_path / "node")]
    )
    assert code == 0
    assert "rejoined" in capsys.readouterr().out
    assert store.left_groups() == []
    assert (
        store.record(
            group["id"],
            "theirs",
            [OWN_HASH, OTHER_HASH, THIRD_HASH],
            creator=OTHER_HASH,
        )
        is not None
    )
    # --json leave round-trips.
    code = cli.main(
        ["group", "leave", "--json", "theirs", "--data-dir", str(tmp_path / "node")]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"left": group["id"], "name": "theirs"}
    # Unknown groups and never-left ids still error.
    assert (
        cli.main(["group", "leave", "nope", "--data-dir", str(tmp_path / "node")])
        != 0
    )
    assert (
        cli.main(
            ["group", "rejoin", "00" * 16, "--data-dir", str(tmp_path / "node")]
        )
        != 0
    )
    assert (
        cli.main(["group", "rejoin", "xyz", "--data-dir", str(tmp_path / "node")])
        != 0
    )
    assert "invalid group id" in capsys.readouterr().err


def test_group_leave_via_cli_leaves_pinned_successor_too(tmp_path, capsys):
    config = TauConfig.resolve(data_dir=tmp_path / "node", name="alice")
    store = GroupStore(config.groups_path)
    first = store.record(
        derive_group_id(OTHER_HASH, "theirs", [OWN_HASH, OTHER_HASH, THIRD_HASH]),
        "theirs",
        [OWN_HASH, OTHER_HASH, THIRD_HASH],
        creator=OTHER_HASH,
    )
    grown_id = derive_group_id(
        OTHER_HASH, "theirs", [OWN_HASH, OTHER_HASH, THIRD_HASH, "12" * 16]
    )
    store.record(
        grown_id,
        "theirs",
        [OWN_HASH, OTHER_HASH, THIRD_HASH, "12" * 16],
        creator=OTHER_HASH,
        sender=THIRD_HASH,
    )
    code = cli.main(
        ["group", "leave", "--json", "theirs", "--data-dir", str(tmp_path / "node")]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["left"] == first["id"]
    assert payload["also_left"] == [grown_id]
    assert store.list_groups() == []
    code = cli.main(
        ["group", "left", "--json", "--data-dir", str(tmp_path / "node")]
    )
    assert json.loads(capsys.readouterr().out)["count"] == 2


def test_group_leave_via_cli_resolves_32_char_name(tmp_path, capsys):
    config = TauConfig.resolve(data_dir=tmp_path / "node", name="alice")
    store = GroupStore(config.groups_path)
    group = store.record(
        derive_group_id(OTHER_HASH, "n" * 32, [OWN_HASH, OTHER_HASH, THIRD_HASH]),
        "n" * 32,
        [OWN_HASH, OTHER_HASH, THIRD_HASH],
        creator=OTHER_HASH,
    )
    code = cli.main(
        ["group", "leave", "n" * 32, "--data-dir", str(tmp_path / "node")]
    )
    assert code == 0
    assert store.list_groups() == []
    assert [entry["id"] for entry in store.left_groups()] == [group["id"]]


def test_group_leave_refuses_group_this_node_created(tmp_path, capsys):
    # No identity file in this data dir: the refusal must come from
    # the group's `local` flag alone, not from loading the identity.
    config = TauConfig.resolve(data_dir=tmp_path / "node", name="alice")
    store = GroupStore(config.groups_path)
    store.create("mine", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH)
    code = cli.main(["group", "leave", "mine", "--data-dir", str(tmp_path / "node")])
    assert code == 1
    assert "delete your own group" in capsys.readouterr().err
    assert len(store.list_groups()) == 1


class _FakeGroupNode:
    """Stub TauNode for `tau group add-member` / `remove-member`.

    Resolves peers from the on-disk registry only — no network — and
    answers as the group creator.
    """

    def __init__(self, config, **kwargs):
        self.config = config
        self.groups = GroupStore(config.groups_path)
        self.registry = PeerRegistry(config.peers_path)
        self.started = False

    @property
    def destination_hexhash(self):
        return OWN_HASH

    def start(self, **kwargs):
        self.started = True

    def stop(self):
        self.started = False

    def resolve_peer(self, target, timeout=10.0):
        peer = self.registry.resolve(target)
        if peer is None:
            raise NodeError(f"unknown peer: {target!r}")
        return peer


def _seed_group_cli(tmp_path, monkeypatch, members=(OWN_HASH, OTHER_HASH)):
    config = TauConfig.resolve(data_dir=tmp_path / "node", name="alice")
    store = GroupStore(config.groups_path)
    group = store.create("weekend", list(members), created_by=OWN_HASH)
    registry = PeerRegistry(config.peers_path)
    for hexhash, name in (
        (OWN_HASH, "alice"),
        (OTHER_HASH, "bob"),
        (THIRD_HASH, "carol"),
    ):
        registry.upsert(bytes.fromhex(hexhash), b"\x00" * 64, name=name)
    registry.save()
    monkeypatch.setattr(cli, "TauNode", _FakeGroupNode)
    return config, group


def test_group_add_member_via_cli(tmp_path, capsys, monkeypatch):
    config, group = _seed_group_cli(tmp_path, monkeypatch)
    code = cli.main(
        [
            "group",
            "add-member",
            "weekend",
            "--peer",
            "carol",
            "--data-dir",
            str(tmp_path / "node"),
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "carol added to group weekend" in out
    assert group["id"][:8] in out
    store = GroupStore(config.groups_path)
    assert len(store) == 1
    grown = store.list_groups()[0]
    assert grown["members"] == [OWN_HASH, OTHER_HASH, THIRD_HASH]
    assert grown["id"] == derive_group_id(
        OWN_HASH, "weekend", [OWN_HASH, OTHER_HASH, THIRD_HASH]
    )
    assert store.get(group["id"]) is None


def test_group_remove_member_json(tmp_path, capsys, monkeypatch):
    config, group = _seed_group_cli(
        tmp_path, monkeypatch, members=(OWN_HASH, OTHER_HASH, THIRD_HASH)
    )
    code = cli.main(
        [
            "group",
            "remove-member",
            "weekend",
            "--peer",
            "carol",
            "--data-dir",
            str(tmp_path / "node"),
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["previous_id"] == group["id"]
    assert [member["hash"] for member in payload["group"]["members"]] == [
        OWN_HASH,
        OTHER_HASH,
    ]
    assert payload["group"]["id"] == derive_group_id(
        OWN_HASH, "weekend", [OWN_HASH, OTHER_HASH]
    )
    store = GroupStore(config.groups_path)
    assert len(store) == 1


def test_group_remove_member_by_hash_without_registry(
    tmp_path, capsys, monkeypatch
):
    config, group = _seed_group_cli(
        tmp_path, monkeypatch, members=(OWN_HASH, OTHER_HASH, THIRD_HASH)
    )
    # Carol left the registry, but her hash is in the roster: resolving
    # her would wait out discovery, so removal by hash must not need it.
    # (The stub node raises for any peer it cannot resolve, so this test
    # fails outright without the roster fast path.)
    registry = PeerRegistry(config.peers_path)
    registry.forget(THIRD_HASH)
    registry.save()
    code = cli.main(
        [
            "group",
            "remove-member",
            "weekend",
            "--peer",
            THIRD_HASH,
            "--data-dir",
            str(tmp_path / "node"),
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "removed from group weekend" in out
    store = GroupStore(config.groups_path)
    assert store.list_groups()[0]["members"] == [OWN_HASH, OTHER_HASH]


def test_group_change_members_refusals(tmp_path, capsys, monkeypatch):
    config, group = _seed_group_cli(tmp_path, monkeypatch)
    data_dir = str(tmp_path / "node")
    store = GroupStore(config.groups_path)
    learned_id = derive_group_id(OTHER_HASH, "theirs", [OTHER_HASH, THIRD_HASH])
    store.record(
        learned_id, "theirs", [OTHER_HASH, THIRD_HASH], creator=OTHER_HASH
    )

    def _run(argv):
        code = cli.main([*argv, "--data-dir", data_dir])
        assert code != 0
        return capsys.readouterr().err

    assert "already a member" in _run(
        ["group", "add-member", "weekend", "--peer", "bob"]
    )
    assert "unknown peer" in _run(
        ["group", "add-member", "weekend", "--peer", "mallory"]
    )
    assert "only the creator" in _run(
        ["group", "add-member", "theirs", "--peer", "carol"]
    )
    assert "not a member" in _run(
        ["group", "remove-member", "weekend", "--peer", "carol"]
    )
    assert "cannot remove the creator" in _run(
        ["group", "remove-member", "weekend", "--peer", "alice"]
    )
    # Refusals leave the store untouched.
    assert store.list_groups() != []
    assert store.get(group["id"]) == group


def test_messages_group_filter(tmp_path, capsys):
    config = TauConfig.resolve(data_dir=tmp_path / "node", name="alice")
    inbox = Inbox(config.inbox_path)
    group = GroupStore(config.groups_path).create(
        "weekend", [OWN_HASH, OTHER_HASH], created_by=OWN_HASH
    )
    inbox.append(
        {
            "from": "bob",
            "destination": OTHER_HASH,
            "text": "group hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "g-1",
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
        }
    )
    inbox.append(
        {
            "from": "bob",
            "destination": OTHER_HASH,
            "text": "plain hello",
            "sent_at": 11.0,
            "received_at": 21.0,
            "id": "m-1",
        }
    )
    code = cli.main(
        [
            "messages",
            "--group",
            "weekend",
            "--data-dir",
            str(tmp_path / "node"),
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["messages"][0]["text"] == "group hello"

    code = cli.main(
        [
            "messages",
            "--group",
            "nope",
            "--data-dir",
            str(tmp_path / "node"),
            "--json",
        ]
    )
    assert code != 0
