"""Ignored-groups list: module behaviour and CLI behaviour."""

from __future__ import annotations

import json
import os
import stat

import pytest

from tau import cli
from tau.groups import GroupStore
from tau.config import TauConfig
from tau.ignored_groups import (
    IgnoredGroups,
    IgnoredGroupsError,
    load_ignored_group_ids,
    load_ignored_groups,
    save_ignored_groups,
)
from tau.inbox import Inbox


# -- module ---------------------------------------------------------------

GROUP_A = "aa" * 16
GROUP_B = "bb" * 16


def test_missing_file_ignores_nothing(tmp_path):
    ignored = load_ignored_groups(tmp_path / "ignored-groups.json")
    assert ignored.ignored == frozenset()
    assert ignored.is_ignored(GROUP_A) is False
    assert ignored.is_ignored(None) is False


def test_unreadable_location_raises(tmp_path):
    # A path that cannot even be statted (here: a file where a
    # directory should be) is not "missing" — it raises.
    blocker = tmp_path / "blocker"
    blocker.write_text("x", encoding="utf-8")
    with pytest.raises(IgnoredGroupsError):
        load_ignored_groups(blocker / "ignored-groups.json")


def test_ignored_membership_is_case_insensitive():
    ignored = IgnoredGroups(ignored=frozenset({GROUP_A}))
    assert ignored.is_ignored(GROUP_A) is True
    assert ignored.is_ignored(GROUP_A.upper()) is True
    assert ignored.is_ignored(GROUP_B) is False
    assert ignored.is_ignored("not-a-group") is False
    assert ignored.is_ignored(None) is False


def test_save_load_roundtrip_and_mode(tmp_path):
    path = tmp_path / "ignored-groups.json"
    saved = save_ignored_groups(
        path, IgnoredGroups(ignored=frozenset({GROUP_B, GROUP_A}))
    )
    assert saved == path
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    loaded = load_ignored_groups(path)
    assert loaded.ignored == frozenset({GROUP_A, GROUP_B})
    assert loaded.to_dict() == {"ignored": sorted((GROUP_A, GROUP_B))}


def test_malformed_documents_raise(tmp_path):
    path = tmp_path / "ignored-groups.json"
    path.write_text(json.dumps({"ignored": "weekend"}), encoding="utf-8")
    with pytest.raises(IgnoredGroupsError):
        load_ignored_groups(path)
    path.write_text(json.dumps({"other": []}), encoding="utf-8")
    with pytest.raises(IgnoredGroupsError):
        load_ignored_groups(path)
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(IgnoredGroupsError):
        load_ignored_groups(path)


def test_malformed_group_ids_raise(tmp_path):
    path = tmp_path / "ignored-groups.json"
    # Too short, not hex, and not a string are all malformed — a bad
    # id must not silently become a filter that matches nothing.
    for bad in (["abc"], ["zz" * 16], [123], ["aa" * 15 + "zz"]):
        path.write_text(json.dumps({"ignored": bad}), encoding="utf-8")
        with pytest.raises(IgnoredGroupsError):
            load_ignored_groups(path)


def test_ignored_set_fails_open_on_corrupt_file(tmp_path, caplog):
    # Ignoring is a display convenience, not a security boundary: a
    # corrupt file shows everything (with a warning) instead of
    # hiding mail the owner never saw.
    path = tmp_path / "ignored-groups.json"
    path.write_text("{not json", encoding="utf-8")
    assert load_ignored_group_ids(path) == frozenset()
    assert "unusable" in caplog.text


def test_with_added_and_removed_are_idempotent():
    ignored = IgnoredGroups()
    added = ignored.with_added(GROUP_A)
    assert added.ignored == frozenset({GROUP_A})
    assert added.with_added(GROUP_A).ignored == frozenset({GROUP_A})
    removed = added.with_removed(GROUP_A)
    assert removed.ignored == frozenset()
    assert removed.with_removed(GROUP_A).ignored == frozenset()
    assert ignored.ignored == frozenset()


# -- CLI ------------------------------------------------------------------


def _seed_group_and_dm(tmp_path):
    from tau.identity import destination_hash
    from tau.peers import PeerRegistry
    import RNS

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    senders = {}
    inbox = Inbox(config.inbox_path)
    for name in ("alice", "carol"):
        sender = RNS.Identity()
        sender_hash = destination_hash(sender)
        registry = PeerRegistry(config.peers_path)
        registry.upsert(sender_hash, sender.get_public_key(), name=name, now=100.0)
        registry.save()
        senders[name] = sender_hash.hex()
    # The group post arrives first (older seq), then alice's DM: the
    # hidden post is older than a shown DM from the same sender, which
    # is the case the read-cursor cap has to handle.
    group = GroupStore(config.groups_path).create(
        "weekend",
        [senders["alice"], senders["carol"]],
        created_by=senders["alice"],
    )
    inbox.append(
        {
            "from": "alice",
            "destination": senders["alice"],
            "text": "group hello",
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "m-group",
        }
    )
    inbox.append(
        {
            "from": "alice",
            "destination": senders["alice"],
            "text": "first hello",
            "sent_at": 10.0,
            "received_at": 23.0,
            "id": "m-alice",
        }
    )
    inbox.append(
        {
            "from": "carol",
            "destination": senders["carol"],
            "text": "hi from carol",
            "sent_at": 10.0,
            "received_at": 24.0,
            "id": "m-carol",
        }
    )
    return data_dir, senders, group["id"]


def test_parser_group_ignore_arguments():
    parser = cli.build_parser()
    ignore = parser.parse_args(["group", "ignore", "weekend"])
    assert ignore.group_command == "ignore"
    assert ignore.group == "weekend"
    unignore = parser.parse_args(["group", "unignore", "weekend"])
    assert unignore.group_command == "unignore"
    assert unignore.group == "weekend"
    ignored = parser.parse_args(["group", "ignored"])
    assert ignored.group_command == "ignored"
    messages = parser.parse_args(["messages", "--ignored"])
    assert messages.ignored is True
    plain = parser.parse_args(["messages"])
    assert plain.ignored is False


def test_ignore_unignore_roundtrip(tmp_path, capsys):
    data_dir, _, _ = _seed_group_and_dm(tmp_path)

    code = cli.main(["group", "ignore", "weekend", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "Ignored group weekend" in out

    # Idempotent: ignoring again says so and exits 0.
    code = cli.main(["group", "ignore", "weekend", "--data-dir", str(data_dir)])
    assert code == 0
    assert "already ignored" in capsys.readouterr().out

    code = cli.main(["group", "ignored", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "weekend" in out
    assert "1 message(s)" in out

    # The group list marks the ignored group.
    code = cli.main(["group", "list", "--data-dir", str(data_dir)])
    assert code == 0
    assert "[ignored]" in capsys.readouterr().out

    code = cli.main(["group", "unignore", "weekend", "--data-dir", str(data_dir)])
    assert code == 0
    assert "Restored group weekend" in capsys.readouterr().out

    # Idempotent the other way too.
    code = cli.main(["group", "unignore", "weekend", "--data-dir", str(data_dir)])
    assert code == 0
    assert "was not ignored" in capsys.readouterr().out

    code = cli.main(["group", "ignored", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No ignored groups" in capsys.readouterr().out


def test_ignore_json_roundtrip(tmp_path, capsys):
    data_dir, _, group_id = _seed_group_and_dm(tmp_path)

    code = cli.main(
        ["group", "ignore", "weekend", "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "group": group_id,
        "name": "weekend",
        "ignored": True,
        "changed": True,
    }
    code = cli.main(
        ["group", "ignore", "weekend", "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    assert json.loads(capsys.readouterr().out)["changed"] is False

    code = cli.main(["group", "ignored", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["ignored"][0]["name"] == "weekend"
    assert payload["ignored"][0]["messages"] == 1

    code = cli.main(["group", "list", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["groups"][0]["ignored"] is True


def test_messages_hides_ignored_group_and_reports_unread(tmp_path, capsys):
    data_dir, _, _ = _seed_group_and_dm(tmp_path)
    cli.main(["group", "ignore", "weekend", "--data-dir", str(data_dir)])
    capsys.readouterr()

    # Default listing hides the group post but reports its unread mail.
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "group hello" not in out
    assert "first hello" in out
    assert "hi from carol" in out
    assert "1 unread in ignored groups" in out

    # JSON carries the hidden unread count (the group post is still
    # unread: the default listing never marked its hidden mail read).
    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 2
    assert payload["ignored_unread"] == 1

    # --ignored shows only the ignored group's mail — and reading it
    # there marks it read, so the hidden count drops to zero afterwards.
    code = cli.main(["messages", "--data-dir", str(data_dir), "--ignored"])
    assert code == 0
    out = capsys.readouterr().out
    assert "group hello" in out
    assert "first hello" not in out
    assert "hi from carol" not in out
    assert "ignored groups" not in out

    code = cli.main(["messages", "--data-dir", str(data_dir), "--ignored", "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["ignored_unread"] == 0


def test_messages_group_filter_shows_ignored_group(tmp_path, capsys):
    data_dir, _, _ = _seed_group_and_dm(tmp_path)
    cli.main(["group", "ignore", "weekend", "--data-dir", str(data_dir)])
    capsys.readouterr()

    # --group is an explicit view: it shows the ignored group's posts.
    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--group", "weekend"]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "group hello" in out
    assert "first hello" not in out


def test_messages_search_still_finds_ignored_group(tmp_path, capsys):
    data_dir, _, _ = _seed_group_and_dm(tmp_path)
    cli.main(["group", "ignore", "weekend", "--data-dir", str(data_dir)])
    capsys.readouterr()

    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--search", "group hello"]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "group hello" in out
    assert "ignored groups" not in out


def test_ignored_group_post_is_not_marked_read(tmp_path, capsys):
    # Hiding a group's posts must not clear their unread state: the
    # footer keeps reporting them on every listing until the owner looks.
    data_dir, _, _ = _seed_group_and_dm(tmp_path)
    cli.main(["group", "ignore", "weekend", "--data-dir", str(data_dir)])
    capsys.readouterr()

    for _ in range(2):
        code = cli.main(["messages", "--data-dir", str(data_dir)])
        assert code == 0
        assert "1 unread in ignored groups" in capsys.readouterr().out


def test_dm_view_does_not_clear_hidden_group_post_unread(tmp_path, capsys):
    # The read cursor is per-sender, shared by DMs and group posts:
    # showing alice's newer DM must not mark her older hidden group
    # post read.  The cursor stops just before it.
    data_dir, _, _ = _seed_group_and_dm(tmp_path)
    cli.main(["group", "ignore", "weekend", "--data-dir", str(data_dir)])
    capsys.readouterr()

    code = cli.main(["messages", "--data-dir", str(data_dir), "--peer", "alice"])
    assert code == 0
    out = capsys.readouterr().out
    assert "first hello" in out
    assert "group hello" not in out
    assert "1 unread in ignored groups" in out

    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ignored_unread"] == 1

    # Once the group post is read via --ignored, the next default
    # listing advances past the DM too.
    code = cli.main(["messages", "--data-dir", str(data_dir), "--ignored"])
    assert code == 0
    assert "group hello" in capsys.readouterr().out

    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ignored_unread"] == 0
    assert payload["count"] == 2
    # The listing annotates read state before its own mark-read, so
    # the DM still shows unread here; the next listing sees it read.
    # (Newest first, so the DM is the second entry.)
    dm = next(entry for entry in payload["messages"] if entry["id"] == "m-alice")
    assert dm["unread"] is True

    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert all(entry["unread"] is False for entry in payload["messages"])


def test_ignore_unknown_group_errors(tmp_path, capsys):
    data_dir, _, _ = _seed_group_and_dm(tmp_path)
    code = cli.main(["group", "ignore", "nobody", "--data-dir", str(data_dir)])
    assert code == 1
    assert "unknown group" in capsys.readouterr().err


def test_ignore_takes_the_lock(tmp_path, capsys):
    data_dir, _, _ = _seed_group_and_dm(tmp_path)
    code = cli.main(["group", "ignore", "weekend", "--data-dir", str(data_dir)])
    assert code == 0
    capsys.readouterr()
    assert (data_dir / ".ignored-groups.json.lock").exists()


def _seed_orphan_ignored(tmp_path, group_id):
    # An ignored id whose group record is gone — as after a backup
    # restore that brought ignored-groups.json without groups.json.
    from tau.ignored_groups import save_ignored_groups

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    save_ignored_groups(
        config.ignored_groups_path,
        IgnoredGroups(ignored=frozenset({group_id})),
    )
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": "aa" * 16,
            "text": "orphan group hello",
            "kind": "group",
            "group": group_id,
            "group_name": "weekend",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "m-orphan",
        }
    )
    return data_dir


def test_unignore_orphan_id_by_full_id_and_prefix(tmp_path, capsys):
    orphan = "cc" * 16
    data_dir = _seed_orphan_ignored(tmp_path, orphan)

    code = cli.main(["group", "ignored", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "unknown" in out
    assert "1 message(s)" in out

    # A unique id prefix resolves the orphan too.
    code = cli.main(
        ["group", "unignore", orphan[:8], "--data-dir", str(data_dir)]
    )
    assert code == 0
    assert "Restored group unknown" in capsys.readouterr().out

    code = cli.main(["group", "ignored", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No ignored groups" in capsys.readouterr().out

    # And the full id works when seeded again.
    data_dir = _seed_orphan_ignored(tmp_path, orphan)
    code = cli.main(["group", "unignore", orphan, "--data-dir", str(data_dir)])
    assert code == 0
    assert "Restored group unknown" in capsys.readouterr().out


def test_unignore_orphan_prefix_errors(tmp_path, capsys):
    first = "ab" * 16
    second = "ab" * 15 + "cd"
    data_dir = _seed_orphan_ignored(tmp_path, first)
    from tau.ignored_groups import load_ignored_groups, save_ignored_groups

    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    ignored = load_ignored_groups(config.ignored_groups_path)
    save_ignored_groups(
        config.ignored_groups_path, ignored.with_added(second)
    )

    # A prefix matching two ignored ids is ambiguous.
    code = cli.main(["group", "unignore", "abab", "--data-dir", str(data_dir)])
    assert code == 1
    assert "2 ignored groups match the id prefix" in capsys.readouterr().err

    # An id that is ignored nowhere reports the original unknown group.
    code = cli.main(
        ["group", "unignore", "dd" * 16, "--data-dir", str(data_dir)]
    )
    assert code == 1
    assert "unknown group" in capsys.readouterr().err


def test_group_delete_keeps_ignored_mail_hidden(tmp_path, capsys):
    # Deleting an ignored group frees capacity; it does not un-ignore
    # it. Its stored posts stay hidden, and the id stays removable
    # through the orphan fallback.
    data_dir, _, group_id = _seed_group_and_dm(tmp_path)
    cli.main(["group", "ignore", "weekend", "--data-dir", str(data_dir)])
    capsys.readouterr()

    code = cli.main(["group", "delete", "weekend", "--data-dir", str(data_dir)])
    assert code == 0
    assert "deleted group weekend" in capsys.readouterr().out

    code = cli.main(["group", "ignored", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "unknown" in out
    assert "1 message(s)" in out

    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "group hello" not in out
    assert "1 unread in ignored groups" in out

    code = cli.main(["group", "unignore", group_id, "--data-dir", str(data_dir)])
    assert code == 0
    assert "Restored group unknown" in capsys.readouterr().out

    code = cli.main(["group", "ignored", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No ignored groups" in capsys.readouterr().out


def test_unignore_ambiguous_name_is_rejected(tmp_path, capsys):
    # Two stored groups share the name "dead", and an orphaned
    # ignored id shares the same prefix: the query is ambiguous and
    # must be rejected, not silently resolved against the orphan.
    from tau.identity import destination_hash
    from tau.peers import PeerRegistry
    import RNS

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    senders = {}
    for name in ("alice", "carol", "dave"):
        identity = RNS.Identity()
        sender_hash = destination_hash(identity)
        registry = PeerRegistry(config.peers_path)
        registry.upsert(sender_hash, identity.get_public_key(), name=name, now=100.0)
        registry.save()
        senders[name] = sender_hash.hex()
    store = GroupStore(config.groups_path)
    store.create(
        "dead", [senders["alice"], senders["carol"]], created_by=senders["alice"]
    )
    store.create(
        "dead", [senders["alice"], senders["dave"]], created_by=senders["alice"]
    )
    orphan = "deadbeef" * 4
    data_dir = _seed_orphan_ignored(tmp_path, orphan)

    code = cli.main(["group", "unignore", "dead", "--data-dir", str(data_dir)])
    assert code == 1
    assert "2 groups are named 'dead'" in capsys.readouterr().err

    # The orphan is still ignored: nothing was unignored by mistake.
    code = cli.main(["group", "ignored", "--data-dir", str(data_dir)])
    assert code == 0
    assert "1 message(s)" in capsys.readouterr().out
