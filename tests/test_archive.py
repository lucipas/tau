"""Archive list: module behaviour and CLI behaviour."""

from __future__ import annotations

import json
import os
import stat

import pytest

from tau import cli
from tau.archive import (
    ArchiveError,
    ArchiveList,
    load_archive,
    load_archived_set,
    save_archive,
)
from tau.config import TauConfig
from tau.inbox import Inbox


# -- module ---------------------------------------------------------------

ALICE = "aa" * 16
BOB = "bb" * 16


def test_missing_file_archives_nobody(tmp_path):
    archive = load_archive(tmp_path / "archived.json")
    assert archive.archived == frozenset()
    assert archive.is_archived(ALICE) is False
    assert archive.is_archived(None) is False


def test_unreadable_location_raises(tmp_path):
    # A path that cannot even be statted (here: a file where a
    # directory should be) is not "missing" — it raises.
    blocker = tmp_path / "blocker"
    blocker.write_text("x", encoding="utf-8")
    with pytest.raises(ArchiveError):
        load_archive(blocker / "archived.json")


def test_archived_set_membership_is_case_insensitive():
    archive = ArchiveList(archived=frozenset({ALICE}))
    assert archive.is_archived(ALICE) is True
    assert archive.is_archived(ALICE.upper()) is True
    assert archive.is_archived(BOB) is False
    assert archive.is_archived("not-a-hash") is False
    assert archive.is_archived(None) is False


def test_save_load_roundtrip_and_mode(tmp_path):
    path = tmp_path / "archived.json"
    saved = save_archive(path, ArchiveList(archived=frozenset({BOB, ALICE})))
    assert saved == path
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    loaded = load_archive(path)
    assert loaded.archived == frozenset({ALICE, BOB})
    assert loaded.to_dict() == {"archived": sorted((ALICE, BOB))}


def test_malformed_documents_raise(tmp_path):
    path = tmp_path / "archived.json"
    path.write_text(json.dumps({"archived": "alice"}), encoding="utf-8")
    with pytest.raises(ArchiveError):
        load_archive(path)
    path.write_text(json.dumps({"other": []}), encoding="utf-8")
    with pytest.raises(ArchiveError):
        load_archive(path)
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(ArchiveError):
        load_archive(path)


def test_archived_set_fails_open_on_corrupt_file(tmp_path, caplog):
    # Archive is a display convenience, not a security boundary: a
    # corrupt file shows everything (with a warning) instead of
    # hiding mail the owner never saw.
    path = tmp_path / "archived.json"
    path.write_text("{not json", encoding="utf-8")
    assert load_archived_set(path) == frozenset()
    assert "unusable" in caplog.text


def test_with_added_and_removed_are_idempotent():
    archive = ArchiveList()
    added = archive.with_added(ALICE)
    assert added.archived == frozenset({ALICE})
    assert added.with_added(ALICE).archived == frozenset({ALICE})
    removed = added.with_removed(ALICE)
    assert removed.archived == frozenset()
    assert removed.with_removed(ALICE).archived == frozenset()
    assert archive.archived == frozenset()


# -- CLI ------------------------------------------------------------------

def _seed_two_peers(tmp_path):
    from tau.identity import destination_hash
    from tau.peers import PeerRegistry
    import RNS

    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    senders = {}
    inbox = Inbox(config.inbox_path)
    for name, text, received_at in (
        ("alice", "first hello", 20.0),
        ("carol", "hi from carol", 21.0),
    ):
        sender = RNS.Identity()
        sender_hash = destination_hash(sender)
        registry = PeerRegistry(config.peers_path)
        registry.upsert(sender_hash, sender.get_public_key(), name=name, now=100.0)
        registry.save()
        inbox.append(
            {
                "from": name,
                "destination": sender_hash.hex(),
                "text": text,
                "sent_at": 10.0,
                "received_at": received_at,
                "id": f"m-{name}",
            }
        )
        senders[name] = sender_hash.hex()
    return data_dir, senders


def test_parser_archive_arguments():
    parser = cli.build_parser()
    archive = parser.parse_args(["archive", "alice"])
    assert archive.command == "archive"
    assert archive.peer == "alice"
    unarchive = parser.parse_args(["unarchive", "alice"])
    assert unarchive.command == "unarchive"
    assert unarchive.peer == "alice"
    archived = parser.parse_args(["archived"])
    assert archived.command == "archived"
    messages = parser.parse_args(["messages", "--archived"])
    assert messages.archived is True
    plain = parser.parse_args(["messages"])
    assert plain.archived is False


def test_archive_unarchive_roundtrip(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)

    code = cli.main(["archive", "--data-dir", str(data_dir), "alice"])
    assert code == 0
    out = capsys.readouterr().out
    assert "Archived conversation with alice" in out

    # Idempotent: archiving again says so and exits 0.
    code = cli.main(["archive", "--data-dir", str(data_dir), "alice"])
    assert code == 0
    assert "already archived" in capsys.readouterr().out

    code = cli.main(["archived", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "alice" in out
    assert "1 message(s)" in out
    assert "carol" not in out

    code = cli.main(["unarchive", "--data-dir", str(data_dir), "alice"])
    assert code == 0
    assert "Restored conversation with alice" in capsys.readouterr().out

    # Idempotent the other way too.
    code = cli.main(["unarchive", "--data-dir", str(data_dir), "alice"])
    assert code == 0
    assert "was not archived" in capsys.readouterr().out

    code = cli.main(["archived", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No archived conversations" in capsys.readouterr().out


def test_archive_json_roundtrip(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)

    code = cli.main(["archive", "--data-dir", str(data_dir), "--json", "alice"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "destination": senders["alice"],
        "name": "alice",
        "archived": True,
        "changed": True,
    }
    code = cli.main(["archive", "--data-dir", str(data_dir), "--json", "alice"])
    assert code == 0
    assert json.loads(capsys.readouterr().out)["changed"] is False

    code = cli.main(["archived", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["archived"][0]["name"] == "alice"
    assert payload["archived"][0]["messages"] == 1


def test_messages_hides_archived_and_reports_unread(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)
    cli.main(["archive", "--data-dir", str(data_dir), "alice"])
    capsys.readouterr()

    # Default listing hides alice's thread but reports her unread mail.
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "first hello" not in out
    assert "hi from carol" in out
    assert "1 unread in archived conversations" in out

    # JSON carries the hidden unread count (alice is still unread: the
    # default listing never marked her hidden mail read).
    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["archived_unread"] == 1

    # --archived shows only the archived thread — and reading it there
    # marks it read, so the hidden count drops to zero afterwards.
    code = cli.main(["messages", "--data-dir", str(data_dir), "--archived"])
    assert code == 0
    out = capsys.readouterr().out
    assert "first hello" in out
    assert "hi from carol" not in out
    assert "archived conversations" not in out

    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--archived", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["archived_unread"] == 0


def test_messages_peer_names_archived_conversation(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)
    cli.main(["archive", "--data-dir", str(data_dir), "alice"])
    capsys.readouterr()

    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--peer", "alice"]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "is archived" in out
    assert "tau unarchive alice" in out
    assert "first hello" not in out

    # The same lookup with --archived shows the thread.
    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--archived", "--peer", "alice"]
    )
    assert code == 0
    assert "first hello" in capsys.readouterr().out


def test_messages_search_still_finds_archived(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)
    cli.main(["archive", "--data-dir", str(data_dir), "alice"])
    capsys.readouterr()

    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--search", "first hello"]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "first hello" in out
    assert "archived conversations" not in out


def test_archived_messages_are_not_marked_read(tmp_path, capsys):
    # Hiding a thread must not clear its unread state: the footer
    # keeps reporting it on every listing until the owner looks.
    data_dir, senders = _seed_two_peers(tmp_path)
    cli.main(["archive", "--data-dir", str(data_dir), "alice"])
    capsys.readouterr()

    for _ in range(2):
        code = cli.main(["messages", "--data-dir", str(data_dir)])
        assert code == 0
        assert "1 unread in archived conversations" in capsys.readouterr().out

    code = cli.main(["messages", "--data-dir", str(data_dir), "--count"])
    assert code == 0
    out = capsys.readouterr().out
    assert "alice" in out
    assert "1 unread" in out


def test_archive_unknown_peer_errors(tmp_path, capsys):
    data_dir, _ = _seed_two_peers(tmp_path)
    code = cli.main(["archive", "--data-dir", str(data_dir), "nobody"])
    assert code == 1
    assert "unknown peer" in capsys.readouterr().err


def _seed_group_message(data_dir, senders, sender, text):
    from tau.groups import GroupStore

    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    group = GroupStore(config.groups_path).create(
        "weekend",
        [senders["alice"], senders["carol"]],
        created_by=senders["alice"],
    )
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": sender,
            "destination": senders[sender],
            "text": text,
            "kind": "group",
            "group": group["id"],
            "group_name": "weekend",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "m-group",
        }
    )
    return group["id"]


def test_archived_sender_group_posts_stay_listed(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)
    _seed_group_message(data_dir, senders, "alice", "group hello")
    cli.main(["archive", "--data-dir", str(data_dir), "alice"])
    capsys.readouterr()

    # The group post still shows in the default listing, and only the
    # DM counts toward the archived unread footer.  (The group post
    # shares its sender's read cursor — pre-existing inbox semantics —
    # so displaying it marks the older DM read too; the footer below
    # is from the listing before that mark.)
    code = cli.main(["messages", "--data-dir", str(data_dir)])
    assert code == 0
    out = capsys.readouterr().out
    assert "group hello" in out
    assert "hi from carol" in out
    assert "1 unread in archived conversations" in out

    # --archived is DM-only: the group post is not an archived thread,
    # and the archived list does not count it.
    code = cli.main(["messages", "--data-dir", str(data_dir), "--archived"])
    assert code == 0
    out = capsys.readouterr().out
    assert "first hello" in out
    assert "group hello" not in out

    code = cli.main(["archived", "--data-dir", str(data_dir)])
    assert code == 0
    assert "1 message(s)" in capsys.readouterr().out

    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--archived", "--group", "weekend"]
    )
    assert code == 0
    assert "group hello" not in capsys.readouterr().out


def test_archived_search_excludes_unarchived_matches(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "carol",
            "destination": senders["carol"],
            "text": "hello again",
            "sent_at": 12.0,
            "received_at": 22.0,
            "id": "m-carol-2",
        }
    )
    cli.main(["archive", "--data-dir", str(data_dir), "alice"])
    capsys.readouterr()

    # A plain search is an explicit lookup: it still finds archived mail.
    code = cli.main(["messages", "--data-dir", str(data_dir), "--search", "hello"])
    assert code == 0
    out = capsys.readouterr().out
    assert "first hello" in out
    assert "hello again" in out

    # --archived narrows the same search to archived conversations.
    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--archived", "--search", "hello"]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "first hello" in out
    assert "hello again" not in out


def test_archive_takes_the_lock(tmp_path, capsys):
    data_dir, _ = _seed_two_peers(tmp_path)
    code = cli.main(["archive", "--data-dir", str(data_dir), "alice"])
    assert code == 0
    capsys.readouterr()
    assert (data_dir / ".archived.json.lock").exists()


def test_group_view_does_not_clear_hidden_dm_unread(tmp_path, capsys):
    data_dir, senders = _seed_two_peers(tmp_path)
    _seed_group_message(data_dir, senders, "alice", "group hello")
    cli.main(["archive", "--data-dir", str(data_dir), "alice"])
    capsys.readouterr()

    # Viewing the group shows the group post, but alice's hidden DM
    # must stay unread: the shared per-sender cursor stops just
    # before it.
    code = cli.main(["messages", "--data-dir", str(data_dir), "--group", "weekend"])
    assert code == 0
    assert "group hello" in capsys.readouterr().out

    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["archived_unread"] == 1

    # Once the DM is read via --archived, the next group listing
    # advances past the group post too.
    code = cli.main(["messages", "--data-dir", str(data_dir), "--archived"])
    assert code == 0
    assert "first hello" in capsys.readouterr().out

    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--group", "weekend", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    # The listing annotates read state before its own mark-read, so
    # the post still shows unread here; the next listing sees it read.
    assert payload["messages"][0]["unread"] is True

    code = cli.main(
        ["messages", "--data-dir", str(data_dir), "--group", "weekend", "--json"]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["messages"][0]["unread"] is False
