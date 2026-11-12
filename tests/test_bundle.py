from __future__ import annotations

import base64
import json
import time

import RNS
import pytest

from tau.bundle import (
    BundleError,
    apply_bundle,
    build_bundle,
    parse_bundle,
    serialize_bundle,
)
from tau.cli import main
from tau.config import TauConfig
from tau.identity import destination_hash, load_identity
from tau.mailbox import Mailbox, open_message, seal_message
from tau.peers import PeerRegistry
from tau.protocol import derive_group_id
from tau.tangents.rules import Ruleset
from tau.tangents.tan import create_post, export_entries, follow, get_post, init_db


def _identities():
    return RNS.Identity(), RNS.Identity(), RNS.Identity()


def _registry_with(recipient, name="bob"):
    registry = PeerRegistry()
    registry.upsert(
        destination_hash(recipient),
        recipient.get_public_key(),
        name=name,
    )
    return registry


def _sealed(sender, recipient, message_id, text, sent_at=1000.0):
    return seal_message(
        sender,
        sender_name="alice",
        recipient_public_key=recipient.get_public_key(),
        message_id=message_id,
        text=text,
        sent_at=sent_at,
    )


def _seed_sender(tmp_path, sender, recipient):
    """A sender mailbox with one held envelope and one queued outbox entry."""
    mailbox = Mailbox(tmp_path / "mailbox.json")
    blob = _sealed(sender, recipient, "held-1", "held hello")
    mailbox.hold(
        to=destination_hash(recipient),
        blob=blob,
        message_id="held-1",
        expires_at=1000.0 + 7 * 24 * 3600,
        now=1000.0,
    )
    mailbox.enqueue(
        to=destination_hash(recipient),
        text="queued hello",
        to_name="bob",
        message_id="queued-1",
        now=1000.0,
    )
    return mailbox


def _build(tmp_path, sender, recipient, **kwargs):
    mailbox = _seed_sender(tmp_path, sender, recipient)
    registry = _registry_with(recipient)
    kwargs.setdefault("now", 1000.0)
    return build_bundle(
        identity=sender,
        sender_name="alice",
        mailbox=mailbox,
        registry=registry,
        **kwargs,
    )


# -- round trip ------------------------------------------------------------


def test_bundle_roundtrip_held_and_outbox(tmp_path):
    sender, recipient, _ = _identities()
    document, summary = _build(tmp_path, sender, recipient)
    assert summary["mail"] == 2
    assert summary["skipped"] == []
    assert summary["tan"] == 0

    parsed = parse_bundle(serialize_bundle(document))
    receiver_box = Mailbox(tmp_path / "receiver.json")
    result = apply_bundle(mailbox=receiver_box, tan_conn=None, document=parsed, now=1000.0)
    assert result == {
        "imported": 2,
        "present": 0,
        "skipped": 0,
        "tan": None,
        "errors": [],
    }
    held = receiver_box.held_list()
    assert {item.id for item in held} == {"held-1", "queued-1"}

    # The importer is only a relay: the sealed envelopes still open as
    # the intended recipient, with the sender's signature intact.
    texts = {
        open_message(recipient, item.blob)["text"] for item in held
    }
    assert texts == {"held hello", "queued hello"}

    # A second import of the same bundle dedupes instead of duplicating.
    again = apply_bundle(
        mailbox=receiver_box, tan_conn=None, document=parsed, now=1000.0
    )
    assert again["imported"] == 0
    assert again["present"] == 2
    assert again["skipped"] == 0


def test_bundle_tan_roundtrip(tmp_path):
    sender, _, _ = _identities()
    sender_conn = init_db(tmp_path / "sender.sqlite3")
    try:
        post = create_post(sender_conn, sender, "hello sneakernet", name="alice", now=1000)
    finally:
        sender_conn.close()
    mailbox = Mailbox(tmp_path / "mailbox.json")
    tan_conn = init_db(tmp_path / "sender.sqlite3")
    try:
        document, summary = build_bundle(
            identity=sender,
            sender_name="alice",
            mailbox=mailbox,
            registry=PeerRegistry(),
            tan_conn=tan_conn,
            include_tan=True,
            now=1000.0,
        )
    finally:
        tan_conn.close()
    assert summary["tan"] == 1

    parsed = parse_bundle(serialize_bundle(document))
    receiver_box = Mailbox(tmp_path / "receiver.json")
    receiver_conn = init_db(tmp_path / "receiver.sqlite3")
    try:
        result = apply_bundle(
            mailbox=receiver_box, tan_conn=receiver_conn, document=parsed, now=1000.0
        )
    finally:
        receiver_conn.close()
    assert result["tan"]["imported"] == 1
    assert result["tan"]["skipped"] == 0
    check_conn = init_db(tmp_path / "receiver.sqlite3")
    try:
        assert get_post(check_conn, post["hash"]) is not None
    finally:
        check_conn.close()


def test_bundle_without_tan_flag_carries_no_posts(tmp_path):
    sender, _, _ = _identities()
    sender_conn = init_db(tmp_path / "sender.sqlite3")
    try:
        create_post(sender_conn, sender, "stays home", name="alice", now=1000)
    finally:
        sender_conn.close()
    mailbox = Mailbox(tmp_path / "mailbox.json")
    tan_conn = init_db(tmp_path / "sender.sqlite3")
    try:
        document, summary = build_bundle(
            identity=sender,
            sender_name="alice",
            mailbox=mailbox,
            registry=PeerRegistry(),
            tan_conn=tan_conn,
            include_tan=False,
            now=1000.0,
        )
    finally:
        tan_conn.close()
    assert summary["tan"] == 0
    assert document["tan"] is None


def test_bundle_import_applies_tan_admission_rules(tmp_path):
    alice, bob, _ = _identities()
    alice_author = destination_hash(alice).hex()
    bob_author = destination_hash(bob).hex()
    sender_conn = init_db(tmp_path / "sender.sqlite3")
    try:
        alice_post = create_post(
            sender_conn, alice, "admitted", name="alice", now=1000
        )
        bob_post = create_post(
            sender_conn, bob, "blocked author", name="bob", now=1001
        )
    finally:
        sender_conn.close()
    mailbox = Mailbox(tmp_path / "mailbox.json")
    tan_conn = init_db(tmp_path / "sender.sqlite3")
    try:
        document, _ = build_bundle(
            identity=alice,
            sender_name="alice",
            mailbox=mailbox,
            registry=PeerRegistry(),
            tan_conn=tan_conn,
            include_tan=True,
            now=1000.0,
        )
    finally:
        tan_conn.close()

    parsed = parse_bundle(serialize_bundle(document))
    receiver_box = Mailbox(tmp_path / "receiver.json")
    receiver_conn = init_db(tmp_path / "receiver.sqlite3")
    try:
        # A refused author is reported as skipped and never stored.
        deny_bob = Ruleset(block_authors=frozenset({bob_author}))
        result = apply_bundle(
            mailbox=receiver_box,
            tan_conn=receiver_conn,
            document=parsed,
            admit=lambda author: deny_bob.decide(
                author, own_author=alice_author
            ),
            now=1000.0,
        )
        assert result["tan"]["imported"] == 1
        assert result["tan"]["skipped"] == 1
        assert "denied by this node's Tan ruleset" in result["tan"]["errors"][0]["error"]
        assert get_post(receiver_conn, alice_post["hash"]) is not None
        assert get_post(receiver_conn, bob_post["hash"]) is None

        # Under a default-deny ruleset the local author is still admitted.
        deny_all = Ruleset(default_allow=False)
        receiver_conn2 = init_db(tmp_path / "receiver2.sqlite3")
        try:
            result = apply_bundle(
                mailbox=Mailbox(tmp_path / "receiver2.json"),
                tan_conn=receiver_conn2,
                document=parsed,
                admit=lambda author: deny_all.decide(
                    author, own_author=alice_author
                ),
                now=1000.0,
            )
        finally:
            receiver_conn2.close()
        assert result["tan"]["imported"] == 1
        assert result["tan"]["skipped"] == 1
    finally:
        receiver_conn.close()


def test_bundle_import_admit_uses_ruleset_and_follow_list(tmp_path):
    """The CLI's admission check: followed authors pass a deny default."""
    from tau.cli import _bundle_tan_admit

    alice, bob, carol = _identities()
    alice_author = destination_hash(alice).hex()
    bob_author = destination_hash(bob).hex()
    carol_author = destination_hash(carol).hex()

    node_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=node_dir)
    config.tan_rules_path.parent.mkdir(parents=True, exist_ok=True)
    config.tan_rules_path.write_text(
        '{"default": "deny", "allow_followed": true}', encoding="utf-8"
    )
    receiver_conn = init_db(tmp_path / "receiver.sqlite3")
    try:
        follow(receiver_conn, bob_author)
        admit = _bundle_tan_admit(config, alice, receiver_conn)
    finally:
        receiver_conn.close()
    assert admit(alice_author) is True  # own author always admitted
    assert admit(bob_author) is True  # followed author admitted
    assert admit(carol_author) is False  # stranger denied


def test_bundle_export_caps_tan_posts(tmp_path):
    sender, _, _ = _identities()
    sender_conn = init_db(tmp_path / "sender.sqlite3")
    try:
        oldest = create_post(
            sender_conn, sender, "oldest post", name="alice", now=1000
        )
        middle = create_post(
            sender_conn, sender, "middle post", name="alice", now=1001
        )
        newest = create_post(
            sender_conn, sender, "newest post", name="alice", now=1002
        )
        # The export limit keeps the most recent entries; the oldest
        # post is left out (chain order within the kept set is kept).
        limited = export_entries(sender_conn, all_authors=True, limit=2)
    finally:
        sender_conn.close()
    assert {e["hash"] for e in limited["entries"]} == {
        middle["hash"],
        newest["hash"],
    }
    assert oldest["hash"] not in {e["hash"] for e in limited["entries"]}

    mailbox = Mailbox(tmp_path / "mailbox.json")
    tan_conn = init_db(tmp_path / "sender.sqlite3")
    try:
        document, summary = build_bundle(
            identity=sender,
            sender_name="alice",
            mailbox=mailbox,
            registry=PeerRegistry(),
            tan_conn=tan_conn,
            include_tan=True,
            now=1000.0,
        )
    finally:
        tan_conn.close()
    # Three posts is far under the 256-post bundle cap: all travel.
    assert summary["tan"] == 3
    assert len(document["tan"]["entries"]) == 3


# -- skipping --------------------------------------------------------------


def test_bundle_skips_unknown_recipient_and_expired(tmp_path):
    sender, recipient, stranger = _identities()
    mailbox = Mailbox(tmp_path / "mailbox.json")
    mailbox.enqueue(
        to=destination_hash(stranger),
        text="no key for this peer",
        message_id="unknown-1",
        now=1000.0,
    )
    blob = _sealed(sender, recipient, "old-1", "already gone")
    mailbox.hold(
        to=destination_hash(recipient),
        blob=blob,
        message_id="old-1",
        expires_at=500.0,
        now=1000.0,
    )
    registry = _registry_with(recipient)
    document, summary = build_bundle(
        identity=sender,
        sender_name="alice",
        mailbox=mailbox,
        registry=registry,
        now=1000.0,
    )
    assert summary["mail"] == 0
    assert {item["id"] for item in summary["skipped"]} == {"unknown-1", "old-1"}
    assert {item["reason"] for item in summary["skipped"]} == {
        "unknown recipient key",
        "expired",
    }


def test_bundle_export_keeps_group_expiry_and_context(tmp_path):
    sender, recipient, third = _identities()
    sender_hash = destination_hash(sender).hex()
    recipient_hash = destination_hash(recipient).hex()
    third_hash = destination_hash(third).hex()
    members = [sender_hash, recipient_hash, third_hash]
    group_id = derive_group_id(sender_hash, "weekend", members)
    mailbox = Mailbox(tmp_path / "mailbox.json")
    mailbox.enqueue(
        to=destination_hash(recipient),
        text="group hello",
        to_name="bob",
        message_id="group-1",
        expires_at=2000.0,
        group={
            "id": group_id,
            "name": "weekend",
            "creator": sender_hash,
            "members": members,
        },
        now=1000.0,
    )
    registry = _registry_with(recipient)
    document, summary = build_bundle(
        identity=sender,
        sender_name="alice",
        mailbox=mailbox,
        registry=registry,
        now=1000.0,
    )
    assert summary["mail"] == 1
    blob = base64.b64decode(document["mail"][0]["blob"], validate=True)
    opened = open_message(recipient, blob)
    # The bundled copy is a group message with the same deadline, so
    # the recipient's relayed path stores it as one and purges it.
    assert opened["group"]["id"] == group_id
    assert opened["group"]["members"] == members
    assert opened["expires_at"] == 2000.0


def test_bundle_apply_skips_malformed_entries(tmp_path):
    sender, recipient, _ = _identities()
    document, _ = _build(tmp_path, sender, recipient)
    document["mail"].append(
        {"id": "bogus", "to": "zz", "blob": "!!!", "expires_at": 9999999.0}
    )
    document["mail"].append(
        {"id": "expired", "to": destination_hash(recipient).hex(),
         "blob": "", "expires_at": 1.0}
    )
    receiver_box = Mailbox(tmp_path / "receiver.json")
    result = apply_bundle(
        mailbox=receiver_box, tan_conn=None, document=document, now=1000.0
    )
    assert result["imported"] == 2
    assert result["skipped"] == 2
    assert len(result["errors"]) == 2


def test_bundle_apply_validates_mail_entries_like_the_network_path(tmp_path):
    sender, recipient, _ = _identities()
    document, _ = _build(tmp_path, sender, recipient)
    good_blob = document["mail"][0]["blob"]
    to_hex = destination_hash(recipient).hex()
    document["mail"].extend(
        [
            # Over the 64-character id limit the fetch protocol enforces.
            {"id": "x" * 65, "to": to_hex, "blob": good_blob,
             "expires_at": 9999999.0},
            # An empty envelope the recipient could never open.
            {"id": "empty-blob", "to": to_hex, "blob": "",
             "expires_at": 9999999.0},
            # An expiry too large for a float, and a non-finite one.
            {"id": "huge-expiry", "to": to_hex, "blob": good_blob,
             "expires_at": 10**400},
            {"id": "inf-expiry", "to": to_hex, "blob": good_blob,
             "expires_at": float("inf")},
            {"id": "bool-expiry", "to": to_hex, "blob": good_blob,
             "expires_at": True},
        ]
    )
    receiver_box = Mailbox(tmp_path / "receiver.json")
    result = apply_bundle(
        mailbox=receiver_box, tan_conn=None, document=document, now=1000.0
    )
    assert result["imported"] == 2
    assert result["skipped"] == 5
    assert len(result["errors"]) == 5


def test_bundle_apply_without_tan_store_skips_posts(tmp_path):
    sender, _, _ = _identities()
    sender_conn = init_db(tmp_path / "sender.sqlite3")
    try:
        create_post(sender_conn, sender, "needs a store", name="alice", now=1000)
    finally:
        sender_conn.close()
    mailbox = Mailbox(tmp_path / "mailbox.json")
    tan_conn = init_db(tmp_path / "sender.sqlite3")
    try:
        document, _ = build_bundle(
            identity=sender,
            sender_name="alice",
            mailbox=mailbox,
            registry=PeerRegistry(),
            tan_conn=tan_conn,
            include_tan=True,
            now=1000.0,
        )
    finally:
        tan_conn.close()
    receiver_box = Mailbox(tmp_path / "receiver.json")
    result = apply_bundle(
        mailbox=receiver_box, tan_conn=None, document=document, now=1000.0
    )
    assert result["skipped"] == 1
    assert result["tan"] is None
    assert "no Tan store" in result["errors"][0]["error"]


# -- verification and size caps --------------------------------------------


def test_bundle_tamper_rejected(tmp_path):
    sender, recipient, _ = _identities()
    document, _ = _build(tmp_path, sender, recipient)
    raw = bytearray(serialize_bundle(document))
    # Flip a character inside the first sealed blob: the signature no
    # longer matches the payload.
    blob_at = raw.find(b"blob")
    assert blob_at != -1
    raw[blob_at + 10] = ord("A") if raw[blob_at + 10] != ord("A") else ord("B")
    with pytest.raises(BundleError, match="signature does not verify"):
        parse_bundle(bytes(raw))

    # A manifest edit is caught the same way.
    edited = json.loads(serialize_bundle(document))
    edited["manifest"]["mail"] = 99
    with pytest.raises(BundleError, match="signature does not verify"):
        parse_bundle(serialize_bundle(edited))


def test_bundle_wrong_format_or_version_rejected(tmp_path):
    sender, recipient, _ = _identities()
    document, _ = _build(tmp_path, sender, recipient)
    edited = json.loads(serialize_bundle(document))
    edited["format"] = "not-a-bundle"
    with pytest.raises(BundleError, match="unsupported bundle format"):
        parse_bundle(serialize_bundle(edited))
    edited = json.loads(serialize_bundle(document))
    edited["version"] = 99
    with pytest.raises(BundleError, match="unsupported bundle version"):
        parse_bundle(serialize_bundle(edited))


def test_bundle_size_caps(tmp_path):
    sender, recipient, _ = _identities()
    with pytest.raises(BundleError, match="over the .* byte cap"):
        _build(tmp_path, sender, recipient, max_bytes=10)
    document, _ = _build(tmp_path, sender, recipient)
    with pytest.raises(BundleError, match="over the .* byte cap"):
        parse_bundle(serialize_bundle(document), max_bytes=10)
    with pytest.raises(BundleError, match="not valid JSON"):
        parse_bundle(b"{not json")


# -- CLI end to end ----------------------------------------------------------


def test_cli_export_import_roundtrip(tmp_path, capsys):
    sender_dir = tmp_path / "sender"
    receiver_dir = tmp_path / "receiver"
    assert main(["init", "--data-dir", str(sender_dir), "--json"]) == 0
    assert main(["init", "--data-dir", str(receiver_dir), "--json"]) == 0

    sender_config = TauConfig.resolve(data_dir=sender_dir)
    receiver_config = TauConfig.resolve(data_dir=receiver_dir)
    receiver_identity = load_identity(receiver_config.identity_path)
    assert receiver_identity is not None
    sender_box = Mailbox(sender_config.mailbox_path)
    sender_registry = PeerRegistry(sender_config.peers_path)
    sender_registry.upsert(
        destination_hash(receiver_identity),
        receiver_identity.get_public_key(),
        name="receiver",
    )
    sender_registry.save()
    sender_box.enqueue(
        to=destination_hash(receiver_identity),
        text="over the sneakernet",
        to_name="receiver",
        now=time.time(),
    )

    bundle_file = tmp_path / "bundle.json"
    assert (
        main(
            ["bundle", "export", str(bundle_file), "--data-dir", str(sender_dir)]
        )
        == 0
    )
    assert bundle_file.exists()
    capsys.readouterr()
    assert (
        main(
            ["bundle", "import", str(bundle_file), "--data-dir", str(receiver_dir)]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "Imported 1 sealed message(s)" in out

    receiver_box = Mailbox(receiver_config.mailbox_path)
    held = receiver_box.held_list()
    assert len(held) == 1
    opened = open_message(receiver_identity, held[0].blob)
    assert opened["text"] == "over the sneakernet"


def test_cli_export_missing_identity_errors(tmp_path):
    data_dir = tmp_path / "empty"
    bundle_file = tmp_path / "bundle.json"
    code = main(["bundle", "export", str(bundle_file), "--data-dir", str(data_dir)])
    assert code == 1
    assert not bundle_file.exists()


def test_cli_export_preserves_target_directory_permissions(tmp_path):
    import os
    import stat

    sender_dir = tmp_path / "sender"
    assert main(["init", "--data-dir", str(sender_dir), "--json"]) == 0
    shared = tmp_path / "shared"
    shared.mkdir()
    os.chmod(shared, 0o755)
    bundle_file = shared / "bundle.json"
    assert (
        main(
            ["bundle", "export", str(bundle_file), "--data-dir", str(sender_dir)]
        )
        == 0
    )
    assert bundle_file.exists()
    assert stat.S_IMODE(os.stat(shared).st_mode) == 0o755
