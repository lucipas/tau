"""Tests for linked-device pairing (slices 1–3 of #469, #713/#736/#745)."""

from __future__ import annotations

import base64
import io
import json
import os
import stat
import time
from pathlib import Path

import pytest
import RNS

from tau import cli
from tau.config import TauConfig
from tau.devices import (
    DEFAULT_PAIRING_TTL_SECONDS,
    MAX_PAIRING_BUNDLE_BYTES,
    MAX_PAIRING_HISTORY_BYTES,
    MAX_PAIRING_HISTORY_MESSAGES,
    MAX_PAIRING_TTL_SECONDS,
    DeviceRegistry,
    DevicesError,
    build_pairing_bundle,
    snapshot_pairing_history,
    verify_pairing_bundle,
)
from tau.identity import (
    IdentityError,
    destination_hash,
    load_identity,
    load_or_create_identity,
)
from tau.inbox import INBOX_EXPORT_FORMAT, INBOX_EXPORT_VERSION, Inbox
from tau.mailbox import Mailbox
from tau.node import InstanceLock
from tau.peers import PUBLIC_KEY_BYTES, PeerRegistry
from tau.protocol import encode


def _identity():
    return RNS.Identity()


def _tampered(bundle, field, value):
    tampered = dict(bundle)
    tampered[field] = value
    return tampered


def test_pairing_bundle_roundtrip():
    identity = _identity()
    bundle = build_pairing_bundle(identity, ttl_seconds=600, now=1000.0)
    assert bundle["v"] == 2
    assert bundle["type"] == "tau-pairing"
    assert bundle["primary"] == destination_hash(identity).hex()
    assert bundle["created_at"] == 1000.0
    assert bundle["expires_at"] == 1600.0
    recovered, cleaned = verify_pairing_bundle(bundle, now=1200.0)
    assert destination_hash(recovered).hex() == destination_hash(identity).hex()
    assert recovered.get_public_key() == identity.get_public_key()
    assert cleaned["token"] == bundle["token"]


def test_pairing_bundle_tampered_identity_rejected():
    identity = _identity()
    bundle = build_pairing_bundle(identity, now=1000.0)
    # Flip a character inside the base64 identity: the signature covers
    # the identity bytes, so the bundle must not verify.
    identity_b64 = bundle["identity"]
    flipped = ("A" if identity_b64[10] != "A" else "B") + identity_b64[11:]
    # The flipped bytes decode to a different identity, so the primary
    # check fires before the signature check — either way the bundle
    # must be rejected.
    with pytest.raises(DevicesError):
        verify_pairing_bundle(
            _tampered(bundle, "identity", identity_b64[:10] + flipped),
            now=1100.0,
        )


def test_pairing_bundle_tampered_token_rejected():
    identity = _identity()
    bundle = build_pairing_bundle(identity, now=1000.0)
    other_token = "0" * 32 if bundle["token"] != "0" * 32 else "1" * 32
    with pytest.raises(DevicesError, match="signature does not verify"):
        verify_pairing_bundle(
            _tampered(bundle, "token", other_token), now=1100.0
        )


def test_pairing_bundle_tampered_primary_rejected():
    identity = _identity()
    bundle = build_pairing_bundle(identity, now=1000.0)
    other = RNS.Identity()
    with pytest.raises(DevicesError, match="does not match"):
        verify_pairing_bundle(
            _tampered(bundle, "primary", destination_hash(other).hex()),
            now=1100.0,
        )


def test_pairing_bundle_expired_rejected():
    identity = _identity()
    bundle = build_pairing_bundle(identity, ttl_seconds=60, now=1000.0)
    with pytest.raises(DevicesError, match="expired"):
        verify_pairing_bundle(bundle, now=2000.0)


def test_pairing_bundle_bad_ttl_rejected():
    identity = _identity()
    for ttl in (0, -5, MAX_PAIRING_TTL_SECONDS + 1):
        with pytest.raises(DevicesError, match="TTL"):
            build_pairing_bundle(identity, ttl_seconds=ttl)


def test_pairing_bundle_wrong_version_or_type_rejected():
    identity = _identity()
    bundle = build_pairing_bundle(identity, now=1000.0)
    with pytest.raises(DevicesError, match="version"):
        verify_pairing_bundle(_tampered(bundle, "v", 3), now=1100.0)
    with pytest.raises(DevicesError, match="type"):
        verify_pairing_bundle(_tampered(bundle, "type", "other"), now=1100.0)


def test_pairing_bundle_malformed_rejected():
    identity = _identity()
    bundle = build_pairing_bundle(identity, now=1000.0)
    with pytest.raises(DevicesError):
        verify_pairing_bundle(None, now=1100.0)
    with pytest.raises(DevicesError):
        verify_pairing_bundle(_tampered(bundle, "identity", "!!!"), now=1100.0)
    with pytest.raises(DevicesError):
        verify_pairing_bundle(_tampered(bundle, "sig", "!!!"), now=1100.0)
    unsigned = {key: value for key, value in bundle.items() if key != "sig"}
    with pytest.raises(DevicesError, match="signature"):
        verify_pairing_bundle(unsigned, now=1100.0)


def test_pairing_bundle_oversize_identity_rejected():
    # A hostile bundle must be refused before its identity field is
    # decoded or written to a temporary file (see #714).
    identity = _identity()
    bundle = build_pairing_bundle(identity, now=1000.0)
    with pytest.raises(DevicesError, match="too large"):
        verify_pairing_bundle(
            _tampered(bundle, "identity", "A" * 70000), now=1100.0
        )


def _message(text, *, msg_id, sent_at=1000.0, **extra):
    record = {
        "from": "peer",
        "destination": "a" * 32,
        "text": text,
        "sent_at": sent_at,
        "received_at": sent_at + 1,
        "id": msg_id,
        "direction": "in",
    }
    record.update(extra)
    return record


def _history_doc(messages):
    return {
        "format": INBOX_EXPORT_FORMAT,
        "version": INBOX_EXPORT_VERSION,
        "exported_at": 1000.0,
        "messages": messages,
    }


def _resign(identity, bundle):
    """Re-sign a mutated bundle, so verify judges the content, not the signature."""
    unsigned = {key: value for key, value in bundle.items() if key != "sig"}
    signature = identity.sign(encode(unsigned))
    assert signature is not None
    return {**unsigned, "sig": base64.b64encode(signature).decode("ascii")}


def test_pairing_bundle_roundtrip_with_history():
    identity = _identity()
    history = _history_doc(
        [_message("hello", msg_id="m1"), _message("again", msg_id="m2")]
    )
    bundle = build_pairing_bundle(
        identity, ttl_seconds=600, now=1000.0, history=history
    )
    assert bundle["v"] == 2
    _, cleaned = verify_pairing_bundle(bundle, now=1200.0)
    assert cleaned["history"]["messages"] == history["messages"]
    assert cleaned["history"]["format"] == INBOX_EXPORT_FORMAT


def test_pairing_bundle_tampered_history_rejected():
    identity = _identity()
    history = _history_doc([_message("hello", msg_id="m1")])
    bundle = build_pairing_bundle(identity, now=1000.0, history=history)
    tampered = json.loads(json.dumps(bundle))
    tampered["history"]["messages"][0]["text"] = "forged"
    with pytest.raises(DevicesError, match="signature does not verify"):
        verify_pairing_bundle(tampered, now=1100.0)


def test_pairing_bundle_v1_still_verifies():
    # A bundle minted before history existed (version 1, no history
    # key) redeems unchanged; its signature covers the version-1
    # payload only.
    identity = _identity()
    minted = build_pairing_bundle(identity, ttl_seconds=600, now=1000.0)
    unsigned_v1 = {
        "v": 1,
        "type": "tau-pairing",
        "token": minted["token"],
        "primary": minted["primary"],
        "created_at": 1000.0,
        "expires_at": 1600.0,
        "identity": minted["identity"],
    }
    signature = identity.sign(encode(unsigned_v1))
    assert signature is not None
    bundle_v1 = {
        **unsigned_v1,
        "sig": base64.b64encode(signature).decode("ascii"),
    }
    recovered, cleaned = verify_pairing_bundle(bundle_v1, now=1200.0)
    assert cleaned["v"] == 1
    assert cleaned["history"] is None
    assert destination_hash(recovered).hex() == minted["primary"]


def test_pairing_bundle_v1_with_history_rejected():
    identity = _identity()
    minted = build_pairing_bundle(identity, ttl_seconds=600, now=1000.0)
    unsigned_v1 = {
        "v": 1,
        "type": "tau-pairing",
        "token": minted["token"],
        "primary": minted["primary"],
        "created_at": 1000.0,
        "expires_at": 1600.0,
        "identity": minted["identity"],
        "history": _history_doc([_message("hello", msg_id="m1")]),
    }
    signature = identity.sign(encode(unsigned_v1))
    assert signature is not None
    bundle_v1 = {
        **unsigned_v1,
        "sig": base64.b64encode(signature).decode("ascii"),
    }
    with pytest.raises(DevicesError, match="version 1 must not carry history"):
        verify_pairing_bundle(bundle_v1, now=1200.0)


def test_pairing_bundle_history_over_caps_rejected():
    identity = _identity()
    too_many = _history_doc(
        [
            _message(f"m{i}", msg_id=f"m{i}", sent_at=1000.0 + i)
            for i in range(MAX_PAIRING_HISTORY_MESSAGES + 1)
        ]
    )
    with pytest.raises(DevicesError, match="too many messages"):
        build_pairing_bundle(identity, now=1000.0, history=too_many)
    too_big = _history_doc([_message("x" * 300 * 1024, msg_id="big")])
    with pytest.raises(DevicesError, match="too large"):
        build_pairing_bundle(identity, now=1000.0, history=too_big)
    # The same caps hold at verify time for a bundle signed by hand:
    # a valid signature over an over-cap snapshot must not redeem.
    bundle = build_pairing_bundle(
        identity,
        now=1000.0,
        history=_history_doc([_message("hello", msg_id="m1")]),
    )
    oversize = _resign(identity, _tampered(bundle, "history", too_many))
    with pytest.raises(DevicesError, match="too many messages"):
        verify_pairing_bundle(oversize, now=1100.0)


def test_snapshot_pairing_history_drops_files_and_caps():
    file_record = {
        "kind": "file",
        "from": "peer",
        "destination": "a" * 32,
        "name": "photo.jpg",
        "size": 10,
        "sha256": "e" * 64,
        "path": "/tmp/photo.jpg",
        "sent_at": 1001.0,
        "received_at": 1002.0,
        "id": "f1",
    }
    messages = [
        _message("old", msg_id="m1", sent_at=1000.0),
        file_record,
        _message("new", msg_id="m2", sent_at=1002.0),
    ]
    snapshot = snapshot_pairing_history(_history_doc(messages))
    assert snapshot is not None
    assert [entry["id"] for entry in snapshot["messages"]] == ["m1", "m2"]
    # The message cap keeps the newest; the file record never counts.
    snapshot = snapshot_pairing_history(_history_doc(messages), max_messages=1)
    assert [entry["id"] for entry in snapshot["messages"]] == ["m2"]
    # The byte cap drops the oldest until the snapshot fits.  The cap
    # measures the whole document, envelope included, so one small
    # record rides in ~240 bytes: a 300-byte budget keeps only it.
    big = _message("x" * 1000, msg_id="big", sent_at=1000.0)
    small = _message("y", msg_id="small", sent_at=1001.0)
    snapshot = snapshot_pairing_history(
        _history_doc([big, small]), max_bytes=300
    )
    assert [entry["id"] for entry in snapshot["messages"]] == ["small"]


def test_snapshot_pairing_history_empty_is_none():
    assert snapshot_pairing_history(None) is None
    assert snapshot_pairing_history(_history_doc([])) is None
    file_only = _history_doc(
        [
            {
                "kind": "file",
                "from": "peer",
                "destination": "a" * 32,
                "name": "photo.jpg",
                "size": 10,
                "sha256": "e" * 64,
                "path": "/tmp/photo.jpg",
                "sent_at": 1001.0,
                "received_at": 1002.0,
                "id": "f1",
            }
        ]
    )
    assert snapshot_pairing_history(file_only) is None


def test_registry_records_issued_and_redeemed(tmp_path):
    path = tmp_path / "devices.json"
    registry = DeviceRegistry(path)
    assert registry.issued_offers(now=1000.0) == []
    assert registry.paired_from is None
    registry.record_issued("a" * 32, 1000.0, 4600.0)
    offers = registry.issued_offers(now=1100.0)
    assert len(offers) == 1 and offers[0]["token"] == "a" * 32
    assert not registry.is_redeemed("a" * 32)
    registry.record_redeemed("a" * 32, "b" * 32, 4600.0, now=1200.0)
    assert registry.is_redeemed("a" * 32)
    # The token alone is not a pairing: the pairing is only recorded
    # once the identity is installed.
    assert registry.paired_from is None
    registry.record_paired_from("a" * 32, "b" * 32, now=1200.0)
    assert registry.paired_from == {
        "primary": "b" * 32,
        "token": "a" * 32,
        "paired_at": 1200.0,
    }
    # The record survives a reload from disk.
    reloaded = DeviceRegistry(path)
    assert reloaded.is_redeemed("a" * 32)
    assert reloaded.paired_from == registry.paired_from
    assert len(reloaded.issued_offers(now=1100.0)) == 1


def test_registry_prunes_only_expired_redeemed(tmp_path):
    # 105 distinct bundles redeemed inside one validity window: the
    # first must still be recorded — capping the list would let it
    # pass the replay check again (see #714).
    registry = DeviceRegistry(tmp_path / "devices.json")
    for index in range(105):
        registry.record_redeemed(
            f"{index:032x}", "b" * 32, 4600.0, now=1200.0
        )
    assert all(
        registry.is_redeemed(f"{index:032x}") for index in range(105)
    )
    # Once the window ends the expired tokens are pruned.
    registry.record_redeemed("c" * 32, "b" * 32, 9000.0, now=5000.0)
    assert not registry.is_redeemed("0" * 32)
    assert registry.is_redeemed("c" * 32)


def test_registry_keeps_every_unexpired_offer(tmp_path):
    # 33 offers minted while the earlier ones are still valid: the
    # oldest must still be listed (see #714).
    import time

    now = time.time()
    registry = DeviceRegistry(tmp_path / "devices.json")
    for index in range(33):
        registry.record_issued(f"{index:032x}", now, now + 3600.0)
    offers = registry.issued_offers(now=now + 60.0)
    assert len(offers) == 33
    assert offers[0]["token"] == "0" * 32


def test_registry_reset_keeps_redeemed_tokens(tmp_path):
    # An identity swap drops the old identity's offers and pairing,
    # but a redeemed token stays recorded until its own window ends:
    # the bundle is still live for up to 24 hours, and clearing the
    # list would let it be redeemed again with --force (see #716).
    # Only expired tokens are pruned.
    registry = DeviceRegistry(tmp_path / "devices.json")
    registry.record_issued("a" * 32, 1000.0, 4600.0)
    registry.record_redeemed("a" * 32, "b" * 32, 4600.0, now=1200.0)
    registry.record_redeemed("c" * 32, "b" * 32, 1300.0, now=1200.0)
    registry.record_paired_from("a" * 32, "b" * 32, now=1200.0)
    registry.reset(now=2000.0)
    assert registry.issued_offers(now=1100.0) == []
    assert registry.is_redeemed("a" * 32)
    assert not registry.is_redeemed("c" * 32)
    assert registry.paired_from is None


def test_registry_reset_keeps_offers_owned_by_new_identity(tmp_path):
    # A stranded-pairing retry completes a pending reset (#732):
    # offers minted by the identity being installed survive; offers
    # minted by the replaced identity go.  A plain reset with no
    # owner still drops every offer.
    import time

    now = time.time()
    registry = DeviceRegistry(tmp_path / "devices.json")
    registry.record_issued("a" * 32, now - 300.0, now + 3600.0, owner="1" * 32)
    registry.record_issued("b" * 32, now - 100.0, now + 3600.0, owner="2" * 32)
    registry.record_issued("c" * 32, now + 60.0, now + 3600.0, owner="2" * 32)
    registry.reset(now=now + 120.0, keep_offers_owned_by="2" * 32)
    offers = registry.issued_offers(now=now)
    assert [offer["token"] for offer in offers] == ["b" * 32, "c" * 32]
    registry.reset(now=now + 120.0)
    assert registry.issued_offers(now=now) == []


def test_registry_reset_owner_scope_ignores_clock_rollback(tmp_path):
    # The owner decides which offers survive, not wall-clock order: an
    # offer minted by the new identity after the clock moved backward
    # (so its created_at predates the failed redeem) is still kept, and
    # an offer without a recorded owner is dropped.
    import time

    now = time.time()
    registry = DeviceRegistry(tmp_path / "devices.json")
    registry.record_issued("a" * 32, now - 500.0, now + 3600.0, owner="1" * 32)
    registry.record_issued("b" * 32, now - 1000.0, now + 3600.0, owner="2" * 32)
    registry.record_issued("c" * 32, now - 2000.0, now + 3600.0)
    registry.reset(now=now + 120.0, keep_offers_owned_by="2" * 32)
    offers = registry.issued_offers(now=now)
    assert [offer["token"] for offer in offers] == ["b" * 32]


def test_registry_prunes_expired_offers(tmp_path):
    registry = DeviceRegistry(tmp_path / "devices.json")
    registry.record_issued("a" * 32, 1000.0, 1060.0)
    assert registry.issued_offers(now=2000.0) == []


def test_registry_revoke_offer_removes_live_offer(tmp_path):
    # #745: a revoked offer is gone from the list and stays gone
    # after a reload; the other outstanding offer is untouched.
    # Real wall-clock times: record_issued prunes against the real
    # clock, so fake-epoch offers would be pruned on the next write.
    now = time.time()
    registry = DeviceRegistry(tmp_path / "devices.json")
    registry.record_issued("a" * 32, now, now + 3600.0)
    registry.record_issued("b" * 32, now, now + 3600.0)
    revoked = registry.revoke_offer("a" * 8, now=now + 60.0)
    assert revoked["token"] == "a" * 32
    assert [offer["token"] for offer in registry.issued_offers(now=now + 60.0)] == [
        "b" * 32
    ]
    reloaded = DeviceRegistry(tmp_path / "devices.json")
    assert [offer["token"] for offer in reloaded.issued_offers(now=now + 60.0)] == [
        "b" * 32
    ]


def test_registry_revoke_offer_accepts_full_token(tmp_path):
    now = time.time()
    registry = DeviceRegistry(tmp_path / "devices.json")
    registry.record_issued("a" * 32, now, now + 3600.0)
    assert registry.revoke_offer("a" * 32, now=now + 60.0)["token"] == "a" * 32
    assert registry.issued_offers(now=now + 60.0) == []


def test_registry_revoke_offer_unknown_token(tmp_path):
    now = time.time()
    registry = DeviceRegistry(tmp_path / "devices.json")
    registry.record_issued("a" * 32, now, now + 3600.0)
    with pytest.raises(DevicesError, match="no outstanding pairing offer"):
        registry.revoke_offer("c" * 32, now=now + 60.0)
    # The failed revoke changed nothing.
    assert len(registry.issued_offers(now=now + 60.0)) == 1


def test_registry_revoke_offer_ambiguous_prefix(tmp_path):
    now = time.time()
    registry = DeviceRegistry(tmp_path / "devices.json")
    registry.record_issued("ab" + "0" * 30, now, now + 3600.0)
    registry.record_issued("ab" + "1" * 30, now, now + 3600.0)
    with pytest.raises(DevicesError, match="2 pairing offers match prefix"):
        registry.revoke_offer("ab", now=now + 60.0)
    assert len(registry.issued_offers(now=now + 60.0)) == 2


def test_registry_revoke_offer_names_expired_offer(tmp_path):
    now = time.time()
    registry = DeviceRegistry(tmp_path / "devices.json")
    registry.record_issued("a" * 32, now - 3600.0, now - 60.0)
    with pytest.raises(DevicesError, match="already expired"):
        registry.revoke_offer("a" * 8, now=now)


def test_registry_revoke_offer_names_redeemed_token(tmp_path):
    registry = DeviceRegistry(tmp_path / "devices.json")
    registry.record_redeemed("a" * 32, "b" * 32, 4600.0, now=1200.0)
    with pytest.raises(DevicesError, match="already redeemed"):
        registry.revoke_offer("a" * 8, now=1300.0)


def test_registry_revoke_offer_prunes_expired_offers(tmp_path):
    # The revoke transaction applies the same housekeeping as
    # record_issued: an expired offer goes with the revoked one.
    # The expired offer is recorded after the live one, so the
    # record call itself does not prune it first.
    now = time.time()
    registry = DeviceRegistry(tmp_path / "devices.json")
    registry.record_issued("b" * 32, now, now + 3600.0)
    registry.record_issued("a" * 32, now - 3600.0, now - 60.0)
    registry.revoke_offer("b" * 32, now=now + 60.0)
    reloaded = DeviceRegistry(tmp_path / "devices.json")
    assert reloaded.issued_offers(now=now + 60.0) == []


def test_registry_corrupt_file_fails_closed(tmp_path):
    path = tmp_path / "devices.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(DevicesError, match="corrupt"):
        DeviceRegistry(path)


def test_parser_devices_subcommands():
    parser = cli.build_parser()

    pair = parser.parse_args(["devices", "pair", "--to", "/tmp/bundle"])
    assert pair.func is cli.cmd_devices_pair
    assert str(pair.to) == "/tmp/bundle"
    assert pair.ttl == DEFAULT_PAIRING_TTL_SECONDS
    assert pair.force is False
    assert pair.no_history is False

    no_history = parser.parse_args(
        ["devices", "pair", "--to", "/tmp/bundle", "--no-history"]
    )
    assert no_history.no_history is True

    redeem = parser.parse_args(
        ["devices", "redeem", "--from", "/tmp/bundle", "--force"]
    )
    assert redeem.func is cli.cmd_devices_redeem
    assert str(redeem.from_path) == "/tmp/bundle"
    assert redeem.force is True

    listing = parser.parse_args(["devices", "list"])
    assert listing.func is cli.cmd_devices_list

    revoke = parser.parse_args(["devices", "revoke", "abcdef12"])
    assert revoke.func is cli.cmd_devices_revoke
    assert revoke.token == "abcdef12"

    bare = parser.parse_args(["devices"])
    assert bare.func is cli.cmd_devices_list
    assert bare.devices_command is None


def _init_node(tmp_path, dirname, name):
    data_dir = tmp_path / dirname
    assert (
        cli.main(["init", "--data-dir", str(data_dir), "--name", name, "--json"])
        == 0
    )
    return data_dir


def _destination(data_dir, capsys):
    assert cli.main(["id", "--data-dir", str(data_dir), "--json"]) == 0
    return json.loads(capsys.readouterr().out)["destination"]


def test_devices_pair_redeem_round_trip(tmp_path, capsys):
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    destination_a = _destination(node_a, capsys)

    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            [
                "devices",
                "pair",
                "--to",
                str(bundle_path),
                "--data-dir",
                str(node_a),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["destination"] == destination_a
    assert payload["path"] == str(bundle_path)
    assert stat.S_IMODE(os.stat(bundle_path).st_mode) == 0o600

    # A fresh second device redeems the bundle and takes the identity.
    node_b = tmp_path / "b"
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["destination"] == destination_a
    assert payload["paired_from"] == destination_a
    assert _destination(node_b, capsys) == destination_a

    # The same bundle cannot be redeemed twice on the same node.
    code = cli.main(
        ["devices", "redeem", "--from", str(bundle_path), "--data-dir", str(node_b)]
    )
    assert code == 1
    assert "already redeemed" in capsys.readouterr().err

    # The primary lists its issued offer; the second lists its pairing.
    assert cli.main(["devices", "list", "--data-dir", str(node_a)]) == 0
    assert "pairing offers issued: 1" in capsys.readouterr().out
    assert cli.main(["devices", "list", "--data-dir", str(node_b)]) == 0
    assert f"paired from: {destination_a}" in capsys.readouterr().out


def test_devices_revoke_cancels_offer(tmp_path, capsys):
    # #745: a minted offer can be cancelled by its 8-character
    # prefix; afterwards `devices list` shows none, and revoking
    # the same prefix again fails because nothing is outstanding.
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            [
                "devices",
                "pair",
                "--to",
                str(bundle_path),
                "--data-dir",
                str(node_a),
                "--json",
            ]
        )
        == 0
    )
    token = json.loads(capsys.readouterr().out)["token"]
    assert cli.main(["devices", "list", "--data-dir", str(node_a)]) == 0
    assert "pairing offers issued: 1" in capsys.readouterr().out

    assert (
        cli.main(
            ["devices", "revoke", token[:8], "--data-dir", str(node_a), "--json"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["revoked"]["token"] == token
    assert cli.main(["devices", "list", "--data-dir", str(node_a)]) == 0
    assert "no pairing offers issued" in capsys.readouterr().out

    code = cli.main(["devices", "revoke", token[:8], "--data-dir", str(node_a)])
    assert code == 1
    assert "no outstanding pairing offer" in capsys.readouterr().err


def test_devices_revoke_says_the_bundle_still_redeems(tmp_path, capsys):
    # #745: the human output must not oversell the revoke — a bundle
    # file already copied still redeems until it expires, so the
    # command tells the owner to delete the file too.
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            [
                "devices",
                "pair",
                "--to",
                str(bundle_path),
                "--data-dir",
                str(node_a),
                "--json",
            ]
        )
        == 0
    )
    token = json.loads(capsys.readouterr().out)["token"]
    assert cli.main(["devices", "revoke", token, "--data-dir", str(node_a)]) == 0
    out = capsys.readouterr().out
    assert f"revoked pairing offer {token[:8]}" in out
    assert "delete the bundle file too" in out


def _seed_history(node):
    """Three messages and a file record in *node*'s inbox."""
    inbox = Inbox(node / "inbox.json")
    inbox.append(
        {
            "from": "peer",
            "destination": "a" * 32,
            "text": "hello from a peer",
            "sent_at": 1000.0,
            "received_at": 1001.0,
            "id": "msg-1",
            "direction": "in",
        }
    )
    inbox.append(
        {
            "from": "alice",
            "destination": "b" * 32,
            "text": "my reply",
            "sent_at": 1002.0,
            "received_at": 1003.0,
            "id": "msg-2",
            "direction": "out",
        }
    )
    inbox.append(
        {
            "from": "peer",
            "destination": "c" * 32,
            "text": "group hello",
            "sent_at": 1004.0,
            "received_at": 1005.0,
            "id": "msg-3",
            "direction": "in",
            "kind": "group",
            "group": "d" * 32,
            "group_name": "test group",
        }
    )
    inbox.append(
        {
            "kind": "file",
            "from": "peer",
            "destination": "a" * 32,
            "name": "photo.jpg",
            "size": 10,
            "sha256": "e" * 64,
            "path": "/tmp/photo.jpg",
            "sent_at": 1006.0,
            "received_at": 1007.0,
            "id": "msg-4",
            "direction": "in",
        }
    )


def test_devices_pair_redeem_carries_history(tmp_path, capsys):
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_history(node_a)
    capsys.readouterr()

    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            [
                "devices",
                "pair",
                "--to",
                str(bundle_path),
                "--data-dir",
                str(node_a),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    # Three messages ride the bundle; the file record does not.
    assert payload["history_messages"] == 3

    node_b = tmp_path / "b"
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["history"] == {
        "imported": 3,
        "present": 0,
        "skipped": 0,
        "errors": [],
    }
    inbox_b = Inbox(node_b / "inbox.json")
    stored = inbox_b.list_messages(direction=None)
    assert [entry["text"] for entry in stored] == [
        "group hello",
        "my reply",
        "hello from a peer",
    ]
    assert all(entry.get("kind") != "file" for entry in stored)


def test_devices_redeem_retry_reimports_missing_history(tmp_path, capsys):
    # A pairing can complete with its history missing — the import
    # never fails the redeem — so a retry of the same bundle must be
    # allowed to re-import the snapshot instead of failing as a plain
    # duplicate (see #736).  Once every snapshot record is stored,
    # the next redeem is a duplicate again.
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_history(node_a)
    capsys.readouterr()

    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            [
                "devices",
                "pair",
                "--to",
                str(bundle_path),
                "--data-dir",
                str(node_a),
                "--json",
            ]
        )
        == 0
    )
    capsys.readouterr()

    node_b = tmp_path / "b"
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["history"]["imported"] == 3

    # The history is lost on the second device; the same bundle must
    # still redeem, importing only the missing snapshot.
    inbox_b = Inbox(node_b / "inbox.json")
    for msg_id in ("msg-1", "msg-2", "msg-3"):
        inbox_b.delete(msg_id)
    assert len(Inbox(node_b / "inbox.json")) == 0
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["history"] == {
        "imported": 3,
        "present": 0,
        "skipped": 0,
        "errors": [],
    }
    assert payload["verifications_cleared"] == 0

    # With the history back in place, the bundle is spent again.
    code = cli.main(
        ["devices", "redeem", "--from", str(bundle_path), "--data-dir", str(node_b)]
    )
    assert code == 1
    assert "already redeemed" in capsys.readouterr().err


def test_devices_redeem_retry_refused_when_inbox_full(tmp_path, capsys):
    # Qodo Warning on PR #739: a missing snapshot record that the
    # capacity-preserving import cannot store must not keep the bundle
    # retryable — each replay would "succeed" without importing it.
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_history(node_a)
    capsys.readouterr()

    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            [
                "devices",
                "pair",
                "--to",
                str(bundle_path),
                "--data-dir",
                str(node_a),
                "--json",
            ]
        )
        == 0
    )
    capsys.readouterr()

    node_b = tmp_path / "b"
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["history"]["imported"] == 3

    # Lose one snapshot record, then fill the inbox to its cap: the
    # retry has nothing it could store, so the bundle is spent.
    inbox_b = Inbox(node_b / "inbox.json")
    inbox_b.delete("msg-1")
    fill = {
        "format": "tau-messages-export",
        "version": 1,
        "messages": [
            {
                "id": f"fill-{i}",
                "destination": "aa" * 32,
                "text": f"fill {i}",
            }
            for i in range(499)
        ],
    }
    assert Inbox(node_b / "inbox.json").import_messages(fill)["imported"] == 499
    # 500 received (one group record + 499 fill) plus one sent record.
    assert len(Inbox(node_b / "inbox.json")) == 501

    code = cli.main(
        ["devices", "redeem", "--from", str(bundle_path), "--data-dir", str(node_b)]
    )
    assert code == 1
    assert "already redeemed" in capsys.readouterr().err

    # Freeing a received slot makes the same bundle retryable again.
    assert Inbox(node_b / "inbox.json").delete("fill-0")
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["history"]["imported"] >= 1


def test_devices_redeem_retry_refused_when_only_missing_record_retracted(
    tmp_path, capsys
):
    # Retraction variant of the capacity gap in #740: a missing
    # snapshot record its sender retracted is skipped by the import,
    # so it must not keep the bundle retryable either — each replay
    # would "succeed" without importing it (see #742).
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_history(node_a)
    capsys.readouterr()

    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            [
                "devices",
                "pair",
                "--to",
                str(bundle_path),
                "--data-dir",
                str(node_a),
                "--json",
            ]
        )
        == 0
    )
    capsys.readouterr()

    node_b = tmp_path / "b"
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["history"]["imported"] == 3

    # Lose one snapshot record to a retract: the sender retracted it
    # after the bundle was minted, so the import would skip it — the
    # bundle is spent, not retryable.
    inbox_b = Inbox(node_b / "inbox.json")
    inbox_b.delete("msg-1")
    Mailbox(node_b / "mailbox.json").record_retraction("a" * 32, "msg-1")

    code = cli.main(
        ["devices", "redeem", "--from", str(bundle_path), "--data-dir", str(node_b)]
    )
    assert code == 1
    assert "already redeemed" in capsys.readouterr().err


def test_devices_redeem_retry_refused_when_only_missing_group_record_retracted(
    tmp_path, capsys
):
    # Same gate, group leg: a missing group record covered by a live
    # group-retraction marker (sender, group, send time) must not
    # keep the bundle retryable either (see #742).
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_history(node_a)
    capsys.readouterr()

    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            [
                "devices",
                "pair",
                "--to",
                str(bundle_path),
                "--data-dir",
                str(node_a),
                "--json",
            ]
        )
        == 0
    )
    capsys.readouterr()

    node_b = tmp_path / "b"
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["history"]["imported"] == 3

    inbox_b = Inbox(node_b / "inbox.json")
    inbox_b.delete("msg-3")
    Mailbox(node_b / "mailbox.json").record_group_retraction(
        "c" * 32, "d" * 32, 1004.0
    )

    code = cli.main(
        ["devices", "redeem", "--from", str(bundle_path), "--data-dir", str(node_b)]
    )
    assert code == 1
    assert "already redeemed" in capsys.readouterr().err


def test_devices_redeem_retry_allowed_when_retraction_is_for_another_sender(
    tmp_path, capsys
):
    # A retraction marker names its sender: one recorded for another
    # peer must not close the gate for a record that peer never sent
    # (see #742).  The missing record re-imports on retry.
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_history(node_a)
    capsys.readouterr()

    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            [
                "devices",
                "pair",
                "--to",
                str(bundle_path),
                "--data-dir",
                str(node_a),
                "--json",
            ]
        )
        == 0
    )
    capsys.readouterr()

    node_b = tmp_path / "b"
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["history"]["imported"] == 3

    inbox_b = Inbox(node_b / "inbox.json")
    inbox_b.delete("msg-1")
    Mailbox(node_b / "mailbox.json").record_retraction("f" * 32, "msg-1")

    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["history"] == {
        "imported": 1,
        "present": 2,
        "skipped": 0,
        "errors": [],
    }


def test_devices_redeem_retry_gate_skips_mailbox_when_history_present(
    tmp_path, capsys, monkeypatch
):
    # Qodo Warning on #743: a replay whose history is entirely
    # present must not wait on the mailbox lock at all — the gate
    # decides from the inbox alone and never opens the mailbox.
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_history(node_a)
    capsys.readouterr()

    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            [
                "devices",
                "pair",
                "--to",
                str(bundle_path),
                "--data-dir",
                str(node_a),
                "--json",
            ]
        )
        == 0
    )
    capsys.readouterr()

    node_b = tmp_path / "b"
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["history"]["imported"] == 3

    # Every snapshot record is stored: the gate must refuse the
    # retry without constructing a Mailbox at all.
    class _NoMailbox:
        def __init__(self, *args, **kwargs):
            raise AssertionError("gate must not open the mailbox")

    monkeypatch.setattr(cli, "Mailbox", _NoMailbox)
    config = TauConfig.resolve(data_dir=node_b)
    registry = DeviceRegistry(config.devices_path)
    identity = load_identity(config.identity_path)
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    assert cli._history_retry_allowed(config, registry, identity, bundle) is False


def test_devices_redeem_retry_gate_falls_back_when_mailbox_lock_held(
    tmp_path, capsys, monkeypatch
):
    # Qodo Warning on #743: when the gate does need the markers and
    # the mailbox lock cannot be acquired in time, it falls back to
    # the missing-record check instead of failing the retry.
    fcntl = pytest.importorskip("fcntl")
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_history(node_a)
    capsys.readouterr()

    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            [
                "devices",
                "pair",
                "--to",
                str(bundle_path),
                "--data-dir",
                str(node_a),
                "--json",
            ]
        )
        == 0
    )
    capsys.readouterr()

    node_b = tmp_path / "b"
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["history"]["imported"] == 3

    # One snapshot record is missing, so the gate needs the markers —
    # but another process holds the mailbox lock.
    inbox_b = Inbox(node_b / "inbox.json")
    inbox_b.delete("msg-1")
    lock_path = node_b / "mailbox.json.lock"
    handle = open(lock_path, "a")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        monkeypatch.setattr(cli, "_HISTORY_RETRY_LOCK_TIMEOUT", 0.0)
        config = TauConfig.resolve(data_dir=node_b)
        registry = DeviceRegistry(config.devices_path)
        identity = load_identity(config.identity_path)
        bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
        assert (
            cli._history_retry_allowed(config, registry, identity, bundle)
            is True
        )
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def test_devices_pair_redeem_skips_retracted_history(tmp_path, capsys):
    # A message the sender retracted after the bundle was minted must
    # not be resurrected by the stale snapshot: the redeem passes this
    # node's retraction markers into the history import (see #736).
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_history(node_a)
    capsys.readouterr()

    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            [
                "devices",
                "pair",
                "--to",
                str(bundle_path),
                "--data-dir",
                str(node_a),
                "--json",
            ]
        )
        == 0
    )
    capsys.readouterr()

    # The second device learned of the retract before redeeming: its
    # own mailbox already carries the marker for the peer's message.
    node_b = tmp_path / "b"
    node_b.mkdir(parents=True)
    Mailbox(node_b / "mailbox.json").record_retraction("a" * 32, "msg-1")
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["history"] == {
        "imported": 2,
        "present": 0,
        "skipped": 1,
        "errors": [
            {"id": "msg-1", "error": "message was retracted by its sender"}
        ],
    }
    inbox_b = Inbox(node_b / "inbox.json")
    stored = inbox_b.list_messages(direction=None)
    assert [entry["text"] for entry in stored] == ["group hello", "my reply"]


def test_devices_pair_no_history_mints_identity_only(tmp_path, capsys):
    node_a = _init_node(tmp_path, "a", "alice")
    _seed_history(node_a)
    capsys.readouterr()

    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            [
                "devices",
                "pair",
                "--to",
                str(bundle_path),
                "--no-history",
                "--data-dir",
                str(node_a),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["history_messages"] == 0

    node_b = tmp_path / "b"
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert "history" not in payload
    assert len(Inbox(node_b / "inbox.json")) == 0


def test_devices_pair_refuses_existing_bundle_without_force(tmp_path, capsys):
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node)]
        )
        == 0
    )
    capsys.readouterr()
    code = cli.main(
        ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node)]
    )
    assert code == 1
    assert "already exists" in capsys.readouterr().err
    assert (
        cli.main(
            [
                "devices",
                "pair",
                "--to",
                str(bundle_path),
                "--data-dir",
                str(node),
                "--force",
            ]
        )
        == 0
    )


def test_devices_pair_refuses_while_node_serving(tmp_path, capsys):
    # Minting under the instance lock: a concurrent `tau id import
    # --force` cannot slip between the bundle creation and
    # record_issued() and land the old identity's offer in the
    # replacement identity's registry (see #715).  When the lock is
    # held — this process stands in for the serving node — pair fails
    # before minting anything: no bundle file and no recorded offer.
    # The refusal names the lock holder as another tau process, not
    # necessarily a serving node: a backup holds the same lock.
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    config = TauConfig.resolve(data_dir=node)
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    try:
        bundle_path = tmp_path / "pairing.json"
        code = cli.main(
            [
                "devices",
                "pair",
                "--to",
                str(bundle_path),
                "--data-dir",
                str(node),
            ]
        )
    finally:
        lock.release()
    assert code == 1
    err = capsys.readouterr().err
    assert "is in use by another tau process" in err
    assert f"(pid {os.getpid()})" in err
    assert not bundle_path.exists()
    assert not list(tmp_path.glob(".pairing.json.*.tmp"))
    assert cli.main(["devices", "list", "--data-dir", str(node)]) == 0
    assert "no pairing offers issued" in capsys.readouterr().out


def test_devices_pair_failed_repair_keeps_previous_bundle(
    tmp_path, capsys, monkeypatch
):
    # A failed --force re-pair must not destroy the previous bundle:
    # the new bundle is staged until its offer is recorded, so a
    # registry-write failure removes only the staged file and the old
    # bundle stays redeemable (see #717).
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    original = bundle_path.read_bytes()
    capsys.readouterr()

    def _failing_record_issued(*args, **kwargs):
        raise OSError("registry write failed")

    monkeypatch.setattr(DeviceRegistry, "record_issued", _failing_record_issued)
    code = cli.main(
        [
            "devices",
            "pair",
            "--to",
            str(bundle_path),
            "--data-dir",
            str(node_a),
            "--force",
        ]
    )
    assert code == 1
    assert "registry write failed" in capsys.readouterr().err
    # The previous bundle is byte-identical, no staged file is left
    # behind, no orphan offer was recorded, and the old bundle still
    # redeems on a fresh device.
    assert bundle_path.read_bytes() == original
    assert list(tmp_path.glob(".pairing.json.*.tmp")) == []
    assert cli.main(["devices", "list", "--data-dir", str(node_a)]) == 0
    assert "pairing offers issued: 1" in capsys.readouterr().out
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(tmp_path / "b"),
                "--json",
            ]
        )
        == 0
    )


def test_devices_pair_corrupt_reload_keeps_previous_bundle(
    tmp_path, capsys, monkeypatch
):
    # The registry's locked reload inside record_issued() can reject
    # a file that was fine at the initial load (DevicesError, not
    # OSError) — that failure must clean up the staged bundle too, or
    # a redeemable bundle with the private identity is left behind in
    # a hidden file the offer list does not track (see #723 review).
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    original = bundle_path.read_bytes()
    capsys.readouterr()

    original_load = DeviceRegistry._load
    calls = []

    def _load_then_corrupt(self):
        calls.append(1)
        if len(calls) == 2:
            # The locked reload inside record_issued(): the registry
            # was fine at the initial load (call 1) and is corrupt
            # now.  Later loads pass through, so the list/redeem
            # checks below see the untouched registry.
            raise DevicesError("device registry is corrupt (test)")
        return original_load(self)

    monkeypatch.setattr(DeviceRegistry, "_load", _load_then_corrupt)
    code = cli.main(
        [
            "devices",
            "pair",
            "--to",
            str(bundle_path),
            "--data-dir",
            str(node_a),
            "--force",
        ]
    )
    assert code == 1
    assert "corrupt" in capsys.readouterr().err
    assert bundle_path.read_bytes() == original
    assert list(tmp_path.glob(".pairing.json.*.tmp")) == []
    assert cli.main(["devices", "list", "--data-dir", str(node_a)]) == 0
    assert "pairing offers issued: 1" in capsys.readouterr().out
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(tmp_path / "b"),
                "--json",
            ]
        )
        == 0
    )


def test_devices_pair_interrupt_during_replace_removes_staging(
    tmp_path, capsys, monkeypatch
):
    # An interrupt during the final os.replace must clean up the
    # staged bundle like any other failure there: the window is one
    # syscall, but a KeyboardInterrupt bypasses an OSError-only
    # handler and leaves the private identity in a hidden staging
    # file (see #724).  The previous bundle stays byte-identical
    # and redeemable.  The patch raises only for the final replace
    # (destination is the bundle target): the registry's own atomic
    # write inside record_issued() also uses os.replace, and an
    # interrupt there is already covered by the recording cleanup.
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    original = bundle_path.read_bytes()
    capsys.readouterr()

    real_replace = os.replace

    def _interrupted_replace(src, dst):
        if os.fspath(dst) == str(bundle_path):
            raise KeyboardInterrupt("interrupted during replace (test)")
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", _interrupted_replace)
    code = cli.main(
        [
            "devices",
            "pair",
            "--to",
            str(bundle_path),
            "--data-dir",
            str(node_a),
            "--force",
        ]
    )
    assert code == cli._EXIT_INTERRUPTED
    assert bundle_path.read_bytes() == original
    assert list(tmp_path.glob(".pairing.json.*.tmp")) == []
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(tmp_path / "b"),
                "--json",
            ]
        )
        == 0
    )


def test_devices_pair_without_identity_returns_error(tmp_path, capsys):
    code = cli.main(
        [
            "devices",
            "pair",
            "--to",
            str(tmp_path / "pairing.json"),
            "--data-dir",
            str(tmp_path / "missing"),
        ]
    )
    assert code == 1
    assert "no identity" in capsys.readouterr().err


def test_devices_redeem_refuses_existing_identity_without_force(tmp_path, capsys):
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "bob")
    capsys.readouterr()
    destination_a = _destination(node_a, capsys)
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()

    code = cli.main(
        ["devices", "redeem", "--from", str(bundle_path), "--data-dir", str(node_b)]
    )
    assert code == 1
    assert "already exists" in capsys.readouterr().err

    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--force",
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["destination"] == destination_a


def test_devices_redeem_rejects_tampered_bundle(tmp_path, capsys):
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    raw = json.loads(bundle_path.read_text(encoding="utf-8"))
    raw["token"] = "0" * 32 if raw["token"] != "0" * 32 else "1" * 32
    bundle_path.write_text(json.dumps(raw), encoding="utf-8")
    capsys.readouterr()

    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(tmp_path / "b"),
        ]
    )
    assert code == 1
    assert "signature does not verify" in capsys.readouterr().err


def test_devices_redeem_rejects_expired_bundle(tmp_path, capsys):
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    identity = load_or_create_identity(node_a / "identity")
    bundle = build_pairing_bundle(identity, ttl_seconds=60, now=1000.0)
    bundle_path = tmp_path / "pairing.json"
    bundle_path.write_text(json.dumps(bundle), encoding="utf-8")

    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(tmp_path / "b"),
        ]
    )
    assert code == 1
    assert "expired" in capsys.readouterr().err


def test_devices_pair_corrupt_registry_writes_no_bundle(tmp_path, capsys):
    # A corrupt registry must fail the command before any bundle file
    # exists — not leave a redeemable bundle `devices list` cannot
    # see (see #714).
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    (node / "devices.json").write_text("{not json", encoding="utf-8")
    bundle_path = tmp_path / "pairing.json"
    code = cli.main(
        ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node)]
    )
    assert code == 1
    assert "corrupt" in capsys.readouterr().err
    assert not bundle_path.exists()


def test_devices_pair_preserves_parent_permissions(tmp_path, capsys):
    # The bundle goes into a caller-selected folder: the folder keeps
    # its permissions, only the bundle file itself is private — the
    # same rule `tau id export` applies (see #714).
    node = _init_node(tmp_path, "node", "alice")
    capsys.readouterr()
    shared = tmp_path / "shared"
    shared.mkdir()
    os.chmod(shared, 0o755)
    bundle_path = shared / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node)]
        )
        == 0
    )
    assert stat.S_IMODE(os.stat(shared).st_mode) == 0o755
    assert stat.S_IMODE(os.stat(bundle_path).st_mode) == 0o600


def test_devices_redeem_records_token_before_install(
    tmp_path, capsys, monkeypatch
):
    # If recording the token fails, the old identity must stay in
    # place: the reverse order would install an identity with no
    # redeemed-token record (see #714).
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "bob")
    capsys.readouterr()
    destination_b = _destination(node_b, capsys)
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()

    def _failing_record(self, *args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(DeviceRegistry, "record_redeemed", _failing_record)
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "disk full" in capsys.readouterr().err
    assert _destination(node_b, capsys) == destination_b


def test_devices_redeem_retry_restores_pairing_after_failed_record(
    tmp_path, capsys, monkeypatch
):
    # If the pairing-status write fails, the token is already
    # recorded and the identity is installed, so a plain retry was
    # rejected as "already redeemed" and `devices list` never showed
    # the pairing (see #718).  The retry now completes the record
    # instead of rejecting the bundle; a third redeem is still a
    # plain duplicate.
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = tmp_path / "b"
    capsys.readouterr()
    destination_a = _destination(node_a, capsys)
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()

    original_record = DeviceRegistry.record_paired_from
    calls = []

    def _failing_first_record(self, *args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise OSError("disk full")
        return original_record(self, *args, **kwargs)

    monkeypatch.setattr(DeviceRegistry, "record_paired_from", _failing_first_record)
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--json",
        ]
    )
    assert code == 1
    err = capsys.readouterr().err
    assert "could not record pairing" in err
    assert "retry the same bundle" in err
    # The identity was installed, but no pairing status landed yet.
    assert _destination(node_b, capsys) == destination_a
    assert cli.main(["devices", "list", "--data-dir", str(node_b)]) == 0
    assert "paired from:" not in capsys.readouterr().out

    # Retry the same bundle: the token is redeemed, but the pairing
    # record is restored instead of rejecting the retry.
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--json",
        ]
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["paired_from"] == destination_a
    assert payload["verifications_cleared"] == 0
    assert cli.main(["devices", "list", "--data-dir", str(node_b)]) == 0
    assert f"paired from: {destination_a}" in capsys.readouterr().out

    # A third redeem is still a plain duplicate, not a recovery.
    code = cli.main(
        ["devices", "redeem", "--from", str(bundle_path), "--data-dir", str(node_b)]
    )
    assert code == 1
    assert "already redeemed" in capsys.readouterr().err


def test_devices_list_after_identity_replace_resets_records(tmp_path, capsys):
    # Pairing records belong to the installed identity: replacing it
    # with an unrelated one must not leave the old pairing listed
    # under the new identity (see #714).
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "bob")
    capsys.readouterr()
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()
    assert cli.main(["devices", "list", "--data-dir", str(node_a)]) == 0
    assert "pairing offers issued: 1" in capsys.readouterr().out

    backup = tmp_path / "bob.identity"
    assert (
        cli.main(
            ["id", "export", "--to", str(backup), "--data-dir", str(node_b), "--json"]
        )
        == 0
    )
    capsys.readouterr()
    assert (
        cli.main(
            [
                "id",
                "import",
                "--from",
                str(backup),
                "--data-dir",
                str(node_a),
                "--force",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert cli.main(["devices", "list", "--data-dir", str(node_a)]) == 0
    out = capsys.readouterr().out
    assert "no pairing offers issued" in out
    assert "paired from:" not in out


def test_devices_redeem_force_resets_previous_records(tmp_path, capsys):
    # A forced redeem that swaps the identity drops the old node's
    # issued offers, then records the new pairing (see #714).
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "bob")
    capsys.readouterr()
    bundle_a = tmp_path / "pairing-a.json"
    bundle_b = tmp_path / "pairing-b.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_a), "--data-dir", str(node_a)]
        )
        == 0
    )
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_b), "--data-dir", str(node_b)]
        )
        == 0
    )
    capsys.readouterr()
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_b),
                "--data-dir",
                str(node_a),
                "--force",
                "--json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["paired_from"] == _destination(node_b, capsys)
    assert cli.main(["devices", "list", "--data-dir", str(node_a)]) == 0
    out = capsys.readouterr().out
    assert "no pairing offers issued" in out
    assert "paired from:" in out


def test_devices_redeem_refused_after_identity_swap(tmp_path, capsys):
    # A bundle redeemed under one identity must stay single-use
    # after that identity is swapped out: the reset drops the old
    # pairing, not the redeemed-token record (see #716).
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "bob")
    node_c = _init_node(tmp_path, "c", "carol")
    capsys.readouterr()
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--force",
            ]
        )
        == 0
    )
    capsys.readouterr()
    backup = tmp_path / "carol.identity"
    assert (
        cli.main(
            ["id", "export", "--to", str(backup), "--data-dir", str(node_c), "--json"]
        )
        == 0
    )
    capsys.readouterr()
    assert (
        cli.main(
            [
                "id",
                "import",
                "--from",
                str(backup),
                "--data-dir",
                str(node_b),
                "--force",
            ]
        )
        == 0
    )
    capsys.readouterr()
    destination_carol = _destination(node_c, capsys)
    assert _destination(node_b, capsys) == destination_carol
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "already redeemed" in capsys.readouterr().err
    assert _destination(node_b, capsys) == destination_carol


def test_devices_import_failure_keeps_pairing_records(
    tmp_path, capsys, monkeypatch
):
    # The reset is staged until the new identity is installed: if
    # the identity write fails, the old node's offers and pairing
    # must still be listed (see #716).
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "bob")
    node_c = _init_node(tmp_path, "c", "carol")
    capsys.readouterr()
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--force",
            ]
        )
        == 0
    )
    capsys.readouterr()
    second_bundle = tmp_path / "pairing-b.json"
    assert (
        cli.main(
            [
                "devices",
                "pair",
                "--to",
                str(second_bundle),
                "--data-dir",
                str(node_b),
            ]
        )
        == 0
    )
    capsys.readouterr()
    backup = tmp_path / "carol.identity"
    assert (
        cli.main(
            ["id", "export", "--to", str(backup), "--data-dir", str(node_c), "--json"]
        )
        == 0
    )
    capsys.readouterr()
    destination_alice = _destination(node_b, capsys)

    def _failing_save(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(cli, "save_identity", _failing_save)
    code = cli.main(
        [
            "id",
            "import",
            "--from",
            str(backup),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "disk full" in capsys.readouterr().err
    # The old identity is still installed and its pairing records
    # survived the failed swap.
    assert _destination(node_b, capsys) == destination_alice
    assert cli.main(["devices", "list", "--data-dir", str(node_b)]) == 0
    out = capsys.readouterr().out
    assert "pairing offers issued: 1" in out
    assert "paired from:" in out


def test_devices_redeem_retry_after_failed_identity_write(
    tmp_path, capsys, monkeypatch
):
    # A redeem whose identity write fails must not burn the bundle:
    # the recorded token is released, so the same bundle can be
    # retried once the write works (see #716).  `save_identity`
    # raises IdentityError (not OSError) when the write itself fails,
    # so the rollback must cover that too.
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "bob")
    capsys.readouterr()
    destination_b = _destination(node_b, capsys)
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()

    real_save = cli.save_identity
    calls = {"n": 0}

    def _flaky_save(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise IdentityError("could not write identity")
        return real_save(*args, **kwargs)

    monkeypatch.setattr(cli, "save_identity", _flaky_save)
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "could not write identity" in capsys.readouterr().err
    assert _destination(node_b, capsys) == destination_b
    # The failed attempt left no redeemed-token record behind: the
    # same bundle redeems cleanly on retry.
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--force",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert _destination(node_b, capsys) == _destination(node_a, capsys)


def test_devices_redeem_failed_reset_restores_identity_symlink(
    tmp_path, capsys, monkeypatch
):
    # A live identity symlink is a shared identity: a failed reset
    # after a successful install must restore the link, not the
    # snapshot bytes as a regular file (see #722).
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "bob")
    capsys.readouterr()
    destination_b = _destination(node_b, capsys)
    # Bob's identity lives in a shared file the data dir links to.
    shared = tmp_path / "shared.identity"
    identity_path = node_b / "identity"
    shared.write_bytes(identity_path.read_bytes())
    identity_path.unlink()
    identity_path.symlink_to(shared)
    assert _destination(node_b, capsys) == destination_b
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()

    original_reset = DeviceRegistry.reset
    calls = {"n": 0}

    def _fail_first_reset(self, *, now=None, keep_offers_owned_by=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise DevicesError("disk full")
        return original_reset(self, now=now, keep_offers_owned_by=keep_offers_owned_by)

    monkeypatch.setattr(DeviceRegistry, "reset", _fail_first_reset)
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "disk full" in capsys.readouterr().err
    # The rollback restored the symlink itself, and Bob's identity
    # is still the one installed.
    assert identity_path.is_symlink()
    assert os.readlink(identity_path) == str(shared)
    assert _destination(node_b, capsys) == destination_b
    # The failed attempt left no redeemed-token record behind: the
    # same bundle redeems cleanly on retry.
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--force",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert _destination(node_b, capsys) == _destination(node_a, capsys)


def test_devices_redeem_failed_reset_keeps_symlink_target_at_staging_path(
    tmp_path, capsys, monkeypatch
):
    # The rollback stages the restored link beside the identity: when
    # the old symlink itself points at ``identity.restore-tmp``, that
    # path is the live identity's target, not scratch space — a failed
    # reset must restore the link without deleting the target file.
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "bob")
    capsys.readouterr()
    destination_b = _destination(node_b, capsys)
    # Bob's identity lives in the file the old fixed staging name
    # would have pointed at.
    target = node_b / "identity.restore-tmp"
    identity_path = node_b / "identity"
    target.write_bytes(identity_path.read_bytes())
    original = target.read_bytes()
    identity_path.unlink()
    identity_path.symlink_to(target)
    assert _destination(node_b, capsys) == destination_b
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()

    original_reset = DeviceRegistry.reset
    calls = {"n": 0}

    def _fail_first_reset(self, *, now=None, keep_offers_owned_by=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise DevicesError("disk full")
        return original_reset(self, now=now, keep_offers_owned_by=keep_offers_owned_by)

    monkeypatch.setattr(DeviceRegistry, "reset", _fail_first_reset)
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "disk full" in capsys.readouterr().err
    # The rollback restored the symlink itself, and the target file
    # the link points at still holds Bob's identity bytes.
    assert identity_path.is_symlink()
    assert os.readlink(identity_path) == str(target)
    assert target.read_bytes() == original
    assert _destination(node_b, capsys) == destination_b
    # The failed attempt left no redeemed-token record behind: the
    # same bundle redeems cleanly on retry.
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--force",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert _destination(node_b, capsys) == _destination(node_a, capsys)


def test_devices_redeem_failed_reset_restores_dangling_identity_symlink(
    tmp_path, capsys, monkeypatch
):
    # A dangling identity symlink is not an absent identity: a
    # failed reset after a successful install must restore the
    # link, not remove it (see #722).
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "bob")
    capsys.readouterr()
    identity_path = node_b / "identity"
    dangling = tmp_path / "gone.identity"
    identity_path.unlink()
    identity_path.symlink_to(dangling)
    assert not identity_path.exists()
    assert identity_path.is_symlink()
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()

    original_reset = DeviceRegistry.reset
    calls = {"n": 0}

    def _fail_first_reset(self, *, now=None, keep_offers_owned_by=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise DevicesError("disk full")
        return original_reset(self, now=now, keep_offers_owned_by=keep_offers_owned_by)

    # A dangling link counts as "no identity" for the reset
    # decision, so force the reset this rollback must survive.
    monkeypatch.setattr(DeviceRegistry, "reset", _fail_first_reset)
    monkeypatch.setattr(
        cli, "_devices_reset_needed", lambda config, identity: True
    )
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "disk full" in capsys.readouterr().err
    # The rollback restored the dangling link itself.
    assert identity_path.is_symlink()
    assert os.readlink(identity_path) == str(dangling)
    assert not identity_path.exists()
    # The failed attempt left no redeemed-token record behind: the
    # same bundle redeems cleanly on retry.
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--force",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert _destination(node_b, capsys) == _destination(node_a, capsys)


def test_devices_import_failed_reset_restores_identity_symlink(
    tmp_path, capsys, monkeypatch
):
    # The import path shares the rollback: a failed reset after a
    # successful install must restore the symlinked identity, not
    # leave the imported bytes as a regular file (see #722).
    node_b = _init_node(tmp_path, "b", "bob")
    node_c = _init_node(tmp_path, "c", "carol")
    capsys.readouterr()
    destination_b = _destination(node_b, capsys)
    shared = tmp_path / "shared.identity"
    identity_path = node_b / "identity"
    shared.write_bytes(identity_path.read_bytes())
    identity_path.unlink()
    identity_path.symlink_to(shared)
    assert _destination(node_b, capsys) == destination_b
    backup = tmp_path / "carol.identity"
    assert (
        cli.main(
            ["id", "export", "--to", str(backup), "--data-dir", str(node_c), "--json"]
        )
        == 0
    )
    capsys.readouterr()

    original_reset = DeviceRegistry.reset
    calls = {"n": 0}

    def _fail_first_reset(self, *, now=None, keep_offers_owned_by=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise DevicesError("disk full")
        return original_reset(self, now=now, keep_offers_owned_by=keep_offers_owned_by)

    monkeypatch.setattr(DeviceRegistry, "reset", _fail_first_reset)
    code = cli.main(
        [
            "id",
            "import",
            "--from",
            str(backup),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "disk full" in capsys.readouterr().err
    # The rollback restored the symlink itself, and Bob's identity
    # is still the one installed.
    assert identity_path.is_symlink()
    assert os.readlink(identity_path) == str(shared)
    assert _destination(node_b, capsys) == destination_b


def test_devices_redeem_snapshot_failure_keeps_bundle_retryable(
    tmp_path, capsys, monkeypatch
):
    # The rollback snapshot is taken before the token is recorded:
    # if the old identity cannot be read, the redeem fails while
    # the bundle is still unredeemed, so it can be retried (see #716).
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "bob")
    capsys.readouterr()
    destination_b = _destination(node_b, capsys)
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()

    real_snapshot = cli._snapshot_previous_identity
    calls = {"n": 0}

    def _flaky_snapshot(config):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("cannot read identity")
        return real_snapshot(config)

    monkeypatch.setattr(cli, "_snapshot_previous_identity", _flaky_snapshot)
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "cannot read identity" in capsys.readouterr().err
    assert _destination(node_b, capsys) == destination_b
    # No token was recorded: the same bundle redeems on retry.
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_path),
                "--data-dir",
                str(node_b),
                "--force",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert _destination(node_b, capsys) == _destination(node_a, capsys)


def test_devices_redeem_snapshot_failure_keeps_verifications(
    tmp_path, capsys, monkeypatch
):
    # The snapshot is taken before verifications are cleared: if the
    # old identity cannot be read, the redeem fails while the old
    # identity's peer verifications are still intact (see #721).
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "bob")
    capsys.readouterr()
    destination_a = _destination(node_a, capsys)
    destination_b = _destination(node_b, capsys)
    # Bob verified Alice's safety number under Bob's identity; a
    # failed redeem must not throw that trust away.
    registry = PeerRegistry(node_b / "peers.json")
    peer = registry.upsert(
        bytes.fromhex(destination_a),
        bytes([7]) * PUBLIC_KEY_BYTES,
        name="alice",
        now=100.0,
    )
    registry.mark_verified(peer.destination_hash, "00001 00002", now=150.0)
    assert registry.save()
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()

    def _failing_snapshot(config):
        raise OSError("cannot read identity")

    monkeypatch.setattr(cli, "_snapshot_previous_identity", _failing_snapshot)
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "cannot read identity" in capsys.readouterr().err
    # The old identity is still installed and its verification of
    # Alice survived the failed redeem.
    assert _destination(node_b, capsys) == destination_b
    reloaded = PeerRegistry(node_b / "peers.json")
    reloaded_peer = reloaded.get(peer.destination_hash)
    assert reloaded_peer is not None
    assert reloaded_peer.is_verified
    assert reloaded_peer.verified_fingerprint == "00001 00002"
    assert reloaded_peer.verified_at == 150.0


def test_devices_redeem_failed_restore_keeps_token(
    tmp_path, capsys, monkeypatch
):
    # Identity write succeeded, the pairing reset failed, and the
    # rollback restore failed too: the replacement identity stays
    # installed and the token must be kept — releasing it would let
    # the same bundle be redeemed again (see #716).
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "bob")
    capsys.readouterr()
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()

    def _fail_reset(self, *, now=None, keep_offers_owned_by=None):
        raise OSError("disk full")

    def _fail_restore(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(cli.DeviceRegistry, "reset", _fail_reset)
    monkeypatch.setattr(cli, "atomic_write_bytes", _fail_restore)
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "disk full" in capsys.readouterr().err
    # The new identity stayed installed (the restore failed), so the
    # token must be kept — releasing it would let the same bundle be
    # redeemed again (see #716).
    assert _destination(node_b, capsys) == _destination(node_a, capsys)
    destination_a = _destination(node_a, capsys)
    # The retry cannot complete while the reset still fails: the
    # recovery completes the pending reset before recording the
    # pairing (see #718), and must not report success with the old
    # identity's records still in place.
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "could not complete the pairing reset" in capsys.readouterr().err
    # Once the failure clears, the retry completes the pairing; the
    # token was kept throughout, so a further redeem is refused.
    monkeypatch.undo()
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 0
    assert cli.main(["devices", "list", "--data-dir", str(node_b)]) == 0
    assert f"paired from: {destination_a}" in capsys.readouterr().out
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "already redeemed" in capsys.readouterr().err


def test_devices_redeem_retry_clears_stale_offers_after_failed_reset(
    tmp_path, capsys, monkeypatch
):
    # A failed reset followed by a failed identity rollback leaves
    # the old identity's issued offers in place (Qodo Warning on
    # #728): the retry must complete the pending reset before
    # recording the pairing, or `devices list` would attribute the
    # old offers to the new identity.
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "bob")
    capsys.readouterr()
    destination_a = _destination(node_a, capsys)
    # Bob issues an offer as his own identity first.
    offer_path = tmp_path / "bob-offer.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(offer_path), "--data-dir", str(node_b)]
        )
        == 0
    )
    capsys.readouterr()
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()

    original_reset = DeviceRegistry.reset
    calls = []

    def _fail_first_reset(self, *, now=None, keep_offers_owned_by=None):
        calls.append(1)
        if len(calls) == 1:
            raise OSError("disk full")
        return original_reset(self, now=now, keep_offers_owned_by=keep_offers_owned_by)

    def _fail_restore(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(DeviceRegistry, "reset", _fail_first_reset)
    monkeypatch.setattr(cli, "atomic_write_bytes", _fail_restore)
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "disk full" in capsys.readouterr().err
    # Alice's identity is installed, but Bob's offer is still listed.
    assert _destination(node_b, capsys) == destination_a
    assert cli.main(["devices", "list", "--data-dir", str(node_b)]) == 0
    out = capsys.readouterr().out
    assert "pairing offers issued: 1" in out
    assert "paired from:" not in out

    # Retry: the pending reset runs first, so the stale offer is
    # gone and the pairing is recorded for the new identity.
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 0
    assert cli.main(["devices", "list", "--data-dir", str(node_b)]) == 0
    out = capsys.readouterr().out
    assert "no pairing offers issued" in out
    assert f"paired from: {destination_a}" in out


def test_devices_redeem_retry_keeps_offers_minted_after_failed_attempt(
    tmp_path, capsys, monkeypatch
):
    # A failed reset followed by a failed identity rollback leaves
    # the old identity's issued offers in place (see #728).  The node
    # then serves as the new identity, so an offer minted before the
    # retry belongs to the new identity: the pending reset the retry
    # completes must drop only the old identity's offers and keep
    # the new one (see #732).
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "bob")
    capsys.readouterr()
    destination_a = _destination(node_a, capsys)
    # Bob issues an offer as his own identity first.
    offer_path = tmp_path / "bob-offer.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(offer_path), "--data-dir", str(node_b)]
        )
        == 0
    )
    capsys.readouterr()
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()

    original_reset = DeviceRegistry.reset
    calls = []

    def _fail_first_reset(self, *, now=None, keep_offers_owned_by=None):
        calls.append(1)
        if len(calls) == 1:
            raise OSError("disk full")
        return original_reset(
            self, now=now, keep_offers_owned_by=keep_offers_owned_by
        )

    def _fail_restore(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(DeviceRegistry, "reset", _fail_first_reset)
    monkeypatch.setattr(cli, "atomic_write_bytes", _fail_restore)
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "disk full" in capsys.readouterr().err
    # Alice's identity is installed, but Bob's offer is still listed.
    assert _destination(node_b, capsys) == destination_a
    assert cli.main(["devices", "list", "--data-dir", str(node_b)]) == 0
    out = capsys.readouterr().out
    assert "pairing offers issued: 1" in out
    assert "paired from:" not in out

    # Mint an offer as the new identity before retrying: the node
    # serves as Alice now, so this offer is hers.
    new_offer_path = tmp_path / "alice-offer.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(new_offer_path), "--data-dir", str(node_b)]
        )
        == 0
    )
    capsys.readouterr()
    assert cli.main(["devices", "list", "--data-dir", str(node_b)]) == 0
    out = capsys.readouterr().out
    assert "pairing offers issued: 2" in out

    # Retry: the pending reset drops Bob's stale offer but keeps
    # Alice's, and the pairing is recorded for the new identity.
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 0
    assert cli.main(["devices", "list", "--data-dir", str(node_b)]) == 0
    out = capsys.readouterr().out
    assert "pairing offers issued: 1" in out
    assert f"paired from: {destination_a}" in out

    # The surviving offer is Alice's (minted by the new identity), not
    # Bob's stale one.
    surviving = DeviceRegistry(node_b / "devices.json").issued_offers()
    assert [offer["token"] for offer in surviving] == [
        json.loads(new_offer_path.read_text(encoding="utf-8"))["token"]
    ]
    assert json.loads(offer_path.read_text(encoding="utf-8"))["token"] not in {
        offer["token"] for offer in surviving
    }


def test_devices_redeem_retry_clears_stale_pairing_after_failed_reset(
    tmp_path, capsys, monkeypatch
):
    # A node that was already paired, then suffers a failed reset
    # plus a failed identity rollback, keeps the previous identity's
    # pairing record (Qodo Warning on #729): the retry must be
    # allowed to clear that stale pairing via the pending reset
    # instead of being rejected as already redeemed.
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "bob")
    node_c = _init_node(tmp_path, "c", "carol")
    capsys.readouterr()
    destination_a = _destination(node_a, capsys)
    destination_c = _destination(node_c, capsys)
    # Bob pairs from Carol first.
    bundle_c_path = tmp_path / "carol-pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_c_path), "--data-dir", str(node_c)]
        )
        == 0
    )
    capsys.readouterr()
    assert (
        cli.main(
            [
                "devices",
                "redeem",
                "--from",
                str(bundle_c_path),
                "--data-dir",
                str(node_b),
                "--force",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert cli.main(["devices", "list", "--data-dir", str(node_b)]) == 0
    assert f"paired from: {destination_c}" in capsys.readouterr().out
    # Now Alice's bundle for Bob, with the reset and the rollback
    # both failing.
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()

    original_reset = DeviceRegistry.reset
    calls = []

    def _fail_first_reset(self, *, now=None, keep_offers_owned_by=None):
        calls.append(1)
        if len(calls) == 1:
            raise OSError("disk full")
        return original_reset(self, now=now, keep_offers_owned_by=keep_offers_owned_by)

    def _fail_restore(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(DeviceRegistry, "reset", _fail_first_reset)
    monkeypatch.setattr(cli, "atomic_write_bytes", _fail_restore)
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "disk full" in capsys.readouterr().err
    # Alice's identity is installed, but Carol's pairing is still listed.
    assert _destination(node_b, capsys) == destination_a
    assert cli.main(["devices", "list", "--data-dir", str(node_b)]) == 0
    out = capsys.readouterr().out
    assert f"paired from: {destination_c}" in out

    # Retry: the pending reset clears the stale pairing, and the new
    # pairing is recorded for Alice.
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 0
    assert cli.main(["devices", "list", "--data-dir", str(node_b)]) == 0
    out = capsys.readouterr().out
    assert f"paired from: {destination_a}" in out

    # A replay of the same bundle now carries the same token as the
    # recorded pairing, so it must still be refused.
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(node_b),
            "--force",
        ]
    )
    assert code == 1
    assert "already redeemed" in capsys.readouterr().err


def test_devices_import_corrupt_registry_rolls_back_identity(
    tmp_path, capsys, monkeypatch
):
    # A DevicesError from the post-install reset (corrupt registry
    # on reload) must roll the identity back, not leave the
    # replacement installed alongside the old pairing state (see #716).
    node_a = _init_node(tmp_path, "a", "alice")
    node_b = _init_node(tmp_path, "b", "carol")
    capsys.readouterr()
    destination_a = _destination(node_a, capsys)
    backup = tmp_path / "carol.identity"
    assert (
        cli.main(
            ["id", "export", "--to", str(backup), "--data-dir", str(node_b), "--json"]
        )
        == 0
    )
    capsys.readouterr()

    def _fail_reset(self, *, now=None, keep_offers_owned_by=None):
        raise DevicesError("device registry corrupt")

    monkeypatch.setattr(cli.DeviceRegistry, "reset", _fail_reset)
    code = cli.main(
        [
            "id",
            "import",
            "--from",
            str(backup),
            "--data-dir",
            str(node_a),
            "--force",
        ]
    )
    assert code == 1
    assert "corrupt" in capsys.readouterr().err
    assert _destination(node_a, capsys) == destination_a


def test_devices_redeem_rejects_oversize_bundle_file(tmp_path, capsys):
    bundle_path = tmp_path / "pairing.json"
    bundle_path.write_bytes(b"x" * (MAX_PAIRING_BUNDLE_BYTES + 1))
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(tmp_path / "b"),
        ]
    )
    assert code == 1
    assert "too large" in capsys.readouterr().err


def test_devices_redeem_rejects_bundle_that_grows_after_stat(
    tmp_path, capsys, monkeypatch
):
    # A bundle that is under the cap when stat runs but grows before
    # the read must still be refused (see #719): the redeem reads
    # through one bounded handle instead of trusting an earlier stat,
    # so the read itself — not the stat — enforces the cap.
    bundle_path = tmp_path / "pairing.json"
    bundle_path.write_bytes(b"{}")
    real_open = Path.open

    def _growing_open(self, *args, **kwargs):
        handle = real_open(self, *args, **kwargs)
        if self == bundle_path:
            handle.close()
            return io.BytesIO(b"x" * (MAX_PAIRING_BUNDLE_BYTES + 1))
        return handle

    monkeypatch.setattr(Path, "open", _growing_open)
    code = cli.main(
        [
            "devices",
            "redeem",
            "--from",
            str(bundle_path),
            "--data-dir",
            str(tmp_path / "b"),
        ]
    )
    assert code == 1
    assert "too large" in capsys.readouterr().err


def test_backup_preserves_device_registry(tmp_path, capsys):
    # A restore must keep the redeemed-token record: otherwise an
    # unexpired, previously redeemed bundle would pass the replay
    # check on the restored node (see #714).
    node_a = _init_node(tmp_path, "a", "alice")
    capsys.readouterr()
    bundle_path = tmp_path / "pairing.json"
    assert (
        cli.main(
            ["devices", "pair", "--to", str(bundle_path), "--data-dir", str(node_a)]
        )
        == 0
    )
    capsys.readouterr()
    node_b = tmp_path / "b"
    assert (
        cli.main(
            ["devices", "redeem", "--from", str(bundle_path), "--data-dir", str(node_b)]
        )
        == 0
    )
    capsys.readouterr()

    archive = tmp_path / "backup.tar.gz"
    assert cli.main(["backup", str(archive), "--data-dir", str(node_b)]) == 0
    capsys.readouterr()
    node_c = tmp_path / "c"
    assert (
        cli.main(["restore", str(archive), "--data-dir", str(node_c), "--json"]) == 0
    )
    capsys.readouterr()
    assert (node_c / "devices.json").is_file()
    code = cli.main(
        ["devices", "redeem", "--from", str(bundle_path), "--data-dir", str(node_c)]
    )
    assert code == 1
    assert "already redeemed" in capsys.readouterr().err
