"""Tests for linked-device pairing (slice 1 of #469, #713)."""

from __future__ import annotations

import json
import os
import stat

import pytest
import RNS

from tau import cli
from tau.devices import (
    DEFAULT_PAIRING_TTL_SECONDS,
    MAX_PAIRING_BUNDLE_BYTES,
    MAX_PAIRING_TTL_SECONDS,
    DeviceRegistry,
    DevicesError,
    build_pairing_bundle,
    verify_pairing_bundle,
)
from tau.identity import destination_hash, load_or_create_identity


def _identity():
    return RNS.Identity()


def _tampered(bundle, field, value):
    tampered = dict(bundle)
    tampered[field] = value
    return tampered


def test_pairing_bundle_roundtrip():
    identity = _identity()
    bundle = build_pairing_bundle(identity, ttl_seconds=600, now=1000.0)
    assert bundle["v"] == 1
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
        verify_pairing_bundle(_tampered(bundle, "v", 2), now=1100.0)
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


def test_registry_reset_drops_pairing_records(tmp_path):
    registry = DeviceRegistry(tmp_path / "devices.json")
    registry.record_issued("a" * 32, 1000.0, 4600.0)
    registry.record_redeemed("a" * 32, "b" * 32, 4600.0, now=1200.0)
    registry.record_paired_from("a" * 32, "b" * 32, now=1200.0)
    registry.reset()
    assert registry.issued_offers(now=1100.0) == []
    assert not registry.is_redeemed("a" * 32)
    assert registry.paired_from is None


def test_registry_prunes_expired_offers(tmp_path):
    registry = DeviceRegistry(tmp_path / "devices.json")
    registry.record_issued("a" * 32, 1000.0, 1060.0)
    assert registry.issued_offers(now=2000.0) == []


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

    redeem = parser.parse_args(
        ["devices", "redeem", "--from", "/tmp/bundle", "--force"]
    )
    assert redeem.func is cli.cmd_devices_redeem
    assert str(redeem.from_path) == "/tmp/bundle"
    assert redeem.force is True

    listing = parser.parse_args(["devices", "list"])
    assert listing.func is cli.cmd_devices_list

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
