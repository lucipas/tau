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


def test_registry_records_issued_and_redeemed(tmp_path):
    path = tmp_path / "devices.json"
    registry = DeviceRegistry(path)
    assert registry.issued_offers(now=1000.0) == []
    assert registry.paired_from is None
    registry.record_issued("a" * 32, 1000.0, 4600.0)
    offers = registry.issued_offers(now=1100.0)
    assert len(offers) == 1 and offers[0]["token"] == "a" * 32
    assert not registry.is_redeemed("a" * 32)
    registry.record_redeemed("a" * 32, "b" * 32, now=1200.0)
    assert registry.is_redeemed("a" * 32)
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
