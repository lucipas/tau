from __future__ import annotations

import os
import stat

import pytest
import RNS

from tau.identity import (
    IdentityError,
    destination_hash,
    load_identity,
    load_or_create_identity,
    load_public_key,
    save_identity,
)


def test_identity_is_created_once_and_reused(tmp_path):
    path = tmp_path / "identity"
    first = load_or_create_identity(path)
    assert path.exists()
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600

    second = load_or_create_identity(path)
    assert first.get_public_key() == second.get_public_key()
    assert first.hexhash == second.hexhash
    assert destination_hash(first) == destination_hash(second)


def test_load_identity_missing_returns_none(tmp_path):
    assert load_identity(tmp_path / "absent") is None


def test_corrupt_identity_raises(tmp_path):
    path = tmp_path / "identity"
    path.write_bytes(b"this is not a private key")
    with pytest.raises(IdentityError):
        load_identity(path)


def test_save_identity_roundtrip(tmp_path):
    import RNS

    identity = RNS.Identity()
    target = tmp_path / "nested" / "identity"
    save_identity(identity, target)
    loaded = load_identity(target)
    assert loaded is not None
    assert loaded.get_private_key() == identity.get_private_key()
    assert stat.S_IMODE(os.stat(target).st_mode) == 0o600


def test_load_public_key_accepts_valid_key():
    identity = RNS.Identity()
    loaded = RNS.Identity(create_keys=False)
    assert load_public_key(loaded, identity.get_public_key()) is True
    assert loaded.hash == identity.hash


def test_load_public_key_tolerates_none_return_on_success(
    load_public_key_without_return,
):
    # RNS 1.4.1 returns None even on success; the identity hash set by
    # the load is the success signal.
    identity = RNS.Identity()
    loaded = RNS.Identity(create_keys=False)
    assert load_public_key(loaded, identity.get_public_key()) is True
    assert loaded.hash == identity.hash


def test_load_public_key_rejects_bad_key():
    # Wrong length: RNS cannot split it into encryption/signature keys.
    loaded = RNS.Identity(create_keys=False)
    assert load_public_key(loaded, bytes(32)) is False
    assert loaded.hash is None


def test_load_public_key_rejects_failed_load_returning_none(monkeypatch):
    # 1.4.1 also returns None on failure, so a load that never sets
    # the identity hash must still be rejected.
    def load_noop(self, pub_bytes):
        return None

    monkeypatch.setattr(RNS.Identity, "load_public_key", load_noop)
    loaded = RNS.Identity(create_keys=False)
    assert load_public_key(loaded, RNS.Identity().get_public_key()) is False
    assert loaded.hash is None
