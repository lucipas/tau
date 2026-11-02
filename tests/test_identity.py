from __future__ import annotations

import os
import stat

import pytest

from tau.identity import (
    IdentityError,
    destination_hash,
    load_identity,
    load_or_create_identity,
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
