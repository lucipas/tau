"""Reticulum identity management."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

import RNS

from .protocol import APP_NAME, ASPECT
from .util import atomic_write_bytes, ensure_dir

log = logging.getLogger(__name__)

IDENTITY_FILE_MODE = 0o600


class IdentityError(RuntimeError):
    """Raised when an identity cannot be loaded or created."""


def load_identity(path: Path) -> Optional[RNS.Identity]:
    """Load an existing identity, returning ``None`` when it does not exist."""
    path = Path(path).expanduser()
    if not path.exists():
        return None
    identity = RNS.Identity.from_file(str(path))
    if identity is None:
        raise IdentityError(f"could not load identity from {path}")
    try:
        os.chmod(path, IDENTITY_FILE_MODE)
    except OSError:  # pragma: no cover - best effort
        pass
    return identity


def save_identity(identity: RNS.Identity, path: Path) -> Path:
    """Persist an identity atomically with owner-only permissions."""
    path = Path(path).expanduser()
    ensure_dir(path.parent)
    tmp = path.with_name(f".{path.name}.tmp")
    if not identity.to_file(str(tmp)):
        raise IdentityError(f"could not write identity to {path}")
    with open(tmp, "rb") as handle:
        data = handle.read()
    os.unlink(tmp)
    atomic_write_bytes(path, data, mode=IDENTITY_FILE_MODE)
    return path


def load_or_create_identity(path: Path) -> RNS.Identity:
    """Load the identity at *path*, creating it on first use."""
    identity = load_identity(path)
    if identity is not None:
        return identity
    identity = RNS.Identity()
    save_identity(identity, path)
    log.info("Created a new identity at %s", path)
    return identity


def destination_hash(identity: RNS.Identity) -> bytes:
    """Return the tau destination hash for an identity."""
    return RNS.Destination.hash(identity, APP_NAME, ASPECT)


def public_key_hex(identity: RNS.Identity) -> str:
    return identity.get_public_key().hex()
