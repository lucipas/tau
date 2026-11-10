"""Reticulum identity management."""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from pathlib import Path
from tempfile import mkstemp
from typing import Iterator, Optional

if os.name == "nt":  # pragma: no cover - exercised on Windows
    import msvcrt
else:
    import fcntl

import RNS

from .protocol import APP_NAME, ASPECT
from .util import atomic_write_bytes, ensure_dir

log = logging.getLogger(__name__)

IDENTITY_FILE_MODE = 0o600


class IdentityError(RuntimeError):
    """Raised when an identity cannot be loaded or created."""


def load_identity(path: Path, harden: bool = True) -> Optional[RNS.Identity]:
    """Load an existing identity, returning ``None`` when it does not exist.

    ``harden=False`` skips the owner-only chmod, for read-only callers.
    """
    path = Path(path).expanduser()
    if not path.exists():
        return None
    identity = RNS.Identity.from_file(str(path))
    if identity is None:
        raise IdentityError(f"could not load identity from {path}")
    if harden:
        try:
            os.chmod(path, IDENTITY_FILE_MODE)
        except OSError:  # pragma: no cover - best effort
            pass
    return identity


def save_identity(
    identity: RNS.Identity, path: Path, restrict_parent: bool = True
) -> Path:
    """Persist an identity atomically with owner-only permissions.

    ``restrict_parent=False`` leaves an existing parent directory's
    permissions alone (used for backups written into a caller-selected
    folder); a newly created parent is still private.
    """
    path = Path(path).expanduser()
    ensure_dir(path.parent, restrict_existing=restrict_parent)
    fd, tmp_name = mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    os.close(fd)
    try:
        os.chmod(tmp_name, IDENTITY_FILE_MODE)
        if not identity.to_file(tmp_name):
            raise IdentityError(f"could not write identity to {path}")
        with open(tmp_name, "rb") as handle:
            data = handle.read()
    finally:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
    atomic_write_bytes(path, data, mode=IDENTITY_FILE_MODE, restrict_parent=restrict_parent)
    return path


@contextmanager
def _identity_lock(path: Path) -> Iterator[None]:
    """Hold an inter-process lock for identity load-or-create operations."""
    lock_path = path.with_name(f".{path.name}.lock")
    ensure_dir(path.parent)
    with open(lock_path, "a+") as lock:
        try:
            os.chmod(lock_path, IDENTITY_FILE_MODE)
        except OSError:  # pragma: no cover - best effort
            pass
        if os.name == "nt":  # pragma: no cover - exercised on Windows
            lock.seek(0, os.SEEK_END)
            if lock.tell() == 0:
                lock.write(" ")
                lock.flush()
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
        else:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            if os.name == "nt":  # pragma: no cover - exercised on Windows
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def load_or_create_identity(path: Path) -> RNS.Identity:
    """Load the identity at *path*, creating it on first use."""
    path = Path(path).expanduser()
    with _identity_lock(path):
        identity = load_identity(path)
        if identity is not None:
            return identity
        identity = RNS.Identity()
        save_identity(identity, path)
        persisted = load_identity(path)
        if persisted is None:  # pragma: no cover - save_identity should guarantee this
            raise IdentityError(f"could not load identity from {path}")
    log.info("Created a new identity at %s", path)
    return persisted


def destination_hash(identity: RNS.Identity) -> bytes:
    """Return the tau destination hash for an identity."""
    return RNS.Destination.hash(identity, APP_NAME, ASPECT)


def load_public_key(identity: RNS.Identity, public_key: bytes) -> bool:
    """Load *public_key* into a fresh keyless *identity*.

    RNS 1.4.1's ``Identity.load_public_key`` returns ``None`` even on
    success (the True/False return only arrived in a later release),
    and it also returns ``None`` on failure, so the return value alone
    cannot tell the two apart there. A successful load runs
    ``update_hashes()``, which sets ``identity.hash``; a failed load
    never reaches it, so the hash is the version-tolerant success
    signal (and an explicit ``False`` still means failure on newer
    RNS). Callers pass a fresh keyless identity.
    """
    loaded = identity.load_public_key(public_key)
    return loaded is not False and identity.hash is not None


def public_key_hex(identity: RNS.Identity) -> str:
    return identity.get_public_key().hex()
