from __future__ import annotations

import errno
import os
import sys
from pathlib import Path

import pytest
import RNS

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class FakeMsvcrt:
    """Stand-in for the Windows CRT file-locking calls.

    ``msvcrt`` is importable only on Windows, so the Windows branch of
    ``InstanceLock`` is exercised on Linux through this fake.  Locks are
    keyed by the locked file's identity, the way the CRT keys by file:
    a second lock of the same file from another handle conflicts.
    """

    LK_UNLCK = 0
    LK_LOCK = 1
    LK_NBLCK = 2

    def __init__(self) -> None:
        self._locked = set()

    @staticmethod
    def _key(fd: int):
        st = os.fstat(fd)
        return (st.st_dev, st.st_ino)

    def locking(self, fd: int, mode: int, nbytes: int) -> None:
        key = self._key(fd)
        if mode == self.LK_NBLCK:
            if key in self._locked:
                raise OSError(errno.EDEADLOCK, "Resource deadlock avoided")
            self._locked.add(key)
        elif mode == self.LK_UNLCK:
            self._locked.discard(key)
        else:  # pragma: no cover - only NBLCK/UNLCK are used
            raise AssertionError(f"unexpected locking mode {mode}")


@pytest.fixture()
def windows_lock(monkeypatch):
    """Simulate the Windows branch of ``InstanceLock`` on Linux."""
    import tau.node as node_mod

    fake = FakeMsvcrt()
    monkeypatch.setattr(node_mod, "fcntl", None)
    monkeypatch.setattr(node_mod, "msvcrt", fake)
    return fake


@pytest.fixture()
def load_public_key_without_return(monkeypatch):
    """Simulate RNS 1.4.1, whose load_public_key returns None on success."""
    real_load = RNS.Identity.load_public_key

    def load_without_return(self, pub_bytes):
        real_load(self, pub_bytes)
        return None

    monkeypatch.setattr(RNS.Identity, "load_public_key", load_without_return)
