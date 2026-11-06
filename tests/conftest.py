from __future__ import annotations

import sys
from pathlib import Path

import pytest
import RNS

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture()
def load_public_key_without_return(monkeypatch):
    """Simulate RNS 1.4.1, whose load_public_key returns None on success."""
    real_load = RNS.Identity.load_public_key

    def load_without_return(self, pub_bytes):
        real_load(self, pub_bytes)
        return None

    monkeypatch.setattr(RNS.Identity, "load_public_key", load_without_return)
