"""Persistent peer bookkeeping for tau."""

from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from .protocol import PROTOCOL_VERSION, sanitize_name, short_hash
from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

DESTINATION_HASH_BYTES = 16
PUBLIC_KEY_BYTES = 64
REGISTRY_VERSION = 1
MAX_PEERS = 256
DEFAULT_MAX_AGE = 7 * 24 * 60 * 60

_HEX32 = re.compile(r"^[0-9a-fA-F]{32}$")


class PeerError(RuntimeError):
    """Base class for peer lookup problems."""


class AmbiguousPeer(PeerError):
    """Raised when a name or hash prefix matches more than one peer."""


@dataclass
class Peer:
    """A single tau node discovered on the network."""

    destination_hash: bytes
    name: str
    public_key: bytes
    first_seen: float
    last_seen: float
    hops: int = 0
    protocol: int = PROTOCOL_VERSION
    caps: Tuple[str, ...] = field(default_factory=tuple)

    @property
    def hexhash(self) -> str:
        return self.destination_hash.hex()

    @property
    def display_name(self) -> str:
        return self.name or short_hash(self.hexhash)

    def age(self, now: Optional[float] = None) -> float:
        return max(0.0, (now if now is not None else time.time()) - self.last_seen)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "destination": self.hexhash,
            "name": self.name,
            "public_key": self.public_key.hex(),
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "hops": self.hops,
            "protocol": self.protocol,
            "caps": list(self.caps),
        }

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "Peer":
        destination = raw.get("destination", "")
        public_key = raw.get("public_key", "")
        if not isinstance(destination, str) or not _HEX32.match(destination):
            raise ValueError("invalid destination hash")
        destination_bytes = bytes.fromhex(destination)
        if len(destination_bytes) != DESTINATION_HASH_BYTES:
            raise ValueError("invalid destination hash length")
        try:
            public_key_bytes = bytes.fromhex(public_key)
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid public key") from exc
        if len(public_key_bytes) != PUBLIC_KEY_BYTES:
            raise ValueError("invalid public key length")

        last_seen = raw.get("last_seen")
        if not isinstance(last_seen, (int, float)):
            raise ValueError("invalid last_seen timestamp")
        first_seen = raw.get("first_seen")
        if not isinstance(first_seen, (int, float)):
            first_seen = last_seen
        hops = raw.get("hops")
        if not isinstance(hops, int) or hops < 0:
            hops = 0
        protocol = raw.get("protocol")
        if not isinstance(protocol, int):
            protocol = 0
        caps = raw.get("caps")
        if not isinstance(caps, list):
            caps = []
        return cls(
            destination_hash=destination_bytes,
            name=sanitize_name(raw.get("name"), fallback="unknown"),
            public_key=public_key_bytes,
            first_seen=float(first_seen),
            last_seen=float(last_seen),
            hops=hops,
            protocol=protocol,
            caps=tuple(cap for cap in caps if isinstance(cap, str))[:16],
        )

    def public_dict(self, now: Optional[float] = None) -> Dict[str, Any]:
        """A peer description that is safe to send to other peers."""
        return {
            "destination": self.hexhash,
            "name": self.name,
            "last_seen": self.last_seen,
            "age": round(self.age(now), 1),
            "hops": self.hops,
            "protocol": self.protocol,
        }


class PeerRegistry:
    """Thread-safe collection of known peers, optionally backed by JSON."""

    def __init__(
        self,
        path: Optional[Union[str, Path]] = None,
        max_peers: int = MAX_PEERS,
    ) -> None:
        self.path = Path(path).expanduser() if path is not None else None
        self.max_peers = max(1, int(max_peers))
        self._peers: Dict[bytes, Peer] = {}
        self._lock = threading.RLock()
        if self.path is not None:
            self.load()

    def __len__(self) -> int:
        with self._lock:
            return len(self._peers)

    def __contains__(self, key: object) -> bool:
        return self.get(key) is not None

    def get(self, key: Union[bytes, str]) -> Optional[Peer]:
        with self._lock:
            if isinstance(key, bytes):
                return self._peers.get(key)
            if not isinstance(key, str):
                return None
            value = key.strip()
            if _HEX32.match(value):
                return self._peers.get(bytes.fromhex(value))
            for peer in self._peers.values():
                if peer.name.casefold() == value.casefold():
                    return peer
            return None

    def resolve(self, target: Union[bytes, str]) -> Optional[Peer]:
        """Resolve a destination hash, hash prefix or peer name."""
        if isinstance(target, bytes):
            if len(target) != DESTINATION_HASH_BYTES:
                raise PeerError("destination hash must be 16 bytes")
            return self.get(target)
        if not isinstance(target, str):
            raise PeerError("peer target must be a string or bytes")

        value = target.strip()
        if not value:
            return None
        if _HEX32.match(value):
            return self.get(value)

        with self._lock:
            peers = list(self._peers.values())

        by_name = [p for p in peers if p.name.casefold() == value.casefold()]
        if len(by_name) == 1:
            return by_name[0]
        if len(by_name) > 1:
            raise AmbiguousPeer(
                f"{len(by_name)} peers are named {value!r}; use a destination hash"
            )

        if len(value) >= 4:
            prefix = value.casefold()
            by_prefix = [p for p in peers if p.hexhash.startswith(prefix)]
            if len(by_prefix) == 1:
                return by_prefix[0]
            if len(by_prefix) > 1:
                raise AmbiguousPeer(
                    f"{len(by_prefix)} peers match the hash prefix {value!r}"
                )
        return None

    def upsert(
        self,
        destination_hash: bytes,
        public_key: bytes,
        *,
        name: str = "unknown",
        hops: int = 0,
        protocol: int = PROTOCOL_VERSION,
        caps: Iterable[str] = (),
        now: Optional[float] = None,
    ) -> Peer:
        """Insert or update a peer, returning the stored record."""
        if len(destination_hash) != DESTINATION_HASH_BYTES:
            raise ValueError("destination hash must be 16 bytes")
        if len(public_key) != PUBLIC_KEY_BYTES:
            raise ValueError("public key must be 64 bytes")

        timestamp = now if now is not None else time.time()
        clean_name = sanitize_name(name, fallback="unknown")
        caps_tuple = tuple(cap for cap in caps if isinstance(cap, str))[:16]

        with self._lock:
            existing = self._peers.get(destination_hash)
            if existing is None:
                peer = Peer(
                    destination_hash=destination_hash,
                    name=clean_name,
                    public_key=public_key,
                    first_seen=timestamp,
                    last_seen=timestamp,
                    hops=max(0, int(hops)),
                    protocol=int(protocol),
                    caps=caps_tuple,
                )
                self._peers[destination_hash] = peer
            else:
                if clean_name != "unknown" or existing.name == "unknown":
                    existing.name = clean_name
                existing.public_key = public_key
                existing.last_seen = max(existing.last_seen, timestamp)
                existing.hops = max(0, int(hops))
                existing.protocol = int(protocol) or existing.protocol
                if caps_tuple:
                    existing.caps = caps_tuple
                peer = existing
            self._enforce_limit()
            return peer

    def all(
        self,
        *,
        max_age: Optional[float] = None,
        now: Optional[float] = None,
    ) -> List[Peer]:
        timestamp = now if now is not None else time.time()
        with self._lock:
            peers = list(self._peers.values())
        if max_age is not None:
            peers = [p for p in peers if timestamp - p.last_seen <= max_age]
        peers.sort(key=lambda p: p.last_seen, reverse=True)
        return peers

    def prune(
        self,
        max_age: float = DEFAULT_MAX_AGE,
        now: Optional[float] = None,
        *,
        keep: Iterable[bytes] = (),
    ) -> int:
        """Drop peers not seen for longer than *max_age*.

        Peers whose destination hash is in *keep* are spared, so a caller
        can hold on to a stale peer it still owes something (for example
        queued mail).  Returns the number of peers dropped.
        """
        timestamp = now if now is not None else time.time()
        keep_set = set(keep)
        with self._lock:
            stale = [
                key
                for key, peer in self._peers.items()
                if timestamp - peer.last_seen > max_age and key not in keep_set
            ]
            for key in stale:
                del self._peers[key]
        return len(stale)

    def clear(self) -> None:
        with self._lock:
            self._peers.clear()

    def _enforce_limit(self) -> None:
        if len(self._peers) <= self.max_peers:
            return
        oldest = sorted(self._peers.values(), key=lambda p: p.last_seen)
        for peer in oldest[: len(self._peers) - self.max_peers]:
            del self._peers[peer.destination_hash]

    def load(self) -> None:
        if self.path is None:
            return
        raw = read_json(self.path)
        if raw is None:
            return
        if isinstance(raw, dict):
            entries = raw.get("peers", [])
        elif isinstance(raw, list):  # tolerate an older hand-written format
            entries = raw
        else:
            log.warning("Ignoring peer registry at %s: unexpected format", self.path)
            return
        if not isinstance(entries, list):
            log.warning("Ignoring peer registry at %s: invalid peers list", self.path)
            return

        loaded: Dict[bytes, Peer] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                peer = Peer.from_dict(entry)
            except (TypeError, ValueError) as exc:
                log.debug("Skipping invalid peer entry: %s", exc)
                continue
            loaded[peer.destination_hash] = peer

        with self._lock:
            self._peers = loaded
            self._enforce_limit()
        log.debug("Loaded %d peers from %s", len(loaded), self.path)

    def save(self) -> bool:
        if self.path is None:
            return False
        with self._lock:
            payload = {
                "version": REGISTRY_VERSION,
                "saved_at": time.time(),
                "peers": [peer.to_dict() for peer in self._peers.values()],
            }
        atomic_write_json(self.path, payload, mode=0o600)
        return True

    def public_peers(
        self,
        *,
        max_age: Optional[float] = DEFAULT_MAX_AGE,
        now: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        timestamp = now if now is not None else time.time()
        return [
            p.public_dict(timestamp) for p in self.all(max_age=max_age, now=timestamp)
        ]
