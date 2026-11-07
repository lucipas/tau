"""Persistent peer bookkeeping for tau."""

from __future__ import annotations

import hashlib
import logging
import math
import re
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import RNS

from .identity import load_public_key
from .lxmf import lxmf_delivery_hash_for_key
from .protocol import APP_NAME, ASPECT, PROTOCOL_VERSION, sanitize_name, short_hash
from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

DESTINATION_HASH_BYTES = 16
PUBLIC_KEY_BYTES = 64
REGISTRY_VERSION = 1
MAX_PEERS = 256
DEFAULT_MAX_AGE = 7 * 24 * 60 * 60

_HEX32 = re.compile(r"^[0-9a-fA-F]{32}$")


def safety_number(*public_keys: bytes) -> str:
    """A human-comparable safety number for one or more public keys.

    The construction follows the Signal pattern: the keys are sorted so
    the number is the same from either side of a pair, hashed together,
    and rendered as 12 groups of 5 decimal digits (60 digits).  With a
    single key it is that identity's fingerprint; with two keys it is
    the pairwise safety number the two owners compare over another
    channel to confirm neither side is talking to an impostor.
    """
    if not public_keys:
        raise ValueError("at least one public key is required")
    for key in public_keys:
        if len(key) != PUBLIC_KEY_BYTES:
            raise ValueError("public key must be 64 bytes")
    digest = hashlib.sha512(b"".join(sorted(public_keys))).digest()
    groups = [
        f"{int.from_bytes(digest[5 * i : 5 * i + 5], 'big') % 100000:05d}"
        for i in range(12)
    ]
    return " ".join(groups)


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
    # True while the peer was learned only from another peer's exchange and
    # never heard directly.  Its reported liveness is second-hand, so mail
    # sweeps rank directly heard peers ahead of it.
    via_exchange: bool = False
    # When the owner compared safety numbers with this peer out of band
    # and confirmed the key is theirs (``tau peers verify``).  The
    # fingerprint is the pairwise safety number recorded at that moment.
    verified_at: Optional[float] = None
    verified_fingerprint: Optional[str] = None
    # Last time this destination presented a different public key than the
    # one recorded.  A destination hash is derived from its key, so a
    # change here means tampering or corruption, never a re-key — the
    # registry keeps the recorded key and the node warns instead.
    key_change_at: Optional[float] = None

    @property
    def hexhash(self) -> str:
        return self.destination_hash.hex()

    @property
    def is_verified(self) -> bool:
        return self.verified_at is not None

    @property
    def lxmf_hash(self) -> Optional[str]:
        """This peer's LXMF delivery address, derived from its public key.

        LXMF addresses a node at a destination derived from its identity,
        so the address follows from the advertised key with no extra
        announcement.  ``None`` when the stored key cannot be loaded
        (a peer built from untrusted data); real peers always have one.
        """
        try:
            return lxmf_delivery_hash_for_key(self.public_key).hex()
        except ValueError:
            return None

    @property
    def display_name(self) -> str:
        return self.name or short_hash(self.hexhash)

    def age(self, now: Optional[float] = None) -> float:
        return max(0.0, (now if now is not None else time.time()) - self.last_seen)

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "destination": self.hexhash,
            "name": self.name,
            "public_key": self.public_key.hex(),
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "hops": self.hops,
            "protocol": self.protocol,
            "caps": list(self.caps),
            "via_exchange": self.via_exchange,
            "lxmf": self.lxmf_hash,
        }
        if self.verified_at is not None:
            payload["verified_at"] = self.verified_at
        if self.verified_fingerprint is not None:
            payload["verified_fingerprint"] = self.verified_fingerprint
        if self.key_change_at is not None:
            payload["key_change_at"] = self.key_change_at
        return payload

    @staticmethod
    def _optional_timestamp(raw: Dict[str, Any], key: str) -> Optional[float]:
        value = raw.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        try:
            value = float(value)
        except (OverflowError, ValueError):
            return None
        return value if math.isfinite(value) else None

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
        verified_fingerprint = raw.get("verified_fingerprint")
        if not isinstance(verified_fingerprint, str):
            verified_fingerprint = None
        return cls(
            destination_hash=destination_bytes,
            name=sanitize_name(raw.get("name"), fallback="unknown"),
            public_key=public_key_bytes,
            first_seen=float(first_seen),
            last_seen=float(last_seen),
            hops=hops,
            protocol=protocol,
            caps=tuple(cap for cap in caps if isinstance(cap, str))[:16],
            via_exchange=raw.get("via_exchange") is True,
            verified_at=cls._optional_timestamp(raw, "verified_at"),
            verified_fingerprint=verified_fingerprint,
            key_change_at=cls._optional_timestamp(raw, "key_change_at"),
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
            "lxmf": self.lxmf_hash,
        }

    def local_dict(self, now: Optional[float] = None) -> Dict[str, Any]:
        """``public_dict`` plus this node's own trust state about the peer.

        Verification is a local judgement, never shared with other peers:
        it stays out of ``public_dict`` (and therefore out of exchange
        payloads) and is only shown to this node's owner — the CLI, the
        web console, and the persisted registry.
        """
        payload = self.public_dict(now)
        payload["verified"] = self.is_verified
        payload["verified_at"] = self.verified_at
        payload["key_change_at"] = self.key_change_at
        return payload

    def exchange_dict(self, now: Optional[float] = None) -> Dict[str, Any]:
        """A peer description for peer exchange: ``public_dict`` plus the key.

        The public key is public by design — it is broadcast in every
        Reticulum announce — and the destination hash is derived from it,
        so the receiver can verify a reported key really belongs to the
        reported destination before trusting it.  The console's
        ``public_dict`` stays keyless; only the exchange payload carries keys.
        """
        payload = self.public_dict(now)
        payload["public_key"] = self.public_key.hex()
        return payload


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
                if existing.public_key != public_key:
                    # A destination hash is derived from its public key, so
                    # a different key under the same destination cannot be
                    # a re-key — it is tampering, a corrupt registry, or a
                    # bug.  Never silently accept it: keep the recorded
                    # key (and the verification that hangs off it), stamp
                    # the attempt so the owner sees it, and leave the
                    # record otherwise untouched.
                    log.warning(
                        "Peer %s (%s) presented a different public key "
                        "under the same destination hash; keeping the "
                        "recorded key and refusing the update — re-verify "
                        "this peer out of band before trusting it",
                        existing.display_name,
                        existing.hexhash,
                    )
                    existing.key_change_at = timestamp
                    return existing
                if clean_name != "unknown" or existing.name == "unknown":
                    existing.name = clean_name
                existing.public_key = public_key
                # Heard directly (e.g. via announce): this is first-hand now.
                existing.via_exchange = False
                existing.last_seen = max(existing.last_seen, timestamp)
                existing.hops = max(0, int(hops))
                existing.protocol = int(protocol) or existing.protocol
                if caps_tuple:
                    existing.caps = caps_tuple
                peer = existing
            self._enforce_limit()
            return peer

    def mark_verified(
        self,
        target: Union[bytes, str],
        fingerprint: str,
        *,
        now: Optional[float] = None,
    ) -> Peer:
        """Record that the owner compared safety numbers with *target*.

        The *fingerprint* is the pairwise safety number shown at verify
        time; storing it lets a later verification notice if the local
        identity (and therefore the number) changed since.  The caller
        persists the registry.  Raises :class:`PeerError` for an unknown
        or ambiguous target.
        """
        peer = self.resolve(target)
        if peer is None:
            raise PeerError(f"no peer matches {target!r}")
        timestamp = now if now is not None else time.time()
        with self._lock:
            peer.verified_fingerprint = fingerprint
            peer.verified_at = timestamp
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
        queued mail).  Verified peers are always spared: the owner has
        confirmed that key out of band, and losing the record would
        silently drop the verification.  Returns the number of peers
        dropped.
        """
        timestamp = now if now is not None else time.time()
        keep_set = set(keep)
        with self._lock:
            stale = [
                key
                for key, peer in self._peers.items()
                if timestamp - peer.last_seen > max_age
                and key not in keep_set
                and not peer.is_verified
            ]
            for key in stale:
                del self._peers[key]
        return len(stale)

    def clear(self) -> None:
        with self._lock:
            self._peers.clear()

    def _enforce_limit(self, keep: Iterable[bytes] = ()) -> None:
        if len(self._peers) <= self.max_peers:
            return
        keep_set = set(keep)
        excess = len(self._peers) - self.max_peers
        # Peers in *keep* (e.g. recipients of queued mail) are never evicted;
        # the registry may briefly exceed the cap rather than lose them.
        # Verified peers are keepers too, for the same reason as in prune().
        candidates = sorted(
            (
                p
                for p in self._peers.values()
                if p.destination_hash not in keep_set and not p.is_verified
            ),
            key=lambda p: p.last_seen,
        )
        for peer in candidates[:excess]:
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
        include_keys: bool = False,
    ) -> List[Dict[str, Any]]:
        timestamp = now if now is not None else time.time()
        describe = (
            (lambda p: p.exchange_dict(timestamp))
            if include_keys
            else (lambda p: p.public_dict(timestamp))
        )
        return [describe(p) for p in self.all(max_age=max_age, now=timestamp)]

    def merge_exchanged(
        self,
        entries: Any,
        *,
        now: Optional[float] = None,
        self_hash: Optional[Union[bytes, str]] = None,
        keep: Iterable[bytes] = (),
    ) -> Dict[str, int]:
        """Merge peer descriptions learned from another peer.

        Every entry is validated before it is trusted: the destination must
        be a 32-hex hash, the public key must be 64 bytes, and the key must
        derive the destination hash — a reporter cannot plant a bogus key
        for a real peer.  Entries for ourselves, malformed entries, entries
        with non-finite or boolean timestamps, and entries older than the
        prune horizon are skipped.

        A new peer is recorded with ``first_seen`` now and the reported
        ``last_seen`` (clamped to now), so pruning still works honestly, and
        marked ``via_exchange`` until it is heard directly.  A peer we
        already know keeps its own record: only a missing name is filled in
        — a third party's claim never refreshes our liveness record.  A
        reported name that collides with another known peer's name is
        dropped, so a reporter cannot shadow a peer's name and misdirect
        name-based resolution.  Peers in *keep* (e.g. recipients of queued
        mail) are never evicted by the capacity limit.  Returns
        ``{"added": n, "updated": n, "skipped": n}``.
        """
        timestamp = now if now is not None else time.time()
        own: Optional[bytes] = None
        if isinstance(self_hash, bytes) and len(self_hash) == DESTINATION_HASH_BYTES:
            own = self_hash
        elif isinstance(self_hash, str):
            try:
                candidate = bytes.fromhex(self_hash.strip())
            except ValueError:
                candidate = b""
            if len(candidate) == DESTINATION_HASH_BYTES:
                own = candidate

        added = updated = skipped = 0
        added_hashes: set = set()
        if not isinstance(entries, list):
            return {"added": 0, "updated": 0, "skipped": 0}

        with self._lock:
            for entry in entries:
                if not isinstance(entry, dict):
                    skipped += 1
                    continue
                try:
                    destination = entry.get("destination", "")
                    if not isinstance(destination, str) or not _HEX32.match(destination):
                        raise ValueError("invalid destination hash")
                    destination_bytes = bytes.fromhex(destination)
                    public_key = entry.get("public_key", "")
                    if not isinstance(public_key, str):
                        raise ValueError("invalid public key")
                    public_key_bytes = bytes.fromhex(public_key)
                    if len(public_key_bytes) != PUBLIC_KEY_BYTES:
                        raise ValueError("invalid public key length")
                    identity = RNS.Identity(create_keys=False)
                    if not load_public_key(identity, public_key_bytes):
                        raise ValueError("invalid public key")
                    if (
                        RNS.Destination.hash(identity, APP_NAME, ASPECT)
                        != destination_bytes
                    ):
                        raise ValueError("public key does not match destination")
                except (TypeError, ValueError):
                    skipped += 1
                    continue
                if own is not None and destination_bytes == own:
                    skipped += 1
                    continue
                last_seen = entry.get("last_seen")
                # Booleans are ints in Python, and JSON accepts NaN/Infinity:
                # all three would otherwise smuggle an unprunable timestamp
                # into the registry (a NaN last_seen never ages out).
                if isinstance(last_seen, bool) or not isinstance(
                    last_seen, (int, float)
                ):
                    skipped += 1
                    continue
                try:
                    last_seen = float(last_seen)
                except (OverflowError, ValueError):
                    skipped += 1
                    continue
                if not math.isfinite(last_seen):
                    skipped += 1
                    continue
                last_seen = min(last_seen, timestamp)
                if timestamp - last_seen > DEFAULT_MAX_AGE:
                    skipped += 1
                    continue
                hops = entry.get("hops")
                # One more hop than the reporting peer's own distance.
                hops = (
                    hops + 1
                    if isinstance(hops, int)
                    and not isinstance(hops, bool)
                    and hops >= 0
                    else 1
                )
                protocol = entry.get("protocol")
                protocol = (
                    protocol
                    if isinstance(protocol, int)
                    and not isinstance(protocol, bool)
                    else 0
                )
                name = sanitize_name(entry.get("name"), fallback="unknown")
                if name != "unknown" and any(
                    p.name.casefold() == name.casefold()
                    for p in self._peers.values()
                    if p.destination_hash != destination_bytes
                ):
                    # A reporter must not shadow a known peer's name.
                    name = "unknown"

                existing = self._peers.get(destination_bytes)
                if existing is None:
                    self._peers[destination_bytes] = Peer(
                        destination_hash=destination_bytes,
                        name=name,
                        public_key=public_key_bytes,
                        first_seen=timestamp,
                        last_seen=last_seen,
                        hops=hops,
                        protocol=protocol,
                        via_exchange=True,
                    )
                    added_hashes.add(destination_bytes)
                    added += 1
                elif existing.name == "unknown" and name != "unknown":
                    existing.name = name
                    updated += 1
            # Newly added peers are protected from eviction in this same
            # merge: when everything already stored is protected, the
            # registry briefly exceeds the cap instead of dropping the peer
            # it just learned (pruning and later upserts bring it back).
            self._enforce_limit(keep=set(keep) | added_hashes)
        return {"added": added, "updated": updated, "skipped": skipped}
