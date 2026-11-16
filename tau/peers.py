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
MAX_NOTE_LENGTH = 500
DEFAULT_MAX_AGE = 7 * 24 * 60 * 60

_HEX32 = re.compile(r"^[0-9a-fA-F]{32}$")
# Hex strings that resolve() may read as a hash or hash prefix (>= 4
# chars): a 32-char target is read as a full hash before aliases, and a
# 4-or-more-char target as a prefix after them, so such an alias could
# never reliably select its owner and could misdirect a hash target.
_HEX_LIKE = re.compile(r"^[0-9a-fA-F]{4,}$")


def _clean_stored_alias(value: Any) -> Optional[str]:
    """Normalise a stored alias, dropping values that are unusable.

    Non-strings and blank strings become ``None``; a hash-like alias
    (one that would be read as a destination hash or prefix) is also
    dropped, so an alias saved by an older version or edited by hand
    cannot misdirect a hash target.
    """
    alias = sanitize_name(value, fallback="") or None
    if alias is not None and _HEX_LIKE.match(alias):
        return None
    return alias


def _clean_stored_note(value: Any) -> Optional[str]:
    """Normalise a stored peer note, dropping values that are unusable.

    Non-strings and blank strings become ``None``; a note longer than
    :data:`MAX_NOTE_LENGTH` is truncated, so a note saved by an older
    version or edited by hand cannot grow the registry without bound.
    """
    if not isinstance(value, str):
        return None
    note = value.strip()
    if not note:
        return None
    return note[:MAX_NOTE_LENGTH]


def _dedupe_aliases(peers: Iterable[Peer]) -> None:
    """Drop duplicate aliases in place, keeping the first occurrence.

    Aliases must be unique across the registry for ``resolve`` to pick
    exactly one peer.  A file written by two racing writers can hold
    the same alias twice; dropping the later ones keeps resolution
    unambiguous and lets the next save heal the file.
    """
    seen: set = set()
    for peer in peers:
        if peer.alias is None:
            continue
        key = peer.alias.casefold()
        if key in seen:
            peer.alias = None
        else:
            seen.add(key)


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


def _is_saved_contact(peer: "Peer") -> bool:
    """True when the owner has verified *peer* or saved it as a contact.

    Such records are kept by age pruning and capacity eviction, so the
    lookalike warning they anchor does not silently disappear.
    """
    return peer.is_verified or bool(peer.alias or peer.note)


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
    # Local-only label for this peer, set with `tau peers alias`.  Never
    # sent in peer exchange or announces, and never overwritten by them.
    alias: Optional[str] = None
    # Private note about this peer, set with `tau peers note`.  Like the
    # alias, it is owner-only context ("met at ATO, runs the roof node")
    # that never leaves this node: it stays out of peer exchange and
    # announces, and re-announces never overwrite it.
    note: Optional[str] = None
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
    # Last time a *verified* peer changed its announced name, and the
    # name it carried before the change.  A verified peer renaming
    # itself is the one impersonation-adjacent event tau can see
    # locally, so the rename is recorded and warned about until the
    # owner re-verifies the peer under its new name.
    name_change_at: Optional[float] = None
    previous_name: Optional[str] = None
    # Outbound delivery history: how many messages this node has
    # successfully delivered directly to this peer, and when the last
    # one landed.  A peer with a delivered history is a proven peer —
    # the higher held-mail quota tier requires it.
    delivered_count: int = 0
    last_delivered_at: Optional[float] = None

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
        if self.alias:
            return self.alias
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
            "alias": self.alias,
            "note": self.note,
            "lxmf": self.lxmf_hash,
            "delivered_count": self.delivered_count,
            "last_delivered_at": self.last_delivered_at,
        }
        if self.verified_at is not None:
            payload["verified_at"] = self.verified_at
        if self.verified_fingerprint is not None:
            payload["verified_fingerprint"] = self.verified_fingerprint
        if self.key_change_at is not None:
            payload["key_change_at"] = self.key_change_at
        if self.name_change_at is not None:
            payload["name_change_at"] = self.name_change_at
        if self.previous_name is not None:
            payload["previous_name"] = self.previous_name
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
        if not isinstance(last_seen, (int, float)) or not math.isfinite(last_seen):
            raise ValueError("invalid last_seen timestamp")
        first_seen = raw.get("first_seen")
        if not isinstance(first_seen, (int, float)) or not math.isfinite(first_seen):
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
        delivered_count = raw.get("delivered_count")
        if not isinstance(delivered_count, int) or delivered_count < 0:
            delivered_count = 0
        last_delivered_at = raw.get("last_delivered_at")
        if not isinstance(last_delivered_at, (int, float)):
            last_delivered_at = None
        previous_name = raw.get("previous_name")
        if not isinstance(previous_name, str):
            previous_name = None
        else:
            previous_name = sanitize_name(previous_name, fallback="") or None
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
            alias=_clean_stored_alias(raw.get("alias")),
            note=_clean_stored_note(raw.get("note")),
            verified_at=cls._optional_timestamp(raw, "verified_at"),
            verified_fingerprint=verified_fingerprint,
            key_change_at=cls._optional_timestamp(raw, "key_change_at"),
            name_change_at=cls._optional_timestamp(raw, "name_change_at"),
            previous_name=previous_name,
            delivered_count=delivered_count,
            last_delivered_at=(
                float(last_delivered_at)
                if last_delivered_at is not None
                else None
            ),
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

    def local_dict(
        self, now: Optional[float] = None, *, include_note: bool = False
    ) -> Dict[str, Any]:
        """``public_dict`` plus this node's own trust state about the peer.

        Verification is a local judgement, never shared with other peers:
        it stays out of ``public_dict`` (and therefore out of exchange
        payloads) and is only shown to this node's owner — the CLI, the
        web console, and the persisted registry.  The local alias is the
        same kind of owner-only label.

        The private note is owner-only too, and stricter: the console
        can be exposed remotely (``--http-allow-remote``) without owner
        authentication, so it is included only when *include_note* is
        set — the web bridge asks for it only for loopback clients.
        The CLI reads notes from to_dict() instead.
        """
        payload = self.public_dict(now)
        payload["alias"] = self.alias
        if include_note:
            payload["note"] = self.note
        payload["verified"] = self.is_verified
        payload["verified_at"] = self.verified_at
        payload["key_change_at"] = self.key_change_at
        payload["name_change_at"] = self.name_change_at
        payload["previous_name"] = self.previous_name
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
                if peer.alias and peer.alias.casefold() == value.casefold():
                    return peer
            for peer in self._peers.values():
                if peer.name.casefold() == value.casefold():
                    return peer
            return None

    def resolve(
        self, target: Union[bytes, str], *, include_alias: bool = True
    ) -> Optional[Peer]:
        """Resolve a destination hash, hash prefix, peer name or alias.

        Pass ``include_alias=False`` for security boundaries such as the
        allow-list, where a local label must never stand in for the
        peer's asserted name.
        """
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

        if include_alias:
            by_alias = [
                p
                for p in peers
                if p.alias and p.alias.casefold() == value.casefold()
            ]
            if len(by_alias) == 1:
                return by_alias[0]
            if len(by_alias) > 1:
                raise AmbiguousPeer(
                    f"{len(by_alias)} peers share the alias {value!r}; "
                    "use a destination hash"
                )

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

    def set_alias(self, target: Union[bytes, str], alias: str) -> Peer:
        """Give a known peer a local alias, stored only in this registry.

        The alias is unique across the registry (case-insensitive) so it
        always resolves to exactly one peer.  Raises :class:`PeerError`
        for an unknown peer, an empty alias, or an alias another peer
        already uses.  The alias survives re-announces: :meth:`upsert`
        never touches it.
        """
        clean = sanitize_name(alias, fallback="")
        if not clean:
            raise PeerError("alias must not be empty")
        # A hex-like alias could never reliably select its owner (full
        # hashes resolve before aliases, prefixes after them), so reject
        # it up front instead of letting it misdirect a hash target.
        if _HEX_LIKE.match(clean):
            raise PeerError(
                f"alias {clean!r} looks like a destination hash or hash "
                "prefix; pick a label with a non-hex character"
            )
        with self._lock:
            peer = self.resolve(target)
            if peer is None:
                raise PeerError(f"unknown peer {target!r}")
            clash = next(
                (
                    other
                    for other in self._peers.values()
                    if other.destination_hash != peer.destination_hash
                    and other.alias
                    and other.alias.casefold() == clean.casefold()
                ),
                None,
            )
            if clash is not None:
                raise PeerError(
                    f"alias {clean!r} is already used by "
                    f"{clash.hexhash[:8]} ({clash.display_name})"
                )
            peer.alias = clean
        self.save()
        return peer

    def clear_alias(self, target: Union[bytes, str]) -> Peer:
        """Remove a peer's local alias.

        Raises :class:`PeerError` when the peer is unknown or has no
        alias set.
        """
        with self._lock:
            peer = self.resolve(target)
            if peer is None:
                raise PeerError(f"unknown peer {target!r}")
            if not peer.alias:
                raise PeerError(f"peer {peer.display_name} has no alias")
            peer.alias = None
        self.save()
        return peer

    def set_note(self, target: Union[bytes, str], note: str) -> Peer:
        """Save a private note on a known peer, stored only in this registry.

        The note is trimmed and capped at :data:`MAX_NOTE_LENGTH`; an
        empty or overlong note raises :class:`PeerError` (use
        :meth:`clear_note` to remove one).  Raises :class:`PeerError`
        for an unknown peer.  The note survives re-announces:
        :meth:`upsert` never touches it.
        """
        if not isinstance(note, str):
            raise PeerError("note must be a string")
        clean = note.strip()
        if not clean:
            raise PeerError("note must not be empty")
        if len(clean) > MAX_NOTE_LENGTH:
            raise PeerError(
                f"note must be at most {MAX_NOTE_LENGTH} characters "
                f"({len(clean)} given)"
            )
        with self._lock:
            peer = self.resolve(target)
            if peer is None:
                raise PeerError(f"unknown peer {target!r}")
            peer.note = clean
        self.save()
        return peer

    def clear_note(self, target: Union[bytes, str]) -> Peer:
        """Remove a peer's private note.

        Raises :class:`PeerError` when the peer is unknown or has no
        note set.
        """
        with self._lock:
            peer = self.resolve(target)
            if peer is None:
                raise PeerError(f"unknown peer {target!r}")
            if not peer.note:
                raise PeerError(f"peer {peer.display_name} has no note")
            peer.note = None
        self.save()
        return peer

    def refresh_aliases(self) -> int:
        """Pick up alias and note changes another process made to the registry file.

        A running node holds its registry in memory and rewrites the
        whole file on every save, so an alias or note set by a one-shot command
        (``tau peers alias``, ``tau peers note``) while the node serves would be lost at the
        next save.  Only aliases and notes are refreshed — peer records themselves
        are this process's observations — and a peer absent from the file
        keeps its alias and note.  A disk alias that another in-memory peer
        already holds is not applied: applying it would leave two peers
        sharing one alias and make resolution ambiguous.  Returns the
        number of peers whose alias or note changed.
        """
        if self.path is None:
            return 0
        raw = read_json(self.path)
        entries = raw.get("peers", []) if isinstance(raw, dict) else []
        if not isinstance(entries, list):
            return 0
        changed = 0
        with self._lock:
            claimed: Dict[str, bytes] = {}
            for peer in self._peers.values():
                if peer.alias is not None:
                    claimed.setdefault(
                        peer.alias.casefold(), peer.destination_hash
                    )
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                if "alias" not in entry and "note" not in entry:
                    continue
                destination = entry.get("destination", "")
                if not isinstance(destination, str) or not _HEX32.match(
                    destination
                ):
                    continue
                peer = self._peers.get(bytes.fromhex(destination))
                if peer is None:
                    continue
                peer_changed = False
                if "alias" in entry:
                    alias = _clean_stored_alias(entry.get("alias"))
                    if alias is not None:
                        owner = claimed.get(alias.casefold())
                        if owner is not None and owner != peer.destination_hash:
                            # Another peer already holds this alias (for
                            # example one missing from this file kept it);
                            # applying it here would make the alias ambiguous.
                            # The note below can still be applied.
                            alias = peer.alias
                    if peer.alias != alias:
                        if peer.alias is not None:
                            claimed.pop(peer.alias.casefold(), None)
                        peer.alias = alias
                        if alias is not None:
                            claimed[alias.casefold()] = peer.destination_hash
                        peer_changed = True
                if "note" in entry:
                    note = _clean_stored_note(entry.get("note"))
                    if peer.note != note:
                        peer.note = note
                        peer_changed = True
                if peer_changed:
                    changed += 1
        return changed

    def lookalike_for(
        self, peer: Peer, *, include_notes: bool = True
    ) -> Optional[Peer]:
        """Return the verified peer or saved contact *peer* may impersonate.

        An unverified peer is a lookalike when its *announced* name
        matches (case-insensitively) the display name — local alias or
        announced name — of another peer the owner has verified out of
        band or saved as a contact (a peer with an alias or a private
        note).  The candidate's own alias is never compared: it is the
        owner's label, not something the peer announced.  A bare
        ``"unknown"`` never collides — it is the fallback for peers
        that never announced a name.  Returns ``None`` for verified
        peers and when nothing collides.  The result is local-only: it
        is never sent to other peers.

        With *include_notes* false, a peer whose only saved-contact
        context is a private note does not count: callers that show
        results to a non-loopback console must not reveal which peers
        carry a note.
        """
        if peer.is_verified:
            return None
        claimed = peer.name.casefold()
        if not claimed or claimed == "unknown":
            return None
        with self._lock:
            for other in self._peers.values():
                if other.destination_hash == peer.destination_hash:
                    continue
                contact = other.alias or (include_notes and other.note)
                if not (other.is_verified or contact):
                    continue
                names = {
                    name.casefold()
                    for name in (other.name, other.alias)
                    if name and name.casefold() != "unknown"
                }
                if claimed in names:
                    return other
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
            is_new = existing is None
            name_changed = False
            first_direct = False
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
                    if (
                        existing.is_verified
                        and clean_name != "unknown"
                        and clean_name != existing.name
                        and existing.name != "unknown"
                    ):
                        if clean_name == existing.previous_name:
                            # Renamed back to the name it was verified
                            # under: the rename warning is answered.
                            existing.name_change_at = None
                            existing.previous_name = None
                        else:
                            log.warning(
                                "Verified peer %s (%s) changed its "
                                "announced name from %r to %r; re-verify "
                                "out of band before trusting the new name",
                                existing.display_name,
                                existing.hexhash,
                                existing.name,
                                clean_name,
                            )
                            # Keep the first post-verification baseline:
                            # an intermediate name must not replace the
                            # name the owner verified under.
                            if existing.previous_name is None:
                                existing.previous_name = existing.name
                            existing.name_change_at = timestamp
                    if clean_name != existing.name:
                        name_changed = True
                    existing.name = clean_name
                existing.public_key = public_key
                # Heard directly (e.g. via announce): this is first-hand now.
                # A first direct announce of an exchange-learned peer is
                # logged too, since it is the first chance to warn.
                first_direct = existing.via_exchange
                existing.via_exchange = False
                existing.last_seen = max(existing.last_seen, timestamp)
                existing.hops = max(0, int(hops))
                existing.protocol = int(protocol) or existing.protocol
                if caps_tuple:
                    existing.caps = caps_tuple
                peer = existing
            self._enforce_limit()
            if (is_new or name_changed or first_direct) and not peer.is_verified:
                collided = self.lookalike_for(peer)
                if collided is not None:
                    log.warning(
                        "Unverified peer %s (%s) announces with the same "
                        "name as %s (%s) — a possible lookalike; it is "
                        "not verified, treat it as a stranger",
                        peer.display_name,
                        peer.hexhash,
                        collided.display_name,
                        collided.hexhash,
                    )
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
            # The owner just checked this key out of band: any earlier
            # key-change warning about it is answered, not left to warn
            # on every subsequent view.
            peer.key_change_at = None
            # Same for a rename warning: verifying under the current
            # name accepts it as the peer's name going forward.
            peer.name_change_at = None
            peer.previous_name = None
            return peer

    def clear_verifications(self) -> int:
        """Drop every peer's verification state, returning how many cleared.

        Pairwise safety numbers are computed with the local key, so a
        comparison made under a previous local identity says nothing
        about the current one — importing a different identity must
        invalidate them all rather than leave stale checkmarks behind.
        """
        with self._lock:
            cleared = 0
            for peer in self._peers.values():
                if peer.is_verified:
                    peer.verified_at = None
                    peer.verified_fingerprint = None
                    # A rename warning is about a *verified* peer; once the
                    # verification is gone it must not keep describing one.
                    peer.name_change_at = None
                    peer.previous_name = None
                    cleared += 1
                elif peer.name_change_at is not None:
                    peer.name_change_at = None
                    peer.previous_name = None
            return cleared

    def forget(self, target: Union[bytes, str]) -> Peer:
        """Remove a peer from the registry, returning the removed record.

        The explicit owner-controlled recovery path for a record that
        cannot repair itself — for example a stored key corrupted on
        disk, which ``upsert`` rightly refuses to overwrite: forget the
        peer and the next genuine announce re-adds a clean record.
        Raises :class:`PeerError` for an unknown or ambiguous target.
        """
        peer = self.resolve(target)
        if peer is None:
            raise PeerError(f"no peer matches {target!r}")
        with self._lock:
            del self._peers[peer.destination_hash]
            return peer

    def restore(self, peer: Peer) -> None:
        """Put back a record removed by :meth:`forget` after a failed save.

        Only inserts when the destination is absent, so a record that was
        re-announced in the meantime is never overwritten by the stale copy.
        """
        with self._lock:
            self._peers.setdefault(peer.destination_hash, peer)

    def record_delivered(
        self, destination_hash: bytes, *, now: Optional[float] = None
    ) -> bool:
        """Record a successful direct delivery to a peer.

        Bumps the peer's delivered count and stamps the time, so the
        registry carries the delivered history the held-mail quota tier
        (and any future trust decision) needs.  Returns ``False`` when
        the peer is not in the registry.  The caller owns persistence:
        the serving node saves peers on its usual cadence and on stop.
        """
        timestamp = now if now is not None else time.time()
        with self._lock:
            peer = self._peers.get(destination_hash)
            if peer is None:
                return False
            peer.delivered_count += 1
            peer.last_delivered_at = timestamp
            return True

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
                and not _is_saved_contact(peer)
            ]
            for key in stale:
                del self._peers[key]
        return len(stale)

    def remove(self, destination_hash: bytes) -> Optional[Peer]:
        """Drop one peer from the registry. Returns the removed peer, or None."""
        with self._lock:
            return self._peers.pop(destination_hash, None)

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
        # Verified peers and saved contacts are keepers too, for the same
        # reason as in prune().
        candidates = sorted(
            (
                p
                for p in self._peers.values()
                if p.destination_hash not in keep_set and not _is_saved_contact(p)
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

        _dedupe_aliases(loaded.values())

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
