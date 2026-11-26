"""Live location beacons.

A beacon shares the sender's position with one conversation — a DM
peer, a group, or a broadcast list — as a series of location
messages, one every few minutes, until it expires.  The static pin
(``tau send --location``) is one place at one moment; a beacon is
the same pin shape with a ``beacon`` marker (see
:func:`tau.protocol.clean_beacon`), so each update renders as a pin
and the receiver can tell a live update from a one-off pin and show
the beacon's expiry.

The position is resolved at each tick, not frozen at start: a beacon
may name a position file (JSON ``{"lat", "lon", "name"?}``) that an
external GPS writer refreshes — on a phone, a ``termux-location``
loop — and the tick re-reads it, so the updates follow the sender.
Without a file, the start position repeats; ``tau beacon update``
refreshes it by hand.  ``--coarse`` rounds every update to ~1 km
and drops the place name, since a precise name would leak the
precision the rounding removes.

Beacons live in ``beacons.json`` next to the other per-node state,
persisted as JSON (mode 0600) with the same load/save/lock pattern
as the scheduled-posts queue.  Updates go out from the serving
node's mail loop (``tau serve``): the loop re-reads the file each
round, so a ``tau beacon`` command in another process is picked up
without a restart, and a beacon started while no node serves waits
for one — exactly like a scheduled post.  A peer that cannot be
reached queues the update in the outbox like any other message, so
beacons are delay-tolerant by construction.
"""

from __future__ import annotations

import json
import logging
import math
import os
import secrets
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Union

from .protocol import clean_beacon, clean_location, sanitize_group_name
from .util import atomic_write_json, ensure_dir

log = logging.getLogger(__name__)

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore[assignment]

try:
    import msvcrt
except ImportError:  # pragma: no cover - non-Windows
    msvcrt = None  # type: ignore[assignment]

BEACONS_FILE_MODE = 0o600

# A beacon updates every 5 minutes and expires after 1 hour unless
# the starter says otherwise; an 8-hour cap keeps a forgotten beacon
# from pinging a conversation all day, and a 1-minute floor keeps a
# mis-typed interval from hammering the mesh.
BEACON_UPDATE_INTERVAL = 300.0
BEACON_DEFAULT_DURATION = 3600.0
BEACON_MAX_DURATION = 8 * 3600.0
BEACON_MIN_INTERVAL = 60.0
BEACON_MAX_INTERVAL = 3600.0
# Coarse precision rounds to 0.01 degrees — about 1.1 km — the same
# order as the "~1 km" the issue asks for.
BEACON_COARSE_STEP = 0.01


class BeaconsError(ValueError):
    """Raised when a beacons file or value is malformed."""


def new_beacon_id() -> str:
    """A fresh beacon id: 8 random bytes, hex."""
    return secrets.token_hex(8)


def coarsen_location(location: Dict[str, Any]) -> Dict[str, Any]:
    """Round *location* to coarse precision (~1 km) and drop its name.

    The name goes because a precise place name ("my house") would
    leak the precision the rounding removes.
    """
    cleaned = clean_location(location)
    if cleaned is None:
        raise BeaconsError(
            "location must have a latitude -90..90 and a longitude -180..180"
        )
    # Floor in hundredths, not by dividing by the step: 34.05 / 0.01
    # is 3404.999... in binary floating point, and flooring that
    # would blur the pin a whole step further south-west.  The
    # hundredth is rounded first for the same reason: -83.88 * 100 is
    # -8388.000...1, whose floor is a step too far west.
    return {
        "lat": round(math.floor(round(cleaned["lat"] * 100, 9)) / 100, 2),
        "lon": round(math.floor(round(cleaned["lon"] * 100, 9)) / 100, 2),
    }


def read_location_file(path: Union[str, Path]) -> Optional[Dict[str, Any]]:
    """Read a position file: JSON ``{"lat", "lon", "name"?}``.

    Returns ``None`` when the file is missing or unusable, with a
    warning — a tick that cannot read the file keeps the beacon's
    last position instead of failing the whole round.
    """
    target = Path(path).expanduser()
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except FileNotFoundError:
        log.warning("Beacon position file %s is missing", target)
        return None
    except (OSError, ValueError) as exc:
        log.warning("Beacon position file %s is unreadable: %s", target, exc)
        return None
    cleaned = clean_location(data)
    if cleaned is None:
        log.warning("Beacon position file %s has no valid position", target)
        return None
    return cleaned


def _clean_target(value: Any, *, kind: str, target_kind: str) -> str:
    """Clean a beacon target for its kind.

    A DM or group target is a 32-hex destination hash or group id,
    as before.  A broadcast target is a list name — the list itself
    is resolved (and its members read) at each tick, so the stored
    target is the name, cleaned the same way broadcast lists clean
    theirs.
    """
    if target_kind == "broadcast":
        # sanitize_group_name falls back to "group" for a blank name,
        # so the blank check runs first: a blank list name is a bad
        # target, not the list literally called "group".
        if not isinstance(value, str) or not value.strip():
            raise BeaconsError(
                "beacon broadcast target must be a non-empty list name"
            )
        return sanitize_group_name(value)
    if not isinstance(value, str):
        raise BeaconsError(f"beacon {kind} target must be a string")
    cleaned = value.strip().lower()
    if len(cleaned) != 32:
        raise BeaconsError(f"beacon {kind} target must be 32 hex characters")
    try:
        bytes.fromhex(cleaned)
    except ValueError as exc:
        raise BeaconsError(
            f"beacon {kind} target must be 32 hex characters"
        ) from exc
    return cleaned


def _clean_target_kind(value: Any) -> str:
    if value not in ("dm", "group", "broadcast"):
        raise BeaconsError(
            'beacon target kind must be "dm", "group" or "broadcast"'
        )
    return str(value)


def _clean_timestamp(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BeaconsError(f"beacon {label} must be a number")
    try:
        cleaned = float(value)
    except (OverflowError, ValueError) as exc:
        raise BeaconsError(f"beacon {label} must be a finite number") from exc
    if not math.isfinite(cleaned):
        raise BeaconsError(f"beacon {label} must be a finite number")
    return cleaned


def _clean_interval(value: Any) -> float:
    cleaned = _clean_timestamp(value, "interval")
    if not BEACON_MIN_INTERVAL <= cleaned <= BEACON_MAX_INTERVAL:
        raise BeaconsError(
            "beacon interval must be between "
            f"{int(BEACON_MIN_INTERVAL)}s and {int(BEACON_MAX_INTERVAL)}s"
        )
    return cleaned


@dataclass
class Beacon:
    """One live beacon: a conversation plus its update schedule."""

    id: str
    target: str
    target_kind: str
    target_name: str = ""
    # A broadcast beacon's list, pinned by stable id: the stored
    # name can change (rename) and be reused (delete then recreate),
    # so the tick resolves the id, never the name alone.  Empty for
    # DM/group beacons — and for a beacon stored before lists
    # carried ids, which keeps resolving by name as before.
    target_id: str = ""
    started_at: float = 0.0
    expires_at: float = 0.0
    interval: float = BEACON_UPDATE_INTERVAL
    coarse: bool = False
    location: Dict[str, Any] = field(default_factory=dict)
    location_file: Optional[str] = None
    next_update_at: float = 0.0
    updates_sent: int = 0
    stopped: bool = False

    def is_active(self, now: Optional[float] = None) -> bool:
        """True while the beacon is still sharing: not stopped and unexpired."""
        now = time.time() if now is None else now
        return not self.stopped and now < self.expires_at

    def is_due(self, now: Optional[float] = None) -> bool:
        """True when an active beacon owes an update."""
        now = time.time() if now is None else now
        return self.is_active(now) and now >= self.next_update_at

    def wire_marker(self, seq: int) -> Dict[str, Any]:
        """The ``beacon`` field one update carries on the wire."""
        return {"id": self.id, "expires_at": self.expires_at, "seq": seq}

    def current_location(self) -> Dict[str, Any]:
        """The position the next update carries.

        A position file is re-read on every tick, so an external GPS
        writer moves the beacon; a failed read keeps the last
        position.  Coarse beacons round here, so the stored position
        stays precise and only the wire copy is blurred.
        """
        location = dict(self.location)
        if self.location_file is not None:
            fresh = read_location_file(self.location_file)
            if fresh is not None:
                location = fresh
        cleaned = clean_location(location)
        if cleaned is None:
            raise BeaconsError("beacon has no valid position to send")
        if self.coarse:
            return coarsen_location(cleaned)
        return cleaned

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "id": self.id,
            "target": self.target,
            "target_kind": self.target_kind,
            "target_name": self.target_name,
            "target_id": self.target_id,
            "started_at": self.started_at,
            "expires_at": self.expires_at,
            "interval": self.interval,
            "coarse": self.coarse,
            "location": dict(self.location),
            "next_update_at": self.next_update_at,
            "updates_sent": self.updates_sent,
            "stopped": self.stopped,
        }
        if self.location_file is not None:
            payload["location_file"] = self.location_file
        return payload

    @classmethod
    def from_dict(cls, data: Any) -> "Beacon":
        if not isinstance(data, dict):
            raise BeaconsError("beacon must be a JSON object")
        beacon_id = data.get("id")
        if not isinstance(beacon_id, str) or not beacon_id:
            raise BeaconsError("beacon id must be a non-empty string")
        location = clean_location(data.get("location"))
        if location is None:
            raise BeaconsError("beacon location must be a valid pin")
        location_file = data.get("location_file")
        if location_file is not None and not isinstance(location_file, str):
            raise BeaconsError("beacon location_file must be a string")
        updates_sent = data.get("updates_sent", 0)
        if (
            isinstance(updates_sent, bool)
            or not isinstance(updates_sent, int)
            or updates_sent < 0
        ):
            raise BeaconsError("beacon updates_sent must be a non-negative integer")
        stopped = data.get("stopped", False)
        if not isinstance(stopped, bool):
            raise BeaconsError("beacon stopped must be a boolean")
        return cls(
            id=beacon_id,
            target=_clean_target(
                data.get("target"),
                kind="beacon",
                target_kind=_clean_target_kind(data.get("target_kind")),
            ),
            target_kind=_clean_target_kind(data.get("target_kind")),
            target_name=data.get("target_name")
            if isinstance(data.get("target_name"), str)
            else "",
            target_id=data.get("target_id")
            if isinstance(data.get("target_id"), str)
            else "",
            started_at=_clean_timestamp(data.get("started_at"), "started_at"),
            expires_at=_clean_timestamp(data.get("expires_at"), "expires_at"),
            interval=_clean_interval(data.get("interval")),
            coarse=bool(data.get("coarse", False)),
            location=location,
            location_file=location_file,
            next_update_at=_clean_timestamp(
                data.get("next_update_at"), "next_update_at"
            ),
            updates_sent=updates_sent,
            stopped=stopped,
        )


@dataclass
class Beacons:
    """The live beacons this node owns, by beacon id."""

    beacons: Dict[str, Beacon] = field(default_factory=dict)

    def active(self, now: Optional[float] = None) -> list:
        """Active beacons, oldest first."""
        now = time.time() if now is None else now
        return sorted(
            (b for b in self.beacons.values() if b.is_active(now)),
            key=lambda b: b.started_at,
        )

    def by_target(
        self, target: str, target_kind: str, now: Optional[float] = None
    ) -> Optional[Beacon]:
        """The active beacon for a conversation, if one is running.

        Broadcast targets compare case-insensitively, like the list
        store itself: "Family" and "family" are the same list, so a
        beacon started for one must be found — and refused as a
        duplicate — under the other spelling.
        """
        now = time.time() if now is None else now
        for beacon in self.beacons.values():
            if beacon.target_kind != target_kind:
                continue
            if target_kind == "broadcast":
                same = (
                    isinstance(beacon.target, str)
                    and isinstance(target, str)
                    and beacon.target.casefold() == target.casefold()
                )
            else:
                same = beacon.target == target
            if same and beacon.is_active(now):
                return beacon
        return None

    def get(self, beacon_id: str) -> Optional[Beacon]:
        """A beacon by id, active or not."""
        return self.beacons.get(beacon_id)

    def stop_by_id(
        self, beacon_id: str, now: Optional[float] = None
    ) -> Optional[Beacon]:
        """Stop the active beacon with this id, if one is running.

        The console stops by id (it lists beacons by id); the CLI
        stops by conversation (see :meth:`stop`).  A stopped or
        expired beacon is already over, so stopping it again is a
        no-op that returns ``None``.
        """
        now = time.time() if now is None else now
        beacon = self.beacons.get(beacon_id)
        if beacon is None or not beacon.is_active(now):
            return None
        beacon.stopped = True
        return beacon

    def start(
        self,
        *,
        target: str,
        target_kind: str,
        target_name: str = "",
        target_id: str = "",
        location: Dict[str, Any],
        coarse: bool = False,
        duration: float = BEACON_DEFAULT_DURATION,
        interval: float = BEACON_UPDATE_INTERVAL,
        location_file: Optional[str] = None,
        now: Optional[float] = None,
    ) -> Beacon:
        """Start a beacon for a conversation, or raise when one runs.

        The first update goes out at once (the caller sends it); the
        next one is due one interval later, so the mail loop cannot
        double-send it in the same round.
        """
        now = time.time() if now is None else now
        target_kind = _clean_target_kind(target_kind)
        target = _clean_target(target, kind="beacon", target_kind=target_kind)
        if self.by_target(target, target_kind, now) is not None:
            raise BeaconsError(
                "a beacon is already running for this conversation; "
                "stop it first"
            )
        cleaned_location = clean_location(location)
        if cleaned_location is None:
            raise BeaconsError(
                "location must have a latitude -90..90 and a longitude -180..180"
            )
        duration = _clean_timestamp(duration, "duration")
        if not 0 < duration <= BEACON_MAX_DURATION:
            raise BeaconsError(
                "beacon duration must be between 1s and "
                f"{int(BEACON_MAX_DURATION // 3600)}h"
            )
        interval = _clean_interval(interval)
        beacon = Beacon(
            id=new_beacon_id(),
            target=target,
            target_kind=target_kind,
            target_name=target_name,
            target_id=target_id,
            started_at=now,
            expires_at=now + duration,
            interval=interval,
            coarse=bool(coarse),
            location=cleaned_location,
            location_file=location_file,
            next_update_at=now + interval,
            updates_sent=0,
        )
        self.beacons[beacon.id] = beacon
        return beacon

    def stop(
        self, target: str, target_kind: str, now: Optional[float] = None
    ) -> Optional[Beacon]:
        """Stop the active beacon for a conversation, if one runs."""
        now = time.time() if now is None else now
        beacon = self.by_target(target, target_kind, now)
        if beacon is None:
            return None
        beacon.stopped = True
        return beacon

    def update_location(
        self,
        target: str,
        target_kind: str,
        *,
        location: Optional[Dict[str, Any]] = None,
        location_file: Optional[str] = None,
        now: Optional[float] = None,
    ) -> Beacon:
        """Refresh a running beacon's position or position file."""
        now = time.time() if now is None else now
        beacon = self.by_target(target, target_kind, now)
        if beacon is None:
            raise BeaconsError("no beacon is running for this conversation")
        if location is not None:
            cleaned = clean_location(location)
            if cleaned is None:
                raise BeaconsError(
                    "location must have a latitude -90..90 and a longitude "
                    "-180..180"
                )
            beacon.location = cleaned
        if location_file is not None:
            beacon.location_file = location_file
        return beacon

    def to_dict(self) -> Dict[str, Any]:
        return {
            "beacons": [b.to_dict() for b in sorted(
                self.beacons.values(), key=lambda b: b.started_at
            )],
        }

    @classmethod
    def from_dict(cls, data: Any) -> "Beacons":
        if not isinstance(data, dict):
            raise BeaconsError("beacons must be a JSON object")
        entries = data.get("beacons")
        if not isinstance(entries, list):
            raise BeaconsError("beacons must be a JSON array")
        beacons = {}
        for entry in entries:
            beacon = Beacon.from_dict(entry)
            # A corrupt duplicate keeps the newest copy; the store
            # still loads instead of failing the whole file.
            previous = beacons.get(beacon.id)
            if previous is None or beacon.started_at >= previous.started_at:
                beacons[beacon.id] = beacon
        return cls(beacons=beacons)


def load_beacons(path: Optional[Union[str, Path]]) -> Beacons:
    """Load the beacons at *path*; a missing file means none running."""
    if path is None:
        return Beacons()
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return Beacons()
    except OSError as exc:
        raise BeaconsError(f"could not read beacons {target}: {exc}") from exc
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise BeaconsError(f"could not read beacons {target}: {exc}") from exc
    return Beacons.from_dict(data)


def save_beacons(path: Union[str, Path], beacons: Beacons) -> Path:
    """Persist *beacons* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    return atomic_write_json(target, beacons.to_dict(), mode=BEACONS_FILE_MODE)


def repoint_broadcast_beacons(
    path: Optional[Union[str, Path]], old_name: str, new_name: str
) -> int:
    """Point active broadcast beacons at a renamed list.

    A rename keeps the list's stable id, so ticks keep resolving —
    but the beacon's stored target is the old name, and the CLI and
    the console match beacons to lists by that name.  Update the
    stored name (and display name) so ``tau beacon stop --broadcast
    NEW`` and the console's list row still find the running beacon,
    including after a case-only rename.  Returns how many beacons
    were re-pointed; a missing beacons file means none running.
    """
    if path is None:
        return 0
    target = Path(path).expanduser()
    count = 0
    with beacons_lock(target):
        beacons = load_beacons(target)
        now = time.time()
        for beacon in beacons.beacons.values():
            if (
                beacon.target_kind == "broadcast"
                and beacon.is_active(now)
                and isinstance(beacon.target, str)
                and beacon.target.casefold() == old_name.casefold()
            ):
                beacon.target = new_name
                beacon.target_name = new_name
                count += 1
        if count:
            save_beacons(target, beacons)
    return count


def stop_broadcast_beacons(
    path: Optional[Union[str, Path]], name: str, *, list_id: Optional[str] = None
) -> int:
    """Stop the active broadcast beacons for a deleted list.

    The tick would fail on its own once the list is gone, but the
    beacon would stay due until it expires; stopping it here ends
    it at once.  Matches by stored name (case-insensitive) or, when
    given, by the list's stable id.  Returns how many beacons were
    stopped; a missing beacons file means none running.
    """
    if path is None:
        return 0
    target = Path(path).expanduser()
    count = 0
    with beacons_lock(target):
        beacons = load_beacons(target)
        now = time.time()
        for beacon in beacons.beacons.values():
            if beacon.target_kind != "broadcast" or not beacon.is_active(now):
                continue
            same_name = (
                isinstance(beacon.target, str)
                and beacon.target.casefold() == name.casefold()
            )
            same_id = list_id is not None and beacon.target_id == list_id
            if same_name or same_id:
                beacon.stopped = True
                count += 1
        if count:
            save_beacons(target, beacons)
    return count


@contextmanager
def beacons_lock(path: Union[str, Path]) -> Iterator[None]:
    """Serialize read-modify-write updates of the beacons file.

    Same shape as the scheduled-posts lock: a flock on a
    ``.beacons.json.lock`` sidecar.  An atomic file replacement
    alone does not protect the whole update — without the lock, two
    commands racing load the same file and the last save discards
    the other's change.  Callers must reload inside the lock.
    """
    if fcntl is None and msvcrt is None:  # pragma: no cover - neither platform
        yield
        return
    target = Path(path).expanduser()
    # The data directory may not exist yet (a first beacon on a
    # fresh node): the lock sidecar cannot be opened until it does.
    ensure_dir(target.parent)
    lock_path = target.with_name(f".{target.name}.lock")
    with open(lock_path, "a+") as lock:
        if fcntl is not None:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        else:  # pragma: no cover - exercised on Windows
            lock.seek(0, os.SEEK_END)
            if lock.tell() == 0:
                lock.write(" ")
                lock.flush()
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
            else:  # pragma: no cover - exercised on Windows
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)


# Re-exported for callers that validate a wire marker without
# importing the protocol module directly.
__all__ = [
    "BEACON_COARSE_STEP",
    "BEACON_DEFAULT_DURATION",
    "BEACON_MAX_DURATION",
    "BEACON_MAX_INTERVAL",
    "BEACON_MIN_INTERVAL",
    "BEACON_UPDATE_INTERVAL",
    "Beacon",
    "Beacons",
    "BeaconsError",
    "beacons_lock",
    "clean_beacon",
    "coarsen_location",
    "load_beacons",
    "new_beacon_id",
    "read_location_file",
    "repoint_broadcast_beacons",
    "save_beacons",
    "stop_broadcast_beacons",
]
