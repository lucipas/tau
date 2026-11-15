"""Owner-controlled per-conversation disappearing-message defaults.

The standing version of ``--expire``: instead of passing an expiry on
every send, the owner sets a default once per conversation — a DM peer
or a group — and every immediate message sent to that conversation
carries ``expires_at = now + default`` unless the send passes an
explicit ``--expire`` (explicit wins) or is scheduled (a scheduled
send takes no default, so it cannot expire before it is sent).

The defaults live next to the block-list, persisted as JSON in the
data directory (``disappearing.json``), with the same load/save pattern
as the archive list — and the same failure direction, with one
difference in emphasis.  A disappearing default is a hygiene
preference, not a security boundary: a corrupt file fails *open* at
send time (the message goes out without an expiry, with a warning)
rather than breaking every send, while the setters (CLI and console)
fail loudly so a bad file is never silently overwritten.  The peer
sees only the normal per-message ``expires_at``; the default itself
is never sent.

Durations are seconds, 1s..30d (``MAX_MAIL_TTL``, the same cap as
``--expire``): a disappearing message cannot outlive the
store-and-forward envelope that might carry it.
"""

from __future__ import annotations

import json
import logging
import math
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, Mapping, Optional, Union

from .allowlist import AllowListError, clean_destination
from .protocol import MAX_MAIL_TTL
from .util import atomic_write_json

log = logging.getLogger(__name__)

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore[assignment]

DISAPPEARING_FILE_MODE = 0o600

MAX_DISAPPEARING_SECONDS = MAX_MAIL_TTL


class DisappearingError(ValueError):
    """Raised when a disappearing-defaults file or value is malformed."""


def _clean_seconds(value: Any) -> float:
    # A present ``null`` or a non-number is malformed, not "no default":
    # treating it as empty would switch the default off instead of
    # failing loudly.  Zero or negative durations and anything past
    # the mail TTL are rejected the same way ``--expire`` rejects them.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DisappearingError("disappearing default must be a number of seconds")
    seconds = float(value)
    if not math.isfinite(seconds) or seconds <= 0:
        raise DisappearingError("disappearing default must be longer than 0s")
    if seconds > MAX_DISAPPEARING_SECONDS:
        raise DisappearingError(
            "disappearing default must be at most 30d (the mail TTL)"
        )
    return seconds


def _clean_group_id(value: Any) -> str:
    # Group ids are 32 lowercase hex characters (see
    # ``tau.groups._valid_group``); anything else is not a group.
    if not isinstance(value, str):
        raise DisappearingError("group id must be a string")
    cleaned = value.strip().lower()
    if len(cleaned) != 32:
        raise DisappearingError("group id must be 32 hex characters")
    try:
        bytes.fromhex(cleaned)
    except ValueError as exc:
        raise DisappearingError("group id must be 32 hex characters") from exc
    return cleaned


def _clean_defaults(values: Any, *, kind: str) -> Dict[str, float]:
    if not isinstance(values, dict):
        raise DisappearingError(f"{kind} disappearing defaults must be a JSON object")
    cleaned: Dict[str, float] = {}
    for key, seconds in values.items():
        if kind == "peer":
            try:
                cleaned[clean_destination(key)] = _clean_seconds(seconds)
            except AllowListError as exc:
                raise DisappearingError(str(exc)) from exc
        else:
            cleaned[_clean_group_id(key)] = _clean_seconds(seconds)
    return cleaned


@dataclass(frozen=True)
class DisappearingDefaults:
    """Per-conversation disappearing defaults, in seconds, if any."""

    peers: Mapping[str, float] = field(default_factory=dict)
    groups: Mapping[str, float] = field(default_factory=dict)

    def peer_default(self, destination: Optional[str]) -> Optional[float]:
        """Default seconds for a DM peer, or ``None`` when unset."""
        if destination is None:
            return None
        try:
            return self.peers.get(clean_destination(destination))
        except AllowListError:
            return None

    def group_default(self, group_id: Optional[str]) -> Optional[float]:
        """Default seconds for a group, or ``None`` when unset."""
        if group_id is None:
            return None
        try:
            return self.groups.get(_clean_group_id(group_id))
        except DisappearingError:
            return None

    def with_peer(self, destination: str, seconds: float) -> "DisappearingDefaults":
        peers = dict(self.peers)
        peers[clean_destination(destination)] = _clean_seconds(seconds)
        return DisappearingDefaults(peers=peers, groups=self.groups)

    def without_peer(self, destination: str) -> "DisappearingDefaults":
        peers = dict(self.peers)
        peers.pop(clean_destination(destination), None)
        return DisappearingDefaults(peers=peers, groups=self.groups)

    def with_group(self, group_id: str, seconds: float) -> "DisappearingDefaults":
        groups = dict(self.groups)
        groups[_clean_group_id(group_id)] = _clean_seconds(seconds)
        return DisappearingDefaults(peers=self.peers, groups=groups)

    def without_group(self, group_id: str) -> "DisappearingDefaults":
        groups = dict(self.groups)
        groups.pop(_clean_group_id(group_id), None)
        return DisappearingDefaults(peers=self.peers, groups=groups)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "peers": {key: self.peers[key] for key in sorted(self.peers)},
            "groups": {key: self.groups[key] for key in sorted(self.groups)},
        }

    @classmethod
    def from_dict(cls, data: Any) -> "DisappearingDefaults":
        if not isinstance(data, dict):
            raise DisappearingError("disappearing defaults must be a JSON object")
        # A file that lost a section must not silently turn that side's
        # defaults off; the fail-open loader treats the file as corrupt
        # instead.
        if "peers" not in data or "groups" not in data:
            raise DisappearingError("peers and groups are required")
        return cls(
            peers=_clean_defaults(data.get("peers"), kind="peer"),
            groups=_clean_defaults(data.get("groups"), kind="group"),
        )


def load_disappearing_defaults(
    path: Optional[Union[str, Path]],
) -> DisappearingDefaults:
    """Load the disappearing defaults at *path*; a missing file means none set."""
    if path is None:
        return DisappearingDefaults()
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return DisappearingDefaults()
    except OSError as exc:
        # The file is there but cannot be inspected (permissions, a
        # broken path component): fail loudly rather than treat it as
        # missing and silently send permanent messages the owner
        # expected to disappear.
        raise DisappearingError(
            f"could not read disappearing defaults {target}: {exc}"
        ) from exc
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise DisappearingError(
            f"could not read disappearing defaults {target}: {exc}"
        ) from exc
    return DisappearingDefaults.from_dict(data)


def save_disappearing_defaults(
    path: Union[str, Path], defaults: DisappearingDefaults
) -> Path:
    """Persist *defaults* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    return atomic_write_json(target, defaults.to_dict(), mode=DISAPPEARING_FILE_MODE)


@contextmanager
def disappearing_lock(path: Union[str, Path]) -> Iterator[None]:
    """Serialize read-modify-write updates of the disappearing defaults.

    Same shape as the node.json lock in ``tau.config``: a flock on a
    ``.disappearing.json.lock`` sidecar.  An atomic file replacement
    alone does not protect the whole update — without the lock, two
    set commands racing load the same file and the last save discards
    the other's change.  Callers must reload inside the lock.
    Non-POSIX platforms get no lock, like the node lock.
    """
    if fcntl is None:  # pragma: no cover - non-POSIX
        yield
        return
    target = Path(path).expanduser()
    lock_path = target.with_name(f".{target.name}.lock")
    with open(lock_path, "a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def peer_default_for(
    path: Optional[Union[str, Path]], destination: Optional[str]
) -> Optional[float]:
    """Default seconds for a DM peer, failing open on a corrupt file.

    Send paths use this: a corrupt defaults file must never break a
    send — the message goes out without an expiry, and the warning is
    what surfaces the corruption.
    """
    try:
        return load_disappearing_defaults(path).peer_default(destination)
    except DisappearingError as exc:
        log.warning(
            "Disappearing defaults at %s are unusable (%s); sending without a default",
            path,
            exc,
        )
        return None


def group_default_for(
    path: Optional[Union[str, Path]], group_id: Optional[str]
) -> Optional[float]:
    """Default seconds for a group, failing open on a corrupt file."""
    try:
        return load_disappearing_defaults(path).group_default(group_id)
    except DisappearingError as exc:
        log.warning(
            "Disappearing defaults at %s are unusable (%s); sending without a default",
            path,
            exc,
        )
        return None
