"""Owner-controlled allow-list for incoming direct messages.

By default a tau node answers direct messages from any peer that can
reach it (Reticulum ``ALLOW_ALL``).  This module lets the node's owner
restrict that: a small allow-list, persisted as JSON in the data
directory (``allowlist.json``), that is consulted for every incoming
message before it is stored or delivered to a callback.

The list is disabled by default, preserving the original open
behaviour.  When enabled, only senders whose destination hash is on
the list get through; everyone else receives a ``not_allowed`` error
and their message is dropped — never stored, never marked seen, and
never passed to ``on_message``.

A missing file means the list is disabled.  A file that cannot be
parsed fails closed: the list is treated as enabled but empty, so no
remote sender is admitted, mirroring the Tan admission ruleset.
Identity here means the Reticulum destination hash (32 hex
characters), which is safe to filter on because the request handler
receives the caller's verified identity — a sender cannot spoof
another node's hash.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, FrozenSet, Iterable, Optional, Union

from .util import atomic_write_json

DESTINATION_HEX_LENGTH = 32
ALLOWLIST_FILE_MODE = 0o600


class AllowListError(ValueError):
    """Raised when an allow-list file or value is malformed."""


def clean_destination(value: Any) -> str:
    """Normalise *value* to a lowercase 32-hex destination hash."""
    if not isinstance(value, str):
        raise AllowListError("destination must be a hex string")
    text = value.strip().lower()
    if len(text) != DESTINATION_HEX_LENGTH:
        raise AllowListError(
            f"destination must be {DESTINATION_HEX_LENGTH} hex characters: {value!r}"
        )
    try:
        bytes.fromhex(text)
    except ValueError as exc:
        raise AllowListError(f"destination is not valid hex: {value!r}") from exc
    return text


def _clean_destinations(values: Any) -> FrozenSet[str]:
    if values is None:
        return frozenset()
    if not isinstance(values, (list, tuple, set, frozenset)):
        raise AllowListError("allow must be a list of destination hashes")
    return frozenset(clean_destination(value) for value in values)


@dataclass(frozen=True)
class AllowList:
    """Whether DMs are restricted, and to which destination hashes."""

    enabled: bool = False
    allow: FrozenSet[str] = field(default_factory=frozenset)

    def is_allowed(self, destination: Optional[str]) -> bool:
        """True when *destination* may send this node a direct message."""
        if not self.enabled:
            return True
        if destination is None:
            return False
        try:
            return clean_destination(destination) in self.allow
        except AllowListError:
            return False

    def with_added(self, destination: str) -> "AllowList":
        return AllowList(enabled=self.enabled, allow=self.allow | {clean_destination(destination)})

    def with_removed(self, destination: str) -> "AllowList":
        return AllowList(
            enabled=self.enabled, allow=self.allow - {clean_destination(destination)}
        )

    def with_enabled(self, enabled: bool) -> "AllowList":
        return AllowList(enabled=bool(enabled), allow=self.allow)

    def to_dict(self) -> Dict[str, Any]:
        return {"enabled": self.enabled, "allow": sorted(self.allow)}

    @classmethod
    def from_dict(cls, data: Any) -> "AllowList":
        if not isinstance(data, dict):
            raise AllowListError("allow-list must be a JSON object")
        if "enabled" not in data:
            raise AllowListError("allow-list is missing the enabled field")
        enabled = data["enabled"]
        if not isinstance(enabled, bool):
            raise AllowListError("enabled must be a boolean")
        return cls(enabled=enabled, allow=_clean_destinations(data.get("allow")))


def load_allowlist(path: Optional[Union[str, Path]]) -> AllowList:
    """Load the allow-list at *path*; a missing file means disabled."""
    if path is None:
        return AllowList()
    target = Path(path).expanduser()
    if not target.exists():
        return AllowList()
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise AllowListError(f"could not read allow-list {target}: {exc}") from exc
    return AllowList.from_dict(data)


def save_allowlist(path: Union[str, Path], allowlist: AllowList) -> Path:
    """Persist *allowlist* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    return atomic_write_json(target, allowlist.to_dict(), mode=ALLOWLIST_FILE_MODE)


def load_allowlist_or_deny_all(
    path: Optional[Union[str, Path]],
) -> AllowList:
    """Load the allow-list, failing closed to deny-all on a bad file.

    Used by the running node: a list the owner cannot trust must not
    silently admit everyone.
    """
    try:
        return load_allowlist(path)
    except AllowListError:
        return AllowList(enabled=True, allow=frozenset())
