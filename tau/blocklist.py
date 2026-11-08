"""Owner-controlled block list (deny-list) for direct messages and Tan.

The complement to the DM allow-list: a node running open still needs a
way to silence one abusive or mistaken peer.  A blocked sender's direct
messages are dropped — never stored, never marked seen, never passed
to ``on_message`` — and the sender gets no signal that they are
blocked (see :func:`tau.node.TauNode._handle_message`).  Blocked
authors' Tan posts stay in the chain store (chain integrity for other
peers is unaffected) but are hidden from the local timeline and feed
views, and blocked senders' history is hidden from ``tau messages``
without being deleted, so unblocking restores the view.

The list lives next to the allow-list, persisted as JSON in the data
directory (``blocklist.json``), with the same fail-closed loading
care: a missing file means nobody is blocked; a file that cannot be
parsed is treated as blocking *everyone*, so a corrupt list can never
silently admit a sender the owner meant to silence.  The same
fail-closed logic applies to senders who never identify on the link:
with an active list, an unidentified sender is blocked, because they
cannot prove they are not the peer the owner silenced; an empty list
keeps DMs open, as before.  Blocking an
allow-listed peer is allowed and wins — the block check runs before
the allow-list check.

Identity here means the Reticulum destination hash (32 hex
characters), which is safe to filter on because the request handler
receives the caller's verified identity — a sender cannot spoof
another node's hash.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, FrozenSet, Iterable, Optional, Union

from .allowlist import AllowListError, clean_destination
from .util import atomic_write_json

log = logging.getLogger(__name__)

BLOCKLIST_FILE_MODE = 0o600


class BlockListError(ValueError):
    """Raised when a block-list file or value is malformed."""


def _clean_destinations(values: Any) -> FrozenSet[str]:
    # A present ``null`` is malformed, not an empty list: treating it as
    # empty would switch enforcement off instead of failing closed.
    if not isinstance(values, (list, tuple, set, frozenset)):
        raise BlockListError("blocked must be a list of destination hashes")
    try:
        return frozenset(clean_destination(value) for value in values)
    except AllowListError as exc:
        raise BlockListError(str(exc)) from exc


@dataclass(frozen=True)
class BlockList:
    """Which destination hashes are blocked, if any.

    ``block_all`` is an in-memory fail-closed sentinel set only by
    :func:`load_blocklist_or_block_all` when the file on disk cannot
    be trusted; it is never persisted.
    """

    blocked: FrozenSet[str] = field(default_factory=frozenset)
    block_all: bool = False

    def is_blocked(self, destination: Optional[str]) -> bool:
        """True when *destination* is silenced by this list.

        An unidentified sender (``None``) cannot prove they are not
        blocked, so with an active list — any entries, or ``block_all``
        — they are blocked too; an empty list keeps DMs open, as
        before.  Honest tau nodes always identify on the link, so
        this only bites a client that deliberately skips identifying
        to dodge the list.
        """
        if self.block_all:
            return True
        if destination is None:
            return bool(self.blocked)
        try:
            return clean_destination(destination) in self.blocked
        except AllowListError:
            return False

    def with_added(self, destination: str) -> "BlockList":
        return BlockList(
            blocked=self.blocked | {clean_destination(destination)},
            block_all=self.block_all,
        )

    def with_removed(self, destination: str) -> "BlockList":
        return BlockList(
            blocked=self.blocked - {clean_destination(destination)},
            block_all=self.block_all,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {"blocked": sorted(self.blocked)}

    @classmethod
    def from_dict(cls, data: Any) -> "BlockList":
        if not isinstance(data, dict):
            raise BlockListError("block-list must be a JSON object")
        if "blocked" not in data:
            # A list that lost its blocked key must not silently turn
            # enforcement off; the fail-closed loader treats the file
            # as corrupt instead.
            raise BlockListError("blocked is required")
        return cls(blocked=_clean_destinations(data.get("blocked")))


def load_blocklist(path: Optional[Union[str, Path]]) -> BlockList:
    """Load the block-list at *path*; a missing file means nobody blocked."""
    if path is None:
        return BlockList()
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return BlockList()
    except OSError as exc:
        # The file is there but cannot be inspected (permissions,
        # a broken path component): fail closed rather than treat
        # it as missing and silently admit everyone.
        raise BlockListError(f"could not read block-list {target}: {exc}") from exc
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise BlockListError(f"could not read block-list {target}: {exc}") from exc
    return BlockList.from_dict(data)


def save_blocklist(path: Union[str, Path], blocklist: BlockList) -> Path:
    """Persist *blocklist* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    return atomic_write_json(target, blocklist.to_dict(), mode=BLOCKLIST_FILE_MODE)


def load_blocklist_or_block_all(
    path: Optional[Union[str, Path]],
) -> BlockList:
    """Load the block-list, failing closed to block-all on a bad file.

    Used by the running node: a list the owner cannot trust must not
    silently admit a sender the owner meant to silence.
    """
    try:
        return load_blocklist(path)
    except BlockListError as exc:
        log.warning(
            "Block-list at %s is unusable (%s); blocking all senders",
            path,
            exc,
        )
        return BlockList(block_all=True)


def load_blocked_set(path: Optional[Union[str, Path]]) -> Optional[FrozenSet[str]]:
    """Load the blocked hashes at *path* for display filtering.

    Returns ``None`` when the list cannot be read or parsed: display
    callers must then hide everything (fail closed), matching the
    node, which blocks all senders on the same condition — a corrupt
    file must not unhide content the owner had hidden.
    """
    try:
        return load_blocklist(path).blocked
    except BlockListError as exc:
        log.warning("Block-list at %s is unusable (%s); hiding everything", path, exc)
        return None
