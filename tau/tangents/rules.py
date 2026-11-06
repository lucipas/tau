"""Owner-controlled admission rules for Tan chain entries.

By default a Tan node stores any well-signed entry a peer pushes to it.
This module lets the node's owner decide what gets committed to their
local database instead: a small ruleset, persisted as JSON in the data
directory (``tan-rules.json``), that is consulted for every *remote*
entry before it is stored.

A ruleset is evaluated in a fixed order:

1. The node's own author is always admitted (its entries never arrive
   as remote commits anyway; this guards gossip echoes).
2. Authors on the block list are denied.
3. Authors on the allow list, and — when ``allow_followed`` is on —
   authors the node follows, are admitted.
4. Everything else falls to the default policy: ``allow`` or ``deny``.

Admission deliberately never filters by entry *kind*.  Tan entries
form a hash chain, so refusing to store one kind (say, a ``like``)
would leave that author's later admitted entries pointing at a
missing parent: they would sit as unlinked orphans, the chain head
would never advance, and sync would stall at the gap.  The ``kinds``
setting is therefore display-only: every kind from an admitted author
is stored so chains stay complete, and ``displays()`` tells the
node's timeline views which kinds to present.

Identity here means the Tan author hash, which is safe to filter on
because admission runs only after the entry's signature has been
verified against the public key that derives that hash — an author
cannot be spoofed.  Entries the local user creates are never filtered;
the ruleset governs remote commits only.  With no rules file, the
default ruleset admits everything, matching Tan's original behaviour.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Set, Union

AUTHOR_HEX_LENGTH = 32
ENTRY_KINDS = ("post", "like", "unlike", "delete")


class RulesError(ValueError):
    """Raised when a ruleset file or value is malformed."""


def _clean_author(value: Any) -> str:
    if not isinstance(value, str):
        raise RulesError("author must be a hex string")
    text = value.strip().lower()
    if len(text) != AUTHOR_HEX_LENGTH:
        raise RulesError(
            f"author must be {AUTHOR_HEX_LENGTH} hex characters: {value!r}"
        )
    try:
        bytes.fromhex(text)
    except ValueError as exc:
        raise RulesError(f"author is not valid hex: {value!r}") from exc
    return text


def _clean_authors(values: Any, field_name: str) -> frozenset:
    if values is None:
        return frozenset()
    if not isinstance(values, (list, tuple, set, frozenset)):
        raise RulesError(f"{field_name} must be a list of author hashes")
    return frozenset(_clean_author(value) for value in values)


def _clean_kinds(values: Any) -> Optional[frozenset]:
    if values is None:
        return None
    if not isinstance(values, (list, tuple, set, frozenset)):
        raise RulesError("kinds must be a list of entry kinds")
    kinds = frozenset(str(value).strip().lower() for value in values)
    unknown = kinds - set(ENTRY_KINDS)
    if unknown:
        raise RulesError(f"unknown entry kinds: {', '.join(sorted(unknown))}")
    return kinds


@dataclass(frozen=True)
class Ruleset:
    """An immutable Tan admission ruleset."""

    default_allow: bool = True
    allow_authors: frozenset = field(default_factory=frozenset)
    block_authors: frozenset = field(default_factory=frozenset)
    allow_followed: bool = False
    kinds: Optional[frozenset] = None

    def decide(
        self,
        author: str,
        *,
        own_author: Optional[str] = None,
        followed: Iterable[str] = (),
    ) -> bool:
        """Return True when entries from *author* are admitted.

        Admission is per-author only: entry kind is never a reason to
        refuse storage (see the module docstring — a refused kind
        would punch a hole in the author's hash chain).
        """
        author = author.strip().lower()
        if own_author is not None and author == own_author.strip().lower():
            return True
        if author in self.block_authors:
            return False
        if author in self.allow_authors:
            return True
        if self.allow_followed and author in {a.strip().lower() for a in followed}:
            return True
        return self.default_allow

    def displays(self, kind: str) -> bool:
        """Return True when timeline views should present *kind* entries.

        Display-only counterpart to :meth:`decide`: hidden kinds are
        still stored, they just do not appear in the node's views.
        """
        return self.kinds is None or kind.strip().lower() in self.kinds

    def to_dict(self) -> Dict[str, Any]:
        return {
            "default": "allow" if self.default_allow else "deny",
            "allow_authors": sorted(self.allow_authors),
            "block_authors": sorted(self.block_authors),
            "allow_followed": self.allow_followed,
            "kinds": sorted(self.kinds) if self.kinds is not None else None,
        }

    @classmethod
    def from_dict(cls, data: Any) -> "Ruleset":
        if not isinstance(data, dict):
            raise RulesError("ruleset must be a JSON object")
        default = data.get("default", "allow")
        if default not in ("allow", "deny"):
            raise RulesError(f"default must be 'allow' or 'deny': {default!r}")
        allow_followed = data.get("allow_followed", False)
        if not isinstance(allow_followed, bool):
            raise RulesError("allow_followed must be a boolean")
        return cls(
            default_allow=default == "allow",
            allow_authors=_clean_authors(
                data.get("allow_authors"), "allow_authors"
            ),
            block_authors=_clean_authors(
                data.get("block_authors"), "block_authors"
            ),
            allow_followed=allow_followed,
            kinds=_clean_kinds(data.get("kinds")),
        )


def load_ruleset(path: Optional[Union[str, Path]]) -> Ruleset:
    """Load the ruleset at *path*; a missing file means admit everything."""
    if path is None:
        return Ruleset()
    target = Path(path).expanduser()
    if not target.exists():
        return Ruleset()
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RulesError(f"could not read ruleset {target}: {exc}") from exc
    return Ruleset.from_dict(data)


def save_ruleset(path: Union[str, Path], ruleset: Ruleset) -> Path:
    """Persist *ruleset* to *path* as JSON and return the path."""
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(ruleset.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return target
