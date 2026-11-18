"""Durable message-draft store: one unsent message per conversation.

A draft is text parked for a single peer or a single group, saved with
``tau draft save`` and sent later with ``tau draft send``.  Drafts are
local-only by construction — there is no wire format for them, so a
draft never leaves the node and no recipient can learn one exists.

One draft per conversation: saving again overwrites the old text.  The
store is capped at 50 drafts, mirroring the broadcast-list cap: drafts
are local-only, so the cap is really about a runaway script, not a
remote peer.  Draft text goes through the same normalization as a send
(``normalize_message``), so a draft that can be saved can be sent.
"""

from __future__ import annotations

import logging
import math
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

try:  # POSIX only; Windows locks through msvcrt instead (see below).
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None  # type: ignore[assignment]

try:  # Windows only; the lock uses msvcrt there, as identity.py does.
    import msvcrt
except ImportError:  # pragma: no cover - POSIX platforms
    msvcrt = None  # type: ignore[assignment]

from .protocol import ProtocolError, normalize_message
from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

# Local drafts are capped like broadcast lists: a peer cannot make this
# node hold unbounded state through any wire path (drafts are
# local-only, so the cap is really about a runaway script), and
# `tau draft delete` frees capacity when the cap is reached.
MAX_DRAFTS = 50

DRAFT_KINDS = ("peer", "group")


def _valid_hash(value: Any) -> Optional[str]:
    """A 32-hex destination hash (or group id), lowercased, or ``None``."""
    if not isinstance(value, str) or len(value) != 32:
        return None
    try:
        bytes.fromhex(value)
    except ValueError:
        return None
    return value.lower()


def _valid_draft(raw: Any) -> Optional[Dict[str, Any]]:
    """Normalize one stored draft, or ``None`` when it is unusable."""
    if not isinstance(raw, dict):
        return None
    kind = raw.get("kind")
    if kind not in DRAFT_KINDS:
        return None
    target = _valid_hash(raw.get("target"))
    if target is None:
        return None
    try:
        text = normalize_message(raw.get("text"))
    except ProtocolError:
        return None
    updated_at = raw.get("updated_at")
    try:
        updated_at = float(updated_at) if updated_at is not None else time.time()
    except (TypeError, ValueError):
        return None
    if not math.isfinite(updated_at):
        return None
    return {
        "kind": kind,
        "target": target,
        "text": text,
        "updated_at": updated_at,
    }


def _draft_key(kind: str, target: str) -> Tuple[str, str]:
    return (kind, target)


class DraftStore:
    """Unsent messages, one per conversation, persisted in the data dir.

    A node without a data directory (tests, embeds) keeps drafts in
    memory only.  Writes are atomic and the file is mode 0600, like the
    peer registry and mailbox; a failed write only logs a warning.
    """

    def __init__(self, path: Optional[Union[str, Path]] = None) -> None:
        self.path = Path(path).expanduser() if path is not None else None
        self._lock = threading.Lock()
        # Keyed by (kind, target): one draft per conversation.
        self._drafts: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if self.path is None:
            return
        raw = read_json(self.path, default=None)
        if not isinstance(raw, dict):
            return
        entries = raw.get("drafts")
        if not isinstance(entries, list):
            return
        loaded: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for entry in entries:
            draft = _valid_draft(entry)
            if draft is not None:
                loaded[_draft_key(draft["kind"], draft["target"])] = draft
        self._drafts = loaded

    def _save(self) -> None:
        """Persist the store; raises :class:`OSError` when the write fails."""
        if self.path is None:
            return
        atomic_write_json(
            self.path,
            {"drafts": list(self._drafts.values())},
            mode=0o600,
        )

    def _snapshot(self) -> Dict[Tuple[str, str], Dict[str, Any]]:
        """In-memory state before a mutation, for rollback on a failed save."""
        return dict(self._drafts)

    def _restore(self, snapshot: Dict[Tuple[str, str], Dict[str, Any]]) -> None:
        """Restore the state captured by :meth:`_snapshot`."""
        self._drafts = dict(snapshot)

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        """Serialise a read-modify-write of the store across processes.

        Mirrors :meth:`GroupStore._transaction`: each mutation takes an
        exclusive lock on a sidecar lock file and reloads the latest
        on-disk state first, so a serving process and a CLI command
        cannot interleave a load and a save.
        """
        with self._lock:
            if self.path is None:
                yield
                return
            if fcntl is None and msvcrt is None:
                # pragma: no cover - neither platform offers a
                # cross-process lock; best effort only.
                self._load()
                yield
                return
            lock_path = self.path.with_name(self.path.name + ".lock")
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            with open(lock_path, "a+") as handle:
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                else:  # pragma: no cover - exercised on Windows
                    handle.seek(0, os.SEEK_END)
                    if handle.tell() == 0:
                        handle.write(" ")
                        handle.flush()
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
                try:
                    self._load()
                    yield
                finally:
                    if fcntl is not None:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    else:  # pragma: no cover - exercised on Windows
                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)

    def save(self, kind: str, target: str, text: str) -> Dict[str, Any]:
        """Save (or overwrite) the draft for one conversation.

        Raises :class:`ValueError` when the kind or target is invalid,
        the text is empty or over the send limit, or the draft cap is
        reached by a new conversation, and :class:`OSError` when the
        write fails (the in-memory store is left as it was on disk).
        """
        if kind not in DRAFT_KINDS:
            raise ValueError(f"unknown draft target kind: {kind!r}")
        hashed = _valid_hash(target)
        if hashed is None:
            raise ValueError("draft target must be a destination hash")
        try:
            cleaned = normalize_message(text)
        except ProtocolError as exc:
            raise ValueError(str(exc)) from exc
        draft = {
            "kind": kind,
            "target": hashed,
            "text": cleaned,
            "updated_at": time.time(),
        }
        with self._transaction():
            key = _draft_key(kind, hashed)
            if key not in self._drafts and len(self._drafts) >= MAX_DRAFTS:
                raise ValueError(
                    f"draft limit reached ({MAX_DRAFTS} drafts;"
                    " delete one with `tau draft delete`)"
                )
            snapshot = self._snapshot()
            self._drafts[key] = draft
            try:
                self._save()
            except OSError:
                self._restore(snapshot)
                raise
        return dict(draft)

    def get(self, kind: str, target: str) -> Optional[Dict[str, Any]]:
        """The draft for a conversation, or ``None`` when there is none."""
        hashed = _valid_hash(target)
        if hashed is None or kind not in DRAFT_KINDS:
            return None
        with self._transaction():
            draft = self._drafts.get(_draft_key(kind, hashed))
        return dict(draft) if draft is not None else None

    def delete(self, kind: str, target: str) -> bool:
        """Delete a draft; ``True`` when one was removed."""
        hashed = _valid_hash(target)
        if hashed is None or kind not in DRAFT_KINDS:
            return False
        with self._transaction():
            key = _draft_key(kind, hashed)
            if key not in self._drafts:
                return False
            snapshot = self._snapshot()
            del self._drafts[key]
            try:
                self._save()
            except OSError:
                self._restore(snapshot)
                raise
        return True

    def list_drafts(self) -> List[Dict[str, Any]]:
        """All drafts, oldest first.

        Reloads from disk first, so drafts saved by another process
        (for example ``tau draft save`` while ``tau serve`` runs) are
        seen.
        """
        with self._transaction():
            return sorted(
                (dict(entry) for entry in self._drafts.values()),
                key=lambda entry: entry["updated_at"],
            )

    def __len__(self) -> int:
        with self._lock:
            return len(self._drafts)
