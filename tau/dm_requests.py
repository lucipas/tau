"""DM requests: hold direct messages denied by the allow-list for review.

When the DM allow-list is on, a message from a peer who is not on the
list is denied with ``not_allowed`` and dropped.  The owner never
learns anyone tried to reach them, so a first contact from a new peer
is invisible and the only way to start a conversation is for the owner
to add the peer first, which needs out-of-band coordination.

This module holds those denied messages instead of discarding them:
each one is kept in ``dm-requests.json`` in the data directory until
the owner accepts it (which allow-lists the sender and delivers the
message into the inbox) or declines it (which drops it).  Block-list
denials are never recorded — blocking means the message is never
stored, never marked seen, and never passed on, and a request would
break that promise.

A request is a copy of the denied message: its id, sender destination
hash, text, kind, send time, the time it was received, the message's
own expiry, and the reply it quotes.  Requests expire 7 days after
receipt, the same TTL as held mail, and the store is capped at 100
entries with the oldest dropped — a stranger's retries must not fill
the disk, and a flood of requests must not evict anything else.

Only messages that carry an id are recorded: the id is what dedupes
a sender's retries and what the accept path marks seen, so a retry
after acceptance lands as a duplicate instead of a second copy.  An
id-less message is still denied, exactly as before, it just leaves
no request behind.  Anonymous senders (no verifiable identity) leave
no request either — there is no sender to allow-list on accept.

The sender-visible behaviour does not change: the sender still gets
``not_allowed``, so no new protocol signal leaks whether the message
was held or dropped.  The store is local-only, like the allow-list
itself.
"""

from __future__ import annotations

import json
import logging
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

from .allowlist import clean_destination
from .util import atomic_write_json

log = logging.getLogger(__name__)

DM_REQUEST_TTL = 7 * 24 * 60 * 60
MAX_DM_REQUESTS = 100
DM_REQUESTS_FILE_MODE = 0o600

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore[assignment]

try:  # Windows only; the lock uses msvcrt there, as identity.py does.
    import msvcrt
except ImportError:  # pragma: no cover - POSIX
    msvcrt = None  # type: ignore[assignment]


class DmRequestsError(ValueError):
    """Raised when a DM-requests file or reference is malformed."""


def _clean_text(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise DmRequestsError("request text must be a non-empty string")
    return value


def _clean_timestamp(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DmRequestsError(f"request {field} must be a number")
    if value != value or value in (float("inf"), float("-inf")):
        raise DmRequestsError(f"request {field} must be finite")
    return float(value)


def _clean_optional_timestamp(value: Any, field: str) -> Optional[float]:
    if value is None:
        return None
    return _clean_timestamp(value, field)


@dataclass(frozen=True)
class DmRequest:
    """One denied DM held for owner review."""

    id: str
    sender: str
    text: str
    kind: str
    sent_at: float
    received_at: float
    request_expires_at: float
    expires_at: Optional[float] = None
    in_reply_to: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "id": self.id,
            "sender": self.sender,
            "text": self.text,
            "kind": self.kind,
            "sent_at": self.sent_at,
            "received_at": self.received_at,
            "request_expires_at": self.request_expires_at,
        }
        if self.expires_at is not None:
            data["expires_at"] = self.expires_at
        if self.in_reply_to is not None:
            data["in_reply_to"] = self.in_reply_to
        return data

    @classmethod
    def from_dict(cls, data: Any) -> "DmRequest":
        if not isinstance(data, dict):
            raise DmRequestsError("request must be a JSON object")
        request_id = data.get("id")
        if not isinstance(request_id, str) or not request_id:
            raise DmRequestsError("request id must be a non-empty string")
        try:
            sender = clean_destination(data.get("sender"))
        except ValueError as exc:
            raise DmRequestsError(f"request sender is not a destination: {exc}") from exc
        kind = data.get("kind")
        if not isinstance(kind, str) or not kind:
            raise DmRequestsError("request kind must be a non-empty string")
        in_reply_to = data.get("in_reply_to")
        if in_reply_to is not None and not isinstance(in_reply_to, str):
            raise DmRequestsError("request in_reply_to must be a string")
        return cls(
            id=request_id,
            sender=sender,
            text=_clean_text(data.get("text")),
            kind=kind,
            sent_at=_clean_timestamp(data.get("sent_at"), "sent_at"),
            received_at=_clean_timestamp(data.get("received_at"), "received_at"),
            request_expires_at=_clean_timestamp(
                data.get("request_expires_at"), "request_expires_at"
            ),
            expires_at=_clean_optional_timestamp(data.get("expires_at"), "expires_at"),
            in_reply_to=in_reply_to,
        )

    def is_expired(self, now: Optional[float] = None) -> bool:
        """True when the request itself has passed its 7-day hold."""
        moment = time.time() if now is None else now
        return self.request_expires_at <= moment

    def message_expired(self, now: Optional[float] = None) -> bool:
        """True when the held message's own expiry has passed."""
        if self.expires_at is None:
            return False
        moment = time.time() if now is None else now
        return self.expires_at <= moment


def _clean_requests(data: Any) -> List[DmRequest]:
    if not isinstance(data, dict) or not isinstance(data.get("requests"), list):
        raise DmRequestsError("requests file must be an object with a requests list")
    return [DmRequest.from_dict(entry) for entry in data["requests"]]


def load_dm_requests(path: Optional[Union[str, Path]]) -> List[DmRequest]:
    """Load the held DM requests at *path*; a missing file means none."""
    if path is None:
        return []
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise DmRequestsError(
            f"could not read DM requests {target}: {exc}"
        ) from exc
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise DmRequestsError(
            f"could not read DM requests {target}: {exc}"
        ) from exc
    return _clean_requests(data)


def save_dm_requests(path: Union[str, Path], requests: List[DmRequest]) -> Path:
    """Persist *requests* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    document = {"requests": [entry.to_dict() for entry in requests]}
    return atomic_write_json(target, document, mode=DM_REQUESTS_FILE_MODE)


@contextmanager
def dm_requests_lock(path: Union[str, Path]) -> Iterator[None]:
    """Serialize read-modify-write updates of the requests store.

    Same shape as the ignored-groups lock: a flock on a
    ``.dm-requests.json.lock`` sidecar.  An atomic file replacement
    alone does not protect the whole update — without the lock, two
    recordings racing load the same file and the last save discards
    the other's request.  Callers must reload inside the lock.
    Windows locks the first byte of the sidecar with ``msvcrt.locking``,
    as ``tau.identity`` does.
    """
    if fcntl is None and msvcrt is None:  # pragma: no cover - neither platform
        yield
        return
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
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


def _prune(requests: List[DmRequest], now: float) -> List[DmRequest]:
    return [entry for entry in requests if not entry.is_expired(now)]


def _enforce_cap(requests: List[DmRequest]) -> List[DmRequest]:
    if len(requests) <= MAX_DM_REQUESTS:
        return requests
    ordered = sorted(requests, key=lambda entry: entry.received_at)
    return ordered[len(ordered) - MAX_DM_REQUESTS:]


def record_dm_request(
    path: Union[str, Path],
    request: DmRequest,
    *,
    now: Optional[float] = None,
) -> bool:
    """Hold *request* for review; return False when its id is already held.

    A sender retrying a denied message re-records the same id, so the
    second copy is dropped and the first (earliest) receipt is kept.
    Expired requests are pruned on the way in, and past the cap the
    oldest request is dropped to make room.  The caller decides what a
    ``False`` means — the node hook just keeps denying.
    """
    moment = time.time() if now is None else now
    with dm_requests_lock(path):
        stored = _prune(load_dm_requests(path), moment)
        if any(entry.id == request.id for entry in stored):
            return False
        stored.append(request)
        save_dm_requests(path, _enforce_cap(stored))
    return True


def list_dm_requests(
    path: Optional[Union[str, Path]],
    *,
    now: Optional[float] = None,
) -> List[DmRequest]:
    """Pending requests, oldest first; expired ones are left out.

    A listing never rewrites the file: expired entries are skipped in
    memory and pruned for real the next time a request is recorded or
    removed.
    """
    moment = time.time() if now is None else now
    stored = _prune(load_dm_requests(path), moment)
    return sorted(stored, key=lambda entry: entry.received_at)


def resolve_dm_request(
    requests: List[DmRequest], ref: str
) -> DmRequest:
    """Resolve *ref* to one request by full id or unique id prefix."""
    if not isinstance(ref, str) or not ref:
        raise DmRequestsError("a DM request id is required")
    exact = [entry for entry in requests if entry.id == ref]
    if len(exact) == 1:
        return exact[0]
    matches = [entry for entry in requests if entry.id.startswith(ref)]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise DmRequestsError(f"no DM request {ref!r}")
    raise DmRequestsError(f"ambiguous DM request {ref!r}: matches {len(matches)}")


def remove_dm_request(
    path: Union[str, Path],
    ref: str,
    *,
    now: Optional[float] = None,
) -> DmRequest:
    """Drop the request *ref* names and return it.

    Expired requests are pruned first, so naming one that already
    expired reports it as unknown rather than resurrecting it.
    """
    moment = time.time() if now is None else now
    with dm_requests_lock(path):
        stored = _prune(load_dm_requests(path), moment)
        request = resolve_dm_request(stored, ref)
        save_dm_requests(
            path, [entry for entry in stored if entry.id != request.id]
        )
    return request
