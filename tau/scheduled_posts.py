"""Scheduled Tan posts: queue a post to publish later.

The Tan analogue of a scheduled DM (``tau send --delay``): ``tau tan
post --delay 5m`` stores the post instead of publishing it, and a
node serving with ``--social`` publishes due posts on its mail-loop
pass.  A post whose publish time has come goes out at the next pass,
about every 45 seconds.

Polls queue the same way (``tau tan poll --delay 5m``): a queued entry
carries ``kind`` ``"poll"`` with the question as its text and the fixed
options alongside it, and the publish pass signs the poll at publish
time, so its expiry clock still starts when it goes out.  Queue files
written before polls could be scheduled carry no ``kind`` and load as
posts, exactly as before.

Reposts and quotes queue the same way (``tau tan repost --delay 5m``,
``tau tan quote --delay 5m``): a queued entry carries ``kind``
``"repost"`` or ``"quote"`` with the original post's hash in
``target_post`` — a repost carries no text, a quote carries its
comment — and the publish pass goes out through ``app.repost`` /
``app.quote``, so the target checks (cached, linked, not deleted, no
standing repost) still apply at publish time.  Queue files written
before kinds existed carry no ``kind`` and load as posts, exactly as
before.

The queue lives in the data directory (``scheduled-posts.json``) so
scheduled posts survive restarts.  A publish that fails keeps the
entry queued and retries with exponential backoff (up to a limit) instead of dropping the
post — the same failure direction as the outbox: never lose a queued
post silently.  Cancelling removes the entry before it goes out.

Entries are validated on load the way the other small JSON stores
are: a malformed file raises rather than silently dropping queued
posts, and the mail-loop publisher treats a corrupt queue as
nothing-due (with a warning) so one bad file cannot stall mail.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import time
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union

from .protocol import (
    ProtocolError,
    clean_contact,
    clean_location,
    normalize_message,
    sanitize_file_name,
)
from .util import atomic_write_json, ensure_dir

log = logging.getLogger(__name__)

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore[assignment]

try:  # Windows only; the lock uses msvcrt there, as identity.py does.
    import msvcrt
except ImportError:  # pragma: no cover - POSIX
    msvcrt = None  # type: ignore[assignment]

SCHEDULED_POSTS_FILE_MODE = 0o600
SCHEDULED_POST_ID_BYTES = 8
MAX_SCHEDULED_POST_DELAY = 7 * 24 * 60 * 60
MAX_SCHEDULED_POST_TTL = 255
# Retry policy for a failed publish: exponential backoff from the base,
# capped, and a hard attempt limit after which the entry stays queued
# (never dropped silently) but is no longer retried automatically.
MAX_PUBLISH_ATTEMPTS = 12
PUBLISH_RETRY_BASE_SECONDS = 60.0
PUBLISH_RETRY_CAP_SECONDS = 60.0 * 60.0
# Wall-clock bound on one publish pass.  The pass runs on the mail-loop
# thread, so entries not started within the budget wait for a later pass.
PUBLISH_PASS_BUDGET_SECONDS = 20.0


class ScheduledPostsError(ValueError):
    """Raised when a scheduled-posts file or value is malformed."""


def new_scheduled_post_id() -> str:
    """A fresh queue id: 16 lowercase hex characters."""
    return secrets.token_hex(SCHEDULED_POST_ID_BYTES)


def _clean_id(value: Any) -> str:
    if not isinstance(value, str):
        raise ScheduledPostsError(f"not a scheduled post id: {value!r}")
    lowered = value.lower()
    if len(lowered) != 2 * SCHEDULED_POST_ID_BYTES:
        raise ScheduledPostsError(f"not a scheduled post id: {value!r}")
    try:
        bytes.fromhex(lowered)
    except ValueError:
        raise ScheduledPostsError(f"not a scheduled post id: {value!r}") from None
    return lowered


def _clean_text(value: Any, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not value.strip() and not allow_empty):
        raise ScheduledPostsError("scheduled post text must not be empty")
    return value


def _clean_reply_to(value: Any) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ScheduledPostsError(f"not a post hash: {value!r}")
    lowered = value.lower()
    if len(lowered) != 64:
        raise ScheduledPostsError(f"not a post hash: {value!r}")
    try:
        bytes.fromhex(lowered)
    except ValueError:
        raise ScheduledPostsError(f"not a post hash: {value!r}") from None
    return lowered


def _clean_targets(value: Any) -> Tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise ScheduledPostsError("scheduled post targets must be a list")
    cleaned = []
    for target in value:
        if not isinstance(target, str) or not target.strip():
            raise ScheduledPostsError(f"not a publish target: {target!r}")
        cleaned.append(target)
    return tuple(cleaned)


def _clean_image(value: Any) -> Optional[Dict[str, Any]]:
    """Validate a queued post's image attachment: ``{sha256, size, mime, name}``.

    The same shape :func:`tau.tangents.tan._clean_image` validates for
    an immediate post, re-checked here so a hand-edited queue file
    cannot smuggle a bad descriptor into the publish pass.  The bytes
    themselves are never in the queue — only the descriptor; they live
    pinned in the local media store from schedule time.  ``None`` means
    no image (and is what old queue files without the field load as).
    """
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ScheduledPostsError("scheduled post image must be an object")
    sha256 = value.get("sha256")
    if not isinstance(sha256, str) or len(sha256) != 64:
        raise ScheduledPostsError(
            "scheduled post image sha256 must be a 64 character hex digest"
        )
    try:
        bytes.fromhex(sha256)
    except ValueError as exc:
        raise ScheduledPostsError(
            "scheduled post image sha256 must be a 64 character hex digest"
        ) from exc
    size = value.get("size")
    if not isinstance(size, int) or isinstance(size, bool):
        raise ScheduledPostsError("scheduled post image size must be an integer")
    # Lazy import: this module stays importable without Reticulum, which
    # ``tau.backup`` relies on — the same reason schedule_post imports
    # its Tan helpers lazily.
    from .tangents.tan import IMAGE_MIME_TYPES, MAX_IMAGE_BYTES

    if size < 1 or size > MAX_IMAGE_BYTES:
        raise ScheduledPostsError(
            f"scheduled post image of {size} bytes exceeds the "
            f"{MAX_IMAGE_BYTES} byte limit"
        )
    mime = value.get("mime")
    if mime not in IMAGE_MIME_TYPES:
        raise ScheduledPostsError(
            "scheduled post image mime must be one of: " + ", ".join(IMAGE_MIME_TYPES)
        )
    name = value.get("name")
    if (
        not isinstance(name, str)
        or not sanitize_file_name(name)
        or sanitize_file_name(name) != name
    ):
        raise ScheduledPostsError(
            "scheduled post image name must be a safe bare file name"
        )
    return {
        "sha256": sha256.lower(),
        "size": size,
        "mime": mime,
        "name": name,
    }


def _clean_location(value: Any) -> Optional[Dict[str, Any]]:
    """Validate a queued post's place attachment: ``{lat, lon, name?}``.

    The same shape :func:`tau.tangents.tan._clean_location` validates
    for an immediate post, re-checked here so a hand-edited queue file
    cannot smuggle a bad pin into the publish pass.  The pin itself is
    tiny — coordinates plus an optional name — so it rides the queue
    entry, unlike an image whose bytes live in the media store.
    ``None`` means no place (and is what old queue files without the
    field load as).
    """
    if value is None:
        return None
    cleaned = clean_location(value)
    if cleaned is None:
        raise ScheduledPostsError(
            "scheduled post location must be an object with latitude "
            "-90..90 and longitude -180..180"
        )
    return cleaned


def _clean_contact(value: Any) -> Optional[Dict[str, Any]]:
    """Validate a queued post's contact-card attachment: ``{name, destination_hash}``.

    The same shape :func:`tau.protocol.clean_contact` validates for
    an immediate post, re-checked here so a hand-edited queue file
    cannot smuggle a bad card into the publish pass.  The card itself
    is tiny — a name plus a hash — so it rides the queue entry, like
    a location pin.  ``None`` means no card (and is what old queue
    files without the field load as).
    """
    if value is None:
        return None
    cleaned = clean_contact(value)
    if cleaned is None:
        raise ScheduledPostsError(
            "scheduled post contact must be an object with a name and a "
            "32-character hex destination hash"
        )
    return cleaned


def _clean_ttl(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ScheduledPostsError(f"not a gossip TTL: {value!r}")
    if value < 0 or value > MAX_SCHEDULED_POST_TTL:
        raise ScheduledPostsError(f"not a gossip TTL: {value!r}")
    return value


def _clean_timestamp(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ScheduledPostsError(f"not a timestamp: {label}={value!r}")
    stamp = float(value)
    if stamp <= 0 or stamp != stamp or stamp in (float("inf"), float("-inf")):
        raise ScheduledPostsError(f"not a timestamp: {label}={value!r}")
    return stamp


def _clean_attempts(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ScheduledPostsError(f"not an attempt count: {value!r}")
    return value


def _clean_retry_at(value: Any) -> float:
    # 0.0 means "eligible now"; unlike publish_at it is not a real time.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ScheduledPostsError(f"not a retry time: {value!r}")
    stamp = float(value)
    if stamp < 0 or stamp != stamp or stamp == float("inf"):
        raise ScheduledPostsError(f"not a retry time: {value!r}")
    return stamp


def _clean_duration(value: Any) -> float:
    # 0.0 means "no expiry"; a duration is a non-negative span, not a time.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ScheduledPostsError(f"not a duration: {value!r}")
    seconds = float(value)
    if seconds < 0 or seconds != seconds or seconds == float("inf"):
        raise ScheduledPostsError(f"not a duration: {value!r}")
    return seconds


def _clean_kind(value: Any) -> str:
    if value not in ("post", "repost", "quote", "poll"):
        raise ScheduledPostsError(f"not a scheduled entry kind: {value!r}")
    return value


def _clean_options(value: Any) -> Tuple[str, ...]:
    # Lightweight shape check for the dataclass: the full option rules
    # (distinct, normalised, length-capped) run at schedule time via
    # ``_clean_poll_options`` and again when the poll is created, so a
    # hand-built entry cannot slip a malformed list into the queue.
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise ScheduledPostsError("scheduled poll options must be a list")
    cleaned = []
    for option in value:
        if not isinstance(option, str) or not option.strip():
            raise ScheduledPostsError(f"not a poll option: {option!r}")
        cleaned.append(option)
    if len(cleaned) > 8:
        raise ScheduledPostsError("a scheduled poll has at most 8 options")
    return tuple(cleaned)


@dataclass(frozen=True)
class ScheduledPost:
    """One queued Tan post, waiting for its publish time."""

    id: str
    text: str
    publish_at: float
    created_at: float
    reply_to: Optional[str] = None
    to: Tuple[str, ...] = ()
    ttl: int = 3
    # Once a publish has created the signed post, its hash is kept so
    # retries re-send that same post instead of signing a new copy.
    post_hash: Optional[str] = None
    attempts: int = 0
    next_attempt_at: float = 0.0
    # When a publish pass last paused on this entry (stop or budget):
    # 0.0 means it never paused.  A pass starts with entries that have
    # never paused (or paused longest ago), so one post whose peers
    # keep stalling cannot monopolize the queue and starve later due
    # posts — the pause is scheduling state, not a failure, so it
    # leaves attempts and the retry limit untouched.
    paused_at: float = 0.0
    # Destination hashes that still owe delivery after a partial
    # failure; empty means "all of ``to`` (or every peer)".
    pending_peers: Tuple[str, ...] = ()
    # Seconds the post lives after it is published; 0.0 means it never
    # expires.  Stored as a duration, not a timestamp: the clock starts
    # when the post is published, not when it is queued.
    expires_in: float = 0.0
    # What the entry publishes as: ``"post"`` (the text is the post,
    # optionally a reply), ``"poll"`` (the text is the question and
    # ``options`` carries its fixed choices), or ``"repost"`` /
    # ``"quote"`` (the text is the quote's comment, empty for a repost,
    # and ``target_post`` is the original post's hash).  Defaults to
    # ``"post"`` so queue files written before kinds existed load
    # unchanged.
    kind: str = "post"
    # The post being reposted or quoted; only present for those kinds.
    target_post: Optional[str] = None
    options: Tuple[str, ...] = ()
    # Image attachment descriptor (``{sha256, size, mime, name}``) for a
    # post scheduled with ``--image``; the bytes live pinned in the
    # local media store, never in the queue.  ``None`` means no image.
    image: Optional[Dict[str, Any]] = None
    # Place attachment (``{lat, lon, name?}``) for a post scheduled
    # with ``--location``; the pin is small, so it rides the queue
    # entry itself.  ``None`` means no place.
    location: Optional[Dict[str, Any]] = None
    # Contact-card attachment (``{name, destination_hash}``) for a
    # post scheduled with ``--contact``; the card is small, so it
    # rides the queue entry itself.  ``None`` means no card.
    contact: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        # A frozen dataclass still builds through __init__: validate
        # here so a hand-built entry cannot slip past from_dict's
        # checks with an empty text or an out-of-range TTL.
        object.__setattr__(self, "id", _clean_id(self.id))
        object.__setattr__(
            self, "text", _clean_text(self.text, allow_empty=self.kind == "repost")
        )
        object.__setattr__(self, "reply_to", _clean_reply_to(self.reply_to))
        object.__setattr__(self, "to", _clean_targets(list(self.to)))
        object.__setattr__(self, "ttl", _clean_ttl(self.ttl))
        object.__setattr__(self, "publish_at", _clean_timestamp(self.publish_at, "publish_at"))
        object.__setattr__(self, "created_at", _clean_timestamp(self.created_at, "created_at"))
        object.__setattr__(self, "post_hash", _clean_reply_to(self.post_hash))
        object.__setattr__(self, "attempts", _clean_attempts(self.attempts))
        object.__setattr__(self, "next_attempt_at", _clean_retry_at(self.next_attempt_at))
        object.__setattr__(self, "paused_at", _clean_retry_at(self.paused_at))
        object.__setattr__(self, "pending_peers", _clean_targets(list(self.pending_peers)))
        object.__setattr__(self, "expires_in", _clean_duration(self.expires_in))
        object.__setattr__(self, "kind", _clean_kind(self.kind))
        object.__setattr__(self, "target_post", _clean_reply_to(self.target_post))
        object.__setattr__(self, "options", _clean_options(self.options))
        object.__setattr__(self, "image", _clean_image(self.image))
        object.__setattr__(self, "location", _clean_location(self.location))
        object.__setattr__(self, "contact", _clean_contact(self.contact))
        if self.kind == "poll":
            # A poll is a top-level entry: it has no reply target, and
            # its options are fixed at schedule time.
            if self.reply_to is not None:
                raise ScheduledPostsError("a scheduled poll cannot be a reply")
            if not 2 <= len(self.options) <= 8:
                raise ScheduledPostsError("a scheduled poll needs 2-8 options")
            if self.image is not None:
                raise ScheduledPostsError("a scheduled poll cannot carry an image")
            if self.location is not None:
                raise ScheduledPostsError("a scheduled poll cannot carry a location")
            if self.contact is not None:
                raise ScheduledPostsError("a scheduled poll cannot carry a contact card")
            if self.target_post is not None:
                raise ScheduledPostsError(
                    "a scheduled poll cannot carry a target post"
                )
        elif self.kind in ("repost", "quote"):
            # A repost/quote amplifies one post: it has no reply target
            # and no expiry (those commands have no --expire — the
            # created records carry none), and it names its original.
            if self.reply_to is not None:
                raise ScheduledPostsError(
                    f"a scheduled {self.kind} cannot be a reply"
                )
            if self.target_post is None:
                raise ScheduledPostsError(
                    f"a scheduled {self.kind} needs a target post"
                )
            if self.expires_in > 0:
                raise ScheduledPostsError(
                    f"a scheduled {self.kind} cannot expire"
                )
            if self.kind == "repost" and self.text.strip():
                raise ScheduledPostsError("a scheduled repost carries no text")
            if self.options:
                raise ScheduledPostsError(
                    f"a scheduled {self.kind} cannot carry poll options"
                )
            # A quote is a top-level entry, so it may carry an image
            # like a post; a repost has no comment of its own and
            # still cannot.
            if self.kind == "repost" and self.image is not None:
                raise ScheduledPostsError("a scheduled repost cannot carry an image")
            if self.location is not None:
                raise ScheduledPostsError(
                    f"a scheduled {self.kind} cannot carry a location"
                )
            if self.contact is not None:
                raise ScheduledPostsError(
                    f"a scheduled {self.kind} cannot carry a contact card"
                )
        else:
            if self.target_post is not None:
                raise ScheduledPostsError(
                    "a scheduled post cannot carry a target post"
                )
            if self.options:
                raise ScheduledPostsError("a scheduled post cannot carry poll options")
            # The write path refuses both combinations, and the publish
            # pass drops them silently (a reply is created without the
            # attachment) — so a hand-edited queue file carrying either
            # must fail to load, not publish a stripped reply.
            if self.image is not None and self.reply_to is not None:
                raise ScheduledPostsError(
                    "an image can only be attached to a top-level post, not a reply"
                )
            if self.location is not None and self.reply_to is not None:
                raise ScheduledPostsError(
                    "a location can only be attached to a top-level post, not a reply"
                )
            if self.contact is not None and self.reply_to is not None:
                raise ScheduledPostsError(
                    "a contact card can only be attached to a top-level post, not a reply"
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "text": self.text,
            "reply_to": self.reply_to,
            "to": list(self.to),
            "ttl": self.ttl,
            "publish_at": self.publish_at,
            "created_at": self.created_at,
            "post_hash": self.post_hash,
            "attempts": self.attempts,
            "next_attempt_at": self.next_attempt_at,
            "paused_at": self.paused_at,
            "pending_peers": list(self.pending_peers),
            "expires_in": self.expires_in,
            "kind": self.kind,
            "target_post": self.target_post,
            "options": list(self.options),
            "image": self.image,
            "location": self.location,
            "contact": self.contact,
        }

    @classmethod
    def from_dict(cls, data: Any) -> "ScheduledPost":
        if not isinstance(data, dict):
            raise ScheduledPostsError("scheduled post must be a JSON object")
        kind = _clean_kind(data.get("kind", "post"))
        return cls(
            id=_clean_id(data.get("id")),
            text=_clean_text(data.get("text"), allow_empty=(kind == "repost")),
            reply_to=_clean_reply_to(data.get("reply_to")),
            to=_clean_targets(data.get("to")),
            ttl=_clean_ttl(data.get("ttl", 3)),
            publish_at=_clean_timestamp(data.get("publish_at"), "publish_at"),
            created_at=_clean_timestamp(data.get("created_at"), "created_at"),
            post_hash=_clean_reply_to(data.get("post_hash")),
            attempts=_clean_attempts(data.get("attempts", 0)),
            next_attempt_at=_clean_retry_at(data.get("next_attempt_at", 0.0)),
            paused_at=_clean_retry_at(data.get("paused_at", 0.0)),
            pending_peers=_clean_targets(data.get("pending_peers")),
            expires_in=_clean_duration(data.get("expires_in", 0.0)),
            kind=kind,
            target_post=_clean_reply_to(data.get("target_post")),
            options=_clean_options(data.get("options")),
            image=_clean_image(data.get("image")),
            location=_clean_location(data.get("location")),
            contact=_clean_contact(data.get("contact")),
        )


@dataclass(frozen=True)
class ScheduledPosts:
    """The queued posts, oldest publish time first."""

    posts: Tuple[ScheduledPost, ...] = ()

    def __len__(self) -> int:
        return len(self.posts)

    def due(self, now: Optional[float] = None) -> List[ScheduledPost]:
        """Queued posts whose publish time has come, oldest first."""
        at = now if now is not None else time.time()
        return [post for post in self.posts if post.publish_at <= at]

    def resolve(self, prefix: str) -> ScheduledPost:
        """Find one queued post by full id or unique prefix."""
        if not isinstance(prefix, str) or not prefix.strip():
            raise ScheduledPostsError("a scheduled post id is required")
        matches = [post for post in self.posts if post.id.startswith(prefix.lower())]
        if not matches:
            raise ScheduledPostsError(f"no scheduled post {prefix!r}")
        if len(matches) > 1:
            raise ScheduledPostsError(
                f"ambiguous scheduled post id {prefix!r} "
                f"({len(matches)} match)"
            )
        return matches[0]

    def with_added(self, post: ScheduledPost) -> "ScheduledPosts":
        if any(existing.id == post.id for existing in self.posts):
            raise ScheduledPostsError(f"scheduled post {post.id} is already queued")
        ordered = tuple(sorted((*self.posts, post), key=lambda p: p.publish_at))
        return ScheduledPosts(posts=ordered)

    def with_replaced(self, post: ScheduledPost) -> "ScheduledPosts":
        if not any(existing.id == post.id for existing in self.posts):
            raise ScheduledPostsError(f"no scheduled post {post.id!r}")
        swapped = tuple(post if existing.id == post.id else existing for existing in self.posts)
        return ScheduledPosts(posts=swapped)

    def with_removed(self, post_id: str) -> "ScheduledPosts":
        kept = tuple(post for post in self.posts if post.id != post_id)
        if len(kept) == len(self.posts):
            raise ScheduledPostsError(f"no scheduled post {post_id!r}")
        return ScheduledPosts(posts=kept)

    def to_dict(self) -> Dict[str, Any]:
        return {"posts": [post.to_dict() for post in self.posts]}

    @classmethod
    def from_dict(cls, data: Any) -> "ScheduledPosts":
        if not isinstance(data, dict):
            raise ScheduledPostsError("scheduled-posts must be a JSON object")
        if "posts" not in data:
            raise ScheduledPostsError("posts is required")
        raw = data.get("posts")
        if not isinstance(raw, list):
            raise ScheduledPostsError("posts must be a list")
        return cls(posts=tuple(ScheduledPost.from_dict(item) for item in raw))


def load_scheduled_posts(path: Optional[Union[str, Path]]) -> ScheduledPosts:
    """Load the queue at *path*; a missing file means nothing scheduled."""
    if path is None:
        return ScheduledPosts()
    target = Path(path).expanduser()
    try:
        target.stat()
    except FileNotFoundError:
        return ScheduledPosts()
    except OSError as exc:
        raise ScheduledPostsError(
            f"could not read scheduled-posts {target}: {exc}"
        ) from exc
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ScheduledPostsError(
            f"could not read scheduled-posts {target}: {exc}"
        ) from exc
    return ScheduledPosts.from_dict(data)


def save_scheduled_posts(path: Union[str, Path], posts: ScheduledPosts) -> Path:
    """Persist *posts* to *path* as JSON (mode 0600) and return the path."""
    target = Path(path).expanduser()
    return atomic_write_json(target, posts.to_dict(), mode=SCHEDULED_POSTS_FILE_MODE)


@contextmanager
def scheduled_posts_lock(path: Union[str, Path]) -> Iterator[None]:
    """Serialize read-modify-write updates of the scheduled-posts queue.

    Same shape as the ignored-groups lock: a flock on a
    ``.scheduled-posts.json.lock`` sidecar.  An atomic file replacement
    alone does not protect the whole update — without the lock, two
    commands racing load the same file and the last save discards the
    other's change.  Callers must reload inside the lock.
    """
    if fcntl is None and msvcrt is None:  # pragma: no cover - neither platform
        yield
        return
    target = Path(path).expanduser()
    # The data directory may not exist yet (a first schedule on a
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


def schedule_post(
    path: Union[str, Path],
    *,
    text: str,
    publish_at: float,
    reply_to: Optional[str] = None,
    to: Optional[Sequence[str]] = None,
    ttl: int = 3,
    now: Optional[float] = None,
    name: Optional[str] = None,
    expires_in: float = 0.0,
    kind: str = "post",
    target_post: Optional[str] = None,
    options: Optional[Sequence[str]] = None,
    image: Optional[Dict[str, Any]] = None,
    location: Optional[Dict[str, Any]] = None,
    contact: Optional[Dict[str, Any]] = None,
) -> ScheduledPost:
    """Queue a post for *publish_at*; returns the stored entry.

    The text gets the same validation as an immediate ``tau tan post``
    (``normalize_message``: non-empty, at most ``MAX_MESSAGE_CHARS``), so
    a post that could never be created is refused here, not on every
    publish pass.  When *expires_in* is given the post disappears from
    display surfaces that long after it is published; it shares the
    delay cap — a post cannot outlive the furthest it may be queued.
    With ``kind="poll"`` the text is the poll question and *options*
    carries its fixed choices: a poll cannot be a reply, and the
    question plus options get the same wire-size preflight an
    immediate ``tau tan poll`` gets, so a poll that could never fit
    the 8 KiB Tan request is refused here.  A poll cannot carry an
    image: no side supports a scheduled poll with an attachment.

    With ``kind="repost"`` the entry carries no text and *target_post*
    is the post being reposted; with ``kind="quote"`` the text is the
    comment on *target_post*, and it gets the same wire-size preflight
    as a scheduled post (the target hash counts toward the 8 KiB Tan
    request, the way a reply target does).  Neither kind takes a reply
    target or an expiry.

    When *image* is given — the ``{sha256, size, mime, name}``
    descriptor an immediate post carries — the attachment is stored on
    the entry and signed with the post at publish time; the bytes live
    pinned in the local media store, never in the queue.  An image is
    top-level-only, the same rule as an immediate post, so it is
    refused with a reply target, a poll, or a repost — but a quote is
    a top-level entry, so it may carry one, exactly like a post.

    When *location* is given — the ``{lat, lon, name?}`` pin an
    immediate post carries — it rides the entry itself (it is small)
    and is signed with the post at publish time.  A location is
    top-level-only too, the same rule as an immediate post and as an
    image, so it is refused with a reply target, a poll, a repost,
    or a quote.

    When *contact* is given — the ``{name, destination_hash}`` card
    an immediate post carries — it rides the entry itself (it is
    small) and is signed with the post at publish time.  A card is
    top-level-only too, the same rule as an immediate post and as a
    location, so it is refused with a reply target, a poll, a repost,
    or a quote.
    """
    clean_kind = _clean_kind(kind)
    clean_target = _clean_reply_to(target_post)
    if clean_kind in ("repost", "quote"):
        if reply_to is not None:
            raise ScheduledPostsError(f"a scheduled {clean_kind} cannot be a reply")
        if clean_target is None:
            raise ScheduledPostsError(f"a scheduled {clean_kind} needs a target post")
        if expires_in:
            raise ScheduledPostsError(f"a scheduled {clean_kind} cannot expire")
    elif clean_kind == "post" and clean_target is not None:
        raise ScheduledPostsError("a scheduled post cannot carry a target post")
    if clean_kind == "repost":
        if text.strip():
            raise ScheduledPostsError("a scheduled repost carries no text")
        clean_text = ""
    else:
        try:
            clean_text = normalize_message(text)
        except ProtocolError as exc:
            raise ScheduledPostsError(str(exc)) from exc
    # The character limit alone is not enough: 2,000 four-byte
    # characters pass it but overflow the 8 KiB Tan request once
    # encoded, and such a post would fail every publish attempt
    # forever.  Refuse it here, before the entry is saved.  (Lazy
    # import: this module stays importable without Reticulum, which
    # ``tau.backup`` relies on.)
    from .tangents.tan import PostError, check_poll_fits, check_post_text_fits

    clean_expires_in = _clean_duration(expires_in)
    if clean_expires_in > MAX_SCHEDULED_POST_DELAY:
        raise ScheduledPostsError(
            "expires_in must be at most 7d (the scheduled-post delay cap)"
        )
    clean_options: Tuple[str, ...] = ()
    clean_image = _clean_image(image)
    clean_location = _clean_location(location)
    clean_contact = _clean_contact(contact)
    if clean_kind == "poll":
        if reply_to is not None:
            raise ScheduledPostsError("a scheduled poll cannot be a reply")
        if clean_image is not None:
            raise ScheduledPostsError("a scheduled poll cannot carry an image")
        if clean_location is not None:
            raise ScheduledPostsError("a scheduled poll cannot carry a location")
        if clean_contact is not None:
            raise ScheduledPostsError("a scheduled poll cannot carry a contact card")
        if clean_target is not None:
            raise ScheduledPostsError("a scheduled poll cannot carry a target post")
        from .tangents.tan import _clean_poll_options

        try:
            clean_options = tuple(_clean_poll_options(list(options or [])))
        except PostError as exc:
            raise ScheduledPostsError(str(exc)) from exc
        try:
            check_poll_fits(
                clean_text,
                clean_options,
                name=name,
                expires=clean_expires_in > 0,
            )
        except PostError as exc:
            raise ScheduledPostsError(str(exc)) from exc
    elif clean_kind in ("repost", "quote"):
        if options:
            raise ScheduledPostsError(
                f"a scheduled {clean_kind} cannot carry poll options"
            )
        if clean_kind == "repost" and clean_image is not None:
            raise ScheduledPostsError("a scheduled repost cannot carry an image")
        if clean_location is not None:
            raise ScheduledPostsError(
                f"a scheduled {clean_kind} cannot carry a location"
            )
        if clean_contact is not None:
            raise ScheduledPostsError(
                f"a scheduled {clean_kind} cannot carry a contact card"
            )
    else:
        if options:
            raise ScheduledPostsError("a scheduled post cannot carry poll options")
        if clean_image is not None and reply_to is not None:
            raise ScheduledPostsError(
                "an image can only be attached to a top-level post, not a reply"
            )
        if clean_location is not None and reply_to is not None:
            raise ScheduledPostsError(
                "a location can only be attached to a top-level post, not a reply"
            )
        if clean_contact is not None and reply_to is not None:
            raise ScheduledPostsError(
                "a contact card can only be attached to a top-level post, not a reply"
            )
    if clean_kind != "repost" and clean_kind != "poll":
        try:
            check_post_text_fits(
                clean_text,
                name=name,
                # A quote's target rides in the envelope like a reply
                # target, so it counts toward the wire-size budget.
                reply_to=clean_target if clean_kind == "quote" else reply_to,
                expires=clean_expires_in > 0,
                image=clean_image,
                # A pin rides in the signed post too — its name is
                # short, but a borderline text plus a named place can
                # still overflow the 8 KiB budget together.
                location=clean_location,
                # A card rides in the signed post too — its name can
                # be long enough to matter on a borderline text.
                contact=clean_contact,
            )
        except PostError as exc:
            raise ScheduledPostsError(str(exc)) from exc
    post = ScheduledPost(
        id=new_scheduled_post_id(),
        text=clean_text,
        reply_to=_clean_reply_to(reply_to),
        to=_clean_targets(list(to) if to is not None else None),
        ttl=_clean_ttl(ttl),
        publish_at=_clean_timestamp(publish_at, "publish_at"),
        created_at=now if now is not None else time.time(),
        expires_in=clean_expires_in,
        kind=clean_kind,
        target_post=clean_target,
        options=clean_options,
        image=clean_image,
        location=clean_location,
        contact=clean_contact,
    )
    with scheduled_posts_lock(path):
        queue = load_scheduled_posts(path)
        save_scheduled_posts(path, queue.with_added(post))
    return post


def schedule_repost(
    path: Union[str, Path],
    *,
    target_post: str,
    publish_at: float,
    to: Optional[Sequence[str]] = None,
    ttl: int = 3,
    now: Optional[float] = None,
    name: Optional[str] = None,
) -> ScheduledPost:
    """Queue a repost of *target_post* for *publish_at*.

    Thin wrapper over :func:`schedule_post` with ``kind="repost"``:
    the entry carries no text, only the original post's hash.
    """
    return schedule_post(
        path,
        text="",
        publish_at=publish_at,
        to=to,
        ttl=ttl,
        now=now,
        name=name,
        kind="repost",
        target_post=target_post,
    )


def schedule_quote(
    path: Union[str, Path],
    *,
    target_post: str,
    text: str,
    publish_at: float,
    to: Optional[Sequence[str]] = None,
    ttl: int = 3,
    now: Optional[float] = None,
    name: Optional[str] = None,
    image: Optional[Dict[str, Any]] = None,
) -> ScheduledPost:
    """Queue a quote of *target_post* carrying *text* for *publish_at*.

    Thin wrapper over :func:`schedule_post` with ``kind="quote"``:
    the text is the comment, and it gets the same wire-size preflight
    as a scheduled post.  When *image* is given — the ``{sha256,
    size, mime, name}`` descriptor an immediate quote carries — the
    attachment is stored on the entry and signed with the quote at
    publish time, exactly like a scheduled post's image.
    """
    return schedule_post(
        path,
        text=text,
        publish_at=publish_at,
        to=to,
        ttl=ttl,
        now=now,
        name=name,
        kind="quote",
        target_post=target_post,
        image=image,
    )


def schedule_poll(
    path: Union[str, Path],
    *,
    question: str,
    options: Sequence[str],
    publish_at: float,
    to: Optional[Sequence[str]] = None,
    ttl: int = 3,
    now: Optional[float] = None,
    name: Optional[str] = None,
    expires_in: float = 0.0,
) -> ScheduledPost:
    """Queue a poll for *publish_at*; returns the stored entry.

    Thin wrapper over :func:`schedule_post` with ``kind="poll"``: the
    question is the entry text and the options ride alongside it.
    """
    return schedule_post(
        path,
        text=question,
        publish_at=publish_at,
        to=to,
        ttl=ttl,
        now=now,
        name=name,
        expires_in=expires_in,
        kind="poll",
        options=options,
    )


def cancel_scheduled_post(
    path: Union[str, Path],
    prefix: str,
    *,
    on_cancel: Optional[Callable[[ScheduledPost], None]] = None,
) -> ScheduledPost:
    """Drop one queued post by full id or unique prefix; returns it.

    When *on_cancel* is given it runs under the queue lock, after the
    entry is resolved and before it is removed — so a mail-loop pass
    cannot publish the post between the callback and the removal.
    The callback is how the CLI deletes the locally stored post for
    an entry whose publish already created one.
    """
    with scheduled_posts_lock(path):
        queue = load_scheduled_posts(path)
        post = queue.resolve(prefix)
        if on_cancel is not None:
            on_cancel(post)
        save_scheduled_posts(path, queue.with_removed(post.id))
    return post


def edit_scheduled_post(
    path: Union[str, Path],
    prefix: str,
    *,
    text: Optional[str] = None,
    publish_at: Optional[float] = None,
    name: Optional[str] = None,
    now: Optional[float] = None,
) -> ScheduledPost:
    """Change the text and/or publish time of one queued post.

    At least one of *text* / *publish_at* must be given.  The new text
    gets the same validation as scheduling does (``normalize_message``
    plus the wire-size preflight against the entry's kind and retained
    fields — a poll's options, a quote's target, an image descriptor,
    a location pin — so an edit cannot queue a post that could never
    be published.  A scheduled repost carries no text, so a text edit
    on one is refused.  A new publish time must be in the future and within the
    7-day queue cap; expiry counts from publication, so it does not
    limit how far out the publish time may move.

    An entry whose publish already created the signed post (it has a
    stored ``post_hash``) cannot be edited: the text is already signed,
    so changing it would orphan the stored copy.  Cancel it instead.
    An edit also clears the entry's retry state (attempts, backoff,
    pause), so a post that gave up publishes at its new time instead
    of staying stuck.  Everything else about the entry — reply
    target, audience, TTL, expiry — is left untouched.
    """
    if text is None and publish_at is None:
        raise ScheduledPostsError(
            "nothing to change — pass a new text and/or a new publish time"
        )
    at = now if now is not None else time.time()
    clean_text: Optional[str] = None
    if text is not None:
        try:
            clean_text = normalize_message(text)
        except ProtocolError as exc:
            raise ScheduledPostsError(str(exc)) from exc
    clean_publish_at: Optional[float] = None
    if publish_at is not None:
        clean_publish_at = _clean_timestamp(publish_at, "publish_at")
        if clean_publish_at <= at:
            raise ScheduledPostsError("publish time must be in the future")
        if clean_publish_at - at > MAX_SCHEDULED_POST_DELAY:
            raise ScheduledPostsError(
                "publish time must be within 7d (the scheduled-post delay cap)"
            )
    with scheduled_posts_lock(path):
        queue = load_scheduled_posts(path)
        post = queue.resolve(prefix)
        if post.post_hash is not None:
            raise ScheduledPostsError(
                "that post was already created — cancel it instead of editing it"
            )
        if clean_publish_at is not None:
            # The time was checked before the lock, but the edit may
            # have waited behind a publish pass: re-check against a
            # fresh clock so a stale "future" time is never saved.
            fresh = now if now is not None else time.time()
            if clean_publish_at <= fresh:
                raise ScheduledPostsError("publish time must be in the future")
            if clean_publish_at - fresh > MAX_SCHEDULED_POST_DELAY:
                raise ScheduledPostsError(
                    "publish time must be within 7d (the scheduled-post delay cap)"
                )
        if clean_text is not None:
            if post.kind == "repost":
                raise ScheduledPostsError("a scheduled repost carries no text")
            # Lazy import: this module stays importable without
            # Reticulum, which ``tau.backup`` relies on.
            from .tangents.tan import (
                PostError,
                check_poll_fits,
                check_post_text_fits,
            )

            try:
                if post.kind == "poll":
                    check_poll_fits(
                        clean_text,
                        post.options,
                        name=name,
                        expires=post.expires_in > 0,
                    )
                else:
                    check_post_text_fits(
                        clean_text,
                        name=name,
                        # A quote's target rides in the envelope like
                        # a reply target, so it counts toward the
                        # wire-size budget.
                        reply_to=(
                            post.target_post
                            if post.kind == "quote"
                            else post.reply_to
                        ),
                        expires=post.expires_in > 0,
                        image=post.image,
                        location=post.location,
                        contact=post.contact,
                    )
            except PostError as exc:
                raise ScheduledPostsError(str(exc)) from exc
        edited = replace(
            post,
            text=clean_text if clean_text is not None else post.text,
            publish_at=(
                clean_publish_at if clean_publish_at is not None else post.publish_at
            ),
            # A failed entry that gave up stays excluded from the
            # publish pass until it is edited; clear the retry state
            # so the edited entry publishes at its new time.
            attempts=0,
            next_attempt_at=0.0,
            paused_at=0.0,
        )
        save_scheduled_posts(path, queue.with_replaced(edited))
    return edited


class _PassPaused(Exception):
    """Internal: the publish pass ends early — node stopping, or the
    pass budget elapsed.  Raised instead of recording a publish
    failure, so the entry being worked on keeps its attempts and its
    unsent peers stay queued for the next pass.  The handler records
    the pause on the entry (``paused_at``) so the next pass starts
    with posts that have not paused, instead of this one again.
    """


def publish_due_scheduled_posts(
    path: Union[str, Path],
    app: Any,
    *,
    resolve_peer: Optional[Callable[[str], Any]] = None,
    now: Optional[float] = None,
    should_stop: Optional[Callable[[], bool]] = None,
) -> Dict[str, int]:
    """Publish every due queued post through *app* (a ``TanApp``).

    The whole pass runs under the queue lock, so a cancel either lands
    before the entry is considered (and it is never sent) or waits until
    the entry has been sent and removed (and then reports it gone).  The
    lock is held across the network sends, which is the price of that
    guarantee.

    A post is published when ``app.publish`` returns with no per-peer
    errors — the same bar as an immediate ``tau tan post``, where zero
    known peers still counts as published.  The signed post is created
    once and its hash saved before sending, so retries re-send the same
    post.  Peers that already accepted it are not sent it again.  A
    queued poll publishes the same way, through ``app.create_poll``
    with the question and its fixed options, so its expiry clock also
    starts at publish time.  A queued repost or quote publishes the
    same way, through ``app.repost`` / ``app.quote`` with its target
    post, so the target checks (cached, linked, not deleted, no
    standing repost) apply at publish time and a failure there keeps
    the entry queued like any other failed publish.

    A failed attempt (exception or per-peer errors) keeps the entry
    queued with exponential backoff.  After ``MAX_PUBLISH_ATTEMPTS`` it
    is no longer retried automatically but stays queued for the user to
    inspect or cancel.  A pass stops starting new entries once
    ``PUBLISH_PASS_BUDGET_SECONDS`` have elapsed.  A pass that pauses
    on an entry remembers the pause on it, so the next pass starts
    with entries that have not paused — a post whose peers keep
    stalling cannot starve later due posts, and it is still retried
    once they have had their turn.

    When ``should_stop`` is given (the node's stop event), a pass ends
    early — between posts, between peer resolutions, and between
    sends — so ``stop()`` never waits out a whole publish pass; the
    entry being worked on stays queued with its attempts untouched
    and its unsent peers kept for the next pass.  The pass budget
    (``deadline``) is checked at the same points, so several peers
    that each announce near their timeout cannot stretch one pass
    into minutes either.  The in-flight peer resolution or send
    still runs to its own timeout, so a stop waits at most one such
    wait, not a whole pass.

    Returns ``{"published": n, "kept": m}``.  A corrupt queue file logs a
    warning and publishes nothing, so one bad file cannot stall the mail
    loop that calls this.
    """
    at = now if now is not None else time.time()
    deadline = time.monotonic() + PUBLISH_PASS_BUDGET_SECONDS
    with scheduled_posts_lock(path):
        try:
            queue = load_scheduled_posts(path)
        except ScheduledPostsError as exc:
            log.warning("Scheduled-posts at %s is unusable (%s); skipping", path, exc)
            return {"published": 0, "kept": 0}
        candidates = [
            post
            for post in queue.due(at)
            if post.next_attempt_at <= at and post.attempts < MAX_PUBLISH_ATTEMPTS
        ]
        # Fairness across passes: entries that have never paused — or
        # paused longest ago — go first, so a post whose peers keep
        # stalling the pass does not monopolize the queue while later
        # due posts wait.  A paused entry is still attempted in the
        # same pass once the fresher entries have had their turn.
        candidates.sort(key=lambda post: (post.paused_at, post.publish_at))
        published = 0
        try:
            for post in candidates:
                if should_stop is not None and should_stop():
                    break
                if time.monotonic() > deadline:
                    break
                failed_peers: Optional[Tuple[str, ...]] = None
                try:
                    targets = _resolve_publish_targets(
                        post, resolve_peer, should_stop, deadline
                    )
                    stored = app.get_post(post.post_hash) if post.post_hash else None
                    # get_post returns the canonical post plus storage-only
                    # "seq"/"linked" fields; publish() encodes the dict
                    # unchanged, so strip them — _check_wire_size only
                    # measures the canonical post.
                    created = (
                        {k: v for k, v in stored.items() if k not in ("seq", "linked")}
                        if stored is not None
                        else None
                    )
                    if created is None:
                        # The expiry clock starts at publish time, not
                        # queue time: a post queued for days must not
                        # arrive already dead.  Sample the clock here, not
                        # at the pass start: lock and target resolution can
                        # take long enough to eat a short expiry.
                        create_at = now if now is not None else time.time()
                        expires_at = (
                            create_at + post.expires_in
                            if post.expires_in > 0
                            else None
                        )
                        if post.kind == "poll":
                            created = app.create_poll(
                                post.text,
                                list(post.options),
                                expires_at=expires_at,
                            )
                        elif post.kind == "repost":
                            created = app.repost(post.target_post)
                        elif post.kind == "quote":
                            created = app.quote(
                                post.target_post, post.text, image=post.image
                            )
                        else:
                            created = app.create_post(
                                post.text,
                                reply_to=post.reply_to,
                                expires_at=expires_at,
                                image=post.image,
                                location=post.location,
                                contact=post.contact,
                            )
                        # Persist the signed post's identity before sending, so
                        # a retry after a crash re-sends it rather than signing
                        # a second copy.
                        post = replace(post, post_hash=created["hash"])
                        queue = queue.with_replaced(post)
                        save_scheduled_posts(path, queue)
                    result = app.publish(
                        created,
                        targets=targets,
                        ttl=post.ttl,
                        should_stop=should_stop,
                        deadline=deadline,
                    )
                    errors = list(result.get("errors") or []) if isinstance(result, dict) else []
                    unsent = (
                        tuple(str(h) for h in result.get("unsent") or [])
                        if isinstance(result, dict)
                        else ()
                    )
                    if unsent:
                        # The send paused mid-post (stop or budget): the
                        # peers never attempted stay queued for the next
                        # pass, ahead of the peers that already failed
                        # this pass — the next pass reaches the fresh
                        # peers first instead of burning its budget
                        # re-trying the ones that just timed out.
                        # No attempt is burned.
                        failed = tuple(str(err.get("peer") or "") for err in errors)
                        pending = tuple(dict.fromkeys([*unsent, *failed]))
                        if not all(p and p != "?" for p in pending):
                            pending = ()
                        post = replace(post, pending_peers=pending)
                        queue = queue.with_replaced(post)
                        save_scheduled_posts(path, queue)
                        raise _PassPaused()
                    if errors:
                        failed_peers = tuple(str(err.get("peer") or "") for err in errors)
                        raise ScheduledPostsError(
                            f"{len(errors)} peer(s) did not receive the post"
                        )
                except _PassPaused:
                    # Not a publish failure: end the pass without burning
                    # an attempt on the entry being worked on — but
                    # remember the pause, so the next pass starts with
                    # posts that have not paused instead of this one
                    # again.  A post that keeps pausing yields to later
                    # due posts and is retried once they have had
                    # their turn; its attempts and retry limit are
                    # untouched.
                    post = replace(post, paused_at=at)
                    queue = queue.with_replaced(post)
                    save_scheduled_posts(path, queue)
                    raise
                except Exception as exc:  # noqa: BLE001 - a failed publish retries
                    attempts = post.attempts + 1
                    retry_in = min(
                        PUBLISH_RETRY_BASE_SECONDS * 2 ** (attempts - 1),
                        PUBLISH_RETRY_CAP_SECONDS,
                    )
                    pending = post.pending_peers
                    if failed_peers is not None:
                        # Retry only the peers that failed.  An unknown peer
                        # hash cannot be re-targeted, so fall back to all targets.
                        pending = failed_peers if all(p and p != "?" for p in failed_peers) else ()
                    post = replace(
                        post,
                        attempts=attempts,
                        next_attempt_at=at + retry_in,
                        pending_peers=pending,
                    )
                    queue = queue.with_replaced(post)
                    save_scheduled_posts(path, queue)
                    log.warning(
                        "Scheduled post %s could not publish (%s); keeping it queued",
                        post.id,
                        exc,
                    )
                    if attempts >= MAX_PUBLISH_ATTEMPTS:
                        log.warning(
                            "Scheduled post %s gave up after %d attempts; it stays "
                            "queued until cancelled",
                            post.id,
                            attempts,
                        )
                    continue
                queue = queue.with_removed(post.id)
                save_scheduled_posts(path, queue)
                published += 1
        except _PassPaused:
            pass
    return {"published": published, "kept": len(candidates) - published}


def _resolve_publish_targets(
    post: ScheduledPost,
    resolve_peer: Optional[Callable[[str], Any]],
    should_stop: Optional[Callable[[], bool]] = None,
    deadline: Optional[float] = None,
) -> Optional[List[Any]]:
    """Peers for one publish attempt: the still-owed ones, else ``to``.

    A stop or an expired pass budget between resolutions pauses the
    pass (``_PassPaused``) before the next peer is resolved, so a
    queue of slow announcers cannot stretch one pass into minutes.
    """
    names = post.pending_peers or post.to
    if not names:
        return None
    if resolve_peer is None:
        raise ScheduledPostsError("scheduled post has targets but no peer resolver")
    resolved: List[Any] = []
    for name in names:
        if should_stop is not None and should_stop():
            raise _PassPaused()
        if deadline is not None and time.monotonic() > deadline:
            raise _PassPaused()
        resolved.append(resolve_peer(name))
    return resolved
