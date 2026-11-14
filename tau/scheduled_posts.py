"""Scheduled Tan posts: queue a post to publish later.

The Tan analogue of a scheduled DM (``tau send --delay``): ``tau tan
post --delay 5m`` stores the post instead of publishing it, and a
node serving with ``--social`` publishes due posts on its mail-loop
pass.  A post whose publish time has come goes out at the next pass,
about every 45 seconds.

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

from .protocol import ProtocolError, normalize_message
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


def _clean_text(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
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

    def __post_init__(self) -> None:
        # A frozen dataclass still builds through __init__: validate
        # here so a hand-built entry cannot slip past from_dict's
        # checks with an empty text or an out-of-range TTL.
        object.__setattr__(self, "id", _clean_id(self.id))
        object.__setattr__(self, "text", _clean_text(self.text))
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
        }

    @classmethod
    def from_dict(cls, data: Any) -> "ScheduledPost":
        if not isinstance(data, dict):
            raise ScheduledPostsError("scheduled post must be a JSON object")
        return cls(
            id=_clean_id(data.get("id")),
            text=_clean_text(data.get("text")),
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
) -> ScheduledPost:
    """Queue a post for *publish_at*; returns the stored entry.

    The text gets the same validation as an immediate ``tau tan post``
    (``normalize_message``: non-empty, at most ``MAX_MESSAGE_CHARS``), so
    a post that could never be created is refused here, not on every
    publish pass.  When *expires_in* is given the post disappears from
    display surfaces that long after it is published; it shares the
    delay cap — a post cannot outlive the furthest it may be queued.
    """
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
    from .tangents.tan import PostError, check_post_text_fits

    clean_expires_in = _clean_duration(expires_in)
    if clean_expires_in > MAX_SCHEDULED_POST_DELAY:
        raise ScheduledPostsError(
            "expires_in must be at most 7d (the scheduled-post delay cap)"
        )
    try:
        check_post_text_fits(
            clean_text,
            name=name,
            reply_to=reply_to,
            expires=clean_expires_in > 0,
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
    )
    with scheduled_posts_lock(path):
        queue = load_scheduled_posts(path)
        save_scheduled_posts(path, queue.with_added(post))
    return post


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
    post.  Peers that already accepted it are not sent it again.

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
                        created = app.create_post(
                            post.text,
                            reply_to=post.reply_to,
                            expires_at=expires_at,
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
