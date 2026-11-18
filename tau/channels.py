"""Public channels: open topic rooms anyone can join by name.

Channels are tau's community primitive. Everything else in tau is
addressed (a DM or a group with a fixed roster) or a signed personal
feed (Tan). A channel is neither: a named, unlisted room with no
roster and no admin, where any peer that knows the exact name can
join, send, and fetch history.

A channel message is a signed record::

    {
      "v": 1,
      "channel": "mesh-meetup",
      "author": "<32 hex destination hash>",
      "key": "<128 hex public key>",
      "name": "alice",
      "text": "see you at the park",
      "created_at": 1760000000.0,
      "hash": "<64 hex sha256(canonical body)>",
      "sig": "<128 hex Ed25519 signature over the hash>"
    }

Unlike Tan there is no per-author hash chain: channels are addressed
by name, not by author, so messages are ordered by
``(created_at, hash)`` and deduplicated by hash. The signature still
makes a message verifiable even when it is relayed by a node that
does not own the author's identity, so nodes can cache and re-serve
messages for each other — which is how a new joiner fetches the last
N messages as history.

The network protocol is two request paths served on the same
Reticulum destination as the rest of tau:

    /tau/channel/feed   {"channel": "...", "before": 1760000000.0, "limit": 20}
    /tau/channel/post   {"message": {...}, "ttl": 2}

``/tau/channel/feed`` reads cached messages for a channel.
``/tau/channel/post`` accepts a signed message and, when it is new,
gossips it to a small number of other peers.  Pulling a feed and
pushing a message are deliberately separate: a node can read a
channel it joined even when the author of a message is offline, as
long as any peer still has that message cached.

Membership is local-only and never gossiped: joining records the
name in the local store, leaving drops it (cached messages stay
until the node stops serving them — a leave is about what *you*
follow, not a retraction).  Blocked authors are refused at the door
like everywhere else in tau: a message from a blocked destination is
dropped, never stored, never re-gossipped.

A channel also carries one topic: a short signed record saying what
the room is about::

    {
      "v": 1,
      "channel": "mesh-meetup",
      "author": "<32 hex destination hash>",
      "key": "<128 hex public key>",
      "topic": "monthly mesh meetup planning",
      "updated_at": 1760000000.0,
      "hash": "<64 hex sha256(canonical body)>",
      "sig": "<128 hex Ed25519 signature over the hash>"
    }

Channels have no owner, so any member may set the topic and the
newest ``updated_at`` wins; an empty topic clears the stored one.
Topics ride the same paths as messages — ``/tau/channel/post``
accepts a ``{"topic": {...}}`` payload and ``/tau/channel/feed``
answers with the channel's current topic — so a topic set by one
member reaches the rest of the room through gossip and sync.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import random
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import RNS

from .blocklist import load_blocklist
from .identity import load_public_key
from .protocol import (
    APP_NAME,
    ASPECT,
    MAX_GROUP_TOPIC_LENGTH,
    MAX_MESSAGE_CHARS,
    MAX_REQUEST_BYTES,
    ProtocolError,
    clean_group_topic,
    decode,
    encode,
    normalize_message,
    response_error,
    response_ok,
    sanitize_name,
)
from .util import ensure_dir

log = logging.getLogger(__name__)

CHANNEL_VERSION = 1
CHANNEL_FEED_PATH = "/tau/channel/feed"
CHANNEL_POST_PATH = "/tau/channel/post"

AUTHOR_HEX_LENGTH = 32
HASH_HEX_LENGTH = 64
KEY_HEX_LENGTH = 128
SIG_HEX_LENGTH = 128

MAX_CHANNEL_NAME_LENGTH = 64
MAX_CHANNEL_MESSAGE_CHARS = MAX_MESSAGE_CHARS
MAX_CHANNEL_HISTORY = 50
# Bound on what one channel may hold on disk: history serves the
# last 50, so 500 (10 pages) leaves ample headroom for sync while a
# peer flooding one room cannot grow the store without limit.
MAX_STORED_PER_CHANNEL = 500
# Bound on how many distinct rooms a node caches messages for: joined
# channels are never evicted, so a peer spraying random room names
# cannot grow the store without limit either.
MAX_CACHED_CHANNELS = 100
DEFAULT_CHANNEL_LIMIT = 20
MAX_GOSSIP_TTL = 3
GOSSIP_FANOUT = 3
MAX_SYNC_PAGES = 32

# Channel names are joined by exact name, so the character set is
# tight: start alphanumerical, then letters, digits, dots, dashes and
# underscores.  No spaces, no slashes — a name must survive a CLI
# argument, a URL query and a gossip payload unchanged.
_CHANNEL_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")

# Fields covered by the message hash.  The channel is part of the
# signed body, so a relay cannot move a message into another room
# without breaking the hash.
HASH_FIELDS = ("v", "channel", "author", "key", "name", "text", "created_at")

# Fields covered by the topic hash: the same shape as a message,
# minus the display name (a topic is metadata, not a post) and with
# the topic text in place of the message text.
TOPIC_FIELDS = ("v", "channel", "author", "key", "topic", "updated_at")

# Column order used by every SELECT so rows convert consistently.
_MESSAGE_COLUMNS = (
    "v",
    "hash",
    "channel",
    "author",
    "key",
    "name",
    "text",
    "created_at",
    "received_at",
    "sig",
)

_TOPIC_COLUMNS = (
    "v",
    "hash",
    "channel",
    "author",
    "key",
    "topic",
    "updated_at",
    "sig",
)


class ChannelError(ValueError):
    """Base class for channel validation and storage errors."""


def clean_channel_name(raw: Any) -> str:
    """Validate a channel name, returning it unchanged or raising."""
    if not isinstance(raw, str):
        raise ChannelError("channel name must be a string")
    name = raw.strip()
    if not name:
        raise ChannelError("channel name must not be empty")
    if len(name) > MAX_CHANNEL_NAME_LENGTH:
        raise ChannelError(
            f"channel name is longer than {MAX_CHANNEL_NAME_LENGTH} characters"
        )
    if _CHANNEL_NAME_RE.fullmatch(name) is None:
        raise ChannelError(
            "channel names start with a letter or digit and contain only "
            "letters, digits, '.', '-' and '_'"
        )
    return name


def _require_hex(value: Any, length: int, field: str) -> str:
    if not isinstance(value, str):
        raise ChannelError(f"{field} must be a hex string")
    text = value.strip().lower()
    if len(text) != length:
        raise ChannelError(f"{field} must be {length} hex characters")
    try:
        bytes.fromhex(text)
    except ValueError as exc:
        raise ChannelError(f"{field} is not valid hex") from exc
    return text


def _require_timestamp(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ChannelError(f"{field} must be a number")
    try:
        timestamp = float(value)
    except OverflowError as exc:
        # A peer can send an integer far larger than a double holds;
        # that is a bad value, not a crash past the handler's catches.
        raise ChannelError(f"{field} is too large") from exc
    if not math.isfinite(timestamp) or timestamp < 0:
        raise ChannelError(f"{field} must be a finite, non-negative timestamp")
    return timestamp


def _canonical_body(message: Dict[str, Any]) -> bytes:
    body = {field: message[field] for field in HASH_FIELDS}
    return json.dumps(
        body,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def hash_message(message: Dict[str, Any]) -> str:
    """Hash a channel message: lowercase hex sha256 of its body."""
    if not isinstance(message, dict):
        raise ChannelError("a channel message must be a JSON object")
    missing = [field for field in HASH_FIELDS if field not in message]
    if missing:
        raise ChannelError(
            f"channel message is missing fields: {', '.join(missing)}"
        )
    return hashlib.sha256(_canonical_body(message)).hexdigest()


def _identity_from_message(message: Dict[str, Any]) -> RNS.Identity:
    try:
        identity = RNS.Identity(create_keys=False)
        if not load_public_key(identity, bytes.fromhex(message["key"])):
            raise ValueError("could not load public key")
    except Exception as exc:  # pragma: no cover - depends on RNS internals
        raise ChannelError(f"invalid public key: {exc}") from exc
    return identity


def _author_from_identity(identity: RNS.Identity) -> str:
    return RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()


def _check_wire_size(message: Dict[str, Any]) -> None:
    """Reject a message whose publish envelope exceeds the budget.

    The envelope is measured with the message's own ``hash`` and
    ``sig`` (or placeholders of the same length before signing), so
    a message that passes here can actually be published: measuring
    the unsigned body would admit a multibyte message that only fails
    once the signature is attached.
    """
    envelope = dict(message)
    envelope.setdefault("hash", "0" * HASH_HEX_LENGTH)
    envelope.setdefault("sig", "0" * SIG_HEX_LENGTH)
    try:
        encode(
            {"message": envelope, "ttl": MAX_GOSSIP_TTL},
            max_bytes=MAX_REQUEST_BYTES,
        )
    except ProtocolError as exc:
        raise ChannelError(
            f"channel message is too large to publish: {exc}"
        ) from exc


def _normalize_message(mesg: Dict[str, Any], *, now: Optional[float] = None) -> Dict[str, Any]:
    """Validate and normalise the signed part of a channel message."""
    if not isinstance(mesg, dict):
        raise ChannelError("a channel message must be a JSON object")

    version = mesg.get("v", CHANNEL_VERSION)
    if isinstance(version, bool) or version != CHANNEL_VERSION:
        raise ChannelError(f"unsupported channel message version: {version!r}")

    channel = clean_channel_name(mesg.get("channel"))
    author = _require_hex(mesg.get("author"), AUTHOR_HEX_LENGTH, "author")
    key = _require_hex(mesg.get("key"), KEY_HEX_LENGTH, "key")

    raw_text = mesg.get("text")
    try:
        text = normalize_message(raw_text)
    except ProtocolError as exc:
        # normalize_message raises ProtocolError, but every channel
        # handler catches ChannelError: wrap it so a malformed peer
        # entry is a validation failure, never an unhandled error.
        raise ChannelError(f"invalid channel message text: {exc}") from exc
    if text != raw_text:
        # Messages are hashed before they reach us.  Silently cleaning
        # the text would produce a hash that no longer matches what
        # the author signed.
        raise ChannelError("channel message text is not normalised")

    raw_name = mesg.get("name", "")
    name = sanitize_name(raw_name)
    if name != raw_name:
        raise ChannelError("channel message name is not normalised")

    created_at = _require_timestamp(mesg.get("created_at"), "created_at")
    if now is not None and created_at > float(now) + 86400.0:
        raise ChannelError("channel message timestamp is too far in the future")

    message = {
        "v": CHANNEL_VERSION,
        "channel": channel,
        "author": author,
        "key": key,
        "name": name,
        "text": text,
        "created_at": created_at,
    }
    _check_wire_size(message)
    return message


def verify_message(
    mesg: Dict[str, Any], *, now: Optional[float] = None
) -> Dict[str, Any]:
    """Validate a complete channel message, including hash and signature."""
    message = _normalize_message(mesg, now=now)

    digest = mesg.get("hash")
    expected = hash_message(message)
    if not isinstance(digest, str) or digest.strip().lower() != expected:
        raise ChannelError("channel message hash does not match its contents")
    message["hash"] = expected

    signature = _require_hex(mesg.get("sig"), SIG_HEX_LENGTH, "sig")
    message["sig"] = signature

    identity = _identity_from_message(message)
    author = _author_from_identity(identity)
    if author != message["author"]:
        raise ChannelError("public key does not match the message author")
    try:
        valid = identity.validate(bytes.fromhex(signature), bytes.fromhex(expected))
    except Exception as exc:  # pragma: no cover - depends on RNS internals
        raise ChannelError(f"could not check signature: {exc}") from exc
    if not valid:
        raise ChannelError("channel message signature is invalid")
    return message


def _canonical_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """Return the public, canonical representation of a message."""
    return {
        "v": message["v"],
        "channel": message["channel"],
        "author": message["author"],
        "key": message["key"],
        "name": message["name"],
        "text": message["text"],
        "created_at": message["created_at"],
        "hash": message["hash"],
        "sig": message["sig"],
    }


# -- storage ---------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    v INTEGER NOT NULL DEFAULT 1,
    hash TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    author TEXT NOT NULL,
    key TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    text TEXT NOT NULL,
    created_at REAL NOT NULL,
    received_at REAL NOT NULL,
    sig TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS messages_channel_created
    ON messages(channel, created_at DESC, hash);

CREATE TABLE IF NOT EXISTS memberships (
    channel TEXT PRIMARY KEY,
    joined_at REAL NOT NULL
);

-- One topic per channel: the newest signed topic wins, and an empty
-- topic clears the row instead of storing a blank.  Topics are
-- metadata, not messages, so they never count toward the history
-- cap and are never pruned with it.
CREATE TABLE IF NOT EXISTS topics (
    channel TEXT PRIMARY KEY,
    v INTEGER NOT NULL DEFAULT 1,
    hash TEXT NOT NULL,
    author TEXT NOT NULL,
    key TEXT NOT NULL,
    topic TEXT NOT NULL,
    updated_at REAL NOT NULL,
    sig TEXT NOT NULL
);
"""


def init_db(path: Optional[Union[str, Path]] = None) -> sqlite3.Connection:
    """Open (creating it) the channel database at *path*."""
    if path is None:
        raise ChannelError("a channel database path is required")
    db_path = Path(path).expanduser()
    ensure_dir(db_path.parent)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(_SCHEMA)
    conn.execute(
        "INSERT OR IGNORE INTO meta(key, value) VALUES('version', ?)",
        (str(CHANNEL_VERSION),),
    )
    conn.commit()
    return conn


def _row_to_message(row: sqlite3.Row) -> Dict[str, Any]:
    return _canonical_message(dict(row))


def _fetch_message(conn: sqlite3.Connection, digest: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        f"SELECT {', '.join(_MESSAGE_COLUMNS)} FROM messages WHERE hash = ?",
        (digest,),
    ).fetchone()


def get_message(conn: sqlite3.Connection, digest: str) -> Optional[Dict[str, Any]]:
    """Return a stored message by hash, or ``None``."""
    row = _fetch_message(conn, digest)
    return _row_to_message(row) if row is not None else None


def store_message(
    conn: sqlite3.Connection,
    mesg: Dict[str, Any],
    *,
    now: Optional[float] = None,
    verify: bool = True,
) -> Tuple[Dict[str, Any], bool]:
    """Validate and store a channel message, returning ``(message, is_new)``.

    ``is_new`` is false when the message was already stored — or when
    retention evicted it during its own insert (an old message arriving
    into a full store).  Eviction is not an error: the message was
    accepted, it simply did not survive the per-channel bound.
    """
    timestamp = float(now if now is not None else time.time())
    if verify:
        message = verify_message(mesg, now=timestamp)
    else:
        base = _normalize_message(mesg, now=timestamp)
        message = dict(base)
        message["hash"] = hash_message(base)
        message["sig"] = _require_hex(mesg.get("sig"), SIG_HEX_LENGTH, "sig")

    digest = message["hash"]
    existing = _fetch_message(conn, digest)
    if existing is not None:
        return _canonical_message(dict(existing)), False

    try:
        with conn:
            conn.execute(
                "INSERT INTO messages(v, hash, channel, author, key, name, text, "
                "created_at, received_at, sig) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    CHANNEL_VERSION,
                    digest,
                    message["channel"],
                    message["author"],
                    message["key"],
                    message["name"],
                    message["text"],
                    message["created_at"],
                    timestamp,
                    message["sig"],
                ),
            )
            _prune_store(conn, message["channel"])
    except sqlite3.IntegrityError:
        # Another thread or process inserted the same message first.
        existing = _fetch_message(conn, digest)
        if existing is None:
            raise
        return _canonical_message(dict(existing)), False

    record = get_message(conn, digest)
    if record is None:
        # Retention evicted the message during its own insert: an old
        # message arriving into a full store.  Report it as not newly
        # stored rather than as a disappearance — the caller already
        # has the validated message, and the store bound is working.
        return _canonical_message(message), False
    return _canonical_message(record), True


def _prune_store(conn: sqlite3.Connection, channel: str) -> None:
    """Bound the channel store so one peer cannot fill the disk.

    *channel* keeps its newest ``MAX_STORED_PER_CHANNEL`` messages,
    and the store keeps messages for at most ``MAX_CACHED_CHANNELS``
    distinct channels.  Joined channels are never evicted — only rooms
    this node merely relays for are dropped, least-recently-active
    first — so a peer spraying random room names cannot grow the
    store without limit.  Runs inside the caller's transaction, and
    only touches the channel just written plus the eviction check, so
    a busy relay does not rescan the whole store per message.

    Retention ranks by ``MIN(created_at, received_at)``: a peer's
    future-dated post is capped at the time it arrived, so it cannot
    crowd out current-time sends — 500 posts dated a day ahead must
    not stop this node from sending now.
    """
    conn.execute(
        "DELETE FROM messages WHERE channel = ? AND hash NOT IN ("
        "SELECT hash FROM messages WHERE channel = ? "
        "ORDER BY MIN(created_at, received_at) DESC, hash DESC "
        f"LIMIT {MAX_STORED_PER_CHANNEL})",
        (channel, channel),
    )
    while True:
        count = conn.execute(
            "SELECT COUNT(DISTINCT channel) FROM messages"
        ).fetchone()[0]
        if count <= MAX_CACHED_CHANNELS:
            return
        row = conn.execute(
            "SELECT channel FROM messages "
            "WHERE channel NOT IN (SELECT channel FROM memberships) "
            "GROUP BY channel ORDER BY MAX(received_at) ASC LIMIT 1"
        ).fetchone()
        if row is None:
            # Every cached channel is joined: nothing evictable.
            return
        conn.execute("DELETE FROM messages WHERE channel = ?", (row[0],))


def create_message(
    conn: sqlite3.Connection,
    identity: RNS.Identity,
    channel: str,
    text: str,
    *,
    name: str = "anonymous",
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Create, sign and store a message owned by *identity*."""
    timestamp = round(float(now if now is not None else time.time()), 3)
    author = _author_from_identity(identity)
    body = {
        "v": CHANNEL_VERSION,
        "channel": clean_channel_name(channel),
        "author": author,
        "key": identity.get_public_key().hex(),
        "name": sanitize_name(name),
        "text": normalize_message(text),
        "created_at": timestamp,
    }
    digest = hash_message(body)
    mesg = dict(body)
    mesg["hash"] = digest
    mesg["sig"] = identity.sign(bytes.fromhex(digest)).hex()
    # Measure the signed envelope, not the unsigned body: a multibyte
    # message near the budget would otherwise store locally and only
    # fail when publish() encodes it for peers.
    _check_wire_size(mesg)
    record, _ = store_message(conn, mesg, now=timestamp)
    return record


# -- topics ------------------------------------------------------------------


def hash_topic(topic: Dict[str, Any]) -> str:
    """Hash a channel topic: lowercase hex sha256 of its body."""
    if not isinstance(topic, dict):
        raise ChannelError("a channel topic must be a JSON object")
    missing = [field for field in TOPIC_FIELDS if field not in topic]
    if missing:
        raise ChannelError(
            f"channel topic is missing fields: {', '.join(missing)}"
        )
    body = {field: topic[field] for field in TOPIC_FIELDS}
    canonical = json.dumps(
        body,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _normalize_topic(topic: Dict[str, Any], *, now: Optional[float] = None) -> Dict[str, Any]:
    """Validate and normalise the signed part of a channel topic."""
    if not isinstance(topic, dict):
        raise ChannelError("a channel topic must be a JSON object")

    version = topic.get("v", CHANNEL_VERSION)
    if isinstance(version, bool) or version != CHANNEL_VERSION:
        raise ChannelError(f"unsupported channel topic version: {version!r}")

    channel = clean_channel_name(topic.get("channel"))
    author = _require_hex(topic.get("author"), AUTHOR_HEX_LENGTH, "author")
    key = _require_hex(topic.get("key"), KEY_HEX_LENGTH, "key")

    raw_topic = topic.get("topic")
    if not isinstance(raw_topic, str):
        raise ChannelError("channel topic must be a string")
    if len(" ".join(raw_topic.split())) > MAX_GROUP_TOPIC_LENGTH:
        raise ChannelError(
            f"channel topic is longer than {MAX_GROUP_TOPIC_LENGTH} characters"
        )
    cleaned = clean_group_topic(raw_topic)
    if cleaned != raw_topic:
        # Topics are hashed before they reach us.  Silently cleaning
        # the text would produce a hash that no longer matches what
        # the author signed.
        raise ChannelError("channel topic is not normalised")

    updated_at = _require_timestamp(topic.get("updated_at"), "updated_at")
    if now is not None and updated_at > float(now) + 86400.0:
        raise ChannelError("channel topic timestamp is too far in the future")

    return {
        "v": CHANNEL_VERSION,
        "channel": channel,
        "author": author,
        "key": key,
        "topic": cleaned,
        "updated_at": updated_at,
    }


def verify_topic(topic: Dict[str, Any], *, now: Optional[float] = None) -> Dict[str, Any]:
    """Validate a complete channel topic, including hash and signature."""
    record = _normalize_topic(topic, now=now)

    digest = topic.get("hash")
    expected = hash_topic(record)
    if not isinstance(digest, str) or digest.strip().lower() != expected:
        raise ChannelError("channel topic hash does not match its contents")
    record["hash"] = expected

    signature = _require_hex(topic.get("sig"), SIG_HEX_LENGTH, "sig")
    record["sig"] = signature

    identity = _identity_from_message(record)
    author = _author_from_identity(identity)
    if author != record["author"]:
        raise ChannelError("public key does not match the topic author")
    try:
        valid = identity.validate(bytes.fromhex(signature), bytes.fromhex(expected))
    except Exception as exc:  # pragma: no cover - depends on RNS internals
        raise ChannelError(f"could not check signature: {exc}") from exc
    if not valid:
        raise ChannelError("channel topic signature is invalid")
    return record


def _canonical_topic(topic: Dict[str, Any]) -> Dict[str, Any]:
    """Return the public, canonical representation of a topic."""
    return {
        "v": topic["v"],
        "channel": topic["channel"],
        "author": topic["author"],
        "key": topic["key"],
        "topic": topic["topic"],
        "updated_at": topic["updated_at"],
        "hash": topic["hash"],
        "sig": topic["sig"],
    }


def _check_topic_wire_size(topic: Dict[str, Any]) -> None:
    """Reject a topic whose publish envelope exceeds the budget."""
    envelope = dict(topic)
    envelope.setdefault("hash", "0" * HASH_HEX_LENGTH)
    envelope.setdefault("sig", "0" * SIG_HEX_LENGTH)
    try:
        encode(
            {"topic": envelope, "ttl": MAX_GOSSIP_TTL},
            max_bytes=MAX_REQUEST_BYTES,
        )
    except ProtocolError as exc:
        raise ChannelError(
            f"channel topic is too large to publish: {exc}"
        ) from exc


def create_topic(
    conn: sqlite3.Connection,
    identity: RNS.Identity,
    channel: str,
    topic: str,
    *,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Create, sign and store a topic owned by *identity*.

    An empty topic is a clear: the signed envelope is still stored
    (so peers learn the topic was cleared) but :func:`get_topic`
    returns ``None`` for the channel afterwards.
    """
    timestamp = round(float(now if now is not None else time.time()), 3)
    author = _author_from_identity(identity)
    if not isinstance(topic, str):
        raise ChannelError("channel topic must be a string")
    if len(" ".join(topic.split())) > MAX_GROUP_TOPIC_LENGTH:
        raise ChannelError(
            f"channel topic is longer than {MAX_GROUP_TOPIC_LENGTH} characters"
        )
    cleaned = clean_group_topic(topic)
    if cleaned != topic:
        # Refuse to silently clean: the caller passed text with
        # newlines or doubled spaces, and the stored topic would not
        # be the text they asked for.
        raise ChannelError("channel topic is not normalised")
    body = {
        "v": CHANNEL_VERSION,
        "channel": clean_channel_name(channel),
        "author": author,
        "key": identity.get_public_key().hex(),
        "topic": cleaned,
        "updated_at": timestamp,
    }
    envelope = dict(body)
    digest = hash_topic(body)
    envelope["hash"] = digest
    envelope["sig"] = identity.sign(bytes.fromhex(digest)).hex()
    _check_topic_wire_size(envelope)
    store_topic(conn, envelope, now=timestamp)
    return envelope


def store_topic(
    conn: sqlite3.Connection,
    topic: Dict[str, Any],
    *,
    now: Optional[float] = None,
    verify: bool = True,
) -> Tuple[Optional[Dict[str, Any]], bool]:
    """Validate and store a channel topic, returning ``(record, changed)``.

    Newest ``updated_at`` wins — an older topic never overwrites a
    newer one, and a stale clear never wipes a newer topic.  A tie
    breaks on the hash, so two nodes holding the same timestamp keep
    the same topic.  ``changed`` is false when the topic was already
    stored or was older than the stored one; ``record`` is ``None``
    when the stored topic was cleared.
    """
    timestamp = float(now if now is not None else time.time())
    if verify:
        record = verify_topic(topic, now=timestamp)
    else:
        record = _normalize_topic(topic, now=timestamp)
        record["hash"] = hash_topic(record)
        record["sig"] = _require_hex(topic.get("sig"), SIG_HEX_LENGTH, "sig")

    existing = get_topic(conn, record["channel"])
    if existing is not None and (record["updated_at"], record["hash"]) <= (
        existing["updated_at"],
        existing["hash"],
    ):
        return existing, False

    with conn:
        if not record["topic"]:
            conn.execute(
                "DELETE FROM topics WHERE channel = ?", (record["channel"],)
            )
        else:
            conn.execute(
                "INSERT INTO topics(v, hash, channel, author, key, topic, "
                "updated_at, sig) VALUES(?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(channel) DO UPDATE SET "
                "v = excluded.v, hash = excluded.hash, author = excluded.author, "
                "key = excluded.key, topic = excluded.topic, "
                "updated_at = excluded.updated_at, sig = excluded.sig",
                (
                    CHANNEL_VERSION,
                    record["hash"],
                    record["channel"],
                    record["author"],
                    record["key"],
                    record["topic"],
                    record["updated_at"],
                    record["sig"],
                ),
            )
    if not record["topic"]:
        return None, existing is not None
    return _canonical_topic(record), True


def get_topic(conn: sqlite3.Connection, channel: str) -> Optional[Dict[str, Any]]:
    """Return the stored topic for *channel*, or ``None``."""
    name = clean_channel_name(channel)
    row = conn.execute(
        f"SELECT {', '.join(_TOPIC_COLUMNS)} FROM topics WHERE channel = ?",
        (name,),
    ).fetchone()
    return _canonical_topic(dict(row)) if row is not None else None


def channel_history_page(
    conn: sqlite3.Connection,
    channel: str,
    *,
    limit: int = DEFAULT_CHANNEL_LIMIT,
    before: Optional[float] = None,
    before_hash: Optional[str] = None,
    exclude_authors: Optional[Sequence[str]] = None,
) -> Tuple[List[Dict[str, Any]], bool]:
    """Return one page of stored messages for *channel*, newest first.

    Returns ``(messages, has_more)``: ``has_more`` says whether older
    messages exist past this page, so a reader never mistakes a
    shortened page for the end of history.  One extra row is fetched
    to decide that, then dropped.

    The page cursor is ``(before, before_hash)`` — a timestamp plus
    the hash of the last message on the previous page.  A timestamp
    alone would drop records when a page cuts through a tie: several
    messages can share one ``created_at``, and ordering breaks ties
    by hash.  The predicate matches the ``(created_at DESC, hash
    DESC)`` order exactly, so every message is returned once.
    """
    name = clean_channel_name(channel)
    count = max(1, min(int(limit), MAX_CHANNEL_HISTORY))
    clauses = ["channel = ?"]
    params: List[Any] = [name]
    if before is not None:
        stamp = _require_timestamp(before, "before")
        if before_hash is not None:
            cursor_hash = _require_hex(before_hash, HASH_HEX_LENGTH, "before_hash")
            clauses.append("(created_at < ? OR (created_at = ? AND hash < ?))")
            params.extend([stamp, stamp, cursor_hash])
        else:
            clauses.append("created_at < ?")
            params.append(stamp)
    excluded = [author for author in (exclude_authors or []) if author]
    if excluded:
        placeholders = ", ".join("?" for _ in excluded)
        clauses.append(f"author NOT IN ({placeholders})")
        params.extend(excluded)
    rows = conn.execute(
        f"SELECT {', '.join(_MESSAGE_COLUMNS)} FROM messages "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY created_at DESC, hash DESC LIMIT ?",
        (*params, count + 1),
    ).fetchall()
    has_more = len(rows) > count
    return [_row_to_message(row) for row in rows[:count]], has_more


def channel_history(
    conn: sqlite3.Connection,
    channel: str,
    *,
    limit: int = DEFAULT_CHANNEL_LIMIT,
    before: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """Return stored messages for *channel*, newest first.

    *before* pages backwards: only messages created strictly before
    that timestamp are returned, so a sync loop can walk history
    without seeing the same page twice.
    """
    messages, _ = channel_history_page(
        conn, channel, limit=limit, before=before
    )
    return messages


def join_channel(conn: sqlite3.Connection, channel: str) -> Dict[str, Any]:
    """Record membership in *channel*; joining twice is a no-op."""
    name = clean_channel_name(channel)
    now = time.time()
    with conn:
        conn.execute(
            "INSERT OR IGNORE INTO memberships(channel, joined_at) VALUES(?, ?)",
            (name, now),
        )
    row = conn.execute(
        "SELECT channel, joined_at FROM memberships WHERE channel = ?",
        (name,),
    ).fetchone()
    return {"channel": row["channel"], "joined_at": row["joined_at"]}


def leave_channel(conn: sqlite3.Connection, channel: str) -> bool:
    """Drop membership in *channel*; cached messages are kept."""
    name = clean_channel_name(channel)
    with conn:
        cursor = conn.execute(
            "DELETE FROM memberships WHERE channel = ?", (name,)
        )
    return cursor.rowcount > 0


def is_member(conn: sqlite3.Connection, channel: str) -> bool:
    """Whether this node has joined *channel*."""
    name = clean_channel_name(channel)
    row = conn.execute(
        "SELECT channel FROM memberships WHERE channel = ?", (name,)
    ).fetchone()
    return row is not None


def list_memberships(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Joined channels, most recently joined first, with message counts."""
    rows = conn.execute(
        "SELECT m.channel, m.joined_at, "
        "(SELECT COUNT(*) FROM messages WHERE channel = m.channel) AS messages "
        "FROM memberships m ORDER BY m.joined_at DESC, m.channel ASC"
    ).fetchall()
    return [
        {
            "channel": row["channel"],
            "joined_at": row["joined_at"],
            "messages": row["messages"],
        }
        for row in rows
    ]


# -- application layer -------------------------------------------------------


class ChannelApp:
    """Serve and synchronise channel messages for a running :class:`TauNode`."""

    def __init__(
        self,
        config: Any,
        identity: RNS.Identity,
        *,
        name: Optional[str] = None,
        db_path: Optional[Union[str, Path]] = None,
        on_message: Optional[Callable[[Dict[str, Any]], None]] = None,
        gossip: bool = True,
        gossip_fanout: int = GOSSIP_FANOUT,
        request_timeout: float = 15.0,
    ) -> None:
        self.config = config
        self.identity = identity
        self.name = sanitize_name(name or getattr(config, "name", "anonymous"))
        self.db_path = Path(db_path).expanduser() if db_path is not None else Path(
            getattr(config, "channels_db_path")
        )
        self.author = _author_from_identity(identity)
        self.on_message = on_message
        self.gossip = bool(gossip)
        self.gossip_fanout = max(1, int(gossip_fanout))
        self.request_timeout = float(request_timeout)
        self.node: Any = None
        self._gossip_slots = threading.BoundedSemaphore(4)
        self._closed = False

    def _blocked(self, author: str) -> bool:
        """Whether *author* is on this node's block-list, right now.

        The running node's list when attached — the console replaces
        it in place when the owner blocks someone, so a block takes
        effect without a restart.  Otherwise the list is re-read from
        disk on every check, never cached: a snapshot taken at startup
        would keep admitting an author blocked while the service runs.
        A list that cannot be read fails closed.
        """
        node = self.node
        blocklist = getattr(node, "blocklist", None) if node is not None else None
        if blocklist is not None:
            return blocklist.is_blocked(author)
        try:
            return load_blocklist(
                getattr(self.config, "blocklist_path", None)
            ).is_blocked(author)
        except Exception:
            return True

    def _blocked_authors(self) -> Optional[Sequence[str]]:
        """The current blocked authors, or ``None`` when fail-closed.

        ``None`` means the list cannot be trusted (corrupt or
        unreadable): callers must hide everything, not just the names
        they can see.
        """
        node = self.node
        blocklist = getattr(node, "blocklist", None) if node is not None else None
        if blocklist is not None:
            if blocklist.block_all:
                return None
            return sorted(blocklist.blocked)
        try:
            loaded = load_blocklist(getattr(self.config, "blocklist_path", None))
        except Exception:
            return None
        if loaded.block_all:
            return None
        return sorted(loaded.blocked)

    # -- lifecycle ---------------------------------------------------------

    def attach(self, node: Any) -> None:
        """Register channel request handlers on a started node."""
        if getattr(node, "destination", None) is None:
            raise ChannelError("cannot attach channels to a node that is not started")
        self.node = node
        node.add_request_handler(CHANNEL_FEED_PATH, self._handle_feed)
        node.add_request_handler(CHANNEL_POST_PATH, self._handle_post)
        log.info("Channel feed ready at %s", CHANNEL_FEED_PATH)

    def detach(self) -> None:
        self.node = None
        self._closed = True

    # -- local operations --------------------------------------------------

    def connect(self) -> sqlite3.Connection:
        return init_db(self.db_path)

    def join(self, channel: str) -> Dict[str, Any]:
        """Join *channel*, recording membership locally."""
        conn = self.connect()
        try:
            return join_channel(conn, channel)
        finally:
            conn.close()

    def leave(self, channel: str) -> bool:
        """Leave *channel*; cached messages are kept."""
        conn = self.connect()
        try:
            return leave_channel(conn, channel)
        finally:
            conn.close()

    def memberships(self) -> List[Dict[str, Any]]:
        """Joined channels, most recently joined first."""
        conn = self.connect()
        try:
            return list_memberships(conn)
        finally:
            conn.close()

    def history(
        self,
        channel: str,
        *,
        limit: int = DEFAULT_CHANNEL_LIMIT,
        before: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Stored messages for *channel*, newest first."""
        conn = self.connect()
        try:
            return channel_history(conn, channel, limit=limit, before=before)
        finally:
            conn.close()

    def topic(self, channel: str) -> Optional[Dict[str, Any]]:
        """Stored topic for *channel*, or ``None``.

        A topic from a blocked author never surfaces, and a
        block-list that cannot be read fails closed to no topic —
        the same rule as history.
        """
        conn = self.connect()
        try:
            record = get_topic(conn, channel)
        finally:
            conn.close()
        if record is None or self._blocked(record["author"]):
            return None
        return record

    def set_topic(
        self,
        channel: str,
        topic: str,
        *,
        publish: bool = False,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
    ) -> Dict[str, Any]:
        """Set the topic of a joined channel, gossiping it when asked.

        An empty topic clears the stored one.  Setting requires
        membership, like sending.
        """
        name = clean_channel_name(channel)
        conn = self.connect()
        try:
            if not is_member(conn, name):
                raise ChannelError(
                    f"not a member of channel {name!r}; join it first"
                )
            envelope = create_topic(conn, self.identity, name, topic)
        finally:
            conn.close()
        published = None
        if publish and self.node is not None:
            published = self.publish_topic(envelope, targets=targets, ttl=ttl)
        return {
            "channel": name,
            "topic": envelope,
            "cleared": not envelope["topic"],
            "published": published,
        }

    def send(
        self,
        channel: str,
        text: str,
        *,
        publish: bool = False,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
    ) -> Dict[str, Any]:
        """Create a message in a joined channel, gossiping it when asked.

        Sending requires membership: a channel you have not joined is
        not yours to post in.
        """
        name = clean_channel_name(channel)
        conn = self.connect()
        try:
            if not is_member(conn, name):
                raise ChannelError(
                    f"not a member of channel {name!r}; join it first"
                )
            message = create_message(
                conn, self.identity, name, text, name=self.name
            )
        finally:
            conn.close()
        if publish and self.node is not None:
            self.publish(message, targets=targets, ttl=ttl)
        return message

    # -- sync --------------------------------------------------------------

    def sync(
        self,
        node: Any,
        channel: str,
        *,
        targets: Optional[Sequence[Any]] = None,
        limit: int = DEFAULT_CHANNEL_LIMIT,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Pull missing history for *channel* from peers; return a summary."""
        name = clean_channel_name(channel)
        if node is not self.node:
            self.node = node
        peers = list(targets) if targets is not None else list(node.registry.all())
        peers = [peer for peer in peers if getattr(peer, "hexhash", "") != self.author]
        if not peers:
            return {
                "channel": name,
                "peers": 0,
                "stored": 0,
                "duplicates": 0,
                "skipped": 0,
                "topic_updated": False,
                "errors": [],
            }

        page_limit = max(1, min(int(limit), MAX_CHANNEL_HISTORY))
        request_timeout = float(timeout if timeout is not None else self.request_timeout)
        conn = self.connect()
        stored = 0
        duplicates = 0
        skipped = 0
        topic_updated = False
        errors: List[Dict[str, str]] = []
        try:
            for peer in peers:
                before: Optional[float] = None
                before_hash: Optional[str] = None
                for _ in range(MAX_SYNC_PAGES):
                    payload = encode(
                        {
                            "channel": name,
                            "before": before,
                            "before_hash": before_hash,
                            "limit": page_limit,
                        },
                        max_bytes=MAX_REQUEST_BYTES,
                    )
                    try:
                        response = node.request(
                            peer,
                            CHANNEL_FEED_PATH,
                            payload,
                            timeout=request_timeout,
                        )
                    except Exception as exc:
                        errors.append(
                            {
                                "peer": getattr(peer, "hexhash", "?"),
                                "error": str(exc),
                            }
                        )
                        break
                    messages = response.get("messages")
                    # The feed carries the channel's current topic
                    # alongside history, so a topic reaches a node
                    # even when the room has no messages to page
                    # through.  A bad topic is recorded like a bad
                    # message, never allowed to abort the sync.
                    raw_topic = response.get("topic")
                    if isinstance(raw_topic, dict):
                        if self._blocked(str(raw_topic.get("author", ""))):
                            skipped += 1
                        else:
                            try:
                                _, topic_changed = store_topic(conn, raw_topic)
                            except (ChannelError, ProtocolError) as exc:
                                errors.append(
                                    {
                                        "peer": getattr(peer, "hexhash", "?"),
                                        "error": str(exc),
                                    }
                                )
                            else:
                                if topic_changed:
                                    topic_updated = True
                    if not isinstance(messages, list) or not messages:
                        break
                    # Entries arrive from an untrusted peer: a bad one
                    # is recorded, never allowed to abort the peers
                    # and channels still to sync.
                    for raw in messages:
                        if not isinstance(raw, dict):
                            errors.append(
                                {
                                    "peer": getattr(peer, "hexhash", "?"),
                                    "error": "feed entry is not an object",
                                }
                            )
                            continue
                        if self._blocked(str(raw.get("author", ""))):
                            skipped += 1
                            continue
                        try:
                            _, is_new = store_message(conn, raw)
                        except (ChannelError, ProtocolError) as exc:
                            errors.append(
                                {
                                    "peer": getattr(peer, "hexhash", "?"),
                                    "error": str(exc),
                                }
                            )
                            continue
                        if is_new:
                            stored += 1
                        else:
                            duplicates += 1
                    # Page backwards from the last message actually
                    # received — its (timestamp, hash) cursor, not the
                    # page length: a trimmed page can be short while
                    # older history still exists, and a page can end
                    # inside a timestamp tie.  Only valid entries
                    # count as a cursor; a page of nothing usable
                    # ends the walk for this peer.  Each entry is
                    # validated on its own: a peer timestamp too large
                    # for a double (or a malformed hash) must not
                    # abort the walk — such an entry was already
                    # recorded (or skipped as blocked) above.
                    stamps: List[Tuple[float, str]] = []
                    for raw in messages:
                        if not isinstance(raw, dict):
                            continue
                        try:
                            stamp = _require_timestamp(
                                raw.get("created_at"), "created_at"
                            )
                            digest = _require_hex(
                                raw.get("hash"), HASH_HEX_LENGTH, "hash"
                            )
                        except (ChannelError, OverflowError):
                            continue
                        stamps.append((stamp, digest))
                    if not stamps:
                        break
                    before, before_hash = min(stamps)
                    if not response.get("has_more"):
                        break
        finally:
            conn.close()

        return {
            "channel": name,
            "peers": len(peers),
            "stored": stored,
            "duplicates": duplicates,
            "skipped": skipped,
            "topic_updated": topic_updated,
            "errors": errors,
        }

    def sync_all(
        self,
        node: Any,
        *,
        targets: Optional[Sequence[Any]] = None,
        limit: int = DEFAULT_CHANNEL_LIMIT,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Pull history for every joined channel; return a per-channel summary."""
        summaries = {}
        total_stored = 0
        for membership in self.memberships():
            summary = self.sync(
                node,
                membership["channel"],
                targets=targets,
                limit=limit,
                timeout=timeout,
            )
            summaries[membership["channel"]] = summary
            total_stored += summary["stored"]
        return {"channels": summaries, "stored": total_stored}

    def _publish_payload(
        self,
        payload: Dict[str, Any],
        *,
        targets: Optional[Sequence[Any]] = None,
        exclude: Optional[str] = None,
        fanout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Push a post payload to peers; return delivery counts."""
        node = self.node
        if node is None:
            raise ChannelError("channels are not attached to a running node")
        peers = list(targets) if targets is not None else list(node.registry.all())
        peers = [
            peer
            for peer in peers
            if getattr(peer, "hexhash", "") not in (self.author, exclude)
        ]
        if fanout is not None and len(peers) > max(1, int(fanout)):
            peers = random.sample(peers, max(1, int(fanout)))
        encoded = encode(payload, max_bytes=MAX_REQUEST_BYTES)
        delivered = 0
        errors: List[Dict[str, str]] = []
        for peer in peers:
            try:
                node.request(
                    peer, CHANNEL_POST_PATH, encoded, timeout=self.request_timeout
                )
                delivered += 1
            except Exception as exc:
                errors.append(
                    {"peer": getattr(peer, "hexhash", "?"), "error": str(exc)}
                )
        return {"delivered": delivered, "peers": len(peers), "errors": errors}

    def publish(
        self,
        message: Dict[str, Any],
        *,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
        exclude: Optional[str] = None,
        fanout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Push a channel message to peers; return delivery counts."""
        return self._publish_payload(
            {"message": message, "ttl": max(0, min(int(ttl), MAX_GOSSIP_TTL))},
            targets=targets,
            exclude=exclude,
            fanout=fanout,
        )

    def publish_topic(
        self,
        topic: Dict[str, Any],
        *,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
        exclude: Optional[str] = None,
        fanout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Push a channel topic to peers; return delivery counts."""
        return self._publish_payload(
            {"topic": topic, "ttl": max(0, min(int(ttl), MAX_GOSSIP_TTL))},
            targets=targets,
            exclude=exclude,
            fanout=fanout,
        )

    # -- request handlers --------------------------------------------------

    def _caller_hash(self, remote_identity: Optional[RNS.Identity]) -> Optional[str]:
        if remote_identity is None:
            return None
        try:
            return _author_from_identity(remote_identity)
        except Exception:  # pragma: no cover - defensive
            return None

    def _handle_feed(
        self,
        path: str,
        data: Any,
        request_id: bytes,
        remote_identity: Optional[RNS.Identity],
        requested_at: float,
    ) -> bytes:
        try:
            request = decode(data, max_bytes=MAX_REQUEST_BYTES)
            channel = clean_channel_name(request.get("channel"))
            before = request.get("before")
            if before is not None:
                before = _require_timestamp(before, "before")
            before_hash = request.get("before_hash")
            if before_hash is not None:
                before_hash = _require_hex(before_hash, HASH_HEX_LENGTH, "before_hash")
            limit = request.get("limit", DEFAULT_CHANNEL_LIMIT)
            if isinstance(limit, bool) or not isinstance(limit, int):
                limit = DEFAULT_CHANNEL_LIMIT

            # Blocked authors are not served to peers either: a block
            # placed after their messages were cached must stop them
            # travelling further, not just hide them locally.  A
            # block-list that fails closed serves nothing at all.
            excluded = self._blocked_authors()
            if excluded is None:
                messages: List[Dict[str, Any]] = []
                has_more = False
            else:
                conn = self.connect()
                try:
                    messages, has_more = channel_history_page(
                        conn,
                        channel,
                        limit=limit,
                        before=before,
                        before_hash=before_hash,
                        exclude_authors=excluded,
                    )
                finally:
                    conn.close()
        except (ProtocolError, ChannelError) as exc:
            return response_error("invalid_feed_request", str(exc))
        # The channel's current topic rides the feed with history, so
        # a syncing peer learns it without a separate round trip.  A
        # topic from a blocked author is not served, and a block-list
        # that fails closed serves no topic at all.
        topic: Optional[Dict[str, Any]] = None
        if excluded is not None:
            conn = self.connect()
            try:
                record = get_topic(conn, channel)
            finally:
                conn.close()
            if record is not None and record["author"] not in excluded:
                topic = record
        while True:
            try:
                return response_ok(
                    messages=messages,
                    count=len(messages),
                    channel=channel,
                    has_more=has_more,
                    topic=topic,
                )
            except ProtocolError as exc:
                if len(messages) <= 1:
                    return response_error("feed_too_large", str(exc))
                messages = messages[:-1]
                # The page was shortened to fit: older messages still
                # exist past it, so say so instead of letting the
                # requester mistake a short page for the end.
                has_more = True

    def _handle_post(
        self,
        path: str,
        data: Any,
        request_id: bytes,
        remote_identity: Optional[RNS.Identity],
        requested_at: float,
    ) -> bytes:
        try:
            request = decode(data, max_bytes=MAX_REQUEST_BYTES)
            ttl = request.get("ttl", 0)
            if isinstance(ttl, bool) or not isinstance(ttl, int):
                ttl = 0
            ttl = max(0, min(ttl, MAX_GOSSIP_TTL))

            raw_topic = request.get("topic")
            if raw_topic is not None:
                if not isinstance(raw_topic, dict):
                    raise ChannelError("topic must be a JSON object")
                verified_topic = verify_topic(raw_topic)
                if self._blocked(verified_topic["author"]):
                    log.info(
                        "Channel topic from blocked %s dropped",
                        verified_topic["author"][:8],
                    )
                    return response_error(
                        "not_admitted",
                        "author is blocked on this node",
                    )
                conn = self.connect()
                try:
                    record, changed = store_topic(conn, raw_topic)
                finally:
                    conn.close()
                if changed:
                    if record is not None:
                        log.info(
                            "Channel #%s topic set by %s: %s",
                            verified_topic["channel"],
                            verified_topic["author"][:8],
                            verified_topic["topic"],
                        )
                    else:
                        log.info(
                            "Channel #%s topic cleared by %s",
                            verified_topic["channel"],
                            verified_topic["author"][:8],
                        )
                    if self.gossip and ttl > 0:
                        self._spawn_gossip_topic(
                            verified_topic,
                            ttl=ttl - 1,
                            exclude=self._caller_hash(remote_identity),
                        )
                return response_ok(
                    topic=verified_topic["channel"],
                    updated=changed,
                    server=self.name,
                    node=getattr(self.node, "destination_hexhash", None),
                )

            raw_message = request.get("message")
            if not isinstance(raw_message, dict):
                raise ChannelError("message must be a JSON object")

            verified = verify_message(raw_message)
            if self._blocked(verified["author"]):
                log.info(
                    "Channel message from blocked %s dropped",
                    verified["author"][:8],
                )
                return response_error(
                    "not_admitted",
                    "author is blocked on this node",
                )
            conn = self.connect()
            try:
                record, is_new = store_message(conn, raw_message)
            finally:
                conn.close()

            if is_new:
                log.info(
                    "Channel #%s message from %s (%s): %s",
                    record.get("channel", "?"),
                    record.get("name", "unknown"),
                    record.get("author", "")[:8],
                    record.get("text", ""),
                )
                if self.on_message is not None:
                    try:
                        self.on_message(record)
                    except Exception:  # pragma: no cover - user callback
                        log.warning("Channel on_message callback failed", exc_info=True)
                if self.gossip and ttl > 0:
                    self._spawn_gossip(
                        record,
                        ttl=ttl - 1,
                        exclude=self._caller_hash(remote_identity),
                    )
            return response_ok(
                stored=record["hash"],
                new=is_new,
                server=self.name,
                node=getattr(self.node, "destination_hexhash", None),
            )
        except (ProtocolError, ChannelError) as exc:
            return response_error("invalid_post", str(exc))

    def _spawn_gossip(
        self, message: Dict[str, Any], *, ttl: int, exclude: Optional[str]
    ) -> None:
        if not self._gossip_slots.acquire(blocking=False):
            log.debug("Skipping channel gossip: too many pending relays")
            return

        def run() -> None:
            try:
                if self.node is not None and not self._closed:
                    self.publish(
                        message,
                        ttl=ttl,
                        exclude=exclude,
                        fanout=self.gossip_fanout,
                    )
            except Exception:  # pragma: no cover - network dependent
                log.debug("Channel gossip failed", exc_info=True)
            finally:
                self._gossip_slots.release()

        threading.Thread(target=run, name="tau-channel-gossip", daemon=True).start()

    def _spawn_gossip_topic(
        self, topic: Dict[str, Any], *, ttl: int, exclude: Optional[str]
    ) -> None:
        if not self._gossip_slots.acquire(blocking=False):
            log.debug("Skipping channel topic gossip: too many pending relays")
            return

        def run() -> None:
            try:
                if self.node is not None and not self._closed:
                    self.publish_topic(
                        topic,
                        ttl=ttl,
                        exclude=exclude,
                        fanout=self.gossip_fanout,
                    )
            except Exception:  # pragma: no cover - network dependent
                log.debug("Channel topic gossip failed", exc_info=True)
            finally:
                self._gossip_slots.release()

        threading.Thread(target=run, name="tau-channel-gossip", daemon=True).start()
