"""Tan: a federated social feed carried by tau nodes.

Tan stores posts in SQLite and chains each author's posts together by hash.
A post is the signed record::

    {
      "v": 1,
      "author": "<32 hex destination hash>",
      "key": "<128 hex public key>",
      "name": "alice",
      "text": "hello mesh",
      "created_at": 1760000000.0,
      "prev": "<64 hex hash of this author's previous post, or ''>",
      "hash": "<64 hex sha256(prev || canonical(post body))>",
      "sig": "<128 hex Ed25519 signature over the hash>"
    }

Reactions are chain entries of the same shape, written by the *reactor* onto
their own chain with two extra signed fields::

    {
      ...same fields, with "text": "",
      "kind": "like" | "unlike",
      "target": "<64 hex hash of the post being reacted to>"
    }

A like is therefore committed like any other envelope: signed, hash-chained,
gossipped and synced.  A post's like count is derived, never stored: for each
reactor, their latest like/unlike entry for that post decides whether their
like currently stands.

A reply is a targeted entry of the same shape, but it carries text: it is
written by the *replier* onto their own chain with ``kind`` ``"reply"`` and
``target`` set to the parent post's hash, so the parent link is covered by
the reply's signature and cannot be swapped by a relay.  Replies are
gossipped and synced like posts, appear in the timeline and feed, can be
liked, and can be deleted by their own author.  A reply whose parent has
not arrived yet is stored anyway; the timeline shows it without a parent
snippet until the parent is cached.

An edit is a targeted entry of the same shape, written by the *author*
onto their own chain with ``kind`` ``"edit"``, ``target`` set to the
post (or reply) being corrected, and the replacement text carried like a
post's.  Only the post's own author may edit it — an edit from anyone
else is stored, so the editor's chain stays complete, but is inert and
never changes what the post shows.  The latest edit wins, and a delete
still hides the post (and its edits) from display.  Like replies, an
edit is a new entry kind: a peer running a version that predates edits
rejects it, leaving that author's later entries unlinked until it
upgrades and resyncs.

What gets committed at all is the owner's decision: a ruleset (see
``tau.tangents.rules``) can allow or block authors by identity, admit
only followed authors, or deny by default.  It is consulted for
remote entries — pushes and sync imports — after signature
verification; locally created entries are never filtered, and with no
ruleset configured every well-signed entry is admitted.  Entry kinds
are never an admission criterion — filtering them at storage time
would break hash-chain continuity — so the ruleset's ``kinds``
setting only controls what the node's timeline views display.

The hash chain makes edits and reordering detectable, while the signature makes
a post verifiable even when it is relayed by a node that does not own the
author's identity.  Nodes can therefore cache and re-serve posts for each other.

The network protocol is two request paths served on the same Reticulum
destination as the rest of tau:

    /tan/feed   {"author": "...", "after_seq": 0, "limit": 20}
                {"all": true, "before": 1760000000.0, "limit": 20}
    /tan/post   {"post": {...}, "ttl": 2}

``/tan/feed`` reads cached posts.  ``/tan/post`` accepts a signed post and, when
the post is new, gossips it to a small number of other peers.  Pulling a feed
and pushing a post are deliberately separate: a node can follow an author that
is offline as long as any peer still has that author's posts cached.
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
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple, Union

import RNS

from ..identity import load_public_key
from ..protocol import (
    APP_NAME,
    ASPECT,
    MAX_MESSAGE_CHARS,
    MAX_REQUEST_BYTES,
    ProtocolError,
    decode,
    encode,
    normalize_message,
    response_error,
    response_ok,
    sanitize_name,
)
from ..util import ensure_dir
from .rules import RulesError, Ruleset, load_ruleset

log = logging.getLogger(__name__)

TAN_VERSION = 1
TAN_FEED_PATH = "/tan/feed"
TAN_POST_PATH = "/tan/post"

AUTHOR_HEX_LENGTH = 32
HASH_HEX_LENGTH = 64
KEY_HEX_LENGTH = 128
SIG_HEX_LENGTH = 128

MAX_POST_CHARS = MAX_MESSAGE_CHARS
MAX_FEED_POSTS = 20
DEFAULT_FEED_LIMIT = 20
DEFAULT_SUGGEST_LIMIT = 5
# Weight of one distinct followed author engaging with a candidate, in
# suggestion score points; one like on the candidate's posts is worth 1.
SUGGEST_FOLLOWED_WEIGHT = 2
MAX_FOLLOWS = 512
MAX_MUTED = 512
MAX_MUTED_KEYWORDS = 256
MAX_KEYWORD_LENGTH = 100
MAX_SAVED_POSTS = 512
MAX_GOSSIP_TTL = 3
GOSSIP_FANOUT = 3
MAX_SYNC_PAGES = 32

# Fields covered by the post hash.  ``prev`` is deliberately not in this tuple:
# it is prefixed to the canonical body, exactly as the original scaffold
# described ("cat the previous hash to the new message and hash it").
HASH_FIELDS = ("v", "author", "key", "name", "text", "created_at")

KIND_POST = "post"
KIND_LIKE = "like"
KIND_UNLIKE = "unlike"
KIND_DELETE = "delete"
KIND_REPLY = "reply"
KIND_REPOST = "repost"
KIND_EDIT = "edit"
KIND_QUOTE = "quote"
REACTION_KINDS = (KIND_LIKE, KIND_UNLIKE)
# Reactions, deletes and reposts carry no text; replies, edits and quotes
# carry text but are still targeted entries, so their parent link is
# signed like a reaction's.
TEXTLESS_KINDS = REACTION_KINDS + (KIND_DELETE, KIND_REPOST)
# Targeted entries (reactions, deletes, replies, edits, quotes and
# reposts) point at another post.
TARGETED_KINDS = TEXTLESS_KINDS + (KIND_REPLY, KIND_EDIT, KIND_QUOTE)
ENTRY_KINDS = (KIND_POST,) + TARGETED_KINDS
# Kinds that appear as timeline items: top-level posts, replies, reposts
# and quotes.  A repost shows the original post's text with attribution;
# a quote is a post-like entry: it carries its own text, can be replied
# to and can be deleted.
TIMELINE_KINDS = (KIND_POST, KIND_REPLY, KIND_REPOST, KIND_QUOTE)
# Kinds a reply or a repost may target: a post or a reply.  A repost is
# not replyable — it carries no text of its own, so a reply to it would
# have no parent snippet to show — and neither kind targets reactions
# or deletes.
REPLYABLE_KINDS = (KIND_POST, KIND_REPLY)
# Kinds a quote may target.  Quoting a quote would nest embeds, so only
# posts and replies are quotable and the embedded original stays one
# level deep.
QUOTABLE_KINDS = (KIND_POST, KIND_REPLY)
# SQL fragment listing the timeline kinds, for the queries that filter
# on kind directly.  Built from TIMELINE_KINDS so the two cannot drift.
TIMELINE_KINDS_SQL = ", ".join(f"'{kind}'" for kind in TIMELINE_KINDS)
# Same treatment for the kinds a quote may target.
QUOTABLE_KINDS_SQL = ", ".join(f"'{kind}'" for kind in QUOTABLE_KINDS)

# Targeted envelopes sign two extra fields on top of the post fields, so a
# reaction's or deletion's target cannot be swapped without invalidating
# its signature.
TARGETED_HASH_FIELDS = HASH_FIELDS + ("kind", "target")
REACTION_HASH_FIELDS = TARGETED_HASH_FIELDS

# Column order used by every SELECT so rows can be converted consistently.
_POST_COLUMNS = (
    "v",
    "hash",
    "author",
    "prev",
    "key",
    "name",
    "text",
    "created_at",
    "received_at",
    "sig",
    "seq",
    "linked",
    "kind",
    "target",
)


class TanError(ValueError):
    """Base class for Tan validation and storage errors."""


class PostError(TanError):
    """Raised when a post is malformed, unsigned or has a broken chain."""


# -- small validation helpers ---------------------------------------------


def _require_hex(value: Any, length: int, field: str) -> str:
    if not isinstance(value, str):
        raise PostError(f"{field} must be a hex string")
    text = value.strip().lower()
    if len(text) != length:
        raise PostError(f"{field} must be {length} hex characters")
    try:
        bytes.fromhex(text)
    except ValueError as exc:
        raise PostError(f"{field} is not valid hex") from exc
    return text


def _require_timestamp(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PostError(f"{field} must be a number")
    timestamp = float(value)
    if not math.isfinite(timestamp) or timestamp < 0:
        raise PostError(f"{field} must be a finite, non-negative timestamp")
    return timestamp


def _hash_fields_for(mesg: Dict[str, Any]) -> Tuple[str, ...]:
    """Fields covered by the hash for *mesg*, based on its entry kind."""
    if mesg.get("kind") in TARGETED_KINDS:
        return TARGETED_HASH_FIELDS
    return HASH_FIELDS


def _canonical_body(post: Dict[str, Any]) -> bytes:
    body = {field: post[field] for field in _hash_fields_for(post)}
    return json.dumps(
        body,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def _canonical_post(post: Dict[str, Any]) -> Dict[str, Any]:
    """Return the public, canonical representation of a chain entry."""
    return {
        "v": post["v"],
        "author": post["author"],
        "key": post["key"],
        "name": post["name"],
        "text": post["text"],
        "created_at": post["created_at"],
        "prev": post["prev"],
        "hash": post["hash"],
        "sig": post["sig"],
        "kind": post.get("kind") or KIND_POST,
        "target": post.get("target") or "",
    }


def hash(mesg: Dict[str, Any], previous: Optional[str] = None) -> str:
    """Hash *mesg*, prefixing the previous post hash.

    This keeps the scaffold's shape: ``hash(mesg)`` reads ``prev`` from the
    message, while ``hash(mesg, previous)`` lets a caller pass the previous
    hash explicitly.  The result is lowercase hex sha256, which is also the
    post's identifier.
    """
    if not isinstance(mesg, dict):
        raise PostError("a post must be a JSON object")

    if previous is None:
        previous = mesg.get("prev", "")
    if previous is None:
        previous = ""
    if not isinstance(previous, str):
        raise PostError("prev must be a string")
    previous = previous.strip().lower()
    if previous and len(previous) != HASH_HEX_LENGTH:
        raise PostError(f"prev must be {HASH_HEX_LENGTH} hex characters")

    missing = [field for field in _hash_fields_for(mesg) if field not in mesg]
    if missing:
        raise PostError(f"post is missing fields: {', '.join(missing)}")

    digest = hashlib.sha256()
    digest.update(previous.encode("ascii"))
    digest.update(_canonical_body(mesg))
    return digest.hexdigest()


def _normalize_post(mesg: Dict[str, Any], *, now: Optional[float] = None) -> Dict[str, Any]:
    """Validate and normalise the signed part of a post."""
    if not isinstance(mesg, dict):
        raise PostError("post must be a JSON object")

    version = mesg.get("v", TAN_VERSION)
    if isinstance(version, bool) or version != TAN_VERSION:
        raise PostError(f"unsupported post version: {version!r}")

    author = _require_hex(mesg.get("author"), AUTHOR_HEX_LENGTH, "author")
    key = _require_hex(mesg.get("key"), KEY_HEX_LENGTH, "key")
    prev = mesg.get("prev", "")
    if prev is None:
        prev = ""
    if not isinstance(prev, str):
        raise PostError("prev must be a string")
    prev = prev.strip().lower()
    if prev:
        _require_hex(prev, HASH_HEX_LENGTH, "prev")

    kind = mesg.get("kind", KIND_POST)
    if kind not in ENTRY_KINDS:
        raise PostError(f"unsupported entry kind: {kind!r}")

    raw_text = mesg.get("text")
    if kind in TEXTLESS_KINDS:
        # Reactions, deletes and reposts carry no text; normalize_message would also
        # reject the empty string it requires here.
        if raw_text not in (None, ""):
            raise PostError(f"{kind} entries cannot carry text")
        text = ""
    else:
        text = normalize_message(raw_text)
        if text != raw_text:
            # Posts are hashed before they reach us.  Silently cleaning the text
            # would produce a hash that no longer matches what the author signed.
            raise PostError("post text is not normalised")

    raw_name = mesg.get("name", "")
    name = sanitize_name(raw_name)
    if name != raw_name:
        raise PostError("post name is not normalised")

    created_at = _require_timestamp(mesg.get("created_at"), "created_at")
    if now is not None and created_at > float(now) + 86400.0:
        raise PostError("post timestamp is too far in the future")

    raw_target = mesg.get("target", "")
    if raw_target is None:
        raw_target = ""
    if kind in TARGETED_KINDS:
        target = _require_hex(raw_target, HASH_HEX_LENGTH, "target")
    else:
        if raw_target not in ("", None):
            raise PostError("posts cannot carry a reaction target")
        target = ""

    post = {
        "v": TAN_VERSION,
        "author": author,
        "key": key,
        "name": name,
        "text": text,
        "created_at": created_at,
        "prev": prev,
        "kind": kind,
        "target": target,
    }
    _check_wire_size(post)
    return post


def _check_wire_size(post: Dict[str, Any]) -> None:
    """Reject an entry whose publish envelope exceeds the request budget.

    The character limit alone is not enough: 2,000 four-byte characters
    pass it but overflow the 8 KiB ``/tan/post`` request once encoded.
    Such a post must be rejected *before* it is stored — otherwise it
    sits as the author's chain head, undeliverable, and retrying the
    post forks a duplicate.  Hash and signature are fixed-length hex,
    so placeholders give the exact encoded size of the real envelope.
    """
    wire = _canonical_post(
        {**post, "hash": "0" * HASH_HEX_LENGTH, "sig": "0" * SIG_HEX_LENGTH}
    )
    try:
        encode({"post": wire, "ttl": MAX_GOSSIP_TTL}, max_bytes=MAX_REQUEST_BYTES)
    except ProtocolError as exc:
        raise PostError(
            f"entry exceeds the {MAX_REQUEST_BYTES} byte encoded limit "
            "for a Tan request; shorten the post"
        ) from exc


def _identity_from_post(post: Dict[str, Any]) -> RNS.Identity:
    try:
        identity = RNS.Identity(create_keys=False)
        if not load_public_key(identity, bytes.fromhex(post["key"])):
            raise ValueError("could not load public key")
    except Exception as exc:  # pragma: no cover - depends on RNS internals
        raise PostError(f"invalid public key: {exc}") from exc
    return identity


def _author_from_identity(identity: RNS.Identity) -> str:
    return RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()


def verify_post(mesg: Dict[str, Any], *, now: Optional[float] = None) -> Dict[str, Any]:
    """Validate a complete post, including its signature and hash."""
    post = _normalize_post(mesg, now=now)

    digest = mesg.get("hash")
    expected = hash({**post, "prev": post["prev"]}, previous=post["prev"])
    if not isinstance(digest, str) or digest.strip().lower() != expected:
        raise PostError("post hash does not match its contents")
    post["hash"] = expected

    signature = _require_hex(mesg.get("sig"), SIG_HEX_LENGTH, "sig")
    post["sig"] = signature

    identity = _identity_from_post(post)
    author = _author_from_identity(identity)
    if author != post["author"]:
        raise PostError("public key does not match the post author")
    try:
        valid = identity.validate(bytes.fromhex(signature), bytes.fromhex(expected))
    except Exception as exc:  # pragma: no cover - depends on RNS internals
        raise PostError(f"could not check signature: {exc}") from exc
    if not valid:
        raise PostError("post signature is invalid")
    return post


# -- storage ---------------------------------------------------------------


_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS posts (
    v INTEGER NOT NULL DEFAULT 1,
    hash TEXT PRIMARY KEY,
    author TEXT NOT NULL,
    prev TEXT NOT NULL DEFAULT '',
    key TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    text TEXT NOT NULL,
    created_at REAL NOT NULL,
    received_at REAL NOT NULL,
    sig TEXT NOT NULL,
    seq INTEGER NOT NULL DEFAULT 0,
    linked INTEGER NOT NULL DEFAULT 0,
    kind TEXT NOT NULL DEFAULT 'post',
    target TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS posts_author_seq ON posts(author, seq);
CREATE INDEX IF NOT EXISTS posts_author_created ON posts(author, created_at DESC);
CREATE INDEX IF NOT EXISTS posts_created ON posts(created_at DESC);
CREATE INDEX IF NOT EXISTS posts_prev ON posts(prev);

CREATE TABLE IF NOT EXISTS heads (
    author TEXT PRIMARY KEY,
    hash TEXT NOT NULL,
    seq INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS follows (
    author TEXT PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    added_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS muted_keywords (
    keyword TEXT PRIMARY KEY,
    display TEXT NOT NULL,
    added_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS muted_authors (
    author TEXT PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    muted_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS saved_posts (
    hash TEXT PRIMARY KEY,
    saved_at REAL NOT NULL
);
"""


def init_db(path: Optional[Union[str, Path]] = None) -> sqlite3.Connection:
    """Initialises db storage using SQLite if needed. Returns the DB object."""
    if path is None:
        target = ":memory:"
    else:
        target = str(Path(path).expanduser())
        if target != ":memory:":
            ensure_dir(Path(target).parent)
    conn = sqlite3.connect(target, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    if target != ":memory:":
        try:
            conn.execute("PRAGMA journal_mode = WAL")
        except sqlite3.DatabaseError:  # pragma: no cover - filesystem dependent
            log.debug("Could not enable WAL for %s", target, exc_info=True)
        try:
            import os

            os.chmod(target, 0o600)
        except OSError:  # pragma: no cover - best effort
            pass
    conn.executescript(_SCHEMA)
    columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(posts)").fetchall()
    }
    if columns and "v" not in columns:  # migrate an early scaffold database
        conn.execute("ALTER TABLE posts ADD COLUMN v INTEGER NOT NULL DEFAULT 1")
    if columns and "kind" not in columns:  # migrate a pre-reactions database
        conn.execute(
            "ALTER TABLE posts ADD COLUMN kind TEXT NOT NULL DEFAULT 'post'"
        )
        conn.execute(
            "ALTER TABLE posts ADD COLUMN target TEXT NOT NULL DEFAULT ''"
        )
    # Created after the migration above: the index references columns a
    # pre-reactions database does not have yet when _SCHEMA first runs.
    conn.execute("CREATE INDEX IF NOT EXISTS posts_target ON posts(target)")
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES('version', ?)",
        (str(TAN_VERSION),),
    )
    conn.commit()
    return conn


def open_readonly_db(path: Union[str, Path]) -> sqlite3.Connection:
    """Open an existing Tan database read-only, without schema or metadata writes.

    The store must already have the current schema: a store created by an
    older tau (before reactions) needs one migrating open first — any other
    ``tau tan`` command does that — so a stale schema raises TanError
    instead of failing mid-query on a missing column.
    """
    target = Path(path).expanduser().resolve()
    conn = sqlite3.connect(f"{target.as_uri()}?mode=ro", uri=True, timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        columns = {
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(posts)").fetchall()
        }
        tables = {
            str(row["name"])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    except sqlite3.Error as exc:
        conn.close()
        raise TanError(f"could not read the Tan store: {exc}") from exc
    required = {"author", "name", "created_at", "seq", "kind", "target", "linked"}
    if not required <= columns or "follows" not in tables:
        conn.close()
        raise TanError(
            "Tan store schema is outdated; run `tau tan timeline` once to migrate it"
        )
    return conn


def open_db_readonly(path: Union[str, Path]) -> sqlite3.Connection:
    """Open an existing Tan database read-only, without migrating it.

    Unlike :func:`init_db` this never creates, chmods, migrates or writes
    the file, so it is safe to use while a node is serving it.
    """
    uri = Path(path).expanduser().resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn
    return conn


def _row_to_post(row: sqlite3.Row) -> Dict[str, Any]:
    return _canonical_post(dict(row))


def _row_to_post_with_meta(row: sqlite3.Row) -> Dict[str, Any]:
    post = _row_to_post(row)
    post["seq"] = int(row["seq"])
    post["linked"] = bool(row["linked"])
    return post


def _fetch_post(conn: sqlite3.Connection, digest: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        f"SELECT {', '.join(_POST_COLUMNS)} FROM posts WHERE hash = ?",
        (digest,),
    ).fetchone()


def get_post(conn: sqlite3.Connection, digest: str) -> Optional[Dict[str, Any]]:
    """Return a stored post by hash, or ``None``."""
    _require_hex(digest, HASH_HEX_LENGTH, "hash")
    row = _fetch_post(conn, digest)
    return _row_to_post_with_meta(row) if row is not None else None


def head(conn: sqlite3.Connection, author: str) -> Optional[str]:
    """Return the hash of *author*'s current chain head, if any."""
    author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
    row = conn.execute("SELECT hash FROM heads WHERE author = ?", (author,)).fetchone()
    return str(row["hash"]) if row is not None else None


def max_seq(conn: sqlite3.Connection, author: str) -> int:
    """Return the highest linked sequence number known for *author*."""
    author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
    row = conn.execute(
        "SELECT COALESCE(MAX(seq), 0) AS seq FROM posts WHERE author = ? AND linked = 1",
        (author,),
    ).fetchone()
    return int(row["seq"]) if row is not None else 0


def _advance_head(
    conn: sqlite3.Connection, author: str, digest: str, seq: int, created_at: float
) -> None:
    row = conn.execute(
        "SELECT seq FROM heads WHERE author = ?", (author,)
    ).fetchone()
    if row is None or seq > int(row["seq"]):
        conn.execute(
            "INSERT INTO heads(author, hash, seq) VALUES(?, ?, ?) "
            "ON CONFLICT(author) DO UPDATE SET hash = excluded.hash, seq = excluded.seq",
            (author, digest, seq),
        )


def _promote_children(
    conn: sqlite3.Connection, author: str, parent_hash: str, parent_seq: int
) -> None:
    """Attach orphaned descendants once their parent becomes linked."""
    queue: List[Tuple[str, int]] = [(parent_hash, parent_seq)]
    while queue:
        parent, parent_sequence = queue.pop(0)
        children = conn.execute(
            f"SELECT {', '.join(_POST_COLUMNS)} FROM posts "
            "WHERE author = ? AND prev = ? AND linked = 0 ORDER BY created_at ASC",
            (author, parent),
        ).fetchall()
        for child in children:
            child_hash = str(child["hash"])
            child_seq = parent_sequence + 1
            conn.execute(
                "UPDATE posts SET linked = 1, seq = ? WHERE hash = ?",
                (child_seq, child_hash),
            )
            _advance_head(
                conn, author, child_hash, child_seq, float(child["created_at"])
            )
            queue.append((child_hash, child_seq))


def store_post(
    conn: sqlite3.Connection,
    mesg: Dict[str, Any],
    *,
    now: Optional[float] = None,
    verify: bool = True,
) -> Tuple[Dict[str, Any], bool]:
    """Validate and store a post, returning ``(post, is_new)``."""
    timestamp = float(now if now is not None else time.time())
    if verify:
        post = verify_post(mesg, now=timestamp)
    else:
        base = _normalize_post(mesg, now=timestamp)
        post = dict(base)
        post["hash"] = hash(base, previous=base["prev"])
        post["sig"] = _require_hex(mesg.get("sig"), SIG_HEX_LENGTH, "sig")

    digest = post["hash"]
    existing = _fetch_post(conn, digest)
    if existing is not None:
        return _canonical_post(dict(existing)), False

    author = post["author"]
    prev = post["prev"]
    linked = True
    seq = 1
    if prev:
        parent = _fetch_post(conn, prev)
        if parent is None:
            linked = False
            seq = 0
        else:
            if str(parent["author"]) != author:
                raise PostError("previous post belongs to a different author")
            if bool(parent["linked"]):
                seq = int(parent["seq"]) + 1
            else:
                linked = False
                seq = 0

    try:
        with conn:
            conn.execute(
                "INSERT INTO posts(v, hash, author, prev, key, name, text, created_at, "
                "received_at, sig, seq, linked, kind, target) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    TAN_VERSION,
                    digest,
                    author,
                    prev,
                    post["key"],
                    post["name"],
                    post["text"],
                    post["created_at"],
                    timestamp,
                    post["sig"],
                    seq,
                    1 if linked else 0,
                    post.get("kind") or KIND_POST,
                    post.get("target") or "",
                ),
            )
            if linked:
                _advance_head(conn, author, digest, seq, post["created_at"])
                _promote_children(conn, author, digest, seq)
    except sqlite3.IntegrityError:
        # Another thread or process inserted the same post first.
        existing = _fetch_post(conn, digest)
        if existing is None:
            raise
        return _canonical_post(dict(existing)), False

    record = get_post(conn, digest)
    if record is None:  # pragma: no cover - insert should guarantee this
        raise TanError("post disappeared after it was stored")
    return _canonical_post(record), True


def store(
    conn: sqlite3.Connection,
    mesg: Dict[str, Any],
    *,
    now: Optional[float] = None,
    verify: bool = True,
) -> Dict[str, Any]:
    """Validates and stores new mesg."""
    record, _ = store_post(conn, mesg, now=now, verify=verify)
    return record


def create_post(
    conn: sqlite3.Connection,
    identity: RNS.Identity,
    text: str,
    *,
    name: str = "anonymous",
    now: Optional[float] = None,
    previous: Optional[str] = None,
) -> Dict[str, Any]:
    """Create, sign and store a post owned by *identity*."""
    timestamp = round(float(now if now is not None else time.time()), 3)
    author = _author_from_identity(identity)
    clean_name = sanitize_name(name)
    clean_text = normalize_message(text)
    prev = previous
    if prev is None:
        prev = head(conn, author) or ""
    prev = prev.strip().lower()
    if prev:
        _require_hex(prev, HASH_HEX_LENGTH, "prev")

    body = {
        "v": TAN_VERSION,
        "author": author,
        "key": identity.get_public_key().hex(),
        "name": clean_name,
        "text": clean_text,
        "created_at": timestamp,
        "prev": prev,
    }
    digest = hash(body, previous=prev)
    mesg = dict(body)
    mesg["hash"] = digest
    mesg["sig"] = identity.sign(bytes.fromhex(digest)).hex()
    record, _ = store_post(conn, mesg, now=timestamp)
    return record


def create_reaction(
    conn: sqlite3.Connection,
    identity: RNS.Identity,
    target: str,
    *,
    liked: bool,
    name: str = "anonymous",
    now: Optional[float] = None,
    previous: Optional[str] = None,
) -> Dict[str, Any]:
    """Create, sign and store a like/unlike entry for the post *target*.

    The reaction is appended to the reactor's own chain, exactly like a
    post: ``liked=True`` writes a ``like`` entry, ``liked=False`` an
    ``unlike`` entry.  The reactor's latest entry for a target decides
    whether their like stands, so both operations are idempotent in
    effect and safe to repeat.
    """
    target = _require_hex(target, HASH_HEX_LENGTH, "target")
    timestamp = round(float(now if now is not None else time.time()), 3)
    author = _author_from_identity(identity)
    prev = previous
    if prev is None:
        prev = head(conn, author) or ""
    prev = prev.strip().lower()
    if prev:
        _require_hex(prev, HASH_HEX_LENGTH, "prev")

    body = {
        "v": TAN_VERSION,
        "author": author,
        "key": identity.get_public_key().hex(),
        "name": sanitize_name(name),
        "text": "",
        "created_at": timestamp,
        "prev": prev,
        "kind": KIND_LIKE if liked else KIND_UNLIKE,
        "target": target,
    }
    digest = hash(body, previous=prev)
    mesg = dict(body)
    mesg["hash"] = digest
    mesg["sig"] = identity.sign(bytes.fromhex(digest)).hex()
    record, _ = store_post(conn, mesg, now=timestamp)
    return record


def create_reply(
    conn: sqlite3.Connection,
    identity: RNS.Identity,
    target: str,
    text: str,
    *,
    name: str = "anonymous",
    now: Optional[float] = None,
    previous: Optional[str] = None,
) -> Dict[str, Any]:
    """Create, sign and store a reply to the post *target*.

    A reply is a targeted entry like a reaction: ``kind`` is ``reply``
    and ``target`` is the parent post's hash, so the parent link is
    signed — but unlike a reaction it carries text, like a post.  It is
    appended to the replier's own chain, exactly like a post, and is
    gossipped and synced through the same paths.  When the target is
    not cached locally, or is cached but has not linked yet (its chain
    is incomplete), this raises: a reply is written to a post the
    author has seen in full.  Replies received from peers for parents
    that have not arrived yet are still stored through the sync path
    (:func:`store_post`) and shown without a parent snippet until the
    parent arrives.
    """
    target = _require_hex(target, HASH_HEX_LENGTH, "target")
    timestamp = round(float(now if now is not None else time.time()), 3)
    author = _author_from_identity(identity)
    existing = get_post(conn, target)
    if existing is None:
        raise TanError(f"unknown post: {target!r}")
    if existing.get("kind", KIND_POST) not in REPLYABLE_KINDS + (KIND_QUOTE,):
        raise TanError("only posts, replies and quotes can be replied to")
    if not existing.get("linked"):
        raise TanError(
            "post has not linked yet; sync its chain before replying"
        )
    if _is_deleted(conn, existing):
        raise TanError("post has been deleted")
    clean_text = normalize_message(text)
    prev = previous
    if prev is None:
        prev = head(conn, author) or ""
    prev = prev.strip().lower()
    if prev:
        _require_hex(prev, HASH_HEX_LENGTH, "prev")

    body = {
        "v": TAN_VERSION,
        "author": author,
        "key": identity.get_public_key().hex(),
        "name": sanitize_name(name),
        "text": clean_text,
        "created_at": timestamp,
        "prev": prev,
        "kind": KIND_REPLY,
        "target": target,
    }
    digest = hash(body, previous=prev)
    mesg = dict(body)
    mesg["hash"] = digest
    mesg["sig"] = identity.sign(bytes.fromhex(digest)).hex()
    record, _ = store_post(conn, mesg, now=timestamp)
    return record


def create_quote(
    conn: sqlite3.Connection,
    identity: RNS.Identity,
    target: str,
    text: str,
    *,
    name: str = "anonymous",
    now: Optional[float] = None,
    previous: Optional[str] = None,
) -> Dict[str, Any]:
    """Create, sign and store a quote of the post *target*.

    A quote is a targeted entry like a reply: ``kind`` is ``quote``
    and ``target`` is the quoted post's hash, so the quoted link is
    signed — but unlike a reply it is a standalone timeline item: it
    carries the quoter's own comment as its text and the original is
    shown embedded beneath it.  It is appended to the quoter's own
    chain, exactly like a post, and is gossipped and synced through
    the same paths.  Only posts and replies can be quoted, so embeds
    stay one level deep; quoting your own post is allowed.  When the
    target is not cached locally, or is cached but has not linked yet
    (its chain is incomplete), this raises: a quote is written to a
    post the author has seen in full.  Quotes received from peers for
    parents that have not arrived yet are still stored through the
    sync path (:func:`store_post`) and shown without the embedded
    original until the parent arrives.
    """
    target = _require_hex(target, HASH_HEX_LENGTH, "target")
    timestamp = round(float(now if now is not None else time.time()), 3)
    author = _author_from_identity(identity)
    existing = get_post(conn, target)
    if existing is None:
        raise TanError(f"unknown post: {target!r}")
    if existing.get("kind", KIND_POST) not in QUOTABLE_KINDS:
        raise TanError("only posts and replies can be quoted")
    if not existing.get("linked"):
        raise TanError(
            "post has not linked yet; sync its chain before quoting"
        )
    if _is_deleted(conn, existing):
        raise TanError("post has been deleted")
    clean_text = normalize_message(text)
    prev = previous
    if prev is None:
        prev = head(conn, author) or ""
    prev = prev.strip().lower()
    if prev:
        _require_hex(prev, HASH_HEX_LENGTH, "prev")

    body = {
        "v": TAN_VERSION,
        "author": author,
        "key": identity.get_public_key().hex(),
        "name": sanitize_name(name),
        "text": clean_text,
        "created_at": timestamp,
        "prev": prev,
        "kind": KIND_QUOTE,
        "target": target,
    }
    digest = hash(body, previous=prev)
    mesg = dict(body)
    mesg["hash"] = digest
    mesg["sig"] = identity.sign(bytes.fromhex(digest)).hex()
    record, _ = store_post(conn, mesg, now=timestamp)
    return record


def create_delete(
    conn: sqlite3.Connection,
    identity: RNS.Identity,
    target: str,
    *,
    name: str = "anonymous",
    now: Optional[float] = None,
    previous: Optional[str] = None,
) -> Dict[str, Any]:
    """Create, sign and store a delete entry for the post *target*.

    The delete is appended to the deleter's own chain, exactly like a
    reaction: ``kind`` is ``delete`` and ``target`` is the post hash.
    Only the post's author can delete it.  When the target post is
    cached locally and belongs to someone else — or is not a post at
    all — this raises; a delete for a post that is not cached yet is
    stored anyway and stays inert until the post arrives and its
    author matches (see :func:`deleted_targets`).
    """
    target = _require_hex(target, HASH_HEX_LENGTH, "target")
    timestamp = round(float(now if now is not None else time.time()), 3)
    author = _author_from_identity(identity)
    existing = get_post(conn, target)
    if existing is not None:
        if existing.get("kind", KIND_POST) not in TIMELINE_KINDS:
            raise TanError("only posts, replies, reposts and quotes can be deleted")
        if existing["author"] != author:
            raise TanError("cannot delete a post you did not author")
        if _is_deleted(conn, existing):
            raise TanError("post has been deleted")
    prev = previous
    if prev is None:
        prev = head(conn, author) or ""
    prev = prev.strip().lower()
    if prev:
        _require_hex(prev, HASH_HEX_LENGTH, "prev")

    body = {
        "v": TAN_VERSION,
        "author": author,
        "key": identity.get_public_key().hex(),
        "name": sanitize_name(name),
        "text": "",
        "created_at": timestamp,
        "prev": prev,
        "kind": KIND_DELETE,
        "target": target,
    }
    digest = hash(body, previous=prev)
    mesg = dict(body)
    mesg["hash"] = digest
    mesg["sig"] = identity.sign(bytes.fromhex(digest)).hex()
    record, _ = store_post(conn, mesg, now=timestamp)
    return record


def create_repost(
    conn: sqlite3.Connection,
    identity: RNS.Identity,
    target: str,
    *,
    name: str = "anonymous",
    now: Optional[float] = None,
    previous: Optional[str] = None,
) -> Dict[str, Any]:
    """Create, sign and store a repost of the post *target*.

    A repost is a textless targeted entry like a reaction: ``kind`` is
    ``repost`` and ``target`` is the original post's hash, so the link
    is signed — but unlike a reaction it is a timeline item, so the
    reposter's followers see the original post in their timeline.  It
    is appended to the reposter's own chain, exactly like a post, and
    is gossipped and synced through the same paths.

    Only posts and replies can be reposted, and only when they are
    cached locally, linked, and not deleted — a repost is written to a
    post the author has seen in full.  You cannot repost your own post,
    and you cannot repost the same post twice while your earlier
    repost still stands (deleting it first frees you to repost again).
    Reposts received from peers for originals that have not arrived
    yet are still stored through the sync path (:func:`store_post`)
    and shown without the original's text until it arrives.
    """
    target = _require_hex(target, HASH_HEX_LENGTH, "target")
    timestamp = round(float(now if now is not None else time.time()), 3)
    author = _author_from_identity(identity)
    existing = get_post(conn, target)
    if existing is None:
        raise TanError(f"unknown post: {target!r}")
    if existing.get("kind", KIND_POST) not in REPLYABLE_KINDS:
        raise TanError("only posts and replies can be reposted")
    if not existing.get("linked"):
        raise TanError(
            "post has not linked yet; sync its chain before reposting"
        )
    if _is_deleted(conn, existing):
        raise TanError("post has been deleted")
    if existing["author"] == author:
        raise TanError("cannot repost your own post")
    standing = conn.execute(
        "SELECT hash, author FROM posts WHERE kind = 'repost' "
        "AND linked = 1 AND author = ? AND target = ?",
        (author, target),
    ).fetchall()
    for row in standing:
        if not _is_deleted(conn, {"hash": str(row["hash"]), "author": str(row["author"])}):
            raise TanError("post has already been reposted")
    prev = previous
    if prev is None:
        prev = head(conn, author) or ""
    prev = prev.strip().lower()
    if prev:
        _require_hex(prev, HASH_HEX_LENGTH, "prev")

    body = {
        "v": TAN_VERSION,
        "author": author,
        "key": identity.get_public_key().hex(),
        "name": sanitize_name(name),
        "text": "",
        "created_at": timestamp,
        "prev": prev,
        "kind": KIND_REPOST,
        "target": target,
    }
    digest = hash(body, previous=prev)
    mesg = dict(body)
    mesg["hash"] = digest
    mesg["sig"] = identity.sign(bytes.fromhex(digest)).hex()
    record, _ = store_post(conn, mesg, now=timestamp)
    return record


def create_edit(
    conn: sqlite3.Connection,
    identity: RNS.Identity,
    target: str,
    text: str,
    *,
    name: str = "anonymous",
    now: Optional[float] = None,
    previous: Optional[str] = None,
) -> Dict[str, Any]:
    """Create, sign and store an edit entry for the post *target*.

    The edit is appended to the editor's own chain, exactly like a
    reply: ``kind`` is ``edit`` and ``target`` is the post hash, and
    the replacement text is carried like a post's.  Only the post's
    author can edit it, and only posts and replies can be edited.
    When the target is not cached locally, is not linked yet, or has
    been deleted, this raises: an edit is written to a post the
    author has seen in full.  Edits received from peers for posts
    that have not arrived yet are still stored through the sync path
    (:func:`store_post`); they stay inert until the post arrives and
    its author matches (see :func:`edit_states`).
    """
    target = _require_hex(target, HASH_HEX_LENGTH, "target")
    timestamp = round(float(now if now is not None else time.time()), 3)
    author = _author_from_identity(identity)
    existing = get_post(conn, target)
    if existing is None:
        raise TanError(f"unknown post: {target!r}")
    if existing.get("kind", KIND_POST) not in (KIND_POST, KIND_REPLY):
        raise TanError("only posts and replies can be edited")
    if not existing.get("linked"):
        raise TanError(
            "post has not linked yet; sync its chain before editing"
        )
    if existing["author"] != author:
        raise TanError("cannot edit a post you did not author")
    if _is_deleted(conn, existing):
        raise TanError("post has been deleted")
    clean_text = normalize_message(text)
    prev = previous
    if prev is None:
        prev = head(conn, author) or ""
    prev = prev.strip().lower()
    if prev:
        _require_hex(prev, HASH_HEX_LENGTH, "prev")

    body = {
        "v": TAN_VERSION,
        "author": author,
        "key": identity.get_public_key().hex(),
        "name": sanitize_name(name),
        "text": clean_text,
        "created_at": timestamp,
        "prev": prev,
        "kind": KIND_EDIT,
        "target": target,
    }
    digest = hash(body, previous=prev)
    mesg = dict(body)
    mesg["hash"] = digest
    mesg["sig"] = identity.sign(bytes.fromhex(digest)).hex()
    record, _ = store_post(conn, mesg, now=timestamp)
    return record


def reaction_states(
    conn: sqlite3.Connection, targets: Sequence[str]
) -> Dict[str, Dict[str, str]]:
    """Map each target hash to ``{reactor: latest kind}`` from linked entries.

    Only linked (chain-verified) reactions count; an orphaned reaction
    starts counting as soon as its chain links up.
    """
    clean = []
    for target in targets:
        try:
            clean.append(_require_hex(target, HASH_HEX_LENGTH, "target"))
        except PostError:
            continue
    states: Dict[str, Dict[str, str]] = {target: {} for target in clean}
    if not clean:
        return states
    placeholders = ", ".join("?" for _ in clean)
    rows = conn.execute(
        "SELECT target, author, kind FROM posts "
        f"WHERE kind IN ('like', 'unlike') AND linked = 1 AND target IN ({placeholders}) "
        "ORDER BY target, author, seq ASC",
        clean,
    ).fetchall()
    for row in rows:
        # Rows arrive in chain order per reactor, so the last write wins.
        states[str(row["target"])][str(row["author"])] = str(row["kind"])
    return states


def like_counts(
    conn: sqlite3.Connection,
    targets: Sequence[str],
    *,
    viewer: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """Return ``{target: {"likes": n, "liked": bool}}`` for timeline display."""
    states = reaction_states(conn, targets)
    result: Dict[str, Dict[str, Any]] = {}
    for target, reactors in states.items():
        likers = [a for a, kind in reactors.items() if kind == KIND_LIKE]
        result[target] = {
            "likes": len(likers),
            "liked": viewer is not None and viewer in likers,
        }
    return result


def post_likers(
    conn: sqlite3.Connection, target: str
) -> List[Dict[str, Any]]:
    """Return the likers of one post, newest like first.

    Each entry is ``{"author": <hex>, "liked_at": <created_at>}``. Only
    linked reactions count and the latest entry per reactor wins, so an
    ``unlike`` after a ``like`` removes that author. A target with no
    reactions (or no stored post at all) yields an empty list.
    """
    clean = _require_hex(target, HASH_HEX_LENGTH, "target")
    rows = conn.execute(
        "SELECT author, kind, created_at FROM posts "
        "WHERE kind IN ('like', 'unlike') AND linked = 1 AND target = ? "
        "ORDER BY author, seq ASC",
        (clean,),
    ).fetchall()
    latest: Dict[str, Tuple[str, float]] = {}
    for row in rows:
        # Rows arrive in chain order per reactor, so the last write wins.
        latest[str(row["author"])] = (str(row["kind"]), float(row["created_at"]))
    likers = [
        {"author": author, "liked_at": liked_at}
        for author, (kind, liked_at) in latest.items()
        if kind == KIND_LIKE
    ]
    likers.sort(key=lambda entry: entry["liked_at"], reverse=True)
    return likers


def repost_counts(
    conn: sqlite3.Connection,
    targets: Sequence[str],
    *,
    viewer: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """Return ``{target: {"reposts": n, "reposted": bool}}`` for display.

    Only linked reposts count, and only while they stand: a repost its
    author deleted stops counting, the same way an unlike retires a
    like.  The count is distinct reposters, not entries: one author
    counts once even if their chain somehow carries two reposts of the
    same target (a fork, or a repost stored past the duplicate guard).
    ``reposted`` is true when the viewer's own repost of the
    target still stands.
    """
    clean = []
    for target in targets:
        try:
            clean.append(_require_hex(target, HASH_HEX_LENGTH, "target"))
        except PostError:
            continue
    result: Dict[str, Dict[str, Any]] = {
        target: {"reposts": 0, "reposted": False} for target in clean
    }
    if not clean:
        return result
    placeholders = ", ".join("?" for _ in clean)
    rows = conn.execute(
        "SELECT hash, target, author FROM posts "
        f"WHERE kind = 'repost' AND linked = 1 AND target IN ({placeholders})",
        clean,
    ).fetchall()
    seen: Dict[str, Set[str]] = {target: set() for target in clean}
    for row in rows:
        target = str(row["target"])
        author = str(row["author"])
        if author in seen[target]:
            continue
        if _is_deleted(
            conn, {"hash": str(row["hash"]), "author": author}
        ):
            continue
        seen[target].add(author)
        result[target]["reposts"] += 1
        if viewer is not None and author == viewer:
            result[target]["reposted"] = True
    return result


def annotate_repost_counts(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
    *,
    viewer: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Attach ``reposts``/``reposted`` fields to post dicts, in place."""
    counts = repost_counts(conn, [post["hash"] for post in posts], viewer=viewer)
    for post in posts:
        info = counts.get(post["hash"], {"reposts": 0, "reposted": False})
        post["reposts"] = info["reposts"]
        post["reposted"] = info["reposted"]
    return posts


def likers(
    conn: sqlite3.Connection,
    ref: str,
    *,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
) -> Dict[str, Any]:
    """Return the post *ref* names plus who currently likes it.

    *ref* is a post or reply hash or unique prefix, resolved exactly as
    :func:`resolve_post` does — an unknown, ambiguous or deleted
    reference raises :class:`TanError`.  Like :func:`thread`, an
    unlinked entry and a reply beneath a deleted ancestor are refused
    the same way, so no likers are shown for a post the thread view
    would hide.  The result carries the resolved post under ``"post"``
    and the current likers under ``"likers"``: one ``{"author": ...,
    "name": ...}`` record per reactor whose latest reaction for the
    post is a like, sorted by name then author hash.  The name is the
    one on the reactor's latest reaction entry for the post.  Only
    linked reactions count, like :func:`reaction_states`.  Like
    :func:`thread`, a post from a blocked author — or a reply beneath
    one — is refused, and an unusable block list hides everything
    (``exclude_all``), so the likers view never shows a post the
    thread view would hide.
    """
    post = resolve_post(conn, ref)
    if not post.get("linked"):
        # Like thread(): an unlinked entry is not shown, so its likers are not either.
        raise TanError(f"post has not linked yet: {ref!r}")
    excluded = set(_clean_excluded(exclude_authors))
    if exclude_all:
        raise TanError("cannot show likers: the block-list is unusable")
    if post.get("author") in excluded:
        raise TanError(f"post is from a blocked author: {ref!r}")
    # Like thread(): a reply beneath a deleted ancestor stays hidden.
    current_entry = post
    seen_ancestors = {current_entry["hash"]}
    while current_entry.get("kind") == KIND_REPLY and current_entry.get("target"):
        parent = get_post(conn, str(current_entry["target"]))
        if parent is None or parent.get("kind") not in TIMELINE_KINDS:
            break
        if parent["hash"] in seen_ancestors:
            break
        if _is_deleted(conn, parent):
            raise TanError(f"post has been deleted: {ref!r}")
        if parent["author"] in excluded:
            raise TanError(f"post is from a blocked author: {ref!r}")
        seen_ancestors.add(parent["hash"])
        current_entry = parent
    target = str(post["hash"])
    states = reaction_states(conn, [target]).get(target, {})
    current = [a for a, kind in states.items() if kind == KIND_LIKE]
    records: List[Dict[str, Any]] = []
    if current:
        # One target-scoped query, no author list expanded into
        # placeholders: the name lookup cannot hit SQLite's host
        # parameter limit no matter how many likers a post has.
        rows = conn.execute(
            "SELECT author, name FROM posts "
            "WHERE kind IN ('like', 'unlike') AND linked = 1 AND target = ? "
            "ORDER BY author, seq ASC",
            (target,),
        ).fetchall()
        names: Dict[str, str] = {}
        for row in rows:
            # Rows arrive in chain order per reactor, so the last write
            # wins — the name on the reactor's latest reaction.
            names[str(row["author"])] = str(row["name"])
        records = [
            {"author": author, "name": names.get(author, "")}
            for author in current
        ]
    records.sort(key=lambda record: (record["name"].lower(), record["author"]))
    return {"post": _canonical_post(post), "likers": records}


def annotate_reactions(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
    *,
    viewer: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Attach ``likes``/``liked``/``own`` fields to post dicts, in place.

    ``own`` is true when *viewer* is given and the post's author is the
    viewer, so a display surface can offer author-only actions (like
    deleting the post) without learning the viewer's identity itself.
    """
    counts = like_counts(conn, [post["hash"] for post in posts], viewer=viewer)
    for post in posts:
        info = counts.get(post["hash"], {"likes": 0, "liked": False})
        post["likes"] = info["likes"]
        post["liked"] = info["liked"]
        post["own"] = viewer is not None and post["author"] == viewer
    return posts


def annotate_saved(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Attach a ``saved`` flag to post dicts, in place.

    A display surface uses it to show save state on each card without
    a second request: the saved list itself is fetched separately.
    """
    hashes = [str(post["hash"]) for post in posts if post.get("hash")]
    saved: Set[str] = set()
    if hashes:
        placeholders = ", ".join("?" for _ in hashes)
        rows = conn.execute(
            f"SELECT hash FROM saved_posts WHERE hash IN ({placeholders})",
            hashes,
        ).fetchall()
        saved = {str(row["hash"]) for row in rows}
    for post in posts:
        post["saved"] = str(post.get("hash")) in saved
    return posts


def annotate_replies(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
    *,
    muted_keywords: Optional[Sequence[str]] = None,
    exclude_authors: Optional[Set[str]] = None,
    blocked_authors: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    """Attach a ``reply_to`` parent summary to reply entries, in place.

    The summary carries the parent's hash, author, name and text when
    the parent is cached locally, and is ``None`` when the parent has
    not arrived yet — a reply is still shown on its own.  A deleted
    parent is reported as deleted with its text blanked, so the thread
    stays readable without resurfacing removed content.  A parent
    matching a muted keyword (see :func:`muted_keywords`) is reported
    the same way, with its text blanked and ``muted`` set, so the
    muted content does not leak through the preview.  A parent whose
    author is in *exclude_authors* (muted) is reported as ``None`` so
    its name and text never surface.  A parent whose author is in
    *blocked_authors* is reported with its text blanked, its author
    and name redacted, and ``blocked`` set, so blocking an author
    never leaks their words — or their name — through a reply
    snippet.  Top-level posts get no ``reply_to`` key.

    Parents are fetched in one query and their deletion status in a
    second, so a reply-heavy page does not issue one lookup per reply.
    Only linked timeline entries count as parents: a reply that points
    at a reaction, a delete or an unlinked entry is shown without a
    parent snippet, the same as an uncached parent.
    """
    targets = [
        str(post["target"])
        for post in posts
        if post.get("kind") == KIND_REPLY and post.get("target")
    ]
    parents: Dict[str, Dict[str, Any]] = {}
    if targets:
        placeholders = ", ".join("?" for _ in targets)
        rows = conn.execute(
            f"SELECT {', '.join(_POST_COLUMNS)} FROM posts "
            f"WHERE hash IN ({placeholders}) "
            f"AND kind IN ({TIMELINE_KINDS_SQL}) AND linked = 1",
            targets,
        ).fetchall()
        for row in rows:
            parents[str(row["hash"])] = _canonical_post(dict(row))
    deleted: Set[Tuple[str, str]] = set()
    if parents:
        placeholders = ", ".join("?" for _ in parents)
        rows = conn.execute(
            "SELECT target, author FROM posts WHERE kind = 'delete' "
            f"AND linked = 1 AND target IN ({placeholders})",
            list(parents),
        ).fetchall()
        deleted = {(str(row["target"]), str(row["author"])) for row in rows}
    if parents:
        # A parent's snippet shows its current text, so apply its
        # author's latest edit before the summary is built.
        annotate_edits(conn, list(parents.values()))
    muted_pattern = _muted_keyword_pattern(list(muted_keywords or []))
    for post in posts:
        if post.get("kind") != KIND_REPLY:
            continue
        parent = parents.get(str(post.get("target") or ""))
        if parent is None:
            post["reply_to"] = None
            continue
        is_deleted = (parent["hash"], parent["author"]) in deleted
        if blocked_authors and parent["author"] in blocked_authors:
            # Redact identity too: a blocked author's name and hash must
            # not leak through a reply's parent summary.
            post["reply_to"] = {
                "hash": parent["hash"],
                "author": "",
                "name": "",
                "text": "",
                "deleted": is_deleted,
                "blocked": True,
            }
            continue
        if exclude_authors and parent["author"] in exclude_authors:
            post["reply_to"] = None
            continue
        is_muted = _text_matches_muted_keywords(
            str(parent.get("text") or ""), muted_pattern
        )
        preview = {
            "hash": parent["hash"],
            "author": parent["author"],
            "name": parent["name"],
            "text": "" if is_deleted or is_muted else parent["text"],
            "deleted": is_deleted,
            "edited": bool(parent.get("edited")),
        }
        if is_muted:
            # Only present when muted, so unmuted previews keep their
            # established shape.
            preview["muted"] = True
        post["reply_to"] = preview
    return posts


def quote_counts(
    conn: sqlite3.Connection,
    targets: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    """Return ``{target: {"quotes": n}}`` for timeline display.

    Every linked, not-deleted quote of a target counts, including
    quotes of a post that has since been deleted — the count
    describes the quoting activity, not the target's current
    visibility.  A quote its author deleted stops counting, the same
    way a deleted post stops appearing.
    """
    clean = []
    for target in targets:
        try:
            clean.append(_require_hex(target, HASH_HEX_LENGTH, "target"))
        except PostError:
            continue
    result: Dict[str, Dict[str, Any]] = {target: {"quotes": 0} for target in clean}
    if not clean:
        return result
    placeholders = ", ".join("?" for _ in clean)
    rows = conn.execute(
        "SELECT target, COUNT(*) AS n FROM posts "
        f"WHERE kind = 'quote' AND linked = 1 AND target IN ({placeholders}) "
        f"AND {_NOT_DELETED} GROUP BY target",
        clean,
    ).fetchall()
    for row in rows:
        result[str(row["target"])] = {"quotes": int(row["n"])}
    return result


def annotate_quote_counts(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Attach a ``quotes`` count to post dicts, in place."""
    counts = quote_counts(conn, [post["hash"] for post in posts])
    for post in posts:
        info = counts.get(post["hash"], {"quotes": 0})
        post["quotes"] = info["quotes"]
    return posts


def annotate_quotes(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
    *,
    muted_keywords: Optional[Sequence[str]] = None,
    exclude_authors: Optional[Set[str]] = None,
    blocked_authors: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    """Attach a ``quoted`` original summary to quote entries, in place.

    The summary carries the original's hash, author, name and text
    when the original is cached locally, and is ``None`` when the
    original has not arrived yet — a quote is still shown on its own.
    A deleted original is reported as deleted with its text blanked,
    so the thread stays readable without resurfacing removed content.
    An original matching a muted keyword (see :func:`muted_keywords`)
    is reported the same way, with its text blanked and ``muted`` set,
    so the muted content does not leak through the preview.  An
    original whose author is in *exclude_authors* (muted) is reported
    as ``None`` so its name and text never surface.  An original whose
    author is in *blocked_authors* is reported with its text blanked,
    its author and name redacted, and ``blocked`` set, so blocking an
    author never leaks their words — or their name — through a quote
    preview, the same protection :func:`annotate_replies` gives reply
    snippets.  Top-level posts
    and replies get no ``quoted`` key.

    Parents are fetched in one query and their deletion status in a
    second, so a quote-heavy page does not issue one lookup per quote.
    Only linked posts and replies count as originals: a quote that
    points at a reaction, a delete, a quote or an unlinked entry is
    shown without the embedded original, the same as an uncached
    original.
    """
    targets = [
        str(post["target"])
        for post in posts
        if post.get("kind") == KIND_QUOTE and post.get("target")
    ]
    parents: Dict[str, Dict[str, Any]] = {}
    if targets:
        placeholders = ", ".join("?" for _ in targets)
        rows = conn.execute(
            f"SELECT {', '.join(_POST_COLUMNS)} FROM posts "
            f"WHERE hash IN ({placeholders}) "
            f"AND kind IN ({QUOTABLE_KINDS_SQL}) AND linked = 1",
            targets,
        ).fetchall()
        for row in rows:
            parents[str(row["hash"])] = _canonical_post(dict(row))
    deleted: Set[Tuple[str, str]] = set()
    if parents:
        placeholders = ", ".join("?" for _ in parents)
        rows = conn.execute(
            "SELECT target, author FROM posts WHERE kind = 'delete' "
            f"AND linked = 1 AND target IN ({placeholders})",
            list(parents),
        ).fetchall()
        deleted = {(str(row["target"]), str(row["author"])) for row in rows}
    muted_pattern = _muted_keyword_pattern(list(muted_keywords or []))
    for post in posts:
        if post.get("kind") != KIND_QUOTE:
            continue
        parent = parents.get(str(post.get("target") or ""))
        if parent is None or (
            exclude_authors and parent["author"] in exclude_authors
        ):
            post["quoted"] = None
            continue
        is_deleted = (parent["hash"], parent["author"]) in deleted
        if blocked_authors and parent["author"] in blocked_authors:
            # Redact identity too: a blocked author's name and hash must
            # not leak through a quote preview, as in annotate_replies.
            post["quoted"] = {
                "hash": parent["hash"],
                "author": "",
                "name": "",
                "text": "",
                "deleted": is_deleted,
                "blocked": True,
            }
            continue
        is_muted = _text_matches_muted_keywords(
            str(parent.get("text") or ""), muted_pattern
        )
        summary = {
            "hash": parent["hash"],
            "author": parent["author"],
            "name": parent["name"],
            "text": "" if is_deleted or is_muted else parent["text"],
            "deleted": is_deleted,
        }
        if is_muted:
            # Only present when muted, so unmuted previews keep their
            # established shape.
            summary["muted"] = True
        post["quoted"] = summary
    return posts


def annotate_reposts(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
    *,
    muted_keywords: Optional[Sequence[str]] = None,
    exclude_authors: Optional[Set[str]] = None,
    blocked_authors: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    """Attach a ``repost_of`` original-post summary to repost entries, in place.

    The summary carries the original's hash, author, name and text
    when it is cached locally, and is ``None`` when it has not arrived
    yet — a repost is still shown on its own.  A deleted original is
    reported as deleted with its text blanked, so the timeline stays
    readable without resurfacing removed content.  Top-level posts and
    replies get no ``repost_of`` key.

    Originals are fetched in one query and their deletion status in a
    second, so a repost-heavy page does not issue one lookup per
    repost.  Only linked posts and replies count as originals: a
    repost that points at a reaction, a delete or an unlinked entry is
    shown without the original's text, the same as an uncached one.

    A repost carries no text of its own — the original's words are the
    whole visible content — so the block, mute and muted-keyword rules
    that hide a post hide its reposts too: a repost whose original is
    by an author in *blocked_authors* or *exclude_authors* (muted), or
    whose original text matches a muted keyword, is dropped from the
    returned list instead of being kept with a redacted preview the
    way a reply is.  Without the filter arguments nothing is dropped,
    so an explicit lookup still shows the repost.  The summary shows
    the original's current text: its author's latest edit is applied
    first, the same way :func:`annotate_replies` edits its parents.
    """
    targets = [
        str(post["target"])
        for post in posts
        if post.get("kind") == KIND_REPOST and post.get("target")
    ]
    originals: Dict[str, Dict[str, Any]] = {}
    if targets:
        placeholders = ", ".join("?" for _ in targets)
        rows = conn.execute(
            f"SELECT {', '.join(_POST_COLUMNS)} FROM posts "
            f"WHERE hash IN ({placeholders}) "
            "AND kind IN ('post', 'reply') AND linked = 1",
            targets,
        ).fetchall()
        for row in rows:
            originals[str(row["hash"])] = _canonical_post(dict(row))
    deleted: Set[Tuple[str, str]] = set()
    if originals:
        placeholders = ", ".join("?" for _ in originals)
        rows = conn.execute(
            "SELECT target, author FROM posts WHERE kind = 'delete' "
            f"AND linked = 1 AND target IN ({placeholders})",
            list(originals),
        ).fetchall()
        deleted = {(str(row["target"]), str(row["author"])) for row in rows}
    if originals:
        # A repost's summary shows its original's current text, so
        # apply its author's latest edit before the summary is built.
        annotate_edits(conn, list(originals.values()))
    muted_pattern = _muted_keyword_pattern(list(muted_keywords or []))
    kept: List[Dict[str, Any]] = []
    for post in posts:
        if post.get("kind") != KIND_REPOST:
            kept.append(post)
            continue
        original = originals.get(str(post.get("target") or ""))
        if original is None:
            post["repost_of"] = None
            kept.append(post)
            continue
        if blocked_authors and original["author"] in blocked_authors:
            continue
        if exclude_authors and original["author"] in exclude_authors:
            continue
        if _text_matches_muted_keywords(
            str(original.get("text") or ""), muted_pattern
        ):
            continue
        is_deleted = (original["hash"], original["author"]) in deleted
        post["repost_of"] = {
            "hash": original["hash"],
            "author": original["author"],
            "name": original["name"],
            "text": "" if is_deleted else original["text"],
            "deleted": is_deleted,
        }
        kept.append(post)
    return kept


def edit_states(
    conn: sqlite3.Connection, targets: Sequence[str]
) -> Dict[str, Dict[str, Any]]:
    """Map each target hash to its latest edit by the post's own author.

    Only linked (chain-verified) edits count; an edit from anyone
    other than the target post's author is inert — it is stored so
    the editor's chain stays complete, but it never changes what the
    post shows.  An edit that arrives before its post applies as soon
    as the post is stored, the same way a delete does.  The latest
    edit wins: rows arrive in chain order per editor, so the last
    write for a target decides its text.
    """
    clean = []
    for target in targets:
        try:
            clean.append(_require_hex(target, HASH_HEX_LENGTH, "target"))
        except PostError:
            continue
    states: Dict[str, Dict[str, Any]] = {}
    if not clean:
        return states
    authors: Dict[str, str] = {}
    for start in range(0, len(clean), _SQL_BATCH):
        batch = clean[start : start + _SQL_BATCH]
        placeholders = ", ".join("?" for _ in batch)
        rows = conn.execute(
            f"SELECT hash, author FROM posts WHERE hash IN ({placeholders}) "
            "AND kind IN ('post', 'reply') AND linked = 1",
            batch,
        ).fetchall()
        authors.update({str(row["hash"]): str(row["author"]) for row in rows})
    if not authors:
        return states
    # Only edits on the post author's retained chain (from their head back
    # through ``prev``) may count; sibling forks with the same seq are ignored.
    chains: Dict[str, Set[str]] = {}
    for author in set(authors.values()):
        chains[author] = _chain_hashes(conn, author)
    targets = list(authors)
    for start in range(0, len(targets), _SQL_BATCH):
        batch = targets[start : start + _SQL_BATCH]
        placeholders = ", ".join("?" for _ in batch)
        rows = conn.execute(
            "SELECT target, author, text, created_at, hash FROM posts "
            f"WHERE kind = 'edit' AND linked = 1 AND target IN ({placeholders}) "
            "ORDER BY target, author, seq ASC",
            batch,
        ).fetchall()
        for row in rows:
            target = str(row["target"])
            author = str(row["author"])
            if author != authors.get(target):
                continue
            if str(row["hash"]) not in chains.get(author, set()):
                continue
            states[target] = {
                "text": str(row["text"]),
                "edited_at": float(row["created_at"]),
                "edit": str(row["hash"]),
            }
    return states


def _chain_hashes(conn: sqlite3.Connection, author: str) -> Set[str]:
    """Hashes of linked entries on *author*'s chain, from the retained head back."""
    head_row = conn.execute(
        "SELECT hash FROM heads WHERE author = ?", (author,)
    ).fetchone()
    if head_row is None:
        return set()
    rows = conn.execute(
        "WITH RECURSIVE chain(hash, prev) AS ("
        "SELECT hash, prev FROM posts WHERE hash = ? AND author = ? AND linked = 1 "
        "UNION ALL "
        "SELECT p.hash, p.prev FROM posts p JOIN chain c ON p.hash = c.prev "
        "WHERE p.author = ? AND p.linked = 1"
        ") SELECT hash FROM chain",
        (str(head_row["hash"]), author, author),
    ).fetchall()
    return {str(row["hash"]) for row in rows}


def annotate_edits(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Replace post text with the author's latest edit, in place.

    Each post gains an ``edited`` flag (and ``edited_at`` when it was
    edited) so display surfaces can mark edited posts.  The post's
    hash, author and creation time are untouched — an edit is a new
    entry on the author's chain, not a rewrite of the original.
    Applying this twice is a no-op: the same edit text is written
    again, so callers that already received annotated posts (through
    :func:`feed` with ``posts_only``) can annotate again safely.
    """
    states = edit_states(conn, [post["hash"] for post in posts])
    for post in posts:
        edit = states.get(post["hash"])
        if edit is None:
            post["edited"] = False
            continue
        post["text"] = edit["text"]
        post["edited"] = True
        post["edited_at"] = edit["edited_at"]
    return posts


# SQL fragment excluding posts their author deleted.  The outer query must
# read from a table named ``posts``.  Deletion is derived at read time, the
# same way like counts are, so a delete that arrives before its post
# applies as soon as the post is stored.
_NOT_DELETED = (
    "NOT EXISTS (SELECT 1 FROM posts AS d WHERE d.kind = 'delete' "
    "AND d.linked = 1 AND d.target = posts.hash AND d.author = posts.author)"
)


def deleted_targets(conn: sqlite3.Connection) -> Set[str]:
    """Hashes of posts hidden by a delete entry from their own author.

    A post is deleted when a linked ``delete`` entry exists whose author
    is the post's author.  Deletes from anyone else are inert, and a
    delete that arrives before its post applies as soon as the post is
    stored.  Deleted posts stay in the database — the chain must stay
    verifiable and sync must keep carrying them — they are only hidden
    from display surfaces.
    """
    rows = conn.execute(
        "SELECT p.hash FROM posts p "
        "JOIN posts d ON d.target = p.hash AND d.author = p.author "
        f"WHERE p.kind IN ({TIMELINE_KINDS_SQL}) AND p.linked = 1 "
        "AND d.kind = 'delete' AND d.linked = 1"
    ).fetchall()
    return {str(row["hash"]) for row in rows}


def _is_deleted(conn: sqlite3.Connection, post: Dict[str, Any]) -> bool:
    """Whether *post* is hidden by a delete entry from its own author."""
    row = conn.execute(
        "SELECT 1 FROM posts d WHERE d.kind = 'delete' AND d.linked = 1 "
        "AND d.target = ? AND d.author = ? LIMIT 1",
        (post["hash"], post["author"]),
    ).fetchone()
    return row is not None


def apply_display_rules(
    posts: List[Dict[str, Any]], ruleset: Ruleset
) -> List[Dict[str, Any]]:
    """Apply the ruleset's display-only ``kinds`` preference to *posts*.

    Entries whose kind the owner does not display are dropped from the
    view (they remain stored, keeping chains complete).  When neither
    reaction kind is displayed, the derived ``likes``/``liked``
    annotations are stripped as well, so hidden reactions leave no
    trace in the view.
    """
    shown = [
        post
        for post in posts
        if ruleset.displays(str(post.get("kind") or KIND_POST))
    ]
    if not (ruleset.displays(KIND_LIKE) or ruleset.displays(KIND_UNLIKE)):
        for post in shown:
            post.pop("likes", None)
            post.pop("liked", None)
    if not ruleset.displays(KIND_REPOST):
        for post in shown:
            post.pop("reposts", None)
            post.pop("reposted", None)
    if not ruleset.displays(KIND_QUOTE):
        for post in shown:
            post.pop("quotes", None)
    return shown


def resolve_post(conn: sqlite3.Connection, ref: str) -> Dict[str, Any]:
    """Resolve a post or reply hash or unique hash prefix to a stored entry.

    A post its author deleted resolves as deleted, not as unknown, so
    callers can tell the two apart.
    """
    if not isinstance(ref, str) or not ref.strip():
        raise TanError("post reference must not be empty")
    value = ref.strip().lower()
    if len(value) == HASH_HEX_LENGTH:
        post = get_post(conn, value)
        if post is not None and post.get("kind", KIND_POST) in TIMELINE_KINDS:
            if _is_deleted(conn, post):
                raise TanError(f"post has been deleted: {ref!r}")
            return post
        raise TanError(f"unknown post: {ref!r}")
    if len(value) >= 8:
        try:
            bytes.fromhex(value)
        except ValueError:
            raise TanError(f"unknown post: {ref!r}") from None
        rows = conn.execute(
            "SELECT hash FROM posts WHERE kind IN "
            f"({TIMELINE_KINDS_SQL}) "
            "AND hash LIKE ? LIMIT 8",
            (value + "%",),
        ).fetchall()
        if len(rows) == 1:
            post = get_post(conn, str(rows[0]["hash"]))
            if post is not None:
                if _is_deleted(conn, post):
                    raise TanError(f"post has been deleted: {ref!r}")
                return post
        if len(rows) > 1:
            raise TanError(f"{len(rows)} posts match prefix {value!r}")
    raise TanError(f"unknown post: {ref!r}")


def resolve_delete_target(conn: sqlite3.Connection, ref: str) -> str:
    """Resolve a delete target to a post hash.

    A full hash passes through even when the post is not cached — the
    signed tombstone applies when the post arrives.  A shorter prefix
    resolves against cached posts, so an unknown or already-deleted
    post fails here with a clear error.  Cached targets are still
    checked by :func:`create_delete` for ownership and prior deletion.
    """
    if not isinstance(ref, str) or not ref.strip():
        raise TanError("post reference must not be empty")
    value = ref.strip().lower()
    if len(value) == HASH_HEX_LENGTH:
        return _require_hex(value, HASH_HEX_LENGTH, "post")
    return resolve_post(conn, ref)["hash"]


def _public_posts(rows: Iterable[sqlite3.Row]) -> List[Dict[str, Any]]:
    return [_canonical_post(dict(row)) for row in rows]


def _clean_excluded(
    exclude_authors: Optional[Sequence[str]],
) -> List[str]:
    """Normalise *exclude_authors* to validated author hashes."""
    if not exclude_authors:
        return []
    return [
        _require_hex(author, AUTHOR_HEX_LENGTH, "author")
        for author in exclude_authors
    ]


def feed(
    conn: sqlite3.Connection,
    *,
    author: Optional[str] = None,
    after_seq: int = 0,
    limit: int = DEFAULT_FEED_LIMIT,
    all_authors: bool = False,
    before: Optional[float] = None,
    posts_only: bool = False,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
) -> List[Dict[str, Any]]:
    """Read cached entries in chain order or reverse chronological order.

    Sync needs every entry (reactions, deletes, replies and quotes
    included) to keep chains complete; display callers pass
    ``posts_only=True`` to hide reaction entries and posts their author
    deleted.  Replies, reposts and quotes are timeline items, so
    ``posts_only`` keeps them alongside top-level posts.  ``exclude_authors`` hides
    entries from blocked authors from display surfaces; the entries
    stay in the store, so chain integrity and sync are unaffected.
    ``exclude_all`` hides every entry — the display side of a
    block-list that failed closed.
    """
    if exclude_all:
        return []
    count = max(1, min(int(limit), MAX_FEED_POSTS))
    excluded = _clean_excluded(exclude_authors)
    if author is not None:
        author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
        if author in excluded:
            return []
        kind_filter = ""
        if posts_only:
            kind_filter = (
                f" AND kind IN ({TIMELINE_KINDS_SQL}) AND {_NOT_DELETED}"
            )
        rows = conn.execute(
            f"SELECT {', '.join(_POST_COLUMNS)} FROM posts "
            f"WHERE author = ? AND linked = 1 AND seq > ?{kind_filter} "
            "ORDER BY seq ASC LIMIT ?",
            (author, max(0, int(after_seq)), count),
        ).fetchall()
        posts = _public_posts(rows)
        return annotate_edits(conn, posts) if posts_only else posts

    params: List[Any] = []
    where = "linked = 1"
    if posts_only:
        where += f" AND kind IN ({TIMELINE_KINDS_SQL}) AND {_NOT_DELETED}"
    if excluded:
        # Display-only exclusion: sync callers leave this unset, so
        # chains stay complete for every author.  One JSON parameter
        # instead of one placeholder per author, so a large block-list
        # cannot exceed SQLite's bound-variable limit.
        where += " AND author NOT IN (SELECT value FROM json_each(?))"
        params.append(json.dumps(excluded))
    if before is not None:
        where += " AND created_at < ?"
        params.append(float(before))
    params.append(count)
    rows = conn.execute(
        f"SELECT {', '.join(_POST_COLUMNS)} FROM posts WHERE {where} "
        "ORDER BY created_at DESC, hash DESC LIMIT ?",
        params,
    ).fetchall()
    posts = _public_posts(rows)
    return annotate_edits(conn, posts) if posts_only else posts


TAN_EXPORT_FORMAT = "tau-tan-export"
TAN_EXPORT_VERSION = 1


def export_entries(
    conn: sqlite3.Connection,
    *,
    author: Optional[str] = None,
    all_authors: bool = False,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Return a portable backup of stored chain entries.

    Entries are the canonical signed envelopes (the same dicts
    :func:`store_post` accepts), ordered per author so a chain replays
    in order: linked entries by sequence — the stored head first among
    same-sequence forks, so the restored head matches the source — then
    any entries whose parent is missing from this store (sequence 0)
    by creation time.  An entry whose parent is missing stays unlinked
    on import until that parent arrives, exactly as sync behaves.

    With *limit* and ``all_authors``, only the *limit* most recent
    entries across all authors are exported (the rest are left out).
    """
    columns = ", ".join(f"posts.{column}" for column in _POST_COLUMNS)
    order = (
        "posts.linked DESC, posts.seq ASC, heads.hash IS NULL ASC, "
        "posts.created_at ASC, posts.hash ASC"
    )
    if all_authors:
        source = "posts"
        params: Tuple[Any, ...] = ()
        if limit is not None:
            if limit <= 0:
                raise TanError("export limit must be positive")
            source = (
                "(SELECT * FROM posts ORDER BY created_at DESC, hash DESC LIMIT ?)"
                " AS posts"
            )
            params = (int(limit),)
        rows = conn.execute(
            f"SELECT {columns} FROM {source} LEFT JOIN heads "
            "ON heads.author = posts.author AND heads.hash = posts.hash "
            f"ORDER BY posts.author ASC, {order}",
            params,
        ).fetchall()
    else:
        if author is None:
            raise TanError("export needs an author or --all")
        author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
        rows = conn.execute(
            f"SELECT {columns} FROM posts LEFT JOIN heads "
            "ON heads.author = posts.author AND heads.hash = posts.hash "
            f"WHERE posts.author = ? ORDER BY {order}",
            (author,),
        ).fetchall()
    entries = [_canonical_post(dict(row)) for row in rows]
    return {
        "format": TAN_EXPORT_FORMAT,
        "version": TAN_EXPORT_VERSION,
        "exported_at": time.time(),
        "authors": sorted({entry["author"] for entry in entries}),
        "entries": entries,
    }


def import_entries(
    conn: sqlite3.Connection, document: Any
) -> Dict[str, Any]:
    """Store every entry in an export document, verifying each one.

    Returns ``{"imported": n, "present": n, "skipped": n, "errors": [...]}``.
    An entry that fails verification is skipped with its error recorded;
    it never aborts the rest of the import, and entries already in the
    store are counted as present rather than stored twice.
    """
    if not isinstance(document, dict):
        raise TanError("export file must contain a JSON object")
    if document.get("format") != TAN_EXPORT_FORMAT:
        raise TanError(f"unsupported export format: {document.get('format')!r}")
    if document.get("version") != TAN_EXPORT_VERSION:
        raise TanError(
            f"unsupported export version: {document.get('version')!r}"
        )
    entries = document.get("entries")
    if not isinstance(entries, list):
        raise TanError("export file has no entries list")

    imported = present = skipped = 0
    errors: List[Dict[str, str]] = []
    for entry in entries:
        digest = ""
        if isinstance(entry, dict):
            digest = str(entry.get("hash") or "")
        try:
            _, is_new = store_post(conn, entry, verify=True)
        except (
            TanError,
            ProtocolError,
            ValueError,
            TypeError,
            AttributeError,
            OverflowError,
        ) as exc:
            # A bad entry is recorded and skipped; it never aborts the
            # rest of the import.  OverflowError needs listing because
            # float() on a huge JSON integer raises it, and it is not a
            # ValueError; ProtocolError is listed for the same reason
            # even though validation currently wraps it as a PostError.
            skipped += 1
            errors.append({"hash": digest, "error": str(exc)})
            continue
        if is_new:
            imported += 1
        else:
            present += 1
    return {
        "imported": imported,
        "present": present,
        "skipped": skipped,
        "errors": errors,
    }


# A tag is ``#`` followed by a Unicode letter, then letters, digits,
# ``_`` or ``-``.  The ``#`` must start the text or follow a character
# that is not a word character, so ``C#`` and ``foo#bar`` are not tags.
_TAG_PATTERN = re.compile(r"(?<![\w])#([^\W\d_][\w-]*)", re.UNICODE)


def extract_tags(text: Any) -> List[str]:
    """Tags carried by *text*, casefolded and deduplicated in order.

    Tags are derived from the text at read time, never stored: a post
    gains its tags without a re-publish, and no envelope or schema
    change is involved.  A trailing ``-`` is not part of a tag, so
    ``#mesh-`` reads as ``mesh``.  A candidate that does not survive
    casefolding as one tag — a capital ``İ`` casefolds to ``i`` plus a
    combining mark, which a tag cannot carry — is skipped, so every
    tag returned here passes :func:`clean_tag`.
    """
    if not isinstance(text, str):
        return []
    seen: Set[str] = set()
    found: List[str] = []
    for match in _TAG_PATTERN.finditer(text):
        tag = match.group(1).rstrip("-").casefold()
        if not tag or _TAG_PATTERN.fullmatch("#" + tag) is None:
            continue
        if tag not in seen:
            seen.add(tag)
            found.append(tag)
    return found


def clean_tag(value: Any) -> str:
    """Normalise a user-supplied tag, or raise :class:`TanError`.

    A leading ``#`` is stripped, surrounding whitespace is ignored,
    and the tag is casefolded.  Anything that is not exactly one tag —
    empty, starting with a digit, or carrying characters a tag cannot
    carry — is rejected.
    """
    if not isinstance(value, str):
        raise TanError("tag must be text")
    raw = value.strip()
    if raw.startswith("#"):
        raw = raw[1:]
    tag = raw.strip().casefold()
    if not tag or extract_tags("#" + tag) != [tag]:
        raise TanError(f"not a tag: {value!r}")
    return tag


def _filter_posts_by_tag(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
    tag: str,
) -> List[Dict[str, Any]]:
    """Keep only the posts in *posts* carrying *tag*.

    The match runs on each post's current text — an edited post matches
    its edited text, not the original — and a repost matches on the
    text of the post it reposts, since a repost carries no text of its
    own.  A repost whose original has not arrived (or is deleted)
    carries no visible text and never matches.  Originals are fetched
    in one query, the same way :func:`annotate_reposts` fetches them.
    """
    if not posts:
        return []
    states = edit_states(conn, [str(post["hash"]) for post in posts])
    targets = [
        str(post["target"])
        for post in posts
        if post.get("kind") == KIND_REPOST and post.get("target")
    ]
    originals: Dict[str, str] = {}
    if targets:
        placeholders = ", ".join("?" for _ in targets)
        rows = conn.execute(
            f"SELECT {', '.join(_POST_COLUMNS)} FROM posts "
            f"WHERE hash IN ({placeholders}) "
            f"AND kind IN ('post', 'reply') AND linked = 1 AND {_NOT_DELETED}",
            targets,
        ).fetchall()
        original_posts = _public_posts(rows)
        original_states = edit_states(
            conn, [post["hash"] for post in original_posts]
        )
        for original in original_posts:
            text = original_states.get(original["hash"], {}).get(
                "text", original["text"]
            )
            originals[original["hash"]] = str(text)
    kept: List[Dict[str, Any]] = []
    for post in posts:
        if post.get("kind") == KIND_REPOST:
            text = originals.get(str(post.get("target") or ""), "")
        else:
            text = states.get(str(post["hash"]), {}).get(
                "text", str(post.get("text") or "")
            )
        if tag in extract_tags(text):
            kept.append(post)
    return kept


def _tag_page_filter(
    conn: sqlite3.Connection,
    tag: Optional[str],
    *,
    keywords: List[str],
    exclude_authors: Optional[Set[str]] = None,
    blocked_authors: Optional[Set[str]] = None,
) -> Optional[Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]]:
    """Page filter for a tag search, or ``None`` when no tag is given.

    Reposts the timeline would hide — of a blocked or muted author's
    post, or of a post matching a muted keyword — are dropped before
    the tag match, so they never count toward the paging limit.  The
    arguments mirror the call site's final :func:`annotate_reposts`:
    the author view keeps reposts of muted authors, so it passes no
    *exclude_authors*.
    """
    if tag is None:
        return None

    def keep(page: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        visible = annotate_reposts(
            conn,
            page,
            muted_keywords=keywords,
            exclude_authors=exclude_authors,
            blocked_authors=blocked_authors,
        )
        return _filter_posts_by_tag(conn, visible, tag)

    return keep


def timeline(
    conn: sqlite3.Connection,
    authors: Optional[Sequence[str]] = None,
    *,
    limit: int = 30,
    include_all: bool = False,
    before: Optional[float] = None,
    viewer: Optional[str] = None,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
    tag: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return followed posts (and local posts) newest first.

    Reaction entries never appear as timeline items; instead each post
    is annotated with its derived ``likes`` count and, when *viewer* is
    given, whether the viewer's like currently stands and whether the
    post is the viewer's own.  Replies, reposts and quotes appear as
    timeline items, annotated with their parent post when it is cached:
    a repost carries the original post it points at and its own repost
    count, and each post carries its derived ``quotes`` count.  Posts
    their author deleted never appear either.  Posts whose text matches
    a muted keyword (see :func:`muted_keywords`) never appear either,
    and a reply to one keeps its ``reply_to`` preview text blanked —
    muting is display-only, so entries stay stored and unmuting
    restores them.  Posts by muted authors never appear either, for the
    same reason.  A repost of a post by a muted or blocked author, or
    of a post matching a muted keyword, never appears either — a
    repost carries no text of its own, so hiding the original hides
    the repost.  Pages are read until ``limit`` unmuted posts are
    collected or the feed is exhausted, so muted posts never shrink the
    result below what is available.  ``exclude_authors`` hides posts
    from blocked authors — and blanks their words from reply
    snippets — while the entries stay stored, so chains stay intact.
    ``exclude_all`` hides every post — the display side of a
    block-list that failed closed.  When *tag* is given, only posts
    carrying that tag are returned — a repost counts when the post it
    reposts carries the tag — and paging keeps going until ``limit``
    matching posts are collected.
    """
    if exclude_all:
        return []
    silenced = muted_author_set(conn)
    blocked = set(_clean_excluded(exclude_authors))
    hidden = silenced | blocked
    keywords = _muted_keyword_list(conn)
    wanted_tag = clean_tag(tag) if tag is not None else None
    keep_tag = _tag_page_filter(
        conn,
        wanted_tag,
        keywords=keywords,
        exclude_authors=silenced,
        blocked_authors=blocked,
    )
    if include_all:
        posts = _fill_unmuted(
            conn,
            keywords,
            limit,
            lambda cursor: feed(
                conn,
                limit=MAX_FEED_POSTS,
                before=cursor,
                posts_only=True,
                exclude_authors=hidden,
            ),
            before,
            lambda post: float(post["created_at"]),
            filter_page=keep_tag,
        )
        return annotate_reposts(
            conn,
            annotate_repost_counts(
                conn,
                annotate_quotes(
                    conn,
                    annotate_quote_counts(
                        conn,
                        annotate_replies(
                            conn,
                            annotate_edits(
                                conn, annotate_reactions(conn, posts, viewer=viewer)
                            ),
                            muted_keywords=keywords,
                            exclude_authors=silenced,
                            blocked_authors=blocked,
                        ),
                    ),
                    muted_keywords=keywords,
                    exclude_authors=silenced,
                    blocked_authors=blocked,
                ),
                viewer=viewer,
            ),
            muted_keywords=keywords,
            exclude_authors=silenced,
            blocked_authors=blocked,
        )
    clean_authors = [
        _require_hex(author, AUTHOR_HEX_LENGTH, "author") for author in authors or []
    ]
    clean_authors = [author for author in clean_authors if author not in hidden]
    if not clean_authors:
        return []
    placeholders = ", ".join("?" for _ in clean_authors)
    def page(cursor: Optional[float]) -> List[Dict[str, Any]]:
        params: List[Any] = list(clean_authors)
        where = (
            f"linked = 1 AND kind IN ({TIMELINE_KINDS_SQL}) "
            f"AND author IN ({placeholders}) AND {_NOT_DELETED}"
        )
        if cursor is not None:
            where += " AND created_at < ?"
            params.append(float(cursor))
        params.append(MAX_FEED_POSTS)
        rows = conn.execute(
            f"SELECT {', '.join(_POST_COLUMNS)} FROM posts WHERE {where} "
            "ORDER BY created_at DESC, hash DESC LIMIT ?",
            params,
        ).fetchall()
        return _public_posts(rows)

    posts = _fill_unmuted(
        conn,
        keywords,
        limit,
        page,
        before,
        lambda post: float(post["created_at"]),
        filter_page=keep_tag,
    )
    return annotate_reposts(
        conn,
        annotate_repost_counts(
            conn,
            annotate_quotes(
                conn,
                annotate_quote_counts(
                    conn,
                    annotate_replies(
                        conn,
                        annotate_edits(
                            conn, annotate_reactions(conn, posts, viewer=viewer)
                        ),
                        muted_keywords=keywords,
                        exclude_authors=silenced,
                        blocked_authors=blocked,
                    ),
                ),
                muted_keywords=keywords,
                exclude_authors=silenced,
                blocked_authors=blocked,
            ),
            viewer=viewer,
        ),
        muted_keywords=keywords,
        exclude_authors=silenced,
        blocked_authors=blocked,
    )


def author_timeline(
    conn: sqlite3.Connection,
    author: str,
    *,
    limit: int = 30,
    viewer: Optional[str] = None,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
    tag: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return one author's posts newest first, with muted posts skipped.

    Pages through the author's chain until *limit* unmuted posts are
    collected, so muted posts never shrink the result.  Reply previews
    are redacted for muted parents, as in :func:`timeline`.  The page
    reads rows with their sequence numbers attached — :func:`feed`
    strips ``seq`` from its results, so paging on it would fail as
    soon as a second page was needed.  A blocked author (named in
    *exclude_authors*) shows nothing at all, and ``exclude_all``
    hides every author — the display side of a block-list that
    failed closed.  When *tag* is given, only posts carrying that tag
    are returned, as in :func:`timeline`.
    """
    if exclude_all:
        return []
    author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
    blocked = set(_clean_excluded(exclude_authors))
    if author in blocked:
        return []
    keywords = _muted_keyword_list(conn)
    silenced = muted_author_set(conn)
    wanted_tag = clean_tag(tag) if tag is not None else None
    keep_tag = _tag_page_filter(
        conn, wanted_tag, keywords=keywords, blocked_authors=blocked
    )

    def page(cursor: int) -> List[Dict[str, Any]]:
        rows = conn.execute(
            f"SELECT {', '.join(_POST_COLUMNS)} FROM posts "
            "WHERE author = ? AND linked = 1 AND seq > ? "
            f"AND kind IN ({TIMELINE_KINDS_SQL}) AND {_NOT_DELETED} "
            "ORDER BY seq ASC LIMIT ?",
            (author, max(0, int(cursor)), MAX_FEED_POSTS),
        ).fetchall()
        return [_row_to_post_with_meta(row) for row in rows]

    posts = _fill_unmuted(
        conn,
        keywords,
        limit,
        page,
        0,
        lambda post: int(post["seq"]),
        filter_page=keep_tag,
    )
    return annotate_reposts(
        conn,
        annotate_repost_counts(
            conn,
            annotate_quotes(
                conn,
                annotate_quote_counts(
                    conn,
                    annotate_replies(
                        conn,
                        annotate_edits(
                            conn,
                            annotate_reactions(
                                conn, list(reversed(posts)), viewer=viewer
                            ),
                        ),
                        muted_keywords=keywords,
                        exclude_authors=silenced,
                        blocked_authors=blocked,
                    ),
                ),
                muted_keywords=keywords,
                exclude_authors=silenced,
                blocked_authors=blocked,
            ),
            viewer=viewer,
        ),
        muted_keywords=keywords,
        blocked_authors=blocked,
    )


MAX_TOP_POSTS = 100


def top_posts(
    conn: sqlite3.Connection,
    *,
    author: Optional[str] = None,
    limit: int = 10,
    viewer: Optional[str] = None,
    ruleset: Optional[Ruleset] = None,
    exclude_authors: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Return the most-liked posts in the local store, best first.

    Posts by *exclude_authors* (blocked) are dropped in the query, before
    the limit is applied, so they never take up result slots.  A reply
    to a blocked author's post keeps its parent preview redacted, as in
    the timeline.  The exclusion is a single JSON parameter, so a large
    block-list cannot exceed SQLite's bound-variable limit.

    When *ruleset* is given, kinds it hides are dropped before the limit
    is applied, so hidden entries never take up result slots.  If it
    hides both reaction kinds, hidden likes do not influence the order
    and posts rank by recency.

    Ranking is by derived like count, then newest first (the same
    ``created_at DESC, hash DESC`` order as the timeline).  Posts with
    no likes sort last by recency, so the ranking is still useful
    before anything has been liked.  Only linked ``post`` and ``reply``
    entries rank: reaction entries never appear as items, and posts
    their author deleted never appear either.  Posts matching a muted
    keyword (see :func:`muted_keywords`) are skipped, and a reply to
    one keeps its ``reply_to`` preview text blanked, as in the
    timeline.  Posts by muted authors never rank either, and a reply
    to one keeps no parent preview at all, as in the timeline —
    muting is display-only, so entries stay stored and unmuting
    restores them.  An explicit *author* is an explicit lookup, like
    ``timeline --author``: their posts rank even when they are muted,
    but a reply of theirs to a muted author's post still keeps no
    parent preview, so the mute never leaks through a preview.
    Results carry the timeline's ``likes`` / ``liked`` / ``own``
    annotations and ``reply_to`` parent summaries.

    The ranking needs every candidate's like count before any
    ordering is possible, so the scan reads the whole visible store
    in one query — the same scale :func:`search_posts` already scans,
    and a personal mesh feed keeps the local post table small.
    """
    count = max(1, min(int(limit), MAX_TOP_POSTS))
    keywords = _muted_keyword_list(conn)
    silenced = muted_author_set(conn)
    blocked = set(_clean_excluded(exclude_authors))
    params: List[Any] = []
    where = f"linked = 1 AND kind IN ('post', 'reply') AND {_NOT_DELETED}"
    if silenced and author is None:
        # One JSON parameter instead of one placeholder per author, so a
        # large mute list cannot exceed SQLite's bound-variable limit —
        # the same pattern feed() uses.
        where += " AND author NOT IN (SELECT value FROM json_each(?))"
        params.append(json.dumps(sorted(silenced)))
    if blocked:
        where += " AND author NOT IN (SELECT value FROM json_each(?))"
        params.append(json.dumps(sorted(blocked)))
    if author is not None:
        author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
        where += " AND author = ?"
        params.append(author)
    rows = conn.execute(
        f"SELECT {', '.join(_POST_COLUMNS)} FROM posts WHERE {where} "
        "ORDER BY created_at DESC, hash DESC",
        params,
    ).fetchall()
    # Edits first, so keyword muting and the ruleset see the text that
    # is actually displayed.
    candidates = annotate_edits(conn, _public_posts(rows))
    candidates = without_muted_keywords(conn, candidates, keywords)
    if ruleset is not None:
        candidates = [
            post
            for post in candidates
            if ruleset.displays(str(post.get("kind") or KIND_POST))
        ]
    if ruleset is None or ruleset.displays(KIND_LIKE) or ruleset.displays(KIND_UNLIKE):
        # Batch the reaction lookup so large stores stay under the
        # SQLite bound-parameter limit.
        for batch in _thread_batches(candidates, _SQL_BATCH):
            annotate_reactions(conn, batch, viewer=viewer)
        candidates.sort(
            key=lambda post: (
                int(post.get("likes") or 0),
                float(post.get("created_at") or 0),
                str(post.get("hash") or ""),
            ),
            reverse=True,
        )
    else:
        # Hidden reactions must not shape the order; the scan is already
        # newest first, so keep that order and only attach ownership.
        for post in candidates:
            post["own"] = viewer is not None and post["author"] == viewer
    shown = annotate_replies(
        conn,
        candidates[:count],
        muted_keywords=keywords,
        exclude_authors=silenced,
        blocked_authors=blocked,
    )
    return annotate_repost_counts(
        conn,
        annotate_quote_counts(conn, shown),
        viewer=viewer,
    )


def _fill_unmuted(
    conn: sqlite3.Connection,
    keywords: List[str],
    limit: int,
    fetch: Callable[[Any], List[Dict[str, Any]]],
    cursor: Any,
    next_cursor: Callable[[Dict[str, Any]], Any],
    filter_page: Optional[
        Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]
    ] = None,
) -> List[Dict[str, Any]]:
    """Collect up to *limit* unmuted posts by paging through *fetch*.

    *fetch* receives a cursor and returns one page of at most
    ``MAX_FEED_POSTS`` posts in feed order; *next_cursor* derives the
    cursor for the page after the last post it returned.  A short page
    means the feed is exhausted.  *filter_page*, when given, further
    narrows each fetched page after the muted-keyword filter — paging
    keeps going until *limit* posts survive both filters, so a narrow
    filter never shrinks the result below what is available.
    """
    wanted = max(1, min(int(limit), 200))
    kept: List[Dict[str, Any]] = []
    while len(kept) < wanted:
        page = fetch(cursor)
        visible = without_muted_keywords(conn, page, keywords)
        if filter_page is not None:
            visible = filter_page(visible)
        kept.extend(visible)
        if len(page) < MAX_FEED_POSTS:
            break
        cursor = next_cursor(page[-1])
    return kept[:wanted]


# Upper bound on bound parameters per batched query.  SQLite builds often cap
# host parameters at 999, so wide generations, long threads and large edit
# lookups are processed in batches of this size.
_SQL_BATCH = 500


def _thread_batches(items: List[Any], size: int) -> Iterator[List[Any]]:
    for offset in range(0, len(items), size):
        yield items[offset : offset + size]


def thread(
    conn: sqlite3.Connection,
    ref: str,
    *,
    viewer: Optional[str] = None,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
) -> List[Dict[str, Any]]:
    """Return the thread rooted at the post *ref* names, oldest first.

    *ref* may name any entry in the thread — a reply resolves to its
    root post, walking up through cached, linked, non-deleted parents,
    and a repost resolves to the original post it points at.  The root
    is the highest such ancestor; a quote is its own root, so naming a
    quote (or a reply to one) shows the quote and the replies to it.
    A reply whose parent has not arrived yet still anchors its own
    visible thread.  Every entry carries a ``depth`` (the root is 0)
    and the derived ``likes`` / ``liked`` / ``own`` annotations, like
    the timeline.  Replies appear in depth-first order — a reply,
    then its own replies, then the next sibling — with siblings oldest
    first, so the thread reads as a conversation.  Deleted and unlinked
    entries never appear, and a reply to a deleted entry is hidden with
    its parent's subtree rather than re-parented.  Replies by muted
    authors are hidden the same way, subtree included — except the
    entry the caller named, which always anchors the thread even when
    its author is muted, and a muted ancestor above it, which simply
    ends the climb instead of being promoted to the root.  An unknown,
    ambiguous or deleted reference raises :class:`TanError`, exactly as
    :func:`resolve_post` does; so does a reference to an unlinked entry
    or to a reply beneath a deleted entry, since showing it would
    present a partial thread.  A reply matching a muted keyword (see
    :func:`muted_keywords`) is hidden with its whole subtree the same
    way, but the entry the reference names always anchors the thread
    even when its own text matches — an explicit lookup — and a muted
    ancestor above it ends the climb instead of being promoted to the
    root.
    """
    start = resolve_post(conn, ref)
    if not start.get("linked"):
        # A cached entry still awaiting its chain predecessor is not shown.
        raise TanError(f"post has not linked yet: {ref!r}")
    excluded = set(_clean_excluded(exclude_authors))
    if exclude_all:
        raise TanError("cannot show thread: the block-list is unusable")
    if start.get("author") in excluded:
        raise TanError(f"post is from a blocked author: {ref!r}")
    silenced = muted_author_set(conn)
    keywords = _muted_keyword_list(conn)
    muted_pattern = _muted_keyword_pattern(keywords)
    root = start
    ancestors = {root["hash"]}
    # Once a muted entry is met (the named entry itself, or an ancestor),
    # the visible root stops moving, but the climb continues so that
    # deleted ancestors above it are still rejected.
    anchored = start["author"] in silenced or _text_matches_muted_keywords(
        str(start.get("text") or ""), muted_pattern
    )
    cursor = start
    while cursor.get("kind") in (KIND_REPLY, KIND_REPOST) and cursor.get("target"):
        parent = get_post(conn, str(cursor["target"]))
        if parent is None or parent.get("kind") not in TIMELINE_KINDS:
            break
        if parent["hash"] in ancestors:
            break
        if _is_deleted(conn, parent):
            # Replies under a deleted entry stay hidden, even when named
            # directly, so refuse rather than showing a fragment.  The
            # deletion check comes before the unlinked shortcut: a
            # deleted parent whose chain never linked up hides its
            # subtree just the same.
            raise TanError(f"post has been deleted: {ref!r}")
        if parent["author"] in excluded:
            raise TanError(f"post is from a blocked author: {ref!r}")
        if not parent.get("linked"):
            break
        ancestors.add(parent["hash"])
        cursor = parent
        if not anchored and (
            parent["author"] in silenced
            or _text_matches_muted_keywords(
                str(parent.get("text") or ""), muted_pattern
            )
        ):
            # A muted ancestor ends the visible climb: the named entry
            # anchors its own thread rather than promoting the muted
            # parent to the root.
            anchored = True
        if not anchored:
            root = parent
    root = _canonical_post(root)
    # Collect descendants level by level — one query per depth, so a
    # deep thread costs a handful of queries instead of one per entry —
    # then flatten depth-first, siblings oldest first.  The ``seen``
    # set also stops a pathological target cycle from looping forever;
    # it is separate from ``ancestors`` above, because an ancestor of
    # the named entry is also one of the root's children.
    levels: List[List[Dict[str, Any]]] = [[root]]
    frontier = [root["hash"]]
    seen = {root["hash"]}
    while frontier:
        rows: List[sqlite3.Row] = []
        for batch in _thread_batches(frontier, _SQL_BATCH):
            placeholders = ", ".join("?" for _ in batch)
            rows.extend(
                conn.execute(
                    f"SELECT {', '.join(_POST_COLUMNS)} FROM posts "
                    f"WHERE linked = 1 AND kind IN ('post', 'reply') "
                    f"AND target IN ({placeholders}) AND {_NOT_DELETED}",
                    batch,
                ).fetchall()
            )
        # Batches are merged here, so re-sort the whole generation.
        generation = sorted(
            _public_posts(rows), key=lambda post: (post["created_at"], post["hash"])
        )
        # A muted reply hides its whole subtree: it never enters the
        # frontier, so its own replies are never collected.  Replies by
        # muted authors are hidden the same way.
        children = [
            child
            for child in generation
            if child["hash"] not in seen
            and child["author"] not in silenced
            and child["author"] not in excluded
            and not _text_matches_muted_keywords(
                str(child.get("text") or ""), muted_pattern
            )
        ]
        for child in children:
            seen.add(child["hash"])
        if not children:
            break
        levels.append(children)
        frontier = [child["hash"] for child in children]
    by_parent: Dict[str, List[Dict[str, Any]]] = {}
    for level in levels[1:]:
        for child in level:
            by_parent.setdefault(str(child.get("target") or ""), []).append(child)
    ordered: List[Dict[str, Any]] = []
    stack: List[Tuple[Dict[str, Any], int]] = [(root, 0)]
    while stack:
        entry, depth = stack.pop()
        entry["depth"] = depth
        ordered.append(entry)
        for child in reversed(by_parent.get(entry["hash"], [])):
            stack.append((child, depth + 1))
    # Annotation fills each entry in place, so batching keeps the reaction
    # lookup within the parameter limit for long threads.
    for batch in _thread_batches(ordered, _SQL_BATCH):
        annotate_repost_counts(
            conn,
            annotate_quotes(
                conn,
                annotate_quote_counts(
                    conn,
                    annotate_edits(conn, annotate_reactions(conn, batch, viewer=viewer)),
                ),
                muted_keywords=keywords,
                exclude_authors=silenced,
                blocked_authors=excluded,
            ),
            viewer=viewer,
        )
    return ordered


# Search is a display surface like the timeline, so it shares the
# timeline's ceiling rather than the feed's 20-post page.
MAX_SEARCH_POSTS = 200


def search_posts(
    conn: sqlite3.Connection,
    query: str,
    *,
    author: Optional[str] = None,
    limit: int = DEFAULT_FEED_LIMIT,
    viewer: Optional[str] = None,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
) -> List[Dict[str, Any]]:
    """Search stored posts by keyword, newest first.

    Only linked ``post`` and ``quote`` entries match: reaction entries
    never appear as results, and posts their author deleted are
    excluded the same way the timeline excludes them.  The match is a
    case-insensitive substring search over the post's current text — an
    edited post matches its edited text, not the original — with
    ``casefold`` on both sides, so it folds Unicode case (``Ä`` matches
    ``ä``) — and the search reads only the local store: it makes no
    network requests.  Results carry the same like, quote and edit
    annotations as the timeline.

    The match runs in Python rather than as a SQL ``LIKE``: SQLite's
    ``LIKE`` folds ASCII case only, so a ``LIKE`` query would miss
    exactly the Unicode matches this promises.  The scan is over the
    local post table only, which a personal mesh feed keeps small.

    Search is an explicit lookup, so it ignores muted keywords
    entirely: searching for a word you muted still finds it.  Posts by
    muted authors never match an open search either; searching with an
    explicit *author* is a direct lookup and still shows their posts.
    """
    if not isinstance(query, str) or not query.strip():
        raise TanError("search text must not be empty")
    if exclude_all:
        return []
    excluded = set(_clean_excluded(exclude_authors))
    count = max(1, min(int(limit), MAX_SEARCH_POSTS))
    needle = query.strip().casefold()
    params: List[Any] = []
    where = f"linked = 1 AND kind IN ('post', 'quote') AND {_NOT_DELETED}"
    if author is not None:
        author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
        if author in excluded:
            return []
        where += " AND author = ?"
        params.append(author)
    rows = conn.execute(
        f"SELECT {', '.join(_POST_COLUMNS)} FROM posts WHERE {where} "
        "ORDER BY created_at DESC, hash DESC",
        params,
    ).fetchall()
    candidates = _public_posts(rows)
    states = edit_states(conn, [post["hash"] for post in candidates])
    matches = [
        post
        for post in candidates
        if needle
        in (states[post["hash"]]["text"] if post["hash"] in states else post["text"]).casefold()
    ]
    if author is None:
        silenced = muted_author_set(conn)
        matches = [
            post
            for post in matches
            if str(post["author"]) not in silenced
            and str(post["author"]) not in excluded
        ]
    return annotate_repost_counts(
        conn,
        annotate_quotes(
            conn,
            annotate_quote_counts(
                conn,
                annotate_edits(
                    conn, annotate_reactions(conn, matches[:count], viewer=viewer)
                ),
            ),
            blocked_authors=excluded,
        ),
        viewer=viewer,
    )


# Tags share search's ceiling: they are a display surface like the
# timeline, not a sync page.
MAX_TAGS = 100


def tags(
    conn: sqlite3.Connection,
    *,
    limit: int = 30,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
) -> List[Dict[str, Any]]:
    """Tags in stored posts, with the number of posts carrying each.

    Only linked ``post``, ``reply`` and ``quote`` entries count — a
    repost carries no text of its own, so it adds nothing, and its
    original is counted once on its own.  Each post counts once per
    tag, on its current text: an edited post contributes the tags of
    its edited text, not the original.  Posts their author deleted
    never count, and posts by muted or blocked authors never count
    either.  Muted keywords are ignored — this is an explicit lookup,
    the way search is.  Results are sorted by count, most-used first,
    then by tag, and carry ``{"tag": ..., "count": ...}``.
    """
    if exclude_all:
        return []
    excluded = set(_clean_excluded(exclude_authors))
    hidden = excluded | muted_author_set(conn)
    count = max(1, min(int(limit), MAX_TAGS))
    where = (
        "linked = 1 AND kind IN ('post', 'reply', 'quote') "
        f"AND {_NOT_DELETED}"
    )
    rows = conn.execute(
        f"SELECT {', '.join(_POST_COLUMNS)} FROM posts WHERE {where} "
        "ORDER BY created_at DESC, hash DESC",
    ).fetchall()
    posts = _public_posts(rows)
    states = edit_states(conn, [post["hash"] for post in posts])
    counts: Dict[str, int] = {}
    for post in posts:
        if str(post["author"]) in hidden:
            continue
        text = states.get(post["hash"], {}).get("text", post["text"])
        for tag in extract_tags(text):
            counts[tag] = counts.get(tag, 0) + 1
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [{"tag": tag, "count": n} for tag, n in ranked[:count]]


# Mentions share search's ceiling: they are a display surface like the
# timeline, not a sync page.
MAX_MENTIONS_POSTS = MAX_SEARCH_POSTS


def _hidden_under_deleted(conn: sqlite3.Connection, row: sqlite3.Row) -> bool:
    """Whether the reply *row* sits beneath a deleted ancestor.

    Walks up from the reply's target the same way :func:`thread` walks
    up to its root, stopping where the thread view stops, and reports
    True when any entry on that chain has been deleted by its author.
    """
    seen = {str(row["hash"])}
    parent_hash = str(row["target"] or "")
    while parent_hash and parent_hash not in seen:
        seen.add(parent_hash)
        parent = get_post(conn, parent_hash)
        if parent is None or parent.get("kind") not in TIMELINE_KINDS:
            return False
        if _is_deleted(conn, parent):
            return True
        if not parent.get("linked") or parent.get("kind") != KIND_REPLY:
            return False
        parent_hash = str(parent.get("target") or "")
    return False


def mentions(
    conn: sqlite3.Connection,
    viewer: str,
    *,
    limit: int = DEFAULT_FEED_LIMIT,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
) -> List[Dict[str, Any]]:
    """Return stored replies to the viewer's own posts, newest first.

    A mention is a linked ``reply`` entry whose target is a linked
    ``post`` or ``reply`` authored by *viewer* — a reply to something
    you wrote, including a reply to one of your own replies.  A reply
    you wrote to your own post is not a mention of you.  The same
    hiding rules as the thread view apply: a reply its author deleted
    never appears, and neither does a reply to a post you deleted.
    Unlinked replies never appear either.  The console's hiding rules
    apply as in :func:`timeline`: replies by muted or blocked authors
    (``exclude_authors``) and replies matching a muted keyword never
    appear, and ``exclude_all`` hides every mention — the display side
    of a block-list that failed closed.  Pages are read until *limit*
    visible mentions are collected or the store is exhausted.  Results
    carry the same like annotations and ``reply_to`` parent summary as
    the timeline, and the query reads only the local store: it makes
    no network requests.
    """
    viewer = _require_hex(viewer, AUTHOR_HEX_LENGTH, "viewer")
    if exclude_all:
        return []
    count = max(1, min(int(limit), MAX_MENTIONS_POSTS))
    silenced = muted_author_set(conn)
    blocked = set(_clean_excluded(exclude_authors))
    hidden = sorted(silenced | blocked)
    keywords = _muted_keyword_list(conn)
    author_clause = ""
    if hidden:
        # One JSON parameter instead of one placeholder per author,
        # so a large mute/block list cannot exceed SQLite's
        # bound-variable limit — the same pattern :func:`feed` uses.
        author_clause = "AND r.author NOT IN (SELECT value FROM json_each(?)) "
    kept: List[Dict[str, Any]] = []
    cursor: Optional[Tuple[float, str]] = None
    while len(kept) < count:
        where = "r.linked = 1 AND r.kind = 'reply' AND r.author != ? " + author_clause
        params: List[Any] = [viewer]
        if hidden:
            params.append(json.dumps(hidden))
        if cursor is not None:
            where += "AND (r.created_at < ? OR (r.created_at = ? AND r.hash < ?)) "
            params.extend([cursor[0], cursor[0], cursor[1]])
        where += (
            "AND NOT EXISTS (SELECT 1 FROM posts AS d WHERE d.kind = 'delete' "
            "AND d.linked = 1 AND d.target = r.hash AND d.author = r.author) "
            "AND EXISTS (SELECT 1 FROM posts AS p WHERE p.hash = r.target "
            "AND p.linked = 1 AND p.kind IN ('post', 'reply') AND p.author = ? "
            "AND NOT EXISTS (SELECT 1 FROM posts AS d WHERE d.kind = 'delete' "
            "AND d.linked = 1 AND d.target = p.hash AND d.author = p.author)) "
        )
        params.append(viewer)
        rows = conn.execute(
            f"SELECT {', '.join(_POST_COLUMNS)} FROM posts AS r WHERE {where}"
            "ORDER BY r.created_at DESC, r.hash DESC LIMIT ?",
            [*params, MAX_FEED_POSTS],
        ).fetchall()
        if not rows:
            break
        # A reply beneath a deleted ancestor is hidden with that subtree, as
        # the thread view hides it.  Hidden rows still advance the cursor so
        # paging continues past them.
        visible = [row for row in rows if not _hidden_under_deleted(conn, row)]
        kept.extend(without_muted_keywords(conn, _public_posts(visible), keywords))
        if len(rows) < MAX_FEED_POSTS:
            break
        last = rows[-1]
        cursor = (float(last["created_at"]), str(last["hash"]))
    posts = annotate_repost_counts(conn, kept[:count], viewer=viewer)
    return annotate_replies(
        conn,
        annotate_edits(
            conn, annotate_reactions(conn, posts, viewer=viewer)
        ),
        muted_keywords=keywords,
        exclude_authors=silenced,
        blocked_authors=blocked,
    )


def follow(
    conn: sqlite3.Connection,
    author: str,
    *,
    name: str = "",
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Add *author* to the local follow list."""
    author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
    clean_name = sanitize_name(name, fallback="") if name else ""
    timestamp = float(now if now is not None else time.time())
    count = conn.execute("SELECT COUNT(*) AS n FROM follows").fetchone()["n"]
    existing = conn.execute(
        "SELECT author FROM follows WHERE author = ?", (author,)
    ).fetchone()
    if existing is None and int(count) >= MAX_FOLLOWS:
        raise TanError(f"follow list is limited to {MAX_FOLLOWS} authors")
    with conn:
        conn.execute(
            "INSERT INTO follows(author, name, added_at) VALUES(?, ?, ?) "
            "ON CONFLICT(author) DO UPDATE SET "
            "name = CASE WHEN excluded.name != '' THEN excluded.name ELSE follows.name END",
            (author, clean_name, timestamp),
        )
    return {"author": author, "name": clean_name, "added_at": timestamp}


def unfollow(conn: sqlite3.Connection, author: str) -> bool:
    author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
    with conn:
        cursor = conn.execute("DELETE FROM follows WHERE author = ?", (author,))
    return cursor.rowcount > 0


def following(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT author, name, added_at FROM follows ORDER BY added_at ASC"
    ).fetchall()
    return [
        {
            "author": str(row["author"]),
            "name": str(row["name"]),
            "added_at": float(row["added_at"]),
        }
        for row in rows
    ]


def suggest_authors(
    conn: sqlite3.Connection,
    *,
    viewer: Optional[str] = None,
    limit: int = DEFAULT_SUGGEST_LIMIT,
    exclude: Optional[Iterable[str]] = None,
) -> List[Dict[str, Any]]:
    """Rank cached authors the viewer does not follow yet.

    Candidates are authors with at least one linked, non-deleted post or
    reply in the local store, excluding followed authors, the viewer, and
    any authors in ``exclude`` (for example blocked authors). Excluded
    authors are dropped before ``limit`` is applied.
    Each candidate scores ``SUGGEST_FOLLOWED_WEIGHT`` per distinct author
    the viewer follows who liked or replied to one of the candidate's
    posts or replies, plus one per like on the candidate's posts (so a
    like from a followed author counts in both terms). With no follows
    the ranking degrades to the most-liked cached authors. Ties break by
    most recent post, then author hash, so the order is stable.
    """
    followed = {record["author"]: record["name"] for record in following(conn)}
    excluded = set(followed)
    if exclude is not None:
        excluded.update(exclude)
    if viewer is not None:
        excluded.add(_require_hex(viewer, AUTHOR_HEX_LENGTH, "viewer"))
    limit = max(0, int(limit))
    if limit == 0:
        return []

    candidates: Dict[str, Dict[str, Any]] = {}
    rows = conn.execute(
        "SELECT author, name, created_at FROM posts "
        "WHERE linked = 1 AND kind IN ('post', 'reply') "
        f"AND {_NOT_DELETED} ORDER BY created_at DESC"
    ).fetchall()
    for row in rows:
        # Rows arrive newest-first, so the first row per author carries
        # the name on their latest post.
        author = str(row["author"])
        if author in excluded or author in candidates:
            continue
        candidates[author] = {
            "author": author,
            "name": str(row["name"] or ""),
            "last_post": float(row["created_at"]),
        }

    for author, candidate in candidates.items():
        # Post hashes drive the post count and the like targets; reply
        # hashes join them as engagement targets, so a reply to one of
        # the candidate's replies counts as engagement with the candidate
        # too. The two lists stay separate so "posts" never counts replies.
        post_hashes = [
            str(row["hash"])
            for row in conn.execute(
                "SELECT hash FROM posts WHERE author = ? AND linked = 1 "
                f"AND kind = 'post' AND {_NOT_DELETED}",
                (author,),
            ).fetchall()
        ]
        reply_hashes = [
            str(row["hash"])
            for row in conn.execute(
                "SELECT hash FROM posts WHERE author = ? AND linked = 1 "
                f"AND kind = 'reply' AND {_NOT_DELETED}",
                (author,),
            ).fetchall()
        ]
        # Each active like counts once per post, so likes are summed across
        # the candidate's posts rather than deduplicated per liker.
        # Hashes are looked up in bounded batches: a prolific author must
        # not trip SQLite's host-parameter limit.
        like_total = 0
        likers: Set[str] = set()
        for batch in _thread_batches(post_hashes, _SQL_BATCH):
            states = reaction_states(conn, batch)
            for reactors in states.values():
                for reactor, kind in reactors.items():
                    if kind == KIND_LIKE:
                        like_total += 1
                        likers.add(reactor)
        repliers: Set[str] = set()
        for batch in _thread_batches(post_hashes + reply_hashes, _SQL_BATCH):
            placeholders = ", ".join("?" for _ in batch)
            repliers.update(
                str(row["author"])
                for row in conn.execute(
                    "SELECT DISTINCT author FROM posts WHERE kind = 'reply' "
                    f"AND linked = 1 AND {_NOT_DELETED} "
                    f"AND target IN ({placeholders})",
                    batch,
                ).fetchall()
            )
        followed_likers = sorted(a for a in likers if a in followed)
        followed_repliers = sorted(a for a in repliers if a in followed)
        engagers = sorted(set(followed_likers) | set(followed_repliers))
        candidate["posts"] = len(post_hashes)
        candidate["likes"] = like_total
        candidate["score"] = SUGGEST_FOLLOWED_WEIGHT * len(engagers) + like_total
        candidate["engagers"] = [_engager_name(a, followed) for a in engagers]
        reasons: List[str] = []
        if followed_likers:
            reasons.append("liked by " + _format_engagers(followed_likers, followed))
        if followed_repliers:
            reasons.append(
                "replied to by " + _format_engagers(followed_repliers, followed)
            )
        if like_total:
            reasons.append(f"{like_total} like(s)")
        if not reasons:
            reasons.append(f"{len(post_hashes)} post(s)")
        candidate["reason"] = "; ".join(reasons)

    ranked = sorted(
        candidates.values(),
        key=lambda item: (-item["score"], -item["last_post"], item["author"]),
    )
    return ranked[:limit]


def _engager_name(author: str, followed: Dict[str, str]) -> str:
    """Display name for a followed engager, falling back to a short hash."""
    name = followed.get(author) or ""
    if name:
        return name
    return author[:8]


def _format_engagers(authors: List[str], followed: Dict[str, str]) -> str:
    """Comma-joined engager names, capped so the reason stays one line."""
    names = [_engager_name(author, followed) for author in authors]
    if len(names) > 2:
        return ", ".join(names[:2]) + f", and {len(names) - 2} more"
    return ", ".join(names)


def _normalize_keyword(keyword: Any) -> str:
    """Return the casefolded form of *keyword*, or raise :class:`TanError`.

    The casefolded form is the storage key, so ``Crypto`` and ``crypto``
    are the same mute; the display form the user typed is kept
    separately.  A keyword is one word or phrase — it may contain
    spaces, but it must not be empty and must fit in
    ``MAX_KEYWORD_LENGTH`` characters.  Internal whitespace is
    collapsed to single spaces first, so ``climate  change`` and
    ``climate change`` are the same mute.
    """
    if not isinstance(keyword, str) or not keyword.strip():
        raise TanError("keyword must not be empty")
    clean = " ".join(str(keyword).split())
    if len(clean) > MAX_KEYWORD_LENGTH:
        raise TanError(f"keyword is limited to {MAX_KEYWORD_LENGTH} characters")
    return clean.casefold()


def _muted_keyword_pattern(keywords: Sequence[str]) -> Optional["re.Pattern[str]"]:
    """Compile one whole-word matcher for the given casefolded keywords.

    The match is a case-insensitive whole-word (or whole-phrase) match
    over casefolded text: ``art`` never matches ``start``, while
    ``climate change`` matches the phrase.  The ``(?<!\\w)`` /
    ``(?!\\w)`` guards are word boundaries that also work when a
    keyword starts or ends with a non-word character, where ``\\b``
    would not.  Inside a phrase, any run of whitespace matches — a
    post saying ``climate\\nchange`` is hidden by a ``climate change``
    mute.  Returns ``None`` when there is nothing to match.
    """
    clean = [keyword.casefold() for keyword in keywords if keyword.strip()]
    if not clean:
        return None
    alternatives = [
        r"\s+".join(re.escape(part) for part in keyword.split())
        for keyword in clean
    ]
    return re.compile(
        r"(?<!\w)(?:" + "|".join(alternatives) + r")(?!\w)"
    )


def _text_matches_muted_keywords(
    text: str, pattern: Optional["re.Pattern[str]"]
) -> bool:
    return pattern is not None and pattern.search(text.casefold()) is not None


def mute_keyword(
    conn: sqlite3.Connection,
    keyword: str,
    *,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Mute *keyword* in local views.  Re-muting is a no-op returning
    the original record, so the first mute time is kept."""
    folded = _normalize_keyword(keyword)
    display = str(keyword).strip()
    timestamp = float(now if now is not None else time.time())
    existing = conn.execute(
        "SELECT keyword, display, added_at FROM muted_keywords WHERE keyword = ?",
        (folded,),
    ).fetchone()
    if existing is not None:
        return {
            "keyword": str(existing["keyword"]),
            "display": str(existing["display"]),
            "added_at": float(existing["added_at"]),
        }
    count = conn.execute("SELECT COUNT(*) AS n FROM muted_keywords").fetchone()["n"]
    if int(count) >= MAX_MUTED_KEYWORDS:
        raise TanError(
            f"muted keyword list is limited to {MAX_MUTED_KEYWORDS} keywords"
        )
    with conn:
        conn.execute(
            "INSERT INTO muted_keywords(keyword, display, added_at) "
            "VALUES(?, ?, ?)",
            (folded, display, timestamp),
        )
    return {"keyword": folded, "display": display, "added_at": timestamp}


def unmute_keyword(conn: sqlite3.Connection, keyword: str) -> bool:
    """Remove the mute for *keyword*.  Returns whether one was removed."""
    folded = _normalize_keyword(keyword)
    with conn:
        cursor = conn.execute(
            "DELETE FROM muted_keywords WHERE keyword = ?", (folded,)
        )
    return cursor.rowcount > 0


def muted_keywords(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Muted keywords, newest first."""
    rows = conn.execute(
        "SELECT keyword, display, added_at FROM muted_keywords "
        "ORDER BY added_at DESC"
    ).fetchall()
    return [
        {
            "keyword": str(row["keyword"]),
            "display": str(row["display"]),
            "added_at": float(row["added_at"]),
        }
        for row in rows
    ]


def mute(
    conn: sqlite3.Connection,
    author: str,
    *,
    name: str = "",
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Mute *author*: hide their posts from local views, keep storing them.

    Muting is display-only, unlike blocking at admission: the author's
    entries keep syncing and stay stored, so unmuting restores the full
    history.  Re-muting an already-muted author keeps the original
    ``muted_at`` and only refreshes the stored name when one is given.
    """
    author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
    clean_name = sanitize_name(name, fallback="") if name else ""
    timestamp = float(now if now is not None else time.time())
    existing = conn.execute(
        "SELECT muted_at FROM muted_authors WHERE author = ?", (author,)
    ).fetchone()
    if existing is None:
        count = conn.execute("SELECT COUNT(*) AS n FROM muted_authors").fetchone()["n"]
        if int(count) >= MAX_MUTED:
            raise TanError(f"mute list is limited to {MAX_MUTED} authors")
    with conn:
        conn.execute(
            "INSERT INTO muted_authors(author, name, muted_at) VALUES(?, ?, ?) "
            "ON CONFLICT(author) DO UPDATE SET "
            "name = CASE WHEN excluded.name != '' THEN excluded.name ELSE muted_authors.name END, "
            "muted_at = muted_authors.muted_at",
            (author, clean_name, timestamp),
        )
    if existing is not None:
        timestamp = float(existing["muted_at"])
    return {"author": author, "name": clean_name, "muted_at": timestamp}


def unmute(conn: sqlite3.Connection, author: str) -> bool:
    author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
    with conn:
        cursor = conn.execute("DELETE FROM muted_authors WHERE author = ?", (author,))
    return cursor.rowcount > 0


def muted(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Muted authors, most recently muted first."""
    rows = conn.execute(
        "SELECT author, name, muted_at FROM muted_authors ORDER BY muted_at DESC"
    ).fetchall()
    return [
        {
            "author": str(row["author"]),
            "name": str(row["name"]),
            "muted_at": float(row["muted_at"]),
        }
        for row in rows
    ]


def _muted_keyword_list(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute("SELECT keyword FROM muted_keywords").fetchall()
    return [str(row["keyword"]) for row in rows]


def without_muted_keywords(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
    keywords: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Drop posts whose text matches a muted keyword, keeping the rest.

    Muting is display-only: entries stay stored and keep syncing, so
    unmuting restores them.  The match runs in Python over the given
    page — the same scale search already scans — rather than as SQL,
    because SQLite cannot express the whole-word match.  Pass
    *keywords* when the caller already loaded them to avoid a second
    lookup.
    """
    pattern = _muted_keyword_pattern(
        list(keywords) if keywords is not None else _muted_keyword_list(conn)
    )
    if pattern is None:
        return list(posts)
    return [
        post
        for post in posts
        if not _text_matches_muted_keywords(str(post.get("text") or ""), pattern)
    ]


def muted_author_set(conn: sqlite3.Connection) -> Set[str]:
    """Authors currently muted, as a set of destination hashes."""
    rows = conn.execute("SELECT author FROM muted_authors").fetchall()
    return {str(row["author"]) for row in rows}


def _resolve_saved_ref(conn: sqlite3.Connection, ref: str) -> str:
    """Resolve a saved-post reference to a cached post hash.

    Unlike :func:`resolve_post`, a post its author deleted still
    resolves: unsaving must keep working after the save's target is
    deleted.  A reference to nothing cached fails as unknown.
    """
    if not isinstance(ref, str) or not ref.strip():
        raise TanError("post reference must not be empty")
    value = ref.strip().lower()
    if len(value) == HASH_HEX_LENGTH:
        post = get_post(conn, _require_hex(value, HASH_HEX_LENGTH, "post"))
        if post is not None and post.get("kind", KIND_POST) in TIMELINE_KINDS:
            return str(post["hash"])
        raise TanError(f"unknown post: {ref!r}")
    if len(value) >= 8:
        try:
            bytes.fromhex(value)
        except ValueError:
            raise TanError(f"unknown post: {ref!r}") from None
        rows = conn.execute(
            "SELECT hash FROM posts WHERE kind IN ('post', 'reply') "
            "AND hash LIKE ? LIMIT 8",
            (value + "%",),
        ).fetchall()
        if len(rows) == 1:
            return str(rows[0]["hash"])
        if len(rows) > 1:
            raise TanError(f"{len(rows)} posts match prefix {value!r}")
    raise TanError(f"unknown post: {ref!r}")


def save_post(
    conn: sqlite3.Connection,
    ref: str,
    *,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Save a cached post to the local read-later list.

    Saves are private to this node — no signed entry, nothing gossiped.
    The target must be a cached, linked post or reply that has not been
    deleted; saving the same post twice is a no-op that reports the
    original save time.
    """
    post = resolve_post(conn, ref)
    if not post.get("linked"):
        raise TanError("post has not linked yet; sync its chain before saving")
    digest = str(post["hash"])
    timestamp = float(now if now is not None else time.time())
    # Take the write lock before checking for an existing save and the
    # capacity, so a competing save cannot slip in between check and insert.
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute(
            "SELECT saved_at FROM saved_posts WHERE hash = ?", (digest,)
        ).fetchone()
        if existing is not None:
            conn.rollback()
            return {
                "hash": digest,
                "author": str(post["author"]),
                "name": str(post.get("name") or ""),
                "saved_at": float(existing["saved_at"]),
                "already_saved": True,
            }
        count = conn.execute("SELECT COUNT(*) AS n FROM saved_posts").fetchone()["n"]
        if int(count) >= MAX_SAVED_POSTS:
            raise TanError(f"saved posts are limited to {MAX_SAVED_POSTS} entries")
        conn.execute(
            "INSERT INTO saved_posts(hash, saved_at) VALUES(?, ?)",
            (digest, timestamp),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return {
        "hash": digest,
        "author": str(post["author"]),
        "name": str(post.get("name") or ""),
        "saved_at": timestamp,
        "already_saved": False,
    }


def unsave_post(conn: sqlite3.Connection, ref: str) -> Dict[str, Any]:
    """Remove a post from the local read-later list.

    Returns the resolved post hash with whether a save was removed, so
    callers — including ``--json`` output — can identify the post even
    when *ref* was a short prefix.
    """
    digest = _resolve_saved_ref(conn, ref)
    with conn:
        cursor = conn.execute("DELETE FROM saved_posts WHERE hash = ?", (digest,))
    return {"hash": digest, "unsaved": cursor.rowcount > 0}


def saved_posts(
    conn: sqlite3.Connection,
    *,
    limit: int = DEFAULT_FEED_LIMIT,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """Saved posts, newest save first.

    A post deleted after being saved stays in the list with its text
    blanked and ``deleted`` set, so the save is not silently lost.
    *offset* pages through the list in save order, so a caller can
    reach saves older than one page.
    """
    count = max(1, min(int(limit), MAX_SAVED_POSTS))
    start = max(0, int(offset))
    rows = conn.execute(
        "SELECT p.*, s.saved_at FROM saved_posts s "
        "JOIN posts p ON p.hash = s.hash "
        "ORDER BY s.saved_at DESC, s.hash DESC LIMIT ? OFFSET ?",
        (count, start),
    ).fetchall()
    deleted = deleted_targets(conn)
    records: List[Dict[str, Any]] = []
    for row in rows:
        post = _row_to_post(row)
        post["saved_at"] = float(row["saved_at"])
        if str(post["hash"]) in deleted:
            post["deleted"] = True
            post["text"] = ""
        records.append(post)
    return records


def annotated_saved_posts(
    conn: sqlite3.Connection,
    *,
    limit: int = DEFAULT_FEED_LIMIT,
    offset: int = 0,
    viewer: Optional[str] = None,
    ruleset: Optional[Ruleset] = None,
) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """Saved posts with the timeline's annotations and muting applied.

    Reaction counts, reply parents, quote counts and quoted originals
    are attached exactly as :func:`timeline` does: a reply parent or
    quoted original from a muted author is redacted, and a parent
    matching a muted keyword has its text blanked.  Posts by muted
    authors or matching a muted keyword are dropped from the view —
    muting is display-only, so the saves stay stored and unmuting
    restores them.  Each returned post carries ``saved`` and
    ``saved_at``.

    Returns ``(posts, next_offset)``: up to *limit* visible posts
    starting at raw offset *offset*, and the raw offset to resume
    paging from, or ``None`` when the saved list is exhausted.  Muted
    entries and entries hidden by *ruleset* never shrink a page: raw
    rows are read until *limit* visible posts are collected or the
    list is exhausted, so a caller must page with the returned
    ``next_offset``, not with the number of posts it received.
    """
    silenced = muted_author_set(conn)
    keywords = _muted_keyword_list(conn)
    kept: List[Dict[str, Any]] = []
    raw_offset = max(0, int(offset))
    while len(kept) < limit:
        batch = saved_posts(conn, limit=limit - len(kept), offset=raw_offset)
        if not batch:
            break
        raw_offset += len(batch)
        batch = annotate_quotes(
            conn,
            annotate_quote_counts(
                conn,
                annotate_replies(
                    conn,
                    annotate_reactions(conn, batch, viewer=viewer),
                    muted_keywords=keywords,
                    exclude_authors=silenced,
                ),
            ),
            muted_keywords=keywords,
            exclude_authors=silenced,
        )
        annotate_repost_counts(conn, batch, viewer=viewer)
        for post in batch:
            post["saved"] = True
        batch = [post for post in batch if post["author"] not in silenced]
        batch = without_muted_keywords(conn, batch, keywords)
        if ruleset is not None:
            batch = apply_display_rules(batch, ruleset)
        kept.extend(batch)
    if len(kept) < limit:
        return kept, None
    total = conn.execute("SELECT COUNT(*) AS n FROM saved_posts").fetchone()["n"]
    return kept, raw_offset if raw_offset < int(total) else None




def author_stats(conn: sqlite3.Connection, author: str) -> Dict[str, Any]:
    author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
    row = conn.execute(
        "SELECT COUNT(*) AS posts, COALESCE(MAX(created_at), 0) AS last_post "
        f"FROM posts WHERE author = ? AND linked = 1 AND kind = 'post' AND {_NOT_DELETED}",
        (author,),
    ).fetchone()
    seq_row = conn.execute(
        "SELECT COALESCE(MAX(seq), 0) AS seq FROM posts "
        "WHERE author = ? AND linked = 1",
        (author,),
    ).fetchone()
    return {
        "author": author,
        "posts": int(row["posts"]),
        "seq": int(seq_row["seq"]),
        "last_post": float(row["last_post"]),
    }


def _author_stats_query(
    exclude_authors: Optional[Sequence[str]] = None,
) -> Tuple[str, List[str]]:
    """The per-author stats query shared by ``list_authors`` and
    ``count_authors``, with the block-list applied the same way.

    Returns the SQL and its parameters.  Callers short-circuit a
    block-list that failed closed before reaching this helper.
    """
    excluded = _clean_excluded(exclude_authors)
    excluded_sql = ""
    excluded_params: List[str] = []
    if excluded:
        excluded_sql = (
            f" AND posts.author NOT IN ({', '.join('?' for _ in excluded)})"
        )
        excluded_params = list(excluded)
    # Only authors with at least one visible post are listed.
    stats = (
        "SELECT posts.author AS author, "
        "(SELECT name FROM posts AS latest WHERE latest.author = posts.author "
        "AND latest.linked = 1 "
        "ORDER BY latest.created_at DESC, latest.seq DESC LIMIT 1) AS name, "
        f"SUM(CASE WHEN posts.kind = 'post' AND {_NOT_DELETED} THEN 1 ELSE 0 END) AS posts, "
        f"MAX(CASE WHEN posts.kind = 'post' AND {_NOT_DELETED} THEN posts.created_at ELSE 0 END) AS last_post, "
        "CASE WHEN follows.author IS NULL THEN 0 ELSE 1 END AS followed "
        "FROM posts LEFT JOIN follows ON follows.author = posts.author "
        f"WHERE posts.linked = 1{excluded_sql} GROUP BY posts.author "
        f"HAVING SUM(CASE WHEN posts.kind = 'post' AND {_NOT_DELETED} THEN 1 ELSE 0 END) > 0"
    )
    return stats, excluded_params


def author_stats_many(
    conn: sqlite3.Connection, authors: Sequence[str]
) -> Dict[str, Dict[str, Any]]:
    """:func:`author_stats` for many authors, using two grouped queries.

    Returns the same per-author values :func:`author_stats` would, keyed
    by author hash; authors with no cached posts get zero values.
    """
    clean = [_require_hex(author, AUTHOR_HEX_LENGTH, "author") for author in authors]
    stats: Dict[str, Dict[str, Any]] = {
        author: {"author": author, "posts": 0, "seq": 0, "last_post": 0.0}
        for author in clean
    }
    if not clean:
        return stats
    placeholders = ",".join("?" for _ in clean)
    for row in conn.execute(
        "SELECT author, COUNT(*) AS posts, COALESCE(MAX(created_at), 0) AS last_post "
        f"FROM posts WHERE linked = 1 AND kind = 'post' AND {_NOT_DELETED} "
        f"AND author IN ({placeholders}) GROUP BY author",
        clean,
    ).fetchall():
        stats[str(row["author"])].update(
            posts=int(row["posts"]), last_post=float(row["last_post"])
        )
    for row in conn.execute(
        "SELECT author, COALESCE(MAX(seq), 0) AS seq FROM posts "
        f"WHERE linked = 1 AND author IN ({placeholders}) GROUP BY author",
        clean,
    ).fetchall():
        stats[str(row["author"])]["seq"] = int(row["seq"])
    return stats


def list_authors(
    conn: sqlite3.Connection,
    *,
    sort: str = "recent",
    limit: int = 50,
    offset: int = 0,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
) -> List[Dict[str, Any]]:
    """Every author with at least one linked entry in the local store.

    Each record carries the display name from the author's most recent
    linked entry, the count of visible posts (linked, kind ``post`` and
    not deleted — the same visibility ``author_stats`` uses), the time
    of the most recent visible post (0 when the author has none, e.g.
    only a delete entry has arrived so far), and whether the author is
    followed. ``sort`` is ``recent`` (default), ``posts`` or ``name``.
    ``exclude_authors`` hides blocked authors entirely and ``exclude_all``
    hides every author (a block-list that failed closed), as the display
    surfaces do for posts. ``offset`` pages past the first ``limit``
    authors in the same sort order; pair it with :func:`count_authors`
    for the total.
    """
    if sort not in ("recent", "posts", "name"):
        raise TanError(f"unknown author sort: {sort!r}")
    if limit < 0:
        raise TanError("--limit must not be negative")
    if offset < 0:
        raise TanError("--offset must not be negative")
    if exclude_all:
        return []
    stats, excluded_params = _author_stats_query(exclude_authors)
    order_by = {
        "recent": "last_post DESC, author ASC",
        "posts": "posts DESC, last_post DESC, author ASC",
    }.get(sort)
    if order_by is not None:
        # Ordering and limiting happen in SQL so only the page is materialised.
        rows = conn.execute(
            f"SELECT author, name, posts, last_post, followed FROM ({stats}) "
            f"ORDER BY {order_by} LIMIT ? OFFSET ?",
            (*excluded_params, limit, offset),
        ).fetchall()
    else:
        rows = conn.execute(stats, excluded_params).fetchall()
    records = [
        {
            "author": str(row["author"]),
            "name": str(row["name"] or ""),
            "posts": int(row["posts"]),
            "last_post": float(row["last_post"]),
            "followed": bool(row["followed"]),
        }
        for row in rows
    ]
    if order_by is None:
        # Unicode casefold ordering for names is not expressible in SQLite.
        records.sort(
            key=lambda record: ((record["name"] or "").casefold(), record["author"])
        )
        return records[offset : offset + limit]
    return records


def count_authors(
    conn: sqlite3.Connection,
    *,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
) -> int:
    """How many authors :func:`list_authors` would list with the same
    block-list applied — the directory total behind a page.
    """
    if exclude_all:
        return 0
    stats, params = _author_stats_query(exclude_authors)
    row = conn.execute(f"SELECT COUNT(*) FROM ({stats})", params).fetchone()
    return int(row[0])


def _int_or_none(value: Any) -> Optional[int]:
    """Return *value* as an int, or None when it is not integer-shaped.

    SQLite columns are typeless, so a hand-edited or corrupted row can
    hold text (or a non-integral real) where the audit expects a number;
    that is a store problem to report, not a crash.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def verify_store(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Audit the local Tan store, re-verifying every entry and its chain.

    Re-checks each stored entry's hash and signature (the same check
    ``store_post`` runs on ingest), then checks the per-author chains:
    each linked entry must extend a linked entry of the same author at
    the previous sequence (forks, i.e. several children of one parent,
    are valid), and the ``heads`` table must point at an entry of that
    author at its latest sequence.

    Entries whose parent has not arrived yet (unlinked, ``seq == 0``) are
    legitimate partial-sync state and are only counted, not reported. An
    unlinked entry whose parent is *linked* is an inconsistency and is
    reported, as is an unlinked entry with no parent or a nonzero sequence.

    Returns ``{"entries", "authors", "unlinked", "problems", "ok"}``,
    where each problem is ``{"hash", "author", "problem"}``. Read-only:
    nothing in the database is changed.
    """
    # Read posts and heads inside one read transaction so a concurrent
    # writer cannot land between the reads and make a healthy store look
    # damaged.
    started = not conn.in_transaction
    if started:
        conn.execute("BEGIN")
    try:
        rows = conn.execute("SELECT * FROM posts").fetchall()
        head_rows = conn.execute("SELECT author, hash, seq FROM heads").fetchall()
    finally:
        if started and conn.in_transaction:
            conn.rollback()
    by_hash = {str(row["hash"]): row for row in rows}
    heads = {str(row["author"]): row for row in head_rows}
    problems: List[Dict[str, str]] = []

    def _report(row: Any, problem: str) -> None:
        problems.append(
            {
                "hash": str(row["hash"]),
                "author": str(row["author"]),
                "problem": problem,
            }
        )

    unlinked = 0
    linked_by_author: Dict[str, List[Tuple[Any, int]]] = {}
    for row in rows:
        seq = _int_or_none(row["seq"])
        linked = _int_or_none(row["linked"])
        if linked not in (0, 1):
            _report(row, "linked flag is not 0 or 1")
            continue
        if not linked:
            unlinked += 1
            parent = str(row["prev"])
            if seq != 0:
                _report(row, f"unlinked entry has sequence {row['seq']}, expected 0")
            if not parent:
                _report(row, "unlinked entry has no parent")
            parent_row = by_hash.get(parent)
            if parent_row is not None and _int_or_none(parent_row["linked"]) == 1:
                _report(row, "entry is unlinked but its parent is stored")
            continue
        if seq is None:
            _report(row, f"entry has a non-numeric sequence number {row['seq']!r}")
            continue
        linked_by_author.setdefault(str(row["author"]), []).append((row, seq))

    for row in rows:
        try:
            verify_post(_row_to_post(row))
        except (PostError, ProtocolError, ValueError, TypeError, KeyError) as exc:
            _report(row, str(exc))

    for author, entries in linked_by_author.items():
        entries.sort(key=lambda item: item[1])
        seq_by_hash = {str(row["hash"]): seq for row, seq in entries}
        for entry, seq in entries:
            parent = str(entry["prev"])
            if not parent:
                if seq != 1:
                    _report(
                        entry,
                        f"first linked entry has sequence {seq}, expected 1",
                    )
                continue
            parent_seq = seq_by_hash.get(parent)
            if parent_seq is None:
                _report(
                    entry,
                    "entry does not point at the previous entry in its chain",
                )
                continue
            if seq != parent_seq + 1:
                _report(
                    entry,
                    f"sequence jumps from {parent_seq} to {seq}",
                )
        last, last_seq = entries[-1]
        head_row = heads.get(author)
        if head_row is None:
            _report(last, "author has linked entries but no head record")
            continue
        head_entry = by_hash.get(str(head_row["hash"]))
        if head_entry is None:
            _report(last, "head points at an entry that is not stored")
            continue
        if str(head_entry["author"]) != author:
            _report(
                head_entry,
                "head points at an entry that belongs to a different author",
            )
            continue
        if _int_or_none(head_entry["linked"]) != 1:
            _report(
                head_entry,
                "head points at an entry that is not linked",
            )
            continue
        head_seq = _int_or_none(head_row["seq"])
        entry_seq = _int_or_none(head_entry["seq"])
        if entry_seq is None:
            # Already reported when the row was classified.
            continue
        if head_seq is None:
            _report(head_entry, "head sequence is not an integer")
            continue
        if head_seq != entry_seq:
            _report(
                head_entry,
                "head sequence does not match the entry it points at",
            )
        if entry_seq != last_seq:
            _report(
                head_entry, "head does not point at the author's latest entry"
            )

    for head_row in head_rows:
        if str(head_row["author"]) not in linked_by_author:
            problems.append(
                {
                    "hash": str(head_row["hash"]),
                    "author": str(head_row["author"]),
                    "problem": (
                        "head record exists but the author "
                        "has no linked entries"
                    ),
                }
            )

    return {
        "entries": len(rows),
        "authors": len({str(row["author"]) for row in rows}),
        "unlinked": unlinked,
        "problems": problems,
        "ok": not problems,
    }


def resolve_author(
    conn: sqlite3.Connection,
    target: str,
    *,
    registry: Any = None,
) -> Tuple[str, str]:
    """Resolve a peer name, destination hash or prefix to an author + name.

    Aliases are excluded on purpose: an author identity must never be
    hijacked by a local label, so a peer aliased ``alice`` can never
    stand in for the peer actually named ``alice`` when following or
    unfollowing.
    """
    if not isinstance(target, str) or not target.strip():
        raise TanError("author must not be empty")
    value = target.strip()

    if registry is not None and hasattr(registry, "resolve"):
        try:
            peer = registry.resolve(value, include_alias=False)
        except Exception as exc:
            raise TanError(str(exc)) from exc
        if peer is not None:
            return peer.hexhash, peer.name

    if len(value) == AUTHOR_HEX_LENGTH:
        try:
            author = _require_hex(value, AUTHOR_HEX_LENGTH, "author")
        except PostError:
            pass
        else:
            row = conn.execute(
                "SELECT name FROM posts WHERE author = ? AND name != '' "
                "ORDER BY created_at DESC LIMIT 1",
                (author,),
            ).fetchone()
            return author, (str(row["name"]) if row is not None else "")

    if len(value) >= 4:
        prefix = value.casefold()
        rows = conn.execute(
            "SELECT DISTINCT author FROM posts WHERE author LIKE ? LIMIT 8",
            (prefix + "%",),
        ).fetchall()
        if len(rows) == 1:
            author = str(rows[0]["author"])
            row = conn.execute(
                "SELECT name FROM posts WHERE author = ? AND name != '' "
                "ORDER BY created_at DESC LIMIT 1",
                (author,),
            ).fetchone()
            return author, (str(row["name"]) if row is not None else "")
        if len(rows) > 1:
            raise TanError(f"{len(rows)} cached authors match {value!r}")

    raise TanError(f"unknown author or peer: {target!r}")


def resolve_cached_author(conn: sqlite3.Connection, name: str) -> Tuple[str, str]:
    """Resolve an author by the name recorded on their stored posts.

    The peer registry does not know authors whose posts are only cached,
    so commands that select a stored author by name fall back to this.
    The match is case-insensitive and must pick out exactly one author.
    """
    if not isinstance(name, str) or not name.strip():
        raise TanError("author must not be empty")
    clean = name.strip()
    rows = conn.execute(
        "SELECT DISTINCT author FROM posts WHERE name = ? COLLATE NOCASE",
        (clean,),
    ).fetchall()
    if len(rows) == 1:
        author = str(rows[0]["author"])
        latest = conn.execute(
            "SELECT name FROM posts WHERE author = ? AND name != '' "
            "ORDER BY created_at DESC LIMIT 1",
            (author,),
        ).fetchone()
        return author, str(latest["name"]) if latest is not None else ""
    if len(rows) > 1:
        raise TanError(
            f"{len(rows)} cached authors are named {clean!r}; "
            "use a destination hash"
        )
    raise TanError(f"no cached posts from an author named {clean!r}")


def resolve_muted_name(conn: sqlite3.Connection, name: str) -> Optional[Tuple[str, str]]:
    """Resolve a muted author by the name recorded when they were muted.

    Returns ``None`` when no muted author has that name.  The match is
    case-insensitive; more than one match is an error asking for a hash.
    """
    clean = name.strip() if isinstance(name, str) else ""
    if not clean:
        return None
    rows = conn.execute(
        "SELECT author, name FROM muted_authors WHERE name = ? COLLATE NOCASE",
        (clean,),
    ).fetchall()
    if len(rows) == 1:
        return str(rows[0]["author"]), str(rows[0]["name"])
    if len(rows) > 1:
        raise TanError(
            f"{len(rows)} muted authors are named {clean!r}; "
            "use a destination hash"
        )
    return None


# -- network application ---------------------------------------------------


class TanApp:
    """Serve and synchronise Tan posts for a running :class:`TauNode`."""

    def __init__(
        self,
        config: Any,
        identity: RNS.Identity,
        *,
        name: Optional[str] = None,
        db_path: Optional[Union[str, Path]] = None,
        on_post: Optional[Callable[[Dict[str, Any]], None]] = None,
        gossip: bool = True,
        gossip_fanout: int = GOSSIP_FANOUT,
        request_timeout: float = 15.0,
        ruleset: Optional[Ruleset] = None,
    ) -> None:
        self.config = config
        self.identity = identity
        self.name = sanitize_name(name or getattr(config, "name", "anonymous"))
        self.db_path = Path(db_path).expanduser() if db_path is not None else Path(
            getattr(config, "social_db_path")
        )
        self.author = _author_from_identity(identity)
        self.on_post = on_post
        self.gossip = bool(gossip)
        self.gossip_fanout = max(1, int(gossip_fanout))
        self.request_timeout = float(request_timeout)
        self.node: Any = None
        self._gossip_slots = threading.BoundedSemaphore(4)
        self._closed = False
        self._ruleset = ruleset
        self._ruleset_lock = threading.Lock()
        self._ruleset_path = getattr(config, "tan_rules_path", None)

    @property
    def ruleset(self) -> Ruleset:
        """The admission ruleset, loaded lazily from the data directory.

        A ruleset that cannot be parsed fails closed — remote entries
        are denied — rather than silently reverting to admit-all, but
        loading is lazy so a broken file never blocks local operations.
        Loading holds ``_ruleset_lock`` so it cannot interleave with
        ``reload_ruleset`` and install a pre-save value after it.
        """
        with self._ruleset_lock:
            if self._ruleset is None:
                try:
                    self._ruleset = load_ruleset(self._ruleset_path)
                except RulesError as exc:
                    log.error(
                        "Tan ruleset unusable (%s); denying remote entries", exc
                    )
                    self._ruleset = Ruleset(default_allow=False)
            return self._ruleset

    def reload_ruleset(self) -> None:
        """Drop the cached admission ruleset so the next access re-reads it.

        Lets a ruleset change made in-process — for example from the web
        console — apply to the running node at once instead of waiting
        for a restart. A rules file that cannot be parsed still fails
        closed on the next load, through the ``ruleset`` property.
        """
        with self._ruleset_lock:
            self._ruleset = None

    def _admitted(self, conn: sqlite3.Connection, author: str) -> bool:
        ruleset = self.ruleset
        followed: Iterable[str] = ()
        if ruleset.allow_followed:
            followed = [record["author"] for record in following(conn)]
        return ruleset.decide(
            author, own_author=self.author, followed=followed
        )

    # -- lifecycle ---------------------------------------------------------

    def attach(self, node: Any) -> None:
        """Register Tan request handlers on a started node."""
        if getattr(node, "destination", None) is None:
            raise TanError("cannot attach Tan to a node that is not started")
        self.node = node
        node.add_request_handler(TAN_FEED_PATH, self._handle_feed)
        node.add_request_handler(TAN_POST_PATH, self._handle_post)
        log.info("Tan feed ready at %s", TAN_FEED_PATH)

    def detach(self) -> None:
        self.node = None
        self._closed = True

    # -- local operations --------------------------------------------------

    def connect(self) -> sqlite3.Connection:
        return init_db(self.db_path)

    def create_post(
        self,
        text: str,
        *,
        reply_to: Optional[str] = None,
        publish: bool = False,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
    ) -> Dict[str, Any]:
        """Create a post, or a reply when *reply_to* names a parent post."""
        conn = self.connect()
        try:
            if reply_to is not None:
                post = create_reply(
                    conn, self.identity, reply_to, text, name=self.name
                )
            else:
                post = create_post(conn, self.identity, text, name=self.name)
        finally:
            conn.close()
        if publish and self.node is not None:
            self.publish(post, targets=targets, ttl=ttl)
        return post

    def get_post(self, digest: str) -> Optional[Dict[str, Any]]:
        """Return a stored post by hash, or ``None``."""
        conn = self.connect()
        try:
            return get_post(conn, digest)
        finally:
            conn.close()

    def quote(
        self,
        target: str,
        text: str,
        *,
        publish: bool = False,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
    ) -> Dict[str, Any]:
        """Create a quote of *target* carrying *text* as the comment."""
        conn = self.connect()
        try:
            record = create_quote(
                conn, self.identity, target, text, name=self.name
            )
        finally:
            conn.close()
        if publish and self.node is not None:
            self.publish(record, targets=targets, ttl=ttl)
        return record

    def timeline(
        self,
        *,
        include_all: bool = False,
        authors: Optional[Sequence[str]] = None,
        limit: int = 30,
        exclude_authors: Optional[Sequence[str]] = None,
        exclude_all: bool = False,
    ) -> List[Dict[str, Any]]:
        conn = self.connect()
        try:
            if authors is None and not include_all:
                authors = [record["author"] for record in following(conn)]
                authors.append(self.author)
            posts = timeline(
                conn,
                authors,
                limit=limit,
                include_all=include_all,
                viewer=self.author,
                exclude_authors=exclude_authors,
                exclude_all=exclude_all,
            )
            return apply_display_rules(posts, self.ruleset)
        finally:
            conn.close()

    def react(
        self,
        target: str,
        *,
        liked: bool,
        publish: bool = False,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
    ) -> Dict[str, Any]:
        """Append a like (``liked=True``) or unlike entry for *target*."""
        conn = self.connect()
        try:
            record = create_reaction(
                conn, self.identity, target, liked=liked, name=self.name
            )
        finally:
            conn.close()
        if publish and self.node is not None:
            self.publish(record, targets=targets, ttl=ttl)
        return record

    def like(self, target: str, **kwargs: Any) -> Dict[str, Any]:
        return self.react(target, liked=True, **kwargs)

    def unlike(self, target: str, **kwargs: Any) -> Dict[str, Any]:
        return self.react(target, liked=False, **kwargs)

    def repost(
        self,
        target: str,
        *,
        publish: bool = False,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
    ) -> Dict[str, Any]:
        """Append a repost entry for *target* (a post or reply)."""
        conn = self.connect()
        try:
            record = create_repost(
                conn, self.identity, target, name=self.name
            )
        finally:
            conn.close()
        if publish and self.node is not None:
            self.publish(record, targets=targets, ttl=ttl)
        return record

    def delete(
        self,
        target: str,
        *,
        publish: bool = False,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
    ) -> Dict[str, Any]:
        """Append a delete entry for *target* (one of your own posts)."""
        conn = self.connect()
        try:
            record = create_delete(
                conn, self.identity, target, name=self.name
            )
        finally:
            conn.close()
        if publish and self.node is not None:
            self.publish(record, targets=targets, ttl=ttl)
        return record

    def edit(
        self,
        target: str,
        text: str,
        *,
        publish: bool = False,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
    ) -> Dict[str, Any]:
        """Append an edit entry for *target* (one of your own posts)."""
        conn = self.connect()
        try:
            record = create_edit(
                conn, self.identity, target, text, name=self.name
            )
        finally:
            conn.close()
        if publish and self.node is not None:
            self.publish(record, targets=targets, ttl=ttl)
        return record

    def following_ids(self) -> List[str]:
        conn = self.connect()
        try:
            return [record["author"] for record in following(conn)]
        finally:
            conn.close()

    def sync(
        self,
        node: Any,
        *,
        targets: Optional[Sequence[Any]] = None,
        authors: Optional[Sequence[str]] = None,
        limit: int = DEFAULT_FEED_LIMIT,
        explore: bool = False,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Pull missing posts from peers and return a summary."""
        if node is not self.node:
            self.node = node
        peers = list(targets) if targets is not None else list(node.registry.all())
        peers = [peer for peer in peers if getattr(peer, "hexhash", "") != self.author]
        if not peers:
            return {
                "peers": 0,
                "authors": 0,
                "stored": 0,
                "duplicates": 0,
                "skipped": 0,
                "errors": [],
            }

        page_limit = max(1, min(int(limit), MAX_FEED_POSTS))
        request_timeout = float(timeout if timeout is not None else self.request_timeout)
        conn = self.connect()
        stored = 0
        duplicates = 0
        skipped = 0
        errors: List[Dict[str, str]] = []
        try:
            if authors is None:
                authors = [record["author"] for record in following(conn)]
            authors = list(authors)
            if self.author not in authors:
                authors.append(self.author)

            for author in authors:
                _require_hex(author, AUTHOR_HEX_LENGTH, "author")
                local_seq = max_seq(conn, author)
                author_peers = [
                    peer for peer in peers if getattr(peer, "hexhash", "") == author
                ]
                # Prefer the author's node, but use relays when it is offline or
                # does not have the requested chain cached.
                candidates = author_peers + [
                    peer for peer in peers if peer not in author_peers
                ]
                for peer in candidates:
                    for _ in range(MAX_SYNC_PAGES):
                        payload = encode(
                            {
                                "author": author,
                                "after_seq": local_seq,
                                "limit": page_limit,
                            },
                            max_bytes=MAX_REQUEST_BYTES,
                        )
                        try:
                            response = node.request(
                                peer, TAN_FEED_PATH, payload, timeout=request_timeout
                            )
                        except Exception as exc:
                            errors.append(
                                {
                                    "peer": getattr(peer, "hexhash", "?"),
                                    "author": author,
                                    "error": str(exc),
                                }
                            )
                            break
                        posts = response.get("posts")
                        if not isinstance(posts, list) or not posts:
                            break
                        for raw in posts:
                            if not isinstance(raw, dict):
                                continue
                            if not self._admitted(
                                conn, str(raw.get("author", ""))
                            ):
                                skipped += 1
                                continue
                            try:
                                _, is_new = store_post(conn, raw)
                            except PostError as exc:
                                errors.append(
                                    {
                                        "peer": getattr(peer, "hexhash", "?"),
                                        "author": author,
                                        "error": str(exc),
                                    }
                                )
                                continue
                            if is_new:
                                stored += 1
                            else:
                                duplicates += 1
                        new_seq = max_seq(conn, author)
                        if new_seq <= local_seq:
                            break
                        # A page may be short because it was limited by the
                        # response byte budget, not because the chain ended.
                        local_seq = new_seq

            if explore:
                for peer in peers:
                    payload = encode(
                        {"all": True, "limit": page_limit},
                        max_bytes=MAX_REQUEST_BYTES,
                    )
                    try:
                        response = node.request(
                            peer, TAN_FEED_PATH, payload, timeout=request_timeout
                        )
                    except Exception as exc:
                        errors.append(
                            {
                                "peer": getattr(peer, "hexhash", "?"),
                                "author": "",
                                "error": str(exc),
                            }
                        )
                        continue
                    posts = response.get("posts")
                    if not isinstance(posts, list):
                        continue
                    for raw in posts:
                        if not isinstance(raw, dict):
                            continue
                        if not self._admitted(
                            conn, str(raw.get("author", ""))
                        ):
                            skipped += 1
                            continue
                        try:
                            _, is_new = store_post(conn, raw)
                        except PostError as exc:
                            errors.append(
                                {
                                    "peer": getattr(peer, "hexhash", "?"),
                                    "author": str(raw.get("author", "")),
                                    "error": str(exc),
                                }
                            )
                            continue
                        if is_new:
                            stored += 1
                        else:
                            duplicates += 1
        finally:
            conn.close()

        return {
            "peers": len(peers),
            "authors": len(authors),
            "stored": stored,
            "duplicates": duplicates,
            "skipped": skipped,
            "errors": errors,
        }

    def publish(
        self,
        post: Dict[str, Any],
        *,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
        exclude: Optional[str] = None,
        fanout: Optional[int] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        deadline: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Push a post to peers; return delivery counts.

        When ``should_stop`` or ``deadline`` (a ``time.monotonic``
        value) is given, sending stops early — between peers — and
        the peers never attempted come back in ``"unsent"`` so the
        caller can keep them queued.  Callers that pass neither send
        to every peer, as before.
        """
        node = self.node
        if node is None:
            raise TanError("Tan is not attached to a running node")
        peers = list(targets) if targets is not None else list(node.registry.all())
        peers = [
            peer
            for peer in peers
            if getattr(peer, "hexhash", "") not in (self.author, exclude)
        ]
        if fanout is not None and len(peers) > max(1, int(fanout)):
            peers = random.sample(peers, max(1, int(fanout)))
        payload = encode(
            {"post": post, "ttl": max(0, min(int(ttl), MAX_GOSSIP_TTL))},
            max_bytes=MAX_REQUEST_BYTES,
        )
        delivered = 0
        errors: List[Dict[str, str]] = []
        unsent: List[str] = []
        for index, peer in enumerate(peers):
            if should_stop is not None and should_stop():
                unsent = [getattr(p, "hexhash", "?") for p in peers[index:]]
                break
            if deadline is not None and time.monotonic() > deadline:
                unsent = [getattr(p, "hexhash", "?") for p in peers[index:]]
                break
            try:
                node.request(
                    peer, TAN_POST_PATH, payload, timeout=self.request_timeout
                )
                delivered += 1
            except Exception as exc:
                errors.append(
                    {"peer": getattr(peer, "hexhash", "?"), "error": str(exc)}
                )
        return {
            "delivered": delivered,
            "peers": len(peers),
            "errors": errors,
            "unsent": unsent,
        }

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
            author = request.get("author")
            all_authors = bool(request.get("all", False))
            if author is not None and not all_authors:
                author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
            after_seq = request.get("after_seq", 0)
            if isinstance(after_seq, bool) or not isinstance(after_seq, int):
                after_seq = 0
            limit = request.get("limit", DEFAULT_FEED_LIMIT)
            if isinstance(limit, bool) or not isinstance(limit, int):
                limit = DEFAULT_FEED_LIMIT
            before = request.get("before")
            if before is not None:
                before = _require_timestamp(before, "before")

            conn = self.connect()
            try:
                posts = feed(
                    conn,
                    author=None if all_authors else author,
                    after_seq=max(0, after_seq),
                    limit=limit,
                    all_authors=all_authors,
                    before=before,
                )
            finally:
                conn.close()
        except (ProtocolError, TanError) as exc:
            return response_error("invalid_feed_request", str(exc))
        while True:
            try:
                return response_ok(
                    posts=posts,
                    count=len(posts),
                    author=None if all_authors else author,
                )
            except ProtocolError as exc:
                if len(posts) <= 1:
                    return response_error("feed_too_large", str(exc))
                posts = posts[:-1]

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
            raw_post = request.get("post")
            if not isinstance(raw_post, dict):
                raise PostError("post must be a JSON object")
            ttl = request.get("ttl", 0)
            if isinstance(ttl, bool) or not isinstance(ttl, int):
                ttl = 0
            ttl = max(0, min(ttl, MAX_GOSSIP_TTL))

            verified = verify_post(raw_post)
            conn = self.connect()
            try:
                if not self._admitted(conn, verified["author"]):
                    log.info(
                        "Tan entry from %s denied by the admission ruleset",
                        verified["author"][:8],
                    )
                    return response_error(
                        "not_admitted",
                        "entry denied by this node's Tan ruleset",
                    )
                record, is_new = store_post(conn, raw_post)
            finally:
                conn.close()

            if is_new:
                log.info(
                    "Tan post from %s (%s): %s",
                    record.get("name", "unknown"),
                    record.get("author", "")[:8],
                    record.get("text", ""),
                )
                if self.on_post is not None:
                    try:
                        self.on_post(record)
                    except Exception:  # pragma: no cover - user callback
                        log.warning("Tan on_post callback failed", exc_info=True)
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
        except (ProtocolError, TanError) as exc:
            return response_error("invalid_post", str(exc))

    def _spawn_gossip(
        self, post: Dict[str, Any], *, ttl: int, exclude: Optional[str]
    ) -> None:
        if not self._gossip_slots.acquire(blocking=False):
            log.debug("Skipping Tan gossip: too many pending relays")
            return

        def run() -> None:
            try:
                if self.node is not None and not self._closed:
                    self.publish(
                        post,
                        ttl=ttl,
                        exclude=exclude,
                        fanout=self.gossip_fanout,
                    )
            except Exception:  # pragma: no cover - network dependent
                log.debug("Tan gossip failed", exc_info=True)
            finally:
                self._gossip_slots.release()

        threading.Thread(target=run, name="tau-tan-gossip", daemon=True).start()
