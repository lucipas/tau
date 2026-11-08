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
REACTION_KINDS = (KIND_LIKE, KIND_UNLIKE)
# Reactions and deletes carry no text; replies carry text but are still
# targeted entries, so their parent link is signed like a reaction's.
TEXTLESS_KINDS = REACTION_KINDS + (KIND_DELETE,)
# Targeted entries (reactions, deletes and replies) point at another post.
TARGETED_KINDS = TEXTLESS_KINDS + (KIND_REPLY,)
ENTRY_KINDS = (KIND_POST,) + TARGETED_KINDS
# Kinds that appear as timeline items: top-level posts and replies.
TIMELINE_KINDS = (KIND_POST, KIND_REPLY)

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
        # Reactions and deletes carry no text; normalize_message would also
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
    if existing.get("kind", KIND_POST) not in TIMELINE_KINDS:
        raise TanError("only posts and replies can be replied to")
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
            raise TanError("only posts and replies can be deleted")
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


def annotate_replies(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
    *,
    muted_keywords: Optional[Sequence[str]] = None,
    exclude_authors: Optional[Set[str]] = None,
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
    its name and text never surface.  Top-level
    posts get no ``reply_to`` key.

    Parents are fetched in one query and their deletion status in a
    second, so a reply-heavy page does not issue one lookup per reply.
    Only linked posts and replies count as parents: a reply that points
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
            "AND kind IN ('post', 'reply') AND linked = 1",
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
        if post.get("kind") != KIND_REPLY:
            continue
        parent = parents.get(str(post.get("target") or ""))
        if parent is None or (
            exclude_authors and parent["author"] in exclude_authors
        ):
            post["reply_to"] = None
            continue
        is_deleted = (parent["hash"], parent["author"]) in deleted
        is_muted = _text_matches_muted_keywords(
            str(parent.get("text") or ""), muted_pattern
        )
        preview = {
            "hash": parent["hash"],
            "author": parent["author"],
            "name": parent["name"],
            "text": "" if is_deleted or is_muted else parent["text"],
            "deleted": is_deleted,
        }
        if is_muted:
            # Only present when muted, so unmuted previews keep their
            # established shape.
            preview["muted"] = True
        post["reply_to"] = preview
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
        "WHERE p.kind IN ('post', 'reply') AND p.linked = 1 "
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
            "SELECT hash FROM posts WHERE kind IN ('post', 'reply') "
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
) -> List[Dict[str, Any]]:
    """Read cached entries in chain order or reverse chronological order.

    Sync needs every entry (reactions, deletes and replies included) to
    keep chains complete; display callers pass ``posts_only=True`` to
    hide reaction entries and posts their author deleted.  Replies are
    timeline items, so ``posts_only`` keeps them alongside top-level
    posts.
    """
    count = max(1, min(int(limit), MAX_FEED_POSTS))
    if author is not None:
        author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
        kind_filter = ""
        if posts_only:
            kind_filter = f" AND kind IN ('post', 'reply') AND {_NOT_DELETED}"
        rows = conn.execute(
            f"SELECT {', '.join(_POST_COLUMNS)} FROM posts "
            f"WHERE author = ? AND linked = 1 AND seq > ?{kind_filter} "
            "ORDER BY seq ASC LIMIT ?",
            (author, max(0, int(after_seq)), count),
        ).fetchall()
        return _public_posts(rows)

    params: List[Any] = []
    where = "linked = 1"
    if posts_only:
        where += f" AND kind IN ('post', 'reply') AND {_NOT_DELETED}"
    if exclude_authors:
        # Display-only exclusion: the author-specific path above is an
        # explicit lookup and never excludes.  Sync callers leave this
        # unset, so chains stay complete for every author.
        clean_excluded = [
            _require_hex(item, AUTHOR_HEX_LENGTH, "author")
            for item in exclude_authors
        ]
        placeholders = ", ".join("?" for _ in clean_excluded)
        where += f" AND author NOT IN ({placeholders})"
        params.extend(clean_excluded)
    if before is not None:
        where += " AND created_at < ?"
        params.append(float(before))
    params.append(count)
    rows = conn.execute(
        f"SELECT {', '.join(_POST_COLUMNS)} FROM posts WHERE {where} "
        "ORDER BY created_at DESC, hash DESC LIMIT ?",
        params,
    ).fetchall()
    return _public_posts(rows)


TAN_EXPORT_FORMAT = "tau-tan-export"
TAN_EXPORT_VERSION = 1


def export_entries(
    conn: sqlite3.Connection,
    *,
    author: Optional[str] = None,
    all_authors: bool = False,
) -> Dict[str, Any]:
    """Return a portable backup of stored chain entries.

    Entries are the canonical signed envelopes (the same dicts
    :func:`store_post` accepts), ordered per author so a chain replays
    in order: linked entries by sequence — the stored head first among
    same-sequence forks, so the restored head matches the source — then
    any entries whose parent is missing from this store (sequence 0)
    by creation time.  An entry whose parent is missing stays unlinked
    on import until that parent arrives, exactly as sync behaves.
    """
    columns = ", ".join(f"posts.{column}" for column in _POST_COLUMNS)
    order = (
        "posts.linked DESC, posts.seq ASC, heads.hash IS NULL ASC, "
        "posts.created_at ASC, posts.hash ASC"
    )
    if all_authors:
        rows = conn.execute(
            f"SELECT {columns} FROM posts LEFT JOIN heads "
            "ON heads.author = posts.author AND heads.hash = posts.hash "
            f"ORDER BY posts.author ASC, {order}"
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


def timeline(
    conn: sqlite3.Connection,
    authors: Optional[Sequence[str]] = None,
    *,
    limit: int = 30,
    include_all: bool = False,
    before: Optional[float] = None,
    viewer: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return followed posts (and local posts) newest first.

    Reaction entries never appear as timeline items; instead each post
    is annotated with its derived ``likes`` count and, when *viewer* is
    given, whether the viewer's like currently stands and whether the
    post is the viewer's own.  Replies appear as timeline items,
    annotated with their parent post when it is cached.  Posts their
    author deleted never appear either.  Posts whose text matches a
    muted keyword (see :func:`muted_keywords`) never appear either,
    and a reply to one keeps its ``reply_to`` preview text blanked —
    muting is display-only, so entries stay stored and unmuting
    restores them.  Posts by muted authors never appear either, for the
    same reason.  Pages are read until ``limit`` unmuted posts are
    collected or the feed is exhausted, so muted posts never shrink the
    result below what is available.
    """
    silenced = muted_author_set(conn)
    keywords = _muted_keyword_list(conn)
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
                exclude_authors=silenced,
            ),
            before,
            lambda post: float(post["created_at"]),
        )
        return annotate_replies(
            conn,
            annotate_reactions(conn, posts, viewer=viewer),
            muted_keywords=keywords,
            exclude_authors=silenced,
        )
    clean_authors = [
        _require_hex(author, AUTHOR_HEX_LENGTH, "author") for author in authors or []
    ]
    clean_authors = [author for author in clean_authors if author not in silenced]
    if not clean_authors:
        return []
    placeholders = ", ".join("?" for _ in clean_authors)

    def page(cursor: Optional[float]) -> List[Dict[str, Any]]:
        params: List[Any] = list(clean_authors)
        where = (
            f"linked = 1 AND kind IN ('post', 'reply') AND author IN ({placeholders}) "
            f"AND {_NOT_DELETED}"
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
    )
    return annotate_replies(
        conn,
        annotate_reactions(conn, posts, viewer=viewer),
        muted_keywords=keywords,
        exclude_authors=silenced,
    )


def author_timeline(
    conn: sqlite3.Connection,
    author: str,
    *,
    limit: int = 30,
    viewer: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return one author's posts newest first, with muted posts skipped.

    Pages through the author's chain until *limit* unmuted posts are
    collected, so muted posts never shrink the result.  Reply previews
    are redacted for muted parents, as in :func:`timeline`.  The page
    reads rows with their sequence numbers attached — :func:`feed`
    strips ``seq`` from its results, so paging on it would fail as
    soon as a second page was needed.
    """
    author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
    keywords = _muted_keyword_list(conn)

    def page(cursor: int) -> List[Dict[str, Any]]:
        rows = conn.execute(
            f"SELECT {', '.join(_POST_COLUMNS)} FROM posts "
            "WHERE author = ? AND linked = 1 AND seq > ? "
            f"AND kind IN ('post', 'reply') AND {_NOT_DELETED} "
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
    )
    return annotate_replies(
        conn,
        annotate_reactions(conn, list(reversed(posts)), viewer=viewer),
        muted_keywords=keywords,
    )


MAX_TOP_POSTS = 100


def top_posts(
    conn: sqlite3.Connection,
    *,
    author: Optional[str] = None,
    limit: int = 10,
    viewer: Optional[str] = None,
    ruleset: Optional[Ruleset] = None,
) -> List[Dict[str, Any]]:
    """Return the most-liked posts in the local store, best first.

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
    ``timeline --author``: their posts rank even when they are muted.
    Results carry the timeline's ``likes`` / ``liked`` / ``own``
    annotations and ``reply_to`` parent summaries.

    The ranking needs every candidate's like count before any
    ordering is possible, so the scan reads the whole visible store
    in one query — the same scale :func:`search_posts` already scans,
    and a personal mesh feed keeps the local post table small.
    """
    count = max(1, min(int(limit), MAX_TOP_POSTS))
    keywords = _muted_keyword_list(conn)
    silenced = muted_author_set(conn) if author is None else set()
    params: List[Any] = []
    where = f"linked = 1 AND kind IN ('post', 'reply') AND {_NOT_DELETED}"
    if silenced:
        placeholders = ", ".join("?" for _ in silenced)
        where += f" AND author NOT IN ({placeholders})"
        params.extend(sorted(silenced))
    if author is not None:
        author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
        where += " AND author = ?"
        params.append(author)
    rows = conn.execute(
        f"SELECT {', '.join(_POST_COLUMNS)} FROM posts WHERE {where} "
        "ORDER BY created_at DESC, hash DESC",
        params,
    ).fetchall()
    candidates = without_muted_keywords(conn, _public_posts(rows), keywords)
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
    return annotate_replies(
        conn,
        candidates[:count],
        muted_keywords=keywords,
        exclude_authors=silenced,
    )


def _fill_unmuted(
    conn: sqlite3.Connection,
    keywords: List[str],
    limit: int,
    fetch: Callable[[Any], List[Dict[str, Any]]],
    cursor: Any,
    next_cursor: Callable[[Dict[str, Any]], Any],
) -> List[Dict[str, Any]]:
    """Collect up to *limit* unmuted posts by paging through *fetch*.

    *fetch* receives a cursor and returns one page of at most
    ``MAX_FEED_POSTS`` posts in feed order; *next_cursor* derives the
    cursor for the page after the last post it returned.  A short page
    means the feed is exhausted.
    """
    wanted = max(1, min(int(limit), 200))
    kept: List[Dict[str, Any]] = []
    while len(kept) < wanted:
        page = fetch(cursor)
        kept.extend(without_muted_keywords(conn, page, keywords))
        if len(page) < MAX_FEED_POSTS:
            break
        cursor = next_cursor(page[-1])
    return kept[:wanted]


# Upper bound on bound parameters per thread query.  SQLite builds often cap
# host parameters at 999, so wide generations and long threads are processed
# in batches of this size.
_SQL_BATCH = 500


def _thread_batches(items: List[Any], size: int) -> Iterator[List[Any]]:
    for offset in range(0, len(items), size):
        yield items[offset : offset + size]


def thread(
    conn: sqlite3.Connection,
    ref: str,
    *,
    viewer: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return the thread rooted at the post *ref* names, oldest first.

    *ref* may name any entry in the thread — a reply resolves to its
    root post, walking up through cached, linked, non-deleted parents.
    The root is the highest such ancestor; a reply whose parent has not
    arrived yet still anchors its own visible thread.  Every entry
    carries a ``depth`` (the root is 0) and the derived ``likes`` /
    ``liked`` / ``own`` annotations, like the timeline.  Replies appear
    in depth-first order — a reply, then its own replies, then the next
    sibling — with siblings oldest first, so the thread reads as a
    conversation.  Deleted and unlinked entries never appear, and a
    reply to a deleted entry is hidden with its parent's subtree rather
    than re-parented.  Replies by muted authors are hidden the same
    way, subtree included — except the entry the caller named, which
    always anchors the thread even when its author is muted, and a
    muted ancestor above it, which simply ends the climb instead of
    being promoted to the root.  An unknown, ambiguous or deleted reference
    raises :class:`TanError`, exactly as :func:`resolve_post` does; so
    does a reference to an unlinked entry or to a reply beneath a
    deleted entry, since showing it would present a partial thread.
    A reply matching a muted keyword (see :func:`muted_keywords`) is
    hidden with its whole subtree the same way, but the entry the
    reference names always anchors the thread even when its own text
    matches — an explicit lookup — and a muted ancestor above it ends
    the climb instead of being promoted to the root.
    """
    start = resolve_post(conn, ref)
    if not start.get("linked"):
        # A cached entry still awaiting its chain predecessor is not shown.
        raise TanError(f"post has not linked yet: {ref!r}")
    silenced = muted_author_set(conn)
    muted_pattern = _muted_keyword_pattern(_muted_keyword_list(conn))
    root = start
    ancestors = {root["hash"]}
    # Once a muted entry is met (the named entry itself, or an ancestor),
    # the visible root stops moving, but the climb continues so that
    # deleted ancestors above it are still rejected.
    anchored = start["author"] in silenced or _text_matches_muted_keywords(
        str(start.get("text") or ""), muted_pattern
    )
    cursor = start
    while cursor.get("kind") == KIND_REPLY and cursor.get("target"):
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
        annotate_reactions(conn, batch, viewer=viewer)
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
) -> List[Dict[str, Any]]:
    """Search stored posts by keyword, newest first.

    Only linked ``post`` entries match: reaction entries never appear
    as results, and posts their author deleted are excluded the same
    way the timeline excludes them.  The match is a case-insensitive
    substring search over the post text — ``casefold`` on both sides,
    so it folds Unicode case (``Ä`` matches ``ä``) — and the search
    reads only the local store: it makes no network requests.  Results
    carry the same like annotations as the timeline.

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
    count = max(1, min(int(limit), MAX_SEARCH_POSTS))
    needle = query.strip().casefold()
    params: List[Any] = []
    where = f"linked = 1 AND kind = 'post' AND {_NOT_DELETED}"
    if author is not None:
        author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
        where += " AND author = ?"
        params.append(author)
    rows = conn.execute(
        f"SELECT {', '.join(_POST_COLUMNS)} FROM posts WHERE {where} "
        "ORDER BY created_at DESC, hash DESC",
        params,
    ).fetchall()
    matches = [row for row in rows if needle in str(row["text"]).casefold()]
    if author is None:
        silenced = muted_author_set(conn)
        matches = [row for row in matches if str(row["author"]) not in silenced]
    return annotate_reactions(conn, _public_posts(matches[:count]), viewer=viewer)


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
) -> List[Dict[str, Any]]:
    """Return stored replies to the viewer's own posts, newest first.

    A mention is a linked ``reply`` entry whose target is a linked
    ``post`` or ``reply`` authored by *viewer* — a reply to something
    you wrote, including a reply to one of your own replies.  A reply
    you wrote to your own post is not a mention of you.  The same
    hiding rules as the thread view apply: a reply its author deleted
    never appears, and neither does a reply to a post you deleted.
    Unlinked replies never appear either.  Results carry the same like
    annotations and ``reply_to`` parent summary as the timeline, and
    the query reads only the local store: it makes no network requests.
    """
    viewer = _require_hex(viewer, AUTHOR_HEX_LENGTH, "viewer")
    count = max(1, min(int(limit), MAX_MENTIONS_POSTS))
    rows = conn.execute(
        f"SELECT {', '.join(_POST_COLUMNS)} FROM posts AS r "
        "WHERE r.linked = 1 AND r.kind = 'reply' AND r.author != ? "
        "AND NOT EXISTS (SELECT 1 FROM posts AS d WHERE d.kind = 'delete' "
        "AND d.linked = 1 AND d.target = r.hash AND d.author = r.author) "
        "AND EXISTS (SELECT 1 FROM posts AS p WHERE p.hash = r.target "
        "AND p.linked = 1 AND p.kind IN ('post', 'reply') AND p.author = ? "
        "AND NOT EXISTS (SELECT 1 FROM posts AS d WHERE d.kind = 'delete' "
        "AND d.linked = 1 AND d.target = p.hash AND d.author = p.author)) "
        "ORDER BY r.created_at DESC, r.hash DESC",
        (viewer, viewer),
    ).fetchall()
    # A reply beneath a deleted ancestor is hidden with that subtree, as
    # the thread view hides it, so filter on the ancestor chain before
    # applying the limit to keep the requested count of visible mentions.
    visible = [row for row in rows if not _hidden_under_deleted(conn, row)]
    rows = visible[:count]
    return annotate_replies(
        conn, annotate_reactions(conn, _public_posts(rows), viewer=viewer)
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
    conn: sqlite3.Connection, *, limit: int = DEFAULT_FEED_LIMIT
) -> List[Dict[str, Any]]:
    """Saved posts, newest save first.

    A post deleted after being saved stays in the list with its text
    blanked and ``deleted`` set, so the save is not silently lost.
    """
    count = max(1, min(int(limit), MAX_SAVED_POSTS))
    rows = conn.execute(
        "SELECT p.*, s.saved_at FROM saved_posts s "
        "JOIN posts p ON p.hash = s.hash "
        "ORDER BY s.saved_at DESC, s.hash DESC LIMIT ?",
        (count,),
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


def resolve_author(
    conn: sqlite3.Connection,
    target: str,
    *,
    registry: Any = None,
) -> Tuple[str, str]:
    """Resolve a peer name, destination hash or prefix to an author + name."""
    if not isinstance(target, str) or not target.strip():
        raise TanError("author must not be empty")
    value = target.strip()

    if registry is not None and hasattr(registry, "resolve"):
        try:
            peer = registry.resolve(value)
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
        self._ruleset_path = getattr(config, "tan_rules_path", None)

    @property
    def ruleset(self) -> Ruleset:
        """The admission ruleset, loaded lazily from the data directory.

        A ruleset that cannot be parsed fails closed — remote entries
        are denied — rather than silently reverting to admit-all, but
        loading is lazy so a broken file never blocks local operations.
        """
        if self._ruleset is None:
            try:
                self._ruleset = load_ruleset(self._ruleset_path)
            except RulesError as exc:
                log.error(
                    "Tan ruleset unusable (%s); denying remote entries", exc
                )
                self._ruleset = Ruleset(default_allow=False)
        return self._ruleset

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

    def timeline(
        self,
        *,
        include_all: bool = False,
        authors: Optional[Sequence[str]] = None,
        limit: int = 30,
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
    ) -> Dict[str, Any]:
        """Push a post to peers; return delivery counts."""
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
        for peer in peers:
            try:
                node.request(
                    peer, TAN_POST_PATH, payload, timeout=self.request_timeout
                )
                delivered += 1
            except Exception as exc:
                errors.append(
                    {"peer": getattr(peer, "hexhash", "?"), "error": str(exc)}
                )
        return {"delivered": delivered, "peers": len(peers), "errors": errors}

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
