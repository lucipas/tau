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

What gets committed at all is the owner's decision: a ruleset (see
``tau.tangents.rules``) can allow or block authors by identity, restrict
kinds, admit only followed authors, or deny by default.  It is consulted
for remote entries — pushes and sync imports — after signature
verification; locally created entries are never filtered, and with no
ruleset configured every well-signed entry is admitted.

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
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import RNS

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
REACTION_KINDS = (KIND_LIKE, KIND_UNLIKE)
ENTRY_KINDS = (KIND_POST,) + REACTION_KINDS

# Reaction envelopes sign two extra fields on top of the post fields, so a
# reaction's target cannot be swapped without invalidating its signature.
REACTION_HASH_FIELDS = HASH_FIELDS + ("kind", "target")

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
    if mesg.get("kind") in REACTION_KINDS:
        return REACTION_HASH_FIELDS
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
    if kind in REACTION_KINDS:
        # Reactions carry no text; normalize_message would also reject the
        # empty string it requires here.
        if raw_text not in (None, ""):
            raise PostError("reaction entries cannot carry text")
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
    if kind in REACTION_KINDS:
        target = _require_hex(raw_target, HASH_HEX_LENGTH, "target")
    else:
        if raw_target not in ("", None):
            raise PostError("posts cannot carry a reaction target")
        target = ""

    return {
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


def _identity_from_post(post: Dict[str, Any]) -> RNS.Identity:
    try:
        identity = RNS.Identity(create_keys=False)
        if not identity.load_public_key(bytes.fromhex(post["key"])):
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
    """Attach ``likes``/``liked`` fields to post dicts, in place."""
    counts = like_counts(conn, [post["hash"] for post in posts], viewer=viewer)
    for post in posts:
        info = counts.get(post["hash"], {"likes": 0, "liked": False})
        post["likes"] = info["likes"]
        post["liked"] = info["liked"]
    return posts


def resolve_post(conn: sqlite3.Connection, ref: str) -> Dict[str, Any]:
    """Resolve a post hash or unique hash prefix to a stored post."""
    if not isinstance(ref, str) or not ref.strip():
        raise TanError("post reference must not be empty")
    value = ref.strip().lower()
    if len(value) == HASH_HEX_LENGTH:
        post = get_post(conn, value)
        if post is not None and post.get("kind", KIND_POST) == KIND_POST:
            return post
        raise TanError(f"unknown post: {ref!r}")
    if len(value) >= 8:
        try:
            bytes.fromhex(value)
        except ValueError:
            raise TanError(f"unknown post: {ref!r}") from None
        rows = conn.execute(
            "SELECT hash FROM posts WHERE kind = 'post' AND hash LIKE ? LIMIT 8",
            (value + "%",),
        ).fetchall()
        if len(rows) == 1:
            post = get_post(conn, str(rows[0]["hash"]))
            if post is not None:
                return post
        if len(rows) > 1:
            raise TanError(f"{len(rows)} posts match prefix {value!r}")
    raise TanError(f"unknown post: {ref!r}")


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
) -> List[Dict[str, Any]]:
    """Read cached entries in chain order or reverse chronological order.

    Sync needs every entry (reactions included) to keep chains complete;
    display callers pass ``posts_only=True`` to hide reaction entries.
    """
    count = max(1, min(int(limit), MAX_FEED_POSTS))
    if author is not None:
        author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
        kind_filter = " AND kind = 'post'" if posts_only else ""
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
        where += " AND kind = 'post'"
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
    given, whether the viewer's like currently stands.
    """
    if include_all:
        posts = feed(conn, limit=limit, before=before, posts_only=True)
        return annotate_reactions(conn, posts, viewer=viewer)
    clean_authors = [
        _require_hex(author, AUTHOR_HEX_LENGTH, "author") for author in authors or []
    ]
    if not clean_authors:
        return []
    placeholders = ", ".join("?" for _ in clean_authors)
    params: List[Any] = list(clean_authors)
    where = f"linked = 1 AND kind = 'post' AND author IN ({placeholders})"
    if before is not None:
        where += " AND created_at < ?"
        params.append(float(before))
    params.append(max(1, min(int(limit), 200)))
    rows = conn.execute(
        f"SELECT {', '.join(_POST_COLUMNS)} FROM posts WHERE {where} "
        "ORDER BY created_at DESC, hash DESC LIMIT ?",
        params,
    ).fetchall()
    return annotate_reactions(conn, _public_posts(rows), viewer=viewer)


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


def author_stats(conn: sqlite3.Connection, author: str) -> Dict[str, Any]:
    author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
    row = conn.execute(
        "SELECT COUNT(*) AS posts, COALESCE(MAX(created_at), 0) AS last_post "
        "FROM posts WHERE author = ? AND linked = 1 AND kind = 'post'",
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
            "SELECT DISTINCT author, name FROM posts WHERE author LIKE ? LIMIT 8",
            (prefix + "%",),
        ).fetchall()
        if len(rows) == 1:
            return str(rows[0]["author"]), str(rows[0]["name"])
        if len(rows) > 1:
            raise TanError(f"{len(rows)} cached authors match {value!r}")

    raise TanError(f"unknown author or peer: {target!r}")


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

    def _admitted(self, conn: sqlite3.Connection, author: str, kind: str) -> bool:
        ruleset = self.ruleset
        followed: Iterable[str] = ()
        if ruleset.allow_followed:
            followed = [record["author"] for record in following(conn)]
        return ruleset.decide(
            author, kind, own_author=self.author, followed=followed
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
        publish: bool = False,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
    ) -> Dict[str, Any]:
        conn = self.connect()
        try:
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
            return timeline(
                conn,
                authors,
                limit=limit,
                include_all=include_all,
                viewer=self.author,
            )
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
                                conn,
                                str(raw.get("author", "")),
                                str(raw.get("kind") or KIND_POST),
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
                            conn,
                            str(raw.get("author", "")),
                            str(raw.get("kind") or KIND_POST),
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
                if not self._admitted(
                    conn, verified["author"], verified.get("kind", KIND_POST)
                ):
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
