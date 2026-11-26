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

A poll is a top-level entry like a post, but it carries a question and a
fixed list of options: it is written by the *asker* onto their own chain
with ``kind`` ``"poll"`` and the options signed alongside the question,
so a relay cannot swap them.  A vote is a targeted entry like a
reaction, written by the *voter* onto their own chain with ``kind``
``"vote"``, ``target`` set to the poll's hash, and the chosen option's
index as its text.  One standing vote per voter per poll decides the
tally — voting again changes your vote, the same way the latest
like/unlike entry decides a like.  Polls appear in the timeline, can be
replied to, liked and deleted like posts, and their tallies are derived
from stored votes, never stored; their options cannot be edited, since
changing them would rewrite tallies already cast.

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

import base64
import hashlib
import json
import logging
import math
import os
import random
import re
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple, Union

import RNS

from ..identity import load_public_key
from ..protocol import (
    APP_NAME,
    ASPECT,
    MAX_MESSAGE_CHARS,
    MAX_NAME_LENGTH,
    MAX_REQUEST_BYTES,
    ProtocolError,
    clean_contact,
    clean_location,
    decode,
    encode,
    normalize_message,
    response_error,
    response_ok,
    sanitize_file_name,
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
# How many hops of the follow graph a Tan sync (and the Following
# timeline) reaches: 1 = followed authors only, 2 = plus the authors
# they follow.  Default 2, per issue #467 — the people your friends
# follow are the authors most likely worth reading next.  Depth 3+
# is out: the closure grows fast and the marginal author is rarely
# worth the bandwidth.  A fetch of a followed author's list that
# fails is recorded, never fatal; at most this many lists are fetched
# per sync, so one slow peer cannot stall the whole round.
TAN_SYNC_DEPTH_DEFAULT = 2
TAN_SYNC_DEPTH_MIN = 1
TAN_SYNC_DEPTH_MAX = 2
TAN_SYNC_DEPTH_FETCH_TIMEOUT = 5.0
MAX_SYNC_DEPTH_FETCHES = 32
MAX_MUTED = 512
MAX_MUTED_KEYWORDS = 256
MAX_FOLLOWED_TAGS = 256
MAX_MUTED_TAGS = 256
MAX_TAN_LISTS = 32
MAX_LIST_MEMBERS = 256
MAX_LIST_NAME = 32
MAX_KEYWORD_LENGTH = 100
MAX_SAVED_POSTS = 512
MAX_DRAFTS = 100

# A poll asks one question with a fixed list of options: at least two,
# so there is something to choose between, and at most eight, so the
# signed entry still fits the request budget with room for the
# question.  An option is short — a choice, not a paragraph.
MIN_POLL_OPTIONS = 2
MAX_POLL_OPTIONS = 8
MAX_POLL_OPTION_CHARS = 100

# A draft publish claim goes stale after this long, so a publisher
# that crashed mid-publish cannot wedge its draft forever.  A normal
# publish finishes in seconds, far inside this window.
DRAFT_CLAIM_TTL = 600.0
MAX_HIDDEN_POSTS = 512
MAX_PINNED_POSTS = 10
MAX_GOSSIP_TTL = 3
GOSSIP_FANOUT = 3
MAX_SYNC_PAGES = 32

# Longest a disappearing Tan post may live after it is published: a
# post that outlives this is a permanent post, so refuse it at the
# flag instead of storing an expiry nobody will honour.  The same 7d
# cap as a scheduled post's delay — a post cannot be scheduled
# further out than it is allowed to live.
MAX_TAN_POST_EXPIRY = 7 * 24 * 60 * 60

# One image per Tan post, at most 2 MiB: the signed post carries only
# the hash, size, mime and name, and the bytes move separately over
# an RNS Resource when a peer asks for them, so the 8 KiB publish
# envelope stays small.  The local media store keeps at most 64 MiB
# of published and fetched images, pruning the least recently used.
MAX_IMAGE_BYTES = 2 * 1024 * 1024
MAX_MEDIA_STORE_BYTES = 64 * 1024 * 1024
IMAGE_MIME_TYPES = ("image/jpeg", "image/png", "image/gif", "image/webp")

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
KIND_POLL = "poll"
KIND_VOTE = "vote"
KIND_CLOSE = "close"
REACTION_KINDS = (KIND_LIKE, KIND_UNLIKE)
# Reactions, deletes and reposts carry no text; replies, edits, quotes
# and votes carry text but are still targeted entries, so their parent
# link is signed like a reaction's.  A vote's text is the chosen
# option's index — the choice is signed with the vote.
TEXTLESS_KINDS = REACTION_KINDS + (KIND_DELETE, KIND_REPOST, KIND_CLOSE)
# Targeted entries (reactions, deletes, replies, edits, quotes,
# reposts and votes) point at another post.
TARGETED_KINDS = TEXTLESS_KINDS + (KIND_REPLY, KIND_EDIT, KIND_QUOTE, KIND_VOTE)
ENTRY_KINDS = (KIND_POST, KIND_POLL) + TARGETED_KINDS
# Kinds that appear as timeline items: top-level posts, replies, reposts,
# quotes and polls.  A repost shows the original post's text with attribution;
# a quote is a post-like entry: it carries its own text, can be replied
# to and can be deleted.
TIMELINE_KINDS = (KIND_POST, KIND_REPLY, KIND_REPOST, KIND_QUOTE, KIND_POLL)
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
# A poll signs its options alongside the question, so a relay cannot
# swap them without breaking the hash.  A vote signs its target like
# any targeted entry; the chosen option rides in the text, which the
# post hash fields already cover.
POLL_HASH_FIELDS = HASH_FIELDS + ("kind", "options")

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
    "expires_at",
    "sig",
    "seq",
    "linked",
    "kind",
    "target",
    "options",
    "image",
    "location",
    "contact",
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
    """Fields covered by the hash for *mesg*, based on its entry kind.

    An expiry is signed when the post carries one, so a relay cannot
    strip or extend it without breaking the hash — but a post without
    one hashes exactly as before, so entries written before expiry
    existed still verify.
    """
    if mesg.get("kind") in TARGETED_KINDS:
        fields = TARGETED_HASH_FIELDS
    elif mesg.get("kind") == KIND_POLL:
        fields = POLL_HASH_FIELDS
    else:
        fields = HASH_FIELDS
    if mesg.get("expires_at") is not None:
        fields = fields + ("expires_at",)
    if mesg.get("image") is not None:
        # An image is signed like an expiry: a relay cannot swap the
        # attachment for another file's hash without breaking the
        # hash, and a post without one hashes exactly as before.
        fields = fields + ("image",)
    if mesg.get("location") is not None:
        # A place is signed the same way: a relay cannot move the
        # post to another place without breaking the hash, and a post
        # without one hashes exactly as before.
        fields = fields + ("location",)
    if mesg.get("contact") is not None:
        # A shared card is signed the same way: a relay cannot swap
        # the peer the post points at without breaking the hash, and
        # a post without one hashes exactly as before.
        fields = fields + ("contact",)
    return fields


def _canonical_body(post: Dict[str, Any]) -> bytes:
    body = {field: post[field] for field in _hash_fields_for(post)}
    return json.dumps(
        body,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def _encode_options(options: Optional[Sequence[str]]) -> Optional[str]:
    """Encode a poll's options for the posts table, or None."""
    if options is None:
        return None
    return json.dumps(list(options), ensure_ascii=False)


def _clean_image(value: Any) -> Dict[str, Any]:
    """Validate a Tan post image attachment: ``{sha256, size, mime, name}``.

    The bytes are never in the post — only the digest, size, mime and
    a bare file name — so a peer that wants the image fetches it by
    digest and re-checks the hash before trusting it.
    """
    if not isinstance(value, dict):
        raise PostError("post image must be an object")
    sha256 = value.get("sha256")
    if not isinstance(sha256, str) or len(sha256) != HASH_HEX_LENGTH:
        raise PostError("post image sha256 must be a 64 character hex digest")
    try:
        bytes.fromhex(sha256)
    except ValueError as exc:
        raise PostError("post image sha256 must be a 64 character hex digest") from exc
    size = value.get("size")
    if not isinstance(size, int) or isinstance(size, bool):
        raise PostError("post image size must be an integer")
    if size < 1 or size > MAX_IMAGE_BYTES:
        raise PostError(
            f"post image of {size} bytes exceeds the {MAX_IMAGE_BYTES} byte limit"
        )
    mime = value.get("mime")
    if mime not in IMAGE_MIME_TYPES:
        raise PostError(
            "post image mime must be one of: " + ", ".join(IMAGE_MIME_TYPES)
        )
    name = value.get("name")
    if (
        not isinstance(name, str)
        or not sanitize_file_name(name)
        or sanitize_file_name(name) != name
    ):
        raise PostError("post image name must be a safe bare file name")
    return {
        "sha256": sha256.lower(),
        "size": size,
        "mime": mime,
        "name": name,
    }


def _encode_image(image: Optional[Dict[str, Any]]) -> Optional[str]:
    """Encode a post's image attachment for the posts table, or None."""
    if image is None:
        return None
    return json.dumps(_clean_image(image), ensure_ascii=False)


def _decode_image(value: Any) -> Optional[Dict[str, Any]]:
    """Decode a stored image attachment: a dict, a JSON string, or None.

    Rows read from the posts table carry the JSON string; normalised
    entries carry the dict itself.  Anything else decodes to None, so
    a damaged row never crashes a display surface.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        try:
            return _clean_image(value)
        except PostError:
            return None
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return None
        if isinstance(parsed, dict):
            try:
                return _clean_image(parsed)
            except PostError:
                return None
    return None


def _clean_location(value: Any) -> Dict[str, Any]:
    """Validate a Tan post place attachment: ``{lat, lon, name?}``.

    The place rides the signed post, so a malformed one must be
    rejected here — not dropped — or the hash would no longer cover
    what the author meant to publish.  Reuses the DM pin validator
    (:func:`tau.protocol.clean_location`), which also strips a blank
    name and rejects out-of-range coordinates.
    """
    cleaned = clean_location(value)
    if cleaned is None:
        raise PostError(
            "post location must be an object with latitude -90..90 and "
            "longitude -180..180"
        )
    return cleaned


def _encode_location(location: Optional[Dict[str, Any]]) -> Optional[str]:
    """Encode a post's place attachment for the posts table, or None."""
    if location is None:
        return None
    return json.dumps(_clean_location(location), ensure_ascii=False)


def _decode_location(value: Any) -> Optional[Dict[str, Any]]:
    """Decode a stored place attachment: a dict, a JSON string, or None.

    Rows read from the posts table carry the JSON string; normalised
    entries carry the dict itself.  Anything else decodes to None, so
    a damaged row never crashes a display surface.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        try:
            return _clean_location(value)
        except PostError:
            return None
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return None
        if isinstance(parsed, dict):
            try:
                return _clean_location(parsed)
            except PostError:
                return None
    return None


def _clean_contact(value: Any) -> Dict[str, Any]:
    """Validate a Tan post contact-card attachment: ``{name, destination_hash}``.

    The card rides the signed post, so a malformed one must be
    rejected here — not dropped — or the hash would no longer cover
    what the author meant to publish.  Reuses the DM card validator
    (:func:`tau.protocol.clean_contact`), which also rejects a blank
    name and a hash that is not 32 hex characters.
    """
    cleaned = clean_contact(value)
    if cleaned is None:
        raise PostError(
            "post contact must be an object with a name and a "
            "32-character hex destination hash"
        )
    return cleaned


def _encode_contact(contact: Optional[Dict[str, Any]]) -> Optional[str]:
    """Encode a post's contact-card attachment for the posts table, or None."""
    if contact is None:
        return None
    return json.dumps(_clean_contact(contact), ensure_ascii=False)


def _decode_contact(value: Any) -> Optional[Dict[str, Any]]:
    """Decode a stored contact-card attachment: a dict, a JSON string, or None.

    Rows read from the posts table carry the JSON string; normalised
    entries carry the dict itself.  Anything else decodes to None, so
    a damaged row never crashes a display surface.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        try:
            return _clean_contact(value)
        except PostError:
            return None
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return None
        if isinstance(parsed, dict):
            try:
                return _clean_contact(parsed)
            except PostError:
                return None
    return None


def _decode_options(value: Any) -> Optional[List[str]]:
    """Decode stored poll options: a list, a JSON string, or None.

    Rows read from the posts table carry the JSON string; normalised
    entries carry the list itself.  Anything else decodes to None, so
    a damaged row never crashes a display surface.
    """
    if value is None:
        return None
    if isinstance(value, list):
        return [str(option) for option in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return None
        if isinstance(parsed, list) and all(
            isinstance(option, str) for option in parsed
        ):
            return list(parsed)
    return None


def _clean_poll_options(value: Any) -> List[str]:
    """Validate a poll's options: 2-8 distinct, short, normalised choices."""
    if not isinstance(value, list):
        raise PostError("poll options must be a list")
    if not MIN_POLL_OPTIONS <= len(value) <= MAX_POLL_OPTIONS:
        raise PostError(
            f"a poll needs {MIN_POLL_OPTIONS}-{MAX_POLL_OPTIONS} options"
        )
    options: List[str] = []
    seen: Set[str] = set()
    for raw in value:
        if not isinstance(raw, str):
            raise PostError("poll options must be strings")
        try:
            option = normalize_message(raw)
        except ProtocolError as exc:
            raise PostError(f"invalid poll option: {exc}") from exc
        if option != raw:
            raise PostError("poll option is not normalised")
        if len(option) > MAX_POLL_OPTION_CHARS:
            raise PostError(
                f"poll option is longer than {MAX_POLL_OPTION_CHARS} characters"
            )
        folded = option.casefold()
        if folded in seen:
            raise PostError(f"duplicate poll option: {option!r}")
        seen.add(folded)
        options.append(option)
    return options


def _canonical_post(post: Dict[str, Any]) -> Dict[str, Any]:
    """Return the public, canonical representation of a chain entry."""
    canonical = {
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
        "expires_at": post.get("expires_at"),
    }
    if post.get("kind") == KIND_POLL:
        # Options ride with the poll entry itself, so an exported or
        # synced poll carries its choices with it.
        canonical["options"] = _decode_options(post.get("options"))
    if post.get("image") is not None:
        # The attachment rides with the post entry itself, so an
        # exported or synced post carries its image reference with it.
        canonical["image"] = _decode_image(post.get("image"))
    if post.get("location") is not None:
        # The place rides with the post entry itself, so an exported
        # or synced post carries its place with it.
        canonical["location"] = _decode_location(post.get("location"))
    if post.get("contact") is not None:
        # The card rides with the post entry itself, so an exported
        # or synced post carries its peer with it.
        canonical["contact"] = _decode_contact(post.get("contact"))
    return canonical


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

    raw_expires = mesg.get("expires_at")
    expires_at: Optional[float] = None
    if raw_expires is not None:
        expires_at = _require_timestamp(raw_expires, "expires_at")
        if expires_at <= created_at:
            raise PostError("expires_at must be after created_at")

    raw_target = mesg.get("target", "")
    if raw_target is None:
        raw_target = ""
    if kind in TARGETED_KINDS:
        target = _require_hex(raw_target, HASH_HEX_LENGTH, "target")
    else:
        if raw_target not in ("", None):
            raise PostError("posts cannot carry a reaction target")
        target = ""

    if kind == KIND_VOTE:
        # A vote's text is the chosen option's index, in canonical
        # decimal form.  The poll itself is not consulted here — it
        # may not have arrived yet — so the range check against the
        # poll's options happens where the tally is derived.
        # ``str.isdigit`` is True for Unicode digits ("²") that
        # ``int`` rejects, so require ASCII digits first: otherwise a
        # crafted vote raises a bare ValueError out of verification —
        # past the ``(ProtocolError, TanError)`` catch in the sync
        # handler — instead of a clean PostError.
        if (
            not text.isascii()
            or not text.isdigit()
            or text != str(int(text))
        ):
            raise PostError("vote text must be an option index")
        if not 0 <= int(text) < MAX_POLL_OPTIONS:
            raise PostError(
                f"vote option index must be 0-{MAX_POLL_OPTIONS - 1}"
            )
    if kind == KIND_POLL:
        options = _clean_poll_options(mesg.get("options"))
    else:
        if mesg.get("options") is not None:
            raise PostError("only polls can carry options")
        options = None

    if kind in (KIND_POST, KIND_QUOTE):
        # A quote is a standalone timeline item like a post, not a
        # reply, so it may carry an image — the same signed descriptor
        # and on-demand bytes as a post's.  A location stays post-only:
        # a quote already names the place it answers through its
        # target, and a second pin would fight the embedded original.
        # A contact card stays post-only for the same reason: the card
        # names a peer the post points at, and a quote already names
        # the post it answers.
        image = (
            _clean_image(mesg["image"]) if mesg.get("image") is not None else None
        )
        if kind == KIND_POST:
            location = (
                _clean_location(mesg["location"])
                if mesg.get("location") is not None
                else None
            )
            contact = (
                _clean_contact(mesg["contact"])
                if mesg.get("contact") is not None
                else None
            )
        else:
            if mesg.get("location") is not None:
                raise PostError("only posts can carry a location")
            if mesg.get("contact") is not None:
                raise PostError("only posts can carry a contact card")
            location = None
            contact = None
    else:
        if mesg.get("image") is not None:
            raise PostError("only posts and quotes can carry an image")
        if mesg.get("location") is not None:
            raise PostError("only posts can carry a location")
        if mesg.get("contact") is not None:
            raise PostError("only posts can carry a contact card")
        image = None
        location = None
        contact = None

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
        "expires_at": expires_at,
        "options": options,
        "image": image,
        "location": location,
        "contact": contact,
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


def check_post_text_fits(
    text: str,
    *,
    name: Optional[str] = None,
    reply_to: Optional[str] = None,
    expires: bool = False,
    image: Optional[Dict[str, Any]] = None,
    location: Optional[Dict[str, Any]] = None,
    contact: Optional[Dict[str, Any]] = None,
) -> None:
    """Raise PostError if *text* cannot fit a Tan request once encoded.

    The same check ``_check_wire_size`` runs before a post is stored —
    here for a post that does not exist yet, so a caller scheduling one
    can refuse it up front instead of failing every publish attempt
    later.  Maximum-length placeholders stand in for the author, key,
    chain head and (when *name* is unknown) display name the real post
    will carry, so a text that passes here always fits; one that fails
    is rejected before it is stored.  When *expires* is true the post
    will also carry a float ``expires_at`` instead of ``null``, so a
    widest-possible timestamp stands in for it.  When *image* is given
    the attachment rides in the estimate too — its descriptor is
    ~150 bytes of JSON, enough to push a borderline text over budget.
    When *location* is given the pin rides in the estimate the same
    way — its name can be long enough to matter on a borderline text.
    When *contact* is given the card rides in the estimate the same
    way — its name can be long enough to matter on a borderline text.
    """
    try:
        clean_text = normalize_message(text)
    except ProtocolError as exc:
        raise PostError(str(exc)) from exc
    now = time.time()
    _check_wire_size(
        {
            "v": TAN_VERSION,
            "author": "0" * AUTHOR_HEX_LENGTH,
            "key": "0" * KEY_HEX_LENGTH,
            "name": (
                sanitize_name(name)
                if name is not None
                else "x" * MAX_NAME_LENGTH
            ),
            "text": clean_text,
            "created_at": round(now, 3),
            "prev": "0" * HASH_HEX_LENGTH,
            "kind": KIND_REPLY if reply_to is not None else KIND_POST,
            "target": reply_to or "",
            "expires_at": (
                now + MAX_TAN_POST_EXPIRY + 0.123456789 if expires else None
            ),
            **({"image": _clean_image(image)} if image is not None else {}),
            **(
                {"location": _clean_location(location)}
                if location is not None
                else {}
            ),
            **(
                {"contact": _clean_contact(contact)}
                if contact is not None
                else {}
            ),
        }
    )


def check_poll_fits(
    question: str,
    options: Sequence[str],
    *,
    name: Optional[str] = None,
    expires: bool = False,
) -> None:
    """Raise PostError if a poll cannot fit a Tan request once encoded.

    The poll analogue of :func:`check_post_text_fits`: the question and
    the options ride in one signed entry, so a question that fits alone
    can still overflow the 8 KiB budget once 8 long options join it.
    Callers scheduling a poll refuse it up front instead of failing
    every publish attempt later.  Options are cleaned the same way
    :func:`create_poll` cleans them, so a poll that passes here is one
    ``create_poll`` will accept.
    """
    try:
        clean_question = normalize_message(question)
    except ProtocolError as exc:
        raise PostError(str(exc)) from exc
    clean_options = _clean_poll_options(list(options))
    now = time.time()
    _check_wire_size(
        {
            "v": TAN_VERSION,
            "author": "0" * AUTHOR_HEX_LENGTH,
            "key": "0" * KEY_HEX_LENGTH,
            "name": (
                sanitize_name(name)
                if name is not None
                else "x" * MAX_NAME_LENGTH
            ),
            "text": clean_question,
            "created_at": round(now, 3),
            "prev": "0" * HASH_HEX_LENGTH,
            "kind": KIND_POLL,
            "options": clean_options,
            "expires_at": (
                now + MAX_TAN_POST_EXPIRY + 0.123456789 if expires else None
            ),
        }
    )


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
    expires_at REAL,
    sig TEXT NOT NULL,
    seq INTEGER NOT NULL DEFAULT 0,
    linked INTEGER NOT NULL DEFAULT 0,
    kind TEXT NOT NULL DEFAULT 'post',
    target TEXT NOT NULL DEFAULT '',
    options TEXT,
    image TEXT,
    location TEXT,
    contact TEXT
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

CREATE TABLE IF NOT EXISTS followed_tags (
    tag TEXT PRIMARY KEY,
    display TEXT NOT NULL,
    added_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS muted_tags (
    tag TEXT PRIMARY KEY,
    display TEXT NOT NULL,
    added_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS muted_authors (
    author TEXT PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    muted_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS tan_lists (
    name TEXT PRIMARY KEY,
    display TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS tan_list_members (
    list_name TEXT NOT NULL,
    author TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    added_at REAL NOT NULL,
    PRIMARY KEY (list_name, author)
);

CREATE TABLE IF NOT EXISTS saved_posts (
    hash TEXT PRIMARY KEY,
    saved_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS pinned_posts (
    hash TEXT PRIMARY KEY,
    pinned_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS hidden_posts (
    hash TEXT PRIMARY KEY,
    hidden_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS purged_posts (
    hash TEXT PRIMARY KEY,
    author TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    prev TEXT NOT NULL DEFAULT '',
    linked INTEGER NOT NULL DEFAULT 0,
    target TEXT NOT NULL DEFAULT '',
    restored INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS drafts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    text TEXT NOT NULL,
    reply_to TEXT NOT NULL DEFAULT '',
    expires_in REAL,
    post_hash TEXT,
    pending_peers TEXT,
    claimed_at REAL,
    claim_token TEXT,
    created_at REAL NOT NULL
);
"""


def _table_columns(conn: sqlite3.Connection, table: str) -> Set[str]:
    """Column names of *table* right now — a fresh read, not a snapshot.

    Migrations run while other threads may be migrating the same file,
    so a column set read once at the start of :func:`init_db` can go
    stale mid-migration: a thread that snapshots after ``kind`` lands
    but before ``target`` would conclude the pair is done and return
    without it.  Every migration decision below re-reads, so a column
    is only skipped when it is really there; the check-then-ALTER gap
    that remains is the one :func:`_add_column` already tolerates.
    """
    return {
        str(row["name"])
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }


def _add_column(
    conn: sqlite3.Connection, table: str, column: str, column_type: str
) -> None:
    """Add a column to *table*, tolerating a concurrent migration.

    Two processes opening the same pre-migration database can both
    see the column missing and both try the ALTER; the loser gets
    "duplicate column name" from SQLite, which means the column is
    there — success, not an error.
    """
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")
    except sqlite3.OperationalError as exc:
        if "duplicate column" not in str(exc):
            raise


def _add_draft_column(
    conn: sqlite3.Connection, column: str, column_type: str
) -> None:
    """Add a column to ``drafts`` (see :func:`_add_column`)."""
    _add_column(conn, "drafts", column, column_type)


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
    columns = _table_columns(conn, "posts")
    if columns and "v" not in columns:  # migrate an early scaffold database
        _add_column(conn, "posts", "v", "INTEGER NOT NULL DEFAULT 1")
    if columns and "kind" not in _table_columns(conn, "posts"):
        # migrate a pre-reactions database; kind and target are checked
        # separately — a concurrent migration may have added one but
        # not the other yet (see _table_columns).
        _add_column(conn, "posts", "kind", "TEXT NOT NULL DEFAULT 'post'")
    if columns and "target" not in _table_columns(conn, "posts"):
        _add_column(conn, "posts", "target", "TEXT NOT NULL DEFAULT ''")
    if columns and "expires_at" not in _table_columns(conn, "posts"):
        # migrate a pre-expiry database
        _add_column(conn, "posts", "expires_at", "REAL")
    if columns and "options" not in _table_columns(conn, "posts"):
        # migrate a pre-polls database: a poll's options live in one
        # JSON column on its own entry, so the migration is a single
        # ALTER, not a new table.
        _add_column(conn, "posts", "options", "TEXT")
    if columns and "image" not in _table_columns(conn, "posts"):
        # migrate a pre-media database: a post's image attachment lives
        # in one JSON column on its own entry, like a poll's options.
        _add_column(conn, "posts", "image", "TEXT")
    if columns and "location" not in columns:  # migrate a pre-location database
        # A post's place lives in one JSON column on its own entry,
        # like a post's image attachment.
        _add_column(conn, "posts", "location", "TEXT")
    if columns and "contact" not in columns:  # migrate a pre-contact database
        # A post's shared card lives in one JSON column on its own
        # entry, like a post's place attachment.
        _add_column(conn, "posts", "contact", "TEXT")
    draft_columns = _table_columns(conn, "drafts")
    if draft_columns and "post_hash" not in draft_columns:
        # migrate a pre-retry database: drafts could not remember
        # the signed post a partial publish had already created
        _add_draft_column(conn, "post_hash", "TEXT")
    if draft_columns and "pending_peers" not in _table_columns(conn, "drafts"):
        _add_draft_column(conn, "pending_peers", "TEXT")
    if draft_columns and "claimed_at" not in _table_columns(conn, "drafts"):
        # migrate a pre-claim database: drafts could not be claimed,
        # so two concurrent publish-draft runs could sign two posts
        _add_draft_column(conn, "claimed_at", "REAL")
    if draft_columns and "claim_token" not in _table_columns(conn, "drafts"):
        # the claim's owner token, so only the publisher that won
        # the claim can refresh or release it
        _add_draft_column(conn, "claim_token", "TEXT")
    purged_columns = _table_columns(conn, "purged_posts")
    if purged_columns and "linked" not in purged_columns:
        # migrate a pre-provenance database: purge records made
        # before the linked flag existed cannot prove a chain
        # position, so they bridge nothing — the column defaults to 0.
        _add_column(conn, "purged_posts", "linked", "INTEGER NOT NULL DEFAULT 0")
    if purged_columns and "target" not in _table_columns(conn, "purged_posts"):
        # migrate a pre-target database: records made before the
        # target was carried cannot name the deleted original, so an
        # edit record stays unbacked until it is re-recorded.
        _add_column(conn, "purged_posts", "target", "TEXT NOT NULL DEFAULT ''")
    if purged_columns and "restored" not in _table_columns(conn, "purged_posts"):
        # migrate a pre-provenance database: records made before the
        # restored flag existed were all made locally, so they count
        # as verified — the column defaults to 0.
        _add_column(conn, "purged_posts", "restored", "INTEGER NOT NULL DEFAULT 0")
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
    required = {
        "author",
        "name",
        "created_at",
        "seq",
        "kind",
        "target",
        "linked",
        "expires_at",
        "options",
    }
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


def find_post_by_image(
    conn: sqlite3.Connection, sha256: str
) -> Optional[Dict[str, Any]]:
    """Return the newest stored post carrying image *sha256*, or ``None``.

    Used when serving a media fetch: the responder needs the post's
    recorded name for the file offer, and the requester needs the
    post's author to know who to ask.  A node that fetched the bytes
    without the post serves them under a fallback name instead.
    """
    digest = sha256.strip().lower()
    if len(digest) != HASH_HEX_LENGTH:
        return None
    try:
        bytes.fromhex(digest)
    except ValueError:
        return None
    row = conn.execute(
        f"SELECT {', '.join(_POST_COLUMNS)} FROM posts "
        "WHERE image LIKE ? ORDER BY created_at DESC LIMIT 1",
        (f'%"sha256": "{digest}"%',),
    ).fetchone()
    return _row_to_post_with_meta(row) if row is not None else None


# -- media store -----------------------------------------------------------
#
# Image bytes live outside the Tan database, in ``<data_dir>/media``,
# one file per SHA-256 digest.  The signed post carries only the
# digest, so any peer holding the bytes can serve them — the author,
# or a peer that already fetched them.


def media_dir(data_dir: Union[str, Path]) -> Path:
    """Return the media store directory under *data_dir*."""
    return Path(data_dir).expanduser() / "media"


_PINNED_MEDIA_SUBDIR = "own"


def _media_file_matches(path: Path, digest: str) -> bool:
    """Return True when *path*'s bytes hash to *digest*."""
    hasher = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 16), b""):
                hasher.update(chunk)
    except OSError:
        return False
    return hasher.hexdigest() == digest


def media_requires_publication(stored: Path) -> bool:
    """Return True when *stored* is a local upload that must not be served
    until a post carrying it is published (see :func:`store_media`)."""
    return stored.parent.name == _PINNED_MEDIA_SUBDIR


def media_path(data_dir: Union[str, Path], sha256: str) -> Optional[Path]:
    """Return the stored bytes for *sha256*, or ``None`` when absent.

    A file whose bytes no longer hash to *sha256* counts as absent so
    a later fetch or store can replace it.  A hit refreshes the file's
    recency for the LRU prune order.
    """
    digest = str(sha256).strip().lower()
    if len(digest) != HASH_HEX_LENGTH:
        return None
    try:
        bytes.fromhex(digest)
    except ValueError:
        return None
    directory = media_dir(data_dir)
    for candidate in (directory / _PINNED_MEDIA_SUBDIR / digest, directory / digest):
        if candidate.is_file() and _media_file_matches(candidate, digest):
            now = time.time()
            try:
                os.utime(candidate, (now, now))
            except OSError:  # pragma: no cover - best effort
                pass
            return candidate
    return None


def store_media(
    data_dir: Union[str, Path],
    sha256: str,
    data: bytes,
    *,
    max_total_bytes: int = MAX_MEDIA_STORE_BYTES,
    pinned: bool = False,
) -> Path:
    """Store *data* in the media store under its SHA-256, verifying it.

    Writes atomically (temp file + rename) and prunes the least
    recently used cached files when the store would exceed
    *max_total_bytes*, so one prolific author cannot fill the disk.
    ``pinned`` files (the local user's own uploads) live in a separate
    subdirectory that pruning never touches.  An existing copy is kept
    only if its bytes still verify; otherwise it is replaced.  Returns
    the path of the stored file.
    """
    digest = str(sha256).strip().lower()
    if len(digest) != HASH_HEX_LENGTH:
        raise TanError("media sha256 must be a 64 character hex digest")
    try:
        bytes.fromhex(digest)
    except ValueError as exc:
        raise TanError("media sha256 must be a 64 character hex digest") from exc
    if hashlib.sha256(data).hexdigest() != digest:
        raise TanError("media bytes do not match their sha256")
    if len(data) > MAX_IMAGE_BYTES:
        raise TanError(
            f"media of {len(data)} bytes exceeds the {MAX_IMAGE_BYTES} byte limit"
        )
    directory = media_dir(data_dir)
    target_dir = directory / _PINNED_MEDIA_SUBDIR if pinned else directory
    ensure_dir(target_dir)
    target = target_dir / digest
    if target.is_file() and _media_file_matches(target, digest):
        # Already stored and intact: refresh its recency for the prune order.
        now = time.time()
        os.utime(target, (now, now))
        return target
    tmp = target_dir / f".{digest}.tmp-{uuid.uuid4().hex}"
    try:
        tmp.write_bytes(data)
        os.replace(tmp, target)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
        except OSError:  # pragma: no cover - best effort
            pass
    if not pinned:
        _prune_media_store(directory, max_total_bytes=max_total_bytes)
    return target


def detect_image_mime(data: bytes) -> Optional[str]:
    """Detect a Tan image's MIME type from its magic bytes, or None.

    The declared type is never trusted — the bytes decide — so a
    mislabelled upload cannot smuggle another format past the post.
    """
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return None


def image_attachment_from_data_url(
    value: Any, data_dir: Union[str, Path]
) -> Dict[str, Any]:
    """Decode a base64 data-URL image upload into a Tan attachment dict.

    Validates the same way the CLI's ``_read_image`` does — JPEG, PNG,
    GIF or WebP by magic bytes, 1 byte .. ``MAX_IMAGE_BYTES`` — then
    stores the bytes pinned in the local media store, so the author can
    serve them when a peer asks.  Returns the ``{sha256, size, mime,
    name}`` descriptor the signed post carries; the bytes themselves
    never go in the post.  Raises :class:`PostError` on bad input.
    """
    if not isinstance(value, str) or not value.startswith("data:"):
        raise PostError("post image must be a base64 data URL")
    try:
        header, encoded = value[5:].split(",", 1)
    except ValueError as exc:
        raise PostError("post image must be a base64 data URL") from exc
    if not header.endswith(";base64"):
        raise PostError("post image must be a base64 data URL")
    try:
        data = base64.b64decode(encoded, validate=True)
    except Exception as exc:
        raise PostError("post image is not valid base64") from exc
    if len(data) < 1 or len(data) > MAX_IMAGE_BYTES:
        raise PostError(
            f"post image of {len(data)} bytes is outside the "
            f"1..{MAX_IMAGE_BYTES} byte limit"
        )
    mime = detect_image_mime(data)
    if mime is None:
        raise PostError("post image must be a JPEG, PNG, GIF or WebP file")
    digest = hashlib.sha256(data).hexdigest()
    try:
        store_media(data_dir, digest, data, pinned=True)
    except TanError as exc:
        raise PostError(str(exc)) from exc
    ext = {
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/gif": "gif",
        "image/webp": "webp",
    }[mime]
    return {"sha256": digest, "size": len(data), "mime": mime, "name": f"image.{ext}"}


def _prune_media_store(directory: Path, *, max_total_bytes: int) -> None:
    """Drop least-recently-used media files until under *max_total_bytes*."""
    try:
        files = [
            (path.stat().st_mtime, path.stat().st_size, path)
            for path in directory.iterdir()
            if path.is_file() and not path.name.startswith(".")
        ]
    except OSError:  # pragma: no cover - defensive
        return
    remaining = sum(size for _, size, _ in files)
    if remaining <= max_total_bytes:
        return
    for _, size, path in sorted(files):
        if remaining <= max_total_bytes:
            break
        try:
            path.unlink()
        except OSError:  # pragma: no cover - defensive
            continue
        remaining -= size


def head(conn: sqlite3.Connection, author: str) -> Optional[str]:
    """Return the hash of *author*'s current chain head, if any."""
    author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
    row = conn.execute("SELECT hash FROM heads WHERE author = ?", (author,)).fetchone()
    return str(row["hash"]) if row is not None else None


@contextmanager
def _chain_write(conn: sqlite3.Connection) -> Iterator[None]:
    """Hold the database write lock across a head read and its append.

    Every local author-chain writer reads the chain head and then
    appends: two writers racing that window both read the same head
    and append siblings with the same ``seq`` — a fork no later check
    repairs.  ``BEGIN IMMEDIATE`` takes the write lock before the head
    read and releases it only once the append has committed, so the
    read and the append are one atomic step.  The append itself commits
    through :func:`store_post`, so the exit only commits when the
    append took the duplicate-hash early return and left the
    transaction open.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        if conn.in_transaction:
            conn.rollback()
        raise
    else:
        if conn.in_transaction:
            conn.commit()


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


def _resolve_pending_purged(
    conn: sqlite3.Connection, author: str, resolved_hash: str
) -> None:
    """Upgrade pending purge records whose predecessor just linked.

    A record kept pending because its predecessor had not arrived
    yet is reconsidered whenever an entry takes a chain position:
    if its predecessor now resolves, the record upgrades to linked
    and its waiting children link up.  The upgrade repeats for
    consecutive gaps — a record upgraded here may itself resolve
    another pending record naming it as predecessor.  Only the
    linked flag moves: the record's metadata stands as restored,
    and the upgrade never re-judges the backing delete (a pending
    record was already backed and referenced when stored).
    """
    queue = [resolved_hash]
    seen: Set[str] = set()
    while queue:
        current = queue.pop(0)
        if current in seen:
            continue
        seen.add(current)
        rows = conn.execute(
            "SELECT hash, name, prev, target FROM purged_posts "
            "WHERE author = ? AND prev = ? AND linked = 0",
            (author, current),
        ).fetchall()
        for row in rows:
            digest = str(row["hash"])
            if not _purge_prev_resolves(conn, author, str(row["prev"] or "")):
                continue
            conn.execute(
                "UPDATE purged_posts SET linked = 1 WHERE hash = ? AND author = ?",
                (digest, author),
            )
            _link_children_of_purged(conn, author, digest)
            queue.append(digest)


def _promote_children(
    conn: sqlite3.Connection, author: str, parent_hash: str, parent_seq: int
) -> None:
    """Attach orphaned descendants once their parent becomes linked."""
    # The parent just took a chain position: a pending purge record
    # naming it as predecessor may now resolve — upgrade it and
    # link its waiting children (see _resolve_pending_purged).
    _resolve_pending_purged(conn, author, parent_hash)
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
            if str(child["kind"]) == KIND_DELETE:
                # A delete that arrived before its parent links up here:
                # it is authoritative now, so its target goes with it.
                _apply_delete(
                    conn,
                    str(child["author"]),
                    str(child["target"] or ""),
                    linked=True,
                )
            _resolve_pending_purged(conn, author, child_hash)
            queue.append((child_hash, child_seq))


def _would_link(conn: sqlite3.Connection, author: str, prev: str) -> bool:
    """Whether an entry with parent *prev* would take a chain position.

    The position an entry would have taken if it had been stored: a
    first entry links, an entry whose parent row is linked links, and
    an entry whose parent is missing links only on purge proof — a
    linked delete targeting the parent, or a linked purge record for
    it.  The drop guard uses this to record a dropped entry's chain
    provenance: a dropped edit whose parent was linked would have
    linked, so its later children may link through its record; a
    dropped entry whose parent was missing or unlinked would not, so
    its children keep waiting.  This is the same proof the
    missing-parent branch in :func:`store_post` applies, minus the
    parent-row check it does separately.
    """
    if not prev:
        return True
    parent = _fetch_post(conn, prev)
    if parent is not None:
        # The normal storage path rejects a parent from another
        # author; the drop path must not let one through either, or
        # a dropped entry could take a chain position through a
        # predecessor that was never on its author's chain.
        if str(parent["author"]) != author:
            return False
        return _int_or_none(parent["linked"]) == 1
    row = conn.execute(
        "SELECT 1 FROM posts d WHERE d.kind = 'delete' "
        "AND d.linked = 1 AND d.target = ? AND d.author = ? LIMIT 1",
        (prev, author),
    ).fetchone()
    if row is not None:
        return True
    return _purged_post(conn, prev, author, linked_only=True) is not None


def store_post(
    conn: sqlite3.Connection,
    mesg: Dict[str, Any],
    *,
    now: Optional[float] = None,
    verify: bool = True,
) -> Tuple[Dict[str, Any], bool]:
    """Validate and store a post, returning ``(post, is_new)``.

    A timeline post or edit its author deleted is dropped instead of
    stored — the tombstone stays authoritative after the purge, so a
    re-offered post must not resurrect the row.  The drop reports
    ``is_new`` as ``False``, like a duplicate.
    """
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

    # A post its author deleted never lands again: the tombstone stays
    # authoritative after the purge, so a peer re-offering the post on
    # a later sync must not resurrect it — and a post arriving after
    # its delete is dropped outright instead of being stored hidden.
    # An edit carries the post's text, so an edit of a deleted post is
    # dropped the same way; reactions and votes carry no content and
    # stay inert without the post row.  The drop also records the
    # purge: this node has now seen a real, signed entry with this
    # hash from this author, so the gap it leaves is proven — and any
    # children already waiting on it link up.
    if post.get("kind") in TIMELINE_KINDS:
        if _delete_entry_exists(conn, digest, post["author"]):
            with conn:
                _record_purged(
                    conn,
                    digest,
                    post["author"],
                    str(post.get("name") or ""),
                    str(post.get("prev") or ""),
                    linked=_would_link(
                        conn, post["author"], str(post.get("prev") or "")
                    ),
                )
                _link_children_of_purged(conn, post["author"], digest)
            return _canonical_post(post), False
    if post.get("kind") == KIND_EDIT:
        # An edit is dropped when its author deleted the edited post,
        # so the check targets the post, not the edit's own hash.
        # The record carries that post as the edit's target: a delete
        # never targets the edit directly, so without it the record
        # could never be backed by its signed delete on import.
        if _delete_entry_exists(conn, str(post.get("target") or ""), post["author"]):
            with conn:
                _record_purged(
                    conn,
                    digest,
                    post["author"],
                    str(post.get("name") or ""),
                    str(post.get("prev") or ""),
                    linked=_would_link(
                        conn, post["author"], str(post.get("prev") or "")
                    ),
                    target=str(post.get("target") or ""),
                )
                _link_children_of_purged(conn, post["author"], digest)
            return _canonical_post(post), False

    author = post["author"]
    prev = post["prev"]
    linked = True
    seq = 1
    if prev:
        parent = _fetch_post(conn, prev)
        if parent is None:
            # The parent may have been purged: a linked delete entry
            # from this author targeting it, or a linked purge record
            # for it, means the parent is gone by design, not by
            # partial sync, so this entry links up here instead of
            # waiting for a parent that never comes.  The delete must
            # be linked itself — an unlinked tombstone may have
            # arrived before its own parent, and its chain position is
            # not established yet, so the child waits until the
            # tombstone links and the purge is applied.  The record
            # must be linked too — it is the proof for a dropped
            # *edit* row (no delete targets the edit directly; the
            # delete targets the post), but a record from an entry
            # that never linked proves no chain position, so the child
            # of such an entry keeps waiting.  A forged delete from
            # anyone else cannot match this author, and a forged
            # delete creates no record — records are made only when a
            # verified row is actually purged or dropped — so neither
            # proof admits a forgery.
            row = conn.execute(
                "SELECT 1 FROM posts d WHERE d.kind = 'delete' "
                "AND d.linked = 1 AND d.target = ? AND d.author = ? LIMIT 1",
                (prev, author),
            ).fetchone()
            if (
                row is not None
                or _purged_post(conn, prev, author, linked_only=True)
                is not None
            ):
                linked = True
                seq = max_seq(conn, author) + 1
            else:
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
                "received_at, expires_at, sig, seq, linked, kind, target, options, image, location, contact) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                    post.get("expires_at"),
                    post["sig"],
                    seq,
                    1 if linked else 0,
                    post.get("kind") or KIND_POST,
                    post.get("target") or "",
                    _encode_options(post.get("options")),
                    _encode_image(post.get("image")),
                    _encode_location(post.get("location")),
                    _encode_contact(post.get("contact")),
                ),
            )
            if linked:
                _advance_head(conn, author, digest, seq, post["created_at"])
                _promote_children(conn, author, digest, seq)
            if post.get("kind") == KIND_DELETE:
                # A stored delete is authoritative, linked or not: the
                # target leaves the store once the delete is linked,
                # and the entries created after it link up again either
                # way (see :func:`_apply_delete`).
                _apply_delete(
                    conn,
                    author,
                    str(post.get("target") or ""),
                    linked=linked,
                )
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
    expires_at: Optional[float] = None,
    image: Optional[Dict[str, Any]] = None,
    location: Optional[Dict[str, Any]] = None,
    contact: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Create, sign and store a post owned by *identity*.

    When *expires_at* is given the post disappears from display
    surfaces once that time passes; the expiry is signed with the
    post, so it cannot be stripped or extended without breaking the
    hash.  It must be after the post's creation time.  When *image*
    is given — ``{sha256, size, mime, name}`` — the attachment is
    signed with the post the same way; the bytes themselves live in
    the local media store, never in the post.  When *location* is
    given — ``{lat, lon, name?}`` — the place is signed with the post
    the same way, so a relay cannot move the post without breaking
    the hash.  When *contact* is given — ``{name, destination_hash}``
    — the card is signed with the post the same way, so a relay
    cannot swap the peer the post points at without breaking the
    hash.
    """
    timestamp = round(float(now if now is not None else time.time()), 3)
    author = _author_from_identity(identity)
    clean_name = sanitize_name(name)
    clean_text = normalize_message(text)
    clean_image = _clean_image(image) if image is not None else None
    clean_location = _clean_location(location) if location is not None else None
    clean_contact = _clean_contact(contact) if contact is not None else None
    if expires_at is not None:
        expires_at = _require_timestamp(expires_at, "expires_at")
        if expires_at <= timestamp:
            raise PostError("expires_at must be after created_at")
    # The head read and the append must be one atomic step: see
    # _chain_write — two writers racing this window would fork the chain.
    with _chain_write(conn):
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
        if expires_at is not None:
            body["expires_at"] = expires_at
        if clean_image is not None:
            body["image"] = clean_image
        if clean_location is not None:
            body["location"] = clean_location
        if clean_contact is not None:
            body["contact"] = clean_contact
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
    # The head read and the append must be one atomic step: see
    # _chain_write — two writers racing this window would fork the chain.
    with _chain_write(conn):
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
    expires_at: Optional[float] = None,
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
    parent arrives.  A reply may carry its own *expires_at*, like a
    post; replying to a post whose expiry has already passed raises,
    the same as replying to a deleted post.
    """
    target = _require_hex(target, HASH_HEX_LENGTH, "target")
    timestamp = round(float(now if now is not None else time.time()), 3)
    author = _author_from_identity(identity)
    existing = _target_post_or_raise(conn, target, "post has been deleted")
    if existing is None:
        raise TanError(f"unknown post: {target!r}")
    if existing.get("kind", KIND_POST) not in REPLYABLE_KINDS + (KIND_QUOTE, KIND_POLL):
        raise TanError("only posts, replies, quotes and polls can be replied to")
    if not existing.get("linked"):
        raise TanError(
            "post has not linked yet; sync its chain before replying"
        )
    if _is_deleted(conn, existing):
        raise TanError("post has been deleted")
    if is_expired(existing, now=timestamp):
        raise TanError("post has expired")
    if expires_at is not None:
        expires_at = _require_timestamp(expires_at, "expires_at")
        if expires_at <= timestamp:
            raise PostError("expires_at must be after created_at")
    clean_text = normalize_message(text)
    # The head read and the append must be one atomic step: see
    # _chain_write — two writers racing this window would fork the chain.
    with _chain_write(conn):
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
        if expires_at is not None:
            body["expires_at"] = expires_at
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
    image: Optional[Dict[str, Any]] = None,
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
    original until the parent arrives.  When *image* is given — the
    ``{sha256, size, mime, name}`` descriptor a post carries — the
    quote carries it the same way: a quote is a top-level entry, so
    the top-level-only image rule bars replies, not quotes.  Peers
    on releases from before quotes could carry images reject such a
    quote outright, the same way they reject expiring posts.
    """
    target = _require_hex(target, HASH_HEX_LENGTH, "target")
    timestamp = round(float(now if now is not None else time.time()), 3)
    author = _author_from_identity(identity)
    existing = _target_post_or_raise(conn, target, "post has been deleted")
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
    clean_image = _clean_image(image) if image is not None else None
    # The head read and the append must be one atomic step: see
    # _chain_write — two writers racing this window would fork the chain.
    with _chain_write(conn):
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
        if clean_image is not None:
            body["image"] = clean_image
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
    stored anyway, and the post is dropped instead of stored when it
    later arrives (see :func:`store_post`).

    Deleting also purges the post from the local store: its row, its
    author's edit entries on it, and its saved, pinned and hidden refs
    are removed (see :func:`_purge_post`).  The signed delete entry
    itself stays, so peers learn of the deletion, the chain stays
    linked, and a second delete still fails as "has been deleted".
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
    elif _delete_entry_exists(conn, target, author):
        # The post itself is already purged: the tombstone is the only
        # trace left, and it still refuses a second delete.
        raise TanError("post has been deleted")
    # The head read and the append must be one atomic step: see
    # _chain_write — two writers racing this window would fork the chain.
    with _chain_write(conn):
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
    existing = _target_post_or_raise(conn, target, "post has been deleted")
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
    # The head read and the append must be one atomic step: see
    # _chain_write — two writers racing this window would fork the chain.
    with _chain_write(conn):
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
    existing = _target_post_or_raise(conn, target, "post has been deleted")
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
    # The head read and the append must be one atomic step: see
    # _chain_write — two writers racing this window would fork the chain.
    with _chain_write(conn):
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


def create_poll(
    conn: sqlite3.Connection,
    identity: RNS.Identity,
    question: str,
    options: Sequence[str],
    *,
    name: str = "anonymous",
    now: Optional[float] = None,
    previous: Optional[str] = None,
    expires_at: Optional[float] = None,
) -> Dict[str, Any]:
    """Create, sign and store a poll: a question with fixed options.

    A poll is a top-level entry like a post: ``kind`` is ``"poll"``,
    ``text`` carries the question and the options ride alongside it,
    signed, so a relay cannot swap them.  Options are fixed at
    creation — changing them would rewrite tallies already cast — so
    there is no edit path for polls.  A poll may carry an *expires_at*
    like a post; votes on an expired poll are rejected, the same as
    replies to one.
    """
    timestamp = round(float(now if now is not None else time.time()), 3)
    author = _author_from_identity(identity)
    clean_options = _clean_poll_options(list(options))
    if expires_at is not None:
        expires_at = _require_timestamp(expires_at, "expires_at")
        if expires_at <= timestamp:
            raise PostError("expires_at must be after created_at")
    clean_text = normalize_message(question)
    # The head read and the append must be one atomic step: see
    # _chain_write — two writers racing this window would fork the chain.
    with _chain_write(conn):
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
            "kind": KIND_POLL,
            "options": clean_options,
        }
        if expires_at is not None:
            body["expires_at"] = expires_at
        digest = hash(body, previous=prev)
        mesg = dict(body)
        mesg["hash"] = digest
        mesg["sig"] = identity.sign(bytes.fromhex(digest)).hex()
        record, _ = store_post(conn, mesg, now=timestamp)
    return record


def create_vote(
    conn: sqlite3.Connection,
    identity: RNS.Identity,
    target: str,
    option: int,
    *,
    name: str = "anonymous",
    now: Optional[float] = None,
    previous: Optional[str] = None,
) -> Dict[str, Any]:
    """Create, sign and store a vote for one option of the poll *target*.

    A vote is a targeted entry like a reaction: ``kind`` is ``"vote"``,
    ``target`` is the poll's hash and the chosen option's index rides
    in the text, so the choice is signed like a reaction's target.
    The vote is appended to the voter's own chain, exactly like a
    post, and is gossipped and synced through the same paths.  Only
    a cached, linked poll that has not expired or been deleted can be
    voted on — a vote is cast on a poll the voter has seen in full —
    and *option* must name one of its options.  One standing vote per
    voter per poll decides the tally: voting again changes your vote,
    the same way the latest like/unlike entry decides a like.  Votes
    received from peers for polls that have not arrived yet are still
    stored through the sync path (:func:`store_post`) and start
    counting once the poll is cached.
    """
    target = _require_hex(target, HASH_HEX_LENGTH, "target")
    timestamp = round(float(now if now is not None else time.time()), 3)
    author = _author_from_identity(identity)
    existing = _target_post_or_raise(conn, target, "poll has been deleted")
    if existing is None:
        raise TanError(f"unknown post: {target!r}")
    if existing.get("kind", KIND_POST) != KIND_POLL:
        raise TanError("only polls can be voted on")
    if not existing.get("linked"):
        raise TanError(
            "poll has not linked yet; sync its chain before voting"
        )
    if _is_deleted(conn, existing):
        raise TanError("poll has been deleted")
    if is_expired(existing, now=timestamp):
        raise TanError("poll has expired")
    if is_poll_closed(conn, existing):
        raise TanError("poll is closed")
    options = _decode_options(existing.get("options")) or []
    if not isinstance(option, int) or isinstance(option, bool):
        raise TanError("vote option must be an option index")
    if not 0 <= option < len(options):
        raise TanError(f"poll has {len(options)} options")
    # The head read and the append must be one atomic step: see
    # _chain_write — two writers racing this window would fork the chain.
    with _chain_write(conn):
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
            "text": str(option),
            "created_at": timestamp,
            "prev": prev,
            "kind": KIND_VOTE,
            "target": target,
        }
        digest = hash(body, previous=prev)
        mesg = dict(body)
        mesg["hash"] = digest
        mesg["sig"] = identity.sign(bytes.fromhex(digest)).hex()
        record, _ = store_post(conn, mesg, now=timestamp)
    return record


def create_close_poll(
    conn: sqlite3.Connection,
    identity: RNS.Identity,
    target: str,
    *,
    name: str = "anonymous",
    now: Optional[float] = None,
    previous: Optional[str] = None,
) -> Dict[str, Any]:
    """Create, sign and store a close entry for the poll *target*.

    The close is appended to the closer's own chain, exactly like a
    delete: ``kind`` is ``close`` and ``target`` is the poll hash, so
    it gossips and syncs through the same paths and every node that
    holds the poll learns it is closed.  Only the poll's author can
    close it — a close from anyone else stays inert, the same
    author-match rule :func:`is_poll_closed` applies — and only when
    the poll is cached locally, linked, and not deleted or expired:
    a close is written to a poll the author has seen in full.  Closing
    freezes the tally: votes cast before the close keep counting, and
    votes after it are rejected (see :func:`create_vote`).  A close is
    final — there is no reopen — and closing an already-closed poll
    raises, like deleting an already-deleted post.
    """
    target = _require_hex(target, HASH_HEX_LENGTH, "target")
    timestamp = round(float(now if now is not None else time.time()), 3)
    author = _author_from_identity(identity)
    existing = _target_post_or_raise(conn, target, "poll has been deleted")
    if existing is None:
        raise TanError(f"unknown post: {target!r}")
    if existing.get("kind", KIND_POST) != KIND_POLL:
        raise TanError("only polls can be closed")
    if not existing.get("linked"):
        raise TanError(
            "poll has not linked yet; sync its chain before closing"
        )
    if _is_deleted(conn, existing):
        raise TanError("poll has been deleted")
    if is_expired(existing, now=timestamp):
        raise TanError("poll has expired")
    if existing["author"] != author:
        raise TanError("cannot close a poll you did not author")
    # The head read and the append must be one atomic step: see
    # _chain_write — two writers racing this window would fork the chain.
    with _chain_write(conn):
        # Checked under the write lock so a concurrent close that
        # committed first is seen here rather than appended twice.
        if is_poll_closed(conn, existing):
            raise TanError("poll is already closed")
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
            "kind": KIND_CLOSE,
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


def _poll_close_cutoffs(
    conn: sqlite3.Connection, targets: Sequence[str]
) -> Dict[str, float]:
    """Map each closed poll hash to the ``created_at`` of its earliest close.

    Only linked close entries written by the poll's own author count,
    the same rule :func:`is_poll_closed` applies. Votes created after
    the cutoff are not part of the frozen tally.
    """
    if not targets:
        return {}
    placeholders = ", ".join("?" for _ in targets)
    rows = conn.execute(
        "SELECT c.target AS target, MIN(c.created_at) AS closed_at "
        "FROM posts c JOIN posts p ON p.hash = c.target AND p.author = c.author "
        "WHERE c.kind = 'close' AND c.linked = 1 AND p.kind = 'poll' "
        f"AND c.target IN ({placeholders}) GROUP BY c.target",
        list(targets),
    ).fetchall()
    return {str(row["target"]): float(row["closed_at"]) for row in rows}


def vote_states(
    conn: sqlite3.Connection, targets: Sequence[str]
) -> Dict[str, Dict[str, int]]:
    """Map each poll hash to ``{voter: option index}`` from linked votes.

    Only linked votes count; an orphaned vote starts counting as soon
    as its chain links up.  The latest vote per voter wins, so voting
    again changes your vote.  A vote whose text is not a plain option
    index never reaches the store (see :func:`_normalize_post`), but a
    signed vote can still name an option the poll does not have: such
    a vote stays stored, yet it never replaces a valid standing vote,
    or it would silently un-vote its author in both tally consumers.
    A vote stored before its poll arrived has an option count that is
    unknown until then, so it is returned as-is and callers check the
    index against the poll's options and ignore the ones that do not
    name a real option.
    """
    clean = []
    for target in targets:
        try:
            clean.append(_require_hex(target, HASH_HEX_LENGTH, "target"))
        except PostError:
            continue
    states: Dict[str, Dict[str, int]] = {target: {} for target in clean}
    if not clean:
        return states
    placeholders = ", ".join("?" for _ in clean)
    option_counts: Dict[str, int] = {}
    for row in conn.execute(
        "SELECT hash, options FROM posts "
        f"WHERE kind = 'poll' AND hash IN ({placeholders})",
        clean,
    ).fetchall():
        options = _decode_options(row["options"])
        if options is not None:
            option_counts[str(row["hash"])] = len(options)
    closed_at = _poll_close_cutoffs(conn, clean)
    rows = conn.execute(
        "SELECT target, author, text, created_at, expires_at FROM posts "
        f"WHERE kind = 'vote' AND linked = 1 AND target IN ({placeholders}) "
        "ORDER BY target, author, seq ASC",
        clean,
    ).fetchall()
    for row in rows:
        text = str(row["text"])
        if not (text.isascii() and text.isdigit()):
            continue
        if is_expired({"expires_at": row["expires_at"]}):
            # An expired vote no longer stands, and it does not
            # supersede an earlier live vote by the same voter.
            continue
        cutoff = closed_at.get(str(row["target"]))
        if cutoff is not None and float(row["created_at"]) > cutoff:
            # Cast after the poll's author closed it: never counts,
            # and does not supersede the voter's pre-close vote.
            continue
        index = int(text)
        option_count = option_counts.get(str(row["target"]))
        if option_count is not None and not 0 <= index < option_count:
            # The poll has arrived and this choice names no option:
            # keep the voter's earlier valid vote standing.
            continue
        # Rows arrive in chain order per voter, so the last write wins.
        states[str(row["target"])][str(row["author"])] = index
    return states


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


def post_reposters(
    conn: sqlite3.Connection, target: str
) -> List[Dict[str, Any]]:
    """Return the reposters of one post, newest repost first.

    Each entry is ``{"author": <hex>, "reposted_at": <created_at>}``.
    Only linked reposts count and one author counts once: rows arrive
    in chain order per reposter, so the last live write wins when a
    chain somehow carries two reposts of the same target. A repost
    its author deleted stops listing, the same way an unlike retires
    a like. A target with no reposts (or no stored post at all) yields
    an empty list.
    """
    clean = _require_hex(target, HASH_HEX_LENGTH, "target")
    rows = conn.execute(
        "SELECT hash, author, created_at FROM posts "
        "WHERE kind = 'repost' AND linked = 1 AND target = ? "
        "ORDER BY author, seq ASC",
        (clean,),
    ).fetchall()
    latest: Dict[str, float] = {}
    for row in rows:
        if _is_deleted(
            conn, {"hash": str(row["hash"]), "author": str(row["author"])}
        ):
            # A deleted repost never lists, but it does not retire an
            # earlier live repost by the same author either.
            continue
        latest[str(row["author"])] = float(row["created_at"])
    reposters = [
        {"author": author, "reposted_at": reposted_at}
        for author, reposted_at in latest.items()
    ]
    reposters.sort(key=lambda entry: entry["reposted_at"], reverse=True)
    return reposters


def post_quoters(
    conn: sqlite3.Connection, target: str
) -> List[Dict[str, Any]]:
    """Return the standing quotes of one post, newest first.

    Each entry carries the quote itself — ``{"author": <hex>,
    "name": ..., "hash": ..., "text": ..., "quoted_at":
    <created_at>}`` — so a reader can see what was said about the
    post, not just who said it. Only linked quotes count, and a quote
    its author deleted stops listing. Unlike likes and reposts, one
    author may quote the same post more than once, and each standing
    quote is listed: the comments differ, so collapsing them would
    hide content. An expired quote stops listing, and an edited quote
    carries its latest text. A target with no quotes (or no stored
    post at all) yields an empty list.
    """
    clean = _require_hex(target, HASH_HEX_LENGTH, "target")
    rows = conn.execute(
        "SELECT hash, author, name, text, created_at, expires_at FROM posts "
        "WHERE kind = 'quote' AND linked = 1 AND target = ? "
        "ORDER BY created_at DESC, seq DESC",
        (clean,),
    ).fetchall()
    quoters = []
    for row in rows:
        if is_expired({"expires_at": row["expires_at"]}):
            continue
        if _is_deleted(
            conn, {"hash": str(row["hash"]), "author": str(row["author"])}
        ):
            continue
        quoters.append(
            {
                "author": str(row["author"]),
                "name": str(row["name"]),
                "hash": str(row["hash"]),
                "text": str(row["text"]),
                "quoted_at": float(row["created_at"]),
            }
        )
    return annotate_edits(conn, quoters)


def poll_results(
    conn: sqlite3.Connection,
    ref: str,
    *,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
) -> Dict[str, Any]:
    """Return the poll *ref* names plus its current tally.

    Mirrors :func:`likers`: *ref* is resolved exactly as
    :func:`resolve_post` does — an unknown, ambiguous, deleted or
    expired reference raises :class:`TanError` — and a reference to
    anything but a poll raises too.  The result carries the resolved
    poll under ``"poll"``, its options under ``"options"``, the vote
    count per option under ``"counts"``, the total number of voters
    under ``"total"``, whether the poll is closed under ``"closed"``,
    and the voters under ``"voters"``: one
    ``{"author": ..., "name": ..., "option": <index>}`` record per
    voter whose standing vote names a real option, newest vote first.
    The name is the one on the voter's latest vote entry for the poll.
    Like :func:`likers`, a poll from a blocked author is refused, and
    an unusable block list hides everything.
    """
    post = resolve_post(conn, ref)
    if not post.get("linked"):
        # Like likers(): an unlinked entry is not shown, so its tally is not either.
        raise TanError(f"poll has not linked yet: {ref!r}")
    excluded = set(_clean_excluded(exclude_authors))
    if exclude_all:
        raise TanError("cannot show poll results: the block-list is unusable")
    if post.get("author") in excluded:
        raise TanError(f"poll is from a blocked author: {ref!r}")
    if post.get("kind") != KIND_POLL:
        raise TanError(f"not a poll: {ref!r}")
    options = _decode_options(post.get("options")) or []
    target = str(post["hash"])
    states = vote_states(conn, [target]).get(target, {})
    voters = [
        (author, index)
        for author, index in states.items()
        if 0 <= index < len(options)
    ]
    records: List[Dict[str, Any]] = []
    if voters:
        # One target-scoped query, no voter list expanded into
        # placeholders: the name lookup cannot hit SQLite's host
        # parameter limit no matter how many voters a poll has.
        rows = conn.execute(
            "SELECT author, name, created_at FROM posts "
            "WHERE kind = 'vote' AND linked = 1 AND target = ? "
            "ORDER BY author, seq ASC",
            (target,),
        ).fetchall()
        cutoff = _poll_close_cutoffs(conn, [target]).get(target)
        names: Dict[str, str] = {}
        voted_at: Dict[str, float] = {}
        for row in rows:
            if cutoff is not None and float(row["created_at"]) > cutoff:
                # A vote cast after the close is not part of the tally.
                continue
            # Rows arrive in chain order per voter, so the last write
            # wins — the name and time on the voter's latest vote.
            names[str(row["author"])] = str(row["name"])
            voted_at[str(row["author"])] = float(row["created_at"])
        records = [
            {
                "author": author,
                "name": names.get(author, ""),
                "option": index,
                "voted_at": voted_at.get(author, 0.0),
            }
            for author, index in voters
        ]
    records.sort(
        key=lambda record: (
            -record["voted_at"],
            record["name"].lower(),
            record["author"],
        )
    )
    counts = [0] * len(options)
    for record in records:
        counts[int(record["option"])] += 1
    return {
        "poll": _canonical_post(post),
        "options": options,
        "counts": counts,
        "total": len(records),
        "closed": is_poll_closed(conn, post),
        "voters": records,
    }


def without_hidden_quotes(
    conn: sqlite3.Connection, quotes: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Drop quotes the viewer's timeline would hide, keeping the rest.

    A quote matching a muted keyword, carrying a muted tag, or hidden
    by the viewer is not disclosed as a quote of its target, the same
    way the timeline hides it. Entries stay stored.
    """
    kept = without_muted_keywords(conn, quotes)
    kept = without_muted_tags(conn, kept)
    return without_hidden_posts(kept, hidden_post_set(conn))


def ancestry_hidden(
    conn: sqlite3.Connection, post: Dict[str, Any], excluded: Set[str]
) -> bool:
    """Whether a reply hangs beneath a deleted or blocked ancestor.

    Walks the same ancestor chain as :func:`thread`, :func:`likers`,
    :func:`reposters` and :func:`quoters`, without raising.  A parent
    whose row is gone still hides the subtree when a linked delete
    entry proves it was purged, and an expired ancestor hides it the
    same way — the thread climb refuses both, so this check must too.
    """
    current = post
    seen_ancestors = {current["hash"]}
    while current.get("kind") == KIND_REPLY and current.get("target"):
        parent = get_post(conn, str(current["target"]))
        if parent is None:
            if _delete_entry_exists(conn, str(current["target"])):
                # The parent was purged when its author deleted it; the
                # tombstone still hides the subtree.
                return True
            break
        if parent.get("kind") not in TIMELINE_KINDS:
            break
        if parent["hash"] in seen_ancestors:
            break
        if _is_deleted(conn, parent) or parent["author"] in excluded:
            return True
        if is_expired(parent):
            return True
        seen_ancestors.add(parent["hash"])
        current = parent
    return False


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
        if parent is None:
            if _delete_entry_exists(conn, str(current_entry["target"])):
                # The parent was purged when its author deleted it; the
                # tombstone still hides the subtree.
                raise TanError(f"post has been deleted: {ref!r}")
            break
        if parent.get("kind") not in TIMELINE_KINDS:
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


def reposters(
    conn: sqlite3.Connection,
    ref: str,
    *,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
) -> Dict[str, Any]:
    """Return the post *ref* names plus who currently reposted it.

    Mirrors :func:`likers`: *ref* is resolved exactly as
    :func:`resolve_post` does, an unlinked entry and a reply beneath
    a deleted ancestor are refused the same way, and a post from a
    blocked author — or a reply beneath one — is refused, so no
    reposters are shown for a post the thread view would hide. The
    result carries the resolved post under ``"post"`` and the current
    reposters under ``"reposters"``: one ``{"author": ...,
    "name": ..., "reposted_at": ...}`` record per author whose latest
    repost of the post still stands, sorted by name then author hash.
    The name is the one on the reposter's latest repost entry for the
    post. Only linked reposts count, like :func:`repost_counts`.
    """
    post = resolve_post(conn, ref)
    if not post.get("linked"):
        # Like thread(): an unlinked entry is not shown, so its
        # reposters are not either.
        raise TanError(f"post has not linked yet: {ref!r}")
    excluded = set(_clean_excluded(exclude_authors))
    if exclude_all:
        raise TanError("cannot show reposters: the block-list is unusable")
    if post.get("author") in excluded:
        raise TanError(f"post is from a blocked author: {ref!r}")
    # Like thread(): a reply beneath a deleted ancestor stays hidden.
    current_entry = post
    seen_ancestors = {current_entry["hash"]}
    while current_entry.get("kind") == KIND_REPLY and current_entry.get("target"):
        parent = get_post(conn, str(current_entry["target"]))
        if parent is None:
            if _delete_entry_exists(conn, str(current_entry["target"])):
                # The parent was purged when its author deleted it; the
                # tombstone still hides the subtree.
                raise TanError(f"post has been deleted: {ref!r}")
            break
        if parent.get("kind") not in TIMELINE_KINDS:
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
    current = [
        entry
        for entry in post_reposters(conn, target)
        if entry["author"] not in excluded
    ]
    records: List[Dict[str, Any]] = []
    if current:
        # One target-scoped query, no author list expanded into
        # placeholders: the name lookup cannot hit SQLite's host
        # parameter limit no matter how many reposters a post has.
        rows = conn.execute(
            "SELECT author, name FROM posts "
            "WHERE kind = 'repost' AND linked = 1 AND target = ? "
            "ORDER BY author, seq ASC",
            (target,),
        ).fetchall()
        names: Dict[str, str] = {}
        for row in rows:
            # Rows arrive in chain order per reposter, so the last write
            # wins — the name on the reposter's latest repost.
            names[str(row["author"])] = str(row["name"])
        records = [
            {
                "author": entry["author"],
                "name": names.get(entry["author"], ""),
                "reposted_at": entry["reposted_at"],
            }
            for entry in current
        ]
    records.sort(key=lambda record: (-record["reposted_at"], record["author"]))
    return {"post": _canonical_post(post), "reposters": records}


def quoters(
    conn: sqlite3.Connection,
    ref: str,
    *,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
) -> Dict[str, Any]:
    """Return the post *ref* names plus who currently quoted it.

    Mirrors :func:`likers`: *ref* is resolved exactly as
    :func:`resolve_post` does, an unlinked entry and a reply beneath
    a deleted ancestor are refused the same way, and a post from a
    blocked author — or a reply beneath one — is refused, so no
    quoters are shown for a post the thread view would hide. The
    result carries the resolved post under ``"post"`` and the standing
    quotes under ``"quoters"``: one ``{"author": ..., "name": ...,
    "hash": ..., "text": ..., "quoted_at": ...}`` record per standing
    quote, newest first. The name and text come from the quote entry
    itself. Only linked quotes count, like :func:`quote_counts`.
    """
    post = resolve_post(conn, ref)
    if not post.get("linked"):
        # Like thread(): an unlinked entry is not shown, so its
        # quoters are not either.
        raise TanError(f"post has not linked yet: {ref!r}")
    excluded = set(_clean_excluded(exclude_authors))
    if exclude_all:
        raise TanError("cannot show quoters: the block-list is unusable")
    if post.get("author") in excluded:
        raise TanError(f"post is from a blocked author: {ref!r}")
    # Like thread(): a reply beneath a deleted ancestor stays hidden.
    current_entry = post
    seen_ancestors = {current_entry["hash"]}
    while current_entry.get("kind") == KIND_REPLY and current_entry.get("target"):
        parent = get_post(conn, str(current_entry["target"]))
        if parent is None:
            if _delete_entry_exists(conn, str(current_entry["target"])):
                # The parent was purged when its author deleted it; the
                # tombstone still hides the subtree.
                raise TanError(f"post has been deleted: {ref!r}")
            break
        if parent.get("kind") not in TIMELINE_KINDS:
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
    current = without_hidden_quotes(
        conn,
        [
            entry
            for entry in post_quoters(conn, target)
            if entry["author"] not in excluded
        ],
    )
    records: List[Dict[str, Any]] = []
    if current:
        # One target-scoped query, no author list expanded into
        # placeholders: the name lookup cannot hit SQLite's host
        # parameter limit no matter how many quoters a post has.
        rows = conn.execute(
            "SELECT author, name FROM posts "
            "WHERE kind = 'quote' AND linked = 1 AND target = ? "
            "ORDER BY author, seq ASC",
            (target,),
        ).fetchall()
        names: Dict[str, str] = {}
        for row in rows:
            # Rows arrive in chain order per quoter, so the last write
            # wins — the name on the quoter's latest quote.
            names[str(row["author"])] = str(row["name"])
        records = [
            {
                "author": entry["author"],
                "name": names.get(entry["author"], entry["name"]),
                "hash": entry["hash"],
                "text": entry["text"],
                "quoted_at": entry["quoted_at"],
            }
            for entry in current
        ]
    return {"post": _canonical_post(post), "quoters": records}


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
    Poll tallies ride this same step — every surface that shows likes
    also shows polls — so :func:`annotate_polls` runs here rather
    than in each caller.
    """
    counts = like_counts(conn, [post["hash"] for post in posts], viewer=viewer)
    for post in posts:
        info = counts.get(post["hash"], {"likes": 0, "liked": False})
        post["likes"] = info["likes"]
        post["liked"] = info["liked"]
        post["own"] = viewer is not None and post["author"] == viewer
    return annotate_polls(conn, posts, viewer=viewer)


def annotate_polls(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
    *,
    viewer: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Attach a ``poll`` tally to poll entries, in place.

    ``poll`` carries the options, the vote count per option in option
    order, the total number of voters, whether the poll is closed, and
    — when *viewer* is given — the index of the viewer's own standing
    vote (or ``None``).  Votes naming an option the poll does not have
    are ignored: a poll's options never change once published, so a
    mismatch means a bad entry.  Non-poll entries are untouched.
    """
    polls = [post for post in posts if post.get("kind") == KIND_POLL]
    if not polls:
        return posts
    states = vote_states(conn, [str(post["hash"]) for post in polls])
    for post in polls:
        options = _decode_options(post.get("options")) or []
        voters = states.get(str(post["hash"]), {})
        counts = [0] * len(options)
        total = 0
        voted = None
        for author, index in voters.items():
            if 0 <= index < len(counts):
                counts[index] += 1
                total += 1
                if viewer is not None and author == viewer:
                    voted = index
        post["poll"] = {
            "options": options,
            "counts": counts,
            "total": total,
            "closed": is_poll_closed(conn, post),
            "voted": voted,
        }
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


def annotate_pinned(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Attach a ``pinned`` flag to post dicts, in place.

    Mirrors :func:`annotate_saved`: a display surface uses it to show
    pin state on each card without a second request.
    """
    hashes = [str(post["hash"]) for post in posts if post.get("hash")]
    pinned: Set[str] = set()
    if hashes:
        placeholders = ", ".join("?" for _ in hashes)
        rows = conn.execute(
            f"SELECT hash FROM pinned_posts WHERE hash IN ({placeholders})",
            hashes,
        ).fetchall()
        pinned = {str(row["hash"]) for row in rows}
    for post in posts:
        post["pinned"] = str(post.get("hash")) in pinned
    return posts


def _purged_previews(
    conn: sqlite3.Connection, targets: Sequence[str]
) -> Dict[str, Dict[str, Any]]:
    """Deleted-parent previews for targets whose post row is gone.

    A post deleted through :func:`create_delete` is purged from the
    store, so a reply, quote or repost pointing at it finds no parent
    row — but the purge record still names the post's author and the
    name they used on it.  Returns ``{target: preview}`` with the
    text blanked and ``deleted`` set, so the thread stays readable
    without resurfacing removed content.  Targets with no purge
    record are simply absent: the parent has not arrived yet, or the
    only delete naming it is unproven — a foreign delete for a hash
    this node never held must not supply a parent identity.  A node
    that never held the post shows no preview for it, the same as an
    uncached parent.
    """
    missing = [str(target) for target in targets]
    if not missing:
        return {}
    # Only a locally verified purge names the author: a restored
    # record's author is asserted by the document that carried it,
    # and a peer could claim any hash as its own — so a restored
    # record never supplies a parent identity here.
    placeholders = ", ".join("?" for _ in missing)
    rows = conn.execute(
        "SELECT hash, author, name FROM purged_posts "
        f"WHERE hash IN ({placeholders}) AND restored = 0",
        missing,
    ).fetchall()
    return {
        str(row["hash"]): {
            "hash": str(row["hash"]),
            "author": str(row["author"]),
            "name": str(row["name"]),
            "text": "",
            "deleted": True,
        }
        for row in rows
    }


def annotate_replies(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
    *,
    muted_keywords: Optional[Sequence[str]] = None,
    exclude_authors: Optional[Set[str]] = None,
    blocked_authors: Optional[Set[str]] = None,
    hidden_posts: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    """Attach a ``reply_to`` parent summary to reply entries, in place.

    The summary carries the parent's hash, author, name and text when
    the parent is cached locally, and is ``None`` when the parent has
    not arrived yet — a reply is still shown on its own.  A deleted
    parent is reported as deleted with its text blanked, so the thread
    stays readable without resurfacing removed content.  An expired
    parent is reported the same way, with its text blanked and
    ``expired`` set.  A parent matching a muted keyword (see
    :func:`muted_keywords`) is reported the same way, with its text
    blanked and ``muted`` set, so the muted content does not leak
    through the preview.  A parent in *hidden_posts* is reported the
    same way, with its text blanked and ``hidden`` set, so hiding a
    post never leaks its words through a reply snippet.  A parent whose
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
    purged = _purged_previews(
        conn, [target for target in targets if target not in parents]
    )
    muted_pattern = _muted_keyword_pattern(list(muted_keywords or []))
    for post in posts:
        if post.get("kind") != KIND_REPLY:
            continue
        parent = parents.get(str(post.get("target") or ""))
        if parent is None:
            preview = purged.get(str(post.get("target") or ""))
            if preview is None:
                post["reply_to"] = None
                continue
            if exclude_authors and preview["author"] in exclude_authors:
                post["reply_to"] = None
                continue
            if blocked_authors and preview["author"] in blocked_authors:
                # Redact identity too, as for a stored blocked parent.
                post["reply_to"] = {
                    **preview,
                    "author": "",
                    "name": "",
                    "blocked": True,
                }
                continue
            # The normal preview carries the edited flag; a purged post
            # has no edit to show (its edits were purged with it).
            post["reply_to"] = {**preview, "edited": False}
            continue
        is_deleted = (parent["hash"], parent["author"]) in deleted
        is_expired_parent = is_expired(parent)
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
        is_hidden = hidden_posts is not None and parent["hash"] in hidden_posts
        preview = {
            "hash": parent["hash"],
            "author": parent["author"],
            "name": parent["name"],
            "kind": parent["kind"],
            "text": (
                ""
                if is_deleted or is_muted or is_expired_parent or is_hidden
                else parent["text"]
            ),
            "deleted": is_deleted,
            "edited": bool(parent.get("edited")),
        }
        if is_muted:
            # Only present when muted, so unmuted previews keep their
            # established shape.
            preview["muted"] = True
        if is_hidden:
            # Only present when hidden, so visible previews keep their
            # established shape.
            preview["hidden"] = True
        if is_expired_parent:
            # Only present when expired, so live previews keep their
            # established shape.
            preview["expired"] = True
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
    muted_tags: Optional[Sequence[str]] = None,
    exclude_authors: Optional[Set[str]] = None,
    blocked_authors: Optional[Set[str]] = None,
    hidden_posts: Optional[Set[str]] = None,
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
    original carrying a muted tag (see :func:`mute_tag`) is reported
    the same way.  An original in *hidden_posts* is reported the same way, with its text
    blanked and ``hidden`` set, so hiding a post never leaks its words
    through a quote preview.  An
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
    purged = _purged_previews(
        conn, [target for target in targets if target not in parents]
    )
    muted_pattern = _muted_keyword_pattern(list(muted_keywords or []))
    for post in posts:
        if post.get("kind") != KIND_QUOTE:
            continue
        parent = parents.get(str(post.get("target") or ""))
        if parent is None:
            preview = purged.get(str(post.get("target") or ""))
            if preview is None:
                post["quoted"] = None
                continue
            if exclude_authors and preview["author"] in exclude_authors:
                post["quoted"] = None
                continue
            if blocked_authors and preview["author"] in blocked_authors:
                # Redact identity too, as for a stored blocked original.
                post["quoted"] = {
                    **preview,
                    "author": "",
                    "name": "",
                    "blocked": True,
                }
                continue
            post["quoted"] = dict(preview)
            continue
        if exclude_authors and parent["author"] in exclude_authors:
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
                "kind": parent["kind"],
                "text": "",
                "deleted": is_deleted,
                "blocked": True,
            }
            continue
        is_muted = _text_matches_muted_keywords(
            str(parent.get("text") or ""), muted_pattern
        ) or bool(
            set(extract_tags(str(parent.get("text") or "")))
            & set(muted_tags or [])
        )
        is_hidden = hidden_posts is not None and parent["hash"] in hidden_posts
        is_expired_parent = is_expired(parent)
        summary = {
            "hash": parent["hash"],
            "author": parent["author"],
            "name": parent["name"],
            "kind": parent["kind"],
            "text": (
                ""
                if is_deleted or is_muted or is_expired_parent or is_hidden
                else parent["text"]
            ),
            "deleted": is_deleted,
        }
        if is_muted:
            # Only present when muted, so unmuted previews keep their
            # established shape.
            summary["muted"] = True
        if is_hidden:
            # Only present when hidden, so visible previews keep their
            # established shape.
            summary["hidden"] = True
        if is_expired_parent:
            # Only present when expired, so live previews keep their shape.
            summary["expired"] = True
        post["quoted"] = summary
    return posts


def annotate_reposts(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
    *,
    muted_keywords: Optional[Sequence[str]] = None,
    exclude_authors: Optional[Set[str]] = None,
    blocked_authors: Optional[Set[str]] = None,
    hidden_posts: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    """Attach a ``repost_of`` original-post summary to repost entries, in place.

    The summary carries the original's hash, author, name and text
    when it is cached locally, and is ``None`` when it has not arrived
    yet — a repost is still shown on its own.  A deleted original is
    reported as deleted with its text blanked, so the timeline stays
    readable without resurfacing removed content.  An expired original
    is reported the same way, with its text blanked and an ``expired``
    flag, so a live repost cannot resurface disappeared content.  Top-level posts and
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
    way a reply is.  A repost whose original is in *hidden_posts* is
    dropped the same way.  Without the filter arguments nothing is dropped,
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
    purged = _purged_previews(
        conn, [target for target in targets if target not in originals]
    )
    muted_pattern = _muted_keyword_pattern(list(muted_keywords or []))
    kept: List[Dict[str, Any]] = []
    for post in posts:
        if post.get("kind") != KIND_REPOST:
            kept.append(post)
            continue
        original = originals.get(str(post.get("target") or ""))
        if original is None:
            preview = purged.get(str(post.get("target") or ""))
            if preview is None:
                post["repost_of"] = None
                kept.append(post)
                continue
            # A repost of a blocked or muted original is dropped, the
            # same as for a stored original.
            if blocked_authors and preview["author"] in blocked_authors:
                continue
            if exclude_authors and preview["author"] in exclude_authors:
                continue
            post["repost_of"] = dict(preview)
            kept.append(post)
            continue
        if blocked_authors and original["author"] in blocked_authors:
            continue
        if exclude_authors and original["author"] in exclude_authors:
            continue
        if hidden_posts and original["hash"] in hidden_posts:
            continue
        if _text_matches_muted_keywords(
            str(original.get("text") or ""), muted_pattern
        ):
            continue
        is_deleted = (original["hash"], original["author"]) in deleted
        is_expired_original = is_expired(original)
        post["repost_of"] = {
            "hash": original["hash"],
            "author": original["author"],
            "name": original["name"],
            "kind": original["kind"],
            "text": "" if is_deleted or is_expired_original else original["text"],
            "deleted": is_deleted,
        }
        if is_expired_original:
            # Only present when expired, so live previews keep their shape.
            post["repost_of"]["expired"] = True
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
    """Hashes of linked entries on *author*'s chain, from the retained head back.

    Walks back from the head through ``prev``.  A purged post breaks
    the walk — its row is gone — so when the walk reaches a missing
    hash covered by a linked purge record from the same author, it
    crosses the gap by continuing from the recorded ``prev``: the
    purge record is what lets the walk follow the retained head's
    actual path past a deletion.  A gap with no linked purge record
    ends the walk — entries before it are not on the retained chain,
    and a record from an entry that never linked proves no chain
    position to cross on.  A bare delete entry never bridges a gap:
    it may target a hash that never existed.  Fork siblings are never
    swept in: only entries on the head's own prev path are collected.
    """
    head_row = conn.execute(
        "SELECT hash FROM heads WHERE author = ?", (author,)
    ).fetchone()
    if head_row is None:
        return set()
    chain: Set[str] = set()
    seen: Set[str] = set()
    current = str(head_row["hash"])
    while current and current not in seen:
        seen.add(current)
        row = conn.execute(
            "SELECT hash, prev, linked FROM posts "
            "WHERE hash = ? AND author = ?",
            (current, author),
        ).fetchone()
        if row is None:
            # The entry is gone: cross the gap through its purge
            # record, or stop — there is no retained chain past an
            # unproven gap.
            record = _purged_post(conn, current, author, linked_only=True)
            if record is None:
                break
            current = str(record["prev"] or "")
            continue
        if not int(row["linked"]):
            break
        chain.add(str(row["hash"]))
        prev = str(row["prev"] or "")
        if not prev:
            break
        current = prev
    return chain


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


def _record_purged(
    conn: sqlite3.Connection,
    hash: str,
    author: str,
    name: str,
    prev: str,
    *,
    linked: bool = False,
    target: str = "",
    restored: bool = False,
) -> None:
    """Record author-bound evidence that (``hash``, ``author``) was purged.

    A record means this node held a real, signed entry with this hash
    from this author and removed it because that author deleted it —
    either by purging a stored row (:func:`_purge_post`) or by dropping
    a re-offered entry at the door (:func:`store_post`'s guard).  The
    record is what later gap-bridging trusts: a signed delete alone
    proves nothing about a hash this node never held, since anyone can
    sign a delete for an arbitrary hash.  ``name`` is the name the
    author used on the entry, so deleted-parent previews can still
    name them without trusting a delete entry; ``prev`` is the entry's
    parent hash, so the chain walk can cross the gap.  ``target`` is
    the post a purged *edit* belonged to (empty for the post itself):
    a delete targets the post, never the edit, so the target is what
    associates an edit's record with the signed delete that backs it.
    A hash is a content hash, so one record per hash is enough — the
    author is determined by it.

    ``linked`` records whether the entry had an established chain
    position when it was purged — a stored row's linked flag, or, for
    an entry dropped at the door, whether its parent was linked (the
    position it would have taken).  Only a linked record proves a
    chain position: an edit stored while its own predecessor was
    missing, then purged, never acquired one, so a later child of it
    must keep waiting rather than link on the record.  Deletion and
    preview lookups ignore ``linked`` — a purged entry was deleted
    whatever its chain position — but gap-bridging (the chain walk,
    the missing-parent branch, the audit) consults linked records
    only; see :func:`_purged_post`.

    Provenance advances but never retreats: a record first made
    while its chain position was unproven (``linked=False``) upgrades
    to linked when the position is later established — a re-offered
    entry whose parent has since linked, or an imported record whose
    predecessor has arrived — but a linked record is never downgraded
    by a later unlinked claim for the same hash.  A verified re-offer
    also fills an empty ``target`` left by the migration (a record
    made before targets were carried), without replacing established
    metadata with conflicting values.

    ``restored`` marks a record that arrived in an export document
    or a sync page rather than from an entry this node held.  Such a
    record is gap proof only — it never supplies an identity: the
    previews and the author-less deletion lookup ignore it, because
    its author is asserted by the document, not proven by a held
    entry.  A restored claim never displaces another author's
    record, and a verified claim for the same hash displaces a
    restored one: the held entry's authorship is proven by its
    signature, so the verified record wins and the restored claim
    is dropped rather than squatting the hash.
    """
    existing = conn.execute(
        "SELECT author, restored FROM purged_posts WHERE hash = ?", (hash,)
    ).fetchone()
    if existing is not None and str(existing["author"]) != author:
        if restored:
            # A restored claim never displaces another author's
            # record for the same hash — the first claim stands
            # until a verified entry proves the authorship.
            return
        # A verified claim displaces a restored one: the entry was
        # held here and its signature proves its author.
        conn.execute("DELETE FROM purged_posts WHERE hash = ?", (hash,))
        existing = None
    if (
        existing is not None
        and int(existing["restored"])
        and not restored
    ):
        # A verified claim upgrades a restored record to verified,
        # with the held entry's metadata as ground truth.
        conn.execute(
            "UPDATE purged_posts SET name = ?, prev = ?, target = ?, "
            "linked = MAX(linked, ?), restored = 0 WHERE hash = ?",
            (name, prev, target, 1 if linked else 0, hash),
        )
        return
    conn.execute(
        "INSERT INTO purged_posts(hash, author, name, prev, linked, target, restored) "
        "VALUES(?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(hash) DO UPDATE SET "
        "linked = MAX(purged_posts.linked, excluded.linked), "
        "target = CASE WHEN purged_posts.target = '' AND excluded.restored = 0 "
        "THEN excluded.target ELSE purged_posts.target END",
        (hash, author, name, prev, 1 if linked else 0, target, 1 if restored else 0),
    )


def _purged_post(
    conn: sqlite3.Connection,
    target: str,
    author: Optional[str] = None,
    *,
    linked_only: bool = False,
) -> Optional[Dict[str, Any]]:
    """Return the purge record for *target*, or ``None``.

    With *author* given, only a record from that author counts; without
    it, any author counts.  A record is proof this node held (or was
    offered and dropped) a real entry with this hash and removed it on
    its author's delete.  A delete entry alone, linked or not, never
    counts: it may target a hash that never existed.

    With *linked_only*, only a record whose entry had an established
    chain position counts — the gap-bridging callers (the chain walk,
    the missing-parent branch, the audit) use this, since a record
    from an entry that never linked proves no chain position.  The
    deletion and preview lookups use the record either way: a purged
    entry was deleted whatever its chain position.
    """
    if author is not None:
        row = conn.execute(
            "SELECT hash, author, name, prev, linked, target, restored "
            "FROM purged_posts "
            "WHERE hash = ? AND author = ?"
            + (" AND linked = 1" if linked_only else "")
            + " LIMIT 1",
            (target, author),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT hash, author, name, prev, linked, target, restored "
            "FROM purged_posts "
            "WHERE hash = ?" + (" AND linked = 1" if linked_only else "")
            + " LIMIT 1",
            (target,),
        ).fetchone()
    return dict(row) if row is not None else None


def _delete_entry_exists(
    conn: sqlite3.Connection, target: str, author: Optional[str] = None
) -> bool:
    """Whether a linked delete entry targets *target*.

    With *author* given, only a delete from that author counts — the
    authorship check :func:`create_delete` needs when the target post
    itself has already been purged.  Without it, only a proven purge
    counts (see :func:`_purged_post`): callers that only know the
    target hash (a thread climb past a missing parent,
    :func:`resolve_post` on a hash with no row) must not treat a bare
    tombstone as the deletion signal, since a delete from the wrong
    author — or for a hash that never existed — can only mislead them
    about a post that is not stored here.  A proven purge cannot hide
    a post whose row exists, where :func:`_is_deleted` still applies
    the author match.  A restored purge record never counts here:
    its author is asserted by the document that carried it, not
    proven by an entry this node held, so it cannot supply the
    identity these callers need.
    """
    if author is not None:
        row = conn.execute(
            "SELECT 1 FROM posts d WHERE d.kind = 'delete' AND d.linked = 1 "
            "AND d.target = ? AND d.author = ? LIMIT 1",
            (target, author),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT 1 FROM purged_posts WHERE hash = ? AND restored = 0 LIMIT 1",
            (target,),
        ).fetchone()
    return row is not None


def _purge_post(conn: sqlite3.Connection, author: str, target: str) -> List[str]:
    """Drop a deleted post's row — and its content-carrying traces — from the store.

    Removes the target row itself when it is a timeline post by
    *author*, the author's own edit entries on it (an edit carries
    the post's text, so leaving one would leave the content behind),
    and the saved, pinned and hidden refs pointing at it (they exist
    to surface the post; with the post gone they surface nothing).
    The signed delete entry itself is untouched: it is the tombstone
    peers sync, and the chain stays linked through it.  A delete from
    anyone but the post's author purges nothing.  Returns the hashes
    of the rows removed — the target first, then its edit rows — so
    the caller can re-link the entries created after each of them.
    An empty list means nothing was purged.  The caller owns the
    transaction: this runs inside :func:`store_post`'s insert or
    :func:`_promote_children`, so it commits with the entry that
    authorised the purge.

    Every removed row is recorded in ``purged_posts`` first (see
    :func:`_record_purged`): the row's hash is gone, but the chain
    still needs to know it existed and was purged — later entries
    point at it as their parent, and the audit must tell a genuine
    purge gap from an invented one.  An edit row cannot stay with its
    text blanked instead: the text is covered by the entry's hash, so
    blanking it would fail verification — the row goes, and its hash
    is recorded so the gap it leaves is recognised.
    """
    row = conn.execute(
        "SELECT author, kind, name, prev, linked FROM posts WHERE hash = ?",
        (target,),
    ).fetchone()
    if row is None:
        return []
    if str(row["author"]) != author or str(row["kind"]) not in TIMELINE_KINDS:
        return []
    _record_purged(
        conn,
        target,
        author,
        str(row["name"] or ""),
        str(row["prev"] or ""),
        linked=_int_or_none(row["linked"]) == 1,
    )
    conn.execute("DELETE FROM posts WHERE hash = ?", (target,))
    edits = conn.execute(
        "SELECT hash, name, prev, linked, target FROM posts "
        "WHERE kind = 'edit' AND target = ? AND author = ?",
        (target, author),
    ).fetchall()
    purged = [target]
    for edit in edits:
        _record_purged(
            conn,
            str(edit["hash"]),
            author,
            str(edit["name"] or ""),
            str(edit["prev"] or ""),
            linked=_int_or_none(edit["linked"]) == 1,
            target=str(edit["target"] or ""),
        )
        purged.append(str(edit["hash"]))
    conn.execute(
        "DELETE FROM posts WHERE kind = 'edit' AND target = ? AND author = ?",
        (target, author),
    )
    for table in ("saved_posts", "pinned_posts", "hidden_posts"):
        conn.execute(f"DELETE FROM {table} WHERE hash = ?", (target,))
    return purged


def _link_children_of_purged(
    conn: sqlite3.Connection, author: str, purged_hash: str
) -> None:
    """Link orphaned children of a purged post once its delete is known.

    Entries created after a deleted post point at it as their parent.
    On a store that never held the post — a fresh sync or import after
    the delete — they arrive unlinked and would stay that way forever,
    since the parent never comes back.  Once the delete entry is
    stored, these children link up in chain order, taking the next
    sequence each, so they show and count again.  Their own children
    are promoted in turn, and a child that is itself a delete applies
    the same way.  This runs on a delete's arrival, linked or not:
    it is what lets a fresh peer sync a chain past a deletion, and
    what lets an unlinked delete take its own chain position (its
    parent is the target).  Whether the gap is genuine is judged
    elsewhere, on purge evidence — see :func:`_chain_hashes` and
    :func:`verify_store` — not here, with one exception: when a purge
    record exists for the purged hash and it says the row was never
    linked, the children keep waiting — the purged entry never had a
    chain position to hand on, and a child that arrived before the
    drop must not gain one the child arriving after it is denied.
    With no record at all (the row was never held here), the re-link
    still runs: the delete entry is the only proof this store has,
    and the walk and the audit judge that gap on their own terms.
    """
    record = _purged_post(conn, purged_hash, author)
    if record is not None and not int(record.get("linked") or 0):
        return
    children = conn.execute(
        f"SELECT {', '.join(_POST_COLUMNS)} FROM posts "
        "WHERE author = ? AND prev = ? AND linked = 0 ORDER BY created_at ASC",
        (author, purged_hash),
    ).fetchall()
    for child in children:
        seq = max_seq(conn, author) + 1
        conn.execute(
            "UPDATE posts SET linked = 1, seq = ? WHERE hash = ?",
            (seq, str(child["hash"])),
        )
        _advance_head(
            conn, author, str(child["hash"]), seq, float(child["created_at"])
        )
        _promote_children(conn, author, str(child["hash"]), seq)
        if str(child["kind"]) == KIND_DELETE:
            _apply_delete(
                conn, author, str(child["target"] or ""), linked=True
            )


def _apply_delete(
    conn: sqlite3.Connection, author: str, target: str, *, linked: bool
) -> None:
    """Apply a stored delete entry: purge its target, re-link orphans.

    The purge removes the target post's row and its content-carrying
    traces, but only once the delete itself is linked on the chain:
    an unlinked delete must not remove a cached row yet, or the head
    would point at a missing row and a re-offered post could come
    back before the delete links.  The re-link runs either way: it
    attaches the entries created after the deleted post — including
    the delete itself, whose parent is the target — which would
    otherwise stay orphaned on a store that never held the parent.
    That re-link is also what lets an unlinked delete take its chain
    position, so a post arriving after its delete is dropped by the
    guard instead of being stored and then purged mid-write.  A
    delete for a hash this node never held therefore still links its
    waiting children; the audit and the chain walk judge the gap on
    purge evidence instead (see :func:`verify_store` and
    :func:`_chain_hashes`), so an invented gap cannot leak into
    displayed text.  The purge also removes the author's edit rows on
    the post, and entries created after one of those edits point at
    the edit as their parent — so the re-link covers each purged
    edit's children as well, or a later entry would stay orphaned
    behind a gap the purge created.  Idempotent: a second application
    finds nothing left to purge or link.
    """
    purged: List[str] = []
    if linked:
        purged = _purge_post(conn, author, target)
    _link_children_of_purged(conn, author, target)
    for edit_hash in purged:
        if edit_hash != target:
            _link_children_of_purged(conn, author, edit_hash)


def _target_post_or_raise(
    conn: sqlite3.Connection, target: str, deleted_message: str
) -> Optional[Dict[str, Any]]:
    """Fetch the target post row, or raise the right error for a missing one.

    A post its author deleted is purged from the store, so a missing
    row still reports deleted — with the caller's message — when a
    linked delete entry targets it.  Otherwise the row is simply
    unknown, and the caller reports that.
    """
    existing = get_post(conn, target)
    if existing is None and _delete_entry_exists(conn, target):
        raise TanError(deleted_message)
    return existing


def deleted_targets(conn: sqlite3.Connection) -> Set[str]:
    """Hashes of posts hidden by a delete entry from their own author.

    A post is deleted when a linked ``delete`` entry exists whose author
    is the post's author.  Deletes from anyone else are inert, and a
    delete that arrives before its post applies as soon as the post is
    stored.  Posts deleted through :func:`create_delete` are purged
    from the store (see :func:`_purge_post`), so they no longer appear
    here — this still covers posts stored before the purge change, and
    any retained row a linked delete hides.
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


def is_poll_closed(conn: sqlite3.Connection, post: Dict[str, Any]) -> bool:
    """Whether *post* is closed by a close entry from its own author."""
    row = conn.execute(
        "SELECT 1 FROM posts c WHERE c.kind = 'close' AND c.linked = 1 "
        "AND c.target = ? AND c.author = ? LIMIT 1",
        (post["hash"], post["author"]),
    ).fetchone()
    return row is not None


def is_expired(post: Dict[str, Any], *, now: Optional[float] = None) -> bool:
    """Whether *post*'s expiry has passed.

    A post without an expiry never expires.  An expired post stays in
    the store — the chain must stay verifiable and sync must keep
    carrying it — it is only hidden from display surfaces.  (A deleted
    post is different: deleting purges its row, see
    :func:`_purge_post`.)
    """
    expires_at = post.get("expires_at")
    if expires_at is None:
        return False
    try:
        deadline = float(expires_at)
    except (TypeError, ValueError):
        return False
    at = float(now if now is not None else time.time())
    return deadline <= at


def _without_expired(
    posts: List[Dict[str, Any]], *, now: Optional[float] = None
) -> List[Dict[str, Any]]:
    """Drop expired posts from a display list, keeping the rest in order."""
    return [post for post in posts if not is_expired(post, now=now)]


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
    if not ruleset.displays(KIND_VOTE):
        for post in shown:
            post.pop("poll", None)
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
    callers can tell the two apart.  A deleted post's row is purged
    from the store, so a full hash with no row still reports deleted
    when a linked delete entry targets it — the tombstone is the only
    trace left, and it is authoritative.
    """
    if not isinstance(ref, str) or not ref.strip():
        raise TanError("post reference must not be empty")
    value = ref.strip().lower()
    if len(value) == HASH_HEX_LENGTH:
        post = get_post(conn, value)
        if post is not None and post.get("kind", KIND_POST) in TIMELINE_KINDS:
            if _is_deleted(conn, post):
                raise TanError(f"post has been deleted: {ref!r}")
            if is_expired(post):
                raise TanError(f"post has expired: {ref!r}")
            return post
        if _delete_entry_exists(conn, value):
            raise TanError(f"post has been deleted: {ref!r}")
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
                if is_expired(post):
                    raise TanError(f"post has expired: {ref!r}")
                return post
        if len(rows) > 1:
            raise TanError(f"{len(rows)} posts match prefix {value!r}")
    raise TanError(f"unknown post: {ref!r}")


def resolve_share_post(conn: sqlite3.Connection, ref: str) -> Dict[str, Any]:
    """Resolve a post reference to the post a share sends.

    A repost carries no text of its own, so sharing one shares its
    original instead — the share attributes the original's author, not
    the reposter.  Follows the repost chain defensively (creation
    refuses a repost of a repost, but an old chain is followed rather
    than trusted).
    """
    post = resolve_post(conn, ref)
    if not post.get("linked", True):
        raise TanError(f"post is not linked yet: {ref!r}")
    hops = 0
    while post.get("kind") == KIND_REPOST:
        target = post.get("target") or ""
        if not target:
            raise TanError(f"repost has no original: {ref!r}")
        hops += 1
        if hops > 8:
            raise TanError(f"repost chain too deep: {ref!r}")
        post = resolve_post(conn, target)
        if not post.get("linked", True):
            raise TanError(f"post is not linked yet: {ref!r}")
    # Share the author's latest edit, matching what Tan views display.
    return annotate_edits(conn, [post])[0]


def share_text(post: Dict[str, Any], label: str) -> str:
    """Render a stored post as the text of a DM or group message sharing it.

    The shared copy is an ordinary message — no protocol change — so the
    recipient reads the post inline even when their node never saw it,
    and the full hash lets them open the thread (``tau tan thread``) or
    vote (``tau tan vote``) once they have it.  A poll carries its
    options with the question.

    The body is truncated to fit ``MAX_MESSAGE_CHARS``: a post may use
    the whole budget itself, so the attribution header and hash footer
    would otherwise push the share over the DM limit.  The hash stays
    intact, so nothing is lost — the recipient can fetch the full text.
    """
    kind = post.get("kind") or KIND_POST
    header = (
        f"Shared Tan poll by {label}:"
        if kind == KIND_POLL
        else f"Shared Tan post by {label}:"
    )
    options = ""
    if kind == KIND_POLL:
        choices = _decode_options(post.get("options"))
        options = "".join(f"\n{i + 1}. {choice}" for i, choice in enumerate(choices))
    footer = f"\n(tan {post['hash']})"
    fixed = len(header) + 1 + len(options) + len(footer)
    body = str(post.get("text") or "")
    budget = MAX_MESSAGE_CHARS - fixed
    if len(body) > budget:
        body = body[: max(0, budget - 1)] + "…"
    return f"{header}\n{body}{options}{footer}"


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
    before_hash: Optional[str] = None,
    posts_only: bool = False,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
) -> List[Dict[str, Any]]:
    """Read cached entries in chain order or reverse chronological order.

    With *before_hash*, reverse chronological paging resumes after the
    entry ``(before, before_hash)`` in feed order, so entries sharing a
    timestamp are not skipped.

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
        expiry_params: List[Any] = []
        if posts_only:
            # Display only: expired entries are excluded in SQL, before
            # LIMIT, so a page is never short because of them.  Sync
            # callers keep them so chains stay complete.
            kind_filter = (
                f" AND kind IN ({TIMELINE_KINDS_SQL}) AND {_NOT_DELETED}"
                " AND (expires_at IS NULL OR expires_at > ?)"
            )
            expiry_params = [time.time()]
        rows = conn.execute(
            f"SELECT {', '.join(_POST_COLUMNS)} FROM posts "
            f"WHERE author = ? AND linked = 1 AND seq > ?{kind_filter} "
            "ORDER BY seq ASC LIMIT ?",
            (author, max(0, int(after_seq)), *expiry_params, count),
        ).fetchall()
        posts = _public_posts(rows)
        return annotate_edits(conn, posts) if posts_only else posts

    params: List[Any] = []
    where = "linked = 1"
    if posts_only:
        where += (
            f" AND kind IN ({TIMELINE_KINDS_SQL}) AND {_NOT_DELETED}"
            " AND (expires_at IS NULL OR expires_at > ?)"
        )
        params.append(time.time())
    if excluded:
        # Display-only exclusion: sync callers leave this unset, so
        # chains stay complete for every author.  One JSON parameter
        # instead of one placeholder per author, so a large block-list
        # cannot exceed SQLite's bound-variable limit.
        where += " AND author NOT IN (SELECT value FROM json_each(?))"
        params.append(json.dumps(excluded))
    if before is not None and before_hash is not None:
        where += " AND (created_at < ? OR (created_at = ? AND hash < ?))"
        params.extend([float(before), float(before), str(before_hash)])
    elif before is not None:
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


def _chunked(values: Sequence[str], size: int) -> Iterator[List[str]]:
    """Yield *values* in chunks of at most *size*.

    SQLite caps the number of bound variables per statement, so an
    ``IN (...)`` list built from an unbounded document must be
    split; 400 stays well under the limit with room for the rest
    of the statement's parameters.
    """
    for start in range(0, len(values), size):
        yield list(values[start : start + size])


def _purge_prev_resolves(conn: sqlite3.Connection, author: str, prev: str) -> bool:
    """Whether the chain below *prev* is established on this store.

    Walks back from *prev* through stored rows and linked purge
    records: genesis (an empty parent) resolves, a stored linked entry
    from the same author resolves, and a linked purge record resolves
    by way of its own parent.  A missing hash with no record, a stored
    but unlinked row, a row from another author, or a cycle does not
    resolve — the ancestry below the gap is unproven, so an entry
    purged above it never had an established chain position.  This is
    what lets an imported record's ``prev`` be trusted without
    trusting the document that carried it: the landing it claims must
    already be verifiable here, from signed entries and records that
    proved themselves the same way.
    """
    seen: Set[str] = set()
    current = prev
    while current:
        if current in seen:
            return False
        seen.add(current)
        row = _fetch_post(conn, current)
        if row is not None:
            return str(row["author"]) == author and bool(row["linked"])
        record = _purged_post(conn, current, author, linked_only=True)
        if record is None:
            return False
        current = str(record["prev"] or "")
    return True


def _purge_records(
    conn: sqlite3.Connection,
    authors: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Purge records as plain dicts, optionally limited to *authors*.

    A record is content-free evidence — hash, author, name, the
    parent hash, whether the purged row was linked, and the post a
    purged edit belonged to — so it can travel in an export document
    or a sync page without carrying the deleted content.  Sorted for
    a deterministic document.
    """
    if authors is None:
        rows = conn.execute(
            "SELECT hash, author, name, prev, linked, target FROM purged_posts "
            "ORDER BY author ASC, hash ASC"
        ).fetchall()
    else:
        wanted = sorted({str(author) for author in authors})
        if not wanted:
            return []
        placeholders = ", ".join("?" for _ in wanted)
        rows = conn.execute(
            "SELECT hash, author, name, prev, linked, target FROM purged_posts "
            f"WHERE author IN ({placeholders}) ORDER BY author ASC, hash ASC",
            wanted,
        ).fetchall()
    return [
        {
            "hash": str(row["hash"]),
            "author": str(row["author"]),
            "name": str(row["name"] or ""),
            "prev": str(row["prev"] or ""),
            "linked": int(row["linked"] or 0),
            "target": str(row["target"] or ""),
        }
        for row in rows
    ]


def _restore_purged(
    conn: sqlite3.Connection, records: Any
) -> Tuple[List[Tuple[str, str]], List[Dict[str, str]]]:
    """Store purge records from an export document or a sync page.

    Returns ``(linked, errors)``: ``linked`` is the ``(author, hash)``
    pairs whose records now prove a chain gap (newly restored or
    upgraded from pending), for the caller to re-link children of;
    ``errors`` is one entry per malformed record skipped.

    A record is restored only on provenance this store can verify
    itself, never on the document's say-so alone:

    * a signed delete from the record's author must be present in
      this store — targeting the record's hash, or, for a purged edit,
      the post the edit belonged to (``target``).  The delete is the
      authentication that the entry was deleted; a bare record proves
      nothing.  The delete need only be signature-verified (it was,
      on ingest) — its chain position is a separate question, settled
      by the chain walk.
    * a stored entry from the same author must reference the hash —
      as a parent, or as a delete target — so a record for a hash
      nothing points at is skipped rather than stored.
    * the record's ``prev`` must resolve (see
      :func:`_purge_prev_resolves`): the landing it claims must
      already be verifiable here.  The document's own ``linked`` flag
      is never trusted — the stored flag is derived from that
      resolution instead, so an altered export cannot fabricate a
      chain position by flipping it.
    * the hash must not be a stored entry by a different author: a
      stored row is signature-verified proof of who authored it, so
      a record claiming it for someone else is forged and skipped.

    Candidates failing the backing, reference or authorship checks
    are rejected outright — they are never stored, not even as
    pending: an unbacked record is exactly how a peer could label
    an arbitrary hash deleted.  Only a candidate passing all three
    checks is stored, and only as pending (``linked=0``) while its
    predecessor is unresolved: the deletion is proven, but the gap
    is not — it bridges nothing, and a later re-offer, re-import or
    predecessor arrival upgrades it when the chain catches up (see
    :func:`_record_purged` and :func:`_resolve_pending_purged`).
    Records are restored in predecessor order, so a chain of
    consecutive purges resolves in one pass.  A record this store
    already has wins on its name, ``prev`` and ``target``; only its
    ``linked`` flag can advance — and a restored record never
    displaces another author's record (see :func:`_record_purged`).

    A restored record is marked as such: it is gap proof only, and
    never supplies an identity — the previews and the author-less
    deletion lookup ignore it.  The provenance checks run on
    scoped queries over the candidate hashes and targets, not full
    table scans, so a sync page costs lookups proportional to the
    records it carries.
    """
    errors: List[Dict[str, str]] = []
    if not isinstance(records, list):
        return [], [{"hash": "", "error": "purge records must be a list"}]
    candidates: List[Tuple[str, str, str, str, str]] = []
    for record in records:
        digest = ""
        try:
            if not isinstance(record, dict):
                raise TanError("purge record must be an object")
            digest = str(record.get("hash") or "")
            _require_hex(digest, HASH_HEX_LENGTH, "hash")
            author = _require_hex(
                str(record.get("author") or ""),
                AUTHOR_HEX_LENGTH,
                "author",
            )
            name = record.get("name", "")
            if not isinstance(name, str):
                raise TanError("purge record name must be a string")
            prev = str(record.get("prev") or "")
            if prev:
                _require_hex(prev, HASH_HEX_LENGTH, "prev")
            target = str(record.get("target") or "")
            if target:
                _require_hex(target, HASH_HEX_LENGTH, "target")
            candidates.append((digest, author, name, prev, target))
        except (TanError, ValueError, TypeError) as exc:
            errors.append({"hash": digest, "error": str(exc)})
    if not candidates:
        return [], errors
    # Provenance is settled before anything is stored: only
    # candidates passing the backing, reference and authorship
    # checks below are restored at all.  The lookups are scoped to
    # the candidate hashes and targets, chunked to stay under
    # SQLite's variable limit, so a page costs work proportional
    # to the records it carries rather than a full table scan.
    hashes = sorted({digest for digest, _, _, _, _ in candidates})
    targets = sorted(
        {target for _, _, _, _, target in candidates if target}
    )
    wanted = sorted(set(hashes) | set(targets))
    backed: Set[Tuple[str, str]] = set()
    referenced: Set[Tuple[str, str]] = set()
    stored_authors: Dict[str, str] = {}
    for chunk in _chunked(wanted, 400):
        placeholders = ", ".join("?" for _ in chunk)
        for row in conn.execute(
            "SELECT author, target FROM posts WHERE kind = 'delete' "
            f"AND target IN ({placeholders})",
            chunk,
        ).fetchall():
            backed.add((str(row["author"]), str(row["target"])))
        for row in conn.execute(
            "SELECT author, prev FROM posts WHERE prev IN "
            f"({placeholders})",
            chunk,
        ).fetchall():
            referenced.add((str(row["author"]), str(row["prev"])))
        for row in conn.execute(
            "SELECT author, target FROM posts WHERE kind = 'delete' "
            f"AND target IN ({placeholders})",
            chunk,
        ).fetchall():
            referenced.add((str(row["author"]), str(row["target"])))
    for chunk in _chunked(hashes, 400):
        placeholders = ", ".join("?" for _ in chunk)
        for row in conn.execute(
            f"SELECT hash, author FROM posts WHERE hash IN ({placeholders})",
            chunk,
        ).fetchall():
            stored_authors[str(row["hash"])] = str(row["author"])
    validated: List[Tuple[str, str, str, str, str]] = []
    for digest, author, name, prev, target in candidates:
        if (author, digest) not in backed and (
            not target or (author, target) not in backed
        ):
            continue
        if (author, digest) not in referenced:
            continue
        if stored_authors.get(digest, author) != author:
            continue
        validated.append((digest, author, name, prev, target))
    linked: List[Tuple[str, str]] = []
    pending = list(validated)
    with conn:
        while pending:
            progressed = False
            remaining: List[Tuple[str, str, str, str, str]] = []
            for digest, author, name, prev, target in pending:
                if _purge_prev_resolves(conn, author, prev):
                    _record_purged(
                        conn, digest, author, name, prev,
                        linked=True, target=target, restored=True,
                    )
                    linked.append((author, digest))
                    progressed = True
                else:
                    remaining.append((digest, author, name, prev, target))
            if not progressed:
                break
            pending = remaining
        for digest, author, name, prev, target in pending:
            _record_purged(
                conn, digest, author, name, prev,
                linked=False, target=target, restored=True,
            )
    return linked, errors


def _relink_purged_children(
    conn: sqlite3.Connection, restored: Sequence[Tuple[str, str]]
) -> None:
    """Link entries waiting on purge records that now prove a gap.

    Runs after a restore (import or sync page): a child stored
    unlinked while its parent's record was missing links up once the
    record is there, and the promotion carries through its own
    descendants, including a delete that follows it.  A newly linked
    record can also resolve an older pending record naming it as
    predecessor — the pending record upgrades here, in the same
    committed transaction, so entries behind that gap link up too
    even when the pending record itself was not re-offered.  The
    relink commits here, in its own transaction: the callers close
    the connection right after (the CLI and web imports, the last
    sync page), and an uncommitted relink would be rolled back on
    close — the entries would stay unlinked on disk while the
    process that just linked them still sees them linked.
    """
    if not restored:
        return
    with conn:
        for author, digest in restored:
            _resolve_pending_purged(conn, author, digest)
            _link_children_of_purged(conn, author, digest)


def _close_purge_seeds(
    by_hash: Dict[str, Dict[str, Any]], seeds: Set[str]
) -> List[Dict[str, Any]]:
    """Records reachable from *seeds* by walking ``prev`` links.

    Follows each seed hash through the purge records — a seed's
    record points back to its own parent, which may itself be purged
    — so consecutive purges on one chain are all covered.  Hashes
    with no record are skipped: they are stored posts, or gaps no
    record proves.
    """
    wanted: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    queue = [seed for seed in seeds if seed]
    while queue:
        current = queue.pop(0)
        if current in seen:
            continue
        seen.add(current)
        record = by_hash.get(current)
        if record is None:
            continue
        wanted.append(record)
        prev = str(record.get("prev") or "")
        if prev and prev not in seen:
            queue.append(prev)
    wanted.sort(key=lambda record: (record["author"], record["hash"]))
    return wanted


def _purge_records_for_posts(
    conn: sqlite3.Connection, posts: Sequence[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Purge records a sync page's posts need, for the feed response.

    A fresh peer stores the page's entries one by one, and a post
    whose parent was purged only links — and only renders its
    earlier edits — when the purge record is already there.  The
    records carried are the ones the page can actually use: the
    parents and targets of the served posts (a reply, quote or
    delete names its deleted target, a post names its purged
    parent), plus the records those parents point back to, so
    consecutive purges on one chain are covered.  Anything else
    stays behind: the response budget is 64 KiB, and a record no
    served entry references is inert on the receiving side until
    such an entry arrives — with a page of its own carrying the
    record then.
    """
    authors = sorted({str(post["author"]) for post in posts})
    if not authors:
        return []
    by_hash = {record["hash"]: record for record in _purge_records(conn, authors)}
    seeds = {str(post.get("prev") or "") for post in posts}
    seeds |= {str(post.get("target") or "") for post in posts}
    return _close_purge_seeds(by_hash, seeds)


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

    The document also carries the purge records for the exported
    authors (``"purged"``): the hash, author, name, predecessor,
    chain position and edit target of every post this store purged.
    A record is content-free — it carries no post text — but a fresh
    peer needs it to walk past the gap the purge left, or edits made
    before the purged post would stop rendering after import.  The
    import restores a record only on provenance it can verify itself
    (see :func:`_restore_purged`), so a record travels only with the
    signed delete that backs it — the delete targeting its hash, or,
    for a purged edit, the post the edit belonged to.

    With *limit* and ``all_authors``, only the *limit* most recent
    entries across all authors are exported (the rest are left out),
    and the purge records shrink to match: only records the exported
    entries can actually use — the parents and targets of the
    exported entries, plus the records those parents point back to,
    so consecutive purges on one chain are all covered.  A record
    whose backing delete the limit excluded stays behind instead of
    travelling alone, so a later import keeps its gap proof intact
    rather than discarding it and losing the proof for good.
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
    if all_authors:
        authors = sorted({entry["author"] for entry in entries})
    else:
        # The requested author, even when every entry of theirs is
        # deleted — the records are the only trace left of them.
        authors = [str(author)]
    purged = _purge_records(conn, authors)
    if limit is not None:
        by_hash = {record["hash"]: record for record in purged}
        seeds = {str(entry.get("prev") or "") for entry in entries}
        seeds |= {str(entry.get("target") or "") for entry in entries}
        purged = _close_purge_seeds(by_hash, seeds)
    backed = {
        (str(entry["author"]), str(entry.get("target") or ""))
        for entry in entries
        if entry.get("kind") == KIND_DELETE
    }
    purged = [
        record
        for record in purged
        if (record["author"], record["hash"]) in backed
        or (record["target"] and (record["author"], record["target"]) in backed)
    ]
    return {
        "format": TAN_EXPORT_FORMAT,
        "version": TAN_EXPORT_VERSION,
        "exported_at": time.time(),
        "authors": sorted({entry["author"] for entry in entries}),
        "entries": entries,
        "purged": purged,
    }


def import_entries(
    conn: sqlite3.Connection, document: Any
) -> Dict[str, Any]:
    """Store every entry in an export document, verifying each one.

    Returns ``{"imported": n, "present": n, "skipped": n, "errors": [...]}``.
    An entry that fails verification is skipped with its error recorded;
    it never aborts the rest of the import, and entries already in the
    store are counted as present rather than stored twice.

    A document may also carry purge records (``"purged"``) for the
    posts its source purged; each record is restored only on
    provenance this store can verify itself — a signed delete from
    the record's author present in the store (imported above, or
    already present), a stored entry from that author referencing
    the hash, and a predecessor that resolves here (see
    :func:`_restore_purged`).  A record without its delete is
    skipped — an arbitrary signed delete need not prove the missing
    post, and a bare record proves even less.  A record whose
    predecessor has not arrived yet is kept pending rather than
    skipped, so a later import can upgrade it when the chain
    catches up.  Restoring the records lets this store walk past
    purge gaps the same way the source does, so edits made before
    a purged post keep rendering — and entries that arrived
    unlinked while a record was missing link up once it is there.
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
    linked, record_errors = _restore_purged(conn, document.get("purged") or [])
    errors.extend(record_errors)
    _relink_purged_children(conn, linked)
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

    See :func:`_post_tag_sets` for how each post's text is matched.
    """
    tag_sets = _post_tag_sets(conn, posts)
    return [
        post for post in posts if tag in tag_sets.get(str(post["hash"]), set())
    ]


def _post_tag_sets(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
) -> Dict[str, Set[str]]:
    """Map each post's hash to the set of tags its text carries.

    The match runs on each post's current text — an edited post matches
    its edited text, not the original — and a repost matches on the
    text of the post it reposts, since a repost carries no text of its
    own.  A repost whose original has not arrived (or is deleted, or
    has expired) carries no visible text and never matches: the
    display blanks an expired original's text, so matching on it would
    route disappeared content into tag timelines.  Originals are fetched
    in one query, the same way :func:`annotate_reposts` fetches them.
    """
    if not posts:
        return {}
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
            if is_expired(original):
                continue
            text = original_states.get(original["hash"], {}).get(
                "text", original["text"]
            )
            originals[original["hash"]] = str(text)
    tag_sets: Dict[str, Set[str]] = {}
    for post in posts:
        if post.get("kind") == KIND_REPOST:
            text = originals.get(str(post.get("target") or ""), "")
        else:
            text = states.get(str(post["hash"]), {}).get(
                "text", str(post.get("text") or "")
            )
        tag_sets[str(post["hash"])] = set(extract_tags(text))
    return tag_sets


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
    restores them.  Posts carrying a muted tag (see :func:`mute_tag`)
    never appear either — a muted tag wins over a followed tag and
    over a followed author — for the same reason.  Posts by muted
    authors never appear either, for the same reason.  Hidden posts never appear either, and a reply to
    one keeps its ``reply_to`` preview text blanked — hiding is
    display-only, so unhiding restores them.  A repost of a post by
    a muted or blocked author, or
    of a post matching a muted keyword, never appears either — a
    repost carries no text of its own, so hiding the original hides
    the repost.  Posts carrying a tag the viewer follows (see
    :func:`follow_tag`) also appear when their author is not
    followed, marked with ``via_tag`` — but the silencing rules
    above still win: a muted or blocked author's tagged post never
    appears, and neither does one matching a muted keyword or carrying
    a muted tag.  Pages are read until ``limit`` unmuted posts are
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
    muted_tag_names = _muted_tag_list(conn)
    wanted_tag = clean_tag(tag) if tag is not None else None
    keep_tag = _tag_page_filter(
        conn,
        wanted_tag,
        keywords=keywords,
        exclude_authors=silenced,
        blocked_authors=blocked,
    )
    hidden_hashes = hidden_post_set(conn)

    def _drop_hidden(page: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        visible = keep_tag(page) if keep_tag is not None else page
        visible = without_muted_tags(conn, visible, muted_tag_names)
        return without_hidden_posts(visible, hidden_hashes)
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
            filter_page=_drop_hidden,
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
                            hidden_posts=hidden_hashes,
                        ),
                    ),
                    muted_keywords=keywords,
                    muted_tags=muted_tag_names,
                    exclude_authors=silenced,
                    blocked_authors=blocked,
                    hidden_posts=hidden_hashes,
                ),
                viewer=viewer,
            ),
            muted_keywords=keywords,
            exclude_authors=silenced,
            blocked_authors=blocked,
            hidden_posts=hidden_hashes,
        )
    clean_authors = [
        _require_hex(author, AUTHOR_HEX_LENGTH, "author") for author in authors or []
    ]
    clean_authors = [author for author in clean_authors if author not in hidden]
    followed = _followed_tag_list(conn)
    if not clean_authors and not followed:
        return []
    if followed:
        # A followed tag pulls in posts from authors the viewer does
        # not follow, so a topic is followed, not just its authors.
        # The page then scans every author's posts and keeps followed
        # authors' posts plus tag matches; without followed tags the
        # author-filtered query below stays the cheaper path.  A post
        # that enters only through a tag is marked with ``via_tag``,
        # so the owner can tell why an unfamiliar author appears.
        followed_set = set(clean_authors)

        def _keep_followed_or_tagged(
            page: List[Dict[str, Any]],
        ) -> List[Dict[str, Any]]:
            # Drop reposts the final annotation would hide before tag
            # matching, so they never consume the paging limit.
            visible = annotate_reposts(
                conn,
                _drop_hidden(page),
                muted_keywords=keywords,
                exclude_authors=silenced,
                blocked_authors=blocked,
                hidden_posts=hidden_hashes,
            )
            tag_sets = _post_tag_sets(conn, visible)
            kept: List[Dict[str, Any]] = []
            for post in visible:
                if post.get("author") in followed_set:
                    kept.append(post)
                    continue
                tags = tag_sets.get(str(post["hash"]), set())
                via = next((tag for tag in followed if tag in tags), None)
                if via is not None:
                    kept.append(dict(post, via_tag=via))
            return kept

        def _fetch_followed_page(cursor: Any) -> List[Dict[str, Any]]:
            if isinstance(cursor, tuple):
                before_created, before_hash = cursor
            else:
                before_created, before_hash = cursor, None
            return feed(
                conn,
                limit=MAX_FEED_POSTS,
                before=before_created,
                before_hash=before_hash,
                posts_only=True,
                exclude_authors=hidden,
            )

        posts = _fill_unmuted(
            conn,
            keywords,
            limit,
            _fetch_followed_page,
            before,
            lambda post: (float(post["created_at"]), str(post["hash"])),
            filter_page=_keep_followed_or_tagged,
        )
    else:
        placeholders = ", ".join("?" for _ in clean_authors)

        def page(cursor: Optional[float]) -> List[Dict[str, Any]]:
            params: List[Any] = list(clean_authors)
            where = (
                f"linked = 1 AND kind IN ({TIMELINE_KINDS_SQL}) "
                f"AND author IN ({placeholders}) AND {_NOT_DELETED} "
                "AND (expires_at IS NULL OR expires_at > ?)"
            )
            params.append(time.time())
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
            filter_page=_drop_hidden,
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
                        hidden_posts=hidden_hashes,
                    ),
                ),
                muted_keywords=keywords,
                muted_tags=muted_tag_names,
                exclude_authors=silenced,
                blocked_authors=blocked,
                hidden_posts=hidden_hashes,
            ),
            viewer=viewer,
        ),
        muted_keywords=keywords,
        exclude_authors=silenced,
        blocked_authors=blocked,
        hidden_posts=hidden_hashes,
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
    """Return one author's posts newest first, with muted and hidden posts skipped.

    Pages through the author's chain until *limit* unmuted posts are
    collected, so muted posts never shrink the result.  Posts carrying
    a muted tag never appear either, as in :func:`timeline`.  Reply previews
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
    muted_tag_names = _muted_tag_list(conn)
    silenced = muted_author_set(conn)
    wanted_tag = clean_tag(tag) if tag is not None else None
    keep_tag = _tag_page_filter(
        conn, wanted_tag, keywords=keywords, blocked_authors=blocked
    )
    hidden_hashes = hidden_post_set(conn)

    def _drop_hidden(page: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        visible = keep_tag(page) if keep_tag is not None else page
        visible = without_muted_tags(conn, visible, muted_tag_names)
        return without_hidden_posts(visible, hidden_hashes)

    def page(cursor: int) -> List[Dict[str, Any]]:
        rows = conn.execute(
            f"SELECT {', '.join(_POST_COLUMNS)} FROM posts "
            "WHERE author = ? AND linked = 1 AND seq > ? "
            f"AND kind IN ({TIMELINE_KINDS_SQL}) AND {_NOT_DELETED} "
            "AND (expires_at IS NULL OR expires_at > ?) "
            "ORDER BY seq ASC LIMIT ?",
            (author, max(0, int(cursor)), time.time(), MAX_FEED_POSTS),
        ).fetchall()
        return [_row_to_post_with_meta(row) for row in rows]

    posts = _fill_unmuted(
        conn,
        keywords,
        limit,
        page,
        0,
        lambda post: int(post["seq"]),
        filter_page=_drop_hidden,
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
                        hidden_posts=hidden_hashes,
                    ),
                ),
                muted_keywords=keywords,
                muted_tags=muted_tag_names,
                exclude_authors=silenced,
                blocked_authors=blocked,
                hidden_posts=hidden_hashes,
            ),
            viewer=viewer,
        ),
        muted_keywords=keywords,
        blocked_authors=blocked,
        hidden_posts=hidden_hashes,
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
    restores them.  Hidden posts never rank either, and a reply to
    one keeps its parent preview text blanked with a ``hidden``
    flag.  An explicit *author* is an explicit lookup, like
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
    hidden_hashes = hidden_post_set(conn)
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
    candidates = annotate_edits(conn, _without_expired(_public_posts(rows)))
    candidates = without_muted_keywords(conn, candidates, keywords)
    candidates = without_hidden_posts(candidates, hidden_hashes)
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
        hidden_posts=hidden_hashes,
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
    its author is muted or the entry itself is hidden, and a muted or
    hidden ancestor above it, which simply
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
    hidden_hashes = hidden_post_set(conn)
    muted_pattern = _muted_keyword_pattern(keywords)
    root = start
    ancestors = {root["hash"]}
    # Once a muted entry is met (the named entry itself, or an ancestor),
    # the visible root stops moving, but the climb continues so that
    # deleted ancestors above it are still rejected.
    anchored = (
        start["author"] in silenced
        or str(start["hash"]) in hidden_hashes
        or _text_matches_muted_keywords(
            str(start.get("text") or ""), muted_pattern
        )
    )
    cursor = start
    while cursor.get("kind") in (KIND_REPLY, KIND_REPOST) and cursor.get("target"):
        parent = get_post(conn, str(cursor["target"]))
        if parent is None:
            if _delete_entry_exists(conn, str(cursor["target"])):
                # The parent was purged when its author deleted it; the
                # tombstone still hides the subtree.
                raise TanError(f"post has been deleted: {ref!r}")
            break
        if parent.get("kind") not in TIMELINE_KINDS:
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
        if is_expired(parent):
            # An expired parent hides its subtree the same way: the
            # thread cannot be read past a post that is gone.
            raise TanError(f"post has expired: {ref!r}")
        if parent["author"] in excluded:
            raise TanError(f"post is from a blocked author: {ref!r}")
        if not parent.get("linked"):
            break
        ancestors.add(parent["hash"])
        cursor = parent
        if not anchored and (
            parent["author"] in silenced
            or str(parent["hash"]) in hidden_hashes
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
            _without_expired(_public_posts(rows)),
            key=lambda post: (post["created_at"], post["hash"]),
        )
        # A muted reply hides its whole subtree: it never enters the
        # frontier, so its own replies are never collected.  Replies by
        # muted authors are hidden the same way.
        children = [
            child
            for child in generation
            if child["hash"] not in seen
            and child["hash"] not in hidden_hashes
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
                hidden_posts=hidden_hashes,
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

    Only linked ``post``, ``quote`` and ``poll`` entries match: reaction entries
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
    Posts in the local hidden list never match, open search or
    explicit-author search alike — hiding is about the post, not
    the author.
    """
    if not isinstance(query, str) or not query.strip():
        raise TanError("search text must not be empty")
    if exclude_all:
        return []
    excluded = set(_clean_excluded(exclude_authors))
    count = max(1, min(int(limit), MAX_SEARCH_POSTS))
    needle = query.strip().casefold()
    params: List[Any] = []
    where = f"linked = 1 AND kind IN ('post', 'quote', 'poll') AND {_NOT_DELETED}"
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
    candidates = _without_expired(_public_posts(rows))
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
    hidden_hashes = hidden_post_set(conn)
    matches = [
        post for post in matches if str(post["hash"]) not in hidden_hashes
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
            hidden_posts=hidden_hashes,
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
    either.  Hidden posts never count either.  Muted keywords are ignored — this is an explicit lookup,
    the way search is.  Results are sorted by count, most-used first,
    then by tag, and carry ``{"tag": ..., "count": ...}``.
    """
    if exclude_all:
        return []
    excluded = set(_clean_excluded(exclude_authors))
    hidden = excluded | muted_author_set(conn)
    hidden_hashes = hidden_post_set(conn)
    count = max(1, min(int(limit), MAX_TAGS))
    where = (
        "linked = 1 AND kind IN ('post', 'reply', 'quote') "
        f"AND {_NOT_DELETED}"
    )
    rows = conn.execute(
        f"SELECT {', '.join(_POST_COLUMNS)} FROM posts WHERE {where} "
        "ORDER BY created_at DESC, hash DESC",
    ).fetchall()
    posts = _without_expired(_public_posts(rows))
    states = edit_states(conn, [post["hash"] for post in posts])
    counts: Dict[str, int] = {}
    for post in posts:
        if str(post["author"]) in hidden:
            continue
        if str(post["hash"]) in hidden_hashes:
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
        if parent is None:
            # The parent was purged when its author deleted it; the
            # tombstone still hides the subtree.
            return _delete_entry_exists(conn, parent_hash)
        if parent.get("kind") not in TIMELINE_KINDS:
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
    appear, and neither do replies you hid, and ``exclude_all``
    hides every mention — the display side
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
    hidden_hashes = hidden_post_set(conn)
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
        kept.extend(
            without_hidden_posts(
                without_muted_keywords(
                    conn, _without_expired(_public_posts(visible)), keywords
                ),
                hidden_hashes,
            )
        )
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
        hidden_posts=hidden_hashes,
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


def followers_from_lists(
    follow_lists: Dict[str, Sequence[str]], own_author: str
) -> List[str]:
    """Authors whose stored follow list includes *own_author*, sorted.

    The input maps a list author to the authors on their published,
    signature-verified follow list — the shape
    :meth:`TanApp._stored_follow_lists` returns.  Entries that are not
    well-formed author hashes are skipped, and a list that names its
    own author does not make that author their own follower.
    """
    own = _require_hex(own_author, AUTHOR_HEX_LENGTH, "own_author")
    followers: Set[str] = set()
    for list_author, authors in follow_lists.items():
        if not isinstance(list_author, str) or not isinstance(authors, (list, tuple)):
            continue
        try:
            list_author = _require_hex(list_author, AUTHOR_HEX_LENGTH, "author")
        except PostError:
            continue
        if list_author == own:
            continue
        for entry in authors:
            if isinstance(entry, str) and entry == own:
                followers.add(list_author)
                break
    return sorted(followers)


def expand_follow_scope(
    followed: Sequence[str],
    follow_lists: Dict[str, Sequence[str]],
    *,
    depth: int = TAN_SYNC_DEPTH_MAX,
    limit: int = MAX_FOLLOWS,
) -> List[str]:
    """Authors in scope at a follow-graph depth, in stable order.

    Depth 1 returns the followed authors.  Depth 2 adds every author
    on a followed author's published follow list — the second hop.
    Followed authors always come first, in follow order, then the
    second hop in sorted order, so the result is deterministic and a
    cap keeps the newest follows instead of the newest strangers.
    Entries that are not well-formed author hashes are skipped: a
    stored list is signature-verified, so this is belt and braces
    against a hand-edited file, not a trust decision.
    """
    seen: Set[str] = set()
    scoped: List[str] = []
    for author in followed:
        try:
            clean = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
        except TanError:
            continue
        if clean in seen:
            continue
        seen.add(clean)
        scoped.append(clean)
    if depth < TAN_SYNC_DEPTH_MAX:
        return scoped
    second_hop: Set[str] = set()
    for author in scoped:
        for candidate in follow_lists.get(author, []):
            try:
                clean = _require_hex(candidate, AUTHOR_HEX_LENGTH, "author")
            except TanError:
                continue
            if clean not in seen:
                second_hop.add(clean)
    for author in sorted(second_hop):
        if len(scoped) >= max(1, int(limit)):
            break
        seen.add(author)
        scoped.append(author)
    return scoped


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


def clean_list_name(value: Any) -> Tuple[str, str]:
    """Normalise a user-supplied list name, or raise :class:`TanError`.

    Returns the (key, display) pair: the key is the casefolded name
    lists are stored and looked up under, the display is the name as
    typed.  A list name is 1..32 characters of letters, digits, spaces,
    ``-`` or ``_``, starting with a letter or digit.
    """
    if not isinstance(value, str):
        raise TanError("list name must be text")
    raw = value.strip()
    if (
        not raw
        or len(raw) > MAX_LIST_NAME
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9 _-]*", raw) is None
    ):
        raise TanError(
            f"not a list name: {value!r} "
            f"(1-{MAX_LIST_NAME} letters, digits, spaces, - or _)"
        )
    return raw.casefold(), raw


def create_list(
    conn: sqlite3.Connection,
    name: str,
    *,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Create an empty named author list.

    Lists are local-only curation: they are never published, signed or
    synced, so creating one touches no chain and no peer.  Creating a
    list that already exists (case-insensitively) is an error.
    """
    key, display = clean_list_name(name)
    timestamp = float(now if now is not None else time.time())
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute(
            "SELECT name, display, created_at FROM tan_lists WHERE name = ?",
            (key,),
        ).fetchone()
        if existing is not None:
            conn.rollback()
            raise TanError(f"list {existing['display']!r} already exists")
        count = conn.execute("SELECT COUNT(*) AS n FROM tan_lists").fetchone()["n"]
        if int(count) >= MAX_TAN_LISTS:
            raise TanError(f"lists are limited to {MAX_TAN_LISTS}")
        conn.execute(
            "INSERT INTO tan_lists(name, display, created_at) VALUES(?, ?, ?)",
            (key, display, timestamp),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return {"name": display, "created_at": timestamp, "members": 0}


def delete_list(conn: sqlite3.Connection, name: str) -> bool:
    """Delete a list and its memberships.  Returns whether one existed.

    Deleting a list never unfollows anyone and never deletes a post —
    it only drops the local curation.
    """
    key, _ = clean_list_name(name)
    with conn:
        cursor = conn.execute("DELETE FROM tan_lists WHERE name = ?", (key,))
        conn.execute(
            "DELETE FROM tan_list_members WHERE list_name = ?", (key,)
        )
    return cursor.rowcount > 0


def tan_lists(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Named author lists, oldest first, each with its member count."""
    rows = conn.execute(
        "SELECT l.name AS key, l.display AS name, l.created_at AS created_at, "
        "COUNT(m.author) AS members FROM tan_lists l "
        "LEFT JOIN tan_list_members m ON m.list_name = l.name "
        "GROUP BY l.name ORDER BY l.created_at ASC"
    ).fetchall()
    return [
        {
            "name": str(row["name"]),
            "created_at": float(row["created_at"]),
            "members": int(row["members"]),
        }
        for row in rows
    ]


def _list_display(conn: sqlite3.Connection, name: str) -> str:
    """The display name of the list *name* names, or raise :class:`TanError`."""
    key, _ = clean_list_name(name)
    row = conn.execute(
        "SELECT display FROM tan_lists WHERE name = ?", (key,)
    ).fetchone()
    if row is None:
        raise TanError(f"no list named {name!r}")
    return str(row["display"])


def list_members(conn: sqlite3.Connection, name: str) -> List[Dict[str, Any]]:
    """Members of one list, most recently added first."""
    display = _list_display(conn, name)
    key = display.casefold()
    rows = conn.execute(
        "SELECT author, name, added_at FROM tan_list_members "
        "WHERE list_name = ? ORDER BY added_at DESC",
        (key,),
    ).fetchall()
    return [
        {
            "author": str(row["author"]),
            "name": str(row["name"]),
            "added_at": float(row["added_at"]),
        }
        for row in rows
    ]


def list_member_authors(conn: sqlite3.Connection, name: str) -> List[str]:
    """Author hashes of one list's members, in add order."""
    display = _list_display(conn, name)
    rows = conn.execute(
        "SELECT author FROM tan_list_members WHERE list_name = ? "
        "ORDER BY added_at ASC",
        (display.casefold(),),
    ).fetchall()
    return [str(row["author"]) for row in rows]


def add_to_list(
    conn: sqlite3.Connection,
    name: str,
    author: str,
    *,
    author_name: str = "",
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Add *author* to a list.  Re-adding keeps the original ``added_at``.

    The author need not be followed — a list is a curation, not a
    second follow graph — so callers that care should say so.  Returns
    the membership record with ``added`` saying whether this call added
    them (``False`` when they were already a member).
    """
    display = _list_display(conn, name)
    key = display.casefold()
    author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
    clean_name = sanitize_name(author_name, fallback="") if author_name else ""
    timestamp = float(now if now is not None else time.time())
    # Take the write lock before checking for an existing membership
    # and the capacity, so a concurrent add cannot slip in between
    # check and insert.
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute(
            "SELECT added_at FROM tan_list_members "
            "WHERE list_name = ? AND author = ?",
            (key, author),
        ).fetchone()
        if existing is None:
            count = conn.execute(
                "SELECT COUNT(*) AS n FROM tan_list_members WHERE list_name = ?",
                (key,),
            ).fetchone()["n"]
            if int(count) >= MAX_LIST_MEMBERS:
                raise TanError(
                    f"list {display!r} is limited to {MAX_LIST_MEMBERS} members"
                )
        conn.execute(
            "INSERT INTO tan_list_members(list_name, author, name, added_at) "
            "VALUES(?, ?, ?, ?) "
            "ON CONFLICT(list_name, author) DO UPDATE SET "
            "name = CASE WHEN excluded.name != '' THEN excluded.name "
            "ELSE tan_list_members.name END, "
            "added_at = tan_list_members.added_at",
            (key, author, clean_name, timestamp),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    if existing is not None:
        timestamp = float(existing["added_at"])
    return {
        "list": display,
        "author": author,
        "name": clean_name,
        "added_at": timestamp,
        "added": existing is None,
    }


def remove_from_list(conn: sqlite3.Connection, name: str, author: str) -> bool:
    """Remove *author* from a list.  Returns whether one was removed."""
    display = _list_display(conn, name)
    author = _require_hex(author, AUTHOR_HEX_LENGTH, "author")
    with conn:
        cursor = conn.execute(
            "DELETE FROM tan_list_members WHERE list_name = ? AND author = ?",
            (display.casefold(), author),
        )
    return cursor.rowcount > 0


def list_timeline(
    conn: sqlite3.Connection,
    name: str,
    *,
    limit: int = 30,
    viewer: Optional[str] = None,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
    tag: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """The timeline read through one list: only its members' posts.

    The same silencing as :func:`timeline` applies — muted and blocked
    authors stay hidden — and a followed tag never pulls a non-member's
    post into a list view, even though it would into the full timeline.
    A list with no members, or a list naming no stored posts, reads
    empty; an unknown list is an error.
    """
    members = list_member_authors(conn, name)
    if not members:
        return []
    posts = timeline(
        conn,
        members,
        limit=limit,
        viewer=viewer,
        exclude_authors=exclude_authors,
        exclude_all=exclude_all,
        tag=tag,
    )
    wanted = set(members)
    return [post for post in posts if post.get("author") in wanted]


def _muted_keyword_list(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute("SELECT keyword FROM muted_keywords").fetchall()
    return [str(row["keyword"]) for row in rows]


def follow_tag(
    conn: sqlite3.Connection,
    tag: str,
    *,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Follow *tag* so its posts join the timeline.  Re-following is a
    no-op returning the original record, so the first follow time is kept."""
    clean = clean_tag(tag)
    display = str(tag).strip()
    timestamp = float(now if now is not None else time.time())
    # Take the write lock before checking for an existing follow and the
    # capacity, so a concurrent follow cannot slip in between check and insert.
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute(
            "SELECT tag, display, added_at FROM followed_tags WHERE tag = ?",
            (clean,),
        ).fetchone()
        if existing is not None:
            conn.rollback()
            return {
                "tag": str(existing["tag"]),
                "display": str(existing["display"]),
                "added_at": float(existing["added_at"]),
            }
        count = conn.execute("SELECT COUNT(*) AS n FROM followed_tags").fetchone()["n"]
        if int(count) >= MAX_FOLLOWED_TAGS:
            raise TanError(
                f"followed tag list is limited to {MAX_FOLLOWED_TAGS} tags"
            )
        conn.execute(
            "INSERT INTO followed_tags(tag, display, added_at) "
            "VALUES(?, ?, ?)",
            (clean, display, timestamp),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return {"tag": clean, "display": display, "added_at": timestamp}


def unfollow_tag(conn: sqlite3.Connection, tag: str) -> bool:
    """Stop following *tag*.  Returns whether one was removed."""
    clean = clean_tag(tag)
    with conn:
        cursor = conn.execute(
            "DELETE FROM followed_tags WHERE tag = ?", (clean,)
        )
    return cursor.rowcount > 0


def followed_tags(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Followed tags, newest first."""
    rows = conn.execute(
        "SELECT tag, display, added_at FROM followed_tags "
        "ORDER BY added_at DESC"
    ).fetchall()
    return [
        {
            "tag": str(row["tag"]),
            "display": str(row["display"]),
            "added_at": float(row["added_at"]),
        }
        for row in rows
    ]


def _followed_tag_list(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute("SELECT tag FROM followed_tags").fetchall()
    return [str(row["tag"]) for row in rows]


def mute_tag(
    conn: sqlite3.Connection,
    tag: str,
    *,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Mute *tag* so its posts stay out of the timeline.  Re-muting is a
    no-op returning the original record, so the first mute time is kept."""
    clean = clean_tag(tag)
    display = str(tag).strip()
    timestamp = float(now if now is not None else time.time())
    # Take the write lock before checking for an existing mute and the
    # capacity, so a concurrent mute cannot slip in between check and insert.
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute(
            "SELECT tag, display, added_at FROM muted_tags WHERE tag = ?",
            (clean,),
        ).fetchone()
        if existing is not None:
            conn.rollback()
            return {
                "tag": str(existing["tag"]),
                "display": str(existing["display"]),
                "added_at": float(existing["added_at"]),
            }
        count = conn.execute("SELECT COUNT(*) AS n FROM muted_tags").fetchone()["n"]
        if int(count) >= MAX_MUTED_TAGS:
            raise TanError(
                f"muted tag list is limited to {MAX_MUTED_TAGS} tags"
            )
        conn.execute(
            "INSERT INTO muted_tags(tag, display, added_at) "
            "VALUES(?, ?, ?)",
            (clean, display, timestamp),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return {"tag": clean, "display": display, "added_at": timestamp}


def unmute_tag(conn: sqlite3.Connection, tag: str) -> bool:
    """Stop muting *tag*.  Returns whether one was removed."""
    clean = clean_tag(tag)
    with conn:
        cursor = conn.execute(
            "DELETE FROM muted_tags WHERE tag = ?", (clean,)
        )
    return cursor.rowcount > 0


def muted_tags(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Muted tags, newest first."""
    rows = conn.execute(
        "SELECT tag, display, added_at FROM muted_tags "
        "ORDER BY added_at DESC"
    ).fetchall()
    return [
        {
            "tag": str(row["tag"]),
            "display": str(row["display"]),
            "added_at": float(row["added_at"]),
        }
        for row in rows
    ]


def _muted_tag_list(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute("SELECT tag FROM muted_tags").fetchall()
    return [str(row["tag"]) for row in rows]


def without_muted_tags(
    conn: sqlite3.Connection,
    posts: List[Dict[str, Any]],
    tags: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Drop posts carrying a muted tag, keeping the rest.

    Muting is display-only: entries stay stored and keep syncing, so
    unmuting restores them.  Tags come from each post's current text —
    a repost matches on the text of the post it reposts, so muting a
    tag hides reposts of muted-tag posts too, and an edited post
    matches on its edited text.  Pass *tags* when the caller already
    loaded them to avoid a second lookup.
    """
    muted = set(tags) if tags is not None else set(_muted_tag_list(conn))
    if not muted:
        return list(posts)
    tag_sets = _post_tag_sets(conn, posts)
    return [
        post
        for post in posts
        if not (muted & tag_sets.get(str(post["hash"]), set()))
    ]


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


def without_hidden_posts(
    posts: List[Dict[str, Any]],
    hidden: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    """Drop posts the viewer hid, keeping the rest.

    Hiding is display-only: entries stay stored and keep syncing, so
    unhiding restores them.  A repost of a hidden post is dropped too
    — a repost carries no text of its own, so hiding the original
    hides the repost, the way muting an author hides reposts of their
    posts.  An empty *hidden* set keeps every post.
    """
    if not hidden:
        return list(posts)
    return [
        post
        for post in posts
        if str(post.get("hash") or "") not in hidden
        and not (
            post.get("kind") == KIND_REPOST
            and str(post.get("target") or "") in hidden
        )
    ]


def muted_author_set(conn: sqlite3.Connection) -> Set[str]:
    """Authors currently muted, as a set of destination hashes."""
    rows = conn.execute("SELECT author FROM muted_authors").fetchall()
    return {str(row["author"]) for row in rows}


def _resolve_saved_ref(conn: sqlite3.Connection, ref: str) -> str:
    """Resolve a saved-post reference to a cached post hash.

    Unlike :func:`resolve_post`, a post its author deleted still
    resolves: unsaving must keep working after the save's target is
    deleted — and the save ref itself is purged with the post, so
    resolving the hash is what lets the unsave report a clean no-op
    instead of failing as unknown.  A reference to nothing cached
    fails as unknown.
    """
    if not isinstance(ref, str) or not ref.strip():
        raise TanError("post reference must not be empty")
    value = ref.strip().lower()
    if len(value) == HASH_HEX_LENGTH:
        post = get_post(conn, _require_hex(value, HASH_HEX_LENGTH, "post"))
        if post is not None and post.get("kind", KIND_POST) in TIMELINE_KINDS:
            return str(post["hash"])
        if _delete_entry_exists(conn, value):
            return value
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
            return str(rows[0]["hash"])
        if len(rows) > 1:
            raise TanError(f"{len(rows)} posts match prefix {value!r}")
        # A purged post has no row left: a delete entry targeting the
        # prefix still resolves it, so unsaving a deleted post keeps
        # working by prefix too.
        rows = conn.execute(
            "SELECT target FROM posts WHERE kind = 'delete' "
            "AND linked = 1 AND target LIKE ? LIMIT 8",
            (value + "%",),
        ).fetchall()
        if len(rows) == 1:
            return str(rows[0]["target"])
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


def _parse_pending_peers(raw: Any) -> List[str]:
    """Peer hashes still owed a draft's post, or ``[]`` for a fresh draft."""
    if not raw:
        return []
    try:
        peers = json.loads(str(raw))
    except (TypeError, ValueError):
        return []
    if not isinstance(peers, list):
        return []
    return [str(peer) for peer in peers if peer and str(peer) != "?"]


def _draft_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": int(row["id"]),
        "text": str(row["text"]),
        "reply_to": str(row["reply_to"] or ""),
        "expires_in": (
            float(row["expires_in"]) if row["expires_in"] is not None else None
        ),
        "post_hash": str(row["post_hash"] or ""),
        "pending_peers": _parse_pending_peers(row["pending_peers"]),
        "claimed_at": (
            float(row["claimed_at"]) if row["claimed_at"] is not None else None
        ),
        "created_at": float(row["created_at"]),
    }


def _parse_draft_id(ref: Any) -> int:
    try:
        draft_id = int(str(ref).strip())
    except (TypeError, ValueError):
        raise TanError(f"unknown draft: {ref!r}") from None
    if draft_id <= 0:
        raise TanError(f"unknown draft: {ref!r}")
    return draft_id


def save_draft(
    conn: sqlite3.Connection,
    text: str,
    *,
    reply_to: str = "",
    expires_in: Optional[float] = None,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Save a local-only Tan post draft.

    Drafts are private to this node — no signed entry, nothing
    gossiped — so a half-written post can wait until its author is
    ready to publish it.  The text is validated exactly like a post's
    (``normalize_message``), so a draft can never hold text the post
    path would reject.  ``reply_to`` is the resolved hash of the post
    being replied to, or ``""``; ``expires_in`` is seconds from
    publish time, or ``None`` for a post that never expires — the
    expiry clock starts when the draft is published, not when it is
    saved.
    """
    clean_text = normalize_message(text)
    target = (reply_to or "").strip().lower()
    if target:
        _require_hex(target, HASH_HEX_LENGTH, "reply_to")
    if expires_in is not None:
        expires_in = float(expires_in)
        if not expires_in > 0:
            raise TanError("expires_in must be positive")
        if expires_in > MAX_TAN_POST_EXPIRY:
            raise TanError(
                "draft expiry must be at most "
                f"{MAX_TAN_POST_EXPIRY} seconds"
            )
    timestamp = float(now if now is not None else time.time())
    # Take the write lock before checking the capacity, so a competing
    # save cannot slip in between check and insert.
    conn.execute("BEGIN IMMEDIATE")
    try:
        count = conn.execute("SELECT COUNT(*) AS n FROM drafts").fetchone()["n"]
        if int(count) >= MAX_DRAFTS:
            raise TanError(f"drafts are limited to {MAX_DRAFTS} entries")
        cursor = conn.execute(
            "INSERT INTO drafts(text, reply_to, expires_in, created_at)"
            " VALUES(?, ?, ?, ?)",
            (clean_text, target, expires_in, timestamp),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return _draft_dict(
        conn.execute(
            "SELECT * FROM drafts WHERE id = ?", (cursor.lastrowid,)
        ).fetchone()
    )


def list_drafts(
    conn: sqlite3.Connection,
    *,
    limit: int = DEFAULT_FEED_LIMIT,
) -> List[Dict[str, Any]]:
    """Saved drafts, newest first."""
    count = max(1, min(int(limit), MAX_DRAFTS))
    rows = conn.execute(
        "SELECT * FROM drafts ORDER BY created_at DESC, id DESC LIMIT ?",
        (count,),
    ).fetchall()
    return [_draft_dict(row) for row in rows]


def get_draft(conn: sqlite3.Connection, ref: Any) -> Dict[str, Any]:
    """Resolve a draft id to its stored draft, or raise ``TanError``."""
    draft_id = _parse_draft_id(ref)
    row = conn.execute(
        "SELECT * FROM drafts WHERE id = ?", (draft_id,)
    ).fetchone()
    if row is None:
        raise TanError(f"unknown draft: {ref!r}")
    return _draft_dict(row)


def delete_draft(conn: sqlite3.Connection, ref: Any) -> Dict[str, Any]:
    """Delete a draft and return it, or raise ``TanError`` when unknown."""
    draft = get_draft(conn, ref)
    with conn:
        conn.execute("DELETE FROM drafts WHERE id = ?", (draft["id"],))
    return draft


def record_draft_publish(
    conn: sqlite3.Connection,
    ref: Any,
    *,
    post_hash: str,
    pending_peers: Sequence[str] = (),
) -> Dict[str, Any]:
    """Remember the signed post a draft published as, and who still owes it.

    A draft whose first publish reached only some peers keeps the post's
    hash and the failed peers' hashes, so a retry re-sends that same
    signed post to the peers that missed it instead of signing a
    second copy the first peers would see as a duplicate.  Unknown
    peer hashes are dropped — the retry then falls back to the
    draft's original targets.
    """
    draft = get_draft(conn, ref)
    digest = (post_hash or "").strip().lower()
    if digest:
        _require_hex(digest, HASH_HEX_LENGTH, "post_hash")
    pending = [
        str(peer)
        for peer in dict.fromkeys(pending_peers)
        if peer and str(peer) != "?"
    ]
    with conn:
        conn.execute(
            "UPDATE drafts SET post_hash = ?, pending_peers = ? WHERE id = ?",
            (digest, json.dumps(pending), draft["id"]),
        )
    return get_draft(conn, draft["id"])


def claim_draft(
    conn: sqlite3.Connection,
    ref: Any,
    *,
    now: Optional[float] = None,
    ttl: float = DRAFT_CLAIM_TTL,
) -> Optional[str]:
    """Atomically claim a draft for publishing, or report it is taken.

    The claim is one ``UPDATE`` that only wins when the draft is
    unclaimed or its claim is older than *ttl*, so two concurrent
    ``publish-draft`` runs can never both sign a post for the same
    draft: the loser's update touches zero rows.  Returns the claim
    token when this caller won the claim, ``None`` otherwise.  The
    token is the claim's owner: only its holder can refresh or
    release the claim, so a publisher that lost its lease cannot
    clear the claim its successor just took.  Raises ``TanError``
    for an unknown draft.
    """
    draft = get_draft(conn, ref)
    stamp = float(now if now is not None else time.time())
    token = uuid.uuid4().hex
    with conn:
        cursor = conn.execute(
            "UPDATE drafts SET claimed_at = ?, claim_token = ? WHERE id = ? "
            "AND (claimed_at IS NULL OR claimed_at <= ?)",
            (stamp, token, draft["id"], stamp - float(ttl)),
        )
    return token if cursor.rowcount == 1 else None


def refresh_draft_claim(
    conn: sqlite3.Connection,
    ref: Any,
    token: str,
    *,
    now: Optional[float] = None,
) -> bool:
    """Renew a live claim, or report it is no longer owned.

    Only the token that won the claim can renew it, and only while
    the claim is still live: returns ``False`` when another
    publisher has taken the draft over, or the claim was released.
    A live publisher calls this on a timer so a slow publish never
    lets its own claim go stale.
    """
    draft_id = _parse_draft_id(ref)
    stamp = float(now if now is not None else time.time())
    with conn:
        cursor = conn.execute(
            "UPDATE drafts SET claimed_at = ? WHERE id = ? AND claim_token = ?",
            (stamp, draft_id, token),
        )
    return cursor.rowcount == 1


def draft_claim_held(
    conn: sqlite3.Connection,
    ref: Any,
    token: str,
    *,
    now: Optional[float] = None,
    ttl: float = DRAFT_CLAIM_TTL,
) -> bool:
    """True when *token* still owns the draft's live claim."""
    try:
        draft_id = _parse_draft_id(ref)
    except TanError:
        return False
    row = conn.execute(
        "SELECT claimed_at, claim_token FROM drafts WHERE id = ?",
        (draft_id,),
    ).fetchone()
    if row is None:
        return False
    if row["claim_token"] != token:
        return False
    if row["claimed_at"] is None:
        return False
    stamp = float(now if now is not None else time.time())
    return float(row["claimed_at"]) > stamp - float(ttl)


def release_draft_claim(conn: sqlite3.Connection, ref: Any, token: str) -> None:
    """Drop a draft's publish claim, if *token* still owns it.

    Called whenever a publish attempt leaves the draft behind — a
    failed publish, a partial delivery — so an immediate retry is not
    blocked by this run's own claim.  A no-op when the draft was
    already deleted by a successful publish, was never claimed, or
    the claim has passed to another publisher: clearing a
    successor's claim would reopen the duplicate-post race.  A claim
    a crashed publisher left behind goes stale on its own after
    ``DRAFT_CLAIM_TTL`` instead.
    """
    draft_id = _parse_draft_id(ref)
    with conn:
        conn.execute(
            "UPDATE drafts SET claimed_at = NULL, claim_token = NULL "
            "WHERE id = ? AND claim_token = ?",
            (draft_id, token),
        )


# Shelf tables whose rows are pageable posts, for the shared
# pageable-rows predicate below.
_SHELF_TABLES = frozenset({"saved_posts", "hidden_posts", "pinned_posts"})


def _shelf_pageable_sql(table: str) -> str:
    """Shared FROM/WHERE fragment for a shelf table's pageable rows.

    A shelf row is pageable when its post still exists and has not
    expired.  The list query and the continuation count for each shelf
    both build from this fragment, so a filter added here reaches both
    and the paging total can never drift from the rows the list query
    can return.
    """
    if table not in _SHELF_TABLES:
        raise ValueError(f"unknown shelf table: {table!r}")
    return (
        f"FROM {table} s JOIN posts p ON p.hash = s.hash "
        "WHERE p.expires_at IS NULL OR p.expires_at > ?"
    )


def _shelf_pageable_total(
    conn: sqlite3.Connection, table: str, *, now: Optional[float] = None
) -> int:
    """Rows the shelf's list query can return, at *now*.

    Uses the same predicate as the list query, so the continuation
    total counts exactly the rows paging can reach.
    """
    row = conn.execute(
        f"SELECT COUNT(*) AS n {_shelf_pageable_sql(table)}",
        (float(now if now is not None else time.time()),),
    ).fetchone()
    return int(row["n"])


def saved_posts(
    conn: sqlite3.Connection,
    *,
    limit: int = DEFAULT_FEED_LIMIT,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """Saved posts, newest save first.

    A post deleted after being saved leaves the list with the post:
    deleting purges the save ref along with the post row, so there is
    no blanked stub to keep.  *offset* pages through the list in save
    order, so a caller can reach saves older than one page.
    """
    count = max(1, min(int(limit), MAX_SAVED_POSTS))
    start = max(0, int(offset))
    # Expired posts are filtered in SQL, before LIMIT/OFFSET, so the raw
    # offset a caller advances by counts exactly the rows it was given.
    rows = conn.execute(
        f"SELECT p.*, s.saved_at {_shelf_pageable_sql('saved_posts')} "
        "ORDER BY s.saved_at DESC, s.hash DESC LIMIT ? OFFSET ?",
        (time.time(), count, start),
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

    Reaction counts, edits, reply parents, quote counts and quoted
    originals are attached exactly as :func:`timeline` does: a reply
    parent or quoted original from a muted author is redacted, and a
    parent matching a muted keyword has its text blanked.  Posts by
    muted authors or matching a muted keyword are dropped from the
    view — muting is display-only, so the saves stay stored and
    unmuting restores them.  Hidden posts are dropped the same way,
    and a hidden parent keeps its preview text blanked.  Each returned post carries ``saved`` and
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
    hidden_hashes = hidden_post_set(conn)
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
                    annotate_edits(
                        conn, annotate_reactions(conn, batch, viewer=viewer)
                    ),
                    muted_keywords=keywords,
                    exclude_authors=silenced,
                    hidden_posts=hidden_hashes,
                ),
            ),
            muted_keywords=keywords,
            exclude_authors=silenced,
            hidden_posts=hidden_hashes,
        )
        annotate_repost_counts(conn, batch, viewer=viewer)
        for post in batch:
            # Edits replace the stored text, so re-blank deleted entries.
            if post.get("deleted"):
                post["text"] = ""
                post["edited"] = False
            post["saved"] = True
        batch = [post for post in batch if post["author"] not in silenced]
        batch = without_hidden_posts(batch, hidden_hashes)
        batch = without_muted_keywords(conn, batch, keywords)
        if ruleset is not None:
            batch = apply_display_rules(batch, ruleset)
        kept.extend(batch)
    if len(kept) < limit:
        return kept, None
    # Count only rows saved_posts can return — same shared predicate
    # as the list query — so an expired-only tail does not offer an
    # empty next page.
    total = _shelf_pageable_total(conn, "saved_posts")
    return kept, raw_offset if raw_offset < int(total) else None


def annotated_hidden_posts(
    conn: sqlite3.Connection,
    *,
    limit: int = DEFAULT_FEED_LIMIT,
    offset: int = 0,
    viewer: Optional[str] = None,
    ruleset: Optional[Ruleset] = None,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """Hidden posts with the timeline's annotations and muting applied.

    Reaction counts, edits, reply parents, quote counts, quoted
    originals and repost originals are attached exactly as
    :func:`timeline` does: a reply parent, quoted or repost original
    from a muted or blocked author is redacted or dropped, and a parent
    matching a muted keyword has its text blanked.  The hidden list
    itself is not filtered by hides — every entry stays listed, in
    :func:`hidden_posts` order — but posts by muted authors or
    matching a muted keyword are dropped from the view, the same as
    the saved and pinned views: muting is display-only, so the hides
    stay stored and unmuting restores them.  ``exclude_authors``
    hides blocked authors' words the same way, and ``exclude_all``
    hides everything when the blocklist could not be read.  A repost
    whose original is also hidden drops out — it carries no text of
    its own — and returns when the original is unhidden; the hide
    itself stays stored either way.

    Returns ``(posts, next_offset)``: up to *limit* visible posts
    starting at raw offset *offset*, and the raw offset to resume
    paging from, or ``None`` when the hidden list is exhausted.
    Muted entries and entries hidden by *ruleset* never shrink a
    page: raw rows are read until *limit* visible posts are collected
    or the list is exhausted, so a caller must page with the returned
    ``next_offset``, not with the number of posts it received.
    """
    if exclude_all:
        return [], None
    silenced = muted_author_set(conn)
    blocked = set(_clean_excluded(exclude_authors))
    keywords = _muted_keyword_list(conn)
    hidden_hashes = hidden_post_set(conn)
    kept: List[Dict[str, Any]] = []
    raw_offset = max(0, int(offset))
    while len(kept) < limit:
        batch = hidden_posts(conn, limit=limit - len(kept), offset=raw_offset)
        if not batch:
            break
        raw_offset += len(batch)
        batch = annotate_reposts(
            conn,
            annotate_repost_counts(
                conn,
                annotate_quotes(
                    conn,
                    annotate_quote_counts(
                        conn,
                        annotate_replies(
                            conn,
                            annotate_reactions(conn, batch, viewer=viewer),
                            muted_keywords=keywords,
                            exclude_authors=silenced,
                            blocked_authors=blocked,
                            hidden_posts=hidden_hashes,
                        ),
                    ),
                    muted_keywords=keywords,
                    exclude_authors=silenced,
                    blocked_authors=blocked,
                    hidden_posts=hidden_hashes,
                ),
                viewer=viewer,
            ),
            muted_keywords=keywords,
            exclude_authors=silenced,
            blocked_authors=blocked,
            hidden_posts=hidden_hashes,
        )
        batch = [
            post
            for post in batch
            if post["author"] not in silenced and post["author"] not in blocked
        ]
        batch = without_muted_keywords(conn, batch, keywords)
        if ruleset is not None:
            batch = apply_display_rules(batch, ruleset)
        kept.extend(batch)
    if len(kept) < limit:
        return kept, None
    # Count only rows hidden_posts can return — same shared predicate
    # as the list query — so an expired-only tail does not offer an
    # empty next page.
    total = _shelf_pageable_total(conn, "hidden_posts")
    return kept, raw_offset if raw_offset < int(total) else None


def pin_post(
    conn: sqlite3.Connection,
    ref: str,
    *,
    owner: str,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Pin one of the node's own posts to the top of its pinned shelf.

    Pins are private to this node — no signed entry, nothing gossiped.
    The target must be a cached, linked post or reply authored by
    *owner* (the node's own destination hash) that has not been
    deleted; pinning the same post twice is a no-op that reports the
    original pin time.  The shelf holds at most ``MAX_PINNED_POSTS``
    entries: a pin shelf is curated, not a second archive.
    """
    post = resolve_post(conn, ref)
    if not post.get("linked"):
        raise TanError("post has not linked yet; sync its chain before pinning")
    owner = _require_hex(owner, AUTHOR_HEX_LENGTH, "owner")
    author = str(post["author"])
    if author.lower() != owner.lower():
        raise TanError("only your own posts can be pinned")
    digest = str(post["hash"])
    timestamp = float(now if now is not None else time.time())
    # Take the write lock before checking for an existing pin and the
    # capacity, so a competing pin cannot slip in between check and insert.
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute(
            "SELECT pinned_at FROM pinned_posts WHERE hash = ?", (digest,)
        ).fetchone()
        if existing is not None:
            conn.rollback()
            return {
                "hash": digest,
                "author": author,
                "name": str(post.get("name") or ""),
                "pinned_at": float(existing["pinned_at"]),
                "already_pinned": True,
            }
        count = conn.execute("SELECT COUNT(*) AS n FROM pinned_posts").fetchone()["n"]
        if int(count) >= MAX_PINNED_POSTS:
            raise TanError(f"pinned posts are limited to {MAX_PINNED_POSTS} entries")
        conn.execute(
            "INSERT INTO pinned_posts(hash, pinned_at) VALUES(?, ?)",
            (digest, timestamp),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return {
        "hash": digest,
        "author": author,
        "name": str(post.get("name") or ""),
        "pinned_at": timestamp,
        "already_pinned": False,
    }


def unpin_post(conn: sqlite3.Connection, ref: str) -> Dict[str, Any]:
    """Remove a post from the local pinned shelf.

    Returns the resolved post hash with whether a pin was removed, so
    callers — including ``--json`` output — can identify the post even
    when *ref* was a short prefix.
    """
    # The saved-post resolver is shared: it resolves any cached
    # post/reply reference, including one deleted after pinning, so
    # unpinning keeps working after the pin's target is deleted.
    digest = _resolve_saved_ref(conn, ref)
    with conn:
        cursor = conn.execute("DELETE FROM pinned_posts WHERE hash = ?", (digest,))
    return {"hash": digest, "unpinned": cursor.rowcount > 0}


def pinned_posts(
    conn: sqlite3.Connection,
    *,
    limit: int = DEFAULT_FEED_LIMIT,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """Pinned posts, newest pin first.

    A post deleted after being pinned leaves the list with the post:
    deleting purges the pin ref along with the post row, so there is
    no blanked stub to keep.  *offset* pages through the list in pin
    order, so a caller can reach pins older than one page.
    """
    count = max(1, min(int(limit), MAX_PINNED_POSTS))
    start = max(0, int(offset))
    # Expired posts are filtered in SQL, before LIMIT/OFFSET, so the raw
    # offset a caller advances by counts exactly the rows it was given.
    rows = conn.execute(
        f"SELECT p.*, s.pinned_at {_shelf_pageable_sql('pinned_posts')} "
        "ORDER BY s.pinned_at DESC, s.hash DESC LIMIT ? OFFSET ?",
        (time.time(), count, start),
    ).fetchall()
    deleted = deleted_targets(conn)
    records: List[Dict[str, Any]] = []
    for row in rows:
        post = _row_to_post(row)
        post["pinned_at"] = float(row["pinned_at"])
        if str(post["hash"]) in deleted:
            post["deleted"] = True
            post["text"] = ""
        records.append(post)
    return records


def hidden_post_set(conn: sqlite3.Connection) -> Set[str]:
    """Posts currently hidden, as a set of entry hashes."""
    rows = conn.execute("SELECT hash FROM hidden_posts").fetchall()
    return {str(row["hash"]) for row in rows}


def hide_post(
    conn: sqlite3.Connection,
    ref: str,
    *,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Hide one cached post from this node's local views.

    Hiding is private to this node — no signed entry, nothing gossiped.
    The target must be a cached, linked post, reply, quote or repost
    that has not been deleted; hiding the same post twice is a no-op
    that reports the original hide time.  Hidden posts stay stored and
    keep syncing, so unhiding restores them everywhere.
    """
    post = resolve_post(conn, ref)
    if not post.get("linked"):
        raise TanError("post has not linked yet; sync its chain before hiding")
    digest = str(post["hash"])
    timestamp = float(now if now is not None else time.time())
    # Take the write lock before checking for an existing hide and the
    # capacity, so a competing hide cannot slip in between check and insert.
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute(
            "SELECT hidden_at FROM hidden_posts WHERE hash = ?", (digest,)
        ).fetchone()
        if existing is not None:
            conn.rollback()
            return {
                "hash": digest,
                "author": str(post["author"]),
                "name": str(post.get("name") or ""),
                "hidden_at": float(existing["hidden_at"]),
                "already_hidden": True,
            }
        count = conn.execute("SELECT COUNT(*) AS n FROM hidden_posts").fetchone()["n"]
        if int(count) >= MAX_HIDDEN_POSTS:
            raise TanError(f"hidden posts are limited to {MAX_HIDDEN_POSTS} entries")
        conn.execute(
            "INSERT INTO hidden_posts(hash, hidden_at) VALUES(?, ?)",
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
        "hidden_at": timestamp,
        "already_hidden": False,
    }


def unhide_post(conn: sqlite3.Connection, ref: str) -> Dict[str, Any]:
    """Remove a post from the local hidden list.

    Returns the resolved post hash with whether a hide was removed, so
    callers — including ``--json`` output — can identify the post even
    when *ref* was a short prefix.
    """
    digest = _resolve_saved_ref(conn, ref)
    with conn:
        cursor = conn.execute("DELETE FROM hidden_posts WHERE hash = ?", (digest,))
    return {"hash": digest, "unhidden": cursor.rowcount > 0}


def hidden_posts(
    conn: sqlite3.Connection,
    *,
    limit: int = DEFAULT_FEED_LIMIT,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """Hidden posts, newest hide first.

    A post deleted after being hidden leaves the list with the post:
    deleting purges the hidden ref along with the post row, so there
    is no blanked stub to keep.  The list shows each post's current
    text: an edit that landed after the hide replaces the stored
    text, the way the saved view shows edited text.  *offset* pages
    through the list in hide order, so a caller can reach hides older
    than one page.
    """
    count = max(1, min(int(limit), MAX_HIDDEN_POSTS))
    start = max(0, int(offset))
    # Expired posts are filtered in SQL, before LIMIT/OFFSET, so the raw
    # offset a caller advances by counts exactly the rows it was given.
    rows = conn.execute(
        f"SELECT p.*, s.hidden_at {_shelf_pageable_sql('hidden_posts')} "
        "ORDER BY s.hidden_at DESC, s.hash DESC LIMIT ? OFFSET ?",
        (time.time(), count, start),
    ).fetchall()
    deleted = deleted_targets(conn)
    records: List[Dict[str, Any]] = []
    for row in rows:
        post = _row_to_post(row)
        post["hidden_at"] = float(row["hidden_at"])
        records.append(post)
    annotate_edits(conn, records)
    for post in records:
        if str(post["hash"]) in deleted:
            # Edits replace the stored text, so re-blank deleted entries.
            post["deleted"] = True
            post["text"] = ""
            post["edited"] = False
    return records


def annotated_pinned_posts(
    conn: sqlite3.Connection,
    *,
    limit: int = DEFAULT_FEED_LIMIT,
    offset: int = 0,
    viewer: Optional[str] = None,
    ruleset: Optional[Ruleset] = None,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_all: bool = False,
) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """Pinned posts with the timeline's annotations and muting applied.

    Reaction counts, edits, reply parents, quote counts, quoted
    originals and repost originals are attached exactly as
    :func:`timeline` does: a reply parent, quoted or repost original
    from a muted or blocked author is redacted or dropped, and a parent
    matching a muted keyword has its text blanked.  Posts by muted
    authors or matching a muted keyword are dropped from the view —
    muting is display-only, so the pins stay stored and unmuting
    restores them.  Hidden posts are dropped the same way, and a
    hidden parent keeps its preview text blanked.  ``exclude_authors``
    hides blocked authors' words the same way, and ``exclude_all``
    hides everything when the blocklist could not be read.  Each
    returned post carries ``pinned`` and ``pinned_at``.

    Returns ``(posts, next_offset)``: up to *limit* visible posts
    starting at raw offset *offset*, and the raw offset to resume
    paging from, or ``None`` when the pinned list is exhausted.  Muted
    entries and entries hidden by *ruleset* never shrink a page: raw
    rows are read until *limit* visible posts are collected or the
    list is exhausted, so a caller must page with the returned
    ``next_offset``, not with the number of posts it received.
    """
    if exclude_all:
        return [], None
    silenced = muted_author_set(conn)
    blocked = set(_clean_excluded(exclude_authors))
    keywords = _muted_keyword_list(conn)
    hidden_hashes = hidden_post_set(conn)
    kept: List[Dict[str, Any]] = []
    raw_offset = max(0, int(offset))
    while len(kept) < limit:
        batch = pinned_posts(conn, limit=limit - len(kept), offset=raw_offset)
        if not batch:
            break
        raw_offset += len(batch)
        batch = annotate_reposts(
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
                                conn, annotate_reactions(conn, batch, viewer=viewer)
                            ),
                            muted_keywords=keywords,
                            exclude_authors=silenced,
                            blocked_authors=blocked,
                            hidden_posts=hidden_hashes,
                        ),
                    ),
                    muted_keywords=keywords,
                    exclude_authors=silenced,
                    blocked_authors=blocked,
                    hidden_posts=hidden_hashes,
                ),
                viewer=viewer,
            ),
            muted_keywords=keywords,
            exclude_authors=silenced,
            blocked_authors=blocked,
            hidden_posts=hidden_hashes,
        )
        for post in batch:
            # Edits replace the stored text, so re-blank deleted entries.
            if post.get("deleted"):
                post["text"] = ""
                post["edited"] = False
            post["pinned"] = True
        batch = [
            post
            for post in batch
            if post["author"] not in silenced and post["author"] not in blocked
        ]
        batch = without_hidden_posts(batch, hidden_hashes)
        batch = without_muted_keywords(conn, batch, keywords)
        if ruleset is not None:
            batch = apply_display_rules(batch, ruleset)
        kept.extend(batch)
    if len(kept) < limit:
        return kept, None
    # Count only rows pinned_posts can return — same shared predicate
    # as the list query — so an expired-only tail does not offer an
    # empty next page.
    total = _shelf_pageable_total(conn, "pinned_posts")
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


def followers(
    conn: sqlite3.Connection,
    follow_lists: Dict[str, Sequence[str]],
    own_author: str,
    *,
    registry: Any = None,
) -> List[Dict[str, Any]]:
    """One record per author who follows *own_author*.

    A follower is an author whose latest stored, signature-verified
    follow list names *own_author* (see :func:`followers_from_lists`).
    Only stored lists are consulted — this never does network I/O, so
    a follower whose list was never fetched does not appear yet.
    Each record carries the display name (the peer registry's when it
    knows the author, else the name on their most recent stored post),
    the visible-post stats :func:`author_stats_many` returns, and
    whether the viewer follows them back.  Records come back most
    recently posting first, then by author hash, so the order is stable.
    """
    authors = followers_from_lists(follow_lists, own_author)
    if not authors:
        return []
    stats = author_stats_many(conn, authors)
    followed = {record["author"] for record in following(conn)}
    records: List[Dict[str, Any]] = []
    for author in authors:
        name = ""
        if registry is not None and hasattr(registry, "get"):
            peer = registry.get(author)
            if peer is not None:
                name = getattr(peer, "display_name", "") or ""
        if not name:
            row = conn.execute(
                "SELECT name FROM posts WHERE author = ? AND name != '' "
                "ORDER BY created_at DESC LIMIT 1",
                (author,),
            ).fetchone()
            if row is not None:
                name = str(row["name"])
        record = dict(stats[author])
        record["name"] = name
        record["followed"] = author in followed
        records.append(record)
    records.sort(
        key=lambda record: (-record["last_post"], record["author"])
    )
    return records


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

    A linked entry whose parent is missing is also legitimate when
    the parent was purged: either a linked delete entry from the
    same author targets the parent, or a purge record covers it (see
    :func:`_purge_post`).  The tombstone is the only proof a fresh
    peer importing after a purge ever has — the post itself never
    arrives, so no record is created for it — and the record is the
    proof for a purged edit row, which no delete targets directly.
    That is also why a forged delete can excuse an invented gap
    here; the chain walk and the no-author lookups use the purge
    record instead, so the forgery cannot leak into displayed text
    or previews — see :func:`_chain_hashes` and
    :func:`_delete_entry_exists`.)

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
        purged_rows = conn.execute(
            "SELECT author, hash FROM purged_posts WHERE linked = 1"
        ).fetchall()
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

    # A missing parent is excused when either proof covers it: a
    # linked delete entry from the same author targeting the parent,
    # or a linked purge record for the parent.  The tombstone is the
    # only proof a fresh peer importing after a purge ever has (the
    # post itself never arrives, so no record is created for it).  The
    # record is the proof for a purged *edit* row: no delete targets
    # the edit directly — the delete targets the post — but the edit's
    # hash is recorded when it is purged, and the entry created after
    # it must not look like a broken chain.  Either proof alone
    # excuses the gap; that is also why a forged delete can excuse
    # an invented gap here, and why the chain walk and the no-author
    # lookups judge gaps on the purge record instead (see
    # :func:`_chain_hashes` and :func:`_delete_entry_exists`), so the
    # forgery cannot leak into displayed text or previews.  Only a
    # linked record excuses a gap: a record from an entry that never
    # acquired a chain position proves no position for its children,
    # so the audit must not excuse a linked child standing on one.
    purged_parents = {
        (str(row["author"]), str(row["target"]))
        for row in rows
        if str(row["kind"]) == KIND_DELETE
        and _int_or_none(row["linked"]) == 1
    } | {
        (str(row["author"]), str(row["hash"])) for row in purged_rows
    }
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
                if (author, parent) in purged_parents:
                    # The parent was purged when its author deleted it;
                    # the tombstone explains the gap.
                    continue
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

    def _sync_depth(self) -> int:
        """This node's Tan sync depth (1 or 2), defaulting to 2.

        Read live from ``node.json`` on every call, so a change made
        on the CLI or in the console applies to the next sync without
        a restart.  A node without a data directory (tests, embeds)
        takes the default.
        """
        from ..config import tan_sync_depth

        data_dir = getattr(self.config, "data_dir", None)
        if data_dir is None:
            return TAN_SYNC_DEPTH_DEFAULT
        try:
            return tan_sync_depth(Path(data_dir))
        except Exception:
            return TAN_SYNC_DEPTH_DEFAULT

    def _stored_follow_lists(self) -> Dict[str, List[str]]:
        """Follow lists this node has already verified and stored.

        Prefers the running node's store (it picks up lists fetched
        since start); falls back to opening the store from the data
        directory.  Anything missing or unreadable means no second
        hop is known yet — depth 2 then behaves like depth 1 for
        those authors, rather than failing the sync.
        """
        from ..follows import FollowListStore

        store = getattr(self.node, "follow_list_store", None)
        docs: List[Dict[str, Any]] = []
        if store is not None:
            try:
                docs = store.all()
            except Exception:
                docs = []
        if not docs:
            path = getattr(self.config, "follow_lists_path", None)
            if path is not None:
                try:
                    docs = FollowListStore(path).all()
                except Exception:
                    docs = []
        lists: Dict[str, List[str]] = {}
        for doc in docs:
            if not isinstance(doc, dict):
                continue
            author = doc.get("author")
            authors = doc.get("authors")
            if isinstance(author, str) and isinstance(authors, list):
                lists[author] = [
                    entry for entry in authors if isinstance(entry, str)
                ]
        return lists

    def _fetch_missing_follow_lists(
        self,
        lists: Dict[str, List[str]],
        followed: Sequence[str],
        *,
        peers: Optional[Sequence[Any]] = None,
        errors: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, List[str]]:
        """Fetch the follow lists depth 2 needs that are not stored yet.

        Only authors whose list is not already stored are asked, only
        when the peer is known to the registry (no discovery during a
        sync), and at most ``MAX_SYNC_DEPTH_FETCHES`` fetches per sync.
        A fetch that fails is recorded in *errors* when given and never
        raises — the author simply contributes no second hop this round.
        """
        node = self.node
        request_follows = (
            getattr(node, "request_follows", None) if node is not None else None
        )
        if request_follows is None:
            return lists
        by_hash = {
            getattr(peer, "hexhash", ""): peer for peer in (peers or [])
        }
        fetched = dict(lists)
        attempts = 0
        for author in followed:
            if author in fetched or attempts >= MAX_SYNC_DEPTH_FETCHES:
                continue
            peer = by_hash.get(author)
            if peer is None:
                continue
            attempts += 1
            try:
                doc = request_follows(
                    peer, timeout=TAN_SYNC_DEPTH_FETCH_TIMEOUT
                )
            except Exception as exc:
                if errors is not None:
                    errors.append(
                        {
                            "peer": author,
                            "author": author,
                            "error": f"follow list fetch failed: {exc}",
                        }
                    )
                continue
            if isinstance(doc, dict) and isinstance(doc.get("authors"), list):
                fetched[author] = [
                    entry for entry in doc["authors"] if isinstance(entry, str)
                ]
        return fetched

    def _scope_authors(
        self,
        conn: sqlite3.Connection,
        *,
        peers: Optional[Sequence[Any]] = None,
        fetch: bool = False,
        errors: Optional[List[Dict[str, str]]] = None,
    ) -> List[str]:
        """Authors in scope for sync and the Following timeline.

        Followed authors, plus at depth 2 the authors on their
        published follow lists.  The display path (``fetch=False``)
        uses stored lists only, so reading the timeline never does
        network I/O; sync (``fetch=True``) asks known peers for lists
        it does not hold yet.  Blocked and muted authors are not
        filtered here — the sync loop's admission check and the
        timeline's display rules still exclude them, at every depth.
        """
        followed = [record["author"] for record in following(conn)]
        if self._sync_depth() < TAN_SYNC_DEPTH_MAX:
            return followed
        lists = self._stored_follow_lists()
        if fetch:
            lists = self._fetch_missing_follow_lists(
                lists, followed, peers=peers, errors=errors
            )
        return expand_follow_scope(followed, lists, depth=self._sync_depth())

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
        expires_at: Optional[float] = None,
        image: Optional[Dict[str, Any]] = None,
        location: Optional[Dict[str, Any]] = None,
        contact: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create a post, or a reply when *reply_to* names a parent post.

        When *expires_at* is given the post disappears from display
        surfaces once that time passes; see :func:`create_post`.
        A reply cannot carry an image — only a top-level post can.
        A reply cannot carry a location either, for the same reason.
        A reply cannot carry a contact card either, for the same reason.
        """
        conn = self.connect()
        try:
            if reply_to is not None:
                post = create_reply(
                    conn,
                    self.identity,
                    reply_to,
                    text,
                    name=self.name,
                    expires_at=expires_at,
                )
            else:
                post = create_post(
                    conn,
                    self.identity,
                    text,
                    name=self.name,
                    expires_at=expires_at,
                    image=image,
                    location=location,
                    contact=contact,
                )
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
        image: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create a quote of *target* carrying *text* as the comment.

        When *image* is given — the ``{sha256, size, mime, name}``
        descriptor a post carries — the quote carries it the same way;
        the bytes live pinned in the local media store, never in the
        quote.
        """
        conn = self.connect()
        try:
            record = create_quote(
                conn, self.identity, target, text, name=self.name, image=image
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
                authors = self._scope_authors(conn)
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

    def create_poll(
        self,
        question: str,
        options: Sequence[str],
        *,
        publish: bool = False,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
        expires_at: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Create a poll (see :func:`create_poll`), optionally publishing it."""
        conn = self.connect()
        try:
            record = create_poll(
                conn,
                self.identity,
                question,
                options,
                name=self.name,
                expires_at=expires_at,
            )
        finally:
            conn.close()
        if publish and self.node is not None:
            self.publish(record, targets=targets, ttl=ttl)
        return record

    def vote(
        self,
        target: str,
        option: int,
        *,
        publish: bool = False,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
    ) -> Dict[str, Any]:
        """Cast a vote for one option of a poll (see :func:`create_vote`)."""
        conn = self.connect()
        try:
            record = create_vote(
                conn, self.identity, target, option, name=self.name
            )
        finally:
            conn.close()
        if publish and self.node is not None:
            self.publish(record, targets=targets, ttl=ttl)
        return record

    def close_poll(
        self,
        target: str,
        *,
        publish: bool = False,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
    ) -> Dict[str, Any]:
        """Close one of your own polls early (see :func:`create_close_poll`)."""
        conn = self.connect()
        try:
            record = create_close_poll(
                conn, self.identity, target, name=self.name
            )
        finally:
            conn.close()
        if publish and self.node is not None:
            self.publish(record, targets=targets, ttl=ttl)
        return record

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

    def _stored_follow_lists(self) -> Dict[str, List[str]]:
        """Follow lists this node has already verified and stored.

        Prefers the running node's store (it picks up lists fetched
        since start); falls back to opening the store from the data
        directory.  Anything missing or unreadable means no follower
        is known yet — the caller sees an empty list rather than an
        error, because a follower whose list was never fetched simply
        has not been observed.
        """
        from ..follows import FollowListStore

        store = getattr(self.node, "follow_list_store", None)
        docs: List[Dict[str, Any]] = []
        if store is not None:
            try:
                docs = store.all()
            except Exception:
                docs = []
        if not docs:
            path = getattr(self.config, "follow_lists_path", None)
            if path is not None:
                try:
                    docs = FollowListStore(path).all()
                except Exception:
                    docs = []
        lists: Dict[str, List[str]] = {}
        for doc in docs:
            if not isinstance(doc, dict):
                continue
            author = doc.get("author")
            authors = doc.get("authors")
            if isinstance(author, str) and isinstance(authors, list):
                lists[author] = [
                    entry for entry in authors if isinstance(entry, str)
                ]
        return lists

    def followers(self) -> List[Dict[str, Any]]:
        """Authors who follow this node, from stored follow lists.

        See :func:`followers` for the record shape.  Names come from
        the attached node's peer registry when it knows the author.
        """
        conn = self.connect()
        try:
            registry = getattr(self.node, "registry", None)
            return followers(
                conn, self._stored_follow_lists(), self.author, registry=registry
            )
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
                authors = self._scope_authors(
                    conn, peers=peers, fetch=True, errors=errors
                )
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
                        # A page may carry purge records for posts it
                        # serves; restore them after the page's
                        # entries, then link up any entries that
                        # arrived unlinked while a record was missing.
                        # A malformed record is skipped, but its
                        # error is reported — the missing chain
                        # evidence is a real gap in what this page
                        # proved, not silent noise.
                        linked, record_errors = _restore_purged(
                            conn, response.get("purged") or []
                        )
                        _relink_purged_children(conn, linked)
                        for record_error in record_errors:
                            errors.append(
                                {
                                    "peer": getattr(peer, "hexhash", "?"),
                                    "author": author,
                                    "error": (
                                        "purge record "
                                        f"{record_error['hash'] or '?'}: "
                                        f"{record_error['error']}"
                                    ),
                                }
                            )
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
                    linked, record_errors = _restore_purged(
                        conn, response.get("purged") or []
                    )
                    _relink_purged_children(conn, linked)
                    for record_error in record_errors:
                        errors.append(
                            {
                                "peer": getattr(peer, "hexhash", "?"),
                                "author": "",
                                "error": (
                                    "purge record "
                                    f"{record_error['hash'] or '?'}: "
                                    f"{record_error['error']}"
                                ),
                            }
                        )
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
                purged = _purge_records_for_posts(conn, posts)
            finally:
                conn.close()
        except (ProtocolError, TanError) as exc:
            return response_error("invalid_feed_request", str(exc))
        while True:
            try:
                return response_ok(
                    posts=posts,
                    purged=purged,
                    count=len(posts),
                    author=None if all_authors else author,
                )
            except ProtocolError as exc:
                if len(posts) <= 1:
                    return response_error("feed_too_large", str(exc))
                posts = posts[:-1]
                # The page shrank, so re-filter the records to the
                # posts still served: a record whose parent post was
                # dropped is useless weight on a 64 KiB response.
                by_hash = {record["hash"]: record for record in purged}
                seeds = {str(post.get("prev") or "") for post in posts}
                seeds |= {str(post.get("target") or "") for post in posts}
                purged = _close_purge_seeds(by_hash, seeds)

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
