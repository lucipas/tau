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

The network protocol is three request paths served on the same
Reticulum destination as the rest of tau:

    /tau/channel/feed   {"channel": "...", "before": 1760000000.0, "limit": 20}
    /tau/channel/post   {"message": {...}, "ttl": 2}
    /tau/channel/react  {"reaction": {...}, "ttl": 2}
    /tau/channel/post   {"retraction": {...}, "ttl": 2}

``/tau/channel/feed`` reads cached messages for a channel.  It
also carries the channel's recent retract notices, so a peer that
was offline when a message was retracted still learns the retract
on its next sync instead of re-fetching the message.
``/tau/channel/post`` accepts a signed message and, when it is new,
gossips it to a small number of other peers — or a signed retract
notice, which deletes the message and is gossipped the same way.
Pulling a feed and pushing a message are deliberately separate: a
node can read a channel it joined even when the author of a message
is offline, as long as any peer still has that message cached.

An edit is a second signed record that points at a message by hash::

    {
      "v": 1,
      "kind": "channel-edit",
      "channel": "mesh-meetup",
      "target": "<64 hex digest of the original message>",
      "text": "see you at the park — 6pm, not 5",
      "author": "<32 hex destination hash>",
      "key": "<128 hex public key>",
      "created_at": 1760000000.0,
      "hash": "<64 hex sha256(canonical body)>",
      "sig": "<128 hex Ed25519 signature over the hash>"
    }

Only the message's author can edit it, and only the newest
author-matching edit changes what a reader sees: the original
message is never rewritten — its hash must stay valid so peers can
still verify and store it — so the feed serves original messages
plus the channel's newest edit notices, and readers overlay the
latest edit on display.  An edit notice for a message that has not
arrived yet is stored as pending and applies the moment the message
does; a notice signed by anyone but the message's author is stored
but never applies.


A retract notice is a signed record of its own::

    {
      "v": 1,
      "kind": "channel-retract",
      "channel": "mesh-meetup",
      "hash": "<64 hex digest of the retracted message>",
      "author": "<32 hex destination hash>",
      "key": "<128 hex public key>",
      "created_at": 1760000000.0,
      "sig": "<128 hex Ed25519 signature over the notice hash>"
    }

Only the message's author can retract it: the notice is only
applied when its signer is the author of the message it names, so
a notice signed by anyone else is stored as a tombstone but never
deletes anything — a forged retract cannot censor another peer's
message.  A tombstone is remembered even when the message is not
held yet, so a message that arrives later from another peer is
refused instead of re-appearing.

A reply is an ordinary message with an optional ``reply_to`` digest
naming the parent message in the same channel.  The digest is part
of the signed body when present, so a relay cannot re-point a reply
at another message; messages without one verify exactly as before.
A reply whose parent has not arrived yet still stores and verifies —
readers resolve the parent locally and show its digest until the
parent lands.

A reaction is a separate signed record naming a stored message by
hash::

    {
      "v": 1,
      "channel": "mesh-meetup",
      "target": "<64 hex message hash>",
      "reactor": "<32 hex destination hash>",
      "key": "<128 hex public key>",
      "name": "alice",
      "reaction": "👍",
      "created_at": 1760000000.0,
      "hash": "<64 hex sha256(canonical body)>",
      "sig": "<128 hex Ed25519 signature over the hash>"
    }

One reaction per reactor per message: a new reaction from the same
reactor replaces the old one, and an empty ``reaction`` clears it.
The target must already be stored in the same channel when reacting
locally — a reaction to a message you have never seen would leave
readers with a dangling reference.  A reaction arriving from a peer
whose target has not landed yet still stores and verifies; readers
show it once the message arrives.  Reactions ride
``/tau/channel/react`` with the same TTL/fanout as posts, and a feed
page carries the reactions for the messages on that page, so sync
pulls reactions along with history.

Membership is local-only and never gossiped: joining records the
name in the local store, leaving drops it (cached messages stay
until the node stops serving them — a leave is about what *you*
follow, not a retraction).  Blocked authors are refused at the door
like everywhere else in tau: a message from a blocked destination is
dropped, never stored, never re-gossipped.

Polls are ordinary messages with one extra field: a poll carries
``poll_options`` (the question is the text), a vote carries
``poll_vote`` (the poll's author plus ``created_at`` and the chosen
option index), and a close carries ``poll_close`` (the same target).
The extra field is covered by the message hash when present, so a
relay cannot strip or swap it without breaking the signature, and
votes and closes flow through the same feed, post and sync paths as
chat — tallies are derived from stored messages (see
:mod:`tau.channel_polls`), and vote/close records are hidden from
listings by the callers that render them.

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
from .disappearing import channel_default_for
from .identity import load_public_key
from .muted import muted_channels_for
from .protocol import (
    APP_NAME,
    ASPECT,
    MAX_GROUP_TOPIC_LENGTH,
    MAX_MAIL_TTL,
    MAX_MESSAGE_CHARS,
    MAX_REQUEST_BYTES,
    ProtocolError,
    clean_group_poll_target,
    clean_group_poll_vote,
    clean_group_topic,
    clean_location,
    clean_reaction,
    decode,
    encode,
    normalize_message,
    response_error,
    response_ok,
    sanitize_name,
)
from .tangents.tan import PostError, _clean_poll_options
from .util import ensure_dir

log = logging.getLogger(__name__)

CHANNEL_VERSION = 1
CHANNEL_FEED_PATH = "/tau/channel/feed"
CHANNEL_POST_PATH = "/tau/channel/post"
CHANNEL_REACT_PATH = "/tau/channel/react"

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
# Bound on how many retract notices one channel may hold on disk:
# the same bound as messages, so a peer spraying retracts cannot
# grow the store without limit either.
MAX_RETRACTIONS_PER_CHANNEL = 500
# How many of a channel's newest retract notices ride on one feed
# response, so a peer syncing history learns retracts it missed.
MAX_FEED_RETRACTIONS = 50
DEFAULT_CHANNEL_LIMIT = 20
MAX_GOSSIP_TTL = 3
GOSSIP_FANOUT = 3
MAX_SYNC_PAGES = 32
# Bound on how many edit notices one channel may hold: an edit is
# small, but a peer spamming re-edits must not grow the store
# without limit.  The newest 500 win; older ones are pruned.
MAX_EDITS_PER_CHANNEL = 500
# How many of a channel's newest edit notices a feed response
# carries, so a syncing peer learns the current text without a
# second round trip.
MAX_FEED_EDITS = 50

# Channel names are joined by exact name, so the character set is
# tight: start alphanumerical, then letters, digits, dots, dashes and
# underscores.  No spaces, no slashes — a name must survive a CLI
# argument, a URL query and a gossip payload unchanged.
_CHANNEL_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")

# Fields covered by the message hash.  The channel is part of the
# signed body, so a relay cannot move a message into another room
# without breaking the hash.  ``reply_to`` is covered too, when a
# message has one — _canonical_body adds it — so a reply cannot be
# re-pointed at another message either.  ``location`` is covered the
# same way when a message pins a place: a relay that strips the pin
# breaks the hash exactly like one that edits the text.
HASH_FIELDS = ("v", "channel", "author", "key", "name", "text", "created_at")

# A channel message's expiry is deliberately NOT one of the hash
# fields: ``expires_at`` rides as an unsigned advisory field, so an
# older node verifies, stores and serves an expiring message exactly
# like any other (it just never expires it) and there is no interop
# break.  Honest nodes enforce it; a modified peer can keep a copy,
# the same as with a DM — disappearing is a hygiene preference, not
# a security boundary.  The cap mirrors ``tau send --expire``.
MAX_CHANNEL_EXPIRE_SECONDS = MAX_MAIL_TTL

# Poll fields ride on the message and are covered by the hash when
# present (see _canonical_body): a poll carries poll_options, a vote
# carries poll_vote, a close carries poll_close — at most one per
# message, like a group poll.
POLL_FIELDS = ("poll_options", "poll_vote", "poll_close")

# Fields covered by the topic hash: the same shape as a message,
# minus the display name (a topic is metadata, not a post) and with
# the topic text in place of the message text.
TOPIC_FIELDS = ("v", "channel", "author", "key", "topic", "updated_at")

# Fields covered by a reaction's hash: the channel and the target
# keep a reaction in its room and on its message, so a relay cannot
# move it or re-point it without breaking the hash.
REACTION_HASH_FIELDS = (
    "v",
    "channel",
    "target",
    "reactor",
    "key",
    "name",
    "reaction",
    "created_at",
)

# Minimum hex characters for a message-hash prefix to resolve: short
# enough to type, long enough that a collision is a real ambiguity
# rather than a typo.
MIN_DIGEST_PREFIX = 8
# Fields covered by an edit notice's hash.  The target digest pins
# the notice to one message, so it cannot be replayed onto another.
EDIT_HASH_FIELDS = (
    "v",
    "kind",
    "channel",
    "target",
    "text",
    "author",
    "key",
    "created_at",
)
EDIT_KIND = "channel-edit"

# Fields covered by a retract notice's hash.  ``kind`` pins the
# signed body to a retract, so a notice can never be mistaken for a
# message, and ``hash`` names the message being retracted.
RETRACTION_KIND = "channel-retract"
RETRACT_HASH_FIELDS = ("v", "kind", "channel", "hash", "author", "key", "created_at")

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
    "expires_at",
    "received_at",
    "sig",
    "poll_options",
    "poll_vote",
    "poll_close",
    "reply_to",
    "location",
)

# Column order for reaction rows.  A reaction is keyed by
# (target, reactor): one live reaction per reactor per message.
_REACTION_COLUMNS = (
    "v",
    "hash",
    "channel",
    "target",
    "reactor",
    "key",
    "name",
    "reaction",
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
    # Poll fields are covered too, when a message carries one: a
    # relay that strips a poll's options (or a vote's target) breaks
    # the hash exactly like one that edits the text.
    for field in POLL_FIELDS:
        if message.get(field) is not None:
            body[field] = message[field]
    reply_to = message.get("reply_to")
    if reply_to:
        # Covered by the hash when present, so a reply cannot be
        # re-pointed; absent for plain messages, whose hashes — and
        # verification — are unchanged.
        body["reply_to"] = reply_to
    location = message.get("location")
    if location is not None:
        # Covered by the hash when present, so a relay cannot strip
        # a pin; absent for plain messages, whose hashes — and
        # verification — are unchanged.
        body["location"] = location
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

    # An expiry is advisory and unsigned (see MAX_CHANNEL_EXPIRE_SECONDS):
    # validate its shape and range, but never let it into the hash.
    raw_expires_at = mesg.get("expires_at")
    expires_at = None
    if raw_expires_at is not None:
        expires_at = _require_timestamp(raw_expires_at, "expires_at")
        if expires_at <= created_at:
            raise ChannelError("channel message expires_at must be after created_at")
        if expires_at > created_at + MAX_CHANNEL_EXPIRE_SECONDS:
            raise ChannelError(
                "channel message expires_at is more than 30d after created_at"
            )
    poll_options = mesg.get("poll_options")
    poll_vote = mesg.get("poll_vote")
    poll_close = mesg.get("poll_close")
    if sum(field is not None for field in (poll_options, poll_vote, poll_close)) > 1:
        raise ChannelError(
            "a channel message can carry only one of poll_options, "
            "poll_vote, poll_close"
        )
    cleaned_options: Optional[List[str]] = None
    if poll_options is not None:
        try:
            cleaned_options = _clean_poll_options(poll_options)
        except PostError as exc:
            raise ChannelError(f"invalid channel poll options: {exc}") from exc
    cleaned_vote: Optional[Dict[str, Any]] = None
    if poll_vote is not None:
        try:
            cleaned_vote = clean_group_poll_vote(poll_vote)
        except ProtocolError as exc:
            raise ChannelError(f"invalid channel poll vote: {exc}") from exc
    cleaned_close: Optional[Dict[str, Any]] = None
    if poll_close is not None:
        try:
            cleaned_close = clean_group_poll_target(poll_close)
        except ProtocolError as exc:
            raise ChannelError(f"invalid channel poll close: {exc}") from exc
    raw_reply_to = mesg.get("reply_to", "")
    if raw_reply_to in (None, ""):
        reply_to = ""
    else:
        reply_to = _require_hex(raw_reply_to, HASH_HEX_LENGTH, "reply_to")

    # A pin belongs on a plain top-level message: a reply already
    # points at its parent, and a poll question carries its own
    # structured payload — the CLI never produces either combination.
    raw_location = mesg.get("location")
    cleaned_location = None
    if raw_location is not None:
        cleaned_location = clean_location(raw_location)
        if cleaned_location is None:
            raise ChannelError(
                "channel message location must have a latitude -90..90 "
                "and a longitude -180..180"
            )
        if reply_to or any(
            mesg.get(field) is not None for field in POLL_FIELDS
        ):
            raise ChannelError(
                "a channel location pin can only be attached to a "
                "top-level message, not a reply or a poll"
            )

    message = {
        "v": CHANNEL_VERSION,
        "channel": channel,
        "author": author,
        "key": key,
        "name": name,
        "text": text,
        "created_at": created_at,
        "expires_at": expires_at,
        "reply_to": reply_to,
    }
    if cleaned_options is not None:
        message["poll_options"] = cleaned_options
    if cleaned_vote is not None:
        message["poll_vote"] = cleaned_vote
    if cleaned_close is not None:
        message["poll_close"] = cleaned_close
    if cleaned_location is not None:
        message["location"] = cleaned_location
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
    canonical = {
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
    # The expiry is part of the public message when set; a message
    # without one keeps the exact shape older readers expect.
    if message.get("expires_at") is not None:
        canonical["expires_at"] = message["expires_at"]
    for field in POLL_FIELDS:
        if message.get(field) is not None:
            canonical[field] = message[field]
    reply_to = message.get("reply_to")
    if reply_to:
        # Plain messages keep the old wire shape: the field appears
        # only on replies, matching how the hash treats it.
        canonical["reply_to"] = reply_to
    if message.get("location") is not None:
        # Plain messages keep the old wire shape: the pin appears
        # only on located messages, matching how the hash treats it.
        canonical["location"] = message["location"]
    return canonical


# -- edit notices ------------------------------------------------------------

# Column order used by every edit SELECT so rows convert consistently.
_EDIT_COLUMNS = (
    "v",
    "kind",
    "hash",
    "channel",
    "target",
    "text",
    "author",
    "key",
    "created_at",
    "received_at",
    "sig",
)


def _canonical_edit_body(edit: Dict[str, Any]) -> bytes:
    body = {field: edit[field] for field in EDIT_HASH_FIELDS}
    return json.dumps(
        body,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def hash_edit(edit: Dict[str, Any]) -> str:
    """Hash an edit notice: lowercase hex sha256 of its body."""
    if not isinstance(edit, dict):
        raise ChannelError("a channel edit must be a JSON object")
    missing = [field for field in EDIT_HASH_FIELDS if field not in edit]
    if missing:
        raise ChannelError(
            f"channel edit is missing fields: {', '.join(missing)}"
        )
    return hashlib.sha256(_canonical_edit_body(edit)).hexdigest()


def _check_edit_wire_size(edit: Dict[str, Any]) -> None:
    """Reject an edit whose publish envelope exceeds the budget."""
    envelope = dict(edit)
    envelope.setdefault("hash", "0" * HASH_HEX_LENGTH)
    envelope.setdefault("sig", "0" * SIG_HEX_LENGTH)
    try:
        encode(
            {"edit": envelope, "ttl": MAX_GOSSIP_TTL},
            max_bytes=MAX_REQUEST_BYTES,
        )
    except ProtocolError as exc:
        raise ChannelError(
            f"channel edit is too large to publish: {exc}"
        ) from exc


def _normalize_edit(edit: Dict[str, Any], *, now: Optional[float] = None) -> Dict[str, Any]:
    """Validate and normalise the signed part of an edit notice."""
    if not isinstance(edit, dict):
        raise ChannelError("a channel edit must be a JSON object")

    version = edit.get("v", CHANNEL_VERSION)
    if isinstance(version, bool) or version != CHANNEL_VERSION:
        raise ChannelError(f"unsupported channel edit version: {version!r}")
    if edit.get("kind") != EDIT_KIND:
        raise ChannelError(f"unsupported channel edit kind: {edit.get('kind')!r}")

    channel = clean_channel_name(edit.get("channel"))
    target = _require_hex(edit.get("target"), HASH_HEX_LENGTH, "target")
    author = _require_hex(edit.get("author"), AUTHOR_HEX_LENGTH, "author")
    key = _require_hex(edit.get("key"), KEY_HEX_LENGTH, "key")

    raw_text = edit.get("text")
    try:
        text = normalize_message(raw_text)
    except ProtocolError as exc:
        raise ChannelError(f"invalid channel edit text: {exc}") from exc
    if text != raw_text:
        raise ChannelError("channel edit text is not normalised")

    created_at = _require_timestamp(edit.get("created_at"), "created_at")
    if now is not None and created_at > float(now) + 86400.0:
        raise ChannelError("channel edit timestamp is too far in the future")

    notice = {
        "v": CHANNEL_VERSION,
        "kind": EDIT_KIND,
        "channel": channel,
        "target": target,
        "text": text,
        "author": author,
        "key": key,
        "created_at": created_at,
    }
    _check_edit_wire_size(notice)
    return notice


def verify_edit(edit: Dict[str, Any], *, now: Optional[float] = None) -> Dict[str, Any]:
    """Validate a complete edit notice, including hash and signature."""
    notice = _normalize_edit(edit, now=now)

    digest = edit.get("hash")
    expected = hash_edit(notice)
    if not isinstance(digest, str) or digest.strip().lower() != expected:
        raise ChannelError("channel edit hash does not match its contents")
    notice["hash"] = expected

    signature = _require_hex(edit.get("sig"), SIG_HEX_LENGTH, "sig")
    notice["sig"] = signature

    identity = _identity_from_message(notice)
    author = _author_from_identity(identity)
    if author != notice["author"]:
        raise ChannelError("public key does not match the edit author")
    try:
        valid = identity.validate(bytes.fromhex(signature), bytes.fromhex(expected))
    except Exception as exc:  # pragma: no cover - depends on RNS internals
        raise ChannelError(f"could not check signature: {exc}") from exc
    if not valid:
        raise ChannelError("channel edit signature is invalid")
    return notice


def _canonical_edit(edit: Dict[str, Any]) -> Dict[str, Any]:
    """Return the public, canonical representation of an edit notice."""
    return {
        "v": edit["v"],
        "kind": edit["kind"],
        "hash": edit["hash"],
        "channel": edit["channel"],
        "target": edit["target"],
        "text": edit["text"],
        "author": edit["author"],
        "key": edit["key"],
        "created_at": edit["created_at"],
        "sig": edit["sig"],
    }


def _row_to_edit(row: sqlite3.Row) -> Dict[str, Any]:
    return _canonical_edit(dict(row))


def get_edit(conn: sqlite3.Connection, digest: str) -> Optional[Dict[str, Any]]:
    """Return a stored edit notice by hash, or ``None``."""
    row = conn.execute(
        f"SELECT {', '.join(_EDIT_COLUMNS)} FROM edits WHERE hash = ?",
        (digest,),
    ).fetchone()
    return _row_to_edit(row) if row is not None else None


def list_edits(
    conn: sqlite3.Connection,
    channel: str,
    *,
    limit: int = MAX_FEED_EDITS,
    exclude_authors: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Newest edit notices for *channel*, newest first."""
    name = clean_channel_name(channel)
    count = max(1, min(int(limit), MAX_EDITS_PER_CHANNEL))
    clauses = ["channel = ?"]
    params: List[Any] = [name]
    excluded = [author for author in (exclude_authors or []) if author]
    if excluded:
        placeholders = ", ".join("?" for _ in excluded)
        clauses.append(f"author NOT IN ({placeholders})")
        params.extend(excluded)
    rows = conn.execute(
        f"SELECT {', '.join(_EDIT_COLUMNS)} FROM edits "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY created_at DESC, hash DESC LIMIT ?",
        (*params, count),
    ).fetchall()
    return [_row_to_edit(row) for row in rows]


def latest_edit_for(
    conn: sqlite3.Connection, target: str
) -> Optional[Dict[str, Any]]:
    """Newest stored edit notice for one message digest, or ``None``."""
    row = conn.execute(
        f"SELECT {', '.join(_EDIT_COLUMNS)} FROM edits WHERE target = ? "
        "ORDER BY created_at DESC, hash DESC LIMIT 1",
        (target,),
    ).fetchone()
    return _row_to_edit(row) if row is not None else None


def store_edit(
    conn: sqlite3.Connection,
    edit: Dict[str, Any],
    *,
    now: Optional[float] = None,
    verify: bool = True,
) -> Tuple[Dict[str, Any], bool]:
    """Validate and store an edit notice, returning ``(edit, is_new)``.

    A notice is stored even when its author is not the message's
    author — it simply never applies (see
    :func:`_apply_edits_to_messages`).  Storing it keeps the record
    for ``tau channel edits`` and for re-sync, and costs nothing
    beyond the per-channel cap.  A notice for a message that has not
    arrived yet is pending: it applies the moment the message does.
    """
    timestamp = float(now if now is not None else time.time())
    if verify:
        notice = verify_edit(edit, now=timestamp)
    else:
        base = _normalize_edit(edit, now=timestamp)
        notice = dict(base)
        notice["hash"] = hash_edit(base)
        notice["sig"] = _require_hex(edit.get("sig"), SIG_HEX_LENGTH, "sig")

    digest = notice["hash"]
    existing = get_edit(conn, digest)
    if existing is not None:
        return existing, False

    try:
        with conn:
            conn.execute(
                "INSERT INTO edits(v, kind, hash, channel, target, text, "
                "author, key, created_at, received_at, sig) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    CHANNEL_VERSION,
                    EDIT_KIND,
                    digest,
                    notice["channel"],
                    notice["target"],
                    notice["text"],
                    notice["author"],
                    notice["key"],
                    notice["created_at"],
                    timestamp,
                    notice["sig"],
                ),
            )
            conn.execute(
                "DELETE FROM edits WHERE channel = ? AND hash NOT IN ("
                "SELECT hash FROM edits WHERE channel = ? "
                "ORDER BY created_at DESC, hash DESC "
                f"LIMIT {MAX_EDITS_PER_CHANNEL})",
                (notice["channel"], notice["channel"]),
            )
    except sqlite3.IntegrityError:
        existing = get_edit(conn, digest)
        if existing is None:
            raise
        return existing, False

    record = get_edit(conn, digest)
    if record is None:
        return _canonical_edit(notice), False
    return record, True


def resolve_message_digest(
    conn: sqlite3.Connection, channel: str, digest: str
) -> str:
    """Resolve a full message digest or a hex prefix to a full digest.

    A prefix must be at least 4 hex characters and match exactly one
    stored message in *channel*; an ambiguous or unknown prefix is an
    error, so a typo never edits the wrong message.
    """
    name = clean_channel_name(channel)
    if not isinstance(digest, str):
        raise ChannelError("message digest must be a hex string")
    text = digest.strip().lower()
    if len(text) < 4 or any(ch not in "0123456789abcdef" for ch in text):
        raise ChannelError(
            "message digest must be at least 4 hex characters"
        )
    if len(text) == HASH_HEX_LENGTH:
        row = _fetch_message(conn, text)
        if row is None or row["channel"] != name:
            raise ChannelError(f"unknown message: {digest!r}")
        return text
    if len(text) > HASH_HEX_LENGTH:
        raise ChannelError("message digest is too long")
    rows = conn.execute(
        "SELECT hash FROM messages WHERE channel = ? AND hash LIKE ?",
        (name, text + "%"),
    ).fetchall()
    if not rows:
        raise ChannelError(f"unknown message: {digest!r}")
    if len(rows) > 1:
        raise ChannelError(
            f"ambiguous message digest prefix {digest!r}: "
            f"matches {len(rows)} messages"
        )
    return rows[0]["hash"]


def current_message_text(
    conn: sqlite3.Connection, message: Dict[str, Any]
) -> Tuple[str, Optional[float]]:
    """A message's current text: its latest author-matching edit wins.

    Returns ``(text, edited_at)``; ``edited_at`` is ``None`` when no
    edit applies, so callers can tell an untouched message from an
    edited one.  Only an edit signed by the message's own author
    applies — a notice from anyone else is ignored.
    """
    notice = latest_edit_for(conn, message["hash"])
    if notice is not None and notice["author"] == message["author"]:
        return notice["text"], notice["created_at"]
    return message["text"], None


def _apply_edits_to_messages(
    conn: sqlite3.Connection, messages: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Overlay the latest author-matching edit onto each message.

    The stored message rows are never rewritten — their hashes must
    stay valid for peers — so the overlay happens on the returned
    dicts only: an edited message carries its new ``text`` plus
    ``edited: True`` and ``edited_at``.  Untouched messages pass
    through unchanged.
    """
    if not messages:
        return messages
    targets = [message["hash"] for message in messages]
    placeholders = ", ".join("?" for _ in targets)
    rows = conn.execute(
        f"SELECT {', '.join(_EDIT_COLUMNS)} FROM edits "
        f"WHERE target IN ({placeholders}) "
        "ORDER BY created_at DESC, hash DESC",
        targets,
    ).fetchall()
    newest: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        notice = _row_to_edit(row)
        newest.setdefault(notice["target"], notice)
    applied = []
    for message in messages:
        notice = newest.get(message["hash"])
        if notice is not None and notice["author"] == message["author"]:
            shown = dict(message)
            shown["text"] = notice["text"]
            shown["edited"] = True
            shown["edited_at"] = notice["created_at"]
            applied.append(shown)
        else:
            applied.append(message)
    return applied


def create_edit(
    conn: sqlite3.Connection,
    identity: RNS.Identity,
    channel: str,
    target: str,
    text: str,
    *,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Create, sign and store an edit notice owned by *identity*.

    Only the message's author may edit it.  Re-editing to the text
    the message already shows returns the stored notice instead of
    signing a duplicate; editing to the message's own current text
    with no notice on file is an error — there is nothing to change.
    """
    timestamp = round(float(now if now is not None else time.time()), 3)
    name = clean_channel_name(channel)
    digest = resolve_message_digest(conn, name, target)
    message = get_message(conn, digest)
    if message is None:  # pragma: no cover - resolver already checked
        raise ChannelError(f"unknown message: {target!r}")
    author = _author_from_identity(identity)
    if author != message["author"]:
        raise ChannelError("only the message author can edit it")
    current, _ = current_message_text(conn, message)
    try:
        new_text = normalize_message(text)
    except ProtocolError as exc:
        raise ChannelError(f"invalid channel edit text: {exc}") from exc
    if new_text == current:
        notice = latest_edit_for(conn, digest)
        if notice is not None and notice["author"] == author:
            return notice
        raise ChannelError("message already has that text")
    body = {
        "v": CHANNEL_VERSION,
        "kind": EDIT_KIND,
        "channel": name,
        "target": digest,
        "text": new_text,
        "author": author,
        "key": identity.get_public_key().hex(),
        "created_at": timestamp,
    }
    digest_hash = hash_edit(body)
    notice = dict(body)
    notice["hash"] = digest_hash
    notice["sig"] = identity.sign(bytes.fromhex(digest_hash)).hex()
    _check_edit_wire_size(notice)
    record, _ = store_edit(conn, notice, now=timestamp)
    return record


# -- retract notices -------------------------------------------------------


def _canonical_retraction_body(notice: Dict[str, Any]) -> bytes:
    body = {field: notice[field] for field in RETRACT_HASH_FIELDS}
    return json.dumps(
        body,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def hash_retraction(notice: Dict[str, Any]) -> str:
    """Hash a retract notice: lowercase hex sha256 of its signed body."""
    if not isinstance(notice, dict):
        raise ChannelError("a channel retraction must be a JSON object")
    missing = [field for field in RETRACT_HASH_FIELDS if field not in notice]
    if missing:
        raise ChannelError(
            f"channel retraction is missing fields: {', '.join(missing)}"
        )
    return hashlib.sha256(_canonical_retraction_body(notice)).hexdigest()


def _normalize_retraction(
    notice: Dict[str, Any], *, now: Optional[float] = None
) -> Dict[str, Any]:
    """Validate and normalise the signed part of a retract notice."""
    if not isinstance(notice, dict):
        raise ChannelError("a channel retraction must be a JSON object")

    version = notice.get("v", CHANNEL_VERSION)
    if isinstance(version, bool) or version != CHANNEL_VERSION:
        raise ChannelError(f"unsupported channel retraction version: {version!r}")

    kind = notice.get("kind")
    if kind != RETRACTION_KIND:
        raise ChannelError(f"unsupported channel retraction kind: {kind!r}")

    channel = clean_channel_name(notice.get("channel"))
    author = _require_hex(notice.get("author"), AUTHOR_HEX_LENGTH, "author")
    key = _require_hex(notice.get("key"), KEY_HEX_LENGTH, "key")
    digest = _require_hex(notice.get("hash"), HASH_HEX_LENGTH, "hash")
    created_at = _require_timestamp(notice.get("created_at"), "created_at")
    if now is not None and created_at > float(now) + 86400.0:
        raise ChannelError("channel retraction timestamp is too far in the future")

    return {
        "v": CHANNEL_VERSION,
        "kind": RETRACTION_KIND,
        "channel": channel,
        "hash": digest,
        "author": author,
        "key": key,
        "created_at": created_at,
    }


# -- reactions ---------------------------------------------------------------

# A reaction is a signed record in its own right, so it can travel
# and be verified without its target message: the hash covers the
# channel, the target and the token, and one row per (target,
# reactor) keeps the latest reaction from each reactor.


def _canonical_reaction_body(reaction: Dict[str, Any]) -> bytes:
    body = {field: reaction[field] for field in REACTION_HASH_FIELDS}
    return json.dumps(
        body,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def hash_reaction(reaction: Dict[str, Any]) -> str:
    """Hash a channel reaction: lowercase hex sha256 of its body."""
    if not isinstance(reaction, dict):
        raise ChannelError("a channel reaction must be a JSON object")
    missing = [field for field in REACTION_HASH_FIELDS if field not in reaction]
    if missing:
        raise ChannelError(
            f"channel reaction is missing fields: {', '.join(missing)}"
        )
    return hashlib.sha256(_canonical_reaction_body(reaction)).hexdigest()


def _check_reaction_wire_size(reaction: Dict[str, Any]) -> None:
    """Reject a reaction whose publish envelope exceeds the budget."""
    envelope = dict(reaction)
    envelope.setdefault("hash", "0" * HASH_HEX_LENGTH)
    envelope.setdefault("sig", "0" * SIG_HEX_LENGTH)
    try:
        encode(
            {"reaction": envelope, "ttl": MAX_GOSSIP_TTL},
            max_bytes=MAX_REQUEST_BYTES,
        )
    except ProtocolError as exc:
        raise ChannelError(
            f"channel reaction is too large to publish: {exc}"
        ) from exc


def _normalize_reaction(
    mesg: Dict[str, Any], *, now: Optional[float] = None
) -> Dict[str, Any]:
    """Validate and normalise the signed part of a channel reaction."""
    if not isinstance(mesg, dict):
        raise ChannelError("a channel reaction must be a JSON object")

    version = mesg.get("v", CHANNEL_VERSION)
    if isinstance(version, bool) or version != CHANNEL_VERSION:
        raise ChannelError(f"unsupported channel reaction version: {version!r}")

    channel = clean_channel_name(mesg.get("channel"))
    target = _require_hex(mesg.get("target"), HASH_HEX_LENGTH, "target")
    reactor = _require_hex(mesg.get("reactor"), AUTHOR_HEX_LENGTH, "reactor")
    key = _require_hex(mesg.get("key"), KEY_HEX_LENGTH, "key")

    raw_name = mesg.get("name", "")
    name = sanitize_name(raw_name)
    if name != raw_name:
        raise ChannelError("channel reaction name is not normalised")

    try:
        reaction = clean_reaction(mesg.get("reaction"))
    except ProtocolError as exc:
        # clean_reaction raises ProtocolError, but every channel
        # handler catches ChannelError: wrap it so a malformed peer
        # entry is a validation failure, never an unhandled error.
        raise ChannelError(f"invalid channel reaction: {exc}") from exc

    created_at = _require_timestamp(mesg.get("created_at"), "created_at")
    if now is not None and created_at > float(now) + 86400.0:
        raise ChannelError("channel reaction timestamp is too far in the future")

    record = {
        "v": CHANNEL_VERSION,
        "channel": channel,
        "target": target,
        "reactor": reactor,
        "key": key,
        "name": name,
        "reaction": reaction,
        "created_at": created_at,
    }
    _check_reaction_wire_size(record)
    return record


def verify_reaction(
    mesg: Dict[str, Any], *, now: Optional[float] = None
) -> Dict[str, Any]:
    """Validate a complete channel reaction, including hash and signature."""
    reaction = _normalize_reaction(mesg, now=now)

    digest = mesg.get("hash")
    expected = hash_reaction(reaction)
    if not isinstance(digest, str) or digest.strip().lower() != expected:
        raise ChannelError("channel reaction hash does not match its contents")
    reaction["hash"] = expected

    signature = _require_hex(mesg.get("sig"), SIG_HEX_LENGTH, "sig")
    reaction["sig"] = signature

    identity = _identity_from_message(reaction)
    reactor = _author_from_identity(identity)
    if reactor != reaction["reactor"]:
        raise ChannelError("public key does not match the reaction reactor")
    try:
        valid = identity.validate(bytes.fromhex(signature), bytes.fromhex(expected))
    except Exception as exc:  # pragma: no cover - depends on RNS internals
        raise ChannelError(f"could not check signature: {exc}") from exc
    if not valid:
        raise ChannelError("channel reaction signature is invalid")
    return reaction


def _canonical_reaction(reaction: Dict[str, Any]) -> Dict[str, Any]:
    """Return the public, canonical representation of a reaction."""
    return {
        "v": reaction["v"],
        "channel": reaction["channel"],
        "target": reaction["target"],
        "reactor": reaction["reactor"],
        "key": reaction["key"],
        "name": reaction["name"],
        "reaction": reaction["reaction"],
        "created_at": reaction["created_at"],
        "hash": reaction["hash"],
        "sig": reaction["sig"],
    }


def summarize_reactions(
    reactions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Group reactions by token: ``[{reaction, count, reactors}]``.

    ``reactors`` names the reactors in the order they reacted, so a
    reader can show who reacted without re-sorting.  Tokens sort by
    count descending, then token, so the most-reacted token leads.
    """
    grouped: Dict[str, Dict[str, Any]] = {}
    for record in reactions:
        token = str(record.get("reaction", ""))
        if not token:
            continue
        entry = grouped.setdefault(
            token, {"reaction": token, "count": 0, "reactors": []}
        )
        entry["count"] += 1
        entry["reactors"].append(str(record.get("name") or "anonymous"))
    return sorted(
        grouped.values(), key=lambda item: (-item["count"], item["reaction"])
    )


# -- storage ---------------------------------------------------------------
def verify_retraction(
    notice: Dict[str, Any], *, now: Optional[float] = None
) -> Dict[str, Any]:
    """Validate a complete retract notice, including its signature."""
    retraction = _normalize_retraction(notice, now=now)

    digest = hash_retraction(retraction)
    signature = _require_hex(notice.get("sig"), SIG_HEX_LENGTH, "sig")
    retraction["sig"] = signature

    identity = _identity_from_message(retraction)
    author = _author_from_identity(identity)
    if author != retraction["author"]:
        raise ChannelError("public key does not match the retraction author")
    try:
        valid = identity.validate(bytes.fromhex(signature), bytes.fromhex(digest))
    except Exception as exc:  # pragma: no cover - depends on RNS internals
        raise ChannelError(f"could not check signature: {exc}") from exc
    if not valid:
        raise ChannelError("channel retraction signature is invalid")
    return retraction


def _canonical_retraction(notice: Dict[str, Any]) -> Dict[str, Any]:
    """Return the public, canonical representation of a retract notice."""
    return {
        "v": notice["v"],
        "kind": notice["kind"],
        "channel": notice["channel"],
        "hash": notice["hash"],
        "author": notice["author"],
        "key": notice["key"],
        "created_at": notice["created_at"],
        "sig": notice["sig"],
    }
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
    expires_at REAL,
    received_at REAL NOT NULL,
    sig TEXT NOT NULL,
    poll_options TEXT,
    poll_vote TEXT,
    poll_close TEXT,
    reply_to TEXT NOT NULL DEFAULT '',
    location TEXT
);

CREATE INDEX IF NOT EXISTS messages_channel_created
    ON messages(channel, created_at DESC, hash);

-- One live reaction per reactor per message: a new reaction from
-- the same reactor replaces the old one, and an empty ``reaction``
-- is a retraction that readers filter out.  The hash is stored so
-- a re-sent copy is recognised without re-verifying.
CREATE TABLE IF NOT EXISTS reactions (
    v INTEGER NOT NULL DEFAULT 1,
    hash TEXT NOT NULL,
    channel TEXT NOT NULL,
    target TEXT NOT NULL,
    reactor TEXT NOT NULL,
    key TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    reaction TEXT NOT NULL,
    created_at REAL NOT NULL,
    received_at REAL NOT NULL,
    sig TEXT NOT NULL,
    PRIMARY KEY (target, reactor)
);

CREATE INDEX IF NOT EXISTS reactions_channel_target
    ON reactions(channel, target, created_at);
CREATE TABLE IF NOT EXISTS retractions (
    hash TEXT NOT NULL,
    channel TEXT NOT NULL,
    author TEXT NOT NULL,
    key TEXT NOT NULL,
    created_at REAL NOT NULL,
    received_at REAL NOT NULL,
    sig TEXT NOT NULL,
    PRIMARY KEY (hash, author)
);

CREATE INDEX IF NOT EXISTS retractions_channel_created
    ON retractions(channel, created_at DESC, hash);


CREATE TABLE IF NOT EXISTS memberships (
    channel TEXT PRIMARY KEY,
    joined_at REAL NOT NULL
);

-- Last-read cursor per joined channel: the newest message the owner
-- has marked read, as (created_at, hash).  Unread is everything newer
-- than the cursor; with no cursor yet, everything received since
-- joining.  Local-only, never gossiped; leaving drops the cursor.
CREATE TABLE IF NOT EXISTS read_cursors (
    channel TEXT PRIMARY KEY,
    last_read_hash TEXT NOT NULL,
    last_read_at REAL NOT NULL
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

-- Pins are local-only: a message this node marked to keep surfaced.
-- A pinned message is never dropped by per-channel pruning, and its
-- row is deleted with the channel when a merely-cached room is
-- evicted whole, so a pin can never outlive its message.
CREATE TABLE IF NOT EXISTS pins (
    channel TEXT NOT NULL,
    hash TEXT NOT NULL,
    pinned_at REAL NOT NULL,
    PRIMARY KEY (channel, hash)
);
CREATE TABLE IF NOT EXISTS edits (
    v INTEGER NOT NULL DEFAULT 1,
    hash TEXT PRIMARY KEY,
    kind TEXT NOT NULL DEFAULT 'channel-edit',
    channel TEXT NOT NULL,
    target TEXT NOT NULL,
    text TEXT NOT NULL,
    author TEXT NOT NULL,
    key TEXT NOT NULL,
    created_at REAL NOT NULL,
    received_at REAL NOT NULL,
    sig TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS edits_channel_target
    ON edits(channel, target, created_at DESC, hash DESC);
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
    # A store created before expiring messages has no expires_at
    # column: CREATE TABLE IF NOT EXISTS leaves it alone, so add
    # the column in place rather than rebuilding the store.
    columns = [
        row[1] for row in conn.execute("PRAGMA table_info(messages)").fetchall()
    ]
    if "expires_at" not in columns:
        conn.execute("ALTER TABLE messages ADD COLUMN expires_at REAL")
    # Stores written before polls existed have no poll columns:
    # add them so an old store keeps working without a rebuild.
    columns = {row[1] for row in conn.execute("PRAGMA table_info(messages)")}
    for column in POLL_FIELDS:
        if column not in columns:
            conn.execute(f"ALTER TABLE messages ADD COLUMN {column} TEXT")
    # Stores written before replies existed have no reply_to column:
    # add it in place so an old database keeps its history.
    columns = [
        row[1] for row in conn.execute("PRAGMA table_info(messages)").fetchall()
    ]
    if "reply_to" not in columns:
        conn.execute(
            "ALTER TABLE messages ADD COLUMN reply_to TEXT NOT NULL DEFAULT ''"
        )
    # Stores written before location pins existed have no location
    # column: add it in place so an old database keeps its history.
    columns = {row[1] for row in conn.execute("PRAGMA table_info(messages)")}
    if "location" not in columns:
        conn.execute("ALTER TABLE messages ADD COLUMN location TEXT")
    conn.execute(
        "INSERT OR IGNORE INTO meta(key, value) VALUES('version', ?)",
        (str(CHANNEL_VERSION),),
    )
    conn.commit()
    return conn


def _dump_poll_field(value: Any) -> Optional[str]:
    """Encode a poll field for storage, or ``None`` when absent."""
    if value is None:
        return None
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )


def _row_to_message(row: sqlite3.Row) -> Dict[str, Any]:
    record = dict(row)
    for field in POLL_FIELDS:
        raw = record.get(field)
        if raw is None:
            record.pop(field, None)
        else:
            record[field] = json.loads(raw)
    raw_location = record.get("location")
    if raw_location is None:
        record.pop("location", None)
    else:
        record["location"] = json.loads(raw_location)
    return _canonical_message(record)


def _fetch_message(conn: sqlite3.Connection, digest: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        f"SELECT {', '.join(_MESSAGE_COLUMNS)} FROM messages WHERE hash = ?",
        (digest,),
    ).fetchone()


def get_message(conn: sqlite3.Connection, digest: str) -> Optional[Dict[str, Any]]:
    """Return a stored message by hash, or ``None``."""
    row = _fetch_message(conn, digest)
    return _row_to_message(row) if row is not None else None


def get_visible_message(
    conn: sqlite3.Connection, digest: str
) -> Optional[Dict[str, Any]]:
    """Return a stored message as channel history shows it, or ``None``.

    Applies the same lifecycle as :func:`channel_history_page`: an
    expired or retracted message is gone (``None``), and a live one
    carries its latest author-matching edit.
    """
    row = conn.execute(
        f"SELECT {', '.join(_MESSAGE_COLUMNS)} FROM messages WHERE hash = ? "
        "AND (expires_at IS NULL OR expires_at > ?) "
        "AND NOT EXISTS (SELECT 1 FROM retractions r "
        "WHERE r.hash = messages.hash AND r.author = messages.author)",
        (digest, time.time()),
    ).fetchone()
    if row is None:
        return None
    return _apply_edits_to_messages(conn, [_row_to_message(row)])[0]


def delete_message(conn: sqlite3.Connection, digest: str) -> bool:
    """Drop one stored message by hash; returns whether one was removed.

    Local-only: peers that already received a copy keep theirs, the
    same way a Tan delete leaves copies peers already hold.  Used
    when a scheduled message is cancelled after a publish attempt
    already stored its signed copy — dropping only the queue entry
    would leave the message in the local channel history.
    """
    with conn:
        cursor = conn.execute(
            "DELETE FROM messages WHERE hash = ?",
            (digest,),
        )
    return cursor.rowcount > 0
def reply_parent(
    conn: sqlite3.Connection, message: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """The stored parent of a reply, or ``None``.

    ``None`` covers both cases a reader must handle the same way: a
    plain message (no ``reply_to``) and a reply whose parent has not
    arrived yet.  The caller shows the digest when the parent is
    missing, so a reply never blocks on history it does not have.
    """
    target = message.get("reply_to")
    if not target:
        return None
    return get_message(conn, target)


# -- retract notices: storage ------------------------------------------------


def _retraction_for(
    conn: sqlite3.Connection, digest: str, author: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT hash FROM retractions WHERE hash = ? AND author = ?",
        (digest, author),
    ).fetchone()


def _prune_retractions(conn: sqlite3.Connection, channel: str) -> None:
    """Bound one channel's retract notices to the newest ones."""
    conn.execute(
        "DELETE FROM retractions WHERE channel = ? AND rowid NOT IN ("
        "SELECT rowid FROM retractions WHERE channel = ? "
        "ORDER BY created_at DESC, hash DESC "
        f"LIMIT {MAX_RETRACTIONS_PER_CHANNEL})",
        (channel, channel),
    )


def create_retraction(
    conn: sqlite3.Connection,
    identity: RNS.Identity,
    channel: str,
    digest: str,
    *,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Sign and store a retract notice for one of *identity*'s own messages.

    The message is deleted from the local store and the signed notice
    recorded, so the retract survives to be gossiped and served on
    sync.  Retracting a message that is already retracted is a no-op
    returning the stored notice.
    """
    name = clean_channel_name(channel)
    message = get_message(conn, digest)
    if message is None:
        # Already retracted: the message is gone, but the notice
        # stands — return it instead of failing, so re-running a
        # retract is a no-op rather than an error.
        author = _author_from_identity(identity)
        existing = get_retraction(conn, digest, author)
        if existing is not None and existing["channel"] == name:
            return existing
        raise ChannelError(f"unknown channel message: {digest}")
    if message["channel"] != name:
        raise ChannelError(f"that message is not in #{name}")
    author = _author_from_identity(identity)
    if message["author"] != author:
        raise ChannelError("only the author can retract a channel message")
    timestamp = round(float(now if now is not None else time.time()), 3)
    body = {
        "v": CHANNEL_VERSION,
        "kind": RETRACTION_KIND,
        "channel": name,
        "hash": message["hash"],
        "author": author,
        "key": identity.get_public_key().hex(),
        "created_at": timestamp,
    }
    notice_digest = hash_retraction(body)
    notice = dict(body)
    notice["sig"] = identity.sign(bytes.fromhex(notice_digest)).hex()
    stored, _, _ = store_retraction(conn, notice)
    return stored


def store_retraction(
    conn: sqlite3.Connection, notice: Dict[str, Any]
) -> Tuple[Dict[str, Any], bool, bool]:
    """Validate and store a retract notice.

    Returns ``(notice, is_new, applied)``: ``is_new`` is false when
    the same author already retracted the same message, and
    ``applied`` is true when a held copy of the message was deleted.
    A notice whose author is not the message's author is stored but
    never applied — it is a tombstone for a message that may arrive
    later, and the authorship check runs again then, so a forged
    notice cannot censor someone else's message.
    """
    retraction = verify_retraction(notice)
    timestamp = time.time()
    with conn:
        cursor = conn.execute(
            "INSERT OR IGNORE INTO retractions"
            "(hash, channel, author, key, created_at, received_at, sig) "
            "VALUES(?, ?, ?, ?, ?, ?, ?)",
            (
                retraction["hash"],
                retraction["channel"],
                retraction["author"],
                retraction["key"],
                retraction["created_at"],
                timestamp,
                retraction["sig"],
            ),
        )
        is_new = cursor.rowcount == 1
        applied = False
        if is_new:
            message = _fetch_message(conn, retraction["hash"])
            if message is not None and message["author"] == retraction["author"]:
                conn.execute(
                    "DELETE FROM messages WHERE hash = ?", (retraction["hash"],)
                )
                applied = True
            _prune_retractions(conn, retraction["channel"])
    return _canonical_retraction(retraction), is_new, applied


def get_retraction(
    conn: sqlite3.Connection, digest: str, author: str
) -> Optional[Dict[str, Any]]:
    """Return a stored retract notice by message digest and author, or ``None``."""
    row = conn.execute(
        "SELECT hash, channel, author, key, created_at, sig "
        "FROM retractions WHERE hash = ? AND author = ?",
        (digest, author),
    ).fetchone()
    if row is None:
        return None
    return _canonical_retraction(
        {"v": CHANNEL_VERSION, "kind": RETRACTION_KIND, **dict(row)}
    )


def list_retractions(
    conn: sqlite3.Connection,
    channel: str,
    *,
    limit: int = MAX_FEED_RETRACTIONS,
    exclude_authors: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Return a channel's retract notices, newest first."""
    name = clean_channel_name(channel)
    clauses = ["channel = ?"]
    params: List[Any] = [name]
    excluded = [author for author in (exclude_authors or []) if author]
    if excluded:
        placeholders = ", ".join("?" for _ in excluded)
        clauses.append(f"author NOT IN ({placeholders})")
        params.extend(excluded)
    rows = conn.execute(
        "SELECT hash, channel, author, key, created_at, sig "
        "FROM retractions "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY created_at DESC, hash DESC LIMIT ?",
        (*params, max(1, int(limit))),
    ).fetchall()
    return [
        _canonical_retraction(
            {"v": CHANNEL_VERSION, "kind": RETRACTION_KIND, **dict(row)}
        )
        for row in rows
    ]


def store_message(
    conn: sqlite3.Connection,
    mesg: Dict[str, Any],
    *,
    now: Optional[float] = None,
    verify: bool = True,
) -> Tuple[Dict[str, Any], bool]:
    """Validate and store a channel message, returning ``(message, is_new)``.

    ``is_new`` is false when the message was already stored, when a
    retract notice for it is already held — a retracted message must
    never re-appear from another peer's copy — or when retention
    evicted it during its own insert (an old message arriving into a
    full store).  Eviction is not an error: the message was
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
    if (
        message.get("expires_at") is not None
        and message["expires_at"] <= timestamp
    ):
        # Already expired on arrival — a slow gossip, or a sender
        # whose clock ran ahead: never store it, like an expired DM.
        log.info("Dropping expired channel message %s", digest)
        return _canonical_message(message), False
    if _retraction_for(conn, digest, message["author"]) is not None:
        # A retract notice arrived before the message did: the
        # tombstone stands, and the message is refused, not stored.
        return _canonical_message(message), False
    existing = _fetch_message(conn, digest)
    if existing is not None:
        # Parse the stored row the same way every other read does:
        # the raw row carries poll/location fields as JSON strings,
        # and returning them unparsed would hand callers a string
        # where a list or dict belongs.
        return _row_to_message(existing), False

    try:
        with conn:
            conn.execute(
                "INSERT INTO messages(v, hash, channel, author, key, name, text, "
                "created_at, expires_at, received_at, sig, poll_options, poll_vote, poll_close, reply_to, location) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    CHANNEL_VERSION,
                    digest,
                    message["channel"],
                    message["author"],
                    message["key"],
                    message["name"],
                    message["text"],
                    message["created_at"],
                    message.get("expires_at"),
                    timestamp,
                    message["sig"],
                    _dump_poll_field(message.get("poll_options")),
                    _dump_poll_field(message.get("poll_vote")),
                    _dump_poll_field(message.get("poll_close")),
                    message.get("reply_to", ""),
                    _dump_poll_field(message.get("location")),
                ),
            )
            _prune_store(conn, message["channel"])
            # Expired rows are invisible to history either way, but
            # dropping them here keeps the store bounded on a node
            # that receives far more than it reads.
            conn.execute(
                "DELETE FROM messages "
                "WHERE expires_at IS NOT NULL AND expires_at <= ?",
                (timestamp,),
            )
    except sqlite3.IntegrityError:
        # Another thread or process inserted the same message first.
        existing = _fetch_message(conn, digest)
        if existing is None:
            raise
        return _row_to_message(existing), False

    record = get_message(conn, digest)
    if record is None:
        # Retention evicted the message during its own insert: an old
        # message arriving into a full store.  Report it as not newly
        # stored rather than as a disappearance — the caller already
        # has the validated message, and the store bound is working.
        return _canonical_message(message), False
    return _canonical_message(record), True


def resolve_reaction_target(
    conn: sqlite3.Connection, channel: str, ref: str
) -> str:
    """Resolve a full message hash or a unique hex prefix to a hash.

    The target of a reaction must be a message this node has already
    stored in the same channel: reacting to a message you have never
    seen would leave readers with a dangling reference and no way to
    check the target is even in this room.
    """
    name = clean_channel_name(channel)
    text = ref.strip().lower() if isinstance(ref, str) else ""
    if not text or re.fullmatch(r"[0-9a-f]+", text) is None:
        raise ChannelError("reaction target must be a message hash or hex prefix")
    if len(text) < MIN_DIGEST_PREFIX:
        raise ChannelError(
            f"reaction target prefix must be at least {MIN_DIGEST_PREFIX} hex characters"
        )
    if len(text) == HASH_HEX_LENGTH:
        row = conn.execute(
            "SELECT hash FROM messages WHERE channel = ? AND hash = ?",
            (name, text),
        ).fetchone()
        if row is None:
            raise ChannelError(
                f"no stored message with hash {text} in #{name}"
            )
        return text
    if len(text) > HASH_HEX_LENGTH:
        raise ChannelError("reaction target must be a message hash or hex prefix")
    rows = conn.execute(
        "SELECT hash FROM messages WHERE channel = ? AND hash LIKE ? ORDER BY hash",
        (name, text + "%"),
    ).fetchall()
    if not rows:
        raise ChannelError(f"no stored message with prefix {text} in #{name}")
    if len(rows) > 1:
        raise ChannelError(
            f"reaction target prefix {text} is ambiguous in #{name}; "
            "use more characters"
        )
    return rows[0][0]


def _row_to_reaction(row: sqlite3.Row) -> Dict[str, Any]:
    return _canonical_reaction(dict(row))


def _fetch_reaction(
    conn: sqlite3.Connection, target: str, reactor: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        f"SELECT {', '.join(_REACTION_COLUMNS)} FROM reactions "
        "WHERE target = ? AND reactor = ?",
        (target, reactor),
    ).fetchone()


def store_reaction(
    conn: sqlite3.Connection,
    mesg: Dict[str, Any],
    *,
    now: Optional[float] = None,
    verify: bool = True,
) -> Tuple[Dict[str, Any], bool]:
    """Validate and store a channel reaction, returning ``(reaction, changed)``.

    ``changed`` is false when the stored reaction is already this
    one — or when an older copy arrived after a newer one (a stale
    re-send must not overwrite the reactor's latest reaction: the
    newer ``(created_at, hash)`` wins, so two nodes converge on the
    same reaction regardless of arrival order).  An empty
    ``reaction`` is a retraction: it replaces the row, and readers
    filter it out, so the clear itself gossips.
    """
    timestamp = float(now if now is not None else time.time())
    if verify:
        reaction = verify_reaction(mesg, now=timestamp)
    else:
        base = _normalize_reaction(mesg, now=timestamp)
        reaction = dict(base)
        reaction["hash"] = hash_reaction(base)
        reaction["sig"] = _require_hex(mesg.get("sig"), SIG_HEX_LENGTH, "sig")

    existing = _fetch_reaction(conn, reaction["target"], reaction["reactor"])
    if existing is not None:
        before = (existing["created_at"], existing["hash"])
        after = (reaction["created_at"], reaction["hash"])
        if before >= after:
            return _canonical_reaction(dict(existing)), False
    changed = existing is None or existing["reaction"] != reaction["reaction"]

    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO reactions("
            + ", ".join(_REACTION_COLUMNS)
            + ") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                CHANNEL_VERSION,
                reaction["hash"],
                reaction["channel"],
                reaction["target"],
                reaction["reactor"],
                reaction["key"],
                reaction["name"],
                reaction["reaction"],
                reaction["created_at"],
                timestamp,
                reaction["sig"],
            ),
        )
    row = _fetch_reaction(conn, reaction["target"], reaction["reactor"])
    if row is None:  # pragma: no cover - the insert just succeeded
        raise ChannelError("channel reaction was not stored")
    return _canonical_reaction(dict(row)), changed


def create_reaction(
    conn: sqlite3.Connection,
    identity: RNS.Identity,
    channel: str,
    target: str,
    reaction: str,
    *,
    name: str = "anonymous",
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Create, sign and store a reaction owned by *identity*.

    ``target`` is a parent message's hash or unique hex prefix; it
    must already be stored in this channel.  ``reaction`` is an emoji
    or short token; an empty value clears the reactor's reaction.
    """
    timestamp = round(float(now if now is not None else time.time()), 3)
    reactor = _author_from_identity(identity)
    try:
        token = clean_reaction(reaction)
    except ProtocolError as exc:
        raise ChannelError(f"invalid channel reaction: {exc}") from exc
    body = {
        "v": CHANNEL_VERSION,
        "channel": clean_channel_name(channel),
        "target": resolve_reaction_target(conn, channel, target),
        "reactor": reactor,
        "key": identity.get_public_key().hex(),
        "name": sanitize_name(name),
        "reaction": token,
        "created_at": timestamp,
    }
    digest = hash_reaction(body)
    mesg = dict(body)
    mesg["hash"] = digest
    mesg["sig"] = identity.sign(bytes.fromhex(digest)).hex()
    # Measure the signed envelope, not the unsigned body: a multibyte
    # token near the budget would otherwise store locally and only
    # fail when publish() encodes it for peers.
    _check_reaction_wire_size(mesg)
    record, _ = store_reaction(conn, mesg, now=timestamp)
    return record


def reactions_for_messages(
    conn: sqlite3.Connection,
    channel: str,
    targets: Sequence[str],
    *,
    exclude_authors: Optional[Sequence[str]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Live reactions for *targets* in *channel*, keyed by target hash.

    Retractions (an empty ``reaction``) are filtered out, and so are
    reactions from *exclude_authors* — a blocked reactor's reactions
    are never displayed or served on, like a blocked author's
    messages.  Reactions whose target has not arrived yet are stored
    but not returned here: a reader can only show them once the
    message lands.
    """
    name = clean_channel_name(channel)
    wanted = [str(target) for target in targets if target]
    if not wanted:
        return {}
    placeholders = ", ".join("?" for _ in wanted)
    clauses = ["channel = ?", f"target IN ({placeholders})", "reaction != ''"]
    params: List[Any] = [name, *wanted]
    excluded = [author for author in (exclude_authors or []) if author]
    if excluded:
        placeholders = ", ".join("?" for _ in excluded)
        clauses.append(f"reactor NOT IN ({placeholders})")
        params.extend(excluded)
    rows = conn.execute(
        f"SELECT {', '.join(_REACTION_COLUMNS)} FROM reactions "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY created_at ASC, hash ASC",
        params,
    ).fetchall()
    grouped: Dict[str, List[Dict[str, Any]]] = {target: [] for target in wanted}
    for row in rows:
        record = _row_to_reaction(row)
        grouped.setdefault(record["target"], []).append(record)
    return grouped


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
        f"LIMIT {MAX_STORED_PER_CHANNEL}) "
        "AND hash NOT IN (SELECT hash FROM pins WHERE channel = ?)",
        (channel, channel, channel),
    )
    # Reactions on a pruned message go with it: a reaction whose
    # target is gone can never be displayed, and a peer re-serving
    # it would only grow the store again.
    conn.execute(
        "DELETE FROM reactions WHERE channel = ? AND target NOT IN ("
        "SELECT hash FROM messages WHERE channel = ?)",
        (channel, channel),
    )
    # An edit notice is only useful while its message survives: drop
    # notices whose target is gone, so a pruned message does not
    # leave orphaned edits behind.
    conn.execute(
        "DELETE FROM edits WHERE channel = ? AND target NOT IN ("
        "SELECT hash FROM messages WHERE channel = ?)",
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
        # A pin must never outlive its message.
        conn.execute("DELETE FROM pins WHERE channel = ?", (row[0],))
        conn.execute("DELETE FROM reactions WHERE channel = ?", (row[0],))
        conn.execute("DELETE FROM edits WHERE channel = ?", (row[0],))
        # The room is gone: its retract notices go with it, so a
        # re-joined room starts from a clean slate.
        conn.execute("DELETE FROM retractions WHERE channel = ?", (row[0],))


def create_message(
    conn: sqlite3.Connection,
    identity: RNS.Identity,
    channel: str,
    text: str,
    *,
    name: str = "anonymous",
    now: Optional[float] = None,
    expires_at: Optional[float] = None,
    poll_options: Optional[List[str]] = None,
    poll_vote: Optional[Dict[str, Any]] = None,
    poll_close: Optional[Dict[str, Any]] = None,
    reply_to: Optional[str] = None,
    location: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Create, sign and store a message owned by *identity*.

    At most one of ``poll_options``, ``poll_vote`` and ``poll_close``
    may be set: a poll (the text is the question), a vote on a poll,
    or a close of one — each validated like a group poll's.

    ``reply_to`` is a parent message's hash or unique hex prefix; it
    must already be stored in this channel, and it is covered by the
    new message's hash.

    ``location`` pins a place on a top-level message (``{"lat": ..,
    "lon": .., "name": ..}``); it is covered by the hash when
    present, and refused on a reply or a poll.

    """
    timestamp = round(float(now if now is not None else time.time()), 3)
    if expires_at is not None and float(expires_at) <= timestamp:
        raise ChannelError("channel message expiry must be in the future")
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
    if expires_at is not None:
        # Unsigned advisory field (see MAX_CHANNEL_EXPIRE_SECONDS):
        # it rides the message but never enters the hash, so older
        # nodes verify the message exactly as before.
        body["expires_at"] = float(expires_at)
    if poll_options is not None:
        body["poll_options"] = poll_options
    if poll_vote is not None:
        body["poll_vote"] = poll_vote
    if poll_close is not None:
        body["poll_close"] = poll_close
    if reply_to is not None:
        body["reply_to"] = resolve_message_digest(conn, body["channel"], reply_to)
    if location is not None:
        body["location"] = location
    normalized = _normalize_message(body, now=timestamp)
    digest = hash_message(normalized)
    mesg = dict(normalized)
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
    apply_edits: bool = True,
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

    With ``apply_edits`` (the default) each message shows its latest
    author-matching edit, marked ``edited``.  The feed handler passes
    ``False``: a peer must receive the original, hash-valid text plus
    the edit notices separately, or it could not verify what it stores.
    """
    name = clean_channel_name(channel)
    count = max(1, min(int(limit), MAX_CHANNEL_HISTORY))
    clauses = ["channel = ?"]
    params: List[Any] = [name]
    # An expired message is gone from history even before the store
    # purges it: a node that only reads must never show one.
    clauses.append("(expires_at IS NULL OR expires_at > ?)")
    params.append(time.time())
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
    # A retracted message is gone from history: the tombstone is
    # checked per message, so a forged notice (whose author is not
    # the message's author) never hides anything.
    clauses.append(
        "NOT EXISTS (SELECT 1 FROM retractions r "
        "WHERE r.hash = messages.hash AND r.author = messages.author)"
    )

    rows = conn.execute(
        f"SELECT {', '.join(_MESSAGE_COLUMNS)} FROM messages "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY created_at DESC, hash DESC LIMIT ?",
        (*params, count + 1),
    ).fetchall()
    has_more = len(rows) > count
    messages = [_row_to_message(row) for row in rows[:count]]
    if apply_edits:
        messages = _apply_edits_to_messages(conn, messages)
    return messages, has_more


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


def channel_messages_all(
    conn: sqlite3.Connection,
    channel: str,
) -> List[Dict[str, Any]]:
    """Every stored message for *channel*, newest first.

    Unpaged (bounded by the per-channel store cap) for callers that
    derive state from the whole history — poll tallies, which count
    votes a displayed page may have left out.
    """
    name = clean_channel_name(channel)
    rows = conn.execute(
        f"SELECT {', '.join(_MESSAGE_COLUMNS)} FROM messages "
        "WHERE channel = ? ORDER BY created_at DESC, hash DESC "
        f"LIMIT {MAX_STORED_PER_CHANNEL}",
        (name,),
    ).fetchall()
    return [_row_to_message(row) for row in rows]


def search_channel_messages(
    conn: sqlite3.Connection,
    channel: str,
    query: str,
    *,
    limit: int = DEFAULT_CHANNEL_LIMIT,
    exclude_authors: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Search stored messages in *channel* by keyword, newest first.

    The match is a case-insensitive substring search over the stored
    message text, with ``casefold`` on both sides, so it folds Unicode
    case (``Ä`` matches ``ä``) — the same promise ``tan search`` makes.
    The match runs in Python rather than as a SQL ``LIKE``: SQLite's
    ``LIKE`` folds ASCII case only, so a ``LIKE`` query would miss
    exactly the Unicode matches this promises.  The scan is over one
    channel's stored messages only, which the store prunes to a small
    cap, and it reads only the local store: it makes no network
    requests.

    *exclude_authors* hides messages from those authors, the same way
    history hides them; an empty query raises :class:`ChannelError`.
    """
    if not isinstance(query, str) or not query.strip():
        raise ChannelError("search text must not be empty")
    name = clean_channel_name(channel)
    count = max(1, min(int(limit), MAX_CHANNEL_HISTORY))
    needle = query.strip().casefold()
    clauses = ["channel = ?"]
    params: List[Any] = [name]
    excluded = [author for author in (exclude_authors or []) if author]
    if excluded:
        placeholders = ", ".join("?" for _ in excluded)
        clauses.append(f"author NOT IN ({placeholders})")
        params.extend(excluded)
    rows = conn.execute(
        f"SELECT {', '.join(_MESSAGE_COLUMNS)} FROM messages "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY created_at DESC, hash DESC",
        params,
    ).fetchall()
    matches = [
        _row_to_message(row)
        for row in rows
        if needle in str(row["text"]).casefold()
    ]
    return matches[:count]


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
        # Leaving drops the read cursor too: a rejoin starts fresh,
        # so old cached history is not resurrected as unread.
        conn.execute("DELETE FROM read_cursors WHERE channel = ?", (name,))
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


def mark_channel_read(
    conn: sqlite3.Connection,
    channel: str,
    *,
    exclude_authors: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Advance *channel*'s read cursor to its newest stored message.

    Requires membership: marking a channel you have not joined is an
    error, so a typo never silently clears a badge.  Returns the
    cursor plus how many messages it cleared; with no stored messages
    there is nothing to mark and the cursor is left alone.
    *exclude_authors* keeps the cleared count honest — the same
    authors :func:`channel_unread_counts` excludes from the badge.
    """
    name = clean_channel_name(channel)
    row = conn.execute(
        "SELECT channel, joined_at FROM memberships WHERE channel = ?",
        (name,),
    ).fetchone()
    if row is None:
        raise ChannelError(f"not a member of #{name} — join it first")
    newest = conn.execute(
        "SELECT hash, created_at FROM messages WHERE channel = ? "
        "ORDER BY created_at DESC, hash DESC LIMIT 1",
        (name,),
    ).fetchone()
    cleared = channel_unread_counts(conn, exclude_authors=exclude_authors).get(
        name, 0
    )
    if newest is None:
        return {"channel": name, "hash": None, "created_at": None, "cleared": 0}
    with conn:
        conn.execute(
            "INSERT INTO read_cursors(channel, last_read_hash, last_read_at) "
            "VALUES(?, ?, ?) "
            "ON CONFLICT(channel) DO UPDATE SET "
            "last_read_hash = excluded.last_read_hash, "
            "last_read_at = excluded.last_read_at",
            (name, newest["hash"], newest["created_at"]),
        )
    return {
        "channel": name,
        "hash": newest["hash"],
        "created_at": newest["created_at"],
        "cleared": cleared,
    }


def channel_unread_counts(
    conn: sqlite3.Connection,
    *,
    exclude_authors: Optional[Sequence[str]] = None,
    exclude_channels: Optional[Sequence[str]] = None,
) -> Dict[str, int]:
    """Unread message counts per joined channel.

    A message counts when it is newer than the channel's read cursor
    — strictly newer by ``(created_at, hash)``, the same order history
    pages in, so a late-arriving message created before the cursor
    does not re-light the badge.  With no cursor yet, messages
    received since joining count: history synced at join time is
    unread, but cached messages from before a rejoin are not.
    Authors in *exclude_authors* never count (own messages, blocked
    authors); pass nothing to count every stored message.  Channels
    in *exclude_channels* never count either — a muted channel's
    badge stays dark — and get no entry at all, like a channel with
    nothing unread.
    """
    excluded = [author for author in (exclude_authors or []) if author]
    author_clause = ""
    params: List[Any] = []
    if excluded:
        placeholders = ", ".join("?" for _ in excluded)
        author_clause = f" AND msg.author NOT IN ({placeholders})"
        params.extend(excluded)
    rows = conn.execute(
        "SELECT m.channel AS channel, "
        "COUNT(msg.hash) AS unread "
        "FROM memberships m "
        "LEFT JOIN read_cursors c ON c.channel = m.channel "
        "LEFT JOIN messages msg ON msg.channel = m.channel "
        "AND ((c.last_read_at IS NULL AND msg.received_at > m.joined_at) "
        "OR (c.last_read_at IS NOT NULL "
        "AND (msg.created_at > c.last_read_at "
        "OR (msg.created_at = c.last_read_at AND msg.hash > c.last_read_hash))))"
        f"{author_clause} "
        "GROUP BY m.channel",
        params,
    ).fetchall()
    # Like the inbox counts, a channel with nothing unread gets no
    # entry — the console only badges channels that have one.
    muted = set(exclude_channels or [])
    return {
        row["channel"]: int(row["unread"])
        for row in rows
        if int(row["unread"]) > 0 and row["channel"] not in muted
    }



def pin_message(
    conn: sqlite3.Connection,
    channel: str,
    digest: str,
    *,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Pin a stored message so pruning never drops it; return the pin.

    Pins are local-only: they mark a message in this node's store,
    they are not gossiped, and a pin on a message that is not stored
    here (or is stored under another channel) is an error.  Pinning
    the same message twice is an error, not a silent no-op.
    """
    name = clean_channel_name(channel)
    digest = _require_hex(digest, HASH_HEX_LENGTH, "hash")
    row = _fetch_message(conn, digest)
    if row is None or row["channel"] != name:
        raise ChannelError(
            f"no stored message {digest} in channel {name!r}"
        )
    pinned_at = float(now if now is not None else time.time())
    try:
        with conn:
            conn.execute(
                "INSERT INTO pins(channel, hash, pinned_at) VALUES(?, ?, ?)",
                (name, digest, pinned_at),
            )
    except sqlite3.IntegrityError:
        raise ChannelError(
            f"message {digest} is already pinned in channel {name!r}"
        )
    return {"channel": name, "hash": digest, "pinned_at": pinned_at}


def unpin_message(conn: sqlite3.Connection, channel: str, digest: str) -> bool:
    """Drop the pin on a message; ``True`` when a pin was removed."""
    name = clean_channel_name(channel)
    digest = _require_hex(digest, HASH_HEX_LENGTH, "hash")
    with conn:
        cursor = conn.execute(
            "DELETE FROM pins WHERE channel = ? AND hash = ?",
            (name, digest),
        )
    return cursor.rowcount > 0


def is_pinned(conn: sqlite3.Connection, channel: str, digest: str) -> bool:
    """Whether *digest* is pinned in *channel*."""
    name = clean_channel_name(channel)
    digest = _require_hex(digest, HASH_HEX_LENGTH, "hash")
    row = conn.execute(
        "SELECT hash FROM pins WHERE channel = ? AND hash = ?",
        (name, digest),
    ).fetchone()
    return row is not None


def list_pins(
    conn: sqlite3.Connection,
    channel: str,
    *,
    exclude_authors: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Pinned messages for *channel*, newest pin first.

    Each entry is the stored message plus ``pinned_at``.  A pin whose
    message is gone (evicted with its channel) never surfaces: the
    join drops it rather than returning a pin with nothing behind it.
    """
    name = clean_channel_name(channel)
    excluded = [author for author in (exclude_authors or []) if author]
    clauses = ["p.channel = ?"]
    params: List[Any] = [name]
    if excluded:
        placeholders = ", ".join("?" for _ in excluded)
        clauses.append(f"m.author NOT IN ({placeholders})")
        params.extend(excluded)
    columns = ", ".join(f"m.{column}" for column in _MESSAGE_COLUMNS)
    rows = conn.execute(
        f"SELECT {columns}, p.pinned_at FROM pins p "
        "JOIN messages m ON m.hash = p.hash "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY p.pinned_at DESC, p.hash DESC",
        params,
    ).fetchall()
    return [
        {**_row_to_message(row), "pinned_at": row["pinned_at"]} for row in rows
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
        node.add_request_handler(CHANNEL_REACT_PATH, self._handle_react)
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

    def mark_read(self, channel: str) -> Dict[str, Any]:
        """Advance *channel*'s read cursor to its newest message."""
        conn = self.connect()
        try:
            return mark_channel_read(
                conn, channel, exclude_authors=self._unread_excluded()
            )
        finally:
            conn.close()

    def unread_counts(self) -> Dict[str, int]:
        """Unread message counts per joined channel.

        Own messages never count, and neither do blocked authors; a
        block-list that cannot be read fails closed to all zeroes
        rather than lighting badges for messages the reader hides.
        A muted channel never counts either — its badge stays dark —
        while a corrupt mutes file fails open, so the channel counts
        as unmuted rather than breaking the whole read.
        """
        excluded = self._unread_excluded()
        if excluded is None:
            # Fail closed: no badge at all, not even a zero entry.
            return {}
        conn = self.connect()
        try:
            return channel_unread_counts(
                conn,
                exclude_authors=excluded,
                exclude_channels=muted_channels_for(
                    getattr(self.config, "muted_path", None)
                ),
            )
        finally:
            conn.close()

    def _unread_excluded(self) -> Optional[List[str]]:
        """Authors excluded from unread counts, or ``None`` fail-closed.

        The node's own author hash plus the blocked list; ``None``
        when the block-list cannot be trusted.
        """
        blocked = self._blocked_authors()
        if blocked is None:
            return None
        return [self.author, *blocked]

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

    def get_message(self, message_hash: str) -> Optional[Dict[str, Any]]:
        """Return a stored message by hash, or ``None``.

        The scheduled-message publisher re-fetches the signed message
        it created on an earlier pass, so a retry re-sends the same
        message instead of signing a second copy for the same text.
        """
        conn = self.connect()
        try:
            return get_message(conn, message_hash)
        finally:
            conn.close()

    def delete_message(self, message_hash: str) -> bool:
        """Drop one stored message by hash; returns whether one was removed.

        Local-only: peers that already received a copy keep theirs.
        """
        conn = self.connect()
        try:
            return delete_message(conn, message_hash)
        finally:
            conn.close()

    def all_history(self, channel: str) -> List[Dict[str, Any]]:
        """Every stored message for *channel*, newest first.

        For deriving state from the whole history (poll tallies),
        not for paging a listing.
        """
        conn = self.connect()
        try:
            return channel_messages_all(conn, channel)
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

    def pin(self, channel: str, digest: str) -> Dict[str, Any]:
        """Pin a stored message in *channel*; pins survive pruning."""
        conn = self.connect()
        try:
            return pin_message(conn, channel, digest)
        finally:
            conn.close()

    def unpin(self, channel: str, digest: str) -> bool:
        """Drop the pin on a message in *channel*."""
        conn = self.connect()
        try:
            return unpin_message(conn, channel, digest)
        finally:
            conn.close()

    def pins(self, channel: str) -> List[Dict[str, Any]]:
        """Pinned messages for *channel*, newest pin first.

        Blocked authors never surface, and a block-list that cannot
        be read fails closed to no pins — the same rule as history.
        """
        conn = self.connect()
        try:
            excluded = self._blocked_authors()
            if excluded is None:
                return []
            return list_pins(conn, channel, exclude_authors=excluded)
        finally:
            conn.close()

    def search(
        self,
        channel: str,
        query: str,
        *,
        limit: int = DEFAULT_CHANNEL_LIMIT,
        exclude_authors: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Stored messages in *channel* matching *query*, newest first."""
        conn = self.connect()
        try:
            return search_channel_messages(
                conn, channel, query, limit=limit, exclude_authors=exclude_authors
            )
        finally:
            conn.close()

    def send(
        self,
        channel: str,
        text: str,
        *,
        publish: bool = False,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
        expires_at: Optional[float] = None,
        poll_options: Optional[List[str]] = None,
        poll_vote: Optional[Dict[str, Any]] = None,
        poll_close: Optional[Dict[str, Any]] = None,
        reply_to: Optional[str] = None,
        location: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create a message in a joined channel, gossiping it when asked.

        Sending requires membership: a channel you have not joined is
        not yours to post in.  An explicit *expires_at* wins; without
        one, a per-channel disappearing default fills the expiry in,
        exactly like a DM or group send — a corrupt defaults file
        fails open with a warning inside ``channel_default_for``,
        so a send never breaks over it.  At most one of
        ``poll_options``, ``poll_vote`` and ``poll_close`` may be set,
        making the message a poll, a vote on one, or a close of one.
        ``reply_to`` is a parent message's hash or unique hex
        prefix in the same channel.  ``location`` pins a place on a
        top-level message; it is refused on a reply or a poll.
        """
        name = clean_channel_name(channel)
        if expires_at is None:
            default = channel_default_for(
                getattr(self.config, "disappearing_path", None), name
            )
            if default is not None:
                expires_at = time.time() + default
        conn = self.connect()
        try:
            if not is_member(conn, name):
                raise ChannelError(
                    f"not a member of channel {name!r}; join it first"
                )
            message = create_message(
                conn,
                self.identity,
                name,
                text,
                name=self.name,
                expires_at=expires_at,
                poll_options=poll_options,
                poll_vote=poll_vote,
                poll_close=poll_close,
                reply_to=reply_to,
                location=location,
            )
        finally:
            conn.close()
        if publish and self.node is not None:
            self.publish(message, targets=targets, ttl=ttl)
        return message

    def react(
        self,
        channel: str,
        target: str,
        reaction: str,
        *,
        publish: bool = False,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
    ) -> Dict[str, Any]:
        """React to a stored message in a joined channel, gossiping it when asked.

        Reacting requires membership, like sending, and the target
        must already be stored in this channel.  An empty
        ``reaction`` clears this node's reaction to the message.
        """
        name = clean_channel_name(channel)
        conn = self.connect()
        try:
            if not is_member(conn, name):
                raise ChannelError(
                    f"not a member of channel {name!r}; join it first"
                )
            record = create_reaction(
                conn, self.identity, name, target, reaction, name=self.name
            )
        finally:
            conn.close()
        if publish and self.node is not None:
            self.publish_reaction(record, targets=targets, ttl=ttl)
        return record

    def reactions(
        self,
        channel: str,
        targets: Sequence[str],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Live reactions for *targets* in *channel*, keyed by target hash.

        Blocked reactors are excluded, like blocked authors in
        history: a block placed after their reactions were cached
        must stop them being displayed.
        """
        name = clean_channel_name(channel)
        excluded = self._blocked_authors()
        if excluded is None:
            return {str(target): [] for target in targets}
        conn = self.connect()
        try:
            return reactions_for_messages(
                conn, name, targets, exclude_authors=excluded
            )
        finally:
            conn.close()

    def edit(
        self,
        channel: str,
        target: str,
        text: str,
        *,
        publish: bool = False,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
    ) -> Dict[str, Any]:
        """Edit your own message in a joined channel, gossiping it when asked.

        Editing requires membership, like sending.  The notice is
        signed by this node's identity, so peers can verify it came
        from the message's author.
        """
        name = clean_channel_name(channel)
        conn = self.connect()
        try:
            if not is_member(conn, name):
                raise ChannelError(
                    f"not a member of channel {name!r}; join it first"
                )
            notice = create_edit(conn, self.identity, name, target, text)
        finally:
            conn.close()
        if publish and self.node is not None:
            self.publish_edit(notice, targets=targets, ttl=ttl)
        return notice

    def edits(
        self, channel: str, *, limit: int = MAX_FEED_EDITS
    ) -> List[Dict[str, Any]]:
        """Stored edit notices for *channel*, newest first."""
        conn = self.connect()
        try:
            return list_edits(conn, channel, limit=limit)
        finally:
            conn.close()

    def retract(
        self,
        channel: str,
        digest: str,
        *,
        publish: bool = False,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
    ) -> Dict[str, Any]:
        """Retract one of this node's own messages in *channel*.

        *digest* is the message hash or a unique prefix of it (the
        prefixes ``tau channel read`` prints).  The message is deleted
        locally and a signed retract notice recorded; when *publish*
        is set and this app is attached to a running node, the notice
        is gossiped to peers like a message.  Retracting needs no
        membership: leaving a channel does not take back the right to
        retract what you posted in it.
        """
        name = clean_channel_name(channel)
        conn = self.connect()
        try:
            full = resolve_message_digest(conn, name, digest)
            notice = create_retraction(conn, self.identity, name, full)
        finally:
            conn.close()
        published = None
        if publish and self.node is not None:
            published = self.publish_retraction(notice, targets=targets, ttl=ttl)
        return {"retraction": notice, "published": published}

    def retractions(
        self, channel: str, *, limit: int = DEFAULT_CHANNEL_LIMIT
    ) -> List[Dict[str, Any]]:
        """Stored retract notices for *channel*, newest first."""
        conn = self.connect()
        try:
            return list_retractions(
                conn, clean_channel_name(channel), limit=limit
            )
        finally:
            conn.close()

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
                "reactions_stored": 0,
                "reactions_duplicates": 0,
                "edited": 0,
                "retracted": 0,
                "skipped": 0,
                "topic_updated": False,
                "errors": [],
            }

        page_limit = max(1, min(int(limit), MAX_CHANNEL_HISTORY))
        request_timeout = float(timeout if timeout is not None else self.request_timeout)
        conn = self.connect()
        stored = 0
        duplicates = 0
        reactions_stored = 0
        reactions_duplicates = 0
        edited = 0
        retracted = 0
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
                    # Reactions ride the same feed page as the
                    # messages they belong to: store them with the
                    # same per-entry care, and skip a blocked
                    # reactor's reactions like a blocked author's
                    # messages.  A reaction whose target has not
                    # arrived yet still stores — it shows once the
                    # message lands.
                    incoming = response.get("reactions")
                    if isinstance(incoming, list):
                        for raw in incoming:
                            if not isinstance(raw, dict):
                                errors.append(
                                    {
                                        "peer": getattr(peer, "hexhash", "?"),
                                        "error": "feed reaction is not an object",
                                    }
                                )
                                continue
                            if self._blocked(str(raw.get("reactor", ""))):
                                skipped += 1
                                continue
                            try:
                                _, changed = store_reaction(conn, raw)
                            except (ChannelError, ProtocolError) as exc:
                                errors.append(
                                    {
                                        "peer": getattr(peer, "hexhash", "?"),
                                        "error": str(exc),
                                    }
                                )
                                continue
                            if changed:
                                reactions_stored += 1
                            else:
                                reactions_duplicates += 1
                    # Retract notices ride every page, independent of
                    # the message page itself: a page with no messages
                    # can still carry a retract for one already held.
                    # Each entry is validated on its own, like messages.
                    for raw in response.get("retractions") or []:
                        if not isinstance(raw, dict):
                            errors.append(
                                {
                                    "peer": getattr(peer, "hexhash", "?"),
                                    "error": "retraction entry is not an object",
                                }
                            )
                            continue
                        if self._blocked(str(raw.get("author", ""))):
                            skipped += 1
                            continue
                        try:
                            _, is_new, _ = store_retraction(conn, raw)
                        except (ChannelError, ProtocolError) as exc:
                            errors.append(
                                {
                                    "peer": getattr(peer, "hexhash", "?"),
                                    "error": str(exc),
                                }
                            )
                            continue
                        if is_new:
                            retracted += 1
                    # Edit notices travel with the feed: the newest
                    # ones for this channel, applied on arrival so a
                    # reader never sees text the author already fixed.
                    # A notice for a message not stored yet is kept
                    # as pending and applies when the message lands.
                    notices = response.get("edits")
                    if isinstance(notices, list):
                        for raw in notices:
                            if not isinstance(raw, dict):
                                errors.append(
                                    {
                                        "peer": getattr(peer, "hexhash", "?"),
                                        "error": "feed edit is not an object",
                                    }
                                )
                                continue
                            if self._blocked(str(raw.get("author", ""))):
                                skipped += 1
                                continue
                            try:
                                _, is_new = store_edit(conn, raw)
                            except (ChannelError, ProtocolError) as exc:
                                errors.append(
                                    {
                                        "peer": getattr(peer, "hexhash", "?"),
                                        "error": str(exc),
                                    }
                                )
                                continue
                            if is_new:
                                edited += 1
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
            "reactions_stored": reactions_stored,
            "reactions_duplicates": reactions_duplicates,
            "edited": edited,
            "retracted": retracted,
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
        total_edited = 0
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
            total_edited += summary.get("edited", 0)
        return {"channels": summaries, "stored": total_stored, "edited": total_edited}

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

    def publish_reaction(
        self,
        reaction: Dict[str, Any],
        *,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
        exclude: Optional[str] = None,
        fanout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Push a channel reaction to peers; return delivery counts."""
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
        payload = encode(
            {"reaction": reaction, "ttl": max(0, min(int(ttl), MAX_GOSSIP_TTL))},
            max_bytes=MAX_REQUEST_BYTES,
        )
        delivered = 0
        errors: List[Dict[str, str]] = []
        for peer in peers:
            try:
                node.request(
                    peer, CHANNEL_REACT_PATH, payload, timeout=self.request_timeout
                )
                delivered += 1
            except Exception as exc:
                errors.append(
                    {"peer": getattr(peer, "hexhash", "?"), "error": str(exc)}
                )
        return {"delivered": delivered, "peers": len(peers), "errors": errors}

    def publish_edit(
        self,
        edit: Dict[str, Any],
        *,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
        exclude: Optional[str] = None,
        fanout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Push an edit notice to peers; return delivery counts."""
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
        payload = encode(
            {"edit": edit, "ttl": max(0, min(int(ttl), MAX_GOSSIP_TTL))},
            max_bytes=MAX_REQUEST_BYTES,
        )
        delivered = 0
        errors: List[Dict[str, str]] = []
        for peer in peers:
            try:
                node.request(
                    peer, CHANNEL_POST_PATH, payload, timeout=self.request_timeout
                )
                delivered += 1
            except Exception as exc:
                errors.append(
                    {"peer": getattr(peer, "hexhash", "?"), "error": str(exc)}
                )
        return {"delivered": delivered, "peers": len(peers), "errors": errors}

    def publish_retraction(
        self,
        notice: Dict[str, Any],
        *,
        targets: Optional[Sequence[Any]] = None,
        ttl: int = MAX_GOSSIP_TTL,
        exclude: Optional[str] = None,
        fanout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Push a retract notice to peers; return delivery counts."""
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
        payload = encode(
            {"retraction": notice, "ttl": max(0, min(int(ttl), MAX_GOSSIP_TTL))},
            max_bytes=MAX_REQUEST_BYTES,
        )
        delivered = 0
        errors: List[Dict[str, str]] = []
        for peer in peers:
            try:
                node.request(
                    peer, CHANNEL_POST_PATH, payload, timeout=self.request_timeout
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
                edits: List[Dict[str, Any]] = []
                retractions: List[Dict[str, Any]] = []
                reactions: List[Dict[str, Any]] = []
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
                        # Serve the original, hash-verifiable text:
                        # the requester overlays edits itself from
                        # the notices below.
                        apply_edits=False,
                    )
                    grouped = reactions_for_messages(
                        conn,
                        channel,
                        [item["hash"] for item in messages],
                        exclude_authors=excluded,
                    )
                    reactions = [
                        record
                        for item in messages
                        for record in grouped.get(item["hash"], [])
                    ]
                    edits = list_edits(
                        conn,
                        channel,
                        limit=MAX_FEED_EDITS,
                        exclude_authors=excluded,
                    )
                    # The channel's recent retract notices ride every
                    # page, so a peer syncing history learns about
                    # retracts for messages it already holds.
                    retractions = list_retractions(
                        conn,
                        channel,
                        limit=MAX_FEED_RETRACTIONS,
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
                    reactions=reactions,
                    edits=edits,
                    retractions=retractions,
                    count=len(messages),
                    edits_count=len(edits),
                    channel=channel,
                    has_more=has_more,
                    topic=topic,
                )
            except ProtocolError as exc:
                if len(messages) <= 1 and not reactions and not edits and not retractions:
                    return response_error("feed_too_large", str(exc))
                if len(messages) > 1:
                    dropped = messages[-1]
                    messages = messages[:-1]
                    reactions = [
                        record
                        for record in reactions
                        if record["target"] != dropped["hash"]
                    ]
                    # The page was shortened to fit: older messages
                    # still exist past it, so say so instead of
                    # letting the requester mistake a short page for
                    # the end.
                    has_more = True
                    continue
                if reactions:
                    reactions = reactions[:-1]
                    continue
                if edits:
                    edits = edits[:-1]
                    continue
                if retractions:
                    retractions = retractions[:-1]
                    continue
                return response_error("feed_too_large", str(exc))

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
            raw_message = request.get("message")
            raw_edit = request.get("edit")
            raw_retraction = request.get("retraction")
            raw_topic = request.get("topic")
            present = [
                part
                for part in (raw_message, raw_edit, raw_retraction, raw_topic)
                if part is not None
            ]
            if not present:
                raise ChannelError(
                    "a channel post needs a message, an edit or a retraction"
                )
            if len(present) > 1:
                raise ChannelError(
                    "a channel post carries one message, edit or retraction"
                )
            ttl = request.get("ttl", 0)
            if isinstance(ttl, bool) or not isinstance(ttl, int):
                ttl = 0
            ttl = max(0, min(ttl, MAX_GOSSIP_TTL))
            if raw_edit is not None:
                if not isinstance(raw_edit, dict):
                    raise ChannelError("edit must be a JSON object")
                verified = verify_edit(raw_edit)
                if self._blocked(verified["author"]):
                    log.info(
                        "Channel edit from blocked %s dropped",
                        verified["author"][:8],
                    )
                    return response_error(
                        "not_admitted",
                        "author is blocked on this node",
                    )
                conn = self.connect()
                try:
                    record, is_new = store_edit(conn, raw_edit)
                finally:
                    conn.close()

                if is_new:
                    log.info(
                        "Channel #%s edit from %s for %s",
                        record.get("channel", "?"),
                        record.get("author", "")[:8],
                        record.get("target", "")[:8],
                    )
                    if self.gossip and ttl > 0:
                        self._spawn_edit_gossip(
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
            if raw_retraction is not None:
                return self._handle_retraction_post(raw_retraction, ttl, remote_identity)

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

    def _handle_react(
        self,
        path: str,
        data: Any,
        request_id: bytes,
        remote_identity: Optional[RNS.Identity],
        requested_at: float,
    ) -> bytes:
        try:
            request = decode(data, max_bytes=MAX_REQUEST_BYTES)
            raw_reaction = request.get("reaction")
            if not isinstance(raw_reaction, dict):
                raise ChannelError("reaction must be a JSON object")
            ttl = request.get("ttl", 0)
            if isinstance(ttl, bool) or not isinstance(ttl, int):
                ttl = 0
            ttl = max(0, min(ttl, MAX_GOSSIP_TTL))

            verified = verify_reaction(raw_reaction)
            if self._blocked(verified["reactor"]):
                log.info(
                    "Channel reaction from blocked %s dropped",
                    verified["reactor"][:8],
                )
                return response_error(
                    "not_admitted",
                    "reactor is blocked on this node",
                )
            conn = self.connect()
            try:
                record, changed = store_reaction(conn, raw_reaction)
            finally:
                conn.close()

            if changed:
                log.info(
                    "Channel #%s reaction from %s (%s): %s on %s",
                    record.get("channel", "?"),
                    record.get("name", "unknown"),
                    record.get("reactor", "")[:8],
                    record.get("reaction", ""),
                    record.get("target", "")[:8],
                )
                if self.gossip and ttl > 0:
                    self._spawn_gossip_reaction(
                        record,
                        ttl=ttl - 1,
                        exclude=self._caller_hash(remote_identity),
                    )
            return response_ok(
                stored=record["hash"],
                new=changed,
                server=self.name,
                node=getattr(self.node, "destination_hexhash", None),
            )
        except (ProtocolError, ChannelError) as exc:
            return response_error("invalid_react", str(exc))
    def _handle_retraction_post(
        self,
        raw_retraction: Any,
        ttl: int,
        remote_identity: Optional[RNS.Identity],
    ) -> bytes:
        """Store a gossiped retract notice, deleting the message it names."""
        if not isinstance(raw_retraction, dict):
            raise ChannelError("retraction must be a JSON object")
        notice = verify_retraction(raw_retraction)
        if self._blocked(notice["author"]):
            log.info(
                "Channel retraction from blocked %s dropped",
                notice["author"][:8],
            )
            return response_error(
                "not_admitted",
                "author is blocked on this node",
            )
        conn = self.connect()
        try:
            stored, is_new, applied = store_retraction(conn, notice)
        finally:
            conn.close()

        if is_new:
            log.info(
                "Channel #%s retraction for %s from %s (applied: %s)",
                stored.get("channel", "?"),
                stored.get("hash", "")[:8],
                stored.get("author", "")[:8],
                applied,
            )
            if self.gossip and ttl > 0:
                self._spawn_retraction_gossip(
                    stored,
                    ttl=ttl - 1,
                    exclude=self._caller_hash(remote_identity),
                )
        return response_ok(
            stored=stored["hash"],
            new=is_new,
            retraction=True,
            applied=applied,
            server=self.name,
            node=getattr(self.node, "destination_hexhash", None),
        )

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

    def _spawn_gossip_reaction(
        self, reaction: Dict[str, Any], *, ttl: int, exclude: Optional[str]
    ) -> None:
        if not self._gossip_slots.acquire(blocking=False):
            log.debug("Skipping channel reaction gossip: too many pending relays")
            return

        def run() -> None:
            try:
                if self.node is not None and not self._closed:
                    self.publish_reaction(
                        reaction,
                        ttl=ttl,
                        exclude=exclude,
                        fanout=self.gossip_fanout,
                    )
            except Exception:  # pragma: no cover - network dependent
                log.debug("Channel reaction gossip failed", exc_info=True)
            finally:
                self._gossip_slots.release()

        threading.Thread(
            target=run, name="tau-channel-reaction-gossip", daemon=True
        ).start()

    def _spawn_edit_gossip(
        self, edit: Dict[str, Any], *, ttl: int, exclude: Optional[str]
    ) -> None:
        if not self._gossip_slots.acquire(blocking=False):
            log.debug("Skipping channel edit gossip: too many pending relays")
            return

        def run() -> None:
            try:
                if self.node is not None and not self._closed:
                    self.publish_edit(
                        edit,
                        ttl=ttl,
                        exclude=exclude,
                        fanout=self.gossip_fanout,
                    )
            except Exception:  # pragma: no cover - network dependent
                log.debug("Channel edit gossip failed", exc_info=True)
            finally:
                self._gossip_slots.release()

        threading.Thread(
            target=run, name="tau-channel-edit-gossip", daemon=True
        ).start()

    def _spawn_retraction_gossip(
        self, notice: Dict[str, Any], *, ttl: int, exclude: Optional[str]
    ) -> None:
        if not self._gossip_slots.acquire(blocking=False):
            log.debug("Skipping channel retraction gossip: too many pending relays")
            return

        def run() -> None:
            try:
                if self.node is not None and not self._closed:
                    self.publish_retraction(
                        notice,
                        ttl=ttl,
                        exclude=exclude,
                        fanout=self.gossip_fanout,
                    )
            except Exception:  # pragma: no cover - network dependent
                log.debug("Channel retraction gossip failed", exc_info=True)
            finally:
                self._gossip_slots.release()

        threading.Thread(
            target=run, name="tau-channel-retract-gossip", daemon=True
        ).start()
