"""Channel polls: a poll posted as a channel message, voted on in-room.

A poll is a channel message whose text is the question and whose
``poll_options`` carry the choices — the same 2-8 distinct, short,
normalised options a Tan poll enforces (see
:func:`tau.tangents.tan._clean_poll_options`).  A vote is a channel
message carrying ``poll_vote``: the poll's logical identity plus the
chosen option's index.  A close carries ``poll_close`` with the same
identity.  The identity is the one group polls already use — the
poll author's destination hash and the poll's ``created_at`` — so a
vote or close resolves against every peer's copy of the poll,
whatever each node stored.

The tally is derived from stored messages, never stored: one
standing vote per voter (the latest by send time, then hash), votes
past the close are excluded, and only a close from the poll's own
author closes it.  Vote and close records are not chat — callers
hide them from listings and render the tally on the poll itself
instead.

Poll fields ride on the ordinary signed channel message (see
:mod:`tau.channels`): ``poll_options``, ``poll_vote`` and
``poll_close`` are covered by the message hash when present, so a
relay cannot strip or swap them without breaking the signature.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from .protocol import (
    ProtocolError,
    clean_group_poll_target as clean_poll_target,
    clean_group_poll_vote as clean_poll_vote,
)
from .tangents.tan import PostError, _clean_poll_options

# A vote or close names its poll by the poll author's destination
# hash and the poll's created_at — the same logical identity group
# polls use, minus the leg bookkeeping groups need.
PollIdentity = Tuple[str, float]


def clean_poll_options(value: Any) -> List[str]:
    """Validate a poll's options with the Tan poll rules (2-8, distinct).

    Raises :class:`tau.tangents.tan.PostError` on a bad list, so
    callers that already handle Tan errors keep one error type.
    """
    return _clean_poll_options(value)


def is_channel_poll(record: Dict[str, Any]) -> bool:
    """True when *record* is a channel poll with usable options."""
    if not isinstance(record, dict) or record.get("poll_options") is None:
        return False
    try:
        _clean_poll_options(record.get("poll_options"))
    except PostError:
        return False
    return True


def is_channel_poll_vote(record: Dict[str, Any]) -> bool:
    """True when *record* is a vote cast on a channel poll."""
    if not isinstance(record, dict) or record.get("poll_vote") is None:
        return False
    try:
        clean_poll_vote(record.get("poll_vote"))
    except ProtocolError:
        return False
    return True


def is_channel_poll_close(record: Dict[str, Any]) -> bool:
    """True when *record* closes a channel poll."""
    if not isinstance(record, dict) or record.get("poll_close") is None:
        return False
    try:
        clean_poll_target(record.get("poll_close"))
    except ProtocolError:
        return False
    return True


def poll_identity(record: Dict[str, Any]) -> Optional[PollIdentity]:
    """A poll record's logical identity, or ``None`` when unresolvable.

    A channel message always carries its author directly, so the
    identity is just the author hash plus the poll's ``created_at``.
    """
    author = record.get("author")
    if not isinstance(author, str) or len(author) != 32:
        return None
    try:
        bytes.fromhex(author)
    except ValueError:
        return None
    created_at = record.get("created_at")
    if isinstance(created_at, bool) or not isinstance(created_at, (int, float)):
        return None
    return (author.lower(), float(created_at))


def _record_author(record: Dict[str, Any]) -> Optional[str]:
    """The destination hash that sent *record*, or ``None``."""
    author = record.get("author")
    if not isinstance(author, str) or len(author) != 32:
        return None
    try:
        bytes.fromhex(author)
    except ValueError:
        return None
    return author.lower()


def channel_poll_tally(
    messages: Sequence[Dict[str, Any]],
    poll: Dict[str, Any],
    *,
    own_hash: Optional[str] = None,
) -> Dict[str, Any]:
    """Derive a channel poll's tally from the channel's stored messages.

    ``messages`` is the channel's history (any order); ``poll`` is
    the poll record.  Returns the options, the vote count per option,
    the total number of voters, whether the poll is closed, and one
    voter record per standing vote — ``{"author", "name", "option",
    "voted_at"}``, newest vote first — plus this node's own standing
    vote under ``"my_vote"`` (an option index, or ``None``) and
    whether this node authored the poll under ``"own"``.
    """
    options = _clean_poll_options(poll.get("poll_options"))
    identity = poll_identity(poll)
    channel = poll.get("channel")
    if identity is None or not isinstance(channel, str):
        return {
            "options": options,
            "counts": [0] * len(options),
            "total": 0,
            "closed": False,
            "voters": [],
            "my_vote": None,
            "own": False,
        }
    author, _created_at = identity
    # The earliest close from the poll's own author freezes the
    # tally: votes sent after it do not count, like a Tan poll's
    # close cutoff.  A close from anyone else is not a close.
    cutoff: Optional[float] = None
    for record in messages:
        if record.get("channel") != channel or not is_channel_poll_close(record):
            continue
        target = record["poll_close"]
        if (target["author"], target["sent_at"]) != identity:
            continue
        if _record_author(record) != author:
            continue
        close_at = record.get("created_at")
        if isinstance(close_at, bool) or not isinstance(close_at, (int, float)):
            continue
        close_at = float(close_at)
        if cutoff is None or close_at < cutoff:
            cutoff = close_at
    # One standing vote per voter: the latest by send time, then by
    # hash, wins — voting again changes your vote.
    standing: Dict[str, Dict[str, Any]] = {}
    for record in messages:
        if record.get("channel") != channel or not is_channel_poll_vote(record):
            continue
        vote = record["poll_vote"]
        target = vote["target"]
        if (target["author"], target["sent_at"]) != identity:
            continue
        option = vote["option"]
        if not 0 <= option < len(options):
            continue
        sent_at = record.get("created_at")
        if isinstance(sent_at, bool) or not isinstance(sent_at, (int, float)):
            continue
        sent_at = float(sent_at)
        if cutoff is not None and sent_at > cutoff:
            continue
        voter = _record_author(record)
        if voter is None:
            continue
        key = (sent_at, str(record.get("hash") or ""))
        previous = standing.get(voter)
        if previous is None or key > previous["_key"]:
            standing[voter] = {
                "_key": key,
                "author": voter,
                "name": str(record.get("name") or ""),
                "option": option,
                "voted_at": sent_at,
            }
    voters = [
        {
            "author": vote["author"],
            "name": vote["name"],
            "option": vote["option"],
            "voted_at": vote["voted_at"],
        }
        for vote in sorted(
            standing.values(),
            key=lambda vote: (
                -vote["voted_at"],
                vote["name"].lower(),
                vote["author"],
            ),
        )
    ]
    counts = [0] * len(options)
    for vote in voters:
        counts[int(vote["option"])] += 1
    my_vote = None
    if own_hash is not None:
        own = standing.get(own_hash.lower())
        if own is not None:
            my_vote = own["option"]
    return {
        "options": options,
        "counts": counts,
        "total": len(voters),
        "closed": cutoff is not None,
        "voters": voters,
        "my_vote": my_vote,
        "own": own_hash is not None and author == own_hash.lower(),
    }


def attach_channel_poll_tallies(
    messages: Sequence[Dict[str, Any]],
    *,
    own_hash: Optional[str] = None,
) -> None:
    """Attach a ``poll`` tally to each channel-poll record, in place.

    Vote and close records are left untouched — callers hide them
    from listings and render the tally on the poll instead.
    """
    for record in messages:
        if is_channel_poll(record):
            record["poll"] = channel_poll_tally(
                messages, record, own_hash=own_hash
            )


def resolve_channel_poll(
    messages: Sequence[Dict[str, Any]], ref: str
) -> Dict[str, Any]:
    """Resolve a poll hash or unique hex prefix to one stored poll.

    Only the poll itself resolves — a vote or close record never
    does.  Raises :class:`ValueError` when the reference is empty,
    matches nothing, or matches more than one poll.
    """
    from .channels import ChannelError

    ref = (ref or "").strip()
    if not ref:
        raise ChannelError("poll must not be empty")
    folded = ref.lower()
    matches = [
        record
        for record in messages
        if is_channel_poll(record)
        and str(record.get("hash") or "").lower().startswith(folded)
    ]
    exact = [
        record
        for record in matches
        if str(record.get("hash") or "").lower() == folded
    ]
    if exact:
        matches = exact
    if len(matches) > 1:
        raise ChannelError(
            f"{len(matches)} polls match {ref!r}; use a longer prefix"
        )
    if not matches:
        raise ChannelError(
            f"no poll {ref!r} in this channel;"
            " votes name a poll from the channel's history"
        )
    return matches[0]
