"""Group polls: a poll posted as a group message, voted on in-chat.

A poll is a group message whose text is the question and whose
``poll_options`` carry the choices — the same 2-8 distinct, short,
normalised options a Tan poll enforces (see
:func:`tau.tangents.tan._clean_poll_options`).  A vote is a group
message carrying ``poll_vote``: the poll's logical identity plus the
chosen option's index.  A close carries ``poll_close`` with the same
identity.  The identity is the one reactions and retracts already
use — the group, the poll author's destination hash, and the send
time every leg of the poll shares — so a vote or close resolves
against every member's copy of the poll, whatever leg id each holds.

A DM poll is the same shape without the group: the poll is a DM
whose text is the question, a vote is a DM carrying ``poll_vote``,
and a close is a DM carrying ``poll_close``; the identity is the
poll author's destination hash and the send time, and the
conversation is the peer instead of the group.  The tally below is
shared — only the conversation key differs.

The tally is derived from stored messages, never stored: one
standing vote per voter (the latest by send time, then arrival
order), votes past the close are excluded, and only a close from the
poll's own author closes it.  Vote and close records are not chat —
callers hide them from listings and render the tally on the poll
itself instead.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from .protocol import (
    ProtocolError,
    clean_group_poll_target,
    clean_group_poll_vote,
)
from .tangents.tan import PostError, _clean_poll_options

# A vote or close names its poll by the logical identity a reaction
# uses: the poll author's destination hash and the shared send time
# of the poll's legs.
PollIdentity = Tuple[str, float]


def clean_poll_options(value: Any) -> List[str]:
    """Validate a poll's options with the Tan poll rules (2-8, distinct).

    Raises :class:`tau.tangents.tan.PostError` on a bad list, so
    callers that already handle Tan errors keep one error type.
    """
    return _clean_poll_options(value)


def is_group_poll(record: Dict[str, Any]) -> bool:
    """True when *record* is a group poll with usable options."""
    if record.get("kind") != "group":
        return False
    try:
        _clean_poll_options(record.get("poll_options"))
    except PostError:
        return False
    return True


def is_dm_poll(record: Dict[str, Any]) -> bool:
    """True when *record* is a DM poll with usable options.

    A DM record is any record that is not a group record; its kind
    is the message kind (``chat``, ``location``, ...), so the poll
    field is what marks it.
    """
    if record.get("kind") == "group":
        return False
    try:
        _clean_poll_options(record.get("poll_options"))
    except PostError:
        return False
    return True


def is_dm_poll_vote(record: Dict[str, Any]) -> bool:
    """True when *record* is a vote cast on a DM poll."""
    if record.get("kind") == "group":
        return False
    try:
        clean_group_poll_vote(record.get("poll_vote"))
    except ProtocolError:
        return False
    return True


def is_dm_poll_close(record: Dict[str, Any]) -> bool:
    """True when *record* closes a DM poll."""
    if record.get("kind") == "group":
        return False
    try:
        clean_group_poll_target(record.get("poll_close"))
    except ProtocolError:
        return False
    return True


def is_group_poll_vote(record: Dict[str, Any]) -> bool:
    """True when *record* is a vote cast on a group poll."""
    if record.get("kind") != "group":
        return False
    try:
        clean_group_poll_vote(record.get("poll_vote"))
    except ProtocolError:
        return False
    return True


def is_group_poll_close(record: Dict[str, Any]) -> bool:
    """True when *record* closes a group poll."""
    if record.get("kind") != "group":
        return False
    try:
        clean_group_poll_target(record.get("poll_close"))
    except ProtocolError:
        return False
    return True


def poll_identity(
    record: Dict[str, Any], own_hash: Optional[str]
) -> Optional[PollIdentity]:
    """A poll record's logical identity, or ``None`` when unresolvable.

    The author is the sender's hash for a received poll and this
    node's hash for one it sent; the send time is the legs' shared
    time (``leg_sent_at`` on a sent record, ``sent_at`` otherwise).
    Without ``own_hash`` a sent poll cannot be attributed, so it has
    no identity here.  Group and DM polls share this identity: the
    conversation (group id, or the peer) is the caller's filter, not
    part of the identity.
    """
    if record.get("poll_options") is None:
        return None
    if record.get("direction") == "out":
        author = own_hash
    else:
        author = record.get("destination")
    if not isinstance(author, str) or len(author) != 32:
        return None
    try:
        bytes.fromhex(author)
    except ValueError:
        return None
    sent_at = record.get("leg_sent_at", record.get("sent_at"))
    if isinstance(sent_at, bool) or not isinstance(sent_at, (int, float)):
        return None
    return (author.lower(), float(sent_at))


def _logical_sent_at(record: Dict[str, Any]) -> Optional[float]:
    """The send time every leg of *record* shares.

    A sent record carries it under ``leg_sent_at`` (its own
    ``sent_at`` is the local record time); a received record's
    ``sent_at`` already is the shared time.
    """
    sent_at = record.get("leg_sent_at", record.get("sent_at"))
    if isinstance(sent_at, bool) or not isinstance(sent_at, (int, float)):
        return None
    return float(sent_at)


def _record_author(
    record: Dict[str, Any], own_hash: Optional[str]
) -> Optional[str]:
    """The destination hash that sent *record*, or ``None``."""
    if record.get("direction") == "out":
        author = own_hash
    else:
        author = record.get("destination")
    if not isinstance(author, str) or len(author) != 32:
        return None
    try:
        bytes.fromhex(author)
    except ValueError:
        return None
    return author.lower()


def _poll_tally(
    messages: Sequence[Dict[str, Any]],
    poll: Dict[str, Any],
    *,
    own_hash: Optional[str] = None,
    is_vote: Any = None,
    is_close: Any = None,
    conversation_field: str = "group",
) -> Dict[str, Any]:
    """Derive a poll's tally from the conversation's stored messages.

    ``messages`` is the conversation's history (any order); ``poll``
    is the poll record.  The conversation is the field that keys the
    history — ``"group"`` for a group poll, ``"destination"`` (the
    peer) for a DM poll — and ``is_vote`` / ``is_close`` pick the
    vote and close records out of it.  Returns the options, the vote
    count per option, the total number of voters, whether the poll is
    closed, and one voter record per standing vote — ``{"author",
    "name", "option", "voted_at"}``, newest vote first — plus this
    node's own standing vote under ``"my_vote"`` (an option index,
    or ``None``).
    """
    options = _clean_poll_options(poll.get("poll_options"))
    identity = poll_identity(poll, own_hash)
    conversation = poll.get(conversation_field)
    if identity is None or not isinstance(conversation, str):
        return {
            "options": options,
            "counts": [0] * len(options),
            "total": 0,
            "closed": False,
            "voters": [],
            "my_vote": None,
        }
    author, _sent_at = identity
    # The earliest close from the poll's own author freezes the
    # tally: votes sent after it do not count, like a Tan poll's
    # close cutoff.  A close from anyone else is not a close.
    cutoff: Optional[float] = None
    for record in messages:
        if record.get(conversation_field) != conversation or not is_close(
            record
        ):
            continue
        target = record["poll_close"]
        if (target["author"], target["sent_at"]) != identity:
            continue
        if _record_author(record, own_hash) != author:
            continue
        close_at = _logical_sent_at(record)
        if close_at is None:
            continue
        if cutoff is None or close_at < cutoff:
            cutoff = close_at
    # One standing vote per voter: the latest by send time, then by
    # arrival order, wins — voting again changes your vote.
    standing: Dict[str, Dict[str, Any]] = {}
    for record in messages:
        if record.get(conversation_field) != conversation or not is_vote(
            record
        ):
            continue
        vote = record["poll_vote"]
        target = vote["target"]
        if (target["author"], target["sent_at"]) != identity:
            continue
        option = vote["option"]
        if not 0 <= option < len(options):
            continue
        sent_at = _logical_sent_at(record)
        if sent_at is None:
            continue
        if cutoff is not None and sent_at > cutoff:
            continue
        voter = _record_author(record, own_hash)
        if voter is None:
            continue
        key = (sent_at, int(record.get("seq") or 0))
        previous = standing.get(voter)
        if previous is None or key > previous["_key"]:
            standing[voter] = {
                "_key": key,
                "author": voter,
                "name": str(record.get("from") or ""),
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
    }


def group_poll_tally(
    messages: Sequence[Dict[str, Any]],
    poll: Dict[str, Any],
    *,
    own_hash: Optional[str] = None,
) -> Dict[str, Any]:
    """Derive a group poll's tally from the group's stored messages.

    ``messages`` is the group's history (any order); ``poll`` is the
    poll record.  See :func:`_poll_tally` for the shape returned.
    """
    return _poll_tally(
        messages,
        poll,
        own_hash=own_hash,
        is_vote=is_group_poll_vote,
        is_close=is_group_poll_close,
        conversation_field="group",
    )


def dm_poll_tally(
    messages: Sequence[Dict[str, Any]],
    poll: Dict[str, Any],
    *,
    own_hash: Optional[str] = None,
) -> Dict[str, Any]:
    """Derive a DM poll's tally from the conversation's stored messages.

    ``messages`` is the DM history with the peer (any order); ``poll``
    is the poll record.  The conversation key is the peer's
    destination hash, which is ``destination`` on both sent and
    received records.  See :func:`_poll_tally` for the shape returned.
    """
    return _poll_tally(
        messages,
        poll,
        own_hash=own_hash,
        is_vote=is_dm_poll_vote,
        is_close=is_dm_poll_close,
        conversation_field="destination",
    )


def attach_group_poll_tallies(
    messages: Sequence[Dict[str, Any]],
    *,
    own_hash: Optional[str] = None,
) -> None:
    """Attach a ``poll`` tally to each group-poll record, in place.

    Vote and close records are left untouched — callers hide them
    from listings and render the tally on the poll instead.
    """
    for record in messages:
        if is_group_poll(record):
            record["poll"] = group_poll_tally(
                messages, record, own_hash=own_hash
            )


def attach_dm_poll_tallies(
    messages: Sequence[Dict[str, Any]],
    *,
    own_hash: Optional[str] = None,
) -> None:
    """Attach a ``poll`` tally to each DM-poll record, in place.

    Vote and close records are left untouched — callers hide them
    from listings and render the tally on the poll instead.
    """
    for record in messages:
        if is_dm_poll(record):
            record["poll"] = dm_poll_tally(
                messages, record, own_hash=own_hash
            )
