from __future__ import annotations

import pytest

from tau.group_polls import (
    attach_group_poll_tallies,
    clean_poll_options,
    group_poll_tally,
    is_group_poll,
    is_group_poll_close,
    is_group_poll_vote,
    poll_identity,
)
from tau.tangents.tan import PostError

ALICE = "ab" * 16
BOB = "cd" * 16
CAROL = "ef" * 16
GROUP = "12" * 16
OWN = ALICE


def _record(
    *,
    text="which day?",
    sender=ALICE,
    direction="in",
    sent_at=1000.0,
    seq=1,
    group=GROUP,
    **extra,
):
    record = {
        "from": sender[:8],
        "destination": sender if direction == "in" else "",
        "text": text,
        "sent_at": sent_at,
        "received_at": sent_at,
        "id": f"{seq:032x}",
        "kind": "group",
        "group": group,
        "group_name": "weekend",
        "direction": direction,
        "seq": seq,
    }
    if direction == "out":
        record["leg_sent_at"] = sent_at
    record.update(extra)
    return record


def _poll(**kwargs):
    kwargs.setdefault("poll_options", ["Saturday", "Sunday"])
    return _record(**kwargs)


def _vote(poll, voter, option, sent_at, seq):
    identity = poll_identity(poll, OWN)
    assert identity is not None
    return _record(
        text=f"voted: {identity}",
        sender=voter,
        direction="in" if voter != OWN else "out",
        sent_at=sent_at,
        seq=seq,
        poll_vote={"target": {"author": identity[0], "sent_at": identity[1]}, "option": option},
    )


def _close(poll, closer, sent_at, seq):
    identity = poll_identity(poll, OWN)
    assert identity is not None
    return _record(
        text="closed the poll",
        sender=closer,
        direction="in" if closer != OWN else "out",
        sent_at=sent_at,
        seq=seq,
        poll_close={"author": identity[0], "sent_at": identity[1]},
    )


def test_clean_poll_options_uses_tan_rules():
    assert clean_poll_options(["a", "b"]) == ["a", "b"]
    with pytest.raises(PostError):
        clean_poll_options(["only one"])
    with pytest.raises(PostError):
        clean_poll_options(["dup", "DUP"])


def test_record_classification():
    poll = _poll()
    assert is_group_poll(poll)
    assert not is_group_poll_vote(poll)
    assert not is_group_poll_close(poll)
    vote = _vote(poll, BOB, 0, 1001.0, 2)
    assert is_group_poll_vote(vote)
    assert not is_group_poll(vote)
    close = _close(poll, ALICE, 1002.0, 3)
    assert is_group_poll_close(close)
    assert not is_group_poll(close)
    assert not is_group_poll(_record(text="plain chat"))


def test_poll_identity_uses_leg_sent_at_for_sent_polls():
    poll = _poll(direction="out", sent_at=1000.0)
    poll["sent_at"] = 1005.0  # local record time, not the shared time
    assert poll_identity(poll, OWN) == (ALICE, 1000.0)
    assert poll_identity(_poll(), OWN) == (ALICE, 1000.0)
    assert poll_identity(_poll(direction="out"), None) is None


def test_tally_counts_one_standing_vote_per_voter():
    poll = _poll()
    messages = [
        poll,
        _vote(poll, BOB, 0, 1001.0, 2),
        _vote(poll, CAROL, 1, 1002.0, 3),
        _vote(poll, BOB, 1, 1003.0, 4),  # Bob changes his vote
    ]
    tally = group_poll_tally(messages, poll, own_hash=OWN)
    assert tally["options"] == ["Saturday", "Sunday"]
    assert tally["counts"] == [0, 2]
    assert tally["total"] == 2
    assert tally["closed"] is False
    assert tally["my_vote"] is None
    assert [v["author"] for v in tally["voters"]] == [BOB, CAROL]


def test_tally_ignores_out_of_range_options_and_other_polls():
    poll = _poll()
    other = _poll(sent_at=2000.0, seq=10)
    messages = [
        poll,
        other,
        _vote(poll, BOB, 5, 1001.0, 2),  # no such option
        _vote(other, BOB, 0, 2001.0, 11),
        _vote(poll, CAROL, 0, 1002.0, 3),
    ]
    tally = group_poll_tally(messages, poll, own_hash=OWN)
    assert tally["counts"] == [1, 0]
    assert tally["total"] == 1


def test_tally_freezes_at_author_close_only():
    poll = _poll()
    messages = [
        poll,
        _vote(poll, BOB, 0, 1001.0, 2),
        _close(poll, BOB, 1002.0, 3),  # not the author: no close
        _vote(poll, CAROL, 1, 1003.0, 4),
        _close(poll, ALICE, 1004.0, 5),  # the author closes
        _vote(poll, CAROL, 0, 1005.0, 6),  # after the close: excluded
    ]
    tally = group_poll_tally(messages, poll, own_hash=OWN)
    assert tally["closed"] is True
    assert tally["counts"] == [1, 1]
    assert tally["total"] == 2


def test_tally_tracks_own_vote():
    poll = _poll(sender=BOB)
    messages = [poll, _vote(poll, OWN, 1, 1001.0, 2)]
    tally = group_poll_tally(messages, poll, own_hash=OWN)
    assert tally["my_vote"] == 1
    assert tally["counts"] == [0, 1]


def test_attach_tallies_leaves_votes_untouched():
    poll = _poll()
    vote = _vote(poll, BOB, 0, 1001.0, 2)
    messages = [poll, vote]
    attach_group_poll_tallies(messages, own_hash=OWN)
    assert messages[0]["poll"]["counts"] == [1, 0]
    assert "poll" not in messages[1]
