from __future__ import annotations

import pytest

from tau.channel_polls import (
    attach_channel_poll_tallies,
    channel_poll_tally,
    clean_poll_options,
    is_channel_poll,
    is_channel_poll_close,
    is_channel_poll_vote,
    poll_identity,
    resolve_channel_poll,
)

ALICE = "aa" * 16
BOB = "bb" * 16
CAROL = "cc" * 16


def _poll(author=ALICE, created_at=1000.0, options=("Red", "Blue")):
    return {
        "v": 1,
        "hash": "p" * 64,
        "channel": "general",
        "author": author,
        "key": "k" * 128,
        "name": "alice",
        "text": "which colour?",
        "created_at": created_at,
        "received_at": created_at,
        "sig": "s" * 128,
        "poll_options": list(options),
    }


def _vote(
    author,
    created_at,
    option,
    target_author=ALICE,
    target_sent_at=1000.0,
    channel="general",
    digest="v",
):
    return {
        "v": 1,
        "hash": digest * 64,
        "channel": channel,
        "author": author,
        "key": "k" * 128,
        "name": "voter",
        "text": "voted",
        "created_at": created_at,
        "received_at": created_at,
        "sig": "s" * 128,
        "poll_vote": {
            "target": {"author": target_author, "sent_at": target_sent_at},
            "option": option,
        },
    }


def _close(author, created_at, target_author=ALICE, target_sent_at=1000.0):
    return {
        "v": 1,
        "hash": "c" * 64,
        "channel": "general",
        "author": author,
        "key": "k" * 128,
        "name": "alice",
        "text": "closed the poll",
        "created_at": created_at,
        "received_at": created_at,
        "sig": "s" * 128,
        "poll_close": {"author": target_author, "sent_at": target_sent_at},
    }


def test_clean_poll_options_uses_tan_rules():
    assert clean_poll_options(["a", "b"]) == ["a", "b"]
    with pytest.raises(Exception):
        clean_poll_options(["only one"])
    with pytest.raises(Exception):
        clean_poll_options(["dup", "dup"])


def test_poll_identity_is_author_plus_created_at():
    assert poll_identity(_poll()) == (ALICE, 1000.0)
    assert poll_identity({"author": "not-hex", "created_at": 1000.0}) is None
    assert poll_identity({"author": ALICE}) is None


def test_is_channel_poll_vote_close():
    poll = _poll()
    assert is_channel_poll(poll)
    assert not is_channel_poll_vote(poll)
    assert not is_channel_poll_close(poll)
    assert is_channel_poll_vote(_vote(BOB, 1001.0, 0))
    assert is_channel_poll_close(_close(ALICE, 1002.0))
    assert not is_channel_poll({"text": "plain"})
    assert not is_channel_poll_vote({"poll_vote": {"target": {}, "option": 0}})


def test_tally_counts_one_vote_per_voter():
    poll = _poll()
    messages = [
        poll,
        _vote(BOB, 1001.0, 0, digest="1"),
        _vote(CAROL, 1002.0, 1, digest="2"),
    ]
    tally = channel_poll_tally(messages, poll)
    assert tally["options"] == ["Red", "Blue"]
    assert tally["counts"] == [1, 1]
    assert tally["total"] == 2
    assert tally["closed"] is False
    assert tally["my_vote"] is None
    assert tally["own"] is False


def test_tally_revote_moves_vote_and_my_vote():
    poll = _poll()
    messages = [
        poll,
        _vote(BOB, 1001.0, 0, digest="1"),
        _vote(BOB, 1003.0, 1, digest="2"),
    ]
    tally = channel_poll_tally(messages, poll, own_hash=BOB)
    assert tally["counts"] == [0, 1]
    assert tally["total"] == 1
    assert tally["my_vote"] == 1


def test_tally_own_when_author_is_this_node():
    poll = _poll()
    tally = channel_poll_tally([poll], poll, own_hash=ALICE)
    assert tally["own"] is True
    tally = channel_poll_tally([poll], poll, own_hash=BOB)
    assert tally["own"] is False


def test_tally_close_freezes_later_votes():
    poll = _poll()
    messages = [
        poll,
        _vote(BOB, 1001.0, 0, digest="1"),
        _close(ALICE, 1002.0),
        _vote(CAROL, 1003.0, 1, digest="2"),
    ]
    tally = channel_poll_tally(messages, poll)
    assert tally["closed"] is True
    assert tally["counts"] == [1, 0]
    assert tally["total"] == 1


def test_tally_close_from_non_author_is_not_a_close():
    poll = _poll()
    messages = [poll, _close(BOB, 1002.0), _vote(CAROL, 1003.0, 1, digest="2")]
    tally = channel_poll_tally(messages, poll)
    assert tally["closed"] is False
    assert tally["counts"] == [0, 1]


def test_tally_ignores_other_channel_and_bad_options():
    poll = _poll()
    messages = [
        poll,
        _vote(BOB, 1001.0, 0, channel="other-room", digest="1"),
        _vote(CAROL, 1002.0, 9, digest="2"),
        _vote(BOB, 1003.0, 1, target_author=BOB, digest="3"),
    ]
    tally = channel_poll_tally(messages, poll)
    assert tally["counts"] == [0, 0]
    assert tally["total"] == 0


def test_attach_tallies_only_polls():
    poll = _poll()
    vote = _vote(BOB, 1001.0, 0, digest="1")
    messages = [poll, vote]
    attach_channel_poll_tallies(messages)
    assert messages[0]["poll"]["counts"] == [1, 0]
    assert "poll" not in messages[1]


def test_resolve_channel_poll_by_prefix():
    poll = _poll()
    other = _poll(created_at=2000.0)
    other["hash"] = "q" * 64
    messages = [poll, other, _vote(BOB, 1001.0, 0, digest="1")]
    assert resolve_channel_poll(messages, "p" * 8) is poll
    assert resolve_channel_poll(messages, "p" * 64) is poll
    with pytest.raises(ValueError, match="no poll"):
        resolve_channel_poll(messages, "z" * 8)
    # A vote record never resolves as the poll itself.
    with pytest.raises(ValueError, match="no poll"):
        resolve_channel_poll(messages, "1" * 8)
    # Ambiguous prefix: two polls sharing a prefix.
    twin = _poll(created_at=3000.0)
    twin["hash"] = "p" * 8 + "f" * 56
    with pytest.raises(ValueError, match="use a longer prefix"):
        resolve_channel_poll([poll, twin], "p" * 8)
