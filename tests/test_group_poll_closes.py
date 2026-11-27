from __future__ import annotations

import json
import threading
import time
from types import SimpleNamespace

import pytest

from tau.config import TauConfig
from tau.group_poll_closes import (
    CLOSE_PASS_BUDGET_SECONDS,
    MAX_POLL_CLOSE_AFTER,
    GroupPollClosesError,
    close_due_group_polls,
    drop_group_poll_close,
    due_group_poll_closes,
    group_poll_close_key,
    load_group_poll_closes,
    schedule_group_poll_close,
)
from tau.inbox import Inbox
from tau.node import TauNode

OWN_HASH = "ab" * 16
OTHER_HASH = "cd" * 16
THIRD_HASH = "ef" * 16
GROUP_ID = "12" * 16


def _key(group=GROUP_ID, author=OWN_HASH, sent_at=1000.0):
    return group_poll_close_key(group, author, sent_at)


def _poll_record(*, author=OWN_HASH, sent_at=1000.0, direction="out"):
    record = {
        "from": "alice",
        "destination": "",
        "text": "which day?",
        "sent_at": 1001.0,
        "received_at": 1001.0,
        "id": "p" * 32,
        "kind": "group",
        "group": GROUP_ID,
        "group_name": "weekend",
        "direction": direction,
        "poll_options": ["Saturday", "Sunday"],
    }
    if direction == "out":
        record["leg_sent_at"] = sent_at
    else:
        record["destination"] = author
        record["sent_at"] = sent_at
    return record


def _close_record(*, author=OWN_HASH, sent_at=1000.0):
    return {
        "from": "alice",
        "destination": author,
        "text": "closed the poll",
        "sent_at": 1004.0,
        "received_at": 1005.0,
        "id": "c" * 31 + "1",
        "kind": "group",
        "group": GROUP_ID,
        "group_name": "weekend",
        "direction": "in",
        "poll_close": {"author": author, "sent_at": sent_at},
    }


class _FakeGroups:
    def __init__(self, groups=None):
        self._groups = groups or {}
        self._retired_groups = {}

    def find(self, group_id):
        return self._groups.get(group_id)

    def get_retired(self, group_id):
        # Mirror GroupStore.get_retired: the retained record of a
        # retired group, for closes addressed to a rotated roster.
        return self._retired_groups.get(group_id)


class _FakeNode:
    """A serving node stub: a group, an inbox, and a canned fan-out."""

    def __init__(self, tmp_path, *, group=True, send_results=None):
        self.destination_hexhash = OWN_HASH
        self.config = SimpleNamespace(
            group_poll_closes_path=tmp_path / "group-poll-closes.json"
        )
        self._stop_event = threading.Event()
        self.groups = _FakeGroups(
            {GROUP_ID: {"id": GROUP_ID, "name": "weekend"}}
            if group
            else {}
        )
        self.inbox = Inbox(tmp_path / "inbox.json")
        self.send_calls = []
        self.send_results = (
            [{"to": OTHER_HASH, "delivered": True, "id": "f" * 32}]
            if send_results is None
            else send_results
        )
        self.send_error = None

    def send_group_message(self, group, text, **kwargs):
        self.send_calls.append((group, text, kwargs))
        if self.send_error is not None:
            raise self.send_error
        return {"ok": True, "results": list(self.send_results)}


def _seed_poll(node, **kwargs):
    node.inbox.append(_poll_record(**kwargs))


def test_schedule_and_load_round_trip(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    close_at = schedule_group_poll_close(
        path, GROUP_ID, OWN_HASH, 1000.0, 5000.0, now=1000.0
    )
    assert close_at == 5000.0
    assert load_group_poll_closes(path) == {
        _key(): {
            "group": GROUP_ID,
            "author": OWN_HASH,
            "sent_at": 1000.0,
            "close_at": 5000.0,
        }
    }


def test_schedule_replaces_the_deadline_for_the_same_poll(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 5000.0, now=1000.0)
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 9000.0, now=1000.0)
    assert load_group_poll_closes(path)[_key()]["close_at"] == 9000.0


def test_schedule_refuses_a_deadline_in_the_past(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    with pytest.raises(GroupPollClosesError, match="in the future"):
        schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 500.0, now=1000.0)
    assert load_group_poll_closes(path) == {}


def test_schedule_refuses_bad_group_author_or_times(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    with pytest.raises(GroupPollClosesError, match="not a group id"):
        schedule_group_poll_close(path, "short", OWN_HASH, 1000.0, 5000.0, now=1000.0)
    with pytest.raises(GroupPollClosesError, match="not a author hash"):
        schedule_group_poll_close(path, GROUP_ID, "short", 1000.0, 5000.0, now=1000.0)
    with pytest.raises(GroupPollClosesError, match="not a close time"):
        schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, -5.0, now=1000.0)
    assert load_group_poll_closes(path) == {}


def test_due_returns_only_due_closes_soonest_first(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 3000.0, now=1000.0)
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 2000.0, 2000.0, now=1000.0)
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 3000.0, 9000.0, now=1000.0)
    due = due_group_poll_closes(path, now=2500.0)
    assert [entry["sent_at"] for _, entry in due] == [2000.0]
    due = due_group_poll_closes(path, now=3500.0)
    assert [entry["sent_at"] for _, entry in due] == [2000.0, 1000.0]
    assert due_group_poll_closes(path, now=1000.0) == []


def test_drop_removes_only_the_named_close(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 5000.0, now=1000.0)
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 2000.0, 6000.0, now=1000.0)
    assert drop_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0) is True
    assert drop_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0) is False
    assert list(load_group_poll_closes(path)) == [_key(sent_at=2000.0)]


def test_load_missing_file_is_empty_and_corrupt_file_raises(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    assert load_group_poll_closes(path) == {}
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(GroupPollClosesError, match="unusable"):
        load_group_poll_closes(path)
    path.write_text(json.dumps({_key(): {"group": "short"}}), encoding="utf-8")
    with pytest.raises(GroupPollClosesError, match="not a group id"):
        load_group_poll_closes(path)
    path.write_text(
        json.dumps(
            {
                _key(): {
                    "group": GROUP_ID,
                    "author": OWN_HASH,
                    "sent_at": 1000.0,
                    "close_at": 5000.0,
                },
                "bogus-key": {
                    "group": GROUP_ID,
                    "author": OWN_HASH,
                    "sent_at": 2000.0,
                    "close_at": 5000.0,
                },
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(GroupPollClosesError, match="does not match its entry"):
        load_group_poll_closes(path)


def test_close_due_group_polls_closes_and_drops(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 2000.0, 9000.0, now=1000.0)
    node = _FakeNode(tmp_path)
    _seed_poll(node)

    result = close_due_group_polls(path, node, now=2500.0)
    assert result == {"closed": 1, "dropped": 0, "kept": 0}
    assert len(node.send_calls) == 1
    group, text, kwargs = node.send_calls[0]
    assert group["id"] == GROUP_ID
    assert text == "closed the poll"
    assert kwargs["poll_close"] == {"author": OWN_HASH, "sent_at": 1000.0}
    assert load_group_poll_closes(path) == {
        _key(sent_at=2000.0): {
            "group": GROUP_ID,
            "author": OWN_HASH,
            "sent_at": 2000.0,
            "close_at": 9000.0,
        }
    }


def test_close_due_group_polls_drops_a_poll_that_is_gone(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    node = _FakeNode(tmp_path)  # no poll seeded

    result = close_due_group_polls(path, node, now=2500.0)
    assert result == {"closed": 0, "dropped": 1, "kept": 0}
    assert node.send_calls == []
    assert load_group_poll_closes(path) == {}


def test_close_due_group_polls_drops_a_poll_that_is_not_ours(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OTHER_HASH, 1000.0, 2000.0, now=1000.0)
    node = _FakeNode(tmp_path)
    _seed_poll(node, author=OTHER_HASH, direction="in")

    result = close_due_group_polls(path, node, now=2500.0)
    assert result == {"closed": 0, "dropped": 1, "kept": 0}
    assert node.send_calls == []
    assert load_group_poll_closes(path) == {}


def test_close_due_group_polls_drops_a_manually_closed_poll(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    node = _FakeNode(tmp_path)
    _seed_poll(node)
    node.inbox.append(_close_record())

    result = close_due_group_polls(path, node, now=2500.0)
    assert result == {"closed": 0, "dropped": 1, "kept": 0}
    assert node.send_calls == []
    assert load_group_poll_closes(path) == {}


def test_close_due_group_polls_drops_a_group_that_is_gone(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    node = _FakeNode(tmp_path, group=False)
    _seed_poll(node)

    result = close_due_group_polls(path, node, now=2500.0)
    assert result == {"closed": 0, "dropped": 1, "kept": 0}
    assert node.send_calls == []
    assert load_group_poll_closes(path) == {}


def test_close_due_group_polls_drops_terminal_node_errors(tmp_path):
    from tau.node import NodeError

    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    node = _FakeNode(tmp_path)
    _seed_poll(node)
    node.send_error = NodeError("group record has no creator")

    result = close_due_group_polls(path, node, now=2500.0)
    assert result == {"closed": 0, "dropped": 1, "kept": 0}
    assert load_group_poll_closes(path) == {}


def test_close_due_group_polls_keeps_transient_failures(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    node = _FakeNode(tmp_path)
    _seed_poll(node)
    node.send_error = RuntimeError("reticulum is down")

    result = close_due_group_polls(path, node, now=2500.0)
    assert result == {"closed": 0, "dropped": 0, "kept": 1}
    assert load_group_poll_closes(path) == {
        _key(): {
            "group": GROUP_ID,
            "author": OWN_HASH,
            "sent_at": 1000.0,
            "close_at": 2000.0,
        }
    }


def test_close_due_group_polls_keeps_a_close_that_reached_no_member(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    node = _FakeNode(
        tmp_path,
        send_results=[
            {
                "to": OTHER_HASH,
                "ok": False,
                "error": "could not queue: outbox is full",
                "id": "f" * 32,
            }
        ],
    )
    _seed_poll(node)

    result = close_due_group_polls(path, node, now=2500.0)
    assert result == {"closed": 0, "dropped": 0, "kept": 1}
    assert load_group_poll_closes(path) == {
        _key(): {
            "group": GROUP_ID,
            "author": OWN_HASH,
            "sent_at": 1000.0,
            "close_at": 2000.0,
        }
    }
    # The first failure backs off 30s: the close is not due again
    # until 2530, even though its deadline has passed.
    assert due_group_poll_closes(path, now=2529.0) == []
    result = close_due_group_polls(path, node, now=2520.0)
    assert result == {"closed": 0, "dropped": 0, "kept": 0}
    assert len(node.send_calls) == 1
    assert due_group_poll_closes(path, now=2530.0) != []


def test_close_due_group_polls_drops_a_close_every_member_refused(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    node = _FakeNode(
        tmp_path,
        send_results=[
            {"to": OTHER_HASH, "ok": False, "error": "not_allowed", "id": "f" * 32}
        ],
    )
    _seed_poll(node)

    result = close_due_group_polls(path, node, now=2500.0)
    assert result == {"closed": 0, "dropped": 1, "kept": 0}
    assert load_group_poll_closes(path) == {}


def test_close_due_group_polls_counts_a_queued_close_as_closed(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    node = _FakeNode(
        tmp_path,
        send_results=[
            {"to": OTHER_HASH, "queued": True, "id": "f" * 32},
            {"to": THIRD_HASH, "ok": False, "error": "not_allowed", "id": "e" * 32},
        ],
    )
    _seed_poll(node)

    result = close_due_group_polls(path, node, now=2500.0)
    assert result == {"closed": 1, "dropped": 0, "kept": 0}
    assert load_group_poll_closes(path) == {}


def test_close_due_group_polls_stops_when_asked_or_over_budget(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 2000.0, 2000.0, now=1000.0)
    node = _FakeNode(tmp_path)
    _seed_poll(node)
    _seed_poll(node, sent_at=2000.0)

    result = close_due_group_polls(path, node, now=2500.0, should_stop=lambda: True)
    assert result == {"closed": 0, "dropped": 0, "kept": 2}
    assert node.send_calls == []

    result = close_due_group_polls(
        path, node, now=2500.0, deadline=time.monotonic() - 1.0
    )
    assert result == {"closed": 0, "dropped": 0, "kept": 2}
    assert node.send_calls == []


def test_close_retry_backoff_doubles_on_each_failure(tmp_path):
    from tau.group_poll_closes import _close_backoff_until

    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    node = _FakeNode(
        tmp_path,
        send_results=[
            {
                "to": OTHER_HASH,
                "ok": False,
                "error": "could not queue: outbox is full",
                "id": "f" * 32,
            }
        ],
    )
    _seed_poll(node)
    # 30s after the first failure, 60s after the second, 120s
    # after the third — and the deadline itself never moves.
    close_due_group_polls(path, node, now=2500.0)
    assert _close_backoff_until(path, _key()) == 2530.0
    close_due_group_polls(path, node, now=2530.0)
    assert _close_backoff_until(path, _key()) == 2590.0
    close_due_group_polls(path, node, now=2590.0)
    assert _close_backoff_until(path, _key()) == 2710.0
    assert load_group_poll_closes(path)[_key()]["close_at"] == 2000.0


def test_close_backoff_clears_once_the_close_lands(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    node = _FakeNode(
        tmp_path,
        send_results=[
            {
                "to": OTHER_HASH,
                "ok": False,
                "error": "could not queue: outbox is full",
                "id": "f" * 32,
            }
        ],
    )
    _seed_poll(node)
    result = close_due_group_polls(path, node, now=2500.0)
    assert result == {"closed": 0, "dropped": 0, "kept": 1}
    assert due_group_poll_closes(path, now=2520.0) == []

    # The close lands on the retry: the schedule is dropped and the
    # backoff with it, so a fresh schedule for the same poll is due
    # on its own deadline, not the old retry time.
    node.send_results = [{"to": OTHER_HASH, "delivered": True, "id": "f" * 32}]
    result = close_due_group_polls(path, node, now=2530.0)
    assert result == {"closed": 1, "dropped": 0, "kept": 0}
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 3000.0, now=2600.0)
    assert [entry["close_at"] for _, entry in due_group_poll_closes(path, now=3050.0)] == [
        3000.0
    ]


def test_close_due_group_polls_skips_a_corrupt_schedule_file(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    path.write_text("{not json", encoding="utf-8")
    node = _FakeNode(tmp_path)
    result = close_due_group_polls(path, node, now=2500.0)
    assert result == {"closed": 0, "dropped": 0, "kept": 0}
    assert node.send_calls == []


def test_node_close_hook_skips_without_a_schedule_path(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    node = SimpleNamespace(config=SimpleNamespace(), _stop_event=threading.Event())
    TauNode._close_due_group_polls(node)
    assert load_group_poll_closes(path) == {
        _key(): {
            "group": GROUP_ID,
            "author": OWN_HASH,
            "sent_at": 1000.0,
            "close_at": 2000.0,
        }
    }


def test_node_close_hook_closes_through_the_node(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    node = _FakeNode(tmp_path)
    _seed_poll(node)
    TauNode._close_due_group_polls(node)
    assert len(node.send_calls) == 1
    assert node.send_calls[0][2]["poll_close"] == {
        "author": OWN_HASH,
        "sent_at": 1000.0,
    }
    assert load_group_poll_closes(path) == {}


def test_close_due_group_polls_closes_a_real_poll(tmp_path):
    from tau.peers import Peer, PeerRegistry

    config = TauConfig.resolve(data_dir=tmp_path, name="alice")
    node = TauNode(config, registry=PeerRegistry(config.peers_path))
    node.destination = SimpleNamespace(
        hexhash=OWN_HASH, hash=bytes.fromhex(OWN_HASH)
    )
    node.reticulum = object()  # started enough for queueing
    group = node.groups.create(
        "weekend", [OWN_HASH, OTHER_HASH, THIRD_HASH], created_by=OWN_HASH
    )
    calls = []

    def fake_request(peer, path, payload, timeout, *, deadline=None):
        calls.append((peer.hexhash, path, payload, deadline))
        return {"ok": True}

    def fake_resolve(target, timeout=10.0):
        if target == OTHER_HASH:
            now = time.time()
            return Peer(
                destination_hash=bytes.fromhex(target),
                name="bob",
                hops=1,
                first_seen=now,
                last_seen=now,
                public_key=b"\x00" * 64,
            )
        from tau.node import NodeError

        raise NodeError("unreachable")

    node._request = fake_request  # type: ignore[method-assign]
    node.resolve_peer = fake_resolve  # type: ignore[method-assign]
    sent = node.send_group_message(
        group, "which day?", poll_options=["Saturday", "Sunday"]
    )
    assert sent["ok"] is True
    poll_sent_at = sent["sent_at"]
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(
        path,
        group["id"],
        OWN_HASH,
        poll_sent_at,
        poll_sent_at + 3600.0,
        now=poll_sent_at,
    )

    result = close_due_group_polls(path, node, now=poll_sent_at + 3700.0)
    assert result == {"closed": 1, "dropped": 0, "kept": 0}
    assert load_group_poll_closes(path) == {}
    # The close went to the reachable member as a group message
    # carrying the poll_close target, and was queued for the other.
    from tau.group_polls import group_poll_tally
    from tau.protocol import parse_group_message

    close_calls = [
        call for call in calls if call[1] == "/tau/group"
    ]
    assert len(close_calls) == 2  # poll + close to the reachable member
    parsed = parse_group_message(close_calls[1][2])
    assert parsed["poll_close"] == {
        "author": OWN_HASH,
        "sent_at": poll_sent_at,
    }
    history = node.inbox.list_messages(
        limit=None, group=group["id"], direction=None
    )
    poll = next(
        record
        for record in history
        if record.get("poll_options") is not None
    )
    assert group_poll_tally(history, poll, own_hash=OWN_HASH)["closed"] is True


def test_close_gives_up_after_the_attempt_limit(tmp_path, monkeypatch):
    import tau.group_poll_closes as closes

    monkeypatch.setattr(closes, "CLOSE_RETRY_MAX_ATTEMPTS", 3)
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    node = _FakeNode(
        tmp_path,
        send_results=[
            {
                "to": OTHER_HASH,
                "ok": False,
                "error": "could not queue: outbox is full",
                "id": "f" * 32,
            }
        ],
    )
    _seed_poll(node)
    # Each pass is past the backoff, so every pass is a real attempt.
    assert close_due_group_polls(path, node, now=2500.0) == {
        "closed": 0,
        "dropped": 0,
        "kept": 1,
    }
    assert close_due_group_polls(path, node, now=6100.0) == {
        "closed": 0,
        "dropped": 0,
        "kept": 1,
    }
    assert close_due_group_polls(path, node, now=9700.0) == {
        "closed": 0,
        "dropped": 1,
        "kept": 0,
    }
    assert load_group_poll_closes(path) == {}
    assert len(node.send_calls) == 3


def test_close_that_reaches_no_leg_keeps_its_schedule(tmp_path):
    from tau.group_poll_closes import _close_backoff_until

    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    node = _FakeNode(tmp_path, send_results=[])
    node.groups._groups[GROUP_ID]["members"] = [OWN_HASH, OTHER_HASH]
    _seed_poll(node)
    result = close_due_group_polls(path, node, now=2500.0)
    assert result == {"closed": 0, "dropped": 0, "kept": 1}
    assert _close_backoff_until(path, _key()) == 2530.0
    assert _key() in load_group_poll_closes(path)


def test_close_sends_to_the_retired_original_roster(tmp_path):
    path = tmp_path / "group-poll-closes.json"
    schedule_group_poll_close(path, GROUP_ID, OWN_HASH, 1000.0, 2000.0, now=1000.0)
    node = _FakeNode(tmp_path, group=False)
    node.groups._retired_groups[GROUP_ID] = {
        "id": GROUP_ID,
        "name": "weekend",
        "creator": OWN_HASH,
        "members": [OWN_HASH, OTHER_HASH],
    }
    _seed_poll(node)
    result = close_due_group_polls(path, node, now=2500.0)
    assert result == {"closed": 1, "dropped": 0, "kept": 0}
    assert node.send_calls[0][0]["id"] == GROUP_ID
    assert load_group_poll_closes(path) == {}


def test_max_close_after_is_the_seven_day_horizon():
    assert MAX_POLL_CLOSE_AFTER == 7 * 24 * 60 * 60


def test_close_pass_budget_is_shared_with_tan_closes():
    assert CLOSE_PASS_BUDGET_SECONDS == 20.0
