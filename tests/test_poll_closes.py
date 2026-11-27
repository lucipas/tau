from __future__ import annotations

import json
import time
from types import SimpleNamespace

import pytest

from tau.node import TauNode
from tau.poll_closes import (
    CLOSE_PASS_BUDGET_SECONDS,
    MAX_POLL_CLOSE_AFTER,
    PollClosesError,
    close_due_polls,
    drop_poll_close,
    due_poll_closes,
    load_poll_closes,
    schedule_poll_close,
)


def _hash(letter: str) -> str:
    return letter * 64


def test_schedule_and_load_round_trip(tmp_path):
    path = tmp_path / "poll-closes.json"
    close_at = schedule_poll_close(path, _hash("a"), 5000.0, now=1000.0)
    assert close_at == 5000.0
    assert load_poll_closes(path) == {_hash("a"): 5000.0}


def test_schedule_replaces_the_deadline_for_the_same_poll(tmp_path):
    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, _hash("a"), 5000.0, now=1000.0)
    schedule_poll_close(path, _hash("a"), 9000.0, now=1000.0)
    assert load_poll_closes(path) == {_hash("a"): 9000.0}


def test_schedule_refuses_a_deadline_in_the_past(tmp_path):
    path = tmp_path / "poll-closes.json"
    with pytest.raises(PollClosesError, match="in the future"):
        schedule_poll_close(path, _hash("a"), 500.0, now=1000.0)
    assert load_poll_closes(path) == {}


def test_schedule_refuses_a_bad_target_or_time(tmp_path):
    path = tmp_path / "poll-closes.json"
    with pytest.raises(PollClosesError, match="not a post hash"):
        schedule_poll_close(path, "not-a-hash", 5000.0, now=1000.0)
    with pytest.raises(PollClosesError, match="not a close time"):
        schedule_poll_close(path, _hash("a"), -5.0, now=1000.0)
    assert load_poll_closes(path) == {}


def test_due_returns_only_due_closes_soonest_first(tmp_path):
    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, _hash("b"), 3000.0, now=1000.0)
    schedule_poll_close(path, _hash("a"), 2000.0, now=1000.0)
    schedule_poll_close(path, _hash("c"), 9000.0, now=1000.0)
    assert due_poll_closes(path, now=2500.0) == [(_hash("a"), 2000.0)]
    assert due_poll_closes(path, now=3500.0) == [
        (_hash("a"), 2000.0),
        (_hash("b"), 3000.0),
    ]
    assert due_poll_closes(path, now=1000.0) == []


def test_drop_removes_only_the_named_close(tmp_path):
    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, _hash("a"), 5000.0, now=1000.0)
    schedule_poll_close(path, _hash("b"), 6000.0, now=1000.0)
    assert drop_poll_close(path, _hash("a")) is True
    assert drop_poll_close(path, _hash("a")) is False
    assert load_poll_closes(path) == {_hash("b"): 6000.0}


def test_load_missing_file_is_empty_and_corrupt_file_raises(tmp_path):
    path = tmp_path / "poll-closes.json"
    assert load_poll_closes(path) == {}
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(PollClosesError, match="unusable"):
        load_poll_closes(path)
    path.write_text(json.dumps({"a": 5000.0}), encoding="utf-8")
    with pytest.raises(PollClosesError, match="not a post hash"):
        load_poll_closes(path)


class _FakeTanApp:
    def __init__(self, *, fail_with=None, publish_result=None, node=True):
        self.closed = []
        self.published = []
        self.publish_calls = []
        self.fail_with = fail_with
        self.publish_result = (
            {"delivered": 1, "peers": 1, "errors": [], "unsent": []}
            if publish_result is None
            else publish_result
        )
        # A node is attached unless the test says otherwise: without
        # one the sweep records the close locally, like a manual
        # close_poll with no node, and publishes nothing.
        self.node = object() if node else None
        self.gossip_fanout = 3

    def close_poll(self, target, *, publish=False, targets=None, ttl=3):
        if self.fail_with is not None:
            raise self.fail_with
        self.closed.append((target, publish))
        return {"hash": "f" * 64, "target": target}

    def get_poll_close(self, target):
        for closed_target, _publish in self.closed:
            if closed_target == target:
                return {"hash": "f" * 64, "target": target}
        return None

    def publish(self, record, **kwargs):
        self.published.append(record)
        self.publish_calls.append(kwargs)
        return dict(self.publish_result)


def test_close_due_polls_closes_and_drops(tmp_path):
    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, _hash("a"), 2000.0, now=1000.0)
    schedule_poll_close(path, _hash("b"), 9000.0, now=1000.0)
    app = _FakeTanApp()

    result = close_due_polls(path, app, now=2500.0)
    assert result == {"closed": 1, "dropped": 0, "kept": 0}
    assert app.closed == [(_hash("a"), False)]
    assert app.published == [{"hash": "f" * 64, "target": _hash("a")}]
    assert load_poll_closes(path) == {_hash("b"): 9000.0}


def test_close_due_polls_drops_terminal_failures_and_keeps_transient_ones(
    tmp_path,
):
    from tau.tangents.tan import TanError

    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, _hash("a"), 2000.0, now=1000.0)
    schedule_poll_close(path, _hash("b"), 2000.0, now=1000.0)

    app = _FakeTanApp(fail_with=TanError("poll is already closed"))
    result = close_due_polls(path, app, now=2500.0)
    assert result == {"closed": 0, "dropped": 2, "kept": 0}
    assert load_poll_closes(path) == {}

    schedule_poll_close(path, _hash("a"), 2000.0, now=1000.0)
    app = _FakeTanApp(fail_with=RuntimeError("database is locked"))
    result = close_due_polls(path, app, now=2500.0)
    assert result == {"closed": 0, "dropped": 0, "kept": 1}
    assert load_poll_closes(path) == {_hash("a"): 2000.0}


def test_close_due_polls_stops_when_asked_or_over_budget(tmp_path):
    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, _hash("a"), 2000.0, now=1000.0)
    schedule_poll_close(path, _hash("b"), 2000.0, now=1000.0)
    app = _FakeTanApp()

    result = close_due_polls(path, app, now=2500.0, should_stop=lambda: True)
    assert result == {"closed": 0, "dropped": 0, "kept": 2}
    assert app.closed == []
    assert load_poll_closes(path) == {
        _hash("a"): 2000.0,
        _hash("b"): 2000.0,
    }

    result = close_due_polls(
        path, app, now=2500.0, deadline=time.monotonic() - 1.0
    )
    assert result == {"closed": 0, "dropped": 0, "kept": 2}
    assert app.closed == []


def test_close_due_polls_publishes_with_fanout_stop_and_deadline(tmp_path):
    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, _hash("a"), 2000.0, now=1000.0)
    app = _FakeTanApp()
    stopped = False

    result = close_due_polls(
        path, app, now=2500.0, should_stop=lambda: stopped
    )
    assert result == {"closed": 1, "dropped": 0, "kept": 0}
    assert len(app.publish_calls) == 1
    call = app.publish_calls[0]
    assert call["fanout"] == 3
    assert call["should_stop"] is not None
    assert call["deadline"] is not None
    assert call["deadline"] <= time.monotonic() + CLOSE_PASS_BUDGET_SECONDS


def test_close_due_polls_keeps_a_close_that_reached_no_peer(tmp_path):
    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, _hash("a"), 2000.0, now=1000.0)
    app = _FakeTanApp(
        publish_result={
            "delivered": 0,
            "peers": 2,
            "errors": [
                {"peer": "aa", "error": "timeout"},
                {"peer": "bb", "error": "timeout"},
            ],
            "unsent": [],
        }
    )

    result = close_due_polls(path, app, now=2500.0)
    assert result == {"closed": 0, "dropped": 0, "kept": 1}
    assert load_poll_closes(path) == {_hash("a"): 2000.0}
    assert len(app.closed) == 1  # created once …

    # … and the next pass re-sends the same close instead of
    # creating a second one.
    app.publish_result = {
        "delivered": 1,
        "peers": 2,
        "errors": [{"peer": "bb", "error": "timeout"}],
        "unsent": [],
    }
    result = close_due_polls(path, app, now=2600.0)
    assert result == {"closed": 1, "dropped": 0, "kept": 0}
    assert len(app.closed) == 1
    assert len(app.published) == 2
    assert app.published[0] == app.published[1]


def test_close_due_polls_drops_a_close_that_reached_a_peer(tmp_path):
    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, _hash("a"), 2000.0, now=1000.0)
    app = _FakeTanApp(
        publish_result={
            "delivered": 1,
            "peers": 3,
            "errors": [{"peer": "cc", "error": "timeout"}],
            "unsent": [],
        }
    )
    result = close_due_polls(path, app, now=2500.0)
    assert result == {"closed": 1, "dropped": 0, "kept": 0}
    assert load_poll_closes(path) == {}


def test_close_due_polls_drops_a_close_with_no_peers_to_reach(tmp_path):
    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, _hash("a"), 2000.0, now=1000.0)
    app = _FakeTanApp(
        publish_result={"delivered": 0, "peers": 0, "errors": [], "unsent": []}
    )
    result = close_due_polls(path, app, now=2500.0)
    assert result == {"closed": 1, "dropped": 0, "kept": 0}
    assert load_poll_closes(path) == {}


def test_close_due_polls_pauses_the_pass_when_a_send_is_cut_short(
    tmp_path,
):
    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, _hash("a"), 2000.0, now=1000.0)
    schedule_poll_close(path, _hash("b"), 2000.0, now=1000.0)
    app = _FakeTanApp(
        publish_result={
            "delivered": 0,
            "peers": 2,
            "errors": [],
            "unsent": ["aa", "bb"],
        }
    )
    result = close_due_polls(path, app, now=2500.0)
    assert result == {"closed": 0, "dropped": 0, "kept": 2}
    assert len(app.closed) == 1  # the pass stopped after the first
    assert load_poll_closes(path) == {
        _hash("a"): 2000.0,
        _hash("b"): 2000.0,
    }
    path = tmp_path / "poll-closes.json"
    path.write_text("{not json", encoding="utf-8")
    app = _FakeTanApp()
    result = close_due_polls(path, app, now=2500.0)
    assert result == {"closed": 0, "dropped": 0, "kept": 0}
    assert app.closed == []


def test_close_due_polls_closes_a_real_poll(tmp_path):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, init_db, is_poll_closed

    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(config.social_db_path)
    conn.close()
    app = TanApp(config, identity, name="tester", gossip=False)
    poll = app.create_poll("pizza or tacos?", ["Pizza", "Tacos"])
    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, poll["hash"], 2000.0, now=1000.0)

    result = close_due_polls(path, app, now=2500.0)
    assert result == {"closed": 1, "dropped": 0, "kept": 0}
    assert load_poll_closes(path) == {}

    conn = init_db(config.social_db_path)
    try:
        stored = conn.execute(
            "SELECT * FROM posts WHERE hash = ?", (poll["hash"],)
        ).fetchone()
        assert stored is not None
        assert is_poll_closed(conn, dict(stored)) is True
    finally:
        conn.close()


def test_close_due_polls_retries_a_failed_publish_without_a_second_close(
    tmp_path, monkeypatch
):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, init_db, is_poll_closed

    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(config.social_db_path)
    conn.close()
    app = TanApp(config, identity, name="tester", gossip=False)
    poll = app.create_poll("pizza or tacos?", ["Pizza", "Tacos"])
    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, poll["hash"], 2000.0, now=1000.0)

    # The sweep publishes from a node; stub the send so the first
    # pass reaches nobody and the second reaches one peer.
    app.node = object()
    sent = []

    def _publish_fail(record, **kwargs):
        sent.append(record)
        return {
            "delivered": 0,
            "peers": 2,
            "errors": [
                {"peer": "aa", "error": "timeout"},
                {"peer": "bb", "error": "timeout"},
            ],
            "unsent": [],
        }

    monkeypatch.setattr(app, "publish", _publish_fail)
    result = close_due_polls(path, app, now=2500.0)
    assert result == {"closed": 0, "dropped": 0, "kept": 1}
    assert load_poll_closes(path) == {poll["hash"]: 2000.0}

    def _publish_ok(record, **kwargs):
        sent.append(record)
        return {"delivered": 1, "peers": 2, "errors": [], "unsent": []}

    monkeypatch.setattr(app, "publish", _publish_ok)
    result = close_due_polls(path, app, now=2600.0)
    assert result == {"closed": 1, "dropped": 0, "kept": 0}
    assert load_poll_closes(path) == {}
    # The same signed close was re-sent, not re-created.
    assert len(sent) == 2
    assert sent[0]["hash"] == sent[1]["hash"]

    conn = init_db(config.social_db_path)
    try:
        count = conn.execute(
            "SELECT COUNT(*) FROM posts WHERE kind = 'close' AND target = ?",
            (poll["hash"],),
        ).fetchone()[0]
        assert count == 1
        stored = conn.execute(
            "SELECT * FROM posts WHERE hash = ?", (poll["hash"],)
        ).fetchone()
        assert is_poll_closed(conn, dict(stored)) is True
    finally:
        conn.close()


def test_close_due_polls_drops_a_manually_closed_poll_without_republishing(
    tmp_path, monkeypatch
):
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import TanApp, create_close_poll, init_db

    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    identity = RNS.Identity()
    conn = init_db(config.social_db_path)
    conn.close()
    app = TanApp(config, identity, name="tester", gossip=False)
    poll = app.create_poll("pizza or tacos?", ["Pizza", "Tacos"])
    # A manual close made before the deadline.
    conn = init_db(config.social_db_path)
    try:
        create_close_poll(conn, identity, poll["hash"], name="tester", now=1500.0)
    finally:
        conn.close()
    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, poll["hash"], 2000.0, now=1000.0)

    app.node = object()
    monkeypatch.setattr(
        app,
        "publish",
        lambda record, **kwargs: (_ for _ in ()).throw(
            AssertionError("a manual close must not be republished")
        ),
    )
    result = close_due_polls(path, app, now=2500.0)
    assert result == {"closed": 0, "dropped": 1, "kept": 0}
    assert load_poll_closes(path) == {}


def test_node_close_hook_skips_without_tan_app(tmp_path):
    import threading

    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, _hash("a"), 2000.0, now=1000.0)
    config = SimpleNamespace(poll_closes_path=path)
    node = SimpleNamespace(
        app=None, config=config, _stop_event=threading.Event()
    )
    TauNode._close_due_polls(node)
    assert load_poll_closes(path) == {_hash("a"): 2000.0}

    node.app = SimpleNamespace()  # attached, but not a Tan app
    TauNode._close_due_polls(node)
    assert load_poll_closes(path) == {_hash("a"): 2000.0}


def test_node_close_hook_closes_through_attached_app(tmp_path):
    import threading

    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, _hash("a"), 2000.0, now=1000.0)
    config = SimpleNamespace(poll_closes_path=path)
    app = _FakeTanApp()
    node = SimpleNamespace(
        app=app, config=config, _stop_event=threading.Event()
    )
    TauNode._close_due_polls(node)
    assert app.closed == [(_hash("a"), False)]
    assert load_poll_closes(path) == {}


def test_max_close_after_is_the_seven_day_horizon():
    assert MAX_POLL_CLOSE_AFTER == 7 * 24 * 60 * 60
