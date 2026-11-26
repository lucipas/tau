from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from tau.node import TauNode
from tau.poll_closes import (
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
    def __init__(self, *, fail_with=None):
        self.closed = []
        self.fail_with = fail_with

    def close_poll(self, target, *, publish=False, targets=None, ttl=3):
        if self.fail_with is not None:
            raise self.fail_with
        self.closed.append((target, publish))
        return {"hash": "f" * 64, "target": target}


def test_close_due_polls_closes_and_drops(tmp_path):
    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, _hash("a"), 2000.0, now=1000.0)
    schedule_poll_close(path, _hash("b"), 9000.0, now=1000.0)
    app = _FakeTanApp()

    result = close_due_polls(path, app, now=2500.0)
    assert result == {"closed": 1, "dropped": 0, "kept": 0}
    assert app.closed == [(_hash("a"), True)]
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


def test_close_due_polls_treats_a_corrupt_file_as_nothing_due(tmp_path):
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


def test_node_close_hook_skips_without_tan_app(tmp_path):
    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, _hash("a"), 2000.0, now=1000.0)
    config = SimpleNamespace(poll_closes_path=path)
    node = SimpleNamespace(app=None, config=config)
    TauNode._close_due_polls(node)
    assert load_poll_closes(path) == {_hash("a"): 2000.0}

    node.app = SimpleNamespace()  # attached, but not a Tan app
    TauNode._close_due_polls(node)
    assert load_poll_closes(path) == {_hash("a"): 2000.0}


def test_node_close_hook_closes_through_attached_app(tmp_path):
    path = tmp_path / "poll-closes.json"
    schedule_poll_close(path, _hash("a"), 2000.0, now=1000.0)
    config = SimpleNamespace(poll_closes_path=path)
    app = _FakeTanApp()
    node = SimpleNamespace(app=app, config=config)
    TauNode._close_due_polls(node)
    assert app.closed == [(_hash("a"), True)]
    assert load_poll_closes(path) == {}


def test_max_close_after_is_the_seven_day_horizon():
    assert MAX_POLL_CLOSE_AFTER == 7 * 24 * 60 * 60
