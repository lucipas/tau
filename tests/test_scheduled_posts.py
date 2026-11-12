from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from tau import cli
from tau.node import TauNode
from tau.scheduled_posts import (
    ScheduledPost,
    ScheduledPosts,
    ScheduledPostsError,
    cancel_scheduled_post,
    load_scheduled_posts,
    publish_due_scheduled_posts,
    save_scheduled_posts,
    schedule_post,
)


def _post(
    post_id: str = "a" * 16,
    *,
    text: str = "hello mesh",
    publish_at: float = 1000.0,
    **overrides,
) -> ScheduledPost:
    values = {
        "id": post_id,
        "text": text,
        "publish_at": publish_at,
        "created_at": 900.0,
    }
    values.update(overrides)
    return ScheduledPost(**values)


def test_schedule_and_load_round_trip(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(
        path,
        text="good morning",
        publish_at=2000.0,
        reply_to="b" * 64,
        to=["alice"],
        ttl=2,
        now=1000.0,
    )
    queue = load_scheduled_posts(path)
    assert len(queue) == 1
    assert queue.posts[0] == post
    assert post.text == "good morning"
    assert post.reply_to == "b" * 64
    assert post.to == ("alice",)
    assert post.ttl == 2
    assert post.publish_at == 2000.0
    assert post.created_at == 1000.0


def test_retry_state_survives_a_save_round_trip(tmp_path):
    # The retry fields are load-bearing: a save that drops them would
    # reset every failed post to zero attempts and re-sign a new post
    # on the next pass.
    path = tmp_path / "scheduled-posts.json"
    post = _post(
        post_hash="c" * 64,
        attempts=2,
        next_attempt_at=5000.0,
        pending_peers=("d" * 64,),
    )
    save_scheduled_posts(path, ScheduledPosts(posts=(post,)))
    assert load_scheduled_posts(path).posts[0] == post


def test_load_missing_file_is_empty(tmp_path):
    assert len(load_scheduled_posts(tmp_path / "missing.json")) == 0


def test_store_rejects_malformed_entries(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    path.write_text(
        json.dumps({"posts": [{"id": "xyz", "text": "hi", "publish_at": 1.0}]}),
        encoding="utf-8",
    )
    with pytest.raises(ScheduledPostsError):
        load_scheduled_posts(path)


def test_store_rejects_empty_text_and_bad_ttl():
    with pytest.raises(ScheduledPostsError):
        _post(text="   ")
    with pytest.raises(ScheduledPostsError):
        _post(ttl=-1)
    with pytest.raises(ScheduledPostsError):
        _post(ttl=256)
    with pytest.raises(ScheduledPostsError):
        _post(reply_to="not-a-hash")


def test_due_returns_only_past_posts_oldest_first():
    queue = ScheduledPosts(
        posts=(_post("a" * 16, publish_at=300.0), _post("b" * 16, publish_at=100.0))
    )
    due = queue.due(now=250.0)
    assert [post.id for post in due] == ["b" * 16]
    assert queue.due(now=50.0) == []


def test_resolve_by_prefix_requires_unique_match():
    queue = ScheduledPosts(
        posts=(_post("ab" + "c" * 14), _post("ab" + "d" * 14))
    )
    assert queue.resolve("abc").id == "ab" + "c" * 14
    with pytest.raises(ScheduledPostsError):
        queue.resolve("ab")
    with pytest.raises(ScheduledPostsError):
        queue.resolve("ff")


def test_with_added_rejects_duplicate_and_removed_requires_present():
    post = _post()
    queue = ScheduledPosts(posts=(post,))
    with pytest.raises(ScheduledPostsError):
        queue.with_added(post)
    with pytest.raises(ScheduledPostsError):
        queue.with_removed("f" * 16)
    assert len(queue.with_removed(post.id)) == 0


def test_cancel_scheduled_post_end_to_end(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(path, text="later", publish_at=2000.0, now=1000.0)
    assert len(load_scheduled_posts(path)) == 1
    cancelled = cancel_scheduled_post(path, post.id[:8])
    assert cancelled.id == post.id
    assert len(load_scheduled_posts(path)) == 0
    with pytest.raises(ScheduledPostsError):
        cancel_scheduled_post(path, post.id)


class _FakeTanApp:
    """TanApp stand-in: records creates and publishes, delivers to nobody."""

    def __init__(self, fail_publish: bool = False):
        self.created = []
        self.published = []
        self.fail_publish = fail_publish

    def create_post(self, text, *, reply_to=None):
        post = {"hash": "c" * 64, "text": text, "reply_to": reply_to}
        self.created.append(post)
        return post

    def get_post(self, digest):
        for post in self.created:
            if post["hash"] == digest:
                return post
        return None

    def publish(self, post, *, targets=None, ttl=3, should_stop=None, deadline=None):
        if self.fail_publish:
            raise RuntimeError("no route to peers")
        self.published.append((post, targets, ttl))
        return {"delivered": 0, "peers": 0, "errors": []}


def test_publish_due_publishes_and_removes_only_due(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    due = schedule_post(path, text="now", publish_at=1000.0, to=["alice"], now=900.0)
    schedule_post(path, text="later", publish_at=5000.0, now=900.0)
    app = _FakeTanApp()
    resolved = [SimpleNamespace(hexhash="alice-hash")]

    result = publish_due_scheduled_posts(
        path, app, resolve_peer=lambda target: resolved[0], now=2000.0
    )
    assert result == {"published": 1, "kept": 0}
    assert [post["text"] for post in app.created] == ["now"]
    assert app.published[0][1] == resolved
    queue = load_scheduled_posts(path)
    assert all(post.id != due.id for post in queue.posts)
    assert len(queue) == 1


def test_publish_due_keeps_failed_posts_queued(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    schedule_post(path, text="now", publish_at=1000.0, now=900.0)
    app = _FakeTanApp(fail_publish=True)

    result = publish_due_scheduled_posts(path, app, now=2000.0)
    assert result == {"published": 0, "kept": 1}
    assert len(load_scheduled_posts(path)) == 1


def test_publish_due_keeps_posts_with_unresolved_targets(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    schedule_post(path, text="now", publish_at=1000.0, to=["ghost"], now=900.0)
    app = _FakeTanApp()

    def _missing(target):
        raise ValueError(f"unknown peer {target}")

    result = publish_due_scheduled_posts(path, app, resolve_peer=_missing, now=2000.0)
    assert result == {"published": 0, "kept": 1}
    assert app.created == []
    assert len(load_scheduled_posts(path)) == 1


def test_publish_pass_stops_between_posts_when_asked(tmp_path):
    # A stop requested mid-pass ends it: the first post goes out, the
    # second stays queued with its attempts untouched.
    path = tmp_path / "scheduled-posts.json"
    first = schedule_post(path, text="first", publish_at=1000.0, now=900.0)
    second = schedule_post(path, text="second", publish_at=1000.0, now=900.0)
    app = _FakeTanApp()
    calls = {"n": 0}

    def _stop_after_first():
        calls["n"] += 1
        return calls["n"] > 1

    result = publish_due_scheduled_posts(
        path, app, should_stop=_stop_after_first, now=2000.0
    )
    assert result == {"published": 1, "kept": 1}
    assert [post["text"] for post in app.created] == ["first"]
    remaining = load_scheduled_posts(path)
    assert [post.id for post in remaining.posts] == [second.id]
    assert remaining.posts[0].attempts == 0


def test_publish_pass_stops_during_target_resolution(tmp_path):
    # A stop requested while resolving a post's peers ends the pass
    # before anything is created or published — and, crucially, does
    # not burn an attempt on the entry, the way a failed publish would.
    path = tmp_path / "scheduled-posts.json"
    schedule_post(path, text="now", publish_at=1000.0, to=["alice", "bob"], now=900.0)
    app = _FakeTanApp()
    resolved_names = []
    stop = {"set": False}

    def _resolve(name):
        resolved_names.append(name)
        if len(resolved_names) == 1:
            stop["set"] = True
        return SimpleNamespace(hexhash=f"{name}-hash")

    result = publish_due_scheduled_posts(
        path, app, resolve_peer=_resolve, should_stop=lambda: stop["set"], now=2000.0
    )
    assert result == {"published": 0, "kept": 1}
    assert resolved_names == ["alice"]
    assert app.created == []
    remaining = load_scheduled_posts(path)
    assert len(remaining) == 1
    assert remaining.posts[0].attempts == 0


def test_publish_due_tolerates_corrupt_queue(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    path.write_text("{not json", encoding="utf-8")
    app = _FakeTanApp()
    result = publish_due_scheduled_posts(path, app, now=2000.0)
    assert result == {"published": 0, "kept": 0}
    assert app.created == []


def test_failed_publish_backs_off_before_retrying(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    schedule_post(path, text="now", publish_at=1000.0, now=900.0)
    app = _FakeTanApp(fail_publish=True)

    result = publish_due_scheduled_posts(path, app, now=2000.0)
    assert result == {"published": 0, "kept": 1}
    queued = load_scheduled_posts(path).posts[0]
    assert queued.attempts == 1
    assert queued.next_attempt_at == 2000.0 + 60.0
    assert queued.post_hash == "c" * 64

    # Inside the backoff window the pass does not retry.
    result = publish_due_scheduled_posts(path, app, now=2030.0)
    assert result == {"published": 0, "kept": 0}
    assert len(app.created) == 1

    # Past the backoff it retries the SAME signed post, not a new one.
    app.fail_publish = False
    result = publish_due_scheduled_posts(path, app, now=2100.0)
    assert result == {"published": 1, "kept": 0}
    assert len(app.created) == 1
    assert len(load_scheduled_posts(path)) == 0


def test_partial_failure_retries_only_failed_peers(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    schedule_post(path, text="now", publish_at=1000.0, to=["alice", "bob"], now=900.0)
    peers = {
        "alice": SimpleNamespace(hexhash="a" * 64),
        "bob": SimpleNamespace(hexhash="b" * 64),
    }
    by_name_or_hash = {**peers}
    by_name_or_hash.update({p.hexhash: p for p in peers.values()})

    class _PartialApp(_FakeTanApp):
        fail_bob = True

        def publish(self, post, *, targets=None, ttl=3, should_stop=None, deadline=None):
            self.published.append((post, targets, ttl))
            failed = (
                [t for t in (targets or []) if t.hexhash == "b" * 64]
                if self.fail_bob
                else []
            )
            return {
                "delivered": len(targets or []) - len(failed),
                "peers": len(targets or []),
                "errors": [
                    {"peer": t.hexhash, "error": "no route"} for t in failed
                ],
            }

    app = _PartialApp()
    resolve = lambda name: by_name_or_hash[name]
    result = publish_due_scheduled_posts(path, app, resolve_peer=resolve, now=2000.0)
    assert result == {"published": 0, "kept": 1}
    queued = load_scheduled_posts(path).posts[0]
    assert queued.pending_peers == ("b" * 64,)
    assert queued.attempts == 1

    # The retry goes to the failed peer only.
    app.fail_bob = False
    result = publish_due_scheduled_posts(path, app, resolve_peer=resolve, now=2100.0)
    assert result == {"published": 1, "kept": 0}
    assert [t.hexhash for t in app.published[-1][1]] == ["b" * 64]


def test_schedule_rejects_oversized_text(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    with pytest.raises(ScheduledPostsError):
        schedule_post(path, text="x" * 2001, publish_at=2000.0, now=1000.0)
    assert len(load_scheduled_posts(path)) == 0


def test_node_publish_hook_skips_without_tan_app(tmp_path):
    # The mail-loop hook delegates to publish_due_scheduled_posts only
    # when a Tan-like app is attached; without one it is a no-op.
    path = tmp_path / "scheduled-posts.json"
    schedule_post(path, text="now", publish_at=1000.0, now=900.0)
    config = SimpleNamespace(scheduled_posts_path=path)
    node = SimpleNamespace(
        app=None,
        config=config,
        resolve_peer=lambda t: None,
        _stop_event=threading.Event(),
    )
    TauNode._publish_due_scheduled_posts(node)
    assert len(load_scheduled_posts(path)) == 1

    node.app = SimpleNamespace()  # attached, but not a Tan app
    TauNode._publish_due_scheduled_posts(node)
    assert len(load_scheduled_posts(path)) == 1


def test_node_publish_hook_publishes_through_attached_app(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    schedule_post(path, text="now", publish_at=1000.0, now=900.0)
    config = SimpleNamespace(scheduled_posts_path=path)
    app = _FakeTanApp()
    node = SimpleNamespace(
        app=app,
        config=config,
        resolve_peer=lambda t: None,
        _stop_event=threading.Event(),
    )
    TauNode._publish_due_scheduled_posts(node)
    assert [post["text"] for post in app.created] == ["now"]
    assert len(load_scheduled_posts(path)) == 0


def test_node_publish_hook_stops_pass_when_stopping(tmp_path):
    # The hook wires the node's stop event into the pass, so a node
    # shutting down does not wait out a publish pass in the mail loop.
    path = tmp_path / "scheduled-posts.json"
    schedule_post(path, text="now", publish_at=1000.0, now=900.0)
    config = SimpleNamespace(scheduled_posts_path=path)
    app = _FakeTanApp()
    stop_event = threading.Event()
    stop_event.set()
    node = SimpleNamespace(
        app=app,
        config=config,
        resolve_peer=lambda t: None,
        _stop_event=stop_event,
    )
    TauNode._publish_due_scheduled_posts(node)
    assert app.created == []
    assert len(load_scheduled_posts(path)) == 1


def test_publish_pass_keeps_post_queued_when_deadline_expires_during_resolution(
    monkeypatch, tmp_path
):
    # The resolve step checks the pass budget too: when it expires
    # mid-resolution, the post stays queued with no attempt burned
    # and no peer resolved after the expiry.
    path = tmp_path / "scheduled-posts.json"
    schedule_post(
        path, text="now", publish_at=1000.0, to=["alice", "bob"], now=900.0
    )
    app = _FakeTanApp()
    resolved = []

    def resolve_peer(name):
        resolved.append(name)
        return SimpleNamespace(hexhash="a" * 64 if name == "alice" else "b" * 64)

    # time.monotonic() reads in order: pass start (deadline = 1020.0),
    # the per-post budget check, the check before resolving alice,
    # the check before resolving bob — which finds the budget gone.
    ticks = iter([1000.0, 1000.0, 1000.0, 1021.0])
    monkeypatch.setattr(
        "tau.scheduled_posts.time.monotonic", lambda: next(ticks)
    )
    result = publish_due_scheduled_posts(
        path, app, resolve_peer=resolve_peer, now=2000.0
    )
    assert result == {"published": 0, "kept": 1}
    assert resolved == ["alice"]
    assert app.created == []
    queued = load_scheduled_posts(path).posts[0]
    assert queued.attempts == 0
    assert queued.pending_peers == ()
    assert queued.post_hash is None


def test_mail_loop_still_publishes_scheduled_posts_when_flush_fails(tmp_path):
    # A mail-round failure must not skip scheduled publishing: the
    # publish step runs outside the mail round's exception handler.
    published = []
    stop_event = threading.Event()

    def fake_publish():
        published.append(True)
        stop_event.set()

    def failing_flush(*args, **kwargs):
        raise RuntimeError("no route")

    node = SimpleNamespace(
        _stop_event=stop_event,
        _mail_pending=set(),
        registry=SimpleNamespace(get=lambda h: None),
        _mail_round_for_peer=lambda peer: None,
        flush_outbox=failing_flush,
        _publish_due_scheduled_posts=fake_publish,
        _exchange_sweep=lambda **kwargs: None,
    )
    TauNode._mail_loop(node)
    assert published == [True]


def test_publish_pass_resumes_unsent_peers_before_failed_ones(tmp_path):
    # A paused pass keeps unsent peers ahead of peers that already
    # failed this pass: the next pass reaches the never-attempted
    # peers first instead of burning its budget re-trying the ones
    # that just timed out.
    from tau.config import TauConfig
    from tau.identity import load_identity
    from tau.tangents.tan import TanApp

    data_dir = tmp_path / "node"
    assert _run_cli(data_dir, ["init", "--json"]) == 0
    queue_path = data_dir / "scheduled-posts.json"
    schedule_post(
        queue_path, text="good morning", publish_at=1000.0,
        to=["alice", "bob", "carol"], now=900.0,
    )
    config = TauConfig.resolve(data_dir=data_dir)
    app = TanApp(config, load_identity(config.identity_path), name=config.name)

    stop = {"set": False}

    class _FailingFirstNode:
        def request(self, peer, path, payload, *, timeout=15.0):
            if peer.hexhash == "a" * 64:
                stop["set"] = True
                raise RuntimeError("no route")
            return None

    app.node = _FailingFirstNode()
    by_name_or_hash = {
        "alice": SimpleNamespace(hexhash="a" * 64),
        "bob": SimpleNamespace(hexhash="b" * 64),
        "carol": SimpleNamespace(hexhash="c" * 64),
        "a" * 64: SimpleNamespace(hexhash="a" * 64),
        "b" * 64: SimpleNamespace(hexhash="b" * 64),
        "c" * 64: SimpleNamespace(hexhash="c" * 64),
    }
    result = publish_due_scheduled_posts(
        queue_path,
        app,
        resolve_peer=lambda name: by_name_or_hash[name],
        should_stop=lambda: stop["set"],
        now=2000.0,
    )
    assert result == {"published": 0, "kept": 1}
    queued = load_scheduled_posts(queue_path).posts[0]
    assert queued.pending_peers == ("b" * 64, "c" * 64, "a" * 64)
    assert queued.attempts == 0


def test_publish_pass_keeps_unsent_peers_when_stopping(tmp_path):
    # A stop mid-send leaves the peers never attempted queued for the
    # next pass — no attempt burned, and the retry sends only to them.
    from tau.config import TauConfig
    from tau.identity import load_identity
    from tau.tangents.tan import TanApp

    data_dir = tmp_path / "node"
    assert _run_cli(data_dir, ["init", "--json"]) == 0
    queue_path = data_dir / "scheduled-posts.json"
    schedule_post(
        queue_path, text="good morning", publish_at=1000.0,
        to=["alice", "bob"], now=900.0,
    )
    config = TauConfig.resolve(data_dir=data_dir)
    app = TanApp(config, load_identity(config.identity_path), name=config.name)

    sent = []
    stop = {"set": False}

    class _RecordingNode:
        def request(self, peer, path, payload, *, timeout=15.0):
            sent.append(peer.hexhash)
            if len(sent) == 1:
                stop["set"] = True
            return None

    app.node = _RecordingNode()
    by_name_or_hash = {
        "alice": SimpleNamespace(hexhash="a" * 64),
        "bob": SimpleNamespace(hexhash="b" * 64),
        "a" * 64: SimpleNamespace(hexhash="a" * 64),
        "b" * 64: SimpleNamespace(hexhash="b" * 64),
    }
    result = publish_due_scheduled_posts(
        queue_path,
        app,
        resolve_peer=lambda name: by_name_or_hash[name],
        should_stop=lambda: stop["set"],
        now=2000.0,
    )
    assert result == {"published": 0, "kept": 1}
    assert sent == ["a" * 64]
    queued = load_scheduled_posts(queue_path).posts[0]
    assert queued.pending_peers == ("b" * 64,)
    assert queued.attempts == 0

    # The next pass sends only to the peer that never got it.
    stop["set"] = False
    result = publish_due_scheduled_posts(
        queue_path,
        app,
        resolve_peer=lambda name: by_name_or_hash[name],
        should_stop=lambda: stop["set"],
        now=2100.0,
    )
    assert result == {"published": 1, "kept": 0}
    assert sent == ["a" * 64, "b" * 64]


def test_tan_publish_reports_unsent_peers_past_deadline(tmp_path):
    # TanApp.publish checks the deadline between sends: with a
    # deadline already past, nothing is sent and every peer is unsent.
    from tau.config import TauConfig
    from tau.identity import load_identity
    from tau.tangents.tan import TanApp

    data_dir = tmp_path / "node"
    assert _run_cli(data_dir, ["init", "--json"]) == 0
    config = TauConfig.resolve(data_dir=data_dir)
    app = TanApp(config, load_identity(config.identity_path), name=config.name)

    sent = []

    class _RecordingNode:
        def request(self, peer, path, payload, *, timeout=15.0):
            sent.append(peer.hexhash)
            return None

    app.node = _RecordingNode()
    post = app.create_post("hello mesh")
    peers = [SimpleNamespace(hexhash="a" * 64), SimpleNamespace(hexhash="b" * 64)]
    result = app.publish(post, targets=peers, deadline=time.monotonic() - 1.0)
    assert result["delivered"] == 0
    assert result["unsent"] == ["a" * 64, "b" * 64]
    assert sent == []
    result = app.publish(post, targets=peers, deadline=time.monotonic() + 60.0)
    assert result["delivered"] == 2
    assert result["unsent"] == []
    assert sent == ["a" * 64, "b" * 64]


def _run_cli(data_dir: Path, argv):
    return cli.main(["--data-dir", str(data_dir), *argv])


def test_cli_schedule_list_cancel(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert _run_cli(data_dir, ["tan", "post", "--delay", "5m", "good morning"]) == 0
    out = capsys.readouterr().out
    assert "scheduled" in out and "publishes in 5m" in out

    assert _run_cli(data_dir, ["tan", "scheduled"]) == 0
    listed = capsys.readouterr().out
    assert "good morning" in listed

    queue = load_scheduled_posts(data_dir / "scheduled-posts.json")
    assert len(queue) == 1
    post_id = queue.posts[0].id
    assert _run_cli(data_dir, ["tan", "cancel-scheduled", post_id[:8]]) == 0
    assert len(load_scheduled_posts(data_dir / "scheduled-posts.json")) == 0


def test_cli_schedule_rejects_bad_delay_and_no_publish(tmp_path):
    data_dir = tmp_path / "node"
    assert _run_cli(data_dir, ["tan", "post", "--delay", "soon", "hi"]) == 2
    assert (
        _run_cli(data_dir, ["tan", "post", "--delay", "5m", "--no-publish", "hi"])
        == 2
    )
    assert _run_cli(data_dir, ["tan", "post", "--delay", "30d", "hi"]) == 2
    assert len(load_scheduled_posts(data_dir / "scheduled-posts.json")) == 0


def test_cli_scheduled_empty_and_cancel_unknown(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert _run_cli(data_dir, ["tan", "scheduled"]) == 0
    assert "No Tan posts scheduled." in capsys.readouterr().out
    assert _run_cli(data_dir, ["tan", "cancel-scheduled", "abcd"]) == 1


def test_cli_schedule_json_output(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert (
        _run_cli(
            data_dir, ["tan", "post", "--delay", "5m", "--json", "hello json"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["scheduled"]["text"] == "hello json"


def test_cancel_removes_stored_post_after_failed_publish(tmp_path, capsys):
    # A publish attempt already stored the signed post locally; cancelling
    # must remove that stored post too, not just the queue entry.
    from tau.config import TauConfig
    from tau.identity import load_identity
    from tau.tangents.tan import TanApp, deleted_targets, init_db

    data_dir = tmp_path / "node"
    assert _run_cli(data_dir, ["init", "--json"]) == 0
    assert _run_cli(data_dir, ["tan", "post", "--delay", "5m", "good morning"]) == 0
    queue_path = data_dir / "scheduled-posts.json"
    post = load_scheduled_posts(queue_path).posts[0]
    config = TauConfig.resolve(data_dir=data_dir)

    # A real TanApp whose only peer never answers: create_post stores
    # the post locally, publish fails, the entry stays queued.
    peer = SimpleNamespace(hexhash="d" * 64)

    class _FailingNode:
        registry = SimpleNamespace(all=lambda: [peer])

        def request(self, *args, **kwargs):
            raise RuntimeError("no route")

    app = TanApp(config, load_identity(config.identity_path), name=config.name)
    app.node = _FailingNode()
    result = publish_due_scheduled_posts(
        queue_path, app, now=post.publish_at + 1.0
    )
    assert result == {"published": 0, "kept": 1}
    stored_hash = load_scheduled_posts(queue_path).posts[0].post_hash
    assert stored_hash is not None

    assert _run_cli(data_dir, ["tan", "cancel-scheduled", post.id]) == 0
    assert "removed the stored post" in capsys.readouterr().out
    assert len(load_scheduled_posts(queue_path)) == 0
    conn = init_db(config.social_db_path)
    try:
        assert stored_hash in deleted_targets(conn)
    finally:
        conn.close()


def test_cancelled_post_syncs_only_as_deleted(tmp_path, capsys):
    # Cancellation after a publish attempt writes a Tan delete, not a
    # removal: the post stays stored so the chain stays verifiable,
    # but the sync feed carries the delete entry with it, so a peer
    # that syncs learns the deletion — and the display feed hides it.
    from tau.config import TauConfig
    from tau.identity import load_identity
    from tau.tangents.tan import TanApp, deleted_targets, feed, init_db

    data_dir = tmp_path / "node"
    assert _run_cli(data_dir, ["init", "--json"]) == 0
    assert _run_cli(data_dir, ["tan", "post", "--delay", "5m", "good morning"]) == 0
    queue_path = data_dir / "scheduled-posts.json"
    post = load_scheduled_posts(queue_path).posts[0]
    config = TauConfig.resolve(data_dir=data_dir)

    peer = SimpleNamespace(hexhash="d" * 64)

    class _FailingNode:
        registry = SimpleNamespace(all=lambda: [peer])

        def request(self, *args, **kwargs):
            raise RuntimeError("no route")

    app = TanApp(config, load_identity(config.identity_path), name=config.name)
    app.node = _FailingNode()
    result = publish_due_scheduled_posts(
        queue_path, app, now=post.publish_at + 1.0
    )
    assert result == {"published": 0, "kept": 1}
    stored_hash = load_scheduled_posts(queue_path).posts[0].post_hash
    assert stored_hash is not None

    assert _run_cli(data_dir, ["tan", "cancel-scheduled", post.id]) == 0
    conn = init_db(config.social_db_path)
    try:
        assert stored_hash in deleted_targets(conn)
        # What /tan/feed serves to a syncing peer: the post is still
        # there (chain verifiable), but so is its delete entry.
        synced = feed(conn)
        assert any(p["hash"] == stored_hash for p in synced)
        assert any(
            p.get("kind") == "delete" and p.get("target") == stored_hash
            for p in synced
        )
        # And the display feed — what any peer shows — hides it.
        shown = feed(conn, posts_only=True)
        assert all(p["hash"] != stored_hash for p in shown)
    finally:
        conn.close()


def test_cancel_tolerates_already_deleted_post(tmp_path):
    # The stored post was already deleted by hand: cancelling the
    # queue entry still succeeds instead of failing on the delete.
    from tau.config import TauConfig
    from tau.identity import load_identity
    from tau.tangents.tan import TanApp

    data_dir = tmp_path / "node"
    assert _run_cli(data_dir, ["init", "--json"]) == 0
    config = TauConfig.resolve(data_dir=data_dir)
    app = TanApp(config, load_identity(config.identity_path), name=config.name)
    created = app.create_post("doomed post")
    app.delete(created["hash"])

    path = data_dir / "scheduled-posts.json"
    entry = _post(post_hash=created["hash"], publish_at=2000.0)
    save_scheduled_posts(path, ScheduledPosts(posts=(entry,)))
    assert _run_cli(data_dir, ["tan", "cancel-scheduled", entry.id]) == 0
    assert len(load_scheduled_posts(path)) == 0
