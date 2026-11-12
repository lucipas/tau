from __future__ import annotations

import json
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

    def publish(self, post, *, targets=None, ttl=3):
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

        def publish(self, post, *, targets=None, ttl=3):
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
    node = SimpleNamespace(app=None, config=config, resolve_peer=lambda t: None)
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
    node = SimpleNamespace(app=app, config=config, resolve_peer=lambda t: None)
    TauNode._publish_due_scheduled_posts(node)
    assert [post["text"] for post in app.created] == ["now"]
    assert len(load_scheduled_posts(path)) == 0


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
