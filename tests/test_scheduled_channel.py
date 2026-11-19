from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from tau.disappearing import DisappearingDefaults, save_disappearing_defaults
from tau.scheduled_channel import (
    MAX_PUBLISH_ATTEMPTS,
    ScheduledChannelError,
    ScheduledChannelMessage,
    ScheduledChannelMessages,
    cancel_scheduled_channel_message,
    load_scheduled_channel_messages,
    publish_due_scheduled_channel_messages,
    save_scheduled_channel_messages,
    schedule_channel_message,
    schedule_channel_poll,
)


def _message(
    message_id: str = "a" * 16,
    *,
    channel: str = "general",
    text: str = "hello mesh",
    publish_at: float = 1000.0,
    **overrides,
) -> ScheduledChannelMessage:
    values = {
        "id": message_id,
        "channel": channel,
        "text": text,
        "publish_at": publish_at,
        "created_at": 900.0,
    }
    values.update(overrides)
    return ScheduledChannelMessage(**values)


def test_schedule_and_load_round_trip(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path,
        channel="general",
        text="good morning",
        publish_at=2000.0,
        to=["alice"],
        ttl=2,
        now=1000.0,
    )
    queue = load_scheduled_channel_messages(path)
    assert len(queue) == 1
    assert queue.messages[0] == entry
    assert entry.channel == "general"
    assert entry.text == "good morning"
    assert entry.to == ("alice",)
    assert entry.ttl == 2
    assert entry.publish_at == 2000.0
    assert entry.created_at == 1000.0


def test_retry_state_survives_a_save_round_trip(tmp_path):
    # The retry fields are load-bearing: a save that drops them would
    # reset every failed message to zero attempts and re-sign a new
    # message on the next pass.
    path = tmp_path / "scheduled-channel-messages.json"
    entry = _message(
        message_hash="c" * 64,
        attempts=2,
        next_attempt_at=5000.0,
        pending_peers=("d" * 64,),
    )
    save_scheduled_channel_messages(path, ScheduledChannelMessages(messages=(entry,)))
    assert load_scheduled_channel_messages(path).messages[0] == entry


def test_load_missing_file_is_empty(tmp_path):
    assert len(load_scheduled_channel_messages(tmp_path / "missing.json")) == 0


def test_due_returns_only_past_messages_oldest_first():
    queue = ScheduledChannelMessages(
        messages=(_message("a" * 16, publish_at=300.0), _message("b" * 16, publish_at=100.0))
    )
    due = queue.due(now=250.0)
    assert [entry.id for entry in due] == ["b" * 16]
    assert queue.due(now=50.0) == []


def test_resolve_by_prefix_requires_unique_match():
    queue = ScheduledChannelMessages(
        messages=(_message("ab" + "c" * 14), _message("ab" + "d" * 14))
    )
    assert queue.resolve("abc").id == "ab" + "c" * 14
    with pytest.raises(ScheduledChannelError):
        queue.resolve("ab")
    with pytest.raises(ScheduledChannelError):
        queue.resolve("ff")


def test_with_added_rejects_duplicate_and_removed_requires_present():
    entry = _message()
    queue = ScheduledChannelMessages(messages=(entry,))
    with pytest.raises(ScheduledChannelError):
        queue.with_added(entry)
    with pytest.raises(ScheduledChannelError):
        queue.with_removed("f" * 16)
    assert len(queue.with_removed(entry.id)) == 0


def test_schedule_rejects_empty_text_and_bad_channel(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    with pytest.raises(ScheduledChannelError):
        schedule_channel_message(
            path, channel="general", text="   ", publish_at=1000.0, now=900.0
        )
    with pytest.raises(ScheduledChannelError):
        schedule_channel_message(
            path, channel="not a channel!", text="hi", publish_at=1000.0, now=900.0
        )
    assert load_scheduled_channel_messages(path).messages == ()


def test_schedule_rejects_text_that_cannot_fit_the_wire(tmp_path):
    # 2,000 four-byte characters pass the character limit but overflow
    # the 8 KiB publish envelope once encoded: refuse at schedule time,
    # instead of failing every publish attempt forever.  Nothing is saved.
    path = tmp_path / "scheduled-channel-messages.json"
    with pytest.raises(ScheduledChannelError, match="too large"):
        schedule_channel_message(
            path, channel="general", text="😀" * 2000, publish_at=1000.0, now=900.0
        )
    assert load_scheduled_channel_messages(path).messages == ()


def test_schedule_accepts_long_text_that_fits(tmp_path):
    # 1,900 four-byte characters (7,600 bytes) fit the envelope with
    # the real-length placeholder author and key — the preflight
    # measures the same envelope the publish pass measures.
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path, channel="general", text="😀" * 1900, publish_at=1000.0, now=900.0
    )
    assert load_scheduled_channel_messages(path).messages[0].id == entry.id


def test_schedule_poll_round_trip(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_poll(
        path,
        channel="general",
        question="which day?",
        options=["Saturday", "Sunday"],
        publish_at=2000.0,
        to=["alice"],
        ttl=2,
        now=1000.0,
    )
    queue = load_scheduled_channel_messages(path)
    assert len(queue) == 1
    assert queue.messages[0] == entry
    assert entry.kind == "poll"
    assert entry.text == "which day?"
    assert entry.options == ("Saturday", "Sunday")
    assert entry.to == ("alice",)
    assert entry.ttl == 2
    assert entry.publish_at == 2000.0


def test_schedule_poll_rejects_bad_options(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    for options in (
        ["only-one"],
        ["a", "b", "c", "d", "e", "f", "g", "h", "i"],
        ["Saturday", "saturday"],
        ["Saturday", "  "],
    ):
        with pytest.raises(ScheduledChannelError):
            schedule_channel_poll(
                path,
                channel="general",
                question="which day?",
                options=options,
                publish_at=2000.0,
                now=1000.0,
            )
    assert load_scheduled_channel_messages(path).messages == ()


def test_schedule_message_rejects_poll_options(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    with pytest.raises(ScheduledChannelError, match="cannot carry poll options"):
        schedule_channel_message(
            path,
            channel="general",
            text="hi",
            publish_at=1000.0,
            now=900.0,
            kind="message",
            options=["a", "b"],
        )
    with pytest.raises(ScheduledChannelError, match="needs 2-8 options"):
        schedule_channel_poll(
            path,
            channel="general",
            question="which day?",
            options=["only-one"],
            publish_at=2000.0,
            now=1000.0,
        )
    assert load_scheduled_channel_messages(path).messages == ()


def test_schedule_poll_rejects_options_that_cannot_fit_the_wire(tmp_path):
    # A 1,900-emoji question fits on its own, but eight 100-char
    # options on top of it overflow the 8 KiB publish envelope once
    # encoded: refuse at schedule time, instead of failing every
    # publish attempt forever.  (Options are capped at 100 chars
    # each, so the overflow needs the question plus the options.)
    path = tmp_path / "scheduled-channel-messages.json"
    with pytest.raises(ScheduledChannelError, match="too large"):
        schedule_channel_poll(
            path,
            channel="general",
            question="😀" * 1900,
            options=["😀" * 99 + str(i) for i in range(8)],
            publish_at=2000.0,
            now=1000.0,
        )
    assert load_scheduled_channel_messages(path).messages == ()


def test_schedule_poll_preflight_is_expiry_aware(tmp_path):
    # The schedule-time wire-size preflight must mirror the expiry
    # the publish pass will actually apply.  A 1,915-emoji question
    # fits the 8 KiB envelope without an expiry but overflows it
    # once the ``expires_at`` field (~20 bytes) is added, so it is
    # accepted for a channel with no disappearing default and refused
    # for one with a default.  (With the old always-max-expiry
    # preflight, the no-default case was wrongly refused too.)
    question = "😀" * 1915
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_poll(
        path,
        channel="general",
        question=question,
        options=["aa", "bb"],
        publish_at=2000.0,
        now=1000.0,
    )
    assert load_scheduled_channel_messages(path).messages[0].id == entry.id

    defaults_path = tmp_path / "disappearing.json"
    save_disappearing_defaults(
        defaults_path, DisappearingDefaults(channels={"general": 3600.0})
    )
    path_with_default = tmp_path / "scheduled-with-default.json"
    with pytest.raises(ScheduledChannelError, match="too large"):
        schedule_channel_poll(
            path_with_default,
            channel="general",
            question=question,
            options=["aa", "bb"],
            publish_at=2000.0,
            now=1000.0,
            disappearing_path=defaults_path,
        )
    assert load_scheduled_channel_messages(path_with_default).messages == ()
    # And a poll five emoji shorter still fits even with the default:
    # the expiry-aware check still runs when an expiry will be carried.
    entry = schedule_channel_poll(
        path_with_default,
        channel="general",
        question="😀" * 1910,
        options=["aa", "bb"],
        publish_at=2000.0,
        now=1000.0,
        disappearing_path=defaults_path,
    )
    assert load_scheduled_channel_messages(path_with_default).messages[0].id == entry.id


def test_queue_without_kind_loads_as_message(tmp_path):
    # Queue files written before polls could be scheduled carry no
    # kind or options: they load as plain messages, exactly as before.
    path = tmp_path / "scheduled-channel-messages.json"
    import json

    path.write_text(
        json.dumps(
            {
                "messages": [
                    {
                        "id": "a" * 16,
                        "channel": "general",
                        "text": "old hello",
                        "publish_at": 1000.0,
                        "created_at": 900.0,
                        "to": [],
                        "ttl": 3,
                        "message_hash": None,
                        "attempts": 0,
                        "next_attempt_at": 0.0,
                        "paused_at": 0.0,
                        "pending_peers": [],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    entry = load_scheduled_channel_messages(path).messages[0]
    assert entry.kind == "message"
    assert entry.options == ()
    assert entry.text == "old hello"


def test_cancel_scheduled_channel_message_end_to_end(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path, channel="general", text="later", publish_at=2000.0, now=1000.0
    )
    assert len(load_scheduled_channel_messages(path)) == 1
    cancelled = cancel_scheduled_channel_message(path, entry.id[:8])
    assert cancelled.id == entry.id
    assert len(load_scheduled_channel_messages(path)) == 0
    with pytest.raises(ScheduledChannelError):
        cancel_scheduled_channel_message(path, entry.id)


def test_cancel_runs_on_cancel_under_the_lock(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path, channel="general", text="later", publish_at=2000.0, now=1000.0
    )
    seen = []
    cancel_scheduled_channel_message(path, entry.id, on_cancel=seen.append)
    assert seen == [entry]


class _FakeChannelApp:
    """ChannelApp stand-in: records creates and publishes, delivers to nobody."""

    def __init__(self, fail_publish: bool = False, fail_with_errors: bool = False):
        self.created = []
        self.published = []
        self.stored = {}
        self.fail_publish = fail_publish
        self.fail_with_errors = fail_with_errors

    def send(self, channel, text, *, publish=False, poll_options=None):
        message = {
            "hash": "c" * 64,
            "channel": channel,
            "text": text,
            "created_at": 2000.0,
        }
        if poll_options is not None:
            message["poll_options"] = list(poll_options)
        self.created.append(message)
        self.stored[message["hash"]] = message
        return message

    def get_message(self, digest):
        return self.stored.get(digest)

    def publish(self, message, *, targets=None, ttl=3):
        if self.fail_publish:
            raise RuntimeError("no route to peers")
        if self.fail_with_errors:
            return {
                "delivered": 0,
                "peers": 1,
                "errors": [{"peer": "deadbeef", "error": "timeout"}],
            }
        self.published.append((message, targets, ttl))
        return {"delivered": 0, "peers": 0, "errors": []}


def test_publish_due_publishes_and_removes_only_due(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    due = schedule_channel_message(
        path, channel="general", text="now", publish_at=1000.0, to=["alice"], now=900.0
    )
    schedule_channel_message(
        path, channel="general", text="later", publish_at=5000.0, now=900.0
    )
    app = _FakeChannelApp()
    resolved = [SimpleNamespace(hexhash="alice-hash")]

    result = publish_due_scheduled_channel_messages(
        path, app, resolve_peer=lambda target: resolved[0], now=2000.0
    )
    assert result == {"published": 1, "kept": 0}
    assert [message["text"] for message in app.created] == ["now"]
    assert app.published[0][1] == resolved
    queue = load_scheduled_channel_messages(path)
    assert all(entry.id != due.id for entry in queue.messages)
    assert len(queue) == 1


def test_publish_due_publishes_a_scheduled_poll(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    poll = schedule_channel_poll(
        path,
        channel="general",
        question="which day?",
        options=["Saturday", "Sunday"],
        publish_at=1000.0,
        now=900.0,
    )
    app = _FakeChannelApp()

    result = publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert result == {"published": 1, "kept": 0}
    assert len(app.created) == 1
    assert app.created[0]["text"] == "which day?"
    assert app.created[0]["poll_options"] == ["Saturday", "Sunday"]
    assert app.published[0][0]["poll_options"] == ["Saturday", "Sunday"]
    queue = load_scheduled_channel_messages(path)
    assert all(entry.id != poll.id for entry in queue.messages)
    assert len(queue) == 0


def test_publish_due_keeps_failed_messages_queued(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    schedule_channel_message(
        path, channel="general", text="now", publish_at=1000.0, now=900.0
    )
    app = _FakeChannelApp(fail_publish=True)

    result = publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert result == {"published": 0, "kept": 1}
    assert len(load_scheduled_channel_messages(path)) == 1


def test_publish_due_keeps_messages_with_per_peer_errors_queued(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    schedule_channel_message(
        path, channel="general", text="now", publish_at=1000.0, now=900.0
    )
    app = _FakeChannelApp(fail_with_errors=True)

    result = publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert result == {"published": 0, "kept": 1}
    remaining = load_scheduled_channel_messages(path)
    assert remaining.messages[0].attempts == 1
    assert remaining.messages[0].next_attempt_at > 2000.0


def test_publish_due_keeps_messages_with_unresolved_targets(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    schedule_channel_message(
        path, channel="general", text="now", publish_at=1000.0, to=["ghost"], now=900.0
    )
    app = _FakeChannelApp()

    def _missing(target):
        raise ValueError(f"unknown peer {target}")

    result = publish_due_scheduled_channel_messages(
        path, app, resolve_peer=_missing, now=2000.0
    )
    assert result == {"published": 0, "kept": 1}
    assert app.created == []
    assert len(load_scheduled_channel_messages(path)) == 1


def test_publish_retry_resends_the_same_signed_message(tmp_path):
    # A failed first attempt signs the message once; the retry
    # re-sends that same message instead of signing a second copy
    # peers would see as a duplicate.
    path = tmp_path / "scheduled-channel-messages.json"
    schedule_channel_message(
        path, channel="general", text="now", publish_at=1000.0, now=900.0
    )
    app = _FakeChannelApp(fail_publish=True)
    publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert len(app.created) == 1
    remaining = load_scheduled_channel_messages(path)
    assert remaining.messages[0].message_hash == "c" * 64
    assert remaining.messages[0].attempts == 1

    app.fail_publish = False
    result = publish_due_scheduled_channel_messages(
        path, app, now=2000.0 + 61.0
    )
    assert result == {"published": 1, "kept": 0}
    assert len(app.created) == 1
    assert app.published[0][0]["hash"] == "c" * 64


def test_publish_stops_retrying_after_the_attempt_limit(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    entry = _message(attempts=MAX_PUBLISH_ATTEMPTS - 1, next_attempt_at=0.0)
    save_scheduled_channel_messages(path, ScheduledChannelMessages(messages=(entry,)))
    app = _FakeChannelApp(fail_publish=True)

    result = publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert result == {"published": 0, "kept": 1}
    remaining = load_scheduled_channel_messages(path)
    assert remaining.messages[0].attempts == MAX_PUBLISH_ATTEMPTS

    # One more pass leaves it queued but never retries it.
    app2 = _FakeChannelApp()
    result = publish_due_scheduled_channel_messages(path, app2, now=3000.0)
    assert result == {"published": 0, "kept": 0}
    assert app2.created == []
    assert len(load_scheduled_channel_messages(path)) == 1


def test_publish_pass_stops_between_messages_when_asked(tmp_path):
    # A stop requested mid-pass ends it: the first message goes out, the
    # second stays queued with its attempts untouched.
    path = tmp_path / "scheduled-channel-messages.json"
    first = schedule_channel_message(
        path, channel="general", text="first", publish_at=1000.0, now=900.0
    )
    second = schedule_channel_message(
        path, channel="general", text="second", publish_at=1000.0, now=900.0
    )
    app = _FakeChannelApp()
    calls = {"n": 0}

    def _stop_after_first():
        calls["n"] += 1
        return calls["n"] > 1

    result = publish_due_scheduled_channel_messages(
        path, app, should_stop=_stop_after_first, now=2000.0
    )
    assert result == {"published": 1, "kept": 1}
    assert [message["text"] for message in app.created] == ["first"]
    remaining = load_scheduled_channel_messages(path)
    assert [entry.id for entry in remaining.messages] == [second.id]
    assert remaining.messages[0].attempts == 0


def test_publish_pass_stops_during_target_resolution(tmp_path):
    # A stop requested while resolving a message's peers ends the pass
    # before anything is created or published — and, crucially, does
    # not burn an attempt on the entry, the way a failed publish would.
    path = tmp_path / "scheduled-channel-messages.json"
    schedule_channel_message(
        path,
        channel="general",
        text="now",
        publish_at=1000.0,
        to=["alice", "bob"],
        now=900.0,
    )
    app = _FakeChannelApp()
    resolved_names = []
    stop = {"set": False}

    def _resolve(name):
        resolved_names.append(name)
        if len(resolved_names) == 1:
            stop["set"] = True
        return SimpleNamespace(hexhash=f"{name}-hash")

    result = publish_due_scheduled_channel_messages(
        path, app, resolve_peer=_resolve, should_stop=lambda: stop["set"], now=2000.0
    )
    assert result == {"published": 0, "kept": 1}
    assert resolved_names == ["alice"]
    assert app.created == []
    remaining = load_scheduled_channel_messages(path)
    assert len(remaining) == 1
    assert remaining.messages[0].attempts == 0


def test_publish_due_tolerates_corrupt_queue(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    path.write_text("{not json", encoding="utf-8")
    app = _FakeChannelApp()

    result = publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert result == {"published": 0, "kept": 0}
    assert app.created == []


def test_publish_signs_a_fresh_copy_when_the_stored_one_is_gone(tmp_path):
    # Retention evicted the signed message between passes: re-create
    # rather than leaving the entry stuck forever.
    path = tmp_path / "scheduled-channel-messages.json"
    entry = _message(message_hash="c" * 64)
    save_scheduled_channel_messages(path, ScheduledChannelMessages(messages=(entry,)))
    app = _FakeChannelApp()

    result = publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert result == {"published": 1, "kept": 0}
    assert len(app.created) == 1
    assert len(load_scheduled_channel_messages(path)) == 0
