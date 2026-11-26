from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from tau.blocklist import BlockList
from tau.disappearing import DisappearingDefaults, save_disappearing_defaults
from tau.scheduled_channel import (
    MAX_PUBLISH_ATTEMPTS,
    ScheduledChannelError,
    ScheduledChannelMessage,
    ScheduledChannelMessages,
    cancel_scheduled_channel_message,
    channel_contact_refusal,
    edit_scheduled_channel_message,
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
    # And a poll ten emoji shorter still fits even with the default:
    # the expiry-aware check still runs when an expiry will be carried.
    entry = schedule_channel_poll(
        path_with_default,
        channel="general",
        question="😀" * 1905,
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


PIN = {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"}

CARD = {"name": "carol", "destination_hash": "cc" * 16}


def test_schedule_location_round_trip(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path,
        channel="general",
        text="meet here",
        publish_at=2000.0,
        now=1000.0,
        location=PIN,
    )
    queue = load_scheduled_channel_messages(path)
    assert len(queue) == 1
    assert queue.messages[0] == entry
    assert entry.location == PIN
    assert entry.to_dict()["location"] == PIN


def test_queue_without_location_loads_with_no_pin(tmp_path):
    # Queue files written before pins could be scheduled carry no
    # location: they load with no pin, exactly as before — and a
    # hand-built entry without one publishes a plain message.
    entry = _message()
    assert entry.location is None
    loaded = ScheduledChannelMessage.from_dict(
        {
            "id": "a" * 16,
            "channel": "general",
            "text": "old hello",
            "publish_at": 1000.0,
            "created_at": 900.0,
        }
    )
    assert loaded.location is None


def test_schedule_rejects_bad_location(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    for bad in (
        {"lat": 91.0, "lon": 0.0},
        {"lat": 34.05, "lon": -200.0},
        {"lat": "34.05", "lon": -83.88},
        {"lat": 34.05, "lon": -83.88, "name": "x" * 201},
        "34.05,-83.88",
    ):
        with pytest.raises(ScheduledChannelError, match="location is invalid"):
            schedule_channel_message(
                path,
                channel="general",
                text="meet here",
                publish_at=1000.0,
                now=900.0,
                location=bad,
            )
    assert load_scheduled_channel_messages(path).messages == ()


def test_schedule_poll_rejects_location(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    with pytest.raises(ScheduledChannelError, match="cannot carry a location"):
        schedule_channel_message(
            path,
            channel="general",
            text="which day?",
            publish_at=1000.0,
            now=900.0,
            kind="poll",
            options=["Saturday", "Sunday"],
            location=PIN,
        )
    assert load_scheduled_channel_messages(path).messages == ()


def test_schedule_location_wire_preflight(tmp_path):
    # 1,920 emoji fit the 8 KiB envelope on their own but overflow
    # once the pin (~90 bytes) rides along: refuse at schedule time,
    # instead of failing every publish attempt forever.  Nothing is saved.
    path = tmp_path / "scheduled-channel-messages.json"
    with pytest.raises(ScheduledChannelError, match="too large"):
        schedule_channel_message(
            path,
            channel="general",
            text="😀" * 1920,
            publish_at=1000.0,
            now=900.0,
            location=PIN,
        )
    assert load_scheduled_channel_messages(path).messages == ()
    # And 1,900 emoji with the same pin still fit.
    entry = schedule_channel_message(
        path,
        channel="general",
        text="😀" * 1900,
        publish_at=1000.0,
        now=900.0,
        location=PIN,
    )
    assert load_scheduled_channel_messages(path).messages[0].id == entry.id


def test_schedule_contact_round_trip(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path,
        channel="general",
        text="meet carol",
        publish_at=2000.0,
        now=1000.0,
        contact=CARD,
    )
    queue = load_scheduled_channel_messages(path)
    assert len(queue) == 1
    assert queue.messages[0] == entry
    assert entry.contact == CARD
    assert entry.to_dict()["contact"] == CARD


def test_queue_without_contact_loads_with_no_card(tmp_path):
    # Queue files written before cards could be scheduled carry no
    # contact: they load with no card, exactly as before — and a
    # hand-built entry without one publishes a plain message.
    entry = _message()
    assert entry.contact is None
    loaded = ScheduledChannelMessage.from_dict(
        {
            "id": "a" * 16,
            "channel": "general",
            "text": "old hello",
            "publish_at": 1000.0,
            "created_at": 900.0,
        }
    )
    assert loaded.contact is None


def test_schedule_rejects_bad_contact(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    for bad in (
        {"name": "carol"},
        {"destination_hash": "cc" * 16},
        {"name": "", "destination_hash": "cc" * 16},
        {"name": "carol", "destination_hash": "xyz"},
        {"name": "carol", "destination_hash": "cc" * 15},
        "carol",
    ):
        with pytest.raises(ScheduledChannelError, match="contact is invalid"):
            schedule_channel_message(
                path,
                channel="general",
                text="meet carol",
                publish_at=1000.0,
                now=900.0,
                contact=bad,
            )
    assert load_scheduled_channel_messages(path).messages == ()


def test_schedule_poll_rejects_contact(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    with pytest.raises(ScheduledChannelError, match="cannot carry a contact card"):
        schedule_channel_message(
            path,
            channel="general",
            text="which day?",
            publish_at=1000.0,
            now=900.0,
            kind="poll",
            options=["Saturday", "Sunday"],
            contact=CARD,
        )
    assert load_scheduled_channel_messages(path).messages == ()


def test_schedule_contact_wire_preflight(tmp_path):
    # 1,950 emoji fit the 8 KiB envelope on their own but overflow
    # once the card (~70 bytes) rides along: refuse at schedule time,
    # instead of failing every publish attempt forever.  Nothing is saved.
    path = tmp_path / "scheduled-channel-messages.json"
    with pytest.raises(ScheduledChannelError, match="too large"):
        schedule_channel_message(
            path,
            channel="general",
            text="😀" * 1950,
            publish_at=1000.0,
            now=900.0,
            contact=CARD,
        )
    assert load_scheduled_channel_messages(path).messages == ()
    # And 1,850 emoji with the same card still fit.
    entry = schedule_channel_message(
        path,
        channel="general",
        text="😀" * 1850,
        publish_at=1000.0,
        now=900.0,
        contact=CARD,
    )
    assert load_scheduled_channel_messages(path).messages[0].id == entry.id


def test_schedule_expires_in_round_trip(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path,
        channel="general",
        text="goodbye soon",
        publish_at=2000.0,
        now=1000.0,
        expires_in=7200.0,
    )
    queue = load_scheduled_channel_messages(path)
    assert len(queue) == 1
    assert queue.messages[0] == entry
    assert entry.expires_in == 7200.0
    assert entry.to_dict()["expires_in"] == 7200.0


def test_queue_without_expires_in_loads_with_no_expiry(tmp_path):
    # Queue files written before expiries could be scheduled carry no
    # expires_in: they load with no explicit expiry, exactly as
    # before — and a hand-built entry without one publishes with the
    # channel's disappearing default, like an immediate send.
    entry = _message()
    assert entry.expires_in is None
    loaded = ScheduledChannelMessage.from_dict(
        {
            "id": "a" * 16,
            "channel": "general",
            "text": "old hello",
            "publish_at": 1000.0,
            "created_at": 900.0,
        }
    )
    assert loaded.expires_in is None


def test_schedule_rejects_bad_expires_in(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    for bad in (0, -5, 0.0, 30 * 24 * 60 * 60 + 1, "2h", True, float("nan")):
        with pytest.raises(ScheduledChannelError, match="expires_in"):
            schedule_channel_message(
                path,
                channel="general",
                text="goodbye soon",
                publish_at=1000.0,
                now=900.0,
                expires_in=bad,
            )
    assert load_scheduled_channel_messages(path).messages == ()
    # A JSON integer beyond float range is the same invalid-duration
    # error, not an OverflowError escaping the queue's validation.
    with pytest.raises(ScheduledChannelError, match="expires_in"):
        schedule_channel_message(
            path,
            channel="general",
            text="goodbye soon",
            publish_at=1000.0,
            now=900.0,
            expires_in=10**400,
        )
    assert load_scheduled_channel_messages(path).messages == ()


def test_schedule_poll_accepts_expires_in(tmp_path):
    # Expiry is not a top-level attachment like a pin or a card, so a
    # scheduled poll can carry one — the protocol already allows
    # expires_at on poll messages.
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_poll(
        path,
        channel="general",
        question="which day?",
        options=["Saturday", "Sunday"],
        publish_at=2000.0,
        now=1000.0,
        expires_in=3600.0,
    )
    assert load_scheduled_channel_messages(path).messages[0].expires_in == 3600.0
    assert entry.to_dict()["expires_in"] == 3600.0


def test_schedule_expires_in_wire_preflight(tmp_path):
    # 1,917 emoji fit the 8 KiB envelope on their own but overflow
    # once the expiry (~20 bytes) rides along: refuse at schedule
    # time, instead of failing every publish attempt forever.  Nothing
    # is saved.  (The preflight measures with a full three-digit
    # fraction on the timestamps, so the boundary sits one emoji
    # below the no-expiry maximum.)
    path = tmp_path / "scheduled-channel-messages.json"
    with pytest.raises(ScheduledChannelError, match="too large"):
        schedule_channel_message(
            path,
            channel="general",
            text="😀" * 1917,
            publish_at=1000.0,
            now=900.0,
            expires_in=3600.0,
        )
    assert load_scheduled_channel_messages(path).messages == ()
    # And 1,912 emoji with the same expiry still fit.
    entry = schedule_channel_message(
        path,
        channel="general",
        text="😀" * 1912,
        publish_at=1000.0,
        now=900.0,
        expires_in=3600.0,
    )
    assert load_scheduled_channel_messages(path).messages[0].id == entry.id


def test_schedule_fractional_expires_in_wire_preflight(tmp_path):
    # A fractional expires_in serializes more digits than the
    # three-decimal rounding the old preflight measured, so the
    # boundary sits below the whole-second one: 1,917 emoji fit with
    # expires_in=3600 but overflow once the expiry carries thirteen
    # fractional digits.  Refuse at schedule time, instead of failing
    # every publish attempt forever.  Nothing is saved.
    path = tmp_path / "scheduled-channel-messages.json"
    with pytest.raises(ScheduledChannelError, match="too large"):
        schedule_channel_message(
            path,
            channel="general",
            text="😀" * 1917,
            publish_at=1000.0,
            now=900.0,
            expires_in=3600.123456789,
        )
    assert load_scheduled_channel_messages(path).messages == ()
    # And 1,915 emoji with the same expiry still fit.
    entry = schedule_channel_message(
        path,
        channel="general",
        text="😀" * 1915,
        publish_at=1000.0,
        now=900.0,
        expires_in=3600.123456789,
    )
    assert load_scheduled_channel_messages(path).messages[0].id == entry.id

    # Same for the channel disappearing default: a fractional default
    # serializes just as long as a fractional explicit expiry.
    defaults_path = tmp_path / "disappearing.json"
    save_disappearing_defaults(
        defaults_path,
        DisappearingDefaults(channels={"general": 3600.123456789}),
    )
    path_with_default = tmp_path / "scheduled-with-default.json"
    with pytest.raises(ScheduledChannelError, match="too large"):
        schedule_channel_message(
            path_with_default,
            channel="general",
            text="😀" * 1917,
            publish_at=1000.0,
            now=900.0,
            disappearing_path=defaults_path,
        )
    assert load_scheduled_channel_messages(path_with_default).messages == ()
    entry = schedule_channel_message(
        path_with_default,
        channel="general",
        text="😀" * 1915,
        publish_at=1000.0,
        now=900.0,
        disappearing_path=defaults_path,
    )
    assert (
        load_scheduled_channel_messages(path_with_default).messages[0].id
        == entry.id
    )


def test_queue_with_huge_expires_in_is_corrupt(tmp_path):
    # A hand-edited queue file carrying an expiry beyond float range
    # loads as a corrupt queue, so the publisher takes its
    # corrupt-queue path (logs and publishes nothing) instead of
    # crashing the mail loop on an OverflowError.
    import json

    path = tmp_path / "scheduled-channel-messages.json"
    path.write_text(
        json.dumps(
            {
                "messages": [
                    {
                        "id": "a" * 16,
                        "channel": "general",
                        "text": "goodbye soon",
                        "publish_at": 1000.0,
                        "created_at": 900.0,
                        "expires_in": 10**400,
                    }
                ]
            }
        )
    )
    with pytest.raises(ScheduledChannelError, match="expires_in"):
        load_scheduled_channel_messages(path)
    app = _FakeChannelApp()
    result = publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert result == {"published": 0, "kept": 0}
    assert app.created == []


def test_schedule_rejects_huge_publish_at(tmp_path):
    # A JSON integer beyond float range is the same invalid-timestamp
    # error, not an OverflowError escaping the schedule path.
    path = tmp_path / "scheduled-channel-messages.json"
    with pytest.raises(ScheduledChannelError, match="not a timestamp"):
        schedule_channel_message(
            path,
            channel="general",
            text="later",
            publish_at=10**400,
            now=900.0,
        )
    assert load_scheduled_channel_messages(path).messages == ()


@pytest.mark.parametrize(
    "field",
    ["publish_at", "created_at", "next_attempt_at", "paused_at"],
)
def test_queue_with_huge_timestamp_is_corrupt(tmp_path, field):
    # A hand-edited queue file carrying a timestamp beyond float range
    # in any of the four timestamp fields loads as a corrupt queue, so
    # the publisher takes its corrupt-queue path (logs and publishes
    # nothing) instead of crashing the mail loop on an OverflowError.
    import json

    entry = {
        "id": "a" * 16,
        "channel": "general",
        "text": "later",
        "publish_at": 1000.0,
        "created_at": 900.0,
        "next_attempt_at": 0.0,
        "paused_at": 0.0,
    }
    entry[field] = 10**400
    path = tmp_path / "scheduled-channel-messages.json"
    path.write_text(json.dumps({"messages": [entry]}))
    with pytest.raises(ScheduledChannelError, match="not a (timestamp|retry time)"):
        load_scheduled_channel_messages(path)
    app = _FakeChannelApp()
    result = publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert result == {"published": 0, "kept": 0}
    assert app.created == []


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


def test_edit_scheduled_channel_message_text_only(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path,
        channel="general",
        text="later",
        publish_at=2000.0,
        to=["alice"],
        ttl=2,
        now=1000.0,
    )
    edited = edit_scheduled_channel_message(
        path, entry.id[:8], text="fixed typo", now=1500.0
    )
    assert edited.id == entry.id
    assert edited.text == "fixed typo"
    assert edited.publish_at == 2000.0
    # Channel, targets, TTL and retry state are untouched.
    assert edited.channel == "general"
    assert edited.to == ("alice",)
    assert edited.ttl == 2
    assert edited.attempts == 0
    assert load_scheduled_channel_messages(path).messages[0].text == "fixed typo"


def test_edit_scheduled_channel_message_time_only(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path, channel="general", text="later", publish_at=2000.0, now=1000.0
    )
    edited = edit_scheduled_channel_message(
        path, entry.id, publish_at=3000.0, now=1500.0
    )
    assert edited.text == "later"
    assert edited.publish_at == 3000.0


def test_edit_uses_wire_created_at_in_preflight(tmp_path):
    # An edit measures the publish timestamp the way the publish pass
    # stamps it — with a full three-digit fraction — not the raw
    # publish time.  1,916 emoji fit the edit preflight measured with
    # a whole-second created_at but overflow it measured with the
    # wire stamp, so the edit is refused instead of queueing a
    # message that could never be published.  Nothing is changed.
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path, channel="general", text="hi", publish_at=1000.0, now=900.0
    )
    with pytest.raises(ScheduledChannelError, match="too large"):
        edit_scheduled_channel_message(path, entry.id, text="😀" * 1916)
    assert load_scheduled_channel_messages(path).messages[0].text == "hi"
    # And 1,915 emoji still fit.
    edited = edit_scheduled_channel_message(path, entry.id, text="😀" * 1915)
    assert len(load_scheduled_channel_messages(path).messages[0].text) == 1915
    assert edited.id == entry.id


def test_edit_scheduled_channel_message_text_and_time_reorders(tmp_path):
    # The queue stays soonest first: moving the sooner message later
    # swaps the list order.
    path = tmp_path / "scheduled-channel-messages.json"
    first = schedule_channel_message(
        path, channel="general", text="first", publish_at=2000.0, now=1000.0
    )
    second = schedule_channel_message(
        path, channel="general", text="second", publish_at=3000.0, now=1000.0
    )
    edited = edit_scheduled_channel_message(
        path, first.id, text="first, moved", publish_at=4000.0, now=1500.0
    )
    assert edited.text == "first, moved"
    assert [entry.id for entry in load_scheduled_channel_messages(path).messages] == [
        second.id,
        first.id,
    ]


def test_edit_scheduled_channel_message_requires_a_change(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path, channel="general", text="later", publish_at=2000.0, now=1000.0
    )
    with pytest.raises(ScheduledChannelError, match="nothing to change"):
        edit_scheduled_channel_message(path, entry.id, now=1500.0)
    assert load_scheduled_channel_messages(path).messages[0].text == "later"


def test_edit_scheduled_channel_message_unknown_and_ambiguous(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    save_scheduled_channel_messages(
        path,
        ScheduledChannelMessages(
            messages=(
                _message("ab" * 8, publish_at=2000.0),
                _message("ab" + "cd" * 7, publish_at=3000.0),
            )
        ),
    )
    with pytest.raises(ScheduledChannelError, match="no scheduled channel message"):
        edit_scheduled_channel_message(path, "ff" * 8, text="nope", now=1500.0)
    with pytest.raises(ScheduledChannelError, match="ambiguous"):
        edit_scheduled_channel_message(path, "ab", text="nope", now=1500.0)
    assert len(load_scheduled_channel_messages(path)) == 2


def test_edit_scheduled_channel_message_refuses_already_created(tmp_path):
    # A publish attempt already signed the message (message_hash set):
    # changing the text would orphan the stored copy — cancel instead.
    path = tmp_path / "scheduled-channel-messages.json"
    save_scheduled_channel_messages(
        path,
        ScheduledChannelMessages(
            messages=(_message(publish_at=2000.0, message_hash="f" * 64),)
        ),
    )
    with pytest.raises(ScheduledChannelError, match="already created"):
        edit_scheduled_channel_message(
            path, "a" * 16, text="too late", publish_at=3000.0, now=1500.0
        )
    assert load_scheduled_channel_messages(path).messages[0].text == "hello mesh"


def test_edit_scheduled_channel_message_rejects_bad_text(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path, channel="general", text="later", publish_at=2000.0, now=1000.0
    )
    with pytest.raises(ScheduledChannelError):
        edit_scheduled_channel_message(path, entry.id, text="   ", now=1500.0)
    with pytest.raises(ScheduledChannelError, match="too large"):
        edit_scheduled_channel_message(
            path, entry.id, text="😀" * 2000, now=1500.0
        )
    assert load_scheduled_channel_messages(path).messages[0].text == "later"


def test_edit_rejects_text_that_creation_would_reject(tmp_path):
    # 1,920 four-byte characters pass the old preflight envelope,
    # which omits the v / expires_at / reply_to fields the publish
    # pass adds — but fail the full normalized envelope, so the same
    # text is refused at schedule time too when the channel carries a
    # disappearing default (the schedule preflight sizes the channel's
    # actual default).  The edit always measures the largest expiry,
    # so it must refuse the text as well, or the entry would sit
    # queued for failed retries.
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path, channel="general", text="later", publish_at=2000.0, now=1000.0
    )
    defaults_path = tmp_path / "disappearing.json"
    save_disappearing_defaults(
        defaults_path, DisappearingDefaults(channels={"general": 3600.0})
    )
    with pytest.raises(ScheduledChannelError, match="too large"):
        schedule_channel_message(
            path,
            channel="general",
            text="😀" * 1920,
            publish_at=2000.0,
            now=1000.0,
            disappearing_path=defaults_path,
        )
    with pytest.raises(ScheduledChannelError, match="too large"):
        edit_scheduled_channel_message(
            path, entry.id, text="😀" * 1920, now=1500.0
        )
    assert load_scheduled_channel_messages(path).messages[0].text == "later"


def test_edit_rejects_text_that_only_fits_without_an_expiry(tmp_path):
    # Disappearing-message boundary: 1,918 four-byte characters fit
    # the normalized envelope with a null expiry, but overflow once
    # publishing attaches the numeric expiry from the channel's
    # disappearing-message default.  Scheduling refuses the text when
    # the channel has a default (the schedule preflight sizes the
    # actual default), and the edit refuses it unconditionally — it
    # always measures the largest expiry, since a default can be set
    # after scheduling.
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path, channel="general", text="later", publish_at=2000.0, now=1000.0
    )
    defaults_path = tmp_path / "disappearing.json"
    save_disappearing_defaults(
        defaults_path, DisappearingDefaults(channels={"general": 3600.0})
    )
    with pytest.raises(ScheduledChannelError, match="too large"):
        schedule_channel_message(
            path,
            channel="general",
            text="😀" * 1918,
            publish_at=2000.0,
            now=1000.0,
            disappearing_path=defaults_path,
        )
    with pytest.raises(ScheduledChannelError, match="too large"):
        edit_scheduled_channel_message(
            path, entry.id, text="😀" * 1918, now=1500.0
        )
    assert load_scheduled_channel_messages(path).messages[0].text == "later"


def test_edit_scheduled_channel_message_rejects_bad_time(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path, channel="general", text="later", publish_at=2000.0, now=1000.0
    )
    with pytest.raises(ScheduledChannelError, match="in the future"):
        edit_scheduled_channel_message(path, entry.id, publish_at=1400.0, now=1500.0)
    with pytest.raises(ScheduledChannelError, match="within 7d"):
        edit_scheduled_channel_message(
            path, entry.id, publish_at=1500.0 + 7 * 24 * 60 * 60 + 1, now=1500.0
        )
    assert load_scheduled_channel_messages(path).messages[0].publish_at == 2000.0


class _FakeChannelApp:
    """ChannelApp stand-in: records creates and publishes, delivers to nobody."""

    def __init__(
        self,
        fail_publish: bool = False,
        fail_with_errors: bool = False,
        default_lifetime=None,
    ):
        self.created = []
        self.published = []
        self.stored = {}
        self.fail_publish = fail_publish
        self.fail_with_errors = fail_with_errors
        # Simulated channel disappearing default, applied only when the
        # caller passes no explicit expires_at (as ChannelApp.send does).
        self.default_lifetime = default_lifetime

    def send(
        self, channel, text, *, publish=False, expires_at=None, poll_options=None,
        location=None, contact=None,
    ):
        message = {
            "hash": "c" * 64,
            "channel": channel,
            "text": text,
            "created_at": 2000.0,
        }
        if expires_at is None and self.default_lifetime is not None:
            expires_at = message["created_at"] + self.default_lifetime
        if expires_at is not None:
            message["expires_at"] = expires_at
        if poll_options is not None:
            message["poll_options"] = list(poll_options)
        if location is not None:
            message["location"] = location
        if contact is not None:
            message["contact"] = contact
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


def test_publish_due_attaches_location(tmp_path):
    # The published message carries the queued pin, so it renders
    # exactly like one sent immediately with the same pin.
    path = tmp_path / "scheduled-channel-messages.json"
    pinned = schedule_channel_message(
        path,
        channel="general",
        text="meet here",
        publish_at=1000.0,
        now=900.0,
        location=PIN,
    )
    app = _FakeChannelApp()

    result = publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert result == {"published": 1, "kept": 0}
    assert len(app.created) == 1
    assert app.created[0]["text"] == "meet here"
    assert app.created[0]["location"] == PIN
    assert app.published[0][0]["location"] == PIN
    queue = load_scheduled_channel_messages(path)
    assert all(entry.id != pinned.id for entry in queue.messages)
    assert len(queue) == 0


def test_publish_due_attaches_contact(tmp_path):
    # The published message carries the queued card, so it renders
    # exactly like one sent immediately with the same card.
    path = tmp_path / "scheduled-channel-messages.json"
    shared = schedule_channel_message(
        path,
        channel="general",
        text="meet carol",
        publish_at=1000.0,
        now=900.0,
        contact=CARD,
    )
    app = _FakeChannelApp()

    result = publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert result == {"published": 1, "kept": 0}
    assert len(app.created) == 1
    assert app.created[0]["text"] == "meet carol"
    assert app.created[0]["contact"] == CARD
    assert app.published[0][0]["contact"] == CARD
    queue = load_scheduled_channel_messages(path)
    assert all(entry.id != shared.id for entry in queue.messages)
    assert len(queue) == 0


def test_publish_due_applies_expires_in(tmp_path):
    # The published message expires that far past the publish time, so
    # it disappears exactly like one sent immediately with the same
    # --expire — and the explicit expiry wins over the channel
    # disappearing default (the fake app applies a 1h default here,
    # and the entry's 2h still wins).
    path = tmp_path / "scheduled-channel-messages.json"
    expiring = schedule_channel_message(
        path,
        channel="general",
        text="goodbye soon",
        publish_at=1000.0,
        now=900.0,
        expires_in=7200.0,
    )
    app = _FakeChannelApp(default_lifetime=3600.0)

    result = publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert result == {"published": 1, "kept": 0}
    assert len(app.created) == 1
    assert app.created[0]["text"] == "goodbye soon"
    assert app.created[0]["expires_at"] == 2000.0 + 7200.0
    assert app.published[0][0]["expires_at"] == 2000.0 + 7200.0
    queue = load_scheduled_channel_messages(path)
    assert all(entry.id != expiring.id for entry in queue.messages)
    assert len(queue) == 0


def test_publish_due_applies_expires_in_to_a_poll(tmp_path):
    path = tmp_path / "scheduled-channel-messages.json"
    poll = schedule_channel_poll(
        path,
        channel="general",
        question="which day?",
        options=["Saturday", "Sunday"],
        publish_at=1000.0,
        now=900.0,
        expires_in=3600.0,
    )
    app = _FakeChannelApp()

    result = publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert result == {"published": 1, "kept": 0}
    assert app.created[0]["expires_at"] == 2000.0 + 3600.0
    assert app.published[0][0]["expires_at"] == 2000.0 + 3600.0
    assert app.published[0][0]["poll_options"] == ["Saturday", "Sunday"]
    queue = load_scheduled_channel_messages(path)
    assert all(entry.id != poll.id for entry in queue.messages)


def test_publish_due_replacement_reaches_all_peers_after_expiry(tmp_path):
    # Partial delivery, then the stored copy expires before the retry:
    # the replacement is a new message for the full intended set, not
    # a retry for only the peers that missed the old copy — the peer
    # that got the first copy holds an expired one and needs the
    # replacement too.
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path,
        channel="general",
        text="meet soon",
        publish_at=1000.0,
        to=["alice", "bob"],
        now=900.0,
        expires_in=60.0,
    )
    app = _FakeChannelApp(fail_with_errors=True)
    resolved = {
        "alice": SimpleNamespace(hexhash="alice-hash"),
        "bob": SimpleNamespace(hexhash="bob-hash"),
        "deadbeef": SimpleNamespace(hexhash="deadbeef-hash"),
    }
    # First pass: the publish reports a failure, so the entry stays
    # queued with that peer still owed.
    result = publish_due_scheduled_channel_messages(
        path, app, resolve_peer=lambda target: resolved[target], now=2000.0
    )
    assert result == {"published": 0, "kept": 1}
    assert len(app.created) == 1
    # The stored copy expires 60s after the first send; retry after it.
    app.fail_with_errors = False
    result = publish_due_scheduled_channel_messages(
        path, app, resolve_peer=lambda target: resolved[target], now=3000.0
    )
    assert result == {"published": 1, "kept": 0}
    assert len(app.created) == 2
    # The replacement went to the full intended set, not only to the
    # peer that missed the old copy.
    assert [peer.hexhash for peer in app.published[0][1]] == [
        "alice-hash",
        "bob-hash",
    ]
    queue = load_scheduled_channel_messages(path)
    assert all(message.id != entry.id for message in queue.messages)


def test_publish_due_replacement_reaches_all_peers_when_copy_is_gone(tmp_path):
    # Same partial delivery, but the stored copy was already deleted
    # by the time of the retry (retention cleanup of the expired row):
    # the publisher still signs a replacement for the full intended
    # set, because it cannot tell a cleaned-up expired copy from an
    # evicted one — and the peers that got the first copy may hold
    # an expired one.
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path,
        channel="general",
        text="meet soon",
        publish_at=1000.0,
        to=["alice", "bob"],
        now=900.0,
        expires_in=60.0,
    )
    app = _FakeChannelApp(fail_with_errors=True)
    resolved = {
        "alice": SimpleNamespace(hexhash="alice-hash"),
        "bob": SimpleNamespace(hexhash="bob-hash"),
        "deadbeef": SimpleNamespace(hexhash="deadbeef-hash"),
    }
    result = publish_due_scheduled_channel_messages(
        path, app, resolve_peer=lambda target: resolved[target], now=2000.0
    )
    assert result == {"published": 0, "kept": 1}
    assert len(app.created) == 1
    # The expired row is cleaned up before the retry.
    app.stored.clear()
    app.fail_with_errors = False
    result = publish_due_scheduled_channel_messages(
        path, app, resolve_peer=lambda target: resolved[target], now=3000.0
    )
    assert result == {"published": 1, "kept": 0}
    assert len(app.created) == 2
    assert [peer.hexhash for peer in app.published[0][1]] == [
        "alice-hash",
        "bob-hash",
    ]
    queue = load_scheduled_channel_messages(path)
    assert all(message.id != entry.id for message in queue.messages)


def test_publish_due_rechecks_expiry_after_resolution(tmp_path, monkeypatch):
    # The stored copy is still alive when the pass starts, but peer
    # resolution outlasts its remaining lifetime: the pass must not
    # publish it as delivered.  It signs a fresh copy for the full
    # intended set, with the new expiry measured from a clock reading
    # taken immediately before sending.
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path,
        channel="general",
        text="meet soon",
        publish_at=1000.0,
        to=["alice", "bob"],
        now=900.0,
        expires_in=60.0,
    )
    app = _FakeChannelApp(fail_with_errors=True)
    resolved = {
        "alice": SimpleNamespace(hexhash="alice-hash"),
        "bob": SimpleNamespace(hexhash="bob-hash"),
        "deadbeef": SimpleNamespace(hexhash="deadbeef-hash"),
    }
    result = publish_due_scheduled_channel_messages(
        path, app, resolve_peer=lambda target: resolved[target], now=2000.0
    )
    assert result == {"published": 0, "kept": 1}
    assert len(app.created) == 1
    # Second pass with a live clock: the pass reads the clock three
    # times (pass start, before resolution, after resolution) plus
    # once more before signing the replacement.  The copy (expiring
    # at 2060) is alive at the pre-resolution reading (2059) and
    # expired by the post-resolution one (3000).  Extra readings
    # (e.g. the logging module's own clock calls) get the same
    # post-resolution value, so they cannot change the outcome.
    readings = iter([3000.0, 2059.0, 3000.0, 3000.0])
    monkeypatch.setattr(time, "time", lambda: next(readings, 3000.0))
    app.fail_with_errors = False
    result = publish_due_scheduled_channel_messages(
        path, app, resolve_peer=lambda target: resolved[target]
    )
    assert result == {"published": 1, "kept": 0}
    assert len(app.created) == 2
    assert app.created[1]["expires_at"] == 3000.0 + 60.0
    assert [peer.hexhash for peer in app.published[0][1]] == [
        "alice-hash",
        "bob-hash",
    ]
    queue = load_scheduled_channel_messages(path)
    assert all(message.id != entry.id for message in queue.messages)


def test_publish_due_explicit_expires_in_wins_over_channel_default(tmp_path):
    # With a channel disappearing default in force, an entry carrying its
    # own expires_in keeps that expiry, while one without it takes the
    # default — the same precedence an immediate send applies.
    path = tmp_path / "scheduled-channel-messages.json"
    schedule_channel_message(
        path,
        channel="general",
        text="explicit",
        publish_at=1000.0,
        now=900.0,
        expires_in=7200.0,
    )
    schedule_channel_message(
        path, channel="general", text="default", publish_at=1000.0, now=900.0
    )
    app = _FakeChannelApp(default_lifetime=86400.0)

    result = publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert result == {"published": 2, "kept": 0}
    expires = {m["text"]: m["expires_at"] for m in app.created}
    assert expires["explicit"] == 2000.0 + 7200.0
    assert expires["default"] == 2000.0 + 86400.0


def test_publish_due_without_expires_in_passes_no_expiry(tmp_path):
    # Without an explicit expiry the publish pass passes none, so the
    # channel's disappearing default applies at send time — exactly
    # like an immediate send with no --expire.
    path = tmp_path / "scheduled-channel-messages.json"
    schedule_channel_message(
        path, channel="general", text="hello", publish_at=1000.0, now=900.0
    )
    app = _FakeChannelApp()

    result = publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert result == {"published": 1, "kept": 0}
    assert "expires_at" not in app.created[0]


def test_publish_due_reuses_a_live_stored_copy(tmp_path):
    # A failed first publish signs the message and leaves the entry
    # queued with its hash.  A retry before the copy's expiry re-sends
    # that same copy instead of signing a second one.
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path,
        channel="general",
        text="goodbye soon",
        publish_at=1000.0,
        now=900.0,
        expires_in=3600.0,
    )
    app = _FakeChannelApp(fail_publish=True)
    result = publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert result == {"published": 0, "kept": 1}
    assert len(app.created) == 1
    assert app.created[0]["expires_at"] == 2000.0 + 3600.0
    assert app.published == []
    queued = load_scheduled_channel_messages(path).messages[0]
    assert queued.id == entry.id
    assert queued.message_hash == "c" * 64
    assert queued.attempts == 1

    app.fail_publish = False
    result = publish_due_scheduled_channel_messages(path, app, now=2100.0)
    assert result == {"published": 1, "kept": 0}
    assert len(app.created) == 1
    assert app.published[0][0]["expires_at"] == 2000.0 + 3600.0
    assert load_scheduled_channel_messages(path).messages == ()


def test_publish_due_resigns_an_expired_stored_copy(tmp_path):
    # The same retry after the signed copy's expiry signs a fresh
    # copy instead: recipients drop expired messages on arrival, so
    # re-sending the old copy can never deliver, and a stuck entry
    # is worse than a duplicate for the peers that hold the first copy.
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path,
        channel="general",
        text="goodbye soon",
        publish_at=1000.0,
        now=900.0,
        expires_in=3600.0,
    )
    app = _FakeChannelApp(fail_publish=True)
    result = publish_due_scheduled_channel_messages(path, app, now=2000.0)
    assert result == {"published": 0, "kept": 1}
    assert app.created[0]["expires_at"] == 2000.0 + 3600.0

    app.fail_publish = False
    result = publish_due_scheduled_channel_messages(path, app, now=6000.0)
    assert result == {"published": 1, "kept": 0}
    assert len(app.created) == 2
    assert app.created[1]["expires_at"] == 6000.0 + 3600.0
    assert app.published[0][0]["expires_at"] == 6000.0 + 3600.0
    queue = load_scheduled_channel_messages(path)
    assert all(item.id != entry.id for item in queue.messages)
    assert len(queue) == 0


def test_channel_contact_refusal_policy():
    # The single policy for immediate and scheduled channel sends:
    # a clean card for an allowed peer passes, and the refusals —
    # own card, blocked peer, unreadable block-list — each name
    # themselves.
    card = {"name": "carol", "destination_hash": "cc" * 16}
    assert (
        channel_contact_refusal(card, own_hash="aa" * 16, blocklist=BlockList())
        is None
    )
    assert "own contact" in (
        channel_contact_refusal(
            {"name": "me", "destination_hash": "aa" * 16},
            own_hash="aa" * 16,
            blocklist=BlockList(),
        )
        or ""
    )
    assert "blocked peer" in (
        channel_contact_refusal(
            card,
            own_hash="aa" * 16,
            blocklist=BlockList(blocked=frozenset({"cc" * 16})),
        )
        or ""
    )
    assert "block-list could not be read" in (
        channel_contact_refusal(card, own_hash="aa" * 16, blocklist=None) or ""
    )


def test_publish_due_parks_refused_contact(tmp_path):
    # A card whose peer was blocked after scheduling is never
    # published: the entry parks at the attempt cap instead of
    # burning retries, and stays queued for the user to cancel.
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path,
        channel="general",
        text="meet carol",
        publish_at=1000.0,
        now=900.0,
        contact=CARD,
    )
    app = _FakeChannelApp()
    vetted = []

    def refuse(card):
        vetted.append(card)
        return "cannot share a contact for a blocked peer"

    result = publish_due_scheduled_channel_messages(
        path, app, contact_refusal=refuse, now=2000.0
    )
    assert result == {"published": 0, "kept": 1}
    assert vetted == [CARD]
    assert app.created == []
    assert app.published == []
    queue = load_scheduled_channel_messages(path)
    assert len(queue) == 1
    assert queue.messages[0].id == entry.id
    assert queue.messages[0].attempts == MAX_PUBLISH_ATTEMPTS
    # A parked entry is never retried: a later pass leaves it alone.
    result = publish_due_scheduled_channel_messages(
        path, app, contact_refusal=refuse, now=3000.0
    )
    assert result == {"published": 0, "kept": 0}
    assert app.created == []


def test_publish_due_vet_refuses_stored_message_on_retry(tmp_path):
    # The vet runs before a stored message is re-published too: a
    # first attempt that fails to deliver, followed by the peer being
    # blocked, must not send the stored copy on the retry.
    path = tmp_path / "scheduled-channel-messages.json"
    schedule_channel_message(
        path,
        channel="general",
        text="meet carol",
        publish_at=1000.0,
        now=900.0,
        contact=CARD,
    )
    app = _FakeChannelApp(fail_with_errors=True)

    def allow(card):
        return None

    result = publish_due_scheduled_channel_messages(
        path, app, contact_refusal=allow, now=2000.0
    )
    assert result == {"published": 0, "kept": 1}
    assert len(app.created) == 1
    assert app.published == []
    queued = load_scheduled_channel_messages(path).messages[0]
    assert queued.message_hash == "c" * 64
    assert queued.attempts == 1

    def refuse(card):
        return "cannot share a contact for a blocked peer"

    result = publish_due_scheduled_channel_messages(
        path, app, contact_refusal=refuse, now=3000.0
    )
    assert result == {"published": 0, "kept": 1}
    assert app.published == []
    queued = load_scheduled_channel_messages(path).messages[0]
    assert queued.message_hash == "c" * 64
    assert queued.attempts == MAX_PUBLISH_ATTEMPTS


def test_edit_scheduled_message_keeps_location(tmp_path):
    # Editing a pinned entry's text keeps its pin, and the edit
    # preflight measures the pin: 1,910 emoji fit without one but
    # overflow with it, so that edit is refused while a 1,899-emoji
    # edit (which fits with the pin) goes through with the pin intact.
    # (The boundary sits one emoji below the old whole-second
    # measurement: the edit preflight stamps the publish time with a
    # full three-digit fraction, like the schedule path.)
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path,
        channel="general",
        text="meet here",
        publish_at=5000.0,
        now=1000.0,
        location=PIN,
    )
    with pytest.raises(ScheduledChannelError, match="too large"):
        edit_scheduled_channel_message(path, entry.id, text="😀" * 1910, now=1500.0)
    edited = edit_scheduled_channel_message(
        path, entry.id, text="😀" * 1899, now=1500.0
    )
    assert edited.location == PIN
    assert load_scheduled_channel_messages(path).messages[0].location == PIN


def test_edit_scheduled_message_keeps_contact(tmp_path):
    # Editing a card-carrying entry's text keeps its card, and the
    # edit preflight measures the card: 1,950 emoji fit without one
    # but overflow with it, so that edit is refused while a 1,850-emoji
    # edit (which fits with the card) goes through with the card intact.
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path,
        channel="general",
        text="meet carol",
        publish_at=5000.0,
        now=1000.0,
        contact=CARD,
    )
    with pytest.raises(ScheduledChannelError, match="too large"):
        edit_scheduled_channel_message(path, entry.id, text="😀" * 1950, now=1500.0)
    edited = edit_scheduled_channel_message(
        path, entry.id, text="😀" * 1850, now=1500.0
    )
    assert edited.contact == CARD
    assert load_scheduled_channel_messages(path).messages[0].contact == CARD


def test_edit_scheduled_message_keeps_expires_in(tmp_path):
    # Editing an expiring entry's text keeps its expiry, and the edit
    # preflight measures the expiry: 1,920 emoji fit without one but
    # overflow with it, so that edit is refused while a 1,910-emoji
    # edit (which fits with the expiry) goes through with the expiry
    # intact.
    path = tmp_path / "scheduled-channel-messages.json"
    entry = schedule_channel_message(
        path,
        channel="general",
        text="goodbye soon",
        publish_at=5000.0,
        now=1000.0,
        expires_in=3600.0,
    )
    with pytest.raises(ScheduledChannelError, match="too large"):
        edit_scheduled_channel_message(path, entry.id, text="😀" * 1920, now=1500.0)
    edited = edit_scheduled_channel_message(
        path, entry.id, text="😀" * 1910, now=1500.0
    )
    assert edited.expires_in == 3600.0
    assert load_scheduled_channel_messages(path).messages[0].expires_in == 3600.0


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
