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
    MAX_PUBLISH_ATTEMPTS,
    ScheduledPost,
    ScheduledPosts,
    ScheduledPostsError,
    cancel_scheduled_post,
    edit_scheduled_post,
    load_scheduled_posts,
    publish_due_scheduled_posts,
    save_scheduled_posts,
    schedule_quote,
    schedule_repost,
    schedule_poll,
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


def test_store_rejects_reply_with_attachment(tmp_path):
    # The write path refuses both combinations, and the publish pass
    # would drop the attachment silently — so a hand-edited queue file
    # carrying either must fail to load.
    for field, value in (
        ("location", {"lat": 34.05, "lon": -83.88}),
        ("contact", {"name": "carol", "destination_hash": "cc" * 16}),
        ("image", {"sha256": "ab" * 32, "size": 10, "mime": "image/png"}),
    ):
        path = tmp_path / "scheduled-posts.json"
        entry = {
            "id": "ab" * 8,
            "text": "a reply",
            "reply_to": "de" * 32,
            "publish_at": 1000.0,
            "created_at": 999.0,
            field: value,
        }
        path.write_text(json.dumps({"posts": [entry]}), encoding="utf-8")
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


def test_schedule_post_rejects_text_that_cannot_fit_a_tan_request(tmp_path):
    # 2,000 four-byte characters pass the character limit but overflow
    # the 8 KiB Tan request once encoded: refuse at schedule time,
    # instead of failing every publish attempt forever.  Nothing is saved.
    path = tmp_path / "scheduled-posts.json"
    with pytest.raises(ScheduledPostsError, match="encoded limit"):
        schedule_post(path, text="😀" * 2000, publish_at=1000.0, now=900.0)
    assert load_scheduled_posts(path).posts == ()


def test_schedule_post_rejects_text_that_only_fits_with_short_key(tmp_path):
    # The preflight check once reserved 64 characters for the author's
    # key, but a real post key is 128 hex characters: 1,900 four-byte
    # characters passed the old check yet overflowed the 8 KiB request
    # once the real key was encoded.  Refuse at schedule time.
    path = tmp_path / "scheduled-posts.json"
    with pytest.raises(ScheduledPostsError, match="encoded limit"):
        schedule_post(path, text="😀" * 1900, publish_at=1000.0, now=900.0)
    assert load_scheduled_posts(path).posts == ()


def test_schedule_post_accepts_long_text_that_fits(tmp_path):
    # 1,500 four-byte characters (6,000 bytes) fit the request with
    # room to spare.
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(path, text="😀" * 1500, publish_at=1000.0, now=900.0)
    assert load_scheduled_posts(path).posts[0].id == post.id


def test_schedule_poll_round_trip(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    poll = schedule_poll(
        path,
        question="pizza or tacos?",
        options=["Pizza", "Tacos"],
        publish_at=2000.0,
        to=["alice"],
        ttl=2,
        now=1000.0,
    )
    queue = load_scheduled_posts(path)
    assert len(queue) == 1
    assert queue.posts[0] == poll
    assert poll.kind == "poll"
    assert poll.text == "pizza or tacos?"
    assert poll.options == ("Pizza", "Tacos")
    assert poll.reply_to is None
    assert poll.to == ("alice",)
    assert poll.ttl == 2
    assert poll.publish_at == 2000.0
    assert poll.created_at == 1000.0


def test_schedule_repost_and_quote_round_trip(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    repost = schedule_repost(
        path, target_post="d" * 64, publish_at=2000.0, now=1000.0
    )
    quote = schedule_quote(
        path,
        target_post="e" * 64,
        text="this, tomorrow",
        publish_at=3000.0,
        now=1000.0,
    )
    queue = load_scheduled_posts(path)
    assert len(queue) == 2
    assert queue.posts[0] == repost
    assert repost.kind == "repost"
    assert repost.text == ""
    assert repost.target_post == "d" * 64
    assert queue.posts[1] == quote
    assert quote.kind == "quote"
    assert quote.text == "this, tomorrow"
    assert quote.target_post == "e" * 64


def test_load_old_queue_file_without_kind_is_post(tmp_path):
    # Queue files written before polls could be scheduled carry no
    # kind or options: they load as posts, exactly as before.
    path = tmp_path / "scheduled-posts.json"
    path.write_text(
        json.dumps(
            {
                "posts": [
                    {
                        "id": "a" * 16,
                        "text": "old post",
                        "publish_at": 1000.0,
                        "created_at": 900.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    loaded = load_scheduled_posts(path).posts[0]
    assert loaded.kind == "post"
    assert loaded.options == ()


def test_old_queue_file_without_kind_loads_as_post(tmp_path):
    # Queue files written before kinds existed carry no kind or
    # target_post: they load as plain posts, exactly as before.
    path = tmp_path / "scheduled-posts.json"
    path.write_text(
        json.dumps(
            {
                "posts": [
                    {
                        "id": "a" * 16,
                        "text": "hi",
                        "publish_at": 1000.0,
                        "created_at": 900.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    queue = load_scheduled_posts(path)
    assert queue.posts[0].kind == "post"
    assert queue.posts[0].target_post is None


def test_schedule_poll_refuses_bad_options(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    for options in (
        ["only one"],
        ["a", "b", "c", "d", "e", "f", "g", "h", "i"],
        ["Pizza", "Pizza"],
        ["Pizza", "  "],
        ["Pizza", 7],
    ):
        with pytest.raises(ScheduledPostsError):
            schedule_poll(
                path,
                question="pick one",
                options=options,  # type: ignore[arg-type]
                publish_at=2000.0,
                now=1000.0,
            )
    assert load_scheduled_posts(path).posts == ()


def test_schedule_poll_refuses_reply_and_post_refuses_options(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    with pytest.raises(ScheduledPostsError, match="cannot be a reply"):
        schedule_post(
            path,
            text="pick one",
            publish_at=2000.0,
            reply_to="b" * 64,
            kind="poll",
            options=["A", "B"],
            now=1000.0,
        )
    with pytest.raises(ScheduledPostsError, match="cannot carry poll options"):
        schedule_post(
            path,
            text="plain post",
            publish_at=2000.0,
            options=["A", "B"],
            now=1000.0,
        )
    assert load_scheduled_posts(path).posts == ()


def test_schedule_repost_and_quote_validation(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    # A repost carries no text, no reply target, no expiry — and it
    # must name the post it amplifies.
    with pytest.raises(ScheduledPostsError, match="carries no text"):
        schedule_post(path, text="nope", publish_at=1000.0, kind="repost",
                      target_post="d" * 64)
    with pytest.raises(ScheduledPostsError, match="needs a target post"):
        schedule_post(path, text="", publish_at=1000.0, kind="repost")
    with pytest.raises(ScheduledPostsError, match="cannot be a reply"):
        schedule_post(path, text="", publish_at=1000.0, kind="repost",
                      target_post="d" * 64, reply_to="b" * 64)
    with pytest.raises(ScheduledPostsError, match="cannot expire"):
        schedule_post(path, text="", publish_at=1000.0, kind="repost",
                      target_post="d" * 64, expires_in=60.0)
    # A quote needs its target too, and a plain post cannot carry one.
    with pytest.raises(ScheduledPostsError, match="needs a target post"):
        schedule_post(path, text="hi", publish_at=1000.0, kind="quote")
    with pytest.raises(ScheduledPostsError, match="cannot carry a target post"):
        schedule_post(path, text="hi", publish_at=1000.0, target_post="d" * 64)
    with pytest.raises(ScheduledPostsError, match="not a scheduled entry kind"):
        schedule_post(path, text="hi", publish_at=1000.0, kind="bogus")
    # The dataclass enforces the same rules for hand-built entries.
    with pytest.raises(ScheduledPostsError, match="carries no text"):
        _post(kind="repost", text="nope", target_post="d" * 64)
    with pytest.raises(ScheduledPostsError, match="needs a target post"):
        _post(kind="quote", target_post=None)
    assert _post(kind="repost", text="", target_post="d" * 64).kind == "repost"
    assert load_scheduled_posts(path).posts == ()


def test_schedule_poll_rejects_question_and_options_that_cannot_fit(tmp_path):
    # A 1,860-character question fits alone and with two short options,
    # but not once 8 hundred-character options join it: refuse at
    # schedule time, instead of failing every publish attempt forever.
    path = tmp_path / "scheduled-posts.json"
    long_options = [chr(ord("a") + i) + "x" * 99 for i in range(8)]
    with pytest.raises(ScheduledPostsError, match="encoded limit"):
        schedule_poll(
            path,
            question="😀" * 1860,
            options=long_options,
            publish_at=1000.0,
            now=900.0,
        )
    assert load_scheduled_posts(path).posts == ()


def test_schedule_quote_rejects_comment_that_only_fits_without_a_target(tmp_path):
    # 1,880 four-byte characters fit a plain post, but a quote carries
    # its target hash in the envelope too: refuse at schedule time
    # instead of failing every publish attempt forever.
    path = tmp_path / "scheduled-posts.json"
    with pytest.raises(ScheduledPostsError, match="encoded limit"):
        schedule_quote(
            path, target_post="d" * 64, text="😀" * 1880,
            publish_at=1000.0, now=900.0,
        )
    assert load_scheduled_posts(path).posts == ()


def _image() -> dict:
    return {
        "sha256": "a" * 64,
        "size": 72,
        "mime": "image/png",
        "name": "photo.png",
    }


def test_schedule_post_with_image_round_trips(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(
        path, text="morning", publish_at=1000.0, now=900.0, image=_image()
    )
    assert post.image == _image()
    assert load_scheduled_posts(path).posts[0].image == _image()
    assert load_scheduled_posts(path).posts[0].to_dict()["image"] == _image()


def test_schedule_quote_with_image_round_trips(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    quote = schedule_quote(
        path,
        target_post="d" * 64,
        text="look at this",
        publish_at=1000.0,
        now=900.0,
        image=_image(),
    )
    assert quote.image == _image()
    assert load_scheduled_posts(path).posts[0].image == _image()
    assert load_scheduled_posts(path).posts[0].to_dict()["image"] == _image()


def test_schedule_quote_rejects_image_on_repost_but_not_quote(tmp_path):
    # A quote is a top-level entry, so it may carry an image; a
    # repost has no comment of its own and still cannot.
    path = tmp_path / "scheduled-posts.json"
    with pytest.raises(ScheduledPostsError, match="cannot carry an image"):
        schedule_post(
            path, text="", publish_at=1000.0, now=900.0,
            kind="repost", target_post="d" * 64, image=_image(),
        )
    quote = schedule_post(
        path, text="look", publish_at=1000.0, now=900.0,
        kind="quote", target_post="d" * 64, image=_image(),
    )
    assert quote.image == _image()
    assert load_scheduled_posts(path).posts[0].image == _image()


def test_schedule_post_without_image_loads_as_none(tmp_path):
    # Old queue files predate the image field: they load with no image.
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(path, text="plain", publish_at=1000.0, now=900.0)
    assert post.image is None
    stored = post.to_dict()
    del stored["image"]
    path.write_text(json.dumps({"posts": [stored]}))
    assert load_scheduled_posts(path).posts[0].image is None


def test_schedule_post_rejects_image_on_reply(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    with pytest.raises(ScheduledPostsError, match="top-level"):
        schedule_post(
            path,
            text="reply",
            publish_at=1000.0,
            now=900.0,
            reply_to="b" * 64,
            image=_image(),
        )
    assert load_scheduled_posts(path).posts == ()


def test_schedule_poll_accepts_long_question_with_long_options(tmp_path):
    # 1,500 four-byte characters plus 8 hundred-character options still
    # fit the request with room to spare.
    path = tmp_path / "scheduled-posts.json"
    long_options = [chr(ord("a") + i) + "x" * 99 for i in range(8)]
    poll = schedule_poll(
        path,
        question="😀" * 1500,
        options=long_options,
        publish_at=1000.0,
        now=900.0,
    )
    assert load_scheduled_posts(path).posts[0].id == poll.id


def test_schedule_post_rejects_bad_image(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    bad_images = [
        {"sha256": "zz" * 32, "size": 72, "mime": "image/png", "name": "photo.png"},
        {"sha256": "a" * 64, "size": 0, "mime": "image/png", "name": "photo.png"},
        {"sha256": "a" * 64, "size": 72, "mime": "image/tiff", "name": "photo.png"},
        {"sha256": "a" * 64, "size": 72, "mime": "image/png", "name": "../photo.png"},
        "not-an-object",
    ]
    for bad in bad_images:
        with pytest.raises(ScheduledPostsError):
            schedule_post(path, text="x", publish_at=1000.0, now=900.0, image=bad)
    assert load_scheduled_posts(path).posts == ()


def test_schedule_post_rejects_text_that_only_fits_without_image(tmp_path):
    # The image descriptor is ~130 bytes of JSON inside the 8 KiB Tan
    # request: 1,860 four-byte characters fit on their own (the first
    # failure without an image is 1,891) but not with one attached
    # (the first failure with an image is 1,858).  Refuse at schedule
    # time, instead of failing every publish attempt forever.
    path = tmp_path / "scheduled-posts.json"
    schedule_post(path, text="😀" * 1860, publish_at=1000.0, now=900.0)
    with pytest.raises(ScheduledPostsError, match="encoded limit"):
        schedule_post(
            path, text="😀" * 1860, publish_at=1000.0, now=900.0, image=_image()
        )
    assert len(load_scheduled_posts(path).posts) == 1


def _location() -> dict:
    return {"lat": 34.05, "lon": -83.88, "name": "Rock Springs Park"}


def test_schedule_post_with_location_round_trips(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(
        path, text="morning", publish_at=1000.0, now=900.0, location=_location()
    )
    assert post.location == _location()
    assert load_scheduled_posts(path).posts[0].location == _location()
    assert load_scheduled_posts(path).posts[0].to_dict()["location"] == _location()


def test_schedule_post_without_location_loads_as_none(tmp_path):
    # Old queue files predate the location field: they load with no place.
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(path, text="plain", publish_at=1000.0, now=900.0)
    assert post.location is None
    stored = post.to_dict()
    del stored["location"]
    path.write_text(json.dumps({"posts": [stored]}))
    assert load_scheduled_posts(path).posts[0].location is None


def test_schedule_post_rejects_location_on_reply(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    with pytest.raises(ScheduledPostsError, match="top-level"):
        schedule_post(
            path,
            text="reply",
            publish_at=1000.0,
            now=900.0,
            reply_to="b" * 64,
            location=_location(),
        )
    assert load_scheduled_posts(path).posts == ()


def test_schedule_post_rejects_location_on_poll_repost_and_quote(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    target = "d" * 64
    with pytest.raises(ScheduledPostsError, match="cannot carry a location"):
        schedule_post(
            path,
            text="where?",
            publish_at=1000.0,
            now=900.0,
            kind="poll",
            options=["here", "there"],
            location=_location(),
        )
    with pytest.raises(ScheduledPostsError, match="cannot carry a location"):
        schedule_post(
            path, text="", publish_at=1000.0, now=900.0,
            kind="repost", target_post=target, location=_location(),
        )
    with pytest.raises(ScheduledPostsError, match="cannot carry a location"):
        schedule_post(
            path, text="look", publish_at=1000.0, now=900.0,
            kind="quote", target_post=target, location=_location(),
        )
    assert load_scheduled_posts(path).posts == ()


def test_schedule_post_rejects_bad_location(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    bad_locations = [
        {"lat": 91.0, "lon": -83.88},
        {"lat": 34.05, "lon": 181.0},
        {"lat": 34.05},
        {"lat": "34.05", "lon": -83.88},
        {"lat": 34.05, "lon": -83.88, "name": "x" * 201},
        "not-an-object",
    ]
    for bad in bad_locations:
        with pytest.raises(ScheduledPostsError):
            schedule_post(
                path, text="x", publish_at=1000.0, now=900.0, location=bad
            )
    assert load_scheduled_posts(path).posts == ()


def test_schedule_post_rejects_text_that_only_fits_without_location(tmp_path):
    # The pin is ~70 bytes of JSON inside the 8 KiB Tan request:
    # 1,875 four-byte characters fit on their own (the first failure
    # without a location is 1,891) but not with one attached (the first
    # failure with a location is 1,875).  Refuse at schedule time,
    # instead of failing every publish attempt forever.
    path = tmp_path / "scheduled-posts.json"
    schedule_post(path, text="😀" * 1875, publish_at=1000.0, now=900.0)
    with pytest.raises(ScheduledPostsError, match="encoded limit"):
        schedule_post(
            path, text="😀" * 1875, publish_at=1000.0, now=900.0,
            location=_location(),
        )
    assert len(load_scheduled_posts(path).posts) == 1


def _contact() -> dict:
    return {"name": "carol", "destination_hash": "cc" * 16}


def test_schedule_post_with_contact_round_trips(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(
        path, text="morning", publish_at=1000.0, now=900.0, contact=_contact()
    )
    assert post.contact == _contact()
    assert load_scheduled_posts(path).posts[0].contact == _contact()
    assert load_scheduled_posts(path).posts[0].to_dict()["contact"] == _contact()


def test_schedule_post_without_contact_loads_as_none(tmp_path):
    # Old queue files predate the contact field: they load with no card.
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(path, text="plain", publish_at=1000.0, now=900.0)
    assert post.contact is None
    stored = post.to_dict()
    del stored["contact"]
    path.write_text(json.dumps({"posts": [stored]}))
    assert load_scheduled_posts(path).posts[0].contact is None


def test_schedule_post_rejects_contact_on_reply(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    with pytest.raises(ScheduledPostsError, match="top-level"):
        schedule_post(
            path,
            text="reply",
            publish_at=1000.0,
            now=900.0,
            reply_to="b" * 64,
            contact=_contact(),
        )
    assert load_scheduled_posts(path).posts == ()


def test_schedule_post_rejects_contact_on_poll_repost_and_quote(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    target = "d" * 64
    with pytest.raises(ScheduledPostsError, match="cannot carry a contact card"):
        schedule_post(
            path,
            text="where?",
            publish_at=1000.0,
            now=900.0,
            kind="poll",
            options=["here", "there"],
            contact=_contact(),
        )
    with pytest.raises(ScheduledPostsError, match="cannot carry a contact card"):
        schedule_post(
            path, text="", publish_at=1000.0, now=900.0,
            kind="repost", target_post=target, contact=_contact(),
        )
    with pytest.raises(ScheduledPostsError, match="cannot carry a contact card"):
        schedule_post(
            path, text="look", publish_at=1000.0, now=900.0,
            kind="quote", target_post=target, contact=_contact(),
        )
    assert load_scheduled_posts(path).posts == ()


def test_schedule_post_rejects_bad_contact(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    bad_contacts = [
        {"name": "", "destination_hash": "cc" * 16},
        {"name": "carol"},
        {"destination_hash": "cc" * 16},
        {"name": "carol", "destination_hash": "cc" * 15},
        {"name": "carol", "destination_hash": "zz" * 16},
        "not-an-object",
    ]
    for bad in bad_contacts:
        with pytest.raises(ScheduledPostsError):
            schedule_post(
                path, text="x", publish_at=1000.0, now=900.0, contact=bad
            )
    assert load_scheduled_posts(path).posts == ()


def test_schedule_post_rejects_text_that_only_fits_without_contact(tmp_path):
    # The card is ~70 bytes of JSON inside the 8 KiB Tan request:
    # 1,875 four-byte characters fit on their own but not with one
    # attached.  Refuse at schedule time, instead of failing every
    # publish attempt forever.
    path = tmp_path / "scheduled-posts.json"
    schedule_post(path, text="😀" * 1875, publish_at=1000.0, now=900.0)
    with pytest.raises(ScheduledPostsError, match="encoded limit"):
        schedule_post(
            path, text="😀" * 1875, publish_at=1000.0, now=900.0,
            contact=_contact(),
        )
    assert len(load_scheduled_posts(path).posts) == 1


def test_cancel_scheduled_post_end_to_end(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(path, text="later", publish_at=2000.0, now=1000.0)
    assert len(load_scheduled_posts(path)) == 1
    cancelled = cancel_scheduled_post(path, post.id[:8])
    assert cancelled.id == post.id
    assert len(load_scheduled_posts(path)) == 0
    with pytest.raises(ScheduledPostsError):
        cancel_scheduled_post(path, post.id)


def test_edit_scheduled_post_text_and_time(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(
        path, text="later", publish_at=2000.0, now=1000.0, expires_in=3600.0
    )
    edited = edit_scheduled_post(
        path, post.id[:8], text="much later", publish_at=3000.0, now=1000.0
    )
    assert edited.id == post.id
    assert edited.text == "much later"
    assert edited.publish_at == 3000.0
    # Everything else rides along untouched.
    assert edited.created_at == 1000.0
    assert edited.expires_in == 3600.0
    assert edited.reply_to is None
    stored = load_scheduled_posts(path).posts[0]
    assert stored.text == "much later" and stored.publish_at == 3000.0


def test_edit_scheduled_post_text_only_keeps_time(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(path, text="later", publish_at=2000.0, now=1000.0)
    edited = edit_scheduled_post(path, post.id, text="corrected", now=1000.0)
    assert edited.text == "corrected"
    assert edited.publish_at == 2000.0


def test_edit_scheduled_post_time_only_keeps_text(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(path, text="later", publish_at=2000.0, now=1000.0)
    edited = edit_scheduled_post(path, post.id, publish_at=2500.0, now=1000.0)
    assert edited.text == "later"
    assert edited.publish_at == 2500.0


def test_edit_scheduled_post_requires_a_change(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(path, text="later", publish_at=2000.0, now=1000.0)
    with pytest.raises(ScheduledPostsError, match="nothing to change"):
        edit_scheduled_post(path, post.id, now=1000.0)
    assert load_scheduled_posts(path).posts[0].text == "later"


def test_edit_scheduled_post_rejects_unknown_and_ambiguous(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(path, text="later", publish_at=2000.0, now=1000.0)
    with pytest.raises(ScheduledPostsError, match="no scheduled post"):
        edit_scheduled_post(path, "zz" * 8, text="nope", now=1000.0)
    # Two entries sharing a one-character prefix: that prefix is ambiguous.
    save_scheduled_posts(
        path,
        ScheduledPosts(
            posts=(
                _post("aa" + "0" * 14, text="first"),
                _post("ab" + "0" * 14, text="second"),
            )
        ),
    )
    with pytest.raises(ScheduledPostsError, match="ambiguous"):
        edit_scheduled_post(path, "a", text="nope", now=1000.0)
    assert post.id not in {p.id for p in load_scheduled_posts(path).posts}


def test_edit_scheduled_post_refuses_already_created(tmp_path):
    # A publish attempt already stored the signed post: the text is
    # signed, so changing it would orphan the stored copy.  Cancel
    # instead — the entry stays exactly as it was.
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(path, text="later", publish_at=2000.0, now=1000.0)
    queue = load_scheduled_posts(path)
    stored = queue.posts[0]
    save_scheduled_posts(
        path,
        queue.with_replaced(
            ScheduledPost(**{**stored.to_dict(), "post_hash": "a" * 64})
        ),
    )
    with pytest.raises(ScheduledPostsError, match="already created"):
        edit_scheduled_post(path, post.id, text="too late", now=1000.0)
    assert load_scheduled_posts(path).posts[0].text == "later"


def test_edit_scheduled_post_validates_new_text(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(path, text="later", publish_at=2000.0, now=1000.0)
    with pytest.raises(ScheduledPostsError):
        edit_scheduled_post(path, post.id, text="   ", now=1000.0)
    with pytest.raises(ScheduledPostsError, match="encoded limit"):
        edit_scheduled_post(path, post.id, text="😀" * 2000, now=1000.0)
    # Nothing saved by the refused edits.
    assert load_scheduled_posts(path).posts[0].text == "later"


def test_edit_scheduled_post_validates_new_time(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(path, text="later", publish_at=2000.0, now=1000.0)
    with pytest.raises(ScheduledPostsError, match="in the future"):
        edit_scheduled_post(path, post.id, publish_at=1000.0, now=1000.0)
    with pytest.raises(ScheduledPostsError, match="within 7d"):
        edit_scheduled_post(
            path, post.id, publish_at=1000.0 + 7 * 86400 + 1, now=1000.0
        )
    assert load_scheduled_posts(path).posts[0].publish_at == 2000.0


def test_edit_scheduled_post_allows_time_beyond_expiry(tmp_path):
    # Expiry counts from publication, not from scheduling: a post
    # with a one-hour expiry may be moved to publish two hours out.
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(
        path, text="later", publish_at=2000.0, now=1000.0, expires_in=3600.0
    )
    edited = edit_scheduled_post(
        path, post.id, publish_at=1000.0 + 7200.0, now=1000.0
    )
    assert edited.publish_at == 1000.0 + 7200.0


def test_edit_scheduled_post_refuses_text_on_repost(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    post = schedule_repost(
        path, target_post="d" * 64, publish_at=2000.0, now=1000.0
    )
    with pytest.raises(ScheduledPostsError, match="carries no text"):
        edit_scheduled_post(path, post.id, text="sneaky", now=1000.0)
    assert load_scheduled_posts(path).posts[0].text == ""
    # A time-only edit is still fine: the repost itself is unchanged.
    edited = edit_scheduled_post(path, post.id, publish_at=3000.0, now=1000.0)
    assert edited.text == "" and edited.publish_at == 3000.0


def test_edit_scheduled_post_counts_poll_options_in_size_check(tmp_path):
    # 1,860 four-byte characters fit a plain question, but this poll's
    # eight long options ride in the same entry: refuse the edit
    # instead of queueing a poll that could never publish.
    path = tmp_path / "scheduled-posts.json"
    long_options = [chr(ord("a") + i) + "x" * 99 for i in range(8)]
    post = schedule_poll(
        path,
        question="pizza or tacos?",
        options=long_options,
        publish_at=2000.0,
        now=1000.0,
    )
    with pytest.raises(ScheduledPostsError, match="encoded limit"):
        edit_scheduled_post(path, post.id, text="😀" * 1860, now=1000.0)
    assert load_scheduled_posts(path).posts[0].text == "pizza or tacos?"
    # The same question fits a poll with short options.
    short = schedule_poll(
        path, question="pick one?", options=["A", "B"],
        publish_at=2000.0, now=1000.0,
    )
    edited = edit_scheduled_post(path, short.id, text="😀" * 1860, now=1000.0)
    assert edited.text == "😀" * 1860


def test_edit_scheduled_post_counts_quote_target_in_size_check(tmp_path):
    # 1,880 four-byte characters fit a plain post, but a quote carries
    # its target hash in the envelope too: refuse the edit instead of
    # queueing a quote that could never publish.
    path = tmp_path / "scheduled-posts.json"
    post = schedule_quote(
        path, target_post="d" * 64, text="my take",
        publish_at=2000.0, now=1000.0,
    )
    with pytest.raises(ScheduledPostsError, match="encoded limit"):
        edit_scheduled_post(path, post.id, text="😀" * 1880, now=1000.0)
    assert load_scheduled_posts(path).posts[0].text == "my take"
    # The same text fits a plain post: the target hash is what pushes
    # the quote over.
    plain = schedule_post(path, text="short", publish_at=2000.0, now=1000.0)
    edited = edit_scheduled_post(path, plain.id, text="😀" * 1880, now=1000.0)
    assert edited.text == "😀" * 1880


def test_edit_scheduled_post_counts_image_in_size_check(tmp_path):
    # 1,860 four-byte characters fit a plain post, but the image
    # descriptor rides in the same entry: refuse the edit instead of
    # queueing a post that could never publish.
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(
        path, text="morning", publish_at=2000.0, now=1000.0, image=_image()
    )
    with pytest.raises(ScheduledPostsError, match="encoded limit"):
        edit_scheduled_post(path, post.id, text="😀" * 1860, now=1000.0)
    assert load_scheduled_posts(path).posts[0].text == "morning"
    plain = schedule_post(path, text="short", publish_at=2000.0, now=1000.0)
    edited = edit_scheduled_post(path, plain.id, text="😀" * 1860, now=1000.0)
    assert edited.text == "😀" * 1860


def test_edit_scheduled_post_counts_location_in_size_check(tmp_path):
    # 1,875 four-byte characters fit a plain post, but the pin rides
    # in the same entry: refuse the edit instead of queueing a post
    # that could never publish.
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(
        path, text="morning", publish_at=2000.0, now=1000.0, location=_location()
    )
    with pytest.raises(ScheduledPostsError, match="encoded limit"):
        edit_scheduled_post(path, post.id, text="😀" * 1875, now=1000.0)
    assert load_scheduled_posts(path).posts[0].text == "morning"
    plain = schedule_post(path, text="short", publish_at=2000.0, now=1000.0)
    edited = edit_scheduled_post(path, plain.id, text="😀" * 1875, now=1000.0)
    assert edited.text == "😀" * 1875


def test_edit_scheduled_post_counts_contact_in_size_check(tmp_path):
    # 1,875 four-byte characters fit a plain post, but the card rides
    # in the same entry: refuse the edit instead of queueing a post
    # that could never publish.
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(
        path, text="morning", publish_at=2000.0, now=1000.0, contact=_contact()
    )
    with pytest.raises(ScheduledPostsError, match="encoded limit"):
        edit_scheduled_post(path, post.id, text="😀" * 1875, now=1000.0)
    assert load_scheduled_posts(path).posts[0].text == "morning"


def test_edit_scheduled_post_revalidates_time_after_lock(tmp_path, monkeypatch):
    # The requested time was in the future when the edit started, but
    # the clock moved past it while the edit waited for the queue
    # lock: the edit is refused instead of saving a past send time.
    import itertools

    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(path, text="later", publish_at=2000.0, now=1000.0)
    clock = itertools.chain([1000.0], itertools.repeat(2000.0))
    monkeypatch.setattr(time, "time", lambda: next(clock))
    with pytest.raises(ScheduledPostsError, match="in the future"):
        edit_scheduled_post(path, post.id, publish_at=1500.0)
    assert load_scheduled_posts(path).posts[0].publish_at == 2000.0


def test_edit_scheduled_post_clears_retry_state(tmp_path):
    # An entry that exhausted its retries stays excluded from the
    # publish pass — until it is edited, which clears the retry state
    # so it publishes at its new time instead of staying stuck.
    path = tmp_path / "scheduled-posts.json"
    save_scheduled_posts(
        path,
        ScheduledPosts(
            posts=(
                _post(
                    publish_at=2000.0,
                    attempts=MAX_PUBLISH_ATTEMPTS,
                    next_attempt_at=99999.0,
                    paused_at=5000.0,
                ),
            )
        ),
    )
    edited = edit_scheduled_post(
        path, "a" * 16, text="second chance", publish_at=3000.0, now=1000.0
    )
    assert edited.text == "second chance"
    assert edited.publish_at == 3000.0
    assert edited.attempts == 0
    assert edited.next_attempt_at == 0.0
    assert edited.paused_at == 0.0


class _FakeTanApp:
    """TanApp stand-in: records creates and publishes, delivers to nobody."""

    def __init__(
        self, fail_publish: bool = False, fail_repost: bool = False
    ):
        self.created = []
        self.reposted = []
        self.quoted = []
        self.polled = []
        self.published = []
        self.closed = []
        self.fail_publish = fail_publish
        self.fail_repost = fail_repost

    def close_poll(self, target, *, publish=False):
        record = {"hash": "e" * 64, "kind": "close", "target": target}
        self.closed.append(record)
        return record

    def create_post(
        self, text, *, reply_to=None, expires_at=None, image=None, location=None,
        contact=None
    ):
        post = {
            "hash": "c" * 64,
            "text": text,
            "reply_to": reply_to,
            "expires_at": expires_at,
            "image": image,
            "location": location,
            "contact": contact,
        }
        self.created.append(post)
        return post

    def repost(self, target):
        if self.fail_repost:
            raise RuntimeError("post has been deleted")
        record = {"hash": "a" * 64, "kind": "repost", "target": target}
        self.reposted.append(record)
        return record

    def quote(self, target, text, *, image=None):
        record = {
            "hash": "b" * 64,
            "kind": "quote",
            "target": target,
            "text": text,
            "image": image,
        }
        self.quoted.append(record)
        return record

    def create_poll(self, question, options, *, expires_at=None):
        poll = {
            "hash": "d" * 64,
            "text": question,
            "options": list(options),
            "expires_at": expires_at,
        }
        self.polled.append(poll)
        return poll

    def get_post(self, digest):
        for post in [*self.created, *self.reposted, *self.quoted, *self.polled]:
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


def test_publish_due_publishes_poll_through_create_poll(tmp_path):
    # A due poll is signed as a poll, not a post: the question and its
    # fixed options go to create_poll, and the entry leaves the queue.
    path = tmp_path / "scheduled-posts.json"
    due = schedule_poll(
        path,
        question="pizza or tacos?",
        options=["Pizza", "Tacos"],
        publish_at=1000.0,
        to=["alice"],
        now=900.0,
    )
    app = _FakeTanApp()
    resolved = [SimpleNamespace(hexhash="alice-hash")]

    result = publish_due_scheduled_posts(
        path, app, resolve_peer=lambda target: resolved[0], now=2000.0
    )
    assert result == {"published": 1, "kept": 0}
    assert app.created == []
    assert len(app.polled) == 1
    assert app.polled[0]["text"] == "pizza or tacos?"
    assert app.polled[0]["options"] == ["Pizza", "Tacos"]
    assert app.published[0][1] == resolved
    queue = load_scheduled_posts(path)
    assert all(post.id != due.id for post in queue.posts)
    assert len(queue) == 0


def test_publish_due_publishes_scheduled_repost_and_quote(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    repost = schedule_repost(
        path, target_post="d" * 64, publish_at=1000.0, now=900.0
    )
    quote = schedule_quote(
        path, target_post="e" * 64, text="this, tomorrow",
        publish_at=1000.0, now=900.0,
    )
    app = _FakeTanApp()

    result = publish_due_scheduled_posts(path, app, now=2000.0)
    assert result == {"published": 2, "kept": 0}
    assert [record["target"] for record in app.reposted] == ["d" * 64]
    assert [(record["target"], record["text"]) for record in app.quoted] == [
        ("e" * 64, "this, tomorrow")
    ]
    # Both went out through the same publish path as a post.
    assert len(app.published) == 2
    assert app.published[0][0]["hash"] == "a" * 64
    assert app.published[1][0]["hash"] == "b" * 64
    assert load_scheduled_posts(path).posts == ()
    # The stored records are the ones that went out.
    assert app.published[0][0] is app.reposted[0]
    assert app.published[1][0] is app.quoted[0]
    assert repost.id != quote.id


def test_publish_due_publishes_scheduled_quote_with_image(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    quote = schedule_quote(
        path,
        target_post="e" * 64,
        text="look at this",
        publish_at=1000.0,
        now=900.0,
        image=_image(),
    )
    app = _FakeTanApp()

    result = publish_due_scheduled_posts(path, app, now=2000.0)
    assert result == {"published": 1, "kept": 0}
    assert [(record["target"], record["text"], record["image"]) for record in app.quoted] == [
        ("e" * 64, "look at this", _image())
    ]
    assert app.published[0][0] is app.quoted[0]
    assert load_scheduled_posts(path).posts == ()
    assert quote.id is not None


def test_publish_due_keeps_failed_poll_queued(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    schedule_poll(
        path,
        question="pizza or tacos?",
        options=["Pizza", "Tacos"],
        publish_at=1000.0,
        now=900.0,
    )
    app = _FakeTanApp(fail_publish=True)

    result = publish_due_scheduled_posts(path, app, now=2000.0)
    assert result == {"published": 0, "kept": 1}
    assert len(app.polled) == 1
    assert len(load_scheduled_posts(path)) == 1


def test_publish_due_keeps_failed_repost_queued(tmp_path):
    # A repost whose target is gone by publish time (deleted, or
    # already reposted) fails the target checks: it stays queued with
    # backoff like any other failed publish, never dropped silently.
    path = tmp_path / "scheduled-posts.json"
    schedule_repost(path, target_post="d" * 64, publish_at=1000.0, now=900.0)
    app = _FakeTanApp(fail_repost=True)

    result = publish_due_scheduled_posts(path, app, now=2000.0)
    assert result == {"published": 0, "kept": 1}
    queue = load_scheduled_posts(path)
    assert len(queue) == 1
    assert queue.posts[0].attempts == 1
    assert app.published == []


def test_publish_due_repost_retry_resends_same_record(tmp_path):
    # A publish failure after the repost was created must not sign a
    # second repost on retry: the stored record is re-sent instead.
    path = tmp_path / "scheduled-posts.json"
    schedule_repost(path, target_post="d" * 64, publish_at=1000.0, now=900.0)
    app = _FakeTanApp(fail_publish=True)

    result = publish_due_scheduled_posts(path, app, now=2000.0)
    assert result == {"published": 0, "kept": 1}
    assert len(app.reposted) == 1
    app.fail_publish = False
    result = publish_due_scheduled_posts(path, app, now=10000.0)
    assert result == {"published": 1, "kept": 0}
    assert len(app.reposted) == 1
    assert load_scheduled_posts(path).posts == ()


def test_publish_due_attaches_scheduled_image(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    schedule_post(
        path, text="morning", publish_at=1000.0, now=900.0, image=_image()
    )
    app = _FakeTanApp()

    result = publish_due_scheduled_posts(path, app, now=2000.0)
    assert result == {"published": 1, "kept": 0}
    assert app.created[0]["image"] == _image()
    assert app.published[0][0]["image"] == _image()
    assert load_scheduled_posts(path).posts == ()


def test_publish_due_attaches_scheduled_location(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    schedule_post(
        path, text="morning", publish_at=1000.0, now=900.0, location=_location()
    )
    app = _FakeTanApp()

    result = publish_due_scheduled_posts(path, app, now=2000.0)
    assert result == {"published": 1, "kept": 0}
    assert app.created[0]["location"] == _location()
    assert app.published[0][0]["location"] == _location()
    assert load_scheduled_posts(path).posts == ()


def test_publish_due_attaches_scheduled_contact(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    schedule_post(
        path, text="morning", publish_at=1000.0, now=900.0, contact=_contact()
    )
    app = _FakeTanApp()

    result = publish_due_scheduled_posts(path, app, now=2000.0)
    assert result == {"published": 1, "kept": 0}
    assert app.created[0]["contact"] == _contact()
    assert app.published[0][0]["contact"] == _contact()
    assert load_scheduled_posts(path).posts == ()


def test_publish_due_parks_refused_contact(tmp_path):
    # A card whose peer was blocked after scheduling is never
    # published: the entry parks at the attempt cap instead of
    # burning retries, and stays queued for the user to cancel.
    path = tmp_path / "scheduled-posts.json"
    entry = schedule_post(
        path, text="meet carol", publish_at=1000.0, now=900.0, contact=_contact()
    )
    app = _FakeTanApp()
    vetted = []

    def refuse(card):
        vetted.append(card)
        return "cannot share a contact for a blocked peer"

    result = publish_due_scheduled_posts(
        path, app, contact_refusal=refuse, now=2000.0
    )
    assert result == {"published": 0, "kept": 1}
    assert vetted == [_contact()]
    assert app.created == []
    assert app.published == []
    queue = load_scheduled_posts(path)
    assert len(queue) == 1
    assert queue.posts[0].id == entry.id
    assert queue.posts[0].attempts == MAX_PUBLISH_ATTEMPTS
    # A parked entry is never retried: a later pass leaves it alone.
    result = publish_due_scheduled_posts(
        path, app, contact_refusal=refuse, now=3000.0
    )
    assert result == {"published": 0, "kept": 0}
    assert app.created == []


def test_publish_due_vet_refuses_stored_post_on_retry(tmp_path):
    # The vet runs before a stored post is re-published too: a first
    # attempt that fails to deliver, followed by the peer being
    # blocked, must not send the stored copy on the retry.
    path = tmp_path / "scheduled-posts.json"
    schedule_post(
        path, text="meet carol", publish_at=1000.0, now=900.0, contact=_contact()
    )
    app = _FakeTanApp(fail_publish=True)

    def allow(card):
        return None

    result = publish_due_scheduled_posts(
        path, app, contact_refusal=allow, now=2000.0
    )
    assert result == {"published": 0, "kept": 1}
    assert len(app.created) == 1
    assert app.published == []
    queued = load_scheduled_posts(path).posts[0]
    assert queued.post_hash == "c" * 64
    assert queued.attempts == 1

    def refuse(card):
        return "cannot share a contact for a blocked peer"

    result = publish_due_scheduled_posts(
        path, app, contact_refusal=refuse, now=3000.0
    )
    assert result == {"published": 0, "kept": 1}
    assert app.published == []
    queued = load_scheduled_posts(path).posts[0]
    assert queued.post_hash == "c" * 64
    assert queued.attempts == MAX_PUBLISH_ATTEMPTS


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


def test_retry_publishes_canonical_post_without_storage_fields(tmp_path):
    # The real TanApp.get_post returns the canonical post plus the
    # storage-only "seq"/"linked" fields; publish() encodes the dict
    # unchanged, so the retry path must strip them before publishing.
    path = tmp_path / "scheduled-posts.json"
    schedule_post(path, text="now", publish_at=1000.0, now=900.0)

    class _MetaApp(_FakeTanApp):
        def get_post(self, digest):
            stored = super().get_post(digest)
            return {**stored, "seq": 3, "linked": True} if stored else None

    app = _MetaApp(fail_publish=True)
    result = publish_due_scheduled_posts(path, app, now=2000.0)
    assert result == {"published": 0, "kept": 1}

    app.fail_publish = False
    result = publish_due_scheduled_posts(path, app, now=2100.0)
    assert result == {"published": 1, "kept": 0}
    assert "seq" not in app.published[-1][0]
    assert "linked" not in app.published[-1][0]


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


def test_paused_post_yields_to_later_due_posts(tmp_path):
    # A post whose peers keep stalling the pass used to monopolize the
    # queue: every pass started with it, burned its budget, and later
    # due posts never published.  A pause is now remembered on the
    # entry, so the next pass starts with posts that have not paused.
    from tau.config import TauConfig
    from tau.identity import load_identity
    from tau.tangents.tan import TanApp

    data_dir = tmp_path / "node"
    assert _run_cli(data_dir, ["init", "--json"]) == 0
    queue_path = data_dir / "scheduled-posts.json"
    schedule_post(
        queue_path, text="stalled post", publish_at=1000.0,
        to=["alice", "bob", "carol"], now=900.0,
    )
    schedule_post(
        queue_path, text="later post", publish_at=1001.0,
        to=["bob"], now=900.0,
    )
    config = TauConfig.resolve(data_dir=data_dir)
    app = TanApp(config, load_identity(config.identity_path), name=config.name)

    sent = []
    stop = {"set": False}
    calls = {"n": 0}

    class _StallingNode:
        def request(self, peer, path, payload, *, timeout=15.0):
            sent.append(peer.hexhash)
            calls["n"] += 1
            if peer.hexhash == "a" * 64 and calls["n"] == 1:
                stop["set"] = True
                raise RuntimeError("no route")
            return None

    app.node = _StallingNode()
    by_name_or_hash = {
        "alice": SimpleNamespace(hexhash="a" * 64),
        "bob": SimpleNamespace(hexhash="b" * 64),
        "carol": SimpleNamespace(hexhash="c" * 64),
        "a" * 64: SimpleNamespace(hexhash="a" * 64),
        "b" * 64: SimpleNamespace(hexhash="b" * 64),
        "c" * 64: SimpleNamespace(hexhash="c" * 64),
    }
    # Pass 1 pauses on the stalled post; the later post never gets a turn.
    result = publish_due_scheduled_posts(
        queue_path,
        app,
        resolve_peer=lambda name: by_name_or_hash[name],
        should_stop=lambda: stop["set"],
        now=2000.0,
    )
    assert result == {"published": 0, "kept": 2}
    stalled, later = load_scheduled_posts(queue_path).posts
    assert stalled.text == "stalled post"
    assert stalled.attempts == 0
    assert stalled.paused_at == 2000.0
    assert later.paused_at == 0.0

    # Pass 2 starts with the post that has not paused, then retries the
    # stalled one once it has had its turn.
    stop["set"] = False
    result = publish_due_scheduled_posts(
        queue_path,
        app,
        resolve_peer=lambda name: by_name_or_hash[name],
        should_stop=lambda: stop["set"],
        now=2100.0,
    )
    assert result == {"published": 2, "kept": 0}
    assert sent == ["a" * 64, "b" * 64, "b" * 64, "c" * 64, "a" * 64]


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


def test_cli_edit_scheduled(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert _run_cli(data_dir, ["tan", "post", "--delay", "5m", "good morning"]) == 0
    queue = load_scheduled_posts(data_dir / "scheduled-posts.json")
    post_id = queue.posts[0].id

    assert (
        _run_cli(
            data_dir,
            ["tan", "edit-scheduled", post_id[:8], "--text", "good morning!"],
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "edited" in out and "good morning!" in out
    queue = load_scheduled_posts(data_dir / "scheduled-posts.json")
    assert queue.posts[0].text == "good morning!"
    first_publish_at = queue.posts[0].publish_at

    assert (
        _run_cli(data_dir, ["tan", "edit-scheduled", post_id, "--delay", "2h"])
        == 0
    )
    queue = load_scheduled_posts(data_dir / "scheduled-posts.json")
    assert queue.posts[0].text == "good morning!"
    assert queue.posts[0].publish_at > first_publish_at

    # Nothing to change, an unknown id, and an over-cap delay are errors.
    assert _run_cli(data_dir, ["tan", "edit-scheduled", post_id]) == 2
    assert (
        _run_cli(data_dir, ["tan", "edit-scheduled", "zz" * 8, "--text", "nope"])
        == 1
    )
    assert (
        _run_cli(data_dir, ["tan", "edit-scheduled", post_id, "--delay", "30d"])
        == 2
    )
    assert len(load_scheduled_posts(data_dir / "scheduled-posts.json")) == 1


def test_cli_schedule_poll_list_cancel(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert (
        _run_cli(
            data_dir,
            [
                "tan", "poll", "--delay", "2h",
                "pizza or tacos?", "--option", "Pizza", "--option", "Tacos",
            ],
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "scheduled" in out and "asks in 2h" in out

    assert _run_cli(data_dir, ["tan", "scheduled"]) == 0
    listed = capsys.readouterr().out
    assert "pizza or tacos?" in listed
    assert "poll · 2 options" in listed
    assert "1. Pizza" in listed
    assert "2. Tacos" in listed

    queue = load_scheduled_posts(data_dir / "scheduled-posts.json")
    assert len(queue) == 1
    assert queue.posts[0].kind == "poll"
    assert queue.posts[0].options == ("Pizza", "Tacos")
    poll_id = queue.posts[0].id
    assert _run_cli(data_dir, ["tan", "cancel-scheduled", poll_id[:8]]) == 0
    assert len(load_scheduled_posts(data_dir / "scheduled-posts.json")) == 0


def test_cli_schedule_poll_rejects_bad_delay_and_no_publish(tmp_path):
    data_dir = tmp_path / "node"
    argv = ["tan", "poll", "pick one?", "--option", "A", "--option", "B"]
    assert _run_cli(data_dir, [*argv, "--delay", "soon"]) == 2
    assert _run_cli(data_dir, [*argv, "--delay", "2h", "--no-publish"]) == 2
    assert _run_cli(data_dir, [*argv, "--delay", "30d"]) == 2
    assert (
        _run_cli(data_dir, [*argv, "--delay", "2h", "--expire", "1h"]) == 2
    )
    assert len(load_scheduled_posts(data_dir / "scheduled-posts.json")) == 0


def test_cli_schedule_poll_json_output(tmp_path, capsys):
    data_dir = tmp_path / "node"
    assert (
        _run_cli(
            data_dir,
            [
                "tan", "poll", "--delay", "5m", "--json",
                "pick one?", "--option", "A", "--option", "B",
            ],
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["scheduled"]["kind"] == "poll"
    assert payload["scheduled"]["text"] == "pick one?"
    assert payload["scheduled"]["options"] == ["A", "B"]


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
    assert "No Tan posts, reposts, quotes or polls scheduled." in capsys.readouterr().out
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


def _seed_post(data_dir: Path) -> str:
    """Store one post from another author; returns its hash."""
    import RNS

    from tau.config import TauConfig
    from tau.tangents.tan import create_post, init_db

    config = TauConfig.resolve(data_dir=data_dir)
    conn = init_db(config.social_db_path)
    try:
        post = create_post(conn, RNS.Identity(), "worth resharing", name="alice", now=1000)
    finally:
        conn.close()
    return post["hash"]


def test_cli_schedule_repost_and_quote(tmp_path, capsys):
    data_dir = tmp_path / "node"
    target = _seed_post(data_dir)
    assert (
        _run_cli(data_dir, ["tan", "repost", target[:12], "--delay", "5m"]) == 0
    )
    out = capsys.readouterr().out
    assert "scheduled" in out and "reposts" in out and "in 5m" in out
    assert (
        _run_cli(
            data_dir,
            ["tan", "quote", target[:12], "my take", "--delay", "5m"],
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "scheduled" in out and "quotes" in out and "in 5m" in out

    assert _run_cli(data_dir, ["tan", "scheduled"]) == 0
    listed = capsys.readouterr().out
    assert "repost of" in listed
    assert "quote of" in listed
    assert "my take" in listed

    queue = load_scheduled_posts(data_dir / "scheduled-posts.json")
    assert len(queue) == 2
    assert [post.kind for post in queue.posts] == ["repost", "quote"]
    assert all(post.target_post == target for post in queue.posts)


def test_cli_schedule_repost_and_quote_reject_bad_delay(tmp_path):
    data_dir = tmp_path / "node"
    target = _seed_post(data_dir)
    assert _run_cli(data_dir, ["tan", "repost", target[:12], "--delay", "soon"]) == 2
    assert (
        _run_cli(
            data_dir,
            ["tan", "repost", target[:12], "--delay", "5m", "--no-publish"],
        )
        == 2
    )
    assert (
        _run_cli(
            data_dir,
            ["tan", "quote", target[:12], "x", "--delay", "5m", "--no-publish"],
        )
        == 2
    )
    # An unknown target is refused fast, before anything is queued.
    assert _run_cli(data_dir, ["tan", "repost", "f" * 12, "--delay", "5m"]) == 1
    assert len(load_scheduled_posts(data_dir / "scheduled-posts.json")) == 0


def test_cancel_removes_stored_post_after_failed_publish(tmp_path, capsys):
    # A publish attempt already stored the signed post locally; cancelling
    # must remove that stored post too, not just the queue entry.
    from tau.config import TauConfig
    from tau.identity import load_identity
    from tau.tangents.tan import TanApp, deleted_targets, get_post, init_db

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
        # Cancelling deletes the stored post, and deleting purges its
        # row — the text is gone, not just hidden — while the signed
        # delete entry stays as the tombstone.
        assert get_post(conn, stored_hash) is None
        assert deleted_targets(conn) == set()
    finally:
        conn.close()


def test_cancelled_post_syncs_only_as_deleted(tmp_path, capsys):
    # Cancellation after a publish attempt writes a Tan delete, not a
    # removal: deleting purges the post row from the store, but the
    # sync feed carries the delete entry, so a peer that syncs learns
    # the deletion — and the display feed hides it.
    from tau.config import TauConfig
    from tau.identity import load_identity
    from tau.tangents.tan import TanApp, deleted_targets, feed, get_post, init_db

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
        # The post row is purged, so the sync feed carries only the
        # tombstone — the post itself never lands on a peer.
        assert get_post(conn, stored_hash) is None
        assert deleted_targets(conn) == set()
        synced = feed(conn)
        assert all(p["hash"] != stored_hash for p in synced)
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


# -- disappearing scheduled posts ---------------------------------------------


def test_schedule_post_with_expiry_round_trips(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    post = schedule_post(
        path, text="later, briefly", publish_at=2000.0, now=1000.0, expires_in=3600.0
    )
    assert post.expires_in == 3600.0
    reloaded = load_scheduled_posts(path).posts[0]
    assert reloaded.expires_in == 3600.0
    assert reloaded.to_dict()["expires_in"] == 3600.0


def test_schedule_post_rejects_expiry_over_cap(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    with pytest.raises(ScheduledPostsError, match="at most 7d"):
        schedule_post(
            path, text="too long", publish_at=2000.0, now=1000.0,
            expires_in=8 * 24 * 3600.0,
        )
    with pytest.raises(ScheduledPostsError):
        schedule_post(
            path, text="negative", publish_at=2000.0, now=1000.0, expires_in=-1.0
        )
    assert len(load_scheduled_posts(path)) == 0


def test_publish_due_applies_expiry_at_publish_time(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    schedule_post(
        path, text="brief", publish_at=1000.0, now=900.0, expires_in=3600.0
    )
    app = _FakeTanApp()
    result = publish_due_scheduled_posts(path, app, now=2000.0)
    assert result == {"published": 1, "kept": 0}
    assert len(app.created) == 1
    # The clock starts when the post is published, not when it was queued.
    assert app.created[0]["expires_at"] == 2000.0 + 3600.0
    assert app.published[-1][0]["expires_at"] == 2000.0 + 3600.0


def test_publish_due_retry_keeps_the_same_expiry(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    schedule_post(
        path, text="brief", publish_at=1000.0, now=900.0, expires_in=3600.0
    )
    app = _FakeTanApp(fail_publish=True)
    result = publish_due_scheduled_posts(path, app, now=2000.0)
    assert result == {"published": 0, "kept": 1}

    app.fail_publish = False
    result = publish_due_scheduled_posts(path, app, now=2100.0)
    assert result == {"published": 1, "kept": 0}
    # The retry re-sends the post signed on the first attempt, so the
    # expiry does not slide forward with the retry.
    assert app.published[-1][0]["expires_at"] == 2000.0 + 3600.0


def test_publish_due_without_expiry_passes_none(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    schedule_post(path, text="forever", publish_at=1000.0, now=900.0)
    app = _FakeTanApp()
    result = publish_due_scheduled_posts(path, app, now=2000.0)
    assert result == {"published": 1, "kept": 0}
    assert app.created[0]["expires_at"] is None


def test_schedule_poll_close_after_round_trip(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    poll = schedule_poll(
        path,
        question="pizza or tacos?",
        options=["Pizza", "Tacos"],
        publish_at=2000.0,
        now=1000.0,
        expires_in=7200.0,
        close_after=3600.0,
    )
    assert poll.close_after == 3600.0
    assert poll.expires_in == 7200.0
    queue = load_scheduled_posts(path)
    assert len(queue) == 1
    assert queue.posts[0] == poll
    assert queue.posts[0].to_dict()["close_after"] == 3600.0
    # Queue files written before close deadlines existed load as
    # never closing, exactly like the other optional fields.
    old = queue.posts[0].to_dict()
    del old["close_after"]
    assert ScheduledPost.from_dict(old).close_after == 0.0


def test_schedule_poll_close_after_refusals(tmp_path):
    path = tmp_path / "scheduled-posts.json"
    with pytest.raises(
        ScheduledPostsError, match="close-after must be at most 7d"
    ):
        schedule_poll(
            path,
            question="pizza or tacos?",
            options=["Pizza", "Tacos"],
            publish_at=2000.0,
            now=1000.0,
            close_after=8 * 24 * 60 * 60,
        )
    with pytest.raises(
        ScheduledPostsError, match="must close before it expires"
    ):
        schedule_poll(
            path,
            question="pizza or tacos?",
            options=["Pizza", "Tacos"],
            publish_at=2000.0,
            now=1000.0,
            expires_in=3600.0,
            close_after=3600.0,
        )
    with pytest.raises(
        ScheduledPostsError, match="only a poll can"
    ):
        schedule_post(
            path,
            text="a plain post",
            publish_at=2000.0,
            now=1000.0,
            close_after=3600.0,
        )
    with pytest.raises(
        ScheduledPostsError, match="only a poll can"
    ):
        schedule_post(
            path,
            text="",
            publish_at=2000.0,
            now=1000.0,
            kind="repost",
            target_post="d" * 64,
            close_after=3600.0,
        )
    assert len(load_scheduled_posts(path)) == 0


def test_publish_due_registers_poll_close(tmp_path):
    from tau.poll_closes import load_poll_closes

    path = tmp_path / "scheduled-posts.json"
    closes_path = tmp_path / "poll-closes.json"
    schedule_poll(
        path,
        question="pizza or tacos?",
        options=["Pizza", "Tacos"],
        publish_at=1000.0,
        now=900.0,
        close_after=3600.0,
    )
    app = _FakeTanApp()

    result = publish_due_scheduled_posts(
        path, app, now=2000.0, poll_close_path=closes_path
    )
    assert result == {"published": 1, "kept": 0}
    # The fake app's poll carries no creation time, so the deadline
    # falls back to this pass: 2000 + 3600.
    assert load_poll_closes(closes_path) == {"d" * 64: 5600.0}
    assert len(load_scheduled_posts(path)) == 0


def test_publish_due_without_close_after_registers_nothing(tmp_path):
    from tau.poll_closes import load_poll_closes

    path = tmp_path / "scheduled-posts.json"
    closes_path = tmp_path / "poll-closes.json"
    schedule_poll(
        path,
        question="pizza or tacos?",
        options=["Pizza", "Tacos"],
        publish_at=1000.0,
        now=900.0,
    )
    app = _FakeTanApp()

    result = publish_due_scheduled_posts(
        path, app, now=2000.0, poll_close_path=closes_path
    )
    assert result == {"published": 1, "kept": 0}
    assert load_poll_closes(closes_path) == {}


def test_publish_due_keeps_poll_queued_when_close_registration_fails(
    tmp_path,
):
    # A close schedule that cannot be recorded keeps the poll queued:
    # the poll never goes out without the close it was promised.  A
    # corrupt close file is local state, not a failed publication, so
    # the entry keeps its publish attempts and retries after a bounded
    # delay instead of burning one.
    from tau.poll_closes import load_poll_closes

    path = tmp_path / "scheduled-posts.json"
    closes_path = tmp_path / "poll-closes.json"
    closes_path.write_text("{not json", encoding="utf-8")
    schedule_poll(
        path,
        question="pizza or tacos?",
        options=["Pizza", "Tacos"],
        publish_at=1000.0,
        now=900.0,
        close_after=3600.0,
    )
    app = _FakeTanApp()

    result = publish_due_scheduled_posts(
        path, app, now=2000.0, poll_close_path=closes_path
    )
    assert result == {"published": 0, "kept": 1}
    assert app.published == []
    queue = load_scheduled_posts(path)
    assert len(queue) == 1
    assert queue.posts[0].attempts == 0
    assert queue.posts[0].next_attempt_at == 2060.0


class _FakeTanAppWithCreatedAt(_FakeTanApp):
    # A poll whose signed creation time is fixed: the close deadline
    # must be computed from it, not from the pass that publishes it.

    def create_poll(self, question, options, *, expires_at=None):
        poll = {
            "hash": "d" * 64,
            "text": question,
            "options": list(options),
            "expires_at": expires_at,
            "created_at": 1500.0,
        }
        self.polled.append(poll)
        return poll


def test_publish_due_registers_the_same_deadline_on_retry(tmp_path):
    from tau.poll_closes import load_poll_closes

    path = tmp_path / "scheduled-posts.json"
    closes_path = tmp_path / "poll-closes.json"
    schedule_poll(
        path,
        question="pizza or tacos?",
        options=["Pizza", "Tacos"],
        publish_at=1000.0,
        now=900.0,
        close_after=3600.0,
    )
    app = _FakeTanAppWithCreatedAt(fail_publish=True)
    result = publish_due_scheduled_posts(
        path, app, now=2000.0, poll_close_path=closes_path
    )
    assert result == {"published": 0, "kept": 1}
    # The deadline was registered when the poll was signed, even
    # though the publish itself failed.
    assert load_poll_closes(closes_path) == {"d" * 64: 5100.0}

    app.fail_publish = False
    result = publish_due_scheduled_posts(
        path, app, now=2100.0, poll_close_path=closes_path
    )
    assert result == {"published": 1, "kept": 0}
    # The retry re-sends the same poll, so the deadline does not
    # slide forward with the retry.
    assert load_poll_closes(closes_path) == {"d" * 64: 5100.0}


def test_publish_due_closes_poll_whose_deadline_passed_while_queued(
    tmp_path,
):
    # A retry inside the final second before expiry: the deadline
    # already passed while the poll was queued, so pushing it one
    # second out would land after expiry and the sweep would drop
    # it.  The publish pass closes the poll itself, right after
    # the publish, and leaves no schedule behind.
    from tau.poll_closes import load_poll_closes

    path = tmp_path / "scheduled-posts.json"
    closes_path = tmp_path / "poll-closes.json"
    schedule_poll(
        path,
        question="pizza or tacos?",
        options=["Pizza", "Tacos"],
        publish_at=1000.0,
        now=900.0,
        close_after=3600.0,
        expires_in=3601.0,
    )
    app = _FakeTanAppWithCreatedAt(fail_publish=True)
    result = publish_due_scheduled_posts(
        path, app, now=2000.0, poll_close_path=closes_path
    )
    assert result == {"published": 0, "kept": 1}
    # The deadline was registered when the poll was signed, even
    # though the publish itself failed.
    assert load_poll_closes(closes_path) == {"d" * 64: 5100.0}

    app.fail_publish = False
    result = publish_due_scheduled_posts(
        path, app, now=5100.5, poll_close_path=closes_path
    )
    assert result == {"published": 1, "kept": 0}
    # Closed in the same pass, not scheduled: the poll expires at
    # 5101, so a deadline one second out would have been dropped.
    assert [entry["target"] for entry in app.closed] == ["d" * 64]
    assert load_poll_closes(closes_path) == {}
