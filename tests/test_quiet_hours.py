"""Quiet hours: a recurring daily do-not-disturb window."""

from __future__ import annotations

import json
import os
import stat
import time

import pytest
import RNS

from tau import cli
from tau.config import TauConfig
from tau.identity import destination_hash
from tau.inbox import Inbox
from tau.peers import PeerRegistry
from tau.quiet_hours import (
    QuietHours,
    QuietHoursError,
    load_quiet_hours,
    quiet_hours_active,
    save_quiet_hours,
)

ALICE = "aa" * 16


def _epoch_at(hour: int, minute: int) -> float:
    """Epoch for a local wall-clock time today, timezone-independent."""
    now = time.localtime()
    return time.mktime(
        (now.tm_year, now.tm_mon, now.tm_mday, hour, minute, 0, 0, 0, -1)
    )


# -- module ---------------------------------------------------------------

def test_missing_file_means_off(tmp_path):
    quiet_hours = load_quiet_hours(tmp_path / "quiet_hours.json")
    assert quiet_hours.enabled is False
    assert quiet_hours.is_quiet() is False
    assert quiet_hours.minutes_until_end() is None


def test_unreadable_location_raises(tmp_path):
    blocker = tmp_path / "blocker"
    blocker.write_text("x", encoding="utf-8")
    with pytest.raises(QuietHoursError):
        load_quiet_hours(blocker / "quiet_hours.json")


def test_save_load_roundtrip_and_mode(tmp_path):
    path = tmp_path / "quiet_hours.json"
    saved = save_quiet_hours(
        path, QuietHours(enabled=True, start=22 * 60, end=7 * 60)
    )
    assert saved == path
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    loaded = load_quiet_hours(path)
    assert loaded.enabled is True
    assert loaded.start == 22 * 60
    assert loaded.end == 7 * 60
    assert loaded.to_dict() == {
        "enabled": True,
        "start": "22:00",
        "end": "07:00",
    }


def test_malformed_documents_raise(tmp_path):
    path = tmp_path / "quiet_hours.json"
    for document in (
        {"start": "22:00"},
        {"end": "07:00"},
        {"start": "24:00", "end": "07:00"},
        {"start": "22:60", "end": "07:00"},
        {"start": "22", "end": "07:00"},
        {"start": "22:00:00", "end": "07:00"},
        {"start": 2200, "end": "07:00"},
        {"start": "22:00", "end": "07:00", "enabled": "yes"},
        {"start": "07:00", "end": "07:00"},
        "not-an-object",
    ):
        path.write_text(json.dumps(document), encoding="utf-8")
        with pytest.raises(QuietHoursError):
            load_quiet_hours(path)
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(QuietHoursError):
        load_quiet_hours(path)


def test_fail_open_helper_returns_false_on_corrupt_file(tmp_path, caplog):
    # An unread read must never break over a corrupt window file: the
    # badge lights again, and the warning surfaces it.
    path = tmp_path / "quiet_hours.json"
    path.write_text("{not json", encoding="utf-8")
    assert quiet_hours_active(path) is False
    assert "unusable" in caplog.text


def test_daytime_window_boundaries():
    window = QuietHours(enabled=True, start=9 * 60, end=17 * 60)
    assert window.is_quiet(_epoch_at(8, 59)) is False
    assert window.is_quiet(_epoch_at(9, 0)) is True
    assert window.is_quiet(_epoch_at(12, 30)) is True
    assert window.is_quiet(_epoch_at(16, 59)) is True
    assert window.is_quiet(_epoch_at(17, 0)) is False
    assert window.minutes_until_end(_epoch_at(16, 0)) == 60
    assert window.minutes_until_end(_epoch_at(8, 0)) is None


def test_overnight_window_crosses_midnight():
    window = QuietHours(enabled=True, start=22 * 60, end=7 * 60)
    assert window.is_quiet(_epoch_at(21, 59)) is False
    assert window.is_quiet(_epoch_at(22, 0)) is True
    assert window.is_quiet(_epoch_at(23, 30)) is True
    assert window.is_quiet(_epoch_at(0, 0)) is True
    assert window.is_quiet(_epoch_at(6, 59)) is True
    assert window.is_quiet(_epoch_at(7, 0)) is False
    assert window.is_quiet(_epoch_at(12, 0)) is False
    assert window.minutes_until_end(_epoch_at(23, 0)) == 8 * 60
    assert window.minutes_until_end(_epoch_at(6, 0)) == 60


def test_disabled_window_is_never_quiet():
    window = QuietHours(enabled=False, start=22 * 60, end=7 * 60)
    assert window.is_quiet(_epoch_at(23, 0)) is False
    assert window.minutes_until_end(_epoch_at(23, 0)) is None


def test_equal_start_and_end_rejected():
    with pytest.raises(QuietHoursError):
        QuietHours(enabled=True, start=7 * 60, end=7 * 60)
    with pytest.raises(QuietHoursError):
        QuietHours.from_dict({"start": "07:00", "end": "07:00"})


def test_single_digit_times_accepted_and_normalized(tmp_path):
    loaded = QuietHours.from_dict({"start": "9:00", "end": "7:00"})
    assert loaded.start == 9 * 60
    assert loaded.end == 7 * 60
    assert loaded.to_dict()["start"] == "09:00"


# -- CLI ------------------------------------------------------------------

def _seed_one_peer(tmp_path):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    sender = RNS.Identity()
    sender_hash = destination_hash(sender)
    registry = PeerRegistry(config.peers_path)
    registry.upsert(sender_hash, sender.get_public_key(), name="alice", now=100.0)
    registry.save()
    inbox.append(
        {
            "from": "alice",
            "destination": sender_hash.hex(),
            "text": "first hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "m-alice",
        }
    )
    return data_dir


def _window_covering_now():
    """A two-hour window around the current local time, as HH:MM pair."""
    now = time.localtime()
    current = now.tm_hour * 60 + now.tm_min
    start = (current - 60) % (24 * 60)
    end = (current + 60) % (24 * 60)
    return f"{start // 60:02d}:{start % 60:02d}", f"{end // 60:02d}:{end % 60:02d}"


def test_parser_quiet_hours_arguments():
    parser = cli.build_parser()
    args = parser.parse_args(
        ["quiet-hours", "set", "--start", "22:00", "--end", "07:00"]
    )
    assert args.command == "quiet-hours"
    assert args.quiet_hours_command == "set"
    assert args.start == "22:00"
    assert args.end == "07:00"
    args = parser.parse_args(["quiet-hours", "off"])
    assert args.quiet_hours_command == "off"
    args = parser.parse_args(["quiet-hours", "status"])
    assert args.quiet_hours_command == "status"
    args = parser.parse_args(["quiet-hours"])
    assert args.quiet_hours_command is None


def test_set_off_status_roundtrip(tmp_path, capsys):
    data_dir = tmp_path / "node"

    code = cli.main(["quiet-hours", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No quiet hours set." in capsys.readouterr().out

    code = cli.main(
        [
            "quiet-hours", "set",
            "--data-dir", str(data_dir),
            "--start", "22:00", "--end", "07:00",
        ]
    )
    assert code == 0
    assert "Quiet hours set: 22:00–07:00 every day." in capsys.readouterr().out
    loaded = load_quiet_hours(data_dir / "quiet_hours.json")
    assert loaded.enabled is True
    assert (loaded.start, loaded.end) == (22 * 60, 7 * 60)

    code = cli.main(["quiet-hours", "status", "--data-dir", str(data_dir)])
    assert code == 0
    assert "Quiet hours 22:00–07:00" in capsys.readouterr().out

    code = cli.main(
        ["quiet-hours", "status", "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["enabled"] is True
    assert shown["start"] == "22:00"
    assert shown["end"] == "07:00"
    # quiet_now / quiet_minutes_left are time-dependent; check only
    # that they agree with the stored window at this instant (the
    # minutes-left value may tick over by one between the two reads).
    assert shown["quiet_now"] == loaded.is_quiet()
    expected_left = loaded.minutes_until_end()
    if expected_left is None:
        assert shown["quiet_minutes_left"] is None
    else:
        assert abs(shown["quiet_minutes_left"] - expected_left) <= 1

    code = cli.main(["quiet-hours", "off", "--data-dir", str(data_dir)])
    assert code == 0
    assert "Quiet hours off (window 22:00–07:00 kept)." in capsys.readouterr().out
    loaded = load_quiet_hours(data_dir / "quiet_hours.json")
    assert loaded.enabled is False
    assert (loaded.start, loaded.end) == (22 * 60, 7 * 60)

    code = cli.main(
        ["quiet-hours", "status", "--data-dir", str(data_dir), "--json"]
    )
    assert code == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["enabled"] is False
    assert shown["quiet_now"] is False
    assert shown["quiet_minutes_left"] is None


def test_quiet_hours_rejects_bad_input(tmp_path, capsys):
    data_dir = tmp_path / "node"
    for start, end in (
        ("25:00", "07:00"),
        ("22:00", "07:60"),
        ("22:00", "22:00"),
        ("ten", "07:00"),
    ):
        code = cli.main(
            [
                "quiet-hours", "set",
                "--data-dir", str(data_dir),
                "--start", start, "--end", end,
            ]
        )
        assert code == 1
        assert "error" in capsys.readouterr().err
    assert not (data_dir / "quiet_hours.json").exists()

    code = cli.main(["quiet-hours", "off", "--data-dir", str(data_dir)])
    assert code == 0
    assert "No quiet hours set." in capsys.readouterr().out


# -- unread suppression ---------------------------------------------------

def test_quiet_hours_silence_counts_and_markers(tmp_path, capsys):
    data_dir = _seed_one_peer(tmp_path)
    start, end = _window_covering_now()

    code = cli.main(["messages", "--data-dir", str(data_dir), "--count"])
    assert code == 0
    assert "Total: 1 unread" in capsys.readouterr().out

    code = cli.main(
        [
            "quiet-hours", "set",
            "--data-dir", str(data_dir),
            "--start", start, "--end", end,
        ]
    )
    assert code == 0
    capsys.readouterr()

    # The count reads 0 while the window covers now, and the listing
    # clears the unread marker — the badge and the marker agree.
    code = cli.main(["messages", "--data-dir", str(data_dir), "--count"])
    assert code == 0
    assert "No unread messages." in capsys.readouterr().out

    code = cli.main(["messages", "--data-dir", str(data_dir), "--json"])
    assert code == 0
    shown = json.loads(capsys.readouterr().out)
    assert shown["messages"][0]["unread"] is False

    # The listing above marked the message read, so restore it unread
    # before checking the counts come back: the read cursor is what
    # quiet hours must leave untouched, not the listing.
    code = cli.main(
        ["messages", "unread", "alice", "--all", "--data-dir", str(data_dir)]
    )
    assert code == 0
    capsys.readouterr()

    # Turning quiet hours off restores the count: the read cursor
    # never moved, so nothing was lost.
    code = cli.main(["quiet-hours", "off", "--data-dir", str(data_dir)])
    assert code == 0
    capsys.readouterr()

    code = cli.main(["messages", "--data-dir", str(data_dir), "--count"])
    assert code == 0
    assert "Total: 1 unread" in capsys.readouterr().out


def test_inbox_quiet_flag_clears_counts_and_markers(tmp_path):
    data_dir = tmp_path / "node"
    config = TauConfig.resolve(data_dir=data_dir, name="bob")
    inbox = Inbox(config.inbox_path)
    inbox.append(
        {
            "from": "alice",
            "destination": ALICE,
            "text": "hello",
            "sent_at": 10.0,
            "received_at": 20.0,
            "id": "m-1",
        }
    )
    assert inbox.unread_counts()["total"] == 1
    assert inbox.list_messages()[0]["unread"] is True
    assert inbox.unread_counts(quiet=True) == {
        "per_peer": {},
        "per_group": {},
        "total": 0,
    }
    assert inbox.list_messages(quiet=True)[0]["unread"] is False
    # Quiet is a read-time flag, not stored state: the counts come back.
    assert inbox.unread_counts()["total"] == 1
