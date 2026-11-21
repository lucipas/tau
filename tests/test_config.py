from __future__ import annotations

from pathlib import Path

import pytest

from tau import config
from tau.config import (
    ANNOUNCE_INTERVAL_DEFAULT,
    RNS_STAMPS_POLICY,
    TauConfig,
    announce_interval,
    ensure_rns_stamps_policy,
    parse_host_port,
    render_rns_config,
    set_node_setting,
    tan_sync_depth,
    write_node_metadata,
    write_rns_config,
)


@pytest.mark.parametrize(
    "value,default_host,expected",
    [
        ("127.0.0.1:4343", "127.0.0.1", ("127.0.0.1", 4343)),
        ("4343", "0.0.0.0", ("0.0.0.0", 4343)),
        ("example.com:12", "127.0.0.1", ("example.com", 12)),
        ("[::1]:5000", "127.0.0.1", ("::1", 5000)),
        (":7000", "127.0.0.1", ("127.0.0.1", 7000)),
    ],
)
def test_parse_host_port(value, default_host, expected):
    assert parse_host_port(value, default_host=default_host) == expected


@pytest.mark.parametrize(
    "value",
    ["", "host", "host:notaport", "host:0", "host:70000", "[::1]", "[]:1"],
)
def test_parse_host_port_rejects_invalid_values(value):
    with pytest.raises(ValueError):
        parse_host_port(value)


def test_render_rns_config_with_all_interfaces():
    text = render_rns_config(
        tcp_listen="0.0.0.0:4343",
        tcp_connect="127.0.0.1:4344",
        auto_interface=True,
        loglevel=4,
    )
    assert "[reticulum]" in text
    assert "share_instance = No" in text
    assert "loglevel = 4" in text
    assert "[[AutoInterface]]" in text
    assert "type = TCPServerInterface" in text
    assert "listen_port = 4343" in text
    assert "type = TCPClientInterface" in text
    assert "target_port = 4344" in text


def test_render_rns_config_without_interfaces_is_commented():
    text = render_rns_config(auto_interface=False)
    assert "[[AutoInterface]]\n    type" not in text
    assert "#   type = AutoInterface" in text
    assert "No interfaces are enabled" in text


def test_render_rns_config_includes_zero_cost_stamp_policy():
    # Reticulum 1.6.1 generates proof-of-work destination stamps by
    # default; tau opts out (issue #642).
    text = render_rns_config(auto_interface=False)
    assert "[stamps]" in text
    for key, value in RNS_STAMPS_POLICY.items():
        assert f"{key} = {value}" in text


def test_ensure_rns_stamps_policy_appends_missing_section(tmp_path):
    cfg = tmp_path / "config"
    cfg.write_text("[reticulum]\n  enable_transport = No\n")
    assert ensure_rns_stamps_policy(cfg) is True
    text = cfg.read_text()
    assert "[reticulum]" in text
    assert "enable_transport = No" in text
    assert "[stamps]" in text
    for key, value in RNS_STAMPS_POLICY.items():
        assert f"{key} = {value}" in text
    # Second call is a no-op: the policy is already there.
    assert ensure_rns_stamps_policy(cfg) is False
    assert cfg.read_text() == text


def test_ensure_rns_stamps_policy_fills_missing_keys_only(tmp_path):
    cfg = tmp_path / "config"
    cfg.write_text(
        "[stamps]\n  destination_value = 25\n\n[interfaces]\n  [[AutoInterface]]\n"
    )
    assert ensure_rns_stamps_policy(cfg) is True
    text = cfg.read_text()
    # A value the user set by hand is never overridden.
    assert "destination_value = 25" in text
    assert "destination_value = 0" not in text
    for key in ("destination_cost", "pr_value", "pr_cost", "lr_cost"):
        assert f"{key} = 0" in text
    assert "[[AutoInterface]]" in text


def test_ensure_rns_stamps_policy_recognizes_annotated_header(tmp_path):
    cfg = tmp_path / "config"
    cfg.write_text("[ stamps ]  # zero-cost\n  destination_value = 25\n")
    assert ensure_rns_stamps_policy(cfg) is True
    text = cfg.read_text()
    assert text.count("stamps") == 1
    assert text.count("[") == 1
    assert "destination_value = 25" in text
    assert "destination_value = 0" not in text
    for key in ("destination_cost", "pr_value", "pr_cost", "lr_cost"):
        assert f"{key} = 0" in text


def test_ensure_rns_stamps_policy_preserves_symlink(tmp_path):
    real = tmp_path / "managed.conf"
    real.write_text("[reticulum]\n  enable_transport = No\n")
    link = tmp_path / "config"
    link.symlink_to(real)
    assert ensure_rns_stamps_policy(link) is True
    assert link.is_symlink()
    assert "[stamps]" in real.read_text()


def test_ensure_rns_stamps_policy_missing_file_is_noop(tmp_path):
    assert ensure_rns_stamps_policy(tmp_path / "nope") is False


def test_write_rns_config_respects_existing_file(tmp_path):
    cfg = tmp_path / "reticulum" / "config"
    assert write_rns_config(cfg, auto_interface=False) is True
    first = cfg.read_text()
    assert "No interfaces are enabled" in first

    assert write_rns_config(cfg, tcp_listen="4343", force=False) is False
    assert cfg.read_text() == first

    assert write_rns_config(cfg, tcp_listen="4343", force=True) is True
    assert "TCPServerInterface" in cfg.read_text()


def test_tau_config_resolve_uses_metadata_name(tmp_path, monkeypatch):
    monkeypatch.setenv("TAU_HOME", str(tmp_path / "home"))
    cfg = TauConfig.resolve(data_dir=tmp_path / "data")
    assert cfg.data_dir == tmp_path / "data"
    assert cfg.rns_config_dir == tmp_path / "data" / "reticulum"
    assert cfg.identity_path == tmp_path / "data" / "identity"
    assert cfg.peers_path == tmp_path / "data" / "peers.json"

    write_node_metadata(cfg.data_dir, "  alice  ")
    renamed = TauConfig.resolve(data_dir=tmp_path / "data")
    assert renamed.name == "alice"

    explicit = TauConfig.resolve(data_dir=tmp_path / "data", name="bob")
    assert explicit.name == "bob"


def test_default_data_dir_uses_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("TAU_HOME", str(tmp_path / "custom"))
    assert config.default_data_dir() == tmp_path / "custom"

    monkeypatch.delenv("TAU_HOME")
    assert config.default_data_dir() == Path.home() / ".tau"


def test_tan_sync_depth_defaults_to_two(tmp_path):
    # A fresh data directory has no node.json: the #467 default applies.
    assert tan_sync_depth(tmp_path) == 2
    assert not (tmp_path / "node.json").exists()


@pytest.mark.parametrize("depth", [1, 2])
def test_tan_sync_depth_round_trips(tmp_path, depth):
    set_node_setting(tmp_path, "tan_sync_depth", depth)
    assert tan_sync_depth(tmp_path) == depth


@pytest.mark.parametrize("value", [0, 3, -1, "2", True, None, 1.5])
def test_tan_sync_depth_rejects_bad_values(tmp_path, value):
    # Any value outside 1..2 — including a boolean or a string, which a
    # hand-edited node.json might hold — falls back to the default.
    set_node_setting(tmp_path, "tan_sync_depth", value)
    assert tan_sync_depth(tmp_path) == 2


def test_tan_sync_depth_keeps_other_settings(tmp_path):
    set_node_setting(tmp_path, "send_read_receipts", False)
    set_node_setting(tmp_path, "tan_sync_depth", 1)
    assert tan_sync_depth(tmp_path) == 1
    stored = config.load_node_settings(tmp_path)
    assert stored["send_read_receipts"] is False
    assert stored["tan_sync_depth"] == 1


def test_announce_interval_defaults_to_300(tmp_path):
    # A fresh data directory has no node.json: the #619 default applies.
    assert announce_interval(tmp_path) == 300.0
    assert announce_interval(tmp_path) == ANNOUNCE_INTERVAL_DEFAULT
    assert not (tmp_path / "node.json").exists()


@pytest.mark.parametrize("seconds", [0, 60, 300.0, 3600])
def test_announce_interval_round_trips(tmp_path, seconds):
    set_node_setting(tmp_path, "announce_interval", seconds)
    assert announce_interval(tmp_path) == float(seconds)


@pytest.mark.parametrize(
    "value", [-1, -0.5, "300", True, None, float("nan"), float("inf")]
)
def test_announce_interval_rejects_bad_values(tmp_path, value):
    # Any value outside 0.. — including a boolean, a string, or a
    # non-finite number, which a hand-edited node.json might hold —
    # falls back to the default.
    set_node_setting(tmp_path, "announce_interval", value)
    assert announce_interval(tmp_path) == 300.0


def test_announce_interval_rejects_oversized_values(tmp_path):
    # A finite value above the cap would overflow the wait; a huge
    # hand-edited integer cannot be converted to float at all. Both
    # fall back to the default instead of raising.
    set_node_setting(tmp_path, "announce_interval", config.ANNOUNCE_INTERVAL_MAX * 2)
    assert announce_interval(tmp_path) == 300.0
    (tmp_path / "node.json").write_text('{"announce_interval": ' + "9" * 400 + "}")
    assert announce_interval(tmp_path) == 300.0


def test_announce_interval_keeps_other_settings(tmp_path):
    set_node_setting(tmp_path, "send_read_receipts", False)
    set_node_setting(tmp_path, "announce_interval", 60)
    assert announce_interval(tmp_path) == 60.0
    stored = config.load_node_settings(tmp_path)
    assert stored["send_read_receipts"] is False
    assert stored["announce_interval"] == 60
