from __future__ import annotations

from pathlib import Path

import pytest

from tau import config
from tau.config import (
    TauConfig,
    parse_host_port,
    render_rns_config,
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
