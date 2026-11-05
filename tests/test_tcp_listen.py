"""Tests for the TCP listen port fail-fast check (issue #16).

A second ``tau serve`` on a different data directory but the same TCP
listen port used to start silently with a dead interface: Reticulum
is configured with ``panic_on_interface_error = No``, so the failed
bind was only a log line.  ``check_tcp_listen_ports`` probes the
configured endpoints before Reticulum starts and fails fast instead.
"""

from __future__ import annotations

import socket

import pytest

from tau.config import (
    TauConfig,
    TcpListenEndpoint,
    render_rns_config,
    tcp_listen_endpoints,
    write_rns_config,
)
from tau.node import NodeError, check_tcp_listen_ports


def write_config(tmp_path, **kwargs) -> TauConfig:
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    config.prepare()
    write_rns_config(config.rns_config_file, **kwargs)
    return config


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def test_endpoints_from_rendered_tcp_listen_config(tmp_path):
    port = free_port()
    config = write_config(tmp_path, tcp_listen=f"127.0.0.1:{port}")
    assert tcp_listen_endpoints(config.rns_config_file) == [
        TcpListenEndpoint("127.0.0.1", port)
    ]


def test_endpoints_bare_port_defaults_to_all_interfaces(tmp_path):
    port = free_port()
    config = write_config(tmp_path, tcp_listen=str(port))
    assert tcp_listen_endpoints(config.rns_config_file) == [
        TcpListenEndpoint("0.0.0.0", port)
    ]


def test_endpoints_empty_without_tcp_server(tmp_path):
    config = write_config(tmp_path)
    assert tcp_listen_endpoints(config.rns_config_file) == []


def test_endpoints_missing_config_file(tmp_path):
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    assert tcp_listen_endpoints(config.rns_config_file) == []


def test_endpoints_skips_disabled_interface(tmp_path):
    port = free_port()
    text = render_rns_config(tcp_listen=f"127.0.0.1:{port}")
    text = text.replace("interface_enabled = True", "interface_enabled = False")
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    config.prepare()
    config.rns_config_file.write_text(text, encoding="utf-8")
    assert tcp_listen_endpoints(config.rns_config_file) == []


def test_endpoints_honours_enabled_key(tmp_path):
    # Reticulum attaches an interface when either interface_enabled or
    # enabled is true, and attaches neither when both are absent.
    port = free_port()
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    config.prepare()
    config.rns_config_file.write_text(
        "[interfaces]\n"
        "  [[TCP Server]]\n"
        "    type = TCPServerInterface\n"
        "    interface_enabled = No\n"
        "    enabled = Yes\n"
        "    listen_ip = 127.0.0.1\n"
        f"    listen_port = {port}\n",
        encoding="utf-8",
    )
    assert tcp_listen_endpoints(config.rns_config_file) == [
        TcpListenEndpoint("127.0.0.1", port)
    ]

    config.rns_config_file.write_text(
        "[interfaces]\n"
        "  [[TCP Server]]\n"
        "    type = TCPServerInterface\n"
        "    listen_ip = 127.0.0.1\n"
        f"    listen_port = {port}\n",
        encoding="utf-8",
    )
    assert tcp_listen_endpoints(config.rns_config_file) == []


def test_endpoints_strips_quoted_values(tmp_path):
    # Reticulum's ConfigObj strips surrounding quotes; a quoted port
    # is a real port and must still be probed.
    port = free_port()
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    config.prepare()
    config.rns_config_file.write_text(
        "[interfaces]\n"
        "  [[TCP Server]]\n"
        "    type = TCPServerInterface\n"
        '    interface_enabled = "True"\n'
        '    listen_ip = "127.0.0.1"\n'
        f'    listen_port = "{port}"\n',
        encoding="utf-8",
    )
    assert tcp_listen_endpoints(config.rns_config_file) == [
        TcpListenEndpoint("127.0.0.1", port)
    ]


def test_endpoints_skips_unusable_port(tmp_path):
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    config.prepare()
    config.rns_config_file.write_text(
        "[interfaces]\n"
        "  [[TCP Server]]\n"
        "    type = TCPServerInterface\n"
        "    interface_enabled = True\n"
        "    listen_ip = 127.0.0.1\n"
        "    listen_port = not-a-port\n",
        encoding="utf-8",
    )
    assert tcp_listen_endpoints(config.rns_config_file) == []


def test_check_passes_when_port_is_free(tmp_path):
    config = write_config(tmp_path, tcp_listen=f"127.0.0.1:{free_port()}")
    check_tcp_listen_ports(config)


def test_check_passes_without_tcp_server(tmp_path):
    config = write_config(tmp_path)
    check_tcp_listen_ports(config)


def test_check_fails_fast_when_port_is_held(tmp_path):
    port = free_port()
    holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    holder.bind(("127.0.0.1", port))
    holder.listen(1)
    try:
        config = write_config(tmp_path, tcp_listen=f"127.0.0.1:{port}")
        with pytest.raises(NodeError) as excinfo:
            check_tcp_listen_ports(config)
        message = str(excinfo.value)
        assert str(port) in message
        assert "already in use" in message
    finally:
        holder.close()


def test_check_names_unbindable_listen_address(tmp_path):
    # This sandbox's resolver answers for .invalid names, so the probe
    # reaches the bind step and fails there; either way the address
    # must be named and the start must fail fast.
    config = write_config(tmp_path, tcp_listen="nonexistent.invalid:4343")
    with pytest.raises(NodeError) as excinfo:
        check_tcp_listen_ports(config)
    assert "nonexistent.invalid:4343" in str(excinfo.value)


def test_check_names_unresolvable_listen_address(tmp_path, monkeypatch):
    def failing_getaddrinfo(*args, **kwargs):
        raise socket.gaierror("Name or service not known")

    monkeypatch.setattr(socket, "getaddrinfo", failing_getaddrinfo)
    config = write_config(tmp_path, tcp_listen="unresolvable.example:4343")
    with pytest.raises(NodeError, match="cannot resolve"):
        check_tcp_listen_ports(config)


def test_check_fails_fast_when_quoted_port_is_held(tmp_path):
    port = free_port()
    holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    holder.bind(("127.0.0.1", port))
    holder.listen(1)
    try:
        config = TauConfig.resolve(data_dir=tmp_path, name="tester")
        config.prepare()
        config.rns_config_file.write_text(
            "[interfaces]\n"
            "  [[TCP Server]]\n"
            "    type = TCPServerInterface\n"
            "    interface_enabled = True\n"
            '    listen_ip = "127.0.0.1"\n'
            f'    listen_port = "{port}"\n',
            encoding="utf-8",
        )
        with pytest.raises(NodeError, match="already in use"):
            check_tcp_listen_ports(config)
    finally:
        holder.close()


def test_check_prefers_ipv6_when_configured(tmp_path):
    # A dual-stack hostname with prefer_ipv6 must probe the IPv6
    # endpoint Reticulum will bind, not the free IPv4 one.
    port = free_port()
    holder = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    holder.bind(("::1", port))
    holder.listen(1)
    try:
        config = TauConfig.resolve(data_dir=tmp_path, name="tester")
        config.prepare()
        config.rns_config_file.write_text(
            "[interfaces]\n"
            "  [[TCP Server]]\n"
            "    type = TCPServerInterface\n"
            "    interface_enabled = True\n"
            "    listen_ip = localhost\n"
            f"    listen_port = {port}\n"
            "    prefer_ipv6 = Yes\n",
            encoding="utf-8",
        )
        with pytest.raises(NodeError, match="already in use"):
            check_tcp_listen_ports(config)
    finally:
        holder.close()


def _non_loopback_ipv4() -> str | None:
    """An IPv4 address on a non-loopback interface, if the host has one."""
    from RNS.Interfaces import netinfo

    for _, name in socket.if_nameindex():
        if name == "lo":
            continue
        for entry in netinfo.ifaddresses(name).get(netinfo.AF_INET, []):
            return entry["addr"]
    return None


def _write_device_config(tmp_path, port: int) -> TauConfig:
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    config.prepare()
    config.rns_config_file.write_text(
        "[interfaces]\n"
        "  [[TCP Server]]\n"
        "    type = TCPServerInterface\n"
        "    interface_enabled = True\n"
        "    device = lo\n"
        f"    listen_port = {port}\n",
        encoding="utf-8",
    )
    return config


def test_check_probes_device_address_not_wildcard(tmp_path):
    # A device-bound server binds loopback; a listener on the same
    # port on another interface must not trip the check.
    other = _non_loopback_ipv4()
    if other is None:
        pytest.skip("no non-loopback interface to hold the port on")
    port = free_port()
    holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    holder.bind((other, port))
    holder.listen(1)
    try:
        config = _write_device_config(tmp_path, port)
        check_tcp_listen_ports(config)
    finally:
        holder.close()


def test_check_fails_fast_when_device_port_is_held(tmp_path):
    port = free_port()
    holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    holder.bind(("127.0.0.1", port))
    holder.listen(1)
    try:
        config = _write_device_config(tmp_path, port)
        with pytest.raises(NodeError, match="already in use"):
            check_tcp_listen_ports(config)
    finally:
        holder.close()
