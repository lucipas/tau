"""Tests for LXMF addressing (tau/lxmf.py) and its peer/CLI surface."""

from __future__ import annotations

import sys
import threading
import time
import types

import pytest
import RNS

from tau import lxmf
from tau.config import TauConfig
from tau.identity import destination_hash
from tau.node import NodeError, TauNode
from tau.peers import Peer, PeerRegistry


def test_delivery_hash_matches_rns_destination_hash() -> None:
    identity = RNS.Identity()
    assert lxmf.lxmf_delivery_hash(identity) == RNS.Destination.hash(
        identity, "lxmf", "delivery"
    )


def test_delivery_hash_matches_independent_formula() -> None:
    # Recompute the hash from RNS primitives the way Destination.hash
    # does, so the test does not just call the same helper twice: the
    # LXMF delivery destination is ("lxmf", "delivery") hashed with the
    # identity hash, full-hashed and truncated.
    identity = RNS.Identity()
    name_hash = RNS.Identity.full_hash(
        RNS.Destination.expand_name(None, "lxmf", "delivery").encode("utf-8")
    )[: RNS.Identity.NAME_HASH_LENGTH // 8]
    expected = RNS.Identity.full_hash(name_hash + identity.hash)[
        : RNS.Reticulum.TRUNCATED_HASHLENGTH // 8
    ]
    assert lxmf.lxmf_delivery_hash(identity) == expected


def test_delivery_propagation_and_tau_hashes_all_differ() -> None:
    identity = RNS.Identity()
    delivery = lxmf.lxmf_delivery_hash(identity)
    propagation = lxmf.lxmf_propagation_hash(identity)
    assert len(delivery) == 16
    assert delivery != propagation
    assert delivery != destination_hash(identity)
    assert propagation != destination_hash(identity)


def test_delivery_hash_for_key_matches_identity() -> None:
    identity = RNS.Identity()
    public_key = identity.get_public_key()
    assert lxmf.lxmf_delivery_hash_for_key(public_key) == lxmf.lxmf_delivery_hash(
        identity
    )
    assert lxmf.lxmf_propagation_hash_for_key(
        public_key
    ) == lxmf.lxmf_propagation_hash(identity)


def test_delivery_hash_for_key_rejects_bad_keys() -> None:
    with pytest.raises(ValueError):
        lxmf.lxmf_delivery_hash_for_key(bytes(32))
    with pytest.raises(ValueError):
        lxmf.lxmf_delivery_hash_for_key("not bytes")  # type: ignore[arg-type]


def test_delivery_hash_for_key_tolerates_none_return(monkeypatch) -> None:
    # RNS 1.4.1's load_public_key returns None even on success (the
    # True/False return came later). The helper must accept that.
    real_load = RNS.Identity.load_public_key

    def load_without_return(self, pub_bytes):
        real_load(self, pub_bytes)
        return None

    monkeypatch.setattr(RNS.Identity, "load_public_key", load_without_return)
    identity = RNS.Identity()
    assert lxmf.lxmf_delivery_hash_for_key(
        identity.get_public_key()
    ) == lxmf.lxmf_delivery_hash(identity)


def test_delivery_hash_for_key_rejects_failed_load_returning_none(monkeypatch) -> None:
    # 1.4.1 also returns None on failure, so a load that never sets the
    # identity hash must still be rejected.
    def load_noop(self, pub_bytes):
        return None

    monkeypatch.setattr(RNS.Identity, "load_public_key", load_noop)
    with pytest.raises(ValueError):
        lxmf.lxmf_delivery_hash_for_key(RNS.Identity().get_public_key())


def test_peer_lxmf_hash_derived_from_public_key() -> None:
    identity = RNS.Identity()
    peer = Peer(
        destination_hash=destination_hash(identity),
        name="alice",
        public_key=identity.get_public_key(),
        first_seen=100.0,
        last_seen=200.0,
    )
    expected = lxmf.lxmf_delivery_hash(identity).hex()
    assert peer.lxmf_hash == expected
    assert peer.to_dict()["lxmf"] == expected
    assert peer.public_dict(now=200.0)["lxmf"] == expected


def test_peer_lxmf_hash_round_trips_through_dict() -> None:
    identity = RNS.Identity()
    peer = Peer(
        destination_hash=destination_hash(identity),
        name="alice",
        public_key=identity.get_public_key(),
        first_seen=100.0,
        last_seen=200.0,
    )
    restored = Peer.from_dict(peer.to_dict())
    assert restored.lxmf_hash == peer.lxmf_hash


def test_peer_lxmf_hash_none_for_unloadable_key() -> None:
    peer = Peer(
        destination_hash=bytes(16),
        name="broken",
        public_key=bytes(32),
        first_seen=100.0,
        last_seen=200.0,
    )
    assert peer.lxmf_hash is None
    assert peer.to_dict()["lxmf"] is None


def test_peer_table_shows_lxmf_column(capsys) -> None:
    from tau import cli

    identity = RNS.Identity()
    peer = Peer(
        destination_hash=destination_hash(identity),
        name="alice",
        public_key=identity.get_public_key(),
        first_seen=100.0,
        last_seen=200.0,
    )
    cli._print_peer_table([peer], now=200.0)
    out = capsys.readouterr().out
    assert "LXMF" in out
    assert peer.lxmf_hash in out


# -- router lifecycle ------------------------------------------------------

class _FakeDeliveryDestination:
    def __init__(self, destination_hash: bytes) -> None:
        self.hash = destination_hash


class _FakeLXMRouter:
    """Stand-in for LXMF.LXMRouter: records construction and calls.

    It never touches Reticulum, so the router lifecycle can be tested
    without a live node — the real router is exercised by the serve
    integration test instead.
    """

    created: list = []

    def __init__(self, identity=None, storagepath=None, **_kwargs) -> None:
        self.identity = identity
        self.storagepath = storagepath
        self.delivery_destinations = {}
        self.propagation_node = False
        self.propagation_destination = _FakeDeliveryDestination(
            lxmf.lxmf_propagation_hash(identity)
            if identity is not None
            else bytes(16)
        )
        self.propagation_enabled_calls = 0
        self.message_storage_limit_mb = None
        self.display_name = None
        self.announced: list = []
        self.exit_handler_calls = 0
        type(self).created.append(self)

    def register_delivery_identity(self, identity, display_name=None):
        self.display_name = display_name
        destination = _FakeDeliveryDestination(
            lxmf.lxmf_delivery_hash(identity)
        )
        self.delivery_destinations[destination.hash] = destination
        return destination

    def enable_propagation(self):
        self.propagation_enabled_calls += 1
        self.propagation_node = True

    def set_message_storage_limit(
        self, kilobytes=None, megabytes=None, gigabytes=None
    ):
        self.message_storage_limit_mb = megabytes

    def announce(self, destination_hash):
        self.announced.append(destination_hash)

    def exit_handler(self):
        self.exit_handler_calls += 1


@pytest.fixture
def fake_lxmf(monkeypatch):
    """Install a fake LXMF package exposing _FakeLXMRouter."""
    _FakeLXMRouter.created.clear()
    module = types.ModuleType("LXMF")
    module.LXMRouter = _FakeLXMRouter
    monkeypatch.setitem(sys.modules, "LXMF", module)
    return _FakeLXMRouter.created


def test_start_router_registers_delivery_and_announces(fake_lxmf, tmp_path):
    identity = RNS.Identity()
    router = lxmf.start_lxmf_router(
        identity, tmp_path, display_name="tester"
    )
    assert fake_lxmf == [router]
    assert router.identity is identity
    assert router.storagepath == str(tmp_path)
    assert router.display_name == "tester"
    delivery = lxmf.lxmf_delivery_hash(identity)
    assert lxmf.lxmf_router_delivery_hash(router) == delivery
    assert router.announced == [delivery]


def test_start_router_requires_installed_package(monkeypatch, tmp_path):
    monkeypatch.setitem(sys.modules, "LXMF", None)
    with pytest.raises(lxmf.LXMFError, match="not installed"):
        lxmf.start_lxmf_router(RNS.Identity(), tmp_path)


def test_start_router_requires_main_thread(fake_lxmf, tmp_path):
    errors: list = []

    def run():
        try:
            lxmf.start_lxmf_router(RNS.Identity(), tmp_path)
        except lxmf.LXMFError as exc:
            errors.append(str(exc))

    thread = threading.Thread(target=run)
    thread.start()
    thread.join(timeout=10)
    assert errors and "main thread" in errors[0]
    assert fake_lxmf == []


def test_stop_router_calls_exit_handler_and_tolerates_none(fake_lxmf, tmp_path):
    router = lxmf.start_lxmf_router(RNS.Identity(), tmp_path)
    lxmf.stop_lxmf_router(router)
    lxmf.stop_lxmf_router(router)
    assert router.exit_handler_calls == 2
    lxmf.stop_lxmf_router(None)
    assert lxmf.lxmf_router_delivery_hash(None) is None


def test_router_delivery_hash_none_without_registration(fake_lxmf, tmp_path):
    router = _FakeLXMRouter(identity=RNS.Identity(), storagepath=str(tmp_path))
    assert lxmf.lxmf_router_delivery_hash(router) is None


def test_node_lxmf_router_lifecycle(fake_lxmf, tmp_path):
    # The node wiring runs without a live Reticulum instance: the
    # identity is set directly and the router is faked, so no signal
    # handlers or Transport state leak into the unit suite.
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    node = TauNode(config, registry=PeerRegistry(config.peers_path))
    node.identity = RNS.Identity()
    node._start_lxmf_router()
    assert fake_lxmf == [node.lxmf_router]
    assert lxmf.lxmf_router_delivery_hash(node.lxmf_router) == (
        lxmf.lxmf_delivery_hash(node.identity)
    )
    node.stop()
    assert node.lxmf_router is None
    assert fake_lxmf[0].exit_handler_calls == 1


def test_node_lxmf_router_missing_package_is_node_error(monkeypatch, tmp_path):
    monkeypatch.setitem(sys.modules, "LXMF", None)
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    node = TauNode(config, registry=PeerRegistry(config.peers_path))
    node.identity = RNS.Identity()
    with pytest.raises(NodeError, match="not installed"):
        node._start_lxmf_router()
    assert node.lxmf_router is None

# -- propagation node ------------------------------------------------------

def test_start_router_with_propagation_enables_it(fake_lxmf, tmp_path):
    identity = RNS.Identity()
    router = lxmf.start_lxmf_router(
        identity, tmp_path, display_name="tester", propagation=True
    )
    assert router.propagation_enabled_calls == 1
    assert router.propagation_node is True
    # The store is capped before the node advertises itself, so it
    # cannot fill the filesystem under sustained traffic.
    assert router.message_storage_limit_mb == 256
    assert lxmf.lxmf_router_propagation_hash(router) == (
        lxmf.lxmf_propagation_hash(identity)
    )
    # The delivery destination is still registered and announced.
    assert lxmf.lxmf_router_delivery_hash(router) == (
        lxmf.lxmf_delivery_hash(identity)
    )
    assert router.announced == [lxmf.lxmf_delivery_hash(identity)]


def test_start_router_without_propagation_leaves_it_off(fake_lxmf, tmp_path):
    identity = RNS.Identity()
    router = lxmf.start_lxmf_router(identity, tmp_path)
    assert router.propagation_enabled_calls == 0
    assert router.propagation_node is False
    assert lxmf.lxmf_router_propagation_hash(router) is None


def test_router_propagation_hash_none_without_router_or_flag(
    fake_lxmf, tmp_path
):
    assert lxmf.lxmf_router_propagation_hash(None) is None
    router = _FakeLXMRouter(identity=RNS.Identity(), storagepath=str(tmp_path))
    assert lxmf.lxmf_router_propagation_hash(router) is None


def test_start_router_propagation_failure_tears_down_router(
    fake_lxmf, tmp_path, monkeypatch
):
    def enable_broken(self):
        raise RuntimeError("no message store")

    monkeypatch.setattr(_FakeLXMRouter, "enable_propagation", enable_broken)
    with pytest.raises(lxmf.LXMFError, match="could not enable LXMF propagation"):
        lxmf.start_lxmf_router(RNS.Identity(), tmp_path, propagation=True)
    # The half-started router is torn down, not left running.
    assert fake_lxmf[0].exit_handler_calls == 1


def test_node_lxmf_router_propagation(fake_lxmf, tmp_path):
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    node = TauNode(config, registry=PeerRegistry(config.peers_path))
    node.identity = RNS.Identity()
    node._start_lxmf_router(propagation=True)
    assert fake_lxmf == [node.lxmf_router]
    assert lxmf.lxmf_router_propagation_hash(node.lxmf_router) == (
        lxmf.lxmf_propagation_hash(node.identity)
    )
    node.stop()
    assert node.lxmf_router is None


def test_node_lxmf_router_propagation_failure_is_node_error(
    fake_lxmf, tmp_path, monkeypatch
):
    def enable_broken(self):
        raise RuntimeError("no message store")

    monkeypatch.setattr(_FakeLXMRouter, "enable_propagation", enable_broken)
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    node = TauNode(config, registry=PeerRegistry(config.peers_path))
    node.identity = RNS.Identity()
    with pytest.raises(NodeError, match="could not enable LXMF propagation"):
        node._start_lxmf_router(propagation=True)
    assert node.lxmf_router is None


def test_serve_propagation_requires_lxmf(monkeypatch):
    from tau import cli

    args = cli.build_parser().parse_args(
        ["serve", "--lxmf-propagation", "--data-dir", "/tmp/tau-test"]
    )
    with pytest.raises(cli.NodeError, match="requires --lxmf"):
        cli.cmd_serve(args)


def test_serve_parser_accepts_lxmf_propagation():
    from tau import cli

    args = cli.build_parser().parse_args(
        ["serve", "--lxmf", "--lxmf-propagation"]
    )
    assert args.lxmf is True
    assert args.lxmf_propagation is True
    args = cli.build_parser().parse_args(["serve"])
    assert args.lxmf is False
    assert args.lxmf_propagation is False

def test_enable_propagation_caps_message_store(fake_lxmf, tmp_path):
    router = _FakeLXMRouter(identity=RNS.Identity(), storagepath=str(tmp_path))
    lxmf.enable_lxmf_propagation(router, message_storage_limit_mb=64)
    assert router.message_storage_limit_mb == 64
    assert router.propagation_enabled_calls == 1


def test_node_start_rejects_propagation_without_lxmf(tmp_path):
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    node = TauNode(config, registry=PeerRegistry(config.peers_path))
    with pytest.raises(NodeError, match="lxmf_propagation requires lxmf"):
        node.start(lxmf_propagation=True)
    assert node.reticulum is None
    assert node.lxmf_router is None
