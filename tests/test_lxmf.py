"""Tests for LXMF addressing (tau/lxmf.py) and its peer/CLI surface."""

from __future__ import annotations

import sys
import threading
import time
import types

import pytest
import RNS

from tau import lxmf
from tau.allowlist import AllowList
from tau.blocklist import BlockList
from tau.config import TauConfig
from tau.identity import destination_hash
from tau.node import NodeError, NotAllowedError, PeerRefusedError, TauNode
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


class _FakeLXMessage:
    """Stand-in for LXMF.LXMessage: records construction, drives callbacks."""

    def __init__(self, destination, source, content="", title="", fields=None, **_kwargs) -> None:
        self.destination = destination
        self.source = source
        self.content = content
        self.title = title
        self.fields = fields
        self.message_id = None
        self.hash = b"\x00" * 32
        self.packed = False
        self._delivery_callback = None
        self.failed_callback = None

    def get_destination(self):
        return self.destination

    def pack(self):
        self.packed = True
        self.message_id = self.hash

    def register_delivery_callback(self, callback):
        self._delivery_callback = callback

    def register_failed_callback(self, callback):
        self.failed_callback = callback

    def deliver(self):
        if self._delivery_callback is not None:
            self._delivery_callback(self)

    def fail(self):
        if self.failed_callback is not None:
            self.failed_callback(self)


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
        self.delivery_callback = None
        self.exit_handler_calls = 0
        self.outbound: list = []
        self.cancelled: list = []
        self.outcome = None  # "deliver" | "fail" | None (no callback)
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

    def register_delivery_callback(self, callback):
        self.delivery_callback = callback

    def announce(self, destination_hash):
        self.announced.append(destination_hash)

    def handle_outbound(self, message):
        self.outbound.append(message)
        message.pack()
        if self.outcome == "deliver":
            message.deliver()
        elif self.outcome == "fail":
            message.fail()

    def cancel_outbound(self, message_id):
        self.cancelled.append(message_id)

    def exit_handler(self):
        self.exit_handler_calls += 1


@pytest.fixture
def fake_lxmf(monkeypatch):
    """Install a fake LXMF package exposing _FakeLXMRouter."""
    _FakeLXMRouter.created.clear()
    module = types.ModuleType("LXMF")
    module.LXMRouter = _FakeLXMRouter
    module.LXMessage = _FakeLXMessage
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


def test_start_router_registers_delivery_callback(fake_lxmf, tmp_path):
    received: list = []

    def callback(message):
        received.append(message)

    router = lxmf.start_lxmf_router(
        RNS.Identity(), tmp_path, delivery_callback=callback
    )
    assert router.delivery_callback is callback
    router.delivery_callback("message")
    assert received == ["message"]


def test_start_router_without_callback_registers_none(fake_lxmf, tmp_path):
    router = lxmf.start_lxmf_router(RNS.Identity(), tmp_path)
    assert router.delivery_callback is None


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


# -- sending ---------------------------------------------------------------

def test_build_lxmf_message_addresses_peer_delivery_destination(fake_lxmf):
    own = RNS.Identity()
    peer = RNS.Identity()
    message = lxmf.build_lxmf_message(own, peer.get_public_key(), "hello")
    assert message.content == "hello"
    assert message.destination.hash == lxmf.lxmf_delivery_hash(peer)
    assert message.source.hash == lxmf.lxmf_delivery_hash(own)


def test_build_lxmf_message_requires_installed_package(monkeypatch):
    monkeypatch.setitem(sys.modules, "LXMF", None)
    with pytest.raises(lxmf.LXMFError, match="not installed"):
        lxmf.build_lxmf_message(
            RNS.Identity(), RNS.Identity().get_public_key(), "hi"
        )


def test_build_lxmf_message_rejects_bad_key(fake_lxmf):
    with pytest.raises(ValueError):
        lxmf.build_lxmf_message(RNS.Identity(), bytes(32), "hi")


def test_send_lxmf_message_returns_true_on_delivery(fake_lxmf, tmp_path):
    router = _FakeLXMRouter(identity=RNS.Identity(), storagepath=str(tmp_path))
    router.outcome = "deliver"
    message = _FakeLXMessage(None, None, content="hi")
    assert lxmf.send_lxmf_message(router, message, timeout=1.0) is True
    assert router.outbound == [message]
    assert router.cancelled == []


def test_send_lxmf_message_returns_false_on_failure(fake_lxmf, tmp_path):
    router = _FakeLXMRouter(identity=RNS.Identity(), storagepath=str(tmp_path))
    router.outcome = "fail"
    message = _FakeLXMessage(None, None, content="hi")
    assert lxmf.send_lxmf_message(router, message, timeout=1.0) is False
    assert router.cancelled == [message.message_id]


def test_send_lxmf_message_cancels_on_timeout(fake_lxmf, tmp_path):
    router = _FakeLXMRouter(identity=RNS.Identity(), storagepath=str(tmp_path))
    router.outcome = None
    message = _FakeLXMessage(None, None, content="hi")
    assert lxmf.send_lxmf_message(router, message, timeout=0.05) is False
    assert message.message_id is not None
    assert router.cancelled == [message.message_id]


def test_send_lxmf_message_requires_router():
    with pytest.raises(lxmf.LXMFError, match="no LXMF router"):
        lxmf.send_lxmf_message(None, _FakeLXMessage(None, None), timeout=1.0)


def test_send_lxmf_message_raises_when_router_refuses(fake_lxmf, tmp_path):
    class _RefusingRouter(_FakeLXMRouter):
        def handle_outbound(self, message):
            raise IOError("no outbound propagation node")

    router = _RefusingRouter(identity=RNS.Identity(), storagepath=str(tmp_path))
    with pytest.raises(lxmf.LXMFError, match="could not hand message"):
        lxmf.send_lxmf_message(
            router, _FakeLXMessage(None, None), timeout=1.0
        )


# -- outbox fallback -------------------------------------------------------

def _lxmf_send_node(tmp_path):
    """A node with a fake LXMF router attached and one known peer."""
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    node = TauNode(config, registry=PeerRegistry(config.peers_path))
    node.identity = RNS.Identity()
    peer_identity = RNS.Identity()
    peer = Peer(
        destination_hash=destination_hash(peer_identity),
        name="alice",
        public_key=peer_identity.get_public_key(),
        first_seen=1.0,
        last_seen=1.0,
    )
    node.registry.upsert(
        peer.destination_hash, peer.public_key, name="alice", now=1.0
    )
    router = _FakeLXMRouter(identity=node.identity, storagepath=str(tmp_path))
    node.lxmf_router = router
    return node, peer, router


def _failing_request(node, monkeypatch):
    def _boom(*args, **kwargs):
        raise NodeError("no path")

    monkeypatch.setattr(node, "_request", _boom)


def _request_raising(node, monkeypatch, error):
    def _raise(*args, **kwargs):
        raise error

    monkeypatch.setattr(node, "_request", _raise)


def test_deliver_entry_never_routes_a_refusal_over_lxmf(
    tmp_path, monkeypatch, fake_lxmf
):
    # A peer that explicitly refuses the DM — allow-list (NotAllowedError)
    # or block-list ("unavailable" answers as PeerRefusedError) — must
    # never have the refused text routed around the refusal via LXMF.
    node, peer, router = _lxmf_send_node(tmp_path)
    router.outcome = "deliver"
    for message_id, error in [
        ("cc" + "59" * 15, NotAllowedError("peer rejected the request: not allowed")),
        ("cc" + "5a" * 15, PeerRefusedError("peer rejected the request: unavailable")),
    ]:
        entry = node.mailbox.enqueue(
            to=peer.destination_hash, text="refused", message_id=message_id
        )
        _request_raising(node, monkeypatch, error)
        assert node._deliver_entry(peer, entry, timeout=1.0) is False
        assert node.mailbox.get_outbox(entry.id) is not None
    assert router.outbound == []


def test_deliver_via_lxmf_skips_expired_message(tmp_path, fake_lxmf):
    # A deadline that passes while the direct request is in flight must
    # not be sent over LXMF: the LXMF receiver carries no deadline, so
    # the copy would live in the recipient's inbox past its expiry.
    node, peer, router = _lxmf_send_node(tmp_path)
    router.outcome = "deliver"
    entry = node.mailbox.enqueue(
        to=peer.destination_hash,
        text="too late",
        message_id="cc" + "5b" * 15,
        expires_at=time.time() - 1.0,
    )
    assert node._deliver_via_lxmf(entry, timeout=1.0) is False
    assert router.outbound == []

    # Same for a message expiry alone: the queue entry is still live,
    # but the sender-set deadline has passed.
    live = node.mailbox.enqueue(
        to=peer.destination_hash,
        text="also too late",
        message_id="cc" + "5c" * 15,
        expires_at=time.time() + 3600.0,
    )
    live.message_expires_at = time.time() - 1.0
    assert node._deliver_via_lxmf(live, timeout=1.0) is False
    assert router.outbound == []


def test_deliver_entry_falls_back_to_lxmf_when_direct_fails(
    tmp_path, monkeypatch, fake_lxmf
):
    node, peer, router = _lxmf_send_node(tmp_path)
    router.outcome = "deliver"
    entry = node.mailbox.enqueue(
        to=peer.destination_hash, text="via lxmf", message_id="cc" + "55" * 15
    )
    _failing_request(node, monkeypatch)
    assert node._deliver_entry(peer, entry, timeout=1.0) is True
    assert node.mailbox.get_outbox(entry.id) is None
    assert len(router.outbound) == 1
    sent = router.outbound[0]
    assert sent.content == "via lxmf"
    assert sent.destination.hash == lxmf.lxmf_delivery_hash_for_key(
        peer.public_key
    )
    assert sent.source.hash == lxmf.lxmf_delivery_hash(node.identity)


def test_deliver_entry_keeps_entry_when_lxmf_fails(
    tmp_path, monkeypatch, fake_lxmf
):
    node, peer, router = _lxmf_send_node(tmp_path)
    router.outcome = "fail"
    entry = node.mailbox.enqueue(
        to=peer.destination_hash, text="stays queued", message_id="cc" + "56" * 15
    )
    _failing_request(node, monkeypatch)
    assert node._deliver_entry(peer, entry, timeout=1.0) is False
    assert node.mailbox.get_outbox(entry.id) is not None
    assert len(router.outbound) == 1


def test_deliver_entry_keeps_entry_without_router(tmp_path, monkeypatch, fake_lxmf):
    node, peer, router = _lxmf_send_node(tmp_path)
    node.lxmf_router = None
    entry = node.mailbox.enqueue(
        to=peer.destination_hash, text="no router", message_id="cc" + "57" * 15
    )
    _failing_request(node, monkeypatch)
    assert node._deliver_entry(peer, entry, timeout=1.0) is False
    assert node.mailbox.get_outbox(entry.id) is not None
    assert router.outbound == []


def test_deliver_entry_skips_lxmf_for_unknown_recipient(
    tmp_path, monkeypatch, fake_lxmf
):
    node, peer, router = _lxmf_send_node(tmp_path)
    router.outcome = "deliver"
    entry = node.mailbox.enqueue(
        to=b"\xbb" * 16, text="nobody", message_id="cc" + "58" * 15
    )
    _failing_request(node, monkeypatch)
    assert node._deliver_entry(peer, entry, timeout=1.0) is False
    assert node.mailbox.get_outbox(entry.id) is not None
    assert router.outbound == []
# -- incoming LXMF delivery ------------------------------------------------

def _lxmf_node(tmp_path, *, on_message=None):
    """A node with a fake LXMF router running, no live Reticulum."""
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    node = TauNode(
        config,
        registry=PeerRegistry(config.peers_path),
        on_message=on_message,
    )
    node.identity = RNS.Identity()
    node._start_lxmf_router()
    return node


def _lxmf_peer(node, name="alice"):
    """Add a peer to *node*'s registry; return (peer, sender identity)."""
    sender = RNS.Identity()
    peer = node.registry.upsert(
        destination_hash(sender),
        sender.get_public_key(),
        name=name,
    )
    return peer, sender


def _lxmf_message(sender, *, text="hello via lxmf", verified=True):
    """A delivered LXMessage stand-in for *sender*."""
    return types.SimpleNamespace(
        source_hash=lxmf.lxmf_delivery_hash(sender),
        signature_validated=verified,
        content_as_string=lambda: text,
        hash=b"\x11" * 16,
        timestamp=1_700_000_000.0,
    )


def test_node_lxmf_delivery_stores_message(fake_lxmf, tmp_path):
    seen: list = []
    node = _lxmf_node(tmp_path, on_message=seen.append)
    peer, sender = _lxmf_peer(node)
    try:
        router = node.lxmf_router
        assert router.delivery_callback is not None
        router.delivery_callback(_lxmf_message(sender))
        stored = node.inbox.list_messages()
        assert len(stored) == 1
        record = stored[0]
        assert record["text"] == "hello via lxmf"
        assert record["from"] == "alice"
        assert record["destination"] == peer.hexhash
        assert record["direction"] == "in"
        assert record["id"] == "lxmf:" + (b"\x11" * 16).hex()
        assert record["sent_at"] == 1_700_000_000.0
        # on_message gets the same record (before the inbox stamps seq).
        assert len(seen) == 1
        assert seen[0]["id"] == record["id"]
        assert seen[0]["text"] == record["text"]
    finally:
        node.stop()


def test_node_lxmf_delivery_requires_verified_signature(fake_lxmf, tmp_path):
    node = _lxmf_node(tmp_path)
    _peer, sender = _lxmf_peer(node)
    try:
        node.lxmf_router.delivery_callback(
            _lxmf_message(sender, verified=False)
        )
        assert node.inbox.list_messages() == []
    finally:
        node.stop()


def test_node_lxmf_delivery_drops_unknown_sender(fake_lxmf, tmp_path):
    node = _lxmf_node(tmp_path)
    try:
        node.lxmf_router.delivery_callback(_lxmf_message(RNS.Identity()))
        assert node.inbox.list_messages() == []
    finally:
        node.stop()


def test_node_lxmf_delivery_respects_blocklist(fake_lxmf, tmp_path):
    node = _lxmf_node(tmp_path)
    peer, sender = _lxmf_peer(node)
    node._blocklist = BlockList(blocked={peer.hexhash})
    try:
        node.lxmf_router.delivery_callback(_lxmf_message(sender))
        assert node.inbox.list_messages() == []
    finally:
        node.stop()


def test_node_lxmf_delivery_respects_allowlist(fake_lxmf, tmp_path):
    node = _lxmf_node(tmp_path)
    peer, sender = _lxmf_peer(node)
    node._allowlist = AllowList(enabled=True, allow=frozenset())
    try:
        node.lxmf_router.delivery_callback(_lxmf_message(sender))
        assert node.inbox.list_messages() == []
        node._allowlist = AllowList(enabled=True, allow={peer.hexhash})
        node.lxmf_router.delivery_callback(_lxmf_message(sender))
        assert len(node.inbox.list_messages()) == 1
    finally:
        node.stop()


def test_node_lxmf_delivery_drops_unreadable_content(fake_lxmf, tmp_path):
    node = _lxmf_node(tmp_path)
    _peer, sender = _lxmf_peer(node)
    try:
        node.lxmf_router.delivery_callback(_lxmf_message(sender, text=None))
        node.lxmf_router.delivery_callback(_lxmf_message(sender, text=""))
        assert node.inbox.list_messages() == []
    finally:
        node.stop()


def test_node_lxmf_delivery_drops_invalid_utf8(fake_lxmf, tmp_path):
    node = _lxmf_node(tmp_path)
    _peer, sender = _lxmf_peer(node)
    try:
        message = _lxmf_message(sender)

        def _undecodable():
            return b"\xff\xfe".decode("utf-8")  # raises UnicodeDecodeError

        message.content_as_string = _undecodable
        node.lxmf_router.delivery_callback(message)
        assert node.inbox.list_messages() == []
    finally:
        node.stop()


def test_node_lxmf_long_hash_id_fits_wire_limit(fake_lxmf, tmp_path):
    node = _lxmf_node(tmp_path)
    _peer, sender = _lxmf_peer(node)
    try:
        message = _lxmf_message(sender)
        message.hash = b"\xab" * 32  # full-length LXMF hash
        node.lxmf_router.delivery_callback(message)
        stored = node.inbox.list_messages()
        assert len(stored) == 1
        assert len(stored[0]["id"]) <= 64
    finally:
        node.stop()


def test_node_lxmf_delivery_dedupes_redelivery(fake_lxmf, tmp_path):
    node = _lxmf_node(tmp_path)
    _peer, sender = _lxmf_peer(node)
    try:
        message = _lxmf_message(sender)
        node.lxmf_router.delivery_callback(message)
        node.lxmf_router.delivery_callback(message)
        assert len(node.inbox.list_messages()) == 1
    finally:
        node.stop()


def test_node_lxmf_delivery_callback_never_raises(fake_lxmf, tmp_path):
    node = _lxmf_node(tmp_path)
    _peer, sender = _lxmf_peer(node)
    try:
        class _BrokenMessage:
            @property
            def signature_validated(self):
                raise RuntimeError("boom")

        # A broken message object must not take the router down with it.
        node._handle_lxmf_message(_BrokenMessage())
        assert node.inbox.list_messages() == []
    finally:
        node.stop()


def test_node_lxmf_concurrent_duplicates_store_once(fake_lxmf, tmp_path):
    node = _lxmf_node(tmp_path)
    _peer, sender = _lxmf_peer(node)
    try:
        message = _lxmf_message(sender)
        threads = [
            threading.Thread(
                target=node.lxmf_router.delivery_callback, args=(message,)
            )
            for _ in range(8)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        assert len(node.inbox.list_messages()) == 1
    finally:
        node.stop()


def test_node_lxmf_delivery_after_stop_is_dropped(fake_lxmf, tmp_path):
    node = _lxmf_node(tmp_path)
    _peer, sender = _lxmf_peer(node)
    router = node.lxmf_router
    node.stop()
    router.delivery_callback(_lxmf_message(sender))
    assert node.inbox.list_messages() == []


def test_node_lxmf_stop_waits_for_inflight_delivery(
    fake_lxmf, tmp_path, monkeypatch
):
    node = _lxmf_node(tmp_path)
    _peer, sender = _lxmf_peer(node)
    entered = threading.Event()
    release = threading.Event()
    real_append = node.inbox.append

    def _blocking_append(record):
        entered.set()
        assert release.wait(timeout=10)
        return real_append(record)

    monkeypatch.setattr(node.inbox, "append", _blocking_append)
    try:
        delivery = threading.Thread(
            target=node.lxmf_router.delivery_callback,
            args=(_lxmf_message(sender),),
        )
        delivery.start()
        assert entered.wait(timeout=10)
        stopped = threading.Thread(target=node.stop)
        stopped.start()
        # stop() is still waiting for the delivery while it is blocked.
        stopped.join(timeout=1)
        assert stopped.is_alive()
        release.set()
        stopped.join(timeout=10)
        assert not stopped.is_alive()
        delivery.join(timeout=10)
        assert len(node.inbox.list_messages()) == 1
    finally:
        release.set()
        node.stop()
