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
        self.display_name = None
        self.announced: list = []
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

def _lxmf_node(tmp_path):
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
    node, peer, router = _lxmf_node(tmp_path)
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
    node, peer, router = _lxmf_node(tmp_path)
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
    node, peer, router = _lxmf_node(tmp_path)
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
    node, peer, router = _lxmf_node(tmp_path)
    router.outcome = "fail"
    entry = node.mailbox.enqueue(
        to=peer.destination_hash, text="stays queued", message_id="cc" + "56" * 15
    )
    _failing_request(node, monkeypatch)
    assert node._deliver_entry(peer, entry, timeout=1.0) is False
    assert node.mailbox.get_outbox(entry.id) is not None
    assert len(router.outbound) == 1


def test_deliver_entry_keeps_entry_without_router(tmp_path, monkeypatch, fake_lxmf):
    node, peer, router = _lxmf_node(tmp_path)
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
    node, peer, router = _lxmf_node(tmp_path)
    router.outcome = "deliver"
    entry = node.mailbox.enqueue(
        to=b"\xbb" * 16, text="nobody", message_id="cc" + "58" * 15
    )
    _failing_request(node, monkeypatch)
    assert node._deliver_entry(peer, entry, timeout=1.0) is False
    assert node.mailbox.get_outbox(entry.id) is not None
    assert router.outbound == []
