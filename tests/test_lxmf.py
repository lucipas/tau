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
        self.display_name = None
        self.delivery_callback = None
        self.call_order: list = []
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

    def register_delivery_callback(self, callback):
        self.delivery_callback = callback
        self.call_order.append("callback")

    def announce(self, destination_hash):
        self.announced.append(destination_hash)
        self.call_order.append("announce")

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


def test_start_router_registers_callback_before_announce(
    fake_lxmf, tmp_path
):
    # A callback registered only after the announce would miss mail
    # sent in response to it; the router drops messages it has no
    # callback for.
    received = []
    router = lxmf.start_lxmf_router(
        RNS.Identity(), tmp_path, delivery_callback=received.append
    )
    assert router.delivery_callback == received.append
    assert router.call_order == ["callback", "announce"]


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


# -- incoming LXMF delivery -------------------------------------------------

class _FakeLXMessage:
    """Stand-in for an unpacked LXMF.LXMessage."""

    def __init__(
        self,
        sender,
        *,
        content=b"hello over lxmf",
        timestamp=1234.5,
        signature_validated=True,
        message_hash=None,
    ) -> None:
        self.source_hash = lxmf.lxmf_delivery_hash(sender)
        self.content = content
        self.timestamp = timestamp
        self.signature_validated = signature_validated
        self.hash = (
            message_hash if message_hash is not None else bytes(32)
        )


def _node_with_sender(tmp_path, *, on_message=None):
    """A node whose registry knows one sender, ``alice``."""
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    node = TauNode(config, registry=PeerRegistry(config.peers_path),
                   on_message=on_message)
    sender = RNS.Identity()
    node.registry.upsert(
        destination_hash(sender), sender.get_public_key(), name="alice"
    )
    return node, sender


def test_registry_get_by_lxmf(tmp_path):
    node, sender = _node_with_sender(tmp_path)
    delivery = lxmf.lxmf_delivery_hash(sender)
    assert node.registry.get_by_lxmf(delivery).name == "alice"
    assert node.registry.get_by_lxmf(delivery.hex()).name == "alice"
    assert node.registry.get_by_lxmf(delivery.hex().upper()).name == "alice"
    assert node.registry.get_by_lxmf(bytes(16)) is None
    assert node.registry.get_by_lxmf("not a hash") is None
    assert node.registry.get_by_lxmf(None) is None


def test_node_registers_lxmf_delivery_callback(fake_lxmf, tmp_path):
    node, _sender = _node_with_sender(tmp_path)
    node.identity = RNS.Identity()
    node._start_lxmf_router()
    router = node.lxmf_router
    assert router.delivery_callback == node._deliver_lxmf_message
    node.stop()
    assert node.lxmf_router is None


def test_deliver_lxmf_message_stores_in_inbox(tmp_path):
    seen = []
    node, sender = _node_with_sender(tmp_path, on_message=seen.append)
    message = _FakeLXMessage(sender, message_hash=b"\x01" * 32)
    node._deliver_lxmf_message(message)
    entries = node.inbox.list_messages()
    assert len(entries) == 1
    record = entries[0]
    assert record["text"] == "hello over lxmf"
    assert record["from"] == "alice"
    assert record["destination"] == destination_hash(sender).hex()
    assert record["kind"] == "chat"
    assert record["sent_at"] == 1234.5
    assert record["direction"] == "in"
    assert record["id"] == (b"\x01" * 32).hex()
    assert len(record["id"]) <= 64
    assert node.mailbox.is_seen(record["id"])
    # on_message fires with the same message (the stored copy carries
    # the inbox's normalized fields, so compare by id, not by dict).
    assert len(seen) == 1
    assert seen[0]["id"] == record["id"]
    assert seen[0]["text"] == "hello over lxmf"


def test_deliver_lxmf_message_drops_unverified(tmp_path):
    node, sender = _node_with_sender(tmp_path)
    node._deliver_lxmf_message(
        _FakeLXMessage(sender, signature_validated=False)
    )
    assert node.inbox.list_messages() == []


def test_deliver_lxmf_message_drops_unknown_sender(tmp_path):
    node, _sender = _node_with_sender(tmp_path)
    stranger = RNS.Identity()
    node._deliver_lxmf_message(_FakeLXMessage(stranger))
    assert node.inbox.list_messages() == []


def test_deliver_lxmf_message_blocklist_wins(tmp_path):
    from tau.blocklist import BlockList

    node, sender = _node_with_sender(tmp_path)
    node.set_blocklist(
        BlockList(blocked=frozenset({destination_hash(sender).hex()}))
    )
    node._deliver_lxmf_message(_FakeLXMessage(sender))
    assert node.inbox.list_messages() == []


def test_deliver_lxmf_message_allowlist_gate(tmp_path):
    from tau.allowlist import AllowList

    node, sender = _node_with_sender(tmp_path)
    node.set_allowlist(AllowList(enabled=True, allow=frozenset()))
    node._deliver_lxmf_message(_FakeLXMessage(sender))
    assert node.inbox.list_messages() == []
    node.set_allowlist(
        AllowList(
            enabled=True,
            allow=frozenset({destination_hash(sender).hex()}),
        )
    )
    node._deliver_lxmf_message(_FakeLXMessage(sender))
    assert len(node.inbox.list_messages()) == 1


def test_deliver_lxmf_message_drops_bad_content(tmp_path):
    node, sender = _node_with_sender(tmp_path)
    node._deliver_lxmf_message(_FakeLXMessage(sender, content=b"\xff\xfe"))
    node._deliver_lxmf_message(_FakeLXMessage(sender, content=b""))
    node._deliver_lxmf_message(_FakeLXMessage(sender, content=b"   "))
    node._deliver_lxmf_message(_FakeLXMessage(sender, content=None))
    assert node.inbox.list_messages() == []


def test_deliver_lxmf_message_dedupes_redelivery(tmp_path):
    seen = []
    node, sender = _node_with_sender(tmp_path, on_message=seen.append)
    message = _FakeLXMessage(sender, message_hash=b"\x02" * 32)
    node._deliver_lxmf_message(message)
    node._deliver_lxmf_message(message)
    assert len(node.inbox.list_messages()) == 1
    assert len(seen) == 1


def test_deliver_lxmf_message_never_raises(tmp_path):
    node, sender = _node_with_sender(tmp_path)
    # A bare object: every attribute lookup falls back to a drop.
    node._deliver_lxmf_message(object())
    # A message whose content access explodes mid-delivery.  A local
    # subclass, not a property patched onto the shared fake class:
    # patching the class would break every later _FakeLXMessage.
    class _BrokenMessage(_FakeLXMessage):
        @property
        def content(self):
            raise RuntimeError("boom")

        @content.setter
        def content(self, value):
            pass

    node._deliver_lxmf_message(_BrokenMessage(sender))
    assert node.inbox.list_messages() == []


def test_deliver_lxmf_message_normalizes_body(tmp_path):
    from tau.protocol import MAX_MESSAGE_CHARS

    node, sender = _node_with_sender(tmp_path)
    # Terminal escapes are stripped like a tau-protocol DM.
    node._deliver_lxmf_message(
        _FakeLXMessage(sender, content=b"\x1b[31mred\x1b[0m ok")
    )
    assert node.inbox.list_messages()[0]["text"] == "[31mred[0m ok"
    # An overlong body is dropped, not stored.
    node._deliver_lxmf_message(
        _FakeLXMessage(
            sender,
            content=b"x" * (MAX_MESSAGE_CHARS + 1),
            message_hash=b"\x03" * 32,
        )
    )
    assert len(node.inbox.list_messages()) == 1
