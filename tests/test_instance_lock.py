"""Tests for the single-instance data-directory lock."""

from __future__ import annotations

import os

import pytest

from tau.config import NODE_LOCK_FILE_NAME, TauConfig
from tau.node import InstanceLock, NodeError, TauNode
from tau.peers import PeerRegistry


def make_lock(tmp_path) -> InstanceLock:
    return InstanceLock(tmp_path / NODE_LOCK_FILE_NAME, tmp_path)


def test_second_acquire_on_same_dir_fails_fast(tmp_path):
    first = make_lock(tmp_path)
    first.acquire()
    try:
        second = make_lock(tmp_path)
        with pytest.raises(NodeError) as excinfo:
            second.acquire()
        message = str(excinfo.value)
        assert "already serving" in message
        assert str(tmp_path) in message
        # The holder records its PID in the lock file for the message.
        assert str(os.getpid()) in message
    finally:
        first.release()


def test_lock_file_records_holder_pid(tmp_path):
    lock = make_lock(tmp_path)
    lock.acquire()
    try:
        text = (tmp_path / NODE_LOCK_FILE_NAME).read_text(encoding="utf-8")
        assert text.strip() == str(os.getpid())
    finally:
        lock.release()


def test_acquire_after_release(tmp_path):
    first = make_lock(tmp_path)
    first.acquire()
    first.release()

    second = make_lock(tmp_path)
    second.acquire()
    second.release()


def test_release_is_idempotent(tmp_path):
    lock = make_lock(tmp_path)
    lock.release()
    lock.acquire()
    lock.release()
    lock.release()


def test_windows_second_acquire_on_same_dir_fails_fast(tmp_path, windows_lock):
    """The Windows (msvcrt) lock must detect a serving node too.

    Without it, `tau send`'s lock probe reads "free" on Windows and
    starts a second Reticulum instance on the serving node's identity.
    """
    first = make_lock(tmp_path)
    first.acquire()
    try:
        second = make_lock(tmp_path)
        with pytest.raises(NodeError) as excinfo:
            second.acquire()
        message = str(excinfo.value)
        assert "already serving" in message
        assert str(tmp_path) in message
        assert str(os.getpid()) in message
    finally:
        first.release()


def test_windows_lock_file_records_holder_pid(tmp_path, windows_lock):
    lock = make_lock(tmp_path)
    lock.acquire()
    try:
        text = (tmp_path / NODE_LOCK_FILE_NAME).read_text(encoding="utf-8")
        assert text.strip() == str(os.getpid())
    finally:
        lock.release()


def test_windows_acquire_after_release(tmp_path, windows_lock):
    first = make_lock(tmp_path)
    first.acquire()
    first.release()

    second = make_lock(tmp_path)
    second.acquire()
    second.release()


def test_windows_release_is_idempotent(tmp_path, windows_lock):
    lock = make_lock(tmp_path)
    lock.release()
    lock.acquire()
    lock.release()
    lock.release()


def test_locks_on_different_dirs_do_not_conflict(tmp_path):
    dir_a = tmp_path / "a"
    dir_b = tmp_path / "b"
    dir_a.mkdir()
    dir_b.mkdir()
    lock_a = InstanceLock(dir_a / NODE_LOCK_FILE_NAME, dir_a)
    lock_b = InstanceLock(dir_b / NODE_LOCK_FILE_NAME, dir_b)
    lock_a.acquire()
    try:
        lock_b.acquire()
    finally:
        lock_a.release()
        lock_b.release()


def make_config(tmp_path) -> TauConfig:
    return TauConfig.resolve(data_dir=tmp_path, name="tester")


def test_node_start_fails_fast_when_data_dir_is_locked(tmp_path):
    config = make_config(tmp_path)
    holder = InstanceLock(config.instance_lock_path, config.data_dir)
    config.prepare()
    holder.acquire()
    try:
        node = TauNode(config, registry=PeerRegistry(config.peers_path))
        with pytest.raises(NodeError, match="already serving"):
            node.start(exclusive=True)
        # The failure happens before Reticulum is touched.
        assert not node.started
    finally:
        holder.release()


@pytest.mark.parametrize("failure", [False, True])
def test_workers_finish_before_lock_release(tmp_path, monkeypatch, failure):
    import threading

    config = TauConfig.resolve(data_dir=tmp_path)
    node = TauNode(config)
    finished = []

    def worker():
        node._stop_event.wait()
        # The directory must still be exclusively owned while workers exit.
        with pytest.raises(NodeError):
            make_lock(tmp_path).acquire()
        finished.append(True)

    def start_workers(**kwargs):
        node._announce_thread = threading.Thread(target=worker)
        node._mail_thread = threading.Thread(target=worker)
        node._announce_thread.start()
        node._mail_thread.start()
        if failure:
            raise ValueError("startup failed")

    monkeypatch.setattr(node, "_start", start_workers)
    if failure:
        with pytest.raises(ValueError, match="startup failed"):
            node.start(exclusive=True)
    else:
        node.start(exclusive=True)
        monkeypatch.setattr(node.mailbox, "prune", lambda: len(finished) == 2 or pytest.fail("workers still running"))
        node.stop()
    assert len(finished) == 2
    lock = make_lock(tmp_path)
    lock.acquire()
    lock.release()


def test_stop_workers_handles_current_and_unstarted_threads(tmp_path):
    import threading

    node = TauNode(TauConfig.resolve(data_dir=tmp_path))
    node._announce_thread = threading.current_thread()
    node._mail_thread = threading.Thread(target=lambda: None)
    node._stop_workers()
    assert node._stop_event.is_set()


def stub_start(monkeypatch) -> None:
    """Replace the Reticulum startup with a no-op for lock tests."""
    monkeypatch.setattr(
        TauNode,
        "_start",
        lambda self, *, announce, mail_loop=True, lxmf=False,
        lxmf_propagation=False: None,
    )


def test_exclusive_start_takes_and_stop_releases_the_lock(
    tmp_path, monkeypatch
):
    stub_start(monkeypatch)
    config = make_config(tmp_path)
    node = TauNode(config, registry=PeerRegistry(config.peers_path))
    node.start(exclusive=True)

    contender = make_lock(tmp_path)
    with pytest.raises(NodeError, match="already serving"):
        contender.acquire()

    node.stop()
    contender.acquire()
    contender.release()


def test_exclusive_start_reloads_registry_after_forget(tmp_path, monkeypatch):
    """A peer forgotten before start() takes the lock must not come back.

    The registry is loaded when the node is constructed, before the
    exclusive lock is held. A concurrent `tau peers forget` in that
    window removes the peer from the file; without a reload under the
    lock, the node's next save would restore it.
    """
    stub_start(monkeypatch)
    config = make_config(tmp_path)
    config.prepare()
    registry = PeerRegistry(config.peers_path)
    registry.upsert(b"\xbb" * 16, b"\x02" * 64, name="bob", now=100.0)
    registry.save()

    node = TauNode(config, registry=PeerRegistry(config.peers_path))
    assert node.registry.get("bob") is not None

    # The concurrent forget: a fresh registry removes the peer from
    # the file before start() takes the lock.
    fresh = PeerRegistry(config.peers_path)
    fresh.remove(b"\xbb" * 16)
    fresh.save()

    node.start(exclusive=True)
    assert node.registry.get("bob") is None
    # stop() saves the registry: the forgotten peer must not come back.
    node.stop()
    assert PeerRegistry(config.peers_path).get("bob") is None


def test_plain_start_does_not_take_the_lock(tmp_path, monkeypatch):
    stub_start(monkeypatch)
    config = make_config(tmp_path)
    config.prepare()
    holder = InstanceLock(config.instance_lock_path, config.data_dir)
    holder.acquire()
    try:
        # A transient command must keep working while a serve holds
        # the lock on the same directory.
        node = TauNode(config, registry=PeerRegistry(config.peers_path))
        node.start()
        assert node._instance_lock is None
        node.stop()
    finally:
        holder.release()


def test_failed_exclusive_start_releases_the_lock(tmp_path, monkeypatch):
    def failing_start(self, *, announce, mail_loop=True, lxmf=False,
                      lxmf_propagation=False):
        raise RuntimeError("boom")

    monkeypatch.setattr(TauNode, "_start", failing_start)
    config = make_config(tmp_path)
    node = TauNode(config, registry=PeerRegistry(config.peers_path))
    with pytest.raises(RuntimeError, match="boom"):
        node.start(exclusive=True)

    assert node._instance_lock is None
    contender = make_lock(tmp_path)
    contender.acquire()
    contender.release()
