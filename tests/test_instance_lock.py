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


def stub_start(monkeypatch) -> None:
    """Replace the Reticulum startup with a no-op for lock tests."""
    monkeypatch.setattr(
        TauNode, "_start", lambda self, *, announce: None
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
    def failing_start(self, *, announce):
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
