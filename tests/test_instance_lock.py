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
        assert "already running" in message
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


def test_node_start_fails_fast_when_data_dir_is_locked(tmp_path):
    config = TauConfig.resolve(data_dir=tmp_path, name="tester")
    holder = InstanceLock(config.instance_lock_path, config.data_dir)
    config.prepare()
    holder.acquire()
    try:
        node = TauNode(config, registry=PeerRegistry(config.peers_path))
        with pytest.raises(NodeError, match="already running"):
            node.start()
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
            node.start()
    else:
        node.start()
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
