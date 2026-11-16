from __future__ import annotations

import json
import os
import socket

import pytest

from tau import cli
from tau.config import TauConfig
from tau.identity import load_or_create_identity
from tau.local_publish import PublishServer
from tau.node import InstanceLock, NodeError, TauNode


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_poll_and_vote_use_lock_owner(tmp_path, capsys):
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    node = TauNode(config)
    node.identity = load_or_create_identity(config.identity_path)
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    server = PublishServer(tmp_path, lambda **request: cli._publish_local_post(node, **request))
    try:
        # A second Reticulum instance must never start while the
        # serving node owns the data directory: the poll is created
        # locally and the running node publishes it instead.
        assert cli.main([
            "tan", "poll", "meet thursday or friday?",
            "--data-dir", str(tmp_path), "--json",
            "--option", "Thursday", "--option", "Friday",
        ]) == 0
        result = json.loads(capsys.readouterr().out)
        assert result["poll"]["kind"] == "poll"
        assert result["poll"]["options"] == ["Thursday", "Friday"]
        assert result["published"] == {
            "delivered": 0,
            "peers": 0,
            "errors": [],
            "unsent": [],
        }
        poll_hash = result["poll"]["hash"]
        assert cli.main([
            "tan", "vote", poll_hash[:16], "Friday",
            "--data-dir", str(tmp_path), "--json",
        ]) == 0
        result = json.loads(capsys.readouterr().out)
        assert result["vote"]["kind"] == "vote"
        assert result["vote"]["text"] == "1"
        assert result["published"] == {
            "delivered": 0,
            "peers": 0,
            "errors": [],
            "unsent": [],
        }
        assert cli.main([
            "tan", "results", poll_hash, "--data-dir", str(tmp_path), "--json",
        ]) == 0
        result = json.loads(capsys.readouterr().out)
        assert result["counts"] == [0, 1]
        assert result["total"] == 1
    finally:
        server.stop()
        lock.release()
    assert not server.path.exists()


@pytest.mark.skipif(not hasattr(socket, "AF_UNIX"), reason="Unix sockets required")
def test_post_uses_lock_owner(tmp_path, capsys):
    config = TauConfig.resolve(data_dir=tmp_path)
    config.prepare()
    node = TauNode(config)
    node.identity = load_or_create_identity(config.identity_path)
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    server = PublishServer(tmp_path, lambda **request: cli._publish_local_post(node, **request))
    try:
        assert os.stat(server.path).st_mode & 0o777 == 0o600
        assert cli.main(["tan", "post", "hello mesh", "--data-dir", str(tmp_path), "--json"]) == 0
        result = json.loads(capsys.readouterr().out)
        assert result["post"]["text"] == "hello mesh"
        assert result["published"] == {
            "delivered": 0,
            "peers": 0,
            "errors": [],
            "unsent": [],
        }
        with pytest.raises(NodeError, match="already serving"):
            TauNode(config).start(exclusive=True)
        # Invalid targets must fail before signing/storing a post.
        assert cli.main(["tan", "post", "bad target", "--to", "nobody", "--data-dir", str(tmp_path)]) == 1
    finally:
        server.stop()
        lock.release()
    assert not server.path.exists()
