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
        assert result["published"] == {"delivered": 0, "peers": 0, "errors": []}
        with pytest.raises(NodeError, match="already running"):
            TauNode(config).start()
        # Invalid targets must fail before signing/storing a post.
        assert cli.main(["tan", "post", "bad target", "--to", "nobody", "--data-dir", str(tmp_path)]) == 1
    finally:
        server.stop()
        lock.release()
    assert not server.path.exists()
