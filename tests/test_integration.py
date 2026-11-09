"""End-to-end test with two real Reticulum instances over TCP loopback.

Run with ``TAU_INTEGRATION=1 pytest -m integration``.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tau.lxmf import lxmf_delivery_hash_for_key

if os.environ.get("TAU_INTEGRATION") != "1":
    pytest.skip(
        "set TAU_INTEGRATION=1 to run the Reticulum integration test",
        allow_module_level=True,
    )

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[1]


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def run_tau(*args: str, timeout: float = 60.0) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    env["PYTHONUNBUFFERED"] = "1"
    return subprocess.run(
        [sys.executable, "-m", "tau", *args],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def require_ok(result: subprocess.CompletedProcess) -> str:
    assert result.returncode == 0, (
        f"command failed ({result.returncode})\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    return result.stdout


def test_two_nodes_discover_and_exchange_a_message(tmp_path: Path) -> None:
    port = free_port()
    alice_dir = tmp_path / "alice"
    bob_dir = tmp_path / "bob"

    alice_init = json.loads(
        require_ok(
            run_tau(
                "init",
                "--data-dir",
                str(alice_dir),
                "--name",
                "alice",
                "--tcp-listen",
                f"127.0.0.1:{port}",
                "--no-auto-interface",
                "--json",
            )
        )
    )
    alice_hash = alice_init["destination"]
    assert len(alice_hash) == 32

    require_ok(
        run_tau(
            "init",
            "--data-dir",
            str(bob_dir),
            "--name",
            "bob",
            "--tcp-connect",
            f"127.0.0.1:{port}",
            "--no-auto-interface",
            "--json",
        )
    )

    log_path = tmp_path / "alice.log"
    with log_path.open("w", encoding="utf-8") as log_file:
        server = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tau",
                "serve",
                "--data-dir",
                str(alice_dir),
                "--announce-interval",
                "1",
                "--json",
            ],
            cwd=str(ROOT),
            env={**os.environ, "PYTHONPATH": str(ROOT)},
            stdout=subprocess.DEVNULL,
            stderr=log_file,
            text=True,
        )

    try:
        # Give the TCP server time to come up before the client connects.
        time.sleep(2.0)
        assert server.poll() is None, log_path.read_text()

        discovered = {}
        for _ in range(3):
            result = run_tau(
                "discover",
                "--data-dir",
                str(bob_dir),
                "--duration",
                "6",
                "--json",
                timeout=40.0,
            )
            payload = json.loads(require_ok(result))
            discovered = {
                peer["destination"]: peer for peer in payload.get("peers", [])
            }
            if alice_hash in discovered:
                break

        assert alice_hash in discovered, (
            f"alice was not discovered; alice log:\n{log_path.read_text()}"
        )
        assert discovered[alice_hash]["name"] == "alice"
        # LXMF addressing: a peer discovered over the network carries its
        # LXMF delivery address, derived from its announced public key.
        alice_peer = discovered[alice_hash]
        assert alice_peer["lxmf"] == lxmf_delivery_hash_for_key(
            bytes.fromhex(alice_peer["public_key"])
        ).hex()

        sent = run_tau(
            "send",
            "--data-dir",
            str(bob_dir),
            "--to",
            alice_hash,
            "hello over reticulum",
            "--timeout",
            "20",
            "--discover-timeout",
            "10",
            "--json",
            timeout=60.0,
        )
        response = json.loads(require_ok(sent))
        assert response["ok"] is True
        assert response["echo"] == "hello over reticulum"
        assert response["server"] == "alice"
        assert response["node"] == alice_hash

        ping = json.loads(
            require_ok(
                run_tau(
                    "ping",
                    "--data-dir",
                    str(bob_dir),
                    "--to",
                    alice_hash,
                    "--timeout",
                    "20",
                    "--json",
                    timeout=60.0,
                )
            )
        )
        assert ping["pong"] is True
        assert ping["server"] == "alice"
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)


def start_serve(
    data_dir: Path, stdout_path: Path, stderr_path: Path, *extra: str
):
    env = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONUNBUFFERED": "1"}
    stdout_file = stdout_path.open("w", encoding="utf-8")
    stderr_file = stderr_path.open("w", encoding="utf-8")
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "tau",
            "serve",
            "--data-dir",
            str(data_dir),
            "--announce-interval",
            "1",
            "--json",
            *extra,
        ],
        cwd=str(ROOT),
        env=env,
        stdout=stdout_file,
        stderr=stderr_file,
        text=True,
    )
    return process


def stop_process(process: subprocess.Popen) -> None:
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def wait_for_text(path: Path, needle: str, timeout: float = 120.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists() and needle in path.read_text(encoding="utf-8"):
            return True
        time.sleep(1.0)
    return False


def test_store_and_forward_delivers_when_peer_returns(tmp_path: Path) -> None:
    port = free_port()
    alice_dir = tmp_path / "alice"
    bob_dir = tmp_path / "bob"

    alice_init = json.loads(
        require_ok(
            run_tau(
                "init",
                "--data-dir",
                str(alice_dir),
                "--name",
                "alice",
                "--tcp-listen",
                f"127.0.0.1:{port}",
                "--no-auto-interface",
                "--json",
            )
        )
    )
    alice_hash = alice_init["destination"]
    require_ok(
        run_tau(
            "init",
            "--data-dir",
            str(bob_dir),
            "--name",
            "bob",
            "--tcp-connect",
            f"127.0.0.1:{port}",
            "--no-auto-interface",
            "--json",
        )
    )

    alice_out = tmp_path / "alice.out"
    alice_err = tmp_path / "alice.err"
    alice = start_serve(alice_dir, alice_out, alice_err)
    try:
        time.sleep(2.0)
        assert alice.poll() is None, alice_err.read_text()

        # Bob learns who alice is while she is online.
        discovered = {}
        for _ in range(3):
            result = run_tau(
                "discover",
                "--data-dir",
                str(bob_dir),
                "--duration",
                "6",
                "--json",
                timeout=40.0,
            )
            payload = json.loads(require_ok(result))
            discovered = {
                peer["destination"]: peer for peer in payload.get("peers", [])
            }
            if alice_hash in discovered:
                break
        assert alice_hash in discovered, alice_err.read_text()
    finally:
        stop_process(alice)

    # Alice goes offline. Bob's message must queue, not vanish.
    sent = run_tau(
        "send",
        "--data-dir",
        str(bob_dir),
        "--to",
        alice_hash,
        "while you were away",
        "--timeout",
        "8",
        "--discover-timeout",
        "3",
        "--json",
        timeout=90.0,
    )
    response = json.loads(require_ok(sent))
    assert response["queued"] is True, response

    outbox = json.loads(
        require_ok(
            run_tau("outbox", "--data-dir", str(bob_dir), "--json", timeout=30.0)
        )
    )
    assert outbox["count"] == 1

    # Both come back; bob's node must hand the queued message over.
    alice = start_serve(alice_dir, alice_out, alice_err)
    bob_out = tmp_path / "bob.out"
    bob_err = tmp_path / "bob.err"
    bob = None
    try:
        time.sleep(2.0)
        bob = start_serve(bob_dir, bob_out, bob_err)
        assert wait_for_text(alice_out, "while you were away"), (
            f"alice never received the queued message\n"
            f"alice err:\n{alice_err.read_text()}\n"
            f"bob err:\n{bob_err.read_text()}"
        )
    finally:
        if bob is not None:
            stop_process(bob)
        stop_process(alice)

    outbox = json.loads(
        require_ok(
            run_tau("outbox", "--data-dir", str(bob_dir), "--json", timeout=30.0)
        )
    )
    assert outbox["count"] == 0


def test_outbox_retry_delivers_when_peer_returns(tmp_path: Path) -> None:
    """`tau outbox retry` forces delivery past backoff (issue #65).

    Bob queues a message while alice is offline (the failed send records
    an attempt, so backoff is active), then retries with alice still
    offline — nothing moves — then retries once alice is back, and the
    message is delivered without any serving node on bob's side.
    """
    port = free_port()
    alice_dir = tmp_path / "alice"
    bob_dir = tmp_path / "bob"

    alice_init = init_node(
        tmp_path, "alice", "--tcp-listen", f"127.0.0.1:{port}"
    )
    alice_hash = alice_init["destination"]
    init_node(tmp_path, "bob", "--tcp-connect", f"127.0.0.1:{port}")

    alice_out = tmp_path / "alice.out"
    alice_err = tmp_path / "alice.err"
    alice = start_serve(alice_dir, alice_out, alice_err)
    try:
        time.sleep(2.0)
        assert alice.poll() is None, alice_err.read_text()
        discovered = discover_until(bob_dir, alice_hash)
        assert alice_hash in discovered, alice_err.read_text()
    finally:
        stop_process(alice)

    # Alice goes offline. Bob's message must queue, not vanish.
    sent = run_tau(
        "send",
        "--data-dir",
        str(bob_dir),
        "--to",
        alice_hash,
        "retry me when you are back",
        "--timeout",
        "8",
        "--discover-timeout",
        "3",
        "--json",
        timeout=90.0,
    )
    response = json.loads(require_ok(sent))
    assert response["queued"] is True, response

    def retry_bob() -> dict:
        return json.loads(
            require_ok(
                run_tau(
                    "outbox",
                    "retry",
                    "--data-dir",
                    str(bob_dir),
                    "--json",
                    timeout=90.0,
                )
            )
        )

    # Alice is still offline: the retry attempts delivery (backoff is
    # ignored) but nothing can move.
    summary = retry_bob()
    assert summary["delivered"] == 0, summary
    assert summary["still_queued"] == 1, summary

    # Alice returns; a one-shot retry from bob delivers the message even
    # though backoff from the failed send has not elapsed.
    alice = start_serve(alice_dir, alice_out, alice_err)
    try:
        time.sleep(2.0)
        assert alice.poll() is None, alice_err.read_text()
        summary = retry_bob()
        assert summary["delivered"] == 1, summary
        assert summary["still_queued"] == 0, summary
        assert wait_for_text(alice_out, "retry me when you are back"), (
            f"alice never received the retried message\n"
            f"alice err:\n{alice_err.read_text()}"
        )
    finally:
        stop_process(alice)


def init_node(tmp_path: Path, name: str, *extra: str) -> dict:
    return json.loads(
        require_ok(
            run_tau(
                "init",
                "--data-dir",
                str(tmp_path / name),
                "--name",
                name,
                "--no-auto-interface",
                "--json",
                *extra,
            )
        )
    )


def discover_until(data_dir: Path, wanted_hash: str, attempts: int = 4) -> dict:
    discovered = {}
    for _ in range(attempts):
        result = run_tau(
            "discover",
            "--data-dir",
            str(data_dir),
            "--duration",
            "6",
            "--json",
            timeout=40.0,
        )
        payload = json.loads(require_ok(result))
        discovered = {peer["destination"]: peer for peer in payload.get("peers", [])}
        if wanted_hash in discovered:
            break
    return discovered


def test_relay_delivers_via_a_holding_peer(tmp_path: Path) -> None:
    """Bob -> (alice offline) -> sealed copy with carol -> alice collects."""
    alice_port = free_port()
    carol_port = free_port()
    alice_dir = tmp_path / "alice"
    bob_dir = tmp_path / "bob"
    carol_dir = tmp_path / "carol"

    alice_hash = init_node(
        tmp_path, "alice", "--tcp-listen", f"127.0.0.1:{alice_port}"
    )["destination"]
    init_node(tmp_path, "carol", "--tcp-connect", f"127.0.0.1:{alice_port}")
    init_node(tmp_path, "bob", "--tcp-connect", f"127.0.0.1:{carol_port}")

    # Carol becomes a transport node and also listens for bob, so the
    # chain bob <-> carol <-> alice can carry announces and packets.
    carol_config = carol_dir / "reticulum" / "config"
    text = carol_config.read_text(encoding="utf-8")
    text = text.replace("enable_transport = No", "enable_transport = Yes")
    text += (
        "\n  [[TCP Server]]\n"
        "    type = TCPServerInterface\n"
        "    interface_enabled = True\n"
        "    listen_ip = 127.0.0.1\n"
        f"    listen_port = {carol_port}\n"
    )
    carol_config.write_text(text, encoding="utf-8")

    alice_out, alice_err = tmp_path / "alice.out", tmp_path / "alice.err"
    carol_out, carol_err = tmp_path / "carol.out", tmp_path / "carol.err"
    bob_out, bob_err = tmp_path / "bob.out", tmp_path / "bob.err"

    alice = start_serve(alice_dir, alice_out, alice_err)
    carol = None
    bob = None
    try:
        time.sleep(2.0)
        carol = start_serve(carol_dir, carol_out, carol_err)
        time.sleep(2.0)
        # Bob learns about alice (and her public key) through carol.
        discovered = discover_until(bob_dir, alice_hash)
        assert alice_hash in discovered, carol_err.read_text()
    finally:
        stop_process(alice)

    try:
        # Alice is offline now. Bob sends; the message queues, and once
        # bob's node is serving it leaves a sealed copy with carol.
        sent = run_tau(
            "send",
            "--data-dir",
            str(bob_dir),
            "--to",
            alice_hash,
            "carried by carol",
            "--timeout",
            "8",
            "--discover-timeout",
            "3",
            "--json",
            timeout=90.0,
        )
        assert json.loads(require_ok(sent))["queued"] is True

        bob = start_serve(bob_dir, bob_out, bob_err)
        mailbox_file = carol_dir / "mailbox.json"
        deadline = time.time() + 90
        held = 0
        while time.time() < deadline:
            if mailbox_file.exists():
                held = len(json.loads(mailbox_file.read_text())["held"])
                if held >= 1:
                    break
            time.sleep(1.0)
        assert held >= 1, (
            f"carol never received a sealed copy\ncarol err:\n{carol_err.read_text()}"
        )
        stop_process(bob)
        bob = None

        # Alice returns while bob stays offline: only the relay can deliver.
        alice = start_serve(alice_dir, alice_out, alice_err)
        assert wait_for_text(alice_out, "carried by carol", timeout=150), (
            f"alice never collected the relayed message\n"
            f"alice err:\n{alice_err.read_text()}\n"
            f"carol err:\n{carol_err.read_text()}"
        )
        assert '"relayed": true' in alice_out.read_text(encoding="utf-8")
        stop_process(alice)
    finally:
        if bob is not None:
            stop_process(bob)
        if carol is not None:
            stop_process(carol)


def test_two_nodes_exchange_tan_posts(tmp_path: Path) -> None:
    port = free_port()
    alice_dir = tmp_path / "alice"
    bob_dir = tmp_path / "bob"

    alice_init = json.loads(
        require_ok(
            run_tau(
                "init",
                "--data-dir",
                str(alice_dir),
                "--name",
                "alice",
                "--tcp-listen",
                f"127.0.0.1:{port}",
                "--no-auto-interface",
                "--json",
            )
        )
    )
    alice_hash = alice_init["destination"]

    require_ok(
        run_tau(
            "init",
            "--data-dir",
            str(bob_dir),
            "--name",
            "bob",
            "--tcp-connect",
            f"127.0.0.1:{port}",
            "--no-auto-interface",
            "--json",
        )
    )

    log_path = tmp_path / "alice-tan.log"
    with log_path.open("w", encoding="utf-8") as log_file:
        server = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tau",
                "serve",
                "--data-dir",
                str(alice_dir),
                "--announce-interval",
                "1",
                "--social",
                "--json",
            ],
            cwd=str(ROOT),
            env={**os.environ, "PYTHONPATH": str(ROOT)},
            stdout=subprocess.DEVNULL,
            stderr=log_file,
            text=True,
        )

    try:
        time.sleep(2.0)
        assert server.poll() is None, log_path.read_text()

        discovered = {}
        for _ in range(3):
            result = run_tau(
                "discover",
                "--data-dir",
                str(bob_dir),
                "--duration",
                "6",
                "--json",
                timeout=40.0,
            )
            payload = json.loads(require_ok(result))
            discovered = {
                peer["destination"]: peer for peer in payload.get("peers", [])
            }
            if alice_hash in discovered:
                break
        assert alice_hash in discovered, log_path.read_text()

        posted = json.loads(
            require_ok(
                run_tau(
                    "tan",
                    "post",
                    "--data-dir",
                    str(alice_dir),
                    "--no-publish",
                    "--json",
                    "hello from alice",
                    timeout=30.0,
                )
            )
        )
        assert posted["post"]["text"] == "hello from alice"

        synced = json.loads(
            require_ok(
                run_tau(
                    "tan",
                    "sync",
                    "--data-dir",
                    str(bob_dir),
                    "--to",
                    alice_hash,
                    "--author",
                    alice_hash,
                    "--json",
                    timeout=60.0,
                )
            )
        )
        assert synced["stored"] == 1, synced

        timeline = json.loads(
            require_ok(
                run_tau(
                    "tan",
                    "timeline",
                    "--data-dir",
                    str(bob_dir),
                    "--all",
                    "--json",
                    timeout=30.0,
                )
            )
        )
        assert [post["text"] for post in timeline["posts"]] == ["hello from alice"]

        require_ok(
            run_tau(
                "tan",
                "follow",
                "--data-dir",
                str(bob_dir),
                alice_hash,
                "--json",
                timeout=30.0,
            )
        )

        pushed = json.loads(
            require_ok(
                run_tau(
                    "tan",
                    "post",
                    "--data-dir",
                    str(bob_dir),
                    "--to",
                    alice_hash,
                    "--json",
                    "hello from bob",
                    timeout=60.0,
                )
            )
        )
        assert pushed["post"]["text"] == "hello from bob"
        assert pushed["published"]["delivered"] >= 1, pushed

        time.sleep(1.0)
        alice_timeline = json.loads(
            require_ok(
                run_tau(
                    "tan",
                    "timeline",
                    "--data-dir",
                    str(alice_dir),
                    "--all",
                    "--json",
                    timeout=30.0,
                )
            )
        )
        assert "hello from bob" in [
            post["text"] for post in alice_timeline["posts"]
        ], alice_timeline
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)


def test_second_serve_on_same_data_dir_fails_fast(tmp_path: Path) -> None:
    port = free_port()
    alice_dir = tmp_path / "alice"

    require_ok(
        run_tau(
            "init",
            "--data-dir",
            str(alice_dir),
            "--name",
            "alice",
            "--tcp-listen",
            f"127.0.0.1:{port}",
            "--no-auto-interface",
            "--json",
        )
    )

    out_path = tmp_path / "alice.out"
    err_path = tmp_path / "alice.err"
    server = start_serve(alice_dir, out_path, err_path)
    try:
        # Wait until the first node is up (and holds the data-dir lock).
        assert wait_for_text(out_path, '"ready"', timeout=60), (
            err_path.read_text(encoding="utf-8")
        )

        second = run_tau(
            "serve",
            "--data-dir",
            str(alice_dir),
            "--json",
            timeout=30.0,
        )
        assert second.returncode != 0
        assert "already serving" in second.stderr
        assert str(alice_dir) in second.stderr
        # The first node is untouched by the rejected start.
        assert server.poll() is None
    finally:
        stop_process(server)

    # After the first node exits, the directory can be served again.
    out2_path = tmp_path / "alice2.out"
    err2_path = tmp_path / "alice2.err"
    restarted = start_serve(alice_dir, out2_path, err2_path)
    try:
        assert wait_for_text(out2_path, '"ready"', timeout=60), (
            err2_path.read_text(encoding="utf-8")
        )
        assert restarted.poll() is None
    finally:
        stop_process(restarted)


def test_second_serve_on_same_tcp_port_fails_fast(tmp_path: Path) -> None:
    """Two nodes, different data dirs, same TCP listen port: the second
    serve must exit non-zero with the port named, not run deaf with a
    dead TCP interface (issue #16)."""
    port = free_port()
    alice_dir = tmp_path / "alice"
    bob_dir = tmp_path / "bob"

    for data_dir, name in ((alice_dir, "alice"), (bob_dir, "bob")):
        require_ok(
            run_tau(
                "init",
                "--data-dir",
                str(data_dir),
                "--name",
                name,
                "--tcp-listen",
                f"127.0.0.1:{port}",
                "--no-auto-interface",
                "--json",
            )
        )

    alice_out = tmp_path / "alice.out"
    alice_err = tmp_path / "alice.err"
    alice = start_serve(alice_dir, alice_out, alice_err)
    try:
        time.sleep(2.0)
        assert alice.poll() is None, alice_err.read_text()

        # The second serve must fail fast; if the check regressed it
        # would serve forever and hit the timeout instead.
        try:
            result = run_tau(
                "serve",
                "--data-dir",
                str(bob_dir),
                "--duration",
                "5",
                "--json",
                timeout=30.0,
            )
        except subprocess.TimeoutExpired:
            pytest.fail("second serve kept running instead of failing fast")
        assert result.returncode != 0, (
            f"second serve exited 0\nstdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
        assert "already in use" in result.stderr, result.stderr
        assert str(port) in result.stderr, result.stderr
        assert "ready" not in result.stdout
    finally:
        stop_process(alice)


def test_three_nodes_exchange_group_messages(tmp_path: Path) -> None:
    """Group DMs (issue #80): create, fan out, offline leg queues.

    Alice creates the group with one-shot commands (her node binds the
    TCP listen port); bob and carol serve as TCP clients and announce,
    so alice discovers them first.  The offline leg is queued in
    alice's outbox and delivered by `tau outbox retry` once carol
    returns.
    """
    port = free_port()
    alice_init = init_node(tmp_path, "alice", "--tcp-listen", f"127.0.0.1:{port}")
    alice_hash = alice_init["destination"]
    bob_init = init_node(tmp_path, "bob", "--tcp-connect", f"127.0.0.1:{port}")
    bob_hash = bob_init["destination"]
    carol_init = init_node(tmp_path, "carol", "--tcp-connect", f"127.0.0.1:{port}")
    carol_hash = carol_init["destination"]
    alice_dir = tmp_path / "alice"
    bob_dir = tmp_path / "bob"
    carol_dir = tmp_path / "carol"

    bob_out = tmp_path / "bob.out"
    bob_err = tmp_path / "bob.err"
    carol_out = tmp_path / "carol.out"
    carol_err = tmp_path / "carol.err"
    bob = start_serve(bob_dir, bob_out, bob_err)
    carol = start_serve(carol_dir, carol_out, carol_err)
    try:
        time.sleep(2.0)
        assert bob.poll() is None, bob_err.read_text()
        assert carol.poll() is None, carol_err.read_text()

        # Alice's one-shot discover binds the listen port, letting the
        # clients connect and announce; wait until both are known.
        for wanted in (bob_hash, carol_hash):
            discovered = discover_until(alice_dir, wanted, attempts=4)
            assert wanted in discovered, (
                f"peer {wanted} was not discovered"
            )

        created = json.loads(
            require_ok(
                run_tau(
                    "group",
                    "create",
                    "weekend",
                    "--peer",
                    bob_hash,
                    "--peer",
                    carol_hash,
                    "--data-dir",
                    str(alice_dir),
                    "--discover-timeout",
                    "15",
                    "--json",
                    timeout=120.0,
                )
            )
        )
        group_id = created["group"]["id"]
        assert created["group"]["name"] == "weekend"
        assert len(created["group"]["members"]) == 3

        sent = json.loads(
            require_ok(
                run_tau(
                    "group",
                    "send",
                    "weekend",
                    "hello group",
                    "--data-dir",
                    str(alice_dir),
                    "--timeout",
                    "20",
                    "--discover-timeout",
                    "10",
                    "--json",
                    timeout=120.0,
                )
            )
        )
        assert sent["group"] == group_id
        assert all(
            result.get("delivered") for result in sent["results"]
        ), sent

        assert wait_for_text(bob_out, "hello group"), bob_err.read_text()
        assert wait_for_text(carol_out, "hello group"), carol_err.read_text()

        for member_dir in (bob_dir, carol_dir):
            inbox = json.loads(
                require_ok(
                    run_tau(
                        "messages",
                        "--data-dir",
                        str(member_dir),
                        "--json",
                        timeout=30.0,
                    )
                )
            )
            matches = [
                entry
                for entry in inbox["messages"]
                if entry["text"] == "hello group"
            ]
            assert len(matches) == 1, inbox
            assert matches[0].get("kind") == "group"
            assert matches[0].get("group") == group_id
            assert matches[0].get("group_name") == "weekend"

        listed = json.loads(
            require_ok(
                run_tau(
                    "group",
                    "list",
                    "--data-dir",
                    str(alice_dir),
                    "--json",
                    timeout=30.0,
                )
            )
        )
        assert listed["count"] == 1
        assert listed["groups"][0]["id"] == group_id
    finally:
        stop_process(bob)
        stop_process(carol)

    # Carol stays offline but bob returns, so only carol's leg queues.
    bob = start_serve(bob_dir, bob_out, bob_err)
    try:
        time.sleep(2.0)
        assert bob.poll() is None, bob_err.read_text()
        # Bob's client reconnects once alice's one-shot node binds the
        # listen port; confirm the path is live before sending.
        discovered = discover_until(alice_dir, bob_hash, attempts=3)
        assert bob_hash in discovered

        # Carol goes offline. The next group message queues her leg only.
        sent = json.loads(
            require_ok(
                run_tau(
                    "group",
                    "send",
                    "weekend",
                    "while carol was away",
                    "--data-dir",
                    str(alice_dir),
                    "--timeout",
                    "8",
                    "--discover-timeout",
                    "3",
                    "--json",
                    timeout=120.0,
                )
            )
        )
        by_to = {result["to"]: result for result in sent["results"]}
        assert by_to[bob_hash].get("delivered") is True, sent
        assert by_to[carol_hash].get("queued") is True, sent
    finally:
        stop_process(bob)

    outbox = json.loads(
        require_ok(
            run_tau("outbox", "--data-dir", str(alice_dir), "--json", timeout=30.0)
        )
    )
    assert outbox["count"] == 1
    assert outbox["outbox"][0]["group"]["id"] == group_id

    # Carol returns; `tau outbox retry` delivers the queued leg.
    carol = start_serve(carol_dir, carol_out, carol_err)
    try:
        time.sleep(2.0)
        assert carol.poll() is None, carol_err.read_text()
        summary = json.loads(
            require_ok(
                run_tau(
                    "outbox",
                    "retry",
                    "--data-dir",
                    str(alice_dir),
                    "--json",
                    timeout=120.0,
                )
            )
        )
        assert summary["delivered"] == 1, summary
        assert wait_for_text(carol_out, "while carol was away"), (
            carol_err.read_text()
        )
    finally:
        stop_process(carol)

    outbox = json.loads(
        require_ok(
            run_tau("outbox", "--data-dir", str(alice_dir), "--json", timeout=30.0)
        )
    )
    assert outbox["count"] == 0
    inbox = json.loads(
        require_ok(
            run_tau("messages", "--data-dir", str(carol_dir), "--json", timeout=30.0)
        )
    )
    matches = [
        entry
        for entry in inbox["messages"]
        if entry["text"] == "while carol was away"
    ]
    assert len(matches) == 1, inbox
    assert matches[0].get("kind") == "group"
    assert matches[0].get("group") == group_id


def test_two_nodes_send_a_file(tmp_path: Path) -> None:
    port = free_port()
    alice_dir = tmp_path / "alice"
    bob_dir = tmp_path / "bob"

    alice_init = json.loads(
        require_ok(
            run_tau(
                "init",
                "--data-dir",
                str(alice_dir),
                "--name",
                "alice",
                "--tcp-listen",
                f"127.0.0.1:{port}",
                "--no-auto-interface",
                "--json",
            )
        )
    )
    alice_hash = alice_init["destination"]
    bob_init = json.loads(
        require_ok(
            run_tau(
                "init",
                "--data-dir",
                str(bob_dir),
                "--name",
                "bob",
                "--tcp-connect",
                f"127.0.0.1:{port}",
                "--no-auto-interface",
                "--json",
            )
        )
    )
    bob_hash = bob_init["destination"]

    # Alice opts in to files before she starts serving.
    require_ok(
        run_tau(
            "config",
            "accept-files",
            "on",
            "--data-dir",
            str(alice_dir),
            "--json",
            timeout=30.0,
        )
    )

    payload = b"tau file transfer\n" * 200
    source = tmp_path / "notes.txt"
    source.write_bytes(payload)

    alice_out = tmp_path / "alice.out"
    alice_err = tmp_path / "alice.err"
    alice = start_serve(alice_dir, alice_out, alice_err)
    try:
        time.sleep(2.0)
        assert alice.poll() is None, alice_err.read_text()

        discovered = {}
        for _ in range(3):
            result = run_tau(
                "discover",
                "--data-dir",
                str(bob_dir),
                "--duration",
                "6",
                "--json",
                timeout=40.0,
            )
            payload_discovered = json.loads(require_ok(result))
            discovered = {
                peer["destination"]: peer
                for peer in payload_discovered.get("peers", [])
            }
            if alice_hash in discovered:
                break
        assert alice_hash in discovered, alice_err.read_text()

        sent = run_tau(
            "send-file",
            "--data-dir",
            str(bob_dir),
            "--to",
            alice_hash,
            str(source),
            "--timeout",
            "120",
            "--json",
            timeout=180.0,
        )
        response = json.loads(require_ok(sent))
        assert response["ok"] is True
        assert response["name"] == "notes.txt"
        assert response["size"] == len(payload)
        assert response["sha256"] == hashlib.sha256(payload).hexdigest()

        # The receiver saves the file once its own Resource concludes,
        # which trails the sender's conclusion slightly — poll for it.
        saved = alice_dir / "files" / bob_hash / "notes.txt"
        deadline = time.time() + 60.0
        while time.time() < deadline:
            if saved.exists() and saved.read_bytes() == payload:
                break
            time.sleep(0.5)
        assert saved.exists(), alice_err.read_text()
        assert saved.read_bytes() == payload

        messages = json.loads(
            require_ok(
                run_tau("messages", "--data-dir", str(alice_dir), "--json", timeout=30.0)
            )
        )
        file_entries = [
            entry
            for entry in messages["messages"]
            if entry.get("kind") == "file"
        ]
        assert len(file_entries) == 1
        assert file_entries[0]["name"] == "notes.txt"
        assert file_entries[0]["size"] == len(payload)
        assert file_entries[0]["sha256"] == hashlib.sha256(payload).hexdigest()

        # Policy is re-read per offer, so opting out live refuses the
        # next transfer instead of queueing it.
        require_ok(
            run_tau(
                "config",
                "accept-files",
                "off",
                "--data-dir",
                str(alice_dir),
                "--json",
                timeout=30.0,
            )
        )
        refused = run_tau(
            "send-file",
            "--data-dir",
            str(bob_dir),
            "--to",
            alice_hash,
            str(source),
            "--timeout",
            "30",
            "--json",
            timeout=90.0,
        )
        assert refused.returncode != 0
        assert "does not accept files" in refused.stdout + refused.stderr
    finally:
        stop_process(alice)

def test_serve_with_lxmf_runs_router(tmp_path: Path) -> None:
    """`tau serve --lxmf` runs a real LXMRouter and exits cleanly.

    The router's delivery destination is registered (its ratchet store
    appears under the data dir), the ready event reports the LXMF
    delivery address, and SIGTERM shuts the serve down with exit code 0
    and no traceback — the router's exit handler runs before Reticulum
    exits.
    """
    pytest.importorskip("LXMF")
    port = free_port()
    data_dir = tmp_path / "lxmf-node"
    require_ok(
        run_tau(
            "init",
            "--data-dir",
            str(data_dir),
            "--name",
            "lxmf-node",
            "--tcp-listen",
            f"127.0.0.1:{port}",
            "--no-auto-interface",
            "--json",
        )
    )
    stdout_path = tmp_path / "serve.out"
    stderr_path = tmp_path / "serve.err"
    process = start_serve(
        data_dir, stdout_path, stderr_path, "--lxmf", "--duration", "60"
    )
    try:
        assert wait_for_text(stdout_path, '"ready"', timeout=60), (
            stderr_path.read_text(encoding="utf-8")
            if stderr_path.exists()
            else "serve produced no output"
        )
        assert process.poll() is None, stderr_path.read_text(encoding="utf-8")
        deadline = time.time() + 60
        ratchets = data_dir / "lxmf" / "ratchets"
        while not ratchets.is_dir():
            if time.time() > deadline:
                pytest.fail(
                    "LXMF router storage never appeared; "
                    f"stderr:\n{stderr_path.read_text(encoding='utf-8')}"
                )
            time.sleep(0.5)
        ready = json.loads(stdout_path.read_text(encoding="utf-8").splitlines()[0])
        lxmf_info = ready["node"]["lxmf"]
        assert lxmf_info["enabled"] is True
        # A 16-byte destination hash, hex-encoded.
        assert isinstance(lxmf_info["delivery"], str)
        assert len(lxmf_info["delivery"]) == 32
    finally:
        stop_process(process)
    assert process.returncode == 0, stderr_path.read_text(encoding="utf-8")
    assert "Traceback" not in stderr_path.read_text(encoding="utf-8")


def test_reply_quoting_round_trip(tmp_path: Path) -> None:
    """A reply sent with --reply-to arrives with its reference intact,
    and the receiver's CLI renders the quote from local history."""
    port = free_port()
    alice_dir = tmp_path / "alice"
    bob_dir = tmp_path / "bob"

    alice_init = json.loads(
        require_ok(
            run_tau(
                "init",
                "--data-dir",
                str(alice_dir),
                "--name",
                "alice",
                "--tcp-listen",
                f"127.0.0.1:{port}",
                "--no-auto-interface",
                "--json",
            )
        )
    )
    alice_hash = alice_init["destination"]

    require_ok(
        run_tau(
            "init",
            "--data-dir",
            str(bob_dir),
            "--name",
            "bob",
            "--tcp-connect",
            f"127.0.0.1:{port}",
            "--no-auto-interface",
            "--json",
        )
    )

    log_path = tmp_path / "alice.log"
    with log_path.open("w", encoding="utf-8") as log_file:
        server = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tau",
                "serve",
                "--data-dir",
                str(alice_dir),
                "--announce-interval",
                "1",
                "--json",
            ],
            cwd=str(ROOT),
            env={**os.environ, "PYTHONPATH": str(ROOT)},
            stdout=subprocess.DEVNULL,
            stderr=log_file,
            text=True,
        )

    try:
        time.sleep(2.0)
        assert server.poll() is None, log_path.read_text()

        discovered = {}
        for _ in range(3):
            result = run_tau(
                "discover",
                "--data-dir",
                str(bob_dir),
                "--duration",
                "6",
                "--json",
                timeout=40.0,
            )
            payload = json.loads(require_ok(result))
            discovered = {
                peer["destination"]: peer for peer in payload.get("peers", [])
            }
            if alice_hash in discovered:
                break
        assert alice_hash in discovered, (
            f"alice was not discovered; alice log:\n{log_path.read_text()}"
        )

        sent = json.loads(
            require_ok(
                run_tau(
                    "send",
                    "--data-dir",
                    str(bob_dir),
                    "--to",
                    alice_hash,
                    "original thought",
                    "--timeout",
                    "20",
                    "--discover-timeout",
                    "10",
                    "--json",
                    timeout=60.0,
                )
            )
        )
        assert sent["ok"] is True
        original_id = sent["id"]
        assert original_id

        reply = json.loads(
            require_ok(
                run_tau(
                    "send",
                    "--data-dir",
                    str(bob_dir),
                    "--to",
                    alice_hash,
                    "--reply-to",
                    original_id[:8],
                    "seconded",
                    "--timeout",
                    "20",
                    "--discover-timeout",
                    "10",
                    "--json",
                    timeout=60.0,
                )
            )
        )
        assert reply["ok"] is True
        assert reply["echo"] == "seconded"

        # Alice's inbox carries the reference; her CLI renders the
        # quote from the original she received.
        messages = json.loads(
            require_ok(
                run_tau(
                    "messages",
                    "--data-dir",
                    str(alice_dir),
                    "--json",
                    timeout=30.0,
                )
            )
        )
        by_id = {item["id"]: item for item in messages["messages"]}
        assert by_id[reply["id"]]["in_reply_to"]["id"] == original_id
        assert by_id[reply["id"]]["in_reply_to"]["excerpt"]

        text = require_ok(
            run_tau(
                "messages",
                "--data-dir",
                str(alice_dir),
                timeout=30.0,
            )
        )
        quote_lines = [
            line.strip()
            for line in text.splitlines()
            if line.strip().startswith("↩ ")
        ]
        assert len(quote_lines) == 1
        assert quote_lines[0].endswith(": original thought")
        assert "seconded" in text
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)


def _console_request(http_port: int, path: str, *, method="GET", body=None):
    """One JSON call against a serving node's console API (no proxy)."""
    import urllib.error
    import urllib.request

    url = f"http://127.0.0.1:{http_port}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=30) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def _wait_for_console_message(
    http_port: int, wire_id: str, *, expect_reactions, timeout: float = 60.0
):
    """Poll a console's /api/messages until *wire_id* matches the state.

    ``expect_reactions`` is the exact reactions list expected (``[]``
    means present without reactions, ``None`` means any state).
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, payload = _console_request(http_port, "/api/messages")
        assert status == 200, payload
        for entry in payload.get("messages", []):
            if entry.get("wire_id") != wire_id:
                continue
            if expect_reactions is None:
                return entry
            if entry.get("reactions", []) == expect_reactions:
                return entry
        time.sleep(1.0)
    raise AssertionError(
        f"console message {wire_id} never reached reactions={expect_reactions}"
    )


def _wait_for_inbox_reactions(
    data_dir: Path, target: str, *, expect_empty: bool, timeout: float = 60.0
):
    """Poll inbox.json until the reaction state for *target* matches."""
    deadline = time.time() + timeout
    inbox_path = data_dir / "inbox.json"
    while time.time() < deadline:
        if inbox_path.exists():
            stored = json.loads(inbox_path.read_text(encoding="utf-8"))
            reactions = [
                item
                for item in stored.get("reactions", [])
                if item.get("target") == target and item.get("reaction")
            ]
            if expect_empty and not reactions:
                return []
            if not expect_empty and reactions:
                return reactions
        time.sleep(1.0)
    state = "empty" if expect_empty else "present"
    raise AssertionError(f"reactions for {target} never became {state}")


def test_two_nodes_exchange_dm_reactions(tmp_path: Path) -> None:
    # Both nodes serve with a console: one-shot commands cannot share a
    # data directory with a serving node (Reticulum refuses the second
    # bind of the TCP listen port), so the live reaction path is driven
    # through the serving nodes' own HTTP APIs instead.
    alice_port = free_port()
    bob_port = free_port()
    alice_http = free_port()
    bob_http = free_port()
    alice_dir = tmp_path / "alice"
    bob_dir = tmp_path / "bob"

    alice_init = json.loads(
        require_ok(
            run_tau(
                "init",
                "--data-dir",
                str(alice_dir),
                "--name",
                "alice",
                "--tcp-listen",
                f"127.0.0.1:{alice_port}",
                "--tcp-connect",
                f"127.0.0.1:{bob_port}",
                "--no-auto-interface",
                "--json",
            )
        )
    )
    alice_hash = alice_init["destination"]
    bob_init = json.loads(
        require_ok(
            run_tau(
                "init",
                "--data-dir",
                str(bob_dir),
                "--name",
                "bob",
                "--tcp-listen",
                f"127.0.0.1:{bob_port}",
                "--tcp-connect",
                f"127.0.0.1:{alice_port}",
                "--no-auto-interface",
                "--json",
            )
        )
    )
    bob_hash = bob_init["destination"]

    alice_out = tmp_path / "alice.out"
    alice_err = tmp_path / "alice.err"
    bob_out = tmp_path / "bob.out"
    bob_err = tmp_path / "bob.err"
    alice_server = start_serve(
        alice_dir, alice_out, alice_err, "--http", f"127.0.0.1:{alice_http}"
    )
    bob_server = start_serve(
        bob_dir, bob_out, bob_err, "--http", f"127.0.0.1:{bob_http}"
    )
    try:
        assert wait_for_text(alice_out, '"ready"', timeout=60), alice_err.read_text()
        assert wait_for_text(bob_out, '"ready"', timeout=60), bob_err.read_text()
        # Let the two serving nodes exchange announces before sending.
        time.sleep(5.0)

        status, sent = _console_request(
            alice_http,
            "/api/messages",
            method="POST",
            body={"to": bob_hash, "text": "hello bob, react to this"},
        )
        assert status == 200, sent
        assert sent["ok"] is True
        message_id = sent["message"]["wire_id"]
        assert message_id

        entry = _wait_for_console_message(bob_http, message_id, expect_reactions=[])
        assert entry["text"] == "hello bob, react to this"

        status, reacted = _console_request(
            bob_http,
            "/api/react",
            method="POST",
            body={"target": message_id, "reaction": "👍"},
        )
        assert status == 200, reacted
        assert reacted["ok"] is True
        assert reacted["reaction"] == "👍"
        assert reacted["target"] == message_id

        # The reactor sees their own reaction under the message...
        entry = _wait_for_console_message(
            bob_http,
            message_id,
            expect_reactions=[
                {"reactor": bob_hash, "name": "bob", "reaction": "👍"}
            ],
        )
        assert entry["text"] == "hello bob, react to this"
        # ...and the author records it even though the sent message is
        # not in her own inbox (sent messages are not stored there).
        reactions = _wait_for_inbox_reactions(alice_dir, message_id, expect_empty=False)
        assert len(reactions) == 1
        assert reactions[0]["reaction"] == "👍"
        assert reactions[0]["destination"] == bob_hash

        status, cleared = _console_request(
            bob_http,
            "/api/react",
            method="POST",
            body={"target": message_id, "clear": True},
        )
        assert status == 200, cleared
        assert cleared["ok"] is True
        assert cleared["cleared"] is True
        _wait_for_inbox_reactions(alice_dir, message_id, expect_empty=True)
        _wait_for_console_message(bob_http, message_id, expect_reactions=[])
    finally:
        stop_process(alice_server)
        stop_process(bob_server)


def test_disappearing_message_carries_expiry_to_receiver(tmp_path: Path) -> None:
    port = free_port()
    alice_dir = tmp_path / "alice"
    bob_dir = tmp_path / "bob"

    alice_init = json.loads(
        require_ok(
            run_tau(
                "init",
                "--data-dir",
                str(alice_dir),
                "--name",
                "alice",
                "--tcp-listen",
                f"127.0.0.1:{port}",
                "--no-auto-interface",
                "--json",
            )
        )
    )
    alice_hash = alice_init["destination"]

    require_ok(
        run_tau(
            "init",
            "--data-dir",
            str(bob_dir),
            "--name",
            "bob",
            "--tcp-connect",
            f"127.0.0.1:{port}",
            "--no-auto-interface",
            "--json",
        )
    )

    log_path = tmp_path / "alice.log"
    with log_path.open("w", encoding="utf-8") as log_file:
        server = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tau",
                "serve",
                "--data-dir",
                str(alice_dir),
                "--announce-interval",
                "1",
                "--json",
            ],
            cwd=str(ROOT),
            env={**os.environ, "PYTHONPATH": str(ROOT)},
            stdout=subprocess.DEVNULL,
            stderr=log_file,
            text=True,
        )

    try:
        time.sleep(2.0)
        assert server.poll() is None, log_path.read_text()

        discovered = {}
        for _ in range(3):
            result = run_tau(
                "discover",
                "--data-dir",
                str(bob_dir),
                "--duration",
                "6",
                "--json",
                timeout=40.0,
            )
            payload = json.loads(require_ok(result))
            discovered = {
                peer["destination"]: peer for peer in payload.get("peers", [])
            }
            if alice_hash in discovered:
                break
        assert alice_hash in discovered, (
            f"alice was not discovered; alice log:\n{log_path.read_text()}"
        )

        before = time.time()
        sent = run_tau(
            "send",
            "--data-dir",
            str(bob_dir),
            "--to",
            alice_hash,
            "--expire",
            "1h",
            "read fast",
            "--timeout",
            "20",
            "--discover-timeout",
            "10",
            "--json",
            timeout=60.0,
        )
        response = json.loads(require_ok(sent))
        assert response["ok"] is True
        assert response["echo"] == "read fast"

        # The receiver's durable inbox carries the sender-set deadline…
        inbox = json.loads((alice_dir / "inbox.json").read_text(encoding="utf-8"))
        messages = inbox["messages"]
        assert len(messages) == 1
        assert messages[0]["text"] == "read fast"
        expires_at = messages[0]["expires_at"]
        assert before + 3500 <= expires_at <= time.time() + 3700

        # …and `tau messages` shows it.
        listing = require_ok(
            run_tau("messages", "--data-dir", str(alice_dir), timeout=30.0)
        )
        assert "read fast" in listing
        assert "disappears in" in listing
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)


def test_read_receipts_flow_end_to_end(tmp_path: Path) -> None:
    """Bob sends alice a DM; alice reads it; bob sees it read (issue #83).

    Alice reads via `tau messages`, which sends one signed receipt
    back over the link; bob's serving node records `read_at` against
    the message, and `tau messages --sent` shows the read state.
    """
    port = free_port()
    alice_dir = tmp_path / "alice"
    bob_dir = tmp_path / "bob"

    alice_hash = init_node(
        tmp_path, "alice", "--tcp-listen", f"127.0.0.1:{port}"
    )["destination"]
    init_node(tmp_path, "bob", "--tcp-connect", f"127.0.0.1:{port}")

    alice_out, alice_err = tmp_path / "alice.out", tmp_path / "alice.err"
    bob_out, bob_err = tmp_path / "bob.out", tmp_path / "bob.err"
    alice = start_serve(alice_dir, alice_out, alice_err)
    bob = None
    try:
        time.sleep(2.0)
        assert alice.poll() is None, alice_err.read_text()
        bob = start_serve(bob_dir, bob_out, bob_err)
        time.sleep(2.0)
        assert bob.poll() is None, bob_err.read_text()

        discovered = discover_until(bob_dir, alice_hash)
        assert alice_hash in discovered, alice_err.read_text()

        sent = run_tau(
            "send",
            "--data-dir",
            str(bob_dir),
            "--to",
            alice_hash,
            "hello, please read this",
            "--timeout",
            "20",
            "--discover-timeout",
            "10",
            "--json",
            timeout=60.0,
        )
        response = json.loads(require_ok(sent))
        assert response["ok"] is True
        assert response["echo"] == "hello, please read this"
        wire_id = response["id"]
        assert isinstance(wire_id, str) and len(wire_id) == 32

        assert wait_for_text(alice_out, "hello, please read this"), (
            f"alice never received the message\n"
            f"alice err:\n{alice_err.read_text()}"
        )

        # Alice lists her inbox: reading it stamps the message read.
        # Her serving node already holds the TCP listen port, so the
        # one-shot list cannot start a node of its own — the serving
        # node's mail sweep delivers the receipt from the stamp.
        listed = run_tau(
            "messages", "--data-dir", str(alice_dir), "--json", timeout=60.0
        )
        inbox = json.loads(require_ok(listed))
        assert inbox["count"] >= 1
        assert any(
            entry.get("id") == wire_id for entry in inbox["messages"]
        ), inbox

        # Bob's sent history flips to read once the receipt lands.
        deadline = time.time() + 90.0
        state = None
        while time.time() < deadline:
            result = run_tau(
                "messages",
                "--sent",
                "--data-dir",
                str(bob_dir),
                "--json",
                timeout=30.0,
            )
            payload = json.loads(require_ok(result))
            entry = next(
                (
                    item
                    for item in payload["messages"]
                    if item["id"] == wire_id
                ),
                None,
            )
            assert entry is not None, payload
            state = entry["state"]
            if state == "read" and entry["read_at"]:
                break
            time.sleep(2.0)
        assert state == "read", (
            f"bob never saw the receipt (last state: {state})\n"
            f"alice err:\n{alice_err.read_text()}\n"
            f"bob err:\n{bob_err.read_text()}"
        )
    finally:
        if bob is not None:
            stop_process(bob)
        stop_process(alice)
