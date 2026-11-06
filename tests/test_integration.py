"""End-to-end test with two real Reticulum instances over TCP loopback.

Run with ``TAU_INTEGRATION=1 pytest -m integration``.
"""

from __future__ import annotations

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
