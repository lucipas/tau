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
