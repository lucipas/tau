#!/usr/bin/env python3
"""Spin up two tau nodes on TCP loopback and exchange a message."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def tau(*args: str, timeout: float = 60.0, check: bool = True) -> str:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    env["PYTHONUNBUFFERED"] = "1"
    result = subprocess.run(
        [PYTHON, "-m", "tau", *args],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if check and result.returncode != 0:
        print(result.stdout, end="")
        print(result.stderr, end="", file=sys.stderr)
        raise SystemExit(f"tau {' '.join(args)} failed")
    return result.stdout


def step(message: str) -> None:
    print(f"\n==> {message}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--keep", action="store_true", help="keep the temporary data directories"
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=6.0,
        help="how long bob listens for alice's announce (default: %(default)s)",
    )
    args = parser.parse_args()

    root = Path(tempfile.mkdtemp(prefix="tau-demo-"))
    alice_dir = root / "alice"
    bob_dir = root / "bob"
    port = free_port()
    server = None

    try:
        step(f"Creating nodes in {root} (TCP port {port})")
        alice = json.loads(
            tau(
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
        bob = json.loads(
            tau(
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
        print(f"    alice: {alice['destination']}")
        print(f"    bob:   {bob['destination']}")

        step("Starting alice's node")
        log_path = root / "alice.log"
        with log_path.open("w", encoding="utf-8") as log_file:
            env = os.environ.copy()
            env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
            server = subprocess.Popen(
                [
                    PYTHON,
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
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=log_file,
                text=True,
            )
        time.sleep(2.0)
        if server.poll() is not None:
            raise SystemExit(f"alice exited early:\n{log_path.read_text()}")

        step(f"bob listens for {args.duration:.0f}s")
        discovered = json.loads(
            tau(
                "discover",
                "--data-dir",
                str(bob_dir),
                "--duration",
                str(args.duration),
                "--json",
            )
        )
        peers = discovered.get("peers", [])
        if not peers:
            raise SystemExit(
                "bob did not discover alice; alice log:\n" + log_path.read_text()
            )
        for peer in peers:
            print(
                f"    found {peer['name']} ({peer['destination'][:8]}...) "
                f"hops={peer['hops']}"
            )

        step("bob sends a message")
        sent = json.loads(
            tau(
                "send",
                "--data-dir",
                str(bob_dir),
                "--to",
                alice["destination"],
                "hello from bob, over an encrypted Reticulum link",
                "--timeout",
                "20",
                "--json",
            )
        )
        print(f"    server echoed: {sent['echo']!r}")
        if isinstance(sent.get("_rtt"), (int, float)):
            print(f"    round trip:    {sent['_rtt'] * 1000:.0f} ms")

        step("bob pings alice")
        ping = json.loads(
            tau(
                "ping",
                "--data-dir",
                str(bob_dir),
                "--to",
                alice["destination"],
                "--timeout",
                "20",
                "--json",
            )
        )
        rtt = ping.get("_rtt")
        suffix = f" in {rtt * 1000:.0f} ms" if isinstance(rtt, (int, float)) else ""
        print(f"    pong from {ping['server']}{suffix}")

        step("bob asks alice for her node information")
        remote = json.loads(
            tau(
                "info",
                "--data-dir",
                str(bob_dir),
                "--to",
                alice["destination"],
                "--timeout",
                "20",
                "--json",
            )
        )
        print(
            f"    alice reports {remote['peers']} peer(s) and "
            f"{len(remote.get('interfaces', []))} interface(s)"
        )

        print("\nDemo complete.")
        return 0
    finally:
        if server is not None and server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)
        if args.keep:
            print(f"Data kept in {root}")
        else:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
