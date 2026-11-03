#!/usr/bin/env python3
"""Run two tau nodes on TCP loopback and exchange Tan social posts."""

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

    root = Path(tempfile.mkdtemp(prefix="tau-social-demo-"))
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

        step("Starting alice's node with the Tan feed enabled")
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
                    "--social",
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
        if not discovered.get("peers"):
            raise SystemExit(
                "bob did not discover alice; alice log:\n" + log_path.read_text()
            )
        for peer in discovered["peers"]:
            print(
                f"    found {peer['name']} ({peer['destination'][:8]}...) "
                f"hops={peer['hops']}"
            )

        step("alice writes a post locally")
        posted = json.loads(
            tau(
                "tan",
                "post",
                "--data-dir",
                str(alice_dir),
                "--no-publish",
                "--json",
                "hello from alice, over a signed hash chain",
            )
        )
        print(f"    {posted['post']['hash'][:16]}...  {posted['post']['text']!r}")

        step("bob pulls alice's feed")
        synced = json.loads(
            tau(
                "tan",
                "sync",
                "--data-dir",
                str(bob_dir),
                "--to",
                alice["destination"],
                "--author",
                alice["destination"],
                "--json",
            )
        )
        print(f"    stored {synced['stored']} new post(s)")

        step("bob follows alice and reads the timeline")
        tau(
            "tan",
            "follow",
            "--data-dir",
            str(bob_dir),
            alice["destination"],
            "--json",
        )
        timeline = json.loads(
            tau("tan", "timeline", "--data-dir", str(bob_dir), "--json")
        )
        for post in timeline["posts"]:
            print(f"    {post['name']}: {post['text']}")

        step("bob publishes a post to alice")
        pushed = json.loads(
            tau(
                "tan",
                "post",
                "--data-dir",
                str(bob_dir),
                "--to",
                alice["destination"],
                "--json",
                "hello back from bob",
            )
        )
        print(
            f"    delivered to {pushed['published']['delivered']} peer(s): "
            f"{pushed['post']['text']!r}"
        )

        time.sleep(1.0)
        alice_timeline = json.loads(
            tau("tan", "timeline", "--data-dir", str(alice_dir), "--all", "--json")
        )
        texts = [post["text"] for post in alice_timeline["posts"]]
        if "hello back from bob" not in texts:
            raise SystemExit("alice did not receive bob's post")
        print("    alice now has bob's post in her cache")

        print("\nSocial demo complete.")
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
