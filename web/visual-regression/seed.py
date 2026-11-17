#!/usr/bin/env python3
"""Seed the demo node pair for the visual regression run.

Waits for alice's bridge (http://127.0.0.1:8787), then arranges the
exact two-message state the screenshots expect: one inbound message
from bob, one outbound message to bob. The bob -> alice leg goes over
the wire via the CLI (recorded as inbound on alice); the alice -> bob
leg goes through the console's own POST /api/messages, which records
it as outbound (a plain CLI `tau send` is not recorded in the
sender's store).

Usage: seed.py <data-dir>   (data-dir holds alice/ and bob/)
Stdlib only. Exits non-zero if the messages do not land within 60s.
"""

import json
import subprocess
import sys
import time
import urllib.request

BRIDGE = "http://127.0.0.1:8787"


def get(path):
    with urllib.request.urlopen(BRIDGE + path, timeout=5) as resp:
        return json.load(resp)


def post(path, payload):
    req = urllib.request.Request(
        BRIDGE + path,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def wait_for_bridge(deadline):
    while time.time() < deadline:
        try:
            status = get("/api/status")
            if status.get("node", {}).get("name") == "alice":
                return status["node"]["destination"]
        except Exception:
            pass
        time.sleep(1)
    raise SystemExit("bridge did not come up on :8787")


def wait_for_peer(deadline, name="bob"):
    while time.time() < deadline:
        try:
            for peer in get("/api/peers").get("peers", []):
                if peer.get("name") == name:
                    return peer["destination"]
        except Exception:
            pass
        time.sleep(1)
    raise SystemExit(f"peer {name} never announced to alice")


def messages_with_bob(deadline):
    """Poll until both directions have landed; return (inbound, outbound)."""
    inbound, outbound = [], []
    while time.time() < deadline:
        try:
            feed = get("/api/messages?peer=bob")
            msgs = feed.get("messages", feed if isinstance(feed, list) else [])
            inbound = [m for m in msgs if m.get("direction") == "in"]
            outbound = [m for m in msgs if m.get("direction") == "out"]
            if inbound and outbound:
                return inbound, outbound
        except Exception:
            pass
        time.sleep(1)
    return inbound, outbound


def send_bob_to_alice(data_dir):
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "tau.cli",
            "send",
            "--data-dir",
            f"{data_dir}/bob",
            "--to",
            "alice",
            "--timeout",
            "60",
            "--wait",
            "30",
            "wooo i loike sheese",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    print(f"bob->alice send rc={result.returncode}: {(result.stdout or '').strip().splitlines()[-1:] or (result.stderr or '').strip().splitlines()[-1:]}")


def main():
    data_dir = sys.argv[1]
    deadline = time.time() + 180
    alice_dest = wait_for_bridge(deadline)
    bob_dest = wait_for_peer(deadline)
    print(f"alice={alice_dest[:12]}... bob={bob_dest[:12]}...")

    # bob -> alice over the wire (CLI send is recorded on the receiver);
    # alice -> bob via the console API (recorded as outbound on alice).
    # Under load an attempt can time out before the path is up, so
    # retry only the direction that has not landed yet.
    for attempt in range(1, 4):
        inbound, outbound = messages_with_bob(time.time() + 5)
        if not inbound:
            send_bob_to_alice(data_dir)
        if not outbound:
            try:
                resp = post("/api/messages", {"to": "bob", "text": "You do I want to"})
                print(f"alice->bob post: {resp}")
            except Exception as exc:
                print(f"alice->bob post failed: {exc}")
        inbound, outbound = messages_with_bob(time.time() + 30)
        if inbound and outbound:
            print(f"seeded: {len(inbound)} inbound, {len(outbound)} outbound")
            return
        print(f"attempt {attempt}: only {len(inbound)} inbound, {len(outbound)} outbound — retrying")
    raise SystemExit("seeded messages did not land on alice")


if __name__ == "__main__":
    main()
