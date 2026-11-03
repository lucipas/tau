# tau

**A peer-to-peer network in Python, built on the [Reticulum Network Stack](https://reticulum.network/).**

`tau` gives every participant a cryptographic identity, a self-authenticating
address, and an encrypted link to other peers. There is no server, no account
and no central directory: nodes find each other through signed Reticulum
announces, then talk directly over encrypted links.

```
             announce: "tau.node",  name + capabilities
        .----------------------------------------------.
        |                                              |
        v                                              |
  +-----------+      encrypted RNS Link          +-----------+
  |   alice   | <------------------------------> |    bob    |
  |  identity |   /tau/ping   /tau/info         |  identity |
  |  tau.node |   /tau/message /tau/peers        |  tau.node |
  +-----------+                                  +-----------+
        ^                                              ^
        |          Reticulum transports:               |
        +------ TCP, LAN AutoInterface, LoRa, ... -----+
```

## Why Reticulum?

Reticulum handles identity, encryption, path discovery and transport. `tau`
adds the peer-to-peer application layer:

- **Self-sovereign identity** — X25519 + Ed25519 keys stored locally.
- **Encrypted, authenticated links** — no plaintext on the wire.
- **Transport agnostic** — TCP, LAN multicast, packet radio, and the other
  Reticulum interfaces work without changing the application.
- **Small and local-first** — a single Python package and one dependency
  (`rns`); no database, broker or daemon required.

## Install

Requires Python 3.9+ and [Reticulum](https://pypi.org/project/rns/).

```bash
git clone https://github.com/lucipas/tau.git
cd tau
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"     # or: make venv
.venv/bin/tau --help
```

The package installs a `tau` console script. In the examples below, use
`.venv/bin/tau` if the virtualenv is not activated.

## Quick start: two nodes on one machine

The included demo starts both nodes, discovers the peer and exchanges a
message:

```bash
make demo
```

Or do it by hand. **Terminal 1** — start a listening node:

```bash
tau init --data-dir /tmp/alice --name alice \
  --tcp-listen 127.0.0.1:4343 --no-auto-interface
tau serve --data-dir /tmp/alice --announce-interval 5
```

**Terminal 2** — start a second node, discover Alice and message her:

```bash
tau init --data-dir /tmp/bob --name bob \
  --tcp-connect 127.0.0.1:4343 --no-auto-interface

tau discover --data-dir /tmp/bob --duration 10
tau send --data-dir /tmp/bob --to alice "hello over reticulum"
tau ping --data-dir /tmp/bob --to alice
```

`--to` accepts a peer name, a full 32-character destination hash, or a unique
hash prefix (at least 4 characters). Alice sees the message in her `serve`
terminal; Bob gets the echoed response and the round-trip time.

## LAN use

`tau init` enables Reticulum's **AutoInterface** by default, so nodes on the
same LAN find each other with no configuration:

```bash
tau init --name alice
tau serve
```

```bash
tau init --name bob
tau discover --duration 10
tau send --to alice "hi"
```

If multicast is unavailable (some containers, firewalled networks), use
`--tcp-listen` / `--tcp-connect` as in the quick start, or edit
`<data-dir>/reticulum/config` and add any interface Reticulum supports (TCP,
I2P, RNode/LoRa, serial, KISS, ...). All tau nodes must use the `tau` app name
and the `node` aspect, which is hard-coded in the protocol.

## Web console

A small Next.js + Tailwind console can mirror everything that reaches a node
and send messages back over the mesh. It is exported as static files and served
by the Python bridge, so no Node process is needed at runtime.

```bash
make web-build            # npm install && next build -> web/out
tau serve --http          # console + JSON API on http://127.0.0.1:8787/
```

Open <http://127.0.0.1:8787/>. The page streams inbound messages, peers and
their latency in real time over server-sent events.

The same bridge is the local integration point for scripts:

| Endpoint | Method | Purpose |
| --- | --- | --- |
| `/api/status` | GET | node info, peers, recent messages |
| `/api/peers` | GET | peer registry |
| `/api/messages` | GET | recent inbound/outbound messages |
| `/api/messages` | POST | send `{"to": "alice", "text": "hi"}` |
| `/api/ping` | POST | measure a peer's round trip |
| `/api/announce` | POST | announce this node immediately |
| `/api/events` | GET | SSE stream of messages and peer updates |

```bash
# What arrived from the mesh, as JSON:
curl -s http://127.0.0.1:8787/api/messages | jq .

# Send a message from this machine:
curl -s -X POST http://127.0.0.1:8787/api/messages \
  -H 'Content-Type: application/json' \
  -d '{"to": "alice", "text": "hello from a script"}'
```

The bridge binds to loopback by default. It can send messages as this node, so
only pass `--http-allow-remote` together with a non-loopback `--http` address
when you really mean to expose it. Use `--web-dir` or `TAU_WEB_DIR` to serve a
different build, and `make web-dev` for the Next.js dev server on port 3000
(the console automatically talks to the bridge on port 8787).

## Command reference

Global options may appear before or after the command:

| Option | Description |
| --- | --- |
| `--data-dir DIR` | Data directory (default: `$TAU_HOME` or `~/.tau`) |
| `--rns-config DIR` | Reticulum config directory (default: `<data-dir>/reticulum`) |
| `--name NAME` | Node name announced to peers |
| `--json` | Machine-readable output |
| `-v`, `-vv` | Info / debug logging |
| `-q` | Errors only |
| `--rns-log-level 0-8` | Explicit Reticulum log level |

| Command | Description |
| --- | --- |
| `tau init` | Create the data directory, identity and Reticulum config |
| `tau serve` | Announce this node and answer requests until interrupted |
| `tau discover` | Listen for announces for `--duration` seconds |
| `tau send --to PEER TEXT` | Send a message over an encrypted link |
| `tau ping --to PEER` | Check reachability and measure round-trip time |
| `tau info --to PEER` | Fetch a remote node's information |
| `tau peers` | List peers from the local registry |
| `tau id` | Show this node's name and destination hash |

Useful flags:

```bash
# Don't overwrite an existing config, but add a TCP server alongside AutoInterface
tau init --tcp-listen 0.0.0.0:4343 --init

# Announce every 30 seconds, stop after one minute
tau serve --announce-interval 30 --duration 60

# Serve the web console and JSON API on loopback (port 8787)
tau serve --http

# Serve on a specific address (remote bind needs an explicit opt-in)
tau serve --http 0.0.0.0:8787 --http-allow-remote

# Read the message from a file or stdin
tau send --to alice --file message.txt
echo "piped message" | tau send --to alice

# Only show peers heard from in the last day
tau peers --max-age 1

# Everything as JSON
tau discover --duration 10 --json
tau send --to alice "hi" --json
```

`serve` emits one JSON object per line with `--json` (`ready`, `peer`,
`message` events), which makes it easy to feed into another process.
`discover`, `peers`, `send`, `ping`, `info` and `id` emit a single JSON object.

## Data directory

By default everything lives in `~/.tau` (override with `TAU_HOME` or
`--data-dir`):

```
~/.tau/
├── identity              # private key, chmod 600 — this is your address
├── node.json             # local display name
├── peers.json            # discovered peers, chmod 600
└── reticulum/
    ├── config            # generated Reticulum configuration
    └── storage/          # Reticulum internals (paths, ratchets, ...)
```

Back up `identity` if you want to keep the same address. Anyone who reads it
can impersonate the node and decrypt its traffic, so keep it private.

## How it works

### Identity and addressing

Each node creates one `RNS.Identity`. Its tau destination is:

```
tau.node.<destination-hash>
```

The destination hash is derived from the identity's public key, so it is a
stable, self-authenticating address. Names are only human-friendly labels:
they are self-asserted, not unique and not trusted. Use destination hashes when
identity matters.

### Discovery

`tau serve` announces the destination every 300 seconds by default (with
jitter). The announce carries a small, signed JSON payload:

```json
{"v": 1, "name": "alice", "proto": "tau", "caps": ["message", "ping", "peers"]}
```

Reticulum verifies the signature and distributes the announce through the
mesh. Every tau node that hears it stores the peer in `peers.json` and updates
its path table. A node that is asked to message an unknown destination sends a
Reticulum path request and waits for the announce before giving up.

### Messaging

Messages use a fresh encrypted `RNS.Link` per request. The client announces
itself once (so the recipient can name the caller), identifies itself on the
link, sends a request, waits for the response, and tears the link down. Four
request paths are implemented:

| Path | Request | Response |
| --- | --- | --- |
| `/tau/ping` | `{}` | `{"ok": true, "pong": true, "server": ..., "time": ...}` |
| `/tau/info` | `{}` | name, destination, versions, uptime, peer/interface counts |
| `/tau/message` | `{"type": "message", "v": 1, "text": "...", "sent_at": ...}` | `{"ok": true, "echo": "...", "server": ..., "node": ...}` |
| `/tau/peers` | `{}` | up to 100 public peer records |

Payloads are compact UTF-8 JSON. Everything is size limited so a malicious or
buggy peer cannot force unbounded buffering:

| Limit | Value |
| --- | --- |
| Announce payload | 256 bytes |
| Request payload | 8 KiB |
| Response payload | 64 KiB |
| Message text | 2,000 characters |
| Node name | 48 characters |
| Peers per `/tau/peers` response | 100 |
| Peer registry size | 256 entries |

Names and messages are stripped of control characters (including terminal
escape sequences) before display, and message text is never written to disk.

## Security model

- **Confidentiality and integrity** come from Reticulum: links are encrypted
  and authenticated, announces are signed, and identities are verified.
- **No accounts or central authority.** Trust is key-based. Names can collide,
  so compare destination hashes out of band when it matters.
- **Open by default.** The request handlers use `ALLOW_ALL`: any node that can
  reach the destination may send a message. This is appropriate for a mesh
  chat; deployments that need allow-lists should add one in
  `TauNode._handle_message`.
- **Not anonymous.** The destination hash is a stable pseudonym. Peers can
  correlate your identity across sessions and transports.
- **No store-and-forward.** If the peer is offline, the send fails. Messages
  are not queued, relayed or persisted by tau.
- **Identity is the trust anchor.** Protect `identity`; it is stored with mode
  `0600`. The peer registry is also `0600` and contains only public keys and
  announce metadata.
- **One Reticulum instance per process.** Reticulum is a process-wide
  singleton, so a process runs at most one tau node. This is why the generated
  config sets `share_instance = No`.

## Limitations and possible next steps

- Live, direct delivery only — no offline mailbox or store-and-forward layer
  (LXMF would be the natural fit).
- Request/response messaging, not streaming or group chat.
- Peer information is self-asserted and can be stale; prune with
  `tau peers --max-age`.
- No NAT traversal beyond what the configured Reticulum interfaces provide.
- Natural extensions: group destinations, file transfer using Reticulum
  resources, allow-lists, typed messages, LXMF interop, and a shared-instance
  mode for running several local clients against one node.

## Development

```bash
make venv        # create .venv and install the package + dev tools
make test        # unit tests (no network)
make test-all    # unit + two-node TCP integration test
make demo        # run the loopback demo
make lint        # byte-compile every module
```

Layout:

```
tau/
  cli.py        argparse commands and output formatting
  config.py     data paths and Reticulum config generation
  identity.py   identity create/load/save
  node.py       announces, discovery, request handlers, links
  peers.py      persistent peer registry
  protocol.py   JSON wire format, validation and size limits
  util.py       atomic file helpers
tests/          unit tests + optional Reticulum integration test
scripts/demo.py two-node loopback demonstration
web/            Next.js + Tailwind console (static export served by tau)
```

## Troubleshooting

| Symptom | Likely cause / fix |
| --- | --- |
| `peer did not announce within ...` | No common interface, wrong port, firewall, or peer not running. Check `tau discover -v` on both sides. |
| AutoInterface does not work | Multicast is blocked (common in containers). Use TCP loopback/LAN interfaces instead. |
| `Attempt to reinitialise Reticulum` | Two `TauNode`s (or two `tau` commands sharing a process) — use one node per process. |
| `no path to ...` | Reticulum has not learned a route yet. Keep the peer announcing (`tau serve --announce-interval 5`) or wait for a path request to resolve. |
| Noisy output | Reticulum logs go to stderr. Use `-q`, `--rns-log-level 1`, or `--json` for clean stdout. |
| Stale peers | `tau peers --max-age 1`, or delete `peers.json`. |

## License

MIT. Reticulum itself is distributed under its own MIT-style license; see the
[upstream project](https://github.com/markqvist/Reticulum).
