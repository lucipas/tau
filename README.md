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
their latency in real time over server-sent events. Each peer also has an
info button that shows what the peer reports about itself — its tau version,
protocol, uptime, and peer count — so you can see whether it understands
newer entry kinds before sending it a reply or a quote. Message history is kept
in `messages.json` in the data directory, so earlier conversations are still
there after the node restarts.

Read state is per conversation and strictly local: the node inbox keeps a
read cursor per sender, `tau messages` marks entries unread with `●` and
advances the cursor when you read a conversation, and the console shows an
unread badge next to each peer and the unread total in the tab title.
Opening a peer in the console clears its badge; read receipts to the sender
are a separate feature and do not exist yet.

The same bridge is the local integration point for scripts:

| Endpoint | Method | Purpose |
| --- | --- | --- |
| `/api/status` | GET | node info, peers, recent messages |
| `/api/peers` | GET | peer registry |
| `/api/peers/forget` | POST | forget a peer `{"target": "<name or hash>", "force": true}` |
| `/api/peers/alias` | POST | set a peer's local alias `{"destination": "<hash>", "alias": "name"}` (empty alias clears it) |
| `/api/peers/verify` | POST | mark a peer verified `{"target": "alice"}` — only after comparing the safety number out of band (returns it) |
| `/api/peers/safety-number` | POST | show a peer's safety number `{"target": "alice"}` without recording anything |
| `/api/peers/block` | POST | block `{"target": "alice"}` |
| `/api/peers/unblock` | POST | unblock `{"target": "alice"}` |
| `/api/groups` | GET | known group DMs with their member lists |
| `/api/messages` | GET | recent inbound/outbound messages |
| `/api/messages/search` | GET | search stored messages (`?q=<text>`, optional `peer` and `limit`) |
| `/api/messages` | POST | send `{"to": "alice", "text": "hi"}` — or to a group with `{"group": "<id, id prefix or name>", "text": "hi"}` (one outbound entry per member; replies and expiry are peer-DM-only) |
| `/api/messages/export` | GET | stored DMs as the CLI's JSON export document (localhost only) |
| `/api/messages/import` | POST | import a DM export document (localhost only) |
| `/api/files/<sender>/<name>` | GET | download a file received from `<sender>` |
| `/api/react` | POST | react `{"target": "<wire id>", "reaction": "👍"}` or retract `{"target": "<wire id>", "clear": true}` |
| `/api/messages/read` | POST | mark a conversation read `{"peer": "<hash>"}` (omit `peer` for all) |
| `/api/outbox` | GET | queued mail waiting on offline peers |
| `/api/outbox/cancel` | POST | cancel `{"target": "<message id or prefix>"}` |
| `/api/bundle/export` | POST | build a signed sneakernet bundle `{"include_tan": true}` |
| `/api/bundle/import` | POST | merge a bundle document (the signed JSON) into this node |
| `/api/outbox/retry` | POST | retry queued mail now, ignoring backoff (`{"target": "<message id or prefix>"}` for one entry; omit it to retry everything) |
| `/api/outbox/held` | GET | sealed mail held for other peers |
| `/api/outbox/drop-held` | POST | drop `{"target": "<message id or prefix>"}` |
| `/api/ping` | POST | measure a peer's round trip |
| `/api/info` | POST | show a peer's reported info (`{"to": "alice"}`) — version, protocol, uptime, peer count, LXMF state |
| `/api/allowlist` | GET | DM allow-list: whether enforced, and who is on it |
| `/api/allowlist/add` | POST | allow DMs from `{"target": "alice"}` (name, hash or prefix) |
| `/api/allowlist/remove` | POST | stop allowing DMs from `{"target": "alice"}` |
| `/api/allowlist/set-enabled` | POST | turn enforcement `{"enabled": true}` on/off |
| `/api/announce` | POST | announce this node immediately |
| `/api/id/rename` | POST | rename this node `{"name": "..."}` (live name + re-announce) |
| `/api/settings` | GET | node settings (`accept_files`, `send_read_receipts`) |
| `/api/settings` | POST | change one setting `{"setting": "accept_files", "value": true}` |
| `/api/backup` | GET | download a backup archive of the data directory |
| `/api/doctor` | GET | health verdicts for the data directory (`tau doctor`) |
| `/api/tan/timeline` | GET | followed authors' posts (`?all=1` for every cached post) |
| `/api/tan/top` | GET | most-liked stored posts (`?limit=10`, `?author=<name or hash>`) |
| `/api/tan/search` | GET | search stored posts by keyword (`?q=...`, optional `?author=...`, `?limit=N`) |
| `/api/tan/mentions` | GET | replies to your own posts (`?limit=N`) |
| `/api/tan/thread` | GET | thread rooted at a post (`?ref=<hash-or-prefix>`) |
| `/api/tan/following` | GET | followed authors and their post counts |
| `/api/tan/authors` | GET | every known author (`?sort=recent`, `posts`, or `name`; `?limit=N`, `?offset=N`; the response carries the directory `total`) |
| `/api/tan/suggest` | GET | suggested authors to follow (`?limit=5`) |
| `/api/tan/posts` | GET | recent Tan posts received by this node |
| `/api/tan/posts` | POST | publish `{"text": "hello"}` |
| `/api/tan/quote` | POST | quote `{"target": "<post hash>", "text": "…"}` |
| `/api/tan/like` | POST | like `{"target": "<post hash>"}` |
| `/api/tan/unlike` | POST | unlike `{"target": "<post hash>"}` |
| `/api/tan/repost` | POST | repost `{"target": "<post hash>"}` |
| `/api/tan/likes` | GET | who liked a post (`?target=<post hash>`) |
| `/api/tan/delete` | POST | delete one of your own posts |
| `/api/tan/follow` | POST | follow `{"target": "alice"}` |
| `/api/tan/unfollow` | POST | unfollow `{"target": "alice"}` |
| `/api/tan/muted` | GET | muted authors and their post counts |
| `/api/tan/mute` | POST | mute `{"target": "alice"}` |
| `/api/tan/unmute` | POST | unmute `{"target": "alice"}` |
| `/api/tan/muted-keywords` | GET | muted keywords |
| `/api/tan/mute-keyword` | POST | mute a keyword `{"keyword": "crypto"}` |
| `/api/tan/unmute-keyword` | POST | unmute a keyword `{"keyword": "crypto"}` |
| `/api/tan/sync` | POST | pull missing posts from peers |
| `/api/tan/rules` | GET | current Tan admission ruleset and the kinds that can be displayed |
| `/api/tan/rules` | POST | replace the ruleset (applies to the running node at once) |
| `/api/tan/verify` | GET | re-verify every stored post's signature and chain linkage |
| `/api/tan/export` | GET | export stored posts (`?scope=mine` or `?scope=all`) |
| `/api/tan/import` | POST | import an export document (every entry verified) |
| `/api/tan/saved` | GET | saved posts, newest save first (`?limit=` and `?offset=` page through them; the response carries `next_offset`, null when exhausted) |
| `/api/tan/save` | POST | save `{"target": "<post hash>"}` to read later |
| `/api/tan/unsave` | POST | unsave `{"target": "<post hash>"}` |
| `/api/events` | GET | SSE stream of messages, peers and posts |

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

## Social feed: Tan

`tan` is a federated social feed built on the same tau destinations. Every
post is signed by its author, chained to that author's previous post by SHA-256,
and cached in SQLite. Posts can be relayed by any node without giving that node
the author's private key.

```bash
# Terminal 1: serve the feed and the normal tau request handlers
tau serve --social --announce-interval 5

# Terminal 2: write a post and publish it to known peers
tau tan post "hello mesh"

# Read your timeline (your posts plus authors you follow)
tau tan timeline

# Discover cached posts from peers, then follow someone
tau tan sync --explore
tau tan follow alice
tau tan timeline --json
```

`tau serve --social --social-sync-interval 60` also pulls the public timeline
from known peers every minute, so a long-running node keeps its cache fresh.

Useful flags:

```bash
# Store a post without starting Reticulum or contacting peers
tau tan post --no-publish "draft"

# Publish only to one peer
tau tan post --to alice "direct delivery"

# Schedule a post for the morning crowd (publishes while serving with --social)
tau tan post --delay 8h "good morning, mesh"
tau tan scheduled

# Show every cached post, or one author's chain
tau tan timeline --all
tau tan timeline --author alice

# Search stored posts by keyword (local only, newest first)
tau tan search mesh --limit 10
tau tan search mesh --author alice

# Show the most-liked stored posts (best first; unliked posts last, newest first)
tau tan top --limit 10
tau tan top --author alice

# Follow by destination hash or by a unique hash prefix
tau tan follow 5c04c868
tau tan following

# Hide a word or phrase from your local views (whole-word match,
# case-insensitive; entries keep syncing, so unmuting restores them)
tau tan mute-keyword crypto
tau tan mute-keyword "climate change"
tau tan muted-keywords
tau tan unmute-keyword crypto

# Mute an author: their posts leave your timeline, search and threads,
# but keep syncing and storing — unmute to see them again
tau tan mute alice
tau tan muted

# Back up your posts (or every cached author's) and restore them later —
# every entry's signature and hash are re-verified on import
tau tan export --output tan-backup.json
tau tan export --all --output tan-backup.json
tau tan import tan-backup.json
```

More Tan flags — drafts, direct delivery, search, keyword and author
mutes, export/import: see [FLAGS.md](FLAGS.md#tan-flags).

Posts are limited to 2,000 characters and 20 posts per feed response. A
long-running node stores only posts it created, received or synced. Tan is
public-feed oriented: there are no private posts or media yet. Posts can be
answered with `tau tan post --reply-to <post>` — a reply is a signed entry
on the replier's chain pointing at its parent, so it propagates to peers
like any other post. A post can also be shared with your own comment with
`tau tan quote <post> "comment"` — a quote is a signed entry on the
quoter's chain carrying the comment text and the original as its target,
so it appears in followers' timelines as a standalone entry with the
original embedded beneath it. Read a whole conversation with
`tau tan thread <post>` — it prints the post and its replies nested under
it, oldest first, and accepts a reply's hash to show the thread it belongs
to. An author can delete one of their own posts with
`tau tan delete <post>` — the delete is a signed entry on the author's
chain, so it propagates to peers and the post disappears from timelines
(the entry itself stays stored, keeping chains verifiable). Entries are
verified when they are stored, but nothing re-checks the local copy
afterwards: `tau tan verify` re-verifies every stored entry's signature
and hash and checks each author's chain linkage, and exits non-zero
listing any problem it finds — run it after a restore, or whenever you
suspect the database was damaged. Entries whose parent has not arrived
yet are counted, not reported. A word or phrase can be muted locally
with `tau tan mute-keyword <word>` — matching posts disappear from your
timeline and threads (a muted reply hides its whole subtree), while
search still finds them and unmuting restores them. An author can also
be muted with `tau tan mute <peer>` — their posts and replies leave your
timeline, search results and threads, while their entries keep syncing
and storing, so `tau tan unmute <peer>` brings the whole history back.
Muting is display-only and never touches DMs.
In the web console your own posts show a delete button that calls the same endpoint.
The local database is `<data-dir>/tan.sqlite3`.

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
| `tau send --to PEER TEXT` | Send a message over an encrypted link (queued if the peer is offline) |
| `tau ping --to PEER` | Check reachability and measure round-trip time |
| `tau info --to PEER` | Fetch a remote node's information |
| `tau doctor` | Check the local data directory's health (read-only; `--json` for scripting) |
| `tau peers` | List peers from the local registry (`--exchange` asks known peers for the peers they know) |
| `tau peers show NAME\|HASH` | Show what this node knows about one peer (offline, read-only) |
| `tau peers alias PEER ALIAS` | Give a known peer a local alias (unique, never shared with other peers) |
| `tau peers unalias PEER` | Remove a peer's local alias |
| `tau messages` | List received direct messages kept in the node inbox |
| `tau messages prune --older-than 90d` | Delete stored messages older than a cutoff (`--peer`, `--group`, `--dry-run`) |
| `tau note TEXT` | Save a private note to self in the local inbox (never sent; list with `tau messages --kind note`) |
| `tau outbox` | List messages queued for offline peers |
| `tau outbox retry [ID]` | Try delivering queued messages now, ignoring backoff (one message, or the whole queue) |
| `tau outbox held` | List sealed messages held for other peers |
| `tau outbox drop-held ID` | Drop a held message so it is never delivered |
| `tau allowlist` | Show or change who may send this node direct messages |
| `tau outbox cancel ID` | Cancel a queued message (full id or unique prefix) |
| `tau id` | Show this node's name, destination hash, and LXMF delivery address |
| `tau id export --to PATH` | Back up the node identity to a file |
| `tau id import --from PATH` | Restore the node identity from a backup |
| `tau id rename NEWNAME` | Change this node's display name (identity and destination unchanged) |
| `tau tan post TEXT` | Sign, store and publish a social post |
| `tau tan post --reply-to POST TEXT` | Reply to a post (signed entry on your chain) |
| `tau tan post --delay 5m TEXT` | Queue a post to publish in 5 minutes (needs `tau serve --social` running) |
| `tau tan scheduled` | List Tan posts queued for later publishing |
| `tau tan cancel-scheduled ID` | Drop a queued Tan post before it publishes |
| `tau tan like POST` | Like a post (signed reaction on your chain) |
| `tau tan unlike POST` | Remove your like from a post |
| `tau tan repost POST` | Repost a post to your followers (signed entry on your chain) |
| `tau tan delete POST` | Delete one of your own posts |
| `tau tan timeline` | Show your posts and the authors you follow |
| `tau tan thread POST` | Show a post with its replies as a nested thread |
| `tau tan search TEXT` | Search stored posts by keyword (local only) |
| `tau tan top` | Show the most-liked stored posts, best first |
| `tau tan mentions` | Show replies to your own posts |
| `tau tan verify` | Re-verify every stored post's signature and chain linkage |
| `tau tan follow PEER` | Follow a peer or destination hash |
| `tau tan unfollow PEER` | Stop following an author |
| `tau tan following` | List followed authors |
| `tau tan authors` | List every author in the local Tan store |
| `tau tan sync` | Pull missing posts from known peers |
| `tau tan export` | Back up stored Tan posts as a JSON export |
| `tau tan import FILE` | Restore Tan posts from a JSON export |
| `tau tan rules` | Show or change which remote entries get committed |

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

# Enable the social feed and refresh it every minute
tau serve --social --social-sync-interval 60

# Pull the public feed from peers to discover new authors
tau tan sync --explore

# Read the message from a file or stdin
tau send --to alice --file message.txt
echo "piped message" | tau send --to alice

# Fail fast instead of queueing when the peer is offline
tau send --to alice "hi" --no-queue

# Tag a message with a kind (default chat); the receiver can filter by it
tau send --to alice --kind ping "are you there?"
tau messages --kind ping

# When a node is already serving on this data directory, `tau send`
# runs through it over a local socket instead of starting a second
# Reticulum instance on the same identity — same result, same queueing.
# A stale socket left by a crashed serve simply falls back to a local send.

# See what is waiting on offline peers
tau outbox

# Read back messages this node received (newest first)
tau messages
tau messages --peer alice --limit 10 --json

# Only show peers heard from in the last day
tau peers --max-age 1

# Everything as JSON
tau discover --duration 10 --json
tau send --to alice "hi" --json
```

`serve` emits one JSON object per line with `--json` (`ready`, `peer`,
`message` events), which makes it easy to feed into another process.
`discover`, `peers`, `send`, `ping`, `info` and `id` emit a single JSON object.

Full command and flag reference: [FLAGS.md](FLAGS.md).


## Data directory

By default everything lives in `~/.tau` (override with `TAU_HOME` or
`--data-dir`):

```
~/.tau/
├── identity              # private key, chmod 600 — this is your address
├── node.json             # local display name
├── peers.json            # discovered peers, chmod 600
├── mailbox.json          # store-and-forward queues, chmod 600
├── inbox.json            # direct messages + per-peer read cursors, chmod 600, capped at 500 received + 500 sent
├── messages.json         # console message history, chmod 600
└── reticulum/
    ├── config            # generated Reticulum configuration
    └── storage/          # Reticulum internals (paths, ratchets, ...)
```

Back up `identity` if you want to keep the same address:

```bash
tau id export --to ~/tau-identity.backup   # atomic write, chmod 600
tau id import --from ~/tau-identity.backup # into a fresh data directory
```

`export` refuses to overwrite an existing backup unless `--force` is given.
`import` validates the file first, refuses to replace an existing identity
unless `--force` is given, and refuses while a node is serving on the same
data directory. It prints the imported destination hash so you can verify it
matches the old node. Anyone who reads the backup can impersonate the node
and decrypt its traffic, so keep it private.

To back up identity, peer registry, queued outbox
mail, the durable DM inbox, the Tan database and rules, the DM allowlist,
groups, the block list, received files, and the Reticulum config, use
`tau backup`:

```bash
tau backup ~/tau-backup.tar.gz
tau restore ~/tau-backup.tar.gz   # into a fresh data directory
```

The archive is a gzipped tar with a manifest (format version, node name,
tau version, file list); the runtime lock file is never included. The Tan
database is snapshotted with SQLite's backup API, so committed rows still
sitting in the write-ahead log are included. Both
commands refuse while a node is serving on the data directory, so a backup
never catches the mailbox or the Tan database half-written. The web
console's Back up button is the exception: the bridge builds the same
archive from the live node, whose own writes are atomic, so a
console-only user can back up without stopping the node. Restore stays a
terminal operation — stop the node and run `tau restore` — because a
restore replaces the state under the running node. `restore`
validates the archive first — unknown format, unexpected members, absolute
paths and `..` traversal are all rejected — and refuses to overwrite
existing included node state unless `--force` is given; a `--force` restore
also removes included node state the archive does not hold. It leaves
excluded state unchanged. The archive holds the
identity, so keep it as private as the identity file itself.

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
{"v": 1, "name": "alice", "proto": "tau", "caps": ["message", "ping", "peers", "mail"]}
```

Reticulum verifies the signature and distributes the announce through the
mesh. Every tau node that hears it stores the peer in `peers.json` and updates
its path table. A node that is asked to message an unknown destination sends a
Reticulum path request and waits for the announce before giving up.

Announces are not the only way to learn about a peer. After every mail round
with a peer, the node also asks it for the peers *it* knows (`/tau/peers`) and
merges the answer into its own registry — peer exchange. Each exchanged record
carries the peer's public key, and the receiver verifies the key derives the
reported destination hash before trusting it; records for our own destination,
malformed records and records older than the 7-day prune horizon are dropped.
A peer we already know keeps its own liveness record — a third party's claim
never refreshes it, and only a missing name is filled in. A reported name that
collides with another known peer's name is dropped, so a reporter cannot shadow
a peer's name. Peers learned only through exchange are marked as such until
they are heard directly: mail sweeps rank directly heard peers ahead of them,
and the background sweep asks a few quiet known peers for their lists every 15
minutes, so discovery does not depend on announces alone. Peers owed mail or
pending acks are never evicted by the registry cap to make room for gossip.
`tau peers --exchange [--to NAME|HASH]` runs the same exchange on demand.
A peer can also be given a local alias with `tau peers alias PEER ALIAS`:
the alias is shown by `tau peers` (with the peer's asserted name alongside
when they differ), works as a target for `send`, `ping`, `info` and
`--to`, and is never sent to other peers or overwritten when the peer
re-announces. Aliases are unique across the registry, so one alias always
resolves to exactly one peer; an alias that looks like a destination hash
or hash prefix (four or more hex characters) is rejected, so an alias can
never be mistaken for a hash target. A running node picks up aliases set
while it serves at its next registry save. Remove one with
`tau peers unalias PEER`. The DM allow-list and Tan author resolution are the exceptions to alias
resolution: `tau allowlist add` matches asserted names, hashes, and hash
prefixes only, and lists asserted names, and `tau tan follow`/`unfollow`
resolve authors the same way, so a local label can never stand in for
who was actually allowed or followed. If a registry file ever holds
the same alias twice (two writers racing), the first peer keeps it and
the other is dropped on load, so an alias always resolves to exactly
one peer.

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
| `/tau/peers` | `{}` | up to 100 peer records, each with its public key (verified against its destination hash on receipt) |
| `/tau/mail/store` | `{"type": "mail_store", "to": ..., "id": ..., "blob": ..., "expires_at": ...}` | hold a sealed message for an offline peer |
| `/tau/mail/fetch` | `{"type": "mail_fetch", "ack": [...]}` | hand over held mail for the caller, dropping acked ids |

### Store-and-forward

Direct delivery needs both nodes online at once. When a send cannot reach
its peer, the message is not lost — it lands in the sender's **outbox**
(`mailbox.json`, kept for 7 days) and is delivered automatically the next
time the recipient is heard from. While it waits, the sender also leaves
**sealed copies** with up to three reachable peers: the message is encrypted
to the recipient and signed by the sender, so a holder learns only the
recipient hash, the message id and the expiry — never the content, and it
cannot forge a message in the sender's name. When the recipient later meets
any holder, it fetches its envelopes, verifies and decrypts them locally,
and acks them so holders can drop their copies. Every message carries a
unique id, and each node remembers ids it has already shown, so direct,
retried and relayed copies of the same message are delivered to the user
exactly once. Holders cap what they keep (512 envelopes total, 64 per
recipient) and everything expires. A holder also caps each depositing
peer — 16 envelopes / 128 KiB from an unknown peer, 128 / 1 MiB from a
long-standing known peer with a delivered history (in the registry 24h+,
heard directly, and at least one message this node has delivered to
them) — shedding that sender's oldest envelopes first, so one sender
can crowd out only its own mail; `tau outbox held`
shows the per-sender usage. Failed delivery attempts back off
exponentially (60s, doubling, capped at an hour), so an unreachable peer
does not cost a link attempt on every sweep; a peer announcing itself
retries immediately. `tau outbox` shows what is waiting, including the
next retry time; `tau outbox held` shows the sealed envelopes this node
is holding for other peers, and `tau outbox drop-held ID` drops one (a
copy already fetched by the recipient may still be delivered). A drop
is remembered until the envelope would have expired, so a sender
retrying its deposit cannot resurrect the copy. `tau outbox cancel ID`
takes a queued message back — it stops this node's future delivery
attempts and deposits, but sealed copies already held by other peers
may still reach the recipient, since the sender cannot recall them.
`tau outbox retry [ID]` forces a delivery attempt now instead of waiting
out the backoff — for one queued message by id or prefix, or for the
whole queue — and reports what was delivered, deposited and received.
`tau send --no-queue` restores the old fail-fast behaviour. A message
sent from the web console shows as `queued` while it waits, and flips
to `delivered` in the feed as soon as the sweep delivers it.

Disappearing messages: `tau send --to PEER --expire 1h TEXT` marks a DM
ephemeral (durations like `30s`, `5m`, `2h`, `7d`; at most 30d). The
expiry travels inside the encrypted payload — and inside the signed
sealed envelope for relayed mail, so a holder cannot extend it — the
receiver shows the deadline and purges the message once it passes, and
a queued copy (and any sealed copy deposited from it) expires on the
same schedule. The web console composer has the same picker. Honest
caveat: expiry is cooperative — a modified client can keep copies, so
this is hygiene, not a guarantee. Tan posts are public signed chains
and never expire; tombstones already cover their removal.

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
| Mail store/fetch request | 24 KiB |
| Sealed message blob | 12 KiB |
| Outbox / held envelopes | 256 / 512 (64 per recipient) |
| Held envelopes per sender | 16 envelopes / 128 KiB unknown, 128 / 1 MiB known |
| Queued message lifetime | 7 days |

Names and messages are stripped of control characters (including terminal
escape sequences) before display. Received messages are recorded in the
node inbox (`inbox.json`, mode `0600`, capped at 500 received plus 500
sent — sent messages keep their own allowance, so they never evict
received history) as they arrive — direct and relayed alike — and can
be read back with `tau messages`; a message waiting in the outbox is
stored in `mailbox.json` (mode `0600`) until it is delivered or expires.

### Reply quoting

`tau send --reply-to <id-prefix>` quotes an earlier message in the same
conversation: the DM carries an `in_reply_to` reference (the quoted
message's id plus a short hash of the text the sender quoted), and the
reference travels the direct and the store-and-forward paths alike. The
web console has the same action — a ↩ reply button on each message.
The quoted text is never trusted from the wire: the renderer pulls it
from the local history (the node's inbox, which records sent messages
too, or the console's `messages.json`) and shows "original not
available" when the original is not there — it arrived before this
node's history began, or was already pruned — and a reference to
another peer's message never resolves.

### DM reactions

A DM reaction is its own message kind (`/tau/react`), not a text message:
`tau react <message-id> <emoji>` sends one emoji or short text token
(`👍`, `+1`, `lol` — at most 32 characters, never an image) naming the
message it answers, and `tau react <message-id> --clear` retracts it. The
payload is signed with the reactor's Reticulum identity and verified by
the receiver against the link identity — a reaction carries its own
proof of attribution, like a sealed mail envelope, so a relayed or
replayed reaction still has to prove its reactor. The receiver resolves
state per (message, reactor) with the latest reaction winning, ordered
by the reactor's `sent_at` — the same derivation Tan likes use, so
re-sending a reaction never double-counts and a late older copy cannot
override a newer one or resurrect a retraction. Reactions render under
the message in `tau messages` and in the web console, and `POST
/api/react` sends one from the console's API.

Reactions are delivered live in v1: the peer must be online, because the
sealed-mail path is text-shaped throughout and cannot carry a reaction
envelope yet. Queueing reactions for offline peers is a follow-up. A
reaction whose target this node has never seen is still stored (the target
may arrive later over a relay), but it is only rendered under a message the
inbox actually holds.

### DM retracts

`tau retract <message-id>` takes back a message you sent: it sends a
signed retract (`/tau/dm/retract`) to the recipient, who deletes their
stored copy, then deletes your own sent copy — so the message is gone
on both sides. The payload is signed with the sender's Reticulum
identity and verified by the receiver against the link identity, like a
reaction, and the receiver only deletes a message the caller actually
sent: a retract from anyone else fails with `not_sender`, and a retract
for an id the receiver never held answers ok with nothing deleted, so
retries are safe. A retract also leaves a marker keyed by (sender,
message id) that lives for the message's delivery lifetime (7 days),
unless the marker cap is reached first — at the cap the
earliest-expiry markers are evicted, even unexpired ones — so a
sealed relay copy that arrives after the retract — or after the
id leaves the receiver's seen cache — is dropped instead of stored,
and `tau messages import` skips the message instead of resurrecting
it. Group messages cannot be retracted in v1 — a group
message lives in every member's inbox, and deleting it from one member
alone would silently half-retract it.

Retracts are delivered live in v1, like reactions: the peer must be
online. A message still queued in the outbox is cancelled with
`tau outbox cancel` instead. A peer running a version that predates
retracts rejects the unknown path, so a retract to an old peer fails
instead of silently succeeding.

### DM edits

`tau edit <message-id> <text>` fixes a message you already sent, in place:
it sends a signed `/tau/dm/edit` request carrying the target message id
and the replacement text — the same signed-envelope pattern as a reaction,
verified by the receiver against the link identity. The receiver applies
it only when the editor is the message's original sender, and only when
the edit is newer than the last one applied, so a replayed older edit
cannot rewind a newer one and a retried edit is safe. The stored text is
replaced and the message is marked edited (`edited_at`); `tau messages`
shows an `(edited)` marker, and only the latest text is kept — there is
no edit history. A group message is refused: group messages have a
different id on every member's copy, so one edit cannot name them all.

Edits are delivered live in v1, like reactions: the peer must be online.
A peer running a version that predates edits rejects the unknown path,
so an edit to an old peer fails instead of silently succeeding.

### Sneakernet bundles

When two nodes share no network at all, mail can still travel on a
file. `tau bundle export FILE` writes a signed bundle containing the
sealed envelopes this node is holding for other peers, plus its queued
outbox messages sealed fresh to each recipient (an outbox message whose
recipient's key is not in the peer registry is skipped and reported,
never written in the clear). With `--tan` it also includes the 256 most
recent Tan posts from every stored author, so a long history cannot
crowd the bundle past its size cap. `tau bundle import FILE` on another
node verifies the exporter's signature, then merges the contents
through the normal checks: sealed envelopes become held mail under the
same caps and dedupe rules, and Tan posts first pass the importer's
admission rules — a post from a blocked author is reported as skipped
and never stored — then go through the usual verify path. A tampered bundle fails signature verification
and nothing inside it is applied; bundles over the size cap (8 MiB by
default, `--max-bytes N` on either command) are refused. The import
reports what was accepted, already present and skipped. No identity or
private material is ever included, and there is no auto-import — a
bundle only moves when someone carries the file and runs the command.
The web console offers the same flow in its Sneakernet section
(`POST /api/bundle/export`, `POST /api/bundle/import`): export downloads
the signed bundle as a file, import takes a bundle file and reports the
merge.

### Tan posts

A Tan post is a small JSON object:

```json
{
  "v": 1,
  "author": "<tau destination hash>",
  "key": "<author public key>",
  "name": "alice",
  "text": "hello mesh",
  "created_at": 1760000000.0,
  "prev": "<hash of alice's previous post, or empty>",
  "hash": "<sha256(prev || canonical post body)>",
  "sig": "<Ed25519 signature over hash>"
}
```

The hash chain is per author: the first post has an empty `prev`, and every
later post points at the previous one. Editing a post changes its hash, and a
gap or reordering is visible in the chain. Because the signature covers the
hash and the public key derives the author's destination, a relayed post can be
verified even when the relay is not its author.

Two request paths are added when a node runs with `--social`:

| Path | Request | Response |
| --- | --- | --- |
| `/tan/feed` | `{"author": "...", "after_seq": 0, "limit": 20}` or `{"all": true}` | cached posts |
| `/tan/post` | `{"post": {...}, "ttl": 2}` | store result |

`tan post` publishes to known peers, and `/tan/post` relays new posts to a
small, random fan-out. `tan sync` is pull-based: it asks peers for posts after
the highest sequence it already has, so an author can be offline as long as any
peer still caches their feed. Posts that arrive before their parent are stored
as orphans and linked automatically when the parent arrives.

### Likes

A like is not a counter stored anywhere: it is a signed *reaction envelope*
appended to the reactor's own chain, committed exactly like a post. The
envelope carries two extra signed fields — `"kind": "like"` (or `"unlike"`)
and `"target": "<hash of the post>"` — and no text. Reactions gossip and sync
with the rest of the chain, and a post's like count is derived from them: for
each reactor, their latest like/unlike entry for that post decides whether
their like stands, so liking twice never counts twice and an unlike is just a
later entry superseding an earlier one.

```bash
tau tan like 5c04c868          # by post hash or unique prefix
tau tan unlike 5c04c868
tau tan timeline               # shows ♥ counts under each post
tau tan top                    # ranks stored posts by ♥ count
tau tan likes 5c04c868         # names the authors whose like currently stands
```

The timeline only shows counts. `tau tan likes` prints the post followed by
one line per liker; an author who unliked, or whose like was superseded by a
later unlike, does not appear. Like the thread view, it refuses a post your
Tan display rules hide.

The web console shows the same counts with a ♥ toggle on every post
(`POST /api/tan/like` / `POST /api/tan/unlike`). Saved posts work the
same way: `tau tan save` keeps a post to read later (local only, never
published), and the console has a Saved view with a save toggle on every
post (`GET /api/tan/saved`, `POST /api/tan/save` / `POST /api/tan/unsave`).

### Admission rules

By default a node commits any well-signed entry a peer pushes to it. The
owner can decide otherwise with an admission ruleset, stored as
`tan-rules.json` in the data directory and managed via the CLI:

```bash
tau tan rules                                  # show the current ruleset
tau tan rules --default deny                   # admit nobody by default
tau tan rules --allow-author alice             # ...except alice
tau tan rules --followed-only                  # ...and authors you follow
tau tan rules --block-author 5c04c868          # never admit this author
tau tan rules --kinds post                     # display posts only
```

Rules are evaluated for remote entries only — pushes to `/tan/post` and
posts imported by `tan sync` — after the entry's signature has been
verified, so an author identity cannot be spoofed. Order: the node's own
author is always admitted, the block list denies, the allow list and
(optionally) followed authors admit, and everything else falls to the
default policy. Entries you create yourself are never filtered, and with
no rules file everything is admitted, as before. A rules file that
cannot be parsed fails closed (remote entries denied) rather than
silently opening the node up. The web console's admission-rules section
(`GET`/`POST /api/tan/rules`) applies a change to the running node at
once; rules saved from the CLI apply when `serve` restarts.

### DM allow-list

Direct messages are open by default, like Tan posts. The owner can
restrict them to known senders with an allow-list, stored as
`allowlist.json` in the data directory (mode `0600`) and managed via
the CLI or the web console's allow-list section (console changes apply
to the running node immediately; CLI changes apply when it restarts):

```bash
tau allowlist list             # show whether the list is enforced and who is on it
tau allowlist add alice        # allow DMs from alice (name, hash or hash prefix)
tau allowlist remove alice     # stop allowing DMs from alice
tau allowlist on               # enforce the list: unlisted senders get not_allowed
tau allowlist off              # stop enforcing; anyone may message again
```

When enforced, a message from an unlisted sender is rejected with a
`not_allowed` error and is never stored, never marked seen, and never
passed to the message callback — so admitting the sender later and
having them resend delivers it normally. The same check applies to
messages delivered through store-and-forward: a sealed copy from an
unlisted sender is dropped on collection, not delivered. An explicit
denial is also not queued: `tau send` to a node that rejects you
fails with an error instead of leaving a message in the outbox to
retry until its TTL expires. The sender's identity is the verified
destination hash from the request, so it cannot be spoofed. A list
file that cannot be parsed fails closed (nobody admitted) rather than
silently opening the node up. A running `serve` applies saved changes
when it restarts.

### DM block-list

The allow-list is all-or-nothing; a node running open still needs a
way to silence one abusive or mistaken peer. The block-list is a
deny-list, stored as `blocklist.json` in the data directory (mode
`0600`) and managed via the CLI:

```bash
tau block list               # show who is blocked
tau block alice              # block alice (name, hash or hash prefix)
tau unblock alice            # unblock alice
```

(`tau block add alice` and `tau block remove alice` work too, matching
the `tau allowlist` shape; a peer literally named `list`, `add` or
`remove` is blocked via `block add <name>`.)

The block check runs before the allow-list check, so blocking an
allow-listed peer wins. A blocked sender's direct messages are
dropped silently — never stored, never marked seen, never passed to
the message callback — and the sender gets only a generic delivery
failure, never a signal that they are blocked. The same silent drop
applies to their messages arriving through store-and-forward. A
sender who never identifies on the link is blocked too while the
list has entries — they cannot prove they are not the peer the owner
silenced (honest tau nodes always identify); with an empty list,
DMs stay open as before. Their
Tan posts stay in the chain store (chain integrity for other peers is
unaffected) but are hidden from the local timeline, feed, search and
thread views, and a reply to one of their posts shows only "a blocked
author's post". `tau messages` and the console hide blocked senders'
history locally without deleting it, so unblocking restores the view;
the console peers view marks blocked peers. A list file that cannot
be parsed fails closed (everybody blocked) rather than silently
admitting a sender the owner meant to silence — and while it stays
unusable, the local views hide everything too, instead of resurfacing
history the owner had hidden. A running `serve`
applies saved changes when it restarts.

`--kinds` is display-only, on purpose: it selects which entry kinds
your timeline views present (hiding reactions also hides their like
counts), but every kind from an admitted author is still stored.
Filtering kinds at admission time would punch holes in an author's
hash chain — later entries would point at a missing parent, sit
unlinked, and stall sync at the gap.

## Security model

- **Confidentiality and integrity** come from Reticulum: links are encrypted
  and authenticated, announces are signed, and identities are verified.
- **No accounts or central authority.** Trust is key-based. Names can collide,
  so compare destination hashes out of band when it matters.
- **Open by default.** The request handlers use `ALLOW_ALL`: any node that can
  reach the destination may send a message or a Tan post. This is appropriate
  for a mesh chat. Owners who want to restrict direct messages can turn on
  the DM allow-list (`tau allowlist on`, see below); Tan posts are governed
  separately by the admission ruleset.
- **Posts are signed and hash-chained.** Tan validates every post before it is
  stored, rejects edits, impersonation and broken chains, and can verify posts
  that were relayed by a third party. Names are still self-asserted.
- **Not anonymous.** The destination hash is a stable pseudonym. Peers can
  correlate your identity across sessions and transports.
- **Store-and-forward is sealed.** Queued messages wait in the sender's
  outbox in plaintext (`mailbox.json`, mode `0600` — protect it like the
  peer registry). Copies left with other peers are encrypted to the
  recipient and signed by the sender: a holder sees only the recipient
  hash, an id and an expiry, cannot read the content, and cannot forge a
  message, because the recipient verifies the signature against the
  sender key inside the envelope.
- **Identity is the trust anchor.** Protect `identity`; it is stored with mode
  `0600`. The peer registry is also `0600` and contains only public keys and
  announce metadata.
- **One Reticulum instance per process.** Reticulum is a process-wide
  singleton, so a process runs at most one tau node. This is why the generated
  config sets `share_instance = No`.

## Limitations and possible next steps

- Store-and-forward is best-effort: a queued message is delivered when the
  sender next hears from the recipient or from a peer holding a sealed
  copy, and it expires after 7 days undelivered. There is no guarantee a
  holder will ever meet the recipient.
- Request/response messaging, not streaming. Group DMs fan one message out
  per member over the existing encrypted links (no MLS or group ratcheting);
  membership is fixed at creation, a changed roster is a new group, and a
  peer running a version that predates groups rejects `/tau/group` requests.
  The group id is derived from the creator, name and roster, so a roster
  cannot be forked under an existing id; a peer can still invent a fresh
  group that includes you, gated by the DM allow-list like a direct message.
  Members learn a group from the first message sent to it, and the console
  is read-only for groups — composing from the console comes later.
  Manage groups with `tau group create NAME --peer ...`, `tau group list`,
  `tau group send GROUP TEXT`, `tau group add-member` / `remove-member`,
  `tau group rename GROUP NEW-NAME` (creator only; a rename is a successor
  group, so members learn the new name — and retire the old one — from the
  next message sent to it, and `tau messages --group` history spans the
  rename), and `tau group delete`.
- Tan is a public timeline: no private posts or media yet.
  Authors can reply to posts, edit their own posts, and delete their own posts. It is not a full ActivityPub-style
  federation protocol.
- Replies are a new entry kind: a peer running a version that predates
  replies rejects them, and — because entries are hash-chained — a
  rejected reply also leaves that author's later entries (including
  deletes) unlinked on the old peer until it upgrades and resyncs.
  Edits are a new entry kind with the same caveat: an old peer rejects
  an edit and leaves the author's later entries unlinked until it
  upgrades, so it keeps showing the pre-edit text meanwhile.
- Quotes are a new entry kind too: a peer running a version that predates
  quotes rejects them, and the same hash-chain effect applies — the rejected
  quote leaves that author's later entries (including deletes) unlinked on the
  old peer until it upgrades and resyncs. Upgrade and resync older peers before
  exchanging quotes.
- A node only has the posts it created, received or synced. If every peer that
  cached an author's feed disappears, the feed is gone until the author returns.
  Back up your own chain with `tau tan export` and restore it with
  `tau tan import` — every entry is re-verified on import.
- Peer information is self-asserted and can be stale; prune with
  `tau peers --max-age`. Safety numbers are the check against that: a
  destination hash is derived from its public key, so a peer whose key
  ever changes under the same destination is tampering or corruption —
  the registry keeps the recorded key, warns loudly, and marks the peer
  in `tau peers` and the console until you re-verify it out of band
  (`tau peers verify`).
- No NAT traversal beyond what the configured Reticulum interfaces provide.
- File transfer is direct links only: a file goes over an RNS Resource
  after a signed offer, at most 10 MiB, and only to a node that opted
  in (or allow-listed the sender). There is no store-and-forward for
  files yet — the recipient must be online — and no resume: a failed
  transfer restarts from the beginning.
- Natural extensions: group destinations, allow-lists, LXMF interop, a shared-instance mode
  for running several local clients against one node, and richer Tan clients
  (media, moderation, follow recommendations). Direct messages can already
  carry a kind (`tau send --kind`, default `chat`); every kind is
  delivered, queued and stored exactly like chat.
- LXMF addressing is in place: every peer's LXMF delivery address is
  derived from its public key and shown by `tau peers` (and the web
  console); `tau id` shows this node's own address. `tau serve --lxmf`
  (requires the `lxmf` extra) also runs an LXMRouter on the node's
  identity, registers its delivery destination and announces it.
  `tau serve --lxmf --lxmf-propagation` also serves as an LXMF
  propagation node, holding mail for offline recipients until they
  fetch it (message store capped at 256 MB). Nothing is sent over
  LXMF yet — sending and the migration of store-and-forward follow
  under #67.

## Development

```bash
make venv        # create .venv and install the package + dev tools
make test        # unit tests (no network)
make test-all    # unit + two-node TCP integration test
make demo        # run the loopback messaging demo
make social-demo # run the loopback Tan social demo
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
  tangents/
    tan.py      signed, hash-chained social feed and its network app
tests/          unit tests + optional Reticulum integration test
scripts/demo.py two-node messaging demonstration
scripts/social_demo.py two-node Tan social demonstration
web/            Next.js + Tailwind console (static export served by tau)
```

## Troubleshooting

| Symptom | Likely cause / fix |
| --- | --- |
| `peer did not announce within ...` | No common interface, wrong port, firewall, or peer not running. Check `tau discover -v` on both sides. |
| AutoInterface does not work | Multicast is blocked (common in containers). Use TCP loopback/LAN interfaces instead. |
| `Attempt to reinitialise Reticulum` | Two `TauNode`s (or two `tau` commands sharing a process) — use one node per process. |
| `another tau node is already serving on data directory ...` | A `tau serve` already holds that data directory's `node.lock`; two serving nodes on one directory would corrupt the shared peer registry, mailbox and Tan database. Stop the running serve (the message names its pid) or use a different `--data-dir`. One-shot commands (`send`, `tan post`, …) are unaffected — they share the directory briefly by design. |
| `no path to ...` | Reticulum has not learned a route yet. Keep the peer announcing (`tau serve --announce-interval 5`) or wait for a path request to resolve. |
| Noisy output | Reticulum logs go to stderr. Use `-q`, `--rns-log-level 1`, or `--json` for clean stdout. |
| Stale peers | Peers unseen for over 7 days are pruned automatically when the registry is saved (peers with queued mail and verified peers are kept). `tau peers --max-age 1` lists only peers seen in the last day. Remove one now with `tau peers forget PEER` — it refuses while mail or acknowledgements are owed, or while a node is serving on the data directory. |

## License

MIT. Reticulum itself is distributed under its own MIT-style license; see the
[upstream project](https://github.com/markqvist/Reticulum).
