# tau command and flag reference

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
| `tau send --to PEER [--to PEER ...] TEXT` | Send a message over an encrypted link (queued if the peer is offline; `--to` is repeatable) |
| `tau send --to PEER --reply-to ID TEXT` | Reply to an earlier message in the conversation, quoting it |
| `tau react MESSAGE-ID EMOJI` | React to a received message (one emoji or short token; `--clear` retracts) |
| `tau send-file --to PEER PATH` | Send a small file (at most 10 MiB) over an RNS Resource on a direct link |
| `tau config accept-files on\|off` | Opt in or out of receiving files (off by default) |
| `tau ping --to PEER` | Check reachability and measure round-trip time |
| `tau info --to PEER` | Fetch a remote node's information |
| `tau peers` | List peers from the local registry (`--exchange` asks known peers for the peers they know; `--safety-number` shows each peer's pairwise safety number) |
| `tau peers verify NAME\|HASH` | Record that the safety number was compared with a peer out of band and matched — the peer is marked verified and kept through pruning |
| `tau peers forget NAME\|HASH` | Remove a peer from the local registry (the next genuine announce re-adds it; the recovery path for a corrupt record — refuses while mail is queued unless `--force`). Stop a running `tau serve` first: its in-memory registry would restore the peer on its next save. |
| `tau messages` | List received direct messages kept in the node inbox (`--group ID` filters to one group; `--unread` for only unread, `--count` for per-peer unread counts; listing marks shown messages read) |
| `tau group create NAME --peer PEER --peer PEER` | Create a private group DM with a fixed member list (the creator is always a member) |
| `tau group list` | List known groups |
| `tau group delete GROUP` | Delete a group (frees capacity when a group cap is reached) |
| `tau group send GROUP TEXT` | Send a message to every member of a group (queued per member when offline) |
| `tau outbox` | List messages queued for offline peers |
| `tau outbox retry [ID]` | Try delivering queued messages now, ignoring backoff (one message, or the whole queue) |
| `tau outbox held` | List sealed messages held for other peers |
| `tau outbox drop-held ID` | Drop a held message so it is never delivered |
| `tau allowlist` | Show or change who may send this node direct messages |
| `tau block` | Silence a peer: drop their DMs and hide their Tan posts |
| `tau outbox cancel ID` | Cancel a queued message (full id or unique prefix) |
| `tau id` | Show this node's name, destination hash, LXMF delivery address, and safety number (fingerprint of this identity) |
| `tau id export --to PATH` | Back up the node identity to a file |
| `tau id import --from PATH` | Restore the node identity from a backup |
| `tau tan post TEXT` | Sign, store and publish a social post |
| `tau tan post --reply-to POST TEXT` | Reply to a post (signed entry on your chain) |
| `tau tan quote POST TEXT` | Quote a post with your own comment (signed entry on your chain) |
| `tau tan like POST` | Like a post (signed reaction on your chain) |
| `tau tan unlike POST` | Remove your like from a post |
| `tau tan delete POST` | Delete one of your own posts |
| `tau tan timeline` | Show your posts and the authors you follow |
| `tau tan thread POST` | Show a post with its replies as a nested thread |
| `tau tan search TEXT` | Search stored posts by keyword (local only) |
| `tau tan mentions` | Show replies to your own posts |
| `tau tan follow PEER` | Follow a peer or destination hash |
| `tau tan unfollow PEER` | Stop following an author |
| `tau tan following` | List followed authors |
| `tau tan mute-keyword WORD` | Hide posts containing a word or phrase from local views |
| `tau tan unmute-keyword WORD` | Stop hiding posts containing a word or phrase |
| `tau tan muted-keywords` | List muted keywords |
| `tau tan mute PEER` | Hide an author's posts from local views |
| `tau tan unmute PEER` | Show a muted author's posts again |
| `tau tan muted` | List muted authors |
| `tau tan save POST` | Save a post to read later (local only, never published) |
| `tau tan unsave POST` | Remove a post from your saved list |
| `tau tan saved` | List your saved posts, newest first |
| `tau tan sync` | Pull missing posts from known peers |
| `tau tan export` | Back up stored Tan posts as a JSON export |
| `tau tan import FILE` | Restore Tan posts from a JSON export |
| `tau tan rules` | Show or change which remote entries get committed |
| `tau bundle export FILE [--tan]` | Write sealed mail (and optionally Tan posts) to a signed bundle file for offline carrying |
| `tau bundle import FILE` | Merge a bundle file into this node's mailbox and Tan store |

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

# Wait up to 30 seconds for a queued message to be delivered
# (exits non-zero if it is still queued when the wait expires)
tau send --to alice "hi" --wait 30

# Send a small file (at most 10 MiB) over a direct link. The receiver
# first gets a signed offer (name, size, SHA-256) and may refuse it;
# files are never queued and never stored-and-forwarded. A node only
# accepts files when its owner opted in — or when the sender is on
# its DM allow-list — and received files land in
# <data-dir>/files/<sender>/, verified against the offered hash.
# The opt-in runs on the RECEIVER's node, not the sender's:
tau config accept-files on

# Then, from the sender's node:
tau send-file --to alice photo.png

# Send the same message to several peers (one message id per recipient)
tau send --to alice --to bob "hi"

# See what is waiting on offline peers
tau outbox

# Read back messages this node received (newest first)
tau messages
tau messages --peer alice --limit 10 --json

# Group DMs: fixed membership, no rekeying — a changed roster is a new group
tau group create weekend --peer bob --peer carol
tau group send weekend "who is driving?"
tau messages --group weekend
tau group list

# Reply to an earlier message in the conversation (message id or prefix)
tau send --to alice --reply-to a1b2c3d4 "seconded"

# Only what arrived since you last read, and per-peer unread counts
tau messages --unread
tau messages --count            # read-only, for scripts; does not mark read

# React to a received message (id or unique prefix from `tau messages`)
tau react a1b2c3d4 👍
tau react a1b2c3d4 +1
tau react a1b2c3d4 --clear

# Only show peers heard from in the last day
tau peers --max-age 1

# Compare safety numbers with a peer over another channel (a call, in
# person), then record the match — verified peers show a ✓ in `tau peers`
# and the console, and are never pruned away
tau peers --safety-number
tau peers verify alice

# Importing a different identity clears every verification: the numbers
# were compared under the old identity, so they mean nothing under the
# new one — re-verify each peer

# Everything as JSON
tau discover --duration 10 --json
tau send --to alice "hi" --json
```

`serve` emits one JSON object per line with `--json` (`ready`, `peer`,
`message` events), which makes it easy to feed into another process.
`discover`, `peers`, `send`, `ping`, `info` and `id` emit a single JSON object.

## Tan flags

```bash
# Store a post without starting Reticulum or contacting peers
tau tan post --no-publish "draft"

# Publish only to one peer
tau tan post --to alice "direct delivery"

# Show every cached post, or one author's chain
tau tan timeline --all
tau tan timeline --author alice

# Search stored posts by keyword (local only, newest first)
tau tan search mesh --limit 10
tau tan search mesh --author alice

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
