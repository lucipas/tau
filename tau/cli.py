"""Command line interface for tau."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import signal
import socket
import sqlite3
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from . import __version__
from .allowlist import (
    AllowListError,
    clean_destination,
    load_allowlist,
    load_allowlist_or_deny_all,
    save_allowlist,
)
from .backup import create_backup, restore_backup
from .blocklist import (
    BlockListError,
    load_blocked_set,
    load_blocklist,
    save_blocklist,
)
from .bundle import (
    DEFAULT_MAX_BUNDLE_BYTES,
    apply_bundle,
    build_bundle,
    parse_bundle,
    serialize_bundle,
)
from .config import (
    TauConfig,
    accept_files_enabled,
    load_node_name,
    send_read_receipts_enabled,
    set_node_setting,
    write_node_metadata,
    write_rns_config,
)
from .doctor import all_ok, run_checks
from .identity import (
    IdentityError,
    _identity_lock,
    destination_hash,
    load_identity,
    load_or_create_identity,
    save_identity,
)
from .groups import GroupStore
from .inbox import Inbox, InboxError
from .lxmf import lxmf_delivery_hash
from .mailbox import Mailbox, MailboxError
from .node import (
    InstanceAlreadyRunning,
    InstanceLock,
    NodeError,
    TauNode,
    check_tcp_listen_ports,
    instance_lock_holder,
)
from .peers import Peer, PeerError, PeerRegistry, safety_number
from .protocol import DEFAULT_TCP_PORT as PROTOCOL_TCP_PORT
from .protocol import (
    DEFAULT_MESSAGE_KIND,
    MAX_MAIL_TTL,
    PROTOCOL_VERSION,
    ProtocolError,
    clean_in_reply_to,
    clean_reaction,
    excerpt_hash,
    normalize_message,
    normalize_message_kind,
    sanitize_name,
    short_hash,
)
from .util import atomic_write_bytes, atomic_write_text
from .tangents.rules import RulesError, Ruleset, load_ruleset, save_ruleset
from .tangents.tan import (
    DEFAULT_FEED_LIMIT,
    DEFAULT_SUGGEST_LIMIT,
    MAX_GOSSIP_TTL,
    TanApp,
    TanError,
    _author_from_identity,
    annotate_quote_counts,
    annotate_quotes,
    annotate_reactions,
    annotate_replies,
    annotated_saved_posts,
    apply_display_rules,
    author_stats,
    author_timeline,
    export_entries,
    follow,
    following,
    import_entries,
    init_db,
    likers,
    list_authors,
    mentions,
    mute,
    mute_keyword,
    muted,
    muted_keywords,
    open_db_readonly,
    open_readonly_db,
    resolve_author,
    resolve_cached_author,
    resolve_delete_target,
    resolve_muted_name,
    resolve_post,
    save_post,
    search_posts,
    suggest_authors,
    thread,
    timeline,
    top_posts,
    unfollow,
    unmute,
    unmute_keyword,
    unsave_post,
    verify_store,
)

log = logging.getLogger(__name__)

DEFAULT_ANNOUNCE_INTERVAL = 300.0

_EXIT_OK = 0
_EXIT_ERROR = 1
_EXIT_INTERRUPTED = 130


# -- argument parsing ------------------------------------------------------


def _add_common_options(parser: argparse.ArgumentParser) -> None:
    """Options shared by every subcommand.

    ``SUPPRESS`` defaults let the same options appear before *or* after the
    subcommand without the sub-parser clobbering values from the main parser.
    """
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=argparse.SUPPRESS,
        help="tau data directory (default: $TAU_HOME or ~/.tau)",
    )
    parser.add_argument(
        "--rns-config",
        type=Path,
        default=argparse.SUPPRESS,
        help="Reticulum config directory (default: DATA_DIR/reticulum)",
    )
    parser.add_argument(
        "--name",
        default=argparse.SUPPRESS,
        help="node name announced to peers",
    )
    parser.add_argument(
        "--rns-log-level",
        type=int,
        default=argparse.SUPPRESS,
        metavar="0-8",
        help="Reticulum log level (default: 2, or higher with -v)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=argparse.SUPPRESS,
        help="increase log verbosity (repeatable)",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        default=argparse.SUPPRESS,
        help="only log errors",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        default=argparse.SUPPRESS,
        dest="json_output",
        help="emit machine readable JSON",
    )


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    _add_common_options(common)

    parser = argparse.ArgumentParser(
        prog="tau",
        description=(
            "Peer-to-peer messaging over the Reticulum Network Stack. "
            "Every node gets an encrypted, self-authenticating address and "
            "discovers peers through signed announces."
        ),
    )
    _add_common_options(parser)
    parser.add_argument(
        "--version",
        action="version",
        version=f"tau {__version__} (protocol {PROTOCOL_VERSION})",
    )

    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")
    subparsers.required = True

    init = subparsers.add_parser(
        "init",
        parents=[common],
        help="create a data directory, identity and Reticulum config",
    )
    init.add_argument(
        "--tcp-listen",
        nargs="?",
        const=str(PROTOCOL_TCP_PORT),
        metavar="[HOST:]PORT",
        help="add a TCP server interface (default port: %d)" % PROTOCOL_TCP_PORT,
    )
    init.add_argument(
        "--tcp-connect",
        metavar="HOST:PORT",
        help="add a TCP client interface",
    )
    init.add_argument(
        "--no-auto-interface",
        action="store_true",
        help="do not enable Reticulum's AutoInterface (LAN discovery)",
    )
    init.add_argument(
        "--init",
        action="store_true",
        help="write default Reticulum config (overwrites)",
    )
    init.set_defaults(func=cmd_init)

    serve = subparsers.add_parser(
        "serve",
        parents=[common],
        help="announce this node and answer peer requests",
    )
    serve.add_argument(
        "--announce-interval",
        type=float,
        default=DEFAULT_ANNOUNCE_INTERVAL,
        metavar="SECONDS",
        help="periodic announce interval (default: %(default)s, 0 disables)",
    )
    serve.add_argument(
        "--no-announce",
        action="store_true",
        help="do not send an announce on startup",
    )
    serve.add_argument(
        "--duration",
        type=float,
        default=None,
        metavar="SECONDS",
        help="stop after this many seconds (default: run until interrupted)",
    )
    serve.add_argument(
        "--http",
        nargs="?",
        const="127.0.0.1:8787",
        metavar="[HOST:]PORT",
        help=("serve the web console and JSON API (default: 127.0.0.1:8787)"),
    )
    serve.add_argument(
        "--web-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="static console build to serve (default: $TAU_WEB_DIR or web/out)",
    )
    serve.add_argument(
        "--http-allow-remote",
        action="store_true",
        help="allow binding the console to a non-loopback address",
    )
    serve.add_argument(
        "--social",
        action="store_true",
        help="enable the Tan social feed request handlers",
    )
    serve.add_argument(
        "--social-sync-interval",
        type=float,
        default=0.0,
        metavar="SECONDS",
        help="periodically pull the Tan feed from peers (0 disables)",
    )
    serve.add_argument(
        "--lxmf",
        action="store_true",
        help=(
            "also run an LXMF router on this node's identity "
            "(requires the lxmf extra: pip install \"tau-p2p[lxmf]\")"
        ),
    )
    serve.add_argument(
        "--lxmf-propagation",
        action="store_true",
        help=(
            "also serve as an LXMF propagation node, holding mail for "
            "offline recipients (requires --lxmf)"
        ),
    )
    serve.set_defaults(func=cmd_serve)

    discover = subparsers.add_parser(
        "discover",
        parents=[common],
        help="listen for tau announces for a while",
    )
    discover.add_argument(
        "--duration",
        type=float,
        default=15.0,
        metavar="SECONDS",
        help="how long to listen (default: %(default)s)",
    )
    discover.add_argument(
        "--max-age",
        type=float,
        default=None,
        metavar="DAYS",
        help="only show peers heard within this many days",
    )
    discover.set_defaults(func=cmd_discover)

    send = subparsers.add_parser(
        "send",
        parents=[common],
        help="send a message to a peer",
    )
    send.add_argument(
        "--to",
        action="append",
        required=True,
        metavar="NAME|HASH",
        help="peer name or destination hash (repeatable: send to several peers)",
    )
    send.add_argument(
        "message", nargs="?", help="message text (reads stdin when omitted)"
    )
    send.add_argument("--file", type=Path, help="read the message from a file")
    send.add_argument("--timeout", type=float, default=20.0, metavar="SECONDS")
    send.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    send.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing the message when the peer is offline",
    )
    send.add_argument(
        "--kind",
        default=None,
        metavar="SLUG",
        help="tag the message with a kind (default chat); receivers can"
        " filter their inbox by kind",
    )
    send.add_argument(
        "--wait",
        type=float,
        default=None,
        metavar="SECONDS",
        help="after queueing, wait up to SECONDS for the message to be delivered",
    )
    send.add_argument(
        "--reply-to",
        metavar="ID|PREFIX",
        help="quote an earlier message in this conversation (message id or unique prefix)",
    )
    send.add_argument(
        "--expire",
        metavar="DURATION",
        help="disappearing message: purge it from the receiver's inbox "
        "after this long (e.g. 30s, 5m, 2h, 7d; at most 30d)",
    )
    send.set_defaults(func=cmd_send)

    # `send --file` already means "read the message text from a file",
    # so file transfer gets its own subcommand instead of overloading it.
    send_file = subparsers.add_parser(
        "send-file",
        parents=[common],
        help="send a small file to a peer over an RNS Resource",
    )
    send_file.add_argument(
        "--to", required=True, metavar="NAME|HASH", help="peer name or destination hash"
    )
    send_file.add_argument(
        "path", type=Path, help="local file to send (at most 10 MiB)"
    )
    send_file.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        metavar="SECONDS",
        help="how long the whole transfer may take (default 300)",
    )
    send_file.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    send_file.set_defaults(func=cmd_send_file)

    react = subparsers.add_parser(
        "react",
        parents=[common],
        help="react to a received message",
    )
    react.add_argument(
        "message",
        metavar="MESSAGE-ID",
        help="message id or unique prefix (see `tau messages`)",
    )
    react.add_argument(
        "reaction",
        nargs="?",
        help="emoji or short text token, e.g. 👍 or +1 (omit with --clear)",
    )
    react.add_argument(
        "--clear",
        action="store_true",
        help="retract your reaction to the message",
    )
    react.add_argument("--timeout", type=float, default=20.0, metavar="SECONDS")
    react.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    react.set_defaults(func=cmd_react)

    retract = subparsers.add_parser(
        "retract",
        parents=[common],
        help="retract a sent message from the recipient's inbox",
    )
    retract.add_argument(
        "message",
        metavar="MESSAGE-ID",
        help="message id or unique prefix (see `tau messages --sent`)",
    )
    retract.add_argument("--timeout", type=float, default=20.0, metavar="SECONDS")
    retract.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    retract.set_defaults(func=cmd_retract)

    edit = subparsers.add_parser(
        "edit",
        parents=[common],
        help="edit a message you sent",
    )
    edit.add_argument(
        "target",
        metavar="MESSAGE-ID",
        help="message id or unique prefix (see `tau messages --sent`)",
    )
    edit.add_argument(
        "text", nargs="?", help="replacement text (reads stdin when omitted)"
    )
    edit.add_argument("--file", type=Path, help="read the replacement text from a file")
    edit.add_argument("--timeout", type=float, default=20.0, metavar="SECONDS")
    edit.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    edit.set_defaults(func=cmd_edit)

    ping = subparsers.add_parser(
        "ping", parents=[common], help="measure reachability of a peer"
    )
    ping.add_argument("--to", required=True, metavar="NAME|HASH")
    ping.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    ping.add_argument("--discover-timeout", type=float, default=10.0, metavar="SECONDS")
    ping.set_defaults(func=cmd_ping)

    info = subparsers.add_parser(
        "info", parents=[common], help="show a remote peer's information"
    )
    info.add_argument("--to", required=True, metavar="NAME|HASH")
    info.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    info.add_argument("--discover-timeout", type=float, default=10.0, metavar="SECONDS")
    info.set_defaults(func=cmd_info)

    doctor = subparsers.add_parser(
        "doctor",
        parents=[common],
        help="check the local data directory's health (read-only)",
    )
    doctor.set_defaults(func=cmd_doctor)

    peers = subparsers.add_parser(
        "peers", parents=[common], help="list peers from the local registry"
    )
    peers.add_argument(
        "--max-age",
        type=float,
        default=None,
        metavar="DAYS",
        help="only show peers heard within this many days",
    )
    peers.add_argument(
        "--exchange",
        action="store_true",
        help="ask known peers for the peers they know and merge the answers",
    )
    peers.add_argument(
        "--to",
        default=None,
        metavar="NAME|HASH",
        help="with --exchange, ask only this peer instead of all known peers",
    )
    peers.add_argument(
        "--safety-number",
        action="store_true",
        help="show each peer's pairwise safety number (compare it with the "
        "peer over another channel to confirm the key is theirs)",
    )
    peers.set_defaults(func=cmd_peers)
    peers_sub = peers.add_subparsers(
        dest="peers_command", metavar="PEERS_COMMAND"
    )

    peers_show = peers_sub.add_parser(
        "show",
        parents=[common],
        help="show what this node knows about one peer (offline, read-only)",
    )
    peers_show.add_argument(
        "target",
        metavar="NAME|HASH",
        help="peer name, destination hash or hash prefix",
    )
    peers_show.set_defaults(func=cmd_peers_show)

    peers_alias = peers_sub.add_parser(
        "alias",
        parents=[common],
        help="give a known peer a local alias",
    )
    peers_alias.add_argument(
        "peer",
        metavar="PEER",
        help="peer name, destination hash, or hash prefix",
    )
    peers_alias.add_argument(
        "alias", metavar="ALIAS", help="local label for the peer"
    )
    peers_alias.set_defaults(func=cmd_peers_alias)

    peers_unalias = peers_sub.add_parser(
        "unalias",
        parents=[common],
        help="remove a peer's local alias",
    )
    peers_unalias.add_argument(
        "peer",
        metavar="PEER",
        help="peer name, alias, destination hash, or hash prefix",
    )
    peers_unalias.set_defaults(func=cmd_peers_unalias)
    peers_verify = peers_sub.add_parser(
        "verify",
        parents=[common],
        help="record that the safety number was compared with a peer "
        "out of band and matched",
    )
    peers_verify.add_argument(
        "target",
        metavar="NAME|HASH",
        help="peer to mark verified",
    )
    peers_verify.set_defaults(func=cmd_peers_verify)
    peers_forget = peers_sub.add_parser(
        "forget",
        parents=[common],
        help="remove a peer from the local registry (the next genuine "
        "announce re-adds it; use this to drop a corrupt record)",
    )
    peers_forget.add_argument(
        "target",
        metavar="NAME|HASH",
        help="peer to remove",
    )
    peers_forget.add_argument(
        "--force",
        action="store_true",
        help="forget even while messages are still queued for the peer "
        "(they will not be attempted until the peer is rediscovered)",
    )
    peers_forget.set_defaults(func=cmd_peers_forget)

    status = subparsers.add_parser(
        "status",
        parents=[common],
        help="show a read-only summary of this node's state",
    )
    status.set_defaults(func=cmd_status)

    identity = subparsers.add_parser(
        "id",
        parents=[common],
        help="show this node's name and destination, or back up / restore its identity",
    )
    identity.set_defaults(func=cmd_id)
    id_sub = identity.add_subparsers(dest="id_command", metavar="ID_COMMAND")
    id_export = id_sub.add_parser(
        "export",
        parents=[common],
        help="write a backup copy of the node identity to a file",
    )
    id_export.add_argument(
        "--to",
        required=True,
        type=Path,
        metavar="PATH",
        help="backup file to write",
    )
    id_export.add_argument(
        "--force",
        action="store_true",
        help="overwrite an existing backup file",
    )
    id_export.set_defaults(func=cmd_id_export)
    id_import = id_sub.add_parser(
        "import",
        parents=[common],
        help="restore the node identity from a backup file",
    )
    id_import.add_argument(
        "--from",
        required=True,
        type=Path,
        dest="from_path",
        metavar="PATH",
        help="backup file to read",
    )
    id_import.add_argument(
        "--force",
        action="store_true",
        help="replace an existing identity",
    )
    id_import.set_defaults(func=cmd_id_import)
    id_rename = id_sub.add_parser(
        "rename",
        parents=[common],
        help="change this node's display name",
    )
    id_rename.add_argument(
        "new_name",
        metavar="NEWNAME",
        help="new node name",
    )
    id_rename.set_defaults(func=cmd_id_rename)

    backup = subparsers.add_parser(
        "backup",
        parents=[common],
        help="back up the whole data directory to one archive file",
    )
    backup.add_argument(
        "file",
        type=Path,
        metavar="FILE",
        help="backup archive to write",
    )
    backup.add_argument(
        "--force",
        action="store_true",
        help="overwrite an existing backup archive",
    )
    backup.set_defaults(func=cmd_backup)

    restore = subparsers.add_parser(
        "restore",
        parents=[common],
        help="restore a backup archive into the data directory",
    )
    restore.add_argument(
        "file",
        type=Path,
        metavar="FILE",
        help="backup archive to read",
    )
    restore.add_argument(
        "--force",
        action="store_true",
        help="replace existing node state in the data directory",
    )
    restore.set_defaults(func=cmd_restore)

    outbox = subparsers.add_parser(
        "outbox",
        parents=[common],
        help="list messages queued for offline peers (store-and-forward)",
    )
    outbox.set_defaults(func=cmd_outbox)
    outbox_sub = outbox.add_subparsers(dest="outbox_command", metavar="OUTBOX_COMMAND")

    outbox_list = outbox_sub.add_parser(
        "list",
        parents=[common],
        help="list messages queued for offline peers",
    )
    outbox_list.set_defaults(func=cmd_outbox)

    outbox_held = outbox_sub.add_parser(
        "held",
        parents=[common],
        help="list sealed messages held for other peers",
    )
    outbox_held.set_defaults(func=cmd_outbox_held)

    outbox_drop_held = outbox_sub.add_parser(
        "drop-held",
        parents=[common],
        help="drop a held message so it is never delivered",
    )
    outbox_drop_held.add_argument(
        "target", metavar="ID", help="message id or unique id prefix"
    )
    outbox_drop_held.set_defaults(func=cmd_outbox_drop_held)
    outbox_cancel = outbox_sub.add_parser(
        "cancel",
        parents=[common],
        help="cancel a queued message so it is never delivered",
    )
    outbox_cancel.add_argument(
        "target", metavar="ID", help="message id or unique id prefix"
    )
    outbox_cancel.set_defaults(func=cmd_outbox_cancel)

    outbox_retry = outbox_sub.add_parser(
        "retry",
        parents=[common],
        help="try delivering queued messages now, ignoring backoff",
    )
    outbox_retry.add_argument(
        "target",
        metavar="ID",
        nargs="?",
        default=None,
        help="message id or unique id prefix (default: retry everything queued)",
    )
    outbox_retry.add_argument(
        "--timeout",
        type=float,
        default=6.0,
        help="seconds to wait per peer contact (default: 6.0)",
    )
    outbox_retry.set_defaults(func=cmd_outbox_retry)

    allowlist = subparsers.add_parser(
        "allowlist",
        parents=[common],
        help="restrict who may send this node direct messages",
    )
    allowlist_sub = allowlist.add_subparsers(
        dest="allowlist_command", metavar="ALLOWLIST_COMMAND"
    )
    allowlist_sub.required = True

    allowlist_list = allowlist_sub.add_parser(
        "list", parents=[common], help="show the DM allow-list"
    )
    allowlist_list.set_defaults(func=cmd_allowlist)

    allowlist_add = allowlist_sub.add_parser(
        "add",
        parents=[common],
        help="allow direct messages from a peer (name, hash or prefix)",
    )
    allowlist_add.add_argument(
        "peer", help="peer name, destination hash or hash prefix"
    )
    allowlist_add.set_defaults(func=cmd_allowlist)

    allowlist_remove = allowlist_sub.add_parser(
        "remove",
        parents=[common],
        help="stop allowing direct messages from a peer",
    )
    allowlist_remove.add_argument(
        "peer", help="peer name, destination hash or hash prefix"
    )
    allowlist_remove.set_defaults(func=cmd_allowlist)

    allowlist_on = allowlist_sub.add_parser(
        "on", parents=[common], help="enforce the DM allow-list"
    )
    allowlist_on.set_defaults(func=cmd_allowlist)

    allowlist_off = allowlist_sub.add_parser(
        "off", parents=[common], help="stop enforcing the DM allow-list"
    )
    allowlist_off.set_defaults(func=cmd_allowlist)

    config_cmd = subparsers.add_parser(
        "config",
        parents=[common],
        help="show or change this node's own settings",
    )
    config_sub = config_cmd.add_subparsers(
        dest="config_command", metavar="CONFIG_COMMAND"
    )
    config_files = config_sub.add_parser(
        "accept-files",
        parents=[common],
        help="turn receiving files on or off (default off)",
    )
    config_files.add_argument(
        "state",
        choices=["on", "off"],
        help="whether this node accepts file transfers",
    )
    config_files.set_defaults(func=cmd_config)
    config_cmd.set_defaults(func=cmd_config)

    config_receipts = config_sub.add_parser(
        "read-receipts",
        parents=[common],
        help="turn sending read receipts on or off (default on)",
    )
    config_receipts.add_argument(
        "state",
        choices=["on", "off"],
        help="whether this node sends read receipts when it reads a DM",
    )
    config_receipts.set_defaults(func=cmd_config)
    config_cmd.set_defaults(func=cmd_config)

    block = subparsers.add_parser(
        "block",
        parents=[common],
        help="silence a peer: drop their DMs and hide their Tan posts",
    )
    block.add_argument(
        "target",
        nargs="?",
        help="peer to block (name, hash or prefix); 'list' shows the list, "
        "'add'/'remove' take the peer as a second word",
    )
    block.add_argument(
        "peer",
        nargs="?",
        help="peer for 'add'/'remove'",
    )
    block.set_defaults(func=cmd_block)

    unblock = subparsers.add_parser(
        "unblock",
        parents=[common],
        help="unblock a peer (name, hash or prefix)",
    )
    unblock.add_argument("peer", help="peer name, destination hash or hash prefix")
    unblock.set_defaults(func=cmd_block, block_command="remove")


    messages = subparsers.add_parser(
        "messages",
        parents=[common],
        help="list received direct messages kept in the node inbox",
    )
    messages.add_argument(
        "--peer",
        default=None,
        metavar="NAME|HASH",
        help="only show messages from this peer (name, hash or hash prefix)",
    )
    messages.add_argument(
        "--search",
        default=None,
        metavar="TEXT",
        help="only show messages whose text contains this (case-insensitive)",
    )
    messages.add_argument(
        "--limit",
        type=int,
        default=30,
        metavar="N",
        help="show the newest N messages (default 30)",
    )
    messages.add_argument(
        "--group",
        default=None,
        metavar="ID|NAME",
        help="only show messages from this group (id, id prefix or name)",
    )
    messages.add_argument(
        "--kind",
        default=None,
        metavar="SLUG",
        help="only show messages of this kind (default: all kinds)",
    )
    messages.add_argument(
        "--sent",
        action="store_true",
        help="show messages this node sent, with their read state, instead",
    )
    messages.add_argument(
        "--unread",
        action="store_true",
        help="only show messages not yet read",
    )
    messages.add_argument(
        "--count",
        action="store_true",
        help="print per-peer unread counts instead of listing messages",
    )
    messages.set_defaults(func=cmd_messages)

    messages_sub = messages.add_subparsers(metavar="COMMAND")
    messages_delete = messages_sub.add_parser(
        "delete",
        parents=[common],
        help="delete one stored direct message from the local inbox",
    )
    messages_delete.add_argument(
        "target", metavar="ID", help="message id or unique id prefix"
    )
    messages_delete.set_defaults(func=cmd_messages_delete)

    messages_export = messages_sub.add_parser(
        "export",
        parents=[common],
        help="back up stored direct messages as a JSON export",
    )
    messages_export.add_argument(
        "--peer",
        default=None,
        metavar="NAME|HASH",
        help="export one sender's messages (name, hash or hash prefix)",
    )
    messages_export.add_argument(
        "--output",
        type=Path,
        metavar="FILE",
        help="write the export to a file instead of stdout",
    )
    messages_export.set_defaults(func=cmd_messages_export)

    messages_import = messages_sub.add_parser(
        "import",
        parents=[common],
        help="restore direct messages from a JSON export",
    )
    messages_import.add_argument(
        "file",
        metavar="FILE",
        type=Path,
        help='export file to read ("-" reads stdin)',
    )
    messages_import.set_defaults(func=cmd_messages_import)

    group = subparsers.add_parser(
        "group",
        parents=[common],
        help="private group DMs with a fixed member list",
    )
    group.set_defaults(func=cmd_group_list)
    group_sub = group.add_subparsers(dest="group_command", metavar="GROUP_COMMAND")

    group_create = group_sub.add_parser(
        "create",
        parents=[common],
        help="create a group with a fixed member list",
    )
    group_create.add_argument("name", help="group name")
    group_create.add_argument(
        "--peer",
        required=True,
        action="append",
        default=[],
        metavar="NAME|HASH",
        help="a member peer (repeatable; the creator is always a member)",
    )
    group_create.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    group_create.set_defaults(func=cmd_group_create)

    group_list = group_sub.add_parser(
        "list",
        parents=[common],
        help="list known groups",
    )
    group_list.set_defaults(func=cmd_group_list)

    group_delete = group_sub.add_parser(
        "delete",
        parents=[common],
        help="delete a group (frees capacity when a group cap is reached)",
    )
    group_delete.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_delete.set_defaults(func=cmd_group_delete)

    group_add_member = group_sub.add_parser(
        "add-member",
        parents=[common],
        help="add peers to a group you created",
    )
    group_add_member.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_add_member.add_argument(
        "--peer",
        required=True,
        action="append",
        default=[],
        metavar="NAME|HASH",
        help="a peer to add (repeatable)",
    )
    group_add_member.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    group_add_member.set_defaults(func=cmd_group_add_member)

    group_remove_member = group_sub.add_parser(
        "remove-member",
        parents=[common],
        help="remove peers from a group you created",
    )
    group_remove_member.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_remove_member.add_argument(
        "--peer",
        required=True,
        action="append",
        default=[],
        metavar="NAME|HASH",
        help="a peer to remove (repeatable)",
    )
    group_remove_member.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    group_remove_member.set_defaults(func=cmd_group_remove_member)

    group_send = group_sub.add_parser(
        "send",
        parents=[common],
        help="send a message to every member of a group",
    )
    group_send.add_argument(
        "group", metavar="GROUP", help="group id, id prefix or name"
    )
    group_send.add_argument(
        "message", nargs="?", help="message text (reads stdin when omitted)"
    )
    group_send.add_argument("--file", type=Path, help="read the message from a file")
    group_send.add_argument("--timeout", type=float, default=20.0, metavar="SECONDS")
    group_send.add_argument(
        "--discover-timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="how long to wait for an unknown peer to announce",
    )
    group_send.add_argument(
        "--no-queue",
        action="store_true",
        help="fail instead of queueing the message when a member is offline",
    )
    group_send.set_defaults(func=cmd_group_send)

    tan = subparsers.add_parser(
        "tan",
        parents=[common],
        help="use the Tan federated social feed",
    )
    tan_sub = tan.add_subparsers(dest="tan_command", metavar="TAN_COMMAND")
    tan_sub.required = True

    tan_post = tan_sub.add_parser(
        "post",
        parents=[common],
        help="sign and store a post, then publish it to known peers",
    )
    tan_post.add_argument(
        "text", nargs="?", help="post text (reads stdin when omitted)"
    )
    tan_post.add_argument("--file", type=Path, help="read the post text from a file")
    tan_post.add_argument(
        "--reply-to",
        default=None,
        metavar="POST",
        help="reply to this post instead of posting standalone (post hash or unique hash prefix)",
    )
    tan_post.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    tan_post.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    tan_post.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    tan_post.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    tan_post.set_defaults(func=cmd_tan_post)

    tan_quote = tan_sub.add_parser(
        "quote",
        parents=[common],
        help="quote a post with your own comment (writes a signed quote entry to your chain)",
    )
    tan_quote.add_argument(
        "target", metavar="POST", help="post hash or unique hash prefix"
    )
    tan_quote.add_argument(
        "text", nargs="?", help="quote comment (reads stdin when omitted)"
    )
    tan_quote.add_argument("--file", type=Path, help="read the comment from a file")
    tan_quote.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    tan_quote.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    tan_quote.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    tan_quote.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    tan_quote.set_defaults(func=cmd_tan_quote)

    for verb, help_text in (
        ("like", "like a post (writes a signed like entry to your chain)"),
        ("unlike", "remove your like from a post (writes an unlike entry)"),
    ):
        tan_react = tan_sub.add_parser(verb, parents=[common], help=help_text)
        tan_react.add_argument(
            "target", metavar="POST", help="post hash or unique hash prefix"
        )
        tan_react.add_argument(
            "--to",
            action="append",
            default=None,
            metavar="NAME|HASH",
            help="publish only to these peers (repeatable)",
        )
        tan_react.add_argument(
            "--no-publish",
            action="store_true",
            help="store locally without contacting peers",
        )
        tan_react.add_argument(
            "--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS"
        )
        tan_react.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
        tan_react.set_defaults(func=cmd_tan_like if verb == "like" else cmd_tan_unlike)

    tan_repost = tan_sub.add_parser(
        "repost",
        parents=[common],
        help="repost a post (writes a signed repost entry to your chain)",
    )
    tan_repost.add_argument(
        "target", metavar="POST", help="post hash or unique hash prefix"
    )
    tan_repost.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    tan_repost.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    tan_repost.add_argument(
        "--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS"
    )
    tan_repost.add_argument(
        "--timeout", type=float, default=15.0, metavar="SECONDS"
    )
    tan_repost.set_defaults(func=cmd_tan_repost)

    tan_delete = tan_sub.add_parser(
        "delete",
        parents=[common],
        help="delete one of your own posts (writes a signed delete entry to your chain)",
    )
    tan_delete.add_argument(
        "target", metavar="POST", help="post hash or unique hash prefix"
    )
    tan_delete.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    tan_delete.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    tan_delete.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    tan_delete.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    tan_delete.set_defaults(func=cmd_tan_delete)

    tan_edit = tan_sub.add_parser(
        "edit",
        parents=[common],
        help="edit one of your own posts (writes a signed edit entry to your chain)",
    )
    tan_edit.add_argument("target", metavar="POST", help="post hash or unique hash prefix")
    tan_edit.add_argument("text", nargs="?", help="replacement text (reads stdin when omitted)")
    tan_edit.add_argument("--file", type=Path, help="read the replacement text from a file")
    tan_edit.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="publish only to these peers (repeatable)",
    )
    tan_edit.add_argument(
        "--no-publish",
        action="store_true",
        help="store locally without contacting peers",
    )
    tan_edit.add_argument("--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS")
    tan_edit.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    tan_edit.set_defaults(func=cmd_tan_edit)

    tan_timeline = tan_sub.add_parser(
        "timeline",
        parents=[common],
        help="show posts from you and the authors you follow",
    )
    tan_timeline.add_argument(
        "--all", action="store_true", help="show every cached post"
    )
    tan_timeline.add_argument(
        "--author", metavar="NAME|HASH", help="show one author's posts"
    )
    tan_timeline.add_argument(
        "--limit", type=int, default=30, metavar="N", help="maximum posts"
    )
    tan_timeline.set_defaults(func=cmd_tan_timeline)

    tan_search = tan_sub.add_parser(
        "search",
        parents=[common],
        help="search stored posts by keyword",
    )
    tan_search.add_argument("query", metavar="TEXT", help="text to search for")
    tan_search.add_argument(
        "--author", metavar="NAME|HASH", help="search one author's posts"
    )
    tan_search.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FEED_LIMIT,
        metavar="N",
        help="maximum posts",
    )
    tan_search.set_defaults(func=cmd_tan_search)

    tan_top = tan_sub.add_parser(
        "top",
        parents=[common],
        help="show the most-liked stored posts",
    )
    tan_top.add_argument(
        "--author", metavar="NAME|HASH", help="rank one author's posts"
    )
    tan_top.add_argument(
        "--limit",
        type=int,
        default=10,
        metavar="N",
        help="maximum posts (default 10)",
    )
    tan_top.set_defaults(func=cmd_tan_top)

    tan_verify = tan_sub.add_parser(
        "verify",
        parents=[common],
        help="re-verify every stored post's signature and chain linkage",
    )
    tan_verify.set_defaults(func=cmd_tan_verify)

    tan_mentions = tan_sub.add_parser(
        "mentions",
        parents=[common],
        help="show replies to your own posts",
    )
    tan_mentions.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FEED_LIMIT,
        metavar="N",
        help="maximum posts",
    )
    tan_mentions.set_defaults(func=cmd_tan_mentions)

    tan_thread = tan_sub.add_parser(
        "thread",
        parents=[common],
        help="show a post with its replies as a nested thread",
    )
    tan_thread.add_argument(
        "ref",
        metavar="HASH|PREFIX",
        help="post or reply hash (full hash or unique prefix)",
    )
    tan_thread.set_defaults(func=cmd_tan_thread)

    tan_likes = tan_sub.add_parser(
        "likes",
        parents=[common],
        help="show who currently likes a post",
    )
    tan_likes.add_argument(
        "ref", metavar="HASH|PREFIX", help="post or reply hash (full hash or unique prefix)"
    )
    tan_likes.set_defaults(func=cmd_tan_likes)

    tan_follow = tan_sub.add_parser(
        "follow", parents=[common], help="follow a peer or author"
    )
    tan_follow.add_argument("target", metavar="NAME|HASH")
    tan_follow.set_defaults(func=cmd_tan_follow)

    tan_unfollow = tan_sub.add_parser(
        "unfollow", parents=[common], help="stop following an author"
    )
    tan_unfollow.add_argument("target", metavar="NAME|HASH")
    tan_unfollow.set_defaults(func=cmd_tan_unfollow)

    tan_following = tan_sub.add_parser(
        "following", parents=[common], help="list followed authors"
    )
    tan_following.set_defaults(func=cmd_tan_following)

    tan_authors = tan_sub.add_parser(
        "authors", parents=[common], help="list every author in the local Tan store"
    )
    tan_authors.add_argument(
        "--sort",
        choices=["recent", "posts", "name"],
        default="recent",
        help="sort order (default: recent)",
    )
    tan_authors.add_argument(
        "--limit", type=int, default=50, metavar="N", help="maximum authors"
    )
    tan_authors.set_defaults(func=cmd_tan_authors)

    tan_suggest = tan_sub.add_parser(
        "suggest",
        parents=[common],
        help="suggest authors to follow, from who the authors you follow engage with",
    )
    tan_suggest.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_SUGGEST_LIMIT,
        metavar="N",
        help="maximum suggestions (default: %(default)s)",
    )
    tan_suggest.set_defaults(func=cmd_tan_suggest)
    tan_mute = tan_sub.add_parser(
        "mute", parents=[common], help="hide an author's posts from local views"
    )
    tan_mute.add_argument("target", metavar="NAME|HASH")
    tan_mute.set_defaults(func=cmd_tan_mute)

    tan_unmute = tan_sub.add_parser(
        "unmute", parents=[common], help="show a muted author's posts again"
    )
    tan_unmute.add_argument("target", metavar="NAME|HASH")
    tan_unmute.set_defaults(func=cmd_tan_unmute)

    tan_muted = tan_sub.add_parser("muted", parents=[common], help="list muted authors")
    tan_muted.set_defaults(func=cmd_tan_muted)

    tan_mute_keyword = tan_sub.add_parser(
        "mute-keyword",
        parents=[common],
        help="hide posts containing a word or phrase from local views",
    )
    tan_mute_keyword.add_argument(
        "keyword", metavar="WORD|PHRASE", help="word or phrase to mute"
    )
    tan_mute_keyword.set_defaults(func=cmd_tan_mute_keyword)

    tan_unmute_keyword = tan_sub.add_parser(
        "unmute-keyword",
        parents=[common],
        help="stop hiding posts containing a word or phrase",
    )
    tan_unmute_keyword.add_argument(
        "keyword", metavar="WORD|PHRASE", help="word or phrase to unmute"
    )
    tan_unmute_keyword.set_defaults(func=cmd_tan_unmute_keyword)

    tan_muted_keywords = tan_sub.add_parser(
        "muted-keywords",
        parents=[common],
        help="list muted keywords",
    )
    tan_muted_keywords.set_defaults(func=cmd_tan_muted_keywords)

    tan_save = tan_sub.add_parser(
        "save", parents=[common], help="save a post to read later"
    )
    tan_save.add_argument(
        "ref", metavar="HASH|PREFIX", help="post hash (full hash or unique prefix)"
    )
    tan_save.set_defaults(func=cmd_tan_save)

    tan_unsave = tan_sub.add_parser(
        "unsave", parents=[common], help="remove a post from your saved list"
    )
    tan_unsave.add_argument(
        "ref", metavar="HASH|PREFIX", help="post hash (full hash or unique prefix)"
    )
    tan_unsave.set_defaults(func=cmd_tan_unsave)

    tan_saved = tan_sub.add_parser(
        "saved", parents=[common], help="list your saved posts"
    )
    tan_saved.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FEED_LIMIT,
        metavar="N",
        help="maximum posts",
    )
    tan_saved.set_defaults(func=cmd_tan_saved)

    tan_sync = tan_sub.add_parser(
        "sync",
        parents=[common],
        help="pull missing posts from known peers",
    )
    tan_sync.add_argument(
        "--to",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="sync only with these peers (repeatable)",
    )
    tan_sync.add_argument(
        "--author",
        action="append",
        default=None,
        metavar="NAME|HASH",
        help="sync only these authors (repeatable)",
    )
    tan_sync.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_FEED_LIMIT,
        metavar="N",
        help="posts per request (default: %(default)s)",
    )
    tan_sync.add_argument(
        "--explore",
        action="store_true",
        help="also fetch recent public posts to discover new authors",
    )
    tan_sync.add_argument("--timeout", type=float, default=15.0, metavar="SECONDS")
    tan_sync.set_defaults(func=cmd_tan_sync)

    tan_export = tan_sub.add_parser(
        "export",
        parents=[common],
        help="back up stored Tan posts as a JSON export",
    )
    tan_export.add_argument(
        "--author",
        metavar="NAME|HASH",
        help="export one author's entries (default: your own)",
    )
    tan_export.add_argument(
        "--all", action="store_true", help="export every stored author's entries"
    )
    tan_export.add_argument(
        "--output",
        type=Path,
        metavar="FILE",
        help="write the export to a file instead of stdout",
    )
    tan_export.set_defaults(func=cmd_tan_export)

    tan_import = tan_sub.add_parser(
        "import",
        parents=[common],
        help="restore Tan posts from a JSON export (signatures re-verified)",
    )
    tan_import.add_argument(
        "file",
        metavar="FILE",
        type=Path,
        help='export file to read ("-" reads stdin)',
    )
    tan_import.set_defaults(func=cmd_tan_import)

    tan_rules = tan_sub.add_parser(
        "rules",
        parents=[common],
        help="show or change which remote Tan entries this node commits",
    )
    tan_rules.add_argument(
        "--default",
        choices=["allow", "deny"],
        default=None,
        help="policy for authors no other rule covers",
    )
    tan_rules.add_argument(
        "--allow-author",
        action="append",
        default=None,
        metavar="PEER",
        help="always admit this author (repeatable)",
    )
    tan_rules.add_argument(
        "--block-author",
        action="append",
        default=None,
        metavar="PEER",
        help="never admit this author (repeatable)",
    )
    tan_rules.add_argument(
        "--unallow-author",
        action="append",
        default=None,
        metavar="PEER",
        help="remove an author from the allow list (repeatable)",
    )
    tan_rules.add_argument(
        "--unblock-author",
        action="append",
        default=None,
        metavar="PEER",
        help="remove an author from the block list (repeatable)",
    )
    tan_rules.add_argument(
        "--followed-only",
        action="store_true",
        default=argparse.SUPPRESS,
        help="also admit authors you follow",
    )
    tan_rules.add_argument(
        "--no-followed-only",
        dest="followed_only",
        action="store_false",
        default=argparse.SUPPRESS,
        help="stop admitting authors just because you follow them",
    )
    tan_rules.add_argument(
        "--kinds",
        default=None,
        metavar="post,reply,repost,quote,edit,like,unlike,delete",
        help="comma-separated entry kinds to display (display-only: "
        "every kind is still stored, so chains stay complete)",
    )
    tan_rules.add_argument(
        "--all-kinds",
        action="store_true",
        help="display every entry kind (clears --kinds)",
    )
    tan_rules.set_defaults(func=cmd_tan_rules)

    bundle = subparsers.add_parser(
        "bundle",
        parents=[common],
        help="carry mail and Tan posts on a file (sneakernet)",
    )
    bundle_sub = bundle.add_subparsers(
        dest="bundle_command", metavar="BUNDLE_COMMAND"
    )
    bundle_sub.required = True

    bundle_export = bundle_sub.add_parser(
        "export",
        parents=[common],
        help="write sealed mail (and optionally Tan posts) to a signed bundle file",
    )
    bundle_export.add_argument(
        "file", metavar="FILE", type=Path, help="bundle file to write"
    )
    bundle_export.add_argument(
        "--tan",
        action="store_true",
        help="also include Tan posts from every stored author",
    )
    bundle_export.add_argument(
        "--max-bytes",
        type=int,
        default=DEFAULT_MAX_BUNDLE_BYTES,
        metavar="N",
        help="refuse to write a bundle larger than N bytes (default: %(default)s)",
    )
    bundle_export.set_defaults(func=cmd_bundle_export)

    bundle_import = bundle_sub.add_parser(
        "import",
        parents=[common],
        help="merge a bundle file into this node's mailbox and Tan store",
    )
    bundle_import.add_argument(
        "file",
        metavar="FILE",
        type=Path,
        help='bundle file to read ("-" reads stdin)',
    )
    bundle_import.add_argument(
        "--max-bytes",
        type=int,
        default=DEFAULT_MAX_BUNDLE_BYTES,
        metavar="N",
        help="refuse to read a bundle larger than N bytes (default: %(default)s)",
    )
    bundle_import.set_defaults(func=cmd_bundle_import)

    return parser


# -- logging and output helpers -------------------------------------------


def _app_log_level(args: argparse.Namespace) -> int:
    if getattr(args, "quiet", False):
        return logging.ERROR
    verbose = getattr(args, "verbose", 0)
    if verbose >= 2:
        return logging.DEBUG
    if verbose == 1:
        return logging.INFO
    return logging.WARNING


def _rns_log_level(args: argparse.Namespace) -> int:
    explicit = getattr(args, "rns_log_level", None)
    if explicit is not None:
        return max(0, min(8, int(explicit)))
    if getattr(args, "quiet", False):
        return 1
    verbose = getattr(args, "verbose", 0)
    if verbose >= 2:
        return 5
    if verbose == 1:
        return 4
    return 2


def _configure_logging(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=_app_log_level(args),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )


def _json_mode(args: argparse.Namespace) -> bool:
    return bool(getattr(args, "json_output", False))


def _emit(payload: Any, json_output: bool, *, file: Any = None) -> None:
    stream = file or sys.stdout
    if json_output:
        print(json.dumps(payload, sort_keys=True), file=stream, flush=True)
        return
    print(payload, file=stream, flush=True)


def _emit_event(args: argparse.Namespace, event: str, **fields: Any) -> None:
    if _json_mode(args):
        payload = {"event": event}
        payload.update(fields)
        print(json.dumps(payload, sort_keys=True), flush=True)


def _install_stop_handlers(stop: threading.Event) -> Dict[int, Any]:
    """Turn SIGINT/SIGTERM into a graceful stop request.

    Reticulum installs its own handlers that call ``os._exit``; overriding them
    lets us save the peer registry and detach interfaces cleanly.
    """
    previous: Dict[int, Any] = {}

    def request_stop(signum: int, frame: Any) -> None:
        stop.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            previous[signum] = signal.signal(signum, request_stop)
        except (ValueError, OSError):  # pragma: no cover - non-main thread
            pass
    return previous


def _restore_stop_handlers(previous: Dict[int, Any]) -> None:
    for signum, handler in previous.items():
        try:
            signal.signal(signum, handler)
        except (ValueError, OSError):  # pragma: no cover
            pass


def _stamp(timestamp: Optional[float] = None) -> str:
    return datetime.fromtimestamp(timestamp or time.time()).strftime("%H:%M:%S")


def _ago(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"



_DURATION_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 7 * 86400}


def _parse_duration(value: str) -> float:
    """Parse a ``--expire`` duration like ``30s``, ``5m``, ``2h``, ``7d``.

    A bare number means seconds.  Returns the duration in seconds;
    raises ``ValueError`` for anything unparseable, non-positive, or
    longer than the mail TTL (a disappearing message cannot outlive
    the store-and-forward envelope that might carry it).
    """
    text = (value or "").strip().lower()
    if not text:
        raise ValueError("--expire needs a duration like 30s, 5m, 2h or 7d")
    unit = 1.0
    if text[-1] in _DURATION_UNITS:
        unit = float(_DURATION_UNITS[text[-1]])
        text = text[:-1]
    try:
        amount = float(text)
    except ValueError as exc:
        raise ValueError(
            f"invalid --expire duration {value!r} (try 30s, 5m, 2h or 7d)"
        ) from exc
    seconds = amount * unit
    if not seconds > 0:
        raise ValueError("--expire duration must be positive")
    if seconds > MAX_MAIL_TTL:
        raise ValueError("--expire duration must be at most 30d")
    return seconds

def _print_peer_table(
    peers: Iterable[Peer],
    *,
    now: Optional[float] = None,
    safety_numbers: Optional[Dict[str, str]] = None,
) -> None:
    rows = list(peers)
    if not rows:
        print("No peers known. Run `tau discover` or `tau serve` first.")
        return
    timestamp = now if now is not None else time.time()
    header = ("DESTINATION", "NAME", "VERIFIED", "HOPS", "LAST SEEN", "LXMF")
    table: List[tuple] = [header]
    for peer in rows:
        name = peer.display_name
        if peer.alias and peer.alias != peer.name:
            name = f"{peer.alias} ({peer.name})"
        table.append(
            (
                peer.hexhash,
                name,
                "✓" if peer.is_verified else "",
                str(peer.hops),
                _ago(peer.age(timestamp)),
                peer.lxmf_hash or "—",
            )
        )
    widths = [max(len(row[i]) for row in table) for i in range(len(header))]
    for row in table:
        print("  ".join(value.ljust(widths[i]) for i, value in enumerate(row)))
    changed = [p for p in rows if p.key_change_at is not None]
    for peer in changed:
        print(
            f"WARNING: {peer.display_name} ({short_hash(peer.hexhash)}) "
            "presented a different public key under the same destination "
            "hash; the recorded key was kept. Re-verify this peer out of "
            "band before trusting it.",
            file=sys.stderr,
        )
    if safety_numbers:
        print("\nSafety numbers — compare each with the peer over another "
              "channel; a match confirms the key is theirs:")
        for peer in rows:
            number = safety_numbers.get(peer.hexhash)
            if number:
                print(f"  {peer.display_name} ({short_hash(peer.hexhash)}): {number}")


def _make_config(
    args: argparse.Namespace, *, announce_interval: float = DEFAULT_ANNOUNCE_INTERVAL
) -> TauConfig:
    return TauConfig.resolve(
        data_dir=getattr(args, "data_dir", None),
        rns_config_dir=getattr(args, "rns_config", None),
        name=getattr(args, "name", None),
        rns_loglevel=_rns_log_level(args),
        announce_interval=announce_interval,
    )


def _read_message(args: argparse.Namespace) -> str:
    if getattr(args, "file", None):
        try:
            return Path(args.file).expanduser().read_text(encoding="utf-8")
        except OSError as exc:
            raise NodeError(f"could not read {args.file}: {exc}") from exc
    text = getattr(args, "message", None)
    if text is None:
        text = getattr(args, "text", None)
    if text is not None:
        return text
    if not sys.stdin.isatty():
        return sys.stdin.read()
    raise NodeError("provide a message, --file, or pipe text on stdin")


# -- commands --------------------------------------------------------------


def cmd_init(args: argparse.Namespace) -> int:
    config = _make_config(args)
    config.prepare()

    auto_interface = not getattr(args, "no_auto_interface", False)
    changed = write_rns_config(
        config.rns_config_file,
        tcp_listen=getattr(args, "tcp_listen", None),
        tcp_connect=getattr(args, "tcp_connect", None),
        auto_interface=auto_interface,
        loglevel=config.rns_loglevel,
        force=bool(getattr(args, "force", False)),
    )
    if getattr(args, "name", None):
        write_node_metadata(config.data_dir, args.name)
        config = _make_config(args)  # refresh the resolved name

    identity = load_or_create_identity(config.identity_path)
    destination = destination_hash(identity).hex()
    payload = {
        "name": config.name,
        "destination": destination,
        "identity": identity.hexhash,
        "data_dir": str(config.data_dir),
        "rns_config": str(config.rns_config_file),
        "config_written": changed,
        "auto_interface": auto_interface,
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f'Node "{config.name}" is ready.')
        print(f"  destination: {destination}")
        print(f"  identity:    {identity.hexhash}")
        print(f"  data dir:    {config.data_dir}")
        print(
            f"  config:      {config.rns_config_file}"
            + ("" if changed else " (existing)")
        )
        print()
        print("Start a node with:  tau serve")
        print("Find peers with:    tau discover")
    return _EXIT_OK


def _node_callbacks(args: argparse.Namespace):
    json_output = _json_mode(args)

    def on_peer(peer: Peer) -> None:
        if json_output:
            _emit_event(args, "peer", peer=peer.public_dict())
        else:
            print(
                f"[{_stamp()}] peer {peer.display_name} "
                f"({short_hash(peer.hexhash)}) at {peer.hops} hop(s)",
                flush=True,
            )

    def on_message(record: Dict[str, Any]) -> None:
        if json_output:
            _emit_event(args, "message", message=record)
        else:
            sender = record.get("from", "unknown")
            destination = short_hash(record.get("destination") or "")
            suffix = f" ({destination})" if destination else ""
            text = record.get("text", "")
            if record.get("kind") == "group":
                group_name = record.get("group_name") or "group"
                print(
                    f"[{_stamp()}] group message from {sender}{suffix}"
                    f" in {group_name}:",
                    flush=True,
                )
            else:
                print(f"[{_stamp()}] message from {sender}{suffix}:", flush=True)
            for line in str(text).splitlines() or [""]:
                print(f"  {line}", flush=True)

    def on_file(record: Dict[str, Any]) -> None:
        if json_output:
            _emit_event(args, "file", file=record)
        else:
            sender = record.get("from", "unknown")
            destination = short_hash(record.get("destination") or "")
            suffix = f" ({destination})" if destination else ""
            print(
                f"[{_stamp()}] file from {sender}{suffix}: "
                f"{record.get('name')} ({record.get('size')} bytes)",
                flush=True,
            )
            print(f"  saved to {record.get('path')}", flush=True)

    return on_peer, on_message, on_file


def _default_web_dir() -> Optional[Path]:
    override = os.environ.get("TAU_WEB_DIR")
    if override:
        return Path(override).expanduser()
    candidate = Path(__file__).resolve().parent.parent / "web" / "out"
    return candidate if candidate.exists() else None


def _print_tan_posts(
    posts: Sequence[Dict[str, Any]],
    *,
    json_output: bool,
    empty_message: str = 'No posts yet. Try `tau tan post "hello"` or `tau tan sync --explore`.',
) -> None:
    if json_output:
        _emit({"posts": list(posts), "count": len(posts)}, True)
        return
    if not posts:
        print(empty_message)
        return
    for post in posts:
        name = post.get("name") or "anonymous"
        author = short_hash(str(post.get("author", "")))
        when = datetime.fromtimestamp(float(post.get("created_at", 0))).strftime(
            "%Y-%m-%d %H:%M"
        )
        print(f"[{when}] {name} ({author}) {short_hash(str(post.get('hash', '')))}{' (edited)' if post.get('edited') else ''}")
        if post.get("kind") == "reply":
            parent = post.get("reply_to")
            if isinstance(parent, dict):
                if parent.get("blocked"):
                    print("  in reply to a blocked author's post")
                elif parent.get("deleted"):
                    print("  in reply to a deleted post")
                elif parent.get("muted"):
                    print("  in reply to a muted post")
                else:
                    parent_text = str(parent.get("text") or "")
                    if len(parent_text) > 80:
                        parent_text = parent_text[:80] + "…"
                    print(
                        f"  in reply to {parent.get('name') or 'anonymous'}: "
                        f"{parent_text}"
                    )
            else:
                print(f"  in reply to {short_hash(str(post.get('target', '')))}")
        if post.get("kind") == "repost":
            original = post.get("repost_of")
            if isinstance(original, dict):
                if original.get("deleted"):
                    print("  reposted a deleted post")
                else:
                    print(
                        f"  reposted {original.get('name') or 'anonymous'} "
                        f"({short_hash(str(original.get('author', '')))}):"
                    )
                    for line in str(original.get("text") or "").splitlines() or [""]:
                        print(f"  {line}")
            else:
                print(
                    f"  reposted {short_hash(str(post.get('target', '')))}"
                    " (original not cached yet)"
                )
        else:
            for line in str(post.get("text", "")).splitlines() or [""]:
                print(f"  {line}")
        if post.get("kind") == "quote":
            quoted = post.get("quoted")
            if isinstance(quoted, dict):
                if quoted.get("blocked"):
                    print("  quoting a blocked author's post")
                elif quoted.get("deleted"):
                    print("  quoting a deleted post")
                else:
                    print(f"  quoting {quoted.get('name') or 'anonymous'}:")
                    for line in str(quoted.get("text") or "").splitlines() or [""]:
                        print(f"    {line}")
            else:
                print(f"  quoting {short_hash(str(post.get('target', '')))}")
        likes = int(post.get("likes") or 0)
        if likes or post.get("liked"):
            suffix = " (you liked this)" if post.get("liked") else ""
            print(f"  ♥ {likes}{suffix}")
        reposts = int(post.get("reposts") or 0)
        if reposts or post.get("reposted"):
            suffix = " (you reposted this)" if post.get("reposted") else ""
            print(f"  ↻ {reposts}{suffix}")
        quotes = int(post.get("quotes") or 0)
        if quotes:
            print(f"  ❝ {quotes}")


def _print_tan_thread(
    posts: Sequence[Dict[str, Any]],
    *,
    json_output: bool,
) -> None:
    """Print a thread as returned by :func:`tan.thread`, indented by depth."""
    if json_output:
        _emit({"posts": list(posts), "count": len(posts)}, True)
        return
    if not posts:
        print("No posts in this thread.")
        return
    for post in posts:
        indent = "  " * int(post.get("depth") or 0)
        name = post.get("name") or "anonymous"
        author = short_hash(str(post.get("author", "")))
        when = datetime.fromtimestamp(float(post.get("created_at", 0))).strftime(
            "%Y-%m-%d %H:%M"
        )
        print(
            f"{indent}[{when}] {name} ({author}) "
            f"{short_hash(str(post.get('hash', '')))}"
            f"{' (edited)' if post.get('edited') else ''}"
        )
        for line in str(post.get("text", "")).splitlines() or [""]:
            print(f"{indent}  {line}")
        if post.get("kind") == "quote":
            quoted = post.get("quoted")
            if isinstance(quoted, dict):
                if quoted.get("blocked"):
                    print(f"{indent}  quoting a blocked author's post")
                elif quoted.get("deleted"):
                    print(f"{indent}  quoting a deleted post")
                else:
                    print(f"{indent}  quoting {quoted.get('name') or 'anonymous'}:")
                    for line in str(quoted.get("text") or "").splitlines() or [""]:
                        print(f"{indent}    {line}")
            else:
                print(
                    f"{indent}  quoting "
                    f"{short_hash(str(post.get('target', '')))}"
                )
        likes = int(post.get("likes") or 0)
        if likes or post.get("liked"):
            suffix = " (you liked this)" if post.get("liked") else ""
            print(f"{indent}  ♥ {likes}{suffix}")
        reposts = int(post.get("reposts") or 0)
        if reposts or post.get("reposted"):
            suffix = " (you reposted this)" if post.get("reposted") else ""
            print(f"{indent}  ↻ {reposts}{suffix}")
        quotes = int(post.get("quotes") or 0)
        if quotes:
            print(f"{indent}  ❝ {quotes}")


def _print_saved_posts(
    posts: Sequence[Dict[str, Any]],
    *,
    json_output: bool,
) -> None:
    """Print saved posts, newest save first."""
    if json_output:
        _emit({"saved": list(posts), "count": len(posts)}, True)
        return
    if not posts:
        print("No saved posts yet. Try `tau tan save <hash>`.")
        return
    for post in posts:
        name = post.get("name") or "anonymous"
        author = short_hash(str(post.get("author", "")))
        when = datetime.fromtimestamp(float(post.get("created_at", 0))).strftime(
            "%Y-%m-%d %H:%M"
        )
        saved = datetime.fromtimestamp(float(post.get("saved_at", 0))).strftime(
            "%Y-%m-%d %H:%M"
        )
        print(f"[{when}] {name} ({author}) {short_hash(str(post.get('hash', '')))}")
        print(f"  saved {saved}")
        if post.get("deleted"):
            print("  (deleted post)")
            continue
        for line in str(post.get("text", "")).splitlines() or [""]:
            print(f"  {line}")
        likes = int(post.get("likes") or 0)
        if likes or post.get("liked"):
            suffix = " (you liked this)" if post.get("liked") else ""
            print(f"  ♥ {likes}{suffix}")


def _print_tan_likes(result: Dict[str, Any], *, json_output: bool) -> None:
    """Print a post plus its current likers, as returned by tan.likers."""
    post = result["post"]
    likers = result["likers"]
    if json_output:
        _emit({"post": post, "likers": likers, "count": len(likers)}, True)
        return
    name = post.get("name") or "anonymous"
    author = short_hash(str(post.get("author", "")))
    when = datetime.fromtimestamp(float(post.get("created_at", 0))).strftime(
        "%Y-%m-%d %H:%M"
    )
    print(f"[{when}] {name} ({author}) {short_hash(str(post.get('hash', '')))}")
    for line in str(post.get("text", "")).splitlines() or [""]:
        print(f"  {line}")
    if not likers:
        print("No likes yet.")
        return
    print(f"♥ {len(likers)} like(s):")
    for liker in likers:
        liker_name = liker.get("name") or "anonymous"
        print(f"  {liker_name} ({short_hash(str(liker.get('author', '')))})")


def _tan_identity(config: TauConfig) -> Any:
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    return identity


def _publish_local_post(
    node: TauNode,
    *,
    text: str,
    reply_to: Optional[str] = None,
    quote_of: Optional[str] = None,
    to: Optional[List[str]] = None,
    ttl: int = MAX_GOSSIP_TTL,
    timeout: float = 15.0,
) -> Dict[str, Any]:
    # Use a separate app so per-command timeouts do not mutate the serving app.
    publisher = TanApp(
        node.config, node.identity, name=node.name, request_timeout=float(timeout)
    )
    publisher.node = node
    targets = [node.resolve_peer(target) for target in to] if to else None
    if quote_of is not None:
        post = publisher.quote(quote_of, text)
    else:
        post = publisher.create_post(text, reply_to=reply_to)
    published = publisher.publish(
        post, targets=targets, ttl=max(0, min(int(ttl), MAX_GOSSIP_TTL))
    )
    return {"post": post, "published": published}


def cmd_tan_post(args: argparse.Namespace) -> int:
    text = _read_message(args)
    config = _make_config(args)
    json_output = _json_mode(args)

    reply_to = None
    if getattr(args, "reply_to", None):
        conn = init_db(config.social_db_path)
        try:
            reply_to = resolve_post(conn, args.reply_to)["hash"]
        finally:
            conn.close()

    if getattr(args, "no_publish", False):
        app = TanApp(config, _tan_identity(config), name=config.name)
        post = app.create_post(text, reply_to=reply_to)
        published = None
    else:
        node = TauNode(config, name=config.name)
        try:
            node.start(exclusive=True)
        except InstanceAlreadyRunning:
            from .local_publish import publish_running

            result = publish_running(
                config.data_dir,
                text=text,
                reply_to=reply_to,
                to=getattr(args, "to", None),
                ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                timeout=float(getattr(args, "timeout", 15.0)),
            )
            post, published = result["post"], result["published"]
        else:
            try:
                app = TanApp(
                    config,
                    node.identity,
                    name=config.name,
                    request_timeout=float(getattr(args, "timeout", 15.0)),
                )
                node.attach_app(app)
                result = _publish_local_post(
                    node,
                    text=text,
                    reply_to=reply_to,
                    to=getattr(args, "to", None),
                    ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                    timeout=float(getattr(args, "timeout", 15.0)),
                )
                post, published = result["post"], result["published"]
            finally:
                node.stop()

    if json_output:
        _emit({"post": post, "published": published}, True)
    else:
        verb = "replied" if reply_to is not None else "posted"
        print(f"{verb} {short_hash(post['hash'])}")
        if reply_to is not None:
            print(f"  in reply to {short_hash(reply_to)}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_tan_quote(args: argparse.Namespace) -> int:
    text = _read_message(args)
    config = _make_config(args)
    json_output = _json_mode(args)

    conn = init_db(config.social_db_path)
    try:
        target = resolve_post(conn, args.target)["hash"]
    finally:
        conn.close()

    if getattr(args, "no_publish", False):
        app = TanApp(config, _tan_identity(config), name=config.name)
        post = app.quote(target, text)
        published = None
    else:
        node = TauNode(config, name=config.name)
        try:
            node.start(exclusive=True)
        except InstanceAlreadyRunning:
            from .local_publish import publish_running

            result = publish_running(
                config.data_dir, text=text, quote_of=target,
                to=getattr(args, "to", None),
                ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                timeout=float(getattr(args, "timeout", 15.0)),
            )
            post, published = result["post"], result["published"]
        else:
            try:
                app = TanApp(
                    config, node.identity, name=config.name,
                    request_timeout=float(getattr(args, "timeout", 15.0)),
                )
                node.attach_app(app)
                result = _publish_local_post(
                    node, text=text, quote_of=target,
                    to=getattr(args, "to", None),
                    ttl=getattr(args, "ttl", MAX_GOSSIP_TTL),
                    timeout=float(getattr(args, "timeout", 15.0)),
                )
                post, published = result["post"], result["published"]
            finally:
                node.stop()

    if json_output:
        _emit({"post": post, "published": published}, True)
    else:
        print(f"quoted {short_hash(post['hash'])}")
        print(f"  quoting {short_hash(target)}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def _cmd_tan_react(args: argparse.Namespace, *, liked: bool) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)

    conn = init_db(config.social_db_path)
    try:
        target = resolve_post(conn, args.target)["hash"]
    finally:
        conn.close()

    if getattr(args, "no_publish", False):
        app = TanApp(config, _tan_identity(config), name=config.name)
        record = app.react(target, liked=liked)
        published = None
    else:
        node = TauNode(config, name=config.name)
        node.start()
        try:
            app = TanApp(
                config,
                node.identity,
                name=config.name,
                request_timeout=float(getattr(args, "timeout", 15.0)),
            )
            node.attach_app(app)
            targets = None
            if getattr(args, "to", None):
                targets = [node.resolve_peer(target) for target in args.to]
            record = app.react(target, liked=liked)
            published = app.publish(
                record,
                targets=targets,
                ttl=max(
                    0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)
                ),
            )
        finally:
            node.stop()

    if json_output:
        _emit({"reaction": record, "published": published}, True)
    else:
        verb = "liked" if liked else "unliked"
        print(f"{verb} {short_hash(target)}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_tan_like(args: argparse.Namespace) -> int:
    return _cmd_tan_react(args, liked=True)


def cmd_tan_unlike(args: argparse.Namespace) -> int:
    return _cmd_tan_react(args, liked=False)


def cmd_tan_repost(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)

    conn = init_db(config.social_db_path)
    try:
        target = resolve_post(conn, args.target)["hash"]
    finally:
        conn.close()

    if getattr(args, "no_publish", False):
        app = TanApp(config, _tan_identity(config), name=config.name)
        record = app.repost(target)
        published = None
    else:
        node = TauNode(config, name=config.name)
        node.start()
        try:
            app = TanApp(
                config,
                node.identity,
                name=config.name,
                request_timeout=float(getattr(args, "timeout", 15.0)),
            )
            node.attach_app(app)
            targets = None
            if getattr(args, "to", None):
                targets = [node.resolve_peer(target) for target in args.to]
            record = app.repost(target)
            published = app.publish(
                record,
                targets=targets,
                ttl=max(0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)),
            )
        finally:
            node.stop()

    if json_output:
        _emit({"repost": record, "published": published}, True)
    else:
        print(f"reposted {short_hash(target)}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_tan_delete(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)

    conn = init_db(config.social_db_path)
    try:
        target = resolve_delete_target(conn, args.target)
    finally:
        conn.close()

    if getattr(args, "no_publish", False):
        app = TanApp(config, _tan_identity(config), name=config.name)
        record = app.delete(target)
        published = None
    else:
        node = TauNode(config, name=config.name)
        node.start()
        try:
            app = TanApp(
                config,
                node.identity,
                name=config.name,
                request_timeout=float(getattr(args, "timeout", 15.0)),
            )
            node.attach_app(app)
            targets = None
            if getattr(args, "to", None):
                targets = [node.resolve_peer(target) for target in args.to]
            record = app.delete(target)
            published = app.publish(
                record,
                targets=targets,
                ttl=max(
                    0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)
                ),
            )
        finally:
            node.stop()

    if json_output:
        _emit({"delete": record, "published": published}, True)
    else:
        print(f"deleted {short_hash(target)}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_tan_edit(args: argparse.Namespace) -> int:
    text = _read_message(args)
    config = _make_config(args)
    json_output = _json_mode(args)

    conn = init_db(config.social_db_path)
    try:
        target = resolve_post(conn, args.target)["hash"]
    finally:
        conn.close()

    if getattr(args, "no_publish", False):
        app = TanApp(config, _tan_identity(config), name=config.name)
        record = app.edit(target, text)
        published = None
    else:
        node = TauNode(config, name=config.name)
        node.start()
        try:
            app = TanApp(
                config,
                node.identity,
                name=config.name,
                request_timeout=float(getattr(args, "timeout", 15.0)),
            )
            node.attach_app(app)
            targets = None
            if getattr(args, "to", None):
                targets = [node.resolve_peer(target) for target in args.to]
            record = app.edit(target, text)
            published = app.publish(
                record,
                targets=targets,
                ttl=max(0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)),
            )
        finally:
            node.stop()

    if json_output:
        _emit({"edit": record, "published": published}, True)
    else:
        print(f"edited {short_hash(target)}")
        if published is None:
            print("  stored locally (not published)")
        else:
            print(
                f"  published to {published['delivered']}/{published['peers']} peer(s)"
            )
            for error in published["errors"]:
                print(f"  failed {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_tan_timeline(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    blocked = load_blocked_set(config.blocklist_path)
    conn = init_db(config.social_db_path)
    try:
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        if getattr(args, "author", None):
            author, _ = resolve_author(
                conn, args.author, registry=PeerRegistry(config.peers_path)
            )
            posts = author_timeline(
                conn,
                author,
                limit=args.limit,
                viewer=viewer,
                exclude_authors=blocked,
                exclude_all=blocked is None,
            )
        elif getattr(args, "all", False):
            posts = timeline(
                conn,
                include_all=True,
                limit=args.limit,
                viewer=viewer,
                exclude_authors=blocked,
                exclude_all=blocked is None,
            )
        else:
            authors = [record["author"] for record in following(conn)]
            if viewer is not None:
                authors.append(viewer)
            posts = timeline(
                conn, authors, limit=args.limit, viewer=viewer,
                exclude_authors=blocked,
                exclude_all=blocked is None,
            )
    finally:
        conn.close()
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    posts = apply_display_rules(posts, display_ruleset)
    _print_tan_posts(posts, json_output=json_output)
    return _EXIT_OK


def cmd_tan_search(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    blocked = load_blocked_set(config.blocklist_path)
    conn = init_db(config.social_db_path)
    try:
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        author = None
        if getattr(args, "author", None):
            author, _ = resolve_author(
                conn, args.author, registry=PeerRegistry(config.peers_path)
            )
        posts = search_posts(
            conn,
            args.query,
            author=author,
            limit=args.limit,
            viewer=viewer,
            exclude_authors=blocked,
            exclude_all=blocked is None,
        )
    finally:
        conn.close()
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    posts = apply_display_rules(posts, display_ruleset)
    _print_tan_posts(
        posts,
        json_output=json_output,
        empty_message=f"No stored posts match {args.query!r}.",
    )
    return _EXIT_OK


def cmd_tan_top(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    conn = init_db(config.social_db_path)
    try:
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        author = None
        if getattr(args, "author", None):
            author, _ = resolve_author(
                conn, args.author, registry=PeerRegistry(config.peers_path)
            )
        posts = top_posts(
            conn,
            author=author,
            limit=args.limit,
            viewer=viewer,
            ruleset=display_ruleset,
        )
    finally:
        conn.close()
    posts = apply_display_rules(posts, display_ruleset)
    _print_tan_posts(
        posts,
        json_output=json_output,
        empty_message="No stored posts yet.",
    )
    return _EXIT_OK


def cmd_tan_verify(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    path = Path(config.social_db_path).expanduser()
    # Audit a read-only connection to the existing file: opening through
    # init_db would create a missing store, or migrate and rewrite an old one.
    try:
        if not path.is_file():
            raise FileNotFoundError(f"no Tan store at {path}")
        conn = sqlite3.connect(
            f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=10.0
        )
        conn.row_factory = sqlite3.Row
        try:
            report = verify_store(conn)
        finally:
            conn.close()
    except (OSError, sqlite3.Error) as exc:
        message = f"could not audit Tan store at {path}: {exc}"
        if json_output:
            _emit({"ok": False, "error": message}, True)
        else:
            print(f"tau: error: {message}", file=sys.stderr)
        return _EXIT_ERROR
    if json_output:
        _emit(report, True)
        return _EXIT_OK if report["ok"] else _EXIT_ERROR
    if report["ok"]:
        print(
            f"Tan store OK: {report['entries']} entries from "
            f"{report['authors']} authors verified."
        )
        if report["unlinked"]:
            print(
                f"{report['unlinked']} unlinked entries are waiting for "
                "their parent to arrive."
            )
        return _EXIT_OK
    for problem in report["problems"]:
        print(
            f"{_safe_id_prefix(problem['hash'])} "
            f"({short_hash(problem['author'])}): {problem['problem']}"
        )
    print(
        f"Tan store has {len(report['problems'])} problem(s) in "
        f"{report['entries']} entries."
    )
    return _EXIT_ERROR


def cmd_tan_thread(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        blocked = load_blocked_set(config.blocklist_path)
        posts = thread(
            conn,
            args.ref,
            viewer=viewer,
            exclude_authors=blocked,
            exclude_all=blocked is None,
        )
    finally:
        conn.close()
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    posts = apply_display_rules(posts, display_ruleset)
    _print_tan_thread(posts, json_output=json_output)
    return _EXIT_OK


def cmd_tan_likes(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        blocked = load_blocked_set(config.blocklist_path)
        result = likers(
            conn,
            args.ref,
            exclude_authors=blocked,
            exclude_all=blocked is None,
        )
    finally:
        conn.close()
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    if not apply_display_rules([result["post"]], display_ruleset):
        # Like thread() and search(): a post the owner's display rules
        # hide is not shown here either.
        raise TanError(f"post is hidden by your Tan display rules: {args.ref!r}")
    _print_tan_likes(result, json_output=json_output)
    return _EXIT_OK


def cmd_tan_mentions(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        # Mentions are defined relative to this node's identity: a
        # reply to one of your posts.  Without an identity there is
        # nothing of yours to be replied to.
        identity = _tan_identity(config)
        viewer = destination_hash(identity).hex()
        posts = mentions(conn, viewer, limit=args.limit)
    finally:
        conn.close()
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    posts = apply_display_rules(posts, display_ruleset)
    _print_tan_posts(
        posts,
        json_output=json_output,
        empty_message="No replies to your posts yet.",
    )
    return _EXIT_OK


def cmd_tan_follow(args: argparse.Namespace) -> int:
    config = _make_config(args)
    config.prepare()
    conn = init_db(config.social_db_path)
    try:
        author, name = resolve_author(
            conn, args.target, registry=PeerRegistry(config.peers_path)
        )
        record = follow(conn, author, name=name)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(record, True)
    else:
        print(f"following {name or author} ({short_hash(author)})")
    return _EXIT_OK


def cmd_tan_unfollow(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        author, name = resolve_author(
            conn, args.target, registry=PeerRegistry(config.peers_path)
        )
        removed = unfollow(conn, author)
    finally:
        conn.close()
    if _json_mode(args):
        _emit({"author": author, "unfollowed": removed}, True)
    else:
        if removed:
            print(f"unfollowed {name or author} ({short_hash(author)})")
        else:
            print(f"was not following {name or author} ({short_hash(author)})")
    return _EXIT_OK


def cmd_tan_following(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        records = following(conn)
        for record in records:
            record.update(author_stats(conn, record["author"]))
    finally:
        conn.close()

    if _json_mode(args):
        _emit({"following": records, "count": len(records)}, True)
    else:
        if not records:
            print("Not following anyone. Try `tau tan follow alice`.")
        for record in records:
            name = record.get("name") or "unknown"
            print(
                f"{name:<20} {short_hash(record['author']):<10} "
                f"{record.get('posts', 0)} post(s)"
            )
    return _EXIT_OK


def cmd_tan_authors(args: argparse.Namespace) -> int:
    config = _make_config(args)
    limit = getattr(args, "limit", 50)
    if limit is not None and limit < 0:
        raise NodeError("--limit must not be negative")
    # A read-only listing must not create the store on a fresh data dir.
    if not config.social_db_path.exists():
        records: List[Dict[str, Any]] = []
    else:
        conn = open_readonly_db(config.social_db_path)
        try:
            records = list_authors(conn, sort=args.sort, limit=limit)
        finally:
            conn.close()

    if _json_mode(args):
        _emit({"authors": records, "count": len(records)}, True)
        return _EXIT_OK
    if not records:
        print("No authors in the local Tan store yet. Post or sync first.")
        return _EXIT_OK
    for record in records:
        name = record.get("name") or "unknown"
        last = (
            _format_message_time(record["last_post"])
            if record["last_post"]
            else "never"
        )
        followed = "following" if record.get("followed") else ""
        print(
            f"{name:<20} {short_hash(record['author']):<10} "
            f"{record.get('posts', 0)} post(s)  last {last}  {followed}".rstrip()
        )
    return _EXIT_OK


def cmd_tan_suggest(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        suggestions = suggest_authors(conn, viewer=viewer, limit=args.limit)
    finally:
        conn.close()

    if json_output:
        _emit({"suggestions": suggestions, "count": len(suggestions)}, True)
    else:
        if not suggestions:
            print("No suggestions yet. Follow an author or sync more posts first.")
        for item in suggestions:
            name = item.get("name") or "unknown"
            print(
                f"{name:<20} {short_hash(item['author']):<10} "
                f"{item.get('posts', 0)} post(s), {item.get('likes', 0)} like(s) "
                f"— {item.get('reason', '')}"
            )
    return _EXIT_OK


def _resolve_mute_target(
    conn: sqlite3.Connection, target: str, registry: PeerRegistry
) -> Tuple[str, str]:
    """Resolve a mute/unmute target, falling back to stored names.

    The peer registry does not know authors whose posts are only cached,
    and a muted author may have no cached posts at all — so after the
    registry/hash/prefix lookup fails, try the name stored with the
    mute, then the name on the author's cached posts.
    """
    try:
        return resolve_author(conn, target, registry=registry)
    except TanError as exc:
        match = resolve_muted_name(conn, target)
        if match is not None:
            return match
        try:
            return resolve_cached_author(conn, target)
        except TanError:
            raise exc from None


def cmd_tan_mute(args: argparse.Namespace) -> int:
    config = _make_config(args)
    config.prepare()
    conn = init_db(config.social_db_path)
    try:
        author, name = _resolve_mute_target(
            conn, args.target, PeerRegistry(config.peers_path)
        )
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        if viewer is not None and author == viewer:
            raise TanError("cannot mute yourself")
        record = mute(conn, author, name=name)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(record, True)
    else:
        print(f"muted {name or author} ({short_hash(author)})")
    return _EXIT_OK


def cmd_tan_unmute(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        author, name = _resolve_mute_target(
            conn, args.target, PeerRegistry(config.peers_path)
        )
        removed = unmute(conn, author)
    finally:
        conn.close()
    if _json_mode(args):
        _emit({"author": author, "unmuted": removed}, True)
    else:
        if removed:
            print(f"unmuted {name or author} ({short_hash(author)})")
        else:
            print(f"was not muted: {name or author} ({short_hash(author)})")
    return _EXIT_OK


def cmd_tan_muted(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        records = muted(conn)
        for record in records:
            record.update(author_stats(conn, record["author"]))
    finally:
        conn.close()

    if _json_mode(args):
        _emit({"muted": records, "count": len(records)}, True)
    else:
        if not records:
            print("No muted authors. Mute one with `tau tan mute alice`.")
        for record in records:
            name = record.get("name") or "unknown"
            when = datetime.fromtimestamp(float(record["muted_at"])).strftime(
                "%Y-%m-%d %H:%M"
            )
            print(
                f"{name:<20} {short_hash(record['author']):<10} "
                f"{record.get('posts', 0)} post(s) muted {when}"
            )
    return _EXIT_OK


def cmd_tan_mute_keyword(args: argparse.Namespace) -> int:
    config = _make_config(args)
    config.prepare()
    conn = init_db(config.social_db_path)
    try:
        record = mute_keyword(conn, args.keyword)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(record, True)
    else:
        print(f"muted keyword {record['display']!r}")
    return _EXIT_OK


def cmd_tan_unmute_keyword(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        removed = unmute_keyword(conn, args.keyword)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(
            {
                "keyword": " ".join(args.keyword.split()).casefold(),
                "unmuted": removed,
            },
            True,
        )
    else:
        if removed:
            print(f"unmuted keyword {args.keyword.strip()!r}")
        else:
            print(f"keyword {args.keyword.strip()!r} was not muted")
    return _EXIT_OK


def cmd_tan_muted_keywords(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        records = muted_keywords(conn)
    finally:
        conn.close()

    if _json_mode(args):
        _emit({"muted_keywords": records, "count": len(records)}, True)
    else:
        if not records:
            print("No muted keywords. Try `tau tan mute-keyword WORD`.")
        for record in records:
            when = datetime.fromtimestamp(float(record["added_at"])).strftime(
                "%Y-%m-%d %H:%M"
            )
            print(f"{record['display']} (muted {when})")
    return _EXIT_OK


def cmd_tan_save(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        record = save_post(conn, args.ref)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(record, True)
    else:
        if record.get("already_saved"):
            when = datetime.fromtimestamp(float(record["saved_at"])).strftime(
                "%Y-%m-%d %H:%M"
            )
            print(f"already saved {short_hash(str(record['hash']))} at {when}")
        else:
            print(f"saved {short_hash(str(record['hash']))}")
    return _EXIT_OK


def cmd_tan_unsave(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        record = unsave_post(conn, args.ref)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(record, True)
    else:
        if record["unsaved"]:
            print(f"unsaved {short_hash(str(record['hash']))}")
        else:
            print(f"was not saved: {short_hash(str(record['hash']))}")
    return _EXIT_OK


def cmd_tan_saved(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    try:
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        posts, _ = annotated_saved_posts(
            conn, limit=args.limit, viewer=viewer, ruleset=display_ruleset
        )
    finally:
        conn.close()
    _print_saved_posts(posts, json_output=json_output)
    return _EXIT_OK
    return _EXIT_OK


def cmd_tan_rules(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        ruleset = load_ruleset(config.tan_rules_path)
    except RulesError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return _EXIT_ERROR
    data = ruleset.to_dict()
    changed = False

    author_args = (
        ("allow_authors", getattr(args, "allow_author", None)),
        ("block_authors", getattr(args, "block_author", None)),
    )
    removal_args = (
        ("allow_authors", getattr(args, "unallow_author", None)),
        ("block_authors", getattr(args, "unblock_author", None)),
    )
    if any(values for _, values in author_args + removal_args):
        conn = init_db(config.social_db_path)
        try:
            registry = PeerRegistry(config.peers_path)
            for field_name, values in author_args:
                for value in values or []:
                    author, _ = resolve_author(conn, value, registry=registry)
                    if author not in data[field_name]:
                        data[field_name].append(author)
                        changed = True
            for field_name, values in removal_args:
                for value in values or []:
                    author, _ = resolve_author(conn, value, registry=registry)
                    if author in data[field_name]:
                        data[field_name].remove(author)
                        changed = True
        finally:
            conn.close()

    if getattr(args, "default", None) is not None:
        data["default"] = args.default
        changed = True
    followed_only = getattr(args, "followed_only", None)
    if followed_only is not None:
        data["allow_followed"] = bool(followed_only)
        changed = True
    if getattr(args, "all_kinds", False):
        data["kinds"] = None
        changed = True
    elif getattr(args, "kinds", None) is not None:
        data["kinds"] = [
            part.strip() for part in str(args.kinds).split(",") if part.strip()
        ]
        changed = True

    if changed:
        try:
            ruleset = Ruleset.from_dict(data)
        except RulesError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return _EXIT_ERROR
        data = ruleset.to_dict()
        save_ruleset(config.tan_rules_path, ruleset)

    if json_output:
        payload = ruleset.to_dict()
        payload["path"] = str(config.tan_rules_path)
        _emit(payload, True)
    else:
        print(f"Tan admission rules ({config.tan_rules_path}):")
        print(f"  default: {data['default']}")
        print(
            f"  followed authors admitted: {'yes' if data['allow_followed'] else 'no'}"
        )
        kinds = data["kinds"]
        print(
            f"  kinds displayed: {', '.join(kinds) if kinds else 'all'}"
            " (display-only; all kinds are stored)"
        )
        for label, field_name in (
            ("allow", "allow_authors"),
            ("block", "block_authors"),
        ):
            authors = data[field_name]
            print(f"  {label} list: {len(authors)} author(s)")
            for author in authors:
                print(f"    {author}")
        if changed:
            print("  (a running serve applies saved rules when it restarts)")
    return _EXIT_OK


def cmd_tan_sync(args: argparse.Namespace) -> int:
    config = _make_config(args)
    identity = load_or_create_identity(config.identity_path)
    node = TauNode(config, name=config.name)
    node.start(announce=False)
    try:
        app = TanApp(
            config,
            identity,
            name=config.name,
            request_timeout=float(getattr(args, "timeout", 15.0)),
        )
        node.attach_app(app)
        targets = None
        if getattr(args, "to", None):
            targets = [node.resolve_peer(target) for target in args.to]

        authors = None
        if getattr(args, "author", None):
            conn = app.connect()
            try:
                authors = [
                    resolve_author(
                        conn, target, registry=PeerRegistry(config.peers_path)
                    )[0]
                    for target in args.author
                ]
            finally:
                conn.close()

        summary = app.sync(
            node,
            targets=targets,
            authors=authors,
            limit=int(getattr(args, "limit", DEFAULT_FEED_LIMIT)),
            explore=bool(getattr(args, "explore", False)),
            timeout=float(getattr(args, "timeout", 15.0)),
        )
    finally:
        node.stop()

    if _json_mode(args):
        _emit(summary, True)
    else:
        print(
            f"synced {summary['stored']} new post(s) "
            f"from {summary['peers']} peer(s); "
            f"{summary['duplicates']} duplicate(s)"
        )
        for error in summary["errors"]:
            print(f"  {short_hash(error['peer'])}: {error['error']}")
    return _EXIT_OK


def cmd_tan_export(args: argparse.Namespace) -> int:
    config = _make_config(args)
    conn = init_db(config.social_db_path)
    try:
        if getattr(args, "all", False):
            if getattr(args, "author", None):
                raise TanError("--author and --all cannot be combined")
            document = export_entries(conn, all_authors=True)
            scope = "all authors"
        else:
            if getattr(args, "author", None):
                try:
                    author, _ = resolve_author(
                        conn,
                        args.author,
                        registry=PeerRegistry(config.peers_path),
                    )
                except TanError as exc:
                    # The peer registry does not know authors whose posts
                    # are only cached; fall back to the name on the posts.
                    try:
                        author, _ = resolve_cached_author(conn, args.author)
                    except TanError:
                        raise exc from None
            else:
                identity = _tan_identity(config)
                author = destination_hash(identity).hex()
            document = export_entries(conn, author=author)
            scope = f"author {short_hash(author)}"
    finally:
        conn.close()
    count = len(document["entries"])
    text = json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True)
    if getattr(args, "output", None):
        atomic_write_text(args.output, text + "\n")
        print(f"Exported {count} entries ({scope}) to {args.output}")
    else:
        print(text)
    return _EXIT_OK


def cmd_tan_import(args: argparse.Namespace) -> int:
    config = _make_config(args)
    if str(args.file) == "-":
        raw = sys.stdin.read()
    else:
        raw = args.file.read_text(encoding="utf-8")
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise TanError(f"export file is not valid JSON: {exc}") from exc
    conn = init_db(config.social_db_path)
    try:
        result = import_entries(conn, document)
    finally:
        conn.close()
    if _json_mode(args):
        _emit(result, True)
    else:
        print(
            f"Imported {result['imported']} entries "
            f"({result['present']} already present, "
            f"{result['skipped']} skipped)"
        )
        for error in result["errors"]:
            ref = short_hash(error["hash"]) if error["hash"] else "?"
            print(f"  skipped {ref}: {error['error']}")
    return _EXIT_OK if result["skipped"] == 0 else _EXIT_ERROR


def _refresh_served_name(
    config: TauConfig, node: Any, args: argparse.Namespace, *, no_announce: bool
) -> None:
    """Pick up a rename that landed between config resolution and the serve lock.

    Once the serving lock is held no rename can write, so the stored name is
    stable here. A hand-edited ``node.json`` bypasses ``init``'s validation,
    so the re-read name is sanitised exactly as config resolution does.
    An explicit ``--name`` override wins over the stored name.
    """
    if getattr(args, "name", None) is not None:
        return
    stored_name = load_node_name(config.data_dir)
    if not stored_name:
        return
    stored_name = sanitize_name(stored_name)
    if stored_name != node.name:
        node.name = stored_name
        if not no_announce:
            node.announce()
def cmd_bundle_export(args: argparse.Namespace) -> int:
    config = _make_config(args)
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(
            f"no identity at {config.identity_path}; run `tau init` first"
        )
    mailbox = Mailbox(config.mailbox_path)
    registry = PeerRegistry(config.peers_path)
    include_tan = bool(getattr(args, "tan", False))
    tan_conn = None
    try:
        if include_tan:
            tan_conn = init_db(config.social_db_path)
        document, summary = build_bundle(
            identity=identity,
            sender_name=config.name,
            mailbox=mailbox,
            registry=registry,
            tan_conn=tan_conn,
            include_tan=include_tan,
            max_bytes=int(
                getattr(args, "max_bytes", DEFAULT_MAX_BUNDLE_BYTES)
            ),
        )
    finally:
        if tan_conn is not None:
            tan_conn.close()
    atomic_write_bytes(
        args.file, serialize_bundle(document), restrict_parent=False
    )
    if _json_mode(args):
        _emit(summary, True)
    else:
        tan_text = (
            f" and {summary['tan']} Tan post(s)" if include_tan else ""
        )
        print(
            f"Exported bundle with {summary['mail']} sealed message(s)"
            f"{tan_text} ({summary['bytes']} bytes) to {args.file}"
        )
        for skipped in summary["skipped"]:
            print(f"  skipped {_safe_id_prefix(skipped['id'])}: {skipped['reason']}")
    return _EXIT_OK


def _bundle_tan_admit(
    config: TauConfig, identity: Any, tan_conn: Any
) -> Any:
    """Admission check for Tan posts carried in a bundle.

    Uses the same ruleset the network path applies.  A ruleset that
    cannot be parsed fails closed, denying remote posts, as ``TanApp``
    does.
    """
    try:
        ruleset = load_ruleset(config.tan_rules_path)
    except RulesError as exc:
        print(
            f"tau: warning: Tan ruleset unusable ({exc}); denying remote Tan posts",
            file=sys.stderr,
        )
        ruleset = Ruleset(default_allow=False)
    own_author = _author_from_identity(identity) if identity is not None else None
    followed: List[str] = []
    if ruleset.allow_followed:
        followed = [record["author"] for record in following(tan_conn)]
    return lambda author: ruleset.decide(
        author, own_author=own_author, followed=followed
    )


def cmd_bundle_import(args: argparse.Namespace) -> int:
    config = _make_config(args)
    max_bytes = int(getattr(args, "max_bytes", DEFAULT_MAX_BUNDLE_BYTES))
    if max_bytes <= 0:
        raise NodeError("max bundle size must be positive")
    # Read at most one byte past the cap, so an oversized input is
    # detected without being loaded whole.
    if str(args.file) == "-":
        raw = sys.stdin.buffer.read(max_bytes + 1)
    else:
        try:
            with Path(args.file).expanduser().open("rb") as handle:
                raw = handle.read(max_bytes + 1)
        except OSError as exc:
            raise NodeError(f"could not read {args.file}: {exc}") from exc
    if len(raw) > max_bytes:
        raise NodeError(f"bundle is over the {max_bytes} byte cap")
    document = parse_bundle(raw, max_bytes=max_bytes)
    mailbox = Mailbox(config.mailbox_path)
    tan_conn = None
    try:
        if document.get("tan") is not None:
            tan_conn = init_db(config.social_db_path)
        admit = None
        if tan_conn is not None:
            identity = load_identity(config.identity_path)
            admit = _bundle_tan_admit(config, identity, tan_conn)
        result = apply_bundle(
            mailbox=mailbox, tan_conn=tan_conn, document=document, admit=admit
        )
    finally:
        if tan_conn is not None:
            tan_conn.close()
    if _json_mode(args):
        _emit(result, True)
    else:
        tan = result["tan"]
        tan_text = ""
        if tan is not None:
            tan_text = (
                f"; Tan: {tan['imported']} imported, "
                f"{tan['present']} already present, "
                f"{tan['skipped']} skipped"
            )
        print(
            f"Imported {result['imported']} sealed message(s) "
            f"({result['present']} already held, "
            f"{result['skipped']} skipped){tan_text}"
        )
        for error in result["errors"]:
            ref = _safe_id_prefix(error["id"]) if error["id"] else "?"
            print(f"  skipped {ref}: {error['error']}")
    tan_clean = result["tan"] is None or result["tan"]["skipped"] == 0
    return _EXIT_OK if result["skipped"] == 0 and tan_clean else _EXIT_ERROR


def cmd_serve(args: argparse.Namespace) -> int:
    if getattr(args, "lxmf_propagation", False) and not getattr(
        args, "lxmf", False
    ):
        raise NodeError("--lxmf-propagation requires --lxmf")
    interval = max(
        0.0, float(getattr(args, "announce_interval", DEFAULT_ANNOUNCE_INTERVAL))
    )
    config = _make_config(args, announce_interval=interval)

    bridge = None
    web_settings = None
    http_bind = getattr(args, "http", None)
    if http_bind:
        from .webapi import parse_bind

        web_dir = getattr(args, "web_dir", None) or _default_web_dir()
        try:
            host, port = parse_bind(http_bind)
        except ValueError as exc:
            raise NodeError(str(exc)) from exc
        web_settings = (host, port, web_dir)

    console_peer, console_message, console_file = _node_callbacks(args)
    node = TauNode(
        config,
        name=config.name,
        on_message=console_message,
        on_peer=console_peer,
        on_file=console_file,
    )
    if web_settings is not None:
        from .webapi import (
            WebBridge,
            WebBridgeError,
            check_web_console_bind,
            file_arrival_text,
        )

        host, port, web_dir = web_settings
        try:
            bridge = WebBridge(
                node,
                host=host,
                port=port,
                web_dir=web_dir,
                allow_remote=bool(getattr(args, "http_allow_remote", False)),
            )
        except WebBridgeError as exc:
            raise NodeError(str(exc)) from exc

        def on_peer(peer: Peer) -> None:
            console_peer(peer)
            bridge.record_peer(peer)

        def on_message(record: Dict[str, Any]) -> None:
            console_message(record)
            bridge.record_inbound(record)

        def on_file(record: Dict[str, Any]) -> None:
            console_file(record)
            # The console lists messages, not files: record the arrival
            # as an inbound message so the file shows up in its feed.
            # The file metadata rides along so the feed can offer the
            # bytes back as a download (GET /api/files/<sender>/<name>).
            bridge.record_inbound(
                {
                    "destination": record.get("destination"),
                    "from": record.get("from"),
                    "text": file_arrival_text(record),
                    "sent_at": record.get("sent_at"),
                    "received_at": record.get("received_at"),
                    "file": {
                        "name": record.get("name"),
                        "size": record.get("size"),
                        "sha256": record.get("sha256"),
                    },
                }
            )

        node.on_peer = on_peer
        node.on_message = on_message
        node.on_file = on_file

        def on_retract(record: Dict[str, Any]) -> None:
            # The node already deleted the inbox record; drop the
            # console's own copy so a retracted DM leaves the feed
            # and never comes back after a restart.
            bridge.delete_by_wire_id(str(record.get("id") or ""))

        node.on_retract = on_retract
    # The data-directory lock is the more specific guard, so probe it
    # before the port check: a second serve on the same directory
    # reports the instance error even though it would also trip the
    # port check below. node.start(exclusive=True) re-checks it
    # authoritatively when it takes the lock for real.
    probe = InstanceLock(config.instance_lock_path, config.data_dir)
    probe.acquire()
    probe.release()
    # A port already held by another node would leave Reticulum's TCP
    # server interface silently dead (panic_on_interface_error = No),
    # so fail fast here with the port named instead of serving deaf.
    check_tcp_listen_ports(config)
    # Same for the web console: its bind used to fail only after the node
    # had already started, with a bare "[Errno 98] Address already in
    # use". Probe first so the error names the console address.
    if web_settings is not None:
        host, port, _ = web_settings
        try:
            check_web_console_bind(host, port)
        except WebBridgeError as exc:
            raise NodeError(str(exc)) from exc
    no_announce = bool(getattr(args, "no_announce", False))
    node.start(
        announce=not no_announce,
        exclusive=True,
        lxmf=bool(getattr(args, "lxmf", False)),
        lxmf_propagation=bool(getattr(args, "lxmf_propagation", False)),
    )
    _refresh_served_name(config, node, args, no_announce=no_announce)

    social_app = None
    if getattr(args, "social", False):

        def on_tan_post(record: Dict[str, Any]) -> None:
            if _json_mode(args):
                _emit_event(args, "post", post=record)
            else:
                name = record.get("name") or "anonymous"
                author = short_hash(str(record.get("author", "")))
                print(
                    f"[{_stamp(record.get('created_at'))}] post from {name} ({author}):",
                    flush=True,
                )
                for line in str(record.get("text", "")).splitlines() or [""]:
                    print(f"  {line}", flush=True)
            if bridge is not None:
                try:
                    bridge.record_tan_post(record)
                except Exception:  # pragma: no cover - optional web layer
                    log.debug(
                        "Could not forward Tan post to web console", exc_info=True
                    )

        social_app = TanApp(
            config,
            node.identity,
            # The node's name, not the resolved config name: a rename that
            # landed during startup refreshes node.name (see
            # _refresh_served_name), and posts must carry the same name the
            # node announces with.
            name=node.name,
            on_post=on_tan_post,
        )
        node.attach_app(social_app)

    console_url = None
    if bridge is not None:
        try:
            console_url = bridge.start()
        except WebBridgeError as exc:
            # The pre-start probe covers the usual collision; a bind race
            # can still land here. Stop the node cleanly instead of
            # exiting with it running, and keep the named error.
            node.stop()
            raise NodeError(str(exc)) from exc

    stop = threading.Event()
    previous_handlers = _install_stop_handlers(stop)

    social_interval = max(0.0, float(getattr(args, "social_sync_interval", 0.0) or 0.0))
    if social_app is not None and social_interval > 0:

        def social_sync_loop() -> None:
            while not stop.wait(social_interval):
                try:
                    summary = social_app.sync(
                        node, limit=DEFAULT_FEED_LIMIT, explore=True
                    )
                    if summary.get("stored"):
                        log.info("Tan sync stored %s new post(s)", summary["stored"])
                except Exception:  # pragma: no cover - network dependent
                    log.warning("Tan background sync failed", exc_info=True)

        threading.Thread(
            target=social_sync_loop, name="tau-tan-sync", daemon=True
        ).start()

    publish_server = None
    if social_app is not None and hasattr(socket, "AF_UNIX"):
        from .local_publish import PublishServer

        try:
            publish_server = PublishServer(
                config.data_dir,
                lambda **request: _publish_local_post(node, **request),
            )
        except BaseException:
            if bridge is not None:
                bridge.stop()
            node.stop()
            _restore_stop_handlers(previous_handlers)
            raise

    send_server = None
    if hasattr(socket, "AF_UNIX"):
        from .local_send import SendServer

        try:
            send_server = SendServer(
                config.data_dir,
                lambda **request: _dispatch_local_send(node, **request),
            )
        except Exception:
            # The send socket is best-effort: a data directory too long
            # for a Unix socket path, or a filesystem without socket
            # support, must not stop the node from serving. One-shot
            # sends fail closed in that case — the lock probe refuses to
            # start a second instance on this identity — so no send is
            # lost silently.
            log.warning("Could not start the local send server", exc_info=True)

    info = node.info()
    send_socket = send_server is not None
    if _json_mode(args):
        _emit_event(args, "ready", node=info, send_socket=send_socket)
        if console_url is not None:
            _emit_event(args, "web", url=console_url, api=f"{console_url}api/")
        if social_app is not None:
            _emit_event(
                args,
                "social",
                enabled=True,
                db=str(config.social_db_path),
                feed="/tan/feed",
            )
    else:
        print(f'tau node "{info["name"]}" ready', flush=True)
        print(f"  destination: {info['destination']}")
        print(f"  rns:         {info['rns_version']}")
        lxmf_info = info.get("lxmf") or {}
        if lxmf_info.get("enabled"):
            print(f"  lxmf:        delivery {lxmf_info.get('delivery')}")
            if lxmf_info.get("propagation"):
                print(
                    f"  lxmf:        propagation {lxmf_info.get('propagation')}"
                )
        print(f"  data dir:    {config.data_dir}")
        if not send_socket and hasattr(socket, "AF_UNIX"):
            # Best-effort serve without a send socket: say so up front,
            # so the operator can see one-shot sends will fail while
            # this node serves instead of wondering why.
            print(
                "  send:        socket unavailable — tau send fails while "
                "this node serves"
            )
        mailbox_counts = info.get("mailbox") or {}
        if mailbox_counts.get("outbox") or mailbox_counts.get("held"):
            print(
                f"  mail:        {mailbox_counts.get('outbox', 0)} queued, "
                f"{mailbox_counts.get('held', 0)} held for others"
            )
        if console_url is not None:
            print(f"  console:     {console_url}")
            print(f"  api:         {console_url}api/")
        if social_app is not None:
            print(f"  social:      Tan feed at /tan/feed")
            if social_interval > 0:
                print(f"  social sync: every {social_interval:g}s")
        print("Listening for peers and messages. Press Ctrl-C to stop.", flush=True)

    duration = getattr(args, "duration", None)
    try:
        if duration is not None:
            stop.wait(max(0.0, float(duration)))
        else:
            while not stop.wait(0.5):
                pass
    finally:
        if send_server is not None:
            send_server.stop()
        if publish_server is not None:
            publish_server.stop()
        if bridge is not None:
            bridge.stop()
        node.stop()
        _restore_stop_handlers(previous_handlers)
    return _EXIT_OK


def cmd_discover(args: argparse.Namespace) -> int:
    config = _make_config(args, announce_interval=0.0)

    def on_peer(peer: Peer) -> None:
        if not _json_mode(args):
            print(
                f"[{_stamp()}] {peer.display_name} ({peer.hexhash}) hops={peer.hops}",
                flush=True,
            )

    node = TauNode(config, name=config.name, on_peer=on_peer)
    node.start(announce=False)
    duration = max(0.0, float(getattr(args, "duration", 15.0)))
    stop = threading.Event()
    previous_handlers = _install_stop_handlers(stop)
    try:
        stop.wait(duration)
    finally:
        node.stop()
        _restore_stop_handlers(previous_handlers)

    max_age_days = getattr(args, "max_age", None)
    max_age = float(max_age_days) * 86400.0 if max_age_days is not None else None
    peers = node.registry.all(max_age=max_age)
    if _json_mode(args):
        _emit(
            {
                "peers": [peer.to_dict() for peer in peers],
                "count": len(peers),
                "duration": duration,
            },
            True,
        )
    else:
        print()
        _print_peer_table(peers)
        print(f"\n{len(peers)} peer(s) in {duration:.1f}s")
    return _EXIT_OK


def cmd_peers(args: argparse.Namespace) -> int:
    config = _make_config(args)
    if getattr(args, "exchange", False):
        return cmd_peers_exchange(args, config)
    registry = PeerRegistry(config.peers_path)
    max_age_days = getattr(args, "max_age", None)
    max_age = float(max_age_days) * 86400.0 if max_age_days is not None else None
    peers = registry.all(max_age=max_age)
    safety_numbers: Optional[Dict[str, str]] = None
    if getattr(args, "safety_number", False):
        identity = load_identity(config.identity_path)
        if identity is None:
            raise NodeError(
                f"no identity at {config.identity_path}; run `tau init` first"
            )
        local_key = identity.get_public_key()
        safety_numbers = {
            peer.hexhash: safety_number(local_key, peer.public_key)
            for peer in peers
        }
    if _json_mode(args):
        payload = [peer.to_dict() for peer in peers]
        if safety_numbers is not None:
            for entry in payload:
                entry["safety_number"] = safety_numbers[entry["destination"]]
        _emit({"peers": payload, "count": len(peers)}, True)
    else:
        _print_peer_table(peers, safety_numbers=safety_numbers)
    return _EXIT_OK


def cmd_peers_verify(args: argparse.Namespace) -> int:
    """Mark a peer verified after comparing safety numbers out of band."""
    config = _make_config(args)
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(
            f"no identity at {config.identity_path}; run `tau init` first"
        )
    registry = PeerRegistry(config.peers_path)
    try:
        peer = registry.resolve(args.target)
    except PeerError as exc:
        raise NodeError(str(exc)) from exc
    if peer is None:
        raise NodeError(f"no peer matches {args.target!r}")
    number = safety_number(identity.get_public_key(), peer.public_key)
    previously_verified = peer.is_verified
    previous_fingerprint = peer.verified_fingerprint
    registry.mark_verified(peer.destination_hash, number)
    registry.save()
    if _json_mode(args):
        _emit(
            {
                "peer": peer.hexhash,
                "name": peer.display_name,
                "safety_number": number,
                "verified_at": peer.verified_at,
                "reverified": previously_verified,
                "fingerprint_changed": previously_verified
                and previous_fingerprint != number,
            },
            True,
        )
    else:
        print(f"Verified {peer.display_name} ({short_hash(peer.hexhash)}).")
        print(f"  safety number: {number}")
        if previously_verified and previous_fingerprint != number:
            print(
                "  note: the recorded safety number changed since the last "
                "verification (the local identity changed); updated."
            )
        print(
            "  Compare this number with the peer over another channel; "
            "if it matches, the key is theirs."
        )
    return _EXIT_OK


def cmd_peers_forget(args: argparse.Namespace) -> int:
    """Remove one peer from the local registry.

    A serving node keeps its own in-memory registry and rewrites the file
    on its next save, so forgetting refuses while one is running on this
    data directory.  The mailbox transaction is held across the fresh
    safety checks and the registry removal, mirroring ``_save_peers``:
    a one-shot send cannot queue mail for the peer between the check
    and the removal.  Mail sweeps iterate registered peers, so forgetting
    a peer with queued mail would strand those messages until it is
    rediscovered (or they expire): refuse unless ``--force`` is passed.
    """
    config = _make_config(args)
    config.data_dir.mkdir(parents=True, exist_ok=True)
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    try:
        lock.acquire()
    except InstanceAlreadyRunning as exc:
        raise NodeError(
            "a tau node is already serving on this data directory; "
            "stop it before forgetting a peer"
        ) from exc
    try:
        mailbox = Mailbox(config.mailbox_path)
        with mailbox.transaction():
            registry = PeerRegistry(config.peers_path)
            try:
                peer = registry.resolve(args.target)
            except PeerError as exc:
                raise NodeError(str(exc)) from exc
            if peer is None:
                raise NodeError(f"no peer matches {args.target!r}")
            queued = mailbox.outbox_list(to=peer.destination_hash)
            if queued and not getattr(args, "force", False):
                raise NodeError(
                    f"{peer.display_name} still has {len(queued)} queued "
                    "message(s); forgetting this peer would strand them "
                    "until it is rediscovered. Deliver or cancel the mail "
                    "first, or pass --force to forget anyway."
                )
            if peer.hexhash in mailbox.pending_ack_holders():
                raise NodeError(
                    f"{peer.display_name} still has mail acknowledgements owed; "
                    "wait for the next mail sweep to send them, then try again"
                )
            registry.remove(peer.destination_hash)
            registry.save()
    finally:
        lock.release()
    if _json_mode(args):
        _emit(
            {
                "peer": peer.hexhash,
                "name": peer.display_name,
                "forgotten": peer.to_dict(),
                "queued_messages": len(queued),
            },
            True,
        )
    else:
        print(f"Forgot {peer.display_name} ({short_hash(peer.hexhash)}).")
        if queued:
            print(
                f"  warning: {len(queued)} queued message(s) for this peer "
                "will not be attempted until it is rediscovered."
            )
        print("  The next genuine announce from this peer re-adds it.")
    return _EXIT_OK


def cmd_peers_alias(args: argparse.Namespace) -> int:
    config = _make_config(args)
    registry = PeerRegistry(config.peers_path)
    peer = registry.set_alias(args.peer, args.alias)
    if _json_mode(args):
        _emit(peer.to_dict(), True)
    else:
        print(f"alias {peer.alias!r} set for {peer.name} ({peer.hexhash[:8]})")
    return _EXIT_OK


def cmd_peers_unalias(args: argparse.Namespace) -> int:
    config = _make_config(args)
    registry = PeerRegistry(config.peers_path)
    peer = registry.clear_alias(args.peer)
    if _json_mode(args):
        _emit(peer.to_dict(), True)
    else:
        print(f"removed alias for {peer.name} ({peer.hexhash[:8]})")
    return _EXIT_OK


def cmd_peers_show(args: argparse.Namespace) -> int:
    """Show everything the local stores know about one peer.

    Read-only and offline: no node is started and nothing is sent.  The
    registry answers identity and liveness; the mailbox and inbox answer
    how much mail and how many messages involve this peer.
    """
    config = _make_config(args)
    registry = PeerRegistry(config.peers_path)
    target = args.target
    peer = registry.resolve(target)
    if peer is None:
        raise PeerError(f"unknown peer: {target!r}")
    mailbox = Mailbox(config.mailbox_path)
    inbox = Inbox(config.inbox_path)
    queued = mailbox.outbox_list(to=peer.destination_hash)
    held = mailbox.held_for(peer.destination_hash)
    received = inbox.list_messages(peer=peer.hexhash, limit=None)
    now = time.time()
    payload = {
        "name": peer.name,
        "destination": peer.hexhash,
        "lxmf": peer.lxmf_hash,
        "public_key": peer.public_key.hex(),
        "first_seen": peer.first_seen,
        "last_seen": peer.last_seen,
        "hops": peer.hops,
        "protocol": peer.protocol,
        "caps": list(peer.caps),
        "via_exchange": peer.via_exchange,
        "queued": len(queued),
        "held": len(held),
        "received": len(received),
    }
    if _json_mode(args):
        _emit(payload, True)
        return _EXIT_OK
    print(f"name:         {peer.display_name}")
    print(f"destination:  {peer.hexhash}")
    print(f"lxmf:         {peer.lxmf_hash or '—'}")
    print(f"public key:   {peer.public_key.hex()}")
    print(
        f"first seen:   {_format_message_time(peer.first_seen)}"
        f" ({_ago(now - peer.first_seen)} ago)"
    )
    print(
        f"last seen:    {_format_message_time(peer.last_seen)}"
        f" ({_ago(now - peer.last_seen)} ago)"
    )
    print(f"hops:         {peer.hops}")
    print(f"protocol:     {peer.protocol}")
    print(f"caps:         {', '.join(peer.caps) if peer.caps else '—'}")
    print(
        "via exchange: "
        + ("yes — never heard directly" if peer.via_exchange else "no")
    )
    print(f"queued mail:  {len(queued)} message(s) waiting for this peer")
    print(f"held mail:    {len(held)} sealed message(s) held for this peer")
    print(f"received:     {len(received)} message(s) from this peer in the inbox")
    return _EXIT_OK


def cmd_peers_exchange(args: argparse.Namespace, config: TauConfig) -> int:
    """Exchange peer lists with known peers and report what was learned."""
    node = TauNode(config, name=config.name)
    node.start()
    try:
        target = getattr(args, "to", None)
        targets = [node.resolve_peer(target)] if target else node.registry.all()
        results: List[Dict[str, Any]] = []
        for peer in targets:
            try:
                summary = node.exchange_peers(peer)
            except NodeError as exc:
                results.append(
                    {
                        "peer": peer.hexhash,
                        "name": peer.display_name,
                        "ok": False,
                        "error": str(exc),
                    }
                )
                continue
            results.append(
                {"peer": peer.hexhash, "name": peer.display_name, "ok": True, **summary}
            )
        totals = {
            key: sum(int(r.get(key, 0)) for r in results if r.get("ok"))
            for key in ("added", "updated", "skipped")
        }
        totals["peers"] = len(results)
        totals["failed"] = sum(1 for r in results if not r.get("ok"))
    finally:
        node.stop()

    if _json_mode(args):
        _emit({"exchanged": results, **totals}, True)
    else:
        for result in results:
            if result.get("ok"):
                print(
                    f"{result['name']} ({result['peer'][:8]}): "
                    f"{result['added']} new, {result['updated']} updated, "
                    f"{result['skipped']} skipped"
                )
            else:
                print(
                    f"{result['name']} ({result['peer'][:8]}): "
                    f"failed — {result['error']}"
                )
        print(
            f"\n{totals['added']} new peer(s), {totals['updated']} updated, "
            f"{totals['skipped']} skipped from {totals['peers']} peer(s)"
            + (f", {totals['failed']} failed" if totals["failed"] else "")
        )
    # A failed exchange is still reported per peer above, but scripts
    # checking only the exit status must not read it as success.
    return _EXIT_ERROR if totals["failed"] else _EXIT_OK


def cmd_id(args: argparse.Namespace) -> int:
    config = _make_config(args)
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    destination = destination_hash(identity).hex()
    payload = {
        "name": config.name,
        "destination": destination,
        "identity": identity.hexhash,
        "lxmf": lxmf_delivery_hash(identity).hex(),
        # Fingerprint of this identity alone — the pairwise safety number
        # for a peer (shown by `tau peers --safety-number`) folds the
        # peer's key in too, so the two numbers are different by design.
        "safety_number": safety_number(identity.get_public_key()),
        "data_dir": str(config.data_dir),
        "rns_config": str(config.rns_config_file),
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"name:          {config.name}")
        print(f"destination:   {destination}")
        print(f"identity:      {identity.hexhash}")
        print(f"lxmf:          {payload['lxmf']}")
        print(f"safety number: {payload['safety_number']}")
        print(f"data dir:      {config.data_dir}")
    return _EXIT_OK


def cmd_status(args: argparse.Namespace) -> int:
    """Summarise the local node's state without starting it.

    Read-only: no RNS, no network, and the data-directory lock is only
    probed, never taken, so this keeps working while a node is serving.
    A missing Tan database reports zero posts instead of creating one.
    """
    config = _make_config(args)
    identity = load_identity(config.identity_path, harden=False)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    mailbox = Mailbox(config.mailbox_path)
    counts = mailbox.counts()
    now = time.time()
    due = sum(
        1
        for entry in mailbox.outbox_list()
        if entry.expires_at > now and entry.is_due(now)
    )
    registry = PeerRegistry(config.peers_path)
    inbox = Inbox(config.inbox_path)
    # Fail closed like the running node: an unreadable allow-list file
    # reports as enabled-but-empty rather than silently open.
    allowlist = load_allowlist_or_deny_all(config.allowlist_path)
    tan_posts = 0
    tan_following = 0
    if config.social_db_path.exists():
        # Read-only: never migrate or write a database a node may be serving.
        try:
            conn = open_db_readonly(config.social_db_path)
        except sqlite3.Error:
            conn = None
        if conn is not None:
            try:
                tan_posts = int(
                    conn.execute(
                        "SELECT COUNT(*) AS n FROM posts WHERE kind = 'post'"
                    ).fetchone()["n"]
                )
                tan_following = len(following(conn))
            except sqlite3.Error:
                # Missing or incompatible tables report zero, not an error.
                tan_posts = 0
                tan_following = 0
            finally:
                conn.close()
    serving_pid = instance_lock_holder(config.instance_lock_path)
    payload = {
        "name": config.name,
        "destination": destination_hash(identity).hex(),
        "lxmf": lxmf_delivery_hash(identity).hex(),
        "data_dir": str(config.data_dir),
        "serving": serving_pid is not None,
        "serving_pid": serving_pid,
        "peers": len(registry),
        "outbox": {
            "queued": counts["outbox"],
            "due": due,
            "held": counts["held"],
        },
        "inbox": {"messages": len(inbox)},
        "tan": {"posts": tan_posts, "following": tan_following},
        "allowlist": {
            "enabled": allowlist.enabled,
            "entries": len(allowlist.allow),
        },
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"name:        {payload['name']}")
        print(f"destination: {payload['destination']}")
        print(f"lxmf:        {payload['lxmf']}")
        print(f"data dir:    {payload['data_dir']}")
        if serving_pid is None:
            print("serving:     no")
        elif serving_pid < 0:
            print("serving:     yes (pid unknown)")
        else:
            print(f"serving:     yes (pid {serving_pid})")
        print(f"peers:       {payload['peers']}")
        print(
            f"outbox:      {payload['outbox']['queued']} queued "
            f"({payload['outbox']['due']} due), "
            f"{payload['outbox']['held']} held"
        )
        print(f"inbox:       {payload['inbox']['messages']} messages")
        print(
            f"tan:         {payload['tan']['posts']} posts, "
            f"following {payload['tan']['following']} authors"
        )
        print(
            f"allowlist:   {'enabled' if payload['allowlist']['enabled'] else 'disabled'} "
            f"({payload['allowlist']['entries']} entries)"
        )
    return _EXIT_OK


def _same_path(left: Path, right: Path) -> bool:
    """Compare two paths after expanding ``~`` and resolving symlinks."""
    try:
        return Path(left).expanduser().resolve() == Path(right).expanduser().resolve()
    except OSError:
        return False


def cmd_id_export(args: argparse.Namespace) -> int:
    config = _make_config(args)
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    target = Path(args.to).expanduser()
    if _same_path(target, config.identity_path):
        raise NodeError(
            f"cannot export the identity to itself ({target}); "
            "choose a different backup path"
        )
    if target.is_dir():
        raise NodeError(f"backup path {target} is a directory")
    # lexists, not exists: a dangling symlink at the target counts as an
    # existing backup and must not be silently replaced without --force.
    if os.path.lexists(target) and not getattr(args, "force", False):
        raise NodeError(
            f"backup already exists at {target}; pass --force to overwrite it"
        )
    # The backup goes into a caller-selected folder: never change that
    # folder's permissions, only the backup file itself is private.
    save_identity(identity, target, restrict_parent=False)
    payload = {
        "destination": destination_hash(identity).hex(),
        "identity": identity.hexhash,
        "path": str(target),
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"identity:    {payload['identity']}")
        print(f"destination: {payload['destination']}")
        print(f"backup:      {payload['path']}")
    return _EXIT_OK


def cmd_id_import(args: argparse.Namespace) -> int:
    config = _make_config(args)
    source = Path(args.from_path).expanduser()
    if not source.exists():
        raise NodeError(f"no identity backup at {source}")
    if _same_path(source, config.identity_path):
        raise NodeError(f"{source} is already this node's identity file")
    try:
        identity = load_identity(source)
    except Exception as exc:
        # A backup file is untrusted input: any parse failure is a bad backup.
        raise NodeError(f"not a valid identity backup: {exc}") from exc
    if (
        identity is None
    ):  # pragma: no cover - load_identity returns None only when missing
        raise NodeError(f"no identity backup at {source}")
    config.prepare()
    # Hold the instance lock across the whole replacement: a node that
    # starts after the probe but before the write would load the old
    # identity and then serve with a different identity than the one on
    # disk.  The identity-creation lock serialises the check-and-write
    # against a concurrent `tau init` on the same directory.  Lock order
    # (instance, then identity) matches `TauNode.start`, which takes the
    # instance lock before loading or creating its identity.
    # Note the instance lock is a no-op on a platform with neither
    # fcntl nor msvcrt, the same limitation `tau serve` itself accepts.
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    try:
        with _identity_lock(config.identity_path):
            if config.identity_path.exists() and not getattr(args, "force", False):
                raise NodeError(
                    f"an identity already exists at {config.identity_path}; "
                    "pass --force to replace it"
                )
            # Pairwise safety numbers are computed with the local key, so
            # verifications made under the old identity mean nothing
            # under the new one.  An unreadable old identity is treated
            # the same way: its comparisons cannot be trusted either.
            try:
                old_identity = load_identity(config.identity_path)
                old_key = (
                    old_identity.get_public_key()
                    if old_identity is not None
                    else None
                )
            except Exception as exc:
                # The identity is about to be replaced either way, so the
                # comparisons are cleared — but say why, so a transient
                # read failure (e.g. permissions) is not silent.
                log.warning(
                    "Could not read the existing identity (%s); clearing "
                    "peer verifications",
                    exc,
                )
                old_key = None
            verifications_cleared = 0
            if old_key != identity.get_public_key():
                # Persist the invalidation BEFORE replacing the identity:
                # if the registry write fails, abort with the old
                # identity still in place rather than leaving the new
                # identity alongside stale verified markers.
                registry = PeerRegistry(config.peers_path)
                verifications_cleared = registry.clear_verifications()
                if verifications_cleared:
                    try:
                        saved = registry.save()
                    except OSError as exc:
                        raise NodeError(
                            "could not save peer registry "
                            f"({exc}); identity not replaced"
                        ) from exc
                    if not saved:
                        raise NodeError(
                            "could not save peer registry; "
                            "identity not replaced"
                        )
            save_identity(identity, config.identity_path)
    finally:
        lock.release()
    payload = {
        "destination": destination_hash(identity).hex(),
        "identity": identity.hexhash,
        "data_dir": str(config.data_dir),
        "verifications_cleared": verifications_cleared,
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"identity:    {payload['identity']}")
        print(f"destination: {payload['destination']}")
        print(f"data dir:    {payload['data_dir']}")
        if verifications_cleared:
            print(
                f"verifications cleared: {verifications_cleared} "
                "(local identity changed; re-verify peers)"
            )
    return _EXIT_OK


def _resolve_send_reply_to(
    node: TauNode, target: str, ref: str, discover_timeout: Optional[float]
) -> Dict[str, Any]:
    """Resolve ``--reply-to`` to an ``in_reply_to`` wire reference on *node*.

    Derive the conversation hash without network discovery, so a reply
    to a queued message still reaches the outbox when the recipient is
    offline; discovery is only a fallback.
    """
    target_hash = node._target_hash(target)
    if target_hash is None:
        peer = node.resolve_peer(
            target,
            timeout=float(
                discover_timeout if discover_timeout is not None else 10.0
            ),
        )
        target_hash = peer.destination_hash
    return _resolve_reply_target(
        Inbox(node.config.inbox_path),
        node.mailbox,
        ref,
        target_hash.hex(),
    )


def _finalize_send_result(
    response: Dict[str, Any],
    *,
    target: str,
    expires_at: Optional[float],
    queued_before: set,
) -> Dict[str, Any]:
    """Massage a raw send response into the result ``tau send`` reports.

    The peer response is untrusted JSON: keep the local recipient label
    authoritative, so a peer cannot relabel this result. A "queued"
    claim only counts with a new id (see below); a direct delivery keeps
    no "queued" key at all, preserving the established single-recipient
    JSON shape.
    """
    result = dict(response)
    result["target"] = target
    if expires_at is not None:
        result["expires_at"] = expires_at
    if response.get("queued"):
        result["queued"] = (
            isinstance(response.get("id"), str)
            and response["id"] not in queued_before
        )
        if result["queued"] and "to" not in response:
            result["to"] = target
    return result


def _reconcile_queued_sends(
    node: TauNode, results: List[Dict[str, Any]], expires_at: Optional[float]
) -> None:
    """Flip queued results to delivered when a flush moved them.

    After a queued send, give reachable peers a brief chance to take a
    sealed copy (or the message itself), then report as delivered any
    message that left the outbox during the flush.
    """
    if not any(result.get("queued") for result in results):
        return
    node.flush_outbox(timeout=3.0, max_peers=5)
    for result in results:
        if not result.get("queued"):
            continue
        message_id = result["id"]
        # Leaving the outbox is not proof of delivery: an entry can also
        # expire or be cancelled. Only a recorded delivery counts.
        sent = node.mailbox.sent_record(message_id)
        if node.mailbox.get_outbox(message_id) is None and (
            sent is not None and sent.get("delivered_at") is not None
        ):
            to_hex = result["to"]
            target = result["target"]
            # Keep the private "_" fields (e.g. the resolved reply id).
            private = {k: v for k, v in result.items() if k.startswith("_")}
            result.clear()
            result.update(
                {
                    "target": target,
                    "ok": True,
                    "delivered": True,
                    "id": message_id,
                    "to": to_hex,
                    **(
                        {"expires_at": expires_at}
                        if expires_at is not None
                        else {}
                    ),
                    **private,
                }
            )


def _wait_for_queued_sends(
    node: TauNode, results: List[Dict[str, Any]], wait: float
) -> None:
    """Wait for each queued result in turn, each with the full budget.

    The ``--wait`` handling from :func:`cmd_send`, shared by the local
    send path and the send server so both routes report the same
    result: ``--wait`` replaces the one-shot flush, a direct delivery
    records ``waited: 0.0``, and a queued message still queued at the
    deadline — or one that left the queue without a delivery
    confirmation from this node — is recorded as not delivered.
    """
    for result in results:
        if not result.get("queued"):
            if result.get("ok", True):
                result["delivered"] = True
                result["waited"] = 0.0
            continue
        started = time.monotonic()
        delivered = node.wait_for_delivery(result["id"], timeout=wait)
        waited = time.monotonic() - started
        if delivered:
            result.pop("queued", None)
            result["delivered"] = True
            result["waited"] = round(waited, 1)
        elif node.mailbox.get_outbox(result["id"]) is None:
            # The entry left the queue without a delivery confirmation
            # from this node: cancelled by another process, or delivered
            # by one.  Either way it must not be reported as delivered.
            result["queued"] = False
            result["delivered"] = False
            result["waited"] = round(waited, 1)
        else:
            result["delivered"] = False
            result["waited"] = round(waited, 1)


def _send_local_message(
    node: TauNode,
    *,
    target: str,
    text: str,
    timeout: float = 20.0,
    discover_timeout: Optional[float] = None,
    queue: bool = True,
    expires_at: Optional[float] = None,
    reply_to: Optional[str] = None,
    kind: Optional[str] = None,
    wait: float = 0.0,
) -> Dict[str, Any]:
    """Send one DM on the running node, for the local send server.

    Mirrors the one-shot path in :func:`cmd_send` (same queued-before
    snapshot, same queued-to-delivered flush reconciliation) so a send
    routed through the serving node reports the same result. The
    resolved reply id travels back under ``_reply_id`` for the caller's
    human-readable output; callers strip it before printing.  With
    ``--wait`` the wait replaces the flush reconciliation (see
    :func:`_wait_for_queued_sends`).
    """
    queued_before = {entry.id for entry in node.mailbox.outbox_list()}
    in_reply_to = None
    if reply_to is not None:
        in_reply_to = _resolve_send_reply_to(
            node, target, reply_to, discover_timeout
        )
    response = node.send_message(
        target,
        text,
        timeout=float(timeout),
        discover_timeout=discover_timeout,
        queue=bool(queue),
        in_reply_to=in_reply_to,
        expires_at=expires_at,
        kind=kind,
    )
    result = _finalize_send_result(
        response, target=target, expires_at=expires_at, queued_before=queued_before
    )
    if in_reply_to is not None:
        result["_reply_id"] = in_reply_to["id"]
    if wait > 0:
        _wait_for_queued_sends(node, [result], wait)
    else:
        _reconcile_queued_sends(node, [result], expires_at)
    return result


def _retract_local_message(
    node: TauNode,
    *,
    target: str,
    message_id: str,
    timeout: float = 20.0,
    discover_timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """Retract one DM on the running node, for the local send server."""
    return node.send_retraction(
        target,
        message_id,
        timeout=float(timeout),
        discover_timeout=discover_timeout,
    )


def _edit_local_message(
    node: TauNode,
    *,
    target: str,
    message_id: str,
    text: str,
    timeout: float = 20.0,
    discover_timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """Edit one DM on the running node, for the local send server."""
    return node.send_edit(
        target,
        message_id,
        text,
        timeout=float(timeout),
        discover_timeout=discover_timeout,
    )


def _dispatch_local_send(node: TauNode, **request: Any) -> Dict[str, Any]:
    """Serve one local send-socket request: a DM send, a retraction, or an edit.

    The socket carries all three, distinguished by ``op``: ``"send"`` (the
    default, so older one-shot clients keep working) runs
    :func:`_send_local_message`, while ``"retract"`` and ``"edit"`` run the
    retraction and the edit on the serving node, so ``tau retract`` and
    ``tau edit`` work while ``tau serve`` holds the data directory.
    """
    op = request.pop("op", "send")
    if op == "retract":
        return _retract_local_message(node, **request)
    if op == "edit":
        return _edit_local_message(node, **request)
    if op == "send":
        return _send_local_message(node, **request)
    raise NodeError(f"unknown local send operation: {op!r}")


def _send_via_local_node(
    args: argparse.Namespace,
    config: TauConfig,
    text: str,
    targets: List[str],
    expires_at: Optional[float],
    reply_ids: Dict[str, str],
    kind: Optional[str] = None,
    wait: float = 0.0,
) -> List[Dict[str, Any]]:
    """One-shot send on a node this process starts and stops itself."""
    node = TauNode(config, name=config.name)
    node.start()
    results: List[Dict[str, Any]] = []
    try:
        for target in targets:
            # Outbox ids present before this call: only this node's own
            # queue path creates an entry for the message it just queued,
            # so a "queued" reply is only genuine when its id is new
            # since this snapshot. A peer claiming queued with a
            # pre-existing id (e.g. one it holds a sealed copy of) is
            # treated as delivered — the link request succeeded.
            queued_before = {entry.id for entry in node.mailbox.outbox_list()}
            try:
                reply_to = None
                if getattr(args, "reply_to", None):
                    reply_to = _resolve_send_reply_to(
                        node,
                        target,
                        args.reply_to,
                        float(getattr(args, "discover_timeout", 10.0)),
                    )
                    reply_ids[target] = reply_to["id"]
                response = node.send_message(
                    target,
                    text,
                    timeout=float(getattr(args, "timeout", 20.0)),
                    discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
                    queue=not getattr(args, "no_queue", False),
                    in_reply_to=reply_to,
                    expires_at=expires_at,
                    kind=kind,
                )
            except (NodeError, PeerError, MailboxError) as exc:
                if len(targets) == 1:
                    # Single recipient keeps the historical behaviour:
                    # the error reaches main() and exits non-zero.
                    raise
                # One recipient failing must not stop the rest; the
                # failure is reported per recipient below. This also
                # covers a full outbox: enqueue raises MailboxFull
                # inside send_message, and that must not abort the
                # remaining recipients.
                results.append({"target": target, "ok": False, "error": str(exc)})
                continue
            results.append(
                _finalize_send_result(
                    response,
                    target=target,
                    expires_at=expires_at,
                    queued_before=queued_before,
                )
            )
        if wait > 0:
            _wait_for_queued_sends(node, results, wait)
        else:
            _reconcile_queued_sends(node, results, expires_at)
    finally:
        node.stop()
    return results


def _send_via_running_node(
    args: argparse.Namespace,
    config: TauConfig,
    text: str,
    targets: List[str],
    expires_at: Optional[float],
    reply_ids: Dict[str, str],
    kind: Optional[str] = None,
    wait: float = 0.0,
) -> List[Dict[str, Any]]:
    """Send through the serving node on its local socket."""
    from .local_ipc import LocalIPCUnavailable
    from .local_send import send_running

    results: List[Dict[str, Any]] = []
    for target in targets:
        try:
            result = send_running(
                config.data_dir,
                target=target,
                text=text,
                timeout=float(getattr(args, "timeout", 20.0)),
                discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
                queue=not getattr(args, "no_queue", False),
                expires_at=expires_at,
                reply_to=getattr(args, "reply_to", None),
                kind=kind,
                wait=wait,
            )
        except LocalIPCUnavailable as exc:
            raise NodeError(
                "the running node stopped answering mid-send; retry the send"
            ) from exc
        except (NodeError, PeerError, MailboxError) as exc:
            if len(targets) == 1:
                raise
            results.append({"target": target, "ok": False, "error": str(exc)})
            continue
        reply_id = result.pop("_reply_id", None)
        if reply_id is not None:
            reply_ids[target] = reply_id
        results.append(result)
    return results


def _retract_via_running_node(
    config: TauConfig,
    recipient: str,
    message_id: str,
    *,
    timeout: float,
    discover_timeout: Optional[float],
) -> Dict[str, Any]:
    """Retract through the serving node on its local socket."""
    from .local_ipc import LocalIPCUnavailable
    from .local_send import send_running

    try:
        return send_running(
            config.data_dir,
            op="retract",
            target=recipient,
            message_id=message_id,
            timeout=timeout,
            discover_timeout=discover_timeout,
        )
    except LocalIPCUnavailable as exc:
        raise NodeError(
            "the running node stopped answering mid-retract; retry the retract"
        ) from exc


def _edit_via_running_node(
    config: TauConfig,
    recipient: str,
    message_id: str,
    text: str,
    *,
    timeout: float,
    discover_timeout: Optional[float],
) -> Dict[str, Any]:
    """Edit through the serving node on its local socket."""
    from .local_ipc import LocalIPCUnavailable
    from .local_send import send_running

    try:
        return send_running(
            config.data_dir,
            op="edit",
            target=recipient,
            message_id=message_id,
            text=text,
            timeout=timeout,
            discover_timeout=discover_timeout,
        )
    except LocalIPCUnavailable as exc:
        raise NodeError(
            "the running node stopped answering mid-edit; retry the edit"
        ) from exc


#: How long a one-shot send waits for a serving node's socket to appear
#: (or a transient lock holder to finish) before failing closed.
_SEND_ROUTE_WAIT_SECONDS = 10.0


def _instance_lock_free(config: TauConfig) -> bool:
    """True when no live process holds the data-directory lock.

    The probe is the ground truth: taking the lock (and releasing it)
    is the only way to know no holder owns the directory.  A PID read
    from the lock file is never trusted on its own — a holder records
    its PID just after taking the lock, so the file can still name a
    dead predecessor while a new holder is starting up, and a dead
    PID then reads as "free" while a serve is mid-startup.  The probe
    holds the lock only for microseconds; a `tau serve` starting in
    that exact window fails fast with "already serving" and can be
    retried, which beats a false "free" starting a second node on the
    same identity.
    """
    probe = InstanceLock(config.instance_lock_path, config.data_dir)
    try:
        probe.acquire()
    except InstanceAlreadyRunning:
        return False
    probe.release()
    return True


def _send_routes_via_running_node(config: TauConfig) -> bool:
    """True when a serving node should carry the send; False for a local node.

    A serving node holds the data-directory lock for its whole life but
    answers on its send socket only between startup and shutdown. A
    socket that answers wins. Otherwise the lock decides: when it is free
    no node owns the directory (a stale socket is harmless), so a local
    one-shot node may start. When it is held, the serve may still be
    starting, so poll briefly for the socket; the holder may also be
    transient — a standalone `tau tan post` holds the lock without
    running a send server — so the poll also watches for the lock to
    free, and a local send proceeds the moment it does. Only a lock
    still held after the wait fails closed instead of starting a second
    node on the same identity.
    """
    from .local_send import send_server_available

    has_socket = hasattr(socket, "AF_UNIX")
    if has_socket and send_server_available(config.data_dir):
        return True
    config.prepare()
    if _instance_lock_free(config):
        return False
    deadline = time.monotonic() + _SEND_ROUTE_WAIT_SECONDS
    while True:
        if has_socket and send_server_available(config.data_dir):
            return True
        if _instance_lock_free(config):
            return False
        if time.monotonic() >= deadline:
            break
        time.sleep(0.1)
    raise NodeError(
        "a tau node holds this data directory but is not accepting "
        "sends; retry shortly"
    ) from None


def cmd_backup(args: argparse.Namespace) -> int:
    config = _make_config(args)
    target, manifest = create_backup(
        config, args.file, force=getattr(args, "force", False)
    )
    identity = load_identity(config.identity_path)
    payload = {
        "backup": str(target),
        "node": manifest.get("node"),
        "destination": destination_hash(identity).hex() if identity else None,
        "files": manifest.get("files"),
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"node:        {payload['node']}")
        print(f"destination: {payload['destination']}")
        print(f"backup:      {payload['backup']}")
        print(f"files:       {len(payload['files'] or [])}")
    return _EXIT_OK


def cmd_restore(args: argparse.Namespace) -> int:
    config = _make_config(args)
    manifest, files = restore_backup(
        config, args.file, force=getattr(args, "force", False)
    )
    identity = load_identity(config.identity_path)
    payload = {
        "data_dir": str(config.data_dir),
        "node": manifest.get("node"),
        "destination": destination_hash(identity).hex() if identity else None,
        "files": files,
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"node:        {payload['node']}")
        print(f"destination: {payload['destination']}")
        print(f"data dir:    {payload['data_dir']}")
        print(f"files:       {len(payload['files'])}")
    return _EXIT_OK


def cmd_id_rename(args: argparse.Namespace) -> int:
    config = _make_config(args)
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    raw_name = getattr(args, "new_name", None)
    if not isinstance(raw_name, str):
        raise NodeError("new name must not be empty")
    # Empty fallback: a name that sanitises to nothing (blank, or control
    # characters only) is rejected instead of silently becoming "anonymous".
    new_name = sanitize_name(raw_name, fallback="")
    if not new_name:
        raise NodeError("new name must not be empty")
    # Read the stored name, not the resolved one: a global --name flag
    # would override the resolved name and misreport the rename.
    old_name = load_node_name(config.data_dir) or config.name
    payload = {
        "name": new_name,
        "previous_name": old_name,
        "changed": new_name != old_name,
        "destination": destination_hash(identity).hex(),
    }
    if new_name == old_name:
        if _json_mode(args):
            _emit(payload, True)
        else:
            print(f'Node is already named "{old_name}".')
        return _EXIT_OK
    # A running node announces with its in-memory name, so a rename under
    # it would sit ignored on disk until restart. Probe the instance lock
    # (same pattern as `tau serve`) and refuse while the node is serving.
    # Hold the lock through the write so a serve starting now cannot read
    # the old name and then start announcing it after the write lands.
    probe = InstanceLock(config.instance_lock_path, config.data_dir)
    probe.acquire()
    try:
        write_node_metadata(config.data_dir, new_name)
    finally:
        probe.release()
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f'Renamed node "{old_name}" to "{new_name}".')
    return _EXIT_OK


def cmd_send(args: argparse.Namespace) -> int:
    # Validate flags before reading the message, so an invalid command
    # fails at once instead of blocking on stdin.
    wait_given = getattr(args, "wait", None) is not None
    wait = float(getattr(args, "wait", None) or 0.0)
    if not math.isfinite(wait):
        print("error: --wait must be a finite number of seconds", file=sys.stderr)
        return 2
    if wait < 0:
        print("error: --wait must not be negative", file=sys.stderr)
        return 2
    if wait_given and getattr(args, "no_queue", False):
        print("error: --wait cannot be combined with --no-queue", file=sys.stderr)
        return 2
    text = _read_message(args)
    config = _make_config(args)
    try:
        kind = normalize_message_kind(getattr(args, "kind", None))
    except ProtocolError as exc:
        raise NodeError(str(exc)) from exc
    expires_at = None
    expire_text = getattr(args, "expire", None)
    if expire_text is not None:
        expires_at = time.time() + _parse_duration(expire_text)

    targets = args.to if isinstance(args.to, list) else [args.to]
    reply_ids: Dict[str, str] = {}
    # A serving node owns the data directory: send through it instead of
    # starting a second Reticulum instance on the same identity.
    if _send_routes_via_running_node(config):
        results = _send_via_running_node(
            args, config, text, targets, expires_at, reply_ids, kind=kind, wait=wait
        )
    else:
        results = _send_via_local_node(
            args, config, text, targets, expires_at, reply_ids, kind=kind, wait=wait
        )

    failed = [result for result in results if not result.get("ok", True)]
    # --wait outcomes are recorded on the results by the send path
    # (see _wait_for_queued_sends): a waited result that is not
    # delivered failed the wait, whether it is still queued or left the
    # queue without a delivery confirmation.
    wait_failed = any(
        result.get("waited") is not None and result.get("delivered") is False
        for result in results
    )
    if _json_mode(args):
        if len(results) == 1 and not failed:
            # Single recipient keeps the historical bare-response shape.
            _emit({k: v for k, v in results[0].items() if k != "target"}, True)
        else:
            _emit(
                {
                    "results": results,
                    "delivered": sum(
                        1
                        for r in results
                        if r.get("ok", True)
                        and not r.get("queued")
                        and r.get("delivered") is not False
                    ),
                    "queued": sum(1 for r in results if r.get("queued")),
                    "failed": len(failed),
                },
                True,
            )
    else:
        for result in results:
            target = result["target"]
            if not result.get("ok", True):
                print(f"failed to send to {target}: {result['error']}", file=sys.stderr)
            elif (
                result.get("waited") is not None
                and not result.get("delivered")
                and not result.get("queued")
            ):
                # The entry left the queue while waiting without a delivery
                # confirmation from this node: cancelled, or delivered by
                # another process.  Say so instead of claiming delivery.
                print(f"no longer queued for {target} after {result['waited']:.1f}s")
                print("  delivery was not confirmed — it may have been cancelled,")
                print("  or delivered by another process sharing this data directory")
            elif result.get("queued"):
                waited = result.get("waited")
                if waited is not None:
                    print(f"still queued for {target} after {waited:.1f}s")
                else:
                    print(f"queued for {target} — they are unreachable right now")
                print(f"  id:      {result['id']}")
                if reply_ids.get(target):
                    print(f"  in reply to {short_hash(reply_ids[target])}")
                if expires_at is not None:
                    print(f"  disappears in {_ago(expires_at - time.time())}")
            elif result.get("delivered"):
                waited = result.get("waited")
                if (
                    waited is not None
                    and "_rtt" not in result
                    and "echo" not in result
                ):
                    print(f"delivered to {target} after {waited:.1f}s")
                else:
                    print(f"delivered to {target}")
                if reply_ids.get(target):
                    print(f"  in reply to {short_hash(reply_ids[target])}")
                if expires_at is not None:
                    print(f"  disappears in {_ago(expires_at - time.time())}")
            else:
                print(f"delivered to {target}")
                if reply_ids.get(target):
                    print(f"  in reply to {short_hash(reply_ids[target])}")
                echo = result.get("echo")
                if echo is not None:
                    print(f"echo: {echo}")
                rtt = result.get("_rtt")
                if isinstance(rtt, (int, float)):
                    print(f"round trip: {rtt * 1000:.0f} ms")
                if result.get("expired"):
                    print("  the receiver dropped it — past its expiry")
                if expires_at is not None:
                    print(f"  disappears in {_ago(expires_at - time.time())}")
        if any(result.get("queued") for result in results):
            print("  it will be delivered automatically when they are heard from;")
            print("  check `tau outbox` for the queue, or just leave a node serving")
    return _EXIT_ERROR if (failed or wait_failed) else _EXIT_OK




def cmd_send_file(args: argparse.Namespace) -> int:
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    last_percent = -1

    def on_progress(progress: float) -> None:
        nonlocal last_percent
        percent = int(progress * 100)
        if _json_mode(args) or percent == last_percent:
            return
        last_percent = percent
        print(f"\r  sending… {percent}%", end="", flush=True)

    try:
        response = node.send_file(
            args.to,
            args.path,
            timeout=float(getattr(args, "timeout", 300.0)),
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
            progress_callback=on_progress,
        )
    finally:
        node.stop()

    if _json_mode(args):
        _emit(response, True)
        return _EXIT_OK
    if last_percent >= 0:
        print()
    print(
        f"sent {response['name']} ({response['size']} bytes) to {args.to}"
    )
    print(f"  id:     {response['id']}")
    print(f"  sha256: {response['sha256']}")
    return _EXIT_OK




def cmd_outbox(args: argparse.Namespace) -> int:
    config = _make_config(args)
    mailbox = Mailbox(config.mailbox_path)
    registry = PeerRegistry(config.peers_path)
    entries = mailbox.outbox_list()
    counts = mailbox.counts()

    def describe(entry: Any) -> Dict[str, Any]:
        peer = registry.get(entry.to)
        payload = entry.to_dict()
        payload["to_name"] = peer.display_name if peer is not None else entry.to_name
        payload["held_by"] = sorted(entry.held_by)
        # Still queued, so never read — but the field keeps the shape
        # identical to the sent history, where read_at is meaningful.
        payload["read_at"] = mailbox.read_at(entry.id)
        return payload

    if _json_mode(args):
        _emit(
            {
                "outbox": [describe(entry) for entry in entries],
                "count": len(entries),
                "held_for_others": counts["held"],
                "receipts_queued": counts["receipts"],
            },
            True,
        )
    else:
        if not entries:
            print("Outbox is empty — nothing waiting on offline peers.")
        else:
            now = time.time()
            header = ("ID", "TO", "NAME", "KIND", "AGE", "TRIES", "NEXT TRY", "EXPIRES IN")
            rows: List[tuple] = [header]
            for entry in entries:
                peer = registry.get(entry.to)
                name = peer.display_name if peer is not None else entry.to_name
                next_try = entry.next_attempt_at
                if next_try is None or next_try <= now:
                    next_try_text = "now"
                else:
                    next_try_text = f"in {_ago(next_try - now)}"
                rows.append(
                    (
                        entry.id[:8],
                        entry.to_hex,
                        name or "-",
                        getattr(entry, "kind", None) or DEFAULT_MESSAGE_KIND,
                        _ago(now - entry.created_at),
                        str(entry.attempts),
                        next_try_text,
                        _ago(entry.expires_at - now),
                    )
                )
            widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
            for row in rows:
                print("  ".join(value.ljust(widths[i]) for i, value in enumerate(row)))
            print(f"\n{len(entries)} message(s) waiting for offline peers.")
        if counts["held"]:
            print(
                f"Also holding {counts['held']} sealed message(s) for other peers"
                " — see `tau outbox held`."
            )
        if counts["receipts"]:
            print(
                f"Also waiting to send {counts['receipts']} read receipt(s)"
                " to offline senders."
            )
    return _EXIT_OK


def _is_hex32(value: str) -> bool:
    return len(value) == 32 and all(char in "0123456789abcdefABCDEF" for char in value)


def _format_message_time(timestamp: float) -> str:
    """One inbox entry's received time; never fails the whole listing."""
    try:
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")
    except (OverflowError, OSError, ValueError):
        return "unknown time"


def _resolve_inbox_peer(inbox: Inbox, registry: PeerRegistry, target: str) -> str:
    """Resolve ``--peer`` to a sender hash for inbox filtering.

    A full destination hash filters directly, and an exact name match
    wins over any prefix.  For a hash prefix, registered peers and
    senders stored in the inbox are combined before selecting: a
    prefix shared by two distinct senders is ambiguous, even when one
    of them is registered and the other is not (pruned, or never
    discovered — a direct message needs no prior announce).
    """
    target = target.strip()
    if _is_hex32(target):
        return target.lower()
    peers = registry.all()
    by_name = [peer for peer in peers if peer.name.casefold() == target.casefold()]
    if len(by_name) == 1:
        return by_name[0].hexhash
    if len(by_name) > 1:
        raise NodeError(
            f"{len(by_name)} peers are named {target!r}; use a destination hash"
        )
    if len(target) >= 4:
        prefix = target.casefold()
        senders = {peer.hexhash for peer in peers if peer.hexhash.startswith(prefix)}
        senders |= {
            str(entry.get("destination") or "").casefold()
            for entry in inbox.list_messages(limit=None)
            if str(entry.get("destination") or "").casefold().startswith(prefix)
        }
        if len(senders) == 1:
            return senders.pop()
        if len(senders) > 1:
            raise NodeError(f"{len(senders)} senders match the hash prefix {target!r}")
    raise NodeError(f"unknown peer: {target!r}")


def _send_displayed_receipts(
    config: TauConfig, inbox: Inbox, entries: List[Dict[str, Any]]
) -> Dict[str, int]:
    """Receipt the displayed messages, directly or via the serving node.

    Listing a DM is reading it, so displayed messages are stamped
    ``read_at`` in the inbox first — that stamp is the durable
    handoff: when a serving node already holds this data directory's
    TCP listen port, a second Reticulum instance cannot start here
    (Reticulum exits the process on the bind failure), so the serving
    node's mail sweep delivers those receipts instead.  When no node
    is serving, this command sends them directly over a one-shot node.
    Only messages never receipted before are covered, so listing the
    same inbox twice does not re-send.  Best-effort: a receipt failure
    never breaks the listing.  Returns how many peers got a live
    receipt and how many had one queued for store-and-forward.
    """
    unreceipted = [
        entry
        for entry in entries
        if isinstance(entry.get("id"), str)
        and entry.get("id")
        and entry.get("receipt_sent_at") is None
    ]
    if not unreceipted:
        return {"sent": 0, "queued": 0}
    if not send_read_receipts_enabled(config.data_dir):
        return {"sent": 0, "queued": 0}
    try:
        inbox.mark_read([str(entry["id"]) for entry in unreceipted])
    except Exception:  # pragma: no cover - defensive
        pass
    if not config.identity_path.exists():
        # No identity yet: starting a node here would create one as a
        # side effect of merely listing messages.  The read stamp above
        # is the handoff for when a node next serves this directory.
        return {"sent": 0, "queued": 0}
    try:
        check_tcp_listen_ports(config)
    except NodeError:
        # A serving node already holds the listen port; its mail sweep
        # delivers the receipts from the read stamps above.
        return {"sent": 0, "queued": 0}
    by_peer: Dict[str, List[str]] = {}
    for entry in unreceipted:
        peer_hash = str(entry.get("destination") or "")
        if peer_hash:
            by_peer.setdefault(peer_hash, []).append(str(entry["id"]))
    if not by_peer:
        return {"sent": 0, "queued": 0}
    # No mail loop or announce: this node only sends the explicit receipts
    # below, so a background sweep cannot send the same ids concurrently.
    node = TauNode(config, name=config.name)
    try:
        node.start(mail_loop=False, announce=False)
    except Exception:
        return {"sent": 0, "queued": 0}
    sent = queued = 0
    stamped: List[str] = []
    try:
        for peer_hash, ids in by_peer.items():
            try:
                result = node.send_read_receipts(peer_hash, ids)
            except Exception:
                continue
            handled = result.get("ids") or []
            if not handled:
                continue
            if result.get("queued"):
                queued += 1
            elif result.get("ok"):
                sent += 1
            stamped.extend(handled)
    finally:
        node.stop()
    if stamped:
        try:
            inbox.mark_receipt_sent(stamped)
        except Exception:  # pragma: no cover - defensive
            pass
    return {"sent": sent, "queued": queued}


def _resolve_sent_peer(registry: PeerRegistry, target: str) -> str:
    """Resolve ``--peer`` to a recipient hash for the sent view.

    A full destination hash filters directly even when the peer has
    been pruned from the registry; names and prefixes resolve through
    it.
    """
    target = target.strip()
    if _is_hex32(target):
        return target.lower()
    try:
        peer = registry.resolve(target)
    except PeerError as exc:
        raise NodeError(str(exc)) from exc
    if peer is None:
        raise NodeError(f"unknown peer: {target!r}")
    return peer.hexhash
def _resolve_reply_target(
    inbox: Inbox, mailbox: Mailbox, ref: str, peer_hash: str
) -> Dict[str, Any]:
    """Resolve ``--reply-to`` to an ``in_reply_to`` wire reference.

    The quoted message must be in the same conversation: the inbox is
    searched first (received messages and the node's own sent ones),
    then the outbox for a still-queued message.  A reference to
    another peer's message, or to a message this node has never seen,
    is refused instead of sent dangling — the receiver could only
    render it as "original not available" anyway.
    """
    ref = ref.strip()
    if not ref:
        raise NodeError("--reply-to must not be empty")
    # Gather candidates from both stores before choosing, so a prefix that
    # matches distinct messages across the inbox and the outbox is rejected
    # rather than silently quoting whichever store is searched first.
    candidates: Dict[str, Dict[str, Any]] = {}
    for item in inbox.find_prefix(ref, peer=peer_hash):
        candidates.setdefault(str(item["id"]), item)
    value = ref.lower()
    for entry in mailbox.outbox_list(to=bytes.fromhex(peer_hash)):
        if entry.id.lower().startswith(value):
            candidates.setdefault(
                entry.id, {"id": entry.id, "text": entry.text}
            )
    if len(candidates) > 1:
        raise NodeError(
            f"{len(candidates)} messages in this conversation match {ref!r};"
            " use a longer prefix"
        )
    if not candidates:
        raise NodeError(
            f"no message {ref!r} in this conversation to reply to;"
            " replies quote a message from this peer's history"
        )
    quoted = next(iter(candidates.values()))
    return {"id": str(quoted["id"]), "excerpt": excerpt_hash(str(quoted["text"]))}


def _quote_line(inbox: Inbox, registry: PeerRegistry, entry: Dict[str, Any]) -> Optional[str]:
    """Render one message's quote, or ``None`` when it has none.

    The quoted text is pulled from the local inbox, in the same
    conversation only; when the original is not there — or its text
    no longer matches the excerpt hash the sender quoted — the line
    says so instead of trusting anything on the wire.
    """
    reply_to = entry.get("in_reply_to")
    if not isinstance(reply_to, dict):
        return None
    ref_id = reply_to.get("id")
    if not isinstance(ref_id, str) or not ref_id:
        return None
    quoted = inbox.find(ref_id, peer=str(entry.get("destination") or ""))
    excerpt = reply_to.get("excerpt")
    if quoted is None or (
        isinstance(excerpt, str)
        and excerpt
        and excerpt_hash(str(quoted.get("text") or "")) != excerpt.lower()
    ):
        return "↩ original not available"
    if quoted.get("direction") == "out":
        name = "you"
    else:
        peer = registry.get(str(quoted.get("destination") or ""))
        name = peer.display_name if peer is not None else quoted.get("from") or "unknown"
    quoted_text = str(quoted.get("text") or "")
    first_line = quoted_text.splitlines()[0][:80] if quoted_text else ""
    return f"↩ {name}: {first_line}"


def cmd_messages(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    peer_hash = None
    if getattr(args, "peer", None):
        peer_hash = _resolve_inbox_peer(inbox, registry, args.peer)
    group_id = None
    if getattr(args, "group", None):
        group_id = GroupStore(config.groups_path).find(args.group)["id"]
    if getattr(args, "count", False):
        # A count is for scripting: it never advances the read cursor.
        counts = inbox.unread_counts()
        per_peer = counts["per_peer"]
        if peer_hash is not None:
            per_peer = {
                destination: count
                for destination, count in per_peer.items()
                if destination == peer_hash
            }
        total = sum(per_peer.values())
        if _json_mode(args):
            _emit({"unread": per_peer, "total": total}, True)
            return _EXIT_OK
        if not per_peer:
            print("No unread messages.")
            return _EXIT_OK
        for destination, count in sorted(per_peer.items()):
            peer = registry.get(destination)
            name = peer.display_name if peer is not None else "unknown"
            print(f"{name} ({short_hash(destination)}): {count} unread")
        print(f"Total: {total} unread")
        return _EXIT_OK
    limit = getattr(args, "limit", 30)
    if limit is not None and limit < 0:
        raise NodeError("--limit must not be negative")
    kind = getattr(args, "kind", None)
    if kind is not None:
        try:
            kind = normalize_message_kind(kind)
        except ProtocolError as exc:
            raise NodeError(str(exc)) from exc
    query = getattr(args, "search", None)
    if query is not None and not query.strip():
        raise NodeError("--search must not be empty")
    if getattr(args, "sent", False):
        return _cmd_messages_sent(args, config, registry, limit)
    # Blocked senders' history stays stored but is hidden locally;
    # unblocking restores the view.  Filter before applying the limit so
    # hidden messages do not use up the requested count.  A block-list
    # that cannot be read hides everything, matching the node, which
    # blocks all senders on the same condition.
    entries = inbox.list_messages(
        limit=None, peer=peer_hash, group=group_id, query=query, kind=kind
    )
    blocked = load_blocked_set(config.blocklist_path)
    if blocked is None:
        entries = []
    elif blocked:
        entries = [
            entry for entry in entries if entry.get("destination") not in blocked
        ]
    # Filter before limiting, so --unread finds older unread mail even
    # when newer messages from the same listing are already read.
    unread_only = getattr(args, "unread", False)
    if unread_only:
        entries = [entry for entry in entries if entry.get("unread")]
    shown = entries if limit is None else entries[:limit]
    omitted = entries[len(shown):]
    # Unread mail the window omitted: reported, never silently cleared.
    more_unread: Dict[str, int] = {}
    for entry in omitted:
        if entry.get("unread"):
            destination = str(entry.get("destination") or "")
            more_unread[destination] = more_unread.get(destination, 0) + 1
    if _json_mode(args):
        _emit(
            {"messages": shown, "count": len(shown), "more_unread": more_unread},
            True,
        )
    elif not shown:
        if query is not None:
            print(f"No messages matching {query!r}.")
        elif more_unread:
            _print_more_unread(registry, more_unread)
        else:
            print(
                "No unread messages." if unread_only else "No messages received yet."
            )
    else:
        for entry in shown:
            peer = registry.get(entry.get("destination") or "")
            name = peer.display_name if peer is not None else entry.get("from") or "unknown"
            sender = short_hash(str(entry.get("destination") or ""))
            when = _format_message_time(float(entry.get("received_at", 0)))
            marker = "● " if entry.get("unread") else ""
            # The short id is what `tau react` takes: without it the
            # listing gives the user no way to name a message to react to.
            message_id = entry.get("id")
            id_suffix = (
                f" #{_safe_id_prefix(str(message_id))}"
                if isinstance(message_id, str) and message_id
                else ""
            )
            group_suffix = ""
            if entry.get("kind") == "group":
                group_suffix = f" in {entry.get('group_name') or 'group'}"
            entry_kind = entry.get("kind") or DEFAULT_MESSAGE_KIND
            kind_tag = (
                f" [{entry_kind}]"
                if entry_kind not in (DEFAULT_MESSAGE_KIND, "group", "file")
                else ""
            )
            edited_suffix = " (edited)" if entry.get("edited_at") else ""
            print(
                f"{marker}[{when}] {name} ({sender}){id_suffix}{kind_tag}{group_suffix}{edited_suffix}"
            )
            if entry.get("kind") == "file":
                print(
                    f"  file: {entry.get('name')} "
                    f"({entry.get('size')} bytes, "
                    f"sha256 {short_hash(str(entry.get('sha256') or ''), 12)})"
                )
                print(f"  saved to {entry.get('path')}")
                continue
            quote = _quote_line(inbox, registry, entry)
            if quote is not None:
                print(f"  {quote}")
            for line in str(entry.get("text", "")).splitlines() or [""]:
                print(f"  {line}")
            for reaction in entry.get("reactions") or []:
                reactor = registry.get(str(reaction.get("destination") or ""))
                reactor_name = (
                    reactor.display_name
                    if reactor is not None
                    else str(reaction.get("from") or "unknown")
                )
                print(f"  {reaction.get('reaction')} {reactor_name}")
            entry_expires_at = entry.get("expires_at")
            if isinstance(entry_expires_at, (int, float)) and float(
                entry_expires_at
            ) > time.time():
                print(
                    f"  disappears in {_ago(float(entry_expires_at) - time.time())}"
                )
        if more_unread:
            _print_more_unread(registry, more_unread)
    # Reading a conversation advances its cursor to the newest message
    # just shown, so the next listing only flags what arrived since.  A
    # conversation with unread mail omitted by --limit is left alone:
    # a cursor past the newest shown message would silently clear it.
    # A search is a read-only lookup: messages the filter excluded
    # were never shown, so nothing is marked read and no receipts go
    # out — otherwise an older unread message that did not match would
    # silently lose its unread state.
    if query is None:
        partially_listed = {
            str(entry.get("destination") or "")
            for entry in omitted
            if entry.get("unread")
        }
        newest_shown: Dict[str, int] = {}
        for entry in shown:
            destination = str(entry.get("destination") or "")
            if destination in partially_listed:
                continue
            newest_shown[destination] = max(
                newest_shown.get(destination, 0), int(entry["seq"])
            )
        for destination, up_to_seq in newest_shown.items():
            inbox.mark_read(peer=destination, up_to_seq=up_to_seq)
        # Listing a DM is reading it: send the sender a receipt, unless
        # this node opted out.  Best-effort — failures stay silent.
        outcome = _send_displayed_receipts(config, inbox, shown)
        if not _json_mode(args) and (outcome["sent"] or outcome["queued"]):
            parts = []
            if outcome["sent"]:
                parts.append(f"{outcome['sent']} peer(s) told their message was read")
            if outcome["queued"]:
                parts.append(
                    f"{outcome['queued']} peer(s) unreachable — receipt queued"
                )
            print(f"  ({'; '.join(parts)})")
    return _EXIT_OK


def cmd_messages_delete(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        removed = inbox.delete(args.target)
    except InboxError as exc:
        raise NodeError(str(exc)) from exc
    peer = registry.get(removed.get("destination") or "")
    name = peer.display_name if peer is not None else removed.get("from") or "unknown"
    if _json_mode(args):
        _emit({"deleted": removed}, True)
    else:
        short = _safe_id_prefix(str(removed.get("id") or ""))
        when = _format_message_time(float(removed.get("received_at", 0)))
        print(f"deleted [{short}] from {name} ({when})")
        print(
            "note: only this node's copy was deleted; the sender keeps"
            " theirs, and sealed copies already held by relays are"
            " unaffected."
        )
    return _EXIT_OK


def cmd_messages_export(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    peer_hash = None
    scope = "all senders"
    if getattr(args, "peer", None):
        registry = PeerRegistry(config.peers_path)
        peer_hash = _resolve_inbox_peer(inbox, registry, args.peer)
        scope = f"sender {short_hash(peer_hash)}"
    document = inbox.export_document(peer=peer_hash)
    count = len(document["messages"])
    text = json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True)
    if getattr(args, "output", None):
        atomic_write_text(args.output, text + "\n")
        print(f"Exported {count} messages ({scope}) to {args.output}")
    else:
        print(text)
    return _EXIT_OK


def cmd_messages_import(args: argparse.Namespace) -> int:
    config = _make_config(args)
    if str(args.file) == "-":
        raw = sys.stdin.read()
    else:
        raw = args.file.read_text(encoding="utf-8")
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise NodeError(f"export file is not valid JSON: {exc}") from exc
    inbox = Inbox(config.inbox_path)
    result = inbox.import_messages(document)
    if _json_mode(args):
        _emit(result, True)
    else:
        print(
            f"Imported {result['imported']} messages "
            f"({result['present']} already present, "
            f"{result['skipped']} skipped)"
        )
        for error in result["errors"]:
            ref = error["id"] if error["id"] else "?"
            print(f"  skipped {ref}: {error['error']}")
    return _EXIT_OK if result["skipped"] == 0 else _EXIT_ERROR


def _describe_group(
    group: Dict[str, Any], registry: PeerRegistry
) -> Dict[str, Any]:
    """A group record with member display names for output."""
    members = []
    for member_hash in group["members"]:
        peer = registry.get(member_hash)
        members.append(
            {
                "hash": member_hash,
                "name": peer.display_name if peer is not None else member_hash[:8],
            }
        )
    return {
        "id": group["id"],
        "name": group["name"],
        "members": members,
        "created_at": group["created_at"],
        "created_by": group["created_by"],
    }


def cmd_group_create(args: argparse.Namespace) -> int:
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    try:
        own_hash = node.destination_hexhash
        members: List[str] = [own_hash]
        for target in getattr(args, "peer", []):
            peer = node.resolve_peer(
                target,
                timeout=float(getattr(args, "discover_timeout", 10.0)),
            )
            if peer.hexhash == own_hash:
                raise NodeError("the creator is already a member; drop --peer self")
            if peer.hexhash not in members:
                members.append(peer.hexhash)
        if len(members) < 2:
            raise NodeError("a group needs at least one other peer")
        group = node.groups.create(
            getattr(args, "name"), members, created_by=own_hash
        )
    finally:
        node.stop()

    described = _describe_group(group, node.registry)
    if _json_mode(args):
        _emit({"group": described}, True)
        return _EXIT_OK
    print(f"created group {group['name']} ({group['id'][:8]})")
    for member in described["members"]:
        print(f"  {member['name']} ({member['hash'][:8]})")
    print("Members learn the group from the first message sent to it.")
    return _EXIT_OK


def cmd_group_list(args: argparse.Namespace) -> int:
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    registry = PeerRegistry(config.peers_path)
    groups = [_describe_group(group, registry) for group in store.list_groups()]
    if _json_mode(args):
        _emit({"groups": groups, "count": len(groups)}, True)
        return _EXIT_OK
    if not groups:
        print("No groups yet — create one with `tau group create NAME --peer ...`.")
        return _EXIT_OK
    for group in groups:
        names = ", ".join(member["name"] for member in group["members"])
        print(f"{group['name']} ({group['id'][:8]}) — {names}")
    return _EXIT_OK


def cmd_group_delete(args: argparse.Namespace) -> int:
    config = _make_config(args)
    store = GroupStore(config.groups_path)
    group = store.find(args.group)
    if not store.delete(group["id"]):
        raise NodeError(f"unknown group: {args.group!r}")
    if _json_mode(args):
        _emit({"deleted": group["id"], "name": group["name"]}, True)
        return _EXIT_OK
    print(f"deleted group {group['name']} ({group['id'][:8]})")
    return _EXIT_OK


def _roster_hash(target: Any, members: List[str]) -> Optional[str]:
    """A 32-hex target already in the roster, or ``None``."""
    if not isinstance(target, str):
        return None
    cleaned = target.strip().lower()
    if len(cleaned) != 32 or cleaned not in members:
        return None
    try:
        bytes.fromhex(cleaned)
    except ValueError:
        return None
    return cleaned


def _change_group_members(args: argparse.Namespace, *, adding: bool) -> int:
    """Add peers to, or remove peers from, a group this node created.

    The id derives from the roster, so the change is a successor group
    (same name, new id); the store swaps the old record for the new one
    in a single transaction.
    """
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    try:
        own_hash = node.destination_hexhash
        group = node.groups.find(args.group)
        if not group["local"] or group["creator"] != own_hash:
            raise NodeError("only the creator can change a group's members")
        members = list(group["members"])
        peers: List[Any] = []
        for target in getattr(args, "peer", []):
            # Removing a member only needs the hash already in the
            # roster: an offline member may have left the registry,
            # and resolving it would wait out the whole discover
            # timeout.  Adding still resolves, so a mistyped or
            # unknown peer is refused before the roster changes.
            fast = None if adding else _roster_hash(target, members)
            if fast is not None:
                known = node.registry.get(fast)
                peers.append(
                    SimpleNamespace(
                        hexhash=fast,
                        display_name=(
                            known.display_name
                            if known is not None
                            else fast[:8]
                        ),
                    )
                )
            else:
                peers.append(
                    node.resolve_peer(
                        target,
                        timeout=float(getattr(args, "discover_timeout", 10.0)),
                    )
                )
        for peer in peers:
            if adding:
                if peer.hexhash == own_hash:
                    raise NodeError(
                        "the creator is already a member; drop --peer self"
                    )
                if peer.hexhash in members:
                    raise NodeError(
                        f"{peer.display_name} is already a member of"
                        f" {group['name']!r}"
                    )
                members.append(peer.hexhash)
            else:
                if peer.hexhash == own_hash:
                    raise NodeError("cannot remove the creator from the group")
                if peer.hexhash not in members:
                    raise NodeError(
                        f"{peer.display_name} is not a member of"
                        f" {group['name']!r}"
                    )
                members.remove(peer.hexhash)
        new_group = node.groups.update_members(
            group["id"], members, created_by=own_hash
        )
    finally:
        node.stop()

    described = _describe_group(new_group, node.registry)
    if _json_mode(args):
        _emit({"group": described, "previous_id": group["id"]}, True)
        return _EXIT_OK
    action = "added to" if adding else "removed from"
    names = ", ".join(peer.display_name for peer in peers)
    print(
        f"{names} {action} group {new_group['name']}"
        f" ({group['id'][:8]} -> {new_group['id'][:8]})"
    )
    for member in described["members"]:
        print(f"  {member['name']} ({member['hash'][:8]})")
    print("Members learn the new roster from the next message sent to it.")
    return _EXIT_OK


def cmd_group_add_member(args: argparse.Namespace) -> int:
    return _change_group_members(args, adding=True)


def cmd_group_remove_member(args: argparse.Namespace) -> int:
    return _change_group_members(args, adding=False)


def cmd_group_send(args: argparse.Namespace) -> int:
    text = _read_message(args)
    config = _make_config(args)
    delivered_ids: Set[str] = set()
    node = TauNode(config, name=config.name, on_delivered=delivered_ids.add)
    node.start()
    try:
        group = node.groups.find(args.group)
        response = node.send_group_message(
            group,
            text,
            timeout=float(getattr(args, "timeout", 20.0)),
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
            queue=not getattr(args, "no_queue", False),
        )
        # One-shot send: give reachable members a brief chance to take
        # a queued leg before this process exits.
        if any(result.get("queued") for result in response["results"]):
            node.flush_outbox(timeout=3.0, max_peers=5)
            for result in response["results"]:
                if not result.get("queued"):
                    continue
                # Only a delivery acknowledgement counts as delivered; an
                # entry that merely vanished was cancelled, expired, or
                # taken by another process.
                if result["id"] in delivered_ids:
                    result["queued"] = False
                    result["delivered"] = True
                elif node.mailbox.get_outbox(result["id"]) is None:
                    result["queued"] = False
                    result["ok"] = False
                    result["error"] = "left the outbox without a delivery confirmation"
    finally:
        node.stop()

    failed_legs = [
        r for r in response["results"] if not r.get("delivered") and not r.get("queued")
    ]
    exit_code = _EXIT_ERROR if failed_legs else _EXIT_OK
    if _json_mode(args):
        _emit(response, True)
        return exit_code
    delivered = [r for r in response["results"] if r.get("delivered")]
    queued = [r for r in response["results"] if r.get("queued")]
    failed = failed_legs
    for result in delivered:
        print(f"delivered to {result.get('to_name') or result['to'][:8]}")
    for result in queued:
        print(
            f"queued for {result.get('to_name') or result['to'][:8]}"
            " — they are unreachable right now"
        )
    for result in failed:
        print(
            f"failed for {result.get('to_name') or result['to'][:8]}:"
            f" {result.get('error') or 'unknown error'}"
        )
    return exit_code


def cmd_react(args: argparse.Namespace) -> int:
    if bool(getattr(args, "clear", False)) == bool(getattr(args, "reaction", None)):
        raise NodeError("give a reaction, or --clear to retract it (not both)")
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        target = inbox.resolve(args.message)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    if not target.get("id"):
        raise NodeError("that message has no id to react to")
    author = target.get("destination") or ""
    if not author:
        raise NodeError("that message has no known sender to react to")
    if getattr(args, "clear", False):
        token = ""
    else:
        # Validate the token before starting the node: a bad token is a
        # usage error, not a delivery failure.  A blank argument cleans to
        # the retraction token, so reject it unless --clear was given.
        token = clean_reaction(str(args.reaction))
        if not token:
            raise NodeError("reaction is empty; use --clear to retract it")
    node = TauNode(config, name=config.name)
    node.start()
    try:
        try:
            response = node.send_reaction(
                author,
                str(target["id"]),
                token,
                timeout=float(getattr(args, "timeout", 20.0)),
                discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
            )
        except NodeError as exc:
            # Reactions ride the live link only in v1 — there is no
            # outbox queue for them yet — so an unreachable peer is a
            # plain failure, not a silent queue.
            raise NodeError(
                f"could not deliver reaction: {exc} "
                "(reactions need the peer online in v1)"
            ) from exc
    finally:
        node.stop()

    peer = registry.get(author)
    name = peer.display_name if peer is not None else target.get("from") or author
    if _json_mode(args):
        _emit(
            {
                "target": target["id"],
                "to": author,
                "to_name": name,
                "reaction": token,
                "cleared": not token,
                "id": response.get("id"),
            },
            True,
        )
    elif not token:
        print(f"retracted your reaction to {name}'s message {_safe_id_prefix(str(target['id']))}")
    else:
        print(f"reacted {token} to {name}'s message {_safe_id_prefix(str(target['id']))}")
    return _EXIT_OK


def cmd_retract(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        target = inbox.resolve(args.message)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    if not target.get("id"):
        raise NodeError("that message has no id to retract")
    if target.get("direction") != "out":
        raise NodeError(
            "that message was not sent by this node;"
            " only your own sent messages can be retracted"
        )
    if target.get("kind") == "group":
        raise NodeError("group messages cannot be retracted in v1")
    recipient = target.get("destination") or ""
    if not recipient:
        raise NodeError("that message has no known recipient to retract from")
    # A message still in the outbox has not reached its recipient yet; a
    # retract answered "not found" would let the queued copy arrive later.
    # Cancel the queued send instead.
    if Mailbox(config.mailbox_path).get_outbox(str(target["id"])) is not None:
        raise NodeError(
            "that message is still queued for delivery;"
            " use `tau outbox cancel` to stop it instead"
        )
    timeout = float(getattr(args, "timeout", 20.0))
    discover_timeout = float(getattr(args, "discover_timeout", 10.0))
    if _send_routes_via_running_node(config):
        # A serving node owns the data directory: retract through it
        # instead of starting a second Reticulum instance on the same
        # identity — a second instance exits when it cannot bind the
        # TCP listen port the serving node holds (see cmd_send).
        try:
            response = _retract_via_running_node(
                config,
                recipient,
                str(target["id"]),
                timeout=timeout,
                discover_timeout=discover_timeout,
            )
        except NodeError as exc:
            # Retracts ride the live link only in v1 — there is no
            # outbox queue for them yet — so an unreachable peer is a
            # plain failure, not a silent queue.
            raise NodeError(
                f"could not deliver retract: {exc} "
                "(retracts need the peer online in v1)"
            ) from exc
    else:
        node = TauNode(config, name=config.name)
        node.start()
        try:
            try:
                response = node.send_retraction(
                    recipient,
                    str(target["id"]),
                    timeout=timeout,
                    discover_timeout=discover_timeout,
                )
            except NodeError as exc:
                # Retracts ride the live link only in v1 — there is no
                # outbox queue for them yet — so an unreachable peer is a
                # plain failure, not a silent queue.
                raise NodeError(
                    f"could not deliver retract: {exc} "
                    "(retracts need the peer online in v1)"
                ) from exc
        finally:
            node.stop()

    # The peer confirmed (or already had it gone): drop this node's own
    # sent copy too, so the message is gone on both sides.  A local
    # delete failure is reported, not hidden — the retract itself
    # already succeeded.
    try:
        inbox.delete(str(target["id"]))
    except InboxError as exc:
        raise NodeError(
            "retract delivered but the local copy could not be deleted:"
            f" {exc}"
        ) from exc
    # The sent record lives in mailbox.json, not the inbox: drop it
    # too, or `tau messages --sent` keeps showing the retracted text.
    # Only after the retract and the inbox delete succeeded, so a
    # failed retract never erases the sent record.
    Mailbox(config.mailbox_path).discard_sent(str(target["id"]))

    peer = registry.get(recipient)
    name = peer.display_name if peer is not None else short_hash(recipient)
    if _json_mode(args):
        _emit(
            {
                "target": target["id"],
                "to": recipient,
                "to_name": name,
                "deleted": bool(response.get("deleted")),
                "id": response.get("id"),
            },
            True,
        )
    elif response.get("deleted"):
        print(
            f"retracted your message {_safe_id_prefix(str(target['id']))}"
            f" from {name}'s inbox"
        )
    else:
        print(
            f"{name} had no copy of {_safe_id_prefix(str(target['id']))}"
            " to retract"
        )
    return _EXIT_OK

def cmd_edit(args: argparse.Namespace) -> int:
    text = _read_message(args)
    try:
        cleaned = normalize_message(text)
    except ProtocolError as exc:
        raise NodeError(str(exc)) from exc
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        target = inbox.resolve(args.target)
    except ValueError as exc:
        raise NodeError(str(exc)) from exc
    if not target.get("id"):
        raise NodeError("that message has no id to edit")
    if target.get("kind") == "group":
        raise NodeError(
            "group messages cannot be edited; send a new message instead"
        )
    if target.get("kind") == "file":
        raise NodeError("a file record has no text to edit")
    if target.get("direction") != "out":
        raise NodeError(
            "that message was not sent by this node;"
            " only your own sent messages can be edited"
        )
    author = target.get("destination") or ""
    if not author:
        raise NodeError("that message has no known recipient to edit it for")
    # A message still in the outbox has not reached its recipient yet; an
    # edit answered "not found" would let the queued copy arrive later
    # with the old text. Cancel the queued send instead.
    if Mailbox(config.mailbox_path).get_outbox(str(target["id"])) is not None:
        raise NodeError(
            "that message is still queued for delivery;"
            " use `tau outbox cancel` to stop it and send a new message instead"
        )
    timeout = float(getattr(args, "timeout", 20.0))
    discover_timeout = float(getattr(args, "discover_timeout", 10.0))
    if _send_routes_via_running_node(config):
        # A serving node owns the data directory: edit through it
        # instead of starting a second Reticulum instance on the same
        # identity — a second instance exits when it cannot bind the
        # TCP listen port the serving node holds (see cmd_send).
        try:
            response = _edit_via_running_node(
                config,
                author,
                str(target["id"]),
                cleaned,
                timeout=timeout,
                discover_timeout=discover_timeout,
            )
        except NodeError as exc:
            # Edits ride the live link only in v1 — there is no
            # outbox queue for them yet — so an unreachable peer is a
            # plain failure, not a silent queue.
            raise NodeError(
                f"could not deliver edit: {exc} "
                "(edits need the peer online in v1)"
            ) from exc
    else:
        node = TauNode(config, name=config.name)
        node.start()
        try:
            try:
                response = node.send_edit(
                    author,
                    str(target["id"]),
                    cleaned,
                    timeout=timeout,
                    discover_timeout=discover_timeout,
                )
            except NodeError as exc:
                # Edits ride the live link only in v1 — there is no
                # outbox queue for them yet — so an unreachable peer is a
                # plain failure, not a silent queue.
                raise NodeError(
                    f"could not deliver edit: {exc} "
                    "(edits need the peer online in v1)"
                ) from exc
        finally:
            node.stop()

    peer = registry.get(author)
    name = peer.display_name if peer is not None else target.get("from") or author
    if _json_mode(args):
        _emit(
            {
                "target": target["id"],
                "to": author,
                "to_name": name,
                "text": cleaned,
                "edited": bool(response.get("edited", True)),
                "id": response.get("id"),
            },
            True,
        )
    elif response.get("edited") is False:
        print(
            f"edit was not applied to {name}'s copy of "
            f"{_safe_id_prefix(str(target['id']))} "
            "(unknown message, or a newer edit already applied)"
        )
    else:
        print(f"edited your message to {name} {_safe_id_prefix(str(target['id']))}")
    return _EXIT_OK


def _print_more_unread(
    registry: PeerRegistry, more_unread: Dict[str, int]
) -> None:
    """Hint lines for unread mail a limited listing did not show."""
    for destination, count in sorted(more_unread.items()):
        peer = registry.get(destination)
        name = peer.display_name if peer is not None else "unknown"
        print(
            f"{count} more unread from {name} ({short_hash(destination)})"
            " — raise --limit to see them."
        )
def _safe_id_prefix(ref: str, length: int = 8) -> str:
    """First *length* printable characters of *ref*.

    Held ids arrive from remote senders and may contain terminal escapes
    or newlines; never print them verbatim. JSON output keeps the raw id.
    Non-printable characters are dropped from the full id first, then
    the result is truncated — so the tag is always a true prefix of the
    stored id that ``Inbox.resolve`` can match.
    """
    cleaned = "".join(char for char in ref if char.isprintable())
    return cleaned[:length]


def cmd_outbox_held(args: argparse.Namespace) -> int:
    config = _make_config(args)
    mailbox = Mailbox(config.mailbox_path)
    registry = PeerRegistry(config.peers_path)
    envelopes = mailbox.held_list()
    usage = mailbox.held_usage()

    def describe_sender(row: Dict[str, Any]) -> Dict[str, Any]:
        sender = row["sender"]
        peer = registry.get(bytes.fromhex(sender)) if sender else None
        return {
            "sender": sender,
            "name": peer.display_name if peer is not None else "",
            "count": row["count"],
            "bytes": row["bytes"],
        }

    def describe(envelope: Any) -> Dict[str, Any]:
        peer = registry.get(envelope.to)
        sender_peer = (
            registry.get(bytes.fromhex(envelope.depositor))
            if envelope.depositor
            else None
        )
        return {
            "id": envelope.id,
            "to": envelope.to_hex,
            "to_name": peer.display_name if peer is not None else "",
            "from": envelope.depositor,
            "from_name": sender_peer.display_name if sender_peer is not None else "",
            "stored_at": envelope.stored_at,
            "expires_at": envelope.expires_at,
            "blob_bytes": len(envelope.blob),
        }

    if _json_mode(args):
        _emit(
            {
                "held": [describe(e) for e in envelopes],
                "count": len(envelopes),
                "senders": [describe_sender(row) for row in usage],
            },
            True,
        )
    else:
        if not envelopes:
            print("Not holding any sealed messages for other peers.")
        else:
            now = time.time()
            header = ("ID", "TO", "NAME", "AGE", "EXPIRES IN")
            rows: List[tuple] = [header]
            for envelope in envelopes:
                peer = registry.get(envelope.to)
                name = peer.display_name if peer is not None else "-"
                rows.append(
                    (
                        _safe_id_prefix(envelope.id),
                        envelope.to_hex,
                        name,
                        _ago(now - envelope.stored_at),
                        _ago(envelope.expires_at - now),
                    )
                )
            widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
            for row in rows:
                print("  ".join(value.ljust(widths[i]) for i, value in enumerate(row)))
            print(f"\n{len(envelopes)} sealed message(s) held for other peers.")
        if usage:
            print("\nHeld per sender:")
            sender_header = ("SENDER", "NAME", "COUNT", "BYTES")
            sender_rows: List[tuple] = [sender_header]
            for row in usage:
                peer = (
                    registry.get(bytes.fromhex(row["sender"]))
                    if row["sender"]
                    else None
                )
                sender_rows.append(
                    (
                        row["sender"] if row["sender"] else "unknown",
                        peer.display_name if peer is not None else "-",
                        str(row["count"]),
                        str(row["bytes"]),
                    )
                )
            sender_widths = [
                max(len(row[i]) for row in sender_rows)
                for i in range(len(sender_header))
            ]
            for row in sender_rows:
                print(
                    "  ".join(
                        value.ljust(sender_widths[i])
                        for i, value in enumerate(row)
                    )
                )
    return _EXIT_OK


def cmd_outbox_drop_held(args: argparse.Namespace) -> int:
    config = _make_config(args)
    mailbox = Mailbox(config.mailbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        # Resolve and remove under one cross-process transaction, so the
        # reported recipient belongs to the envelope actually removed and
        # a concurrent fetch or prune cannot slip in between.
        with mailbox.transaction():
            envelope = mailbox.resolve_held(args.target)
            dropped = mailbox.drop_held(envelope.id, tombstone=True)
    except MailboxError as exc:
        raise NodeError(str(exc)) from exc
    if not dropped:
        raise NodeError(f"held message {args.target!r} is no longer present")
    peer = registry.get(envelope.to)
    name = peer.display_name if peer is not None else ""
    if _json_mode(args):
        payload = {
            "id": envelope.id,
            "to": envelope.to_hex,
            "to_name": name,
            # The delivery caveat is part of the result, not just the
            # text rendering: a copy already fetched by the recipient
            # may still be delivered.
            "note": (
                "a copy already fetched by the recipient may still be"
                " delivered; dropping stops this node handing it out again"
            ),
        }
        _emit({"dropped": payload}, True)
    else:
        print(
            f"dropped {_safe_id_prefix(envelope.id)}"
            f" — held for {name or envelope.to_hex}"
        )
        print(
            "note: a copy already fetched by the recipient may still be"
            " delivered; dropping stops this node handing it out again."
        )
    return _EXIT_OK


def cmd_outbox_cancel(args: argparse.Namespace) -> int:
    config = _make_config(args)
    mailbox = Mailbox(config.mailbox_path)
    registry = PeerRegistry(config.peers_path)
    try:
        entry = mailbox.resolve_outbox(args.target)
    except MailboxError as exc:
        raise NodeError(str(exc)) from exc
    mailbox.remove_outbox(entry.id)
    peer = registry.get(entry.to)
    name = peer.display_name if peer is not None else entry.to_name
    if _json_mode(args):
        payload = entry.to_dict()
        payload["to_name"] = name
        _emit({"cancelled": payload}, True)
    else:
        print(f"cancelled {entry.id[:8]} — queued for {name or entry.to_hex}")
        print(
            "note: sealed copies already held by other peers may still be"
            " delivered; cancelling stops this node's future attempts."
        )
    return _EXIT_OK


def cmd_outbox_retry(args: argparse.Namespace) -> int:
    config = _make_config(args)
    mailbox = Mailbox(config.mailbox_path)
    registry = PeerRegistry(config.peers_path)
    target_entry = None
    if getattr(args, "target", None):
        try:
            target_entry = mailbox.resolve_outbox(args.target)
        except MailboxError as exc:
            raise NodeError(str(exc)) from exc
    elif mailbox.counts()["outbox"] == 0:
        # No work to do: do not start a node just to report an empty queue.
        if _json_mode(args):
            _emit(
                {
                    "delivered": 0,
                    "deposited": 0,
                    "received": 0,
                    "still_queued": 0,
                    "left_queue_unconfirmed": 0,
                    "rounds_pending": False,
                },
                True,
            )
        else:
            print("Outbox is empty — nothing waiting on offline peers.")
        return _EXIT_OK

    target_to = None
    target_name = None
    if target_entry is not None:
        peer = registry.get(target_entry.to)
        if peer is None:
            raise NodeError(
                "recipient "
                f"{target_entry.to_name or target_entry.to_hex} is not in the"
                " peer registry — there is no route to retry delivery on"
            )
        target_to = target_entry.to
        target_name = peer.display_name or target_entry.to_name

    # One-shot attempt, like `tau send`: a transient node shares the data
    # directory with a running serve by design, and the flush ignores
    # backoff — the operator asked for a retry *now*, not at the next
    # scheduled attempt.  The node runs without its background mail loop
    # so the loop's first sweep cannot take the mail lock and make this
    # flush silently report nothing moved; the flush waits briefly for
    # an announce-triggered round instead.  A round that wins the race
    # still moves the mail, so the outcome below is read back from the
    # queue itself rather than trusted from the flush summary alone:
    # deliveries are confirmed by the node's delivery callback (which
    # also fires for announce-triggered rounds in this process), and
    # new holders on the entries left behind are deposits.  An entry
    # that vanished without a delivery confirmation was cancelled,
    # expired, or delivered by another process — never counted delivered.
    queued_before = {e.id: e for e in mailbox.outbox_list()}
    delivered_ids: Set[str] = set()
    node = TauNode(config, name=config.name, on_delivered=delivered_ids.add)
    node.start(mail_loop=False)
    try:
        summary = node.flush_outbox(
            timeout=float(getattr(args, "timeout", 6.0)),
            # One-shot retry-all visits every queued recipient; the
            # background sweep's peer cap does not apply here.
            max_peers=None,
            respect_backoff=False,
            to=target_to,
            message_id=target_entry.id if target_entry is not None else None,
            # A single announce-triggered round for one peer finishes in
            # seconds when the peer is reachable; only a round stuck on
            # per-request timeouts to an unreachable peer can run long,
            # and then there is nothing this flush could have delivered
            # anyway, so waiting longer buys nothing.
            wait_for_lock=60.0,
        )
        if summary.get("lock_busy"):
            raise NodeError(
                "another mail round is still running after 60s; nothing"
                " was attempted — try again, or leave a node serving"
            )
        # Let a late announce-triggered round finish before reading
        # final state, so a delivery landing just after this flush is
        # still reported instead of showing up as still queued.
        rounds_pending = not node.wait_for_mail_rounds(timeout=30.0)
    finally:
        node.stop()

    # The node's mailbox is a separate instance on the same file, so reload
    # before reporting what is still queued.
    mailbox.load()
    now = time.time()
    remaining = mailbox.outbox_list()
    remaining_ids = {e.id for e in remaining}
    delivered = sum(1 for entry_id in queued_before if entry_id in delivered_ids)
    expired = sum(
        1
        for entry_id, entry in queued_before.items()
        if entry_id not in remaining_ids
        and entry_id not in delivered_ids
        and entry.expires_at <= now
    )
    left_unconfirmed = sum(
        1
        for entry_id, entry in queued_before.items()
        if entry_id not in remaining_ids
        and entry_id not in delivered_ids
        and entry.expires_at > now
    )
    # Deposits are read back from the holders persisted on the entries
    # left in the queue, not from the flush summary: counting both would
    # report one deposit twice, and the summary cannot see deposits made
    # by a concurrent announce round.
    deposited = 0
    for entry in remaining:
        prior = queued_before.get(entry.id)
        if prior is not None:
            deposited += len(set(entry.held_by) - set(prior.held_by))
    payload: Dict[str, Any] = {
        "delivered": delivered,
        "deposited": deposited,
        "received": summary["received"],
        "still_queued": len(remaining),
        "left_queue_unconfirmed": left_unconfirmed,
        "rounds_pending": rounds_pending,
    }
    fresh = None
    if target_entry is not None:
        fresh = mailbox.get_outbox(target_entry.id)
        payload["target"] = target_entry.id
        payload["target_delivered"] = target_entry.id in delivered_ids
        if fresh is not None:
            payload["target_attempts"] = fresh.attempts
            payload["target_next_attempt_at"] = fresh.next_attempt_at

    if _json_mode(args):
        _emit(payload, True)
    elif target_entry is not None:
        label = f"{target_entry.id[:8]} for {target_name or target_entry.to_hex}"
        if fresh is None and target_entry.id in delivered_ids:
            print(f"delivered {label}")
        elif fresh is None:
            print(
                f"no longer queued: {label} — it left the queue without"
                " a delivery confirmation (cancelled, expired, or"
                " delivered by another process)"
            )
        else:
            next_try = fresh.next_attempt_at
            if next_try is None or next_try <= time.time():
                next_text = "now"
            else:
                next_text = f"in {_ago(next_try - time.time())}"
            print(
                f"still queued: {label} (attempt {fresh.attempts}, next try {next_text})"
            )
            if deposited:
                print(
                    f"  left a sealed copy with {deposited} peer(s)"
                    " for store-and-forward delivery"
                )
    else:
        print(
            f"retried {len(queued_before)} queued message(s):"
            f" {delivered} delivered,"
            f" {deposited} deposited,"
            f" {summary['received']} received"
        )
        if remaining:
            print(
                f"{len(remaining)} message(s) still queued — they will be"
                " retried automatically, or run `tau outbox retry` again"
            )
        if left_unconfirmed:
            print(
                f"{left_unconfirmed} message(s) left the queue without a"
                " delivery confirmation (cancelled, expired, or delivered"
                " by another process)"
            )
        if expired:
            print(f"{expired} message(s) expired during the retry")
    if rounds_pending and not _json_mode(args):
        print(
            "note: a mail round was still running when the retry"
            " finished; re-run `tau outbox retry` to confirm the outcome"
        )
    return _EXIT_OK


def _resolve_allowlist_peer(registry: PeerRegistry, value: str) -> str:
    """Resolve a peer name, hash or prefix to a destination hash.

    Aliases are excluded on purpose: the allow-list is a security
    boundary, so a local label must never stand in for the peer's
    asserted name.
    """
    peer = registry.resolve(value, include_alias=False)
    if peer is not None:
        return peer.hexhash
    try:
        return clean_destination(value)
    except AllowListError:
        raise AllowListError(f"unknown peer or destination: {value!r}") from None


def cmd_config(args: argparse.Namespace) -> int:
    """Show or change this node's own settings (stored in node.json)."""
    config = _make_config(args)
    json_output = _json_mode(args)
    command = getattr(args, "config_command", None)

    if command == "accept-files":
        enabled = getattr(args, "state", "on") == "on"
        set_node_setting(config.data_dir, "accept_files", enabled)
        state = "on" if enabled else "off"
        if json_output:
            _emit({"accept_files": enabled}, True)
        else:
            print(f"Accept files: {state}")
            if not enabled:
                print("  this node refuses file transfers unless the sender")
                print("  is on its DM allow-list")
        return _EXIT_OK

    if command == "read-receipts":
        enabled = getattr(args, "state", "on") == "on"
        set_node_setting(config.data_dir, "send_read_receipts", enabled)
        state = "on" if enabled else "off"
        if json_output:
            _emit({"send_read_receipts": enabled}, True)
        else:
            print(f"Read receipts: {state}")
            if not enabled:
                print("  this node will not send read receipts; senders see")
                print("  its messages as delivered, never read")
        return _EXIT_OK

    settings = {
        "accept_files": accept_files_enabled(config.data_dir),
        "send_read_receipts": send_read_receipts_enabled(config.data_dir),
    }
    if json_output:
        _emit(settings, True)
    else:
        accept_state = "on" if settings["accept_files"] else "off"
        receipts_state = "on" if settings["send_read_receipts"] else "off"
        print(f"Settings ({config.data_dir / 'node.json'}):")
        print(f"  accept files: {accept_state}")
        print(f"  send read receipts: {receipts_state}")
        print("  change with: tau config accept-files on|off")
        print("  change with: tau config read-receipts on|off")
    return _EXIT_OK


def cmd_allowlist(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        allowlist = load_allowlist(config.allowlist_path)
    except AllowListError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return _EXIT_ERROR

    command = getattr(args, "allowlist_command", None)
    registry = PeerRegistry(config.peers_path)
    changed = False

    if command == "add":
        destination = _resolve_allowlist_peer(registry, args.peer)
        if destination not in allowlist.allow:
            allowlist = allowlist.with_added(destination)
            changed = True
    elif command == "remove":
        destination = _resolve_allowlist_peer(registry, args.peer)
        if destination in allowlist.allow:
            allowlist = allowlist.with_removed(destination)
            changed = True
    elif command == "on":
        if not allowlist.enabled:
            allowlist = allowlist.with_enabled(True)
            changed = True
    elif command == "off":
        if allowlist.enabled:
            allowlist = allowlist.with_enabled(False)
            changed = True

    if changed:
        save_allowlist(config.allowlist_path, allowlist)

    def describe(destination: str) -> Dict[str, Any]:
        peer = registry.get(destination)
        return {
            "destination": destination,
            # The asserted name, not the local alias: the operator must
            # see who was actually allowed, and an alias could mask that.
            "name": (
                (peer.name or short_hash(peer.hexhash))
                if peer is not None
                else ""
            ),
        }

    if json_output:
        payload = allowlist.to_dict()
        payload["entries"] = [describe(value) for value in sorted(allowlist.allow)]
        payload["path"] = str(config.allowlist_path)
        _emit(payload, True)
    else:
        state = "enforced" if allowlist.enabled else "off"
        print(f"DM allow-list ({config.allowlist_path}): {state}")
        if not allowlist.allow:
            print("  no senders listed — when enforced, nobody may message this node")
        for entry in sorted(allowlist.allow):
            peer = registry.get(entry)
            name = (
                f" ({peer.name or short_hash(peer.hexhash)})"
                if peer is not None
                else ""
            )
            print(f"  {entry}{name}")
        if changed:
            print("  (a running serve applies saved changes when it restarts)")
    return _EXIT_OK


def cmd_block(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    try:
        blocklist = load_blocklist(config.blocklist_path)
    except BlockListError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return _EXIT_ERROR

    # Forms: `tau block <peer>` blocks, `tau unblock <peer>` unblocks,
    # `tau block list` lists, and `tau block add/remove <peer>` keep
    # working as the allow-list-shaped aliases.  A peer literally
    # named "list", "add" or "remove" is blocked via `block add <name>`.
    command = getattr(args, "block_command", None)
    target = getattr(args, "target", None)
    peer_arg = getattr(args, "peer", None)
    registry = PeerRegistry(config.peers_path)
    changed = False

    if command == "remove":
        destination = _resolve_allowlist_peer(registry, peer_arg)
        if destination in blocklist.blocked:
            blocklist = blocklist.with_removed(destination)
            changed = True
    elif target in (None, "list") and peer_arg is None:
        pass  # list
    elif target == "add":
        if peer_arg is None:
            raise NodeError("`tau block add` needs a peer")
        destination = _resolve_allowlist_peer(registry, peer_arg)
        if destination not in blocklist.blocked:
            blocklist = blocklist.with_added(destination)
            changed = True
    elif target == "remove":
        if peer_arg is None:
            raise NodeError("`tau block remove` needs a peer")
        destination = _resolve_allowlist_peer(registry, peer_arg)
        if destination in blocklist.blocked:
            blocklist = blocklist.with_removed(destination)
            changed = True
    elif peer_arg is not None:
        raise NodeError(f"unexpected argument: {peer_arg!r}")
    else:
        destination = _resolve_allowlist_peer(registry, target)
        if destination not in blocklist.blocked:
            blocklist = blocklist.with_added(destination)
            changed = True

    if changed:
        save_blocklist(config.blocklist_path, blocklist)

    def describe(destination: str) -> Dict[str, Any]:
        peer = registry.get(destination)
        return {
            "destination": destination,
            "name": peer.display_name if peer is not None else "",
        }

    if json_output:
        payload = blocklist.to_dict()
        payload["entries"] = [describe(value) for value in sorted(blocklist.blocked)]
        payload["path"] = str(config.blocklist_path)
        _emit(payload, True)
    else:
        print(f"Block-list ({config.blocklist_path}):")
        if not blocklist.blocked:
            print("  nobody blocked")
        for entry in sorted(blocklist.blocked):
            peer = registry.get(entry)
            name = f" ({peer.display_name})" if peer is not None else ""
            print(f"  {entry}{name}")
        if changed:
            print("  (a running serve applies saved changes when it restarts)")
    return _EXIT_OK


def cmd_ping(args: argparse.Namespace) -> int:
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    try:
        response = node.ping(
            args.to,
            timeout=float(getattr(args, "timeout", 15.0)),
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
        )
    finally:
        node.stop()

    if _json_mode(args):
        _emit(response, True)
    else:
        rtt = response.get("_rtt")
        latency = f" ({rtt * 1000:.0f} ms)" if isinstance(rtt, (int, float)) else ""
        print(f"pong from {response.get('server', args.to)}{latency}")
    return _EXIT_OK


def cmd_doctor(args: argparse.Namespace) -> int:
    """Report the local data directory's health without changing it."""
    config = _make_config(args)
    checks = run_checks(config)
    if _json_mode(args):
        _emit(
            {"ok": all_ok(checks), "checks": [check.to_dict() for check in checks]},
            True,
        )
    else:
        for check in checks:
            print(f"{check.status:<4} {check.name}: {check.detail}")
        failed = [check for check in checks if check.failed]
        if failed:
            print(f"\n{len(failed)} check(s) failed")
        else:
            print("\ndata directory healthy")
    return _EXIT_ERROR if not all_ok(checks) else _EXIT_OK


def cmd_info(args: argparse.Namespace) -> int:
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    try:
        response = node.request_info(
            args.to,
            timeout=float(getattr(args, "timeout", 15.0)),
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
        )
    finally:
        node.stop()

    if _json_mode(args):
        _emit(response, True)
    else:
        for key in (
            "name",
            "destination",
            "version",
            "rns_version",
            "protocol",
            "uptime",
            "peers",
            "caller",
        ):
            if key in response:
                print(f"{key + ':':<13} {response[key]}")
        lxmf_info = response.get("lxmf") or {}
        if lxmf_info.get("enabled"):
            print(f"{'lxmf:':<13} delivery {lxmf_info.get('delivery')}")
            if lxmf_info.get("propagation"):
                print(
                    f"{'lxmf:':<13} propagation {lxmf_info.get('propagation')}"
                )
            else:
                print(f"{'lxmf:':<13} propagation disabled")
    return _EXIT_OK


# -- entry point -----------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args)

    try:
        return int(args.func(args))
    except KeyboardInterrupt:
        return _EXIT_INTERRUPTED
    except (NodeError, PeerError, IdentityError, ProtocolError) as exc:
        print(f"tau: error: {exc}", file=sys.stderr)
        return _EXIT_ERROR
    except (OSError, ValueError) as exc:
        print(f"tau: error: {exc}", file=sys.stderr)
        return _EXIT_ERROR


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())


def _cmd_messages_sent(
    args: argparse.Namespace,
    config: TauConfig,
    registry: PeerRegistry,
    limit: Optional[int],
) -> int:
    """List messages this node sent, with their read state."""
    mailbox = Mailbox(config.mailbox_path)
    peer_hash = None
    if getattr(args, "peer", None):
        peer_hash = _resolve_sent_peer(registry, args.peer)
    entries = mailbox.sent_list()
    if peer_hash is not None:
        entries = [entry for entry in entries if entry.get("to") == peer_hash]
    entries.reverse()  # newest first
    if limit is not None:
        entries = entries[: max(0, limit)]
    queued_ids = {entry.id for entry in mailbox.outbox_list()}

    def describe(entry: Dict[str, Any]) -> Dict[str, Any]:
        peer = registry.get(entry.get("to") or "")
        read_at = entry.get("read_at")
        # ``delivered_at`` is set only once a delivery was confirmed.  Records
        # from before it was tracked have no such key and count as delivered.
        delivered_at = entry.get("delivered_at", entry.get("sent_at"))
        if entry["id"] in queued_ids:
            state = "queued"
        elif read_at is not None:
            state = "read"
        elif delivered_at is not None:
            state = "delivered"
        else:
            # Cancelled or expired before any delivery was confirmed.
            state = "unknown"
        return {
            "id": entry["id"],
            "to": entry.get("to"),
            "to_name": peer.display_name if peer is not None else "",
            "text": entry.get("text", ""),
            "sent_at": entry.get("sent_at"),
            "read_at": read_at,
            "state": state,
        }

    described = [describe(entry) for entry in entries]
    if _json_mode(args):
        _emit({"messages": described, "count": len(described)}, True)
        return _EXIT_OK
    if not described:
        print("No messages sent yet.")
        return _EXIT_OK
    for entry in described:
        name = entry["to_name"] or short_hash(entry["to"] or "")
        when = _format_message_time(float(entry.get("sent_at") or 0))
        if entry["state"] == "read":
            state = f"read {_format_message_time(float(entry['read_at'] or 0))}"
        else:
            state = entry["state"]
        print(f"[{when}] to {name} — {state}")
        for line in str(entry.get("text", "")).splitlines() or [""]:
            print(f"  {line}")
    return _EXIT_OK
