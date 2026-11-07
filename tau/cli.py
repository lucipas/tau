"""Command line interface for tau."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import socket
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

from . import __version__
from .allowlist import (
    AllowListError,
    clean_destination,
    load_allowlist,
    save_allowlist,
)
from .config import (
    TauConfig,
    accept_files_enabled,
    set_node_setting,
    write_node_metadata,
    write_rns_config,
)
from .identity import (
    IdentityError,
    _identity_lock,
    destination_hash,
    load_identity,
    load_or_create_identity,
    save_identity,
)
from .inbox import Inbox
from .lxmf import lxmf_delivery_hash
from .mailbox import Mailbox, MailboxError
from .node import (
    InstanceAlreadyRunning,
    InstanceLock,
    NodeError,
    TauNode,
    check_tcp_listen_ports,
)
from .peers import Peer, PeerError, PeerRegistry
from .protocol import DEFAULT_TCP_PORT as PROTOCOL_TCP_PORT
from .protocol import PROTOCOL_VERSION, ProtocolError, short_hash
from .util import atomic_write_text
from .tangents.rules import RulesError, Ruleset, load_ruleset, save_ruleset
from .tangents.tan import (
    DEFAULT_FEED_LIMIT,
    MAX_GOSSIP_TTL,
    TanApp,
    TanError,
    annotate_reactions,
    annotate_replies,
    apply_display_rules,
    author_stats,
    export_entries,
    feed,
    follow,
    following,
    import_entries,
    init_db,
    resolve_author,
    resolve_cached_author,
    resolve_delete_target,
    resolve_post,
    search_posts,
    thread,
    timeline,
    unfollow,
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
        "--to", required=True, metavar="NAME|HASH", help="peer name or destination hash"
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
    peers.set_defaults(func=cmd_peers)

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

    outbox = subparsers.add_parser(
        "outbox",
        parents=[common],
        help="list messages queued for offline peers (store-and-forward)",
    )
    outbox.set_defaults(func=cmd_outbox)
    outbox_sub = outbox.add_subparsers(
        dest="outbox_command", metavar="OUTBOX_COMMAND"
    )

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
    allowlist_add.add_argument("peer", help="peer name, destination hash or hash prefix")
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
        "--limit",
        type=int,
        default=30,
        metavar="N",
        help="show the newest N messages (default 30)",
    )
    messages.set_defaults(func=cmd_messages)

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
    tan_post.add_argument("text", nargs="?", help="post text (reads stdin when omitted)")
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
        tan_react.add_argument(
            "--timeout", type=float, default=15.0, metavar="SECONDS"
        )
        tan_react.set_defaults(
            func=cmd_tan_like if verb == "like" else cmd_tan_unlike
        )

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
    tan_delete.add_argument(
        "--ttl", type=int, default=MAX_GOSSIP_TTL, metavar="HOPS"
    )
    tan_delete.add_argument(
        "--timeout", type=float, default=15.0, metavar="SECONDS"
    )
    tan_delete.set_defaults(func=cmd_tan_delete)

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

    tan_thread = tan_sub.add_parser(
        "thread",
        parents=[common],
        help="show a post with its replies as a nested thread",
    )
    tan_thread.add_argument(
        "ref", metavar="HASH|PREFIX", help="post or reply hash (full hash or unique prefix)"
    )
    tan_thread.set_defaults(func=cmd_tan_thread)

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
        metavar="post,like,unlike,delete",
        help="comma-separated entry kinds to display (display-only: "
        "every kind is still stored, so chains stay complete)",
    )
    tan_rules.add_argument(
        "--all-kinds",
        action="store_true",
        help="display every entry kind (clears --kinds)",
    )
    tan_rules.set_defaults(func=cmd_tan_rules)

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


def _print_peer_table(peers: Iterable[Peer], *, now: Optional[float] = None) -> None:
    rows = list(peers)
    if not rows:
        print("No peers known. Run `tau discover` or `tau serve` first.")
        return
    timestamp = now if now is not None else time.time()
    header = ("DESTINATION", "NAME", "HOPS", "LAST SEEN", "LXMF")
    table: List[tuple] = [header]
    for peer in rows:
        table.append(
            (
                peer.hexhash,
                peer.display_name,
                str(peer.hops),
                _ago(peer.age(timestamp)),
                peer.lxmf_hash or "—",
            )
        )
    widths = [max(len(row[i]) for row in table) for i in range(len(header))]
    for row in table:
        print("  ".join(value.ljust(widths[i]) for i, value in enumerate(row)))


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
    empty_message: str = "No posts yet. Try `tau tan post \"hello\"` or `tau tan sync --explore`.",
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
        print(f"[{when}] {name} ({author}) {short_hash(str(post.get('hash', '')))}")
        if post.get("kind") == "reply":
            parent = post.get("reply_to")
            if isinstance(parent, dict):
                if parent.get("deleted"):
                    print("  in reply to a deleted post")
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
        for line in str(post.get("text", "")).splitlines() or [""]:
            print(f"  {line}")
        likes = int(post.get("likes") or 0)
        if likes or post.get("liked"):
            suffix = " (you liked this)" if post.get("liked") else ""
            print(f"  ♥ {likes}{suffix}")


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
        )
        for line in str(post.get("text", "")).splitlines() or [""]:
            print(f"{indent}  {line}")
        likes = int(post.get("likes") or 0)
        if likes or post.get("liked"):
            suffix = " (you liked this)" if post.get("liked") else ""
            print(f"{indent}  ♥ {likes}{suffix}")


def _tan_identity(config: TauConfig) -> Any:
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    return identity


def _publish_local_post(
    node: TauNode, *, text: str,
    reply_to: Optional[str] = None,
    to: Optional[List[str]] = None, ttl: int = MAX_GOSSIP_TTL,
    timeout: float = 15.0,
) -> Dict[str, Any]:
    # Use a separate app so per-command timeouts do not mutate the serving app.
    publisher = TanApp(node.config, node.identity, name=node.name,
                       request_timeout=float(timeout))
    publisher.node = node
    targets = [node.resolve_peer(target) for target in to] if to else None
    post = publisher.create_post(text, reply_to=reply_to)
    published = publisher.publish(post, targets=targets,
                                  ttl=max(0, min(int(ttl), MAX_GOSSIP_TTL)))
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
                config.data_dir, text=text, reply_to=reply_to,
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
                    node, text=text, reply_to=reply_to,
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
                ttl=max(0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)),
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
                ttl=max(0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)),
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


def cmd_tan_timeline(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        if getattr(args, "author", None):
            author, _ = resolve_author(
                conn, args.author, registry=PeerRegistry(config.peers_path)
            )
            posts = list(
                reversed(
                    feed(conn, author=author, limit=args.limit, posts_only=True)
                )
            )
            posts = annotate_replies(
                conn, annotate_reactions(conn, posts, viewer=viewer)
            )
        elif getattr(args, "all", False):
            posts = timeline(conn, include_all=True, limit=args.limit, viewer=viewer)
        else:
            authors = [record["author"] for record in following(conn)]
            if viewer is not None:
                authors.append(viewer)
            posts = timeline(conn, authors, limit=args.limit, viewer=viewer)
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


def cmd_tan_thread(args: argparse.Namespace) -> int:
    config = _make_config(args)
    json_output = _json_mode(args)
    conn = init_db(config.social_db_path)
    try:
        identity = load_identity(config.identity_path)
        viewer = destination_hash(identity).hex() if identity is not None else None
        posts = thread(conn, args.ref, viewer=viewer)
    finally:
        conn.close()
    try:
        display_ruleset = load_ruleset(config.tan_rules_path)
    except RulesError:
        display_ruleset = Ruleset()
    posts = apply_display_rules(posts, display_ruleset)
    _print_tan_thread(posts, json_output=json_output)
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
        print(f"  followed authors admitted: {'yes' if data['allow_followed'] else 'no'}")
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


def cmd_serve(args: argparse.Namespace) -> int:
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
        from .webapi import WebBridge, WebBridgeError, check_web_console_bind

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
            # as an inbound message so the file shows up in its feed
            # (a download link can follow later).
            bridge.record_inbound(
                {
                    "destination": record.get("destination"),
                    "from": record.get("from"),
                    "text": (
                        f"Received file: {record.get('name')} "
                        f"({record.get('size')} bytes, "
                        f"sha256 {short_hash(str(record.get('sha256') or ''), 12)})"
                    ),
                    "sent_at": record.get("sent_at"),
                    "received_at": record.get("received_at"),
                }
            )

        node.on_peer = on_peer
        node.on_message = on_message
        node.on_file = on_file
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
    node.start(
        announce=not getattr(args, "no_announce", False), exclusive=True
    )

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
                    log.debug("Could not forward Tan post to web console", exc_info=True)

        social_app = TanApp(
            config,
            node.identity,
            name=config.name,
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

    social_interval = max(
        0.0, float(getattr(args, "social_sync_interval", 0.0) or 0.0)
    )
    if social_app is not None and social_interval > 0:
        def social_sync_loop() -> None:
            while not stop.wait(social_interval):
                try:
                    summary = social_app.sync(
                        node, limit=DEFAULT_FEED_LIMIT, explore=True
                    )
                    if summary.get("stored"):
                        log.info(
                            "Tan sync stored %s new post(s)", summary["stored"]
                        )
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

    info = node.info()
    if _json_mode(args):
        _emit_event(args, "ready", node=info)
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
        print(f"  data dir:    {config.data_dir}")
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
    if _json_mode(args):
        _emit(
            {"peers": [peer.to_dict() for peer in peers], "count": len(peers)},
            True,
        )
    else:
        _print_peer_table(peers)
    return _EXIT_OK


def cmd_peers_exchange(args: argparse.Namespace, config: TauConfig) -> int:
    """Exchange peer lists with known peers and report what was learned."""
    node = TauNode(config, name=config.name)
    node.start()
    try:
        target = getattr(args, "to", None)
        targets = (
            [node.resolve_peer(target)] if target else node.registry.all()
        )
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
            results.append({"peer": peer.hexhash, "name": peer.display_name, "ok": True, **summary})
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
        "data_dir": str(config.data_dir),
        "rns_config": str(config.rns_config_file),
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"name:        {config.name}")
        print(f"destination: {destination}")
        print(f"identity:    {identity.hexhash}")
        print(f"lxmf:        {payload['lxmf']}")
        print(f"data dir:    {config.data_dir}")
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
    if identity is None:  # pragma: no cover - load_identity returns None only when missing
        raise NodeError(f"no identity backup at {source}")
    config.prepare()
    # Hold the instance lock across the whole replacement: a node that
    # starts after the probe but before the write would load the old
    # identity and then serve with a different identity than the one on
    # disk.  The identity-creation lock serialises the check-and-write
    # against a concurrent `tau init` on the same directory.  Lock order
    # (instance, then identity) matches `TauNode.start`, which takes the
    # instance lock before loading or creating its identity.
    # Note the instance lock is POSIX-only (no-op without fcntl), the
    # same limitation `tau serve` itself accepts.
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    try:
        with _identity_lock(config.identity_path):
            if config.identity_path.exists() and not getattr(args, "force", False):
                raise NodeError(
                    f"an identity already exists at {config.identity_path}; "
                    "pass --force to replace it"
                )
            save_identity(identity, config.identity_path)
    finally:
        lock.release()
    payload = {
        "destination": destination_hash(identity).hex(),
        "identity": identity.hexhash,
        "data_dir": str(config.data_dir),
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"identity:    {payload['identity']}")
        print(f"destination: {payload['destination']}")
        print(f"data dir:    {payload['data_dir']}")
    return _EXIT_OK


def cmd_send(args: argparse.Namespace) -> int:
    text = _read_message(args)
    config = _make_config(args)
    node = TauNode(config, name=config.name)
    node.start()
    try:
        response = node.send_message(
            args.to,
            text,
            timeout=float(getattr(args, "timeout", 20.0)),
            discover_timeout=float(getattr(args, "discover_timeout", 10.0)),
            queue=not getattr(args, "no_queue", False),
        )
        if response.get("queued"):
            # One-shot send: give reachable peers a brief chance to take a
            # sealed copy (or the message itself) before this process exits.
            node.flush_outbox(timeout=3.0, max_peers=5)
            if node.mailbox.get_outbox(response["id"]) is None:
                response = {
                    "ok": True,
                    "delivered": True,
                    "id": response["id"],
                    "to": response["to"],
                }
    finally:
        node.stop()

    if _json_mode(args):
        _emit(response, True)
    elif response.get("queued"):
        print(f"queued for {args.to} — they are unreachable right now")
        print(f"  id:      {response['id']}")
        print("  it will be delivered automatically when they are heard from;")
        print("  check `tau outbox` for the queue, or just leave a node serving")
    elif response.get("delivered"):
        print(f"delivered to {args.to}")
    else:
        print(f"delivered to {args.to}")
        echo = response.get("echo")
        if echo is not None:
            print(f"echo: {echo}")
        rtt = response.get("_rtt")
        if isinstance(rtt, (int, float)):
            print(f"round trip: {rtt * 1000:.0f} ms")
    return _EXIT_OK


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
        return payload

    if _json_mode(args):
        _emit(
            {
                "outbox": [describe(entry) for entry in entries],
                "count": len(entries),
                "held_for_others": counts["held"],
            },
            True,
        )
    else:
        if not entries:
            print("Outbox is empty — nothing waiting on offline peers.")
        else:
            now = time.time()
            header = ("ID", "TO", "NAME", "AGE", "TRIES", "NEXT TRY", "EXPIRES IN")
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
    return _EXIT_OK


def _is_hex32(value: str) -> bool:
    return len(value) == 32 and all(
        char in "0123456789abcdefABCDEF" for char in value
    )


def _format_message_time(timestamp: float) -> str:
    """One inbox entry's received time; never fails the whole listing."""
    try:
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")
    except (OverflowError, OSError, ValueError):
        return "unknown time"


def _resolve_inbox_peer(
    inbox: Inbox, registry: PeerRegistry, target: str
) -> str:
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
        senders = {
            peer.hexhash for peer in peers if peer.hexhash.startswith(prefix)
        }
        senders |= {
            str(entry.get("destination") or "").casefold()
            for entry in inbox.list_messages(limit=None)
            if str(entry.get("destination") or "")
            .casefold()
            .startswith(prefix)
        }
        if len(senders) == 1:
            return senders.pop()
        if len(senders) > 1:
            raise NodeError(
                f"{len(senders)} senders match the hash prefix {target!r}"
            )
    raise NodeError(f"unknown peer: {target!r}")


def cmd_messages(args: argparse.Namespace) -> int:
    config = _make_config(args)
    inbox = Inbox(config.inbox_path)
    registry = PeerRegistry(config.peers_path)
    peer_hash = None
    if getattr(args, "peer", None):
        peer_hash = _resolve_inbox_peer(inbox, registry, args.peer)
    limit = getattr(args, "limit", 30)
    if limit is not None and limit < 0:
        raise NodeError("--limit must not be negative")
    entries = inbox.list_messages(limit=limit, peer=peer_hash)
    if _json_mode(args):
        _emit({"messages": entries, "count": len(entries)}, True)
        return _EXIT_OK
    if not entries:
        print("No messages received yet.")
        return _EXIT_OK
    for entry in entries:
        peer = registry.get(entry.get("destination") or "")
        name = peer.display_name if peer is not None else entry.get("from") or "unknown"
        sender = short_hash(str(entry.get("destination") or ""))
        when = _format_message_time(float(entry.get("received_at", 0)))
        print(f"[{when}] {name} ({sender})")
        if entry.get("kind") == "file":
            print(
                f"  file: {entry.get('name')} "
                f"({entry.get('size')} bytes, "
                f"sha256 {short_hash(str(entry.get('sha256') or ''), 12)})"
            )
            print(f"  saved to {entry.get('path')}")
            continue
        for line in str(entry.get("text", "")).splitlines() or [""]:
            print(f"  {line}")
    return _EXIT_OK
def _safe_id_prefix(ref: str, length: int = 8) -> str:
    """First *length* characters of *ref* with control characters dropped.

    Held ids arrive from remote senders and may contain terminal escapes
    or newlines; never print them verbatim. JSON output keeps the raw id.
    """
    return "".join(char for char in ref[:length] if char.isprintable())


def cmd_outbox_held(args: argparse.Namespace) -> int:
    config = _make_config(args)
    mailbox = Mailbox(config.mailbox_path)
    registry = PeerRegistry(config.peers_path)
    envelopes = mailbox.held_list()

    def describe(envelope: Any) -> Dict[str, Any]:
        peer = registry.get(envelope.to)
        return {
            "id": envelope.id,
            "to": envelope.to_hex,
            "to_name": peer.display_name if peer is not None else "",
            "stored_at": envelope.stored_at,
            "expires_at": envelope.expires_at,
            "blob_bytes": len(envelope.blob),
        }

    if _json_mode(args):
        _emit(
            {"held": [describe(e) for e in envelopes], "count": len(envelopes)},
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
    node = TauNode(
        config, name=config.name, on_delivered=delivered_ids.add
    )
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
            print(f"still queued: {label} (attempt {fresh.attempts}, next try {next_text})")
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
    """Resolve a peer name, hash or prefix to a destination hash."""
    peer = registry.resolve(value)
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

    settings = {"accept_files": accept_files_enabled(config.data_dir)}
    if json_output:
        _emit(settings, True)
    else:
        state = "on" if settings["accept_files"] else "off"
        print(f"Settings ({config.data_dir / 'node.json'}):")
        print(f"  accept files: {state}")
        print("  change with: tau config accept-files on|off")
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
            "name": peer.display_name if peer is not None else "",
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
