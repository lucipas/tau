"""Command line interface for tau."""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from . import __version__
from .config import TauConfig, write_node_metadata, write_rns_config
from .identity import (
    IdentityError,
    destination_hash,
    load_identity,
    load_or_create_identity,
)
from .mailbox import Mailbox
from .node import NodeError, TauNode
from .peers import Peer, PeerError, PeerRegistry
from .protocol import DEFAULT_TCP_PORT as PROTOCOL_TCP_PORT
from .protocol import PROTOCOL_VERSION, ProtocolError, short_hash
from .tangents.rules import RulesError, Ruleset, load_ruleset, save_ruleset
from .tangents.tan import (
    DEFAULT_FEED_LIMIT,
    MAX_GOSSIP_TTL,
    TanApp,
    annotate_reactions,
    apply_display_rules,
    author_stats,
    feed,
    follow,
    following,
    init_db,
    resolve_author,
    resolve_delete_target,
    resolve_post,
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
    peers.set_defaults(func=cmd_peers)

    identity = subparsers.add_parser(
        "id", parents=[common], help="show this node's name and destination"
    )
    identity.set_defaults(func=cmd_id)

    outbox = subparsers.add_parser(
        "outbox",
        parents=[common],
        help="list messages queued for offline peers (store-and-forward)",
    )
    outbox.set_defaults(func=cmd_outbox)

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
    header = ("DESTINATION", "NAME", "HOPS", "LAST SEEN")
    table: List[tuple] = [header]
    for peer in rows:
        table.append(
            (
                peer.hexhash,
                peer.display_name,
                str(peer.hops),
                _ago(peer.age(timestamp)),
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

    return on_peer, on_message


def _default_web_dir() -> Optional[Path]:
    override = os.environ.get("TAU_WEB_DIR")
    if override:
        return Path(override).expanduser()
    candidate = Path(__file__).resolve().parent.parent / "web" / "out"
    return candidate if candidate.exists() else None


def _print_tan_posts(posts: Sequence[Dict[str, Any]], *, json_output: bool) -> None:
    if json_output:
        _emit({"posts": list(posts), "count": len(posts)}, True)
        return
    if not posts:
        print("No posts yet. Try `tau tan post \"hello\"` or `tau tan sync --explore`.")
        return
    for post in posts:
        name = post.get("name") or "anonymous"
        author = short_hash(str(post.get("author", "")))
        when = datetime.fromtimestamp(float(post.get("created_at", 0))).strftime(
            "%Y-%m-%d %H:%M"
        )
        print(f"[{when}] {name} ({author}) {short_hash(str(post.get('hash', '')))}")
        for line in str(post.get("text", "")).splitlines() or [""]:
            print(f"  {line}")
        likes = int(post.get("likes") or 0)
        if likes or post.get("liked"):
            suffix = " (you liked this)" if post.get("liked") else ""
            print(f"  ♥ {likes}{suffix}")


def _tan_identity(config: TauConfig) -> Any:
    identity = load_identity(config.identity_path)
    if identity is None:
        raise NodeError(f"no identity at {config.identity_path}; run `tau init` first")
    return identity


def cmd_tan_post(args: argparse.Namespace) -> int:
    text = _read_message(args)
    config = _make_config(args)
    json_output = _json_mode(args)

    if getattr(args, "no_publish", False):
        app = TanApp(config, _tan_identity(config), name=config.name)
        post = app.create_post(text)
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
            post = app.create_post(text)
            published = app.publish(
                post,
                targets=targets,
                ttl=max(0, min(int(getattr(args, "ttl", MAX_GOSSIP_TTL)), MAX_GOSSIP_TTL)),
            )
        finally:
            node.stop()

    if json_output:
        _emit({"post": post, "published": published}, True)
    else:
        print(f"posted {short_hash(post['hash'])}")
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
            posts = annotate_reactions(conn, posts, viewer=viewer)
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

    console_peer, console_message = _node_callbacks(args)
    node = TauNode(
        config,
        name=config.name,
        on_message=console_message,
        on_peer=console_peer,
    )
    if web_settings is not None:
        from .webapi import WebBridge, WebBridgeError

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

        node.on_peer = on_peer
        node.on_message = on_message
    node.start(announce=not getattr(args, "no_announce", False))

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
        console_url = bridge.start()

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
        "data_dir": str(config.data_dir),
        "rns_config": str(config.rns_config_file),
    }
    if _json_mode(args):
        _emit(payload, True)
    else:
        print(f"name:        {config.name}")
        print(f"destination: {destination}")
        print(f"identity:    {identity.hexhash}")
        print(f"data dir:    {config.data_dir}")
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
            header = ("ID", "TO", "NAME", "AGE", "TRIES", "EXPIRES IN")
            rows: List[tuple] = [header]
            for entry in entries:
                peer = registry.get(entry.to)
                name = peer.display_name if peer is not None else entry.to_name
                rows.append(
                    (
                        entry.id[:8],
                        entry.to_hex,
                        name or "-",
                        _ago(now - entry.created_at),
                        str(entry.attempts),
                        _ago(entry.expires_at - now),
                    )
                )
            widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
            for row in rows:
                print("  ".join(value.ljust(widths[i]) for i, value in enumerate(row)))
            print(f"\n{len(entries)} message(s) waiting for offline peers.")
        if counts["held"]:
            print(
                f"Also holding {counts['held']} sealed message(s) for other peers."
            )
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
