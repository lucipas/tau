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
from .node import NodeError, TauNode
from .peers import Peer, PeerError, PeerRegistry
from .protocol import DEFAULT_TCP_PORT as PROTOCOL_TCP_PORT
from .protocol import PROTOCOL_VERSION, ProtocolError, short_hash

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
    if getattr(args, "message", None):
        return args.message
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

    console_url = None
    if bridge is not None:
        console_url = bridge.start()

    stop = threading.Event()
    previous_handlers = _install_stop_handlers(stop)

    info = node.info()
    if _json_mode(args):
        _emit_event(args, "ready", node=info)
        if console_url is not None:
            _emit_event(args, "web", url=console_url, api=f"{console_url}api/")
    else:
        print(f'tau node "{info["name"]}" ready', flush=True)
        print(f"  destination: {info['destination']}")
        print(f"  rns:         {info['rns_version']}")
        print(f"  data dir:    {config.data_dir}")
        if console_url is not None:
            print(f"  console:     {console_url}")
            print(f"  api:         {console_url}api/")
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
        )
    finally:
        node.stop()

    if _json_mode(args):
        _emit(response, True)
    else:
        print(f"delivered to {args.to}")
        echo = response.get("echo")
        if echo is not None:
            print(f"echo: {echo}")
        rtt = response.get("_rtt")
        if isinstance(rtt, (int, float)):
            print(f"round trip: {rtt * 1000:.0f} ms")
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
