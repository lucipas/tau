"""Local HTTP bridge and web console for a running tau node.

The bridge exposes a small JSON API over loopback so the browser (or any local
script) can read what arrives from the mesh and send messages back:

    GET  /api/status       node identity, interfaces and counters
    GET  /api/peers        peers from the local registry
    GET  /api/messages     recent inbound/outbound messages
    POST /api/messages     send ``{"to": "...", "text": "..."}``
    POST /api/ping         measure reachability of a peer
    POST /api/announce     announce this node immediately
    GET  /api/events       server-sent events for live messages and peers

If a static build of the Next.js console exists (``web/out``), it is served
from the same origin.  Everything binds to loopback by default because anyone
who can reach this API can send messages as this node.
"""

from __future__ import annotations

import json
import logging
import mimetypes
import queue
import threading
import time
from collections import deque
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, unquote, urlsplit

from . import __version__
from .node import NodeError, TauNode
from .peers import Peer, PeerError
from .protocol import MAX_MESSAGE_CHARS, ProtocolError, sanitize_name, short_hash

log = logging.getLogger(__name__)

DEFAULT_WEB_PORT = 8787
DEFAULT_WEB_HOST = "127.0.0.1"

MAX_BODY_BYTES = 32 * 1024
MAX_MESSAGE_LOG = 500
MAX_EVENT_LOG = 500
MAX_SUBSCRIBERS = 32
SUBSCRIBER_QUEUE_SIZE = 256
SSE_HEARTBEAT_SECONDS = 15.0

_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}
_LOCAL_ORIGIN_HOSTS = {"localhost", "127.0.0.1", "::1"}


class WebBridgeError(RuntimeError):
    """Raised when the web bridge cannot start or process a request."""


class RequestError(WebBridgeError):
    """Raised for malformed or oversized HTTP requests."""


def is_loopback_host(host: str) -> bool:
    return host.strip().lower() in _LOOPBACK_HOSTS


def parse_bind(value: str) -> Tuple[str, int]:
    """Parse ``host:port`` or a bare port into an address tuple."""
    from .config import parse_host_port

    return parse_host_port(value, default_host=DEFAULT_WEB_HOST)


def _is_local_origin(origin: str) -> bool:
    try:
        parsed = urlsplit(origin)
    except ValueError:
        return False
    if parsed.scheme not in ("http", "https"):
        return False
    host = (parsed.hostname or "").lower()
    return host in _LOCAL_ORIGIN_HOSTS or host.endswith(".localhost")


class EventBus:
    """Fan out events to server-sent-event subscribers with replay."""

    def __init__(self, limit: int = MAX_EVENT_LOG) -> None:
        self._lock = threading.Lock()
        self._events: Deque[Dict[str, Any]] = deque(maxlen=limit)
        self._subscribers: Set[queue.Queue] = set()
        self._next_id = 1
        self._closed = False

    def publish(self, event: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            if self._closed:
                return event
            payload = dict(event)
            payload["id"] = self._next_id
            self._next_id += 1
            self._events.append(payload)
            subscribers = list(self._subscribers)
        for subscriber in subscribers:
            try:
                subscriber.put_nowait(payload)
            except queue.Full:
                log.debug("Dropping event for a slow subscriber")
        return payload

    def replay(self, since: int = 0) -> List[Dict[str, Any]]:
        with self._lock:
            return [event for event in self._events if event["id"] > since]

    def subscribe(self) -> queue.Queue:
        subscriber: queue.Queue = queue.Queue(maxsize=SUBSCRIBER_QUEUE_SIZE)
        with self._lock:
            if len(self._subscribers) >= MAX_SUBSCRIBERS:
                raise WebBridgeError("too many event subscribers")
            self._subscribers.add(subscriber)
        return subscriber

    def unsubscribe(self, subscriber: queue.Queue) -> None:
        with self._lock:
            self._subscribers.discard(subscriber)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            subscribers = list(self._subscribers)
            self._subscribers.clear()
        for subscriber in subscribers:
            try:
                subscriber.put_nowait(None)
            except queue.Full:
                pass


class WebBridge:
    """Expose a :class:`TauNode` over a loopback HTTP API."""

    def __init__(
        self,
        node: TauNode,
        *,
        host: str = DEFAULT_WEB_HOST,
        port: int = DEFAULT_WEB_PORT,
        web_dir: Optional[Path] = None,
        allow_remote: bool = False,
    ) -> None:
        if not allow_remote and not is_loopback_host(host):
            raise WebBridgeError(
                "refusing to expose the tau console on a non-loopback address "
                "without --http-allow-remote"
            )
        self.node = node
        self.host = host
        self.port = int(port)
        self.web_dir = Path(web_dir).expanduser().resolve() if web_dir else None
        self.allow_remote = bool(allow_remote)

        self.bus = EventBus()
        self._lock = threading.Lock()
        self._messages: Deque[Dict[str, Any]] = deque(maxlen=MAX_MESSAGE_LOG)
        self._next_message_id = 1
        self._server: Optional[_WebServer] = None
        self._thread: Optional[threading.Thread] = None
        self._closing = threading.Event()
        self._started_at = time.time()

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> str:
        if self._server is not None:
            raise WebBridgeError("web bridge is already running")
        self._closing.clear()
        server = _WebServer((self.host, self.port), _WebHandler)
        server.bridge = self
        server.daemon_threads = True
        self._server = server
        self.port = int(server.server_address[1])
        self._thread = threading.Thread(
            target=server.serve_forever,
            kwargs={"poll_interval": 0.25},
            name="tau-web",
            daemon=True,
        )
        self._thread.start()
        log.info("tau web console listening on %s", self.url)
        return self.url

    def stop(self) -> None:
        self._closing.set()
        self.bus.close()
        server = self._server
        self._server = None
        if server is not None:
            server.shutdown()
            server.server_close()
        thread = self._thread
        self._thread = None
        if thread is not None:
            thread.join(timeout=3.0)

    @property
    def url(self) -> str:
        host = self.host
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"http://{host}:{self.port}/"

    @property
    def closing(self) -> bool:
        return self._closing.is_set()

    # -- node callbacks ----------------------------------------------------

    def record_peer(self, peer: Peer) -> None:
        self.bus.publish({"type": "peer", "peer": peer.public_dict()})

    def record_inbound(self, record: Dict[str, Any]) -> Dict[str, Any]:
        peer_hash = str(record.get("destination") or "")
        peer_name = str(record.get("from") or "")
        if not peer_name or peer_name == "unknown":
            peer_name = short_hash(peer_hash) if peer_hash else "unknown"
        message = self._append_message(
            direction="in",
            peer=peer_hash or None,
            peer_name=sanitize_name(peer_name, fallback="unknown"),
            text=str(record.get("text") or ""),
            sent_at=_as_float(record.get("sent_at")),
            received_at=_as_float(record.get("received_at"), default=time.time()),
            status="received",
        )
        self.bus.publish({"type": "message", "message": message})
        return message

    def send_message(
        self,
        target: str,
        text: str,
        *,
        timeout: float = 20.0,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        if not isinstance(target, str) or not target.strip():
            raise ValueError("target peer is required")
        if not isinstance(text, str) or not text.strip():
            raise ValueError("message text is required")
        if len(text) > MAX_MESSAGE_CHARS:
            raise ValueError(f"message is longer than {MAX_MESSAGE_CHARS} characters")

        peer = self.node.resolve_peer(target.strip(), timeout=min(timeout, 10.0))
        response = self.node.send_message(peer, text, timeout=timeout)
        message = self._append_message(
            direction="out",
            peer=peer.hexhash,
            peer_name=peer.display_name,
            text=text,
            sent_at=time.time(),
            received_at=time.time(),
            status="delivered",
            rtt=_as_float(response.get("_rtt")),
        )
        self.bus.publish({"type": "message", "message": message})
        return message, response

    def ping(self, target: str, *, timeout: float = 15.0) -> Dict[str, Any]:
        peer = self.node.resolve_peer(target.strip(), timeout=min(timeout, 10.0))
        response = self.node.ping(peer, timeout=timeout)
        rtt = response.get("_rtt")
        return {
            "ok": True,
            "peer": peer.public_dict(),
            "server": response.get("server"),
            "rtt_ms": round(float(rtt) * 1000.0, 2)
            if isinstance(rtt, (int, float))
            else None,
        }

    def announce(self) -> None:
        self.node.announce()
        self.bus.publish({"type": "status", "action": "announce"})

    # -- snapshots ---------------------------------------------------------

    def peers(self) -> List[Dict[str, Any]]:
        now = time.time()
        try:
            return [peer.public_dict(now) for peer in self.node.registry.all()]
        except Exception:  # pragma: no cover - defensive
            log.debug("Could not read peer registry", exc_info=True)
            return []

    def messages(self, limit: int = 200) -> List[Dict[str, Any]]:
        with self._lock:
            items = list(self._messages)
        if limit >= 0:
            items = items[-limit:]
        return items

    def counts(self) -> Dict[str, int]:
        with self._lock:
            message_count = len(self._messages)
        return {"messages": message_count, "peers": len(self.peers())}

    def snapshot(self) -> Dict[str, Any]:
        return {
            "node": self.node.info(),
            "peers": self.peers(),
            "messages": self.messages(limit=200),
            "counts": self.counts(),
            "bridge": {
                "version": __version__,
                "web": self.web_dir is not None,
                "url": self.url,
            },
        }

    def _append_message(
        self,
        *,
        direction: str,
        peer: Optional[str],
        peer_name: str,
        text: str,
        sent_at: float,
        received_at: float,
        status: str,
        rtt: Optional[float] = None,
    ) -> Dict[str, Any]:
        with self._lock:
            message = {
                "id": self._next_message_id,
                "direction": direction,
                "peer": peer,
                "peer_name": peer_name,
                "text": text,
                "sent_at": sent_at,
                "received_at": received_at,
                "status": status,
            }
            if rtt is not None:
                message["rtt_ms"] = round(rtt * 1000.0, 2)
            self._next_message_id += 1
            self._messages.append(message)
        return message

    # -- static files ------------------------------------------------------

    def resolve_static(self, request_path: str) -> Optional[Path]:
        """Return a file inside ``web_dir`` for a URL path, or ``None``."""
        if self.web_dir is None:
            return None
        relative = unquote(request_path).lstrip("/")
        if not relative:
            relative = "index.html"
        base = self.web_dir
        try:
            candidate = (base / relative).resolve()
        except (OSError, ValueError):
            return None
        if candidate != base and base not in candidate.parents:
            return None
        if candidate.is_dir():
            candidate = candidate / "index.html"
        if candidate.is_file() and not _has_hidden_part(candidate, base):
            return candidate
        # Static export uses trailing slashes; fall back to the SPA entry point.
        if "." not in Path(relative).name:
            index = base / "index.html"
            if index.is_file():
                return index
        return None


def _has_hidden_part(path: Path, base: Path) -> bool:
    try:
        parts = path.relative_to(base).parts
    except ValueError:
        return True
    return any(part.startswith(".") for part in parts)


def _as_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    return default


class _WebServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    bridge: WebBridge


class _WebHandler(BaseHTTPRequestHandler):
    server: _WebServer
    server_version = f"tau-web/{__version__}"
    protocol_version = "HTTP/1.1"

    # -- plumbing ----------------------------------------------------------

    @property
    def bridge(self) -> WebBridge:
        return self.server.bridge

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        log.debug("web %s - %s", self.address_string(), fmt % args)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self._cors_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_HEAD(self) -> None:  # noqa: N802
        self._handle(head_only=True)

    def do_GET(self) -> None:  # noqa: N802
        self._handle()

    def do_POST(self) -> None:  # noqa: N802
        self._handle()

    def _handle(self, *, head_only: bool = False) -> None:
        parsed = urlsplit(self.path)
        path = parsed.path
        try:
            if path.startswith("/api/"):
                if self.command == "POST" and not self._post_is_allowed():
                    self._send_error(
                        HTTPStatus.FORBIDDEN,
                        "cross-origin requests are not allowed",
                        head_only,
                    )
                    return
                self._handle_api(path, parse_qs(parsed.query), head_only)
            elif path == "/healthz":
                self._send_json({"ok": True, "version": __version__}, head_only)
            else:
                self._serve_static(path, head_only)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
        except RequestError as exc:
            self._send_error(HTTPStatus.BAD_REQUEST, str(exc), head_only)
        except WebBridgeError as exc:
            self._send_error(HTTPStatus.TOO_MANY_REQUESTS, str(exc), head_only)
        except Exception:  # pragma: no cover - defensive
            log.exception("Unhandled web request error")
            self._send_error(
                HTTPStatus.INTERNAL_SERVER_ERROR, "internal error", head_only
            )

    # -- API ---------------------------------------------------------------

    def _handle_api(
        self, path: str, query: Dict[str, List[str]], head_only: bool
    ) -> None:
        if path == "/api/events" and self.command == "GET" and not head_only:
            self._serve_events(query)
            return

        if self.command == "GET":
            if path == "/api/status":
                self._send_json(self.bridge.snapshot(), head_only)
                return
            if path == "/api/peers":
                peers = self.bridge.peers()
                self._send_json({"peers": peers, "count": len(peers)}, head_only)
                return
            if path == "/api/messages":
                limit = _int_param(query, "limit", default=200, minimum=1, maximum=500)
                self._send_json({"messages": self.bridge.messages(limit)}, head_only)
                return
            self._send_json({"error": {"message": "not found"}}, head_only, 404)
            return

        if self.command == "POST":
            body = self._read_body()
            if path == "/api/messages":
                self._handle_send(body, head_only)
                return
            if path == "/api/ping":
                self._handle_ping(body, head_only)
                return
            if path == "/api/announce":
                self.bridge.announce()
                self._send_json({"ok": True}, head_only)
                return
            self._send_json({"error": {"message": "not found"}}, head_only, 404)
            return

        self._send_json({"error": {"message": "method not allowed"}}, head_only, 405)

    def _handle_send(self, body: Dict[str, Any], head_only: bool) -> None:
        target = body.get("to")
        text = body.get("text")
        timeout = body.get("timeout", 20.0)
        if not isinstance(target, str) or not target.strip():
            self._send_json(
                {"error": {"message": "field 'to' is required"}}, head_only, 400
            )
            return
        if not isinstance(text, str):
            self._send_json(
                {"error": {"message": "field 'text' is required"}}, head_only, 400
            )
            return
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
            timeout = 20.0
        timeout = max(1.0, min(float(timeout), 120.0))
        try:
            message, response = self.bridge.send_message(target, text, timeout=timeout)
        except (ValueError, ProtocolError) as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 400)
            return
        except PeerError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 404)
            return
        except NodeError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 502)
            return
        self._send_json(
            {"ok": True, "message": message, "response": response}, head_only
        )

    def _handle_ping(self, body: Dict[str, Any], head_only: bool) -> None:
        target = body.get("to")
        timeout = body.get("timeout", 15.0)
        if not isinstance(target, str) or not target.strip():
            self._send_json(
                {"error": {"message": "field 'to' is required"}}, head_only, 400
            )
            return
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
            timeout = 15.0
        timeout = max(1.0, min(float(timeout), 120.0))
        try:
            result = self.bridge.ping(target, timeout=timeout)
        except PeerError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 404)
            return
        except NodeError as exc:
            self._send_json({"error": {"message": str(exc)}}, head_only, 502)
            return
        self._send_json(result, head_only)

    # -- server-sent events ------------------------------------------------

    def _serve_events(self, query: Dict[str, List[str]]) -> None:
        since = _int_param(query, "since", default=0, minimum=0, maximum=2**31 - 1)
        replay = "since" in query and since > 0
        try:
            subscriber = self.bridge.bus.subscribe()
        except WebBridgeError as exc:
            self._send_error(HTTPStatus.TOO_MANY_REQUESTS, str(exc))
            return

        self.close_connection = True
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.send_header("X-Accel-Buffering", "no")
        self._cors_headers()
        self.end_headers()

        try:
            self._write_event("snapshot", {"snapshot": self.bridge.snapshot()})
            if replay:
                for event in self.bridge.bus.replay(since):
                    self._write_event(
                        str(event.get("type", "message")),
                        {key: value for key, value in event.items() if key != "type"},
                        event.get("id"),
                    )
            while not self.bridge.closing:
                try:
                    event = subscriber.get(timeout=SSE_HEARTBEAT_SECONDS)
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    continue
                if event is None:
                    break
                self._write_event(
                    str(event.get("type", "message")),
                    {key: value for key, value in event.items() if key != "type"},
                    event.get("id"),
                )
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            self.bridge.bus.unsubscribe(subscriber)

    def _write_event(
        self,
        name: str,
        payload: Dict[str, Any],
        event_id: Optional[int] = None,
    ) -> None:
        if event_id is not None:
            self.wfile.write(f"id: {event_id}\n".encode("utf-8"))
        self.wfile.write(f"event: {name}\n".encode("utf-8"))
        data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
        self.wfile.flush()

    # -- static files ------------------------------------------------------

    def _serve_static(self, path: str, head_only: bool) -> None:
        resolved = self.bridge.resolve_static(path)
        if resolved is None:
            if path in ("/", "/index.html"):
                self._send_json(
                    {
                        "name": "tau",
                        "version": __version__,
                        "message": (
                            "web console not built; run `make web-build` or use "
                            "the API under /api/"
                        ),
                    },
                    head_only,
                    404,
                )
            else:
                self._send_error(HTTPStatus.NOT_FOUND, "not found", head_only)
            return

        try:
            data = resolved.read_bytes()
        except OSError:
            self._send_error(HTTPStatus.NOT_FOUND, "not found", head_only)
            return

        content_type = mimetypes.guess_type(str(resolved))[0] or (
            "application/octet-stream"
        )
        if resolved.suffix in (".js", ".mjs"):
            content_type = "text/javascript; charset=utf-8"
        elif resolved.suffix == ".css":
            content_type = "text/css; charset=utf-8"
        elif resolved.suffix == ".html":
            content_type = "text/html; charset=utf-8"
        elif resolved.suffix == ".svg":
            content_type = "image/svg+xml"

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self._cors_headers()
        self.send_header("X-Content-Type-Options", "nosniff")
        if "/_next/static/" in path or path.startswith("/assets/"):
            self.send_header("Cache-Control", "public, max-age=31536000, immutable")
        else:
            self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if not head_only:
            self.wfile.write(data)

    # -- helpers -----------------------------------------------------------

    def _post_is_allowed(self) -> bool:
        origin = self.headers.get("Origin")
        return not origin or _is_local_origin(origin)

    def _read_body(self) -> Dict[str, Any]:
        content_type = (
            (self.headers.get("Content-Type") or "").split(";", 1)[0].strip().lower()
        )
        if content_type != "application/json":
            raise RequestError("Content-Type must be application/json")
        raw_length = self.headers.get("Content-Length")
        try:
            length = int(raw_length or 0)
        except ValueError:
            raise RequestError("invalid Content-Length")
        if length > MAX_BODY_BYTES:
            raise RequestError("request body is too large")
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise RequestError("request body must be valid JSON")
        if not isinstance(payload, dict):
            raise RequestError("request body must be a JSON object")
        return payload

    def _cors_headers(self) -> None:
        origin = self.headers.get("Origin")
        if origin and _is_local_origin(origin):
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Access-Control-Max-Age", "600")

    def _send_json(
        self,
        payload: Dict[str, Any],
        head_only: bool = False,
        status: int = HTTPStatus.OK,
    ) -> None:
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )
        if status >= 400:
            # The request body may not have been consumed; close instead of
            # risking keep-alive desynchronisation.
            self.close_connection = True
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        if status >= 400:
            self.send_header("Connection", "close")
        self._cors_headers()
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if not head_only:
            self.wfile.write(body)

    def _send_error(self, status: int, message: str, head_only: bool = False) -> None:
        self._send_json({"error": {"message": message}}, head_only, status)


def _int_param(
    query: Dict[str, List[str]],
    name: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    values = query.get(name)
    if not values:
        return default
    try:
        value = int(values[0])
    except (TypeError, ValueError):
        return default
    return max(minimum, min(value, maximum))
