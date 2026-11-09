"""Configuration and Reticulum interface file generation."""

from __future__ import annotations

import logging
import os
import socket
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, NamedTuple, Optional, Tuple

from .protocol import APP_NAME, DEFAULT_TCP_PORT, sanitize_name
from .util import atomic_write_text, ensure_dir, read_json

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore[assignment]

log = logging.getLogger(__name__)

APP_DIR_NAME = APP_NAME
NODE_FILE_NAME = "node.json"
IDENTITY_FILE_NAME = "identity"
PEERS_FILE_NAME = "peers.json"
MAILBOX_FILE_NAME = "mailbox.json"
INBOX_FILE_NAME = "inbox.json"
MESSAGES_FILE_NAME = "messages.json"
SOCIAL_DB_FILE_NAME = "tan.sqlite3"
TAN_RULES_FILE_NAME = "tan-rules.json"
ALLOWLIST_FILE_NAME = "allowlist.json"
FILES_DIR_NAME = "files"
BLOCKLIST_FILE_NAME = "blocklist.json"
NODE_LOCK_FILE_NAME = "node.lock"
RNS_DIR_NAME = "reticulum"
RNS_CONFIG_FILE_NAME = "config"


def default_data_dir() -> Path:
    override = os.environ.get("TAU_HOME")
    if override:
        return Path(override).expanduser()
    return Path.home() / f".{APP_DIR_NAME}"


def default_node_name() -> str:
    hostname = socket.gethostname().split(".")[0]
    return sanitize_name(hostname, fallback="tau-node")


@dataclass(frozen=True)
class TauConfig:
    data_dir: Path
    rns_config_dir: Path
    identity_path: Path
    peers_path: Path
    mailbox_path: Path
    inbox_path: Path
    node_file: Path
    social_db_path: Path
    tan_rules_path: Path
    allowlist_path: Path
    blocklist_path: Path
    name: str
    rns_loglevel: int = 2
    announce_interval: float = 300.0

    @classmethod
    def resolve(
        cls,
        data_dir: Optional[Path] = None,
        rns_config_dir: Optional[Path] = None,
        name: Optional[str] = None,
        rns_loglevel: int = 2,
        announce_interval: float = 300.0,
    ) -> "TauConfig":
        base = (
            Path(data_dir).expanduser() if data_dir is not None else default_data_dir()
        )
        rns_dir = (
            Path(rns_config_dir).expanduser()
            if rns_config_dir is not None
            else base / RNS_DIR_NAME
        )
        node_name = sanitize_name(
            name if name is not None else load_node_name(base) or default_node_name()
        )
        return cls(
            data_dir=base,
            rns_config_dir=rns_dir,
            identity_path=base / IDENTITY_FILE_NAME,
            peers_path=base / PEERS_FILE_NAME,
            mailbox_path=base / MAILBOX_FILE_NAME,
            inbox_path=base / INBOX_FILE_NAME,
            node_file=base / NODE_FILE_NAME,
            social_db_path=base / SOCIAL_DB_FILE_NAME,
            tan_rules_path=base / TAN_RULES_FILE_NAME,
            allowlist_path=base / ALLOWLIST_FILE_NAME,
            blocklist_path=base / BLOCKLIST_FILE_NAME,
            name=node_name,
            rns_loglevel=rns_loglevel,
            announce_interval=announce_interval,
        )

    @property
    def rns_config_file(self) -> Path:
        return self.rns_config_dir / RNS_CONFIG_FILE_NAME

    @property
    def files_dir(self) -> Path:
        """Directory received files land in, grouped by sender hash."""
        return self.data_dir / FILES_DIR_NAME

    @property
    def instance_lock_path(self) -> Path:
        return self.data_dir / NODE_LOCK_FILE_NAME

    def prepare(self) -> "TauConfig":
        ensure_dir(self.data_dir)
        ensure_dir(self.rns_config_dir)
        return self


def load_node_name(data_dir: Path) -> Optional[str]:
    raw = read_json(Path(data_dir) / NODE_FILE_NAME)
    if isinstance(raw, dict):
        name = raw.get("name")
        if isinstance(name, str) and name.strip():
            return name
    return None


@contextmanager
def _node_file_lock(path: Path) -> Iterator[None]:
    """Serialize read-modify-write updates of ``node.json`` across processes."""
    if fcntl is None:  # pragma: no cover - non-POSIX
        yield
        return
    lock_path = path.with_name(f".{path.name}.lock")
    with open(lock_path, "a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def send_read_receipts_enabled(data_dir: Path) -> bool:
    """True unless this node opted out of sending read receipts.

    A node that never sends receipts simply never does; senders treat
    the absence of a receipt as unknown, not unread-forever.  Only an
    explicit ``false`` in ``node.json`` disables sending — any other
    value (including a corrupt one) keeps the default on.
    """
    return load_node_settings(data_dir).get("send_read_receipts", True) is not False


def write_node_metadata(data_dir: Path, name: str) -> Path:
    from .util import atomic_write_json

    path = ensure_dir(Path(data_dir)) / NODE_FILE_NAME
    with _node_file_lock(path):
        # Preserve settings written since init (e.g. the file-acceptance
        # policy and the read-receipt opt-out): re-running `tau init`
        # must not silently reset them.
        raw = read_json(path, default=None)
        metadata = dict(raw) if isinstance(raw, dict) else {}
        metadata.update({"name": sanitize_name(name), "protocol": APP_NAME})
        atomic_write_json(
            path,
            metadata,
            mode=0o600,
        )
    return path


def load_node_settings(data_dir: Path) -> Dict[str, object]:
    """Read this node's own settings out of ``node.json``.

    Only keys the code knows how to interpret are returned, so a
    hand-edited file cannot smuggle an unknown setting into a lookup.
    """
    raw = read_json(Path(data_dir) / NODE_FILE_NAME, default=None)
    if not isinstance(raw, dict):
        return {}
    return {
        key: raw[key]
        for key in ("accept_files", "send_read_receipts")
        if key in raw
    }


def accept_files_enabled(data_dir: Path) -> bool:
    """True only when this node opted in to receiving files.

    Off by default: an unsolicited file is never written to disk
    unless the owner turned acceptance on, or the sender is on the
    DM allow-list (see :meth:`TauNode._handle_file`).  Only an
    explicit ``true`` in ``node.json`` enables it — any other value
    (including a corrupt one) keeps the default off.
    """
    return load_node_settings(data_dir).get("accept_files", False) is True


def set_node_setting(data_dir: Path, key: str, value: object) -> Dict[str, object]:
    """Persist one node setting in ``node.json``, keeping the others."""
    from .util import atomic_write_json

    path = ensure_dir(Path(data_dir)) / NODE_FILE_NAME
    with _node_file_lock(path):
        raw = read_json(path, default=None)
        settings = dict(raw) if isinstance(raw, dict) else {}
        settings[key] = value
        atomic_write_json(path, settings, mode=0o600)
    return settings


def parse_host_port(value: str, *, default_host: str = "127.0.0.1") -> Tuple[str, int]:
    """Parse ``host:port``, ``[v6]:port`` or a bare port."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("interface address must not be empty")
    text = value.strip()

    if text.startswith("["):
        host, _, remainder = text.partition("]")
        host = host[1:]
        if not remainder.startswith(":"):
            raise ValueError("expected [address]:port")
        port_text = remainder[1:]
    elif text.isdigit():
        host, port_text = default_host, text
    else:
        host, separator, port_text = text.rpartition(":")
        if not separator:
            raise ValueError("expected host:port")
        if not host:
            host = default_host

    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError(f"invalid port: {port_text!r}") from exc
    if not 1 <= port <= 65535:
        raise ValueError(f"port out of range: {port}")
    if not host:
        raise ValueError("host must not be empty")
    return host, port


def render_rns_config(
    *,
    tcp_listen: Optional[str] = None,
    tcp_connect: Optional[str] = None,
    auto_interface: bool = True,
    loglevel: int = 2,
) -> str:
    """Render a standalone Reticulum configuration file.

    ``share_instance`` is disabled so every tau node is fully independent.
    This keeps two nodes on one host (and in tests) deterministic, at the cost
    of not sharing a local Reticulum instance between processes.
    """
    lines = [
        "# Reticulum configuration generated by tau.",
        "# Edit freely, or regenerate with `tau init --init`.",
        "",
        "[reticulum]",
        "  enable_transport = No",
        "  share_instance = No",
        "  panic_on_interface_error = No",
        "",
        "[logging]",
        f"  loglevel = {int(loglevel)}",
        "",
        "[interfaces]",
    ]

    written_interface = False

    if auto_interface:
        lines.extend(
            [
                "  [[AutoInterface]]",
                "    type = AutoInterface",
                "    interface_enabled = True",
                "",
            ]
        )
        written_interface = True

    if tcp_listen is not None:
        host, port = parse_host_port(tcp_listen, default_host="0.0.0.0")
        lines.extend(
            [
                "  [[TCP Server]]",
                "    type = TCPServerInterface",
                "    interface_enabled = True",
                f"    listen_ip = {host}",
                f"    listen_port = {port}",
                "",
            ]
        )
        written_interface = True

    if tcp_connect is not None:
        host, port = parse_host_port(tcp_connect)
        lines.extend(
            [
                "  [[TCP Client]]",
                "    type = TCPClientInterface",
                "    interface_enabled = True",
                f"    target_host = {host}",
                f"    target_port = {port}",
                "",
            ]
        )
        written_interface = True

    if not written_interface:
        lines.extend(
            [
                "  # No interfaces are enabled.  Uncomment one of the examples",
                "  # below, or re-run `tau init --init` with --tcp-listen or",
                "  # --tcp-connect.",
                "  #",
                "  # [[AutoInterface]]",
                "  #   type = AutoInterface",
                "  #   interface_enabled = True",
                "  #",
                "  # [[TCP Server]]",
                "  #   type = TCPServerInterface",
                "  #   interface_enabled = True",
                "  #   listen_ip = 0.0.0.0",
                f"  #   listen_port = {DEFAULT_TCP_PORT}",
                "  #",
                "  # [[TCP Client]]",
                "  #   type = TCPClientInterface",
                "  #   interface_enabled = True",
                "  #   target_host = 127.0.0.1",
                f"  #   target_port = {DEFAULT_TCP_PORT}",
            ]
        )

    return "\n".join(lines).rstrip() + "\n"


def write_rns_config(
    path: Path,
    *,
    tcp_listen: Optional[str] = None,
    tcp_connect: Optional[str] = None,
    auto_interface: bool = True,
    loglevel: int = 2,
    force: bool = False,
) -> bool:
    """Write a Reticulum config, returning ``True`` when the file changed."""
    target = Path(path).expanduser()
    if target.exists() and not force:
        log.debug("Keeping existing Reticulum config at %s", target)
        return False
    text = render_rns_config(
        tcp_listen=tcp_listen,
        tcp_connect=tcp_connect,
        auto_interface=auto_interface,
        loglevel=loglevel,
    )
    atomic_write_text(target, text, mode=0o600)
    return True


def _parse_rns_config_sections(path: Path) -> List[Dict[str, str]]:
    """Parse a Reticulum config file into its ``[[subsection]]`` maps.

    Reticulum configs are INI-like with ``[section]`` headers and
    ``[[subsection]]`` interface blocks; each map carries its keys
    lower-cased plus ``__name__`` with the subsection heading.  Only
    subsection content is collected — top-level sections reset the
    current block.  Returns an empty list when the file cannot be read.
    """
    try:
        text = Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    sections: List[Dict[str, str]] = []
    current: Optional[Dict[str, str]] = None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith(";"):
            continue
        if line.startswith("[[") and line.endswith("]]"):
            current = {"__name__": line[2:-2].strip()}
            sections.append(current)
            continue
        if line.startswith("[") and line.endswith("]"):
            current = None
            continue
        if current is None or "=" not in line:
            continue
        key, _, value = line.partition("=")
        # Strip a trailing " # comment" / " ; comment" from the value.
        for marker in (" #", " ;"):
            cut = value.find(marker)
            if cut != -1:
                value = value[:cut]
        value = value.strip()
        # Reticulum's ConfigObj strips matching surrounding quotes.
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        current[key.strip().lower()] = value
    return sections


def _config_bool(value: Optional[str], *, default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "yes", "true", "on"}


class TcpListenEndpoint(NamedTuple):
    """A TCP server interface a Reticulum config enables.

    ``host`` is the configured ``listen_ip`` (``0.0.0.0`` when absent);
    ``device`` is the kernel interface name when the section binds by
    device instead — Reticulum then ignores ``listen_ip`` and binds an
    address on that device.
    """

    host: str
    port: int
    prefer_ipv6: bool = False
    device: Optional[str] = None


def tcp_listen_endpoints(config_path: Path) -> List[TcpListenEndpoint]:
    """Return the TCP server interfaces a Reticulum config enables.

    Reads the config file directly, so the result reflects hand-edited
    configs as well as generated ones.  Returns an empty list when the
    file does not exist or configures no TCP server interface.

    Enabledness mirrors Reticulum itself: an interface is attached when
    either ``interface_enabled`` or ``enabled`` is true — a section with
    neither key is not started.  ``port`` overrides ``listen_port`` in a
    section, the way Reticulum resolves them; a section without a usable
    port is skipped.
    """
    endpoints: List[TcpListenEndpoint] = []
    for section in _parse_rns_config_sections(config_path):
        if section.get("type", "").lower() != "tcpserverinterface":
            continue
        enabled = _config_bool(
            section.get("interface_enabled"), default=False
        ) or _config_bool(section.get("enabled"), default=False)
        if not enabled:
            continue
        port_text = section.get("port") or section.get("listen_port")
        if port_text is None:
            continue
        try:
            port = int(port_text)
        except ValueError:
            continue
        if not 1 <= port <= 65535:
            continue
        host = section.get("listen_ip") or "0.0.0.0"
        prefer_ipv6 = _config_bool(section.get("prefer_ipv6"), default=False)
        endpoints.append(
            TcpListenEndpoint(
                host=host,
                port=port,
                prefer_ipv6=prefer_ipv6,
                device=section.get("device") or None,
            )
        )
    return endpoints
