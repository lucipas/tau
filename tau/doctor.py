"""Read-only health checks for a tau data directory.

``tau doctor`` answers "is my data directory healthy?" without writing
anything or taking the instance lock.  It exists because every store
loader in the codebase fails soft on bad data: unparseable JSON reads
as ``None`` and the peer registry, mailbox and inbox silently skip
invalid entries on load.  A truncated or hand-edited file can therefore
lose queued mail, held envelopes, peers or messages with no visible
symptom until ``serve`` or ``send`` fails later.  Doctor surfaces
exactly what the loaders would silently drop.
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import pathname2url

import RNS

from .allowlist import AllowListError, load_allowlist
from .config import MESSAGES_FILE_NAME, TauConfig
from .identity import IdentityError, destination_hash
from .inbox import MAX_INBOX, _valid_record
from .mailbox import HeldEnvelope, OutboxEntry
from .peers import MAX_PEERS, Peer
from .protocol import short_hash
from .tangents.rules import RulesError, load_ruleset
from .webapi import MAX_MESSAGE_LOG, _clean_stored_message

STATUS_OK = "ok"
STATUS_WARN = "warn"
STATUS_FAIL = "fail"

_TAN_TABLES = ("meta", "posts", "heads", "follows")
# Base posts columns that every Tan read and write relies on.  ``v``,
# ``kind`` and ``target`` are added by Tan's migrations, so they are not
# required of an older file.
_TAN_POST_COLUMNS = (
    "hash",
    "author",
    "prev",
    "key",
    "name",
    "text",
    "created_at",
    "received_at",
    "sig",
    "seq",
    "linked",
)


@dataclass(frozen=True)
class Check:
    """One store's verdict: ``ok``, ``warn`` or ``fail`` plus a detail."""

    name: str
    status: str
    detail: str

    def to_dict(self) -> Dict[str, str]:
        return {"name": self.name, "status": self.status, "detail": self.detail}

    @property
    def failed(self) -> bool:
        return self.status == STATUS_FAIL


def run_checks(config: TauConfig) -> List[Check]:
    """Run every health check against *config*'s data directory.

    Read-only: no file is created, modified or locked.
    """
    return [
        _check_identity(config),
        _check_node_metadata(config),
        _check_peers(config),
        _check_mailbox(config),
        _check_inbox(config),
        _check_messages(config),
        _check_tan(config),
        _check_tan_rules(config),
        _check_allowlist(config),
        _check_instance_lock(config),
    ]


def all_ok(checks: List[Check]) -> bool:
    return not any(check.failed for check in checks)


def _read_json(path: Path) -> Tuple[Optional[Any], Optional[str]]:
    """Parse *path* as JSON, distinguishing missing from unparseable.

    Returns ``(data, None)`` on success, ``(None, "missing")`` when the
    file does not exist, and ``(None, <reason>)`` when it cannot be read
    or parsed.  Unlike :func:`tau.util.read_json` this never swallows a
    parse error: a corrupt store file is exactly what doctor reports.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None, "missing"
    except UnicodeDecodeError as exc:
        return None, f"is not valid UTF-8: {exc}"
    except OSError as exc:
        return None, f"could not read: {exc}"
    try:
        return json.loads(text), None
    except ValueError as exc:
        return None, f"could not parse JSON: {exc}"


def _load_identity_read_only(path: Path) -> RNS.Identity:
    """Load an identity without ``identity.load_identity``'s chmod side effect."""
    identity = RNS.Identity.from_file(str(path))
    if identity is None:
        raise IdentityError(f"could not load identity from {path}")
    return identity


def _check_identity(config: TauConfig) -> Check:
    if not config.identity_path.exists():
        return Check("identity", STATUS_FAIL, "no identity — run `tau init` first")
    try:
        identity = _load_identity_read_only(config.identity_path)
    except IdentityError as exc:
        return Check("identity", STATUS_FAIL, f"could not load identity: {exc}")
    except Exception as exc:  # RNS raises its own errors on corrupt files
        return Check("identity", STATUS_FAIL, f"could not load identity: {exc}")
    if identity is None:  # pragma: no cover - exists() already ruled this out
        return Check("identity", STATUS_FAIL, "no identity — run `tau init` first")
    return Check(
        "identity",
        STATUS_OK,
        f"destination {short_hash(destination_hash(identity).hex())}",
    )


def _check_node_metadata(config: TauConfig) -> Check:
    raw, error = _read_json(config.node_file)
    if error == "missing":
        return Check(
            "node metadata", STATUS_WARN, "no node.json — run `tau init` to set a name"
        )
    if error is not None:
        return Check("node metadata", STATUS_FAIL, f"node.json {error}")
    if not isinstance(raw, dict):
        return Check("node metadata", STATUS_FAIL, "node.json is not a JSON object")
    name = raw.get("name")
    if not isinstance(name, str) or not name.strip():
        return Check("node metadata", STATUS_WARN, "no node name stored in node.json")
    return Check("node metadata", STATUS_OK, f"name {name!r}")


def _check_peers(config: TauConfig) -> Check:
    raw, error = _read_json(config.peers_path)
    if error == "missing":
        return Check("peers", STATUS_OK, "no peers yet")
    if error is not None:
        return Check("peers", STATUS_FAIL, f"peers.json {error}")
    if isinstance(raw, dict):
        entries = raw.get("peers", [])
    elif isinstance(raw, list):
        entries = raw
    else:
        return Check("peers", STATUS_FAIL, "peers.json has an unexpected format")
    if not isinstance(entries, list):
        return Check("peers", STATUS_FAIL, "peers.json has an invalid peers list")
    invalid = 0
    seen_hashes = set()
    duplicates = 0
    for entry in entries:
        if not isinstance(entry, dict):
            invalid += 1
            continue
        try:
            peer = Peer.from_dict(entry)
        except (TypeError, ValueError):
            invalid += 1
            continue
        if peer.destination_hash in seen_hashes:
            duplicates += 1
        seen_hashes.add(peer.destination_hash)
    # The registry evicts the least recently seen peers beyond MAX_PEERS on load.
    retained = min(len(seen_hashes), MAX_PEERS)
    evicted = len(seen_hashes) - retained
    detail = f"{retained} peer(s)"
    problems = []
    if invalid:
        problems.append(f"{invalid} invalid entr{'y' if invalid == 1 else 'ies'} skipped on load")
    if duplicates:
        problems.append(f"{duplicates} duplicate(s)")
    if evicted:
        problems.append(f"{evicted} peer(s) evicted on load (limit {MAX_PEERS})")
    if problems:
        return Check("peers", STATUS_WARN, f"{detail}; {'; '.join(problems)}")
    return Check("peers", STATUS_OK, detail)


def _check_mailbox(config: TauConfig) -> Check:
    raw, error = _read_json(config.mailbox_path)
    if error == "missing":
        return Check("mailbox", STATUS_OK, "empty")
    if error is not None:
        return Check("mailbox", STATUS_FAIL, f"mailbox.json {error}")
    if not isinstance(raw, dict):
        return Check("mailbox", STATUS_FAIL, "mailbox.json is not a JSON object")
    invalid_outbox = 0
    duplicate_outbox = 0
    outbox_ids: set = set()
    outbox = raw.get("outbox")
    if outbox is None:
        pass
    elif isinstance(outbox, list):
        for entry in outbox:
            if not isinstance(entry, dict):
                invalid_outbox += 1
                continue
            try:
                parsed = OutboxEntry.from_dict(entry)
            except (TypeError, ValueError):
                invalid_outbox += 1
                continue
            if parsed.id in outbox_ids:
                duplicate_outbox += 1
            outbox_ids.add(parsed.id)
    else:
        return Check("mailbox", STATUS_FAIL, "mailbox.json has an invalid outbox list")
    outbox_count = len(outbox_ids)
    invalid_held = 0
    duplicate_held = 0
    held_ids: set = set()
    held = raw.get("held")
    if held is None:
        pass
    elif isinstance(held, list):
        for entry in held:
            if not isinstance(entry, dict):
                invalid_held += 1
                continue
            try:
                parsed_held = HeldEnvelope.from_dict(entry)
            except (TypeError, ValueError):
                invalid_held += 1
                continue
            if parsed_held.id in held_ids:
                duplicate_held += 1
            held_ids.add(parsed_held.id)
    else:
        return Check("mailbox", STATUS_FAIL, "mailbox.json has an invalid held list")
    held_count = len(held_ids)
    detail = f"{outbox_count} queued, {held_count} held"
    problems = []
    if invalid_outbox:
        problems.append(
            f"{invalid_outbox} invalid outbox entr{'y' if invalid_outbox == 1 else 'ies'} skipped on load"
        )
    if invalid_held:
        problems.append(
            f"{invalid_held} invalid held envelope(s) skipped on load"
        )
    if duplicate_outbox:
        problems.append(
            f"{duplicate_outbox} duplicate outbox entr{'y' if duplicate_outbox == 1 else 'ies'} "
            "(later copy overwrites earlier on load)"
        )
    if duplicate_held:
        problems.append(
            f"{duplicate_held} duplicate held envelope(s) (later copy overwrites earlier on load)"
        )
    if problems:
        return Check("mailbox", STATUS_WARN, f"{detail}; {'; '.join(problems)}")
    return Check("mailbox", STATUS_OK, detail)


def _check_inbox(config: TauConfig) -> Check:
    raw, error = _read_json(config.inbox_path)
    if error == "missing":
        return Check("inbox", STATUS_OK, "empty")
    if error is not None:
        return Check("inbox", STATUS_FAIL, f"inbox.json {error}")
    if not isinstance(raw, dict):
        return Check("inbox", STATUS_FAIL, "inbox.json is not a JSON object")
    entries = raw.get("messages")
    if entries is None:
        return Check("inbox", STATUS_OK, "0 message(s)")
    if not isinstance(entries, list):
        return Check("inbox", STATUS_FAIL, "inbox.json has an invalid messages list")
    invalid = sum(1 for entry in entries if _valid_record(entry) is None)
    valid = len(entries) - invalid
    # The loader keeps only the newest MAX_INBOX valid records.
    retained = min(valid, MAX_INBOX)
    problems = []
    if invalid:
        problems.append(f"{invalid} invalid record(s) skipped on load")
    if valid > MAX_INBOX:
        problems.append(f"{valid - MAX_INBOX} older message(s) dropped on load (limit {MAX_INBOX})")
    if problems:
        return Check("inbox", STATUS_WARN, f"{retained} message(s); {'; '.join(problems)}")
    return Check("inbox", STATUS_OK, f"{retained} message(s)")


def _check_messages(config: TauConfig) -> Check:
    """Check the web console's persisted message history (messages.json)."""
    path = config.data_dir / MESSAGES_FILE_NAME
    raw, error = _read_json(path)
    if error == "missing":
        return Check("messages", STATUS_OK, "no web history yet")
    if error is not None:
        return Check("messages", STATUS_FAIL, f"messages.json {error}")
    if not isinstance(raw, dict):
        return Check("messages", STATUS_FAIL, "messages.json is not a JSON object")
    entries = raw.get("messages")
    if entries is None:
        return Check("messages", STATUS_OK, "0 message(s)")
    if not isinstance(entries, list):
        return Check("messages", STATUS_FAIL, "messages.json has an invalid messages list")
    invalid = sum(1 for entry in entries if _clean_stored_message(entry) is None)
    valid = len(entries) - invalid
    problems = []
    if invalid:
        problems.append(f"{invalid} invalid record(s) skipped on load")
    if valid > MAX_MESSAGE_LOG:
        # WebBridge._load_messages keeps only the newest MAX_MESSAGE_LOG.
        problems.append(
            f"{valid - MAX_MESSAGE_LOG} older message(s) dropped on load "
            f"(limit {MAX_MESSAGE_LOG})"
        )
        valid = MAX_MESSAGE_LOG
    if problems:
        return Check(
            "messages",
            STATUS_WARN,
            f"{valid} message(s); {'; '.join(problems)}",
        )
    return Check("messages", STATUS_OK, f"{valid} message(s)")


def _check_tan(config: TauConfig) -> Check:
    path = config.social_db_path
    if not path.exists():
        return Check("tan", STATUS_OK, "no posts yet")
    # ``immutable=1`` keeps the probe truly read-only: opening a WAL-mode
    # database normally creates -shm/-wal sidecar files in the data
    # directory.  The trade-off is that un-checkpointed WAL frames from a
    # currently serving node are not seen, so counts may lag the live
    # store — the structural checks (tables, integrity) still hold.
    uri = f"file:{pathname2url(str(path))}?mode=ro&immutable=1"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=10.0)
    except sqlite3.Error as exc:
        return Check("tan", STATUS_FAIL, f"could not open tan.sqlite3: {exc}")
    try:
        try:
            verdict = conn.execute("PRAGMA quick_check").fetchone()
        except sqlite3.Error as exc:
            return Check("tan", STATUS_FAIL, f"tan.sqlite3 integrity check failed: {exc}")
        if verdict is None or str(verdict[0]).strip() != "ok":
            return Check(
                "tan", STATUS_FAIL, f"tan.sqlite3 integrity check failed: {verdict}"
            )
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        # The immutable image ignores WAL frames, so a schema that looks
        # incomplete may just be uncheckpointed.  Report that as unverified.
        wal = path.with_name(path.name + "-wal")
        wal_pending = wal.exists() and wal.stat().st_size > 0
        missing = [table for table in _TAN_TABLES if table not in tables]
        if missing:
            if wal_pending:
                return Check(
                    "tan",
                    STATUS_WARN,
                    f"tan.sqlite3 main file lacks table(s) {', '.join(missing)}; "
                    "uncheckpointed WAL changes could not be read",
                )
            return Check(
                "tan",
                STATUS_FAIL,
                f"tan.sqlite3 is missing table(s): {', '.join(missing)}",
            )
        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(posts)").fetchall()
        }
        missing_columns = [column for column in _TAN_POST_COLUMNS if column not in columns]
        if missing_columns:
            if wal_pending:
                return Check(
                    "tan",
                    STATUS_WARN,
                    f"tan.sqlite3 main file lacks posts column(s) {', '.join(missing_columns)}; "
                    "uncheckpointed WAL changes could not be read",
                )
            return Check(
                "tan",
                STATUS_FAIL,
                f"tan.sqlite3 posts table is missing column(s): {', '.join(missing_columns)}",
            )
        count = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        return Check("tan", STATUS_OK, f"{count} post(s)")
    finally:
        conn.close()


def _check_tan_rules(config: TauConfig) -> Check:
    if not config.tan_rules_path.exists():
        return Check("tan rules", STATUS_OK, "defaults (admit everything)")
    _, error = _read_json(config.tan_rules_path)
    if error is not None:
        return Check("tan rules", STATUS_FAIL, f"tan-rules.json {error}")
    try:
        ruleset = load_ruleset(config.tan_rules_path)
    except RulesError as exc:
        return Check("tan rules", STATUS_FAIL, str(exc))
    default = "allow" if ruleset.default_allow else "deny"
    detail = (
        f"default {default}, {len(ruleset.allow_authors)} allowed author(s), "
        f"{len(ruleset.block_authors)} blocked author(s)"
    )
    if ruleset.allow_followed:
        detail += ", followed authors admitted"
    return Check("tan rules", STATUS_OK, detail)


def _check_allowlist(config: TauConfig) -> Check:
    if not config.allowlist_path.exists():
        return Check("allowlist", STATUS_OK, "disabled")
    _, error = _read_json(config.allowlist_path)
    if error is not None:
        return Check("allowlist", STATUS_FAIL, f"allowlist.json {error}")
    try:
        allowlist = load_allowlist(config.allowlist_path)
    except AllowListError as exc:
        return Check("allowlist", STATUS_FAIL, str(exc))
    if not allowlist.enabled:
        return Check("allowlist", STATUS_OK, "disabled")
    return Check(
        "allowlist", STATUS_OK, f"enabled, {len(allowlist.allow)} allowed peer(s)"
    )


def _check_instance_lock(config: TauConfig) -> Check:
    """Infer whether a node is serving from the PID it wrote to node.lock.

    The lock itself is not probed: taking ``flock`` here would make a
    concurrent ``tau serve`` fail with InstanceAlreadyRunning.  A PID
    check cannot prove the lock state (PIDs get reused, and a node may
    not have written its PID yet), so the verdict is an inference.
    """
    path = config.instance_lock_path
    if not path.exists():
        return Check("instance lock", STATUS_OK, "no node serving")
    try:
        raw = path.read_bytes()
    except OSError as exc:
        return Check("instance lock", STATUS_WARN, f"could not read node.lock: {exc}")
    text = raw.decode("utf-8", errors="replace").strip()
    try:
        pid = int(text)
    except ValueError:
        pid = 0
    if pid <= 0:
        return Check(
            "instance lock", STATUS_WARN, "node.lock has no readable pid; serving state unknown"
        )
    if os.name == "nt":  # pragma: no cover - os.kill(pid, 0) terminates on Windows
        return Check(
            "instance lock", STATUS_WARN, f"node.lock records pid {pid}; cannot probe on Windows"
        )
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        # The file survives a stopped node, so a dead PID means nothing is serving.
        return Check("instance lock", STATUS_OK, f"no node serving (stale pid {pid})")
    except PermissionError:
        pass  # the process exists but belongs to another user
    except OSError as exc:
        return Check("instance lock", STATUS_WARN, f"could not probe pid {pid}: {exc}")
    return Check("instance lock", STATUS_OK, f"node probably serving (pid {pid})")
