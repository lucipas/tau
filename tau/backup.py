"""Whole data-directory backup and restore.

A backup is a gzipped tar archive holding every node-state file that
exists in the data directory, plus a ``manifest.json`` describing the
archive.  The runtime lock file (``node.lock``) is never included, and
of Reticulum's configuration only the active config file is included;
Reticulum's runtime storage is recreated on first run.

Both directions refuse to run while a node is serving on the data
directory (the single-instance lock), so a backup can never catch the
mailbox or the Tan database half-written, and a restore can never land
under a running node.  The Tan database is captured with SQLite's
backup API, so committed entries still sitting in a write-ahead log are
included.  Restore treats the archive as untrusted input: the manifest
must declare format 1, every member must be an allowlisted regular file
with no ``..`` segments, the archive must contain an identity, and
expanded sizes are bounded while streaming.

This module must stay importable without Reticulum: only the identity
check during restore imports :mod:`tau.identity`.
"""

from __future__ import annotations

import io
import json
import os
import sqlite3
import tarfile
import tempfile
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

from . import __version__
from .config import FILES_DIR_NAME, TauConfig
from .instance_lock import BackupError, InstanceLock
from .util import atomic_write_json, ensure_dir, read_json

BACKUP_FORMAT_VERSION = 1
MANIFEST_NAME = "manifest.json"
SOCIAL_DB_NAME = "tan.sqlite3"
RNS_CONFIG_MEMBER = "reticulum/config"

# Archive member -> path relative to the data directory.  Only these
# files are ever written to or read from a backup.  The Reticulum config
# is read from, and restored to, the configured location (--rns-config).
# Received files live under files/<sender>/<name> and are enumerated
# separately: their member names are variable, so they are not listed
# here but validated by _is_received_file_member().
BACKUP_FILES = (
    "identity",
    "node.json",
    "peers.json",
    "mailbox.json",
    "inbox.json",
    "messages.json",
    SOCIAL_DB_NAME,
    "tan-rules.json",
    "allowlist.json",
    "groups.json",
    "blocklist.json",
    RNS_CONFIG_MEMBER,
)

# Files SQLite keeps beside the Tan database.  A restored database must
# never be paired with a stale journal or WAL from the previous state.
_SQLITE_SIDECARS = ("-wal", "-shm", "-journal")

# Bounds on untrusted archives, enforced while streaming.
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_MEMBER_BYTES = 4 * 1024**3
MAX_TOTAL_BYTES = 8 * 1024**3
_COPY_CHUNK = 1024 * 1024

# Exceptions raised by tarfile/gzip/zlib while reading a damaged archive.
_READ_ERRORS = (tarfile.TarError, EOFError, zlib.error, OSError)


def _manifest(data_dir: Path, files: List[str], node_name: str) -> Dict[str, object]:
    return {
        "format": BACKUP_FORMAT_VERSION,
        "tau": __version__,
        "created": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "node": node_name,
        "data_dir": str(data_dir),
        "files": files,
    }


def _is_within(path: Path, directory: Path) -> bool:
    try:
        path.resolve().relative_to(directory.resolve())
    except (OSError, ValueError):
        return False
    return True


def _is_received_file_member(name: str) -> bool:
    """True when *name* is a received-file member: ``files/<sender>/<name>``.

    Received files are always exactly two levels under ``files/`` — the
    sender directory is the sender's destination hash and the file name
    is a bare name (see ``parse_file_offer``) — so anything shallower,
    deeper, or naming ``.`` / ``..`` is not a member this module ever
    writes and is rejected as unexpected on restore.
    """
    parts = Path(name).parts
    return (
        len(parts) == 3
        and parts[0] == FILES_DIR_NAME
        and all(part not in ("", ".", "..") for part in parts[1:])
    )


def _received_files(config: TauConfig) -> List[str]:
    """Member names of the regular files under the received-files directory.

    Symlinks are never followed: a symlinked ``files/`` directory, a
    symlinked sender directory, or a symlinked file is skipped, so a
    backup can never smuggle in a path outside the data directory.
    A directory that cannot be listed is an error, never an empty
    result, so a backup cannot silently omit received files.
    """
    files_dir = config.files_dir
    if files_dir.is_symlink() or not files_dir.is_dir():
        return []
    members: List[str] = []
    try:
        senders = sorted(files_dir.iterdir(), key=lambda path: path.name)
    except OSError as exc:
        raise BackupError(f"could not list received files in {files_dir}: {exc}") from exc
    for sender_dir in senders:
        if sender_dir.is_symlink() or not sender_dir.is_dir():
            continue
        try:
            entries = sorted(sender_dir.iterdir(), key=lambda path: path.name)
        except OSError as exc:
            raise BackupError(
                f"could not list received files in {sender_dir}: {exc}"
            ) from exc
        for entry in entries:
            if entry.is_symlink() or not entry.is_file():
                continue
            members.append(f"{FILES_DIR_NAME}/{sender_dir.name}/{entry.name}")
    return members


def _destinations(
    config: TauConfig, received: List[str] = ()
) -> Dict[str, Path]:
    """Live location of every allowlisted archive member."""
    paths = {name: config.data_dir / name for name in BACKUP_FILES}
    paths.update({name: config.data_dir / name for name in received})
    paths[RNS_CONFIG_MEMBER] = config.rns_config_file
    return paths


def _state_paths(destinations: Dict[str, Path]) -> Dict[str, Path]:
    """Every live node-state path, labelled, including SQLite sidecars."""
    state = dict(destinations)
    db = destinations[SOCIAL_DB_NAME]
    for suffix in _SQLITE_SIDECARS:
        state[f"{SOCIAL_DB_NAME}{suffix}"] = Path(f"{db}{suffix}")
    return state


def _snapshot_sqlite(source: Path, dest: Path) -> Path:
    """Write a consistent copy of the SQLite database *source* to *dest*.

    The backup API reads through the database, so any committed pages
    still in the write-ahead log are part of the copy.
    """
    try:
        src = sqlite3.connect(str(source))
        try:
            dst = sqlite3.connect(str(dest))
            try:
                src.backup(dst)
            finally:
                dst.close()
        finally:
            src.close()
    except sqlite3.Error as exc:
        raise BackupError(f"could not snapshot {source.name}: {exc}") from exc
    return dest


def create_backup(
    config: TauConfig, target: Path, *, force: bool = False
) -> Tuple[Path, Dict[str, object]]:
    """Write a backup of *config*'s data directory to *target*.

    Returns ``(target, manifest)``.  Raises :class:`BackupError` when
    there is no node to back up, the target already exists without
    ``--force``, the target sits inside the data directory, or a node
    is serving on the data directory.
    """
    target = Path(target).expanduser()
    if target.is_dir():
        raise BackupError(f"backup path {target} is a directory")
    if os.path.lexists(target) and not force:
        raise BackupError(
            f"backup already exists at {target}; pass --force to overwrite it"
        )
    if _is_within(target, config.data_dir):
        raise BackupError(
            f"backup path {target} is inside the data directory "
            f"{config.data_dir}; choose a path outside it"
        )
    if not config.identity_path.is_file():
        raise BackupError(
            f"no identity at {config.identity_path}; run `tau init` first"
        )

    destinations = _destinations(config)

    # Hold the instance lock while the archive is written: a node that
    # starts mid-backup would mutate the mailbox and the Tan database
    # under us, and a node already serving means the on-disk state is
    # moving.  The lock file itself is never a backup member.
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    try:
        # Enumerated under the lock, so the member list cannot drift
        # from the files actually archived below.
        received = _received_files(config)
        destinations.update(
            {name: config.data_dir / name for name in received}
        )
        files = [name for name in BACKUP_FILES if destinations[name].is_file()]
        files.extend(received)
        # A symlinked state file would be archived as a symlink member,
        # which restore rejects as not a regular file — so a backup of
        # a node with linked state would succeed and then never
        # restore.  Refuse loudly instead.  (The Tan database is the
        # exception: it is snapshotted to a fresh regular file, so a
        # link there is harmless.)
        for name in files:
            if name != SOCIAL_DB_NAME and destinations[name].is_symlink():
                raise BackupError(
                    f"cannot back up {destinations[name]}: it is a symlink"
                )
        manifest = _manifest(config.data_dir, files, config.name)
        manifest_bytes = (
            json.dumps(manifest, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")
        # Refuse before writing anything a restore could not read back.
        if len(manifest_bytes) > MAX_MANIFEST_BYTES:
            raise BackupError(
                "too many files to back up: the manifest would exceed "
                f"{MAX_MANIFEST_BYTES} bytes, the restore limit"
            )

        parent = target.parent
        # A missing parent is created private (0o700); an existing one is
        # left untouched, matching `tau id export`.
        ensure_dir(parent, restrict_existing=False)
        fd, tmp_name = tempfile.mkstemp(
            dir=str(parent), prefix=f".{target.name}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "wb") as handle:
                with tarfile.open(fileobj=handle, mode="w:gz") as archive:
                    info = tarfile.TarInfo(MANIFEST_NAME)
                    info.size = len(manifest_bytes)
                    info.mtime = int(datetime.now(timezone.utc).timestamp())
                    archive.addfile(info, io.BytesIO(manifest_bytes))
                    with tempfile.TemporaryDirectory() as scratch:
                        expanded = 0
                        for name in files:
                            source = destinations[name]
                            if name == SOCIAL_DB_NAME:
                                source = _snapshot_sqlite(
                                    source, Path(scratch) / SOCIAL_DB_NAME
                                )
                            # Restore bounds each member and the total
                            # expanded payload; enforce the same bounds
                            # here so a successful backup is restorable.
                            size = Path(source).stat().st_size
                            expanded += size
                            if size > MAX_MEMBER_BYTES or expanded > MAX_TOTAL_BYTES:
                                raise BackupError(
                                    f"cannot back up {name!r}: the data directory "
                                    "exceeds the restore size limit"
                                )
                            archive.add(source, arcname=name)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(tmp_name, 0o600)
            os.replace(tmp_name, target)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise
    finally:
        lock.release()
    return target, manifest


def _read_manifest(archive: tarfile.TarFile) -> Dict[str, object]:
    try:
        member = archive.getmember(MANIFEST_NAME)
    except KeyError as exc:
        raise BackupError("not a tau backup: manifest.json is missing") from exc
    if not member.isreg():
        raise BackupError("not a tau backup: manifest.json is not a regular file")
    raw = archive.extractfile(member)
    if raw is None:  # pragma: no cover - defensive
        raise BackupError("not a tau backup: manifest.json is unreadable")
    data = raw.read(MAX_MANIFEST_BYTES + 1)
    if len(data) > MAX_MANIFEST_BYTES:
        raise BackupError("not a tau backup: manifest.json is too large")
    try:
        manifest = json.loads(data)
    except ValueError as exc:
        raise BackupError(f"not a tau backup: manifest.json is invalid: {exc}") from exc
    if not isinstance(manifest, dict):
        raise BackupError("not a tau backup: manifest.json is not an object")
    if manifest.get("format") != BACKUP_FORMAT_VERSION:
        raise BackupError(
            "unsupported backup format "
            f"{manifest.get('format')!r} (expected {BACKUP_FORMAT_VERSION})"
        )
    listed = manifest.get("files")
    if not isinstance(listed, list) or not all(
        isinstance(name, str) for name in listed
    ):
        raise BackupError("not a tau backup: manifest lists no files")
    if len(set(listed)) != len(listed):
        raise BackupError("not a tau backup: manifest lists a file more than once")
    return manifest


def _check_members(archive: tarfile.TarFile, manifest: Dict[str, object]) -> List[str]:
    """Validate every member against the manifest and the allowlist."""
    listed = manifest["files"]
    assert isinstance(listed, list)  # _read_manifest guarantees this
    expected = {MANIFEST_NAME, *listed}
    allowed = {MANIFEST_NAME, *BACKUP_FILES}
    members = archive.getmembers()
    names = [member.name for member in members]
    if len(set(names)) != len(names):
        raise BackupError("not a tau backup: an archive member appears more than once")
    for member in members:
        name = member.name
        if name.startswith("/") or ".." in Path(name).parts:
            raise BackupError(f"not a tau backup: unsafe member path {name!r}")
        if name not in allowed and not _is_received_file_member(name):
            raise BackupError(f"not a tau backup: unexpected member {name!r}")
        if name not in expected:
            raise BackupError(f"not a tau backup: member {name!r} is not in the manifest")
        if not member.isreg():
            raise BackupError(f"not a tau backup: member {name!r} is not a regular file")
    if set(names) != expected:
        missing = sorted(expected - set(names))
        raise BackupError(f"not a tau backup: missing members {missing}")
    if "identity" not in listed:
        raise BackupError("not a tau backup: it contains no identity")
    return listed


def _reserve_name(path: Path) -> Path:
    """Reserve an unused file name beside *path* (same filesystem)."""
    fd, name = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.name}.", suffix=".restore-old"
    )
    os.close(fd)
    return Path(name)


def _unlink_quietly(path: Path) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


def _temp_beside(dest: Path) -> Path:
    """Create an owner-only (0600, whatever the umask) empty file beside *dest*."""
    ensure_dir(dest.parent, restrict_existing=False)
    fd, name = tempfile.mkstemp(
        dir=str(dest.parent), prefix=f".{dest.name}.", suffix=".restore"
    )
    os.close(fd)
    return Path(name)


def _copy_member(raw, handle, name: str, budget: int) -> int:
    """Stream one archive member into *handle*, enforcing *budget* bytes."""
    copied = 0
    while True:
        try:
            chunk = raw.read(_COPY_CHUNK)
        except _READ_ERRORS as exc:
            raise BackupError(
                f"not a valid tau backup: {name!r} is damaged: {exc}"
            ) from exc
        if not chunk:
            return copied
        copied += len(chunk)
        if copied > budget:
            raise BackupError(
                f"not a tau backup: {name!r} expands beyond the restore size limit"
            )
        handle.write(chunk)


def _stage(
    archive: tarfile.TarFile,
    files: List[str],
    destinations: Dict[str, Path],
    staged: Dict[Path, Path],
) -> None:
    """Write every member to a temporary file beside its destination.

    Nothing live is touched here, so a damaged or oversized member fails
    before any node state changes.  Each temporary path is recorded in
    *staged* as soon as it exists so the caller can clean it up.
    """
    total = 0
    for name in files:
        dest = destinations[name]
        try:
            raw = archive.extractfile(archive.getmember(name))
        except _READ_ERRORS as exc:
            raise BackupError(
                f"not a valid tau backup: {name!r} is damaged: {exc}"
            ) from exc
        if raw is None:  # pragma: no cover - defensive
            raise BackupError(f"not a tau backup: {name!r} is unreadable")
        temp = _temp_beside(dest)
        staged[dest] = temp
        with temp.open("wb") as handle:
            total += _copy_member(
                raw, handle, name, min(MAX_MEMBER_BYTES, MAX_TOTAL_BYTES - total)
            )
            handle.flush()
            os.fsync(handle.fileno())


def _check_received_dirs(config: TauConfig, received: List[str]) -> None:
    """Refuse received-file restores whose parent directories are not real.

    Each ``files/<sender>`` member is written beneath ``files/`` and its
    sender directory; a symlink there would redirect the write outside
    the data directory.  Called under the instance lock, before staging.
    """
    files_dir = config.data_dir / FILES_DIR_NAME
    if not received:
        return
    paths = [files_dir] + [files_dir / Path(name).parts[1] for name in received]
    for path in dict.fromkeys(paths):
        if os.path.lexists(path) and (path.is_symlink() or not path.is_dir()):
            raise BackupError(
                f"refusing to restore received files: {path} is a symlink "
                "or not a directory"
            )


def _rebase_inbox(staged: Path, destinations: Dict[str, Path], files: List[str]) -> None:
    """Point restored received-file records at their new locations.

    The staged ``inbox.json`` records each file's path from the machine
    it came from.  A record is repointed only when its own member is in
    the archive, so the rewritten path always names the installed file.
    """
    raw = read_json(staged, default=None)
    if not isinstance(raw, dict) or not isinstance(raw.get("messages"), list):
        return
    members = set(files)
    changed = False
    for record in raw["messages"]:
        if not isinstance(record, dict) or record.get("kind") != "file":
            continue
        sender = record.get("destination")
        name = record.get("name")
        if not isinstance(sender, str) or not isinstance(name, str):
            continue
        member = f"{FILES_DIR_NAME}/{sender}/{name}"
        if member in members and _is_received_file_member(member):
            record["path"] = str(destinations[member])
            changed = True
    if changed:
        atomic_write_json(staged, raw, mode=0o600)


def _verify_identity(path: Path) -> None:
    # Imported here: identity validation is the one step that needs RNS,
    # and the archive logic above must stay importable without it.
    from .identity import load_identity

    try:
        identity = load_identity(path)
    except Exception as exc:
        raise BackupError(
            f"not a valid tau backup: identity is unreadable: {exc}"
        ) from exc
    if identity is None:  # pragma: no cover - the staged file always exists
        raise BackupError("not a valid tau backup: identity is unreadable")


def _install(staged: Dict[Path, Path], removals: List[Path]) -> None:
    """Move staged files into place and delete *removals*, all or nothing.

    Every live path that is replaced or removed is first renamed aside.
    If any step fails, the placed files are removed and the originals
    are renamed back, so the data directory keeps its previous state.
    """
    set_aside: List[Tuple[Path, Path]] = []
    placed: List[Path] = []
    try:
        for dest in [*staged, *removals]:
            if os.path.lexists(dest):
                aside = _reserve_name(dest)
                try:
                    os.replace(dest, aside)
                except BaseException:
                    _unlink_quietly(aside)
                    raise
                set_aside.append((dest, aside))
            if dest in staged:
                os.replace(staged[dest], dest)
                placed.append(dest)
    except BaseException:
        for dest in reversed(placed):
            _unlink_quietly(dest)
        for dest, aside in reversed(set_aside):
            try:
                os.replace(aside, dest)
            except OSError:  # pragma: no cover - best effort rollback
                pass
        raise
    for _, aside in set_aside:
        _unlink_quietly(aside)


def restore_backup(
    config: TauConfig, source: Path, *, force: bool = False
) -> Tuple[Dict[str, object], List[str]]:
    """Restore the backup at *source* into *config*'s data directory.

    Returns ``(manifest, restored_files)``.  Raises
    :class:`BackupError` when the archive is not a valid tau backup,
    the target already holds node state without ``--force``, or a node
    is serving on the data directory.  Members are staged beside their
    destinations and the identity is verified before anything live is
    replaced.  With ``--force``, node state absent from the archive
    (including SQLite sidecars) is removed, so the result reflects only
    the backup.
    """
    # Identity locking is the only RNS-dependent helper the restore needs.
    from .identity import _identity_lock

    source = Path(source).expanduser()
    if not source.is_file():
        raise BackupError(f"no backup at {source}")

    try:
        archive = tarfile.open(source, mode="r:gz")
    except (tarfile.TarError, OSError) as exc:
        raise BackupError(f"not a valid tau backup: {exc}") from exc
    with archive:
        try:
            manifest = _read_manifest(archive)
            files = _check_members(archive, manifest)
        except _READ_ERRORS as exc:
            raise BackupError(f"not a valid tau backup: {exc}") from exc
        destinations = _destinations(
            config, [name for name in files if _is_received_file_member(name)]
        )
        state = _state_paths(destinations)
        # Create the data directory before taking the lock: the lock
        # file lives inside it, so acquiring first would fail on a
        # fresh directory.
        config.prepare()
        # Hold the instance lock across the whole restore: a node that
        # starts after the probe but before the write would load the old
        # state and then serve with different state than the one on disk.
        # The identity-creation lock serialises the move-in against a
        # concurrent `tau init` on the same directory; lock order
        # (instance, then identity) matches `TauNode.start`.
        lock = InstanceLock(config.instance_lock_path, config.data_dir)
        lock.acquire()
        try:
            _check_received_dirs(
                config, [name for name in files if _is_received_file_member(name)]
            )
            # Received files on disk that the archive does not hold are
            # stale state too: a --force restore removes them, like any
            # other included file.  Scanned under the lock so a file a
            # serving node saves before the lock is taken cannot survive.
            for name in _received_files(config):
                state.setdefault(name, config.data_dir / name)
            with _identity_lock(config.identity_path):
                existing = [
                    label for label, path in state.items() if os.path.lexists(path)
                ]
                if existing and not force:
                    raise BackupError(
                        "node state already exists in "
                        f"{config.data_dir} ({', '.join(existing)}); "
                        "pass --force to replace it"
                    )
                staged: Dict[Path, Path] = {}
                try:
                    _stage(archive, files, destinations, staged)
                    if "inbox.json" in files:
                        _rebase_inbox(
                            staged[destinations["inbox.json"]],
                            destinations,
                            files,
                        )
                    _verify_identity(staged[destinations["identity"]])
                    removals = [
                        path
                        for label, path in state.items()
                        if label not in files and os.path.lexists(path)
                    ]
                    _install(staged, removals)
                finally:
                    for temp in staged.values():
                        _unlink_quietly(temp)
        finally:
            lock.release()
    return manifest, files
