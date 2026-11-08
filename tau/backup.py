"""Whole data-directory backup and restore.

A backup is a gzipped tar archive holding every node-state file that
exists in the data directory, plus a ``manifest.json`` describing the
archive.  The runtime lock file (``node.lock``) is never included, and
nothing under ``reticulum/`` is included except the generated
configuration — Reticulum's runtime storage is recreated on first run.

Both directions refuse to run while a node is serving on the data
directory (the single-instance lock), so a backup can never catch the
mailbox or the Tan database half-written, and a restore can never land
under a running node.  Restore treats the archive as untrusted input:
the manifest must declare format 1, and every member must be an
allowlisted relative path with no ``..`` segments.
"""

from __future__ import annotations

import io
import json
import os
import shutil
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from . import __version__
from .config import TauConfig
from .identity import _identity_lock, load_identity
from .node import BackupError, InstanceLock
from .util import ensure_dir

BACKUP_FORMAT_VERSION = 1
MANIFEST_NAME = "manifest.json"

# Archive member -> path relative to the data directory.  Only these
# files are ever written to or read from a backup.
BACKUP_FILES = (
    "identity",
    "node.json",
    "peers.json",
    "mailbox.json",
    "inbox.json",
    "messages.json",
    "tan.sqlite3",
    "tan-rules.json",
    "allowlist.json",
    "reticulum/config",
)


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
    if load_identity(config.identity_path) is None:
        raise BackupError(
            f"no identity at {config.identity_path}; run `tau init` first"
        )

    # Hold the instance lock while the archive is written: a node that
    # starts mid-backup would mutate the mailbox and the Tan database
    # under us, and a node already serving means the on-disk state is
    # moving.  The lock file itself is never a backup member.
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    lock.acquire()
    try:
        files = [name for name in BACKUP_FILES if (config.data_dir / name).is_file()]
        manifest = _manifest(config.data_dir, files, config.name)
        manifest_bytes = (
            json.dumps(manifest, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")

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
                    for name in files:
                        archive.add(config.data_dir / name, arcname=name)
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
    raw = archive.extractfile(member)
    if raw is None:  # pragma: no cover - defensive
        raise BackupError("not a tau backup: manifest.json is unreadable")
    try:
        manifest = json.load(raw)
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
    return manifest


def _check_members(archive: tarfile.TarFile, manifest: Dict[str, object]) -> List[str]:
    """Validate every member against the manifest and the allowlist."""
    listed = manifest["files"]
    assert isinstance(listed, list)  # _read_manifest guarantees this
    expected = {MANIFEST_NAME, *listed}
    allowed = {MANIFEST_NAME, *BACKUP_FILES}
    members = archive.getmembers()
    names = [member.name for member in members]
    for member in members:
        name = member.name
        if member.isdir():
            raise BackupError(f"not a tau backup: unexpected directory {name!r}")
        if name.startswith("/") or ".." in Path(name).parts:
            raise BackupError(f"not a tau backup: unsafe member path {name!r}")
        if name not in allowed:
            raise BackupError(f"not a tau backup: unexpected member {name!r}")
        if name not in expected:
            raise BackupError(f"not a tau backup: member {name!r} is not in the manifest")
    if set(names) != expected:
        missing = sorted(expected - set(names))
        raise BackupError(f"not a tau backup: missing members {missing}")
    return listed


def restore_backup(
    config: TauConfig, source: Path, *, force: bool = False
) -> Tuple[Dict[str, object], List[str]]:
    """Restore the backup at *source* into *config*'s data directory.

    Returns ``(manifest, restored_files)``.  Raises
    :class:`BackupError` when the archive is not a valid tau backup,
    the target already holds node state without ``--force``, or a node
    is serving on the data directory.  Files are extracted to a
    temporary directory and validated first, so a bad archive never
    leaves a half-restored data directory behind.
    """
    source = Path(source).expanduser()
    if not source.is_file():
        raise BackupError(f"no backup at {source}")

    try:
        archive = tarfile.open(source, mode="r:gz")
    except (tarfile.TarError, OSError) as exc:
        raise BackupError(f"not a valid tau backup: {exc}") from exc
    with archive:
        manifest = _read_manifest(archive)
        files = _check_members(archive, manifest)
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
            with _identity_lock(config.identity_path):
                existing = [
                    name
                    for name in files
                    if (config.data_dir / name).exists()
                ]
                if existing and not force:
                    raise BackupError(
                        "node state already exists in "
                        f"{config.data_dir} ({', '.join(existing)}); "
                        "pass --force to replace it"
                    )
                # Stage beside the data directory (same filesystem, so
                # the move-in below is an atomic rename) and validate
                # before anything lands in the data directory.
                ensure_dir(config.data_dir.parent, restrict_existing=False)
                tmp_dir = Path(
                    tempfile.mkdtemp(
                        dir=str(config.data_dir.parent),
                        prefix=".tau-restore-",
                    )
                )
                try:
                    for member in archive.getmembers():
                        if member.name == MANIFEST_NAME:
                            continue
                        target = tmp_dir / member.name
                        target.parent.mkdir(parents=True, exist_ok=True)
                        raw = archive.extractfile(member)
                        if raw is None:  # pragma: no cover - defensive
                            raise BackupError(
                                f"not a tau backup: {member.name!r} is unreadable"
                            )
                        with target.open("wb") as handle:
                            shutil.copyfileobj(raw, handle)
                    if "identity" in files:
                        identity = load_identity(tmp_dir / "identity")
                        if identity is None:  # pragma: no cover - defensive
                            raise BackupError(
                                "not a valid tau backup: identity is unreadable"
                            )
                    for name in files:
                        staged = tmp_dir / name
                        target = config.data_dir / name
                        target.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(staged, target)
                    if "identity" in files:
                        os.chmod(config.identity_path, 0o600)
                finally:
                    shutil.rmtree(tmp_dir, ignore_errors=True)
        finally:
            lock.release()
    return manifest, files
