"""Whole data-directory backup and restore.

A backup is a gzipped tar archive holding every node-state file that
exists in the data directory, plus a ``manifest.json`` describing the
archive.  The runtime lock file (``node.lock``) is never included, and
of Reticulum's configuration only the active config file is included;
Reticulum's runtime storage is recreated on first run.

Both directions refuse to run while a node is serving on the data
directory (the single-instance lock), so a backup can never catch the
mailbox or the Tan database half-written, and a restore can never land
under a running node — with one exception: a backup taken *by* the
serving node itself (``live=True``) skips the lock, because the node's
own writes are atomic and each file it reads is a consistent snapshot.
The Tan database is captured with SQLite's
backup API, so committed entries still sitting in a write-ahead log are
included.  One pair is not atomic file-by-file, though: a scheduled
post publishes by creating the signed post in the Tan database and
then removing its queue entry, so a live backup that snapshots the
database before reading the queue could hold neither.  The backup
therefore holds the scheduled-posts lock from the member-list
decision through both captures, and the publish pass holds that same
lock across the create and the removal, so the pair is always one
consistent state.  The same holds for scheduled files: the backup
holds the scheduled-files lock from the member-list decision through
the archive, so the queue file and the staged bytes it names are one
consistent pair, and restore verifies each staged file against its
queue entry before installing anything.  Restore treats the archive as untrusted input: the manifest
must declare format 1, every member must be an allowlisted regular file
with no ``..`` segments, the archive must contain an identity, and
expanded sizes are bounded while streaming.

An encrypted backup (format 2, ``tau backup --passphrase``) wraps the
same gzipped tar in an envelope: a magic line, one JSON header line
(scrypt parameters, salt, IV, HMAC), then AES-256-CBC ciphertext with
PKCS#7 padding.  The key is derived with scrypt and split into an
encryption key and a MAC key; the HMAC covers IV + ciphertext, so a
wrong passphrase and a tampered archive both fail before anything is
decrypted.  The cipher is RNS's ``AES_256_CBC``, which dispatches on
RNS's active provider (its internal pure-Python AES or PyCA), so
encryption works on installs without the ``cryptography`` package; the
KDF, MAC, and padding are stdlib.  Both directions stream in
``_COPY_CHUNK`` pieces — the plaintext tar is built in a private 0700
scratch directory, never beside the destination — and restore verifies
the HMAC in a first pass before decrypting in a second, so no
unauthenticated plaintext is ever written out.  Restore auto-detects
the envelope, so a plaintext backup (format 1) restores exactly as
before.

This module must stay importable without Reticulum: only the identity
check during restore imports :mod:`tau.identity`.
"""

from __future__ import annotations

import hashlib
import hmac
import io
import json
import logging
import os
import shutil
import sqlite3
import tarfile
import tempfile
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

from . import __version__
from .config import (
    ARCHIVED_FILE_NAME,
    AUTO_BACKUPS_FILE_NAME,
    AWAY_FILE_NAME,
    CHANNELS_DB_FILE_NAME,
    DEVICES_FILE_NAME,
    DISAPPEARING_FILE_NAME,
    DM_REQUESTS_FILE_NAME,
    DRAFTS_FILE_NAME,
    FILES_DIR_NAME,
    MUTED_FILE_NAME,
    QUIET_HOURS_FILE_NAME,
    REMINDERS_FILE_NAME,
    SAVED_MESSAGES_FILE_NAME,
    SCHEDULED_FILES_DIR_NAME,
    SCHEDULED_FILES_FILE_NAME,
    SCHEDULED_POSTS_FILE_NAME,
    TauConfig,
)
from .instance_lock import BackupError, InstanceLock
from .util import atomic_write_json, ensure_dir, read_json

BACKUP_FORMAT_VERSION = 1
MANIFEST_NAME = "manifest.json"
SOCIAL_DB_NAME = "tan.sqlite3"
CHANNELS_DB_NAME = CHANNELS_DB_FILE_NAME
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
    CHANNELS_DB_NAME,
    "tan-rules.json",
    "allowlist.json",
    "ignored-groups.json",
    ARCHIVED_FILE_NAME,
    DISAPPEARING_FILE_NAME,
    DRAFTS_FILE_NAME,
    MUTED_FILE_NAME,
    QUIET_HOURS_FILE_NAME,
    AWAY_FILE_NAME,
    DM_REQUESTS_FILE_NAME,
    REMINDERS_FILE_NAME,
    SAVED_MESSAGES_FILE_NAME,
    SCHEDULED_POSTS_FILE_NAME,
    SCHEDULED_FILES_FILE_NAME,
    AUTO_BACKUPS_FILE_NAME,
    "groups.json",
    "blocklist.json",
    DEVICES_FILE_NAME,
    RNS_CONFIG_MEMBER,
)

# Files SQLite keeps beside a database.  A restored database must
# never be paired with a stale journal or WAL from the previous state.
_SQLITE_SIDECARS = ("-wal", "-shm", "-journal")

# Databases snapshotted through SQLite's backup API instead of copied
# raw, so committed WAL pages are part of the copy.
_SQLITE_DBS = (SOCIAL_DB_NAME, CHANNELS_DB_NAME)

# Bounds on untrusted archives, enforced while streaming.
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_MEMBER_BYTES = 4 * 1024**3
MAX_TOTAL_BYTES = 8 * 1024**3
_COPY_CHUNK = 1024 * 1024

# -- encrypted backups (format 2) -------------------------------------------
# Envelope layout: magic line, one JSON header line, raw ciphertext.
# ``{"kdf": "scrypt", "n": ..., "r": ..., "p": ..., "salt": "<hex>",
# "iv": "<hex>", "hmac": "<hex>"}`` — the HMAC is over IV + ciphertext,
# so the header's own fields are authenticated only through the salt/IV
# they carry; a header naming absurd scrypt parameters is rejected
# before any key derivation, so a hostile file cannot force a huge KDF.
ENCRYPTED_BACKUP_MAGIC = b"TAU-BACKUP-2\n"
ENCRYPTED_BACKUP_FORMAT_VERSION = 2
_BACKUP_KDF_N = 32768
_BACKUP_KDF_R = 8
_BACKUP_KDF_P = 1
_BACKUP_KDF_MAX_N = 2**20
_BACKUP_KDF_MAX_R = 64
_BACKUP_KDF_MAX_P = 64
_BACKUP_SALT_BYTES = 16
_BACKUP_IV_BYTES = 16
_BACKUP_KEY_BYTES = 32
MIN_BACKUP_PASSPHRASE_LENGTH = 8
# A gzipped tar that expands to MAX_TOTAL_BYTES is at most that large
# plus gzip overhead, so an encrypted file past this size can never
# restore — refuse it before streaming it twice (HMAC pass, decrypt pass).
_MAX_ENCRYPTED_BYTES = MAX_TOTAL_BYTES + 64 * 1024 * 1024


def read_passphrase_file(path: Path) -> str:
    """Read a backup passphrase from *path*, dropping one trailing newline.

    The file usually ends with a newline from the editor that wrote it;
    that newline is not part of the passphrase, but any other whitespace
    is — the value must match exactly what a prompt would have read.
    """
    try:
        content = Path(path).expanduser().read_text(encoding="utf-8")
    except OSError as exc:
        raise BackupError(f"cannot read passphrase file {path}: {exc}") from exc
    if content.endswith("\r\n"):
        content = content[:-2]
    elif content.endswith("\n"):
        content = content[:-1]
    return content


def _clean_passphrase(passphrase: object) -> str:
    """Validate a backup passphrase: a string of at least 8 characters.

    The passphrase is never stripped: leading or trailing spaces are
    part of it, so a prompt and a file holding the same characters
    agree exactly.
    """
    if not isinstance(passphrase, str) or not passphrase:
        raise BackupError("backup passphrase must not be empty")
    if len(passphrase) < MIN_BACKUP_PASSPHRASE_LENGTH:
        raise BackupError(
            "backup passphrase must be at least "
            f"{MIN_BACKUP_PASSPHRASE_LENGTH} characters"
        )
    return passphrase


def _derive_backup_keys(
    passphrase: str,
    salt: bytes,
    *,
    n: int = _BACKUP_KDF_N,
    r: int = _BACKUP_KDF_R,
    p: int = _BACKUP_KDF_P,
) -> Tuple[bytes, bytes]:
    """Split a 64-byte scrypt output into encryption and MAC keys.

    ``maxmem`` is named explicitly: n=32768, r=8 needs 32 MiB, and
    OpenSSL's default cap is exactly 32 MiB.
    """
    try:
        derived = hashlib.scrypt(
            passphrase.encode("utf-8"),
            salt=salt,
            n=n,
            r=r,
            p=p,
            dklen=2 * _BACKUP_KEY_BYTES,
            maxmem=64 * 1024 * 1024,
        )
    except ValueError as exc:
        # Parameters needing more than the 64 MiB budget — a hostile
        # header can name large ones, within the bounds below.
        raise BackupError("not a valid encrypted tau backup") from exc
    return derived[:_BACKUP_KEY_BYTES], derived[_BACKUP_KEY_BYTES:]


def _pkcs7_pad(data: bytes) -> bytes:
    pad = 16 - len(data) % 16
    return data + bytes([pad]) * pad


def _pkcs7_unpad(data: bytes) -> bytes:
    if not data or len(data) % 16:
        raise BackupError("wrong passphrase or the backup is corrupt")
    pad = data[-1]
    if pad < 1 or pad > 16 or data[-pad:] != bytes([pad]) * pad:
        raise BackupError("wrong passphrase or the backup is corrupt")
    return data[:-pad]


def _aes_encrypt(data: bytes, key: bytes, iv: bytes) -> bytes:
    """AES-256-CBC encrypt one block-aligned *data* (lazy RNS import).

    ``AES_256_CBC`` dispatches on RNS's active provider — its internal
    pure-Python AES or PyCA — so this works on installs without the
    ``cryptography`` package.  CBC chaining across calls is the
    caller's job: pass the last ciphertext block as the next *iv*.
    """
    try:
        from RNS.Cryptography.AES import AES_256_CBC
    except ImportError as exc:
        raise BackupError(
            "encrypted backups need RNS with AES support; "
            "this Reticulum install has none"
        ) from exc
    try:
        return AES_256_CBC.encrypt(data, key, iv)
    except ValueError as exc:
        raise BackupError("wrong passphrase or the backup is corrupt") from exc


def _aes_decrypt(data: bytes, key: bytes, iv: bytes) -> bytes:
    """AES-256-CBC decrypt one block-aligned *data* (see :func:`_aes_encrypt`)."""
    try:
        from RNS.Cryptography.AES import AES_256_CBC
    except ImportError as exc:
        raise BackupError(
            "encrypted backups need RNS with AES support; "
            "this Reticulum install has none"
        ) from exc
    try:
        return AES_256_CBC.decrypt(data, key, iv)
    except ValueError as exc:
        raise BackupError("wrong passphrase or the backup is corrupt") from exc


# A header line is ~200 bytes; anything past this is not a header.
_MAX_ENCRYPTED_HEADER_BYTES = 4096


def _validate_encrypted_header(
    header: object,
) -> Tuple[int, int, int, bytes, bytes, bytes]:
    """Return ``(n, r, p, salt, iv, tag)`` for a parsed header dict.

    The KDF fields must be real integers — JSON numbers like ``1e999``
    parse to floats (``int(float('inf'))`` raises ``OverflowError``)
    and booleans are ints too, so both are rejected here, before any
    key derivation.  A header naming absurd scrypt parameters is
    rejected before derivation as well.
    """
    if not isinstance(header, dict):
        raise BackupError("not a valid encrypted tau backup")
    if header.get("format") != ENCRYPTED_BACKUP_FORMAT_VERSION:
        raise BackupError(
            "unsupported encrypted backup format "
            f"{header.get('format')!r} (expected {ENCRYPTED_BACKUP_FORMAT_VERSION})"
        )
    if header.get("kdf") != "scrypt":
        raise BackupError(
            f"unsupported backup key derivation {header.get('kdf')!r}"
        )
    for field in ("n", "r", "p"):
        value = header.get(field)
        if isinstance(value, bool) or not isinstance(value, int):
            raise BackupError("not a valid encrypted tau backup")
    try:
        salt = bytes.fromhex(header["salt"])
        iv = bytes.fromhex(header["iv"])
        tag = bytes.fromhex(header["hmac"])
    except (KeyError, TypeError, ValueError) as exc:
        raise BackupError("not a valid encrypted tau backup") from exc
    n = header["n"]
    r = header["r"]
    p = header["p"]
    if (
        n < 1
        or n > _BACKUP_KDF_MAX_N
        or r < 1
        or r > _BACKUP_KDF_MAX_R
        or p < 1
        or p > _BACKUP_KDF_MAX_P
        or len(salt) != _BACKUP_SALT_BYTES
        or len(iv) != _BACKUP_IV_BYTES
        or len(tag) != hashlib.sha256().digest_size
    ):
        raise BackupError("not a valid encrypted tau backup")
    return n, r, p, salt, iv, tag


def _read_encrypted_header(
    handle: Any,
) -> Tuple[Tuple[int, int, int, bytes, bytes, bytes], int]:
    """Read the magic + header line from *handle* at offset 0.

    Returns ``(fields, ciphertext_offset)`` — the validated
    ``(n, r, p, salt, iv, tag)`` tuple and the byte offset where the
    ciphertext starts.
    """
    magic = handle.read(len(ENCRYPTED_BACKUP_MAGIC))
    if magic != ENCRYPTED_BACKUP_MAGIC:
        raise BackupError("not an encrypted tau backup")
    line = handle.readline(_MAX_ENCRYPTED_HEADER_BYTES + 1)
    if not line.endswith(b"\n") or len(line) > _MAX_ENCRYPTED_HEADER_BYTES:
        raise BackupError("not a valid encrypted tau backup")
    try:
        header = json.loads(line)
    except ValueError as exc:
        raise BackupError("not a valid encrypted tau backup") from exc
    fields = _validate_encrypted_header(header)
    return fields, len(ENCRYPTED_BACKUP_MAGIC) + len(line)


def _encrypt_backup_stream(src: Any, dst: Any, passphrase: str) -> Dict[str, object]:
    """Encrypt *src* to *dst* in ``_COPY_CHUNK`` pieces; return the header.

    Only chunk-sized buffers are held: the plaintext is never loaded
    whole.  The HMAC covers the IV plus each ciphertext chunk as it is
    written, and the returned header (with the HMAC) is what the caller
    writes ahead of the ciphertext.
    """
    passphrase = _clean_passphrase(passphrase)
    salt = os.urandom(_BACKUP_SALT_BYTES)
    iv = os.urandom(_BACKUP_IV_BYTES)
    enc_key, mac_key = _derive_backup_keys(passphrase, salt)
    mac = hmac.new(mac_key, iv, hashlib.sha256)
    previous = iv
    pending = b""
    while True:
        chunk = src.read(_COPY_CHUNK)
        if chunk:
            pending += chunk
            # Encrypt all complete blocks except the last one, so the
            # final chunk always has room for its padding.
            cut = (len(pending) - 16) // 16 * 16
            if cut > 0:
                ciphertext = _aes_encrypt(pending[:cut], enc_key, previous)
                previous = ciphertext[-16:]
                dst.write(ciphertext)
                mac.update(ciphertext)
                pending = pending[cut:]
        else:
            ciphertext = _aes_encrypt(_pkcs7_pad(pending), enc_key, previous)
            dst.write(ciphertext)
            mac.update(ciphertext)
            break
    return {
        "format": ENCRYPTED_BACKUP_FORMAT_VERSION,
        "kdf": "scrypt",
        "n": _BACKUP_KDF_N,
        "r": _BACKUP_KDF_R,
        "p": _BACKUP_KDF_P,
        "salt": salt.hex(),
        "iv": iv.hex(),
        "hmac": mac.hexdigest(),
    }


def _decrypt_backup_stream(
    src: Any,
    dst: Any,
    passphrase: str,
    fields: Tuple[int, int, int, bytes, bytes, bytes],
    ciphertext_offset: int,
) -> None:
    """Verify the HMAC, then decrypt *src* to *dst* (both chunked).

    Two passes over the ciphertext: the first authenticates it, the
    second decrypts it, so no unauthenticated plaintext is ever
    written out.  *src* must be seekable and positioned at the
    ciphertext start for the first pass.
    """
    n, r, p, salt, iv, tag = fields
    passphrase = _clean_passphrase(passphrase)
    enc_key, mac_key = _derive_backup_keys(passphrase, salt, n=n, r=r, p=p)
    mac = hmac.new(mac_key, iv, hashlib.sha256)
    while True:
        chunk = src.read(_COPY_CHUNK)
        if not chunk:
            break
        mac.update(chunk)
    if not hmac.compare_digest(mac.digest(), tag):
        raise BackupError("wrong passphrase or the backup is corrupt")
    src.seek(ciphertext_offset)
    previous = iv
    pending = b""
    while True:
        chunk = src.read(_COPY_CHUNK)
        if not chunk:
            break
        if len(chunk) % 16:
            raise BackupError("wrong passphrase or the backup is corrupt")
        plaintext = _aes_decrypt(chunk, enc_key, previous)
        previous = chunk[-16:]
        pending += plaintext
        # Hold the final block back so its padding is stripped, not written.
        dst.write(pending[:-16])
        pending = pending[-16:]
    dst.write(_pkcs7_unpad(pending))


def _encrypt_backup_file(
    source: Path, target: Path, passphrase: str
) -> Dict[str, object]:
    """Stream-encrypt the file at *source* to *target*; return the header."""
    with open(source, "rb") as src, open(target, "wb") as dst:
        return _encrypt_backup_stream(src, dst, passphrase)


def _decrypt_backup_file(source: Path, target: Path, passphrase: str) -> None:
    """Stream-decrypt the envelope at *source* to *target*.

    Raises :class:`BackupError` on a wrong passphrase, a tampered
    archive, or a malformed envelope — and writes nothing to *target*
    in that case beyond an empty or partial file the caller discards.
    """
    with open(source, "rb") as src:
        fields, offset = _read_encrypted_header(src)
        with open(target, "wb") as dst:
            _decrypt_backup_stream(src, dst, passphrase, fields, offset)


def encrypt_backup_archive(plaintext: bytes, passphrase: str) -> bytes:
    """Wrap a gzipped-tar *plaintext* in the encrypted-backup envelope.

    Holds the archive in memory — for real backups use the streaming
    file helpers through :func:`create_backup` instead.
    """
    src = io.BytesIO(plaintext)
    dst = io.BytesIO()
    header = _encrypt_backup_stream(src, dst, passphrase)
    return (
        ENCRYPTED_BACKUP_MAGIC
        + (json.dumps(header, sort_keys=True) + "\n").encode("utf-8")
        + dst.getvalue()
    )


def decrypt_backup_archive(data: bytes, passphrase: str) -> bytes:
    """Unwrap an encrypted-backup envelope; raises :class:`BackupError`.

    A wrong passphrase and a tampered archive fail alike, and both fail
    before any plaintext is returned — the HMAC is verified before the
    ciphertext is decrypted.  Holds the archive in memory; for real
    backups use the streaming file helpers through :func:`restore_backup`
    instead.
    """
    src = io.BytesIO(data)
    fields, offset = _read_encrypted_header(src)
    dst = io.BytesIO()
    _decrypt_backup_stream(src, dst, passphrase, fields, offset)
    return dst.getvalue()


def is_encrypted_backup(path: Path) -> bool:
    """True when *path* starts with the encrypted-backup magic line."""
    try:
        with open(Path(path).expanduser(), "rb") as handle:
            return handle.read(len(ENCRYPTED_BACKUP_MAGIC)) == (
                ENCRYPTED_BACKUP_MAGIC
            )
    except OSError:
        return False

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


def _is_scheduled_file_member(name: str) -> bool:
    """True when *name* is a staged scheduled-file member:
    ``scheduled-files/<id>/<name>``.

    Staged files are always exactly two levels under
    ``scheduled-files/`` — the id directory is the queue id (16 hex
    characters) and the file name is the bare name the queue entry
    carries — so anything shallower, deeper, or naming ``.`` / ``..``
    is not a member this module ever writes and is rejected as
    unexpected on restore.
    """
    parts = Path(name).parts
    if len(parts) != 3 or parts[0] != SCHEDULED_FILES_DIR_NAME:
        return False
    queue_id, file_name = parts[1], parts[2]
    if len(queue_id) != 16 or file_name in ("", ".", ".."):
        return False
    try:
        bytes.fromhex(queue_id)
    except ValueError:
        return False
    return True


def _scheduled_files(config: TauConfig) -> List[str]:
    """Member names of the staged files under the scheduled-files directory.

    Symlinks are never followed: a symlinked ``scheduled-files/``
    directory, a symlinked id directory, or a symlinked file is
    skipped, so a backup can never smuggle in a path outside the data
    directory.  A directory that cannot be listed is an error, never
    an empty result, so a backup cannot silently omit staged files.
    """
    staged_dir = config.scheduled_files_dir
    if staged_dir.is_symlink() or not staged_dir.is_dir():
        return []
    members: List[str] = []
    try:
        id_dirs = sorted(staged_dir.iterdir(), key=lambda path: path.name)
    except OSError as exc:
        raise BackupError(
            f"could not list staged files in {staged_dir}: {exc}"
        ) from exc
    for id_dir in id_dirs:
        if id_dir.is_symlink() or not id_dir.is_dir():
            continue
        try:
            entries = sorted(id_dir.iterdir(), key=lambda path: path.name)
        except OSError as exc:
            raise BackupError(
                f"could not list staged files in {id_dir}: {exc}"
            ) from exc
        for entry in entries:
            if entry.is_symlink() or not entry.is_file():
                continue
            members.append(
                f"{SCHEDULED_FILES_DIR_NAME}/{id_dir.name}/{entry.name}"
            )
    return members


def _owned_scheduled_files(config: TauConfig) -> List[str]:
    """Staged scheduled-file members owned by a queue entry.

    Like :func:`_scheduled_files`, but only members the queue owns:
    ``scheduled-files/<id>/<name>`` for entries whose staged file
    exists as a regular file and is not a symlink.  Orphaned staged
    bytes — a crash between staging and the queue commit, a failed
    removal, a leftover ``.tmp-`` file — are skipped and logged, so a
    backup made while such bytes exist is still restorable: the
    restore side refuses any staged member without a queue entry, so
    archiving an orphan would make the whole archive unrestorable.
    Callers must hold the scheduled-files lock, so the queue and the
    staged bytes cannot drift between the load and the listing.  A
    corrupt queue fails the backup loudly instead of silently
    omitting scheduled files from it.
    """
    from .scheduled_files import ScheduledFileError, load_scheduled_files

    try:
        queue = load_scheduled_files(config.scheduled_files_path)
    except ScheduledFileError as exc:
        raise BackupError(
            f"not a valid tau backup: scheduled-files queue is invalid: {exc}"
        ) from exc
    expected = {
        f"{SCHEDULED_FILES_DIR_NAME}/{entry.id}/{entry.file['name']}"
        for entry in queue.files
    }
    members: List[str] = []
    for name in _scheduled_files(config):
        if name in expected:
            members.append(name)
        else:
            log.warning(
                "Skipping orphaned staged scheduled file %s in backup: "
                "no queue entry owns it",
                name,
            )
    return members


def _destinations(
    config: TauConfig, received: List[str] = (), scheduled: List[str] = ()
) -> Dict[str, Path]:
    """Live location of every allowlisted archive member."""
    paths = {name: config.data_dir / name for name in BACKUP_FILES}
    paths.update({name: config.data_dir / name for name in received})
    paths.update({name: config.data_dir / name for name in scheduled})
    paths[RNS_CONFIG_MEMBER] = config.rns_config_file
    return paths


def _state_paths(destinations: Dict[str, Path]) -> Dict[str, Path]:
    """Every live node-state path, labelled, including SQLite sidecars."""
    state = dict(destinations)
    for db_name in _SQLITE_DBS:
        db = destinations[db_name]
        for suffix in _SQLITE_SIDECARS:
            state[f"{db_name}{suffix}"] = Path(f"{db}{suffix}")
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
    config: TauConfig,
    target: Path,
    *,
    force: bool = False,
    live: bool = False,
    passphrase: Optional[str] = None,
) -> Tuple[Path, Dict[str, object]]:
    """Write a backup of *config*'s data directory to *target*.

    Returns ``(target, manifest)``.  Raises :class:`BackupError` when
    there is no node to back up, the target already exists without
    ``--force``, the target sits inside the data directory, or a node
    is serving on the data directory.

    Pass ``live=True`` only when the caller *is* the serving node (the
    web console bridge): the instance lock is then skipped, because the
    serving node's own writes are atomic (temp file + rename), so each
    state file the backup reads is a consistent snapshot, and the Tan
    database is captured through SQLite's backup API.  Any other
    caller must leave it off, so a node that starts mid-backup cannot
    mutate the mailbox or the Tan database under the archive.

    Pass ``passphrase`` to wrap the archive in the encrypted-backup
    envelope (format 2) instead of writing the plaintext tar.  The
    plaintext tar is then built in a private 0700 scratch directory —
    never beside the destination, where a sync client could copy it —
    and streamed through the encryptor into a fresh destination-side
    temp file, which is renamed onto *target* atomically.  Only
    chunk-sized buffers are held in memory either way.
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
    if passphrase is not None:
        # Validate before any work — a short passphrase must fail
        # here, not after the whole archive has been built.
        _clean_passphrase(passphrase)

    destinations = _destinations(config)

    # Hold the instance lock while the archive is written: a node that
    # starts mid-backup would mutate the mailbox and the Tan database
    # under us, and a node already serving means the on-disk state is
    # moving.  The lock file itself is never a backup member.  A live
    # backup skips the lock: the caller is the serving node, whose own
    # writes are atomic, so each file read is a consistent snapshot.
    lock = InstanceLock(config.instance_lock_path, config.data_dir)
    if not live:
        lock.acquire()
    try:
        # Hold the scheduled-posts lock from the member-list
        # decision through the Tan database snapshot and the queue
        # capture, so a post that publishes mid-backup cannot land
        # in neither file: the publish pass holds this same lock
        # across creating the signed post and removing the queue
        # entry, so the pair the archive holds is always one
        # consistent state — either the post is still queued, or
        # it is in the database snapshot.  (Lazy import: this module
        # stays importable without Reticulum.)
        from .scheduled_files import scheduled_files_lock
        from .scheduled_posts import scheduled_posts_lock

        with (
            scheduled_posts_lock(config.scheduled_posts_path),
            scheduled_files_lock(config.scheduled_files_path),
        ):
            # Enumerated under the locks, so the member list cannot
            # drift from the files actually archived below.  The
            # scheduled-files lock is held from the member-list
            # decision through the archive, so no schedule, cancel,
            # or send claim can interleave: the queue file and the
            # staged bytes the archive holds are one consistent pair.
            # (A file that finishes sending mid-backup still fails
            # the backup with "disappeared ... try again", like any
            # other state file that vanishes under the archive.)
            received = _received_files(config)
            # Only entry-owned staged files: orphaned bytes (a crash
            # between staging and the queue commit, a failed removal,
            # a leftover .tmp- file) would make the archive
            # unrestorable, because the restore refuses any staged
            # member without a queue entry.
            scheduled = _owned_scheduled_files(config)
            destinations.update(
                {name: config.data_dir / name for name in received}
            )
            destinations.update(
                {name: config.data_dir / name for name in scheduled}
            )
            files = [name for name in BACKUP_FILES if destinations[name].is_file()]
            files.extend(received)
            files.extend(scheduled)
            # A symlinked state file would be archived as a symlink member,
            # which restore rejects as not a regular file — so a backup of
            # a node with linked state would succeed and then never
            # restore.  Refuse loudly instead.  (The SQLite databases are
            # the exception: they are snapshotted to fresh regular files,
            # so a link there is harmless.)
            for name in files:
                if name not in _SQLITE_DBS and destinations[name].is_symlink():
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
            enc_scratch = None
            tmp_name = None
            if passphrase is not None:
                # The plaintext tar never sits beside the destination:
                # build it in a private 0700 scratch directory and
                # encrypt from there into a fresh destination-side temp
                # file.  A sync client watching the destination folder
                # can never see the plaintext, and an interruption
                # leaves no named plaintext archive behind.
                enc_scratch = tempfile.TemporaryDirectory(prefix="tau-backup-")
                plain_path = Path(enc_scratch.name) / "backup.tar.gz"
                tar_handle = open(plain_path, "wb")
            else:
                fd, tmp_name = tempfile.mkstemp(
                    dir=str(parent), prefix=f".{target.name}.", suffix=".tmp"
                )
                tar_handle = os.fdopen(fd, "wb")
            try:
                with tar_handle as handle:
                    with tarfile.open(fileobj=handle, mode="w:gz") as archive:
                        info = tarfile.TarInfo(MANIFEST_NAME)
                        info.size = len(manifest_bytes)
                        info.mtime = int(datetime.now(timezone.utc).timestamp())
                        archive.addfile(info, io.BytesIO(manifest_bytes))
                        with tempfile.TemporaryDirectory() as scratch:
                            expanded = 0
                            for name in files:
                                source = destinations[name]
                                if name in _SQLITE_DBS:
                                    source = _snapshot_sqlite(
                                        source, Path(scratch) / name
                                    )
                                # Size and bytes come from one open descriptor:
                                # a live node may atomically replace the file
                                # between a pathname stat and the open.  The
                                # size is read off that same descriptor, so the
                                # restore bounds below are enforced on the bytes
                                # actually archived, keeping a successful backup
                                # restorable.
                                try:
                                    handle_in = open(source, "rb")
                                except FileNotFoundError as exc:
                                    raise BackupError(
                                        f"{name} disappeared while the backup was "
                                        "being written; try again"
                                    ) from exc
                                with handle_in:
                                    info = archive.gettarinfo(
                                        arcname=name, fileobj=handle_in
                                    )
                                    # A hard link to a file already in the
                                    # archive comes back as a link member,
                                    # which restore rejects — store the bytes
                                    # instead.  Only hard links are rewritten:
                                    # a symlink member is still archived as a
                                    # link and rejected on restore, as before.
                                    # gettarinfo reports size 0 for a link, so
                                    # the size comes from the open descriptor.
                                    if info.islnk():
                                        info.type = tarfile.REGTYPE
                                        info.linkname = ""
                                        info.size = os.fstat(
                                            handle_in.fileno()
                                        ).st_size
                                    expanded += info.size
                                    if (
                                        info.size > MAX_MEMBER_BYTES
                                        or expanded > MAX_TOTAL_BYTES
                                    ):
                                        raise BackupError(
                                            f"cannot back up {name!r}: the data directory "
                                            "exceeds the restore size limit"
                                        )
                                    archive.addfile(info, handle_in)
                    handle.flush()
                    os.fsync(handle.fileno())
                if passphrase is not None:
                    cipher_path = Path(enc_scratch.name) / "backup.tar.gz.enc"
                    header = _encrypt_backup_file(
                        plain_path, cipher_path, passphrase
                    )
                    fd, tmp_name = tempfile.mkstemp(
                        dir=str(parent), prefix=f".{target.name}.", suffix=".tmp"
                    )
                    try:
                        # Written through the retained descriptor, never
                        # by reopening the pathname: another process
                        # cannot swap the destination under the write.
                        with os.fdopen(fd, "wb") as handle:
                            handle.write(ENCRYPTED_BACKUP_MAGIC)
                            handle.write(
                                (json.dumps(header, sort_keys=True) + "\n").encode(
                                    "utf-8"
                                )
                            )
                            with open(cipher_path, "rb") as cipher:
                                shutil.copyfileobj(cipher, handle, _COPY_CHUNK)
                            handle.flush()
                            os.fsync(handle.fileno())
                    except BaseException:
                        try:
                            os.unlink(tmp_name)
                        except OSError:
                            pass
                        raise
                os.chmod(tmp_name, 0o600)
                os.replace(tmp_name, target)
            except BaseException:
                if tmp_name is not None:
                    try:
                        os.unlink(tmp_name)
                    except OSError:
                        pass
                raise
            finally:
                if enc_scratch is not None:
                    enc_scratch.cleanup()
    finally:
        if not live:
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
            if not _is_scheduled_file_member(name):
                raise BackupError(
                    f"not a tau backup: unexpected member {name!r}"
                )
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


def _check_scheduled_files_dirs(config: TauConfig, scheduled: List[str]) -> None:
    """Refuse scheduled-file restores whose parent directories are not real.

    Each ``scheduled-files/<id>`` member is written beneath
    ``scheduled-files/`` and its id directory; a symlink there would
    redirect the write outside the data directory.  Called under the
    instance lock, before staging.
    """
    staged_dir = config.data_dir / SCHEDULED_FILES_DIR_NAME
    if not scheduled:
        return
    paths = [staged_dir] + [
        staged_dir / Path(name).parts[1] for name in scheduled
    ]
    for path in dict.fromkeys(paths):
        if os.path.lexists(path) and (path.is_symlink() or not path.is_dir()):
            raise BackupError(
                f"refusing to restore scheduled files: {path} is a "
                "symlink or not a directory"
            )


def _verify_scheduled_files(
    staged: Dict[Path, Path], destinations: Dict[str, Path], files: List[str]
) -> None:
    """Check the staged scheduled-files queue against its staged bytes.

    Every queue entry's bytes must be in the archive and hash to the
    descriptor the entry carries, and every staged member must belong
    to a queue entry — otherwise the restore would hand the send pass
    an entry it can never deliver, or orphaned bytes no entry can
    manage.  Called after staging, before anything live is replaced.
    A missing queue file means no scheduled files were backed up, so
    staged members on their own are refused as orphans.
    """
    from .scheduled_files import load_scheduled_files

    members = set(files)
    staged_members = {
        name for name in files if _is_scheduled_file_member(name)
    }
    if SCHEDULED_FILES_FILE_NAME not in files:
        if staged_members:
            raise BackupError(
                "not a valid tau backup: staged scheduled files without "
                "their queue"
            )
        return
    queue_path = staged[destinations[SCHEDULED_FILES_FILE_NAME]]
    try:
        queue = load_scheduled_files(queue_path)
    except Exception as exc:
        raise BackupError(
            f"not a valid tau backup: scheduled-files queue is invalid: {exc}"
        ) from exc
    expected = set()
    for entry in queue.files:
        member = (
            f"{SCHEDULED_FILES_DIR_NAME}/{entry.id}/{entry.file['name']}"
        )
        expected.add(member)
        if member not in members:
            raise BackupError(
                "not a valid tau backup: scheduled file "
                f"{entry.id} has no staged bytes in the archive"
            )
        staged_bytes = staged[destinations[member]].read_bytes()
        if (
            len(staged_bytes) != entry.file["size"]
            or hashlib.sha256(staged_bytes).hexdigest()
            != entry.file["sha256"]
        ):
            raise BackupError(
                "not a valid tau backup: staged bytes for scheduled "
                f"file {entry.id} do not match its queue entry"
            )
    orphans = staged_members - expected
    if orphans:
        raise BackupError(
            "not a valid tau backup: staged scheduled files without a "
            f"queue entry: {sorted(orphans)}"
        )


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
    config: TauConfig,
    source: Path,
    *,
    force: bool = False,
    passphrase: Optional[str] = None,
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

    An encrypted backup (format 2) is detected by its magic line and
    streamed through the decryptor into a private scratch file before
    the tar is opened — chunk by chunk, never held whole in memory;
    *passphrase* is required for one and ignored for a plaintext backup.
    """
    # Identity locking is the only RNS-dependent helper the restore needs.
    from .identity import _identity_lock

    source = Path(source).expanduser()
    if not source.is_file():
        raise BackupError(f"no backup at {source}")

    # The decrypted plaintext lives in a private scratch directory for
    # exactly as long as the archive is open; a failed or interrupted
    # restore removes it with the directory.
    dec_scratch = None
    try:
        if is_encrypted_backup(source):
            if passphrase is None:
                raise BackupError(
                    "this backup is encrypted; pass its passphrase with "
                    "--passphrase or --passphrase-file"
                )
            try:
                size = source.stat().st_size
            except OSError as exc:
                raise BackupError(f"not a valid tau backup: {exc}") from exc
            if size > _MAX_ENCRYPTED_BYTES:
                raise BackupError(
                    "not a valid tau backup: encrypted backup is too large"
                )
            dec_scratch = tempfile.TemporaryDirectory(prefix="tau-backup-")
            plain_path = Path(dec_scratch.name) / "backup.tar.gz"
            try:
                _decrypt_backup_file(source, plain_path, passphrase)
            except BackupError:
                raise
            except OSError as exc:
                raise BackupError(f"not a valid tau backup: {exc}") from exc
            try:
                archive = tarfile.open(plain_path, mode="r:gz")
            except (tarfile.TarError, OSError) as exc:
                raise BackupError(f"not a valid tau backup: {exc}") from exc
        else:
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
                config,
                [name for name in files if _is_received_file_member(name)],
                [name for name in files if _is_scheduled_file_member(name)],
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
                _check_scheduled_files_dirs(
                    config,
                    [name for name in files if _is_scheduled_file_member(name)],
                )
                # Received files on disk that the archive does not hold are
                # stale state too: a --force restore removes them, like any
                # other included file.  Scanned under the lock so a file a
                # serving node saves before the lock is taken cannot survive.
                for name in _received_files(config):
                    state.setdefault(name, config.data_dir / name)
                # Same for staged scheduled files: a --force restore must
                # not leave bytes behind that no restored queue entry owns.
                for name in _scheduled_files(config):
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
                        _verify_scheduled_files(staged, destinations, files)
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
    finally:
        if dec_scratch is not None:
            dec_scratch.cleanup()

