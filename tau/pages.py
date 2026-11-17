"""Signed personal mesh pages — a small site served over RNS.

A page is a signed document its author publishes about anything: a
meetup notice, a project's status, a file index.  It travels over a
link (``/tau/page``) when a peer asks for it, so the tiny announce
budget stays tiny — and unlike a Tan post it is persistent and
browsable: only the newest signed version per author is kept.

The document is signed exactly like a Tan entry and a public
profile: the author hashes the canonical unsigned body and signs the
digest with their identity.  A receiver verifies the signature
against the public key the page carries, and checks that key derives
the author's destination hash, so a page can only be published by
the peer it describes.  Only the newest signed version per author
is kept; older or invalidly-signed versions are dropped.

Fetching is gated by a NomadNet-style allow-list (``page-allowed.json``):
with no list file the page is public; once the list is enabled, only
listed destination hashes may fetch it.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import RNS

from .identity import load_public_key
from .protocol import APP_NAME, ASPECT
from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

PAGE_VERSION = 1
# A page is a small Markdown document: 8 KiB of UTF-8 is a notice
# board, a meetup page or a status page, and it still fits the 64 KiB
# response budget with the signature and key beside it.
MAX_PAGE_BYTES = 8 * 1024
MAX_PAGE_TITLE_CHARS = 200
SIG_HEX_LENGTH = 128
KEY_HEX_LENGTH = 128
AUTHOR_HEX_LENGTH = 32

_HEX32 = re.compile(r"^[0-9a-fA-F]{32}$")
_HEX64 = re.compile(r"^[0-9a-fA-F]{64}$")
_HEX128 = re.compile(r"^[0-9a-fA-F]{128}$")


class PageError(ValueError):
    """Raised when a page document is malformed or fails verification."""


def clean_title(value: Any) -> str:
    """Normalise a page title: single-line text, at most 200 chars.

    An absent or blank title is ``""`` (the page renders untitled);
    anything longer raises.
    """
    if value is None:
        return ""
    if not isinstance(value, str):
        raise PageError("title must be a string")
    cleaned = "".join(
        char for char in value if char.isprintable() or char.isspace()
    )
    title = " ".join(cleaned.split())
    if len(title) > MAX_PAGE_TITLE_CHARS:
        raise PageError(
            f"title is longer than {MAX_PAGE_TITLE_CHARS} characters "
            f"({len(title)} given)"
        )
    return title


def clean_markdown(value: Any) -> str:
    """Validate page Markdown: non-blank text within the 8 KiB budget.

    The text is kept verbatim — Markdown is whitespace-sensitive, so
    unlike a title it is never collapsed — only its size and shape
    are checked.
    """
    if not isinstance(value, str):
        raise PageError("markdown must be a string")
    if not value.strip():
        raise PageError("markdown must not be blank")
    if "\x00" in value:
        raise PageError("markdown must not contain NUL bytes")
    size = len(value.encode("utf-8"))
    if size > MAX_PAGE_BYTES:
        raise PageError(
            f"page is larger than {MAX_PAGE_BYTES} bytes ({size} given)"
        )
    return value


def title_from_markdown(markdown: str) -> str:
    """The page title from its first ``# `` heading, or ``""``.

    A publish without ``--title`` takes the document's own heading,
    so a hand-written file needs no extra flag to be titled.
    """
    for line in markdown.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            return clean_title(stripped[2:])
    return ""


def _require_hex(value: Any, length: int, field: str) -> str:
    if not isinstance(value, str):
        raise PageError(f"{field} must be a hex string")
    text = value.strip().lower()
    if len(text) != length:
        raise PageError(f"{field} must be {length} hex characters")
    try:
        bytes.fromhex(text)
    except ValueError as exc:
        raise PageError(f"{field} is not valid hex") from exc
    return text


def _require_timestamp(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PageError(f"{field} must be a number")
    timestamp = float(value)
    if not math.isfinite(timestamp) or timestamp < 0:
        raise PageError(f"{field} must be a finite, non-negative timestamp")
    return timestamp


def _unsigned_fields(doc: Dict[str, Any]) -> Dict[str, Any]:
    """The fields covered by the signature, in canonical form."""
    return {
        "v": doc["v"],
        "author": doc["author"],
        "key": doc["key"],
        "title": doc["title"],
        "markdown": doc["markdown"],
        "version": doc["version"],
        "updated_at": doc["updated_at"],
    }


def canonical_page_bytes(doc: Dict[str, Any]) -> bytes:
    """Canonical bytes of a page's unsigned body (what is signed)."""
    return json.dumps(
        _unsigned_fields(doc),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def page_digest(doc: Dict[str, Any]) -> str:
    """SHA-256 of the canonical unsigned body, as lowercase hex."""
    return hashlib.sha256(canonical_page_bytes(doc)).hexdigest()


def normalize_page(doc: Any, *, now: Optional[float] = None) -> Dict[str, Any]:
    """Validate the unsigned part of a page document.

    Returns the normalised document without its signature; the
    signature itself is checked by :func:`verify_page`.  Extra
    fields are ignored, so a page stays verifiable when a newer
    version adds one — only the signed fields matter.
    """
    if not isinstance(doc, dict):
        raise PageError("page must be a JSON object")

    version_field = doc.get("v", PAGE_VERSION)
    if isinstance(version_field, bool) or version_field != PAGE_VERSION:
        raise PageError(f"unsupported page version: {version_field!r}")

    author = _require_hex(doc.get("author"), AUTHOR_HEX_LENGTH, "author")
    key = _require_hex(doc.get("key"), KEY_HEX_LENGTH, "key")

    title = clean_title(doc.get("title", ""))
    markdown = clean_markdown(doc.get("markdown", ""))

    version = doc.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise PageError("page version must be an integer of at least 1")

    updated_at = _require_timestamp(doc.get("updated_at"), "updated_at")
    if now is not None and updated_at > float(now) + 86400.0:
        raise PageError("page timestamp is too far in the future")

    return {
        "v": PAGE_VERSION,
        "author": author,
        "key": key,
        "title": title,
        "markdown": markdown,
        "version": version,
        "updated_at": updated_at,
    }


def build_page(
    identity: RNS.Identity,
    *,
    title: str,
    markdown: str,
    version: int = 1,
    updated_at: Optional[float] = None,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Build and sign a page document owned by *identity*.

    The signature covers the canonical unsigned body, exactly like a
    Tan entry: the digest is signed, so any change to a field breaks
    verification.  *version* is the author's monotonic revision
    number — callers bump it on every publish, and receivers keep
    only the newest version they have seen.
    """
    timestamp = round(float(updated_at if updated_at is not None else time.time()), 3)
    unsigned = normalize_page(
        {
            "v": PAGE_VERSION,
            "author": RNS.Destination.hash(identity, APP_NAME, ASPECT).hex(),
            "key": identity.get_public_key().hex(),
            "title": title,
            "markdown": markdown,
            "version": version,
            "updated_at": timestamp,
        },
        now=now,
    )
    digest = page_digest(unsigned)
    doc = dict(unsigned)
    doc["sig"] = identity.sign(bytes.fromhex(digest)).hex()
    return doc


def verify_page(doc: Any, *, now: Optional[float] = None) -> Dict[str, Any]:
    """Validate a complete page, including its signature and key.

    Returns the normalised document.  A page whose key does not
    derive its author, or whose signature does not cover its body,
    is rejected — a peer cannot publish a page for anyone else.
    """
    unsigned = normalize_page(doc, now=now)
    digest = page_digest(unsigned)

    signature = _require_hex(doc.get("sig"), SIG_HEX_LENGTH, "sig")
    unsigned["sig"] = signature

    try:
        identity = RNS.Identity(create_keys=False)
        if not load_public_key(identity, bytes.fromhex(unsigned["key"])):
            raise ValueError("could not load public key")
    except Exception as exc:  # pragma: no cover - depends on RNS internals
        raise PageError(f"invalid public key: {exc}") from exc
    author = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    if author != unsigned["author"]:
        raise PageError("public key does not match the page author")
    try:
        valid = identity.validate(bytes.fromhex(signature), bytes.fromhex(digest))
    except Exception as exc:  # pragma: no cover - depends on RNS internals
        raise PageError(f"could not check signature: {exc}") from exc
    if not valid:
        raise PageError("page signature is invalid")
    return unsigned


def public_page(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The display view of a page: everything except key and signature.

    The key and signature are public by design (the key is announced),
    but the console and CLI JSON only need the page itself, so they
    stay out of display payloads.
    """
    if doc is None:
        return None
    return {
        "author": doc["author"],
        "title": doc["title"],
        "markdown": doc["markdown"],
        "version": doc["version"],
        "updated_at": doc["updated_at"],
    }


def load_local_page(path: Optional[Union[str, Path]]) -> Optional[Dict[str, Any]]:
    """Load and verify the local page, or ``None`` when absent/invalid.

    An invalid file reads as no page (with a warning), never as a
    crash: a hand-edited or half-written file must not stop the node
    from serving.
    """
    if path is None:
        return None
    raw = read_json(Path(path).expanduser())
    if raw is None:
        return None
    try:
        return verify_page(raw)
    except PageError as exc:
        log.warning("Ignoring invalid local page at %s: %s", path, exc)
        return None


def save_local_page(path: Union[str, Path], doc: Dict[str, Any]) -> Path:
    """Persist a signed local page atomically, owner-only."""
    target = Path(path).expanduser()
    atomic_write_json(target, doc, mode=0o600)
    return target


class PageStore:
    """Received peer pages, keyed by author, newest version only.

    Persisted as one JSON file next to the peer registry.  Only the
    newest signed version per author is kept — an older or
    invalidly-signed document is dropped on arrival, and a
    same-version document never replaces the one already stored, so
    two copies of one version cannot fight over the record.
    """

    def __init__(self, path: Optional[Union[str, Path]] = None) -> None:
        self.path = Path(path).expanduser() if path is not None else None
        self._pages: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        if self.path is not None:
            self.load()

    def __len__(self) -> int:
        with self._lock:
            return len(self._pages)

    def get(self, author: Union[bytes, str]) -> Optional[Dict[str, Any]]:
        """The stored page for *author* (destination hash), or None."""
        if isinstance(author, bytes):
            key = author.hex()
        elif isinstance(author, str):
            key = author.strip().lower()
        else:
            return None
        with self._lock:
            doc = self._pages.get(key)
            return dict(doc) if doc is not None else None

    def all(self) -> List[Dict[str, Any]]:
        """Every stored page, newest first by version then time."""
        with self._lock:
            docs = list(self._pages.values())
        docs.sort(
            key=lambda doc: (doc["version"], doc["updated_at"]), reverse=True
        )
        return [dict(doc) for doc in docs]

    def store(self, doc: Any, *, now: Optional[float] = None) -> bool:
        """Verify *doc* and keep it when it is newer than the stored one.

        Returns ``True`` when the stored record changed.  Raises
        :class:`PageError` for a malformed or invalidly-signed
        document — callers that must not fail on a bad peer document
        catch it and drop the document instead.
        """
        verified = verify_page(doc, now=now)
        with self._lock:
            existing = self._pages.get(verified["author"])
            if existing is not None and verified["version"] <= existing["version"]:
                return False
            self._pages[verified["author"]] = verified
            return True

    def remove(self, author: Union[bytes, str]) -> bool:
        """Drop the stored page for *author*; True when one existed."""
        if isinstance(author, bytes):
            key = author.hex()
        elif isinstance(author, str):
            key = author.strip().lower()
        else:
            return False
        with self._lock:
            return self._pages.pop(key, None) is not None

    def load(self) -> None:
        if self.path is None:
            return
        raw = read_json(self.path)
        if raw is None:
            return
        if isinstance(raw, dict):
            entries = raw.get("pages", [])
        elif isinstance(raw, list):  # tolerate an older hand-written format
            entries = raw
        else:
            log.warning("Ignoring page store at %s: unexpected format", self.path)
            return
        if not isinstance(entries, list):
            log.warning("Ignoring page store at %s: invalid pages list", self.path)
            return
        loaded: Dict[str, Dict[str, Any]] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                verified = verify_page(entry)
            except PageError as exc:
                log.debug("Skipping invalid page entry: %s", exc)
                continue
            existing = loaded.get(verified["author"])
            if existing is not None and verified["version"] <= existing["version"]:
                continue
            loaded[verified["author"]] = verified
        with self._lock:
            self._pages = loaded

    def save(self) -> Optional[Path]:
        """Persist the store atomically, owner-only; None with no path."""
        if self.path is None:
            return None
        with self._lock:
            docs = [dict(doc) for doc in self._pages.values()]
        docs.sort(key=lambda doc: (doc["version"], doc["updated_at"]), reverse=True)
        return atomic_write_json(self.path, {"pages": docs}, mode=0o600)
