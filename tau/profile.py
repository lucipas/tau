"""Signed public peer profiles — a small about-card per peer.

A profile is a signed document its author publishes about themselves:
a display name, a short bio, an avatar image hash and up to three
links.  It travels alongside announces — the announce carries only
the profile version, and the full document is fetched over a link
(``/tau/profile``) when a peer is seen with a newer version — so the
tiny announce budget stays tiny.

The document is signed exactly like a Tan entry: the author hashes
the canonical unsigned body and signs the digest with their identity.
A receiver verifies the signature against the public key the profile
carries, and checks that key derives the author's destination hash,
so a profile can only be published by the peer it describes.  Only
the newest signed version per author is kept; older or
invalidly-signed versions are dropped.
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
from urllib.parse import urlparse

import RNS

from .identity import load_public_key
from .protocol import APP_NAME, ASPECT, sanitize_name
from .util import atomic_write_json, read_json

log = logging.getLogger(__name__)

PROFILE_VERSION = 1
# A bio is a one-line introduction, shown on peer rows and DM headers:
# long enough for a sentence or two, short enough to stay a card.
MAX_BIO_CHARS = 280
MAX_LINKS = 3
MAX_LINK_CHARS = 200
# An avatar is a hash of image bytes, never the bytes themselves: a
# full SHA-256, or a 32-character hash for a short digest a peer
# already publishes (e.g. a Tan media digest prefix is 64; a
# destination-style short hash is 32 — both are accepted as given).
AVATAR_HASH_LENGTHS = (32, 64)
SIG_HEX_LENGTH = 128
KEY_HEX_LENGTH = 128
AUTHOR_HEX_LENGTH = 32

_HEX32 = re.compile(r"^[0-9a-fA-F]{32}$")
_HEX64 = re.compile(r"^[0-9a-fA-F]{64}$")
_HEX128 = re.compile(r"^[0-9a-fA-F]{128}$")


class ProfileError(ValueError):
    """Raised when a profile document is malformed or fails verification."""


def clean_bio(value: Any) -> str:
    """Normalise a bio: single-line printable text, at most 280 chars.

    An absent or blank bio is ``""`` (no bio); anything longer raises.
    Whitespace is collapsed, so a pasted multi-line bio still renders
    as one line on a peer row.
    """
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ProfileError("bio must be a string")
    cleaned = "".join(
        char for char in value if char.isprintable() or char.isspace()
    )
    bio = " ".join(cleaned.split())
    if len(bio) > MAX_BIO_CHARS:
        raise ProfileError(
            f"bio is longer than {MAX_BIO_CHARS} characters "
            f"({len(bio)} given)"
        )
    return bio


def clean_links(value: Any) -> List[str]:
    """Validate profile links: at most 3 http(s) URLs."""
    if value is None:
        return []
    if not isinstance(value, (list, tuple)):
        raise ProfileError("links must be a list")
    if len(value) > MAX_LINKS:
        raise ProfileError(
            f"a profile has at most {MAX_LINKS} links ({len(value)} given)"
        )
    links: List[str] = []
    for raw in value:
        if not isinstance(raw, str):
            raise ProfileError("links must be strings")
        link = raw.strip()
        if not link:
            raise ProfileError("links must not be empty")
        if len(link) > MAX_LINK_CHARS:
            raise ProfileError(
                f"link is longer than {MAX_LINK_CHARS} characters"
            )
        parsed = urlparse(link)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ProfileError(
                f"link {link!r} must be an http(s) URL with a host"
            )
        if any(char.isspace() for char in link):
            raise ProfileError(f"link {link!r} must not contain spaces")
        links.append(link)
    return links


def resolve_avatar(value: Any) -> Optional[str]:
    """Resolve an avatar argument to the hash stored in the profile.

    ``None`` means no avatar.  A 32- or 64-character hex string is
    used as-is (a digest the owner already has); anything else is
    read as a file path, and the SHA-256 of its bytes is stored — the
    bytes themselves never travel in the profile.  Anything that is
    neither raises :class:`ProfileError`.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        raise ProfileError("avatar must be a file path or a hex hash")
    text = value.strip()
    if _HEX32.match(text) or _HEX64.match(text):
        return text.lower()
    path = Path(text).expanduser()
    if path.is_file():
        try:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError as exc:
            raise ProfileError(f"could not read avatar file: {exc}") from exc
        return digest
    raise ProfileError(
        f"avatar {value!r} is neither an existing file nor a "
        "32/64-character hex hash"
    )


def _require_hex(value: Any, length: int, field: str) -> str:
    if not isinstance(value, str):
        raise ProfileError(f"{field} must be a hex string")
    text = value.strip().lower()
    if len(text) != length:
        raise ProfileError(f"{field} must be {length} hex characters")
    try:
        bytes.fromhex(text)
    except ValueError as exc:
        raise ProfileError(f"{field} is not valid hex") from exc
    return text


def _require_timestamp(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProfileError(f"{field} must be a number")
    timestamp = float(value)
    if not math.isfinite(timestamp) or timestamp < 0:
        raise ProfileError(f"{field} must be a finite, non-negative timestamp")
    return timestamp


def _unsigned_fields(doc: Dict[str, Any]) -> Dict[str, Any]:
    """The fields covered by the signature, in canonical form."""
    return {
        "v": doc["v"],
        "author": doc["author"],
        "key": doc["key"],
        "name": doc["name"],
        "bio": doc["bio"],
        "avatar": doc["avatar"],
        "links": doc["links"],
        "version": doc["version"],
        "created_at": doc["created_at"],
    }


def canonical_profile_bytes(doc: Dict[str, Any]) -> bytes:
    """Canonical bytes of a profile's unsigned body (what is signed)."""
    return json.dumps(
        _unsigned_fields(doc),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def profile_digest(doc: Dict[str, Any]) -> str:
    """SHA-256 of the canonical unsigned body, as lowercase hex."""
    return hashlib.sha256(canonical_profile_bytes(doc)).hexdigest()


def normalize_profile(doc: Any, *, now: Optional[float] = None) -> Dict[str, Any]:
    """Validate the unsigned part of a profile document.

    Returns the normalised document without its signature; the
    signature itself is checked by :func:`verify_profile`.  Extra
    fields are ignored, so a profile stays verifiable when a newer
    version adds one — only the signed fields matter.
    """
    if not isinstance(doc, dict):
        raise ProfileError("profile must be a JSON object")

    version_field = doc.get("v", PROFILE_VERSION)
    if isinstance(version_field, bool) or version_field != PROFILE_VERSION:
        raise ProfileError(f"unsupported profile version: {version_field!r}")

    author = _require_hex(doc.get("author"), AUTHOR_HEX_LENGTH, "author")
    key = _require_hex(doc.get("key"), KEY_HEX_LENGTH, "key")

    raw_name = doc.get("name", "")
    name = sanitize_name(raw_name)
    if name != raw_name:
        raise ProfileError("profile name is not normalised")

    bio = clean_bio(doc.get("bio", ""))
    avatar = doc.get("avatar")
    if avatar is not None:
        if not isinstance(avatar, str):
            raise ProfileError("avatar must be a hex hash or null")
        text = avatar.strip().lower()
        if len(text) not in AVATAR_HASH_LENGTHS:
            raise ProfileError("avatar must be a 32/64-character hex hash")
        try:
            bytes.fromhex(text)
        except ValueError as exc:
            raise ProfileError("avatar is not valid hex") from exc
        avatar = text
    links = clean_links(doc.get("links", []))

    version = doc.get("version")
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise ProfileError("profile version must be an integer of at least 1")

    created_at = _require_timestamp(doc.get("created_at"), "created_at")
    if now is not None and created_at > float(now) + 86400.0:
        raise ProfileError("profile timestamp is too far in the future")

    return {
        "v": PROFILE_VERSION,
        "author": author,
        "key": key,
        "name": name,
        "bio": bio,
        "avatar": avatar,
        "links": links,
        "version": version,
        "created_at": created_at,
    }


def build_profile(
    identity: RNS.Identity,
    *,
    name: str,
    bio: str = "",
    avatar: Optional[str] = None,
    links: Optional[List[str]] = None,
    version: int = 1,
    created_at: Optional[float] = None,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Build and sign a profile document owned by *identity*.

    The signature covers the canonical unsigned body, exactly like a
    Tan entry: the digest is signed, so any change to a field breaks
    verification.  *version* is the author's monotonic revision
    number — callers bump it on every publish, and receivers keep
    only the newest version they have seen.
    """
    timestamp = round(float(created_at if created_at is not None else time.time()), 3)
    unsigned = normalize_profile(
        {
            "v": PROFILE_VERSION,
            "author": RNS.Destination.hash(identity, APP_NAME, ASPECT).hex(),
            "key": identity.get_public_key().hex(),
            "name": sanitize_name(name),
            "bio": bio,
            "avatar": avatar,
            "links": list(links) if links is not None else [],
            "version": version,
            "created_at": timestamp,
        },
        now=now,
    )
    digest = profile_digest(unsigned)
    doc = dict(unsigned)
    doc["sig"] = identity.sign(bytes.fromhex(digest)).hex()
    return doc


def verify_profile(doc: Any, *, now: Optional[float] = None) -> Dict[str, Any]:
    """Validate a complete profile, including its signature and key.

    Returns the normalised document.  A profile whose key does not
    derive its author, or whose signature does not cover its body,
    is rejected — a peer cannot publish a profile for anyone else.
    """
    unsigned = normalize_profile(doc, now=now)
    digest = profile_digest(unsigned)

    signature = _require_hex(doc.get("sig"), SIG_HEX_LENGTH, "sig")
    unsigned["sig"] = signature

    try:
        identity = RNS.Identity(create_keys=False)
        if not load_public_key(identity, bytes.fromhex(unsigned["key"])):
            raise ValueError("could not load public key")
    except Exception as exc:  # pragma: no cover - depends on RNS internals
        raise ProfileError(f"invalid public key: {exc}") from exc
    author = RNS.Destination.hash(identity, APP_NAME, ASPECT).hex()
    if author != unsigned["author"]:
        raise ProfileError("public key does not match the profile author")
    try:
        valid = identity.validate(bytes.fromhex(signature), bytes.fromhex(digest))
    except Exception as exc:  # pragma: no cover - depends on RNS internals
        raise ProfileError(f"could not check signature: {exc}") from exc
    if not valid:
        raise ProfileError("profile signature is invalid")
    return unsigned


def public_profile(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The display view of a profile: everything except key and signature.

    The key and signature are public by design (the key is announced),
    but the console and CLI JSON only need the card itself, so they
    stay out of display payloads.
    """
    if doc is None:
        return None
    return {
        "author": doc["author"],
        "name": doc["name"],
        "bio": doc["bio"],
        "avatar": doc["avatar"],
        "links": list(doc["links"]),
        "version": doc["version"],
        "created_at": doc["created_at"],
    }


def load_local_profile(path: Optional[Union[str, Path]]) -> Optional[Dict[str, Any]]:
    """Load and verify the local profile, or ``None`` when absent/invalid.

    An invalid file reads as no profile (with a warning), never as a
    crash: a hand-edited or half-written file must not stop the node
    from announcing.
    """
    if path is None:
        return None
    raw = read_json(Path(path).expanduser())
    if raw is None:
        return None
    try:
        return verify_profile(raw)
    except ProfileError as exc:
        log.warning("Ignoring invalid local profile at %s: %s", path, exc)
        return None


def save_local_profile(path: Union[str, Path], doc: Dict[str, Any]) -> Path:
    """Persist a signed local profile atomically, owner-only."""
    target = Path(path).expanduser()
    atomic_write_json(target, doc, mode=0o600)
    return target


class ProfileStore:
    """Received peer profiles, keyed by author, newest version only.

    Persisted as one JSON file next to the peer registry.  Only the
    newest signed version per author is kept — an older or
    invalidly-signed document is dropped on arrival, and a
    same-version document never replaces the one already stored, so
    two copies of one version cannot fight over the record.
    """

    def __init__(self, path: Optional[Union[str, Path]] = None) -> None:
        self.path = Path(path).expanduser() if path is not None else None
        self._profiles: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        if self.path is not None:
            self.load()

    def __len__(self) -> int:
        with self._lock:
            return len(self._profiles)

    def get(self, author: Union[bytes, str]) -> Optional[Dict[str, Any]]:
        """The stored profile for *author* (destination hash), or None."""
        if isinstance(author, bytes):
            key = author.hex()
        elif isinstance(author, str):
            key = author.strip().lower()
        else:
            return None
        with self._lock:
            doc = self._profiles.get(key)
            return dict(doc) if doc is not None else None

    def all(self) -> List[Dict[str, Any]]:
        """Every stored profile, newest first by version then time."""
        with self._lock:
            docs = list(self._profiles.values())
        docs.sort(
            key=lambda doc: (doc["version"], doc["created_at"]), reverse=True
        )
        return [dict(doc) for doc in docs]

    def store(self, doc: Any, *, now: Optional[float] = None) -> bool:
        """Verify *doc* and keep it when it is newer than the stored one.

        Returns ``True`` when the stored record changed.  Raises
        :class:`ProfileError` for a malformed or invalidly-signed
        document — callers that must not fail on a bad peer document
        catch it and drop the document instead.
        """
        verified = verify_profile(doc, now=now)
        with self._lock:
            existing = self._profiles.get(verified["author"])
            if existing is not None and verified["version"] <= existing["version"]:
                return False
            self._profiles[verified["author"]] = verified
            return True

    def remove(self, author: Union[bytes, str]) -> bool:
        """Drop the stored profile for *author*; True when one existed."""
        if isinstance(author, bytes):
            key = author.hex()
        elif isinstance(author, str):
            key = author.strip().lower()
        else:
            return False
        with self._lock:
            return self._profiles.pop(key, None) is not None

    def load(self) -> None:
        if self.path is None:
            return
        raw = read_json(self.path)
        if raw is None:
            return
        if isinstance(raw, dict):
            entries = raw.get("profiles", [])
        elif isinstance(raw, list):  # tolerate an older hand-written format
            entries = raw
        else:
            log.warning("Ignoring profile store at %s: unexpected format", self.path)
            return
        if not isinstance(entries, list):
            log.warning("Ignoring profile store at %s: invalid profiles list", self.path)
            return
        loaded: Dict[str, Dict[str, Any]] = {}
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            try:
                verified = verify_profile(entry)
            except ProfileError as exc:
                log.debug("Skipping invalid profile entry: %s", exc)
                continue
            existing = loaded.get(verified["author"])
            if existing is None or verified["version"] > existing["version"]:
                loaded[verified["author"]] = verified
        with self._lock:
            self._profiles = loaded
        log.debug("Loaded %d profiles from %s", len(loaded), self.path)

    def save(self) -> bool:
        if self.path is None:
            return False
        with self._lock:
            payload = {
                "version": PROFILE_VERSION,
                "saved_at": time.time(),
                "profiles": list(self._profiles.values()),
            }
        atomic_write_json(self.path, payload, mode=0o600)
        return True
