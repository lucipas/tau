"""Sneakernet bundles: carry tau mail and Tan posts on a file.

Every other tau transport is a live radio or network path. A bundle is
the offline one: ``tau bundle export`` writes the sealed mail this node
owes or is holding — plus, when asked, recent Tan posts — into a signed
JSON file, and ``tau bundle import`` on another node merges the contents
through the same checks the network path would apply.

The bundle is just the existing wire formats in a container:

* held envelopes are copied verbatim — they are already sealed to their
  recipient and signed by their sender;
* outbox entries are sealed fresh at export time, to the recipient's
  public key from the peer registry (an entry whose recipient key is
  unknown is skipped and reported, never exported in the clear);
* Tan posts travel as a ``tan export`` document and are re-verified on
  import by the normal ``import_entries`` path.

The manifest carries the exporter's identity hash and public key, and
the exporter signs the canonical JSON of everything else in the bundle.
The signature is tamper evidence for the container itself: it proves
the file was not altered after export, but the contents are still
verified on their own merits when they are opened — a forged sealed
envelope fails exactly as it would over the network.  Identity and
private material are never included.
"""

from __future__ import annotations

import base64
import binascii
import json
import time
from typing import Any, Dict, List, Optional, Tuple

import RNS

from .identity import destination_hash, load_public_key, public_key_hex
from .mailbox import Mailbox, MailboxError, MailboxFull, seal_message
from .peers import PeerRegistry
from .tangents.tan import TanError, export_entries, import_entries

BUNDLE_FORMAT = "tau-bundle"
BUNDLE_VERSION = 1
#: Refuse bundles larger than this (overridable per command with
#: ``--max-bytes``).  Sealed blobs are 12 KiB each and Tan posts 8 KiB,
#: so 8 MiB still holds hundreds of envelopes and posts.
DEFAULT_MAX_BUNDLE_BYTES = 8 * 1024 * 1024


class BundleError(ValueError):
    """A bundle could not be built, parsed or verified."""


def _canonical_bytes(document: Dict[str, Any]) -> bytes:
    """Deterministic JSON encoding used for the bundle signature."""
    return json.dumps(
        document, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _payload(document: Dict[str, Any]) -> Dict[str, Any]:
    """Everything in a bundle except its signature."""
    return {
        "format": document["format"],
        "version": document["version"],
        "manifest": document["manifest"],
        "mail": document["mail"],
        "tan": document["tan"],
    }


def serialize_bundle(document: Dict[str, Any]) -> bytes:
    """Human-readable JSON encoding of a signed bundle document."""
    return (
        json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def build_bundle(
    *,
    identity: RNS.Identity,
    sender_name: str,
    mailbox: Mailbox,
    registry: PeerRegistry,
    tan_conn: Any = None,
    include_tan: bool = False,
    now: Optional[float] = None,
    max_bytes: int = DEFAULT_MAX_BUNDLE_BYTES,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Build a signed bundle from this node's mail (and optionally Tan).

    Returns ``(document, summary)``.  The summary reports what was
    included and what was skipped, so the exporter can see entries that
    did not make it into the file.
    """
    if include_tan and tan_conn is None:
        raise BundleError("Tan export needs a Tan database connection")
    if max_bytes <= 0:
        raise BundleError("max bundle size must be positive")
    timestamp = now if now is not None else time.time()

    mail: List[Dict[str, Any]] = []
    skipped: List[Dict[str, str]] = []

    for envelope in mailbox.held_list():
        if envelope.expires_at <= timestamp:
            skipped.append({"id": envelope.id, "reason": "expired"})
            continue
        mail.append(
            {
                "id": envelope.id,
                "to": envelope.to_hex,
                "blob": base64.b64encode(envelope.blob).decode("ascii"),
                "expires_at": envelope.expires_at,
            }
        )

    for entry in mailbox.outbox_list():
        if entry.expires_at <= timestamp:
            skipped.append({"id": entry.id, "reason": "expired"})
            continue
        peer = registry.get(entry.to)
        if peer is None or not peer.public_key:
            skipped.append({"id": entry.id, "reason": "unknown recipient key"})
            continue
        try:
            blob = seal_message(
                identity,
                sender_name=sender_name,
                recipient_public_key=peer.public_key,
                message_id=entry.id,
                text=entry.text,
                sent_at=entry.created_at,
            )
        except MailboxError as exc:
            skipped.append({"id": entry.id, "reason": str(exc)})
            continue
        mail.append(
            {
                "id": entry.id,
                "to": entry.to_hex,
                "blob": base64.b64encode(blob).decode("ascii"),
                "expires_at": entry.expires_at,
            }
        )

    tan_document: Optional[Dict[str, Any]] = None
    tan_count = 0
    if include_tan:
        tan_document = export_entries(tan_conn, all_authors=True)
        tan_count = len(tan_document["entries"])

    manifest = {
        "created_at": timestamp,
        "exporter": destination_hash(identity).hex(),
        "exporter_key": public_key_hex(identity),
        "mail": len(mail),
        "tan": tan_count,
    }
    document: Dict[str, Any] = {
        "format": BUNDLE_FORMAT,
        "version": BUNDLE_VERSION,
        "manifest": manifest,
        "mail": mail,
        "tan": tan_document,
    }
    signature = identity.sign(_canonical_bytes(document))
    if signature is None:  # pragma: no cover - identity without private keys
        raise BundleError("exporter identity cannot sign")
    document["sig"] = base64.b64encode(signature).decode("ascii")

    encoded = serialize_bundle(document)
    if len(encoded) > max_bytes:
        raise BundleError(
            f"bundle is {len(encoded)} bytes, over the {max_bytes} byte cap"
        )
    summary = {
        "mail": len(mail),
        "tan": tan_count,
        "skipped": skipped,
        "bytes": len(encoded),
    }
    return document, summary


def parse_bundle(raw: bytes, *, max_bytes: int = DEFAULT_MAX_BUNDLE_BYTES) -> Dict[str, Any]:
    """Parse and verify a bundle file's bytes.

    Checks the size cap before decoding, then the format, version and
    the exporter's signature over the canonical payload.  A bundle that
    fails any of these is rejected whole — no envelope inside it is
    trusted far enough to apply.
    """
    if max_bytes <= 0:
        raise BundleError("max bundle size must be positive")
    if len(raw) > max_bytes:
        raise BundleError(
            f"bundle is {len(raw)} bytes, over the {max_bytes} byte cap"
        )
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BundleError(f"bundle is not valid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise BundleError("bundle must contain a JSON object")
    if document.get("format") != BUNDLE_FORMAT:
        raise BundleError(f"unsupported bundle format: {document.get('format')!r}")
    if document.get("version") != BUNDLE_VERSION:
        raise BundleError(
            f"unsupported bundle version: {document.get('version')!r}"
        )
    manifest = document.get("manifest")
    if not isinstance(manifest, dict):
        raise BundleError("bundle has no manifest")
    mail = document.get("mail")
    if not isinstance(mail, list):
        raise BundleError("bundle has no mail list")
    if "tan" not in document:
        raise BundleError("bundle has no Tan section")

    exporter_key = manifest.get("exporter_key")
    if not isinstance(exporter_key, str):
        raise BundleError("bundle manifest has no exporter key")
    try:
        key_bytes = bytes.fromhex(exporter_key)
    except ValueError as exc:
        raise BundleError("bundle exporter key is not hex") from exc
    if len(key_bytes) != 64:
        raise BundleError("bundle exporter key has the wrong length")
    exporter = RNS.Identity(create_keys=False)
    if not load_public_key(exporter, key_bytes):
        raise BundleError("could not load the bundle exporter key")
    if destination_hash(exporter).hex() != str(manifest.get("exporter") or "").lower():
        raise BundleError("bundle exporter key does not match the manifest")

    signature_text = document.get("sig")
    if not isinstance(signature_text, str):
        raise BundleError("bundle is missing its signature")
    try:
        signature = base64.b64decode(signature_text, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise BundleError("bundle signature is not valid base64") from exc
    try:
        verified = exporter.validate(signature, _canonical_bytes(_payload(document)))
    except Exception as exc:
        raise BundleError(f"could not verify bundle signature: {exc}") from exc
    if not verified:
        raise BundleError("bundle signature does not verify")
    return document


def apply_bundle(
    *,
    mailbox: Mailbox,
    tan_conn: Any,
    document: Dict[str, Any],
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Merge a verified bundle into this node's mailbox and Tan store.

    Every sealed envelope goes through :meth:`Mailbox.hold`, so the
    normal caps and dedupe rules apply; an envelope already held counts
    as present, a full mailbox or a malformed envelope is skipped with
    its error recorded.  Tan posts go through ``import_entries`` and
    are verified entry by entry.  Returns a report of what was
    accepted, already present and skipped.
    """
    timestamp = now if now is not None else time.time()
    imported = present = skipped = 0
    errors: List[Dict[str, str]] = []

    for entry in document.get("mail") or []:
        message_id = entry.get("id") if isinstance(entry, dict) else None
        ref = str(message_id or "")
        try:
            if not isinstance(entry, dict):
                raise BundleError("mail entry is not an object")
            if not isinstance(message_id, str) or not message_id:
                raise BundleError("mail entry has no id")
            to_hex = entry.get("to")
            if not isinstance(to_hex, str):
                raise BundleError("mail entry has no destination")
            to = bytes.fromhex(to_hex)
            if len(to) != 16:
                raise BundleError("mail entry destination has the wrong length")
            blob_text = entry.get("blob")
            if not isinstance(blob_text, str):
                raise BundleError("mail entry has no blob")
            blob = base64.b64decode(blob_text, validate=True)
            expires_at = entry.get("expires_at")
            if not isinstance(expires_at, (int, float)):
                raise BundleError("mail entry has no expiry")
            if float(expires_at) <= timestamp:
                raise BundleError("mail entry is expired")
            newly_held = mailbox.hold(
                to=to,
                blob=blob,
                message_id=message_id,
                expires_at=float(expires_at),
                now=timestamp,
            )
        except (BundleError, MailboxError, MailboxFull, ValueError, TypeError) as exc:
            skipped += 1
            errors.append({"id": ref, "error": str(exc)})
            continue
        if newly_held:
            imported += 1
        else:
            present += 1

    tan_result: Optional[Dict[str, Any]] = None
    tan_document = document.get("tan")
    if tan_document is not None:
        if tan_conn is None:
            skipped += 1
            errors.append({"id": "", "error": "bundle has Tan posts but no Tan store is open"})
        else:
            try:
                tan_result = import_entries(tan_conn, tan_document)
            except TanError as exc:
                skipped += 1
                errors.append({"id": "", "error": f"Tan section unusable: {exc}"})

    return {
        "imported": imported,
        "present": present,
        "skipped": skipped,
        "tan": tan_result,
        "errors": errors,
    }
