import type { Peer } from "./types";

/** Case folds `toLowerCase` leaves alone, mirroring Python
 * `str.casefold()` for the characters that can appear in display
 * names: sharp s, final sigma, and the Latin ligatures (U+FB00–FB06).
 * A fuller table would cover all of Unicode's CaseFolding.txt; these
 * are the folds a peer name can realistically carry. */
const CASE_FOLDS: Record<string, string> = {
  "ß": "ss",
  "ς": "σ",
  "ﬀ": "ff",
  "ﬁ": "fi",
  "ﬂ": "fl",
  "ﬃ": "ffi",
  "ﬄ": "ffl",
  "ﬅ": "st",
  "ﬆ": "st",
};

/** Lowercase a peer name the way the server does: Python `str.casefold`
 * maps ß to ss and final sigma ς to σ, which JavaScript `toLowerCase`
 * leaves alone — as do the ligatures above. */
function casefold(name: string): string {
  return name
    .toLowerCase()
    .replace(/[ßςﬀﬁﬂﬃﬄﬅﬆ]/g, (ch) => CASE_FOLDS[ch] ?? ch);
}

/** Display name of a peer the way the server reports it: the local
 * alias first, then the announced name. */
function displayName(peer: Peer): string {
  return peer.alias || peer.name;
}

/**
 * Mirror of `PeerRegistry.lookalike_for` (tau/peers.py) over the
 * console's local peer list: the display name of the verified peer or
 * saved contact *peer* may be impersonating, or null.
 *
 * An unverified peer is a lookalike when its announced name matches
 * (case-insensitively) the display name — local alias or announced
 * name — of another peer the owner verified out of band or saved as
 * a contact (a peer with an alias or a private note). The peer's own
 * alias is never compared, a bare "unknown" never collides, and
 * verified peers are never lookalikes.
 *
 * A live peer event carries `lookalike_of` computed without private
 * notes (a remote console must never learn which peers carry one),
 * so an event can erase a warning the loopback snapshot supplied
 * from a note-only contact. The hook recomputes from its local list
 * when the event's value is null — the list holds exactly what this
 * console knows (notes only on loopback), so the recompute respects
 * the same boundary the server enforces. A non-null value from the
 * server is authoritative and is kept as-is.
 */
export function lookalikeOf(peers: Peer[], peer: Peer): string | null {
  if (peer.verified) return null;
  const claimed = casefold(peer.name ?? "");
  if (!claimed || claimed === "unknown") return null;
  for (const other of peers) {
    if (other.destination === peer.destination) continue;
    const contact = other.alias || other.note;
    if (!other.verified && !contact) continue;
    if (lookalikeNames(other).includes(claimed)) {
      return displayName(other);
    }
  }
  return null;
}

/** Casefolded names *peer* can be known by for lookalike matching:
 * its announced name and its local alias, excluding the "unknown"
 * fallback for peers that never announced a name. */
export function lookalikeNames(peer: Peer): string[] {
  return [peer.name, peer.alias]
    .map((name) => casefold(name ?? ""))
    .filter((name) => name && name !== "unknown");
}

/**
 * Recompute `lookalike_of` for every peer announcing *contact*'s name
 * or alias, against *peers* — which must already carry the contact's
 * new state. Used after a note edit reconciles the contact's note:
 * the anchor's note just changed, so a warning kept from the stale
 * note (or missing for the new one) must be re-derived, not trusted.
 * The contact itself is left alone: its warning comes from the reply.
 */
export function recomputeLookalikeWarnings(
  peers: Peer[],
  contact: Peer,
): Peer[] {
  const names = lookalikeNames(contact);
  if (names.length === 0) return peers;
  return peers.map((entry) => {
    if (entry.destination === contact.destination) return entry;
    if (!names.includes(casefold(entry.name ?? ""))) return entry;
    const recomputed = lookalikeOf(peers, entry);
    return recomputed === entry.lookalike_of
      ? entry
      : { ...entry, lookalike_of: recomputed };
  });
}
