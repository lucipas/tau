import type { Peer } from "./types";

/** Lowercase a peer name the way the server does: Python `str.casefold`
 * maps ß to ss and final sigma ς to σ, which JavaScript `toLowerCase`
 * leaves alone — same two replacements as `casefoldTag` in format.ts,
 * kept here so this module stays self-contained. */
function casefold(name: string): string {
  return name.toLowerCase().replace(/ß/g, "ss").replace(/ς/g, "σ");
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
    const names = [other.name, other.alias]
      .map((name) => casefold(name ?? ""))
      .filter((name) => name && name !== "unknown");
    if (names.includes(claimed)) {
      return displayName(other);
    }
  }
  return null;
}
