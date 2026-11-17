// peer-lookalike.ts — client-side lookalike (impersonation) detection.
//
// The bridge already annotates every peer with `lookalike_of` (the display
// name of the verified peer or saved contact this unverified peer may be
// impersonating, or null). This helper mirrors that rule locally so the UI
// can explain *why* a peer is flagged and can re-derive the flag from a
// peer list without another round-trip (e.g. for peers arriving over /ws
// before their annotated record lands).
//
// Rule (mirrors tau/peers.py PeerRegistry.lookalike_for): an UNVERIFIED
// peer whose display name case-folds to the same name as a VERIFIED peer
// (or a saved contact — one with an alias or a private note) is a
// lookalike of that peer. Verified peers and saved contacts are never
// lookalikes themselves. A peer is never a lookalike of itself.

import type { Peer } from "./types";

function fold(name: string | null | undefined): string {
  return (name ?? "").trim().toLowerCase();
}

/** Display name the way the bridge computes it: alias, else name. */
export function peerDisplayName(peer: Peer): string {
  return peer.alias || peer.name || "";
}

function isSavedContact(peer: Peer): boolean {
  return Boolean(peer.alias || peer.note);
}

/**
 * The peer `candidate` may be impersonating, or null when it is not a
 * lookalike. `peers` is the full peer list (including the candidate).
 * Returns the anchor peer — the verified peer or saved contact whose
 * name the candidate shares.
 */
export function findLookalikeAnchor(
  candidate: Peer,
  peers: readonly Peer[],
): Peer | null {
  if (candidate.verified) return null;
  const name = fold(peerDisplayName(candidate));
  if (!name || name === "unknown") return null;
  for (const other of peers) {
    if (other.destination === candidate.destination) continue;
    if (!other.verified && !isSavedContact(other)) continue;
    if (fold(peerDisplayName(other)) === name) return other;
  }
  return null;
}

/** True when `candidate` is a lookalike of any peer in `peers`. */
export function isLookalike(candidate: Peer, peers: readonly Peer[]): boolean {
  return findLookalikeAnchor(candidate, peers) !== null;
}

/**
 * Group peers by folded display name, returning only the groups where
 * at least one unverified peer shares a name with a verified peer or
 * saved contact. Useful for a "possible impersonation" review list.
 * Each entry is `{ name, anchor, lookalikes }`.
 */
export function lookalikeGroups(peers: readonly Peer[]): {
  name: string;
  anchor: Peer;
  lookalikes: Peer[];
}[] {
  const byName = new Map<string, Peer[]>();
  for (const peer of peers) {
    const name = fold(peerDisplayName(peer));
    if (!name || name === "unknown") continue;
    const list = byName.get(name) ?? [];
    list.push(peer);
    byName.set(name, list);
  }
  const groups: { name: string; anchor: Peer; lookalikes: Peer[] }[] = [];
  for (const [name, list] of byName) {
    const anchor = list.find((p) => p.verified || isSavedContact(p));
    if (!anchor) continue;
    const lookalikes = list.filter(
      (p) => p.destination !== anchor.destination && !p.verified,
    );
    if (lookalikes.length > 0) {
      groups.push({ name, anchor, lookalikes });
    }
  }
  return groups;
}
