import { describe, expect, it } from "vitest";

import type { Peer } from "./types";
import { lookalikeOf } from "./peer-lookalike";

function makePeer(overrides: Partial<Peer> = {}): Peer {
  return {
    destination: "ab".repeat(16),
    name: "alice",
    last_seen: 1000,
    age: 5,
    hops: 1,
    protocol: 1,
    alias: null,
    ...overrides,
  };
}

describe("lookalikeOf", () => {
  it("returns null for a verified peer", () => {
    const anchor = makePeer({ destination: "cd".repeat(16), verified: true });
    const peer = makePeer({
      destination: "ef".repeat(16),
      name: "Alice",
      verified: true,
    });
    expect(lookalikeOf([anchor, peer], peer)).toBeNull();
  });

  it("returns null when the peer never announced a name", () => {
    const anchor = makePeer({ destination: "cd".repeat(16), verified: true });
    expect(
      lookalikeOf([anchor, makePeer({ name: "unknown" })], makePeer({ name: "unknown" })),
    ).toBeNull();
  });

  it("matches a verified anchor's announced name", () => {
    const anchor = makePeer({ destination: "cd".repeat(16), verified: true });
    const peer = makePeer({ destination: "ef".repeat(16), name: "Alice" });
    expect(lookalikeOf([anchor, peer], peer)).toBe("alice");
  });

  it("prefers the anchor's alias as its display name", () => {
    const anchor = makePeer({
      destination: "cd".repeat(16),
      verified: true,
      alias: "ally",
    });
    const peer = makePeer({ destination: "ef".repeat(16), name: "Ally" });
    expect(lookalikeOf([anchor, peer], peer)).toBe("ally");
  });

  it("counts a note-only contact as an anchor", () => {
    const anchor = makePeer({
      destination: "cd".repeat(16),
      note: "met at the mesh meetup",
    });
    const peer = makePeer({ destination: "ef".repeat(16), name: "Alice" });
    expect(lookalikeOf([anchor, peer], peer)).toBe("alice");
  });

  it("matches case-insensitively, like the server's casefold", () => {
    const anchor = makePeer({ destination: "cd".repeat(16), verified: true });
    const peer = makePeer({ destination: "ef".repeat(16), name: "ALICE" });
    expect(lookalikeOf([anchor, peer], peer)).toBe("alice");
  });

  it("casefolds ß the way the server does", () => {
    const anchor = makePeer({
      destination: "cd".repeat(16),
      verified: true,
      name: "Straße",
    });
    const peer = makePeer({ destination: "ef".repeat(16), name: "STRASSE" });
    expect(lookalikeOf([anchor, peer], peer)).toBe("Straße");
  });

  it("folds ligatures the way the server does, for note-only contacts", () => {
    const anchor = makePeer({
      destination: "cd".repeat(16),
      note: "met at the mesh meetup",
      name: "ﬀoo",
    });
    const peer = makePeer({ destination: "ef".repeat(16), name: "ffoo" });
    expect(lookalikeOf([anchor, peer], peer)).toBe("ﬀoo");
  });

  it("never compares the peer's own alias", () => {
    const anchor = makePeer({ destination: "cd".repeat(16), verified: true });
    const peer = makePeer({
      destination: "ef".repeat(16),
      name: "bob",
      alias: "alice",
    });
    expect(lookalikeOf([anchor, peer], peer)).toBeNull();
  });

  it("never collides with an anchor named unknown", () => {
    const anchor = makePeer({
      destination: "cd".repeat(16),
      verified: true,
      name: "unknown",
    });
    const peer = makePeer({ destination: "ef".repeat(16), name: "unknown" });
    expect(lookalikeOf([anchor, peer], peer)).toBeNull();
  });

  it("skips the peer itself", () => {
    const peer = makePeer({ note: "my own note" });
    expect(lookalikeOf([peer], peer)).toBeNull();
  });

  it("returns null when nothing collides", () => {
    const anchor = makePeer({ destination: "cd".repeat(16), verified: true });
    const peer = makePeer({ destination: "ef".repeat(16), name: "carol" });
    expect(lookalikeOf([anchor, peer], peer)).toBeNull();
  });

  it("ignores an unverified anchor with no alias or note", () => {
    const anchor = makePeer({ destination: "cd".repeat(16) });
    const peer = makePeer({ destination: "ef".repeat(16), name: "Alice" });
    expect(lookalikeOf([anchor, peer], peer)).toBeNull();
  });
});
