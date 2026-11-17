import { describe, expect, it } from "vitest";
import {
  findLookalikeAnchor,
  isLookalike,
  lookalikeGroups,
} from "./peer-lookalike";
import type { Peer } from "./types";

function peer(over: Partial<Peer> & { destination: string }): Peer {
  return {
    name: "unknown",
    last_seen: 0,
    age: 0,
    hops: null,
    protocol: null,
    lxmf: null,
    alias: "",
    verified: false,
    verified_at: null,
    key_change_at: null,
    name_change_at: null,
    previous_name: null,
    lookalike_of: null,
    blocked: false,
    profile: null,
    ...over,
  };
}

const alice = peer({ destination: "a".repeat(32), name: "alice", verified: true });
const aliceClone = peer({ destination: "b".repeat(32), name: "Alice" });
const bob = peer({ destination: "c".repeat(32), name: "bob" });
const bobAlias = peer({ destination: "d".repeat(32), name: "robert", alias: "bob" });
const bobClone = peer({ destination: "e".repeat(32), name: "BOB" });

describe("findLookalikeAnchor", () => {
  it("flags an unverified peer sharing a verified peer's name", () => {
    expect(findLookalikeAnchor(aliceClone, [alice, aliceClone])?.destination).toBe(
      alice.destination,
    );
  });
  it("is case-insensitive", () => {
    expect(findLookalikeAnchor(bobClone, [bobAlias, bobClone])?.destination).toBe(
      bobAlias.destination,
    );
  });
  it("never flags verified peers or itself", () => {
    expect(findLookalikeAnchor(alice, [alice, aliceClone])).toBeNull();
    expect(findLookalikeAnchor(aliceClone, [aliceClone])).toBeNull();
  });
  it("ignores unknown names and non-anchors", () => {
    const unknown = peer({ destination: "f".repeat(32), name: "unknown" });
    expect(findLookalikeAnchor(unknown, [unknown, bob])).toBeNull();
    expect(findLookalikeAnchor(bobClone, [bob, bobClone])).toBeNull();
  });
  it("a saved contact (alias/note) anchors a lookalike", () => {
    const noted = peer({ destination: "1".repeat(32), name: "carol", note: "met at conf" });
    const carolClone = peer({ destination: "2".repeat(32), name: "Carol" });
    expect(findLookalikeAnchor(carolClone, [noted, carolClone])?.destination).toBe(
      noted.destination,
    );
  });
});

describe("isLookalike", () => {
  it("is true exactly when an anchor exists", () => {
    expect(isLookalike(aliceClone, [alice, aliceClone])).toBe(true);
    expect(isLookalike(bob, [alice, bob])).toBe(false);
  });
});

describe("lookalikeGroups", () => {
  it("groups lookalikes under their anchor", () => {
    const groups = lookalikeGroups([alice, aliceClone, bob, bobClone, bobAlias]);
    expect(groups).toHaveLength(2);
    const aliceGroup = groups.find((g) => g.name === "alice");
    expect(aliceGroup?.anchor.destination).toBe(alice.destination);
    expect(aliceGroup?.lookalikes).toHaveLength(1);
  });
  it("returns nothing when no name collides with an anchor", () => {
    expect(lookalikeGroups([bob, bobClone])).toEqual([]);
  });
});
