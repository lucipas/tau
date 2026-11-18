import { describe, expect, it } from "vitest";
import {
  displayName,
  extractMentions,
  extractTags,
  formatBytes,
  formatCount,
  formatCountdown,
  formatDate,
  formatDuration,
  formatExpiry,
  formatRelative,
  formatTime,
  shortHash,
  truncatedHash,
} from "./format";

describe("shortHash / truncatedHash", () => {
  it("truncates to 8 chars by default", () => {
    expect(shortHash("abcdef0123456789")).toBe("abcdef01");
    expect(shortHash("abcdef0123456789", 4)).toBe("abcd");
  });
  it("handles empty input", () => {
    expect(shortHash(null)).toBe("");
    expect(shortHash(undefined)).toBe("");
    expect(truncatedHash("")).toBe("");
  });
  it("adds an ellipsis only for long hashes", () => {
    expect(truncatedHash("abcdef0123456789")).toBe("abcdef01…");
    expect(truncatedHash("abcdef")).toBe("abcdef");
  });
});

describe("displayName", () => {
  it("prefers alias, then name, then short hash", () => {
    expect(displayName({ alias: "Al", name: "alice", destination: "aabbccdd" })).toBe("Al");
    expect(displayName({ name: "alice", destination: "aabbccdd" })).toBe("alice");
    expect(displayName({ name: "unknown", destination: "aabbccdd1122" })).toBe("aabbccdd");
    expect(displayName(null)).toBe("unknown");
  });
});

describe("formatTime / formatDate", () => {
  it("returns empty for missing timestamps", () => {
    expect(formatTime(null)).toBe("");
    expect(formatDate(undefined)).toBe("");
  });
  it("formats a known timestamp as HH:MM", () => {
    const ts = 1794000000; // 2026-11-17-ish
    expect(formatTime(ts)).toMatch(/^\d{2}:\d{2}$/);
    expect(formatDate(ts)).toMatch(/^[A-Z][a-z]{2} \d{1,2}$/);
  });
});

describe("formatRelative", () => {
  const now = 1_800_000_000;
  it("buckets elapsed time", () => {
    expect(formatRelative(now - 30, now)).toBe("just now");
    expect(formatRelative(now - 90, now)).toBe("1m");
    expect(formatRelative(now - 5 * 60, now)).toBe("5m");
    expect(formatRelative(now - 3 * 3600, now)).toBe("3h");
    expect(formatRelative(now - 2 * 86400, now)).toBe("2d");
  });
  it("clamps future timestamps and handles missing input", () => {
    expect(formatRelative(now + 100, now)).toBe("just now");
    expect(formatRelative(null, now)).toBe("");
  });
});

describe("formatCountdown / formatExpiry", () => {
  const now = 1_800_000_000;
  it("counts down to a future time", () => {
    expect(formatCountdown(now + 30, now)).toBe("in 30s");
    expect(formatCountdown(now + 5 * 60, now)).toBe("in 5m");
    expect(formatCountdown(now + 2 * 3600, now)).toBe("in 2h");
    expect(formatCountdown(now - 1, now)).toBe("now");
  });
  it("labels expiry", () => {
    expect(formatExpiry(now + 3600, now)).toBe("expires in 1h");
    expect(formatExpiry(now - 10, now)).toBe("expired");
    expect(formatExpiry(null, now)).toBe("");
  });
});

describe("formatBytes", () => {
  it("uses binary units", () => {
    expect(formatBytes(823)).toBe("823 B");
    expect(formatBytes(1536)).toBe("1.5 KiB");
    expect(formatBytes(10 * 1024 * 1024)).toBe("10.0 MiB");
    expect(formatBytes(null)).toBe("");
  });
});

describe("formatCount", () => {
  it("compacts large counts", () => {
    expect(formatCount(999)).toBe("999");
    expect(formatCount(1500)).toBe("1.5k");
    expect(formatCount(2000)).toBe("2k");
    expect(formatCount(2_500_000)).toBe("2.5M");
    expect(formatCount(undefined)).toBe("0");
  });
});

describe("formatDuration", () => {
  it("picks the largest fitting unit", () => {
    expect(formatDuration(45)).toBe("45s");
    expect(formatDuration(120)).toBe("2m");
    expect(formatDuration(7200)).toBe("2h");
    expect(formatDuration(3 * 86400)).toBe("3d");
  });
});

describe("extractTags / extractMentions", () => {
  it("finds tags lowercased and deduped", () => {
    expect(extractTags("hello #Tau #mesh #TAU!")).toEqual(["tau", "mesh"]);
    expect(extractTags("no tags")).toEqual([]);
    expect(extractTags(null)).toEqual([]);
  });
  it("finds mentions in order", () => {
    expect(extractMentions("@alice and @bob_2")).toEqual(["alice", "bob_2"]);
  });
});
