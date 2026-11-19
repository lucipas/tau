// api.test.ts — URL / param / body building for the api.ts client, with
// fetch mocked. No live node is needed: every test drives a deferred
// promise so the request can be inspected before it resolves.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  ApiError,
  clearReaction,
  eventsUrl,
  fetchTanMedia,
  fileUrl,
  getBeacons,
  getChannelMessages,
  getChannelPins,
  getMessages,
  getTanThread,
  getTanTimeline,
  markRead,
  pageFileUrl,
  pinChannelMessage,
  reactToChannelMessage,
  reactToMessage,
  closeDmPoll,
  searchChannelMessages,
  sendChannelMessage,
  sendMessage,
  startBeacon,
  stopBeacon,
  tanMediaUrl,
  unpinChannelMessage,
  updateBeaconPosition,
  voteDmPoll,
  voteTanPoll,
  wsUrl,
} from "./api";

interface Captured {
  url: string;
  init?: RequestInit;
}

let captured: Captured[] = [];
let nextJson: unknown = {};
let nextOk = true;
let nextStatus = 200;

function jsonResponse(body: unknown, ok = true, status = 200): Response {
  return {
    ok,
    status,
    text: async () => JSON.stringify(body),
  } as Response;
}

beforeEach(() => {
  captured = [];
  nextJson = {};
  nextOk = true;
  nextStatus = 200;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      captured.push({ url, init });
      return jsonResponse(nextJson, nextOk, nextStatus);
    }),
  );
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function bodyOf(call: number): Record<string, unknown> {
  return JSON.parse(String(captured[call]?.init?.body ?? "{}")) as Record<string, unknown>;
}

describe("GET endpoints", () => {
  it("fetches messages with a limit", async () => {
    nextJson = { messages: [] };
    await getMessages(50);
    expect(captured[0].url).toBe("/api/messages?limit=50");
    expect(captured[0].init?.headers).toEqual({ "Content-Type": "application/json" });
    expect(captured[0].init?.method).toBeUndefined();
  });

  it("builds the Tan timeline query (all / author / tag)", async () => {
    nextJson = { posts: [], count: 0 };
    await getTanTimeline({ limit: 10, all: true, author: "alice", tag: "mesh" });
    expect(captured[0].url).toBe("/api/tan/timeline?limit=10&all=1&author=alice&tag=mesh");
  });

  it("omits empty timeline params", async () => {
    nextJson = { posts: [], count: 0 };
    await getTanTimeline();
    expect(captured[0].url).toBe("/api/tan/timeline?limit=30");
  });

  it("encodes the thread ref", async () => {
    nextJson = { posts: [], count: 0 };
    await getTanThread("ab12 cd");
    expect(captured[0].url).toBe("/api/tan/thread?ref=ab12%20cd");
  });

  it("encodes the channel name", async () => {
    nextJson = { channel: "general", messages: [], count: 0 };
    await getChannelMessages("my channel", 5);
    expect(captured[0].url).toBe("/api/channels/messages?channel=my%20channel&limit=5");
  });

  it("fetches channel pins", async () => {
    nextJson = { channel: "general", pins: [], count: 0 };
    await getChannelPins("my channel");
    expect(captured[0].url).toBe("/api/channels/pins?channel=my%20channel");
  });

  it("builds the channel search query", async () => {
    nextJson = { channel: "general", query: "hello", messages: [], count: 0 };
    await searchChannelMessages("my channel", "hello world", 5);
    expect(captured[0].url).toBe(
      "/api/channels/search?channel=my%20channel&q=hello%20world&limit=5",
    );
  });

  it("reacts to a channel message by hash", async () => {
    nextJson = { ok: true, reaction: {} };
    await reactToChannelMessage("general", "ab12", "👍");
    expect(captured[0].url).toBe("/api/channels/react");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({
      channel: "general",
      target: "ab12",
      reaction: "👍",
    });
  });
});

describe("POST endpoints", () => {
  it("sends a channel message without reply_to when standalone", async () => {
    nextJson = { ok: true, message: {} };
    await sendChannelMessage("mesh-general", "hello");
    expect(captured[0].url).toBe("/api/channels/send");
    expect(bodyOf(0)).toEqual({ channel: "mesh-general", text: "hello" });
  });

  it("sends a channel reply with reply_to", async () => {
    nextJson = { ok: true, message: {} };
    await sendChannelMessage("mesh-general", "count me in", "m1".padEnd(64, "0"));
    expect(captured[0].url).toBe("/api/channels/send");
    expect(bodyOf(0)).toEqual({
      channel: "mesh-general",
      text: "count me in",
      reply_to: "m1".padEnd(64, "0"),
    });
  });

  it("sends a DM with reply / expiry / delay / poll options", async () => {
    nextJson = { ok: true, message: {}, response: {} };
    await sendMessage(
      { to: "alice" },
      "hello",
      { replyTo: "wire1", expiresIn: 3600, sendIn: 60, pollOptions: ["a", "b"] },
    );
    expect(captured[0].url).toBe("/api/messages");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({
      to: "alice",
      text: "hello",
      reply_to: "wire1",
      expires_in: 3600,
      send_in: 60,
      poll_options: ["a", "b"],
    });
  });

  it("sends a group message with the group key, not `to`", async () => {
    nextJson = { ok: true, message: {}, response: {} };
    await sendMessage({ group: "g123" }, "hi");
    expect(bodyOf(0)).toEqual({ group: "g123", text: "hi" });
  });

  it("reacts and clears a reaction on the same route", async () => {
    nextJson = { ok: true };
    await reactToMessage("wire9", "👍");
    expect(bodyOf(0)).toEqual({ target: "wire9", reaction: "👍" });
    await clearReaction("wire9");
    expect(bodyOf(1)).toEqual({ target: "wire9", clear: true });
  });

  it("votes a Tan poll with a 0-based option index", async () => {
    nextJson = { ok: true };
    await voteTanPoll("posthash", 2);
    expect(captured[0].url).toBe("/api/tan/vote");
    expect(bodyOf(0)).toEqual({ target: "posthash", option: 2 });
  });

  it("votes and closes a DM poll on the DM routes", async () => {
    nextJson = { ok: true, messages: [], response: {} };
    await voteDmPoll("alice", "wire1", 2);
    expect(captured[0].url).toBe("/api/dm/vote");
    expect(bodyOf(0)).toEqual({ to: "alice", poll: "wire1", option: 2 });
    await closeDmPoll("alice", "wire1");
    expect(captured[1].url).toBe("/api/dm/close-poll");
    expect(bodyOf(1)).toEqual({ to: "alice", poll: "wire1" });
  });

  it("lists, starts, stops and feeds beacons on the beacon routes", async () => {
    nextJson = { beacons: [], count: 0 };
    await getBeacons();
    expect(captured[0].url).toBe("/api/beacons");

    nextJson = { ok: true, beacon: { id: "aabbccdd" }, update: {} };
    await startBeacon(
      { peer: "alice" },
      { lat: 34.05, lon: -83.88, name: "Rock Springs Park", durationSeconds: 900, coarse: true },
    );
    expect(captured[1].url).toBe("/api/beacons/start");
    expect(bodyOf(1)).toEqual({
      peer: "alice",
      lat: 34.05,
      lon: -83.88,
      name: "Rock Springs Park",
      durationSeconds: 900,
      coarse: true,
    });

    await startBeacon({ group: "ab".repeat(16) }, { lat: 34.05, lon: -83.88 });
    expect(captured[2].url).toBe("/api/beacons/start");
    expect(bodyOf(2)).toEqual({ group: "ab".repeat(16), lat: 34.05, lon: -83.88 });

    await stopBeacon("aabbccdd");
    expect(captured[3].url).toBe("/api/beacons/stop");
    expect(bodyOf(3)).toEqual({ id: "aabbccdd" });

    await updateBeaconPosition("aabbccdd", 34.06, -83.87);
    expect(captured[4].url).toBe("/api/beacons/position");
    expect(bodyOf(4)).toEqual({ id: "aabbccdd", lat: 34.06, lon: -83.87 });

    await updateBeaconPosition("aabbccdd", 34.06, -83.87, "Elsewhere");
    expect(bodyOf(5)).toEqual({ id: "aabbccdd", lat: 34.06, lon: -83.87, name: "Elsewhere" });
  });

  it("marks read with an empty body when no conversation is given", async () => {
    nextJson = { ok: true, unread: { per_peer: {}, per_group: {}, total: 0, revision: 1 } };
    await markRead();
    expect(bodyOf(0)).toEqual({});
  });

  it("fetches Tan media with the author in the body", async () => {
    nextJson = { ok: true };
    await fetchTanMedia("a".repeat(64), "b".repeat(32));
    expect(captured[0].url).toBe(`/api/tan/media/${"a".repeat(64)}/fetch`);
    expect(bodyOf(0)).toEqual({ author: "b".repeat(32) });
  });

  it("pins and unpins a channel message by hash", async () => {
    nextJson = { ok: true };
    await pinChannelMessage("general", "a".repeat(64));
    expect(captured[0].url).toBe("/api/channels/pin");
    expect(bodyOf(0)).toEqual({ channel: "general", hash: "a".repeat(64) });
    await unpinChannelMessage("general", "a".repeat(64));
    expect(captured[1].url).toBe("/api/channels/unpin");
    expect(bodyOf(1)).toEqual({ channel: "general", hash: "a".repeat(64) });
  });
});

describe("ApiError", () => {
  it("surfaces the bridge's error message and status", async () => {
    nextOk = false;
    nextStatus = 404;
    nextJson = { error: { message: "no peer matches 'alice'" } };
    await expect(getMessages()).rejects.toMatchObject({
      message: "no peer matches 'alice'",
      status: 404,
    });
    await expect(getMessages()).rejects.toBeInstanceOf(ApiError);
  });

  it("falls back to a generic message on a non-JSON error body", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => ({ ok: false, status: 500, text: async () => "boom" }) as Response),
    );
    await expect(getMessages()).rejects.toMatchObject({ status: 500 });
  });
});

describe("URL helpers", () => {
  it("builds file / media / page / event / ws URLs", () => {
    expect(fileUrl("sender1", "my file.txt")).toBe("/api/files/sender1/my%20file.txt");
    expect(tanMediaUrl("a".repeat(64))).toBe(`/api/tan/media/${"a".repeat(64)}`);
    expect(pageFileUrl("b".repeat(32), "c".repeat(64))).toBe(
      `/api/pages/${"b".repeat(32)}/files/${"c".repeat(64)}`,
    );
    expect(eventsUrl()).toBe("/api/events");
    expect(eventsUrl(42)).toBe("/api/events?since=42");
    expect(wsUrl()).toBe("ws://example.com/ws");
  });
});
