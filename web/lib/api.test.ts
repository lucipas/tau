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
  getChannelMessages,
  getMessages,
  getTanThread,
  getTanTimeline,
  markRead,
  pageFileUrl,
  publishTanPost,
  reactToMessage,
  readFileAsDataUrl,
  closeDmPoll,
  sendChannelMessage,
  sendMessage,
  tanMediaUrl,
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

  it("publishes a Tan post with an image data URL when attached", async () => {
    nextJson = { ok: true, post: {} };
    await publishTanPost("with a picture", {
      image: { dataUrl: "data:image/png;base64,iVBORw==" },
    });
    expect(captured[0].url).toBe("/api/tan/posts");
    expect(bodyOf(0)).toEqual({
      text: "with a picture",
      image: "data:image/png;base64,iVBORw==",
    });
  });

  it("publishes a Tan post without an image field when none is attached", async () => {
    nextJson = { ok: true, post: {} };
    await publishTanPost("text only");
    expect(bodyOf(0)).toEqual({ text: "text only" });
  });

  it("reads a picked file as a base64 data URL", async () => {
    const file = new File(["hello"], "pic.png", { type: "image/png" });
    await expect(readFileAsDataUrl(file)).resolves.toBe(
      "data:image/png;base64,aGVsbG8=",
    );
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
