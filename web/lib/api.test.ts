// api.test.ts — URL / param / body building for the api.ts client, with
// fetch mocked. No live node is needed: every test drives a deferred
// promise so the request can be inspected before it resolves.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  ApiError,
  cancelScheduledChannelMessage,
  clearReaction,
  createBroadcast,
  deleteBroadcast,
  closeChannelPoll,
  editScheduledChannelMessage,
  deleteDraft,
  deleteTanDraft,
  editScheduledTanPost,
  eventsUrl,
  fetchTanMedia,
  fileUrl,
  forwardMessage,
  getAwayMessage,
  getBeacons,
  getBroadcasts,
  getChannelMessages,
  getChannelPins,
  getChannelScheduled,
  getChannelTopic,
  getDrafts,
  getGroupInviteStatus,
  getMessages,
  getOwnProfile,
  getQuietHours,
  getTanDrafts,
  getTanLists,
  getTanThread,
  getTanTimeline,
  joinGroupViaLink,
  markRead,
  mintGroupInvite,
  pageFileUrl,
  publishTanPost,
  pinChannelMessage,
  postChannelPoll,
  reactToChannelMessage,
  reactToMessage,
  readFileAsDataUrl,
  revokeGroupInvites,
  saveTanDraft,
  closeDmPoll,
  editOutbox,
  saveDraft,
  sendBroadcast,
  scheduleChannelMessage,
  scheduleChannelPoll,
  scheduleTanPoll,
  scheduleTanPost,
  scheduleTanQuote,
  scheduleTanRepost,
  searchChannelMessages,
  sendChannelMessage,
  sendMessage,
  setAwayMessage,
  setChannelTopic,
  setDisappearingDefault,
  setOwnProfile,
  setQuietHours,
  startBeacon,
  stopBeacon,
  tanMediaUrl,
  unpinChannelMessage,
  updateBeaconPosition,
  voteChannelPoll,
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

  it("builds the Tan timeline query for a list", async () => {
    nextJson = { posts: [], count: 0 };
    await getTanTimeline({ list: "Mesh Core" });
    expect(captured[0].url).toBe("/api/tan/timeline?limit=30&list=Mesh%20Core");
  });

  it("lists Tan lists", async () => {
    nextJson = { lists: [{ name: "Friends", created_at: 1000, members: 2 }], count: 1 };
    const lists = await getTanLists();
    expect(captured[0].url).toBe("/api/tan/lists");
    expect(lists).toEqual([{ name: "Friends", created_at: 1000, members: 2 }]);
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

  it("lists scheduled channel messages, optionally filtered", async () => {
    nextJson = { scheduled: [], count: 0 };
    await getChannelScheduled("my channel");
    expect(captured[0].url).toBe("/api/channels/scheduled?channel=my%20channel");
    await getChannelScheduled();
    expect(captured[1].url).toBe("/api/channels/scheduled");
  });

  it("lists saved Tan drafts", async () => {
    const draft = { id: 1, text: "half-written", reply_to: "", expires_in: null };
    nextJson = { drafts: [draft], count: 1 };
    const drafts = await getTanDrafts();
    expect(captured[0].url).toBe("/api/tan/drafts");
    expect(drafts).toEqual([draft]);
  });

  it("lists message drafts", async () => {
    nextJson = {
      drafts: [{ kind: "peer", target: "ab".repeat(16), text: "hi", updated_at: 100, name: "alice" }],
      count: 1,
    };
    const drafts = await getDrafts();
    expect(captured[0].url).toBe("/api/drafts");
    expect(drafts).toHaveLength(1);
    expect(drafts[0].name).toBe("alice");
  });

  it("posts a channel poll with question and options", async () => {
    nextJson = { ok: true, message: { hash: "p".repeat(64) } };
    const message = await postChannelPoll("general", "which day?", ["Saturday", "Sunday"]);
    expect(captured[0].url).toBe("/api/channels/poll");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({
      channel: "general",
      question: "which day?",
      options: ["Saturday", "Sunday"],
    });
    expect(message.hash).toBe("p".repeat(64));
  });

  it("votes on a channel poll by index", async () => {
    nextJson = { ok: true, message: { hash: "v".repeat(64) } };
    await voteChannelPoll("general", "p".repeat(64), 1);
    expect(captured[0].url).toBe("/api/channels/vote");
    expect(bodyOf(0)).toEqual({ channel: "general", poll: "p".repeat(64), option: 1 });
  });

  it("closes a channel poll", async () => {
    nextJson = { ok: true, message: { hash: "c".repeat(64) } };
    await closeChannelPoll("general", "p".repeat(64));
    expect(captured[0].url).toBe("/api/channels/close-poll");
    expect(bodyOf(0)).toEqual({ channel: "general", poll: "p".repeat(64) });
  });

  it("fetches a channel topic by name", async () => {
    nextJson = { channel: "general", topic: null };
    await getChannelTopic("my channel");
    expect(captured[0].url).toBe("/api/channels/topic?channel=my%20channel");
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

  it("fetches the away message", async () => {
    nextJson = {
      enabled: false,
      message: "I'm away right now. I'll reply when I'm back.",
      since: null,
      replied_to: [],
      replied_count: 0,
    };
    await getAwayMessage();
    expect(captured[0].url).toBe("/api/settings/away-message");
  });

  it("fetches the quiet-hours window", async () => {
    nextJson = {
      enabled: false,
      start: "22:00",
      end: "07:00",
      active: false,
      minutes_until_end: null,
    };
    await getQuietHours();
    expect(captured[0].url).toBe("/api/settings/quiet-hours");
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

  it("sends a DM with a location pin", async () => {
    nextJson = { ok: true, message: {}, response: {} };
    await sendMessage(
      { to: "alice" },
      "",
      { location: { lat: 34.05, lon: -83.88, name: "Rock Springs Park" } },
    );
    expect(captured[0].url).toBe("/api/messages");
    expect(bodyOf(0)).toEqual({
      to: "alice",
      text: "",
      location: { lat: 34.05, lon: -83.88, name: "Rock Springs Park" },
    });
  });

  it("sends a group message with the group key, not `to`", async () => {
    nextJson = { ok: true, message: {}, response: {} };
    await sendMessage({ group: "g123" }, "hi");
    expect(bodyOf(0)).toEqual({ group: "g123", text: "hi" });
  });

  it("edits a scheduled outbox message with text and/or send_in", async () => {
    nextJson = { ok: true, edited: [], legs: 1 };
    await editOutbox("aa".repeat(16), { text: "reworded", send_in: 3600 });
    expect(captured[0].url).toBe("/api/outbox/edit");
    expect(bodyOf(0)).toEqual({
      target: "aa".repeat(16),
      text: "reworded",
      send_in: 3600,
    });

    nextJson = { ok: true, edited: [], legs: 1 };
    await editOutbox("bb".repeat(16), { send_in: 300 });
    expect(bodyOf(1)).toEqual({ target: "bb".repeat(16), send_in: 300 });
  });

  it("saves and deletes a message draft", async () => {
    nextJson = {
      ok: true,
      draft: { kind: "peer", target: "ab".repeat(16), text: "hi", updated_at: 100, name: "alice" },
    };
    const draft = await saveDraft("peer", "ab".repeat(16), "hi");
    expect(captured[0].url).toBe("/api/drafts");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({ kind: "peer", target: "ab".repeat(16), text: "hi" });
    expect(draft.text).toBe("hi");

    nextJson = { ok: true, deleted: true };
    const deleted = await deleteDraft("peer", "ab".repeat(16));
    expect(captured[1].url).toBe("/api/drafts/delete");
    expect(bodyOf(1)).toEqual({ kind: "peer", target: "ab".repeat(16) });
    expect(deleted).toBe(true);
  });

  it("saves and deletes a channel draft", async () => {
    nextJson = {
      ok: true,
      draft: { kind: "channel", target: "general", text: "hi all", updated_at: 100, name: "general" },
    };
    const draft = await saveDraft("channel", "general", "hi all");
    expect(captured[0].url).toBe("/api/drafts");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({ kind: "channel", target: "general", text: "hi all" });
    expect(draft.kind).toBe("channel");
    expect(draft.name).toBe("general");

    nextJson = { ok: true, deleted: true };
    const deleted = await deleteDraft("channel", "general");
    expect(captured[1].url).toBe("/api/drafts/delete");
    expect(bodyOf(1)).toEqual({ kind: "channel", target: "general" });
    expect(deleted).toBe(true);
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

  it("edits a scheduled Tan post's text and time", async () => {
    nextJson = { ok: true, scheduled: { id: "abc123", text: "edited" } };
    const edited = await editScheduledTanPost("abc123", {
      text: "edited",
      sendIn: 3600,
    });
    expect(captured[0].url).toBe("/api/tan/edit-scheduled");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({ id: "abc123", text: "edited", send_in: 3600 });
    expect(edited).toEqual({ id: "abc123", text: "edited" });
  });

  it("schedules a channel message with send_in seconds", async () => {
    nextJson = { ok: true, scheduled: {} };
    await scheduleChannelMessage("general", "later", 300);
    expect(captured[0].url).toBe("/api/channels/schedule");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({ channel: "general", text: "later", send_in: 300 });
  });

  it("schedules a channel poll with its options", async () => {
    nextJson = { ok: true, scheduled: {} };
    await scheduleChannelPoll("general", "which day?", ["Saturday", "Sunday"], 300);
    expect(captured[0].url).toBe("/api/channels/schedule");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({
      channel: "general",
      text: "which day?",
      poll_options: ["Saturday", "Sunday"],
      send_in: 300,
    });
  });

it("schedules a Tan repost or quote with kind and target_post", async () => {
    nextJson = { ok: true, scheduled: {} };
    await scheduleTanRepost("d".repeat(64), 3600);
    expect(captured[0].url).toBe("/api/tan/schedule");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({
      kind: "repost",
      target_post: "d".repeat(64),
      send_in: 3600,
    });
    await scheduleTanQuote("e".repeat(64), "my take", 300);
    expect(captured[1].url).toBe("/api/tan/schedule");
    expect(bodyOf(1)).toEqual({
      kind: "quote",
      target_post: "e".repeat(64),
      text: "my take",
      send_in: 300,
    });
  });


  it("schedules a Tan poll with poll_options and send_in seconds", async () => {
    nextJson = { ok: true, scheduled: {} };
    await scheduleTanPoll("pizza or tacos?", ["Pizza", "Tacos"], 300);
    expect(captured[0].url).toBe("/api/tan/schedule");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({
      text: "pizza or tacos?",
      poll_options: ["Pizza", "Tacos"],
      send_in: 300,
      expires_in: 0,
    });
  });

  it("schedules a Tan post with an image data URL", async () => {
    nextJson = { ok: true, scheduled: {} };
    await scheduleTanPost("later with pic", 300, {
      image: { dataUrl: "data:image/png;base64,aGVsbG8=" },
    });
    expect(captured[0].url).toBe("/api/tan/schedule");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({
      text: "later with pic",
      reply_to: undefined,
      send_in: 300,
      expires_in: 0,
      image: "data:image/png;base64,aGVsbG8=",
    });
  });

  it("cancels a scheduled channel message by id", async () => {
    nextJson = { ok: true };
    await cancelScheduledChannelMessage("abc123");
    expect(captured[0].url).toBe("/api/channels/cancel-scheduled");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({ id: "abc123" });
  });

  it("edits a scheduled channel message's text and time", async () => {
    nextJson = { ok: true, edited: { id: "abc123", text: "fixed" } };
    const edited = await editScheduledChannelMessage("abc123", {
      text: "fixed",
      sendIn: 3600,
    });
    expect(captured[0].url).toBe("/api/channels/edit-scheduled");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({ id: "abc123", text: "fixed", send_in: 3600 });
    expect(edited.text).toBe("fixed");
  });

  it("edits only a scheduled channel message's time", async () => {
    nextJson = { ok: true, edited: { id: "abc123", text: "later" } };
    await editScheduledChannelMessage("abc123", { sendIn: 1800 });
    expect(captured[0].url).toBe("/api/channels/edit-scheduled");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({ id: "abc123", send_in: 1800 });
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
    nextJson = { ok: true, unread: { per_peer: {}, per_group: {}, per_channel: {}, total: 0, revision: 1 } };
    await markRead();
    expect(bodyOf(0)).toEqual({});
  });

  it("sets a disappearing default for a channel by name", async () => {
    nextJson = { ok: true };
    await setDisappearingDefault({ channel: "general" }, 300);
    expect(captured[0].url).toBe("/api/messages/disappearing");
    expect(bodyOf(0)).toEqual({ channel: "general", seconds: 300 });
    await setDisappearingDefault({ channel: "general" }, null);
    expect(bodyOf(1)).toEqual({ channel: "general", seconds: null });
  });

  it("sends a channel message with an optional expiry", async () => {
    nextJson = { ok: true, message: {} };
    await sendChannelMessage("general", "ephemeral");
    expect(captured[0].url).toBe("/api/channels/send");
    expect(bodyOf(0)).toEqual({ channel: "general", text: "ephemeral" });
    await sendChannelMessage("general", "ephemeral", undefined, 3600);
    expect(bodyOf(1)).toEqual({ channel: "general", text: "ephemeral", expires_in: 3600 });
  });

  it("marks a channel read by name", async () => {
    nextJson = { ok: true, unread: { per_peer: {}, per_group: {}, per_channel: {}, total: 0, revision: 2 } };
    const unread = await markRead({ channel: "mesh-general" });
    expect(captured[0].url).toBe("/api/messages/read");
    expect(bodyOf(0)).toEqual({ channel: "mesh-general" });
    expect(unread.total).toBe(0);
  });

  it("fetches Tan media with the author in the body", async () => {
    nextJson = { ok: true };
    await fetchTanMedia("a".repeat(64), "b".repeat(32));
    expect(captured[0].url).toBe(`/api/tan/media/${"a".repeat(64)}/fetch`);
    expect(bodyOf(0)).toEqual({ author: "b".repeat(32) });
  });

  it("sets quiet hours with only the changed fields", async () => {
    nextJson = { ok: true, quiet_hours: {} };
    await setQuietHours({ enabled: true, start: "23:00", end: "06:30" });
    expect(captured[0].url).toBe("/api/settings/quiet-hours");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({ enabled: true, start: "23:00", end: "06:30" });
    await setQuietHours({ enabled: false });
    expect(bodyOf(1)).toEqual({ enabled: false });
  });

  it("fetches the own profile from /api/profile", async () => {
    nextJson = {
      author: null,
      name: "testnode",
      bio: "",
      avatar: null,
      links: [],
      version: 0,
      created_at: null,
      published: false,
    };
    const profile = await getOwnProfile();
    expect(captured[0].url).toBe("/api/profile");
    expect(captured[0].init?.method).toBeUndefined();
    expect(profile.name).toBe("testnode");
    expect(profile.published).toBe(false);
  });

  it("publishes a profile patch to /api/profile", async () => {
    nextJson = { ok: true, profile: { name: "Tester Two", version: 1 } };
    await setOwnProfile({
      name: "Tester Two",
      bio: "mesh enjoyer",
      avatar: "ab".repeat(32),
      links: ["https://example.org"],
    });
    expect(captured[0].url).toBe("/api/profile");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({
      name: "Tester Two",
      bio: "mesh enjoyer",
      avatar: "ab".repeat(32),
      links: ["https://example.org"],
    });
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

  it("saves a Tan draft with reply and expiry", async () => {
    nextJson = { ok: true, draft: { id: 1 } };
    const draft = await saveTanDraft("half-written", {
      replyTo: "h1".padEnd(64, "0"),
      expiresIn: 3600,
    });
    expect(captured[0].url).toBe("/api/tan/drafts");
    expect(bodyOf(0)).toEqual({
      text: "half-written",
      reply_to: "h1".padEnd(64, "0"),
      expires_in: 3600,
    });
    expect(draft).toEqual({ id: 1 });
  });

  it("saves a plain Tan draft without optional fields", async () => {
    nextJson = { ok: true, draft: { id: 2 } };
    await saveTanDraft("plain draft");
    expect(bodyOf(0)).toEqual({ text: "plain draft" });
  });

  it("discards a Tan draft by id", async () => {
    nextJson = { ok: true, draft: { id: 3 } };
    await deleteTanDraft(3);
    expect(captured[0].url).toBe("/api/tan/drafts/delete");
    expect(bodyOf(0)).toEqual({ id: 3 });
  });

  it("reads a picked file as a base64 data URL", async () => {
    const file = new File(["hello"], "pic.png", { type: "image/png" });
    await expect(readFileAsDataUrl(file)).resolves.toBe(
      "data:image/png;base64,aGVsbG8=",
    );
  });

  it("lists broadcast lists from /api/broadcasts", async () => {
    nextJson = { broadcasts: [], count: 0 };
    const lists = await getBroadcasts();
    expect(captured[0].url).toBe("/api/broadcasts");
    expect(lists).toEqual([]);
  });

  it("creates a broadcast list with name and member hashes", async () => {
    nextJson = { ok: true, broadcast: { name: "team", members: [], created_at: 1 } };
    await createBroadcast("team", ["a".repeat(32)]);
    expect(captured[0].url).toBe("/api/broadcasts");
    expect(bodyOf(0)).toEqual({ name: "team", members: ["a".repeat(32)] });
  });

  it("deletes a broadcast list by name", async () => {
    nextJson = { ok: true, name: "team" };
    await deleteBroadcast("team");
    expect(captured[0].url).toBe("/api/broadcasts/delete");
    expect(bodyOf(0)).toEqual({ broadcast: "team" });
  });

  it("sends a broadcast with list name and text", async () => {
    nextJson = { ok: true, name: "team", results: [], delivered: 0, queued: 0, failed: 0 };
    await sendBroadcast("team", "meet at noon");
    expect(captured[0].url).toBe("/api/broadcasts/send");
    expect(bodyOf(0)).toEqual({ broadcast: "team", text: "meet at noon" });
  });

  it("sets a channel topic with channel and topic in the body", async () => {
    nextJson = { ok: true, channel: "general", topic: {}, cleared: false };
    await setChannelTopic("general", "monthly mesh meetup");
    expect(captured[0].url).toBe("/api/channels/topic");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({ channel: "general", topic: "monthly mesh meetup" });
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

  it("sets the away message with only the changed fields", async () => {
    nextJson = { ok: true, away_message: {} };
    await setAwayMessage({ enabled: true, message: "back on Monday" });
    expect(captured[0].url).toBe("/api/settings/away-message");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({ enabled: true, message: "back on Monday" });
    await setAwayMessage({ enabled: false });
    expect(bodyOf(1)).toEqual({ enabled: false });
  });
});

describe("forwardMessage", () => {
  it("returns the forward result on success", async () => {
    nextJson = { ok: true, message: {}, response: {} };
    const result = await forwardMessage(1, { to: "alice" });
    expect(captured[0].url).toBe("/api/messages/forward");
    expect(bodyOf(0)).toEqual({ id: 1, to: "alice" });
    expect(result).toEqual({ ok: true, message: {}, response: {} });
  });

  it("rejects when the attribution went out but the file leg failed", async () => {
    // The bridge reports a partial file forward as HTTP 200 with
    // ok: false and file_error: the picker must show the failure
    // instead of closing on a success flag.
    nextJson = {
      ok: false,
      message: {},
      response: {},
      file_error: "no path to alice",
    };
    await expect(forwardMessage(1, { to: "alice" })).rejects.toThrow(
      "the message was forwarded but the file did not send: no path to alice",
    );
  });

  it("rejects when a group file forward missed some members", async () => {
    nextJson = {
      ok: false,
      messages: [],
      response: { ok: true },
      file_messages: [],
      file_response: { ok: false, results: [] },
    };
    await expect(forwardMessage(1, { group: "crew" })).rejects.toThrow(
      "the message was forwarded but the file did not reach all members",
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

describe("group invite links", () => {
  it("fetches the invite status by group", async () => {
    nextJson = { group: {}, mode: "open", generation: 0, link: null };
    await getGroupInviteStatus("abc123");
    expect(captured[0].url).toBe("/api/groups/invite?group=abc123");
    expect(captured[0].init?.method).toBeUndefined();
  });

  it("mints a link without a mode, or with one", async () => {
    nextJson = { ok: true, link: "tau://invite/abc", mode: "open", generation: 0 };
    await mintGroupInvite("abc123");
    expect(captured[0].url).toBe("/api/groups/invite");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({ group: "abc123" });

    await mintGroupInvite("abc123", "closed");
    expect(bodyOf(1)).toEqual({ group: "abc123", mode: "closed" });
  });

  it("revokes invites for a group", async () => {
    nextJson = { ok: true, generation: 1 };
    await revokeGroupInvites("abc123");
    expect(captured[0].url).toBe("/api/groups/invite/revoke");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({ group: "abc123" });
  });

  it("joins a group from a pasted link", async () => {
    nextJson = { ok: true, status: "pending", group: { id: "abc123" } };
    await joinGroupViaLink("tau://invite/abc");
    expect(captured[0].url).toBe("/api/groups/join");
    expect(captured[0].init?.method).toBe("POST");
    expect(bodyOf(0)).toEqual({ link: "tau://invite/abc" });
  });
});
