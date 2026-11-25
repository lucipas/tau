// use-tau.test.tsx — live snapshot behaviour with a fake WebSocket and a
// mocked fetch, plus the pure conversation/traffic builders. The fake
// socket is driven by deferred promises: the test decides when frames
// arrive, exactly like the bridge decides when events arrive.

import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  buildConversations,
  buildTraffic,
  mergeIgnoredGroups,
  useBeaconFeed,
  useBeacons,
  useBroadcasts,
  useTau,
} from "./use-tau";
import type { Beacon, BroadcastList, ChatMessage, Group, Peer, Snapshot } from "./types";

// ------------------------------------------------------------ fake socket --

class FakeWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  static instances: FakeWebSocket[] = [];

  readyState = FakeWebSocket.CONNECTING;
  onopen: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: string }) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;

  constructor(public url: string) {
    FakeWebSocket.instances.push(this);
  }

  open() {
    this.readyState = FakeWebSocket.OPEN;
    this.onopen?.({});
  }

  receive(data: unknown) {
    this.onmessage?.({ data: typeof data === "string" ? data : JSON.stringify(data) });
  }

  close() {
    this.readyState = FakeWebSocket.CLOSED;
    this.onclose?.({});
  }
}

function makePeer(destination: string, name: string): Peer {
  return {
    destination,
    name,
    last_seen: 1000,
    age: 10,
    hops: 1,
    protocol: 1,
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
  };
}

function makeMessage(over: Partial<ChatMessage> & { id: number }): ChatMessage {
  return {
    direction: "in",
    peer: "a".repeat(32),
    peer_name: "alice",
    text: "hi",
    sent_at: 1000,
    received_at: 1000,
    status: "delivered",
    reactions: [],
    ...over,
  };
}

function makeSnapshot(over: Partial<Snapshot> = {}): Snapshot {
  return {
    node: {
      name: "me",
      destination: "0".repeat(32),
      protocol: 1,
      version: "0.0.0",
      rns_version: "1.0",
      uptime: 5,
      peers: 1,
      mailbox: { outbox: 0, held: 0, seen: 0, sent: 0, receipts: 0, retracted: 0, sent_retracted: 0 },
      interfaces: [],
      lxmf: { enabled: false, delivery: null, propagation: null },
    },
    peers: [makePeer("a".repeat(32), "alice")],
    groups: [],
    messages: [makeMessage({ id: 1 })],
    counts: { messages: 1, peers: 1 },
    unread: { per_peer: {}, per_group: {}, per_channel: {}, total: 0, revision: 1 },
    bridge: { version: "0.0.0", web: true, url: "http://127.0.0.1:8787/", notes_editable: true },
    social: { enabled: true, posts: 0 },
    ...over,
  };
}

let fetchImpl: (url: string) => Promise<Response>;

beforeEach(() => {
  FakeWebSocket.instances = [];
  vi.stubGlobal("WebSocket", FakeWebSocket);
  fetchImpl = async () =>
    ({ ok: true, status: 200, text: async () => JSON.stringify(makeSnapshot()) }) as Response;
  vi.stubGlobal("fetch", vi.fn((url: string) => fetchImpl(url)));
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("useTau live socket", () => {
  it("takes the snapshot from the first /ws frame and appends live messages", async () => {
    const { result } = renderHook(() => useTau());
    const ws = FakeWebSocket.instances[0];
    expect(ws).toBeDefined();
    expect(ws.url).toContain("/ws");

    await act(async () => {
      ws.open();
      ws.receive({ type: "snapshot", snapshot: makeSnapshot() });
    });
    await waitFor(() => expect(result.current.transport).toBe("ws"));
    expect(result.current.messages).toHaveLength(1);
    expect(result.current.peers).toHaveLength(1);

    const live = makeMessage({ id: 2, text: "second", sent_at: 1001 });
    await act(async () => {
      ws.receive({ type: "message", message: live, unread: { per_peer: { ["a".repeat(32)]: 1 }, per_group: {}, per_channel: {}, total: 1, revision: 2 } });
    });
    expect(result.current.messages).toHaveLength(2);
    expect(result.current.messages[1].text).toBe("second");
    expect(result.current.unread?.total).toBe(1);
  });

  it("applies message_updated and message_deleted events", async () => {
    const { result } = renderHook(() => useTau());
    const ws = FakeWebSocket.instances[0];
    await act(async () => {
      ws.open();
      ws.receive({ type: "snapshot", snapshot: makeSnapshot() });
    });
    await waitFor(() => expect(result.current.messages).toHaveLength(1));

    await act(async () => {
      ws.receive({
        type: "message_updated",
        message: makeMessage({ id: 1, text: "edited" }),
      });
    });
    expect(result.current.messages[0].text).toBe("edited");

    await act(async () => {
      ws.receive({ type: "message_deleted", message_id: 1 });
    });
    expect(result.current.messages).toHaveLength(0);
  });

  it("upserts a peer event without duplicating the row", async () => {
    const { result } = renderHook(() => useTau());
    const ws = FakeWebSocket.instances[0];
    await act(async () => {
      ws.open();
      ws.receive({ type: "snapshot", snapshot: makeSnapshot() });
    });
    await waitFor(() => expect(result.current.peers).toHaveLength(1));
    await act(async () => {
      ws.receive({ type: "peer", peer: makePeer("a".repeat(32), "alice-renamed") });
    });
    expect(result.current.peers).toHaveLength(1);
    expect(result.current.peers[0].name).toBe("alice-renamed");
  });

  it("replaces the group list on a groups event", async () => {
    const group: Group = {
      id: "g".repeat(32),
      name: "crew",
      members: [],
      created_at: 900,
      topic: "",
      created_by_me: true,
      can_leave: false,
      ignored: false,
    };
    // The mount refresh fetches a snapshot too: it must carry the
    // same group, or its late resolution would overwrite the socket
    // frame with an empty group list.
    fetchImpl = async () =>
      ({ ok: true, status: 200, text: async () => JSON.stringify(makeSnapshot({ groups: [group] })) }) as Response;
    const { result } = renderHook(() => useTau());
    const ws = FakeWebSocket.instances[0];
    await act(async () => {
      ws.open();
      ws.receive({ type: "snapshot", snapshot: makeSnapshot({ groups: [group] }) });
    });
    await waitFor(() => expect(result.current.groups).toHaveLength(1));
    expect(result.current.groups[0].ignored).toBe(false);
    // An ignore-state change on the bridge publishes the fresh list,
    // so the snapshot's ignored flag follows the ignore file (#784).
    await act(async () => {
      ws.receive({ type: "groups", groups: [{ ...group, ignored: true }] });
    });
    expect(result.current.groups).toHaveLength(1);
    expect(result.current.groups[0].ignored).toBe(true);
  });

  it("falls back to REST when WebSocket is unavailable", async () => {
    vi.unstubAllGlobals();
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => ({ ok: true, status: 200, text: async () => JSON.stringify(makeSnapshot()) }) as Response),
    );
    // No WebSocket global: the hook must poll /api/status instead.
    vi.stubGlobal("WebSocket", undefined);
    const { result } = renderHook(() => useTau());
    await waitFor(() => expect(result.current.snapshot).not.toBeNull());
    expect(result.current.transport).toBe("rest");
    expect(result.current.messages).toHaveLength(1);
  });
});

describe("buildConversations", () => {
  const alice = makePeer("a".repeat(32), "alice");
  const bob = makePeer("b".repeat(32), "bob");
  const group: Group = {
    id: "g".repeat(32),
    name: "crew",
    members: [],
    created_at: 900,
    topic: "",
    created_by_me: true,
    can_leave: false,
    ignored: false,
  };

  it("groups DMs by peer and groups by id, newest first", () => {
    const messages = [
      makeMessage({ id: 1, peer: alice.destination, peer_name: "alice", sent_at: 1000 }),
      makeMessage({ id: 2, peer: bob.destination, peer_name: "bob", sent_at: 2000 }),
      makeMessage({ id: 3, peer: alice.destination, peer_name: "alice", sent_at: 1500, group: group.id, group_name: "crew" }),
    ];
    const unread = { per_peer: { [bob.destination]: 2 }, per_group: { [group.id]: 1 }, per_channel: {}, total: 3, revision: 1 };
    const list = buildConversations(messages, [alice, bob], [group], unread);
    expect(list.map((c) => c.kind + ":" + c.key)).toEqual([
      `dm:${bob.destination}`,
      `group:${group.id}`,
      `dm:${alice.destination}`,
    ]);
    expect(list[0].unread).toBe(2);
    expect(list[1].unread).toBe(1);
  });

  it("includes message-less groups and hides archived DMs by default", () => {
    const list = buildConversations([], [alice], [group], null, {
      archived: new Set([alice.destination]),
    });
    expect(list).toHaveLength(1);
    expect(list[0].kind).toBe("group");
    expect(list[0].lastMessage).toBeNull();
  });

  it("hides ignored groups by default and keeps them with includeIgnored", () => {
    const ignoredGroup = { ...group, id: "i".repeat(32), name: "noisy", ignored: true };
    const messages = [
      makeMessage({ id: 3, peer: alice.destination, peer_name: "alice", sent_at: 1500, group: ignoredGroup.id, group_name: "noisy" }),
    ];
    const list = buildConversations(messages, [alice], [group, ignoredGroup], null);
    expect(list.map((c) => c.key)).toEqual([group.id]);
    const withIgnored = buildConversations(messages, [alice], [group, ignoredGroup], null, {
      includeIgnored: true,
    });
    // Newest first: the ignored group carries the message, the other has none.
    expect(withIgnored.map((c) => c.key)).toEqual([ignoredGroup.id, group.id]);
    expect(withIgnored[0].ignored).toBe(true);
    expect(withIgnored[1].ignored).toBe(false);
    // A message-less ignored group is hidden too.
    const noMessages = buildConversations([], [alice], [ignoredGroup], null);
    expect(noMessages).toHaveLength(0);
  });

  it("pinned conversations sort first, then by recency", () => {
    const messages = [
      makeMessage({ id: 1, peer: alice.destination, peer_name: "alice", sent_at: 1000 }),
      makeMessage({ id: 2, peer: bob.destination, peer_name: "bob", sent_at: 2000 }),
      makeMessage({ id: 3, peer: alice.destination, peer_name: "alice", sent_at: 1500, group: group.id, group_name: "crew" }),
    ];
    const pinnedAlice = { ...alice, pinned: true };
    const list = buildConversations(messages, [pinnedAlice, bob], [group], null);
    expect(list.map((c) => c.kind + ":" + c.key)).toEqual([
      `dm:${alice.destination}`,
      `dm:${bob.destination}`,
      `group:${group.id}`,
    ]);
    expect(list[0].pinned).toBe(true);
    expect(list[1].pinned).toBe(false);

    // A pinned group jumps the newer DM too.
    const pinnedGroup = { ...group, pinned: true };
    const list2 = buildConversations(messages, [alice, bob], [pinnedGroup], null);
    expect(list2.map((c) => c.kind + ":" + c.key)).toEqual([
      `group:${group.id}`,
      `dm:${bob.destination}`,
      `dm:${alice.destination}`,
    ]);
    expect(list2[0].pinned).toBe(true);
  });
});

describe("mergeIgnoredGroups", () => {
  const group: Group = {
    id: "g".repeat(32),
    name: "crew",
    members: [],
    created_at: 900,
    topic: "",
    created_by_me: true,
    can_leave: false,
    ignored: false,
  };

  it("takes ignored groups from the snapshot and orphans from /api/groups/left", () => {
    const ignoredGroup = { ...group, id: "i".repeat(32), name: "noisy", ignored: true };
    const merged = mergeIgnoredGroups([group, ignoredGroup], [
      { id: "i".repeat(32), name: "noisy" },
      { id: "o".repeat(32), name: "" },
    ]);
    expect(merged).toEqual([
      { id: "i".repeat(32), name: "noisy", openable: true },
      { id: "o".repeat(32), name: "oooooooo", openable: false },
    ]);
  });

  it("prefers the snapshot name over the left endpoint's", () => {
    const merged = mergeIgnoredGroups(
      [{ ...group, ignored: true }],
      [{ id: group.id, name: "stale name" }],
    );
    expect(merged).toEqual([{ id: group.id, name: "crew", openable: true }]);
  });

  it("skips left entries the snapshot still knows (un-ignored elsewhere)", () => {
    // The group is in the snapshot but no longer ignored; the cached
    // left entry is stale, not an orphan, so it produces no row.
    const merged = mergeIgnoredGroups([group], [{ id: group.id, name: "crew" }]);
    expect(merged).toEqual([]);
  });

  it("is empty when nothing is ignored", () => {
    expect(mergeIgnoredGroups([group], [])).toEqual([]);
  });
});

describe("buildTraffic", () => {
  it("counts sent/received/files per peer, newest first", () => {
    const messages = [
      makeMessage({ id: 1, direction: "out", sent_at: 1000 }),
      makeMessage({ id: 2, direction: "in", sent_at: 1001 }),
      makeMessage({ id: 3, direction: "in", sent_at: 1002, file: { name: "a.bin", size: 10, sha256: "x".repeat(64) } }),
      makeMessage({ id: 4, direction: "in", peer: "b".repeat(32), peer_name: "bob", sent_at: 500 }),
    ];
    const rows = buildTraffic(messages, [makePeer("a".repeat(32), "alice")]);
    expect(rows).toHaveLength(2);
    expect(rows[0].name).toBe("alice");
    expect(rows[0].sent).toBe(1);
    expect(rows[0].received).toBe(2);
    expect(rows[0].files).toBe(1);
  });
});

describe("useBeacons / useBeaconFeed", () => {
  function makeBeacon(over: Partial<Beacon> = {}): Beacon {
    return {
      id: "aabbccdd",
      target: "a".repeat(32),
      target_kind: "dm",
      target_name: "alice",
      started_at: 1700000000,
      expires_at: Date.now() / 1000 + 1800,
      interval: 300,
      coarse: false,
      location: { lat: 34.05, lon: -83.88, name: "Rock Springs Park" },
      next_update_at: 1700000300,
      updates_sent: 1,
      stopped: false,
      active: true,
      expires_in_seconds: 1800,
      ...over,
    };
  }

  let posted: Array<{ url: string; body: unknown }> = [];

  beforeEach(() => {
    posted = [];
    fetchImpl = async (url: string) => {
      if (url.includes("/api/beacons/position")) {
        return { ok: true, status: 200, text: async () => JSON.stringify({ ok: true }) } as Response;
      }
      if (url.includes("/api/beacons")) {
        return {
          ok: true,
          status: 200,
          text: async () => JSON.stringify({ beacons: [makeBeacon()], count: 1 }),
        } as Response;
      }
      return { ok: true, status: 200, text: async () => JSON.stringify(makeSnapshot()) } as Response;
    };
    // The stubbed fetch above drops the request body; capture it
    // through the init instead.
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        if (url.includes("/api/beacons/position")) {
          posted.push({ url, body: init?.body ? JSON.parse(String(init.body)) : undefined });
        }
        return fetchImpl(url);
      }),
    );
  });

  afterEach(() => {
    const nav = navigator as unknown as Record<string, unknown>;
    if ("geolocation" in nav) delete nav.geolocation;
  });

  it("polls the beacons endpoint and reports the active list", async () => {
    const { result } = renderHook(() => useBeacons(100000));
    await waitFor(() => expect(result.current.beacons).toHaveLength(1));
    expect(result.current.beacons[0].id).toBe("aabbccdd");
    expect(result.current.loading).toBe(false);
    expect(result.current.error).toBeNull();
  });

  it("feeds active beacons from a geolocation watch", async () => {
    const callbacks: Array<(pos: GeolocationPosition) => void> = [];
    const watchPosition = vi.fn((cb: (pos: GeolocationPosition) => void) => {
      callbacks.push(cb);
      return callbacks.length;
    });
    const clearWatch = vi.fn();
    Object.defineProperty(navigator, "geolocation", {
      value: { watchPosition, clearWatch },
      configurable: true,
    });
    const { unmount } = renderHook(() => useBeaconFeed());
    await waitFor(() => expect(watchPosition).toHaveBeenCalled());
    expect(callbacks).toHaveLength(1);
    await act(async () => {
      callbacks[0]({
        coords: {
          latitude: 34.06,
          longitude: -83.87,
          accuracy: 10,
          altitude: null,
          altitudeAccuracy: null,
          heading: null,
          speed: null,
        },
        timestamp: Date.now(),
      } as GeolocationPosition);
    });
    await waitFor(() => expect(posted).toHaveLength(1));
    expect(posted[0].url).toBe("/api/beacons/position");
    expect(posted[0].body).toEqual({ id: "aabbccdd", lat: 34.06, lon: -83.87 });
    unmount();
    expect(clearWatch).toHaveBeenCalledWith(1);
  });

  it("does nothing without geolocation", async () => {
    renderHook(() => useBeaconFeed());
    // Give the beacons poll a chance to land; with no geolocation
    // there is no watch to start and nothing to post.
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 50));
    });
    expect(posted).toHaveLength(0);
  });
});

describe("useBroadcasts", () => {
  function deferred<T>() {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>((r) => {
      resolve = r;
    });
    return { promise, resolve };
  }

  function jsonResponse(data: unknown): Response {
    return {
      ok: true,
      status: 200,
      text: async () => JSON.stringify(data),
    } as Response;
  }

  const member = { hash: "a".repeat(32), name: "alice" };
  const oldList: BroadcastList = { name: "team", members: [member], created_at: 1000 };
  const newList: BroadcastList = { name: "squad", members: [member], created_at: 1000 };

  it("a stale pre-rename refresh never restores the old name", async () => {
    // One deferred response per GET /api/broadcasts, in call order.
    const listsCalls: Array<ReturnType<typeof deferred<Response>>> = [];
    const renameCall = deferred<Response>();
    fetchImpl = async (url: string) => {
      if (url === "/api/broadcasts") {
        const call = deferred<Response>();
        listsCalls.push(call);
        return call.promise;
      }
      if (url === "/api/broadcasts/rename") {
        return renameCall.promise;
      }
      return jsonResponse(makeSnapshot());
    };

    const { result } = renderHook(() => useBroadcasts());
    // The mount refresh lands first, so the list is on screen.
    await waitFor(() => expect(listsCalls).toHaveLength(1));
    await act(async () => {
      listsCalls[0].resolve(jsonResponse({ broadcasts: [oldList], count: 1 }));
      await listsCalls[0].promise;
    });
    expect(result.current.broadcasts[0]?.name).toBe("team");

    // A second refresh starts, then a rename begins while it is in flight.
    let refreshDone: Promise<void>;
    act(() => {
      refreshDone = result.current.refresh();
    });
    await waitFor(() => expect(listsCalls).toHaveLength(2));
    let renameDone: Promise<string>;
    act(() => {
      renameDone = result.current.rename("team", "squad");
    });
    await act(async () => {
      renameCall.resolve(
        jsonResponse({ ok: true, broadcast: newList, previous_name: "team" }),
      );
      await renameCall.promise;
    });
    // The rename applied the response and started its own refresh.
    await waitFor(() => expect(listsCalls).toHaveLength(3));
    expect(result.current.broadcasts[0]?.name).toBe("squad");

    // The stale pre-rename refresh now resolves with the old snapshot.
    await act(async () => {
      listsCalls[1].resolve(jsonResponse({ broadcasts: [oldList], count: 1 }));
      await listsCalls[1].promise;
      await refreshDone;
    });
    expect(result.current.broadcasts[0]?.name).toBe("squad");

    // The rename's own refresh resolves with the post-rename snapshot.
    await act(async () => {
      listsCalls[2].resolve(jsonResponse({ broadcasts: [newList], count: 1 }));
      await listsCalls[2].promise;
      await renameDone;
    });
    expect(result.current.broadcasts[0]?.name).toBe("squad");
    expect(result.current.loading).toBe(false);
});
});
