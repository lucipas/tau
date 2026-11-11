import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type {
  BlockPeerResponse,
  Peer,
  Snapshot,
  TauGroup,
  TauMessage,
} from "./types";
import { useTau } from "./use-tau";

vi.mock("./api", () => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
  eventsUrl: vi.fn(() => "/api/events"),
}));

import { apiGet, apiPost } from "./api";

const mockedGet = apiGet as Mock;
const mockedPost = apiPost as Mock;

// useTau opens an EventSource on mount; capture it so tests can push
// snapshot events into the hook.
class FakeEventSource {
  static instances: FakeEventSource[] = [];
  listeners: Record<string, ((event: Event) => void)[]> = {};

  constructor(_url: string) {
    FakeEventSource.instances.push(this);
  }

  addEventListener(type: string, fn: (event: Event) => void) {
    (this.listeners[type] ??= []).push(fn);
  }

  removeEventListener(type: string, fn: (event: Event) => void) {
    this.listeners[type] = (this.listeners[type] ?? []).filter(
      (entry) => entry !== fn,
    );
  }

  emit(type: string, data: unknown) {
    const event = { data: JSON.stringify(data) } as MessageEvent;
    for (const fn of this.listeners[type] ?? []) fn(event);
  }

  close() {}
}

const GROUP_ID = "a".repeat(32);
const PEER_HASH = "ab".repeat(16);
const BOB_HASH = "ef".repeat(16);

function makePeer(overrides: Partial<Peer> = {}): Peer {
  return {
    destination: "ab".repeat(16),
    name: "alice",
    last_seen: Date.now() / 1000,
    age: 5,
    hops: 1,
    protocol: 1,
    ...overrides,
  };
}

function makeGroup(overrides: Partial<TauGroup> = {}): TauGroup {
  return {
    id: GROUP_ID,
    name: "crew",
    members: [
      { hash: PEER_HASH, name: "alice" },
      { hash: BOB_HASH, name: "bob" },
    ],
    created_at: 1000,
    ...overrides,
  };
}

// Defaults reproduce the branch's zero-arg snapshot (one crew group,
// alice as the peer); the main line calls it with explicit peers and
// messages and takes the same default group.
function makeSnapshot(
  peers: Peer[] = [makePeer()],
  messages: TauMessage[] = [],
  groups: TauGroup[] = [makeGroup()],
): Snapshot {
  return {
    node: {
      name: "tester",
      destination: "cd".repeat(16),
      protocol: 1,
      version: "0.1.0",
      rns_version: "test",
      uptime: 12.5,
      peers: peers.length,
      interfaces: [{ name: "TCP", online: true }],
    },
    peers,
    messages,
    groups,
    counts: { messages: messages.length, peers: peers.length },
    unread: { per_peer: {}, total: 0 },
    bridge: { version: "0.1.0", web: true, url: "http://127.0.0.1:8787" },
  };
}

function makeMessage(overrides: Partial<TauMessage> = {}): TauMessage {
  return {
    id: 1,
    direction: "in",
    peer: PEER_HASH,
    peer_name: "alice",
    text: "hi",
    sent_at: 1000,
    received_at: 1000,
    status: "received",
    group: GROUP_ID,
    group_name: "crew",
    ...overrides,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

beforeEach(() => {
  vi.resetAllMocks();
  FakeEventSource.instances.length = 0;
  vi.stubGlobal("EventSource", FakeEventSource);
  // Default answers for the hook's background reads: the outbox poll
  // and any other GET the hook issues on mount.
  mockedGet.mockImplementation((path: string) =>
    path === "/api/outbox"
      ? Promise.resolve({ outbox: [], count: 0, held_for_others: 0 })
      : Promise.resolve({ groups: [], count: 0 }),
  );
});

afterEach(() => {
  vi.unstubAllGlobals();
});

async function renderWithPeer(peer: Peer, messages: TauMessage[] = []) {
  const { result } = renderHook(() => useTau());
  const source = FakeEventSource.instances[0];
  expect(source).toBeTruthy();
  await act(async () => {
    source.emit("snapshot", { snapshot: makeSnapshot([peer], messages) });
  });
  await waitFor(() => expect(result.current.peers).toEqual([peer]));
  return { result, source };
}

describe("useTau blockPeer / unblockPeer", () => {
  it("posts to /api/peers/block and flips the peer's blocked flag", async () => {
    mockedPost.mockResolvedValue({
      ok: true,
      block: {
        peer: { destination: "ab".repeat(16), name: "alice" },
        blocked: true,
        changed: true,
      },
    });
    const { result } = await renderWithPeer(makePeer());

    await act(async () => {
      await result.current.blockPeer("ab".repeat(16));
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/peers/block", {
      target: "ab".repeat(16),
    });
    expect(result.current.peers[0].blocked).toBe(true);
    expect(result.current.blocks["ab".repeat(16)]).toEqual({});
  });

  it("posts to /api/peers/unblock and clears the peer's blocked flag", async () => {
    mockedPost.mockResolvedValue({
      ok: true,
      block: {
        peer: { destination: "ab".repeat(16), name: "alice" },
        blocked: false,
        changed: true,
      },
    });
    const { result } = await renderWithPeer(makePeer({ blocked: true }));

    await act(async () => {
      await result.current.unblockPeer("ab".repeat(16));
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/peers/unblock", {
      target: "ab".repeat(16),
    });
    expect(result.current.peers[0].blocked).toBe(false);
  });

  it("stores the error and rethrows when blocking fails", async () => {
    mockedPost.mockRejectedValue(new Error("unknown peer"));
    const { result } = await renderWithPeer(makePeer());

    // Catch inside act so the rejection does not escape it: state
    // updates from the failing call still need to flush.
    let caught: unknown;
    await act(async () => {
      try {
        await result.current.blockPeer("ab".repeat(16));
      } catch (error) {
        caught = error;
      }
    });

    expect(caught).toBeInstanceOf(Error);
    expect((caught as Error).message).toBe("unknown peer");
    expect(result.current.blocks["ab".repeat(16)]?.error).toBe(
      "unknown peer",
    );
    expect(result.current.peers[0].blocked).not.toBe(true);
  });

  it("ignores a block reply superseded by a newer peer event", async () => {
    const pending = deferred<BlockPeerResponse>();
    mockedPost.mockReturnValue(pending.promise);
    const { result, source } = await renderWithPeer(makePeer());

    let outcome: Promise<unknown>;
    act(() => {
      outcome = result.current.blockPeer("ab".repeat(16));
    });
    // A newer peer event arrives while the request is in flight:
    // another console unblocked the peer after this block was sent.
    act(() => {
      source.emit("peer", { peer: makePeer({ blocked: false }) });
    });
    expect(result.current.peers[0].blocked).toBe(false);

    await act(async () => {
      pending.resolve({
        ok: true,
        block: {
          peer: { destination: "ab".repeat(16), name: "alice" },
          blocked: true,
          changed: true,
        },
      });
      await outcome;
    });

    // The stale reply must not overwrite the newer event.
    expect(result.current.peers[0].blocked).toBe(false);
  });

  it("drops the blocked peer's inbound messages from the live feed", async () => {
    mockedPost.mockResolvedValue({
      ok: true,
      block: {
        peer: { destination: "ab".repeat(16), name: "alice" },
        blocked: true,
        changed: true,
      },
    });
    const inbound = makeMessage({ id: 1 });
    const outbound = makeMessage({ id: 2, direction: "out", text: "reply" });
    const other = makeMessage({
      id: 3,
      peer: "cd".repeat(16),
      peer_name: "bob",
      text: "hey",
    });
    const { result } = await renderWithPeer(makePeer(), [
      inbound,
      outbound,
      other,
    ]);

    await act(async () => {
      await result.current.blockPeer("ab".repeat(16));
    });

    // Only the blocked sender's inbound messages go, matching the
    // server's snapshot filter; sent messages and other peers stay.
    expect(result.current.messages.map((entry) => entry.id)).toEqual([2, 3]);
  });

  it("drops inbound messages when a peer event reports a block", async () => {
    const inbound = makeMessage({ id: 1 });
    const { result, source } = await renderWithPeer(makePeer(), [inbound]);
    expect(result.current.messages).toHaveLength(1);

    // A block from another console arrives as a peer event.
    act(() => {
      source.emit("peer", { peer: makePeer({ blocked: true }) });
    });

    expect(result.current.messages).toEqual([]);
    expect(result.current.peers[0].blocked).toBe(true);
  });
});

describe("useTau search", () => {
  it("fetches the search endpoint and shows the results", async () => {
    const hit = makeMessage({ id: 2, text: "mesh hit" });
    mockedGet.mockResolvedValue({ messages: [hit], count: 1 });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.search("mesh");
    });

    expect(mockedGet).toHaveBeenCalledWith(
      "/api/messages/search?q=mesh&limit=100",
    );
    expect(result.current.searchQuery).toBe("mesh");
    expect(result.current.searchResults).toEqual([hit]);
    expect(result.current.searchError).toBeNull();
  });

  it("resubmitting the same query refreshes instead of clearing", async () => {
    const first = makeMessage({ id: 2, text: "mesh hit" });
    const refreshed = makeMessage({ id: 3, text: "mesh hit, again" });
    mockedGet
      .mockResolvedValueOnce({ messages: [first], count: 1 })
      .mockResolvedValueOnce({ messages: [refreshed], count: 1 });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.search("mesh");
    });
    expect(result.current.searchResults).toEqual([first]);

    await act(async () => {
      await result.current.search("mesh");
    });

    expect(mockedGet).toHaveBeenCalledTimes(2);
    // The refresh replaces the results; it never flashes an empty list.
    expect(result.current.searchResults).toEqual([refreshed]);
    expect(result.current.searchQuery).toBe("mesh");
  });

  it("surfaces a search failure and keeps the previous results", async () => {
    const hit = makeMessage({ id: 2, text: "mesh hit" });
    mockedGet
      .mockResolvedValueOnce({ messages: [hit], count: 1 })
      .mockRejectedValueOnce(new Error("bridge unreachable"));

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.search("mesh");
    });
    expect(result.current.searchResults).toEqual([hit]);

    await act(async () => {
      await result.current.search("mesh");
    });

    expect(result.current.searchError).toBe("bridge unreachable");
    expect(result.current.searchResults).toEqual([hit]);
    expect(result.current.searchQuery).toBe("mesh");
  });

  it("clearing the search returns to the live feed", async () => {
    const hit = makeMessage({ id: 2, text: "mesh hit" });
    mockedGet.mockResolvedValue({ messages: [hit], count: 1 });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.search("mesh");
    });
    expect(result.current.searchQuery).toBe("mesh");

    act(() => {
      result.current.clearSearch();
    });

    expect(result.current.searchQuery).toBeNull();
    expect(result.current.searchResults).toEqual([]);
    expect(result.current.searchError).toBeNull();
  });

  it("ignores a blank query", async () => {
    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.search("   ");
    });

    expect(mockedGet).not.toHaveBeenCalled();
    expect(result.current.searchQuery).toBeNull();
  });
});

describe("useTau sendGroup", () => {
  it("posts the group and text and appends one entry per leg", async () => {
    const legs = [
      makeMessage({ id: 1, peer: PEER_HASH, peer_name: "alice" }),
      makeMessage({
        id: 2,
        peer: BOB_HASH,
        peer_name: "bob",
        status: "queued",
        mail_id: "mail-bob-1",
      }),
    ];
    mockedPost.mockResolvedValue({
      ok: true,
      messages: legs,
      response: { ok: true, group: GROUP_ID, name: "crew", results: [] },
    });

    const { result } = renderHook(() => useTau());
    act(() => {
      FakeEventSource.instances[0].emit("snapshot", {
        snapshot: makeSnapshot(),
      });
    });
    await waitFor(() => expect(result.current.groups).toHaveLength(1));

    let sent: TauMessage[];
    await act(async () => {
      sent = await result.current.sendGroup(GROUP_ID, "hi all");
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/messages", {
      group: GROUP_ID,
      text: "hi all",
    });
    expect(sent!).toEqual(legs);
    expect(result.current.messages).toEqual(legs);
    // A queued leg lands in the outbox; the send refreshes it at once.
    expect(mockedGet).toHaveBeenCalledWith("/api/outbox");
  });

  it("throws naming the failed member but keeps the legs in the feed", async () => {
    const legs = [
      makeMessage({ id: 1, peer: PEER_HASH, peer_name: "alice" }),
      makeMessage({
        id: 2,
        peer: BOB_HASH,
        peer_name: "bob",
        status: "failed",
      }),
    ];
    mockedPost.mockResolvedValue({
      ok: false,
      messages: legs,
      response: {
        ok: false,
        group: GROUP_ID,
        name: "crew",
        results: [
          { to: PEER_HASH, to_name: "alice", delivered: true },
          { to: BOB_HASH, to_name: "bob", ok: false, error: "not_allowed" },
        ],
      },
    });

    const { result } = renderHook(() => useTau());
    act(() => {
      FakeEventSource.instances[0].emit("snapshot", {
        snapshot: makeSnapshot(),
      });
    });
    await waitFor(() => expect(result.current.groups).toHaveLength(1));

    await act(async () => {
      await expect(result.current.sendGroup(GROUP_ID, "hi all")).rejects.toThrow(
        /could not send to bob — alice already received this/,
      );
    });

    expect(result.current.messages).toEqual(legs);
  });

  it("warns about a queued leg when another leg fails", async () => {
    const legs = [
      makeMessage({
        id: 1,
        peer: PEER_HASH,
        peer_name: "alice",
        status: "queued",
        mail_id: "mail-alice-1",
      }),
      makeMessage({
        id: 2,
        peer: BOB_HASH,
        peer_name: "bob",
        status: "failed",
      }),
    ];
    mockedPost.mockResolvedValue({
      ok: false,
      messages: legs,
      response: {
        ok: false,
        group: GROUP_ID,
        name: "crew",
        results: [
          { to: PEER_HASH, to_name: "alice", queued: true, id: "mail-alice-1" },
          { to: BOB_HASH, to_name: "bob", ok: false, error: "not_allowed" },
        ],
      },
    });

    const { result } = renderHook(() => useTau());
    act(() => {
      FakeEventSource.instances[0].emit("snapshot", {
        snapshot: makeSnapshot(),
      });
    });
    await waitFor(() => expect(result.current.groups).toHaveLength(1));

    // The queued copy will still reach alice, so the error must say so:
    // retrying without that warning would send her a duplicate.
    await act(async () => {
      await expect(result.current.sendGroup(GROUP_ID, "hi all")).rejects.toThrow(
        /could not send to bob — alice have a copy queued/,
      );
    });

    expect(result.current.messages).toEqual(legs);
  });
});
