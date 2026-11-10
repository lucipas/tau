import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type { Peer, Snapshot } from "./types";
import { useTau } from "./use-tau";

vi.mock("./api", () => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
  eventsUrl: vi.fn(() => ""),
}));

import { apiGet, apiPost } from "./api";

const mockedGet = apiGet as Mock;
const mockedPost = apiPost as Mock;

type Listener = (event: Event) => void;

class MockEventSource {
  static instances: MockEventSource[] = [];
  private listeners = new Map<string, Set<Listener>>();

  constructor(_url: string) {
    MockEventSource.instances.push(this);
  }

  addEventListener(type: string, listener: Listener) {
    const set = this.listeners.get(type) ?? new Set<Listener>();
    set.add(listener);
    this.listeners.set(type, set);
  }

  removeEventListener(type: string, listener: Listener) {
    this.listeners.get(type)?.delete(listener);
  }

  close() {}

  emit(type: string, data: unknown) {
    const event = { data: JSON.stringify(data) } as MessageEvent;
    for (const listener of this.listeners.get(type) ?? []) {
      listener(event);
    }
  }
}

function makePeer(overrides: Partial<Peer> = {}): Peer {
  return {
    destination: "ab".repeat(16),
    name: "alice",
    last_seen: 1000,
    age: 5,
    hops: 1,
    protocol: 1,
    ...overrides,
  };
}

function makeSnapshot(peers: Peer[]): Snapshot {
  return {
    node: {
      name: "tester",
      destination: "cd".repeat(16),
      protocol: 1,
      version: "0.1.0",
      rns_version: "test",
      uptime: 12.5,
      peers: peers.length,
      interfaces: [],
    },
    peers,
    groups: [],
    messages: [],
    counts: { messages: 0, peers: peers.length },
    unread: { per_peer: {}, total: 0 },
    bridge: { version: "0.1.0", web: true, url: "http://127.0.0.1:8787" },
  };
}

const emptyOutbox = { outbox: [], count: 0, held_for_others: 0 };
const emptyGroups = { groups: [], count: 0 };

beforeEach(() => {
  vi.resetAllMocks();
  MockEventSource.instances = [];
  vi.stubGlobal("EventSource", MockEventSource);
  mockedGet.mockImplementation((path: string) =>
    path === "/api/peers"
      ? Promise.resolve({ peers: [], count: 0 })
      : path === "/api/outbox"
        ? Promise.resolve(emptyOutbox)
        : Promise.resolve(emptyGroups),
  );
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("useTau forgetPeer", () => {
  it("ignores pre-forget snapshots and peer events, but not a genuine re-announce", async () => {
    const alice = makePeer();
    const { result, unmount } = renderHook(() => useTau());
    const source = MockEventSource.instances[0];
    expect(source).toBeTruthy();

    await act(async () => {
      source.emit("snapshot", { snapshot: makeSnapshot([alice]) });
    });
    await waitFor(() =>
      expect(result.current.peers.map((peer) => peer.name)).toEqual(["alice"]),
    );

    mockedPost.mockResolvedValue({
      ok: true,
      peer: alice,
    });
    await act(async () => {
      await result.current.forgetPeer(alice.destination, false);
    });
    expect(result.current.peers).toEqual([]);

    // A snapshot queued before the forget still carries alice: it must
    // not bring her back.
    await act(async () => {
      source.emit("snapshot", { snapshot: makeSnapshot([alice]) });
    });
    expect(result.current.peers).toEqual([]);

    // A peer event for the pre-forget record is stale too.
    await act(async () => {
      source.emit("peer", { peer: alice });
    });
    expect(result.current.peers).toEqual([]);

    // A genuine re-announce carries a newer last_seen and is admitted.
    const reannounced = makePeer({ last_seen: 2000 });
    await act(async () => {
      source.emit("peer", { peer: reannounced });
    });
    expect(result.current.peers.map((peer) => peer.name)).toEqual(["alice"]);
    expect(result.current.peers[0].last_seen).toBe(2000);

    unmount();
  });
});
