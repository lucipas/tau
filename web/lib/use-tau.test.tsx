import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type { Peer, Snapshot } from "./types";
import { useTau } from "./use-tau";

vi.mock("./api", () => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
  eventsUrl: () => "/api/events",
}));

import { apiPost } from "./api";

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

vi.stubGlobal("EventSource", FakeEventSource);

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
    messages: [],
    counts: { messages: 0, peers: peers.length },
    unread: { per_peer: {}, total: 0 },
    bridge: { version: "0.1.0", web: true, url: "http://127.0.0.1:8787" },
  };
}

beforeEach(() => {
  vi.resetAllMocks();
  FakeEventSource.instances.length = 0;
});

async function renderWithPeer(peer: Peer) {
  const { result } = renderHook(() => useTau());
  const source = FakeEventSource.instances[0];
  expect(source).toBeTruthy();
  await act(async () => {
    source.emit("snapshot", { snapshot: makeSnapshot([peer]) });
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
});
