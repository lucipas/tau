import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type { BlockPeerResponse, Peer, Snapshot, TauMessage } from "./types";
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

function makeSnapshot(peers: Peer[], messages: TauMessage[] = []): Snapshot {
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
    messages,
    counts: { messages: messages.length, peers: peers.length },
    unread: { per_peer: {}, total: 0 },
    bridge: { version: "0.1.0", web: true, url: "http://127.0.0.1:8787" },
  };
}

function makeMessage(overrides: Partial<TauMessage> = {}): TauMessage {
  return {
    id: 1,
    direction: "in",
    peer: "ab".repeat(16),
    peer_name: "alice",
    text: "hi",
    sent_at: 1000,
    received_at: 1000,
    status: "received",
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
