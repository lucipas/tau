import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type { Peer, Snapshot } from "./types";
import { useTau } from "./use-tau";

vi.mock("./api", () => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
  eventsUrl: vi.fn(() => "/api/events?since=0"),
}));

import { apiGet, apiPost } from "./api";

const mockedGet = apiGet as Mock;
const mockedPost = apiPost as Mock;

// Minimal EventSource stand-in: records listeners so tests can emit
// snapshot and peer events into the hook.
class FakeEventSource {
  static instances: FakeEventSource[] = [];
  listeners: Record<string, ((event: Event) => void)[]> = {};
  constructor(_url: string) {
    FakeEventSource.instances.push(this);
  }
  addEventListener(type: string, listener: (event: Event) => void) {
    (this.listeners[type] ??= []).push(listener);
  }
  removeEventListener(type: string, listener: (event: Event) => void) {
    this.listeners[type] = (this.listeners[type] ?? []).filter(
      (entry) => entry !== listener,
    );
  }
  close() {}
  emit(type: string, data: unknown) {
    const event = { data: JSON.stringify(data) } as MessageEvent;
    for (const listener of this.listeners[type] ?? []) listener(event);
  }
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

function makePeer(overrides: Partial<Peer> = {}): Peer {
  return {
    destination: "ab".repeat(16),
    name: "alice",
    last_seen: 100,
    age: 5,
    hops: 1,
    protocol: 1,
    alias: null,
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
      uptime: 1,
      peers: peers.length,
      interfaces: [],
    },
    peers,
    groups: [],
    messages: [],
    counts: { messages: 0, peers: peers.length },
    unread: { per_peer: {}, total: 0, revision: 0 },
    bridge: { version: "0.1.0", web: true, url: "http://127.0.0.1:8787/" },
  };
}

beforeEach(() => {
  vi.resetAllMocks();
  FakeEventSource.instances = [];
  vi.stubGlobal("EventSource", FakeEventSource);
  mockedGet.mockImplementation((path: string) =>
    path === "/api/outbox"
      ? Promise.resolve({ outbox: [], held_for_others: 0 })
      : Promise.resolve({ groups: [], count: 0 }),
  );
});

function snapshotInto(source: FakeEventSource, peers: Peer[]) {
  act(() => {
    source.emit("snapshot", { snapshot: makeSnapshot(peers) });
  });
}

describe("useTau setPeerAlias", () => {
  it("applies the returned peer when nothing newer arrived", async () => {
    const peer = makePeer();
    mockedPost.mockResolvedValue({
      ok: true,
      peer: makePeer({ alias: "laptop" }),
    });

    const { result } = renderHook(() => useTau());
    const source = FakeEventSource.instances[0];
    snapshotInto(source, [peer]);
    await waitFor(() => expect(result.current.peers).toEqual([peer]));

    let updated: Peer | undefined;
    await act(async () => {
      updated = await result.current.setPeerAlias(peer.destination, "laptop");
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/peers/alias", {
      destination: peer.destination,
      alias: "laptop",
    });
    expect(updated?.alias).toBe("laptop");
    expect(result.current.peers[0].alias).toBe("laptop");
  });

  it("drops a delayed response when a newer snapshot landed meanwhile", async () => {
    const peer = makePeer();
    const pending = deferred<{ ok: boolean; peer: Peer }>();
    mockedPost.mockReturnValue(pending.promise);

    const { result } = renderHook(() => useTau());
    const source = FakeEventSource.instances[0];
    snapshotInto(source, [peer]);
    await waitFor(() => expect(result.current.peers).toEqual([peer]));

    let settled: Promise<Peer>;
    act(() => {
      settled = result.current.setPeerAlias(peer.destination, "laptop");
    });
    // A reconnect snapshot arrives while the save is in flight, with a
    // fresher last_seen and no alias yet.
    snapshotInto(source, [makePeer({ last_seen: 200 })]);
    await waitFor(() => expect(result.current.peers[0].last_seen).toBe(200));

    // The stale response carries the old last_seen: it must not
    // overwrite the snapshot.
    await act(async () => {
      pending.resolve({ ok: true, peer: makePeer({ alias: "laptop" }) });
      await settled;
    });

    expect(result.current.peers[0].last_seen).toBe(200);
    expect(result.current.peers[0].alias).toBeNull();
  });

  it("drops a delayed response when a newer peer event landed meanwhile", async () => {
    const peer = makePeer();
    const pending = deferred<{ ok: boolean; peer: Peer }>();
    mockedPost.mockReturnValue(pending.promise);

    const { result } = renderHook(() => useTau());
    const source = FakeEventSource.instances[0];
    snapshotInto(source, [peer]);
    await waitFor(() => expect(result.current.peers).toEqual([peer]));

    let settled: Promise<Peer>;
    act(() => {
      settled = result.current.setPeerAlias(peer.destination, "laptop");
    });
    // The server's peer event for this alias change arrives first.
    act(() => {
      source.emit("peer", { peer: makePeer({ alias: "laptop" }) });
    });
    await waitFor(() =>
      expect(result.current.peers[0].alias).toBe("laptop"),
    );
    // A further event lands with fresher data before the save responds.
    act(() => {
      source.emit("peer", { peer: makePeer({ alias: "laptop", last_seen: 300 }) });
    });
    await waitFor(() =>
      expect(result.current.peers[0].last_seen).toBe(300),
    );

    // The late response carries the older last_seen: resolving it must
    // not rewind the event's record.
    await act(async () => {
      pending.resolve({ ok: true, peer: makePeer({ alias: "laptop" }) });
      await settled;
    });

    expect(result.current.peers[0].last_seen).toBe(300);
    expect(result.current.peers[0].alias).toBe("laptop");
  });
});
