import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type {
  BlockPeerResponse,
  EditMessageResponse,
  NodeInfo,
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

function makeNode(overrides: Partial<NodeInfo> = {}): NodeInfo {
  return {
    name: "tester",
    destination: "cd".repeat(16),
    protocol: 1,
    version: "0.1.0",
    rns_version: "test",
    uptime: 12.5,
    peers: 1,
    interfaces: [{ name: "TCP", online: true }],
    ...overrides,
  };
}

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
    alias: null,
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
    path === "/api/peers"
      ? Promise.resolve({ peers: [], count: 0 })
      : path === "/api/outbox"
        ? Promise.resolve({ outbox: [], count: 0, held_for_others: 0 })
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

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("useTau forgetPeer", () => {
  it("ignores pre-forget snapshots and peer events, but not a genuine re-announce", async () => {
    const alice = makePeer({ last_seen: 1000 });
    const { result, unmount } = renderHook(() => useTau());
    const source = FakeEventSource.instances[0];
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
describe("useTau group management", () => {
  it("shows a created group even when the follow-up refresh fails", async () => {
    const created = makeGroup();
    mockedPost.mockResolvedValueOnce({ ok: true, group: created });
    mockedGet.mockRejectedValueOnce(new Error("refresh failed"));

    const { result } = renderHook(() => useTau());
    let returned: TauGroup | undefined;
    await act(async () => {
      returned = await result.current.createGroup("weekend", [
        "ab".repeat(16),
      ]);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/groups", {
      name: "weekend",
      members: ["ab".repeat(16)],
    });
    expect(returned).toEqual(created);
    expect(result.current.groups).toEqual([created]);
  });

  it("drops a deleted group even when the follow-up refresh fails", async () => {
    const created = makeGroup();
    mockedPost.mockResolvedValueOnce({ ok: true, group: created });
    mockedGet.mockRejectedValueOnce(new Error("refresh failed"));

    const { result } = renderHook(() => useTau());
    await act(async () => {
      await result.current.createGroup("weekend", ["ab".repeat(16)]);
    });
    expect(result.current.groups).toEqual([created]);

    mockedPost.mockResolvedValueOnce({
      ok: true,
      deleted: { id: created.id, name: "weekend" },
    });
    mockedGet.mockRejectedValueOnce(new Error("refresh failed"));
    await act(async () => {
      await result.current.deleteGroup(created.id);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/groups/delete", {
      group: created.id,
    });
    expect(result.current.groups).toEqual([]);
  });

  it("swaps in the successor group when members are added", async () => {
    const created = makeGroup();
    const successor = makeGroup({
      id: "bb".repeat(16),
      members: [
        { hash: PEER_HASH, name: "alice" },
        { hash: BOB_HASH, name: "bob" },
        { hash: "cd".repeat(16), name: "carol" },
      ],
    });
    mockedPost.mockResolvedValueOnce({ ok: true, group: created });
    mockedGet.mockRejectedValueOnce(new Error("refresh failed"));

    const { result } = renderHook(() => useTau());
    await act(async () => {
      await result.current.createGroup("crew", [PEER_HASH]);
    });
    expect(result.current.groups).toEqual([created]);

    mockedPost.mockResolvedValueOnce({
      ok: true,
      group: successor,
      previous_id: created.id,
    });
    mockedGet.mockResolvedValueOnce({ ok: true, groups: [successor] });
    let returned:
      | { ok: boolean; group: TauGroup; previous_id: string }
      | undefined;
    await act(async () => {
      returned = await result.current.addGroupMembers(created.id, [
        "cd".repeat(16),
      ]);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/groups/add-member", {
      group: created.id,
      members: ["cd".repeat(16)],
    });
    expect(returned).toEqual({
      ok: true,
      group: successor,
      previous_id: created.id,
    });
    expect(mockedGet).toHaveBeenCalledWith("/api/groups");
    expect(result.current.groups).toEqual([successor]);
  });

  it("swaps in the successor group when members are removed", async () => {
    const created = makeGroup();
    const successor = makeGroup({
      id: "bb".repeat(16),
      members: [{ hash: PEER_HASH, name: "alice" }],
    });
    mockedPost.mockResolvedValueOnce({ ok: true, group: created });
    mockedGet.mockRejectedValueOnce(new Error("refresh failed"));

    const { result } = renderHook(() => useTau());
    await act(async () => {
      await result.current.createGroup("crew", [PEER_HASH]);
    });

    mockedPost.mockResolvedValueOnce({
      ok: true,
      group: successor,
      previous_id: created.id,
    });
    mockedGet.mockResolvedValueOnce({ ok: true, groups: [successor] });
    await act(async () => {
      await result.current.removeGroupMembers(created.id, [BOB_HASH]);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/groups/remove-member", {
      group: created.id,
      members: [BOB_HASH],
    });
    expect(mockedGet).toHaveBeenCalledWith("/api/groups");
    expect(result.current.groups).toEqual([successor]);
  });

  it("does not duplicate a group when the successor is already listed", async () => {
    const created = makeGroup();
    const successor = makeGroup({
      id: "bb".repeat(16),
      members: [{ hash: PEER_HASH, name: "alice" }],
    });
    const { result } = renderHook(() => useTau());
    const source = FakeEventSource.instances[0];
    expect(source).toBeTruthy();
    // A poll landed with the successor before the POST came back.
    act(() => {
      source.emit("snapshot", {
        snapshot: makeSnapshot([makePeer()], [], [created, successor]),
      });
    });
    await waitFor(() => expect(result.current.groups).toHaveLength(2));

    mockedPost.mockResolvedValueOnce({
      ok: true,
      group: successor,
      previous_id: created.id,
    });
    mockedGet.mockRejectedValueOnce(new Error("refresh failed"));
    await act(async () => {
      await result.current.removeGroupMembers(created.id, [BOB_HASH]);
    });

    expect(result.current.groups).toEqual([successor]);
  });

  it("swaps in the successor group when it is renamed", async () => {
    const created = makeGroup();
    const successor = makeGroup({ id: "bb".repeat(16), name: "weekdays" });
    mockedPost.mockResolvedValueOnce({ ok: true, group: created });
    mockedGet.mockRejectedValueOnce(new Error("refresh failed"));

    const { result } = renderHook(() => useTau());
    await act(async () => {
      await result.current.createGroup("crew", [PEER_HASH]);
    });
    expect(result.current.groups).toEqual([created]);

    mockedPost.mockResolvedValueOnce({
      ok: true,
      group: successor,
      previous_id: created.id,
    });
    mockedGet.mockResolvedValueOnce({ ok: true, groups: [successor] });
    let returned:
      | { ok: boolean; group: TauGroup; previous_id: string }
      | undefined;
    await act(async () => {
      returned = await result.current.renameGroup(created.id, "weekdays");
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/groups/rename", {
      group: created.id,
      name: "weekdays",
    });
    expect(returned).toEqual({
      ok: true,
      group: successor,
      previous_id: created.id,
    });
    expect(result.current.groups).toEqual([successor]);
    expect(result.current.groupSwap).toEqual({
      from: created.id,
      to: successor.id,
    });
  });

  it("updates a group's topic in place and clears it with null", async () => {
    const created = makeGroup();
    mockedPost.mockResolvedValueOnce({ ok: true, group: created });
    mockedGet.mockRejectedValueOnce(new Error("refresh failed"));

    const { result } = renderHook(() => useTau());
    await act(async () => {
      await result.current.createGroup("crew", [PEER_HASH]);
    });

    const updated = makeGroup({ topic: "hiking plans" });
    mockedPost.mockResolvedValueOnce({ ok: true, group: updated });
    mockedGet.mockRejectedValueOnce(new Error("refresh failed"));
    await act(async () => {
      await result.current.setGroupTopic(created.id, "hiking plans");
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/groups/topic", {
      group: created.id,
      topic: "hiking plans",
    });
    expect(result.current.groups).toEqual([updated]);

    const cleared = makeGroup({ topic: "" });
    mockedPost.mockResolvedValueOnce({ ok: true, group: cleared });
    mockedGet.mockRejectedValueOnce(new Error("refresh failed"));
    await act(async () => {
      await result.current.setGroupTopic(created.id, null);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/groups/topic", {
      group: created.id,
      clear: true,
    });
    expect(result.current.groups).toEqual([cleared]);
  });
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

  it("drops a retracted message from later searches", async () => {
    const hit = makeMessage({ id: 2, text: "mesh hit" });
    mockedGet.mockResolvedValue({ messages: [hit], count: 1 });
    mockedPost.mockResolvedValue({
      ok: true,
      retracted: { id: 2, wire_id: "a".repeat(32) },
      deleted: true,
      id: "retract-1",
    });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.search("mesh");
    });
    expect(result.current.searchResults).toEqual([hit]);

    await act(async () => {
      await result.current.retractMessage(2);
    });

    // The search ran before the retract, so its stale hit is still
    // in state; a re-run must not bring the retracted message back.
    await act(async () => {
      await result.current.search("mesh");
    });
    expect(result.current.searchResults).toEqual([]);
  });
});

describe("useTau editMessage", () => {
  it("swaps the edited text in place", async () => {
    const original = makeMessage({
      id: 1,
      direction: "out",
      text: "typo here",
      status: "delivered",
      wire_id: "a".repeat(32),
    });
    const edited = { ...original, text: "fixed here", edited_at: 2000 };
    const { result } = await renderWithPeer(makePeer(), [original]);
    await waitFor(() => expect(result.current.messages).toEqual([original]));

    mockedPost.mockResolvedValue({
      ok: true,
      edited: true,
      message: edited,
      id: "edit-1",
    });
    let response: EditMessageResponse | undefined;
    await act(async () => {
      response = await result.current.editMessage(1, "fixed here");
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/messages/edit", {
      id: 1,
      text: "fixed here",
    });
    expect(response).toEqual({
      ok: true,
      edited: true,
      message: edited,
      id: "edit-1",
    });
    expect(result.current.messages).toEqual([edited]);
  });

  it("leaves the bubble alone when the peer had no copy", async () => {
    const original = makeMessage({
      id: 1,
      direction: "out",
      text: "typo here",
      status: "delivered",
      wire_id: "a".repeat(32),
    });
    const { result } = await renderWithPeer(makePeer(), [original]);
    await waitFor(() => expect(result.current.messages).toEqual([original]));

    mockedPost.mockResolvedValue({
      ok: true,
      edited: false,
      message: null,
      id: "edit-1",
    });
    let response: EditMessageResponse | undefined;
    await act(async () => {
      response = await result.current.editMessage(1, "fixed here");
    });

    expect(response?.edited).toBe(false);
    expect(result.current.messages).toEqual([original]);
  });

  it("ignores a stale message_updated event", async () => {
    const original = makeMessage({
      id: 1,
      direction: "out",
      text: "fixed here",
      status: "delivered",
      wire_id: "a".repeat(32),
      edited_at: 2000,
    });
    const { result, source } = await renderWithPeer(makePeer(), [original]);
    await waitFor(() => expect(result.current.messages).toEqual([original]));

    // A late event carrying an older edit must not rewind the bubble.
    const stale = { ...original, text: "older text", edited_at: 1000 };
    act(() => {
      source.emit("message_updated", { message: stale });
    });
    expect(result.current.messages).toEqual([original]);
  });

  it("re-runs an active search after an edit", async () => {
    const original = makeMessage({
      id: 1,
      direction: "out",
      text: "typo here",
      status: "delivered",
      wire_id: "a".repeat(32),
    });
    const edited = { ...original, text: "fixed here", edited_at: 2000 };
    const { result } = await renderWithPeer(makePeer(), [original]);
    await waitFor(() => expect(result.current.messages).toEqual([original]));

    mockedGet.mockResolvedValue({ messages: [original], count: 1 });
    await act(async () => {
      await result.current.search("typo");
    });
    expect(result.current.searchQuery).toBe("typo");
    const searchCalls = () =>
      mockedGet.mock.calls.filter(([path]) =>
        String(path).includes("/api/messages/search"),
      );
    expect(searchCalls()).toHaveLength(1);

    mockedPost.mockResolvedValue({
      ok: true,
      edited: true,
      message: edited,
      id: "edit-1",
    });
    mockedGet.mockResolvedValue({ messages: [edited], count: 1 });
    await act(async () => {
      await result.current.editMessage(1, "fixed here");
    });
    await waitFor(() => expect(searchCalls()).toHaveLength(2));
    expect(result.current.searchResults).toEqual([edited]);
  });
});

describe("useTau rename", () => {
  it("renames the node and refreshes its record", async () => {
    mockedPost.mockResolvedValue({
      ok: true,
      name: "renamed",
      previous_name: "tester",
      changed: true,
    });
    mockedGet.mockResolvedValue({ node: makeNode({ name: "renamed" }) });

    const { result } = renderHook(() => useTau());

    let response;
    await act(async () => {
      response = await result.current.renameNode("renamed");
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/id/rename", {
      name: "renamed",
    });
    expect(response).toEqual({
      ok: true,
      name: "renamed",
      previous_name: "tester",
      changed: true,
    });
    expect(mockedGet).toHaveBeenCalledWith("/api/status");
    expect(result.current.node?.name).toBe("renamed");
  });

  it("a status event re-reads the node record", async () => {
    let source: FakeEventSource | undefined;
    class CapturingSource extends FakeEventSource {
      constructor(url: string) {
        super(url);
        source = this;
      }
    }
    vi.stubGlobal("EventSource", CapturingSource);
    mockedGet.mockResolvedValue({ node: makeNode({ name: "renamed" }) });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      source?.listeners["status"]?.forEach((listener) =>
        listener(new Event("status")),
      );
    });

    expect(mockedGet).toHaveBeenCalledWith("/api/status");
    expect(result.current.node?.name).toBe("renamed");
  });
});

describe("useTau send", () => {
  it("posts a delay as send_in and appends the scheduled message", async () => {
    const message = makeMessage({
      direction: "out",
      status: "queued",
      send_after: Date.now() / 1000 + 3600,
    });
    mockedPost.mockResolvedValue({ ok: true, message, response: {} });

    const { result } = renderHook(() => useTau());

    let sent: TauMessage | null | undefined;
    await act(async () => {
      sent = await result.current.send(PEER_HASH, "later", undefined, undefined, 3600);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/messages", {
      to: PEER_HASH,
      text: "later",
      send_in: 3600,
    });
    expect(sent).toEqual(message);
    expect(result.current.messages).toContainEqual(message);
  });

  it("resolves null and appends nothing when the send was retracted first", async () => {
    mockedPost.mockResolvedValue({ ok: true, message: null, response: {} });

    const { result } = renderHook(() => useTau());

    let sent: TauMessage | null | undefined;
    await act(async () => {
      sent = await result.current.send(PEER_HASH, "now");
    });

    expect(sent).toBeNull();
    expect(result.current.messages).toEqual([]);
  });

  it("omits send_in when no delay is picked", async () => {
    const message = makeMessage({ direction: "out", status: "delivered" });
    mockedPost.mockResolvedValue({ ok: true, message, response: {} });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.send(PEER_HASH, "now");
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/messages", {
      to: PEER_HASH,
      text: "now",
    });
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

  it("passes expiry, reply and delay through to the group send", async () => {
    const legs = [
      makeMessage({ id: 1, peer: PEER_HASH, peer_name: "alice" }),
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

    await act(async () => {
      await result.current.sendGroup(
        GROUP_ID,
        "hi all",
        3600,
        "ab".repeat(16),
        1800,
      );
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/messages", {
      group: GROUP_ID,
      text: "hi all",
      expires_in: 3600,
      reply_to: "ab".repeat(16),
      send_in: 1800,
    });
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

describe("useTau forwardMessageToGroup", () => {
  it("posts the group target and appends one entry per leg", async () => {
    const legs = [
      makeMessage({ id: 2, peer: PEER_HASH, peer_name: "alice" }),
      makeMessage({
        id: 3,
        peer: BOB_HASH,
        peer_name: "bob",
        status: "queued",
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
      sent = await result.current.forwardMessageToGroup(1, GROUP_ID);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/messages/forward", {
      id: 1,
      group: GROUP_ID,
    });
    expect(sent!).toEqual(legs);
    expect(result.current.messages).toEqual(legs);
    // A queued leg lands in the outbox; the forward refreshes it at once.
    expect(mockedGet).toHaveBeenCalledWith("/api/outbox");
  });
});

describe("useTau retractMessage", () => {
  it("posts the console id and drops the bubble at once", async () => {
    const message = makeMessage({
      id: 1,
      direction: "out",
      status: "delivered",
      group: undefined,
      wire_id: "a".repeat(32),
    });
    mockedPost.mockResolvedValue({
      ok: true,
      retracted: { id: 1, wire_id: "a".repeat(32) },
      deleted: true,
      id: "retract-1",
    });

    const { result } = renderHook(() => useTau());
    act(() => {
      FakeEventSource.instances[0].emit("snapshot", {
        snapshot: makeSnapshot([makePeer()], [message]),
      });
    });
    await waitFor(() => expect(result.current.messages).toHaveLength(1));

    let returned;
    await act(async () => {
      returned = await result.current.retractMessage(1);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/messages/retract", {
      id: 1,
    });
    expect(returned).toEqual({
      ok: true,
      retracted: { id: 1, wire_id: "a".repeat(32) },
      deleted: true,
      id: "retract-1",
    });
    expect(result.current.messages).toEqual([]);
  });

  it("drops every leg of a retracted group message at once", async () => {
    const firstLeg = makeMessage({
      id: 1,
      direction: "out",
      status: "delivered",
      group: "ab".repeat(16),
      group_name: "crew",
      wire_id: "a".repeat(32),
    });
    const secondLeg = makeMessage({
      id: 2,
      direction: "out",
      status: "delivered",
      group: "ab".repeat(16),
      group_name: "crew",
      wire_id: "b".repeat(32),
    });
    mockedPost.mockResolvedValue({
      ok: true,
      retracted: { id: 1, wire_id: "a".repeat(32) },
      retracted_ids: [1, 2],
      deleted: true,
      id: "retract-1",
    });

    const { result } = renderHook(() => useTau());
    act(() => {
      FakeEventSource.instances[0].emit("snapshot", {
        snapshot: makeSnapshot([makePeer()], [firstLeg, secondLeg]),
      });
    });
    await waitFor(() => expect(result.current.messages).toHaveLength(2));

    await act(async () => {
      await result.current.retractMessage(1);
    });

    expect(result.current.messages).toEqual([]);
  });
});

describe("useTau pinned view", () => {
  it("fetches the pinned endpoint and shows the pins newest first", async () => {
    const older = makeMessage({ id: 1, text: "first pin", pinned: true });
    const newer = makeMessage({ id: 2, text: "second pin", pinned: true });
    mockedGet.mockResolvedValue({ messages: [older, newer], count: 2 });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.showPinned();
    });

    expect(mockedGet).toHaveBeenCalledWith("/api/messages?pinned=1");
    expect(result.current.pinnedMessages).toEqual([newer, older]);
    expect(result.current.pinnedError).toBeNull();
  });

  it("surfaces a pinned fetch failure", async () => {
    mockedGet.mockRejectedValueOnce(new Error("bridge unreachable"));

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.showPinned();
    });

    expect(result.current.pinnedError).toBe("bridge unreachable");
    expect(result.current.pinnedMessages).toBeNull();
  });

  it("clearing the pinned view returns to the live feed", async () => {
    const pin = makeMessage({ id: 2, text: "a pin", pinned: true });
    mockedGet.mockResolvedValue({ messages: [pin], count: 1 });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.showPinned();
    });
    expect(result.current.pinnedMessages).toEqual([pin]);

    act(() => {
      result.current.clearPinned();
    });

    expect(result.current.pinnedMessages).toBeNull();
    expect(result.current.pinnedError).toBeNull();
  });

  it("pins and unpins through the new endpoints", async () => {
    mockedPost.mockResolvedValue({
      ok: true,
      pinned: { id: 1, wire_id: "wire-1", already_pinned: false },
    });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.pinMessage(1);
    });
    expect(mockedPost).toHaveBeenCalledWith("/api/messages/pin", { id: 1 });

    mockedPost.mockResolvedValue({
      ok: true,
      unpinned: { id: 1, wire_id: "wire-1", was_pinned: true },
    });
    await act(async () => {
      await result.current.unpinMessage(1);
    });
    expect(mockedPost).toHaveBeenCalledWith("/api/messages/unpin", {
      id: 1,
    });
  });

  it("drops an unpinned entry from an open pinned view", async () => {
    const pin = makeMessage({ id: 2, text: "a pin", pinned: true });
    mockedGet.mockResolvedValue({ messages: [pin], count: 1 });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.showPinned();
    });
    expect(result.current.pinnedMessages).toEqual([pin]);

    // The bridge re-sends the message as `message_updated` after an
    // unpin, with `pinned: false`; the pinned view must drop it, not
    // leave a stale bubble.
    act(() => {
      FakeEventSource.instances[0].emit("message_updated", {
        message: { ...pin, pinned: false },
      });
    });

    expect(result.current.pinnedMessages).toEqual([]);
  });

  it("inserts a newly pinned message into an open pinned view", async () => {
    const older = makeMessage({ id: 1, text: "older pin", pinned: true });
    const newer = makeMessage({ id: 2, text: "newer pin", pinned: true });
    mockedGet.mockResolvedValue({ messages: [older], count: 1 });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.showPinned();
    });
    expect(result.current.pinnedMessages).toEqual([older]);

    // Another tab pinned a message this view does not list yet: the pin
    // event inserts it newest first instead of waiting for a reopen.
    act(() => {
      FakeEventSource.instances[0].emit("message_updated", {
        message: newer,
      });
    });

    expect(result.current.pinnedMessages).toEqual([newer, older]);
  });

  it("keeps the pin on an update that omits the pinned flag", async () => {
    const pin = makeMessage({ id: 2, text: "a pin", pinned: true });
    mockedGet.mockResolvedValue({ messages: [pin], count: 1 });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.showPinned();
    });

    // A delivery-style update without `pinned` must not clear the pin.
    act(() => {
      FakeEventSource.instances[0].emit("message_updated", {
        message: { ...pin, pinned: undefined, status: "read" },
      });
    });

    expect(result.current.pinnedMessages).toEqual([
      { ...pin, pinned: true, status: "read" },
    ]);
  });

  it("drops a retracted message from an open pinned view at once", async () => {
    const pin = makeMessage({ id: 2, text: "a pin", pinned: true });
    mockedGet.mockResolvedValue({ messages: [pin], count: 1 });
    mockedPost.mockResolvedValue({
      ok: true,
      retracted: { id: 2, wire_id: "a".repeat(32) },
      deleted: true,
      id: "retract-1",
    });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.showPinned();
    });
    expect(result.current.pinnedMessages).toEqual([pin]);

    await act(async () => {
      await result.current.retractMessage(2);
    });

    expect(result.current.pinnedMessages).toEqual([]);
  });

  it("drops a deleted message from an open pinned view at once", async () => {
    const pin = makeMessage({ id: 2, text: "a pin", pinned: true });
    mockedGet.mockResolvedValue({ messages: [pin], count: 1 });
    mockedPost.mockResolvedValue({
      ok: true,
      deleted: { id: 2, wire_id: "a".repeat(32) },
    });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.showPinned();
    });
    expect(result.current.pinnedMessages).toEqual([pin]);

    await act(async () => {
      await result.current.deleteMessage(2);
    });

    expect(result.current.pinnedMessages).toEqual([]);
  });

  it("does not reinstate a locally removed message from a stale snapshot", async () => {
    const pin = makeMessage({ id: 2, text: "a pin", pinned: true });
    mockedPost.mockResolvedValue({
      ok: true,
      retracted: { id: 2, wire_id: "a".repeat(32) },
      deleted: true,
      id: "retract-1",
    });
    // The pinned endpoint still lists the entry: the snapshot started
    // before the retract, so it must be filtered against the removed ids.
    mockedGet.mockResolvedValue({ messages: [pin], count: 1 });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.retractMessage(2);
    });
    await act(async () => {
      await result.current.showPinned();
    });

    expect(result.current.pinnedMessages).toEqual([]);
  });

  it("re-fetches when a deletion lands while the pinned GET is in flight", async () => {
    const pin = makeMessage({ id: 2, text: "a pin", pinned: true });
    const { result } = renderHook(() => useTau());

    // Installed after mount so the hook's background reads keep their
    // default answers; only the pinned endpoint counts calls.
    let pinnedCalls = 0;
    mockedGet.mockImplementation((path: string) => {
      if (path === "/api/messages?pinned=1") {
        pinnedCalls += 1;
        if (pinnedCalls === 1) {
          // Another tab retracts the pin while this GET is in flight.
          FakeEventSource.instances[0].emit("message_deleted", {
            message_id: 2,
          });
          return Promise.resolve({ messages: [pin], count: 1 });
        }
        return Promise.resolve({ messages: [], count: 0 });
      }
      return Promise.resolve({ outbox: [], count: 0, held_for_others: 0 });
    });

    await act(async () => {
      await result.current.showPinned();
    });

    expect(pinnedCalls).toBe(2);
    expect(result.current.pinnedMessages).toEqual([]);
  });
});

describe("useTau allowlist polling", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function mockAllowlistPoll(first: unknown, second: unknown) {
    let allowlistCalls = 0;
    mockedGet.mockImplementation((path: string) => {
      if (path === "/api/allowlist") {
        allowlistCalls += 1;
        return allowlistCalls === 1
          ? Promise.resolve(first)
          : Promise.resolve(second);
      }
      if (path === "/api/peers") {
        return Promise.resolve({ peers: [], count: 0 });
      }
      if (path === "/api/outbox") {
        return Promise.resolve({ outbox: [], count: 0, held_for_others: 0 });
      }
      return Promise.resolve({ groups: [], count: 0 });
    });
    return () => allowlistCalls;
  }

  it("loads the allow-list on connect and refreshes it on the 15s poll", async () => {
    const first = { enabled: false, entries: [] };
    const second = {
      enabled: true,
      entries: [{ destination: PEER_HASH, name: "alice" }],
    };
    const allowlistCalls = mockAllowlistPoll(first, second);

    const { result } = renderHook(() => useTau());
    const source = FakeEventSource.instances[0];

    // A snapshot takes the hook live, which starts the poll tick.
    await act(async () => {
      source.emit("snapshot", { snapshot: makeSnapshot() });
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(result.current.allowlist).toEqual(first);

    // A change made from another tab (same live API) shows up on
    // the next 15s tick, like the other side panels.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(15000);
    });
    expect(allowlistCalls()).toBe(2);
    expect(result.current.allowlist).toEqual(second);
  });

  it("keeps the last list when a poll fails", async () => {
    const first = { enabled: false, entries: [] };
    let allowlistCalls = 0;
    mockedGet.mockImplementation((path: string) => {
      if (path === "/api/allowlist") {
        allowlistCalls += 1;
        return allowlistCalls === 1
          ? Promise.resolve(first)
          : Promise.reject(new Error("bridge down"));
      }
      if (path === "/api/peers") {
        return Promise.resolve({ peers: [], count: 0 });
      }
      if (path === "/api/outbox") {
        return Promise.resolve({ outbox: [], count: 0, held_for_others: 0 });
      }
      return Promise.resolve({ groups: [], count: 0 });
    });

    const { result } = renderHook(() => useTau());
    const source = FakeEventSource.instances[0];

    await act(async () => {
      source.emit("snapshot", { snapshot: makeSnapshot() });
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(result.current.allowlist).toEqual(first);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(15000);
    });
    expect(allowlistCalls).toBe(2);
    expect(result.current.allowlist).toEqual(first);
  });

  it("does not poll the allow-list while an add is in flight", async () => {
    const before = { enabled: false, entries: [] };
    const after = {
      enabled: false,
      entries: [{ destination: PEER_HASH, name: "alice" }],
    };
    const addGate = deferred<unknown>();
    let allowlistCalls = 0;
    mockedGet.mockImplementation((path: string) => {
      if (path === "/api/allowlist") {
        allowlistCalls += 1;
        return Promise.resolve(before);
      }
      if (path === "/api/peers") {
        return Promise.resolve({ peers: [], count: 0 });
      }
      if (path === "/api/outbox") {
        return Promise.resolve({ outbox: [], count: 0, held_for_others: 0 });
      }
      return Promise.resolve({ groups: [], count: 0 });
    });
    mockedPost.mockImplementation((path: string) =>
      path === "/api/allowlist/add"
        ? addGate.promise
        : Promise.resolve({}),
    );

    const { result } = renderHook(() => useTau());
    const source = FakeEventSource.instances[0];

    await act(async () => {
      source.emit("snapshot", { snapshot: makeSnapshot() });
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(result.current.allowlist).toEqual(before);
    expect(allowlistCalls).toBe(1);

    // Start an add and hold its response: the next poll must skip the
    // allow-list instead of overwriting the pending add with a
    // pre-write snapshot.
    let addPromise: Promise<unknown> = Promise.resolve();
    act(() => {
      addPromise = result.current.addAllowlistPeer(PEER_HASH);
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(15000);
    });
    expect(allowlistCalls).toBe(1);

    // The add resolves with the updated list; the following poll
    // resumes fetching it.
    await act(async () => {
      addGate.resolve(after);
      await addPromise;
    });
    expect(result.current.allowlist).toEqual(after);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(15000);
    });
    expect(allowlistCalls).toBe(2);
  });

  it("resumes polling after a stalled write times out", async () => {
    const before = { enabled: false, entries: [] };
    let allowlistCalls = 0;
    mockedGet.mockImplementation((path: string) => {
      if (path === "/api/allowlist") {
        allowlistCalls += 1;
        return Promise.resolve(before);
      }
      if (path === "/api/peers") {
        return Promise.resolve({ peers: [], count: 0 });
      }
      if (path === "/api/outbox") {
        return Promise.resolve({ outbox: [], count: 0, held_for_others: 0 });
      }
      return Promise.resolve({ groups: [], count: 0 });
    });
    // The bridge accepts the add but never answers — until the write
    // timeout aborts it, like fetch would.
    mockedPost.mockImplementation(
      (_path: string, _body: unknown, signal?: AbortSignal) =>
        new Promise((_resolve, reject) => {
          signal?.addEventListener("abort", () =>
            reject(
              new DOMException("The operation was aborted.", "AbortError"),
            ),
          );
        }),
    );

    const { result } = renderHook(() => useTau());
    const source = FakeEventSource.instances[0];

    await act(async () => {
      source.emit("snapshot", { snapshot: makeSnapshot() });
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(result.current.allowlist).toEqual(before);
    expect(allowlistCalls).toBe(1);

    // Start an add that stalls: the next poll skips the allow-list
    // while the write is pending.
    let addSettled = false;
    act(() => {
      result.current.addAllowlistPeer(PEER_HASH).then(
        () => {
          addSettled = true;
        },
        () => {
          addSettled = true;
        },
      );
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(15000);
    });
    expect(allowlistCalls).toBe(1);
    expect(addSettled).toBe(false);

    // The 30s write timeout aborts the stalled POST, the pending
    // counter clears, and the next tick polls again.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(30000);
    });
    expect(addSettled).toBe(true);
    expect(allowlistCalls).toBeGreaterThanOrEqual(2);
  });
});

describe("useTau acceptDmRequest", () => {
  it("does not let a stale messages refresh resurrect a retracted message, and refreshes the allow-list", async () => {
    const msg = makeMessage({ id: 2, text: "held hello" });
    const firstAllowlist = { enabled: true, entries: [] };
    const secondAllowlist = {
      enabled: true,
      entries: [{ destination: PEER_HASH, name: "alice" }],
    };
    let allowlistCalls = 0;
    mockedGet.mockImplementation((path: string) => {
      if (path === "/api/allowlist") {
        allowlistCalls += 1;
        return Promise.resolve(
          allowlistCalls === 1 ? firstAllowlist : secondAllowlist,
        );
      }
      if (path === "/api/messages/requests") {
        return Promise.resolve({ requests: [], count: 0 });
      }
      if (path === "/api/messages?limit=200") {
        // A stale response that still carries the retracted message.
        return Promise.resolve({ messages: [msg], count: 1 });
      }
      if (path === "/api/peers") {
        return Promise.resolve({ peers: [], count: 0 });
      }
      if (path === "/api/outbox") {
        return Promise.resolve({ outbox: [], count: 0, held_for_others: 0 });
      }
      return Promise.resolve({ groups: [], count: 0 });
    });
    mockedPost.mockResolvedValue({
      accepted: {
        id: "req-1",
        sender: PEER_HASH,
        sender_name: "alice",
        text: "held hello",
        kind: "message",
        sent_at: 1000,
        received_at: 1000,
        request_expires_at: 1000 + 7 * 24 * 60 * 60,
      },
      delivered: true,
      allowlisted: true,
      skipped: null,
    });

    const { result } = renderHook(() => useTau());
    const source = FakeEventSource.instances[0];

    await act(async () => {
      source.emit("snapshot", { snapshot: makeSnapshot([makePeer()], [msg]) });
    });
    expect(result.current.messages).toEqual([msg]);

    // The message is retracted while the accept's refresh is in flight.
    await act(async () => {
      source.emit("message_deleted", { message_id: 2 });
    });
    expect(result.current.messages).toEqual([]);

    await act(async () => {
      await result.current.acceptDmRequest("req-1");
    });

    // The stale refresh must not put the retracted message back.
    expect(result.current.messages).toEqual([]);
    // The accept changed the running node's allow-list, so the panel
    // must refresh instead of waiting for the next 15s poll.
    expect(allowlistCalls).toBeGreaterThanOrEqual(2);
    expect(result.current.allowlist).toEqual(secondAllowlist);
  });
});

describe("useTau archived conversations", () => {
  const OTHER_HASH = "cd".repeat(16);
  const archivedList = {
    archived: [{ destination: PEER_HASH, name: "alice", messages: 2 }],
    count: 1,
  };

  function mockArchivedGet(list = archivedList) {
    mockedGet.mockImplementation((path: string) =>
      path === "/api/conversations/archived"
        ? Promise.resolve(list)
        : path === "/api/peers"
          ? Promise.resolve({ peers: [], count: 0 })
          : path === "/api/outbox"
            ? Promise.resolve({ outbox: [], count: 0, held_for_others: 0 })
            : Promise.resolve({ groups: [], count: 0 }),
    );
  }

  it("archives a thread at once and clears its unread badge", async () => {
    mockArchivedGet({ archived: [], count: 0 });
    const alice = makePeer();
    const carol = makePeer({ destination: OTHER_HASH, name: "carol" });
    const thread = [
      makeMessage({ id: 1, peer: PEER_HASH, group: undefined, text: "hi" }),
      makeMessage({
        id: 2,
        peer: PEER_HASH,
        group: undefined,
        direction: "out",
        text: "yo",
      }),
      makeMessage({
        id: 3,
        peer: OTHER_HASH,
        peer_name: "carol",
        group: undefined,
        text: "hey",
      }),
    ];
    const { result, unmount } = renderHook(() => useTau());
    const source = FakeEventSource.instances[0];
    await act(async () => {
      source.emit("snapshot", {
        snapshot: {
          ...makeSnapshot([alice, carol], thread),
          unread: { per_peer: { [PEER_HASH]: 1 }, total: 1 },
        },
      });
    });
    await waitFor(() =>
      expect(result.current.messages.map((m) => m.id)).toEqual([1, 2, 3]),
    );

    mockArchivedGet();
    mockedPost.mockResolvedValue({
      ok: true,
      archive: {
        destination: PEER_HASH,
        name: "alice",
        archived: true,
        changed: true,
      },
    });
    await act(async () => {
      await result.current.archiveConversation(PEER_HASH);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/conversations/archive", {
      target: PEER_HASH,
    });
    // Both directions of the thread drop; the other peer stays.
    expect(result.current.messages.map((m) => m.id)).toEqual([3]);
    expect(result.current.unread).toEqual({
      per_peer: {},
      total: 0,
      revision: undefined,
    });
    expect(result.current.archivedConversations).toEqual(
      archivedList.archived,
    );
    unmount();
  });

  it("restores a thread by re-reading the feed", async () => {
    mockArchivedGet();
    const { result, unmount } = renderHook(() => useTau());
    const source = FakeEventSource.instances[0];
    await act(async () => {
      source.emit("snapshot", { snapshot: makeSnapshot([makePeer()], []) });
    });

    const restored = makeMessage({
      id: 1,
      peer: PEER_HASH,
      group: undefined,
      text: "back",
    });
    mockedGet.mockImplementation((path: string) =>
      path === "/api/messages?limit=200"
        ? Promise.resolve({ messages: [restored] })
        : path === "/api/conversations/archived"
          ? Promise.resolve({ archived: [], count: 0 })
          : path === "/api/peers"
            ? Promise.resolve({ peers: [], count: 0 })
            : path === "/api/outbox"
              ? Promise.resolve({ outbox: [], count: 0, held_for_others: 0 })
              : Promise.resolve({ groups: [], count: 0 }),
    );
    mockedPost.mockResolvedValue({
      ok: true,
      archive: {
        destination: PEER_HASH,
        name: "alice",
        archived: false,
        changed: true,
      },
    });
    await act(async () => {
      await result.current.unarchiveConversation(PEER_HASH);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/conversations/unarchive", {
      target: PEER_HASH,
    });
    expect(result.current.messages.map((m) => m.id)).toEqual([1]);
    expect(result.current.archivedConversations).toEqual([]);
    unmount();
  });

  it("drops live messages for an archived peer, keeps the rest", async () => {
    mockArchivedGet();
    const { result, unmount } = renderHook(() => useTau());
    const source = FakeEventSource.instances[0];
    await act(async () => {
      source.emit("snapshot", {
        snapshot: makeSnapshot([makePeer()], [
          makeMessage({ id: 1, peer: PEER_HASH, group: undefined }),
        ]),
      });
    });
    await waitFor(() =>
      expect(result.current.archivedConversations).toEqual(
        archivedList.archived,
      ),
    );

    // A new arrival for the archived peer stays hidden; one for
    // another peer lands in the feed. Group posts are never archived.
    // The snapshot DM from the archived peer is hidden too, once the
    // archived list lands and reconciles the open views.
    await act(async () => {
      source.emit("message", {
        message: makeMessage({
          id: 2,
          peer: PEER_HASH,
          group: undefined,
          text: "still hidden",
        }),
      });
      source.emit("message", {
        message: makeMessage({
          id: 3,
          peer: OTHER_HASH,
          peer_name: "carol",
          group: undefined,
          text: "visible",
        }),
      });
      source.emit("message", {
        message: makeMessage({
          id: 4,
          peer: PEER_HASH,
          text: "group post",
        }),
      });
    });
    expect(result.current.messages.map((m) => m.id)).toEqual([3, 4]);
    unmount();
  });
});
