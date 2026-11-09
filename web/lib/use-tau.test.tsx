import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type { Snapshot, TauGroup, TauMessage } from "./types";
import { useTau } from "./use-tau";

vi.mock("./api", () => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
  eventsUrl: vi.fn(() => "/api/events?since=0"),
}));

import { apiGet, apiPost } from "./api";

const mockedGet = apiGet as Mock;
const mockedPost = apiPost as Mock;

type Listener = (event: Event) => void;

// The hook reads its snapshot over server-sent events, so the test
// drives a stand-in EventSource instead of a real connection.
class FakeEventSource {
  static instances: FakeEventSource[] = [];
  listeners: Record<string, Listener[]> = {};

  constructor(_url: string) {
    FakeEventSource.instances.push(this);
  }

  addEventListener(type: string, fn: Listener) {
    (this.listeners[type] ??= []).push(fn);
  }

  removeEventListener(type: string, fn: Listener) {
    this.listeners[type] = (this.listeners[type] ?? []).filter(
      (known) => known !== fn,
    );
  }

  close() {}

  emit(type: string, data: unknown) {
    const event = { data: JSON.stringify(data) } as MessageEvent;
    for (const fn of this.listeners[type] ?? []) fn(event);
  }
}

const GROUP_ID = "a".repeat(32);
const PEER_HASH = "ab".repeat(16);
const BOB_HASH = "ef".repeat(16);

function makeMessage(overrides: Partial<TauMessage> = {}): TauMessage {
  return {
    id: 1,
    direction: "out",
    peer: PEER_HASH,
    peer_name: "alice",
    text: "hi all",
    sent_at: 1000,
    received_at: 1001,
    status: "delivered",
    group: GROUP_ID,
    group_name: "crew",
    ...overrides,
  };
}

function makeSnapshot(): Snapshot {
  const group: TauGroup = {
    id: GROUP_ID,
    name: "crew",
    members: [
      { hash: PEER_HASH, name: "alice" },
      { hash: BOB_HASH, name: "bob" },
    ],
    created_at: 1000,
  };
  return {
    node: {
      name: "tester",
      destination: "cd".repeat(16),
      protocol: 1,
      version: "0.1.0",
      rns_version: "test",
      uptime: 12.5,
      peers: 1,
      interfaces: [{ name: "TCP", online: true }],
    },
    peers: [
      {
        destination: PEER_HASH,
        name: "alice",
        last_seen: 1000,
        age: 0,
        hops: 1,
        protocol: 1,
      },
    ],
    groups: [group],
    messages: [],
    counts: { messages: 0, peers: 1 },
    unread: { per_peer: {}, total: 0 },
    bridge: { version: "0.1.0", web: true, url: "http://127.0.0.1:8787" },
  };
}

beforeEach(() => {
  vi.resetAllMocks();
  FakeEventSource.instances = [];
  vi.stubGlobal("EventSource", FakeEventSource);
  mockedGet.mockImplementation((path: string) =>
    path === "/api/outbox"
      ? Promise.resolve({ outbox: [], count: 0, held_for_others: 0 })
      : Promise.resolve({ groups: [], count: 0 }),
  );
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
        "bob",
      );
    });

    expect(result.current.messages).toEqual(legs);
  });
});
