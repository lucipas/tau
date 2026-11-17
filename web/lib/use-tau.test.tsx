// use-tau.test.tsx — live snapshot behaviour with a fake WebSocket and a
// mocked fetch, plus the pure conversation/traffic builders. The fake
// socket is driven by deferred promises: the test decides when frames
// arrive, exactly like the bridge decides when events arrive.

import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  buildConversations,
  buildTraffic,
  useTau,
} from "./use-tau";
import type { ChatMessage, Group, Peer, Snapshot } from "./types";

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
    unread: { per_peer: {}, per_group: {}, total: 0, revision: 1 },
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
      ws.receive({ type: "message", message: live, unread: { per_peer: { ["a".repeat(32)]: 1 }, per_group: {}, total: 1, revision: 2 } });
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
    const unread = { per_peer: { [bob.destination]: 2 }, per_group: { [group.id]: 1 }, total: 3, revision: 1 };
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
