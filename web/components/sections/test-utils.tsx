// test-utils.tsx — shared fakes for the section tests: a minimal
// UseTauResult for TauContext.Provider, and a fetch router keyed on URL
// substrings (api.ts only ever calls response.text() + JSON.parse).

import { vi } from "vitest";
import type { UseTauResult } from "@/lib/use-tau";
import type { NodeInfo, Peer, Snapshot } from "@/lib/types";

export function fakeNode(overrides: Partial<NodeInfo> = {}): NodeInfo {
  return {
    name: "testnode",
    destination: "a".repeat(32),
    protocol: 1,
    version: "0.1.0",
    rns_version: "1.0.0",
    uptime: 100,
    peers: 1,
    mailbox: {
      outbox: 0,
      held: 0,
      seen: 0,
      sent: 0,
      receipts: 0,
      retracted: 0,
      sent_retracted: 0,
    },
    interfaces: [{ name: "test", online: true }],
    lxmf: { enabled: false, delivery: null, propagation: null },
    ...overrides,
  };
}

export function fakePeer(overrides: Partial<Peer> = {}): Peer {
  return {
    destination: "b".repeat(32),
    name: "bob",
    last_seen: 1000,
    age: 1,
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
    ...overrides,
  };
}

export function fakeTau(overrides: Partial<UseTauResult> = {}): UseTauResult {
  const node = fakeNode();
  const snapshot: Snapshot = {
    node,
    peers: [fakePeer()],
    groups: [],
    messages: [],
    counts: { messages: 0, peers: 1 },
    unread: { per_peer: {}, per_group: {}, per_channel: {}, total: 0, revision: 0 },
    bridge: { version: "0.1.0", web: true, url: "", notes_editable: true },
    social: { enabled: true, posts: 0 },
  };
  return {
    snapshot,
    node,
    peers: snapshot.peers,
    groups: [],
    messages: [],
    unread: snapshot.unread,
    socialEnabled: true,
    transport: "rest",
    connected: true,
    error: null,
    typing: {},
    refresh: vi.fn(async () => undefined),
    ...overrides,
  };
}

/** Route fetch calls: `handler(url, method)` returns the JSON payload. */
export function mockFetch(
  handler: (url: string, method: string) => unknown,
): { calls: string[] } {
  const calls: string[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      const method = init?.method ?? "GET";
      calls.push(`${method} ${url}`);
      const payload = await handler(url, method);
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify(payload ?? {}),
      } as Response;
    }),
  );
  return { calls };
}
