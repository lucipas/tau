// test-utils.tsx — shared fakes for component tests. Not a test file itself.

import { vi } from "vitest";
import type { UseTauResult } from "@/lib/use-tau";
import type { ChatMessage, Peer, UnreadCounts } from "@/lib/types";

export function makePeer(overrides: Partial<Peer> & { destination: string }): Peer {
  return {
    name: "Alice",
    last_seen: 1700000000,
    age: 60,
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

export function makeMessage(overrides: Partial<ChatMessage> & { id: number }): ChatMessage {
  return {
    direction: "in",
    peer: "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
    peer_name: "Alice",
    text: "hello",
    sent_at: 1700000000,
    received_at: 1700000000,
    status: "delivered",
    reactions: [],
    ...overrides,
  };
}

export function makeUnread(overrides: Partial<UnreadCounts> = {}): UnreadCounts {
  return { per_peer: {}, per_group: {}, per_channel: {}, total: 0, revision: 1, ...overrides };
}

export function fakeTau(overrides: Partial<UseTauResult> = {}): UseTauResult {
  return {
    snapshot: null,
    node: null,
    peers: [],
    groups: [],
    messages: [],
    unread: makeUnread(),
    socialEnabled: false,
    transport: "ws",
    connected: true,
    error: null,
    typing: {},
    refresh: vi.fn(async () => null),
    ...overrides,
  };
}

/** Stub global fetch for the bridge calls components make (DM requests,
 *  outbox, muted, archived, mark-read). Unknown paths get `{ ok: true }`. */
export function stubBridgeFetch() {
  const handler = (url: string) => {
    if (url.includes("/api/messages/requests")) return { requests: [], count: 0 };
    if (url.includes("/api/broadcasts")) return { broadcasts: [], count: 0 };
    if (url.includes("/api/outbox/held")) return { held: [], count: 0 };
    if (url.includes("/api/outbox")) return { outbox: [], count: 0, held_for_others: 0 };
    if (url.includes("/api/messages/muted")) return { peers: [], groups: [] };
    if (url.includes("/api/conversations/archived")) return { archived: [] };
    if (url.includes("/api/drafts")) return { drafts: [], count: 0 };
    if (url.includes("/api/messages/read")) {
      return { ok: true, unread: makeUnread() };
    }
    return { ok: true };
  };
  return vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string | URL | Request) => {
      const url = String(typeof input === "string" ? input : (input as Request).url ?? input);
      const body = handler(url);
      return { ok: true, status: 200, text: async () => JSON.stringify(body) };
    }),
  );
}
