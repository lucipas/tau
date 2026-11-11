import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { TauGroup } from "@/lib/types";

import { Console } from "./console";

const { mockedUseTau } = vi.hoisted(() => ({ mockedUseTau: vi.fn() }));

vi.mock("@/lib/use-tau", () => ({ useTau: mockedUseTau }));

// jsdom does not implement scrollIntoView, which the feed calls to
// stick to the newest message.
if (!Element.prototype.scrollIntoView) {
  Element.prototype.scrollIntoView = () => {};
}

function makeGroup(id: string, name: string): TauGroup {
  return {
    id,
    name,
    members: [
      { hash: "ab".repeat(16), name: "alice" },
      { hash: "ef".repeat(16), name: "bob" },
    ],
    created_at: 1000,
  };
}

function tauState(groups: TauGroup[]) {
  return {
    node: null,
    peers: [],
    groups,
    messages: [],
    unread: { per_peer: {}, total: 0 },
    outbox: [],
    heldForOthers: 0,
    bridge: null,
    connection: "live",
    pings: {},
    announcing: false,
    send: vi.fn(),
    sendGroup: vi.fn(),
    sendFile: vi.fn(),
    searchQuery: null,
    searchResults: [],
    searchError: null,
    search: vi.fn(),
    clearSearch: vi.fn(),
    blocks: {},
    verifications: {},
    social: null,
    showSafetyNumber: vi.fn(),
    verifyPeer: vi.fn(),
    clearVerification: vi.fn(),
    blockPeer: vi.fn(),
    unblockPeer: vi.fn(),
    allowlist: null,
    addAllowlistPeer: vi.fn(),
    removeAllowlistPeer: vi.fn(),
    setAllowlistEnabled: vi.fn(),
    refreshAllowlist: vi.fn(),
    heldMail: [],
    dropHeldMail: vi.fn(),
    refreshHeldMail: vi.fn(),
    retryOutbox: vi.fn(),
    ping: vi.fn(),
    announce: vi.fn(),
    markRead: vi.fn().mockResolvedValue({ per_peer: {}, total: 0 }),
    refreshOutbox: vi.fn(),
    refreshGroups: vi.fn(),
    addGroupMembers: vi.fn(),
    removeGroupMembers: vi.fn(),
    cancelOutbox: vi.fn(),
  };
}

describe("Console group target", () => {
  it("retargets to a remaining group when the selected one disappears", async () => {
    const crew = makeGroup("a".repeat(32), "crew");
    const team = makeGroup("b".repeat(32), "team");
    mockedUseTau.mockReturnValue(tauState([crew, team]));

    const { rerender } = render(<Console />);
    await waitFor(() =>
      expect(screen.getByDisplayValue("crew · 2 members")).toBeTruthy(),
    );

    // Another process deleted the selected group; the refresh drops it.
    mockedUseTau.mockReturnValue(tauState([team]));
    rerender(<Console />);
    await waitFor(() =>
      expect(screen.getByDisplayValue("team · 2 members")).toBeTruthy(),
    );
  });
});
