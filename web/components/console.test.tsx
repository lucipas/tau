import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { Peer, TauGroup } from "@/lib/types";

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

function tauState(groups: TauGroup[], peers: Peer[] = []) {
  return {
    node: null,
    peers,
    groups,
    messages: [],
    unread: { per_peer: {}, total: 0 },
    outbox: [],
    heldForOthers: 0,
    bridge: null,
    connection: "live",
    pings: {},
    infos: {},
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
    dmRequests: [],
    acceptDmRequest: vi.fn(),
    declineDmRequest: vi.fn(),
    refreshDmRequests: vi.fn(),
    archivedConversations: [],
    archivedError: null,
    archiveConversation: vi.fn(),
    unarchiveConversation: vi.fn(),
    refreshArchived: vi.fn(),
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

  it("follows the selected group to its successor after a roster change", async () => {
    const team = makeGroup("b".repeat(32), "team");
    const crew = makeGroup("a".repeat(32), "crew");
    crew.created_by_me = true;
    const successor = makeGroup("c".repeat(32), "crew");
    successor.members = [{ hash: "ab".repeat(16), name: "alice" }];
    successor.created_by_me = true;
    // The mock hook refreshes its groups and records the swap when the
    // change lands, as the real hook does.
    const removeGroupMembers = vi.fn(async () => {
      mockedUseTau.mockReturnValue({
        ...tauState([team, successor]),
        groupSwap: { from: crew.id, to: successor.id },
        removeGroupMembers,
      });
      return { ok: true, group: successor, previous_id: crew.id };
    });
    mockedUseTau.mockReturnValue({
      ...tauState([team, crew]),
      removeGroupMembers,
    });

    const { rerender } = render(<Console />);
    await waitFor(() =>
      expect(screen.getByDisplayValue("team · 2 members")).toBeTruthy(),
    );
    // Select the second group, crew, as the composer target.
    fireEvent.change(screen.getByDisplayValue("team · 2 members"), {
      target: { value: crew.id },
    });
    await waitFor(() =>
      expect(screen.getByDisplayValue("crew · 2 members")).toBeTruthy(),
    );

    // Remove bob from crew from the sidebar.
    fireEvent.click(screen.getByRole("button", { name: "members" }));
    fireEvent.click(screen.getByRole("checkbox", { name: /bob/ }));
    fireEvent.click(screen.getByRole("button", { name: "remove selected" }));

    await waitFor(() =>
      expect(removeGroupMembers).toHaveBeenCalledWith(crew.id, [
        "ef".repeat(16),
      ]),
    );
    // The hook update lands as a new render, as in the real app: the
    // groups list and the recorded swap arrive together.
    rerender(<Console />);
    // The composer still addresses crew — now one member — instead
    // of falling back to team, the first group in the list.
    await waitFor(() =>
      expect(screen.getByDisplayValue("crew · 1 member")).toBeTruthy(),
    );
  });
});

describe("Console archived target", () => {
  function makePeer(destination: string, name: string): Peer {
    return {
      destination,
      name,
      last_seen: Date.now() / 1000,
      age: 5,
      hops: 1,
      protocol: 1,
    };
  }

  it("retargets to a remaining peer when the selected conversation is archived", async () => {
    const alice = makePeer("ab".repeat(16), "alice");
    const bob = makePeer("cd".repeat(16), "bob");
    mockedUseTau.mockReturnValue(tauState([], [alice, bob]));

    const { rerender } = render(<Console />);
    await waitFor(() =>
      expect(screen.getByDisplayValue("alice · abababab")).toBeTruthy(),
    );

    // Another tab archived the selected conversation; the poll drops it.
    mockedUseTau.mockReturnValue({
      ...tauState([], [alice, bob]),
      archivedConversations: [
        { destination: alice.destination, name: "alice", messages: 2 },
      ],
    });
    rerender(<Console />);
    await waitFor(() =>
      expect(screen.getByDisplayValue("bob · cdcdcdcd")).toBeTruthy(),
    );
  });

  it("auto-select skips an already-archived peer", async () => {
    const alice = makePeer("ab".repeat(16), "alice");
    const bob = makePeer("cd".repeat(16), "bob");
    mockedUseTau.mockReturnValue({
      ...tauState([], [alice, bob]),
      archivedConversations: [
        { destination: alice.destination, name: "alice", messages: 2 },
      ],
    });

    render(<Console />);
    await waitFor(() =>
      expect(screen.getByDisplayValue("bob · cdcdcdcd")).toBeTruthy(),
    );
  });
});
