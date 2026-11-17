import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { Peer, TauGroup } from "@/lib/types";

import { Console } from "./console";

const { mockedUseTau } = vi.hoisted(() => ({ mockedUseTau: vi.fn() }));

vi.mock("@/lib/use-tau", () => ({ useTau: mockedUseTau }));

// The social tab embeds the real feed; its internals are covered by
// social-console.test.tsx, so the tab tests only need its presence.
vi.mock("./social-console", () => ({
  SocialConsole: () => <div data-testid="social-tab-feed">social feed</div>,
}));

// Same for the channels tab: channels-console.test.tsx covers the view.
vi.mock("./channels-console", () => ({
  ChannelsConsole: () => (
    <div data-testid="channels-tab-feed">channels feed</div>
  ),
}));

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
    leftGroups: [],
    ignoredGroups: [],
    leaveGroup: vi.fn(),
    rejoinGroup: vi.fn(),
    ignoreGroup: vi.fn(),
    unignoreGroup: vi.fn(),
    messages: [],
    unread: { per_peer: {}, per_group: {}, total: 0 },
    restoredUnread: null,
    typing: {},
    sendTyping: vi.fn(),
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
    disappearingDefaults: { peers: {}, groups: {} },
    disappearingError: null,
    setDisappearingDefault: vi.fn(),
    refreshDisappearing: vi.fn(),
    mutedConversations: { peers: [], groups: [] },
    mutedError: null,
    setMuted: vi.fn(),
    refreshMuted: vi.fn(),
    retryOutbox: vi.fn(),
    ping: vi.fn(),
    announce: vi.fn(),
    markRead: vi.fn().mockResolvedValue({ per_peer: {}, per_group: {}, total: 0 }),
    markUnread: vi.fn().mockResolvedValue({ per_peer: {}, per_group: {}, total: 0 }),
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

  it("marks the open group read by group id, not by a member peer", async () => {
    const crew = makeGroup("a".repeat(32), "crew");
    const markRead = vi.fn().mockResolvedValue({ per_peer: {}, per_group: {}, total: 0 });
    mockedUseTau.mockReturnValue({
      ...tauState([crew]),
      markRead,
      unread: { per_peer: {}, per_group: { [crew.id]: 1 }, total: 1 },
    });

    render(<Console />);
    await waitFor(() =>
      expect(screen.getByDisplayValue("crew · 2 members")).toBeTruthy(),
    );
    await waitFor(() =>
      expect(markRead).toHaveBeenCalledWith(undefined, crew.id),
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

describe("Console mark unread", () => {
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

  it("holds the badge for the open conversation until new mail arrives", async () => {
    const alice = makePeer("ab".repeat(16), "alice");
    const bob = makePeer("cd".repeat(16), "bob");
    const markRead = vi.fn().mockResolvedValue({ per_peer: {}, per_group: {}, total: 0 });
    const markUnread = vi.fn().mockResolvedValue({
      per_peer: { [alice.destination]: 1 },
      per_group: {},
      total: 1,
    });
    mockedUseTau.mockReturnValue({
      ...tauState([], [alice, bob]),
      markRead,
      markUnread,
    });

    const { rerender } = render(<Console />);
    await waitFor(() =>
      expect(screen.getByDisplayValue("alice · abababab")).toBeTruthy(),
    );
    expect(markRead).not.toHaveBeenCalled();

    // Mark the open conversation unread: the badge rises, and the
    // auto-read effect must not undo it at once.
    fireEvent.click(
      screen.getByRole("button", {
        name: "Mark conversation with alice unread",
      }),
    );
    await waitFor(() =>
      expect(markUnread).toHaveBeenCalledWith(alice.destination, false),
    );
    // The mark-all action forwards its flag too.
    fireEvent.click(
      screen.getByRole("button", {
        name: "Mark whole conversation with alice unread",
      }),
    );
    await waitFor(() =>
      expect(markUnread).toHaveBeenCalledWith(alice.destination, true),
    );
    mockedUseTau.mockReturnValue({
      ...tauState([], [alice, bob]),
      markRead,
      markUnread,
      unread: { per_peer: { [alice.destination]: 1 }, per_group: {}, total: 1 },
    });
    rerender(<Console />);
    await waitFor(() =>
      expect(screen.getByTitle("1 unread message")).toBeTruthy(),
    );
    expect(markRead).not.toHaveBeenCalled();

    // Read elsewhere clears the hold: the count drops to zero, and the
    // next message auto-reads again instead of sticking at the held level.
    mockedUseTau.mockReturnValue({
      ...tauState([], [alice, bob]),
      markRead,
      markUnread,
      unread: { per_peer: {}, per_group: {}, total: 0 },
    });
    rerender(<Console />);
    mockedUseTau.mockReturnValue({
      ...tauState([], [alice, bob]),
      markRead,
      markUnread,
      unread: { per_peer: { [alice.destination]: 1 }, per_group: {}, total: 1 },
    });
    rerender(<Console />);
    await waitFor(() =>
      expect(markRead).toHaveBeenCalledWith(alice.destination),
    );

    // New mail above the held level releases the hold: the open
    // conversation reads as read again.
    mockedUseTau.mockReturnValue({
      ...tauState([], [alice, bob]),
      markRead,
      markUnread,
      unread: { per_peer: { [alice.destination]: 2 }, per_group: {}, total: 2 },
    });
    rerender(<Console />);
    await waitFor(() =>
      expect(markRead).toHaveBeenCalledWith(alice.destination),
    );
  });

  it("does not reinstall a hold after the conversation was reopened", async () => {
    const alice = makePeer("ab".repeat(16), "alice");
    const bob = makePeer("cd".repeat(16), "bob");
    const markRead = vi.fn().mockResolvedValue({ per_peer: {}, per_group: {}, total: 0 });
    let resolveMark!: (value: {
      per_peer: Record<string, number>;
      per_group: Record<string, number>;
      total: number;
    }) => void;
    const markUnread = vi.fn(
      () =>
        new Promise<{
          per_peer: Record<string, number>;
          per_group: Record<string, number>;
          total: number;
        }>((resolve) => {
          resolveMark = resolve;
        }),
    );
    mockedUseTau.mockReturnValue({
      ...tauState([], [alice, bob]),
      markRead,
      markUnread,
    });

    const { rerender } = render(<Console />);
    await waitFor(() =>
      expect(screen.getByDisplayValue("alice · abababab")).toBeTruthy(),
    );

    // Start a mark-unread, then reopen the conversation before it lands.
    fireEvent.click(
      screen.getByRole("button", {
        name: "Mark conversation with alice unread",
      }),
    );
    await waitFor(() =>
      expect(markUnread).toHaveBeenCalledWith(alice.destination, false),
    );
    fireEvent.click(
      screen.getByTitle(`Message alice (${alice.destination})`),
    );
    await waitFor(() =>
      expect(markRead).toHaveBeenCalledWith(alice.destination),
    );

    // The late response must not reinstall the hold: the count it
    // produced auto-reads like any new mail.
    await act(async () => {
      resolveMark({ per_peer: { [alice.destination]: 1 }, per_group: {}, total: 1 });
    });
    mockedUseTau.mockReturnValue({
      ...tauState([], [alice, bob]),
      markRead,
      markUnread,
      unread: { per_peer: { [alice.destination]: 1 }, per_group: {}, total: 1 },
    });
    rerender(<Console />);
    await waitFor(() => expect(markRead).toHaveBeenCalledTimes(2));
  });
});

describe("Console restored unread", () => {
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

  it("holds mail restored by another console until new mail arrives", async () => {
    const alice = makePeer("ab".repeat(16), "alice");
    const bob = makePeer("cd".repeat(16), "bob");
    const markRead = vi.fn().mockResolvedValue({ per_peer: {}, per_group: {}, total: 0 });
    mockedUseTau.mockReturnValue({
      ...tauState([], [alice, bob]),
      markRead,
    });

    const { rerender } = render(<Console />);
    await waitFor(() =>
      expect(screen.getByDisplayValue("alice · abababab")).toBeTruthy(),
    );
    expect(markRead).not.toHaveBeenCalled();

    // Another console unmutes the open conversation: the badge rises
    // with restored mail, and the auto-read effect must not clear it.
    mockedUseTau.mockReturnValue({
      ...tauState([], [alice, bob]),
      markRead,
      unread: { per_peer: { [alice.destination]: 1 }, per_group: {}, total: 1 },
      restoredUnread: [{ target: alice.destination, unread: 1 }],
    });
    rerender(<Console />);
    await waitFor(() =>
      expect(screen.getByTitle("1 unread message")).toBeTruthy(),
    );
    expect(markRead).not.toHaveBeenCalled();

    // New mail above the held level releases the hold: the open
    // conversation reads as read again.
    mockedUseTau.mockReturnValue({
      ...tauState([], [alice, bob]),
      markRead,
      unread: { per_peer: { [alice.destination]: 2 }, per_group: {}, total: 2 },
    });
    rerender(<Console />);
    await waitFor(() =>
      expect(markRead).toHaveBeenCalledWith(alice.destination),
    );
  });

  it("holds restored group mail in the open group the same way", async () => {
    const crew = makeGroup("a".repeat(32), "crew");
    const markRead = vi.fn().mockResolvedValue({ per_peer: {}, per_group: {}, total: 0 });
    mockedUseTau.mockReturnValue({
      ...tauState([crew], []),
      markRead,
    });

    const { rerender } = render(<Console />);
    await waitFor(() =>
      expect(screen.getByDisplayValue("crew · 2 members")).toBeTruthy(),
    );
    expect(markRead).not.toHaveBeenCalled();

    // Another console unignores the open group: its restored posts
    // must not auto-read.
    mockedUseTau.mockReturnValue({
      ...tauState([crew], []),
      markRead,
      unread: { per_peer: {}, per_group: { [crew.id]: 1 }, total: 1 },
      restoredUnread: [{ target: crew.id, unread: 1 }],
    });
    rerender(<Console />);
    await waitFor(() =>
      expect(screen.getByTitle("1 unread message")).toBeTruthy(),
    );
    expect(markRead).not.toHaveBeenCalled();

    // A new post above the held level releases the hold.
    mockedUseTau.mockReturnValue({
      ...tauState([crew], []),
      markRead,
      unread: { per_peer: {}, per_group: { [crew.id]: 2 }, total: 2 },
    });
    rerender(<Console />);
    await waitFor(() =>
      expect(markRead).toHaveBeenCalledWith(undefined, crew.id),
    );
  });

  it("clears the hold on leaving, so returning reads as read", async () => {
    const crew = makeGroup("a".repeat(32), "crew");
    const team = makeGroup("b".repeat(32), "team");
    const markRead = vi.fn().mockResolvedValue({ per_peer: {}, per_group: {}, total: 0 });
    mockedUseTau.mockReturnValue({
      ...tauState([crew, team], []),
      markRead,
    });

    const { rerender } = render(<Console />);
    await waitFor(() =>
      expect(screen.getByDisplayValue("crew · 2 members")).toBeTruthy(),
    );
    expect(markRead).not.toHaveBeenCalled();

    // Another console unmutes the open group: its restored post is held.
    mockedUseTau.mockReturnValue({
      ...tauState([crew, team], []),
      markRead,
      unread: { per_peer: {}, per_group: { [crew.id]: 1 }, total: 1 },
      restoredUnread: [{ target: crew.id, unread: 1 }],
    });
    rerender(<Console />);
    await waitFor(() =>
      expect(screen.getByTitle("1 unread message")).toBeTruthy(),
    );
    expect(markRead).not.toHaveBeenCalled();

    // Leaving the group drops the hold, so returning to it follows
    // the normal read path instead of skipping markRead.
    fireEvent.change(screen.getByDisplayValue("crew · 2 members"), {
      target: { value: team.id },
    });
    await waitFor(() =>
      expect(screen.getByDisplayValue("team · 2 members")).toBeTruthy(),
    );
    fireEvent.change(screen.getByDisplayValue("team · 2 members"), {
      target: { value: crew.id },
    });
    await waitFor(() =>
      expect(screen.getByDisplayValue("crew · 2 members")).toBeTruthy(),
    );
    await waitFor(() =>
      expect(markRead).toHaveBeenCalledWith(undefined, crew.id),
    );
  });
});

describe("Console tabs", () => {
  // A tab switch writes the tab into the URL hash; clear it between
  // tests so each one starts on the chat tab.
  afterEach(() => {
    window.location.hash = "";
  });

  // The chat and social panels stay mounted while hidden, so a
  // section's presence in the document is not visibility: only a
  // panel without a `hidden` ancestor is actually shown.
  function isShown(element: HTMLElement | null): boolean {
    return element !== null && element.closest(".hidden") === null;
  }

  function tabState(overrides: Record<string, unknown> = {}) {
    return {
      ...tauState([], []),
      settings: { accept_files: true, send_read_receipts: true },
      settingsError: null,
      updateSetting: vi.fn(),
      exporting: false,
      importing: false,
      exportMessages: vi.fn(),
      importMessages: vi.fn(),
      exportBundle: vi.fn(),
      importBundle: vi.fn(),
      runDoctor: vi.fn(),
      backup: vi.fn(),
      backingUp: false,
      backupError: null,
      // Without these the feed reads pinnedMessages as undefined,
      // which counts as the pinned view instead of traffic.
      pinnedMessages: null,
      pinnedError: null,
      showPinned: vi.fn(),
      clearPinned: vi.fn(),
      ...overrides,
    };
  }

  it("shows the chat tab by default and hides the other tabs", () => {
    mockedUseTau.mockReturnValue(tabState());
    render(<Console />);

    expect(isShown(screen.getByText("traffic"))).toBe(true);
    expect(isShown(screen.queryByText("sneakernet"))).toBe(false);
    expect(isShown(screen.queryByText("dm allow-list"))).toBe(false);
    expect(isShown(screen.queryByTestId("social-tab-feed"))).toBe(false);
  });

  it("switches to the tools tab on click", () => {
    mockedUseTau.mockReturnValue(tabState());
    render(<Console />);

    fireEvent.click(screen.getByRole("tab", { name: "tools" }));

    expect(isShown(screen.getByText("sneakernet"))).toBe(true);
    expect(isShown(screen.getByText("health"))).toBe(true);
    expect(isShown(screen.getByText("backup"))).toBe(true);
    expect(isShown(screen.getByText("node backup"))).toBe(true);
    expect(isShown(screen.queryByText("traffic"))).toBe(false);
  });

  it("switches to the channels tab on click", () => {
    mockedUseTau.mockReturnValue(tabState());
    render(<Console />);

    fireEvent.click(screen.getByRole("tab", { name: "channels" }));

    expect(isShown(screen.getByTestId("channels-tab-feed"))).toBe(true);
    expect(isShown(screen.queryByText("traffic"))).toBe(false);
  });

  it("switches to the settings tab on click", () => {
    mockedUseTau.mockReturnValue(tabState());
    render(<Console />);

    fireEvent.click(screen.getByRole("tab", { name: "settings" }));

    expect(isShown(screen.getByText("dm allow-list"))).toBe(true);
    expect(isShown(screen.queryByText("sneakernet"))).toBe(false);
  });

  it("opens on the tab named by the URL hash", () => {
    window.location.hash = "#tools";
    mockedUseTau.mockReturnValue(tabState());
    render(<Console />);

    expect(isShown(screen.getByText("sneakernet"))).toBe(true);
    expect(isShown(screen.queryByText("traffic"))).toBe(false);
  });

  it("jumps between tabs with Alt+1 through Alt+5", () => {
    mockedUseTau.mockReturnValue(tabState());
    render(<Console />);

    fireEvent.keyDown(document.body, { key: "4", code: "Digit4", altKey: true });
    expect(isShown(screen.getByText("sneakernet"))).toBe(true);

    fireEvent.keyDown(document.body, { key: "3", code: "Digit3", altKey: true });
    expect(isShown(screen.getByTestId("channels-tab-feed"))).toBe(true);

    fireEvent.keyDown(document.body, { key: "2", code: "Digit2", altKey: true });
    expect(isShown(screen.getByTestId("social-tab-feed"))).toBe(true);

    fireEvent.keyDown(document.body, { key: "1", code: "Digit1", altKey: true });
    expect(isShown(screen.getByText("traffic"))).toBe(true);
  });

  it("matches Alt+digit on the physical key, not the produced character", () => {
    mockedUseTau.mockReturnValue(tabState());
    render(<Console />);

    // macOS Option+2 produces a symbol in event.key.
    fireEvent.keyDown(document.body, { key: "™", code: "Digit2", altKey: true });
    expect(isShown(screen.getByTestId("social-tab-feed"))).toBe(true);
  });

  it("selecting a sidebar peer switches back to the chat tab", async () => {
    const alice = {
      destination: "ab".repeat(16),
      name: "alice",
      last_seen: Date.now() / 1000,
      age: 5,
      hops: 1,
      protocol: 1,
    };
    window.location.hash = "#tools";
    mockedUseTau.mockReturnValue(tabState({ peers: [alice] }));
    render(<Console />);
    expect(isShown(screen.getByText("sneakernet"))).toBe(true);

    fireEvent.click(
      screen.getByTitle(`Message alice (${alice.destination})`),
    );

    await waitFor(() =>
      expect(isShown(screen.getByText("traffic"))).toBe(true),
    );
    expect(isShown(screen.queryByText("sneakernet"))).toBe(false);
  });

  it("keeps the composer draft across a tab switch", () => {
    mockedUseTau.mockReturnValue(tabState());
    render(<Console />);

    const composer = screen.getByPlaceholderText(
      "Write something to the mesh…",
    ) as HTMLTextAreaElement;
    fireEvent.change(composer, { target: { value: "draft kept" } });

    fireEvent.click(screen.getByRole("tab", { name: "tools" }));
    fireEvent.click(screen.getByRole("tab", { name: "chat" }));

    expect(
      (screen.getByPlaceholderText(
        "Write something to the mesh…",
      ) as HTMLTextAreaElement).value,
    ).toBe("draft kept");
  });

  it("offers the whole-node backup download in the tools tab", () => {
    const backup = vi.fn();
    mockedUseTau.mockReturnValue(
      tabState({
        node: {
          name: "tester",
          destination: "cd".repeat(16),
          protocol: 1,
          version: "0.1.0",
          rns_version: "test",
          uptime: 1,
          peers: 0,
          interfaces: [],
        },
        backup,
      }),
    );
    render(<Console />);

    fireEvent.click(screen.getByRole("tab", { name: "tools" }));

    fireEvent.click(
      screen.getByRole("button", { name: "download backup" }),
    );
    expect(backup).toHaveBeenCalledTimes(1);
  });

  it("starts the outbox drawer open when mail is queued, and collapses it", () => {
    mockedUseTau.mockReturnValue(
      tabState({
        outbox: [
          {
            id: "1",
            to: "ab".repeat(16),
            to_name: "alice",
            text: "hello queued",
            created_at: 1000,
            expires_at: 2000,
            attempts: 0,
            last_attempt_at: null,
            last_error: null,
            held_by: [],
            scheduled_at: null,
          },
        ],
      }),
    );
    render(<Console />);

    const toggle = screen.getByRole("button", { name: /outbox.*queued/ });
    expect(toggle.getAttribute("aria-expanded")).toBe("true");
    expect(isShown(screen.getByText("hello queued"))).toBe(true);

    fireEvent.click(toggle);
    expect(toggle.getAttribute("aria-expanded")).toBe("false");
    expect(isShown(screen.queryByText("hello queued"))).toBe(false);
  });
});

describe("Console typing indicator", () => {
  it("shows who is typing in the open group", async () => {
    const crew = makeGroup("a".repeat(32), "crew");
    mockedUseTau.mockReturnValue({
      ...tauState([crew]),
      typing: {
        [`group:${crew.id}`]: [
          {
            peer: "ab".repeat(16),
            name: "alice",
            receivedAt: Date.now() / 1000,
          },
        ],
      },
    });
    render(<Console />);
    await waitFor(() =>
      expect(screen.getByText("alice is typing…")).toBeTruthy(),
    );
  });

  it("hides the indicator for another conversation", async () => {
    const crew = makeGroup("a".repeat(32), "crew");
    mockedUseTau.mockReturnValue({
      ...tauState([crew]),
      typing: {
        ["ab".repeat(16)]: [
          {
            peer: "ab".repeat(16),
            name: "alice",
            receivedAt: Date.now() / 1000,
          },
        ],
      },
    });
    render(<Console />);
    await waitFor(() =>
      expect(screen.getByDisplayValue("crew · 2 members")).toBeTruthy(),
    );
    expect(screen.queryByText(/is typing/)).toBeNull();
  });
});
