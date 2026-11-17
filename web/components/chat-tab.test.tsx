"use client";

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { UseTauResult } from "@/lib/use-tau";
import ChatTab from "@/components/chat-tab";

const NOW = Math.floor(Date.now() / 1000);

function makeTau(overrides: Record<string, unknown> = {}): UseTauResult {
  const base = {
    node: {
      name: "me",
      destination: "ownhash00000000000000000000000001",
      protocol: 1,
      version: "0.1.0",
      rns_version: "1.0",
      uptime: 100,
      peers: 2,
      interfaces: [],
    },
    peers: [
      {
        destination: "peer1hash00000000000000000000000001",
        name: "alice",
        last_seen: NOW - 60,
        age: 60,
        hops: 2,
        protocol: 1,
        reachable: true,
        verified: true,
      },
      {
        destination: "peer2hash00000000000000000000000002",
        name: "bob",
        last_seen: NOW - 600,
        age: 600,
        hops: 3,
        protocol: 1,
        reachable: false,
        blocked: true,
      },
    ],
    groups: [
      {
        id: "group1",
        name: "mesh crew",
        members: [
          { hash: "peer1hash00000000000000000000000001", name: "alice" },
          { hash: "peer2hash00000000000000000000000002", name: "bob" },
        ],
        created_at: NOW - 86400,
        topic: "all things mesh",
      },
    ],
    leftGroups: [],
    ignoredGroups: [],
    messages: [
      {
        id: 1,
        direction: "in",
        peer: "peer1hash00000000000000000000000001",
        peer_name: "alice",
        text: "hello from the mesh",
        sent_at: NOW - 120,
        received_at: NOW - 120,
        status: "received",
        wire_id: "wire-1",
        excerpt_hash: "abc123",
      },
      {
        id: 2,
        direction: "out",
        peer: "peer1hash00000000000000000000000001",
        peer_name: "alice",
        text: "hello back",
        sent_at: NOW - 60,
        received_at: NOW - 60,
        status: "delivered",
        read_at: NOW - 30,
        wire_id: "wire-2",
        excerpt_hash: "def456",
      },
      {
        id: 3,
        direction: "in",
        peer: "peer2hash00000000000000000000000002",
        peer_name: "bob",
        group: "group1",
        group_name: "mesh crew",
        text: "group hello",
        sent_at: NOW - 30,
        received_at: NOW - 30,
        status: "received",
        wire_id: "wire-3",
      },
    ],
    unread: {
      per_peer: { peer1hash00000000000000000000000001: 2 },
      per_group: { group1: 1 },
      total: 3,
    },
    outbox: [
      {
        id: "out1",
        to: "peer2hash00000000000000000000000002",
        to_name: "bob",
        text: "queued hello",
        created_at: NOW - 300,
        expires_at: NOW + 86400,
        attempts: 1,
        last_attempt_at: NOW - 200,
        last_error: null,
        held_by: [],
        send_after: null,
      },
    ],
    heldForOthers: 1,
    heldMail: [
      {
        id: "held1",
        to: "peer2hash00000000000000000000000002",
        to_name: "bob",
        from: "peer1hash00000000000000000000000001",
        from_name: "alice",
        stored_at: NOW - 500,
        expires_at: NOW + 86400,
        blob_bytes: 128,
      },
    ],
    settings: null,
    dmRequests: [],
    joinRequests: [],
    archivedConversations: [],
    disappearingDefaults: { peers: {}, groups: {} },
    mutedConversations: { peers: [], groups: [] },
    typing: {},
    channelMessage: null,
    allowlist: null,
    bridge: {
      version: "0.1.0",
      web: true,
      url: "",
      notes_editable: true,
    },
    social: null,
    connection: "live",
    pings: {},
    infos: {},
    verifications: {},
    blocks: {},
    announcing: false,
    backingUp: false,
    backupError: null,
    exporting: false,
    importing: false,
    refreshSnapshot: vi.fn(),
    refreshOutbox: vi.fn(),
    refreshGroups: vi.fn(),
    refreshMessages: vi.fn(),
    refreshRequests: vi.fn(),
    refreshSettings: vi.fn(),
    refreshExtras: vi.fn(),
    send: vi.fn().mockResolvedValue(null),
    sendGroup: vi.fn().mockResolvedValue([]),
    sendFile: vi.fn().mockResolvedValue(null),
    sendGroupFile: vi.fn().mockResolvedValue([]),
    sendTyping: vi.fn().mockResolvedValue({ ok: true }),
    react: vi.fn().mockResolvedValue({ ok: true }),
    clearReaction: vi.fn().mockResolvedValue({ ok: true }),
    editMessage: vi.fn().mockResolvedValue({ ok: true, edited: true }),
    retractMessage: vi.fn().mockResolvedValue({ ok: true }),
    deleteMessage: vi.fn().mockResolvedValue({ ok: true }),
    forwardMessage: vi.fn().mockResolvedValue({ ok: true }),
    pinMessage: vi.fn().mockResolvedValue({ ok: true }),
    unpinMessage: vi.fn().mockResolvedValue({ ok: true }),
    markRead: vi.fn().mockResolvedValue({ per_peer: {}, per_group: {}, total: 0 }),
    markUnread: vi.fn().mockResolvedValue({ per_peer: {}, per_group: {}, total: 0 }),
    voteGroupPoll: vi.fn().mockResolvedValue({ ok: true, messages: [] }),
    closeGroupPoll: vi.fn().mockResolvedValue({ ok: true, messages: [] }),
    pingPeer: vi.fn().mockResolvedValue({ ok: true, rtt_ms: 12 }),
    fetchPeerInfo: vi.fn().mockResolvedValue({ ok: true, info: {} }),
    verifyPeer: vi.fn().mockResolvedValue({}),
    confirmVerified: vi.fn().mockResolvedValue({}),
    blockPeer: vi.fn().mockResolvedValue({ ok: true }),
    forgetPeer: vi.fn().mockResolvedValue({ ok: true }),
    setAlias: vi.fn().mockResolvedValue({ ok: true }),
    setNote: vi.fn().mockResolvedValue({ ok: true }),
    announce: vi.fn(),
    renameNode: vi.fn(),
    updateSettings: vi.fn(),
    createGroup: vi.fn().mockResolvedValue({
      ok: true,
      group: {
        id: "group2",
        name: "new crew",
        members: [],
        created_at: NOW,
      },
    }),
    deleteGroup: vi.fn(),
    leaveGroup: vi.fn().mockResolvedValue({ ok: true }),
    rejoinGroup: vi.fn().mockResolvedValue({ ok: true }),
    ignoreGroup: vi.fn().mockResolvedValue({ ok: true }),
    changeGroupMembers: vi.fn(),
    renameGroup: vi.fn(),
    setGroupTopic: vi.fn(),
    retryOutbox: vi.fn().mockResolvedValue({ ok: true }),
    cancelOutbox: vi.fn().mockResolvedValue({ ok: true }),
    dropHeld: vi.fn().mockResolvedValue({ ok: true }),
    acceptDmRequest: vi.fn(),
    declineDmRequest: vi.fn(),
    decideJoinRequest: vi.fn(),
    archiveConversation: vi.fn().mockResolvedValue({ ok: true }),
    setDisappearing: vi.fn().mockResolvedValue({ ok: true }),
    muteConversation: vi.fn().mockResolvedValue({ ok: true }),
    allowlistAdd: vi.fn(),
    allowlistRemove: vi.fn(),
    allowlistSetEnabled: vi.fn(),
    exportBundle: vi.fn(),
    importBundle: vi.fn(),
    downloadBackup: vi.fn(),
    runDoctor: vi.fn(),
    importMessages: vi.fn(),
    ...overrides,
  };
  return base as unknown as UseTauResult;
}

describe("ChatTab", () => {
  it("lists peers and groups with unread badges", () => {
    const tau = makeTau();
    render(<ChatTab tau={tau} />);
    expect(screen.getByText("alice")).toBeTruthy();
    expect(screen.getByText("bob")).toBeTruthy();
    expect(screen.getByText("mesh crew")).toBeTruthy();
    expect(screen.getByText("2")).toBeTruthy();
    expect(screen.getByText("1")).toBeTruthy();
    expect(
      screen.getByText("Pick a peer or group to open a conversation."),
    ).toBeTruthy();
  });

  it("selecting a peer shows the thread, traffic counts and composer in flow", async () => {
    const tau = makeTau();
    render(<ChatTab tau={tau} />);
    fireEvent.click(screen.getByRole("button", { name: /alice/ }));
    await waitFor(() =>
      expect(screen.getByText("hello from the mesh")).toBeTruthy(),
    );
    expect(screen.getByText("hello back")).toBeTruthy();
    // Traffic strip: 1 inbound / 1 outbound in the open DM.
    expect(screen.getByText("1 in / 1 out")).toBeTruthy();
    expect(screen.getByText("streaming")).toBeTruthy();
    // Composer sits in normal flow at the bottom of the thread.
    expect(screen.getByLabelText("Message text")).toBeTruthy();
    expect(screen.getByRole("button", { name: "Send" })).toBeTruthy();
    // Group message must not leak into the DM thread.
    expect(screen.queryByText("group hello")).toBeNull();
    // Back button returns to the list on mobile.
    expect(screen.getByRole("button", { name: /Back/ })).toBeTruthy();
    // Peer panel renders below the thread.
    expect(screen.getByText("Peer")).toBeTruthy();
    expect(screen.getByRole("button", { name: "ping" })).toBeTruthy();
  });

  it("sends a DM through the composer", async () => {
    const tau = makeTau();
    render(<ChatTab tau={tau} />);
    fireEvent.click(screen.getByRole("button", { name: /alice/ }));
    await waitFor(() =>
      expect(screen.getByLabelText("Message text")).toBeTruthy(),
    );
    fireEvent.change(screen.getByLabelText("Message text"), {
      target: { value: "test message" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Send" }));
    await waitFor(() =>
      expect(tau.send).toHaveBeenCalledWith(
        "peer1hash00000000000000000000000001",
        "test message",
        undefined,
        undefined,
        undefined,
      ),
    );
  });

  it("renders the outbox with cancel, retry-all and held mail", () => {
    const tau = makeTau();
    render(<ChatTab tau={tau} />);
    expect(screen.getByText(/Outbox/)).toBeTruthy();
    expect(screen.getByText(/queued hello/)).toBeTruthy();
    expect(screen.getByRole("button", { name: "cancel" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "retry all" })).toBeTruthy();
    expect(screen.getByText(/holding 1 for others/)).toBeTruthy();
    expect(screen.getByText(/Held mail for others/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "cancel" }));
    expect(tau.cancelOutbox).toHaveBeenCalledWith("out1");
  });

  it("selecting a group shows its messages and topic", async () => {
    const tau = makeTau();
    render(<ChatTab tau={tau} />);
    fireEvent.click(screen.getByRole("button", { name: /mesh crew/ }));
    await waitFor(() =>
      expect(screen.getByText("group hello")).toBeTruthy(),
    );
    expect(screen.getByText("all things mesh")).toBeTruthy();
    expect(screen.getByText("1 in / 0 out")).toBeTruthy();
    expect(screen.queryByText("hello from the mesh")).toBeNull();
  });

  it("creates a group from the list", async () => {
    const tau = makeTau();
    render(<ChatTab tau={tau} />);
    fireEvent.click(screen.getByRole("button", { name: /new group/ }));
    fireEvent.change(screen.getByLabelText("Group name"), {
      target: { value: "new crew" },
    });
    fireEvent.click(screen.getByLabelText("alice"));
    fireEvent.click(screen.getByRole("button", { name: "Create group" }));
    await waitFor(() =>
      expect(tau.createGroup).toHaveBeenCalledWith("new crew", [
        "peer1hash00000000000000000000000001",
      ]),
    );
  });

  it("shows an empty thread state for a peer with no messages", async () => {
    const tau = makeTau({
      messages: [],
      unread: { per_peer: {}, per_group: {}, total: 0 },
    });
    render(<ChatTab tau={tau} />);
    fireEvent.click(screen.getByRole("button", { name: /bob/ }));
    await waitFor(() =>
      expect(screen.getByText("No messages yet")).toBeTruthy(),
    );
  });
});
