"use client";

import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { UseTauResult } from "@/lib/use-tau";
import { SettingsTab } from "./settings-tab";

const rulesPayload = {
  rules: {
    default: "allow",
    allow_authors: [],
    block_authors: ["blockedauthorhash1111111111"],
    allow_followed: true,
    kinds: null,
  },
  available_kinds: ["post", "like"],
};

function fakeTau(
  overrides: Partial<UseTauResult> = {},
): UseTauResult {
  return {
    node: { name: "mynode" },
    peers: [
      {
        destination: "peerhash1111111111",
        name: "Alice",
        last_seen: 0,
        age: 0,
        hops: 0,
        protocol: 0,
      },
    ],
    groups: [
      { id: "groupid1111111111", name: "Crew", members: [], created_at: 0 },
    ],
    settings: { accept_files: false, send_read_receipts: true },
    disappearingDefaults: {
      peers: { peerhash1111111111: 3600 },
      groups: {},
    },
    mutedConversations: {
      peers: ["peerhash1111111111"],
      groups: [],
    },
    archivedConversations: [
      {
        destination: "peerhash1111111111",
        name: "Alice",
        messages: 3,
      },
    ],
    updateSettings: vi.fn().mockResolvedValue(undefined),
    renameNode: vi.fn().mockResolvedValue(undefined),
    setDisappearing: vi.fn().mockResolvedValue(undefined),
    muteConversation: vi.fn().mockResolvedValue(undefined),
    archiveConversation: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  } as unknown as UseTauResult;
}

beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => ({
      ok: true,
      status: 200,
      text: async () => JSON.stringify(rulesPayload),
    })) as unknown as typeof fetch,
  );
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("SettingsTab", () => {
  it("renders the node settings toggles", () => {
    render(<SettingsTab tau={fakeTau()} />);
    expect(
      screen.getByRole("switch", { name: "Accept file transfers" }),
    ).toBeTruthy();
    expect(
      screen.getByRole("switch", { name: "Send read receipts" }),
    ).toBeTruthy();
  });

  it("toggles a setting off through updateSettings", () => {
    const tau = fakeTau();
    render(<SettingsTab tau={tau} />);
    fireEvent.click(
      screen.getByRole("switch", { name: "Send read receipts" }),
    );
    expect(tau.updateSettings).toHaveBeenCalledWith({
      send_read_receipts: false,
    });
  });

  it("prefills the node name for renaming", () => {
    render(<SettingsTab tau={fakeTau()} />);
    const input = screen.getByLabelText("Node name") as HTMLInputElement;
    expect(input.value).toBe("mynode");
    expect(screen.getByRole("button", { name: "rename" })).toBeTruthy();
  });

  it("lists disappearing defaults, muted and archived conversations", () => {
    render(<SettingsTab tau={fakeTau()} />);
    expect(screen.getByText("Disappearing messages")).toBeTruthy();
    expect(screen.getByText(/Alice · peerhash…1111 · 1h/)).toBeTruthy();
    expect(screen.getByRole("button", { name: "turn off" })).toBeTruthy();
    expect(screen.getByText("Muted conversations")).toBeTruthy();
    expect(screen.getByRole("button", { name: "unmute" })).toBeTruthy();
    expect(screen.getByText("Archived conversations")).toBeTruthy();
    expect(screen.getByText(/Alice · 3 messages/)).toBeTruthy();
    expect(screen.getByRole("button", { name: "unarchive" })).toBeTruthy();
  });

  it("loads and renders the social rules", async () => {
    render(<SettingsTab tau={fakeTau()} />);
    expect(await screen.findByText("Social rules")).toBeTruthy();
    expect(
      await screen.findByRole("switch", { name: "Admit authors you follow" }),
    ).toBeTruthy();
    expect(screen.getByRole("button", { name: "allow all" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "deny all" })).toBeTruthy();
    expect(screen.getByLabelText("Block list author")).toBeTruthy();
  });
});
