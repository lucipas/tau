"use client";

import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { UseTauResult } from "@/lib/use-tau";
import { ToolsTab } from "./tools-tab";

function fakeTau(
  overrides: Partial<UseTauResult> = {},
): UseTauResult {
  return {
    peers: [],
    backingUp: false,
    backupError: null,
    exporting: false,
    importing: false,
    allowlist: {
      enabled: false,
      entries: [{ destination: "peerhash1111111111", name: "Alice" }],
    },
    dmRequests: [],
    joinRequests: [],
    heldMail: [],
    runDoctor: vi.fn().mockResolvedValue({
      ok: true,
      checks: [
        { name: "data dir", status: "ok", detail: "all good" },
        { name: "mailbox", status: "warn", detail: "1 stale entry" },
      ],
    }),
    downloadBackup: vi.fn().mockResolvedValue(undefined),
    exportBundle: vi.fn().mockResolvedValue({
      ok: true,
      bundle: "{}",
      summary: { mail: 1, tan: 0, skipped: [], bytes: 42 },
    }),
    importBundle: vi.fn().mockResolvedValue({
      ok: true,
      import: { imported: 1, present: 0, skipped: 0, tan: null, errors: [] },
    }),
    importMessages: vi.fn().mockResolvedValue({
      ok: true,
      import: { imported: 2, present: 0, skipped: 0, errors: [] },
    }),
    allowlistAdd: vi.fn().mockResolvedValue(undefined),
    allowlistRemove: vi.fn().mockResolvedValue(undefined),
    allowlistSetEnabled: vi.fn().mockResolvedValue(undefined),
    acceptDmRequest: vi.fn().mockResolvedValue(undefined),
    declineDmRequest: vi.fn().mockResolvedValue(undefined),
    decideJoinRequest: vi.fn().mockResolvedValue(undefined),
    dropHeld: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  } as unknown as UseTauResult;
}

describe("ToolsTab", () => {
  it("renders every tools section", () => {
    render(<ToolsTab tau={fakeTau()} />);
    expect(screen.getByText("Node doctor")).toBeTruthy();
    expect(screen.getByText("Backup")).toBeTruthy();
    expect(screen.getByText("Bundle export / import")).toBeTruthy();
    expect(screen.getByText("Messages export / import")).toBeTruthy();
    expect(screen.getByText("DM allowlist")).toBeTruthy();
    expect(screen.getByText("DM requests")).toBeTruthy();
    expect(screen.getByText("Join requests")).toBeTruthy();
    expect(screen.getByText("Held mail")).toBeTruthy();
  });

  it("runs the doctor and renders the checks", async () => {
    render(<ToolsTab tau={fakeTau()} />);
    fireEvent.click(screen.getByRole("button", { name: "run health check" }));
    expect(await screen.findByText("data dir")).toBeTruthy();
    expect(screen.getByText("all good")).toBeTruthy();
    expect(screen.getByText("mailbox")).toBeTruthy();
    expect(screen.getByText("healthy")).toBeTruthy();
  });

  it("renders the allowlist toggle and entries", () => {
    render(<ToolsTab tau={fakeTau()} />);
    expect(
      screen.getByRole("switch", { name: "Enforce allowlist" }),
    ).toBeTruthy();
    expect(screen.getByText(/Alice/)).toBeTruthy();
    expect(screen.getByRole("button", { name: "allow" })).toBeTruthy();
  });

  it("renders the bundle export controls including the Tan checkbox", () => {
    render(<ToolsTab tau={fakeTau()} />);
    expect(
      screen.getByText("Include recent Tan posts in the export"),
    ).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "download bundle" }),
    ).toBeTruthy();
    expect(
      screen.getByPlaceholderText("paste bundle JSON here"),
    ).toBeTruthy();
  });

  it("renders the messages export button and empty request queues", () => {
    render(<ToolsTab tau={fakeTau()} />);
    expect(
      screen.getByRole("button", { name: "export messages" }),
    ).toBeTruthy();
    expect(screen.getByText("No DM requests held.")).toBeTruthy();
    expect(screen.getByText("No join requests pending.")).toBeTruthy();
    expect(screen.getByText("No mail held for other peers.")).toBeTruthy();
  });
});
