// ToolsSection.test.tsx — doctor report renders, allowlist add/remove
// hit the API, bundle export posts.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import ToolsSection from "./ToolsSection";
import { mockFetch } from "./test-utils";

let calls: string[] = [];
let allowlistEntries: { destination: string; name: string }[] = [];
let deviceOffers: { token: string; created_at: number; expires_at: number; owner: string | null }[] = [];

beforeEach(() => {
  allowlistEntries = [{ destination: "c".repeat(32), name: "carol" }];
  deviceOffers = [
    { token: "ab".repeat(16), created_at: 1700000000, expires_at: 1794000000, owner: null },
  ];
  const router = mockFetch((url, method) => {
    if (url.includes("/api/doctor")) {
      return {
        ok: true,
        checks: [
          { name: "Node running", ok: true, message: "up" },
          { name: "Data directory", ok: false, status: "warn", message: "low disk" },
        ],
      };
    }
    if (url.includes("/api/allowlist/add")) {
      allowlistEntries.push({ destination: "d".repeat(32), name: "" });
      return { enabled: true, entries: allowlistEntries };
    }
    if (url.includes("/api/allowlist/remove")) {
      allowlistEntries = allowlistEntries.filter((e) => e.destination !== "c".repeat(32));
      return { enabled: true, entries: allowlistEntries };
    }
    if (url.includes("/api/allowlist")) {
      return { enabled: true, entries: allowlistEntries };
    }
    if (url.includes("/api/bundle/export")) return { bundle: true };
    if (url.includes("/api/devices/pair")) {
      return {
        ok: true,
        token: "ab".repeat(16),
        expires_at: 1794000000,
        history_messages: 0,
        download: `/api/devices/bundle/${"ab".repeat(16)}`,
        filename: "tau-pairing-abababab-20261124-035800.json",
      };
    }
    if (url.includes("/api/devices/revoke")) {
      deviceOffers = [];
      return {
        ok: true,
        revoked: { token: "ab".repeat(16), created_at: 1700000000, expires_at: 1794000000, owner: null },
      };
    }
    if (url.includes("/api/devices")) {
      return {
        identity: "cd".repeat(32),
        destination: "ef".repeat(32),
        paired_from: null,
        issued_offers: deviceOffers,
      };
    }
    return { ok: true };
  });
  calls = router.calls;
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("ToolsSection", () => {
  it("renders the doctor report with pass/fail checks", async () => {
    render(<ToolsSection />);
    expect(await screen.findByText("✓ All checks passed")).toBeTruthy();
    expect(screen.getByText("Node running")).toBeTruthy();
    expect(screen.getByText("Data directory")).toBeTruthy();
    expect(screen.getByText("low disk")).toBeTruthy();
  });

  it("lists allowlist entries and removes one", async () => {
    render(<ToolsSection />);
    expect(await screen.findByText("carol")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Remove" }));
    await waitFor(() => expect(calls).toContain("POST /api/allowlist/remove"));
    await waitFor(() => expect(screen.queryByText("carol")).toBeNull());
  });

  it("adds a destination to the allowlist", async () => {
    render(<ToolsSection />);
    await screen.findByText("carol");
    fireEvent.change(screen.getByLabelText("Destination hash to allow"), {
      target: { value: "d".repeat(32) },
    });
    fireEvent.click(screen.getByRole("button", { name: "Add" }));
    await waitFor(() => expect(calls).toContain("POST /api/allowlist/add"));
    expect(await screen.findByText("Destination added to the allowlist.")).toBeTruthy();
  });

  it("exports a bundle", async () => {
    render(<ToolsSection />);
    await screen.findByText("carol");
    fireEvent.click(screen.getByRole("button", { name: "Export bundle" }));
    await waitFor(() => expect(calls).toContain("POST /api/bundle/export"));
  });

  it("offers the backup download link", async () => {
    render(<ToolsSection />);
    const link = await screen.findByRole("link", { name: /Download backup/ });
    expect(link.getAttribute("href")).toContain("/api/backup");
  });

  it("lists this device and its outstanding pairing offers", async () => {
    render(<ToolsSection />);
    expect(await screen.findByText("token abababab…")).toBeTruthy();
    expect(screen.getByText(`This device: ${"cd".repeat(32)}`)).toBeTruthy();
    expect(screen.getByRole("button", { name: "Revoke" })).toBeTruthy();
  });

  it("pairs a new device and revokes an offer", async () => {
    render(<ToolsSection />);
    await screen.findByText("token abababab…");
    fireEvent.click(screen.getByRole("button", { name: "Pair a new device" }));
    await waitFor(() => expect(calls).toContain("POST /api/devices/pair"));
    expect(
      await screen.findByText(/Pairing bundle downloaded/),
    ).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Revoke" }));
    await waitFor(() => expect(calls).toContain("POST /api/devices/revoke"));
    await waitFor(() =>
      expect(screen.queryByText("token abababab…")).toBeNull(),
    );
  });
});
