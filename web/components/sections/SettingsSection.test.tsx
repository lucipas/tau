// SettingsSection.test.tsx — settings toggles, sync depth, rename,
// disappearing defaults. Runs inside a TauContext.Provider with a fake node.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { TauContext } from "@/components/chrome/TauContext";
import SettingsSection from "./SettingsSection";
import { fakeTau, mockFetch } from "./test-utils";

let calls: string[] = [];
let settings = { accept_files: true, send_read_receipts: false, tan_sync_depth: 2 };

beforeEach(() => {
  settings = { accept_files: true, send_read_receipts: false, tan_sync_depth: 2 };
  const router = mockFetch((url, method) => {
    if (url.includes("/api/settings")) {
      if (method === "POST") {
        return { ok: true, settings: { ...settings, tan_sync_depth: 1 } };
      }
      return settings;
    }
    if (url.includes("/api/messages/disappearing")) return { peers: {}, groups: {} };
    return { ok: true };
  });
  calls = router.calls;
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function renderSettings() {
  return render(
    <TauContext.Provider value={fakeTau()}>
      <SettingsSection />
    </TauContext.Provider>,
  );
}

describe("SettingsSection", () => {
  it("renders the node name and settings checkboxes", async () => {
    renderSettings();
    expect(await screen.findByDisplayValue("testnode")).toBeTruthy();
    const accept = (await screen.findByLabelText(
      /Accept incoming files/,
    )) as HTMLInputElement;
    expect(accept.checked).toBe(true);
    const receipts = (await screen.findByLabelText(
      /Send read receipts/,
    )) as HTMLInputElement;
    expect(receipts.checked).toBe(false);
  });

  it("toggling a setting posts to /api/settings", async () => {
    renderSettings();
    const accept = (await screen.findByLabelText(
      /Accept incoming files/,
    )) as HTMLInputElement;
    fireEvent.click(accept);
    await waitFor(() => expect(calls).toContain("POST /api/settings"));
  });

  it("shows the Tan sync depth and changing it posts to /api/settings", async () => {
    renderSettings();
    const depth = (await screen.findByLabelText(
      "Tan sync depth",
    )) as HTMLSelectElement;
    expect(depth.value).toBe("2");
    fireEvent.change(depth, { target: { value: "1" } });
    await waitFor(() => expect(calls).toContain("POST /api/settings"));
    await waitFor(() => expect(depth.value).toBe("1"));
    const notice = await screen.findByRole("status");
    expect(notice.textContent).toMatch(/followed authors only/);
  });

  it("renaming the node posts to /api/id/rename", async () => {
    renderSettings();
    await screen.findByDisplayValue("testnode");
    fireEvent.change(screen.getByLabelText("Node name"), {
      target: { value: "newname" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Rename" }));
    await waitFor(() => expect(calls).toContain("POST /api/id/rename"));
    expect(await screen.findByText(/Node renamed to/)).toBeTruthy();
  });

  it("saves a disappearing default for a peer", async () => {
    renderSettings();
    await screen.findByDisplayValue("testnode");
    fireEvent.change(screen.getByLabelText("Peer or group"), {
      target: { value: `peer:${"b".repeat(32)}` },
    });
    fireEvent.change(screen.getByLabelText("Disappearing time"), {
      target: { value: "3600" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save default" }));
    await waitFor(() => expect(calls).toContain("POST /api/messages/disappearing"));
  });
});
