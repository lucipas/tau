// SettingsSection.test.tsx — settings toggles, rename, away message,
// disappearing defaults. Runs inside a TauContext.Provider with a fake node.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { TauContext } from "@/components/chrome/TauContext";
import SettingsSection from "./SettingsSection";
import { fakeTau, mockFetch } from "./test-utils";

let calls: string[] = [];
let settings = { accept_files: true, send_read_receipts: false };

beforeEach(() => {
  settings = { accept_files: true, send_read_receipts: false };
  const router = mockFetch((url, method) => {
    if (url.includes("/api/settings/away-message"))
      return method === "POST"
        ? {
            ok: true,
            away_message: {
              enabled: true,
              message: "back on Monday",
              since: 1234567890,
              replied_to: [],
              replied_count: 0,
            },
          }
        : {
            enabled: false,
            message: "I'm away right now. I'll reply when I'm back.",
            since: null,
            replied_to: [],
            replied_count: 0,
          };
    if (url.includes("/api/settings")) return settings;
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

  it("shows the away message and saves a new one", async () => {
    renderSettings();
    expect(await screen.findByText(/Off — DMs arrive as usual/)).toBeTruthy();
    const message = screen.getByLabelText("Away reply text") as HTMLInputElement;
    expect(message.value).toBe("I'm away right now. I'll reply when I'm back.");
    fireEvent.click(screen.getByLabelText("Enable away reply"));
    fireEvent.change(message, { target: { value: "back on Monday" } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(calls).toContain("POST /api/settings/away-message"));
    expect(await screen.findByText(/On — replying: “back on Monday”/)).toBeTruthy();
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
