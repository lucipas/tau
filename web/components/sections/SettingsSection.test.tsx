// SettingsSection.test.tsx — settings toggles, rename, quiet hours,
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
    if (url.includes("/api/settings/quiet-hours"))
      return method === "POST"
        ? {
            ok: true,
            quiet_hours: {
              enabled: true,
              start: "23:00",
              end: "07:00",
              active: false,
              minutes_until_end: null,
            },
          }
        : {
            enabled: false,
            start: "22:00",
            end: "07:00",
            active: false,
            minutes_until_end: null,
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

  it("shows the quiet-hours window and saves a new one", async () => {
    renderSettings();
    expect(await screen.findByText(/Off — badges light/)).toBeTruthy();
    const start = screen.getByLabelText("Quiet hours start") as HTMLInputElement;
    const end = screen.getByLabelText("Quiet hours end") as HTMLInputElement;
    expect(start.value).toBe("22:00");
    expect(end.value).toBe("07:00");
    fireEvent.click(screen.getByLabelText("Enable quiet hours"));
    fireEvent.change(start, { target: { value: "23:00" } });
    fireEvent.click(screen.getByRole("button", { name: "Save quiet hours" }));
    await waitFor(() => expect(calls).toContain("POST /api/settings/quiet-hours"));
    expect(await screen.findByText(/Quiet hours on 23:00–07:00/)).toBeTruthy();
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
