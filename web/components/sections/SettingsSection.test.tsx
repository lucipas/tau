// SettingsSection.test.tsx — settings toggles, rename, disappearing
// defaults. Runs inside a TauContext.Provider with a fake node.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { TauContext } from "@/components/chrome/TauContext";
import SettingsSection from "./SettingsSection";
import { fakeTau, mockFetch } from "./test-utils";

let calls: string[] = [];
let settings = { accept_files: true, send_read_receipts: false };

beforeEach(() => {
  settings = { accept_files: true, send_read_receipts: false };
  const router = mockFetch((url) => {
    if (url.includes("/api/settings")) return settings;
    if (url.includes("/api/messages/disappearing"))
      return { peers: {}, groups: {}, channels: {} };
    if (url.includes("/api/channels"))
      return { channels: [{ channel: "general", joined_at: 1 }], count: 1 };
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

  it("saves a disappearing default for a peer", async () => {
    renderSettings();
    await screen.findByDisplayValue("testnode");
    fireEvent.change(screen.getByLabelText("Peer, group or channel"), {
      target: { value: `peer:${"b".repeat(32)}` },
    });
    fireEvent.change(screen.getByLabelText("Disappearing time"), {
      target: { value: "3600" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save default" }));
    await waitFor(() => expect(calls).toContain("POST /api/messages/disappearing"));
  });

  it("saves a disappearing default for a channel", async () => {
    renderSettings();
    await screen.findByDisplayValue("testnode");
    fireEvent.change(screen.getByLabelText("Peer, group or channel"), {
      target: { value: "channel:general" },
    });
    fireEvent.change(screen.getByLabelText("Disappearing time"), {
      target: { value: "300" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save default" }));
    await waitFor(() => expect(calls).toContain("POST /api/messages/disappearing"));
    expect(await screen.findByText(/Disappearing default saved/)).toBeTruthy();
  });

  it("lists a channel default under its channel name", async () => {
    const router = mockFetch((url) => {
      if (url.includes("/api/settings")) return settings;
      if (url.includes("/api/messages/disappearing"))
        return { peers: {}, groups: {}, channels: { general: 300 } };
      if (url.includes("/api/channels"))
        return { channels: [{ channel: "general", joined_at: 1 }], count: 1 };
      return { ok: true };
    });
    calls = router.calls;
    renderSettings();
    // The label appears twice: once as a picker option, once as the
    // saved-default row; the row's "5 minutes" likewise.
    const labels = await screen.findAllByText("Channel: #general");
    expect(labels).toHaveLength(2);
    const times = screen.getAllByText("5 minutes");
    expect(times).toHaveLength(2);
  });
});
