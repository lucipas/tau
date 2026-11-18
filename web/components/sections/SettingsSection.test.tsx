// SettingsSection.test.tsx — settings toggles, rename, your profile,
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
    if (url.includes("/api/settings")) return settings;
    if (url.includes("/api/messages/disappearing")) return { peers: {}, groups: {} };
    if (url.includes("/api/profile")) {
      if (method === "POST") {
        return {
          ok: true,
          profile: {
            author: "a".repeat(32),
            name: "Tester Two",
            bio: "mesh enjoyer",
            avatar: null,
            links: ["https://example.org"],
            version: 1,
            created_at: 1000,
            published: true,
            changed: true,
          },
        };
      }
      return {
        author: null,
        name: "testnode",
        bio: "",
        avatar: null,
        links: [],
        version: 0,
        created_at: null,
        published: false,
      };
    }
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

  it("shows the unpublished profile and saves it", async () => {
    renderSettings();
    expect(await screen.findByText(/Not published yet/)).toBeTruthy();
    const name = screen.getByLabelText("Profile display name") as HTMLInputElement;
    expect(name.value).toBe("testnode");
    fireEvent.change(name, { target: { value: "Tester Two" } });
    fireEvent.change(screen.getByLabelText("Profile bio"), {
      target: { value: "mesh enjoyer" },
    });
    fireEvent.change(screen.getByLabelText("Profile links"), {
      target: { value: "https://example.org\n" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save profile" }));
    await waitFor(() => expect(calls).toContain("POST /api/profile"));
    expect(await screen.findByText(/Profile saved/)).toBeTruthy();
    expect(await screen.findByText(/Published as version 1/)).toBeTruthy();
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
