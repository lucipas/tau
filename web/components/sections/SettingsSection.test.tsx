// SettingsSection.test.tsx — settings toggles, sync depth, rename,
// your profile, away message, quiet hours, disappearing defaults.
// Runs inside a TauContext.Provider with a fake node.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { TauContext } from "@/components/chrome/TauContext";
import SettingsSection from "./SettingsSection";
import { fakeTau, mockFetch } from "./test-utils";

let calls: string[] = [];
let settings = {
  accept_files: true,
  send_read_receipts: false,
  tan_sync_depth: 2,
  announce_interval: 300,
};

beforeEach(() => {
  settings = {
    accept_files: true,
    send_read_receipts: false,
    tan_sync_depth: 2,
    announce_interval: 300,
  };
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
    if (url.includes("/api/settings")) {
      if (method === "POST") {
        return {
          ok: true,
          settings: { ...settings, tan_sync_depth: 1, announce_interval: 60 },
        };
      }
      return settings;
    }
    if (url.includes("/api/messages/disappearing"))
      return { peers: {}, groups: {}, channels: {} };
    if (url.includes("/api/channels"))
      return { channels: [{ channel: "general", joined_at: 1 }], count: 1 };
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

  it("shows the Tan sync depth and changing it posts to /api/settings", async () => {
    renderSettings();
    const depth = (await screen.findByLabelText(
      "Tan sync depth",
    )) as HTMLSelectElement;
    expect(depth.value).toBe("2");
    fireEvent.change(depth, { target: { value: "1" } });
    await waitFor(() => expect(calls).toContain("POST /api/settings"));
    await waitFor(() => expect(depth.value).toBe("1"));
    const notice = await screen.findByText(
      /Tan sync now pulls followed authors only/,
    );
    expect(notice.textContent).toMatch(/followed authors only/);
  });

  it("shows the announce interval and saving it posts to /api/settings", async () => {
    renderSettings();
    const interval = (await screen.findByLabelText(
      "Announce interval in seconds",
    )) as HTMLInputElement;
    expect(interval.value).toBe("300");
    fireEvent.change(interval, { target: { value: "60" } });
    fireEvent.click(screen.getByRole("button", { name: "Save interval" }));
    await waitFor(() => expect(calls).toContain("POST /api/settings"));
    await waitFor(() => expect(interval.value).toBe("60"));
    expect(await screen.findByText(/Announcing every 60 seconds/)).toBeTruthy();
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
      if (url.includes("/api/profile"))
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
