// ChannelsSection.test.tsx — channel list, join, message send, sync.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import ChannelsSection from "./ChannelsSection";
import { mockFetch } from "./test-utils";

let calls: string[] = [];

beforeEach(() => {
  const router = mockFetch((url) => {
    if (url.includes("/api/channels/messages")) {
      return {
        channel: "mesh-general",
        messages: [
          {
            v: 1,
            hash: "m1".padEnd(64, "0"),
            channel: "mesh-general",
            author: "a".repeat(32),
            key: "a".repeat(32),
            name: "alice",
            text: "hello channel",
            created_at: 1000,
            received_at: 1000,
            sig: "s",
          },
        ],
        count: 1,
      };
    }
    if (url.includes("/api/channels")) {
      return {
        channels: [{ channel: "mesh-general", joined_at: 900 }],
        count: 1,
      };
    }
    return { ok: true };
  });
  calls = router.calls;
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("ChannelsSection", () => {
  it("lists joined channels and their messages", async () => {
    render(<ChannelsSection />);
    expect(await screen.findByRole("button", { name: "#mesh-general" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "#mesh-general" }));
    expect(await screen.findByText("hello channel")).toBeTruthy();
  });

  it("joins a new channel by name", async () => {
    render(<ChannelsSection />);
    await screen.findByRole("button", { name: "#mesh-general" });
    fireEvent.change(screen.getByLabelText("Channel name to join"), {
      target: { value: "mesh-radio" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Join" }));
    await waitFor(() => expect(calls).toContain("POST /api/channels/join"));
  });

  it("sends a message to the selected channel", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    await screen.findByText("hello channel");
    fireEvent.change(screen.getByLabelText("Message #mesh-general"), {
      target: { value: "hi all" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Send" }));
    await waitFor(() => expect(calls).toContain("POST /api/channels/send"));
  });

  it("sync pulls history for the selected channel", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    fireEvent.click(screen.getByRole("button", { name: /Sync/ }));
    await waitFor(() => expect(calls).toContain("POST /api/channels/sync"));
  });
});
