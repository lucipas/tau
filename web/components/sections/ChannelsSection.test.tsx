// ChannelsSection.test.tsx — channel list, join, message send, sync.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import ChannelsSection from "./ChannelsSection";
import { mockFetch } from "./test-utils";

let calls: string[] = [];

beforeEach(() => {
  const router = mockFetch((url) => {
    if (url.includes("/api/status")) {
      // This node is alice: the fixture message below is hers, so
      // the Edit button shows for it and for nothing else.
      return { node: { destination: "a".repeat(32) } };
    }
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
          {
            v: 1,
            hash: "m2".padEnd(64, "0"),
            channel: "mesh-general",
            author: "b".repeat(32),
            key: "b".repeat(32),
            name: "bob",
            text: "hello, fixed",
            created_at: 1001,
            received_at: 1001,
            sig: "s",
            edited: true,
            edited_at: 1002,
          },
        ],
        count: 2,
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

  it("marks edited messages", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    expect(await screen.findByText("hello, fixed")).toBeTruthy();
    expect(screen.getByText(/edited/)).toBeTruthy();
    // Only alice's own message gets an Edit button; bob's does not.
    expect(screen.getAllByRole("button", { name: "Edit" })).toHaveLength(1);
  });

  it("edits your own message from the console", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    fireEvent.click(await screen.findByRole("button", { name: "Edit" }));
    fireEvent.change(screen.getByLabelText("Edit message"), {
      target: { value: "hello channel, fixed" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() =>
      expect(calls).toContain("POST /api/channels/edit"),
    );
  });
});
