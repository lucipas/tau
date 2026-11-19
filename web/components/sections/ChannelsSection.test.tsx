// ChannelsSection.test.tsx — channel list, join, message send, sync.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import ChannelsSection from "./ChannelsSection";
import { mockFetch } from "./test-utils";

let calls: string[] = [];

beforeEach(() => {
  const message = {
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
  };
  const router = mockFetch((url) => {
    if (url.includes("/api/channels/scheduled")) {
      return {
        scheduled: [
          {
            id: "ab12cd34ef56ab78",
            channel: "mesh-general",
            text: "morning hello",
            publish_at: Math.floor(Date.now() / 1000) + 300,
            created_at: 1000,
            to: [],
            ttl: 3,
            message_hash: null,
            attempts: 0,
            next_attempt_at: 0,
            paused_at: 0,
          },
        ],
        count: 1,
      };
    }
    if (url.includes("/api/channels/topic")) {
      return {
        channel: "mesh-general",
        topic: {
          v: 1,
          hash: "t1".padEnd(64, "0"),
          channel: "mesh-general",
          author: "a".repeat(32),
          key: "a".repeat(32),
          topic: "monthly mesh meetup",
          updated_at: 1000,
          sig: "s",
        },
      };
    }
    if (url.includes("/api/channels/search")) {
      return {
        channel: "mesh-general",
        query: "hello",
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
    if (url.includes("/api/channels/pins")) {
      return {
        channel: "mesh-general",
        pins: [{ ...message, pinned_at: 1500 }],
        count: 1,
      };
    }
    if (url.includes("/api/status")) {
      // This node is alice: the fixture message below is hers, so
      // the Edit button shows for it and for nothing else.
      return { node: { destination: "a".repeat(32) } };
    }
    if (url.includes("/api/channels/messages")) {
      return {
        channel: "mesh-general",
        // Newest first, as the API serves them: the reply, then its parent.
        messages: [
          {
            v: 1,
            hash: "m2".padEnd(64, "0"),
            channel: "mesh-general",
            author: "b".repeat(32),
            key: "b".repeat(32),
            name: "bob",
            text: "count me in",
            created_at: 1001,
            received_at: 1001,
            sig: "s",
            reply_to: "m1".padEnd(64, "0"),
          },
          {
            v: 1,
            hash: "p1".padEnd(64, "0"),
            channel: "mesh-general",
            author: "b".repeat(32),
            key: "b".repeat(32),
            name: "bob",
            text: "which day?",
            created_at: 1001,
            received_at: 1001,
            sig: "s",
            poll_options: ["Saturday", "Sunday"],
            poll: {
              options: ["Saturday", "Sunday"],
              counts: [1, 1],
              total: 2,
              closed: false,
              voters: [],
              my_vote: null,
              own: true,
            },
          },
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
    // The message shows twice: once in the pinned strip, once in history.
    expect((await screen.findAllByText("hello channel")).length).toBe(2);
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
    await screen.findAllByText("hello channel");
    fireEvent.change(screen.getByLabelText("Message #mesh-general"), {
      target: { value: "hi all" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Send" }));
    await waitFor(() => expect(calls).toContain("POST /api/channels/send"));
  });

  it("shows a reply with its quoted parent", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    expect(await screen.findByText("count me in")).toBeTruthy();
    expect(await screen.findByText("↩ alice: hello channel")).toBeTruthy();
  });

  it("reply button arms the composer for that message", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    await screen.findAllByText("hello channel");
    fireEvent.click(screen.getByRole("button", { name: "Reply to alice" }));
    expect(await screen.findByText(/Replying to/)).toBeTruthy();
    expect(screen.getByText("alice", { selector: "span.font-semibold" })).toBeTruthy();
    fireEvent.change(screen.getByLabelText("Message #mesh-general"), {
      target: { value: "count me in" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Send" }));
    await waitFor(() => expect(calls).toContain("POST /api/channels/send"));
    // The banner clears once the reply is sent.
    await waitFor(() =>
      expect(screen.queryByText(/Replying to/)).toBeNull(),
    );
  });

  it("sync pulls history for the selected channel", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    fireEvent.click(screen.getByRole("button", { name: /Sync/ }));
    await waitFor(() => expect(calls).toContain("POST /api/channels/sync"));
  });

  it("shows scheduled messages for the selected channel with cancel", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    expect(await screen.findByText("morning hello")).toBeTruthy();
    expect(screen.getByRole("button", { name: "Cancel" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    await waitFor(() =>
      expect(calls).toContain("POST /api/channels/cancel-scheduled"),
    );
  });

  it("schedules a message when Send later is picked", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    // The message shows twice: once in the pinned strip, once in history.
    await screen.findAllByText("hello channel");
    fireEvent.change(screen.getByLabelText("Message #mesh-general"), {
      target: { value: "later hello" },
    });
    fireEvent.change(screen.getByLabelText("Send later"), {
      target: { value: "300" },
    });
    expect(screen.getByRole("button", { name: "Schedule" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Schedule" }));
    await waitFor(() =>
      expect(calls).toContain("POST /api/channels/schedule"),
    );
  });

  it("renders a channel poll with options, tally and close", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    expect(await screen.findByText("which day?")).toBeTruthy();
    expect(screen.getByRole("button", { name: /Saturday/ })).toBeTruthy();
    expect(screen.getByText(/2 votes/)).toBeTruthy();
    expect(screen.getByRole("button", { name: "Close poll" })).toBeTruthy();
  });

  it("votes on a channel poll option", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    fireEvent.click(await screen.findByRole("button", { name: /Sunday/ }));
    await waitFor(() => expect(calls).toContain("POST /api/channels/vote"));
  });

  it("posts a channel poll from the composer", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    fireEvent.click(screen.getByRole("button", { name: "Poll" }));
    fireEvent.change(screen.getByLabelText("Poll question"), {
      target: { value: "which day?" },
    });
    fireEvent.change(screen.getByLabelText("Poll option 1"), {
      target: { value: "Saturday" },
    });
    fireEvent.change(screen.getByLabelText("Poll option 2"), {
      target: { value: "Sunday" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Post poll" }));
    await waitFor(() => expect(calls).toContain("POST /api/channels/poll"));
  });

  it("shows the channel topic and saves an edited one", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    expect(await screen.findByText("monthly mesh meetup")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Edit topic" }));
    fireEvent.change(screen.getByLabelText("Channel topic"), {
      target: { value: "weekly mesh meetup" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(calls).toContain("POST /api/channels/topic"));
  });

  it("clears the channel topic", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    await screen.findByText("monthly mesh meetup");
    fireEvent.click(screen.getByRole("button", { name: "Clear topic" }));
    await waitFor(() => expect(calls).toContain("POST /api/channels/topic"));
  });

  it("shows pins in a strip and unpins from it", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    // The pinned message shows in the strip and in history.
    await screen.findByRole("button", { name: "Unpin pinned message from alice" });
    expect(screen.getAllByText("hello channel").length).toBe(2);
    fireEvent.click(
      screen.getByRole("button", { name: "Unpin pinned message from alice" }),
    );
    await waitFor(() => expect(calls).toContain("POST /api/channels/unpin"));
  });

  it("marks a pinned message Pinned in history", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    const button = await screen.findByRole("button", {
      name: "Unpin message from alice",
    });
    expect(button.textContent).toBe("Pinned");
  });

  it("searches the selected channel and clears back to history", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    await screen.findAllByText("hello channel");
    fireEvent.change(screen.getByLabelText("Search #mesh-general"), {
      target: { value: "hello" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    await waitFor(() =>
      expect(calls).toContain(
        "GET /api/channels/search?channel=mesh-general&q=hello&limit=20",
      ),
    );
    expect(await screen.findByText(/1 match for/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Clear" }));
    expect((await screen.findAllByText("hello channel")).length).toBe(2);
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

  it("reacts to a message from the quick-react row", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    await screen.findAllByText("hello channel");
    // The fixture lists three messages, so every one renders a
    // quick-react row: react to the first message's row.
    fireEvent.click(
      screen.getAllByRole("button", { name: "React 👍 to this message" })[0],
    );
    await waitFor(() => expect(calls).toContain("POST /api/channels/react"));
  });

  it("shows reaction counts and their reactors", async () => {
    mockFetch((url) => {
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
              reactions: [
                { reaction: "👍", count: 2, reactors: ["alice", "bob"] },
                { reaction: "🎉", count: 1, reactors: ["carol"] },
              ],
            },
          ],
          count: 1,
        };
      }
      if (url.includes("/api/channels/pins")) {
        return { channel: "mesh-general", pins: [], count: 0 };
      }
      if (url.includes("/api/channels/scheduled")) {
        return { channel: "mesh-general", scheduled: [], count: 0 };
      }
      if (url.includes("/api/channels/topic")) {
        return { channel: "mesh-general", topic: null };
      }
      if (url.includes("/api/channels")) {
        return {
          channels: [{ channel: "mesh-general", joined_at: 900 }],
          count: 1,
        };
      }
      return { ok: true };
    });
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "#mesh-general" }));
    expect(await screen.findByRole("button", { name: "React 👍 (2)" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "React 🎉 (1)" })).toBeTruthy();
  });
});
