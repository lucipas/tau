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
          {
            id: "cd34ef56ab78ab12",
            channel: "mesh-general",
            text: "pizza or tacos?",
            publish_at: Math.floor(Date.now() / 1000) + 3600,
            created_at: 1000,
            to: [],
            ttl: 3,
            message_hash: null,
            attempts: 0,
            next_attempt_at: 0,
            paused_at: 0,
            kind: "poll",
            options: ["Pizza", "Tacos"],
          },
        ],
        count: 2,
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
        channels: [
          { channel: "mesh-general", joined_at: 900, unread: 3 },
          { channel: "quiet-net", joined_at: 800, unread: 0, muted: true },
          { channel: "mesh-radio", joined_at: 800, pinned: true, unread: 0 },
        ],
        count: 3,
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
    expect(await screen.findByRole("button", { name: /#mesh-general/ })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /#mesh-general/ }));
    // The message shows twice: once in the pinned strip, once in history.
    expect((await screen.findAllByText("hello channel")).length).toBe(2);
  });

  it("shows an unread badge on a channel with unread messages", async () => {
    render(<ChannelsSection />);
    expect(await screen.findByLabelText("3 unread")).toBeTruthy();
  });

  it("marks a muted channel and shows no badge for it", async () => {
    render(<ChannelsSection />);
    expect(await screen.findByLabelText("Muted channel")).toBeTruthy();
    expect(screen.queryByLabelText("0 unread")).toBeNull();
  });

  it("mutes the selected channel", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    fireEvent.click(screen.getByRole("button", { name: "Mute" }));
    await waitFor(() => expect(calls).toContain("POST /api/messages/mute"));
  });

  it("marks a channel read when it is opened", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    await waitFor(() => expect(calls).toContain("POST /api/messages/read"));
  });

  it("joins a new channel by name", async () => {
    render(<ChannelsSection />);
    await screen.findByRole("button", { name: /#mesh-general/ });
    fireEvent.change(screen.getByLabelText("Channel name to join"), {
      target: { value: "mesh-radio" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Join" }));
    await waitFor(() => expect(calls).toContain("POST /api/channels/join"));
  });

  it("sends a message to the selected channel", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    await screen.findAllByText("hello channel");
    fireEvent.change(screen.getByLabelText("Message #mesh-general"), {
      target: { value: "hi all" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Send" }));
    await waitFor(() => expect(calls).toContain("POST /api/channels/send"));
  });

  it("shows a reply with its quoted parent", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    expect(await screen.findByText("count me in")).toBeTruthy();
    expect(await screen.findByText("↩ alice: hello channel")).toBeTruthy();
  });

  it("reply button arms the composer for that message", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
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
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    fireEvent.click(screen.getByRole("button", { name: /Sync/ }));
    await waitFor(() => expect(calls).toContain("POST /api/channels/sync"));
  });

  it("marks a pinned channel and pins the selected one", async () => {
    render(<ChannelsSection />);
    expect(await screen.findByRole("button", { name: "📌 #mesh-radio" })).toBeTruthy();
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    fireEvent.click(screen.getByRole("button", { name: "Pin" }));
    await waitFor(() => expect(calls).toContain("POST /api/conversations/pin"));
  });

  it("offers Unpin for a pinned selected channel", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: "📌 #mesh-radio" }));
    expect(screen.getByRole("button", { name: "Unpin" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Unpin" }));
    await waitFor(() => expect(calls).toContain("POST /api/conversations/pin"));
  });

  it("shows scheduled messages for the selected channel with cancel", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    expect(await screen.findByText("morning hello")).toBeTruthy();
    // One Cancel per scheduled entry (message + poll).
    const cancelButtons = screen.getAllByRole("button", { name: "Cancel" });
    expect(cancelButtons.length).toBe(2);
    fireEvent.click(cancelButtons[0]);
    await waitFor(() =>
      expect(calls).toContain("POST /api/channels/cancel-scheduled"),
    );
  });

  it("shows a scheduled poll with its options", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    expect(await screen.findByText("pizza or tacos?")).toBeTruthy();
    expect(screen.getByText("Poll · 2 options")).toBeTruthy();
    expect(screen.getByText("1. Pizza")).toBeTruthy();
    expect(screen.getByText("2. Tacos")).toBeTruthy();
  });

  it("edits a scheduled message's text and time from the Scheduled rows", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    expect(await screen.findByText("morning hello")).toBeTruthy();
    // Two scheduled entries render an Edit button (message + poll);
    // the first is the "morning hello" message this test edits.
    fireEvent.click(
      screen.getAllByRole("button", { name: "Edit scheduled message" })[0],
    );
    fireEvent.change(
      screen.getByLabelText("Edit scheduled message", {
        selector: "textarea",
      }),
      { target: { value: "morning, mesh!" } },
    );
    fireEvent.change(screen.getByLabelText("New send time"), {
      target: { value: "3600" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() =>
      expect(calls).toContain("POST /api/channels/edit-scheduled"),
    );
  });

  it("an unchanged scheduled edit makes no request", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    expect(await screen.findByText("morning hello")).toBeTruthy();
    // Two scheduled entries render an Edit button (message + poll);
    // the first is the "morning hello" message this test edits.
    fireEvent.click(
      screen.getAllByRole("button", { name: "Edit scheduled message" })[0],
    );
    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    // The form closes (its Edit button is back) without touching the API.
    await waitFor(() =>
      expect(
        screen.getAllByRole("button", { name: "Edit scheduled message" })[0],
      ).toBeTruthy(),
    );
    expect(screen.queryByLabelText("New send time")).toBeNull();
    expect(calls).not.toContain("POST /api/channels/edit-scheduled");
  });

  it("schedules a message when Send later is picked", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
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
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    expect(await screen.findByText("which day?")).toBeTruthy();
    expect(screen.getByRole("button", { name: /Saturday/ })).toBeTruthy();
    expect(screen.getByText(/2 votes/)).toBeTruthy();
    expect(screen.getByRole("button", { name: "Close poll" })).toBeTruthy();
  });

  it("votes on a channel poll option", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    fireEvent.click(await screen.findByRole("button", { name: /Sunday/ }));
    await waitFor(() => expect(calls).toContain("POST /api/channels/vote"));
  });

  it("posts a channel poll from the composer", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
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

  it("schedules a poll when Send later is picked in the poll form", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    fireEvent.click(screen.getByRole("button", { name: "Poll" }));
    fireEvent.change(screen.getByLabelText("Poll question"), {
      target: { value: "pizza or tacos?" },
    });
    fireEvent.change(screen.getByLabelText("Poll option 1"), {
      target: { value: "Pizza" },
    });
    fireEvent.change(screen.getByLabelText("Poll option 2"), {
      target: { value: "Tacos" },
    });
    fireEvent.change(screen.getByLabelText("Send later"), {
      target: { value: "300" },
    });
    expect(screen.getByRole("button", { name: "Schedule poll" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Schedule poll" }));
    await waitFor(() =>
      expect(calls).toContain("POST /api/channels/schedule"),
    );
    expect(calls).not.toContain("POST /api/channels/poll");
  });

  it("shows the channel topic and saves an edited one", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
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
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    await screen.findByText("monthly mesh meetup");
    fireEvent.click(screen.getByRole("button", { name: "Clear topic" }));
    await waitFor(() => expect(calls).toContain("POST /api/channels/topic"));
  });

  it("shows pins in a strip and unpins from it", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
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
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    const button = await screen.findByRole("button", {
      name: "Unpin message from alice",
    });
    expect(button.textContent).toBe("Pinned");
  });

  it("searches the selected channel and clears back to history", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
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
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    expect(await screen.findByText("hello, fixed")).toBeTruthy();
    expect(screen.getByText(/edited/)).toBeTruthy();
    // Only alice's own message gets an Edit button; bob's does not.
    expect(screen.getAllByRole("button", { name: "Edit" })).toHaveLength(1);
  });

  it("edits your own message from the console", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
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
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
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
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    expect(await screen.findByRole("button", { name: "React 👍 (2)" })).toBeTruthy();
    expect(screen.getByRole("button", { name: "React 🎉 (1)" })).toBeTruthy();
  });

  it("saves the composer text as a channel draft", async () => {
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    await screen.findAllByText("hello channel");
    fireEvent.change(screen.getByLabelText("Message #mesh-general"), {
      target: { value: "park me" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save draft" }));
    await waitFor(() => expect(calls).toContain("POST /api/drafts"));
    // The box clears once the save lands, and the just-saved draft
    // does not pour straight back in.
    await waitFor(() =>
      expect(
        (screen.getByLabelText("Message #mesh-general") as HTMLInputElement).value,
      ).toBe(""),
    );
  });

  it("saving a draft does not pour it back into the composer", async () => {
    const saved: { kind: string; target: string; text: string; updated_at: number; name: string }[] =
      [];
    calls = mockFetch((url, method) => {
      if (url.includes("/api/drafts/delete")) return { ok: true, deleted: true };
      if (url.includes("/api/drafts")) {
        if (method === "POST") {
          saved.push({
            kind: "channel",
            target: "mesh-general",
            text: "park me",
            updated_at: 100,
            name: "mesh-general",
          });
          return { ok: true, draft: saved[0] };
        }
        return { drafts: saved, count: saved.length };
      }
      if (url.includes("/api/channels/messages")) {
        return { channel: "mesh-general", messages: [], count: 0 };
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
          channels: [{ channel: "mesh-general", joined_at: 900, unread: 0 }],
          count: 1,
        };
      }
      return { ok: true };
    }).calls;
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    const input = await screen.findByLabelText("Message #mesh-general");
    fireEvent.change(input, { target: { value: "park me" } });
    fireEvent.click(screen.getByRole("button", { name: "Save draft" }));
    await waitFor(() => expect(calls).toContain("POST /api/drafts"));
    // The box clears on save; give the drafts refresh time to land
    // the just-saved draft back in the list, then confirm it did not
    // re-fill the box.
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect((input as HTMLInputElement).value).toBe("");
  });

  it("opens the channel named by the hash and loads its saved draft", async () => {
    window.location.hash = "#channels?channel=mesh-general";
    calls = mockFetch((url, method) => {
      if (url.includes("/api/drafts/delete")) return { ok: true, deleted: true };
      if (url.includes("/api/drafts")) {
        return {
          drafts: [
            {
              kind: "channel",
              target: "mesh-general",
              text: "parked hello",
              updated_at: 100,
              name: "mesh-general",
            },
          ],
          count: 1,
        };
      }
      if (url.includes("/api/channels/messages")) {
        return { channel: "mesh-general", messages: [], count: 0 };
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
          channels: [{ channel: "mesh-general", joined_at: 900, unread: 0 }],
          count: 1,
        };
      }
      return { ok: true };
    }).calls;
    try {
      render(<ChannelsSection />);
      const input = await screen.findByLabelText("Message #mesh-general");
      await waitFor(() =>
        expect((input as HTMLInputElement).value).toBe("parked hello"),
      );
    } finally {
      window.location.hash = "";
    }
  });

  it("deletes the channel draft after the seeded message is sent", async () => {
    window.location.hash = "#channels?channel=mesh-general";
    calls = mockFetch((url, method) => {
      if (url.includes("/api/drafts/delete")) return { ok: true, deleted: true };
      if (url.includes("/api/drafts")) {
        return {
          drafts: [
            {
              kind: "channel",
              target: "mesh-general",
              text: "parked hello",
              updated_at: 100,
              name: "mesh-general",
            },
          ],
          count: 1,
        };
      }
      if (url.includes("/api/channels/messages")) {
        return { channel: "mesh-general", messages: [], count: 0 };
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
          channels: [{ channel: "mesh-general", joined_at: 900, unread: 0 }],
          count: 1,
        };
      }
      return { ok: true };
    }).calls;
    try {
      render(<ChannelsSection />);
      const input = await screen.findByLabelText("Message #mesh-general");
      await waitFor(() =>
        expect((input as HTMLInputElement).value).toBe("parked hello"),
      );
      fireEvent.click(screen.getByRole("button", { name: "Send" }));
      await waitFor(() => expect(calls).toContain("POST /api/channels/send"));
      await waitFor(() => expect(calls).toContain("POST /api/drafts/delete"));
    } finally {
      window.location.hash = "";
    }
  });

  it("selecting a channel updates the URL hash", async () => {
    try {
      render(<ChannelsSection />);
      fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
      await screen.findAllByText("hello channel");
      expect(window.location.hash).toBe("#channels?channel=mesh-general");
      fireEvent.click(screen.getByRole("button", { name: /#quiet-net/ }));
      await screen.findByLabelText("Message #quiet-net");
      expect(window.location.hash).toBe("#channels?channel=quiet-net");
    } finally {
      window.location.hash = "";
    }
  });

  it("sending a new message after saving a draft keeps the draft", async () => {
    const saved: { kind: string; target: string; text: string; updated_at: number; name: string }[] =
      [];
    calls = mockFetch((url, method) => {
      if (url.includes("/api/drafts/delete")) return { ok: true, deleted: true };
      if (url.includes("/api/drafts")) {
        if (method === "POST") {
          saved.push({
            kind: "channel",
            target: "mesh-general",
            text: "park me",
            updated_at: 100,
            name: "mesh-general",
          });
          return { ok: true, draft: saved[0] };
        }
        return { drafts: saved, count: saved.length };
      }
      if (url.includes("/api/channels/messages")) {
        return { channel: "mesh-general", messages: [], count: 0 };
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
          channels: [{ channel: "mesh-general", joined_at: 900, unread: 0 }],
          count: 1,
        };
      }
      return { ok: true };
    }).calls;
    render(<ChannelsSection />);
    fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
    const input = await screen.findByLabelText("Message #mesh-general");
    fireEvent.change(input, { target: { value: "park me" } });
    fireEvent.click(screen.getByRole("button", { name: "Save draft" }));
    await waitFor(() => expect(calls).toContain("POST /api/drafts"));
    await waitFor(() =>
      expect((input as HTMLInputElement).value).toBe(""),
    );
    // Let the drafts refresh land the just-saved draft, then send an
    // unrelated message: the saved draft must survive it.
    await new Promise((resolve) => setTimeout(resolve, 100));
    fireEvent.change(input, { target: { value: "a fresh message" } });
    fireEvent.click(screen.getByRole("button", { name: "Send" }));
    await waitFor(() => expect(calls).toContain("POST /api/channels/send"));
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(calls).not.toContain("POST /api/drafts/delete");
  });

  it("returning to a channel after switching away still closes its draft on send", async () => {
    const saved: { kind: string; target: string; text: string; updated_at: number; name: string }[] =
      [
        {
          kind: "channel",
          target: "mesh-general",
          text: "parked hello",
          updated_at: 100,
          name: "mesh-general",
        },
      ];
    calls = mockFetch((url) => {
      if (url.includes("/api/drafts/delete")) {
        saved.length = 0;
        return { ok: true, deleted: true };
      }
      if (url.includes("/api/drafts")) {
        return { drafts: saved, count: saved.length };
      }
      if (url.includes("/api/channels/messages")) {
        return { channel: "mesh-general", messages: [], count: 0 };
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
          channels: [
            { channel: "mesh-general", joined_at: 900, unread: 0 },
            { channel: "mesh-random", joined_at: 800, unread: 0 },
          ],
          count: 2,
        };
      }
      return { ok: true };
    }).calls;
    try {
      render(<ChannelsSection />);
      fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
      const input = await screen.findByLabelText("Message #mesh-general");
      await waitFor(() =>
        expect((input as HTMLInputElement).value).toBe("parked hello"),
      );
      // Leave and come back: the draft text returns, and it must
      // still be recognized as the loaded draft when sent.
      fireEvent.click(screen.getByRole("button", { name: /#mesh-random/ }));
      await screen.findByLabelText("Message #mesh-random");
      fireEvent.click(screen.getByRole("button", { name: /#mesh-general/ }));
      const inputAgain = await screen.findByLabelText("Message #mesh-general");
      await waitFor(() =>
        expect((inputAgain as HTMLInputElement).value).toBe("parked hello"),
      );
      fireEvent.click(screen.getByRole("button", { name: "Send" }));
      await waitFor(() => expect(calls).toContain("POST /api/channels/send"));
      await waitFor(() => expect(calls).toContain("POST /api/drafts/delete"));
      // The draft is deleted exactly once and does not re-seed the
      // composer, so it cannot be sent a second time.
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(
        calls.filter((c) => c === "POST /api/drafts/delete").length,
      ).toBe(1);
      expect((inputAgain as HTMLInputElement).value).toBe("");
    } finally {
      window.location.hash = "";
    }
  });

  it("a saved draft pre-fills the composer again after switching away and back", async () => {
    const saved: { kind: string; target: string; text: string; updated_at: number; name: string }[] =
      [];
    calls = mockFetch((url, method) => {
      if (url.includes("/api/drafts/delete")) return { ok: true, deleted: true };
      if (url.includes("/api/drafts")) {
        if (method === "POST") {
          saved.push({
            kind: "channel",
            target: "mesh-general",
            text: "park me",
            updated_at: 100,
            name: "mesh-general",
          });
          return { ok: true, draft: saved[0] };
        }
        return { drafts: saved, count: saved.length };
      }
      if (url.includes("/api/channels/messages")) {
        return { channel: "mesh-general", messages: [], count: 0 };
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
          channels: [
            { channel: "mesh-general", joined_at: 900, unread: 0 },
            { channel: "mesh-random", joined_at: 800, unread: 0 },
          ],
          count: 2,
        };
      }
      return { ok: true };
    }).calls;
    try {
      render(<ChannelsSection />);
      fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
      const input = await screen.findByLabelText("Message #mesh-general");
      fireEvent.change(input, { target: { value: "park me" } });
      fireEvent.click(screen.getByRole("button", { name: "Save draft" }));
      await waitFor(() => expect(calls).toContain("POST /api/drafts"));
      await waitFor(() =>
        expect((input as HTMLInputElement).value).toBe(""),
      );
      // Leave and come back: the saved draft pre-fills the box
      // again, and nothing is saved a second time.
      fireEvent.click(screen.getByRole("button", { name: /#mesh-random/ }));
      await screen.findByLabelText("Message #mesh-random");
      fireEvent.click(screen.getByRole("button", { name: /#mesh-general/ }));
      const inputAgain = await screen.findByLabelText("Message #mesh-general");
      await waitFor(() =>
        expect((inputAgain as HTMLInputElement).value).toBe("park me"),
      );
      expect(
        calls.filter((c) => c === "POST /api/drafts").length,
      ).toBe(1);
    } finally {
      window.location.hash = "";
    }
  });

  it("switching away and back while the draft deletion is pending does not restore the sent text", async () => {
    const saved: { kind: string; target: string; text: string; updated_at: number; name: string }[] =
      [
        {
          kind: "channel",
          target: "mesh-general",
          text: "parked hello",
          updated_at: 100,
          name: "mesh-general",
        },
      ];
    let resolveDelete: ((value: string) => void) | null = null;
    calls = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        const method = init?.method ?? "GET";
        calls.push(`${method} ${url}`);
        if (url.includes("/api/drafts/delete")) {
          saved.length = 0;
          return {
            ok: true,
            status: 200,
            text: () =>
              new Promise<string>((resolve) => {
                resolveDelete = resolve;
              }),
          } as Response;
        }
        if (url.includes("/api/drafts")) {
          return {
            ok: true,
            status: 200,
            text: async () =>
              JSON.stringify({ drafts: saved, count: saved.length }),
          } as Response;
        }
        if (url.includes("/api/channels/messages")) {
          return {
            ok: true,
            status: 200,
            text: async () =>
              JSON.stringify({ channel: "mesh-general", messages: [], count: 0 }),
          } as Response;
        }
        if (url.includes("/api/channels/pins")) {
          return {
            ok: true,
            status: 200,
            text: async () =>
              JSON.stringify({ channel: "mesh-general", pins: [], count: 0 }),
          } as Response;
        }
        if (url.includes("/api/channels/scheduled")) {
          return {
            ok: true,
            status: 200,
            text: async () =>
              JSON.stringify({ channel: "mesh-general", scheduled: [], count: 0 }),
          } as Response;
        }
        if (url.includes("/api/channels/topic")) {
          return {
            ok: true,
            status: 200,
            text: async () =>
              JSON.stringify({ channel: "mesh-general", topic: null }),
          } as Response;
        }
        if (url.includes("/api/channels")) {
          return {
            ok: true,
            status: 200,
            text: async () =>
              JSON.stringify({
                channels: [
                  { channel: "mesh-general", joined_at: 900, unread: 0 },
                  { channel: "mesh-random", joined_at: 800, unread: 0 },
                ],
                count: 2,
              }),
          } as Response;
        }
        return {
          ok: true,
          status: 200,
          text: async () => JSON.stringify({ ok: true }),
        } as Response;
      }),
    );
    try {
      render(<ChannelsSection />);
      fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
      const input = await screen.findByLabelText("Message #mesh-general");
      await waitFor(() =>
        expect((input as HTMLInputElement).value).toBe("parked hello"),
      );
      fireEvent.click(screen.getByRole("button", { name: "Send" }));
      await waitFor(() => expect(calls).toContain("POST /api/channels/send"));
      // The deletion is still pending; the box is already clear.
      await waitFor(() => expect(resolveDelete).not.toBeNull());
      await waitFor(() =>
        expect((input as HTMLInputElement).value).toBe(""),
      );
      // Switch away and back while it hangs: the sent text must not
      // come back.
      fireEvent.click(screen.getByRole("button", { name: /#mesh-random/ }));
      await screen.findByLabelText("Message #mesh-random");
      fireEvent.click(screen.getByRole("button", { name: /#mesh-general/ }));
      const inputAgain = await screen.findByLabelText("Message #mesh-general");
      await waitFor(() =>
        expect((inputAgain as HTMLInputElement).value).toBe(""),
      );
      resolveDelete!(JSON.stringify({ ok: true, deleted: true }));
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect((inputAgain as HTMLInputElement).value).toBe("");
      expect(
        calls.filter((c) => c === "POST /api/drafts/delete").length,
      ).toBe(1);
    } finally {
      window.location.hash = "";
    }
  });

  it("a stale draft-list response cannot restore a sent draft", async () => {
    const saved: { kind: string; target: string; text: string; updated_at: number; name: string }[] =
      [
        {
          kind: "channel",
          target: "mesh-general",
          text: "parked hello",
          updated_at: 100,
          name: "mesh-general",
        },
      ];
    const staleDraft = { ...saved[0] };
    // GET /api/drafts calls: the first resolves at once; the second
    // (the save's refresh) is held until after the send.
    const heldDrafts: { resolve: (value: string) => void }[] = [];
    let draftsGets = 0;
    calls = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        const method = init?.method ?? "GET";
        calls.push(`${method} ${url}`);
        if (url.includes("/api/drafts/delete")) {
          saved.length = 0;
          return {
            ok: true,
            status: 200,
            text: async () =>
              JSON.stringify({ ok: true, deleted: true }),
          } as Response;
        }
        if (url.includes("/api/drafts")) {
          if (method === "POST") {
            return {
              ok: true,
              status: 200,
              text: async () =>
                JSON.stringify({ ok: true, draft: saved[0] }),
            } as Response;
          }
          draftsGets += 1;
          if (draftsGets === 2) {
            const entry: { resolve: (value: string) => void } = {
              resolve: () => undefined,
            };
            const promise = new Promise<string>((resolve) => {
              entry.resolve = resolve;
            });
            heldDrafts.push(entry);
            return { ok: true, status: 200, text: () => promise } as Response;
          }
          return {
            ok: true,
            status: 200,
            text: async () =>
              JSON.stringify({ drafts: saved, count: saved.length }),
          } as Response;
        }
        if (url.includes("/api/channels/messages")) {
          return {
            ok: true,
            status: 200,
            text: async () =>
              JSON.stringify({ channel: "mesh-general", messages: [], count: 0 }),
          } as Response;
        }
        if (url.includes("/api/channels/pins")) {
          return {
            ok: true,
            status: 200,
            text: async () =>
              JSON.stringify({ channel: "mesh-general", pins: [], count: 0 }),
          } as Response;
        }
        if (url.includes("/api/channels/scheduled")) {
          return {
            ok: true,
            status: 200,
            text: async () =>
              JSON.stringify({ channel: "mesh-general", scheduled: [], count: 0 }),
          } as Response;
        }
        if (url.includes("/api/channels/topic")) {
          return {
            ok: true,
            status: 200,
            text: async () =>
              JSON.stringify({ channel: "mesh-general", topic: null }),
          } as Response;
        }
        if (url.includes("/api/channels")) {
          return {
            ok: true,
            status: 200,
            text: async () =>
              JSON.stringify({
                channels: [
                  { channel: "mesh-general", joined_at: 900, unread: 0 },
                  { channel: "mesh-random", joined_at: 800, unread: 0 },
                ],
                count: 2,
              }),
          } as Response;
        }
        return {
          ok: true,
          status: 200,
          text: async () => JSON.stringify({ ok: true }),
        } as Response;
      }),
    );
    try {
      render(<ChannelsSection />);
      fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
      const input = await screen.findByLabelText("Message #mesh-general");
      await waitFor(() =>
        expect((input as HTMLInputElement).value).toBe("parked hello"),
      );
      // Save the loaded draft: its refresh is the stale response.
      fireEvent.click(screen.getByRole("button", { name: "Save draft" }));
      await waitFor(() => expect(calls).toContain("POST /api/drafts"));
      await waitFor(() =>
        expect((input as HTMLInputElement).value).toBe(""),
      );
      await waitFor(() => expect(heldDrafts.length).toBe(1));
      // Switch away and back: the saved draft pre-fills again.
      fireEvent.click(screen.getByRole("button", { name: /#mesh-random/ }));
      await screen.findByLabelText("Message #mesh-random");
      fireEvent.click(screen.getByRole("button", { name: /#mesh-general/ }));
      const inputAgain = await screen.findByLabelText("Message #mesh-general");
      await waitFor(() =>
        expect((inputAgain as HTMLInputElement).value).toBe("parked hello"),
      );
      // Send it: the post-delete refresh resolves with an empty list.
      fireEvent.click(screen.getByRole("button", { name: "Send" }));
      await waitFor(() => expect(calls).toContain("POST /api/drafts/delete"));
      await waitFor(() =>
        expect((inputAgain as HTMLInputElement).value).toBe(""),
      );
      // Now the stale pre-delete response arrives: it must be dropped.
      heldDrafts[0].resolve(
        JSON.stringify({ drafts: [staleDraft], count: 1 }),
      );
      await new Promise((resolve) => setTimeout(resolve, 100));
      // Switch away and back: the deleted draft must not come back.
      fireEvent.click(screen.getByRole("button", { name: /#mesh-random/ }));
      await screen.findByLabelText("Message #mesh-random");
      fireEvent.click(screen.getByRole("button", { name: /#mesh-general/ }));
      const inputFinal = await screen.findByLabelText("Message #mesh-general");
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect((inputFinal as HTMLInputElement).value).toBe("");
    } finally {
      window.location.hash = "";
    }
  });

  it("switching channels keeps each channel's unsent text and draft separate", async () => {
    calls = mockFetch((url) => {
      if (url.includes("/api/drafts")) {
        return {
          drafts: [
            {
              kind: "channel",
              target: "mesh-random",
              text: "random draft",
              updated_at: 100,
              name: "mesh-random",
            },
          ],
          count: 1,
        };
      }
      if (url.includes("/api/channels/messages")) {
        return { channel: "mesh-general", messages: [], count: 0 };
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
          channels: [
            { channel: "mesh-general", joined_at: 900, unread: 0 },
            { channel: "mesh-random", joined_at: 800, unread: 0 },
          ],
          count: 2,
        };
      }
      return { ok: true };
    }).calls;
    try {
      render(<ChannelsSection />);
      fireEvent.click(await screen.findByRole("button", { name: /#mesh-general/ }));
      const generalInput = await screen.findByLabelText("Message #mesh-general");
      fireEvent.change(generalInput, { target: { value: "general text" } });
      // The other channel's saved draft loads into an empty box.
      fireEvent.click(screen.getByRole("button", { name: /#mesh-random/ }));
      const randomInput = await screen.findByLabelText("Message #mesh-random");
      await waitFor(() =>
        expect((randomInput as HTMLInputElement).value).toBe("random draft"),
      );
      // Back to the first channel: its own unsent text returns, and
      // nothing was saved over the other channel's draft.
      fireEvent.click(screen.getByRole("button", { name: /#mesh-general/ }));
      const generalAgain = await screen.findByLabelText("Message #mesh-general");
      await waitFor(() =>
        expect((generalAgain as HTMLInputElement).value).toBe("general text"),
      );
      expect(calls).not.toContain("POST /api/drafts");
      fireEvent.click(screen.getByRole("button", { name: /#mesh-random/ }));
      const randomAgain = await screen.findByLabelText("Message #mesh-random");
      await waitFor(() =>
        expect((randomAgain as HTMLInputElement).value).toBe("random draft"),
      );
    } finally {
      window.location.hash = "";
    }
  });
});
