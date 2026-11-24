import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import MessageBubble, { secondsUntilTomorrowNine } from "./MessageBubble";
import { makeMessage } from "../test-utils";

describe("secondsUntilTomorrowNine", () => {
  it("returns the seconds until 09:00, today or tomorrow", () => {
    expect(secondsUntilTomorrowNine(new Date("2026-11-21T08:00:00"))).toBe(25 * 3600);
    expect(secondsUntilTomorrowNine(new Date("2026-11-21T10:00:00"))).toBe(23 * 3600);
    expect(secondsUntilTomorrowNine(new Date("2026-11-21T09:00:00"))).toBe(24 * 3600);
  });
});

describe("MessageBubble", () => {
  it("renders the text and timestamp, and the action menu for own messages", () => {
    const onAction = vi.fn();
    render(
      <MessageBubble
        message={makeMessage({ id: 7, direction: "out", text: "my message", sent_at: 1700000000 })}
        onAction={onAction}
        now={1700000060}
      />,
    );
    expect(screen.getByText("my message")).toBeTruthy();
    fireEvent.click(screen.getByLabelText("Actions for message 7"));
    for (const label of ["Reply", "Edit", "Retract", "Delete", "Pin", "Forward", "React"]) {
      expect(screen.getByText(label)).toBeTruthy();
    }
    fireEvent.click(screen.getByText("Reply"));
    expect(onAction).toHaveBeenCalledWith("reply", expect.objectContaining({ id: 7 }));
  });

  it("does not offer edit/retract on incoming messages", () => {
    render(
      <MessageBubble
        message={makeMessage({ id: 8, direction: "in", text: "their message" })}
        now={1700000060}
      />,
    );
    fireEvent.click(screen.getByLabelText("Actions for message 8"));
    expect(screen.queryByText("Edit")).toBeNull();
    expect(screen.queryByText("Retract")).toBeNull();
    expect(screen.getByText("Reply")).toBeTruthy();
  });

  it("offers snooze on incoming messages and calls onSnooze with the preset", () => {
    const onSnooze = vi.fn();
    render(
      <MessageBubble
        message={makeMessage({ id: 8, direction: "in", text: "their message" })}
        onSnooze={onSnooze}
        now={1700000060}
      />,
    );
    fireEvent.click(screen.getByLabelText("Actions for message 8"));
    fireEvent.click(screen.getByText("Snooze"));
    expect(screen.getByText("30 min")).toBeTruthy();
    expect(screen.getByText("Tomorrow 9am")).toBeTruthy();
    fireEvent.click(screen.getByText("1 hour"));
    expect(onSnooze).toHaveBeenCalledWith(expect.objectContaining({ id: 8 }), 3600);
  });

  it("does not offer snooze on own messages", () => {
    render(
      <MessageBubble
        message={makeMessage({ id: 7, direction: "out", text: "my message", sent_at: 1700000000 })}
        onSnooze={() => undefined}
        now={1700000060}
      />,
    );
    fireEvent.click(screen.getByLabelText("Actions for message 7"));
    expect(screen.queryByText("Snooze")).toBeNull();
  });

  it("does not offer snooze on incoming file messages or without onSnooze", () => {
    const { unmount } = render(
      <MessageBubble
        message={makeMessage({
          id: 10,
          direction: "in",
          text: "",
          file: { name: "notes.txt", size: 2048, sha256: "ab".repeat(32) },
        })}
        onSnooze={() => undefined}
        now={1700000060}
      />,
    );
    fireEvent.click(screen.getByLabelText("Actions for message 10"));
    expect(screen.queryByText("Snooze")).toBeNull();
    unmount();

    render(
      <MessageBubble
        message={makeMessage({ id: 11, direction: "in", text: "their message" })}
        now={1700000060}
      />,
    );
    fireEvent.click(screen.getByLabelText("Actions for message 11"));
    expect(screen.queryByText("Snooze")).toBeNull();
  });

  it("shows grouped reactions and a file download link", () => {
    render(
      <MessageBubble
        message={makeMessage({
          id: 9,
          text: "see attached",
          file: { name: "notes.txt", size: 2048, sha256: "ab".repeat(32) },
          reactions: [
            { reactor: "aa", name: "Alice", reaction: "👍" },
            { reactor: "bb", name: "Bob", reaction: "👍" },
          ],
        })}
        now={1700000060}
      />,
    );
    expect(screen.getByText("👍 2")).toBeTruthy();
    const link = screen.getByText(/notes\.txt/);
    expect(link.getAttribute("href")).toContain("/api/files/");
    expect(link.getAttribute("href")).toContain("notes.txt");
  });

  it("renders poll options and votes through onVotePoll", () => {
    const onVotePoll = vi.fn();
    render(
      <MessageBubble
        message={makeMessage({
          id: 10,
          text: "Best band?",
          poll_options: ["Reticulum", "LoRa"],
          poll: { options: ["Reticulum", "LoRa"], counts: [3, 1], total: 4, closed: false, voters: [], my_vote: null },
        })}
        onVotePoll={onVotePoll}
        now={1700000060}
      />,
    );
    expect(screen.getByText("Reticulum")).toBeTruthy();
    expect(screen.getByText("3 · 75%")).toBeTruthy();
    fireEvent.click(screen.getByText("LoRa"));
    expect(onVotePoll).toHaveBeenCalledWith(expect.objectContaining({ id: 10 }), 1);
  });

  it("renders a location pin with an open-in-maps link", () => {
    render(
      <MessageBubble
        message={makeMessage({
          id: 11,
          text: "meet here",
          location: { lat: 34.05, lon: -83.88, name: "Rock Springs Park" },
        })}
        now={1700000060}
      />,
    );
    const link = screen.getByRole("link", { name: /Rock Springs Park/ });
    expect(link.getAttribute("href")).toContain("openstreetmap.org");
    expect(link.getAttribute("href")).toContain("mlat=34.05");
    expect(link.getAttribute("href")).toContain("mlon=-83.88");
  });

  it("renders coordinates when a pin has no place name", () => {
    render(
      <MessageBubble
        message={makeMessage({
          id: 12,
          text: "📍 34.05, -83.88",
          location: { lat: 34.05, lon: -83.88 },
        })}
        now={1700000060}
      />,
    );
    expect(screen.getByText(/34\.05000, -83\.88000/)).toBeTruthy();
  });

  it("renders a contact card with an Add peer action", async () => {
    const onAddPeer = vi.fn().mockResolvedValue(undefined);
    render(
      <MessageBubble
        message={makeMessage({
          id: 13,
          text: "👤 Shared contact: carol (cccccccc…)",
          contact: { name: "carol", destination_hash: "cc".repeat(16) },
        })}
        onAddPeer={onAddPeer}
        now={1700000060}
      />,
    );
    // The card shows the peer's name and short hash (the message text
    // also names them, so assert on the card's hash line).
    expect(screen.getByText(/tau:cccccccc/)).toBeTruthy();
    fireEvent.click(screen.getByText("Add peer"));
    expect(onAddPeer).toHaveBeenCalledWith(expect.objectContaining({ id: 13 }));
    await vi.waitFor(() => expect(screen.getByText(/Added/)).toBeTruthy());
  });

  it("shows the inline error and keeps Add peer when adding fails", async () => {
    const onAddPeer = vi.fn().mockRejectedValue(new Error("peer did not announce"));
    render(
      <MessageBubble
        message={makeMessage({
          id: 15,
          text: "👤 Shared contact: carol (cccccccc…)",
          contact: { name: "carol", destination_hash: "cc".repeat(16) },
        })}
        onAddPeer={onAddPeer}
        now={1700000060}
      />,
    );
    fireEvent.click(screen.getByText("Add peer"));
    await vi.waitFor(() =>
      expect(screen.getByRole("alert").textContent).toMatch(/did not announce/),
    );
    // No "Added" line, and the button stays so the user can retry.
    expect(screen.queryByText(/Added/)).toBeNull();
    expect(screen.getByText("Add peer")).toBeTruthy();
  });

  it("hides Add peer on your own sent card", () => {
    render(
      <MessageBubble
        message={makeMessage({
          id: 14,
          direction: "out",
          text: "👤 Shared contact: carol (cccccccc…)",
          contact: { name: "carol", destination_hash: "cc".repeat(16) },
        })}
        onAddPeer={vi.fn()}
        now={1700000060}
      />,
    );
    expect(screen.queryByText("Add peer")).toBeNull();
  });

  it("shows the disappearing countdown", () => {
    render(
      <MessageBubble
        message={makeMessage({ id: 11, text: "secret", expires_at: 1700003600 })}
        now={1700000000}
      />,
    );
    expect(screen.getByText(/expires in 1h/)).toBeTruthy();
  });
});
