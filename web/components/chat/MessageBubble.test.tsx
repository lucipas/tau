import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import MessageBubble from "./MessageBubble";
import { makeMessage } from "../test-utils";

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
