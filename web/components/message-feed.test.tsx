import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { TauMessage } from "@/lib/types";

import { MessageFeed } from "./message-feed";

// jsdom does not implement scrollIntoView, which the feed calls to
// stick to the newest message.
if (!Element.prototype.scrollIntoView) {
  Element.prototype.scrollIntoView = () => {};
}

function makeMessage(overrides: Partial<TauMessage> = {}): TauMessage {
  return {
    id: 1,
    direction: "in",
    peer: "ab".repeat(16),
    peer_name: "alice",
    text: "hello",
    sent_at: 1000,
    received_at: 1001,
    status: "received",
    ...overrides,
  };
}

describe("MessageFeed reactions", () => {
  it("renders reactions under the message bubble", () => {
    const message = makeMessage({
      reactions: [
        { reactor: "cd".repeat(16), name: "bob", reaction: "👍" },
        { reactor: "ef".repeat(16), name: "carol", reaction: "❤️" },
      ],
    });
    render(<MessageFeed messages={[message]} node={null} live={true} />);

    expect(screen.getByLabelText("Reactions")).toBeTruthy();
    expect(screen.getByText("👍")).toBeTruthy();
    expect(screen.getByText("bob")).toBeTruthy();
    expect(screen.getByText("❤️")).toBeTruthy();
    expect(screen.getByText("carol")).toBeTruthy();
  });

  it("renders nothing extra when a message has no reactions", () => {
    render(
      <MessageFeed messages={[makeMessage()]} node={null} live={true} />,
    );
    expect(screen.queryByLabelText("Reactions")).toBeNull();
  });
});

describe("MessageFeed file entries", () => {
  it("renders the file name, size and hash for a sent file", () => {
    const message = makeMessage({
      direction: "out",
      text: "Sent file: notes.txt (10 bytes)",
      file: { name: "notes.txt", size: 10, sha256: "a".repeat(64) },
    });
    render(<MessageFeed messages={[message]} node={null} live={true} />);

    expect(screen.getByText("notes.txt")).toBeTruthy();
    expect(screen.getByText("10 B")).toBeTruthy();
    expect(screen.getByTitle(`SHA-256 ${"a".repeat(64)}`)).toBeTruthy();
  });

  it("renders nothing extra when a message carries no file", () => {
    render(
      <MessageFeed messages={[makeMessage()]} node={null} live={true} />,
    );
    expect(screen.queryByText("10 B")).toBeNull();
  });
});
