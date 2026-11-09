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
  const file = {
    name: "report.pdf",
    size: 2048,
    sha256: "ab".repeat(32),
  };

  it("shows a download link for a received file", () => {
    const message = makeMessage({
      text: "Received file: report.pdf (2048 bytes, sha256 ab…)",
      file,
    });
    render(<MessageFeed messages={[message]} node={null} live={true} />);

    expect(screen.getByText("report.pdf")).toBeTruthy();
    expect(screen.getByText("2.0 KB")).toBeTruthy();
    const link = screen.getByRole("link", { name: "Download" });
    // jsdom runs on :3000, so the configured API base points at the
    // bridge on :8787 — the link must target the bridge, not the
    // page's own origin.
    expect(link.getAttribute("href")).toBe(
      `http://localhost:8787/api/files/${message.peer}/report.pdf`,
    );
  });

  it("shows file metadata but no download link for an outbound file", () => {
    const message = makeMessage({ direction: "out", file });
    render(<MessageFeed messages={[message]} node={null} live={true} />);

    expect(screen.getByText("report.pdf")).toBeTruthy();
    expect(screen.queryByRole("link", { name: "Download" })).toBeNull();
  });

  it("renders no file block when a message has no file", () => {
    render(
      <MessageFeed messages={[makeMessage()]} node={null} live={true} />,
    );
    expect(screen.queryByRole("link", { name: "Download" })).toBeNull();
    expect(screen.queryByText("📎")).toBeNull();
  });
});
