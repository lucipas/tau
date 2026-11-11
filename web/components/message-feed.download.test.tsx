import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

// The dev console runs on :3000 while the bridge listens on :8787, so
// the download link must go through the configured API base, not a
// relative URL that would hit the Next dev server instead.
vi.mock("@/lib/api", () => ({ API_BASE: "http://localhost:8787" }));

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
    text: "Received file: report.pdf (2048 bytes, sha256 ab…)",
    sent_at: 1000,
    received_at: 1001,
    status: "received",
    file: { name: "report.pdf", size: 2048, sha256: "ab".repeat(32) },
    ...overrides,
  };
}

describe("MessageFeed download link", () => {
  it("targets the bridge through the configured API base", () => {
    const message = makeMessage();
    render(<MessageFeed messages={[message]} node={null} live={true} />);

    const link = screen.getByRole("link", { name: "Download" });
    expect(link.getAttribute("href")).toBe(
      `http://localhost:8787/api/files/${message.peer}/report.pdf`,
    );
  });
});
