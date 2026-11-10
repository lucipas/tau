import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

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

describe("MessageFeed delete", () => {
  it("shows no delete button without an onDelete handler", () => {
    render(<MessageFeed messages={[makeMessage()]} node={null} live={true} />);
    expect(screen.queryByTitle("Delete this message")).toBeNull();
  });

  it("asks for confirmation before deleting", async () => {
    const message = makeMessage();
    const onDelete = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[message]}
        node={null}
        live={true}
        onDelete={onDelete}
      />,
    );

    fireEvent.click(screen.getByTitle("Delete this message"));
    expect(onDelete).not.toHaveBeenCalled();
    expect(screen.getByText("confirm delete")).toBeTruthy();

    fireEvent.click(screen.getByText("confirm delete"));
    expect(onDelete).toHaveBeenCalledTimes(1);
    expect(onDelete).toHaveBeenCalledWith(message);
  });

  it("keeps the message when the confirmation is cancelled", () => {
    const onDelete = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[makeMessage()]}
        node={null}
        live={true}
        onDelete={onDelete}
      />,
    );

    fireEvent.click(screen.getByTitle("Delete this message"));
    fireEvent.click(screen.getByText("keep"));
    expect(onDelete).not.toHaveBeenCalled();
    expect(screen.queryByText("confirm delete")).toBeNull();
    expect(screen.getByTitle("Delete this message")).toBeTruthy();
  });

  it("reports a failed delete inline and keeps the message", async () => {
    const onDelete = vi.fn(async () => {
      throw new Error("boom");
    });
    render(
      <MessageFeed
        messages={[makeMessage()]}
        node={null}
        live={true}
        onDelete={onDelete}
      />,
    );

    fireEvent.click(screen.getByTitle("Delete this message"));
    fireEvent.click(screen.getByText("confirm delete"));
    expect(await screen.findByText("boom")).toBeTruthy();
    // The bubble stays, with the plain delete button back for a retry.
    expect(screen.getByText("hello")).toBeTruthy();
    expect(screen.getByTitle("Delete this message")).toBeTruthy();
  });

  it("hides the delete button while the message is still queued", () => {
    // A queued message is removed by cancelling its outbox entry, not
    // by deleting the bubble: deleting here would leave the outbox
    // entry to deliver it anyway.
    const onDelete = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[makeMessage({ status: "queued" })]}
        node={null}
        live={true}
        onDelete={onDelete}
      />,
    );
    expect(screen.queryByTitle("Delete this message")).toBeNull();
  });
});
