import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { TauMessage } from "@/lib/types";

import { MessageFeed, resolveQuote } from "./message-feed";

// jsdom has no scrollIntoView; MessageFeed calls it on new messages.
if (typeof Element !== "undefined") {
  Element.prototype.scrollIntoView ??= () => {};
}

const PEER = "ab".repeat(16);

function makeMessage(overrides: Partial<TauMessage> = {}): TauMessage {
  return {
    id: 1,
    direction: "in",
    peer: PEER,
    peer_name: "alice",
    text: "hello",
    sent_at: 1000,
    received_at: 1001,
    status: "received",
    ...overrides,
  };
}

describe("resolveQuote", () => {
  it("resolves the quoted text from the same conversation", () => {
    const original = makeMessage({ id: 1, text: "original thought", wire_id: "a".repeat(32) });
    const reply = makeMessage({
      id: 2,
      text: "seconded",
      wire_id: "b".repeat(32),
      in_reply_to: { id: "a".repeat(32), excerpt: "0123456789abcdef" },
    });
    expect(resolveQuote([original, reply], reply)).toEqual({
      name: "alice",
      text: "original thought",
    });
  });

  it("names the sender 'you' for the node's own quoted message", () => {
    const original = makeMessage({
      id: 1,
      direction: "out",
      peer_name: "alice",
      text: "my earlier point",
      wire_id: "a".repeat(32),
    });
    const reply = makeMessage({
      id: 2,
      text: "as I said",
      wire_id: "b".repeat(32),
      in_reply_to: { id: "a".repeat(32), excerpt: null },
    });
    expect(resolveQuote([original, reply], reply)).toEqual({
      name: "you",
      text: "my earlier point",
    });
  });

  it("is unavailable when the original never arrived", () => {
    const reply = makeMessage({
      id: 2,
      text: "reply to a ghost",
      wire_id: "b".repeat(32),
      in_reply_to: { id: "d".repeat(32), excerpt: "0123456789abcdef" },
    });
    expect(resolveQuote([reply], reply)).toBe("unavailable");
  });

  it("does not resolve a reference to another peer's message", () => {
    const otherPeer = makeMessage({
      id: 1,
      peer: "cd".repeat(16),
      peer_name: "carol",
      text: "carol's thought",
      wire_id: "a".repeat(32),
    });
    const reply = makeMessage({
      id: 2,
      text: "cross quote",
      wire_id: "b".repeat(32),
      in_reply_to: { id: "a".repeat(32), excerpt: "0123456789abcdef" },
    });
    expect(resolveQuote([otherPeer, reply], reply)).toBe("unavailable");
  });

  it("returns null for a standalone message", () => {
    const message = makeMessage();
    expect(resolveQuote([message], message)).toBeNull();
  });
});

describe("MessageFeed reply display", () => {
  it("shows the quote above the reply text", () => {
    const original = makeMessage({ id: 1, text: "original thought", wire_id: "a".repeat(32) });
    const reply = makeMessage({
      id: 2,
      text: "seconded",
      wire_id: "b".repeat(32),
      in_reply_to: { id: "a".repeat(32), excerpt: "0123456789abcdef" },
    });
    render(
      <MessageFeed messages={[original, reply]} node={null} live={false} />,
    );

    expect(screen.getByText(/↩ alice: original thought/)).toBeTruthy();
    expect(screen.getByText("seconded")).toBeTruthy();
  });

  it("reports an unavailable original without resurfacing wire text", () => {
    const reply = makeMessage({
      id: 2,
      text: "reply to a ghost",
      wire_id: "b".repeat(32),
      in_reply_to: { id: "d".repeat(32), excerpt: "0123456789abcdef" },
    });
    render(
      <MessageFeed messages={[reply]} node={null} live={false} />,
    );

    expect(screen.getByText(/original not available/)).toBeTruthy();
  });

  it("shows no quote line on a standalone message", () => {
    render(
      <MessageFeed messages={[makeMessage()]} node={null} live={false} />,
    );

    expect(screen.queryByText(/↩/)).toBeNull();
  });

  it("calls onReply with the message when the reply button is clicked", () => {
    const onReply = vi.fn();
    const message = makeMessage({ wire_id: "a".repeat(32) });
    render(
      <MessageFeed
        messages={[message]}
        node={null}
        live={false}
        onReply={onReply}
      />,
    );

    fireEvent.click(screen.getByTitle("Reply to this message"));
    expect(onReply).toHaveBeenCalledTimes(1);
    expect(onReply).toHaveBeenCalledWith(message);
  });
});
