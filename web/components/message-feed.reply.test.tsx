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
    const original = makeMessage({
      id: 1,
      text: "original thought",
      wire_id: "a".repeat(32),
      excerpt_hash: "0123456789abcdef",
    });
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

  it("is unavailable when the excerpt hash does not match", () => {
    const original = makeMessage({
      id: 1,
      text: "edited after the fact",
      wire_id: "a".repeat(32),
      excerpt_hash: "fedcba9876543210",
    });
    const reply = makeMessage({
      id: 2,
      text: "seconded",
      wire_id: "b".repeat(32),
      in_reply_to: { id: "a".repeat(32), excerpt: "0123456789abcdef" },
    });
    expect(resolveQuote([original, reply], reply)).toBe("unavailable");
  });

  it("picks the hash-matching entry when a wire id is shared", () => {
    const stale = makeMessage({
      id: 1,
      text: "stale copy",
      wire_id: "a".repeat(32),
      excerpt_hash: "fedcba9876543210",
    });
    const current = makeMessage({
      id: 2,
      text: "current copy",
      wire_id: "a".repeat(32),
      excerpt_hash: "0123456789abcdef",
    });
    const reply = makeMessage({
      id: 3,
      text: "seconded",
      wire_id: "b".repeat(32),
      in_reply_to: { id: "a".repeat(32), excerpt: "0123456789abcdef" },
    });
    expect(resolveQuote([stale, current, reply], reply)).toEqual({
      name: "alice",
      text: "current copy",
    });
  });

  it("still renders history written before excerpt hashes existed", () => {
    const original = makeMessage({
      id: 1,
      text: "original thought",
      wire_id: "a".repeat(32),
    });
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

  it("resolves a group quote by its logical reference, not the leg id", () => {
    const GROUP = "cd".repeat(16);
    const AUTHOR = "aa".repeat(16);
    // This console holds its own leg of the quoted send (wire id
    // "c…"); the reply names the sender's leg ("a…"), which this
    // console never held — the wire id alone cannot resolve it.
    const original = makeMessage({
      id: 1,
      peer: AUTHOR,
      peer_name: "alice",
      text: "original thought",
      group: GROUP,
      sent_at: 1000,
      wire_id: "c".repeat(32),
      excerpt_hash: "0123456789abcdef",
    });
    const reply = makeMessage({
      id: 2,
      peer: "bb".repeat(16),
      peer_name: "bob",
      text: "seconded",
      group: GROUP,
      sent_at: 1002,
      wire_id: "d".repeat(32),
      in_reply_to: {
        id: "a".repeat(32),
        excerpt: "0123456789abcdef",
        group: GROUP,
        author: AUTHOR,
        sent_at: 1000,
      },
    });
    expect(resolveQuote([original, reply], reply)).toEqual({
      name: "alice",
      text: "original thought",
    });
  });

  it("does not resolve a group reference carried by a DM", () => {
    const GROUP = "cd".repeat(16);
    const original = makeMessage({
      id: 1,
      peer: "aa".repeat(16),
      peer_name: "alice",
      text: "original thought",
      group: GROUP,
      sent_at: 1000,
      wire_id: "c".repeat(32),
      excerpt_hash: "0123456789abcdef",
    });
    // No group on the reply: it is a DM, so a group reference it
    // carries — a remote peer can send one — must not render that
    // group's text under it.
    const reply = makeMessage({
      id: 2,
      text: "seconded",
      wire_id: "d".repeat(32),
      in_reply_to: {
        id: "a".repeat(32),
        excerpt: "0123456789abcdef",
        group: GROUP,
        author: "aa".repeat(16),
        sent_at: 1000,
      },
    });
    expect(resolveQuote([original, reply], reply)).toBe("unavailable");
  });

  it("does not resolve a group reference from another group", () => {
    const GROUP = "cd".repeat(16);
    const original = makeMessage({
      id: 1,
      peer: "aa".repeat(16),
      peer_name: "alice",
      text: "original thought",
      group: GROUP,
      sent_at: 1000,
      wire_id: "c".repeat(32),
      excerpt_hash: "0123456789abcdef",
    });
    const reply = makeMessage({
      id: 2,
      peer: "bb".repeat(16),
      peer_name: "bob",
      text: "seconded",
      group: "ef".repeat(16),
      sent_at: 1002,
      wire_id: "d".repeat(32),
      in_reply_to: {
        id: "a".repeat(32),
        excerpt: "0123456789abcdef",
        group: GROUP,
        author: "aa".repeat(16),
        sent_at: 1000,
      },
    });
    expect(resolveQuote([original, reply], reply)).toBe("unavailable");
  });

  it("does not resolve a group quote whose logical reference names another author", () => {
    const GROUP = "cd".repeat(16);
    const original = makeMessage({
      id: 1,
      peer: "aa".repeat(16),
      peer_name: "alice",
      text: "original thought",
      group: GROUP,
      sent_at: 1000,
      wire_id: "c".repeat(32),
      excerpt_hash: "0123456789abcdef",
    });
    const reply = makeMessage({
      id: 2,
      peer: "bb".repeat(16),
      peer_name: "bob",
      text: "seconded",
      group: GROUP,
      sent_at: 1002,
      wire_id: "d".repeat(32),
      in_reply_to: {
        id: "a".repeat(32),
        excerpt: "0123456789abcdef",
        group: GROUP,
        author: "ee".repeat(16),
        sent_at: 1000,
      },
    });
    expect(resolveQuote([original, reply], reply)).toBe("unavailable");
  });

  it("resolves a group quote of this node's own sent message", () => {
    const GROUP = "cd".repeat(16);
    const OWN = "ff".repeat(16);
    // The quoted send is an outbound leg: its peer is the member it
    // went to, so only the node's own hash names its author.
    const original = makeMessage({
      id: 1,
      direction: "out",
      peer: "bb".repeat(16),
      peer_name: "bob",
      text: "my earlier point",
      group: GROUP,
      sent_at: 1000,
      wire_id: "c".repeat(32),
      excerpt_hash: "0123456789abcdef",
    });
    const reply = makeMessage({
      id: 2,
      peer: "bb".repeat(16),
      peer_name: "bob",
      text: "as you said",
      group: GROUP,
      sent_at: 1002,
      wire_id: "d".repeat(32),
      in_reply_to: {
        id: "a".repeat(32),
        excerpt: "0123456789abcdef",
        group: GROUP,
        author: OWN,
        sent_at: 1000,
      },
    });
    expect(resolveQuote([original, reply], reply, OWN)).toEqual({
      name: "you",
      text: "my earlier point",
    });
  });

  it("falls back to the wire id when the logical reference matches nothing", () => {
    // A reply from a sender that predates the reference, quoting
    // its own message: the triple names a send time this console
    // never held, but the wire id still names the sender's copy.
    const original = makeMessage({
      id: 1,
      text: "original thought",
      wire_id: "a".repeat(32),
      excerpt_hash: "0123456789abcdef",
    });
    const reply = makeMessage({
      id: 2,
      text: "seconded",
      wire_id: "b".repeat(32),
      in_reply_to: {
        id: "a".repeat(32),
        excerpt: "0123456789abcdef",
        group: "cd".repeat(16),
        author: "aa".repeat(16),
        sent_at: 9999,
      },
    });
    expect(resolveQuote([original, reply], reply)).toEqual({
      name: "alice",
      text: "original thought",
    });
  });

  it("resolves a group quote across legs of the same group", () => {
    const groupId = "e".repeat(32);
    // The quoted message arrived from alice; the reply is an outbound
    // leg to bob, so the two entries name different peers — only the
    // group scopes the quote.
    const original = makeMessage({
      id: 1,
      group: groupId,
      group_name: "crew",
      text: "original thought",
      wire_id: "a".repeat(32),
      excerpt_hash: "0123456789abcdef",
    });
    const reply = makeMessage({
      id: 2,
      direction: "out",
      peer: "cd".repeat(16),
      peer_name: "bob",
      group: groupId,
      group_name: "crew",
      text: "seconded",
      wire_id: "b".repeat(32),
      in_reply_to: { id: "a".repeat(32), excerpt: "0123456789abcdef" },
    });
    expect(resolveQuote([original, reply], reply)).toEqual({
      name: "alice",
      text: "original thought",
    });
  });

  it("does not resolve a group quote from another group", () => {
    const original = makeMessage({
      id: 1,
      group: "e".repeat(32),
      group_name: "crew",
      text: "original thought",
      wire_id: "a".repeat(32),
      excerpt_hash: "0123456789abcdef",
    });
    const reply = makeMessage({
      id: 2,
      group: "f".repeat(32),
      group_name: "other",
      text: "cross quote",
      wire_id: "b".repeat(32),
      in_reply_to: { id: "a".repeat(32), excerpt: "0123456789abcdef" },
    });
    expect(resolveQuote([original, reply], reply)).toBe("unavailable");
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
