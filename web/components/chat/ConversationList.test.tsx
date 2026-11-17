import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import ConversationList from "./ConversationList";
import { makeMessage, makePeer } from "../test-utils";
import type { Conversation } from "@/lib/use-tau";

const PEER_HASH = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6";
const peer = makePeer({ destination: PEER_HASH, name: "Alice" });

function conv(overrides: Partial<Conversation> = {}): Conversation {
  return {
    kind: "dm",
    key: PEER_HASH,
    name: "Alice",
    peer,
    lastMessage: makeMessage({ id: 1, text: "hey there", peer: PEER_HASH }),
    unread: 0,
    muted: false,
    archived: false,
    ...overrides,
  };
}

describe("ConversationList", () => {
  it("shows unread badges and the last-message preview", () => {
    render(
      <ConversationList
        conversations={[conv({ unread: 2 })]}
        archived={[]}
        dmRequests={[]}
        onSelect={() => undefined}
        now={1700000100}
      />,
    );
    expect(screen.getByLabelText("2 unread")).toBeTruthy();
    expect(screen.getByText("hey there")).toBeTruthy();
  });

  it("marks muted conversations and hides their badge count style", () => {
    render(
      <ConversationList
        conversations={[conv({ unread: 4, muted: true })]}
        archived={[]}
        dmRequests={[]}
        onSelect={() => undefined}
        now={1700000100}
      />,
    );
    expect(screen.getByLabelText("Muted conversation")).toBeTruthy();
    expect(screen.queryByLabelText("4 unread")).toBeNull();
  });

  it("shows DM requests with accept / decline", () => {
    const onAccept = vi.fn();
    render(
      <ConversationList
        conversations={[]}
        archived={[]}
        dmRequests={[{ id: "req-1", sender: "ff".repeat(16), sender_name: "Stranger", text: "hi!" }]}
        onSelect={() => undefined}
        onAcceptRequest={onAccept}
        now={1700000100}
      />,
    );
    expect(screen.getByText("Message requests (1)")).toBeTruthy();
    fireEvent.click(screen.getByText("Accept"));
    expect(onAccept).toHaveBeenCalledWith(expect.objectContaining({ id: "req-1" }));
  });

  it("reveals archived conversations behind a toggle", () => {
    const onSelect = vi.fn();
    render(
      <ConversationList
        conversations={[]}
        archived={[conv({ key: " archived".trim(), name: "Old Pal", archived: true })]}
        dmRequests={[]}
        onSelect={onSelect}
        now={1700000100}
      />,
    );
    expect(screen.queryByText("Old Pal")).toBeNull();
    fireEvent.click(screen.getByText("Archived (1)"));
    expect(screen.getByText("Old Pal")).toBeTruthy();
    fireEvent.click(screen.getByText("Old Pal"));
    expect(onSelect).toHaveBeenCalled();
  });
});
