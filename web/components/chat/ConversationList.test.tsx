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
    ignored: false,
    pinned: false,
    ...overrides,
  };
}

describe("ConversationList", () => {
  it("shows unread badges and the last-message preview", () => {
    render(
      <ConversationList
        conversations={[conv({ unread: 2 })]}
        ignoredGroups={[]}
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
        ignoredGroups={[]}
        archived={[]}
        dmRequests={[]}
        onSelect={() => undefined}
        now={1700000100}
      />,
    );
    expect(screen.getByLabelText("Muted conversation")).toBeTruthy();
    expect(screen.queryByLabelText("4 unread")).toBeNull();
  });

  it("marks pinned conversations with a pin indicator", () => {
    render(
      <ConversationList
        conversations={[conv({ pinned: true }), conv({ key: "b".repeat(32), name: "Bob", pinned: false })]}
        ignoredGroups={[]}
        archived={[]}
        dmRequests={[]}
        onSelect={() => undefined}
        now={1700000100}
      />,
    );
    expect(screen.getByLabelText("Pinned conversation")).toBeTruthy();
    expect(screen.getByText("Bob")).toBeTruthy();
  });

  it("shows DM requests with accept / decline", () => {
    const onAccept = vi.fn();
    render(
      <ConversationList
        conversations={[]}
        ignoredGroups={[]}
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
        ignoredGroups={[]}
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

describe("ConversationList ignored groups", () => {
  const ignored = [
    { id: "g".repeat(32), name: "noisy crew", openable: true },
    { id: "h".repeat(32), name: "hhhhhhhh", openable: false },
  ];

  function renderList(props: Record<string, unknown> = {}) {
    return render(
      <ConversationList
        conversations={[]}
        ignoredGroups={ignored}
        archived={[]}
        dmRequests={[]}
        onSelect={() => undefined}
        now={1700000100}
        {...props}
      />,
    );
  }

  it("hides ignored groups behind a toggle", () => {
    renderList();
    expect(screen.queryByText("noisy crew")).toBeNull();
    fireEvent.click(screen.getByText("Ignored groups (2)"));
    expect(screen.getByText("noisy crew")).toBeTruthy();
    expect(screen.getByText("hhhhhhhh")).toBeTruthy();
  });

  it("un-ignores from the row button", () => {
    const onUnignoreGroup = vi.fn();
    renderList({ onUnignoreGroup });
    fireEvent.click(screen.getByText("Ignored groups (2)"));
    fireEvent.click(screen.getByLabelText("Un-ignore noisy crew"));
    expect(onUnignoreGroup).toHaveBeenCalledWith(
      expect.objectContaining({ id: "g".repeat(32) }),
    );
  });

  it("opens an openable ignored group on row click, but not an orphan", () => {
    const onSelectIgnoredGroup = vi.fn();
    renderList({ onSelectIgnoredGroup });
    fireEvent.click(screen.getByText("Ignored groups (2)"));
    fireEvent.click(screen.getByText("noisy crew"));
    expect(onSelectIgnoredGroup).toHaveBeenCalledWith(
      expect.objectContaining({ id: "g".repeat(32) }),
    );
    fireEvent.click(screen.getByText("hhhhhhhh"));
    expect(onSelectIgnoredGroup).toHaveBeenCalledTimes(1);
  });

  it("shows no ignored section when none are ignored", () => {
    render(
      <ConversationList
        conversations={[]}
        ignoredGroups={[]}
        archived={[]}
        dmRequests={[]}
        onSelect={() => undefined}
        now={1700000100}
      />,
    );
    expect(screen.queryByText(/Ignored groups/)).toBeNull();
  });
});
