import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

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

describe("MessageFeed search", () => {
  const mockSearch = vi.fn();
  const mockClearSearch = vi.fn();

  beforeEach(() => {
    vi.resetAllMocks();
  });

  function renderSearching(options: {
    messages?: TauMessage[];
    searching?: boolean;
    searchQuery?: string | null;
    searchError?: string | null;
  } = {}) {
    return render(
      <MessageFeed
        messages={options.messages ?? [makeMessage({ id: 1, text: "mesh post" })]}
        node={null}
        live={true}
        searching={options.searching}
        searchQuery={options.searchQuery}
        searchError={options.searchError}
        onSearch={mockSearch}
        onClearSearch={mockClearSearch}
      />,
    );
  }

  it("submits the query to the search handler", () => {
    renderSearching();

    fireEvent.change(screen.getByLabelText("Search stored messages"), {
      target: { value: "mesh" },
    });
    fireEvent.click(screen.getByRole("button", { name: "search" }));

    expect(mockSearch).toHaveBeenCalledTimes(1);
    expect(mockSearch).toHaveBeenCalledWith("mesh");
  });

  it("shows search results instead of the traffic while searching", () => {
    const hit = makeMessage({ id: 2, text: "mesh hit" });
    renderSearching({
      messages: [hit],
      searching: true,
      searchQuery: "mesh",
    });

    expect(screen.getByText("mesh hit")).toBeTruthy();
    expect(screen.getByText("search results")).toBeTruthy();
    expect(screen.getByText("1 match(es)")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "clear search" }));
    expect(mockClearSearch).toHaveBeenCalledTimes(1);
  });

  it("says when nothing matches the search", () => {
    renderSearching({ messages: [], searching: true, searchQuery: "zzz" });

    expect(screen.getByText("No stored messages match “zzz”")).toBeTruthy();
    expect(screen.queryByText("Nothing on the wire yet")).toBeNull();
  });

  it("shows a search failure instead of hiding it", () => {
    renderSearching({
      messages: [],
      searching: true,
      searchQuery: "mesh",
      searchError: "bridge unreachable",
    });

    expect(screen.getByText("bridge unreachable")).toBeTruthy();
  });

  it("resolves reply quotes from the live traffic while searching", () => {
    // A search hit that is a reply quotes an original that is not itself
    // a hit: the quote must resolve from the live traffic, not render
    // as unavailable.
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
    render(
      <MessageFeed
        messages={[reply]}
        node={null}
        live={true}
        searching={true}
        searchQuery="seconded"
        quoteContext={[original, reply]}
        onSearch={mockSearch}
        onClearSearch={mockClearSearch}
      />,
    );

    expect(screen.getByText(/original thought/)).toBeTruthy();
  });
});
