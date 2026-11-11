import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { NodeInfo, TauMessage } from "@/lib/types";

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


describe("MessageFeed react picker", () => {
  const NODE: NodeInfo = {
    name: "me",
    destination: "ff".repeat(16),
    protocol: 1,
    version: "0.0.0",
    rns_version: "0.0.0",
    uptime: 1,
    peers: 0,
    interfaces: [],
  };

  it("shows no react button when no handler is wired", () => {
    render(
      <MessageFeed
        messages={[makeMessage({ wire_id: "a".repeat(32) })]}
        node={NODE}
        live={true}
      />,
    );
    expect(screen.queryByLabelText("React to this message")).toBeNull();
  });

  it("shows no react button on messages without a wire id", () => {
    const onReact = vi.fn().mockResolvedValue(undefined);
    render(
      <MessageFeed
        messages={[makeMessage()]}
        node={NODE}
        live={true}
        onReact={onReact}
      />,
    );
    expect(screen.queryByLabelText("React to this message")).toBeNull();
  });

  it("opens the picker and posts the chosen reaction", async () => {
    const message = makeMessage({ wire_id: "a".repeat(32) });
    const onReact = vi.fn().mockResolvedValue(undefined);
    render(
      <MessageFeed
        messages={[message]}
        node={NODE}
        live={true}
        onReact={onReact}
      />,
    );

    fireEvent.click(screen.getByLabelText("React to this message"));
    expect(screen.getByLabelText("Choose a reaction")).toBeTruthy();
    fireEvent.click(screen.getByLabelText("React with 👍"));
    await waitFor(() =>
      expect(onReact).toHaveBeenCalledWith(message, "👍"),
    );
    await waitFor(() =>
      expect(screen.queryByLabelText("Choose a reaction")).toBeNull(),
    );
  });

  it("marks your own reaction and retracts it on a second tap", async () => {
    const message = makeMessage({
      wire_id: "a".repeat(32),
      reactions: [
        { reactor: NODE.destination, name: "me", reaction: "👍" },
      ],
    });
    const onReact = vi.fn().mockResolvedValue(undefined);
    render(
      <MessageFeed
        messages={[message]}
        node={NODE}
        live={true}
        onReact={onReact}
      />,
    );

    fireEvent.click(screen.getByLabelText("React to this message"));
    const mine = screen.getByLabelText("Remove your 👍 reaction");
    expect(mine.getAttribute("aria-pressed")).toBe("true");
    fireEvent.click(mine);
    await waitFor(() =>
      expect(onReact).toHaveBeenCalledWith(message, null),
    );
  });

  it("keeps the picker open and reports a failed reaction", async () => {
    const message = makeMessage({ wire_id: "a".repeat(32) });
    const onReact = vi.fn().mockRejectedValue(new Error("peer unreachable"));
    render(
      <MessageFeed
        messages={[message]}
        node={NODE}
        live={true}
        onReact={onReact}
      />,
    );

    fireEvent.click(screen.getByLabelText("React to this message"));
    fireEvent.click(screen.getByLabelText("React with ❤️"));
    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toContain("peer unreachable");
    expect(screen.getByLabelText("Choose a reaction")).toBeTruthy();
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

describe("MessageFeed outbound status", () => {
  it("renders a failed group leg as failed, not delivered", () => {
    const message = makeMessage({
      direction: "out",
      status: "failed",
      group: "a".repeat(32),
      group_name: "crew",
    });
    render(<MessageFeed messages={[message]} node={null} live={true} />);

    expect(screen.getByText("failed")).toBeTruthy();
    expect(screen.getByText("crew")).toBeTruthy();
  });
});
