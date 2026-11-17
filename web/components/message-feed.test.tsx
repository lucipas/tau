import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { NodeInfo, TauMessage } from "@/lib/types";

import { MessageFeed, resolveQuote } from "./message-feed";

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

describe("MessageFeed scheduled", () => {
  it("shows the send time for a scheduled queued message", () => {
    const message = makeMessage({
      direction: "out",
      status: "queued",
      send_after: Date.now() / 1000 + 3601,
    });
    render(<MessageFeed messages={[message]} node={null} live={true} />);
    expect(screen.getByText("sends in 1h")).toBeTruthy();
    expect(screen.queryByText("queued")).toBeNull();
  });

  it("still shows plain queued for an unscheduled queued message", () => {
    render(
      <MessageFeed
        messages={[makeMessage({ direction: "out", status: "queued" })]}
        node={null}
        live={true}
      />,
    );
    expect(screen.getByText("queued")).toBeTruthy();
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

describe("MessageFeed edit", () => {
  const sent = (overrides: Partial<TauMessage> = {}) =>
    makeMessage({
      direction: "out",
      status: "delivered",
      wire_id: "a".repeat(32),
      ...overrides,
    });

  it("shows no edit button without an onEdit handler", () => {
    render(<MessageFeed messages={[sent()]} node={null} live={true} />);
    expect(screen.queryByTitle("Edit this message")).toBeNull();
  });

  it("shows the edit button only on your own sent messages", () => {
    const onEdit = vi.fn(async () => true);
    const { rerender } = render(
      <MessageFeed
        messages={[makeMessage({ wire_id: "a".repeat(32) })]}
        node={null}
        live={true}
        onEdit={onEdit}
      />,
    );
    // A received message is the sender's to edit, not this node's.
    expect(screen.queryByTitle("Edit this message")).toBeNull();

    // File, queued, and wire-id-less sent messages cannot be
    // edited from the console either.
    rerender(
      <MessageFeed
        messages={[sent({ status: "queued" })]}
        node={null}
        live={true}
        onEdit={onEdit}
      />,
    );
    expect(screen.queryByTitle("Edit this message")).toBeNull();
    rerender(
      <MessageFeed
        messages={[sent({ wire_id: undefined })]}
        node={null}
        live={true}
        onEdit={onEdit}
      />,
    );
    expect(screen.queryByTitle("Edit this message")).toBeNull();
  });

  it("shows the edit button on your own sent group messages", async () => {
    const message = sent({ group: "ab".repeat(16), group_name: "crew" });
    const onEdit = vi.fn(async () => true);
    render(
      <MessageFeed
        messages={[message]}
        node={null}
        live={true}
        onEdit={onEdit}
      />,
    );

    fireEvent.click(screen.getByTitle("Edit this message"));
    const editor = screen.getByLabelText(
      "Edit message text",
    ) as HTMLTextAreaElement;
    expect(editor.value).toBe("hello");
    fireEvent.change(editor, { target: { value: "fixed here" } });
    fireEvent.click(screen.getByText("save"));
    expect(onEdit).toHaveBeenCalledTimes(1);
    expect(onEdit).toHaveBeenCalledWith(message, "fixed here");
    await waitFor(() =>
      expect(screen.queryByLabelText("Edit message text")).toBeNull(),
    );
  });

  it("opens an editor prefilled with the text and saves through onEdit", async () => {
    const message = sent();
    const onEdit = vi.fn(async () => true);
    render(
      <MessageFeed
        messages={[message]}
        node={null}
        live={true}
        onEdit={onEdit}
      />,
    );

    fireEvent.click(screen.getByTitle("Edit this message"));
    const editor = screen.getByLabelText(
      "Edit message text",
    ) as HTMLTextAreaElement;
    expect(editor.value).toBe("hello");
    fireEvent.change(editor, { target: { value: "fixed here" } });
    fireEvent.click(screen.getByText("save"));

    expect(onEdit).toHaveBeenCalledTimes(1);
    expect(onEdit).toHaveBeenCalledWith(message, "fixed here");
    await waitFor(() =>
      expect(screen.queryByLabelText("Edit message text")).toBeNull(),
    );
  });

  it("keeps the editor open when the peer had no copy", async () => {
    const onEdit = vi.fn(async () => false);
    render(
      <MessageFeed
        messages={[sent()]}
        node={null}
        live={true}
        onEdit={onEdit}
      />,
    );

    fireEvent.click(screen.getByTitle("Edit this message"));
    fireEvent.change(screen.getByLabelText("Edit message text"), {
      target: { value: "fixed here" },
    });
    fireEvent.click(screen.getByText("save"));

    expect(
      await screen.findByText("the peer no longer has this message"),
    ).toBeTruthy();
    expect(screen.getByLabelText("Edit message text")).toBeTruthy();
  });

  it("reports a failed edit inline and keeps the editor open", async () => {
    const onEdit = vi.fn(async () => {
      throw new Error("boom");
    });
    render(
      <MessageFeed
        messages={[sent()]}
        node={null}
        live={true}
        onEdit={onEdit}
      />,
    );

    fireEvent.click(screen.getByTitle("Edit this message"));
    fireEvent.change(screen.getByLabelText("Edit message text"), {
      target: { value: "fixed here" },
    });
    fireEvent.click(screen.getByText("save"));

    expect(await screen.findByText("boom")).toBeTruthy();
    expect(screen.getByLabelText("Edit message text")).toBeTruthy();
  });

  it("disables save when the text is unchanged or empty", () => {
    const onEdit = vi.fn(async () => true);
    render(
      <MessageFeed
        messages={[sent()]}
        node={null}
        live={true}
        onEdit={onEdit}
      />,
    );

    fireEvent.click(screen.getByTitle("Edit this message"));
    const save = screen.getByText("save") as HTMLButtonElement;
    expect(save.disabled).toBe(true);
    fireEvent.change(screen.getByLabelText("Edit message text"), {
      target: { value: "   " },
    });
    expect(save.disabled).toBe(true);
  });

  it("discards the edit on cancel", () => {
    const onEdit = vi.fn(async () => true);
    render(
      <MessageFeed
        messages={[sent()]}
        node={null}
        live={true}
        onEdit={onEdit}
      />,
    );

    fireEvent.click(screen.getByTitle("Edit this message"));
    fireEvent.change(screen.getByLabelText("Edit message text"), {
      target: { value: "fixed here" },
    });
    fireEvent.click(screen.getByText("cancel"));
    expect(onEdit).not.toHaveBeenCalled();
    expect(screen.queryByLabelText("Edit message text")).toBeNull();
    expect(screen.getByText("hello")).toBeTruthy();
  });

  it("shows an edited marker on edited messages", () => {
    const onEdit = vi.fn(async () => true);
    const { rerender } = render(
      <MessageFeed
        messages={[sent({ edited_at: 2000 })]}
        node={null}
        live={true}
        onEdit={onEdit}
      />,
    );
    expect(screen.getByText("· edited")).toBeTruthy();
    rerender(
      <MessageFeed
        messages={[sent()]}
        node={null}
        live={true}
        onEdit={onEdit}
      />,
    );
    expect(screen.queryByText("· edited")).toBeNull();
  });
});


describe("MessageFeed retract", () => {
  const sent = (overrides: Partial<TauMessage> = {}) =>
    makeMessage({
      direction: "out",
      status: "delivered",
      wire_id: "a".repeat(32),
      ...overrides,
    });

  it("shows no retract button without an onRetract handler", () => {
    render(<MessageFeed messages={[sent()]} node={null} live={true} />);
    expect(screen.queryByTitle("Retract this message")).toBeNull();
  });

  it("asks for confirmation before retracting", async () => {
    const message = sent();
    const onRetract = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[message]}
        node={null}
        live={true}
        onRetract={onRetract}
      />,
    );

    fireEvent.click(screen.getByTitle("Retract this message"));
    expect(onRetract).not.toHaveBeenCalled();
    expect(screen.getByText("confirm retract")).toBeTruthy();

    fireEvent.click(screen.getByText("confirm retract"));
    expect(onRetract).toHaveBeenCalledTimes(1);
    expect(onRetract).toHaveBeenCalledWith(message);
  });

  it("keeps the message when the confirmation is cancelled", () => {
    const onRetract = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[sent()]}
        node={null}
        live={true}
        onRetract={onRetract}
      />,
    );

    fireEvent.click(screen.getByTitle("Retract this message"));
    fireEvent.click(screen.getByText("keep"));
    expect(onRetract).not.toHaveBeenCalled();
    expect(screen.queryByText("confirm retract")).toBeNull();
    expect(screen.getByTitle("Retract this message")).toBeTruthy();
  });

  it("reports a failed retract inline and keeps the message", async () => {
    const onRetract = vi.fn(async () => {
      throw new Error("boom");
    });
    render(
      <MessageFeed
        messages={[sent()]}
        node={null}
        live={true}
        onRetract={onRetract}
      />,
    );

    fireEvent.click(screen.getByTitle("Retract this message"));
    fireEvent.click(screen.getByText("confirm retract"));
    expect(await screen.findByText("boom")).toBeTruthy();
    // The bubble stays, with the plain retract button back for a retry.
    expect(screen.getByText("hello")).toBeTruthy();
    expect(screen.getByTitle("Retract this message")).toBeTruthy();
  });

  it("shows no retract button on received messages", () => {
    const onRetract = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[makeMessage({ wire_id: "a".repeat(32) })]}
        node={null}
        live={true}
        onRetract={onRetract}
      />,
    );
    expect(screen.queryByTitle("Retract this message")).toBeNull();
  });

  it("shows a retract button on your own sent group messages", async () => {
    const message = sent({ group: "ab".repeat(16), group_name: "crew" });
    const onRetract = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[message]}
        node={null}
        live={true}
        onRetract={onRetract}
      />,
    );

    fireEvent.click(screen.getByTitle("Retract this message"));
    fireEvent.click(screen.getByText("confirm retract"));
    expect(onRetract).toHaveBeenCalledTimes(1);
    expect(onRetract).toHaveBeenCalledWith(message);
  });

  it("shows no retract button on file entries", () => {
    // The peer refuses retracts of saved files, so offering retract
    // on a sent file would always fail.
    const onRetract = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[
          sent({
            file: { name: "notes.txt", size: 10, sha256: "ab".repeat(32) },
          }),
        ]}
        node={null}
        live={true}
        onRetract={onRetract}
      />,
    );
    expect(screen.queryByTitle("Retract this message")).toBeNull();
  });

  it("shows no retract button without a wire id", () => {
    const onRetract = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[sent({ wire_id: undefined })]}
        node={null}
        live={true}
        onRetract={onRetract}
      />,
    );
    expect(screen.queryByTitle("Retract this message")).toBeNull();
  });

  it("hides the retract button while the message is still queued", () => {
    const onRetract = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[sent({ status: "queued" })]}
        node={null}
        live={true}
        onRetract={onRetract}
      />,
    );
    expect(screen.queryByTitle("Retract this message")).toBeNull();
  });
});

describe("MessageFeed forward", () => {
  const peers = [
    {
      destination: "cd".repeat(16),
      name: "bob",
      last_seen: 1000,
      age: 10,
      hops: 1,
      protocol: 1,
    },
  ];

  it("shows no forward button without an onForward handler", () => {
    render(<MessageFeed messages={[makeMessage()]} node={null} live={true} />);
    expect(screen.queryByLabelText("Forward this message")).toBeNull();
  });

  it("opens a peer picker and forwards to the picked peer", async () => {
    const message = makeMessage();
    const onForward = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[message]}
        node={null}
        peers={peers}
        live={true}
        onForward={onForward}
      />,
    );

    fireEvent.click(screen.getByLabelText("Forward this message"));
    expect(screen.getByLabelText("Forward to a peer or group")).toBeTruthy();
    fireEvent.click(screen.getByTitle("Forward to bob"));
    expect(onForward).toHaveBeenCalledTimes(1);
    expect(onForward).toHaveBeenCalledWith(message, "cd".repeat(16));
    await waitFor(() =>
      expect(
        screen.queryByLabelText("Forward to a peer or group"),
      ).toBeNull(),
    );
  });

  it("shows no forward button on group or file messages", () => {
    const onForward = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[
          makeMessage({ id: 1, group: "ab".repeat(16), group_name: "crew" }),
          makeMessage({
            id: 2,
            file: { name: "photo.jpg", size: 12, sha256: "ab".repeat(32) },
          }),
        ]}
        node={null}
        live={true}
        onForward={onForward}
      />,
    );
    expect(screen.queryByLabelText("Forward this message")).toBeNull();
  });

  it("reports a failed forward inline and keeps the picker open", async () => {
    const onForward = vi.fn(async () => {
      throw new Error("boom");
    });
    render(
      <MessageFeed
        messages={[makeMessage()]}
        node={null}
        peers={peers}
        live={true}
        onForward={onForward}
      />,
    );

    fireEvent.click(screen.getByLabelText("Forward this message"));
    fireEvent.click(screen.getByTitle("Forward to bob"));
    await waitFor(() =>
      expect(screen.getByText("forward failed: boom")).toBeTruthy(),
    );
    expect(screen.getByLabelText("Forward to a peer or group")).toBeTruthy();
  });

  it("says so when there are no known peers or groups", () => {
    const onForward = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[makeMessage()]}
        node={null}
        peers={[]}
        groups={[]}
        live={true}
        onForward={onForward}
      />,
    );
    fireEvent.click(screen.getByLabelText("Forward this message"));
    expect(screen.getByText("no known peers or groups yet")).toBeTruthy();
    expect(onForward).not.toHaveBeenCalled();
  });

  it("opens a group picker and forwards to the picked group", async () => {
    const message = makeMessage();
    const onForwardGroup = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[message]}
        node={null}
        peers={[]}
        groups={[
          {
            id: "ab".repeat(16),
            name: "weekend",
            members: [],
            created_at: 1000,
          },
        ]}
        live={true}
        onForward={vi.fn(async () => {})}
        onForwardGroup={onForwardGroup}
      />,
    );

    fireEvent.click(screen.getByLabelText("Forward this message"));
    expect(screen.getByLabelText("Forward to a peer or group")).toBeTruthy();
    fireEvent.click(screen.getByTitle("Forward to group weekend"));
    expect(onForwardGroup).toHaveBeenCalledTimes(1);
    expect(onForwardGroup).toHaveBeenCalledWith(message, "ab".repeat(16));
    await waitFor(() =>
      expect(
        screen.queryByLabelText("Forward to a peer or group"),
      ).toBeNull(),
    );
  });

  it("offers groups alongside peers in the same picker", () => {
    render(
      <MessageFeed
        messages={[makeMessage()]}
        node={null}
        peers={peers}
        groups={[
          {
            id: "ab".repeat(16),
            name: "weekend",
            members: [],
            created_at: 1000,
          },
        ]}
        live={true}
        onForward={vi.fn(async () => {})}
        onForwardGroup={vi.fn(async () => {})}
      />,
    );

    fireEvent.click(screen.getByLabelText("Forward this message"));
    expect(screen.getByTitle("Forward to bob")).toBeTruthy();
    expect(screen.getByText("#weekend")).toBeTruthy();
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

describe("resolveQuote", () => {
  it("resolves a reply through the pre-edit hash", () => {
    // The sender edited the message after the reply quoted it: the
    // entry's current hash is the new text's, but the old hash is kept.
    const edited = makeMessage({
      wire_id: "a".repeat(32),
      text: "fixed here",
      excerpt_hash: "newhash",
      excerpt_hashes: ["oldhash"],
    });
    const reply = makeMessage({
      id: 2,
      text: "noted",
      in_reply_to: { id: "a".repeat(32), excerpt: "oldhash" },
    });
    expect(resolveQuote([edited], reply)).toEqual({
      name: "alice",
      text: "fixed here",
    });
  });

  it("still reports unavailable when no kept hash matches", () => {
    const edited = makeMessage({
      wire_id: "a".repeat(32),
      text: "fixed here",
      excerpt_hash: "newhash",
      excerpt_hashes: ["oldhash"],
    });
    const reply = makeMessage({
      id: 2,
      text: "noted",
      in_reply_to: { id: "a".repeat(32), excerpt: "otherhash" },
    });
    expect(resolveQuote([edited], reply)).toBe("unavailable");
  });
});

describe("MessageFeed pins", () => {
  it("shows no pin button without an onPin handler", () => {
    render(<MessageFeed messages={[makeMessage()]} node={null} live={true} />);
    expect(screen.queryByTitle("Pin this message")).toBeNull();
  });

  it("pins an unpinned message", async () => {
    const message = makeMessage({ wire_id: "a".repeat(32) });
    const onPin = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[message]}
        node={null}
        live={true}
        onPin={onPin}
      />,
    );

    fireEvent.click(screen.getByTitle("Pin this message"));
    await waitFor(() => expect(onPin).toHaveBeenCalledTimes(1));
    expect(onPin).toHaveBeenCalledWith(message, true);
  });

  it("marks a pinned message and offers to unpin it", async () => {
    const message = makeMessage({ pinned: true, wire_id: "a".repeat(32) });
    const onPin = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[message]}
        node={null}
        live={true}
        onPin={onPin}
      />,
    );

    expect(
      screen.getByTitle("Pinned — kept easy to find, local only"),
    ).toBeTruthy();
    fireEvent.click(screen.getByTitle("Unpin this message"));
    await waitFor(() => expect(onPin).toHaveBeenCalledTimes(1));
    expect(onPin).toHaveBeenCalledWith(message, false);
  });

  it("hides the pin button while the message is still queued", () => {
    // A queued message has no stored inbox copy yet, so there is
    // nothing to pin; it can be pinned once delivered.
    const message = makeMessage({ direction: "out", status: "queued" });
    render(
      <MessageFeed
        messages={[message]}
        node={null}
        live={true}
        onPin={vi.fn(async () => {})}
      />,
    );
    expect(screen.queryByTitle("Pin this message")).toBeNull();
  });

  it("hides the pin button when the message has no stored copy", () => {
    // A console entry from before wire ids existed has no inbox record
    // to pin; the button stays hidden instead of offering a pin the
    // bridge would refuse.
    render(
      <MessageFeed
        messages={[makeMessage()]}
        node={null}
        live={true}
        onPin={vi.fn(async () => {})}
      />,
    );
    expect(screen.queryByTitle("Pin this message")).toBeNull();
  });

  it("reports a failed pin inline and keeps the message", async () => {
    const onPin = vi.fn(async () => {
      throw new Error("boom");
    });
    render(
      <MessageFeed
        messages={[makeMessage({ wire_id: "a".repeat(32) })]}
        node={null}
        live={true}
        onPin={onPin}
      />,
    );

    fireEvent.click(screen.getByTitle("Pin this message"));
    expect(await screen.findByText("pin failed: boom")).toBeTruthy();
    expect(screen.getByText("hello")).toBeTruthy();
    expect(screen.getByTitle("Pin this message")).toBeTruthy();
  });

  it("shows the pinned view with a count and a way back", () => {
    const onClearPinned = vi.fn();
    render(
      <MessageFeed
        messages={[makeMessage({ pinned: true })]}
        node={null}
        live={true}
        showingPinned={true}
        onClearPinned={onClearPinned}
      />,
    );

    expect(screen.getByText("1 pinned")).toBeTruthy();
    fireEvent.click(screen.getByText("show all"));
    expect(onClearPinned).toHaveBeenCalledTimes(1);
  });

  it("offers the pinned view from the live traffic", () => {
    const onShowPinned = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[makeMessage()]}
        node={null}
        live={true}
        onShowPinned={onShowPinned}
      />,
    );

    fireEvent.click(screen.getByText("📌 pinned"));
    expect(onShowPinned).toHaveBeenCalledTimes(1);
  });

  it("shows an empty state when nothing is pinned", () => {
    render(
      <MessageFeed
        messages={[]}
        node={null}
        live={true}
        showingPinned={true}
        onClearPinned={vi.fn()}
      />,
    );

    expect(screen.getByText("No pinned messages")).toBeTruthy();
  });
});

describe("MessageFeed peer profile", () => {
  it("shows the open peer's profile bio and links in the header", () => {
    const message = makeMessage();
    render(
      <MessageFeed
        messages={[message]}
        node={null}
        live={true}
        peer={{
          destination: "ab".repeat(16),
          name: "alice",
          last_seen: 1000,
          age: 5,
          hops: 1,
          protocol: 1,
          profile: {
            author: "ab".repeat(16),
            name: "alice",
            bio: "mesh tinkerer",
            avatar: "ab".repeat(32),
            links: ["https://example.com"],
            version: 2,
            created_at: 1000,
          },
        }}
      />,
    );

    expect(screen.getByText("alice — mesh tinkerer")).toBeTruthy();
    expect(screen.getByText("example.com")).toBeTruthy();
    expect(
      screen.getByTitle(`Avatar SHA-256 ${"ab".repeat(32)}`),
    ).toBeTruthy();
  });

  it("shows no profile strip without a peer profile", () => {
    const message = makeMessage();
    render(
      <MessageFeed
        messages={[message]}
        node={null}
        live={true}
        peer={{
          destination: "ab".repeat(16),
          name: "alice",
          last_seen: 1000,
          age: 5,
          hops: 1,
          protocol: 1,
          profile: null,
        }}
      />,
    );

    expect(screen.queryByText(/mesh tinkerer/)).toBeNull();
  });
});

describe("MessageFeed group polls", () => {
  const poll = makeMessage({
    id: 1,
    group: "g1",
    group_name: "crew",
    text: "Tea or coffee?",
    wire_id: "wire-poll",
    poll_options: ["Tea", "Coffee"],
    poll: {
      options: ["Tea", "Coffee"],
      counts: [0, 1],
      total: 1,
      closed: false,
      voters: [
        { author: "cd".repeat(16), name: "carol", option: 1, voted_at: 110 },
      ],
      my_vote: null,
    },
  });

  it("renders the options with counts and the total", () => {
    render(
      <MessageFeed
        messages={[poll]}
        node={null}
        live={true}
        onVotePoll={vi.fn()}
      />,
    );

    expect(screen.getByTitle("Vote for Tea")).toBeTruthy();
    expect(screen.getByTitle("Vote for Coffee")).toBeTruthy();
    expect(screen.getByText("0 · 0%")).toBeTruthy();
    expect(screen.getByText("1 · 100%")).toBeTruthy();
    expect(screen.getByText("1 vote")).toBeTruthy();
  });

  it("marks the viewer's vote and lets it change", () => {
    const mine = { ...poll, poll: { ...poll.poll!, my_vote: 1 } };
    render(
      <MessageFeed
        messages={[mine]}
        node={null}
        live={true}
        onVotePoll={vi.fn()}
      />,
    );

    expect(
      screen.getByTitle("Your vote — pick another option to change it"),
    ).toBeTruthy();
    expect(screen.getByText("1 vote · you voted")).toBeTruthy();
  });

  it("votes for the clicked option", () => {
    const onVotePoll = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[poll]}
        node={null}
        live={true}
        onVotePoll={onVotePoll}
      />,
    );

    fireEvent.click(screen.getByTitle("Vote for Tea"));
    expect(onVotePoll).toHaveBeenCalledTimes(1);
    expect(onVotePoll).toHaveBeenCalledWith(poll, 0);
  });

  it("disables voting when no vote handler is wired", () => {
    render(<MessageFeed messages={[poll]} node={null} live={true} />);

    expect(
      screen.getByTitle("Vote for Tea") as HTMLButtonElement,
    ).toHaveProperty("disabled", true);
  });

  it("hides vote and close entries from the feed", () => {
    const vote = makeMessage({
      id: 2,
      group: "g1",
      group_name: "crew",
      text: "voted: Coffee",
      wire_id: "wire-vote",
      poll_vote: {
        target: { author: "ab".repeat(16), sent_at: 100 },
        option: 1,
      },
    });
    const close = makeMessage({
      id: 3,
      group: "g1",
      group_name: "crew",
      text: "closed the poll",
      wire_id: "wire-close",
      poll_close: { author: "ab".repeat(16), sent_at: 100 },
    });
    render(
      <MessageFeed
        messages={[poll, vote, close]}
        node={null}
        live={true}
        onVotePoll={vi.fn()}
      />,
    );

    expect(screen.getByText("Tea or coffee?")).toBeTruthy();
    expect(screen.queryByText("voted: Coffee")).toBeNull();
    expect(screen.queryByText("closed the poll")).toBeNull();
  });

  it("shows the tally without voting on a closed poll", () => {
    const closed = { ...poll, poll: { ...poll.poll!, closed: true } };
    render(
      <MessageFeed
        messages={[closed]}
        node={null}
        live={true}
        onVotePoll={vi.fn()}
      />,
    );

    // Every option button carries the closed title, so one per option.
    expect(screen.getAllByTitle("This poll is closed")).toHaveLength(2);
    expect(screen.getByText("1 vote · closed")).toBeTruthy();
  });

  it("offers close on the creator's own open poll", () => {
    const own = { ...poll, direction: "out" as const };
    const onClosePoll = vi.fn(async () => {});
    render(
      <MessageFeed
        messages={[own]}
        node={null}
        live={true}
        onVotePoll={vi.fn()}
        onClosePoll={onClosePoll}
      />,
    );

    fireEvent.click(screen.getByText("close poll"));
    expect(onClosePoll).toHaveBeenCalledTimes(1);
    expect(onClosePoll).toHaveBeenCalledWith(own);
  });

  it("offers no close button on someone else's poll", () => {
    render(
      <MessageFeed
        messages={[poll]}
        node={null}
        live={true}
        onVotePoll={vi.fn()}
        onClosePoll={vi.fn()}
      />,
    );

    expect(screen.queryByText("close poll")).toBeNull();
  });
});

describe("MessageFeed location", () => {
  it("renders a pin with an open-in-maps link", () => {
    const message = makeMessage({
      text: "meet here",
      location: { lat: 34.05, lon: -83.88, name: "Rock Springs Park" },
    });
    render(<MessageFeed messages={[message]} node={null} live={true} />);

    expect(screen.getByText("meet here")).toBeTruthy();
    expect(screen.getByText("Rock Springs Park")).toBeTruthy();
    expect(screen.getByText("34.05, -83.88")).toBeTruthy();
    const link = screen.getByText("Open in Maps");
    expect(link.getAttribute("href")).toBe(
      "https://www.openstreetmap.org/?mlat=34.05&mlon=-83.88#map=15/34.05/-83.88",
    );
  });

  it("labels a nameless pin Location", () => {
    const message = makeMessage({
      text: "📍 34.05, -83.88",
      location: { lat: 34.05, lon: -83.88 },
    });
    render(<MessageFeed messages={[message]} node={null} live={true} />);

    expect(screen.getByText("Location")).toBeTruthy();
    expect(screen.getByText("Open in Maps")).toBeTruthy();
  });

  it("renders nothing extra when a message has no pin", () => {
    render(
      <MessageFeed messages={[makeMessage()]} node={null} live={true} />,
    );
    expect(screen.queryByText("Open in Maps")).toBeNull();
  });
});

describe("MessageFeed mobile layout", () => {
  it("sizes the feed to its content on narrow screens and only stretches on lg", () => {
    const { container } = render(
      <MessageFeed messages={[makeMessage()]} node={null} live={true} />,
    );
    const section = container.querySelector("section");
    expect(section).not.toBeNull();
    // The section used to be `flex-1` at every width, which collapsed it
    // to ~0px tall inside the auto-sized mobile grid row while its
    // content spilled out over the composer.
    expect(section!.classList.contains("lg:flex-1")).toBe(true);
    expect(section!.classList.contains("flex-1")).toBe(false);
    // Same for `min-h-0`: at mobile widths it let the section shrink
    // to its borders inside the 0px grid row.
    expect(section!.classList.contains("lg:min-h-0")).toBe(true);
    expect(section!.classList.contains("min-h-0")).toBe(false);
  });

  it("bounds the message list height on narrow screens instead of flexing to zero", () => {
    render(
      <MessageFeed messages={[makeMessage()]} node={null} live={true} />,
    );
    const scroll = screen.getByLabelText("Message traffic");
    expect(scroll.classList.contains("max-h-[62dvh]")).toBe(true);
    expect(scroll.classList.contains("lg:flex-1")).toBe(true);
    expect(scroll.classList.contains("flex-1")).toBe(false);
  });

  it("wraps the per-message action row instead of overflowing the bubble", () => {
    render(
      <MessageFeed
        messages={[makeMessage({ wire_id: "wire-1" })]}
        node={null}
        live={true}
        onReply={vi.fn()}
        onDelete={async () => {}}
        onReact={async () => {}}
        onPin={async () => {}}
        onForward={async () => {}}
      />,
    );
    const reply = screen.getByTitle("Reply to this message");
    const row = reply.closest("div");
    expect(row).not.toBeNull();
    expect(row!.classList.contains("flex-wrap")).toBe(true);
  });

  it("right-aligns the traffic header actions when the header wraps", () => {
    render(
      <MessageFeed
        messages={[makeMessage()]}
        node={null}
        live={true}
        onShowPinned={async () => {}}
      />,
    );
    // The header is a wrapping justify-between row: at 360px (and in
    // the search state at 390px) the pinned/streaming cluster drops to
    // its own line, where it used to sit stranded at the left edge.
    const pinned = screen.getByText("📌 pinned");
    const actions = pinned.closest("div");
    expect(actions).not.toBeNull();
    expect(actions!.classList.contains("ml-auto")).toBe(true);
  });
});
