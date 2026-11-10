import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

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
