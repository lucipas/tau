import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { Peer } from "@/lib/types";

import { PeerSidebar } from "./peer-sidebar";

function makePeer(overrides: Partial<Peer> = {}): Peer {
  return {
    destination: "ab".repeat(16),
    name: "alice",
    last_seen: Date.now() / 1000,
    age: 5,
    hops: 1,
    protocol: 1,
    ...overrides,
  };
}

function renderSidebar(
  unread: Record<string, number>,
  onSelectPeer = vi.fn(),
  overrides: {
    peers?: Peer[];
    blocks?: Record<string, { pending?: boolean; error?: string }>;
    onBlock?: (destination: string) => void;
    onUnblock?: (destination: string) => void;
  } = {},
) {
  return render(
    <PeerSidebar
      node={null}
      peers={
        overrides.peers ?? [
          makePeer(),
          makePeer({ destination: "cd".repeat(16), name: "bob" }),
        ]
      }
      groups={[]}
      bridge={null}
      pings={{}}
      blocks={overrides.blocks ?? {}}
      verifications={{}}
      unread={unread}
      selected=""
      onPing={() => {}}
      onBlock={overrides.onBlock ?? (() => {})}
      onUnblock={overrides.onUnblock ?? (() => {})}
      onVerify={() => {}}
      onShowSafetyNumber={() => {}}
      onDismissVerification={() => {}}
      onSelectPeer={onSelectPeer}
    />,
  );
}

describe("PeerSidebar unread badges", () => {
  it("shows a badge with the unread count for a peer", () => {
    renderSidebar({ ["ab".repeat(16)]: 3 });

    const badge = screen.getByTitle("3 unread messages");
    expect(badge.textContent).toBe("3");
    expect(screen.queryByTitle(/unread message/)).not.toBeNull();
  });

  it("shows no badge when a peer has nothing unread", () => {
    renderSidebar({});

    expect(screen.queryByTitle(/unread message/)).toBeNull();
  });

  it("reports a peer selection to the parent, which marks it read", () => {
    const onSelectPeer = vi.fn();
    renderSidebar({ ["ab".repeat(16)]: 2 }, onSelectPeer);

    expect(screen.getByTitle("2 unread messages")).toBeTruthy();
    fireEvent.click(screen.getByTitle(/Message alice/));
    expect(onSelectPeer).toHaveBeenCalledWith("ab".repeat(16));
  });

  it("caps a large unread count at 99+", () => {
    renderSidebar({ ["ab".repeat(16)]: 150 });

    expect(screen.getByTitle("150 unread messages").textContent).toBe("99+");
  });
});

describe("PeerSidebar block and unblock", () => {
  it("asks twice before blocking: arm, then confirm", () => {
    const onBlock = vi.fn();
    renderSidebar({}, vi.fn(), { onBlock });

    const button = screen.getByRole("button", { name: "Block alice" });
    fireEvent.click(button);
    expect(onBlock).not.toHaveBeenCalled();
    expect(
      screen.getByText(/click again to block alice/),
    ).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "Confirm blocking alice" }));
    expect(onBlock).toHaveBeenCalledWith("ab".repeat(16));
  });

  it("unblocks a blocked peer with one click and shows the badge", () => {
    const onUnblock = vi.fn();
    renderSidebar(
      {},
      vi.fn(),
      {
        peers: [makePeer({ blocked: true })],
        onUnblock,
      },
    );

    expect(screen.getByText("blocked")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Unblock alice" }));
    expect(onUnblock).toHaveBeenCalledWith("ab".repeat(16));
  });

  it("shows a block error under the peer", () => {
    renderSidebar(
      {},
      vi.fn(),
      { blocks: { ["ab".repeat(16)]: { error: "unknown peer" } } },
    );

    expect(screen.getByText("unknown peer")).toBeTruthy();
  });

  it("disables the block button while a block is pending", () => {
    renderSidebar(
      {},
      vi.fn(),
      { blocks: { ["ab".repeat(16)]: { pending: true } } },
    );

    expect(screen.getByText("blocking…")).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "Block alice" }),
    ).toHaveProperty("disabled", true);
  });
});
