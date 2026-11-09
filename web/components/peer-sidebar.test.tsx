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

function renderSidebar(unread: Record<string, number>, onSelectPeer = vi.fn()) {
  return render(
    <PeerSidebar
      node={null}
      peers={[makePeer(), makePeer({ destination: "cd".repeat(16), name: "bob" })]}
      bridge={null}
      pings={{}}
      verifications={{}}
      unread={unread}
      selected=""
      onPing={() => {}}
      onVerify={() => {}}
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
