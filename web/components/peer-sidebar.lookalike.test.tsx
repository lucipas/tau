import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

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

function renderSidebar({ peers = [makePeer()] }: { peers?: Peer[] } = {}) {
  return render(
    <PeerSidebar
      node={null}
      peers={peers}
      groups={[]}
      leftGroups={[]}
      ignoredGroups={[]}
      bridge={null}
      social={null}
      pings={{}}
      infos={{}}
      blocks={{}}
      verifications={{}}
      unread={{}}
      archived={[]}
      archivedError={null}
      muted={{ peers: [], groups: [] }}
      mutedError={null}
      selected=""
      onPing={() => {}}
      onInfo={() => {}}
      onForgetPeer={async () => {}}
      onBlock={() => {}}
      onUnblock={() => {}}
      onArchivePeer={async () => {}}
      onUnarchivePeer={async () => {}}
      onVerify={() => {}}
      onShowSafetyNumber={() => {}}
      onDismissVerification={() => {}}
      onSelectPeer={() => {}}
    />,
  );
}

describe("PeerSidebar impersonation guard", () => {
  it("flags an unverified lookalike with the peer it resembles", () => {
    renderSidebar({
      peers: [
        makePeer({
          destination: "cd".repeat(16),
          name: "Alice",
          lookalike_of: "alice",
        }),
      ],
    });

    expect(screen.getByText("⚠ lookalike of alice")).toBeTruthy();
  });

  it("flags a verified peer that changed its announced name", () => {
    renderSidebar({
      peers: [
        makePeer({
          verified: true,
          verified_at: 1500,
          name: "alicia",
          name_change_at: 2000,
          previous_name: "alice",
        }),
      ],
    });

    expect(screen.getByText("⚠ renamed")).toBeTruthy();
  });

  it("shows no warning for an ordinary unverified peer", () => {
    renderSidebar({
      peers: [makePeer({ lookalike_of: null, name_change_at: null })],
    });

    expect(screen.queryByText(/lookalike/)).toBeNull();
    expect(screen.queryByText(/renamed/)).toBeNull();
  });
});
