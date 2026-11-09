import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { Peer, VerifyState } from "@/lib/types";

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

function renderSidebar({
  peers = [makePeer()],
  verifications = {},
  onVerify = vi.fn(),
}: {
  peers?: Peer[];
  verifications?: Record<string, VerifyState>;
  onVerify?: (destination: string) => void;
} = {}) {
  return render(
    <PeerSidebar
      node={null}
      peers={peers}
      bridge={null}
      pings={{}}
      verifications={verifications}
      unread={{}}
      selected=""
      onPing={() => {}}
      onVerify={onVerify}
      onSelectPeer={() => {}}
    />,
  );
}

describe("PeerSidebar peer verification", () => {
  it("offers a Verify action for each peer", () => {
    renderSidebar();

    expect(
      screen.getByRole("button", { name: "Verify alice" }),
    ).toBeTruthy();
  });

  it("calls onVerify with the peer destination", () => {
    const onVerify = vi.fn();
    renderSidebar({ onVerify });

    fireEvent.click(screen.getByRole("button", { name: "Verify alice" }));

    expect(onVerify).toHaveBeenCalledWith("ab".repeat(16));
  });

  it("shows the safety number once a peer is verified", () => {
    const number = "12345 67890 12345 67890 12345 67890";
    renderSidebar({
      verifications: {
        ["ab".repeat(16)]: {
          result: {
            peer: "ab".repeat(16),
            name: "alice",
            safety_number: number,
            verified_at: 1500,
            reverified: false,
            fingerprint_changed: false,
          },
        },
      },
    });

    expect(screen.getByText(number)).toBeTruthy();
    expect(
      screen.getByText(/compare it with alice over another channel/),
    ).toBeTruthy();
  });

  it("flags a changed number after re-verification", () => {
    renderSidebar({
      verifications: {
        ["ab".repeat(16)]: {
          result: {
            peer: "ab".repeat(16),
            name: "alice",
            safety_number: "12345 67890",
            verified_at: 1500,
            reverified: true,
            fingerprint_changed: true,
          },
        },
      },
    });

    expect(
      screen.getByText(/number changed since the last verification/),
    ).toBeTruthy();
  });

  it("shows a verification failure under the peer", () => {
    renderSidebar({
      verifications: {
        ["ab".repeat(16)]: { error: "no peer matches 'alice'" },
      },
    });

    expect(screen.getByText("no peer matches 'alice'")).toBeTruthy();
  });

  it("disables the button while a verification is pending", () => {
    renderSidebar({
      verifications: { ["ab".repeat(16)]: { pending: true } },
    });

    expect(
      screen.getByRole("button", { name: "Verify alice" }),
    ).toHaveProperty("disabled", true);
  });

  it("keeps the key-change warning next to the Verify action", () => {
    renderSidebar({ peers: [makePeer({ key_change_at: 1500 })] });

    expect(
      screen.getByText(/key changed — re-verify before trusting/),
    ).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "Verify alice" }),
    ).toBeTruthy();
  });
});
