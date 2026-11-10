import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { Peer } from "@/lib/types";
import type { PeerInfoState } from "@/lib/use-tau";

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
  infos: Record<string, PeerInfoState> = {},
  onInfo = vi.fn(),
) {
  return render(
    <PeerSidebar
      node={null}
      peers={[makePeer(), makePeer({ destination: "cd".repeat(16), name: "bob" })]}
      groups={[]}
      bridge={null}
      pings={{}}
      infos={infos}
      unread={unread}
      selected=""
      onPing={() => {}}
      onInfo={onInfo}
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

describe("PeerSidebar peer info", () => {
  const aliceHash = "ab".repeat(16);

  it("asks for info when the info button is clicked", () => {
    const onInfo = vi.fn();
    renderSidebar({}, vi.fn(), {}, onInfo);

    fireEvent.click(screen.getByLabelText("Show info for alice"));
    expect(onInfo).toHaveBeenCalledWith(aliceHash);
  });

  it("shows the reported version, protocol, uptime, peers and LXMF state", () => {
    renderSidebar(
      {},
      vi.fn(),
      {
        [aliceHash]: {
          info: {
            version: "0.2.0",
            protocol: 1,
            uptime: 3661,
            peers: 3,
            lxmf_enabled: true,
          },
        },
      },
    );

    expect(
      screen.getByText(
        "tau 0.2.0 · RNS 0.9.3 · protocol 1 · up 1h 01m · 3 peers · lxmf on",
      ),
    ).toBeTruthy();
  });

  it("shows a pending line while asking the peer", () => {
    renderSidebar({}, vi.fn(), { [aliceHash]: { pending: true } });

    expect(screen.getByText("asking peer…")).toBeTruthy();
  });

  it("shows the error when the peer does not answer", () => {
    renderSidebar({}, vi.fn(), {
      [aliceHash]: { error: "peer did not answer" },
    });

    expect(screen.getByText("peer did not answer")).toBeTruthy();
  });

  it("says so when the peer answers with no usable info", () => {
    renderSidebar({}, vi.fn(), { [aliceHash]: { info: {} } });

    expect(screen.getByText("answered with no usable info")).toBeTruthy();
  });
});
