import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { NodeInfo, Peer, SocialInfo } from "@/lib/types";

import { PeerSidebar } from "./peer-sidebar";

function makeNode(overrides: Partial<NodeInfo> = {}): NodeInfo {
  return {
    name: "mynode",
    destination: "ef".repeat(16),
    protocol: 1,
    version: "0.1.0",
    rns_version: "1.0.0",
    uptime: 3600,
    peers: 2,
    interfaces: [{ name: "tcp", online: true }],
    mailbox: { outbox: 2, held: 1, seen: 5, sent: 4, receipts: 3 },
    lxmf: { enabled: true, delivery: "12".repeat(16) },
    ...overrides,
  };
}

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
  options: { node?: NodeInfo | null; social?: SocialInfo | null } = {},
) {
  return render(
    <PeerSidebar
      node={options.node ?? null}
      peers={[makePeer(), makePeer({ destination: "cd".repeat(16), name: "bob" })]}
      bridge={null}
      social={options.social ?? null}
      pings={{}}
      unread={unread}
      selected=""
      onPing={() => {}}
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

describe("PeerSidebar node stores", () => {
  it("shows mailbox queued and held counts with the rest in a tooltip", () => {
    renderSidebar({}, vi.fn(), {
      node: makeNode(),
      social: { enabled: true, posts: 7 },
    });

    expect(screen.getByText("stores")).toBeTruthy();
    const mailbox = screen.getByTitle("5 seen · 4 sent · 3 receipts");
    expect(mailbox.textContent).toBe("2 queued · 1 held");
  });

  it("shows LXMF on with the short delivery hash", () => {
    renderSidebar({}, vi.fn(), {
      node: makeNode(),
      social: { enabled: true, posts: 7 },
    });

    expect(screen.getByText("lxmf")).toBeTruthy();
    expect(screen.getByText("on · 1212121212…1212")).toBeTruthy();
  });

  it("shows how many Tan posts are cached", () => {
    renderSidebar({}, vi.fn(), {
      node: makeNode(),
      social: { enabled: true, posts: 7 },
    });

    expect(screen.getByText("7 posts cached")).toBeTruthy();
  });

  it("shows off states when LXMF and Tan are disabled", () => {
    renderSidebar({}, vi.fn(), {
      node: makeNode({ lxmf: { enabled: false, delivery: null } }),
      social: { enabled: false, posts: 0 },
    });

    const offs = screen.getAllByText("off");
    expect(offs).toHaveLength(2);
    expect(screen.queryByText(/cached/)).toBeNull();
  });

  it("hides the stores section when the snapshot carries no store data", () => {
    const { mailbox: _mailbox, lxmf: _lxmf, ...bare } = makeNode();
    void _mailbox;
    void _lxmf;
    renderSidebar({}, vi.fn(), { node: bare, social: null });

    expect(screen.queryByText("stores")).toBeNull();
    expect(screen.getByText("mynode")).toBeTruthy();
  });
});
