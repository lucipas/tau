import { fireEvent, render, screen, waitFor } from "@testing-library/react";
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
  options: {
    peers?: Peer[];
    onSetAlias?: (destination: string, alias: string | null) => Promise<Peer>;
  } = {},
) {
  const onSetAlias =
    options.onSetAlias ??
    (async (destination: string) => makePeer({ destination }));
  return render(
    <PeerSidebar
      node={null}
      peers={
        options.peers ?? [
          makePeer(),
          makePeer({ destination: "cd".repeat(16), name: "bob" }),
        ]
      }
      groups={[]}
      bridge={null}
      pings={{}}
      unread={unread}
      selected=""
      onPing={() => {}}
      onSelectPeer={onSelectPeer}
      onSetAlias={onSetAlias}
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

describe("PeerSidebar aliases", () => {
  it("shows the alias as the label and keeps the announced name visible", () => {
    renderSidebar({}, vi.fn(), {
      peers: [makePeer({ alias: "laptop" })],
    });

    expect(screen.getByText("laptop")).toBeTruthy();
    expect(screen.getByText("announced as alice")).toBeTruthy();
    expect(screen.getByTitle("Announced as alice")).toBeTruthy();
  });

  it("shows no alias badge for a peer without one", () => {
    renderSidebar({});

    expect(screen.queryByTitle(/Announced as/)).toBeNull();
  });

  it("saves a new alias through onSetAlias", async () => {
    const onSetAlias = vi.fn(async (destination: string) =>
      makePeer({ destination, alias: "laptop" }),
    );
    renderSidebar({}, vi.fn(), { onSetAlias });

    fireEvent.click(screen.getByLabelText("Set alias for alice"));
    fireEvent.change(screen.getByLabelText("Local alias for alice"), {
      target: { value: "laptop" },
    });
    fireEvent.click(screen.getByText("save"));

    await waitFor(() =>
      expect(onSetAlias).toHaveBeenCalledWith("ab".repeat(16), "laptop"),
    );
  });

  it("clears an alias through onSetAlias", async () => {
    const onSetAlias = vi.fn(async (destination: string) =>
      makePeer({ destination, alias: null }),
    );
    renderSidebar({}, vi.fn(), {
      peers: [makePeer({ alias: "laptop" })],
      onSetAlias,
    });

    fireEvent.click(screen.getByLabelText("Set alias for laptop"));
    fireEvent.click(screen.getByText("clear alias"));

    await waitFor(() =>
      expect(onSetAlias).toHaveBeenCalledWith("ab".repeat(16), null),
    );
  });

  it("shows the server's message when saving fails", async () => {
    const onSetAlias = vi.fn(async () => {
      throw new Error("alias 'laptop' is already used by another peer");
    });
    renderSidebar({}, vi.fn(), { onSetAlias });

    fireEvent.click(screen.getByLabelText("Set alias for alice"));
    fireEvent.change(screen.getByLabelText("Local alias for alice"), {
      target: { value: "laptop" },
    });
    fireEvent.click(screen.getByText("save"));

    expect(
      await screen.findByText(
        "alias 'laptop' is already used by another peer",
      ),
    ).toBeTruthy();
    expect(onSetAlias).toHaveBeenCalledWith("ab".repeat(16), "laptop");
  });
});
