import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ApiError } from "@/lib/api";
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
  onForgetPeer = vi.fn(),
) {
  return render(
    <PeerSidebar
      node={null}
      peers={[makePeer(), makePeer({ destination: "cd".repeat(16), name: "bob" })]}
      groups={[]}
      bridge={null}
      pings={{}}
      unread={unread}
      selected=""
      onPing={() => {}}
      onForgetPeer={onForgetPeer}
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

describe("PeerSidebar forget", () => {
  it("asks for confirmation before forgetting a peer", async () => {
    const onForgetPeer = vi.fn().mockResolvedValue(undefined);
    renderSidebar({}, vi.fn(), onForgetPeer);

    const forget = screen.getByRole("button", { name: "Forget alice" });
    fireEvent.click(forget);
    expect(onForgetPeer).not.toHaveBeenCalled();
    expect(
      screen.getByRole("button", { name: "Forget alice" }).textContent,
    ).toBe("confirm forget");

    fireEvent.click(screen.getByRole("button", { name: "Forget alice" }));
    expect(onForgetPeer).toHaveBeenCalledWith("ab".repeat(16), false);
  });

  it("offers to forget anyway when mail is still queued", async () => {
    const onForgetPeer = vi
      .fn()
      .mockRejectedValueOnce(
        new ApiError("alice still has 2 queued message(s)", 400, "queued_mail", 2),
      )
      .mockResolvedValue(undefined);
    renderSidebar({}, vi.fn(), onForgetPeer);

    fireEvent.click(screen.getByRole("button", { name: "Forget alice" }));
    fireEvent.click(screen.getByRole("button", { name: "Forget alice" }));

    expect(
      await screen.findByText(/still has 2 queued message/),
    ).toBeTruthy();
    const anyway = screen.getByRole("button", {
      name: /forget anyway — strand 2 queued messages/,
    });
    fireEvent.click(anyway);
    expect(onForgetPeer).toHaveBeenCalledTimes(2);
    expect(onForgetPeer).toHaveBeenLastCalledWith("ab".repeat(16), true);
  });

  it("shows a plain error with no force option for other refusals", async () => {
    const onForgetPeer = vi
      .fn()
      .mockRejectedValueOnce(
        new ApiError("alice still has mail acknowledgements owed", 400),
      );
    renderSidebar({}, vi.fn(), onForgetPeer);

    fireEvent.click(screen.getByRole("button", { name: "Forget alice" }));
    fireEvent.click(screen.getByRole("button", { name: "Forget alice" }));

    expect(
      await screen.findByText(/acknowledgements owed/),
    ).toBeTruthy();
    expect(
      screen.queryByRole("button", { name: /forget anyway/ }),
    ).toBeNull();
  });
});
