import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { Peer, TauGroup } from "@/lib/types";

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

function makeGroup(overrides: Partial<TauGroup> = {}): TauGroup {
  return {
    id: "11".repeat(16),
    name: "weekend",
    members: [
      { hash: "ab".repeat(16), name: "alice" },
      { hash: "cd".repeat(16), name: "bob" },
    ],
    created_at: 1000,
    ...overrides,
  };
}

function renderSidebar(
  unread: Record<string, number>,
  onSelectPeer = vi.fn(),
  options: {
    groups?: TauGroup[];
    onCreateGroup?: (name: string, members: string[]) => Promise<TauGroup>;
    onDeleteGroup?: (group: string) => Promise<{ id: string; name: string }>;
  } = {},
) {
  return render(
    <PeerSidebar
      node={null}
      peers={[makePeer(), makePeer({ destination: "cd".repeat(16), name: "bob" })]}
      groups={options.groups ?? []}
      bridge={null}
      pings={{}}
      unread={unread}
      selected=""
      onPing={() => {}}
      onSelectPeer={onSelectPeer}
      onCreateGroup={options.onCreateGroup}
      onDeleteGroup={options.onDeleteGroup}
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

describe("PeerSidebar group management", () => {
  it("creates a group from the name and picked members", async () => {
    const onCreateGroup = vi.fn(async (name: string, members: string[]) =>
      makeGroup({ name, members: members.map((hash) => ({ hash, name })) }),
    );
    renderSidebar({}, vi.fn(), { onCreateGroup });

    fireEvent.click(screen.getByRole("button", { name: "+ new group" }));
    fireEvent.change(screen.getByLabelText("Group name"), {
      target: { value: "weekend" },
    });
    // The member picker lists each known peer once.
    fireEvent.click(screen.getByRole("checkbox", { name: /alice/ }));

    const create = screen.getByRole("button", { name: "create group" });
    expect((create as HTMLButtonElement).disabled).toBe(false);
    fireEvent.click(create);

    expect(onCreateGroup).toHaveBeenCalledWith("weekend", ["ab".repeat(16)]);
    await screen.findByRole("button", { name: "+ new group" });
  });

  it("keeps create disabled until a member is picked", () => {
    const onCreateGroup = vi.fn(async () => makeGroup());
    renderSidebar({}, vi.fn(), { onCreateGroup });

    fireEvent.click(screen.getByRole("button", { name: "+ new group" }));
    fireEvent.change(screen.getByLabelText("Group name"), {
      target: { value: "weekend" },
    });

    expect(
      (
        screen.getByRole("button", { name: "create group" }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
    expect(onCreateGroup).not.toHaveBeenCalled();
  });

  it("shows a create error instead of closing the form", async () => {
    const onCreateGroup = vi.fn(async () => {
      throw new Error("group limit reached");
    });
    renderSidebar({}, vi.fn(), { onCreateGroup });

    fireEvent.click(screen.getByRole("button", { name: "+ new group" }));
    fireEvent.change(screen.getByLabelText("Group name"), {
      target: { value: "weekend" },
    });
    fireEvent.click(screen.getByRole("checkbox", { name: /alice/ }));
    fireEvent.click(screen.getByRole("button", { name: "create group" }));

    await screen.findByText("group limit reached");
    expect(screen.getByLabelText("Group name")).toBeTruthy();
  });

  it("asks for confirmation before deleting a group", async () => {
    const onDeleteGroup = vi.fn(async (group: string) => ({
      id: group,
      name: "weekend",
    }));
    renderSidebar({}, vi.fn(), {
      groups: [makeGroup()],
      onDeleteGroup,
    });

    fireEvent.click(screen.getByRole("button", { name: "delete" }));
    expect(onDeleteGroup).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "confirm delete" }));
    expect(onDeleteGroup).toHaveBeenCalledWith("11".repeat(16));
    await screen.findByRole("button", { name: "delete" });
  });

  it("hides group management when no handlers are passed", () => {
    renderSidebar({}, vi.fn(), { groups: [makeGroup()] });

    expect(screen.queryByRole("button", { name: "+ new group" })).toBeNull();
    expect(screen.queryByRole("button", { name: "delete" })).toBeNull();
  });
});
