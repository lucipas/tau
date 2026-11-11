import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { NodeInfo, Peer, SocialInfo, TauGroup } from "@/lib/types";

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
    peers?: Peer[];
    onSetAlias?: (destination: string, alias: string | null) => Promise<Peer>;
    groups?: TauGroup[];
    onCreateGroup?: (name: string, members: string[]) => Promise<TauGroup>;
    onDeleteGroup?: (group: string) => Promise<{ id: string; name: string }>;
    node?: NodeInfo | null;
    social?: SocialInfo | null;
    blocks?: Record<string, { pending?: boolean; error?: string }>;
    onBlock?: (destination: string) => void;
    onUnblock?: (destination: string) => void;
  } = {},
) {
  const onSetAlias =
    options.onSetAlias ??
    (async (destination: string) => makePeer({ destination }));
  return render(
    <PeerSidebar
      node={options.node ?? null}
      peers={
        options.peers ?? [
          makePeer(),
          makePeer({ destination: "cd".repeat(16), name: "bob" }),
        ]
      }
      groups={options.groups ?? []}
      bridge={null}
      social={options.social ?? null}
      pings={{}}
      blocks={options.blocks ?? {}}
      verifications={{}}
      unread={unread}
      selected=""
      onPing={() => {}}
      onBlock={options.onBlock ?? (() => {})}
      onUnblock={options.onUnblock ?? (() => {})}
      onVerify={() => {}}
      onShowSafetyNumber={() => {}}
      onDismissVerification={() => {}}
      onSelectPeer={onSelectPeer}
      onSetAlias={onSetAlias}
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
