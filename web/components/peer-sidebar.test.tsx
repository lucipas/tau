import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ApiError } from "@/lib/api";
import type {
  ArchivedConversation,
  ChangeGroupMembersResponse,
  NodeInfo,
  Peer,
  RenameGroupResponse,
  SetGroupTopicResponse,
  SocialInfo,
  TauGroup,
} from "@/lib/types";
import type { PeerInfoState } from "@/lib/use-tau";

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


type SidebarOptions = {
  peers?: Peer[];
  onSetAlias?: (destination: string, alias: string | null) => Promise<Peer>;
  groups?: TauGroup[];
  onCreateGroup?: (name: string, members: string[]) => Promise<TauGroup>;
  onDeleteGroup?: (group: string) => Promise<{ id: string; name: string }>;
  onAddGroupMembers?: (
    group: string,
    members: string[],
  ) => Promise<ChangeGroupMembersResponse>;
  onRemoveGroupMembers?: (
    group: string,
    members: string[],
  ) => Promise<ChangeGroupMembersResponse>;
  onRenameGroup?: (
    group: string,
    name: string,
  ) => Promise<RenameGroupResponse>;
  onSetGroupTopic?: (
    group: string,
    topic: string | null,
  ) => Promise<SetGroupTopicResponse>;
  node?: NodeInfo | null;
  social?: SocialInfo | null;
  blocks?: Record<string, { pending?: boolean; error?: string }>;
  onBlock?: (destination: string) => void;
  onUnblock?: (destination: string) => void;
  onForgetPeer?: (destination: string, force: boolean) => Promise<void>;
  onArchivePeer?: (destination: string) => Promise<void>;
  onUnarchivePeer?: (destination: string) => Promise<void>;
  archived?: ArchivedConversation[];
  archivedError?: string | null;
  infos?: Record<string, PeerInfoState>;
  onInfo?: (destination: string) => void;
};

function renderSidebar(
  unread: Record<string, number>,
  onSelectPeer = vi.fn(),
  third:
    | Record<string, PeerInfoState>
    | ((destination: string, force: boolean) => Promise<void>)
    | SidebarOptions = {},
  onInfo = vi.fn(),
) {
  // The branch's peer-info tests pass the infos record positionally as the
  // third argument; the options object carries the same record as `infos`.
  const infosRecord =
    typeof third !== "function" &&
    Object.values(third).some(
      (value) =>
        value !== null &&
        typeof value === "object" &&
        ("info" in value || "pending" in value || "error" in value),
    );
  const options: SidebarOptions =
    typeof third === "function" || infosRecord
      ? {}
      : (third as unknown as SidebarOptions);
  const infos = infosRecord
    ? (third as Record<string, PeerInfoState>)
    : (options.infos ?? {});
  const onForgetPeer =
    typeof third === "function" ? third : (options.onForgetPeer ?? vi.fn());
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
      infos={infos}
      blocks={options.blocks ?? {}}
      verifications={{}}
      unread={unread}
      archived={options.archived ?? []}
      archivedError={options.archivedError ?? null}
      selected=""
      onPing={() => {}}
      onInfo={options.onInfo ?? onInfo}
      onForgetPeer={onForgetPeer}
      onBlock={options.onBlock ?? (() => {})}
      onUnblock={options.onUnblock ?? (() => {})}
      onArchivePeer={options.onArchivePeer ?? (async () => {})}
      onUnarchivePeer={options.onUnarchivePeer ?? (async () => {})}
      onVerify={() => {}}
      onShowSafetyNumber={() => {}}
      onDismissVerification={() => {}}
      onSelectPeer={onSelectPeer}
      onSetAlias={onSetAlias}
      onCreateGroup={options.onCreateGroup}
      onDeleteGroup={options.onDeleteGroup}
      onAddGroupMembers={options.onAddGroupMembers}
      onRemoveGroupMembers={options.onRemoveGroupMembers}
      onRenameGroup={options.onRenameGroup}
      onSetGroupTopic={options.onSetGroupTopic}
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
            rns_version: "0.9.3",
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

  it("offers member changes only on groups this node created", () => {
    const onAddGroupMembers = vi.fn(async () => ({
      ok: true,
      group: makeGroup(),
      previous_id: "11".repeat(16),
    }));
    const onRemoveGroupMembers = vi.fn(async () => ({
      ok: true,
      group: makeGroup(),
      previous_id: "11".repeat(16),
    }));
    renderSidebar({}, vi.fn(), {
      groups: [
        makeGroup({ created_by_me: true }),
        makeGroup({ id: "22".repeat(16), name: "theirs" }),
      ],
      onAddGroupMembers,
      onRemoveGroupMembers,
    });

    expect(
      screen.getAllByRole("button", { name: "members" }),
    ).toHaveLength(1);
  });

  it("adds a picked peer to a group and closes the panel", async () => {
    const successor = makeGroup({
      id: "22".repeat(16),
      members: [
        { hash: "ab".repeat(16), name: "alice" },
        { hash: "cd".repeat(16), name: "bob" },
      ],
    });
    const onAddGroupMembers = vi.fn(async () => ({
      ok: true,
      group: successor,
      previous_id: "11".repeat(16),
    }));
    const onRemoveGroupMembers = vi.fn(async () => ({
      ok: true,
      group: successor,
      previous_id: "11".repeat(16),
    }));
    renderSidebar({}, vi.fn(), {
      groups: [
        makeGroup({
          created_by_me: true,
          members: [{ hash: "ab".repeat(16), name: "alice" }],
        }),
      ],
      node: makeNode(),
      onAddGroupMembers,
      onRemoveGroupMembers,
    });

    fireEvent.click(screen.getByRole("button", { name: "members" }));
    fireEvent.click(screen.getByRole("checkbox", { name: /bob/ }));
    fireEvent.click(screen.getByRole("button", { name: "add selected" }));

    expect(onAddGroupMembers).toHaveBeenCalledWith("11".repeat(16), [
      "cd".repeat(16),
    ]);
    await waitFor(() =>
      expect(
        screen.queryByRole("button", { name: "add selected" }),
      ).toBeNull(),
    );
  });

  it("removes a picked member from a group", async () => {
    const successor = makeGroup({
      id: "22".repeat(16),
      members: [{ hash: "ab".repeat(16), name: "alice" }],
    });
    const onAddGroupMembers = vi.fn(async () => ({
      ok: true,
      group: successor,
      previous_id: "11".repeat(16),
    }));
    const onRemoveGroupMembers = vi.fn(async () => ({
      ok: true,
      group: successor,
      previous_id: "11".repeat(16),
    }));
    renderSidebar({}, vi.fn(), {
      groups: [makeGroup({ created_by_me: true })],
      node: makeNode(),
      onAddGroupMembers,
      onRemoveGroupMembers,
    });

    fireEvent.click(screen.getByRole("button", { name: "members" }));
    fireEvent.click(screen.getByRole("checkbox", { name: /bob/ }));
    fireEvent.click(screen.getByRole("button", { name: "remove selected" }));

    expect(onRemoveGroupMembers).toHaveBeenCalledWith("11".repeat(16), [
      "cd".repeat(16),
    ]);
    await waitFor(() =>
      expect(
        screen.queryByRole("button", { name: "remove selected" }),
      ).toBeNull(),
    );
  });

  it("shows a member-change error instead of closing the panel", async () => {
    const onAddGroupMembers = vi.fn(async () => {
      throw new Error("only the creator can change a group's members");
    });
    const onRemoveGroupMembers = vi.fn(async () => ({
      ok: true,
      group: makeGroup(),
      previous_id: "11".repeat(16),
    }));
    renderSidebar({}, vi.fn(), {
      groups: [
        makeGroup({
          created_by_me: true,
          members: [{ hash: "ab".repeat(16), name: "alice" }],
        }),
      ],
      node: makeNode(),
      onAddGroupMembers,
      onRemoveGroupMembers,
    });

    fireEvent.click(screen.getByRole("button", { name: "members" }));
    fireEvent.click(screen.getByRole("checkbox", { name: /bob/ }));
    fireEvent.click(screen.getByRole("button", { name: "add selected" }));

    await screen.findByText("only the creator can change a group's members");
    expect(
      screen.getByRole("button", { name: "add selected" }),
    ).toBeTruthy();
  });

  it("does not let a finished change discard another group's editor", async () => {
    let resolveAdd: (value: ChangeGroupMembersResponse) => void = () => {};
    const pending = new Promise<ChangeGroupMembersResponse>((resolve) => {
      resolveAdd = resolve;
    });
    const onAddGroupMembers = vi.fn(() => pending);
    const onRemoveGroupMembers = vi.fn(async () => ({
      ok: true,
      group: makeGroup(),
      previous_id: "11".repeat(16),
    }));
    renderSidebar({}, vi.fn(), {
      groups: [
        makeGroup({
          created_by_me: true,
          members: [{ hash: "ab".repeat(16), name: "alice" }],
        }),
        makeGroup({
          id: "22".repeat(16),
          name: "other",
          created_by_me: true,
          members: [
            { hash: "ab".repeat(16), name: "alice" },
            { hash: "cd".repeat(16), name: "bob" },
          ],
        }),
      ],
      node: makeNode(),
      onAddGroupMembers,
      onRemoveGroupMembers,
    });

    // Open the first group's editor and start adding bob.
    fireEvent.click(screen.getAllByRole("button", { name: "members" })[0]);
    fireEvent.click(screen.getByRole("checkbox", { name: /bob/ }));
    fireEvent.click(screen.getByRole("button", { name: "add selected" }));
    expect(onAddGroupMembers).toHaveBeenCalledTimes(1);

    // Switch to the other group's editor while the change is pending
    // and pick bob for removal there.
    fireEvent.click(screen.getByRole("button", { name: "members" }));
    fireEvent.click(screen.getByRole("checkbox", { name: /bob/ }));

    // The first change lands: the new editor keeps its selection and
    // stays open.
    await act(async () => {
      resolveAdd({
        ok: true,
        group: makeGroup({ id: "33".repeat(16) }),
        previous_id: "11".repeat(16),
      });
    });
    expect(
      screen.getByRole("button", { name: "remove selected" }),
    ).toBeTruthy();
    expect(
      (
        screen.getByRole("checkbox", {
          name: /bob/,
        }) as HTMLInputElement
      ).checked,
    ).toBe(true);
  });

  it("shows a group's topic on its row", () => {
    renderSidebar({}, vi.fn(), {
      groups: [makeGroup({ topic: "hiking plans" })],
    });

    expect(screen.getByText("hiking plans")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "edit" })).toBeNull();
  });

  it("offers rename and topic edits only on groups this node created", () => {
    const onRenameGroup = vi.fn(async () => ({
      ok: true,
      group: makeGroup(),
      previous_id: "11".repeat(16),
    }));
    const onSetGroupTopic = vi.fn(async () => ({
      ok: true,
      group: makeGroup(),
    }));
    renderSidebar({}, vi.fn(), {
      groups: [
        makeGroup({ created_by_me: true }),
        makeGroup({ id: "22".repeat(16), name: "theirs" }),
      ],
      onRenameGroup,
      onSetGroupTopic,
    });

    expect(screen.getAllByRole("button", { name: "edit" })).toHaveLength(1);
  });

  it("renames a group and closes the panel", async () => {
    const successor = makeGroup({ id: "22".repeat(16), name: "weekdays" });
    const onRenameGroup = vi.fn(async () => ({
      ok: true,
      group: successor,
      previous_id: "11".repeat(16),
    }));
    const onSetGroupTopic = vi.fn(async () => ({
      ok: true,
      group: makeGroup(),
    }));
    renderSidebar({}, vi.fn(), {
      groups: [makeGroup({ created_by_me: true })],
      onRenameGroup,
      onSetGroupTopic,
    });

    fireEvent.click(screen.getByRole("button", { name: "edit" }));
    fireEvent.change(screen.getByLabelText("name"), {
      target: { value: "weekdays" },
    });
    fireEvent.click(screen.getByRole("button", { name: "save name" }));

    expect(onRenameGroup).toHaveBeenCalledWith("11".repeat(16), "weekdays");
    await waitFor(() =>
      expect(
        screen.queryByRole("button", { name: "save name" }),
      ).toBeNull(),
    );
  });

  it("sets a group's topic and closes the panel", async () => {
    const updated = makeGroup({ topic: "hiking plans" });
    const onRenameGroup = vi.fn(async () => ({
      ok: true,
      group: makeGroup(),
      previous_id: "11".repeat(16),
    }));
    const onSetGroupTopic = vi.fn(async () => ({
      ok: true,
      group: updated,
    }));
    renderSidebar({}, vi.fn(), {
      groups: [makeGroup({ created_by_me: true })],
      onRenameGroup,
      onSetGroupTopic,
    });

    fireEvent.click(screen.getByRole("button", { name: "edit" }));
    fireEvent.change(screen.getByLabelText("topic"), {
      target: { value: "hiking plans" },
    });
    fireEvent.click(screen.getByRole("button", { name: "save topic" }));

    expect(onSetGroupTopic).toHaveBeenCalledWith(
      "11".repeat(16),
      "hiking plans",
    );
    await waitFor(() =>
      expect(
        screen.queryByRole("button", { name: "save topic" }),
      ).toBeNull(),
    );
  });

  it("clears a group's topic", async () => {
    const onRenameGroup = vi.fn(async () => ({
      ok: true,
      group: makeGroup(),
      previous_id: "11".repeat(16),
    }));
    const onSetGroupTopic = vi.fn(async () => ({
      ok: true,
      group: makeGroup({ topic: "" }),
    }));
    renderSidebar({}, vi.fn(), {
      groups: [makeGroup({ created_by_me: true, topic: "hiking plans" })],
      onRenameGroup,
      onSetGroupTopic,
    });

    fireEvent.click(screen.getByRole("button", { name: "edit" }));
    fireEvent.click(screen.getByRole("button", { name: "clear topic" }));

    expect(onSetGroupTopic).toHaveBeenCalledWith("11".repeat(16), null);
    await waitFor(() =>
      expect(
        screen.queryByRole("button", { name: "clear topic" }),
      ).toBeNull(),
    );
  });

  it("shows a rename error instead of closing the panel", async () => {
    const onRenameGroup = vi.fn(async () => {
      throw new Error("only the creator can rename a group");
    });
    const onSetGroupTopic = vi.fn(async () => ({
      ok: true,
      group: makeGroup(),
    }));
    renderSidebar({}, vi.fn(), {
      groups: [makeGroup({ created_by_me: true })],
      onRenameGroup,
      onSetGroupTopic,
    });

    fireEvent.click(screen.getByRole("button", { name: "edit" }));
    fireEvent.change(screen.getByLabelText("name"), {
      target: { value: "weekdays" },
    });
    fireEvent.click(screen.getByRole("button", { name: "save name" }));

    await screen.findByText("only the creator can rename a group");
    expect(screen.getByRole("button", { name: "save name" })).toBeTruthy();
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

describe("PeerSidebar archived conversations", () => {
  const aliceHash = "ab".repeat(16);
  const bobHash = "cd".repeat(16);

  it("archives a conversation from the peer row without a confirm step", async () => {
    const onArchivePeer = vi.fn().mockResolvedValue(undefined);
    renderSidebar({}, vi.fn(), { onArchivePeer });

    fireEvent.click(
      screen.getByRole("button", { name: "Archive conversation with alice" }),
    );
    await waitFor(() =>
      expect(onArchivePeer).toHaveBeenCalledWith(aliceHash),
    );
  });

  it("hides an archived peer from the list and shows it under archived", async () => {
    const onUnarchivePeer = vi.fn().mockResolvedValue(undefined);
    renderSidebar(
      {},
      vi.fn(),
      {
        archived: [{ destination: aliceHash, name: "alice", messages: 3 }],
        onUnarchivePeer,
      },
    );

    // Alice's row — including its archive button — is gone from the
    // live list; bob is still there.
    expect(
      screen.queryByRole("button", {
        name: "Archive conversation with alice",
      }),
    ).toBeNull();
    expect(
      screen.getByRole("button", { name: "Archive conversation with bob" }),
    ).toBeTruthy();
    // The archived section carries her with the message count.
    expect(screen.getByText("archived")).toBeTruthy();
    expect(screen.getByText(/3 messages/)).toBeTruthy();

    fireEvent.click(
      screen.getByRole("button", { name: "Restore conversation with alice" }),
    );
    await waitFor(() =>
      expect(onUnarchivePeer).toHaveBeenCalledWith(aliceHash),
    );
  });

  it("says so when every conversation is archived", () => {
    renderSidebar(
      {},
      vi.fn(),
      {
        peers: [
          makePeer(),
          makePeer({ destination: bobHash, name: "bob" }),
        ],
        archived: [
          { destination: aliceHash, name: "alice", messages: 1 },
          { destination: bobHash, name: "bob", messages: 2 },
        ],
      },
    );

    expect(
      screen.getByText(/Every conversation is archived/),
    ).toBeTruthy();
  });

  it("shows an archive failure under the row", async () => {
    const onArchivePeer = vi
      .fn()
      .mockRejectedValue(new Error("archive-list is unusable"));
    renderSidebar({}, vi.fn(), { onArchivePeer });

    fireEvent.click(
      screen.getByRole("button", { name: "Archive conversation with alice" }),
    );
    await waitFor(() =>
      expect(screen.getByText("archive-list is unusable")).toBeTruthy(),
    );
  });

  it("shows a failed archived-list poll in the archived section", () => {
    renderSidebar(
      {},
      vi.fn(),
      {
        archived: [],
        archivedError: "could not load archived",
      },
    );

    expect(screen.getByText("could not load archived")).toBeTruthy();
  });
});
