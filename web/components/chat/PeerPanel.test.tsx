// PeerPanel.test.tsx — the DM public-profile section (display name, bio,
// links from the peer's served profile), the group invite-link section
// (shown only for groups this node created, with create / copy / mode /
// revoke), and the group join-requests section (pending closed-invite
// requests, with approve / deny).

import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import PeerPanel from "./PeerPanel";
import { TauContext } from "../chrome/TauContext";
import { fakeTau as fakeTauChat, makePeer } from "../test-utils";
import { fakeTau as fakeTauSections, fakePeer, mockFetch } from "../sections/test-utils";
import type { Conversation } from "@/lib/use-tau";
import type { Group, JoinRequest, Peer } from "@/lib/types";

const PEER_HASH = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6";
const GROUP_ID = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6";

function conv(peer: Peer): Conversation {
  return {
    kind: "dm",
    key: PEER_HASH,
    name: "Alice",
    peer,
    lastMessage: null,
    unread: 0,
    muted: false,
    archived: false,
    ignored: false,
    pinned: false,
  };
}

function renderDmPanel(peer: Peer) {
  render(
    <TauContext.Provider value={fakeTauChat()}>
      <PeerPanel conversation={conv(peer)} />
    </TauContext.Provider>,
  );
}

function groupConv(overrides: Partial<Group> = {}): Conversation {
  const group: Group = {
    id: GROUP_ID,
    name: "crew",
    members: [{ hash: "b".repeat(32), name: "bob" }],
    created_at: 1700000000,
    topic: "",
    created_by_me: true,
    can_leave: false,
    ignored: false,
    ...overrides,
  };
  return {
    kind: "group",
    key: GROUP_ID,
    name: "crew",
    group,
    lastMessage: null,
    unread: 0,
    muted: false,
    archived: false,
    ignored: false,
    pinned: false,
  };
}

function renderPanel(conversation: Conversation) {
  return render(
    <TauContext.Provider value={fakeTauSections()}>
      <PeerPanel conversation={conversation} />
    </TauContext.Provider>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("PeerPanel public profile", () => {
  it("renders the peer's display name, bio and links from the served profile", () => {
    renderDmPanel(
      makePeer({
        destination: PEER_HASH,
        profile: {
          author: PEER_HASH,
          name: "Alice Liddell",
          bio: "Mesh tinkerer",
          avatar: "",
          links: ["https://example.com"],
          version: 1,
          created_at: 1700000000,
        },
      }),
    );
    expect(screen.getByText("Public profile")).toBeTruthy();
    expect(screen.getByText("Alice Liddell")).toBeTruthy();
    expect(screen.getByText("Mesh tinkerer")).toBeTruthy();
    const link = screen.getByRole("link", { name: "https://example.com" });
    expect(link.getAttribute("href")).toBe("https://example.com");
    expect(link.getAttribute("target")).toBe("_blank");
  });

  it("says so when the peer shared no public profile", () => {
    renderDmPanel(makePeer({ destination: PEER_HASH, profile: null }));
    expect(screen.getByText("Public profile")).toBeTruthy();
    expect(screen.getByText("This peer hasn't shared a public profile.")).toBeTruthy();
    expect(screen.queryByRole("link")).toBeNull();
  });

  it("renders a profile with an empty bio and no links", () => {
    renderDmPanel(
      makePeer({
        destination: PEER_HASH,
        profile: {
          author: PEER_HASH,
          name: "Alice Liddell",
          bio: "",
          avatar: "",
          links: [],
          version: 1,
          created_at: 1700000000,
        },
      }),
    );
    expect(screen.getByText("Alice Liddell")).toBeTruthy();
    expect(screen.queryByRole("link")).toBeNull();
  });
});

describe("PeerPanel invite links", () => {
  it("offers to create a link when none exists, then shows it", async () => {
    const { calls } = mockFetch((url, method) => {
      if (url.includes("/api/groups/invite")) {
        if (method === "POST") {
          return { ok: true, link: "tau://invite/xyz", mode: "open", generation: 0 };
        }
        return { mode: "open", generation: 0, link: null };
      }
      return { ok: true };
    });
    renderPanel(groupConv());
    await waitFor(() =>
      expect(screen.getByText("Create invite link")).toBeTruthy(),
    );
    fireEvent.click(screen.getByText("Create invite link"));
    await waitFor(() =>
      expect(screen.getByLabelText("Invite link").textContent).toBe(
        "tau://invite/xyz",
      ),
    );
    expect(calls).toContain("POST /api/groups/invite");
  });

  it("shows the current link with copy and mode controls", async () => {
    Object.assign(navigator, {
      clipboard: { writeText: vi.fn(async () => undefined) },
    });
    mockFetch((url, method) => {
      if (url.includes("/api/groups/invite") && method === "GET") {
        return { mode: "closed", generation: 0, link: "tau://invite/abc" };
      }
      return { ok: true };
    });
    renderPanel(groupConv());
    await waitFor(() =>
      expect(screen.getByLabelText("Invite link").textContent).toBe(
        "tau://invite/abc",
      ),
    );
    expect(screen.getByText("Copy link")).toBeTruthy();
    expect(screen.getByText("New link")).toBeTruthy();
    expect(screen.getByText("Revoke")).toBeTruthy();
    // Closed is current, so only Open is offered as a switch.
    expect(
      screen.getByText("Closed — approve first") as HTMLButtonElement,
    ).toBeTruthy();
    expect(
      (screen.getByText("Closed — approve first") as HTMLButtonElement).disabled,
    ).toBe(true);
    expect(
      (screen.getByText("Open — joins at once") as HTMLButtonElement).disabled,
    ).toBe(false);
    fireEvent.click(screen.getByText("Copy link"));
    await waitFor(() => expect(screen.getByText("Copied")).toBeTruthy());
  });

  it("revokes with a confirm step and shows create again", async () => {
    let currentLink: string | null = "tau://invite/abc";
    const { calls } = mockFetch((url, method) => {
      if (url.includes("/api/groups/invite/revoke")) {
        currentLink = null;
        return { ok: true, generation: 1 };
      }
      if (url.includes("/api/groups/invite")) {
        if (method === "POST") {
          return { ok: true, link: "tau://invite/xyz", mode: "open", generation: 1 };
        }
        return { mode: "open", generation: currentLink ? 0 : 1, link: currentLink };
      }
      return { ok: true };
    });
    renderPanel(groupConv());
    await waitFor(() =>
      expect(screen.getByLabelText("Invite link").textContent).toBe(
        "tau://invite/abc",
      ),
    );
    fireEvent.click(screen.getByText("Revoke"));
    expect(screen.getByText("Confirm revoke?")).toBeTruthy();
    fireEvent.click(screen.getByText("Confirm revoke?"));
    await waitFor(() =>
      expect(screen.getByText("Create invite link")).toBeTruthy(),
    );
    expect(calls).toContain("POST /api/groups/invite/revoke");
  });

  it("shows no invite section for a group this node did not create", async () => {
    const { calls } = mockFetch(() => ({ ok: true }));
    renderPanel(groupConv({ created_by_me: false }));
    await waitFor(() => expect(screen.getByText("Members")).toBeTruthy());
    expect(screen.queryByText("Invite link")).toBeNull();
    expect(screen.queryByText("Create invite link")).toBeNull();
    expect(calls.filter((c) => c.includes("/api/groups/invite"))).toEqual([]);
  });
});

describe("PeerPanel join requests", () => {
  const CAROL = "c".repeat(32);
  const DAVE = "d".repeat(32);
  const OTHER_GROUP = "e".repeat(32);
  const SUCCESSOR_ID = "f".repeat(32);

  function joinRequest(overrides: Partial<JoinRequest> = {}): JoinRequest {
    return {
      group: "crew",
      peer: CAROL,
      peer_name: "carol",
      group_id: GROUP_ID,
      requested_at: 1700000000,
      expires_at: 1700086400,
      ...overrides,
    };
  }

  function renderRequestsPanel(
    conversation: Conversation,
    successor: Group | null = null,
    onSuccessorPending: (successorId: string, originHash: string) => void = () => {},
  ) {
    const tau = fakeTauSections();
    const snap = tau.snapshot;
    tau.refresh = vi.fn(async () =>
      successor && snap ? { ...snap, groups: [successor] } : null,
    );
    return render(
      <TauContext.Provider value={tau}>
        <PeerPanel conversation={conversation} onSuccessorPending={onSuccessorPending} />
      </TauContext.Provider>,
    );
  }

  // mockFetch records method + url only; decisions need the body
  // asserted too, so this local variant records parsed JSON bodies.
  function mockFetchBodies(
    handler: (url: string, method: string, body: unknown) => unknown,
  ) {
    const calls: { method: string; url: string; body: unknown }[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        const method = init?.method ?? "GET";
        let body: unknown = null;
        if (init?.body) {
          try {
            body = JSON.parse(String(init.body));
          } catch {
            body = null;
          }
        }
        calls.push({ method, url, body });
        const payload = await handler(url, method, body);
        return {
          ok: true,
          status: 200,
          text: async () => JSON.stringify(payload ?? {}),
        } as Response;
      }),
    );
    return { calls };
  }

  function inviteOnly(url: string) {
    if (url.includes("/api/groups/invite")) {
      return { mode: "open", generation: 0, link: null };
    }
    return { ok: true };
  }

  afterEach(() => {
    // A rotated group id lands in the URL hash; clear it so the next
    // test starts from the default "#chat" hash.
    window.location.hash = "";
  });

  it("lists this group's pending requests only, with approve and deny", async () => {
    const requests = [
      joinRequest(),
      joinRequest({ peer: DAVE, peer_name: "dave", group_id: OTHER_GROUP }),
    ];
    mockFetchBodies((url) => {
      if (url.includes("/api/groups/join-requests")) {
        return { requests, count: requests.length };
      }
      return inviteOnly(url);
    });
    renderRequestsPanel(groupConv());
    await waitFor(() => expect(screen.getByText("Join requests")).toBeTruthy());
    expect(screen.getByText("carol")).toBeTruthy();
    expect(screen.queryByText("dave")).toBeNull();
    expect(screen.getByLabelText("Approve carol to join")).toBeTruthy();
    expect(screen.getByLabelText("Deny carol to join")).toBeTruthy();
  });

  it("approves a request, posts the decision, and follows the rotated group id", async () => {
    const successor: Group = {
      ...groupConv().group!,
      id: SUCCESSOR_ID,
      members: [...groupConv().group!.members, { hash: CAROL, name: "carol" }],
    };
    let requests = [joinRequest()];
    const { calls } = mockFetchBodies((url, method) => {
      if (url.includes("/api/groups/approve-join")) {
        requests = [];
        return {
          ok: true,
          status: "added",
          group: { id: SUCCESSOR_ID, name: "crew", members: [], creator: "" },
        };
      }
      if (url.includes("/api/groups/join-requests")) {
        return { requests, count: requests.length };
      }
      return inviteOnly(url);
    });
    renderRequestsPanel(groupConv(), successor);
    await waitFor(() =>
      expect(screen.getByLabelText("Approve carol to join")).toBeTruthy(),
    );
    fireEvent.click(screen.getByLabelText("Approve carol to join"));
    await waitFor(() =>
      expect(window.location.hash).toBe(`#chat?group=${SUCCESSOR_ID}`),
    );
    expect(
      calls.some(
        (c) =>
          c.method === "POST" &&
          c.url.includes("/api/groups/approve-join") &&
          JSON.stringify(c.body) ===
            JSON.stringify({ group: GROUP_ID, peer: CAROL }),
      ),
    ).toBe(true);
  });

  it("denies a request and removes it from the list", async () => {
    let requests = [joinRequest()];
    const { calls } = mockFetchBodies((url, method) => {
      if (url.includes("/api/groups/deny-join")) {
        requests = [];
        return {
          ok: true,
          status: "denied",
          group: { id: GROUP_ID, name: "crew", members: [], creator: "" },
        };
      }
      if (url.includes("/api/groups/join-requests")) {
        return { requests, count: requests.length };
      }
      return inviteOnly(url);
    });
    renderRequestsPanel(groupConv());
    await waitFor(() =>
      expect(screen.getByLabelText("Deny carol to join")).toBeTruthy(),
    );
    fireEvent.click(screen.getByLabelText("Deny carol to join"));
    await waitFor(() => expect(screen.queryByText("Join requests")).toBeNull());
    expect(window.location.hash).toBe("");
    expect(
      calls.some(
        (c) =>
          c.method === "POST" &&
          c.url.includes("/api/groups/deny-join") &&
          JSON.stringify(c.body) ===
            JSON.stringify({ group: GROUP_ID, peer: CAROL }),
      ),
    ).toBe(true);
  });

  it("approving a peer who is already a member clears the stale request", async () => {
    let requests = [joinRequest()];
    const { calls } = mockFetchBodies((url, method) => {
      if (url.includes("/api/groups/approve-join")) {
        requests = [];
        return {
          ok: true,
          status: "already_member",
          group: { id: GROUP_ID, name: "crew", members: [], creator: "" },
        };
      }
      if (url.includes("/api/groups/join-requests")) {
        return { requests, count: requests.length };
      }
      return inviteOnly(url);
    });
    renderRequestsPanel(groupConv());
    await waitFor(() =>
      expect(screen.getByLabelText("Approve carol to join")).toBeTruthy(),
    );
    fireEvent.click(screen.getByLabelText("Approve carol to join"));
    await waitFor(() => expect(screen.queryByText("Join requests")).toBeNull());
    expect(window.location.hash).toBe("");
    expect(
      calls.some(
        (c) =>
          c.method === "POST" &&
          c.url.includes("/api/groups/approve-join") &&
          JSON.stringify(c.body) ===
            JSON.stringify({ group: GROUP_ID, peer: CAROL }),
      ),
    ).toBe(true);
  });

  it("stays put and keeps the list when the refresh cannot load the successor", async () => {
    // The approval landed but the snapshot fetch failed, so the
    // successor is not in any snapshot: navigating would point the
    // thread at a group the console cannot display yet, and the
    // retired panel must not take on the successor's requests.
    const onSuccessorPending = vi.fn();
    const { calls } = mockFetchBodies((url, method) => {
      if (url.includes("/api/groups/approve-join")) {
        return {
          ok: true,
          status: "added",
          group: { id: SUCCESSOR_ID, name: "crew", members: [], creator: "" },
        };
      }
      if (url.includes("/api/groups/join-requests")) {
        return { requests: [joinRequest()], count: 1 };
      }
      return inviteOnly(url);
    });
    renderRequestsPanel(groupConv(), null, onSuccessorPending);
    await waitFor(() =>
      expect(screen.getByLabelText("Approve carol to join")).toBeTruthy(),
    );
    fireEvent.click(screen.getByLabelText("Approve carol to join"));
    await waitFor(() =>
      expect(
        screen.getByText(
          "Member added, but the new group couldn't be loaded yet — it will appear when the connection recovers.",
        ),
      ).toBeTruthy(),
    );
    expect(window.location.hash).toBe("");
    expect(onSuccessorPending).toHaveBeenCalledWith(SUCCESSOR_ID, "");
    // The request stays listed: the retired panel never took on the
    // successor's list, so deciding it again would not send the
    // retired id.
    expect(screen.getByLabelText("Approve carol to join")).toBeTruthy();
    expect(
      calls.filter(
        (c) => c.method === "GET" && c.url.includes("/api/groups/join-requests"),
      ),
    ).toHaveLength(1);
  });

  it("shows a load error apart from action errors, and clears it on a fresh load", async () => {
    let calls = 0;
    mockFetchBodies((url) => {
      if (url.includes("/api/groups/join-requests")) {
        calls += 1;
        if (calls === 1) {
          throw new Error("boom");
        }
        return { requests: [joinRequest({ group_id: OTHER_GROUP })], count: 1 };
      }
      return inviteOnly(url);
    });
    const other = {
      ...groupConv(),
      key: OTHER_GROUP,
      name: "crew2",
      group: { ...groupConv().group!, id: OTHER_GROUP },
    };
    const { rerender } = renderPanel(groupConv());
    await waitFor(() =>
      expect(screen.getByText(/Couldn't load join requests/)).toBeTruthy(),
    );
    expect(screen.queryByText("Join requests")).toBeNull();
    rerender(
      <TauContext.Provider value={fakeTauSections()}>
        <PeerPanel conversation={other} />
      </TauContext.Provider>,
    );
    await waitFor(() => expect(screen.getByText("Join requests")).toBeTruthy());
    expect(screen.queryByText(/Couldn't load join requests/)).toBeNull();
    expect(screen.getByText("carol")).toBeTruthy();
  });

  it("shows no section when there are no pending requests", async () => {
    mockFetch((url) => {
      if (url.includes("/api/groups/join-requests")) {
        return { requests: [], count: 0 };
      }
      return inviteOnly(url);
    });
    renderPanel(groupConv());
    await waitFor(() => expect(screen.getByText("Members")).toBeTruthy());
    expect(screen.queryByText("Join requests")).toBeNull();
  });

  it("shows no section and fetches nothing for a group this node did not create", async () => {
    const { calls } = mockFetch(() => ({ ok: true }));
    renderPanel(groupConv({ created_by_me: false }));
    await waitFor(() => expect(screen.getByText("Members")).toBeTruthy());
    expect(screen.queryByText("Join requests")).toBeNull();
    expect(calls.filter((c) => c.includes("/api/groups/join-requests"))).toEqual([]);
  });

  it("shows an API error and keeps the request for a retry", async () => {
    mockFetchBodies((url, method) => {
      if (url.includes("/api/groups/approve-join")) {
        throw new Error("no pending join request from that peer");
      }
      if (url.includes("/api/groups/join-requests")) {
        return { requests: [joinRequest()], count: 1 };
      }
      return inviteOnly(url);
    });
    renderRequestsPanel(groupConv());
    await waitFor(() =>
      expect(screen.getByLabelText("Approve carol to join")).toBeTruthy(),
    );
    fireEvent.click(screen.getByLabelText("Approve carol to join"));
    await waitFor(() =>
      expect(
        screen.getByText("no pending join request from that peer"),
      ).toBeTruthy(),
    );
    expect(screen.getByLabelText("Approve carol to join")).toBeTruthy();
  });
});

describe("PeerPanel announcements-only", () => {
  it("shows the posting state and offers the toggle to the creator", async () => {
    const { calls } = mockFetch((url, method) => {
      if (url.includes("/api/groups/announcements")) {
        return { ok: true, group: { ...groupConv().group, announcements_only: true } };
      }
      if (url.includes("/api/groups/invite")) {
        return { mode: "open", generation: 0, link: null };
      }
      return { ok: true };
    });
    renderPanel(groupConv());
    await waitFor(() => expect(screen.getByText("Members")).toBeTruthy());
    expect(screen.getByText("Everyone")).toBeTruthy();
    fireEvent.click(screen.getByText("Make announcements-only"));
    await waitFor(() =>
      expect(calls).toContain("POST /api/groups/announcements"),
    );
  });

  it("offers to open posting again when the group is announcements-only", async () => {
    mockFetch((url) => {
      if (url.includes("/api/groups/invite")) {
        return { mode: "open", generation: 0, link: null };
      }
      return { ok: true };
    });
    renderPanel(groupConv({ announcements_only: true }));
    await waitFor(() => expect(screen.getByText("Members")).toBeTruthy());
    expect(screen.getByText("Creator only")).toBeTruthy();
    expect(screen.getByText("Open posting to everyone")).toBeTruthy();
  });

  it("shows the state with no toggle for a group this node did not create", async () => {
    mockFetch(() => ({ ok: true }));
    renderPanel(groupConv({ created_by_me: false, announcements_only: true }));
    await waitFor(() => expect(screen.getByText("Members")).toBeTruthy());
    expect(screen.getByText("Creator only")).toBeTruthy();
    expect(screen.queryByText("Make announcements-only")).toBeNull();
    expect(screen.queryByText("Open posting to everyone")).toBeNull();
  });
});

describe("PeerPanel group members", () => {
  const SELF = "a".repeat(32);
  const BOB = "b".repeat(32);
  const CAROL = "c".repeat(32);
  const NEW_ID = "d".repeat(32);

  // The fake node's own destination is "a"*32, so the creator's own
  // row is the "testnode" member.
  function memberGroup(overrides: Partial<Group> = {}): Conversation {
    return groupConv({
      members: [
        { hash: SELF, name: "testnode" },
        { hash: BOB, name: "bob" },
      ],
      ...overrides,
    });
  }

  function renderGroupPanel(
    conversation: Conversation,
    peers: Peer[] = [
      fakePeer({ destination: BOB, name: "bob" }),
      fakePeer({ destination: CAROL, name: "carol" }),
    ],
    // The successor group the refresh returns, when the roster change
    // rotated the id. Null (the default) is a failed refresh: the
    // successor is not in any snapshot, so the panel must not navigate.
    successor: Group | null = null,
    onSuccessorPending: (successorId: string, originHash: string) => void = () => {},
  ) {
    const tau = fakeTauSections({ peers });
    const snap = tau.snapshot;
    tau.refresh = vi.fn(async () =>
      successor && snap ? { ...snap, groups: [successor] } : null,
    );
    return render(
      <TauContext.Provider value={tau}>
        <PeerPanel conversation={conversation} onSuccessorPending={onSuccessorPending} />
      </TauContext.Provider>,
    );
  }

  // mockFetch records method + url only; member changes need the body
  // asserted too, so this local variant records parsed JSON bodies.
  function mockFetchBodies(
    handler: (url: string, method: string, body: unknown) => unknown,
  ) {
    const calls: { method: string; url: string; body: unknown }[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        const method = init?.method ?? "GET";
        let body: unknown = null;
        if (init?.body) {
          try {
            body = JSON.parse(String(init.body));
          } catch {
            body = null;
          }
        }
        calls.push({ method, url, body });
        const payload = await handler(url, method, body);
        return {
          ok: true,
          status: 200,
          text: async () => JSON.stringify(payload ?? {}),
        } as Response;
      }),
    );
    return { calls };
  }

  function inviteOnly(url: string) {
    if (url.includes("/api/groups/invite")) {
      return { mode: "open", generation: 0, link: null };
    }
    return { ok: true };
  }

  afterEach(() => {
    // A rotated group id lands in the URL hash; clear it so the next
    // test starts from the default "#chat" hash.
    window.location.hash = "";
  });

  it("offers Remove on other members, none on the creator's own row, and an add picker for known peers", async () => {
    mockFetchBodies((url) => inviteOnly(url));
    renderGroupPanel(memberGroup());
    await waitFor(() => expect(screen.getByText("Members")).toBeTruthy());
    expect(screen.getByLabelText("Remove bob from group")).toBeTruthy();
    expect(screen.queryByLabelText("Remove testnode from group")).toBeNull();
    const select = screen.getByLabelText("Add member") as HTMLSelectElement;
    expect(Array.from(select.options).map((o) => o.value)).toEqual(["", CAROL]);
    expect(screen.getByText("Add")).toBeTruthy();
    expect((screen.getByText("Add") as HTMLButtonElement).disabled).toBe(true);
  });

  it("adds the picked peer and follows the rotated group id", async () => {
    const successor: Group = { ...memberGroup().group!, id: NEW_ID };
    const { calls } = mockFetchBodies((url, method) => {
      if (url.includes("/api/groups/add-member")) {
        return {
          group: successor,
          previous_id: GROUP_ID,
        };
      }
      return inviteOnly(url);
    });
    renderGroupPanel(memberGroup(), undefined, successor);
    await waitFor(() => expect(screen.getByText("Members")).toBeTruthy());
    fireEvent.change(screen.getByLabelText("Add member"), {
      target: { value: CAROL },
    });
    fireEvent.click(screen.getByText("Add"));
    await waitFor(() =>
      expect(window.location.hash).toBe(`#chat?group=${NEW_ID}`),
    );
    expect(
      calls.some(
        (c) =>
          c.method === "POST" &&
          c.url.includes("/api/groups/add-member") &&
          JSON.stringify(c.body) ===
            JSON.stringify({ group: GROUP_ID, members: [CAROL] }),
      ),
    ).toBe(true);
  });

  it("removes a member after a confirm step and follows the rotated group id", async () => {
    const successor: Group = { ...memberGroup().group!, id: NEW_ID };
    const { calls } = mockFetchBodies((url, method) => {
      if (url.includes("/api/groups/remove-member")) {
        return {
          group: successor,
          previous_id: GROUP_ID,
        };
      }
      return inviteOnly(url);
    });
    renderGroupPanel(memberGroup(), undefined, successor);
    await waitFor(() => expect(screen.getByText("Members")).toBeTruthy());
    fireEvent.click(screen.getByLabelText("Remove bob from group"));
    expect(screen.getByText("Confirm remove?")).toBeTruthy();
    fireEvent.click(screen.getByText("Confirm remove?"));
    await waitFor(() =>
      expect(window.location.hash).toBe(`#chat?group=${NEW_ID}`),
    );
    expect(
      calls.some(
        (c) =>
          c.method === "POST" &&
          c.url.includes("/api/groups/remove-member") &&
          JSON.stringify(c.body) ===
            JSON.stringify({ group: GROUP_ID, members: [BOB] }),
      ),
    ).toBe(true);
  });

  it("stays put and reports when the refresh cannot load the successor", async () => {
    // The roster change landed but the snapshot fetch failed, so the
    // refresh returns null: navigating would point the thread at a
    // group the console cannot display yet.
    const { calls } = mockFetchBodies((url, method) => {
      if (url.includes("/api/groups/remove-member")) {
        return {
          group: { ...memberGroup().group, id: NEW_ID },
          previous_id: GROUP_ID,
        };
      }
      return inviteOnly(url);
    });
    renderGroupPanel(memberGroup());
    await waitFor(() => expect(screen.getByText("Members")).toBeTruthy());
    fireEvent.click(screen.getByLabelText("Remove bob from group"));
    fireEvent.click(screen.getByText("Confirm remove?"));
    await waitFor(() =>
      expect(
        screen.getByText(
          "Members updated, but the new group couldn't be loaded yet — it will appear when the connection recovers.",
        ),
      ).toBeTruthy(),
    );
    expect(window.location.hash).toBe("");
    expect(
      calls.some(
        (c) =>
          c.method === "POST" && c.url.includes("/api/groups/remove-member"),
      ),
    ).toBe(true);
  });

  it("hands the successor to the parent when the refresh cannot load it", async () => {
    // The roster change landed but the snapshot fetch failed: the
    // successor id lives only in this request, so the panel reports it
    // up so a later refresh can still follow it.
    const onSuccessorPending = vi.fn();
    const { calls } = mockFetchBodies((url, method) => {
      if (url.includes("/api/groups/remove-member")) {
        return {
          group: { ...memberGroup().group, id: NEW_ID },
          previous_id: GROUP_ID,
        };
      }
      return inviteOnly(url);
    });
    window.location.hash = `#chat?group=${GROUP_ID}`;
    renderGroupPanel(memberGroup(), undefined, null, onSuccessorPending);
    await waitFor(() => expect(screen.getByText("Members")).toBeTruthy());
    fireEvent.click(screen.getByLabelText("Remove bob from group"));
    fireEvent.click(screen.getByText("Confirm remove?"));
    await waitFor(() =>
      expect(onSuccessorPending).toHaveBeenCalledWith(
        NEW_ID,
        `#chat?group=${GROUP_ID}`,
      ),
    );
    expect(
      calls.some(
        (c) =>
          c.method === "POST" && c.url.includes("/api/groups/remove-member"),
      ),
    ).toBe(true);
  });

  it("shows no add or remove controls for a group this node did not create", async () => {
    mockFetchBodies((url) => inviteOnly(url));
    renderGroupPanel(memberGroup({ created_by_me: false }));
    await waitFor(() => expect(screen.getByText("Members")).toBeTruthy());
    expect(screen.queryByLabelText("Add member")).toBeNull();
    expect(screen.queryByLabelText("Remove bob from group")).toBeNull();
    expect(screen.getByText("bob")).toBeTruthy();
  });

  it("says so when every known peer is already a member", async () => {
    mockFetchBodies((url) => inviteOnly(url));
    renderGroupPanel(memberGroup(), [
      fakePeer({ destination: BOB, name: "bob" }),
    ]);
    await waitFor(() => expect(screen.getByText("Members")).toBeTruthy());
    expect(screen.queryByLabelText("Add member")).toBeNull();
    expect(
      screen.getByText("Every known peer is already a member."),
    ).toBeTruthy();
  });

  it("shows an API error and keeps the roster for a retry", async () => {
    mockFetchBodies((url, method) => {
      if (url.includes("/api/groups/remove-member")) {
        throw new Error("bob is not a member of 'crew'");
      }
      return inviteOnly(url);
    });
    renderGroupPanel(memberGroup());
    await waitFor(() => expect(screen.getByText("Members")).toBeTruthy());
    fireEvent.click(screen.getByLabelText("Remove bob from group"));
    fireEvent.click(screen.getByText("Confirm remove?"));
    await waitFor(() =>
      expect(screen.getByText("bob is not a member of 'crew'")).toBeTruthy(),
    );
    expect(window.location.hash).toBe("");
    expect(screen.getByText("bob")).toBeTruthy();
  });
});

describe("PeerPanel group ignore", () => {
  // Same body-recording fetch stub as the group-members block: the
  // ignore call's body ({ group }) is what proves the right group.
  function mockFetchBodies(
    handler: (url: string, method: string, body: unknown) => unknown,
  ) {
    const calls: { method: string; url: string; body: unknown }[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        const method = init?.method ?? "GET";
        let body: unknown = null;
        if (init?.body) {
          try {
            body = JSON.parse(String(init.body));
          } catch {
            body = null;
          }
        }
        calls.push({ method, url, body });
        const payload = await handler(url, method, body);
        return {
          ok: true,
          status: 200,
          text: async () => JSON.stringify(payload ?? {}),
        } as Response;
      }),
    );
    return { calls };
  }

  it("ignores the group after a confirm step, posting the group id", async () => {
    const { calls } = mockFetchBodies((url) => {
      if (url.includes("/api/groups/invite")) {
        return { mode: "open", generation: 0, link: null };
      }
      return { ok: true };
    });
    renderPanel(groupConv());
    await waitFor(() => expect(screen.getByText("Members")).toBeTruthy());
    expect(screen.queryByText("Un-ignore group")).toBeNull();
    fireEvent.click(screen.getByText("Ignore group"));
    fireEvent.click(screen.getByText("Confirm ignore group?"));
    await waitFor(() =>
      expect(
        calls.some(
          (c) =>
            c.method === "POST" &&
            c.url.includes("/api/groups/ignore") &&
            JSON.stringify(c.body) === JSON.stringify({ group: GROUP_ID }),
        ),
      ).toBe(true),
    );
  });

  it("offers Un-ignore instead of Ignore for an already-ignored group", async () => {
    const { calls } = mockFetchBodies((url) => {
      if (url.includes("/api/groups/invite")) {
        return { mode: "open", generation: 0, link: null };
      }
      return { ok: true };
    });
    renderPanel(groupConv({ ignored: true }));
    await waitFor(() => expect(screen.getByText("Members")).toBeTruthy());
    expect(screen.queryByText("Ignore group")).toBeNull();
    fireEvent.click(screen.getByText("Un-ignore group"));
    await waitFor(() =>
      expect(
        calls.some(
          (c) =>
            c.method === "POST" &&
            c.url.includes("/api/groups/unignore") &&
            JSON.stringify(c.body) === JSON.stringify({ group: GROUP_ID }),
        ),
      ).toBe(true),
    );
  });

  it("shows no ignore controls for a DM", async () => {
    mockFetch(() => ({ ok: true }));
    renderDmPanel(makePeer({ destination: PEER_HASH }));
    await waitFor(() => expect(screen.getByText("Public profile")).toBeTruthy());
    expect(screen.queryByText("Ignore group")).toBeNull();
    expect(screen.queryByText("Un-ignore group")).toBeNull();
  });
});
