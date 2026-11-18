// PeerPanel.test.tsx — the group invite-link section: shown only for
// groups this node created, with create / copy / mode / revoke.

import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import PeerPanel from "./PeerPanel";
import { TauContext } from "../chrome/TauContext";
import { fakeTau, mockFetch } from "../sections/test-utils";
import type { Conversation } from "@/lib/use-tau";
import type { Group } from "@/lib/types";

const GROUP_ID = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6";

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
  };
}

function renderPanel(conversation: Conversation) {
  return render(
    <TauContext.Provider value={fakeTau()}>
      <PeerPanel conversation={conversation} />
    </TauContext.Provider>,
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
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
