// RightSidebar.test.tsx — the pending successor navigation: when a
// roster change rotates the group id but the refresh right after the
// change cannot load the successor, the sidebar remembers the successor
// id + origin route and follows it once a later snapshot contains it
// — but only if the user is still on that thread.

import { afterEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import RightSidebar from "./RightSidebar";
import { TauContext } from "./TauContext";
import { fakeTau, fakePeer } from "../sections/test-utils";
import type { Group } from "@/lib/types";

const SELF = "a".repeat(32);
const BOB = "b".repeat(32);
const CAROL = "c".repeat(32);
const GROUP_ID = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6";
const NEW_ID = "d".repeat(32);

function group(overrides: Partial<Group> = {}): Group {
  return {
    id: GROUP_ID,
    name: "crew",
    members: [
      { hash: SELF, name: "testnode" },
      { hash: BOB, name: "bob" },
    ],
    created_at: 1700000000,
    topic: "",
    created_by_me: true,
    can_leave: false,
    ignored: false,
    ...overrides,
  };
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
  vi.unstubAllGlobals();
  window.location.hash = "";
});

describe("RightSidebar pending successor navigation", () => {
  it("follows the successor once a later snapshot contains it", async () => {
    mockFetchBodies((url) => {
      if (url.includes("/api/groups/remove-member")) {
        return {
          group: { ...group(), id: NEW_ID },
          previous_id: GROUP_ID,
        };
      }
      return inviteOnly(url);
    });
    window.location.hash = `#chat?group=${GROUP_ID}`;
    const peers = [
      fakePeer({ destination: BOB, name: "bob" }),
      fakePeer({ destination: CAROL, name: "carol" }),
    ];
    // The refresh right after the change fails, so the successor is
    // not in any snapshot yet.
    const before = fakeTau({ peers, groups: [group()] });
    before.refresh = vi.fn(async () => null);
    const rendered = render(
      <TauContext.Provider value={before}>
        <RightSidebar />
      </TauContext.Provider>,
    );
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
    expect(window.location.hash).toBe(`#chat?group=${GROUP_ID}`);

    // A later snapshot finally contains the successor: the sidebar
    // follows it because the user never left the thread.
    const after = fakeTau({ peers, groups: [{ ...group(), id: NEW_ID }] });
    rendered.rerender(
      <TauContext.Provider value={after}>
        <RightSidebar />
      </TauContext.Provider>,
    );
    await waitFor(() =>
      expect(window.location.hash).toBe(`#chat?group=${NEW_ID}`),
    );
  });

  it("does not follow the successor once the user has left the thread", async () => {
    mockFetchBodies((url) => {
      if (url.includes("/api/groups/remove-member")) {
        return {
          group: { ...group(), id: NEW_ID },
          previous_id: GROUP_ID,
        };
      }
      return inviteOnly(url);
    });
    window.location.hash = `#chat?group=${GROUP_ID}`;
    const peers = [
      fakePeer({ destination: BOB, name: "bob" }),
      fakePeer({ destination: CAROL, name: "carol" }),
    ];
    const before = fakeTau({ peers, groups: [group()] });
    before.refresh = vi.fn(async () => null);
    const rendered = render(
      <TauContext.Provider value={before}>
        <RightSidebar />
      </TauContext.Provider>,
    );
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

    // The user leaves the thread before the successor arrives.
    window.location.hash = "#social";
    const after = fakeTau({ peers, groups: [{ ...group(), id: NEW_ID }] });
    rendered.rerender(
      <TauContext.Provider value={after}>
        <RightSidebar />
      </TauContext.Provider>,
    );
    // Let any effect run, then confirm the pending navigation was
    // dropped instead of yanking the user back to the thread.
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(window.location.hash).toBe("#social");
  });
});
