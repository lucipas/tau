// ChatSection.test.tsx — chat draft seeding: saving a draft must not
// pour it straight back into the composer (#609).

import { StrictMode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import ChatSection from "./ChatSection";
import { TauContext } from "../chrome/TauContext";
import { fakeTau, makeMessage, makePeer, makeUnread } from "../test-utils";
import type { MessageDraft, Reminder, Snapshot } from "@/lib/types";

const PEER_HASH = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6";
const NEW_GROUP_ID = "d".repeat(32);

let saved: MessageDraft[] = [];
let calls: string[] = [];
let reminders: Reminder[] = [];
let reminderSeq = 0;
/** Stored history a GET /api/messages?group=… answers with: the posts
 *  the snapshot hides while the group is ignored. */
let groupHistory: ReturnType<typeof makeMessage>[] = [];
/** When true, that history fetch answers 500, so a test can prove the
 *  thread shows a retryable error instead of an empty thread. */
let groupHistoryFails = false;

/** A deferred the next GET /api/reminders waits on: the test resolves it
 *  with a stale payload to prove an older response cannot overwrite
 *  newer state. */
function defer<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
let heldGet: ReturnType<typeof defer<unknown>> | null = null;
/** A deferred the next GET /api/messages?group=… waits on: the test
 *  resolves it to prove snapshot moves during an in-flight history
 *  fetch still let a result commit instead of starving the thread. */
let heldHistoryGet: ReturnType<typeof defer<unknown>> | null = null;

/** The next POST /api/groups/join answers with this payload, so a
 *  join test can stage a named or a label-free queued reply. */
let nextJoin: unknown = null;

/** Bridge stub with a stateful drafts list: a POSTed draft shows up in
 *  later GETs, like the real bridge once the save lands. Reminders behave
 *  the same way, except a GET past a reminder's due time no longer
 *  returns it — the serving node consumes it when it fires. */
function stubFetch() {
  calls = [];
  saved = [];
  reminders = [];
  reminderSeq = 0;
  groupHistory = [];
  groupHistoryFails = false;
  heldGet = null;
  heldHistoryGet = null;
  nextJoin = null;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = String(typeof input === "string" ? input : (input as Request).url ?? input);
      const method = init?.method ?? "GET";
      calls.push(`${method} ${url}`);
      let payload: unknown = { ok: true };
      if (url.includes("/api/reminders/cancel")) {
        const body = JSON.parse(String(init?.body ?? "{}"));
        const at = reminders.findIndex((r) => r.id.startsWith(body.id));
        const cancelled = at >= 0 ? reminders.splice(at, 1)[0] : null;
        payload = { ok: true, cancelled };
      } else if (url.includes("/api/reminders")) {
        if (method === "POST") {
          const body = JSON.parse(String(init?.body ?? "{}"));
          reminderSeq += 1;
          const now = Date.now() / 1000;
          const reminder: Reminder = {
            id: `r${reminderSeq}`.padEnd(16, "0"),
            message_id: "wire-1",
            kind: "peer",
            target: PEER_HASH,
            target_name: "Alice",
            text: "hello",
            sender: "Alice",
            due_at: now + (body.in_seconds ?? 3600),
            created_at: now,
          };
          reminders.push(reminder);
          payload = { ok: true, reminder };
        } else if (heldGet) {
          const held = heldGet;
          heldGet = null;
          const stale = await held.promise;
          return {
            ok: true,
            status: 200,
            text: async () => JSON.stringify(stale),
          } as Response;
        } else {
          const pending = reminders.filter((r) => r.due_at > Date.now() / 1000);
          payload = { reminders: pending, count: pending.length };
        }
      } else if (url.includes("/api/drafts/delete")) {
        payload = { ok: true, deleted: true };
      } else if (url.includes("/api/drafts")) {
        if (method === "POST") {
          const body = JSON.parse(String(init?.body ?? "{}"));
          const draft: MessageDraft = {
            kind: body.kind,
            target: body.target,
            text: body.text,
            updated_at: 1700000000,
            name: "Alice",
          };
          const at = saved.findIndex((d) => d.kind === draft.kind && d.target === draft.target);
          if (at >= 0) saved[at] = draft;
          else saved.push(draft);
          payload = { ok: true, draft };
        } else {
          payload = { drafts: saved, count: saved.length };
        }
      } else if (url.includes("/api/messages?group=")) {
        if (groupHistoryFails) {
          return {
            ok: false,
            status: 500,
            text: async () => JSON.stringify({ error: { message: "boom" } }),
          } as Response;
        }
        if (heldHistoryGet) {
          const held = heldHistoryGet;
          heldHistoryGet = null;
          // Honor the fetch's abort signal, like a real fetch: the
          // timeout test aborts the stalled request this way.
          const aborted = new Promise<never>((_resolve, reject) => {
            const signal = init?.signal;
            if (!signal) return;
            if (signal.aborted) {
              reject(new DOMException("The operation was aborted.", "AbortError"));
              return;
            }
            signal.addEventListener(
              "abort",
              () => reject(new DOMException("The operation was aborted.", "AbortError")),
              { once: true },
            );
          });
          const payload = await Promise.race([held.promise, aborted]);
          return {
            ok: true,
            status: 200,
            text: async () => JSON.stringify(payload),
          } as Response;
        }
        payload = { messages: groupHistory };
      } else if (url.includes("/api/messages/requests")) {
        payload = { requests: [], count: 0 };
      } else if (url.includes("/api/broadcasts")) {
        payload = { broadcasts: [], count: 0 };
      } else if (url.includes("/api/outbox/held")) {
        payload = { held: [], count: 0 };
      } else if (url.includes("/api/outbox")) {
        payload = { outbox: [], count: 0, held_for_others: 0 };
      } else if (url.includes("/api/messages/muted")) {
        payload = { peers: [], groups: [] };
      } else if (url.includes("/api/conversations/archived")) {
        payload = { archived: [] };
      } else if (url.includes("/api/beacons")) {
        payload = { beacons: [], count: 0 };
      } else if (url.includes("/api/groups/join")) {
        payload =
          nextJoin ?? { ok: true, status: "queued", group: "" };
      } else if (url.includes("/api/groups") && method === "POST") {
        payload = {
          ok: true,
          group: {
            id: NEW_GROUP_ID,
            name: "crew",
            members: [],
            created_at: 1700000000,
            topic: "",
            created_by_me: true,
            can_leave: true,
            ignored: false,
          },
        };
      } else if (url.includes("/api/messages/read")) {
        payload = {
          ok: true,
          unread: { per_peer: {}, per_group: {}, per_channel: {}, total: 0, revision: 1 },
        };
      } else if (url.includes("/api/messages/edit")) {
        payload = { ok: true, queued: true };
      } else if (url.includes("/api/messages/retract")) {
        payload = { ok: true, queued: true };
      }
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify(payload),
      } as Response;
    }),
  );
}

function renderChat() {
  window.location.hash = `#chat?peer=${PEER_HASH}`;
  render(
    <TauContext.Provider
      value={fakeTau({
        peers: [makePeer({ destination: PEER_HASH, name: "Alice" })],
        messages: [makeMessage({ id: 1, peer: PEER_HASH, text: "hello" })],
      })}
    >
      <ChatSection />
    </TauContext.Provider>,
  );
}

beforeEach(() => {
  stubFetch();
});

afterEach(() => {
  vi.unstubAllGlobals();
  window.location.hash = "";
});

describe("ChatSection drafts", () => {
  it("saving a draft does not pour it back into the composer", async () => {
    renderChat();
    const box = (await screen.findByLabelText("Message")) as HTMLTextAreaElement;
    fireEvent.change(box, { target: { value: "park this for later" } });
    fireEvent.click(screen.getByRole("button", { name: "Save draft" }));
    await waitFor(() => expect(calls).toContain("POST /api/drafts"));
    // The box clears once the save lands…
    await waitFor(() => expect(box.value).toBe(""));
    // …and stays empty once the drafts refresh lands the just-saved
    // draft back in the list (visible in the drafts panel).
    await waitFor(() => expect(screen.getByText("park this for later")).toBeTruthy());
    expect(box.value).toBe("");
  });

  it("opening a conversation with a saved draft still seeds the composer", async () => {
    saved = [
      {
        kind: "peer",
        target: PEER_HASH,
        text: "resume me",
        updated_at: 1700000000,
        name: "Alice",
      },
    ];
    renderChat();
    const box = (await screen.findByLabelText("Message")) as HTMLTextAreaElement;
    await waitFor(() => expect(box.value).toBe("resume me"));
  });
});

describe("ChatSection reminders", () => {
  it("snoozing a message adds a Reminders row, and cancelling removes it", async () => {
    renderChat();
    await waitFor(() => expect(screen.queryByLabelText("Reminders")).toBeNull());
    fireEvent.click(screen.getByLabelText("Actions for message 1"));
    fireEvent.click(screen.getByText("Snooze"));
    fireEvent.click(screen.getByText("30 min"));
    await waitFor(() => expect(calls).toContain("POST /api/reminders"));
    await waitFor(() => expect(screen.getByText("Reminders (1)")).toBeTruthy());
    fireEvent.click(screen.getByLabelText("Cancel reminder for Alice"));
    await waitFor(() => expect(calls).toContain("POST /api/reminders/cancel"));
    await waitFor(() => expect(screen.queryByLabelText("Reminders")).toBeNull());
  });

  it("a fired reminder clears shortly after its due time", async () => {
    const now = Date.now() / 1000;
    reminders = [
      {
        id: "r1".padEnd(16, "0"),
        message_id: "wire-1",
        kind: "peer",
        target: PEER_HASH,
        target_name: "Alice",
        text: "hello",
        sender: "Alice",
        due_at: now + 2,
        created_at: now,
      },
    ];
    renderChat();
    await waitFor(() => expect(screen.getByText("Reminders (1)")).toBeTruthy());
    // The node consumes the reminder at its due time; the sidebar
    // refreshes just after and the row goes away on its own. The
    // refresh timer floors at 5s, so this test needs a longer timeout.
    await waitFor(() => expect(screen.queryByLabelText("Reminders")).toBeNull(), {
      timeout: 10000,
    });
  }, 15000);

  it("reloads reminders when the unread total moves", async () => {
    const tau = fakeTau({
      peers: [makePeer({ destination: PEER_HASH, name: "Alice" })],
      messages: [makeMessage({ id: 1, peer: PEER_HASH, text: "hello" })],
      unread: makeUnread({ total: 0, revision: 1 }),
    });
    window.location.hash = `#chat?peer=${PEER_HASH}`;
    const { rerender } = render(
      <TauContext.Provider value={tau}>
        <ChatSection />
      </TauContext.Provider>,
    );
    await waitFor(() => expect(calls.filter((c) => c === "GET /api/reminders").length).toBeGreaterThanOrEqual(1));
    const before = calls.filter((c) => c === "GET /api/reminders").length;
    rerender(
      <TauContext.Provider
        value={fakeTau({
          peers: [makePeer({ destination: PEER_HASH, name: "Alice" })],
          messages: [makeMessage({ id: 1, peer: PEER_HASH, text: "hello" })],
          unread: makeUnread({ total: 1, revision: 2 }),
        })}
      >
        <ChatSection />
      </TauContext.Provider>,
    );
    await waitFor(() =>
      expect(calls.filter((c) => c === "GET /api/reminders").length).toBeGreaterThan(before),
    );
  });

  it("a slow list response cannot restore a row a newer refresh removed", async () => {
    const now = Date.now() / 1000;
    const stale: Reminder = {
      id: "r1".padEnd(16, "0"),
      message_id: "wire-1",
      kind: "peer",
      target: PEER_HASH,
      target_name: "Alice",
      text: "hello",
      sender: "Alice",
      due_at: now + 3600,
      created_at: now,
    };
    reminders = [stale];
    window.location.hash = `#chat?peer=${PEER_HASH}`;
    const { rerender } = render(
      <TauContext.Provider
        value={fakeTau({
          peers: [makePeer({ destination: PEER_HASH, name: "Alice" })],
          messages: [makeMessage({ id: 1, peer: PEER_HASH, text: "hello" })],
          unread: makeUnread({ total: 0, revision: 1 }),
        })}
      >
        <ChatSection />
      </TauContext.Provider>,
    );
    await waitFor(() => expect(screen.getByText("Reminders (1)")).toBeTruthy());
    const getsBefore = calls.filter((c) => c === "GET /api/reminders").length;
    // The cancel-triggered refresh stalls on a held GET; a snapshot
    // refresh overtakes it and clears the row first. Keep the deferred
    // in a local: the stub nulls the shared handle when it takes it.
    const held = defer<unknown>();
    heldGet = held;
    fireEvent.click(screen.getByLabelText("Cancel reminder for Alice"));
    await waitFor(() => expect(calls).toContain("POST /api/reminders/cancel"));
    // The stalled refresh must have started before the snapshot one
    // overtakes it, or the generations would order the other way.
    await waitFor(() =>
      expect(calls.filter((c) => c === "GET /api/reminders").length).toBeGreaterThan(getsBefore),
    );
    // The row is still there while the stalled refresh is pending —
    // nothing has answered the cancel yet.
    expect(screen.getByText("Reminders (1)")).toBeTruthy();
    rerender(
      <TauContext.Provider
        value={fakeTau({
          peers: [makePeer({ destination: PEER_HASH, name: "Alice" })],
          messages: [makeMessage({ id: 1, peer: PEER_HASH, text: "hello" })],
          unread: makeUnread({ total: 1, revision: 2 }),
        })}
      >
        <ChatSection />
      </TauContext.Provider>,
    );
    await waitFor(() => expect(screen.queryByLabelText("Reminders")).toBeNull());
    // The stalled response finally lands with the canceled row in it —
    // the generation guard drops it instead of restoring the row.
    held.resolve({ reminders: [stale], count: 1 });
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(screen.queryByLabelText("Reminders")).toBeNull();
  });
});

describe("ChatSection queued edit notice", () => {
  const BOB_HASH = "b".repeat(32);

  function renderTwoPeers() {
    window.location.hash = `#chat?peer=${PEER_HASH}`;
    render(
      <TauContext.Provider
        value={fakeTau({
          peers: [
            makePeer({ destination: PEER_HASH, name: "Alice" }),
            makePeer({ destination: BOB_HASH, name: "Bob" }),
          ],
          messages: [
            makeMessage({ id: 1, peer: PEER_HASH, direction: "out", text: "hello" }),
            makeMessage({ id: 2, peer: BOB_HASH, direction: "in", text: "hi" }),
          ],
        })}
      >
        <ChatSection />
      </TauContext.Provider>,
    );
  }

  function switchTo(peerHash: string) {
    window.location.hash = `#chat?peer=${peerHash}`;
    fireEvent(window, new Event("hashchange"));
  }

  it("shows the notice only in the conversation it was queued from", async () => {
    renderTwoPeers();
    // Open the message menu and start editing our own sent message.
    fireEvent.click(screen.getByLabelText("Actions for message 1"));
    fireEvent.click(screen.getByRole("button", { name: "Edit" }));
    const box = (await screen.findByLabelText("Message")) as HTMLTextAreaElement;
    await waitFor(() => expect(box.value).toBe("hello"));
    fireEvent.change(box, { target: { value: "hello edited" } });
    fireEvent.click(screen.getByRole("button", { name: "Save edit" }));
    // The edit queues (peer offline): the notice shows in Alice's chat.
    await waitFor(() => expect(screen.getByText(/Edit queued/)).toBeTruthy());
    // Switching to Bob's chat hides it…
    switchTo(BOB_HASH);
    await waitFor(() => expect(screen.queryByText(/Edit queued/)).toBeNull());
    // …and switching back shows it again — it belongs to Alice's chat.
    switchTo(PEER_HASH);
    await waitFor(() => expect(screen.getByText(/Edit queued/)).toBeTruthy());
  });
});

describe("ChatSection queued retract notice", () => {
  const BOB_HASH = "b".repeat(32);

  function renderTwoPeers() {
    window.location.hash = `#chat?peer=${PEER_HASH}`;
    render(
      <TauContext.Provider
        value={fakeTau({
          peers: [
            makePeer({ destination: PEER_HASH, name: "Alice" }),
            makePeer({ destination: BOB_HASH, name: "Bob" }),
          ],
          messages: [
            makeMessage({ id: 1, peer: PEER_HASH, direction: "out", text: "hello" }),
            makeMessage({ id: 2, peer: BOB_HASH, direction: "in", text: "hi" }),
          ],
        })}
      >
        <ChatSection />
      </TauContext.Provider>,
    );
  }

  function switchTo(peerHash: string) {
    window.location.hash = `#chat?peer=${peerHash}`;
    fireEvent(window, new Event("hashchange"));
  }

  it("shows the notice only in the conversation it was queued from", async () => {
    renderTwoPeers();
    // Open the message menu and retract our own sent message.
    fireEvent.click(screen.getByLabelText("Actions for message 1"));
    fireEvent.click(screen.getByRole("button", { name: "Retract" }));
    // The retract queues (peer offline): the notice shows in Alice's chat.
    await waitFor(() => expect(screen.getByText(/Retraction queued/)).toBeTruthy());
    // Switching to Bob's chat hides it…
    switchTo(BOB_HASH);
    await waitFor(() => expect(screen.queryByText(/Retraction queued/)).toBeNull());
    // …and switching back shows it again — it belongs to Alice's chat.
    switchTo(PEER_HASH);
    await waitFor(() => expect(screen.getByText(/Retraction queued/)).toBeTruthy());
  });
});

describe("ChatSection announcements-only", () => {
  it("locks the composer for a member of an announcements-only group", async () => {
    const GROUP_ID = "c".repeat(32);
    window.location.hash = `#chat?group=${GROUP_ID}`;
    render(
      <TauContext.Provider
        value={fakeTau({
          groups: [
            {
              id: GROUP_ID,
              name: "crew",
              members: [{ hash: "b".repeat(32), name: "bob" }],
              created_at: 1700000000,
              topic: "",
              announcements_only: true,
              created_by_me: false,
              can_leave: true,
              ignored: false,
            },
          ],
          messages: [
            makeMessage({ id: 1, group: GROUP_ID, text: "announcement" }),
          ],
        })}
      >
        <ChatSection />
      </TauContext.Provider>,
    );
    const box = (await screen.findByLabelText("Message")) as HTMLTextAreaElement;
    expect(box.disabled).toBe(true);
    expect(screen.getByText(/Only the creator can post here/)).toBeTruthy();
  });

  it("leaves the composer open for the creator of an announcements-only group", async () => {
    const GROUP_ID = "c".repeat(32);
    window.location.hash = `#chat?group=${GROUP_ID}`;
    render(
      <TauContext.Provider
        value={fakeTau({
          groups: [
            {
              id: GROUP_ID,
              name: "crew",
              members: [{ hash: "b".repeat(32), name: "bob" }],
              created_at: 1700000000,
              topic: "",
              announcements_only: true,
              created_by_me: true,
              can_leave: false,
              ignored: false,
            },
          ],
          messages: [
            makeMessage({ id: 1, group: GROUP_ID, text: "announcement" }),
          ],
        })}
      >
        <ChatSection />
      </TauContext.Provider>,
    );
    const box = (await screen.findByLabelText("Message")) as HTMLTextAreaElement;
    expect(box.disabled).toBe(false);
    expect(screen.queryByText(/Only the creator can post here/)).toBeNull();
  });
});

describe("ChatSection new group", () => {
  const BOB_HASH = "b".repeat(32);

  it("creates a group and opens its thread", async () => {
    window.location.hash = "#chat";
    render(
      <TauContext.Provider
        value={fakeTau({
          peers: [
            makePeer({ destination: PEER_HASH, name: "Alice" }),
            makePeer({ destination: BOB_HASH, name: "Bob" }),
          ],
          messages: [],
        })}
      >
        <ChatSection />
      </TauContext.Provider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "New group" }));
    fireEvent.change(screen.getByLabelText("Group name"), {
      target: { value: "crew" },
    });
    fireEvent.click(screen.getByLabelText("Bob"));
    fireEvent.click(screen.getByRole("button", { name: "Create group" }));
    await waitFor(() => expect(calls).toContain("POST /api/groups"));
    await waitFor(() =>
      expect(window.location.hash).toBe(`#chat?group=${NEW_GROUP_ID}`),
    );
  });

  it("navigates to the new group even when the refresh fails", async () => {
    // Parity with the join flow: refresh, then navigate whether or
    // not the refresh landed — the next snapshot picks the group up,
    // so a failed refresh must not look like a failed creation.
    window.location.hash = "#chat";
    render(
      <TauContext.Provider
        value={fakeTau({
          peers: [
            makePeer({ destination: PEER_HASH, name: "Alice" }),
            makePeer({ destination: BOB_HASH, name: "Bob" }),
          ],
          messages: [],
          refresh: vi.fn(async () => {
            throw new Error("bridge down");
          }),
        })}
      >
        <ChatSection />
      </TauContext.Provider>,
    );
    fireEvent.click(screen.getByRole("button", { name: "New group" }));
    fireEvent.change(screen.getByLabelText("Group name"), {
      target: { value: "crew" },
    });
    fireEvent.click(screen.getByLabelText("Bob"));
    fireEvent.click(screen.getByRole("button", { name: "Create group" }));
    await waitFor(() =>
      expect(window.location.hash).toBe(`#chat?group=${NEW_GROUP_ID}`),
    );
  });
});

describe("ChatSection join group", () => {
  function renderJoin() {
    window.location.hash = "#chat";
    render(
      <TauContext.Provider
        value={fakeTau({
          peers: [makePeer({ destination: PEER_HASH, name: "Alice" })],
          messages: [],
        })}
      >
        <ChatSection />
      </TauContext.Provider>,
    );
  }

  function pasteLinkAndJoin() {
    fireEvent.click(screen.getByRole("button", { name: "Join a group" }));
    fireEvent.change(
      screen.getByLabelText("Paste an invite link to join a group"),
      { target: { value: "tau://invite/abc" } },
    );
    fireEvent.click(screen.getByRole("button", { name: "Join" }));
  }

  it("names the group when the queued reply carries one", async () => {
    nextJoin = { ok: true, status: "queued", group: "remote-crew" };
    renderJoin();
    pasteLinkAndJoin();
    await waitFor(() =>
      expect(
        screen.getByText(/Join request for “remote-crew” queued/),
      ).toBeTruthy(),
    );
  });

  it("shows a label-free message when the queued reply names no group", async () => {
    // A sealed link carries no group label: the queued reply's
    // group is empty, and the banner must not render empty quotes.
    nextJoin = { ok: true, status: "queued", group: "" };
    renderJoin();
    pasteLinkAndJoin();
    await waitFor(() =>
      expect(
        screen.getByText(
          "Join request queued — it will be decided when it reaches the creator.",
        ),
      ).toBeTruthy(),
    );
    expect(screen.queryByText(/“”/)).toBeNull();
  });
});

describe("ChatSection ignored groups", () => {
  const GROUP_ID = "g".repeat(32);

  function ignoredGroupUi(
    messages: ReturnType<typeof makeMessage>[] = [],
    opts: {
      ignored?: boolean;
      refresh?: () => Promise<Snapshot | null>;
    } = {},
  ) {
    return (
      <TauContext.Provider
        value={fakeTau({
          peers: [makePeer({ destination: PEER_HASH, name: "Alice" })],
          // The snapshot hides ignored posts, so the thread starts empty.
          messages,
          groups: [
            {
              id: GROUP_ID,
              name: "crew",
              members: [],
              created_at: 1700000000,
              topic: "",
              created_by_me: true,
              can_leave: true,
              ignored: opts.ignored ?? true,
            },
          ],
          ...(opts.refresh ? { refresh: opts.refresh } : {}),
        })}
      >
        <ChatSection />
      </TauContext.Provider>
    );
  }

  function renderIgnoredGroup(messages: ReturnType<typeof makeMessage>[] = []) {
    window.location.hash = `#chat?group=${GROUP_ID}`;
    return render(ignoredGroupUi(messages));
  }

  const groupHistoryUrl = `GET /api/messages?group=${GROUP_ID}&include_ignored=1&limit=200`;

  it("opening an ignored group fetches its history into the thread", async () => {
    groupHistory = [
      makeMessage({
        id: 2,
        group: GROUP_ID,
        group_name: "crew",
        peer: PEER_HASH,
        peer_name: "Alice",
        text: "crew history",
      }),
    ];
    renderIgnoredGroup();
    await waitFor(() => expect(calls).toContain(groupHistoryUrl));
    await waitFor(() => expect(screen.getByText("crew history")).toBeTruthy());
  });

  it("re-fetches history when the snapshot's messages move", async () => {
    groupHistory = [
      makeMessage({
        id: 2,
        group: GROUP_ID,
        group_name: "crew",
        peer: PEER_HASH,
        peer_name: "Alice",
        text: "crew history",
      }),
    ];
    const { rerender } = renderIgnoredGroup();
    await waitFor(() => expect(calls).toContain(groupHistoryUrl));
    // A new DM lands: the snapshot moves, so the open ignored thread
    // re-fetches — a new post to the group must not stay invisible.
    rerender(
      <TauContext.Provider
        value={fakeTau({
          peers: [makePeer({ destination: PEER_HASH, name: "Alice" })],
          messages: [makeMessage({ id: 1, peer: PEER_HASH, text: "hello" })],
          groups: [
            {
              id: GROUP_ID,
              name: "crew",
              members: [],
              created_at: 1700000000,
              topic: "",
              created_by_me: true,
              can_leave: true,
              ignored: true,
            },
          ],
        })}
      >
        <ChatSection />
      </TauContext.Provider>,
    );
    await waitFor(
      () =>
        expect(
          calls.filter((c) => c === groupHistoryUrl).length,
        ).toBeGreaterThanOrEqual(2),
    );
  });

  it("rapid snapshot moves during an in-flight fetch still commit a result", async () => {
    groupHistory = [
      makeMessage({
        id: 2,
        group: GROUP_ID,
        group_name: "crew",
        peer: PEER_HASH,
        peer_name: "Alice",
        text: "crew history",
      }),
    ];
    // Hold the first history fetch so snapshot moves land mid-flight.
    const held = defer();
    heldHistoryGet = held;
    const { rerender } = renderIgnoredGroup();
    await waitFor(() => expect(calls).toContain(groupHistoryUrl));
    // Three snapshot moves while the fetch is in flight: before the fix
    // each bumped the seq and starved the thread — no response could
    // commit. Now they coalesce: no new fetch starts mid-flight.
    for (let i = 1; i <= 3; i++) {
      rerender(
        ignoredGroupUi([
          makeMessage({ id: 10 + i, peer: PEER_HASH, text: `dm ${i}` }),
        ]),
      );
    }
    expect(calls.filter((c) => c === groupHistoryUrl).length).toBe(1);
    held.resolve({ messages: groupHistory });
    await waitFor(() => expect(screen.getByText("crew history")).toBeTruthy());
    // Exactly one follow-up fetch ran for the moves that landed mid-flight.
    await waitFor(
      () =>
        expect(
          calls.filter((c) => c === groupHistoryUrl).length,
        ).toBe(2),
    );
  });

  it("a failed history load shows a retryable error, not an empty thread", async () => {
    groupHistoryFails = true;
    groupHistory = [
      makeMessage({
        id: 2,
        group: GROUP_ID,
        group_name: "crew",
        peer: PEER_HASH,
        peer_name: "Alice",
        text: "crew history",
      }),
    ];
    renderIgnoredGroup();
    await waitFor(() =>
      expect(
        screen.getByText("Couldn't load this group's history."),
      ).toBeTruthy(),
    );
    expect(screen.queryByText("crew history")).toBeNull();
    // The retry re-fetches, and the history lands once the fetch works.
    groupHistoryFails = false;
    fireEvent.click(screen.getByRole("button", { name: "Retry" }));
    await waitFor(() => expect(screen.getByText("crew history")).toBeTruthy());
    expect(
      screen.queryByText("Couldn't load this group's history."),
    ).toBeNull();
  });

  it("StrictMode remount still loads history for a directly linked ignored group", async () => {
    groupHistory = [
      makeMessage({
        id: 2,
        group: GROUP_ID,
        group_name: "crew",
        peer: PEER_HASH,
        peer_name: "Alice",
        text: "crew history",
      }),
    ];
    // A direct link opens the ignored group at mount. StrictMode runs
    // the mount effects, then their cleanup, then the effects again on
    // the same instance: the cleanup must reset all of the coalescing
    // state, or the fresh request queues behind the invalidated one
    // and history never appears.
    window.location.hash = `#chat?group=${GROUP_ID}`;
    render(<StrictMode>{ignoredGroupUi()}</StrictMode>);
    await waitFor(() => expect(screen.getByText("crew history")).toBeTruthy());
    expect(calls.filter((c) => c === groupHistoryUrl).length).toBe(2);
  });

  it("a stalled history fetch times out instead of blocking refresh", async () => {
    groupHistory = [
      makeMessage({
        id: 2,
        group: GROUP_ID,
        group_name: "crew",
        peer: PEER_HASH,
        peer_name: "Alice",
        text: "crew history",
      }),
    ];
    // Hold the first history fetch so it stalls past the timeout; the
    // stub rejects the held request when its abort signal fires.
    const held = defer();
    heldHistoryGet = held;
    const { rerender } = renderIgnoredGroup();
    await waitFor(() => expect(calls).toContain(groupHistoryUrl));
    // A snapshot move lands while the fetch is stalled: queued, not started.
    rerender(
      ignoredGroupUi([
        makeMessage({ id: 11, peer: PEER_HASH, text: "dm 1" }),
      ]),
    );
    expect(calls.filter((c) => c === groupHistoryUrl).length).toBe(1);
    // Hold the follow-up too, so the timeout's retryable error stays
    // visible instead of flashing past before the assertion polls.
    const heldFollowUp = defer();
    heldHistoryGet = heldFollowUp;
    await waitFor(
      () =>
        expect(
          screen.getByText("Couldn't load this group's history."),
        ).toBeTruthy(),
      { timeout: 17000 },
    );
    // The queued move ran its follow-up fetch once the stalled one
    // timed out — the thread is unblocked.
    await waitFor(
      () =>
        expect(
          calls.filter((c) => c === groupHistoryUrl).length,
        ).toBe(2),
    );
    heldFollowUp.resolve({ messages: groupHistory });
    await waitFor(() => expect(screen.getByText("crew history")).toBeTruthy());
    expect(
      screen.queryByText("Couldn't load this group's history."),
    ).toBeNull();
  }, 25000);

  it("no follow-up fetch fires after the chat unmounts", async () => {
    groupHistory = [
      makeMessage({
        id: 2,
        group: GROUP_ID,
        group_name: "crew",
        peer: PEER_HASH,
        peer_name: "Alice",
        text: "crew history",
      }),
    ];
    const held = defer();
    heldHistoryGet = held;
    const { rerender, unmount } = renderIgnoredGroup();
    await waitFor(() => expect(calls).toContain(groupHistoryUrl));
    // A snapshot move queues a follow-up while the fetch is in flight.
    rerender(
      ignoredGroupUi([
        makeMessage({ id: 11, peer: PEER_HASH, text: "dm 1" }),
      ]),
    );
    unmount();
    held.resolve({ messages: groupHistory });
    // Let the settled fetch's finally run: without the unmount cleanup
    // it would start a follow-up fetch for a chat that no longer exists.
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(calls.filter((c) => c === groupHistoryUrl).length).toBe(1);
  });

  it("a history fetch that started before un-ignore cannot repopulate the thread", async () => {
    groupHistory = [
      makeMessage({
        id: 2,
        group: GROUP_ID,
        group_name: "crew",
        peer: PEER_HASH,
        peer_name: "Alice",
        text: "crew history",
      }),
    ];
    // Hold the first history fetch so it is still in flight at un-ignore.
    const held = defer();
    heldHistoryGet = held;
    const { rerender } = renderIgnoredGroup();
    await waitFor(() => expect(calls).toContain(groupHistoryUrl));
    // Un-ignore while the fetch is in flight: the snapshot has not landed
    // yet, so the thread holds its (empty) history.
    rerender(ignoredGroupUi([], { ignored: false }));
    // The late fetch resolves after the un-ignore: its sequence was
    // invalidated, so it must not commit its older response.
    held.resolve({ messages: groupHistory });
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(screen.queryByText("crew history")).toBeNull();
  });

  it("re-ignoring a held thread keeps its history when the stale refresh lands", async () => {
    groupHistory = [
      makeMessage({
        id: 2,
        group: GROUP_ID,
        group_name: "crew",
        peer: PEER_HASH,
        peer_name: "Alice",
        text: "crew history",
      }),
    ];
    const { rerender } = renderIgnoredGroup();
    await waitFor(() => expect(screen.getByText("crew history")).toBeTruthy());
    // Un-ignore with the refresh held open, so it is still pending when
    // the group is re-ignored.
    const heldRefresh = defer<Snapshot | null>();
    rerender(
      ignoredGroupUi([], {
        ignored: false,
        refresh: () => heldRefresh.promise,
      }),
    );
    // Re-ignore before the refresh lands: a fresh history fetch starts,
    // held so the stale refresh lands first.
    const heldFetch = defer();
    heldHistoryGet = heldFetch;
    rerender(
      ignoredGroupUi([], {
        ignored: true,
        refresh: () => heldRefresh.promise,
      }),
    );
    await waitFor(() =>
      expect(calls.filter((c) => c === groupHistoryUrl).length).toBe(2),
    );
    // The stale un-ignore refresh lands after the re-ignore: it must not
    // clear the history the fresh fetch is about to load.
    heldRefresh.resolve({} as unknown as Snapshot);
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(screen.getByText("crew history")).toBeTruthy();
    // The fresh fetch lands and the history is still there.
    heldFetch.resolve({ messages: groupHistory });
    await waitFor(() =>
      expect(screen.getByText("crew history")).toBeTruthy(),
    );
  });

  it("a failed un-ignore refresh releases the held history once a snapshot lands", async () => {
    groupHistory = [
      makeMessage({
        id: 2,
        group: GROUP_ID,
        group_name: "crew",
        peer: PEER_HASH,
        peer_name: "Alice",
        text: "crew history",
      }),
    ];
    const { rerender } = renderIgnoredGroup();
    await waitFor(() => expect(screen.getByText("crew history")).toBeTruthy());
    // Un-ignore while the snapshot still hides the posts: the default
    // refresh answers null (bridge down), so the hold stays in place.
    rerender(ignoredGroupUi([], { ignored: false }));
    expect(screen.getByText("crew history")).toBeTruthy();
    // A later snapshot lands carrying the group's posts: the hold
    // releases even though the refresh failed, so the stale held posts
    // give way to the snapshot.
    rerender(
      ignoredGroupUi(
        [
          makeMessage({
            id: 3,
            group: GROUP_ID,
            group_name: "crew",
            peer: PEER_HASH,
            peer_name: "Alice",
            text: "snapshot post",
          }),
        ],
        { ignored: false },
      ),
    );
    await waitFor(() =>
      expect(screen.getAllByText("snapshot post").length).toBeGreaterThanOrEqual(1),
    );
    expect(screen.queryByText("crew history")).toBeNull();
  });
});
