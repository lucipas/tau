// ChatSection.test.tsx — chat draft seeding: saving a draft must not
// pour it straight back into the composer (#609).

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import ChatSection from "./ChatSection";
import { TauContext } from "../chrome/TauContext";
import { fakeTau, makeMessage, makePeer } from "../test-utils";
import type { MessageDraft } from "@/lib/types";

const PEER_HASH = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6";

let saved: MessageDraft[] = [];
let calls: string[] = [];

/** Bridge stub with a stateful drafts list: a POSTed draft shows up in
 *  later GETs, like the real bridge once the save lands. */
function stubFetch() {
  calls = [];
  saved = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = String(typeof input === "string" ? input : (input as Request).url ?? input);
      const method = init?.method ?? "GET";
      calls.push(`${method} ${url}`);
      let payload: unknown = { ok: true };
      if (url.includes("/api/drafts/delete")) {
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
      } else if (url.includes("/api/messages/read")) {
        payload = {
          ok: true,
          unread: { per_peer: {}, per_group: {}, per_channel: {}, total: 0, revision: 1 },
        };
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
