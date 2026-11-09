import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type { TauGroup } from "./types";
import { useTau } from "./use-tau";

vi.mock("./api", () => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
  eventsUrl: () => "http://localhost/api/events?since=0",
}));

import { apiGet, apiPost } from "./api";

const mockedGet = apiGet as Mock;
const mockedPost = apiPost as Mock;

class StubEventSource {
  addEventListener() {}
  removeEventListener() {}
  close() {}
}

function makeGroup(overrides: Partial<TauGroup> = {}): TauGroup {
  return {
    id: "11".repeat(16),
    name: "weekend",
    members: [
      { hash: "ab".repeat(16), name: "alice" },
      { hash: "ef".repeat(16), name: "tester" },
    ],
    created_at: 1000,
    ...overrides,
  };
}

beforeEach(() => {
  vi.resetAllMocks();
  vi.stubGlobal("EventSource", StubEventSource);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("useTau group management", () => {
  it("shows a created group even when the follow-up refresh fails", async () => {
    const created = makeGroup();
    mockedPost.mockResolvedValueOnce({ ok: true, group: created });
    mockedGet.mockRejectedValueOnce(new Error("refresh failed"));

    const { result } = renderHook(() => useTau());
    let returned: TauGroup | undefined;
    await act(async () => {
      returned = await result.current.createGroup("weekend", [
        "ab".repeat(16),
      ]);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/groups", {
      name: "weekend",
      members: ["ab".repeat(16)],
    });
    expect(returned).toEqual(created);
    expect(result.current.groups).toEqual([created]);
  });

  it("drops a deleted group even when the follow-up refresh fails", async () => {
    const created = makeGroup();
    mockedPost.mockResolvedValueOnce({ ok: true, group: created });
    mockedGet.mockRejectedValueOnce(new Error("refresh failed"));

    const { result } = renderHook(() => useTau());
    await act(async () => {
      await result.current.createGroup("weekend", ["ab".repeat(16)]);
    });
    expect(result.current.groups).toEqual([created]);

    mockedPost.mockResolvedValueOnce({
      ok: true,
      deleted: { id: created.id, name: "weekend" },
    });
    mockedGet.mockRejectedValueOnce(new Error("refresh failed"));
    await act(async () => {
      await result.current.deleteGroup(created.id);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/groups/delete", {
      group: created.id,
    });
    expect(result.current.groups).toEqual([]);
  });
});
