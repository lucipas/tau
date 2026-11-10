import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type { TauMessage } from "./types";
import { useTau } from "./use-tau";

vi.mock("./api", () => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
  eventsUrl: vi.fn(() => "/api/events?since=0"),
}));

import { apiGet } from "./api";

const mockedGet = apiGet as Mock;

function makeMessage(overrides: Partial<TauMessage> = {}): TauMessage {
  return {
    id: 1,
    direction: "in",
    peer: "ab".repeat(16),
    peer_name: "alice",
    text: "hello from the mesh",
    sent_at: 1000,
    received_at: 1001,
    status: "received",
    ...overrides,
  };
}

// The hook opens a live event stream on mount; the tests only exercise
// search, so a stub that accepts listeners and never fires is enough.
class StubEventSource {
  listeners: Record<string, Array<(event: Event) => void>> = {};

  constructor(_url: string) {}

  addEventListener(type: string, listener: (event: Event) => void) {
    (this.listeners[type] ??= []).push(listener);
  }

  removeEventListener(type: string, listener: (event: Event) => void) {
    this.listeners[type] = (this.listeners[type] ?? []).filter(
      (entry) => entry !== listener,
    );
  }

  close() {}
}

beforeEach(() => {
  vi.resetAllMocks();
  vi.stubGlobal("EventSource", StubEventSource);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("useTau search", () => {
  it("fetches the search endpoint and shows the results", async () => {
    const hit = makeMessage({ id: 2, text: "mesh hit" });
    mockedGet.mockResolvedValue({ messages: [hit], count: 1 });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.search("mesh");
    });

    expect(mockedGet).toHaveBeenCalledWith(
      "/api/messages/search?q=mesh&limit=100",
    );
    expect(result.current.searchQuery).toBe("mesh");
    expect(result.current.searchResults).toEqual([hit]);
    expect(result.current.searchError).toBeNull();
  });

  it("resubmitting the same query refreshes instead of clearing", async () => {
    const first = makeMessage({ id: 2, text: "mesh hit" });
    const refreshed = makeMessage({ id: 3, text: "mesh hit, again" });
    mockedGet
      .mockResolvedValueOnce({ messages: [first], count: 1 })
      .mockResolvedValueOnce({ messages: [refreshed], count: 1 });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.search("mesh");
    });
    expect(result.current.searchResults).toEqual([first]);

    await act(async () => {
      await result.current.search("mesh");
    });

    expect(mockedGet).toHaveBeenCalledTimes(2);
    // The refresh replaces the results; it never flashes an empty list.
    expect(result.current.searchResults).toEqual([refreshed]);
    expect(result.current.searchQuery).toBe("mesh");
  });

  it("surfaces a search failure and keeps the previous results", async () => {
    const hit = makeMessage({ id: 2, text: "mesh hit" });
    mockedGet
      .mockResolvedValueOnce({ messages: [hit], count: 1 })
      .mockRejectedValueOnce(new Error("bridge unreachable"));

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.search("mesh");
    });
    expect(result.current.searchResults).toEqual([hit]);

    await act(async () => {
      await result.current.search("mesh");
    });

    expect(result.current.searchError).toBe("bridge unreachable");
    expect(result.current.searchResults).toEqual([hit]);
    expect(result.current.searchQuery).toBe("mesh");
  });

  it("clearing the search returns to the live feed", async () => {
    const hit = makeMessage({ id: 2, text: "mesh hit" });
    mockedGet.mockResolvedValue({ messages: [hit], count: 1 });

    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.search("mesh");
    });
    expect(result.current.searchQuery).toBe("mesh");

    act(() => {
      result.current.clearSearch();
    });

    expect(result.current.searchQuery).toBeNull();
    expect(result.current.searchResults).toEqual([]);
    expect(result.current.searchError).toBeNull();
  });

  it("ignores a blank query", async () => {
    const { result } = renderHook(() => useTau());

    await act(async () => {
      await result.current.search("   ");
    });

    expect(mockedGet).not.toHaveBeenCalled();
    expect(result.current.searchQuery).toBeNull();
  });
});
