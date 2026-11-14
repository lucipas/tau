import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type { TanScheduledPost, TanScheduledResponse } from "./types";
import { useTan } from "./use-tan";

vi.mock("./api", () => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
}));

import { apiGet, apiPost } from "./api";

const mockedGet = apiGet as Mock;
const mockedPost = apiPost as Mock;

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function makeScheduled(
  overrides: Partial<TanScheduledPost> = {},
): TanScheduledPost {
  return {
    id: "a1b2c3d4",
    text: "hello later",
    reply_to: null,
    to: [],
    ttl: 3,
    publish_at: 2000,
    created_at: 1000,
    post_hash: null,
    attempts: 0,
    next_attempt_at: 0,
    paused_at: 0,
    pending_peers: [],
    ...overrides,
  };
}

const emptyFollowing = { following: [], count: 0 };
const emptyAuthors = { authors: [], count: 0, total: 0 };
const emptySuggestions = { suggestions: [], count: 0 };
const emptyRules = {
  rules: {
    default: "allow",
    allow_authors: [],
    block_authors: [],
    allow_followed: false,
    kinds: null,
  },
  available_kinds: [],
};

// The feed load fetches the timeline, the following list, the authors
// directory, suggestions and rules in parallel.
function routeLoad(path: string) {
  if (path.startsWith("/api/tan/timeline"))
    return Promise.resolve({ posts: [], count: 0 });
  if (path.startsWith("/api/tan/authors"))
    return Promise.resolve(emptyAuthors);
  if (path.startsWith("/api/tan/suggest"))
    return Promise.resolve(emptySuggestions);
  if (path.startsWith("/api/tan/rules")) return Promise.resolve(emptyRules);
  return Promise.resolve(emptyFollowing);
}

beforeEach(() => {
  vi.resetAllMocks();
});

describe("useTan scheduled posts", () => {
  it("loads the scheduled queue on demand", async () => {
    const entry = makeScheduled();
    mockedGet.mockImplementation((path: string) =>
      path.startsWith("/api/tan/scheduled")
        ? Promise.resolve({ scheduled: [entry], count: 1 })
        : routeLoad(path),
    );

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([]));

    await act(async () => {
      await result.current.loadScheduled();
    });

    expect(result.current.scheduled).toEqual([entry]);
    expect(result.current.scheduledLoading).toBe(false);
  });

  it("schedules a post with its delay and refreshes the loaded queue", async () => {
    const entry = makeScheduled();
    mockedGet.mockImplementation((path: string) =>
      path.startsWith("/api/tan/scheduled")
        ? Promise.resolve({ scheduled: [entry], count: 1 })
        : routeLoad(path),
    );
    mockedPost.mockResolvedValue({ ok: true, scheduled: entry });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([]));
    await act(async () => {
      await result.current.loadScheduled();
    });
    const scheduledLoads = () =>
      mockedGet.mock.calls.filter((call) =>
        String(call[0]).startsWith("/api/tan/scheduled"),
      ).length;
    const loadsBefore = scheduledLoads();

    await act(async () => {
      await result.current.schedulePost("hello later", 3600);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/tan/schedule", {
      text: "hello later",
      send_in: 3600,
    });
    // The loaded queue is re-read after scheduling, not just appended
    // to, so the server's soonest-first order holds.
    expect(scheduledLoads()).toBeGreaterThan(loadsBefore);
    expect(result.current.scheduled).toEqual([entry]);
  });

  it("passes the reply target when scheduling a reply", async () => {
    const entry = makeScheduled({ reply_to: "c".repeat(64) });
    mockedGet.mockImplementation(routeLoad);
    mockedPost.mockResolvedValue({ ok: true, scheduled: entry });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([]));

    await act(async () => {
      await result.current.schedulePost("agreed", 60, "c".repeat(64));
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/tan/schedule", {
      text: "agreed",
      send_in: 60,
      reply_to: "c".repeat(64),
    });
  });

  it("passes the expiry when scheduling a disappearing post", async () => {
    const entry = makeScheduled({ expires_in: 3600 });
    mockedGet.mockImplementation(routeLoad);
    mockedPost.mockResolvedValue({ ok: true, scheduled: entry });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([]));

    await act(async () => {
      await result.current.schedulePost("vanish later", 60, undefined, 3600);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/tan/schedule", {
      text: "vanish later",
      send_in: 60,
      expires_in: 3600,
    });
  });

  it("cancelling drops the entry from state without a reload", async () => {
    const first = makeScheduled();
    const second = makeScheduled({ id: "e5f6a7b8", text: "second" });
    mockedGet.mockImplementation((path: string) =>
      path.startsWith("/api/tan/scheduled")
        ? Promise.resolve({ scheduled: [first, second], count: 2 })
        : routeLoad(path),
    );
    mockedPost.mockResolvedValue({
      ok: true,
      cancelled: first,
      deleted_local: false,
    });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([]));
    await act(async () => {
      await result.current.loadScheduled();
    });
    expect(result.current.scheduled).toEqual([first, second]);

    await act(async () => {
      await result.current.cancelScheduled(first.id);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/tan/cancel-scheduled", {
      id: first.id,
    });
    expect(result.current.scheduled).toEqual([second]);
  });

  it("a list load that started before a cancel cannot restore the card", async () => {
    const first = makeScheduled();
    const second = makeScheduled({ id: "e5f6a7b8", text: "second" });
    const pending = deferred<TanScheduledResponse>();
    let scheduledCalls = 0;
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/scheduled")) {
        scheduledCalls += 1;
        // The first load resolves at once; the refresh below stays in
        // flight until the test resolves it.
        return scheduledCalls === 1
          ? Promise.resolve({ scheduled: [first, second], count: 2 })
          : pending.promise;
      }
      return routeLoad(path);
    });
    mockedPost.mockResolvedValue({
      ok: true,
      cancelled: first,
      deleted_local: false,
    });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([]));
    await act(async () => {
      await result.current.loadScheduled();
    });
    expect(result.current.scheduled).toEqual([first, second]);

    // A refresh starts (e.g. the view's poll tick) and is still in
    // flight when the cancel lands.
    let staleLoad: Promise<void>;
    act(() => {
      staleLoad = result.current.loadScheduled();
    });
    await act(async () => {
      await result.current.cancelScheduled(first.id);
    });
    expect(result.current.scheduled).toEqual([second]);

    // The stale refresh resolves with the pre-cancel snapshot; its
    // response must be discarded, not restore the cancelled card.
    await act(async () => {
      pending.resolve({ scheduled: [first, second], count: 2 });
      await staleLoad;
    });
    expect(result.current.scheduled).toEqual([second]);
    expect(result.current.scheduledLoading).toBe(false);
  });
});
