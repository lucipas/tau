import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type { TanPost } from "./types";
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

function makePost(overrides: Partial<TanPost> = {}): TanPost {
  return {
    v: 1,
    author: "a".repeat(64),
    key: "b".repeat(128),
    name: "alice",
    text: "hello",
    created_at: 1000,
    prev: "",
    hash: "c".repeat(64),
    sig: "d".repeat(128),
    likes: 0,
    liked: false,
    own: true,
    ...overrides,
  };
}

const emptyFollowing = { following: [], count: 0 };
const emptySuggestions = { suggestions: [], count: 0 };

function routeLoad(path: string, timeline: unknown) {
  if (path.startsWith("/api/tan/timeline")) return Promise.resolve(timeline);
  if (path.startsWith("/api/tan/suggest")) return Promise.resolve(emptySuggestions);
  return Promise.resolve(emptyFollowing);
}

beforeEach(() => {
  vi.resetAllMocks();
});

describe("useTan deletePost", () => {
  it("posts the hash to /api/tan/delete and reloads the feed", async () => {
    const mine = makePost();
    mockedGet.mockImplementation((path: string) =>
      routeLoad(path, { posts: [mine], count: 1 }),
    );
    mockedPost.mockResolvedValue({ ok: true });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([mine]));
    const loadsBefore = mockedGet.mock.calls.length;

    await act(async () => {
      await result.current.deletePost(mine.hash);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/tan/delete", {
      target: mine.hash,
    });
    // The feed is reloaded after the delete, not just optimistically edited.
    expect(mockedGet.mock.calls.length).toBeGreaterThan(loadsBefore);
  });

  it("skips a poll while a feed load is already in flight", async () => {
    vi.useFakeTimers();
    try {
      const mine = makePost();
      const first = deferred();
      const firstFollowing = deferred();
      const firstSuggestions = deferred();
      const queue = [first, firstFollowing, firstSuggestions];
      let next = 0;
      mockedGet.mockImplementation(() => queue[next++].promise);

      const { result } = renderHook(() => useTan());
      // The initial load is in flight; a poll tick must not invalidate it.
      expect(mockedGet.mock.calls.length).toBe(3);
      await act(async () => {
        vi.advanceTimersByTime(5000);
      });
      expect(mockedGet.mock.calls.length).toBe(3);

      await act(async () => {
        first.resolve({ posts: [mine], count: 1 });
        firstFollowing.resolve(emptyFollowing);
        firstSuggestions.resolve(emptySuggestions);
      });
      expect(result.current.posts).toEqual([mine]);

      // With no load in flight the next poll proceeds normally.
      mockedGet.mockImplementation((path: string) =>
        routeLoad(path, { posts: [mine], count: 1 }),
      );
      await act(async () => {
        vi.advanceTimersByTime(5000);
      });
      expect(mockedGet.mock.calls.length).toBeGreaterThan(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it("drops a stale feed response that resolves after a newer load", async () => {
    const first = makePost({ hash: "a".repeat(64), text: "first" });
    const second = makePost({ hash: "b".repeat(64), text: "second" });
    const queue = [
      deferred(),
      deferred(),
      deferred(),
      deferred(),
      deferred(),
      deferred(),
    ];
    let next = 0;
    mockedGet.mockImplementation(() => queue[next++].promise);

    const { result } = renderHook(() => useTan());
    // The initial load is in flight; start a newer one before it resolves.
    let refreshPromise: Promise<void> | undefined;
    act(() => {
      refreshPromise = result.current.refresh();
    });
    // Resolve the newer load first, then the older one.
    await act(async () => {
      queue[3].resolve({ posts: [second], count: 1 });
      queue[4].resolve(emptyFollowing);
      queue[5].resolve(emptySuggestions);
    });
    await act(async () => {
      queue[0].resolve({ posts: [first], count: 1 });
      queue[1].resolve(emptyFollowing);
      queue[2].resolve(emptySuggestions);
    });
    await act(async () => {
      await refreshPromise;
    });

    // The deleted post's refresh wins; the stale poll response is dropped.
    expect(result.current.posts).toEqual([second]);
  });

  it("loads suggested authors with the feed and refreshes them after a follow", async () => {
    const suggestion = {
      author: "e".repeat(64),
      name: "carol",
      posts: 2,
      likes: 3,
      reason: "3 like(s)",
      last_post: 1002,
    };
    mockedGet.mockImplementation((path: string) =>
      path.startsWith("/api/tan/suggest")
        ? Promise.resolve({ suggestions: [suggestion], count: 1 })
        : routeLoad(path, { posts: [], count: 0 }),
    );
    mockedPost.mockResolvedValue({ ok: true });

    const { result } = renderHook(() => useTan());
    await waitFor(() =>
      expect(result.current.suggestions).toEqual([suggestion]),
    );
    const loadsBefore = mockedGet.mock.calls.length;

    await act(async () => {
      await result.current.follow(suggestion.author);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/tan/follow", {
      target: suggestion.author,
    });
    // Following reloads the feed, so the followed author drops out of
    // the suggestions on the same round trip.
    expect(mockedGet.mock.calls.length).toBeGreaterThan(loadsBefore);
  });
});
