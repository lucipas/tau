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

beforeEach(() => {
  vi.resetAllMocks();
});

describe("useTan deletePost", () => {
  it("posts the hash to /api/tan/delete and reloads the feed", async () => {
    const mine = makePost();
    mockedGet.mockImplementation((path: string) =>
      path.startsWith("/api/tan/timeline")
        ? Promise.resolve({ posts: [mine], count: 1 })
        : Promise.resolve(emptyFollowing),
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
      const queue = [first, firstFollowing];
      let next = 0;
      mockedGet.mockImplementation(() => queue[next++].promise);

      const { result } = renderHook(() => useTan());
      // The initial load is in flight; a poll tick must not invalidate it.
      expect(mockedGet.mock.calls.length).toBe(2);
      await act(async () => {
        vi.advanceTimersByTime(5000);
      });
      expect(mockedGet.mock.calls.length).toBe(2);

      await act(async () => {
        first.resolve({ posts: [mine], count: 1 });
        firstFollowing.resolve(emptyFollowing);
      });
      expect(result.current.posts).toEqual([mine]);

      // With no load in flight the next poll proceeds normally.
      mockedGet.mockImplementation((path: string) =>
        path.startsWith("/api/tan/timeline")
          ? Promise.resolve({ posts: [mine], count: 1 })
          : Promise.resolve(emptyFollowing),
      );
      await act(async () => {
        vi.advanceTimersByTime(5000);
      });
      expect(mockedGet.mock.calls.length).toBeGreaterThan(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it("drops a stale feed response that resolves after a newer load", async () => {
    const first = makePost({ hash: "a".repeat(64), text: "first" });
    const second = makePost({ hash: "b".repeat(64), text: "second" });
    const queue = [deferred(), deferred(), deferred(), deferred()];
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
      queue[2].resolve({ posts: [second], count: 1 });
      queue[3].resolve(emptyFollowing);
    });
    await act(async () => {
      queue[0].resolve({ posts: [first], count: 1 });
      queue[1].resolve(emptyFollowing);
    });
    await act(async () => {
      await refreshPromise;
    });

    // The deleted post's refresh wins; the stale poll response is dropped.
    expect(result.current.posts).toEqual([second]);
  });
});

describe("useTan top view", () => {
  it("loads /api/tan/top when the view switches to top", async () => {
    const top = makePost({ text: "most liked", likes: 5 });
    const latest = makePost({ text: "newest", hash: "e".repeat(64) });
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/top")) {
        return Promise.resolve({ posts: [top], count: 1 });
      }
      if (path.startsWith("/api/tan/timeline")) {
        return Promise.resolve({ posts: [latest], count: 1 });
      }
      return Promise.resolve(emptyFollowing);
    });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([latest]));
    expect(result.current.view).toBe("timeline");

    await act(async () => {
      result.current.setView("top");
    });
    await waitFor(() => expect(result.current.posts).toEqual([top]));
    expect(result.current.view).toBe("top");
    const topCalls = mockedGet.mock.calls.filter((call) =>
      String(call[0]).startsWith("/api/tan/top"),
    );
    expect(topCalls.length).toBeGreaterThan(0);
    expect(topCalls[0][0]).toBe("/api/tan/top?limit=10");

    // Switching back reloads the timeline.
    await act(async () => {
      result.current.setView("timeline");
    });
    await waitFor(() => expect(result.current.posts).toEqual([latest]));
  });

  it("does not poll while the top view is selected", async () => {
    vi.useFakeTimers();
    try {
      const top = makePost({ text: "most liked", likes: 5 });
      const queue = [deferred(), deferred(), deferred(), deferred()];
      let next = 0;
      mockedGet.mockImplementation(() => queue[next++].promise);

      const { result } = renderHook(() => useTan());
      // The initial timeline load is in flight; switch to top, which
      // supersedes it with a top load.
      expect(mockedGet.mock.calls.length).toBe(2);
      act(() => {
        result.current.setView("top");
      });
      expect(mockedGet.mock.calls.length).toBe(4);

      await act(async () => {
        queue[2].resolve({ posts: [top], count: 1 });
        queue[3].resolve(emptyFollowing);
      });
      expect(result.current.posts).toEqual([top]);

      await act(async () => {
        vi.advanceTimersByTime(15000);
      });
      expect(mockedGet.mock.calls.length).toBe(4);
    } finally {
      vi.useRealTimers();
    }
  });

  it("hides the timeline posts while the top view is pending", async () => {
    const latest = makePost({ text: "newest", hash: "e".repeat(64) });
    const top = makePost({ text: "most liked", likes: 5 });
    const topLoad = deferred<{ posts: TanPost[]; count: number }>();
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/top")) return topLoad.promise;
      if (path.startsWith("/api/tan/timeline")) {
        return Promise.resolve({ posts: [latest], count: 1 });
      }
      return Promise.resolve(emptyFollowing);
    });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([latest]));

    act(() => {
      result.current.setView("top");
    });
    // Chronological posts must not appear under the top label.
    expect(result.current.posts).toEqual([]);
    expect(result.current.loading).toBe(true);

    await act(async () => {
      topLoad.resolve({ posts: [top], count: 1 });
    });
    await waitFor(() => expect(result.current.posts).toEqual([top]));
  });

  it("hides the timeline posts when the top view fails to load", async () => {
    const latest = makePost({ text: "newest", hash: "e".repeat(64) });
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/top")) {
        return Promise.reject(new Error("top unavailable"));
      }
      if (path.startsWith("/api/tan/timeline")) {
        return Promise.resolve({ posts: [latest], count: 1 });
      }
      return Promise.resolve(emptyFollowing);
    });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([latest]));

    await act(async () => {
      result.current.setView("top");
    });
    await waitFor(() => expect(result.current.error).toBe("top unavailable"));
    expect(result.current.posts).toEqual([]);
  });

  it("likes and unlikes from the top view, then reloads the top ranking", async () => {
    const top = makePost({ text: "most liked", likes: 5, liked: false, own: false });
    mockedGet.mockImplementation((path: string) =>
      path.startsWith("/api/tan/top")
        ? Promise.resolve({ posts: [top], count: 1 })
        : Promise.resolve(emptyFollowing),
    );
    mockedPost.mockResolvedValue({ ok: true });

    const { result } = renderHook(() => useTan());
    await act(async () => {
      result.current.setView("top");
    });
    await waitFor(() => expect(result.current.posts).toEqual([top]));

    const topLoadsBefore = mockedGet.mock.calls.filter((call) =>
      String(call[0]).startsWith("/api/tan/top"),
    ).length;
    await act(async () => {
      await result.current.like(top.hash);
    });
    expect(mockedPost).toHaveBeenCalledWith("/api/tan/like", {
      target: top.hash,
    });
    const topLoadsAfterLike = mockedGet.mock.calls.filter((call) =>
      String(call[0]).startsWith("/api/tan/top"),
    ).length;
    expect(topLoadsAfterLike).toBeGreaterThan(topLoadsBefore);

    await act(async () => {
      await result.current.unlike(top.hash);
    });
    expect(mockedPost).toHaveBeenCalledWith("/api/tan/unlike", {
      target: top.hash,
    });
    const topLoadsAfterUnlike = mockedGet.mock.calls.filter((call) =>
      String(call[0]).startsWith("/api/tan/top"),
    ).length;
    expect(topLoadsAfterUnlike).toBeGreaterThan(topLoadsAfterLike);
  });

  it("deletes from the top view, then reloads the top ranking", async () => {
    const mine = makePost({ text: "mine and liked", likes: 3, own: true });
    mockedGet.mockImplementation((path: string) =>
      path.startsWith("/api/tan/top")
        ? Promise.resolve({ posts: [mine], count: 1 })
        : Promise.resolve(emptyFollowing),
    );
    mockedPost.mockResolvedValue({ ok: true });

    const { result } = renderHook(() => useTan());
    await act(async () => {
      result.current.setView("top");
    });
    await waitFor(() => expect(result.current.posts).toEqual([mine]));
    const topLoadsBefore = mockedGet.mock.calls.filter((call) =>
      String(call[0]).startsWith("/api/tan/top"),
    ).length;

    await act(async () => {
      await result.current.deletePost(mine.hash);
    });
    expect(mockedPost).toHaveBeenCalledWith("/api/tan/delete", {
      target: mine.hash,
    });
    const topLoadsAfter = mockedGet.mock.calls.filter((call) =>
      String(call[0]).startsWith("/api/tan/top"),
    ).length;
    expect(topLoadsAfter).toBeGreaterThan(topLoadsBefore);
  });
});
