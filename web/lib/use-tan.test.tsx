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
      const firstMuted = deferred();
      const firstMutedKeywords = deferred();
      const queue = [first, firstFollowing, firstMuted, firstMutedKeywords];
      let next = 0;
      mockedGet.mockImplementation(() => queue[next++].promise);

      const { result } = renderHook(() => useTan());
      // The initial load is in flight; a poll tick must not invalidate it.
      expect(mockedGet.mock.calls.length).toBe(4);
      await act(async () => {
        vi.advanceTimersByTime(5000);
      });
      expect(mockedGet.mock.calls.length).toBe(4);

      await act(async () => {
        first.resolve({ posts: [mine], count: 1 });
        firstFollowing.resolve(emptyFollowing);
        firstMuted.resolve({ muted: [], count: 0 });
        firstMutedKeywords.resolve({ muted_keywords: [], count: 0 });
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
      expect(mockedGet.mock.calls.length).toBeGreaterThan(4);
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
      queue[4].resolve({ posts: [second], count: 1 });
      queue[5].resolve(emptyFollowing);
      queue[6].resolve({ muted: [], count: 0 });
      queue[7].resolve({ muted_keywords: [], count: 0 });
    });
    await act(async () => {
      queue[0].resolve({ posts: [first], count: 1 });
      queue[1].resolve(emptyFollowing);
      queue[2].resolve({ muted: [], count: 0 });
      queue[3].resolve({ muted_keywords: [], count: 0 });
    });
    await act(async () => {
      await refreshPromise;
    });

    // The deleted post's refresh wins; the stale poll response is dropped.
    expect(result.current.posts).toEqual([second]);
  });
});

describe("useTan mute actions", () => {
  const mutedAuthors = [
    { author: "a".repeat(64), name: "bob", muted_at: 1000, posts: 2 },
  ];
  const mutedKeywords = [
    { keyword: "crypto", display: "crypto", added_at: 1001 },
  ];

  function mockFeed() {
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/timeline")) {
        return Promise.resolve({ posts: [], count: 0 });
      }
      if (path === "/api/tan/muted") {
        return Promise.resolve({ muted: mutedAuthors, count: 1 });
      }
      if (path === "/api/tan/muted-keywords") {
        return Promise.resolve({ muted_keywords: mutedKeywords, count: 1 });
      }
      return Promise.resolve(emptyFollowing);
    });
    mockedPost.mockResolvedValue({ ok: true });
  }

  it("loads muted authors and keywords with the feed", async () => {
    mockFeed();

    const { result } = renderHook(() => useTan());
    await waitFor(() =>
      expect(result.current.mutedAuthors).toEqual(mutedAuthors),
    );
    expect(result.current.mutedKeywords).toEqual(mutedKeywords);
  });

  it("mutes and unmutes an author and reloads the feed", async () => {
    mockFeed();
    const author = "a".repeat(64);

    const { result } = renderHook(() => useTan());
    await waitFor(() =>
      expect(result.current.mutedAuthors).toEqual(mutedAuthors),
    );
    const loadsBefore = mockedGet.mock.calls.length;

    await act(async () => {
      await result.current.muteAuthor(author);
    });
    expect(mockedPost).toHaveBeenCalledWith("/api/tan/mute", {
      target: author,
    });
    expect(mockedGet.mock.calls.length).toBeGreaterThan(loadsBefore);

    await act(async () => {
      await result.current.unmuteAuthor(author);
    });
    expect(mockedPost).toHaveBeenCalledWith("/api/tan/unmute", {
      target: author,
    });
  });

  it("mutes and unmutes a keyword and reloads the feed", async () => {
    mockFeed();

    const { result } = renderHook(() => useTan());
    await waitFor(() =>
      expect(result.current.mutedKeywords).toEqual(mutedKeywords),
    );

    await act(async () => {
      await result.current.muteKeyword("crypto");
    });
    expect(mockedPost).toHaveBeenCalledWith("/api/tan/mute-keyword", {
      keyword: "crypto",
    });

    await act(async () => {
      await result.current.unmuteKeyword("crypto");
    });
    expect(mockedPost).toHaveBeenCalledWith("/api/tan/unmute-keyword", {
      keyword: "crypto",
    });
  });
});
