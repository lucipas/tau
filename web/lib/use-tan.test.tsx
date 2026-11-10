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


describe("useTan mentions view", () => {
  it("loads mentions instead of the timeline when the view is selected", async () => {
    const mine = makePost();
    const reply = makePost({
      hash: "e".repeat(64),
      text: "seconded",
      author: "f".repeat(64),
      name: "bob",
      own: false,
    });
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/mentions")) {
        return Promise.resolve({ posts: [reply], count: 1 });
      }
      return path.startsWith("/api/tan/timeline")
        ? Promise.resolve({ posts: [mine], count: 1 })
        : Promise.resolve(emptyFollowing);
    });
    mockedPost.mockResolvedValue({ ok: true });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([mine]));
    // Mentions are not part of the timeline's load: nothing loaded yet.
    expect(result.current.mentions).toEqual([]);
    expect(
      mockedGet.mock.calls.some((call) =>
        String(call[0]).startsWith("/api/tan/mentions"),
      ),
    ).toBe(false);

    await act(async () => {
      result.current.setShowMentions(true);
    });
    await waitFor(() => expect(result.current.mentions).toEqual([reply]));
    expect(mockedGet).toHaveBeenCalledWith("/api/tan/mentions?limit=100");

    // Switching back reloads the timeline.
    await act(async () => {
      result.current.setShowMentions(false);
    });
    await waitFor(() => expect(result.current.posts).toEqual([mine]));
  });
});


describe("useTan saved posts", () => {
  it("loads saved posts on demand and posts save/unsave to the API", async () => {
    const mine = makePost();
    const saved = makePost({ hash: "e".repeat(64), text: "keep me" });
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/saved")) {
        return Promise.resolve({ posts: [saved], count: 1, next_offset: null });
      }
      return path.startsWith("/api/tan/timeline")
        ? Promise.resolve({ posts: [mine], count: 1 })
        : Promise.resolve(emptyFollowing);
    });
    mockedPost.mockResolvedValue({ ok: true });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([mine]));
    // Saved posts are not part of the feed's poll: nothing loaded yet.
    expect(result.current.saved).toEqual([]);
    expect(
      mockedGet.mock.calls.some((call) =>
        String(call[0]).startsWith("/api/tan/saved"),
      ),
    ).toBe(false);

    await act(async () => {
      await result.current.loadSaved();
    });
    expect(mockedGet).toHaveBeenCalledWith(
      "/api/tan/saved?limit=100&offset=0",
    );
    expect(result.current.saved).toEqual([saved]);

    await act(async () => {
      await result.current.save(mine.hash);
    });
    expect(mockedPost).toHaveBeenCalledWith("/api/tan/save", {
      target: mine.hash,
    });

    await act(async () => {
      await result.current.unsave(mine.hash);
    });
    expect(mockedPost).toHaveBeenCalledWith("/api/tan/unsave", {
      target: mine.hash,
    });
  });

  it("pages through older saves with load more", async () => {
    const secondPage = [makePost({ hash: "f".repeat(64), text: "older" })];
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/saved")) {
        // A non-null next_offset means there may be more: the first
        // page is full, the second holds the one remaining save.
        const page = path.endsWith("offset=0")
          ? Array.from({ length: 100 }, (_, index) =>
              makePost({
                hash: `${index}`.padStart(64, "0"),
                text: `saved ${index}`,
              }),
            )
          : secondPage;
        return Promise.resolve({
          posts: page,
          count: page.length,
          next_offset: path.endsWith("offset=0") ? 100 : null,
        });
      }
      return path.startsWith("/api/tan/timeline")
        ? Promise.resolve({ posts: [], count: 0 })
        : Promise.resolve(emptyFollowing);
    });
    mockedPost.mockResolvedValue({ ok: true });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([]));

    await act(async () => {
      await result.current.loadSaved();
    });
    expect(result.current.saved).toHaveLength(100);
    expect(result.current.savedHasMore).toBe(true);

    await act(async () => {
      await result.current.loadMoreSaved();
    });
    expect(mockedGet).toHaveBeenCalledWith(
      "/api/tan/saved?limit=100&offset=100",
    );
    expect(result.current.saved).toHaveLength(101);
    expect(result.current.saved[100]).toEqual(secondPage[0]);
    expect(result.current.savedHasMore).toBe(false);
  });

  it("refreshes the saved list after liking from the saved view", async () => {
    const mine = makePost();
    const saved = makePost({ hash: "e".repeat(64), text: "keep me" });
    let savedCalls = 0;
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/saved")) {
        savedCalls += 1;
        return Promise.resolve({ posts: [saved], count: 1, next_offset: null });
      }
      return path.startsWith("/api/tan/timeline")
        ? Promise.resolve({ posts: [mine], count: 1 })
        : Promise.resolve(emptyFollowing);
    });
    mockedPost.mockResolvedValue({ ok: true });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([mine]));

    // Liking before the saved list was ever opened does not fetch it.
    await act(async () => {
      await result.current.like(mine.hash);
    });
    expect(savedCalls).toBe(0);

    await act(async () => {
      await result.current.loadSaved();
    });
    expect(savedCalls).toBe(1);

    // Once opened, a like refreshes the saved cards too.
    await act(async () => {
      await result.current.like(mine.hash);
    });
    expect(savedCalls).toBe(2);
  });
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

describe("useTan search", () => {
  function mockFeed() {
    const timelinePost = makePost({ text: "timeline post" });
    const hit = makePost({ hash: "e".repeat(64), text: "mesh post" });
    mockedGet.mockImplementation((path: string) =>
      path.startsWith("/api/tan/search")
        ? Promise.resolve({ posts: [hit], count: 1 })
        : path.startsWith("/api/tan/timeline")
          ? Promise.resolve({ posts: [timelinePost], count: 1 })
          : Promise.resolve(emptyFollowing),
    );
    return { timelinePost, hit };
  }

  it("fetches the search endpoint and shows results beside the timeline", async () => {
    const { timelinePost, hit } = mockFeed();
    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([timelinePost]));

    await act(async () => {
      await result.current.search("mesh");
    });

    await waitFor(() => expect(result.current.searchResults).toEqual([hit]));
    expect(result.current.searchQuery).toBe("mesh");
    // The timeline is kept, not replaced: clearing the search restores it.
    expect(result.current.posts).toEqual([timelinePost]);
    expect(mockedGet).toHaveBeenCalledWith("/api/tan/search?q=mesh&limit=100");
  });

  it("clearSearch returns to the timeline", async () => {
    const { timelinePost, hit } = mockFeed();
    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([timelinePost]));
    await act(async () => {
      await result.current.search("mesh");
    });
    await waitFor(() => expect(result.current.searchResults).toEqual([hit]));

    const timelineCallsBefore = mockedGet.mock.calls.filter((call) =>
      String(call[0]).startsWith("/api/tan/timeline"),
    ).length;
    await act(async () => {
      result.current.clearSearch();
    });

    await waitFor(() => expect(result.current.searchQuery).toBeNull());
    expect(result.current.searchResults).toEqual([]);
    const timelineCallsAfter = mockedGet.mock.calls.filter((call) =>
      String(call[0]).startsWith("/api/tan/timeline"),
    ).length;
    expect(timelineCallsAfter).toBeGreaterThan(timelineCallsBefore);
  });

  it("ignores a blank search", async () => {
    mockFeed();
    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toHaveLength(1));

    await act(async () => {
      await result.current.search("   ");
    });

    expect(result.current.searchQuery).toBeNull();
    expect(
      mockedGet.mock.calls.some((call) =>
        String(call[0]).startsWith("/api/tan/search"),
      ),
    ).toBe(false);
  });
});

describe("useTan search resubmit", () => {
  it("resubmitting the same query keeps results and refreshes them", async () => {
    const timelinePost = makePost({ text: "timeline post" });
    const hit = makePost({ hash: "e".repeat(64), text: "mesh post" });
    mockedGet.mockImplementation((path: string) =>
      path.startsWith("/api/tan/search")
        ? Promise.resolve({ posts: [hit], count: 1 })
        : path.startsWith("/api/tan/timeline")
          ? Promise.resolve({ posts: [timelinePost], count: 1 })
          : Promise.resolve(emptyFollowing),
    );
    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([timelinePost]));
    await act(async () => {
      await result.current.search("mesh");
    });
    await waitFor(() => expect(result.current.searchResults).toEqual([hit]));

    const searchCallsBefore = mockedGet.mock.calls.filter((call) =>
      String(call[0]).startsWith("/api/tan/search"),
    ).length;
    await act(async () => {
      await result.current.search("mesh");
    });

    // Results are kept, not cleared, and the query is re-fetched.
    expect(result.current.searchResults).toEqual([hit]);
    const searchCallsAfter = mockedGet.mock.calls.filter((call) =>
      String(call[0]).startsWith("/api/tan/search"),
    ).length;
    expect(searchCallsAfter).toBeGreaterThan(searchCallsBefore);
  });
});
