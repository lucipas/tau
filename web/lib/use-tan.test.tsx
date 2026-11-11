import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type { TanPost, TanRules } from "./types";
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
const emptyAuthors = { authors: [], count: 0, total: 0 };

// The feed load fetches the timeline, the following list and the
// authors directory in parallel; the non-timeline responses come back
// in their own shapes.
function otherResponses(path: string) {
  return path.startsWith("/api/tan/authors")
    ? Promise.resolve(emptyAuthors)
    : Promise.resolve(emptyFollowing);
}

const emptySuggestions = { suggestions: [], count: 0 };

function routeLoad(path: string, timeline: unknown) {
  if (path.startsWith("/api/tan/timeline")) return Promise.resolve(timeline);
  if (path.startsWith("/api/tan/authors")) return Promise.resolve(emptyAuthors);
  if (path.startsWith("/api/tan/suggest")) return Promise.resolve(emptySuggestions);
  return Promise.resolve(emptyFollowing);
}

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

beforeEach(() => {
  vi.resetAllMocks();
});

describe("useTan editPost", () => {
  it("posts the hash and text to /api/tan/edit and reloads the feed", async () => {
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
      await result.current.editPost(mine.hash, "fixed typo");
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/tan/edit", {
      target: mine.hash,
      text: "fixed typo",
    });
    // The feed is reloaded after the edit, not just optimistically edited.
    expect(mockedGet.mock.calls.length).toBeGreaterThan(loadsBefore);
  });
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
        : otherResponses(path);
    });
    mockedPost.mockResolvedValue({ ok: true });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([mine]));
    // The authors directory loads with the feed, like following.
    expect(result.current.authors).toEqual([]);
    expect(result.current.authorsTotal).toBe(0);
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
        : otherResponses(path);
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
        : otherResponses(path);
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
        : otherResponses(path);
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
      const firstAuthors = deferred();
      const firstMuted = deferred();
      const firstMutedKeywords = deferred();
      const firstSuggestions = deferred();
      // A feed load fires the timeline, following, muted and muted-
      // keyword requests, and the authors directory and suggestions
      // load beside it.
      const queue = [
        first,
        firstFollowing,
        firstMuted,
        firstMutedKeywords,
        firstAuthors,
        firstSuggestions,
      ];
      let next = 0;
      mockedGet.mockImplementation((path: string) =>
        path === "/api/tan/rules"
          ? Promise.resolve(emptyRules)
          : queue[next++].promise,
      );

      const { result } = renderHook(() => useTan());
      // The initial load is in flight; a poll tick must not invalidate it.
      // Six feed calls plus the one rules load.
      expect(mockedGet.mock.calls.length).toBe(7);
      await act(async () => {
        vi.advanceTimersByTime(5000);
      });
      expect(mockedGet.mock.calls.length).toBe(7);

      await act(async () => {
        first.resolve({ posts: [mine], count: 1 });
        firstFollowing.resolve(emptyFollowing);
        firstMuted.resolve({ muted: [], count: 0 });
        firstMutedKeywords.resolve({ muted_keywords: [], count: 0 });
        firstAuthors.resolve(emptyAuthors);
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
      expect(mockedGet.mock.calls.length).toBeGreaterThan(7);
    } finally {
      vi.useRealTimers();
    }
  });

  it("drops a stale feed response that resolves after a newer load", async () => {
    const first = makePost({ hash: "a".repeat(64), text: "first" });
    const second = makePost({ hash: "b".repeat(64), text: "second" });
    // Two feed loads in flight: the initial load takes the first
    // six deferreds (timeline, following, muted, muted keywords,
    // authors, suggestions), the refresh the next four.
    const queue = [
      deferred(),
      deferred(),
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
    mockedGet.mockImplementation((path: string) =>
      path === "/api/tan/rules"
        ? Promise.resolve(emptyRules)
        : queue[next++].promise,
    );

    const { result } = renderHook(() => useTan());
    // The initial load is in flight; start a newer one before it resolves.
    let refreshPromise: Promise<void> | undefined;
    act(() => {
      refreshPromise = result.current.refresh();
    });
    // Resolve the newer load first, then the older one.
    await act(async () => {
      queue[6].resolve({ posts: [second], count: 1 });
      queue[7].resolve(emptyFollowing);
      queue[8].resolve({ muted: [], count: 0 });
      queue[9].resolve({ muted_keywords: [], count: 0 });
    });
    await act(async () => {
      queue[0].resolve({ posts: [first], count: 1 });
      queue[1].resolve(emptyFollowing);
      queue[2].resolve({ muted: [], count: 0 });
      queue[3].resolve({ muted_keywords: [], count: 0 });
      queue[4].resolve(emptyAuthors);
      queue[5].resolve(emptySuggestions);
    });
    await act(async () => {
      await refreshPromise;
    });

    // The deleted post's refresh wins; the stale poll response is dropped.
    expect(result.current.posts).toEqual([second]);
  });

  it("keeps the feed when the authors directory fails to load", async () => {
    const mine = makePost();
    // The directory loads on its own generation, not the feed's, so a
    // failed authors request must not fail the feed or clear the
    // directory.
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/authors")) {
        return Promise.reject(new Error("authors unavailable"));
      }
      return path.startsWith("/api/tan/timeline")
        ? Promise.resolve({ posts: [mine], count: 1 })
        : otherResponses(path);
    });
    mockedPost.mockResolvedValue({ ok: true });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([mine]));
    expect(result.current.authors).toEqual([]);
    expect(result.current.authorsTotal).toBe(0);
    expect(result.current.error).toBeNull();
  });

  it("pages the authors directory with show more", async () => {
    const mine = makePost();
    function makeAuthorRecord(index: number) {
      return {
        author: `${index}`.padStart(64, "0"),
        name: `author ${index}`,
        posts: 1,
        last_post: 1000 + index,
        followed: false,
      };
    }
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/authors")) {
        // The directory holds 120 authors; each page returns the
        // requested offset slice, like the real API.
        const params = new URL(path, "http://localhost").searchParams;
        const offset = Number(params.get("offset") ?? 0);
        const limit = Number(params.get("limit") ?? 50);
        const page = Array.from(
          { length: Math.max(0, Math.min(limit, 120 - offset)) },
          (_, index) => makeAuthorRecord(offset + index),
        );
        return Promise.resolve({ authors: page, count: page.length, total: 120 });
      }
      return path.startsWith("/api/tan/timeline")
        ? Promise.resolve({ posts: [mine], count: 1 })
        : otherResponses(path);
    });
    mockedPost.mockResolvedValue({ ok: true });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([mine]));
    // The first page loads with the feed: 50 of 120 authors.
    await waitFor(() => expect(result.current.authors).toHaveLength(50));
    expect(result.current.authorsTotal).toBe(120);
    expect(mockedGet).toHaveBeenCalledWith(
      "/api/tan/authors?limit=50&offset=0",
    );

    // Show more appends the next offset page instead of re-fetching
    // the first rows with a larger limit.
    await act(async () => {
      await result.current.loadMoreAuthors();
    });
    expect(mockedGet).toHaveBeenCalledWith(
      "/api/tan/authors?limit=50&offset=50",
    );
    expect(result.current.authors).toHaveLength(100);
    expect(result.current.authors[50].name).toBe("author 50");

    // A third page reaches the total; the last page is short.
    await act(async () => {
      await result.current.loadMoreAuthors();
    });
    expect(mockedGet).toHaveBeenCalledWith(
      "/api/tan/authors?limit=50&offset=100",
    );
    expect(result.current.authors).toHaveLength(120);
  });

  it("loads suggested authors alongside the feed and refreshes them after a follow", async () => {
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
    // Following reloads the feed and refreshes the suggestions, so the
    // followed author drops out of the list on the same round trip.
    expect(mockedGet.mock.calls.length).toBeGreaterThan(loadsBefore);
  });
});

describe("useTan export and import", () => {
  const doc = {
    format: "tau-tan-export",
    version: 1,
    exported_at: 1000,
    authors: ["a".repeat(64)],
    entries: [{ hash: "c".repeat(64), text: "hello" }],
  };

  function mockFeed() {
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/export")) return Promise.resolve(doc);
      if (path.startsWith("/api/tan/timeline")) {
        return Promise.resolve({ posts: [], count: 0 });
      }
      return Promise.resolve(emptyFollowing);
    });
  }

  it("exports the node's own chain and triggers a download", async () => {
    const createObjectURL = vi.fn((_blob: Blob) => "blob:fake");
    const revokeObjectURL = vi.fn();
    const originalCreate = URL.createObjectURL;
    const originalRevoke = URL.revokeObjectURL;
    URL.createObjectURL = createObjectURL;
    URL.revokeObjectURL = revokeObjectURL;
    try {
      mockFeed();
      const { result } = renderHook(() => useTan());
      await waitFor(() => expect(result.current.loading).toBe(false));

      let returned;
      await act(async () => {
        returned = await result.current.exportTan(false);
      });

      expect(mockedGet).toHaveBeenCalledWith("/api/tan/export?scope=mine");
      expect(returned).toEqual(doc);
      expect(createObjectURL).toHaveBeenCalledTimes(1);
      const blob = createObjectURL.mock.calls[0][0];
      expect(blob.type).toBe("application/json");
      expect(await blob.text()).toContain("tau-tan-export");
    } finally {
      URL.createObjectURL = originalCreate;
      URL.revokeObjectURL = originalRevoke;
    }
  });

  it("exports all cached authors when asked", async () => {
    const createObjectURL = vi.fn((_blob: Blob) => "blob:fake");
    const originalCreate = URL.createObjectURL;
    const originalRevoke = URL.revokeObjectURL;
    URL.createObjectURL = createObjectURL;
    URL.revokeObjectURL = vi.fn();
    try {
      mockFeed();
      const { result } = renderHook(() => useTan());
      await waitFor(() => expect(result.current.loading).toBe(false));

      await act(async () => {
        await result.current.exportTan(true);
      });

      expect(mockedGet).toHaveBeenCalledWith("/api/tan/export?scope=all");
    } finally {
      URL.createObjectURL = originalCreate;
      URL.revokeObjectURL = originalRevoke;
    }
  });

  it("imports a document and reloads the feed", async () => {
    mockFeed();
    mockedPost.mockResolvedValue({
      ok: true,
      import: { imported: 2, present: 0, skipped: 0, errors: [] },
    });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.loading).toBe(false));
    const loadsBefore = mockedGet.mock.calls.length;

    let outcome;
    await act(async () => {
      outcome = await result.current.importTan(doc);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/tan/import", doc);
    expect(outcome).toEqual({
      imported: 2,
      present: 0,
      skipped: 0,
      errors: [],
    });
    // The feed is reloaded after the import, not just left stale.
    expect(mockedGet.mock.calls.length).toBeGreaterThan(loadsBefore);
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

describe("useTan verify", () => {
  it("fetches /api/tan/verify and returns the report without reloading the feed", async () => {
    const report = {
      entries: 2,
      authors: 1,
      unlinked: 0,
      problems: [],
      ok: true,
    };
    mockedGet.mockImplementation((path: string) =>
      path === "/api/tan/verify"
        ? Promise.resolve(report)
        : path.startsWith("/api/tan/timeline")
          ? Promise.resolve({ posts: [], count: 0 })
          : Promise.resolve(emptyFollowing),
    );
    mockedPost.mockResolvedValue({ ok: true });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.loading).toBe(false));
    const loadsBefore = mockedGet.mock.calls.length;

    let returned;
    await act(async () => {
      returned = await result.current.verify();
    });

    expect(returned).toEqual(report);
    expect(result.current.verifyReport).toEqual(report);
    // Verifying is read-only: exactly one audit request, no feed reload.
    const laterCalls = mockedGet.mock.calls
      .slice(loadsBefore)
      .map(([path]) => path);
    expect(laterCalls).toEqual(["/api/tan/verify"]);
  });

  it("drops the stored report when a post is published", async () => {
    const report = {
      entries: 2,
      authors: 1,
      unlinked: 0,
      problems: [],
      ok: true,
    };
    const mine = makePost();
    mockedGet.mockImplementation((path: string) =>
      path === "/api/tan/verify"
        ? Promise.resolve(report)
        : path.startsWith("/api/tan/timeline")
          ? Promise.resolve({ posts: [mine], count: 1 })
          : Promise.resolve(emptyFollowing),
    );
    mockedPost.mockResolvedValue({ ok: true, post: mine });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([mine]));
    await act(async () => {
      await result.current.verify();
    });
    expect(result.current.verifyReport).toEqual(report);

    await act(async () => {
      await result.current.publish("another post");
    });
    expect(result.current.verifyReport).toBeNull();
  });

  it("discards an audit that resolves after a mutation", async () => {
    const report = {
      entries: 2,
      authors: 1,
      unlinked: 0,
      problems: [],
      ok: true,
    };
    const mine = makePost();
    const verifyGate = deferred();
    mockedGet.mockImplementation((path: string) =>
      path === "/api/tan/verify"
        ? verifyGate.promise
        : path.startsWith("/api/tan/timeline")
          ? Promise.resolve({ posts: [mine], count: 1 })
          : Promise.resolve(emptyFollowing),
    );
    mockedPost.mockResolvedValue({ ok: true, post: mine });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([mine]));

    let verifyPromise: Promise<unknown> | undefined;
    act(() => {
      verifyPromise = result.current.verify();
    });
    // A publish lands while the audit is in flight.
    await act(async () => {
      await result.current.publish("another post");
    });
    await act(async () => {
      verifyGate.resolve(report);
      await verifyPromise;
    });
    // The audit read the store before the publish: it must not be shown.
    expect(result.current.verifyReport).toBeNull();
  });
});

const allowRules: TanRules = {
  default: "allow",
  allow_authors: [],
  block_authors: [],
  allow_followed: false,
  kinds: null,
};

const denyRules: TanRules = { ...allowRules, default: "deny" };

function mockFeed(rulesImpl: (path: string) => Promise<unknown>) {
  mockedGet.mockImplementation((path: string) => {
    if (path === "/api/tan/rules") return rulesImpl(path);
    if (path.startsWith("/api/tan/timeline"))
      return Promise.resolve({ posts: [], count: 0 });
    return Promise.resolve(emptyFollowing);
  });
}

describe("useTan rules", () => {
  it("loads the rules and available kinds", async () => {
    mockFeed(() =>
      Promise.resolve({
        rules: allowRules,
        available_kinds: ["post", "like"],
      }),
    );

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.rulesLoading).toBe(false));

    expect(result.current.rules).toEqual(allowRules);
    expect(result.current.availableKinds).toEqual(["post", "like"]);
    expect(result.current.rulesError).toBeNull();
  });

  it("sets rulesError when the rules fail to load", async () => {
    mockFeed(() => Promise.reject(new Error("node unreachable")));

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.rulesLoading).toBe(false));

    expect(result.current.rulesError).toBe("node unreachable");
    expect(result.current.rules).toBeNull();
  });

  it("saves the rules, updates state and reloads the feed", async () => {
    mockFeed(() =>
      Promise.resolve({ rules: allowRules, available_kinds: ["post"] }),
    );
    mockedPost.mockResolvedValue({ rules: denyRules });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.rules).toEqual(allowRules));
    const feedLoadsBefore = mockedGet.mock.calls.filter((call) =>
      String(call[0]).startsWith("/api/tan/timeline"),
    ).length;

    let saved: unknown;
    await act(async () => {
      saved = await result.current.updateRules(denyRules);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/tan/rules", denyRules);
    expect(saved).toEqual(denyRules);
    expect(result.current.rules).toEqual(denyRules);
    expect(result.current.rulesError).toBeNull();
    const feedLoadsAfter = mockedGet.mock.calls.filter((call) =>
      String(call[0]).startsWith("/api/tan/timeline"),
    ).length;
    expect(feedLoadsAfter).toBeGreaterThan(feedLoadsBefore);
  });

  it("throws and keeps the previous rules when a save is rejected", async () => {
    mockFeed(() =>
      Promise.resolve({ rules: allowRules, available_kinds: ["post"] }),
    );
    mockedPost.mockRejectedValue(new Error("unknown author"));

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.rules).toEqual(allowRules));

    let failure: unknown;
    await act(async () => {
      try {
        await result.current.updateRules(denyRules);
      } catch (caught) {
        failure = caught;
      }
    });

    expect(failure).toBeInstanceOf(Error);
    expect((failure as Error).message).toBe("unknown author");
    expect(result.current.rules).toEqual(allowRules);
  });

  it("reports a feed reload failure without rejecting a confirmed save", async () => {
    mockedGet.mockImplementation((path: string) => {
      if (path === "/api/tan/rules")
        return Promise.resolve({ rules: allowRules, available_kinds: [] });
      if (path.startsWith("/api/tan/timeline"))
        return Promise.reject(new Error("feed unreachable"));
      return Promise.resolve(emptyFollowing);
    });
    mockedPost.mockResolvedValue({ rules: denyRules });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.rules).toEqual(allowRules));

    let saved: unknown;
    await act(async () => {
      saved = await result.current.updateRules(denyRules);
    });

    expect(saved).toEqual(denyRules);
    expect(result.current.rules).toEqual(denyRules);
    expect(result.current.rulesError).toBeNull();
    expect(result.current.error).toBe("feed unreachable");
  });

  it("discards a rules GET that was superseded by a confirmed save", async () => {
    const initial = deferred<unknown>();
    mockedGet.mockImplementation((path: string) => {
      if (path === "/api/tan/rules") return initial.promise;
      if (path.startsWith("/api/tan/timeline"))
        return Promise.resolve({ posts: [], count: 0 });
      return Promise.resolve(emptyFollowing);
    });
    mockedPost.mockResolvedValue({ rules: denyRules });

    const { result } = renderHook(() => useTan());
    await act(async () => {
      await result.current.updateRules(denyRules);
    });
    expect(result.current.rules).toEqual(denyRules);

    // The GET began before the save and answers with the old policy.
    await act(async () => {
      initial.resolve({ rules: allowRules, available_kinds: [] });
    });

    expect(result.current.rules).toEqual(denyRules);
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
      // Six queued calls for the initial load (feed, following, muted,
      // muted keywords, authors, suggestions) plus the rules load, then
      // five more for the top load (feed, following, muted, muted
      // keywords, authors).
      const queue = [
        deferred(),
        deferred(),
        deferred(),
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
      mockedGet.mockImplementation((path: string) =>
        path === "/api/tan/rules"
          ? Promise.resolve(emptyRules)
          : queue[next++].promise,
      );

      const { result } = renderHook(() => useTan());
      // The initial timeline load is in flight; switch to top, which
      // supersedes it with a top load.
      expect(mockedGet.mock.calls.length).toBe(7);
      act(() => {
        result.current.setView("top");
      });
      expect(mockedGet.mock.calls.length).toBe(12);

      await act(async () => {
        queue[6].resolve({ posts: [top], count: 1 });
        queue[7].resolve(emptyFollowing);
        queue[8].resolve({ muted: [], count: 0 });
        queue[9].resolve({ muted_keywords: [], count: 0 });
        queue[10].resolve(emptyAuthors);
      });
      expect(result.current.posts).toEqual([top]);

      await act(async () => {
        vi.advanceTimersByTime(15000);
      });
      expect(mockedGet.mock.calls.length).toBe(12);
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
