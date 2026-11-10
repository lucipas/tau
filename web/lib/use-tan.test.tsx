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
      mockedGet.mockImplementation((path: string) =>
        path === "/api/tan/rules"
          ? Promise.resolve(emptyRules)
          : queue[next++].promise,
      );

      const { result } = renderHook(() => useTan());
      // The initial load is in flight; a poll tick must not invalidate it.
      // Two feed calls plus the one rules load.
      expect(mockedGet.mock.calls.length).toBe(3);
      await act(async () => {
        vi.advanceTimersByTime(5000);
      });
      expect(mockedGet.mock.calls.length).toBe(3);

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
      expect(mockedGet.mock.calls.length).toBeGreaterThan(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it("drops a stale feed response that resolves after a newer load", async () => {
    const first = makePost({ hash: "a".repeat(64), text: "first" });
    const second = makePost({ hash: "b".repeat(64), text: "second" });
    const queue = [deferred(), deferred(), deferred(), deferred()];
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
