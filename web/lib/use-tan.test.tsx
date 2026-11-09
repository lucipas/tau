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
