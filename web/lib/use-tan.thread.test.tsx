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
    ...overrides,
  };
}

const emptyFollowing = { following: [], count: 0 };

beforeEach(() => {
  vi.resetAllMocks();
});

function statusError(status: number, message: string) {
  return Object.assign(new Error(message), { status });
}

describe("useTan thread view", () => {
  it("loads the thread for the opened post and clears it on close", async () => {
    const root = makePost({ text: "root post" });
    const reply = makePost({
      hash: "e".repeat(64),
      text: "first reply",
      kind: "reply",
      target: root.hash,
      depth: 1,
    });
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/thread")) {
        return Promise.resolve({ posts: [root, reply], count: 2 });
      }
      return path.startsWith("/api/tan/timeline")
        ? Promise.resolve({ posts: [root], count: 1 })
        : Promise.resolve(emptyFollowing);
    });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([root]));

    await act(async () => {
      await result.current.openThread(root.hash);
    });

    expect(mockedGet).toHaveBeenCalledWith(
      `/api/tan/thread?ref=${root.hash}`,
    );
    expect(result.current.threadRef).toBe(root.hash);
    expect(result.current.threadPosts).toEqual([root, reply]);
    expect(result.current.threadLoading).toBe(false);
    expect(result.current.threadError).toBeNull();

    act(() => {
      result.current.closeThread();
    });
    expect(result.current.threadRef).toBeNull();
    expect(result.current.threadPosts).toBeNull();
  });

  it("closes the thread when its anchor is gone", async () => {
    const root = makePost({ text: "root post" });
    let threadFails = false;
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/thread")) {
        return threadFails
          ? Promise.reject(statusError(400, "unknown post: 'abc'"))
          : Promise.resolve({ posts: [root], count: 1 });
      }
      return path.startsWith("/api/tan/timeline")
        ? Promise.resolve({ posts: [root], count: 1 })
        : Promise.resolve(emptyFollowing);
    });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([root]));

    await act(async () => {
      await result.current.openThread(root.hash);
    });
    expect(result.current.threadPosts).toEqual([root]);

    // The root was deleted elsewhere; the next refresh gets a 400 and
    // the console falls back to the timeline instead of a stuck error.
    threadFails = true;
    await act(async () => {
      await result.current.refresh();
    });
    expect(result.current.threadRef).toBeNull();
    expect(result.current.threadPosts).toBeNull();
  });

  it("keeps the previous entries when a thread refresh fails", async () => {
    const root = makePost({ text: "root post" });
    const reply = makePost({
      hash: "e".repeat(64),
      text: "first reply",
      kind: "reply",
      target: root.hash,
      depth: 1,
    });
    let threadFails = false;
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/thread")) {
        return threadFails
          ? Promise.reject(new Error("network down"))
          : Promise.resolve({ posts: [root, reply], count: 2 });
      }
      return path.startsWith("/api/tan/timeline")
        ? Promise.resolve({ posts: [root], count: 1 })
        : Promise.resolve(emptyFollowing);
    });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([root]));

    await act(async () => {
      await result.current.openThread(root.hash);
    });
    expect(result.current.threadPosts).toEqual([root, reply]);

    threadFails = true;
    await act(async () => {
      await result.current.refresh();
    });
    expect(result.current.threadPosts).toEqual([root, reply]);
    expect(result.current.threadError).toBe("network down");
  });

  it("reports a feed failure while opening a thread", async () => {
    const root = makePost({ text: "root post" });
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/thread")) {
        return Promise.resolve({ posts: [root], count: 1 });
      }
      if (path.startsWith("/api/tan/timeline")) {
        return Promise.reject(new Error("could not load the feed"));
      }
      return Promise.resolve(emptyFollowing);
    });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.loading).toBe(false));

    // The feed load fails before the thread request runs; the thread
    // must report the failure instead of claiming to be empty.
    await act(async () => {
      await result.current.openThread(root.hash);
    });
    expect(result.current.threadRef).toBe(root.hash);
    expect(result.current.threadPosts).toBeNull();
    expect(result.current.threadLoading).toBe(false);
    expect(result.current.threadError).toBe("could not load the feed");
  });

  it("reloads the open thread after an action", async () => {
    const root = makePost({ text: "root post" });
    let feed: TanPost[] = [root];
    mockedGet.mockImplementation((path: string) => {
      if (path.startsWith("/api/tan/thread")) {
        return Promise.resolve({ posts: feed, count: feed.length });
      }
      return path.startsWith("/api/tan/timeline")
        ? Promise.resolve({ posts: feed, count: feed.length })
        : Promise.resolve(emptyFollowing);
    });
    mockedPost.mockResolvedValue({ ok: true });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([root]));

    await act(async () => {
      await result.current.openThread(root.hash);
    });
    expect(result.current.threadPosts).toEqual([root]);

    // A reply arrives; liking the root reloads the feed, and the open
    // thread reloads with it instead of going stale.
    const reply = makePost({
      hash: "e".repeat(64),
      text: "first reply",
      kind: "reply",
      target: root.hash,
      depth: 1,
    });
    feed = [root, reply];
    await act(async () => {
      await result.current.like(root.hash);
    });
    expect(result.current.threadPosts).toEqual([root, reply]);
  });
});
