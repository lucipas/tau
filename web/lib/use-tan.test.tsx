// use-tan.test.tsx — Tan feed hook with fetch mocked: initial timeline load,
// like triggers a reload, and search delegates to /api/tan/search.

import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { useTanFeed, useTanLists } from "./use-tan";
import type { TanAuthor, TanPost } from "./types";

function makePost(hash: string, text: string): TanPost {
  return {
    v: 1,
    hash,
    author: "a".repeat(32),
    key: "a".repeat(32),
    name: "alice",
    text,
    created_at: 1000,
    prev: "",
    sig: "s".repeat(128),
    kind: "post",
    target: "",
    expires_at: null,
    likes: 0,
    liked: false,
    own: false,
    quotes: 0,
  };
}

let calls: string[] = [];
let timelinePosts: TanPost[] = [];

beforeEach(() => {
  calls = [];
  timelinePosts = [makePost("h1".padEnd(64, "0"), "first post")];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      calls.push(`${init?.method ?? "GET"} ${url}`);
      if (url.includes("/api/tan/like")) {
        return { ok: true, status: 200, text: async () => JSON.stringify({ ok: true }) } as Response;
      }
      if (url.includes("/api/tan/search")) {
        return {
          ok: true,
          status: 200,
          text: async () => JSON.stringify({ posts: timelinePosts, count: 1 }),
        } as Response;
      }
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify({ posts: timelinePosts, count: timelinePosts.length }),
      } as Response;
    }),
  );
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("useTanFeed", () => {
  it("loads the following timeline by default", async () => {
    const { result } = renderHook(() => useTanFeed({ pollMs: 0 }));
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.posts).toHaveLength(1);
    expect(result.current.posts[0].text).toBe("first post");
    expect(calls[0]).toBe("GET /api/tan/timeline?limit=30");
  });

  it("loads a list timeline in list mode", async () => {
    const { result } = renderHook(() => useTanFeed({ list: "Friends", pollMs: 0 }));
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(calls[0]).toBe("GET /api/tan/timeline?limit=30&list=Friends");
    expect(result.current.posts).toHaveLength(1);
  });

  it("loads all posts in all mode", async () => {
    const { result } = renderHook(() => useTanFeed({ mode: "all", pollMs: 0 }));
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(calls[0]).toBe("GET /api/tan/timeline?limit=30&all=1");
    expect(result.current.posts).toHaveLength(1);
  });

  it("likes a post then reloads the timeline", async () => {
    const { result } = renderHook(() => useTanFeed({ pollMs: 0 }));
    await waitFor(() => expect(result.current.loading).toBe(false));
    const before = calls.length;
    await act(async () => {
      await result.current.like("h1".padEnd(64, "0"));
    });
    expect(calls.slice(before)).toEqual([
      `POST /api/tan/like`,
      "GET /api/tan/timeline?limit=30",
    ]);
  });

  it("searches via /api/tan/search", async () => {
    const { result } = renderHook(() => useTanFeed({ pollMs: 0 }));
    await waitFor(() => expect(result.current.loading).toBe(false));
    let found: TanPost[] = [];
    await act(async () => {
      found = await result.current.search("mesh");
    });
    expect(found).toHaveLength(1);
    expect(calls).toContain("GET /api/tan/search?q=mesh&limit=30");
  });
});

describe("useTanLists", () => {
  it("loads followers from /api/tan/followers", async () => {
    const follower: TanAuthor = {
      author: "b".repeat(64),
      name: "bob",
      posts: 2,
      followed: false,
      last_post: 1000,
    };
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        calls.push(`${init?.method ?? "GET"} ${url}`);
        const body: Record<string, unknown> = { ok: true };
        if (url.includes("/api/tan/tags")) body.tags = [];
        if (url.includes("/api/tan/following")) body.following = [];
        if (url.includes("/api/tan/followers")) body.followers = [follower];
        if (url.includes("/api/tan/suggest")) body.suggestions = [];
        if (url.includes("/api/tan/saved")) body.posts = [];
        if (url.includes("/api/tan/hidden")) body.posts = [];
        if (url.includes("/api/tan/pinned")) body.posts = [];
        if (url.includes("/api/tan/scheduled")) body.scheduled = [];
        if (url.includes("/api/tan/muted-keywords")) body.muted_keywords = [];
        if (url.includes("/api/tan/muted-tags")) body.muted_tags = [];
        if (url.includes("/api/tan/muted")) body.muted = [];
        return {
          ok: true,
          status: 200,
          text: async () => JSON.stringify({ count: 0, ...body }),
        } as Response;
      }),
    );
    const { result } = renderHook(() => useTanLists());
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(calls).toContain("GET /api/tan/followers");
    expect(result.current.followers).toHaveLength(1);
    expect(result.current.followers[0].name).toBe("bob");
  });
});
