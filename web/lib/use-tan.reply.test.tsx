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
const emptySuggestions = { suggestions: [], count: 0 };

function routeLoad(path: string, timeline: unknown) {
  if (path.startsWith("/api/tan/timeline")) return Promise.resolve(timeline);
  if (path.startsWith("/api/tan/suggest"))
    return Promise.resolve(emptySuggestions);
  return Promise.resolve(emptyFollowing);
}

beforeEach(() => {
  vi.resetAllMocks();
});

describe("useTan publish with replies", () => {
  it("sends reply_to when publishing a reply", async () => {
    const parent = makePost();
    const reply = makePost({
      hash: "e".repeat(64),
      text: "seconded",
      kind: "reply",
      target: parent.hash,
    });
    let feed: TanPost[] = [parent];
    mockedGet.mockImplementation((path: string) =>
      routeLoad(path, { posts: feed, count: feed.length }),
    );
    mockedPost.mockImplementation(async () => {
      feed = [reply, parent];
      return { ok: true, post: reply };
    });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([parent]));

    await act(async () => {
      await result.current.publish("seconded", parent.hash);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/tan/posts", {
      text: "seconded",
      reply_to: parent.hash,
    });
    expect(result.current.posts[0]).toEqual(reply);
  });

  it("omits reply_to for a standalone post", async () => {
    mockedGet.mockImplementation((path: string) =>
      routeLoad(path, { posts: [], count: 0 }),
    );
    const post = makePost();
    mockedPost.mockResolvedValue({ ok: true, post });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.loading).toBe(false));

    await act(async () => {
      await result.current.publish("hello");
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/tan/posts", {
      text: "hello",
    });
  });

  it("sends expires_in when publishing a disappearing post", async () => {
    mockedGet.mockImplementation((path: string) =>
      routeLoad(path, { posts: [], count: 0 }),
    );
    const post = makePost({ expires_at: 4600 });
    mockedPost.mockResolvedValue({ ok: true, post });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.loading).toBe(false));

    await act(async () => {
      await result.current.publish("vanish soon", undefined, 3600);
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/tan/posts", {
      text: "vanish soon",
      expires_in: 3600,
    });
  });
});
