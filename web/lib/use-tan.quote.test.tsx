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

describe("useTan quote", () => {
  it("posts target and text to /api/tan/quote and prepends the quote", async () => {
    const original = makePost();
    const quote = makePost({
      hash: "e".repeat(64),
      text: "seconded, with notes",
      kind: "quote",
      target: original.hash,
      quoted: {
        hash: original.hash,
        author: original.author,
        name: "alice",
        text: "hello",
        deleted: false,
      },
    });
    let feed: TanPost[] = [original];
    mockedGet.mockImplementation((path: string) =>
      path.startsWith("/api/tan/timeline")
        ? Promise.resolve({ posts: feed, count: feed.length })
        : Promise.resolve(emptyFollowing),
    );
    mockedPost.mockImplementation(async () => {
      feed = [quote, original];
      return { ok: true, post: quote };
    });

    const { result } = renderHook(() => useTan());
    await waitFor(() => expect(result.current.posts).toEqual([original]));

    await act(async () => {
      await result.current.quote(original.hash, "seconded, with notes");
    });

    expect(mockedPost).toHaveBeenCalledWith("/api/tan/quote", {
      target: original.hash,
      text: "seconded, with notes",
    });
    expect(result.current.posts[0]).toEqual(quote);
  });
});
