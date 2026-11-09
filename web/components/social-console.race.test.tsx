import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { TanLikesResponse, TanPost } from "@/lib/types";

import { SocialConsole } from "./social-console";

function makePost(overrides: Partial<TanPost> = {}): TanPost {
  return {
    v: 1,
    author: "a".repeat(32),
    key: "b".repeat(128),
    name: "alice",
    text: "worth sharing",
    created_at: 1000,
    prev: "",
    hash: "c".repeat(64),
    sig: "d".repeat(128),
    likes: 1,
    liked: false,
    own: false,
    ...overrides,
  };
}

// The mocked hook and its posts live in one hoisted factory: the mock
// factory runs before the module body, so it cannot reference module
// level values defined below it.
const { fakeTan, postA, postB } = vi.hoisted(() => {
  const postA = makePost({ hash: "a".repeat(64), name: "alice" });
  const postB = makePost({ hash: "b".repeat(64), name: "bob", text: "also good" });
  const fakeTan = {
    posts: [postA, postB],
    following: [],
    all: false,
    setAll: () => {},
    loading: false,
    posting: false,
    syncing: false,
    error: null as string | null,
    refresh: async () => {},
    publish: async () => postA,
    follow: async () => {},
    unfollow: async () => {},
    like: async () => {},
    unlike: async () => {},
    fetchLikers: vi.fn(),
    deletePost: async () => {},
    sync: async () => ({ peers: 0, authors: 0, stored: 0 }),
  };
  return { fakeTan, postA, postB };
});

vi.mock("@/lib/use-tan", () => ({
  useTan: () => fakeTan,
}));

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function likersFor(post: TanPost, name: string): TanLikesResponse {
  return {
    target: post.hash,
    likers: [{ author: "e".repeat(32), name, liked_at: 1001 }],
    count: 1,
    total: 1,
  };
}

describe("SocialConsole liker races", () => {
  it("shows the second post's likers when the first request resolves last", async () => {
    const first = deferred<TanLikesResponse>();
    const second = deferred<TanLikesResponse>();
    fakeTan.fetchLikers.mockImplementation((target: string) =>
      target === postA.hash ? first.promise : second.promise,
    );

    render(<SocialConsole />);
    const buttons = screen.getAllByText("who liked");
    expect(buttons).toHaveLength(2);
    fireEvent.click(buttons[0]);
    fireEvent.click(buttons[1]);
    await act(async () => {
      first.resolve(likersFor(postA, "carol"));
    });
    await act(async () => {
      second.resolve(likersFor(postB, "dave"));
    });

    // The liker names are distinct from the post authors' names, so
    // this only passes when the second post's list is the one shown.
    expect(screen.getByText("dave")).toBeTruthy();
    expect(screen.queryByText("carol")).toBeNull();
  });

  it("does not close the second list when the first request fails late", async () => {
    const first = deferred<TanLikesResponse>();
    const second = deferred<TanLikesResponse>();
    fakeTan.fetchLikers.mockImplementation((target: string) =>
      target === postA.hash ? first.promise : second.promise,
    );

    render(<SocialConsole />);
    const buttons = screen.getAllByText("who liked");
    fireEvent.click(buttons[0]);
    fireEvent.click(buttons[1]);
    await act(async () => {
      first.reject(new Error("boom"));
    });
    await act(async () => {
      second.resolve(likersFor(postB, "dave"));
    });

    expect(screen.getByText("dave")).toBeTruthy();
    expect(screen.queryByText("could not load likers")).toBeNull();
  });
});
