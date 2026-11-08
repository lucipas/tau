import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import type { TanPost } from "@/lib/types";

import { PostCard } from "./social-console";

function makePost(overrides: Partial<TanPost> = {}): TanPost {
  return {
    v: 1,
    author: "a".repeat(64),
    key: "b".repeat(128),
    name: "bob",
    text: "",
    created_at: 1000,
    prev: "",
    hash: "c".repeat(64),
    sig: "d".repeat(128),
    likes: 0,
    liked: false,
    reposts: 0,
    reposted: false,
    ...overrides,
  };
}

const card = (post: TanPost) => (
  <PostCard post={post} onToggleLike={() => {}} onReply={() => {}} onDelete={() => {}} deleting={false} />
);

describe("PostCard repost display", () => {
  it("shows the original author and text on a repost", () => {
    const repost = makePost({
      kind: "repost",
      target: "f".repeat(64),
      repost_of: {
        hash: "f".repeat(64),
        author: "a".repeat(64),
        name: "alice",
        text: "original thought",
        deleted: false,
      },
    });
    render(card(repost));

    const line = screen.getByText(/reposted/);
    expect(line.textContent).toContain("alice");
    expect(screen.getByText("original thought")).toBeTruthy();
  });

  it("reports a deleted original without resurfacing its text", () => {
    const repost = makePost({
      kind: "repost",
      target: "f".repeat(64),
      repost_of: {
        hash: "f".repeat(64),
        author: "a".repeat(64),
        name: "alice",
        text: "",
        deleted: true,
      },
    });
    render(card(repost));

    expect(screen.getByText(/a deleted post/)).toBeTruthy();
    expect(screen.queryByText("original thought")).toBeNull();
  });

  it("falls back to the original hash when it has not arrived", () => {
    const repost = makePost({
      kind: "repost",
      target: "f".repeat(64),
      repost_of: null,
    });
    render(card(repost));

    expect(screen.getByText(/reposted/)).toBeTruthy();
    expect(screen.getByText(/original not cached yet/)).toBeTruthy();
  });

  it("shows the repost count on the original post", () => {
    const post = makePost({ text: "worth sharing", reposts: 2 });
    render(card(post));

    expect(screen.getByText(/↻ 2/)).toBeTruthy();
  });

  it("shows no repost line on a standalone post", () => {
    render(card(makePost({ text: "hello" })));

    expect(screen.queryByText(/reposted/)).toBeNull();
  });

  it("hides the reply button on a repost card", () => {
    const repost = makePost({
      kind: "repost",
      target: "f".repeat(64),
      repost_of: {
        hash: "f".repeat(64),
        author: "a".repeat(64),
        name: "alice",
        text: "original thought",
        deleted: false,
      },
    });
    render(card(repost));

    // Replying targets a post or reply; a repost has no text of its
    // own, so the button is hidden rather than offered and failing.
    expect(screen.queryByTitle("Reply to this post")).toBeNull();
  });
});
