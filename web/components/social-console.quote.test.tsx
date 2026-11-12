import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { TanPost } from "@/lib/types";

import { PostCard } from "./social-console";

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

describe("PostCard quote control", () => {
  it("offers a quote button on a post and calls onQuote with it", () => {
    const post = makePost();
    const onQuote = vi.fn();
    render(
      <PostCard
        post={post}
        onToggleLike={() => {}}
        onRepost={() => {}}
        onReply={() => {}}
        onQuote={onQuote}
        onDelete={() => {}}
        deleting={false}
      />,
    );

    fireEvent.click(screen.getByTitle("Quote this post with your own comment"));
    expect(onQuote).toHaveBeenCalledWith(post);
  });

  it("offers a quote button on a reply", () => {
    const reply = makePost({ kind: "reply", target: "f".repeat(64) });
    render(
      <PostCard
        post={reply}
        onToggleLike={() => {}}
        onRepost={() => {}}
        onReply={() => {}}
        onQuote={() => {}}
        onDelete={() => {}}
        deleting={false}
      />,
    );

    expect(
      screen.getByTitle("Quote this post with your own comment"),
    ).toBeTruthy();
  });

  it("hides the quote button on a quote, so embeds stay one level deep", () => {
    const quote = makePost({
      kind: "quote",
      target: "f".repeat(64),
      text: "seconded",
      quoted: {
        hash: "f".repeat(64),
        author: "a".repeat(64),
        name: "alice",
        text: "original thought",
        deleted: false,
      },
    });
    render(
      <PostCard
        post={quote}
        onToggleLike={() => {}}
        onRepost={() => {}}
        onReply={() => {}}
        onQuote={() => {}}
        onDelete={() => {}}
        deleting={false}
      />,
    );

    expect(
      screen.queryByTitle("Quote this post with your own comment"),
    ).toBeNull();
  });
});
