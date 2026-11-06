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

describe("PostCard reply display", () => {
  it("shows the parent name and text on a reply", () => {
    const reply = makePost({
      kind: "reply",
      target: "f".repeat(64),
      text: "seconded",
      reply_to: {
        hash: "f".repeat(64),
        author: "a".repeat(64),
        name: "alice",
        text: "original thought",
        deleted: false,
      },
    });
    render(
      <PostCard post={reply} onToggleLike={() => {}} onReply={() => {}} onDelete={() => {}} deleting={false} />,
    );

    // The author name appears twice (header + parent line), so assert
    // on the reply line itself.
    const line = screen.getByText(/in reply to/);
    expect(line.textContent).toContain("alice");
    expect(line.textContent).toContain("original thought");
  });

  it("reports a deleted parent without resurfacing its text", () => {
    const reply = makePost({
      kind: "reply",
      target: "f".repeat(64),
      text: "too late",
      reply_to: {
        hash: "f".repeat(64),
        author: "a".repeat(64),
        name: "alice",
        text: "",
        deleted: true,
      },
    });
    render(
      <PostCard post={reply} onToggleLike={() => {}} onReply={() => {}} onDelete={() => {}} deleting={false} />,
    );

    expect(screen.getByText(/a deleted post/)).toBeTruthy();
  });

  it("falls back to the parent hash when the parent has not arrived", () => {
    const reply = makePost({
      kind: "reply",
      target: "f".repeat(64),
      text: "early reply",
      reply_to: null,
    });
    render(
      <PostCard post={reply} onToggleLike={() => {}} onReply={() => {}} onDelete={() => {}} deleting={false} />,
    );

    expect(screen.getByText(/in reply to/)).toBeTruthy();
    expect(screen.getByText(/ffffffffffffffff/)).toBeTruthy();
  });

  it("calls onReply with the post when the reply button is clicked", () => {
    const post = makePost();
    const onReply = vi.fn();
    render(
      <PostCard post={post} onToggleLike={() => {}} onReply={onReply} onDelete={() => {}} deleting={false} />,
    );

    fireEvent.click(screen.getByTitle("Reply to this post"));
    expect(onReply).toHaveBeenCalledTimes(1);
    expect(onReply).toHaveBeenCalledWith(post);
  });

  it("shows no reply line on a standalone post", () => {
    render(
      <PostCard
        post={makePost()}
        onToggleLike={() => {}}
        onReply={() => {}}
        onDelete={() => {}}
        deleting={false}
      />,
    );

    expect(screen.queryByText(/in reply to/)).toBeNull();
  });
});
