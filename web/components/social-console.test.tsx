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
    text: "regret this",
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

describe("PostCard delete control", () => {
  it("asks for confirmation and deletes only after confirming", () => {
    const post = makePost();
    const onDelete = vi.fn();
    render(
      <PostCard
        post={post}
        onToggleLike={() => {}}
        onDelete={onDelete}
        deleting={false}
      />,
    );

    fireEvent.click(screen.getByTitle("Delete this post"));
    expect(onDelete).not.toHaveBeenCalled();
    expect(screen.getByText("confirm delete")).toBeTruthy();

    fireEvent.click(screen.getByText("confirm delete"));
    expect(onDelete).toHaveBeenCalledTimes(1);
    expect(onDelete).toHaveBeenCalledWith(post);
  });

  it("keeps the post when the confirmation is cancelled", () => {
    const onDelete = vi.fn();
    render(
      <PostCard
        post={makePost()}
        onToggleLike={() => {}}
        onDelete={onDelete}
        deleting={false}
      />,
    );

    fireEvent.click(screen.getByTitle("Delete this post"));
    fireEvent.click(screen.getByText("keep"));
    expect(onDelete).not.toHaveBeenCalled();
    expect(screen.queryByText("confirm delete")).toBeNull();
    expect(screen.getByTitle("Delete this post")).toBeTruthy();
  });

  it("hides the delete control on other authors' posts", () => {
    render(
      <PostCard
        post={makePost({ own: false })}
        onToggleLike={() => {}}
        onDelete={vi.fn()}
        deleting={false}
      />,
    );

    expect(screen.queryByTitle("Delete this post")).toBeNull();
  });

  it("shows a disabled state while a delete is in flight", () => {
    const onDelete = vi.fn();
    render(
      <PostCard
        post={makePost()}
        onToggleLike={() => {}}
        onDelete={onDelete}
        deleting
      />,
    );

    expect(screen.getByText("deleting…")).toBeTruthy();
    expect(screen.queryByTitle("Delete this post")).toBeNull();
    fireEvent.click(screen.getByText("deleting…"));
    expect(onDelete).not.toHaveBeenCalled();
  });
});
