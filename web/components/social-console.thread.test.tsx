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

function renderCard(post: TanPost, onOpenThread?: (post: TanPost) => void) {
  return render(
    <PostCard
      post={post}
      onToggleLike={() => {}}
      onRepost={() => {}}
      onReply={() => {}}
      onDelete={() => {}}
      deleting={false}
      onOpenThread={onOpenThread}
    />,
  );
}

describe("PostCard thread control", () => {
  it("opens the thread for the post", () => {
    const post = makePost();
    const onOpenThread = vi.fn();
    renderCard(post, onOpenThread);

    fireEvent.click(screen.getByTitle("Open this thread"));
    expect(onOpenThread).toHaveBeenCalledTimes(1);
    expect(onOpenThread).toHaveBeenCalledWith(post);
  });

  it("hides the thread control without a handler", () => {
    renderCard(makePost());

    expect(screen.queryByTitle("Open this thread")).toBeNull();
  });

  it("indents a reply by its thread depth", () => {
    const reply = makePost({
      kind: "reply",
      target: "f".repeat(64),
      text: "nested reply",
      depth: 2,
    });
    renderCard(reply, () => {});

    const article = screen.getByText("nested reply").closest("article");
    expect(article?.style.marginLeft).toBe("28px");
  });

  it("does not indent a top-level post", () => {
    renderCard(makePost(), () => {});

    const article = screen.getByText("hello").closest("article");
    expect(article?.style.marginLeft).toBe("");
  });
});
