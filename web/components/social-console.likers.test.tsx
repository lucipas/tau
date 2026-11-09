import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { shortHash } from "@/lib/format";
import type { TanLiker, TanPost } from "@/lib/types";

import { PostCard } from "./social-console";

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
    likes: 2,
    liked: false,
    own: false,
    ...overrides,
  };
}

function renderCard(
  post: TanPost,
  props: {
    onToggleLikers?: (post: TanPost) => void;
    likersOpen?: boolean;
    likers?: TanLiker[] | null;
  } = {},
) {
  const onToggleLikers = props.onToggleLikers ?? vi.fn();
  render(
    <PostCard
      post={post}
      onToggleLike={() => {}}
      onToggleLikers={onToggleLikers}
      likersOpen={props.likersOpen}
      likers={props.likers}
      onReply={() => {}}
      onDelete={() => {}}
      deleting={false}
    />,
  );
  return onToggleLikers;
}

describe("PostCard likers", () => {
  it("shows no count button when the post has no likes", () => {
    renderCard(makePost({ likes: 0 }));
    expect(screen.queryByTitle("Show who liked this post")).toBeNull();
  });

  it("shows the count button when the post has likes", () => {
    renderCard(makePost({ likes: 2 }));
    expect(screen.getByText("2 likes")).toBeTruthy();
  });

  it("opens the liker list from the count button", () => {
    const post = makePost({ likes: 1 });
    const onToggleLikers = renderCard(post);
    fireEvent.click(screen.getByText("1 like"));
    expect(onToggleLikers).toHaveBeenCalledTimes(1);
    expect(onToggleLikers).toHaveBeenCalledWith(post);
  });

  it("lists likers by name, falling back to the short hash", () => {
    const likers: TanLiker[] = [
      { author: "e".repeat(32), name: "bob", liked_at: 1002 },
      { author: "f".repeat(32), name: "", liked_at: 1001 },
    ];
    renderCard(makePost(), { likersOpen: true, likers });
    expect(screen.getByText("liked by")).toBeTruthy();
    expect(screen.getByText("bob")).toBeTruthy();
    expect(screen.getByText(shortHash("f".repeat(32), 16))).toBeTruthy();
  });

  it("shows a loading state while the likers load", () => {
    renderCard(makePost(), { likersOpen: true, likers: null });
    expect(screen.getByText("loading…")).toBeTruthy();
  });

  it("says so when the list loads empty", () => {
    renderCard(makePost(), { likersOpen: true, likers: [] });
    expect(screen.getByText("no likes yet")).toBeTruthy();
  });

  it("hides the list when it is not open", () => {
    renderCard(makePost(), {
      likersOpen: false,
      likers: [{ author: "e".repeat(32), name: "bob", liked_at: 1002 }],
    });
    expect(screen.queryByText("liked by")).toBeNull();
    expect(screen.queryByText("bob")).toBeNull();
  });
});
