import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { shortHash } from "@/lib/format";
import type { TanPost, TanQuoter, TanReposter } from "@/lib/types";

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
    likes: 0,
    liked: false,
    reposts: 2,
    reposted: false,
    quotes: 1,
    own: false,
    ...overrides,
  };
}

function renderCard(
  post: TanPost,
  props: {
    onToggleReposters?: (post: TanPost) => void;
    repostersOpen?: boolean;
    reposters?: TanReposter[] | null;
    repostersTotal?: number;
    onToggleQuoters?: (post: TanPost) => void;
    quotersOpen?: boolean;
    quoters?: TanQuoter[] | null;
    quotersTotal?: number;
  } = {},
) {
  const onToggleReposters = props.onToggleReposters ?? vi.fn();
  const onToggleQuoters = props.onToggleQuoters ?? vi.fn();
  render(
    <PostCard
      post={post}
      onToggleLike={() => {}}
      onRepost={() => {}}
      onToggleReposters={onToggleReposters}
      repostersOpen={props.repostersOpen}
      reposters={props.reposters}
      repostersTotal={props.repostersTotal}
      onToggleQuoters={onToggleQuoters}
      quotersOpen={props.quotersOpen}
      quoters={props.quoters}
      quotersTotal={props.quotersTotal}
      onReply={() => {}}
      onDelete={() => {}}
      deleting={false}
    />,
  );
  return { onToggleReposters, onToggleQuoters };
}

describe("PostCard reposters", () => {
  it("keeps the ↻ 0 count on a post with no reposts", () => {
    renderCard(makePost({ reposts: 0, quotes: 0 }));
    expect(screen.getByText("↻ 0")).toBeTruthy();
    expect(screen.queryByTitle("Show who reposted this post")).toBeNull();
  });

  it("shows the count and a who-reposted button otherwise", () => {
    renderCard(makePost({ reposts: 2, quotes: 0 }));
    expect(screen.getByText("↻ 2")).toBeTruthy();
    expect(screen.getByText("who reposted")).toBeTruthy();
  });

  it("opens the reposter list from the who-reposted button", () => {
    const post = makePost({ reposts: 1, quotes: 0 });
    const { onToggleReposters } = renderCard(post);
    fireEvent.click(screen.getByText("who reposted"));
    expect(onToggleReposters).toHaveBeenCalledTimes(1);
    expect(onToggleReposters).toHaveBeenCalledWith(post);
  });

  it("lists reposters by name, falling back to the short hash", () => {
    const reposters: TanReposter[] = [
      { author: "e".repeat(32), name: "bob", reposted_at: 1002 },
      { author: "f".repeat(32), name: "", reposted_at: 1001 },
    ];
    renderCard(makePost(), { repostersOpen: true, reposters });
    expect(screen.getByText("reposted by")).toBeTruthy();
    expect(screen.getByText("bob")).toBeTruthy();
    expect(screen.getByText(shortHash("f".repeat(32), 16))).toBeTruthy();
  });

  it("shows a loading state while the reposters load", () => {
    renderCard(makePost(), { repostersOpen: true, reposters: null });
    expect(screen.getByText("loading…")).toBeTruthy();
  });

  it("says so when the list loads empty", () => {
    renderCard(makePost(), { repostersOpen: true, reposters: [] });
    expect(screen.getByText("no reposts yet")).toBeTruthy();
  });

  it("says when the list is truncated by the server cap", () => {
    renderCard(makePost(), {
      repostersOpen: true,
      reposters: [{ author: "e".repeat(32), name: "bob", reposted_at: 1002 }],
      repostersTotal: 3,
    });
    expect(screen.getByText("showing first 1 of 3 reposters")).toBeTruthy();
  });

  it("says nothing about truncation when the list is complete", () => {
    renderCard(makePost(), {
      repostersOpen: true,
      reposters: [{ author: "e".repeat(32), name: "bob", reposted_at: 1002 }],
      repostersTotal: 1,
    });
    expect(screen.queryByText(/showing first/)).toBeNull();
  });

  it("hides the list when it is not open", () => {
    renderCard(makePost(), {
      repostersOpen: false,
      reposters: [{ author: "e".repeat(32), name: "bob", reposted_at: 1002 }],
    });
    expect(screen.queryByText("reposted by")).toBeNull();
    expect(screen.queryByText("bob")).toBeNull();
  });
});

describe("PostCard quoters", () => {
  it("shows no quote button on a post with no quotes", () => {
    renderCard(makePost({ reposts: 0, quotes: 0 }));
    expect(screen.queryByTitle("Show the quotes of this post")).toBeNull();
  });

  it("shows the count and a who-quoted button otherwise", () => {
    renderCard(makePost({ reposts: 0, quotes: 2 }));
    expect(screen.getByText("❝ 2")).toBeTruthy();
    expect(screen.getByText("who quoted")).toBeTruthy();
  });

  it("opens the quoter list from the quote button", () => {
    const post = makePost({ reposts: 0, quotes: 1 });
    const { onToggleQuoters } = renderCard(post);
    fireEvent.click(screen.getByTitle("Show the quotes of this post"));
    expect(onToggleQuoters).toHaveBeenCalledTimes(1);
    expect(onToggleQuoters).toHaveBeenCalledWith(post);
  });

  it("lists quoters by name with their comment, falling back to the short hash", () => {
    const quoters: TanQuoter[] = [
      {
        author: "e".repeat(32),
        name: "bob",
        hash: "1".repeat(64),
        text: "hard agree",
        quoted_at: 1002,
      },
      {
        author: "f".repeat(32),
        name: "",
        hash: "2".repeat(64),
        text: "seconded",
        quoted_at: 1001,
      },
    ];
    renderCard(makePost(), { quotersOpen: true, quoters });
    expect(screen.getByText("quoted by")).toBeTruthy();
    expect(screen.getByText("bob")).toBeTruthy();
    expect(screen.getByText("hard agree")).toBeTruthy();
    expect(screen.getByText(shortHash("f".repeat(32), 16))).toBeTruthy();
    expect(screen.getByText("seconded")).toBeTruthy();
  });

  it("shows a loading state while the quotes load", () => {
    renderCard(makePost(), { quotersOpen: true, quoters: null });
    expect(screen.getByText("loading…")).toBeTruthy();
  });

  it("says so when the list loads empty", () => {
    renderCard(makePost(), { quotersOpen: true, quoters: [] });
    expect(screen.getByText("no quotes yet")).toBeTruthy();
  });

  it("says when the list is truncated by the server cap", () => {
    renderCard(makePost(), {
      quotersOpen: true,
      quoters: [
        {
          author: "e".repeat(32),
          name: "bob",
          hash: "1".repeat(64),
          text: "hard agree",
          quoted_at: 1002,
        },
      ],
      quotersTotal: 3,
    });
    expect(screen.getByText("showing first 1 of 3 quotes")).toBeTruthy();
  });

  it("says nothing about truncation when the list is complete", () => {
    renderCard(makePost(), {
      quotersOpen: true,
      quoters: [
        {
          author: "e".repeat(32),
          name: "bob",
          hash: "1".repeat(64),
          text: "hard agree",
          quoted_at: 1002,
        },
      ],
      quotersTotal: 1,
    });
    expect(screen.queryByText(/showing first/)).toBeNull();
  });

  it("hides the list when it is not open", () => {
    renderCard(makePost(), {
      quotersOpen: false,
      quoters: [
        {
          author: "e".repeat(32),
          name: "bob",
          hash: "1".repeat(64),
          text: "hard agree",
          quoted_at: 1002,
        },
      ],
    });
    expect(screen.queryByText("quoted by")).toBeNull();
    expect(screen.queryByText("hard agree")).toBeNull();
  });
});
