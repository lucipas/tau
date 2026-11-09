import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { shortHash } from "@/lib/format";
import type { TanPost } from "@/lib/types";

import { PostCard, SocialConsole } from "./social-console";

const mockTan = vi.hoisted(() => ({
  current: {} as Record<string, unknown>,
}));

vi.mock("@/lib/use-tan", () => ({
  useTan: () => mockTan.current,
}));

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
        onReply={() => {}}
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
        onReply={() => {}}
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
        onReply={() => {}}
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
        onReply={() => {}}
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

describe("PostCard quote rendering", () => {
  function renderCard(post: TanPost) {
    render(
      <PostCard
        post={post}
        onToggleLike={() => {}}
        onReply={() => {}}
        onDelete={vi.fn()}
        deleting={false}
      />,
    );
  }

  it("shows a quote's comment with the quoted original beneath it", () => {
    renderCard(
      makePost({
        kind: "quote",
        text: "worth sharing",
        target: "e".repeat(64),
        quoted: {
          hash: "e".repeat(64),
          author: "f".repeat(32),
          name: "alice",
          text: "original thought",
          deleted: false,
        },
      }),
    );

    expect(screen.getByText("worth sharing")).toBeTruthy();
    expect(
      screen.getByText(
        (_content, element) =>
          element?.textContent === "quoting alice · original thought",
      ),
    ).toBeTruthy();
  });

  it("reports a deleted original without resurfacing its text", () => {
    renderCard(
      makePost({
        kind: "quote",
        text: "still worth it",
        target: "e".repeat(64),
        quoted: {
          hash: "e".repeat(64),
          author: "f".repeat(32),
          name: "alice",
          text: "",
          deleted: true,
        },
      }),
    );

    expect(screen.getByText("still worth it")).toBeTruthy();
    expect(
      screen.getByText(
        (_content, element) =>
          element?.textContent === "quoting alice · a deleted post",
      ),
    ).toBeTruthy();
  });

  it("shows the quote count on the quoted post and hides it at zero", () => {
    const { rerender } = render(
      <PostCard
        post={makePost({ quotes: 2 })}
        onToggleLike={() => {}}
        onReply={() => {}}
        onDelete={vi.fn()}
        deleting={false}
      />,
    );
    expect(screen.getByText("❝ 2")).toBeTruthy();

    rerender(
      <PostCard
        post={makePost({ quotes: 0 })}
        onToggleLike={() => {}}
        onReply={() => {}}
        onDelete={vi.fn()}
        deleting={false}
      />,
    );
    expect(screen.queryByText(/❝/)).toBeNull();
  });
});

describe("PostCard mute control", () => {
  function renderCard(post: TanPost, onMute = vi.fn()) {
    render(
      <PostCard
        post={post}
        onToggleLike={() => {}}
        onReply={() => {}}
        onDelete={vi.fn()}
        deleting={false}
        onMute={onMute}
      />,
    );
    return onMute;
  }

  it("shows a mute button on other authors' posts and calls onMute", () => {
    const post = makePost({ own: false });
    const onMute = renderCard(post);

    fireEvent.click(screen.getByTitle("Mute this author"));
    expect(onMute).toHaveBeenCalledTimes(1);
    expect(onMute).toHaveBeenCalledWith(post);
  });

  it("hides the mute control on your own posts", () => {
    renderCard(makePost({ own: true }));

    expect(screen.queryByTitle("Mute this author")).toBeNull();
  });

  it("shows a disabled state while a mute is in flight", () => {
    const post = makePost({ own: false });
    const onMute = vi.fn();
    render(
      <PostCard
        post={post}
        onToggleLike={() => {}}
        onReply={() => {}}
        onDelete={vi.fn()}
        deleting={false}
        onMute={onMute}
        muting
      />,
    );

    expect(screen.getByText("muting…")).toBeTruthy();
    expect(screen.queryByTitle("Mute this author")).toBeNull();
    fireEvent.click(screen.getByText("muting…"));
    expect(onMute).not.toHaveBeenCalled();
  });
});

describe("SocialConsole muted sidebar", () => {
  const authorHash = "a".repeat(64);

  function setTan(overrides: Record<string, unknown>) {
    mockTan.current = {
      posts: [],
      following: [],
      mutedAuthors: [],
      mutedKeywords: [],
      all: false,
      setAll: vi.fn(),
      loading: false,
      posting: false,
      syncing: false,
      error: null,
      refresh: vi.fn(),
      publish: vi.fn(),
      follow: vi.fn(),
      unfollow: vi.fn(),
      muteAuthor: vi.fn(),
      unmuteAuthor: vi.fn(),
      muteKeyword: vi.fn(),
      unmuteKeyword: vi.fn(),
      like: vi.fn(),
      unlike: vi.fn(),
      deletePost: vi.fn(),
      sync: vi.fn(),
      ...overrides,
    };
    return mockTan.current;
  }

  it("shows the empty state when nothing is muted", () => {
    setTan({});
    render(<SocialConsole />);

    expect(screen.getByText(/Nothing muted\./)).toBeTruthy();
  });

  it("lists muted authors and keywords and unmutes each by its key", () => {
    const tan = setTan({
      mutedAuthors: [
        { author: authorHash, name: "bob", muted_at: 1, posts: 3, seq: 3, last_post: 1 },
      ],
      mutedKeywords: [
        { keyword: "crypto", display: "Crypto", added_at: 1 },
      ],
    });
    render(<SocialConsole />);

    expect(screen.queryByText(/Nothing muted\./)).toBeNull();
    expect(screen.getByText("bob")).toBeTruthy();
    expect(screen.getByText(`${shortHash(authorHash, 10)} · 3 posts`)).toBeTruthy();
    expect(screen.getByText("“Crypto”")).toBeTruthy();

    fireEvent.click(screen.getByTitle("unmute this author"));
    expect(tan.unmuteAuthor).toHaveBeenCalledWith(authorHash);

    fireEvent.click(screen.getByTitle("unmute this keyword"));
    expect(tan.unmuteKeyword).toHaveBeenCalledWith("Crypto");
  });

  it("submits a typed keyword through muteKeyword", () => {
    const tan = setTan({});
    render(<SocialConsole />);

    fireEvent.change(screen.getByPlaceholderText("mute a word or phrase"), {
      target: { value: "bitcoin" },
    });
    fireEvent.click(screen.getByRole("button", { name: "mute" }));
    expect(tan.muteKeyword).toHaveBeenCalledWith("bitcoin");
  });
});
