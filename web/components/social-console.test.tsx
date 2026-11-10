import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";
import type { ReactNode } from "react";

import type { TanPost } from "@/lib/types";

import { PostCard, SocialConsole } from "./social-console";

vi.mock("next/link", () => ({
  default: ({
    href,
    children,
    className,
  }: {
    href: string;
    children: ReactNode;
    className?: string;
  }) => (
    <a href={href} className={className}>
      {children}
    </a>
  ),
}));

vi.mock("@/lib/use-tan", () => ({
  useTan: vi.fn(),
}));

import { useTan } from "@/lib/use-tan";

const mockedUseTan = useTan as Mock;

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

  it("reports a muted original without resurfacing its text", () => {
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
          deleted: false,
          muted: true,
        },
      }),
    );

    expect(screen.getByText("still worth it")).toBeTruthy();
    expect(
      screen.getByText(
        (_content, element) =>
          element?.textContent === "quoting alice · a muted post",
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


describe("SocialConsole mentions view", () => {
  function hookState(overrides: Record<string, unknown> = {}) {
    return {
      posts: [],
      mentions: [],
      following: [],
      authors: [],
      all: false,
      setAll: vi.fn(),
      showMentions: false,
      setShowMentions: vi.fn(),
      loading: false,
      posting: false,
      syncing: false,
      error: null,
      refresh: vi.fn(),
      publish: vi.fn(),
      follow: vi.fn(),
      unfollow: vi.fn(),
      like: vi.fn(),
      unlike: vi.fn(),
      deletePost: vi.fn(),
      sync: vi.fn(),
      threadRef: null,
      threadPosts: null,
      threadLoading: false,
      threadError: null,
      openThread: vi.fn(),
      closeThread: vi.fn(),
      saved: [],
      savedLoading: false,
      savedHasMore: false,
      loadSaved: vi.fn(),
      loadMoreSaved: vi.fn(),
      ...overrides,
    };
  }

  it("switches the feed to mentions and back", () => {
    const timelinePost = makePost({ text: "timeline post" });
    const mention = makePost({
      hash: "e".repeat(64),
      text: "seconded",
      author: "f".repeat(64),
      name: "bob",
      own: false,
    });
    const state = hookState({ posts: [timelinePost], mentions: [mention] });
    mockedUseTan.mockReturnValue(state);

    const { rerender } = render(<SocialConsole />);
    expect(screen.getByText("timeline post")).toBeTruthy();
    expect(screen.queryByText("seconded")).toBeNull();

    fireEvent.click(screen.getByText("mentions"));
    expect(state.setShowMentions).toHaveBeenCalledWith(true);

    mockedUseTan.mockReturnValue({ ...state, showMentions: true });
    rerender(<SocialConsole />);
    expect(screen.queryByText("timeline post")).toBeNull();
    expect(screen.getByText("seconded")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "following" }));
    expect(state.setShowMentions).toHaveBeenCalledWith(false);
    expect(state.setAll).toHaveBeenCalledWith(false);
  });

  it("says there are no replies yet when the mentions list is empty", () => {
    mockedUseTan.mockReturnValue(hookState({ showMentions: true }));

    render(<SocialConsole />);
    expect(screen.getByText("No replies yet")).toBeTruthy();
  });

  it("shows the load error instead of an empty state when the feed fails", () => {
    mockedUseTan.mockReturnValue(
      hookState({
        showMentions: true,
        error: "this node has no identity yet",
      }),
    );

    render(<SocialConsole />);
    expect(screen.getByText("this node has no identity yet")).toBeTruthy();
    expect(screen.queryByText("No replies yet")).toBeNull();
  });
});

describe("SocialConsole authors directory", () => {
  function hookState(overrides: Record<string, unknown> = {}) {
    return {
      posts: [],
      mentions: [],
      following: [],
      authors: [],
      all: false,
      setAll: vi.fn(),
      showMentions: false,
      setShowMentions: vi.fn(),
      loading: false,
      posting: false,
      syncing: false,
      error: null,
      refresh: vi.fn(),
      publish: vi.fn(),
      follow: vi.fn(),
      unfollow: vi.fn(),
      like: vi.fn(),
      unlike: vi.fn(),
      deletePost: vi.fn(),
      sync: vi.fn(),
      threadRef: null,
      threadPosts: null,
      threadLoading: false,
      threadError: null,
      openThread: vi.fn(),
      closeThread: vi.fn(),
      saved: [],
      savedLoading: false,
      savedHasMore: false,
      loadSaved: vi.fn(),
      loadMoreSaved: vi.fn(),
      ...overrides,
    };
  }

  function makeAuthor(overrides: Record<string, unknown> = {}) {
    return {
      author: "a".repeat(64),
      name: "alice",
      posts: 3,
      last_post: 1000,
      followed: false,
      ...overrides,
    };
  }

  it("lists known authors with a follow button for the unfollowed", () => {
    const state = hookState({
      authors: [
        makeAuthor(),
        makeAuthor({
          author: "b".repeat(64),
          name: "bob",
          posts: 1,
          followed: true,
        }),
      ],
    });
    mockedUseTan.mockReturnValue(state);

    render(<SocialConsole />);
    expect(screen.getByText("alice")).toBeTruthy();
    expect(screen.getByText("bob")).toBeTruthy();
    expect(screen.getByText(/3 posts/)).toBeTruthy();

    fireEvent.click(screen.getByTitle("follow this author"));
    expect(state.follow).toHaveBeenCalledTimes(1);
    expect(state.follow).toHaveBeenCalledWith("a".repeat(64));
  });

  it("offers to remove a followed author from the directory", () => {
    const state = hookState({
      authors: [
        makeAuthor({
          author: "b".repeat(64),
          name: "bob",
          posts: 1,
          followed: true,
        }),
      ],
    });
    mockedUseTan.mockReturnValue(state);

    render(<SocialConsole />);
    fireEvent.click(screen.getByTitle("unfollow this author"));
    expect(state.unfollow).toHaveBeenCalledTimes(1);
    expect(state.unfollow).toHaveBeenCalledWith("b".repeat(64));
  });

  it("says there are no authors yet when the store is empty", () => {
    mockedUseTan.mockReturnValue(hookState());

    render(<SocialConsole />);
    expect(
      screen.getByText("No authors yet. Sync to pull posts from your peers."),
    ).toBeTruthy();
  });
});


describe("PostCard save control", () => {
  function renderCard(post: TanPost, onToggleSave = vi.fn()) {
    render(
      <PostCard
        post={post}
        onToggleLike={() => {}}
        onToggleSave={onToggleSave}
        onReply={() => {}}
        onDelete={vi.fn()}
        deleting={false}
      />,
    );
    return onToggleSave;
  }

  it("offers to save an unsaved post and calls back with the post", () => {
    const post = makePost({ saved: false });
    const onToggleSave = renderCard(post);

    fireEvent.click(screen.getByTitle("Save this post to read later"));
    expect(onToggleSave).toHaveBeenCalledTimes(1);
    expect(onToggleSave).toHaveBeenCalledWith(post);
  });

  it("shows a saved post as saved, with when it was saved", () => {
    renderCard(makePost({ saved: true, saved_at: 2000 }));

    expect(screen.getByTitle("Remove from saved posts")).toBeTruthy();
    expect(screen.getByText("★ saved")).toBeTruthy();
    expect(screen.getByText(/saved /)).toBeTruthy();
  });

  it("hides the save control when no handler is given", () => {
    render(
      <PostCard
        post={makePost()}
        onToggleLike={() => {}}
        onReply={() => {}}
        onDelete={vi.fn()}
        deleting={false}
      />,
    );

    expect(screen.queryByTitle("Save this post to read later")).toBeNull();
    expect(screen.queryByTitle("Remove from saved posts")).toBeNull();
  });

  it("shows a deleted saved post as deleted, keeping only unsave", () => {
    renderCard(makePost({ deleted: true, text: "", saved: true }));

    expect(screen.getByText("This post was deleted.")).toBeTruthy();
    expect(screen.queryByTitle("Like this post")).toBeNull();
    expect(screen.queryByTitle("Reply to this post")).toBeNull();
    expect(screen.queryByTitle("Delete this post")).toBeNull();
    // The save toggle stays so the post can still be removed from the list.
    expect(screen.getByTitle("Remove from saved posts")).toBeTruthy();
  });
});

