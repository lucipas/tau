import { act, fireEvent, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import { shortHash } from "@/lib/format";
import type { TanPost } from "@/lib/types";

import { PostCard, SocialConsole } from "./social-console";

const mockTan = vi.hoisted(() => ({
  current: {} as Record<string, unknown>,
}));

// The search and mentions tests render the whole console, so the data
// hook and the Next link are mocked; the PostCard tests above never
// touch either.
vi.mock("@/lib/use-tan", () => ({
  useTan: vi.fn(() => mockTan.current),
}));

import { useTan } from "@/lib/use-tan";

const mockedUseTan = useTan as Mock;
// The search tests use this name for the same mock.
const mockUseTan = mockedUseTan;
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

  it("reports a blocked original without resurfacing its name or text", () => {
    renderCard(
      makePost({
        kind: "quote",
        text: "worth quoting",
        target: "e".repeat(64),
        quoted: {
          hash: "e".repeat(64),
          author: "",
          name: "",
          text: "",
          deleted: false,
          blocked: true,
        },
      }),
    );

    expect(screen.getByText("worth quoting")).toBeTruthy();
    expect(
      screen.getByText(
        (_content, element) =>
          element?.textContent === "quoting a blocked author's post",
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
      searchResults: [],
      searchQuery: null,
      mentions: [],
      following: [],
      mutedAuthors: [],
      mutedKeywords: [],
      saved: [],
      savedLoading: false,
      savedHasMore: false,
      loadSaved: vi.fn(),
      loadMoreSaved: vi.fn(),
      save: vi.fn(),
      unsave: vi.fn(),
      all: false,
      setAll: vi.fn(),
      showMentions: false,
      setShowMentions: vi.fn(),
      loading: false,
      posting: false,
      syncing: false,
      error: null,
      threadRef: null,
      threadPosts: null,
      threadLoading: false,
      threadError: null,
      openThread: vi.fn(),
      closeThread: vi.fn(),
      refresh: vi.fn(),
      search: vi.fn(),
      clearSearch: vi.fn(),
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

  it("keeps text typed while a keyword mute is in flight", async () => {
    let resolveMute!: (value: unknown) => void;
    const pending = new Promise((resolve) => {
      resolveMute = resolve;
    });
    const tan = setTan({ muteKeyword: vi.fn(() => pending) });
    render(<SocialConsole />);

    const input = screen.getByPlaceholderText("mute a word or phrase");
    fireEvent.change(input, { target: { value: "bitcoin" } });
    fireEvent.click(screen.getByRole("button", { name: "mute" }));
    expect(tan.muteKeyword).toHaveBeenCalledWith("bitcoin");

    // Typing continues while the request is in flight; the clear after
    // success must only drop the text that was actually muted.
    fireEvent.change(input, { target: { value: "bitcoin ethereum" } });
    await act(async () => {
      resolveMute({ ok: true });
    });

    expect(
      (screen.getByPlaceholderText("mute a word or phrase") as HTMLInputElement)
        .value,
    ).toBe("bitcoin ethereum");
  });
});


describe("SocialConsole search", () => {
  const mockSearch = vi.fn();
  const mockClearSearch = vi.fn();

  function makeTan(overrides: Record<string, unknown> = {}) {
    return {
      posts: [],
      searchResults: [],
      searchQuery: null,
      mentions: [],
      following: [],
      mutedAuthors: [],
      mutedKeywords: [],
      all: false,
      setAll: vi.fn(),
      showMentions: false,
      setShowMentions: vi.fn(),
      loading: false,
      posting: false,
      syncing: false,
      error: null,
      refresh: vi.fn(),
      search: mockSearch,
      clearSearch: mockClearSearch,
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
      save: vi.fn(),
      unsave: vi.fn(),
      ...overrides,
    };
  }

  beforeEach(() => {
    vi.resetAllMocks();
  });

  it("submits the query to the search hook", () => {
    const timelinePost = makePost({ text: "timeline post" });
    mockUseTan.mockReturnValue(makeTan({ posts: [timelinePost] }));
    render(<SocialConsole />);

    fireEvent.change(screen.getByLabelText("Search stored posts"), {
      target: { value: "mesh" },
    });
    fireEvent.click(screen.getByRole("button", { name: "search" }));

    expect(mockSearch).toHaveBeenCalledTimes(1);
    expect(mockSearch).toHaveBeenCalledWith("mesh");
  });

  it("shows search results instead of the timeline while searching", () => {
    const timelinePost = makePost({ text: "timeline post" });
    const hit = makePost({ hash: "e".repeat(64), text: "mesh post" });
    mockUseTan.mockReturnValue(
      makeTan({
        posts: [timelinePost],
        searchResults: [hit],
        searchQuery: "mesh",
      }),
    );
    render(<SocialConsole />);

    expect(screen.getByText("mesh post")).toBeTruthy();
    expect(screen.queryByText("timeline post")).toBeNull();
    expect(screen.getByText("search results")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "clear search" }));
    expect(mockClearSearch).toHaveBeenCalledTimes(1);
  });

  it("says when nothing matches the search", () => {
    mockUseTan.mockReturnValue(
      makeTan({ posts: [makePost()], searchResults: [], searchQuery: "zzz" }),
    );
    render(<SocialConsole />);

    expect(screen.getByText("No stored posts match “zzz”")).toBeTruthy();
    expect(screen.queryByText("No posts yet")).toBeNull();
  });
});


describe("SocialConsole mentions view", () => {
  function hookState(overrides: Record<string, unknown> = {}) {
    return {
      posts: [],
      searchResults: [],
      searchQuery: null,
      mentions: [],
      following: [],
      mutedAuthors: [],
      mutedKeywords: [],
      all: false,
      setAll: vi.fn(),
      showMentions: false,
      setShowMentions: vi.fn(),
      loading: false,
      posting: false,
      syncing: false,
      error: null,
      refresh: vi.fn(),
      search: vi.fn(),
      clearSearch: vi.fn(),
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
      save: vi.fn(),
      unsave: vi.fn(),
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
