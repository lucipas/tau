import { fireEvent, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { TanPost } from "@/lib/types";

import { PostCard, SocialConsole } from "./social-console";

// The search tests render the whole console, so the data hook and the
// Next link are mocked; the PostCard tests above never touch either.
const { mockUseTan } = vi.hoisted(() => ({ mockUseTan: vi.fn() }));

vi.mock("@/lib/use-tan", () => ({
  useTan: (...args: unknown[]) => mockUseTan(...args),
}));
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


describe("SocialConsole search", () => {
  const mockSearch = vi.fn();
  const mockClearSearch = vi.fn();

  function makeTan(overrides: Record<string, unknown> = {}) {
    return {
      posts: [],
      searchResults: [],
      searchQuery: null,
      following: [],
      all: false,
      setAll: vi.fn(),
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
      like: vi.fn(),
      unlike: vi.fn(),
      deletePost: vi.fn(),
      sync: vi.fn(),
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
