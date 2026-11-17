import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";

import type { TanPost } from "@/lib/types";
import type { UseTanResult } from "@/lib/use-tan";
import type { UseTauResult } from "@/lib/use-tau";

import { SocialTab } from "./social-tab";

function makePost(overrides: Partial<TanPost> = {}): TanPost {
  return {
    v: 1,
    author: "a".repeat(64),
    key: "key",
    name: "Alice",
    text: "Hello #mesh world",
    created_at: 1763400000,
    prev: "",
    hash: "hash-alice-1",
    sig: "sig",
    likes: 3,
    liked: false,
    reposts: 1,
    quotes: 0,
    own: false,
    ...overrides,
  };
}

function makeTan(overrides: Partial<UseTanResult> = {}): UseTanResult {
  return {
    posts: [makePost()],
    view: "timeline",
    setView: vi.fn(),
    showAll: false,
    setShowAll: vi.fn(),
    tagFilter: null,
    setTagFilter: vi.fn(),
    searchQuery: null,
    setSearchQuery: vi.fn(),
    searchResults: [],
    mentions: [],
    following: [],
    mutedAuthors: [],
    mutedKeywords: [],
    followedTags: [],
    mutedTags: [],
    tags: [{ tag: "mesh", count: 5 }],
    suggestions: [{ author: "b".repeat(64), name: "Bob", posts: 4, likes: 9, reason: "active", last_post: 1763400000 }],
    authors: [],
    threadRef: null,
    threadPosts: [],
    threadError: null,
    scheduled: [],
    rules: null,
    availableKinds: [],
    loading: false,
    error: null,
    posting: false,
    syncing: false,
    refresh: vi.fn(),
    load: vi.fn(),
    publish: vi.fn(),
    publishPoll: vi.fn(),
    quote: vi.fn(),
    like: vi.fn(),
    repost: vi.fn(),
    share: vi.fn(),
    vote: vi.fn(),
    deletePost: vi.fn(),
    editPost: vi.fn(),
    follow: vi.fn(),
    muteAuthor: vi.fn(),
    muteKeyword: vi.fn(),
    followTag: vi.fn(),
    muteTag: vi.fn(),
    savePost: vi.fn(),
    hidePost: vi.fn(),
    pinPost: vi.fn(),
    schedulePost: vi.fn(),
    cancelScheduled: vi.fn(),
    updateRules: vi.fn(),
    sync: vi.fn(),
    exportTan: vi.fn(),
    importTan: vi.fn(),
    fetchLikers: vi.fn(),
    fetchReposters: vi.fn(),
    fetchQuoters: vi.fn(),
    fetchSaved: vi.fn(),
    fetchHidden: vi.fn(),
    fetchPinned: vi.fn(),
    verify: vi.fn(),
    openThread: vi.fn(),
    ...overrides,
  } as unknown as UseTanResult;
}

function makeTau(): UseTauResult {
  return {
    peers: [
      {
        destination: "c".repeat(64),
        name: "Carol",
        last_seen: 0,
        age: 0,
        hops: 1,
        protocol: 1,
      },
    ],
    groups: [{ id: "group-1", name: "Test group", members: [], created_at: 0 }],
  } as unknown as UseTauResult;
}

describe("SocialTab", () => {
  it("renders the composer and the feed", () => {
    render(<SocialTab tau={makeTau()} tan={makeTan()} />);
    expect(
      screen.getByLabelText("New post") as HTMLTextAreaElement,
    ).toBeDefined();
    expect(
      screen.getByRole("button", { name: "Post" }),
    ).toBeDefined();
    expect(screen.getByText("Alice")).toBeDefined();
    expect(screen.getByText(/Hello/)).toBeDefined();
  });

  it("renders like and reply actions on a post", () => {
    const tan = makeTan();
    render(<SocialTab tau={makeTau()} tan={tan} />);
    expect(screen.getByRole("button", { name: "♡ 3" })).toBeDefined();
    expect(
      screen.getByRole("button", { name: "Reply" }),
    ).toBeDefined();
    expect(
      screen.getByRole("button", { name: /Repost/ }),
    ).toBeDefined();
  });

  it("calls like when the like button is clicked", () => {
    const tan = makeTan();
    render(<SocialTab tau={makeTau()} tan={tan} />);
    fireEvent.click(screen.getByRole("button", { name: "♡ 3" }));
    expect(tan.like).toHaveBeenCalledWith("hash-alice-1", true);
  });

  it("calls publish when the composer is submitted", () => {
    const tan = makeTan();
    render(<SocialTab tau={makeTau()} tan={tan} />);
    fireEvent.change(screen.getByLabelText("New post"), {
      target: { value: "a fresh post" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Post" }));
    expect(tan.publish).toHaveBeenCalledWith(
      expect.objectContaining({ text: "a fresh post" }),
    );
  });

  it("filters by tag when a tag in a post is clicked", () => {
    const tan = makeTan();
    render(<SocialTab tau={makeTau()} tan={tan} />);
    fireEvent.click(screen.getByRole("button", { name: "#mesh" }));
    expect(tan.setTagFilter).toHaveBeenCalledWith("mesh");
    expect(tan.setSearchQuery).toHaveBeenCalledWith(null);
  });

  it("opens the thread and marks the reply when Reply is clicked", () => {
    const tan = makeTan();
    render(<SocialTab tau={makeTau()} tan={tan} />);
    fireEvent.click(screen.getByRole("button", { name: "Reply" }));
    expect(tan.openThread).toHaveBeenCalledWith("hash-alice-1");
    expect(screen.getByText(/Replying to Alice/)).toBeDefined();
  });

  it("shows the Discover section with tags and suggestions", () => {
    render(<SocialTab tau={makeTau()} tan={makeTan()} />);
    expect(screen.getByText("Discover")).toBeDefined();
    expect(
      screen.getByRole("button", { name: /#mesh · 5/ }),
    ).toBeDefined();
    expect(screen.getByText("Bob")).toBeDefined();
  });

  it("shows edit and delete only for own posts", () => {
    const own = makePost({ hash: "hash-mine", name: "Me", own: true });
    const tan = makeTan({ posts: [own] });
    render(<SocialTab tau={makeTau()} tan={tan} />);
    expect(screen.getByRole("button", { name: "Edit" })).toBeDefined();
    expect(screen.getByRole("button", { name: "Delete" })).toBeDefined();
  });

  it("renders a poll with vote buttons", () => {
    const pollPost = makePost({
      hash: "hash-poll",
      text: "Best mesh?",
      poll: {
        options: ["Reticulum", "LoRa"],
        counts: [2, 1],
        total: 3,
        voted: null,
      },
    });
    render(<SocialTab tau={makeTau()} tan={makeTan({ posts: [pollPost] })} />);
    expect(screen.getByRole("button", { name: /Reticulum/ })).toBeDefined();
    expect(screen.getByRole("button", { name: /LoRa/ })).toBeDefined();
    expect(screen.getByText(/3 votes/)).toBeDefined();
  });

  it("shows the thread view with a back button when a thread is open", () => {
    const threadPost = makePost({ hash: "hash-thread", text: "thread root" });
    const tan = makeTan({
      threadRef: "hash-thread",
      threadPosts: [threadPost],
    });
    render(<SocialTab tau={makeTau()} tan={tan} />);
    expect(screen.getByText("Thread")).toBeDefined();
    expect(screen.getByText("thread root")).toBeDefined();
    fireEvent.click(
      screen.getByRole("button", { name: /Back to feed/ }),
    );
    expect(tan.openThread).toHaveBeenCalledWith(null);
  });
});
