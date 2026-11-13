import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type { TanPost, TanScheduledPost } from "@/lib/types";

import { SocialConsole } from "./social-console";

const mockTan = vi.hoisted(() => ({
  current: {} as Record<string, unknown>,
}));

vi.mock("@/lib/use-tan", () => ({
  useTan: vi.fn(() => mockTan.current),
}));

import { useTan } from "@/lib/use-tan";

const mockedUseTan = useTan as Mock;
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
    text: "quotable",
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

function makeScheduled(
  overrides: Partial<TanScheduledPost> = {},
): TanScheduledPost {
  return {
    id: "a1b2c3d4",
    text: "hello later",
    reply_to: null,
    to: [],
    ttl: 3,
    publish_at: 2000,
    created_at: 1000,
    post_hash: null,
    attempts: 0,
    next_attempt_at: 0,
    paused_at: 0,
    pending_peers: [],
    ...overrides,
  };
}

function makeTan(overrides: Record<string, unknown> = {}) {
  return {
    posts: [],
    searchResults: [],
    searchQuery: null,
    mentions: [],
    following: [],
    authors: [],
    authorsTotal: 0,
    loadMoreAuthors: vi.fn(),
    suggestions: [],
    mutedAuthors: [],
    mutedKeywords: [],
    all: false,
    setAll: vi.fn(),
    view: "timeline",
    setView: vi.fn(),
    showMentions: false,
    setShowMentions: vi.fn(),
    loading: false,
    posting: false,
    syncing: false,
    verifying: false,
    verifyReport: null,
    exporting: false,
    importing: false,
    error: null,
    rules: null,
    availableKinds: [],
    rulesLoading: true,
    rulesError: null,
    refresh: vi.fn(),
    refreshRules: vi.fn(),
    updateRules: vi.fn(),
    search: vi.fn(),
    clearSearch: vi.fn(),
    publish: vi.fn(),
    quote: vi.fn(),
    follow: vi.fn(),
    unfollow: vi.fn(),
    muteAuthor: vi.fn(),
    unmuteAuthor: vi.fn(),
    muteKeyword: vi.fn(),
    unmuteKeyword: vi.fn(),
    like: vi.fn(),
    unlike: vi.fn(),
    repost: vi.fn(),
    fetchLikers: vi.fn(),
    deletePost: vi.fn(),
    editPost: vi.fn(),
    sync: vi.fn(),
    verify: vi.fn(),
    exportTan: vi.fn(),
    importTan: vi.fn(),
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
    pinned: [],
    pinnedLoading: false,
    pinnedHasMore: false,
    loadPinned: vi.fn(),
    loadMorePinned: vi.fn(),
    pin: vi.fn(),
    unpin: vi.fn(),
    scheduled: [],
    scheduledLoading: false,
    loadScheduled: vi.fn(),
    schedulePost: vi.fn(),
    cancelScheduled: vi.fn(),
    ...overrides,
  };
}

describe("SocialConsole scheduled posts", () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it("schedules the draft with the picked delay and opens the queue", async () => {
    const schedulePost = vi.fn().mockResolvedValue(undefined);
    const loadScheduled = vi.fn().mockResolvedValue(undefined);
    mockedUseTan.mockReturnValue(
      makeTan({ schedulePost, loadScheduled, scheduled: [] }),
    );
    render(<SocialConsole />);

    fireEvent.change(
      screen.getByPlaceholderText("Write something to the mesh…"),
      { target: { value: "hello later" } },
    );
    fireEvent.change(screen.getByLabelText("Post later"), {
      target: { value: "3600" },
    });
    fireEvent.click(screen.getByRole("button", { name: "schedule" }));

    await waitFor(() =>
      expect(schedulePost).toHaveBeenCalledWith("hello later", 3600, undefined),
    );
    // The queue the post just joined opens, so it is visible instead
    // of disappearing into the file.
    await waitFor(() =>
      expect(screen.getByText("No scheduled posts yet")).toBeTruthy(),
    );
    expect(loadScheduled).toHaveBeenCalled();
  });

  it("lists queued posts with their publish time and cancels one", async () => {
    const entry = makeScheduled({ text: "hello later", publish_at: 2000 });
    const cancelScheduled = vi.fn().mockResolvedValue(undefined);
    mockedUseTan.mockReturnValue(
      makeTan({
        scheduled: [entry],
        cancelScheduled,
        loadScheduled: vi.fn().mockResolvedValue(undefined),
      }),
    );
    render(<SocialConsole />);

    fireEvent.click(screen.getByRole("button", { name: "scheduled" }));

    expect(screen.getByText("hello later")).toBeTruthy();
    expect(screen.getByText(/publishes/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "cancel" }));

    await waitFor(() =>
      expect(cancelScheduled).toHaveBeenCalledWith("a1b2c3d4"),
    );
  });

  it("hides the post-later picker while quoting", () => {
    const post = makePost({ text: "quotable" });
    mockedUseTan.mockReturnValue(makeTan({ posts: [post] }));
    render(<SocialConsole />);

    expect(screen.getByLabelText("Post later")).toBeTruthy();
    fireEvent.click(
      screen.getByTitle("Quote this post with your own comment"),
    );

    expect(screen.queryByLabelText("Post later")).toBeNull();
    expect(screen.getByText(/Quoting alice/)).toBeTruthy();
  });
});
