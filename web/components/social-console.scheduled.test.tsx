import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
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
  TAN_POLL_INTERVAL_MS: 5000,
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
      expect(schedulePost).toHaveBeenCalledWith(
        "hello later",
        3600,
        undefined,
        undefined,
      ),
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

  it("leaves an open thread when scheduling, so the queue is visible", async () => {
    const post = makePost({ text: "thread root" });
    const schedulePost = vi.fn().mockResolvedValue(undefined);
    const loadScheduled = vi.fn().mockResolvedValue(undefined);
    const closeThread = vi.fn();
    const tanWithThread = makeTan({
      threadRef: post.hash,
      threadPosts: [post],
      closeThread,
      schedulePost,
      loadScheduled,
      scheduled: [],
    });
    mockedUseTan.mockReturnValue(tanWithThread);
    const { rerender } = render(<SocialConsole />);

    // Reply to the thread post, then schedule the reply.
    fireEvent.click(screen.getByTitle("Reply to this post"));
    fireEvent.change(screen.getByPlaceholderText("Reply to alice…"), {
      target: { value: "agreed, later" },
    });
    fireEvent.change(screen.getByLabelText("Post later"), {
      target: { value: "3600" },
    });
    fireEvent.click(screen.getByRole("button", { name: "schedule" }));

    await waitFor(() =>
      expect(schedulePost).toHaveBeenCalledWith(
        "agreed, later",
        3600,
        post.hash,
        undefined,
      ),
    );
    // The thread view renders first and would hide the queue, so
    // scheduling closes it.
    expect(closeThread).toHaveBeenCalledTimes(1);
    // The mock hook holds the thread open until the test closes it, the
    // way the real hook's closeThread does; then the queue shows.
    mockedUseTan.mockReturnValue({
      ...tanWithThread,
      threadRef: null,
      threadPosts: null,
    });
    rerender(<SocialConsole />);
    await waitFor(() =>
      expect(screen.getByText("No scheduled posts yet")).toBeTruthy(),
    );
  });

  it("re-reads the queue while the scheduled view stays open", async () => {
    vi.useFakeTimers();
    try {
      const entry = makeScheduled();
      const loadScheduled = vi.fn().mockResolvedValue(undefined);
      mockedUseTan.mockReturnValue(
        makeTan({ scheduled: [entry], loadScheduled }),
      );
      render(<SocialConsole />);

      fireEvent.click(screen.getByRole("button", { name: "scheduled" }));
      const callsAfterOpen = loadScheduled.mock.calls.length;
      expect(callsAfterOpen).toBeGreaterThan(0);

      await act(async () => {
        vi.advanceTimersByTime(5000);
      });
      expect(loadScheduled.mock.calls.length).toBeGreaterThan(callsAfterOpen);

      // Leaving the view stops the refresh.
      fireEvent.click(screen.getByRole("button", { name: "following" }));
      const callsAfterClose = loadScheduled.mock.calls.length;
      await act(async () => {
        vi.advanceTimersByTime(10000);
      });
      expect(loadScheduled.mock.calls.length).toBe(callsAfterClose);
    } finally {
      vi.useRealTimers();
    }
  });

  it("publishes the draft with the picked expiry and clears the picker", async () => {
    const publish = vi.fn().mockResolvedValue(undefined);
    mockedUseTan.mockReturnValue(makeTan({ publish }));
    render(<SocialConsole />);

    fireEvent.change(
      screen.getByPlaceholderText("Write something to the mesh…"),
      { target: { value: "vanish soon" } },
    );
    fireEvent.change(screen.getByLabelText("Post expiry"), {
      target: { value: "3600" },
    });
    fireEvent.click(screen.getByRole("button", { name: "post" }));

    await waitFor(() =>
      expect(publish).toHaveBeenCalledWith("vanish soon", undefined, 3600),
    );
    // The expiry applies to one post only: the picker resets.
    expect(
      (screen.getByLabelText("Post expiry") as HTMLSelectElement).value,
    ).toBe("");
  });

  it("schedules the draft with the picked expiry", async () => {
    const schedulePost = vi.fn().mockResolvedValue(undefined);
    const loadScheduled = vi.fn().mockResolvedValue(undefined);
    mockedUseTan.mockReturnValue(
      makeTan({ schedulePost, loadScheduled, scheduled: [] }),
    );
    render(<SocialConsole />);

    fireEvent.change(
      screen.getByPlaceholderText("Write something to the mesh…"),
      { target: { value: "vanish later" } },
    );
    fireEvent.change(screen.getByLabelText("Post later"), {
      target: { value: "3600" },
    });
    fireEvent.change(screen.getByLabelText("Post expiry"), {
      target: { value: "86400" },
    });
    fireEvent.click(screen.getByRole("button", { name: "schedule" }));

    await waitFor(() =>
      expect(schedulePost).toHaveBeenCalledWith(
        "vanish later",
        3600,
        undefined,
        86400,
      ),
    );
    expect(
      (screen.getByLabelText("Post expiry") as HTMLSelectElement).value,
    ).toBe("");
  });

  it("hides the expiry picker while quoting", () => {
    const post = makePost({ text: "quotable" });
    mockedUseTan.mockReturnValue(makeTan({ posts: [post] }));
    render(<SocialConsole />);

    expect(screen.getByLabelText("Post expiry")).toBeTruthy();
    fireEvent.click(
      screen.getByTitle("Quote this post with your own comment"),
    );

    expect(screen.queryByLabelText("Post expiry")).toBeNull();
  });
});
