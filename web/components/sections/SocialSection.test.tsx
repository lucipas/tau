// SocialSection.test.tsx — feed renders, tabs switch endpoints, like
// posts, thread opens, composer validates. Fetch is stubbed; the section
// runs inside a TauContext.Provider with a fake node.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { TauContext } from "@/components/chrome/TauContext";
import type { TanPost } from "@/lib/types";
import SocialSection from "./SocialSection";
import { validateTanText } from "./social/TanComposer";
import { fakeTau, mockFetch } from "./test-utils";

function makePost(hash: string, text: string, extra: Partial<TanPost> = {}): TanPost {
  return {
    v: 1,
    hash,
    author: "a".repeat(32),
    key: "a".repeat(32),
    name: "alice",
    text,
    created_at: Math.floor(Date.now() / 1000) - 60,
    prev: "",
    sig: "s".repeat(128),
    kind: "post",
    target: "",
    expires_at: null,
    likes: 3,
    liked: false,
    own: false,
    quotes: 1,
    replies: 2,
    ...extra,
  };
}

let calls: string[] = [];
let timeline: TanPost[] = [];
let scheduled: import("@/lib/types").ScheduledTanPost[] = [];

beforeEach(() => {
  timeline = [makePost("h1".padEnd(64, "0"), "hello #mesh @bob")];
  scheduled = [];
  const router = mockFetch((url) => {
    if (url.includes("/api/tan/timeline")) return { posts: timeline, count: timeline.length };
    if (url.includes("/api/tan/search")) return { posts: timeline, count: timeline.length };
    if (url.includes("/api/tan/thread")) return { posts: timeline, count: timeline.length };
    if (url.includes("/api/tan/tags")) return { tags: [{ tag: "mesh", count: 5 }], count: 1 };
    if (url.includes("/api/tan/following")) return { following: [], count: 0 };
    if (url.includes("/api/tan/suggest")) return { suggestions: [], count: 0 };
    if (url.includes("/api/tan/saved")) return { posts: [], count: 0, next_offset: null };
    if (url.includes("/api/tan/hidden")) return { posts: [], count: 0, next_offset: null };
    if (url.includes("/api/tan/pinned")) return { posts: [], count: 0, next_offset: null };
    if (url.includes("/api/tan/scheduled")) return { scheduled, count: scheduled.length };
    if (url.includes("/api/tan/muted-keywords")) return { muted_keywords: [], count: 0 };
    if (url.includes("/api/tan/muted-tags")) return { muted_tags: [], count: 0 };
    if (url.includes("/api/tan/muted")) return { muted: [], count: 0 };
    if (url.includes("/api/tan/followed-tags")) return { followed_tags: [], count: 0 };
    if (url.includes("/api/tan/authors")) return { authors: [], count: 0, total: 0 };
    if (url.includes("/api/tan/likes")) return { target: "x", likers: [], count: 0, total: 0 };
    return { ok: true };
  });
  calls = router.calls;
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function renderSocial() {
  return render(
    <TauContext.Provider value={fakeTau()}>
      <SocialSection />
    </TauContext.Provider>,
  );
}

describe("validateTanText", () => {
  it("rejects empty posts and over-long posts", () => {
    expect(validateTanText("")).toMatch(/Write something/);
    expect(validateTanText("   ")).toMatch(/Write something/);
    expect(validateTanText("x".repeat(2001))).toMatch(/limit/);
    expect(validateTanText("hello mesh")).toBeNull();
  });
});

describe("SocialSection", () => {
  it("renders the composer and the feed posts", async () => {
    renderSocial();
    expect(screen.getByLabelText("Compose a post")).toBeTruthy();
    expect(await screen.findByText(/hello/)).toBeTruthy();
    // Hashtag rendered as a button, not raw HTML.
    expect(screen.getByRole("button", { name: "#mesh" })).toBeTruthy();
  });

  it("shows like / reply / quote counts on a post", async () => {
    renderSocial();
    await screen.findByText(/hello/);
    expect(screen.getByLabelText(/Like post, 3 likes/)).toBeTruthy();
    expect(screen.getByLabelText(/Reply to post, 2 replies/)).toBeTruthy();
    expect(screen.getByLabelText(/Show quotes, 1 quotes/)).toBeTruthy();
  });

  it("switching to the All tab fetches the all timeline", async () => {
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.click(screen.getByRole("button", { name: "All" }));
    await waitFor(() =>
      expect(calls).toContain("GET /api/tan/timeline?limit=30&all=1"),
    );
  });

  it("liking a post posts to /api/tan/like", async () => {
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.click(screen.getByLabelText(/Like post, 3 likes/));
    await waitFor(() => expect(calls).toContain("POST /api/tan/like"));
  });

  it("posts typed text and clears the composer", async () => {
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.change(screen.getByLabelText("Compose a post"), {
      target: { value: "fresh post" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Post" }));
    await waitFor(() => expect(calls).toContain("POST /api/tan/posts"));
    await waitFor(() =>
      expect((screen.getByLabelText("Compose a post") as HTMLTextAreaElement).value).toBe(""),
    );
  });

  it("validates polls before posting", async () => {
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.click(screen.getByRole("button", { name: /Poll/ }));
    fireEvent.change(screen.getByLabelText("Poll question"), {
      target: { value: "Best mesh?" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Post poll" }));
    expect(screen.getByRole("alert").textContent).toMatch(/two options/);
    expect(calls).not.toContain("POST /api/tan/poll");
  });

  it("clicking a post opens its thread", async () => {
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.click(screen.getByText("alice"));
    expect(await screen.findByText("Thread")).toBeTruthy();
    await waitFor(() =>
      expect(calls.some((c) => c.startsWith("GET /api/tan/thread"))).toBe(true),
    );
    fireEvent.click(screen.getByRole("button", { name: /Back/ }));
    expect(await screen.findByText(/hello/)).toBeTruthy();
  });

  it("searches posts and shows the result count", async () => {
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.change(screen.getByLabelText("Search posts"), {
      target: { value: "mesh" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    await waitFor(() =>
      expect(calls).toContain("GET /api/tan/search?q=mesh&limit=30"),
    );
    expect(await screen.findByText(/1 result for/)).toBeTruthy();
  });

  it("shows a scheduled post's attached image in the Scheduled tab", async () => {
    scheduled = [
      {
        id: "a".repeat(16),
        text: "morning photo",
        reply_to: null,
        to: [],
        ttl: 3,
        publish_at: Math.floor(Date.now() / 1000) + 300,
        created_at: Math.floor(Date.now() / 1000),
        post_hash: null,
        attempts: 0,
        next_attempt_at: null,
        paused_at: null,
        image: {
          sha256: "b".repeat(64),
          size: 72,
          mime: "image/png",
          name: "photo.png",
        },
      },
    ];
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.click(screen.getByRole("button", { name: "Scheduled" }));
    expect(await screen.findByText("morning photo")).toBeTruthy();
    expect(screen.getByText(/image: photo\.png/)).toBeTruthy();
  });
});
