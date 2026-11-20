// SocialSection.test.tsx — feed renders, tabs switch endpoints, like
// posts, thread opens, composer validates. Fetch is stubbed; the section
// runs inside a TauContext.Provider with a fake node.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { TauContext } from "@/components/chrome/TauContext";
import type { ScheduledTanPost, TanDraft, TanPost } from "@/lib/types";
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
let scheduled: ScheduledTanPost[] = [];
let drafts: TanDraft[] = [];

function makeDraft(id: number, text: string, extra: Partial<TanDraft> = {}): TanDraft {
  return {
    id,
    text,
    reply_to: "",
    expires_in: null,
    post_hash: "",
    pending_peers: [],
    claimed_at: null,
    created_at: Math.floor(Date.now() / 1000) - 60,
    ...extra,
  };
}

let lists: { name: string; created_at: number; members: number }[] = [];
let listMembers: { author: string; name: string; added_at: number; posts?: number }[] = [];
let following: { author: string; name: string; posts: number }[] = [];

function makeScheduled(
  id: string,
  extra: Partial<ScheduledTanPost> = {},
): ScheduledTanPost {
  return {
    id,
    text: "hello later",
    reply_to: null,
    to: [],
    ttl: 3,
    publish_at: Math.floor(Date.now() / 1000) + 3600,
    created_at: Math.floor(Date.now() / 1000),
    post_hash: null,
    attempts: 0,
    next_attempt_at: null,
    paused_at: null,
    ...extra,
  };
}

beforeEach(() => {
  timeline = [makePost("h1".padEnd(64, "0"), "hello #mesh @bob")];
  scheduled = [];
  drafts = [
    makeDraft(1, "half-written thought"),
    makeDraft(2, "agreed", { reply_to: "h1".padEnd(64, "0"), expires_in: 3600 }),
  ];
  lists = [];
  listMembers = [];
  following = [];
  const router = mockFetch((url, method) => {
    if (url.includes("/api/tan/timeline")) return { posts: timeline, count: timeline.length };
    if (url.includes("/api/tan/lists/members"))
      return { name: "Friends", members: listMembers, count: listMembers.length };
    if (url.includes("/api/tan/lists")) return { lists, count: lists.length };
    if (url.includes("/api/tan/search")) return { posts: timeline, count: timeline.length };
    if (url.includes("/api/tan/thread")) return { posts: timeline, count: timeline.length };
    if (url.includes("/api/tan/tags")) return { tags: [{ tag: "mesh", count: 5 }], count: 1 };
    if (url.includes("/api/tan/following")) return { following, count: following.length };
    if (url.includes("/api/tan/followers"))
      return {
        followers: [
          {
            author: "b".repeat(64),
            name: "bob",
            posts: 2,
            followed: false,
            last_post: 1000,
          },
        ],
        count: 1,
      };
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
    if (url.includes("/api/tan/drafts")) {
      if (method === "POST" && url.includes("/delete")) {
        return { ok: true, draft: drafts[0] ?? {} };
      }
      if (method === "POST") {
        return { ok: true, draft: makeDraft(99, "saved draft") };
      }
      return { drafts, count: drafts.length };
    }
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

  it("renders a located post with its place and maps link", async () => {
    timeline = [
      makePost("h2".padEnd(64, "0"), "morning run", {
        location: { lat: 34.05, lon: -83.88, name: "Rock Springs Park" },
      }),
    ];
    renderSocial();
    await screen.findByText(/morning run/);
    const link = screen.getByRole("link", { name: /Rock Springs Park/ });
    expect(link.getAttribute("href")).toContain("openstreetmap.org");
    expect(link.getAttribute("href")).toContain("mlat=34.05");
    expect(link.getAttribute("href")).toContain("mlon=-83.88");
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

  it("shows a list picker and filters the timeline by the chosen list", async () => {
    lists = [{ name: "Friends", created_at: 1000, members: 2 }];
    renderSocial();
    const select = await screen.findByLabelText("Filter feed by list");
    fireEvent.change(select, { target: { value: "Friends" } });
    await waitFor(() =>
      expect(calls).toContain("GET /api/tan/timeline?limit=30&list=Friends"),
    );
    expect((select as HTMLSelectElement).value).toBe("Friends");
  });

  it("manages Tan lists from the Lists tab: create, members, add, remove, delete", async () => {
    lists = [{ name: "Friends", created_at: 1000, members: 1 }];
    listMembers = [{ author: "c".repeat(64), name: "carol", added_at: 1000, posts: 2 }];
    following = [{ author: "d".repeat(64), name: "dave", posts: 1 }];
    renderSocial();
    await screen.findByText(/hello/);

    // The header hint is a Manage button, not a CLI pointer.
    fireEvent.click(screen.getByRole("button", { name: "Manage lists" }));
    expect(await screen.findByText("Tan lists")).toBeTruthy();

    // Create posts to /api/tan/lists.
    fireEvent.change(screen.getByLabelText("New list name"), {
      target: { value: "Mesh crew" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Create list" }));
    await waitFor(() => expect(calls).toContain("POST /api/tan/lists"));

    // Expanding a list loads its members.
    fireEvent.click(screen.getByRole("button", { name: "Members" }));
    await waitFor(() =>
      expect(calls).toContain("GET /api/tan/lists/members?name=Friends"),
    );
    expect(await screen.findByText("carol")).toBeTruthy();

    // Adding by name posts to /api/tan/lists/add.
    fireEvent.change(screen.getByLabelText("Author name or hash to add to Friends"), {
      target: { value: "dave" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Add member" }));
    await waitFor(() => expect(calls).toContain("POST /api/tan/lists/add"));

    // Removing a member posts to /api/tan/lists/remove.
    fireEvent.click(screen.getByRole("button", { name: "Remove carol from Friends" }));
    await waitFor(() => expect(calls).toContain("POST /api/tan/lists/remove"));

    // Deleting needs a confirm step, then posts to /api/tan/lists/delete.
    fireEvent.click(screen.getByRole("button", { name: "Delete" }));
    expect(calls).not.toContain("POST /api/tan/lists/delete");
    fireEvent.click(screen.getByRole("button", { name: "Confirm delete" }));
    await waitFor(() => expect(calls).toContain("POST /api/tan/lists/delete"));
  });

  it("the Lists tab offers followed authors in the add-member picker", async () => {
    lists = [{ name: "Friends", created_at: 1000, members: 0 }];
    following = [{ author: "d".repeat(64), name: "dave", posts: 1 }];
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.click(screen.getByRole("button", { name: "Lists" }));
    fireEvent.click(screen.getByRole("button", { name: "Members" }));
    const select = await screen.findByLabelText("Add a followed author to Friends");
    expect(select.textContent).toContain("dave");
    fireEvent.change(select, { target: { value: "d".repeat(64) } });
    fireEvent.click(screen.getByRole("button", { name: "Add member" }));
    await waitFor(() => expect(calls).toContain("POST /api/tan/lists/add"));
  });

  it("shows followers with a follow-back button", async () => {
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.click(screen.getByRole("button", { name: "Followers" }));
    await waitFor(() => expect(calls).toContain("GET /api/tan/followers"));
    expect(await screen.findByText("bob")).toBeTruthy();
    expect(screen.getByRole("button", { name: "Follow" })).toBeTruthy();
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

  it("edits a scheduled post's text from the Scheduled tab", async () => {
    scheduled = [
      {
        id: "ab12cd34ef56ab12",
        text: "hello later",
        reply_to: null,
        to: [],
        ttl: 3,
        publish_at: Math.floor(Date.now() / 1000) + 3600,
        created_at: Math.floor(Date.now() / 1000),
        post_hash: null,
        attempts: 0,
        next_attempt_at: null,
        paused_at: null,
      },
    ];
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.click(screen.getByRole("button", { name: "Scheduled" }));
    expect(await screen.findByText("hello later")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Edit" }));
    fireEvent.change(screen.getByLabelText("Edit scheduled post"), {
      target: { value: "hello sooner" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() =>
      expect(calls).toContain("POST /api/tan/edit-scheduled"),
    );
  });

  it("saving an unchanged scheduled post makes no request", async () => {
    scheduled = [
      {
        id: "ab12cd34ef56ab12",
        text: "hello later",
        reply_to: null,
        to: [],
        ttl: 3,
        publish_at: Math.floor(Date.now() / 1000) + 3600,
        created_at: Math.floor(Date.now() / 1000),
        post_hash: null,
        attempts: 0,
        next_attempt_at: null,
        paused_at: null,
      },
    ];
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.click(screen.getByRole("button", { name: "Scheduled" }));
    expect(await screen.findByText("hello later")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Edit" }));
    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() =>
      expect(screen.queryByLabelText("Edit scheduled post")).toBeNull(),
    );
    expect(calls).not.toContain("POST /api/tan/edit-scheduled");
  });

  it("shows a scheduled poll with its options in the Scheduled tab", async () => {
    scheduled = [
      {
        id: "a".repeat(16),
        text: "pizza or tacos?",
        reply_to: null,
        to: [],
        ttl: 3,
        publish_at: Math.floor(Date.now() / 1000) + 3600,
        created_at: Math.floor(Date.now() / 1000),
        post_hash: null,
        attempts: 0,
        next_attempt_at: null,
        paused_at: null,
        kind: "poll",
        options: ["Pizza", "Tacos"],
      },
    ];
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.click(screen.getByRole("button", { name: "Scheduled" }));
    expect(await screen.findByText("pizza or tacos?")).toBeTruthy();
    expect(screen.getByText("Poll · 2 options")).toBeTruthy();
    expect(screen.getByText("1. Pizza")).toBeTruthy();
    expect(screen.getByText("2. Tacos")).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "Cancel scheduled poll" }),
    ).toBeTruthy();
  });

  it("shows scheduled reposts and quotes with their target", async () => {
    scheduled = [
      makeScheduled("aa".padEnd(16, "0"), {
        kind: "repost",
        text: "",
        target_post: "d".repeat(64),
      }),
      makeScheduled("bb".padEnd(16, "0"), {
        kind: "quote",
        text: "my take",
        target_post: "e".repeat(64),
      }),
    ];
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.click(screen.getByRole("button", { name: "Scheduled" }));
    expect(await screen.findByText(/Repost ·/)).toBeTruthy();
    expect(screen.getByText(/Quote ·/)).toBeTruthy();
    expect(screen.getByText("my take")).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "Cancel scheduled repost" }),
    ).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "Cancel scheduled quote" }),
    ).toBeTruthy();
  });

  it("scheduling a repost from the post menu posts to /api/tan/schedule", async () => {
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.click(screen.getByRole("button", { name: "Post options" }));
    fireEvent.click(screen.getByRole("button", { name: /Schedule repost/ }));
    fireEvent.click(screen.getByRole("button", { name: "In 1 hour" }));
    await waitFor(() => expect(calls).toContain("POST /api/tan/schedule"));
  });

  it("scheduling a quote from the quote composer posts to /api/tan/schedule", async () => {
    timeline = [makePost("h1".padEnd(64, "0"), "hello #mesh @bob", { quotes: 0 })];
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.click(
      screen.getByRole("button", { name: /Show quotes, 0 quotes/ }),
    );
    fireEvent.change(screen.getByLabelText("Quote text"), {
      target: { value: "my take" },
    });
    fireEvent.change(screen.getByLabelText("Send quote later"), {
      target: { value: "3600" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Schedule quote" }));
    await waitFor(() => expect(calls).toContain("POST /api/tan/schedule"));
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

  it("Drafts tab lists saved drafts with reply and expiry", async () => {
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.click(screen.getByRole("button", { name: "Drafts" }));
    expect(await screen.findByText("half-written thought")).toBeTruthy();
    expect(screen.getByText("agreed")).toBeTruthy();
    expect(screen.getByText(/In reply to h1/)).toBeTruthy();
    expect(screen.getByText(/Disappears 1h after posting/)).toBeTruthy();
  });

  it("Save draft parks the composer's text", async () => {
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.change(screen.getByLabelText("Compose a post"), {
      target: { value: "park this for later" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Save draft" }));
    await waitFor(() => expect(calls).toContain("POST /api/tan/drafts"));
    await waitFor(() =>
      expect((screen.getByLabelText("Compose a post") as HTMLTextAreaElement).value).toBe(""),
    );
  });

  it("editing a draft loads it, and posting it deletes the draft", async () => {
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.click(screen.getByRole("button", { name: "Drafts" }));
    await screen.findByText("half-written thought");
    const rows = screen.getAllByRole("button", { name: "Edit" });
    fireEvent.click(rows[0]);
    await waitFor(() =>
      expect((screen.getByLabelText("Compose a post") as HTMLTextAreaElement).value).toBe(
        "half-written thought",
      ),
    );
    fireEvent.click(screen.getByRole("button", { name: "Post" }));
    await waitFor(() => expect(calls).toContain("POST /api/tan/posts"));
    await waitFor(() => expect(calls).toContain("POST /api/tan/drafts/delete"));
  });

  it("deleting a draft discards it without posting", async () => {
    renderSocial();
    await screen.findByText(/hello/);
    fireEvent.click(screen.getByRole("button", { name: "Drafts" }));
    await screen.findByText("half-written thought");
    const deletes = screen.getAllByRole("button", { name: "Delete draft" });
    fireEvent.click(deletes[0]);
    await waitFor(() => expect(calls).toContain("POST /api/tan/drafts/delete"));
    expect(calls).not.toContain("POST /api/tan/posts");
  });
});
