// PostCard.test.tsx — the share picker shares with a peer (existing
// behavior) or into a joined channel (the --channel share), chosen by
// the Peer / Channel toggle. The actions prop is a fake, so no fetch
// stub is needed.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import PostCard, { type FeedActions } from "./PostCard";
import type { TanPost } from "@/lib/types";

const POST_HASH = "a".repeat(64);

function makePost(): TanPost {
  return {
    v: 1,
    hash: POST_HASH,
    author: "b".repeat(64),
    key: "b".repeat(64),
    name: "alice",
    text: "worth sharing",
    created_at: 1000,
    prev: "",
    sig: "",
    kind: "post",
    target: "",
    expires_at: null,
    likes: 0,
    liked: false,
    own: false,
    quotes: 0,
  };
}

function makeActions(): FeedActions {
  return {
    like: vi.fn().mockResolvedValue(undefined),
    unlike: vi.fn().mockResolvedValue(undefined),
    repost: vi.fn().mockResolvedValue(undefined),
    quote: vi.fn().mockResolvedValue(makePost()),
    scheduleRepost: vi.fn().mockResolvedValue(undefined),
    scheduleQuote: vi.fn().mockResolvedValue({}),
    vote: vi.fn().mockResolvedValue(undefined),
    share: vi.fn().mockResolvedValue(undefined),
    save: vi.fn().mockResolvedValue(undefined),
    unsave: vi.fn().mockResolvedValue(undefined),
    hide: vi.fn().mockResolvedValue(undefined),
    pin: vi.fn().mockResolvedValue(undefined),
    unpin: vi.fn().mockResolvedValue(undefined),
    deletePost: vi.fn().mockResolvedValue(undefined),
    editPost: vi.fn().mockResolvedValue(undefined),
    follow: vi.fn().mockResolvedValue(undefined),
    unfollow: vi.fn().mockResolvedValue(undefined),
    muteAuthor: vi.fn().mockResolvedValue(undefined),
  };
}

function openSharePicker() {
  fireEvent.click(screen.getByRole("button", { name: "Post options" }));
  fireEvent.click(screen.getByRole("button", { name: /Share/ }));
}

describe("PostCard share picker", () => {
  it("shares with a peer by default", async () => {
    const actions = makeActions();
    render(
      <PostCard
        post={makePost()}
        actions={actions}
        peers={[{ destination: "c".repeat(64), name: "bob" }]}
        channels={["general"]}
      />,
    );
    openSharePicker();
    expect(screen.getByText("Choose a peer…")).toBeTruthy();
    fireEvent.change(screen.getByLabelText("Share with"), {
      target: { value: "c".repeat(64) },
    });
    fireEvent.click(screen.getByRole("button", { name: "Share" }));
    await waitFor(() =>
      expect(actions.share).toHaveBeenCalledWith(POST_HASH, {
        to: "c".repeat(64),
      }),
    );
  });

  it("shares into a channel when the Channel toggle is picked", async () => {
    const actions = makeActions();
    render(
      <PostCard
        post={makePost()}
        actions={actions}
        peers={[{ destination: "c".repeat(64), name: "bob" }]}
        channels={["general", "random"]}
      />,
    );
    openSharePicker();
    fireEvent.click(screen.getByRole("button", { name: "Channel" }));
    expect(screen.getByText("Choose a channel…")).toBeTruthy();
    expect(screen.getByText("#general")).toBeTruthy();
    expect(screen.getByText("#random")).toBeTruthy();
    fireEvent.change(screen.getByLabelText("Share with"), {
      target: { value: "random" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Share" }));
    await waitFor(() =>
      expect(actions.share).toHaveBeenCalledWith(POST_HASH, {
        channel: "random",
      }),
    );
  });

  it("switching back to Peer clears the channel choice", async () => {
    const actions = makeActions();
    render(
      <PostCard
        post={makePost()}
        actions={actions}
        peers={[{ destination: "c".repeat(64), name: "bob" }]}
        channels={["general"]}
      />,
    );
    openSharePicker();
    fireEvent.click(screen.getByRole("button", { name: "Channel" }));
    fireEvent.change(screen.getByLabelText("Share with"), {
      target: { value: "general" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Peer" }));
    expect(screen.getByText("Choose a peer…")).toBeTruthy();
    expect(
      screen.getByRole("button", { name: "Share" }) as HTMLButtonElement,
    ).toHaveProperty("disabled", true);
  });
});
