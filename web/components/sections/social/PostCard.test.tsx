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

describe("PostCard quote composer with an image", () => {
  function openQuoteComposer() {
    fireEvent.click(
      screen.getByRole("button", { name: "Show quotes, 0 quotes" }),
    );
  }

  function pickImage(name = "pic.png", type = "image/png", bytes = "hello") {
    const input = screen.getByLabelText(
      "Attach an image to the quote",
    ) as HTMLInputElement;
    const file = new File([bytes], name, { type });
    fireEvent.change(input, { target: { files: [file] } });
    return file;
  }

  it("posts a picked image with the quote as a data URL", async () => {
    const actions = makeActions();
    render(<PostCard post={makePost()} actions={actions} />);
    openQuoteComposer();
    fireEvent.change(screen.getByLabelText("Quote text"), {
      target: { value: "with a picture" },
    });
    pickImage();
    expect(screen.getByText("pic.png")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Post quote" }));
    await waitFor(() =>
      expect(actions.quote).toHaveBeenCalledWith(POST_HASH, "with a picture", {
        image: { dataUrl: "data:image/png;base64,aGVsbG8=" },
      }),
    );
  });

  it("schedules a picked image with the quote", async () => {
    const actions = makeActions();
    render(<PostCard post={makePost()} actions={actions} />);
    openQuoteComposer();
    fireEvent.change(screen.getByLabelText("Quote text"), {
      target: { value: "later, with a picture" },
    });
    pickImage();
    fireEvent.change(screen.getByLabelText("Send quote later"), {
      target: { value: "3600" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Schedule quote" }));
    await waitFor(() =>
      expect(actions.scheduleQuote).toHaveBeenCalledWith(
        POST_HASH,
        "later, with a picture",
        3600,
        { image: { dataUrl: "data:image/png;base64,aGVsbG8=" } },
      ),
    );
  });

  it("refuses an image over the 2 MB limit", async () => {
    const actions = makeActions();
    render(<PostCard post={makePost()} actions={actions} />);
    openQuoteComposer();
    fireEvent.change(screen.getByLabelText("Quote text"), {
      target: { value: "too big" },
    });
    const input = screen.getByLabelText(
      "Attach an image to the quote",
    ) as HTMLInputElement;
    const big = new File([new Uint8Array(2 * 1024 * 1024 + 1)], "big.png", {
      type: "image/png",
    });
    fireEvent.change(input, { target: { files: [big] } });
    fireEvent.click(screen.getByRole("button", { name: "Post quote" }));
    expect((await screen.findByRole("alert")).textContent).toMatch(/2 MB limit/);
    expect(actions.quote).not.toHaveBeenCalled();
  });
});

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

  it("disables Share when the picked channel leaves the joined list", async () => {
    const actions = makeActions();
    const { rerender } = render(
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
    expect(
      screen.getByRole("button", { name: "Share" }) as HTMLButtonElement,
    ).toHaveProperty("disabled", false);
    // The joined list refreshes on a poll; the channel is gone now.
    rerender(
      <PostCard
        post={makePost()}
        actions={actions}
        peers={[{ destination: "c".repeat(64), name: "bob" }]}
        channels={[]}
      />,
    );
    expect(
      screen.getByRole("button", { name: "Share" }) as HTMLButtonElement,
    ).toHaveProperty("disabled", true);
    expect(actions.share).not.toHaveBeenCalled();
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
