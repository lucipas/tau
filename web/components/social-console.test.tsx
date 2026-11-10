import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { TanPost, TanVerifyReport } from "@/lib/types";

import { PostCard, VerifyResult } from "./social-console";

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

describe("VerifyResult", () => {
  function makeReport(overrides: Partial<TanVerifyReport> = {}): TanVerifyReport {
    return {
      entries: 12,
      authors: 3,
      unlinked: 0,
      problems: [],
      ok: true,
      ...overrides,
    };
  }

  it("reports a healthy store with its counts", () => {
    render(<VerifyResult report={makeReport()} />);
    expect(
      screen.getByText("store OK — 12 entries from 3 authors"),
    ).toBeTruthy();
  });

  it("notes unlinked entries as partial sync, not damage", () => {
    render(<VerifyResult report={makeReport({ unlinked: 2 })} />);
    expect(screen.getByText(/2 unlinked \(partial sync\)/)).toBeTruthy();
  });

  it("lists each problem with its post hash and message", () => {
    render(
      <VerifyResult
        report={makeReport({
          entries: 2,
          authors: 1,
          ok: false,
          problems: [
            {
              hash: "c".repeat(64),
              author: "a".repeat(64),
              problem: "post hash does not match its contents",
            },
          ],
        })}
      />,
    );
    expect(
      screen.getByText("store NOT OK — 1 problem in 2 entries"),
    ).toBeTruthy();
    expect(
      screen.getByText("post hash does not match its contents"),
    ).toBeTruthy();
  });

  it("renders two faults on one post as separate rows", () => {
    render(
      <VerifyResult
        report={makeReport({
          entries: 2,
          authors: 1,
          ok: false,
          problems: [
            {
              hash: "c".repeat(64),
              author: "a".repeat(64),
              problem: "post hash does not match its contents",
            },
            {
              hash: "c".repeat(64),
              author: "a".repeat(64),
              problem: "sequence jumps from 1 to 3",
            },
          ],
        })}
      />,
    );
    expect(
      screen.getByText("store NOT OK — 2 problems in 2 entries"),
    ).toBeTruthy();
    expect(
      screen.getByText("post hash does not match its contents"),
    ).toBeTruthy();
    expect(screen.getByText("sequence jumps from 1 to 3")).toBeTruthy();
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
