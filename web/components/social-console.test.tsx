import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { TanPost } from "@/lib/types";

import { PostCard } from "./social-console";

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
        onEdit={async () => true} deleting={false}
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
        onEdit={async () => true} deleting={false}
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
        onEdit={async () => true} deleting={false}
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
        onEdit={async () => true}
        deleting
      />,
    );

    expect(screen.getByText("deleting…")).toBeTruthy();
    expect(screen.queryByTitle("Delete this post")).toBeNull();
    fireEvent.click(screen.getByText("deleting…"));
    expect(onDelete).not.toHaveBeenCalled();
  });
});

describe("PostCard edit control", () => {
  function renderCard(post: TanPost, onEdit = vi.fn().mockResolvedValue(true)) {
    render(
      <PostCard
        post={post}
        onToggleLike={() => {}}
        onReply={() => {}}
        onDelete={vi.fn()}
        onEdit={onEdit}
        deleting={false}
      />,
    );
    return onEdit;
  }

  it("opens a form prefilled with the post text and saves the edit", async () => {
    const post = makePost();
    const onEdit = renderCard(post);

    fireEvent.click(screen.getByTitle("Edit this post"));
    const field = screen.getByLabelText("Edit post text");
    expect((field as HTMLTextAreaElement).value).toBe("regret this");

    fireEvent.change(field, { target: { value: "meant this" } });
    await act(async () => {
      fireEvent.click(screen.getByText("save"));
    });
    expect(onEdit).toHaveBeenCalledWith(post, "meant this");
    expect(screen.queryByLabelText("Edit post text")).toBeNull();
  });

  it("keeps the form open when the save fails", async () => {
    const onEdit = renderCard(makePost(), vi.fn().mockResolvedValue(false));

    fireEvent.click(screen.getByTitle("Edit this post"));
    fireEvent.change(screen.getByLabelText("Edit post text"), {
      target: { value: "new text" },
    });
    await act(async () => {
      fireEvent.click(screen.getByText("save"));
    });
    expect(onEdit).toHaveBeenCalledTimes(1);
    expect(screen.getByLabelText("Edit post text")).toBeTruthy();
  });

  it("cancels without calling onEdit and disables save for empty text", () => {
    const onEdit = renderCard(makePost());

    fireEvent.click(screen.getByTitle("Edit this post"));
    const field = screen.getByLabelText("Edit post text");
    fireEvent.change(field, { target: { value: "   " } });
    expect((screen.getByText("save") as HTMLButtonElement).disabled).toBe(true);
    fireEvent.click(screen.getByText("cancel"));
    expect(onEdit).not.toHaveBeenCalled();
    expect(screen.queryByLabelText("Edit post text")).toBeNull();
  });

  it("disables save while the text is unchanged", () => {
    renderCard(makePost());

    fireEvent.click(screen.getByTitle("Edit this post"));
    expect((screen.getByText("save") as HTMLButtonElement).disabled).toBe(true);
  });

  it("hides the edit control on other authors' posts", () => {
    renderCard(makePost({ own: false }));

    expect(screen.queryByTitle("Edit this post")).toBeNull();
  });

  it("preserves emoji-heavy posts instead of truncating by UTF-16 units", () => {
    // The backend counts code points, not UTF-16 units: 1,200 emoji are
    // 1,200 code points but 2,400 UTF-16 units, so a plain slice(0, 2000)
    // would silently drop 199 emoji from a valid post on first edit.
    renderCard(makePost({ text: "😀".repeat(1200) }));
    fireEvent.click(screen.getByTitle("Edit this post"));
    const field = screen.getByLabelText(
      "Edit post text",
    ) as HTMLTextAreaElement;
    expect(field.value).toBe("😀".repeat(1200));

    fireEvent.change(field, { target: { value: "😀".repeat(2001) } });
    expect(field.value).toBe("😀".repeat(2000));
  });

  it("hides the edit control on your own quotes", () => {
    // Quotes are not editable: create_edit only permits posts and replies,
    // so the console must not offer the button for them.
    renderCard(makePost({ kind: "quote", own: true }));

    expect(screen.queryByTitle("Edit this post")).toBeNull();
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
        onEdit={async () => true} deleting={false}
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
        onEdit={async () => true} deleting={false}
      />,
    );
    expect(screen.getByText("❝ 2")).toBeTruthy();

    rerender(
      <PostCard
        post={makePost({ quotes: 0 })}
        onToggleLike={() => {}}
        onReply={() => {}}
        onDelete={vi.fn()}
        onEdit={async () => true} deleting={false}
      />,
    );
    expect(screen.queryByText(/❝/)).toBeNull();
  });
});
