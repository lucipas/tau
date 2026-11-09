import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { TanExportDocument, TanPost } from "@/lib/types";

import { PostCard, TanBackupSection } from "./social-console";

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

describe("TanBackupSection", () => {
  const doc: TanExportDocument = {
    format: "tau-tan-export",
    version: 1,
    exported_at: 1000,
    authors: ["a".repeat(64)],
    entries: [{ hash: "c".repeat(64), text: "hello" }],
  };

  function renderSection(overrides: {
    exporting?: boolean;
    importing?: boolean;
    onExport?: (allAuthors: boolean) => Promise<TanExportDocument>;
    onImport?: (doc: TanExportDocument) => Promise<{
      imported: number;
      present: number;
      skipped: number;
      errors: Array<{ hash: string; error: string }>;
    }>;
  } = {}) {
    const onExport = overrides.onExport ?? vi.fn(async () => doc);
    const onImport =
      overrides.onImport ??
      vi.fn(async () => ({
        imported: 2,
        present: 1,
        skipped: 0,
        errors: [],
      }));
    render(
      <TanBackupSection
        exporting={overrides.exporting ?? false}
        importing={overrides.importing ?? false}
        onExport={onExport}
        onImport={onImport}
      />,
    );
    return { onExport, onImport };
  }

  function pickFile(contents: string) {
    const file = new File([contents], "backup.json", {
      type: "application/json",
    });
    const input = document.querySelector(
      'input[type="file"]',
    ) as HTMLInputElement;
    fireEvent.change(input, { target: { files: [file] } });
  }

  it("exports the node's own chain and reports the entry count", async () => {
    const { onExport } = renderSection();

    fireEvent.click(screen.getByText("export mine"));
    await waitFor(() => expect(onExport).toHaveBeenCalledWith(false));
    expect(
      await screen.findByText("exported 1 entry (your chain)"),
    ).toBeTruthy();
  });

  it("exports all cached authors when asked", async () => {
    const { onExport } = renderSection();

    fireEvent.click(screen.getByText("export all"));
    await waitFor(() => expect(onExport).toHaveBeenCalledWith(true));
    expect(
      await screen.findByText("exported 1 entry (all authors)"),
    ).toBeTruthy();
  });

  it("disables the export buttons while an export is running", () => {
    renderSection({ exporting: true });

    expect(screen.getByText("exporting…")).toBeTruthy();
    expect(
      (screen.getByText("exporting…") as HTMLButtonElement).disabled,
    ).toBe(true);
  });

  it("imports a picked export file and reports the counts", async () => {
    const { onImport } = renderSection();

    pickFile(JSON.stringify(doc));
    await waitFor(() => expect(onImport).toHaveBeenCalledWith(doc));
    expect(
      await screen.findByText("imported 2 · 1 already present · 0 skipped"),
    ).toBeTruthy();
  });

  it("lists per-entry errors from the import", async () => {
    const onImport = vi.fn(async () => ({
      imported: 0,
      present: 1,
      skipped: 1,
      errors: [{ hash: "d".repeat(64), error: "bad signature" }],
    }));
    renderSection({ onImport });

    pickFile(JSON.stringify(doc));
    await waitFor(() => expect(onImport).toHaveBeenCalled());
    const line = await screen.findByText(/bad signature/);
    expect(line.textContent).toContain("skipped");
  });

  it("rejects a file that is not a valid export document", async () => {
    const { onImport } = renderSection();

    pickFile("not json");
    expect(
      await screen.findByText("that file is not a valid export document"),
    ).toBeTruthy();
    expect(onImport).not.toHaveBeenCalled();
  });
});
