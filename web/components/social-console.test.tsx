import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import { shortHash } from "@/lib/format";
import type { TanExportDocument, TanPost, TanVerifyReport } from "@/lib/types";

import {
  PostCard,
  SocialConsole,
  TanBackupSection,
  VerifyResult,
} from "./social-console";

const mockTan = vi.hoisted(() => ({
  current: {} as Record<string, unknown>,
}));

// The search and mentions tests render the whole console, so the data
// hook and the Next link are mocked; the PostCard tests above never
// touch either.
vi.mock("@/lib/use-tan", () => ({
  useTan: vi.fn(() => mockTan.current),
  TAN_POLL_INTERVAL_MS: 5000,
}));

import { useTan } from "@/lib/use-tan";

const mockedUseTan = useTan as Mock;
// The search tests use this name for the same mock.
const mockUseTan = mockedUseTan;
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
        onRepost={() => {}}
        onReply={() => {}}
        onQuote={() => {}}
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
        onRepost={() => {}}
        onReply={() => {}}
        onQuote={() => {}}
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
        onRepost={() => {}}
        onReply={() => {}}
        onQuote={() => {}}
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
        onRepost={() => {}}
        onReply={() => {}}
        onQuote={() => {}}
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

describe("PostCard edit control", () => {
  function renderCard(post: TanPost, onEdit = vi.fn().mockResolvedValue(true)) {
    render(
      <PostCard
        post={post}
        onToggleLike={() => {}}
        onRepost={() => {}}
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
        onRepost={() => {}}
        onReply={() => {}}
        onQuote={() => {}}
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

  it("reports a blocked original without resurfacing its name or text", () => {
    renderCard(
      makePost({
        kind: "quote",
        text: "worth quoting",
        target: "e".repeat(64),
        quoted: {
          hash: "e".repeat(64),
          author: "",
          name: "",
          text: "",
          deleted: false,
          blocked: true,
        },
      }),
    );

    expect(screen.getByText("worth quoting")).toBeTruthy();
    expect(
      screen.getByText(
        (_content, element) =>
          element?.textContent === "quoting a blocked author's post",
      ),
    ).toBeTruthy();
  });

  it("reports a muted original without resurfacing its text", () => {
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
          deleted: false,
          muted: true,
        },
      }),
    );

    expect(screen.getByText("still worth it")).toBeTruthy();
    expect(
      screen.getByText(
        (_content, element) =>
          element?.textContent === "quoting alice · a muted post",
      ),
    ).toBeTruthy();
  });

  it("shows the quote count on the quoted post and hides it at zero", () => {
    const { rerender } = render(
      <PostCard
        post={makePost({ quotes: 2 })}
        onToggleLike={() => {}}
        onRepost={() => {}}
        onReply={() => {}}
        onQuote={() => {}}
        onDelete={vi.fn()}
        onEdit={async () => true} deleting={false}
      />,
    );
    expect(screen.getByText("❝ 2")).toBeTruthy();

    rerender(
      <PostCard
        post={makePost({ quotes: 0 })}
        onToggleLike={() => {}}
        onRepost={() => {}}
        onReply={() => {}}
        onQuote={() => {}}
        onDelete={vi.fn()}
        onEdit={async () => true} deleting={false}
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

  it("lets the same invalid file be picked again after a parse failure", async () => {
    const { onImport } = renderSection();
    const input = document.querySelector(
      'input[type="file"]',
    ) as HTMLInputElement;

    pickFile("not json");
    await screen.findByText("that file is not a valid export document");
    // The input is cleared even though parsing failed, so choosing
    // the same file again fires onChange instead of doing nothing.
    expect(input.value).toBe("");

    pickFile("not json");
    expect(
      await screen.findByText("that file is not a valid export document"),
    ).toBeTruthy();
    expect(onImport).not.toHaveBeenCalled();
  });
});

describe("PostCard mute control", () => {
  function renderCard(post: TanPost, onMute = vi.fn()) {
    render(
      <PostCard
        post={post}
        onToggleLike={() => {}}
        onRepost={() => {}}
        onReply={() => {}}
        onDelete={vi.fn()}
        deleting={false}
        onMute={onMute}
      />,
    );
    return onMute;
  }

  it("shows a mute button on other authors' posts and calls onMute", () => {
    const post = makePost({ own: false });
    const onMute = renderCard(post);

    fireEvent.click(screen.getByTitle("Mute this author"));
    expect(onMute).toHaveBeenCalledTimes(1);
    expect(onMute).toHaveBeenCalledWith(post);
  });

  it("hides the mute control on your own posts", () => {
    renderCard(makePost({ own: true }));

    expect(screen.queryByTitle("Mute this author")).toBeNull();
  });

  it("shows a disabled state while a mute is in flight", () => {
    const post = makePost({ own: false });
    const onMute = vi.fn();
    render(
      <PostCard
        post={post}
        onToggleLike={() => {}}
        onRepost={() => {}}
        onReply={() => {}}
        onDelete={vi.fn()}
        deleting={false}
        onMute={onMute}
        muting
      />,
    );

    expect(screen.getByText("muting…")).toBeTruthy();
    expect(screen.queryByTitle("Mute this author")).toBeNull();
    fireEvent.click(screen.getByText("muting…"));
    expect(onMute).not.toHaveBeenCalled();
  });
});

describe("SocialConsole muted sidebar", () => {
  const authorHash = "a".repeat(64);

  function setTan(overrides: Record<string, unknown>) {
    mockTan.current = {
      posts: [],
      searchResults: [],
      searchQuery: null,
      mentions: [],
      following: [],
      authors: [],
      authorsTotal: 0,
      loadMoreAuthors: vi.fn(),
      suggestions: [],
      tags: [],
      tagFilter: null,
      setTagFilter: vi.fn(),
      mutedAuthors: [],
      mutedKeywords: [],
      saved: [],
      savedLoading: false,
      savedHasMore: false,
      loadSaved: vi.fn(),
      loadMoreSaved: vi.fn(),
      save: vi.fn(),
      unsave: vi.fn(),
      hidden: [],
      hiddenLoading: false,
      hiddenHasMore: false,
      loadHidden: vi.fn(),
      loadMoreHidden: vi.fn(),
      hide: vi.fn(),
      unhide: vi.fn(),
      all: false,
      setAll: vi.fn(),
      view: "timeline",
      setView: vi.fn(),
      showMentions: false,
      setShowMentions: vi.fn(),
      loading: false,
      posting: false,
      syncing: false,
      error: null,
      threadRef: null,
      threadPosts: null,
      threadLoading: false,
      threadError: null,
      openThread: vi.fn(),
      closeThread: vi.fn(),
      refresh: vi.fn(),
      search: vi.fn(),
      clearSearch: vi.fn(),
      publish: vi.fn(),
      follow: vi.fn(),
      unfollow: vi.fn(),
      muteAuthor: vi.fn(),
      unmuteAuthor: vi.fn(),
      muteKeyword: vi.fn(),
      unmuteKeyword: vi.fn(),
      like: vi.fn(),
      unlike: vi.fn(),
      deletePost: vi.fn(),
      sync: vi.fn(),
      ...overrides,
    };
    return mockTan.current;
  }

  it("shows the empty state when nothing is muted", () => {
    setTan({});
    render(<SocialConsole />);

    expect(screen.getByText(/Nothing muted\./)).toBeTruthy();
  });

  it("lists muted authors and keywords and unmutes each by its key", () => {
    const tan = setTan({
      mutedAuthors: [
        { author: authorHash, name: "bob", muted_at: 1, posts: 3, seq: 3, last_post: 1 },
      ],
      mutedKeywords: [
        { keyword: "crypto", display: "Crypto", added_at: 1 },
      ],
    });
    render(<SocialConsole />);

    expect(screen.queryByText(/Nothing muted\./)).toBeNull();
    expect(screen.getByText("bob")).toBeTruthy();
    expect(screen.getByText(`${shortHash(authorHash, 10)} · 3 posts`)).toBeTruthy();
    expect(screen.getByText("“Crypto”")).toBeTruthy();

    fireEvent.click(screen.getByTitle("unmute this author"));
    expect(tan.unmuteAuthor).toHaveBeenCalledWith(authorHash);

    fireEvent.click(screen.getByTitle("unmute this keyword"));
    expect(tan.unmuteKeyword).toHaveBeenCalledWith("Crypto");
  });

  it("submits a typed keyword through muteKeyword", () => {
    const tan = setTan({});
    render(<SocialConsole />);

    fireEvent.change(screen.getByPlaceholderText("mute a word or phrase"), {
      target: { value: "bitcoin" },
    });
    fireEvent.click(screen.getByRole("button", { name: "mute" }));
    expect(tan.muteKeyword).toHaveBeenCalledWith("bitcoin");
  });

  it("keeps text typed while a keyword mute is in flight", async () => {
    let resolveMute!: (value: unknown) => void;
    const pending = new Promise((resolve) => {
      resolveMute = resolve;
    });
    const tan = setTan({ muteKeyword: vi.fn(() => pending) });
    render(<SocialConsole />);

    const input = screen.getByPlaceholderText("mute a word or phrase");
    fireEvent.change(input, { target: { value: "bitcoin" } });
    fireEvent.click(screen.getByRole("button", { name: "mute" }));
    expect(tan.muteKeyword).toHaveBeenCalledWith("bitcoin");

    // Typing continues while the request is in flight; the clear after
    // success must only drop the text that was actually muted.
    fireEvent.change(input, { target: { value: "bitcoin ethereum" } });
    await act(async () => {
      resolveMute({ ok: true });
    });

    expect(
      (screen.getByPlaceholderText("mute a word or phrase") as HTMLInputElement)
        .value,
    ).toBe("bitcoin ethereum");
  });
});


describe("SocialConsole search", () => {
  const mockSearch = vi.fn();
  const mockClearSearch = vi.fn();

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
      tags: [],
      tagFilter: null,
      setTagFilter: vi.fn(),
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
      error: null,
      refresh: vi.fn(),
      search: mockSearch,
      clearSearch: mockClearSearch,
      publish: vi.fn(),
      follow: vi.fn(),
      unfollow: vi.fn(),
      muteAuthor: vi.fn(),
      unmuteAuthor: vi.fn(),
      muteKeyword: vi.fn(),
      unmuteKeyword: vi.fn(),
      like: vi.fn(),
      unlike: vi.fn(),
      repost: vi.fn(),
      deletePost: vi.fn(),
      sync: vi.fn(),
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
      hidden: [],
      hiddenLoading: false,
      hiddenHasMore: false,
      loadHidden: vi.fn(),
      loadMoreHidden: vi.fn(),
      hide: vi.fn(),
      unhide: vi.fn(),
      ...overrides,
    };
  }

  beforeEach(() => {
    vi.resetAllMocks();
  });

  it("submits the query to the search hook", () => {
    const timelinePost = makePost({ text: "timeline post" });
    mockUseTan.mockReturnValue(makeTan({ posts: [timelinePost] }));
    render(<SocialConsole />);

    fireEvent.change(screen.getByLabelText("Search stored posts"), {
      target: { value: "mesh" },
    });
    fireEvent.click(screen.getByRole("button", { name: "search" }));

    expect(mockSearch).toHaveBeenCalledTimes(1);
    expect(mockSearch).toHaveBeenCalledWith("mesh");
  });

  it("shows search results instead of the timeline while searching", () => {
    const timelinePost = makePost({ text: "timeline post" });
    const hit = makePost({ hash: "e".repeat(64), text: "mesh post" });
    mockUseTan.mockReturnValue(
      makeTan({
        posts: [timelinePost],
        searchResults: [hit],
        searchQuery: "mesh",
      }),
    );
    render(<SocialConsole />);

    expect(screen.getByText("mesh post")).toBeTruthy();
    expect(screen.queryByText("timeline post")).toBeNull();
    expect(screen.getByText("search results")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "clear search" }));
    expect(mockClearSearch).toHaveBeenCalledTimes(1);
  });

  it("says when nothing matches the search", () => {
    mockUseTan.mockReturnValue(
      makeTan({ posts: [makePost()], searchResults: [], searchQuery: "zzz" }),
    );
    render(<SocialConsole />);

    expect(screen.getByText("No stored posts match “zzz”")).toBeTruthy();
    expect(screen.queryByText("No posts yet")).toBeNull();
  });
});


describe("SocialConsole mentions view", () => {
  function hookState(overrides: Record<string, unknown> = {}) {
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
      tags: [],
      tagFilter: null,
      setTagFilter: vi.fn(),
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
      error: null,
      refresh: vi.fn(),
      search: vi.fn(),
      clearSearch: vi.fn(),
      publish: vi.fn(),
      follow: vi.fn(),
      unfollow: vi.fn(),
      muteAuthor: vi.fn(),
      unmuteAuthor: vi.fn(),
      muteKeyword: vi.fn(),
      unmuteKeyword: vi.fn(),
      like: vi.fn(),
      unlike: vi.fn(),
      repost: vi.fn(),
      deletePost: vi.fn(),
      sync: vi.fn(),
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
      hidden: [],
      hiddenLoading: false,
      hiddenHasMore: false,
      loadHidden: vi.fn(),
      loadMoreHidden: vi.fn(),
      hide: vi.fn(),
      unhide: vi.fn(),
      ...overrides,
    };
  }

  it("switches the feed to mentions and back", () => {
    const timelinePost = makePost({ text: "timeline post" });
    const mention = makePost({
      hash: "e".repeat(64),
      text: "seconded",
      author: "f".repeat(64),
      name: "bob",
      own: false,
    });
    const state = hookState({ posts: [timelinePost], mentions: [mention] });
    mockedUseTan.mockReturnValue(state);

    const { rerender } = render(<SocialConsole />);
    expect(screen.getByText("timeline post")).toBeTruthy();
    expect(screen.queryByText("seconded")).toBeNull();

    fireEvent.click(screen.getByText("mentions"));
    expect(state.setShowMentions).toHaveBeenCalledWith(true);

    mockedUseTan.mockReturnValue({ ...state, showMentions: true });
    rerender(<SocialConsole />);
    expect(screen.queryByText("timeline post")).toBeNull();
    expect(screen.getByText("seconded")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "following" }));
    expect(state.setShowMentions).toHaveBeenCalledWith(false);
    expect(state.setView).toHaveBeenCalledWith("timeline");
    expect(state.setAll).toHaveBeenCalledWith(false);
  });

  it("says there are no replies yet when the mentions list is empty", () => {
    mockedUseTan.mockReturnValue(hookState({ showMentions: true }));

    render(<SocialConsole />);
    expect(screen.getByText("No replies yet")).toBeTruthy();
  });

  it("shows the load error instead of an empty state when the feed fails", () => {
    mockedUseTan.mockReturnValue(
      hookState({
        showMentions: true,
        error: "this node has no identity yet",
      }),
    );

    render(<SocialConsole />);
    expect(screen.getByText("this node has no identity yet")).toBeTruthy();
    expect(screen.queryByText("No replies yet")).toBeNull();
  });
});

describe("SocialConsole pinned view", () => {
  function hookState(overrides: Record<string, unknown> = {}) {
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
      tags: [],
      tagFilter: null,
      setTagFilter: vi.fn(),
      mutedAuthors: [],
      mutedKeywords: [],
      saved: [],
      savedLoading: false,
      savedHasMore: false,
      loadSaved: vi.fn(),
      loadMoreSaved: vi.fn(),
      save: vi.fn(),
      unsave: vi.fn(),
      hidden: [],
      hiddenLoading: false,
      hiddenHasMore: false,
      loadHidden: vi.fn(),
      loadMoreHidden: vi.fn(),
      hide: vi.fn(),
      unhide: vi.fn(),
      pinned: [],
      pinnedLoading: false,
      pinnedHasMore: false,
      loadPinned: vi.fn(async () => {}),
      loadMorePinned: vi.fn(),
      pin: vi.fn(),
      unpin: vi.fn(),
      all: false,
      setAll: vi.fn(),
      view: "timeline",
      setView: vi.fn(),
      showMentions: false,
      setShowMentions: vi.fn(),
      loading: false,
      posting: false,
      syncing: false,
      error: null,
      refresh: vi.fn(),
      search: vi.fn(),
      clearSearch: vi.fn(),
      publish: vi.fn(),
      follow: vi.fn(),
      unfollow: vi.fn(),
      muteAuthor: vi.fn(),
      unmuteAuthor: vi.fn(),
      muteKeyword: vi.fn(),
      unmuteKeyword: vi.fn(),
      like: vi.fn(),
      unlike: vi.fn(),
      repost: vi.fn(),
      deletePost: vi.fn(),
      sync: vi.fn(),
      threadRef: null,
      threadPosts: null,
      threadLoading: false,
      threadError: null,
      openThread: vi.fn(),
      closeThread: vi.fn(),
      ...overrides,
    };
  }

  it("switches the feed to pinned posts and loads them on demand", async () => {
    const timelinePost = makePost({ text: "timeline post" });
    const pinnedPost = makePost({
      hash: "e".repeat(64),
      text: "keep me on top",
      pinned: true,
      pinned_at: 2000,
      likes: 1,
    });
    const state = hookState({ posts: [timelinePost], pinned: [pinnedPost] });
    mockedUseTan.mockReturnValue(state);

    const { rerender } = render(<SocialConsole />);
    expect(screen.getByText("timeline post")).toBeTruthy();
    expect(screen.queryByText("keep me on top")).toBeNull();

    fireEvent.click(screen.getByText("pinned"));
    await waitFor(() => expect(state.loadPinned).toHaveBeenCalledTimes(1));

    rerender(<SocialConsole />);
    expect(screen.queryByText("timeline post")).toBeNull();
    expect(screen.getByText("keep me on top")).toBeTruthy();
    expect(screen.getByText("📌 pinned")).toBeTruthy();
    // Pinned cards wire the same likers and thread controls as the
    // timeline cards, not a cut-down set.
    expect(screen.getByTitle("Show who liked this post")).toBeTruthy();
    expect(screen.getByTitle("Open this thread")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "following" }));
    expect(state.setView).toHaveBeenCalledWith("timeline");
    expect(state.setAll).toHaveBeenCalledWith(false);
  });

  it("says there are no pinned posts yet when the pinned list is empty", () => {
    mockedUseTan.mockReturnValue(hookState({}));

    render(<SocialConsole />);
    fireEvent.click(screen.getByText("pinned"));
    expect(screen.getByText("No pinned posts yet")).toBeTruthy();
  });

  it("shows search results instead of pinned posts while searching", async () => {
    const pinnedPost = makePost({
      hash: "e".repeat(64),
      text: "keep me on top",
      pinned: true,
    });
    const hit = makePost({
      hash: "f".repeat(64),
      text: "mesh post",
      own: false,
    });
    const state = hookState({ pinned: [pinnedPost] });
    mockedUseTan.mockReturnValue(state);

    const { rerender } = render(<SocialConsole />);
    fireEvent.click(screen.getByText("pinned"));
    await waitFor(() => expect(state.loadPinned).toHaveBeenCalledTimes(1));
    expect(screen.getByText("keep me on top")).toBeTruthy();

    // A search started from the pinned view shows its results; clearing
    // it returns to the pinned list.
    mockedUseTan.mockReturnValue({
      ...state,
      searchResults: [hit],
      searchQuery: "mesh",
    });
    rerender(<SocialConsole />);
    expect(screen.getByText("mesh post")).toBeTruthy();
    expect(screen.queryByText("keep me on top")).toBeNull();
    expect(screen.getByText("search results")).toBeTruthy();
  });
});

describe("SocialConsole authors directory", () => {
  function hookState(overrides: Record<string, unknown> = {}) {
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
      tags: [],
      tagFilter: null,
      setTagFilter: vi.fn(),
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
      error: null,
      refresh: vi.fn(),
      search: vi.fn(),
      clearSearch: vi.fn(),
      publish: vi.fn(),
      follow: vi.fn(),
      unfollow: vi.fn(),
      muteAuthor: vi.fn(),
      unmuteAuthor: vi.fn(),
      muteKeyword: vi.fn(),
      unmuteKeyword: vi.fn(),
      like: vi.fn(),
      unlike: vi.fn(),
      deletePost: vi.fn(),
      sync: vi.fn(),
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
      ...overrides,
    };
  }

  function makeAuthor(overrides: Record<string, unknown> = {}) {
    return {
      author: "a".repeat(64),
      name: "alice",
      posts: 3,
      last_post: 1000,
      followed: false,
      ...overrides,
    };
  }

  it("lists known authors with a follow button for the unfollowed", () => {
    const state = hookState({
      authors: [
        makeAuthor(),
        makeAuthor({
          author: "b".repeat(64),
          name: "bob",
          posts: 1,
          followed: true,
        }),
      ],
    });
    mockedUseTan.mockReturnValue(state);

    render(<SocialConsole />);
    expect(screen.getByText("alice")).toBeTruthy();
    expect(screen.getByText("bob")).toBeTruthy();
    expect(screen.getByText(/3 posts/)).toBeTruthy();

    fireEvent.click(screen.getByTitle("follow this author"));
    expect(state.follow).toHaveBeenCalledTimes(1);
    expect(state.follow).toHaveBeenCalledWith("a".repeat(64));
  });

  it("offers to remove a followed author from the directory", () => {
    const state = hookState({
      authors: [
        makeAuthor({
          author: "b".repeat(64),
          name: "bob",
          posts: 1,
          followed: true,
        }),
      ],
    });
    mockedUseTan.mockReturnValue(state);

    render(<SocialConsole />);
    fireEvent.click(screen.getByTitle("unfollow this author"));
    expect(state.unfollow).toHaveBeenCalledTimes(1);
    expect(state.unfollow).toHaveBeenCalledWith("b".repeat(64));
  });

  it("says there are no authors yet when the store is empty", () => {
    mockedUseTan.mockReturnValue(hookState());

    render(<SocialConsole />);
    expect(
      screen.getByText("No authors yet. Sync to pull posts from your peers."),
    ).toBeTruthy();
  });

  it("offers the next page while authors are still hidden", () => {
    const state = hookState({
      authors: [makeAuthor()],
      authorsTotal: 120,
    });
    mockedUseTan.mockReturnValue(state);

    render(<SocialConsole />);
    const button = screen.getByText("show more (1 of 120)");
    expect(button).toBeTruthy();

    fireEvent.click(button);
    expect(state.loadMoreAuthors).toHaveBeenCalledTimes(1);
  });

  it("hides the show-more button once every author is shown", () => {
    mockedUseTan.mockReturnValue(
      hookState({ authors: [makeAuthor()], authorsTotal: 1 }),
    );

    render(<SocialConsole />);
    expect(screen.queryByText(/show more/)).toBeNull();
  });
});


describe("SocialConsole hidden view", () => {
  function hookState(overrides: Record<string, unknown> = {}) {
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
      tags: [],
      tagFilter: null,
      setTagFilter: vi.fn(),
      mutedAuthors: [],
      mutedKeywords: [],
      saved: [],
      savedLoading: false,
      savedHasMore: false,
      loadSaved: vi.fn(),
      loadMoreSaved: vi.fn(),
      save: vi.fn(),
      unsave: vi.fn(),
      hidden: [],
      hiddenLoading: false,
      hiddenHasMore: false,
      loadHidden: vi.fn(async () => {}),
      loadMoreHidden: vi.fn(),
      hide: vi.fn(),
      unhide: vi.fn(),
      pinned: [],
      pinnedLoading: false,
      pinnedHasMore: false,
      loadPinned: vi.fn(),
      loadMorePinned: vi.fn(),
      pin: vi.fn(),
      unpin: vi.fn(),
      all: false,
      setAll: vi.fn(),
      view: "timeline",
      setView: vi.fn(),
      showMentions: false,
      setShowMentions: vi.fn(),
      loading: false,
      posting: false,
      syncing: false,
      error: null,
      refresh: vi.fn(),
      search: vi.fn(),
      clearSearch: vi.fn(),
      publish: vi.fn(),
      follow: vi.fn(),
      unfollow: vi.fn(),
      muteAuthor: vi.fn(),
      unmuteAuthor: vi.fn(),
      muteKeyword: vi.fn(),
      unmuteKeyword: vi.fn(),
      like: vi.fn(),
      unlike: vi.fn(),
      repost: vi.fn(),
      deletePost: vi.fn(),
      sync: vi.fn(),
      threadRef: null,
      threadPosts: null,
      threadLoading: false,
      threadError: null,
      openThread: vi.fn(),
      closeThread: vi.fn(),
      ...overrides,
    };
  }

  it("switches the feed to hidden posts and loads them on demand", async () => {
    const timelinePost = makePost({ text: "timeline post" });
    const hiddenPost = makePost({
      hash: "e".repeat(64),
      text: "regret this",
    });
    const state = hookState({ posts: [timelinePost], hidden: [hiddenPost] });
    mockedUseTan.mockReturnValue(state);

    const { rerender } = render(<SocialConsole />);
    expect(screen.getByText("timeline post")).toBeTruthy();
    expect(screen.queryByText("regret this")).toBeNull();

    // A timeline card offers to hide the post.
    fireEvent.click(screen.getByTitle("Hide this post from your local views"));
    expect(state.hide).toHaveBeenCalledWith(timelinePost.hash);

    fireEvent.click(screen.getByText("hidden"));
    await waitFor(() => expect(state.loadHidden).toHaveBeenCalledTimes(1));

    rerender(<SocialConsole />);
    expect(screen.queryByText("timeline post")).toBeNull();
    expect(screen.getByText("regret this")).toBeTruthy();
    // Hidden cards offer to unhide instead of hiding again.
    expect(screen.queryByTitle("Hide this post from your local views")).toBeNull();
    fireEvent.click(
      screen.getByTitle("Show this post in your local views again"),
    );
    expect(state.unhide).toHaveBeenCalledWith(hiddenPost.hash);

    fireEvent.click(screen.getByRole("button", { name: "following" }));
    expect(state.setView).toHaveBeenCalledWith("timeline");
    expect(state.setAll).toHaveBeenCalledWith(false);
  });

  it("says there are no hidden posts yet when the hidden list is empty", () => {
    mockedUseTan.mockReturnValue(hookState({}));

    render(<SocialConsole />);
    fireEvent.click(screen.getByText("hidden"));
    expect(screen.getByText("No hidden posts yet")).toBeTruthy();
  });
});

describe("PostCard save control", () => {
  function renderCard(post: TanPost, onToggleSave = vi.fn()) {
    render(
      <PostCard
        post={post}
        onToggleLike={() => {}}
        onRepost={() => {}}
        onToggleSave={onToggleSave}
        onReply={() => {}}
        onDelete={vi.fn()}
        deleting={false}
      />,
    );
    return onToggleSave;
  }

  it("offers to save an unsaved post and calls back with the post", () => {
    const post = makePost({ saved: false });
    const onToggleSave = renderCard(post);

    fireEvent.click(screen.getByTitle("Save this post to read later"));
    expect(onToggleSave).toHaveBeenCalledTimes(1);
    expect(onToggleSave).toHaveBeenCalledWith(post);
  });

  it("shows a saved post as saved, with when it was saved", () => {
    renderCard(makePost({ saved: true, saved_at: 2000 }));

    expect(screen.getByTitle("Remove from saved posts")).toBeTruthy();
    expect(screen.getByText("★ saved")).toBeTruthy();
    expect(screen.getByText(/saved /)).toBeTruthy();
  });

  it("hides the save control when no handler is given", () => {
    render(
      <PostCard
        post={makePost()}
        onToggleLike={() => {}}
        onRepost={() => {}}
        onReply={() => {}}
        onDelete={vi.fn()}
        deleting={false}
      />,
    );

    expect(screen.queryByTitle("Save this post to read later")).toBeNull();
    expect(screen.queryByTitle("Remove from saved posts")).toBeNull();
  });

  it("shows a deleted saved post as deleted, keeping only unsave", () => {
    renderCard(makePost({ deleted: true, text: "", saved: true }));

    expect(screen.getByText("This post was deleted.")).toBeTruthy();
    expect(screen.queryByTitle("Like this post")).toBeNull();
    expect(screen.queryByTitle("Reply to this post")).toBeNull();
    expect(screen.queryByTitle("Delete this post")).toBeNull();
    // The save toggle stays so the post can still be removed from the list.
    expect(screen.getByTitle("Remove from saved posts")).toBeTruthy();
  });
});

describe("PostCard pin control", () => {
  function renderCard(
    post: TanPost,
    onTogglePin = vi.fn(),
    onToggleSave = vi.fn(),
  ) {
    render(
      <PostCard
        post={post}
        onToggleLike={() => {}}
        onRepost={() => {}}
        onToggleSave={onToggleSave}
        onTogglePin={onTogglePin}
        onReply={() => {}}
        onDelete={vi.fn()}
        deleting={false}
      />,
    );
    return onTogglePin;
  }

  it("offers to pin an unpinned own post and calls back with the post", () => {
    const post = makePost({ own: true, pinned: false });
    const onTogglePin = renderCard(post);

    fireEvent.click(screen.getByTitle("Pin this post to the top of your pinned list"));
    expect(onTogglePin).toHaveBeenCalledTimes(1);
    expect(onTogglePin).toHaveBeenCalledWith(post);
  });

  it("shows a pinned post as pinned, with when it was pinned", () => {
    renderCard(makePost({ own: true, pinned: true, pinned_at: 2000 }));

    expect(screen.getByTitle("Remove from pinned posts")).toBeTruthy();
    expect(screen.getByText("📌 pinned")).toBeTruthy();
    expect(screen.getByText(/pinned /)).toBeTruthy();
  });

  it("hides the pin control on other authors' posts", () => {
    renderCard(makePost({ own: false, pinned: false }));

    expect(screen.queryByTitle("Pin this post to the top of your pinned list")).toBeNull();
    expect(screen.queryByTitle("Remove from pinned posts")).toBeNull();
  });

  it("hides the pin control when no handler is given", () => {
    render(
      <PostCard
        post={makePost({ own: true })}
        onToggleLike={() => {}}
        onRepost={() => {}}
        onToggleSave={vi.fn()}
        onReply={() => {}}
        onDelete={vi.fn()}
        deleting={false}
      />,
    );

    expect(screen.queryByTitle("Pin this post to the top of your pinned list")).toBeNull();
    expect(screen.queryByTitle("Remove from pinned posts")).toBeNull();
  });
});

describe("PostCard hashtags", () => {
  it("renders hashtags as buttons that filter the timeline", () => {
    const onTag = vi.fn();
    render(
      <PostCard
        post={makePost({ text: "hello #mesh and #radio-2" })}
        onToggleLike={() => {}}
        onRepost={() => {}}
        onReply={() => {}}
        onDelete={() => {}}
        deleting={false}
        onTag={onTag}
      />,
    );

    fireEvent.click(screen.getByTitle("show #mesh posts"));
    expect(onTag).toHaveBeenCalledTimes(1);
    expect(onTag).toHaveBeenCalledWith("mesh");
    fireEvent.click(screen.getByTitle("show #radio-2 posts"));
    expect(onTag).toHaveBeenCalledWith("radio-2");
  });

  it("does not linkify a hash inside a word", () => {
    const onTag = vi.fn();
    render(
      <PostCard
        post={makePost({ text: "C# code and a#b are not tags" })}
        onToggleLike={() => {}}
        onRepost={() => {}}
        onReply={() => {}}
        onDelete={() => {}}
        deleting={false}
        onTag={onTag}
      />,
    );

    expect(screen.getByText("C# code and a#b are not tags")).toBeTruthy();
    expect(onTag).not.toHaveBeenCalled();
  });

  it("renders plain text when no tag handler is given", () => {
    render(
      <PostCard
        post={makePost({ text: "hello #mesh" })}
        onToggleLike={() => {}}
        onRepost={() => {}}
        onReply={() => {}}
        onDelete={() => {}}
        deleting={false}
      />,
    );

    expect(screen.getByText("hello #mesh")).toBeTruthy();
    expect(screen.queryByTitle("show #mesh posts")).toBeNull();
  });
});

describe("SocialConsole tags sidebar", () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  // The search describe's makeTan covers the whole hook shape; the
  // tags tests reuse it with the tag fields set.
  function setTanWithTags(overrides: Record<string, unknown>) {
    const setTagFilter = vi.fn();
    mockTan.current = {
      posts: [],
      searchResults: [],
      searchQuery: null,
      mentions: [],
      following: [],
      authors: [],
      authorsTotal: 0,
      loadMoreAuthors: vi.fn(),
      suggestions: [],
      tags: [
        { tag: "mesh", count: 3 },
        { tag: "radio", count: 1 },
      ],
      tagFilter: null,
      setTagFilter,
      mutedAuthors: [],
      mutedKeywords: [],
      saved: [],
      savedLoading: false,
      savedHasMore: false,
      loadSaved: vi.fn(),
      loadMoreSaved: vi.fn(),
      save: vi.fn(),
      unsave: vi.fn(),
      hidden: [],
      hiddenLoading: false,
      hiddenHasMore: false,
      loadHidden: vi.fn(),
      loadMoreHidden: vi.fn(),
      hide: vi.fn(),
      unhide: vi.fn(),
      all: false,
      setAll: vi.fn(),
      view: "timeline",
      setView: vi.fn(),
      showMentions: false,
      setShowMentions: vi.fn(),
      loading: false,
      posting: false,
      syncing: false,
      error: null,
      threadRef: null,
      threadPosts: null,
      threadLoading: false,
      threadError: null,
      openThread: vi.fn(),
      closeThread: vi.fn(),
      refresh: vi.fn(),
      search: vi.fn(),
      clearSearch: vi.fn(),
      publish: vi.fn(),
      follow: vi.fn(),
      unfollow: vi.fn(),
      muteAuthor: vi.fn(),
      unmuteAuthor: vi.fn(),
      muteKeyword: vi.fn(),
      unmuteKeyword: vi.fn(),
      like: vi.fn(),
      unlike: vi.fn(),
      deletePost: vi.fn(),
      sync: vi.fn(),
      ...overrides,
    };
    return { tan: mockTan.current, setTagFilter };
  }

  it("lists tags most-used first and filters on click", () => {
    const { setTagFilter } = setTanWithTags({});
    render(<SocialConsole />);

    const mesh = screen.getByRole("button", { name: "#mesh · 3" });
    const radio = screen.getByRole("button", { name: "#radio · 1" });
    expect(mesh.compareDocumentPosition(radio)).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    );

    fireEvent.click(mesh);
    expect(setTagFilter).toHaveBeenCalledTimes(1);
    expect(setTagFilter).toHaveBeenCalledWith("mesh");
  });

  it("shows the empty state when there are no tags", () => {
    setTanWithTags({ tags: [] });
    render(<SocialConsole />);

    expect(
      screen.getByText("No hashtags in stored posts yet. Post with a #tag, or sync to pull tagged posts from your peers."),
    ).toBeTruthy();
  });

  it("shows the active filter chip and clears it on click", () => {
    const { setTagFilter } = setTanWithTags({ tagFilter: "mesh" });
    render(<SocialConsole />);

    fireEvent.click(screen.getByRole("button", { name: "#mesh ✕" }));
    expect(setTagFilter).toHaveBeenCalledTimes(1);
    expect(setTagFilter).toHaveBeenCalledWith(null);
  });

  it("clicking a hashtag in a post filters the timeline", () => {
    const { setTagFilter } = setTanWithTags({
      posts: [makePost({ text: "hello #mesh" })],
    });
    render(<SocialConsole />);

    fireEvent.click(screen.getByTitle("show #mesh posts"));
    expect(setTagFilter).toHaveBeenCalledTimes(1);
    expect(setTagFilter).toHaveBeenCalledWith("mesh");
  });
});
