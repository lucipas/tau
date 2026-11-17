import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";

import type { MeshPage } from "@/lib/types";
import type { usePages } from "@/lib/use-pages";
import type { UseTauResult } from "@/lib/use-tau";

import { PagesTab } from "./pages-tab";

function makePage(overrides: Partial<MeshPage> = {}): MeshPage {
  return {
    author: "a".repeat(64),
    title: "Alice's page",
    markdown: "# Welcome\n\nThis is **bold** and [a link](https://example.com).",
    files: [{ name: "notes.txt", sha256: "f".repeat(64), size: 120 }],
    version: 2,
    updated_at: 1763400000,
    ...overrides,
  };
}

function makePages(
  overrides: Partial<ReturnType<typeof usePages>> = {},
): ReturnType<typeof usePages> {
  const page = makePage();
  return {
    pages: [page],
    mine: null,
    selected: page.author,
    selectedPage: page,
    loading: false,
    error: null,
    fetching: false,
    fetchError: null,
    fetchNote: null,
    refresh: vi.fn(),
    select: vi.fn(),
    fetchPage: vi.fn(),
    ...overrides,
  } as unknown as ReturnType<typeof usePages>;
}

function makeTau(): UseTauResult {
  return {
    peers: [
      {
        destination: "a".repeat(64),
        name: "Alice",
        last_seen: 0,
        age: 0,
        hops: 1,
        protocol: 1,
      },
    ],
    groups: [],
  } as unknown as UseTauResult;
}

describe("PagesTab", () => {
  it("renders the page list with titles and authors", () => {
    render(<PagesTab tau={makeTau()} pages={makePages()} />);
    // The title shows both in the list row and as the selected page
    // heading; the list row is the button carrying the author line.
    expect(
      screen.getByRole("button", { name: /Alice's page/ }),
    ).toBeDefined();
    expect(
      screen.getByRole("button", { name: /Alice ·/ }),
    ).toBeDefined();
  });

  it("renders the selected page's markdown", () => {
    render(<PagesTab tau={makeTau()} pages={makePages()} />);
    expect(
      screen.getByRole("heading", { level: 1, name: "Welcome" }),
    ).toBeDefined();
    expect(screen.getByText("bold").tagName).toBe("STRONG");
    const link = screen.getByRole("link", { name: "a link" });
    expect(link.getAttribute("href")).toBe("https://example.com");
  });

  it("shows the own-page badge for the own page", () => {
    const mine = makePage({ author: "b".repeat(64), title: "My page" });
    render(
      <PagesTab
        tau={makeTau()}
        pages={makePages({ mine, pages: [], selected: mine.author, selectedPage: mine })}
      />,
    );
    expect(screen.getAllByText("own page").length).toBeGreaterThan(0);
    expect(screen.getByText("My page")).toBeDefined();
  });

  it("lists page files as download buttons", () => {
    render(<PagesTab tau={makeTau()} pages={makePages()} />);
    expect(
      screen.getByRole("button", { name: /notes\.txt/ }),
    ).toBeDefined();
  });

  it("calls select when another page is clicked", () => {
    const other = makePage({
      author: "c".repeat(64),
      title: "Carol's page",
    });
    const pages = makePages({ pages: [makePage(), other] });
    render(<PagesTab tau={makeTau()} pages={pages} />);
    fireEvent.click(
      screen.getByRole("button", { name: /Carol's page/ }),
    );
    expect(pages.select).toHaveBeenCalledWith("c".repeat(64));
  });

  it("calls fetchPage from the fetch form", () => {
    const pages = makePages();
    render(<PagesTab tau={makeTau()} pages={pages} />);
    fireEvent.change(screen.getByLabelText("Fetch a page from a peer"), {
      target: { value: "Alice" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Fetch" }));
    expect(pages.fetchPage).toHaveBeenCalledWith("Alice");
  });

  it("shows an empty state when there are no pages", () => {
    render(
      <PagesTab
        tau={makeTau()}
        pages={makePages({
          pages: [],
          mine: null,
          selected: null,
          selectedPage: null,
        })}
      />,
    );
    expect(screen.getByText(/No pages yet/)).toBeDefined();
  });
});
