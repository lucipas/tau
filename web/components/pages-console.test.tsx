import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type { MeshPage, Peer } from "@/lib/types";

import { PagesConsole } from "./pages-console";

const mockPages = vi.hoisted(() => ({
  current: {} as Record<string, unknown>,
}));

vi.mock("@/lib/use-pages", () => ({
  usePages: vi.fn(() => mockPages.current),
}));

vi.mock("@/lib/api", () => ({
  apiDownload: vi.fn(),
}));

import { apiDownload } from "@/lib/api";

const mockedApiDownload = apiDownload as Mock;

function makePage(overrides: Partial<MeshPage> = {}): MeshPage {
  return {
    author: "a".repeat(32),
    title: "Mesh meetup",
    markdown: "# Mesh meetup\n\nSaturday, 2pm.\n",
    files: [],
    version: 1,
    updated_at: 1000,
    ...overrides,
  };
}

function makePeer(overrides: Partial<Peer> = {}): Peer {
  return {
    destination: "a".repeat(32),
    name: "alice",
    last_seen: 1000,
    age: 5,
    hops: 1,
    protocol: 1,
    ...overrides,
  };
}

function makeState(overrides: Record<string, unknown> = {}) {
  return {
    pages: [] as MeshPage[],
    mine: null as MeshPage | null,
    selected: null as string | null,
    selectedPage: null as MeshPage | null,
    loading: false,
    error: null as string | null,
    fetching: false,
    fetchError: null as string | null,
    fetchNote: null as string | null,
    refresh: vi.fn(async () => undefined),
    select: vi.fn(),
    fetchPage: vi.fn(async () => null),
    ...overrides,
  };
}

describe("PagesConsole", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockPages.current = makeState();
  });

  it("shows the empty state when no pages exist", () => {
    render(<PagesConsole peers={[]} />);

    expect(screen.getByText(/no pages yet/)).toBeTruthy();
  });

  it("lists cached pages and the own page, and selects on click", () => {
    const page = makePage();
    const mine = makePage({
      author: "b".repeat(32),
      title: "Tester page",
      markdown: "# Hello\n",
    });
    const select = vi.fn();
    mockPages.current = makeState({
      pages: [page],
      mine,
      selected: page.author,
      selectedPage: page,
      select,
    });
    render(<PagesConsole peers={[makePeer()]} />);

    expect(screen.getAllByText("Mesh meetup").length).toBeGreaterThan(0);
    expect(screen.getByText(/your page — Tester page/)).toBeTruthy();
    // The author resolves to the peer name, not the raw hash.
    expect(screen.getAllByText(/alice/).length).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("button", { name: /Tester page/ }));
    expect(select).toHaveBeenCalledWith("b".repeat(32));
  });

  it("renders the selected page's Markdown", () => {
    const page = makePage();
    mockPages.current = makeState({
      pages: [page],
      selected: page.author,
      selectedPage: page,
    });
    render(<PagesConsole peers={[makePeer()]} />);

    expect(
      screen.getByRole("heading", { name: "Mesh meetup" }),
    ).toBeTruthy();
    expect(screen.getByText(/Saturday, 2pm/)).toBeTruthy();
  });

  it("fetches a page by peer name and clears the input on success", async () => {
    const fetchPage = vi.fn(async () => makePage());
    mockPages.current = makeState({ fetchPage });
    render(<PagesConsole peers={[]} />);

    fireEvent.change(screen.getByLabelText("fetch a page from a peer"), {
      target: { value: "alice" },
    });
    fireEvent.click(screen.getByRole("button", { name: "fetch" }));

    await waitFor(() => expect(fetchPage).toHaveBeenCalledWith("alice"));
    await waitFor(() =>
      expect(
        (
          screen.getByLabelText(
            "fetch a page from a peer",
          ) as HTMLInputElement
        ).value,
      ).toBe(""),
    );
  });

  it("shows a fetch failure beside the form", () => {
    mockPages.current = makeState({ fetchError: "peer unreachable" });
    render(<PagesConsole peers={[]} />);

    expect(screen.getByText("peer unreachable")).toBeTruthy();
  });

  it("shows a note when the peer publishes no page", () => {
    mockPages.current = makeState({
      fetchNote: "that peer publishes no page",
    });
    render(<PagesConsole peers={[]} />);

    expect(screen.getByText("that peer publishes no page")).toBeTruthy();
  });

  it("downloads a linked file", async () => {
    const page = makePage({
      files: [{ name: "notes.txt", sha256: "c".repeat(64), size: 12 }],
    });
    mockedApiDownload.mockResolvedValue({
      blob: new Blob(["meetup notes"]),
      filename: "notes.txt",
    });
    const createObjectURL = vi.fn(() => "blob:fake");
    const revokeObjectURL = vi.fn();
    vi.stubGlobal("URL", {
      ...URL,
      createObjectURL,
      revokeObjectURL,
    });
    mockPages.current = makeState({
      pages: [page],
      selected: page.author,
      selectedPage: page,
    });
    render(<PagesConsole peers={[makePeer()]} />);

    fireEvent.click(screen.getByRole("button", { name: /notes\.txt/ }));

    await waitFor(() =>
      expect(mockedApiDownload).toHaveBeenCalledWith(
        `/api/pages/${"a".repeat(32)}/files/${"c".repeat(64)}`,
      ),
    );
    vi.unstubAllGlobals();
  });
});
