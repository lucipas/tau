import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Mock } from "vitest";

import type { MeshPage } from "./types";
import { usePages } from "./use-pages";

vi.mock("./api", () => ({
  apiGet: vi.fn(),
  apiPost: vi.fn(),
}));

import { apiGet, apiPost } from "./api";

const mockedGet = apiGet as Mock;
const mockedPost = apiPost as Mock;

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

describe("usePages", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("loads cached pages, selects the first, and keeps the own page separate", async () => {
    const page = makePage();
    const mine = makePage({
      author: "b".repeat(32),
      title: "Tester page",
      markdown: "# Hello\n",
    });
    mockedGet.mockResolvedValue({
      pages: [page],
      mine,
      count: 1,
    });

    const { result } = renderHook(() => usePages());
    await waitFor(() => expect(result.current.loading).toBe(false));

    expect(result.current.pages).toEqual([page]);
    expect(result.current.mine).toEqual(mine);
    expect(result.current.selected).toBe(page.author);
    expect(result.current.selectedPage).toEqual(page);
    expect(result.current.error).toBeNull();
  });

  it("selects the own page when nothing has been fetched yet", async () => {
    const mine = makePage({ author: "b".repeat(32), title: "Tester page" });
    mockedGet.mockResolvedValue({ pages: [], mine, count: 0 });

    const { result } = renderHook(() => usePages());
    await waitFor(() => expect(result.current.loading).toBe(false));

    expect(result.current.selected).toBe(mine.author);
    expect(result.current.selectedPage).toEqual(mine);
  });

  it("surfaces a list failure without clearing the selection", async () => {
    mockedGet.mockRejectedValue(new Error("node down"));

    const { result } = renderHook(() => usePages());
    await waitFor(() => expect(result.current.loading).toBe(false));

    expect(result.current.error).toBe("node down");
    expect(result.current.pages).toEqual([]);
  });

  it("fetches a page, reloads the list and selects it", async () => {
    const page = makePage();
    mockedGet.mockResolvedValue({ pages: [page], mine: null, count: 1 });
    mockedPost.mockResolvedValue({ ok: true, page });

    const { result } = renderHook(() => usePages());
    await waitFor(() => expect(result.current.loading).toBe(false));

    let fetched: MeshPage | null = null;
    await act(async () => {
      fetched = await result.current.fetchPage("alice");
    });
    expect(fetched).toEqual(page);
    expect(mockedPost).toHaveBeenCalledWith("/api/pages/fetch", {
      peer: "alice",
    });
    expect(result.current.selected).toBe(page.author);
    expect(result.current.fetchError).toBeNull();
  });

  it("notes when the peer publishes no page, and records a fetch failure", async () => {
    mockedGet.mockResolvedValue({ pages: [], mine: null, count: 0 });
    mockedPost.mockResolvedValueOnce({ ok: true, page: null });

    const { result } = renderHook(() => usePages());
    await waitFor(() => expect(result.current.loading).toBe(false));

    let fetched: MeshPage | null = null;
    await act(async () => {
      fetched = await result.current.fetchPage("bob");
    });
    expect(fetched).toBeNull();
    expect(result.current.fetchNote).toBe("that peer publishes no page");

    mockedPost.mockRejectedValueOnce(new Error("peer unreachable"));
    await act(async () => {
      fetched = await result.current.fetchPage("alice");
    });
    expect(fetched).toBeNull();
    expect(result.current.fetchError).toBe("peer unreachable");
  });
});
