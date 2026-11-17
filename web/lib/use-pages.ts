"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { apiGet, apiPost } from "./api";
import type { MeshPage, PageFetchResponse, PagesResponse } from "./types";

function errorMessage(caught: unknown): string {
  return caught instanceof Error ? caught.message : "request failed";
}

export function usePages() {
  const [pages, setPages] = useState<MeshPage[]>([]);
  const [mine, setMine] = useState<MeshPage | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [fetching, setFetching] = useState(false);
  const [fetchError, setFetchError] = useState<string | null>(null);
  const [fetchNote, setFetchNote] = useState<string | null>(null);
  const selectedRef = useRef<string | null>(null);
  useEffect(() => {
    selectedRef.current = selected;
  }, [selected]);
  // A newer refresh supersedes an older one, so a slow list never
  // overwrites a page selection that already happened.
  const generationRef = useRef(0);

  const load = useCallback(async () => {
    const generation = ++generationRef.current;
    try {
      const response = await apiGet<PagesResponse>("/api/pages");
      if (generation !== generationRef.current) return;
      setPages(response.pages);
      setMine(response.mine);
      setError(null);
      const current = selectedRef.current;
      const known =
        response.pages.some((page) => page.author === current) ||
        response.mine?.author === current;
      if (!known) {
        // First load, or the selected page went away: open the
        // newest cached page, or the own page when nothing has
        // been fetched yet.
        setSelected(
          response.pages[0]?.author ?? response.mine?.author ?? null,
        );
      }
    } catch (caught) {
      if (generation !== generationRef.current) return;
      setError(errorMessage(caught));
    } finally {
      if (generation === generationRef.current) setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const fetchPage = useCallback(
    async (peer: string) => {
      setFetching(true);
      setFetchError(null);
      setFetchNote(null);
      try {
        const response = await apiPost<PageFetchResponse>("/api/pages/fetch", {
          peer,
        });
        if (response.page === null) {
          setFetchNote("that peer publishes no page");
          return null;
        }
        await load();
        setSelected(response.page.author);
        return response.page;
      } catch (caught) {
        setFetchError(errorMessage(caught));
        return null;
      } finally {
        setFetching(false);
      }
    },
    [load],
  );

  const select = useCallback((author: string | null) => {
    setSelected(author);
  }, []);

  // The own page is selectable by author too, even though it lives
  // outside the cached list.
  const selectedPage =
    pages.find((page) => page.author === selected) ??
    (mine !== null && mine.author === selected ? mine : null);

  return {
    pages,
    mine,
    selected,
    selectedPage,
    loading,
    error,
    fetching,
    fetchError,
    fetchNote,
    refresh: load,
    select,
    fetchPage,
  };
}
