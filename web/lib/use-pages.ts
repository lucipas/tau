// use-pages.ts — personal mesh pages for the Pages tab.
//
// Provides:
//   usePages() — cached peer pages plus this node's own page
//                (GET /api/pages), and fetch-a-peer's-page over RNS
//                (POST /api/pages/fetch). Page files are downloaded via
//                /api/pages/<author>/files/<sha256> (see pageFileUrl in api.ts).

"use client";

import { useCallback, useEffect, useState } from "react";
import { fetchPage, getPages } from "./api";
import type { MeshPage } from "./types";

export interface UsePagesResult {
  pages: MeshPage[];
  mine: MeshPage | null;
  count: number;
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
  /** Fetch one peer's page over RNS; returns the page or null. */
  fetch: (peer: string) => Promise<MeshPage | null>;
}

export function usePages(): UsePagesResult {
  const [pages, setPages] = useState<MeshPage[]>([]);
  const [mine, setMine] = useState<MeshPage | null>(null);
  const [count, setCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const data = await getPages();
      setPages(data.pages);
      setMine(data.mine);
      setCount(data.count);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const fetch = useCallback(
    async (peer: string) => {
      const data = await fetchPage(peer);
      if (data.page) await refresh();
      return data.page;
    },
    [refresh],
  );

  return { pages, mine, count, loading, error, refresh, fetch };
}
