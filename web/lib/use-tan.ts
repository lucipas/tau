"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { apiGet, apiPost } from "./api";
import type {
  TanFollow,
  TanFollowingResponse,
  TanPost,
  TanPostResponse,
  TanSavedResponse,
  TanSyncResponse,
  TanTimelineResponse,
} from "./types";

const POLL_INTERVAL_MS = 5000;
const SAVED_PAGE_SIZE = 100;

export function useTan() {
  const [posts, setPosts] = useState<TanPost[]>([]);
  const [following, setFollowing] = useState<TanFollow[]>([]);
  const [saved, setSaved] = useState<TanPost[]>([]);
  const [savedLoading, setSavedLoading] = useState(false);
  const [savedHasMore, setSavedHasMore] = useState(false);
  const [all, setAll] = useState(false);
  const [loading, setLoading] = useState(true);
  const [posting, setPosting] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // The filter the timeline is showing right now. Actions refresh through
  // this ref instead of a captured `all`, so a filter switch mid-action
  // does not reload the filter that is no longer selected.
  const allRef = useRef(all);
  useEffect(() => {
    allRef.current = all;
  }, [all]);

  // Monotonic generation of feed loads. A newer load supersedes an older
  // one, so a slow response (for example a poll tick started before a
  // delete) must not overwrite a newer refresh.
  const generationRef = useRef(0);
  // The load currently in flight, if any, and when it started. A routine
  // poll skips while one is pending instead of invalidating it — otherwise
  // every response could be discarded while requests stay slow. An action
  // (delete, filter change) still supersedes. A load pending longer than
  // two poll intervals is treated as hung and may be superseded by the
  // next poll, so one stuck request cannot freeze the feed.
  const pendingRef = useRef<{ generation: number; startedAt: number } | null>(
    null,
  );

  const load = useCallback(async (options?: { supersede?: boolean }) => {
    const supersede = options?.supersede ?? true;
    const pending = pendingRef.current;
    if (
      !supersede &&
      pending !== null &&
      Date.now() - pending.startedAt < POLL_INTERVAL_MS * 2
    ) {
      return false;
    }
    const generation = ++generationRef.current;
    pendingRef.current = { generation, startedAt: Date.now() };
    try {
      const suffix = allRef.current ? "?all=1&limit=100" : "?limit=100";
      const [timeline, follows] = await Promise.all([
        apiGet<TanTimelineResponse>(`/api/tan/timeline${suffix}`),
        apiGet<TanFollowingResponse>("/api/tan/following"),
      ]);
      if (generationRef.current !== generation) return false;
      setPosts(timeline.posts);
      setFollowing(follows.following);
      setError(null);
      return true;
    } finally {
      if (pendingRef.current?.generation === generation) {
        pendingRef.current = null;
      }
    }
  }, []);

  useEffect(() => {
    let disposed = false;
    async function tick(supersede: boolean) {
      try {
        await load({ supersede });
      } catch (caught) {
        if (!disposed) {
          setError(
            caught instanceof Error ? caught.message : "could not load the feed",
          );
        }
      } finally {
        if (!disposed) setLoading(false);
      }
    }
    // Re-running on a filter change must load the newly selected filter
    // even if a poll for the old one is still in flight; routine polls
    // skip while a load is pending instead of invalidating it.
    void tick(true);
    const timer = setInterval(() => void tick(false), POLL_INTERVAL_MS);
    return () => {
      disposed = true;
      clearInterval(timer);
    };
  }, [all, load]);

  const refresh = useCallback(async () => {
    await load();
  }, [load]);

  // Saved posts change only through this client's save/unsave actions
  // (or the CLI), so they load on demand instead of on every poll tick.
  // The store holds up to 512 saves, so the view pages through them
  // 100 at a time instead of fetching only the newest page.
  const savedRef = useRef(saved);
  useEffect(() => {
    savedRef.current = saved;
  }, [saved]);
  // Monotonic generation of saved loads, mirroring the feed's: a newer
  // load supersedes an older one, so a slow response cannot restore a
  // post that was unsaved after it started.
  const savedGenerationRef = useRef(0);
  // Whether the saved list has been loaded at least once. Actions
  // refresh it only then, so a like from the timeline does not pay for
  // a list nobody has opened.
  const savedLoadedRef = useRef(false);

  const fetchSavedPage = async (offset: number) => {
    return apiGet<TanSavedResponse>(
      `/api/tan/saved?limit=${SAVED_PAGE_SIZE}&offset=${offset}`,
    );
  };

  const loadSaved = useCallback(async () => {
    const generation = ++savedGenerationRef.current;
    setSavedLoading(true);
    try {
      const result = await fetchSavedPage(0);
      if (savedGenerationRef.current !== generation) return;
      setSaved(result.posts);
      setSavedHasMore(result.posts.length === SAVED_PAGE_SIZE);
      savedLoadedRef.current = true;
    } finally {
      if (savedGenerationRef.current === generation) {
        setSavedLoading(false);
      }
    }
  }, []);

  const loadMoreSaved = useCallback(async () => {
    const generation = ++savedGenerationRef.current;
    setSavedLoading(true);
    try {
      const result = await fetchSavedPage(savedRef.current.length);
      if (savedGenerationRef.current !== generation) return;
      setSaved((previous) => [...previous, ...result.posts]);
      setSavedHasMore(result.posts.length === SAVED_PAGE_SIZE);
    } finally {
      if (savedGenerationRef.current === generation) {
        setSavedLoading(false);
      }
    }
  }, []);

  // Re-read every page currently shown. A save or unsave shifts the
  // offsets behind the shown pages, so refreshing only the first page
  // would leave the rest stale.
  const reloadSaved = useCallback(async () => {
    const shown = savedRef.current.length;
    if (shown === 0) return;
    const generation = ++savedGenerationRef.current;
    setSavedLoading(true);
    try {
      const pages = Math.ceil(shown / SAVED_PAGE_SIZE);
      const all: TanPost[] = [];
      for (let page = 0; page < pages; page += 1) {
        const result = await fetchSavedPage(page * SAVED_PAGE_SIZE);
        if (savedGenerationRef.current !== generation) return;
        all.push(...result.posts);
        if (result.posts.length < SAVED_PAGE_SIZE) break;
      }
      if (savedGenerationRef.current !== generation) return;
      setSaved(all);
      setSavedHasMore(all.length === pages * SAVED_PAGE_SIZE);
    } finally {
      if (savedGenerationRef.current === generation) {
        setSavedLoading(false);
      }
    }
  }, []);

  const refreshSavedIfLoaded = useCallback(async () => {
    if (savedLoadedRef.current) {
      await reloadSaved();
    }
  }, [reloadSaved]);

  const save = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/save", { target });
      await Promise.all([load(), reloadSaved()]);
    },
    [load, reloadSaved],
  );

  const unsave = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/unsave", { target });
      await Promise.all([load(), reloadSaved()]);
    },
    [load, reloadSaved],
  );

  const publish = useCallback(
    async (text: string, replyTo?: string) => {
      setPosting(true);
      try {
        const result = await apiPost<TanPostResponse>("/api/tan/posts", {
          text,
          ...(replyTo ? { reply_to: replyTo } : {}),
        });
        setPosts((previous) => {
          if (previous.some((post) => post.hash === result.post.hash)) {
            return previous;
          }
          return [result.post, ...previous];
        });
        await load();
        return result.post;
      } finally {
        setPosting(false);
      }
    },
    [load],
  );

  const follow = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/follow", { target });
      await load();
    },
    [load],
  );

  const unfollow = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/unfollow", { target });
      await load();
    },
    [load],
  );

  const like = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/like", { target });
      await Promise.all([load(), refreshSavedIfLoaded()]);
    },
    [load, refreshSavedIfLoaded],
  );

  const unlike = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/unlike", { target });
      await Promise.all([load(), refreshSavedIfLoaded()]);
    },
    [load, refreshSavedIfLoaded],
  );

  const deletePost = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/delete", { target });
      await Promise.all([load(), refreshSavedIfLoaded()]);
    },
    [load, refreshSavedIfLoaded],
  );

  const sync = useCallback(async () => {
    setSyncing(true);
    try {
      const result = await apiPost<TanSyncResponse>("/api/tan/sync", {
        explore: true,
      });
      await load();
      return result.sync;
    } finally {
      setSyncing(false);
    }
  }, [load]);

  return {
    posts,
    following,
    saved,
    savedLoading,
    savedHasMore,
    loadSaved,
    loadMoreSaved,
    save,
    unsave,
    all,
    setAll,
    loading,
    posting,
    syncing,
    error,
    refresh,
    publish,
    follow,
    unfollow,
    like,
    unlike,
    deletePost,
    sync,
  };
}
