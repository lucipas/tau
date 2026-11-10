"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { apiGet, apiPost } from "./api";
import type {
  TanFollow,
  TanFollowingResponse,
  TanPost,
  TanPostResponse,
  TanSuggestResponse,
  TanSuggestion,
  TanSyncResponse,
  TanTimelineResponse,
} from "./types";

const POLL_INTERVAL_MS = 5000;
// Suggestion ranking scans the whole cache, so it refreshes on a slower
// cadence than the feed and stays off the feed's poll critical path.
const SUGGEST_INTERVAL_MS = 60000;

export function useTan() {
  const [posts, setPosts] = useState<TanPost[]>([]);
  const [following, setFollowing] = useState<TanFollow[]>([]);
  const [suggestions, setSuggestions] = useState<TanSuggestion[]>([]);
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

  // Suggestions have their own generation so a slow ranking response cannot
  // overwrite a newer one, and they never block the feed load.
  const suggestGenerationRef = useRef(0);
  const loadSuggestions = useCallback(async () => {
    const generation = ++suggestGenerationRef.current;
    const suggested = await apiGet<TanSuggestResponse>("/api/tan/suggest");
    if (suggestGenerationRef.current !== generation) return;
    setSuggestions(suggested.suggestions);
  }, []);

  useEffect(() => {
    let disposed = false;
    async function tickSuggest() {
      try {
        await loadSuggestions();
      } catch {
        // Suggestions are optional; keep the last good list on failure.
      }
      if (disposed) return;
    }
    void tickSuggest();
    const timer = setInterval(() => void tickSuggest(), SUGGEST_INTERVAL_MS);
    return () => {
      disposed = true;
      clearInterval(timer);
    };
  }, [loadSuggestions]);

  const refresh = useCallback(async () => {
    await load();
  }, [load]);

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
      void loadSuggestions().catch(() => undefined);
    },
    [load, loadSuggestions],
  );

  const unfollow = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/unfollow", { target });
      await load();
      void loadSuggestions().catch(() => undefined);
    },
    [load, loadSuggestions],
  );

  const like = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/like", { target });
      await load();
    },
    [load],
  );

  const unlike = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/unlike", { target });
      await load();
    },
    [load],
  );

  const deletePost = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/delete", { target });
      await load();
    },
    [load],
  );

  const sync = useCallback(async () => {
    setSyncing(true);
    try {
      const result = await apiPost<TanSyncResponse>("/api/tan/sync", {
        explore: true,
      });
      await load();
      void loadSuggestions().catch(() => undefined);
      return result.sync;
    } finally {
      setSyncing(false);
    }
  }, [load, loadSuggestions]);

  return {
    posts,
    following,
    suggestions,
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
