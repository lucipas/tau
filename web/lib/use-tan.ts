"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { apiGet, apiPost } from "./api";
import type {
  TanFollow,
  TanFollowingResponse,
  TanPost,
  TanPostResponse,
  TanRules,
  TanRulesResponse,
  TanRulesUpdateResponse,
  TanSyncResponse,
  TanTimelineResponse,
} from "./types";

const POLL_INTERVAL_MS = 5000;

export function useTan() {
  const [posts, setPosts] = useState<TanPost[]>([]);
  const [following, setFollowing] = useState<TanFollow[]>([]);
  const [all, setAll] = useState(false);
  const [loading, setLoading] = useState(true);
  const [posting, setPosting] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [rules, setRules] = useState<TanRules | null>(null);
  const [availableKinds, setAvailableKinds] = useState<string[]>([]);
  const [rulesLoading, setRulesLoading] = useState(true);
  const [rulesError, setRulesError] = useState<string | null>(null);
  // Generation of rules requests; a response from an older request, or
  // one superseded by a confirmed save, is discarded.
  const rulesGenerationRef = useRef(0);

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

  // Admission rules change rarely, so they load once and on demand
  // rather than on the feed's poll: a rules edit on the CLI shows up
  // after a manual refresh (or a page reload), not mid-scroll.
  const refreshRules = useCallback(async () => {
    // A newer rules request (a refresh or a confirmed save) supersedes
    // this one, so a slow GET cannot overwrite a newer policy.
    const generation = ++rulesGenerationRef.current;
    try {
      const result = await apiGet<TanRulesResponse>("/api/tan/rules");
      if (generation !== rulesGenerationRef.current) return;
      if (!result || typeof result.rules !== "object" || result.rules === null) {
        throw new Error("could not load rules");
      }
      setRules(result.rules);
      setAvailableKinds(
        Array.isArray(result.available_kinds) ? result.available_kinds : [],
      );
      setRulesError(null);
    } catch (caught) {
      if (generation !== rulesGenerationRef.current) return;
      setRulesError(
        caught instanceof Error ? caught.message : "could not load rules",
      );
    } finally {
      if (generation === rulesGenerationRef.current) {
        setRulesLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    void refreshRules();
  }, [refreshRules]);

  const updateRules = useCallback(
    async (next: TanRules) => {
      const result = await apiPost<TanRulesUpdateResponse>(
        "/api/tan/rules",
        // apiPost takes a plain JSON object; a TanRules is exactly that.
        { ...next } as Record<string, unknown>,
      );
      // The save is confirmed: supersede any GET still in flight so it
      // cannot restore the policy from before the save.
      rulesGenerationRef.current += 1;
      setRules(result.rules);
      setRulesError(null);
      setRulesLoading(false);
      // A kinds change re-filters the timeline; reload it at once. A
      // failed feed reload is reported as a feed error, not as a failed
      // rules save, since the policy above is already saved.
      try {
        await load();
      } catch (caught) {
        setError(
          caught instanceof Error ? caught.message : "could not load the feed",
        );
      }
      return result.rules;
    },
    [load],
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
      return result.sync;
    } finally {
      setSyncing(false);
    }
  }, [load]);

  return {
    posts,
    following,
    all,
    setAll,
    loading,
    posting,
    syncing,
    error,
    rules,
    availableKinds,
    rulesLoading,
    rulesError,
    refresh,
    refreshRules,
    updateRules,
    publish,
    follow,
    unfollow,
    like,
    unlike,
    deletePost,
    sync,
  };
}
