"use client";

import { useCallback, useEffect, useState } from "react";

import { apiGet, apiPost } from "./api";
import type {
  TanFollow,
  TanFollowingResponse,
  TanPost,
  TanPostResponse,
  TanSyncResponse,
  TanTimelineResponse,
} from "./types";

export function useTan() {
  const [posts, setPosts] = useState<TanPost[]>([]);
  const [following, setFollowing] = useState<TanFollow[]>([]);
  const [all, setAll] = useState(false);
  const [loading, setLoading] = useState(true);
  const [posting, setPosting] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async (includeAll: boolean) => {
    const suffix = includeAll ? "?all=1&limit=100" : "?limit=100";
    const [timeline, follows] = await Promise.all([
      apiGet<TanTimelineResponse>(`/api/tan/timeline${suffix}`),
      apiGet<TanFollowingResponse>("/api/tan/following"),
    ]);
    setPosts(timeline.posts);
    setFollowing(follows.following);
    setError(null);
  }, []);

  useEffect(() => {
    let disposed = false;
    async function tick() {
      try {
        await load(all);
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
    void tick();
    const timer = setInterval(() => void tick(), 5000);
    return () => {
      disposed = true;
      clearInterval(timer);
    };
  }, [all, load]);

  const refresh = useCallback(async () => {
    await load(all);
  }, [all, load]);

  const publish = useCallback(
    async (text: string) => {
      setPosting(true);
      try {
        const result = await apiPost<TanPostResponse>("/api/tan/posts", { text });
        setPosts((previous) => {
          if (previous.some((post) => post.hash === result.post.hash)) {
            return previous;
          }
          return [result.post, ...previous];
        });
        await load(all);
        return result.post;
      } finally {
        setPosting(false);
      }
    },
    [all, load],
  );

  const follow = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/follow", { target });
      await load(all);
    },
    [all, load],
  );

  const unfollow = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/unfollow", { target });
      await load(all);
    },
    [all, load],
  );

  const sync = useCallback(async () => {
    setSyncing(true);
    try {
      const result = await apiPost<TanSyncResponse>("/api/tan/sync", {
        explore: true,
      });
      await load(all);
      return result.sync;
    } finally {
      setSyncing(false);
    }
  }, [all, load]);

  return {
    posts,
    following,
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
    sync,
  };
}
