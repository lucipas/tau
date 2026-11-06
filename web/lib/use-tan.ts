"use client";

import { useCallback, useEffect, useRef, useState } from "react";

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

  // The filter the timeline is showing right now. Actions refresh through
  // this ref instead of a captured `all`, so a filter switch mid-action
  // does not reload the filter that is no longer selected.
  const allRef = useRef(all);
  useEffect(() => {
    allRef.current = all;
  }, [all]);

  // Monotonic generation of feed loads. A slow response (for example a
  // poll tick started before a delete) must not overwrite a newer refresh.
  const generationRef = useRef(0);

  const load = useCallback(async () => {
    const generation = ++generationRef.current;
    const suffix = allRef.current ? "?all=1&limit=100" : "?limit=100";
    const [timeline, follows] = await Promise.all([
      apiGet<TanTimelineResponse>(`/api/tan/timeline${suffix}`),
      apiGet<TanFollowingResponse>("/api/tan/following"),
    ]);
    if (generationRef.current !== generation) return;
    setPosts(timeline.posts);
    setFollowing(follows.following);
    setError(null);
  }, []);

  useEffect(() => {
    let disposed = false;
    async function tick() {
      try {
        await load();
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
    await load();
  }, [load]);

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
