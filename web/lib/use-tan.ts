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

const POLL_INTERVAL_MS = 5000;

export function useTan() {
  const [posts, setPosts] = useState<TanPost[]>([]);
  const [following, setFollowing] = useState<TanFollow[]>([]);
  const [all, setAll] = useState(false);
  const [loading, setLoading] = useState(true);
  const [posting, setPosting] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // The thread currently open in the console, if any: the reference it
  // was opened with and its entries (null while none is open).
  const [threadPosts, setThreadPosts] = useState<TanPost[] | null>(null);
  const [threadRef, setThreadRef] = useState<string | null>(null);
  const [threadLoading, setThreadLoading] = useState(false);
  const [threadError, setThreadError] = useState<string | null>(null);

  // The filter the timeline is showing right now. Actions refresh through
  // this ref instead of a captured `all`, so a filter switch mid-action
  // does not reload the filter that is no longer selected.
  const allRef = useRef(all);
  useEffect(() => {
    allRef.current = all;
  }, [all]);

  // The open thread's reference, kept alongside `threadRef` so a feed
  // load can reload the thread without waiting for the state update.
  // Set directly in `openThread`/`closeThread` for the same reason.
  const openThreadRef = useRef<string | null>(null);

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
      // An open thread reloads with the feed, so a new reply (or a
      // like, or a delete) shows without reopening the thread. A
      // failure here must not fail the feed load: a 400 means the
      // anchor itself is gone (its root was deleted), so the thread
      // closes and the timeline takes over; any other failure keeps
      // the previous entries and reports the error instead.
      const ref = openThreadRef.current;
      if (ref !== null) {
        try {
          const thread = await apiGet<TanTimelineResponse>(
            `/api/tan/thread?ref=${encodeURIComponent(ref)}`,
          );
          if (generationRef.current !== generation) return false;
          setThreadPosts(thread.posts);
          setThreadError(null);
        } catch (caught) {
          if (generationRef.current !== generation) return false;
          const status = (caught as { status?: unknown } | null)?.status;
          if (status === 400) {
            openThreadRef.current = null;
            setThreadRef(null);
            setThreadPosts(null);
            setThreadError(null);
          } else {
            setThreadError(
              caught instanceof Error
                ? caught.message
                : "could not load the thread",
            );
          }
        } finally {
          if (generationRef.current === generation) setThreadLoading(false);
        }
      }
      return true;
    } finally {
      if (pendingRef.current?.generation === generation) {
        pendingRef.current = null;
      }
      // A feed load that fails before reaching the thread (or is the
      // load that opened it) must not leave the thread spinner stuck;
      // a superseded load leaves the flag to the load that replaced it.
      if (generationRef.current === generation) {
        setThreadLoading(false);
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

  const openThread = useCallback(
    async (ref: string) => {
      // The ref goes in the ref first: `load` reads it synchronously,
      // and the state update alone would arrive too late.
      openThreadRef.current = ref;
      setThreadRef(ref);
      setThreadPosts(null);
      setThreadLoading(true);
      setThreadError(null);
      try {
        await load();
      } catch (caught) {
        // The feed load failed before the thread request ran, so the
        // thread has no entries and no error of its own yet. Report
        // the failure instead of leaving a thread view that claims
        // to be empty. (A thread-fetch failure is caught inside
        // `load` and never reaches this path.)
        if (openThreadRef.current === ref) {
          setThreadError(
            caught instanceof Error
              ? caught.message
              : "could not load the thread",
          );
        }
      }
    },
    [load],
  );

  const closeThread = useCallback(() => {
    openThreadRef.current = null;
    setThreadRef(null);
    setThreadPosts(null);
    setThreadLoading(false);
    setThreadError(null);
  }, []);

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
    threadPosts,
    threadRef,
    threadLoading,
    threadError,
    openThread,
    closeThread,
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
