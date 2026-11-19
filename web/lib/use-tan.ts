// use-tan.ts — Tan social feed state for the Social tab.
//
// Provides:
//   useTanFeed(opts)   — timeline (Following / All / author / tag), top posts,
//                        mentions, search; like/unlike, repost, quote, vote,
//                        save/hide/pin, follow/unfollow, mute, tags.
//   useTanThread(ref)  — thread view rooted at one post.
//   useTanComposer()   — publish / schedule / poll composer actions.
//   useTanLists()      — saved / hidden / pinned / scheduled / following /
//                        followers / muted lists and tag follows.
//
// Posts arrive from /api/tan/timeline (following by default, `all=1` for
// everything cached) with bridge annotations: likes/liked/own, quotes,
// poll tally, saved, pinned. A live `post` event on /ws carries new posts,
// but this hook polls the timeline on an interval (default 30s) and after
// every mutation, so it stays correct even when the socket is down. Pass
// `onNewPost` handling to useTau's snapshot if you need instant updates.

"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  createTanPoll,
  deleteTanPost,
  editTanPost,
  followTanAuthor,
  followTanTag,
  getTanAuthors,
  getTanFollowers,
  getTanFollowing,
  getTanHidden,
  getTanLikers,
  getTanMentions,
  getTanMuted,
  getTanMutedKeywords,
  getTanMutedTags,
  getTanPinned,
  getTanQuoters,
  getTanReposters,
  getTanSaved,
  getTanScheduled,
  getTanSuggestions,
  getTanTags,
  getTanThread,
  getTanTimeline,
  getTanTop,
  hideTanPost,
  likeTanPost,
  muteTanAuthor,
  muteTanKeyword,
  muteTanTag,
  pinTanPost,
  publishTanPost,
  quoteTanPost,
  repostTanPost,
  saveTanPost,
  scheduleTanPost,
  searchTan,
  shareTanPost,
  unfollowTanAuthor,
  unfollowTanTag,
  unhideTanPost,
  unlikeTanPost,
  unmuteTanAuthor,
  unmuteTanKeyword,
  unmuteTanTag,
  unpinTanPost,
  unsaveTanPost,
  voteTanPoll,
} from "./api";
import type {
  ScheduledTanPost,
  TanAuthor,
  TanPost,
  TanReactors,
  TanTag,
} from "./types";

export type TanFeedMode = "following" | "all" | "top" | "mentions";

export interface UseTanFeedOptions {
  mode?: TanFeedMode;
  author?: string;
  tag?: string;
  limit?: number;
  /** Poll interval in ms; 0 disables polling. Default 30s. */
  pollMs?: number;
}

export interface UseTanFeedResult {
  posts: TanPost[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
  like: (target: string) => Promise<void>;
  unlike: (target: string) => Promise<void>;
  repost: (target: string) => Promise<void>;
  quote: (target: string, text: string) => Promise<TanPost>;
  vote: (target: string, option: number) => Promise<void>;
  share: (target: string, dest: { to: string } | { group: string }) => Promise<void>;
  save: (target: string) => Promise<void>;
  unsave: (target: string) => Promise<void>;
  hide: (target: string) => Promise<void>;
  pin: (target: string) => Promise<void>;
  unpin: (target: string) => Promise<void>;
  deletePost: (target: string) => Promise<void>;
  editPost: (target: string, text: string) => Promise<void>;
  follow: (target: string) => Promise<void>;
  unfollow: (target: string) => Promise<void>;
  muteAuthor: (target: string) => Promise<void>;
  search: (q: string) => Promise<TanPost[]>;
}

export function useTanFeed(opts: UseTanFeedOptions = {}): UseTanFeedResult {
  const { mode = "following", author, tag, limit = 30, pollMs = 30_000 } = opts;
  const [posts, setPosts] = useState<TanPost[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const mounted = useRef(true);

  const load = useCallback(async () => {
    try {
      let next: TanPost[];
      if (mode === "top") next = await getTanTop(limit, author);
      else if (mode === "mentions") next = await getTanMentions(limit);
      else next = await getTanTimeline({ limit, all: mode === "all", author, tag });
      if (!mounted.current) return;
      setPosts(next);
      setError(null);
    } catch (err) {
      if (!mounted.current) return;
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      if (mounted.current) setLoading(false);
    }
  }, [mode, author, tag, limit]);

  useEffect(() => {
    mounted.current = true;
    setLoading(true);
    void load();
    if (pollMs > 0) {
      const id = setInterval(() => void load(), pollMs);
      return () => {
        mounted.current = false;
        clearInterval(id);
      };
    }
    return () => {
      mounted.current = false;
    };
  }, [load, pollMs]);

  const mutate = useCallback(
    async (fn: () => Promise<unknown>) => {
      await fn();
      await load();
    },
    [load],
  );

  return {
    posts,
    loading,
    error,
    refresh: load,
    like: (target) => mutate(() => likeTanPost(target)),
    unlike: (target) => mutate(() => unlikeTanPost(target)),
    repost: (target) => mutate(() => repostTanPost(target)),
    quote: async (target, text) => {
      const post = await quoteTanPost(target, text);
      await load();
      return post;
    },
    vote: (target, option) => mutate(() => voteTanPoll(target, option)),
    share: (target, dest) => mutate(() => shareTanPost(target, dest)),
    save: (target) => mutate(() => saveTanPost(target)),
    unsave: (target) => mutate(() => unsaveTanPost(target)),
    hide: (target) => mutate(() => hideTanPost(target)),
    pin: (target) => mutate(() => pinTanPost(target)),
    unpin: (target) => mutate(() => unpinTanPost(target)),
    deletePost: (target) => mutate(() => deleteTanPost(target)),
    editPost: (target, text) => mutate(() => editTanPost(target, text)),
    follow: (target) => mutate(() => followTanAuthor(target)),
    unfollow: (target) => mutate(() => unfollowTanAuthor(target)),
    muteAuthor: (target) => mutate(() => muteTanAuthor(target)),
    search: (q) => searchTan(q, { limit }),
  };
}

// --------------------------------------------------------------- thread ---

export interface UseTanThreadResult {
  posts: TanPost[];
  loading: boolean;
  error: string | null;
  root: TanPost | null;
  refresh: () => Promise<void>;
  likers: (target: string) => Promise<TanReactors>;
  reposters: (target: string) => Promise<TanReactors>;
  quoters: (target: string) => Promise<TanReactors>;
}

/** Thread rooted at `ref` (post hash or prefix), oldest first. */
export function useTanThread(ref: string | null): UseTanThreadResult {
  const [posts, setPosts] = useState<TanPost[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (!ref) {
      setPosts([]);
      return;
    }
    setLoading(true);
    try {
      setPosts(await getTanThread(ref));
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [ref]);

  useEffect(() => {
    void load();
  }, [load]);

  const root = useMemo(() => posts.find((p) => p.hash.startsWith(ref ?? "")) ?? posts[0] ?? null, [posts, ref]);
  return {
    posts,
    loading,
    error,
    root,
    refresh: load,
    likers: (target) => getTanLikers(target),
    reposters: (target) => getTanReposters(target),
    quoters: (target) => getTanQuoters(target),
  };
}

// ------------------------------------------------------------- composer ---

export interface UseTanComposerResult {
  publish: (text: string, opts?: { replyTo?: string; expiresIn?: number }) => Promise<TanPost>;
  schedule: (text: string, sendIn: number, opts?: { replyTo?: string; expiresIn?: number }) => Promise<ScheduledTanPost>;
  poll: (question: string, options: string[], expiresIn?: number) => Promise<TanPost>;
  busy: boolean;
  error: string | null;
}

/** Composer actions: publish now, schedule, or ask a poll question. */
export function useTanComposer(): UseTanComposerResult {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const run = useCallback(async <T,>(fn: () => Promise<T>): Promise<T> => {
    setBusy(true);
    setError(null);
    try {
      return await fn();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      throw err;
    } finally {
      setBusy(false);
    }
  }, []);

  return {
    publish: (text, opts) =>
      run(() => publishTanPost(text, { replyTo: opts?.replyTo, expiresIn: opts?.expiresIn })),
    schedule: (text, sendIn, opts) =>
      run(() => scheduleTanPost(text, sendIn, { replyTo: opts?.replyTo, expiresIn: opts?.expiresIn })),
    poll: (question, options, expiresIn) => run(() => createTanPoll(question, options, expiresIn)),
    busy,
    error,
  };
}

// ----------------------------------------------------------------- lists --

export interface UseTanListsResult {
  tags: TanTag[];
  following: TanAuthor[];
  followers: TanAuthor[];
  suggestions: TanAuthor[];
  saved: TanPost[];
  hidden: TanPost[];
  pinned: TanPost[];
  scheduled: ScheduledTanPost[];
  mutedAuthors: TanAuthor[];
  mutedKeywords: string[];
  mutedTags: string[];
  loading: boolean;
  refresh: () => Promise<void>;
  followTag: (tag: string) => Promise<void>;
  unfollowTag: (tag: string) => Promise<void>;
  muteKeyword: (keyword: string) => Promise<void>;
  unmuteKeyword: (keyword: string) => Promise<void>;
  muteTag: (tag: string) => Promise<void>;
  unmuteTag: (tag: string) => Promise<void>;
  unmuteAuthor: (target: string) => Promise<void>;
  unhide: (target: string) => Promise<void>;
  authors: (sort?: "recent" | "posts" | "name", limit?: number, offset?: number) => Promise<{ authors: TanAuthor[]; total: number }>;
}

/** Discover + library lists: tags, following, followers, suggestions, saved/hidden/pinned. */
export function useTanLists(): UseTanListsResult {
  const [tags, setTags] = useState<TanTag[]>([]);
  const [following, setFollowing] = useState<TanAuthor[]>([]);
  const [followers, setFollowers] = useState<TanAuthor[]>([]);
  const [suggestions, setSuggestions] = useState<TanAuthor[]>([]);
  const [saved, setSaved] = useState<TanPost[]>([]);
  const [hidden, setHidden] = useState<TanPost[]>([]);
  const [pinned, setPinned] = useState<TanPost[]>([]);
  const [scheduled, setScheduled] = useState<ScheduledTanPost[]>([]);
  const [mutedAuthors, setMutedAuthors] = useState<TanAuthor[]>([]);
  const [mutedKeywords, setMutedKeywords] = useState<string[]>([]);
  const [mutedTags, setMutedTags] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    try {
      const [t, f, fw, s, sv, h, p, sc, ma, mk, mt] = await Promise.all([
        getTanTags(),
        getTanFollowing(),
        getTanFollowers(),
        getTanSuggestions(),
        getTanSaved(),
        getTanHidden(),
        getTanPinned(),
        getTanScheduled(),
        getTanMuted(),
        getTanMutedKeywords(),
        getTanMutedTags(),
      ]);
      setTags(t);
      setFollowing(f);
      setFollowers(fw);
      setSuggestions(s);
      setSaved(sv.posts);
      setHidden(h.posts);
      setPinned(p.posts);
      setScheduled(sc);
      setMutedAuthors(ma);
      setMutedKeywords(mk);
      setMutedTags(mt);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const mutate = useCallback(
    async (fn: () => Promise<unknown>) => {
      await fn();
      await refresh();
    },
    [refresh],
  );

  return {
    tags,
    following,
    followers,
    suggestions,
    saved,
    hidden,
    pinned,
    scheduled,
    mutedAuthors,
    mutedKeywords,
    mutedTags,
    loading,
    refresh,
    followTag: (tag) => mutate(() => followTanTag(tag)),
    unfollowTag: (tag) => mutate(() => unfollowTanTag(tag)),
    muteKeyword: (keyword) => mutate(() => muteTanKeyword(keyword)),
    unmuteKeyword: (keyword) => mutate(() => unmuteTanKeyword(keyword)),
    muteTag: (tag) => mutate(() => muteTanTag(tag)),
    unmuteTag: (tag) => mutate(() => unmuteTanTag(tag)),
    unmuteAuthor: (target) => mutate(() => unmuteTanAuthor(target)),
    unhide: (target) => mutate(() => unhideTanPost(target)),
    authors: async (sort = "recent", limit = 50, offset = 0) => {
      const data = await getTanAuthors({ sort, limit, offset });
      return { authors: data.authors, total: data.total };
    },
  };
}
