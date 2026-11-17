// Fresh Tan (social feed) data hook for the rewritten console. Same bridge
// endpoints as before, simpler client logic: poll the timeline every few
// seconds, refresh after writes.
"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { apiDownload, apiGet, apiPost, apiPostText } from "./api";
import type {
  TanAuthor,
  TanAuthorsResponse,
  TanCancelScheduledResponse,
  TanExportDocument,
  TanFollow,
  TanFollowedTag,
  TanFollowedTagsResponse,
  TanLikesResponse,
  TanMutedAuthor,
  TanMutedKeyword,
  TanMutedKeywordsResponse,
  TanMutedResponse,
  TanMutedTag,
  TanMutedTagsResponse,
  TanPinnedResponse,
  TanPollResponse,
  TanPost,
  TanPostResponse,
  TanQuotesResponse,
  TanRepostersResponse,
  TanRules,
  TanRulesResponse,
  TanRulesUpdateResponse,
  TanSavedResponse,
  TanScheduleResponse,
  TanScheduledPost,
  TanScheduledResponse,
  TanShareResponse,
  TanSuggestResponse,
  TanSuggestion,
  TanSyncResponse,
  TanTag,
  TanTagsResponse,
  TanTimelineResponse,
  TanVerifyReport,
} from "./types";

export const TAN_POLL_INTERVAL_MS = 5000;

export type TanView = "timeline" | "top" | "mentions";

function errorMessage(caught: unknown): string {
  return caught instanceof Error ? caught.message : "request failed";
}

export function useTan() {
  const [posts, setPosts] = useState<TanPost[]>([]);
  const [view, setView] = useState<TanView>("timeline");
  const [showAll, setShowAll] = useState(false);
  const [tagFilter, setTagFilter] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState<string | null>(null);
  const [searchResults, setSearchResults] = useState<TanPost[]>([]);
  const [mentions, setMentions] = useState<TanPost[]>([]);
  const [following, setFollowing] = useState<TanFollow[]>([]);
  const [mutedAuthors, setMutedAuthors] = useState<TanMutedAuthor[]>([]);
  const [mutedKeywords, setMutedKeywords] = useState<TanMutedKeyword[]>([]);
  const [followedTags, setFollowedTags] = useState<TanFollowedTag[]>([]);
  const [mutedTags, setMutedTags] = useState<TanMutedTag[]>([]);
  const [tags, setTags] = useState<TanTag[]>([]);
  const [suggestions, setSuggestions] = useState<TanSuggestion[]>([]);
  const [authors, setAuthors] = useState<TanAuthor[]>([]);
  const [threadRef, setThreadRef] = useState<string | null>(null);
  const [threadPosts, setThreadPosts] = useState<TanPost[]>([]);
  const [threadError, setThreadError] = useState<string | null>(null);
  const [scheduled, setScheduled] = useState<TanScheduledPost[]>([]);
  const [rules, setRules] = useState<TanRules | null>(null);
  const [availableKinds, setAvailableKinds] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [posting, setPosting] = useState(false);
  const [syncing, setSyncing] = useState(false);

  const generationRef = useRef(0);
  const viewRef = useRef(view);
  const showAllRef = useRef(showAll);
  const tagRef = useRef(tagFilter);
  const searchRef = useRef(searchQuery);
  const threadRefRef = useRef(threadRef);
  useEffect(() => {
    viewRef.current = view;
  }, [view]);
  useEffect(() => {
    showAllRef.current = showAll;
  }, [showAll]);
  useEffect(() => {
    tagRef.current = tagFilter;
  }, [tagFilter]);
  useEffect(() => {
    searchRef.current = searchQuery;
  }, [searchQuery]);
  useEffect(() => {
    threadRefRef.current = threadRef;
  }, [threadRef]);

  const load = useCallback(async () => {
    const generation = ++generationRef.current;
    try {
      const activeSearch = searchRef.current;
      const activeTag = tagRef.current;
      const params = new URLSearchParams({ limit: "100" });
      if (showAllRef.current) params.set("all", "1");
      if (activeTag !== null) params.set("tag", activeTag);
      const [feed, follows, muted, mutedKw, followed, mutedTg] =
        await Promise.all([
          activeSearch !== null
            ? apiGet<TanTimelineResponse>(
                `/api/tan/search?q=${encodeURIComponent(activeSearch)}&limit=100`,
              )
            : viewRef.current === "top"
              ? apiGet<TanTimelineResponse>("/api/tan/top?limit=10")
              : viewRef.current === "mentions"
                ? apiGet<TanTimelineResponse>("/api/tan/mentions?limit=100")
                : apiGet<TanTimelineResponse>(
                    `/api/tan/timeline?${params.toString()}`,
                  ),
          apiGet<{ following: TanFollow[]; count: number }>(
            "/api/tan/following",
          ),
          apiGet<TanMutedResponse>("/api/tan/muted").catch(() => null),
          apiGet<TanMutedKeywordsResponse>("/api/tan/muted-keywords").catch(
            () => null,
          ),
          apiGet<TanFollowedTagsResponse>("/api/tan/followed-tags").catch(
            () => null,
          ),
          apiGet<TanMutedTagsResponse>("/api/tan/muted-tags").catch(
            () => null,
          ),
        ]);
      if (generationRef.current !== generation) return;
      if (activeSearch !== null) {
        setSearchResults(feed.posts);
      } else if (viewRef.current === "mentions") {
        setMentions(feed.posts);
      } else {
        setPosts(feed.posts);
      }
      setFollowing(follows.following ?? []);
      if (muted !== null) setMutedAuthors(muted.muted ?? []);
      if (mutedKw !== null) setMutedKeywords(mutedKw.muted_keywords ?? []);
      if (followed !== null) setFollowedTags(followed.followed_tags ?? []);
      if (mutedTg !== null) setMutedTags(mutedTg.muted_tags ?? []);
      setError(null);
      const ref = threadRefRef.current;
      if (ref !== null) {
        try {
          const thread = await apiGet<TanTimelineResponse>(
            `/api/tan/thread?ref=${encodeURIComponent(ref)}`,
          );
          if (generationRef.current !== generation) return;
          setThreadPosts(thread.posts);
          setThreadError(null);
        } catch (caught) {
          if (generationRef.current !== generation) return;
          if (caught instanceof Error && caught.message.includes("400")) {
            setThreadRef(null);
            setThreadPosts([]);
          }
          setThreadError(errorMessage(caught));
        }
      }
    } catch (caught) {
      if (generationRef.current !== generation) return;
      setError(errorMessage(caught));
    } finally {
      if (generationRef.current === generation) setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
    const timer = setInterval(() => void load(), TAN_POLL_INTERVAL_MS);
    return () => clearInterval(timer);
  }, [load]);

  const refreshMeta = useCallback(async () => {
    const [tagList, suggest, authorList, ruleList, sched] = await Promise.all([
      apiGet<TanTagsResponse>("/api/tan/tags").catch(() => null),
      apiGet<TanSuggestResponse>("/api/tan/suggest").catch(() => null),
      apiGet<TanAuthorsResponse>("/api/tan/authors?limit=50").catch(() => null),
      apiGet<TanRulesResponse>("/api/tan/rules").catch(() => null),
      apiGet<TanScheduledResponse>("/api/tan/scheduled").catch(() => null),
    ]);
    if (tagList) setTags(tagList.tags ?? []);
    if (suggest) setSuggestions(suggest.suggestions ?? []);
    if (authorList) setAuthors(authorList.authors ?? []);
    if (ruleList) {
      setRules(ruleList.rules);
      setAvailableKinds(ruleList.available_kinds ?? []);
    }
    if (sched) setScheduled(sched.scheduled ?? []);
  }, []);

  useEffect(() => {
    void refreshMeta();
  }, [refreshMeta]);

  const refresh = useCallback(async () => {
    await load();
    await refreshMeta();
  }, [load, refreshMeta]);

  const publish = useCallback(
    async (options: {
      text: string;
      replyTo?: string;
      expiresIn?: number;
      image?: { sha256: string; size: number; mime: string; name: string };
    }) => {
      setPosting(true);
      try {
        const body: Record<string, unknown> = { text: options.text };
        if (options.replyTo) body.reply_to = options.replyTo;
        if (options.expiresIn !== undefined) body.expires_in = options.expiresIn;
        if (options.image) body.image = options.image;
        const result = await apiPost<TanPostResponse>("/api/tan/posts", body);
        await load();
        return result.post;
      } finally {
        setPosting(false);
      }
    },
    [load],
  );

  const publishPoll = useCallback(
    async (question: string, options: string[], expiresIn?: number) => {
      setPosting(true);
      try {
        const body: Record<string, unknown> = {
          text: question,
          options,
        };
        if (expiresIn !== undefined) body.expires_in = expiresIn;
        const result = await apiPost<TanPollResponse>("/api/tan/poll", body);
        await load();
        return result.poll;
      } finally {
        setPosting(false);
      }
    },
    [load],
  );

  const quote = useCallback(
    async (target: string, text: string) => {
      const result = await apiPost<TanPostResponse>("/api/tan/quote", {
        target,
        text,
      });
      await load();
      return result.post;
    },
    [load],
  );

  const like = useCallback(
    async (target: string, liked: boolean) => {
      await apiPost<{ ok: boolean }>(
        liked ? "/api/tan/like" : "/api/tan/unlike",
        { target },
      );
      await load();
    },
    [load],
  );

  const repost = useCallback(
    async (target: string) => {
      const result = await apiPost<TanPostResponse>("/api/tan/repost", {
        target,
      });
      await load();
      return result.post;
    },
    [load],
  );

  const share = useCallback(
    async (target: string, to?: string, group?: string) => {
      const result = await apiPost<TanShareResponse>("/api/tan/share", {
        target,
        ...(to ? { to } : {}),
        ...(group ? { group } : {}),
      });
      return result;
    },
    [],
  );

  const vote = useCallback(
    async (target: string, option: number) => {
      await apiPost<{ ok: boolean }>("/api/tan/vote", { target, option });
      await load();
    },
    [load],
  );

  const deletePost = useCallback(
    async (target: string) => {
      await apiPost<{ ok: boolean }>("/api/tan/delete", { target });
      await load();
    },
    [load],
  );

  const editPost = useCallback(
    async (target: string, text: string) => {
      await apiPost<{ ok: boolean }>("/api/tan/edit", { target, text });
      await load();
    },
    [load],
  );

  const follow = useCallback(
    async (target: string, followed: boolean) => {
      await apiPost<{ ok: boolean }>(
        followed ? "/api/tan/follow" : "/api/tan/unfollow",
        { target },
      );
      await load();
    },
    [load],
  );

  const muteAuthor = useCallback(
    async (target: string, muted: boolean) => {
      await apiPost<{ ok: boolean }>(
        muted ? "/api/tan/mute" : "/api/tan/unmute",
        { target },
      );
      await load();
    },
    [load],
  );

  const muteKeyword = useCallback(
    async (keyword: string, muted: boolean) => {
      await apiPost<{ ok: boolean }>(
        muted ? "/api/tan/mute-keyword" : "/api/tan/unmute-keyword",
        { keyword },
      );
      await load();
    },
    [load],
  );

  const followTag = useCallback(
    async (tag: string, followed: boolean) => {
      await apiPost<{ ok: boolean }>(
        followed ? "/api/tan/follow-tag" : "/api/tan/unfollow-tag",
        { tag },
      );
      await load();
    },
    [load],
  );

  const muteTag = useCallback(
    async (tag: string, muted: boolean) => {
      await apiPost<{ ok: boolean }>(
        muted ? "/api/tan/mute-tag" : "/api/tan/unmute-tag",
        { tag },
      );
      await load();
    },
    [load],
  );

  const savePost = useCallback(
    async (target: string, saved: boolean) => {
      await apiPost<{ ok: boolean }>(
        saved ? "/api/tan/save" : "/api/tan/unsave",
        { target },
      );
      await load();
    },
    [load],
  );

  const hidePost = useCallback(
    async (target: string, hidden: boolean) => {
      await apiPost<{ ok: boolean }>(
        hidden ? "/api/tan/hide" : "/api/tan/unhide",
        { target },
      );
      await load();
    },
    [load],
  );

  const pinPost = useCallback(
    async (target: string, pinned: boolean) => {
      await apiPost<{ ok: boolean }>(
        pinned ? "/api/tan/pin" : "/api/tan/unpin",
        { target },
      );
      await load();
    },
    [load],
  );

  const schedulePost = useCallback(
    async (options: {
      text: string;
      publishAt: number;
      replyTo?: string | null;
      to?: string[];
      ttl?: number;
      expiresIn?: number;
    }) => {
      const result = await apiPost<{ ok: boolean; scheduled: TanScheduledPost }>(
        "/api/tan/schedule",
        {
          text: options.text,
          publish_at: options.publishAt,
          reply_to: options.replyTo ?? null,
          to: options.to ?? [],
          ttl: options.ttl ?? 0,
          expires_in: options.expiresIn ?? 0,
        },
      );
      await refreshMeta();
      return result.scheduled;
    },
    [refreshMeta],
  );

  const cancelScheduled = useCallback(
    async (id: string) => {
      await apiPost<{ ok: boolean }>("/api/tan/cancel-scheduled", { id });
      await refreshMeta();
    },
    [refreshMeta],
  );

  const updateRules = useCallback(
    async (next: TanRules) => {
      const result = await apiPost<TanRulesUpdateResponse>("/api/tan/rules", {
        ...next,
      });
      setRules(result.rules);
      return result.rules;
    },
    [],
  );

  const sync = useCallback(async () => {
    setSyncing(true);
    try {
      const result = await apiPost<TanSyncResponse>("/api/tan/sync", {});
      await load();
      return result.sync;
    } finally {
      setSyncing(false);
    }
  }, [load]);

  const exportTan = useCallback(async (allAuthors: boolean) => {
    const doc = await apiGet<TanExportDocument>(
      `/api/tan/export?scope=${allAuthors ? "all" : "following"}`,
    );
    const blob = new Blob([JSON.stringify(doc)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = "tau-tan-export.json";
    anchor.click();
    URL.revokeObjectURL(url);
  }, []);

  const importTan = useCallback(
    async (document: string) => {
      const result = await apiPostText<{ ok: boolean }>("/api/tan/import", document);
      await load();
      return result;
    },
    [load],
  );

  const fetchLikers = useCallback(async (target: string) => {
    const result = await apiGet<{ likers: { author: string; name: string; liked_at: number }[]; count: number; total: number }>(
      `/api/tan/likes?target=${encodeURIComponent(target)}`,
    );
    return result;
  }, []);

  const fetchReposters = useCallback(async (target: string) => {
    return apiGet<{
      reposters: { author: string; name: string; reposted_at: number }[];
      count: number;
      total: number;
    }>(`/api/tan/reposts?target=${encodeURIComponent(target)}`);
  }, []);

  const fetchQuoters = useCallback(async (target: string) => {
    return apiGet<{
      quoters: { author: string; name: string; hash: string; text: string; quoted_at: number }[];
      count: number;
      total: number;
    }>(`/api/tan/quotes?target=${encodeURIComponent(target)}`);
  }, []);

  const fetchSaved = useCallback(async (offset = 0) => {
    return apiGet<{ posts: TanPost[]; count: number; next_offset: number | null }>(
      `/api/tan/saved?offset=${offset}`,
    );
  }, []);

  const fetchHidden = useCallback(async (offset = 0) => {
    return apiGet<{ posts: TanPost[]; count: number; next_offset: number | null }>(
      `/api/tan/hidden?offset=${offset}`,
    );
  }, []);

  const fetchPinned = useCallback(async (offset = 0) => {
    return apiGet<{ posts: TanPost[]; count: number; next_offset: number | null }>(
      `/api/tan/pinned?offset=${offset}`,
    );
  }, []);

  const verify = useCallback(async () => {
    return apiGet<TanVerifyReport>("/api/tan/verify");
  }, []);

  const openThread = useCallback(
    (ref: string | null) => {
      setThreadRef(ref);
      if (ref === null) {
        setThreadPosts([]);
        setThreadError(null);
      } else {
        void load();
      }
    },
    [load],
  );

  return {
    posts,
    view,
    setView,
    showAll,
    setShowAll,
    tagFilter,
    setTagFilter,
    searchQuery,
    setSearchQuery,
    searchResults,
    mentions,
    following,
    mutedAuthors,
    mutedKeywords,
    followedTags,
    mutedTags,
    tags,
    suggestions,
    authors,
    threadRef,
    threadPosts,
    threadError,
    scheduled,
    rules,
    availableKinds,
    loading,
    error,
    posting,
    syncing,
    refresh,
    load,
    publish,
    publishPoll,
    quote,
    like,
    repost,
    share,
    vote,
    deletePost,
    editPost,
    follow,
    muteAuthor,
    muteKeyword,
    followTag,
    muteTag,
    savePost,
    hidePost,
    pinPost,
    schedulePost,
    cancelScheduled,
    updateRules,
    sync,
    exportTan,
    importTan,
    fetchLikers,
    fetchReposters,
    fetchQuoters,
    fetchSaved,
    fetchHidden,
    fetchPinned,
    verify,
    openThread,
  };
}

export type UseTanResult = ReturnType<typeof useTan>;
