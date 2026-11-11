"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { apiGet, apiPost } from "./api";
import type {
  TanExportDocument,
  TanFollow,
  TanFollowingResponse,
  TanImportResponse,
  TanImportResult,
  TanLikesResponse,
  TanMutedAuthor,
  TanMutedKeyword,
  TanMutedKeywordsResponse,
  TanMutedResponse,
  TanPost,
  TanPostResponse,
  TanSavedResponse,
  TanSuggestResponse,
  TanSuggestion,
  TanSyncResponse,
  TanTimelineResponse,
} from "./types";

const POLL_INTERVAL_MS = 5000;
// Suggestion ranking scans the whole cache, so it refreshes on a slower
// cadence than the feed and stays off the feed's poll critical path.
const SUGGEST_INTERVAL_MS = 60000;
const SAVED_PAGE_SIZE = 100;

export function useTan() {
  const [posts, setPosts] = useState<TanPost[]>([]);
  const [searchResults, setSearchResults] = useState<TanPost[]>([]);
  // The search the feed is showing, or null for the timeline. Kept beside
  // `all` so a search mid-action reloads the search, the same way `all`
  // works — a like or delete on a search result refreshes the results.
  const [searchQuery, setSearchQuery] = useState<string | null>(null);
  const [mentions, setMentions] = useState<TanPost[]>([]);
  const [following, setFollowing] = useState<TanFollow[]>([]);
  const [suggestions, setSuggestions] = useState<TanSuggestion[]>([]);
  const [mutedAuthors, setMutedAuthors] = useState<TanMutedAuthor[]>([]);
  const [mutedKeywords, setMutedKeywords] = useState<TanMutedKeyword[]>([]);
  const [saved, setSaved] = useState<TanPost[]>([]);
  const [savedLoading, setSavedLoading] = useState(false);
  const [savedHasMore, setSavedHasMore] = useState(false);
  const [all, setAll] = useState(false);
  // Whether the feed shows replies to your own posts instead of the
  // timeline. Kept beside `all` so a view switch mid-action reloads
  // the view that is actually selected, the same way `all` works.
  const [showMentions, setShowMentions] = useState(false);
  const [loading, setLoading] = useState(true);
  const [posting, setPosting] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [importing, setImporting] = useState(false);
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

  const searchQueryRef = useRef(searchQuery);
  useEffect(() => {
    searchQueryRef.current = searchQuery;
  }, [searchQuery]);
  const showMentionsRef = useRef(showMentions);
  useEffect(() => {
    showMentionsRef.current = showMentions;
  }, [showMentions]);

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
      const activeSearch = searchQueryRef.current;
      const suffix = allRef.current ? "?all=1&limit=100" : "?limit=100";
      // A failed mute list keeps its previous contents rather than blocking
      // the timeline and following refresh.
      const [feed, follows, muted, mutedKeywords] = await Promise.all([
        activeSearch !== null
          ? apiGet<TanTimelineResponse>(
              `/api/tan/search?q=${encodeURIComponent(activeSearch)}&limit=100`,
            )
          : showMentionsRef.current
            ? apiGet<TanTimelineResponse>("/api/tan/mentions?limit=100")
          : apiGet<TanTimelineResponse>(`/api/tan/timeline${suffix}`),
        apiGet<TanFollowingResponse>("/api/tan/following"),
        apiGet<TanMutedResponse>("/api/tan/muted").catch(() => null),
        apiGet<TanMutedKeywordsResponse>("/api/tan/muted-keywords").catch(
          () => null,
        ),
      ]);
      if (generationRef.current !== generation) return false;
      if (activeSearch !== null) {
        setSearchResults(feed.posts);
      } else if (showMentionsRef.current) {
        setMentions(feed.posts);
      } else {
        setPosts(feed.posts);
      }
      setFollowing(follows.following);
      if (muted !== null) setMutedAuthors(muted.muted ?? []);
      if (mutedKeywords !== null) {
        setMutedKeywords(mutedKeywords.muted_keywords ?? []);
      }
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
  }, [all, searchQuery, showMentions, load]);

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

  // A search is a view switch, like `all`: setting the query re-runs the
  // load effect, which fetches the search endpoint instead of the
  // timeline. Clearing it returns to the timeline the same way.
  const search = useCallback(
    async (query: string) => {
      const trimmed = query.trim();
      if (!trimmed) return;
      if (trimmed === searchQueryRef.current) {
        // Resubmitting the current query: setting the same state would
        // bail out without reloading, so refresh directly — and keep
        // the current results instead of clearing them, or the feed
        // would wrongly report no matches until the next poll.
        await load();
        return;
      }
      // Drop hits from the previous query so they are not shown as matches
      // for the new one while its request is in flight.
      setSearchResults([]);
      setSearchQuery(trimmed);
    },
    [load],
  );

  // Saved posts change only through this client's save/unsave actions
  // (or the CLI), so they load on demand instead of on every poll tick.
  // The store holds up to 512 saves, so the view pages through them
  // 100 at a time instead of fetching only the newest page.
  const savedRef = useRef(saved);
  useEffect(() => {
    savedRef.current = saved;
  }, [saved]);
  // The raw offset to resume paging from, as returned by the API.
  // Muted and hidden entries never shrink a page server-side, so the
  // next offset must come from the response — the filtered list length
  // would re-fetch rows already seen.
  const savedNextOffsetRef = useRef<number | null>(null);
  // Monotonic generation of saved loads, mirroring the feed's: a newer
  // load supersedes an older one, so a slow response cannot restore a
  // post that was unsaved after it started.
  const savedGenerationRef = useRef(0);
  // Whether the saved list has been loaded at least once. Actions
  // refresh it only then, so a like from the timeline does not pay for
  // a list nobody has opened.
  const savedLoadedRef = useRef(false);

  const fetchSavedPage = async (
    offset: number,
    limit: number = SAVED_PAGE_SIZE,
  ) => {
    return apiGet<TanSavedResponse>(
      `/api/tan/saved?limit=${limit}&offset=${offset}`,
    );
  };

  const loadSaved = useCallback(async () => {
    const generation = ++savedGenerationRef.current;
    setSavedLoading(true);
    try {
      const result = await fetchSavedPage(0);
      if (savedGenerationRef.current !== generation) return;
      setSaved(result.posts);
      savedNextOffsetRef.current = result.next_offset;
      setSavedHasMore(result.next_offset !== null);
      savedLoadedRef.current = true;
    } finally {
      if (savedGenerationRef.current === generation) {
        setSavedLoading(false);
      }
    }
  }, []);

  const loadMoreSaved = useCallback(async () => {
    const offset = savedNextOffsetRef.current;
    if (offset === null) return;
    const generation = ++savedGenerationRef.current;
    setSavedLoading(true);
    try {
      const result = await fetchSavedPage(offset);
      if (savedGenerationRef.current !== generation) return;
      setSaved((previous) => [...previous, ...result.posts]);
      savedNextOffsetRef.current = result.next_offset;
      setSavedHasMore(result.next_offset !== null);
    } finally {
      if (savedGenerationRef.current === generation) {
        setSavedLoading(false);
      }
    }
  }, []);

  // Re-read every page currently shown, paging with the server's
  // offsets. A save or unsave shifts the offsets behind the shown
  // pages, so refreshing only the first page would leave the rest
  // stale.
  const reloadSaved = useCallback(async () => {
    const shown = savedRef.current.length;
    if (shown === 0) return;
    const generation = ++savedGenerationRef.current;
    setSavedLoading(true);
    try {
      const all: TanPost[] = [];
      let offset: number | null = 0;
      while (all.length < shown && offset !== null) {
        const result = await fetchSavedPage(
          offset,
          Math.min(SAVED_PAGE_SIZE, shown - all.length),
        );
        if (savedGenerationRef.current !== generation) return;
        all.push(...result.posts);
        offset = result.next_offset;
      }
      if (savedGenerationRef.current !== generation) return;
      setSaved(all);
      savedNextOffsetRef.current = offset;
      setSavedHasMore(offset !== null);
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

  const clearSearch = useCallback(() => {
    setSearchQuery(null);
    setSearchResults([]);
  }, []);

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

  const quote = useCallback(
    async (target: string, text: string) => {
      setPosting(true);
      try {
        const result = await apiPost<TanPostResponse>("/api/tan/quote", {
          target,
          text,
        });
        setPosts((previous) => {
          if (previous.some((post) => post.hash === result.post.hash)) {
            return previous;
          }
          return [result.post, ...previous];
        });
        try {
          await load();
        } catch {
          // The quote is already published. A failed timeline refresh must
          // not report the publish as failed; the next poll will catch up.
        }
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

  const muteAuthor = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/mute", { target });
      await load();
    },
    [load],
  );

  const unmuteAuthor = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/unmute", { target });
      await load();
    },
    [load],
  );

  const muteKeyword = useCallback(
    async (keyword: string) => {
      await apiPost("/api/tan/mute-keyword", { keyword });
      await load();
    },
    [load],
  );

  const unmuteKeyword = useCallback(
    async (keyword: string) => {
      await apiPost("/api/tan/unmute-keyword", { keyword });
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

  // Who liked one post, newest like first. Fetched on demand when the
  // liker list is opened, not with the timeline, so the feed payload
  // stays small. Returns the whole response so the caller can tell a
  // truncated list (count < total) from a complete one.
  const fetchLikers = useCallback(async (target: string) => {
    return apiGet<TanLikesResponse>(
      `/api/tan/likes?target=${encodeURIComponent(target)}`,
    );
  }, []);

  const deletePost = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/delete", { target });
      await Promise.all([load(), refreshSavedIfLoaded()]);
    },
    [load, refreshSavedIfLoaded],
  );

  const editPost = useCallback(
    async (target: string, text: string) => {
      await apiPost("/api/tan/edit", { target, text });
      // The edit is already committed. A refresh failure must not make the
      // caller treat the edit as failed, so report it as a feed error instead.
      try {
        await load();
      } catch (caught) {
        setError(
          caught instanceof Error ? caught.message : "could not load the feed",
        );
      }
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

  // Download the signed export document the bridge returns. The anchor
  // click is the download: a blob URL keeps the file local, and the
  // revoke is deferred a beat so the browser has started the save.
  const exportTan = useCallback(async (allAuthors: boolean) => {
    setExporting(true);
    try {
      const scope = allAuthors ? "all" : "mine";
      const doc = await apiGet<TanExportDocument>(
        `/api/tan/export?scope=${scope}`,
      );
      const blob = new Blob([`${JSON.stringify(doc, null, 2)}\n`], {
        type: "application/json",
      });
      const url = URL.createObjectURL(blob);
      try {
        const anchor = document.createElement("a");
        anchor.href = url;
        anchor.download = "tau-tan-export.json";
        anchor.click();
      } finally {
        setTimeout(() => URL.revokeObjectURL(url), 1000);
      }
      return doc;
    } finally {
      setExporting(false);
    }
  }, []);

  const importTan = useCallback(
    async (doc: TanExportDocument): Promise<TanImportResult> => {
      setImporting(true);
      try {
        const result = await apiPost<TanImportResponse>(
          "/api/tan/import",
          doc as unknown as Record<string, unknown>,
        );
        // The entries are already stored once the POST succeeds, so a feed
        // refresh failure must not turn the import into a reported failure.
        try {
          await load();
        } catch (caught) {
          setError(
            caught instanceof Error ? caught.message : "could not load the feed",
          );
        }
        return result.import;
      } finally {
        setImporting(false);
      }
    },
    [load],
  );

  return {
    posts,
    searchResults,
    searchQuery,
    mentions,
    following,
    suggestions,
    mutedAuthors,
    mutedKeywords,
    saved,
    savedLoading,
    savedHasMore,
    loadSaved,
    loadMoreSaved,
    save,
    unsave,
    all,
    setAll,
    showMentions,
    setShowMentions,
    loading,
    posting,
    syncing,
    exporting,
    importing,
    error,
    threadPosts,
    threadRef,
    threadLoading,
    threadError,
    openThread,
    closeThread,
    refresh,
    search,
    clearSearch,
    publish,
    quote,
    follow,
    unfollow,
    muteAuthor,
    unmuteAuthor,
    muteKeyword,
    unmuteKeyword,
    like,
    unlike,
    fetchLikers,
    deletePost,
    editPost,
    sync,
    exportTan,
    importTan,
  };
}
