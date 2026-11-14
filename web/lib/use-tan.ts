"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { apiGet, apiPost } from "./api";
import type {
  TanAuthor,
  TanAuthorsResponse,
  TanCancelScheduledResponse,
  TanExportDocument,
  TanFollow,
  TanFollowingResponse,
  TanHiddenResponse,
  TanImportResponse,
  TanImportResult,
  TanLikesResponse,
  TanMutedAuthor,
  TanMutedKeyword,
  TanMutedKeywordsResponse,
  TanMutedResponse,
  TanPost,
  TanPostResponse,
  TanRules,
  TanRulesResponse,
  TanRulesUpdateResponse,
  TanPinnedResponse,
  TanSavedResponse,
  TanScheduleResponse,
  TanScheduledPost,
  TanScheduledResponse,
  TanSuggestResponse,
  TanSuggestion,
  TanSyncResponse,
  TanTagsResponse,
  TanTag,
  TanTimelineResponse,
  TanVerifyReport,
} from "./types";

// The feed poll cadence, shared with the console's Scheduled view so a
// post the serving node publishes disappears from the queue promptly.
export const TAN_POLL_INTERVAL_MS = 5000;
// Suggestion ranking scans the whole cache, so it refreshes on a slower
// cadence than the feed and stays off the feed's poll critical path.
const SUGGEST_INTERVAL_MS = 60000;
const SAVED_PAGE_SIZE = 100;
// Hidden posts load the same way as saved ones: the store holds at
// most 512 hides, so the view pages through them 100 at a time.
const HIDDEN_PAGE_SIZE = 100;
// Pinned posts load the same way as saved ones. The shelf holds at
// most 10 pins, so one page normally covers the whole list.
const PINNED_PAGE_SIZE = 100;
// Authors per directory page, matching the API's default limit.
const AUTHORS_PAGE_SIZE = 50;

export function useTan() {
  const [posts, setPosts] = useState<TanPost[]>([]);
  // The view that produced `posts`. After a switch the old posts belong to
  // the previous ranking, so they are hidden until the new view loads.
  const [postsView, setPostsView] = useState<"timeline" | "top" | null>(null);
  const [searchResults, setSearchResults] = useState<TanPost[]>([]);
  // The search the feed is showing, or null for the timeline. Kept beside
  // `all` so a search mid-action reloads the search, the same way `all`
  // works — a like or delete on a search result refreshes the results.
  const [searchQuery, setSearchQuery] = useState<string | null>(null);
  const [mentions, setMentions] = useState<TanPost[]>([]);
  const [following, setFollowing] = useState<TanFollow[]>([]);
  // Every known author in the local store, not just the followed ones:
  // the directory the console uses to discover who to follow.
  const [authors, setAuthors] = useState<TanAuthor[]>([]);
  // How many authors the directory holds in total, behind the pages
  // currently shown; the console offers the next page while the shown
  // count is below this.
  const [authorsTotal, setAuthorsTotal] = useState(0);
  // The shown authors, mirrored for refreshes that re-read the loaded
  // pages, and a generation for the directory's own loads: a newer
  // refresh supersedes an older one, as the feed's does.
  const authorsRef = useRef(authors);
  useEffect(() => {
    authorsRef.current = authors;
  }, [authors]);
  const authorsGenerationRef = useRef(0);
  const [suggestions, setSuggestions] = useState<TanSuggestion[]>([]);
  // Tags in stored posts, most-used first. The list is a whole-store
  // scan, so it refreshes on the suggestions cadence rather than on
  // the feed poll, and after an action that can change the tag set.
  const [tags, setTags] = useState<TanTag[]>([]);
  // The tag the timeline is filtered to, or null for the unfiltered
  // timeline. A view switch like the search: setting it fetches the
  // timeline endpoint with `?tag=` instead of the plain timeline.
  const [tagFilter, setTagFilterState] = useState<string | null>(null);
  const [mutedAuthors, setMutedAuthors] = useState<TanMutedAuthor[]>([]);
  const [mutedKeywords, setMutedKeywords] = useState<TanMutedKeyword[]>([]);
  const [saved, setSaved] = useState<TanPost[]>([]);
  const [savedLoading, setSavedLoading] = useState(false);
  const [savedHasMore, setSavedHasMore] = useState(false);
  const [hidden, setHidden] = useState<TanPost[]>([]);
  const [hiddenLoading, setHiddenLoading] = useState(false);
  const [hiddenHasMore, setHiddenHasMore] = useState(false);
  const [pinned, setPinned] = useState<TanPost[]>([]);
  const [pinnedLoading, setPinnedLoading] = useState(false);
  const [pinnedHasMore, setPinnedHasMore] = useState(false);
  // Queued Tan posts waiting for their publish time; loaded on demand
  // like the saved and pinned lists.
  const [scheduled, setScheduled] = useState<TanScheduledPost[]>([]);
  const [scheduledLoading, setScheduledLoading] = useState(false);
  const [all, setAll] = useState(false);
  // Which ranking the feed shows: the chronological timeline, or the
  // most-liked posts. The top view ignores the following/everything
  // filter — it ranks the whole visible store, like `tau tan top`.
  const [view, setView] = useState<"timeline" | "top">("timeline");
  // Whether the feed shows replies to your own posts instead of the
  // timeline. Kept beside `all` so a view switch mid-action reloads
  // the view that is actually selected, the same way `all` works.
  const [showMentions, setShowMentions] = useState(false);
  const [loading, setLoading] = useState(true);
  const [posting, setPosting] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [verifying, setVerifying] = useState(false);
  const [verifyReport, setVerifyReport] = useState<TanVerifyReport | null>(
    null,
  );
  const [exporting, setExporting] = useState(false);
  const [importing, setImporting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [rules, setRules] = useState<TanRules | null>(null);
  const [availableKinds, setAvailableKinds] = useState<string[]>([]);
  const [rulesLoading, setRulesLoading] = useState(true);
  const [rulesError, setRulesError] = useState<string | null>(null);
  // Generation of rules requests; a response from an older request, or
  // one superseded by a confirmed save, is discarded.
  const rulesGenerationRef = useRef(0);

  // The thread currently open in the console, if any: the reference it
  // was opened with and its entries (null while none is open).
  const [threadPosts, setThreadPosts] = useState<TanPost[] | null>(null);
  const [threadRef, setThreadRef] = useState<string | null>(null);
  const [threadLoading, setThreadLoading] = useState(false);
  const [threadError, setThreadError] = useState<string | null>(null);

  // A displayed audit is only as fresh as the store it read. Every
  // action that writes posts bumps this generation and drops the
  // report, so the sidebar never presents a stale audit as the
  // store's current health; an audit that resolves after a mutation
  // is discarded the same way.
  const verifyGenerationRef = useRef(0);
  const invalidateVerify = useCallback(() => {
    verifyGenerationRef.current += 1;
    setVerifyReport(null);
  }, []);

  // The filter the timeline is showing right now. Actions refresh through
  // this ref instead of a captured `all`, so a filter switch mid-action
  // does not reload the filter that is no longer selected.
  const allRef = useRef(all);
  useEffect(() => {
    allRef.current = all;
  }, [all]);

  const viewRef = useRef(view);
  useEffect(() => {
    viewRef.current = view;
  }, [view]);
  const searchQueryRef = useRef(searchQuery);
  useEffect(() => {
    searchQueryRef.current = searchQuery;
  }, [searchQuery]);
  const showMentionsRef = useRef(showMentions);
  useEffect(() => {
    showMentionsRef.current = showMentions;
  }, [showMentions]);

  // The tag filter the timeline is showing, kept beside `tagFilter`
  // so a feed load can read it without waiting for the state update.
  const tagFilterRef = useRef(tagFilter);
  useEffect(() => {
    tagFilterRef.current = tagFilter;
  }, [tagFilter]);

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
      Date.now() - pending.startedAt < TAN_POLL_INTERVAL_MS * 2
    ) {
      return false;
    }
    const generation = ++generationRef.current;
    pendingRef.current = { generation, startedAt: Date.now() };
    try {
      const isTop = viewRef.current === "top";
      const activeSearch = searchQueryRef.current;
      const activeTag = tagFilterRef.current;
      // The tag filter applies to the timeline only: a search, the
      // top ranking and mentions each fetch their own endpoint.
      const timelineParams = new URLSearchParams({ limit: "100" });
      if (allRef.current) timelineParams.set("all", "1");
      if (activeTag !== null) timelineParams.set("tag", activeTag);
      const timelineUrl = `/api/tan/timeline?${timelineParams.toString()}`;
      // A search stays a search even from the top view: the search form
      // is always visible, so a search submitted there must fetch the
      // search endpoint, not the top ranking.
      // A failed mute list keeps its previous contents rather than blocking
      // the timeline and following refresh.
      const [feed, follows, muted, mutedKeywords] = await Promise.all([
        activeSearch !== null
          ? apiGet<TanTimelineResponse>(
              `/api/tan/search?q=${encodeURIComponent(activeSearch)}&limit=100`,
            )
          : isTop
            ? apiGet<TanTimelineResponse>("/api/tan/top?limit=10")
            : showMentionsRef.current
              ? apiGet<TanTimelineResponse>("/api/tan/mentions?limit=100")
              : apiGet<TanTimelineResponse>(timelineUrl),
        apiGet<TanFollowingResponse>("/api/tan/following"),
        apiGet<TanMutedResponse>("/api/tan/muted").catch(() => null),
        apiGet<TanMutedKeywordsResponse>("/api/tan/muted-keywords").catch(
          () => null,
        ),
      ]);
      if (generationRef.current !== generation) return false;
      if (activeSearch !== null) {
        setSearchResults(feed.posts);
      } else if (isTop) {
        setPosts(feed.posts);
        setPostsView("top");
      } else if (showMentionsRef.current) {
        setMentions(feed.posts);
      } else {
        setPosts(feed.posts);
        setPostsView("timeline");
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

  // Re-read the directory pages currently shown, in offset-sized
  // pages, so a follow, unfollow or sync refreshes the followed flags
  // without losing the user's place. The directory loads on its own
  // generation, not the feed's: a failed or stalled authors request
  // never blocks the feed, and on failure the previous directory
  // stays in place.
  const refreshAuthors = useCallback(async () => {
    const generation = ++authorsGenerationRef.current;
    // Re-read at least one page, even before anything is shown.
    const target = Math.max(authorsRef.current.length, AUTHORS_PAGE_SIZE);
    const pages: TanAuthor[] = [];
    let total = 0;
    let offset = 0;
    while (offset < target) {
      const result = await apiGet<TanAuthorsResponse>(
        `/api/tan/authors?limit=${AUTHORS_PAGE_SIZE}&offset=${offset}`,
      );
      if (authorsGenerationRef.current !== generation) return;
      pages.push(...result.authors);
      total = result.total;
      offset += result.authors.length;
      if (result.authors.length === 0) break;
    }
    if (authorsGenerationRef.current !== generation) return;
    setAuthors(pages);
    setAuthorsTotal(total);
  }, []);

  // Fetch the next directory page and append it. Offset paging, not a
  // growing limit: the API caps a single request at 200 rows, so
  // growing the limit would fetch the same first 200 rows forever and
  // never reach author 201.
  const loadMoreAuthors = useCallback(async () => {
    const generation = ++authorsGenerationRef.current;
    const offset = authorsRef.current.length;
    const result = await apiGet<TanAuthorsResponse>(
      `/api/tan/authors?limit=${AUTHORS_PAGE_SIZE}&offset=${offset}`,
    );
    if (authorsGenerationRef.current !== generation) return;
    setAuthors((previous) => [...previous, ...result.authors]);
    setAuthorsTotal(result.total);
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
    setLoading(true);
    void tick(true);
    // The directory loads once with the feed; the poll tick does not
    // re-read it (a follow, unfollow or sync refreshes it instead),
    // so paging past the first page is never undone by a poll.
    void refreshAuthors().catch(() => {});
    // The top ranking changes only when someone likes, unlikes, posts
    // or deletes, and every one of those actions reloads the current
    // view — so the top view loads on demand and on manual refresh,
    // and never joins the timeline's 5s poll. A search submitted from
    // the top view still polls like any other search.
    if (view === "top" && searchQuery === null) {
      return () => {
        disposed = true;
      };
    }
    const timer = setInterval(() => void tick(false), TAN_POLL_INTERVAL_MS);
    return () => {
      disposed = true;
      clearInterval(timer);
    };
  }, [all, view, searchQuery, showMentions, tagFilter, load, refreshAuthors]);

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

  // Tags have their own generation so a slow response cannot overwrite
  // a newer one, and they never block the feed load. A failure keeps
  // the last good list, the way suggestions do.
  const tagsGenerationRef = useRef(0);
  const loadTags = useCallback(async () => {
    const generation = ++tagsGenerationRef.current;
    const result = await apiGet<TanTagsResponse>("/api/tan/tags");
    if (tagsGenerationRef.current !== generation) return;
    setTags(result.tags ?? []);
  }, []);

  useEffect(() => {
    let disposed = false;
    async function tickTags() {
      try {
        await loadTags();
      } catch {
        // Tags are optional; keep the last good list on failure.
      }
      if (disposed) return;
    }
    void tickTags();
    const timer = setInterval(() => void tickTags(), SUGGEST_INTERVAL_MS);
    return () => {
      disposed = true;
      clearInterval(timer);
    };
  }, [loadTags]);

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
      // A search leaves the tag filter: the two switches are
      // exclusive, so a search result is never shown as a tag hit.
      setTagFilterState(null);
    },
    [load],
  );

  // A tag filter is a view switch, like a search: setting it re-runs
  // the load effect, which fetches the timeline with `?tag=` instead
  // of the plain timeline. Clearing it returns to the timeline the
  // same way. Picking a tag leaves any search results, and the two
  // switches stay exclusive.
  const setTagFilter = useCallback(
    async (tag: string | null) => {
      // Tags are stored casefolded without the leading '#', matching
      // the server and the sidebar, so comparisons with them hold.
      const cleaned =
        tag === null ? "" : tag.trim().replace(/^#/, "").toLowerCase();
      const normalized = cleaned !== "" ? cleaned : null;
      if (normalized === tagFilterRef.current) {
        // Re-picking the current tag: setting the same state would
        // bail out without reloading, so refresh directly.
        await load();
        return;
      }
      if (normalized !== null) {
        setSearchResults([]);
        setSearchQuery(null);
      }
      setTagFilterState(normalized);
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


  // A save or pin has already committed when these refreshes run, so a
  // refresh failure must not be reported as the action failing.
  const runRefreshes = useCallback(async (refreshes: Promise<unknown>[]) => {
    const results = await Promise.allSettled(refreshes);
    if (results.some((result) => result.status === "rejected")) {
      setError("the change was saved, but the view could not refresh");
    }
  }, []);

  // Pinned posts change only through this client's pin/unpin actions
  // (or the CLI), so they load on demand instead of on every poll tick,
  // exactly like saved posts.
  const pinnedRef = useRef(pinned);
  useEffect(() => {
    pinnedRef.current = pinned;
  }, [pinned]);
  // The raw offset to resume paging from, as returned by the API.
  // Muted and hidden entries never shrink a page server-side, so the
  // next offset must come from the response — the filtered list length
  // would re-fetch rows already seen.
  const pinnedNextOffsetRef = useRef<number | null>(null);
  // Monotonic generation of pinned loads, mirroring the feed's: a newer
  // load supersedes an older one, so a slow response cannot restore a
  // post that was unpinned after it started.
  const pinnedGenerationRef = useRef(0);
  // Whether the pinned list has been loaded at least once. Actions
  // refresh it only then, so a pin from the timeline does not pay for
  // a list nobody has opened.
  const pinnedLoadedRef = useRef(false);

  const fetchPinnedPage = async (
    offset: number,
    limit: number = PINNED_PAGE_SIZE,
  ) => {
    return apiGet<TanPinnedResponse>(
      `/api/tan/pinned?limit=${limit}&offset=${offset}`,
    );
  };

  const loadPinned = useCallback(async () => {
    const generation = ++pinnedGenerationRef.current;
    setPinnedLoading(true);
    try {
      const result = await fetchPinnedPage(0);
      if (pinnedGenerationRef.current !== generation) return;
      setPinned(result.posts);
      pinnedNextOffsetRef.current = result.next_offset;
      setPinnedHasMore(result.next_offset !== null);
      pinnedLoadedRef.current = true;
    } finally {
      if (pinnedGenerationRef.current === generation) {
        setPinnedLoading(false);
      }
    }
  }, []);

  const loadMorePinned = useCallback(async () => {
    const offset = pinnedNextOffsetRef.current;
    if (offset === null) return;
    const generation = ++pinnedGenerationRef.current;
    setPinnedLoading(true);
    try {
      const result = await fetchPinnedPage(offset);
      if (pinnedGenerationRef.current !== generation) return;
      setPinned((previous) => [...previous, ...result.posts]);
      pinnedNextOffsetRef.current = result.next_offset;
      setPinnedHasMore(result.next_offset !== null);
    } finally {
      if (pinnedGenerationRef.current === generation) {
        setPinnedLoading(false);
      }
    }
  }, []);

  // Re-read every page currently shown, paging with the server's
  // offsets. A pin or unpin shifts the offsets behind the shown
  // pages, so refreshing only the first page would leave the rest
  // stale.
  const reloadPinned = useCallback(async () => {
    // An empty shelf still fetches its first page: a pin from another
    // view may have added posts to it.
    const shown = pinnedRef.current.length;
    const wanted = shown === 0 ? PINNED_PAGE_SIZE : shown;
    const generation = ++pinnedGenerationRef.current;
    setPinnedLoading(true);
    try {
      const all: TanPost[] = [];
      let offset: number | null = 0;
      while (all.length < wanted && offset !== null) {
        const result = await fetchPinnedPage(
          offset,
          Math.min(PINNED_PAGE_SIZE, wanted - all.length),
        );
        if (pinnedGenerationRef.current !== generation) return;
        all.push(...result.posts);
        offset = result.next_offset;
      }
      if (pinnedGenerationRef.current !== generation) return;
      setPinned(all);
      pinnedNextOffsetRef.current = offset;
      setPinnedHasMore(offset !== null);
    } finally {
      if (pinnedGenerationRef.current === generation) {
        setPinnedLoading(false);
      }
    }
  }, []);

  const refreshPinnedIfLoaded = useCallback(async () => {
    if (pinnedLoadedRef.current) {
      await reloadPinned();
    }
  }, [reloadPinned]);

  // Hidden posts change only through this client's hide/unhide actions
  // (or the CLI), so they load on demand instead of on every poll tick,
  // exactly like saved posts.
  const hiddenRef = useRef(hidden);
  useEffect(() => {
    hiddenRef.current = hidden;
  }, [hidden]);
  // The raw offset to resume paging from, as returned by the API.
  // Expired entries are filtered server-side before the page, so the
  // next offset must come from the response — the filtered list length
  // would re-fetch rows already seen.
  const hiddenNextOffsetRef = useRef<number | null>(null);
  // Monotonic generation of hidden loads, mirroring the feed's: a newer
  // load supersedes an older one, so a slow response cannot restore a
  // post that was unhidden after it started.
  const hiddenGenerationRef = useRef(0);
  // Whether the hidden list has been loaded at least once. Actions
  // refresh it only then, so a hide from the timeline does not pay for
  // a list nobody has opened.
  const hiddenLoadedRef = useRef(false);

  const fetchHiddenPage = async (
    offset: number,
    limit: number = HIDDEN_PAGE_SIZE,
  ) => {
    return apiGet<TanHiddenResponse>(
      `/api/tan/hidden?limit=${limit}&offset=${offset}`,
    );
  };

  const loadHidden = useCallback(async () => {
    const generation = ++hiddenGenerationRef.current;
    setHiddenLoading(true);
    try {
      const result = await fetchHiddenPage(0);
      if (hiddenGenerationRef.current !== generation) return;
      setHidden(result.posts);
      hiddenNextOffsetRef.current = result.next_offset;
      setHiddenHasMore(result.next_offset !== null);
      hiddenLoadedRef.current = true;
    } finally {
      if (hiddenGenerationRef.current === generation) {
        setHiddenLoading(false);
      }
    }
  }, []);

  const loadMoreHidden = useCallback(async () => {
    const offset = hiddenNextOffsetRef.current;
    if (offset === null) return;
    const generation = ++hiddenGenerationRef.current;
    setHiddenLoading(true);
    try {
      const result = await fetchHiddenPage(offset);
      if (hiddenGenerationRef.current !== generation) return;
      setHidden((previous) => [...previous, ...result.posts]);
      hiddenNextOffsetRef.current = result.next_offset;
      setHiddenHasMore(result.next_offset !== null);
    } finally {
      if (hiddenGenerationRef.current === generation) {
        setHiddenLoading(false);
      }
    }
  }, []);

  // Re-read every page currently shown, paging with the server's
  // offsets. A hide or unhide shifts the offsets behind the shown
  // pages, so refreshing only the first page would leave the rest
  // stale. One extra row is read so a new hide, which lands at the
  // top, does not push the last shown post out of the list.
  const reloadHidden = useCallback(async () => {
    if (!hiddenLoadedRef.current) return;
    const want = hiddenRef.current.length + 1;
    const generation = ++hiddenGenerationRef.current;
    setHiddenLoading(true);
    try {
      const all: TanPost[] = [];
      let offset: number | null = 0;
      while (all.length < want && offset !== null) {
        const result = await fetchHiddenPage(
          offset,
          Math.min(HIDDEN_PAGE_SIZE, want - all.length),
        );
        if (hiddenGenerationRef.current !== generation) return;
        all.push(...result.posts);
        offset = result.next_offset;
      }
      if (hiddenGenerationRef.current !== generation) return;
      setHidden(all);
      hiddenNextOffsetRef.current = offset;
      setHiddenHasMore(offset !== null);
    } finally {
      if (hiddenGenerationRef.current === generation) {
        setHiddenLoading(false);
      }
    }
  }, []);

  const refreshHiddenIfLoaded = useCallback(async () => {
    if (hiddenLoadedRef.current) {
      await reloadHidden();
    }
  }, [reloadHidden]);

  const pin = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/pin", { target });
      await runRefreshes([
        load(),
        refreshPinnedIfLoaded(),
        refreshSavedIfLoaded(),
        refreshHiddenIfLoaded(),
      ]);
    },
    [
      load,
      refreshHiddenIfLoaded,
      refreshPinnedIfLoaded,
      refreshSavedIfLoaded,
      runRefreshes,
    ],
  );

  const unpin = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/unpin", { target });
      await runRefreshes([
        load(),
        refreshPinnedIfLoaded(),
        refreshSavedIfLoaded(),
        refreshHiddenIfLoaded(),
      ]);
    },
    [
      load,
      refreshHiddenIfLoaded,
      refreshPinnedIfLoaded,
      refreshSavedIfLoaded,
      runRefreshes,
    ],
  );

  const save = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/save", { target });
      await runRefreshes([
        load(),
        reloadSaved(),
        refreshPinnedIfLoaded(),
        refreshHiddenIfLoaded(),
      ]);
    },
    [
      load,
      refreshHiddenIfLoaded,
      reloadSaved,
      refreshPinnedIfLoaded,
      runRefreshes,
    ],
  );

  const unsave = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/unsave", { target });
      await runRefreshes([
        load(),
        reloadSaved(),
        refreshPinnedIfLoaded(),
        refreshHiddenIfLoaded(),
      ]);
    },
    [
      load,
      refreshHiddenIfLoaded,
      reloadSaved,
      refreshPinnedIfLoaded,
      runRefreshes,
    ],
  );

  const hide = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/hide", { target });
      // Hiding drops the post from the feed and from the saved and
      // pinned views server-side, so those refresh too; the hidden
      // list itself only refreshes when it has been opened.
      await runRefreshes([
        load(),
        reloadHidden(),
        refreshSavedIfLoaded(),
        refreshPinnedIfLoaded(),
      ]);
    },
    [
      load,
      reloadHidden,
      refreshPinnedIfLoaded,
      refreshSavedIfLoaded,
      runRefreshes,
    ],
  );

  const unhide = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/unhide", { target });
      // Unhiding restores the post to the feed and to the saved and
      // pinned views, so those refresh too.
      await runRefreshes([
        load(),
        reloadHidden(),
        refreshSavedIfLoaded(),
        refreshPinnedIfLoaded(),
      ]);
    },
    [
      load,
      reloadHidden,
      refreshPinnedIfLoaded,
      refreshSavedIfLoaded,
      runRefreshes,
    ],
  );

  // Scheduled posts change only through this client's schedule/cancel
  // actions (or the CLI), so they load on demand instead of on every
  // poll tick. The queue is small, so one fetch covers the whole list.
  const scheduledGenerationRef = useRef(0);
  // Whether the scheduled list has been loaded at least once. A
  // schedule refreshes it only then, so scheduling from the composer
  // does not pay for a list nobody has opened.
  const scheduledLoadedRef = useRef(false);

  const loadScheduled = useCallback(async () => {
    const generation = ++scheduledGenerationRef.current;
    setScheduledLoading(true);
    try {
      const result = await apiGet<TanScheduledResponse>("/api/tan/scheduled");
      if (scheduledGenerationRef.current !== generation) return;
      setScheduled(result.scheduled);
      scheduledLoadedRef.current = true;
    } finally {
      if (scheduledGenerationRef.current === generation) {
        setScheduledLoading(false);
      }
    }
  }, []);

  const refreshScheduledIfLoaded = useCallback(async () => {
    // A load already in flight may have captured the queue before this
    // schedule, so refresh then too; the newer request supersedes it.
    if (scheduledLoadedRef.current || scheduledGenerationRef.current > 0) {
      await loadScheduled();
    }
  }, [loadScheduled]);

  const schedulePost = useCallback(
    async (text: string, sendIn: number, replyTo?: string, expiresIn?: number) => {
      setPosting(true);
      try {
        await apiPost<TanScheduleResponse>("/api/tan/schedule", {
          text,
          send_in: sendIn,
          ...(replyTo ? { reply_to: replyTo } : {}),
          ...(expiresIn ? { expires_in: expiresIn } : {}),
        });
        // The post is already queued: a failed list refresh must not
        // reject, or the composer keeps the draft and a retry duplicates it.
        try {
          await refreshScheduledIfLoaded();
        } catch {
          setError("post scheduled, but the scheduled list could not refresh");
        }
      } finally {
        setPosting(false);
      }
    },
    [refreshScheduledIfLoaded],
  );

  const cancelScheduled = useCallback(async (id: string) => {
    await apiPost<TanCancelScheduledResponse>("/api/tan/cancel-scheduled", {
      id,
    });
    // A list load that started before this cancel must not restore the
    // card: advance the generation so its response is discarded, and
    // clear the loading flag it would otherwise leave set.
    scheduledGenerationRef.current += 1;
    setScheduledLoading(false);
    setScheduled((previous) => previous.filter((post) => post.id !== id));
  }, []);

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
    async (text: string, replyTo?: string, expiresIn?: number) => {
      invalidateVerify();
      setPosting(true);
      try {
        const result = await apiPost<TanPostResponse>("/api/tan/posts", {
          text,
          ...(replyTo ? { reply_to: replyTo } : {}),
          ...(expiresIn ? { expires_in: expiresIn } : {}),
        });
        setPosts((previous) => {
          if (previous.some((post) => post.hash === result.post.hash)) {
            return previous;
          }
          return [result.post, ...previous];
        });
        // A new post can introduce a tag; refresh the list, keeping
        // the old one on failure. Started before the feed refresh so a
        // failed feed refresh cannot skip it.
        void loadTags().catch(() => undefined);
        await load();
        return result.post;
      } finally {
        setPosting(false);
      }
    },
    [load, invalidateVerify, loadTags],
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
        // A quote can introduce a tag; refresh the list, keeping the
        // old one on failure.
        void loadTags().catch(() => undefined);
        return result.post;
      } finally {
        setPosting(false);
      }
    },
    [load, loadTags],
  );

  const follow = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/follow", { target });
      await Promise.all([load(), refreshAuthors()]);
      void loadSuggestions().catch(() => undefined);
    },
    [load, refreshAuthors, loadSuggestions],
  );

  const unfollow = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/unfollow", { target });
      await Promise.all([load(), refreshAuthors()]);
      void loadSuggestions().catch(() => undefined);
    },
    [load, refreshAuthors, loadSuggestions],
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
      // Likes are chain entries in the store, so they invalidate an
      // audit exactly like a post does.
      invalidateVerify();
      await apiPost("/api/tan/like", { target });
      await Promise.all([
        load(),
        refreshSavedIfLoaded(),
        refreshPinnedIfLoaded(),
        refreshHiddenIfLoaded(),
      ]);
    },
    [
      load,
      invalidateVerify,
      refreshHiddenIfLoaded,
      refreshSavedIfLoaded,
      refreshPinnedIfLoaded,
    ],
  );

  const unlike = useCallback(
    async (target: string) => {
      invalidateVerify();
      await apiPost("/api/tan/unlike", { target });
      await Promise.all([
        load(),
        refreshSavedIfLoaded(),
        refreshPinnedIfLoaded(),
        refreshHiddenIfLoaded(),
      ]);
    },
    [
      load,
      invalidateVerify,
      refreshHiddenIfLoaded,
      refreshSavedIfLoaded,
      refreshPinnedIfLoaded,
    ],
  );

  const repost = useCallback(
    async (target: string) => {
      await apiPost("/api/tan/repost", { target });
      await Promise.all([
        load(),
        refreshSavedIfLoaded(),
        refreshPinnedIfLoaded(),
        refreshHiddenIfLoaded(),
      ]);
    },
    [
      load,
      refreshHiddenIfLoaded,
      refreshSavedIfLoaded,
      refreshPinnedIfLoaded,
    ],
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
      invalidateVerify();
      await apiPost("/api/tan/delete", { target });
      // A delete can retire a tag; refresh the list, keeping the old
      // one on failure. Started before the refreshes below so a failed
      // feed refresh cannot skip it.
      void loadTags().catch(() => undefined);
      await Promise.all([
        load(),
        refreshSavedIfLoaded(),
        refreshPinnedIfLoaded(),
        refreshHiddenIfLoaded(),
      ]);
    },
    [
      load,
      invalidateVerify,
      refreshHiddenIfLoaded,
      refreshSavedIfLoaded,
      refreshPinnedIfLoaded,
      loadTags,
    ],
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
      // An edit can introduce or retire a tag; refresh the list,
      // keeping the old one on failure.
      void loadTags().catch(() => undefined);
    },
    [load, loadTags],
  );

  const sync = useCallback(async () => {
    invalidateVerify();
    setSyncing(true);
    try {
      const result = await apiPost<TanSyncResponse>("/api/tan/sync", {
        explore: true,
      });
      // A sync can bring in new authors, so the directory refreshes
      // alongside the feed.
      await Promise.all([load(), refreshAuthors()]);
      void loadSuggestions().catch(() => undefined);
      // A sync can bring in posts with new tags; refresh the list,
      // keeping the old one on failure.
      void loadTags().catch(() => undefined);
      return result.sync;
    } finally {
      setSyncing(false);
    }
  }, [load, invalidateVerify, refreshAuthors, loadSuggestions, loadTags]);

  // A read-only audit: verifying never writes to the store and never
  // reloads the feed, so it stays independent of the poll cadence.
  // The report is kept only if no store-changing action landed while
  // the audit was in flight.
  const verify = useCallback(async () => {
    setVerifying(true);
    const generation = verifyGenerationRef.current;
    try {
      const report = await apiGet<TanVerifyReport>("/api/tan/verify");
      if (verifyGenerationRef.current === generation) {
        setVerifyReport(report);
      }
      return report;
    } finally {
      setVerifying(false);
    }
  }, []);

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
    // Posts from the other view are never shown under this view's label.
    posts: postsView === view ? posts : [],
    searchResults,
    searchQuery,
    mentions,
    following,
    authors,
    authorsTotal,
    loadMoreAuthors,
    suggestions,
    tags,
    tagFilter,
    setTagFilter,
    mutedAuthors,
    mutedKeywords,
    saved,
    savedLoading,
    savedHasMore,
    loadSaved,
    loadMoreSaved,
    save,
    unsave,
    hidden,
    hiddenLoading,
    hiddenHasMore,
    loadHidden,
    loadMoreHidden,
    hide,
    unhide,
    pinned,
    pinnedLoading,
    pinnedHasMore,
    loadPinned,
    loadMorePinned,
    pin,
    unpin,
    scheduled,
    scheduledLoading,
    loadScheduled,
    schedulePost,
    cancelScheduled,
    all,
    setAll,
    view,
    setView,
    showMentions,
    setShowMentions,
    loading,
    posting,
    syncing,
    verifying,
    verifyReport,
    exporting,
    importing,
    error,
    rules,
    availableKinds,
    rulesLoading,
    rulesError,
    refresh,
    refreshRules,
    updateRules,
    threadPosts,
    threadRef,
    threadLoading,
    threadError,
    openThread,
    closeThread,
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
    repost,
    fetchLikers,
    deletePost,
    editPost,
    sync,
    verify,
    exportTan,
    importTan,
  };
}
