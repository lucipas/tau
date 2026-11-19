// SocialSection.tsx — the Tan home feed (X-style): composer on top, tabs
// Following / All / Top / Mentions, post cards, thread view, search,
// Discover, tags, Tan lists, and the Saved / Hidden / Pinned / Scheduled /
// Muted library. Built on useTanFeed / useTanThread / useTanComposer /
// useTanLists (web/lib/use-tan.ts).

"use client";

import { useCallback, useEffect, useState } from "react";
import Avatar from "@/components/chrome/Avatar";
import { useTauContext } from "@/components/chrome/TauContext";
import {
  cancelScheduledTanPost,
  getTanFollowedTags,
} from "@/lib/api";
import { formatDateTime, formatRelative, shortHash } from "@/lib/format";
import type { TanAuthor, TanListMember, TanPost, TanReactors } from "@/lib/types";
import {
  useTanComposer,
  useTanFeed,
  useTanLists,
  useTanThread,
  type TanFeedMode,
} from "@/lib/use-tan";
import PostCard, { type ReactorKind } from "./social/PostCard";
import TanComposer from "./social/TanComposer";

type LibraryView = "saved" | "hidden" | "pinned" | "scheduled" | "muted";
type View = "feed" | "discover" | "followers" | "lists" | LibraryView;

const FEED_TABS: { id: TanFeedMode; label: string }[] = [
  { id: "following", label: "Following" },
  { id: "all", label: "All" },
  { id: "top", label: "Top" },
  { id: "mentions", label: "Mentions" },
];

const LIBRARY_TABS: { id: LibraryView; label: string }[] = [
  { id: "saved", label: "Saved" },
  { id: "hidden", label: "Hidden" },
  { id: "pinned", label: "Pinned" },
  { id: "scheduled", label: "Scheduled" },
  { id: "muted", label: "Muted" },
];

function safeTau() {
  try {
    return useTauContext();
  } catch {
    return null;
  }
}

export default function SocialSection() {
  const tau = safeTau();
  const [tab, setTab] = useState<TanFeedMode>("following");
  const [view, setView] = useState<View>("feed");
  const [tagFilter, setTagFilter] = useState<string | null>(null);
  const [listFilter, setListFilter] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<TanPost[] | null>(null);
  const [searching, setSearching] = useState(false);
  const [threadHash, setThreadHash] = useState<string | null>(null);
  const [replyTo, setReplyTo] = useState<TanPost | null>(null);
  const [reactors, setReactors] = useState<{
    kind: ReactorKind;
    post: TanPost;
    data: TanReactors | null;
    loading: boolean;
  } | null>(null);
  const [followedTags, setFollowedTags] = useState<string[]>([]);
  const [keywordInput, setKeywordInput] = useState("");
  const [authors, setAuthors] = useState<TanAuthor[]>([]);
  const [notice, setNotice] = useState<string | null>(null);

  const feed = useTanFeed({ mode: tab, tag: tagFilter ?? undefined, list: listFilter ?? undefined, pollMs: 30_000 });
  const lists = useTanLists();
  const composer = useTanComposer();
  const thread = useTanThread(threadHash);

  const refreshFollowedTags = useCallback(async () => {
    try {
      setFollowedTags(await getTanFollowedTags());
    } catch {
      // Tag follows are advisory; the feed works without them.
    }
  }, []);

  useEffect(() => {
    void refreshFollowedTags();
  }, [refreshFollowedTags]);

  useEffect(() => {
    if (view === "discover") {
      void lists.authors("recent", 20).then((r) => setAuthors(r.authors)).catch(() => undefined);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [view]);

  function openThread(post: TanPost) {
    setThreadHash(post.hash);
    setReactors(null);
  }

  function openReply(post: TanPost) {
    setThreadHash(post.hash);
    setReplyTo(post);
  }

  async function runSearch(q: string) {
    const query = q.trim();
    if (!query) {
      setSearchResults(null);
      return;
    }
    setSearching(true);
    try {
      setSearchResults(await feed.search(query));
      setView("feed");
    } finally {
      setSearching(false);
    }
  }

  async function showReactors(kind: ReactorKind, post: TanPost) {
    setReactors({ kind, post, data: null, loading: true });
    try {
      const data =
        kind === "likes"
          ? await thread.likers(post.hash)
          : kind === "reposts"
            ? await thread.reposters(post.hash)
            : await thread.quoters(post.hash);
      setReactors({ kind, post, data, loading: false });
    } catch {
      setReactors({ kind, post, data: null, loading: false });
    }
  }

  function clearFilters() {
    setTagFilter(null);
    setListFilter(null);
    setSearchQuery("");
    setSearchResults(null);
  }

  const peers = (tau?.peers ?? []).map((p) => ({
    destination: p.destination,
    name: p.alias || p.name,
  }));

  const posts = searchResults ?? feed.posts;
  const showComposer = view === "feed" && !threadHash;

  return (
    <div className="mx-auto w-full max-w-[600px]">
      <header className="sticky top-0 z-10 border-b border-zinc-800 bg-zinc-950/95 px-3 py-3 backdrop-blur sm:px-4">
        <div className="flex items-baseline justify-between gap-2">
          <h1 className="text-lg font-bold text-zinc-50">
            <span className="mr-2 text-cyan-400">τ</span>Social
          </h1>
          <span className="truncate text-xs text-zinc-500">
            {tau?.node ? `as ${tau.node.name}` : "Waiting for the node…"}
          </span>
        </div>
        <form
          className="mt-2 flex gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            void runSearch(searchQuery);
          }}
        >
          <input
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="Search posts…"
            aria-label="Search posts"
            className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
          />
          <button
            type="submit"
            disabled={searching}
            className="shrink-0 rounded-full bg-zinc-800 px-3 py-1.5 text-xs font-semibold text-zinc-200 hover:bg-zinc-700 disabled:opacity-50"
          >
            {searching ? "…" : "Search"}
          </button>
          {searchResults && (
            <button
              type="button"
              onClick={clearFilters}
              className="shrink-0 rounded-full border border-zinc-700 px-3 py-1.5 text-xs text-zinc-300"
            >
              Clear
            </button>
          )}
        </form>
        {(tagFilter || listFilter || searchResults) && (
          <p className="mt-2 text-xs text-zinc-400">
            {tagFilter && (
              <>
                Tag <span className="text-cyan-400">#{tagFilter}</span>{" "}
                <button type="button" onClick={clearFilters} className="text-zinc-300 underline">
                  clear
                </button>
              </>
            )}
            {listFilter && (
              <>
                List <span className="text-cyan-400">{listFilter}</span>{" "}
                <button type="button" onClick={clearFilters} className="text-zinc-300 underline">
                  clear
                </button>
              </>
            )}
            {searchResults && (
              <>
                {" "}
                {searchResults.length} result{searchResults.length === 1 ? "" : "s"} for “
                {searchQuery}”
              </>
            )}
          </p>
        )}
        {lists.lists.length > 0 && (
          <div className="mt-2 flex items-center gap-2 text-xs">
            <label htmlFor="tan-list-filter" className="shrink-0 text-zinc-500">
              List
            </label>
            <select
              id="tan-list-filter"
              aria-label="Filter feed by list"
              value={listFilter ?? ""}
              onChange={(e) => setListFilter(e.target.value || null)}
              className="min-w-0 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none focus:border-cyan-500"
            >
              <option value="">All followed</option>
              {lists.lists.map((l) => (
                <option key={l.name} value={l.name}>
                  {l.name} ({l.members})
                </option>
              ))}
            </select>
            <span className="truncate text-zinc-500">
              <button
                type="button"
                onClick={() => setView("lists")}
                className="text-cyan-400 hover:underline"
              >
                Manage lists
              </button>
            </span>
          </div>
        )}
        <p className="mt-0.5 text-xs text-zinc-500">
          The Tan feed lives here — posts, replies, likes, polls and follows from across the mesh.
        </p>
      </header>

      {threadHash ? (
        <div>
          <div className="flex items-center gap-2 border-b border-zinc-800 px-3 py-2">
            <button
              type="button"
              onClick={() => {
                setThreadHash(null);
                setReplyTo(null);
              }}
              className="rounded-full px-2 py-1 text-sm text-cyan-400 hover:bg-zinc-900"
            >
              ← Back
            </button>
            <h2 className="text-sm font-bold text-zinc-100">Thread</h2>
          </div>
          <TanComposer
            composer={composer}
            replyTo={replyTo?.hash ?? threadHash}
            replyToName={replyTo?.name ?? thread.root?.name}
            onCancelReply={() => setReplyTo(null)}
            onPosted={() => {
              setReplyTo(null);
              void thread.refresh();
              void feed.refresh();
            }}
            placeholder="Reply to this post…"
          />
          {thread.loading && (
            <p className="px-4 py-6 text-sm text-zinc-500">Loading thread…</p>
          )}
          {thread.error && (
            <p role="alert" className="px-4 py-6 text-sm text-red-400">
              {thread.error}
            </p>
          )}
          {!thread.loading && thread.posts.length === 0 && !thread.error && (
            <p className="px-4 py-6 text-sm text-zinc-500">No posts in this thread.</p>
          )}
          {thread.posts.map((p) => (
            <PostCard
              key={p.hash}
              post={p}
              actions={feed}
              peers={peers}
              highlighted={p.hash === threadHash}
              onOpenThread={openThread}
              onReply={openReply}
              onShowReactors={showReactors}
              onTag={(t) => {
                setTagFilter(t);
                setThreadHash(null);
                setView("feed");
              }}
            />
          ))}
        </div>
      ) : (
        <>
          <nav
            aria-label="Feed tabs"
            className="flex gap-1 overflow-x-auto border-b border-zinc-800 px-2 py-1"
          >
            {FEED_TABS.map((t) => (
              <button
                key={t.id}
                type="button"
                aria-pressed={view === "feed" && tab === t.id}
                onClick={() => {
                  setTab(t.id);
                  setView("feed");
                  setSearchResults(null);
                }}
                className={`shrink-0 rounded-full px-3 py-1.5 text-sm font-semibold ${
                  view === "feed" && tab === t.id
                    ? "bg-cyan-500 text-zinc-950"
                    : "text-zinc-400 hover:bg-zinc-900 hover:text-zinc-100"
                }`}
              >
                {t.label}
              </button>
            ))}
            <button
              type="button"
              aria-pressed={view === "discover"}
              onClick={() => setView("discover")}
              className={`shrink-0 rounded-full px-3 py-1.5 text-sm font-semibold ${
                view === "discover"
                  ? "bg-cyan-500 text-zinc-950"
                  : "text-zinc-400 hover:bg-zinc-900 hover:text-zinc-100"
              }`}
            >
              Discover
            </button>
            <button
              type="button"
              aria-pressed={view === "followers"}
              onClick={() => setView("followers")}
              className={`shrink-0 rounded-full px-3 py-1.5 text-sm font-semibold ${
                view === "followers"
                  ? "bg-cyan-500 text-zinc-950"
                  : "text-zinc-400 hover:bg-zinc-900 hover:text-zinc-100"
              }`}
            >
              Followers
            </button>
            <button
              type="button"
              aria-pressed={view === "lists"}
              onClick={() => setView("lists")}
              className={`shrink-0 rounded-full px-3 py-1.5 text-sm font-semibold ${
                view === "lists"
                  ? "bg-cyan-500 text-zinc-950"
                  : "text-zinc-400 hover:bg-zinc-900 hover:text-zinc-100"
              }`}
            >
              Lists
            </button>
            {LIBRARY_TABS.map((t) => (
              <button
                key={t.id}
                type="button"
                aria-pressed={view === t.id}
                onClick={() => setView(t.id)}
                className={`shrink-0 rounded-full px-3 py-1.5 text-sm font-semibold ${
                  view === t.id
                    ? "bg-cyan-500 text-zinc-950"
                    : "text-zinc-400 hover:bg-zinc-900 hover:text-zinc-100"
                }`}
              >
                {t.label}
              </button>
            ))}
          </nav>

          {showComposer && (
            <TanComposer
              composer={composer}
              onPosted={() => void feed.refresh()}
            />
          )}

          {notice && (
            <p role="status" className="border-b border-zinc-800 px-4 py-2 text-xs text-cyan-300">
              {notice}
            </p>
          )}

          {view === "feed" && (
            <div>
              {feed.loading && posts.length === 0 && (
                <p className="px-4 py-6 text-sm text-zinc-500">Loading posts…</p>
              )}
              {feed.error && (
                <p role="alert" className="px-4 py-6 text-sm text-red-400">
                  {feed.error}
                </p>
              )}
              {!feed.loading && posts.length === 0 && !feed.error && (
                <p className="px-4 py-6 text-sm text-zinc-500">
                  {searchResults
                    ? "No posts match that search."
                    : tagFilter
                      ? `No posts under #${tagFilter} yet.`
                      : listFilter
                        ? `No posts from “${listFilter}” yet.`
                        : tab === "following"
                          ? "Nothing from people you follow yet — try All, or Discover someone to follow."
                          : "No posts yet."}
                </p>
              )}
              {posts.map((p) => (
                <PostCard
                  key={p.hash}
                  post={p}
                  actions={feed}
                  peers={peers}
                  onOpenThread={openThread}
                  onReply={openReply}
                  onShowReactors={showReactors}
                  onTag={(t) => {
                    setTagFilter(t);
                    setSearchResults(null);
                  }}
                />
              ))}
              {posts.length > 0 && (
                <button
                  type="button"
                  onClick={() => void feed.refresh()}
                  className="mx-auto my-4 block rounded-full border border-zinc-700 px-4 py-1.5 text-xs font-semibold text-zinc-300 hover:bg-zinc-900"
                >
                  Refresh feed
                </button>
              )}
            </div>
          )}

          {view === "discover" && (
            <DiscoverView
              lists={lists}
              authors={authors}
              followedTags={followedTags}
              onFollowAuthor={async (a) => {
                await feed.follow(a);
                setNotice("Followed.");
                void lists.refresh();
              }}
              onUnfollowAuthor={async (a) => {
                await feed.unfollow(a);
                void lists.refresh();
              }}
              onTag={(t) => {
                setTagFilter(t);
                setView("feed");
              }}
              onFollowTag={async (t) => {
                await lists.followTag(t);
                await refreshFollowedTags();
              }}
              onUnfollowTag={async (t) => {
                await lists.unfollowTag(t);
                await refreshFollowedTags();
              }}
              onMuteTag={async (t) => {
                await lists.muteTag(t);
              }}
            />
          )}

          {view === "followers" && (
            <FollowersView
              lists={lists}
              onFollowAuthor={async (a) => {
                await feed.follow(a);
                setNotice("Followed.");
                void lists.refresh();
              }}
              onUnfollowAuthor={async (a) => {
                await feed.unfollow(a);
                void lists.refresh();
              }}
            />
          )}

          {view === "lists" && (
            <ListsView
              lists={lists}
              onViewFeed={(name) => {
                setListFilter(name);
                setView("feed");
              }}
              onDeleteList={async (name) => {
                if (listFilter === name) setListFilter(null);
                await lists.deleteList(name);
                setNotice(`Deleted list “${name}”.`);
              }}
            />
          )}

          {view === "saved" && (
            <LibraryList
              title="Saved posts"
              posts={lists.saved}
              empty="No saved posts yet — save one from its ⋯ menu."
              actions={feed}
              peers={peers}
              onOpenThread={openThread}
              onReply={openReply}
              onShowReactors={showReactors}
              onTag={setTagFilter}
            />
          )}
          {view === "hidden" && (
            <LibraryList
              title="Hidden posts"
              posts={lists.hidden}
              empty="Nothing hidden."
              actions={feed}
              peers={peers}
              onOpenThread={openThread}
              onReply={openReply}
              onShowReactors={showReactors}
              onTag={setTagFilter}
              onUnhide={async (hash) => {
                await lists.unhide(hash);
              }}
            />
          )}
          {view === "pinned" && (
            <LibraryList
              title="Pinned posts"
              posts={lists.pinned}
              empty="No pinned posts yet."
              actions={feed}
              peers={peers}
              onOpenThread={openThread}
              onReply={openReply}
              onShowReactors={showReactors}
              onTag={setTagFilter}
            />
          )}
          {view === "scheduled" && (
            <ScheduledView
              scheduled={lists.scheduled}
              onCancel={async (id) => {
                await cancelScheduledTanPost(id);
                await lists.refresh();
              }}
            />
          )}
          {view === "muted" && (
            <MutedView
              lists={lists}
              keywordInput={keywordInput}
              setKeywordInput={setKeywordInput}
            />
          )}
        </>
      )}

      {reactors && (
        <div
          role="dialog"
          aria-label={`${reactors.kind} for post`}
          className="fixed inset-x-0 bottom-0 z-20 mx-auto max-h-[70vh] w-full max-w-[600px] overflow-y-auto rounded-t-2xl border border-zinc-700 bg-zinc-950 p-4 shadow-2xl"
        >
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-bold text-zinc-100 capitalize">
              {reactors.kind === "likes"
                ? "Liked by"
                : reactors.kind === "reposts"
                  ? "Reposted by"
                  : "Quoted by"}
            </h2>
            <button
              type="button"
              onClick={() => setReactors(null)}
              aria-label="Close reactors list"
              className="rounded-full px-2 py-1 text-zinc-500 hover:text-zinc-200"
            >
              ✕
            </button>
          </div>
          {reactors.loading && <p className="py-4 text-sm text-zinc-500">Loading…</p>}
          {!reactors.loading && !reactors.data && (
            <p className="py-4 text-sm text-zinc-500">Couldn&apos;t load this list.</p>
          )}
          {reactors.data?.likers?.map((r) => (
            <ReactorRow key={r.author} name={r.name} author={r.author} at={r.liked_at} />
          ))}
          {reactors.data?.reposters?.map((r) => (
            <ReactorRow key={r.author} name={r.name} author={r.author} at={r.reposted_at} />
          ))}
          {reactors.data?.quoters?.map((r) => (
            <div key={r.hash} className="flex gap-3 border-b border-zinc-800 py-3">
              <Avatar name={r.name} seed={r.author} size="sm" />
              <div className="min-w-0">
                <p className="text-sm font-semibold text-zinc-100">
                  {r.name || shortHash(r.author)}{" "}
                  <span className="font-normal text-zinc-500">· {formatRelative(r.quoted_at)}</span>
                </p>
                <p className="text-sm whitespace-pre-wrap text-zinc-300">{r.text}</p>
              </div>
            </div>
          ))}
          {!reactors.loading &&
            reactors.data &&
            (reactors.data.likers?.length ?? 0) +
              (reactors.data.reposters?.length ?? 0) +
              (reactors.data.quoters?.length ?? 0) ===
              0 && <p className="py-4 text-sm text-zinc-500">Nobody yet.</p>}
        </div>
      )}
    </div>
  );
}

function ReactorRow({ name, author, at }: { name: string; author: string; at: number }) {
  return (
    <div className="flex items-center gap-3 border-b border-zinc-800 py-3">
      <Avatar name={name} seed={author} size="sm" />
      <p className="min-w-0 text-sm font-semibold text-zinc-100">
        {name || shortHash(author)}{" "}
        <span className="font-normal text-zinc-500">· {formatRelative(at)}</span>
      </p>
    </div>
  );
}

function FollowersView({
  lists,
  onFollowAuthor,
  onUnfollowAuthor,
}: {
  lists: ReturnType<typeof useTanLists>;
  onFollowAuthor: (author: string) => Promise<void>;
  onUnfollowAuthor: (author: string) => Promise<void>;
}) {
  // Followers are derived from stored follow lists only, so the list
  // is empty until a sync has fetched at least one peer's list.
  const following = new Set(lists.following.map((f) => f.author));
  return (
    <div className="px-3 py-4 sm:px-4">
      <h2 className="text-sm font-bold text-zinc-100">Followers</h2>
      <p className="py-1 text-xs text-zinc-500">
        Authors whose published follow list names you, from lists this node has fetched.
      </p>
      {lists.followers.length === 0 && (
        <p className="py-2 text-sm text-zinc-500">
          No followers yet — sync to fetch follow lists.
        </p>
      )}
      {lists.followers.map((a) => (
        <AuthorRow
          key={a.author}
          author={a}
          followed={following.has(a.author)}
          onFollow={() => void onFollowAuthor(a.author)}
          onUnfollow={() => void onUnfollowAuthor(a.author)}
        />
      ))}
    </div>
  );
}

function ListsView({
  lists,
  onViewFeed,
  onDeleteList,
}: {
  lists: ReturnType<typeof useTanLists>;
  onViewFeed: (name: string) => void;
  onDeleteList: (name: string) => Promise<void>;
}) {
  const [newName, setNewName] = useState("");
  const [expanded, setExpanded] = useState<string | null>(null);
  const [members, setMembers] = useState<TanListMember[]>([]);
  const [membersLoading, setMembersLoading] = useState(false);
  const [membersError, setMembersError] = useState<string | null>(null);
  const [addPick, setAddPick] = useState("");
  const [addText, setAddText] = useState("");
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function loadMembers(name: string) {
    setMembersLoading(true);
    setMembersError(null);
    try {
      setMembers(await lists.listMembers(name));
    } catch (err) {
      setMembersError(err instanceof Error ? err.message : String(err));
    } finally {
      setMembersLoading(false);
    }
  }

  function toggleMembers(name: string) {
    if (expanded === name) {
      setExpanded(null);
      return;
    }
    setExpanded(name);
    setAddPick("");
    setAddText("");
    void loadMembers(name);
  }

  async function createList() {
    const name = newName.trim();
    if (!name) {
      setError("Give the list a name.");
      return;
    }
    setError(null);
    try {
      await lists.createList(name);
      setNewName("");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  async function deleteList(name: string) {
    setError(null);
    try {
      await onDeleteList(name);
      setConfirmDelete(null);
      if (expanded === name) setExpanded(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  async function addMember(name: string) {
    const target = addText.trim() || addPick;
    if (!target) {
      setError("Pick a followed author or enter an author name or hash.");
      return;
    }
    setError(null);
    try {
      await lists.addToList(name, target);
      setAddText("");
      setAddPick("");
      await loadMembers(name);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  async function removeMember(name: string, author: string) {
    setError(null);
    try {
      await lists.removeFromList(name, author);
      await loadMembers(name);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  const memberAuthors = new Set(members.map((m) => m.author));
  const addable = lists.following.filter((a) => !memberAuthors.has(a.author));

  return (
    <div className="px-3 py-4 sm:px-4">
      <h2 className="text-sm font-bold text-zinc-100">Tan lists</h2>
      <p className="py-1 text-xs text-zinc-500">
        Curate followed authors into named lists, then filter the feed by one. Lists are
        local-only — they never publish or sync.
      </p>
      {error && (
        <p role="alert" className="py-2 text-sm text-red-400">
          {error}
        </p>
      )}
      <form
        className="mt-2 flex gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          void createList();
        }}
      >
        <input
          value={newName}
          onChange={(e) => setNewName(e.target.value)}
          placeholder="New list name…"
          aria-label="New list name"
          className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
        />
        <button
          type="submit"
          className="shrink-0 rounded-full bg-cyan-500 px-3 py-1.5 text-xs font-bold text-zinc-950 hover:bg-cyan-400"
        >
          Create list
        </button>
      </form>

      {lists.lists.length === 0 && (
        <p className="py-2 text-sm text-zinc-500">
          No lists yet — create one above, then add followed authors to it.
        </p>
      )}
      {lists.lists.map((l) => (
        <div key={l.name} className="border-b border-zinc-800 py-3">
          <div className="flex items-center gap-2">
            <p className="min-w-0 flex-1 truncate text-sm font-semibold text-zinc-100">
              {l.name}{" "}
              <span className="font-normal text-zinc-500">
                · {l.members} member{l.members === 1 ? "" : "s"}
              </span>
            </p>
            <button
              type="button"
              onClick={() => toggleMembers(l.name)}
              aria-expanded={expanded === l.name}
              className="shrink-0 rounded-full border border-zinc-700 px-3 py-1 text-xs text-zinc-300 hover:bg-zinc-900"
            >
              {expanded === l.name ? "Hide members" : "Members"}
            </button>
            <button
              type="button"
              onClick={() => onViewFeed(l.name)}
              className="shrink-0 rounded-full border border-zinc-700 px-3 py-1 text-xs text-zinc-300 hover:bg-zinc-900"
            >
              View feed
            </button>
            {confirmDelete === l.name ? (
              <>
                <button
                  type="button"
                  onClick={() => void deleteList(l.name)}
                  className="shrink-0 rounded-full bg-red-500 px-3 py-1 text-xs font-bold text-white hover:bg-red-400"
                >
                  Confirm delete
                </button>
                <button
                  type="button"
                  onClick={() => setConfirmDelete(null)}
                  className="shrink-0 rounded-full border border-zinc-700 px-3 py-1 text-xs text-zinc-300 hover:bg-zinc-900"
                >
                  Cancel
                </button>
              </>
            ) : (
              <button
                type="button"
                onClick={() => setConfirmDelete(l.name)}
                className="shrink-0 rounded-full border border-red-800 px-3 py-1 text-xs text-red-400 hover:bg-red-950"
              >
                Delete
              </button>
            )}
          </div>

          {expanded === l.name && (
            <div className="mt-3">
              {membersLoading && <p className="py-2 text-sm text-zinc-500">Loading members…</p>}
              {membersError && (
                <p role="alert" className="py-2 text-sm text-red-400">
                  {membersError}
                </p>
              )}
              {!membersLoading && !membersError && members.length === 0 && (
                <p className="py-2 text-sm text-zinc-500">No members yet.</p>
              )}
              {members.map((m) => (
                <div key={m.author} className="flex items-center gap-3 border-b border-zinc-800 py-2">
                  <Avatar name={m.name} seed={m.author} size="sm" />
                  <div className="min-w-0 flex-1">
                    <p className="truncate text-sm font-semibold text-zinc-100">
                      {m.name || shortHash(m.author)}
                    </p>
                    <p className="text-xs text-zinc-500">
                      {m.posts !== undefined
                        ? `${m.posts} post${m.posts === 1 ? "" : "s"}`
                        : `added ${formatRelative(m.added_at)}`}
                    </p>
                  </div>
                  <button
                    type="button"
                    onClick={() => void removeMember(l.name, m.author)}
                    aria-label={`Remove ${m.name || shortHash(m.author)} from ${l.name}`}
                    className="shrink-0 rounded-full border border-zinc-700 px-3 py-1 text-xs text-zinc-300 hover:bg-zinc-900"
                  >
                    Remove
                  </button>
                </div>
              ))}
              <form
                className="mt-3 flex flex-wrap gap-2"
                onSubmit={(e) => {
                  e.preventDefault();
                  void addMember(l.name);
                }}
              >
                {addable.length > 0 && (
                  <select
                    value={addPick}
                    onChange={(e) => setAddPick(e.target.value)}
                    aria-label={`Add a followed author to ${l.name}`}
                    className="min-w-0 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none focus:border-cyan-500"
                  >
                    <option value="">Followed authors…</option>
                    {addable.map((a) => (
                      <option key={a.author} value={a.author}>
                        {a.name || shortHash(a.author)}
                      </option>
                    ))}
                  </select>
                )}
                <input
                  value={addText}
                  onChange={(e) => setAddText(e.target.value)}
                  placeholder="Or enter an author name or hash…"
                  aria-label={`Author name or hash to add to ${l.name}`}
                  className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
                />
                <button
                  type="submit"
                  className="shrink-0 rounded-full bg-zinc-800 px-3 py-1.5 text-xs font-semibold text-zinc-200 hover:bg-zinc-700"
                >
                  Add member
                </button>
              </form>
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

function DiscoverView({
  lists,
  authors,
  followedTags,
  onFollowAuthor,
  onUnfollowAuthor,
  onTag,
  onFollowTag,
  onUnfollowTag,
  onMuteTag,
}: {
  lists: ReturnType<typeof useTanLists>;
  authors: TanAuthor[];
  followedTags: string[];
  onFollowAuthor: (author: string) => Promise<void>;
  onUnfollowAuthor: (author: string) => Promise<void>;
  onTag: (tag: string) => void;
  onFollowTag: (tag: string) => Promise<void>;
  onUnfollowTag: (tag: string) => Promise<void>;
  onMuteTag: (tag: string) => Promise<void>;
}) {
  const following = new Set(lists.following.map((f) => f.author));
  return (
    <div className="px-3 py-4 sm:px-4">
      <h2 className="text-sm font-bold text-zinc-100">Suggested authors</h2>
      {lists.suggestions.length === 0 && (
        <p className="py-2 text-sm text-zinc-500">No suggestions right now.</p>
      )}
      {lists.suggestions.map((a) => (
        <AuthorRow
          key={a.author}
          author={a}
          followed={following.has(a.author)}
          onFollow={() => void onFollowAuthor(a.author)}
          onUnfollow={() => void onUnfollowAuthor(a.author)}
        />
      ))}

      <h2 className="mt-6 text-sm font-bold text-zinc-100">Tags</h2>
      {lists.tags.length === 0 && (
        <p className="py-2 text-sm text-zinc-500">No tags yet.</p>
      )}
      <div className="mt-2 flex flex-wrap gap-2">
        {lists.tags.map((t) => {
          const followed = followedTags.includes(t.tag);
          return (
            <span
              key={t.tag}
              className="inline-flex items-center gap-1 rounded-full border border-zinc-700 px-2 py-1 text-xs text-zinc-300"
            >
              <button
                type="button"
                onClick={() => onTag(t.tag)}
                className="font-semibold text-cyan-400 hover:underline"
              >
                #{t.tag}
              </button>
              <span className="text-zinc-500">{t.count}</span>
              <button
                type="button"
                onClick={() => void (followed ? onUnfollowTag(t.tag) : onFollowTag(t.tag))}
                className="text-zinc-400 hover:text-zinc-100"
              >
                {followed ? "Unfollow" : "Follow"}
              </button>
              <button
                type="button"
                onClick={() => void onMuteTag(t.tag)}
                className="text-zinc-500 hover:text-zinc-200"
                aria-label={`Mute tag ${t.tag}`}
              >
                🔇
              </button>
            </span>
          );
        })}
      </div>

      <h2 className="mt-6 text-sm font-bold text-zinc-100">All authors</h2>
      {authors.length === 0 && (
        <p className="py-2 text-sm text-zinc-500">No authors found.</p>
      )}
      {authors.map((a) => (
        <AuthorRow
          key={a.author}
          author={a}
          followed={following.has(a.author)}
          onFollow={() => void onFollowAuthor(a.author)}
          onUnfollow={() => void onUnfollowAuthor(a.author)}
        />
      ))}
    </div>
  );
}

function AuthorRow({
  author,
  followed,
  onFollow,
  onUnfollow,
}: {
  author: TanAuthor;
  followed: boolean;
  onFollow: () => void;
  onUnfollow: () => void;
}) {
  return (
    <div className="flex items-center gap-3 border-b border-zinc-800 py-3">
      <Avatar name={author.name} seed={author.author} size="sm" />
      <div className="min-w-0 flex-1">
        <p className="truncate text-sm font-semibold text-zinc-100">
          {author.name || shortHash(author.author)}
        </p>
        <p className="text-xs text-zinc-500">
          {author.posts} post{author.posts === 1 ? "" : "s"}
          {author.last_post_at ? ` · last ${formatRelative(author.last_post_at)}` : ""}
        </p>
      </div>
      <button
        type="button"
        onClick={followed ? onUnfollow : onFollow}
        className={`shrink-0 rounded-full px-3 py-1 text-xs font-bold ${
          followed
            ? "border border-zinc-700 text-zinc-300"
            : "bg-cyan-500 text-zinc-950 hover:bg-cyan-400"
        }`}
      >
        {followed ? "Following" : "Follow"}
      </button>
    </div>
  );
}

function LibraryList({
  title,
  posts,
  empty,
  actions,
  peers,
  onOpenThread,
  onReply,
  onShowReactors,
  onTag,
  onUnhide,
}: {
  title: string;
  posts: TanPost[];
  empty: string;
  actions: ReturnType<typeof useTanFeed>;
  peers: { destination: string; name: string }[];
  onOpenThread: (post: TanPost) => void;
  onReply: (post: TanPost) => void;
  onShowReactors: (kind: ReactorKind, post: TanPost) => void;
  onTag: (tag: string) => void;
  onUnhide?: (hash: string) => Promise<void>;
}) {
  return (
    <div>
      <h2 className="border-b border-zinc-800 px-4 py-3 text-sm font-bold text-zinc-100">{title}</h2>
      {posts.length === 0 && <p className="px-4 py-6 text-sm text-zinc-500">{empty}</p>}
      {posts.map((p) => (
        <div key={p.hash}>
          <PostCard
            post={p}
            actions={actions}
            peers={peers}
            onOpenThread={onOpenThread}
            onReply={onReply}
            onShowReactors={onShowReactors}
            onTag={onTag}
          />
          {onUnhide && (
            <div className="border-b border-zinc-800 px-4 pb-3">
              <button
                type="button"
                onClick={() => void onUnhide(p.hash)}
                className="rounded-full border border-zinc-700 px-3 py-1 text-xs text-zinc-300 hover:bg-zinc-900"
              >
                Unhide
              </button>
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

function ScheduledView({
  scheduled,
  onCancel,
}: {
  scheduled: ReturnType<typeof useTanLists>["scheduled"];
  onCancel: (id: string) => Promise<void>;
}) {
  return (
    <div>
      <h2 className="border-b border-zinc-800 px-4 py-3 text-sm font-bold text-zinc-100">
        Scheduled posts
      </h2>
      {scheduled.length === 0 && (
        <p className="px-4 py-6 text-sm text-zinc-500">
          Nothing scheduled — pick “Send later” in the composer to queue a post.
        </p>
      )}
      {scheduled.map((s) => (
        <div key={s.id} className="border-b border-zinc-800 px-4 py-3">
          <p className="text-sm whitespace-pre-wrap text-zinc-200">{s.text}</p>
          <p className="mt-1 text-xs text-zinc-500">
            Publishes {formatDateTime(s.publish_at)}
            {s.paused_at ? " · paused" : ""}
            {s.attempts > 0 ? ` · ${s.attempts} attempt${s.attempts === 1 ? "" : "s"}` : ""}
          </p>
          <button
            type="button"
            onClick={() => void onCancel(s.id)}
            className="mt-2 rounded-full border border-red-800 px-3 py-1 text-xs text-red-400 hover:bg-red-950"
          >
            Cancel scheduled post
          </button>
        </div>
      ))}
    </div>
  );
}

function MutedView({
  lists,
  keywordInput,
  setKeywordInput,
}: {
  lists: ReturnType<typeof useTanLists>;
  keywordInput: string;
  setKeywordInput: (v: string) => void;
}) {
  return (
    <div className="px-3 py-4 sm:px-4">
      <h2 className="text-sm font-bold text-zinc-100">Muted authors</h2>
      {lists.mutedAuthors.length === 0 && (
        <p className="py-2 text-sm text-zinc-500">No muted authors.</p>
      )}
      {lists.mutedAuthors.map((a) => (
        <div key={a.author} className="flex items-center gap-3 border-b border-zinc-800 py-3">
          <Avatar name={a.name} seed={a.author} size="sm" />
          <p className="min-w-0 flex-1 truncate text-sm text-zinc-200">
            {a.name || shortHash(a.author)}
          </p>
          <button
            type="button"
            onClick={() => void lists.unmuteAuthor(a.author)}
            className="shrink-0 rounded-full border border-zinc-700 px-3 py-1 text-xs text-zinc-300 hover:bg-zinc-900"
          >
            Unmute
          </button>
        </div>
      ))}

      <h2 className="mt-6 text-sm font-bold text-zinc-100">Muted keywords</h2>
      <form
        className="mt-2 flex gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          const kw = keywordInput.trim();
          if (kw) {
            void lists.muteKeyword(kw).then(() => setKeywordInput(""));
          }
        }}
      >
        <input
          value={keywordInput}
          onChange={(e) => setKeywordInput(e.target.value)}
          placeholder="Mute a keyword…"
          aria-label="Mute a keyword"
          className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
        />
        <button
          type="submit"
          className="shrink-0 rounded-full bg-zinc-800 px-3 py-1.5 text-xs font-semibold text-zinc-200 hover:bg-zinc-700"
        >
          Mute
        </button>
      </form>
      {lists.mutedKeywords.length === 0 && (
        <p className="py-2 text-sm text-zinc-500">No muted keywords.</p>
      )}
      <div className="mt-2 flex flex-wrap gap-2">
        {lists.mutedKeywords.map((kw) => (
          <span
            key={kw}
            className="inline-flex items-center gap-2 rounded-full border border-zinc-700 px-2 py-1 text-xs text-zinc-300"
          >
            {kw}
            <button
              type="button"
              onClick={() => void lists.unmuteKeyword(kw)}
              aria-label={`Unmute keyword ${kw}`}
              className="text-zinc-500 hover:text-zinc-200"
            >
              ✕
            </button>
          </span>
        ))}
      </div>

      <h2 className="mt-6 text-sm font-bold text-zinc-100">Muted tags</h2>
      {lists.mutedTags.length === 0 && (
        <p className="py-2 text-sm text-zinc-500">No muted tags.</p>
      )}
      <div className="mt-2 flex flex-wrap gap-2">
        {lists.mutedTags.map((tag) => (
          <span
            key={tag}
            className="inline-flex items-center gap-2 rounded-full border border-zinc-700 px-2 py-1 text-xs text-zinc-300"
          >
            #{tag}
            <button
              type="button"
              onClick={() => void lists.unmuteTag(tag)}
              aria-label={`Unmute tag ${tag}`}
              className="text-zinc-500 hover:text-zinc-200"
            >
              ✕
            </button>
          </span>
        ))}
      </div>
    </div>
  );
}
