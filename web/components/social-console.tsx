"use client";

import Link from "next/link";
import { useEffect, useRef, useState } from "react";

import { formatDateTime, shortHash } from "@/lib/format";
import type {
  TanAuthor,
  TanExportDocument,
  TanImportError,
  TanImportResult,
  TanLiker,
  TanPost,
  TanSuggestion,
  TanVerifyReport,
} from "@/lib/types";
import { useTan } from "@/lib/use-tan";

import { CopyButton, SectionLabel, SendIcon, StatusDot } from "./ui";
import { RulesSection } from "./rules-section";

export function SocialConsole() {
  const tan = useTan();
  const [text, setText] = useState("");
  const [target, setTarget] = useState("");
  const [keyword, setKeyword] = useState("");
  const [searchText, setSearchText] = useState("");
  const [replyTarget, setReplyTarget] = useState<TanPost | null>(null);
  const [quoteTarget, setQuoteTarget] = useState<TanPost | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  // Audit failures render beside the verify button, not under the
  // post composer: the shared error state is too far from the action.
  const [verifyError, setVerifyError] = useState<string | null>(null);
  // Whether the feed shows the saved list instead of the timeline.
  // Saved posts load on demand, not on the feed's poll tick.
  const [showSaved, setShowSaved] = useState(false);
  useEffect(() => {
    if (!showSaved) return;
    tan.loadSaved().catch((caught) => {
      setError(
        caught instanceof Error ? caught.message : "could not load saved posts",
      );
    });
  }, [showSaved, tan.loadSaved]);
  // Whether the feed shows the pinned list instead of the timeline.
  // Pinned posts load on demand, not on the feed's poll tick.
  const [showPinned, setShowPinned] = useState(false);
  useEffect(() => {
    if (!showPinned) return;
    tan.loadPinned().catch((caught) => {
      setError(
        caught instanceof Error ? caught.message : "could not load pinned posts",
      );
    });
  }, [showPinned, tan.loadPinned]);
  // Hashes with a delete request in flight. While one is pending the card
  // shows a disabled state instead of the confirm button, so a second
  // click cannot report the already-deleted post as a failure.
  const [deleting, setDeleting] = useState<ReadonlySet<string>>(
    () => new Set(),
  );
  // Hashes with a repost request in flight; see submitRepost.
  const repostingRef = useRef<Set<string>>(new Set());
  // Hash of the post whose liker list is open, if any, and the likers for
  // it (null while the list is loading). Only one list is open at a time,
  // and it is always fetched fresh on open so a like toggle cannot leave
  // a stale list behind.
  const [likersOpen, setLikersOpen] = useState<string | null>(null);
  const [likers, setLikers] = useState<TanLiker[] | null>(null);
  const [likersTotal, setLikersTotal] = useState(0);
  // Bumped on every open, close, or switch of the liker list. A pending
  // fetch or like completion applies only if the generation it started
  // under is still current, so superseded results never reach the UI.
  const likersGeneration = useRef(0);

  function closeLikers() {
    likersGeneration.current += 1;
    setLikersOpen(null);
    setLikers(null);
    setLikersTotal(0);
  }
  // Author hashes with a mute request in flight; same guard as deleting.
  const [muting, setMuting] = useState<ReadonlySet<string>>(() => new Set());
  // The feed shows the timeline or, when the mentions view is selected,
  // the replies to your own posts instead.
  const shown = tan.showMentions ? tan.mentions : tan.posts;
  async function submitPost() {
    if (!text.trim()) return;
    setError(null);
    try {
      if (quoteTarget) {
        const submittedHash = quoteTarget.hash;
        const submittedText = text.trim();
        await tan.quote(submittedHash, submittedText);
        // Clear only the composer state that still belongs to this quote, so
        // a target or draft changed while the request was in flight survives.
        setText((current) => (current.trim() === submittedText ? "" : current));
        setQuoteTarget((current) =>
          current?.hash === submittedHash ? null : current,
        );
        return;
      }
      await tan.publish(text.trim(), replyTarget?.hash);
      setText("");
      setReplyTarget(null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "could not publish");
    }
  }

  async function submitFollow() {
    if (!target.trim()) return;
    setError(null);
    try {
      await tan.follow(target.trim());
      setTarget("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "could not follow");
    }
  }

  // Follow or unfollow an author from the directory. The hook reloads
  // the feed after either, so the directory and the following list
  // update together.
  async function submitToggleFollowAuthor(author: TanAuthor) {
    setError(null);
    try {
      if (author.followed) {
        await tan.unfollow(author.author);
      } else {
        await tan.follow(author.author);
      }
    } catch (caught) {
      setError(
        caught instanceof Error
          ? caught.message
          : author.followed
            ? "could not unfollow"
            : "could not follow",
      );
    }
  }

  async function submitFollowSuggestion(author: string) {
    setError(null);
    try {
      // Follow reloads the feed, so the followed author drops out of
      // the suggestions on the same round trip.
      await tan.follow(author);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "could not follow");
    }
  }

  async function submitSearch() {
    if (!searchText.trim()) return;
    setError(null);
    try {
      await tan.search(searchText.trim());
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "search failed");
    }
  }

  function clearSearch() {
    setSearchText("");
    tan.clearSearch();
  }

  async function toggleLike(post: TanPost) {
    setError(null);
    const generation = likersGeneration.current;
    const listOpenForPost = likersOpen === post.hash;
    try {
      if (post.liked) {
        await tan.unlike(post.hash);
      } else {
        await tan.like(post.hash);
      }
      // The like count changed, so an open liker list for this post is
      // stale; close it and let the count button fetch a fresh one. If
      // the list was closed or reopened while the reaction was pending,
      // leave the current list alone.
      if (listOpenForPost && likersGeneration.current === generation) {
        closeLikers();
      }
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "could not react");
    }
  }

  async function submitRepost(post: TanPost) {
    // A ref, not state: two clicks before React re-renders must not both
    // send a repost, or the second one reports the store's duplicate error.
    if (repostingRef.current.has(post.hash)) return;
    repostingRef.current.add(post.hash);
    setError(null);
    try {
      await tan.repost(post.hash);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "could not repost");
    } finally {
      repostingRef.current.delete(post.hash);
    }
  }

  async function toggleLikers(post: TanPost) {
    if (likersOpen === post.hash) {
      closeLikers();
      return;
    }
    const generation = ++likersGeneration.current;
    setLikersOpen(post.hash);
    setLikers(null);
    setLikersTotal(0);
    setError(null);
    try {
      const result = await tan.fetchLikers(post.hash);
      // A newer open supersedes this response: opening another post's
      // list first must not show this post's names under that post.
      if (likersGeneration.current !== generation) return;
      setLikers(result.likers);
      setLikersTotal(result.total);
    } catch (caught) {
      if (likersGeneration.current !== generation) return;
      setError(
        caught instanceof Error ? caught.message : "could not load likers",
      );
      closeLikers();
    }
  }

  async function toggleSave(post: TanPost) {
    setError(null);
    try {
      if (post.saved) {
        await tan.unsave(post.hash);
      } else {
        await tan.save(post.hash);
      }
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "could not save");    }
  }

  // Pins are own-posts-only, so the button only renders on your own
  // cards; a pin from another surface refreshes the same way a save does.
  async function togglePin(post: TanPost) {
    setError(null);
    try {
      if (post.pinned) {
        await tan.unpin(post.hash);
      } else {
        await tan.pin(post.hash);
      }
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "could not pin");
    }
  }

  async function submitDelete(post: TanPost) {
    if (deleting.has(post.hash)) return;
    setDeleting((previous) => new Set(previous).add(post.hash));
    setError(null);
    try {
      await tan.deletePost(post.hash);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "could not delete");
    } finally {
      setDeleting((previous) => {
        const next = new Set(previous);
        next.delete(post.hash);
        return next;
      });
    }
  }

  // Returns false when the edit failed, so the card keeps its form open
  // instead of closing on an error the user has not seen yet.
  async function submitEdit(post: TanPost, text: string): Promise<boolean> {
    setError(null);
    try {
      await tan.editPost(post.hash, text);
      return true;
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "could not edit");
      return false;
    }
  }

  async function submitSync() {
    setError(null);
    setMessage(null);
    try {
      const result = await tan.sync();
      setMessage(
        `pulled ${result.stored} new post(s) from ${result.peers} peer(s)`,
      );
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "sync failed");
    }
  }

  async function submitVerify() {
    setVerifyError(null);
    try {
      await tan.verify();
    } catch (caught) {
      setVerifyError(
        caught instanceof Error ? caught.message : "verify failed",
      );
    }
  }

  async function submitMute(post: TanPost) {
    if (muting.has(post.author)) return;
    setMuting((previous) => new Set(previous).add(post.author));
    setError(null);
    try {
      await tan.muteAuthor(post.author);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "could not mute");
    } finally {
      setMuting((previous) => {
        const next = new Set(previous);
        next.delete(post.author);
        return next;
      });
    }
  }

  async function submitUnmuteAuthor(author: string) {
    setError(null);
    try {
      await tan.unmuteAuthor(author);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "could not unmute");
    }
  }

  async function submitMuteKeyword() {
    if (!keyword.trim()) return;
    // Capture the submitted text: typing during the request must not be
    // wiped by the clear below, only the text that was actually muted.
    const submitted = keyword;
    setError(null);
    try {
      await tan.muteKeyword(submitted.trim());
      setKeyword((current) => (current === submitted ? "" : current));
    } catch (caught) {
      setError(
        caught instanceof Error ? caught.message : "could not mute keyword",
      );
    }
  }

  async function submitUnmuteKeyword(keywordValue: string) {
    setError(null);
    try {
      await tan.unmuteKeyword(keywordValue);
    } catch (caught) {
      setError(
        caught instanceof Error ? caught.message : "could not unmute keyword",
      );
    }
  }
  // While a search is active the feed shows its results instead of the
  // timeline; the following/everything toggle is a timeline filter, so it
  // hides until the search is cleared.
  const showingSearch = tan.searchQuery !== null;
  const visiblePosts = showingSearch ? tan.searchResults : tan.posts;

  return (
    <div className="relative min-h-dvh">
      <div className="mesh-grid pointer-events-none fixed inset-0" aria-hidden="true" />
      <div
        className="pointer-events-none fixed inset-x-0 top-0 h-96 bg-[radial-gradient(60%_100%_at_50%_0%,rgba(45,212,191,0.13),transparent_72%)]"
        aria-hidden="true"
      />

      <div className="relative">
        <header className="sticky top-0 z-30 border-b border-line/60 bg-void/80 backdrop-blur-xl">
          <div className="mx-auto flex w-full max-w-6xl flex-wrap items-center gap-x-5 gap-y-3 px-4 py-3 lg:px-8">
            <div className="flex items-center gap-3">
              <div className="grid h-10 w-10 place-items-center rounded-xl border border-signal/30 bg-signal/10 font-mono text-xl text-signal shadow-[inset_0_0_18px_rgba(45,212,191,0.15)]">
                τ
              </div>
              <div className="leading-tight">
                <div className="flex items-baseline gap-2">
                  <span className="font-mono text-lg font-semibold tracking-tight text-zinc-100">
                    tan
                  </span>
                  <span className="rounded border border-line bg-panel px-1.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.18em] text-mist">
                    social feed
                  </span>
                </div>
                <p className="mt-0.5 text-[11px] text-mist">
                  signed posts · hash-chained · peer-to-peer
                </p>
              </div>
            </div>

            <nav className="ml-auto flex items-center gap-1 rounded-xl border border-line/70 bg-panel/60 p-1 text-xs">
              <Link
                href="/"
                className="rounded-lg px-2.5 py-1.5 text-mist transition hover:text-zinc-100"
              >
                chat
              </Link>
              <Link
                href="/social/"
                className="rounded-lg bg-signal/15 px-2.5 py-1.5 text-signal"
              >
                social
              </Link>
            </nav>

            <span className="inline-flex items-center gap-2 rounded-full border border-signal/35 bg-signal/10 px-2.5 py-1 font-mono text-[11px] text-signal">
              <StatusDot state={tan.loading ? "connecting" : "live"} />
              {tan.loading ? "loading" : "live"}
            </span>
          </div>
        </header>

        <main className="mx-auto grid w-full max-w-6xl gap-4 px-4 py-4 lg:grid-cols-[290px_minmax(0,1fr)] lg:gap-5 lg:px-8 lg:py-6">
          <aside className="space-y-3">
            <section className="rounded-2xl border border-line/80 bg-panel/40 p-4">
              <div className="flex items-center justify-between">
                <SectionLabel>following</SectionLabel>
                <span className="font-mono text-[10px] text-mist">
                  {tan.following.length}
                </span>
              </div>
              <form
                className="mt-3 flex gap-2"
                onSubmit={(event) => {
                  event.preventDefault();
                  void submitFollow();
                }}
              >
                <input
                  value={target}
                  onChange={(event) => setTarget(event.target.value)}
                  placeholder="name or author hash"
                  spellCheck={false}
                  className="h-9 min-w-0 flex-1 rounded-lg border border-line bg-void/70 px-2.5 font-mono text-xs text-zinc-200 outline-none transition placeholder:text-mist/60 focus:border-signal/60 focus:ring-2 focus:ring-signal/20"
                />
                <button
                  type="submit"
                  disabled={!target.trim()}
                  className="rounded-lg border border-line bg-void/70 px-2.5 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal disabled:opacity-50"
                >
                  follow
                </button>
              </form>

              <ul className="mt-3 space-y-1.5">
                {tan.following.length === 0 ? (
                  <li className="rounded-lg border border-dashed border-line/70 px-3 py-2 text-[11px] leading-relaxed text-mist">
                    No authors yet. Follow a peer, then sync their feed.
                  </li>
                ) : null}
                {tan.following.map((record) => (
                  <li
                    key={record.author}
                    className="flex items-center justify-between gap-2 rounded-lg border border-line/60 bg-void/40 px-2.5 py-2"
                  >
                    <div className="min-w-0">
                      <p className="truncate text-xs text-zinc-200">
                        {record.name || "unknown"}
                      </p>
                      <p className="font-mono text-[10px] text-mist/80">
                        {shortHash(record.author, 10)} · {record.posts ?? 0} posts
                      </p>
                    </div>
                    <button
                      type="button"
                      onClick={() => void tan.unfollow(record.author)}
                      className="shrink-0 font-mono text-[10px] text-mist/70 transition hover:text-rose-300"
                      title="unfollow"
                    >
                      remove
                    </button>
                  </li>
                ))}
              </ul>
            </section>

            <section className="rounded-2xl border border-line/80 bg-panel/40 p-4">
              <div className="flex items-center justify-between">
                <SectionLabel>authors</SectionLabel>
                <span className="font-mono text-[10px] text-mist">
                  {tan.authorsTotal}
                </span>
              </div>
              <p className="mt-2 text-[11px] leading-relaxed text-mist">
                Everyone with a post in the local store. Follow an author
                to see their posts in your timeline.
              </p>
              <ul className="mt-3 space-y-1.5">
                {tan.authors.length === 0 ? (
                  <li className="rounded-lg border border-dashed border-line/70 px-3 py-2 text-[11px] leading-relaxed text-mist">
                    No authors yet. Sync to pull posts from your peers.
                  </li>
                ) : null}
                {tan.authors.map((record) => (
                  <li
                    key={record.author}
                    className="flex items-center justify-between gap-2 rounded-lg border border-line/60 bg-void/40 px-2.5 py-2"
                  >
                    <div className="min-w-0">
                      <p className="truncate text-xs text-zinc-200">
                        {record.name || "unknown"}
                      </p>
                      <p className="font-mono text-[10px] text-mist/80">
                        {shortHash(record.author, 10)} · {record.posts}{" "}
                        post{record.posts === 1 ? "" : "s"} ·{" "}
                        {record.last_post
                          ? formatDateTime(record.last_post)
                          : "never"}
                      </p>
                    </div>
                    <button
                      type="button"
                      onClick={() => void submitToggleFollowAuthor(record)}
                      className="shrink-0 font-mono text-[10px] text-mist/70 transition hover:text-signal"
                      title={
                        record.followed
                          ? "unfollow this author"
                          : "follow this author"
                      }
                    >
                      {record.followed ? "remove" : "follow"}
                    </button>
                  </li>
                ))}
              </ul>
              {tan.authors.length < tan.authorsTotal ? (
                <button
                  type="button"
                  onClick={() => void tan.loadMoreAuthors()}
                  className="mt-3 inline-flex w-full items-center justify-center rounded-lg border border-line bg-void/70 px-3 py-2 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal"
                >
                  show more ({tan.authors.length} of {tan.authorsTotal})
                </button>
              ) : null}
            </section>

            <section className="rounded-2xl border border-line/80 bg-panel/40 p-4">
              <div className="flex items-center justify-between">
                <SectionLabel>suggested authors</SectionLabel>
                <span className="font-mono text-[10px] text-mist">
                  {tan.suggestions.length}
                </span>
              </div>
              <div className="mt-3">
                <SuggestionList
                  suggestions={tan.suggestions}
                  onFollow={(author) => void submitFollowSuggestion(author)}
                />
              </div>
            </section>

            <section className="rounded-2xl border border-line/80 bg-panel/40 p-4">
              <div className="flex items-center justify-between">
                <SectionLabel>muted</SectionLabel>
                <span className="font-mono text-[10px] text-mist">
                  {tan.mutedAuthors.length + tan.mutedKeywords.length}
                </span>
              </div>
              <p className="mt-2 text-[11px] leading-relaxed text-mist">
                Muted authors and keywords stay stored but never appear in
                the timeline. Unmuting restores the full history.
              </p>
              <form
                className="mt-3 flex gap-2"
                onSubmit={(event) => {
                  event.preventDefault();
                  void submitMuteKeyword();
                }}
              >
                <input
                  value={keyword}
                  onChange={(event) => setKeyword(event.target.value)}
                  placeholder="mute a word or phrase"
                  spellCheck={false}
                  className="h-9 min-w-0 flex-1 rounded-lg border border-line bg-void/70 px-2.5 font-mono text-xs text-zinc-200 outline-none transition placeholder:text-mist/60 focus:border-signal/60 focus:ring-2 focus:ring-signal/20"
                />
                <button
                  type="submit"
                  disabled={!keyword.trim()}
                  className="rounded-lg border border-line bg-void/70 px-2.5 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal disabled:opacity-50"
                >
                  mute
                </button>
              </form>

              {tan.mutedAuthors.length === 0 &&
              tan.mutedKeywords.length === 0 ? (
                <p className="mt-3 rounded-lg border border-dashed border-line/70 px-3 py-2 text-[11px] leading-relaxed text-mist">
                  Nothing muted. Mute an author from their post, or a keyword
                  above.
                </p>
              ) : null}
              {tan.mutedAuthors.length > 0 ? (
                <ul className="mt-3 space-y-1.5">
                  {tan.mutedAuthors.map((record) => (
                    <li
                      key={record.author}
                      className="flex items-center justify-between gap-2 rounded-lg border border-line/60 bg-void/40 px-2.5 py-2"
                    >
                      <div className="min-w-0">
                        <p className="truncate text-xs text-zinc-200">
                          {record.name || "unknown"}
                        </p>
                        <p className="font-mono text-[10px] text-mist/80">
                          {shortHash(record.author, 10)} ·{" "}
                          {record.posts ?? 0} posts
                        </p>
                      </div>
                      <button
                        type="button"
                        onClick={() => void submitUnmuteAuthor(record.author)}
                        className="shrink-0 font-mono text-[10px] text-mist/70 transition hover:text-signal"
                        title="unmute this author"
                      >
                        unmute
                      </button>
                    </li>
                  ))}
                </ul>
              ) : null}
              {tan.mutedKeywords.length > 0 ? (
                <ul className="mt-3 space-y-1.5">
                  {tan.mutedKeywords.map((record) => (
                    <li
                      key={record.keyword}
                      className="flex items-center justify-between gap-2 rounded-lg border border-line/60 bg-void/40 px-2.5 py-2"
                    >
                      <p className="min-w-0 truncate font-mono text-xs text-zinc-200">
                        “{record.display}”
                      </p>
                      <button
                        type="button"
                        onClick={() => void submitUnmuteKeyword(record.display)}
                        className="shrink-0 font-mono text-[10px] text-mist/70 transition hover:text-signal"
                        title="unmute this keyword"
                      >
                        unmute
                      </button>
                    </li>
                  ))}
                </ul>
              ) : null}
            </section>

            <section className="rounded-2xl border border-line/80 bg-panel/40 p-4">
              <SectionLabel>replication</SectionLabel>
              <p className="mt-2 text-[11px] leading-relaxed text-mist">
                Pull posts cached by your peers. New posts are also pushed to
                known peers and relayed with a small fan-out.
              </p>
              <button
                type="button"
                onClick={() => void submitSync()}
                disabled={tan.syncing}
                className="mt-3 inline-flex w-full items-center justify-center rounded-lg border border-signal/40 bg-signal/10 px-3 py-2 text-xs font-medium text-signal transition hover:border-signal/70 hover:bg-signal/15 disabled:opacity-50"
              >
                {tan.syncing ? "syncing…" : "sync peers"}
              </button>
              {message ? (
                <p className="mt-2 font-mono text-[10px] text-signal/80">
                  {message}
                </p>
              ) : null}
            </section>

            <RulesSection
              rules={tan.rules}
              availableKinds={tan.availableKinds}
              loading={tan.rulesLoading}
              error={tan.rulesError}
              onUpdate={tan.updateRules}
              onRefresh={tan.refreshRules}
            />

            <section className="rounded-2xl border border-line/80 bg-panel/40 p-4">
              <SectionLabel>store health</SectionLabel>
              <p className="mt-2 text-[11px] leading-relaxed text-mist">
                Re-verify every stored post&apos;s signature and chain
                linkage, the same audit{" "}
                <code className="rounded bg-void/80 px-1 py-0.5 font-mono text-[11px] text-zinc-300">
                  tau tan verify
                </code>{" "}
                runs. Read-only: verifying never writes to the store.
              </p>
              <button
                type="button"
                onClick={() => void submitVerify()}
                disabled={tan.verifying}
                className="mt-3 inline-flex w-full items-center justify-center rounded-lg border border-signal/40 bg-signal/10 px-3 py-2 text-xs font-medium text-signal transition hover:border-signal/70 hover:bg-signal/15 disabled:opacity-50"
              >
                {tan.verifying ? "verifying…" : "verify store"}
              </button>
              {verifyError ? (
                <p className="mt-2 font-mono text-[10px] leading-relaxed text-rose-300">
                  {verifyError}
                </p>
              ) : null}
              {tan.verifyReport ? (
                <VerifyResult report={tan.verifyReport} />
              ) : null}
            </section>
            <TanBackupSection
              exporting={tan.exporting}
              importing={tan.importing}
              onExport={(allAuthors) => tan.exportTan(allAuthors)}
              onImport={(doc) => tan.importTan(doc)}
            />
          </aside>

          <div className="flex min-w-0 flex-col">
            <section className="rounded-2xl border border-line/80 bg-panel/40">
              <div className="flex flex-wrap items-center justify-between gap-2 border-b border-line/60 px-4 py-3">
                {tan.threadRef !== null ? (
                  <div className="flex items-center gap-3">
                    <button
                      type="button"
                      onClick={() => tan.closeThread()}
                      className="rounded-lg border border-line/70 bg-void/50 px-2 py-1 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal"
                    >
                      ← timeline
                    </button>
                    <SectionLabel>thread</SectionLabel>
                    <span className="font-mono text-[10px] text-mist">
                      {tan.threadPosts?.length ?? 0} post(s)
                    </span>
                  </div>

                ) : (
                  <>
                    <div className="flex items-center gap-3">
                      <SectionLabel>
                        {showingSearch
                          ? "search results"
                          : showSaved
                            ? "saved"
                          : showPinned
                            ? "pinned"
                          : tan.showMentions
                            ? "mentions"
                            : tan.view === "top"
                              ? "top posts"
                              : tan.all
                                ? "public cache"
                                : "timeline"}
                      </SectionLabel>
                      <span className="font-mono text-[10px] text-mist">
                        {showingSearch
                          ? `${visiblePosts.length} post(s)`
                          : showSaved
                            ? `${tan.saved.length} post(s)`
                          : showPinned
                            ? `${tan.pinned.length} post(s)`
                          : tan.showMentions
                            ? `${shown.length} post(s)`
                            : `${tan.posts.length} post(s)`}
                      </span>
                    </div>
                    {showingSearch ? (
                    <button
                      type="button"
                      onClick={clearSearch}
                      className="rounded-lg border border-line/70 bg-void/50 px-2 py-1 font-mono text-[11px] text-mist transition hover:text-zinc-100"
                    >
                      clear search
                    </button>
                    ) : (
                    <div className="flex items-center gap-1 rounded-lg border border-line/70 bg-void/50 p-1 text-[11px]">
                      <button
                        type="button"
                        onClick={() => {
                          setShowSaved(false);
                          setShowPinned(false);
                          tan.setShowMentions(false);
                          tan.setView("timeline");
                          tan.setAll(false);
                        }}
                        className={`rounded px-2 py-1 ${
                          tan.view === "timeline" &&
                          !showSaved &&
                          !showPinned &&
                          !tan.showMentions &&
                          !tan.all
                            ? "bg-signal/15 text-signal"
                            : "text-mist"
                        }`}
                      >
                        following
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          setShowSaved(false);
                          setShowPinned(false);
                          tan.setShowMentions(false);
                          tan.setView("timeline");
                          tan.setAll(true);
                        }}
                        className={`rounded px-2 py-1 ${
                          tan.view === "timeline" &&
                          !showSaved &&
                          !showPinned &&
                          !tan.showMentions &&
                          tan.all
                            ? "bg-signal/15 text-signal"
                            : "text-mist"
                        }`}
                      >
                        everything
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          setShowSaved(false);
                          setShowPinned(false);
                          tan.setShowMentions(false);
                          tan.setView("top");
                        }}
                        className={`rounded px-2 py-1 ${
                          tan.view === "top" &&
                          !showSaved &&
                          !showPinned &&
                          !tan.showMentions
                            ? "bg-signal/15 text-signal"
                            : "text-mist"
                        }`}
                      >
                        top
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          setShowSaved(false);
                          setShowPinned(false);
                          tan.setShowMentions(true);
                          tan.setView("timeline");
                        }}
                        className={`rounded px-2 py-1 ${
                          tan.showMentions ? "bg-signal/15 text-signal" : "text-mist"
                        }`}
                      >
                        mentions
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          tan.setShowMentions(false);
                          tan.setView("timeline");
                          setShowSaved(true);
                          setShowPinned(false);
                        }}
                        className={`rounded px-2 py-1 ${
                          showSaved ? "bg-signal/15 text-signal" : "text-mist"
                        }`}
                      >
                        saved
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          tan.setShowMentions(false);
                          tan.setView("timeline");
                          setShowSaved(false);
                          setShowPinned(true);
                        }}
                        className={`rounded px-2 py-1 ${
                          showPinned ? "bg-signal/15 text-signal" : "text-mist"
                        }`}
                      >
                        pinned
                      </button>
                    </div>
                    )}
                  </>
                )}
              </div>

              <form
                className="flex gap-2 border-b border-line/60 px-4 py-3"
                onSubmit={(event) => {
                  event.preventDefault();
                  void submitSearch();
                }}
              >
                <input
                  value={searchText}
                  onChange={(event) => setSearchText(event.target.value)}
                  placeholder="Search stored posts…"
                  spellCheck={false}
                  aria-label="Search stored posts"
                  className="h-9 min-w-0 flex-1 rounded-lg border border-line bg-void/70 px-2.5 font-mono text-xs text-zinc-200 outline-none transition placeholder:text-mist/60 focus:border-signal/60 focus:ring-2 focus:ring-signal/20"
                />
                <button
                  type="submit"
                  disabled={!searchText.trim()}
                  className="rounded-lg border border-line bg-void/70 px-2.5 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal disabled:opacity-50"
                >
                  search
                </button>
              </form>

              <div className="thin-scroll min-h-[320px] space-y-3 px-4 py-4 lg:max-h-[calc(100dvh-370px)] lg:overflow-y-auto">
                {tan.threadRef !== null ? (
                  <ThreadView
                    posts={tan.threadPosts}
                    loading={tan.threadLoading}
                    error={tan.threadError ?? tan.error}
                    onToggleLike={toggleLike}
                    onRepost={submitRepost}
                    onReply={(target) => setReplyTarget(target)}
                    onDelete={submitDelete}
                    onOpenThread={(target) => void tan.openThread(target.hash)}
                    deleting={deleting}
                  />
                ) : showPinned ? (
                  tan.pinned.length === 0 ? (
                    tan.pinnedLoading ? (
                      <p className="py-8 text-center font-mono text-[11px] text-mist">
                        loading pinned posts…
                      </p>
                    ) : (
                      <PinnedEmptyState />
                    )
                  ) : (
                    <>
                      {tan.pinned.map((post) => (
                        <PostCard
                          key={post.hash}
                          post={post}
                          onToggleLike={toggleLike}
                          onRepost={submitRepost}
                          onToggleSave={toggleSave}
                          onTogglePin={togglePin}
                          onReply={(target) => setReplyTarget(target)}
                          onDelete={submitDelete}
                          deleting={deleting.has(post.hash)}
                        />
                      ))}
                      {tan.pinnedHasMore ? (
                        <button
                          type="button"
                          onClick={() => void tan.loadMorePinned()}
                          disabled={tan.pinnedLoading}
                          className="w-full rounded-xl border border-line/70 bg-void/40 px-3 py-2.5 text-xs text-mist transition hover:border-signal/50 hover:text-signal disabled:opacity-50"
                        >
                          {tan.pinnedLoading
                            ? "loading…"
                            : "load more pinned posts"}
                        </button>
                      ) : null}
                    </>
                  )
                ) : showSaved ? (
                  tan.saved.length === 0 ? (
                    tan.savedLoading ? (
                      <p className="py-8 text-center font-mono text-[11px] text-mist">
                        loading saved posts…
                      </p>
                    ) : (
                      <SavedEmptyState />
                    )
                  ) : (
                    <>
                      {tan.saved.map((post) => (
                        <PostCard
                          key={post.hash}
                          post={post}
                          onToggleLike={toggleLike}
                          onRepost={submitRepost}
                          onToggleSave={toggleSave}
                          onTogglePin={togglePin}
                          onReply={(target) => setReplyTarget(target)}
                          onDelete={submitDelete}
                          deleting={deleting.has(post.hash)}
                        />
                      ))}
                      {tan.savedHasMore ? (
                        <button
                          type="button"
                          onClick={() => void tan.loadMoreSaved()}
                          disabled={tan.savedLoading}
                          className="w-full rounded-xl border border-line/70 bg-void/40 px-3 py-2.5 text-xs text-mist transition hover:border-signal/50 hover:text-signal disabled:opacity-50"
                        >
                          {tan.savedLoading
                            ? "loading…"
                            : "load more saved posts"}
                        </button>
                      ) : null}
                    </>
                  )
                ) : tan.showMentions ? (
                  shown.length === 0 && tan.error ? (
                    <p className="px-4 py-10 text-center text-xs leading-relaxed text-rose-300">
                      {tan.error}
                    </p>
                  ) : shown.length === 0 ? (
                    <EmptyState mentions />
                  ) : (
                    shown.map((post) => (
                      <PostCard
                        key={post.hash}
                        post={post}
                        onToggleLike={toggleLike}
                        onRepost={submitRepost}
                        onToggleSave={toggleSave}
                          onTogglePin={togglePin}
                        onReply={(target) => setReplyTarget(target)}
                        onDelete={submitDelete}
                        deleting={deleting.has(post.hash)}
                        onOpenThread={(target) => void tan.openThread(target.hash)}
                      />
                    ))
                  )
                ) : visiblePosts.length === 0 ? (
                  showingSearch ? (
                    <SearchEmptyState query={tan.searchQuery ?? ""} />
                  ) : (
                    <EmptyState top={tan.view === "top"} />
                  )
                ) : (
                  visiblePosts.map((post) => (
                    <PostCard
                      key={post.hash}
                      post={post}
                      onToggleLike={toggleLike}
                      onRepost={submitRepost}
                      onToggleLikers={toggleLikers}
                      likersOpen={likersOpen === post.hash}
                      likers={likersOpen === post.hash ? likers : null}
                      likersTotal={likersOpen === post.hash ? likersTotal : 0}
                      onToggleSave={toggleSave}
                      onTogglePin={togglePin}
                      onReply={(target) => {
                        setQuoteTarget(null);
                        setReplyTarget(target);
                      }}
                      onQuote={(target) => {
                        setReplyTarget(null);
                        setQuoteTarget(target);
                      }}
                      onDelete={submitDelete}
                      onEdit={submitEdit}
                      deleting={deleting.has(post.hash)}
                      onMute={submitMute}
                      muting={muting.has(post.author)}
                      onOpenThread={(target) => void tan.openThread(target.hash)}
                    />
                  ))
                )}
              </div>
            </section>

            <section className="mt-3 rounded-2xl border border-line/80 bg-panel/60 p-3">
              {quoteTarget ? (
                <div className="mb-2 flex items-center justify-between gap-2 rounded-lg border border-signal/30 bg-signal/10 px-2.5 py-1.5 text-[11px] text-signal">
                  <span className="min-w-0 truncate">
                    Quoting {quoteTarget.name || "anonymous"} ·{" "}
                    {quoteTarget.text.length > 80
                      ? `${quoteTarget.text.slice(0, 80)}…`
                      : quoteTarget.text}
                  </span>
                  <button
                    type="button"
                    onClick={() => setQuoteTarget(null)}
                    className="shrink-0 font-mono text-[10px] text-mist/80 transition hover:text-zinc-100"
                  >
                    cancel
                  </button>
                </div>
              ) : null}
              {replyTarget ? (
                <div className="mb-2 flex items-center justify-between gap-2 rounded-lg border border-signal/30 bg-signal/10 px-2.5 py-1.5 text-[11px] text-signal">
                  <span className="min-w-0 truncate">
                    Replying to {replyTarget.name || "anonymous"} ·{" "}
                    {replyTarget.text.length > 80
                      ? `${replyTarget.text.slice(0, 80)}…`
                      : replyTarget.text}
                  </span>
                  <button
                    type="button"
                    onClick={() => setReplyTarget(null)}
                    className="shrink-0 font-mono text-[10px] text-mist/80 transition hover:text-zinc-100"
                  >
                    cancel
                  </button>
                </div>
              ) : null}
              <div className="flex items-end gap-2">
                <textarea
                  value={text}
                  onChange={(event) => setText(event.target.value.slice(0, 2000))}
                  onKeyDown={(event) => {
                    if (
                      event.key === "Enter" &&
                      !event.shiftKey &&
                      !event.nativeEvent.isComposing
                    ) {
                      event.preventDefault();
                      if (!tan.posting) void submitPost();
                    }
                  }}
                  rows={2}
                  maxLength={2000}
                  placeholder={
                    quoteTarget
                      ? `Quote ${quoteTarget.name || "anonymous"}…`
                      : replyTarget
                        ? `Reply to ${replyTarget.name || "anonymous"}…`
                        : "Write something to the mesh…"
                  }
                  className="thin-scroll min-h-[64px] w-full resize-y rounded-xl border border-line bg-void/70 px-3 py-2.5 text-[13px] leading-relaxed text-zinc-100 outline-none transition placeholder:text-mist/60 focus:border-signal/60 focus:ring-2 focus:ring-signal/20"
                />
                <button
                  type="button"
                  onClick={() => void submitPost()}
                  disabled={tan.posting || !text.trim()}
                  className="inline-flex h-[52px] shrink-0 items-center gap-2 rounded-xl border border-signal/40 bg-signal/15 px-4 text-xs font-medium text-signal transition hover:border-signal/70 hover:bg-signal/25 disabled:cursor-not-allowed disabled:border-line disabled:bg-void/60 disabled:text-mist/60"
                >
                  <SendIcon className="h-4 w-4" />
                  <span className="hidden sm:inline">
                    {tan.posting ? "signing…" : "post"}
                  </span>
                </button>
              </div>
              <div className="mt-1.5 flex flex-wrap items-center justify-between gap-2">
                <p
                  className={`text-[11px] ${
                    error || (showingSearch && tan.error)
                      ? "text-rose-300"
                      : "text-mist/70"
                  }`}
                >
                  {error ??
                    (showingSearch && tan.error) ??
                    "Enter to post · Shift+Enter for a new line"}
                </p>
                <span className="font-mono text-[10px] text-mist/60">
                  {text.length}/2000
                </span>
              </div>
            </section>
          </div>
        </main>

        <footer className="mx-auto flex w-full max-w-6xl flex-wrap items-center justify-between gap-2 px-4 pb-6 font-mono text-[10px] text-mist/60 lg:px-8">
          <span>tan · the social layer of tau</span>
          <span>signed posts · no servers · no accounts</span>
        </footer>
      </div>
    </div>
  );
}

export function SuggestionList({
  suggestions,
  onFollow,
}: {
  suggestions: TanSuggestion[];
  onFollow: (author: string) => void;
}) {
  if (suggestions.length === 0) {
    return (
      <p className="rounded-lg border border-dashed border-line/70 px-3 py-2 text-[11px] leading-relaxed text-mist">
        No suggestions yet. Follow an author or sync more posts first.
      </p>
    );
  }
  return (
    <ul className="space-y-1.5">
      {suggestions.map((suggestion) => (
        <li
          key={suggestion.author}
          className="flex items-center justify-between gap-2 rounded-lg border border-line/60 bg-void/40 px-2.5 py-2"
        >
          <div className="min-w-0">
            <p className="truncate text-xs text-zinc-200">
              {suggestion.name || "unknown"}
            </p>
            <p className="font-mono text-[10px] text-mist/80">
              {shortHash(suggestion.author, 10)} · {suggestion.posts} post(s)
              {suggestion.likes ? `, ${suggestion.likes} like(s)` : ""}
            </p>
            <p className="truncate text-[11px] text-mist">
              {suggestion.reason}
            </p>
          </div>
          <button
            type="button"
            onClick={() => onFollow(suggestion.author)}
            className="shrink-0 rounded-lg border border-line bg-void/70 px-2 py-1 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal"
          >
            follow
          </button>
        </li>
      ))}
    </ul>
  );
}

export function TanBackupSection({
  exporting,
  importing,
  onExport,
  onImport,
}: {
  exporting: boolean;
  importing: boolean;
  onExport: (allAuthors: boolean) => Promise<TanExportDocument>;
  onImport: (doc: TanExportDocument) => Promise<TanImportResult>;
}) {
  const [message, setMessage] = useState<string | null>(null);
  const [errors, setErrors] = useState<TanImportError[]>([]);
  const [error, setError] = useState<string | null>(null);
  const importInputRef = useRef<HTMLInputElement>(null);

  async function submitExport(allAuthors: boolean) {
    setError(null);
    setMessage(null);
    setErrors([]);
    try {
      const doc = await onExport(allAuthors);
      setMessage(
        `exported ${doc.entries.length} entr${doc.entries.length === 1 ? "y" : "ies"}${allAuthors ? " (all authors)" : " (your chain)"}`,
      );
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "export failed");
    }
  }

  async function submitImportFile(file: File) {
    setError(null);
    setMessage(null);
    setErrors([]);
    try {
      let doc: unknown;
      try {
        doc = JSON.parse(await file.text());
      } catch {
        setError("that file is not a valid export document");
        return;
      }
      try {
        const result = await onImport(doc as TanExportDocument);
        setMessage(
          `imported ${result.imported} · ${result.present} already present · ${result.skipped} skipped`,
        );
        setErrors(result.errors);
      } catch (caught) {
        setError(caught instanceof Error ? caught.message : "import failed");
      }
    } finally {
      // Picking the same file again must fire onChange, including
      // after a parse failure that returns before the import runs.
      if (importInputRef.current) importInputRef.current.value = "";
    }
  }

  return (
    <section className="rounded-2xl border border-line/80 bg-panel/40 p-4">
      <SectionLabel>backup</SectionLabel>
      <p className="mt-2 text-[11px] leading-relaxed text-mist">
        Back up your signed chain as a file, or restore one. Every imported
        entry is re-verified before it is stored.
      </p>
      <div className="mt-3 flex gap-2">
        <button
          type="button"
          onClick={() => void submitExport(false)}
          disabled={exporting}
          className="flex-1 rounded-lg border border-line bg-void/70 px-2.5 py-2 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal disabled:opacity-50"
        >
          {exporting ? "exporting…" : "export mine"}
        </button>
        <button
          type="button"
          onClick={() => void submitExport(true)}
          disabled={exporting}
          className="flex-1 rounded-lg border border-line bg-void/70 px-2.5 py-2 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal disabled:opacity-50"
        >
          export all
        </button>
        <label
          htmlFor="tan-import-file"
          className={`flex-1 cursor-pointer rounded-lg border border-line bg-void/70 px-2.5 py-2 text-center font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal ${importing ? "pointer-events-none opacity-50" : ""}`}
        >
          {importing ? "importing…" : "import"}
        </label>
        <input
          ref={importInputRef}
          id="tan-import-file"
          type="file"
          accept="application/json,.json"
          className="hidden"
          onChange={(event) => {
            const file = event.target.files?.[0];
            if (file) void submitImportFile(file);
          }}
        />
      </div>
      {message ? (
        <p className="mt-2 font-mono text-[10px] text-signal/80">{message}</p>
      ) : null}
      {error ? (
        <p className="mt-2 font-mono text-[10px] text-rose-300">{error}</p>
      ) : null}
      {errors.length > 0 ? (
        <ul className="mt-2 space-y-1">
          {errors.map((entry) => (
            <li
              key={entry.hash || entry.error}
              className="font-mono text-[10px] leading-relaxed text-rose-300/90"
            >
              skipped {entry.hash ? shortHash(entry.hash, 8) : "?"}:{" "}
              {entry.error}
            </li>
          ))}
        </ul>
      ) : null}
    </section>
  );
}

function SearchEmptyState({ query }: { query: string }) {
  return (
    <div className="flex min-h-[280px] flex-col items-center justify-center px-4 text-center">
      <div className="grid h-20 w-20 place-items-center rounded-full border border-signal/20 bg-signal/[0.04] font-mono text-3xl text-signal/80">
        τ
      </div>
      <h3 className="mt-4 text-sm font-medium text-zinc-200">
        No stored posts match “{query}”
      </h3>
      <p className="mt-1.5 max-w-md text-xs leading-relaxed text-mist">
        Search covers the text of stored posts and quotes. Try a different
        word, or sync to pull posts cached by your peers first.
      </p>
    </div>
  );
}

function ThreadView({
  posts,
  loading,
  error,
  onToggleLike,
  onRepost,
  onReply,
  onDelete,
  onOpenThread,
  deleting,
}: {
  posts: TanPost[] | null;
  loading: boolean;
  error: string | null;
  onToggleLike: (post: TanPost) => void;
  onRepost: (post: TanPost) => void;
  onReply: (post: TanPost) => void;
  onDelete: (post: TanPost) => void;
  onOpenThread: (post: TanPost) => void;
  deleting: ReadonlySet<string>;
}) {
  // Entries are already in depth-first order with their depth, so the
  // thread reads as a conversation. A failed refresh keeps the previous
  // entries and reports the error above them instead of blanking the
  // view; with nothing to show, the error is the view.
  if (posts !== null && posts.length > 0) {
    return (
      <>
        {error ? (
          <p className="rounded-lg border border-rose-400/30 bg-rose-400/10 px-3 py-2 text-[11px] leading-relaxed text-rose-300">
            {error}
          </p>
        ) : null}
        {posts.map((post) => (
          <PostCard
            key={post.hash}
            post={post}
            onToggleLike={onToggleLike}
            onRepost={onRepost}
            onReply={onReply}
            onDelete={onDelete}
            deleting={deleting.has(post.hash)}
            onOpenThread={onOpenThread}
          />
        ))}
      </>
    );
  }
  if (loading) {
    return (
      <p className="py-8 text-center text-xs text-mist">Loading the thread…</p>
    );
  }
  if (error) {
    return (
      <p className="py-8 text-center text-xs leading-relaxed text-rose-300">
        {error}
      </p>
    );
  }
  return (
    <p className="py-8 text-center text-xs text-mist">This thread is empty.</p>
  );
}

function EmptyState({
  mentions = false,
  top = false,
}: {
  mentions?: boolean;
  top?: boolean;
}) {
  return (
    <div className="flex min-h-[280px] flex-col items-center justify-center px-4 text-center">
      <div className="grid h-20 w-20 place-items-center rounded-full border border-signal/20 bg-signal/[0.04] font-mono text-3xl text-signal/80">
        τ
      </div>
      <h3 className="mt-4 text-sm font-medium text-zinc-200">
        {mentions ? "No replies yet" : "No posts yet"}
      </h3>
      <p className="mt-1.5 max-w-md text-xs leading-relaxed text-mist">
        {mentions ? (
          "When someone replies to one of your posts, it shows up here."
        ) : top ? (
          <>
            Once posts arrive, the most-liked rank first here. Like a post
            to move it up.
          </>
        ) : (
          <>
            Start a node with{" "}
            <code className="rounded bg-void/80 px-1 py-0.5 font-mono text-[11px] text-zinc-300">
              tau serve --social --http
            </code>
            , publish a post, then sync or follow another author.
          </>
        )}
      </p>
    </div>
  );
}

function SavedEmptyState() {
  return (
    <div className="flex min-h-[280px] flex-col items-center justify-center px-4 text-center">
      <div className="grid h-20 w-20 place-items-center rounded-full border border-signal/20 bg-signal/[0.04] font-mono text-3xl text-signal/80">
        ☆
      </div>
      <h3 className="mt-4 text-sm font-medium text-zinc-200">
        No saved posts yet
      </h3>
      <p className="mt-1.5 max-w-md text-xs leading-relaxed text-mist">
        Save a post from the timeline to read it later. Saves stay on this
        node — they are never published or shared with peers.
      </p>
    </div>
  );
}

function PinnedEmptyState() {
  return (
    <div className="flex min-h-[280px] flex-col items-center justify-center px-4 text-center">
      <div className="grid h-20 w-20 place-items-center rounded-full border border-signal/20 bg-signal/[0.04] font-mono text-3xl text-signal/80">
        📌
      </div>
      <h3 className="mt-4 text-sm font-medium text-zinc-200">
        No pinned posts yet
      </h3>
      <p className="mt-1.5 max-w-md text-xs leading-relaxed text-mist">
        Pin one of your own posts from the timeline to keep it at the top
        of this list. Pins stay on this node — they are never published or
        shared with peers.
      </p>
    </div>
  );
}

// Whether the console offers a Repost button for this card. The store
// only accepts posts and replies as repost targets (a missing kind reads
// as a plain post, the same default the store uses), and refuses your own
// posts, posts you already reposted, and deleted posts — so the button
// stays hidden there and the count chip remains a plain display.
function canRepost(post: TanPost): boolean {
  if (post.deleted || post.own || post.reposted) return false;
  return (
    post.kind === undefined || post.kind === "post" || post.kind === "reply"
  );
}

export function VerifyResult({ report }: { report: TanVerifyReport }) {
  if (report.ok) {
    return (
      <p className="mt-2 font-mono text-[10px] leading-relaxed text-signal/80">
        store OK — {report.entries} entr{report.entries === 1 ? "y" : "ies"}{" "}
        from {report.authors} author{report.authors === 1 ? "" : "s"}
        {report.unlinked > 0
          ? ` · ${report.unlinked} unlinked (partial sync)`
          : ""}
      </p>
    );
  }
  return (
    <div className="mt-2 space-y-1.5">
      <p className="font-mono text-[10px] leading-relaxed text-rose-300">
        store NOT OK — {report.problems.length} problem
        {report.problems.length === 1 ? "" : "s"} in {report.entries} entries
      </p>
      <ul className="space-y-1.5">
        {report.problems.map((problem, index) => (
          <li
            // One entry can carry several faults (a bad hash and a
            // chain fault, say), so the hash alone is not a unique key.
            key={`${problem.hash}:${index}`}
            className="rounded-lg border border-rose-400/30 bg-rose-400/5 px-2.5 py-2"
          >
            <p className="font-mono text-[10px] text-rose-200">
              {shortHash(problem.hash, 16)} · {shortHash(problem.author, 10)}
            </p>
            <p className="mt-0.5 text-[11px] leading-relaxed text-zinc-300">
              {problem.problem}
            </p>
          </li>
        ))}
      </ul>
    </div>
  );
}

export function PostCard({
  post,
  onToggleLike,
  onRepost,
  onToggleLikers,
  likersOpen,
  likers,
  likersTotal,
  onToggleSave,
  onTogglePin,
  onReply,
  onQuote,
  onDelete,
  onEdit,
  deleting,
  onMute,
  muting,
  onOpenThread,
}: {
  post: TanPost;
  onToggleLike: (post: TanPost) => void;
  onRepost: (post: TanPost) => void;
  // Optional so existing callers (and tests) that only exercise the
  // like toggle keep working; when absent the count button is inert.
  onToggleLikers?: (post: TanPost) => void;
  likersOpen?: boolean;
  // The likers for the open list, or null while it is loading.
  likers?: TanLiker[] | null;
  // Full liker count before the server's cap; 0 when unknown.
  likersTotal?: number;
  onToggleSave?: (post: TanPost) => void;
  // Pins are own-posts-only, so the button renders only on your own
  // cards; when absent the card shows no pin control.
  onTogglePin?: (post: TanPost) => void;
  onReply: (post: TanPost) => void;
  onQuote?: (post: TanPost) => void;
  onDelete: (post: TanPost) => void;
  // Optional like onQuote/onMute: views that do not wire editing (thread,
  // saved, mentions) render the card without an edit button.
  onEdit?: (post: TanPost, text: string) => Promise<boolean>;
  deleting: boolean;
  onMute?: (post: TanPost) => void;
  muting?: boolean;
  onOpenThread?: (post: TanPost) => void;
}) {
  const initial = (post.name || "?").slice(0, 1).toUpperCase();
  const [confirming, setConfirming] = useState(false);
  const [editing, setEditing] = useState(false);
  const [editText, setEditText] = useState("");
  const [saving, setSaving] = useState(false);

  async function submitEditForm() {
    if (saving || !onEdit) return;
    setSaving(true);
    try {
      const ok = await onEdit(post, editText.trim());
      if (ok) setEditing(false);
    } finally {
      setSaving(false);
    }
  }
  // Thread entries indent by depth (capped, so a deep reply chain does
  // not squeeze the text away); timeline entries have no depth.
  const depth = Math.min(Math.max(post.depth ?? 0, 0), 6);
  // A repost carries no text of its own: the card shows the original
  // post's text instead, or nothing when it has not arrived / was deleted.
  const bodyText =
    post.kind === "repost"
      ? post.repost_of && !post.repost_of.deleted
        ? post.repost_of.text
        : ""
      : post.text;
  return (
    <article
      className="rounded-xl border border-line/70 bg-void/40 p-3.5"
      style={depth > 0 ? { marginLeft: `${depth * 14}px` } : undefined}
    >
      <div className="flex items-start gap-3">
        <span className="grid h-9 w-9 shrink-0 place-items-center rounded-lg border border-signal/25 bg-signal/10 font-mono text-sm text-signal">
          {initial}
        </span>
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5">
            <span className="text-xs font-medium text-zinc-200">
              {post.name || "anonymous"}
            </span>
            <span className="font-mono text-[10px] text-mist/80">
              {shortHash(post.author, 10)}
            </span>
            <span className="font-mono text-[10px] text-mist/60">
              {formatDateTime(post.created_at)}
            </span>
            {post.saved_at ? (
              <span className="font-mono text-[10px] text-mist/60">
                saved {formatDateTime(post.saved_at)}
              </span>
            ) : null}
            {post.pinned_at ? (
              <span className="font-mono text-[10px] text-mist/60">
                pinned {formatDateTime(post.pinned_at)}
              </span>
            ) : null}
          </div>
          {post.kind === "reply" ? (
            <p className="mt-1 truncate text-[11px] text-mist/80">
              in reply to{" "}
              {post.reply_to ? (
                <>
                  {post.reply_to.blocked ? (
                    "a blocked author's post"
                  ) : (
                    <>
                      {post.reply_to.name || "anonymous"} ·{" "}
                      {post.reply_to.deleted
                        ? "a deleted post"
                        : post.reply_to.muted
                          ? "a muted post"
                          : post.reply_to.text.length > 80
                            ? `${post.reply_to.text.slice(0, 80)}…`
                            : post.reply_to.text}
                    </>
                  )}
                </>
              ) : (
                shortHash(post.target ?? "", 16)
              )}
            </p>
          ) : null}
          {post.kind === "repost" ? (
            <p className="mt-1 truncate text-[11px] text-mist/80">
              reposted{" "}
              {post.repost_of ? (
                <>
                  {post.repost_of.name || "anonymous"}
                  {post.repost_of.deleted ? " · a deleted post" : ""}
                </>
              ) : (
                `${shortHash(post.target ?? "", 16)} (original not cached yet)`
              )}
            </p>
          ) : null}
          {editing ? (
            <div className="mt-1.5">
              <textarea
                value={editText}
                disabled={saving}
                // Code-point-aware cap: the backend counts Python code
                // points (len), while slice/maxLength count UTF-16 units,
                // so a plain slice would silently drop emoji from a valid
                // existing post on the first keystroke.
                onChange={(event) =>
                  setEditText(
                    [...event.target.value].slice(0, 2000).join(""),
                  )
                }
                rows={3}
                aria-label="Edit post text"
                className="w-full resize-y rounded-lg border border-line/70 bg-void/60 px-2.5 py-2 text-[13px] leading-relaxed text-zinc-200 outline-none focus:border-signal/50"
              />
              <div className="mt-2 flex gap-2 font-mono text-[10px]">
                <button
                  type="button"
                  disabled={
                    saving ||
                    !editText.trim() ||
                    editText.trim() === post.text
                  }
                  onClick={() => void submitEditForm()}
                  title="Save this edit"
                  className="rounded border border-signal/40 bg-signal/10 px-1.5 py-0.5 text-signal transition-colors hover:bg-signal/20 disabled:opacity-50"
                >
                  {saving ? "saving…" : "save"}
                </button>
                <button
                  type="button"
                  disabled={saving}
                  onClick={() => setEditing(false)}
                  title="Discard this edit"
                  className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5 text-mist/80 transition-colors hover:text-zinc-100 disabled:opacity-50"
                >
                  cancel
                </button>
              </div>
            </div>
          ) : post.deleted ? (
            <p className="mt-1.5 whitespace-pre-wrap break-words text-[13px] leading-relaxed text-zinc-200">
              <span className="italic text-mist/70">
                This post was deleted.
              </span>
            </p>
          ) : bodyText ? (
            <p className="mt-1.5 whitespace-pre-wrap break-words text-[13px] leading-relaxed text-zinc-200">
              {bodyText}
            </p>
          ) : null}
          {post.kind === "quote" ? (
            <p className="mt-1 whitespace-pre-wrap break-words text-[11px] text-mist/80">
              quoting{" "}
              {post.quoted ? (
                post.quoted.blocked ? (
                  "a blocked author's post"
                ) : (
                  <>
                    {post.quoted.name || "anonymous"} ·{" "}
                    {post.quoted.deleted
                      ? "a deleted post"
                      : post.quoted.muted
                        ? "a muted post"
                        : post.quoted.text}
                  </>
                )
              ) : (
                shortHash(post.target ?? "", 16)
              )}
            </p>
          ) : null}
          <div className="mt-2 flex flex-wrap items-center gap-2 font-mono text-[10px] text-mist/60">
            <span className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5">
              {post.prev ? "chained" : "genesis"}
            </span>
            <span>{shortHash(post.hash, 16)}</span>
            <CopyButton value={post.hash} label="hash" className="!px-1.5 !py-0.5" />
            {post.deleted ? null : (
              <button
                type="button"
                onClick={() => onToggleLike(post)}
                aria-pressed={Boolean(post.liked)}
                title={post.liked ? "Unlike this post" : "Like this post"}
                className={`rounded border px-1.5 py-0.5 transition-colors ${
                  post.liked
                    ? "border-rose-400/40 bg-rose-400/10 text-rose-300"
                    : "border-line/70 bg-panel/60 text-mist/80 hover:text-rose-300"
                }`}
              >
                ♥ {post.likes ?? 0}
              </button>
            )}
            {canRepost(post) ? (
              <button
                type="button"
                onClick={() => onRepost(post)}
                title="Repost this post"
                className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5 transition-colors hover:text-emerald-300"
              >
                ↻ {post.reposts ?? 0}
              </button>
            ) : (
              <span
                title={post.reposted ? "You reposted this" : "Reposts"}
                className={`rounded border px-1.5 py-0.5 ${
                  post.reposted
                    ? "border-emerald-400/40 bg-emerald-400/10 text-emerald-300"
                    : "border-line/70 bg-panel/60"
                }`}
              >
                ↻ {post.reposts ?? 0}
              </span>
            )}
            {(post.likes ?? 0) > 0 ? (
              <button
                type="button"
                onClick={() => onToggleLikers?.(post)}
                aria-expanded={Boolean(likersOpen)}
                title="Show who liked this post"
                className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5 text-mist/80 transition-colors hover:text-rose-300"
              >
                who liked
              </button>
            ) : null}            {(post.quotes ?? 0) > 0 ? (
              <span
                title="Quotes of this post"
                className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5"
              >
                ❝ {post.quotes}
              </span>
            ) : null}
            {onToggleSave ? (
              <button
                type="button"
                onClick={() => onToggleSave(post)}
                aria-pressed={Boolean(post.saved)}
                title={
                  post.saved
                    ? "Remove from saved posts"
                    : "Save this post to read later"
                }
                className={`rounded border px-1.5 py-0.5 transition-colors ${
                  post.saved
                    ? "border-amber-400/40 bg-amber-400/10 text-amber-300"
                    : "border-line/70 bg-panel/60 text-mist/80 hover:text-amber-300"
                }`}
              >
                {post.saved ? "★ saved" : "☆ save"}
              </button>
            ) : null}
            {onTogglePin && post.own ? (
              <button
                type="button"
                onClick={() => onTogglePin(post)}
                aria-pressed={Boolean(post.pinned)}
                title={
                  post.pinned
                    ? "Remove from pinned posts"
                    : "Pin this post to the top of your pinned list"
                }
                className={`rounded border px-1.5 py-0.5 transition-colors ${
                  post.pinned
                    ? "border-sky-400/40 bg-sky-400/10 text-sky-300"
                    : "border-line/70 bg-panel/60 text-mist/80 hover:text-sky-300"
                }`}
              >
                {post.pinned ? "📌 pinned" : "📌 pin"}
              </button>
            ) : null}
            {post.deleted || post.kind === "repost" ? null : (
              <button
                type="button"
                onClick={() => onReply(post)}
                title="Reply to this post"
                className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5 transition-colors hover:text-signal"
              >
                reply
              </button>
            )}
            {onQuote && post.kind !== "quote" ? (
              <button
                type="button"
                onClick={() => onQuote(post)}
                title="Quote this post with your own comment"
                className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5 transition-colors hover:text-signal"
              >
                quote
              </button>
            ) : null}
            {onEdit && post.own && post.kind !== "quote" && !editing && !deleting ? (
              <button
                type="button"
                onClick={() => {
                  setEditText(post.text);
                  setEditing(true);
                }}
                title="Edit this post"
                className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5 text-mist/80 transition-colors hover:border-signal/40 hover:text-signal"
              >
                edit
              </button>
            ) : null}
            {!post.own && onMute ? (
              muting ? (
                <button
                  type="button"
                  disabled
                  className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5 text-mist/60"
                >
                  muting…
                </button>
              ) : (
                <button
                  type="button"
                  onClick={() => onMute(post)}
                  title="Mute this author"
                  className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5 text-mist/80 transition-colors hover:border-rose-400/40 hover:text-rose-300"
                >
                  mute
                </button>
              )
            ) : null}
            {onOpenThread ? (
              <button
                type="button"
                onClick={() => onOpenThread(post)}
                title="Open this thread"
                className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5 transition-colors hover:text-signal"
              >
                thread
              </button>
            ) : null}
            {post.deleted || !post.own ? null : (
              deleting ? (
                <button
                  type="button"
                  disabled
                  className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5 text-mist/60"
                >
                  deleting…
                </button>
              ) : confirming ? (
                <span className="inline-flex items-center gap-1">
                  <button
                    type="button"
                    onClick={() => {
                      setConfirming(false);
                      onDelete(post);
                    }}
                    title="Delete this post"
                    className="rounded border border-rose-400/40 bg-rose-400/10 px-1.5 py-0.5 text-rose-300 transition-colors hover:bg-rose-400/20"
                  >
                    confirm delete
                  </button>
                  <button
                    type="button"
                    onClick={() => setConfirming(false)}
                    title="Keep this post"
                    className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5 text-mist/80 transition-colors hover:text-zinc-100"
                  >
                    keep
                  </button>
                </span>
              ) : (
                <button
                  type="button"
                  onClick={() => setConfirming(true)}
                  title="Delete this post"
                  className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5 text-mist/80 transition-colors hover:border-rose-400/40 hover:text-rose-300"
                >
                  delete
                </button>
              )
            )}
          </div>
          {likersOpen ? (
            <div className="mt-2 rounded border border-line/70 bg-panel/60 px-2 py-1.5">
              <p className="font-mono text-[10px] uppercase tracking-[0.14em] text-mist/70">
                liked by
              </p>
              {likers === null || likers === undefined ? (
                <p className="mt-1 text-[12px] text-mist/80">loading…</p>
              ) : likers.length === 0 ? (
                <p className="mt-1 text-[12px] text-mist/80">no likes yet</p>
              ) : (
                <>
                  <ul className="mt-1 space-y-0.5">
                    {likers.map((liker) => (
                      <li
                        key={liker.author}
                        className="text-[12px] text-zinc-200"
                      >
                        {liker.name || shortHash(liker.author, 16)}
                      </li>
                    ))}
                  </ul>
                  {(likersTotal ?? 0) > likers.length ? (
                    <p className="mt-1 text-[11px] text-mist/70">
                      showing first {likers.length} of {likersTotal} likers
                    </p>
                  ) : null}
                </>
              )}
            </div>
          ) : null}
        </div>
      </div>
    </article>
  );
}
