"use client";

import Link from "next/link";
import { useState } from "react";

import { formatDateTime, shortHash } from "@/lib/format";
import type { TanPost } from "@/lib/types";
import { useTan } from "@/lib/use-tan";

import { CopyButton, SectionLabel, SendIcon, StatusDot } from "./ui";

export function SocialConsole() {
  const tan = useTan();
  const [text, setText] = useState("");
  const [target, setTarget] = useState("");
  const [replyTarget, setReplyTarget] = useState<TanPost | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  // Hashes with a delete request in flight. While one is pending the card
  // shows a disabled state instead of the confirm button, so a second
  // click cannot report the already-deleted post as a failure.
  const [deleting, setDeleting] = useState<ReadonlySet<string>>(
    () => new Set(),
  );

  async function submitPost() {
    if (!text.trim()) return;
    setError(null);
    try {
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

  async function toggleLike(post: TanPost) {
    setError(null);
    try {
      if (post.liked) {
        await tan.unlike(post.hash);
      } else {
        await tan.like(post.hash);
      }
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "could not react");
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
          </aside>

          <div className="flex min-w-0 flex-col">
            <section className="rounded-2xl border border-line/80 bg-panel/40">
              <div className="flex flex-wrap items-center justify-between gap-2 border-b border-line/60 px-4 py-3">
                <div className="flex items-center gap-3">
                  <SectionLabel>{tan.all ? "public cache" : "timeline"}</SectionLabel>
                  <span className="font-mono text-[10px] text-mist">
                    {tan.posts.length} post(s)
                  </span>
                </div>
                <div className="flex items-center gap-1 rounded-lg border border-line/70 bg-void/50 p-1 text-[11px]">
                  <button
                    type="button"
                    onClick={() => tan.setAll(false)}
                    className={`rounded px-2 py-1 ${
                      tan.all ? "text-mist" : "bg-signal/15 text-signal"
                    }`}
                  >
                    following
                  </button>
                  <button
                    type="button"
                    onClick={() => tan.setAll(true)}
                    className={`rounded px-2 py-1 ${
                      tan.all ? "bg-signal/15 text-signal" : "text-mist"
                    }`}
                  >
                    everything
                  </button>
                </div>
              </div>

              <div className="thin-scroll min-h-[320px] space-y-3 px-4 py-4 lg:max-h-[calc(100dvh-370px)] lg:overflow-y-auto">
                {tan.posts.length === 0 ? (
                  <EmptyState />
                ) : (
                  tan.posts.map((post) => (
                    <PostCard
                      key={post.hash}
                      post={post}
                      onToggleLike={toggleLike}
                      onReply={(target) => setReplyTarget(target)}
                      onDelete={submitDelete}
                      deleting={deleting.has(post.hash)}
                    />
                  ))
                )}
              </div>
            </section>

            <section className="mt-3 rounded-2xl border border-line/80 bg-panel/60 p-3">
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
                    replyTarget
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
                <p className={`text-[11px] ${error ? "text-rose-300" : "text-mist/70"}`}>
                  {error ?? "Enter to post · Shift+Enter for a new line"}
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

function EmptyState() {
  return (
    <div className="flex min-h-[280px] flex-col items-center justify-center px-4 text-center">
      <div className="grid h-20 w-20 place-items-center rounded-full border border-signal/20 bg-signal/[0.04] font-mono text-3xl text-signal/80">
        τ
      </div>
      <h3 className="mt-4 text-sm font-medium text-zinc-200">No posts yet</h3>
      <p className="mt-1.5 max-w-md text-xs leading-relaxed text-mist">
        Start a node with{" "}
        <code className="rounded bg-void/80 px-1 py-0.5 font-mono text-[11px] text-zinc-300">
          tau serve --social --http
        </code>
        , publish a post, then sync or follow another author.
      </p>
    </div>
  );
}

export function PostCard({
  post,
  onToggleLike,
  onReply,
  onDelete,
  deleting,
}: {
  post: TanPost;
  onToggleLike: (post: TanPost) => void;
  onReply: (post: TanPost) => void;
  onDelete: (post: TanPost) => void;
  deleting: boolean;
}) {
  const initial = (post.name || "?").slice(0, 1).toUpperCase();
  const [confirming, setConfirming] = useState(false);
  return (
    <article className="rounded-xl border border-line/70 bg-void/40 p-3.5">
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
          </div>
          {post.kind === "reply" ? (
            <p className="mt-1 truncate text-[11px] text-mist/80">
              in reply to{" "}
              {post.reply_to ? (
                <>
                  {post.reply_to.blocked ? null : (
                    <>{post.reply_to.name || "anonymous"} · </>
                  )}
                  {post.reply_to.blocked
                    ? "a blocked author's post"
                    : post.reply_to.deleted
                      ? "a deleted post"
                      : post.reply_to.text.length > 80
                        ? `${post.reply_to.text.slice(0, 80)}…`
                        : post.reply_to.text}
                </>
              ) : (
                shortHash(post.target ?? "", 16)
              )}
            </p>
          ) : null}
          <p className="mt-1.5 whitespace-pre-wrap break-words text-[13px] leading-relaxed text-zinc-200">
            {post.text}
          </p>
          <div className="mt-2 flex flex-wrap items-center gap-2 font-mono text-[10px] text-mist/60">
            <span className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5">
              {post.prev ? "chained" : "genesis"}
            </span>
            <span>{shortHash(post.hash, 16)}</span>
            <CopyButton value={post.hash} label="hash" className="!px-1.5 !py-0.5" />
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
            <button
              type="button"
              onClick={() => onReply(post)}
              title="Reply to this post"
              className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5 transition-colors hover:text-signal"
            >
              reply
            </button>
            {post.own ? (
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
            ) : null}
          </div>
        </div>
      </div>
    </article>
  );
}
