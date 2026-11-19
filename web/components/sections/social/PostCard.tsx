// PostCard.tsx — one Tan post in the X-style feed: avatar, author,
// relative time, rich text, attached image, poll with vote bars, and the
// like / reply / repost / quote action row plus a ⋯ menu (save, hide, pin,
// share, mute, follow, edit, delete). All rendering is React elements;
// post text is never injected as HTML.

"use client";

import { useState } from "react";
import Avatar from "@/components/chrome/Avatar";
import { formatCountdown, formatRelative, shortHash } from "@/lib/format";
import { tanMediaUrl } from "@/lib/api";
import type { TanPost } from "@/lib/types";
import type { UseTanFeedResult } from "@/lib/use-tan";
import RichText from "./RichText";

export type FeedActions = Pick<
  UseTanFeedResult,
  | "like"
  | "unlike"
  | "repost"
  | "quote"
  | "vote"
  | "share"
  | "save"
  | "unsave"
  | "hide"
  | "pin"
  | "unpin"
  | "deletePost"
  | "editPost"
  | "follow"
  | "unfollow"
  | "muteAuthor"
>;

export type ReactorKind = "likes" | "reposts" | "quotes";

export interface PostCardProps {
  post: TanPost;
  actions: FeedActions;
  /** Peer hashes/names for the share picker (from the live snapshot). */
  peers?: { destination: string; name: string }[];
  /** Open the thread view for this post. */
  onOpenThread?: (post: TanPost) => void;
  /** Start a reply to this post. */
  onReply?: (post: TanPost) => void;
  /** Show the likers / reposters / quoters list for this post. */
  onShowReactors?: (kind: ReactorKind, post: TanPost) => void;
  /** A hashtag in this post was clicked. */
  onTag?: (tag: string) => void;
  /** Highlighted when rendered as the thread root. */
  highlighted?: boolean;
}

function PollView({ post, actions }: { post: TanPost; actions: FeedActions }) {
  const poll = post.poll;
  const options = poll?.options ?? post.options ?? [];
  if (options.length === 0) return null;
  const total = poll?.total ?? 0;
  const closed = poll?.closed ?? false;
  const voted = poll?.voted ?? null;
  return (
    <div className="mt-2 flex flex-col gap-1.5" role="group" aria-label="Poll">
      {options.map((option, i) => {
        const count = poll?.counts[i] ?? 0;
        const pct = total > 0 ? Math.round((count / total) * 100) : 0;
        const mine = voted === i;
        return (
          <button
            key={i}
            type="button"
            disabled={closed || voted !== null}
            onClick={() => void actions.vote(post.hash, i)}
            aria-label={`Vote for ${option}`}
            className={`relative overflow-hidden rounded-lg border px-3 py-1.5 text-left text-sm ${
              mine
                ? "border-cyan-500 text-cyan-100"
                : "border-zinc-700 text-zinc-200 hover:border-zinc-500"
            } disabled:cursor-default`}
          >
            <span
              aria-hidden="true"
              className={`absolute inset-y-0 left-0 ${mine ? "bg-cyan-900" : "bg-zinc-800"}`}
              style={{ width: `${pct}%` }}
            />
            <span className="relative flex items-center justify-between gap-2">
              <span>
                {option}
                {mine ? " ✓" : ""}
              </span>
              <span className="text-xs text-zinc-400">
                {count} · {pct}%
              </span>
            </span>
          </button>
        );
      })}
      <p className="text-xs text-zinc-500">
        {total} vote{total === 1 ? "" : "s"}
        {closed ? " · poll closed" : voted !== null ? " · you voted" : ""}
      </p>
    </div>
  );
}

export default function PostCard({
  post,
  actions,
  peers = [],
  onOpenThread,
  onReply,
  onShowReactors,
  onTag,
  highlighted = false,
}: PostCardProps) {
  const [menuOpen, setMenuOpen] = useState(false);
  const [editing, setEditing] = useState(false);
  const [editText, setEditText] = useState(post.text);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [quoteOpen, setQuoteOpen] = useState(false);
  const [quoteText, setQuoteText] = useState("");
  const [shareOpen, setShareOpen] = useState(false);
  const [shareTarget, setShareTarget] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  // A repost wraps the original: show who reposted, then the original.
  const original = post.kind === "repost" && post.reposted ? post.reposted : null;
  const shown: TanPost = original ?? post;
  const replyCount = shown.replies ?? 0;

  async function run(fn: () => Promise<unknown>) {
    setBusy(true);
    setError(null);
    try {
      await fn();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  function openThread() {
    onOpenThread?.(shown);
  }

  const quoted = shown.quoted;
  const parent = shown.reply_to;

  return (
    <article
      className={`border-b border-zinc-800 px-3 py-3 sm:px-4 ${
        highlighted ? "bg-zinc-900/60" : ""
      }`}
    >
      {original && (
        <p className="mb-1 pl-10 text-xs text-zinc-500">
          🔁 {post.name || shortHash(post.author)} reposted
        </p>
      )}
      {parent && !parent.deleted && (
        <button
          type="button"
          onClick={() => onOpenThread?.({ ...shown, hash: parent.hash } as TanPost)}
          className="mb-1 block pl-10 text-left text-xs text-zinc-500 hover:text-zinc-300"
        >
          Replying to {parent.name || shortHash(parent.author)}
        </button>
      )}
      <div className="flex gap-3">
        <Avatar name={shown.name} seed={shown.author} size="md" />
        <div className="min-w-0 flex-1">
          <div className="flex items-baseline gap-1.5">
            <button
              type="button"
              onClick={openThread}
              className="min-w-0 truncate text-sm font-bold text-zinc-100 hover:underline"
            >
              {shown.name || shortHash(shown.author)}
            </button>
            <span className="shrink-0 text-xs text-zinc-500">
              @{shortHash(shown.author)}
            </span>
            <button
              type="button"
              onClick={openThread}
              title={new Date(shown.created_at * 1000).toLocaleString()}
              className="shrink-0 text-xs text-zinc-500 hover:underline"
            >
              · {formatRelative(shown.created_at)}
            </button>
            {shown.pinned && (
              <span className="shrink-0 text-xs text-amber-400" title="Pinned">
                📌
              </span>
            )}
            <span className="ml-auto flex shrink-0 gap-1">
              {shown.own ? (
                <span className="rounded-full bg-zinc-800 px-2 py-0.5 text-[10px] font-semibold text-zinc-400">
                  you
                </span>
              ) : (
                <button
                  type="button"
                  disabled={busy}
                  onClick={() => void run(() => actions.follow(shown.author))}
                  className="rounded-full border border-zinc-700 px-2 py-0.5 text-[11px] font-semibold text-zinc-300 hover:bg-zinc-900 disabled:opacity-50"
                >
                  Follow
                </button>
              )}
              <button
                type="button"
                aria-label="Post options"
                aria-expanded={menuOpen}
                onClick={() => setMenuOpen((v) => !v)}
                className="rounded-full px-2 text-zinc-500 hover:bg-zinc-900 hover:text-zinc-200"
              >
                ⋯
              </button>
            </span>
          </div>

          {editing ? (
            <div className="mt-2 flex flex-col gap-2">
              <textarea
                value={editText}
                onChange={(e) => setEditText(e.target.value)}
                aria-label="Edit post"
                rows={3}
                className="w-full resize-none rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-500"
              />
              <div className="flex gap-2">
                <button
                  type="button"
                  disabled={busy || !editText.trim()}
                  onClick={() =>
                    void run(async () => {
                      await actions.editPost(shown.hash, editText.trim());
                      setEditing(false);
                    })
                  }
                  className="rounded-full bg-cyan-500 px-3 py-1 text-xs font-bold text-zinc-950 disabled:opacity-40"
                >
                  Save
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setEditing(false);
                    setEditText(shown.text);
                  }}
                  className="rounded-full border border-zinc-700 px-3 py-1 text-xs text-zinc-300"
                >
                  Cancel
                </button>
              </div>
            </div>
          ) : (
            <button
              type="button"
              onClick={openThread}
              className="mt-0.5 block w-full text-left text-sm leading-relaxed whitespace-pre-wrap text-zinc-200"
            >
              <RichText text={shown.text} onTag={onTag} />
            </button>
          )}

          {shown.image?.sha256 && (
            <img
              src={tanMediaUrl(shown.image.sha256)}
              alt="Image attached to post"
              className="mt-2 max-h-80 w-full rounded-xl border border-zinc-800 object-cover"
              loading="lazy"
            />
          )}

          {shown.location && (
            <p className="mt-2 text-sm">
              <a
                href={`https://www.openstreetmap.org/?mlat=${shown.location.lat}&mlon=${shown.location.lon}#map=15/${shown.location.lat}/${shown.location.lon}`}
                target="_blank"
                rel="noreferrer"
                className="text-cyan-300 underline"
              >
                📍{" "}
                {shown.location.name ??
                  `${shown.location.lat.toFixed(5)}, ${shown.location.lon.toFixed(5)}`}
              </a>
            </p>
          )}

          {quoted && (
            <div className="mt-2 rounded-xl border border-zinc-800 p-3">
              <p className="text-xs text-zinc-500">
                {quoted.name || shortHash(quoted.author)} ·{" "}
                {quoted.deleted ? "deleted post" : quoted.text.slice(0, 120)}
              </p>
            </div>
          )}

          {shown.kind === "poll" && <PollView post={shown} actions={actions} />}

          {shown.expires_at && (
            <p className="mt-1 text-xs text-zinc-500">
              ⏱ Disappears {formatCountdown(shown.expires_at)}
            </p>
          )}

          {menuOpen && (
            <div
              className="mt-2 flex flex-wrap gap-1.5 rounded-lg border border-zinc-800 bg-zinc-900 p-2"
              role="menu"
              aria-label="Post actions"
            >
              <button
                type="button"
                disabled={busy}
                onClick={() =>
                  void run(async () => {
                    if (shown.saved) await actions.unsave(shown.hash);
                    else await actions.save(shown.hash);
                    setMenuOpen(false);
                  })
                }
                className="rounded-full border border-zinc-700 px-2.5 py-1 text-xs text-zinc-300 hover:bg-zinc-800 disabled:opacity-50"
              >
                {shown.saved ? "★ Unsave" : "☆ Save"}
              </button>
              <button
                type="button"
                disabled={busy}
                onClick={() =>
                  void run(async () => {
                    await actions.hide(shown.hash);
                    setMenuOpen(false);
                  })
                }
                className="rounded-full border border-zinc-700 px-2.5 py-1 text-xs text-zinc-300 hover:bg-zinc-800 disabled:opacity-50"
              >
                🙈 Hide
              </button>
              <button
                type="button"
                disabled={busy}
                onClick={() =>
                  void run(async () => {
                    if (shown.pinned) await actions.unpin(shown.hash);
                    else await actions.pin(shown.hash);
                    setMenuOpen(false);
                  })
                }
                className="rounded-full border border-zinc-700 px-2.5 py-1 text-xs text-zinc-300 hover:bg-zinc-800 disabled:opacity-50"
              >
                {shown.pinned ? "Unpin" : "📌 Pin"}
              </button>
              <button
                type="button"
                onClick={() => {
                  setShareOpen((v) => !v);
                }}
                className="rounded-full border border-zinc-700 px-2.5 py-1 text-xs text-zinc-300 hover:bg-zinc-800"
              >
                ↗ Share
              </button>
              {!shown.own && (
                <button
                  type="button"
                  disabled={busy}
                  onClick={() =>
                    void run(async () => {
                      await actions.muteAuthor(shown.author);
                      setMenuOpen(false);
                    })
                  }
                  className="rounded-full border border-zinc-700 px-2.5 py-1 text-xs text-zinc-300 hover:bg-zinc-800 disabled:opacity-50"
                >
                  🔇 Mute author
                </button>
              )}
              {shown.own && (
                <>
                  <button
                    type="button"
                    onClick={() => {
                      setEditing(true);
                      setMenuOpen(false);
                    }}
                    className="rounded-full border border-zinc-700 px-2.5 py-1 text-xs text-zinc-300 hover:bg-zinc-800"
                  >
                    ✏️ Edit
                  </button>
                  {confirmDelete ? (
                    <button
                      type="button"
                      disabled={busy}
                      onClick={() =>
                        void run(async () => {
                          await actions.deletePost(shown.hash);
                          setConfirmDelete(false);
                          setMenuOpen(false);
                        })
                      }
                      className="rounded-full border border-red-800 bg-red-950 px-2.5 py-1 text-xs font-semibold text-red-300 disabled:opacity-50"
                    >
                      Confirm delete?
                    </button>
                  ) : (
                    <button
                      type="button"
                      onClick={() => setConfirmDelete(true)}
                      className="rounded-full border border-red-800 px-2.5 py-1 text-xs text-red-400 hover:bg-red-950"
                    >
                      🗑 Delete
                    </button>
                  )}
                </>
              )}
            </div>
          )}

          {shareOpen && (
            <div className="mt-2 flex flex-wrap items-center gap-2 rounded-lg border border-zinc-800 bg-zinc-900 p-2">
              <label className="text-xs text-zinc-400" htmlFor={`share-${shown.hash.slice(0, 8)}`}>
                Share with
              </label>
              <select
                id={`share-${shown.hash.slice(0, 8)}`}
                value={shareTarget}
                onChange={(e) => setShareTarget(e.target.value)}
                className="min-w-0 flex-1 rounded-md border border-zinc-700 bg-zinc-950 px-2 py-1 text-xs text-zinc-200 outline-none"
              >
                <option value="">Choose a peer…</option>
                {peers.map((p) => (
                  <option key={p.destination} value={p.destination}>
                    {p.name || shortHash(p.destination)}
                  </option>
                ))}
              </select>
              <button
                type="button"
                disabled={busy || !shareTarget}
                onClick={() =>
                  void run(async () => {
                    await actions.share(shown.hash, { to: shareTarget });
                    setShareOpen(false);
                  })
                }
                className="rounded-full bg-cyan-500 px-3 py-1 text-xs font-bold text-zinc-950 disabled:opacity-40"
              >
                Share
              </button>
            </div>
          )}

          {quoteOpen && (
            <div className="mt-2 flex flex-col gap-2 rounded-lg border border-zinc-800 bg-zinc-900 p-2">
              <textarea
                value={quoteText}
                onChange={(e) => setQuoteText(e.target.value)}
                placeholder="Add your take…"
                aria-label="Quote text"
                rows={2}
                className="w-full resize-none rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none focus:border-cyan-500"
              />
              <div className="flex gap-2">
                <button
                  type="button"
                  disabled={busy || !quoteText.trim()}
                  onClick={() =>
                    void run(async () => {
                      await actions.quote(shown.hash, quoteText.trim());
                      setQuoteText("");
                      setQuoteOpen(false);
                    })
                  }
                  className="rounded-full bg-cyan-500 px-3 py-1 text-xs font-bold text-zinc-950 disabled:opacity-40"
                >
                  Post quote
                </button>
                <button
                  type="button"
                  onClick={() => setQuoteOpen(false)}
                  className="rounded-full border border-zinc-700 px-3 py-1 text-xs text-zinc-300"
                >
                  Cancel
                </button>
              </div>
            </div>
          )}

          <div className="mt-2 flex items-center justify-between text-xs text-zinc-500">
            <button
              type="button"
              aria-label={`Reply to post, ${replyCount} replies`}
              onClick={() => onReply?.(shown)}
              className="flex items-center gap-1 rounded-full px-2 py-1 hover:bg-zinc-900 hover:text-cyan-400"
            >
              💬 {replyCount > 0 ? replyCount : ""}
              <span className="sr-only">Reply</span>
            </button>
            <button
              type="button"
              aria-label={`Repost post, ${shown.quotes} quotes`}
              disabled={busy}
              onClick={() => void run(() => actions.repost(shown.hash))}
              className="flex items-center gap-1 rounded-full px-2 py-1 hover:bg-zinc-900 hover:text-emerald-400 disabled:opacity-50"
            >
              🔁
            </button>
            <button
              type="button"
              aria-label={`Like post, ${shown.likes} likes`}
              disabled={busy}
              onClick={() =>
                void run(() =>
                  shown.liked ? actions.unlike(shown.hash) : actions.like(shown.hash),
                )
              }
              className={`flex items-center gap-1 rounded-full px-2 py-1 hover:bg-zinc-900 ${
                shown.liked ? "text-rose-400" : "hover:text-rose-400"
              } disabled:opacity-50`}
            >
              {shown.liked ? "♥" : "♡"} {shown.likes > 0 ? shown.likes : ""}
            </button>
            <button
              type="button"
              aria-label={`Show likers, ${shown.likes} likes`}
              onClick={() => onShowReactors?.("likes", shown)}
              className="rounded-full px-2 py-1 hover:bg-zinc-900 hover:text-zinc-200"
            >
              Liked by
            </button>
            <button
              type="button"
              aria-label={`Show quotes, ${shown.quotes} quotes`}
              onClick={() => {
                if (shown.quotes > 0) onShowReactors?.("quotes", shown);
                else setQuoteOpen((v) => !v);
              }}
              className="rounded-full px-2 py-1 hover:bg-zinc-900 hover:text-zinc-200"
            >
              ❝ {shown.quotes > 0 ? shown.quotes : ""}
            </button>
          </div>
          {error && (
            <p role="alert" className="mt-1 text-xs text-red-400">
              {error}
            </p>
          )}
        </div>
      </div>
    </article>
  );
}
