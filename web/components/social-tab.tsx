// Social (Tan) tab for the rewritten console: composer, feed with
// Timeline / Top / Mentions views, threads, polls, and a Discover
// column. All data flows through the useTan hook — this component
// never fetches directly.
"use client";

import { useState } from "react";
import type { ReactNode } from "react";

import {
  casefoldTag,
  formatDateTime,
  formatExpiresIn,
  shortHash,
} from "@/lib/format";
import type { TanPost } from "@/lib/types";
import type { UseTanResult } from "@/lib/use-tan";
import type { UseTauResult } from "@/lib/use-tau";

import { Button, Card, CardTitle, Chip, EmptyState, ErrorNote } from "./ui";

const TAG_PATTERN = /(?<![\p{L}\p{N}_])#([\p{L}][\p{L}\p{N}_-]*)/gu;

const EXPIRY_OPTIONS: { label: string; seconds: number | null }[] = [
  { label: "never expires", seconds: null },
  { label: "disappears in 1 hour", seconds: 3600 },
  { label: "disappears in 1 day", seconds: 86400 },
  { label: "disappears in 7 days", seconds: 604800 },
];

function errorMessage(caught: unknown): string {
  return caught instanceof Error ? caught.message : "request failed";
}

/** Post text with #tags as filter buttons; @mentions stay plain text. */
function renderPostText(
  text: string,
  onTag: (tag: string) => void,
): ReactNode[] {
  const parts: ReactNode[] = [];
  let last = 0;
  let key = 0;
  for (const match of text.matchAll(TAG_PATTERN)) {
    const index = match.index ?? 0;
    if (index > last) {
      parts.push(<span key={key++}>{text.slice(last, index)}</span>);
    }
    const tag = match[1];
    parts.push(
      <button
        key={key++}
        type="button"
        onClick={() => onTag(casefoldTag(tag))}
        className="min-h-6 shrink-0 rounded px-0.5 text-signal hover:underline"
      >
        #{tag}
      </button>,
    );
    last = index + match[0].length;
  }
  if (last < text.length) {
    parts.push(<span key={key++}>{text.slice(last)}</span>);
  }
  return parts;
}

function ParentQuote({
  label,
  name,
  text,
  deleted,
}: {
  label: string;
  name: string;
  text: string;
  deleted: boolean;
}) {
  return (
    <div className="rounded-lg border border-line-soft bg-panel-2 px-2.5 py-2 text-sm break-words">
      <p className="text-xs text-faint">
        {label} {name}
      </p>
      <p className="mt-0.5 text-mist">
        {deleted ? "deleted post" : text}
      </p>
    </div>
  );
}

function PostCard({
  post,
  tan,
  tau,
  onTag,
  onReply,
}: {
  post: TanPost;
  tan: UseTanResult;
  tau: UseTauResult;
  onTag: (tag: string) => void;
  onReply: (post: TanPost) => void;
}) {
  const [editing, setEditing] = useState(false);
  const [editText, setEditText] = useState(post.text);
  const [quoting, setQuoting] = useState(false);
  const [quoteText, setQuoteText] = useState("");
  const [sharing, setSharing] = useState(false);
  const [shareTarget, setShareTarget] = useState("");
  const [shareNote, setShareNote] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<
    "likers" | "reposters" | "quoters" | null
  >(null);
  const [likers, setLikers] = useState<{ name: string; author: string }[]>(
    [],
  );
  const [reposters, setReposters] = useState<
    { name: string; author: string }[]
  >([]);
  const [quoters, setQuoters] = useState<
    { name: string; author: string; text: string }[]
  >([]);
  const [err, setErr] = useState<string | null>(null);

  const isFollowing = tan.following.some((f) => f.author === post.author);
  const expires = formatExpiresIn(post.expires_at);

  async function run(action: () => Promise<unknown>) {
    setErr(null);
    try {
      await action();
    } catch (caught) {
      setErr(errorMessage(caught));
    }
  }

  async function toggleExpanded(kind: "likers" | "reposters" | "quoters") {
    if (expanded === kind) {
      setExpanded(null);
      return;
    }
    setErr(null);
    try {
      if (kind === "likers") {
        const result = await tan.fetchLikers(post.hash);
        setLikers(result.likers);
      } else if (kind === "reposters") {
        const result = await tan.fetchReposters(post.hash);
        setReposters(result.reposters);
      } else {
        const result = await tan.fetchQuoters(post.hash);
        setQuoters(result.quoters);
      }
      setExpanded(kind);
    } catch (caught) {
      setErr(errorMessage(caught));
    }
  }

  async function submitShare() {
    if (shareTarget === "") return;
    setShareNote(null);
    await run(async () => {
      if (shareTarget.startsWith("group:")) {
        await tan.share(post.hash, undefined, shareTarget.slice(6));
        setShareNote("shared to group");
      } else {
        await tan.share(post.hash, shareTarget);
        setShareNote("shared to peer");
      }
      setSharing(false);
    });
  }

  const depth = post.depth ?? 0;
  return (
    <article
      className="min-w-0 rounded-xl border border-line bg-panel px-3 py-3"
      style={depth > 0 ? { marginLeft: `${Math.min(depth, 4) * 12}px` } : undefined}
    >
      <div className="flex min-w-0 flex-wrap items-baseline gap-x-2 gap-y-0.5">
        <span className="min-w-0 truncate text-sm font-semibold">
          {post.name || "unknown"}
        </span>
        <span
          className="shrink-0 font-mono text-xs text-faint"
          title={post.author}
        >
          {shortHash(post.author, 8)}
        </span>
        <span className="shrink-0 text-xs text-faint">
          {formatDateTime(post.created_at)}
        </span>
        {post.pinned ? <Chip>pinned</Chip> : null}
        {post.saved ? <Chip>saved</Chip> : null}
        {expires !== null ? <Chip>{expires}</Chip> : null}
      </div>

      {post.reply_to ? (
        <div className="mt-2">
          <ParentQuote
            label="replied to"
            name={post.reply_to.name}
            text={post.reply_to.blocked ? "" : post.reply_to.text}
            deleted={post.reply_to.deleted}
          />
        </div>
      ) : null}
      {post.repost_of ? (
        <div className="mt-2">
          <ParentQuote
            label="reposted"
            name={post.repost_of.name}
            text={post.repost_of.blocked ? "" : post.repost_of.text}
            deleted={post.repost_of.deleted}
          />
        </div>
      ) : null}
      {post.quoted ? (
        <div className="mt-2">
          <ParentQuote
            label="quoted"
            name={post.quoted.name}
            text={post.quoted.blocked ? "" : post.quoted.text}
            deleted={post.quoted.deleted}
          />
        </div>
      ) : null}

      {editing ? (
        <div className="mt-2 flex min-w-0 flex-col gap-2">
          <textarea
            value={editText}
            onChange={(event) => setEditText(event.target.value)}
            rows={3}
            aria-label="Edit post"
            className="min-w-0 rounded-lg border border-line bg-void/70 px-2.5 py-2 text-sm break-words focus:border-signal/60 focus:outline-none"
          />
          <div className="flex flex-wrap gap-2">
            <Button
              variant="primary"
              disabled={editText.trim() === ""}
              onClick={() =>
                void run(async () => {
                  await tan.editPost(post.hash, editText.trim());
                  setEditing(false);
                })
              }
            >
              Save edit
            </Button>
            <Button
              onClick={() => {
                setEditText(post.text);
                setEditing(false);
              }}
            >
              Cancel
            </Button>
          </div>
        </div>
      ) : (
        <p className="mt-2 text-sm break-words">
          {renderPostText(post.text, onTag)}
        </p>
      )}

      {post.image ? (
        <img
          src={`/api/tan/media/${post.image.sha256}`}
          alt={post.image.name}
          loading="lazy"
          className="mt-2 max-w-full rounded-lg"
        />
      ) : null}

      {post.poll ? (
        <div className="mt-2 flex min-w-0 flex-col gap-1.5">
          {post.poll.options.map((option, index) => {
            const count = post.poll?.counts[index] ?? 0;
            const total = post.poll?.total ?? 0;
            const percent =
              total > 0 ? Math.round((count / total) * 100) : 0;
            return (
              <button
                key={index}
                type="button"
                onClick={() => void run(() => tan.vote(post.hash, index))}
                className="flex min-h-9 min-w-0 items-center justify-between gap-2 rounded-lg border border-line bg-panel-2 px-2.5 py-1.5 text-left text-sm hover:border-signal-dim"
              >
                <span className="min-w-0 truncate">
                  {option}
                  {post.poll?.voted === index ? " ✓" : ""}
                </span>
                <span className="shrink-0 text-xs text-mist">
                  {count} · {percent}%
                </span>
              </button>
            );
          })}
          <p className="text-xs text-faint">
            {post.poll.total} vote{post.poll.total === 1 ? "" : "s"}
          </p>
        </div>
      ) : null}

      <div className="mt-2 flex flex-wrap items-center gap-1.5">
        <Button
          variant="subtle"
          onClick={() => void run(() => tan.like(post.hash, !post.liked))}
          title={post.liked ? "Unlike" : "Like"}
        >
          {post.liked ? "♥" : "♡"} {post.likes ?? 0}
        </Button>
        <Button variant="subtle" onClick={() => onReply(post)}>
          Reply
        </Button>
        <Button
          variant="subtle"
          onClick={() => void run(() => tan.repost(post.hash))}
        >
          Repost {post.reposts ? `· ${post.reposts}` : ""}
        </Button>
        <Button variant="subtle" onClick={() => setQuoting(!quoting)}>
          Quote{post.quotes ? ` · ${post.quotes}` : ""}
        </Button>
        <Button variant="subtle" onClick={() => setSharing(!sharing)}>
          Share
        </Button>
        <Button
          variant="subtle"
          onClick={() => void run(() => tan.savePost(post.hash, !post.saved))}
        >
          {post.saved ? "Saved" : "Save"}
        </Button>
        <Button
          variant="subtle"
          onClick={() => void run(() => tan.pinPost(post.hash, !post.pinned))}
        >
          {post.pinned ? "Unpin" : "Pin"}
        </Button>
        {post.own ? (
          <>
            <Button
              variant="subtle"
              onClick={() => {
                setEditText(post.text);
                setEditing(!editing);
              }}
            >
              Edit
            </Button>
            <Button
              variant="danger"
              onClick={() => void run(() => tan.deletePost(post.hash))}
            >
              Delete
            </Button>
          </>
        ) : (
          <>
            <Button
              variant="subtle"
              onClick={() =>
                void run(() => tan.follow(post.author, !isFollowing))
              }
            >
              {isFollowing ? "Following" : "Follow"}
            </Button>
            <Button
              variant="subtle"
              onClick={() => void run(() => tan.muteAuthor(post.author, true))}
            >
              Mute
            </Button>
          </>
        )}
        <Button
          variant="subtle"
          onClick={() => void run(() => tan.hidePost(post.hash, true))}
        >
          Hide
        </Button>
      </div>

      {quoting ? (
        <div className="mt-2 flex min-w-0 flex-col gap-2">
          <textarea
            value={quoteText}
            onChange={(event) => setQuoteText(event.target.value)}
            rows={2}
            placeholder="Add your words…"
            aria-label="Quote text"
            className="min-w-0 rounded-lg border border-line bg-void/70 px-2.5 py-2 text-sm break-words focus:border-signal/60 focus:outline-none"
          />
          <div className="flex flex-wrap gap-2">
            <Button
              variant="primary"
              disabled={quoteText.trim() === ""}
              onClick={() =>
                void run(async () => {
                  await tan.quote(post.hash, quoteText.trim());
                  setQuoteText("");
                  setQuoting(false);
                })
              }
            >
              Post quote
            </Button>
            <Button onClick={() => setQuoting(false)}>Cancel</Button>
          </div>
        </div>
      ) : null}

      {sharing ? (
        <div className="mt-2 flex min-w-0 flex-col gap-2">
          <select
            value={shareTarget}
            onChange={(event) => setShareTarget(event.target.value)}
            aria-label="Share target"
            className="min-h-9 min-w-0 rounded-lg border border-line bg-void/70 px-2.5 text-sm focus:border-signal/60 focus:outline-none"
          >
            <option value="">share to a peer or group…</option>
            <optgroup label="Peers">
              {tau.peers.map((peer) => (
                <option key={peer.destination} value={peer.destination}>
                  {peer.name || shortHash(peer.destination, 8)}
                </option>
              ))}
            </optgroup>
            <optgroup label="Groups">
              {tau.groups.map((group) => (
                <option key={group.id} value={`group:${group.id}`}>
                  {group.name}
                </option>
              ))}
            </optgroup>
          </select>
          <div className="flex flex-wrap gap-2">
            <Button
              variant="primary"
              disabled={shareTarget === ""}
              onClick={() => void submitShare()}
            >
              Share
            </Button>
            <Button onClick={() => setSharing(false)}>Cancel</Button>
          </div>
          {shareNote !== null ? (
            <p className="text-xs text-mist">{shareNote}</p>
          ) : null}
        </div>
      ) : null}

      <div className="mt-1 flex flex-wrap gap-1.5">
        <button
          type="button"
          onClick={() => void toggleExpanded("likers")}
          className="min-h-6 rounded px-1 text-xs text-faint hover:text-signal"
        >
          {post.likes ?? 0} likes
        </button>
        <button
          type="button"
          onClick={() => void toggleExpanded("reposters")}
          className="min-h-6 rounded px-1 text-xs text-faint hover:text-signal"
        >
          {post.reposts ?? 0} reposts
        </button>
        <button
          type="button"
          onClick={() => void toggleExpanded("quoters")}
          className="min-h-6 rounded px-1 text-xs text-faint hover:text-signal"
        >
          {post.quotes ?? 0} quotes
        </button>
      </div>
      {expanded === "likers" ? (
        <ul className="mt-1 flex flex-col gap-0.5 border-t border-line-soft pt-1 text-sm">
          {likers.length === 0 ? (
            <li className="text-xs text-faint">no likes yet</li>
          ) : (
            likers.map((liker) => (
              <li key={liker.author} className="min-w-0 truncate">
                {liker.name}{" "}
                <span className="font-mono text-xs text-faint">
                  {shortHash(liker.author, 8)}
                </span>
              </li>
            ))
          )}
        </ul>
      ) : null}
      {expanded === "reposters" ? (
        <ul className="mt-1 flex flex-col gap-0.5 border-t border-line-soft pt-1 text-sm">
          {reposters.length === 0 ? (
            <li className="text-xs text-faint">no reposts yet</li>
          ) : (
            reposters.map((reposter) => (
              <li key={reposter.author} className="min-w-0 truncate">
                {reposter.name}{" "}
                <span className="font-mono text-xs text-faint">
                  {shortHash(reposter.author, 8)}
                </span>
              </li>
            ))
          )}
        </ul>
      ) : null}
      {expanded === "quoters" ? (
        <ul className="mt-1 flex flex-col gap-0.5 border-t border-line-soft pt-1 text-sm">
          {quoters.length === 0 ? (
            <li className="text-xs text-faint">no quotes yet</li>
          ) : (
            quoters.map((quoter) => (
              <li key={quoter.author} className="min-w-0 break-words">
                <span className="font-medium">{quoter.name}</span>{" "}
                <span className="text-mist">{quoter.text}</span>
              </li>
            ))
          )}
        </ul>
      ) : null}

      {err !== null ? (
        <div className="mt-2">
          <ErrorNote message={err} />
        </div>
      ) : null}
    </article>
  );
}

export function SocialTab({
  tau,
  tan,
}: {
  tau: UseTauResult;
  tan: UseTanResult;
}) {
  const [text, setText] = useState("");
  const [replyTo, setReplyTo] = useState<TanPost | null>(null);
  const [pollMode, setPollMode] = useState(false);
  const [pollOptions, setPollOptions] = useState<string[]>(["", ""]);
  const [scheduleAt, setScheduleAt] = useState("");
  const [expireSeconds, setExpireSeconds] = useState<number | null>(null);
  const [searchText, setSearchText] = useState("");
  const [composerError, setComposerError] = useState<string | null>(null);

  const feed =
    tan.searchQuery !== null
      ? tan.searchResults
      : tan.view === "mentions"
        ? tan.mentions
        : tan.posts;
  const inThread = tan.threadRef !== null;

  function applyTag(tag: string) {
    tan.setSearchQuery(null);
    setSearchText("");
    tan.setTagFilter(tag);
  }

  function clearFilters() {
    tan.setTagFilter(null);
    tan.setSearchQuery(null);
    setSearchText("");
  }

  function submitSearch() {
    const query = searchText.trim();
    if (query === "") return;
    tan.setTagFilter(null);
    tan.setSearchQuery(query);
  }

  function enterPollMode() {
    setPollMode(true);
    setReplyTo(null);
    setScheduleAt("");
  }

  function exitPollMode() {
    setPollMode(false);
    setPollOptions(["", ""]);
  }

  function handleReply(post: TanPost) {
    setReplyTo(post);
    exitPollMode();
    tan.openThread(post.hash);
  }

  function resetComposer() {
    setText("");
    setReplyTo(null);
    setScheduleAt("");
    setExpireSeconds(null);
    exitPollMode();
  }

  async function submitPost() {
    const body = text.trim();
    if (body === "") return;
    setComposerError(null);
    try {
      if (pollMode) {
        const options = pollOptions
          .map((option) => option.trim())
          .filter((option) => option !== "");
        if (options.length < 2) {
          setComposerError("a poll needs at least two options");
          return;
        }
        await tan.publishPoll(body, options, expireSeconds ?? undefined);
      } else if (scheduleAt !== "") {
        const publishAt = Math.floor(new Date(scheduleAt).getTime() / 1000);
        if (!Number.isFinite(publishAt) || publishAt <= Date.now() / 1000) {
          setComposerError("pick a future time to schedule");
          return;
        }
        await tan.schedulePost({
          text: body,
          publishAt,
          replyTo: replyTo?.hash ?? null,
          expiresIn: expireSeconds ?? undefined,
        });
      } else {
        await tan.publish({
          text: body,
          replyTo: replyTo?.hash,
          expiresIn: expireSeconds ?? undefined,
        });
      }
      resetComposer();
    } catch (caught) {
      setComposerError(errorMessage(caught));
    }
  }

  return (
    <div
      data-testid="social-tab"
      className="flex min-w-0 flex-col gap-4 lg:grid lg:grid-cols-[minmax(0,1fr)_280px] lg:items-start"
    >
      <div className="flex min-w-0 flex-col gap-4">
        <Card>
          <CardTitle>New post</CardTitle>
          <div className="flex min-w-0 flex-col gap-2 px-3 py-3">
            {replyTo !== null ? (
              <p className="flex min-w-0 flex-wrap items-center gap-2 rounded-lg border border-line-soft bg-panel-2 px-2.5 py-2 text-sm">
                <span className="min-w-0 truncate">
                  Replying to {replyTo.name || "unknown"} ·{" "}
                  {shortHash(replyTo.author, 8)}
                </span>
                <Button variant="subtle" onClick={() => setReplyTo(null)}>
                  Cancel reply
                </Button>
              </p>
            ) : null}
            <textarea
              value={text}
              onChange={(event) => setText(event.target.value)}
              rows={3}
              placeholder={
                pollMode ? "Poll question…" : "What’s happening on the mesh?"
              }
              aria-label={pollMode ? "Poll question" : "New post"}
              className="min-w-0 rounded-lg border border-line bg-void/70 px-2.5 py-2 text-sm break-words focus:border-signal/60 focus:outline-none"
            />
            {pollMode ? (
              <div className="flex min-w-0 flex-col gap-2">
                {pollOptions.map((option, index) => (
                  <div
                    key={index}
                    className="flex min-w-0 items-center gap-2"
                  >
                    <input
                      value={option}
                      onChange={(event) => {
                        const next = [...pollOptions];
                        next[index] = event.target.value;
                        setPollOptions(next);
                      }}
                      placeholder={`Option ${index + 1}`}
                      aria-label={`Poll option ${index + 1}`}
                      className="min-h-9 min-w-0 flex-1 rounded-lg border border-line bg-void/70 px-2.5 text-sm focus:border-signal/60 focus:outline-none"
                    />
                    {pollOptions.length > 2 ? (
                      <Button
                        variant="subtle"
                        onClick={() =>
                          setPollOptions(
                            pollOptions.filter((_, i) => i !== index),
                          )
                        }
                      >
                        Remove
                      </Button>
                    ) : null}
                  </div>
                ))}
                {pollOptions.length < 6 ? (
                  <Button
                    variant="subtle"
                    onClick={() => setPollOptions([...pollOptions, ""])}
                  >
                    Add option
                  </Button>
                ) : null}
              </div>
            ) : null}
            <div className="flex flex-wrap items-center gap-2">
              <label className="flex min-w-0 items-center gap-1.5 text-xs text-mist">
                <span className="shrink-0">Expires</span>
                <select
                  value={expireSeconds === null ? "" : String(expireSeconds)}
                  onChange={(event) =>
                    setExpireSeconds(
                      event.target.value === ""
                        ? null
                        : Number(event.target.value),
                    )
                  }
                  aria-label="Post expiry"
                  className="min-h-9 min-w-0 rounded-lg border border-line bg-void/70 px-2 text-xs focus:border-signal/60 focus:outline-none"
                >
                  {EXPIRY_OPTIONS.map((option) => (
                    <option
                      key={option.label}
                      value={
                        option.seconds === null ? "" : String(option.seconds)
                      }
                    >
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
              {!pollMode ? (
                <label className="flex min-w-0 items-center gap-1.5 text-xs text-mist">
                  <span className="shrink-0">Schedule</span>
                  <input
                    type="datetime-local"
                    value={scheduleAt}
                    onChange={(event) => setScheduleAt(event.target.value)}
                    aria-label="Schedule publish time"
                    className="min-h-9 min-w-0 rounded-lg border border-line bg-void/70 px-2 text-xs focus:border-signal/60 focus:outline-none"
                  />
                </label>
              ) : null}
              <span className="min-w-0 flex-1" />
              <Button
                variant="subtle"
                onClick={() => (pollMode ? exitPollMode() : enterPollMode())}
              >
                {pollMode ? "Cancel poll" : "Poll"}
              </Button>
              <Button
                variant="primary"
                disabled={tan.posting || text.trim() === ""}
                onClick={() => void submitPost()}
              >
                {tan.posting
                  ? "Posting…"
                  : scheduleAt !== "" && !pollMode
                    ? "Schedule"
                    : "Post"}
              </Button>
            </div>
            <ErrorNote message={composerError} />
          </div>
        </Card>

        <Card>
          <div className="flex min-w-0 flex-col gap-2 px-3 py-3">
            <div className="flex flex-wrap items-center gap-1.5">
              {(["timeline", "top", "mentions"] as const).map((v) => (
                <Button
                  key={v}
                  variant={tan.view === v ? "primary" : "ghost"}
                  onClick={() => {
                    clearFilters();
                    tan.setView(v);
                  }}
                >
                  {v === "timeline"
                    ? "Timeline"
                    : v === "top"
                      ? "Top"
                      : "Mentions"}
                </Button>
              ))}
              <span className="mx-1 hidden h-5 w-px bg-line sm:block" />
              <Button
                variant={!tan.showAll ? "primary" : "ghost"}
                onClick={() => tan.setShowAll(false)}
              >
                Following
              </Button>
              <Button
                variant={tan.showAll ? "primary" : "ghost"}
                onClick={() => tan.setShowAll(true)}
              >
                All
              </Button>
              <span className="min-w-0 flex-1" />
              <Button
                variant="subtle"
                disabled={tan.syncing}
                onClick={() => void tan.sync().catch(() => undefined)}
              >
                {tan.syncing ? "Syncing…" : "Sync"}
              </Button>
              <Button variant="subtle" onClick={() => void tan.refresh()}>
                Refresh
              </Button>
            </div>
            <form
              className="flex min-w-0 gap-2"
              onSubmit={(event) => {
                event.preventDefault();
                submitSearch();
              }}
            >
              <input
                value={searchText}
                onChange={(event) => setSearchText(event.target.value)}
                placeholder="Search posts…"
                aria-label="Search posts"
                className="min-h-9 min-w-0 flex-1 rounded-lg border border-line bg-void/70 px-2.5 text-sm focus:border-signal/60 focus:outline-none"
              />
              <Button type="submit" disabled={searchText.trim() === ""}>
                Search
              </Button>
            </form>
            {tan.tagFilter !== null || tan.searchQuery !== null ? (
              <div className="flex flex-wrap items-center gap-2">
                {tan.tagFilter !== null ? (
                  <Chip>
                    #{tan.tagFilter}
                    <button
                      type="button"
                      onClick={() => tan.setTagFilter(null)}
                      aria-label="Clear tag filter"
                      className="min-h-6 rounded px-1 hover:text-signal"
                    >
                      ×
                    </button>
                  </Chip>
                ) : null}
                {tan.searchQuery !== null ? (
                  <Chip>
                    “{tan.searchQuery}”
                    <button
                      type="button"
                      onClick={() => {
                        tan.setSearchQuery(null);
                        setSearchText("");
                      }}
                      aria-label="Clear search"
                      className="min-h-6 rounded px-1 hover:text-signal"
                    >
                      ×
                    </button>
                  </Chip>
                ) : null}
                <Button variant="subtle" onClick={clearFilters}>
                  Clear filters
                </Button>
              </div>
            ) : null}
            <ErrorNote message={tan.error} />
          </div>
        </Card>

        {inThread ? (
          <Card>
            <CardTitle
              right={
                <Button variant="subtle" onClick={() => tan.openThread(null)}>
                  ← Back to feed
                </Button>
              }
            >
              Thread
            </CardTitle>
            <div className="flex min-w-0 flex-col gap-2 px-3 py-3">
              {tan.threadError !== null ? (
                <ErrorNote message={tan.threadError} />
              ) : null}
              {tan.threadPosts.length === 0 ? (
                <EmptyState>No posts in this thread yet.</EmptyState>
              ) : (
                tan.threadPosts.map((post) => (
                  <PostCard
                    key={post.hash}
                    post={post}
                    tan={tan}
                    tau={tau}
                    onTag={applyTag}
                    onReply={handleReply}
                  />
                ))
              )}
            </div>
          </Card>
        ) : (
          <div className="flex min-w-0 flex-col gap-2">
            {tan.loading ? (
              <EmptyState>Loading posts…</EmptyState>
            ) : feed.length === 0 ? (
              <EmptyState>
                No posts yet — write the first one above, or sync with a
                peer.
              </EmptyState>
            ) : (
              feed.map((post) => (
                <PostCard
                  key={post.hash}
                  post={post}
                  tan={tan}
                  tau={tau}
                  onTag={applyTag}
                  onReply={handleReply}
                />
              ))
            )}
          </div>
        )}
      </div>

      <aside className="flex min-w-0 flex-col gap-4">
        <Card>
          <CardTitle>Discover</CardTitle>
          <div className="flex min-w-0 flex-col gap-3 px-3 py-3">
            <div>
              <h3 className="text-xs font-semibold tracking-widest text-mist uppercase">
                Popular tags
              </h3>
              {tan.tags.length === 0 ? (
                <p className="mt-1 text-xs text-faint">No tags yet.</p>
              ) : (
                <div className="mt-1.5 flex flex-wrap gap-1.5">
                  {tan.tags.slice(0, 20).map((tag) => (
                    <button
                      key={tag.tag}
                      type="button"
                      onClick={() => applyTag(tag.tag)}
                      className="min-h-9 rounded-full border border-line bg-panel-2 px-2.5 text-xs text-mist hover:border-signal-dim hover:text-signal"
                    >
                      #{tag.tag} · {tag.count}
                    </button>
                  ))}
                </div>
              )}
            </div>
            <div>
              <h3 className="text-xs font-semibold tracking-widest text-mist uppercase">
                Suggested authors
              </h3>
              {tan.suggestions.length === 0 ? (
                <p className="mt-1 text-xs text-faint">
                  No suggestions yet.
                </p>
              ) : (
                <ul className="mt-1.5 flex flex-col gap-1.5">
                  {tan.suggestions.map((suggestion) => (
                    <li
                      key={suggestion.author}
                      className="flex min-w-0 items-center gap-2"
                    >
                      <span className="min-w-0 flex-1 truncate text-sm">
                        {suggestion.name}{" "}
                        <span className="text-xs text-faint">
                          · {suggestion.posts} posts
                          {suggestion.reason
                            ? ` · ${suggestion.reason}`
                            : ""}
                        </span>
                      </span>
                      <Button
                        variant="subtle"
                        onClick={() =>
                          void tan
                            .follow(suggestion.author, true)
                            .catch(() => undefined)
                        }
                      >
                        Follow
                      </Button>
                    </li>
                  ))}
                </ul>
              )}
            </div>
            <div>
              <h3 className="text-xs font-semibold tracking-widest text-mist uppercase">
                Following
              </h3>
              {tan.following.length === 0 ? (
                <p className="mt-1 text-xs text-faint">
                  Not following anyone yet.
                </p>
              ) : (
                <ul className="mt-1.5 flex flex-col gap-1.5">
                  {tan.following.map((follow) => (
                    <li
                      key={follow.author}
                      className="flex min-w-0 items-center gap-2"
                    >
                      <span className="min-w-0 flex-1 truncate text-sm">
                        {follow.name}{" "}
                        <span className="font-mono text-xs text-faint">
                          {shortHash(follow.author, 8)}
                        </span>
                      </span>
                      <Button
                        variant="subtle"
                        onClick={() =>
                          void tan
                            .follow(follow.author, false)
                            .catch(() => undefined)
                        }
                      >
                        Unfollow
                      </Button>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>
        </Card>

        {tan.scheduled.length > 0 ? (
          <Card>
            <CardTitle>Scheduled</CardTitle>
            <ul className="flex flex-col gap-2 px-3 py-3">
              {tan.scheduled.map((item) => (
                <li
                  key={item.id}
                  className="flex min-w-0 items-start gap-2 text-sm"
                >
                  <span className="min-w-0 flex-1 break-words">
                    {item.text}
                    <span className="block text-xs text-faint">
                      {formatDateTime(item.publish_at)}
                    </span>
                  </span>
                  <Button
                    variant="subtle"
                    onClick={() =>
                      void tan.cancelScheduled(item.id).catch(() => undefined)
                    }
                  >
                    Cancel
                  </Button>
                </li>
              ))}
            </ul>
          </Card>
        ) : null}
      </aside>
    </div>
  );
}

export default SocialTab;
