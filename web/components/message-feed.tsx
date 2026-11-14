"use client";

import { useEffect, useMemo, useRef, useState } from "react";

import { API_BASE } from "@/lib/api";
import { MAX_MESSAGE_CHARS } from "@/lib/constants";
import type { NodeInfo, Peer, TauMessage } from "@/lib/types";
import {
  formatExpiresIn,
  formatFileSize,
  formatRtt,
  formatSendsIn,
  formatTime,
  shortHash,
} from "@/lib/format";

import { CopyButton, RadarIcon, SectionLabel } from "./ui";

// One emoji or short text token per the protocol limit (32 chars);
// a fixed picker keeps every reaction inside it by construction.
export const REACT_OPTIONS = ["👍", "❤️", "😂", "😮", "😢", "🙏"];

export function MessageFeed({
  messages,
  node,
  peers,
  live,
  onReply,
  onDelete,
  onEdit,
  onRetract,
  onForward,
  onReact,
  onPin,
  searching,
  searchQuery,
  searchError,
  onSearch,
  onClearSearch,
  quoteContext,
  showingPinned,
  pinnedError,
  onShowPinned,
  onClearPinned,
}: {
  messages: TauMessage[];
  node: NodeInfo | null;
  // Known peers to offer as forward targets; the picker reports none
  // when the list is empty.
  peers?: Peer[];
  live: boolean;
  onReply?: (message: TauMessage) => void;
  onDelete?: (message: TauMessage) => Promise<void>;
  // Resolves true when the edit landed on both sides; false keeps the
  // editor open (the peer had no copy, or a newer edit landed).
  onEdit?: (message: TauMessage, text: string) => Promise<boolean>;
  onRetract?: (message: TauMessage) => Promise<void>;
  onForward?: (message: TauMessage, to: string) => Promise<void>;
  // `reaction` null retracts the caller's reaction to the message.
  onReact?: (message: TauMessage, reaction: string | null) => Promise<void>;
  // `pinned` true pins the message, false unpins it.
  onPin?: (message: TauMessage, pinned: boolean) => Promise<void>;
  searching?: boolean;
  searchQuery?: string | null;
  searchError?: string | null;
  onSearch?: (query: string) => Promise<unknown> | unknown;
  onClearSearch?: () => void;
  // History used to resolve reply quotes; defaults to `messages`. Search
  // passes the live traffic here so originals outside the hits still resolve.
  quoteContext?: TauMessage[];
  showingPinned?: boolean;
  pinnedError?: string | null;
  onShowPinned?: () => Promise<unknown> | unknown;
  onClearPinned?: () => void;
}) {
  const endRef = useRef<HTMLDivElement>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const stickToBottom = useRef(true);
  const [searchText, setSearchText] = useState("");

  const counts = useMemo(() => {
    let inbound = 0;
    let outbound = 0;
    for (const message of messages) {
      if (message.direction === "in") inbound += 1;
      else outbound += 1;
    }
    return { inbound, outbound };
  }, [messages]);

  useEffect(() => {
    // Search results and the pinned view are newest first: sticking to
    // the bottom would land on the oldest entry, so only the live feed
    // auto-scrolls.
    if (searching || showingPinned || !stickToBottom.current) return;
    endRef.current?.scrollIntoView({ block: "end", behavior: "smooth" });
  }, [messages.length, searching, showingPinned]);

  useEffect(() => {
    // Show newest-first search hits and pins from the top, not wherever
    // a scrolled live feed left the container.
    if ((!searching && !showingPinned) || !scrollRef.current) return;
    scrollRef.current.scrollTop = 0;
  }, [searching, showingPinned, messages]);

  function onScroll() {
    const element = scrollRef.current;
    if (!element) return;
    const distance =
      element.scrollHeight - element.scrollTop - element.clientHeight;
    stickToBottom.current = distance < 140;
  }

  async function submitSearch() {
    if (!searchText.trim() || !onSearch) return;
    await onSearch(searchText.trim());
  }

  function clearSearch() {
    setSearchText("");
    onClearSearch?.();
  }

  return (
    <section className="flex min-h-0 flex-1 flex-col rounded-2xl border border-line/80 bg-panel/40">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-line/60 px-4 py-3">
        <div className="flex items-center gap-3">
          <SectionLabel>
            {showingPinned ? "pinned" : searching ? "search results" : "traffic"}
          </SectionLabel>
          <span className="font-mono text-[10px] text-mist">
            {showingPinned
              ? `${messages.length} pinned`
              : searching
                ? `${messages.length} match(es)`
                : `${counts.inbound} in / ${counts.outbound} out`}
          </span>
        </div>
        <div className="flex items-center gap-2">
          {showingPinned && onClearPinned ? (
            <button
              type="button"
              onClick={onClearPinned}
              className="rounded-lg border border-line/70 bg-void/50 px-2 py-1 font-mono text-[11px] text-mist transition hover:text-zinc-100"
            >
              show all
            </button>
          ) : null}
          {!showingPinned && !searching && onShowPinned ? (
            <button
              type="button"
              onClick={() => void onShowPinned()}
              className="rounded-lg border border-line/70 bg-void/50 px-2 py-1 font-mono text-[11px] text-mist transition hover:text-zinc-100"
            >
              📌 pinned
            </button>
          ) : null}
          {searching && onClearSearch ? (
            <button
              type="button"
              onClick={clearSearch}
              className="rounded-lg border border-line/70 bg-void/50 px-2 py-1 font-mono text-[11px] text-mist transition hover:text-zinc-100"
            >
              clear search
            </button>
          ) : null}
          <span className="inline-flex items-center gap-1.5 font-mono text-[10px] text-mist">
            <span
              className={`h-1.5 w-1.5 rounded-full ${
                live ? "bg-signal" : "bg-amber"
              }`}
            />
            {live ? "streaming" : "waiting for bridge"}
          </span>
        </div>
      </div>

      {onSearch ? (
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
            placeholder="Search stored messages…"
            spellCheck={false}
            aria-label="Search stored messages"
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
      ) : null}

      {searchError ? (
        <p className="border-b border-line/60 px-4 py-2 font-mono text-[11px] text-rose-400">
          {searchError}
        </p>
      ) : null}

      {pinnedError ? (
        <p className="border-b border-line/60 px-4 py-2 font-mono text-[11px] text-rose-400">
          {pinnedError}
        </p>
      ) : null}

      <div
        ref={scrollRef}
        onScroll={onScroll}
        aria-live="polite"
        aria-label="Message traffic"
        className="thin-scroll min-h-[320px] flex-1 space-y-4 overflow-y-auto px-4 py-5 lg:max-h-[calc(100dvh-330px)]"
      >
        {messages.length === 0 ? (
          searching ? (
            <SearchEmptyState query={searchQuery ?? ""} />
          ) : showingPinned ? (
            <PinnedEmptyState />
          ) : (
            <EmptyState node={node} />
          )
        ) : (
          messages.map((message) => (
            <MessageBubble
              key={message.id}
              message={message}
              messages={quoteContext ?? messages}
              node={node}
              peers={peers}
              onReply={onReply}
              onDelete={onDelete}
              onEdit={onEdit}
              onRetract={onRetract}
              onForward={onForward}
              onReact={onReact}
              onPin={onPin}
            />
          ))
        )}
        <div ref={endRef} />
      </div>
    </section>
  );
}

function SearchEmptyState({ query }: { query: string }) {
  return (
    <div className="flex h-full min-h-[280px] flex-col items-center justify-center px-4 text-center">
      <div className="grid h-20 w-20 place-items-center rounded-full border border-signal/20 bg-signal/[0.04] font-mono text-3xl text-signal/80">
        τ
      </div>
      <h3 className="mt-4 text-sm font-medium text-zinc-200">
        No stored messages match “{query}”
      </h3>
      <p className="mt-1.5 max-w-md text-xs leading-relaxed text-mist">
        Search covers the text of stored messages, sent and received. Try a
        different word, or clear the search to return to the live traffic.
      </p>
    </div>
  );
}

function PinnedEmptyState() {
  return (
    <div className="flex h-full min-h-[280px] flex-col items-center justify-center px-4 text-center">
      <div className="grid h-20 w-20 place-items-center rounded-full border border-signal/20 bg-signal/[0.04] font-mono text-3xl text-signal/80">
        📌
      </div>
      <h3 className="mt-4 text-sm font-medium text-zinc-200">
        No pinned messages
      </h3>
      <p className="mt-1.5 max-w-md text-xs leading-relaxed text-mist">
        Pin a message from the traffic view to keep it here. Pins are
        local-only — the peer never sees them — and they show in{" "}
        <code className="font-mono text-zinc-300">tau pinned</code> too.
      </p>
    </div>
  );
}

function EmptyState({ node }: { node: NodeInfo | null }) {
  return (
    <div className="flex h-full min-h-[280px] flex-col items-center justify-center px-4 text-center">
      <div className="relative grid h-24 w-24 place-items-center">
        <span className="absolute inset-0 rounded-full border border-signal/15" />
        <span className="absolute inset-4 rounded-full border border-signal/25" />
        <RadarIcon className="h-12 w-12 text-signal/80" />
      </div>
      <h3 className="mt-4 text-sm font-medium text-zinc-200">
        Nothing on the wire yet
      </h3>
      <p className="mt-1.5 max-w-sm text-xs leading-relaxed text-mist">
        Messages published to this node&apos;s tau destination land here in real
        time. From another terminal, run{" "}
        <code className="rounded bg-void/80 px-1 py-0.5 font-mono text-[11px] text-zinc-300">
          tau send --to {node?.name ?? "alice"} &quot;hello&quot;
        </code>
        .
      </p>
      {node ? (
        <div className="mt-4 flex flex-col items-center gap-2">
          <p className="max-w-md break-all font-mono text-[10px] text-mist/80">
            {node.destination}
          </p>
          <CopyButton value={node.destination} label="copy destination" />
        </div>
      ) : null}
    </div>
  );
}

export type ResolvedQuote =
  | { name: string; text: string }
  | "unavailable";

export function resolveQuote(
  messages: TauMessage[],
  message: TauMessage,
  ownHash?: string | null,
): ResolvedQuote | null {
  // A quote resolves only inside the same conversation (same peer):
  // the quoted text is pulled from this console's own history, never
  // from the wire, so a reference to a message that never arrived —
  // or to another peer's message — renders as unavailable instead of
  // trusting the sender's claim about what was said. When the reply
  // carries an excerpt hash, the rendered text must match it: if
  // several history entries share the wire id, the hash-matching one
  // wins, and a hash present on every candidate but matching none
  // means the local copy changed, so the original is unavailable.
  // Entries written before excerpt hashes existed carry no hash and
  // cannot be checked; they still render.
  //
  // A group reply that carries the logical reference (group, author,
  // shared sent_at) resolves against any leg of the quoted send: the
  // wire id differs per recipient, so it only ever matches the
  // sender's own copy. The author is the peer for an inbound entry
  // and this node for an outbound one; when the triple matches
  // nothing, the wire-id match below still runs, which covers
  // replies from senders that predate the reference.
  const ref = message.in_reply_to;
  if (!ref || !ref.id) return null;
  if (ref.group && ref.author && typeof ref.sent_at === "number") {
    const logical = messages.filter(
      (entry) =>
        entry.group === ref.group &&
        entry.sent_at === ref.sent_at &&
        (entry.direction === "in"
          ? entry.peer === ref.author
          : ref.author === ownHash),
    );
    if (logical.length > 0) {
      return pickQuoted(logical, ref.excerpt);
    }
  }
  const candidates = messages.filter(
    (entry) => entry.peer === message.peer && entry.wire_id === ref.id,
  );
  if (candidates.length === 0) return "unavailable";
  return pickQuoted(candidates, ref.excerpt);
}

function pickQuoted(
  candidates: TauMessage[],
  excerpt: string | null | undefined,
): ResolvedQuote {
  let quoted = candidates[0];
  if (excerpt) {
    const verified = candidates.find(
      (entry) =>
        entry.excerpt_hash === excerpt ||
        entry.excerpt_hashes?.includes(excerpt),
    );
    if (verified) {
      quoted = verified;
    } else if (candidates.some((entry) => entry.excerpt_hash !== undefined)) {
      return "unavailable";
    }
  }
  return {
    name: quoted.direction === "out" ? "you" : quoted.peer_name,
    text: quoted.text.split("\n")[0].slice(0, 80),
  };
}

function MessageBubble({
  message,
  messages,
  node,
  peers,
  onReply,
  onDelete,
  onEdit,
  onRetract,
  onForward,
  onReact,
  onPin,
}: {
  message: TauMessage;
  messages: TauMessage[];
  node: NodeInfo | null;
  peers?: Peer[];
  onReply?: (message: TauMessage) => void;
  onDelete?: (message: TauMessage) => Promise<void>;
  onEdit?: (message: TauMessage, text: string) => Promise<boolean>;
  onRetract?: (message: TauMessage) => Promise<void>;
  onForward?: (message: TauMessage, to: string) => Promise<void>;
  onReact?: (message: TauMessage, reaction: string | null) => Promise<void>;
  onPin?: (message: TauMessage, pinned: boolean) => Promise<void>;
}) {
  const outbound = message.direction === "out";
  const peerHash = message.peer ?? "";
  const initial = (message.peer_name || "?").slice(0, 1).toUpperCase();
  const expiry = formatExpiresIn(message.expires_at);
  // Re-render scheduled bubbles so the countdown stays current, and falls
  // back to "queued" once due, even while the live feed is not polling.
  const [, setClockTick] = useState(0);
  useEffect(() => {
    if (!message.send_after) return;
    const timer = setInterval(() => setClockTick((tick) => tick + 1), 30000);
    return () => clearInterval(timer);
  }, [message.send_after]);
  const scheduled = formatSendsIn(message.send_after);
  const quote = resolveQuote(messages, message, node?.destination ?? null);
  const [confirmingDelete, setConfirmingDelete] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);

  async function handleConfirmDelete() {
    if (!onDelete || deleting) return;
    setDeleting(true);
    setDeleteError(null);
    try {
      await onDelete(message);
    } catch (error) {
      // A failed delete keeps the bubble, with the reason inline and
      // the plain delete button back so the user can retry.
      setDeleteError(
        error instanceof Error ? error.message : "delete failed",
      );
      setConfirmingDelete(false);
    } finally {
      setDeleting(false);
    }
  }

  const [editing, setEditing] = useState(false);
  const [editText, setEditText] = useState("");
  const [savingEdit, setSavingEdit] = useState(false);
  const [editError, setEditError] = useState<string | null>(null);

  async function submitEditForm() {
    if (savingEdit || !onEdit) return;
    setSavingEdit(true);
    setEditError(null);
    try {
      const applied = await onEdit(message, editText.trim());
      if (applied) {
        setEditing(false);
      } else {
        // The peer had no copy (or a newer edit landed): the local
        // text stays as it was, so say so and keep the editor open
        // to retry.
        setEditError("the peer no longer has this message");
      }
    } catch (error) {
      setEditError(error instanceof Error ? error.message : "edit failed");
    } finally {
      setSavingEdit(false);
    }
  }

  const [confirmingRetract, setConfirmingRetract] = useState(false);
  const [retracting, setRetracting] = useState(false);
  const [retractError, setRetractError] = useState<string | null>(null);

  async function handleConfirmRetract() {
    if (!onRetract || retracting) return;
    setRetracting(true);
    setRetractError(null);
    try {
      await onRetract(message);
    } catch (error) {
      // A failed retract keeps the bubble, with the reason inline and
      // the plain retract button back so the user can retry.
      setRetractError(
        error instanceof Error ? error.message : "retract failed",
      );
      setConfirmingRetract(false);
    } finally {
      setRetracting(false);
    }
  }
  const [pickerOpen, setPickerOpen] = useState(false);
  const [reactError, setReactError] = useState<string | null>(null);
  const [reactPending, setReactPending] = useState(false);
  const [pinPending, setPinPending] = useState(false);
  const [pinError, setPinError] = useState<string | null>(null);
  // Synchronous guard so rapid clicks cannot start overlapping requests
  // before the pending state has re-rendered.
  const reactInFlight = useRef(false);
  const pinInFlight = useRef(false);

  async function handlePin() {
    if (!onPin || pinInFlight.current) return;
    pinInFlight.current = true;
    setPinPending(true);
    setPinError(null);
    try {
      // The bridge re-sends the message as `message_updated`, so the
      // bubble (and the pinned view) update through the SSE stream.
      await onPin(message, !message.pinned);
    } catch (error) {
      setPinError(error instanceof Error ? error.message : "pin failed");
    } finally {
      setPinPending(false);
      pinInFlight.current = false;
    }
  }

  const [forwardOpen, setForwardOpen] = useState(false);
  const [forwardPending, setForwardPending] = useState(false);
  const [forwardError, setForwardError] = useState<string | null>(null);
  const forwardInFlight = useRef(false);

  async function handleForward(to: string) {
    if (!onForward || forwardInFlight.current) return;
    forwardInFlight.current = true;
    setForwardPending(true);
    setForwardError(null);
    try {
      // The bridge records the forwarded copy as a new outbound entry,
      // so it appears in the feed through the SSE stream.
      await onForward(message, to);
      setForwardOpen(false);
    } catch (error) {
      setForwardError(
        error instanceof Error ? error.message : "forward failed",
      );
    } finally {
      setForwardPending(false);
      forwardInFlight.current = false;
    }
  }

  // The node's own destination hash is what the bridge records as the
  // reactor, so this is how the console knows which reaction is mine.
  const ownReaction =
    node === null
      ? undefined
      : message.reactions?.find(
          (reaction) => reaction.reactor === node.destination,
        );

  async function handleReact(reaction: string) {
    if (!onReact || reactInFlight.current) return;
    reactInFlight.current = true;
    setReactPending(true);
    setReactError(null);
    try {
      // Tapping the emoji already in use retracts it.
      await onReact(
        message,
        ownReaction?.reaction === reaction ? null : reaction,
      );
      setPickerOpen(false);
    } catch (error) {
      setReactError(
        error instanceof Error ? error.message : "reaction failed",
      );
    } finally {
      reactInFlight.current = false;
      setReactPending(false);
    }
  }

  return (
    <article
      className={`flex items-start gap-2.5 ${
        outbound ? "flex-row-reverse" : ""
      }`}
    >
      <span
        className={`mt-0.5 grid h-8 w-8 shrink-0 place-items-center rounded-lg border font-mono text-xs ${
          outbound
            ? "border-signal/35 bg-signal/10 text-signal"
            : "border-line bg-void/60 text-zinc-300"
        }`}
        aria-hidden="true"
      >
        {outbound ? "→" : initial}
      </span>

      <div
        className={`flex min-w-0 max-w-[86%] flex-col ${
          outbound ? "items-end" : "items-start"
        }`}
      >
        <div className="flex flex-wrap items-center gap-x-2 gap-y-0.5 px-1 pb-1">
          <span className="text-[11px] font-medium text-zinc-300">
            {outbound ? "you" : message.peer_name}
          </span>
          {message.group ? (
            <span className="rounded border border-signal/30 bg-signal/10 px-1.5 py-px font-mono text-[10px] text-signal">
              {message.group_name ?? "group"}
            </span>
          ) : null}
          <span className="font-mono text-[10px] text-mist/80">
            {peerHash ? shortHash(peerHash, 8) : "anonymous"}
          </span>
          <span className="font-mono text-[10px] text-mist/60">
            {formatTime(message.received_at)}
          </span>
          {message.pinned ? (
            <span
              className="font-mono text-[10px] text-signal"
              title="Pinned — kept easy to find, local only"
            >
              <span aria-hidden="true">📌</span> pinned
            </span>
          ) : null}
        </div>

        <div
          className={`whitespace-pre-wrap break-words rounded-2xl border px-3.5 py-2.5 text-[13px] leading-relaxed ${
            outbound
              ? "rounded-tr-sm border-signal/30 bg-signal/[0.08] text-zinc-100"
              : "rounded-tl-sm border-line/80 bg-void/60 text-zinc-200"
          }`}
        >
          {quote !== null ? (
            <span className="mb-1.5 block border-l-2 border-signal/50 pl-2 font-mono text-[11px] leading-snug text-mist">
              {quote === "unavailable" ? (
                "↩ original not available"
              ) : (
                <>
                  ↩ {quote.name}: {quote.text}
                </>
              )}
            </span>
          ) : null}
          {editing ? (
            <span className="block">
              <textarea
                value={editText}
                disabled={savingEdit}
                // Code-point-aware cap: the backend counts Python code
                // points (len), while slice/maxLength count UTF-16 units,
                // so a plain slice would silently drop emoji from a valid
                // existing message on the first keystroke.
                onChange={(event) =>
                  setEditText(
                    [...event.target.value]
                      .slice(0, MAX_MESSAGE_CHARS)
                      .join(""),
                  )
                }
                rows={3}
                aria-label="Edit message text"
                className="w-full resize-y rounded-lg border border-line/70 bg-void/60 px-2.5 py-2 text-[13px] leading-relaxed text-zinc-200 outline-none focus:border-signal/50"
              />
              <span className="mt-2 flex gap-2 font-mono text-[10px]">
                <button
                  type="button"
                  disabled={
                    savingEdit ||
                    !editText.trim() ||
                    editText.trim() === message.text
                  }
                  onClick={() => void submitEditForm()}
                  title="Save this edit"
                  className="rounded border border-signal/40 bg-signal/10 px-1.5 py-0.5 text-signal transition-colors hover:bg-signal/20 disabled:opacity-50"
                >
                  {savingEdit ? "saving…" : "save"}
                </button>
                <button
                  type="button"
                  disabled={savingEdit}
                  onClick={() => setEditing(false)}
                  title="Discard this edit"
                  className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5 text-mist/80 transition-colors hover:text-zinc-100 disabled:opacity-50"
                >
                  cancel
                </button>
              </span>
              {editError ? (
                <span className="mt-1 block font-mono text-[10px] text-rose-300">
                  {editError}
                </span>
              ) : null}
            </span>
          ) : (
            message.text
          )}
        </div>

        {message.file ? (
          <div className="mt-1.5 flex items-center gap-2 rounded-lg border border-line/70 bg-void/50 px-2.5 py-1.5 font-mono text-[11px] text-zinc-300">
            <span aria-hidden="true">📎</span>
            <span className="min-w-0 flex-1 truncate" title={message.file.name}>
              {message.file.name}
            </span>
            <span className="shrink-0 text-mist">
              {formatFileSize(message.file.size)}
            </span>
            <span
              className="hidden shrink-0 text-mist/70 sm:inline"
              title={`SHA-256 ${message.file.sha256}`}
            >
              sha256 {shortHash(message.file.sha256, 8)}
            </span>
            {message.direction === "in" && message.peer ? (
              <a
                className="shrink-0 text-signal hover:underline"
                href={`${API_BASE}/api/files/${message.peer}/${encodeURIComponent(message.file.name)}`}
                download={message.file.name}
              >
                Download
              </a>
            ) : null}
          </div>
        ) : null}

        {message.reactions && message.reactions.length > 0 ? (
          <div
            className={`flex flex-wrap gap-1.5 px-1 pt-1.5 ${
              outbound ? "justify-end" : ""
            }`}
            aria-label="Reactions"
          >
            {message.reactions.map((reaction) => (
              <span
                key={reaction.reactor}
                title={reaction.name}
                className="inline-flex items-center gap-1 rounded-full border border-line/80 bg-void/60 px-2 py-0.5 font-mono text-[11px] text-zinc-300"
              >
                <span aria-hidden="true">{reaction.reaction}</span>
                <span>{reaction.name}</span>
              </span>
            ))}
          </div>
        ) : null}

        <div
          className={`flex items-center gap-2 px-1 pt-1 font-mono text-[10px] ${
            outbound ? "text-signal/70" : "text-mist/60"
          }`}
        >
          <span>
            {outbound
              ? message.status === "queued"
                ? (scheduled ?? "queued")
                : message.status === "read"
                  ? "read"
                  : message.status === "failed"
                    ? "failed"
                    : "delivered"
              : "received"}
          </span>
          {message.edited_at !== undefined ? <span>· edited</span> : null}
          {message.rtt_ms !== undefined ? (
            <span>· rtt {formatRtt(message.rtt_ms)}</span>
          ) : null}
          {expiry !== null ? <span>· {expiry}</span> : null}
          {onReply && message.wire_id ? (
            <button
              type="button"
              title="Reply to this message"
              onClick={() => onReply(message)}
              className="rounded px-1 py-0.5 text-mist transition hover:bg-void/60 hover:text-zinc-200"
            >
              ↩ reply
            </button>
          ) : null}
          {onDelete && message.status !== "queued" ? (
            deleting ? (
              <span className="px-1 py-0.5 text-mist/60">deleting…</span>
            ) : confirmingDelete ? (
              <>
                <button
                  type="button"
                  title="Delete this message"
                  onClick={() => void handleConfirmDelete()}
                  className="rounded px-1 py-0.5 text-rose-300 transition hover:bg-rose-400/10"
                >
                  confirm delete
                </button>
                <button
                  type="button"
                  title="Keep this message"
                  onClick={() => setConfirmingDelete(false)}
                  className="rounded px-1 py-0.5 text-mist transition hover:bg-void/60 hover:text-zinc-200"
                >
                  keep
                </button>
              </>
            ) : (
              <button
                type="button"
                title="Delete this message"
                onClick={() => setConfirmingDelete(true)}
                className="rounded px-1 py-0.5 text-mist transition hover:bg-void/60 hover:text-rose-300"
              >
                delete
              </button>
            )
          ) : null}
          {deleteError ? (
            <span className="text-rose-300">{deleteError}</span>
          ) : null}
          {onEdit &&
          outbound &&
          !message.file &&
          message.wire_id &&
          message.status !== "queued" &&
          !editing ? (
            <button
              type="button"
              title="Edit this message"
              onClick={() => {
                setEditText(message.text);
                setEditError(null);
                setEditing(true);
              }}
              className="rounded px-1 py-0.5 text-mist transition hover:bg-void/60 hover:text-zinc-200"
            >
              edit
            </button>
          ) : null}
          {onRetract &&
          outbound &&
          !message.file &&
          message.wire_id &&
          message.status !== "queued" ? (
            retracting ? (
              <span className="px-1 py-0.5 text-mist/60">retracting…</span>
            ) : confirmingRetract ? (
              <>
                <button
                  type="button"
                  title="Retract this message"
                  onClick={() => void handleConfirmRetract()}
                  className="rounded px-1 py-0.5 text-rose-300 transition hover:bg-rose-400/10"
                >
                  confirm retract
                </button>
                <button
                  type="button"
                  title="Keep this message"
                  onClick={() => setConfirmingRetract(false)}
                  className="rounded px-1 py-0.5 text-mist transition hover:bg-void/60 hover:text-zinc-200"
                >
                  keep
                </button>
              </>
            ) : (
              <button
                type="button"
                title="Retract this message"
                onClick={() => setConfirmingRetract(true)}
                className="rounded px-1 py-0.5 text-mist transition hover:bg-void/60 hover:text-rose-300"
              >
                retract
              </button>
            )
          ) : null}
          {retractError ? (
            <span className="text-rose-300">{retractError}</span>
          ) : null}
          {onReact && message.wire_id ? (
            <button
              type="button"
              title="React to this message"
              aria-label="React to this message"
              aria-expanded={pickerOpen}
              onClick={() => {
                setReactError(null);
                setPickerOpen((open) => !open);
              }}
              className="rounded px-1 py-0.5 text-mist transition hover:bg-void/60 hover:text-zinc-200"
            >
              ♡ react
            </button>
          ) : null}
          {onPin && message.status !== "queued" && message.wire_id ? (
            <button
              type="button"
              title={
                message.pinned ? "Unpin this message" : "Pin this message"
              }
              aria-label={
                message.pinned ? "Unpin this message" : "Pin this message"
              }
              aria-pressed={message.pinned}
              disabled={pinPending}
              onClick={() => void handlePin()}
              className={`rounded px-1 py-0.5 transition hover:bg-void/60 ${
                message.pinned
                  ? "text-signal hover:text-signal"
                  : "text-mist hover:text-zinc-200"
              }`}
            >
              {pinPending ? "pinning…" : message.pinned ? "📌 pinned" : "pin"}
            </button>
          ) : null}
          {onForward && !message.group && !message.file ? (
            <button
              type="button"
              title="Forward this message"
              aria-label="Forward this message"
              aria-expanded={forwardOpen}
              disabled={forwardPending}
              onClick={() => {
                setForwardError(null);
                setForwardOpen((open) => !open);
              }}
              className="rounded px-1 py-0.5 text-mist transition hover:bg-void/60 hover:text-zinc-200"
            >
              {forwardPending ? "forwarding…" : "↗ forward"}
            </button>
          ) : null}
        </div>

        {pickerOpen ? (
          <div
            className={`flex flex-wrap gap-1 px-1 pt-1 ${
              outbound ? "justify-end" : ""
            }`}
            role="group"
            aria-label="Choose a reaction"
          >
            {REACT_OPTIONS.map((option) => {
              const active = ownReaction?.reaction === option;
              return (
                <button
                  key={option}
                  type="button"
                  title={
                    active
                      ? `Remove your ${option} reaction`
                      : `React with ${option}`
                  }
                  aria-label={
                    active
                      ? `Remove your ${option} reaction`
                      : `React with ${option}`
                  }
                  aria-pressed={active}
                  disabled={reactPending}
                  onClick={() => void handleReact(option)}
                  className={`grid h-7 w-7 place-items-center rounded-lg border text-sm transition ${
                    active
                      ? "border-signal/50 bg-signal/15"
                      : "border-line/80 bg-void/60 hover:border-signal/35 hover:bg-void"
                  }`}
                >
                  <span aria-hidden="true">{option}</span>
                </button>
              );
            })}
          </div>
        ) : null}
        {forwardOpen ? (
          <div
            className={`flex flex-wrap gap-1 px-1 pt-1 ${
              outbound ? "justify-end" : ""
            }`}
            role="group"
            aria-label="Forward to a peer"
          >
            {(peers ?? []).length === 0 ? (
              <span className="font-mono text-[10px] text-mist/60">
                no known peers yet
              </span>
            ) : (
              (peers ?? []).map((peer) => (
                <button
                  key={peer.destination}
                  type="button"
                  title={`Forward to ${peer.alias ?? peer.name}`}
                  disabled={forwardPending}
                  onClick={() => void handleForward(peer.destination)}
                  className="rounded-lg border border-line/80 bg-void/60 px-2 py-1 font-mono text-[11px] text-zinc-200 transition hover:border-signal/35 hover:bg-void"
                >
                  {peer.alias ?? peer.name}
                </button>
              ))
            )}
          </div>
        ) : null}
        {forwardError ? (
          <p
            className="px-1 pt-1 font-mono text-[10px] text-amber"
            role="alert"
          >
            forward failed: {forwardError}
          </p>
        ) : null}
        {reactError ? (
          <p
            className="px-1 pt-1 font-mono text-[10px] text-amber"
            role="alert"
          >
            reaction failed: {reactError}
          </p>
        ) : null}
        {pinError ? (
          <p
            className="px-1 pt-1 font-mono text-[10px] text-amber"
            role="alert"
          >
            pin failed: {pinError}
          </p>
        ) : null}
      </div>
    </article>
  );
}
