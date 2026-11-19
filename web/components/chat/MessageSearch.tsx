// MessageSearch.tsx — search box + results for stored DM/group messages.
// The search itself already existed below the console (`tau messages
// --search`, `GET /api/messages/search`, `searchMessages` in lib/api);
// this is the console UI for it, following the Tan search box in the
// Social tab. A search is an explicit lookup, like the CLI: it finds
// messages from ignored groups and archived conversations too.

"use client";

import { useEffect, useState } from "react";
import { searchMessages } from "@/lib/api";
import { formatRelative } from "@/lib/format";
import { useNow } from "@/lib/use-now";
import type { ChatMessage } from "@/lib/types";
import Avatar from "../chrome/Avatar";

export interface MessageSearchProps {
  /** Open the conversation the result belongs to. */
  onOpen: (message: ChatMessage) => void;
  /** Parent hides the conversation list while results are shown. */
  onActiveChange?: (active: boolean) => void;
  /** Increment to clear the search from the parent (e.g. after opening a result). */
  resetToken?: number;
  now?: number;
}

function conversationName(message: ChatMessage): string {
  if (message.group) return message.group_name || "Group";
  return message.peer_name || "Unknown";
}

export default function MessageSearch({
  onOpen,
  onActiveChange,
  resetToken = 0,
  now: nowProp,
}: MessageSearchProps) {
  const liveNow = useNow();
  const now = nowProp ?? liveNow;
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<ChatMessage[] | null>(null);
  const [searching, setSearching] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (resetToken === 0) return;
    setQuery("");
    setResults(null);
    setError(null);
    onActiveChange?.(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [resetToken]);

  async function runSearch(q: string) {
    const text = q.trim();
    if (!text) {
      setResults(null);
      setError(null);
      onActiveChange?.(false);
      return;
    }
    setSearching(true);
    setError(null);
    try {
      const found = await searchMessages(text);
      setResults(found);
      onActiveChange?.(true);
    } catch (err) {
      setResults(null);
      setError(`Search failed: ${err instanceof Error ? err.message : String(err)}`);
      onActiveChange?.(true);
    } finally {
      setSearching(false);
    }
  }

  function clear() {
    setQuery("");
    setResults(null);
    setError(null);
    onActiveChange?.(false);
  }

  return (
    <>
      <form
        className="shrink-0 border-b border-zinc-800 px-3 py-2"
        onSubmit={(e) => {
          e.preventDefault();
          void runSearch(query);
        }}
      >
        <div className="flex gap-2">
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search messages…"
            aria-label="Search messages"
            className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
          />
          <button
            type="submit"
            disabled={searching}
            className="shrink-0 rounded-full bg-zinc-800 px-3 py-1.5 text-xs font-semibold text-zinc-200 hover:bg-zinc-700 disabled:opacity-50"
          >
            {searching ? "…" : "Search"}
          </button>
          {results !== null && (
            <button
              type="button"
              onClick={clear}
              className="shrink-0 rounded-full border border-zinc-700 px-3 py-1.5 text-xs text-zinc-300"
            >
              Clear
            </button>
          )}
        </div>
        {results !== null && !error && (
          <p className="mt-2 text-xs text-zinc-400">
            {results.length} result{results.length === 1 ? "" : "s"} for “{query.trim()}”
          </p>
        )}
        {error && (
          <p role="alert" className="mt-2 text-xs text-red-300">
            {error}
          </p>
        )}
      </form>

      {results !== null && !error && (
        <div className="min-h-0 flex-1 overflow-y-auto" aria-label="Message search results">
          {results.length === 0 ? (
            <p className="px-3 py-4 text-sm text-zinc-500">No messages match that search.</p>
          ) : (
            <ul className="flex flex-col">
              {results.map((message) => {
                const name = conversationName(message);
                const when = formatRelative(
                  message.received_at || message.sent_at,
                  now,
                );
                return (
                  <li key={message.id} className="border-b border-zinc-800/60">
                    <button
                      type="button"
                      onClick={() => onOpen(message)}
                      className="flex w-full items-start gap-2 px-3 py-2 text-left hover:bg-zinc-900"
                      aria-label={`Open conversation with ${name}`}
                    >
                      <Avatar name={name} seed={message.group ?? message.peer ?? name} size="sm" />
                      <span className="min-w-0 flex-1">
                        <span className="flex items-baseline justify-between gap-2">
                          <span className="truncate text-sm font-semibold text-zinc-100">
                            {name}
                          </span>
                          {when && (
                            <span className="shrink-0 text-xs text-zinc-500">{when}</span>
                          )}
                        </span>
                        <span className="block truncate text-xs text-zinc-400">
                          {message.direction === "out" ? "You: " : ""}
                          {message.text}
                        </span>
                      </span>
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      )}
    </>
  );
}
