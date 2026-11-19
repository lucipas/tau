// ChannelsSection.tsx — public channels: join / leave, per-channel
// message history with a composer, search, and sync. Built on useChannels
// (web/lib/use-channels.ts).

"use client";

import { useEffect, useState } from "react";
import Avatar from "@/components/chrome/Avatar";
import { formatRelative, shortHash } from "@/lib/format";
import { getStatus } from "@/lib/api";
import { useChannels } from "@/lib/use-channels";

export default function ChannelsSection() {
  const channels = useChannels(30_000);
  const [joinName, setJoinName] = useState("");
  const [text, setText] = useState("");
  const [searchText, setSearchText] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [sending, setSending] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [editingTopic, setEditingTopic] = useState(false);
  const [topicText, setTopicText] = useState("");
  const [savingTopic, setSavingTopic] = useState(false);
  const [searching, setSearching] = useState(false);
  const [reacting, setReacting] = useState<string | null>(null);
  // The message being answered, if any: the composer sends with its hash.
  const [replyTo, setReplyTo] = useState<{ hash: string; name: string } | null>(
    null,
  );

  // This node's own destination hash, so only its messages get an
  // Edit button — an edit is author-only on the wire.  Null until
  // /api/status answers, or when it cannot (edit stays hidden).
  const [ownHash, setOwnHash] = useState<string | null>(null);
  const [editingHash, setEditingHash] = useState<string | null>(null);
  const [editText, setEditText] = useState("");
  const [savingEdit, setSavingEdit] = useState(false);

  useEffect(() => {
    let live = true;
    getStatus()
      .then((snapshot) => {
        if (live) setOwnHash(snapshot?.node?.destination ?? null);
      })
      .catch(() => {
        // Status unavailable: no edit buttons, reading still works.
      });
    return () => {
      live = false;
    };
  }, []);

  async function handleSaveEdit(hash: string) {
    const body = editText.trim();
    if (!body) return;
    setSavingEdit(true);
    setError(null);
    try {
      await channels.edit(hash, body);
      setEditingHash(null);
      setEditText("");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSavingEdit(false);
    }
  }

  const selectedChannel = channels.channels.find((c) => c.channel === channels.selected);

  async function handleReact(hash: string, reaction: string) {
    setReacting(hash);
    setError(null);
    try {
      await channels.react(hash, reaction);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setReacting(null);
    }
  }

  async function handleJoin(e: React.FormEvent) {
    e.preventDefault();
    const name = joinName.trim();
    if (!name) return setError("Type a channel name to join.");
    setError(null);
    try {
      await channels.join(name);
      setJoinName("");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  async function handleSend(e: React.FormEvent) {
    e.preventDefault();
    const body = text.trim();
    if (!body || !channels.selected) return;
    setSending(true);
    setError(null);
    try {
      await channels.send(body, replyTo?.hash);
      setText("");
      setReplyTo(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSending(false);
    }
  }

  async function handleSync() {
    setSyncing(true);
    setError(null);
    try {
      await channels.sync();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSyncing(false);
    }
  }

  async function handleSaveTopic(e: React.FormEvent) {
    e.preventDefault();
    const body = topicText.trim();
    if (!body || !channels.selected) return;
    setSavingTopic(true);
    setError(null);
    try {
      await channels.setTopic(body);
      setEditingTopic(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSavingTopic(false);
    }
  }

  async function handleClearTopic() {
    if (!channels.selected) return;
    setSavingTopic(true);
    setError(null);
    try {
      await channels.clearTopic();
      setEditingTopic(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSavingTopic(false);
    }
  }

  async function handlePin(hash: string) {
    setError(null);
    try {
      await channels.pin(hash);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  async function handleUnpin(hash: string) {
    setError(null);
    try {
      await channels.unpin(hash);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  async function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    const query = searchText.trim();
    if (!query || !channels.selected) return;
    setSearching(true);
    setError(null);
    try {
      await channels.search(query);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSearching(false);
    }
  }

  function handleClearSearch() {
    setSearchText("");
    channels.clearSearch();
  }

  // History arrives newest-first; show it oldest-first like a chat log.
  // Search matches stay newest-first: the most recent match first.
  const showingSearch = channels.searchResults !== null;
  const messages = showingSearch
    ? channels.searchResults!
    : [...channels.messages].reverse();
  // Pins arrive newest pin first and stay in that order up top.
  const pinnedHashes = new Set(channels.pins.map((p) => p.hash));
  // Replies quote their parent from the loaded page; a parent that
  // has not arrived yet shows its short digest instead.
  const byHash = new Map(channels.messages.map((m) => [m.hash, m]));

  return (
    <div className="mx-auto w-full max-w-[600px]">
      <header className="sticky top-0 z-10 border-b border-zinc-800 bg-zinc-950/95 px-3 py-3 backdrop-blur sm:px-4">
        <h1 className="text-lg font-bold text-zinc-50">
          <span className="mr-2 text-cyan-400">τ</span>Channels
        </h1>
        <p className="mt-0.5 text-xs text-zinc-500">
          Public channels — messages here go to the whole mesh, not to one peer.
        </p>
      </header>

      <div className="border-b border-zinc-800 px-3 py-3 sm:px-4">
        <form onSubmit={handleJoin} className="flex gap-2">
          <input
            value={joinName}
            onChange={(e) => setJoinName(e.target.value)}
            placeholder="Join a channel, e.g. mesh-general"
            aria-label="Channel name to join"
            className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
          />
          <button
            type="submit"
            className="shrink-0 rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-bold text-zinc-950 hover:bg-cyan-400"
          >
            Join
          </button>
        </form>
        {channels.loading && channels.channels.length === 0 && (
          <p className="py-3 text-sm text-zinc-500">Loading channels…</p>
        )}
        {channels.error && (
          <p role="alert" className="py-2 text-sm text-red-400">
            {channels.error}
          </p>
        )}
        <div className="mt-3 flex flex-wrap gap-2" aria-label="Joined channels">
          {channels.channels.length === 0 && !channels.loading && (
            <p className="text-sm text-zinc-500">
              You haven&apos;t joined any channels yet.
            </p>
          )}
          {channels.channels.map((c) => (
            <button
              key={c.channel}
              type="button"
              aria-pressed={channels.selected === c.channel}
              onClick={() => channels.select(c.channel)}
              className={`rounded-full border px-3 py-1.5 text-sm font-semibold ${
                channels.selected === c.channel
                  ? "border-cyan-500 bg-cyan-950 text-cyan-200"
                  : "border-zinc-700 text-zinc-300 hover:bg-zinc-900"
              }`}
            >
              #{c.channel}
            </button>
          ))}
        </div>
      </div>

      {error && (
        <p role="alert" className="border-b border-zinc-800 px-4 py-2 text-sm text-red-400">
          {error}
        </p>
      )}

      {!channels.selected && (
        <p className="px-4 py-8 text-center text-sm text-zinc-500">
          Pick a channel above to read its messages.
        </p>
      )}

      {channels.selected && (
        <div>
          <div className="flex items-center justify-between gap-2 border-b border-zinc-800 px-3 py-2 sm:px-4">
            <h2 className="min-w-0 truncate text-sm font-bold text-zinc-100">
              #{channels.selected}
              {selectedChannel && (
                <span className="ml-2 font-normal text-xs text-zinc-500">
                  joined {formatRelative(selectedChannel.joined_at)}
                </span>
              )}
            </h2>
            <div className="flex shrink-0 gap-2">
              <button
                type="button"
                onClick={() => void handleSync()}
                disabled={syncing}
                className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-semibold text-zinc-300 hover:bg-zinc-900 disabled:opacity-50"
              >
                {syncing ? "Syncing…" : "↻ Sync"}
              </button>
              <button
                type="button"
                onClick={() => void channels.leave(channels.selected!)}
                className="rounded-full border border-red-800 px-3 py-1 text-xs text-red-400 hover:bg-red-950"
              >
                Leave
              </button>
            </div>
          </div>

          <div className="border-b border-zinc-800 px-3 py-2 sm:px-4">
            {editingTopic ? (
              <form onSubmit={handleSaveTopic} className="flex gap-2">
                <input
                  value={topicText}
                  onChange={(e) => setTopicText(e.target.value)}
                  placeholder="What is this channel about? (max 140 characters)"
                  aria-label="Channel topic"
                  maxLength={140}
                  disabled={savingTopic}
                  className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
                />
                <button
                  type="submit"
                  disabled={savingTopic || !topicText.trim()}
                  className="shrink-0 rounded-full bg-cyan-500 px-3 py-1.5 text-xs font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
                >
                  {savingTopic ? "…" : "Save"}
                </button>
                <button
                  type="button"
                  onClick={() => setEditingTopic(false)}
                  disabled={savingTopic}
                  className="shrink-0 rounded-full border border-zinc-700 px-3 py-1.5 text-xs font-semibold text-zinc-300 hover:bg-zinc-900 disabled:opacity-50"
                >
                  Cancel
                </button>
              </form>
            ) : (
              <div className="flex items-center justify-between gap-2">
                <p className="min-w-0 truncate text-sm text-zinc-400">
                  {channels.topic ? (
                    <span title={channels.topic.topic}>{channels.topic.topic}</span>
                  ) : (
                    <span className="italic text-zinc-500">No topic set.</span>
                  )}
                </p>
                <div className="flex shrink-0 gap-2">
                  <button
                    type="button"
                    onClick={() => {
                      setTopicText(channels.topic?.topic ?? "");
                      setEditingTopic(true);
                    }}
                    className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-semibold text-zinc-300 hover:bg-zinc-900"
                  >
                    {channels.topic ? "Edit topic" : "Set topic"}
                  </button>
                  {channels.topic && (
                    <button
                      type="button"
                      onClick={() => void handleClearTopic()}
                      disabled={savingTopic}
                      className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-semibold text-zinc-300 hover:bg-zinc-900 disabled:opacity-50"
                    >
                      Clear topic
                    </button>
                  )}
                </div>
              </div>
            )}
          </div>

          <form
            onSubmit={handleSearch}
            className="flex gap-2 border-b border-zinc-800 px-3 py-2 sm:px-4"
          >
            <input
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              placeholder={`Search #${channels.selected}`}
              aria-label={`Search #${channels.selected}`}
              disabled={searching}
              className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
            />
            <button
              type="submit"
              disabled={searching || !searchText.trim()}
              className="shrink-0 rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
            >
              {searching ? "…" : "Search"}
            </button>
            {channels.searchResults !== null && (
              <button
                type="button"
                onClick={handleClearSearch}
                className="shrink-0 rounded-full border border-zinc-700 px-3 py-1.5 text-sm text-zinc-300 hover:bg-zinc-900"
              >
                Clear
              </button>
            )}
          </form>
          {channels.searchResults !== null && (
            <p className="border-b border-zinc-800 px-4 py-2 text-xs text-zinc-500">
              {channels.searchResults.length} match
              {channels.searchResults.length === 1 ? "" : "es"} for &ldquo;
              {channels.searchQuery}&rdquo; — newest first.
            </p>
          )}

          <div className="px-3 py-2 sm:px-4">
            {channels.pins.length > 0 && (
              <div className="mb-2 rounded-lg border border-amber-800/60 bg-amber-950/40 px-3 py-2">
                <p className="text-xs font-bold uppercase tracking-wide text-amber-400">
                  Pinned
                </p>
                {channels.pins.map((m) => (
                  <div key={m.hash} className="flex gap-3 border-b border-amber-800/30 py-2 last:border-b-0">
                    <Avatar name={m.name} seed={m.author} size="sm" />
                    <div className="min-w-0 flex-1">
                      <p className="text-sm">
                        <span className="font-bold text-zinc-100">
                          {m.name || shortHash(m.author)}
                        </span>{" "}
                        <span className="text-xs text-zinc-500">
                          · {formatRelative(m.created_at)}
                        </span>
                      </p>
                      <p className="mt-0.5 text-sm leading-relaxed whitespace-pre-wrap text-zinc-200">
                        {m.text}
                      </p>
                    </div>
                    <button
                      type="button"
                      onClick={() => void handleUnpin(m.hash)}
                      aria-label={`Unpin pinned message from ${m.name || shortHash(m.author)}`}
                      className="h-fit shrink-0 rounded-full border border-zinc-700 px-2 py-0.5 text-xs font-semibold text-zinc-300 hover:bg-zinc-900"
                    >
                      Unpin
                    </button>
                  </div>
                ))}
              </div>
            )}
            {messages.length === 0 && !showingSearch && (
              <p className="py-6 text-center text-sm text-zinc-500">
                No messages in this channel yet — say hello.
              </p>
            )}
            {messages.length === 0 && showingSearch && (
              <p className="py-6 text-center text-sm text-zinc-500">
                No messages in this channel match &ldquo;{channels.searchQuery}&rdquo;.
              </p>
            )}
            {messages.map((m) => {
              const parent = m.reply_to ? byHash.get(m.reply_to) : undefined;
              return (
              <div key={m.hash} className="flex gap-3 border-b border-zinc-800/60 py-3">
                <Avatar name={m.name} seed={m.author} size="sm" />
                <div className="min-w-0 flex-1">
                  <p className="text-sm">
                    <span className="font-bold text-zinc-100">
                      {m.name || shortHash(m.author)}
                    </span>{" "}
                    <span className="text-xs text-zinc-500">
                      · {formatRelative(m.created_at)}
                      {m.edited ? " · edited" : ""}
                    </span>
                    {ownHash && m.author === ownHash && editingHash !== m.hash && (
                      <button
                        type="button"
                        onClick={() => {
                          setEditingHash(m.hash);
                          setEditText(m.text);
                        }}
                        className="ml-2 text-xs font-semibold text-cyan-400 hover:text-cyan-300"
                      >
                        Edit
                      </button>
                    )}
                  </p>
                  {m.reply_to && (
                    <p className="mt-0.5 truncate border-l-2 border-zinc-700 pl-2 text-xs text-zinc-500">
                      ↩{" "}
                      {parent
                        ? `${parent.name || shortHash(parent.author)}: ${parent.text.split("\n")[0]}`
                        : `replying to ${shortHash(m.reply_to)}`}
                    </p>
                  )}
                  {editingHash === m.hash ? (
                    <div className="mt-1">
                      <textarea
                        value={editText}
                        onChange={(e) => setEditText(e.target.value)}
                        aria-label="Edit message"
                        rows={3}
                        className="w-full rounded-lg border border-zinc-700 bg-zinc-900 px-2 py-1.5 text-sm text-zinc-100 outline-none focus:border-cyan-500"
                      />
                      <div className="mt-1 flex gap-2">
                        <button
                          type="button"
                          onClick={() => void handleSaveEdit(m.hash)}
                          disabled={savingEdit || !editText.trim() || editText === m.text}
                          className="rounded-full bg-cyan-500 px-3 py-1 text-xs font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
                        >
                          {savingEdit ? "Saving…" : "Save"}
                        </button>
                        <button
                          type="button"
                          onClick={() => {
                            setEditingHash(null);
                            setEditText("");
                          }}
                          className="rounded-full border border-zinc-700 px-3 py-1 text-xs text-zinc-300 hover:bg-zinc-900"
                        >
                          Cancel
                        </button>
                      </div>
                    </div>
                  ) : (
                    <p className="mt-0.5 text-sm leading-relaxed whitespace-pre-wrap text-zinc-200">
                      {m.text}
                    </p>
                  )}
                  <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                    {(m.reactions ?? []).map((r) => (
                      <button
                        key={r.reaction}
                        type="button"
                        title={r.reactors.join(", ")}
                        aria-label={`React ${r.reaction} (${r.count})`}
                        onClick={() => void handleReact(m.hash, r.reaction)}
                        disabled={reacting === m.hash}
                        className="rounded-full border border-zinc-700 bg-zinc-900 px-2 py-0.5 text-xs text-zinc-200 hover:border-zinc-500 disabled:opacity-50"
                      >
                        {r.reaction} {r.count}
                      </button>
                    ))}
                    {["👍", "❤️", "🎉", "🚀", "👀"]
                      .filter((token) => !(m.reactions ?? []).some((r) => r.reaction === token))
                      .map((token) => (
                        <button
                          key={token}
                          type="button"
                          aria-label={`React ${token} to this message`}
                          onClick={() => void handleReact(m.hash, token)}
                          disabled={reacting === m.hash}
                          className="rounded-full border border-transparent px-1.5 py-0.5 text-xs text-zinc-500 hover:border-zinc-700 hover:text-zinc-200 disabled:opacity-50"
                        >
                          {token}
                        </button>
                      ))}
                  </div>
                  <button
                    type="button"
                    aria-label={`Reply to ${m.name || shortHash(m.author)}`}
                    onClick={() =>
                      setReplyTo({ hash: m.hash, name: m.name || shortHash(m.author) })
                    }
                    className="mt-1 text-xs font-semibold text-zinc-500 hover:text-cyan-400"
                  >
                    Reply
                  </button>
                </div>
                <button
                  type="button"
                  onClick={() =>
                    void (pinnedHashes.has(m.hash)
                      ? handleUnpin(m.hash)
                      : handlePin(m.hash))
                  }
                  aria-label={`${pinnedHashes.has(m.hash) ? "Unpin" : "Pin"} message from ${m.name || shortHash(m.author)}`}
                  className="h-fit shrink-0 rounded-full border border-zinc-700 px-2 py-0.5 text-xs font-semibold text-zinc-300 hover:bg-zinc-900"
                >
                  {pinnedHashes.has(m.hash) ? "Pinned" : "Pin"}
                </button>
              </div>
              );
            })}
          </div>

          <form
            onSubmit={handleSend}
            className="sticky bottom-0 border-t border-zinc-800 bg-zinc-950 px-3 py-2"
          >
            {replyTo && (
              <p className="mb-2 flex items-center justify-between text-xs text-zinc-400">
                <span className="truncate">
                  Replying to <span className="font-semibold text-zinc-200">{replyTo.name}</span>
                </span>
                <button
                  type="button"
                  aria-label="Cancel reply"
                  onClick={() => setReplyTo(null)}
                  className="ml-2 shrink-0 rounded-full border border-zinc-700 px-2 py-0.5 text-xs text-zinc-400 hover:bg-zinc-900"
                >
                  ✕ Cancel
                </button>
              </p>
            )}
            <div className="flex gap-2">
              <input
                value={text}
                onChange={(e) => setText(e.target.value)}
                placeholder={`Message #${channels.selected}`}
                aria-label={`Message #${channels.selected}`}
                disabled={sending}
                className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
              />
              <button
                type="submit"
                disabled={sending || !text.trim()}
                className="shrink-0 rounded-full bg-cyan-500 px-4 py-2 text-sm font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
              >
                {sending ? "…" : "Send"}
              </button>
            </div>
          </form>
        </div>
      )}
    </div>
  );
}
