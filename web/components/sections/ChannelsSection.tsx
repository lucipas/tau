// ChannelsSection.tsx — public channels: join / leave, per-channel
// message history with a composer, and sync. Built on useChannels
// (web/lib/use-channels.ts).

"use client";

import { useState } from "react";
import Avatar from "@/components/chrome/Avatar";
import { formatRelative, shortHash } from "@/lib/format";
import { useChannels } from "@/lib/use-channels";

export default function ChannelsSection() {
  const channels = useChannels(30_000);
  const [joinName, setJoinName] = useState("");
  const [text, setText] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [sending, setSending] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [reacting, setReacting] = useState<string | null>(null);

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
      await channels.send(body);
      setText("");
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

  // History arrives newest-first; show it oldest-first like a chat log.
  const messages = [...channels.messages].reverse();

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

          <div className="px-3 py-2 sm:px-4">
            {messages.length === 0 && (
              <p className="py-6 text-center text-sm text-zinc-500">
                No messages in this channel yet — say hello.
              </p>
            )}
            {messages.map((m) => (
              <div key={m.hash} className="flex gap-3 border-b border-zinc-800/60 py-3">
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
                </div>
              </div>
            ))}
          </div>

          <form
            onSubmit={handleSend}
            className="sticky bottom-0 border-t border-zinc-800 bg-zinc-950 px-3 py-2"
          >
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
