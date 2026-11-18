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
  const [pollOpen, setPollOpen] = useState(false);
  const [pollQuestion, setPollQuestion] = useState("");
  const [pollOptions, setPollOptions] = useState<string[]>(["", ""]);

  const selectedChannel = channels.channels.find((c) => c.channel === channels.selected);

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

  async function handlePoll(e: React.FormEvent) {
    e.preventDefault();
    const question = pollQuestion.trim();
    const options = pollOptions.map((o) => o.trim()).filter(Boolean);
    if (!question) return setError("A poll needs a question.");
    if (options.length < 2) return setError("A poll needs at least two options.");
    if (options.length > 8) return setError("A poll can have at most eight options.");
    if (!channels.selected) return;
    setSending(true);
    setError(null);
    try {
      await channels.poll(question, options);
      setPollQuestion("");
      setPollOptions(["", ""]);
      setPollOpen(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSending(false);
    }
  }

  async function handleVote(hash: string, option: number) {
    setError(null);
    try {
      await channels.vote(hash, option);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  async function handleClosePoll(hash: string) {
    setError(null);
    try {
      await channels.closePoll(hash);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
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
                  {m.poll_options && (
                    <div className="mt-2 flex flex-col gap-1.5" role="group" aria-label="Poll">
                      {m.poll_options.map((option, i) => {
                        const count = m.poll?.counts?.[i] ?? 0;
                        const total = m.poll?.total ?? 0;
                        const pct = total > 0 ? Math.round((count / total) * 100) : 0;
                        const closed = m.poll?.closed ?? false;
                        const myVote = m.poll?.my_vote ?? null;
                        return (
                          <button
                            key={i}
                            type="button"
                            disabled={closed}
                            onClick={() => void handleVote(m.hash, i)}
                            title={closed ? "Poll closed" : `Vote for ${option}`}
                            className={`relative overflow-hidden rounded-lg border px-2 py-1.5 text-left text-sm ${
                              myVote === i
                                ? "border-cyan-400 bg-cyan-500/20 font-semibold"
                                : "border-zinc-600 bg-zinc-900/60 hover:border-zinc-400"
                            }`}
                          >
                            <span
                              className="absolute inset-y-0 left-0 bg-zinc-700/60"
                              style={{ width: `${pct}%` }}
                              aria-hidden="true"
                            />
                            <span className="relative flex justify-between gap-2">
                              <span>{option}</span>
                              <span className="text-xs text-zinc-400">
                                {count} · {pct}%
                              </span>
                            </span>
                          </button>
                        );
                      })}
                      <span className="text-xs text-zinc-400">
                        {m.poll?.total ?? 0} vote{(m.poll?.total ?? 0) === 1 ? "" : "s"}
                        {m.poll?.closed ? " · closed" : ""}
                        {m.poll?.own && !m.poll?.closed && (
                          <button
                            type="button"
                            onClick={() => void handleClosePoll(m.hash)}
                            className="ml-2 underline hover:text-zinc-200"
                          >
                            Close poll
                          </button>
                        )}
                      </span>
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>

          <form
            onSubmit={pollOpen ? handlePoll : handleSend}
            className="sticky bottom-0 border-t border-zinc-800 bg-zinc-950 px-3 py-2"
          >
            {pollOpen ? (
              <div className="flex flex-col gap-2">
                <input
                  value={pollQuestion}
                  onChange={(e) => setPollQuestion(e.target.value)}
                  placeholder="Poll question"
                  aria-label="Poll question"
                  disabled={sending}
                  className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
                />
                {pollOptions.map((option, i) => (
                  <input
                    key={i}
                    value={option}
                    onChange={(e) =>
                      setPollOptions((prev) =>
                        prev.map((value, j) => (j === i ? e.target.value : value)),
                      )
                    }
                    placeholder={`Option ${i + 1}`}
                    aria-label={`Poll option ${i + 1}`}
                    disabled={sending}
                    className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
                  />
                ))}
                <div className="flex gap-2">
                  {pollOptions.length < 8 && (
                    <button
                      type="button"
                      onClick={() => setPollOptions((prev) => [...prev, ""])}
                      className="rounded-full border border-zinc-700 px-3 py-1.5 text-xs font-semibold text-zinc-300 hover:bg-zinc-900"
                    >
                      + Option
                    </button>
                  )}
                  <button
                    type="submit"
                    disabled={sending}
                    className="shrink-0 rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
                  >
                    {sending ? "…" : "Post poll"}
                  </button>
                  <button
                    type="button"
                    onClick={() => setPollOpen(false)}
                    className="rounded-full border border-zinc-700 px-3 py-1.5 text-xs text-zinc-300 hover:bg-zinc-900"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            ) : (
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
                  type="button"
                  onClick={() => setPollOpen(true)}
                  className="shrink-0 rounded-full border border-zinc-700 px-3 py-2 text-sm font-semibold text-zinc-300 hover:bg-zinc-900"
                >
                  Poll
                </button>
                <button
                  type="submit"
                  disabled={sending || !text.trim()}
                  className="shrink-0 rounded-full bg-cyan-500 px-4 py-2 text-sm font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
                >
                  {sending ? "…" : "Send"}
                </button>
              </div>
            )}
          </form>
        </div>
      )}
    </div>
  );
}
