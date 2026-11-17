"use client";

import { useState } from "react";

import type { useChannels } from "@/lib/use-channels";
import type { UseTauResult } from "@/lib/use-tau";
import { formatDateTime, shortHash } from "@/lib/format";
import { Button, Card, CardTitle, EmptyState, ErrorNote } from "./ui";

const inputClass =
  "min-w-0 flex-1 rounded-lg border border-line bg-panel-2 px-3 py-2 text-sm text-ink placeholder:text-faint focus:border-signal-dim focus:outline-none";

export function ChannelsTab({
  channels,
}: {
  tau: UseTauResult;
  channels: ReturnType<typeof useChannels>;
}) {
  const [joinName, setJoinName] = useState("");
  const [draft, setDraft] = useState("");
  const [syncNote, setSyncNote] = useState<string | null>(null);

  const selected = channels.selected;

  async function submitJoin(event: React.FormEvent) {
    event.preventDefault();
    const name = joinName.trim();
    if (name === "" || channels.joining) return;
    await channels.join(name);
    setJoinName("");
  }

  async function submitSend(event: React.FormEvent) {
    event.preventDefault();
    const text = draft.trim();
    if (text === "" || channels.sending || selected === null) return;
    try {
      await channels.send(text);
      setDraft("");
    } catch {
      // The hook already recorded the failure; keep the draft so the
      // message is not lost to a transient send error.
    }
  }

  async function runSync() {
    if (channels.syncing || selected === null) return;
    setSyncNote(null);
    const summary = await channels.sync();
    if (summary !== null) {
      setSyncNote(
        summary.stored > 0
          ? `pulled ${summary.stored} new message${summary.stored === 1 ? "" : "s"} from ${summary.peers} peer${summary.peers === 1 ? "" : "s"}`
          : "already up to date",
      );
    }
  }

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <Card>
        <CardTitle>Channels</CardTitle>
        <div className="flex flex-col gap-3 px-3 py-3">
          <p className="text-sm leading-relaxed text-mist">
            Public topic rooms anyone can join by name. Messages are signed
            and gossiped between peers; a channel has no roster or admin.
          </p>
          <form onSubmit={submitJoin} className="flex min-w-0 gap-2">
            <label htmlFor="channel-join" className="sr-only">
              Join a channel by name
            </label>
            <input
              id="channel-join"
              value={joinName}
              onChange={(event) => setJoinName(event.target.value)}
              placeholder="channel name"
              autoComplete="off"
              className={inputClass}
            />
            <Button
              type="submit"
              variant="primary"
              disabled={joinName.trim() === "" || channels.joining}
            >
              {channels.joining ? "joining…" : "join"}
            </Button>
          </form>
          {channels.loading ? (
            <p className="text-sm text-faint">loading channels…</p>
          ) : channels.channels.length === 0 ? (
            <EmptyState>
              No channels joined yet — join one by name above.
            </EmptyState>
          ) : (
            <ul className="flex flex-col gap-1">
              {channels.channels.map((channel) => {
                const active = channel.channel === selected;
                return (
                  <li key={channel.channel}>
                    <button
                      type="button"
                      onClick={() => channels.select(channel.channel)}
                      aria-current={active}
                      className={`flex w-full min-w-0 flex-wrap items-center justify-between gap-2 rounded-lg border px-3 py-2 text-left text-sm ${
                        active
                          ? "border-signal-dim bg-signal/10 text-signal"
                          : "border-transparent text-mist hover:border-line hover:text-ink"
                      }`}
                    >
                      <span className="min-w-0 break-words">
                        #{channel.channel}
                      </span>
                      <span className="shrink-0 text-xs opacity-70">
                        joined {formatDateTime(channel.joined_at)}
                      </span>
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
          <ErrorNote message={channels.error} />
        </div>
      </Card>

      {selected === null ? (
        <Card>
          <EmptyState>
            Select a channel above to read its messages.
          </EmptyState>
        </Card>
      ) : (
        <Card>
          <CardTitle
            right={
              <span className="flex flex-wrap gap-2">
                <Button
                  onClick={() => void runSync()}
                  disabled={channels.syncing}
                >
                  {channels.syncing ? "syncing…" : "sync"}
                </Button>
                <Button
                  variant="danger"
                  onClick={() => void channels.leave(selected)}
                >
                  leave
                </Button>
              </span>
            }
          >
            <span className="min-w-0 break-words">#{selected}</span>
          </CardTitle>
          <div className="flex flex-col gap-3 px-3 py-3">
            {syncNote !== null ? (
              <p className="text-sm text-mist">{syncNote}</p>
            ) : null}
            {channels.messages.length === 0 ? (
              <EmptyState>
                No messages yet — say hello, or sync to pull history.
              </EmptyState>
            ) : (
              <ul className="flex flex-col gap-3">
                {channels.messages.map((message) => (
                  <li key={message.hash} className="min-w-0">
                    <p className="flex min-w-0 flex-wrap items-baseline gap-x-2 text-xs text-mist">
                      <span className="min-w-0 break-words font-medium text-ink">
                        {message.name || shortHash(message.author)}
                      </span>
                      <span title={message.author} className="break-all">
                        {shortHash(message.author)}
                      </span>
                      <span className="shrink-0">
                        {formatDateTime(message.created_at)}
                      </span>
                    </p>
                    <p className="mt-0.5 break-words text-sm leading-relaxed text-ink">
                      {message.text}
                    </p>
                  </li>
                ))}
              </ul>
            )}
            <form onSubmit={submitSend} className="flex min-w-0 gap-2">
              <label htmlFor="channel-send" className="sr-only">
                Send a message to #{selected}
              </label>
              <textarea
                id="channel-send"
                value={draft}
                onChange={(event) => setDraft(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" && !event.shiftKey) {
                    event.preventDefault();
                    void submitSend(event);
                  }
                }}
                rows={2}
                placeholder={`message #${selected}`}
                className={`${inputClass} resize-y`}
              />
              <Button
                type="submit"
                variant="primary"
                disabled={draft.trim() === "" || channels.sending}
              >
                {channels.sending ? "sending…" : "send"}
              </Button>
            </form>
          </div>
        </Card>
      )}
    </div>
  );
}

export default ChannelsTab;
