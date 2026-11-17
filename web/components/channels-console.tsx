"use client";

import { useEffect, useRef, useState } from "react";

import { formatDateTime, shortHash } from "@/lib/format";
import type { ChannelMessage } from "@/lib/types";
import { useChannels } from "@/lib/use-channels";

import { SectionLabel, SendIcon } from "./ui";

// The Channels view: joined public channels on the left, the open
// channel's history oldest-first on the right. Joining is by exact
// name — channels are unlisted, so there is no directory to browse.
// Blocked authors never reach this view: the API filters them out.
//
// Sending and syncing need the serving node to run with --channels;
// when it does not, those actions fail with the bridge's own message
// and the failure renders beside the action instead of clearing the
// list.
export function ChannelsConsole({
  embedded = false,
  channelMessage,
}: {
  embedded?: boolean;
  // The latest live channel-message event, passed down from the
  // console: the hook appends it to the open channel.
  channelMessage: ChannelMessage | null;
}) {
  const channels = useChannels(channelMessage);
  const [joinName, setJoinName] = useState("");
  const [draft, setDraft] = useState("");
  const [syncNote, setSyncNote] = useState<string | null>(null);
  // The history scrolls to the newest message after a load or a live
  // append; a user who scrolled up to read stays where they are.
  const historyRef = useRef<HTMLDivElement | null>(null);
  const stickToBottomRef = useRef(true);

  useEffect(() => {
    const element = historyRef.current;
    if (element === null || !stickToBottomRef.current) return;
    element.scrollTop = element.scrollHeight;
  }, [channels.messages]);

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
      // The hook already recorded the failure; the draft stays so the
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
    <div
      className={
        embedded
          ? "flex min-h-0 flex-1 flex-col gap-4"
          : "flex min-h-0 flex-col gap-4"
      }
    >
      <section className="rounded-2xl border border-line/80 bg-panel/40 p-4">
        <SectionLabel>channels</SectionLabel>
        <p className="mt-2 text-[11px] leading-relaxed text-mist">
          Public topic rooms anyone can join by name. Messages are signed
          and gossiped between peers; a channel has no roster or admin.
        </p>
        <form onSubmit={submitJoin} className="mt-3 flex items-center gap-2">
          <label htmlFor="channel-join" className="sr-only">
            join a channel by name
          </label>
          <input
            id="channel-join"
            value={joinName}
            onChange={(event) => setJoinName(event.target.value)}
            placeholder="channel name"
            autoComplete="off"
            className="min-w-0 flex-1 rounded-lg border border-line bg-void/70 px-2.5 py-2 font-mono text-[12px] text-ink placeholder:text-mist/60 focus:border-signal/60 focus:outline-none"
          />
          <button
            type="submit"
            disabled={joinName.trim() === "" || channels.joining}
            className="shrink-0 rounded-lg border border-line bg-void/70 px-2.5 py-2 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal disabled:opacity-50"
          >
            {channels.joining ? "joining…" : "join"}
          </button>
        </form>
        {channels.loading ? (
          <p className="mt-3 font-mono text-[11px] text-mist">
            loading channels…
          </p>
        ) : channels.channels.length === 0 ? (
          <p className="mt-3 font-mono text-[11px] text-mist">
            no channels joined yet — join one by name above.
          </p>
        ) : (
          <ul className="mt-3 flex flex-col gap-1">
            {channels.channels.map((channel) => {
              const active = channel.channel === selected;
              return (
                <li key={channel.channel}>
                  <button
                    type="button"
                    onClick={() => channels.select(channel.channel)}
                    aria-current={active}
                    className={
                      active
                        ? "flex w-full items-center justify-between rounded-lg border border-signal/50 bg-signal/10 px-2.5 py-2 text-left font-mono text-[12px] text-signal"
                        : "flex w-full items-center justify-between rounded-lg border border-transparent px-2.5 py-2 text-left font-mono text-[12px] text-mist transition hover:border-line hover:text-ink"
                    }
                  >
                    <span className="min-w-0 truncate">
                      #{channel.channel}
                    </span>
                    <span className="shrink-0 text-[10px] opacity-70">
                      {formatDateTime(channel.joined_at)}
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
        )}
      </section>

      {selected !== null ? (
        <section className="flex min-h-0 flex-1 flex-col rounded-2xl border border-line/80 bg-panel/40 p-4">
          <div className="flex items-center justify-between gap-2">
            <SectionLabel>#{selected}</SectionLabel>
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => void runSync()}
                disabled={channels.syncing}
                className="rounded-lg border border-line bg-void/70 px-2.5 py-1.5 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal disabled:opacity-50"
              >
                {channels.syncing ? "syncing…" : "pull new history"}
              </button>
              <button
                type="button"
                onClick={() => void channels.leave(selected)}
                className="rounded-lg border border-line bg-void/70 px-2.5 py-1.5 font-mono text-[11px] text-mist transition hover:border-rose-400/50 hover:text-rose-300"
              >
                leave
              </button>
            </div>
          </div>
          {syncNote !== null ? (
            <p className="mt-2 font-mono text-[11px] text-mist">{syncNote}</p>
          ) : null}
          <div
            ref={historyRef}
            onScroll={(event) => {
              const element = event.currentTarget;
              stickToBottomRef.current =
                element.scrollHeight - element.scrollTop - element.clientHeight <
                48;
            }}
            className="thin-scroll mt-3 max-h-[50dvh] min-h-0 flex-1 overflow-y-auto"
          >
            {channels.messages.length === 0 ? (
              <p className="font-mono text-[11px] text-mist">
                no messages yet — say hello, or pull new history.
              </p>
            ) : (
              <ul className="flex flex-col gap-3">
                {channels.messages.map((message) => (
                  <li key={message.hash} className="min-w-0">
                    <p className="font-mono text-[11px] text-mist">
                      <span className="text-ink">
                        {message.name || shortHash(message.author)}
                      </span>{" "}
                      <span title={message.author}>
                        {shortHash(message.author)}
                      </span>{" "}
                      · {formatDateTime(message.created_at)}
                    </p>
                    <p className="mt-0.5 break-words text-[13px] leading-relaxed text-ink">
                      {message.text}
                    </p>
                  </li>
                ))}
              </ul>
            )}
          </div>
          <form onSubmit={submitSend} className="mt-3 flex items-start gap-2">
            <label htmlFor="channel-send" className="sr-only">
              send a message to #{selected}
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
              className="thin-scroll min-w-0 flex-1 resize-y rounded-lg border border-line bg-void/70 px-2.5 py-2 text-[13px] text-ink placeholder:text-mist/60 focus:border-signal/60 focus:outline-none"
            />
            <button
              type="submit"
              disabled={draft.trim() === "" || channels.sending}
              aria-label="send"
              className="mt-1 shrink-0 rounded-lg border border-line bg-void/70 p-2 text-mist transition hover:border-signal/50 hover:text-signal disabled:opacity-50"
            >
              <SendIcon className="h-4 w-4" />
            </button>
          </form>
        </section>
      ) : null}

      {channels.error !== null ? (
        <p role="alert" className="font-mono text-[11px] text-rose-300">
          {channels.error}
        </p>
      ) : null}
    </div>
  );
}
