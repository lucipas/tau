// MessageBubble.tsx — one chat message: bubble, reply quote, reactions,
// file attachment, location pin, contact card, group poll + vote,
// disappearing countdown, and the reply / edit / delete / retract / react /
// pin / forward actions.

"use client";

import { useState } from "react";
import { fileUrl } from "@/lib/api";
import { formatBytes, formatCountdown, formatExpiry, formatTime } from "@/lib/format";
import { useNow } from "@/lib/use-now";
import type { ChatMessage } from "@/lib/types";
import Avatar from "../chrome/Avatar";

export type MessageAction =
  | "reply"
  | "edit"
  | "delete"
  | "retract"
  | "pin"
  | "forward";

export interface MessageBubbleProps {
  message: ChatMessage;
  /** Name shown above incoming messages (group threads). */
  senderName?: string;
  senderSeed?: string;
  /** Show the sender avatar (group threads / incoming). */
  showAvatar?: boolean;
  /** Forward targets for the forward picker. */
  forwardTargets?: { key: string; kind: "dm" | "group"; name: string }[];
  now?: number;
  onAction?: (action: MessageAction, message: ChatMessage) => void;
  onReact?: (message: ChatMessage, reaction: string) => void;
  onClearReaction?: (message: ChatMessage) => void;
  onForward?: (message: ChatMessage, target: string) => void;
  /** Snooze a received message: re-ping about it after this many seconds. */
  onSnooze?: (message: ChatMessage, inSeconds: number) => void;
  onVotePoll?: (message: ChatMessage, option: number) => void;
  onClosePoll?: (message: ChatMessage) => void;
  /** Add the peer from a received contact card to the registry. */
  onAddPeer?: (message: ChatMessage) => Promise<void> | void;
}

const REACTION_CHOICES = ["👍", "❤️", "😂", "😮", "😢", "🔥"];

/** Seconds until 09:00 local time on the next calendar day. */
export function secondsUntilTomorrowNine(from = new Date()): number {
  const target = new Date(from);
  target.setDate(target.getDate() + 1);
  target.setHours(9, 0, 0, 0);
  return Math.max(60, Math.round((target.getTime() - from.getTime()) / 1000));
}

const SNOOZE_PRESETS: { label: string; seconds: number | "tomorrow" }[] = [
  { label: "30 min", seconds: 30 * 60 },
  { label: "1 hour", seconds: 60 * 60 },
  { label: "3 hours", seconds: 3 * 60 * 60 },
  { label: "Tomorrow 9am", seconds: "tomorrow" },
];

function groupReactions(message: ChatMessage): { reaction: string; count: number; names: string[] }[] {
  const map = new Map<string, { count: number; names: string[] }>();
  for (const r of message.reactions ?? []) {
    const entry = map.get(r.reaction) ?? { count: 0, names: [] };
    entry.count += 1;
    entry.names.push(r.name || r.reactor.slice(0, 8));
    map.set(r.reaction, entry);
  }
  return [...map.entries()].map(([reaction, v]) => ({ reaction, ...v }));
}

export default function MessageBubble({
  message,
  senderName,
  senderSeed,
  showAvatar = false,
  forwardTargets = [],
  now: nowProp,
  onAction,
  onReact,
  onClearReaction,
  onForward,
  onSnooze,
  onVotePoll,
  onClosePoll,
  onAddPeer,
}: MessageBubbleProps) {
  const liveNow = useNow(1000);
  const now = nowProp ?? liveNow;
  const [menuOpen, setMenuOpen] = useState(false);
  const [reactOpen, setReactOpen] = useState(false);
  const [forwardOpen, setForwardOpen] = useState(false);
  const [snoozeOpen, setSnoozeOpen] = useState(false);
  const [forwardTarget, setForwardTarget] = useState("");
  const [peerAdded, setPeerAdded] = useState(false);
  const [addPeerError, setAddPeerError] = useState<string | null>(null);

  const own = message.direction === "out";
  const reactions = groupReactions(message);
  const pollOptions = message.poll_options ?? message.poll?.options ?? null;
  const pollClosed = message.poll?.closed ?? false;
  const myVote = message.poll?.my_vote ?? null;

  const statusLabel =
    message.status === "read"
      ? "Read"
      : message.status === "delivered"
        ? "Delivered"
        : message.status === "failed"
          ? "Failed"
          : message.status === "queued"
            ? "Queued"
            : message.status === "sent"
              ? "Sent"
              : message.status;

  return (
    <div className={`flex gap-2 px-3 py-1 ${own ? "justify-end" : "justify-start"}`} data-message-id={message.id}>
      {!own && showAvatar && (
        <Avatar name={senderName ?? message.peer_name} seed={senderSeed ?? message.peer ?? ""} size="sm" className="mt-1" />
      )}
      <div className={`max-w-[80%] ${own ? "items-end" : "items-start"} flex flex-col`}>
        {!own && senderName && (
          <span className="mb-0.5 text-xs font-semibold text-zinc-400">{senderName}</span>
        )}
        {message.in_reply_to?.excerpt && (
          <div className="mb-1 max-w-full truncate rounded-lg border-l-2 border-cyan-500 bg-zinc-900 px-2 py-1 text-xs text-zinc-400">
            <span className="font-semibold text-zinc-300">{message.in_reply_to.peer_name ?? "Reply"}: </span>
            {message.in_reply_to.excerpt}
          </div>
        )}

        <div
          className={`rounded-2xl px-3 py-2 text-[15px] leading-snug break-words ${
            own ? "rounded-br-sm bg-cyan-600 text-white" : "rounded-bl-sm bg-zinc-800 text-zinc-100"
          }`}
        >
          {message.text && <p className="whitespace-pre-wrap">{message.text}</p>}

          {message.file && message.file.voice ? (
            <span className="mt-1 block">
              <span className="mb-1 block text-xs opacity-80">🎙️ Voice note</span>
              <audio
                controls
                preload="metadata"
                src={fileUrl(message.peer ?? "", message.file.name)}
                className="max-w-full"
                aria-label={`Voice note (${formatBytes(message.file.size)})`}
              />
            </span>
          ) : (
            message.file && (
              <p className="mt-1">
                <a
                  href={fileUrl(message.peer ?? "", message.file.name)}
                  download={message.file.name}
                  className={`inline-flex items-center gap-1 underline ${own ? "text-cyan-100" : "text-cyan-300"}`}
                >
                  📎 {message.file.name} ({formatBytes(message.file.size)})
                </a>
              </p>
            )
          )}

          {message.location && (
            <p className="mt-1 text-sm">
              <a
                href={`https://www.openstreetmap.org/?mlat=${message.location.lat}&mlon=${message.location.lon}#map=15/${message.location.lat}/${message.location.lon}`}
                target="_blank"
                rel="noreferrer"
                className={`underline ${own ? "text-cyan-100" : "text-cyan-300"}`}
              >
                📍 {message.location.name ?? `${message.location.lat.toFixed(5)}, ${message.location.lon.toFixed(5)}`}
              </a>
            </p>
          )}

          {message.contact && (
            <div className="mt-1 rounded-lg border border-zinc-600/60 bg-zinc-900/60 px-2 py-1.5 text-sm">
              <p className="font-semibold">👤 {message.contact.name}</p>
              <p className="text-xs text-zinc-400">tau:{message.contact.destination_hash.slice(0, 8)}…</p>
              {!own && onAddPeer && !peerAdded && (
                <button
                  type="button"
                  onClick={async () => {
                    setAddPeerError(null);
                    try {
                      await onAddPeer(message);
                      setPeerAdded(true);
                    } catch (err) {
                      setAddPeerError(err instanceof Error ? err.message : String(err));
                    }
                  }}
                  className={`mt-1 underline ${own ? "text-cyan-100" : "text-cyan-300"} hover:text-zinc-200`}
                >
                  Add peer
                </button>
              )}
              {peerAdded && <p className="mt-1 text-xs text-zinc-400">Added — you can message them now.</p>}
              {addPeerError && (
                <p role="alert" className="mt-1 text-xs text-red-300">
                  {addPeerError}
                </p>
              )}
            </div>
          )}

          {message.beacon && (
            <p className="mt-1 text-sm text-zinc-300">
              📍 Live location until {formatCountdown(message.beacon.expires_at, now)}
            </p>
          )}

          {pollOptions && (
            <div className="mt-2 flex flex-col gap-1.5" role="group" aria-label="Poll">
              {pollOptions.map((option, i) => {
                const count = message.poll?.counts?.[i] ?? 0;
                const total = message.poll?.total ?? 0;
                const pct = total > 0 ? Math.round((count / total) * 100) : 0;
                return (
                  <button
                    key={i}
                    type="button"
                    disabled={pollClosed || !onVotePoll}
                    onClick={() => onVotePoll?.(message, i)}
                    title={pollClosed ? "Poll closed" : `Vote for ${option}`}
                    className={`relative overflow-hidden rounded-lg border px-2 py-1.5 text-left text-sm ${
                      myVote === i
                        ? "border-cyan-400 bg-cyan-500/20 font-semibold"
                        : "border-zinc-600 bg-zinc-900/60 hover:border-zinc-400"
                    }`}
                  >
                    <span className="absolute inset-y-0 left-0 bg-zinc-700/60" style={{ width: `${pct}%` }} aria-hidden="true" />
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
                {message.poll?.total ?? 0} vote{(message.poll?.total ?? 0) === 1 ? "" : "s"}
                {pollClosed ? " · closed" : ""}
                {own && !pollClosed && onClosePoll && (
                  <button type="button" onClick={() => onClosePoll(message)} className="ml-2 underline hover:text-zinc-200">
                    Close poll
                  </button>
                )}
              </span>
            </div>
          )}
        </div>

        <div className="mt-0.5 flex items-center gap-2 text-[11px] text-zinc-500">
          <span title={message.edited_at ? `Edited ${formatTime(message.edited_at)}` : undefined}>
            {formatTime(message.sent_at || message.received_at)}
            {message.edited_at ? " · edited" : ""}
          </span>
          {message.expires_at != null && (
            <span className="text-amber-400/90" title="Disappearing message">
              ⏳ {formatExpiry(message.expires_at, now)}
            </span>
          )}
          {message.send_after != null && message.send_after > now && (
            <span title="Scheduled send">🕑 {formatCountdown(message.send_after, now)}</span>
          )}
          {own && <span>{statusLabel}</span>}
          {message.pinned && <span title="Pinned">📌</span>}
          <button
            type="button"
            aria-label={`Actions for message ${message.id}`}
            aria-expanded={menuOpen}
            onClick={() => { setMenuOpen((v) => !v); setReactOpen(false); setForwardOpen(false); setSnoozeOpen(false); }}
            className="rounded px-1 text-zinc-500 hover:bg-zinc-800 hover:text-zinc-200"
          >
            ⋯
          </button>
        </div>

        {reactions.length > 0 && (
          <div className="mt-1 flex flex-wrap gap-1" aria-label="Reactions">
            {reactions.map((r) => (
              <button
                key={r.reaction}
                type="button"
                title={r.names.join(", ")}
                onClick={() => onClearReaction?.(message)}
                className="rounded-full border border-zinc-700 bg-zinc-900 px-2 py-0.5 text-xs text-zinc-200 hover:border-zinc-500"
              >
                {r.reaction} {r.count}
              </button>
            ))}
          </div>
        )}

        {menuOpen && (
          <div className="mt-1 flex flex-wrap items-center gap-1 rounded-lg border border-zinc-700 bg-zinc-900 p-1" role="menu">
            <button type="button" onClick={() => { onAction?.("reply", message); setMenuOpen(false); }} className="rounded px-2 py-1 text-xs text-zinc-200 hover:bg-zinc-800">Reply</button>
            {own && (
              <>
                <button type="button" onClick={() => { onAction?.("edit", message); setMenuOpen(false); }} className="rounded px-2 py-1 text-xs text-zinc-200 hover:bg-zinc-800">Edit</button>
                <button type="button" onClick={() => { onAction?.("retract", message); setMenuOpen(false); }} className="rounded px-2 py-1 text-xs text-zinc-200 hover:bg-zinc-800">Retract</button>
              </>
            )}
            <button type="button" onClick={() => { onAction?.("delete", message); setMenuOpen(false); }} className="rounded px-2 py-1 text-xs text-zinc-200 hover:bg-zinc-800">Delete</button>
            <button type="button" onClick={() => { onAction?.("pin", message); setMenuOpen(false); }} className="rounded px-2 py-1 text-xs text-zinc-200 hover:bg-zinc-800">{message.pinned ? "Unpin" : "Pin"}</button>
            {!own && !message.file && onSnooze && (
              <button type="button" onClick={() => { setSnoozeOpen((v) => !v); setReactOpen(false); setForwardOpen(false); }} aria-expanded={snoozeOpen} className="rounded px-2 py-1 text-xs text-zinc-200 hover:bg-zinc-800">Snooze</button>
            )}
            <button type="button" onClick={() => { setForwardOpen((v) => !v); setReactOpen(false); setSnoozeOpen(false); }} aria-expanded={forwardOpen} className="rounded px-2 py-1 text-xs text-zinc-200 hover:bg-zinc-800">Forward</button>
            <button type="button" onClick={() => { setReactOpen((v) => !v); setForwardOpen(false); setSnoozeOpen(false); }} aria-expanded={reactOpen} className="rounded px-2 py-1 text-xs text-zinc-200 hover:bg-zinc-800">React</button>
          </div>
        )}

        {snoozeOpen && (
          <div className="mt-1 flex flex-wrap gap-1 rounded-lg border border-zinc-700 bg-zinc-900 p-1" role="group" aria-label="Snooze for">
            {SNOOZE_PRESETS.map((preset) => (
              <button
                key={preset.label}
                type="button"
                onClick={() => {
                  onSnooze?.(message, preset.seconds === "tomorrow" ? secondsUntilTomorrowNine() : preset.seconds);
                  setSnoozeOpen(false);
                  setMenuOpen(false);
                }}
                className="rounded px-2 py-1 text-xs text-zinc-200 hover:bg-zinc-800"
              >
                {preset.label}
              </button>
            ))}
          </div>
        )}

        {reactOpen && (
          <div className="mt-1 flex gap-1 rounded-lg border border-zinc-700 bg-zinc-900 p-1" role="group" aria-label="Choose a reaction">
            {REACTION_CHOICES.map((emoji) => (
              <button
                key={emoji}
                type="button"
                aria-label={`React ${emoji}`}
                onClick={() => { onReact?.(message, emoji); setReactOpen(false); }}
                className="rounded px-1.5 py-0.5 text-lg hover:bg-zinc-800"
              >
                {emoji}
              </button>
            ))}
          </div>
        )}

        {forwardOpen && (
          <div className="mt-1 flex items-center gap-2 rounded-lg border border-zinc-700 bg-zinc-900 p-2">
            <label className="sr-only" htmlFor={`forward-${message.id}`}>Forward to</label>
            <select
              id={`forward-${message.id}`}
              value={forwardTarget}
              onChange={(e) => setForwardTarget(e.target.value)}
              className="min-w-0 rounded-md border border-zinc-700 bg-zinc-950 px-2 py-1 text-xs text-zinc-200 outline-none"
            >
              <option value="">Forward to…</option>
              {forwardTargets.map((t) => (
                <option key={`${t.kind}:${t.key}`} value={`${t.kind}:${t.key}`}>
                  {t.kind === "group" ? "Group: " : ""}{t.name}
                </option>
              ))}
            </select>
            <button
              type="button"
              disabled={!forwardTarget}
              onClick={() => { onForward?.(message, forwardTarget); setForwardOpen(false); }}
              data-forward-target={forwardTarget}
              className="rounded-full bg-cyan-500 px-3 py-1 text-xs font-bold text-zinc-950 disabled:opacity-40"
            >
              Forward
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
