"use client";

import { useEffect, useRef, useState } from "react";

import type { Peer, TauMessage } from "@/lib/types";
import { MAX_MESSAGE_CHARS } from "@/lib/constants";

import { SendIcon } from "./ui";

export function Composer({
  peers,
  target,
  mode,
  onTargetChange,
  onModeChange,
  onSend,
  replyTo,
  onCancelReply,
}: {
  peers: Peer[];
  target: string;
  mode: "peer" | "custom";
  onTargetChange: (value: string) => void;
  onModeChange: (mode: "peer" | "custom") => void;
  onSend: (to: string, text: string, replyTo?: string) => Promise<TauMessage>;
  replyTo?: TauMessage | null;
  onCancelReply?: () => void;
}) {
  const [text, setText] = useState("");
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const custom = peers.length === 0 || mode === "custom";
  const selectValue = custom ? "__custom" : target;

  useEffect(() => {
    textareaRef.current?.focus();
  }, []);

  async function submit() {
    const trimmedTarget = target.trim();
    const trimmedText = text.trim();
    if (!trimmedTarget) {
      setError("Choose a peer or enter a destination hash first.");
      return;
    }
    if (!trimmedText) {
      setError("Write a message first.");
      return;
    }
    setSending(true);
    setError(null);
    try {
      await onSend(trimmedTarget, text, replyTo?.wire_id);
      setText("");
      onCancelReply?.();
      textareaRef.current?.focus();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "send failed");
    } finally {
      setSending(false);
    }
  }

  function onKeyDown(event: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) {
      event.preventDefault();
      if (!sending) void submit();
    }
  }

  return (
    <section className="mt-3 rounded-2xl border border-line/80 bg-panel/60 p-3">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center">
        <label
          htmlFor="peer-target"
          className="font-mono text-[10px] uppercase tracking-[0.18em] text-mist"
        >
          to
        </label>
        <div className="flex min-w-0 flex-1 flex-col gap-2 sm:flex-row sm:items-center">
          {peers.length > 0 ? (
            <select
              id="peer-target"
              value={selectValue}
              onChange={(event) => {
                const value = event.target.value;
                if (value === "__custom") {
                  onModeChange("custom");
                  onTargetChange("");
                } else {
                  onModeChange("peer");
                  onTargetChange(value);
                }
              }}
              className="h-9 w-full rounded-lg border border-line bg-void/70 px-2.5 font-mono text-xs text-zinc-200 outline-none transition focus:border-signal/60 focus:ring-2 focus:ring-signal/20 sm:max-w-[260px]"
            >
              {peers.map((peer) => (
                <option key={peer.destination} value={peer.destination}>
                  {peer.name || "unknown"} · {peer.destination.slice(0, 8)}
                </option>
              ))}
              <option value="__custom">custom address…</option>
            </select>
          ) : null}

          {custom ? (
            <input
              id={peers.length === 0 ? "peer-target" : "peer-target-custom"}
              value={target}
              onChange={(event) => {
                onTargetChange(event.target.value);
                setError(null);
              }}
              placeholder="peer name or 32-character destination hash"
              spellCheck={false}
              autoComplete="off"
              className="h-9 w-full rounded-lg border border-line bg-void/70 px-2.5 font-mono text-xs text-zinc-200 outline-none transition placeholder:text-mist/60 focus:border-signal/60 focus:ring-2 focus:ring-signal/20"
            />
          ) : null}
        </div>
        <span className="hidden font-mono text-[10px] text-mist/70 sm:inline">
          encrypted link
        </span>
      </div>

      {replyTo ? (
        <div className="mt-2 flex items-center gap-2 rounded-lg border border-signal/30 bg-signal/[0.06] px-3 py-2 text-[11px] text-zinc-300">
          <span className="min-w-0 flex-1 truncate font-mono">
            ↩ replying to {replyTo.direction === "out" ? "you" : replyTo.peer_name}:{" "}
            {replyTo.text.split("\n")[0].slice(0, 80)}
          </span>
          <button
            type="button"
            title="Cancel reply"
            onClick={() => onCancelReply?.()}
            className="shrink-0 rounded px-1.5 py-0.5 font-mono text-mist transition hover:bg-void/60 hover:text-zinc-100"
          >
            ✕
          </button>
        </div>
      ) : null}

      <div className="mt-2 flex items-end gap-2">
        <textarea
          ref={textareaRef}
          id="message-text"
          value={text}
          onChange={(event) => {
            setText(event.target.value.slice(0, MAX_MESSAGE_CHARS));
            setError(null);
          }}
          onKeyDown={onKeyDown}
          rows={2}
          maxLength={MAX_MESSAGE_CHARS}
          placeholder={
            replyTo
              ? `Replying to ${replyTo.direction === "out" ? "you" : replyTo.peer_name}…`
              : "Write something to the mesh…"
          }
          className="thin-scroll min-h-[64px] w-full resize-y rounded-xl border border-line bg-void/70 px-3 py-2.5 text-[13px] leading-relaxed text-zinc-100 outline-none transition placeholder:text-mist/60 focus:border-signal/60 focus:ring-2 focus:ring-signal/20"
        />
        <button
          type="button"
          onClick={() => void submit()}
          disabled={sending || !text.trim() || !target.trim()}
          className="inline-flex h-[52px] shrink-0 items-center gap-2 rounded-xl border border-signal/40 bg-signal/15 px-4 text-xs font-medium text-signal transition hover:border-signal/70 hover:bg-signal/25 disabled:cursor-not-allowed disabled:border-line disabled:bg-void/60 disabled:text-mist/60"
        >
          <SendIcon className="h-4 w-4" />
          <span className="hidden sm:inline">
            {sending ? "sending…" : "send"}
          </span>
        </button>
      </div>

      <div className="mt-1.5 flex flex-wrap items-center justify-between gap-2">
        <p
          className={`text-[11px] ${
            error ? "text-rose-300" : "text-mist/70"
          }`}
          role={error ? "alert" : undefined}
        >
          {error ??
            (sending
              ? "opening link and waiting for the peer…"
              : "Enter to send · Shift+Enter for a new line")}
        </p>
        <span className="font-mono text-[10px] text-mist/60">
          {text.length}/{MAX_MESSAGE_CHARS}
        </span>
      </div>
    </section>
  );
}
