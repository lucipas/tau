"use client";

import { useEffect, useMemo, useRef } from "react";

import type { NodeInfo, TauMessage } from "@/lib/types";
import {
  formatExpiresIn,
  formatFileSize,
  formatRtt,
  formatTime,
  shortHash,
} from "@/lib/format";

import { CopyButton, RadarIcon, SectionLabel } from "./ui";

export function MessageFeed({
  messages,
  node,
  live,
  onReply,
}: {
  messages: TauMessage[];
  node: NodeInfo | null;
  live: boolean;
  onReply?: (message: TauMessage) => void;
}) {
  const endRef = useRef<HTMLDivElement>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const stickToBottom = useRef(true);

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
    if (!stickToBottom.current) return;
    endRef.current?.scrollIntoView({ block: "end", behavior: "smooth" });
  }, [messages.length]);

  function onScroll() {
    const element = scrollRef.current;
    if (!element) return;
    const distance =
      element.scrollHeight - element.scrollTop - element.clientHeight;
    stickToBottom.current = distance < 140;
  }

  return (
    <section className="flex min-h-0 flex-1 flex-col rounded-2xl border border-line/80 bg-panel/40">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-line/60 px-4 py-3">
        <div className="flex items-center gap-3">
          <SectionLabel>traffic</SectionLabel>
          <span className="font-mono text-[10px] text-mist">
            {counts.inbound} in / {counts.outbound} out
          </span>
        </div>
        <span className="inline-flex items-center gap-1.5 font-mono text-[10px] text-mist">
          <span
            className={`h-1.5 w-1.5 rounded-full ${
              live ? "bg-signal" : "bg-amber"
            }`}
          />
          {live ? "streaming" : "waiting for bridge"}
        </span>
      </div>

      <div
        ref={scrollRef}
        onScroll={onScroll}
        aria-live="polite"
        aria-label="Message traffic"
        className="thin-scroll min-h-[320px] flex-1 space-y-4 overflow-y-auto px-4 py-5 lg:max-h-[calc(100dvh-330px)]"
      >
        {messages.length === 0 ? (
          <EmptyState node={node} />
        ) : (
          messages.map((message) => (
            <MessageBubble
              key={message.id}
              message={message}
              messages={messages}
              onReply={onReply}
            />
          ))
        )}
        <div ref={endRef} />
      </div>
    </section>
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
  const ref = message.in_reply_to;
  if (!ref || !ref.id) return null;
  const candidates = messages.filter(
    (entry) => entry.peer === message.peer && entry.wire_id === ref.id,
  );
  if (candidates.length === 0) return "unavailable";
  let quoted = candidates[0];
  if (ref.excerpt) {
    const verified = candidates.find(
      (entry) => entry.excerpt_hash === ref.excerpt,
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
  onReply,
}: {
  message: TauMessage;
  messages: TauMessage[];
  onReply?: (message: TauMessage) => void;
}) {
  const outbound = message.direction === "out";
  const peerHash = message.peer ?? "";
  const initial = (message.peer_name || "?").slice(0, 1).toUpperCase();
  const expiry = formatExpiresIn(message.expires_at);
  const quote = resolveQuote(messages, message);

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
          {message.text}
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
                href={`/api/files/${message.peer}/${encodeURIComponent(message.file.name)}`}
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
                ? "queued"
                : message.status === "read"
                  ? "read"
                  : "delivered"
              : "received"}
          </span>
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
        </div>
      </div>
    </article>
  );
}
