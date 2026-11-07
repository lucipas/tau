"use client";

import { useEffect, useMemo, useRef } from "react";

import type { NodeInfo, TauMessage } from "@/lib/types";
import {
  formatExpiresIn,
  formatRtt,
  formatTime,
  shortHash,
} from "@/lib/format";

import { CopyButton, RadarIcon, SectionLabel } from "./ui";

export function MessageFeed({
  messages,
  node,
  live,
}: {
  messages: TauMessage[];
  node: NodeInfo | null;
  live: boolean;
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
            <MessageBubble key={message.id} message={message} />
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

function MessageBubble({ message }: { message: TauMessage }) {
  const outbound = message.direction === "out";
  const peerHash = message.peer ?? "";
  const initial = (message.peer_name || "?").slice(0, 1).toUpperCase();
  const expiry = formatExpiresIn(message.expires_at);

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
          {message.text}
        </div>

        <div
          className={`flex items-center gap-2 px-1 pt-1 font-mono text-[10px] ${
            outbound ? "text-signal/70" : "text-mist/60"
          }`}
        >
          <span>
            {outbound
              ? message.status === "queued"
                ? "queued"
                : "delivered"
              : "received"}
          </span>
          {message.rtt_ms !== undefined ? (
            <span>· rtt {formatRtt(message.rtt_ms)}</span>
          ) : null}
          {expiry !== null ? <span>· {expiry}</span> : null}
        </div>
      </div>
    </article>
  );
}
