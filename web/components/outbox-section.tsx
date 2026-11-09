"use client";

import { useEffect, useRef, useState } from "react";

import { formatAge, formatDateTime, peerLabel } from "@/lib/format";
import type { OutboxEntry, RetryOutboxResponse } from "@/lib/types";
import { useNow } from "@/lib/use-now";

import { SectionLabel } from "./ui";

/** Queued mail waiting on offline peers, with a way to take one back. */
export function OutboxSection({
  entries,
  heldForOthers,
  onCancel,
  onRetry,
}: {
  entries: OutboxEntry[];
  heldForOthers: number;
  onCancel: (id: string) => Promise<unknown>;
  onRetry: (id?: string) => Promise<RetryOutboxResponse>;
}) {
  const nowMs = useNow(30000);
  const now = (nowMs ?? Date.now()) / 1000;
  const [confirmingId, setConfirmingId] = useState<string | null>(null);
  const [cancellingId, setCancellingId] = useState<string | null>(null);
  const [retryingId, setRetryingId] = useState<string | "all" | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const confirmTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (confirmTimer.current) clearTimeout(confirmTimer.current);
    };
  }, []);

  function armConfirm(id: string) {
    setError(null);
    setConfirmingId(id);
    if (confirmTimer.current) clearTimeout(confirmTimer.current);
    confirmTimer.current = setTimeout(() => setConfirmingId(null), 5000);
  }

  async function confirmCancel(id: string) {
    if (confirmTimer.current) clearTimeout(confirmTimer.current);
    setConfirmingId(null);
    setCancellingId(id);
    setError(null);
    try {
      await onCancel(id);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "cancel failed");
    } finally {
      setCancellingId(null);
    }
  }

  function describeRetry(
    result: RetryOutboxResponse,
    entry?: OutboxEntry,
  ): string {
    if (entry !== undefined) {
      if (result.target_delivered) {
        return `delivered to ${peerLabel(entry.to_name, entry.to)}`;
      }
      if (result.target_still_queued) {
        return "still queued — peer unreachable";
      }
      return "left the queue (expired or cancelled)";
    }
    const parts = [
      `delivered ${result.delivered}`,
      ...(result.deposited > 0 ? [`deposited ${result.deposited}`] : []),
      `still queued ${result.still_queued}`,
      ...(result.received > 0 ? [`received ${result.received}`] : []),
    ];
    return parts.join(" · ");
  }

  async function runRetry(id?: string) {
    const key = id ?? "all";
    // The entry may leave the list on refresh; keep its name for the
    // result line before the request resolves.
    const entry = id ? entries.find((item) => item.id === id) : undefined;
    setError(null);
    setNotice(null);
    setRetryingId(key);
    try {
      const result = await onRetry(id);
      setNotice(describeRetry(result, entry));
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "retry failed");
    } finally {
      setRetryingId(null);
    }
  }

  return (
    <section className="rounded-2xl border border-line/80 bg-panel/60 p-4 shadow-[0_20px_60px_-40px_rgba(0,0,0,0.9)]">
      <div className="flex items-center justify-between">
        <SectionLabel>outbox</SectionLabel>
        <span className="inline-flex items-center gap-2">
          <button
            type="button"
            disabled={retryingId !== null}
            onClick={() => void runRetry()}
            className="rounded-md border border-line/80 bg-panel/60 px-2 py-1 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50"
          >
            {retryingId === "all" ? "retrying…" : "retry all"}
          </button>
          <span className="inline-flex items-center gap-1.5 rounded-full border border-amber/25 bg-amber/5 px-2 py-0.5 font-mono text-[10px] text-amber">
            {entries.length} queued
          </span>
        </span>
      </div>

      <p className="mt-2 font-mono text-[11px] leading-relaxed text-mist/80">
        waiting on offline peers — delivery is retried automatically.
      </p>

      {notice ? (
        <p className="mt-2 font-mono text-[11px] text-signal">{notice}</p>
      ) : null}

      {error ? (
        <p className="mt-2 font-mono text-[11px] text-rose-400">{error}</p>
      ) : null}

      <ul className="mt-3 flex flex-col gap-3">
        {entries.map((entry) => (
          <li
            key={entry.id}
            className="rounded-xl border border-line/70 bg-void/50 p-3"
          >
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <p className="truncate font-mono text-[12px] font-semibold text-zinc-100">
                  {peerLabel(entry.to_name, entry.to)}
                </p>
                <p className="mt-1 line-clamp-2 break-words text-[13px] leading-relaxed text-zinc-200">
                  {entry.text}
                </p>
              </div>
              <div className="flex shrink-0 gap-2">
                <button
                  type="button"
                  disabled={retryingId !== null}
                  onClick={() => void runRetry(entry.id)}
                  className="rounded-md border border-line/80 bg-panel/60 px-2 py-1 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50"
                >
                  {retryingId === entry.id ? "retrying…" : "retry"}
                </button>
                <button
                  type="button"
                  disabled={cancellingId === entry.id}
                  onClick={() => {
                    if (confirmingId === entry.id) {
                      void confirmCancel(entry.id);
                    } else {
                      armConfirm(entry.id);
                    }
                  }}
                  className={`rounded-md border px-2 py-1 font-mono text-[11px] transition focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50 ${
                    confirmingId === entry.id
                      ? "border-amber/60 bg-amber/10 text-amber"
                      : "border-line/80 bg-panel/60 text-mist hover:border-rose-400/50 hover:text-rose-300"
                  }`}
                >
                  {cancellingId === entry.id
                    ? "cancelling…"
                    : confirmingId === entry.id
                      ? "confirm cancel"
                      : "cancel"}
                </button>
              </div>
            </div>

            <p className="mt-2 font-mono text-[10px] leading-relaxed text-mist/60">
              {formatAge(now - entry.created_at)} · {entry.attempts}{" "}
              {entry.attempts === 1 ? "try" : "tries"} · expires{" "}
              {formatDateTime(entry.expires_at)}
              {entry.held_by.length > 0
                ? ` · sealed copy held by ${entry.held_by.length} ${entry.held_by.length === 1 ? "peer" : "peers"}`
                : ""}
            </p>
            {entry.last_error ? (
              <p className="mt-1 font-mono text-[10px] leading-relaxed text-amber/80">
                last try: {entry.last_error}
              </p>
            ) : null}
          </li>
        ))}
      </ul>

      {heldForOthers > 0 ? (
        <p className="mt-3 font-mono text-[10px] text-mist/60">
          also holding {heldForOthers} sealed{" "}
          {heldForOthers === 1 ? "message" : "messages"} for other peers.
        </p>
      ) : null}
    </section>
  );
}
