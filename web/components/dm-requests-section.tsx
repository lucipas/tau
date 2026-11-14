"use client";

import { useEffect, useRef, useState } from "react";

import { formatAge, formatDateTime, peerLabel } from "@/lib/format";
import type {
  AcceptDmRequestResponse,
  DeclineDmRequestResponse,
  DmRequestEntry,
} from "@/lib/types";

import { useNow } from "@/lib/use-now";

import { SectionLabel } from "./ui";

/** DMs held for review from peers outside the allow-list. */
export function DmRequestsSection({
  requests,
  onAccept,
  onDecline,
}: {
  requests: DmRequestEntry[];
  onAccept: (id: string) => Promise<AcceptDmRequestResponse>;
  onDecline: (
    id: string,
    options?: { block?: boolean },
  ) => Promise<DeclineDmRequestResponse>;
}) {
  const nowMs = useNow(30000);
  const now = (nowMs ?? Date.now()) / 1000;
  const [confirmingId, setConfirmingId] = useState<string | null>(null);
  const [confirmingBlockId, setConfirmingBlockId] = useState<string | null>(
    null,
  );
  const [acceptingId, setAcceptingId] = useState<string | null>(null);
  const [decliningId, setDecliningId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const confirmTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (confirmTimer.current) clearTimeout(confirmTimer.current);
    };
  }, []);

  // Nothing held and no feedback to show: stay out of the way, but
  // keep this component (and its result state) mounted.
  if (requests.length === 0 && !notice && !error) return null;

  function armConfirm(id: string) {
    setError(null);
    setNotice(null);
    setConfirmingId(id);
    setConfirmingBlockId(null);
    if (confirmTimer.current) clearTimeout(confirmTimer.current);
    confirmTimer.current = setTimeout(() => setConfirmingId(null), 5000);
  }

  function armBlockConfirm(id: string) {
    setError(null);
    setNotice(null);
    setConfirmingBlockId(id);
    setConfirmingId(null);
    if (confirmTimer.current) clearTimeout(confirmTimer.current);
    confirmTimer.current = setTimeout(
      () => setConfirmingBlockId(null),
      5000,
    );
  }

  async function confirmAccept(id: string) {
    // Accepting is not destructive, so it needs no second click — but
    // a second accept while one is in flight would resolve the same
    // request twice.
    if (acceptingId !== null || decliningId !== null) return;
    setAcceptingId(id);
    setError(null);
    setNotice(null);
    try {
      const result = await onAccept(id);
      setNotice(
        result.delivered
          ? "accepted — sender allow-listed, message delivered to the inbox."
          : `accepted — sender allow-listed, message not delivered (${result.skipped ?? "already delivered"}).`,
      );
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "accept failed");
    } finally {
      setAcceptingId(null);
    }
  }

  async function confirmDecline(id: string, block: boolean) {
    // Declining drops the held message for good; blocking as well
    // keeps the sender from re-requesting straight away.
    if (acceptingId !== null || decliningId !== null) return;
    if (confirmTimer.current) clearTimeout(confirmTimer.current);
    setConfirmingId(null);
    setConfirmingBlockId(null);
    setDecliningId(id);
    setError(null);
    setNotice(null);
    try {
      await onDecline(id, { block });
      setNotice(
        block
          ? "declined — sender blocked, the held message was dropped."
          : "declined — the held message was dropped.",
      );
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "decline failed");
    } finally {
      setDecliningId(null);
    }
  }

  return (
    <section className="rounded-2xl border border-line/80 bg-panel/60 p-4 shadow-[0_20px_60px_-40px_rgba(0,0,0,0.9)]">
      <div className="flex items-center justify-between">
        <SectionLabel>DM requests</SectionLabel>
        <span className="inline-flex items-center gap-1.5 rounded-full border border-amber/25 bg-amber/5 px-2 py-0.5 font-mono text-[10px] text-amber">
          {requests.length} held
        </span>
      </div>

      <p className="mt-2 font-mono text-[11px] leading-relaxed text-mist/80">
        messages from peers outside the allow-list — accept to allow-list
        the sender and deliver the message, decline to drop it, decline
        &amp; block to drop it and keep the sender from re-requesting.
      </p>

      {notice ? (
        <p className="mt-2 font-mono text-[11px] text-signal">{notice}</p>
      ) : null}

      {error ? (
        <p className="mt-2 font-mono text-[11px] text-rose-400">{error}</p>
      ) : null}

      <ul className="mt-3 flex flex-col gap-3">
        {requests.map((request) => (
          <li
            key={request.id}
            className="rounded-xl border border-line/70 bg-void/50 p-3"
          >
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <p className="truncate font-mono text-[12px] font-semibold text-zinc-100">
                  {peerLabel(request.sender_name, request.sender)}
                </p>
                <p className="mt-1 line-clamp-2 break-words text-[13px] leading-relaxed text-zinc-200">
                  {request.text}
                </p>
              </div>
              <div className="flex shrink-0 gap-2">
                <button
                  type="button"
                  disabled={acceptingId !== null || decliningId !== null}
                  onClick={() => void confirmAccept(request.id)}
                  className="rounded-md border border-line/80 bg-panel/60 px-2 py-1 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50"
                >
                  {acceptingId === request.id ? "accepting…" : "accept"}
                </button>
                <button
                  type="button"
                  disabled={acceptingId !== null || decliningId !== null}
                  onClick={() => {
                    if (confirmingId === request.id) {
                      void confirmDecline(request.id, false);
                    } else {
                      armConfirm(request.id);
                    }
                  }}
                  className={`rounded-md border px-2 py-1 font-mono text-[11px] transition focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50 ${
                    confirmingId === request.id
                      ? "border-amber/60 bg-amber/10 text-amber"
                      : "border-line/80 bg-panel/60 text-mist hover:border-rose-400/50 hover:text-rose-300"
                  }`}
                >
                  {decliningId === request.id
                    ? "declining…"
                    : confirmingId === request.id
                      ? "confirm decline"
                      : "decline"}
                </button>
                <button
                  type="button"
                  disabled={acceptingId !== null || decliningId !== null}
                  onClick={() => {
                    if (confirmingBlockId === request.id) {
                      void confirmDecline(request.id, true);
                    } else {
                      armBlockConfirm(request.id);
                    }
                  }}
                  className={`rounded-md border px-2 py-1 font-mono text-[11px] transition focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50 ${
                    confirmingBlockId === request.id
                      ? "border-amber/60 bg-amber/10 text-amber"
                      : "border-line/80 bg-panel/60 text-mist hover:border-rose-400/50 hover:text-rose-300"
                  }`}
                >
                  {decliningId === request.id
                    ? "declining…"
                    : confirmingBlockId === request.id
                      ? "confirm decline & block"
                      : "decline & block"}
                </button>
              </div>
            </div>

            <p className="mt-2 font-mono text-[10px] leading-relaxed text-mist/60">
              {formatAge(now - request.received_at)} · held until{" "}
              {formatDateTime(request.request_expires_at)}
              {request.expires_at
                ? ` · message expires ${formatDateTime(request.expires_at)}`
                : ""}
            </p>
          </li>
        ))}
      </ul>
    </section>
  );
}
