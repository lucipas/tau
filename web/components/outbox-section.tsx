"use client";

import { useEffect, useRef, useState } from "react";

import { formatAge, formatDateTime, peerLabel } from "@/lib/format";
import type { HeldMailEntry, OutboxEntry } from "@/lib/types";
import { useNow } from "@/lib/use-now";

import { SectionLabel } from "./ui";

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  return `${(bytes / 1024).toFixed(1)} KiB`;
}

/** Queued mail waiting on offline peers, with a way to take one back. */
export function OutboxSection({
  entries,
  heldForOthers,
  held,
  onCancel,
  onDropHeld,
}: {
  entries: OutboxEntry[];
  heldForOthers: number;
  held: HeldMailEntry[];
  onCancel: (id: string) => Promise<unknown>;
  onDropHeld: (id: string) => Promise<unknown>;
}) {
  const nowMs = useNow(30000);
  const now = (nowMs ?? Date.now()) / 1000;
  const [confirmingId, setConfirmingId] = useState<string | null>(null);
  const [cancellingId, setCancellingId] = useState<string | null>(null);
  const [confirmingHeldId, setConfirmingHeldId] = useState<string | null>(
    null,
  );
  const [droppingHeldId, setDroppingHeldId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const confirmTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const heldConfirmTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (confirmTimer.current) clearTimeout(confirmTimer.current);
      if (heldConfirmTimer.current) clearTimeout(heldConfirmTimer.current);
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

  function armHeldConfirm(id: string) {
    setError(null);
    setConfirmingHeldId(id);
    if (heldConfirmTimer.current) clearTimeout(heldConfirmTimer.current);
    heldConfirmTimer.current = setTimeout(
      () => setConfirmingHeldId(null),
      5000,
    );
  }

  async function confirmDropHeld(id: string) {
    if (heldConfirmTimer.current) clearTimeout(heldConfirmTimer.current);
    setConfirmingHeldId(null);
    setDroppingHeldId(id);
    setError(null);
    try {
      await onDropHeld(id);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "drop failed");
    } finally {
      setDroppingHeldId(null);
    }
  }

  return (
    <section className="rounded-2xl border border-line/80 bg-panel/60 p-4 shadow-[0_20px_60px_-40px_rgba(0,0,0,0.9)]">
      <div className="flex items-center justify-between">
        <SectionLabel>outbox</SectionLabel>
        <span className="inline-flex items-center gap-1.5 rounded-full border border-amber/25 bg-amber/5 px-2 py-0.5 font-mono text-[10px] text-amber">
          {entries.length} queued
        </span>
      </div>

      <p className="mt-2 font-mono text-[11px] leading-relaxed text-mist/80">
        waiting on offline peers — delivery is retried automatically.
      </p>

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
                className={`shrink-0 rounded-md border px-2 py-1 font-mono text-[11px] transition focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50 ${
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

      {held.length > 0 ? (
        <>
          <p className="mt-4 font-mono text-[10px] uppercase tracking-[0.18em] text-mist/70">
            held for others · {held.length} sealed{" "}
            {held.length === 1 ? "message" : "messages"}
          </p>
          <p className="mt-1 font-mono text-[11px] leading-relaxed text-mist/80">
            sealed copies waiting for their recipients to fetch them —
            contents stay unreadable here.
          </p>
          <ul className="mt-3 flex flex-col gap-3">
            {held.map((entry) => (
              <li
                key={entry.id}
                className="rounded-xl border border-line/70 bg-void/50 p-3"
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <p className="truncate font-mono text-[12px] font-semibold text-zinc-100">
                      {peerLabel(entry.to_name, entry.to)}
                    </p>
                    <p className="mt-1 truncate font-mono text-[11px] text-mist">
                      from {peerLabel(entry.from_name, entry.from || "unknown")}
                    </p>
                  </div>
                  <button
                    type="button"
                    disabled={droppingHeldId === entry.id}
                    onClick={() => {
                      if (confirmingHeldId === entry.id) {
                        void confirmDropHeld(entry.id);
                      } else {
                        armHeldConfirm(entry.id);
                      }
                    }}
                    className={`shrink-0 rounded-md border px-2 py-1 font-mono text-[11px] transition focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50 ${
                      confirmingHeldId === entry.id
                        ? "border-amber/60 bg-amber/10 text-amber"
                        : "border-line/80 bg-panel/60 text-mist hover:border-rose-400/50 hover:text-rose-300"
                    }`}
                  >
                    {droppingHeldId === entry.id
                      ? "dropping…"
                      : confirmingHeldId === entry.id
                        ? "confirm drop"
                        : "drop"}
                  </button>
                </div>

                <p className="mt-2 font-mono text-[10px] leading-relaxed text-mist/60">
                  held {formatAge(now - entry.stored_at)} · expires{" "}
                  {formatDateTime(entry.expires_at)} ·{" "}
                  {formatBytes(entry.blob_bytes)} sealed
                </p>
              </li>
            ))}
          </ul>
        </>
      ) : null}

      {held.length === 0 && heldForOthers > 0 ? (
        <p className="mt-3 font-mono text-[10px] text-mist/60">
          also holding {heldForOthers} sealed{" "}
          {heldForOthers === 1 ? "message" : "messages"} for other peers.
        </p>
      ) : null}
    </section>
  );
}
