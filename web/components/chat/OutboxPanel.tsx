// OutboxPanel.tsx — queued / held store-and-forward mail with cancel,
// retry, and drop-held actions (useOutbox()).

"use client";

import { formatCountdown, formatDateTime } from "@/lib/format";
import { useNow } from "@/lib/use-now";
import type { HeldEnvelope, Outbox } from "@/lib/types";

export interface OutboxPanelProps {
  outbox: Outbox | null;
  held: HeldEnvelope[];
  loading?: boolean;
  error?: string | null;
  onCancel: (target: string) => void;
  onRetry: (target?: string) => void;
  onDropHeld: (target: string) => void;
}

export default function OutboxPanel({
  outbox,
  held,
  loading = false,
  error,
  onCancel,
  onRetry,
  onDropHeld,
}: OutboxPanelProps) {
  const now = useNow();
  const entries = outbox?.outbox ?? [];
  const count = outbox?.count ?? entries.length;

  return (
    <section aria-label="Outbox" className="border-b border-zinc-800 px-3 py-2">
      <div className="flex items-center justify-between">
        <h2 className="text-xs font-bold uppercase tracking-wide text-zinc-500">
          Outbox {count > 0 ? `(${count})` : ""}
        </h2>
        {count > 0 && (
          <button
            type="button"
            onClick={() => onRetry()}
            className="text-xs font-semibold text-cyan-400 hover:text-cyan-300"
          >
            Retry all
          </button>
        )}
      </div>

      {loading && <p className="py-2 text-sm text-zinc-500">Loading outbox…</p>}
      {error && (
        <p role="alert" className="py-1 text-sm text-red-400">
          Outbox unavailable: {error}
        </p>
      )}
      {!loading && !error && count === 0 && held.length === 0 && (
        <p className="py-1 text-sm text-zinc-500">Nothing queued.</p>
      )}

      {entries.length > 0 && (
        <ul className="mt-1 flex flex-col gap-1.5">
          {entries.map((entry) => (
            <li key={entry.id} className="rounded-lg bg-zinc-900 px-2 py-1.5 text-sm">
              <div className="flex items-center justify-between gap-2">
                <span className="min-w-0 flex-1 truncate font-medium text-zinc-200">
                  → {entry.to_name || entry.to.slice(0, 8)}
                </span>
                <span className="shrink-0 text-xs text-zinc-500">
                  {entry.attempts > 0 ? `${entry.attempts} attempt${entry.attempts === 1 ? "" : "s"}` : "queued"}
                </span>
              </div>
              <p className="truncate text-xs text-zinc-400">{entry.text}</p>
              {entry.last_error && <p className="truncate text-xs text-red-400/90">{entry.last_error}</p>}
              <p className="text-xs text-zinc-500">
                {entry.next_attempt_at ? `Next try ${formatCountdown(entry.next_attempt_at, now)}` : ""}
                {entry.send_after ? ` · sends ${formatCountdown(entry.send_after, now)}` : ""}
                {` · queued ${formatDateTime(entry.created_at)}`}
              </p>
              <div className="mt-1 flex gap-2">
                <button
                  type="button"
                  onClick={() => onRetry(entry.to)}
                  className="rounded-full border border-zinc-700 px-2.5 py-0.5 text-xs text-zinc-200 hover:bg-zinc-800"
                >
                  Retry
                </button>
                <button
                  type="button"
                  onClick={() => onCancel(entry.to)}
                  className="rounded-full border border-zinc-700 px-2.5 py-0.5 text-xs text-zinc-200 hover:bg-zinc-800"
                >
                  Cancel
                </button>
              </div>
            </li>
          ))}
        </ul>
      )}

      {held.length > 0 && (
        <div className="mt-2">
          <h3 className="text-xs font-bold uppercase tracking-wide text-zinc-500">
            Held for others ({held.length})
          </h3>
          <ul className="mt-1 flex flex-col gap-1.5">
            {held.map((env) => (
              <li key={env.id} className="flex items-center justify-between gap-2 rounded-lg bg-zinc-900 px-2 py-1.5 text-sm">
                <span className="min-w-0 flex-1 truncate text-zinc-300">
                  {env.from_name || env.from.slice(0, 8)} → {env.to_name || env.to.slice(0, 8)}
                  <span className="block text-xs text-zinc-500">
                    {env.blob_bytes} B · expires {formatCountdown(env.expires_at, now)}
                  </span>
                </span>
                <button
                  type="button"
                  onClick={() => onDropHeld(env.to)}
                  className="shrink-0 rounded-full border border-zinc-700 px-2.5 py-0.5 text-xs text-zinc-200 hover:bg-zinc-800"
                >
                  Drop
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}
    </section>
  );
}
