// OutboxPanel.tsx — queued / held store-and-forward mail with cancel,
// retry, and drop-held actions (useOutbox()).

"use client";

import { useState } from "react";
import { formatCountdown, formatDateTime } from "@/lib/format";
import { useNow } from "@/lib/use-now";
import type { HeldEnvelope, Outbox, OutboxEntry } from "@/lib/types";

const EDIT_DELAY_OPTIONS: { label: string; seconds: number | undefined }[] = [
  { label: "Keep time", seconds: undefined },
  { label: "In 5 minutes", seconds: 5 * 60 },
  { label: "In 1 hour", seconds: 3600 },
  { label: "Tomorrow", seconds: 86400 },
];

export interface OutboxPanelProps {
  outbox: Outbox | null;
  held: HeldEnvelope[];
  loading?: boolean;
  error?: string | null;
  onCancel: (target: string) => void;
  onEdit: (
    target: string,
    patch: { text?: string; send_in?: number },
  ) => void;
  onRetry: (target?: string) => void;
  onDropHeld: (target: string) => void;
}

export default function OutboxPanel({
  outbox,
  held,
  loading = false,
  error,
  onCancel,
  onEdit,
  onRetry,
  onDropHeld,
}: OutboxPanelProps) {
  const now = useNow();
  const entries = outbox?.outbox ?? [];
  const count = outbox?.count ?? entries.length;
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editText, setEditText] = useState("");
  const [editDelay, setEditDelay] = useState<number | undefined>(undefined);

  function startEditing(entry: OutboxEntry) {
    setEditingId(entry.id);
    setEditText(entry.text);
    setEditDelay(undefined);
  }

  function saveEdit(entry: OutboxEntry) {
    const trimmed = editText.trim();
    if (!trimmed) return;
    const patch: { text?: string; send_in?: number } = {};
    if (trimmed !== entry.text) patch.text = trimmed;
    if (editDelay !== undefined) patch.send_in = editDelay;
    // Unchanged save makes no request.
    if (patch.text === undefined && patch.send_in === undefined) {
      setEditingId(null);
      return;
    }
    onEdit(entry.id, patch);
    setEditingId(null);
  }

  // Only a still-scheduled plain message can be edited: poll, vote,
  // close and beacon legs carry no plain text, and a due or
  // retry-queued entry is no longer waiting on its send time.
  function isEditable(entry: OutboxEntry) {
    return (
      entry.send_after != null &&
      entry.poll_options == null &&
      entry.poll_vote == null &&
      entry.poll_close == null &&
      entry.beacon == null
    );
  }

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
              {editingId === entry.id ? (
                <div className="mt-1 flex flex-col gap-1.5">
                  <input
                    type="text"
                    value={editText}
                    onChange={(event) => setEditText(event.target.value)}
                    aria-label="Edit scheduled message text"
                    className="w-full rounded-lg border border-zinc-700 bg-zinc-800 px-2 py-1 text-sm text-zinc-100"
                  />
                  <div className="flex items-center gap-2">
                    <select
                      value={editDelay === undefined ? "" : String(editDelay)}
                      onChange={(event) =>
                        setEditDelay(
                          event.target.value === ""
                            ? undefined
                            : Number(event.target.value),
                        )
                      }
                      aria-label="New send time"
                      className="rounded-lg border border-zinc-700 bg-zinc-800 px-2 py-1 text-xs text-zinc-200"
                    >
                      {EDIT_DELAY_OPTIONS.map((option) => (
                        <option
                          key={option.label}
                          value={option.seconds === undefined ? "" : String(option.seconds)}
                        >
                          {option.label}
                        </option>
                      ))}
                    </select>
                    <button
                      type="button"
                      onClick={() => saveEdit(entry)}
                      disabled={
                        !editText.trim() ||
                        (editText.trim() === entry.text &&
                          editDelay === undefined)
                      }
                      className="rounded-full bg-cyan-500 px-2.5 py-0.5 text-xs font-semibold text-white hover:bg-cyan-400 disabled:opacity-50"
                    >
                      Save
                    </button>
                    <button
                      type="button"
                      onClick={() => setEditingId(null)}
                      className="rounded-full border border-zinc-700 px-2.5 py-0.5 text-xs text-zinc-200 hover:bg-zinc-800"
                    >
                      Cancel
                    </button>
                  </div>
                </div>
              ) : (
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
                  {isEditable(entry) && (
                    <button
                      type="button"
                      onClick={() => startEditing(entry)}
                      aria-label="Edit scheduled message"
                      className="rounded-full border border-zinc-700 px-2.5 py-0.5 text-xs text-zinc-200 hover:bg-zinc-800"
                    >
                      Edit
                    </button>
                  )}
                </div>
              )}
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
