// RemindersPanel.tsx — pending message reminders for the chat sidebar: one
// row per reminder with the conversation name, a text preview and the due
// time. Clicking a row opens that conversation; each row also has a cancel
// button. Hidden entirely when there are no reminders to show.

"use client";

import { formatCountdown, truncatedHash } from "@/lib/format";
import type { Reminder } from "@/lib/types";

export interface RemindersPanelProps {
  reminders: Reminder[];
  loading?: boolean;
  onOpen: (reminder: Reminder) => void;
  onCancel: (reminder: Reminder) => void;
}

export default function RemindersPanel({ reminders, loading = false, onOpen, onCancel }: RemindersPanelProps) {
  if (!loading && reminders.length === 0) return null;
  return (
    <section aria-label="Reminders" className="border-b border-zinc-800 px-3 py-2">
      <h2 className="text-xs font-bold uppercase tracking-wide text-zinc-500">
        Reminders {reminders.length > 0 ? `(${reminders.length})` : ""}
      </h2>
      {loading && <p className="py-2 text-sm text-zinc-500">Loading reminders…</p>}
      {reminders.length > 0 && (
        <ul className="mt-1 flex flex-col gap-1.5">
          {reminders.map((reminder) => {
            const label = reminder.target_name ?? truncatedHash(reminder.target);
            const firstLine = reminder.text.split("\n")[0];
            return (
              <li
                key={reminder.id}
                className="rounded-lg bg-zinc-900 px-2 py-1.5 text-sm"
              >
                <div className="flex items-center justify-between gap-2">
                  <button
                    type="button"
                    onClick={() => onOpen(reminder)}
                    className="min-w-0 flex-1 truncate text-left font-medium text-zinc-200 hover:text-cyan-300"
                    title={`Open reminder for ${label}`}
                  >
                    {reminder.kind === "group" ? "# " : ""}
                    {label}
                  </button>
                  <span className="shrink-0 text-xs text-zinc-500">
                    {formatCountdown(reminder.due_at)}
                  </span>
                </div>
                <p className="truncate text-xs text-zinc-400">{firstLine}</p>
                <p className="truncate text-xs text-zinc-500">from {reminder.sender}</p>
                <button
                  type="button"
                  onClick={() => onCancel(reminder)}
                  aria-label={`Cancel reminder for ${label}`}
                  className="mt-1 rounded-full border border-zinc-700 px-2.5 py-0.5 text-xs text-zinc-200 hover:bg-zinc-800"
                >
                  Cancel
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}
