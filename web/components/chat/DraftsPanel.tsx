// DraftsPanel.tsx — saved message drafts for the chat sidebar: one row
// per draft with the conversation name, a text preview and the save time.
// Clicking a row opens that conversation with the draft loaded into the
// composer; each row also has a delete button. Hidden entirely when there
// are no drafts to show.

"use client";

import { formatRelative, truncatedHash } from "@/lib/format";
import type { MessageDraft } from "@/lib/types";

export interface DraftsPanelProps {
  drafts: MessageDraft[];
  loading?: boolean;
  onOpen: (draft: MessageDraft) => void;
  onDelete: (draft: MessageDraft) => void;
}

export default function DraftsPanel({ drafts, loading = false, onOpen, onDelete }: DraftsPanelProps) {
  if (!loading && drafts.length === 0) return null;
  return (
    <section aria-label="Drafts" className="border-b border-zinc-800 px-3 py-2">
      <h2 className="text-xs font-bold uppercase tracking-wide text-zinc-500">
        Drafts {drafts.length > 0 ? `(${drafts.length})` : ""}
      </h2>
      {loading && <p className="py-2 text-sm text-zinc-500">Loading drafts…</p>}
      {drafts.length > 0 && (
        <ul className="mt-1 flex flex-col gap-1.5">
          {drafts.map((draft) => {
            const label = draft.name ?? truncatedHash(draft.target);
            const firstLine = draft.text.split("\n")[0];
            return (
              <li
                key={`${draft.kind}:${draft.target}`}
                className="rounded-lg bg-zinc-900 px-2 py-1.5 text-sm"
              >
                <div className="flex items-center justify-between gap-2">
                  <button
                    type="button"
                    onClick={() => onOpen(draft)}
                    className="min-w-0 flex-1 truncate text-left font-medium text-zinc-200 hover:text-cyan-300"
                    title={`Open draft for ${label}`}
                  >
                    {draft.kind === "group" || draft.kind === "channel" ? "# " : ""}
                    {label}
                  </button>
                  <span className="shrink-0 text-xs text-zinc-500">
                    {formatRelative(draft.updated_at)}
                  </span>
                </div>
                <p className="truncate text-xs text-zinc-400">{firstLine}</p>
                <button
                  type="button"
                  onClick={() => onDelete(draft)}
                  aria-label={`Delete draft for ${label}`}
                  className="mt-1 rounded-full border border-zinc-700 px-2.5 py-0.5 text-xs text-zinc-200 hover:bg-zinc-800"
                >
                  Delete
                </button>
              </li>
            );
          })}
        </ul>
      )}
    </section>
  );
}
