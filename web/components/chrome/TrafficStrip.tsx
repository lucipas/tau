// TrafficStrip.tsx — compact live traffic summary derived from message
// history via useTraffic() (there is no /api/traffic endpoint).

"use client";

import { formatRelative } from "@/lib/format";
import type { TrafficRow } from "@/lib/use-tau";
import Avatar from "./Avatar";

export default function TrafficStrip({ rows, now }: { rows: TrafficRow[]; now: number }) {
  const total = rows.reduce((sum, r) => sum + r.sent + r.received, 0);
  return (
    <section aria-label="Recent traffic" className="rounded-2xl border border-zinc-800 bg-zinc-900/60 p-3">
      <div className="mb-2 flex items-baseline justify-between">
        <h2 className="text-sm font-bold text-zinc-100">Traffic</h2>
        <span className="text-xs text-zinc-500">
          {total} msg{total === 1 ? "" : "s"} recent
        </span>
      </div>
      {rows.length === 0 ? (
        <p className="text-sm text-zinc-500">No traffic in the recent window.</p>
      ) : (
        <ul className="flex flex-col gap-2">
          {rows.slice(0, 5).map((row) => (
            <li key={row.destination} className="flex items-center gap-2 text-sm">
              <Avatar name={row.name} seed={row.destination} size="sm" />
              <span className="min-w-0 flex-1 truncate text-zinc-200">{row.name}</span>
              <span className="shrink-0 text-xs text-zinc-500" title={`${row.sent} sent · ${row.received} received${row.files ? ` · ${row.files} files` : ""}`}>
                ↑{row.sent} ↓{row.received}
                {row.files > 0 ? ` · 📎${row.files}` : ""}
              </span>
              <span className="w-10 shrink-0 text-right text-xs text-zinc-500">
                {row.lastAt ? formatRelative(row.lastAt, now) : ""}
              </span>
            </li>
          ))}
        </ul>
      )}
      <p className="mt-2 text-[11px] leading-snug text-zinc-600">
        Derived from stored message history — no traffic endpoint exists.
      </p>
    </section>
  );
}
