"use client";

import { useState } from "react";

import type { NodeSettings } from "@/lib/types";

import { SectionLabel } from "./ui";

const ROWS: {
  key: keyof NodeSettings;
  label: string;
  description: string;
}[] = [
  {
    key: "accept_files",
    label: "accept file transfers",
    description:
      "off by default — an unsolicited file is never written to disk unless you turn this on.",
  },
  {
    key: "send_read_receipts",
    label: "send read receipts",
    description:
      "on by default — peers see when you have read their messages. Turning it off sends nothing.",
  },
];

/** This node's own settings (`tau config`), changeable without a restart. */
export function SettingsSection({
  settings,
  loadError = null,
  onToggle,
}: {
  settings: NodeSettings | null;
  loadError?: string | null;
  onToggle: (setting: keyof NodeSettings, value: boolean) => Promise<unknown>;
}) {
  const [pendingKey, setPendingKey] = useState<keyof NodeSettings | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function toggle(key: keyof NodeSettings) {
    if (settings === null || pendingKey !== null) return;
    setPendingKey(key);
    setError(null);
    try {
      await onToggle(key, !settings[key]);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "setting failed");
    } finally {
      setPendingKey(null);
    }
  }

  return (
    <section className="rounded-2xl border border-line/80 bg-panel/60 p-4 shadow-[0_20px_60px_-40px_rgba(0,0,0,0.9)]">
      <div className="flex items-center justify-between">
        <SectionLabel>settings</SectionLabel>
        <span className="font-mono text-[10px] text-mist/60">tau config</span>
      </div>

      <p className="mt-2 font-mono text-[11px] leading-relaxed text-mist/80">
        changes apply to the running node at once — no restart.
      </p>

      {loadError ? (
        <p className="mt-2 font-mono text-[11px] text-rose-400">
          couldn&apos;t load settings: {loadError}
        </p>
      ) : null}

      {error ? (
        <p className="mt-2 font-mono text-[11px] text-rose-400">{error}</p>
      ) : null}

      <ul className="mt-3 flex flex-col gap-3">
        {ROWS.map((row) => {
          const on = settings?.[row.key] ?? false;
          return (
            <li
              key={row.key}
              className="flex items-start justify-between gap-3 rounded-xl border border-line/70 bg-void/50 p-3"
            >
              <div className="min-w-0">
                <p className="font-mono text-[12px] font-semibold text-zinc-100">
                  {row.label}
                </p>
                <p className="mt-1 text-[12px] leading-relaxed text-mist/80">
                  {row.description}
                </p>
              </div>
              <button
                type="button"
                role="switch"
                aria-checked={on}
                aria-label={row.label}
                disabled={settings === null || pendingKey !== null}
                onClick={() => {
                  void toggle(row.key);
                }}
                className={`shrink-0 rounded-full border px-2.5 py-1 font-mono text-[11px] transition focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50 ${
                  on
                    ? "border-signal/60 bg-signal/10 text-signal"
                    : "border-line/80 bg-panel/60 text-mist hover:border-mist/50"
                }`}
              >
                {pendingKey === row.key ? "saving…" : on ? "on" : "off"}
              </button>
            </li>
          );
        })}
      </ul>
    </section>
  );
}
