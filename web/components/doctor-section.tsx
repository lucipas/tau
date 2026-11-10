"use client";

import { useState } from "react";

import type { DoctorCheck, DoctorResponse } from "@/lib/types";

import { SectionLabel } from "./ui";

function statusClass(status: DoctorCheck["status"]): string {
  if (status === "fail") {
    return "border-rose-400/25 bg-rose-400/10 text-rose-300";
  }
  if (status === "warn") {
    return "border-amber/25 bg-amber/5 text-amber";
  }
  return "border-signal/25 bg-signal/5 text-signal";
}

/** Read-only health verdicts for this node's data directory (`tau doctor`). */
export function DoctorSection({
  onRun,
}: {
  onRun: () => Promise<DoctorResponse>;
}) {
  const [report, setReport] = useState<DoctorResponse | null>(null);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const failed =
    report?.checks.filter((check) => check.status === "fail").length ?? 0;

  async function run() {
    setRunning(true);
    setError(null);
    try {
      setReport(await onRun());
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "health check failed");
    } finally {
      setRunning(false);
    }
  }

  return (
    <section className="rounded-2xl border border-line/80 bg-panel/60 p-4 shadow-[0_20px_60px_-40px_rgba(0,0,0,0.9)]">
      <div className="flex items-center justify-between">
        <SectionLabel>health</SectionLabel>
        {report ? (
          <span
            className={`inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 font-mono text-[10px] ${statusClass(report.ok ? "ok" : "fail")}`}
          >
            {report.ok
              ? "healthy"
              : `${failed} check${failed === 1 ? "" : "s"} failed`}
          </span>
        ) : null}
      </div>

      <p className="mt-2 font-mono text-[11px] leading-relaxed text-mist/80">
        read-only check of the data directory — the same verdicts as{" "}
        <code className="text-mist">tau doctor</code>. Nothing is changed or
        locked.
      </p>

      {error ? (
        <p className="mt-2 font-mono text-[11px] text-rose-400">{error}</p>
      ) : null}

      {report ? (
        <ul className="mt-3 flex flex-col gap-2">
          {report.checks.map((check) => (
            <li
              key={check.name}
              className="flex items-start justify-between gap-3 rounded-xl border border-line/70 bg-void/50 px-3 py-2"
            >
              <div className="min-w-0">
                <p className="font-mono text-[12px] font-semibold text-zinc-100">
                  {check.name}
                </p>
                <p className="mt-0.5 break-words font-mono text-[11px] leading-relaxed text-mist/80">
                  {check.detail}
                </p>
              </div>
              <span
                className={`inline-flex shrink-0 items-center rounded-full border px-2 py-0.5 font-mono text-[10px] ${statusClass(check.status)}`}
              >
                {check.status}
              </span>
            </li>
          ))}
        </ul>
      ) : null}

      <button
        type="button"
        onClick={() => void run()}
        disabled={running}
        className="mt-3 rounded-xl border border-line/80 bg-void/60 px-3 py-2 font-mono text-[12px] text-zinc-200 transition hover:border-signal/40 hover:text-zinc-100 disabled:opacity-50"
      >
        {running ? "checking…" : report ? "run again" : "run health check"}
      </button>
    </section>
  );
}
