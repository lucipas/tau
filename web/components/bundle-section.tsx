"use client";

import { useRef, useState } from "react";

import type {
  BundleExportResponse,
  BundleImportReport,
  BundleImportResponse,
  BundleSummary,
} from "@/lib/types";

import { SectionLabel } from "./ui";

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
}

// The server returns the bundle as its serialised text; download that
// text verbatim. Re-parsing and re-stringifying it here could rewrite
// a number the signature covers (an integral float such as
// 1700000000.0 becomes 1700000000) and break the file on import.
function downloadBundle(bundleText: string) {
  const stamp = new Date().toISOString().slice(0, 10);
  const blob = new Blob([bundleText], {
    type: "application/json",
  });
  const url = URL.createObjectURL(blob);
  const anchor = window.document.createElement("a");
  anchor.href = url;
  anchor.download = `tau-bundle-${stamp}.json`;
  anchor.click();
  URL.revokeObjectURL(url);
}

function plural(count: number, singular: string, pluralForm?: string): string {
  return count === 1 ? singular : (pluralForm ?? `${singular}s`);
}

/** Carry sealed mail (and optionally Tan posts) on a file, no live path needed. */
export function BundleSection({
  onExport,
  onImport,
}: {
  onExport: (includeTan: boolean) => Promise<BundleExportResponse>;
  onImport: (documentText: string) => Promise<BundleImportResponse>;
}) {
  const [includeTan, setIncludeTan] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [importing, setImporting] = useState(false);
  const [exportSummary, setExportSummary] = useState<BundleSummary | null>(null);
  const [importReport, setImportReport] = useState<BundleImportReport | null>(
    null,
  );
  const [error, setError] = useState<string | null>(null);
  const fileInput = useRef<HTMLInputElement | null>(null);

  async function handleExport() {
    setExporting(true);
    setError(null);
    setExportSummary(null);
    try {
      const result = await onExport(includeTan);
      downloadBundle(result.bundle);
      setExportSummary(result.summary);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "export failed");
    } finally {
      setExporting(false);
    }
  }

  async function handleFilePicked(file: File) {
    setImporting(true);
    setError(null);
    setImportReport(null);
    try {
      // The file's text goes to the server verbatim: the bundle is
      // signed, and a JSON parse/stringify round trip here can rewrite
      // a number the signature covers. The server reports a file that
      // is not a valid bundle.
      const result = await onImport(await file.text());
      setImportReport(result.import);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "import failed");
    } finally {
      setImporting(false);
      if (fileInput.current) fileInput.current.value = "";
    }
  }

  return (
    <section className="rounded-2xl border border-line/80 bg-panel/60 p-4 shadow-[0_20px_60px_-40px_rgba(0,0,0,0.9)]">
      <div className="flex items-center justify-between">
        <SectionLabel>sneakernet</SectionLabel>
        <span className="inline-flex items-center gap-1.5 rounded-full border border-line/80 bg-panel/60 px-2 py-0.5 font-mono text-[10px] text-mist">
          carry mail on a file
        </span>
      </div>

      <p className="mt-2 font-mono text-[11px] leading-relaxed text-mist/80">
        when two nodes share no network at all, mail still travels — export
        a signed bundle, carry the file over, and import it there.
      </p>

      {error ? (
        <p className="mt-2 font-mono text-[11px] text-rose-400">{error}</p>
      ) : null}

      <div className="mt-3 flex flex-col gap-3">
        <div className="rounded-xl border border-line/70 bg-void/50 p-3">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <p className="font-mono text-[12px] font-semibold text-zinc-100">
                export bundle
              </p>
              <label className="mt-1.5 flex cursor-pointer items-center gap-2 font-mono text-[11px] text-mist/80">
                <input
                  type="checkbox"
                  checked={includeTan}
                  onChange={(event) => setIncludeTan(event.target.checked)}
                  className="accent-teal-400"
                />
                include recent Tan posts
              </label>
            </div>
            <button
              type="button"
              disabled={exporting}
              onClick={() => void handleExport()}
              className="shrink-0 rounded-md border border-line/80 bg-panel/60 px-2 py-1 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50"
            >
              {exporting ? "exporting…" : "download bundle"}
            </button>
          </div>
          {exportSummary ? (
            <div className="mt-2">
              <p className="font-mono text-[10px] leading-relaxed text-mist/60">
                {exportSummary.mail} sealed{" "}
                {plural(exportSummary.mail, "message")} ·{" "}
                {exportSummary.tan} Tan{" "}
                {plural(exportSummary.tan, "post")} ·{" "}
                {formatBytes(exportSummary.bytes)}
              </p>
              {exportSummary.skipped.length > 0 ? (
                <ul className="mt-1 flex flex-col gap-0.5">
                  {exportSummary.skipped.map((entry) => (
                    <li
                      key={entry.id}
                      className="font-mono text-[10px] leading-relaxed text-amber/80"
                    >
                      skipped {entry.id}: {entry.reason}
                    </li>
                  ))}
                </ul>
              ) : null}
            </div>
          ) : null}
        </div>

        <div className="rounded-xl border border-line/70 bg-void/50 p-3">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <p className="font-mono text-[12px] font-semibold text-zinc-100">
              import bundle
            </p>
            <label
              htmlFor="bundle-file"
              className={`shrink-0 cursor-pointer rounded-md border border-line/80 bg-panel/60 px-2 py-1 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 ${
                importing ? "pointer-events-none opacity-50" : ""
              }`}
            >
              {importing ? "importing…" : "choose bundle file"}
            </label>
            <input
              id="bundle-file"
              ref={fileInput}
              type="file"
              accept=".json,application/json"
              className="hidden"
              disabled={importing}
              onChange={(event) => {
                const file = event.target.files?.[0];
                if (file) void handleFilePicked(file);
              }}
            />
          </div>
          {importReport ? (
            <div className="mt-2">
              <p className="font-mono text-[10px] leading-relaxed text-mist/60">
                imported {importReport.imported} · already held{" "}
                {importReport.present} · skipped {importReport.skipped}
                {importReport.tan
                  ? ` · Tan: ${importReport.tan.imported} imported, ${importReport.tan.present} already present, ${importReport.tan.skipped} skipped`
                  : ""}
              </p>
              {importReport.errors.length > 0 ? (
                <ul className="mt-1 flex flex-col gap-0.5">
                  {importReport.errors.map((entry, index) => (
                    <li
                      key={`${entry.id}-${index}`}
                      className="font-mono text-[10px] leading-relaxed text-amber/80"
                    >
                      skipped {entry.id || "?"}: {entry.error}
                    </li>
                  ))}
                </ul>
              ) : null}
              {importReport.tan && importReport.tan.errors.length > 0 ? (
                <ul className="mt-1 flex flex-col gap-0.5">
                  {importReport.tan.errors.map((entry, index) => (
                    <li
                      key={`${entry.hash}-${index}`}
                      className="font-mono text-[10px] leading-relaxed text-amber/80"
                    >
                      Tan skipped {entry.hash || "?"}: {entry.error}
                    </li>
                  ))}
                </ul>
              ) : null}
            </div>
          ) : null}
        </div>
      </div>
    </section>
  );
}
