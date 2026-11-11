"use client";

import { useRef, useState } from "react";

import { shortHash } from "@/lib/format";
import type {
  MessagesExportDocument,
  MessagesImportError,
  MessagesImportResult,
} from "@/lib/types";

import { SectionLabel } from "./ui";

export function MessagesBackupSection({
  exporting,
  importing,
  onExport,
  onImport,
}: {
  exporting: boolean;
  importing: boolean;
  onExport: () => Promise<MessagesExportDocument>;
  onImport: (doc: MessagesExportDocument) => Promise<MessagesImportResult>;
}) {
  const [message, setMessage] = useState<string | null>(null);
  const [errors, setErrors] = useState<MessagesImportError[]>([]);
  const [error, setError] = useState<string | null>(null);
  const importInputRef = useRef<HTMLInputElement>(null);

  async function submitExport() {
    setError(null);
    setMessage(null);
    setErrors([]);
    try {
      const doc = await onExport();
      setMessage(
        `exported ${doc.messages.length} message${doc.messages.length === 1 ? "" : "s"}`,
      );
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "export failed");
    }
  }

  async function submitImportFile(file: File) {
    setError(null);
    setMessage(null);
    setErrors([]);
    try {
      let doc: unknown;
      try {
        doc = JSON.parse(await file.text());
      } catch {
        setError("that file is not a valid export document");
        return;
      }
      try {
        const result = await onImport(doc as MessagesExportDocument);
        setMessage(
          `imported ${result.imported} · ${result.present} already present · ${result.skipped} skipped`,
        );
        setErrors(result.errors);
      } catch (caught) {
        setError(caught instanceof Error ? caught.message : "import failed");
      }
    } finally {
      // Picking the same file again must fire onChange, including
      // after a parse failure that returns before the import runs.
      if (importInputRef.current) importInputRef.current.value = "";
    }
  }

  return (
    <section className="rounded-2xl border border-line/80 bg-panel/40 p-4">
      <SectionLabel>backup</SectionLabel>
      <p className="mt-2 text-[11px] leading-relaxed text-mist">
        Back up your stored DMs as a file, or restore one. Restored
        messages show up in the feed right away.
      </p>
      <div className="mt-3 flex gap-2">
        <button
          type="button"
          onClick={() => void submitExport()}
          disabled={exporting}
          className="flex-1 rounded-lg border border-line bg-void/70 px-2.5 py-2 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal disabled:opacity-50"
        >
          {exporting ? "exporting…" : "export"}
        </button>
        <label
          htmlFor="messages-import-file"
          className={`flex-1 cursor-pointer rounded-lg border border-line bg-void/70 px-2.5 py-2 text-center font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal ${importing ? "pointer-events-none opacity-50" : ""}`}
        >
          {importing ? "importing…" : "import"}
        </label>
        <input
          ref={importInputRef}
          id="messages-import-file"
          type="file"
          accept="application/json,.json"
          className="hidden"
          onChange={(event) => {
            const file = event.target.files?.[0];
            if (file) void submitImportFile(file);
          }}
        />
      </div>
      {message ? (
        <p className="mt-2 font-mono text-[10px] text-signal/80">{message}</p>
      ) : null}
      {error ? (
        <p className="mt-2 font-mono text-[10px] text-rose-300">{error}</p>
      ) : null}
      {errors.length > 0 ? (
        <ul className="mt-2 space-y-1">
          {errors.map((entry) => (
            <li
              key={entry.id || entry.error}
              className="font-mono text-[10px] leading-relaxed text-rose-300/90"
            >
              skipped {entry.id ? shortHash(entry.id, 8) : "?"}:{" "}
              {entry.error}
            </li>
          ))}
        </ul>
      ) : null}
    </section>
  );
}
