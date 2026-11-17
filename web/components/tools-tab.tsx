"use client";

import { useEffect, useRef, useState } from "react";

import { apiDownload } from "@/lib/api";
import type { UseTauResult } from "@/lib/use-tau";
import {
  formatDateTime,
  formatFileSize,
  peerLabel,
  shortHash,
} from "@/lib/format";
import type { DoctorResponse } from "@/lib/types";
import { Button, Card, CardTitle, Chip, EmptyState, ErrorNote, Toggle } from "./ui";

const inputClass =
  "min-w-0 flex-1 rounded-lg border border-line bg-panel-2 px-3 py-2 text-sm text-ink placeholder:text-faint focus:border-signal-dim focus:outline-none";

function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
}

function downloadText(text: string, filename: string) {
  downloadBlob(new Blob([text], { type: "application/json" }), filename);
}

function statusChip(status: "ok" | "warn" | "fail") {
  if (status === "fail") {
    return (
      <Chip>
        <span className="text-danger">fail</span>
      </Chip>
    );
  }
  if (status === "warn") {
    return (
      <Chip>
        <span className="text-amber">warn</span>
      </Chip>
    );
  }
  return (
    <Chip>
      <span className="text-signal">ok</span>
    </Chip>
  );
}

export function ToolsTab({ tau }: { tau: UseTauResult }) {
  return (
    <div className="flex min-w-0 flex-col gap-4">
      <DoctorCard tau={tau} />
      <BackupCard tau={tau} />
      <BundleCard tau={tau} />
      <MessagesBackupCard tau={tau} />
      <AllowlistCard tau={tau} />
      <DmRequestsCard tau={tau} />
      <JoinRequestsCard tau={tau} />
      <HeldMailCard tau={tau} />
    </div>
  );
}

function DoctorCard({ tau }: { tau: UseTauResult }) {
  const [report, setReport] = useState<DoctorResponse | null>(null);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const failed =
    report?.checks.filter((check) => check.status === "fail").length ?? 0;

  async function run() {
    setRunning(true);
    setError(null);
    // Drop the previous verdict first: if this run fails, the old
    // healthy badge must not stay visible next to the new error.
    setReport(null);
    try {
      setReport(await tau.runDoctor());
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "health check failed");
    } finally {
      setRunning(false);
    }
  }

  return (
    <Card>
      <CardTitle
        right={
          report ? (
            <Chip>
              {report.ok
                ? "healthy"
                : `${failed} check${failed === 1 ? "" : "s"} failed`}
            </Chip>
          ) : undefined
        }
      >
        Node doctor
      </CardTitle>
      <div className="flex flex-col gap-3 px-3 py-3">
        <p className="text-sm leading-relaxed text-mist">
          Read-only check of the data directory — the same verdicts as{" "}
          <code className="text-ink">tau doctor</code>. Nothing is changed or
          locked.
        </p>
        <ErrorNote message={error} />
        {report ? (
          <ul className="flex flex-col gap-2">
            {report.checks.map((check) => (
              <li
                key={check.name}
                className="flex min-w-0 flex-wrap items-start justify-between gap-2 rounded-lg border border-line bg-panel-2 px-3 py-2"
              >
                <div className="min-w-0 flex-1">
                  <p className="text-sm font-medium text-ink">{check.name}</p>
                  <p className="mt-0.5 break-words text-sm leading-relaxed text-mist">
                    {check.detail}
                  </p>
                </div>
                {statusChip(check.status)}
              </li>
            ))}
          </ul>
        ) : null}
        <div>
          <Button onClick={() => void run()} disabled={running}>
            {running ? "checking…" : report ? "run again" : "run health check"}
          </Button>
        </div>
      </div>
    </Card>
  );
}

function BackupCard({ tau }: { tau: UseTauResult }) {
  const [error, setError] = useState<string | null>(null);
  const [done, setDone] = useState(false);

  async function run() {
    setError(null);
    setDone(false);
    try {
      await tau.downloadBackup();
      setDone(true);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "backup failed");
    }
  }

  return (
    <Card>
      <CardTitle>Backup</CardTitle>
      <div className="flex flex-col gap-3 px-3 py-3">
        <p className="text-sm leading-relaxed text-mist">
          Download a full backup of the node&apos;s data directory as a
          single archive.
        </p>
        <ErrorNote message={error ?? tau.backupError} />
        {done ? (
          <p className="text-sm text-signal">Backup downloaded.</p>
        ) : null}
        <div>
          <Button onClick={() => void run()} disabled={tau.backingUp}>
            {tau.backingUp ? "backing up…" : "download backup"}
          </Button>
        </div>
      </div>
    </Card>
  );
}

function BundleCard({ tau }: { tau: UseTauResult }) {
  const [includeTan, setIncludeTan] = useState(false);
  const [exportSummary, setExportSummary] = useState<string | null>(null);
  const [importReport, setImportReport] = useState<string | null>(null);
  const [importText, setImportText] = useState("");
  const [error, setError] = useState<string | null>(null);
  const fileInput = useRef<HTMLInputElement | null>(null);

  async function handleExport() {
    setError(null);
    setExportSummary(null);
    try {
      const result = await tau.exportBundle(includeTan);
      // Download the bundle text verbatim: re-parsing it could rewrite
      // a number the signature covers.
      const stamp = new Date().toISOString().slice(0, 10);
      downloadText(result.bundle, `tau-bundle-${stamp}.json`);
      const summary = result.summary;
      setExportSummary(
        `${summary.mail} sealed message${summary.mail === 1 ? "" : "s"} · ${summary.tan} Tan post${summary.tan === 1 ? "" : "s"} · ${formatFileSize(summary.bytes)}`,
      );
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "export failed");
    }
  }

  async function handleImport(text: string) {
    const bundle = text.trim();
    if (bundle === "" || tau.importing) return;
    setError(null);
    setImportReport(null);
    try {
      // The text goes to the server verbatim: the bundle is signed.
      const result = await tau.importBundle(bundle);
      const report = result.import;
      setImportReport(
        `imported ${report.imported} · already held ${report.present} · skipped ${report.skipped}` +
          (report.tan
            ? ` · Tan: ${report.tan.imported} imported, ${report.tan.present} already present, ${report.tan.skipped} skipped`
            : ""),
      );
      setImportText("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "import failed");
    }
  }

  async function handleFilePicked(file: File) {
    await handleImport(await file.text());
    if (fileInput.current) fileInput.current.value = "";
  }

  return (
    <Card>
      <CardTitle>Bundle export / import</CardTitle>
      <div className="flex flex-col gap-3 px-3 py-3">
        <p className="text-sm leading-relaxed text-mist">
          Carry sealed mail (and optionally Tan posts) on a file between
          nodes with no live path between them.
        </p>
        <ErrorNote message={error} />
        <label className="flex min-w-0 cursor-pointer items-center gap-2 text-sm text-ink">
          <input
            type="checkbox"
            checked={includeTan}
            onChange={(event) => setIncludeTan(event.target.checked)}
            className="h-4 w-4 shrink-0 accent-teal-400"
          />
          <span className="min-w-0">Include recent Tan posts in the export</span>
        </label>
        <div>
          <Button onClick={() => void handleExport()} disabled={tau.exporting}>
            {tau.exporting ? "exporting…" : "download bundle"}
          </Button>
        </div>
        {exportSummary ? (
          <p className="break-words text-sm text-signal">{exportSummary}</p>
        ) : null}
        <div className="flex min-w-0 flex-col gap-2 border-t border-line-soft pt-3">
          <label htmlFor="bundle-import-text" className="text-sm text-mist">
            Paste a bundle to import, or choose a bundle file:
          </label>
          <textarea
            id="bundle-import-text"
            value={importText}
            onChange={(event) => setImportText(event.target.value)}
            rows={3}
            placeholder="paste bundle JSON here"
            className={`${inputClass} resize-y`}
          />
          <div className="flex flex-wrap gap-2">
            <Button
              onClick={() => void handleImport(importText)}
              disabled={importText.trim() === "" || tau.importing}
            >
              {tau.importing ? "importing…" : "import bundle"}
            </Button>
            <label
              htmlFor="bundle-file"
              className={`inline-flex min-h-9 shrink-0 cursor-pointer items-center justify-center gap-1.5 rounded-lg border border-line bg-transparent px-3 py-1.5 text-sm font-medium text-ink hover:border-signal-dim hover:text-signal ${tau.importing ? "pointer-events-none opacity-50" : ""}`}
            >
              {tau.importing ? "importing…" : "choose bundle file"}
            </label>
            <input
              id="bundle-file"
              ref={fileInput}
              type="file"
              accept=".json,application/json"
              className="hidden"
              disabled={tau.importing}
              onChange={(event) => {
                const file = event.target.files?.[0];
                if (file) void handleFilePicked(file);
              }}
            />
          </div>
        </div>
        {importReport ? (
          <p className="break-words text-sm text-signal">{importReport}</p>
        ) : null}
      </div>
    </Card>
  );
}

function MessagesBackupCard({ tau }: { tau: UseTauResult }) {
  const [message, setMessage] = useState<string | null>(null);
  const [errors, setErrors] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [exporting, setExporting] = useState(false);
  const [importing, setImporting] = useState(false);
  const [importText, setImportText] = useState("");
  const importInputRef = useRef<HTMLInputElement | null>(null);

  async function submitExport() {
    setExporting(true);
    setError(null);
    setMessage(null);
    setErrors([]);
    try {
      const { blob, filename } = await apiDownload("/api/messages/export");
      let count: number | null = null;
      try {
        const doc = JSON.parse(await blob.text()) as {
          messages?: unknown[];
        };
        count = Array.isArray(doc.messages) ? doc.messages.length : null;
      } catch {
        count = null;
      }
      downloadBlob(blob, filename);
      setMessage(
        count === null
          ? "messages exported"
          : `exported ${count} message${count === 1 ? "" : "s"}`,
      );
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "export failed");
    } finally {
      setExporting(false);
    }
  }

  async function submitImportText() {
    const text = importText.trim();
    if (text === "" || importing) return;
    setImporting(true);
    setError(null);
    setMessage(null);
    setErrors([]);
    try {
      const result = await tau.importMessages(text);
      const report = result.import;
      setMessage(
        `imported ${report.imported} · ${report.present} already present · ${report.skipped} skipped`,
      );
      setErrors(report.errors.map((entry) => entry.error));
      setImportText("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "import failed");
    } finally {
      setImporting(false);
    }
  }

  async function submitImportFile(file: File) {
    setImporting(true);
    setError(null);
    setMessage(null);
    setErrors([]);
    try {
      const result = await tau.importMessages(await file.text());
      const report = result.import;
      setMessage(
        `imported ${report.imported} · ${report.present} already present · ${report.skipped} skipped`,
      );
      setErrors(report.errors.map((entry) => entry.error));
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "import failed");
    } finally {
      setImporting(false);
      if (importInputRef.current) importInputRef.current.value = "";
    }
  }

  return (
    <Card>
      <CardTitle>Messages export / import</CardTitle>
      <div className="flex flex-col gap-3 px-3 py-3">
        <p className="text-sm leading-relaxed text-mist">
          Back up your stored DMs as a file, or restore one. Restored
          messages show up in the feed right away.
        </p>
        <ErrorNote message={error} />
        {message ? (
          <p className="break-words text-sm text-signal">{message}</p>
        ) : null}
        {errors.length > 0 ? (
          <ul className="flex flex-col gap-1">
            {errors.map((entry, index) => (
              <li key={`${entry}-${index}`} className="break-words text-sm text-danger">
                {entry}
              </li>
            ))}
          </ul>
        ) : null}
        <div className="flex flex-wrap gap-2">
          <Button onClick={() => void submitExport()} disabled={exporting}>
            {exporting ? "exporting…" : "export messages"}
          </Button>
          <label
            htmlFor="messages-import-file"
            className={`inline-flex min-h-9 shrink-0 cursor-pointer items-center justify-center gap-1.5 rounded-lg border border-line bg-transparent px-3 py-1.5 text-sm font-medium text-ink hover:border-signal-dim hover:text-signal ${importing ? "pointer-events-none opacity-50" : ""}`}
          >
            {importing ? "importing…" : "import from file"}
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
        <div className="flex min-w-0 flex-col gap-2 border-t border-line-soft pt-3">
          <label htmlFor="messages-import-text" className="text-sm text-mist">
            Or paste an export document:
          </label>
          <textarea
            id="messages-import-text"
            value={importText}
            onChange={(event) => setImportText(event.target.value)}
            rows={3}
            placeholder="paste messages export JSON here"
            className={`${inputClass} resize-y`}
          />
          <div>
            <Button
              onClick={() => void submitImportText()}
              disabled={importText.trim() === "" || importing}
            >
              {importing ? "importing…" : "import messages"}
            </Button>
          </div>
        </div>
      </div>
    </Card>
  );
}

function AllowlistCard({ tau }: { tau: UseTauResult }) {
  const [target, setTarget] = useState("");
  const [adding, setAdding] = useState(false);
  const [removing, setRemoving] = useState<string | null>(null);
  const [toggling, setToggling] = useState(false);
  const [confirmingEnable, setConfirmingEnable] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const confirmTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (confirmTimer.current) clearTimeout(confirmTimer.current);
    };
  }, []);

  const enabled = tau.allowlist?.enabled ?? false;
  const entries = tau.allowlist?.entries ?? [];

  async function handleAdd() {
    const value = target.trim();
    if (!value || adding) return;
    setAdding(true);
    setError(null);
    try {
      await tau.allowlistAdd(value);
      setTarget("");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "add failed");
    } finally {
      setAdding(false);
    }
  }

  async function handleRemove(destination: string) {
    if (removing) return;
    setRemoving(destination);
    setError(null);
    try {
      await tau.allowlistRemove(destination);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "remove failed");
    } finally {
      setRemoving(null);
    }
  }

  async function handleToggle(next: boolean) {
    if (toggling) return;
    // Enforcing an empty list locks every sender out, so it needs an
    // explicit second click.
    if (next && entries.length === 0 && !confirmingEnable) {
      setConfirmingEnable(true);
      if (confirmTimer.current) clearTimeout(confirmTimer.current);
      confirmTimer.current = setTimeout(
        () => setConfirmingEnable(false),
        5000,
      );
      return;
    }
    if (confirmTimer.current) clearTimeout(confirmTimer.current);
    setConfirmingEnable(false);
    setToggling(true);
    setError(null);
    try {
      await tau.allowlistSetEnabled(next);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "toggle failed");
    } finally {
      setToggling(false);
    }
  }

  return (
    <Card>
      <CardTitle>DM allowlist</CardTitle>
      <div className="flex flex-col gap-3 px-3 py-3">
        <Toggle
          checked={enabled}
          onChange={(next) => void handleToggle(next)}
          label={
            confirmingEnable
              ? "Confirm — enforcing an empty list blocks every DM"
              : "Enforce allowlist"
          }
        />
        <p className="text-sm leading-relaxed text-mist">
          {tau.allowlist === null
            ? "loading…"
            : enabled
              ? "Only listed senders can DM this node — everyone else is dropped."
              : "Anyone can DM this node. Turn enforcement on to restrict it to the list."}
        </p>
        <ErrorNote message={error} />
        {entries.length > 0 ? (
          <ul className="flex flex-col gap-2">
            {entries.map((entry) => (
              <li
                key={entry.destination}
                className="flex min-w-0 flex-wrap items-center justify-between gap-2 rounded-lg border border-line bg-panel-2 px-3 py-2"
              >
                <span
                  className="min-w-0 break-words text-sm text-ink"
                  title={entry.destination}
                >
                  {peerLabel(entry.name, entry.destination)}
                </span>
                <Button
                  variant="danger"
                  disabled={removing === entry.destination}
                  onClick={() => void handleRemove(entry.destination)}
                >
                  {removing === entry.destination ? "removing…" : "remove"}
                </Button>
              </li>
            ))}
          </ul>
        ) : tau.allowlist !== null ? (
          <p className="text-sm text-faint">No senders listed.</p>
        ) : null}
        <form
          className="flex min-w-0 gap-2"
          onSubmit={(event) => {
            event.preventDefault();
            void handleAdd();
          }}
        >
          <input
            value={target}
            onChange={(event) => setTarget(event.target.value)}
            placeholder="peer name, hash or prefix"
            aria-label="peer to allow"
            list="allowlist-peers"
            autoComplete="off"
            className={inputClass}
          />
          <datalist id="allowlist-peers">
            {tau.peers.map((peer) => (
              <option key={peer.destination} value={peer.destination}>
                {peer.name || shortHash(peer.destination)}
              </option>
            ))}
          </datalist>
          <Button
            type="submit"
            variant="primary"
            disabled={adding || target.trim() === ""}
          >
            {adding ? "adding…" : "allow"}
          </Button>
        </form>
      </div>
    </Card>
  );
}

function DmRequestsCard({ tau }: { tau: UseTauResult }) {
  const [blockIds, setBlockIds] = useState<Record<string, boolean>>({});
  const [acceptingId, setAcceptingId] = useState<string | null>(null);
  const [decliningId, setDecliningId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const requests = tau.dmRequests;

  async function accept(id: string) {
    if (acceptingId !== null || decliningId !== null) return;
    setAcceptingId(id);
    setError(null);
    setNotice(null);
    try {
      const result = await tau.acceptDmRequest(id);
      setNotice(
        result.delivered
          ? "Accepted — sender allow-listed, message delivered to the inbox."
          : `Accepted — sender allow-listed, message not delivered (${result.skipped ?? "already delivered"}).`,
      );
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "accept failed");
    } finally {
      setAcceptingId(null);
    }
  }

  async function decline(id: string) {
    if (acceptingId !== null || decliningId !== null) return;
    setDecliningId(id);
    setError(null);
    setNotice(null);
    try {
      await tau.declineDmRequest(id, blockIds[id] ?? false);
      setNotice(
        blockIds[id]
          ? "Declined — sender blocked, the held message was dropped."
          : "Declined — the held message was dropped.",
      );
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "decline failed");
    } finally {
      setDecliningId(null);
    }
  }

  return (
    <Card>
      <CardTitle
        right={requests.length > 0 ? <Chip>{requests.length} held</Chip> : undefined}
      >
        DM requests
      </CardTitle>
      <div className="flex flex-col gap-3 px-3 py-3">
        <p className="text-sm leading-relaxed text-mist">
          Messages from peers outside the allowlist — accept to allow-list
          the sender and deliver the message, decline to drop it.
        </p>
        {notice ? <p className="text-sm text-signal">{notice}</p> : null}
        <ErrorNote message={error} />
        {requests.length === 0 ? (
          <EmptyState>No DM requests held.</EmptyState>
        ) : (
          <ul className="flex flex-col gap-3">
            {requests.map((request) => (
              <li
                key={request.id}
                className="min-w-0 rounded-lg border border-line bg-panel-2 p-3"
              >
                <div className="flex min-w-0 flex-wrap items-start justify-between gap-2">
                  <div className="min-w-0 flex-1">
                    <p className="min-w-0 break-words text-sm font-medium text-ink">
                      {peerLabel(request.sender_name, request.sender)}
                    </p>
                    <p className="mt-1 break-words text-sm leading-relaxed text-ink">
                      {request.text}
                    </p>
                  </div>
                  <div className="flex shrink-0 flex-wrap gap-2">
                    <Button
                      variant="primary"
                      disabled={acceptingId !== null || decliningId !== null}
                      onClick={() => void accept(request.id)}
                    >
                      {acceptingId === request.id ? "accepting…" : "accept"}
                    </Button>
                    <Button
                      variant="danger"
                      disabled={acceptingId !== null || decliningId !== null}
                      onClick={() => void decline(request.id)}
                    >
                      {decliningId === request.id ? "declining…" : "decline"}
                    </Button>
                  </div>
                </div>
                <label className="mt-2 flex min-w-0 cursor-pointer items-center gap-2 text-sm text-mist">
                  <input
                    type="checkbox"
                    checked={blockIds[request.id] ?? false}
                    onChange={(event) =>
                      setBlockIds((previous) => ({
                        ...previous,
                        [request.id]: event.target.checked,
                      }))
                    }
                    className="h-4 w-4 shrink-0 accent-teal-400"
                  />
                  <span className="min-w-0">Block the sender when declining</span>
                </label>
                <p className="mt-2 text-xs leading-relaxed text-faint">
                  held until {formatDateTime(request.request_expires_at)}
                  {request.expires_at
                    ? ` · message expires ${formatDateTime(request.expires_at)}`
                    : ""}
                </p>
              </li>
            ))}
          </ul>
        )}
      </div>
    </Card>
  );
}

function JoinRequestsCard({ tau }: { tau: UseTauResult }) {
  const [approvingKey, setApprovingKey] = useState<string | null>(null);
  const [denyingKey, setDenyingKey] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const requests = tau.joinRequests;

  function keyFor(group: string, peer: string) {
    return `${group}:${peer}`;
  }

  async function decide(
    group: string,
    peer: string,
    peerName: string,
    approve: boolean,
  ) {
    if (approvingKey !== null || denyingKey !== null) return;
    const key = keyFor(group, peer);
    if (approve) setApprovingKey(key);
    else setDenyingKey(key);
    setError(null);
    setNotice(null);
    try {
      const result = await tau.decideJoinRequest(group, peer, approve);
      setNotice(
        approve
          ? result.status === "already_member"
            ? `${peerName} is already a member of ${group}.`
            : `${peerName} joined ${group}.`
          : `${peerName} was denied for ${group}.`,
      );
    } catch (caught) {
      setError(
        caught instanceof Error
          ? caught.message
          : approve
            ? "approve failed"
            : "deny failed",
      );
    } finally {
      setApprovingKey(null);
      setDenyingKey(null);
    }
  }

  return (
    <Card>
      <CardTitle
        right={requests.length > 0 ? <Chip>{requests.length} pending</Chip> : undefined}
      >
        Join requests
      </CardTitle>
      <div className="flex flex-col gap-3 px-3 py-3">
        <p className="text-sm leading-relaxed text-mist">
          Peers asking to join a closed group — approve to add them to the
          roster, deny to turn them away.
        </p>
        {notice ? <p className="text-sm text-signal">{notice}</p> : null}
        <ErrorNote message={error} />
        {requests.length === 0 ? (
          <EmptyState>No join requests pending.</EmptyState>
        ) : (
          <ul className="flex flex-col gap-3">
            {requests.map((request) => {
              const key = keyFor(request.group, request.peer);
              return (
                <li
                  key={key}
                  className="min-w-0 rounded-lg border border-line bg-panel-2 p-3"
                >
                  <div className="flex min-w-0 flex-wrap items-start justify-between gap-2">
                    <div className="min-w-0 flex-1">
                      <p className="min-w-0 break-words text-sm font-medium text-ink">
                        {peerLabel(request.peer_name, request.peer)}
                      </p>
                      <p className="mt-1 break-words text-sm leading-relaxed text-ink">
                        wants to join {request.group}
                      </p>
                    </div>
                    <div className="flex shrink-0 flex-wrap gap-2">
                      <Button
                        variant="primary"
                        disabled={approvingKey !== null || denyingKey !== null}
                        onClick={() =>
                          void decide(
                            request.group,
                            request.peer,
                            request.peer_name,
                            true,
                          )
                        }
                      >
                        {approvingKey === key ? "approving…" : "approve"}
                      </Button>
                      <Button
                        variant="danger"
                        disabled={approvingKey !== null || denyingKey !== null}
                        onClick={() =>
                          void decide(
                            request.group,
                            request.peer,
                            request.peer_name,
                            false,
                          )
                        }
                      >
                        {denyingKey === key ? "denying…" : "deny"}
                      </Button>
                    </div>
                  </div>
                  <p className="mt-2 text-xs leading-relaxed text-faint">
                    requested {formatDateTime(request.requested_at)} · expires{" "}
                    {formatDateTime(request.expires_at)}
                  </p>
                </li>
              );
            })}
          </ul>
        )}
      </div>
    </Card>
  );
}

function HeldMailCard({ tau }: { tau: UseTauResult }) {
  const [droppingId, setDroppingId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const held = tau.heldMail;

  async function drop(id: string) {
    if (droppingId !== null) return;
    setDroppingId(id);
    setError(null);
    setNotice(null);
    try {
      await tau.dropHeld(id);
      setNotice("Held mail dropped.");
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "drop failed");
    } finally {
      setDroppingId(null);
    }
  }

  return (
    <Card>
      <CardTitle
        right={held.length > 0 ? <Chip>{held.length} held</Chip> : undefined}
      >
        Held mail
      </CardTitle>
      <div className="flex flex-col gap-3 px-3 py-3">
        <p className="text-sm leading-relaxed text-mist">
          Sealed mail this node is holding for offline peers.
        </p>
        {notice ? <p className="text-sm text-signal">{notice}</p> : null}
        <ErrorNote message={error} />
        {held.length === 0 ? (
          <EmptyState>No mail held for other peers.</EmptyState>
        ) : (
          <ul className="flex flex-col gap-3">
            {held.map((entry) => (
              <li
                key={entry.id}
                className="min-w-0 rounded-lg border border-line bg-panel-2 p-3"
              >
                <div className="flex min-w-0 flex-wrap items-start justify-between gap-2">
                  <div className="min-w-0 flex-1">
                    <p className="min-w-0 break-words text-sm text-ink">
                      <span className="text-mist">to </span>
                      {peerLabel(entry.to_name, entry.to)}
                    </p>
                    <p className="mt-1 min-w-0 break-words text-sm text-ink">
                      <span className="text-mist">from </span>
                      {peerLabel(entry.from_name, entry.from)}
                    </p>
                    <p className="mt-1 break-words text-xs leading-relaxed text-faint">
                      stored {formatDateTime(entry.stored_at)} · expires{" "}
                      {formatDateTime(entry.expires_at)} ·{" "}
                      {formatFileSize(entry.blob_bytes)}
                    </p>
                  </div>
                  <Button
                    variant="danger"
                    disabled={droppingId !== null}
                    onClick={() => void drop(entry.id)}
                  >
                    {droppingId === entry.id ? "dropping…" : "drop"}
                  </Button>
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </Card>
  );
}

export default ToolsTab;
