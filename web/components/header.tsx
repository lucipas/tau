"use client";

import Link from "next/link";

import type { ConnectionState } from "@/lib/use-tau";
import type { NodeInfo } from "@/lib/types";
import { shortHash } from "@/lib/format";

import { BoltIcon, CopyButton, DownloadIcon, SignalIcon, StatusDot } from "./ui";

const connectionLabel: Record<ConnectionState, string> = {
  live: "live",
  connecting: "connecting",
  reconnecting: "reconnecting",
};

const connectionClass: Record<ConnectionState, string> = {
  live: "border-signal/35 bg-signal/10 text-signal",
  connecting: "border-amber/35 bg-amber/10 text-amber",
  reconnecting: "border-rose-400/35 bg-rose-400/10 text-rose-300",
};

export function Header({
  node,
  connection,
  announcing,
  onAnnounce,
  backingUp,
  backupError,
  onBackup,
}: {
  node: NodeInfo | null;
  connection: ConnectionState;
  announcing: boolean;
  onAnnounce: () => void;
  backingUp: boolean;
  backupError: string | null;
  onBackup: () => void;
}) {
  return (
    <header className="sticky top-0 z-30 border-b border-line/60 bg-void/80 backdrop-blur-xl">
      <div className="mx-auto flex w-full max-w-7xl flex-wrap items-center gap-x-5 gap-y-3 px-4 py-3 lg:px-8">
        <div className="flex items-center gap-3">
          <div className="grid h-10 w-10 place-items-center rounded-xl border border-signal/30 bg-signal/10 font-mono text-xl text-signal shadow-[inset_0_0_18px_rgba(45,212,191,0.15)]">
            τ
          </div>
          <div className="leading-tight">
            <div className="flex items-baseline gap-2">
              <span className="font-mono text-lg font-semibold tracking-tight text-zinc-100">
                tau
              </span>
              <span className="rounded border border-line bg-panel px-1.5 py-0.5 font-mono text-[10px] uppercase tracking-[0.18em] text-mist">
                mesh console
              </span>
            </div>
            <p className="mt-0.5 text-[11px] text-mist">
              Reticulum peer-to-peer node
            </p>
          </div>
        </div>

        <nav className="hidden items-center gap-1 rounded-xl border border-line/70 bg-panel/60 p-1 text-xs sm:flex">
          <Link
            href="/"
            className="rounded-lg bg-signal/15 px-2.5 py-1.5 text-signal"
          >
            chat
          </Link>
          <Link
            href="/social/"
            className="rounded-lg px-2.5 py-1.5 text-mist transition hover:text-zinc-100"
          >
            social
          </Link>
        </nav>

        <div className="ml-auto flex flex-wrap items-center gap-2">
          <span
            className={`inline-flex items-center gap-2 rounded-full border px-2.5 py-1 font-mono text-[11px] ${connectionClass[connection]}`}
            role="status"
          >
            <StatusDot state={connection} />
            {connectionLabel[connection]}
          </span>

          {node ? (
            <>
              <span className="hidden items-center gap-2 rounded-full border border-line bg-panel/70 px-3 py-1 text-xs text-zinc-300 sm:inline-flex">
                <SignalIcon className="h-3.5 w-3.5 text-signal" />
                {node.name}
              </span>
              <span className="hidden items-center gap-2 rounded-full border border-line bg-panel/70 py-1 pl-3 pr-1 lg:flex">
                <span className="font-mono text-[11px] text-mist">
                  {shortHash(node.destination, 12)}
                </span>
                <CopyButton value={node.destination} label="dest" />
              </span>
            </>
          ) : null}

          <button
            type="button"
            onClick={onAnnounce}
            disabled={!node || announcing}
            className="inline-flex items-center gap-1.5 rounded-lg border border-signal/40 bg-signal/10 px-3 py-1.5 text-xs font-medium text-signal transition hover:border-signal/70 hover:bg-signal/15 disabled:cursor-not-allowed disabled:opacity-50"
          >
            <BoltIcon className="h-3.5 w-3.5" />
            {announcing ? "announcing…" : "announce"}
          </button>

          <button
            type="button"
            onClick={onBackup}
            disabled={!node || backingUp}
            title="Download a backup archive of this node's data directory"
            className="inline-flex items-center gap-1.5 rounded-lg border border-line bg-panel/70 px-3 py-1.5 text-xs font-medium text-zinc-300 transition hover:border-mist/50 hover:text-zinc-100 disabled:cursor-not-allowed disabled:opacity-50"
          >
            <DownloadIcon className="h-3.5 w-3.5" />
            {backingUp ? "backing up…" : "back up"}
          </button>
          {backupError ? (
            <span
              role="alert"
              className="max-w-64 truncate font-mono text-[11px] text-rose-300"
              title={backupError}
            >
              backup failed: {backupError}
            </span>
          ) : null}
        </div>
      </div>
    </header>
  );
}
