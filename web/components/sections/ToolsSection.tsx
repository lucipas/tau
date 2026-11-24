// ToolsSection.tsx — node tools: doctor / health report, backup
// download, sneakernet bundle export/import, Tan export/import, linked
// devices (pair / list / revoke), and the allowlist (add / remove /
// enable / disable). Built on api.ts.

"use client";

import { useEffect, useState } from "react";
import {
  allowlistAdd,
  allowlistRemove,
  allowlistSetEnabled,
  backupUrl,
  deviceBundleUrl,
  exportBundle,
  exportTan,
  getAllowlist,
  getDevices,
  getDoctor,
  importBundle,
  importTan,
  pairDevice,
  revokeDeviceOffer,
} from "@/lib/api";
import type { AllowList, DevicesInfo, DoctorReport } from "@/lib/types";

function downloadBundleFile(url: string, filename: string) {
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
}

function downloadJson(filename: string, doc: unknown) {
  const blob = new Blob([JSON.stringify(doc, null, 2)], {
    type: "application/json",
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

async function readJsonFile(file: File): Promise<unknown> {
  return JSON.parse(await file.text());
}

export default function ToolsSection() {
  const [doctor, setDoctor] = useState<DoctorReport | null>(null);
  const [allowlist, setAllowlist] = useState<AllowList | null>(null);
  const [devices, setDevices] = useState<DevicesInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [addTarget, setAddTarget] = useState("");
  const [includeTan, setIncludeTan] = useState(false);
  const [includeHistory, setIncludeHistory] = useState(true);

  const refresh = async () => {
    try {
      const [d, a] = await Promise.all([getDoctor(), getAllowlist()]);
      setDoctor(d);
      setAllowlist(a);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
    // Linked devices are loopback-only: a remote console gets a 403
    // here, which must not fail the whole Tools page — the section
    // then explains itself instead of listing devices.
    try {
      setDevices(await getDevices());
    } catch {
      setDevices(null);
    }
  };

  useEffect(() => {
    void refresh();
  }, []);

  async function run(fn: () => Promise<unknown>, okMsg: string) {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const result = await fn();
      setNotice(okMsg);
      return result;
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      return null;
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mx-auto w-full max-w-[600px]">
      <header className="sticky top-0 z-10 border-b border-zinc-800 bg-zinc-950/95 px-3 py-3 backdrop-blur sm:px-4">
        <h1 className="text-lg font-bold text-zinc-50">
          <span className="mr-2 text-cyan-400">τ</span>Tools
        </h1>
        <p className="mt-0.5 text-xs text-zinc-500">
          Health checks, backup and restore, and who may reach this node.
        </p>
      </header>

      {error && (
        <p role="alert" className="border-b border-zinc-800 px-4 py-2 text-sm text-red-400">
          {error}
        </p>
      )}
      {notice && (
        <p role="status" className="border-b border-zinc-800 px-4 py-2 text-sm text-cyan-300">
          {notice}
        </p>
      )}

      <section className="border-b border-zinc-800 px-3 py-4 sm:px-4" aria-label="Node health">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-bold text-zinc-100">Node doctor</h2>
          <button
            type="button"
            onClick={() => void refresh()}
            disabled={loading}
            className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-semibold text-zinc-300 hover:bg-zinc-900 disabled:opacity-50"
          >
            Re-run checks
          </button>
        </div>
        {loading && <p className="py-3 text-sm text-zinc-500">Running checks…</p>}
        {doctor && (
          <>
            <p
              className={`mt-2 text-sm font-semibold ${doctor.ok ? "text-emerald-400" : "text-amber-400"}`}
            >
              {doctor.ok ? "✓ All checks passed" : "⚠ Some checks need attention"}
            </p>
            <ul className="mt-2 flex flex-col gap-2">
              {doctor.checks.map((c, i) => (
                <li
                  key={`${c.name}-${i}`}
                  className="rounded-lg border border-zinc-800 bg-zinc-900 px-3 py-2"
                >
                  <p className="text-sm font-semibold text-zinc-100">
                    <span aria-hidden="true">
                      {c.ok === false || c.status === "fail" || c.status === "error" ? "✗ " : "✓ "}
                    </span>
                    {c.name}
                    {c.status && c.status !== "ok" && (
                      <span className="ml-2 text-xs font-normal text-amber-400">{c.status}</span>
                    )}
                  </p>
                  {(c.message ?? c.detail) && (
                    <p className="mt-0.5 text-xs text-zinc-400">{c.message ?? c.detail}</p>
                  )}
                </li>
              ))}
            </ul>
            {doctor.checks.length === 0 && (
              <p className="py-2 text-sm text-zinc-500">No checks reported.</p>
            )}
          </>
        )}
      </section>

      <section className="border-b border-zinc-800 px-3 py-4 sm:px-4" aria-label="Backup and bundles">
        <h2 className="text-sm font-bold text-zinc-100">Backup &amp; bundles</h2>
        <p className="mt-1 text-xs leading-relaxed text-zinc-500">
          A backup is a full archive of the node&apos;s data directory. A bundle is a signed
          sneakernet document you can carry to another node (optionally with Tan posts).
        </p>
        <div className="mt-3 flex flex-wrap items-center gap-2">
          <a
            href={backupUrl()}
            download
            className="rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-bold text-zinc-950 hover:bg-cyan-400"
          >
            ⬇ Download backup
          </a>
          <label className="flex items-center gap-1.5 text-xs text-zinc-300">
            <input
              type="checkbox"
              checked={includeTan}
              onChange={(e) => setIncludeTan(e.target.checked)}
            />
            Include Tan posts in bundle
          </label>
          <button
            type="button"
            disabled={busy}
            onClick={() =>
              void run(
                async () => {
                  const doc = await exportBundle(includeTan);
                  downloadJson(
                    `tau-bundle-${new Date().toISOString().slice(0, 10)}.json`,
                    doc,
                  );
                },
                "Bundle exported — check your downloads.",
              )
            }
            className="rounded-full border border-zinc-700 px-4 py-1.5 text-sm font-semibold text-zinc-200 hover:bg-zinc-900 disabled:opacity-50"
          >
            Export bundle
          </button>
          <label className="cursor-pointer rounded-full border border-zinc-700 px-4 py-1.5 text-sm font-semibold text-zinc-200 hover:bg-zinc-900">
            Import bundle…
            <input
              type="file"
              accept="application/json,.json"
              aria-label="Import bundle file"
              className="hidden"
              disabled={busy}
              onChange={(e) => {
                const file = e.target.files?.[0];
                if (!file) return;
                void run(
                  async () => readJsonFile(file).then((doc) => importBundle(doc)),
                  "Bundle imported.",
                );
                e.target.value = "";
              }}
            />
          </label>
        </div>
        <div className="mt-3 flex flex-wrap gap-2">
          <button
            type="button"
            disabled={busy}
            onClick={() =>
              void run(
                async () => {
                  const doc = await exportTan("mine");
                  downloadJson(
                    `tau-tan-mine-${new Date().toISOString().slice(0, 10)}.json`,
                    doc,
                  );
                },
                "Your Tan posts exported.",
              )
            }
            className="rounded-full border border-zinc-700 px-3 py-1.5 text-xs font-semibold text-zinc-200 hover:bg-zinc-900 disabled:opacity-50"
          >
            Export my Tan posts
          </button>
          <label className="cursor-pointer rounded-full border border-zinc-700 px-3 py-1.5 text-xs font-semibold text-zinc-200 hover:bg-zinc-900">
            Import Tan export…
            <input
              type="file"
              accept="application/json,.json"
              aria-label="Import Tan export file"
              className="hidden"
              disabled={busy}
              onChange={(e) => {
                const file = e.target.files?.[0];
                if (!file) return;
                void run(
                  async () => readJsonFile(file).then((doc) => importTan(doc)),
                  "Tan export imported.",
                );
                e.target.value = "";
              }}
            />
          </label>
        </div>
      </section>

      <section className="border-b border-zinc-800 px-3 py-4 sm:px-4" aria-label="Linked devices">
        <h2 className="text-sm font-bold text-zinc-100">Linked devices</h2>
        <p className="mt-1 text-xs leading-relaxed text-zinc-500">
          Pair a second device with this identity, or cancel a pairing offer you no
          longer need. A bundle carries the private identity — treat the downloaded
          file like an identity backup, and redeem it before it expires.
        </p>
        {devices === null && (
          <p className="py-2 text-sm text-zinc-500">
            Linked devices are available when the console runs on the same machine as
            the node.
          </p>
        )}
        {devices !== null && (
          <>
            <p className="mt-2 truncate font-mono text-xs text-zinc-400">
              This device: {devices.identity ?? "no identity yet"}
            </p>
            {devices.paired_from && (
              <p className="mt-0.5 text-xs text-zinc-500">
                Paired from {devices.paired_from.primary.slice(0, 8)}… on{" "}
                {new Date(devices.paired_from.paired_at * 1000).toLocaleString()}
              </p>
            )}
            <div className="mt-3 flex flex-wrap items-center gap-2">
              <label className="flex items-center gap-1.5 text-xs text-zinc-300">
                <input
                  type="checkbox"
                  checked={includeHistory}
                  onChange={(e) => setIncludeHistory(e.target.checked)}
                />
                Include recent history in the bundle
              </label>
              <button
                type="button"
                disabled={busy}
                onClick={() =>
                  void run(
                    async () => {
                      const result = await pairDevice({ history: includeHistory });
                      downloadBundleFile(
                        deviceBundleUrl(result.token),
                        result.filename,
                      );
                      setDevices(await getDevices());
                    },
                    "Pairing bundle downloaded — copy it to the second device and run `tau devices redeem --from <file>` before it expires.",
                  )
                }
                className="rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-50"
              >
                Pair a new device
              </button>
            </div>
            {devices.issued_offers.length === 0 && (
              <p className="py-2 text-sm text-zinc-500">No outstanding pairing offers.</p>
            )}
            <ul className="mt-3 flex flex-col gap-2">
              {devices.issued_offers.map((offer) => (
                <li
                  key={offer.token}
                  className="flex items-center gap-2 rounded-lg border border-zinc-800 bg-zinc-900 px-3 py-2"
                >
                  <div className="min-w-0 flex-1">
                    <p className="truncate font-mono text-sm font-semibold text-zinc-100">
                      token {offer.token.slice(0, 8)}…
                    </p>
                    <p className="text-xs text-zinc-500">
                      Expires {new Date(offer.expires_at * 1000).toLocaleString()}
                    </p>
                  </div>
                  <button
                    type="button"
                    disabled={busy}
                    onClick={() =>
                      void run(
                        async () => {
                          await revokeDeviceOffer(offer.token);
                          setDevices(await getDevices());
                        },
                        "Pairing offer revoked.",
                      )
                    }
                    className="shrink-0 rounded-full border border-red-800 px-3 py-1 text-xs text-red-400 hover:bg-red-950 disabled:opacity-50"
                  >
                    Revoke
                  </button>
                </li>
              ))}
            </ul>
          </>
        )}
      </section>

      <section className="px-3 py-4 sm:px-4" aria-label="Allowlist">
        <div className="flex items-center justify-between gap-2">
          <h2 className="text-sm font-bold text-zinc-100">Allowlist</h2>
          <label className="flex items-center gap-1.5 text-xs text-zinc-300">
            <input
              type="checkbox"
              checked={allowlist?.enabled ?? false}
              disabled={busy || !allowlist}
              onChange={(e) =>
                void run(
                  async () => {
                    setAllowlist(await allowlistSetEnabled(e.target.checked));
                  },
                  e.target.checked ? "Allowlist enabled." : "Allowlist disabled.",
                )
              }
            />
            Enabled
          </label>
        </div>
        <p className="mt-1 text-xs leading-relaxed text-zinc-500">
          When enabled, only listed destinations may reach this node.
        </p>
        <form
          className="mt-3 flex gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            const target = addTarget.trim();
            if (!target) return setError("Type a destination hash to add.");
            void run(
              async () => {
                setAllowlist(await allowlistAdd(target));
                setAddTarget("");
              },
              "Destination added to the allowlist.",
            );
          }}
        >
          <input
            value={addTarget}
            onChange={(e) => setAddTarget(e.target.value)}
            placeholder="Destination hash…"
            aria-label="Destination hash to allow"
            className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm font-mono text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
          />
          <button
            type="submit"
            disabled={busy}
            className="shrink-0 rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
          >
            Add
          </button>
        </form>
        {allowlist && allowlist.entries.length === 0 && (
          <p className="py-3 text-sm text-zinc-500">Allowlist is empty.</p>
        )}
        <ul className="mt-3 flex flex-col gap-2">
          {allowlist?.entries.map((entry) => (
            <li
              key={entry.destination}
              className="flex items-center gap-2 rounded-lg border border-zinc-800 bg-zinc-900 px-3 py-2"
            >
              <div className="min-w-0 flex-1">
                <p className="truncate text-sm font-semibold text-zinc-100">
                  {entry.name || "Unnamed peer"}
                </p>
                <p className="truncate font-mono text-xs text-zinc-500">{entry.destination}</p>
              </div>
              <button
                type="button"
                disabled={busy}
                onClick={() =>
                  void run(
                    async () => {
                      setAllowlist(await allowlistRemove(entry.destination));
                    },
                    "Destination removed from the allowlist.",
                  )
                }
                className="shrink-0 rounded-full border border-red-800 px-3 py-1 text-xs text-red-400 hover:bg-red-950 disabled:opacity-50"
              >
                Remove
              </button>
            </li>
          ))}
        </ul>
      </section>
    </div>
  );
}
