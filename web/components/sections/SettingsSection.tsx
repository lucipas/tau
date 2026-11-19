// SettingsSection.tsx — node settings: file acceptance / read receipts,
// rename the node, announce, quiet hours, and per-peer / per-group /
// per-channel disappearing-message defaults. Built on api.ts getSettings /
// updateSetting / renameNode / announce / getQuietHours / setQuietHours /
// getDisappearingDefaults / setDisappearingDefault / getChannels.

"use client";

import { useEffect, useState } from "react";
import { useTauContext } from "@/components/chrome/TauContext";
import {
  announce,
  getChannels,
  getDisappearingDefaults,
  getQuietHours,
  getSettings,
  renameNode,
  setDisappearingDefault,
  setQuietHours,
  updateSetting,
} from "@/lib/api";
import type { Channel, NodeSettings, QuietHoursSettings } from "@/lib/types";

const EXPIRY_OPTIONS: { label: string; seconds: number | null }[] = [
  { label: "No disappearing default", seconds: null },
  { label: "5 minutes", seconds: 5 * 60 },
  { label: "1 hour", seconds: 3600 },
  { label: "1 day", seconds: 86400 },
  { label: "1 week", seconds: 7 * 86400 },
  { label: "30 days", seconds: 30 * 86400 },
];

function safeTau() {
  try {
    return useTauContext();
  } catch {
    return null;
  }
}

export default function SettingsSection() {
  const tau = safeTau();
  const [settings, setSettings] = useState<NodeSettings | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [nameSeeded, setNameSeeded] = useState(false);
  const [quietHours, setQuietHoursState] = useState<QuietHoursSettings | null>(null);
  const [qhStart, setQhStart] = useState("22:00");
  const [qhEnd, setQhEnd] = useState("07:00");
  const [qhEnabled, setQhEnabled] = useState(false);
  const [qhSeeded, setQhSeeded] = useState(false);
  const [disappearing, setDisappearing] = useState<Record<string, number>>({});
  const [channels, setChannels] = useState<Channel[]>([]);
  const [ddTarget, setDdTarget] = useState("");
  const [ddSeconds, setDdSeconds] = useState<string>("");

  useEffect(() => {
    void (async () => {
      try {
        const [s, dd, qh, ch] = await Promise.all([
          getSettings(),
          getDisappearingDefaults(),
          getQuietHours(),
          getChannels(),
        ]);
        setSettings(s);
        setDisappearing({ ...dd.peers, ...dd.groups, ...dd.channels });
        setQuietHoursState(qh);
        setChannels(ch);
        setError(null);
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  if (tau?.node && !nameSeeded) {
    setNameSeeded(true);
    setName(tau.node.name);
  }

  if (quietHours && !qhSeeded) {
    setQhSeeded(true);
    setQhStart(quietHours.start);
    setQhEnd(quietHours.end);
    setQhEnabled(quietHours.enabled);
  }

  async function run(fn: () => Promise<unknown>, okMsg: string) {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await fn();
      setNotice(okMsg);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  const peers = tau?.peers ?? [];
  const groups = tau?.groups ?? [];

  return (
    <div className="mx-auto w-full max-w-[600px]">
      <header className="sticky top-0 z-10 border-b border-zinc-800 bg-zinc-950/95 px-3 py-3 backdrop-blur sm:px-4">
        <h1 className="text-lg font-bold text-zinc-50">
          <span className="mr-2 text-cyan-400">τ</span>Settings
        </h1>
        <p className="mt-0.5 text-xs text-zinc-500">
          This node&apos;s name, what it accepts, and how long messages live.
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

      <section className="border-b border-zinc-800 px-3 py-4 sm:px-4" aria-label="Node identity">
        <h2 className="text-sm font-bold text-zinc-100">Node identity</h2>
        {tau?.node && (
          <dl className="mt-2 text-sm text-zinc-400">
            <div className="flex gap-2">
              <dt className="shrink-0 text-zinc-500">Destination</dt>
              <dd className="min-w-0 truncate font-mono text-xs text-zinc-300">
                {tau.node.destination}
              </dd>
            </div>
            <div className="flex gap-2">
              <dt className="shrink-0 text-zinc-500">Protocol</dt>
              <dd>
                v{tau.node.protocol} · tau {tau.node.version} · RNS {tau.node.rns_version}
              </dd>
            </div>
          </dl>
        )}
        <form
          className="mt-3 flex gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            const next = name.trim();
            if (!next) return setError("The node needs a name.");
            void run(
              async () => {
                await renameNode(next);
                await tau?.refresh();
              },
              `Node renamed to “${next}”.`,
            );
          }}
        >
          <label className="sr-only" htmlFor="node-name">
            Node name
          </label>
          <input
            id="node-name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="Node name"
            disabled={busy}
            className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
          />
          <button
            type="submit"
            disabled={busy || !name.trim()}
            className="shrink-0 rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
          >
            Rename
          </button>
        </form>
        <button
          type="button"
          disabled={busy}
          onClick={() => void run(async () => announce(), "Announce sent to the mesh.")}
          className="mt-3 rounded-full border border-zinc-700 px-4 py-1.5 text-sm font-semibold text-zinc-200 hover:bg-zinc-900 disabled:opacity-50"
        >
          📡 Announce now
        </button>
      </section>

      <section className="border-b border-zinc-800 px-3 py-4 sm:px-4" aria-label="Node settings">
        <h2 className="text-sm font-bold text-zinc-100">Node settings</h2>
        {loading && <p className="py-3 text-sm text-zinc-500">Loading settings…</p>}
        {settings && (
          <div className="mt-2 flex flex-col gap-3">
            <label className="flex items-start gap-3 rounded-lg border border-zinc-800 bg-zinc-900 px-3 py-3">
              <input
                type="checkbox"
                checked={settings.accept_files}
                disabled={busy}
                onChange={(e) =>
                  void run(
                    async () => {
                      setSettings(await updateSetting("accept_files", e.target.checked));
                    },
                    e.target.checked
                      ? "File acceptance turned on."
                      : "File acceptance turned off.",
                  )
                }
                className="mt-0.5"
              />
              <span>
                <span className="block text-sm font-semibold text-zinc-100">
                  Accept incoming files
                </span>
                <span className="block text-xs text-zinc-500">
                  Peers may send files up to 10 MiB to this node.
                </span>
              </span>
            </label>
            <label className="flex items-start gap-3 rounded-lg border border-zinc-800 bg-zinc-900 px-3 py-3">
              <input
                type="checkbox"
                checked={settings.send_read_receipts}
                disabled={busy}
                onChange={(e) =>
                  void run(
                    async () => {
                      setSettings(await updateSetting("send_read_receipts", e.target.checked));
                    },
                    e.target.checked ? "Read receipts turned on." : "Read receipts turned off.",
                  )
                }
                className="mt-0.5"
              />
              <span>
                <span className="block text-sm font-semibold text-zinc-100">
                  Send read receipts
                </span>
                <span className="block text-xs text-zinc-500">
                  Let senders know when you&apos;ve read their messages.
                </span>
              </span>
            </label>
          </div>
        )}
      </section>

      <section className="border-b border-zinc-800 px-3 py-4 sm:px-4" aria-label="Quiet hours">
        <h2 className="text-sm font-bold text-zinc-100">Quiet hours</h2>
        <p className="mt-1 text-xs leading-relaxed text-zinc-500">
          A daily do-not-disturb window: while it is quiet, unread badges stay dark
          and no message carries the unread marker. The counts come back when the
          window ends.
        </p>
        {loading && <p className="py-3 text-sm text-zinc-500">Loading quiet hours…</p>}
        {quietHours && (
          <>
            <p className="mt-2 text-sm text-zinc-300" role="status">
              {quietHours.active
                ? `Quiet now until ${quietHours.end}.`
                : quietHours.enabled
                  ? `On ${quietHours.start}–${quietHours.end} every day.`
                  : "Off — badges light as messages arrive."}
            </p>
            <form
              className="mt-3 flex flex-wrap items-center gap-2"
              onSubmit={(e) => {
                e.preventDefault();
                if (qhStart === qhEnd)
                  return setError("Quiet hours start and end must differ.");
                void run(
                  async () => {
                    const { quiet_hours } = await setQuietHours({
                      enabled: qhEnabled,
                      start: qhStart,
                      end: qhEnd,
                    });
                    setQuietHoursState(quiet_hours);
                  },
                  qhEnabled
                    ? `Quiet hours on ${qhStart}–${qhEnd} every day.`
                    : "Quiet hours turned off.",
                );
              }}
            >
              <label className="flex items-center gap-2 text-sm text-zinc-200">
                <input
                  type="checkbox"
                  checked={qhEnabled}
                  disabled={busy}
                  onChange={(e) => setQhEnabled(e.target.checked)}
                />
                Enable quiet hours
              </label>
              <label className="flex items-center gap-2 text-sm text-zinc-200">
                From
                <input
                  type="time"
                  value={qhStart}
                  disabled={busy}
                  onChange={(e) => setQhStart(e.target.value)}
                  aria-label="Quiet hours start"
                  className="rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none focus:border-cyan-500 disabled:opacity-50"
                />
              </label>
              <label className="flex items-center gap-2 text-sm text-zinc-200">
                To
                <input
                  type="time"
                  value={qhEnd}
                  disabled={busy}
                  onChange={(e) => setQhEnd(e.target.value)}
                  aria-label="Quiet hours end"
                  className="rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none focus:border-cyan-500 disabled:opacity-50"
                />
              </label>
              <button
                type="submit"
                disabled={busy}
                className="rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
              >
                Save quiet hours
              </button>
            </form>
          </>
        )}
      </section>

      <section className="px-3 py-4 sm:px-4" aria-label="Disappearing message defaults">
        <h2 className="text-sm font-bold text-zinc-100">Disappearing defaults</h2>
        <p className="mt-1 text-xs leading-relaxed text-zinc-500">
          New messages in a conversation disappear after the default you set here. The
          composer can still override it per message.
        </p>
        <form
          className="mt-3 flex flex-wrap gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            if (!ddTarget) return setError("Pick a peer, group or channel first.");
            const seconds = ddSeconds === "" ? null : Number(ddSeconds);
            const [kind, key] = ddTarget.split(":", 2);
            void run(
              async () => {
                await setDisappearingDefault(
                  kind === "group"
                    ? { group: key }
                    : kind === "channel"
                      ? { channel: key }
                      : { peer: key },
                  seconds,
                );
                const dd = await getDisappearingDefaults();
                setDisappearing({ ...dd.peers, ...dd.groups, ...dd.channels });
              },
              seconds === null
                ? "Disappearing default cleared."
                : "Disappearing default saved.",
            );
          }}
        >
          <select
            value={ddTarget}
            onChange={(e) => setDdTarget(e.target.value)}
            aria-label="Peer, group or channel"
            className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none"
          >
            <option value="">Pick a peer, group or channel…</option>
            {peers.map((p) => (
              <option key={p.destination} value={`peer:${p.destination}`}>
                {p.alias || p.name}
              </option>
            ))}
            {groups.map((g) => (
              <option key={g.id} value={`group:${g.id}`}>
                Group: {g.name}
              </option>
            ))}
            {channels.map((c) => (
              <option key={c.channel} value={`channel:${c.channel}`}>
                Channel: #{c.channel}
              </option>
            ))}
          </select>
          <select
            value={ddSeconds}
            onChange={(e) => setDdSeconds(e.target.value)}
            aria-label="Disappearing time"
            className="rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none"
          >
            {EXPIRY_OPTIONS.map((o) => (
              <option key={o.label} value={o.seconds === null ? "" : String(o.seconds)}>
                {o.label}
              </option>
            ))}
          </select>
          <button
            type="submit"
            disabled={busy || !ddTarget}
            className="rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
          >
            Save default
          </button>
        </form>
        {Object.keys(disappearing).length === 0 && (
          <p className="py-3 text-sm text-zinc-500">No disappearing defaults set.</p>
        )}
        <ul className="mt-3 flex flex-col gap-2">
          {Object.entries(disappearing).map(([key, seconds]) => {
            const peer = peers.find((p) => p.destination === key);
            const group = groups.find((g) => g.id === key);
            const channel = channels.find((c) => c.channel === key);
            const label = peer
              ? peer.alias || peer.name
              : group
                ? `Group: ${group.name}`
                : channel
                  ? `Channel: #${channel.channel}`
                  : key;
            const option = EXPIRY_OPTIONS.find((o) => o.seconds === seconds);
            return (
              <li
                key={key}
                className="flex items-center justify-between gap-2 rounded-lg border border-zinc-800 bg-zinc-900 px-3 py-2 text-sm"
              >
                <span className="min-w-0 truncate text-zinc-200">{label}</span>
                <span className="shrink-0 text-xs text-zinc-500">
                  {option?.label ?? `${seconds}s`}
                </span>
              </li>
            );
          })}
        </ul>
      </section>
    </div>
  );
}
