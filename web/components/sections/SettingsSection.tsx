// SettingsSection.tsx — node settings: file acceptance / read receipts,
// rename the node, announce, your public profile, and per-peer / per-group
// disappearing-message defaults. Built on api.ts getSettings /
// updateSetting / renameNode / announce / getOwnProfile / setOwnProfile /
// getDisappearingDefaults / setDisappearingDefault.

"use client";

import { useEffect, useState } from "react";
import { useTauContext } from "@/components/chrome/TauContext";
import {
  announce,
  getDisappearingDefaults,
  getOwnProfile,
  getSettings,
  renameNode,
  setDisappearingDefault,
  setOwnProfile,
  updateSetting,
} from "@/lib/api";
import type { NodeSettings, OwnProfile } from "@/lib/types";

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
  const [profile, setProfile] = useState<OwnProfile | null>(null);
  const [profileName, setProfileName] = useState("");
  const [profileBio, setProfileBio] = useState("");
  const [profileAvatar, setProfileAvatar] = useState("");
  const [profileLinks, setProfileLinks] = useState("");
  const [profileSeeded, setProfileSeeded] = useState(false);
  const [disappearing, setDisappearing] = useState<Record<string, number>>({});
  const [ddTarget, setDdTarget] = useState("");
  const [ddSeconds, setDdSeconds] = useState<string>("");

  useEffect(() => {
    void (async () => {
      try {
        const [s, dd, p] = await Promise.all([
          getSettings(),
          getDisappearingDefaults(),
          getOwnProfile(),
        ]);
        setSettings(s);
        setDisappearing({ ...dd.peers, ...dd.groups });
        setProfile(p);
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

  if (profile && !profileSeeded) {
    setProfileSeeded(true);
    setProfileName(profile.name);
    setProfileBio(profile.bio);
    setProfileAvatar(profile.avatar ?? "");
    setProfileLinks(profile.links.join("\n"));
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
          This node&apos;s name, what it accepts, the profile peers see, and how long
          messages live.
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

      <section className="border-b border-zinc-800 px-3 py-4 sm:px-4" aria-label="Your profile">
        <h2 className="text-sm font-bold text-zinc-100">Your profile</h2>
        <p className="mt-1 text-xs leading-relaxed text-zinc-500">
          The signed public card peers see — display name, bio, avatar and links.
          Saving publishes a new version, which reaches peers at the next announce.
        </p>
        {loading && <p className="py-3 text-sm text-zinc-500">Loading profile…</p>}
        {profile && (
          <p className="mt-2 text-xs text-zinc-500">
            {profile.published
              ? `Published as version ${profile.version}.`
              : "Not published yet — peers see the node name."}
          </p>
        )}
        {profile && (
          <form
            className="mt-3 flex flex-col gap-2"
            onSubmit={(e) => {
              e.preventDefault();
              const links = profileLinks
                .split("\n")
                .map((line) => line.trim())
                .filter((line) => line.length > 0);
              void run(
                async () => {
                  const { profile: saved } = await setOwnProfile({
                    name: profileName.trim(),
                    bio: profileBio,
                    avatar: profileAvatar.trim(),
                    links,
                  });
                  setProfile(saved);
                  setProfileName(saved.name);
                  setProfileBio(saved.bio);
                  setProfileAvatar(saved.avatar ?? "");
                  setProfileLinks(saved.links.join("\n"));
                },
                "Profile saved.",
              );
            }}
          >
            <label className="sr-only" htmlFor="profile-name">
              Profile display name
            </label>
            <input
              id="profile-name"
              value={profileName}
              onChange={(e) => setProfileName(e.target.value)}
              placeholder="Display name"
              disabled={busy}
              aria-label="Profile display name"
              className="min-w-0 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
            />
            <label className="sr-only" htmlFor="profile-bio">
              Profile bio
            </label>
            <input
              id="profile-bio"
              value={profileBio}
              onChange={(e) => setProfileBio(e.target.value)}
              placeholder="Short bio (at most 280 characters)"
              maxLength={280}
              disabled={busy}
              aria-label="Profile bio"
              className="min-w-0 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
            />
            <label className="sr-only" htmlFor="profile-avatar">
              Profile avatar hash
            </label>
            <input
              id="profile-avatar"
              value={profileAvatar}
              onChange={(e) => setProfileAvatar(e.target.value)}
              placeholder="Avatar media hash — 32 or 64 hex characters (optional)"
              disabled={busy}
              aria-label="Profile avatar hash"
              className="min-w-0 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 font-mono text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
            />
            <label className="sr-only" htmlFor="profile-links">
              Profile links
            </label>
            <textarea
              id="profile-links"
              value={profileLinks}
              onChange={(e) => setProfileLinks(e.target.value)}
              placeholder={"Links — one https:// URL per line (up to 3)"}
              rows={3}
              disabled={busy}
              aria-label="Profile links"
              className="min-w-0 rounded-2xl border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
            />
            <button
              type="submit"
              disabled={busy}
              className="self-start rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
            >
              Save profile
            </button>
          </form>
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
            if (!ddTarget) return setError("Pick a peer or group first.");
            const seconds = ddSeconds === "" ? null : Number(ddSeconds);
            const [kind, key] = ddTarget.split(":", 2);
            void run(
              async () => {
                await setDisappearingDefault(
                  kind === "group" ? { group: key } : { peer: key },
                  seconds,
                );
                const dd = await getDisappearingDefaults();
                setDisappearing({ ...dd.peers, ...dd.groups });
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
            aria-label="Peer or group"
            className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none"
          >
            <option value="">Pick a peer or group…</option>
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
            const label = peer ? peer.alias || peer.name : group ? `Group: ${group.name}` : key;
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
