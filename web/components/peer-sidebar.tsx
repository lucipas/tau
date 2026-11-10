"use client";

import { useState, type FormEvent } from "react";

import type { BridgeInfo, NodeInfo, Peer, TauGroup } from "@/lib/types";
import type { PingState } from "@/lib/use-tau";
import {
  formatAge,
  formatRtt,
  formatUptime,
  shortHash,
} from "@/lib/format";
import { useNow } from "@/lib/use-now";

import { CopyButton, PingIcon, SectionLabel, SignalIcon } from "./ui";

export function PeerSidebar({
  node,
  peers,
  groups,
  bridge,
  pings,
  unread,
  selected,
  onPing,
  onSelectPeer,
  onSetAlias,
}: {
  node: NodeInfo | null;
  peers: Peer[];
  groups: TauGroup[];
  bridge: BridgeInfo | null;
  pings: Record<string, PingState>;
  unread: Record<string, number>;
  selected: string;
  onPing: (destination: string) => void;
  onSelectPeer: (destination: string) => void;
  onSetAlias: (destination: string, alias: string | null) => Promise<Peer>;
}) {
  const now = useNow();
  const [editingAlias, setEditingAlias] = useState<string | null>(null);
  const [aliasDraft, setAliasDraft] = useState("");
  const [aliasError, setAliasError] = useState<string | null>(null);
  const [aliasSaving, setAliasSaving] = useState(false);

  const startAliasEdit = (peer: Peer) => {
    setEditingAlias(peer.destination);
    setAliasDraft(peer.alias ?? "");
    setAliasError(null);
  };

  const saveAlias = async (event: FormEvent, peer: Peer) => {
    event.preventDefault();
    const alias = aliasDraft.trim();
    // Saving a blank label for a peer with no alias is a no-op, not a
    // trip to the server (which would 400: nothing to clear).
    if (alias === "" && !peer.alias) {
      setEditingAlias(null);
      return;
    }
    setAliasSaving(true);
    setAliasError(null);
    try {
      await onSetAlias(peer.destination, alias === "" ? null : alias);
      setEditingAlias(null);
    } catch (error) {
      setAliasError(
        error instanceof Error ? error.message : "could not save alias",
      );
    } finally {
      setAliasSaving(false);
    }
  };

  const clearAlias = async (peer: Peer) => {
    setAliasSaving(true);
    setAliasError(null);
    try {
      await onSetAlias(peer.destination, null);
      setEditingAlias(null);
    } catch (error) {
      setAliasError(
        error instanceof Error ? error.message : "could not clear alias",
      );
    } finally {
      setAliasSaving(false);
    }
  };

  return (
    <aside className="flex flex-col gap-4 lg:sticky lg:top-[86px] lg:self-start">
      <section className="rounded-2xl border border-line/80 bg-panel/60 p-4 shadow-[0_20px_60px_-40px_rgba(0,0,0,0.9)]">
        <div className="flex items-center justify-between">
          <SectionLabel>this node</SectionLabel>
          {node ? (
            <span className="inline-flex items-center gap-1.5 rounded-full border border-signal/25 bg-signal/5 px-2 py-0.5 font-mono text-[10px] text-signal">
              <span className="h-1.5 w-1.5 rounded-full bg-signal" />
              online
            </span>
          ) : null}
        </div>

        {node ? (
          <>
            <h2 className="mt-2 truncate font-mono text-xl font-semibold text-zinc-100">
              {node.name}
            </h2>
            <div className="mt-3 rounded-xl border border-line/70 bg-void/50 p-3">
              <p className="font-mono text-[10px] uppercase tracking-[0.18em] text-mist">
                destination
              </p>
              <p className="mt-1 break-all font-mono text-[11px] leading-relaxed text-zinc-300">
                {node.destination}
              </p>
              <div className="mt-2 flex items-center gap-2">
                <CopyButton value={node.destination} label="copy hash" />
                <CopyButton
                  value={`tau send --to ${node.name}`}
                  label="send cmd"
                />
              </div>
            </div>

            <dl className="mt-3 grid grid-cols-2 gap-x-3 gap-y-2 text-[11px]">
              <div>
                <dt className="text-mist">uptime</dt>
                <dd className="font-mono text-zinc-300">
                  {formatUptime(node.uptime)}
                </dd>
              </div>
              <div>
                <dt className="text-mist">tau / rns</dt>
                <dd className="font-mono text-zinc-300">
                  {node.version} / {node.rns_version}
                </dd>
              </div>
              <div>
                <dt className="text-mist">peers</dt>
                <dd className="font-mono text-zinc-300">{peers.length}</dd>
              </div>
              <div>
                <dt className="text-mist">protocol</dt>
                <dd className="font-mono text-zinc-300">v{node.protocol}</dd>
              </div>
            </dl>

            <div className="mt-3 flex flex-wrap gap-1.5">
              {node.interfaces.length === 0 ? (
                <span className="rounded border border-line/70 px-2 py-0.5 font-mono text-[10px] text-mist">
                  no interfaces
                </span>
              ) : (
                node.interfaces.map((iface) => (
                  <span
                    key={iface.name}
                    className="inline-flex items-center gap-1.5 rounded border border-line/70 bg-void/40 px-2 py-0.5 font-mono text-[10px] text-mist"
                  >
                    <span
                      className={`h-1.5 w-1.5 rounded-full ${
                        iface.online ? "bg-signal" : "bg-zinc-600"
                      }`}
                    />
                    {iface.name}
                  </span>
                ))
              )}
            </div>

            {bridge ? (
              <div className="mt-3 border-t border-line/60 pt-3">
                <div className="flex items-center justify-between gap-2">
                  <SectionLabel>local api</SectionLabel>
                  <CopyButton value={bridge.url} label="url" />
                </div>
                <p className="mt-1.5 font-mono text-[10px] leading-relaxed text-mist">
                  GET /api/messages · GET /api/events
                  <br />
                  POST /api/messages · POST /api/messages/read · POST /api/ping
                </p>
              </div>
            ) : null}
          </>
        ) : (
          <div className="mt-3 animate-pulse space-y-2">
            <div className="h-5 w-2/3 rounded bg-line/60" />
            <div className="h-16 rounded bg-line/40" />
          </div>
        )}
      </section>

      <section className="rounded-2xl border border-line/80 bg-panel/60 p-4">
        <div className="flex items-center justify-between">
          <SectionLabel>peers</SectionLabel>
          <span className="rounded-full border border-line bg-void/60 px-2 py-0.5 font-mono text-[10px] text-mist">
            {peers.length}
          </span>
        </div>

        {peers.length === 0 ? (
          <div className="mt-3 rounded-xl border border-dashed border-line/80 p-3">
            <p className="text-xs text-zinc-300">No announces heard yet.</p>
            <p className="mt-1 text-[11px] leading-relaxed text-mist">
              Keep this node running, then start another one and announce. Tau
              stores peers from signed Reticulum announces.
            </p>
          </div>
        ) : (
          <ul className="mt-2 -mx-1 max-h-[46vh] space-y-1 overflow-y-auto px-1 py-1 thin-scroll">
            {peers.map((peer) => {
              const ping = pings[peer.destination];
              const age = now
                ? Math.max(0, now / 1000 - peer.last_seen)
                : peer.age;
              const isSelected = peer.destination === selected;
              const unreadCount = unread[peer.destination] ?? 0;
              const displayName = peer.alias || peer.name || "unknown";
              const isEditingAlias = editingAlias === peer.destination;
              return (
                <li key={peer.destination}>
                  <div
                    className={`group rounded-xl border px-2.5 py-2 transition ${
                      isSelected
                        ? "border-signal/40 bg-signal/5"
                        : "border-transparent hover:border-line hover:bg-void/40"
                    }`}
                  >
                    <div className="flex items-center gap-2.5">
                      <button
                        type="button"
                        onClick={() => onSelectPeer(peer.destination)}
                        className="flex min-w-0 flex-1 items-center gap-2.5 text-left focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60"
                        title={`Message ${displayName} (${peer.destination})`}
                      >
                        <span className="grid h-8 w-8 shrink-0 place-items-center rounded-lg border border-line bg-void/60 font-mono text-xs uppercase text-signal">
                          {displayName.slice(0, 1) || "?"}
                        </span>
                        <span className="min-w-0">
                          <span className="flex items-center gap-1.5">
                            <span className="block truncate text-xs font-medium text-zinc-200">
                              {displayName}
                            </span>
                            {peer.alias ? (
                              <span
                                className="ml-1.5 shrink-0 rounded-full border border-line/70 bg-void/60 px-1.5 py-px font-mono text-[9px] uppercase tracking-wide text-mist"
                                title={`Announced as ${peer.name}`}
                              >
                                alias
                              </span>
                            ) : null}
                            {peer.verified ? (
                              <span
                                className="ml-1.5 rounded-full border border-signal/40 bg-signal/10 px-1.5 py-px text-[10px] text-signal"
                                title="Safety number compared out of band — this key is confirmed theirs"
                              >
                                ✓ verified
                              </span>
                            ) : null}
                            {peer.blocked ? (
                              <span
                                className="ml-1.5 rounded-full border border-rose-400/40 bg-rose-400/10 px-1.5 py-px font-mono text-[9px] uppercase tracking-wide text-rose-300"
                                title="Blocked: their DMs are dropped and their Tan posts hidden"
                              >
                                blocked
                              </span>
                            ) : null}
                            {unreadCount > 0 ? (
                              <span
                                className="shrink-0 rounded-full bg-signal px-1.5 py-px font-mono text-[10px] font-semibold text-void"
                                title={`${unreadCount} unread message${unreadCount === 1 ? "" : "s"}`}
                              >
                                {unreadCount > 99 ? "99+" : unreadCount}
                              </span>
                            ) : null}
                          </span>
                          <span className="block truncate font-mono text-[10px] text-mist">
                            {shortHash(peer.destination, 10)} · {peer.hops} hop
                            {peer.hops === 1 ? "" : "s"} · {formatAge(age)}
                          </span>
                          {peer.alias ? (
                            <span className="block truncate font-mono text-[10px] text-mist">
                              announced as {peer.name}
                            </span>
                          ) : null}
                          {peer.key_change_at ? (
                            <span
                              className="block truncate font-mono text-[10px] text-amber-300"
                              title="This destination presented a different public key; the recorded key was kept"
                            >
                              ⚠ key changed — re-verify before trusting
                            </span>
                          ) : null}
                          {peer.lxmf ? (
                            <span
                              className="block truncate font-mono text-[10px] text-mist"
                              title={`LXMF delivery ${peer.lxmf}`}
                            >
                              lxmf {shortHash(peer.lxmf, 10)}
                            </span>
                          ) : null}
                        </span>
                      </button>
                      <button
                        type="button"
                        onClick={() => startAliasEdit(peer)}
                        aria-label={`Set alias for ${displayName}`}
                        title="Set a local alias for this peer"
                        className="grid h-7 w-7 shrink-0 place-items-center rounded-md border border-line/70 text-mist transition hover:border-signal/50 hover:text-signal"
                      >
                        ✎
                      </button>
                      <button
                        type="button"
                        onClick={() => onPing(peer.destination)}
                        disabled={ping?.pending}
                        aria-label={`Ping ${displayName}`}
                        title="Ping peer"
                        className="grid h-7 w-7 shrink-0 place-items-center rounded-md border border-line/70 text-mist transition hover:border-signal/50 hover:text-signal disabled:opacity-50"
                      >
                        <PingIcon
                          className={`h-3.5 w-3.5 ${
                            ping?.pending ? "animate-pulse" : ""
                          }`}
                        />
                      </button>
                    </div>
                    {isEditingAlias ? (
                      <form
                        className="mt-2 pl-[42px]"
                        onSubmit={(event) => void saveAlias(event, peer)}
                      >
                        <label className="sr-only" htmlFor={`alias-${peer.destination}`}>
                          Local alias for {displayName}
                        </label>
                        <input
                          id={`alias-${peer.destination}`}
                          type="text"
                          value={aliasDraft}
                          maxLength={48}
                          autoFocus
                          placeholder={peer.name || "alias"}
                          onChange={(event) =>
                            setAliasDraft(event.target.value)
                          }
                          className="w-full rounded-md border border-line bg-void/60 px-2 py-1 text-xs text-zinc-100 placeholder:text-mist focus:border-signal/60 focus:outline-none"
                        />
                        <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                          <button
                            type="submit"
                            disabled={aliasSaving}
                            className="rounded-md border border-signal/50 bg-signal/10 px-2 py-0.5 text-[11px] font-medium text-signal transition hover:bg-signal/20 disabled:opacity-50"
                          >
                            {aliasSaving ? "saving…" : "save"}
                          </button>
                          <button
                            type="button"
                            disabled={aliasSaving}
                            onClick={() => setEditingAlias(null)}
                            className="rounded-md border border-line/70 px-2 py-0.5 text-[11px] text-mist transition hover:text-zinc-200 disabled:opacity-50"
                          >
                            cancel
                          </button>
                          {peer.alias ? (
                            <button
                              type="button"
                              disabled={aliasSaving}
                              onClick={() => void clearAlias(peer)}
                              className="rounded-md border border-line/70 px-2 py-0.5 text-[11px] text-mist transition hover:text-rose-300 disabled:opacity-50"
                            >
                              clear alias
                            </button>
                          ) : null}
                        </div>
                        {aliasError ? (
                          <p className="mt-1.5 text-[11px] leading-relaxed text-rose-300">
                            {aliasError}
                          </p>
                        ) : null}
                      </form>
                    ) : null}
                    {ping ? (
                      <p
                        className={`mt-1 pl-[42px] font-mono text-[10px] ${
                          ping.error
                            ? "text-rose-300"
                            : ping.pending
                              ? "text-mist"
                              : "text-signal"
                        }`}
                      >
                        {ping.pending
                          ? "waiting for pong…"
                          : ping.error
                            ? ping.error
                            : `pong ${formatRtt(ping.rtt_ms)}`}
                      </p>
                    ) : null}
                  </div>
                </li>
              );
            })}
          </ul>
        )}
      </section>

      <section className="rounded-2xl border border-line/80 bg-panel/60 p-4">
        <div className="flex items-center justify-between">
          <SectionLabel>groups</SectionLabel>
          <span className="rounded-full border border-line bg-void/60 px-2 py-0.5 font-mono text-[10px] text-mist">
            {groups.length}
          </span>
        </div>

        {groups.length === 0 ? (
          <p className="mt-3 text-[11px] leading-relaxed text-mist">
            No group chats yet. Create one with{" "}
            <code className="text-zinc-300">
              tau group create NAME --peer ...
            </code>
            ; composing from the console comes later.
          </p>
        ) : (
          <ul className="mt-2 space-y-2">
            {groups.map((group) => (
              <li
                key={group.id}
                className="rounded-xl border border-line/70 bg-void/40 px-2.5 py-2"
              >
                <p className="truncate text-xs font-medium text-zinc-200">
                  {group.name}
                </p>
                <p className="mt-0.5 truncate font-mono text-[10px] text-mist">
                  {group.members.map((member) => member.name).join(", ")}
                </p>
              </li>
            ))}
          </ul>
        )}
      </section>

      <div className="hidden items-start gap-2 rounded-2xl border border-line/60 bg-panel/40 p-3 text-[11px] leading-relaxed text-mist lg:flex">
        <SignalIcon className="mt-0.5 h-4 w-4 shrink-0 text-signal/70" />
        <p>
          Everything that reaches this node is mirrored to the local API, so
          scripts on this machine can consume the mesh feed too.
        </p>
      </div>
    </aside>
  );
}
