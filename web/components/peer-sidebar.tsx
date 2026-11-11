"use client";

import { useEffect, useRef, useState } from "react";

import type {
  BridgeInfo,
  ChangeGroupMembersResponse,
  NodeInfo,
  Peer,
  SocialInfo,
  TauGroup,
  VerifyState,
} from "@/lib/types";
import type { BlockState, PingState } from "@/lib/use-tau";
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
  social,
  pings,
  blocks,
  verifications,
  unread,
  selected,
  onPing,
  onBlock,
  onUnblock,
  onVerify,
  onShowSafetyNumber,
  onDismissVerification,
  onSelectPeer,
  onCreateGroup,
  onDeleteGroup,
  onAddGroupMembers,
  onRemoveGroupMembers,
}: {
  node: NodeInfo | null;
  peers: Peer[];
  groups: TauGroup[];
  bridge: BridgeInfo | null;
  social: SocialInfo | null;
  pings: Record<string, PingState>;
  blocks: Record<string, BlockState>;
  verifications: Record<string, VerifyState>;
  unread: Record<string, number>;
  selected: string;
  onPing: (destination: string) => void;
  onBlock: (destination: string) => void;
  onUnblock: (destination: string) => void;
  onVerify: (destination: string) => void;
  onShowSafetyNumber: (destination: string) => void;
  onDismissVerification: (destination: string) => void;
  onSelectPeer: (destination: string) => void;
  onCreateGroup?: (name: string, members: string[]) => Promise<TauGroup>;
  onDeleteGroup?: (group: string) => Promise<{ id: string; name: string }>;
  onAddGroupMembers?: (
    group: string,
    members: string[],
  ) => Promise<ChangeGroupMembersResponse>;
  onRemoveGroupMembers?: (
    group: string,
    members: string[],
  ) => Promise<ChangeGroupMembersResponse>;
}) {
  const now = useNow();
  const [showCreate, setShowCreate] = useState(false);
  const [groupName, setGroupName] = useState("");
  const [pickedMembers, setPickedMembers] = useState<string[]>([]);
  const [creating, setCreating] = useState(false);
  const [confirmingId, setConfirmingId] = useState<string | null>(null);
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [pickedAdd, setPickedAdd] = useState<string[]>([]);
  const [pickedRemove, setPickedRemove] = useState<string[]>([]);
  const [changingId, setChangingId] = useState<string | null>(null);
  const [groupError, setGroupError] = useState<string | null>(null);
  // Bumped whenever the member editor switches, so a change that
  // finishes later only touches the editor it was started from.
  const memberEditSession = useRef(0);
  const confirmTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (confirmTimer.current) clearTimeout(confirmTimer.current);
    };
  }, []);

  function armDeleteConfirm(id: string) {
    setGroupError(null);
    setConfirmingId(id);
    if (confirmTimer.current) clearTimeout(confirmTimer.current);
    confirmTimer.current = setTimeout(() => setConfirmingId(null), 5000);
  }

  async function confirmDelete(id: string) {
    if (!onDeleteGroup) return;
    if (confirmTimer.current) clearTimeout(confirmTimer.current);
    setConfirmingId(null);
    setDeletingId(id);
    setGroupError(null);
    try {
      await onDeleteGroup(id);
    } catch (caught) {
      setGroupError(caught instanceof Error ? caught.message : "delete failed");
    } finally {
      setDeletingId(null);
    }
  }

  function toggleEditMembers(id: string) {
    memberEditSession.current += 1;
    setGroupError(null);
    setPickedAdd([]);
    setPickedRemove([]);
    setEditingId((previous) => (previous === id ? null : id));
  }

  async function submitMemberChange(groupId: string, adding: boolean) {
    const apply = adding ? onAddGroupMembers : onRemoveGroupMembers;
    const picked = adding ? pickedAdd : pickedRemove;
    if (!apply || picked.length === 0) return;
    const session = memberEditSession.current;
    setChangingId(groupId);
    setGroupError(null);
    try {
      await apply(groupId, picked);
      // A roster change is a successor group with a new id, so the
      // panel belongs to the old id: close it on success, unless the
      // user has already moved on to another group's editor.
      if (memberEditSession.current === session) {
        setPickedAdd([]);
        setPickedRemove([]);
        setEditingId(null);
      }
    } catch (caught) {
      if (memberEditSession.current === session) {
        setGroupError(
          caught instanceof Error ? caught.message : "change failed",
        );
      }
    } finally {
      setChangingId((current) => (current === groupId ? null : current));
    }
  }

  async function submitCreate(event: React.FormEvent) {
    event.preventDefault();
    if (!onCreateGroup) return;
    const name = groupName.trim();
    if (!name || pickedMembers.length === 0) return;
    setCreating(true);
    setGroupError(null);
    try {
      await onCreateGroup(name, pickedMembers);
      setGroupName("");
      setPickedMembers([]);
      setShowCreate(false);
    } catch (caught) {
      setGroupError(caught instanceof Error ? caught.message : "create failed");
    } finally {
      setCreating(false);
    }
  }
  // Blocking is the one peer action that silences someone, so it takes
  // two clicks: arm on the first, confirm on the second. Unblocking is
  // directly reversible and needs only one.
  const [confirmBlock, setConfirmBlock] = useState<string | null>(null);

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

            {node.mailbox || node.lxmf || social ? (
              <div className="mt-3 border-t border-line/60 pt-3">
                <SectionLabel>stores</SectionLabel>
                <dl className="mt-2 space-y-1.5 text-[11px]">
                  {node.mailbox ? (
                    <div className="flex items-baseline justify-between gap-2">
                      <dt className="text-mist">mailbox</dt>
                      <dd
                        className="font-mono text-zinc-300"
                        title={`${node.mailbox.seen} seen · ${node.mailbox.sent} sent · ${node.mailbox.receipts} receipts`}
                      >
                        {node.mailbox.outbox} queued · {node.mailbox.held}{" "}
                        held
                      </dd>
                    </div>
                  ) : null}
                  {node.lxmf ? (
                    <div className="flex items-baseline justify-between gap-2">
                      <dt className="text-mist">lxmf</dt>
                      <dd className="font-mono text-zinc-300">
                        {node.lxmf.enabled
                          ? `on · ${shortHash(node.lxmf.delivery, 10)}`
                          : "off"}
                      </dd>
                    </div>
                  ) : null}
                  {social ? (
                    <div className="flex items-baseline justify-between gap-2">
                      <dt className="text-mist">tan</dt>
                      <dd className="font-mono text-zinc-300">
                        {social.enabled
                          ? `${social.posts} post${social.posts === 1 ? "" : "s"} cached`
                          : "off"}
                      </dd>
                    </div>
                  ) : null}
                </dl>
              </div>
            ) : null}

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
              const block = blocks[peer.destination];
              const verification = verifications[peer.destination];
              const age = now
                ? Math.max(0, now / 1000 - peer.last_seen)
                : peer.age;
              const isSelected = peer.destination === selected;
              const unreadCount = unread[peer.destination] ?? 0;
              const armingBlock = confirmBlock === peer.destination;
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
                        title={`Message ${peer.name} (${peer.destination})`}
                      >
                        <span className="grid h-8 w-8 shrink-0 place-items-center rounded-lg border border-line bg-void/60 font-mono text-xs uppercase text-signal">
                          {peer.name.slice(0, 1) || "?"}
                        </span>
                        <span className="min-w-0">
                          <span className="flex items-center gap-1.5">
                            <span className="block truncate text-xs font-medium text-zinc-200">
                              {peer.name || "unknown"}
                            </span>
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
                      <div className="flex shrink-0 items-center gap-1">
                        <button
                          type="button"
                          onClick={() => onPing(peer.destination)}
                          disabled={ping?.pending}
                          aria-label={`Ping ${peer.name}`}
                          title="Ping peer"
                          className="grid h-7 w-7 place-items-center rounded-md border border-line/70 text-mist transition hover:border-signal/50 hover:text-signal disabled:opacity-50"
                        >
                          <PingIcon
                            className={`h-3.5 w-3.5 ${
                              ping?.pending ? "animate-pulse" : ""
                            }`}
                          />
                        </button>
                        <button
                          type="button"
                          onClick={() => {
                            if (peer.blocked) {
                              onUnblock(peer.destination);
                              return;
                            }
                            if (armingBlock) {
                              setConfirmBlock(null);
                              onBlock(peer.destination);
                              return;
                            }
                            setConfirmBlock(peer.destination);
                          }}
                          disabled={block?.pending}
                          aria-label={
                            peer.blocked
                              ? `Unblock ${peer.name}`
                              : armingBlock
                                ? `Confirm blocking ${peer.name}`
                                : `Block ${peer.name}`
                          }
                          title={
                            peer.blocked
                              ? `Unblock ${peer.name} — their DMs and Tan posts come back`
                              : armingBlock
                                ? `Click again to block ${peer.name}`
                                : `Block ${peer.name} — their DMs are dropped and their Tan posts hidden`
                          }
                          className={`grid h-7 w-7 place-items-center rounded-md border transition disabled:opacity-50 ${
                            peer.blocked || armingBlock
                              ? "border-rose-400/50 bg-rose-400/10 text-rose-300 hover:border-rose-400/70"
                              : "border-line/70 text-mist hover:border-rose-400/50 hover:text-rose-300"
                          }`}
                        >
                          <span aria-hidden="true" className="text-sm leading-none">
                            ⊘
                          </span>
                        </button>
                        <button
                          type="button"
                          onClick={() => onShowSafetyNumber(peer.destination)}
                          disabled={verification?.pending}
                          aria-label={`Verify ${peer.name}`}
                          title={
                            peer.key_change_at
                              ? "Re-verify — this peer presented a different key"
                              : "Verify this peer"
                          }
                          className={`shrink-0 rounded-md border px-2 py-1 font-mono text-[10px] transition disabled:opacity-50 ${
                            peer.key_change_at
                              ? "border-amber-300/50 text-amber-300 hover:border-amber-300"
                              : "border-line/70 text-mist hover:border-signal/50 hover:text-signal"
                          }`}
                        >
                          {verification?.pending ? "verifying…" : "Verify"}
                        </button>
                      </div>
                    </div>
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
                    {verification?.safetyNumber && !verification?.result ? (
                      <div className="mt-1 pl-[42px]">
                        <p className="font-mono text-[10px] leading-relaxed text-mist">
                          safety number — compare it with {peer.name} over
                          another channel; nothing is recorded until you
                          confirm it matches
                        </p>
                        <p className="mt-0.5 break-all font-mono text-[11px] leading-relaxed text-zinc-200">
                          {verification.safetyNumber}
                        </p>
                        <div className="mt-1 flex flex-wrap items-center gap-2">
                          <CopyButton
                            value={verification.safetyNumber}
                            label="copy number"
                          />
                          <button
                            type="button"
                            onClick={() => onVerify(peer.destination)}
                            disabled={verification.pending}
                            className="rounded-md border border-signal/50 px-2 py-1 font-mono text-[10px] text-signal transition hover:border-signal disabled:opacity-50"
                          >
                            Numbers match — mark verified
                          </button>
                          <button
                            type="button"
                            onClick={() =>
                              onDismissVerification(peer.destination)
                            }
                            className="rounded-md border border-line/70 px-2 py-1 font-mono text-[10px] text-mist transition hover:border-line hover:text-zinc-200"
                          >
                            Cancel
                          </button>
                        </div>
                      </div>
                    ) : null}
                    {verification?.result ? (
                      <div className="mt-1 pl-[42px]">
                        <p className="font-mono text-[10px] leading-relaxed text-mist">
                          verified — safety number matched with {peer.name}
                        </p>
                        <p className="mt-0.5 break-all font-mono text-[11px] leading-relaxed text-zinc-200">
                          {verification.result.safety_number}
                        </p>
                        <div className="mt-1 flex flex-wrap items-center gap-2">
                          <CopyButton
                            value={verification.result.safety_number}
                            label="copy number"
                          />
                          {verification.result.fingerprint_changed ? (
                            <span className="font-mono text-[10px] text-amber-300">
                              number changed since the last verification —
                              updated
                            </span>
                          ) : null}
                        </div>
                      </div>
                    ) : null}
                    {verification?.error ? (
                      <p className="mt-1 pl-[42px] font-mono text-[10px] text-rose-300">
                        {verification.error}
                      </p>
                    ) : null}
                    {armingBlock && !peer.blocked ? (
                      <p className="mt-1 pl-[42px] font-mono text-[10px] text-rose-300">
                        click again to block {peer.name} — their DMs will be
                        dropped
                      </p>
                    ) : null}
                    {block?.pending ? (
                      <p className="mt-1 pl-[42px] font-mono text-[10px] text-mist">
                        {peer.blocked ? "unblocking…" : "blocking…"}
                      </p>
                    ) : block?.error ? (
                      <p className="mt-1 pl-[42px] font-mono text-[10px] text-rose-300">
                        {block.error}
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

        {onCreateGroup ? (
          showCreate ? (
            <form onSubmit={submitCreate} className="mt-3 space-y-2">
              <input
                type="text"
                value={groupName}
                onChange={(event) => setGroupName(event.target.value)}
                placeholder="Group name"
                aria-label="Group name"
                maxLength={64}
                className="w-full rounded-lg border border-line/80 bg-void/60 px-2.5 py-1.5 text-xs text-zinc-100 placeholder:text-mist focus:border-signal/60 focus:outline-none"
              />
              {peers.length === 0 ? (
                <p className="text-[11px] leading-relaxed text-mist">
                  No peers yet — a group needs at least one other peer.
                </p>
              ) : (
                <fieldset>
                  <legend className="sr-only">Members</legend>
                  <ul className="max-h-32 space-y-1 overflow-y-auto thin-scroll">
                    {peers.map((peer) => (
                      <li key={peer.destination}>
                        <label className="flex cursor-pointer items-center gap-2 rounded-lg px-1.5 py-1 text-xs text-zinc-200 hover:bg-void/40">
                          <input
                            type="checkbox"
                            checked={pickedMembers.includes(peer.destination)}
                            onChange={(event) => {
                              setPickedMembers((previous) =>
                                event.target.checked
                                  ? [...previous, peer.destination]
                                  : previous.filter(
                                      (hash) => hash !== peer.destination,
                                    ),
                              );
                            }}
                            className="h-3.5 w-3.5 accent-emerald-400"
                          />
                          <span className="min-w-0 truncate">
                            {peer.name || "unknown"}{" "}
                            <span className="font-mono text-[10px] text-mist">
                              {shortHash(peer.destination, 10)}
                            </span>
                          </span>
                        </label>
                      </li>
                    ))}
                  </ul>
                </fieldset>
              )}
              <div className="flex gap-2">
                <button
                  type="submit"
                  disabled={
                    creating ||
                    !groupName.trim() ||
                    pickedMembers.length === 0
                  }
                  className="rounded-md border border-signal/50 bg-signal/10 px-2.5 py-1 font-mono text-[11px] text-signal transition hover:bg-signal/20 disabled:opacity-50"
                >
                  {creating ? "creating…" : "create group"}
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setShowCreate(false);
                    setGroupName("");
                    setPickedMembers([]);
                    setGroupError(null);
                  }}
                  className="rounded-md border border-line/80 px-2.5 py-1 font-mono text-[11px] text-mist transition hover:text-zinc-200"
                >
                  cancel
                </button>
              </div>
            </form>
          ) : (
            <button
              type="button"
              onClick={() => {
                setGroupError(null);
                setShowCreate(true);
              }}
              className="mt-3 rounded-md border border-line/80 px-2.5 py-1 font-mono text-[11px] text-zinc-200 transition hover:border-signal/50 hover:text-signal"
            >
              + new group
            </button>
          )
        ) : null}

        {groupError ? (
          <p className="mt-2 font-mono text-[11px] text-rose-400">
            {groupError}
          </p>
        ) : null}

        {groups.length === 0 ? (
          <p className="mt-3 text-[11px] leading-relaxed text-mist">
            No group chats yet.
            {onCreateGroup
              ? " Create one above — pick a name and at least one peer."
              : " Create one with tau group create NAME --peer ..."}
          </p>
        ) : (
          <ul className="mt-2 space-y-2">
            {groups.map((group) => {
              const canEditMembers =
                group.created_by_me === true &&
                onAddGroupMembers !== undefined &&
                onRemoveGroupMembers !== undefined;
              const addCandidates = canEditMembers
                ? peers.filter(
                    (peer) =>
                      !group.members.some(
                        (member) => member.hash === peer.destination,
                      ),
                  )
                : [];
              const removeCandidates = canEditMembers
                ? group.members.filter(
                    (member) => member.hash !== node?.destination,
                  )
                : [];
              return (
              <li
                key={group.id}
                className="rounded-xl border border-line/70 bg-void/40 px-2.5 py-2"
              >
                <div className="flex items-start justify-between gap-2">
                  <div className="min-w-0">
                    <p className="truncate text-xs font-medium text-zinc-200">
                      {group.name}
                    </p>
                    <p className="mt-0.5 truncate font-mono text-[10px] text-mist">
                      {group.members.map((member) => member.name).join(", ")}
                    </p>
                  </div>
                  <div className="flex shrink-0 gap-1.5">
                    {canEditMembers ? (
                      <button
                        type="button"
                        disabled={changingId === group.id}
                        onClick={() => toggleEditMembers(group.id)}
                        className="rounded-md border border-line/80 bg-panel/60 px-2 py-1 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50"
                      >
                        {editingId === group.id ? "close" : "members"}
                      </button>
                    ) : null}
                    {onDeleteGroup ? (
                      <button
                        type="button"
                        disabled={deletingId === group.id}
                        onClick={() => {
                          if (confirmingId === group.id) {
                            void confirmDelete(group.id);
                          } else {
                            armDeleteConfirm(group.id);
                          }
                        }}
                        className={`rounded-md border px-2 py-1 font-mono text-[11px] transition focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50 ${
                          confirmingId === group.id
                            ? "border-amber/60 bg-amber/10 text-amber"
                            : "border-line/80 bg-panel/60 text-mist hover:border-rose-400/50 hover:text-rose-300"
                        }`}
                      >
                        {deletingId === group.id
                          ? "deleting…"
                          : confirmingId === group.id
                            ? "confirm delete"
                            : "delete"}
                      </button>
                    ) : null}
                  </div>
                </div>
                {canEditMembers && editingId === group.id ? (
                  <div className="mt-2 space-y-2 border-t border-line/50 pt-2">
                    {addCandidates.length === 0 ? (
                      <p className="text-[11px] leading-relaxed text-mist">
                        Everyone you know is already in this group.
                      </p>
                    ) : (
                      <fieldset>
                        <legend className="mb-1 font-mono text-[10px] uppercase tracking-wide text-mist">
                          add
                        </legend>
                        <ul className="max-h-24 space-y-1 overflow-y-auto thin-scroll">
                          {addCandidates.map((peer) => (
                            <li key={peer.destination}>
                              <label className="flex cursor-pointer items-center gap-2 rounded-lg px-1.5 py-1 text-xs text-zinc-200 hover:bg-void/40">
                                <input
                                  type="checkbox"
                                  checked={pickedAdd.includes(peer.destination)}
                                  onChange={(event) => {
                                    setPickedAdd((previous) =>
                                      event.target.checked
                                        ? [...previous, peer.destination]
                                        : previous.filter(
                                            (hash) =>
                                              hash !== peer.destination,
                                          ),
                                    );
                                  }}
                                  className="h-3.5 w-3.5 accent-emerald-400"
                                />
                                <span className="min-w-0 truncate">
                                  {peer.name || "unknown"}{" "}
                                  <span className="font-mono text-[10px] text-mist">
                                    {shortHash(peer.destination, 10)}
                                  </span>
                                </span>
                              </label>
                            </li>
                          ))}
                        </ul>
                      </fieldset>
                    )}
                    {removeCandidates.length === 0 ? (
                      <p className="text-[11px] leading-relaxed text-mist">
                        No members to remove — you stay in a group you
                        created.
                      </p>
                    ) : (
                      <fieldset>
                        <legend className="mb-1 font-mono text-[10px] uppercase tracking-wide text-mist">
                          remove
                        </legend>
                        <ul className="max-h-24 space-y-1 overflow-y-auto thin-scroll">
                          {removeCandidates.map((member) => (
                            <li key={member.hash}>
                              <label className="flex cursor-pointer items-center gap-2 rounded-lg px-1.5 py-1 text-xs text-zinc-200 hover:bg-void/40">
                                <input
                                  type="checkbox"
                                  checked={pickedRemove.includes(member.hash)}
                                  onChange={(event) => {
                                    setPickedRemove((previous) =>
                                      event.target.checked
                                        ? [...previous, member.hash]
                                        : previous.filter(
                                            (hash) => hash !== member.hash,
                                          ),
                                    );
                                  }}
                                  className="h-3.5 w-3.5 accent-emerald-400"
                                />
                                <span className="min-w-0 truncate">
                                  {member.name}{" "}
                                  <span className="font-mono text-[10px] text-mist">
                                    {shortHash(member.hash, 10)}
                                  </span>
                                </span>
                              </label>
                            </li>
                          ))}
                        </ul>
                      </fieldset>
                    )}
                    <div className="flex gap-2">
                      <button
                        type="button"
                        disabled={
                          changingId === group.id || pickedAdd.length === 0
                        }
                        onClick={() => void submitMemberChange(group.id, true)}
                        className="rounded-md border border-signal/50 bg-signal/10 px-2.5 py-1 font-mono text-[11px] text-signal transition hover:bg-signal/20 disabled:opacity-50"
                      >
                        {changingId === group.id ? "changing…" : "add selected"}
                      </button>
                      <button
                        type="button"
                        disabled={
                          changingId === group.id || pickedRemove.length === 0
                        }
                        onClick={() => void submitMemberChange(group.id, false)}
                        className="rounded-md border border-line/80 px-2.5 py-1 font-mono text-[11px] text-mist transition hover:text-rose-300 disabled:opacity-50"
                      >
                        {changingId === group.id
                          ? "changing…"
                          : "remove selected"}
                      </button>
                    </div>
                  </div>
                ) : null}
              </li>
              );
            })}
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
