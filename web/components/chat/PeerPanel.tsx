// PeerPanel.tsx — right-sidebar detail for the open conversation.
//
// DM: peer info, public profile, ping, safety number + verify flow (the safety number is
// shown first; verification happens only after a separate "Numbers match"
// confirm — never automatically), block/unblock, forget, alias, note,
// archive, mute, pin, disappearing default, mark unread/read.
// Group: members, topic, mute, mark unread/read, leave.

"use client";

import { useEffect, useState } from "react";
import {
  getGroupInviteStatus,
  leaveGroup,
  mintGroupInvite,
  revokeGroupInvites,
  setDisappearingDefault,
  type GroupInviteStatus,
} from "@/lib/api";
import { formatAge, formatDateTime, truncatedHash } from "@/lib/format";
import { useNow } from "@/lib/use-now";
import { usePeerActions, type Conversation } from "@/lib/use-tau";
import type { Peer } from "@/lib/types";
import Avatar from "../chrome/Avatar";
import { useTauContext } from "../chrome/TauContext";

const DISAPPEARING_CHOICES: { label: string; seconds: number | null }[] = [
  { label: "Off", seconds: null },
  { label: "5 minutes", seconds: 5 * 60 },
  { label: "1 hour", seconds: 3600 },
  { label: "1 day", seconds: 86400 },
  { label: "1 week", seconds: 7 * 86400 },
];

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-start justify-between gap-3 border-b border-zinc-800/60 py-2 text-sm">
      <span className="shrink-0 text-zinc-500">{label}</span>
      <span className="min-w-0 text-right text-zinc-200">{children}</span>
    </div>
  );
}

function ActionButton({
  onClick,
  children,
  danger = false,
  disabled = false,
}: {
  onClick: () => void;
  children: React.ReactNode;
  danger?: boolean;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className={`rounded-full border px-3 py-1.5 text-xs font-semibold ${
        danger
          ? "border-red-800 text-red-300 hover:bg-red-950"
          : "border-zinc-700 text-zinc-200 hover:bg-zinc-800"
      } disabled:opacity-40`}
    >
      {children}
    </button>
  );
}

export default function PeerPanel({ conversation }: { conversation: Conversation | null }) {
  const tau = useTauContext();
  const actions = usePeerActions();
  const now = useNow();
  const [alias, setAlias] = useState("");
  const [note, setNote] = useState("");
  const [safetyNumber, setSafetyNumber] = useState<string | null>(null);
  const [verifyState, setVerifyState] = useState<"idle" | "showing" | "verified" | "mismatch">("idle");
  const [pingResult, setPingResult] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [confirmForget, setConfirmForget] = useState(false);
  const [confirmLeave, setConfirmLeave] = useState(false);
  const [disappearing, setDisappearing] = useState<number | null>(null);
  const [invite, setInvite] = useState<GroupInviteStatus | null>(null);
  const [inviteBusy, setInviteBusy] = useState(false);
  const [confirmRevoke, setConfirmRevoke] = useState(false);
  const [copied, setCopied] = useState(false);

  const peer: Peer | undefined = conversation?.peer;
  const isGroup = conversation?.kind === "group";
  const groupId = isGroup ? conversation.key : null;
  const groupCreatedByMe = Boolean(isGroup && conversation.group?.created_by_me);

  useEffect(() => {
    setAlias(peer?.alias ?? "");
    setNote(peer?.note ?? "");
    setSafetyNumber(null);
    setVerifyState(peer?.verified ? "verified" : "idle");
    setPingResult(null);
    setError(null);
    setConfirmForget(false);
    setConfirmLeave(false);
  }, [peer?.destination, peer?.alias, peer?.note, peer?.verified]);

  // The invite link is creator-managed: only fetch it for groups this
  // node created, and refresh it after every mint/revoke.
  useEffect(() => {
    setInvite(null);
    setConfirmRevoke(false);
    setCopied(false);
    if (!groupCreatedByMe || !groupId) return;
    let cancelled = false;
    void getGroupInviteStatus(groupId)
      .then((status) => {
        if (!cancelled) setInvite(status);
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      });
    return () => {
      cancelled = true;
    };
  }, [groupId, groupCreatedByMe]);

  if (!conversation) {
    return (
      <div className="p-4 text-sm text-zinc-500">
        Open a conversation to see peer details here.
      </div>
    );
  }

  async function run(label: string, fn: () => Promise<unknown>) {
    setBusy(label);
    setError(null);
    try {
      await fn();
      await tau.refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(null);
    }
  }

  async function runInvite(fn: () => Promise<GroupInviteStatus>) {
    setInviteBusy(true);
    setError(null);
    try {
      setInvite(await fn());
      await tau.refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setInviteBusy(false);
    }
  }

  async function handleRevokeInvite() {
    if (!groupId) return;
    setConfirmRevoke(false);
    await runInvite(async () => {
      await revokeGroupInvites(groupId);
      return getGroupInviteStatus(groupId);
    });
  }

  async function copyInviteLink() {
    if (!invite?.link) return;
    try {
      await navigator.clipboard.writeText(invite.link);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 2000);
    } catch {
      setError("Copy failed — select the link text manually.");
    }
  }

  const target = isGroup ? { group: conversation.key } : { peer: conversation.key };

  return (
    <div className="flex flex-col gap-1 p-4" aria-label={`Details for ${conversation.name}`}>
      <div className="flex flex-col items-center gap-2 border-b border-zinc-800 pb-3 text-center">
        <Avatar name={conversation.name} seed={conversation.key} size="xl" />
        <h2 className="text-lg font-bold text-zinc-50">{conversation.name}</h2>
        <p className="break-all text-xs text-zinc-500" title={conversation.key}>
          {isGroup ? `Group · ${conversation.key.slice(0, 12)}…` : truncatedHash(conversation.key)}
          {peer?.lookalike_of && (
            <span className="mt-1 block rounded-md bg-amber-950 px-2 py-1 text-amber-300">
              ⚠ Looks like {peer.lookalike_of} — unverified lookalike. Check the safety number before trusting this peer.
            </span>
          )}
        </p>
        {peer?.verified && (
          <span className="rounded-full bg-emerald-950 px-2.5 py-0.5 text-xs font-semibold text-emerald-300">
            ✓ Verified{peer.verified_at ? ` · ${formatDateTime(peer.verified_at)}` : ""}
          </span>
        )}
        {peer && !peer.verified && peer.key_change_at && (
          <span className="rounded-full bg-amber-950 px-2.5 py-0.5 text-xs text-amber-300">
            Key changed {formatAge(now - peer.key_change_at)}
          </span>
        )}
      </div>

      {peer && (
        <div className="mt-3 rounded-xl border border-zinc-800 p-3">
          <h3 className="text-sm font-bold text-zinc-100">Public profile</h3>
          {peer.profile ? (
            <div className="mt-2 flex items-start gap-3">
              <Avatar name={peer.profile.name} seed={peer.profile.author} size="lg" />
              <div className="min-w-0">
                <p className="truncate text-sm font-semibold text-zinc-100">
                  {peer.profile.name || peer.name}
                </p>
                {peer.profile.bio && (
                  <p className="mt-1 whitespace-pre-wrap break-words text-sm text-zinc-300">
                    {peer.profile.bio}
                  </p>
                )}
                {peer.profile.links.length > 0 && (
                  <ul className="mt-2 flex flex-col gap-1">
                    {peer.profile.links.map((link) => (
                      <li key={link}>
                        <a
                          href={link}
                          target="_blank"
                          rel="noreferrer"
                          className="break-all text-sm text-cyan-400 hover:underline"
                        >
                          {link}
                        </a>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
          ) : (
            <p className="mt-1 text-xs leading-snug text-zinc-500">
              This peer hasn&apos;t shared a public profile.
            </p>
          )}
        </div>
      )}

      {peer && (
        <div>
          <Row label="Last seen">{peer.last_seen ? formatAge(peer.age) : "never"}</Row>
          <Row label="Hops">{peer.hops ?? "—"}</Row>
          <Row label="Reachable">{peer.reachable === undefined ? "—" : peer.reachable ? "Yes" : "No"}</Row>
          <Row label="LXMF">{peer.lxmf ? truncatedHash(peer.lxmf) : "—"}</Row>
          {peer.previous_name && <Row label="Previous name">{peer.previous_name}</Row>}
        </div>
      )}
      {isGroup && conversation.group && (
        <div>
          {conversation.group.topic && <Row label="Topic">{conversation.group.topic}</Row>}
          <Row label="Members">{conversation.group.members.length}</Row>
          <ul className="mt-2 flex flex-col gap-1.5">
            {conversation.group.members.map((m) => (
              <li key={m.hash} className="flex items-center gap-2 text-sm">
                <Avatar name={m.name} seed={m.hash} size="sm" />
                <span className="truncate text-zinc-200">{m.name}</span>
                <span className="ml-auto text-xs text-zinc-500">{truncatedHash(m.hash)}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
      {isGroup && conversation.group?.created_by_me && (
        <div className="mt-3 rounded-xl border border-zinc-800 p-3">
          <h3 className="text-sm font-bold text-zinc-100">Invite link</h3>
          <p className="mt-1 text-xs leading-snug text-zinc-500">
            Anyone with the link can ask to join. Open groups add them at once; closed groups leave
            a request for you to approve.
          </p>
          {invite?.link ? (
            <>
              <p
                className="mt-2 break-all rounded-lg bg-zinc-900 px-3 py-2 font-mono text-xs text-zinc-100"
                aria-label="Invite link"
              >
                {invite.link}
              </p>
              <div className="mt-2 flex flex-wrap gap-2">
                <ActionButton disabled={inviteBusy} onClick={() => void copyInviteLink()}>
                  {copied ? "Copied" : "Copy link"}
                </ActionButton>
                <ActionButton
                  disabled={inviteBusy}
                  onClick={() => void runInvite(() => mintGroupInvite(conversation.key))}
                >
                  New link
                </ActionButton>
                {confirmRevoke ? (
                  <ActionButton danger disabled={inviteBusy} onClick={() => void handleRevokeInvite()}>
                    Confirm revoke?
                  </ActionButton>
                ) : (
                  <ActionButton danger onClick={() => setConfirmRevoke(true)}>
                    Revoke
                  </ActionButton>
                )}
              </div>
              <div className="mt-2 flex flex-wrap items-center gap-2 text-xs">
                <span className="text-zinc-500">Mode:</span>
                {(["open", "closed"] as const).map((mode) => (
                  <button
                    key={mode}
                    type="button"
                    disabled={inviteBusy || invite.mode === mode}
                    onClick={() => void runInvite(() => mintGroupInvite(conversation.key, mode))}
                    className={`rounded-full border px-2.5 py-1 text-xs font-semibold disabled:opacity-40 ${
                      invite.mode === mode
                        ? "border-cyan-500 text-cyan-300"
                        : "border-zinc-700 text-zinc-200 hover:bg-zinc-800"
                    }`}
                  >
                    {mode === "open" ? "Open — joins at once" : "Closed — approve first"}
                  </button>
                ))}
              </div>
            </>
          ) : (
            <div className="mt-2">
              <ActionButton
                disabled={inviteBusy}
                onClick={() => void runInvite(() => mintGroupInvite(conversation.key))}
              >
                Create invite link
              </ActionButton>
            </div>
          )}
        </div>
      )}

      {peer && (
        <>
          <div className="mt-3 flex flex-col gap-2">
            <label className="text-xs font-semibold text-zinc-400" htmlFor="peer-alias">Alias</label>
            <div className="flex gap-2">
              <input
                id="peer-alias"
                value={alias}
                onChange={(e) => setAlias(e.target.value)}
                placeholder="Local alias…"
                className="min-w-0 flex-1 rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
              />
              <button
                type="button"
                disabled={busy === "alias"}
                onClick={() => void run("alias", () => actions.setAlias(peer.destination, alias.trim()))}
                className="rounded-full bg-zinc-800 px-3 py-1.5 text-xs font-semibold text-zinc-200 hover:bg-zinc-700 disabled:opacity-40"
              >
                Save
              </button>
            </div>
            <label className="text-xs font-semibold text-zinc-400" htmlFor="peer-note">Private note</label>
            <div className="flex gap-2">
              <input
                id="peer-note"
                value={note}
                onChange={(e) => setNote(e.target.value)}
                placeholder="Only you can see this…"
                className="min-w-0 flex-1 rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
              />
              <button
                type="button"
                disabled={busy === "note"}
                onClick={() => void run("note", () => actions.setNote(peer.destination, note))}
                className="rounded-full bg-zinc-800 px-3 py-1.5 text-xs font-semibold text-zinc-200 hover:bg-zinc-700 disabled:opacity-40"
              >
                Save
              </button>
            </div>
          </div>

          <div className="mt-3 rounded-xl border border-zinc-800 p-3">
            <h3 className="text-sm font-bold text-zinc-100">Safety number</h3>
            {verifyState === "verified" ? (
              <p className="mt-1 text-sm text-emerald-300">✓ This peer is verified.</p>
            ) : (
              <>
                <p className="mt-1 text-xs leading-snug text-zinc-500">
                  Compare this number with your peer out of band (in person, another channel).
                  Only confirm when the numbers match.
                </p>
                {safetyNumber ? (
                  <>
                    <p className="mt-2 break-all rounded-lg bg-zinc-900 px-3 py-2 font-mono text-sm text-zinc-100" aria-label="Safety number">
                      {safetyNumber}
                    </p>
                    <div className="mt-2 flex flex-wrap gap-2">
                      <button
                        type="button"
                        disabled={busy === "verify"}
                        onClick={() => void run("verify", async () => {
                          await actions.verify(peer.destination);
                          setVerifyState("verified");
                        })}
                        className="rounded-full bg-cyan-500 px-3 py-1.5 text-xs font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
                      >
                        Numbers match — verify
                      </button>
                      <button
                        type="button"
                        onClick={() => { setSafetyNumber(null); setVerifyState("mismatch"); }}
                        className="rounded-full border border-zinc-700 px-3 py-1.5 text-xs font-semibold text-zinc-200 hover:bg-zinc-800"
                      >
                        Numbers don&apos;t match
                      </button>
                    </div>
                  </>
                ) : (
                  <button
                    type="button"
                    disabled={busy === "safety"}
                    onClick={() => void run("safety", async () => {
                      const result = (await actions.safetyNumber(peer.destination)) as { safety_number?: string };
                      setSafetyNumber(result.safety_number ?? "");
                      setVerifyState("showing");
                    })}
                    className="mt-2 rounded-full border border-zinc-700 px-3 py-1.5 text-xs font-semibold text-zinc-200 hover:bg-zinc-800 disabled:opacity-40"
                  >
                    Show safety number
                  </button>
                )}
                {verifyState === "mismatch" && (
                  <p role="alert" className="mt-2 text-xs text-amber-300">
                    Numbers don&apos;t match — do not trust this peer. It may be an impersonator; block it below if you don&apos;t know them.
                  </p>
                )}
              </>
            )}
          </div>

          <div className="mt-3 flex flex-wrap gap-2">
            <ActionButton disabled={busy === "ping"} onClick={() => void run("ping", async () => {
              const result = (await actions.ping(peer.destination)) as { rtt_ms?: number | null };
              setPingResult(result.rtt_ms != null ? `Ping OK — ${result.rtt_ms} ms` : "Ping OK — no RTT reported");
            })}>
              Ping
            </ActionButton>
            <ActionButton disabled={busy === "info"} onClick={() => void run("info", () => actions.info(peer.destination))}>
              Peer info
            </ActionButton>
            <ActionButton
              disabled={busy === "block"}
              danger={!!peer.blocked}
              onClick={() => void run("block", () => (peer.blocked ? actions.unblock(peer.destination) : actions.block(peer.destination)))}
            >
              {peer.blocked ? "Unblock" : "Block"}
            </ActionButton>
            {!confirmForget ? (
              <ActionButton danger onClick={() => setConfirmForget(true)}>Forget</ActionButton>
            ) : (
              <ActionButton
                danger
                disabled={busy === "forget"}
                onClick={() => void run("forget", () => actions.forget(peer.destination))}
              >
                Confirm forget?
              </ActionButton>
            )}
          </div>
          {pingResult && <p className="mt-1 text-xs text-zinc-400">{pingResult}</p>}
        </>
      )}

      <div className="mt-3 flex flex-col gap-2">
        <label className="text-xs font-semibold text-zinc-400" htmlFor="disappearing-default">
          Disappearing messages (default for this conversation)
        </label>
        <select
          id="disappearing-default"
          value={disappearing === null ? "" : String(disappearing)}
          onChange={(e) => {
            const seconds = e.target.value ? Number(e.target.value) : null;
            setDisappearing(seconds);
            void run("disappearing", () => setDisappearingDefault(target, seconds));
          }}
          className="rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none"
        >
          {DISAPPEARING_CHOICES.map((o) => (
            <option key={o.label} value={o.seconds === null ? "" : o.seconds}>
              {o.label}
            </option>
          ))}
        </select>
      </div>

      <div className="mt-3 flex flex-wrap gap-2">
        <ActionButton disabled={busy === "pin"} onClick={() => void run("pin", () => actions.pin(target, !conversation.pinned))}>
          {conversation.pinned ? "Unpin" : "Pin"}
        </ActionButton>
        <ActionButton disabled={busy === "mute"} onClick={() => void run("mute", () => actions.mute(target, !conversation.muted))}>
          {conversation.muted ? "Unmute" : "Mute"}
        </ActionButton>
        <ActionButton disabled={busy === "archive"} onClick={() => void run("archive", () => (conversation.archived ? actions.unarchive(conversation.key) : actions.archive(conversation.key)))}>
          {conversation.archived ? "Unarchive" : "Archive"}
        </ActionButton>
        <ActionButton disabled={busy === "read"} onClick={() => void run("read", () => actions.markRead(target))}>
          Mark read
        </ActionButton>
        <ActionButton disabled={busy === "unread"} onClick={() => void run("unread", () => actions.markUnread(target))}>
          Mark unread
        </ActionButton>
        {isGroup &&
          (confirmLeave ? (
            <ActionButton danger disabled={busy === "leave"} onClick={() => void run("leave", () => leaveGroup(conversation.key))}>
              Confirm leave group?
            </ActionButton>
          ) : (
            <ActionButton danger onClick={() => setConfirmLeave(true)}>Leave group</ActionButton>
          ))}
      </div>

      {error && (
        <p role="alert" className="mt-2 text-xs text-red-400">
          {error}
        </p>
      )}
    </div>
  );
}
