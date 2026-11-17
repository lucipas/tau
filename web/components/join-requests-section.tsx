"use client";

import { useEffect, useRef, useState } from "react";

import { formatAge, formatDateTime, peerLabel } from "@/lib/format";
import type {
  DecideJoinRequestResponse,
  JoinRequestEntry,
} from "@/lib/types";

import { useNow } from "@/lib/use-now";

import { SectionLabel } from "./ui";

/** Join requests for closed groups, waiting on the creator's decision. */
export function JoinRequestsSection({
  requests,
  onApprove,
  onDeny,
}: {
  requests: JoinRequestEntry[];
  onApprove: (
    group: string,
    peer: string,
  ) => Promise<DecideJoinRequestResponse>;
  onDeny: (group: string, peer: string) => Promise<DecideJoinRequestResponse>;
}) {
  const nowMs = useNow(30000);
  const now = (nowMs ?? Date.now()) / 1000;
  const [confirmingKey, setConfirmingKey] = useState<string | null>(null);
  const [approvingKey, setApprovingKey] = useState<string | null>(null);
  const [denyingKey, setDenyingKey] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const confirmTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (confirmTimer.current) clearTimeout(confirmTimer.current);
    };
  }, []);

  // Nothing pending and no feedback to show: stay out of the way, but
  // keep this component (and its result state) mounted.
  if (requests.length === 0 && !notice && !error) return null;

  function keyFor(request: JoinRequestEntry) {
    return `${request.group}:${request.peer}`;
  }

  function armConfirm(key: string) {
    setError(null);
    setNotice(null);
    setConfirmingKey(key);
    if (confirmTimer.current) clearTimeout(confirmTimer.current);
    confirmTimer.current = setTimeout(() => setConfirmingKey(null), 5000);
  }

  async function confirmApprove(request: JoinRequestEntry) {
    // Approving is not destructive, so it needs no second click — but
    // a second approve while one is in flight would admit the same
    // peer twice.
    if (approvingKey !== null || denyingKey !== null) return;
    const key = keyFor(request);
    setApprovingKey(key);
    setError(null);
    setNotice(null);
    try {
      const result = await onApprove(request.group, request.peer);
      setNotice(
        result.status === "already_member"
          ? `${request.peer_name} is already a member of ${request.group}.`
          : `${request.peer_name} joined ${request.group}.`,
      );
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "approve failed");
    } finally {
      setApprovingKey(null);
    }
  }

  async function confirmDeny(request: JoinRequestEntry) {
    // Denying drops the request for good, so it needs a second click.
    if (approvingKey !== null || denyingKey !== null) return;
    if (confirmTimer.current) clearTimeout(confirmTimer.current);
    setConfirmingKey(null);
    const key = keyFor(request);
    setDenyingKey(key);
    setError(null);
    setNotice(null);
    try {
      await onDeny(request.group, request.peer);
      setNotice(`${request.peer_name} was denied for ${request.group}.`);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "deny failed");
    } finally {
      setDenyingKey(null);
    }
  }

  return (
    <section className="rounded-2xl border border-line/80 bg-panel/60 p-4 shadow-[0_20px_60px_-40px_rgba(0,0,0,0.9)]">
      <div className="flex items-center justify-between">
        <SectionLabel>Join requests</SectionLabel>
        <span className="inline-flex items-center gap-1.5 rounded-full border border-amber/25 bg-amber/5 px-2 py-0.5 font-mono text-[10px] text-amber">
          {requests.length} pending
        </span>
      </div>

      <p className="mt-2 font-mono text-[11px] leading-relaxed text-mist/80">
        peers asking to join a closed group with an invite link — approve
        to add them to the roster, deny to turn them away.
      </p>

      {notice ? (
        <p className="mt-2 font-mono text-[11px] text-signal">{notice}</p>
      ) : null}

      {error ? (
        <p className="mt-2 font-mono text-[11px] text-rose-400">{error}</p>
      ) : null}

      <ul className="mt-3 flex flex-col gap-3">
        {requests.map((request) => {
          const key = keyFor(request);
          return (
            <li
              key={key}
              className="rounded-xl border border-line/70 bg-void/50 p-3"
            >
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <p className="truncate font-mono text-[12px] font-semibold text-zinc-100">
                    {peerLabel(request.peer_name, request.peer)}
                  </p>
                  <p className="mt-1 break-words text-[13px] leading-relaxed text-zinc-200">
                    wants to join {request.group}
                  </p>
                </div>
                <div className="flex shrink-0 gap-2">
                  <button
                    type="button"
                    disabled={approvingKey !== null || denyingKey !== null}
                    onClick={() => void confirmApprove(request)}
                    className="rounded-md border border-line/80 bg-panel/60 px-2 py-1 font-mono text-[11px] text-mist transition hover:border-signal/50 hover:text-signal focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50"
                  >
                    {approvingKey === key ? "approving…" : "approve"}
                  </button>
                  <button
                    type="button"
                    disabled={approvingKey !== null || denyingKey !== null}
                    onClick={() => {
                      if (confirmingKey === key) {
                        void confirmDeny(request);
                      } else {
                        armConfirm(key);
                      }
                    }}
                    className={`rounded-md border px-2 py-1 font-mono text-[11px] transition focus:outline-none focus-visible:ring-2 focus-visible:ring-signal/60 disabled:opacity-50 ${
                      confirmingKey === key
                        ? "border-amber/60 bg-amber/10 text-amber"
                        : "border-line/80 bg-panel/60 text-mist hover:border-rose-400/50 hover:text-rose-300"
                    }`}
                  >
                    {denyingKey === key
                      ? "denying…"
                      : confirmingKey === key
                        ? "confirm deny"
                        : "deny"}
                  </button>
                </div>
              </div>

              <p className="mt-2 font-mono text-[10px] leading-relaxed text-mist/60">
                {formatAge(now - request.requested_at)} · expires{" "}
                {formatDateTime(request.expires_at)}
              </p>
            </li>
          );
        })}
      </ul>
    </section>
  );
}
