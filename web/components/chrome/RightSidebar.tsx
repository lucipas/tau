// RightSidebar.tsx — desktop right rail (~320px) and the mobile People view.
// Shows the PeerPanel for the open conversation, otherwise the peers list
// (tap a peer to open its DM) + the traffic strip.

"use client";

import { buildConversations, useTraffic } from "@/lib/use-tau";
import { formatAge } from "@/lib/format";
import { useNow } from "@/lib/use-now";
import { useEffect, useState } from "react";
import Avatar from "./Avatar";
import TrafficStrip from "./TrafficStrip";
import { parseHash, useHash } from "./useHash";
import { useTauContext } from "./TauContext";
import PeerPanel from "../chat/PeerPanel";

export default function RightSidebar({ className = "" }: { className?: string }) {
  const tau = useTauContext();
  const { section, peer: selectedPeer, group: selectedGroup, navigate, hash } = useHash();
  const now = useNow();
  const traffic = useTraffic(tau.messages, tau.peers);

  // A roster change rotates the group to a successor id; when the
  // refresh right after the change cannot load the successor yet, the
  // PeerPanel hands the successor id + origin route up here, because
  // this sidebar outlives the panel (the panel unmounts once the old
  // group drops out of the snapshot). On a later snapshot that
  // contains the successor, follow it — but only if the user is still
  // on the thread the change started on. Leaving the thread clears
  // the pending navigation at once (the hash is an effect dependency,
  // so a departure clears it even if the user comes back before the
  // successor snapshot arrives).
  const [pendingSuccessor, setPendingSuccessor] = useState<{
    id: string;
    originHash: string;
  } | null>(null);
  useEffect(() => {
    if (!pendingSuccessor) return;
    // Compare against the live hash, not the `hash` state: the state
    // updates on the async hashchange event, so a snapshot arriving
    // in the same tick as a navigation would otherwise read the stale
    // origin route and redirect a user who just left.
    if (typeof window === "undefined" || window.location.hash !== pendingSuccessor.originHash) {
      setPendingSuccessor(null);
      return;
    }
    if (tau.groups.some((g) => g.id === pendingSuccessor.id)) {
      const { section: originSection } = parseHash(pendingSuccessor.originHash);
      navigate(originSection, { group: pendingSuccessor.id });
      setPendingSuccessor(null);
    }
  }, [pendingSuccessor, tau.groups, navigate, hash]);

  const openConversation =
    (section === "chat" || section === "people") &&
    (selectedPeer || selectedGroup)
      ? buildConversations(tau.messages, tau.peers, tau.groups, tau.unread, {
          includeArchived: true,
        }).find((c) =>
          selectedPeer
            ? c.kind === "dm" && c.key === selectedPeer
            : selectedGroup
              ? c.kind === "group" && c.key === selectedGroup
              : false,
        ) ?? null
      : null;

  const peers = [...tau.peers].sort((a, b) => b.last_seen - a.last_seen);

  return (
    <aside aria-label="Peers and traffic" className={`flex flex-col gap-3 p-3 ${className}`}>
      {openConversation ? (
        <div className="rounded-2xl border border-zinc-800 bg-zinc-900/60">
          {section === "people" && (
            <button
              type="button"
              onClick={() =>
                navigate(
                  "chat",
                  openConversation.kind === "group"
                    ? { group: openConversation.key }
                    : { peer: openConversation.key },
                )
              }
              className="px-4 pt-3 text-sm font-semibold text-cyan-400 hover:text-cyan-300"
            >
              ← Back to chat
            </button>
          )}
          <PeerPanel
            conversation={openConversation}
            onSuccessorPending={(id, originHash) =>
              setPendingSuccessor({ id, originHash })
            }
          />
        </div>
      ) : (
        <>
          <section aria-label="People" className="rounded-2xl border border-zinc-800 bg-zinc-900/60 p-3">
            <div className="mb-2 flex items-baseline justify-between">
              <h2 className="text-sm font-bold text-zinc-100">People</h2>
              <span className="text-xs text-zinc-500">
                {tau.peers.length} peer{tau.peers.length === 1 ? "" : "s"}
              </span>
            </div>
            {peers.length === 0 ? (
              <p className="text-sm text-zinc-500">
                No peers yet. Peers appear once your node hears their announces.
              </p>
            ) : (
              <ul className="flex flex-col">
                {peers.map((peer) => {
                  const unread = tau.unread?.per_peer[peer.destination] ?? 0;
                  return (
                    <li key={peer.destination}>
                      <button
                        type="button"
                        onClick={() => navigate("chat", { peer: peer.destination })}
                        className="flex w-full items-center gap-2 rounded-lg px-1 py-1.5 text-left hover:bg-zinc-800/70"
                      >
                        <span className="relative">
                          <Avatar name={peer.alias || peer.name} seed={peer.destination} size="sm" />
                          <span
                            aria-hidden="true"
                            title={peer.reachable ? "Reachable" : "Not currently reachable"}
                            className={`absolute -bottom-0.5 -right-0.5 h-2.5 w-2.5 rounded-full border border-zinc-900 ${
                              peer.reachable ? "bg-emerald-400" : "bg-zinc-600"
                            }`}
                          />
                        </span>
                        <span className="min-w-0 flex-1">
                          <span className="block truncate text-sm font-medium text-zinc-200">
                            {peer.alias || peer.name}
                            {peer.verified ? " ✓" : ""}
                            {peer.lookalike_of ? " ⚠" : ""}
                          </span>
                          <span className="block text-xs text-zinc-500">
                            {peer.last_seen ? formatAge(peer.age) : "never seen"}
                            {peer.hops != null ? ` · ${peer.hops} hop${peer.hops === 1 ? "" : "s"}` : ""}
                          </span>
                        </span>
                        {unread > 0 && (
                          <span className="rounded-full bg-cyan-500 px-1.5 py-0.5 text-[11px] font-bold text-zinc-950">
                            {unread}
                          </span>
                        )}
                      </button>
                    </li>
                  );
                })}
              </ul>
            )}
          </section>
          <TrafficStrip rows={traffic} now={now} />
        </>
      )}
    </aside>
  );
}
