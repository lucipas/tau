"use client";

import { useEffect, useRef, useState } from "react";

import { useTau } from "@/lib/use-tau";
import type { TauMessage } from "@/lib/types";

import { BundleSection } from "./bundle-section";
import { Composer } from "./composer";
import { AllowlistSection } from "./allowlist-section";
import { DoctorSection } from "./doctor-section";
import { Header } from "./header";
import { MessageFeed } from "./message-feed";
import { MessagesBackupSection } from "./messages-backup-section";
import { OutboxSection } from "./outbox-section";
import { PeerSidebar } from "./peer-sidebar";

export function Console() {
  const tau = useTau();
  const [target, setTarget] = useState("");
  const [mode, setMode] = useState<"peer" | "custom" | "group">("peer");
  const [replyTo, setReplyTo] = useState<TauMessage | null>(null);
  const customPicked = useRef(false);

  useEffect(() => {
    if (tau.peers.length === 0 && tau.groups.length === 0) {
      setMode("custom");
      return;
    }
    // A refresh can remove the selected group: never keep submitting its
    // stale id. Fall back to a remaining group, then to a peer.
    if (mode === "group" && !tau.groups.some((group) => group.id === target)) {
      if (tau.groups.length > 0) {
        setTarget(tau.groups[0].id);
      } else if (tau.peers.length > 0) {
        setMode("peer");
        setTarget(tau.peers[0].destination);
      } else {
        setMode("custom");
        setTarget("");
      }
      return;
    }
    if (!target && !customPicked.current) {
      if (tau.peers.length > 0) {
        setMode("peer");
        setTarget(tau.peers[0].destination);
      } else if (tau.groups.length > 0) {
        setMode("group");
        setTarget(tau.groups[0].id);
      }
    }
  }, [tau.peers, tau.groups, target, mode]);

  function changeMode(next: "peer" | "custom" | "group") {
    customPicked.current = next === "custom";
    // A pending reply belongs to a peer conversation: the bridge
    // rejects replies on group sends, so entering group mode drops it.
    if (next === "group") {
      setReplyTo(null);
    }
    setMode(next);
  }

  function handleTargetChange(value: string) {
    // Retargeting the composer invalidates the pending reply: the
    // quoted message belongs to the old conversation, and sending it
    // to the new peer would only be rejected by the bridge.
    if (replyTo && value !== (replyTo.peer ?? "")) {
      setReplyTo(null);
    }
    setTarget(value);
  }

  // A deleted message can no longer be quoted: the bridge rejects its
  // wire id. This also covers deletes that arrive from another tab.
  useEffect(() => {
    if (!replyTo) return;
    if (!tau.messages.some((message) => message.id === replyTo.id)) {
      setReplyTo(null);
    }
  }, [replyTo, tau.messages]);

  const { markRead } = tau;
  const openUnread =
    mode === "peer" && target ? (tau.unread.per_peer[target] ?? 0) : 0;

  // The open conversation reads as read: the auto-selected one on load,
  // and one that gets new mail while it stays open. A failed call leaves
  // the badge in place until the count next changes.
  useEffect(() => {
    if (!target || openUnread === 0) return;
    void markRead(target).catch(() => undefined);
  }, [target, openUnread, markRead]);

  function selectPeer(destination: string) {
    customPicked.current = false;
    setMode("peer");
    setTarget(destination);
    setReplyTo(null);
    // Opening the conversation advances its read cursor on the node,
    // so the badge clears. A failed call leaves the badge in place.
    void tau.markRead(destination).catch(() => undefined);
    const textarea = document.getElementById(
      "message-text",
    ) as HTMLTextAreaElement | null;
    textarea?.focus();
    if (window.innerWidth < 1024) {
      textarea?.scrollIntoView({ behavior: "smooth", block: "center" });
    }
  }

  return (
    <div className="relative min-h-dvh">
      <div className="mesh-grid pointer-events-none fixed inset-0" aria-hidden="true" />
      <div
        className="pointer-events-none fixed inset-x-0 top-0 h-96 bg-[radial-gradient(60%_100%_at_50%_0%,rgba(45,212,191,0.13),transparent_72%)]"
        aria-hidden="true"
      />

      <div className="relative">
        <Header
          node={tau.node}
          connection={tau.connection}
          announcing={tau.announcing}
          onAnnounce={() => {
            void tau.announce().catch(() => undefined);
          }}
        />

        {tau.connection === "reconnecting" ? (
          <div className="border-b border-amber/25 bg-amber/[0.07] px-4 py-1.5 text-center font-mono text-[11px] text-amber lg:px-8">
            local bridge unreachable — retrying. Start it with{" "}
            <code className="text-amber/90">tau serve --http</code>
          </div>
        ) : null}

        <main className="mx-auto grid w-full max-w-7xl gap-4 px-4 py-4 lg:grid-cols-[340px_minmax(0,1fr)] lg:gap-5 lg:px-8 lg:py-6">
          <div className="flex min-w-0 flex-col gap-4">
          <PeerSidebar
            node={tau.node}
            peers={tau.peers}
            groups={tau.groups}
            bridge={tau.bridge}
            social={tau.social}
            pings={tau.pings}
            verifications={tau.verifications}
            blocks={tau.blocks}
            unread={tau.unread.per_peer}
            selected={target}
            onPing={(destination) => {
              void tau.ping(destination);
            }}
            onSetAlias={(destination, alias) =>
              tau.setPeerAlias(destination, alias)
            }
            onVerify={(destination) => {
              void tau.verifyPeer(destination).catch(() => undefined);
            }}
            onShowSafetyNumber={(destination) => {
              void tau.showSafetyNumber(destination).catch(() => undefined);
            }}
            onDismissVerification={(destination) => {
              tau.clearVerification(destination);
            }}
            onBlock={(destination) => {
              void tau.blockPeer(destination).catch(() => undefined);
            }}
            onUnblock={(destination) => {
              void tau.unblockPeer(destination).catch(() => undefined);
            }}
            onSelectPeer={selectPeer}
            onCreateGroup={(name, members) => tau.createGroup(name, members)}
            onDeleteGroup={(group) => tau.deleteGroup(group)}
          />
            <AllowlistSection
              allowlist={tau.allowlist}
              onAdd={(value) => tau.addAllowlistPeer(value)}
              onRemove={(value) => tau.removeAllowlistPeer(value)}
              onToggle={(enabled) => tau.setAllowlistEnabled(enabled)}
            />
          </div>

          <div className="flex min-w-0 flex-col">
            <MessageFeed
              messages={
                tau.searchQuery !== null ? tau.searchResults : tau.messages
              }
              node={tau.node}
              live={tau.connection === "live"}
              searching={tau.searchQuery !== null}
              searchQuery={tau.searchQuery}
              searchError={tau.searchError}
              quoteContext={tau.messages}
              onSearch={tau.search}
              onClearSearch={tau.clearSearch}
              onReply={(message) => {
                setReplyTo(message);
                if (message.peer) {
                  setTarget(message.peer);
                  setMode("peer");
                }
              }}
              onDelete={async (message) => {
                await tau.deleteMessage(message.id);
              }}
              onReact={async (message, reaction) => {
                if (!message.wire_id) return;
                if (reaction === null) {
                  await tau.clearReaction(message.wire_id);
                } else {
                  await tau.reactToMessage(message.wire_id, reaction);
                }
              }}
            />
            {/* Stays mounted when the queue empties so a retry's result
                line survives the last entry leaving; it renders nothing
                itself when there is nothing to show. */}
            <div className="mt-4">
              <OutboxSection
                entries={tau.outbox}
                heldForOthers={tau.heldForOthers}
                held={tau.heldMail}
                onCancel={tau.cancelOutbox}
                onRetry={tau.retryOutbox}
                onDropHeld={tau.dropHeldMail}
              />
            </div>
            <div className="mt-4">
              <BundleSection
                onExport={tau.exportBundle}
                onImport={tau.importBundle}
              />
            </div>
            <div className="mt-4">
              <DoctorSection onRun={tau.runDoctor} />
            </div>
            <Composer
              peers={tau.peers}
              groups={tau.groups}
              target={target}
              mode={mode}
              onTargetChange={handleTargetChange}
              onModeChange={changeMode}
              onSend={tau.send}
              onSendGroup={tau.sendGroup}
              onSendFile={tau.sendFile}
              replyTo={replyTo}
              onCancelReply={() => setReplyTo(null)}
            />
          </div>

          <MessagesBackupSection
            exporting={tau.exporting}
            importing={tau.importing}
            onExport={tau.exportMessages}
            onImport={tau.importMessages}
          />
        </main>

        <footer className="mx-auto flex w-full max-w-7xl flex-wrap items-center justify-between gap-2 px-4 pb-6 font-mono text-[10px] text-mist/60 lg:px-8">
          <span>
            tau {tau.node?.version ?? "—"} · Reticulum{" "}
            {tau.node?.rns_version ?? "—"}
          </span>
          <span>no servers · no accounts · encrypted links</span>
        </footer>
      </div>
    </div>
  );
}
