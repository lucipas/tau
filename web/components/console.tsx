"use client";

import { useEffect, useRef, useState } from "react";

import { useTau } from "@/lib/use-tau";
import type { TauMessage } from "@/lib/types";

import { Composer } from "./composer";
import { Header } from "./header";
import { MessageFeed } from "./message-feed";
import { OutboxSection } from "./outbox-section";
import { PeerSidebar } from "./peer-sidebar";
import { SettingsSection } from "./settings-section";

export function Console() {
  const tau = useTau();
  const [target, setTarget] = useState("");
  const [mode, setMode] = useState<"peer" | "custom">("peer");
  const [replyTo, setReplyTo] = useState<TauMessage | null>(null);
  const customPicked = useRef(false);

  useEffect(() => {
    if (tau.peers.length === 0) {
      setMode("custom");
      return;
    }
    if (!target && !customPicked.current) {
      setMode("peer");
      setTarget(tau.peers[0].destination);
    }
  }, [tau.peers, target]);

  function changeMode(next: "peer" | "custom") {
    customPicked.current = next === "custom";
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
          <PeerSidebar
            node={tau.node}
            peers={tau.peers}
            groups={tau.groups}
            bridge={tau.bridge}
            pings={tau.pings}
            unread={tau.unread.per_peer}
            selected={target}
            onPing={(destination) => {
              void tau.ping(destination);
            }}
            onSelectPeer={selectPeer}
          />

          <div className="flex min-w-0 flex-col">
            <MessageFeed
              messages={tau.messages}
              node={tau.node}
              live={tau.connection === "live"}
              onReply={(message) => {
                setReplyTo(message);
                if (message.peer) {
                  setTarget(message.peer);
                  setMode("peer");
                }
              }}
            />
            {tau.outbox.length > 0 ? (
              <div className="mt-4">
                <OutboxSection
                  entries={tau.outbox}
                  heldForOthers={tau.heldForOthers}
                  onCancel={tau.cancelOutbox}
                />
              </div>
            ) : null}
            <Composer
              peers={tau.peers}
              target={target}
              mode={mode}
              onTargetChange={handleTargetChange}
              onModeChange={changeMode}
              onSend={tau.send}
              replyTo={replyTo}
              onCancelReply={() => setReplyTo(null)}
            />
            <div className="mt-4">
              <SettingsSection
                settings={tau.settings}
                loadError={tau.settingsError}
                onToggle={tau.updateSetting}
              />
            </div>
          </div>
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
