"use client";

import { useEffect, useRef, useState } from "react";

import { useTau } from "@/lib/use-tau";

import { Composer } from "./composer";
import { Header } from "./header";
import { MessageFeed } from "./message-feed";
import { OutboxSection } from "./outbox-section";
import { PeerSidebar } from "./peer-sidebar";

export function Console() {
  const tau = useTau();
  const [target, setTarget] = useState("");
  const [mode, setMode] = useState<"peer" | "custom">("peer");
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

  function selectPeer(destination: string) {
    customPicked.current = false;
    setMode("peer");
    setTarget(destination);
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
            bridge={tau.bridge}
            pings={tau.pings}
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
              onTargetChange={setTarget}
              onModeChange={changeMode}
              onSend={tau.send}
            />
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
