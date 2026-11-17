// Fresh shell for the rewritten console: a header, a tab bar, and one tab
// at a time. Mobile-first single column; the chat tab switches to a
// two-column layout on large screens. Tabs sync to the URL hash so a
// refresh or a shared link lands on the same tab.
"use client";

import { useEffect, useState } from "react";

import ChannelsTab from "@/components/channels-tab";
import ChatTab from "@/components/chat-tab";
import PagesTab from "@/components/pages-tab";
import SettingsTab from "@/components/settings-tab";
import SocialTab from "@/components/social-tab";
import ToolsTab from "@/components/tools-tab";
import { Button, Chip } from "@/components/ui";
import { useChannels } from "@/lib/use-channels";
import { usePages } from "@/lib/use-pages";
import { useTan } from "@/lib/use-tan";
import { useTau } from "@/lib/use-tau";
import { formatUptime, shortHash } from "@/lib/format";

type Tab = "chat" | "social" | "channels" | "pages" | "tools" | "settings";

const TABS: { id: Tab; label: string }[] = [
  { id: "chat", label: "Chat" },
  { id: "social", label: "Social" },
  { id: "channels", label: "Channels" },
  { id: "pages", label: "Pages" },
  { id: "tools", label: "Tools" },
  { id: "settings", label: "Settings" },
];

function tabFromHash(): Tab {
  if (typeof window === "undefined") return "chat";
  const hash = window.location.hash.replace(/^#/, "");
  return (TABS.some((entry) => entry.id === hash) ? hash : "chat") as Tab;
}

export default function Home() {
  const tau = useTau();
  const tan = useTan();
  const channels = useChannels(tau.channelMessage);
  const pages = usePages();
  const [tab, setTab] = useState<Tab>("chat");

  useEffect(() => {
    setTab(tabFromHash());
    const onHashChange = () => setTab(tabFromHash());
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (!event.altKey) return;
      const index = Number.parseInt(event.key, 10) - 1;
      if (index >= 0 && index < TABS.length) {
        event.preventDefault();
        selectTab(TABS[index].id);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const selectTab = (next: Tab) => {
    setTab(next);
    window.history.replaceState(null, "", `#${next}`);
  };

  const live = tau.connection === "live";
  const unread = tau.unread.total;

  return (
    <div className="mx-auto flex min-h-dvh w-full max-w-6xl flex-col px-3 pb-6">
      <header className="flex flex-wrap items-center gap-x-3 gap-y-2 pt-4 pb-3">
        <div className="flex min-w-0 items-center gap-2.5">
          <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl border border-line bg-panel font-mono text-xl text-signal">
            τ
          </span>
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <h1 className="text-lg leading-none font-bold">tau</h1>
              <span className="rounded border border-line px-1.5 py-0.5 font-mono text-[10px] tracking-widest text-mist uppercase">
                mesh console
              </span>
            </div>
            <p className="clamp-1 mt-1 text-xs text-faint">
              {tau.node
                ? `${tau.node.name} · ${shortHash(tau.node.destination, 8)} · up ${formatUptime(tau.node.uptime)}`
                : "Reticulum peer-to-peer node"}
            </p>
          </div>
        </div>
        <div className="ml-auto flex shrink-0 items-center gap-2">
          <Chip>
            <span
              className={`h-2 w-2 rounded-full ${live ? "bg-signal" : "bg-amber"}`}
            />
            {live ? "live" : tau.connection}
          </Chip>
          {unread > 0 && (
            <Chip>
              {unread} unread
            </Chip>
          )}
          <Button onClick={() => void tau.announce()} disabled={tau.announcing}>
            ⚡ announce
          </Button>
        </div>
      </header>

      {!live && (
        <p className="mb-3 rounded-lg border border-amber/40 bg-amber/10 px-3 py-2 text-sm text-amber">
          {tau.connection === "offline"
            ? "The bridge is unreachable — check that tau serve is running, then reload."
            : "Reconnecting to the node…"}
        </p>
      )}

      <nav
        className="thin-scroll -mx-3 flex gap-1 overflow-x-auto px-3 pb-3"
        aria-label="Console sections"
      >
        {TABS.map((entry) => (
          <button
            key={entry.id}
            type="button"
            onClick={() => selectTab(entry.id)}
            aria-current={tab === entry.id ? "page" : undefined}
            className={`shrink-0 rounded-full px-3.5 py-1.5 text-sm font-medium ${
              tab === entry.id
                ? "bg-signal font-semibold text-void"
                : "border border-line text-mist hover:text-ink"
            }`}
          >
            {entry.label}
            {entry.id === "social" && tan.mentions.length > 0 && (
              <span className="ml-1.5 rounded-full bg-void/20 px-1.5 text-xs">
                {tan.mentions.length}
              </span>
            )}
          </button>
        ))}
      </nav>

      <main className="min-w-0 flex-1">
        {tab === "chat" && <ChatTab tau={tau} />}
        {tab === "social" && <SocialTab tau={tau} tan={tan} />}
        {tab === "channels" && (
          <ChannelsTab tau={tau} channels={channels} />
        )}
        {tab === "pages" && <PagesTab tau={tau} pages={pages} />}
        {tab === "tools" && <ToolsTab tau={tau} />}
        {tab === "settings" && <SettingsTab tau={tau} />}
      </main>

      <footer className="mt-6 border-t border-line-soft pt-3 text-center text-xs text-faint">
        tau {tau.node?.version ?? "0.1.0"} · Reticulum {tau.node?.rns_version ?? "—"} · no
        servers · no accounts · encrypted links
      </footer>
    </div>
  );
}
