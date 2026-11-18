// LeftNav.tsx — desktop left rail: brand, section nav, live chip, unread chip.

"use client";

import type { Section } from "./useHash";
import { SECTIONS } from "./useHash";
import UnreadChip from "./UnreadChip";
import {
  ChannelsIcon,
  ChatIcon,
  PagesIcon,
  SettingsIcon,
  SocialIcon,
  ToolsIcon,
} from "./icons";
import type { UseTauResult } from "@/lib/use-tau";

const NAV_META: Record<
  (typeof SECTIONS)[number],
  { label: string; icon: (props: { className?: string }) => React.ReactElement; shortcut: string }
> = {
  chat: { label: "Chat", icon: ChatIcon, shortcut: "Alt+1" },
  social: { label: "Social", icon: SocialIcon, shortcut: "Alt+2" },
  channels: { label: "Channels", icon: ChannelsIcon, shortcut: "Alt+3" },
  tools: { label: "Tools", icon: ToolsIcon, shortcut: "Alt+4" },
  settings: { label: "Settings", icon: SettingsIcon, shortcut: "Alt+5" },
  pages: { label: "Pages", icon: PagesIcon, shortcut: "Alt+6" },
};

export const NAV_SECTIONS = SECTIONS;

interface LeftNavProps {
  active: Section;
  unreadTotal: number;
  tau: Pick<UseTauResult, "transport" | "node">;
  onNavigate: (section: Section) => void;
}

export function LiveChip({ transport }: { transport: UseTauResult["transport"] }) {
  const config =
    transport === "ws"
      ? { dot: "bg-emerald-400", label: "Live", text: "text-emerald-300" }
      : transport === "rest"
        ? { dot: "bg-amber-400", label: "Polling", text: "text-amber-300" }
        : { dot: "bg-red-400", label: "Offline", text: "text-red-300" };
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full border border-zinc-800 px-2 py-0.5 text-xs font-medium ${config.text}`}
      title={transport === "ws" ? "Live updates over WebSocket" : transport === "rest" ? "Live socket down — polling REST" : "Bridge unreachable"}
    >
      <span aria-hidden="true" className={`h-2 w-2 rounded-full ${config.dot}`} />
      {config.label}
    </span>
  );
}

export default function LeftNav({ active, unreadTotal, tau, onNavigate }: LeftNavProps) {
  return (
    <nav aria-label="Sections" className="flex w-60 shrink-0 flex-col gap-1 px-3 py-4">
      <a href="#chat" onClick={(e) => { e.preventDefault(); onNavigate("chat"); }} className="mb-4 flex items-center gap-2 px-2">
        <span aria-hidden="true" className="text-2xl font-bold text-cyan-400">τ</span>
        <span className="text-sm font-semibold tracking-widest text-zinc-100">
          MESH <span className="text-zinc-500">CONSOLE</span>
        </span>
      </a>

      {SECTIONS.map((section) => {
        const meta = NAV_META[section];
        const Icon = meta.icon;
        const isActive = active === section;
        return (
          <a
            key={section}
            href={`#${section}`}
            aria-current={isActive ? "page" : undefined}
            onClick={(e) => { e.preventDefault(); onNavigate(section); }}
            title={`${meta.label} (${meta.shortcut})`}
            className={`flex items-center gap-3 rounded-full px-4 py-2.5 text-[15px] font-medium transition-colors ${
              isActive ? "bg-zinc-800 font-bold text-zinc-50" : "text-zinc-300 hover:bg-zinc-900 hover:text-zinc-50"
            }`}
          >
            <Icon className="h-5 w-5" />
            <span className="flex-1">{meta.label}</span>
            {section === "chat" && <UnreadChip count={unreadTotal} />}
          </a>
        );
      })}

      <div className="mt-auto flex flex-col gap-2 px-2 pt-4">
        <LiveChip transport={tau.transport} />
        {tau.node && (
          <p className="truncate text-xs text-zinc-500" title={tau.node.destination}>
            {tau.node.name} · {tau.node.peers} peer{tau.node.peers === 1 ? "" : "s"}
          </p>
        )}
      </div>
    </nav>
  );
}
