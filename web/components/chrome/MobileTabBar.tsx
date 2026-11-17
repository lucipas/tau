// MobileTabBar.tsx — bottom tab bar for <1024px: the six sections plus a
// People tab that opens the peers/traffic sidebar content.

"use client";

import type { Section } from "./useHash";
import UnreadChip from "./UnreadChip";
import {
  ChannelsIcon,
  ChatIcon,
  PagesIcon,
  PeopleIcon,
  SettingsIcon,
  SocialIcon,
  ToolsIcon,
} from "./icons";

const TABS: { section: Section; label: string; icon: (props: { className?: string }) => React.ReactElement }[] = [
  { section: "chat", label: "Chat", icon: ChatIcon },
  { section: "social", label: "Social", icon: SocialIcon },
  { section: "channels", label: "Channels", icon: ChannelsIcon },
  { section: "people", label: "People", icon: PeopleIcon },
  { section: "tools", label: "Tools", icon: ToolsIcon },
  { section: "settings", label: "Settings", icon: SettingsIcon },
  { section: "pages", label: "Pages", icon: PagesIcon },
];

interface MobileTabBarProps {
  active: Section;
  unreadTotal: number;
  onNavigate: (section: Section) => void;
}

export default function MobileTabBar({ active, unreadTotal, onNavigate }: MobileTabBarProps) {
  return (
    <nav
      aria-label="Sections"
      className="fixed inset-x-0 bottom-0 z-40 flex border-t border-zinc-800 bg-zinc-950/95 pb-[env(safe-area-inset-bottom)] backdrop-blur lg:hidden"
    >
      {TABS.map((tab) => {
        const Icon = tab.icon;
        const isActive = active === tab.section;
        return (
          <a
            key={tab.section}
            href={`#${tab.section}`}
            aria-current={isActive ? "page" : undefined}
            onClick={(e) => { e.preventDefault(); onNavigate(tab.section); }}
            className={`relative flex flex-1 flex-col items-center gap-0.5 py-2 text-[11px] ${
              isActive ? "font-semibold text-cyan-400" : "text-zinc-400 hover:text-zinc-100"
            }`}
          >
            <span className="relative">
              <Icon className="h-5 w-5" />
              {tab.section === "chat" && unreadTotal > 0 && (
                <span className="absolute -right-2.5 -top-1">
                  <UnreadChip count={unreadTotal} />
                </span>
              )}
            </span>
            {tab.label}
          </a>
        );
      })}
    </nav>
  );
}
