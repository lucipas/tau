// page.tsx — section router. Reads the URL hash and renders the active
// section in the center column. Chat is built here; the other sections are
// placeholders (web/components/sections/Placeholder.tsx) for the next agent.

"use client";

import { useHash } from "@/components/chrome/useHash";
import RightSidebar from "@/components/chrome/RightSidebar";
import ChatSection from "@/components/chat/ChatSection";
import {
  ChannelsSection,
  PagesSection,
  SettingsSection,
  SocialSection,
  ToolsSection,
} from "@/components/sections/Placeholder";

export default function Home() {
  const { section } = useHash();

  switch (section) {
    case "chat":
      return <ChatSection />;
    case "social":
      return <SocialSection />;
    case "channels":
      return <ChannelsSection />;
    case "tools":
      return <ToolsSection />;
    case "settings":
      return <SettingsSection />;
    case "pages":
      return <PagesSection />;
    case "people":
      // Mobile "People" tab: the right-sidebar content in the center column.
      return <RightSidebar />;
    default:
      return <ChatSection />;
  }
}
