// Placeholder.tsx — re-export shim.
//
// The real sections now live in their own files; page.tsx still imports
// the named section components from here, so this file re-exports them
// unchanged:
//
//   web/components/sections/SocialSection.tsx   → default SocialSection
//   web/components/sections/ChannelsSection.tsx → default ChannelsSection
//   web/components/sections/ToolsSection.tsx    → default ToolsSection
//   web/components/sections/SettingsSection.tsx → default SettingsSection
//   web/components/sections/PagesSection.tsx    → default PagesSection

"use client";

export { default as SocialSection } from "./SocialSection";
export { default as ChannelsSection } from "./ChannelsSection";
export { default as ToolsSection } from "./ToolsSection";
export { default as SettingsSection } from "./SettingsSection";
export { default as PagesSection } from "./PagesSection";
