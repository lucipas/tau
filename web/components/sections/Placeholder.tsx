// Placeholder.tsx — tidy stand-ins for the sections the NEXT agent builds.
//
// CONTRACT FOR THE NEXT AGENT — replace each exported section component
// below with the real implementation, keeping the same export names and
// file layout so page.tsx needs no changes:
//
//   web/components/sections/SocialSection.tsx   → export default SocialSection
//   web/components/sections/ChannelsSection.tsx → export default ChannelsSection
//   web/components/sections/ToolsSection.tsx    → export default ToolsSection
//   web/components/sections/SettingsSection.tsx → export default SettingsSection
//   web/components/sections/PagesSection.tsx    → export default PagesSection
//
// (Splitting each into its own file is preferred; until then they live here
// as thin wrappers around Placeholder.) Each section can consume the shared
// snapshot via `useTauContext()` from `../chrome/TauContext`, and should
// use the shared `Avatar` from `../chrome/Avatar`. Hooks available in
// web/lib (do not edit lib/):
//   Social:   useTanFeed / useTanThread / useTanComposer / useTanLists
//             (web/lib/use-tan.ts)
//   Channels: useChannels (web/lib/use-channels.ts)
//   Pages:    usePages (web/lib/use-pages.ts)
//   Tools:    api.ts (doctor, backup, allowlist, export/import) + useTau()
//   Settings: api.ts getSettings/updateSetting, renameNode, announce

"use client";

import { useTauContext } from "../chrome/TauContext";

export interface PlaceholderProps {
  title: string;
  blurb: string;
  /** Hook(s) / API surface the real section should build on. */
  buildOn?: string;
}

export function Placeholder({ title, blurb, buildOn }: PlaceholderProps) {
  let tau: ReturnType<typeof useTauContext> | null = null;
  try {
    tau = useTauContext();
  } catch {
    tau = null;
  }
  return (
    <div className="flex flex-col items-center gap-3 px-6 py-16 text-center">
      <span aria-hidden="true" className="text-4xl text-cyan-400">τ</span>
      <h1 className="text-xl font-bold text-zinc-50">{title}</h1>
      <p className="max-w-sm text-sm leading-relaxed text-zinc-400">{blurb}</p>
      {buildOn && (
        <p className="max-w-sm rounded-lg border border-zinc-800 bg-zinc-900 px-3 py-2 text-xs leading-relaxed text-zinc-500">
          Next up: build this on <code className="text-zinc-300">{buildOn}</code>
        </p>
      )}
      <p className="text-xs text-zinc-600">
        {tau?.node ? `Connected as ${tau.node.name}` : "Waiting for the node…"}
      </p>
    </div>
  );
}

export function SocialSection() {
  return (
    <Placeholder
      title="Social"
      blurb="The Tan feed lives here — posts, replies, likes, polls and follows from across the mesh."
      buildOn="useTanFeed / useTanThread / useTanComposer / useTanLists (web/lib/use-tan.ts)"
    />
  );
}

export function ChannelsSection() {
  return (
    <Placeholder
      title="Channels"
      blurb="Public channels live here — join a channel, read its history, and post to the mesh."
      buildOn="useChannels (web/lib/use-channels.ts)"
    />
  );
}

export function ToolsSection() {
  return (
    <Placeholder
      title="Tools"
      blurb="Node tools live here — health checks, backup and restore, the allow-list, and bundle export/import."
      buildOn="api.ts getDoctor / backupUrl / getAllowlist / exportBundle (web/lib/api.ts)"
    />
  );
}

export function SettingsSection() {
  return (
    <Placeholder
      title="Settings"
      blurb="Node settings live here — your node's name, file acceptance, read receipts, and disappearing defaults."
      buildOn="api.ts getSettings / updateSetting / renameNode (web/lib/api.ts)"
    />
  );
}

export function PagesSection() {
  return (
    <Placeholder
      title="Pages"
      blurb="Personal mesh pages live here — your page and the pages your peers publish."
      buildOn="usePages (web/lib/use-pages.ts)"
    />
  );
}
