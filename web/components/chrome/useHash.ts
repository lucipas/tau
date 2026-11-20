// useHash.ts — hash routing for the console shell.
//
// Sections live in the URL hash so refresh / deep-linking works with the
// static export: `#chat`, `#social`, `#channels`, `#tools`, `#settings`,
// `#pages`, `#people` (mobile peers view). Chat targets ride along as a
// query string: `#chat?peer=<destination>` or `#chat?group=<id>`; a channel
// target rides the same way: `#channels?channel=<name>`.

"use client";

import { useCallback, useEffect, useState } from "react";

export const SECTIONS = [
  "chat",
  "social",
  "channels",
  "tools",
  "settings",
  "pages",
] as const;

export type Section = (typeof SECTIONS)[number] | "people";

export function isSection(value: string): value is Section {
  return (SECTIONS as readonly string[]).includes(value) || value === "people";
}

export interface ParsedHash {
  section: Section;
  peer?: string;
  group?: string;
  channel?: string;
}

export function parseHash(raw: string): ParsedHash {
  const stripped = raw.startsWith("#") ? raw.slice(1) : raw;
  const [sectionPart, query] = stripped.split("?");
  const section: Section = isSection(sectionPart ?? "") ? (sectionPart as Section) : "chat";
  const params = new URLSearchParams(query ?? "");
  const peer = params.get("peer") ?? undefined;
  const group = params.get("group") ?? undefined;
  const channel = params.get("channel") ?? undefined;
  return { section, peer, group, channel };
}

export function buildHash(section: Section, opts: { peer?: string; group?: string; channel?: string } = {}): string {
  const params = new URLSearchParams();
  if (opts.peer) params.set("peer", opts.peer);
  if (opts.group) params.set("group", opts.group);
  if (opts.channel) params.set("channel", opts.channel);
  const query = params.toString();
  return `#${section}${query ? `?${query}` : ""}`;
}

export interface UseHashResult extends ParsedHash {
  /** Raw hash string, e.g. "#chat?peer=ab12". */
  hash: string;
  navigate: (section: Section, opts?: { peer?: string; group?: string; channel?: string }) => void;
}

export function useHash(): UseHashResult {
  const [hash, setHash] = useState<string>(() =>
    typeof window === "undefined" ? "#chat" : window.location.hash || "#chat",
  );

  useEffect(() => {
    const onChange = () => setHash(window.location.hash || "#chat");
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);

  const navigate = useCallback((section: Section, opts: { peer?: string; group?: string; channel?: string } = {}) => {
    const next = buildHash(section, opts);
    if (typeof window !== "undefined") {
      if (window.location.hash === next) {
        // Same hash: force a re-render for consumers keyed on navigation.
        setHash(next);
      } else {
        window.location.hash = next;
      }
    }
  }, []);

  return { ...parseHash(hash), hash, navigate };
}
