// useHash.ts — hash routing for the console shell.
//
// Sections live in the URL hash so refresh / deep-linking works with the
// static export: `#chat`, `#social`, `#channels`, `#tools`, `#settings`,
// `#pages`, `#people` (mobile peers view). Chat targets ride along as a
// query string: `#chat?peer=<destination>` or `#chat?group=<id>`.

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
}

export function parseHash(raw: string): ParsedHash {
  const stripped = raw.startsWith("#") ? raw.slice(1) : raw;
  const [sectionPart, query] = stripped.split("?");
  const section: Section = isSection(sectionPart ?? "") ? (sectionPart as Section) : "chat";
  const params = new URLSearchParams(query ?? "");
  const peer = params.get("peer") ?? undefined;
  const group = params.get("group") ?? undefined;
  return { section, peer, group };
}

export function buildHash(section: Section, opts: { peer?: string; group?: string } = {}): string {
  const params = new URLSearchParams();
  if (opts.peer) params.set("peer", opts.peer);
  if (opts.group) params.set("group", opts.group);
  const query = params.toString();
  return `#${section}${query ? `?${query}` : ""}`;
}

export interface UseHashResult extends ParsedHash {
  /** Raw hash string, e.g. "#chat?peer=ab12". */
  hash: string;
  navigate: (section: Section, opts?: { peer?: string; group?: string }) => void;
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

  const navigate = useCallback((section: Section, opts: { peer?: string; group?: string } = {}) => {
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
