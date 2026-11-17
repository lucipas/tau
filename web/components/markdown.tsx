// A small, safe Markdown renderer for signed mesh pages and post text.
// Pages arrive from peers, so their Markdown is untrusted: this builds
// React elements only — text is escaped by construction, links allow
// http(s) and nothing else, and no HTML from the source is ever injected.
// No external dependencies.
"use client";

import type { ReactNode } from "react";

const INLINE_PATTERN =
  /(`[^`\n]+`|\*\*[^*\n]+\*\*|\*[^*\n]+\*|\[[^\]\n]+\]\([^)\n]+\))/g;
const LINK_PATTERN = /^\[([^\]]+)\]\(([^)]+)\)$/;

function renderInline(text: string, keyPrefix: string): ReactNode[] {
  return text.split(INLINE_PATTERN).map((part, index) => {
    const key = `${keyPrefix}-${index}`;
    if (part.startsWith("`") && part.endsWith("`") && part.length > 2) {
      return <code key={key}>{part.slice(1, -1)}</code>;
    }
    if (part.startsWith("**") && part.endsWith("**") && part.length > 4) {
      return <strong key={key}>{part.slice(2, -2)}</strong>;
    }
    if (part.startsWith("*") && part.endsWith("*") && part.length > 2) {
      return <em key={key}>{part.slice(1, -1)}</em>;
    }
    const link = LINK_PATTERN.exec(part);
    if (link !== null) {
      const url = link[2].trim();
      // Only http(s) links leave the page: a javascript: or data:
      // href from a peer must never become clickable.
      if (/^https?:\/\//.test(url)) {
        return (
          <a key={key} href={url} target="_blank" rel="noreferrer">
            {link[1]}
          </a>
        );
      }
    }
    return <span key={key}>{part}</span>;
  });
}

const HEADING_CLASS = [
  "text-lg font-semibold",
  "text-base font-semibold",
  "text-sm font-semibold",
  "text-sm font-semibold",
  "text-sm font-medium",
  "text-sm font-medium",
] as const;

function isListItem(line: string): boolean {
  const trimmed = line.trimStart();
  return /^[-*]\s+/.test(trimmed) || /^\d+[.)]\s+/.test(trimmed);
}

function listItemText(line: string): string {
  const trimmed = line.trimStart();
  return trimmed.replace(/^[-*]\s+/, "").replace(/^\d+[.)]\s+/, "");
}

function renderBlocks(text: string): ReactNode[] {
  const lines = text.split("\n");
  const blocks: ReactNode[] = [];
  let index = 0;
  let key = 0;
  while (index < lines.length) {
    const line = lines[index];
    if (line.trim() === "") {
      index += 1;
      continue;
    }
    const fence = line.trimStart();
    if (fence.startsWith("```")) {
      const collected: string[] = [];
      index += 1;
      while (
        index < lines.length &&
        !lines[index].trimStart().startsWith("```")
      ) {
        collected.push(lines[index]);
        index += 1;
      }
      index += 1;
      blocks.push(
        <pre
          key={key++}
          className="overflow-x-auto rounded-lg bg-void/70 px-3 py-2 text-xs"
        >
          <code>{collected.join("\n")}</code>
        </pre>,
      );
      continue;
    }
    const heading = /^(#{1,6})\s+(.*)$/.exec(line);
    if (heading !== null) {
      const level = heading[1].length;
      const Tag = `h${level}` as "h1" | "h2" | "h3" | "h4" | "h5" | "h6";
      const blockKey = key++;
      blocks.push(
        <Tag key={blockKey} className={HEADING_CLASS[level - 1]}>
          {renderInline(heading[2], `h${blockKey}`)}
        </Tag>,
      );
      index += 1;
      continue;
    }
    if (/^\s*(-{3,}|\*{3,}|_{3,})\s*$/.test(line)) {
      blocks.push(<hr key={key++} className="border-line-soft" />);
      index += 1;
      continue;
    }
    if (fence.startsWith(">")) {
      const quoted: string[] = [];
      while (
        index < lines.length &&
        lines[index].trimStart().startsWith(">")
      ) {
        quoted.push(lines[index].trimStart().replace(/^>\s?/, ""));
        index += 1;
      }
      const quoteKey = key++;
      blocks.push(
        <blockquote
          key={quoteKey}
          className="border-l-2 border-line pl-3 text-mist"
        >
          {renderInline(quoted.join(" "), `q${quoteKey}`)}
        </blockquote>,
      );
      continue;
    }
    if (isListItem(line)) {
      const ordered = /^\d+[.)]\s+/.test(line.trimStart());
      const items: string[] = [];
      while (index < lines.length && isListItem(lines[index])) {
        // Stop the list if the marker style flips between bullets
        // and numbers mid-list.
        const nextOrdered = /^\d+[.)]\s+/.test(
          lines[index].trimStart(),
        );
        if (nextOrdered !== ordered) break;
        items.push(listItemText(lines[index]));
        index += 1;
      }
      const listKey = key++;
      const ListTag = ordered ? "ol" : "ul";
      blocks.push(
        <ListTag
          key={listKey}
          className={
            ordered
              ? "list-decimal space-y-1 pl-5"
              : "list-disc space-y-1 pl-5"
          }
        >
          {items.map((item, itemIndex) => (
            <li key={itemIndex}>
              {renderInline(item, `li${listKey}-${itemIndex}`)}
            </li>
          ))}
        </ListTag>,
      );
      continue;
    }
    // Plain paragraph: gather consecutive non-blank, non-block lines.
    const paragraph: string[] = [];
    while (
      index < lines.length &&
      lines[index].trim() !== "" &&
      !lines[index].trimStart().startsWith("```") &&
      !/^(#{1,6})\s+/.test(lines[index]) &&
      !/^\s*(-{3,}|\*{3,}|_{3,})\s*$/.test(lines[index]) &&
      !lines[index].trimStart().startsWith(">") &&
      !isListItem(lines[index])
    ) {
      paragraph.push(lines[index]);
      index += 1;
    }
    const paraKey = key++;
    blocks.push(
      <p key={paraKey} className="leading-relaxed break-words">
        {renderInline(paragraph.join(" "), `p${paraKey}`)}
      </p>,
    );
  }
  return blocks;
}

export function Markdown({ text }: { text: string }) {
  return (
    <div className="flex min-w-0 flex-col gap-2 text-sm break-words">
      {renderBlocks(text)}
    </div>
  );
}
