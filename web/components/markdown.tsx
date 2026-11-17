import type { ReactNode } from "react";

// A small Markdown renderer for signed mesh pages. Pages arrive
// from peers, so their Markdown is untrusted: this renders to React
// elements only — text is escaped by construction, links allow
// http(s) and nothing else, and no HTML from the page is ever
// injected. No dependency: pages are at most 8 KiB and only need
// the common constructs (headings, emphasis, code, links, lists,
// quotes, rules, fenced code). Anything fancier renders as plain
// text rather than failing.

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
      // Only http(s) links leave the page: a javascript: or
      // data: href from a peer must never become clickable.
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

function startsBlock(line: string): boolean {
  const trimmed = line.trimStart();
  return (
    trimmed.startsWith("```") ||
    trimmed.startsWith(">") ||
    /^#{1,6}\s/.test(trimmed) ||
    /^\s*(-{3,}|\*{3,}|_{3,})\s*$/.test(line) ||
    /^[-*]\s+/.test(trimmed) ||
    /^\d+[.)]\s+/.test(trimmed)
  );
}

const HEADING_CLASS = [
  "text-lg font-semibold",
  "text-base font-semibold",
  "text-sm font-semibold",
  "text-sm font-semibold",
  "text-sm font-medium",
  "text-sm font-medium",
];

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
        <pre key={key++}>
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
      blocks.push(<hr key={key++} />);
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
      const blockKey = key++;
      blocks.push(
        <blockquote key={blockKey}>
          {quoted.map((quote, quoteIndex) => (
            <p key={quoteIndex}>
              {renderInline(quote, `q${blockKey}-${quoteIndex}`)}
            </p>
          ))}
        </blockquote>,
      );
      continue;
    }
    const unordered = /^[-*]\s+(.*)$/.exec(fence);
    const ordered = /^\d+[.)]\s+(.*)$/.exec(fence);
    if (unordered !== null || ordered !== null) {
      const orderedList = ordered !== null;
      const items: string[] = [];
      while (index < lines.length) {
        const itemLine = lines[index].trimStart();
        const match = orderedList
          ? /^\d+[.)]\s+(.*)$/.exec(itemLine)
          : /^[-*]\s+(.*)$/.exec(itemLine);
        if (match === null) break;
        items.push(match[1]);
        index += 1;
      }
      const listKey = key++;
      if (orderedList) {
        blocks.push(
          <ol key={listKey}>
            {items.map((item, itemIndex) => (
              <li key={itemIndex}>
                {renderInline(item, `ol${listKey}-${itemIndex}`)}
              </li>
            ))}
          </ol>,
        );
      } else {
        blocks.push(
          <ul key={listKey}>
            {items.map((item, itemIndex) => (
              <li key={itemIndex}>
                {renderInline(item, `ul${listKey}-${itemIndex}`)}
              </li>
            ))}
          </ul>,
        );
      }
      continue;
    }
    const paragraph: string[] = [];
    while (
      index < lines.length &&
      lines[index].trim() !== "" &&
      !startsBlock(lines[index])
    ) {
      paragraph.push(lines[index]);
      index += 1;
    }
    const blockKey = key++;
    blocks.push(
      <p key={blockKey}>
        {renderInline(paragraph.join(" "), `p${blockKey}`)}
      </p>,
    );
  }
  return blocks;
}

export function Markdown({ text }: { text: string }) {
  return (
    <div className="markdown-body flex flex-col gap-3 text-sm leading-relaxed">
      {renderBlocks(text)}
    </div>
  );
}
