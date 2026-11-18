// RichText.tsx — render Tan post text as React elements: #hashtags and
// @mentions become styled, clickable spans. No dangerouslySetInnerHTML
// anywhere: the text is split on a regex and each piece is a React node.

"use client";

import type { ReactNode } from "react";

export interface RichTextProps {
  text: string;
  /** Called with the bare tag name (no "#") when a hashtag is clicked. */
  onTag?: (tag: string) => void;
  /** Called with the bare handle (no "@") when a mention is clicked. */
  onMention?: (handle: string) => void;
}

const TOKEN_RE = /([#@][A-Za-z0-9_]+)/g;

export function renderRichText(
  text: string,
  onTag?: (tag: string) => void,
  onMention?: (handle: string) => void,
): ReactNode[] {
  return text.split(TOKEN_RE).map((part, i) => {
    if (part.startsWith("#") && part.length > 1) {
      const tag = part.slice(1);
      return (
        <button
          key={i}
          type="button"
          onClick={() => onTag?.(tag)}
          className="text-cyan-400 hover:underline"
        >
          {part}
        </button>
      );
    }
    if (part.startsWith("@") && part.length > 1) {
      const handle = part.slice(1);
      return (
        <button
          key={i}
          type="button"
          onClick={() => onMention?.(handle)}
          className="text-cyan-400 hover:underline"
        >
          {part}
        </button>
      );
    }
    return <span key={i}>{part}</span>;
  });
}

export default function RichText({ text, onTag, onMention }: RichTextProps) {
  return <>{renderRichText(text, onTag, onMention)}</>;
}
