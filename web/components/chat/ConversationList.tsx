// ConversationList.tsx — DM + group list with unread badges, last-message
// preview, muted handling, archived section, ignored-groups section, and
// the DM requests entry.

"use client";

import { useState } from "react";
import { formatRelative } from "@/lib/format";
import { useNow } from "@/lib/use-now";
import type { Conversation, IgnoredGroup } from "@/lib/use-tau";
import type { DmRequest } from "@/lib/types";
import Avatar from "../chrome/Avatar";
import UnreadChip from "../chrome/UnreadChip";

export interface ConversationListProps {
  conversations: Conversation[];
  archived: Conversation[];
  ignoredGroups: IgnoredGroup[];
  dmRequests: DmRequest[];
  selectedKey?: string | null;
  onSelect: (conversation: Conversation) => void;
  onSelectIgnoredGroup?: (group: IgnoredGroup) => void;
  onUnignoreGroup?: (group: IgnoredGroup) => void;
  onAcceptRequest?: (request: DmRequest) => void;
  onDeclineRequest?: (request: DmRequest) => void;
  /** True when this conversation kind is muted for the row (already on the
   *  Conversation object; kept as a prop hook for the parent's mute toggles). */
  now?: number;
}

export default function ConversationList({
  conversations,
  archived,
  ignoredGroups,
  dmRequests,
  selectedKey,
  onSelect,
  onSelectIgnoredGroup,
  onUnignoreGroup,
  onAcceptRequest,
  onDeclineRequest,
  now: nowProp,
}: ConversationListProps) {
  const liveNow = useNow();
  const now = nowProp ?? liveNow;
  const [showArchived, setShowArchived] = useState(false);
  const [showIgnored, setShowIgnored] = useState(false);

  return (
    <div className="flex h-full flex-col" aria-label="Conversations">
      {dmRequests.length > 0 && (
        <section aria-label="DM requests" className="border-b border-zinc-800 px-3 py-2">
          <h2 className="mb-1 text-xs font-bold uppercase tracking-wide text-zinc-500">
            Message requests ({dmRequests.length})
          </h2>
          <ul className="flex flex-col gap-1">
            {dmRequests.map((req) => (
              <li key={req.id} className="rounded-lg bg-zinc-900 px-2 py-1.5">
                <div className="flex items-center gap-2">
                  <Avatar name={req.sender_name} seed={req.sender} size="sm" />
                  <div className="min-w-0 flex-1">
                    <p className="truncate text-sm font-semibold text-zinc-100">{req.sender_name || req.sender.slice(0, 8)}</p>
                    {req.text && <p className="truncate text-xs text-zinc-400">{req.text}</p>}
                  </div>
                  <button
                    type="button"
                    onClick={() => onAcceptRequest?.(req)}
                    className="rounded-full bg-cyan-500 px-2.5 py-1 text-xs font-bold text-zinc-950"
                  >
                    Accept
                  </button>
                  <button
                    type="button"
                    onClick={() => onDeclineRequest?.(req)}
                    aria-label={`Decline request from ${req.sender_name || "unknown"}`}
                    className="rounded-full border border-zinc-700 px-2.5 py-1 text-xs text-zinc-300"
                  >
                    Decline
                  </button>
                </div>
              </li>
            ))}
          </ul>
        </section>
      )}

      <ul className="min-h-0 flex-1 overflow-y-auto">
        {conversations.length === 0 && (
          <li className="px-4 py-6 text-center text-sm text-zinc-500">
            No conversations yet — messages from peers will appear here.
          </li>
        )}
        {conversations.map((conv) => {
          const isActive = selectedKey === `${conv.kind}:${conv.key}`;
          const last = conv.lastMessage;
          return (
            <li key={`${conv.kind}:${conv.key}`}>
              <button
                type="button"
                onClick={() => onSelect(conv)}
                aria-current={isActive ? "true" : undefined}
                className={`flex w-full items-center gap-3 border-b border-zinc-800/60 px-3 py-3 text-left hover:bg-zinc-900 ${
                  isActive ? "bg-zinc-900" : ""
                }`}
              >
                <span className="relative shrink-0">
                  <Avatar
                    name={conv.name}
                    seed={conv.kind === "group" ? conv.key : conv.peer?.destination ?? conv.key}
                    size="md"
                  />
                  {conv.kind === "group" && (
                    <span aria-hidden="true" className="absolute -bottom-1 -right-1 flex h-4 w-4 items-center justify-center rounded-full bg-zinc-700 text-[9px] text-zinc-200">
                      #
                    </span>
                  )}
                </span>
                <span className="min-w-0 flex-1">
                  <span className="flex items-baseline justify-between gap-2">
                    <span className={`truncate text-[15px] ${conv.unread > 0 ? "font-bold text-zinc-50" : "font-medium text-zinc-200"}`}>
                      {conv.pinned && <span title="Pinned" aria-label="Pinned conversation">📌 </span>}
                      {conv.muted && <span title="Muted" aria-label="Muted conversation">🔇 </span>}
                      {conv.name}
                    </span>
                    {last && (
                      <span className="shrink-0 text-xs text-zinc-500">
                        {formatRelative(last.sent_at || last.received_at, now)}
                      </span>
                    )}
                  </span>
                  <span className="flex items-center justify-between gap-2">
                    <span className={`min-w-0 flex-1 truncate text-sm ${conv.unread > 0 ? "text-zinc-300" : "text-zinc-500"}`}>
                      {last ? (
                        <>
                          {last.direction === "out" ? "You: " : ""}
                          {last.file ? `📎 ${last.file.name}` : last.poll_options ? `📊 ${last.text || "Poll"}` : last.text || "…"}
                        </>
                      ) : (
                        "No messages yet"
                      )}
                    </span>
                    {conv.unread > 0 && !conv.muted && <UnreadChip count={conv.unread} />}
                    {conv.unread > 0 && conv.muted && (
                      <span className="text-xs text-zinc-500" title={`${conv.unread} unread (muted)`}>
                        {conv.unread}
                      </span>
                    )}
                  </span>
                </span>
              </button>
            </li>
          );
        })}
      </ul>

      {ignoredGroups.length > 0 && (
        <section aria-label="Ignored groups" className="border-t border-zinc-800">
          <button
            type="button"
            aria-expanded={showIgnored}
            onClick={() => setShowIgnored((v) => !v)}
            className="flex w-full items-center justify-between px-3 py-2 text-xs font-bold uppercase tracking-wide text-zinc-500 hover:text-zinc-300"
          >
            <span>Ignored groups ({ignoredGroups.length})</span>
            <span aria-hidden="true">{showIgnored ? "▾" : "▸"}</span>
          </button>
          {showIgnored && (
            <ul>
              {ignoredGroups.map((group) => (
                <li key={`ignored:${group.id}`}>
                  <div className="flex w-full items-center gap-3 px-3 py-2 text-left text-sm text-zinc-400">
                    <Avatar name={group.name} seed={group.id} size="sm" />
                    {group.openable ? (
                      <button
                        type="button"
                        onClick={() => onSelectIgnoredGroup?.(group)}
                        className="min-w-0 flex-1 truncate text-left hover:text-zinc-200"
                      >
                        {group.name}
                      </button>
                    ) : (
                      <span className="min-w-0 flex-1 truncate" title={group.id}>
                        {group.name}
                      </span>
                    )}
                    <button
                      type="button"
                      onClick={() => onUnignoreGroup?.(group)}
                      aria-label={`Un-ignore ${group.name}`}
                      className="shrink-0 rounded-full border border-zinc-700 px-2.5 py-1 text-xs text-zinc-300 hover:bg-zinc-800"
                    >
                      Un-ignore
                    </button>
                  </div>
                </li>
              ))}
            </ul>
          )}
        </section>
      )}

      {archived.length > 0 && (
        <section aria-label="Archived conversations" className="border-t border-zinc-800">
          <button
            type="button"
            aria-expanded={showArchived}
            onClick={() => setShowArchived((v) => !v)}
            className="flex w-full items-center justify-between px-3 py-2 text-xs font-bold uppercase tracking-wide text-zinc-500 hover:text-zinc-300"
          >
            <span>Archived ({archived.length})</span>
            <span aria-hidden="true">{showArchived ? "▾" : "▸"}</span>
          </button>
          {showArchived && (
            <ul>
              {archived.map((conv) => (
                <li key={`archived:${conv.kind}:${conv.key}`}>
                  <button
                    type="button"
                    onClick={() => onSelect(conv)}
                    className="flex w-full items-center gap-3 px-3 py-2 text-left text-sm text-zinc-400 hover:bg-zinc-900 hover:text-zinc-200"
                  >
                    <Avatar name={conv.name} seed={conv.key} size="sm" />
                    <span className="min-w-0 flex-1 truncate">{conv.name}</span>
                    {conv.unread > 0 && <UnreadChip count={conv.unread} />}
                  </button>
                </li>
              ))}
            </ul>
          )}
        </section>
      )}
    </div>
  );
}
