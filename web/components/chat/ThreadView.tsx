// ThreadView.tsx — one open conversation: header (back, name, typing),
// message list, composer. Message actions are wired to api.ts here via the
// parent's callbacks; this file owns filtering + presentation only.

"use client";

import { useEffect, useRef } from "react";
import type { ChatMessage } from "@/lib/types";
import type { Conversation } from "@/lib/use-tau";
import MessageBubble, { type MessageAction } from "./MessageBubble";
import Composer, { type ComposerSendOptions } from "./Composer";

export function filterMessagesForConversation(
  messages: ChatMessage[],
  conversation: Conversation,
): ChatMessage[] {
  const filtered = messages.filter((m) =>
    conversation.kind === "group" ? m.group === conversation.key : m.peer === conversation.key && !m.group,
  );
  return [...filtered].sort((a, b) => (a.sent_at || a.received_at) - (b.sent_at || b.received_at));
}

export interface ThreadViewProps {
  conversation: Conversation;
  messages: ChatMessage[];
  typingName?: string | null;
  replyTo?: ChatMessage | null;
  onReplyToChange?: (message: ChatMessage | null) => void;
  editing?: ChatMessage | null;
  onEditingChange?: (message: ChatMessage | null) => void;
  forwardTargets: { key: string; kind: "dm" | "group"; name: string }[];
  onMessageAction: (action: MessageAction, message: ChatMessage) => void;
  onForward: (message: ChatMessage, target: string) => void;
  onReact: (message: ChatMessage, reaction: string) => void;
  onClearReaction: (message: ChatMessage) => void;
  /** Snooze a received message; undefined hides the snooze action. */
  onSnooze?: (message: ChatMessage, inSeconds: number) => void;
  /** Voting/closing a poll; undefined disables voting in the bubble. */
  onVotePoll?: (message: ChatMessage, option: number) => void;
  onClosePoll?: (message: ChatMessage) => void;
  onSend: (text: string, opts: ComposerSendOptions) => Promise<void> | void;
  onSendFile: (file: File, opts: Omit<ComposerSendOptions, "pollOptions">) => Promise<void> | void;
  /** A saved draft to load into the composer (set when its conversation opens). */
  draftSeed?: { key: string; text: string } | null;
  /** Park the composer's text as a draft for this conversation. */
  onSaveDraft?: (text: string) => Promise<void> | void;
  onBack: () => void;
  onOpenPeerPanel?: () => void;
}

export default function ThreadView({
  conversation,
  messages,
  typingName,
  replyTo,
  onReplyToChange,
  editing,
  onEditingChange,
  forwardTargets,
  onMessageAction,
  onForward,
  onReact,
  onClearReaction,
  onSnooze,
  onVotePoll,
  onClosePoll,
  onSend,
  onSendFile,
  draftSeed,
  onSaveDraft,
  onBack,
  onOpenPeerPanel,
}: ThreadViewProps) {
  const thread = filterMessagesForConversation(messages, conversation);
  const listRef = useRef<HTMLDivElement>(null);
  const pinnedCount = thread.filter((m) => m.pinned).length;
  // Announcements-only groups admit posts only from the creator; a
  // member still reads, reacts and votes — the composer is the only
  // thing locked.
  const announcementsLocked =
    conversation.kind === "group" &&
    Boolean(conversation.group?.announcements_only) &&
    !conversation.group?.created_by_me;

  useEffect(() => {
    const el = listRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [thread.length, typingName]);

  return (
    <div className="flex h-full min-w-0 flex-col" aria-label={`Conversation with ${conversation.name}`}>
      <header className="flex items-center gap-2 border-b border-zinc-800 px-3 py-2.5">
        <button
          type="button"
          onClick={onBack}
          aria-label="Back to conversations"
          className="rounded-full px-2 py-1 text-lg leading-none text-zinc-300 hover:bg-zinc-800"
        >
          ←
        </button>
        <button type="button" onClick={onOpenPeerPanel} className="min-w-0 flex-1 text-left" title="Open peer details">
          <span className="block truncate text-[15px] font-bold text-zinc-50">
            {conversation.kind === "group" ? "# " : ""}
            {conversation.name}
          </span>
          <span className="block truncate text-xs text-zinc-500">
            {typingName
              ? `${typingName} is typing…`
              : conversation.kind === "group"
                ? `${conversation.group?.members.length ?? 0} members${conversation.group?.topic ? ` · ${conversation.group.topic}` : ""}${conversation.group?.secret ? " · Secret" : ""}${conversation.group?.announcements_only ? " · Announcements only" : ""}`
                : conversation.peer?.blocked
                  ? "Blocked"
                  : conversation.peer?.verified
                    ? "✓ Verified"
                    : conversation.peer
                      ? "Peer"
                      : "Direct message"}
            {pinnedCount > 0 ? ` · 📌 ${pinnedCount} pinned` : ""}
          </span>
        </button>
        {conversation.muted && (
          <span className="shrink-0 text-xs text-zinc-500" title="Muted — unread counts are hidden">🔇</span>
        )}
      </header>

      <div ref={listRef} className="min-h-0 flex-1 overflow-y-auto py-2" role="log" aria-label="Messages">
        {thread.length === 0 ? (
          <p className="px-4 py-8 text-center text-sm text-zinc-500">
            No messages yet. Say hello — it travels over the mesh, not a server.
          </p>
        ) : (
          thread.map((message) => (
            <MessageBubble
              key={message.id}
              message={message}
              senderName={
                conversation.kind === "group"
                  ? message.direction === "out"
                    ? "You"
                    : message.peer_name || undefined
                  : undefined
              }
              senderSeed={message.peer ?? undefined}
              showAvatar={conversation.kind === "group" && message.direction === "in"}
              forwardTargets={forwardTargets.filter(
                (t) => !(t.kind === conversation.kind && t.key === conversation.key),
              )}
              onAction={onMessageAction}
              onForward={onForward}
              onReact={onReact}
              onClearReaction={onClearReaction}
              onSnooze={onSnooze}
              onVotePoll={onVotePoll}
              onClosePoll={onClosePoll}
            />
          ))
        )}
        {typingName && (
          <p className="px-5 py-1 text-xs italic text-zinc-500" aria-live="polite">
            {typingName} is typing…
          </p>
        )}
      </div>

      {announcementsLocked && (
        <p className="px-4 pb-1 text-center text-xs text-zinc-500">
          Only the creator can post here — you can still react, vote, and edit your own
          messages.
        </p>
      )}
      <Composer
        placeholder={`Message ${conversation.name}…`}
        replyTo={replyTo}
        onCancelReply={() => onReplyToChange?.(null)}
        editing={editing}
        onCancelEdit={() => onEditingChange?.(null)}
        allowPoll
        // Locked for new posts, but an edit of your own earlier message
        // still goes through — the send path allows it, and the composer
        // is the edit's only submit button.
        disabled={announcementsLocked && !editing}
        draftSeed={draftSeed}
        onSaveDraft={onSaveDraft}
        onSend={onSend}
        onSendFile={onSendFile}
      />
    </div>
  );
}
