// ChatSection.tsx — the Chat section: conversation list + thread + outbox.
// Owns selection (via the URL hash: #chat?peer=… / #chat?group=…), reply /
// edit state, and wires every message action to api.ts.

"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  acceptDmRequest,
  clearReaction,
  closeDmPoll,
  closeGroupPoll,
  declineDmRequest,
  deleteMessage,
  editMessage,
  forwardMessage,
  getArchivedConversations,
  getMutedConversations,
  pinMessage,
  reactToMessage,
  retractMessage,
  sendFile,
  sendGroupFile,
  sendMessage,
  unpinMessage,
  voteDmPoll,
  voteGroupPoll,
  type SendMessageOptions,
} from "@/lib/api";
import type { ChatMessage } from "@/lib/types";
import {
  buildConversations,
  useConversations,
  useOutbox,
  usePeerActions,
  type Conversation,
} from "@/lib/use-tau";
import { useHash } from "../chrome/useHash";
import { useTauContext } from "../chrome/TauContext";
import ConversationList from "./ConversationList";
import MessageSearch from "./MessageSearch";
import OutboxPanel from "./OutboxPanel";
import ThreadView from "./ThreadView";
import type { MessageAction } from "./MessageBubble";
import type { ComposerSendOptions } from "./Composer";

function extractKeys(value: unknown): string[] {
  if (Array.isArray(value)) {
    const out: string[] = [];
    for (const item of value) {
      if (typeof item === "string") out.push(item);
      else if (item && typeof item === "object") {
        const record = item as Record<string, unknown>;
        for (const key of ["destination", "id", "target", "peer", "group"]) {
          if (typeof record[key] === "string") {
            out.push(record[key] as string);
            break;
          }
        }
      }
    }
    return out;
  }
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    for (const key of ["archived", "conversations", "peers", "groups"]) {
      if (record[key] !== undefined) return extractKeys(record[key]);
    }
  }
  return [];
}

function fileToBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const dataUrl = String(reader.result ?? "");
      resolve(dataUrl.split(",")[1] ?? "");
    };
    reader.onerror = () => reject(reader.error ?? new Error("Could not read file"));
    reader.readAsDataURL(file);
  });
}

export default function ChatSection() {
  const tau = useTauContext();
  const { peer: selectedPeer, group: selectedGroup, navigate } = useHash();
  const actions = usePeerActions();
  const outbox = useOutbox();

  const [mutedPeers, setMutedPeers] = useState<Set<string>>(new Set());
  const [mutedGroups, setMutedGroups] = useState<Set<string>>(new Set());
  const [archived, setArchived] = useState<Set<string>>(new Set());
  const [replyTo, setReplyTo] = useState<ChatMessage | null>(null);
  const [editing, setEditing] = useState<ChatMessage | null>(null);
  const [showOutbox, setShowOutbox] = useState(false);
  const [searchActive, setSearchActive] = useState(false);
  const [searchReset, setSearchReset] = useState(0);
  const [error, setError] = useState<string | null>(null);

  const { conversations, archived: archivedConvs, dmRequests, refreshRequests } = useConversations(
    tau,
    { mutedPeers, mutedGroups, archived },
  );

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const [muted, arch] = await Promise.all([getMutedConversations(), getArchivedConversations()]);
        if (cancelled) return;
        setMutedPeers(new Set(muted.peers ?? []));
        setMutedGroups(new Set(muted.groups ?? []));
        setArchived(new Set(extractKeys(arch)));
      } catch {
        // Mute/archive state is advisory; the list works without it.
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const selected: Conversation | null = useMemo(() => {
    if (selectedPeer) {
      return (
        buildConversations(tau.messages, tau.peers, tau.groups, tau.unread, {
          mutedPeers,
          mutedGroups,
          archived,
          includeArchived: true,
        }).find((c) => c.kind === "dm" && c.key === selectedPeer) ?? null
      );
    }
    if (selectedGroup) {
      return (
        buildConversations(tau.messages, tau.peers, tau.groups, tau.unread, {
          mutedPeers,
          mutedGroups,
          archived,
          includeArchived: true,
        }).find((c) => c.kind === "group" && c.key === selectedGroup) ?? null
      );
    }
    return null;
  }, [selectedPeer, selectedGroup, tau.messages, tau.peers, tau.groups, tau.unread, mutedPeers, mutedGroups, archived]);

  // Opening a thread marks it read (local cursor + snapshot refresh).
  const selectedKey = selected ? `${selected.kind}:${selected.key}` : null;
  useEffect(() => {
    if (!selected) return;
    setReplyTo(null);
    setEditing(null);
    const target = selected.kind === "group" ? { group: selected.key } : { peer: selected.key };
    void actions
      .markRead(target)
      .then(() => tau.refresh())
      .catch(() => undefined);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedKey]);

  const handleSelect = useCallback(
    (conv: Conversation) => {
      if (conv.kind === "group") navigate("chat", { group: conv.key });
      else navigate("chat", { peer: conv.key });
    },
    [navigate],
  );

  const handleBack = useCallback(() => navigate("chat"), [navigate]);

  const handleOpenSearchResult = useCallback(
    (message: ChatMessage) => {
      if (message.group) navigate("chat", { group: message.group });
      else if (message.peer) navigate("chat", { peer: message.peer });
      setSearchReset((n) => n + 1);
    },
    [navigate],
  );

  async function run(label: string, fn: () => Promise<unknown>) {
    setError(null);
    try {
      await fn();
      await tau.refresh();
    } catch (err) {
      setError(`${label}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  const handleSend = useCallback(
    async (text: string, opts: ComposerSendOptions) => {
      if (!selected) return;
      if (editing) {
        const id = editing.id;
        setEditing(null);
        await run("Edit failed", () => editMessage(id, text));
        return;
      }
      const target = selected.kind === "group" ? { group: selected.key } : { to: selected.key };
      const sendOpts: SendMessageOptions = {
        replyTo: opts.replyTo,
        expiresIn: opts.expiresIn,
        sendIn: opts.sendIn,
        pollOptions: opts.pollOptions,
      };
      setReplyTo(null);
      await run("Send failed", () => sendMessage(target, text, sendOpts));
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [selected, editing],
  );

  const handleSendFile = useCallback(
    async (file: File, opts: Omit<ComposerSendOptions, "pollOptions">) => {
      if (!selected) return;
      const data = await fileToBase64(file);
      await run("File send failed", () =>
        selected.kind === "group"
          ? sendGroupFile(selected.key, file.name, data)
          : sendFile(selected.key, file.name, data),
      );
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [selected],
  );

  const handleMessageAction = useCallback(
    (action: MessageAction, message: ChatMessage) => {
      switch (action) {
        case "reply":
          setEditing(null);
          setReplyTo(message);
          break;
        case "edit":
          setReplyTo(null);
          setEditing(message);
          break;
        case "delete":
          void run("Delete failed", () => deleteMessage(message.id));
          break;
        case "retract":
          void run("Retract failed", () => retractMessage(message.id));
          break;
        case "pin":
          void run("Pin failed", () => (message.pinned ? unpinMessage(message.id) : pinMessage(message.id)));
          break;
        case "forward":
          // Forward target comes through onForward (the bubble's picker).
          break;
      }
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [],
  );

  const handleForward = useCallback(
    (message: ChatMessage, target: string) => {
      const [kind, key] = target.split(":");
      if (!key) return;
      void run("Forward failed", () =>
        forwardMessage(message.id, kind === "group" ? { group: key } : { to: key }),
      );
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [],
  );

  const handleReact = useCallback((message: ChatMessage, reaction: string) => {
    void run("Reaction failed", () => reactToMessage(message.wire_id ?? String(message.id), reaction));
  }, []);
  const handleClearReaction = useCallback((message: ChatMessage) => {
    void run("Reaction failed", () => clearReaction(message.wire_id ?? String(message.id)));
  }, []);

  const handleVotePoll = useCallback(
    (message: ChatMessage, option: number) => {
      if (!selected) return;
      const pollId = message.wire_id ?? String(message.id);
      void run("Vote failed", () =>
        selected.kind === "group"
          ? voteGroupPoll(selected.key, pollId, option)
          : voteDmPoll(selected.key, pollId, option),
      );
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [selected],
  );

  const handleClosePoll = useCallback(
    (message: ChatMessage) => {
      if (!selected) return;
      const pollId = message.wire_id ?? String(message.id);
      void run("Close poll failed", () =>
        selected.kind === "group"
          ? closeGroupPoll(selected.key, pollId)
          : closeDmPoll(selected.key, pollId),
      );
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [selected],
  );

  const forwardTargets = useMemo(
    () => conversations.map((c) => ({ key: c.key, kind: c.kind, name: c.name })),
    [conversations],
  );

  const typingName = useMemo(() => {
    if (!selected) return null;
    const key = selected.kind === "group" ? `group:${selected.key}` : selected.key;
    return tau.typing[key]?.name ?? null;
  }, [selected, tau.typing]);

  const outboxCount = outbox.outbox?.count ?? 0;

  return (
    <div className="flex h-full min-h-0">
      <div
        className={`flex-col border-r border-zinc-800 ${
          selected ? "hidden lg:flex lg:w-[300px] lg:shrink-0" : "flex w-full lg:w-[300px] lg:shrink-0"
        }`}
      >
        <header className="flex items-center justify-between border-b border-zinc-800 px-3 py-2.5">
          <h1 className="text-lg font-bold text-zinc-50">Chat</h1>
          <button
            type="button"
            aria-expanded={showOutbox}
            onClick={() => setShowOutbox((v) => !v)}
            className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-semibold text-zinc-200 hover:bg-zinc-900"
          >
            Outbox{outboxCount > 0 ? ` (${outboxCount})` : ""}
          </button>
        </header>
        {showOutbox && (
          <OutboxPanel
            outbox={outbox.outbox}
            held={outbox.held}
            loading={outbox.loading}
            error={outbox.error}
            onCancel={(target) => void outbox.cancel(target)}
            onRetry={(target) => void outbox.retry(target)}
            onDropHeld={(target) => void outbox.dropHeld(target)}
          />
        )}
        <MessageSearch
          onOpen={handleOpenSearchResult}
          onActiveChange={setSearchActive}
          resetToken={searchReset}
        />
        {!searchActive && (
          <div className="min-h-0 flex-1">
            <ConversationList
              conversations={conversations}
              archived={archivedConvs}
              dmRequests={dmRequests}
              selectedKey={selectedKey}
              onSelect={handleSelect}
              onAcceptRequest={(req) => {
                void run("Accept failed", () => acceptDmRequest(req.id)).then(() => void refreshRequests());
              }}
              onDeclineRequest={(req) => {
                void run("Decline failed", () => declineDmRequest(req.id)).then(() => void refreshRequests());
              }}
            />
          </div>
        )}
      </div>

      <div className={`min-w-0 flex-1 flex-col ${selected ? "flex" : "hidden lg:flex"}`}>
        {error && (
          <p role="alert" className="border-b border-red-900/60 bg-red-950/60 px-3 py-1.5 text-xs text-red-300">
            {error}
            <button type="button" onClick={() => setError(null)} className="ml-2 underline">
              Dismiss
            </button>
          </p>
        )}
        {selected ? (
          <ThreadView
            conversation={selected}
            messages={tau.messages}
            typingName={typingName}
            replyTo={replyTo}
            onReplyToChange={setReplyTo}
            editing={editing}
            onEditingChange={setEditing}
            forwardTargets={forwardTargets}
            onMessageAction={handleMessageAction}
            onForward={handleForward}
            onReact={handleReact}
            onClearReaction={handleClearReaction}
            onVotePoll={handleVotePoll}
            onClosePoll={handleClosePoll}
            onSend={handleSend}
            onSendFile={handleSendFile}
            onBack={handleBack}
            onOpenPeerPanel={() => {
              // Peer details live in the right sidebar on desktop; on mobile
              // the People tab shows the same panel for this conversation.
              navigate(
                "people",
                selected.kind === "group" ? { group: selected.key } : { peer: selected.key },
              );
            }}
          />
        ) : (
          <div className="flex flex-1 flex-col items-center justify-center gap-2 p-6 text-center">
            <p className="text-lg font-semibold text-zinc-200">Select a conversation</p>
            <p className="max-w-xs text-sm text-zinc-500">
              Pick a peer or group on the left, or find someone in the People tab. Messages travel peer-to-peer over Reticulum — no servers in between.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
