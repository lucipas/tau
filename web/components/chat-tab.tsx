"use client";

import { useEffect, useMemo, useRef, useState } from "react";

import { API_BASE } from "@/lib/api";
import {
  formatDateTime,
  formatExpiresIn,
  formatFileSize,
  formatRtt,
  formatSendsIn,
  formatTime,
  peerLabel,
  shortHash,
} from "@/lib/format";
import type { Peer, TauGroup, TauMessage } from "@/lib/types";
import type { UseTauResult } from "@/lib/use-tau";
import {
  Button,
  Card,
  CardTitle,
  Chip,
  EmptyState,
  ErrorNote,
  Toggle,
} from "@/components/ui";

export const REACT_OPTIONS = ["👍", "❤️", "😂", "😮", "😢", "🙏"];

const MAX_FILE_BYTES = 10 * 1024 * 1024;

const SEND_LATER_OPTIONS: { label: string; value: string }[] = [
  { label: "now", value: "" },
  { label: "in 30 minutes", value: "1800" },
  { label: "in 1 hour", value: "3600" },
  { label: "in 3 hours", value: "10800" },
  { label: "in 24 hours", value: "86400" },
];

const EXPIRY_OPTIONS: { label: string; value: string }[] = [
  { label: "never", value: "" },
  { label: "in 5 minutes", value: "300" },
  { label: "in 1 hour", value: "3600" },
  { label: "in 1 day", value: "86400" },
  { label: "in 7 days", value: "604800" },
];

const DISAPPEARING_PRESETS = [30, 300, 3600, 86400, 604800];

function describeDuration(seconds: number): string {
  for (const [suffix, size] of [
    ["d", 86400],
    ["h", 3600],
    ["m", 60],
    ["s", 1],
  ] as const) {
    if (seconds >= size && seconds % size === 0) {
      return `${seconds / size}${suffix}`;
    }
  }
  return `${seconds}s`;
}

type Selection =
  | { kind: "peer"; id: string }
  | { kind: "group"; id: string }
  | null;

function displayName(peer: Peer | undefined): string {
  if (!peer) return "unknown";
  return peer.alias || peer.name || "unknown";
}

function peerByDestination(
  peers: Peer[],
  destination: string,
): Peer | undefined {
  return peers.find((entry) => entry.destination === destination);
}

// Resolve a reply quote from local history only, never trusting the
// sender's claim about what was said. Mirrors the old console's
// resolveQuote: same-conversation wire-id match, excerpt-hash check,
// and the group logical reference (group + author + shared sent_at).
export type ResolvedQuote = { name: string; text: string } | "unavailable";

export function resolveQuote(
  messages: TauMessage[],
  message: TauMessage,
  ownHash?: string | null,
): ResolvedQuote | null {
  const ref = message.in_reply_to;
  if (!ref || !ref.id) return null;
  if (
    message.group &&
    ref.group === message.group &&
    ref.author &&
    typeof ref.sent_at === "number"
  ) {
    const logical = messages.filter(
      (entry) =>
        entry.group === ref.group &&
        entry.sent_at === ref.sent_at &&
        (entry.direction === "in"
          ? entry.peer === ref.author
          : ref.author === ownHash),
    );
    if (logical.length > 0) return pickQuoted(logical, ref.excerpt);
  }
  const candidates = messages.filter((entry) =>
    message.group
      ? entry.group === message.group && entry.wire_id === ref.id
      : entry.peer === message.peer && entry.wire_id === ref.id,
  );
  if (candidates.length === 0) return "unavailable";
  return pickQuoted(candidates, ref.excerpt);
}

function pickQuoted(
  candidates: TauMessage[],
  excerpt: string | null | undefined,
): ResolvedQuote {
  let quoted = candidates[0];
  if (excerpt) {
    const verified = candidates.find(
      (entry) =>
        entry.excerpt_hash === excerpt ||
        entry.excerpt_hashes?.includes(excerpt),
    );
    if (verified) {
      quoted = verified;
    } else if (candidates.some((entry) => entry.excerpt_hash !== undefined)) {
      return "unavailable";
    }
  }
  return {
    name: quoted.direction === "out" ? "you" : quoted.peer_name,
    text: quoted.text.split("\n")[0].slice(0, 80),
  };
}

export default function ChatTab({ tau }: { tau: UseTauResult }) {
  const [selection, setSelection] = useState<Selection>(null);
  const [listFilter, setListFilter] = useState("");
  const [searchText, setSearchText] = useState("");
  const [pinnedOnly, setPinnedOnly] = useState(false);
  const [replyTo, setReplyTo] = useState<TauMessage | null>(null);
  const [showNewGroup, setShowNewGroup] = useState(false);
  const [groupError, setGroupError] = useState<string | null>(null);

  const selectedPeer =
    selection?.kind === "peer"
      ? peerByDestination(tau.peers, selection.id)
      : undefined;
  const selectedGroup =
    selection?.kind === "group"
      ? tau.groups.find((entry) => entry.id === selection.id)
      : undefined;

  // Opening a conversation clears its unread count.
  useEffect(() => {
    if (!selection) return;
    if (selection.kind === "peer") {
      void tau.markRead(selection.id).catch(() => undefined);
    } else {
      void tau.markRead(undefined, selection.id).catch(() => undefined);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selection?.kind, selection?.id]);

  // A conversation switch drops any reply in progress.
  useEffect(() => {
    setReplyTo(null);
    setSearchText("");
    setPinnedOnly(false);
  }, [selection?.kind, selection?.id]);

  const conversationMessages = useMemo(() => {
    if (!selection) return [];
    const filtered = tau.messages.filter((message) =>
      selection.kind === "peer"
        ? message.peer === selection.id && !message.group
        : message.group === selection.id,
    );
    return [...filtered].sort((a, b) => a.id - b.id);
  }, [tau.messages, selection]);

  const counts = useMemo(() => {
    let inbound = 0;
    let outbound = 0;
    for (const message of conversationMessages) {
      if (message.direction === "in") inbound += 1;
      else outbound += 1;
    }
    return { inbound, outbound };
  }, [conversationMessages]);

  const visibleMessages = useMemo(() => {
    // Vote and close records are not chat: the tally on the poll
    // itself is the display, so they never render as bubbles.
    let list = conversationMessages.filter(
      (message) => !message.poll_vote && !message.poll_close,
    );
    if (pinnedOnly) list = list.filter((message) => message.pinned);
    const query = searchText.trim().toLowerCase();
    if (query) {
      list = list.filter((message) =>
        message.text.toLowerCase().includes(query),
      );
    }
    return list;
  }, [conversationMessages, pinnedOnly, searchText]);

  const typingKey = selection
    ? selection.kind === "group"
      ? `group:${selection.id}`
      : selection.id
    : null;
  const typingEntries = typingKey ? (tau.typing[typingKey] ?? []) : [];
  const live = tau.connection === "live";

  const filter = listFilter.trim().toLowerCase();
  const visiblePeers = tau.peers.filter((peer) =>
    filter ? displayName(peer).toLowerCase().includes(filter) : true,
  );
  const visibleGroups = tau.groups.filter((group) =>
    filter ? group.name.toLowerCase().includes(filter) : true,
  );
  const archivedSet = new Set(
    tau.archivedConversations.map((entry) => entry.destination),
  );
  const mutedPeerSet = new Set(tau.mutedConversations.peers);
  const mutedGroupSet = new Set(tau.mutedConversations.groups);

  async function handleCreateGroup(name: string, members: string[]) {
    setGroupError(null);
    try {
      const result = await tau.createGroup(name, members);
      setShowNewGroup(false);
      setSelection({ kind: "group", id: result.group.id });
    } catch (error) {
      setGroupError(
        error instanceof Error ? error.message : "could not create group",
      );
    }
  }

  const listVisible = !selection;
  return (
    <div className="grid gap-3 lg:grid-cols-[320px_minmax(0,1fr)]">
      {/* ---- Conversation list ---- */}
      <div className={listVisible ? "min-w-0" : "hidden min-w-0 lg:block"}>
        <Card>
          <CardTitle
            right={
              <Button
                variant="subtle"
                onClick={() => setShowNewGroup((open) => !open)}
              >
                {showNewGroup ? "close" : "+ new group"}
              </Button>
            }
          >
            Conversations
          </CardTitle>
          <div className="border-b border-line-soft px-3 py-2">
            <input
              value={listFilter}
              onChange={(event) => setListFilter(event.target.value)}
              placeholder="Filter conversations…"
              aria-label="Filter conversations"
              className="h-9 w-full min-w-0 rounded-lg border border-line bg-void/70 px-2.5 text-sm text-ink outline-none placeholder:text-faint focus:border-signal/60"
            />
          </div>

          {showNewGroup ? (
            <NewGroupForm
              peers={tau.peers}
              onCancel={() => setShowNewGroup(false)}
              onCreate={(name, members) => void handleCreateGroup(name, members)}
              error={groupError}
            />
          ) : null}

          <div className="px-3 pt-2 pb-1 text-xs font-semibold tracking-widest text-mist uppercase">
            Direct messages
          </div>
          {visiblePeers.length === 0 ? (
            <EmptyState>No peers yet</EmptyState>
          ) : (
            <ul className="flex flex-col">
              {visiblePeers.map((peer) => {
                const unreadCount = tau.unread.per_peer[peer.destination] ?? 0;
                const active =
                  selection?.kind === "peer" &&
                  selection.id === peer.destination;
                const archived = archivedSet.has(peer.destination);
                return (
                  <li key={peer.destination}>
                    <button
                      type="button"
                      onClick={() =>
                        setSelection({ kind: "peer", id: peer.destination })
                      }
                      aria-current={active ? "true" : undefined}
                      className={`flex w-full min-w-0 items-center gap-2 px-3 py-2 text-left text-sm hover:bg-panel-2 ${
                        active ? "bg-panel-2" : ""
                      } ${archived ? "opacity-60" : ""}`}
                    >
                      <span
                        aria-label={
                          peer.reachable === true
                            ? "reachable"
                            : peer.reachable === false
                              ? "unreachable"
                              : "reachability unknown"
                        }
                        title={
                          peer.reachable === true
                            ? "reachable"
                            : peer.reachable === false
                              ? "unreachable"
                              : "reachability unknown"
                        }
                        className={`h-2 w-2 shrink-0 rounded-full ${
                          peer.reachable === true
                            ? "bg-signal"
                            : peer.reachable === false
                              ? "bg-amber"
                              : "border border-line bg-transparent"
                        }`}
                      />
                      <span className="min-w-0 flex-1 truncate">
                        {displayName(peer)}
                        {peer.verified ? (
                          <span className="ml-1 text-signal" title="verified">
                            ✓
                          </span>
                        ) : null}
                        {peer.blocked ? (
                          <span className="ml-1 text-danger" title="blocked">
                            blocked
                          </span>
                        ) : null}
                        {mutedPeerSet.has(peer.destination) ? (
                          <span className="ml-1 text-faint" title="muted">
                            muted
                          </span>
                        ) : null}
                        {archived ? (
                          <span className="ml-1 text-faint" title="archived">
                            archived
                          </span>
                        ) : null}
                        {peer.lookalike_of ? (
                          <span
                            className="ml-1 block truncate text-xs text-amber"
                            title={`Looks like ${peer.lookalike_of}`}
                          >
                            looks like {peer.lookalike_of}
                          </span>
                        ) : null}
                      </span>
                      {unreadCount > 0 ? (
                        <span className="shrink-0 rounded-full bg-signal px-2 py-0.5 text-xs font-semibold text-void">
                          {unreadCount}
                        </span>
                      ) : null}
                    </button>
                  </li>
                );
              })}
            </ul>
          )}

          <div className="px-3 pt-3 pb-1 text-xs font-semibold tracking-widest text-mist uppercase">
            Groups
          </div>
          {visibleGroups.length === 0 ? (
            <EmptyState>No groups yet</EmptyState>
          ) : (
            <ul className="flex flex-col pb-2">
              {visibleGroups.map((group) => {
                const unreadCount = tau.unread.per_group[group.id] ?? 0;
                const active =
                  selection?.kind === "group" && selection.id === group.id;
                return (
                  <li key={group.id}>
                    <button
                      type="button"
                      onClick={() =>
                        setSelection({ kind: "group", id: group.id })
                      }
                      aria-current={active ? "true" : undefined}
                      className={`flex w-full min-w-0 items-center gap-2 px-3 py-2 text-left text-sm hover:bg-panel-2 ${
                        active ? "bg-panel-2" : ""
                      } ${group.ignored ? "opacity-60" : ""}`}
                    >
                      <span className="min-w-0 flex-1 truncate">
                        {group.name}
                        <span className="ml-1 text-xs text-faint">
                          {group.members.length} member
                          {group.members.length === 1 ? "" : "s"}
                        </span>
                        {group.ignored ? (
                          <span className="ml-1 text-faint">ignored</span>
                        ) : null}
                        {mutedGroupSet.has(group.id) ? (
                          <span className="ml-1 text-faint">muted</span>
                        ) : null}
                      </span>
                      {unreadCount > 0 ? (
                        <span className="shrink-0 rounded-full bg-signal px-2 py-0.5 text-xs font-semibold text-void">
                          {unreadCount}
                        </span>
                      ) : null}
                    </button>
                  </li>
                );
              })}
            </ul>
          )}

          {tau.leftGroups.length > 0 ? (
            <div className="border-t border-line-soft px-3 py-2">
              <p className="pb-1 text-xs font-semibold tracking-widest text-mist uppercase">
                Left groups
              </p>
              {tau.leftGroups.map((entry) => (
                <div
                  key={entry.id}
                  className="flex min-w-0 flex-wrap items-center gap-2 py-1 text-sm"
                >
                  <span className="min-w-0 flex-1 truncate font-mono text-xs text-mist">
                    {shortHash(entry.id, 10)}
                  </span>
                  <Button
                    variant="subtle"
                    onClick={() => void tau.rejoinGroup(entry.id)}
                  >
                    rejoin
                  </Button>
                </div>
              ))}
            </div>
          ) : null}

          {tau.ignoredGroups.length > 0 ? (
            <div className="border-t border-line-soft px-3 py-2">
              <p className="pb-1 text-xs font-semibold tracking-widest text-mist uppercase">
                Ignored groups
              </p>
              {tau.ignoredGroups.map((entry) => (
                <div
                  key={entry.id}
                  className="flex min-w-0 flex-wrap items-center gap-2 py-1 text-sm"
                >
                  <span className="min-w-0 flex-1 truncate">
                    {entry.name || shortHash(entry.id, 10)}
                  </span>
                  <Button
                    variant="subtle"
                    onClick={() => void tau.ignoreGroup(entry.id, false)}
                  >
                    unignore
                  </Button>
                  <Button
                    variant="subtle"
                    onClick={() => void tau.rejoinGroup(entry.id)}
                  >
                    rejoin
                  </Button>
                </div>
              ))}
            </div>
          ) : null}
        </Card>
      </div>

      {/* ---- Thread column ---- */}
      <div
        className={
          selection
            ? "flex min-w-0 flex-col gap-3"
            : "hidden min-w-0 flex-col gap-3 lg:flex"
        }
      >
        {!selection ? (
          <Card>
            <EmptyState>
              Pick a peer or group to open a conversation.
            </EmptyState>
          </Card>
        ) : (
          <>
            <ThreadCard
              tau={tau}
              selection={selection}
              peer={selectedPeer}
              group={selectedGroup}
              messages={conversationMessages}
              visibleMessages={visibleMessages}
              counts={counts}
              live={live}
              searchText={searchText}
              onSearchText={setSearchText}
              pinnedOnly={pinnedOnly}
              onPinnedOnly={setPinnedOnly}
              typingEntries={typingEntries}
              onReply={setReplyTo}
              onBack={() => setSelection(null)}
            />
            <Composer
              tau={tau}
              selection={selection}
              peer={selectedPeer}
              group={selectedGroup}
              replyTo={replyTo}
              onCancelReply={() => setReplyTo(null)}
            />
            {selection.kind === "peer" && selectedPeer ? (
              <PeerPanel
                tau={tau}
                peer={selectedPeer}
                onPeerGone={() => setSelection(null)}
              />
            ) : null}
          </>
        )}
      </div>

      <OutboxSection tau={tau} />
    </div>
  );
}

function NewGroupForm({
  peers,
  onCancel,
  onCreate,
  error,
}: {
  peers: Peer[];
  onCancel: () => void;
  onCreate: (name: string, members: string[]) => void;
  error: string | null;
}) {
  const [name, setName] = useState("");
  const [members, setMembers] = useState<string[]>([]);
  const [creating, setCreating] = useState(false);
  return (
    <form
      className="flex flex-col gap-2 border-b border-line-soft px-3 py-2"
      onSubmit={(event) => {
        event.preventDefault();
        if (!name.trim() || members.length === 0 || creating) return;
        setCreating(true);
        onCreate(name.trim(), members);
        setCreating(false);
      }}
    >
      <input
        value={name}
        onChange={(event) => setName(event.target.value)}
        placeholder="Group name"
        aria-label="Group name"
        className="h-9 w-full min-w-0 rounded-lg border border-line bg-void/70 px-2.5 text-sm text-ink outline-none placeholder:text-faint focus:border-signal/60"
      />
      <fieldset className="flex flex-col gap-1">
        <legend className="text-xs text-mist">Members</legend>
        {peers.length === 0 ? (
          <p className="text-xs text-faint">No peers to add yet.</p>
        ) : (
          peers.map((peer) => (
            <label
              key={peer.destination}
              className="flex min-w-0 cursor-pointer items-center gap-2 text-sm"
            >
              <input
                type="checkbox"
                checked={members.includes(peer.destination)}
                onChange={(event) =>
                  setMembers((current) =>
                    event.target.checked
                      ? [...current, peer.destination]
                      : current.filter((id) => id !== peer.destination),
                  )
                }
              />
              <span className="min-w-0 truncate">
                {displayName(peer)}
              </span>
            </label>
          ))
        )}
      </fieldset>
      <ErrorNote message={error} />
      <div className="flex flex-wrap gap-2">
        <Button
          variant="primary"
          type="submit"
          disabled={!name.trim() || members.length === 0 || creating}
        >
          {creating ? "creating…" : "Create group"}
        </Button>
        <Button onClick={onCancel}>Cancel</Button>
      </div>
    </form>
  );
}

function ThreadCard({
  tau,
  selection,
  peer,
  group,
  messages,
  visibleMessages,
  counts,
  live,
  searchText,
  onSearchText,
  pinnedOnly,
  onPinnedOnly,
  typingEntries,
  onReply,
  onBack,
}: {
  tau: UseTauResult;
  selection: Exclude<Selection, null>;
  peer?: Peer;
  group?: TauGroup;
  messages: TauMessage[];
  visibleMessages: TauMessage[];
  counts: { inbound: number; outbound: number };
  live: boolean;
  searchText: string;
  onSearchText: (value: string) => void;
  pinnedOnly: boolean;
  onPinnedOnly: (next: boolean) => void;
  typingEntries: { peer: string; name: string; receivedAt: number }[];
  onReply: (message: TauMessage) => void;
  onBack: () => void;
}) {
  const isGroup = selection.kind === "group";
  const title = isGroup
    ? (group?.name ?? "group")
    : displayName(peer);
  const [groupError, setGroupError] = useState<string | null>(null);
  const [confirmingLeave, setConfirmingLeave] = useState(false);

  async function handleIgnore() {
    if (!group) return;
    setGroupError(null);
    try {
      await tau.ignoreGroup(group.id, !group.ignored);
    } catch (error) {
      setGroupError(
        error instanceof Error ? error.message : "could not update group",
      );
    }
  }

  async function handleLeave() {
    if (!group) return;
    setGroupError(null);
    try {
      await tau.leaveGroup(group.id);
      onBack();
    } catch (error) {
      setGroupError(
        error instanceof Error ? error.message : "could not leave group",
      );
    }
  }

  return (
    <Card>
      <div className="flex min-w-0 flex-wrap items-center gap-2 border-b border-line-soft px-3 py-2">
        <Button variant="subtle" onClick={onBack}>
          <span className="lg:hidden">← Back</span>
          <span className="hidden lg:inline">←</span>
        </Button>
        <div className="min-w-0 flex-1">
          <h2 className="truncate text-sm font-semibold">{title}</h2>
          {isGroup && group?.topic ? (
            <p className="truncate text-xs text-mist">{group.topic}</p>
          ) : null}
          {!isGroup && peer ? (
            <p className="truncate font-mono text-xs text-faint">
              {peerLabel(peer.name, peer.destination)}
            </p>
          ) : null}
        </div>
        {isGroup && group ? (
          <div className="flex shrink-0 flex-wrap gap-1">
            <Button variant="subtle" onClick={() => void handleIgnore()}>
              {group.ignored ? "unignore" : "ignore"}
            </Button>
            {confirmingLeave ? (
              <>
                <Button
                  variant="danger"
                  onClick={() => void handleLeave()}
                >
                  confirm leave
                </Button>
                <Button
                  variant="subtle"
                  onClick={() => setConfirmingLeave(false)}
                >
                  keep
                </Button>
              </>
            ) : group.can_leave !== false ? (
              <Button
                variant="danger"
                onClick={() => setConfirmingLeave(true)}
              >
                leave
              </Button>
            ) : null}
          </div>
        ) : null}
      </div>

      {peer?.profile ? (
        <div className="border-b border-line-soft px-3 py-2">
          <p className="min-w-0 truncate text-xs" title={peer.profile.bio}>
            {peer.profile.name}
            {peer.profile.bio ? ` — ${peer.profile.bio}` : ""}
          </p>
          <p className="mt-0.5 flex flex-wrap items-center gap-x-3 gap-y-0.5 font-mono text-[11px] text-mist">
            {peer.profile.avatar ? (
              <span title={`Avatar SHA-256 ${peer.profile.avatar}`}>
                avatar {shortHash(peer.profile.avatar, 10)}
              </span>
            ) : null}
            {peer.profile.links.map((link) => (
              <a
                key={link}
                href={link}
                target="_blank"
                rel="noreferrer"
                className="max-w-full truncate text-signal hover:underline"
              >
                {link.replace(/^https?:\/\//, "")}
              </a>
            ))}
          </p>
        </div>
      ) : null}

      {/* Traffic strip: in/out counts of the open conversation, a
          pinned toggle, and a streaming dot. */}
      <div className="flex min-w-0 flex-wrap items-center justify-between gap-2 border-b border-line-soft px-3 py-2">
        <span className="font-mono text-xs text-mist">
          {pinnedOnly
            ? `${visibleMessages.length} pinned`
            : `${counts.inbound} in / ${counts.outbound} out`}
        </span>
        <div className="flex flex-wrap items-center gap-2">
          <Button
            variant="subtle"
            onClick={() => onPinnedOnly(!pinnedOnly)}
            title={pinnedOnly ? "Show all messages" : "Show pinned only"}
          >
            {pinnedOnly ? "show all" : "📌 pinned"}
          </Button>
          <span className="inline-flex items-center gap-1.5 font-mono text-[11px] text-mist">
            <span
              className={`h-1.5 w-1.5 rounded-full ${
                live ? "bg-signal" : "bg-amber"
              }`}
            />
            {live ? "streaming" : "waiting for bridge"}
          </span>
        </div>
      </div>

      <div className="flex flex-wrap gap-2 border-b border-line-soft px-3 py-2">
        <input
          value={searchText}
          onChange={(event) => onSearchText(event.target.value)}
          placeholder="Search this conversation…"
          aria-label="Search this conversation"
          className="h-9 min-w-0 flex-1 rounded-lg border border-line bg-void/70 px-2.5 text-sm text-ink outline-none placeholder:text-faint focus:border-signal/60"
        />
        {searchText ? (
          <Button variant="subtle" onClick={() => onSearchText("")}>
            clear search
          </Button>
        ) : null}
      </div>

      {groupError ? (
        <div className="px-3 pt-2">
          <ErrorNote message={groupError} />
        </div>
      ) : null}

      <div
        aria-live="polite"
        aria-label="Message thread"
        className="flex flex-col gap-4 px-3 py-4"
      >
        {messages.length === 0 ? (
          <EmptyState>No messages yet</EmptyState>
        ) : visibleMessages.length === 0 ? (
          <EmptyState>
            {pinnedOnly
              ? "No pinned messages in this conversation."
              : `No messages match “${searchText.trim()}”.`}
          </EmptyState>
        ) : (
          visibleMessages.map((message) => (
            <MessageBubble
              key={message.id}
              message={message}
              allMessages={messages}
              tau={tau}
              onReply={onReply}
            />
          ))
        )}
      </div>

      {typingEntries.length > 0 ? (
        <p className="border-t border-line-soft px-3 py-2 text-xs text-mist">
          {typingEntries.map((entry) => entry.name).join(", ")} typing…
        </p>
      ) : null}
    </Card>
  );
}

function MessageBubble({
  message,
  allMessages,
  tau,
  onReply,
}: {
  message: TauMessage;
  allMessages: TauMessage[];
  tau: UseTauResult;
  onReply: (message: TauMessage) => void;
}) {
  const outbound = message.direction === "out";
  const peerHash = message.peer ?? "";
  const initial = (message.peer_name || "?").slice(0, 1).toUpperCase();
  const expiry = formatExpiresIn(message.expires_at);
  const scheduled = formatSendsIn(message.send_after);
  const quote = resolveQuote(
    allMessages,
    message,
    tau.node?.destination ?? null,
  );
  const retracted = message.text === "" || message.status === "retracted";

  const [editing, setEditing] = useState(false);
  const [editText, setEditText] = useState("");
  const [savingEdit, setSavingEdit] = useState(false);
  const [editError, setEditError] = useState<string | null>(null);
  const [confirmingDelete, setConfirmingDelete] = useState(false);
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [reactPickerOpen, setReactPickerOpen] = useState(false);
  const [forwardOpen, setForwardOpen] = useState(false);
  const [forwardTarget, setForwardTarget] = useState("");
  const [votePending, setVotePending] = useState(false);
  const [voteError, setVoteError] = useState<string | null>(null);
  const [closePending, setClosePending] = useState(false);

  const ownReaction = tau.node
    ? message.reactions?.find(
        (reaction) => reaction.reactor === tau.node!.destination,
      )
    : undefined;

  async function submitEdit() {
    setSavingEdit(true);
    setEditError(null);
    try {
      const result = await tau.editMessage(message.id, editText.trim());
      if (result.edited) {
        setEditing(false);
      } else {
        setEditError("the peer no longer has this message");
      }
    } catch (error) {
      setEditError(error instanceof Error ? error.message : "edit failed");
    } finally {
      setSavingEdit(false);
    }
  }

  async function run(label: string, work: () => Promise<unknown>) {
    setBusy(true);
    setActionError(null);
    try {
      await work();
    } catch (error) {
      setActionError(
        `${label} failed: ${error instanceof Error ? error.message : "unknown error"}`,
      );
    } finally {
      setBusy(false);
    }
  }

  async function handleReact(reaction: string) {
    if (!message.wire_id) return;
    setBusy(true);
    setActionError(null);
    try {
      if (ownReaction?.reaction === reaction) {
        await tau.clearReaction(message.wire_id);
      } else {
        await tau.react(message.wire_id, reaction);
      }
      setReactPickerOpen(false);
    } catch (error) {
      setActionError(
        `reaction failed: ${error instanceof Error ? error.message : "unknown error"}`,
      );
    } finally {
      setBusy(false);
    }
  }

  async function handleForward() {
    if (!forwardTarget) return;
    setBusy(true);
    setActionError(null);
    try {
      if (forwardTarget.startsWith("group:")) {
        await tau.forwardMessage(message.id, "", forwardTarget.slice(6));
      } else {
        await tau.forwardMessage(message.id, forwardTarget.slice(5));
      }
      setForwardOpen(false);
    } catch (error) {
      setActionError(
        `forward failed: ${error instanceof Error ? error.message : "unknown error"}`,
      );
    } finally {
      setBusy(false);
    }
  }

  async function handleVotePoll(option: number) {
    if (!message.group || !message.wire_id) return;
    setVotePending(true);
    setVoteError(null);
    try {
      await tau.voteGroupPoll(message.group, message.wire_id, option);
    } catch (error) {
      setVoteError(error instanceof Error ? error.message : "vote failed");
    } finally {
      setVotePending(false);
    }
  }

  async function handleClosePoll() {
    if (!message.group || !message.wire_id) return;
    setClosePending(true);
    setVoteError(null);
    try {
      await tau.closeGroupPoll(message.group, message.wire_id);
    } catch (error) {
      setVoteError(error instanceof Error ? error.message : "close failed");
    } finally {
      setClosePending(false);
    }
  }

  return (
    <article
      className={`flex items-start gap-2.5 ${outbound ? "flex-row-reverse" : ""}`}
    >
      <span
        className={`mt-0.5 grid h-8 w-8 shrink-0 place-items-center rounded-lg border font-mono text-xs ${
          outbound
            ? "border-signal/35 bg-signal/10 text-signal"
            : "border-line bg-void/60 text-zinc-300"
        }`}
        aria-hidden="true"
      >
        {outbound ? "→" : initial}
      </span>

      <div
        className={`flex min-w-0 max-w-[86%] flex-col ${
          outbound ? "items-end" : "items-start"
        }`}
      >
        <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-0.5 px-1 pb-1">
          <span className="text-[11px] font-medium text-zinc-300">
            {outbound ? "you" : message.peer_name}
          </span>
          {message.group ? (
            <span className="rounded border border-signal/30 bg-signal/10 px-1.5 py-px font-mono text-[10px] text-signal">
              {message.group_name ?? "group"}
            </span>
          ) : null}
          <span className="font-mono text-[10px] text-mist/80">
            {peerHash ? shortHash(peerHash, 8) : "anonymous"}
          </span>
          <span className="font-mono text-[10px] text-mist/60">
            {formatTime(message.received_at)}
          </span>
          {message.pinned ? (
            <span className="font-mono text-[10px] text-signal" title="Pinned — local only">
              📌 pinned
            </span>
          ) : null}
        </div>

        <div
          className={`min-w-0 whitespace-pre-wrap break-words rounded-2xl border px-3.5 py-2.5 text-[13px] leading-relaxed ${
            outbound
              ? "rounded-tr-sm border-signal/30 bg-signal/[0.08] text-zinc-100"
              : "rounded-tl-sm border-line/80 bg-void/60 text-zinc-200"
          }`}
        >
          {quote !== null ? (
            <span className="mb-1.5 block border-l-2 border-signal/50 pl-2 font-mono text-[11px] leading-snug text-mist">
              {quote === "unavailable"
                ? message.in_reply_to?.excerpt
                  ? `↩ ${message.in_reply_to.excerpt}`
                  : "↩ original not available"
                : `↩ ${quote.name}: ${quote.text}`}
            </span>
          ) : null}
          {editing ? (
            <span className="block">
              <textarea
                value={editText}
                disabled={savingEdit}
                onChange={(event) => setEditText(event.target.value)}
                rows={3}
                aria-label="Edit message text"
                className="w-full min-w-0 resize-y rounded-lg border border-line/70 bg-void/60 px-2.5 py-2 text-[13px] leading-relaxed text-zinc-200 outline-none focus:border-signal/50"
              />
              <span className="mt-2 flex flex-wrap gap-2 font-mono text-[10px]">
                <button
                  type="button"
                  disabled={
                    savingEdit ||
                    !editText.trim() ||
                    editText.trim() === message.text
                  }
                  onClick={() => void submitEdit()}
                  className="rounded border border-signal/40 bg-signal/10 px-1.5 py-0.5 text-signal disabled:opacity-50"
                >
                  {savingEdit ? "saving…" : "save"}
                </button>
                <button
                  type="button"
                  disabled={savingEdit}
                  onClick={() => setEditing(false)}
                  className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5 text-mist/80"
                >
                  cancel
                </button>
              </span>
              {editError ? (
                <span className="mt-1 block font-mono text-[10px] text-rose-300">
                  {editError}
                </span>
              ) : null}
            </span>
          ) : retracted ? (
            <em className="text-mist">message retracted</em>
          ) : (
            message.text
          )}
        </div>

        {message.poll_options && message.poll_options.length > 0 ? (
          <div className="mt-2 w-full min-w-0 space-y-1.5" aria-label="Group poll">
            {message.poll_options.map((option, index) => {
              const count = message.poll?.counts[index] ?? 0;
              const total = message.poll?.total ?? 0;
              const share = total > 0 ? Math.round((count / total) * 100) : 0;
              const mine = message.poll?.my_vote === index;
              const closed = message.poll?.closed ?? false;
              return (
                <button
                  key={`${index}-${option}`}
                  type="button"
                  disabled={closed || votePending}
                  onClick={() => void handleVotePoll(index)}
                  aria-pressed={mine}
                  title={
                    closed
                      ? "This poll is closed"
                      : mine
                        ? "Your vote — pick another option to change it"
                        : `Vote for ${option}`
                  }
                  className={`relative block w-full min-w-0 overflow-hidden rounded-lg border px-2.5 py-1.5 text-left text-[12px] ${
                    mine
                      ? "border-signal/50 bg-signal/10 text-zinc-100"
                      : "border-line/70 bg-panel/60 text-zinc-200 hover:border-signal/40"
                  }`}
                >
                  <span
                    aria-hidden="true"
                    className="absolute inset-y-0 left-0 bg-signal/15"
                    style={{ width: `${share}%` }}
                  />
                  <span className="relative flex items-baseline justify-between gap-2">
                    <span className="min-w-0 break-words">
                      {mine ? "✓ " : ""}
                      {option}
                    </span>
                    <span className="shrink-0 font-mono text-[10px] text-mist/80">
                      {count} · {share}%
                    </span>
                  </span>
                </button>
              );
            })}
            <p className="flex flex-wrap items-center gap-x-2 font-mono text-[10px] text-mist/60">
              <span>
                {message.poll?.total ?? 0} vote
                {(message.poll?.total ?? 0) === 1 ? "" : "s"}
                {message.poll?.my_vote !== null &&
                message.poll?.my_vote !== undefined
                  ? " · you voted"
                  : ""}
                {message.poll?.closed ? " · closed" : ""}
              </span>
              {outbound && !(message.poll?.closed ?? false) ? (
                <button
                  type="button"
                  disabled={closePending}
                  onClick={() => void handleClosePoll()}
                  className="rounded border border-line/70 bg-panel/60 px-1.5 py-0.5 hover:border-rose-300/50 hover:text-rose-300 disabled:opacity-50"
                >
                  {closePending ? "closing…" : "close poll"}
                </button>
              ) : null}
            </p>
            {voteError ? (
              <p className="font-mono text-[10px] text-rose-300">{voteError}</p>
            ) : null}
          </div>
        ) : null}

        {message.file ? (
          <div className="mt-1.5 flex w-full min-w-0 items-center gap-2 rounded-lg border border-line/70 bg-void/50 px-2.5 py-1.5 font-mono text-[11px] text-zinc-300">
            <span aria-hidden="true">📎</span>
            <span className="min-w-0 flex-1 truncate" title={message.file.name}>
              {message.file.name}
            </span>
            <span className="shrink-0 text-mist">
              {formatFileSize(message.file.size)}
            </span>
            {message.direction === "in" && message.peer ? (
              <a
                className="shrink-0 text-signal hover:underline"
                href={`${API_BASE}/api/files/${message.peer}/${encodeURIComponent(message.file.name)}`}
                download={message.file.name}
              >
                Download
              </a>
            ) : (
              <span className="shrink-0 text-mist">sent</span>
            )}
          </div>
        ) : null}

        {message.location ? (
          <div className="mt-1.5 flex w-full min-w-0 items-center gap-2 rounded-lg border border-line/70 bg-void/50 px-2.5 py-1.5 font-mono text-[11px] text-zinc-300">
            <span aria-hidden="true">📍</span>
            <span className="min-w-0 flex-1 truncate">
              {message.location.name ?? "Location"}
            </span>
            <span className="shrink-0 text-mist">
              {message.location.lat}, {message.location.lon}
            </span>
            <a
              className="shrink-0 text-signal hover:underline"
              href={`https://www.openstreetmap.org/?mlat=${message.location.lat}&mlon=${message.location.lon}#map=15/${message.location.lat}/${message.location.lon}`}
              target="_blank"
              rel="noreferrer"
            >
              Open in Maps
            </a>
          </div>
        ) : null}

        {message.reactions && message.reactions.length > 0 ? (
          <div
            className={`flex flex-wrap gap-1.5 px-1 pt-1.5 ${outbound ? "justify-end" : ""}`}
            aria-label="Reactions"
          >
            {message.reactions.map((reaction) => (
              <span
                key={reaction.reactor}
                title={reaction.name}
                className="inline-flex items-center gap-1 rounded-full border border-line/80 bg-void/60 px-2 py-0.5 font-mono text-[11px] text-zinc-300"
              >
                <span aria-hidden="true">{reaction.reaction}</span>
                <span>{reaction.name}</span>
              </span>
            ))}
          </div>
        ) : null}

        <div
          className={`flex flex-wrap items-center gap-x-2 gap-y-1 px-1 pt-1 font-mono text-[10px] ${
            outbound ? "text-signal/70" : "text-mist/60"
          }`}
        >
          <span>
            {outbound
              ? message.read_at
                ? `read ${formatTime(message.read_at)}`
                : message.status === "queued"
                  ? (scheduled ?? "queued")
                  : message.status === "failed"
                    ? "failed"
                    : "delivered"
              : "received"}
          </span>
          {message.edited_at !== undefined ? <span>· edited</span> : null}
          {message.rtt_ms !== undefined ? (
            <span>· rtt {formatRtt(message.rtt_ms)}</span>
          ) : null}
          {expiry !== null ? <span>· {expiry}</span> : null}
          {message.wire_id && !retracted ? (
            <button
              type="button"
              title="Reply to this message"
              onClick={() => onReply(message)}
              className="rounded px-1 py-0.5 text-mist hover:bg-void/60 hover:text-zinc-200"
            >
              ↩ reply
            </button>
          ) : null}
          {outbound && !message.file && message.wire_id && message.status !== "queued" && !editing ? (
            <button
              type="button"
              title="Edit this message"
              onClick={() => {
                setEditText(message.text);
                setEditError(null);
                setEditing(true);
              }}
              className="rounded px-1 py-0.5 text-mist hover:bg-void/60 hover:text-zinc-200"
            >
              edit
            </button>
          ) : null}
          {outbound && !message.file && message.wire_id && message.status !== "queued" ? (
            confirmingDelete ? (
              <>
                <button
                  type="button"
                  onClick={() => void run("delete", () => tau.deleteMessage(message.id))}
                  className="rounded px-1 py-0.5 text-rose-300 hover:bg-rose-400/10"
                >
                  confirm delete
                </button>
                <button
                  type="button"
                  onClick={() => setConfirmingDelete(false)}
                  className="rounded px-1 py-0.5 text-mist hover:bg-void/60 hover:text-zinc-200"
                >
                  keep
                </button>
              </>
            ) : (
              <button
                type="button"
                title="Delete this message"
                onClick={() => setConfirmingDelete(true)}
                className="rounded px-1 py-0.5 text-mist hover:bg-void/60 hover:text-rose-300"
              >
                delete
              </button>
            )
          ) : null}
          {outbound && !message.file && message.wire_id && message.status !== "queued" ? (
            <RetractButton
              busy={busy}
              onRetract={() => run("retract", () => tau.retractMessage(message.id))}
            />
          ) : null}
          {message.wire_id && !retracted ? (
            <button
              type="button"
              title="React to this message"
              aria-label="React to this message"
              aria-expanded={reactPickerOpen}
              onClick={() => setReactPickerOpen((open) => !open)}
              className="rounded px-1 py-0.5 text-mist hover:bg-void/60 hover:text-zinc-200"
            >
              ♡ react
            </button>
          ) : null}
          {message.wire_id && message.status !== "queued" ? (
            <button
              type="button"
              title={message.pinned ? "Unpin this message" : "Pin this message"}
              aria-label={message.pinned ? "Unpin this message" : "Pin this message"}
              aria-pressed={message.pinned}
              disabled={busy}
              onClick={() =>
                void run(message.pinned ? "unpin" : "pin", () =>
                  message.pinned
                    ? tau.unpinMessage(message.id)
                    : tau.pinMessage(message.id),
                )
              }
              className={`rounded px-1 py-0.5 hover:bg-void/60 ${
                message.pinned ? "text-signal" : "text-mist hover:text-zinc-200"
              }`}
            >
              {message.pinned ? "📌 pinned" : "pin"}
            </button>
          ) : null}
          {!message.group && !message.file && !retracted ? (
            <button
              type="button"
              title="Forward this message"
              aria-label="Forward this message"
              aria-expanded={forwardOpen}
              disabled={busy}
              onClick={() => setForwardOpen((open) => !open)}
              className="rounded px-1 py-0.5 text-mist hover:bg-void/60 hover:text-zinc-200"
            >
              ↗ forward
            </button>
          ) : null}
        </div>

        {reactPickerOpen ? (
          <div
            className={`flex flex-wrap gap-1 px-1 pt-1 ${outbound ? "justify-end" : ""}`}
            role="group"
            aria-label="Choose a reaction"
          >
            {REACT_OPTIONS.map((option) => {
              const active = ownReaction?.reaction === option;
              return (
                <button
                  key={option}
                  type="button"
                  title={active ? `Remove your ${option} reaction` : `React with ${option}`}
                  aria-label={active ? `Remove your ${option} reaction` : `React with ${option}`}
                  aria-pressed={active}
                  disabled={busy}
                  onClick={() => void handleReact(option)}
                  className={`grid h-7 w-7 place-items-center rounded-lg border text-sm ${
                    active
                      ? "border-signal/50 bg-signal/15"
                      : "border-line/80 bg-void/60 hover:border-signal/35 hover:bg-void"
                  }`}
                >
                  <span aria-hidden="true">{option}</span>
                </button>
              );
            })}
          </div>
        ) : null}
        {forwardOpen ? (
          <div className="flex w-full min-w-0 flex-wrap items-center gap-2 px-1 pt-1">
            <select
              value={forwardTarget}
              onChange={(event) => setForwardTarget(event.target.value)}
              aria-label="Forward to"
              className="h-9 min-w-0 flex-1 rounded-lg border border-line bg-void/70 px-2.5 font-mono text-xs text-zinc-200 outline-none focus:border-signal/60"
            >
              <option value="">Forward to…</option>
              {tau.peers.map((peer) => (
                <option key={peer.destination} value={`peer:${peer.destination}`}>
                  {displayName(peer)}
                </option>
              ))}
              {tau.groups.map((group) => (
                <option key={group.id} value={`group:${group.id}`}>
                  {group.name} (group)
                </option>
              ))}
            </select>
            <Button
              variant="primary"
              disabled={!forwardTarget || busy}
              onClick={() => void handleForward()}
            >
              {busy ? "forwarding…" : "Forward"}
            </Button>
          </div>
        ) : null}
        {actionError ? (
          <p className="px-1 pt-1 font-mono text-[10px] text-rose-300" role="alert">
            {actionError}
          </p>
        ) : null}
      </div>
    </article>
  );
}

function RetractButton({
  busy,
  onRetract,
}: {
  busy: boolean;
  onRetract: () => void;
}) {
  const [confirming, setConfirming] = useState(false);
  if (busy && confirming) return <span className="px-1 py-0.5 text-mist/60">retracting…</span>;
  if (confirming) {
    return (
      <>
        <button
          type="button"
          title="Retract this message"
          onClick={() => {
            setConfirming(false);
            onRetract();
          }}
          className="rounded px-1 py-0.5 text-rose-300 hover:bg-rose-400/10"
        >
          confirm retract
        </button>
        <button
          type="button"
          title="Keep this message"
          onClick={() => setConfirming(false)}
          className="rounded px-1 py-0.5 text-mist hover:bg-void/60 hover:text-zinc-200"
        >
          keep
        </button>
      </>
    );
  }
  return (
    <button
      type="button"
      title="Retract this message"
      onClick={() => setConfirming(true)}
      className="rounded px-1 py-0.5 text-mist hover:bg-void/60 hover:text-rose-300"
    >
      retract
    </button>
  );
}

function Composer({
  tau,
  selection,
  peer,
  group,
  replyTo,
  onCancelReply,
}: {
  tau: UseTauResult;
  selection: Exclude<Selection, null>;
  peer?: Peer;
  group?: TauGroup;
  replyTo: TauMessage | null;
  onCancelReply: () => void;
}) {
  const [text, setText] = useState("");
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expireSeconds, setExpireSeconds] = useState<number | null>(null);
  const [sendSeconds, setSendSeconds] = useState<number | null>(null);
  const [file, setFile] = useState<File | null>(null);
  const [pollMode, setPollMode] = useState(false);
  const [pollOptions, setPollOptions] = useState<string[]>(["", ""]);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const lastTypingSentRef = useRef(0);
  const typingActiveRef = useRef(false);

  const isGroup = selection.kind === "group";
  const targetId = selection.id;
  const defaultExpiry = isGroup
    ? tau.disappearingDefaults.groups[targetId]
    : tau.disappearingDefaults.peers[targetId];
  const filledPollOptions = pollOptions
    .map((option) => option.trim())
    .filter((option) => option !== "");

  function reportTyping(state: "start" | "stop") {
    const target = isGroup
      ? { group: targetId }
      : { peer: targetId };
    if (state === "start") {
      const now = Date.now();
      if (typingActiveRef.current && now - lastTypingSentRef.current < 3000) {
        return;
      }
      typingActiveRef.current = true;
      lastTypingSentRef.current = now;
      void tau.sendTyping(target, "start").catch(() => undefined);
    } else {
      if (!typingActiveRef.current) return;
      typingActiveRef.current = false;
      void tau.sendTyping(target, "stop").catch(() => undefined);
    }
  }

  // A conversation switch ends typing in the old one.
  useEffect(() => {
    return () => {
      typingActiveRef.current = false;
      lastTypingSentRef.current = 0;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selection.kind, selection.id]);

  async function submit() {
    const trimmedText = text.trim();
    if (file) {
      if (file.size > MAX_FILE_BYTES) {
        setError("File is larger than 10 MiB.");
        return;
      }
      setSending(true);
      setError(null);
      try {
        if (isGroup) await tau.sendGroupFile(targetId, file);
        else await tau.sendFile(targetId, file);
        setFile(null);
        reportTyping("stop");
        if (fileInputRef.current) fileInputRef.current.value = "";
        onCancelReply();
        textareaRef.current?.focus();
      } catch (caught) {
        setError(caught instanceof Error ? caught.message : "send failed");
      } finally {
        setSending(false);
      }
      return;
    }
    if (!trimmedText) {
      setError("Write a message first.");
      return;
    }
    if (isGroup && pollMode && filledPollOptions.length < 2) {
      setError("Add at least two poll options.");
      return;
    }
    setSending(true);
    setError(null);
    try {
      const expiresIn = expireSeconds === null ? undefined : expireSeconds;
      const sendIn = sendSeconds === null ? undefined : sendSeconds;
      if (isGroup) {
        await tau.sendGroup(
          targetId,
          text,
          expiresIn,
          pollMode ? undefined : replyTo?.wire_id,
          pollMode ? undefined : sendIn,
          pollMode ? filledPollOptions : undefined,
        );
      } else {
        await tau.send(
          targetId,
          text,
          expiresIn,
          replyTo?.wire_id,
          sendIn,
        );
      }
      setText("");
      if (pollMode) {
        setPollMode(false);
        setPollOptions(["", ""]);
      }
      reportTyping("stop");
      setSendSeconds(null);
      onCancelReply();
      textareaRef.current?.focus();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "send failed");
    } finally {
      setSending(false);
    }
  }

  function onKeyDown(event: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (
      event.key === "Enter" &&
      !event.shiftKey &&
      !event.nativeEvent.isComposing
    ) {
      event.preventDefault();
      if (!sending) void submit();
    }
  }

  return (
    <Card>
      <div className="flex flex-col gap-2 px-3 py-3">
        <p className="font-mono text-[11px] text-mist">
          to {isGroup ? (group?.name ?? "group") : displayName(peer)} · encrypted
          link
        </p>
        {replyTo ? (
          <div className="flex min-w-0 items-center gap-2 rounded-lg border border-signal/30 bg-signal/[0.06] px-3 py-2 text-[11px] text-zinc-300">
            <span className="min-w-0 flex-1 truncate font-mono">
              ↩ replying to {replyTo.direction === "out" ? "you" : replyTo.peer_name}:{" "}
              {replyTo.text.split("\n")[0].slice(0, 80)}
            </span>
            <button
              type="button"
              title="Cancel reply"
              onClick={onCancelReply}
              className="shrink-0 rounded px-1.5 py-0.5 font-mono text-mist hover:bg-void/60 hover:text-zinc-100"
            >
              ✕
            </button>
          </div>
        ) : null}
        {file ? (
          <div className="flex min-w-0 items-center gap-2 rounded-lg border border-signal/30 bg-signal/[0.06] px-3 py-2 text-[11px] text-zinc-300">
            <span className="min-w-0 flex-1 truncate font-mono">
              📎 {file.name} · {formatFileSize(file.size)}
            </span>
            <button
              type="button"
              title="Remove file"
              onClick={() => {
                setFile(null);
                if (fileInputRef.current) fileInputRef.current.value = "";
                setError(null);
              }}
              className="shrink-0 rounded px-1.5 py-0.5 font-mono text-mist hover:bg-void/60 hover:text-zinc-100"
            >
              ✕
            </button>
          </div>
        ) : null}
        {pollMode && isGroup ? (
          <div className="space-y-1.5">
            {pollOptions.map((option, index) => (
              <div key={index} className="flex min-w-0 items-center gap-2">
                <input
                  value={option}
                  onChange={(event) =>
                    setPollOptions((current) =>
                      current.map((currentOption, currentIndex) =>
                        currentIndex === index
                          ? event.target.value.slice(0, 100)
                          : currentOption,
                      ),
                    )
                  }
                  maxLength={100}
                  placeholder={`Option ${index + 1}`}
                  aria-label={`Poll option ${index + 1}`}
                  className="h-9 w-full min-w-0 rounded-lg border border-line bg-void/70 px-2.5 text-sm text-ink outline-none placeholder:text-faint focus:border-signal/60"
                />
                {pollOptions.length > 2 ? (
                  <Button
                    variant="subtle"
                    onClick={() =>
                      setPollOptions((current) =>
                        current.filter(
                          (_currentOption, currentIndex) =>
                            currentIndex !== index,
                        ),
                      )
                    }
                  >
                    remove
                  </Button>
                ) : null}
              </div>
            ))}
            {pollOptions.length < 8 ? (
              <Button
                variant="subtle"
                onClick={() => setPollOptions((current) => [...current, ""])}
              >
                + add option
              </Button>
            ) : null}
          </div>
        ) : null}

        <div className="flex items-end gap-2">
          <textarea
            ref={textareaRef}
            value={text}
            onChange={(event) => {
              const next = event.target.value;
              setText(next);
              setError(null);
              if (next.trim()) reportTyping("start");
              else reportTyping("stop");
            }}
            onBlur={() => reportTyping("stop")}
            onKeyDown={onKeyDown}
            rows={2}
            aria-label="Message text"
            placeholder={
              isGroup && pollMode
                ? "Ask the group a question…"
                : isGroup
                  ? "Write something to the group…"
                  : replyTo
                    ? `Replying to ${replyTo.direction === "out" ? "you" : replyTo.peer_name}…`
                    : "Write something to the mesh…"
            }
            className="min-h-[64px] w-full min-w-0 resize-y rounded-xl border border-line bg-void/70 px-3 py-2.5 text-[13px] leading-relaxed text-ink outline-none placeholder:text-faint focus:border-signal/60"
          />
          <input
            ref={fileInputRef}
            type="file"
            className="hidden"
            aria-label="Attach a file"
            onChange={(event) => {
              const picked = event.target.files?.[0] ?? null;
              if (picked && picked.size > MAX_FILE_BYTES) {
                setError("File is larger than 10 MiB.");
                setFile(null);
                event.target.value = "";
                return;
              }
              setFile(picked);
              setError(null);
            }}
          />
          <Button
            variant="ghost"
            title="Attach a file (up to 10 MiB)"
            disabled={sending}
            onClick={() => fileInputRef.current?.click()}
          >
            📎 attach
          </Button>
          <Button
            variant="primary"
            disabled={
              sending ||
              (!text.trim() && !file) ||
              (isGroup && pollMode && filledPollOptions.length < 2)
            }
            onClick={() => void submit()}
          >
            {sending
              ? "sending…"
              : file
                ? "Send file"
                : pollMode
                  ? "Ask poll"
                  : sendSeconds === null
                    ? "Send"
                    : "Schedule"}
          </Button>
        </div>

        <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
          {isGroup ? (
            <Button
              variant="subtle"
              onClick={() => {
                if (pollMode) {
                  setPollMode(false);
                } else {
                  setPollMode(true);
                  setFile(null);
                  setSendSeconds(null);
                  onCancelReply();
                  if (fileInputRef.current) fileInputRef.current.value = "";
                }
              }}
            >
              {pollMode ? "cancel poll" : "🗳️ ask a poll"}
            </Button>
          ) : null}
          {pollMode ? null : (
            <label className="flex items-center gap-1.5 font-mono text-[11px] text-mist">
              send later
              <select
                aria-label="Send later"
                value={sendSeconds === null ? "" : String(sendSeconds)}
                onChange={(event) => {
                  const value = event.target.value;
                  setSendSeconds(value === "" ? null : Number(value));
                }}
                className="h-9 rounded-lg border border-line bg-void/70 px-1.5 text-xs text-ink outline-none focus:border-signal/60"
              >
                {SEND_LATER_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
          )}
          <label className="flex items-center gap-1.5 font-mono text-[11px] text-mist">
            disappears
            <select
              aria-label="Message expiry"
              value={expireSeconds === null ? "" : String(expireSeconds)}
              onChange={(event) => {
                const value = event.target.value;
                setExpireSeconds(value === "" ? null : Number(value));
              }}
              className="h-9 rounded-lg border border-line bg-void/70 px-1.5 text-xs text-ink outline-none focus:border-signal/60"
            >
              <option value="">
                {defaultExpiry != null
                  ? `default (${describeDuration(defaultExpiry)})`
                  : "never"}
              </option>
              {EXPIRY_OPTIONS.slice(1).map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
          <span className="font-mono text-[11px] text-faint">
            Enter to send · Shift+Enter for a new line
          </span>
        </div>
        <ErrorNote message={error} />
      </div>
    </Card>
  );
}

function PeerPanel({
  tau,
  peer,
  onPeerGone,
}: {
  tau: UseTauResult;
  peer: Peer;
  onPeerGone: () => void;
}) {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [aliasDraft, setAliasDraft] = useState(peer.alias ?? "");
  const [aliasSaving, setAliasSaving] = useState(false);
  const [noteDraft, setNoteDraft] = useState(peer.note ?? "");
  const [noteSaving, setNoteSaving] = useState(false);
  const [confirmingForget, setConfirmingForget] = useState(false);

  const dest = peer.destination;
  const ping = tau.pings[dest];
  const info = tau.infos[dest];
  const verification = tau.verifications[dest];
  const block = tau.blocks[dest];
  const archived = tau.archivedConversations.some(
    (entry) => entry.destination === dest,
  );
  const muted = tau.mutedConversations.peers.includes(dest);
  const disappearing = tau.disappearingDefaults.peers[dest];

  async function run(label: string, work: () => Promise<unknown>) {
    setBusy(true);
    setError(null);
    try {
      await work();
    } catch (caught) {
      setError(
        `${label}: ${caught instanceof Error ? caught.message : "failed"}`,
      );
    } finally {
      setBusy(false);
    }
  }

  async function saveAlias() {
    setAliasSaving(true);
    setError(null);
    try {
      await tau.setAlias(dest, aliasDraft.trim());
    } catch (caught) {
      setError(
        `alias: ${caught instanceof Error ? caught.message : "could not save alias"}`,
      );
    } finally {
      setAliasSaving(false);
    }
  }

  async function saveNote() {
    setNoteSaving(true);
    setError(null);
    try {
      await tau.setNote(dest, noteDraft.trim());
    } catch (caught) {
      setError(
        `note: ${caught instanceof Error ? caught.message : "could not save note"}`,
      );
    } finally {
      setNoteSaving(false);
    }
  }

  return (
    <Card>
      <CardTitle>Peer</CardTitle>
      <div className="flex flex-col gap-3 px-3 py-3">
        <div className="flex min-w-0 flex-wrap items-center gap-2 text-sm">
          <span className="min-w-0 flex-1 truncate font-semibold">
            {displayName(peer)}
          </span>
          {peer.verified ? <Chip>✓ verified</Chip> : null}
          {peer.blocked ? <Chip>blocked</Chip> : null}
          {peer.key_change_at ? <Chip>key changed</Chip> : null}
          {peer.name_change_at && peer.previous_name ? (
            <Chip>
              renamed from {peer.previous_name}
            </Chip>
          ) : null}
          <span className="font-mono text-xs text-faint">
            {peerLabel(peer.name, dest)} · {peer.hops} hop
            {peer.hops === 1 ? "" : "s"} · seen{" "}
            {formatDateTime(peer.last_seen)}
          </span>
        </div>

        <div className="flex flex-wrap gap-2">
          <Button
            variant="subtle"
            disabled={ping?.pending}
            onClick={() => void run("ping", () => tau.pingPeer(dest))}
          >
            {ping?.pending
              ? "pinging…"
              : ping?.rttMs !== undefined
                ? `ping · ${formatRtt(ping.rttMs)}`
                : "ping"}
          </Button>
          <Button
            variant="subtle"
            disabled={info?.pending}
            onClick={() => void run("info", () => tau.fetchPeerInfo(dest))}
          >
            {info?.pending ? "loading…" : "info"}
          </Button>
          <Button
            variant="subtle"
            disabled={verification?.pending}
            onClick={() => void run("verify", () => tau.verifyPeer(dest))}
          >
            {verification?.pending ? "verifying…" : "verify"}
          </Button>
          <Button
            variant={peer.blocked ? "primary" : "danger"}
            disabled={block?.pending || busy}
            onClick={() => void run("block", () => tau.blockPeer(dest, !peer.blocked))}
          >
            {peer.blocked ? "unblock" : "block"}
          </Button>
          {confirmingForget ? (
            <>
              <Button
                variant="danger"
                onClick={() =>
                  void run("forget", async () => {
                    await tau.forgetPeer(dest);
                    onPeerGone();
                  })
                }
              >
                confirm forget
              </Button>
              <Button
                variant="subtle"
                onClick={() => setConfirmingForget(false)}
              >
                keep
              </Button>
            </>
          ) : (
            <Button
              variant="danger"
              onClick={() => setConfirmingForget(true)}
            >
              forget
            </Button>
          )}
        </div>

        {ping?.error ? <ErrorNote message={`ping: ${ping.error}`} /> : null}
        {info?.error ? <ErrorNote message={`info: ${info.error}`} /> : null}
        {info?.info ? (
          <div className="rounded-lg border border-line-soft bg-panel-2 px-3 py-2 font-mono text-xs text-mist">
            {info.info.name ? <p>name: {info.info.name}</p> : null}
            {info.info.version ? <p>tau {info.info.version}</p> : null}
            {info.info.rns_version ? (
              <p>Reticulum {info.info.rns_version}</p>
            ) : null}
            {info.info.protocol !== undefined ? (
              <p>protocol {info.info.protocol}</p>
            ) : null}
            {info.info.uptime !== undefined ? (
              <p>uptime {info.info.uptime}s</p>
            ) : null}
            {info.info.peers !== undefined ? (
              <p>{info.info.peers} peers</p>
            ) : null}
            {info.info.lxmf_enabled !== undefined ? (
              <p>LXMF {info.info.lxmf_enabled ? "enabled" : "disabled"}</p>
            ) : null}
          </div>
        ) : null}

        {verification?.error ? (
          <ErrorNote message={`verify: ${verification.error}`} />
        ) : null}
        {verification?.safetyNumber && !verification.result?.verified_at ? (
          <div className="rounded-lg border border-signal/40 bg-signal/[0.06] px-3 py-2">
            <p className="text-xs text-mist">
              Safety number — compare it with {displayName(peer)} over another
              channel. Nothing is recorded until you confirm it matches.
            </p>
            <p className="mt-1 font-mono text-sm break-all">
              {verification.safetyNumber}
            </p>
            <div className="mt-2">
              <Button
                variant="primary"
                onClick={() =>
                  void run("confirm", () => tau.confirmVerified(dest))
                }
              >
                Numbers match
              </Button>
            </div>
          </div>
        ) : null}
        {verification?.result?.verified_at ? (
          <p className="text-sm text-signal">
            ✓ Verified{" "}
            {formatDateTime(verification.result.verified_at)}
          </p>
        ) : null}

        <div className="flex min-w-0 flex-col gap-2">
          <label className="flex min-w-0 flex-col gap-1 text-xs text-mist">
            Alias
            <span className="flex min-w-0 gap-2">
              <input
                value={aliasDraft}
                onChange={(event) => setAliasDraft(event.target.value)}
                placeholder="Local label for this peer"
                aria-label="Alias"
                className="h-9 min-w-0 flex-1 rounded-lg border border-line bg-void/70 px-2.5 text-sm text-ink outline-none placeholder:text-faint focus:border-signal/60"
              />
              <Button
                variant="subtle"
                disabled={aliasSaving}
                onClick={() => void saveAlias()}
              >
                {aliasSaving ? "saving…" : "Save"}
              </Button>
            </span>
          </label>
          {tau.bridge?.notes_editable ? (
            <label className="flex min-w-0 flex-col gap-1 text-xs text-mist">
              Private note
              <span className="flex min-w-0 gap-2">
                <input
                  value={noteDraft}
                  onChange={(event) => setNoteDraft(event.target.value)}
                  placeholder="Private note — never sent to peers"
                  aria-label="Private note"
                  className="h-9 min-w-0 flex-1 rounded-lg border border-line bg-void/70 px-2.5 text-sm text-ink outline-none placeholder:text-faint focus:border-signal/60"
                />
                <Button
                  variant="subtle"
                  disabled={noteSaving}
                  onClick={() => void saveNote()}
                >
                  {noteSaving ? "saving…" : "Save"}
                </Button>
              </span>
            </label>
          ) : null}
          {peer.note && !tau.bridge?.notes_editable ? (
            <p className="text-xs text-mist">Note: {peer.note}</p>
          ) : null}
        </div>

        <div className="flex flex-col gap-1 border-t border-line-soft pt-2">
          <Toggle
            checked={archived}
            label={archived ? "Archived" : "Archive conversation"}
            onChange={(next) =>
              void run("archive", () => tau.archiveConversation(dest, next))
            }
          />
          <Toggle
            checked={muted}
            label={muted ? "Muted" : "Mute conversation"}
            onChange={(next) =>
              void run("mute", () => tau.muteConversation(dest, false, next))
            }
          />
          <label className="flex min-w-0 items-center justify-between gap-3 py-2">
            <span className="min-w-0 text-sm">Disappearing messages</span>
            <select
              aria-label="Disappearing messages default"
              value={disappearing === undefined ? "" : String(disappearing)}
              onChange={(event) => {
                const value = event.target.value;
                void run("disappearing", () =>
                  tau.setDisappearing(
                    dest,
                    false,
                    value === "" ? null : Number(value),
                  ),
                );
              }}
              className="h-9 shrink-0 rounded-lg border border-line bg-void/70 px-1.5 text-xs text-ink outline-none focus:border-signal/60"
            >
              <option value="">off</option>
              {disappearing !== undefined &&
              !DISAPPEARING_PRESETS.includes(disappearing) ? (
                <option value={String(disappearing)}>
                  {describeDuration(disappearing)}
                </option>
              ) : null}
              {DISAPPEARING_PRESETS.map((seconds) => (
                <option key={seconds} value={String(seconds)}>
                  {describeDuration(seconds)}
                </option>
              ))}
            </select>
          </label>
          <div className="flex flex-wrap gap-2 pt-1">
            <Button
              variant="subtle"
              disabled={busy}
              onClick={() => void run("mark unread", () => tau.markUnread(dest))}
            >
              Mark unread
            </Button>
          </div>
        </div>

        <ErrorNote message={error} />
      </div>
    </Card>
  );
}

function OutboxSection({ tau }: { tau: UseTauResult }) {
  const [error, setError] = useState<string | null>(null);
  const [cancellingId, setCancellingId] = useState<string | null>(null);
  const [retrying, setRetrying] = useState(false);
  const [droppingId, setDroppingId] = useState<string | null>(null);
  const [confirmDropId, setConfirmDropId] = useState<string | null>(null);

  if (
    tau.outbox.length === 0 &&
    tau.heldMail.length === 0 &&
    tau.heldForOthers === 0 &&
    !error
  ) {
    return null;
  }

  async function handleRetryAll() {
    setRetrying(true);
    setError(null);
    try {
      await tau.retryOutbox();
    } catch (caught) {
      setError(
        caught instanceof Error ? caught.message : "retry failed",
      );
    } finally {
      setRetrying(false);
    }
  }

  async function handleCancel(entryId: string) {
    setCancellingId(entryId);
    setError(null);
    try {
      await tau.cancelOutbox(entryId);
    } catch (caught) {
      setError(
        caught instanceof Error ? caught.message : "cancel failed",
      );
    } finally {
      setCancellingId(null);
    }
  }

  async function handleDrop(entryId: string) {
    setDroppingId(entryId);
    setError(null);
    try {
      await tau.dropHeld(entryId);
      setConfirmDropId(null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "drop failed");
    } finally {
      setDroppingId(null);
    }
  }

  return (
    <div className="min-w-0 lg:col-span-2">
      <Card>
        <CardTitle
          right={
            tau.outbox.length > 0 ? (
              <Button
                variant="subtle"
                disabled={retrying}
                onClick={() => void handleRetryAll()}
              >
                {retrying ? "retrying…" : "retry all"}
              </Button>
            ) : undefined
          }
        >
          Outbox
          {tau.heldForOthers > 0
            ? ` · holding ${tau.heldForOthers} for others`
            : ""}
        </CardTitle>
        <div className="flex flex-col gap-2 px-3 py-2">
          <ErrorNote message={error} />
          {tau.outbox.length === 0 ? (
            <p className="text-sm text-faint">Outbox is empty.</p>
          ) : (
            tau.outbox.map((entry) => (
              <div
                key={entry.id}
                className="flex min-w-0 flex-wrap items-center gap-2 rounded-lg border border-line-soft bg-panel-2 px-3 py-2"
              >
                <div className="min-w-0 flex-1">
                  <p className="min-w-0 truncate text-sm">
                    to {entry.to_name} · {entry.text}
                  </p>
                  <p className="font-mono text-[11px] text-mist">
                    {entry.attempts} attempt
                    {entry.attempts === 1 ? "" : "s"}
                    {entry.last_error ? ` · ${entry.last_error}` : ""}
                    {entry.held_by.length > 0
                      ? ` · sealed copy held by ${entry.held_by.length} peer${entry.held_by.length === 1 ? "" : "s"}`
                      : ""}
                    {entry.send_after
                      ? ` · ${formatSendsIn(entry.send_after) ?? "scheduled"}`
                      : ""}
                  </p>
                </div>
                <Button
                  variant="subtle"
                  disabled={cancellingId !== null || retrying}
                  onClick={() => void handleCancel(entry.id)}
                >
                  {cancellingId === entry.id ? "cancelling…" : "cancel"}
                </Button>
              </div>
            ))
          )}
          {tau.heldMail.length > 0 ? (
            <div className="border-t border-line-soft pt-2">
              <p className="pb-1 text-xs font-semibold tracking-widest text-mist uppercase">
                Held mail for others · {tau.heldMail.length}
              </p>
              {tau.heldMail.map((entry) => (
                <div
                  key={entry.id}
                  className="flex min-w-0 flex-wrap items-center gap-2 rounded-lg border border-line-soft bg-panel-2 px-3 py-2"
                >
                  <div className="min-w-0 flex-1">
                    <p className="min-w-0 truncate text-sm">
                      {entry.from_name} → {entry.to_name} ·{" "}
                      {formatFileSize(entry.blob_bytes)}
                    </p>
                    <p className="font-mono text-[11px] text-mist">
                      stored {formatDateTime(entry.stored_at)}
                    </p>
                  </div>
                  {confirmDropId === entry.id ? (
                    <>
                      <Button
                        variant="danger"
                        disabled={droppingId !== null}
                        onClick={() => void handleDrop(entry.id)}
                      >
                        {droppingId === entry.id ? "dropping…" : "confirm drop"}
                      </Button>
                      <Button
                        variant="subtle"
                        onClick={() => setConfirmDropId(null)}
                      >
                        keep
                      </Button>
                    </>
                  ) : (
                    <Button
                      variant="danger"
                      disabled={droppingId !== null}
                      onClick={() => setConfirmDropId(entry.id)}
                    >
                      drop
                    </Button>
                  )}
                </div>
              ))}
            </div>
          ) : null}
        </div>
      </Card>
    </div>
  );
}
