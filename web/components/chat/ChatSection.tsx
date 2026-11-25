// ChatSection.tsx — the Chat section: conversation list + thread + outbox.
// Owns selection (via the URL hash: #chat?peer=… / #chat?group=…), reply /
// edit state, and wires every message action to api.ts.

"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  acceptDmRequest,
  addPeer,
  addReminder,
  cancelReminder,
  clearReaction,
  closeDmPoll,
  closeGroupPoll,
  createGroup,
  declineDmRequest,
  deleteDraft,
  deleteMessage,
  editMessage,
  forwardMessage,
  getArchivedConversations,
  getDrafts,
  getGroupMessages,
  getMutedConversations,
  getReminders,
  joinGroupViaLink,
  pinMessage,
  reactToMessage,
  retractMessage,
  saveDraft,
  sendFile,
  sendGroupFile,
  sendMessage,
  unpinMessage,
  voteDmPoll,
  voteGroupPoll,
  type SendMessageOptions,
} from "@/lib/api";
import type { ChatMessage, MessageDraft, Reminder } from "@/lib/types";
import {
  buildConversations,
  useBeacons,
  useBroadcasts,
  useConversations,
  useOutbox,
  usePeerActions,
  type Conversation,
} from "@/lib/use-tau";
import { useHash } from "../chrome/useHash";
import { useTauContext } from "../chrome/TauContext";
import BeaconControl from "./BeaconControl";
import BroadcastsPanel from "./BroadcastsPanel";
import ConversationList from "./ConversationList";
import DraftsPanel from "./DraftsPanel";
import MessageSearch from "./MessageSearch";
import NewGroupPanel from "./NewGroupPanel";
import OutboxPanel from "./OutboxPanel";
import RemindersPanel from "./RemindersPanel";
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

/** The drafts list key for one conversation: `dm:<hash>`, `group:<id>` or `channel:<name>`. */
function draftKeyFor(kind: "peer" | "group" | "channel", target: string): string {
  return `${kind === "group" ? "group" : kind === "channel" ? "channel" : "dm"}:${target}`;
}

export default function ChatSection() {
  const tau = useTauContext();
  const { peer: selectedPeer, group: selectedGroup, navigate } = useHash();
  const actions = usePeerActions();
  const outbox = useOutbox();
  const beacons = useBeacons();
  const broadcasts = useBroadcasts();

  const [mutedPeers, setMutedPeers] = useState<Set<string>>(new Set());
  const [mutedGroups, setMutedGroups] = useState<Set<string>>(new Set());
  const [archived, setArchived] = useState<Set<string>>(new Set());
  const [replyTo, setReplyTo] = useState<ChatMessage | null>(null);
  const [editing, setEditing] = useState<ChatMessage | null>(null);
  // The conversation a queued-edit notice belongs to: a notice set
  // for one chat must not leak into another when the user switches
  // chats, or when the edit request finishes after a switch.
  const [editNotice, setEditNotice] = useState<{
    text: string;
    key: string;
  } | null>(null);
  // Same scoping as the queued-edit notice: a queued-retract notice
  // belongs to one conversation and must not leak into another.
  const [retractNotice, setRetractNotice] = useState<{
    text: string;
    key: string;
  } | null>(null);
  const [showOutbox, setShowOutbox] = useState(false);
  const [showBroadcasts, setShowBroadcasts] = useState(false);
  const [searchActive, setSearchActive] = useState(false);
  const [searchReset, setSearchReset] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [drafts, setDrafts] = useState<MessageDraft[]>([]);
  const [draftsLoading, setDraftsLoading] = useState(true);
  const [reminders, setReminders] = useState<Reminder[]>([]);
  const [remindersLoading, setRemindersLoading] = useState(true);
  const [pendingSeed, setPendingSeed] = useState<{ key: string; text: string } | null>(null);
  // The key of a draft saved from this composer: the seed effect must
  // not pour it straight back into the box the composer just cleared
  // once the drafts refresh lands. The mark survives refreshes and is
  // cleared only on a conversation switch — the channel flow's consumed
  // mark, #608, in chat shape.
  const [justSavedDraftKey, setJustSavedDraftKey] = useState<string | null>(null);
  const [showJoin, setShowJoin] = useState(false);
  const [joinLink, setJoinLink] = useState("");
  const [joinBusy, setJoinBusy] = useState(false);
  const [joinResult, setJoinResult] = useState<string | null>(null);
  const [showNewGroup, setShowNewGroup] = useState(false);

  const { conversations, archived: archivedConvs, ignoredGroups, dmRequests, refreshRequests, refreshIgnoredGroups } = useConversations(
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
          includeIgnored: true,
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
          includeIgnored: true,
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
    // An ignored group keeps its read cursor frozen so its unread posts
    // come back when it is un-ignored.
    if (selected.ignored) return;
    const target = selected.kind === "group" ? { group: selected.key } : { peer: selected.key };
    void actions
      .markRead(target)
      .then(() => tau.refresh())
      .catch(() => undefined);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedKey]);

  // History of the open ignored group, fetched explicitly: the
  // snapshot's default listing hides ignored posts, so the thread would
  // look empty without it. Kept apart from tau.messages so ignored
  // posts never leak into the feed or the conversation list — only the
  // open thread sees them. A slow response must not overwrite a newer
  // selection, and a failed fetch shows a retryable error instead of
  // silently leaving the thread empty.
  const [ignoredHistory, setIgnoredHistory] = useState<ChatMessage[]>([]);
  const [ignoredHistoryError, setIgnoredHistoryError] = useState(false);
  const [ignoredHistoryRetry, setIgnoredHistoryRetry] = useState(0);
  const ignoredHistorySeq = useRef(0);
  // Coalescing for the history fetch: a fetch in flight for the open
  // thread, the thread it belongs to, and whether messages moved while
  // it was running. Without this, every snapshot move bumped the seq
  // and invalidated the in-flight response, so in a busy chat no
  // response could ever commit and the thread stayed empty.
  const ignoredHistoryInFlight = useRef(false);
  const ignoredHistoryPending = useRef(false);
  const ignoredHistoryKey = useRef<string | null>(null);
  // Ignoring or un-ignoring the open group keeps its key, so the
  // ignored flag must be a dependency too.
  const selectedIgnoredGroup = selected?.kind === "group" && !!selected.ignored;
  useEffect(() => {
    const groupId =
      selected?.kind === "group" && selectedIgnoredGroup ? selected.key : null;
    if (!groupId) {
      // Leaving the ignored thread (or un-ignoring it) invalidates any
      // fetch still running for it; the newer fetch owns the flags.
      ignoredHistorySeq.current += 1;
      ignoredHistoryKey.current = null;
      ignoredHistoryPending.current = false;
      setIgnoredHistory([]);
      setIgnoredHistoryError(false);
      return;
    }
    const startFetch = (id: string) => {
      const seq = ++ignoredHistorySeq.current;
      ignoredHistoryInFlight.current = true;
      void (async () => {
        try {
          const history = await getGroupMessages(id);
          if (seq !== ignoredHistorySeq.current) return;
          setIgnoredHistory(history);
          setIgnoredHistoryError(false);
        } catch {
          if (seq === ignoredHistorySeq.current) setIgnoredHistoryError(true);
        } finally {
          // Superseded by a selection change: the newer fetch owns the
          // in-flight flags now, so leave them alone.
          if (seq !== ignoredHistorySeq.current) return;
          ignoredHistoryInFlight.current = false;
          // Messages moved while this fetch ran: run at most one
          // follow-up so the thread still catches up, without starting
          // a fetch per message.
          if (ignoredHistoryPending.current) {
            ignoredHistoryPending.current = false;
            startFetch(id);
          }
        }
      })();
    };
    if (groupId !== ignoredHistoryKey.current) {
      // New selection (or newly ignored): drop any in-flight fetch for
      // the old thread — a slow response must not overwrite a newer
      // selection — and start fresh for this one.
      ignoredHistoryKey.current = groupId;
      ignoredHistoryPending.current = false;
      startFetch(groupId);
      return;
    }
    // Same thread: a new post, edit, or deletion moved the snapshot.
    // Re-fetch when the snapshot's messages move: the snapshot never
    // carries ignored posts, so without this a new post, edit, or
    // deletion in the open ignored group would never reach the thread.
    // If a fetch is already running for this thread, let its result
    // commit and queue at most one follow-up instead.
    if (ignoredHistoryInFlight.current) {
      ignoredHistoryPending.current = true;
      return;
    }
    startFetch(groupId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedKey, selectedIgnoredGroup, tau.messages, ignoredHistoryRetry]);

  // The open thread sees the snapshot plus the ignored group's explicit
  // history; the snapshot's copy wins on overlap (a group un-ignored
  // while open lands in both). The list and feed keep tau.messages.
  const threadMessages = useMemo(() => {
    if (ignoredHistory.length === 0) return tau.messages;
    const byId = new Map<number, ChatMessage>();
    for (const message of ignoredHistory) byId.set(message.id, message);
    for (const message of tau.messages) byId.set(message.id, message);
    return [...byId.values()];
  }, [tau.messages, ignoredHistory]);

  const refreshDrafts = useCallback(async () => {
    try {
      setDrafts(await getDrafts());
    } catch {
      // Drafts are advisory; the chat list works without them.
    } finally {
      setDraftsLoading(false);
    }
  }, []);

  useEffect(() => {
    void refreshDrafts();
  }, [refreshDrafts]);

  // Snapshot, timer and local-action refreshes can overlap, and a slow
  // list response must never overwrite a newer one — a canceled row
  // would reappear. Each refresh takes the next generation and only
  // the latest generation may apply its response.
  const remindersGeneration = useRef(0);
  const reminderRetryTimer = useRef<number | undefined>(undefined);
  const reminderRetryAttempts = useRef(0);
  const remindersMounted = useRef(true);
  const refreshReminders = useCallback(async (isRetry = false) => {
    const generation = ++remindersGeneration.current;
    // Each independent refresh starts a fresh retry budget; a retry in
    // the chain keeps the count, so failures from separate triggers
    // cannot consume each other's retries.
    if (!isRetry) reminderRetryAttempts.current = 0;
    // A newer refresh supersedes any pending retry.
    window.clearTimeout(reminderRetryTimer.current);
    reminderRetryTimer.current = undefined;
    try {
      const next = await getReminders();
      if (generation !== remindersGeneration.current) return;
      reminderRetryAttempts.current = 0;
      setReminders(next);
    } catch {
      // Reminders are advisory; the chat list works without them.
      // If the latest request failed, retry a bounded number of times so
      // an older successful list that was discarded is not lost for good.
      // A retry is scheduled only while mounted: an in-flight request
      // can reject after unmount, and must not keep polling then.
      if (
        generation === remindersGeneration.current &&
        remindersMounted.current &&
        reminderRetryAttempts.current < 3
      ) {
        const attempt = ++reminderRetryAttempts.current;
        reminderRetryTimer.current = window.setTimeout(
          () => void refreshReminders(true),
          5000 * 2 ** (attempt - 1),
        );
      }
    } finally {
      if (generation === remindersGeneration.current) setRemindersLoading(false);
    }
  }, []);

  useEffect(
    () => () => {
      // Invalidate any in-flight list request and stop retries: leaving
      // the chat must not schedule further requests.
      remindersMounted.current = false;
      remindersGeneration.current += 1;
      window.clearTimeout(reminderRetryTimer.current);
    },
    [],
  );

  // Reminders can fire or change outside this tab, so reload them on
  // every snapshot change as well as after local snooze/cancel actions.
  useEffect(() => {
    void refreshReminders();
  }, [refreshReminders, tau.messages, tau.unread]);

  // Reload once shortly after the soonest pending reminder comes due,
  // so a fired reminder leaves the sidebar. An overdue reminder retries
  // at a fixed interval rather than in a tight loop; the timer is
  // clamped to the largest delay setTimeout supports, and each reload
  // re-arms it.
  useEffect(() => {
    if (reminders.length === 0) return;
    const soonest = Math.min(...reminders.map((reminder) => reminder.due_at));
    const delay = Math.max(soonest * 1000 - Date.now() + 1000, 5000);
    const timer = window.setTimeout(
      () => void refreshReminders(),
      Math.min(delay, 2_147_483_647),
    );
    return () => window.clearTimeout(timer);
  }, [reminders, refreshReminders]);

  // A conversation with a saved draft loads it into the composer when
  // opened; a conversation without one clears the seed. A draft saved
  // from this composer is already parked, so it must not re-seed the
  // box it was just cleared from — the mark is cleared only on a
  // conversation switch, below.
  useEffect(() => {
    if (!selectedKey) {
      setPendingSeed(null);
      return;
    }
    const draft = drafts.find((d) => draftKeyFor(d.kind, d.target) === selectedKey);
    if (draft && justSavedDraftKey === selectedKey) {
      setPendingSeed(null);
      return;
    }
    setPendingSeed(draft ? { key: selectedKey, text: draft.text } : null);
  }, [selectedKey, drafts, justSavedDraftKey]);

  // Reopening a conversation later should load its draft like any
  // other — the just-saved mark only covers the conversation it was
  // saved from, until the user leaves it.
  useEffect(() => {
    setJustSavedDraftKey(null);
  }, [selectedKey]);

  const handleOpenDraft = useCallback(
    (draft: MessageDraft) => {
      if (draft.kind === "channel") {
        // Channel drafts open in the channels section, which seeds its
        // own composer from the draft — no pending seed crosses sections.
        navigate("channels", { channel: draft.target });
        return;
      }
      if (draft.kind === "group") navigate("chat", { group: draft.target });
      else navigate("chat", { peer: draft.target });
      setPendingSeed({ key: draftKeyFor(draft.kind, draft.target), text: draft.text });
    },
    [navigate],
  );

  const handleDeleteDraft = useCallback(
    (draft: MessageDraft) => {
      void run("Delete draft failed", () => deleteDraft(draft.kind, draft.target)).then(() =>
        void refreshDrafts(),
      );
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [refreshDrafts],
  );

  const handleOpenReminder = useCallback(
    (reminder: Reminder) => {
      if (reminder.kind === "group") navigate("chat", { group: reminder.target });
      else navigate("chat", { peer: reminder.target });
    },
    [navigate],
  );

  const handleCancelReminder = useCallback(
    (reminder: Reminder) => {
      void run("Cancel reminder failed", () => cancelReminder(reminder.id)).then(() =>
        void refreshReminders(),
      );
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [refreshReminders],
  );

  const handleSnooze = useCallback(
    (message: ChatMessage, inSeconds: number) => {
      void run("Snooze failed", () => addReminder(message.id, inSeconds)).then(() =>
        void refreshReminders(),
      );
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [refreshReminders],
  );

  const handleSaveDraft = useCallback(
    async (text: string) => {
      if (!selected) return;
      // Errors propagate to the composer, which shows them and keeps
      // the text — a failed save must never clear the box.
      await saveDraft(selected.kind === "group" ? "group" : "peer", selected.key, text);
      // Mark the just-saved draft consumed before the drafts refresh
      // lands, so the seed effect does not pour it straight back into
      // the cleared box (#609, mirroring the channel fix in #608).
      if (selectedKey) setJustSavedDraftKey(selectedKey);
      void refreshDrafts();
    },
    [selected, selectedKey, refreshDrafts],
  );

  const handleSelect = useCallback(
    (conv: Conversation) => {
      if (conv.kind === "group") navigate("chat", { group: conv.key });
      else navigate("chat", { peer: conv.key });
    },
    [navigate],
  );

  async function handleJoinGroup() {
    const link = joinLink.trim();
    if (!link || joinBusy) return;
    setJoinBusy(true);
    setError(null);
    setJoinResult(null);
    try {
      const result = await joinGroupViaLink(link);
      await tau.refresh();
      if (
        (result.status === "added" || result.status === "already_member") &&
        typeof result.group === "object" &&
        result.group.id
      ) {
        setJoinLink("");
        setShowJoin(false);
        navigate("chat", { group: result.group.id });
      } else if (result.status === "pending") {
        const name =
          typeof result.group === "object" ? result.group.name : result.group;
        setJoinResult(
          name
            ? `Join request pending — the creator of “${name}” has to approve it first.`
            : "Join request pending — the creator has to approve it first.",
        );
      } else {
        const name =
          typeof result.group === "object" ? result.group.name : result.group;
        setJoinResult(
          name
            ? `Join request for “${name}” queued — it will be decided when it reaches the creator.`
            : "Join request queued — it will be decided when it reaches the creator.",
        );
      }
    } catch (err) {
      setError(`Join failed: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setJoinBusy(false);
    }
  }

  async function handleCreateGroup(
    name: string,
    members: string[],
    opts?: { secret?: boolean },
  ) {
    // The panel owns validation and shows API errors inline; this
    // handler only closes the panel and opens the new thread. It
    // mirrors the join flow: refresh, then navigate whether or not
    // the refresh landed — the next snapshot (≤5s poll, or the next
    // ws frame) picks the group up, so a failed refresh is a brief
    // empty thread, not a failed creation. A local fallback stash
    // was tried here and removed: it needed its own retirement rule,
    // diverged the sidebar's group lookup, and held only one
    // pending group.
    const group = await createGroup(name, members, opts);
    setShowNewGroup(false);
    await tau.refresh().catch(() => undefined);
    navigate("chat", { group: group.id });
    return group;
  }

  const handleBack = useCallback(() => navigate("chat"), [navigate]);

  const handleOpenSearchResult = useCallback(
    (message: ChatMessage) => {
      if (message.group) navigate("chat", { group: message.group });
      else if (message.peer) navigate("chat", { peer: message.peer });
      setSearchReset((n) => n + 1);
    },
    [navigate],
  );

  async function run(label: string, fn: () => Promise<unknown>): Promise<boolean> {
    setError(null);
    try {
      await fn();
      await tau.refresh();
      return true;
    } catch (err) {
      setError(`${label}: ${err instanceof Error ? err.message : String(err)}`);
      return false;
    }
  }

  const handleSend = useCallback(
    async (text: string, opts: ComposerSendOptions): Promise<boolean> => {
      if (!selected) return false;
      if (editing) {
        const id = editing.id;
        setEditing(null);
        setEditNotice(null);
        let queued = false;
        const ok = await run("Edit failed", async () => {
          const response = (await editMessage(id, text)) as { queued?: boolean } | null;
          queued = response?.queued === true;
        });
        // A queued edit is accepted but not yet delivered: the message
        // keeps its old text here until the outbox delivers it.  The
        // notice belongs to this conversation only.
        if (ok && queued) {
          setEditNotice({
            text: "Edit queued — the peer is offline. It goes out when they are reachable, and the message keeps its old text until then.",
            key: selected.key,
          });
        }
        return ok;
      }
      const target = selected.kind === "group" ? { group: selected.key } : { to: selected.key };
      const sendOpts: SendMessageOptions = {
        replyTo: opts.replyTo,
        expiresIn: opts.expiresIn,
        sendIn: opts.sendIn,
        pollOptions: opts.pollOptions,
        location: opts.location,
        contact: opts.contact,
      };
      setReplyTo(null);
      const sent = await run("Send failed", () => sendMessage(target, text, sendOpts));
      // A sent message closes the draft it was loaded from, like
      // `draft send` — but only when the send actually went through.
      if (sent && pendingSeed && pendingSeed.key === selectedKey) {
        await deleteDraft(selected.kind === "group" ? "group" : "peer", selected.key).catch(
          () => undefined,
        );
        void refreshDrafts();
      }
      return sent;
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [selected, editing, pendingSeed, selectedKey, refreshDrafts],
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
          // A queued retract is accepted but not yet delivered: the
          // message stays visible until the outbox delivers it, and
          // a second Retract reuses the queued entry instead of
          // storing a duplicate leg — so say so, like a queued edit.
          void (async () => {
            let queued = false;
            const ok = await run("Retract failed", async () => {
              const response = (await retractMessage(message.id)) as {
                queued?: boolean;
              } | null;
              queued = response?.queued === true;
            });
            if (ok && queued && selected) {
              setRetractNotice({
                text: "Retraction queued — the peer is offline. It goes out when they are reachable, and the message stays visible until then.",
                key: selected.key,
              });
            }
          })();
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

  const handleAddPeer = useCallback(
    async (message: ChatMessage) => {
      if (!message.contact) return;
      // Errors must reach the bubble's catch: it shows the inline
      // error and keeps the Add button for a retry. Routing through
      // `run()` swallows them into a boolean, which marked a failed
      // add as done and hid the button needed to retry.
      await addPeer(message.contact.destination_hash);
      await tau.refresh();
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [],
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
          <div className="flex gap-2">
            <button
              type="button"
              aria-expanded={showNewGroup}
              onClick={() => {
                setShowNewGroup((v) => !v);
                setError(null);
              }}
              className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-semibold text-zinc-200 hover:bg-zinc-900"
            >
              New group
            </button>
            <button
              type="button"
              aria-expanded={showJoin}
              onClick={() => {
                setShowJoin((v) => !v);
                setJoinResult(null);
              }}
              className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-semibold text-zinc-200 hover:bg-zinc-900"
            >
              Join a group
            </button>
            <button
              type="button"
              aria-expanded={showBroadcasts}
              onClick={() => setShowBroadcasts((v) => !v)}
              className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-semibold text-zinc-200 hover:bg-zinc-900"
            >
              Broadcasts
              {broadcasts.broadcasts.length > 0
                ? ` (${broadcasts.broadcasts.length})`
                : ""}
            </button>
            <button
              type="button"
              aria-expanded={showOutbox}
              onClick={() => setShowOutbox((v) => !v)}
              className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-semibold text-zinc-200 hover:bg-zinc-900"
            >
              Outbox{outboxCount > 0 ? ` (${outboxCount})` : ""}
            </button>
          </div>
        </header>
        {showNewGroup && (
          <NewGroupPanel peers={tau.peers} onCreate={handleCreateGroup} />
        )}
        {showJoin && (
          <div className="border-b border-zinc-800 px-3 py-2">
            <label className="text-xs font-semibold text-zinc-400" htmlFor="join-group-link">
              Paste an invite link to join a group
            </label>
            <div className="mt-1 flex gap-2">
              <input
                id="join-group-link"
                value={joinLink}
                onChange={(e) => setJoinLink(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") void handleJoinGroup();
                }}
                placeholder="tau://invite/…"
                className="min-w-0 flex-1 rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-1.5 font-mono text-xs text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
              />
              <button
                type="button"
                disabled={joinBusy || !joinLink.trim()}
                onClick={() => void handleJoinGroup()}
                className="shrink-0 rounded-full bg-cyan-500 px-3 py-1.5 text-xs font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
              >
                Join
              </button>
            </div>
            {joinResult && <p className="mt-1 text-xs text-zinc-400">{joinResult}</p>}
          </div>
        )}
        {showBroadcasts && (
          <BroadcastsPanel
            broadcasts={broadcasts.broadcasts}
            peers={tau.peers}
            loading={broadcasts.loading}
            error={broadcasts.error}
            onCreate={(name, members) => broadcasts.create(name, members)}
            onDelete={(name) => broadcasts.remove(name)}
            onRename={(name, newName) => broadcasts.rename(name, newName)}
            onSend={(name, text) => broadcasts.send(name, text)}
          />
        )}
        {showOutbox && (
          <OutboxPanel
            outbox={outbox.outbox}
            held={outbox.held}
            loading={outbox.loading}
            error={outbox.error}
            onCancel={(target) => void outbox.cancel(target)}
            onEdit={(target, patch) => void outbox.edit(target, patch)}
            onRetry={(target) => void outbox.retry(target)}
            onDropHeld={(target) => void outbox.dropHeld(target)}
          />
        )}
        <MessageSearch
          onOpen={handleOpenSearchResult}
          onActiveChange={setSearchActive}
          resetToken={searchReset}
        />
        <DraftsPanel
          drafts={drafts}
          loading={draftsLoading}
          onOpen={handleOpenDraft}
          onDelete={handleDeleteDraft}
        />
        <RemindersPanel
          reminders={reminders}
          loading={remindersLoading}
          onOpen={handleOpenReminder}
          onCancel={handleCancelReminder}
        />
        {!searchActive && (
          <div className="min-h-0 flex-1">
            <ConversationList
              conversations={conversations}
              archived={archivedConvs}
              ignoredGroups={ignoredGroups}
              dmRequests={dmRequests}
              selectedKey={selectedKey}
              onSelect={handleSelect}
              onSelectIgnoredGroup={(group) => navigate("chat", { group: group.id })}
              onUnignoreGroup={(group) => {
                void run("Un-ignore failed", () => actions.unignoreGroup(group.id)).then(() =>
                  void refreshIgnoredGroups(),
                );
              }}
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
        {editNotice && selected && editNotice.key === selected.key && (
          <p role="status" className="border-b border-zinc-800 bg-zinc-900 px-3 py-1.5 text-xs text-zinc-300">
            {editNotice.text}
            <button type="button" onClick={() => setEditNotice(null)} className="ml-2 underline">
              Dismiss
            </button>
          </p>
        )}
        {retractNotice && selected && retractNotice.key === selected.key && (
          <p role="status" className="border-b border-zinc-800 bg-zinc-900 px-3 py-1.5 text-xs text-zinc-300">
            {retractNotice.text}
            <button type="button" onClick={() => setRetractNotice(null)} className="ml-2 underline">
              Dismiss
            </button>
          </p>
        )}
        {selected ? (
          <>
            <BeaconControl
              target={{ kind: selected.kind, key: selected.key, name: selected.name }}
              beacons={beacons.beacons}
              onRefresh={beacons.refresh}
              onError={(message) => setError(message)}
            />
            {selected.ignored && ignoredHistoryError && (
              <p role="alert" className="border-b border-zinc-800 bg-zinc-900 px-3 py-1.5 text-xs text-zinc-300">
                Couldn&apos;t load this group&apos;s history.
                <button type="button" onClick={() => setIgnoredHistoryRetry((n) => n + 1)} className="ml-2 underline">
                  Retry
                </button>
              </p>
            )}
            <div className="min-h-0 flex-1">
              <ThreadView
                conversation={selected}
                messages={threadMessages}
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
                onSnooze={handleSnooze}
                onVotePoll={handleVotePoll}
                onClosePoll={handleClosePoll}
                onAddPeer={handleAddPeer}
                contactPeers={tau.peers.map((p) => ({ key: p.destination, name: p.name }))}
                onSend={handleSend}
                onSendFile={handleSendFile}
            draftSeed={pendingSeed}
            onSaveDraft={handleSaveDraft}
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
            </div>
          </>
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
