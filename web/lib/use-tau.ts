// Fresh data hook for the rewritten console. It speaks the same bridge API
// as before (GET /api/status snapshot, WebSocket /ws with SSE fallback on
// /api/events, and the /api/* POST actions) but keeps the client logic
// deliberately small: live events merge into state, writes refresh the
// affected list, and a snapshot reload is the backstop for anything missed.
import { useCallback, useEffect, useRef, useState } from "react";

import {
  apiDownload,
  apiGet,
  apiPost,
  apiPostText,
  eventsUrl,
  websocketUrl,
} from "./api";
import type {
  AcceptDmRequestResponse,
  AllowlistResponse,
  ArchivedConversation,
  ArchivedConversationsResponse,
  ArchiveConversationResponse,
  BlockPeerResponse,
  BridgeInfo,
  BundleExportResponse,
  BundleImportResponse,
  CancelOutboxResponse,
  ChannelMessage,
  CreateGroupResponse,
  DeclineDmRequestResponse,
  DeleteGroupResponse,
  DeleteMessageResponse,
  DisappearingDefaults,
  DisappearingResponse,
  DmRequestEntry,
  DmRequestsResponse,
  DoctorResponse,
  DropHeldResponse,
  EditMessageResponse,
  ForwardGroupMessageResponse,
  ForwardMessageResponse,
  GroupMemberResult,
  GroupsResponse,
  HeldMailEntry,
  HeldMailResponse,
  IgnoreGroupResponse,
  JoinRequestEntry,
  JoinRequestsResponse,
  LeaveGroupResponse,
  LeftGroupsResponse,
  MarkReadResponse,
  MarkUnreadResponse,
  MessagesImportResponse,
  MutedConversations,
  MutedConversationsResponse,
  MuteResponse,
  NodeInfo,
  NodeSettings,
  OutboxEntry,
  OutboxResponse,
  Peer,
  PeerInfoResult,
  PingResult,
  PinMessageResponse,
  ReactResponse,
  RejoinGroupResponse,
  RenameGroupResponse,
  RenameResponse,
  RestoredUnread,
  RetractMessageResponse,
  RetryOutboxResponse,
  SafetyNumberResponse,
  SendFileResponse,
  SendGroupFileResponse,
  SendGroupResponse,
  SendResponse,
  SetGroupTopicResponse,
  Snapshot,
  SocialInfo,
  TauGroup,
  TauMessage,
  TypingEntry,
  TypingEvent,
  UnignoreGroupResponse,
  UnpinMessageResponse,
  UnreadCounts,
  UpdateSettingsResponse,
  VerifyPeerResponse,
} from "./types";

export type ConnectionState =
  | "connecting"
  | "live"
  | "reconnecting"
  | "offline";

export const EMPTY_UNREAD: UnreadCounts = { per_peer: {}, per_group: {}, total: 0 };

export interface PingState {
  pending?: boolean;
  rttMs?: number | null;
  error?: string;
}

export interface PeerInfoState {
  pending?: boolean;
  info?: PeerInfoResult["info"];
  error?: string;
}

export interface BlockState {
  pending?: boolean;
  error?: string;
}

export interface VerifyState {
  pending?: boolean;
  safetyNumber?: string;
  result?: VerifyPeerResponse["verify"];
  error?: string;
}

export function fileToBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = reader.result;
      if (typeof result !== "string") {
        reject(new Error("could not read file"));
        return;
      }
      const comma = result.indexOf(",");
      resolve(comma >= 0 ? result.slice(comma + 1) : result);
    };
    reader.onerror = () => reject(new Error("could not read file"));
    reader.readAsDataURL(file);
  });
}

const TYPING_TIMEOUT_MS = 8000;

export function useTau() {
  const [node, setNode] = useState<NodeInfo | null>(null);
  const [peers, setPeers] = useState<Peer[]>([]);
  const [groups, setGroups] = useState<TauGroup[]>([]);
  const [leftGroups, setLeftGroups] = useState<LeftGroupsResponse["left"]>([]);
  const [ignoredGroups, setIgnoredGroups] = useState<
    LeftGroupsResponse["ignored"]
  >([]);
  const [messages, setMessages] = useState<TauMessage[]>([]);
  const [unread, setUnread] = useState<UnreadCounts>(EMPTY_UNREAD);
  const [restoredUnread, setRestoredUnread] = useState<RestoredUnread[] | null>(
    null,
  );
  const [outbox, setOutbox] = useState<OutboxEntry[]>([]);
  const [heldForOthers, setHeldForOthers] = useState(0);
  const [settings, setSettings] = useState<NodeSettings | null>(null);
  const [heldMail, setHeldMail] = useState<HeldMailEntry[]>([]);
  const [dmRequests, setDmRequests] = useState<DmRequestEntry[]>([]);
  const [joinRequests, setJoinRequests] = useState<JoinRequestEntry[]>([]);
  const [archivedConversations, setArchivedConversations] = useState<
    ArchivedConversation[]
  >([]);
  const [disappearingDefaults, setDisappearingDefaults] =
    useState<DisappearingDefaults>({ peers: {}, groups: {} });
  const [mutedConversations, setMutedConversations] =
    useState<MutedConversations>({ peers: [], groups: [] });
  const [typing, setTyping] = useState<Record<string, TypingEntry[]>>({});
  const [channelMessage, setChannelMessage] = useState<ChannelMessage | null>(
    null,
  );
  const [allowlist, setAllowlist] = useState<AllowlistResponse | null>(null);
  const [bridge, setBridge] = useState<BridgeInfo | null>(null);
  const [social, setSocial] = useState<SocialInfo | null>(null);
  const [connection, setConnection] = useState<ConnectionState>("connecting");
  const [pings, setPings] = useState<Record<string, PingState>>({});
  const [infos, setInfos] = useState<Record<string, PeerInfoState>>({});
  const [verifications, setVerifications] = useState<
    Record<string, VerifyState>
  >({});
  const [blocks, setBlocks] = useState<Record<string, BlockState>>({});
  const [announcing, setAnnouncing] = useState(false);
  const [backingUp, setBackingUp] = useState(false);
  const [backupError, setBackupError] = useState<string | null>(null);
  const [exporting, setExporting] = useState(false);
  const [importing, setImporting] = useState(false);

  const unreadRevision = useRef(-1);
  const snapshotMax = useRef(0);

  const applySnapshot = useCallback((snapshot: Snapshot) => {
    setNode(snapshot.node);
    setPeers(snapshot.peers ?? []);
    setGroups(snapshot.groups ?? []);
    setMessages(snapshot.messages ?? []);
    setUnread(snapshot.unread ?? EMPTY_UNREAD);
    setBridge(snapshot.bridge ?? null);
    setSocial(snapshot.social ?? null);
    const max = (snapshot.messages ?? []).reduce(
      (top, entry) => Math.max(top, entry.id),
      0,
    );
    snapshotMax.current = Math.max(snapshotMax.current, max);
  }, []);

  const applyUnread = useCallback((next: UnreadCounts) => {
    setUnread((previous) => {
      const nextRev = next.revision ?? -1;
      if (nextRev >= 0 && nextRev < unreadRevision.current) return previous;
      if (nextRev >= 0) unreadRevision.current = nextRev;
      return next;
    });
  }, []);

  const refreshSnapshot = useCallback(async () => {
    const snapshot = await apiGet<Snapshot>("/api/status");
    applySnapshot(snapshot);
    return snapshot;
  }, [applySnapshot]);

  const refreshOutbox = useCallback(async () => {
    const result = await apiGet<OutboxResponse>("/api/outbox");
    setOutbox(result.outbox ?? []);
    setHeldForOthers(result.held_for_others ?? 0);
  }, []);

  const refreshGroups = useCallback(async () => {
    const result = await apiGet<GroupsResponse>("/api/groups");
    setGroups(result.groups ?? []);
  }, []);

  const refreshMessages = useCallback(async () => {
    const result = await apiGet<{ messages: TauMessage[] }>("/api/messages");
    setMessages(result.messages ?? []);
  }, []);

  const refreshRequests = useCallback(async () => {
    const [dm, join, held, left] = await Promise.all([
      apiGet<DmRequestsResponse>("/api/messages/requests").catch(() => null),
      apiGet<JoinRequestsResponse>("/api/groups/join-requests").catch(() => null),
      apiGet<HeldMailResponse>("/api/outbox/held").catch(() => null),
      apiGet<LeftGroupsResponse>("/api/groups/left").catch(() => null),
    ]);
    if (dm) setDmRequests(dm.requests ?? []);
    if (join) setJoinRequests(join.requests ?? []);
    if (held) setHeldMail(held.held ?? []);
    if (left) {
      setLeftGroups(left.left ?? []);
      setIgnoredGroups(left.ignored ?? []);
    }
  }, []);

  const refreshSettings = useCallback(async () => {
    const result = await apiGet<{ settings: NodeSettings }>("/api/settings");
    setSettings(result.settings);
  }, []);

  const refreshExtras = useCallback(async () => {
    const [archived, disappearing, muted, allow] = await Promise.all([
      apiGet<ArchivedConversationsResponse>("/api/conversations/archived").catch(
        () => null,
      ),
      apiGet<DisappearingDefaults>("/api/messages/disappearing").catch(() => null),
      apiGet<MutedConversationsResponse>("/api/messages/muted").catch(() => null),
      apiGet<AllowlistResponse>("/api/allowlist").catch(() => null),
    ]);
    if (archived) setArchivedConversations(archived.archived ?? []);
    if (disappearing) setDisappearingDefaults(disappearing);
    if (muted) setMutedConversations(muted);
    if (allow) setAllowlist(allow);
  }, []);

  // Initial load.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        await refreshSnapshot();
        if (!cancelled) setConnection("live");
      } catch {
        if (!cancelled) setConnection("offline");
      }
      try {
        await Promise.allSettled([
          refreshOutbox(),
          refreshSettings(),
          refreshRequests(),
          refreshExtras(),
        ]);
      } catch {
        // Best effort; the live stream and polls reconcile.
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [refreshSnapshot, refreshOutbox, refreshSettings, refreshRequests, refreshExtras]);

  // Live stream: WebSocket first, SSE fallback. Both speak the same
  // events: a frame carries the event itself (`type` plus payload), an
  // SSE event carries the payload under its type name.
  useEffect(() => {
    let disposed = false;
    const onSnapshot = (payload: { snapshot: Snapshot } | null) => {
      if (payload?.snapshot) applySnapshot(payload.snapshot);
    };
    const onMessage = (payload: { message: TauMessage } | null) => {
      const entry = payload?.message;
      if (!entry) return;
      // The snapshot is authoritative up to its highest id; an event
      // at or below that id is already in state. Anything newer is
      // appended (the snapshot refresh on reconnect covers the gap
      // when an event races a snapshot load).
      if (entry.id <= snapshotMax.current) return;
      setMessages((previous) => {
        if (previous.some((known) => known.id === entry.id)) return previous;
        return [...previous, entry].sort((a, b) => a.id - b.id);
      });
    };
    const onMessageUpdated = (payload: { message: TauMessage } | null) => {
      const entry = payload?.message;
      if (!entry) return;
      setMessages((previous) =>
        previous.map((known) => (known.id === entry.id ? entry : known)),
      );
    };
    const onMessageDeleted = (payload: { message_id: number } | null) => {
      if (payload?.message_id === undefined) return;
      setMessages((previous) =>
        previous.filter((entry) => entry.id !== payload.message_id),
      );
    };
    const onPeer = (payload: { peer: Peer } | null) => {
      const peer = payload?.peer;
      if (!peer) return;
      setPeers((previous) => {
        const known = previous.some(
          (entry) => entry.destination === peer.destination,
        );
        if (!known) return [...previous, peer];
        return previous.map((entry) =>
          entry.destination === peer.destination ? peer : entry,
        );
      });
    };
    const onUnread = (payload: { unread: UnreadCounts } | null) => {
      if (payload?.unread) applyUnread(payload.unread);
    };
    const onStatus = () => {
      void refreshSnapshot().catch(() => undefined);
    };
    const onTyping = (payload: TypingEvent | null) => {
      if (!payload) return;
      const key = payload.group ? `group:${payload.group}` : payload.peer;
      setTyping((previous) => {
        const current = (previous[key] ?? []).filter(
          (entry) => entry.peer !== payload.peer,
        );
        if (payload.state === "stop") {
          if (current.length === 0) {
            const next = { ...previous };
            delete next[key];
            return next;
          }
          return { ...previous, [key]: current };
        }
        return {
          ...previous,
          [key]: [
            ...current,
            {
              peer: payload.peer,
              name: payload.name,
              receivedAt: payload.received_at,
            },
          ],
        };
      });
    };
    const onChannelMessage = (payload: { message?: ChannelMessage } | null) => {
      if (payload?.message) setChannelMessage(payload.message);
    };

    const dispatch = (type: string, payload: unknown) => {
      switch (type) {
        case "snapshot":
          onSnapshot(payload as { snapshot: Snapshot } | null);
          break;
        case "message":
          onMessage(payload as { message: TauMessage } | null);
          break;
        case "message_updated":
          onMessageUpdated(payload as { message: TauMessage } | null);
          break;
        case "message_deleted":
          onMessageDeleted(payload as { message_id: number } | null);
          break;
        case "peer":
          onPeer(payload as { peer: Peer } | null);
          break;
        case "unread":
          onUnread(payload as { unread: UnreadCounts } | null);
          break;
        case "status":
          onStatus();
          break;
        case "typing":
          onTyping(payload as TypingEvent | null);
          break;
        case "channel-message":
          onChannelMessage(payload as { message?: ChannelMessage } | null);
          break;
        default:
          break;
      }
    };

    const readEventData = (event: Event) => {
      const target = event.target as EventSource | null;
      const data = (event as MessageEvent).data;
      if (typeof data === "string" && data) {
        try {
          return JSON.parse(data) as unknown;
        } catch {
          return null;
        }
      }
      void target;
      return null;
    };

    const onOpen = () => {
      if (!disposed) setConnection("live");
      void refreshSnapshot().catch(() => undefined);
    };
    const onError = () => {
      if (!disposed) setConnection("reconnecting");
    };

    const connectEventSource = () => {
      const source = new EventSource(eventsUrl(0));
      const types = [
        "snapshot",
        "message",
        "message_updated",
        "message_deleted",
        "peer",
        "unread",
        "status",
        "typing",
        "channel-message",
      ];
      const listeners = types.map((type) => {
        const listener = (event: Event) =>
          dispatch(type, readEventData(event));
        source.addEventListener(type, listener);
        return [type, listener] as const;
      });
      source.addEventListener("open", onOpen);
      source.addEventListener("error", onError);
      return () => {
        source.removeEventListener("open", onOpen);
        for (const [type, listener] of listeners) {
          source.removeEventListener(type, listener);
        }
        source.removeEventListener("error", onError);
        source.close();
      };
    };

    let cleanupTransport: (() => void) | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let backoffMs = 1000;
    let everOpened = false;

    const scheduleReconnect = () => {
      setConnection("reconnecting");
      const delay = backoffMs;
      backoffMs = Math.min(backoffMs * 2, 30000);
      reconnectTimer = setTimeout(connectWebSocket, delay);
    };

    const connectWebSocket = () => {
      if (disposed) return;
      let socket: WebSocket;
      try {
        socket = new WebSocket(websocketUrl());
      } catch {
        if (!everOpened) {
          cleanupTransport = connectEventSource();
        } else {
          scheduleReconnect();
        }
        return;
      }
      let opened = false;
      const onSocketOpen = () => {
        opened = true;
        everOpened = true;
        backoffMs = 1000;
        onOpen();
      };
      const onSocketMessage = (event: MessageEvent) => {
        let data: unknown = null;
        try {
          data = JSON.parse(event.data) as unknown;
        } catch {
          return;
        }
        if (
          typeof data !== "object" ||
          data === null ||
          typeof (data as { type?: unknown }).type !== "string"
        ) {
          return;
        }
        dispatch((data as { type: string }).type, data);
      };
      const onSocketClose = () => {
        if (disposed) return;
        if (!opened && !everOpened) {
          cleanupTransport = connectEventSource();
          return;
        }
        scheduleReconnect();
      };
      socket.addEventListener("open", onSocketOpen);
      socket.addEventListener("message", onSocketMessage);
      socket.addEventListener("close", onSocketClose);
      cleanupTransport = () => {
        socket.removeEventListener("open", onSocketOpen);
        socket.removeEventListener("message", onSocketMessage);
        socket.removeEventListener("close", onSocketClose);
        socket.close();
      };
    };

    if (typeof WebSocket === "undefined") {
      cleanupTransport = connectEventSource();
    } else {
      connectWebSocket();
    }

    return () => {
      disposed = true;
      if (reconnectTimer !== null) clearTimeout(reconnectTimer);
      cleanupTransport?.();
    };
  }, [applySnapshot, applyUnread, refreshSnapshot]);

  // Typing entries expire: a sender that never sends `stop` clears
  // after a few seconds.
  useEffect(() => {
    const timer = setInterval(() => {
      const now = Date.now() / 1000;
      setTyping((previous) => {
        let changed = false;
        const next: Record<string, TypingEntry[]> = {};
        for (const [key, entries] of Object.entries(previous)) {
          const kept = entries.filter(
            (entry) => now - entry.receivedAt < TYPING_TIMEOUT_MS / 1000,
          );
          if (kept.length !== entries.length) changed = true;
          if (kept.length > 0) next[key] = kept;
        }
        return changed ? next : previous;
      });
    }, 2000);
    return () => clearInterval(timer);
  }, []);

  // Slow poll for things the live stream does not push: the outbox,
  // groups, and the request queues.
  useEffect(() => {
    const timer = setInterval(() => {
      void Promise.allSettled([
        refreshOutbox(),
        refreshGroups(),
        refreshRequests(),
      ]);
    }, 10000);
    return () => clearInterval(timer);
  }, [refreshOutbox, refreshGroups, refreshRequests]);

  // Unread total on the tab title, like a mail client.
  useEffect(() => {
    document.title =
      unread.total > 0
        ? `(${unread.total}) tau · mesh console`
        : "tau · mesh console";
  }, [unread.total]);

  // ---- Writes: every action posts, then refreshes what it changed. ----

  const send = useCallback(
    async (
      to: string,
      text: string,
      expiresIn?: number,
      replyTo?: string,
      sendIn?: number,
    ) => {
      const body: {
        to: string;
        text: string;
        expires_in?: number;
        reply_to?: string;
        send_in?: number;
      } = { to, text };
      if (expiresIn !== undefined) body.expires_in = expiresIn;
      if (replyTo) body.reply_to = replyTo;
      if (sendIn !== undefined) body.send_in = sendIn;
      const result = await apiPost<SendResponse>("/api/messages", body);
      if (result.message !== null) {
        setMessages((previous) => {
          if (previous.some((entry) => entry.id === result.message!.id)) {
            return previous;
          }
          return [...previous, result.message!].sort((a, b) => a.id - b.id);
        });
        snapshotMax.current = Math.max(
          snapshotMax.current,
          result.message.id,
        );
      }
      try {
        await refreshOutbox();
      } catch {
        // The next poll picks the entry up.
      }
      return result.message;
    },
    [refreshOutbox],
  );

  const sendGroup = useCallback(
    async (
      group: string,
      text: string,
      expiresIn?: number,
      replyTo?: string,
      sendIn?: number,
      pollOptions?: string[],
    ) => {
      const body: {
        group: string;
        text: string;
        expires_in?: number;
        reply_to?: string;
        send_in?: number;
        poll_options?: string[];
      } = { group, text };
      if (expiresIn !== undefined) body.expires_in = expiresIn;
      if (replyTo) body.reply_to = replyTo;
      if (sendIn !== undefined) body.send_in = sendIn;
      if (pollOptions !== undefined) body.poll_options = pollOptions;
      const result = await apiPost<SendGroupResponse>("/api/messages", body);
      setMessages((previous) => {
        const fresh = result.messages.filter(
          (entry) => !previous.some((known) => known.id === entry.id),
        );
        if (fresh.length === 0) return previous;
        return [...previous, ...fresh].sort((a, b) => a.id - b.id);
      });
      try {
        await refreshOutbox();
      } catch {
        // The next poll picks the entry up.
      }
      return result.messages;
    },
    [refreshOutbox],
  );

  const sendFile = useCallback(async (to: string, file: File) => {
    const data = await fileToBase64(file);
    const result = await apiPost<SendFileResponse>("/api/send-file", {
      to,
      name: file.name,
      data,
    });
    if (result.message !== null) {
      setMessages((previous) => {
        if (previous.some((entry) => entry.id === result.message!.id)) {
          return previous;
        }
        return [...previous, result.message!].sort((a, b) => a.id - b.id);
      });
      snapshotMax.current = Math.max(snapshotMax.current, result.message.id);
    }
    return result.message;
  }, []);

  const sendGroupFile = useCallback(async (group: string, file: File) => {
    const data = await fileToBase64(file);
    const result = await apiPost<SendGroupFileResponse>(
      "/api/groups/send-file",
      { group, name: file.name, data },
    );
    setMessages((previous) => {
      const fresh = result.messages.filter(
        (entry) => !previous.some((known) => known.id === entry.id),
      );
      if (fresh.length === 0) return previous;
      return [...previous, ...fresh].sort((a, b) => a.id - b.id);
    });
    if (!result.ok) {
      throw new Error("some group members did not receive the file");
    }
    return result.messages;
  }, []);

  const sendTyping = useCallback(
    async (
      target: { peer: string } | { group: string },
      state: "start" | "stop",
    ) => {
      return apiPost<{ ok: boolean }>(
        "/api/typing",
        { ...target, state },
        AbortSignal.timeout(5000),
      ).catch(() => ({ ok: false }));
    },
    [],
  );

  const react = useCallback(async (target: string, reaction: string) => {
    const result = await apiPost<ReactResponse>("/api/react", {
      target,
      reaction,
    });
    await refreshMessages().catch(() => undefined);
    return result;
  }, [refreshMessages]);

  const clearReaction = useCallback(async (target: string) => {
    const result = await apiPost<ReactResponse>("/api/react", {
      target,
      clear: true,
    });
    await refreshMessages().catch(() => undefined);
    return result;
  }, [refreshMessages]);

  const editMessage = useCallback(
    async (id: number, text: string) => {
      const result = await apiPost<EditMessageResponse>("/api/messages/edit", {
        id,
        text,
      });
      if (result.message) {
        setMessages((previous) =>
          previous.map((entry) =>
            entry.id === result.message!.id ? result.message! : entry,
          ),
        );
      }
      if (result.messages) {
        setMessages((previous) => {
          const byId = new Map(result.messages!.map((entry) => [entry.id, entry]));
          return previous.map((entry) => byId.get(entry.id) ?? entry);
        });
      }
      return result;
    },
    [],
  );

  const retractMessage = useCallback(async (id: number) => {
    const result = await apiPost<RetractMessageResponse>(
      "/api/messages/retract",
      { id },
    );
    const removed = new Set<number>([result.retracted.id]);
    for (const extra of result.retracted_ids ?? []) removed.add(extra);
    setMessages((previous) =>
      previous.filter((entry) => !removed.has(entry.id)),
    );
    return result;
  }, []);

  const deleteMessage = useCallback(async (id: number) => {
    const result = await apiPost<DeleteMessageResponse>(
      "/api/messages/delete",
      { id },
    );
    setMessages((previous) =>
      previous.filter((entry) => entry.id !== result.deleted.id),
    );
    return result;
  }, []);

  const forwardMessage = useCallback(
    async (id: number, to: string, group?: string) => {
      const result = group
        ? await apiPost<ForwardGroupMessageResponse>(
            "/api/messages/forward",
            { id, group },
          )
        : await apiPost<ForwardMessageResponse>("/api/messages/forward", {
            id,
            to,
          });
      await refreshMessages().catch(() => undefined);
      return result;
    },
    [refreshMessages],
  );

  const pinMessage = useCallback(async (id: number) => {
    const result = await apiPost<PinMessageResponse>("/api/messages/pin", {
      id,
    });
    await refreshMessages().catch(() => undefined);
    return result;
  }, [refreshMessages]);

  const unpinMessage = useCallback(async (id: number) => {
    const result = await apiPost<UnpinMessageResponse>("/api/messages/unpin", {
      id,
    });
    await refreshMessages().catch(() => undefined);
    return result;
  }, [refreshMessages]);

  const markRead = useCallback(
    async (peer?: string, group?: string) => {
      const result = await apiPost<MarkReadResponse>(
        "/api/messages/read",
        group ? { group } : peer ? { peer } : {},
      );
      applyUnread(result.unread);
      return result.unread;
    },
    [applyUnread],
  );

  const markUnread = useCallback(
    async (peer: string, all = false, group?: string) => {
      const result = await apiPost<MarkUnreadResponse>(
        "/api/messages/unread",
        group ? { group, all } : { peer, all },
      );
      applyUnread(result.unread);
      return result.unread;
    },
    [applyUnread],
  );

  const voteGroupPoll = useCallback(
    async (group: string, poll: string, option: number) => {
      const result = await apiPost<SendGroupResponse>("/api/groups/vote", {
        group,
        poll,
        option: option + 1,
      });
      await refreshMessages().catch(() => undefined);
      return result;
    },
    [refreshMessages],
  );

  const closeGroupPoll = useCallback(
    async (group: string, poll: string) => {
      const result = await apiPost<SendGroupResponse>("/api/groups/close-poll", {
        group,
        poll,
      });
      await refreshMessages().catch(() => undefined);
      return result;
    },
    [refreshMessages],
  );

  const pingPeer = useCallback(async (destination: string) => {
    setPings((previous) => ({
      ...previous,
      [destination]: { pending: true },
    }));
    try {
      const result = await apiPost<PingResult>("/api/ping", {
        peer: destination,
      });
      setPings((previous) => ({
        ...previous,
        [destination]: { rttMs: result.rtt_ms ?? null },
      }));
      return result;
    } catch (error) {
      setPings((previous) => ({
        ...previous,
        [destination]: {
          error: error instanceof Error ? error.message : "ping failed",
        },
      }));
      throw error;
    }
  }, []);

  const fetchPeerInfo = useCallback(async (destination: string) => {
    setInfos((previous) => ({
      ...previous,
      [destination]: { pending: true },
    }));
    try {
      const result = await apiPost<PeerInfoResult>("/api/info", {
        peer: destination,
      });
      setInfos((previous) => ({
        ...previous,
        [destination]: { info: result.info },
      }));
      return result;
    } catch (error) {
      setInfos((previous) => ({
        ...previous,
        [destination]: {
          error: error instanceof Error ? error.message : "info failed",
        },
      }));
      throw error;
    }
  }, []);

  const verifyPeer = useCallback(async (destination: string) => {
    setVerifications((previous) => ({
      ...previous,
      [destination]: { pending: true },
    }));
    try {
      const result = await apiPost<VerifyPeerResponse>("/api/peers/verify", {
        peer: destination,
      });
      const verify = result.verify;
      setVerifications((previous) => ({
        ...previous,
        [destination]: {
          safetyNumber: verify.safety_number,
          result: verify,
        },
      }));
      return verify;
    } catch (error) {
      setVerifications((previous) => ({
        ...previous,
        [destination]: {
          error: error instanceof Error ? error.message : "verify failed",
        },
      }));
      throw error;
    }
  }, []);

  const confirmVerified = useCallback(
    async (destination: string) => {
      const result = await apiPost<VerifyPeerResponse>("/api/peers/verify", {
        peer: destination,
        confirm: true,
      });
      setVerifications((previous) => ({
        ...previous,
        [destination]: { result: result.verify },
      }));
      await refreshSnapshot().catch(() => undefined);
      return result.verify;
    },
    [refreshSnapshot],
  );

  const blockPeer = useCallback(
    async (destination: string, blocked: boolean) => {
      setBlocks((previous) => ({
        ...previous,
        [destination]: { pending: true },
      }));
      try {
        const result = await apiPost<BlockPeerResponse>(
          blocked ? "/api/peers/block" : "/api/peers/unblock",
          { peer: destination },
        );
        setBlocks((previous) => {
          const next = { ...previous };
          delete next[destination];
          return next;
        });
        await refreshSnapshot().catch(() => undefined);
        return result;
      } catch (error) {
        setBlocks((previous) => ({
          ...previous,
          [destination]: {
            error: error instanceof Error ? error.message : "failed",
          },
        }));
        throw error;
      }
    },
    [refreshSnapshot],
  );

  const forgetPeer = useCallback(
    async (destination: string) => {
      const result = await apiPost<{ ok: boolean; peer: Peer }>(
        "/api/peers/forget",
        { peer: destination },
      );
      await refreshSnapshot().catch(() => undefined);
      return result;
    },
    [refreshSnapshot],
  );

  const setAlias = useCallback(
    async (destination: string, alias: string) => {
      const result = await apiPost<{ ok: boolean; peer: Peer }>(
        "/api/peers/alias",
        { peer: destination, alias },
      );
      await refreshSnapshot().catch(() => undefined);
      return result;
    },
    [refreshSnapshot],
  );

  const setNote = useCallback(
    async (destination: string, note: string) => {
      const result = await apiPost<{ ok: boolean; peer: Peer }>(
        "/api/peers/note",
        { peer: destination, note },
      );
      await refreshSnapshot().catch(() => undefined);
      return result;
    },
    [refreshSnapshot],
  );

  const announce = useCallback(async () => {
    setAnnouncing(true);
    try {
      await apiPost<{ ok: boolean }>("/api/announce", {});
    } finally {
      setAnnouncing(false);
    }
  }, []);

  const renameNode = useCallback(async (name: string) => {
    const result = await apiPost<RenameResponse>("/api/id/rename", { name });
    await refreshSnapshot().catch(() => undefined);
    return result;
  }, [refreshSnapshot]);

  const updateSettings = useCallback(async (settings: Partial<NodeSettings>) => {
    const result = await apiPost<UpdateSettingsResponse>("/api/settings", {
      ...settings,
    });
    setSettings(result.settings);
    return result;
  }, []);

  const createGroup = useCallback(
    async (name: string, members: string[]) => {
      const result = await apiPost<CreateGroupResponse>("/api/groups", {
        name,
        members,
      });
      await refreshGroups().catch(() => undefined);
      return result;
    },
    [refreshGroups],
  );

  const deleteGroup = useCallback(
    async (id: string) => {
      const result = await apiPost<DeleteGroupResponse>("/api/groups/delete", {
        id,
      });
      await Promise.allSettled([refreshGroups(), refreshRequests()]);
      return result;
    },
    [refreshGroups, refreshRequests],
  );

  const leaveGroup = useCallback(
    async (id: string) => {
      const result = await apiPost<LeaveGroupResponse>("/api/groups/leave", {
        id,
      });
      await Promise.allSettled([refreshGroups(), refreshRequests()]);
      return result;
    },
    [refreshGroups, refreshRequests],
  );

  const rejoinGroup = useCallback(
    async (id: string) => {
      const result = await apiPost<RejoinGroupResponse>("/api/groups/rejoin", {
        id,
      });
      await Promise.allSettled([refreshGroups(), refreshRequests()]);
      return result;
    },
    [refreshGroups, refreshRequests],
  );

  const ignoreGroup = useCallback(
    async (id: string, ignored: boolean) => {
      const result = await apiPost<IgnoreGroupResponse | UnignoreGroupResponse>(
        ignored ? "/api/groups/ignore" : "/api/groups/unignore",
        { id },
      );
      await Promise.allSettled([refreshGroups(), refreshRequests()]);
      return result;
    },
    [refreshGroups, refreshRequests],
  );

  const changeGroupMembers = useCallback(
    async (id: string, members: string[]) => {
      const result = await apiPost<{
        ok: boolean;
        group: TauGroup;
        previous_id: string;
      }>("/api/groups/add-member", { id, members });
      await refreshGroups().catch(() => undefined);
      return result;
    },
    [refreshGroups],
  );

  const renameGroup = useCallback(
    async (id: string, name: string) => {
      const result = await apiPost<RenameGroupResponse>("/api/groups/rename", {
        id,
        name,
      });
      await refreshGroups().catch(() => undefined);
      return result;
    },
    [refreshGroups],
  );

  const setGroupTopic = useCallback(
    async (id: string, topic: string) => {
      const result = await apiPost<SetGroupTopicResponse>("/api/groups/topic", {
        id,
        topic,
      });
      await refreshGroups().catch(() => undefined);
      return result;
    },
    [refreshGroups],
  );

  const retryOutbox = useCallback(async () => {
    const result = await apiPost<RetryOutboxResponse>("/api/outbox/retry", {});
    await refreshOutbox().catch(() => undefined);
    return result;
  }, [refreshOutbox]);

  const cancelOutbox = useCallback(
    async (id: string) => {
      const result = await apiPost<CancelOutboxResponse>("/api/outbox/cancel", {
        id,
      });
      await refreshOutbox().catch(() => undefined);
      return result;
    },
    [refreshOutbox],
  );

  const dropHeld = useCallback(
    async (id: string) => {
      const result = await apiPost<DropHeldResponse>("/api/outbox/drop-held", {
        id,
      });
      await Promise.allSettled([refreshOutbox(), refreshRequests()]);
      return result;
    },
    [refreshOutbox, refreshRequests],
  );

  const acceptDmRequest = useCallback(
    async (id: string) => {
      const result = await apiPost<AcceptDmRequestResponse>(
        "/api/messages/requests/accept",
        { id },
      );
      await Promise.allSettled([refreshRequests(), refreshSnapshot()]);
      return result;
    },
    [refreshRequests, refreshSnapshot],
  );

  const declineDmRequest = useCallback(
    async (id: string, block = false) => {
      const result = await apiPost<DeclineDmRequestResponse>(
        "/api/messages/requests/decline",
        { id, block },
      );
      await refreshRequests().catch(() => undefined);
      return result;
    },
    [refreshRequests],
  );

  const decideJoinRequest = useCallback(
    async (group: string, peer: string, approve: boolean) => {
      const result = await apiPost<{
        ok: boolean;
        status: string;
        group: { id: string; name: string; members: string[]; creator: string };
      }>(approve ? "/api/groups/approve-join" : "/api/groups/deny-join", {
        group,
        peer,
      });
      await Promise.allSettled([refreshRequests(), refreshGroups()]);
      return result;
    },
    [refreshRequests, refreshGroups],
  );

  const archiveConversation = useCallback(
    async (destination: string, archived: boolean) => {
      const result = await apiPost<ArchiveConversationResponse>(
        archived
          ? "/api/conversations/archive"
          : "/api/conversations/unarchive",
        { destination },
      );
      await refreshExtras().catch(() => undefined);
      return result;
    },
    [refreshExtras],
  );

  const setDisappearing = useCallback(
    async (target: string, group: boolean, seconds: number | null) => {
      const result = await apiPost<DisappearingResponse>(
        "/api/messages/disappearing",
        { target, group, seconds },
      );
      await refreshExtras().catch(() => undefined);
      return result;
    },
    [refreshExtras],
  );

  const muteConversation = useCallback(
    async (target: string, group: boolean, muted: boolean) => {
      const result = await apiPost<MuteResponse>("/api/messages/mute", {
        target,
        group,
        muted,
      });
      if (result.mute.unread) applyUnread(result.mute.unread);
      await refreshExtras().catch(() => undefined);
      return result;
    },
    [applyUnread, refreshExtras],
  );

  const allowlistAdd = useCallback(
    async (target: string) => {
      const result = await apiPost<AllowlistResponse>("/api/allowlist/add", {
        target,
      });
      setAllowlist(result);
      return result;
    },
    [],
  );

  const allowlistRemove = useCallback(async (target: string) => {
    const result = await apiPost<AllowlistResponse>("/api/allowlist/remove", {
      target,
    });
    setAllowlist(result);
    return result;
  }, []);

  const allowlistSetEnabled = useCallback(async (enabled: boolean) => {
    const result = await apiPost<AllowlistResponse>(
      "/api/allowlist/set-enabled",
      { enabled },
    );
    setAllowlist(result);
    return result;
  }, []);

  const exportBundle = useCallback(async (includeTan: boolean) => {
    setExporting(true);
    try {
      const result = await apiPost<BundleExportResponse>("/api/bundle/export", {
        tan: includeTan,
      });
      return result;
    } finally {
      setExporting(false);
    }
  }, []);

  const importBundle = useCallback(async (bundle: string) => {
    setImporting(true);
    try {
      const result = await apiPostText<BundleImportResponse>(
        "/api/bundle/import",
        bundle,
      );
      await refreshSnapshot().catch(() => undefined);
      return result;
    } finally {
      setImporting(false);
    }
  }, [refreshSnapshot]);

  const downloadBackup = useCallback(async () => {
    setBackingUp(true);
    setBackupError(null);
    try {
      const { blob, filename } = await apiDownload("/api/backup");
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = filename;
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (error) {
      setBackupError(error instanceof Error ? error.message : "backup failed");
      throw error;
    } finally {
      setBackingUp(false);
    }
  }, []);

  const runDoctor = useCallback(async () => {
    return apiGet<DoctorResponse>("/api/doctor");
  }, []);

  const importMessages = useCallback(
    async (document: string) => {
      const result = await apiPostText<MessagesImportResponse>(
        "/api/messages/import",
        document,
      );
      await refreshSnapshot().catch(() => undefined);
      return result;
    },
    [refreshSnapshot],
  );

  return {
    node,
    peers,
    groups,
    leftGroups,
    ignoredGroups,
    messages,
    unread,
    restoredUnread,
    outbox,
    heldForOthers,
    settings,
    heldMail,
    dmRequests,
    joinRequests,
    archivedConversations,
    disappearingDefaults,
    mutedConversations,
    typing,
    channelMessage,
    allowlist,
    bridge,
    social,
    connection,
    pings,
    infos,
    verifications,
    blocks,
    announcing,
    backingUp,
    backupError,
    exporting,
    importing,
    refreshSnapshot,
    refreshOutbox,
    refreshGroups,
    refreshMessages,
    refreshRequests,
    refreshSettings,
    refreshExtras,
    send,
    sendGroup,
    sendFile,
    sendGroupFile,
    sendTyping,
    react,
    clearReaction,
    editMessage,
    retractMessage,
    deleteMessage,
    forwardMessage,
    pinMessage,
    unpinMessage,
    markRead,
    markUnread,
    voteGroupPoll,
    closeGroupPoll,
    pingPeer,
    fetchPeerInfo,
    verifyPeer,
    confirmVerified,
    blockPeer,
    forgetPeer,
    setAlias,
    setNote,
    announce,
    renameNode,
    updateSettings,
    createGroup,
    deleteGroup,
    leaveGroup,
    rejoinGroup,
    ignoreGroup,
    changeGroupMembers,
    renameGroup,
    setGroupTopic,
    retryOutbox,
    cancelOutbox,
    dropHeld,
    acceptDmRequest,
    declineDmRequest,
    decideJoinRequest,
    archiveConversation,
    setDisappearing,
    muteConversation,
    allowlistAdd,
    allowlistRemove,
    allowlistSetEnabled,
    exportBundle,
    importBundle,
    downloadBackup,
    runDoctor,
    importMessages,
  };
}

export type UseTauResult = ReturnType<typeof useTau>;
