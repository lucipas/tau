// use-tau.ts — live node state for the chat side of the console.
//
// Provides:
//   useTau()          — live snapshot via /ws, with REST polling fallback.
//                       Peers, groups, messages, unread counts, node info,
//                       typing indicators, connection state, refresh().
//   useConversations()— DM + group conversation list derived from the snapshot.
//   useOutbox()       — queued mail (store-and-forward outbox + held mail).
//   useBeacons()      — live beacons this node is sharing.
//   useBeaconFeed()   — keeps active beacons fed with the browser's position
//                       (geolocation watch, throttled); mount once, at the
//                       shell, so sharing survives leaving the chat tab.
//   useBroadcasts()   — named peer sets for one-to-many DMs.
//   useTraffic()      — per-peer traffic summary derived from message history.
//                       (There is NO /api/traffic endpoint: the traffic view
//                       is built from the same stored history /api/messages
//                       returns, exactly as the old console did.)
//   usePeerActions()  — ping / info / verify / safety-number / block /
//                       forget / alias / note / archive / mute / pin /
//                       ignore-group actions.
//
// Live protocol (tau/webapi.py _serve_websocket): the first /ws frame is
// `{ "type": "snapshot", "snapshot": {...} }`; later frames are bus events:
// `message`, `message_updated`, `message_deleted`, `peer`, `unread`,
// `typing`, `post`, `channel-message`, `status`. When the socket cannot
// connect (or drops), the hook falls back to polling GET /api/status and
// retries the socket on the next refresh.

"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  archiveConversation,
  blockPeer,
  cancelOutbox,
  createBroadcast,
  deleteBroadcast,
  dropHeldMail,
  editOutbox,
  forgetPeer,
  getBeacons,
  getBroadcasts,
  getDmRequests,
  getHeldMail,
  getLeftGroups,
  getOutbox,
  getPeerInfo,
  getSafetyNumber,
  getStatus,
  ignoreGroup,
  markRead,
  markUnread,
  pingPeer,
  renameBroadcast,
  retryOutbox,
  sendBroadcast,
  setMuted,
  setPeerAlias,
  setPeerNote,
  setPinnedConversation,
  unarchiveConversation,
  unblockPeer,
  unignoreGroup,
  updateBeaconPosition,
  verifyPeer,
  wsUrl,
} from "./api";
import type {
  Beacon,
  BroadcastList,
  BroadcastSendResponse,
  ChatMessage,
  DmRequest,
  Group,
  HeldEnvelope,
  LiveEvent,
  Outbox,
  OutboxEntry,
  Peer,
  Snapshot,
  TypingState,
  UnreadCounts,
} from "./types";

const REST_POLL_MS = 5000;
const WS_RETRY_MS = 5000;
const TYPING_TTL_MS = 8000;

function upsertMessage(list: ChatMessage[], incoming: ChatMessage): ChatMessage[] {
  const idx = list.findIndex((m) => m.id === incoming.id);
  if (idx >= 0) {
    const next = list.slice();
    next[idx] = incoming;
    return next;
  }
  return [...list, incoming];
}

function applyEvent(
  snapshot: Snapshot,
  event: LiveEvent,
): Snapshot | null {
  // The catch-all LiveEvent member carries index-signature payloads, so
  // narrow each event by hand instead of relying on union narrowing.
  switch (event.type) {
    case "snapshot":
      return (event as { snapshot: Snapshot }).snapshot;
    case "message": {
      const ev = event as { message: ChatMessage; unread?: UnreadCounts };
      return {
        ...snapshot,
        messages: upsertMessage(snapshot.messages, ev.message),
        unread: ev.unread ?? snapshot.unread,
      };
    }
    case "message_updated": {
      const ev = event as { message: ChatMessage };
      return { ...snapshot, messages: upsertMessage(snapshot.messages, ev.message) };
    }
    case "message_deleted": {
      const ev = event as { message_id: number };
      return {
        ...snapshot,
        messages: snapshot.messages.filter((m) => m.id !== ev.message_id),
      };
    }
    case "peer": {
      const peer = (event as { peer: Peer }).peer;
      const idx = snapshot.peers.findIndex((p) => p.destination === peer.destination);
      const peers =
        idx >= 0
          ? snapshot.peers.map((p, i) => (i === idx ? peer : p))
          : [...snapshot.peers, peer];
      return { ...snapshot, peers };
    }
    case "groups": {
      // The bridge publishes the full group list when ignore state
      // changes, so the snapshot's ignored flags never lag the
      // ignore file (#784). Replacing the list also re-triggers the
      // conversation list's /api/groups/left re-read.
      const groups = (event as { groups: Group[] }).groups;
      return { ...snapshot, groups };
    }
    case "unread": {
      const unread = (event as { unread?: UnreadCounts }).unread;
      return unread ? { ...snapshot, unread } : null;
    }
    default:
      return null;
  }
}

export interface UseTauResult {
  snapshot: Snapshot | null;
  node: Snapshot["node"] | null;
  peers: Peer[];
  groups: Group[];
  messages: ChatMessage[];
  unread: UnreadCounts | null;
  socialEnabled: boolean;
  /** "ws" when the live socket is up, "rest" while polling, "down" when neither works. */
  transport: "ws" | "rest" | "down";
  connected: boolean;
  error: string | null;
  /** Typing indicators keyed by peer hash or `group:<id>`. */
  typing: Record<string, TypingState>;
  /**
   * Reload the snapshot. Resolves with the fresh snapshot, or null
   * when the fetch failed (the error is also on `error`) — callers
   * that must act on the fresh state, like following a successor
   * group id, check the return instead of assuming the refresh
   * landed.
   */
  refresh: () => Promise<Snapshot | null>;
}

export function useTau(): UseTauResult {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [transport, setTransport] = useState<"ws" | "rest" | "down">("down");
  const [error, setError] = useState<string | null>(null);
  const [typing, setTyping] = useState<Record<string, TypingState>>({});
  const snapshotRef = useRef<Snapshot | null>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const restTimer = useRef<ReturnType<typeof setInterval> | null>(null);
  const retryTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const stopped = useRef(false);
  // Count of groups events seen. A REST refresh that started before a
  // groups event must not replace the flags that event set with its
  // older response.
  const groupsSeq = useRef(0);

  snapshotRef.current = snapshot;

  const apply = useCallback((event: LiveEvent) => {
    const current = snapshotRef.current;
    if (event.type === "snapshot") {
      setSnapshot((event as { snapshot: Snapshot }).snapshot);
      return;
    }
    if (event.type === "groups") {
      groupsSeq.current += 1;
    }
    if (!current) return;
    // Functional update: several live events can land in one batch, and
    // each must build on the previous one rather than on the last render.
    setSnapshot((prev) => (prev ? applyEvent(prev, event) ?? prev : prev));
    if (event.type === "typing") {
      const ev = event as { peer?: string; group?: string; state?: string; name?: string };
      const key = ev.group ? `group:${ev.group}` : ev.peer;
      if (!key) return;
      if (ev.state === "stop") {
        setTyping((prev) => {
          const next = { ...prev };
          delete next[key];
          return next;
        });
      } else {
        setTyping((prev) => ({
          ...prev,
          [key]: { peer: ev.peer, group: ev.group, name: ev.name, state: "start", at: Date.now() },
        }));
      }
    }
  }, []);

  const refresh = useCallback(async (): Promise<Snapshot | null> => {
    // A groups event that lands while this fetch is in flight carries
    // newer ignore flags than the snapshot being fetched: the fetched
    // list must not overwrite them (#784).
    const groupsSeqAtStart = groupsSeq.current;
    try {
      const snap = await getStatus();
      let next = snap;
      if (groupsSeq.current !== groupsSeqAtStart) {
        // Keep the live flags (and any groups the live list learned
        // since) instead of letting the stale fetch overwrite them.
        // The fetched snapshot still wins for everything else.
        // Membership comes from the fetched status, which reflects any
        // deletion that happened after the event. Re-adding groups the
        // fetch no longer lists would resurrect removed groups.
        const live = new Map((liveGroups.current ?? []).map((g) => [g.id, g]));
        const merged = snap.groups.map((g) => {
          const current = live.get(g.id);
          return current ? { ...g, ignored: current.ignored } : g;
        });
        next = { ...snap, groups: merged };
      }
      setSnapshot(next);
      setError(null);
      return next;
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      return null;
    }
  }, []);

  const connectWs = useCallback(() => {
    if (typeof WebSocket === "undefined") return false;
    try {
      const ws = new WebSocket(wsUrl());
      wsRef.current = ws;
      ws.onmessage = (msg) => {
        try {
          apply(JSON.parse(String(msg.data)) as LiveEvent);
        } catch {
          // A malformed frame is ignored; the next snapshot heals state.
        }
        setTransport("ws");
        setError(null);
      };
      ws.onopen = () => setTransport("ws");
      ws.onerror = () => {
        // Fall through to REST polling; the close handler schedules a retry.
      };
      ws.onclose = () => {
        if (stopped.current) return;
        wsRef.current = null;
        setTransport((t) => (t === "ws" ? "rest" : t));
        retryTimer.current = setTimeout(connectWs, WS_RETRY_MS);
      };
      return true;
    } catch {
      return false;
    }
  }, [apply]);

  useEffect(() => {
    stopped.current = false;
    void refresh();
    const wsOk = connectWs();
    if (!wsOk) setTransport("rest");
    restTimer.current = setInterval(() => {
      // REST polling is the fallback: it only runs while the socket is down,
      // so a healthy socket never pays for a redundant snapshot fetch.
      if (wsRef.current?.readyState !== WebSocket.OPEN) {
        void refresh();
      }
    }, REST_POLL_MS);
    const typingTimer = setInterval(() => {
      const cutoff = Date.now() - TYPING_TTL_MS;
      setTyping((prev) => {
        const next: Record<string, TypingState> = {};
        let changed = false;
        for (const [key, value] of Object.entries(prev)) {
          if (value.at >= cutoff) next[key] = value;
          else changed = true;
        }
        return changed ? next : prev;
      });
    }, 2000);
    return () => {
      stopped.current = true;
      if (restTimer.current) clearInterval(restTimer.current);
      if (retryTimer.current) clearTimeout(retryTimer.current);
      clearInterval(typingTimer);
      wsRef.current?.close();
      wsRef.current = null;
    };
  }, [connectWs, refresh]);

  return {
    snapshot,
    node: snapshot?.node ?? null,
    peers: snapshot?.peers ?? [],
    groups: snapshot?.groups ?? [],
    messages: snapshot?.messages ?? [],
    unread: snapshot?.unread ?? null,
    socialEnabled: snapshot?.social.enabled ?? false,
    transport,
    connected: transport === "ws" || (transport === "rest" && snapshot !== null),
    error,
    typing,
    refresh,
  };
}

// ------------------------------------------------------------ conversations

export type ConversationKind = "dm" | "group";

export interface Conversation {
  kind: ConversationKind;
  /** Peer destination hash (DM) or group id. */
  key: string;
  name: string;
  peer?: Peer;
  group?: Group;
  lastMessage: ChatMessage | null;
  unread: number;
  muted: boolean;
  archived: boolean;
  /** True when the group is ignored (local display preference). */
  ignored: boolean;
  pinned: boolean;
}

/**
 * Build the conversation list from snapshot state. Pure — unit-tested in
 * use-tau.test.tsx. `mutedPeers`/`mutedGroups`/`archived` are destination
 * hashes / group ids from /api/messages/muted and
 * /api/conversations/archived. Groups whose latest message predates the
 * DM list still appear (a group with no messages yet shows with null).
 */
export function buildConversations(
  messages: ChatMessage[],
  peers: Peer[],
  groups: Group[],
  unread: UnreadCounts | null,
  opts: {
    mutedPeers?: Set<string>;
    mutedGroups?: Set<string>;
    archived?: Set<string>;
    includeArchived?: boolean;
    includeIgnored?: boolean;
  } = {},
): Conversation[] {
  const byPeer = new Map<string, Peer>();
  for (const peer of peers) byPeer.set(peer.destination, peer);
  const latest = new Map<string, ChatMessage>();
  const keyFor = (m: ChatMessage): string | null =>
    m.group ? `group:${m.group}` : m.peer ? `peer:${m.peer}` : null;
  for (const m of messages) {
    const key = keyFor(m);
    if (!key) continue;
    const prev = latest.get(key);
    if (!prev || m.sent_at >= prev.sent_at) latest.set(key, m);
  }
  const out: Conversation[] = [];
  for (const [key, last] of latest) {
    if (key.startsWith("group:")) {
      const groupId = key.slice("group:".length);
      const group = groups.find((g) => g.id === groupId);
      out.push({
        kind: "group",
        key: groupId,
        name: group?.name ?? last.group_name ?? groupId.slice(0, 8),
        group,
        lastMessage: last,
        unread: unread?.per_group[groupId] ?? 0,
        muted: opts.mutedGroups?.has(groupId) ?? false,
        archived: false,
        ignored: group?.ignored ?? false,
        pinned: group?.pinned ?? false,
      });
    } else {
      const hash = key.slice("peer:".length);
      const peer = byPeer.get(hash);
      out.push({
        kind: "dm",
        key: hash,
        name: peer ? peer.alias || peer.name : last.peer_name || hash.slice(0, 8),
        peer,
        lastMessage: last,
        unread: unread?.per_peer[hash] ?? 0,
        muted: opts.mutedPeers?.has(hash) ?? false,
        archived: opts.archived?.has(hash) ?? false,
        ignored: false,
        pinned: peer?.pinned ?? false,
      });
    }
  }
  // Groups with no messages yet still get a row.
  for (const group of groups) {
    if (!latest.has(`group:${group.id}`)) {
      out.push({
        kind: "group",
        key: group.id,
        name: group.name,
        group,
        lastMessage: null,
        unread: unread?.per_group[group.id] ?? 0,
        muted: opts.mutedGroups?.has(group.id) ?? false,
        archived: false,
        ignored: group.ignored ?? false,
        pinned: group.pinned ?? false,
      });
    }
  }
  // Ignored groups stay out of the main list (they live in the Ignored
  // section instead), like archived DMs — unless the caller is building
  // the list for a thread that is already open, which must still resolve.
  const filtered = out.filter(
    (c) =>
      (opts.includeArchived || !c.archived) &&
      (opts.includeIgnored || !c.ignored),
  );
  // Pinned conversations stay at the top; recency orders the rest.
  filtered.sort(
    (a, b) =>
      Number(b.pinned) - Number(a.pinned) ||
      (b.lastMessage?.sent_at ?? 0) - (a.lastMessage?.sent_at ?? 0),
  );
  return filtered;
}

/** An ignored group for the conversation list's Ignored section. */
export interface IgnoredGroup {
  id: string;
  name: string;
  /** False when the group record is gone (an orphaned ignored id): the
   *  row can only be un-ignored, never opened as a thread. */
  openable: boolean;
}

/**
 * Merge the snapshot's ignored groups with the orphaned ignored ids
 * from /api/groups/left. Pure — unit-tested in use-tau.test.tsx. The
 * snapshot carries the name for every group whose record still exists;
 * an id the snapshot does not know is an orphan whose record was
 * deleted (or whose backup restored the ignore list without
 * groups.json), shown by id prefix so it still has a way out. A left
 * entry the snapshot still knows is stale, not an orphan — the group
 * was un-ignored elsewhere and the cached entry has not refreshed yet —
 * so it produces no row.
 */
export function mergeIgnoredGroups(
  groups: Group[],
  leftIgnored: { id: string; name: string }[],
): IgnoredGroup[] {
  const merged = new Map<string, IgnoredGroup>();
  const knownIds = new Set(groups.map((g) => g.id));
  for (const group of groups) {
    if (group.ignored) {
      merged.set(group.id, { id: group.id, name: group.name, openable: true });
    }
  }
  for (const entry of leftIgnored) {
    if (!merged.has(entry.id) && !knownIds.has(entry.id)) {
      merged.set(entry.id, {
        id: entry.id,
        name: entry.name || entry.id.slice(0, 8),
        openable: false,
      });
    }
  }
  return [...merged.values()].sort((a, b) =>
    (a.name || a.id).localeCompare(b.name || b.id),
  );
}

export interface UseConversationsResult {
  conversations: Conversation[];
  archived: Conversation[];
  /** Ignored groups, from the snapshot plus orphaned ids. */
  ignoredGroups: IgnoredGroup[];
  dmRequests: DmRequest[];
  refreshRequests: () => Promise<void>;
  refreshIgnoredGroups: () => Promise<void>;
}

/** Conversation list + held DM requests, derived from the live snapshot. */
export function useConversations(
  tau: Pick<UseTauResult, "messages" | "peers" | "groups" | "unread">,
  opts: {
    mutedPeers?: Set<string>;
    mutedGroups?: Set<string>;
    archived?: Set<string>;
  } = {},
): UseConversationsResult {
  const [dmRequests, setDmRequests] = useState<DmRequest[]>([]);
  const refreshRequests = useCallback(async () => {
    try {
      setDmRequests(await getDmRequests());
    } catch {
      // Requests are advisory; a failed poll must not break the list.
    }
  }, []);
  useEffect(() => {
    void refreshRequests();
  }, [refreshRequests]);
  // Orphaned ignored ids (record gone) come only from /api/groups/left;
  // the snapshot covers every ignored group whose record still exists.
  const [leftIgnored, setLeftIgnored] = useState<{ id: string; name: string }[]>([]);
  const ignoredSeq = useRef(0);
  const refreshIgnoredGroups = useCallback(async () => {
    const seq = ++ignoredSeq.current;
    try {
      const left = await getLeftGroups();
      // A stale response must not overwrite a newer one.
      if (seq === ignoredSeq.current) setLeftIgnored(left.ignored ?? []);
    } catch {
      // Ignored groups are advisory; a failed poll must not break the list.
    }
  }, []);
  // Re-read the orphan list whenever the snapshot's groups change, so
  // ignore/leave/un-ignore from elsewhere (PeerPanel, CLI, another tab)
  // is reflected without a reload.
  useEffect(() => {
    void refreshIgnoredGroups();
  }, [refreshIgnoredGroups, tau.groups]);

  const conversations = useMemo(
    () =>
      buildConversations(tau.messages, tau.peers, tau.groups, tau.unread, {
        mutedPeers: opts.mutedPeers,
        mutedGroups: opts.mutedGroups,
        archived: opts.archived,
      }),
    [tau.messages, tau.peers, tau.groups, tau.unread, opts.mutedPeers, opts.mutedGroups, opts.archived],
  );
  const archived = useMemo(
    () =>
      buildConversations(tau.messages, tau.peers, tau.groups, tau.unread, {
        mutedPeers: opts.mutedPeers,
        mutedGroups: opts.mutedGroups,
        archived: opts.archived,
        includeArchived: true,
      }).filter((c) => c.archived),
    [tau.messages, tau.peers, tau.groups, tau.unread, opts.mutedPeers, opts.mutedGroups, opts.archived],
  );
  const ignoredGroups = useMemo(
    () => mergeIgnoredGroups(tau.groups, leftIgnored),
    [tau.groups, leftIgnored],
  );
  return { conversations, archived, ignoredGroups, dmRequests, refreshRequests, refreshIgnoredGroups };
}

// ------------------------------------------------------------------ outbox

export interface UseOutboxResult {
  outbox: Outbox | null;
  held: HeldEnvelope[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
  cancel: (target: string) => Promise<void>;
  edit: (
    target: string,
    patch: { text?: string; send_in?: number },
  ) => Promise<void>;
  retry: (target?: string) => Promise<unknown>;
  dropHeld: (target: string) => Promise<void>;
}

/** Queued mail waiting on offline peers, plus sealed mail held for others. */
export function useOutbox(pollMs = 15000): UseOutboxResult {
  const [outbox, setOutbox] = useState<Outbox | null>(null);
  const [held, setHeld] = useState<HeldEnvelope[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const [box, heldMail] = await Promise.all([getOutbox(), getHeldMail()]);
      setOutbox(box);
      setHeld(heldMail.held);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
    const id = setInterval(() => void refresh(), pollMs);
    return () => clearInterval(id);
  }, [refresh, pollMs]);

  const cancel = useCallback(
    async (target: string) => {
      await cancelOutbox(target);
      await refresh();
    },
    [refresh],
  );
  const edit = useCallback(
    async (target: string, patch: { text?: string; send_in?: number }) => {
      await editOutbox(target, patch);
      await refresh();
    },
    [refresh],
  );
  const retry = useCallback(
    async (target?: string) => {
      const result = await retryOutbox(target);
      await refresh();
      return result;
    },
    [refresh],
  );
  const dropHeld = useCallback(
    async (target: string) => {
      await dropHeldMail(target);
      await refresh();
    },
    [refresh],
  );
  return { outbox, held, loading, error, refresh, cancel, edit, retry, dropHeld };
}

// ----------------------------------------------------------------- beacons

export interface UseBeaconsResult {
  beacons: Beacon[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
}

/** Live beacons this node is sharing, polled like the outbox. */
export function useBeacons(pollMs = 15000): UseBeaconsResult {
  const [beacons, setBeacons] = useState<Beacon[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    try {
      const listed = await getBeacons();
      setBeacons(listed.beacons ?? []);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
    const id = setInterval(() => void refresh(), pollMs);
    return () => clearInterval(id);
  }, [refresh, pollMs]);

  return { beacons, loading, error, refresh };
}

/** Seconds between position feeds: the beacon tick is every 5 minutes,
//  so a feed a minute is plenty, and it keeps the browser from
//  hammering the bridge on every GPS fix. */
const BEACON_FEED_MIN_INTERVAL_S = 60;

/**
 * Keep every active beacon fed with the browser's position.
 *
 * Mount once, at the shell: a geolocation watch per active beacon
 * posts its fixes to the bridge (throttled), and watches are dropped
 * when their beacon stops or expires.  Without geolocation there is
 * nothing to feed — a beacon started from a typed position simply
 * repeats it, like a CLI beacon with no position file.
 */
export function useBeaconFeed(): void {
  const { beacons } = useBeacons();
  const watchesRef = useRef(new Map<string, number>());
  const lastSentRef = useRef(new Map<string, number>());

  useEffect(() => {
    if (typeof navigator === "undefined" || !navigator.geolocation) return;
    const active = beacons.filter((b) => b.active);
    for (const beacon of active) {
      if (watchesRef.current.has(beacon.id)) continue;
      const watchId = navigator.geolocation.watchPosition(
        (pos) => {
          const now = Date.now() / 1000;
          const last = lastSentRef.current.get(beacon.id) ?? 0;
          if (now - last < BEACON_FEED_MIN_INTERVAL_S) return;
          lastSentRef.current.set(beacon.id, now);
          void updateBeaconPosition(
            beacon.id,
            pos.coords.latitude,
            pos.coords.longitude,
          ).catch(() => undefined);
        },
        () => undefined,
        { enableHighAccuracy: false, maximumAge: 30000, timeout: 10000 },
      );
      watchesRef.current.set(beacon.id, watchId);
    }
    for (const [id, watchId] of watchesRef.current) {
      if (!active.some((b) => b.id === id)) {
        navigator.geolocation.clearWatch(watchId);
        watchesRef.current.delete(id);
        lastSentRef.current.delete(id);
      }
    }
  }, [beacons]);

  useEffect(
    () => () => {
      if (typeof navigator !== "undefined" && navigator.geolocation) {
        for (const watchId of watchesRef.current.values()) {
          navigator.geolocation.clearWatch(watchId);
        }
      }
      watchesRef.current.clear();
      lastSentRef.current.clear();
    },
    [],
  );
}


// -------------------------------------------------------------- broadcasts

export interface UseBroadcastsResult {
  broadcasts: BroadcastList[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
  create: (name: string, members: string[]) => Promise<void>;
  remove: (name: string) => Promise<void>;
  rename: (name: string, newName: string) => Promise<string>;
  send: (name: string, text: string) => Promise<BroadcastSendResponse>;
}

/** Named peer sets for one-to-many DMs; a send fans out per member. */
export function useBroadcasts(): UseBroadcastsResult {
  const [broadcasts, setBroadcasts] = useState<BroadcastList[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  // Monotonic generation: a refresh only applies its snapshot while no
  // newer refresh or rename has started, so a stale pre-rename snapshot
  // can never overwrite a rename that already committed.
  const generationRef = useRef(0);

  const refresh = useCallback(async () => {
    const generation = ++generationRef.current;
    try {
      const lists = await getBroadcasts();
      if (generationRef.current !== generation) return;
      setBroadcasts(lists);
      setError(null);
    } catch (err) {
      if (generationRef.current !== generation) return;
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      if (generationRef.current === generation) setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const create = useCallback(
    async (name: string, members: string[]) => {
      await createBroadcast(name, members);
      await refresh();
    },
    [refresh],
  );
  const remove = useCallback(
    async (name: string) => {
      await deleteBroadcast(name);
      await refresh();
    },
    [refresh],
  );
  const rename = useCallback(
    async (name: string, newName: string) => {
      // Invalidate any refresh already in flight: it may resolve with a
      // pre-rename snapshot after the rename commits.
      generationRef.current += 1;
      // Return the stored name: the server normalises it (collapsed spaces,
      // length cap), and the panel keys drafts and send results by that name.
      let storedName: string;
      try {
        // The rename is committed server-side once this resolves, so apply
        // the stored list from the response; a failed refresh must not
        // leave the old name on screen.
        const { broadcast, previous_name } = await renameBroadcast(name, newName);
        setBroadcasts((prev) =>
          prev.map((entry) => (entry.name === previous_name ? broadcast : entry)),
        );
        storedName = broadcast.name;
      } catch (err) {
        // Re-sync on failure: the invalidated refresh above must not
        // leave a stale snapshot (or a stuck loading state) behind.
        await refresh();
        throw err;
      }
      await refresh();
      return storedName;
    },
    [refresh],
  );
  const send = useCallback(async (name: string, text: string) => {
    const result = await sendBroadcast(name, text);
    await refresh();
    return result;
  }, [refresh]);
  return { broadcasts, loading, error, refresh, create, remove, rename, send };
}

// ----------------------------------------------------------------- traffic

export interface TrafficRow {
  peer: Peer | null;
  destination: string;
  name: string;
  sent: number;
  received: number;
  lastAt: number;
  files: number;
}

/**
 * Per-peer traffic summary derived from stored message history.
 * `messages` is the same window /api/messages returns (newest 200 by
 * default), so counts are "recent traffic", not all-time totals.
 */
export function buildTraffic(
  messages: ChatMessage[],
  peers: Peer[],
): TrafficRow[] {
  const byPeer = new Map<string, Peer>();
  for (const peer of peers) byPeer.set(peer.destination, peer);
  const rows = new Map<string, TrafficRow>();
  for (const m of messages) {
    if (!m.peer) continue;
    const row =
      rows.get(m.peer) ??
      ({
        peer: byPeer.get(m.peer) ?? null,
        destination: m.peer,
        name: byPeer.get(m.peer)?.alias || byPeer.get(m.peer)?.name || m.peer_name,
        sent: 0,
        received: 0,
        lastAt: 0,
        files: 0,
      } satisfies TrafficRow);
    if (m.direction === "out") row.sent += 1;
    else row.received += 1;
    if (m.file) row.files += 1;
    row.lastAt = Math.max(row.lastAt, m.sent_at, m.received_at);
    rows.set(m.peer, row);
  }
  return [...rows.values()].sort((a, b) => b.lastAt - a.lastAt);
}

export function useTraffic(
  messages: ChatMessage[],
  peers: Peer[],
): TrafficRow[] {
  return useMemo(() => buildTraffic(messages, peers), [messages, peers]);
}

// ------------------------------------------------------------ peer actions

export interface PeerActions {
  ping: (target: string, timeout?: number) => Promise<unknown>;
  info: (target: string, timeout?: number) => Promise<unknown>;
  safetyNumber: (target: string) => Promise<unknown>;
  verify: (target: string) => Promise<unknown>;
  block: (target: string) => Promise<unknown>;
  unblock: (target: string) => Promise<unknown>;
  forget: (target: string, force?: boolean) => Promise<unknown>;
  setAlias: (destination: string, alias: string) => Promise<Peer>;
  setNote: (destination: string, note: string) => Promise<Peer>;
  archive: (target: string) => Promise<unknown>;
  unarchive: (target: string) => Promise<unknown>;
  ignoreGroup: (group: string) => Promise<unknown>;
  unignoreGroup: (group: string) => Promise<unknown>;
  mute: (target: { peer: string } | { group: string }, muted?: boolean) => Promise<unknown>;
  pin: (target: { peer: string } | { group: string }, pinned?: boolean) => Promise<unknown>;
  markRead: (
    target?: { peer: string } | { group: string } | { channel: string } | Record<string, never>,
  ) => Promise<UnreadCounts>;
  markUnread: (target: { peer: string } | { group: string }, all?: boolean) => Promise<UnreadCounts>;
}

/** Peer sidebar actions, each a thin wrapper over api.ts. */
export function usePeerActions(): PeerActions {
  return useMemo<PeerActions>(
    () => ({
      ping: (target, timeout) => pingPeer(target, timeout),
      info: (target, timeout) => getPeerInfo(target, timeout),
      safetyNumber: (target) => getSafetyNumber(target),
      verify: (target) => verifyPeer(target),
      block: (target) => blockPeer(target),
      unblock: (target) => unblockPeer(target),
      forget: (target, force) => forgetPeer(target, force),
      setAlias: (destination, alias) => setPeerAlias(destination, alias),
      setNote: (destination, note) => setPeerNote(destination, note),
      archive: (target) => archiveConversation(target),
      unarchive: (target) => unarchiveConversation(target),
      ignoreGroup: (group) => ignoreGroup(group),
      unignoreGroup: (group) => unignoreGroup(group),
      mute: (target, muted) => setMuted(target, muted),
      pin: (target, pinned) => setPinnedConversation(target, pinned),
      markRead: (target = {}) => markRead(target),
      markUnread: (target, all) => markUnread(target, all),
    }),
    [],
  );
}
 markRead(target),
      markUnread: (target, all) => markUnread(target, all),
    }),
    [],
  );
}
