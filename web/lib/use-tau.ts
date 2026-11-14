"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { apiDownload, apiGet, apiPost, apiPostText, eventsUrl } from "./api";
import type {
  AcceptDmRequestResponse,
  AllowlistResponse,
  BlockPeerResponse,
  BridgeInfo,
  BundleExportResponse,
  BundleImportResponse,
  CancelOutboxResponse,
  ChangeGroupMembersResponse,
  CreateGroupResponse,
  DeclineDmRequestResponse,
  DeleteGroupResponse,
  DeleteMessageResponse,
  DmRequestEntry,
  DmRequestsResponse,
  DoctorResponse,
  DropHeldResponse,
  EditMessageResponse,
  ForgetPeerResponse,
  ForwardMessageResponse,
  GroupsResponse,
  HeldMailEntry,
  HeldMailResponse,
  MarkReadResponse,
  MessagesExportDocument,
  MessagesImportResponse,
  MessagesImportResult,
  MessagesResponse,
  MessagesSearchResponse,
  NodeInfo,
  NodeSettings,
  OutboxEntry,
  OutboxResponse,
  Peer,
  PeerInfo,
  PeerInfoResult,
  PeersResponse,
  PingResult,
  PinMessageResponse,
  ReactResponse,
  RenameResponse,
  RetractMessageResponse,
  RetryOutboxResponse,
  SafetyNumberResponse,
  SendFileResponse,
  SendGroupResponse,
  SendResponse,
  Snapshot,
  SocialInfo,
  TauGroup,
  TauMessage,
  UnpinMessageResponse,
  UnreadCounts,
  UpdateSettingsResponse,
  VerifyPeerResponse,
  VerifyState,
} from "./types";

export type ConnectionState =
  | "connecting"
  | "live"
  | "reconnecting";

export const EMPTY_UNREAD: UnreadCounts = { per_peer: {}, total: 0 };

export interface PingState {
  pending?: boolean;
  rtt_ms?: number | null;
  error?: string;
}

export interface PeerInfoState {
  pending?: boolean;
  info?: PeerInfo;
  error?: string;
}

export interface BlockState {
  pending?: boolean;
  error?: string;
}

function sortPeers(peers: Peer[]): Peer[] {
  return [...peers].sort((a, b) => b.last_seen - a.last_seen);
}

// A peer removed by a forget carries the last_seen it had at that moment.
// Snapshots or peer events queued before the forget are at or below that
// cutoff and must not bring the peer back; a genuine re-announce is newer.
function predatesForget(
  forgotten: Map<string, number>,
  peer: Peer,
): boolean {
  const cutoff = forgotten.get(peer.destination);
  return cutoff !== undefined && peer.last_seen <= cutoff;
}

/** True when the held copy carries a newer edit than an incoming version. */
function holdsNewerEdit(held: TauMessage, incoming: TauMessage): boolean {
  return (held.edited_at ?? 0) > (incoming.edited_at ?? 0);
}

function readEventData<T>(event: Event): T | null {
  try {
    return JSON.parse((event as MessageEvent).data) as T;
  } catch {
    return null;
  }
}

/** Read a browser-picked file as a base64 string (no data-URL prefix). */
export function fileToBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(new Error("could not read file"));
    reader.onload = () => {
      const url = reader.result;
      if (typeof url !== "string") {
        reject(new Error("could not read file"));
        return;
      }
      // "data:<type>;base64,<payload>" — the payload is the file bytes.
      const comma = url.indexOf(",");
      resolve(comma === -1 ? "" : url.slice(comma + 1));
    };
    reader.readAsDataURL(file);
  });
}

// Bound on an allow-list write: a POST to the local bridge settles
// in milliseconds, so 30s means the bridge is hung, not slow —
// aborting lets the pending-write guard clear and the next poll
// reconcile.
const ALLOWLIST_WRITE_TIMEOUT_MS = 30000;

export function useTau() {
  const [node, setNode] = useState<NodeInfo | null>(null);
  const [peers, setPeers] = useState<Peer[]>([]);
  const [groups, setGroups] = useState<TauGroup[]>([]);
  // The latest roster swap: the console uses it to keep a selected old
  // group id pointing at its successor.
  const [groupSwap, setGroupSwap] = useState<{
    from: string;
    to: string;
  } | null>(null);
  const [messages, setMessages] = useState<TauMessage[]>([]);
  // The search the feed is showing, or null for the live traffic. Kept
  // beside `messages` so a search result list is never mixed into the
  // live feed: new arrivals keep updating `messages` underneath, and
  // clearing the search returns to it.
  const [searchResults, setSearchResults] = useState<TauMessage[]>([]);
  const [searchQuery, setSearchQuery] = useState<string | null>(null);
  const [searchError, setSearchError] = useState<string | null>(null);
  // The pinned view, or null for the live traffic. Like search, it is a
  // snapshot kept beside `messages`: pinning and unpinning keep updating
  // `messages` underneath, and clearing the view returns to it.
  const [pinnedMessages, setPinnedMessages] = useState<TauMessage[] | null>(
    null,
  );
  const [pinnedError, setPinnedError] = useState<string | null>(null);
  const [unread, setUnread] = useState<UnreadCounts>(EMPTY_UNREAD);
  const [outbox, setOutbox] = useState<OutboxEntry[]>([]);
  const [heldForOthers, setHeldForOthers] = useState(0);
  const [settings, setSettings] = useState<NodeSettings | null>(null);
  const [settingsError, setSettingsError] = useState<string | null>(null);
  const [heldMail, setHeldMail] = useState<HeldMailEntry[]>([]);
  const [dmRequests, setDmRequests] = useState<DmRequestEntry[]>([]);
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
  const snapshotMax = useRef(0);
  const unreadRevision = useRef(-1);
  // Bumped on every peer event per destination, so a slow alias response
  // cannot overwrite a newer event that arrived while it was in flight.
  const peerEventVersion = useRef<Record<string, number>>({});
  // Bumped whenever a snapshot replaces the peer list: a reconnect
  // snapshot is newer than any alias response that was already in
  // flight, and peer events alone do not cover it.
  const snapshotGeneration = useRef(0);
  // Live (SSE) updates to feed entries, stamped with a rising epoch, so
  // a feed refresh can tell which entries changed while its GET was in
  // flight and keep those newer versions.
  const liveEpoch = useRef(0);
  const liveTouched = useRef(new Map<number, number>());
  // Counts completed group creates/deletes, so a snapshot that predates
  // one is not allowed to restore the old roster.
  const groupEdits = useRef(0);
  const groupsSeq = useRef(0);

  const refreshGroups = useCallback(async () => {
    // Same polling shape as the outbox: a group learned from an
    // incoming message shows up here without waiting for a reload.
    const requestId = ++groupsSeq.current;
    const result = await apiGet<GroupsResponse>("/api/groups");
    if (requestId !== groupsSeq.current) return;
    setGroups(result.groups);
  }, []);

  // Verification time the console last applied per peer. Peer events
  // queued before a successful verify carry the older trust state and
  // must not undo the badge.
  const localVerifiedAt = useRef<Record<string, number | null>>({});
  // Ordering of authoritative peer state: each peer event or snapshot
  // gets a sequence number. A block/unblock reply sent before a newer
  // one must not overwrite the flag the event already delivered.
  const peerEventSeq = useRef(0);
  const peerRevision = useRef<Map<string, number>>(new Map());
  const snapshotRevision = useRef(0);

  const searchQueryRef = useRef(searchQuery);
  const searchSeq = useRef(0);
  // Set to the search runner once it is defined below, so edit handlers
  // can re-run an active search and keep its hits in step with the text.
  const runSearchRef = useRef<((trimmed: string) => Promise<void>) | null>(
    null,
  );
  const refreshActiveSearch = useCallback(() => {
    const query = searchQueryRef.current;
    if (query) void runSearchRef.current?.(query);
  }, []);
  // Console ids removed by a delete or retract in this tab. A search
  // that ran before the removal keeps its stale hits in state, and a
  // search re-run after it still lists the record until the inbox
  // copy is gone — either way the removed id must not come back.
  const removedMessageIds = useRef(new Set<number>());
  const pinnedSeq = useRef(0);
  // Bumped on every live pin or unpin, so a pinned GET that raced one
  // is asked again rather than overwriting the newer event. Deletions
  // bump it too: a pinned GET that raced a retract must not reinstate
  // the removed entry.
  const pinnedEvents = useRef(0);
  // Re-reads the pinned view on a reconnect; set once showPinned exists.
  const refreshPinnedView = useRef<(() => void) | null>(null);

  useEffect(() => {
    searchQueryRef.current = searchQuery;
  }, [searchQuery]);

  // Unread counts can arrive out of order: an SSE event queued before a
  // snapshot, or an HTTP response delayed past a newer event. Only a
  // count at least as new as the last one applied may replace state.
  const applyUnread = useCallback(
    (counts: UnreadCounts, reset = false) => {
      const revision = counts.revision ?? 0;
      if (!reset && revision < unreadRevision.current) return;
      unreadRevision.current = revision;
      setUnread(counts);
    },
    [],
  );

  // The node's identity and name live on the snapshot; a rename (or an
  // announce) publishes a `status` event instead of a full snapshot, so
  // a status event re-reads the snapshot's node record on its own.
  // Only the newest node update may apply: a refresh issued before a
  // rename or snapshot that resolves late must not restore an old name.
  const nodeSeq = useRef(0);
  const refreshNode = useCallback(async () => {
    const requestId = ++nodeSeq.current;
    const snapshot = await apiGet<Snapshot>("/api/status");
    if (requestId !== nodeSeq.current) return;
    setNode(snapshot.node);
  }, []);

  useEffect(() => {
    let disposed = false;
    // Edits made before this connection opened are in its snapshot;
    // edits made after may not be.
    let editsAtOpen = groupEdits.current;
    const source = new EventSource(eventsUrl(0));

    const onOpen = () => {
      editsAtOpen = groupEdits.current;
      if (!disposed) setConnection("live");
    };

    const onSnapshot = (event: Event) => {
      const payload = readEventData<{ snapshot: Snapshot }>(event);
      if (!payload?.snapshot) return;
      const snapshot = payload.snapshot;
      snapshotMax.current =
        snapshot.messages.length > 0
          ? snapshot.messages[snapshot.messages.length - 1].id
          : 0;
      nodeSeq.current += 1;
      setNode(snapshot.node);
      peerEventSeq.current += 1;
      snapshotRevision.current = peerEventSeq.current;
      localVerifiedAt.current = {};
      setPeers(
        sortPeers(
          snapshot.peers.filter(
            (peer) => !predatesForget(forgottenPeers.current, peer),
          ),
        ),
      );
      snapshotGeneration.current += 1;
      if (groupEdits.current !== editsAtOpen) {
        // A group edit completed after this connection opened, so the
        // snapshot's roster may predate it. Re-fetch instead.
        refreshGroups().catch(() => {
          // The next poll reconciles.
        });
      } else {
        setGroups(snapshot.groups ?? []);
      }
      setMessages(snapshot.messages);
      // A snapshot is authoritative for its connection: reset the
      // revision baseline so a delayed event from the old connection
      // cannot overwrite it.
      applyUnread(snapshot.unread ?? EMPTY_UNREAD, true);
      setBridge(snapshot.bridge);
      setSocial(snapshot.social ?? null);
      // A reconnect may have missed pin changes: re-read an open pinned view.
      refreshPinnedView.current?.();
      setConnection("live");
    };

    const onMessage = (event: Event) => {
      const payload = readEventData<{
        message: TauMessage;
        unread?: UnreadCounts;
      }>(event);
      if (!payload?.message) return;
      const message = payload.message;
      if (payload.unread) applyUnread(payload.unread);
      liveTouched.current.set(message.id, ++liveEpoch.current);
      if (message.id <= snapshotMax.current) return;
      snapshotMax.current = Math.max(snapshotMax.current, message.id);
      setMessages((previous) => {
        if (previous.some((entry) => entry.id === message.id)) return previous;
        return [...previous, message].sort((a, b) => a.id - b.id);
      });
    };

    const onMessageUpdated = (event: Event) => {
      const payload = readEventData<{ message: TauMessage }>(event);
      if (!payload?.message) return;
      const message = payload.message;
      liveTouched.current.set(message.id, ++liveEpoch.current);
      if (message.pinned !== undefined) pinnedEvents.current += 1;
      // An update that omits `pinned` keeps the pin state the console
      // already shows; only an explicit `pinned: false` clears it.
      const withPinState = (prior: TauMessage): TauMessage =>
        message.pinned === undefined && prior.pinned
          ? { ...message, pinned: true }
          : message;
      // A queued message that the mail sweep has since delivered: the
      // server re-sends the same console id with status "delivered".
      // Replace it in place rather than appending a duplicate.  A
      // version older than the held edit (a late event) is ignored.
      setMessages((previous) =>
        previous.map((entry) =>
          entry.id === message.id && !holdsNewerEdit(entry, message)
            ? withPinState(entry)
            : entry,
        ),
      );
      if (message.edited_at !== undefined) refreshActiveSearch();
      // Search hits show the same bubble, so they follow pin changes too.
      setSearchResults((previous) =>
        previous.some((entry) => entry.id === message.id)
          ? previous.map((entry) =>
              entry.id === message.id ? withPinState(entry) : entry,
            )
          : previous,
      );
      // A pin or unpin lands here too: keep an open pinned view in
      // step. An unpin removes the entry; a pin replaces it, or inserts
      // it newest first when another tab pinned a message this view
      // does not list yet.
      setPinnedMessages((previous) => {
        if (previous === null) return null;
        if (message.pinned === false) {
          return previous.filter((entry) => entry.id !== message.id);
        }
        const existing = previous.find((entry) => entry.id === message.id);
        if (existing) {
          return previous.map((entry) =>
            entry.id === message.id ? withPinState(entry) : entry,
          );
        }
        if (message.pinned !== true) return previous;
        return [message, ...previous].sort((a, b) => b.id - a.id);
      });
    };

    const onMessageDeleted = (event: Event) => {
      // The bus stamps its own sequence into `id`, so the deleted
      // console id arrives under `message_id` instead.
      const payload = readEventData<{ message_id: number }>(event);
      if (!payload || typeof payload.message_id !== "number") return;
      removedMessageIds.current.add(payload.message_id);
      // A delete that lands while a pinned GET is in flight makes its
      // snapshot stale: force the fetch to retry, like a pin or unpin.
      pinnedEvents.current += 1;
      setMessages((previous) =>
        previous.filter((entry) => entry.id !== payload.message_id),
      );
      setSearchResults((previous) =>
        previous.filter((entry) => entry.id !== payload.message_id),
      );
      setPinnedMessages((previous) =>
        previous === null
          ? null
          : previous.filter((entry) => entry.id !== payload.message_id),
      );
    };

    const onPeer = (event: Event) => {
      const payload = readEventData<{ peer: Peer }>(event);
      if (!payload?.peer) return;
      const peer = payload.peer;
      if (predatesForget(forgottenPeers.current, peer)) return;
      peerEventVersion.current[peer.destination] =
        (peerEventVersion.current[peer.destination] ?? 0) + 1;
      // Ordering for block/unblock replies: a peer event that arrives
      // after a block request was sent is newer than the reply, so the
      // reply must not overwrite the flag it already delivered.
      peerEventSeq.current += 1;
      peerRevision.current.set(peer.destination, peerEventSeq.current);
      // A blocked sender's inbound history is hidden by the server's
      // message snapshots; drop it from the live feed too, so a block
      // from any console takes effect at once. History stays stored
      // and comes back with the next snapshot after an unblock.
      if (peer.blocked) {
        setMessages((previous) =>
          previous.filter(
            (entry) =>
              !(entry.direction === "in" && entry.peer === peer.destination),
          ),
        );
      }
      // A peer event queued before a successful verify carries the
      // older trust state; the local verification is newer, so keep
      // it instead of letting the stale event undo the badge. A
      // snapshot resets the record (see onSnapshot): it is
      // authoritative, and any event after it is newer than the verify.
      const localVerified = localVerifiedAt.current[peer.destination];
      const currentPeer =
        localVerified != null &&
        (peer.verified_at == null || peer.verified_at < localVerified)
          ? {
              ...peer,
              verified: true,
              verified_at: localVerified,
              key_change_at: null,
            }
          : peer;
      setPeers((previous) => {
        const next = previous.filter(
          (entry) => entry.destination !== currentPeer.destination,
        );
        next.push(currentPeer);
        return sortPeers(next);
      });
    };

    const onUnread = (event: Event) => {
      const payload = readEventData<{ unread: UnreadCounts }>(event);
      if (!payload?.unread) return;
      applyUnread(payload.unread);
    };

    const onStatus = () => {
      // A rename or announce in another tab: re-read the node record
      // rather than waiting for the next snapshot. A failed refresh
      // leaves the last known name in place until the next one.
      void refreshNode().catch(() => undefined);
    };

    const onError = () => {
      if (!disposed) setConnection("reconnecting");
    };

    source.addEventListener("open", onOpen);
    source.addEventListener("snapshot", onSnapshot);
    source.addEventListener("message", onMessage);
    source.addEventListener("message_updated", onMessageUpdated);
    source.addEventListener("message_deleted", onMessageDeleted);
    source.addEventListener("peer", onPeer);
    source.addEventListener("unread", onUnread);
    source.addEventListener("status", onStatus);
    source.addEventListener("error", onError);

    return () => {
      disposed = true;
      source.removeEventListener("open", onOpen);
      source.removeEventListener("snapshot", onSnapshot);
      source.removeEventListener("message", onMessage);
      source.removeEventListener("message_updated", onMessageUpdated);
      source.removeEventListener("message_deleted", onMessageDeleted);
      source.removeEventListener("peer", onPeer);
      source.removeEventListener("unread", onUnread);
      source.removeEventListener("status", onStatus);
      source.removeEventListener("error", onError);
      source.close();
    };
  }, [applyUnread, refreshGroups, refreshNode, refreshActiveSearch]);

  // The unread total rides on the tab title, like a mail client.
  useEffect(() => {
    document.title =
      unread.total > 0
        ? `(${unread.total}) tau · mesh console`
        : "tau · mesh console";
  }, [unread.total]);

  const refreshSeq = useRef(0);
  const settingsSeq = useRef(0);

  const refreshSettings = useCallback(async () => {
    // Same polling shape as the outbox: a setting changed on the CLI
    // shows up here without waiting for a reload.
    const requestId = ++settingsSeq.current;
    try {
      const result = await apiGet<NodeSettings>("/api/settings");
      if (requestId !== settingsSeq.current) return;
      setSettings(result);
      setSettingsError(null);
    } catch (caught) {
      // Record the failure so the section can explain why the switches
      // are disabled, then rethrow for callers that care.
      if (requestId === settingsSeq.current) {
        setSettingsError(
          caught instanceof Error && caught.message
            ? caught.message
            : "couldn't load settings",
        );
      }
      throw caught;
    }
  }, []);

  const peersSeq = useRef(0);
  const forgottenPeers = useRef(new Map<string, number>());

  const refreshPeers = useCallback(async () => {
    // Same polling shape as the outbox: only the newest issued request
    // may replace state, so a slow poll cannot restore a peer a forget
    // already removed.
    const requestId = ++peersSeq.current;
    const result = await apiGet<PeersResponse>("/api/peers");
    if (requestId !== peersSeq.current) return;
    setPeers(sortPeers(result.peers));
  }, []);

  const refreshHeldSeq = useRef(0);

  const refreshOutbox = useCallback(async () => {
    // Polls and action-triggered refreshes overlap; only the newest
    // issued request may replace state, so a slow poll cannot restore
    // an entry a cancel (or a send) already removed.
    const requestId = ++refreshSeq.current;
    const result = await apiGet<OutboxResponse>("/api/outbox");
    if (requestId !== refreshSeq.current) return;
    setOutbox(result.outbox);
    setHeldForOthers(result.held_for_others);
  }, []);

  const refreshHeldMail = useCallback(async () => {
    // Same stale-response guard as the outbox refresh: a slow poll
    // must not restore an entry a drop already removed.
    const requestId = ++refreshHeldSeq.current;
    const result = await apiGet<HeldMailResponse>("/api/outbox/held");
    if (requestId !== refreshHeldSeq.current) return;
    setHeldMail(result.held);
  }, []);

  const refreshDmRequestsSeq = useRef(0);

  const refreshDmRequests = useCallback(async () => {
    // Same stale-response guard as the outbox refresh: a slow poll
    // must not restore a request an accept or decline already removed.
    const requestId = ++refreshDmRequestsSeq.current;
    const result = await apiGet<DmRequestsResponse>("/api/messages/requests");
    if (requestId !== refreshDmRequestsSeq.current) return;
    setDmRequests(result.requests);
  }, []);

  const allowlistSeq = useRef(0);

  // Allow-list requests can resolve out of order: only the newest
  // issued request may replace state, so a slow add cannot restore
  // the list a later remove already changed — the same pattern as
  // refreshOutbox and the unread revision guard.
  const applyAllowlist = useCallback(
    (requestId: number, result: AllowlistResponse) => {
      if (requestId !== allowlistSeq.current) return;
      setAllowlist(result);
    },
    [],
  );

  // Count of allow-list writes in flight. A poll must not bump the
  // sequence while one is pending: that would discard the write's
  // successful response and could apply a pre-write snapshot. Each
  // write is bounded by an abort (below), so a stalled POST cannot
  // freeze polling: the counter always clears and the next tick
  // reconciles whatever reached the server.
  const allowlistPending = useRef(0);

  // An allow-list write that never settles (a hung bridge) must not
  // hold the pending counter forever: abort it after a generous
  // bound for a local bridge, and let the next poll reconcile.
  const runAllowlistWrite = useCallback(
    async (
      write: (signal: AbortSignal) => Promise<AllowlistResponse>,
    ): Promise<AllowlistResponse> => {
      allowlistPending.current++;
      const controller = new AbortController();
      const timer = setTimeout(
        () => controller.abort(),
        ALLOWLIST_WRITE_TIMEOUT_MS,
      );
      try {
        const requestId = ++allowlistSeq.current;
        const result = await write(controller.signal);
        applyAllowlist(requestId, result);
        return result;
      } finally {
        clearTimeout(timer);
        allowlistPending.current--;
      }
    },
    [applyAllowlist],
  );

  const refreshAllowlist = useCallback(async () => {
    if (allowlistPending.current > 0) return;
    const requestId = ++allowlistSeq.current;
    const result = await apiGet<AllowlistResponse>("/api/allowlist");
    applyAllowlist(requestId, result);
  }, [applyAllowlist]);

  useEffect(() => {
    if (connection !== "live") return;
    async function tick() {
      // The endpoints are independent: a failure on one must not stop
      // the others from updating their state. Each is a side panel: a
      // failed poll leaves the last known values in place instead of
      // flashing an error.
      // The allow-list polls here too: another console tab writes
      // through the same live API, so a change made there would
      // otherwise leave this section stale behind the rest.
      await Promise.allSettled([
        refreshOutbox(),
        refreshHeldMail(),
        refreshDmRequests(),
        refreshGroups(),
        refreshSettings(),
        refreshAllowlist(),
      ]);
    }
    void tick();
    const timer = setInterval(() => void tick(), 15000);
    return () => {
      clearInterval(timer);
    };
  }, [connection, refreshOutbox, refreshHeldMail, refreshDmRequests, refreshGroups, refreshSettings, refreshAllowlist]);

  const addAllowlistPeer = useCallback(
    (target: string) =>
      runAllowlistWrite((signal) =>
        apiPost<AllowlistResponse>("/api/allowlist/add", { target }, signal),
      ),
    [runAllowlistWrite],
  );

  const removeAllowlistPeer = useCallback(
    (target: string) =>
      runAllowlistWrite((signal) =>
        apiPost<AllowlistResponse>(
          "/api/allowlist/remove",
          { target },
          signal,
        ),
      ),
    [runAllowlistWrite],
  );

  const setAllowlistEnabled = useCallback(
    (enabled: boolean) =>
      runAllowlistWrite((signal) =>
        apiPost<AllowlistResponse>(
          "/api/allowlist/set-enabled",
          { enabled },
          signal,
        ),
      ),
    [runAllowlistWrite],
  );

  const cancelOutbox = useCallback(
    async (id: string) => {
      const result = await apiPost<CancelOutboxResponse>(
        "/api/outbox/cancel",
        { target: id },
      );
      // The cancel already succeeded: drop the entry locally at once,
      // and treat the follow-up refresh as best-effort so a failed GET
      // cannot make a successful cancel look like a failure.
      const cancelledId = result.cancelled.id;
      setOutbox((previous) =>
        previous.filter((entry) => entry.id !== cancelledId),
      );
      try {
        await refreshOutbox();
      } catch {
        // The next poll reconciles.
      }
      return result.cancelled;
    },
    [refreshOutbox],
  );

  const updateSetting = useCallback(
    async (setting: keyof NodeSettings, value: boolean) => {
      const result = await apiPost<UpdateSettingsResponse>("/api/settings", {
        setting,
        value,
      });
      // The update already succeeded: apply the returned settings at
      // once, and treat the follow-up refresh as best-effort so a
      // failed GET cannot make a successful change look like a failure.
      setSettings(result.settings);
      try {
        await refreshSettings();
      } catch {
        // The next poll reconciles.
      }
      return result.settings;
    },
    [refreshSettings],
  );

  const deleteMessage = useCallback(async (id: number) => {
    const result = await apiPost<DeleteMessageResponse>(
      "/api/messages/delete",
      { id },
    );
    // The delete already succeeded: drop the bubble locally at once,
    // and let the message_deleted event reconcile other tabs.
    setMessages((previous) =>
      previous.filter((entry) => entry.id !== result.deleted.id),
    );
    removedMessageIds.current.add(result.deleted.id);
    setSearchResults((previous) =>
      previous.filter((entry) => entry.id !== result.deleted.id),
    );
    setPinnedMessages((previous) =>
      previous === null
        ? null
        : previous.filter((entry) => entry.id !== result.deleted.id),
    );
    return result.deleted;
  }, []);

  const editMessage = useCallback(async (id: number, text: string) => {
    const result = await apiPost<EditMessageResponse>("/api/messages/edit", {
      id,
      text,
    });
    // The edit already succeeded: swap the new text in at once, and
    // let the message_updated event reconcile other tabs. When the
    // peer had no copy the server reports edited false and leaves the
    // bubble alone — the caller says so instead of showing new text
    // one-sidedly.
    // A late response older than a version already received over the
    // event stream must not rewind the bubble.
    // A group edit updates every leg: swap each returned entry, not
    // just the one the user tapped.
    const editedEntries = result.messages?.length
      ? result.messages
      : result.message
        ? [result.message]
        : [];
    if (result.edited && editedEntries.length > 0) {
      const byId = new Map(editedEntries.map((entry) => [entry.id, entry]));
      setMessages((previous) =>
        previous.map((entry) => {
          const replacement = byId.get(entry.id);
          return replacement && !holdsNewerEdit(entry, replacement)
            ? replacement
            : entry;
        }),
      );
      refreshActiveSearch();
    }
    return result;
  }, [refreshActiveSearch]);

  const retractMessage = useCallback(async (id: number) => {
    const result = await apiPost<RetractMessageResponse>(
      "/api/messages/retract",
      { id },
    );
    // The retract already succeeded: drop the bubble locally at once,
    // and let the message_deleted event reconcile other tabs. A group
    // retract removes every leg, so drop all of them, not just the
    // one the user tapped.
    const removedIds = result.retracted_ids ?? [result.retracted.id];
    for (const removedId of removedIds) {
      removedMessageIds.current.add(removedId);
    }
    setMessages((previous) =>
      previous.filter((entry) => !removedIds.includes(entry.id)),
    );
    setSearchResults((previous) =>
      previous.filter((entry) => !removedIds.includes(entry.id)),
    );
    setPinnedMessages((previous) =>
      previous === null
        ? null
        : previous.filter((entry) => !removedIds.includes(entry.id)),
    );
    return result;
  }, []);

  const forwardMessage = useCallback(
    async (id: number, to: string) => {
      const result = await apiPost<ForwardMessageResponse>(
        "/api/messages/forward",
        { id, to },
      );
      // The forwarded copy is a new outbound entry: append it locally
      // at once, like `send`, and let the SSE event reconcile other tabs.
      const sent = result.message;
      if (sent !== null) {
        setMessages((previous) => {
          if (previous.some((entry) => entry.id === sent.id)) {
            return previous;
          }
          return [...previous, sent].sort((a, b) => a.id - b.id);
        });
        snapshotMax.current = Math.max(snapshotMax.current, sent.id);
      }
      // A queued forward lands in the outbox; refresh so it shows at
      // once instead of waiting for the next poll. Best-effort: a
      // failed refresh must not report a successful send as failed.
      try {
        await refreshOutbox();
      } catch {
        // The next poll picks the entry up.
      }
      // Null when the forwarded copy went out but was retracted before
      // the console record landed.
      return sent;
    },
    [refreshOutbox],
  );

  const pinMessage = useCallback(async (id: number) => {
    // The node records the pin and the bridge re-sends the message as
    // `message_updated`, so the bubble updates through the SSE stream —
    // no local state change here, like `reactToMessage`.
    return apiPost<PinMessageResponse>("/api/messages/pin", { id });
  }, []);

  const unpinMessage = useCallback(async (id: number) => {
    return apiPost<UnpinMessageResponse>("/api/messages/unpin", { id });
  }, []);
  const retryOutbox = useCallback(
    async (id?: string) => {
      const result = await apiPost<RetryOutboxResponse>(
        "/api/outbox/retry",
        id ? { target: id } : {},
      );
      // The retry already ran: drop a confirmed delivery locally at once,
      // so a failed refresh cannot leave it listed as actionable.
      if (result.target && result.target_delivered) {
        const deliveredId = result.target;
        setOutbox((previous) =>
          previous.filter((entry) => entry.id !== deliveredId),
        );
      }
      // Refresh so the rest of the queue is current. Best-effort, like
      // cancel: a failed GET cannot make a successful retry look like
      // a failure — but a retry-all cannot name which entries left,
      // so a failed refresh gets one delayed re-attempt before the
      // list is left to the next poll.
      try {
        await refreshOutbox();
      } catch {
        setTimeout(() => {
          void refreshOutbox().catch(() => undefined);
        }, 2000);
      }
      return result;
    },
    [refreshOutbox],
  );

  const dropHeldMail = useCallback(
    async (id: string) => {
      const result = await apiPost<DropHeldResponse>("/api/outbox/drop-held", {
        target: id,
      });
      // The drop already succeeded: remove the entry locally at once,
      // and treat the follow-up refreshes as best-effort so a failed
      // GET cannot make a successful drop look like a failure.
      const droppedId = result.dropped.id;
      setHeldMail((previous) =>
        previous.filter((entry) => entry.id !== droppedId),
      );
      // Invalidate any outbox GET issued before this drop so a stale
      // response cannot restore the old held count.
      refreshSeq.current++;
      setHeldForOthers((previous) => Math.max(0, previous - 1));
      // Run both follow-ups independently so a failed held-mail GET
      // cannot skip reconciling the count.
      await Promise.allSettled([refreshHeldMail(), refreshOutbox()]);
      return result.dropped;
    },
    [refreshHeldMail, refreshOutbox],
  );

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
      const sent = result.message;
      if (sent !== null) {
        setMessages((previous) => {
          if (previous.some((entry) => entry.id === sent.id)) {
            return previous;
          }
          return [...previous, sent].sort((a, b) => a.id - b.id);
        });
        snapshotMax.current = Math.max(snapshotMax.current, sent.id);
      }
      // A queued send lands in the outbox; refresh so it shows at once
      // instead of waiting for the next poll. Best-effort: a failed
      // refresh must not report a successful send as failed.
      try {
        await refreshOutbox();
      } catch {
        // The next poll picks the entry up.
      }
      // Null when the send went out but was retracted before the
      // console record landed: the send succeeded, there is just no
      // record to show.
      return sent;
    },
    [refreshOutbox],
  );

  const sendGroup = useCallback(
    async (group: string, text: string) => {
      const result = await apiPost<SendGroupResponse>("/api/messages", {
        group,
        text,
      });
      setMessages((previous) => {
        const fresh = result.messages.filter(
          (entry) => !previous.some((known) => known.id === entry.id),
        );
        if (fresh.length === 0) return previous;
        return [...previous, ...fresh].sort((a, b) => a.id - b.id);
      });
      // Do not advance snapshotMax here: the legs are not a complete
      // view up to their highest id, and an inbound message recorded
      // between two legs must still be accepted from its SSE event.
      // Duplicate leg events are dropped by the dedupe in onMessage.
      // A queued leg lands in the outbox; refresh so it shows at once
      // instead of waiting for the next poll. Best-effort: a failed
      // refresh must not report a successful send as failed.
      try {
        await refreshOutbox();
      } catch {
        // The next poll picks the entry up.
      }
      if (!result.ok) {
        // The legs are already in the feed with their own statuses, so
        // this only surfaces the summary: the message stays sendable.
        // Name the members who already received it — or have a copy
        // queued and will still receive it — so a retry is a
        // deliberate duplicate fan-out, not a silent one.
        const legs = (result.response.results as Array<
          Record<string, unknown>
        >) ?? [];
        const failed = legs.filter((leg) => !leg.delivered && !leg.queued);
        const deliveredNames = legs
          .filter((leg) => leg.delivered)
          .map((leg) => String(leg.to_name || leg.to || "a member").slice(0, 8));
        const queuedNames = legs
          .filter((leg) => leg.queued)
          .map((leg) => String(leg.to_name || leg.to || "a member").slice(0, 8));
        const names = failed.map((leg) =>
          String(leg.to_name || leg.to || "a member").slice(0, 8),
        );
        const already = [
          ...(deliveredNames.length > 0
            ? [`${deliveredNames.join(", ")} already received this`]
            : []),
          ...(queuedNames.length > 0
            ? [`${queuedNames.join(", ")} have a copy queued`]
            : []),
        ].join("; ");
        throw new Error(
          names.length > 0
            ? already.length > 0
              ? `could not send to ${names.join(", ")} — ${already}; sending again resends it to the whole group`
              : `could not send to ${names.join(", ")}`
            : "some group members did not receive the message",
        );
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
    const sent = result.message;
    if (sent !== null) {
      setMessages((previous) => {
        if (previous.some((entry) => entry.id === sent.id)) {
          return previous;
        }
        return [...previous, sent].sort((a, b) => a.id - b.id);
      });
      snapshotMax.current = Math.max(snapshotMax.current, sent.id);
    }
    // Null when the file send went out but was retracted before the
    // console record landed: the transfer succeeded, there is just no
    // record to show.
    return sent;
  }, []);

  const ping = useCallback(async (destination: string) => {
    setPings((previous) => ({
      ...previous,
      [destination]: { pending: true },
    }));
    try {
      const result = await apiPost<PingResult>("/api/ping", {
        to: destination,
      });
      setPings((previous) => ({
        ...previous,
        [destination]: { rtt_ms: result.rtt_ms },
      }));
    } catch (error) {
      setPings((previous) => ({
        ...previous,
        [destination]: {
          error: error instanceof Error ? error.message : "ping failed",
        },
      }));
    }
  }, []);

  const fetchInfo = useCallback(async (destination: string) => {
    setInfos((previous) => ({
      ...previous,
      [destination]: { pending: true },
    }));
    try {
      const result = await apiPost<PeerInfoResult>("/api/info", {
        to: destination,
      });
      setInfos((previous) => ({
        ...previous,
        [destination]: { info: result.info },
      }));
    } catch (error) {
      setInfos((previous) => ({
        ...previous,
        [destination]: {
          error: error instanceof Error ? error.message : "info failed",
        },
      }));
    }
  }, []);

  const announce = useCallback(async () => {
    setAnnouncing(true);
    try {
      await apiPost<{ ok: boolean }>("/api/announce", {});
    } finally {
      setAnnouncing(false);
    }
  }, []);

  const renameNode = useCallback(
    async (name: string) => {
      const result = await apiPost<RenameResponse>("/api/id/rename", {
        name,
      });
      // The rename already succeeded: show the new name at once, and
      // treat the follow-up refresh as best-effort so a failed GET
      // cannot make a successful rename look like a failure. Invalidate
      // any refresh still in flight so it cannot restore the old name.
      nodeSeq.current += 1;
      setNode((previous) =>
        previous ? { ...previous, name: result.name } : previous,
      );
      try {
        await refreshNode();
      } catch {
        // The next snapshot reconciles.
      }
      return result;
    },
    [refreshNode],
  );

  const backup = useCallback(async () => {
    // A failed download leaves the error on the button until the next
    // attempt; a success needs no confirmation — the file is in the
    // browser's download folder.
    setBackingUp(true);
    setBackupError(null);
    try {
      const { blob, filename } = await apiDownload("/api/backup");
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = filename;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      URL.revokeObjectURL(url);
    } catch (error) {
      setBackupError(
        error instanceof Error ? error.message : "backup failed",
      );
    } finally {
      setBackingUp(false);
    }
  }, []);


  const forgetPeer = useCallback(
    async (destination: string, force = false) => {
      const result = await apiPost<ForgetPeerResponse>("/api/peers/forget", {
        target: destination,
        force,
      });
      // The forget already succeeded: drop the peer locally at once,
      // and treat the follow-up refresh as best-effort so a failed GET
      // cannot make a successful forget look like a failure.
      const removed = result.peer.destination;
      forgottenPeers.current.set(removed, result.peer.last_seen);
      setPeers((previous) =>
        previous.filter((entry) => entry.destination !== removed),
      );
      try {
        await refreshPeers();
      } catch {
        // The next snapshot reconciles.
      }
      return result.peer;
    },
    [refreshPeers],
  );

  const runDoctor = useCallback(async () => {
    // On demand only: the checks read the data directory, and a
    // periodic run would re-read files the node is writing to.
    return apiGet<DoctorResponse>("/api/doctor");
  }, []);

  const createGroup = useCallback(
    async (name: string, members: string[]) => {
      const result = await apiPost<CreateGroupResponse>("/api/groups", {
        name,
        members,
      });
      // The create already succeeded: add the group locally at once,
      // then refresh best-effort so a failed GET cannot hide it.
      const created = result.group;
      groupEdits.current += 1;
      setGroups((previous) =>
        previous.some((entry) => entry.id === created.id)
          ? previous.map((entry) => (entry.id === created.id ? created : entry))
          : [...previous, created],
      );
      try {
        await refreshGroups();
      } catch {
        // The next poll reconciles.
      }
      return result.group;
    },
    [refreshGroups],
  );

  const deleteGroup = useCallback(
    async (group: string) => {
      const result = await apiPost<DeleteGroupResponse>("/api/groups/delete", {
        group,
      });
      // Drop the group locally at once; the best-effort refresh below
      // must not be the only thing that removes it.
      const deletedId = result.deleted.id;
      groupEdits.current += 1;
      setGroups((previous) => previous.filter((entry) => entry.id !== deletedId));
      try {
        await refreshGroups();
      } catch {
        // The next poll reconciles.
      }
      return result.deleted;
    },
    [refreshGroups],
  );

  const changeGroupMembers = useCallback(
    async (group: string, members: string[], adding: boolean) => {
      const result = await apiPost<ChangeGroupMembersResponse>(
        adding ? "/api/groups/add-member" : "/api/groups/remove-member",
        { group, members },
      );
      // A roster change is a successor group: swap the old id for the
      // new one at once, then refresh best-effort so a failed GET
      // cannot leave the stale roster on screen.
      const updated = result.group;
      const previousId = result.previous_id;
      groupEdits.current += 1;
      setGroups((previous) => {
        // A snapshot may already list the successor: keep one entry per
        // id, placed where the old group was.
        const rest = previous.filter(
          (entry) => entry.id !== previousId && entry.id !== updated.id,
        );
        const index = previous.findIndex((entry) => entry.id === previousId);
        if (index === -1) return [...rest, updated];
        const before = previous
          .slice(0, index)
          .filter(
            (entry) => entry.id !== previousId && entry.id !== updated.id,
          ).length;
        return [...rest.slice(0, before), updated, ...rest.slice(before)];
      });
      // Same batch as the swap, so the console can move a selected old
      // id to its successor before the missing-target fallback runs.
      setGroupSwap({ from: previousId, to: updated.id });
      try {
        await refreshGroups();
      } catch {
        // The next poll reconciles.
      }
      return result;
    },
    [refreshGroups],
  );

  const addGroupMembers = useCallback(
    (group: string, members: string[]) =>
      changeGroupMembers(group, members, true),
    [changeGroupMembers],
  );

  const removeGroupMembers = useCallback(
    (group: string, members: string[]) =>
      changeGroupMembers(group, members, false),
    [changeGroupMembers],
  );

  const editBlock = useCallback(
    async (destination: string, block: boolean) => {
      setBlocks((previous) => ({
        ...previous,
        [destination]: { pending: true },
      }));
      const requestSeq = peerEventSeq.current;
      try {
        const result = await apiPost<BlockPeerResponse>(
          block ? "/api/peers/block" : "/api/peers/unblock",
          { target: destination },
        );
        // The bridge also publishes a peer event with the new flag, so
        // this local update is only the immediate feedback; the event
        // reconciles it for every open console. Skip it when a newer
        // peer event or snapshot arrived after the request was sent.
        const newestSeq = Math.max(
          peerRevision.current.get(destination) ?? 0,
          snapshotRevision.current,
        );
        if (newestSeq <= requestSeq) {
          setPeers((previous) =>
            previous.map((entry) =>
              entry.destination === destination
                ? { ...entry, blocked: result.block.blocked }
                : entry,
            ),
          );
          if (result.block.blocked) {
            // Same reconciliation as the peer-event path, for the
            // acting console before the event arrives.
            setMessages((previous) =>
              previous.filter(
                (entry) =>
                  !(entry.direction === "in" && entry.peer === destination),
              ),
            );
          }
        }
        setBlocks((previous) => ({
          ...previous,
          [destination]: {},
        }));
        return result.block;
      } catch (error) {
        setBlocks((previous) => ({
          ...previous,
          [destination]: {
            error: error instanceof Error ? error.message : "block failed",
          },
        }));
        throw error;
      }
    },
    [],
  );

  const blockPeer = useCallback(
    (destination: string) => editBlock(destination, true),
    [editBlock],
  );

  const unblockPeer = useCallback(
    (destination: string) => editBlock(destination, false),
    [editBlock],
  );

  // Showing the safety number records nothing: the peer stays
  // unverified and any key-change warning stays up until the owner
  // compares the number out of band and confirms it matches.
  const showSafetyNumber = useCallback(async (destination: string) => {
    setVerifications((previous) => ({
      ...previous,
      [destination]: { pending: true },
    }));
    try {
      const result = await apiPost<SafetyNumberResponse>(
        "/api/peers/safety-number",
        { target: destination },
      );
      setVerifications((previous) => ({
        ...previous,
        [destination]: {
          safetyNumber: result.safety_number.safety_number,
        },
      }));
      return result.safety_number;
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

  // Confirm step: only call after the owner compared the shown number
  // out of band and found it matches. This is what records the
  // verification and clears any key-change warning.
  const verifyPeer = useCallback(async (destination: string) => {
    setVerifications((previous) => ({
      ...previous,
      [destination]: {
        pending: true,
        safetyNumber: previous[destination]?.safetyNumber,
      },
    }));
    try {
      const result = await apiPost<VerifyPeerResponse>("/api/peers/verify", {
        target: destination,
      });
      const verify = result.verify;
      localVerifiedAt.current[destination] = verify.verified_at;
      setVerifications((previous) => ({
        ...previous,
        [destination]: { result: verify },
      }));
      // The badge and warning update at once instead of waiting for
      // the next snapshot: the peer is verified now, and any earlier
      // key-change warning about it is answered.
      setPeers((previous) =>
        previous.map((peer) =>
          peer.destination === destination
            ? {
                ...peer,
                verified: true,
                verified_at: verify.verified_at,
                key_change_at: null,
              }
            : peer,
        ),
      );
      return verify;
    } catch (error) {
      setVerifications((previous) => ({
        ...previous,
        [destination]: {
          safetyNumber: previous[destination]?.safetyNumber,
          error: error instanceof Error ? error.message : "verify failed",
        },
      }));
      throw error;
    }
  }, []);

  const clearVerification = useCallback((destination: string) => {
    setVerifications((previous) => {
      const next = { ...previous };
      delete next[destination];
      return next;
    });
  }, []);
  const markRead = useCallback(async (peer?: string) => {
    // Opening a conversation advances its read cursor on the node, so
    // the badge clears. A failed call leaves the badge in place; the
    // next snapshot reconciles.
    const result = await apiPost<MarkReadResponse>(
      "/api/messages/read",
      peer ? { peer } : {},
    );
    applyUnread(result.unread);
    return result.unread;
  }, [applyUnread]);

  const exportBundle = useCallback(async (includeTan: boolean) => {
    return apiPost<BundleExportResponse>("/api/bundle/export", {
      include_tan: includeTan,
    });
  }, []);

  const importBundle = useCallback(async (documentText: string) => {
    // The file's text goes to the server verbatim: the bundle is
    // signed, and a JSON parse/stringify round trip here can rewrite
    // a number the signature covers.
    return apiPostText<BundleImportResponse>("/api/bundle/import", documentText);
  }, []);

  const setPeerAlias = useCallback(
    async (destination: string, alias: string | null) => {
      const versionBefore = peerEventVersion.current[destination] ?? 0;
      const snapshotBefore = snapshotGeneration.current;
      const result = await apiPost<{ ok: boolean; peer: Peer }>(
        "/api/peers/alias",
        { destination, alias },
      );
      // The server also publishes a peer event, so other tabs learn at
      // once; apply the returned record now so this edit reflects
      // without waiting for it. Skip it if a peer event for this peer
      // arrived meanwhile, or a reconnect snapshot replaced the list:
      // either one is newer than this response.
      if (
        (peerEventVersion.current[destination] ?? 0) === versionBefore &&
        snapshotGeneration.current === snapshotBefore
      ) {
        setPeers((previous) =>
          sortPeers([
            ...previous.filter((entry) => entry.destination !== destination),
            result.peer,
          ]),
        );
      }
      return result.peer;
    },
    [],
  );

  // Re-read the console feed. The merge never drops a message: a live
  // message that arrived after this GET started is already in state,
  // and a stale response must not overwrite it — the SSE stream stays
  // the live source, this only picks up entries the stream never
  // announced (imported messages mirrored into the log).
  const refreshMessages = useCallback(async () => {
    const startEpoch = liveEpoch.current;
    const result = await apiGet<MessagesResponse>("/api/messages?limit=200");
    setMessages((previous) => {
      const byId = new Map(previous.map((entry) => [entry.id, entry]));
      for (const entry of result.messages) {
        // A live update that landed after this GET started is newer than
        // the response, which may predate it: keep the live version.
        if ((liveTouched.current.get(entry.id) ?? 0) > startEpoch) continue;
        // A deletion that landed while this GET was in flight is already
        // in removedMessageIds: skip the entry, or the stale response
        // would put a retracted message back in the feed.
        if (removedMessageIds.current.has(entry.id)) continue;
        byId.set(entry.id, entry);
      }
      const merged = [...byId.values()].sort((a, b) => a.id - b.id);
      snapshotMax.current = Math.max(
        snapshotMax.current,
        merged.length > 0 ? merged[merged.length - 1].id : 0,
      );
      return merged;
    });
  }, []);

  // Download the export document the bridge returns. The anchor
  // click is the download: a blob URL keeps the file local, and the
  // revoke is deferred a beat so the browser has started the save.
  const exportMessages = useCallback(async () => {
    setExporting(true);
    try {
      const doc =
        await apiGet<MessagesExportDocument>("/api/messages/export");
      const blob = new Blob([`${JSON.stringify(doc, null, 2)}\n`], {
        type: "application/json",
      });
      const url = URL.createObjectURL(blob);
      try {
        const anchor = document.createElement("a");
        anchor.href = url;
        anchor.download = "tau-messages-export.json";
        anchor.click();
      } finally {
        setTimeout(() => URL.revokeObjectURL(url), 1000);
      }
      return doc;
    } finally {
      setExporting(false);
    }
  }, []);

  const importMessages = useCallback(
    async (doc: MessagesExportDocument): Promise<MessagesImportResult> => {
      setImporting(true);
      try {
        const result = await apiPost<MessagesImportResponse>(
          "/api/messages/import",
          doc as unknown as Record<string, unknown>,
        );
        // The entries are already stored once the POST succeeds, so a
        // feed refresh failure must not turn the import into a reported
        // failure.
        // Restored mail can advance unread counts; apply the fresh ones so
        // the badge (and an open conversation's mark-read) follows.
        if (result.import.unread) applyUnread(result.import.unread);
        try {
          await refreshMessages();
        } catch {
          // The next snapshot reconciles.
        }
        return result.import;
      } finally {
        setImporting(false);
      }
    },
    [refreshMessages, applyUnread],
  );

  const reactToMessage = useCallback(
    async (target: string, reaction: string) => {
      // The node records the outgoing reaction and the bridge re-sends
      // the message as `message_updated`, so the bubble updates through
      // the SSE stream — no local state change here.
      return apiPost<ReactResponse>("/api/react", { target, reaction });
    },
    [],
  );

  const clearReaction = useCallback(async (target: string) => {
    return apiPost<ReactResponse>("/api/react", { target, clear: true });
  }, []);

  // A search is a view switch, like the Tan search: setting the query
  // fetches the search endpoint instead of showing the live feed.
  // Clearing it returns to the live traffic the same way.
  const clearSearch = useCallback(() => {
    // Invalidate any in-flight search so its late response is discarded.
    searchSeq.current += 1;
    setSearchQuery(null);
    setSearchResults([]);
    setSearchError(null);
  }, []);

  const clearPinned = useCallback(() => {
    // Invalidate any in-flight pinned fetch so its late response is
    // discarded.
    pinnedSeq.current += 1;
    setPinnedMessages(null);
    setPinnedError(null);
  }, []);

  const runSearch = useCallback(async (trimmed: string) => {
    // Each request takes a sequence number; a response only applies if it
    // is still the latest request, so superseded or cleared searches
    // cannot overwrite the active view.
    const seq = ++searchSeq.current;
    try {
      const result = await apiGet<MessagesSearchResponse>(
        `/api/messages/search?q=${encodeURIComponent(trimmed)}&limit=100`,
      );
      if (seq !== searchSeq.current) return;
      setSearchResults(
        result.messages.filter(
          (entry) => !removedMessageIds.current.has(entry.id),
        ),
      );
      setSearchError(null);
    } catch (caught) {
      if (seq !== searchSeq.current) return;
      setSearchError(
        caught instanceof Error ? caught.message : "search failed",
      );
    }
  }, []);

  useEffect(() => {
    runSearchRef.current = runSearch;
  }, [runSearch]);

  const search = useCallback(
    async (query: string) => {
      const trimmed = query.trim();
      if (!trimmed) return;
      // Opening a search leaves the pinned view: the two switches are
      // exclusive, and a stale pinned list must not survive underneath.
      clearPinned();
      if (trimmed === searchQueryRef.current) {
        // Resubmitting the current query: setting the same state would
        // bail out without reloading, so refresh directly — and keep
        // the current results instead of clearing them, or the feed
        // would wrongly report no matches until the next message.
        await runSearch(trimmed);
        return;
      }
      // Drop hits from the previous query so they are not shown as matches
      // for the new one while its request is in flight.
      setSearchResults([]);
      setSearchError(null);
      setSearchQuery(trimmed);
      await runSearch(trimmed);
    },
    [runSearch, clearPinned],
  );

  // A pinned view is a view switch, like the Tan search: opening it
  // fetches the pinned endpoint instead of showing the live feed, and
  // clearing it returns to the live traffic. The two switches are
  // exclusive — a search and the pinned view never show at once.
  const showPinned = useCallback(async () => {
    const seq = ++pinnedSeq.current;
    try {
      let result: MessagesResponse;
      let events: number;
      do {
        events = pinnedEvents.current;
        result = await apiGet<MessagesResponse>("/api/messages?pinned=1");
        // A pin, unpin, or deletion that landed while this GET was in
        // flight may be missing from it: fetch again rather than apply
        // it over the event.
      } while (seq === pinnedSeq.current && events !== pinnedEvents.current);
      if (seq !== pinnedSeq.current) return;
      // The endpoint returns oldest first; the view shows newest first,
      // like search results. A delete that landed after the final GET
      // returned is already in removedMessageIds, so filter the
      // snapshot against it — otherwise the late set would reinstate
      // an entry the delete event already dropped.
      setPinnedMessages(
        [...result.messages]
          .filter((entry) => !removedMessageIds.current.has(entry.id))
          .reverse(),
      );
      setPinnedError(null);
      clearSearch();
    } catch (caught) {
      if (seq !== pinnedSeq.current) return;
      setPinnedError(
        caught instanceof Error ? caught.message : "pinned view failed",
      );
    }
  }, [clearSearch]);

  // Only an open pinned view needs the reconnect re-read.
  useEffect(() => {
    refreshPinnedView.current =
      pinnedMessages === null
        ? null
        : () => {
            void showPinned().catch(() => undefined);
          };
  }, [pinnedMessages, showPinned]);

  const acceptDmRequest = useCallback(
    async (id: string) => {
      const result = await apiPost<AcceptDmRequestResponse>(
        "/api/messages/requests/accept",
        { id },
      );
      // The accept already succeeded: drop the request locally at
      // once, and treat the follow-up refreshes as best-effort so a
      // failed GET cannot make a successful accept look like a
      // failure. The delivered message lands in the feed, so the
      // messages refresh runs too.
      const acceptedId = result.accepted.id;
      setDmRequests((previous) =>
        previous.filter((entry) => entry.id !== acceptedId),
      );
      try {
        await refreshDmRequests();
      } catch {
        // The next poll reconciles.
      }
      try {
        await refreshMessages();
      } catch {
        // The next poll reconciles.
      }
      try {
        // The accept changed the running node's allow-list: refresh
        // it too, or the panel keeps showing the old list without
        // the new sender until the next poll.
        await refreshAllowlist();
      } catch {
        // The next poll reconciles.
      }
      return result;
    },
    [refreshDmRequests, refreshMessages, refreshAllowlist],
  );

  const declineDmRequest = useCallback(
    async (id: string) => {
      const result = await apiPost<DeclineDmRequestResponse>(
        "/api/messages/requests/decline",
        { id },
      );
      // Same shape as the accept: the decline already succeeded, so
      // drop the request locally at once and refresh best-effort.
      const declinedId = result.declined.id;
      setDmRequests((previous) =>
        previous.filter((entry) => entry.id !== declinedId),
      );
      try {
        await refreshDmRequests();
      } catch {
        // The next poll reconciles.
      }
      return result;
    },
    [refreshDmRequests],
  );

  return {
    node,
    peers,
    groups,
    messages,
    searchResults,
    searchQuery,
    searchError,
    search,
    clearSearch,
    pinnedMessages,
    pinnedError,
    showPinned,
    clearPinned,
    pinMessage,
    unpinMessage,
    unread,
    outbox,
    heldForOthers,
    dmRequests,
    acceptDmRequest,
    declineDmRequest,
    refreshDmRequests,
    settings,
    settingsError,
    heldMail,
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
    send,
    deleteMessage,
    editMessage,
    retractMessage,
    forwardMessage,
    sendGroup,
    sendFile,
    ping,
    fetchInfo,
    showSafetyNumber,
    verifyPeer,
    clearVerification,
    blockPeer,
    unblockPeer,
    announce,
    renameNode,
    backup,
    runDoctor,
    markRead,
    setPeerAlias,
    reactToMessage,
    clearReaction,
    createGroup,
    deleteGroup,
    addGroupMembers,
    removeGroupMembers,
    refreshOutbox,
    refreshGroups,
    groupSwap,
    refreshSettings,
    updateSetting,
    refreshPeers,
    refreshMessages,
    cancelOutbox,
    forgetPeer,
    exportBundle,
    importBundle,
    exportMessages,
    importMessages,
    retryOutbox,
    refreshHeldMail,
    dropHeldMail,
    refreshAllowlist,
    addAllowlistPeer,
    removeAllowlistPeer,
    setAllowlistEnabled,
  };
}
