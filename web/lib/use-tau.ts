"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { apiGet, apiPost, apiPostText, eventsUrl } from "./api";
import type {
  AllowlistResponse,
  BlockPeerResponse,
  BridgeInfo,
  BundleExportResponse,
  BundleImportResponse,
  CancelOutboxResponse,
  CreateGroupResponse,
  DeleteGroupResponse,
  DeleteMessageResponse,
  DoctorResponse,
  DropHeldResponse,
  ForgetPeerResponse,
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
  OutboxEntry,
  OutboxResponse,
  Peer,
  PingResult,
  ReactResponse,
  RetryOutboxResponse,
  SafetyNumberResponse,
  SendFileResponse,
  SendGroupResponse,
  SendResponse,
  Snapshot,
  SocialInfo,
  TauGroup,
  TauMessage,
  UnreadCounts,
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

export interface BlockState {
  pending?: boolean;
  error?: string;
}

export interface ForgetState {
  pending?: boolean;
  error?: string;
  // The refusal was the queued-mail guard, which the owner can
  // override with force — the pending-acks refusal cannot be forced,
  // so it never sets this.
  canForce?: boolean;
}

function sortPeers(peers: Peer[]): Peer[] {
  return [...peers].sort((a, b) => b.last_seen - a.last_seen);
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

export function useTau() {
  const [node, setNode] = useState<NodeInfo | null>(null);
  const [peers, setPeers] = useState<Peer[]>([]);
  const [groups, setGroups] = useState<TauGroup[]>([]);
  const [messages, setMessages] = useState<TauMessage[]>([]);
  // The search the feed is showing, or null for the live traffic. Kept
  // beside `messages` so a search result list is never mixed into the
  // live feed: new arrivals keep updating `messages` underneath, and
  // clearing the search returns to it.
  const [searchResults, setSearchResults] = useState<TauMessage[]>([]);
  const [searchQuery, setSearchQuery] = useState<string | null>(null);
  const [searchError, setSearchError] = useState<string | null>(null);
  const [unread, setUnread] = useState<UnreadCounts>(EMPTY_UNREAD);
  const [outbox, setOutbox] = useState<OutboxEntry[]>([]);
  const [heldForOthers, setHeldForOthers] = useState(0);
  const [heldMail, setHeldMail] = useState<HeldMailEntry[]>([]);
  const [allowlist, setAllowlist] = useState<AllowlistResponse | null>(null);
  const [bridge, setBridge] = useState<BridgeInfo | null>(null);
  const [social, setSocial] = useState<SocialInfo | null>(null);
  const [connection, setConnection] = useState<ConnectionState>("connecting");
  const [pings, setPings] = useState<Record<string, PingState>>({});
  const [verifications, setVerifications] = useState<
    Record<string, VerifyState>
  >({});
  const [blocks, setBlocks] = useState<Record<string, BlockState>>({});
  const [forgets, setForgets] = useState<Record<string, ForgetState>>({});
  const [announcing, setAnnouncing] = useState(false);
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
      setNode(snapshot.node);
      peerEventSeq.current += 1;
      snapshotRevision.current = peerEventSeq.current;
      localVerifiedAt.current = {};
      setPeers(sortPeers(snapshot.peers));
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
      // A queued message that the mail sweep has since delivered: the
      // server re-sends the same console id with status "delivered".
      // Replace it in place rather than appending a duplicate.
      setMessages((previous) =>
        previous.map((entry) => (entry.id === message.id ? message : entry)),
      );
    };

    const onMessageDeleted = (event: Event) => {
      // The bus stamps its own sequence into `id`, so the deleted
      // console id arrives under `message_id` instead.
      const payload = readEventData<{ message_id: number }>(event);
      if (!payload || typeof payload.message_id !== "number") return;
      setMessages((previous) =>
        previous.filter((entry) => entry.id !== payload.message_id),
      );
    };

    const onPeer = (event: Event) => {
      const payload = readEventData<{ peer: Peer }>(event);
      if (!payload?.peer) return;
      const peer = payload.peer;
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

    const onPeerRemoved = (event: Event) => {
      // A forget from any console: drop the peer at once. A later
      // genuine announce re-adds it through the normal peer event.
      const payload = readEventData<{ destination: string }>(event);
      if (!payload?.destination) return;
      setPeers((previous) =>
        previous.filter(
          (entry) => entry.destination !== payload.destination,
        ),
      );
    };

    const onUnread = (event: Event) => {
      const payload = readEventData<{ unread: UnreadCounts }>(event);
      if (!payload?.unread) return;
      applyUnread(payload.unread);
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
    source.addEventListener("peer_removed", onPeerRemoved);
    source.addEventListener("unread", onUnread);
    source.addEventListener("error", onError);

    return () => {
      disposed = true;
      source.removeEventListener("open", onOpen);
      source.removeEventListener("snapshot", onSnapshot);
      source.removeEventListener("message", onMessage);
      source.removeEventListener("message_updated", onMessageUpdated);
      source.removeEventListener("message_deleted", onMessageDeleted);
      source.removeEventListener("peer", onPeer);
      source.removeEventListener("peer_removed", onPeerRemoved);
      source.removeEventListener("unread", onUnread);
      source.removeEventListener("error", onError);
      source.close();
    };
  }, [applyUnread, refreshGroups]);

  // The unread total rides on the tab title, like a mail client.
  useEffect(() => {
    document.title =
      unread.total > 0
        ? `(${unread.total}) tau · mesh console`
        : "tau · mesh console";
  }, [unread.total]);

  const refreshSeq = useRef(0);
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

  useEffect(() => {
    if (connection !== "live") return;
    async function tick() {
      // The three endpoints are independent: a failure on one must not
      // stop the others from updating their state. The outbox is a side
      // panel: a failed poll leaves the last known entries in place
      // instead of flashing an error.
      await Promise.allSettled([
        refreshOutbox(),
        refreshHeldMail(),
        refreshGroups(),
      ]);
    }
    void tick();
    const timer = setInterval(() => void tick(), 15000);
    return () => {
      clearInterval(timer);
    };
  }, [connection, refreshOutbox, refreshHeldMail, refreshGroups]);

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

  const refreshAllowlist = useCallback(async () => {
    const requestId = ++allowlistSeq.current;
    const result = await apiGet<AllowlistResponse>("/api/allowlist");
    applyAllowlist(requestId, result);
  }, [applyAllowlist]);

  // The allow-list only changes through this console or the CLI, and
  // CLI changes do not apply to the running node until it restarts —
  // so a load on connect plus a refresh after each action is enough;
  // no polling interval like the outbox needs.
  useEffect(() => {
    if (connection !== "live") return;
    let disposed = false;
    async function load() {
      try {
        await refreshAllowlist();
      } catch {
        if (disposed) return;
      }
    }
    void load();
    return () => {
      disposed = true;
    };
  }, [connection, refreshAllowlist]);

  const addAllowlistPeer = useCallback(
    async (target: string) => {
      const requestId = ++allowlistSeq.current;
      const result = await apiPost<AllowlistResponse>("/api/allowlist/add", {
        target,
      });
      applyAllowlist(requestId, result);
      return result;
    },
    [applyAllowlist],
  );

  const removeAllowlistPeer = useCallback(
    async (target: string) => {
      const requestId = ++allowlistSeq.current;
      const result = await apiPost<AllowlistResponse>("/api/allowlist/remove", {
        target,
      });
      applyAllowlist(requestId, result);
      return result;
    },
    [applyAllowlist],
  );

  const setAllowlistEnabled = useCallback(
    async (enabled: boolean) => {
      const requestId = ++allowlistSeq.current;
      const result = await apiPost<AllowlistResponse>(
        "/api/allowlist/set-enabled",
        { enabled },
      );
      applyAllowlist(requestId, result);
      return result;
    },
    [applyAllowlist],
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
    return result.deleted;
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
    async (to: string, text: string, expiresIn?: number, replyTo?: string) => {
      const body: {
        to: string;
        text: string;
        expires_in?: number;
        reply_to?: string;
      } = { to, text };
      if (expiresIn !== undefined) body.expires_in = expiresIn;
      if (replyTo) body.reply_to = replyTo;
      const result = await apiPost<SendResponse>("/api/messages", body);
      setMessages((previous) => {
      if (previous.some((entry) => entry.id === result.message.id)) {
        return previous;
      }
      return [...previous, result.message].sort((a, b) => a.id - b.id);
    });
    snapshotMax.current = Math.max(snapshotMax.current, result.message.id);
    // A queued send lands in the outbox; refresh so it shows at once
    // instead of waiting for the next poll. Best-effort: a failed
    // refresh must not report a successful send as failed.
    try {
      await refreshOutbox();
    } catch {
      // The next poll picks the entry up.
    }
    return result.message;
  }, [refreshOutbox]);

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
    setMessages((previous) => {
      if (previous.some((entry) => entry.id === result.message.id)) {
        return previous;
      }
      return [...previous, result.message].sort((a, b) => a.id - b.id);
    });
    snapshotMax.current = Math.max(snapshotMax.current, result.message.id);
    return result.message;
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

  const announce = useCallback(async () => {
    setAnnouncing(true);
    try {
      await apiPost<{ ok: boolean }>("/api/announce", {});
    } finally {
      setAnnouncing(false);
    }
  }, []);

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

  const forgetPeer = useCallback(
    async (destination: string, force = false) => {
      setForgets((previous) => ({
        ...previous,
        [destination]: { pending: true },
      }));
      const requestSeq = peerEventSeq.current;
      try {
        const result = await apiPost<ForgetPeerResponse>(
          "/api/peers/forget",
          { target: destination, force },
        );
        // The bridge also publishes a peer_removed event, so this local
        // drop is only the immediate feedback; the event reconciles it
        // for every open console. Skip it when a newer peer event or
        // snapshot arrived after the request was sent — a genuine
        // announce in that window re-added the peer.
        const newestSeq = Math.max(
          peerRevision.current.get(destination) ?? 0,
          snapshotRevision.current,
        );
        if (newestSeq <= requestSeq) {
          setPeers((previous) =>
            previous.filter((entry) => entry.destination !== destination),
          );
        }
        setForgets((previous) => ({ ...previous, [destination]: {} }));
        return result.forget;
      } catch (error) {
        const message =
          error instanceof Error ? error.message : "forget failed";
        setForgets((previous) => ({
          ...previous,
          [destination]: {
            error: message,
            // The bridge's queued-mail refusal names the guard; the
            // pending-acks refusal does not, and force never overrides it.
            canForce: message.includes("queued message"),
          },
        }));
        throw error;
      }
    },
    [],
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
      setSearchResults(result.messages);
      setSearchError(null);
    } catch (caught) {
      if (seq !== searchSeq.current) return;
      setSearchError(
        caught instanceof Error ? caught.message : "search failed",
      );
    }
  }, []);

  const search = useCallback(
    async (query: string) => {
      const trimmed = query.trim();
      if (!trimmed) return;
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
    [runSearch],
  );

  const clearSearch = useCallback(() => {
    // Invalidate any in-flight search so its late response is discarded.
    searchSeq.current += 1;
    setSearchQuery(null);
    setSearchResults([]);
    setSearchError(null);
  }, []);

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
    unread,
    outbox,
    heldForOthers,
    heldMail,
    allowlist,
    bridge,
    social,
    connection,
    pings,
    verifications,
    blocks,
    forgets,
    announcing,
    exporting,
    importing,
    send,
    deleteMessage,
    sendGroup,
    sendFile,
    ping,
    showSafetyNumber,
    verifyPeer,
    clearVerification,
    blockPeer,
    unblockPeer,
    forgetPeer,
    announce,
    runDoctor,
    markRead,
    setPeerAlias,
    reactToMessage,
    clearReaction,
    createGroup,
    deleteGroup,
    refreshOutbox,
    refreshGroups,
    refreshMessages,
    cancelOutbox,
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
