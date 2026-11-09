"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { apiGet, apiPost, eventsUrl } from "./api";
import type {
  BridgeInfo,
  CancelOutboxResponse,
  MarkReadResponse,
  NodeInfo,
  OutboxEntry,
  OutboxResponse,
  Peer,
  PingResult,
  SendResponse,
  Snapshot,
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

export function useTau() {
  const [node, setNode] = useState<NodeInfo | null>(null);
  const [peers, setPeers] = useState<Peer[]>([]);
  const [messages, setMessages] = useState<TauMessage[]>([]);
  const [unread, setUnread] = useState<UnreadCounts>(EMPTY_UNREAD);
  const [outbox, setOutbox] = useState<OutboxEntry[]>([]);
  const [heldForOthers, setHeldForOthers] = useState(0);
  const [bridge, setBridge] = useState<BridgeInfo | null>(null);
  const [connection, setConnection] = useState<ConnectionState>("connecting");
  const [pings, setPings] = useState<Record<string, PingState>>({});
  const [verifications, setVerifications] = useState<
    Record<string, VerifyState>
  >({});
  const [announcing, setAnnouncing] = useState(false);
  const snapshotMax = useRef(0);
  const unreadRevision = useRef(-1);

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
    const source = new EventSource(eventsUrl(0));

    const onOpen = () => {
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
      setPeers(sortPeers(snapshot.peers));
      setMessages(snapshot.messages);
      // A snapshot is authoritative for its connection: reset the
      // revision baseline so a delayed event from the old connection
      // cannot overwrite it.
      applyUnread(snapshot.unread ?? EMPTY_UNREAD, true);
      setBridge(snapshot.bridge);
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
      // A queued message that the mail sweep has since delivered: the
      // server re-sends the same console id with status "delivered".
      // Replace it in place rather than appending a duplicate.
      setMessages((previous) =>
        previous.map((entry) => (entry.id === message.id ? message : entry)),
      );
    };

    const onPeer = (event: Event) => {
      const payload = readEventData<{ peer: Peer }>(event);
      if (!payload?.peer) return;
      const peer = payload.peer;
      setPeers((previous) => {
        const next = previous.filter(
          (entry) => entry.destination !== peer.destination,
        );
        next.push(peer);
        return sortPeers(next);
      });
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
    source.addEventListener("peer", onPeer);
    source.addEventListener("unread", onUnread);
    source.addEventListener("error", onError);

    return () => {
      disposed = true;
      source.removeEventListener("open", onOpen);
      source.removeEventListener("snapshot", onSnapshot);
      source.removeEventListener("message", onMessage);
      source.removeEventListener("message_updated", onMessageUpdated);
      source.removeEventListener("peer", onPeer);
      source.removeEventListener("unread", onUnread);
      source.removeEventListener("error", onError);
      source.close();
    };
  }, [applyUnread]);

  // The unread total rides on the tab title, like a mail client.
  useEffect(() => {
    document.title =
      unread.total > 0
        ? `(${unread.total}) tau · mesh console`
        : "tau · mesh console";
  }, [unread.total]);

  const refreshSeq = useRef(0);

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

  useEffect(() => {
    if (connection !== "live") return;
    let disposed = false;
    async function tick() {
      try {
        await refreshOutbox();
      } catch {
        // The outbox is a side panel: a failed poll leaves the last
        // known entries in place instead of flashing an error.
        if (disposed) return;
      }
    }
    void tick();
    const timer = setInterval(() => void tick(), 15000);
    return () => {
      disposed = true;
      clearInterval(timer);
    };
  }, [connection, refreshOutbox]);

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

  const verifyPeer = useCallback(async (destination: string) => {
    setVerifications((previous) => ({
      ...previous,
      [destination]: { pending: true },
    }));
    try {
      const result = await apiPost<VerifyPeerResponse>("/api/peers/verify", {
        target: destination,
      });
      const verify = result.verify;
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
          error: error instanceof Error ? error.message : "verify failed",
        },
      }));
      throw error;
    }
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

  return {
    node,
    peers,
    messages,
    unread,
    outbox,
    heldForOthers,
    bridge,
    connection,
    pings,
    verifications,
    announcing,
    send,
    ping,
    verifyPeer,
    announce,
    markRead,
    refreshOutbox,
    cancelOutbox,
  };
}
