"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { apiGet, apiPost, eventsUrl } from "./api";
import type {
  BridgeInfo,
  CancelOutboxResponse,
  NodeInfo,
  OutboxEntry,
  OutboxResponse,
  Peer,
  PingResult,
  SendResponse,
  Snapshot,
  TauMessage,
} from "./types";

export type ConnectionState =
  | "connecting"
  | "live"
  | "reconnecting";

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
  const [outbox, setOutbox] = useState<OutboxEntry[]>([]);
  const [heldForOthers, setHeldForOthers] = useState(0);
  const [bridge, setBridge] = useState<BridgeInfo | null>(null);
  const [connection, setConnection] = useState<ConnectionState>("connecting");
  const [pings, setPings] = useState<Record<string, PingState>>({});
  const [announcing, setAnnouncing] = useState(false);
  const snapshotMax = useRef(0);

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
      setBridge(snapshot.bridge);
      setConnection("live");
    };

    const onMessage = (event: Event) => {
      const payload = readEventData<{ message: TauMessage }>(event);
      if (!payload?.message) return;
      const message = payload.message;
      if (message.id <= snapshotMax.current) return;
      snapshotMax.current = Math.max(snapshotMax.current, message.id);
      setMessages((previous) => {
        if (previous.some((entry) => entry.id === message.id)) return previous;
        return [...previous, message].sort((a, b) => a.id - b.id);
      });
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

    const onError = () => {
      if (!disposed) setConnection("reconnecting");
    };

    source.addEventListener("open", onOpen);
    source.addEventListener("snapshot", onSnapshot);
    source.addEventListener("message", onMessage);
    source.addEventListener("peer", onPeer);
    source.addEventListener("error", onError);

    return () => {
      disposed = true;
      source.removeEventListener("open", onOpen);
      source.removeEventListener("snapshot", onSnapshot);
      source.removeEventListener("message", onMessage);
      source.removeEventListener("peer", onPeer);
      source.removeEventListener("error", onError);
      source.close();
    };
  }, []);

  const refreshOutbox = useCallback(async () => {
    const result = await apiGet<OutboxResponse>("/api/outbox");
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
      await refreshOutbox();
      return result.cancelled;
    },
    [refreshOutbox],
  );

  const send = useCallback(async (to: string, text: string) => {
    const result = await apiPost<SendResponse>("/api/messages", { to, text });
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

  return {
    node,
    peers,
    messages,
    outbox,
    heldForOthers,
    bridge,
    connection,
    pings,
    announcing,
    send,
    ping,
    announce,
    refreshOutbox,
    cancelOutbox,
  };
}
