"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { apiGet, apiPost } from "./api";
import type {
  Channel,
  ChannelJoinResponse,
  ChannelMessage,
  ChannelMessagesResponse,
  ChannelSendResponse,
  ChannelSyncResponse,
  ChannelSyncSummary,
  ChannelsResponse,
} from "./types";

// The history poll cadence, matching the Tan feed: a channel a peer
// posted to shows up within a few seconds even without a live event.
export const CHANNEL_POLL_INTERVAL_MS = 5000;
// History page size: the API caps at 50, and the console shows the
// newest page oldest-first.
const CHANNEL_HISTORY_LIMIT = 50;

function errorMessage(caught: unknown): string {
  return caught instanceof Error ? caught.message : "request failed";
}

export function useChannels(liveMessage: ChannelMessage | null) {
  const [channels, setChannels] = useState<Channel[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  // History for the selected channel, oldest first for display. The
  // API returns newest first, so loads reverse the page.
  const [messages, setMessages] = useState<ChannelMessage[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sending, setSending] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [joining, setJoining] = useState(false);
  const selectedRef = useRef<string | null>(null);
  useEffect(() => {
    selectedRef.current = selected;
  }, [selected]);
  // A newer refresh supersedes an older one, so a slow history page
  // never overwrites a channel switch that already happened.
  const generationRef = useRef(0);

  const loadMessages = useCallback(async (channel: string) => {
    const generation = ++generationRef.current;
    const response = await apiGet<ChannelMessagesResponse>(
      `/api/channels/messages?channel=${encodeURIComponent(channel)}&limit=${CHANNEL_HISTORY_LIMIT}`,
    );
    if (generation !== generationRef.current) return;
    setMessages([...response.messages].reverse());
  }, []);

  const loadChannels = useCallback(async () => {
    const response = await apiGet<ChannelsResponse>("/api/channels");
    const joined = response.channels;
    setChannels(joined);
    const current = selectedRef.current;
    if (current !== null && joined.some((entry) => entry.channel === current)) {
      return current;
    }
    const next = joined.length > 0 ? joined[0].channel : null;
    setSelected(next);
    return next;
  }, []);

  const refresh = useCallback(async () => {
    const current = await loadChannels();
    if (current !== null) {
      await loadMessages(current);
    } else {
      setMessages([]);
    }
  }, [loadChannels, loadMessages]);

  // Initial load, then the poll cadence. A failed refresh surfaces
  // once and retries on the next tick instead of clearing the list.
  useEffect(() => {
    let disposed = false;
    refresh()
      .catch((caught: unknown) => {
        if (!disposed) setError(errorMessage(caught));
      })
      .finally(() => {
        if (!disposed) setLoading(false);
      });
    const timer = setInterval(() => {
      refresh().catch((caught: unknown) => {
        if (!disposed) setError(errorMessage(caught));
      });
    }, CHANNEL_POLL_INTERVAL_MS);
    return () => {
      disposed = true;
      clearInterval(timer);
    };
  }, [refresh]);

  // A live channel-message for the open channel appends at the end,
  // where the newest message belongs. The poll tick is the backstop:
  // it re-reads history, so a dropped event never loses a message.
  useEffect(() => {
    if (!liveMessage) return;
    if (liveMessage.channel !== selectedRef.current) return;
    setMessages((previous) => {
      if (previous.some((entry) => entry.hash === liveMessage.hash)) {
        return previous;
      }
      return [...previous, liveMessage];
    });
  }, [liveMessage]);

  const select = useCallback(
    (channel: string | null) => {
      setSelected(channel);
      selectedRef.current = channel;
      setError(null);
      if (channel === null) {
        setMessages([]);
        return;
      }
      loadMessages(channel).catch((caught: unknown) => {
        setError(errorMessage(caught));
      });
    },
    [loadMessages],
  );

  const join = useCallback(
    async (name: string) => {
      const channel = name.trim();
      if (channel === "") return;
      setJoining(true);
      setError(null);
      try {
        await apiPost<ChannelJoinResponse>("/api/channels/join", {
          channel,
        });
        await loadChannels();
        select(channel);
      } catch (caught: unknown) {
        setError(errorMessage(caught));
      } finally {
        setJoining(false);
      }
    },
    [loadChannels, select],
  );

  const leave = useCallback(
    async (channel: string) => {
      setError(null);
      try {
        await apiPost("/api/channels/leave", { channel });
        const current = await loadChannels();
        if (current === null) {
          setMessages([]);
        } else if (current !== selectedRef.current) {
          await loadMessages(current);
        }
      } catch (caught: unknown) {
        setError(errorMessage(caught));
      }
    },
    [loadChannels, loadMessages],
  );

  const send = useCallback(
    async (text: string) => {
      const channel = selectedRef.current;
      if (channel === null || text.trim() === "") return;
      setSending(true);
      setError(null);
      try {
        const response = await apiPost<ChannelSendResponse>(
          "/api/channels/send",
          { channel, text },
        );
        // The send reply carries the stored message: append it now
        // instead of waiting for the poll tick, and dedupe against
        // the live event and the next history page by hash.
        setMessages((previous) => {
          if (
            previous.some((entry) => entry.hash === response.message.hash)
          ) {
            return previous;
          }
          return [...previous, response.message];
        });
      } catch (caught: unknown) {
        setError(errorMessage(caught));
        throw caught;
      } finally {
        setSending(false);
      }
    },
    [],
  );

  const sync = useCallback(async (): Promise<ChannelSyncSummary | null> => {
    const channel = selectedRef.current;
    if (channel === null) return null;
    setSyncing(true);
    setError(null);
    try {
      const response = await apiPost<ChannelSyncResponse>(
        "/api/channels/sync",
        { channel },
      );
      await loadMessages(channel);
      return response.sync;
    } catch (caught: unknown) {
      setError(errorMessage(caught));
      return null;
    } finally {
      setSyncing(false);
    }
  }, [loadMessages]);

  return {
    channels,
    selected,
    messages,
    loading,
    error,
    sending,
    syncing,
    joining,
    select,
    join,
    leave,
    send,
    sync,
  };
}
