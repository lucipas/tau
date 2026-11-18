// use-channels.ts — public channels state for the Channels tab.
//
// Provides:
//   useChannels() — joined channels, per-channel message history,
//                   join/leave/send/sync. History is newest-first from
//                   /api/channels/messages (default 20, max 50).
// New channel messages also arrive as `channel-message` live events on
// /ws; this hook polls the open channel on an interval (default 30s) and
// after every mutation, so it stays correct when the socket is down.

"use client";

import { useCallback, useEffect, useState } from "react";
import {
  cancelScheduledChannelMessage,
  getChannelMessages,
  getChannelScheduled,
  getChannels,
  joinChannel,
  leaveChannel,
  scheduleChannelMessage,
  sendChannelMessage,
  syncChannels,
} from "./api";
import type { Channel, ChannelMessage, ScheduledChannelMessage } from "./types";

export interface UseChannelsResult {
  channels: Channel[];
  /** Selected channel name, or null. */
  selected: string | null;
  select: (channel: string | null) => void;
  messages: ChannelMessage[];
  /** Queued channel messages for the selected channel (soonest first). */
  scheduled: ScheduledChannelMessage[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
  join: (channel: string) => Promise<void>;
  leave: (channel: string) => Promise<void>;
  send: (text: string) => Promise<ChannelMessage>;
  schedule: (text: string, sendIn: number) => Promise<ScheduledChannelMessage>;
  cancelScheduled: (id: string) => Promise<void>;
  sync: (channel?: string) => Promise<void>;
}

export function useChannels(pollMs = 30_000): UseChannelsResult {
  const [channels, setChannels] = useState<Channel[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [messages, setMessages] = useState<ChannelMessage[]>([]);
  const [scheduled, setScheduled] = useState<ScheduledChannelMessage[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refreshChannels = useCallback(async () => {
    try {
      setChannels(await getChannels());
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, []);

  const refreshMessages = useCallback(async (channel: string | null) => {
    if (!channel) {
      setMessages([]);
      setScheduled([]);
      return;
    }
    try {
      setMessages(await getChannelMessages(channel));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
    try {
      setScheduled(await getChannelScheduled(channel));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, []);

  const refresh = useCallback(async () => {
    await refreshChannels();
    await refreshMessages(selected);
    setLoading(false);
  }, [refreshChannels, refreshMessages, selected]);

  useEffect(() => {
    void refresh();
    if (pollMs > 0) {
      const id = setInterval(() => void refresh(), pollMs);
      return () => clearInterval(id);
    }
    return undefined;
  }, [refresh, pollMs]);

  const select = useCallback((channel: string | null) => {
    setSelected(channel);
    void refreshMessages(channel);
  }, [refreshMessages]);

  return {
    channels,
    selected,
    select,
    messages,
    scheduled,
    loading,
    error,
    refresh,
    join: async (channel) => {
      await joinChannel(channel);
      await refreshChannels();
      setSelected(channel);
      await refreshMessages(channel);
    },
    leave: async (channel) => {
      await leaveChannel(channel);
      await refreshChannels();
      if (selected === channel) {
        setSelected(null);
        setMessages([]);
        setScheduled([]);
      }
    },
    send: async (text) => {
      if (!selected) throw new Error("no channel selected");
      const sent = await sendChannelMessage(selected, text);
      await refreshMessages(selected);
      return sent;
    },
    schedule: async (text, sendIn) => {
      if (!selected) throw new Error("no channel selected");
      const entry = await scheduleChannelMessage(selected, text, sendIn);
      await refreshMessages(selected);
      return entry;
    },
    cancelScheduled: async (id) => {
      await cancelScheduledChannelMessage(id);
      await refreshMessages(selected);
    },
    sync: async (channel) => {
      await syncChannels(channel ?? selected ?? undefined);
      await refreshMessages(selected);
    },
  };
}
