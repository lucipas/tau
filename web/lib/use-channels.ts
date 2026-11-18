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
  getChannelMessages,
  getChannelTopic,
  getChannels,
  joinChannel,
  leaveChannel,
  sendChannelMessage,
  setChannelTopic,
  syncChannels,
} from "./api";
import type { Channel, ChannelMessage, ChannelTopic } from "./types";

export interface UseChannelsResult {
  channels: Channel[];
  /** Selected channel name, or null. */
  selected: string | null;
  select: (channel: string | null) => void;
  messages: ChannelMessage[];
  /** Current topic of the selected channel, or null when unset. */
  topic: ChannelTopic | null;
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
  join: (channel: string) => Promise<void>;
  leave: (channel: string) => Promise<void>;
  send: (text: string) => Promise<ChannelMessage>;
  setTopic: (text: string) => Promise<void>;
  clearTopic: () => Promise<void>;
  sync: (channel?: string) => Promise<void>;
}

export function useChannels(pollMs = 30_000): UseChannelsResult {
  const [channels, setChannels] = useState<Channel[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [messages, setMessages] = useState<ChannelMessage[]>([]);
  const [topic, setTopicState] = useState<ChannelTopic | null>(null);
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
      return;
    }
    try {
      setMessages(await getChannelMessages(channel));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, []);

  const refreshTopic = useCallback(async (channel: string | null) => {
    if (!channel) {
      setTopicState(null);
      return;
    }
    try {
      setTopicState(await getChannelTopic(channel));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, []);

  const refresh = useCallback(async () => {
    await refreshChannels();
    await refreshMessages(selected);
    await refreshTopic(selected);
    setLoading(false);
  }, [refreshChannels, refreshMessages, refreshTopic, selected]);

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
    void refreshTopic(channel);
  }, [refreshMessages, refreshTopic]);

  return {
    channels,
    selected,
    select,
    messages,
    topic,
    loading,
    error,
    refresh,
    join: async (channel) => {
      await joinChannel(channel);
      await refreshChannels();
      setSelected(channel);
      await refreshMessages(channel);
      await refreshTopic(channel);
    },
    leave: async (channel) => {
      await leaveChannel(channel);
      await refreshChannels();
      if (selected === channel) {
        setSelected(null);
        setMessages([]);
        setTopicState(null);
      }
    },
    send: async (text) => {
      if (!selected) throw new Error("no channel selected");
      const sent = await sendChannelMessage(selected, text);
      await refreshMessages(selected);
      return sent;
    },
    setTopic: async (text) => {
      if (!selected) throw new Error("no channel selected");
      await setChannelTopic(selected, text);
      await refreshTopic(selected);
    },
    clearTopic: async () => {
      if (!selected) throw new Error("no channel selected");
      await setChannelTopic(selected, "");
      await refreshTopic(selected);
    },
    sync: async (channel) => {
      await syncChannels(channel ?? selected ?? undefined);
      await refreshMessages(selected);
      await refreshTopic(selected);
    },
  };
}
