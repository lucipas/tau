// use-channels.ts — public channels state for the Channels tab.
//
// Provides:
//   useChannels() — joined channels, per-channel message history,
//                   join/leave/send/sync/search. History is newest-first from
//                   /api/channels/messages (default 20, max 50); search
//                   matches come from /api/channels/search and replace the
//                   history view until cleared.
// New channel messages also arrive as `channel-message` live events on
// /ws; this hook polls the open channel on an interval (default 30s) and
// after every mutation, so it stays correct when the socket is down.

"use client";

import { useCallback, useEffect, useState } from "react";
import {
  cancelScheduledChannelMessage,
  closeChannelPoll,
  editChannelMessage,
  getChannelMessages,
  getChannelPins,
  getChannelScheduled,
  getChannelTopic,
  getChannels,
  joinChannel,
  leaveChannel,
  markRead,
  pinChannelMessage,
  postChannelPoll,
  reactToChannelMessage,
  scheduleChannelMessage,
  searchChannelMessages,
  sendChannelMessage,
  setChannelTopic,
  syncChannels,
  unpinChannelMessage,
  voteChannelPoll,
} from "./api";
import type {
  Channel,
  ChannelMessage,
  ChannelTopic,
  ScheduledChannelMessage,
} from "./types";

export interface UseChannelsResult {
  channels: Channel[];
  /** Selected channel name, or null. */
  selected: string | null;
  select: (channel: string | null) => void;
  messages: ChannelMessage[];
  /** Queued channel messages for the selected channel (soonest first). */
  scheduled: ScheduledChannelMessage[];
  /** Current topic of the selected channel, or null when unset. */
  topic: ChannelTopic | null;
  /** Pinned messages for the selected channel, newest pin first. */
  pins: ChannelMessage[];
  /** Search matches for the selected channel, or null when not searching. */
  searchResults: ChannelMessage[] | null;
  /** The query behind `searchResults`, or null when not searching. */
  searchQuery: string | null;
  search: (query: string) => Promise<void>;
  clearSearch: () => void;
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
  join: (channel: string) => Promise<void>;
  leave: (channel: string) => Promise<void>;
  send: (text: string, replyTo?: string) => Promise<ChannelMessage>;
  schedule: (text: string, sendIn: number) => Promise<ScheduledChannelMessage>;
  cancelScheduled: (id: string) => Promise<void>;
  poll: (question: string, options: string[]) => Promise<ChannelMessage>;
  vote: (poll: string, option: number) => Promise<ChannelMessage>;
  closePoll: (poll: string) => Promise<ChannelMessage>;
  setTopic: (text: string) => Promise<void>;
  clearTopic: () => Promise<void>;
  /** React to a message by hash; an empty reaction clears yours. */
  react: (target: string, reaction: string) => Promise<void>;
  /** Edit your own message in the selected channel, then refresh. */
  edit: (target: string, text: string) => Promise<void>;
  sync: (channel?: string) => Promise<void>;
  pin: (hash: string) => Promise<void>;
  unpin: (hash: string) => Promise<void>;
}

export function useChannels(pollMs = 30_000): UseChannelsResult {
  const [channels, setChannels] = useState<Channel[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [messages, setMessages] = useState<ChannelMessage[]>([]);
  const [scheduled, setScheduled] = useState<ScheduledChannelMessage[]>([]);
  const [topic, setTopicState] = useState<ChannelTopic | null>(null);
  const [pins, setPins] = useState<ChannelMessage[]>([]);
  const [searchResults, setSearchResults] = useState<ChannelMessage[] | null>(null);
  const [searchQuery, setSearchQuery] = useState<string | null>(null);
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
      setPins([]);
      return;
    }
    try {
      setMessages(await getChannelMessages(channel));
      setPins(await getChannelPins(channel));
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
    try {
      setScheduled(await getChannelScheduled(channel));
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

  const select = useCallback(
    (channel: string | null) => {
      setSelected(channel);
      // A search belongs to the channel it ran in; switching channels
      // drops it rather than showing stale matches under a new name.
      setSearchResults(null);
      setSearchQuery(null);
      void refreshMessages(channel);
      void refreshTopic(channel);
      if (channel) {
        // Opening a channel clears its badge; refresh the list so
        // the cleared count shows without waiting for the next poll.
        // A failed mark (e.g. the channel was just left in another
        // tab) must not break opening it.
        void markRead({ channel })
          .then(() => refreshChannels())
          .catch(() => undefined);
      }
    },
    [refreshMessages, refreshChannels, refreshTopic],
  );

  const search = useCallback(async (query: string) => {
    if (!selected) throw new Error("no channel selected");
    const trimmed = query.trim();
    if (!trimmed) {
      setSearchResults(null);
      setSearchQuery(null);
      return;
    }
    try {
      const results = await searchChannelMessages(selected, trimmed);
      setSearchResults(results);
      setSearchQuery(trimmed);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }, [selected]);

  const clearSearch = useCallback(() => {
    setSearchResults(null);
    setSearchQuery(null);
  }, []);

  return {
    channels,
    selected,
    select,
    messages,
    scheduled,
    topic,
    pins,
    searchResults,
    searchQuery,
    search,
    clearSearch,
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
        setScheduled([]);
        setTopicState(null);
        setPins([]);
        setSearchResults(null);
        setSearchQuery(null);
      }
    },
    send: async (text, replyTo) => {
      if (!selected) throw new Error("no channel selected");
      const sent = await sendChannelMessage(selected, text, replyTo);
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
    poll: async (question, options) => {
      if (!selected) throw new Error("no channel selected");
      const posted = await postChannelPoll(selected, question, options);
      await refreshMessages(selected);
      return posted;
    },
    vote: async (poll, option) => {
      if (!selected) throw new Error("no channel selected");
      const cast = await voteChannelPoll(selected, poll, option);
      await refreshMessages(selected);
      return cast;
    },
    closePoll: async (poll) => {
      if (!selected) throw new Error("no channel selected");
      const closed = await closeChannelPoll(selected, poll);
      await refreshMessages(selected);
      return closed;
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
    react: async (target, reaction) => {
      if (!selected) throw new Error("no channel selected");
      await reactToChannelMessage(selected, target, reaction);
      await refreshMessages(selected);
    },
    edit: async (target, text) => {
      if (!selected) throw new Error("no channel selected");
      await editChannelMessage(selected, target, text);
      await refreshMessages(selected);
    },
    sync: async (channel) => {
      await syncChannels(channel ?? selected ?? undefined);
      await refreshChannels();
      await refreshMessages(selected);
      await refreshTopic(selected);
    },
    pin: async (hash) => {
      if (!selected) throw new Error("no channel selected");
      await pinChannelMessage(selected, hash);
      await refreshMessages(selected);
    },
    unpin: async (hash) => {
      if (!selected) throw new Error("no channel selected");
      await unpinChannelMessage(selected, hash);
      await refreshMessages(selected);
    },
  };
}
