// ChannelsSection.tsx — public channels: join / leave, per-channel
// message history with a composer, search, and sync. Built on useChannels
// (web/lib/use-channels.ts).

"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import Avatar from "@/components/chrome/Avatar";
import UnreadChip from "@/components/chrome/UnreadChip";
import { useHash } from "../chrome/useHash";
import { formatCountdown, formatRelative, shortHash } from "@/lib/format";
import { deleteDraft, getDrafts, getStatus, saveDraft } from "@/lib/api";
import type { LocationPin, MessageDraft } from "@/lib/types";
import { useChannels } from "@/lib/use-channels";

/** A pinned place on a channel message, linked out to a map. Shared by
 *  the history list and the pinned strip: a pin can outlive its
 *  presence in the fetched history page, so the strip needs the link
 *  too. */
function ChannelLocationLink({ location }: { location: LocationPin }) {
  return (
    <p className="mt-1 text-sm">
      <a
        href={`https://www.openstreetmap.org/?mlat=${location.lat}&mlon=${location.lon}#map=15/${location.lat}/${location.lon}`}
        target="_blank"
        rel="noreferrer"
        className="underline text-cyan-300"
      >
        📍 {location.name ?? `${location.lat.toFixed(5)}, ${location.lon.toFixed(5)}`}
      </a>
    </p>
  );
}

const SCHEDULE_OPTIONS: { label: string; seconds: number | undefined }[] = [
  { label: "Send now", seconds: undefined },
  { label: "In 5 minutes", seconds: 5 * 60 },
  { label: "In 1 hour", seconds: 3600 },
  { label: "Tomorrow", seconds: 86400 },
];

// Send-time choices when editing a queued message: "Keep time" leaves
// the publish time alone, the rest move it from now.
const EDIT_SCHEDULE_OPTIONS: { label: string; seconds: number | undefined }[] = [
  { label: "Keep time", seconds: undefined },
  { label: "In 5 minutes", seconds: 5 * 60 },
  { label: "In 1 hour", seconds: 3600 },
  { label: "Tomorrow", seconds: 86400 },
];

export default function ChannelsSection() {
  const channels = useChannels(30_000);
  const [joinName, setJoinName] = useState("");
  const [text, setText] = useState("");
  const [sendIn, setSendIn] = useState<number | undefined>(undefined);
  const [searchText, setSearchText] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [sending, setSending] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [muting, setMuting] = useState(false);
  const [cancelling, setCancelling] = useState<string | null>(null);
  // The scheduled message being edited, if any, with its draft text
  // and an optional new send time (undefined keeps the current time).
  const [editingScheduled, setEditingScheduled] = useState<string | null>(null);
  const [editScheduledText, setEditScheduledText] = useState("");
  const [editScheduledSendIn, setEditScheduledSendIn] = useState<
    number | undefined
  >(undefined);
  const [savingScheduledEdit, setSavingScheduledEdit] = useState(false);
  const [pollOpen, setPollOpen] = useState(false);
  const [pollQuestion, setPollQuestion] = useState("");
  const [pollOptions, setPollOptions] = useState<string[]>(["", ""]);
  const [editingTopic, setEditingTopic] = useState(false);
  const [topicText, setTopicText] = useState("");
  const [savingTopic, setSavingTopic] = useState(false);
  const [searching, setSearching] = useState(false);
  const [reacting, setReacting] = useState<string | null>(null);
  // The message being answered, if any: the composer sends with its hash.
  const [replyTo, setReplyTo] = useState<{ hash: string; name: string } | null>(
    null,
  );

  // This node's own destination hash, so only its messages get an
  // Edit button — an edit is author-only on the wire.  Null until
  // /api/status answers, or when it cannot (edit stays hidden).
  const [ownHash, setOwnHash] = useState<string | null>(null);
  const [editingHash, setEditingHash] = useState<string | null>(null);
  const [editText, setEditText] = useState("");
  const [savingEdit, setSavingEdit] = useState(false);

  // Saved message drafts (``tau draft save --channel``): a draft for
  // the selected channel pre-fills the composer, and sending the
  // message closes the draft, like ``draft send``.
  const [drafts, setDrafts] = useState<MessageDraft[]>([]);
  const [pendingSeed, setPendingSeed] = useState<{ key: string; text: string } | null>(null);
  const [draftSeedKey, setDraftSeedKey] = useState<string | null>(null);
  // The draft whose text was actually loaded into the composer for
  // the selected channel. Only sending that text closes the draft —
  // an unrelated message sent in the same channel leaves a saved
  // draft alone.
  const [loadedDraftKey, setLoadedDraftKey] = useState<string | null>(null);
  // Unsent composer state parked per channel: the text plus the
  // mark saying which saved draft (if any) that text came from.
  // A switch restores the whole composer, so a draft opened, left,
  // and returned to still closes on send instead of re-seeding
  // and sending twice. The seed-consumed mark is deliberately NOT
  // parked: a return is a fresh open, so a saved draft may pre-fill
  // the box again — the send path drops a sent draft from the local
  // list at once, so a deleted draft cannot re-seed while its
  // refresh is still in flight.
  const [composerByChannel, setComposerByChannel] = useState<
    Record<string, { text: string; loadedDraftKey: string | null }>
  >({});
  const prevSelectedRef = useRef<string | null>(null);
  // Generation counter for draft-list requests: only the latest
  // response may apply, so a stale list cannot overwrite newer
  // state (e.g. a pre-delete list arriving after the post-delete
  // refresh and resurrecting a sent draft).
  const draftsRequestRef = useRef(0);
  const [savingDraft, setSavingDraft] = useState(false);

  // The channel named by `#channels?channel=<name>` — set when a
  // channel draft is opened from the chat sidebar. The shell mounts
  // one section at a time, so a fresh mount here reads it.
  const { channel: hashChannel, navigate } = useHash();

  useEffect(() => {
    if (hashChannel && hashChannel !== channels.selected) {
      channels.select(hashChannel);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hashChannel]);

  const refreshDrafts = useCallback(async () => {
    const requestId = ++draftsRequestRef.current;
    try {
      const next = await getDrafts();
      if (draftsRequestRef.current === requestId) setDrafts(next);
    } catch {
      // Drafts are advisory; the channel list works without them.
    }
  }, []);

  useEffect(() => {
    void refreshDrafts();
  }, [refreshDrafts]);

  // A channel with a saved draft loads it into the composer when
  // opened; a channel without one clears the seed. The mark of which
  // seed was already applied is cleared only on a channel switch:
  // clearing it on every drafts refresh would undo the save path's
  // consumed mark and pour a just-saved draft back into the box.
  const selectedChannelName = channels.selected;
  const selectedDraftKey = selectedChannelName ? `channel:${selectedChannelName}` : null;
  useEffect(() => {
    // Park the outgoing channel's unsent composer state and restore
    // the incoming channel's own parked state (or an empty box), so
    // a switch never carries one channel's text — or its draft
    // marks — into another. A return is a fresh open: the
    // seed-consumed mark is cleared, so a saved draft may pre-fill
    // the box again.
    const prev = prevSelectedRef.current;
    if (prev && prev !== selectedChannelName) {
      setComposerByChannel((m) => ({
        ...m,
        [prev]: { text, loadedDraftKey },
      }));
    }
    prevSelectedRef.current = selectedChannelName;
    const parked = selectedChannelName
      ? (composerByChannel[selectedChannelName] ?? {
          text: "",
          loadedDraftKey: null,
        })
      : { text: "", loadedDraftKey: null };
    setText(parked.text);
    setLoadedDraftKey(parked.loadedDraftKey);
    setDraftSeedKey(null);
    setPendingSeed(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedDraftKey]);
  useEffect(() => {
    if (!selectedDraftKey || !selectedChannelName) return;
    const draft = drafts.find(
      (d) => d.kind === "channel" && d.target === selectedChannelName,
    );
    setPendingSeed(draft ? { key: selectedDraftKey, text: draft.text } : null);
  }, [selectedDraftKey, selectedChannelName, drafts]);

  // A saved draft pre-fills the box when its channel opens — but
  // never clobbers text the user already typed into this composer.
  // The seed is marked consumed only when its text actually loads:
  // marking it consumed while the box holds other text would lose
  // the draft without the user ever seeing it.
  if (pendingSeed && draftSeedKey !== pendingSeed.key) {
    if (text.trim() === "") {
      setDraftSeedKey(pendingSeed.key);
      setLoadedDraftKey(pendingSeed.key);
      setText(pendingSeed.text);
    }
  }

  useEffect(() => {
    let live = true;
    getStatus()
      .then((snapshot) => {
        if (live) setOwnHash(snapshot?.node?.destination ?? null);
      })
      .catch(() => {
        // Status unavailable: no edit buttons, reading still works.
      });
    return () => {
      live = false;
    };
  }, []);

  async function handleSaveEdit(hash: string) {
    const body = editText.trim();
    if (!body) return;
    setSavingEdit(true);
    setError(null);
    try {
      await channels.edit(hash, body);
      setEditingHash(null);
      setEditText("");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSavingEdit(false);
    }
  }

  const selectedChannel = channels.channels.find((c) => c.channel === channels.selected);

  async function handleReact(hash: string, reaction: string) {
    setReacting(hash);
    setError(null);
    try {
      await channels.react(hash, reaction);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setReacting(null);
    }
  }

  async function handleJoin(e: React.FormEvent) {
    e.preventDefault();
    const name = joinName.trim();
    if (!name) return setError("Type a channel name to join.");
    setError(null);
    try {
      await channels.join(name);
      setJoinName("");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  async function handleSend(e: React.FormEvent) {
    e.preventDefault();
    const body = text.trim();
    const channel = channels.selected;
    if (!body || !channel) return;
    setSending(true);
    setError(null);
    try {
      if (sendIn !== undefined) {
        await channels.schedule(body, sendIn);
        setSendIn(undefined);
      } else {
        await channels.send(body, replyTo?.hash);
      }
      setText("");
      setReplyTo(null);
      // A sent message closes the draft it was loaded from, like
      // `draft send` — but only when the send actually went through
      // and the sent text came from that draft. A saved draft
      // survives an unrelated message sent in the same channel.
      if (loadedDraftKey && loadedDraftKey === selectedDraftKey) {
        // Make the draft ineligible for seeding BEFORE awaiting the
        // deletion: a switch away and back during a slow delete must
        // not reload the already-sent text. (If the delete itself
        // fails, the refresh puts the draft back, and the kept seed
        // mark stops it re-pouring this visit.)
        setDrafts((prev) =>
          prev.filter((d) => !(d.kind === "channel" && d.target === channel)),
        );
        setPendingSeed(null);
        await deleteDraft("channel", channel).catch(() => undefined);
        void refreshDrafts();
      }
      setLoadedDraftKey(null);
      // Clear the sent channel's parked composer text. The live
      // seed-consumed mark is left alone — the optimistic removal
      // above already stops the deleted draft re-seeding the box.
      setComposerByChannel((m) => ({
        ...m,
        [channel]: { text: "", loadedDraftKey: null },
      }));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSending(false);
    }
  }

  async function handleSaveDraft() {
    const channel = channels.selected;
    if (!channel || !text.trim() || savingDraft) return;
    setSavingDraft(true);
    setError(null);
    try {
      await saveDraft("channel", channel, text.trim());
      setText("");
      // A freshly saved draft is not loaded text: a later send of
      // a different message must not delete it.
      setLoadedDraftKey(null);
      // Mark the just-saved draft consumed, so the seed effect does
      // not pour it straight back into the cleared box once the
      // drafts refresh lands.
      if (selectedDraftKey) setDraftSeedKey(selectedDraftKey);
      setComposerByChannel((m) => ({
        ...m,
        [channel]: { text: "", loadedDraftKey: null },
      }));
      void refreshDrafts();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSavingDraft(false);
    }
  }

  async function handleCancelScheduled(id: string) {
    setCancelling(id);
    setError(null);
    try {
      await channels.cancelScheduled(id);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setCancelling(null);
    }
  }

  async function handleSaveScheduledEdit(id: string, originalText: string) {
    const body = editScheduledText.trim();
    if (!body) return;
    // An unchanged save makes no request — just close the form.
    if (body === originalText && editScheduledSendIn === undefined) {
      setEditingScheduled(null);
      return;
    }
    setSavingScheduledEdit(true);
    setError(null);
    try {
      await channels.editScheduled(id, {
        ...(body !== originalText ? { text: body } : {}),
        ...(editScheduledSendIn !== undefined
          ? { sendIn: editScheduledSendIn }
          : {}),
      });
      setEditingScheduled(null);
      setEditScheduledText("");
      setEditScheduledSendIn(undefined);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSavingScheduledEdit(false);
    }
  }

  async function handleSync() {
    setSyncing(true);
    setError(null);
    try {
      await channels.sync();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSyncing(false);
    }
  }

  async function handleMute() {
    if (!channels.selected) return;
    setMuting(true);
    setError(null);
    try {
      await channels.mute(channels.selected, !(selectedChannel?.muted ?? false));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setMuting(false);
    }
  }

  async function handlePoll(e: React.FormEvent) {
    e.preventDefault();
    const question = pollQuestion.trim();
    const options = pollOptions.map((o) => o.trim()).filter(Boolean);
    if (!question) return setError("A poll needs a question.");
    if (options.length < 2) return setError("A poll needs at least two options.");
    if (options.length > 8) return setError("A poll can have at most eight options.");
    if (!channels.selected) return;
    setSending(true);
    setError(null);
    try {
      if (sendIn !== undefined) {
        await channels.schedulePoll(question, options, sendIn);
        setSendIn(undefined);
      } else {
        await channels.poll(question, options);
      }
      setPollQuestion("");
      setPollOptions(["", ""]);
      setPollOpen(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSending(false);
    }
  }

  async function handleVote(hash: string, option: number) {
    setError(null);
    try {
      await channels.vote(hash, option);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  async function handleSaveTopic(e: React.FormEvent) {
    e.preventDefault();
    const body = topicText.trim();
    if (!body || !channels.selected) return;
    setSavingTopic(true);
    setError(null);
    try {
      await channels.setTopic(body);
      setEditingTopic(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSavingTopic(false);
    }
  }

  async function handleClearTopic() {
    if (!channels.selected) return;
    setSavingTopic(true);
    setError(null);
    try {
      await channels.clearTopic();
      setEditingTopic(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSavingTopic(false);
    }
  }

  async function handlePin(hash: string) {
    setError(null);
    try {
      await channels.pin(hash);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  async function handleClosePoll(hash: string) {
    setError(null);
    try {
      await channels.closePoll(hash);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  async function handleUnpin(hash: string) {
    setError(null);
    try {
      await channels.unpin(hash);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  async function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    const query = searchText.trim();
    if (!query || !channels.selected) return;
    setSearching(true);
    setError(null);
    try {
      await channels.search(query);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSearching(false);
    }
  }

  function handleClearSearch() {
    setSearchText("");
    channels.clearSearch();
  }

  // History arrives newest-first; show it oldest-first like a chat log.
  // Search matches stay newest-first: the most recent match first.
  const showingSearch = channels.searchResults !== null;
  const messages = showingSearch
    ? channels.searchResults!
    : [...channels.messages].reverse();
  // Pins arrive newest pin first and stay in that order up top.
  const pinnedHashes = new Set(channels.pins.map((p) => p.hash));
  // Replies quote their parent from the loaded page; a parent that
  // has not arrived yet shows its short digest instead.
  const byHash = new Map(channels.messages.map((m) => [m.hash, m]));

  return (
    <div className="mx-auto w-full max-w-[600px]">
      <header className="sticky top-0 z-10 border-b border-zinc-800 bg-zinc-950/95 px-3 py-3 backdrop-blur sm:px-4">
        <h1 className="text-lg font-bold text-zinc-50">
          <span className="mr-2 text-cyan-400">τ</span>Channels
        </h1>
        <p className="mt-0.5 text-xs text-zinc-500">
          Public channels — messages here go to the whole mesh, not to one peer.
        </p>
      </header>

      <div className="border-b border-zinc-800 px-3 py-3 sm:px-4">
        <form onSubmit={handleJoin} className="flex gap-2">
          <input
            value={joinName}
            onChange={(e) => setJoinName(e.target.value)}
            placeholder="Join a channel, e.g. mesh-general"
            aria-label="Channel name to join"
            className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
          />
          <button
            type="submit"
            className="shrink-0 rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-bold text-zinc-950 hover:bg-cyan-400"
          >
            Join
          </button>
        </form>
        {channels.loading && channels.channels.length === 0 && (
          <p className="py-3 text-sm text-zinc-500">Loading channels…</p>
        )}
        {channels.error && (
          <p role="alert" className="py-2 text-sm text-red-400">
            {channels.error}
          </p>
        )}
        <div className="mt-3 flex flex-wrap gap-2" aria-label="Joined channels">
          {channels.channels.length === 0 && !channels.loading && (
            <p className="text-sm text-zinc-500">
              You haven&apos;t joined any channels yet.
            </p>
          )}
          {channels.channels.map((c) => (
            <button
              key={c.channel}
              type="button"
              aria-pressed={channels.selected === c.channel}
              onClick={() => {
                // Select now (as before) and keep the URL hash in
                // sync, so refresh / shared links reopen this channel.
                channels.select(c.channel);
                navigate("channels", { channel: c.channel });
              }}
              className={`rounded-full border px-3 py-1.5 text-sm font-semibold ${
                channels.selected === c.channel
                  ? "border-cyan-500 bg-cyan-950 text-cyan-200"
                  : "border-zinc-700 text-zinc-300 hover:bg-zinc-900"
              }`}
            >
              {c.pinned ? "📌 " : ""}#{c.channel}
              {c.muted && (
                <span title="Muted" aria-label="Muted channel">
                  🔇{" "}
                </span>
              )}
              <UnreadChip count={c.unread} className="ml-1" />
            </button>
          ))}
        </div>
      </div>

      {error && (
        <p role="alert" className="border-b border-zinc-800 px-4 py-2 text-sm text-red-400">
          {error}
        </p>
      )}

      {!channels.selected && (
        <p className="px-4 py-8 text-center text-sm text-zinc-500">
          Pick a channel above to read its messages.
        </p>
      )}

      {channels.selected && (
        <div>
          <div className="flex items-center justify-between gap-2 border-b border-zinc-800 px-3 py-2 sm:px-4">
            <h2 className="min-w-0 truncate text-sm font-bold text-zinc-100">
              #{channels.selected}
              {selectedChannel && (
                <span className="ml-2 font-normal text-xs text-zinc-500">
                  joined {formatRelative(selectedChannel.joined_at)}
                </span>
              )}
            </h2>
            <div className="flex shrink-0 gap-2">
              <button
                type="button"
                onClick={() => void handleMute()}
                disabled={muting}
                className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-semibold text-zinc-300 hover:bg-zinc-900 disabled:opacity-50"
              >
                {muting ? "…" : selectedChannel?.muted ? "Unmute" : "Mute"}
              </button>
              <button
                type="button"
                onClick={() => void channels.pinConversation(channels.selected!, !selectedChannel?.pinned)}
                className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-semibold text-zinc-300 hover:bg-zinc-900 disabled:opacity-50"
              >
                {selectedChannel?.pinned ? "Unpin" : "Pin"}
              </button>
              <button
                type="button"
                onClick={() => void handleSync()}
                disabled={syncing}
                className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-semibold text-zinc-300 hover:bg-zinc-900 disabled:opacity-50"
              >
                {syncing ? "Syncing…" : "↻ Sync"}
              </button>
              <button
                type="button"
                onClick={() => void channels.leave(channels.selected!)}
                className="rounded-full border border-red-800 px-3 py-1 text-xs text-red-400 hover:bg-red-950"
              >
                Leave
              </button>
            </div>
          </div>

          <div className="border-b border-zinc-800 px-3 py-2 sm:px-4">
            {editingTopic ? (
              <form onSubmit={handleSaveTopic} className="flex gap-2">
                <input
                  value={topicText}
                  onChange={(e) => setTopicText(e.target.value)}
                  placeholder="What is this channel about? (max 140 characters)"
                  aria-label="Channel topic"
                  maxLength={140}
                  disabled={savingTopic}
                  className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
                />
                <button
                  type="submit"
                  disabled={savingTopic || !topicText.trim()}
                  className="shrink-0 rounded-full bg-cyan-500 px-3 py-1.5 text-xs font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
                >
                  {savingTopic ? "…" : "Save"}
                </button>
                <button
                  type="button"
                  onClick={() => setEditingTopic(false)}
                  disabled={savingTopic}
                  className="shrink-0 rounded-full border border-zinc-700 px-3 py-1.5 text-xs font-semibold text-zinc-300 hover:bg-zinc-900 disabled:opacity-50"
                >
                  Cancel
                </button>
              </form>
            ) : (
              <div className="flex items-center justify-between gap-2">
                <p className="min-w-0 truncate text-sm text-zinc-400">
                  {channels.topic ? (
                    <span title={channels.topic.topic}>{channels.topic.topic}</span>
                  ) : (
                    <span className="italic text-zinc-500">No topic set.</span>
                  )}
                </p>
                <div className="flex shrink-0 gap-2">
                  <button
                    type="button"
                    onClick={() => {
                      setTopicText(channels.topic?.topic ?? "");
                      setEditingTopic(true);
                    }}
                    className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-semibold text-zinc-300 hover:bg-zinc-900"
                  >
                    {channels.topic ? "Edit topic" : "Set topic"}
                  </button>
                  {channels.topic && (
                    <button
                      type="button"
                      onClick={() => void handleClearTopic()}
                      disabled={savingTopic}
                      className="rounded-full border border-zinc-700 px-3 py-1 text-xs font-semibold text-zinc-300 hover:bg-zinc-900 disabled:opacity-50"
                    >
                      Clear topic
                    </button>
                  )}
                </div>
              </div>
            )}
          </div>

          <form
            onSubmit={handleSearch}
            className="flex gap-2 border-b border-zinc-800 px-3 py-2 sm:px-4"
          >
            <input
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              placeholder={`Search #${channels.selected}`}
              aria-label={`Search #${channels.selected}`}
              disabled={searching}
              className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
            />
            <button
              type="submit"
              disabled={searching || !searchText.trim()}
              className="shrink-0 rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
            >
              {searching ? "…" : "Search"}
            </button>
            {channels.searchResults !== null && (
              <button
                type="button"
                onClick={handleClearSearch}
                className="shrink-0 rounded-full border border-zinc-700 px-3 py-1.5 text-sm text-zinc-300 hover:bg-zinc-900"
              >
                Clear
              </button>
            )}
          </form>
          {channels.searchResults !== null && (
            <p className="border-b border-zinc-800 px-4 py-2 text-xs text-zinc-500">
              {channels.searchResults.length} match
              {channels.searchResults.length === 1 ? "" : "es"} for &ldquo;
              {channels.searchQuery}&rdquo; — newest first.
            </p>
          )}

          <div className="px-3 py-2 sm:px-4">
            {channels.scheduled.length > 0 && (
              <div className="mb-2 rounded-lg border border-zinc-800 bg-zinc-900/50 px-3 py-2">
                <p className="text-xs font-semibold text-zinc-400">
                  Scheduled — publishes when the time comes
                </p>
                {channels.scheduled.map((s) => (
                  <div key={s.id} className="mt-2 flex items-start gap-2">
                    <div className="min-w-0 flex-1">
                      {editingScheduled === s.id ? (
                        <div>
                          <textarea
                            value={editScheduledText}
                            onChange={(e) => setEditScheduledText(e.target.value)}
                            rows={2}
                            aria-label="Edit scheduled message"
                            className="w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500"
                          />
                          <div className="mt-1.5 flex flex-wrap items-center gap-2">
                            <select
                              value={
                                editScheduledSendIn === undefined
                                  ? ""
                                  : String(editScheduledSendIn)
                              }
                              onChange={(e) =>
                                setEditScheduledSendIn(
                                  e.target.value === ""
                                    ? undefined
                                    : Number(e.target.value),
                                )
                              }
                              aria-label="New send time"
                              className="rounded-full border border-zinc-700 bg-zinc-900 px-2 py-1 text-xs text-zinc-300 outline-none focus:border-cyan-500"
                            >
                              {EDIT_SCHEDULE_OPTIONS.map((o) => (
                                <option
                                  key={o.label}
                                  value={o.seconds === undefined ? "" : o.seconds}
                                >
                                  {o.label}
                                </option>
                              ))}
                            </select>
                            <button
                              type="button"
                              onClick={() => void handleSaveScheduledEdit(s.id, s.text)}
                              disabled={
                                savingScheduledEdit || !editScheduledText.trim()
                              }
                              className="rounded-full bg-cyan-500 px-3 py-1 text-xs font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
                            >
                              {savingScheduledEdit ? "Saving…" : "Save"}
                            </button>
                            <button
                              type="button"
                              onClick={() => {
                                setEditingScheduled(null);
                                setEditScheduledText("");
                                setEditScheduledSendIn(undefined);
                              }}
                              className="rounded-full border border-zinc-700 px-3 py-1 text-xs text-zinc-300 hover:bg-zinc-900"
                            >
                              Cancel
                            </button>
                          </div>
                        </div>
                      ) : (
                        <>
                          {s.kind === "poll" && (
                            <p className="text-xs font-semibold text-zinc-400">
                              Poll · {(s.options ?? []).length} options
                            </p>
                          )}
                          <p className="text-sm whitespace-pre-wrap text-zinc-300">{s.text}</p>
                          {s.kind === "poll" &&
                            (s.options ?? []).map((option, i) => (
                              <p key={i} className="text-xs text-zinc-500">
                                {i + 1}. {option}
                              </p>
                            ))}
                          <p className="text-xs text-zinc-500">
                            {s.publish_at * 1000 <= Date.now()
                              ? "due — publishes on the next serve pass"
                              : formatCountdown(s.publish_at)}
                          </p>
                        </>
                      )}
                    </div>
                    {editingScheduled !== s.id && (
                      <>
                        <button
                          type="button"
                          onClick={() => {
                            setEditingScheduled(s.id);
                            setEditScheduledText(s.text);
                            setEditScheduledSendIn(undefined);
                          }}
                          aria-label="Edit scheduled message"
                          className="shrink-0 rounded-full border border-zinc-700 px-2 py-0.5 text-xs text-zinc-300 hover:bg-zinc-900"
                        >
                          Edit
                        </button>
                        <button
                          type="button"
                          onClick={() => void handleCancelScheduled(s.id)}
                          disabled={cancelling === s.id}
                          className="shrink-0 rounded-full border border-zinc-700 px-2 py-0.5 text-xs text-zinc-300 hover:bg-zinc-900 disabled:opacity-50"
                        >
                          {cancelling === s.id ? "Cancelling…" : "Cancel"}
                        </button>
                      </>
                    )}
                  </div>
                ))}
              </div>
            )}
            {channels.pins.length > 0 && (
              <div className="mb-2 rounded-lg border border-amber-800/60 bg-amber-950/40 px-3 py-2">
                <p className="text-xs font-bold uppercase tracking-wide text-amber-400">
                  Pinned
                </p>
                {channels.pins.map((m) => (
                  <div key={m.hash} className="flex gap-3 border-b border-amber-800/30 py-2 last:border-b-0">
                    <Avatar name={m.name} seed={m.author} size="sm" />
                    <div className="min-w-0 flex-1">
                      <p className="text-sm">
                        <span className="font-bold text-zinc-100">
                          {m.name || shortHash(m.author)}
                        </span>{" "}
                        <span className="text-xs text-zinc-500">
                          · {formatRelative(m.created_at)}
                        </span>
                      </p>
                      <p className="mt-0.5 text-sm leading-relaxed whitespace-pre-wrap text-zinc-200">
                        {m.text}
                      </p>
                      {m.location && <ChannelLocationLink location={m.location} />}
                    </div>
                    <button
                      type="button"
                      onClick={() => void handleUnpin(m.hash)}
                      aria-label={`Unpin pinned message from ${m.name || shortHash(m.author)}`}
                      className="h-fit shrink-0 rounded-full border border-zinc-700 px-2 py-0.5 text-xs font-semibold text-zinc-300 hover:bg-zinc-900"
                    >
                      Unpin
                    </button>
                  </div>
                ))}
              </div>
            )}
            {messages.length === 0 && !showingSearch && (
              <p className="py-6 text-center text-sm text-zinc-500">
                No messages in this channel yet — say hello.
              </p>
            )}
            {messages.length === 0 && showingSearch && (
              <p className="py-6 text-center text-sm text-zinc-500">
                No messages in this channel match &ldquo;{channels.searchQuery}&rdquo;.
              </p>
            )}
            {messages.map((m) => {
              const parent = m.reply_to ? byHash.get(m.reply_to) : undefined;
              return (
              <div key={m.hash} className="flex gap-3 border-b border-zinc-800/60 py-3">
                <Avatar name={m.name} seed={m.author} size="sm" />
                <div className="min-w-0 flex-1">
                  <p className="text-sm">
                    <span className="font-bold text-zinc-100">
                      {m.name || shortHash(m.author)}
                    </span>{" "}
                    <span className="text-xs text-zinc-500">
                      · {formatRelative(m.created_at)}
                      {m.edited ? " · edited" : ""}
                    </span>
                    {ownHash && m.author === ownHash && editingHash !== m.hash && (
                      <button
                        type="button"
                        onClick={() => {
                          setEditingHash(m.hash);
                          setEditText(m.text);
                        }}
                        className="ml-2 text-xs font-semibold text-cyan-400 hover:text-cyan-300"
                      >
                        Edit
                      </button>
                    )}
                  </p>
                  {m.reply_to && (
                    <p className="mt-0.5 truncate border-l-2 border-zinc-700 pl-2 text-xs text-zinc-500">
                      ↩{" "}
                      {parent
                        ? `${parent.name || shortHash(parent.author)}: ${parent.text.split("\n")[0]}`
                        : `replying to ${shortHash(m.reply_to)}`}
                    </p>
                  )}
                  {editingHash === m.hash ? (
                    <div className="mt-1">
                      <textarea
                        value={editText}
                        onChange={(e) => setEditText(e.target.value)}
                        aria-label="Edit message"
                        rows={3}
                        className="w-full rounded-lg border border-zinc-700 bg-zinc-900 px-2 py-1.5 text-sm text-zinc-100 outline-none focus:border-cyan-500"
                      />
                      <div className="mt-1 flex gap-2">
                        <button
                          type="button"
                          onClick={() => void handleSaveEdit(m.hash)}
                          disabled={savingEdit || !editText.trim() || editText === m.text}
                          className="rounded-full bg-cyan-500 px-3 py-1 text-xs font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
                        >
                          {savingEdit ? "Saving…" : "Save"}
                        </button>
                        <button
                          type="button"
                          onClick={() => {
                            setEditingHash(null);
                            setEditText("");
                          }}
                          className="rounded-full border border-zinc-700 px-3 py-1 text-xs text-zinc-300 hover:bg-zinc-900"
                        >
                          Cancel
                        </button>
                      </div>
                    </div>
                  ) : (
                    <p className="mt-0.5 text-sm leading-relaxed whitespace-pre-wrap text-zinc-200">
                      {m.text}
                    </p>
                  )}
                  {m.location && <ChannelLocationLink location={m.location} />}
{m.poll_options && (
                    <div className="mt-2 flex flex-col gap-1.5" role="group" aria-label="Poll">
                      {m.poll_options.map((option, i) => {
                        const count = m.poll?.counts?.[i] ?? 0;
                        const total = m.poll?.total ?? 0;
                        const pct = total > 0 ? Math.round((count / total) * 100) : 0;
                        const closed = m.poll?.closed ?? false;
                        const myVote = m.poll?.my_vote ?? null;
                        return (
                          <button
                            key={i}
                            type="button"
                            disabled={closed}
                            onClick={() => void handleVote(m.hash, i)}
                            title={closed ? "Poll closed" : `Vote for ${option}`}
                            className={`relative overflow-hidden rounded-lg border px-2 py-1.5 text-left text-sm ${
                              myVote === i
                                ? "border-cyan-400 bg-cyan-500/20 font-semibold"
                                : "border-zinc-600 bg-zinc-900/60 hover:border-zinc-400"
                            }`}
                          >
                            <span
                              className="absolute inset-y-0 left-0 bg-zinc-700/60"
                              style={{ width: `${pct}%` }}
                              aria-hidden="true"
                            />
                            <span className="relative flex justify-between gap-2">
                              <span>{option}</span>
                              <span className="text-xs text-zinc-400">
                                {count} · {pct}%
                              </span>
                            </span>
                          </button>
                        );
                      })}
                      <span className="text-xs text-zinc-400">
                        {m.poll?.total ?? 0} vote{(m.poll?.total ?? 0) === 1 ? "" : "s"}
                        {m.poll?.closed ? " · closed" : ""}
                        {m.poll?.own && !m.poll?.closed && (
                          <button
                            type="button"
                            onClick={() => void handleClosePoll(m.hash)}
                            className="ml-2 underline hover:text-zinc-200"
                          >
                            Close poll
                          </button>
                        )}
                      </span>
                    </div>
                  )}
                  <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                    {(m.reactions ?? []).map((r) => (
                      <button
                        key={r.reaction}
                        type="button"
                        title={r.reactors.join(", ")}
                        aria-label={`React ${r.reaction} (${r.count})`}
                        onClick={() => void handleReact(m.hash, r.reaction)}
                        disabled={reacting === m.hash}
                        className="rounded-full border border-zinc-700 bg-zinc-900 px-2 py-0.5 text-xs text-zinc-200 hover:border-zinc-500 disabled:opacity-50"
                      >
                        {r.reaction} {r.count}
                      </button>
                    ))}
                    {["👍", "❤️", "🎉", "🚀", "👀"]
                      .filter((token) => !(m.reactions ?? []).some((r) => r.reaction === token))
                      .map((token) => (
                        <button
                          key={token}
                          type="button"
                          aria-label={`React ${token} to this message`}
                          onClick={() => void handleReact(m.hash, token)}
                          disabled={reacting === m.hash}
                          className="rounded-full border border-transparent px-1.5 py-0.5 text-xs text-zinc-500 hover:border-zinc-700 hover:text-zinc-200 disabled:opacity-50"
                        >
                          {token}
                        </button>
                      ))}
                  </div>
                  <button
                    type="button"
                    aria-label={`Reply to ${m.name || shortHash(m.author)}`}
                    onClick={() =>
                      setReplyTo({ hash: m.hash, name: m.name || shortHash(m.author) })
                    }
                    className="mt-1 text-xs font-semibold text-zinc-500 hover:text-cyan-400"
                  >
                    Reply
                  </button>
                </div>
                <button
                  type="button"
                  onClick={() =>
                    void (pinnedHashes.has(m.hash)
                      ? handleUnpin(m.hash)
                      : handlePin(m.hash))
                  }
                  aria-label={`${pinnedHashes.has(m.hash) ? "Unpin" : "Pin"} message from ${m.name || shortHash(m.author)}`}
                  className="h-fit shrink-0 rounded-full border border-zinc-700 px-2 py-0.5 text-xs font-semibold text-zinc-300 hover:bg-zinc-900"
                >
                  {pinnedHashes.has(m.hash) ? "Pinned" : "Pin"}
                </button>
              </div>
              );
            })}
          </div>

          <form
            onSubmit={pollOpen ? handlePoll : handleSend}
            className="sticky bottom-0 border-t border-zinc-800 bg-zinc-950 px-3 py-2"
          >
            {replyTo && (
              <p className="mb-2 flex items-center justify-between text-xs text-zinc-400">
                <span className="truncate">
                  Replying to <span className="font-semibold text-zinc-200">{replyTo.name}</span>
                </span>
                <button
                  type="button"
                  aria-label="Cancel reply"
                  onClick={() => setReplyTo(null)}
                  className="ml-2 shrink-0 rounded-full border border-zinc-700 px-2 py-0.5 text-xs text-zinc-400 hover:bg-zinc-900"
                >
                  ✕ Cancel
                </button>
              </p>
            )}
            {pollOpen ? (
              <div className="flex flex-col gap-2">
                <input
                  value={pollQuestion}
                  onChange={(e) => setPollQuestion(e.target.value)}
                  placeholder="Poll question"
                  aria-label="Poll question"
                  disabled={sending}
                  className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
                />
                {pollOptions.map((option, i) => (
                  <input
                    key={i}
                    value={option}
                    onChange={(e) =>
                      setPollOptions((prev) =>
                        prev.map((value, j) => (j === i ? e.target.value : value)),
                      )
                    }
                    placeholder={`Option ${i + 1}`}
                    aria-label={`Poll option ${i + 1}`}
                    disabled={sending}
                    className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
                  />
                ))}
                <div className="flex gap-2">
                  {pollOptions.length < 8 && (
                    <button
                      type="button"
                      onClick={() => setPollOptions((prev) => [...prev, ""])}
                      className="rounded-full border border-zinc-700 px-3 py-1.5 text-xs font-semibold text-zinc-300 hover:bg-zinc-900"
                    >
                      + Option
                    </button>
                  )}
                  <label className="flex shrink-0 items-center gap-1 text-xs text-zinc-400">
                    <span aria-hidden="true">🕑</span>
                    <select
                      aria-label="Send later"
                      value={sendIn === undefined ? "" : String(sendIn)}
                      onChange={(e) => setSendIn(e.target.value ? Number(e.target.value) : undefined)}
                      disabled={sending}
                      className="rounded-md border border-zinc-700 bg-zinc-900 px-1.5 py-1 text-xs text-zinc-200 outline-none"
                    >
                      {SCHEDULE_OPTIONS.map((o) => (
                        <option key={o.label} value={o.seconds === undefined ? "" : o.seconds}>
                          {o.label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <button
                    type="submit"
                    disabled={sending}
                    className="shrink-0 rounded-full bg-cyan-500 px-4 py-1.5 text-sm font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
                  >
                    {sending ? "…" : sendIn !== undefined ? "Schedule poll" : "Post poll"}
                  </button>
                  <button
                    type="button"
                    onClick={() => setPollOpen(false)}
                    className="rounded-full border border-zinc-700 px-3 py-1.5 text-xs text-zinc-300 hover:bg-zinc-900"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            ) : (
              <div className="flex gap-2">
                <input
                  value={text}
                  onChange={(e) => {
                    setText(e.target.value);
                    // Emptying the box discards the loaded draft text,
                    // so a later send must not close the saved draft.
                    if (e.target.value.trim() === "") setLoadedDraftKey(null);
                  }}
                  placeholder={`Message #${channels.selected}`}
                  aria-label={`Message #${channels.selected}`}
                  disabled={sending}
                  className="min-w-0 flex-1 rounded-full border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-cyan-500 disabled:opacity-50"
                />
                <label className="flex shrink-0 items-center gap-1 text-xs text-zinc-400">
                  <span aria-hidden="true">🕑</span>
                  <select
                    aria-label="Send later"
                    value={sendIn === undefined ? "" : String(sendIn)}
                    onChange={(e) => setSendIn(e.target.value ? Number(e.target.value) : undefined)}
                    disabled={sending}
                    className="rounded-md border border-zinc-700 bg-zinc-900 px-1.5 py-1 text-xs text-zinc-200 outline-none"
                  >
                    {SCHEDULE_OPTIONS.map((o) => (
                      <option key={o.label} value={o.seconds === undefined ? "" : o.seconds}>
                        {o.label}
                      </option>
                    ))}
                  </select>
                </label>
                <button
                  type="button"
                  onClick={() => setPollOpen(true)}
                  className="shrink-0 rounded-full border border-zinc-700 px-3 py-2 text-sm font-semibold text-zinc-300 hover:bg-zinc-900"
                >
                  Poll
                </button>
                <button
                  type="button"
                  onClick={() => void handleSaveDraft()}
                  disabled={sending || savingDraft || !text.trim()}
                  className="shrink-0 rounded-full border border-zinc-700 px-3 py-2 text-sm font-semibold text-zinc-300 hover:bg-zinc-900 disabled:opacity-50"
                >
                  {savingDraft ? "Saving…" : "Save draft"}
                </button>
                <button
                  type="submit"
                  disabled={sending || !text.trim()}
                  className="shrink-0 rounded-full bg-cyan-500 px-4 py-2 text-sm font-bold text-zinc-950 hover:bg-cyan-400 disabled:opacity-40"
                >
                  {sending ? "…" : sendIn !== undefined ? "Schedule" : "Send"}
                </button>
              </div>
            )}
          </form>
        </div>
      )}
    </div>
  );
}
